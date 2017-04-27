/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "executors.h"

#include <daemon/memcached.h>
#include <daemon/mcbp.h>

std::pair<ENGINE_ERROR_CODE, std::string> list_bucket(
        McbpConnection& connection) {
    if (!connection.isAuthenticated()) {
        return std::make_pair(ENGINE_EACCESS, "");
    }

    // The list bucket command is a bit racy (as it other threads may
    // create/delete/remove access to buckets while we check them), but
    // we don't care about that (we don't want to hold a lock for the
    // entire period, _AND_ the access could be changed right after we
    // built the response anyway.
    std::string blob;
    // The blob string will contain all of the buckets, and to
    // avoid too many reallocations we should probably just reserve
    // a chunk
    blob.reserve(100);

    for (auto& bucket : all_buckets) {
        std::string bucketname;
        cb_mutex_enter(&bucket.mutex);
        if (bucket.state == BucketState::Ready) {
            bucketname = bucket.name;
        }
        cb_mutex_exit(&bucket.mutex);

        if (bucketname.empty()) {
            // ignore this one
            continue;
        }

        try {
            // Check if the user have access to the bucket
            cb::rbac::createContext(connection.getUsername(), bucketname);
            blob += bucketname + " ";
        } catch (const cb::rbac::Exception&) {
            // The client doesn't have access to this bucket
        }
    }

    if (blob.size() > 0) {
        /* remove trailing " " */
        blob.pop_back();
    }

    return std::make_pair(ENGINE_SUCCESS, blob);
}

void list_bucket_executor(McbpConnection* c, void*) {
    c->logCommand();
    std::pair<ENGINE_ERROR_CODE, std::string> ret;
    try {
        ret = list_bucket(*c);
    } catch (std::bad_alloc&) {
        ret.first = ENGINE_ENOMEM;
    }

    if (ret.first == ENGINE_SUCCESS) {
        if (mcbp_response_handler(nullptr,
                                  0,
                                  nullptr,
                                  0,
                                  ret.second.data(),
                                  uint32_t(ret.second.size()),
                                  PROTOCOL_BINARY_RAW_BYTES,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                  0,
                                  c->getCookie())) {
            c->logResponse(ret.first);
            mcbp_write_and_free(c, &c->getDynamicBuffer());
            return;
        }
        ret.first = ENGINE_ENOMEM;
    }
    ret.first = c->remapErrorCode(ret.first);
    c->logResponse(ret.first);

    if (ret.first == ENGINE_DISCONNECT) {
        c->setState(conn_closing);
    } else {
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret.first));
    }
}
