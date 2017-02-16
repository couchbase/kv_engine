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

void list_bucket_executor(McbpConnection* c, void*) {
    if (!c->isAuthenticated()) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EACCESS);
        return;
    }

    // The list bucket command is a bit racy (as it other threads may
    // create/delete/remove access to buckets while we check them), but
    // we don't care about that (we don't want to hold a lock for the
    // entire period, _AND_ the access could be changed right after we
    // built the response anyway.
    try {
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

            try {
                // Check if the user have access to the bucket
                cb::rbac::createContext(c->getUsername(), bucketname);
                blob += bucketname + " ";
            } catch (const cb::rbac::Exception&){
                // The client doesn't have access to this bucket
            }
        }

        if (blob.size() > 0) {
            /* remove trailing " " */
            blob.pop_back();
        }

        if (mcbp_response_handler(NULL, 0, NULL, 0, blob.data(),
                                  uint32_t(blob.size()),
                                  PROTOCOL_BINARY_RAW_BYTES,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS, 0,
                                  c->getCookie())) {
            mcbp_write_and_free(c, &c->getDynamicBuffer());
        } else {
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
        }
    } catch (const std::bad_alloc&) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
    }
}
