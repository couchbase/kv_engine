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

#include "engine_errc_2_mcbp.h"
#include "executors.h"

#include <daemon/buckets.h>
#include <daemon/cookie.h>
#include <daemon/memcached.h>

std::pair<ENGINE_ERROR_CODE, std::string> list_bucket(Connection& connection) {
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

        {
            std::lock_guard<std::mutex> guard(bucket.mutex);
            if (bucket.state == Bucket::State::Ready) {
                bucketname = bucket.name;
            }
        }

        if (bucketname.empty()) {
            // ignore this one
            continue;
        }

        try {
            // Check if the user have access to the bucket
            cb::rbac::createContext(connection.getUsername(),
                                    connection.getDomain(),
                                    bucketname);
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

void list_bucket_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    cookie.logCommand();
    std::pair<ENGINE_ERROR_CODE, std::string> ret;
    try {
        ret = list_bucket(connection);
    } catch (const std::bad_alloc&) {
        ret.first = ENGINE_ENOMEM;
    }

    ret.first = connection.remapErrorCode(ret.first);
    cookie.logResponse(ret.first);

    if (ret.first == ENGINE_SUCCESS) {
        cookie.sendResponse(cb::mcbp::Status::Success,
                            {},
                            {},
                            {ret.second.data(), ret.second.size()},
                            cb::mcbp::Datatype::Raw,
                            0);
    } else if (ret.first == ENGINE_DISCONNECT) {
        connection.shutdown();
    } else {
        cookie.sendResponse(cb::mcbp::to_status(cb::engine_errc(ret.first)));
    }
}
