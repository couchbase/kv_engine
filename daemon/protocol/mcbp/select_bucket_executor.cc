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

#include <daemon/buckets.h>
#include <daemon/cookie.h>
#include <daemon/memcached.h>
#include <logger/logger.h>
#include <mcbp/protocol/request.h>
#include <utilities/engine_errc_2_mcbp.h>

cb::engine_errc select_bucket(Cookie& cookie, const std::string& bucketname) {
    auto& connection = cookie.getConnection();
    if (!connection.isAuthenticated()) {
        cookie.setErrorContext("Not authenticated");
        LOG_INFO(
                "{}: select_bucket failed - Not authenticated. "
                R"({{"cid":"{}/{:x}","connection":"{}","bucket":"{}"}})",
                connection.getId(),
                connection.getConnectionId().data(),
                ntohl(cookie.getRequest().getOpaque()),
                connection.getDescription(),
                bucketname);
        return cb::engine_errc::no_access;
    }

    if (connection.isDCP()) {
        cookie.setErrorContext("DCP connections cannot change bucket");
        LOG_INFO(
                "{}: select_bucket failed - DCP connection. "
                R"({{"cid":"{}/{:x}","connection":"{}","bucket":"{}"}})",
                connection.getId(),
                connection.getConnectionId().data(),
                ntohl(cookie.getRequest().getOpaque()),
                connection.getDescription(),
                bucketname);
        return cb::engine_errc::not_supported;
    }

    auto oldIndex = connection.getBucketIndex();

    if (!mayAccessBucket(cookie, bucketname)) {
        LOG_INFO(
                "{}: select_bucket failed - No access. "
                R"({{"cid":"{}/{:x}","connection":"{}","bucket":"{}"}})",
                connection.getId(),
                connection.getConnectionId().data(),
                ntohl(cookie.getRequest().getOpaque()),
                connection.getDescription(),
                bucketname);
        return cb::engine_errc::no_access;
    }

    if (associate_bucket(cookie, bucketname.c_str())) {
        // We found the bucket, great. Test to see if it is valid for the
        // given connection
        if (connection.isCollectionsSupported() &&
            !connection.getBucket().supports(
                    cb::engine::Feature::Collections)) {
            // It wasn't valid, try to jump back to the bucket we used to be
            // associated with..
            if (oldIndex != connection.getBucketIndex()) {
                associate_bucket(cookie, all_buckets[oldIndex].name);
            }
            cookie.setErrorContext(
                    "Destination bucket does not support collections");
            return cb::engine_errc::not_supported;
        }

        return cb::engine_errc::success;
    }

    if (oldIndex != connection.getBucketIndex()) {
        // try to jump back to the bucket we used to be associated
        // with..
        associate_bucket(cookie, all_buckets[oldIndex].name);
    }
    return cb::engine_errc::no_such_key;
}

void select_bucket_executor(Cookie& cookie) {
    const auto start = std::chrono::steady_clock::now();

    const auto key = cookie.getRequest().getKey();
    // Unfortunately we need to copy it over to a std::string as the
    // internal methods expects the string to be terminated with '\0'
    const std::string bucketname{reinterpret_cast<const char*>(key.data()),
                                 key.size()};

    auto& connection = cookie.getConnection();
    cookie.logCommand();

    cb::engine_errc code = cb::engine_errc::success;

    // We can't switch bucket if we've got multiple commands in flight
    if (connection.getNumberOfCookies() > 1) {
        LOG_INFO(
                "{}: select_bucket failed - multiple commands in flight. "
                R"({{"cid":"{}/{:x}","connection":"{}","bucket":"{}"}})",
                connection.getId(),
                connection.getConnectionId().data(),
                ntohl(cookie.getRequest().getOpaque()),
                connection.getDescription(),
                bucketname);
        code = cb::engine_errc::not_supported;
    } else if (bucketname == "@no bucket@") {
        // unselect bucket!
        associate_bucket(cookie, "");
    } else {
        code = select_bucket(cookie, bucketname);
    }

    handle_executor_status(cookie, code);
    cookie.getTracer().record(cb::tracing::Code::SelectBucket,
                              start,
                              std::chrono::steady_clock::now());
}
