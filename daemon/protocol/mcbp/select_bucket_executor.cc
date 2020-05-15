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

ENGINE_ERROR_CODE select_bucket(Cookie& cookie, const std::string& bucketname) {
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
        return ENGINE_EACCESS;
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
        return ENGINE_ENOTSUP;
    }

    auto oldIndex = connection.getBucketIndex();

    try {
        cb::rbac::createContext(connection.getUser(), bucketname);
        if (associate_bucket(connection, bucketname.c_str())) {
            // We found the bucket, great. Test to see if it is valid for the
            // given connection
            if (connection.isCollectionsSupported() &&
                !connection.getBucket().supports(
                        cb::engine::Feature::Collections)) {
                // It wasn't valid, try to jump back to the bucket we used to be
                // associated with..
                if (oldIndex != connection.getBucketIndex()) {
                    associate_bucket(connection, all_buckets[oldIndex].name);
                }
                cookie.setErrorContext(
                        "Destination bucket does not support collections");
                return ENGINE_ENOTSUP;
            }

            return ENGINE_SUCCESS;
        } else {
            if (oldIndex != connection.getBucketIndex()) {
                // try to jump back to the bucket we used to be associated
                // with..
                associate_bucket(connection, all_buckets[oldIndex].name);
            }
            return ENGINE_KEY_ENOENT;
        }
    } catch (const cb::rbac::Exception&) {
        LOG_INFO(
                "{}: select_bucket failed - No access. "
                R"({{"cid":"{}/{:x}","connection":"{}","bucket":"{}"}})",
                connection.getId(),
                connection.getConnectionId().data(),
                ntohl(cookie.getRequest().getOpaque()),
                connection.getDescription(),
                bucketname);
        return ENGINE_EACCESS;
    }
}

void select_bucket_executor(Cookie& cookie) {
    const auto key = cookie.getRequest().getKey();
    // Unfortunately we need to copy it over to a std::string as the
    // internal methods expects the string to be terminated with '\0'
    const std::string bucketname{reinterpret_cast<const char*>(key.data()),
                                 key.size()};

    auto& connection = cookie.getConnection();
    cookie.logCommand();

    ENGINE_ERROR_CODE code = ENGINE_SUCCESS;

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
        code = ENGINE_ENOTSUP;
    } else if (bucketname == "@no bucket@") {
        // unselect bucket!
        associate_bucket(connection, "");
    } else {
        code = select_bucket(cookie, bucketname);
    }

    handle_executor_status(cookie, code);
}
