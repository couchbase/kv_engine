/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "executors.h"

#include <daemon/buckets.h>
#include <daemon/cookie.h>
#include <daemon/memcached.h>
#include <daemon/settings.h>
#include <logger/logger.h>
#include <mcbp/protocol/request.h>
#include <platform/scope_timer.h>
#include <serverless/config.h>
#include <utilities/engine_errc_2_mcbp.h>

cb::engine_errc select_bucket(Cookie& cookie, const std::string& bucketname) {
    auto& connection = cookie.getConnection();
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

        if (isServerlessDeployment() && !connection.isInternal()) {
            using cb::serverless::Config;
            if (connection.getBucket().clients >=
                Config::instance().maxConnectionsPerBucket.load(
                        std::memory_order_acquire)) {
                if (oldIndex != connection.getBucketIndex()) {
                    associate_bucket(cookie, all_buckets[oldIndex].name);
                }
                cookie.setErrorContext("Too many bucket connections");
                return cb::engine_errc::too_many_connections;
            }
        }

        connection.setPushedClustermapRevno({});
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
    using cb::tracing::Code;
    using cb::tracing::SpanStopwatch;
    ScopeTimer<SpanStopwatch> timer(
            std::forward_as_tuple(cookie, Code::SelectBucket));

    const auto key = cookie.getRequest().getKey();
    // Unfortunately we need to copy it over to a std::string as the
    // internal methods expects the string to be terminated with '\0'
    const std::string bucketname{reinterpret_cast<const char*>(key.data()),
                                 key.size()};

    cb::engine_errc code = cb::engine_errc::success;
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
        code = cb::engine_errc::no_access;
    } else if (connection.isDCP()) {
        cookie.setErrorContext("DCP connections cannot change bucket");
        LOG_INFO(
                "{}: select_bucket failed - DCP connection. "
                R"({{"cid":"{}/{:x}","connection":"{}","bucket":"{}"}})",
                connection.getId(),
                connection.getConnectionId().data(),
                ntohl(cookie.getRequest().getOpaque()),
                connection.getDescription(),
                bucketname);
        code = cb::engine_errc::not_supported;
    } else if (connection.getNumberOfCookies() > 1) {
        // We can't switch bucket if we've got multiple commands in flight
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
}
