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

#include <daemon/bucket_manager.h>
#include <daemon/buckets.h>
#include <daemon/cookie.h>
#include <daemon/memcached.h>
#include <utilities/engine_errc_2_mcbp.h>

std::pair<cb::engine_errc, std::string> list_bucket(Connection& connection) {
    if (!connection.isAuthenticated()) {
        return std::make_pair(cb::engine_errc::no_access, "");
    }

    std::string blob;
    // The blob string will contain all of the buckets, and to
    // avoid too many reallocations we should probably just reserve
    // a chunk
    blob.reserve(100);
    BucketManager::instance().forEach([&connection, &blob](auto& bucket) {
        if (bucket.type != BucketType::NoBucket &&
            connection.mayAccessBucket(bucket.name)) {
            blob.append(bucket.name);
            blob.push_back(' ');
        }
        return true;
    });

    if (!blob.empty()) {
        /* remove trailing " " */
        blob.pop_back();
    }

    return std::make_pair(cb::engine_errc::success, blob);
}

void list_bucket_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    std::pair<cb::engine_errc, std::string> ret;
    try {
        ret = list_bucket(connection);
        if (ret.first == cb::engine_errc::success) {
            cookie.sendResponse(cb::mcbp::Status::Success,
                                {},
                                {},
                                {ret.second.data(), ret.second.size()},
                                cb::mcbp::Datatype::Raw,
                                0);
            return;
        }
    } catch (const std::bad_alloc&) {
        ret.first = cb::engine_errc::no_memory;
    }

    handle_executor_status(cookie, ret.first);
}
