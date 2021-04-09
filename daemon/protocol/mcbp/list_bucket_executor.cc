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
#include <utilities/engine_errc_2_mcbp.h>

std::pair<cb::engine_errc, std::string> list_bucket(Connection& connection) {
    if (!connection.isAuthenticated()) {
        return std::make_pair(cb::engine_errc::no_access, "");
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
            cb::rbac::createContext(connection.getUser(), bucketname);
            blob += bucketname + " ";
        } catch (const cb::rbac::Exception&) {
            // The client doesn't have access to this bucket
        }
    }

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
