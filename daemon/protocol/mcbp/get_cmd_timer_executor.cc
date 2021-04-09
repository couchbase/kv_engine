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
#include "utilities.h"

#include <daemon/buckets.h>
#include <logger/logger.h>
#include <mcbp/protocol/request.h>
#include <utilities/hdrhistogram.h>

/**
 * Get the timing histogram for the specified bucket if we've got access
 * to the bucket.
 *
 * @param Cookie The command cookie
 * @param bucket The bucket to get the timing data from
 * @param opcode The opcode to get the timing histogram for
 * @return A std::pair with the first being the error code for the operation
 *         and the second being the histogram (only valid if the first
 *         parameter is cb::engine_errc::success)
 */
static std::pair<cb::engine_errc, Hdr1sfMicroSecHistogram> get_timings(
        Cookie& cookie, const Bucket& bucket, uint8_t opcode) {
    // Don't creata a new privilege context if the one we've got is for the
    // connected bucket:
    auto& connection = cookie.getConnection();
    if (bucket.name == connection.getBucket().name) {
        auto ret = mcbp::checkPrivilege(cookie,
                                        cb::rbac::Privilege::SimpleStats);
        if (ret != cb::engine_errc::success) {
            return {cb::engine_errc::no_access, {}};
        }
    } else {
        // Check to see if we've got access to the bucket
        bool access = false;
        try {
            auto context =
                    cb::rbac::createContext(connection.getUser(), bucket.name);
            if (context.check(cb::rbac::Privilege::SimpleStats, {}, {})
                        .success()) {
                access = true;
            }
        } catch (const cb::rbac::Exception&) {
            // We don't have access to that bucket
        }

        if (!access) {
            return {cb::engine_errc::no_access, {}};
        }
    }

    auto* histo = bucket.timings.get_timing_histogram(opcode);
    if (histo) {
        return {cb::engine_errc::success, *histo};
    } else {
        // histogram for this opcode hasn't been created yet so just
        // return an histogram with no data in it
        return {cb::engine_errc::success, {}};
    }
}

/**
 * Get the command timings for the provided bucket if:
 *    it's not NoBucket
 *    it is running
 *    it has the given name
 *
 * @param cookie The command cookie
 * @param bucket The bucket to look at
 * @param opcode The opcode we're interested in
 * @param bucketname The name of the bucket we want
 */
static std::pair<cb::engine_errc, Hdr1sfMicroSecHistogram> maybe_get_timings(
        Cookie& cookie,
        const Bucket& bucket,
        uint8_t opcode,
        const std::string& bucketname) {
    std::lock_guard<std::mutex> guard(bucket.mutex);
    if (bucket.type != BucketType::NoBucket &&
        bucket.state == Bucket::State::Ready && bucketname == bucket.name) {
        return get_timings(cookie, bucket, opcode);
    } else {
        return {cb::engine_errc::no_such_key, {}};
    }
}

/**
 * Get the aggregated timings across "all" buckets that the connected
 * client has access to.
 */
static std::pair<cb::engine_errc, std::string> get_aggregated_timings(
        Cookie& cookie, uint8_t opcode) {
    Hdr1sfMicroSecHistogram timings;
    bool found = false;

    for (auto& bucket : all_buckets) {
        auto bt = maybe_get_timings(cookie, bucket, opcode, bucket.name);
        if (bt.first == cb::engine_errc::success) {
            timings += bt.second;
            found = true;
        }
    }

    if (found) {
        return std::make_pair(cb::engine_errc::success, timings.to_string());
    }

    // We didn't have access to any buckets!
    return std::make_pair(cb::engine_errc::no_access, std::string{});
}

std::pair<cb::engine_errc, std::string> get_cmd_timer(Cookie& cookie) {
    const auto& request = cookie.getRequest();
    const auto key = request.getKey();
    const std::string bucket(reinterpret_cast<const char*>(key.data()),
                             key.size());
    const auto extras = request.getExtdata();
    const auto opcode = extras[0];
    int index = cookie.getConnection().getBucketIndex();

    if (bucket == "/all/") {
        index = 0;
    }

    if (index == 0) {
        return get_aggregated_timings(cookie, opcode);
    }

    if (bucket.empty() || bucket == all_buckets[index].name) {
        // The current selected bucket
        auto& connection = cookie.getConnection();
        auto bt = get_timings(cookie, connection.getBucket(), opcode);
        if (bt.first == cb::engine_errc::success) {
            return std::make_pair(cb::engine_errc::success,
                                  bt.second.to_string());
        }

        return std::make_pair(bt.first, std::string{});
    }

    // The user specified a bucket... let's locate the bucket
    std::pair<cb::engine_errc, Hdr1sfMicroSecHistogram> ret;

    for (auto& b : all_buckets) {
        ret = maybe_get_timings(cookie, b, opcode, bucket);
        if (ret.first != cb::engine_errc::no_such_key &&
            ret.first != cb::engine_errc::success) {
            break;
        }
    }

    if (ret.first == cb::engine_errc::success) {
        return std::make_pair(cb::engine_errc::success, ret.second.to_string());
    }

    if (ret.first == cb::engine_errc::no_such_key) {
        // Don't tell the user that the bucket doesn't exist
        ret.first = cb::engine_errc::no_access;
    }

    return std::make_pair(ret.first, std::string{});
}

void get_cmd_timer_executor(Cookie& cookie) {
    cookie.logCommand();
    std::pair<cb::engine_errc, std::string> ret;
    try {
        ret = get_cmd_timer(cookie);
    } catch (const std::bad_alloc&) {
        ret.first = cb::engine_errc::no_memory;
    }
    cookie.logResponse(ret.first);

    if (ret.first == cb::engine_errc::success) {
        cookie.sendResponse(cb::mcbp::Status::Success,
                            {},
                            {},
                            {ret.second.data(), ret.second.size()},
                            cb::mcbp::Datatype::JSON,
                            0);
    } else {
        handle_executor_status(cookie, ret.first);
    }
}
