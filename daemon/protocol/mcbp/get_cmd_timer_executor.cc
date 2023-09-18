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
#include <hdrhistogram/hdrhistogram.h>

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
    auto& connection = cookie.getConnection();
    // Need SimpleStats for normal buckets, but Stats for the
    // no-bucket as that tracks timings for all operations performed
    // not against a specific bucket.
    const auto requiredPrivilege = (bucket.type == BucketType::NoBucket)
                                           ? cb::rbac::Privilege::Stats
                                           : cb::rbac::Privilege::SimpleStats;
    bool access = false;
    // Don't create a new privilege context if the one we've got is for the
    // connected bucket:
    if (bucket.name == connection.getBucket().name) {
        auto ret = mcbp::checkPrivilege(cookie, requiredPrivilege);
        access = (ret == cb::engine_errc::success);
    } else {
        // Check to see if we've got access to the bucket
        try {
            auto context =
                    cb::rbac::createContext(connection.getUser(), bucket.name);
            access = context.check(requiredPrivilege, {}, {}).success();
        } catch (const cb::rbac::Exception&) {
            // We don't have access to that bucket
        }
    }
    if (!access) {
        return {cb::engine_errc::no_access, {}};
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
        std::string_view bucketname) {
    std::lock_guard<std::mutex> guard(bucket.mutex);
    if (bucket.state == Bucket::State::Ready && bucketname == bucket.name) {
        return get_timings(cookie, bucket, opcode);
    }
    return {cb::engine_errc::no_such_key, {}};
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
    const auto bucket = request.getKeyString();
    const auto extras = request.getExtdata();
    const auto opcode = extras[0];

    // Determine which bucket the user wants to lookup stats for.
    // * If the user doesn't specify a bucket name in the command key, they get
    //   the bucket they are currently associated with (or the no-bucket if
    //   they haven't selected one yet).
    // * If the user specifies the name "/all/", they get aggregated stats
    //   across all buckets they have access to, including the no-bucket.
    // * If the user specifies the name of the currently associated bucket,
    //   they get that specific bucket.
    // * If the user specifies the name "@no bucket@", they get the no-bucket.
    // * Otherwise, the user specified the name of a different bucket, which is
    //   not supported.

    if (bucket == "/all/") {
        return get_aggregated_timings(cookie, opcode);
    }

    // Lookup which bucket they are associated with (which could be none -
    // index 0).
    const auto index = cookie.getConnection().getBucketIndex();

    // Requesting stats from the no-bucket is restricted to those who have
    // the global stats privilege as it contains information about non-bucket
    // level opcodes.
    if (index == 0 || bucket == "@no bucket@") {
        if (cookie.testPrivilege(cb::rbac::Privilege::Stats) ==
            cb::rbac::PrivilegeAccessOk) {
            std::lock_guard<std::mutex> guard(all_buckets[0].mutex);
            auto* histo = all_buckets[0].timings.get_timing_histogram(opcode);
            if (histo) {
                return {cb::engine_errc::success, histo->to_string()};
            }

            // histogram for this opcode hasn't been created yet so just
            // return an empty histogram
            Hdr1sfMicroSecHistogram h;
            return {cb::engine_errc::success, h.to_string()};
        }

        return {cb::engine_errc::no_access, {}};
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

    // We removed support for getting command timings for not the current bucket
    // as it was broken and unused
    return std::make_pair(cb::engine_errc::not_supported, std::string{});
}

void get_cmd_timer_executor(Cookie& cookie) {
    std::pair<cb::engine_errc, std::string> ret;
    try {
        ret = get_cmd_timer(cookie);
    } catch (const std::bad_alloc&) {
        ret.first = cb::engine_errc::no_memory;
    }

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
