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
 * Check to see if the provided UserIdent have the provided privilege in
 * the context of the provided bucket
 *
 * @param ui The user to check
 * @param bucket The bucket to check
 * @param requiredPrivilege The privilege to check
 * @return true if the provided user should have access
 */
static bool check_access(Connection& connection,
                         const cb::rbac::UserIdent& ui,
                         const Bucket& bucket,
                         cb::rbac::Privilege requiredPrivilege) {
    cb::rbac::PrivilegeContext context{ui.domain};
    if (ui == connection.getUser()) {
        context = connection.createContext(bucket.name);
    } else {
        context = createContext(ui, std::string{bucket.name});
    }
    if (context.check(requiredPrivilege).failed()) {
        return false;
    }
    return true;
}

/**
 * Get the timing histogram for the specified bucket if we've got access
 * to the bucket.
 *
 * @param Cookie The command cookie
 * @param bucket The bucket to get the timing data from
 * @param opcode The opcode to get the timing histogram for
 * @return the histogram data if the caller have access
 */
static std::optional<Hdr1sfMicroSecHistogram> get_timings(Cookie& cookie,
                                                          const Bucket& bucket,
                                                          uint8_t opcode) {
    auto& connection = cookie.getConnection();
    // Need SimpleStats for normal buckets, but Stats for the
    // no-bucket as that tracks timings for all operations performed
    // not against a specific bucket.
    const auto requiredPrivilege = (bucket.type == BucketType::NoBucket)
                                           ? cb::rbac::Privilege::Stats
                                           : cb::rbac::Privilege::SimpleStats;

    // Don't create a new privilege context if the one we've got is for the
    // connected bucket:
    if (bucket.name == connection.getBucket().name) {
        if (cookie.testPrivilege(requiredPrivilege).failed()) {
            return {};
        }
    } else {
        // Check to see if we've got access to the bucket
        try {
            if (!check_access(connection,
                              connection.getUser(),
                              bucket,
                              requiredPrivilege)) {
                // The user don't have access
                return {};
            }

            // If we have an effective user (and we didn't inherit the simple
            // stat privilege) we need to create a privilege context and
            // check for the priv
            if (cookie.getEffectiveUser() &&
                !cookie.hasImposedUserExtraPrivilege(requiredPrivilege) &&
                !check_access(connection,
                              *cookie.getEffectiveUser(),
                              bucket,
                              requiredPrivilege)) {
                return {};
            }
        } catch (const cb::rbac::Exception&) {
            // We don't have access to that bucket
            return {};
        }
    }

    auto* histo = bucket.timings.get_timing_histogram(opcode);
    if (histo) {
        return {*histo};
    }
    return {Hdr1sfMicroSecHistogram{}};
}

/**
 * Get the aggregated timings across "all" buckets that the connected
 * client has access to.
 */
static std::pair<cb::engine_errc, std::string> get_aggregated_timings(
        Cookie& cookie, uint8_t opcode) {
    Hdr1sfMicroSecHistogram timings;
    bool found = false;

    BucketManager::instance().forEach(
            [&cookie, opcode, &timings, &found](auto& bucket) {
                if (bucket.type == BucketType::NoBucket &&
                    cookie.testPrivilege(cb::rbac::Privilege::Stats).failed()) {
                    return true;
                }

                auto histogram = get_timings(cookie, bucket, opcode);
                if (histogram.has_value()) {
                    timings += *histogram;
                    found = true;
                }
                return true;
            });

    if (found) {
        return {cb::engine_errc::success, timings.to_string()};
    }

    // We didn't have access to any buckets!
    return {cb::engine_errc::no_access, {}};
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
        // Use check privilege to ensure that it get logged if we don't
        // have access.
        if (cookie.checkPrivilege(cb::rbac::Privilege::Stats).success()) {
            auto* histo = BucketManager::instance()
                                  .getNoBucket()
                                  .timings.get_timing_histogram(opcode);
            if (histo) {
                return {cb::engine_errc::success, histo->to_string()};
            }
            return {cb::engine_errc::success, {}};
        }
        return {cb::engine_errc::no_access, {}};
    }

    if (bucket.empty() || bucket == BucketManager::instance().at(index).name) {
        // Use checkPrivilege to ensure that it gets logged.
        if (cookie.checkPrivilege(cb::rbac::Privilege::SimpleStats).failed()) {
            return {cb::engine_errc::no_access, {}};
        }

        // The current selected bucket
        auto& connection = cookie.getConnection();
        auto* histo =
                connection.getBucket().timings.get_timing_histogram(opcode);
        if (histo) {
            return {cb::engine_errc::success, histo->to_string()};
        }
        return {cb::engine_errc::success, {}};
    }

    // We removed support for getting command timings for not the current bucket
    // as it was broken and unused
    return {cb::engine_errc::not_supported, {}};
}

void get_cmd_timer_executor(Cookie& cookie) {
    std::pair<cb::engine_errc, std::string> ret;
    try {
        ret = get_cmd_timer(cookie);
    } catch (const std::bad_alloc&) {
        ret.first = cb::engine_errc::no_memory;
    }

    if (ret.first == cb::engine_errc::success) {
        auto value = std::move(ret.second);
        if (value.empty()) {
            // histogram for this opcode hasn't been created yet so just
            // return a histogram with no data in it
            Hdr1sfMicroSecHistogram h;
            value = h.to_string();
        }
        cookie.sendResponse(cb::mcbp::Status::Success,
                            {},
                            {},
                            value,
                            cb::mcbp::Datatype::JSON,
                            0);
    } else {
        handle_executor_status(cookie, ret.first);
    }
}
