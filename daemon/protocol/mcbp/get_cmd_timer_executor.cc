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
#include "utilities.h"

#include <daemon/buckets.h>
#include <daemon/mcbp.h>
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
 *         parameter is ENGINE_SUCCESS)
 */
static std::pair<ENGINE_ERROR_CODE, Hdr1sfMicroSecHistogram> get_timings(
        Cookie& cookie, const Bucket& bucket, uint8_t opcode) {
    // Don't creata a new privilege context if the one we've got is for the
    // connected bucket:
    auto& connection = cookie.getConnection();
    if (bucket.name == connection.getBucket().name) {
        auto ret = mcbp::checkPrivilege(cookie,
                                        cb::rbac::Privilege::SimpleStats);
        if (ret != ENGINE_SUCCESS) {
            return {ENGINE_EACCESS, {}};
        }
    } else {
        // Check to see if we've got access to the bucket
        bool access = false;
        try {
            auto context = cb::rbac::createContext(connection.getUsername(),
                                                   connection.getDomain(),
                                                   bucket.name);
            const auto check = context.check(cb::rbac::Privilege::SimpleStats);
            if (check == cb::rbac::PrivilegeAccess::Ok) {
                access = true;
            }
        } catch (const cb::rbac::Exception&) {
            // We don't have access to that bucket
        }

        if (!access) {
            return {ENGINE_EACCESS, {}};
        }
    }

    auto* histo = bucket.timings.get_timing_histogram(opcode);
    if (histo) {
        return {ENGINE_SUCCESS, *histo};
    } else {
        // histogram for this opcode hasn't been created yet so just
        // return an histogram with no data in it
        return {ENGINE_SUCCESS, {}};
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
static std::pair<ENGINE_ERROR_CODE, Hdr1sfMicroSecHistogram> maybe_get_timings(
        Cookie& cookie,
        const Bucket& bucket,
        uint8_t opcode,
        const std::string& bucketname) {
    std::lock_guard<std::mutex> guard(bucket.mutex);
    if (bucket.type != BucketType::NoBucket &&
        bucket.state == Bucket::State::Ready && bucketname == bucket.name) {
        return get_timings(cookie, bucket, opcode);
    } else {
        return {ENGINE_KEY_ENOENT, {}};
    }
}

/**
 * Get the aggregated timings across "all" buckets that the connected
 * client has access to.
 */
static std::pair<ENGINE_ERROR_CODE, std::string> get_aggregated_timings(
        Cookie& cookie, uint8_t opcode) {
    Hdr1sfMicroSecHistogram timings;
    bool found = false;

    for (auto& bucket : all_buckets) {
        auto bt = maybe_get_timings(cookie, bucket, opcode, bucket.name);
        if (bt.first == ENGINE_SUCCESS) {
            timings += bt.second;
            found = true;
        }
    }

    if (found) {
        return std::make_pair(ENGINE_SUCCESS, timings.to_string());
    }

    // We didn't have access to any buckets!
    return std::make_pair(ENGINE_EACCESS, std::string{});
}

std::pair<ENGINE_ERROR_CODE, std::string> get_cmd_timer(Cookie& cookie) {
    const auto& request = cookie.getRequest(Cookie::PacketContent::Full);
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
        if (bt.first == ENGINE_SUCCESS) {
            return std::make_pair(ENGINE_SUCCESS, bt.second.to_string());
        }

        return std::make_pair(bt.first, std::string{});
    }

    // The user specified a bucket... let's locate the bucket
    std::pair<ENGINE_ERROR_CODE, Hdr1sfMicroSecHistogram> ret;

    for (auto& b : all_buckets) {
        ret = maybe_get_timings(cookie, b, opcode, bucket);
        if (ret.first != ENGINE_KEY_ENOENT && ret.first != ENGINE_SUCCESS) {
            break;
        }
    }

    if (ret.first == ENGINE_SUCCESS) {
        return std::make_pair(ENGINE_SUCCESS, ret.second.to_string());
    }

    if (ret.first == ENGINE_KEY_ENOENT) {
        // Don't tell the user that the bucket doesn't exist
        ret.first = ENGINE_EACCESS;
    }

    return std::make_pair(ret.first, std::string{});
}

void get_cmd_timer_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    cookie.logCommand();
    std::pair<ENGINE_ERROR_CODE, std::string> ret;
    try {
        ret = get_cmd_timer(cookie);
    } catch (const std::bad_alloc&) {
        ret.first = ENGINE_ENOMEM;
    }

    auto remapErr = connection.remapErrorCode(ret.first);
    cookie.logResponse(remapErr);

    if (remapErr == ENGINE_DISCONNECT) {
        if (ret.first == ENGINE_DISCONNECT) {
            LOG_WARNING(
                    "{}: get_cmd_timer_executor - get_cmd_timer returned "
                    "ENGINE_DISCONNECT - closing connection {}",
                    connection.getId(),
                    connection.getDescription());
        }
        connection.setState(StateMachine::State::closing);
        return;
    }

    if (remapErr == ENGINE_SUCCESS) {
        cookie.sendResponse(cb::mcbp::Status::Success,
                            {},
                            {},
                            {ret.second.data(), ret.second.size()},
                            cb::mcbp::Datatype::JSON,
                            0);
    } else {
        cookie.sendResponse(cb::engine_errc(remapErr));
    }
}
