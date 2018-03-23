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

#include <daemon/mcbp.h>
#include <daemon/buckets.h>

/**
 * Get the timing histogram for the specified bucket if we've got access
 * to the bucket.
 *
 * @param connection The connection executing the command
 * @param bucket The bucket to get the timing data from
 * @param opcode The opcode to get the timing histogram for
 * @return A std::pair with the first being the error code for the operation
 *         and the second being the histogram (only valid if the first
 *         parameter is ENGINE_SUCCESS)
 */
static std::pair<ENGINE_ERROR_CODE, TimingHistogram> get_timings(
        McbpConnection& connection, const Bucket& bucket, uint8_t opcode) {
    // Don't creata a new privilege context if the one we've got is for the
    // connected bucket:
    if (bucket.name == connection.getBucket().name) {
        auto ret = mcbp::checkPrivilege(connection,
                                        cb::rbac::Privilege::SimpleStats);
        if (ret != ENGINE_SUCCESS) {
            return std::make_pair(ENGINE_EACCESS, TimingHistogram{});
        }
    } else {
        // Check to see if we've got access to the bucket
        bool access = false;
        try {
            auto context = cb::rbac::createContext(connection.getUsername(),
                                                   bucket.name);
            const auto check = context.check(cb::rbac::Privilege::SimpleStats);
            if (check == cb::rbac::PrivilegeAccess::Ok) {
                access = true;
            }
        } catch (const cb::rbac::Exception& e) {
            // We don't have access to that bucket
        }

        if (!access) {
            return std::make_pair(ENGINE_EACCESS, TimingHistogram{});
        }
    }

    return std::make_pair(ENGINE_SUCCESS,
                          bucket.timings.get_timing_histogram(opcode));
}

/**
 * Get the command timings for the provided bucket if:
 *    it's not NoBucket
 *    it is running
 *    it has the given name
 *
 * @param connection The connection requesting access
 * @param bucket The bucket to look at
 * @param opcode The opcode we're interested in
 * @param bucketname The name of the bucket we want
 */
static std::pair<ENGINE_ERROR_CODE, TimingHistogram> maybe_get_timings(
    McbpConnection& connection, const Bucket& bucket, uint8_t opcode, const std::string& bucketname) {

    std::pair<ENGINE_ERROR_CODE, TimingHistogram> ret = std::make_pair(ENGINE_KEY_ENOENT, TimingHistogram{});

    cb_mutex_enter(&bucket.mutex);
    try {
        if (bucket.type != BucketType::NoBucket &&
            bucket.state == BucketState::Ready && bucketname == bucket.name) {
            ret = get_timings(connection, bucket, opcode);
        }
    } catch (...) {
        // we don't want to leave the mutex locked
    }
    cb_mutex_exit(&bucket.mutex);

    return ret;
}

/**
 * Get the aggregated timings across "all" buckets that the connected
 * client has access to.
 */
static std::pair<ENGINE_ERROR_CODE, std::string> get_aggregated_timings(
        McbpConnection& connection, uint8_t opcode) {
    TimingHistogram timings;
    bool found = false;

    for (auto& bucket : all_buckets) {
        auto bt = maybe_get_timings(connection, bucket, opcode, bucket.name);
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

std::pair<ENGINE_ERROR_CODE, std::string> get_cmd_timer(
        McbpConnection& connection,
        const protocol_binary_request_get_cmd_timer* req) {
    const char* key = (const char*)(req->bytes + sizeof(req->bytes));
    size_t keylen = ntohs(req->message.header.request.keylen);
    int index = connection.getBucketIndex();
    const std::string bucket(key, keylen);
    const auto opcode = req->message.body.opcode;

    if (bucket == "/all/") {
        index = 0;
    }

    if (index == 0) {
        return get_aggregated_timings(connection, opcode);
    }

    if (bucket.empty() || bucket == all_buckets[index].name) {
        // The current selected bucket
        auto bt = get_timings(connection, connection.getBucket(), opcode);
        if (bt.first == ENGINE_SUCCESS) {
            return std::make_pair(ENGINE_SUCCESS, bt.second.to_string());
        }

        return std::make_pair(bt.first, std::string{});
    }

    // The user specified a bucket... let's locate the bucket
    std::pair<ENGINE_ERROR_CODE, TimingHistogram> ret;

    for (auto& b : all_buckets) {
        ret = maybe_get_timings(connection, b, opcode, bucket);
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

void get_cmd_timer_executor(McbpConnection* c, void* packet) {
    c->logCommand();
    std::pair<ENGINE_ERROR_CODE, std::string> ret;
    try {
        ret = get_cmd_timer(
                *c,
                reinterpret_cast<protocol_binary_request_get_cmd_timer*>(
                        packet));
    } catch (const std::bad_alloc&) {
        ret.first = ENGINE_ENOMEM;
    }

    if (ret.first == ENGINE_SUCCESS) {
        if (mcbp_response_handler(nullptr,
                                  0, // no key
                                  nullptr,
                                  0, // no extras
                                  ret.second.data(),
                                  uint32_t(ret.second.size()),
                                  PROTOCOL_BINARY_RAW_BYTES,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                  0,
                                  c->getCookie())) {
            c->logResponse(ret.first);
            mcbp_write_and_free(c, &c->getDynamicBuffer());
            return;
        }
        ret.first = ENGINE_ENOMEM;
    }

    ret.first = c->remapErrorCode(ret.first);
    c->logResponse(ret.first);

    if (ret.first == ENGINE_DISCONNECT) {
        c->setState(conn_closing);
    } else {
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret.first));
    }
}
