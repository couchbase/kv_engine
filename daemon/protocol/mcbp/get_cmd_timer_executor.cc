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

std::pair<ENGINE_ERROR_CODE, std::string> get_cmd_timer(
        McbpConnection& connection,
        const protocol_binary_request_get_cmd_timer* req) {
    const char* key = (const char*)(req->bytes + sizeof(req->bytes));
    size_t keylen = ntohs(req->message.header.request.keylen);
    int index = connection.getBucketIndex();
    const std::string bucket(key, keylen);
    const auto opcode = req->message.body.opcode;

    if (keylen > 0 && bucket != all_buckets[index].name) {
        // The user specified the current selected bucket
        keylen = 0;
    }

    if (keylen > 0 || index == 0) {
        // You need the Stats privilege in order to specify a bucket
        auto ret = mcbp::checkPrivilege(connection.getCookieObject(),
                                        cb::rbac::Privilege::Stats);
        if (ret != ENGINE_SUCCESS) {
            return std::make_pair(ret, "");
        }
    }

    // At this point we know that the user have the appropriate access
    // and should be permitted to perform the action
    if (bucket == "/all/") {
        // The aggregated timings is stored in index 0 (no bucket)
        index = 0;
        keylen = 0;
    }

    if (keylen == 0) {
        return std::make_pair(ENGINE_SUCCESS,
                              all_buckets[index].timings.generate(opcode));
    }

    // The user specified a bucket... let's locate the bucket
    std::string str;

    bool found = false;
    for (size_t ii = 1; ii < all_buckets.size() && !found; ++ii) {
        // Need the lock to get the bucket state and name
        std::lock_guard<std::mutex> guard(all_buckets[ii].mutex);
        if ((all_buckets[ii].state == BucketState::Ready) &&
            (bucket == all_buckets[ii].name)) {
            str = all_buckets[ii].timings.generate(opcode);
            found = true;
        }
    }

    if (found) {
        return std::make_pair(ENGINE_SUCCESS, str);
    }

    return std::make_pair(ENGINE_KEY_ENOENT, "");
}

void get_cmd_timer_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    cookie.logCommand();
    std::pair<ENGINE_ERROR_CODE, std::string> ret;
    try {
        ret = get_cmd_timer(
                connection,
                reinterpret_cast<protocol_binary_request_get_cmd_timer*>(
                        cookie.getPacketAsVoidPtr()));
    } catch (const std::bad_alloc&) {
        ret.first = ENGINE_ENOMEM;
    }

    ret.first = connection.remapErrorCode(ret.first);
    cookie.logResponse(ret.first);

    if (ret.first == ENGINE_DISCONNECT) {
        connection.setState(McbpStateMachine::State::closing);
        return;
    }

    if (ret.first == ENGINE_SUCCESS) {
        cookie.sendResponse(cb::mcbp::Status::Success,
                            {},
                            {},
                            {ret.second.data(), ret.second.size()},
                            cb::mcbp::Datatype::JSON,
                            0);
    } else {
        cookie.sendResponse(cb::engine_errc(ret.first));
    }
}
