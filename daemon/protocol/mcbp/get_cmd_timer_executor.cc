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

#include <daemon/mcbp.h>
#include <daemon/buckets.h>

void get_cmd_timer_executor(McbpConnection* c, void* packet) {
    std::string str;
    auto* req = reinterpret_cast<protocol_binary_request_get_cmd_timer*>(packet);
    const char* key = (const char*)(req->bytes + sizeof(req->bytes));
    size_t keylen = ntohs(req->message.header.request.keylen);
    int index = c->getBucketIndex();
    std::string bucket(key, keylen);

    if (bucket == "/all/") {
        index = 0;
        keylen = 0;
    }

    if (keylen == 0) {
        if (index == 0 && !c->isInternal()) {
            // We're not connected to a bucket, and we didn't
            // authenticate to a bucket.. Don't leak the
            // global stats...
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EACCESS);
            return;
        }
        str = all_buckets[index].timings.generate(req->message.body.opcode);
        mcbp_response_handler(NULL, 0, NULL, 0, str.data(),
                              uint32_t(str.length()),
                              PROTOCOL_BINARY_RAW_BYTES,
                              PROTOCOL_BINARY_RESPONSE_SUCCESS,
                              0, c->getCookie());
        mcbp_write_and_free(c, &c->getDynamicBuffer());
    } else if (c->isInternal()) {
        bool found = false;
        for (size_t ii = 1; ii < all_buckets.size() && !found; ++ii) {
            // Need the lock to get the bucket state and name
            cb_mutex_enter(&all_buckets[ii].mutex);
            if ((all_buckets[ii].state == BucketState::Ready) &&
                (bucket == all_buckets[ii].name)) {
                str = all_buckets[ii].timings.generate(
                    req->message.body.opcode);
                found = true;
            }
            cb_mutex_exit(&all_buckets[ii].mutex);
        }
        if (found) {
            mcbp_response_handler(NULL, 0, NULL, 0, str.data(),
                                  uint32_t(str.length()),
                                  PROTOCOL_BINARY_RAW_BYTES,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                  0, c->getCookie());
            mcbp_write_and_free(c, &c->getDynamicBuffer());
        } else {
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
        }
    } else {
        // non-privileged connections can't specify bucket
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EACCESS);
    }
}
