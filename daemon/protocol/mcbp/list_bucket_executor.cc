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

#include <daemon/memcached.h>
#include <daemon/mcbp.h>

void list_bucket_executor(McbpConnection* c, void*) {
    // @todo We're currently allowing every user to run list buckets
    //       in the global privilege check, but we have to filter out
    //       the buckets people may use or not
    static bool testing = getenv("MEMCACHED_UNIT_TESTS") != nullptr;
    if (c->isInternal() || testing) {
        try {
            std::string blob;
            for (auto& bucket : all_buckets) {
                cb_mutex_enter(&bucket.mutex);
                if (bucket.state == BucketState::Ready) {
                    blob += bucket.name + std::string(" ");
                }
                cb_mutex_exit(&bucket.mutex);
            }

            if (blob.size() > 0) {
                /* remove trailing " " */
                blob.pop_back();
            }

            if (mcbp_response_handler(NULL, 0, NULL, 0, blob.data(),
                                      uint32_t(blob.size()),
                                      PROTOCOL_BINARY_RAW_BYTES,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS, 0,
                                      c->getCookie())) {
                mcbp_write_and_free(c, &c->getDynamicBuffer());
            } else {
                mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
            }
        } catch (const std::bad_alloc&) {
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
        }
    } else {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EACCESS);
    }
}
