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

void select_bucket_executor(McbpConnection* c, void* packet) {
    // @todo We're currently allowing every user to run select bucket
    //       in the global privilege check, but we have to filter out
    //       the buckets people may use or not
    static bool testing = getenv("MEMCACHED_UNIT_TESTS") != nullptr;
    if (c->isInternal() || testing) {
        /* The validator ensured that we're not doing a buffer overflow */
        char bucketname[1024];
        auto* req = reinterpret_cast<protocol_binary_request_no_extras*>(packet);
        uint16_t klen = ntohs(req->message.header.request.keylen);
        memcpy(bucketname, req->bytes + (sizeof(*req)), klen);
        bucketname[klen] = '\0';

        if (associate_bucket(c, bucketname)) {
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
        } else {
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
        }
    } else {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EACCESS);
    }
}
