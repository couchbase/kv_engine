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
    if (!c->isAuthenticated()) {
        // One have to authenticate to the server before trying to
        // select a bucket
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EACCESS);
        return;
    }

    auto* req = reinterpret_cast<protocol_binary_request_no_extras*>(packet);
    std::string bucketname{
        reinterpret_cast<const char*>(req->bytes + (sizeof(req->bytes))),
        ntohs(req->message.header.request.keylen)};

    auto oldIndex = c->getBucketIndex();

    try {
        cb::rbac::createContext(c->getUsername(), bucketname);
        if (associate_bucket(c, bucketname.c_str())) {
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
        } else {
            if (oldIndex != c->getBucketIndex()) {
                // try to jump back to the bucket we used to be associated
                // with..
                associate_bucket(c, all_buckets[oldIndex].name);
            }
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
        }
    } catch (const cb::rbac::Exception& error) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EACCESS);
    }
}
