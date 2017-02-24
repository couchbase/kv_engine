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

#include <daemon/mcbp.h>
#include "executors.h"
#include "utilities.h"

void dcp_control_executor(McbpConnection* c, void* packet) {
    ENGINE_ERROR_CODE ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    if (ret == ENGINE_SUCCESS) {
        ret = mcbp::haveDcpPrivilege(*c);

        if (ret == ENGINE_SUCCESS) {
            auto* req = reinterpret_cast<protocol_binary_request_dcp_control*>(packet);
            const uint8_t* key = req->bytes + sizeof(req->bytes);
            uint16_t nkey = ntohs(req->message.header.request.keylen);
            const uint8_t* value = key + nkey;
            uint32_t nvalue = ntohl(req->message.header.request.bodylen) - nkey;
            ret = c->getBucketEngine()->dcp.control(c->getBucketEngineAsV0(),
                                                    c->getCookie(),
                                                    c->binary_header.request.opaque,
                                                    key, nkey, value, nvalue);
        }
    }

    ret = c->remapErrorCode(ret);
    switch (ret) {
    case ENGINE_SUCCESS:
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
        break;

    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;

    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        break;

    default:
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }
}
