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
#include "dcp_add_failover_log.h"

void dcp_get_failover_log_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_dcp_get_failover_log*>(packet);

    ENGINE_ERROR_CODE ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    if (ret == ENGINE_SUCCESS) {
        ret = c->getBucketEngine()->dcp.get_failover_log(
            c->getBucketEngineAsV0(), c->getCookie(),
            req->message.header.request.opaque,
            ntohs(req->message.header.request.vbucket),
            add_failover_log);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        if (c->getDynamicBuffer().getRoot() != nullptr) {
            mcbp_write_and_free(c, &c->getDynamicBuffer());
        } else {
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
        }
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