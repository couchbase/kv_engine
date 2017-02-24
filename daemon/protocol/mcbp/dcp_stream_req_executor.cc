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

void dcp_stream_req_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_dcp_stream_req*>(packet);

    uint32_t flags = ntohl(req->message.body.flags);
    uint64_t start_seqno = ntohll(req->message.body.start_seqno);
    uint64_t end_seqno = ntohll(req->message.body.end_seqno);
    uint64_t vbucket_uuid = ntohll(req->message.body.vbucket_uuid);
    uint64_t snap_start_seqno = ntohll(req->message.body.snap_start_seqno);
    uint64_t snap_end_seqno = ntohll(req->message.body.snap_end_seqno);
    uint64_t rollback_seqno;

    ENGINE_ERROR_CODE ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    if (ret == ENGINE_ROLLBACK) {
        LOG_WARNING(c,
                    "%u: dcp_stream_req_executor: Unexpected AIO stat"
                        " result ROLLBACK. Shutting down DCP connection",
                    c->getId());
        c->setState(conn_closing);
        return;
    }

    if (ret == ENGINE_SUCCESS) {
        ret = c->getBucketEngine()->dcp.stream_req(c->getBucketEngineAsV0(),
                                                   c->getCookie(),
                                                   flags,
                                                   c->binary_header.request.opaque,
                                                   c->binary_header.request.vbucket,
                                                   start_seqno, end_seqno,
                                                   vbucket_uuid,
                                                   snap_start_seqno,
                                                   snap_end_seqno,
                                                   &rollback_seqno,
                                                   add_failover_log);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        c->setDCP(true);
        c->setPriority(Connection::Priority::Medium);
        if (c->getDynamicBuffer().getRoot() != nullptr) {
            mcbp_write_and_free(c, &c->getDynamicBuffer());
        } else {
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
        }
        break;

    case ENGINE_ROLLBACK:
        rollback_seqno = htonll(rollback_seqno);
        if (mcbp_response_handler(NULL, 0, NULL, 0, &rollback_seqno,
                                  sizeof(rollback_seqno),
                                  PROTOCOL_BINARY_RAW_BYTES,
                                  PROTOCOL_BINARY_RESPONSE_ROLLBACK, 0,
                                  c->getCookie())) {
            mcbp_write_and_free(c, &c->getDynamicBuffer());
        } else {
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
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

