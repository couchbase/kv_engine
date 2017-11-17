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

void dcp_stream_req_executor(Cookie& cookie) {
    uint64_t rollback_seqno = 0;

    ENGINE_ERROR_CODE ret = cookie.getAiostat();
    cookie.setAiostat(ENGINE_SUCCESS);
    cookie.setEwouldblock(false);

    auto& connection = cookie.getConnection();
    if (ret == ENGINE_ROLLBACK) {
        LOG_WARNING(&connection,
                    "%u: dcp_stream_req_executor: Unexpected AIO stat"
                    " result ROLLBACK. Shutting down DCP connection",
                    connection.getId());
        connection.setState(McbpStateMachine::State::closing);
        return;
    }

    if (ret == ENGINE_SUCCESS) {
        const auto& request = cookie.getRequest(Cookie::PacketContent::Full);
        const auto* req =
                reinterpret_cast<const protocol_binary_request_dcp_stream_req*>(
                        &request);

        uint32_t flags = ntohl(req->message.body.flags);
        uint64_t start_seqno = ntohll(req->message.body.start_seqno);
        uint64_t end_seqno = ntohll(req->message.body.end_seqno);
        uint64_t vbucket_uuid = ntohll(req->message.body.vbucket_uuid);
        uint64_t snap_start_seqno = ntohll(req->message.body.snap_start_seqno);
        uint64_t snap_end_seqno = ntohll(req->message.body.snap_end_seqno);
        ret = connection.getBucketEngine()->dcp.stream_req(
                connection.getBucketEngineAsV0(),
                &cookie,
                flags,
                request.getOpaque(),
                request.getVBucket(),
                start_seqno,
                end_seqno,
                vbucket_uuid,
                snap_start_seqno,
                snap_end_seqno,
                &rollback_seqno,
                add_failover_log);
    }

    ret = connection.remapErrorCode(ret);
    switch (ret) {
    case ENGINE_SUCCESS:
        connection.setDCP(true);
        connection.setPriority(Connection::Priority::Medium);
        if (cookie.getDynamicBuffer().getRoot() != nullptr) {
            cookie.sendDynamicBuffer();
        } else {
            cookie.sendResponse(cb::mcbp::Status::Success);
        }
        break;

    case ENGINE_ROLLBACK:
        rollback_seqno = htonll(rollback_seqno);
        if (mcbp_response_handler(NULL,
                                  0,
                                  NULL,
                                  0,
                                  &rollback_seqno,
                                  sizeof(rollback_seqno),
                                  PROTOCOL_BINARY_RAW_BYTES,
                                  PROTOCOL_BINARY_RESPONSE_ROLLBACK,
                                  0,
                                  static_cast<void*>(&cookie))) {
            cookie.sendDynamicBuffer();
        } else {
            cookie.sendResponse(cb::mcbp::Status::Enomem);
        }
        break;

    case ENGINE_DISCONNECT:
        connection.setState(McbpStateMachine::State::closing);
        break;

    case ENGINE_EWOULDBLOCK:
        cookie.setEwouldblock(true);
        break;

    default:
        cookie.sendResponse(cb::engine_errc(ret));
    }
}

