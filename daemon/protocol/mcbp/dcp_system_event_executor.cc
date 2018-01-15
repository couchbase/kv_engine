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
#include "dcp_system_event_executor.h"
#include "../../mcbp.h"
#include "engine_wrapper.h"
#include "utilities.h"

void dcp_system_event_executor(Cookie& cookie) {
    auto ret = cookie.swapAiostat(ENGINE_SUCCESS);

    auto& connection = cookie.getConnection();
    if (ret == ENGINE_SUCCESS) {
        auto packet = cookie.getPacket(Cookie::PacketContent::Full);
        const auto* req = reinterpret_cast<
                const protocol_binary_request_dcp_system_event*>(packet.data());

        const uint16_t nkey = ntohs(req->message.header.request.keylen);
        cb::const_byte_buffer key{req->bytes + sizeof(req->bytes), nkey};

        size_t bodylen = ntohl(req->message.header.request.bodylen) -
                         req->message.header.request.extlen - nkey;
        cb::const_byte_buffer eventData{req->bytes + sizeof(req->bytes) + nkey,
                                        bodylen};

        ret = connection.getBucketEngine()->dcp.system_event(
                connection.getBucketEngineAsV0(),
                &cookie,
                req->message.header.request.opaque,
                ntohs(req->message.header.request.vbucket),
                mcbp::systemevent::id(ntohl(req->message.body.event)),
                ntohll(req->message.body.by_seqno),
                key,
                eventData);
    }

    ret = connection.remapErrorCode(ret);
    switch (ret) {
    case ENGINE_SUCCESS:
        connection.setState(McbpStateMachine::State::new_cmd);
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
