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

void dcp_system_event_executor(McbpConnection* c, void* packet) {
    auto* req =
            reinterpret_cast<protocol_binary_request_dcp_system_event*>(packet);

    ENGINE_ERROR_CODE ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    if (ret == ENGINE_SUCCESS) {
        const uint16_t nkey = ntohs(req->message.header.request.keylen);
        cb::const_byte_buffer key{req->bytes + sizeof(req->bytes), nkey};

        size_t bodylen = ntohl(req->message.header.request.bodylen) -
                         req->message.header.request.extlen - nkey;
        cb::const_byte_buffer eventData{req->bytes + sizeof(req->bytes) + nkey,
                                        bodylen};

        ret = c->getBucketEngine()->dcp.system_event(
                c->getBucketEngineAsV0(),
                c->getCookie(),
                req->message.header.request.opaque,
                ntohs(req->message.header.request.vbucket),
                mcbp::systemevent::id(ntohl(req->message.body.event)),
                ntohll(req->message.body.by_seqno),
                key,
                eventData);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        c->setState(conn_new_cmd);
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

ENGINE_ERROR_CODE dcp_message_system_event(const void* cookie,
                                          uint32_t opaque,
                                          uint16_t vbucket,
                                          mcbp::systemevent::id event,
                                          uint64_t bySeqno,
                                          cb::const_byte_buffer key,
                                          cb::const_byte_buffer eventData) {
    auto* c = cookie2mcbp(cookie, __func__);
    c->setCmd(PROTOCOL_BINARY_CMD_DCP_SYSTEM_EVENT);

    protocol_binary_request_dcp_system_event packet(
            opaque, vbucket, key.size(), eventData.size(), event, bySeqno);

    // check if we've got enough space in our current buffer to fit
    // this message.
    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        return ENGINE_E2BIG;
    }

    // Add the header
    c->addIov(c->write.curr, sizeof(packet.bytes));
    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    // Add the key and body
    c->addIov(key.data(), key.size());
    c->addIov(eventData.data(), eventData.size());

    return ENGINE_SUCCESS;
}
