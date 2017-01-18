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
#include "dcp_expiration.h"
#include "engine_wrapper.h"
#include "utilities.h"
#include "../../mcbp.h"

void dcp_expiration_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_dcp_expiration*>(packet);

    if (c->getBucketEngine()->dcp.expiration == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->getAiostat();
        c->setAiostat(ENGINE_SUCCESS);
        c->setEwouldblock(false);

        if (ret == ENGINE_SUCCESS) {
            char* key = (char*)packet + sizeof(req->bytes);
            uint16_t nkey = ntohs(req->message.header.request.keylen);
            uint64_t cas = ntohll(req->message.header.request.cas);
            uint16_t vbucket = ntohs(req->message.header.request.vbucket);
            uint64_t by_seqno = ntohll(req->message.body.by_seqno);
            uint64_t rev_seqno = ntohll(req->message.body.rev_seqno);
            uint16_t nmeta = ntohs(req->message.body.nmeta);

            ret = c->getBucketEngine()->dcp.expiration(c->getBucketEngineAsV0(),
                                                       c->getCookie(),
                                                       req->message.header.request.opaque,
                                                       key, nkey, cas, vbucket,
                                                       by_seqno, rev_seqno,
                                                       key + nkey, nmeta);
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
}

ENGINE_ERROR_CODE dcp_message_expiration(const void* void_cookie,
                                         uint32_t opaque,
                                         const void* key,
                                         uint16_t nkey,
                                         uint64_t cas,
                                         uint16_t vbucket,
                                         uint64_t by_seqno,
                                         uint64_t rev_seqno,
                                         const void* meta,
                                         uint16_t nmeta) {
    auto* c = cookie2mcbp(void_cookie, __func__);
    c->setCmd(PROTOCOL_BINARY_CMD_DCP_EXPIRATION);
    protocol_binary_request_dcp_deletion packet;

    if (c->write.bytes + sizeof(packet.bytes) + nkey + nmeta >= c->write.size) {
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_EXPIRATION;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.header.request.cas = htonll(cas);
    packet.message.header.request.keylen = htons(nkey);
    packet.message.header.request.extlen = 18;
    packet.message.header.request.bodylen = ntohl(18 + nkey + nmeta);
    packet.message.body.by_seqno = htonll(by_seqno);
    packet.message.body.rev_seqno = htonll(rev_seqno);
    packet.message.body.nmeta = htons(nmeta);

    c->addIov(c->write.curr, sizeof(packet.bytes) + nkey + nmeta);
    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);
    memcpy(c->write.curr, key, nkey);
    c->write.curr += nkey;
    c->write.bytes += nkey;
    memcpy(c->write.curr, meta, nmeta);
    c->write.curr += nmeta;
    c->write.bytes += nmeta;

    return ENGINE_SUCCESS;
}