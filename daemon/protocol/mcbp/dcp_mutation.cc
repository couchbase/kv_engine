/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include "dcp_mutation.h"
#include "utilities.h"
#include "../../mcbp.h"

#include <limits>
#include <stdexcept>

ENGINE_ERROR_CODE dcp_message_mutation(const void* void_cookie,
                                       uint32_t opaque,
                                       item* it,
                                       uint16_t vbucket,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       uint32_t lock_time,
                                       const void* meta,
                                       uint16_t nmeta,
                                       uint8_t nru) {
    auto* c = cookie2mcbp(void_cookie, __func__);
    c->setCmd(PROTOCOL_BINARY_CMD_DCP_MUTATION);
    item_info_holder info;
    protocol_binary_request_dcp_mutation packet;
    int xx;

    if (c->write.bytes + sizeof(packet.bytes) + nmeta >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    info.info.nvalue = IOV_MAX;

    if (!bucket_get_item_info(c, it, &info.info)) {
        bucket_release_item(c, it);
        LOG_WARNING(c, "%u: Failed to get item info", c->getId());
        return ENGINE_FAILED;
    }

    if (!c->reserveItem(it)) {
        bucket_release_item(c, it);
        LOG_WARNING(c, "%u: Failed to grow item array", c->getId());
        return ENGINE_FAILED;
    }

    memset(packet.bytes, 0, sizeof(packet));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_MUTATION;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.header.request.cas = htonll(info.info.cas);
    packet.message.header.request.keylen = htons(info.info.nkey);
    packet.message.header.request.extlen = 31;
    packet.message.header.request.bodylen = ntohl(
        31 + info.info.nkey + info.info.nbytes + nmeta);
    packet.message.header.request.datatype = info.info.datatype;
    packet.message.body.by_seqno = htonll(by_seqno);
    packet.message.body.rev_seqno = htonll(rev_seqno);
    packet.message.body.lock_time = htonl(lock_time);
    packet.message.body.flags = info.info.flags;
    packet.message.body.expiration = htonl(info.info.exptime);
    packet.message.body.nmeta = htons(nmeta);
    packet.message.body.nru = nru;

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);
    c->addIov(info.info.key, info.info.nkey);
    for (xx = 0; xx < info.info.nvalue; ++xx) {
        c->addIov(info.info.value[xx].iov_base, info.info.value[xx].iov_len);
    }

    memcpy(c->write.curr, meta, nmeta);
    c->addIov(c->write.curr, nmeta);
    c->write.curr += nmeta;
    c->write.bytes += nmeta;

    return ENGINE_SUCCESS;
}

void dcp_mutation_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_dcp_mutation*>(packet);

    if (c->getBucketEngine()->dcp.mutation == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->getAiostat();
        c->setAiostat(ENGINE_SUCCESS);
        c->setEwouldblock(false);

        if (ret == ENGINE_SUCCESS) {
            char* key = (char*)packet + sizeof(req->bytes);
            uint16_t nkey = ntohs(req->message.header.request.keylen);
            void* value = key + nkey;
            uint64_t cas = ntohll(req->message.header.request.cas);
            uint16_t vbucket = ntohs(req->message.header.request.vbucket);
            uint32_t flags = req->message.body.flags;
            uint8_t datatype = req->message.header.request.datatype;
            uint64_t by_seqno = ntohll(req->message.body.by_seqno);
            uint64_t rev_seqno = ntohll(req->message.body.rev_seqno);
            uint32_t expiration = ntohl(req->message.body.expiration);
            uint32_t lock_time = ntohl(req->message.body.lock_time);
            uint16_t nmeta = ntohs(req->message.body.nmeta);
            uint32_t nvalue = ntohl(req->message.header.request.bodylen) - nkey
                              - req->message.header.request.extlen - nmeta;

            ret = c->getBucketEngine()->dcp.mutation(c->getBucketEngineAsV0(),
                                                     c->getCookie(),
                                                     req->message.header.request.opaque,
                                                     key, nkey, value, nvalue,
                                                     cas, vbucket,
                                                     flags, datatype, by_seqno,
                                                     rev_seqno,
                                                     expiration, lock_time,
                                                     (char*)value + nvalue,
                                                     nmeta,
                                                     req->message.body.nru);
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
