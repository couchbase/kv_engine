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
#include "dcp_deletion.h"
#include "engine_wrapper.h"
#include "utilities.h"
#include "../../mcbp.h"

void dcp_deletion_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_dcp_deletion*>(packet);

    ENGINE_ERROR_CODE ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    if (ret == ENGINE_SUCCESS) {
        const uint16_t nkey = ntohs(req->message.header.request.keylen);
        const DocKey key{req->bytes + sizeof(req->bytes), nkey,
                   DocNamespace::DefaultCollection};
        const auto opaque = req->message.header.request.opaque;
        const auto datatype = req->message.header.request.datatype;
        const uint64_t cas = ntohll(req->message.header.request.cas);
        const uint16_t vbucket = ntohs(req->message.header.request.vbucket);
        const uint64_t by_seqno = ntohll(req->message.body.by_seqno);
        const uint64_t rev_seqno = ntohll(req->message.body.rev_seqno);
        const uint16_t nmeta = ntohs(req->message.body.nmeta);
        const uint32_t valuelen = ntohl(req->message.header.request.bodylen) -
                                  nkey - req->message.header.request.extlen -
                                  nmeta;
        cb::const_byte_buffer value{req->bytes + sizeof(req->bytes) + nkey,
                                    valuelen};
        cb::const_byte_buffer meta{value.buf + valuelen, nmeta};
        uint32_t priv_bytes = 0;
        if (mcbp::datatype::is_xattr(datatype)) {
            priv_bytes = valuelen;
        }
        ret = c->getBucketEngine()->dcp.deletion(c->getBucketEngineAsV0(),
                                                 c->getCookie(), opaque,
                                                 key, value, priv_bytes,
                                                 datatype, cas, vbucket,
                                                 by_seqno, rev_seqno, meta);
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

ENGINE_ERROR_CODE dcp_message_deletion(const void* void_cookie,
                                       uint32_t opaque,
                                       item* it,
                                       uint16_t vbucket,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       const void* meta,
                                       uint16_t nmeta) {
    auto* c = cookie2mcbp(void_cookie, __func__);
    c->setCmd(PROTOCOL_BINARY_CMD_DCP_DELETION);

    // Use a unique_ptr to make sure we release the item in all error paths
    cb::unique_item_ptr item(it, cb::ItemDeleter{c->getBucketEngineAsV0()});

    item_info info;
    if (!bucket_get_item_info(c, it, &info)) {
        LOG_WARNING(c, "%u: dcp_message_deletion: Failed to get item info",
                    c->getId());
        return ENGINE_FAILED;
    }

    uint32_t payloadsize = 0;
    protocol_binary_datatype_t datatype = PROTOCOL_BINARY_RAW_BYTES;
    if (c->isDcpXattrAware()) {
        if (mcbp::datatype::is_xattr(info.datatype)) {
            datatype = PROTOCOL_BINARY_DATATYPE_XATTR;
            payloadsize = info.nbytes;
        }
    }

    protocol_binary_request_dcp_deletion packet;
    // check if we've got enough space in our current buffer to fit
    // this message.
    if (c->write.bytes + sizeof(packet.bytes) + nmeta >= c->write.size) {
        return ENGINE_E2BIG;
    }

    if (!c->reserveItem(it)) {
        LOG_WARNING(c, "%u: dcp_message_deletion: Failed to grow item array",
                    c->getId());
        return ENGINE_FAILED;
    }

    // we've reserved the item, and it'll be released when we're done sending
    // the item.
    item.release();

    memset(packet.bytes, 0, sizeof(packet));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_DELETION;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.header.request.cas = htonll(info.cas);
    packet.message.header.request.keylen = htons(info.nkey);
    packet.message.header.request.extlen = 18;
    packet.message.header.request.bodylen = ntohl(18 + info.nkey + nmeta + payloadsize);
    packet.message.header.request.datatype = datatype;
    packet.message.body.by_seqno = htonll(by_seqno);
    packet.message.body.rev_seqno = htonll(rev_seqno);
    packet.message.body.nmeta = htons(nmeta);

    // Add the header
    c->addIov(c->write.curr, sizeof(packet.bytes));
    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    // Add the key
    c->addIov(info.key, info.nkey);

    // Add the optional payload (xattr)
    if (payloadsize > 0) {
        c->addIov(info.value[0].iov_base, payloadsize);
    }

    // Add the nmeta
    if (nmeta > 0) {
        memcpy(c->write.curr, meta, nmeta);
        c->addIov(c->write.curr, nmeta);
        c->write.curr += nmeta;
        c->write.bytes += nmeta;
    }

    return ENGINE_SUCCESS;
}
