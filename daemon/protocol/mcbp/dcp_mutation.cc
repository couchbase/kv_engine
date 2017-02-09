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
#include "engine_wrapper.h"
#include "utilities.h"
#include "../../mcbp.h"

#include <platform/compress.h>
#include <limits>
#include <stdexcept>
#include <xattr/blob.h>
#include <xattr/utils.h>

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
    // Use a unique_ptr to make sure we release the item in all error paths
    cb::unique_item_ptr item(it, cb::ItemDeleter{c->getBucketEngineAsV0()});

    item_info info;

    if (!bucket_get_item_info(c, it, &info)) {
        LOG_WARNING(c, "%u: Failed to get item info", c->getId());
        return ENGINE_FAILED;
    }

    char* root = reinterpret_cast<char*>(info.value[0].iov_base);
    cb::char_buffer buffer{root, info.value[0].iov_len};
    cb::compression::Buffer inflated;

    if (c->isDcpNoValue() && !c->isDcpXattrAware()) {
        // The client don't want the body or any xattrs.. just
        // drop everything
        buffer.len = 0;
        info.datatype = PROTOCOL_BINARY_RAW_BYTES;
    } else if (!c->isDcpNoValue() && c->isDcpXattrAware()) {
        // The client want both value and xattrs.. we don't need to
        // do anything

    } else {
        // we want either values or xattrs
        if (mcbp::datatype::is_compressed(info.datatype)) {
            if (!cb::compression::inflate(
                cb::compression::Algorithm::Snappy,
                buffer.buf, buffer.len, inflated)) {
                LOG_WARNING(c, "%u: Failed to inflate document",
                            c->getId());
                return ENGINE_FAILED;
            }

            // @todo figure out this one.. Right now our tempAlloc list is
            //       memory allocated by cb_malloc... it would probably be
            //       better if we had something similar for new'd memory..
            buffer.buf = reinterpret_cast<char*>(cb_malloc(inflated.len));
            if (buffer.buf == nullptr) {
                return ENGINE_ENOMEM;
            }
            buffer.len = inflated.len;
            memcpy(buffer.buf, inflated.data.get(), buffer.len);
            if (!c->pushTempAlloc(buffer.buf)) {
                cb_free(buffer.buf);
                return ENGINE_ENOMEM;
            }
        }

        if (c->isDcpXattrAware()) {
            if (mcbp::datatype::is_xattr(info.datatype)) {
                buffer.len = cb::xattr::get_body_offset({buffer.buf, buffer.len});
            } else {
                buffer.len = 0;
                info.datatype = PROTOCOL_BINARY_RAW_BYTES;
            }
        } else {
            // we want the body
            if (mcbp::datatype::is_xattr(info.datatype)) {
                auto body = cb::xattr::get_body({buffer.buf, buffer.len});
                buffer.buf = const_cast<char*>(body.buf);
                buffer.len = body.len;
                info.datatype &= ~PROTOCOL_BINARY_DATATYPE_XATTR;
            }
        }
    }
    protocol_binary_request_dcp_mutation packet;

    if (c->write.bytes + sizeof(packet.bytes) + nmeta >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    if (!c->reserveItem(it)) {
        LOG_WARNING(c, "%u: Failed to grow item array", c->getId());
        return ENGINE_FAILED;
    }

    // we've reserved the item, and it'll be released when we're done sending
    // the item.
    item.release();

    const uint8_t extlen = 31; // 2*uint64_t, 3*uint32_t, 1*uint16_t, 1*uint8_t
    const uint32_t bodylen = uint32_t(buffer.len) + extlen + nmeta + info.nkey;

    memset(packet.bytes, 0, sizeof(packet));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_MUTATION;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.header.request.cas = htonll(info.cas);
    packet.message.header.request.keylen = htons(info.nkey);
    packet.message.header.request.extlen = extlen;
    packet.message.header.request.bodylen = ntohl(bodylen);
    packet.message.header.request.datatype = info.datatype;
    packet.message.body.by_seqno = htonll(by_seqno);
    packet.message.body.rev_seqno = htonll(rev_seqno);
    packet.message.body.lock_time = htonl(lock_time);
    packet.message.body.flags = info.flags;
    packet.message.body.expiration = htonl(info.exptime);
    packet.message.body.nmeta = htons(nmeta);
    packet.message.body.nru = nru;

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);
    c->addIov(info.key, info.nkey);
    c->addIov(buffer.buf, buffer.len);

    memcpy(c->write.curr, meta, nmeta);
    c->addIov(c->write.curr, nmeta);
    c->write.curr += nmeta;
    c->write.bytes += nmeta;

    return ENGINE_SUCCESS;
}

static inline ENGINE_ERROR_CODE do_dcp_mutation(McbpConnection* conn,
                                                void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_dcp_mutation*>(packet);

    const uint16_t nkey = ntohs(req->message.header.request.keylen);
    const DocKey key{req->bytes + sizeof(req->bytes), nkey,
                     DocNamespace::DefaultCollection};

    const auto opaque = req->message.header.request.opaque;
    const auto datatype = req->message.header.request.datatype;
    const uint64_t cas = ntohll(req->message.header.request.cas);
    const uint16_t vbucket = ntohs(req->message.header.request.vbucket);
    const uint64_t by_seqno = ntohll(req->message.body.by_seqno);
    const uint64_t rev_seqno = ntohll(req->message.body.rev_seqno);
    const uint32_t flags = req->message.body.flags;
    const uint32_t expiration = ntohl(req->message.body.expiration);
    const uint32_t lock_time = ntohl(req->message.body.lock_time);
    const uint16_t nmeta = ntohs(req->message.body.nmeta);
    const uint32_t valuelen = ntohl(req->message.header.request.bodylen) -
                              nkey - req->message.header.request.extlen -
                              nmeta;
    cb::const_byte_buffer value{req->bytes + sizeof(req->bytes) + nkey,
                                valuelen};
    cb::const_byte_buffer meta{value.buf + valuelen, nmeta};
    uint32_t priv_bytes = 0;
    if (mcbp::datatype::is_xattr(datatype)) {
        cb::const_char_buffer payload{reinterpret_cast<const char*>(value.buf),
                                      value.len};
        cb::byte_buffer buffer{const_cast<uint8_t*>(value.buf),
                               cb::xattr::get_body_offset(payload)};
        cb::xattr::Blob blob(buffer);
        priv_bytes = uint32_t(blob.get_system_size());
    }

    auto engine = conn->getBucketEngine();
    return engine->dcp.mutation(conn->getBucketEngineAsV0(), conn->getCookie(),
                                opaque, key, value, priv_bytes, datatype, cas,
                                vbucket, flags, by_seqno, rev_seqno, expiration,
                                lock_time, meta, req->message.body.nru);
}

void dcp_mutation_executor(McbpConnection* c, void* packet) {
    ENGINE_ERROR_CODE ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    if (ret == ENGINE_SUCCESS) {
        ret = do_dcp_mutation(c, packet);
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
