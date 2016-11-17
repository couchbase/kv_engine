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

#include <platform/compress.h>
#include <limits>
#include <stdexcept>
#include <daemon/xattr_utils.h>
#include <extmeta/extmeta.h>

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
    info.info.nvalue = 1;

    if (!bucket_get_item_info(c, it, &info.info)) {
        bucket_release_item(c, it);
        LOG_WARNING(c, "%u: Failed to get item info", c->getId());
        return ENGINE_FAILED;
    }

    char* root = reinterpret_cast<char*>(info.info.value[0].iov_base);
    char_buffer buffer{root, info.info.value[0].iov_len};
    cb::compression::Buffer inflated;

    if (mcbp::datatype::is_xattr(info.info.datatype)) {
        // @todo we've not updated the dcp repclicaiton stuff to handle
        // @todo xattrs and according to the XATTR spec this should be
        // @todo enabled by the DCP stream (and not by a hello call)
        // @todo Let's strip them off until we've adressed all of that.

        if (mcbp::datatype::is_compressed(info.info.datatype)) {
            if (!cb::compression::inflate(cb::compression::Algorithm::Snappy,
                                          buffer.buf, buffer.len, inflated)) {
                LOG_WARNING(c, "%u: Failed to inflate document", c->getId());
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

        auto body = cb::xattr::get_body({buffer.buf, buffer.len});
        buffer.buf = const_cast<char*>(body.buf);
        buffer.len = body.len;
    }

    protocol_binary_request_dcp_mutation packet;

    if (c->write.bytes + sizeof(packet.bytes) + nmeta >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    if (!c->reserveItem(it)) {
        bucket_release_item(c, it);
        LOG_WARNING(c, "%u: Failed to grow item array", c->getId());
        return ENGINE_FAILED;
    }

    const uint8_t extlen = 31; // 2*uint64_t, 3*uint32_t, 1*uint16_t, 1*uint8_t
    const uint32_t bodylen = uint32_t(buffer.len) + extlen + nmeta + info.info.nkey;

    memset(packet.bytes, 0, sizeof(packet));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_MUTATION;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.header.request.cas = htonll(info.info.cas);
    packet.message.header.request.keylen = htons(info.info.nkey);
    packet.message.header.request.extlen = extlen;
    packet.message.header.request.bodylen = ntohl(bodylen);
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
    c->addIov(buffer.buf, buffer.len);

    memcpy(c->write.curr, meta, nmeta);
    c->addIov(c->write.curr, nmeta);
    c->write.curr += nmeta;
    c->write.bytes += nmeta;

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_xattr_mutation(McbpConnection* conn, void* packet);

static ENGINE_ERROR_CODE dcp_plain_mutation(McbpConnection* conn, void* packet);

void dcp_mutation_executor(McbpConnection* c, void* packet) {

    if (c->getBucketEngine()->dcp.mutation == NULL) {
        // @todo this should be moved over to validator now that it
        //       have access to the cookie object
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        ENGINE_ERROR_CODE ret = c->getAiostat();
        c->setAiostat(ENGINE_SUCCESS);
        c->setEwouldblock(false);

        if (ret == ENGINE_SUCCESS) {
            auto* req = reinterpret_cast<protocol_binary_request_header*>(packet);
            if (mcbp::datatype::is_compressed(req->request.datatype)) {
                LOG_WARNING(nullptr, "%u: Compression not supported right now",
                            c->getId());
                ret = ENGINE_ENOTSUP;
            } else if (mcbp::datatype::is_xattr(req->request.datatype)) {
                ret = dcp_xattr_mutation(c, packet);
            } else {
                ret = dcp_plain_mutation(c, packet);
            }
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

class XattrReceiver : public cb::extmeta::Parser::Receiver {
public:
    XattrReceiver() : length(0) {}

    bool add(const cb::extmeta::Type& type,
             const uint8_t* data, size_t size) override {
        if (type == cb::extmeta::Type::XATTR_LENGTH) {
            if (size != sizeof(length)) {
                throw std::invalid_argument(
                    "XattrReceiver::add: incorrect size for xattr");
            }

            memcpy(&length, data, sizeof(length));
            length = ntohl(length);
            return false;
        }
        return true;
    }

    uint32_t length;
};

static ENGINE_ERROR_CODE dcp_xattr_mutation(McbpConnection* conn,
                                            void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_dcp_mutation*>(packet);

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
    uint8_t* meta = reinterpret_cast<uint8_t*>(value) + nvalue;

    // Ok I need to find the xattrs
    XattrReceiver receiver;

    try {
        cb::extmeta::Parser::parse(meta, nmeta, receiver);
    } catch (const std::exception& ex) {
        LOG_WARNING(conn, "%u - Failed to parse meta: %s", ex.what());
        return ENGINE_FAILED;
    }

    // Unfortunately the engine shouldn't know anything about our encoding so
    // I need to reallocate the packet and stash the length field in..
    // we should be able to fix this in a better way later on (split up the
    // interface to do this and use the "alloc" function and then call the
    // mutation entry with an item reference
    std::unique_ptr<char[]> buffer;
    try {
        buffer.reset(new char[nvalue + sizeof(uint32_t)]);
    } catch (const std::bad_alloc&) {
        LOG_INFO(conn, "%u - Failed to allocate temporary buffer");
        return ENGINE_ENOMEM;
    }

    auto* xattrlen = reinterpret_cast<uint32_t*>(buffer.get());
    *xattrlen = receiver.length;
    memcpy(buffer.get() + 4, value, nvalue);

    auto engine = conn->getBucketEngine();
    auto ret = engine->dcp.mutation(conn->getBucketEngineAsV0(),
                                    conn->getCookie(),
                                    req->message.header.request.opaque,
                                    key, nkey, buffer.get(), nvalue + 4,
                                    cas, vbucket,
                                    flags, datatype, by_seqno,
                                    rev_seqno,
                                    expiration, lock_time,
                                    (char*)value + nvalue,
                                    nmeta,
                                    req->message.body.nru);
    return ret;
}

static ENGINE_ERROR_CODE dcp_plain_mutation(McbpConnection* conn,
                                            void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_dcp_mutation*>(packet);

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

    auto engine = conn->getBucketEngine();
    auto ret = engine->dcp.mutation(conn->getBucketEngineAsV0(),
                                    conn->getCookie(),
                                    req->message.header.request.opaque,
                                    key, nkey, value, nvalue,
                                    cas, vbucket,
                                    flags, datatype, by_seqno,
                                    rev_seqno,
                                    expiration, lock_time,
                                    (char*)value + nvalue,
                                    nmeta,
                                    req->message.body.nru);
    return ret;
}
