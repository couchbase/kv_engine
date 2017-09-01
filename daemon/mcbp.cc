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
#include "mcbp.h"

#include "debug_helpers.h"
#include "memcached.h"
#include "protocol/mcbp/engine_wrapper.h"
#include "utilities/protocol2text.h"
#include "xattr/utils.h"

#include <include/memcached/protocol_binary.h>
#include <platform/compress.h>

/**
 * Send a not my vbucket response to the client. It should piggyback the
 * current vbucket map unless the client knows it already (and is configured
 * to do deduplication of these maps).
 *
 * @param c the connection to send the response to
 * @return true if success, false if memory allocation fails.
 */
static bool send_not_my_vbucket(McbpConnection& c) {
    auto pair = c.getBucket().clusterConfiguration.getConfiguration();
    if (pair.first == -1 ||
        (pair.first == c.getClustermapRevno() && settings.isDedupeNmvbMaps())) {
        // We don't have a vbucket map, or we've already sent it to the
        // client
        mcbp_add_header(&c,
                        PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
                        0,
                        0,
                        0,
                        PROTOCOL_BINARY_RAW_BYTES);
        c.setState(conn_send_data);
        c.setWriteAndGo(conn_new_cmd);
        return true;
    }

    protocol_binary_response_header* header;
    const size_t needed = sizeof(*header) + pair.second->size();

    if (!c.growDynamicBuffer(needed)) {
        LOG_WARNING(&c,
                    "<%d ERROR: Failed to allocate memory for response",
                    c.getId());
        return false;
    }

    auto& buffer = c.getDynamicBuffer();
    auto* buf = buffer.getCurrent();
    header = reinterpret_cast<protocol_binary_response_header*>(buf);
    memset(header, 0, sizeof(*header));

    header->response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    header->response.opcode = c.binary_header.request.opcode;
    header->response.status =
            (uint16_t)htons(PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET);
    header->response.bodylen = htonl((uint32_t)pair.second->size());
    header->response.opaque = c.getOpaque();
    buf += sizeof(*header);
    std::copy(pair.second->begin(), pair.second->end(), buf);
    buffer.moveOffset(needed);

    mcbp_write_and_free(&c, &c.getDynamicBuffer());
    c.setClustermapRevno(pair.first);
    return true;
}

void mcbp_write_response(McbpConnection* c,
                         const void* d,
                         int extlen,
                         int keylen,
                         int dlen) {
    if (!c->isNoReply() || c->getCmd() == PROTOCOL_BINARY_CMD_GET ||
        c->getCmd() == PROTOCOL_BINARY_CMD_GETK) {
        mcbp_add_header(c,
                        PROTOCOL_BINARY_RESPONSE_SUCCESS,
                        extlen,
                        keylen,
                        dlen,
                        PROTOCOL_BINARY_RAW_BYTES);
        c->addIov(d, dlen);
        c->setState(conn_send_data);
        c->setWriteAndGo(conn_new_cmd);
    } else {
        if (c->getStart() != 0) {
            mcbp_collect_timings(reinterpret_cast<McbpConnection*>(c));
            c->setStart(0);
        }
        // The responseCounter is updated here as this is non-responding code
        // hence mcbp_add_header will not be called (which is what normally
        // updates the responseCounters).
        ++c->getBucket().responseCounters[PROTOCOL_BINARY_RESPONSE_SUCCESS];
        c->setState(conn_new_cmd);
    }
}

void mcbp_write_and_free(McbpConnection* c, DynamicBuffer* buf) {
    if (buf->getRoot() == nullptr) {
        c->setState(conn_closing);
    } else {
        if (!c->pushTempAlloc(buf->getRoot())) {
            c->setState(conn_closing);
            return;
        }
        c->addIov(buf->getRoot(), buf->getOffset());
        c->setState(conn_send_data);
        c->setWriteAndGo(conn_new_cmd);

        buf->takeOwnership();
    }
}

void mcbp_write_packet(McbpConnection* c, protocol_binary_response_status err) {
    if (err == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        mcbp_write_response(c, NULL, 0, 0, 0);
        return;
    }
    if (err == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET) {
        if (!send_not_my_vbucket(*c)) {
            throw std::runtime_error(
                    "mcbp_write_packet: failed to send not my vbucket");
        }
        return;
    }

    // MB-23909: Server does not include event id's in the error message.
    auto& cookie = c->getCookieObject();
    const auto& payload = cookie.getErrorJson();

    mcbp_add_header(c,
                    err,
                    0,
                    0,
                    uint32_t(payload.size()),
                    payload.empty() ? PROTOCOL_BINARY_RAW_BYTES
                                    : PROTOCOL_BINARY_DATATYPE_JSON);
    if (!payload.empty()) {
        c->addIov(payload.data(), payload.size());
    }
    c->setState(conn_send_data);
    c->setWriteAndGo(conn_new_cmd);
}

cb::const_byte_buffer mcbp_add_header(cb::Pipe& pipe,
                                      uint8_t opcode,
                                      uint16_t err,
                                      uint8_t ext_len,
                                      uint16_t key_len,
                                      uint32_t body_len,
                                      uint8_t datatype,
                                      uint32_t opaque,
                                      uint64_t cas) {
    auto wbuf = pipe.wdata();
    auto* header = (protocol_binary_response_header*)wbuf.data();

    header->response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    header->response.opcode = opcode;
    header->response.keylen = (uint16_t)htons(key_len);

    header->response.extlen = ext_len;
    header->response.datatype = datatype;
    header->response.status = (uint16_t)htons(err);

    header->response.bodylen = htonl(body_len);
    header->response.opaque = opaque;
    header->response.cas = htonll(cas);
    pipe.produced(sizeof(header->bytes));

    return {wbuf.data(), sizeof(header->bytes)};
}

void mcbp_add_header(McbpConnection* c,
                     uint16_t err,
                     uint8_t ext_len,
                     uint16_t key_len,
                     uint32_t body_len,
                     uint8_t datatype) {
    if (c == nullptr) {
        throw std::invalid_argument("mcbp_add_header: 'c' must be non-NULL");
    }

    c->addMsgHdr(true);
    const auto wbuf = mcbp_add_header(c->write,
                                      c->binary_header.request.opcode,
                                      err,
                                      ext_len,
                                      key_len,
                                      body_len,
                                      datatype,
                                      c->getOpaque(),
                                      c->getCAS());

    if (settings.getVerbose() > 1) {
        char buffer[1024];
        if (bytes_to_output_string(buffer,
                                   sizeof(buffer),
                                   c->getId(),
                                   false,
                                   "Writing bin response:",
                                   reinterpret_cast<const char*>(wbuf.data()),
                                   wbuf.size()) != -1) {
            LOG_DEBUG(c, "%s", buffer);
        }
    }

    ++c->getBucket().responseCounters[err];
    c->addIov(wbuf.data(), wbuf.size());
}

bool mcbp_response_handler(const void* key, uint16_t keylen,
                           const void* ext, uint8_t extlen,
                           const void* body, uint32_t bodylen,
                           protocol_binary_datatype_t datatype, uint16_t status,
                           uint64_t cas, const void* void_cookie)
{
    auto* ccookie = reinterpret_cast<const Cookie*>(void_cookie);
    auto* cookie = const_cast<Cookie*>(ccookie);
    cookie->validate();

    McbpConnection* c = &cookie->connection;
    cb::compression::Buffer buffer;
    cb::const_char_buffer payload(static_cast<const char*>(body), bodylen);

    if (!c->isSnappyEnabled() && mcbp::datatype::is_snappy(datatype)) {
        // The client is not snappy-aware, and the content contains
        // snappy encoded data.. We need to inflate it!
        if (!cb::compression::inflate(cb::compression::Algorithm::Snappy,
                                      payload.buf, payload.len, buffer)) {
            std::string mykey(reinterpret_cast<const char*>(key), keylen);
            LOG_WARNING(c,
                        "<%u ERROR: Failed to inflate body, "
                            "Key: %s may have an incorrect datatype, "
                            "Datatype indicates that document is %s",
                        c->getId(), mykey.c_str(),
                        mcbp::datatype::to_string(datatype).c_str());
            return false;
        }
        payload.buf = buffer.data.get();
        payload.len = buffer.len;
        datatype &= ~(PROTOCOL_BINARY_DATATYPE_SNAPPY);
    }

    if (mcbp::datatype::is_xattr(datatype)) {
        // We need to strip off the xattrs
        payload = cb::xattr::get_body(payload);
        datatype &= ~(PROTOCOL_BINARY_DATATYPE_XATTR);
    }

    datatype = c->getEnabledDatatypes(datatype);
    auto& error_json = cookie->getErrorJson();

    switch (status) {
    case PROTOCOL_BINARY_RESPONSE_SUCCESS:
    case PROTOCOL_BINARY_RESPONSE_SUBDOC_SUCCESS_DELETED:
    case PROTOCOL_BINARY_RESPONSE_SUBDOC_MULTI_PATH_FAILURE:
    case PROTOCOL_BINARY_RESPONSE_ROLLBACK:
        break;
    case PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET:
        return send_not_my_vbucket(*c);
    default:
        //
        payload = {error_json.data(), error_json.size()};
        keylen = 0;
        extlen = 0;
        datatype = payload.empty() ? PROTOCOL_BINARY_RAW_BYTES
                                   : PROTOCOL_BINARY_DATATYPE_JSON;
    }

    const size_t needed = payload.len + keylen + extlen +
                          sizeof(protocol_binary_response_header);

    auto &dbuf = c->getDynamicBuffer();
    if (!dbuf.grow(needed)) {
        LOG_WARNING(c, "<%u ERROR: Failed to allocate memory for response",
                    c->getId());
        return false;
    }

    protocol_binary_response_header header = {};
    header.response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    header.response.opcode = c->binary_header.request.opcode;
    header.response.keylen = (uint16_t)htons(keylen);
    header.response.extlen = extlen;
    header.response.datatype = datatype;
    header.response.status = (uint16_t)htons(status);
    header.response.bodylen = htonl(needed - sizeof(protocol_binary_response_header));
    header.response.opaque = c->getOpaque();
    header.response.cas = htonll(cas);

    ++c->getBucket().responseCounters[status];

    char *buf = dbuf.getCurrent();
    memcpy(buf, header.bytes, sizeof(header.response));
    buf += sizeof(header.response);

    if (extlen > 0) {
        memcpy(buf, ext, extlen);
        buf += extlen;
    }

    if (keylen > 0) {
        memcpy(buf, key, keylen);
        buf += keylen;
    }

    if (payload.len > 0) {
        memcpy(buf, payload.buf, payload.len);
    }

    dbuf.moveOffset(needed);
    return true;
}

void mcbp_collect_timings(const McbpConnection* c) {
    hrtime_t now = gethrtime();
    const hrtime_t elapsed_ns = now - c->getStart();
    // aggregated timing for all buckets
    all_buckets[0].timings.collect(c->getCmd(), elapsed_ns);

    // timing for current bucket
    bucket_id_t bucketid = get_bucket_id(c->getCookie());
    /* bucketid will be zero initially before you run sasl auth
     * (unless there is a default bucket), or if someone tries
     * to delete the bucket you're associated with and your're idle.
     */
    if (bucketid != 0) {
        all_buckets[bucketid].timings.collect(c->getCmd(), elapsed_ns);
    }

    // Log operations taking longer than 0.5s
    const hrtime_t elapsed_ms = elapsed_ns / (1000 * 1000);
    c->maybeLogSlowCommand(std::chrono::milliseconds(elapsed_ms));
}
