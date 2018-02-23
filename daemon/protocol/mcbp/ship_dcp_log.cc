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

#include "ship_dcp_log.h"
#include "dcp_deletion.h"
#include "engine_wrapper.h"
#include "utilities.h"

/**
 * Get the cookie represented by the void pointer passed as a cookie through
 * the engine interface
 *
 * @param void_cookie this is the void pointer passed to all of the engine
 *                    methods
 * @param function the name of the function trying to convert the cookie. This
 *                 is used purely for error reporting (if void_cookie is null)
 * @return The connection object
 */
static McbpConnection* cookie2mcbp(const void* void_cookie, const char* function) {
    const auto * cookie = reinterpret_cast<const Cookie *>(void_cookie);
    if (cookie == nullptr) {
        throw std::invalid_argument(std::string(function) +
                                    ": cookie is nullptr");
    }
    return &cookie->getConnection();
}


static ENGINE_ERROR_CODE add_packet_to_pipe(McbpConnection* c,
                                            cb::const_byte_buffer packet) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    c->write->produce([c, packet, &ret](cb::byte_buffer buffer) -> size_t {
        if (buffer.size() < packet.size()) {
            ret = ENGINE_E2BIG;
            return 0;
        }

        std::copy(packet.begin(), packet.end(), buffer.begin());
        c->addIov(buffer.data(), packet.size());
        return packet.size();
    });

    return ret;
}

static ENGINE_ERROR_CODE dcp_message_get_failover_log(
        gsl::not_null<const void*> void_cookie,
        uint32_t opaque,
        uint16_t vbucket) {
    auto* c = cookie2mcbp(void_cookie, __func__);

    protocol_binary_request_dcp_get_failover_log packet = {};
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode =
            (uint8_t)PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);

    return add_packet_to_pipe(c, {packet.bytes, sizeof(packet.bytes)});
}

static ENGINE_ERROR_CODE dcp_message_stream_req(
        gsl::not_null<const void*> void_cookie,
        uint32_t opaque,
        uint16_t vbucket,
        uint32_t flags,
        uint64_t start_seqno,
        uint64_t end_seqno,
        uint64_t vbucket_uuid,
        uint64_t snap_start_seqno,
        uint64_t snap_end_seqno) {
    auto* c = cookie2mcbp(void_cookie, __func__);
    protocol_binary_request_dcp_stream_req packet = {};
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode =
            (uint8_t)PROTOCOL_BINARY_CMD_DCP_STREAM_REQ;
    packet.message.header.request.extlen = 48;
    packet.message.header.request.bodylen = htonl(48);
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.body.flags = ntohl(flags);
    packet.message.body.start_seqno = ntohll(start_seqno);
    packet.message.body.end_seqno = ntohll(end_seqno);
    packet.message.body.vbucket_uuid = ntohll(vbucket_uuid);
    packet.message.body.snap_start_seqno = ntohll(snap_start_seqno);
    packet.message.body.snap_end_seqno = ntohll(snap_end_seqno);

    return add_packet_to_pipe(c, {packet.bytes, sizeof(packet.bytes)});
}

static ENGINE_ERROR_CODE dcp_message_add_stream_response(
        gsl::not_null<const void*> void_cookie,
        uint32_t opaque,
        uint32_t dialogopaque,
        uint8_t status) {
    auto* c = cookie2mcbp(void_cookie, __func__);

    protocol_binary_response_dcp_add_stream packet = {};
    packet.message.header.response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    packet.message.header.response.opcode =
            (uint8_t)PROTOCOL_BINARY_CMD_DCP_ADD_STREAM;
    packet.message.header.response.extlen = 4;
    packet.message.header.response.status = htons(status);
    packet.message.header.response.setBodylen(4);
    packet.message.header.response.opaque = opaque;
    packet.message.body.opaque = ntohl(dialogopaque);

    return add_packet_to_pipe(c, {packet.bytes, sizeof(packet.bytes)});
}

static ENGINE_ERROR_CODE dcp_message_marker_response(
        gsl::not_null<const void*> void_cookie,
        uint32_t opaque,
        uint8_t status) {
    auto* c = cookie2mcbp(void_cookie, __func__);

    protocol_binary_response_dcp_snapshot_marker packet = {};
    packet.message.header.response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    packet.message.header.response.opcode =
            (uint8_t)PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER;
    packet.message.header.response.extlen = 0;
    packet.message.header.response.status = htons(status);
    packet.message.header.response.bodylen = 0;
    packet.message.header.response.opaque = opaque;

    return add_packet_to_pipe(c, {packet.bytes, sizeof(packet.bytes)});
}

static ENGINE_ERROR_CODE dcp_message_set_vbucket_state_response(
        gsl::not_null<const void*> void_cookie,
        uint32_t opaque,
        uint8_t status) {
    auto* c = cookie2mcbp(void_cookie, __func__);

    protocol_binary_response_dcp_set_vbucket_state packet = {};
    packet.message.header.response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    packet.message.header.response.opcode =
            (uint8_t)PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE;
    packet.message.header.response.extlen = 0;
    packet.message.header.response.status = htons(status);
    packet.message.header.response.bodylen = 0;
    packet.message.header.response.opaque = opaque;

    return add_packet_to_pipe(c, {packet.bytes, sizeof(packet.bytes)});
}

static ENGINE_ERROR_CODE dcp_message_stream_end(
        gsl::not_null<const void*> void_cookie,
        uint32_t opaque,
        uint16_t vbucket,
        uint32_t flags) {
    auto* c = cookie2mcbp(void_cookie, __func__);

    protocol_binary_request_dcp_stream_end packet = {};
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode =
            (uint8_t)PROTOCOL_BINARY_CMD_DCP_STREAM_END;
    packet.message.header.request.extlen = 4;
    packet.message.header.request.bodylen = htonl(4);
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.body.flags = ntohl(flags);

    return add_packet_to_pipe(c, {packet.bytes, sizeof(packet.bytes)});
}

static ENGINE_ERROR_CODE dcp_message_marker(
        gsl::not_null<const void*> void_cookie,
        uint32_t opaque,
        uint16_t vbucket,
        uint64_t start_seqno,
        uint64_t end_seqno,
        uint32_t flags) {
    auto* c = cookie2mcbp(void_cookie, __func__);

    protocol_binary_request_dcp_snapshot_marker packet = {};
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode =
            (uint8_t)PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.header.request.extlen = 20;
    packet.message.header.request.bodylen = htonl(20);
    packet.message.body.start_seqno = htonll(start_seqno);
    packet.message.body.end_seqno = htonll(end_seqno);
    packet.message.body.flags = htonl(flags);

    return add_packet_to_pipe(c, {packet.bytes, sizeof(packet.bytes)});
}

static ENGINE_ERROR_CODE dcp_message_mutation(
        gsl::not_null<const void*> void_cookie,
        uint32_t opaque,
        item* it,
        uint16_t vbucket,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        uint32_t lock_time,
        const void* meta,
        uint16_t nmeta,
        uint8_t nru,
        uint8_t collection_len) {
    if (void_cookie == nullptr) {
        throw std::invalid_argument(
                "dcp_message_deletion: void_cookie can't be nullptr");
    }
    const auto& ccookie = *static_cast<const Cookie*>(void_cookie.get());
    auto& cookie = const_cast<Cookie&>(ccookie);
    auto* c = &cookie.getConnection();

    // Use a unique_ptr to make sure we release the item in all error paths
    cb::unique_item_ptr item(it, cb::ItemDeleter{c->getBucketEngineAsV0()});

    item_info info;
    if (!bucket_get_item_info(cookie, it, &info)) {
        LOG_WARNING("{}: Failed to get item info", c->getId());
        return ENGINE_FAILED;
    }

    char* root = reinterpret_cast<char*>(info.value[0].iov_base);
    cb::char_buffer buffer{root, info.value[0].iov_len};

    if (!c->reserveItem(it)) {
        LOG_WARNING("{}: Failed to grow item array", c->getId());
        return ENGINE_FAILED;
    }

    // we've reserved the item, and it'll be released when we're done sending
    // the item.
    item.release();
    protocol_binary_request_dcp_mutation packet(
            c->isDcpCollectionAware(),
            opaque,
            vbucket,
            info.cas,
            info.nkey,
            gsl::narrow<uint32_t>(buffer.len),
            info.datatype,
            by_seqno,
            rev_seqno,
            info.flags,
            gsl::narrow<uint32_t>(info.exptime),
            lock_time,
            nmeta,
            nru,
            collection_len);

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    c->write->produce([&c, &packet, &info, &buffer, &meta, &nmeta, &ret](
                              cb::byte_buffer wbuf) -> size_t {

        const size_t packetlen =
                protocol_binary_request_dcp_mutation::getHeaderLength(
                        c->isDcpCollectionAware());

        if (wbuf.size() < (packetlen + nmeta)) {
            ret = ENGINE_E2BIG;
            return 0;
        }

        std::copy(packet.bytes, packet.bytes + packetlen, wbuf.begin());

        if (nmeta > 0) {
            std::copy(static_cast<const uint8_t*>(meta),
                      static_cast<const uint8_t*>(meta) + nmeta,
                      wbuf.data() + packetlen);
        }

        // Add the header
        c->addIov(wbuf.data(), packetlen);

        // Add the key
        c->addIov(info.key, info.nkey);

        // Add the value
        c->addIov(buffer.buf, buffer.len);

        // Add the optional meta section
        if (nmeta > 0) {
            c->addIov(wbuf.data() + packetlen, nmeta);
        }

        return packetlen + nmeta;
    });

    return ret;
}

// Shared DCP_DELETION write function for the v1/v2 commands.
static ENGINE_ERROR_CODE dcp_message_deletion(
        McbpConnection& c,
        Cookie& cookie,
        const item_info& info,
        cb::const_byte_buffer packet,
        cb::const_byte_buffer extendedMeta) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    c.write->produce([&c, &packet, &extendedMeta, &info, &ret](
                             cb::byte_buffer buffer) -> size_t {
        if (buffer.size() < (packet.size() + extendedMeta.size())) {
            ret = ENGINE_E2BIG;
            return 0;
        }

        std::copy(packet.begin(), packet.end(), buffer.begin());

        if (extendedMeta.size() > 0) {
            std::copy(extendedMeta.begin(),
                      extendedMeta.end(),
                      buffer.data() + packet.size());
        }

        // Add the header
        c.addIov(buffer.data(), packet.size());

        // Add the key
        c.addIov(info.key, info.nkey);

        // Add the optional payload (xattr)
        if (info.nbytes > 0) {
            c.addIov(info.value[0].iov_base, info.nbytes);
        }

        // Add the optional meta section
        if (extendedMeta.size() > 0) {
            c.addIov(buffer.data() + packet.size(), extendedMeta.size());
        }

        return packet.size() + extendedMeta.size();
    });

    return ret;
}

static ENGINE_ERROR_CODE dcp_message_deletion_v1(
        gsl::not_null<const void*> void_cookie,
        uint32_t opaque,
        item* it,
        uint16_t vbucket,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        const void* meta,
        uint16_t nmeta) {
    if (void_cookie == nullptr) {
        throw std::invalid_argument(
                "dcp_message_deletion_v1: void_cookie can't be nullptr");
    }

    const auto& ccookie = *static_cast<const Cookie*>(void_cookie.get());
    auto& cookie = const_cast<Cookie&>(ccookie);
    auto& c = cookie.getConnection();

    // Use a unique_ptr to make sure we release the item in all error paths
    cb::unique_item_ptr item(it, cb::ItemDeleter{c.getBucketEngineAsV0()});
    item_info info;
    if (!bucket_get_item_info(cookie, it, &info)) {
        LOG_WARNING("{}: dcp_message_deletion_v1: Failed to get item info",
                    c.getId());
        return ENGINE_FAILED;
    }

    if (!c.reserveItem(it)) {
        LOG_WARNING("{}: dcp_message_deletion_v1: Failed to grow item array",
                    c.getId());
        return ENGINE_FAILED;
    }

    // we've reserved the item, and it'll be released when we're done sending
    // the item.
    item.release();

    protocol_binary_request_dcp_deletion packet(opaque,
                                                vbucket,
                                                info.cas,
                                                info.nkey,
                                                info.nbytes,
                                                info.datatype,
                                                by_seqno,
                                                rev_seqno,
                                                nmeta);

    cb::const_byte_buffer packetBuffer{
            reinterpret_cast<const uint8_t*>(&packet), sizeof(packet.bytes)};
    cb::const_byte_buffer extendedMeta{reinterpret_cast<const uint8_t*>(meta),
                                       nmeta};

    return dcp_message_deletion(c, cookie, info, packetBuffer, extendedMeta);
}

static ENGINE_ERROR_CODE dcp_message_deletion_v2(
        gsl::not_null<const void*> void_cookie,
        uint32_t opaque,
        gsl::not_null<item*> it,
        uint16_t vbucket,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        uint32_t delete_time,
        uint8_t collection_len) {
    if (void_cookie == nullptr) {
        throw std::invalid_argument(
                "dcp_message_deletion_v2: void_cookie can't be nullptr");
    }

    const auto& ccookie = *static_cast<const Cookie*>(void_cookie.get());
    auto& cookie = const_cast<Cookie&>(ccookie);
    auto& c = cookie.getConnection();

    // Use a unique_ptr to make sure we release the item in all error paths
    cb::unique_item_ptr item(it, cb::ItemDeleter{c.getBucketEngineAsV0()});
    item_info info;
    if (!bucket_get_item_info(cookie, it, &info)) {
        LOG_WARNING("{}: dcp_message_deletion_v2: Failed to get item info",
                    c.getId());
        return ENGINE_FAILED;
    }

    if (!c.reserveItem(it)) {
        LOG_WARNING("{}: dcp_message_deletion_v2: Failed to grow item array",
                    c.getId());
        return ENGINE_FAILED;
    }

    // we've reserved the item, and it'll be released when we're done sending
    // the item.
    item.release();

    protocol_binary_request_dcp_deletion_v2 packet(opaque,
                                                   vbucket,
                                                   info.cas,
                                                   info.nkey,
                                                   info.nbytes,
                                                   info.datatype,
                                                   by_seqno,
                                                   rev_seqno,
                                                   delete_time,
                                                   collection_len);

    return dcp_message_deletion(
            c,
            cookie,
            info,
            {reinterpret_cast<const uint8_t*>(&packet), sizeof(packet.bytes)},
            {/*no extended meta in v2*/});
}

static ENGINE_ERROR_CODE dcp_message_expiration(
        gsl::not_null<const void*> void_cookie,
        uint32_t opaque,
        item* it,
        uint16_t vbucket,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        const void* meta,
        uint16_t nmeta,
        uint8_t collection_len) {
    /*
     * EP engine don't use expiration, so we won't have tests for this
     * code. Add it back once we have people calling the method
     */
    auto* c = cookie2mcbp(void_cookie, __func__);
    cb::unique_item_ptr item(it, cb::ItemDeleter{c->getBucketEngineAsV0()});
    return ENGINE_ENOTSUP;
}

static ENGINE_ERROR_CODE dcp_message_flush(
        gsl::not_null<const void*> void_cookie,
        uint32_t opaque,
        uint16_t vbucket) {
    auto* c = cookie2mcbp(void_cookie, __func__);
    protocol_binary_request_dcp_flush packet = {};
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode =
            (uint8_t)PROTOCOL_BINARY_CMD_DCP_FLUSH;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);

    return add_packet_to_pipe(c, {packet.bytes, sizeof(packet.bytes)});
}

static ENGINE_ERROR_CODE dcp_message_set_vbucket_state(
        gsl::not_null<const void*> void_cookie,
        uint32_t opaque,
        uint16_t vbucket,
        vbucket_state_t state) {
    auto* c = cookie2mcbp(void_cookie, __func__);
    protocol_binary_request_dcp_set_vbucket_state packet = {};

    if (!is_valid_vbucket_state_t(state)) {
        return ENGINE_EINVAL;
    }

    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode =
            (uint8_t)PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE;
    packet.message.header.request.extlen = 1;
    packet.message.header.request.bodylen = htonl(1);
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.body.state = uint8_t(state);

    return add_packet_to_pipe(c, {packet.bytes, sizeof(packet.bytes)});
}

static ENGINE_ERROR_CODE dcp_message_noop(
        gsl::not_null<const void*> void_cookie, uint32_t opaque) {
    auto* c = cookie2mcbp(void_cookie, __func__);
    protocol_binary_request_dcp_noop packet = {};
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode =
            (uint8_t)PROTOCOL_BINARY_CMD_DCP_NOOP;
    packet.message.header.request.opaque = opaque;

    return add_packet_to_pipe(c, {packet.bytes, sizeof(packet.bytes)});
}

static ENGINE_ERROR_CODE dcp_message_buffer_acknowledgement(
        gsl::not_null<const void*> void_cookie,
        uint32_t opaque,
        uint16_t vbucket,
        uint32_t buffer_bytes) {
    auto* c = cookie2mcbp(void_cookie, __func__);
    protocol_binary_request_dcp_buffer_acknowledgement packet = {};
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode =
            (uint8_t)PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT;
    packet.message.header.request.extlen = 4;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.header.request.bodylen = ntohl(4);
    packet.message.body.buffer_bytes = ntohl(buffer_bytes);

    return add_packet_to_pipe(c, {packet.bytes, sizeof(packet.bytes)});
}

static ENGINE_ERROR_CODE dcp_message_control(
        gsl::not_null<const void*> void_cookie,
        uint32_t opaque,
        const void* key,
        uint16_t nkey,
        const void* value,
        uint32_t nvalue) {
    auto* c = cookie2mcbp(void_cookie, __func__);
    protocol_binary_request_dcp_control packet = {};
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode =
            (uint8_t)PROTOCOL_BINARY_CMD_DCP_CONTROL;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.keylen = ntohs(nkey);
    packet.message.header.request.bodylen = ntohl(nvalue + nkey);

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    c->write->produce([&c, &packet, &key, &nkey, &value, &nvalue, &ret](
                              void* ptr, size_t size) -> size_t {
        if (size < (sizeof(packet.bytes) + nkey + nvalue)) {
            ret = ENGINE_E2BIG;
            return 0;
        }

        std::copy(packet.bytes,
                  packet.bytes + sizeof(packet.bytes),
                  static_cast<uint8_t*>(ptr));

        std::copy(static_cast<const uint8_t*>(key),
                  static_cast<const uint8_t*>(key) + nkey,
                  static_cast<uint8_t*>(ptr) + sizeof(packet.bytes));

        std::copy(static_cast<const uint8_t*>(value),
                  static_cast<const uint8_t*>(value) + nvalue,
                  static_cast<uint8_t*>(ptr) + sizeof(packet.bytes) + nkey);

        c->addIov(ptr, sizeof(packet.bytes) + nkey + nvalue);
        return sizeof(packet.bytes) + nkey + nvalue;
    });

    return ret;
}

static ENGINE_ERROR_CODE dcp_message_system_event(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        uint16_t vbucket,
        mcbp::systemevent::id event,
        uint64_t bySeqno,
        cb::const_byte_buffer key,
        cb::const_byte_buffer eventData) {
    auto* c = cookie2mcbp(cookie, __func__);

    protocol_binary_request_dcp_system_event packet(
            opaque,
            vbucket,
            gsl::narrow<uint16_t>(key.size()),
            eventData.size(),
            event,
            bySeqno);

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    c->write->produce([&c, &packet, &key, &eventData, &ret](
                              cb::byte_buffer buffer) -> size_t {
        if (buffer.size() <
            (sizeof(packet.bytes) + key.size() + eventData.size())) {
            ret = ENGINE_E2BIG;
            return 0;
        }

        std::copy(packet.bytes,
                  packet.bytes + sizeof(packet.bytes),
                  buffer.begin());

        std::copy(
                key.begin(), key.end(), buffer.begin() + sizeof(packet.bytes));
        std::copy(eventData.begin(),
                  eventData.end(),
                  buffer.begin() + sizeof(packet.bytes) + key.size());

        c->addIov(buffer.begin(), sizeof(packet.bytes) + key.size() + eventData.size());
        return sizeof(packet.bytes) + key.size() + eventData.size();
    });

    return ret;
}

void ship_dcp_log(Cookie& cookie) {
    static struct dcp_message_producers producers = {
            dcp_message_get_failover_log,
            dcp_message_stream_req,
            dcp_message_add_stream_response,
            dcp_message_marker_response,
            dcp_message_set_vbucket_state_response,
            dcp_message_stream_end,
            dcp_message_marker,
            dcp_message_mutation,
            dcp_message_deletion_v1,
            dcp_message_deletion_v2,
            dcp_message_expiration,
            dcp_message_flush,
            dcp_message_set_vbucket_state,
            dcp_message_noop,
            dcp_message_buffer_acknowledgement,
            dcp_message_control,
            dcp_message_system_event};
    ENGINE_ERROR_CODE ret;

    auto& c = cookie.getConnection();
    c.addMsgHdr(true);
    cookie.setEwouldblock(false);
    ret = c.getBucketEngine()->dcp.step(
            c.getBucketEngineAsV0(),
            static_cast<const void*>(&c.getCookieObject()),
            &producers);
    if (ret == ENGINE_SUCCESS) {
        /* the engine don't have more data to send at this moment */
        cookie.setEwouldblock(true);
    } else if (ret == ENGINE_WANT_MORE) {
        /* The engine got more data it wants to send */
        ret = ENGINE_SUCCESS;
        c.setState(McbpStateMachine::State::send_data);
        c.setWriteAndGo(McbpStateMachine::State::ship_log);
    }

    if (ret != ENGINE_SUCCESS) {
        c.setState(McbpStateMachine::State::closing);
    }
}
