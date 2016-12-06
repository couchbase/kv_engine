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
#include "get_context.h"

#include <daemon/debug_helpers.h>
#include <daemon/mcbp.h>
#include <daemon/xattr_utils.h>

GetCommandContext::~GetCommandContext() {
    if (it != nullptr) {
        bucket_release_item(&connection, it);
    }
}

ENGINE_ERROR_CODE GetCommandContext::initialize() {
    if (settings.getVerbose() > 1) {
        char buffer[1024];
        if (key_to_printable_buffer(buffer, sizeof(buffer),
                                    connection.getId(), true,
                                    "GET",
                                    reinterpret_cast<const char*>(key.data()),
                                    key.size()) != -1) {
            LOG_DEBUG(&connection, "%s", buffer);
        }
    }
    state = State::GetItem;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE GetCommandContext::getItem() {
    auto ret = bucket_get(&connection, &it, key, vbucket);
    if (ret == ENGINE_SUCCESS) {
        info.info.nvalue = 1;
        if (!bucket_get_item_info(&connection, it, &info.info)) {
            LOG_WARNING(&connection, "%u: Failed to get item info",
                        connection.getId());
            return ENGINE_FAILED;
        }

        payload.buf = static_cast<const char*>(info.info.value[0].iov_base);
        payload.len = info.info.value[0].iov_len;

        bool need_inflate = false;
        if (mcbp::datatype::is_compressed(info.info.datatype)) {
            need_inflate = mcbp::datatype::is_xattr(info.info.datatype) ||
                           !connection.isSupportsDatatype();
        }

        if (need_inflate) {
            state = State::InflateItem;
        } else {
            state = State::SendResponse;
        }
    } else if (ret == ENGINE_KEY_ENOENT) {
        state = State::NoSuchItem;
        ret = ENGINE_SUCCESS;
    }

    return ret;
}

ENGINE_ERROR_CODE GetCommandContext::inflateItem() {
    try {
        if (!cb::compression::inflate(cb::compression::Algorithm::Snappy,
                                      payload.buf, payload.len, buffer)) {
            LOG_WARNING(&connection, "%u: Failed to inflate item",
                        connection.getId());
            return ENGINE_FAILED;
        }
        payload.buf = buffer.data.get();
        payload.len = buffer.len;
    } catch (const std::bad_alloc&) {
        return ENGINE_ENOMEM;
    }

    state = State::SendResponse;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE GetCommandContext::sendResponse() {
    protocol_binary_datatype_t datatype = info.info.datatype;

    if (mcbp::datatype::is_xattr(info.info.datatype)) {
        payload = cb::xattr::get_body(payload);
        datatype &= ~(PROTOCOL_BINARY_DATATYPE_XATTR);
        datatype &= ~(PROTOCOL_BINARY_DATATYPE_COMPRESSED);
    }

    if (!connection.isSupportsDatatype()) {
        datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

    auto* rsp = reinterpret_cast<protocol_binary_response_get*>(connection.write.buf);

    uint16_t keylen = 0;
    uint32_t bodylen = sizeof(rsp->message.body) + payload.len;

    if (shouldSendKey()) {
        keylen = uint16_t(key.size());
        bodylen += keylen;
    }

    mcbp_add_header(&connection,
                    PROTOCOL_BINARY_RESPONSE_SUCCESS,
                    sizeof(rsp->message.body),
                    keylen, bodylen, datatype);

    rsp->message.header.response.cas = htonll(info.info.cas);
    /* add the flags */
    rsp->message.body.flags = info.info.flags;
    connection.addIov(&rsp->message.body, sizeof(rsp->message.body));

    if (shouldSendKey()) {
        connection.addIov(info.info.key, info.info.nkey);
    }

    connection.addIov(payload.buf, payload.len);
    connection.setState(conn_mwrite);

    STATS_HIT(&connection, get, key.buf, key.len);
    update_topkeys(key, &connection);

    state = State::Done;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE GetCommandContext::noSuchItem() {
    STATS_MISS(&connection, get, key.buf, key.len);

    MEMCACHED_COMMAND_GET(connection.getId(),
                          reinterpret_cast<const char*>(key.data()),
                          int(key.size()), -1, 0);

    if (connection.isNoReply()) {
        connection.setState(conn_new_cmd);
    } else {
        if (shouldSendKey()) {
            mcbp_add_header(&connection,
                            PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
                            0, uint16_t(key.size()),
                            uint32_t(key.size()),
                            PROTOCOL_BINARY_RAW_BYTES);
            connection.addIov(key.data(), key.size());
            connection.setState(conn_mwrite);
        } else {
            mcbp_write_packet(&connection, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
        }
    }

    state = State::Done;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE GetCommandContext::step() {
    ENGINE_ERROR_CODE ret;
    do {
        switch (state) {
        case State::Initialize:
            ret = initialize();
            break;
        case State::GetItem:
            ret = getItem();
            break;
        case State::NoSuchItem:
            ret = noSuchItem();
            break;
        case State::InflateItem:
            ret = inflateItem();
            break;
        case State::SendResponse:
            ret = sendResponse();
            break;
        case State::Done:
            return ENGINE_SUCCESS;
        }
    } while (ret == ENGINE_SUCCESS);

    return ret;
}
