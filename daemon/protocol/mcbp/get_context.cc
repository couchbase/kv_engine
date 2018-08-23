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

#include "engine_wrapper.h"

#include <daemon/buckets.h>
#include <daemon/debug_helpers.h>
#include <daemon/mcaudit.h>
#include <daemon/mcbp.h>
#include <daemon/memcached.h>
#include <xattr/utils.h>
#include <gsl/gsl>

ENGINE_ERROR_CODE GetCommandContext::getItem() {
    const auto key = cookie.getRequestKey();
    auto ret = bucket_get(cookie, key, vbucket);
    if (ret.first == cb::engine_errc::success) {
        it = std::move(ret.second);
        if (!bucket_get_item_info(cookie, it.get(), &info)) {
            LOG_WARNING("{}: Failed to get item info", connection.getId());
            return ENGINE_FAILED;
        }

        payload.buf = static_cast<const char*>(info.value[0].iov_base);
        payload.len = info.value[0].iov_len;

        bool need_inflate = false;
        if (mcbp::datatype::is_snappy(info.datatype)) {
            need_inflate = mcbp::datatype::is_xattr(info.datatype) ||
                           !connection.isSnappyEnabled();
        }

        if (need_inflate) {
            state = State::InflateItem;
        } else {
            state = State::SendResponse;
        }
    } else if (ret.first == cb::engine_errc::no_such_key) {
        state = State::NoSuchItem;
        ret.first = cb::engine_errc::success;
    }

    return ENGINE_ERROR_CODE(ret.first);
}

ENGINE_ERROR_CODE GetCommandContext::inflateItem() {
    try {
        if (!cb::compression::inflate(cb::compression::Algorithm::Snappy,
                                      payload, buffer)) {
            LOG_WARNING("{}: Failed to inflate item", connection.getId());
            return ENGINE_FAILED;
        }
        payload = buffer;
    } catch (const std::bad_alloc&) {
        return ENGINE_ENOMEM;
    }

    state = State::SendResponse;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE GetCommandContext::sendResponse() {
    protocol_binary_datatype_t datatype = info.datatype;

    if (mcbp::datatype::is_xattr(datatype)) {
        payload = cb::xattr::get_body(payload);
        datatype &= ~PROTOCOL_BINARY_DATATYPE_XATTR;
    }

    datatype = connection.getEnabledDatatypes(datatype);

    uint16_t keylen = 0;
    uint32_t bodylen = gsl::narrow<uint32_t>(sizeof(info.flags) + payload.len);
    auto key = info.key;

    if (shouldSendKey()) {
        // Client doesn't support collection-ID in the key
        if (!connection.isCollectionsSupported()) {
            key = key.makeDocKeyWithoutCollectionID();
        }
        keylen = gsl::narrow<uint16_t>(key.size());
        bodylen += keylen;
    }

    // Set the CAS to add into the header
    cookie.setCas(info.cas);
    mcbp_add_header(cookie,
                    PROTOCOL_BINARY_RESPONSE_SUCCESS,
                    sizeof(info.flags),
                    keylen,
                    bodylen,
                    datatype);

    // Add the flags
    connection.addIov(&info.flags, sizeof(info.flags));

    // Add the value
    if (shouldSendKey()) {
        connection.addIov(key.data(), key.size());
    }

    connection.addIov(payload.buf, payload.len);
    connection.setState(StateMachine::State::send_data);
    cb::audit::document::add(cookie, cb::audit::document::Operation::Read);

    STATS_HIT(&connection, get);
    update_topkeys(cookie);

    state = State::Done;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE GetCommandContext::noSuchItem() {
    STATS_MISS(&connection, get);

    const auto key = cookie.getRequestKey();

    if (cookie.getRequest().isQuiet()) {
        ++connection.getBucket()
                  .responseCounters[PROTOCOL_BINARY_RESPONSE_KEY_ENOENT];
        connection.setState(StateMachine::State::new_cmd);
    } else {
        if (shouldSendKey()) {
            cookie.sendResponse(
                    cb::mcbp::Status::KeyEnoent,
                    {},
                    {reinterpret_cast<const char*>(key.data()), key.size()},
                    {},
                    cb::mcbp::Datatype::Raw,
                    0);
        } else {
            cookie.sendResponse(cb::mcbp::Status::KeyEnoent);
        }
    }

    state = State::Done;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE GetCommandContext::step() {
    auto ret = ENGINE_SUCCESS;
    do {
        switch (state) {
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
