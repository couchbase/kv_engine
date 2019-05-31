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

#include "get_locked_context.h"
#include "engine_wrapper.h"

#include <daemon/debug_helpers.h>
#include <daemon/memcached.h>
#include <daemon/sendbuffer.h>
#include <daemon/stats.h>
#include <logger/logger.h>
#include <xattr/utils.h>
#include <gsl/gsl>

ENGINE_ERROR_CODE GetLockedCommandContext::getAndLockItem() {
    auto ret = bucket_get_locked(
            cookie, cookie.getRequestKey(), vbucket, lock_timeout);
    if (ret.first == cb::engine_errc::success) {
        it = std::move(ret.second);
        if (!bucket_get_item_info(connection, it.get(), &info)) {
            LOG_WARNING(
                    "{}: GetLockedCommandContext::"
                    "getAndLockItem Failed to get item info",
                    connection.getId());
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
    } else if (ret.first == cb::engine_errc::locked) {
        // In order to be backward compatible we should return TMPFAIL
        // instead of the more correct EEXISTS
        return ENGINE_LOCKED_TMPFAIL;
    }

    return ENGINE_ERROR_CODE(ret.first);
}

ENGINE_ERROR_CODE GetLockedCommandContext::inflateItem() {
    try {
        if (!cb::compression::inflate(cb::compression::Algorithm::Snappy,
                                      payload, buffer)) {
            LOG_WARNING(
                    "{}: GetLockedCommandContext::inflateItem:"
                    " Failed to inflate item",
                    connection.getId());
            return ENGINE_FAILED;
        }
        payload = buffer;
    } catch (const std::bad_alloc&) {
        return ENGINE_ENOMEM;
    }

    state = State::SendResponse;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE GetLockedCommandContext::sendResponse() {
    protocol_binary_datatype_t datatype = info.datatype;

    if (mcbp::datatype::is_xattr(datatype)) {
        payload = cb::xattr::get_body(payload);
        datatype &= ~PROTOCOL_BINARY_DATATYPE_XATTR;
    }

    datatype = connection.getEnabledDatatypes(datatype);

    // Set the CAS to add into the header
    cookie.setCas(info.cas);

    std::unique_ptr<SendBuffer> sendbuffer;
    if (payload.size() > SendBuffer::MinimumDataSize) {
        // we may use the item if we've didn't inflate it
        if (buffer.empty()) {
            sendbuffer = std::make_unique<ItemSendBuffer>(
                    std::move(it), payload, connection.getBucket());
        } else {
            sendbuffer =
                    std::make_unique<CompressionSendBuffer>(buffer, payload);
        }
    }

    connection.sendResponse(
            cookie,
            cb::mcbp::Status::Success,
            {reinterpret_cast<const char*>(&info.flags), sizeof(info.flags)},
            {},
            payload,
            datatype,
            std::move(sendbuffer));

    STATS_INCR(&connection, cmd_lock);
    update_topkeys(cookie);

    state = State::Done;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE GetLockedCommandContext::step() {
    auto ret = ENGINE_SUCCESS;
    do {
        switch (state) {
        case State::GetAndLockItem:
            ret = getAndLockItem();
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
