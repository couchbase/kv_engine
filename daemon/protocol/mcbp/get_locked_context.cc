/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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

cb::engine_errc GetLockedCommandContext::getAndLockItem() {
    auto ret = bucket_get_locked(
            cookie, cookie.getRequestKey(), vbucket, lock_timeout);
    if (ret.first == cb::engine_errc::success) {
        it = std::move(ret.second);
        if (!bucket_get_item_info(connection, it.get(), &info)) {
            LOG_WARNING(
                    "{}: GetLockedCommandContext::"
                    "getAndLockItem Failed to get item info",
                    connection.getId());
            return cb::engine_errc::failed;
        }

        payload = {static_cast<const char*>(info.value[0].iov_base),
                   info.value[0].iov_len};

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
        return cb::engine_errc::locked_tmpfail;
    }

    return cb::engine_errc(ret.first);
}

cb::engine_errc GetLockedCommandContext::inflateItem() {
    try {
        if (!cookie.inflateSnappy(payload, buffer)) {
            LOG_WARNING(
                    "{}: GetLockedCommandContext::inflateItem:"
                    " Failed to inflate item",
                    connection.getId());
            return cb::engine_errc::failed;
        }
        payload = buffer;
    } catch (const std::bad_alloc&) {
        return cb::engine_errc::no_memory;
    }

    state = State::SendResponse;
    return cb::engine_errc::success;
}

cb::engine_errc GetLockedCommandContext::sendResponse() {
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
    return cb::engine_errc::success;
}

cb::engine_errc GetLockedCommandContext::step() {
    auto ret = cb::engine_errc::success;
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
            return cb::engine_errc::success;
        }
    } while (ret == cb::engine_errc::success);

    return ret;
}
