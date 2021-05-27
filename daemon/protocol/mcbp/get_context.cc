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
#include "get_context.h"

#include "engine_wrapper.h"

#include <daemon/buckets.h>
#include <daemon/debug_helpers.h>
#include <daemon/mcaudit.h>
#include <daemon/memcached.h>
#include <daemon/sendbuffer.h>
#include <gsl/gsl-lite.hpp>
#include <logger/logger.h>
#include <xattr/utils.h>

cb::engine_errc GetCommandContext::getItem() {
    const auto key = cookie.getRequestKey();
    auto ret = bucket_get(cookie, key, vbucket);
    if (ret.first == cb::engine_errc::success) {
        it = std::move(ret.second);
        if (!bucket_get_item_info(connection, it.get(), &info)) {
            LOG_WARNING("{}: Failed to get item info", connection.getId());
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
    } else if (ret.first == cb::engine_errc::no_such_key) {
        state = State::NoSuchItem;
        ret.first = cb::engine_errc::success;
    }

    return cb::engine_errc(ret.first);
}

cb::engine_errc GetCommandContext::inflateItem() {
    try {
        if (!cookie.inflateSnappy(payload, buffer)) {
            LOG_WARNING("{}: Failed to inflate item", connection.getId());
            return cb::engine_errc::failed;
        }
        payload = buffer;
        info.datatype &= ~PROTOCOL_BINARY_DATATYPE_SNAPPY;
    } catch (const std::bad_alloc&) {
        return cb::engine_errc::no_memory;
    }

    state = State::SendResponse;
    return cb::engine_errc::success;
}

cb::engine_errc GetCommandContext::sendResponse() {
    if (mcbp::datatype::is_xattr(info.datatype)) {
        payload = cb::xattr::get_body(payload);
        info.datatype &= ~PROTOCOL_BINARY_DATATYPE_XATTR;
    }

    info.datatype = connection.getEnabledDatatypes(info.datatype);

    std::size_t keylen = 0;
    auto key = info.key;

    if (shouldSendKey()) {
        // Client doesn't support collection-ID in the key
        if (!connection.isCollectionsSupported()) {
            key = key.makeDocKeyWithoutCollectionID();
        }
        keylen = key.size();
    }

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
            {reinterpret_cast<const char*>(key.data()), keylen},
            payload,
            info.datatype,
            std::move(sendbuffer));

    cb::audit::document::add(cookie, cb::audit::document::Operation::Read);

    STATS_HIT(&connection, get);
    update_topkeys(cookie);

    state = State::Done;
    return cb::engine_errc::success;
}

cb::engine_errc GetCommandContext::noSuchItem() {
    STATS_MISS(&connection, get);

    const auto key = cookie.getRequestKey();

    if (cookie.getRequest().isQuiet()) {
        ++connection.getBucket()
                  .responseCounters[int(cb::mcbp::Status::KeyEnoent)];
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
    return cb::engine_errc::success;
}

cb::engine_errc GetCommandContext::step() {
    auto ret = cb::engine_errc::success;
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
            return cb::engine_errc::success;
        }
    } while (ret == cb::engine_errc::success);

    return ret;
}
