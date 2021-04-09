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
#include "gat_context.h"

#include "engine_wrapper.h"

#include <daemon/buckets.h>
#include <daemon/cookie.h>
#include <daemon/mcaudit.h>
#include <daemon/sendbuffer.h>
#include <logger/logger.h>
#include <memcached/durability_spec.h>
#include <xattr/utils.h>
#include <gsl/gsl>

uint32_t GatCommandContext::getExptime(Cookie& cookie) {
    auto extras = cookie.getRequest().getExtdata();
    if (extras.size() != sizeof(uint32_t)) {
        throw std::invalid_argument("GatCommandContext: Invalid extdata size");
    }
    using cb::mcbp::request::GatPayload;
    return reinterpret_cast<const GatPayload*>(extras.data())->getExpiration();
}

GatCommandContext::GatCommandContext(Cookie& cookie)
    : SteppableCommandContext(cookie),
      vbucket(cookie.getRequest().getVBucket()),
      exptime(getExptime(cookie)),
      info{},
      state(State::GetAndTouchItem) {
}

cb::engine_errc GatCommandContext::getAndTouchItem() {
    auto ret = bucket_get_and_touch(
            cookie,
            cookie.getRequestKey(),
            vbucket,
            exptime,
            cookie.getRequest().getDurabilityRequirements());
    if (ret.first == cb::engine_errc::success) {
        it = std::move(ret.second);
        if (!bucket_get_item_info(connection, it.get(), &info)) {
            LOG_WARNING("{}: Failed to get item info", connection.getId());
            return cb::engine_errc::failed;
        }

        // Set the cas value (will be copied to the response message)
        cookie.setCas(info.cas);
        payload = {static_cast<const char*>(info.value[0].iov_base),
                   info.value[0].iov_len};

        bool need_inflate = false;
        if (mcbp::datatype::is_snappy(info.datatype) &&
            cookie.getHeader().getRequest().getClientOpcode() !=
                    cb::mcbp::ClientOpcode::Touch) {
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

cb::engine_errc GatCommandContext::inflateItem() {
    try {
        if (!cookie.inflateSnappy(payload, buffer)) {
            LOG_WARNING("{}: Failed to inflate item", connection.getId());
            return cb::engine_errc::failed;
        }
        payload = buffer;
    } catch (const std::bad_alloc&) {
        return cb::engine_errc::no_memory;
    }

    state = State::SendResponse;
    return cb::engine_errc::success;
}

cb::engine_errc GatCommandContext::sendResponse() {
    STATS_HIT(&connection, get);
    update_topkeys(cookie);

    // Audit the modification to the document (change of EXP)
    cb::audit::document::add(cookie, cb::audit::document::Operation::Modify);

    if (cookie.getHeader().getRequest().getClientOpcode() ==
        cb::mcbp::ClientOpcode::Touch) {
        cookie.sendResponse(cb::mcbp::Status::Success);
        state = State::Done;
        return cb::engine_errc::success;
    }

    // This is GAT[Q]
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
            {}, // no key
            payload,
            datatype,
            std::move(sendbuffer));

    cb::audit::document::add(cookie, cb::audit::document::Operation::Read);
    state = State::Done;
    return cb::engine_errc::success;
}

cb::engine_errc GatCommandContext::noSuchItem() {
    STATS_MISS(&connection, get);
    if (cookie.getRequest().isQuiet()) {
        ++connection.getBucket()
                    .responseCounters[int(cb::mcbp::Status::KeyEnoent)];
    } else {
        cookie.sendResponse(cb::mcbp::Status::KeyEnoent);
    }

    state = State::Done;
    return cb::engine_errc::success;
}

cb::engine_errc GatCommandContext::step() {
    auto ret = cb::engine_errc::success;
    do {
        switch (state) {
        case State::GetAndTouchItem:
            ret = getAndTouchItem();
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
