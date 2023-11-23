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
#include "item_dissector.h"
#include <daemon/buckets.h>
#include <daemon/cookie.h>
#include <daemon/mcaudit.h>
#include <daemon/sendbuffer.h>
#include <folly/io/IOBuf.h>
#include <memcached/durability_spec.h>
#include <xattr/utils.h>

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
        item_dissector = std::make_unique<ItemDissector>(
                cookie, std::move(ret.second), !connection.isSnappyEnabled());
        state = State::SendResponse;
    } else if (ret.first == cb::engine_errc::no_such_key) {
        state = State::NoSuchItem;
        ret.first = cb::engine_errc::success;
    }

    return ret.first;
}

cb::engine_errc GatCommandContext::sendResponse() {
    STATS_HIT(&connection, get);

    // Set the CAS to add into the header
    const auto& document = item_dissector->getItem();
    cookie.setCas(document.getCas());

    // Audit the modification to the document (change of EXP)
    cb::audit::document::add(cookie,
                             cb::audit::document::Operation::Modify,
                             cookie.getRequestKey());

    if (cookie.getHeader().getRequest().getClientOpcode() ==
        cb::mcbp::ClientOpcode::Touch) {
        cookie.sendResponse(cb::mcbp::Status::Success);
        state = State::Done;
        return cb::engine_errc::success;
    }

    // This is GAT[Q]
    const auto datatype =
            connection.getEnabledDatatypes(item_dissector->getDatatype());

    auto value = item_dissector->getValue();
    const auto flags = document.getFlags();

    connection.sendResponse(
            cookie,
            cb::mcbp::Status::Success,
            {reinterpret_cast<const char*>(&flags), sizeof(flags)},
            {}, // no key
            value,
            datatype,
            value.size() > SendBuffer::MinimumDataSize
                    ? item_dissector->takeSendBuffer(value,
                                                     connection.getBucket())
                    : std::unique_ptr<SendBuffer>{});

    cb::audit::document::add(cookie,
                             cb::audit::document::Operation::Read,
                             cookie.getRequestKey());
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
        case State::SendResponse:
            ret = sendResponse();
            break;
        case State::Done:
            return cb::engine_errc::success;
        }
    } while (ret == cb::engine_errc::success);

    return ret;
}
