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
#include "item_dissector.h"
#include <daemon/memcached.h>
#include <daemon/sendbuffer.h>
#include <daemon/stats.h>
#include <folly/io/IOBuf.h>
#include <xattr/utils.h>

uint32_t GetLockedCommandContext::get_exptime(
        const cb::mcbp::Request& request) {
    auto extras = request.getExtdata();
    if (extras.empty()) {
        return 0;
    }

    if (extras.size() != sizeof(uint32_t)) {
        throw std::invalid_argument(
                "GetLockedCommandContext: Invalid extdata size");
    }

    const auto* exp = reinterpret_cast<const uint32_t*>(extras.data());
    return ntohl(*exp);
}

GetLockedCommandContext::GetLockedCommandContext(Cookie& cookie)
    : SteppableCommandContext(cookie),
      vbucket(cookie.getRequest().getVBucket()),
      lock_timeout(get_exptime(cookie.getRequest())),
      state(State::GetAndLockItem) {
}

cb::engine_errc GetLockedCommandContext::getAndLockItem() {
    auto ret = bucket_get_locked(
            cookie, cookie.getRequestKey(), vbucket, lock_timeout);
    if (ret.first == cb::engine_errc::success) {
        item_dissector = std::make_unique<ItemDissector>(
                cookie, std::move(ret.second), !connection.isSnappyEnabled());
        state = State::SendResponse;
    } else if (ret.first == cb::engine_errc::locked) {
        // In order to be backward compatible we should return TMPFAIL
        // instead of the more correct EEXISTS
        return cb::engine_errc::locked_tmpfail;
    }

    return ret.first;
}

cb::engine_errc GetLockedCommandContext::sendResponse() {
    STATS_INCR(&connection, cmd_lock);
    const auto datatype =
            connection.getEnabledDatatypes(item_dissector->getDatatype());

    // Set the CAS to add into the header
    const auto& document = item_dissector->getItem();
    cookie.setCas(document.getCas());

    auto value = item_dissector->getValue();
    const auto flags = document.getFlags();

    connection.sendResponse(
            cookie,
            cb::mcbp::Status::Success,
            {reinterpret_cast<const char*>(&flags), sizeof(flags)},
            {},
            value,
            datatype,
            value.size() > SendBuffer::MinimumDataSize
                    ? item_dissector->takeSendBuffer(value,
                                                     connection.getBucket())
                    : std::unique_ptr<SendBuffer>{});

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
        case State::SendResponse:
            ret = sendResponse();
            break;
        case State::Done:
            return cb::engine_errc::success;
        }
    } while (ret == cb::engine_errc::success);

    return ret;
}
