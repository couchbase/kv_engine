/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "get_meta_context.h"

#include "engine_wrapper.h"

#include <daemon/buckets.h>
#include <daemon/cookie.h>
#include <daemon/mcaudit.h>
#include <daemon/memcached.h>
#include <xattr/utils.h>

GetMetaCommandContext::GetMetaCommandContext(Cookie& cookie)
    : SteppableCommandContext(cookie),
      vbucket(cookie.getRequest().getVBucket()),
      state(State::GetItemMeta) {
    auto extras = cookie.getRequest().getExtdata();
    // Read the version if extlen is 1
    if (extras.size() == 1 && *extras.data() == uint8_t(GetMetaVersion::V2)) {
        version = GetMetaVersion::V2;
    }
}

cb::engine_errc GetMetaCommandContext::getItemMeta() {
    auto errorMetaPair =
            bucket_get_meta(cookie, cookie.getRequestKey(), vbucket);
    if (errorMetaPair.first == cb::engine_errc::success) {
        info = errorMetaPair.second;
        state = State::SendResponse;
    } else if (errorMetaPair.first == cb::engine_errc::no_such_key) {
        state = State::NoSuchItem;
        errorMetaPair.first = cb::engine_errc::success;
    }

    return errorMetaPair.first;
}

cb::engine_errc GetMetaCommandContext::sendResponse() {
    // Prepare response
    uint32_t deleted = info.document_state == DocumentState::Deleted ? 1 : 0;
    GetMetaPayload metaResponse(deleted,
                                info.flags,
                                gsl::narrow<uint32_t>(info.exptime),
                                info.seqno,
                                info.datatype);

    const size_t responseExtlen = version == GetMetaVersion::V1
                                          ? GetMetaPayloadV1Size
                                          : GetMetaPayloadV2Size;
    std::string_view extras = {reinterpret_cast<const char*>(&metaResponse),
                               responseExtlen};
    cookie.sendResponse(cb::mcbp::Status::Success,
                        extras,
                        {},
                        {},
                        cb::mcbp::Datatype::Raw,
                        info.cas);
    cb::audit::document::add(cookie,
                             cb::audit::document::Operation::Read,
                             cookie.getRequestKey());

    state = State::Done;
    return cb::engine_errc::success;
}

cb::engine_errc GetMetaCommandContext::noSuchItem() {
    if (cookie.getRequest().isQuiet()) {
        auto& bucket = connection.getBucket();
        bucket.responseCounters[int(cb::mcbp::Status::KeyEnoent)]++;
    } else {
        auto& req = cookie.getRequest();
        if (req.getClientOpcode() != cb::mcbp::ClientOpcode::GetqMeta) {
            cookie.sendResponse(cb::mcbp::Status::KeyEnoent);
        }
    }

    state = State::Done;
    return cb::engine_errc::success;
}

cb::engine_errc GetMetaCommandContext::step() {
    auto ret = cb::engine_errc::success;
    do {
        switch (state) {
        case State::GetItemMeta:
            ret = getItemMeta();
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
