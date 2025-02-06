/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "getex_context.h"

#include "engine_wrapper.h"
#include "item_dissector.h"
#include <daemon/buckets.h>
#include <daemon/mcaudit.h>
#include <daemon/memcached.h>
#include <daemon/sendbuffer.h>
#include <folly/io/IOBuf.h>
#include <xattr/blob.h>

GetExCommandContext::GetExCommandContext(Cookie& cookie)
    : SteppableCommandContext(cookie),
      vbucket(cookie.getRequest().getVBucket()) {
}

cb::engine_errc GetExCommandContext::getItem() {
    cb::EngineErrorItemPair ret;
    if (cookie.getRequest().getClientOpcode() ==
        cb::mcbp::ClientOpcode::GetEx) {
        ret = bucket_get(cookie, cookie.getRequestKey(), vbucket);
    } else {
        ret = bucket_get_replica(
                cookie, cookie.getRequestKey(), vbucket, DocStateFilter::Alive);
    }
    auto status = ret.first;
    if (status == cb::engine_errc::success) {
        STATS_HIT(&connection, get);
        item = std::move(ret.second);
        state = State::SendResponse;
        datatype = item->getDataType();
        flags = item->getFlags();
        cookie.setCas(item->getCas());
        if (cb::mcbp::datatype::is_xattr(datatype)) {
            // The document contains xattrs. Check if we've got access to
            // the system xattrs
            auto [sid, cid] = cookie.getScopeAndCollection();
            if (cookie.testPrivilege(
                              cb::rbac::Privilege::SystemXattrRead, sid, cid)
                        .failed()) {
                // We need to inspect the item to strip xattrs
                state = State::MaybeStripXattrs;
            }
        }
        cb::audit::document::add(cookie,
                                 cb::audit::document::Operation::Read,
                                 item->getDocKey());
    } else if (status == cb::engine_errc::no_such_key) {
        STATS_MISS(&connection, get);
    }
    return status;
}

void GetExCommandContext::maybeStripXattrs() {
    // the user don't have access to system privileges.. we need
    // to strip them
    auto item_dissector =
            std::make_unique<ItemDissector>(cookie, std::move(item), false);
    cb::xattr::Blob blob;
    blob.assign(item_dissector->getExtendedAttributes(), false);
    if (blob.get_system_size() == 0) {
        // The document does not contain any system xattrs
        item = item_dissector->takeItem();
        return;
    }

    // We need to rebuild a new item
    datatype &= ~PROTOCOL_BINARY_DATATYPE_SNAPPY;
    blob.prune_system_keys();
    if (blob.finalize().empty()) {
        datatype &= ~PROTOCOL_BINARY_DATATYPE_XATTR;
        value_copy = item_dissector->getValue();
        return;
    }

    std::string value;
    auto entry = blob.finalize();
    std::ranges::copy(entry, std::back_inserter(value));
    auto val = item_dissector->getValue();
    std::ranges::copy(val, std::back_inserter(value));
    value_copy = std::move(value);
}

void GetExCommandContext::sendResponse() {
    if (value_copy.has_value()) {
        connection.sendResponse(
                cookie,
                cb::mcbp::Status::Success,
                {reinterpret_cast<const char*>(&flags), sizeof(flags)},
                {},
                *value_copy,
                datatype,
                {});
    } else {
        std::unique_ptr<SendBuffer> sendbuffer;
        auto value = item->getValueView();
        if (value.size() > SendBuffer::MinimumDataSize) {
            sendbuffer =
                    std::make_unique<ItemSendBuffer>(std::move(item),
                                                     item->getValueView(),
                                                     connection.getBucket());
        }
        connection.sendResponse(
                cookie,
                cb::mcbp::Status::Success,
                {reinterpret_cast<const char*>(&flags), sizeof(flags)},
                {},
                value,
                datatype,
                std::move(sendbuffer));
    }
}

cb::engine_errc GetExCommandContext::step() {
    auto ret = cb::engine_errc::success;
    do {
        switch (state) {
        case State::GetItem:
            ret = getItem();
            break;
        case State::MaybeStripXattrs:
            maybeStripXattrs();
            state = State::SendResponse;
            break;
        case State::SendResponse:
            sendResponse();
            state = State::Done;
            break;
        case State::Done:
            return cb::engine_errc::success;
        }
    } while (ret == cb::engine_errc::success);

    return ret;
}
