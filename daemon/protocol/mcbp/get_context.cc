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
#include "item_dissector.h"
#include <daemon/buckets.h>
#include <daemon/mcaudit.h>
#include <daemon/memcached.h>
#include <daemon/sendbuffer.h>
#include <folly/io/IOBuf.h>
#include <xattr/utils.h>

GetCommandContext::GetCommandContext(Cookie& cookie)
    : SteppableCommandContext(cookie),
      vbucket(cookie.getRequest().getVBucket()),
      opcode(cookie.getRequest().getClientOpcode()),
      state(State::GetItem) {
}

cb::engine_errc GetCommandContext::getItem() {
    const auto& req = cookie.getRequest();
    const auto opcode = req.getClientOpcode();
    cb::EngineErrorItemPair ret;
    if (opcode == cb::mcbp::ClientOpcode::GetReplica) {
        ret = bucket_get_replica(
                cookie, cookie.getRequestKey(), vbucket, DocStateFilter::Alive);
    } else if (opcode == cb::mcbp::ClientOpcode::GetRandomKey) {
        CollectionID cid{CollectionID::Default};
        if (req.getExtlen()) {
            const auto& specifics = req.getCommandSpecifics<
                    cb::mcbp::request::GetRandomKeyPayload>();
            cid = specifics.getCollectionId();
        }
        ret = bucket_get_random_document(cookie, cid);
    } else {
        ret = bucket_get(cookie, cookie.getRequestKey(), vbucket);
    }
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

void GetCommandContext::sendResponse() {
    const auto datatype =
            connection.getEnabledDatatypes(item_dissector->getDatatype());

    const auto& document = item_dissector->getItem();

    std::size_t keylen = 0;
    auto key = document.getDocKey();

    if (shouldSendKey() || cookie.getRequest().getClientOpcode() ==
                                   cb::mcbp::ClientOpcode::GetRandomKey) {
        // Client doesn't support collection-ID in the key
        if (!connection.isCollectionsSupported()) {
            key = key.makeDocKeyWithoutCollectionID();
        }
        keylen = key.size();
    }

    // Set the CAS to add into the header
    cookie.setCas(document.getCas());

    cb::audit::document::add(
            cookie, cb::audit::document::Operation::Read, document.getDocKey());

    auto value = item_dissector->getValue();
    const auto flags = document.getFlags();

    connection.sendResponse(
            cookie,
            cb::mcbp::Status::Success,
            {reinterpret_cast<const char*>(&flags), sizeof(flags)},
            {reinterpret_cast<const char*>(key.data()), keylen},
            value,
            datatype,
            value.size() > SendBuffer::MinimumDataSize
                    ? item_dissector->takeSendBuffer(value,
                                                     connection.getBucket())
                    : std::unique_ptr<SendBuffer>{});

    STATS_HIT(&connection, get);
}

void GetCommandContext::sendGetRandomKeyResponse() {
    const auto& req = cookie.getRequest();
    if (item_dissector->getExtendedAttributes().empty() ||
        req.getExtlen() < sizeof(cb::mcbp::request::GetRandomKeyPayloadV2) ||
        !req.getCommandSpecifics<cb::mcbp::request::GetRandomKeyPayloadV2>()
                 .isIncludeXattr()) {
        // No xattrs or
        // The request don't include GetRandomKeyPayloadV2, or
        // The client didn't request xattrs to be included
        sendResponse();
        return;
    }

    const auto& document = item_dissector->getItem();
    const auto key = document.getDocKey();
    // Set the CAS to add into the header
    cookie.setCas(document.getCas());
    const auto flags = document.getFlags();
    connection.sendResponse(
            cookie,
            cb::mcbp::Status::Success,
            {reinterpret_cast<const char*>(&flags), sizeof(flags)},
            {reinterpret_cast<const char*>(key.data()), key.size()},
            document.getValueView(),
            document.getDataType(),
            {});

    cb::audit::document::add(
            cookie, cb::audit::document::Operation::Read, document.getDocKey());
    STATS_HIT(&connection, get);
}

cb::engine_errc GetCommandContext::noSuchItem() {
    STATS_MISS(&connection, get);

    if (cookie.getRequest().isQuiet()) {
        ++connection.getBucket()
                  .responseCounters[int(cb::mcbp::Status::KeyEnoent)];
    } else {
        if (shouldSendKey()) {
            const auto key = cookie.getRequestKey();
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
        case State::SendResponse:
            if (opcode == cb::mcbp::ClientOpcode::GetRandomKey) {
                sendGetRandomKeyResponse();
            } else {
                sendResponse();
            }
            state = State::Done;
            ret = cb::engine_errc::success;
            break;
        case State::Done:
            return cb::engine_errc::success;
        }
    } while (ret == cb::engine_errc::success);

    return ret;
}
