/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <mcbp/protocol/unsigned_leb128.h>
#include <memcached/dockey.h>
#include <memcached/systemevent.h>
#include <platform/socket.h>
#include <spdlog/fmt/fmt.h>
#include <ostream>
#include <sstream>
#include <string_view>

bool CollectionID::isReserved(CollectionIDType value) {
    return value >= System && value <= Reserved7;
}

bool operator==(CollectionIDType lhs, const CollectionID& rhs) {
    return lhs == uint32_t(rhs);
}

std::ostream& operator<<(std::ostream& os, const CollectionID& cid) {
    return os << cid.to_string();
}

std::string CollectionID::to_string() const {
    return fmt::format("{:#x}", value);
}

bool operator==(ScopeIDType lhs, const ScopeID& rhs) {
    return lhs == uint32_t(rhs);
}

std::ostream& operator<<(std::ostream& os, const ScopeID& sid) {
    return os << sid.to_string();
}

std::string ScopeID::to_string() const {
    return fmt::format("{:#x}", value);
}

CollectionIDNetworkOrder::CollectionIDNetworkOrder(CollectionID v)
    : value(htonl(uint32_t(v))) {
}

CollectionID CollectionIDNetworkOrder::to_host() const {
    return CollectionID(ntohl(value));
}

ScopeIDNetworkOrder::ScopeIDNetworkOrder(ScopeID v)
    : value(htonl(uint32_t(v))) {
}

ScopeID ScopeIDNetworkOrder::to_host() const {
    return ScopeID(ntohl(value));
}

std::string DocKey::to_string() const {
    // Get the sid of the key and add it to the string
    auto [cid, key] = cb::mcbp::unsigned_leb128<CollectionIDType>::decode(
            {data(), size()});

    std::string_view remainingKey{reinterpret_cast<const char*>(key.data()),
                                  key.size()};
    if (getCollectionID().isSystem()) {
        // Get the hex value of the type of system event
        auto [systemEventID, keyWithoutEvent] =
                cb::mcbp::unsigned_leb128<uint32_t>::decode(
                        {key.data(), key.size()});
        // Get the cid or sid of the system event is for
        auto [cidOrSid, keySuffix] =
                cb::mcbp::unsigned_leb128<uint32_t>::decode(
                        {keyWithoutEvent.data(), keyWithoutEvent.size()});
        if (systemEventID == uint32_t(SystemEvent::Collection) ||
            systemEventID == uint32_t(SystemEvent::Scope)) {
            // Get the string view to the remaining string part of the key
            remainingKey = {reinterpret_cast<const char*>(keySuffix.data()),
                            keySuffix.size()};
            return fmt::format(fmt("cid:{:#x}:{:#x}:{:#x}:{}"),
                               cid,
                               systemEventID,
                               cidOrSid,
                               remainingKey);
        }
    }
    return fmt::format(fmt("cid:{:#x}:{}"), cid, remainingKey);
}

CollectionID DocKey::getCollectionID() const {
    if (encoding == DocKeyEncodesCollectionId::Yes) {
        auto cid = cb::mcbp::unsigned_leb128<CollectionIDType>::decode(buffer)
                           .first;
        return cid;
    }
    return CollectionID::Default;
}

// Inspect the leb128 key prefixes without doing a full leb decode
bool DocKey::isInSystemCollection() const {
    if (encoding == DocKeyEncodesCollectionId::Yes) {
        // Note: when encoding == Yes the size is 2 bytes at a minimum. This is
        // as mcbp_validators will fail 0-byte logical keys, thus we always have
        // at least 1 byte of collection and 1 byte of key so size() checks are
        // not needed. System keys additionally never have empty logical keys.
        // Finally we do not need to consider the Prepared namespace as system
        // events are never created in that way.
        return data()[0] == CollectionID::System;
    }
    return false;
}

// Inspect the leb128 key prefixes without doing a full leb decode
bool DocKey::isInDefaultCollection() const {
    if (encoding == DocKeyEncodesCollectionId::Yes) {
        return data()[0] == CollectionID::Default;
    }
    return true;
}

std::pair<CollectionID, cb::const_byte_buffer> DocKey::getIdAndKey() const {
    if (encoding == DocKeyEncodesCollectionId::Yes) {
        return cb::mcbp::unsigned_leb128<CollectionIDType>::decode(buffer);
    }
    return {CollectionID::Default, {data(), size()}};
}

DocKey DocKey::makeDocKeyWithoutCollectionID() const {
    if (getEncoding() == DocKeyEncodesCollectionId::Yes) {
        auto decoded = cb::mcbp::skip_unsigned_leb128<CollectionIDType>(buffer);
        return {decoded.data(), decoded.size(), DocKeyEncodesCollectionId::No};
    }
    return *this;
}
