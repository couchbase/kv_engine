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
#include <cctype>
#include <ostream>
#include <sstream>
#include <string_view>

namespace Collections {
size_t makeUid(std::string_view uid, size_t limit) {
    // input is a hex number (no prefix)
    if (uid.empty() || uid.size() > limit) {
        throw std::invalid_argument(
                fmt::format("makeUid: input uid must have size > 0 and <= "
                            "limit:{}, input size:{}",
                            limit,
                            uid.size()));
    }

    // verify that the input characters satisfy isxdigit
    for (auto c : uid) {
        if (!std::isxdigit(c)) {
            throw std::invalid_argument(
                    fmt::format("makeUid: uid:{} contains invalid character {} "
                                "fails isxdigit",
                                uid,
                                c));
        }
    }

    return std::strtoul(uid.data(), nullptr, 16);
}
} // namespace Collections

CollectionID::CollectionID(std::string_view id) {
    try {
        // Assign via CollectionID ctor which validates the value is legal
        *this = CollectionID(
                gsl::narrow<CollectionIDType>(Collections::makeUid(id, 8)));
    } catch (const gsl::narrowing_error& e) {
        throw std::invalid_argument(
                fmt::format("CollectionID::CollectionID:: Cannot narrow id:{} "
                            "to a CollectionID",
                            id));
    }
}

ScopeID::ScopeID(std::string_view id) {
    try {
        *this = ScopeID(
                gsl::narrow<CollectionIDType>(Collections::makeUid(id, 8)));
    } catch (const gsl::narrowing_error& e) {
        throw std::invalid_argument(fmt::format(
                "ScopeID::ScopeID:: Cannot narrow id:{} to a ScopeID", id));
    }
}

bool CollectionID::isReserved(CollectionIDType value) {
    return value >= SystemEvent && value <= Reserved7;
}

bool operator==(CollectionIDType lhs, const CollectionID& rhs) {
    return lhs == uint32_t(rhs);
}

std::ostream& operator<<(std::ostream& os, const CollectionID& cid) {
    return os << cid.to_string();
}

std::string CollectionID::to_string(bool xPrefix) const {
    return fmt::format(xPrefix ? "{:#x}" : "{:x}", value);
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
    CollectionID cid = CollectionID::Default;
    cb::const_byte_buffer key{data(), size()};

    if (encoding == DocKeyEncodesCollectionId::Yes) {
        // Get the cid of the key and add it to the string
        std::tie(cid, key) =
                cb::mcbp::unsigned_leb128<CollectionIDType>::decode(
                        {data(), size()});
    }

    std::string_view remainingKey{reinterpret_cast<const char*>(key.data()),
                                  key.size()};
    if (getCollectionID().isSystemEventCollection()) {
        if (key.empty()) {
            // This case can occur if range scanning the system namespace
            return "SystemEvent with empty data";
        }

        // Get the hex value of the type of system event
        auto [systemEventID, keyWithoutEvent] =
                cb::mcbp::unsigned_leb128<uint32_t>::decodeNoThrow(
                        {key.data(), key.size()});
        if (!keyWithoutEvent.data()) {
            // This case can occur if range scanning the system namespace
            return "SystemEvent " + toPrintableString();
        }

        // Get the cid or sid of the system event is for
        auto [cidOrSid, keySuffix] =
                cb::mcbp::unsigned_leb128<uint32_t>::decode(
                        {keyWithoutEvent.data(), keyWithoutEvent.size()});

        switch (SystemEvent(systemEventID)) {
        case SystemEvent::Collection:
        case SystemEvent::Scope:
        case SystemEvent::ModifyCollection:
            // Get the string view to the remaining string part of the key
            remainingKey = {reinterpret_cast<const char*>(keySuffix.data()),
                            keySuffix.size()};
            return fmt::format(FMT_STRING("cid:{:#x}:{:#x}:{:#x}:{}"),
                               uint32_t(cid),
                               systemEventID,
                               cidOrSid,
                               remainingKey);
        }
    }
    return fmt::format(FMT_STRING("cid:{:#x}:{}"), uint32_t(cid), remainingKey);
}

std::string DocKey::toPrintableString() const {
    std::string buffer(getBuffer());
    for (auto& ii : buffer) {
        if (!std::isgraph(ii)) {
            ii = '.';
        }
    }
    return buffer;
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
bool DocKey::isInSystemEventCollection() const {
    if (encoding == DocKeyEncodesCollectionId::Yes) {
        // Note: when encoding == Yes the size is 2 bytes at a minimum. This is
        // as mcbp_validators will fail 0-byte logical keys, thus we always have
        // at least 1 byte of collection and 1 byte of key so size() checks are
        // not needed. System keys additionally never have empty logical keys.
        // Finally we do not need to consider the Prepared namespace as system
        // events are never created in that way.
        return data()[0] == CollectionID::SystemEvent;
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

std::string DocKey::makeWireEncodedString(CollectionID cid,
                                          const std::string& key) {
    cb::mcbp::unsigned_leb128<CollectionIDType> leb(uint32_t{cid});
    std::string ret;
    std::copy(leb.begin(), leb.end(), std::back_inserter(ret));
    ret.append(key);
    return ret;
}
