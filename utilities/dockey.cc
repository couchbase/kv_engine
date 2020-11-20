/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include <mcbp/protocol/unsigned_leb128.h>
#include <memcached/dockey.h>
#include <memcached/systemevent.h>
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
    return os << "0x" << std::hex << uint32_t(cid);
}

std::string CollectionID::to_string() const {
    std::stringstream sstream;
    sstream << *this;
    return sstream.str();
}

bool operator==(ScopeIDType lhs, const ScopeID& rhs) {
    return lhs == uint32_t(rhs);
}

std::ostream& operator<<(std::ostream& os, const ScopeID& sid) {
    return os << "0x" << std::hex << uint32_t(sid);
}

std::string ScopeID::to_string() const {
    std::stringstream sstream;
    sstream << *this;
    return sstream.str();
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
