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
#include <spdlog/fmt/fmt.h>
#include <sstream>
#include <string_view>

std::string CollectionID::to_string() const {
    std::stringstream sstream;
    sstream << "0x" << std::hex << value;
    return sstream.str();
}

bool CollectionID::isReserved(CollectionIDType value) {
    return value >= System && value <= Reserved7;
}

std::string ScopeID::to_string() const {
    std::stringstream sstream;
    sstream << "0x" << std::hex << value;
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
        // Get the string view to the remaining string part of the key
        remainingKey = {reinterpret_cast<const char*>(keySuffix.data()),
                        keySuffix.size()};
        return fmt::format(fmt("cid:{:#x}:{:#x}:{:#x}:{}"),
                           cid,
                           systemEventID,
                           cidOrSid,
                           remainingKey);
    } else {
        return fmt::format(fmt("cid:{:#x}:{}"), cid, remainingKey);
    }
}

CollectionID DocKey::getCollectionID() const {
    // Durability introduces the new "Prepare" namespace in DocKey.
    // So, the new generic format for a DocKey is:
    //
    // [prepare-prefix] [collection-id] [user-key]
    //
    // prepare-prefix is a 1-byte reserved CollectionID value, so it is a leb128
    // type by definition.
    // Note that only pending SyncWrite get the additional prefix, non-durable
    // writes and committed SyncWrites don't get any.
    if (encoding == DocKeyEncodesCollectionId::Yes) {
        auto cid = cb::mcbp::unsigned_leb128<CollectionIDType>::decode(buffer)
                           .first;
        if (cid != CollectionID::DurabilityPrepare) {
            return cid;
        }
        auto noPreparePrefix =
                cb::mcbp::skip_unsigned_leb128<CollectionIDType>(buffer);
        return cb::mcbp::unsigned_leb128<CollectionIDType>::decode(
                       noPreparePrefix)
                .first;
    }
    return CollectionID::Default;
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
