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
#include <sstream>

std::string CollectionID::to_string() const {
    std::stringstream sstream;
    sstream << "0x" << std::hex << value;
    return sstream.str();
}

std::string ScopeID::to_string() const {
    std::stringstream sstream;
    sstream << "0x" << std::hex << value;
    return sstream.str();
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
        auto cid = cb::mcbp::decode_unsigned_leb128<CollectionIDType>(buffer)
                           .first;
        if (cid != CollectionID::DurabilityPrepare) {
            return cid;
        }
        auto noPreparePrefix =
                cb::mcbp::skip_unsigned_leb128<CollectionIDType>(buffer);
        return cb::mcbp::decode_unsigned_leb128<CollectionIDType>(
                       noPreparePrefix)
                .first;
    }
    return CollectionID::Default;
}

std::pair<CollectionID, cb::const_byte_buffer> DocKey::getIdAndKey() const {
    if (encoding == DocKeyEncodesCollectionId::Yes) {
        return cb::mcbp::decode_unsigned_leb128<CollectionIDType>(buffer);
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
