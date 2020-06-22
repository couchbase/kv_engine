/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#include "diskdockey.h"
#include "item.h"
#include <mcbp/protocol/unsigned_leb128.h>
#include <sstream>

DiskDocKey::DiskDocKey(const DocKey& key, bool prepared) {
    uint8_t keyOffset = 0;
    if (prepared) {
        // 1 byte for Prepare prefix
        keydata.resize(1);
        keydata[0] = CollectionID::DurabilityPrepare;
        keyOffset++;
    }

    if (key.getEncoding() == DocKeyEncodesCollectionId::No) {
        // 1 byte for the Default CollectionID
        keydata.resize(keyOffset + 1);
        keydata[keyOffset] = DefaultCollectionLeb128Encoded;
        keyOffset++;
    }

    keydata.resize(keyOffset + key.size());
    std::copy(key.data(), key.data() + key.size(), keydata.begin() + keyOffset);
}

DiskDocKey::DiskDocKey(const Item& item)
    : DiskDocKey(item.getKey(),
                 item.isPending() || item.isAbort() /*Prepare namespace?*/) {
}

DiskDocKey::DiskDocKey(const char* ptr, size_t len) : keydata(ptr, len) {
}

std::size_t DiskDocKey::hash() const {
    return std::hash<std::string>()(keydata);
}

DocKey DiskDocKey::getDocKey() const {
    // Skip past Prepared prefix if present.
    const auto decoded = cb::mcbp::unsigned_leb128<CollectionIDType>::decode(
            {data(), size()});
    if (decoded.first == CollectionID::DurabilityPrepare) {
        return {decoded.second.data(),
                decoded.second.size(),
                DocKeyEncodesCollectionId::Yes};
    }
    return {data(), size(), DocKeyEncodesCollectionId::Yes};
}

bool DiskDocKey::isCommitted() const {
    return !isPrepared();
}

bool DiskDocKey::isPrepared() const {
    const auto prefix = cb::mcbp::unsigned_leb128<CollectionIDType>::decode(
            {data(), size()});
    return prefix.first == CollectionID::DurabilityPrepare;
}

std::string DiskDocKey::to_string() const {
    std::stringstream ss;
    auto decoded = cb::mcbp::unsigned_leb128<CollectionIDType>::decode(
            {data(), size()});
    if (decoded.first == CollectionID::DurabilityPrepare) {
        ss << "pre:";
        decoded = cb::mcbp::unsigned_leb128<CollectionIDType>::decode(
                decoded.second);
    }
    ss << getDocKey().to_string();
    return ss.str();
}
