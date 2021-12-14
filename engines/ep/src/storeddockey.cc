/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "storeddockey.h"
#include <mcbp/protocol/unsigned_leb128.h>
#include <platform/memory_tracking_allocator.h>
#include <iomanip>

std::ostream& operator<<(std::ostream& os, const StoredDocKey& key) {
    return os << key.to_string();
}

template <template <class, class...> class Allocator>
StoredDocKeyT<Allocator>::StoredDocKeyT(
        const DocKey& key,
        typename StoredDocKeyT<Allocator>::allocator_type allocator)
    : keydata(allocator) {
    if (key.getEncoding() == DocKeyEncodesCollectionId::Yes) {
        keydata.resize(key.size());
        std::copy(key.data(), key.data() + key.size(), keydata.begin());
    } else {
        // 1 byte for the Default CollectionID
        keydata.resize(key.size() + 1);
        keydata[0] = DefaultCollectionLeb128Encoded;
        std::copy(key.data(), key.data() + key.size(), keydata.begin() + 1);
    }
}

template <template <class, class...> class Allocator>
StoredDocKeyT<Allocator>::StoredDocKeyT(std::string_view key,
                                        CollectionID cid) {
    cb::mcbp::unsigned_leb128<CollectionIDType> leb128(uint32_t{cid});
    keydata.resize(key.size() + leb128.size());
    std::copy(key.begin(),
              key.end(),
              std::copy(leb128.begin(), leb128.end(), keydata.begin()));
}

template <template <class, class...> class Allocator>
StoredDocKeyT<Allocator>::StoredDocKeyT(const DocKey& key, CollectionID cid) {
    cb::mcbp::unsigned_leb128<CollectionIDType> leb128(uint32_t{cid});
    keydata.resize(key.size() + leb128.size());
    std::copy(key.begin(),
              key.end(),
              std::copy(leb128.begin(), leb128.end(), keydata.begin()));
}

template <template <class, class...> class Allocator>
CollectionID StoredDocKeyT<Allocator>::getCollectionID() const {
    return cb::mcbp::unsigned_leb128<CollectionIDType>::decode({data(), size()})
            .first;
}

template <template <class, class...> class Allocator>
bool StoredDocKeyT<Allocator>::isInSystemCollection() const {
    return data()[0] == CollectionID::System;
}

template <template <class, class...> class Allocator>
bool StoredDocKeyT<Allocator>::isInDefaultCollection() const {
    return data()[0] == CollectionID::Default;
}

template <template <class, class...> class Allocator>
DocKey StoredDocKeyT<Allocator>::makeDocKeyWithoutCollectionID() const {
    auto decoded = cb::mcbp::unsigned_leb128<CollectionIDType>::decode(
            {data(), size()});
    return {decoded.second.data(),
            decoded.second.size(),
            DocKeyEncodesCollectionId::No};
}

template <template <class, class...> class Allocator>
std::string StoredDocKeyT<Allocator>::to_string() const {
    return DocKey(*this).to_string();
}

template <template <class, class...> class Allocator>
const char* StoredDocKeyT<Allocator>::c_str() const {
    // Locate the leb128 stop byte, and return pointer after that
    auto key = cb::mcbp::skip_unsigned_leb128<CollectionIDType>(
            {reinterpret_cast<const uint8_t*>(keydata.data()), keydata.size()});

    if (!key.empty()) {
        return &keydata.c_str()[keydata.size() - key.size()];
    }
    return nullptr;
}

template class StoredDocKeyT<std::allocator>;
template class StoredDocKeyT<MemoryTrackingAllocator>;

CollectionID SerialisedDocKey::getCollectionID() const {
    return cb::mcbp::unsigned_leb128<CollectionIDType>::decode({bytes, length})
            .first;
}

bool SerialisedDocKey::isInSystemCollection() const {
    return data()[0] == CollectionID::System;
}

bool SerialisedDocKey::isInDefaultCollection() const {
    return data()[0] == CollectionID::Default;
}

bool SerialisedDocKey::operator==(const DocKey& rhs) const {
    // Does 'rhs' encode a collection-ID
    if (rhs.getEncoding() == DocKeyEncodesCollectionId::Yes) {
        // compare size and then the entire key
        return rhs.size() == size() &&
               std::equal(rhs.begin(), rhs.end(), data());
    }

    // 'rhs' does not encode a collection - it is therefore a key in the default
    // collection. We can check byte 0 of this key to see if this key is also
    // in the default collection. if so compare sizes (discounting the
    // default collection prefix) and finally the key data.
    return data()[0] == CollectionID::Default && rhs.size() == size() - 1 &&
           std::equal(rhs.begin(), rhs.end(), data() + 1);
}

SerialisedDocKey::SerialisedDocKey(cb::const_byte_buffer key,
                                   CollectionID cid) {
    cb::mcbp::unsigned_leb128<CollectionIDType> leb128(uint32_t{cid});
    length = gsl::narrow_cast<uint8_t>(key.size() + leb128.size());
    std::copy(key.begin(),
              key.end(),
              std::copy(leb128.begin(), leb128.end(), bytes));
}

std::ostream& operator<<(std::ostream& os, const SerialisedDocKey& key) {
    os << key.to_string() << ", size:" << std::dec << key.size();
    return os;
}
