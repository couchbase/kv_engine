/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <mcbp/protocol/unsigned_leb128.h>
#include <memcached/storeddockey.h>
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

template class StoredDocKeyT<std::allocator>;
template class StoredDocKeyT<MemoryTrackingAllocator>;
