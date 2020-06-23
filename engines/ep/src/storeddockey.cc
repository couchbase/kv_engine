/*
 *     Copyright 2016 Couchbase, Inc
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

#include "storeddockey.h"
#include <mcbp/protocol/unsigned_leb128.h>
#include <memory_tracking_allocator.h>
#include <iomanip>

std::ostream& operator<<(std::ostream& os, const StoredDocKey& key) {
    return os << key.to_string();
}

template <template <class> class Allocator>
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

template <template <class> class Allocator>
StoredDocKeyT<Allocator>::StoredDocKeyT(const std::string& key,
                                        CollectionID cid) {
    cb::mcbp::unsigned_leb128<CollectionIDType> leb128(cid);
    keydata.resize(key.size() + leb128.size());
    std::copy(key.begin(),
              key.end(),
              std::copy(leb128.begin(), leb128.end(), keydata.begin()));
}

template <template <class> class Allocator>
StoredDocKeyT<Allocator>::StoredDocKeyT(const DocKey& key, CollectionID cid) {
    cb::mcbp::unsigned_leb128<CollectionIDType> leb128(cid);
    keydata.resize(key.size() + leb128.size());
    std::copy(key.begin(),
              key.end(),
              std::copy(leb128.begin(), leb128.end(), keydata.begin()));
}

template <template <class> class Allocator>
CollectionID StoredDocKeyT<Allocator>::getCollectionID() const {
    return cb::mcbp::unsigned_leb128<CollectionIDType>::decode({data(), size()})
            .first;
}

template <template <class> class Allocator>
DocKey StoredDocKeyT<Allocator>::makeDocKeyWithoutCollectionID() const {
    auto decoded = cb::mcbp::unsigned_leb128<CollectionIDType>::decode(
            {data(), size()});
    return {decoded.second.data(),
            decoded.second.size(),
            DocKeyEncodesCollectionId::No};
}

template <template <class> class Allocator>
std::string StoredDocKeyT<Allocator>::to_string() const {
    return DocKey(*this).to_string();
}

template <template <class> class Allocator>
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
    cb::mcbp::unsigned_leb128<CollectionIDType> leb128(cid);
    length = gsl::narrow_cast<uint8_t>(key.size() + leb128.size());
    std::copy(key.begin(),
              key.end(),
              std::copy(leb128.begin(), leb128.end(), bytes));
}

std::ostream& operator<<(std::ostream& os, const SerialisedDocKey& key) {
    os << key.to_string() << ", size:" << std::dec << key.size();
    return os;
}
