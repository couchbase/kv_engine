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

#pragma once

#include <limits>
#include <memory>
#include <string>
#include <type_traits>

#include <memcached/dockey.h>

#include <gsl/gsl>

#include "ep_types.h"

class SerialisedDocKey;

/**
 * StoredDocKey is a container for key data
 *
 * Internally an n byte key is stored in a n + sizeof(CollectionID) std::string.
 *  a) We zero terminate so that data() is safe for printing as a c-string.
 *  b) We store the CollectionID in byte 0:3. This is because StoredDocKey
 *    typically ends up being written to disk and the CollectionID forms part
 *    of the on-disk key. Accounting and for for the CollectionID means storage
 *    components don't have to create a new buffer into which they can layout
 *    CollectionID and key data.
 */
class StoredDocKey : public DocKeyInterface<StoredDocKey> {
public:
    /**
     * Construct empty - required for some std containers
     */
    StoredDocKey() : keydata() {
    }

    /**
     * Create a StoredDocKey from a DocKey
     * @param key DocKey that is to be copied-in
     */
    StoredDocKey(const DocKey& key) {
        if (key.getEncoding() == DocKeyEncodesCollectionId::Yes) {
            keydata.resize(key.size());
            std::copy(key.data(), key.data() + key.size(), keydata.begin());
        } else {
            // This key is for the default collection (which has a fixed value)
            keydata.resize(key.size() + 1);
            keydata[0] = DefaultCollectionLeb128Encoded;
            std::copy(key.data(), key.data() + key.size(), keydata.begin() + 1);
        }
    }

    /**
     * Create a StoredDocKey from a std::string (test code uses this)
     * @param key std::string to be copied-in
     * @param cid the CollectionID that the key applies to (and will be encoded
     *        into the stored data)
     */
    StoredDocKey(const std::string& key, CollectionID cid) {
        cb::mcbp::unsigned_leb128<CollectionIDType> leb128(cid);
        keydata.resize(key.size() + leb128.size());
        std::copy(key.begin(),
                  key.end(),
                  std::copy(leb128.begin(), leb128.end(), keydata.begin()));
    }

    const uint8_t* data() const {
        return reinterpret_cast<const uint8_t*>(keydata.data());
    }

    size_t size() const {
        return keydata.size();
    }

    DocNamespace getDocNamespace() const {
        return getCollectionID();
    }

    CollectionID getCollectionID() const {
        return cb::mcbp::decode_unsigned_leb128<CollectionIDType>(
                       {data(), size()})
                .first;
    }

    DocKeyEncodesCollectionId getEncoding() const {
        return DocKeyEncodesCollectionId::Yes;
    }

    /**
     * @return a DocKey that views this StoredDocKey but without any
     * collection-ID prefix.
     */
    DocKey makeDocKeyWithoutCollectionID() const {
        auto decoded = cb::mcbp::decode_unsigned_leb128<CollectionIDType>(
                {data(), size()});
        return {decoded.second.data(),
                decoded.second.size(),
                DocKeyEncodesCollectionId::No};
    }

    /**
     * Intended for debug use only
     * @returns cid:key
     */
    std::string to_string() const;

    /**
     * For tests only
     * @returns the 'key' part of the StoredDocKey
     */
    const char* c_str() const {
        // Locate the leb128 stop byte, and return pointer after that
        auto key = cb::mcbp::skip_unsigned_leb128<CollectionIDType>(
                {reinterpret_cast<const uint8_t*>(keydata.data()),
                 keydata.size()});

        if (key.size()) {
            return &keydata.c_str()[keydata.size() - key.size()];
        }
        return nullptr;
    }

    int compare(const StoredDocKey& rhs) const {
        return keydata.compare(rhs.keydata);
    }

    bool operator==(const StoredDocKey& rhs) const {
        return keydata == rhs.keydata;
    }

    bool operator!=(const StoredDocKey& rhs) const {
        return !(*this == rhs);
    }

    bool operator<(const StoredDocKey& rhs) const {
        return keydata < rhs.keydata;
    }

    operator DocKey() const {
        return {keydata, DocKeyEncodesCollectionId::Yes};
    }

protected:
    std::string keydata;
};

std::ostream& operator<<(std::ostream& os, const StoredDocKey& key);

static_assert(sizeof(CollectionID) == sizeof(uint32_t),
              "StoredDocKey: CollectionID has changed size");

/**
 * A hash function for StoredDocKey so they can be used in std::map and friends.
 */
namespace std {
template <>
struct hash<StoredDocKey> {
    std::size_t operator()(const StoredDocKey& key) const {
        return key.hash();
    }
};
}

class MutationLogEntryV2;
class StoredValue;

/**
 * SerialisedDocKey maintains the key data in an allocation that is not owned by
 * the class. The class is essentially immutable, providing a "view" onto the
 * larger block.
 *
 * For example where a StoredDocKey needs to exist as part of a bigger block of
 * data, SerialisedDocKey is the class to use.
 *
 * A limited number of classes are friends and only those classes can construct
 * a SerialisedDocKey.
 */
class SerialisedDocKey : public DocKeyInterface<SerialisedDocKey> {
public:
    /**
     * The copy constructor is deleted due to the bytes living outside of the
     * object.
     */
    SerialisedDocKey(const SerialisedDocKey& obj) = delete;

    const uint8_t* data() const {
        return bytes;
    }

    size_t size() const {
        return length;
    }

    DocNamespace getDocNamespace() const {
        return getCollectionID();
    }

    CollectionID getCollectionID() const {
        return cb::mcbp::decode_unsigned_leb128<CollectionIDType>(
                       {bytes, length})
                .first;
    }

    DocKeyEncodesCollectionId getEncoding() const {
        return DocKeyEncodesCollectionId::Yes;
    }

    bool operator==(const DocKey& rhs) const {
        auto rhsIdAndData = rhs.getIdAndKey();
        auto lhsIdAndData = cb::mcbp::decode_unsigned_leb128<CollectionIDType>(
                {data(), size()});
        return lhsIdAndData.first == rhsIdAndData.first &&
               lhsIdAndData.second.size() == rhsIdAndData.second.size() &&
               std::equal(lhsIdAndData.second.begin(),
                          lhsIdAndData.second.end(),
                          rhsIdAndData.second.begin());
    }

    /**
     * Return how many bytes are (or need to be) allocated to this object
     */
    size_t getObjectSize() const {
        return getObjectSize(length);
    }

    /**
     * Return how many bytes are needed to store the DocKey
     * @param key a DocKey that needs to be stored in a SerialisedDocKey
     */
    static size_t getObjectSize(const DocKey key) {
        return getObjectSize(key.size());
    }

    /**
     * Create a SerialisedDocKey and return a unique_ptr to the object.
     * Note that the allocation is bigger than sizeof(SerialisedDocKey)
     * @param key a DocKey to be stored as a SerialisedDocKey
     */
    struct SerialisedDocKeyDelete {
        void operator()(SerialisedDocKey* p) {
            p->~SerialisedDocKey();
            delete[] reinterpret_cast<uint8_t*>(p);
        }
    };

    operator DocKey() const {
        return {bytes, length, DocKeyEncodesCollectionId::Yes};
    }

    /**
     * make a SerialisedDocKey and return a unique_ptr to it - this is used
     * in test code only.
     */
    static std::unique_ptr<SerialisedDocKey, SerialisedDocKeyDelete> make(
            const StoredDocKey& key) {
        std::unique_ptr<SerialisedDocKey, SerialisedDocKeyDelete> rval(
                reinterpret_cast<SerialisedDocKey*>(
                        new uint8_t[getObjectSize(key)]));
        new (rval.get()) SerialisedDocKey(key);
        return rval;
    }

protected:
    /**
     * These following classes are "white-listed". They know how to allocate
     * and construct this object so are allowed access to the constructor.
     */
    friend class MutationLogEntryV2;
    friend class MutationLogEntryV3;
    friend class StoredValue;

    SerialisedDocKey() : length(0), bytes() {
    }

    /**
     * Create a SerialisedDocKey from a DocKey. Protected constructor as
     * this must be used by friends who know how to pre-allocate the object
     * storage
     * @param key a DocKey to be copied in
     */
    SerialisedDocKey(const DocKey& key)
        : length(gsl::narrow_cast<uint8_t>(key.size())) {
        if (key.getEncoding() == DocKeyEncodesCollectionId::Yes) {
            std::copy(key.data(), key.data() + key.size(), bytes);
        } else {
            // This key is for the default collection
            bytes[0] = DefaultCollectionLeb128Encoded;
            std::copy(key.data(), key.data() + key.size(), bytes + 1);
            length++;
        }
    }

    /**
     * Create a SerialisedDocKey from a byte_buffer that has no collection data
     * and requires the caller to state the collection-ID
     * This is used by MutationLogEntryV1/V2 to V3 upgrades
     */
    SerialisedDocKey(cb::const_byte_buffer key, CollectionID cid) {
        cb::mcbp::unsigned_leb128<CollectionIDType> leb128(cid);
        length = gsl::narrow_cast<uint8_t>(key.size() + leb128.size());
        std::copy(key.begin(),
                  key.end(),
                  std::copy(leb128.begin(), leb128.end(), bytes));
    }

    /**
     * Create a SerialisedDocKey from a byte_buffer that has collection data
     */
    SerialisedDocKey(cb::const_byte_buffer key)
        : length(gsl::narrow_cast<uint8_t>(key.size())) {
        std::copy(key.begin(), key.end(), bytes);
    }

    /**
     * Returns the size in bytes of this object - fixed size plus the variable
     * length for the bytes making up the key.
     */
    static size_t getObjectSize(size_t len) {
        return sizeof(SerialisedDocKey) +
               (len - sizeof(SerialisedDocKey().bytes));
    }

    uint8_t length{0};
    uint8_t bytes[1];
};

std::ostream& operator<<(std::ostream& os, const SerialisedDocKey& key);

static_assert(std::is_standard_layout<SerialisedDocKey>::value,
              "SeralisedDocKey: must satisfy is_standard_layout");
