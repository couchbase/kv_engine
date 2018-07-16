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
     * Create a StoredDocKey from key to key+nkey in docNamespace.
     * @param key pointer to key data
     * @param nkey byte length of key (which will be copied in)
     * @docNamespace the namespace the key applies to
     */
    StoredDocKey(const uint8_t* key, size_t nkey, DocNamespace docNamespace) {
        keydata.resize(nkey + namespaceBytes);
        keydata.replace(
                namespaceBytes, nkey, reinterpret_cast<const char*>(key), nkey);
        std::memcpy(&keydata[0], &docNamespace, sizeof(docNamespace));
    }

    /**
     * Create a StoredDocKey from a DocKey
     * @param key DocKey that is to be copied-in
     */
    StoredDocKey(const DocKey& key)
        : StoredDocKey(key.data(), key.size(), key.getDocNamespace()) {
    }

    /**
     * Create a StoredDocKey from a std::string. Post
     * construction size will return std::string::size();
     * @param key std::string to be copied-in
     * @param docNamespace the namespace that the key applies to
     */
    StoredDocKey(const std::string& key, DocNamespace docNamespace)
        : StoredDocKey(reinterpret_cast<const uint8_t*>(key.c_str()),
                       key.size(),
                       docNamespace) {
    }

    /**
     * Create a StoredDocKey from a buffer that was previously
     * obtained from getNamespacedData()
     * Post construction size() will return nkey - 1
     * @param key pointer to data to be copied in
     * @param nkey the number of bytes to be copied
     */
    StoredDocKey(const uint8_t* key, size_t nkey)
        : keydata(reinterpret_cast<const char*>(key), nkey) {
    }

    const uint8_t* data() const {
        return reinterpret_cast<const uint8_t*>(
                &keydata.data()[namespaceBytes]);
    }

    size_t size() const {
        return keydata.size() - namespaceBytes;
    }

    DocNamespace getDocNamespace() const {
        return getCollectionID();
    }

    CollectionID getCollectionID() const {
        return *reinterpret_cast<const CollectionID*>(&keydata[0]);
    }

    const char* c_str() const {
        return &keydata.c_str()[namespaceBytes];
    }

    int compare(const StoredDocKey& rhs) const {
        return keydata.compare(rhs.keydata);
    }

    /**
     * Return the key with the DocNamespace as a prefix.
     * Thus the "beer::bud" in the Collections namespace becomes "\1beer::bud"
     */
    const uint8_t* getDocNameSpacedData() const {
        return reinterpret_cast<const uint8_t*>(keydata.data());
    }

    /**
     * @return true if StoredDocKey was constructed empty from StoredDocKey()
     */
    bool empty() const {
        return keydata.empty();
    }

    /**
     * Return the size of the buffer returned by getDocNameSpacedData()
     */
    size_t getDocNameSpacedSize() const {
        return keydata.size();
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

protected:
    static const size_t namespaceBytes = sizeof(CollectionID);
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
        return collectionID;
    }

    bool operator==(const DocKey rhs) const {
        return size() == rhs.size() &&
               getCollectionID() == rhs.getCollectionID() &&
               std::memcmp(data(), rhs.data(), size()) == 0;
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

    static std::unique_ptr<SerialisedDocKey, SerialisedDocKeyDelete> make(
            const DocKey key) {
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
    friend class StoredValue;

    SerialisedDocKey() : collectionID(), length(0), bytes() {
    }

    /**
     * Create a SerialisedDocKey from a DocKey. Protected constructor as this
     * must be used by friends who know how to pre-allocate the object storage.
     * throws length_error if DocKey::len exceeds 255
     * @param key a DocKey to be stored
     */
    SerialisedDocKey(const DocKey key)
        : collectionID(key.getCollectionID()), length(0) {
        if (key.size() > std::numeric_limits<uint8_t>::max()) {
            throw std::length_error(
                    "SerialisedDocKey(const DocKey key) " +
                    std::to_string(length) +
                    "exceeds std::numeric_limits<uint8_t>::max()");
        }
        length = gsl::narrow_cast<uint8_t>(key.size());
        // Copy the data into bytes, which should be allocated into a larger
        // buffer.
        std::copy(key.data(), key.data() + key.size(), bytes);
    }

    /**
     * Returns the size in bytes of this object - fixed size plus the variable
     * length for the bytes making up the key.
     */
    static size_t getObjectSize(size_t len) {
        return sizeof(SerialisedDocKey) + len - sizeof(SerialisedDocKey().bytes);
    }

    CollectionID collectionID;
    uint8_t length;
    uint8_t bytes[3]; // Explicitly include the padding
};

std::ostream& operator<<(std::ostream& os, const SerialisedDocKey& key);

static_assert(std::is_standard_layout<SerialisedDocKey>::value,
              "SeralisedDocKey: must satisfy is_standard_layout");
