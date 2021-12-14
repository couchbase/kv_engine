/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "storeddockey_fwd.h"

#include "ep_types.h"

#include <gsl/gsl-lite.hpp>
#include <memcached/dockey.h>
#include <limits>
#include <string>
#include <type_traits>

class SerialisedDocKey;

/**
 * StoredDocKey is a container for key data
 *
 * Internally an n byte key is stored in a n + sizeof(CollectionID) std::string.
 *  a) We zero terminate so that data() is safe for printing as a c-string.
 *  b) The CollectionID is stored before the key string (using LEB128 encoding).
 *    This is because StoredDocKey typically ends up being written to disk and
 *    the CollectionID forms part of the on-disk key. Accounting and for for the
 *    CollectionID means storage components don't have to create a new buffer
 *    into which they can layout CollectionID and key data.
 *
 * StoredDocKeyT is templated over an Allocator type. A StoredDocKey using
 * declaration provides a StoredDocKeyT with std::allocator which is suitable
 * for most purposes. The Allocator here allows us to track checkpoint memory
 * overhead accurately when the key (std::string is the underlying type)
 * requires a heap allocation. This varies based on platform but is typically
 * keys over 16 or 24 bytes.
 */
template <template <class, class...> class Allocator>
class StoredDocKeyT : public DocKeyInterface<StoredDocKeyT<Allocator>> {
public:
    using allocator_type = Allocator<std::string::value_type>;

    /**
     * Construct empty - required for some std containers
     */
    StoredDocKeyT() = default;

    /**
     * Create a StoredDocKey from a DocKey
     *
     * @param key DocKey that is to be copied-in
     */
    explicit StoredDocKeyT(const DocKey& key)
        : StoredDocKeyT(key, allocator_type()) {
    }

    /**
     * Create a StoredDocKey from a DocKey
     *
     * @param key DocKey that is to be copied-in
     */
    StoredDocKeyT(const DocKey& key, allocator_type allocator);

    /**
     * Create a StoredDocKey from another, essentially wrapping a DocKey
     * with another collection.
     * @param key data that is to be copied-in
     * @param cid CollectionID to give to the new key
     */
    StoredDocKeyT(const DocKey& key, CollectionID cid);

    /**
     * Create a StoredDocKey from a std::string_view
     *
     * @param key std::string_view to be copied-in
     * @param cid the CollectionID that the key applies to (and will be encoded
     *        into the stored data)
     */
    StoredDocKeyT(std::string_view key, CollectionID cid);

    const char* keyData() const {
        return keydata.data();
    }

    const uint8_t* data() const {
        return reinterpret_cast<const uint8_t*>(keydata.data());
    }

    size_t size() const {
        return keydata.size();
    }

    CollectionID getCollectionID() const;

    /**
     * @return true if the key has a collection of CollectionID::System
     */
    bool isInSystemCollection() const;

    /**
     * @return true if the key has a collection of CollectionID::Default
     */
    bool isInDefaultCollection() const;

    DocKeyEncodesCollectionId getEncoding() const {
        return DocKeyEncodesCollectionId::Yes;
    }

    /**
     * @return a DocKey that views this StoredDocKey but without any
     * collection-ID prefix.
     */
    DocKey makeDocKeyWithoutCollectionID() const;

    /**
     * Intended for debug use only
     * @returns cid:key
     */
    std::string to_string() const;

    /**
     * For tests only
     * @returns the 'key' part of the StoredDocKey
     */
    const char* c_str() const;

    int compare(const StoredDocKeyT& rhs) const {
        return keydata.compare(rhs.keydata);
    }

    bool operator==(const StoredDocKeyT& rhs) const {
        return keydata == rhs.keydata;
    }

    bool operator!=(const StoredDocKeyT& rhs) const {
        return !(*this == rhs);
    }

    bool operator<(const StoredDocKeyT& rhs) const {
        return keydata < rhs.keydata;
    }

    bool operator==(const DocKey& rhs) const {
        return keydata == StoredDocKeyT(rhs).keydata;
    }

    bool operator!=(const DocKey& rhs) const {
        return !(*this == rhs);
    }

    // Add no lint to allow implicit casting to class DocKey as we use this to
    // implicitly cast to other forms of DocKeys thought out code.
    // NOLINTNEXTLINE(google-explicit-constructor)
    operator DocKey() const {
        return {keydata, DocKeyEncodesCollectionId::Yes};
    }

protected:
    std::basic_string<std::string::value_type,
                      std::string::traits_type,
                      allocator_type>
            keydata;
};

std::ostream& operator<<(std::ostream& os, const StoredDocKey& key);

static_assert(sizeof(CollectionID) == sizeof(uint32_t),
              "StoredDocKey: CollectionID has changed size");

/**
 * A hash function for StoredDocKey so they can be used in std::map and friends.
 */
namespace std {
template <template <class, class...> class Allocator>
struct hash<StoredDocKeyT<Allocator>> {
    std::size_t operator()(const StoredDocKeyT<Allocator>& key) const {
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

    CollectionID getCollectionID() const;

    /**
     * @return true if the key has a collection of CollectionID::System
     */
    bool isInSystemCollection() const;

    /**
     * @return true if the key has a collection of CollectionID::Default
     */
    bool isInDefaultCollection() const;

    DocKeyEncodesCollectionId getEncoding() const {
        return DocKeyEncodesCollectionId::Yes;
    }

    std::string to_string() const {
        return DocKey(*this).to_string();
    }

    bool operator==(const DocKey& rhs) const;

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

    // Add no lint to allow implicit casting to class DocKey as we use this to
    // implicitly cast to other forms of DocKeys thought out code.
    // NOLINTNEXTLINE(google-explicit-constructor)
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
    explicit SerialisedDocKey(const DocKey& key)
        : length(gsl::narrow_cast<uint8_t>(key.size())) {
        if (key.getEncoding() == DocKeyEncodesCollectionId::Yes) {
            std::copy(key.data(), key.data() + key.size(), reinterpret_cast<char*>(bytes));
        } else {
            // This key is for the default collection
            bytes[0] = DefaultCollectionLeb128Encoded;
            std::copy(key.data(), key.data() + key.size(), reinterpret_cast<char*>(bytes) + 1);
            length++;
        }
    }

    /**
     * Create a SerialisedDocKey from a byte_buffer that has no collection data
     * and requires the caller to state the collection-ID
     * This is used by MutationLogEntryV1/V2 to V3 upgrades
     */
    SerialisedDocKey(cb::const_byte_buffer key, CollectionID cid);

    /**
     * Create a SerialisedDocKey from a byte_buffer that has collection data
     */
    explicit SerialisedDocKey(cb::const_byte_buffer key)
        : length(gsl::narrow_cast<uint8_t>(key.size())) {
        std::copy(key.begin(), key.end(), reinterpret_cast<char*>(bytes));
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
