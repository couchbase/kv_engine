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

#include <memcached/storeddockey_fwd.h>

#include <memcached/dockey.h>
#include <limits>
#include <string>
#include <type_traits>

/**
 * StoredDocKey is an owning type for a DocKey (non-owning view of a key).
 *
 * Once a document key is stored in a StoredDocKey it becomes a collection
 * prefixed key, e.g. An n-byte key is stored in a std::string which is sized to
 * store the CollectionID encoded as unsigned_leb128 followed by the n-byte key.
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
     * @return true if the key has a collection of CollectionID::SystemEvent
     */
    bool isInSystemEventCollection() const;

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

    /**
     * Intended for RangeScan key manipulation
     * @param c a char to append to the key
     */
    void append(char c) {
        keydata.push_back(c);
    }

    /**
     * Intended for RangeScan key manipulation, pop the back character from key
     */
    void pop_back() {
        keydata.pop_back();
    }

    /**
     * non-const accessor intended for RangeScan key manipulation
     *
     * @return reference to the back byte (as unsigned) to allow modification
     */
    uint8_t& back() {
        return reinterpret_cast<uint8_t&>(keydata.back());
    }

    // Add no lint to allow implicit casting to class DocKey as we use this to
    // implicitly cast to other forms of DocKeys thought out code.
    // NOLINTNEXTLINE(google-explicit-constructor)
    operator DocKey() const {
        return {keydata, DocKeyEncodesCollectionId::Yes};
    }

    /// Convert to a std::string including the leb128 CollectionID prefix
    explicit operator std::string() const {
        return std::string{keydata.data(), keydata.size()};
    }

protected:
    std::basic_string<std::string::value_type,
                      std::string::traits_type,
                      allocator_type>
            keydata;
};

std::ostream& operator<<(std::ostream& os, const StoredDocKey& key);
inline auto format_as(const StoredDocKey& key) {
    return key.to_string();
}

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
} // namespace std
