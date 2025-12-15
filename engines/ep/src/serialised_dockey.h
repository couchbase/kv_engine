/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <gsl/gsl-lite.hpp>
#include <memcached/dockey_view.h>

#include <memory>

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
        return bytes.data();
    }

    size_t size() const {
        return length;
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

    std::string to_string() const {
        return DocKeyView(*this).to_string();
    }

    bool operator==(const DocKeyView& rhs) const;

    bool operator==(const SerialisedDocKey& rhs) const;

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
    static size_t getObjectSize(const DocKeyView key) {
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
    operator DocKeyView() const {
        return {bytes.data(), length, DocKeyEncodesCollectionId::Yes};
    }

    PrefixedDocKeyView to_prefixed_key_view() const {
        return {bytes.data(), length};
    }

    /**
     * make a SerialisedDocKey and return a unique_ptr to it - this is used
     * in test code only.
     */
    static std::unique_ptr<SerialisedDocKey, SerialisedDocKeyDelete> make(
            DocKeyView key) {
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
    friend class MutationLogEntry;
    friend class StoredValue;

    SerialisedDocKey() = default;

    /**
     * Create a SerialisedDocKey from a DocKey. Protected constructor as
     * this must be used by friends who know how to pre-allocate the object
     * storage
     * @param key a DocKey to be copied in
     */
    explicit SerialisedDocKey(const DocKeyView& key)
        : length(gsl::narrow_cast<uint8_t>(key.size())) {
        if (key.getEncoding() == DocKeyEncodesCollectionId::Yes) {
            std::copy(key.data(),
                      key.data() + key.size(),
                      reinterpret_cast<char*>(bytes.data()));
        } else {
            // This key is for the default collection
            bytes[0] = DefaultCollectionLeb128Encoded;
            std::copy(key.data(),
                      key.data() + key.size(),
                      reinterpret_cast<char*>(bytes.data()) + 1);
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
        std::copy(
                key.begin(), key.end(), reinterpret_cast<char*>(bytes.data()));
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
    std::array<uint8_t, 1> bytes;
};

std::ostream& operator<<(std::ostream& os, const SerialisedDocKey& key);

static_assert(std::is_standard_layout<SerialisedDocKey>::value,
              "SeralisedDocKey: must satisfy is_standard_layout");
