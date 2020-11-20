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

#include <cstdint>
#include <cstring>

#include <platform/sized_buffer.h>
#include <platform/socket.h>

class CollectionIDNetworkOrder;
class ScopeIDNetworkOrder;

/**
 * CollectionID
 * Meta-data that applies to every document stored in an engine.
 *
 * A document "key" with the flag DefaultCollection is not the same document
 * as "key" with the Collections flag and so on...
 *
 * DefaultCollection: describes "legacy" documents stored in a bucket by
 * clients that do not understand collections.
 *
 * Collections: describes documents that have a collection name as part of
 * the key. E.g. "planet::earth" and "planet::mars" are documents belonging
 * to a "planet" collection.
 *
 * System: describes documents that are created by the system for our own
 * uses. This is only planned for use with the collection feature where
 * special keys are interleaved in the users data stream to represent create
 * and delete events. In future more generic "system documents" maybe
 * created by the core but until such plans are more clear, ep-engine will
 * deny the core from performing operations in the System Collection.
 * CollectionID values are persisted to the database and thus are fully
 * described now ready for future use.
 */
using CollectionIDType = uint32_t;
class CollectionID {
public:
    enum {
        /// To allow KV to move legacy data into a collection, reserve 0
        Default = 0,

        /// To weave system things into the users namespace, reserve 1
        System = 1,

        /// Prefix for pending SyncWrites
        DurabilityPrepare = 2,

        /// Reserved for future use
        Reserved3 = 3,
        Reserved4 = 4,
        Reserved5 = 5,
        Reserved6 = 6,
        Reserved7 = 7
    };

    class SkipIDVerificationTag {};

    CollectionID() : value(Default) {
    }

    // Add no lint for explict constructor so that we may use implicit
    // construction as this is used through out our code base.
    // NOLINTNEXTLINE(google-explicit-constructor)
    CollectionID(CollectionIDType value) : value(value) {
        if (value >= DurabilityPrepare && value <= Reserved7) {
            throw std::invalid_argument("CollectionID: invalid value:" +
                                        std::to_string(value));
        }
    }

    /**
     * Tagged constructor for specific use-cases where we want to construct
     * a CollectionID in the reserved namespace. Skips the normal valid
     * cid checks, allowing *any* CollectionID to be created.
     */
    CollectionID(CollectionIDType value, SkipIDVerificationTag t)
        : value(value) {
    }

    explicit operator uint32_t() const {
        return value;
    }

    bool operator==(const CollectionID& other) const {
        return value == other.value;
    }

    bool operator!=(const CollectionID& other) const {
        return value != other.value;
    }

    bool operator<(const CollectionID& other) const {
        return value < other.value;
    }

    bool isDefaultCollection() const {
        return value == Default;
    }

    bool isSystem() const {
        return value == System;
    }

    bool isDurabilityPrepare() const {
        return value == DurabilityPrepare;
    }

    /// Get network byte order of the value
    CollectionIDNetworkOrder to_network() const;

    std::string to_string() const;

    /**
     * @return if the value is a value reserved by KV
     */
    static bool isReserved(CollectionIDType value);

private:
    CollectionIDType value;
};

bool operator==(CollectionIDType lhs, const CollectionID& rhs);
std::ostream& operator<<(std::ostream& os, const CollectionID& cid);

using ScopeIDType = uint32_t;
class ScopeID {
public:
    enum {
        Default = 0,
        // We reserve 1 to 7 for future use
        Reserved1 = 1,
        Reserved2 = 2,
        Reserved3 = 3,
        Reserved4 = 4,
        Reserved5 = 5,
        Reserved6 = 6,
        Reserved7 = 7
    };

    ScopeID() : value(Default) {
    }

    // Add no lint for explict constructor so that we may use implicit
    // construction as this is used through out our code base.
    // NOLINTNEXTLINE(google-explicit-constructor)
    ScopeID(ScopeIDType value) : value(value) {
        if (value > Default && value <= Reserved7) {
            throw std::invalid_argument("ScopeID: invalid value:" +
                                        std::to_string(value));
        }
    }

    explicit operator uint32_t() const {
        return value;
    }

    bool operator==(const ScopeID& other) const {
        return value == other.value;
    }

    bool operator!=(const ScopeID& other) const {
        return value != other.value;
    }

    bool isDefaultScope() const {
        return value == Default;
    }

    /// Get network byte order of the value
    ScopeIDNetworkOrder to_network() const;

    std::string to_string() const;

private:
    ScopeIDType value;
};

bool operator==(ScopeIDType lhs, const ScopeID& rhs);
std::ostream& operator<<(std::ostream& os, const ScopeID& sid);

using ScopeCollectionPair = std::pair<ScopeID, CollectionID>;

/**
 * NetworkByte order version of CollectionID - limited interface for small areas
 * of code which deal with a network byte order CID
 */
class CollectionIDNetworkOrder {
public:
    explicit CollectionIDNetworkOrder(CollectionID v)
        : value(htonl(uint32_t(v))) {
    }

    CollectionID to_host() const {
        return CollectionID(ntohl(value));
    }

private:
    CollectionIDType value;
};

class ScopeIDNetworkOrder {
public:
    explicit ScopeIDNetworkOrder(ScopeID v) : value(htonl(uint32_t(v))) {
    }

    ScopeID to_host() const {
        return ScopeID(ntohl(value));
    }

private:
    ScopeIDType value;
};

static_assert(sizeof(CollectionIDType) == 4,
              "CollectionIDNetworkOrder assumes 4-byte id");

static_assert(sizeof(ScopeIDType) == 4, "ScopeIDType assumes 4-byte id");

inline CollectionIDNetworkOrder CollectionID::to_network() const {
    return CollectionIDNetworkOrder{*this};
}

inline ScopeIDNetworkOrder ScopeID::to_network() const {
    return ScopeIDNetworkOrder{*this};
}

namespace std {
template <>
struct hash<CollectionID> {
    std::size_t operator()(const CollectionID& k) const {
        return std::hash<uint32_t>()(uint32_t(k));
    }
};

template <>
struct hash<ScopeID> {
    std::size_t operator()(const ScopeID& k) const {
        return std::hash<uint32_t>()(static_cast<uint32_t>(k));
    }
};

} // namespace std

/**
 * A DocKey views a key (non-owning). It can view a key with or without
 * defined collection-ID. Keys with a collection-ID, encode the collection-ID
 * as a unsigned_leb128 prefix in the key bytes. This enum defines if the
 * DocKey is viewing a unsigned_leb128 prefixed key (Yes) or not (No)
 */
enum class DocKeyEncodesCollectionId : uint8_t { Yes, No };

// Constant value of the DefaultCollection(value of 0) leb128 encoded
const uint8_t DefaultCollectionLeb128Encoded = 0;

template <class T>
struct DocKeyInterface {
    size_t size() const {
        return static_cast<const T*>(this)->size();
    }

    const uint8_t* data() const {
        return static_cast<const T*>(this)->data();
    }

    CollectionID getCollectionID() const {
        return static_cast<const T*>(this)->getCollectionID();
    }

    /**
     * @return true if the key has a collection of CollectionID::System
     */
    bool isInSystemCollection() const {
        return static_cast<const T*>(this)->isInSystemCollection();
    }

    /**
     * @return true if the key has a collection of CollectionID::Default
     */
    bool isInDefaultCollection() const {
        return static_cast<const T*>(this)->isInDefaultCollection();
    }

    DocKeyEncodesCollectionId getEncoding() const {
        return static_cast<const T*>(this)->getEncoding();
    }

    std::string to_string() const {
        return static_cast<const T*>(this)->to_string();
    }

    uint32_t hash() const {
        uint32_t h = 5381;

        if (getEncoding() == DocKeyEncodesCollectionId::No) {
            h = ((h << 5) + h) ^ uint32_t(DefaultCollectionLeb128Encoded);
        }
        // else hash the entire data which includes an encoded CollectionID

        for (auto c : cb::const_byte_buffer(data(), size())) {
            h = ((h << 5) + h) ^ uint32_t(c);
        }

        return h;
    }
};

/**
 * DocKey is a non-owning structure used to describe a document keys over
 * the engine-API. All API commands working with "keys" must specify the
 * data, length and if the data contain an encoded CollectionID
 */
struct DocKey : DocKeyInterface<DocKey> {
    /**
     * Standard constructor - creates a view onto key/nkey
     *
     * @param key The data pointer, which can be null if nkey is 0
     * @param nkey The length of the data, which can be 0 if encoding is No
     * @param encoding Does the data include/encode a collection-ID
     */
    DocKey(const uint8_t* key, size_t nkey, DocKeyEncodesCollectionId encoding)
        : buffer(key, nkey), encoding(encoding) {
        if (encoding == DocKeyEncodesCollectionId::Yes) {
            if (nkey == 0) {
                throw std::invalid_argument("DocKey: invalid nkey:" +
                                            std::to_string(nkey));
            } else if (key == nullptr) {
                throw std::invalid_argument("DocKey: invalid key:");
            } else if (key[0] == CollectionID::DurabilityPrepare) {
                // DurabilityPrepare is special-case that we disallow a view on
                throw std::invalid_argument(
                        "DocKey: cannot use DurabilityPrepare");
            }
        }
        if (key == nullptr && nkey > 0) {
            throw std::invalid_argument("DocKey: invalid key/nkey:" +
                                        std::to_string(nkey));
        }
    }

    /**
     * C-string constructor - only for use with null terminated strings and
     * creates a view onto key/strlen(key).
     */
    DocKey(const char* key, DocKeyEncodesCollectionId encoding)
        : DocKey(reinterpret_cast<const uint8_t*>(key),
                 std::strlen(key),
                 encoding) {
    }

    /**
     * std::string_view constructor, views the data()/size() of the key
     */
    DocKey(std::string_view key, DocKeyEncodesCollectionId encoding)
        : DocKey(reinterpret_cast<const uint8_t*>(key.data()),
                 key.size(),
                 encoding) {
    }

    /**
     * Disallow rvalue strings as we would view something which would soon be
     * out of scope.
     */
    DocKey(const std::string&& key,
           DocKeyEncodesCollectionId encoding) = delete;

    explicit operator std::string_view() const {
        return std::string_view(reinterpret_cast<const char*>(data()), size());
    }

    const uint8_t* data() const {
        return buffer.data();
    }

    size_t size() const {
        return buffer.size();
    }

    const uint8_t* begin() const {
        return buffer.begin();
    }

    const uint8_t* end() const {
        return buffer.end();
    }

    CollectionID getCollectionID() const;

    /**
     * @return true if the DocKey has a collection of CollectionID::System
     */
    bool isInSystemCollection() const;

    /**
     * @return true if the DocKey has a collection of CollectionID::Default
     */
    bool isInDefaultCollection() const;

    DocKeyEncodesCollectionId getEncoding() const {
        return encoding;
    }

    /**
     * @return the ID and the key as separate entities (the key does not contain
     * the ID). Thus a key which encodes the DefaultCollection can be
     * hashed/compared to the same value as the same logical key which doesn't
     * encode the collection-ID.
     */
    std::pair<CollectionID, cb::const_byte_buffer> getIdAndKey() const;

    /**
     * @return a DocKey that views this DocKey but without any collection-ID
     * prefix. If this was already viewing a key without any encoded
     * collection-ID, then this is returned.
     */
    DocKey makeDocKeyWithoutCollectionID() const;

    /**
     * Intended for debug use only
     * @returns cid:key
     */
    std::string to_string() const;

private:
    cb::const_byte_buffer buffer;
    DocKeyEncodesCollectionId encoding{DocKeyEncodesCollectionId::No};
};

/**
 * A maximum key length (i.e. request.keylen) for when collections are enabled.
 * The value 251 allows for the client to encode all possible 'legacy' keys with
 * the default collection-ID (0x0).
 */
const size_t MaxCollectionsKeyLen = 251;

/**
 * The maximum logical key length, i.e. the document's key without any
 * collection prefix. The value of 246 applies when the collection-ID is not the
 * default collection and ensures that the longest possible key can always be
 * stored regardless of the length of the collection-ID (which can be 1 to 5
 * bytes). NOTE: If the collection ID is the default collection, the logical key
 * length is 250, the pre-collections maximum.
 */
const size_t MaxCollectionsLogicalKeyLen = 246;
