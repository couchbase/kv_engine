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
#include <iomanip>
#include <sstream>

#include <platform/sized_buffer.h>
#include <platform/socket.h>

class CollectionIDNetworkOrder;

/**
 * DocNamespace "Document Namespace"
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
 * deny the core from performing operations in the System DocNamespace.
 * DocNamespace values are persisted ot the database and thus are fully
 * described now ready for future use.
 */
using CollectionIDType = uint32_t;
class CollectionID {
public:
    /// To allow KV to move legacy data into a collection, reserve 0
    static constexpr CollectionIDType DefaultCollection = 0;

    /// To allow KV to weave system things into the users namespace, reserve 1
    static constexpr CollectionIDType System = 1;

    CollectionID() : value(DefaultCollection) {
    }

    CollectionID(CollectionIDType value) : value(value) {
    }

    operator uint32_t() const {
        return value;
    }

    bool isDefaultCollection() const {
        return value == DefaultCollection;
    }

    /// Get network byte order of the value
    CollectionIDNetworkOrder to_network() const;

    std::string to_string() const {
        std::stringstream sstream;
        sstream << "0x" << std::hex << value;
        return sstream.str();
    }

private:
    CollectionIDType value;
};

/**
 * NetworkByte order version of CollectionID - limited interface for small areas
 * of code which deal with a network byte order CID
 */
class CollectionIDNetworkOrder {
public:
    CollectionIDNetworkOrder(CollectionID v) : value(htonl(uint32_t(v))) {
    }

    CollectionID to_host() const {
        return CollectionID(ntohl(value));
    }

private:
    CollectionIDType value;
};

static_assert(sizeof(CollectionIDType) == 4,
              "CollectionIDNetworkOrder assumes 4-byte id");

inline CollectionIDNetworkOrder CollectionID::to_network() const {
    return {*this};
}

namespace std {
template <>
struct hash<CollectionID> {
    std::size_t operator()(const CollectionID& k) const {
        return std::hash<uint32_t>()(k);
    }
};

} // namespace std

/// To allow manageable patches during updates to Collections, allow both names
using DocNamespace = CollectionID;

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

    DocNamespace getDocNamespace() const {
        return static_cast<const T*>(this)->getDocNamespace();
    }

    uint32_t hash() const {
        return hash(size());
    }

protected:
    uint32_t hash(size_t bytes) const {
        uint32_t h = 5381;

        h = ((h << 5) + h) ^ getCollectionID();

        for (size_t i = 0; i < bytes; i++) {
            h = ((h << 5) + h) ^ uint32_t(data()[i]);
        }

        return h;
    }
};

/**
 * A DocKey is non-owning and may refer to a key which has no collection (and
 * is thus automatically a default-collection key) or a key which has a defined
 * collection, in which our encoding scheme is to keep the collection-ID in the
 * key-bytes (as a unsigned_leb128)
 */
enum class DocKeyEncodesCollectionId { Yes, No };

/**
 * DocKey is a non-owning structure used to describe a document keys over
 * the engine-API. All API commands working with "keys" must specify the
 * data, length and the namespace with which the document applies to.
 */
struct DocKey : DocKeyInterface<DocKey> {
    /**
     * Standard constructor - creates a view onto key/nkey
     */
    DocKey(const uint8_t* key, size_t nkey, DocKeyEncodesCollectionId encoding)
        : buffer(key, nkey), collectionID(CollectionID::DefaultCollection) {
        (void)encoding; // Unused at this patch level.
    }

    // Temporary method only for this commit and to be removed in the next
    // patch in this series
    DocKey(const uint8_t* key,
           size_t nkey,
           DocKeyEncodesCollectionId encoding,
           CollectionID id)
        : buffer(key, nkey), collectionID(id) {
        (void)encoding; // Unused at this patch level.
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

    // Temporary method only for this commit and to be removed in the next
    // patch in this series
    DocKey(const char* key, DocKeyEncodesCollectionId encoding, CollectionID id)
        : DocKey(reinterpret_cast<const uint8_t*>(key),
                 std::strlen(key),
                 encoding,
                 id) {
    }

    /**
     * const_char_buffer constructor, views the data()/size() of the key
     */
    DocKey(const cb::const_char_buffer& key, DocKeyEncodesCollectionId encoding)
        : DocKey(reinterpret_cast<const uint8_t*>(key.data()),
                 key.size(),
                 encoding) {
    }

    /**
     * const_char_buffer constructor, views the data()/size() of the key
     */
    DocKey(const cb::const_char_buffer& key,
           DocKeyEncodesCollectionId encoding,
           CollectionID id)
        : DocKey(reinterpret_cast<const uint8_t*>(key.data()),
                 key.size(),
                 encoding,
                 id) {
    }

    /**
     * Disallow rvalue strings as we would view something which would soon be
     * out of scope.
     */
    DocKey(const std::string&& key,
           DocKeyEncodesCollectionId encoding) = delete;

    template <class T>
    DocKey(const DocKeyInterface<T>& key) {
        buffer.len = key.size();
        buffer.buf = key.data();
        collectionID = key.getCollectionID();
    }

    const uint8_t* data() const {
        return buffer.data();
    }

    size_t size() const {
        return buffer.size();
    }

    DocNamespace getDocNamespace() const {
        return collectionID;
    }

    CollectionID getCollectionID() const {
        return collectionID;
    }

private:
    cb::const_byte_buffer buffer;
    CollectionID collectionID;
};
