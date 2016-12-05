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

#include <iostream>
#include <string>
#include <type_traits>

#include <memcached/dockey.h>

#include "ep_types.h"

/**
 * StoredDocKey is a container for key data
 *
 * Internally an n byte key is stored in a n + 1 std::string.
 *  a) We zero terminate so that data() is safe for printing as a c-string.
 *  b) We store the DocNamespace in byte 0 (duplicated in parent class DocKey).
 *    This is because StoredDocKey typically ends up being written to disk and
 *    the DocNamespace forms part of the on-disk key. Pre-allocating space
 *    for the DocNamespace means storage components don't have to create a
 *    new buffer into which we layout DocNamespace and key data.
 */
class StoredDocKey : public DocKeyInterface<StoredDocKey> {
public:
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
        keydata.at(0) = static_cast<std::underlying_type<DocNamespace>::type>(
                docNamespace);
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
        return DocNamespace(keydata.at(0));
    }

    const char* c_str() const {
        return &keydata.c_str()[namespaceBytes];
    }

    /**
     * Return the key with the DocNamespace as a prefix.
     * Thus the "beer::bud" in the Collections namespace becomes "\1beer::bud"
     */
    const uint8_t* getDocNameSpacedData() const {
        return reinterpret_cast<const uint8_t*>(keydata.data());
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
    static const size_t namespaceBytes = sizeof(DocNamespace);
    std::string keydata;
};

std::ostream& operator<<(std::ostream& os, const StoredDocKey& key);

static_assert(sizeof(DocNamespace) == sizeof(uint8_t),
              "StoredDocKey: DocNamespace is too large");

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
