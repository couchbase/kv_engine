/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include <memcached/dockey.h>

#pragma once

namespace Collections {

/**
 * Collections::DocKey extends a DocKey so that the length of the collection
 * can be recorded (the collection is a prefix on the key).
 * The length of the collection only describes how many bytes are before
 * the collection separator, whether the collection is valid or not is not
 * something this class determines.
 */
class DocKey : public ::DocKey {
public:

    uint32_t hash() const {
        return ::DocKey::hash(collectionLen);
    }

    /**
     * Factory method to create a Collections::DocKey from a DocKey
     */
    static DocKey make(const ::DocKey& key, const std::string& separator) {
        const uint8_t* collection = findCollection(key, separator);
        if (collection) {
            return DocKey(key, collection - key.data());
        } else {
            // No collection found, not an error - ok for DefaultNamespace.
            return DocKey(key, 0);
        }
    }

    /**
     * @return how many bytes of the key are a collection
     */
    size_t getCollectionLen() const {
        return collectionLen;
    }

private:
    DocKey(const ::DocKey& key, size_t _collectionLen)
        : ::DocKey(key), collectionLen(_collectionLen) {
    }

    /**
     * Perform a search on the DocKey looking for the separator.
     *
     * @returns pointer to the start of the separator or nullptr if none found.
     */
    static const cb::const_byte_buffer::iterator findCollection(
            const ::DocKey& key, const std::string& separator) {
        if (key.size() == 0 || separator.size() == 0 ||
            separator.size() > key.size()) {
            return nullptr;
        }

        auto rv = std::search(key.data(),
                              key.data() + key.size(),
                              separator.begin(),
                              separator.end());
        if (rv != (key.data() + key.size())) {
            return rv;
        }
        return nullptr;
    }

    uint8_t collectionLen;
};
} // end namespace Collections

namespace std {
template <>
struct hash<Collections::DocKey> {
    std::size_t operator()(const Collections::DocKey& key) const {
        return key.hash();
    }
};
}