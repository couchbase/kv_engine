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
#include <gsl/gsl>

#include "collections/collections_types.h"

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
        switch (key.getDocNamespace()) {
        case DocNamespace::DefaultCollection:
            return DocKey(key, 0, 0);
        case DocNamespace::Collections: {
            const uint8_t* collection = findCollection(key, separator);
            if (collection) {
                return DocKey(key,
                              gsl::narrow<uint8_t>(collection - key.data()),
                              gsl::narrow<uint8_t>(separator.size()));
            }
            // No collection found in this key, so return empty (len 0)
            return DocKey(key, 0, 0);
        }
        case DocNamespace::System:
            throw std::invalid_argument(
                    "DocKey::make incorrect use of SystemKey");
        }
        throw std::invalid_argument("DocKey::make invalid key.namespace: " +
                                    std::to_string(int(key.getDocNamespace())));
    }

    /**
     * Factory method to create a Collections::DocKey from a DocKey
     */
    static Collections::DocKey make(const ::DocKey& key);

    /**
     * @return how many bytes of the key are a collection
     */
    size_t getCollectionLen() const {
        return collectionLen;
    }

    /**
     * @return how the length of the separator used in creating this
     */
    size_t getSeparatorLen() const {
        return separatorLen;
    }

    /**
     * @return const_char_buffer representing the collection part of the DocKey
     */
    cb::const_char_buffer getCollection() const {
        return {reinterpret_cast<const char*>(data()), getCollectionLen()};
    }

    /**
     * @return const_char_buffer representing the 'key' part of the DocKey
     */
    cb::const_char_buffer getKey() const {
        return {reinterpret_cast<const char*>(data() + getCollectionLen() +
                                              getSeparatorLen()),
                size() - getCollectionLen() - getSeparatorLen()};
    }

private:
    DocKey(const ::DocKey& key, uint8_t _collectionLen, uint8_t _separatorLen)
        : ::DocKey(key),
          collectionLen(_collectionLen),
          separatorLen(_separatorLen) {
        if (_separatorLen > std::numeric_limits<uint8_t>::max()) {
            throw std::invalid_argument(
                    "Collections::DocKey invalid separatorLen:" +
                    std::to_string(_separatorLen));
        }
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

        // SystemEvent Doc keys all start with $ (SystemEventFactory::make does)
        // this. The collections separator could legally be $, so we need to
        // ensure we skip character 0 to correctly split the key.
        const int start = key.getDocNamespace() == DocNamespace::System ? 1 : 0;

        auto rv = std::search(key.data() + start,
                              key.data() + key.size(),
                              separator.begin(),
                              separator.end());
        if (rv != (key.data() + key.size())) {
            return rv;
        }
        return nullptr;
    }

    uint8_t collectionLen;
    uint8_t separatorLen;
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
