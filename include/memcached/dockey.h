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
enum class DocNamespace : uint8_t {
    DefaultCollection = 0,
    Collections = 1,
    System = 2
};

template <class T>
struct DocKeyInterface {
    size_t size() const {
        return static_cast<const T*>(this)->size();
    }

    const uint8_t* data() const {
        return static_cast<const T*>(this)->data();
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

        h = ((h << 5) + h) ^ uint32_t(getDocNamespace());

        for (size_t i = 0; i < bytes; i++) {
            h = ((h << 5) + h) ^ uint32_t(data()[i]);
        }

        return h;
    }
};

/**
 * DocKey is a non-owning structure used to describe a document keys over
 * the engine-API. All API commands working with "keys" must specify the
 * data, length and the namespace with which the document applies to.
 */
struct DocKey : DocKeyInterface<DocKey> {
    /**
     * Standard constructor - creates a view onto key/nkey
     */
    DocKey(const uint8_t* key, size_t nkey, DocNamespace ins)
        : buffer(key, nkey), doc_namespace(ins) {
    }

    /**
     * C-string constructor - only for use with null terminated strings and
     * creates a view onto key/strlen(key).
     */
    DocKey(const char* key, DocNamespace ins)
        : DocKey(reinterpret_cast<const uint8_t*>(key), std::strlen(key), ins) {
    }

    /**
     * std::string constructor - creates a view onto the std::string internal
     * C-string buffer - i.e. c_str() for size() bytes
     */
    DocKey(const std::string& key, DocNamespace ins)
        : DocKey(reinterpret_cast<const uint8_t*>(key.data()),
                 key.size(),
                 ins) {
    }

    template <class T>
    DocKey(const DocKeyInterface<T>& key) {
        buffer.len = key.size();
        buffer.buf = key.data();
        doc_namespace = key.getDocNamespace();
    }

    const uint8_t* data() const {
        return buffer.data();
    }

    size_t size() const {
        return buffer.size();
    }

    DocNamespace getDocNamespace() const {
        return doc_namespace;
    }

private:
    cb::const_byte_buffer buffer;
    DocNamespace doc_namespace;
};
