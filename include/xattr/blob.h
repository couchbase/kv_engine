/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include <nlohmann/json_fwd.hpp>
#include <platform/compress.h>
#include <platform/sized_buffer.h>
#include <xattr/utils.h>
#include <xattr/visibility.h>
#include <cstddef>
#include <memory>

namespace cb {
namespace xattr {

/**
 * The cb::xattr::Blob is a class that provides easy access to the
 * binary format of the blob.
 */
class XATTR_PUBLIC_API Blob {
public:
    /**
     * Create an empty Blob
     */
    Blob() : Blob({nullptr, 0}, default_allocator, 0) {}

    /**
     * Create an empty Blob
     *
     * @param allocator_ where to store allocated data when we need to
     *                   reallocate
     * @param size The current allocated size in allocator_ (so that we may
     *             use that space before doing reallocations)
     */
    Blob(std::unique_ptr<char[]>& allocator_, size_t size = 0)
        : Blob({nullptr, 0}, allocator_, false, size) {
    }

    /**
     * Create a Blob to operate on the given buffer. Note that the buffer
     * *MUST* be a valid xattr encoded buffer (if not you WILL crash!)
     * If the incoming buffer is snappy compressed, it must contain a compressed
     * xattr value.
     *
     * @param buffer an existing buffer to use
     * @param compressed the buffer contains snappy compressed data
     */
    Blob(cb::char_buffer buffer, bool compressed)
        : Blob(buffer, default_allocator, compressed, 0) {
    }

    /**
     * Create a Blob to operate on the given buffer by using the named
     * allocator to store new allocations. . Note that the buffer
     * *MUST* be a valid xattr encoded buffer (if not you WILL crash!)
     *
     * @param buffer the buffer containing the current encoded blob
     * @param allocator_ where to store allocated data when we need to
     *                   reallocate
     * @param compressed if the buffer contains snappy data, we will decompress
     * @param size The current allocated size in allocator_ (so that we may
     *             use that space before doing reallocations)
     */
    Blob(cb::char_buffer buffer,
         std::unique_ptr<char[]>& allocator_,
         bool compressed,
         size_t size = 0);

    /**
     * Create a (deep) copy of the Blob (allocate a new backing store)
     */
    Blob(const Blob& other);

    /**
     * Replace the contents of the Blob with the given buffer.
     *
     * If the incoming buffer is snappy compressed, it must contain a
     * compressed xattr value.
     *
     * @param buffer an existing buffer to use
     * @param compressed the buffer contains snappy compressed data
     */
    Blob& assign(cb::char_buffer buffer, bool compressed);

    /**
     * Get the value for a given key located in the blob
     *
     * @param key The key to look up
     * @return a buffer containing it's value. If not found the buffer length
     *         is 0
     */
    cb::char_buffer get(const cb::const_char_buffer& key) const;

    /**
     * Remove a given key (and its value) from the blob.
     *
     * @param key The key to remove
     */
    void remove(const cb::const_char_buffer& key);

    /**
     * Set (add or replace) the given key with the specified value.
     *
     * @param key The key to set
     * @param value The new value for the key
     */
    void set(const cb::const_char_buffer& key,
             const cb::const_char_buffer& value);

    void prune_user_keys();

    /**
     * Finalize the buffer and return it's content.
     *
     * We're currently keeping the blob finalized at all times so
     * we can return the content
     *
     * @return the encoded blob
     */
    cb::char_buffer finalize() {
        return blob;
    }

    /**
     * Get the size of the system xattr's located in the blob
     */
    size_t get_system_size() const;

    /**
     * Get pointer to the xattr data (raw data, including the len word)
     */
    const char* data() const {
        return blob.data();
    }

    /**
     * Get the current size of the Blob
     */
    size_t size() const {
        return blob.size();
    }

    /**
     * Get a JSON representation of the xattrs
     */
    nlohmann::json to_json() const;

    class iterator {
    public:

        iterator(const Blob& blob, size_t c)
            : blob(blob), current(c) {
        }

        iterator& operator++() {
            // Don't increment past the end
            if (current == blob.blob.size()) {
                return *this;
            }

            const auto size = blob.read_length(current);
            current += size;
            current += 4;

            // We're past the end, make this iterator match end()
            if (current > blob.blob.size()) {
                current = blob.blob.size();
            }
            return *this;
        }

        iterator operator++(int) {
            auto rv = *this;
            ++*this;
            return rv;
        }

        std::pair<cb::const_char_buffer, cb::const_char_buffer> operator*()
                const {
            auto* ptr = blob.blob.data() + current + 4;
            const auto keylen = strlen(ptr);
            cb::const_char_buffer key{ptr, keylen};
            ptr += (keylen + 1);
            cb::const_char_buffer value{ptr, strlen(ptr)};
            return {key, value};
        }

        friend bool operator==(const iterator&, const iterator&);
        friend bool operator!=(const iterator&, const iterator&);

        const Blob& blob;
        size_t current;
    };

    iterator begin() const {
        if (blob.empty()) {
            return end();
        }
        return iterator(*this, 4);
    }

    iterator end() const {
        return iterator(*this, blob.size());
    }

protected:

    /**
     * Expand the buffer and write the kv-pair at the end of the buffer
     *
     * @param key The key to append
     * @param value The value to store with the key
     */
    void append_kvpair(const cb::const_char_buffer& key,
                       const cb::const_char_buffer& value);

    /**
     * Write a kv-paid at the given offset
     *
     * @param offset The offset into the buffer to store the kv-pair
     * @param key The key to insert
     * @param value The value to insert
     */
    void write_kvpair(size_t offset,
                      const cb::const_char_buffer& key,
                      const cb::const_char_buffer& value);

    /**
     * Get the length stored at the given offset
     *
     * @param offset the offset into the buffer
     * @return the 32 bit value stored at that offset
     * @throw std::out_of_range if we ended up outside the legal range
     */
    uint32_t read_length(size_t offset) const;

    /**
     * Write a length in network byte order stored at a given location
     *
     * @param offset The offset into the buffer to store the length
     * @param value The value to store
     */
    void write_length(size_t offset, uint32_t value);

    /**
     * Grow the internal buffer so it is (at least) size bytes
     *
     * @param size the minimum number of bytes needed
     */
    void grow_buffer(uint32_t size);

    /**
     * Remove a segment within the blob starting at offset and spans
     * size bytes.
     *
     * @param offset The start offset we want to remove
     * @param size The number of bytes we want to remove
     */
    void remove_segment(const size_t offset, const size_t size);

private:
    cb::char_buffer blob;

    /// When the incoming data is compressed will auto-decompress into this
    cb::compression::Buffer decompressed;

    std::unique_ptr<char[]>& allocator;
    std::unique_ptr<char[]> default_allocator;
    size_t alloc_size;
};

inline bool operator==(const Blob::iterator& lhs, const Blob::iterator& rhs) {
    return lhs.current == rhs.current;
}

inline bool operator!=(const Blob::iterator& lhs, const Blob::iterator& rhs) {
    return !(lhs == rhs);
}

}
}
