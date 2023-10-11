/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <nlohmann/json_fwd.hpp>
#include <platform/compress.h>
#include <xattr/utils.h>
#include <cstddef>
#include <memory>

namespace cb::xattr {

/**
 * The cb::xattr::Blob is a class that provides easy access to the
 * binary format of the blob.
 */
class Blob {
public:
    /// Create an empty Blob
    Blob() : Blob({}, false) {
    }

    /**
     * Create a Blob to operate on the given buffer by using the named
     * allocator to store new allocations. . Note that the buffer
     * *MUST* be a valid xattr encoded buffer (if not you WILL crash!)
     *
     * @param buffer the buffer containing the current encoded blob
     * @param compressed if the buffer contains snappy data, we will decompress
     */
    Blob(cb::char_buffer buffer, bool compressed);

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
    Blob& assign(std::string_view buffer, bool compressed);

    /**
     * Get the value for a given key located in the blob
     *
     * @param key The key to look up
     * @return a buffer containing it's value. If not found the buffer length
     *         is 0
     */
    cb::char_buffer get(std::string_view key);

    /**
     * Remove a given key (and its value) from the blob.
     *
     * @param key The key to remove
     */
    void remove(std::string_view key);

    /**
     * Set (add or replace) the given key with the specified value.
     *
     * @param key The key to set
     * @param value The new value for the key
     */
    void set(std::string_view key, std::string_view value);

    void prune_user_keys();

    /**
     * Finalize the buffer and return it's content.
     *
     * We're currently keeping the blob finalized at all times so
     * we can return the content
     *
     * @return the encoded blob
     */
    std::string_view finalize() {
        return std::string_view(blob.data(), blob.size());
    }

    /**
     * Get the size of the system xattrs located in the blob
     */
    size_t get_system_size() const;

    /**
     * Get the size of the user xattrs located in the blob
     */
    size_t get_user_size() const;

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

    /// Get a JSON representation of the xattrs
    nlohmann::json to_json() const;

    /// Get a JSON representation of the xattrs. If the use case would be
    /// something like: to_json().dump() building the JSON object would
    /// need to parse all values which could be relatively expensive.
    std::string to_string() const;

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

        std::pair<std::string_view, std::string_view> operator*() const {
            auto* ptr = blob.blob.data() + current + 4;
            const auto keylen = strlen(ptr);
            std::string_view key{ptr, keylen};
            ptr += (keylen + 1);
            std::string_view value{ptr, strlen(ptr)};
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
    void append_kvpair(std::string_view key, std::string_view value);

    /**
     * Write a kv-paid at the given offset
     *
     * @param offset The offset into the buffer to store the kv-pair
     * @param key The key to insert
     * @param value The value to insert
     */
    void write_kvpair(size_t offset,
                      std::string_view key,
                      std::string_view value);

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
    enum class Type : uint8_t { System, User };

    /**
     * Get the size of the specific category of Xattrs located in the blob
     */
    size_t get_xattrs_size(Type type) const;

    cb::char_buffer blob;

    /// When the incoming data is compressed will auto-decompress into this
    cb::compression::Buffer decompressed;

    std::unique_ptr<char[]> allocator;
    size_t alloc_size = 0;
};

inline bool operator==(const Blob::iterator& lhs, const Blob::iterator& rhs) {
    return lhs.current == rhs.current;
}

inline bool operator!=(const Blob::iterator& lhs, const Blob::iterator& rhs) {
    return !(lhs == rhs);
}

} // namespace cb::xattr
