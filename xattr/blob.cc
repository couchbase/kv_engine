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
#include <nlohmann/json.hpp>
#include <xattr/blob.h>
#include <gsl/gsl>

#ifdef WIN32
#include <winsock2.h>
#else
#include <arpa/inet.h>
#endif
#include <algorithm>
#include <stdexcept>

namespace cb {
namespace xattr {

Blob::Blob(const Blob& other)
    : allocator(default_allocator),
      alloc_size(other.blob.size()) {
    decompressed.resize(other.decompressed.size());
    std::copy_n(other.decompressed.data(),
                other.decompressed.size(),
                decompressed.data());
    allocator.reset(new char[alloc_size]);
    blob = { allocator.get(), alloc_size };
    std::copy(other.blob.begin(), other.blob.end(), blob.begin());
}

Blob::Blob(cb::char_buffer buffer,
           std::unique_ptr<char[]>& allocator_,
           bool compressed,
           size_t size)
    : blob(buffer), allocator(allocator_), alloc_size(size) {
    assign(buffer, compressed);
}

Blob& Blob::assign(cb::char_buffer buffer, bool compressed) {
    if (compressed && !buffer.empty()) {
        // inflate and attach blob to the compression::buffer
        if (!cb::compression::inflate(
                    cb::compression::Algorithm::Snappy,
                    {static_cast<const char*>(buffer.data()), buffer.size()},
                    decompressed)) {
            // inflate (de-compress) failed.  Try to grab the
            // uncompressedLength for debugging purposes - zero indicates
            // that it failed to return the uncompressedLength.
            size_t uncompressedLength =
                    cb::compression::get_uncompressed_length(
                            cb::compression::Algorithm::Snappy,
                            {static_cast<const char*>(buffer.data()),
                             buffer.size()});
            throw std::runtime_error(
                    "Blob::assign failed to inflate.  buffer.size:" +
                    std::to_string(buffer.size()) + " uncompressedLength:" +
                    std::to_string(uncompressedLength));
        }

        // Connect blob to the decompressed xattrs after resizing which in
        // theory /could/ release the now un-required non xattr data
        decompressed.resize(cb::xattr::get_body_offset(decompressed));
        blob = {decompressed.data(), decompressed.size()};
    } else if (!buffer.empty()) {
        // incoming data is not compressed, just get the size and attach
        blob = {buffer.data(), cb::xattr::get_body_offset(buffer)};
    } else {
        // empty blob
        blob = {};
    }
    return *this;
}

cb::char_buffer Blob::get(const cb::const_char_buffer& key) const {
    try {
        size_t current = 4;
        while (current < blob.len) {
            // Get the length of the next kv-pair
            const auto size = read_length(current);
            current += 4;
            if (size > key.len) {
                // This may be the next key
                if (blob.buf[current + key.len] == '\0' &&
                    std::memcmp(blob.buf + current, key.buf, key.len) == 0) {
                    // Yay this is the key!!!
                    auto* value = blob.buf + current + key.len + 1;
                    return {value, strlen(value)};
                } else {
                    // jump to the next key!!
                    current += size;
                }
            } else {
                current += size;
            }
        }
    } catch (const std::out_of_range&) {
    }

    // Not found!
    return {nullptr, 0};
}

void Blob::prune_user_keys() {
    try {
        size_t current = 4;
        while (current < blob.len) {
            // Get the length of the next kv-pair
            const auto size = read_length(current);

            if (blob.buf[current + 4] != '_') {
                remove_segment(current, size + 4);
            } else {
                current += 4 + size;
            }
        }
    } catch (const std::out_of_range&) {
    }
}

void Blob::remove(const cb::const_char_buffer& key) {
    // Locate the old value
    const auto old = get(key);
    if (old.len == 0) {
        // it's not there
        return;
    }

    // there is no need to reallocate as we can just pack the buffer
    const auto offset = old.buf - blob.buf - 1 - key.len - 4;
    const auto size = 4 + key.len + 1 + old.len + 1;

    remove_segment(offset, size);
}

void Blob::set(const cb::const_char_buffer& key,
               const cb::const_char_buffer& value) {
    if (value.len == 0) {
        remove(key);
        return;
    }

    // Locate the old value
    const auto old = get(key);
    if (old.len == value.len) {
        // lets do an in-place replacement
        std::copy(value.buf, value.buf + value.len, old.buf);
        return;
    } else if (old.len == 0) {
        // The old one didn't exist
        append_kvpair(key, value);
    } else {
        // we need to reorganize the buffer. Determine the size of
        // the resulting document
        const size_t newsize = blob.len + value.len - old.len;
        const auto old_offset = old.buf - blob.buf - 1 - key.len - 4;
        const auto old_kv_size = 4 + key.len + 1 + old.len + 1;

        if (newsize < alloc_size) {
            // we can do an in-place removement
            remove_segment(old_offset, old_kv_size);
        } else {
            std::unique_ptr<char[]> temp(new char[newsize]);
            // copy everything up to the old one
            std::copy(blob.buf, blob.buf + old_offset, temp.get());
            // Skip the old value and copy the rest
            std::copy(blob.buf + old_offset + old_kv_size,
                      blob.buf + blob.len, temp.get() + old_offset);
            allocator.swap(temp);
            blob = {allocator.get(), newsize - 4 - key.len - 1 - value.len - 1};
            alloc_size = newsize;
        }

        append_kvpair(key, value);
    }
}

void Blob::grow_buffer(uint32_t size) {
    if (blob.len < size) {
        if (alloc_size < size) {
            std::unique_ptr<char[]> temp(new char[size]);
            std::copy(blob.buf, blob.buf + blob.len, temp.get());
            allocator.swap(temp);
            blob = {allocator.get(), size};
            alloc_size = size;
        } else {
            blob = {allocator.get(), size};
        }
    }
}

void Blob::write_kvpair(size_t offset,
                        const cb::const_char_buffer& key,
                        const cb::const_char_buffer& value) {
    // offset points to where we want to inject the value
    write_length(offset, uint32_t(key.len + 1 + value.len + 1));
    offset += 4;
    std::copy(key.buf, key.buf + key.len, blob.buf + offset);
    offset += key.len;
    blob.buf[offset++] = '\0';
    std::copy(value.buf, value.buf + value.len, blob.buf + offset);
    offset += value.len;
    blob.buf[offset++] = '\0';
    write_length(0, uint32_t(blob.len - 4));
}

void Blob::append_kvpair(const cb::const_char_buffer& key,
                         const cb::const_char_buffer& value) {
    auto offset = blob.len;
    if (offset == 0) {
        offset += 4;
    }

    const auto needed = offset +
                        4 + // length byte
                        key.len + 1 + // zero terminated key
                        value.len + 1; // zero terminated value

    grow_buffer(gsl::narrow<uint32_t>(needed));
    write_kvpair(offset, key, value);
}

void Blob::remove_segment(const size_t offset, const size_t size) {
    if (offset + size == blob.len) {
        // No need to do anyting as this was the last thing in our blob..
        // just change the length
        blob.len = offset;

        if (blob.len == 4) {
            // the last xattr removed... we could just nuke it..
            blob.len = 0;
        }
    } else {
        std::memmove(blob.buf + offset, blob.buf + offset + size,
                     blob.len - offset - size);
        blob.len -= size;
    }

    if (blob.len > 0) {
        write_length(0, gsl::narrow<uint32_t>(blob.len) - 4);
    }
}

void Blob::write_length(size_t offset, uint32_t value) {
    if (offset + 4 > blob.len) {
        throw std::out_of_range("Blob::write_length: Access to " +
                                std::to_string(offset) +
                                " is outside the legal range of [0, " +
                                std::to_string(blob.len - 1) + "]");
    }

    auto* ptr = reinterpret_cast<uint32_t*>(blob.buf + offset);
    *ptr = htonl(value);
}

uint32_t Blob::read_length(size_t offset) const {
    if (offset + 4 > blob.len) {
        throw std::out_of_range("Blob::read_length: Access to " +
                                std::to_string(offset) +
                                " is outside the legal range of [0, " +
                                std::to_string(blob.len - 1) + "]");
    }

    const auto* ptr = reinterpret_cast<const uint32_t*>(blob.buf + offset);
    return ntohl(*ptr);
}

size_t Blob::get_system_size() const {
    // special case.. there are no xattr's
    if (blob.len == 0) {
        return 0;
    }

    // The global length field should be calculated as part of the
    // system xattr's
    size_t ret = 4;
    try {
        size_t current = 4;
        while (current < blob.len) {
            // Get the length of the next kv-pair
            const auto size = read_length(current);
            if (blob.buf[current + 4] == '_') {
                ret += size + 4;
            }
            current += 4 + size;
        }
    } catch (const std::out_of_range&) {
    }

    return ret;
}

nlohmann::json Blob::to_json() const {
    nlohmann::json ret;

    try {
        size_t current = 4;
        while (current < blob.len) {
            // Get the length of the next kv-pair
            const auto size = read_length(current);
            current += 4;

            auto* ptr = blob.data() + current;
            ret[ptr] = nlohmann::json::parse(ptr + strlen(ptr) + 1);

            current += size;
        }
    } catch (const std::out_of_range&) {
    }

    return ret;
}

} // namespace xattr
} // namespace cb
