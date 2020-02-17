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
    assign({buffer.data(), buffer.size()}, compressed);
}

Blob& Blob::assign(std::string_view buffer, bool compressed) {
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
        blob = {const_cast<char*>(buffer.data()),
                cb::xattr::get_body_offset(buffer)};
    } else {
        // empty blob
        blob = {};
    }
    return *this;
}

cb::char_buffer Blob::get(std::string_view key) const {
    try {
        size_t current = 4;
        while (current < blob.size()) {
            // Get the length of the next kv-pair
            const auto size = read_length(current);
            current += 4;
            if (size > key.size()) {
                // This may be the next key
                if (blob[current + key.size()] == '\0' &&
                    std::memcmp(blob.data() + current,
                                key.data(),
                                key.size()) == 0) {
                    // Yay this is the key!!!
                    auto* value = blob.buf + current + key.size() + 1;
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
        while (current < blob.size()) {
            // Get the length of the next kv-pair
            const auto size = read_length(current);

            if (blob[current + 4] != '_') {
                remove_segment(current, size + 4);
            } else {
                current += 4 + size;
            }
        }
    } catch (const std::out_of_range&) {
    }
}

void Blob::remove(std::string_view key) {
    // Locate the old value
    const auto old = get(key);
    if (old.empty()) {
        // it's not there
        return;
    }

    // there is no need to reallocate as we can just pack the buffer
    const auto offset = old.data() - blob.data() - 1 - key.size() - 4;
    const auto size = 4 + key.size() + 1 + old.size() + 1;

    remove_segment(offset, size);
}

void Blob::set(std::string_view key, std::string_view value) {
    if (value.empty()) {
        remove(key);
        return;
    }

    // Locate the old value
    const auto old = get(key);
    if (old.size() == value.size()) {
        // lets do an in-place replacement
        std::copy(value.begin(), value.end(), old.buf);
        return;
    } else if (old.size() == 0) {
        // The old one didn't exist
        append_kvpair(key, value);
    } else {
        // we need to reorganize the buffer. Determine the size of
        // the resulting document
        const size_t newsize = blob.size() + value.size() - old.size();
        const auto old_offset = old.data() - blob.data() - 1 - key.size() - 4;
        const auto old_kv_size = 4 + key.size() + 1 + old.size() + 1;

        if (newsize < alloc_size) {
            // we can do an in-place removement
            remove_segment(old_offset, old_kv_size);
        } else {
            std::unique_ptr<char[]> temp(new char[newsize]);
            // copy everything up to the old one
            std::copy(blob.data(), blob.data() + old_offset, temp.get());
            // Skip the old value and copy the rest
            std::copy(blob.data() + old_offset + old_kv_size,
                      blob.data() + blob.size(),
                      temp.get() + old_offset);
            allocator.swap(temp);
            blob = {allocator.get(),
                    newsize - 4 - key.size() - 1 - value.size() - 1};
            alloc_size = newsize;
        }

        append_kvpair(key, value);
    }
}

void Blob::grow_buffer(uint32_t size) {
    if (blob.size() < size) {
        if (alloc_size < size) {
            std::unique_ptr<char[]> temp(new char[size]);
            std::copy(blob.data(), blob.data() + blob.size(), temp.get());
            allocator.swap(temp);
            blob = {allocator.get(), size};
            alloc_size = size;
        } else {
            blob = {allocator.get(), size};
        }
    }
}

void Blob::write_kvpair(size_t offset,
                        std::string_view key,
                        std::string_view value) {
    // offset points to where we want to inject the value
    write_length(offset, uint32_t(key.size() + 1 + value.size() + 1));
    offset += 4;
    std::copy(key.begin(), key.end(), blob.data() + offset);
    offset += key.size();
    blob[offset++] = '\0';
    std::copy(value.begin(), value.end(), blob.data() + offset);
    offset += value.size();
    blob[offset++] = '\0';
    write_length(0, uint32_t(blob.size() - 4));
}

void Blob::append_kvpair(std::string_view key, std::string_view value) {
    auto offset = blob.size();
    if (offset == 0) {
        offset += 4;
    }

    const auto needed = offset + 4 + // length byte
                        key.size() + 1 + // zero terminated key
                        value.size() + 1; // zero terminated value

    grow_buffer(gsl::narrow<uint32_t>(needed));
    write_kvpair(offset, key, value);
}

void Blob::remove_segment(const size_t offset, const size_t size) {
    if (offset + size == blob.size()) {
        // No need to do anyting as this was the last thing in our blob..
        // just change the length
        blob.len = offset;

        if (blob.size() == 4) {
            // the last xattr removed... we could just nuke it..
            blob.len = 0;
        }
    } else {
        std::memmove(blob.data() + offset,
                     blob.data() + offset + size,
                     blob.size() - offset - size);
        blob.len -= size;
    }

    if (blob.size() > 0) {
        write_length(0, gsl::narrow<uint32_t>(blob.size()) - 4);
    }
}

void Blob::write_length(size_t offset, uint32_t value) {
    if (offset + 4 > blob.size()) {
        throw std::out_of_range("Blob::write_length: Access to " +
                                std::to_string(offset) +
                                " is outside the legal range of [0, " +
                                std::to_string(blob.size() - 1) + "]");
    }

    auto* ptr = reinterpret_cast<uint32_t*>(blob.data() + offset);
    *ptr = htonl(value);
}

uint32_t Blob::read_length(size_t offset) const {
    if (offset + 4 > blob.size()) {
        throw std::out_of_range("Blob::read_length: Access to " +
                                std::to_string(offset) +
                                " is outside the legal range of [0, " +
                                std::to_string(blob.size() - 1) + "]");
    }

    const auto* ptr = reinterpret_cast<const uint32_t*>(blob.data() + offset);
    return ntohl(*ptr);
}

size_t Blob::get_system_size() const {
    // special case.. there are no xattr's
    if (blob.size() == 0) {
        return 0;
    }

    // The global length field should be calculated as part of the
    // system xattr's
    size_t ret = 4;
    try {
        size_t current = 4;
        while (current < blob.size()) {
            // Get the length of the next kv-pair
            const auto size = read_length(current);
            if (blob[current + 4] == '_') {
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
        while (current < blob.size()) {
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
