/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <fmt/format.h>
#include <folly/io/IOBuf.h>
#include <gsl/gsl-lite.hpp>
#include <nlohmann/json.hpp>
#include <platform/compress.h>
#include <xattr/blob.h>

#ifdef WIN32
#include <winsock2.h>
#else
#include <arpa/inet.h>
#endif
#include <algorithm>
#include <stdexcept>

namespace cb::xattr {

Blob::Blob(const Blob& other) {
    allocator = std::string{other.blob.data(), other.blob.size()};
    blob = {allocator.data(), allocator.size()};
}

Blob::Blob(cb::char_buffer buffer, bool compressed) {
    assign({buffer.data(), buffer.size()}, compressed);
}

Blob& Blob::assign(std::string_view buffer, bool compressed) {
    blob = {};
    allocator = {};

    if (buffer.empty()) {
        return *this;
    }

    if (compressed) {
        auto payload = cb::compression::inflateSnappy(buffer);
        if (!payload) {
            throw std::runtime_error(fmt::format(
                    "Blob::assign failed to inflate.  buffer.size:{}",
                    buffer.size()));
        }

        auto range = folly::StringPiece(payload->coalesce());
        auto size = cb::xattr::get_body_offset(range);
        Expects(size);

        allocator.resize(size);
        std::copy(range.begin(), range.begin() + size, allocator.data());
        blob = {allocator.data(), size};
        return *this;
    }

    // incoming data is not compressed, just get the size and attach
    blob = {const_cast<char*>(buffer.data()),
            cb::xattr::get_body_offset(buffer)};
    return *this;
}

std::string_view Blob::get(std::string_view key) const {
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
                    auto* value = blob.data() + current + key.size() + 1;
                    return {value, strlen(value)};
                }
                // jump to the next key!!
                current += size;
            } else {
                current += size;
            }
        }
    } catch (const std::out_of_range&) {
    }

    // Not found!
    return {};
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
    remove(key);
    if (!value.empty()) {
        append_kvpair(key, value);
    }
}

void Blob::grow_buffer(uint32_t size) {
    if (blob.size() < size) {
        if (allocator.size() < size) {
            allocator.resize(size);
            blob = {allocator.data(), size};
        } else {
            blob = {allocator.data(), size};
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
        blob = {blob.data(), offset};

        if (blob.size() == 4) {
            // the last xattr removed... we could just nuke it..
            blob = {};
        }
    } else {
        std::memmove(blob.data() + offset,
                     blob.data() + offset + size,
                     blob.size() - offset - size);
        blob = {blob.data(), blob.size() - size};
    }

    if (!blob.empty()) {
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

size_t Blob::get_xattrs_size(Type type) const {
    // special case.. there are no xattr's
    if (blob.empty()) {
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

            switch (type) {
            case Type::System:
                if (blob[current + 4] == '_') {
                    ret += size + 4;
                }
                break;
            case Type::User:
                if (blob[current + 4] != '_') {
                    ret += size + 4;
                }
                break;
            }

            current += 4 + size;
        }
    } catch (const std::out_of_range&) {
    }

    return ret;
}

size_t Blob::get_system_size() const {
    return get_xattrs_size(Type::System);
}

size_t Blob::get_user_size() const {
    return get_xattrs_size(Type::User);
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

std::string Blob::to_string() const {
    std::string ret;
    ret.reserve(blob.size());
    ret.push_back('{');
    size_t current = 4;
    while (current < blob.size()) {
        // Get the length of the next kv-pair
        const auto size = read_length(current);
        current += 4;

        auto* ptr = blob.data() + current;
        fmt::format_to(std::back_inserter(ret),
                       R"("{}":{},)",
                       ptr,
                       ptr + strlen(ptr) + 1);
        current += size;
    }
    if (ret.back() == ',') {
        ret.pop_back();
    }
    ret.push_back('}');
    return ret;
}

} // namespace cb::xattr
