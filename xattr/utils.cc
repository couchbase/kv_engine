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
#include <JSON_checker.h>
#include <memcached/protocol_binary.h>
#include <xattr/blob.h>
#include <xattr/key_validator.h>
#include <xattr/utils.h>

#include <nlohmann/json.hpp>

#include <unordered_set>

namespace cb {
namespace xattr {

/**
 * Small utility function to trim the blob object into a '\0' terminated
 * string.
 *
 * @param blob the blob object to operate on
 * @return the trimmed string
 * @throws std::underflow_error if there isn't a '\0' in the buffer
 */
static cb::const_char_buffer trim_string(cb::const_char_buffer blob) {
    const auto* end = (const char*)std::memchr(blob.buf, '\0', blob.len);
    if (end == nullptr) {
        throw std::out_of_range("trim_string: no '\\0' in the input buffer");
    }

    return {blob.buf, size_t(end - blob.buf)};
}

bool validate(const cb::const_char_buffer& blob) {
    if (blob.len < 4) {
        // we must have room for the length field
        return false;
    }

    // Check that the offset of the body is within the blob (note that it
    // may be the same size as the blob if the actual data payload is empty
    auto size = get_body_offset(blob);
    if (size > blob.len) {
        return false;
    }

    // @todo fix the hash thing so I can use the keybuf directly
    std::unordered_set<std::string> keys;

    // You probably want to look in docs/Document.md for a detailed
    // description of the actual memory layout and why I'm adding
    // these "magic" values.
    size_t offset = 4;
    try {
        JSON_checker::Validator validator;

        // Iterate over all of the KV pairs
        while (offset < size) {
            // The next pair _must_ at least have:
            //    4  byte length field,
            //    1  byte key
            //    2x 1 byte '\0'
            if (offset + 7 > size) {
                return false;
            }

            const auto kvsize = ntohl(
                *reinterpret_cast<const uint32_t*>(blob.buf + offset));
            offset += 4;
            if (offset + kvsize > size) {
                // The kvsize exceeds the blob size
                return false;
            }

            // pick out the key
            const auto keybuf = trim_string({blob.buf + offset, size - offset});
            offset += keybuf.len + 1; // swallow the '\0'

            // Validate the key
            if (!is_valid_xattr_key({keybuf.buf, keybuf.len})) {
                return false;
            }

            // pick out the value
            const auto valuebuf = trim_string({blob.buf + offset, size - offset});
            offset += valuebuf.len + 1; // swallow '\0'

            // Validate the value (must be legal json)
            if (!validator.validate(valuebuf.buf)) {
                // Failed to parse the JSON
                return false;
            }

            if (kvsize != (keybuf.len + valuebuf.len + 2)) {
                return false;
            }

            if (!keys.insert(std::string{keybuf.buf, keybuf.len}).second) {
                return false;
            }
        }
    } catch (const std::out_of_range&) {
        return false;
    }

    return offset == size;
}

// Test that a len doesn't exceed size, the idea that len is the value read from
// an xattr payload and size is the document size
static void check_len(uint32_t len, size_t size) {
    if (len > size) {
        throw std::out_of_range("xattr::utils::check_len(" +
                                std::to_string(len) + ") exceeds " +
                                std::to_string(size));
    }
}

uint32_t get_body_offset(const cb::const_char_buffer& payload) {
    Expects(payload.size() > 0);
    const uint32_t* lenptr = reinterpret_cast<const uint32_t*>(payload.buf);
    auto len = ntohl(*lenptr);
    check_len(len, payload.size());
    return len + sizeof(uint32_t);
}

uint32_t get_body_offset(const cb::char_buffer& payload) {
    Expects(payload.size() > 0);
    const uint32_t* lenptr = reinterpret_cast<const uint32_t*>(payload.buf);
    auto len = ntohl(*lenptr);
    check_len(len, payload.size());
    return len + sizeof(uint32_t);
}

const_char_buffer get_body(const cb::const_char_buffer& payload) {
    auto offset = get_body_offset(payload);
    return {payload.buf + offset, payload.len - offset};
}

size_t get_system_xattr_size(uint8_t datatype, const cb::const_char_buffer doc) {
    if (!::mcbp::datatype::is_xattr(datatype)) {
        return 0;
    }

    Blob blob({const_cast<char*>(doc.data()), doc.size()},
              ::mcbp::datatype::is_snappy(datatype));
    return blob.get_system_size();
}

size_t get_body_size(uint8_t datatype, cb::const_char_buffer value) {
    cb::compression::Buffer uncompressed;
    if (::mcbp::datatype::is_snappy(datatype)) {
        if (!cb::compression::inflate(
                    cb::compression::Algorithm::Snappy, value, uncompressed)) {
            throw std::invalid_argument(
                    "get_body_size: Failed to inflate data");
        }
        value = uncompressed;
    }

    if (value.size() == 0) {
        return 0;
    }

    if (!::mcbp::datatype::is_xattr(datatype)) {
        return value.size();
    }

    return value.size() - get_body_offset(value);
}

std::string make_wire_encoded_string(
        const std::string& body,
        const std::unordered_map<std::string, std::string>& xattrSet) {
    Blob xattrs;
    for (auto itr = xattrSet.begin(); itr != xattrSet.end(); ++itr) {
        xattrs.set(itr->first, itr->second);
    }
    auto data = xattrs.finalize();
    std::string encoded{data.data(), data.size()};
    if (!cb::xattr::validate(encoded)) {
        throw std::logic_error(
                "cb::xattr::make_wire_encoded_string Invalid xattr encoding");
    }
    encoded.append(body);
    return encoded;
}

}
}
