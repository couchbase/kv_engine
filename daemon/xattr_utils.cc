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
#include "config.h"
#include "xattr_utils.h"
#include "xattr_key_validator.h"

#include <unordered_set>
#include <cJSON_utils.h>

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
            if (!is_valid_xattr_key({(const uint8_t*)keybuf.buf, keybuf.len})) {
                return false;
            }

            // pick out the value
            const auto valuebuf = trim_string({blob.buf + offset, size - offset});
            offset += valuebuf.len + 1; // swallow '\0'

            // Validate the value (must be legal json)
            unique_cJSON_ptr payload{cJSON_Parse(valuebuf.buf)};
            if (!payload) {
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

uint32_t get_body_offset(const const_char_buffer& payload) {
    const uint32_t* lenptr = reinterpret_cast<const uint32_t*>(payload.buf);
    return ntohl(*lenptr) + sizeof(uint32_t);
}

uint32_t get_body_offset(const char_buffer& payload) {
    const uint32_t* lenptr = reinterpret_cast<const uint32_t*>(payload.buf);
    return ntohl(*lenptr) + sizeof(uint32_t);
}

const_char_buffer get_body(const const_char_buffer& payload) {
    auto offset = get_body_offset(payload);
    return {payload.buf + offset, payload.len - offset};
}

char_buffer get_body(const char_buffer& payload) {
    auto offset = get_body_offset(payload);
    return {payload.buf + offset, payload.len - offset};
}

const_char_buffer get_xattr(const const_char_buffer& payload) {
    const uint32_t* lenptr = reinterpret_cast<const uint32_t*>(payload.buf);
    return {payload.buf + sizeof(uint32_t), *lenptr};
}

char_buffer get_xattr(const char_buffer& payload) {
    const uint32_t* lenptr = reinterpret_cast<const uint32_t*>(payload.buf);
    return {payload.buf + sizeof(uint32_t), *lenptr};
}

}
}
