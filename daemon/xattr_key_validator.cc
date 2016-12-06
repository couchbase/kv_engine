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
#include <cctype>
#include <locale>
#include <stdexcept>

#include "xattr_key_validator.h"
#include "subdocument_validators.h"

class encoding_error : public std::runtime_error {
public:
    encoding_error(const char* msg) : std::runtime_error(msg) {
    }
};

static std::locale loc("C");


/**
 * Get the width of the current UTF8 'character'
 *
 * @param ptr pointer to the raw byte array
 * @param avail The number of bytes available
 * @return the number of bytes this character occupies
 * @throws std::underflow_error if the encoding require more bits to follow
 *
 */
int get_utf8_char_width(const uint8_t* ptr, size_t avail) {
    if (ptr[0] < 0x80) {
        return 1;
    }

    if (avail < 2) {
        throw encoding_error("get_char_width: not enough bytes");
    }

    if ((ptr[0] & 0xE0) == 0xC0) {
        if ((ptr[1] & 0xC0) == 0x80) {
            return 2;
        }
        throw encoding_error("get_char_width: Invalid utf8 encoding");
    }

    if (avail < 3) {
        throw encoding_error("get_char_width: not enough bytes");
    }

    if ((ptr[0] & 0xf0) == 0xE0) {
        if (((ptr[1] & 0xC0) == 0x80) &&
            ((ptr[2] & 0xC0) == 0x80)) {
            return 3;
        }
        throw encoding_error("get_char_width: Invalid utf8 encoding");
    }

    if (avail < 4) {
        throw encoding_error("get_char_width: not enough bytes");
    }

    if ((ptr[0] & 0xf8) == 0xF0) {
        if (((ptr[1] & 0xC0) == 0x80) &&
            ((ptr[2] & 0xC0) == 0x80) &&
            ((ptr[3] & 0xC0) == 0x80)) {
            return 4;
        }
    }

    throw encoding_error("get_char_width: Invalid utf8 encoding");
}

bool is_valid_xattr_key(cb::const_byte_buffer path, size_t& key_length) {
    // Check for the random list of reserved leading characters.
    size_t dot = path.len;
    bool system = false;

    try {
        const auto length = path.len;
        size_t offset = 0;
        const uint8_t* ptr = path.buf;

        while (offset < length) {
            auto width = get_utf8_char_width(ptr, length - offset);
            if (width == 1) {
                if (offset == 0) {
                    if (std::ispunct(static_cast<char>(path.buf[0]), loc) ||
                        std::iscntrl(static_cast<char>(path.buf[0]), loc)) {

                        if (path.buf[0] == '_') {
                            system = true;
                        } else {
                            return false;
                        }
                    }
                }

                if (*ptr == '.' && dot == length) {
                    dot = offset;
                }

                if (*ptr == 0x00) {
                    // 0 not valid in modified UTF-8
                    return false;
                }
            }

            offset += width;
            ptr += width;
        }
    } catch (const encoding_error&) {
        return false;
    }

    if (dot == 0 || dot >= SUBDOC_MAX_XATTR_LENGTH || (system && dot == 1)) {
        return false;
    }

    key_length = dot;
    return true;
}
