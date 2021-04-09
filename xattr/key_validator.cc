/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <cctype>
#include <locale>
#include <stdexcept>

#include <xattr/key_validator.h>
#include <daemon/subdocument_validators.h>

class encoding_error : public std::runtime_error {
public:
    explicit encoding_error(const char* msg) : std::runtime_error(msg) {
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
int get_utf8_char_width(const char* ptr, size_t avail) {
    if (static_cast<uint8_t>(ptr[0]) < 0x80) {
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

bool is_valid_xattr_key(std::string_view path, size_t& key_length) {
    // Check for the random list of reserved leading characters.
    size_t dot = path.size();
    bool system = false;

    try {
        const auto length = path.size();
        size_t offset = 0;
        const char* ptr = path.data();

        while (offset < length) {
            auto width = get_utf8_char_width(ptr, length - offset);
            if (width == 1) {
                if (offset == 0) {
                    if (std::ispunct(path[0], loc) ||
                        std::iscntrl(path[0], loc)) {
                        if (path[0] == '_' || path[0] == '$') {
                            system = true;
                        } else {
                            return false;
                        }
                    }
                }

                if ((*ptr == '.' || *ptr == '[') && dot == length) {
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
