/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "stringutils.h"

#include <stdexcept>

/**
 * According to the RFC:
 *
 * 2.3.  Prohibited Output
 *
 *    This profile specifies the following characters as prohibited input:
 *
 *       - Non-ASCII space characters [StringPrep, C.1.2]
 *       - ASCII control characters [StringPrep, C.2.1]
 *       - Non-ASCII control characters [StringPrep, C.2.2]
 *       - Private Use characters [StringPrep, C.3]
 *       - Non-character code points [StringPrep, C.4]
 *       - Surrogate code points [StringPrep, C.5]
 *       - Inappropriate for plain text characters [StringPrep, C.6]
 *       - Inappropriate for canonical representation characters
 *         [StringPrep, C.7]
 *       - Change display properties or deprecated characters
 *         [StringPrep, C.8]
 *       - Tagging characters [StringPrep, C.9]
 */
std::string SASLPrep(std::string_view string) {
    for (std::size_t ii = 0; ii < string.size(); ++ii) {
        unsigned char cc = string[ii];

        if (cc < 0x20) {
            throw std::runtime_error(
                    "SASLPrep: ASCII control characters are not allowed");
        }

        if (cc == 0x7F) {
            throw std::runtime_error("SASLPrep: DEL character is not allowed");
        }

        if (cc & 0x80) {
            int expected_bytes = 0;
            // 0xC0/0xC1 are overlong; 0xF5-0xF7 exceed U+10FFFF — both invalid
            if ((cc & 0xE0) == 0xC0 && cc >= 0xC2) {
                expected_bytes = 1;
            } else if ((cc & 0xF0) == 0xE0) {
                expected_bytes = 2;
            } else if ((cc & 0xF8) == 0xF0 && cc <= 0xF4) {
                expected_bytes = 3;
            } else {
                throw std::runtime_error(
                        "SASLPrep: Invalid UTF-8 sequence detected");
            }

            for (int jj = 0; jj < expected_bytes; ++jj) {
                ++ii;
                if (ii >= string.size()) {
                    throw std::runtime_error(
                            "SASLPrep: Incomplete UTF-8 sequence at end of "
                            "string");
                }
                unsigned char continuation = string[ii];
                if ((continuation & 0xC0) != 0x80) {
                    throw std::runtime_error(
                            "SASLPrep: Invalid UTF-8 continuation byte");
                }
            }
        }
    }

    return std::string{string};
}
