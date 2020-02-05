/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include "debug_helpers.h"

#include <platform/checked_snprintf.h>
#include <algorithm>
#include <cctype>
#include <stdexcept>

bool buf_to_printable_buffer(char* dest,
                             size_t destsz,
                             const char* src,
                             size_t srcsz) {
    char *ptr = dest;
    // Constrain src if dest cannot hold it all.
    srcsz = std::min(srcsz, destsz - 1);

    for (size_t ii = 0; ii < srcsz; ++ii, ++src, ++ptr) {
        if (std::isgraph(*src)) {
            *ptr = *src;
        } else {
            *ptr = '.';
        }
    }

    *ptr = '\0';
    return true;
}

bool key_to_printable_buffer(char* dest,
                             size_t destsz,
                             uint32_t client,
                             bool from_client,
                             const char* prefix,
                             const char* key,
                             size_t nkey) {
    try {
        auto nw = checked_snprintf(dest,
                                   destsz,
                                   "%c%u %s ",
                                   from_client ? '>' : '<',
                                   (int)client,
                                   prefix);
        char* ptr = dest + nw;
        destsz -= nw;
        return buf_to_printable_buffer(ptr, destsz, key, nkey);
    } catch (const std::overflow_error&) {
        return false;
    }
}

bool bytes_to_output_string(char* dest,
                            size_t destsz,
                            uint32_t client,
                            bool from_client,
                            const char* prefix,
                            const char* data,
                            size_t size) {
    try {
        auto offset = checked_snprintf(dest,
                                       destsz,
                                       "%c%u %s",
                                       from_client ? '>' : '<',
                                       client,
                                       prefix);
        for (size_t ii = 0; ii < size; ++ii) {
            if (ii % 4 == 0) {
                offset += checked_snprintf(dest + offset,
                                           destsz - offset,
                                           "\n%c%d  ",
                                           from_client ? '>' : '<',
                                           client);
            }

            offset += checked_snprintf(dest + offset,
                                       destsz - offset,
                                       " 0x%02x",
                                       (unsigned char)data[ii]);
        }

        checked_snprintf(dest + offset, destsz - offset, "\n");

        return true;
    } catch (const std::overflow_error&) {
        return false;
    }
}
