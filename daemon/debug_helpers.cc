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
