/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include <cctype>
#include <cstdio>

ssize_t buf_to_printable_buffer(char *dest, size_t destsz,
                                const char *src, size_t srcsz)
{
    char *ptr = dest;
    if (srcsz > destsz) {
        srcsz = destsz;
    }

    for (size_t ii = 0; ii < srcsz; ++ii, ++src, ++ptr) {
        if (std::isgraph(*src)) {
            *ptr = *src;
        } else {
            *ptr = '.';
        }
    }

    *ptr = '\0';
    return (ssize_t)(ptr - dest);
}

ssize_t key_to_printable_buffer(char *dest, size_t destsz, SOCKET client,
                                bool from_client, const char *prefix,
                                const char *key, size_t nkey)
{
    ssize_t nw = std::snprintf(dest, destsz, "%c%d %s ",
                               from_client ? '>' : '<',
                               (int)client, prefix);
    if (nw == -1) {
        return -1;
    }
    char* ptr = dest + nw;
    destsz -= nw;
    return buf_to_printable_buffer(ptr, destsz, key, nkey);
}

