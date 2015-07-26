/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#include "dynamic_buffer.h"

bool DynamicBuffer::grow(size_t needed) {
    size_t nsize = size;
    size_t available = nsize - offset;
    bool rv = true;

    /* Special case: No buffer -- need to allocate fresh */
    if (buffer == NULL) {
        nsize = 1024;
        available = size = offset = 0;
    }

    while (needed > available) {
        cb_assert(nsize > 0);
        nsize = nsize << 1;
        available = nsize - offset;
    }

    if (nsize != size) {
        char* ptr = reinterpret_cast<char*>(realloc(buffer, nsize));
        if (ptr) {
            buffer = ptr;
            size = nsize;
        } else {
            rv = false;
        }
    }

    return rv;
}

void DynamicBuffer::clear() {
    free(buffer);
    buffer = nullptr;
    size = 0;
    offset = 0;
}
