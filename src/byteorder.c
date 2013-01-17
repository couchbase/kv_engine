/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

static uint64_t mc_swap64(uint64_t in) {
#ifndef WORDS_BIGENDIAN
    /* Little endian, flip the bytes around until someone makes a faster/better
    * way to do this. */
    uint64_t rv = 0;
    int i = 0;
    for(i = 0; i<8; i++) {
        rv = (rv << 8) | (in & 0xff);
        in >>= 8;
    }
    return rv;
#else
    /* big-endian machines don't need byte swapping */
    return in;
#endif
}

uint64_t ntohll(uint64_t val) {
    return mc_swap64(val);
}

uint64_t htonll(uint64_t val) {
    return mc_swap64(val);
}
