/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc.
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

#include <cbsasl/cbsasl.h>
#include "util.h"

int cbsasl_secure_compare(const char *a, size_t alen,
                          const char *b, size_t blen)
{
    size_t xx;
    size_t yi = 0;
    size_t bi = 0;
    int acc = (int)alen ^ (int)blen;

    for (xx = 0; xx < alen; ++xx) {
        acc |= a[xx] ^ b[bi];
        if (bi < blen) {
            ++bi;
        } else {
            ++yi;
        }
    }

    return acc;
}
