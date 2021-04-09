/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "util.h"

int cbsasl_secure_compare(const char* a,
                          size_t alen,
                          const char* b,
                          size_t blen) {
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
