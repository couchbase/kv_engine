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

#include <stdexcept>

int cbsasl_secure_compare(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) {
        throw std::runtime_error(
                "cbsasl_secure_compare: a and b have different length");
    }
    int acc = 0;
    for (std::size_t xx = 0; xx < a.size(); ++xx) {
        acc |= a[xx] ^ b[xx];
    }

    return acc;
}
