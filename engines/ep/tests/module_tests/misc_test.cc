/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <platform/cbassert.h>
#include <iostream>

#include "common.h"

static void negativeValidate(const char *s) {
    uint16_t n;
    if (parseUint16(s, &n)) {
        std::cerr << "Expected error parsing " << s
                  << " returned " << n << std::endl;
        exit(1);
    }
}

static void positiveValidate(const char *s, uint16_t expected) {
    uint16_t n;
    if (!parseUint16(s, &n)) {
        std::cerr << "Error parsing " << s << std::endl;
        exit(1);
    }
    cb_assert(n == expected);
}

static void testUint16() {
    negativeValidate("-1");
    negativeValidate("65537");
    negativeValidate("65536");
    positiveValidate("65535", UINT16_MAX);
    positiveValidate("0", 0);
    positiveValidate("32768", 32768);
}

int main(int argc, char **argv) {
    (void)argc; (void)argv;
    testUint16();
}
