/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "util.h"

#include <folly/portability/GTest.h>

static void expect_equal(std::string_view input) {
    EXPECT_EQ(0, cbsasl_secure_compare(input, input))
            << "Input data: [" << input << "]";
}

static void expect_different(std::string_view a, std::string_view b) {
    EXPECT_NE(0, cbsasl_secure_compare(a, b))
            << "Ex[ected [" << a << "] to differ from [" << b << "]";
}

TEST(cbsasl, cbsasl_secure_compare) {
    // Check that it enforce the length without running outside the pointer
    // if we don't honor the length field we'll crash by following the
    // nullptr ;)
    EXPECT_EQ(0, cbsasl_secure_compare({}, {}));

    // Check that it compares right with equal length and same input
    expect_equal("");
    expect_equal("abcdef");
    expect_equal("@#$%^&*(&^%$#");
    expect_equal("jdfdsajk;14AFADSF517*(%(nasdfvlajk;ja''1345!%#!%$&%$&@$%@");

    // check that the case matter
    expect_different("abcd", "ABCD");
    expect_different("ABCD", "abcd");
    expect_different("a", "A");

    // check that it differs char from digits from special chars
    expect_different("a", "1");
    expect_different("a", "!");
    expect_different("!", "a");
    expect_different("1", "a");
}
