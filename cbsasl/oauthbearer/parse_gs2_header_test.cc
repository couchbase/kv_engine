/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "parse_gs2_header.h"
#include <folly/portability/GTest.h>

using namespace cb::sasl::mechanism::oauthbearer;

class GS2ParserTest : public ::testing::Test {
protected:
    void invalid(std::string_view input, std::string_view error) {
        try {
            parse_gs2_header(input);
            FAIL() << "Expected std::invalid_argument";
        } catch (const std::invalid_argument& e) {
            EXPECT_EQ(error, e.what());
        } catch (...) {
            FAIL() << "Expected exception of type std::invalid_argument";
        }
    }
};

TEST_F(GS2ParserTest, ParseMinimalGS2Header) {
    auto result = parse_gs2_header("n,,");
    EXPECT_TRUE(result.empty());
}

TEST_F(GS2ParserTest, ParseGS2HeaderWithAuthzid) {
    auto result = parse_gs2_header("n,a=foo,");
    EXPECT_EQ("foo", result);
}

TEST_F(GS2ParserTest, ParseUnsupportedChannelBinding) {
    invalid("y,,", "Only 'n' (no channel binding) is supported");
}

TEST_F(GS2ParserTest, ParseMissingTrailingComma) {
    invalid("n,", "Invalid GS2 header");
}

TEST_F(GS2ParserTest, ParseInvalidAuthzid) {
    // authzid should be "a=saslname"
    invalid("n,b=foo,", "Invalid GS2 header");
}

TEST_F(GS2ParserTest, ParseInvalidAuthzidNoUsername) {
    // saslname can't be empty
    invalid("n,a=,", "Invalid saslname");
}
