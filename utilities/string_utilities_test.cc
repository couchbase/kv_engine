/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "string_utilities.h"
#include <folly/portability/GTest.h>

TEST(Base64, Encode) {
    EXPECT_EQ(R"("cb-content-base64-encoded:YmFzZTY0X2VuY29kZV92YWx1ZQ==")",
              base64_encode_value("base64_encode_value"));
    EXPECT_EQ(R"("cb-content-base64-encoded:")", base64_encode_value(""));
}

TEST(Base64, Decode) {
    EXPECT_EQ(
            "base64_encode_value",
            base64_decode_value(
                    R"("cb-content-base64-encoded:YmFzZTY0X2VuY29kZV92YWx1ZQ==")"));

    EXPECT_EQ("", base64_decode_value(R"("cb-content-base64-encoded:")"));
}

TEST(Base64, EncodeNoData) {
    EXPECT_EQ(R"("cb-content-base64-encoded:")", base64_encode_value({}));
}

TEST(Base64, NoData) {
    for (const char*& str : std::vector<const char*>{R"(")", ""}) {
        try {
            base64_decode_value(str);
            FAIL() << "Decode should fail for " << str;
        } catch (const std::invalid_argument& e) {
            EXPECT_STREQ(
                    "base64_decode_value(): provided value is not base64 "
                    "encoded value",
                    e.what());
        }
    }
}

TEST(Base64, MissingMarker) {
    try {
        base64_decode_value("foo");
        FAIL() << "Decode should fail for \"foo\"";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ(
                "base64_decode_value(): encoded value does not start with "
                "magic",
                e.what());
    }
}

TEST(Base64, MissingValue) {
    EXPECT_EQ("", base64_decode_value("cb-content-base64-encoded:"));
}
