/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "builder.h"
#include "token.h"

#include <folly/portability/GTest.h>

using namespace cb::jwt;
using namespace std::chrono;

TEST(Builder, TestDefaultNoSignature) {
    auto builder = Builder::create();
    auto encoded = builder->build();
    EXPECT_EQ(
            "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0."
            "eyJpc3MiOiJjYi11bml0LXRlc3RzIn0",
            encoded);
    auto token = Token::parse(encoded);
    EXPECT_EQ(R"({"alg":"none","typ":"JWT"})"_json, token->header);
    EXPECT_EQ(R"({"iss":"cb-unit-tests"})"_json, token->payload);
    EXPECT_FALSE(token->verified);
}

TEST(Builder, TestDefaultWithSignature) {
    auto builder = Builder::create("HS256");
    auto encoded = builder->build();
    EXPECT_EQ(
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
            "eyJpc3MiOiJjYi11bml0LXRlc3RzIn0.0JIlsQsDvra-DexK_NT8jBeRyb1-"
            "TpX4MRZ7f_oGcx0",
            encoded);
    auto token = Token::parse(encoded, []() -> std::string { return {}; });
    EXPECT_EQ(R"({"alg":"HS256","typ":"JWT"})"_json, token->header);
    EXPECT_EQ(R"({"iss":"cb-unit-tests"})"_json, token->payload);
    EXPECT_TRUE(token->verified);
}

TEST(Builder, TestCompare) {
    auto builder = Builder::create();
    builder->setExpiration(system_clock::now() + minutes(10));
    auto encoded1 = builder->build();

    builder->setNotBefore(system_clock::now() + minutes(1));
    builder->setIssuedAt(system_clock::now());

    auto encoded2 = builder->build();

    auto token1 = Token::parse(encoded1);
    auto token2 = Token::parse(encoded2);

    EXPECT_TRUE(token1->onlyTimestampsDiffers(*token2));
    builder->setSubject("foo");
    token2 = Token::parse(builder->build());
    EXPECT_FALSE(token1->onlyTimestampsDiffers(*token2));
}

TEST(Builder, UnknownAlgorithm) {
    try {
        auto builder = Builder::create("foo");
        FAIL() << "Unknown algorithm should have failed";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ("Invalid Algorithm", e.what());
    }
}

TEST(Builder, IncorrectSignature) {
    auto builder = Builder::create("HS256");
    auto encoded = builder->build();
    EXPECT_EQ(
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
            "eyJpc3MiOiJjYi11bml0LXRlc3RzIn0.0JIlsQsDvra-DexK_NT8jBeRyb1-"
            "TpX4MRZ7f_oGcx0",
            encoded);

    // Invalidate the signature:
    encoded.back() = 'X';
    try {
        auto token = Token::parse(encoded, []() -> std::string { return {}; });
        FAIL() << "Invalid signature should be detected";
    } catch (const std::runtime_error& e) {
        EXPECT_STREQ("Token::parse: Invalid signature", e.what());
    }
}
