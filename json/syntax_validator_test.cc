/*
 *    Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "syntax_validator.h"
#include <folly/portability/GTest.h>
#include <array>
#include <string_view>

// This is a "copy" of the old unit tests we used to test for JSON_checker
// just to verify that if we decide to replace the validator we don't
// introduce a new regression we had a test for..

using namespace std::literals;

namespace cb::json {
// For some reason gtest won't use the one in the global namespace..
std::ostream& operator<<(std::ostream& os, const SyntaxValidator::Type& t) {
    os << ::to_string(t);
    return os;
}
} // namespace cb::json

class SyntaxValidatorTest
    : public ::testing::Test,
      public ::testing::WithParamInterface<cb::json::SyntaxValidator::Type> {
protected:
    void SetUp() override {
        validator = cb::json::SyntaxValidator::New(GetParam());
    }

    bool validate(std::string_view view) {
        return validator->validate(view);
    }

    std::unique_ptr<cb::json::SyntaxValidator> validator;
};

INSTANTIATE_TEST_SUITE_P(
        SyntaxValidator,
        SyntaxValidatorTest,
        ::testing::Values(cb::json::SyntaxValidator::Type::JSON_checker,
                          cb::json::SyntaxValidator::Type::Nlohmann));

TEST_P(SyntaxValidatorTest, SimpleJsonChecksOk) {
    EXPECT_TRUE(validate(R"({"test": 12})"sv));
}

TEST_P(SyntaxValidatorTest, TrailingDataAfterCompleteJsonObject) {
    EXPECT_FALSE(validate(R"({"test": true}})"sv))
            << "The parser did not detect the extra '}' after the object";
}

TEST_P(SyntaxValidatorTest, IncompleteJsonObject) {
    EXPECT_FALSE(validate(R"({"test": true)"sv))
            << "The parser did not detect the missing '}'";
}

TEST_P(SyntaxValidatorTest, DeepJsonChecksOk) {
    EXPECT_TRUE(validate(
            R"({"test": [[[[[[[[[[[[[[[[[[[[[[12]]]]]]]]]]]]]]]]]]]]]]})"sv));
}

TEST_P(SyntaxValidatorTest, BadDeepJsonIsNotOK) {
    EXPECT_FALSE(validate(
            R"({"test": [[[[[[[[[[[[[[[[[[[[[[12]]]]]]]]]]]]]]]]]]]]]]]]})"sv));
}

TEST_P(SyntaxValidatorTest, BadJsonStartingWithBraceIsNotOk) {
    EXPECT_FALSE(validate("{bad stuff}"sv));
}

TEST_P(SyntaxValidatorTest, BareValuesAreOk) {
    EXPECT_TRUE(validate("null"sv));
}

TEST_P(SyntaxValidatorTest, BareNumbersAreOk) {
    EXPECT_TRUE(validate("99"sv));
}

TEST_P(SyntaxValidatorTest, BadUtf8IsNotOk) {
    EXPECT_FALSE(validate("{\"test\xFF\": 12}"sv));
}

TEST_P(SyntaxValidatorTest, MB15778BadUtf8IsNotOk) {
    // MB-15778: Regression test for memory leaks.
    std::array<unsigned char, 3> mb15778 = {{'"', 0xff, 0}};
    EXPECT_FALSE(validate(
            {reinterpret_cast<const char*>(mb15778.data()), mb15778.size()}));
}

TEST_P(SyntaxValidatorTest, MB15778BadUtf8IsNotOk2) {
    // MB-15778: Regression test for memory leaks.
    std::array<unsigned char, 4> mb15778 = {{'"', 'a', 0xff, 0}};
    EXPECT_FALSE(validate(
            {reinterpret_cast<const char*>(mb15778.data()), mb15778.size()}));
}

TEST_P(SyntaxValidatorTest, MB15778BadUtf8IsNotOk3) {
    // MB-15778: Regression test for memory leaks.
    std::array<unsigned char, 5> mb15778 = {{'"', '1', '2', 0xfe, 0}};
    EXPECT_FALSE(validate(
            {reinterpret_cast<const char*>(mb15778.data()), mb15778.size()}));
}
TEST_P(SyntaxValidatorTest, MB15778BadUtf8IsNotOk4) {
    // MB-15778: Regression test for memory leaks.
    std::array<unsigned char, 5> mb15778 = {{'"', '1', '2', 0xfd, 0}};
    EXPECT_FALSE(validate(
            {reinterpret_cast<const char*>(mb15778.data()), mb15778.size()}));
}
TEST_P(SyntaxValidatorTest, MB15778BadUtf8IsNotOk5) {
    // MB-15778: Regression test for memory leaks.
    std::array<unsigned char, 8> mb15778 = {
            {'{', '"', 'k', '"', ':', '"', 0xfc, 0}};
    EXPECT_FALSE(validate(
            {reinterpret_cast<const char*>(mb15778.data()), mb15778.size()}));
}

TEST_P(SyntaxValidatorTest, SimpleValidatorTest) {
    auto value = R"({"test": 12})"sv;
    EXPECT_TRUE(validator->validate(value));
    value.remove_suffix(2);
    EXPECT_FALSE(validator->validate(value));
}

TEST_P(SyntaxValidatorTest, NumberExponentValidatorTest) {
    EXPECT_TRUE(validator->validate("0e5"sv));
    EXPECT_TRUE(validator->validate("0E5"sv));
    EXPECT_TRUE(validator->validate("0.00e5"sv));
}
