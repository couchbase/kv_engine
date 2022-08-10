/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/GTest.h>
#include <memcached/config_parser.h>

using namespace cb::config;
using namespace std::string_view_literals;

TEST(ConfigNextField, allow_empty) {
    auto input = "  "sv;
    EXPECT_TRUE(internal::next_field(input, '=').empty());
    EXPECT_TRUE(input.empty());
}

TEST(ConfigNextField, trim_leading_space) {
    auto input = "  \ttrim"sv;
    EXPECT_EQ("trim", internal::next_field(input, '='));
    EXPECT_TRUE(input.empty());
}

TEST(ConfigNextField, dont_trim_escaped_leading_space) {
    auto input = R"( \  trim)"sv;
    EXPECT_EQ(R"(  trim)", internal::next_field(input, '='));
    EXPECT_TRUE(input.empty());
}

TEST(ConfigNextField, trim_trailing_space) {
    auto input = "trim  "sv;
    EXPECT_EQ("trim", internal::next_field(input, '='));
    EXPECT_TRUE(input.empty());
}

TEST(ConfigNextField, dont_trim_escaped_trailing_space) {
    auto input = R"(trim\  )"sv;
    EXPECT_EQ(R"(trim )", internal::next_field(input, '='));
    EXPECT_TRUE(input.empty());
}

TEST(ConfigNextField, stop_at_stopsign) {
    auto input = "  trim  = bar"sv;
    EXPECT_EQ("trim", internal::next_field(input, '='));
    EXPECT_EQ(" bar"sv, input);
}

TEST(ConfigNextField, dont_stop_at_escaped_stopsign) {
    auto input = R"(   trim  \= bar = myvalue)"sv;
    EXPECT_EQ(R"(trim  = bar)", internal::next_field(input, '='));
    EXPECT_EQ(" myvalue"sv, input);
}

TEST(ConfigTokenize, EmptyConfig) {
    tokenize({}, [](auto k, auto v) {
        FAIL() << "The callback should not be called for an empty string";
    });
}

TEST(ConfigTokenize, SingleKVPair) {
    bool callback = false;
    tokenize(R"(\ key\ = \ value\ )"sv, [&callback](auto k, auto v) {
        EXPECT_EQ(" key "sv, k);
        EXPECT_EQ(" value ", v);
        callback = true;
    });
    EXPECT_TRUE(callback) << "Callback not called";
}

TEST(ConfigTokenize, SingleKVPairIncludingEscapedSeparator) {
    bool callback = false;
    tokenize(R"(\ key\ = \ value\;next)"sv, [&callback](auto k, auto v) {
        EXPECT_EQ(" key "sv, k);
        EXPECT_EQ(" value;next", v);
        callback = true;
    });
    EXPECT_TRUE(callback) << "Callback not called";
}

TEST(ConfigTokenize, MultipleKVPairs) {
    bool k1 = false;
    bool k2 = false;
    tokenize(R"(k1=v1;k2=v2)"sv, [&k1, &k2](auto k, auto v) {
        if (k == "k1"sv) {
            EXPECT_EQ("v1", v);
            k1 = true;
        } else if (k == "k2"sv) {
            EXPECT_EQ("v2", v);
            k2 = true;
        } else {
            FAIL() << "Unexpected callback: k:" << k << " v:" << v;
        }
    });
    EXPECT_TRUE(k1) << "Callback for k1 not called";
    EXPECT_TRUE(k2) << "Callback for k2 not called";
}

TEST(ConfigValueAsSizeT, WithoutTrailingCharacters) {
    EXPECT_EQ(1234, value_as_size_t("1234"));
}

TEST(ConfigValueAsSizeT, WithTrailingCharacters) {
    bool detected = false;
    try {
        value_as_size_t("1234h");
    } catch (std::runtime_error& e) {
        EXPECT_STREQ(
                R"(cb::config::value_as_size_t: "1234h" contains extra characters)",
                e.what());
        detected = true;
    }
    EXPECT_TRUE(detected) << "Did not detect additional trailing characters";
}

TEST(ConfigValueAsSizeT, WithSizeSpecifiers) {
    EXPECT_EQ(1234ULL * 1024, value_as_size_t("1234k"));
    EXPECT_EQ(1234ULL * 1024 * 1024, value_as_size_t("1234m"));
    EXPECT_EQ(1234ULL * 1024 * 1024 * 1024, value_as_size_t("1234g"));
    EXPECT_EQ(1234ULL * 1024 * 1024 * 1024 * 1024, value_as_size_t("1234t"));
}

TEST(ConfigValueAsSizeT, WithSizeSpecifiersAndTrailingChars) {
    bool detected = false;
    try {
        value_as_size_t("1234kh");
    } catch (std::runtime_error& e) {
        EXPECT_STREQ(
                R"(cb::config::value_as_size_t: "1234kh" contains extra characters)",
                e.what());
        detected = true;
    }
    EXPECT_TRUE(detected) << "Did not detect additional trailing characters";
}

TEST(ConfigValueAsSizeT, WithMultipleSizeSpecifiers) {
    bool detected = false;
    try {
        value_as_size_t("1234kmg");
    } catch (std::runtime_error& e) {
        EXPECT_STREQ(
                R"(cb::config::value_as_size_t: "1234kmg" contains extra characters)",
                e.what());
        detected = true;
    }
    EXPECT_TRUE(detected) << "Did not detect additional trailing characters";
}

TEST(ConfigValueAsSSizeT, WithSizeSpecifiers) {
    EXPECT_EQ(-1234LL * 1024, value_as_ssize_t("-1234k"));
    EXPECT_EQ(-1234LL * 1024 * 1024, value_as_ssize_t("-1234m"));
    EXPECT_EQ(-1234LL * 1024 * 1024 * 1024, value_as_ssize_t("-1234g"));
    EXPECT_EQ(-1234LL * 1024 * 1024 * 1024 * 1024, value_as_ssize_t("-1234t"));
}

TEST(ConfigValueAsBool, True) {
    EXPECT_TRUE(value_as_bool("true"));
    EXPECT_TRUE(value_as_bool("TRUE"));
    EXPECT_TRUE(value_as_bool("TrUe"));
    EXPECT_TRUE(value_as_bool("on"));
    EXPECT_TRUE(value_as_bool("On"));
    EXPECT_TRUE(value_as_bool("oN"));
}

TEST(ConfigValueAsBool, False) {
    EXPECT_FALSE(value_as_bool("false"));
    EXPECT_FALSE(value_as_bool("FALSE"));
    EXPECT_FALSE(value_as_bool("False"));
    EXPECT_FALSE(value_as_bool("off"));
    EXPECT_FALSE(value_as_bool("OFF"));
    EXPECT_FALSE(value_as_bool("OfF"));
}

TEST(ConfigValueAsBool, Illegal) {
    bool detected = false;
    try {
        EXPECT_FALSE(value_as_bool("blah"));
    } catch (const std::runtime_error& e) {
        EXPECT_STREQ(
                R"(cb::config::value_as_bool: "blah" is not a boolean value)",
                e.what());
        detected = true;
    }
    EXPECT_TRUE(detected);
}

TEST(ConfigValueAsFloat, WithoutTrailingCharacters) {
    EXPECT_EQ(12.32f, value_as_float("12.32"));
}

TEST(ConfigValueAsFloat, WithTrailingCharacters) {
    bool detected = false;
    try {
        value_as_float("12.32h");
    } catch (std::runtime_error& e) {
        EXPECT_STREQ(
                R"(cb::config::value_as_float: "12.32h" contains extra characters)",
                e.what());
        detected = true;
    }
    EXPECT_TRUE(detected) << "Did not detect additional trailing characters";
}
