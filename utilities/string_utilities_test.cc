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

// =====================================================================
// split_string
// =====================================================================

TEST(SplitString, EmptyString) {
    // An empty string still produces one (empty) element.
    auto result = split_string("", ";");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ("", result[0]);
}

TEST(SplitString, NoDelimiter) {
    auto result = split_string("hello", ";");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ("hello", result[0]);
}

TEST(SplitString, SingleDelimiter) {
    auto result = split_string("a;b", ";");
    ASSERT_EQ(2, result.size());
    EXPECT_EQ("a", result[0]);
    EXPECT_EQ("b", result[1]);
}

TEST(SplitString, MultipleDelimiters) {
    auto result = split_string("a;b;c", ";");
    ASSERT_EQ(3, result.size());
    EXPECT_EQ("a", result[0]);
    EXPECT_EQ("b", result[1]);
    EXPECT_EQ("c", result[2]);
}

TEST(SplitString, DelimiterAtStart) {
    auto result = split_string(";rest", ";");
    ASSERT_EQ(2, result.size());
    EXPECT_EQ("", result[0]);
    EXPECT_EQ("rest", result[1]);
}

TEST(SplitString, DelimiterAtEnd) {
    auto result = split_string("rest;", ";");
    ASSERT_EQ(2, result.size());
    EXPECT_EQ("rest", result[0]);
    EXPECT_EQ("", result[1]);
}

TEST(SplitString, ConsecutiveDelimiters) {
    auto result = split_string("a;;b", ";");
    ASSERT_EQ(3, result.size());
    EXPECT_EQ("a", result[0]);
    EXPECT_EQ("", result[1]);
    EXPECT_EQ("b", result[2]);
}

TEST(SplitString, MultiCharDelimiter) {
    auto result = split_string("a::b::c", "::");
    ASSERT_EQ(3, result.size());
    EXPECT_EQ("a", result[0]);
    EXPECT_EQ("b", result[1]);
    EXPECT_EQ("c", result[2]);
}

TEST(SplitString, LimitOne) {
    // limit=1 means at most one split → two pieces; the rest stays unsplit.
    auto result = split_string("a;b;c", ";", 1);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ("a", result[0]);
    EXPECT_EQ("b;c", result[1]);
}

TEST(SplitString, LimitTwo) {
    auto result = split_string("a;b;c;d", ";", 2);
    ASSERT_EQ(3, result.size());
    EXPECT_EQ("a", result[0]);
    EXPECT_EQ("b", result[1]);
    EXPECT_EQ("c;d", result[2]);
}

TEST(SplitString, LimitExceedsActualSplits) {
    // Limit larger than the number of delimiters → same as no limit.
    auto result = split_string("a;b", ";", 10);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ("a", result[0]);
    EXPECT_EQ("b", result[1]);
}

// =====================================================================
// percent_decode
// =====================================================================

TEST(PercentDecode, EmptyString) {
    EXPECT_EQ("", percent_decode(""));
}

TEST(PercentDecode, NoEncoding) {
    EXPECT_EQ("hello world", percent_decode("hello world"));
}

TEST(PercentDecode, SpaceEncoded) {
    EXPECT_EQ("hello world", percent_decode("hello%20world"));
}

TEST(PercentDecode, UppercaseHex) {
    EXPECT_EQ("/", percent_decode("%2F"));
}

TEST(PercentDecode, LowercaseHex) {
    EXPECT_EQ("/", percent_decode("%2f"));
}

TEST(PercentDecode, MultipleEncodedChars) {
    // %25 → '%', %26 → '&'  (matches the decode_query docstring example)
    EXPECT_EQ("%&", percent_decode("%25%26"));
}

TEST(PercentDecode, TruncatedAtEnd) {
    EXPECT_THROW(percent_decode("hello%"), std::invalid_argument);
}

TEST(PercentDecode, TruncatedOneCharAfterPercent) {
    EXPECT_THROW(percent_decode("hello%2"), std::invalid_argument);
}

TEST(PercentDecode, InvalidHexChars) {
    EXPECT_THROW(percent_decode("%zz"), std::invalid_argument);
}

// =====================================================================
// decode_query
// =====================================================================

TEST(DecodeQuery, NoQueryString) {
    auto [path, params] = decode_query("mykey");
    EXPECT_EQ("mykey", path);
    EXPECT_TRUE(params.empty());
}

TEST(DecodeQuery, QuestionMarkNoQuery) {
    // "path?" — the part after '?' is empty, so params are not parsed.
    auto [path, params] = decode_query("path?");
    EXPECT_EQ("path", path);
    EXPECT_TRUE(params.empty());
}

TEST(DecodeQuery, SingleParam) {
    auto [path, params] = decode_query("key?arg=val");
    EXPECT_EQ("key", path);
    ASSERT_EQ(1, params.size());
    EXPECT_EQ("val", params.at("arg"));
}

TEST(DecodeQuery, MultipleParams) {
    auto [path, params] = decode_query("key?arg=val&arg2=val2");
    EXPECT_EQ("key", path);
    ASSERT_EQ(2, params.size());
    EXPECT_EQ("val", params.at("arg"));
    EXPECT_EQ("val2", params.at("arg2"));
}

TEST(DecodeQuery, PercentEncodedKeyAndValue) {
    // From the header doc: 'key?%25=%26' becomes ["key", {"%": "&"}]
    auto [path, params] = decode_query("key?%25=%26");
    EXPECT_EQ("key", path);
    ASSERT_EQ(1, params.size());
    EXPECT_EQ("&", params.at("%"));
}

TEST(DecodeQuery, ParamWithoutEquals) {
    EXPECT_THROW(decode_query("key?noequals"), std::invalid_argument);
}

TEST(DecodeQuery, EmptyParamName) {
    EXPECT_THROW(decode_query("key?=value"), std::invalid_argument);
}

// =====================================================================
// get_thread_pool_name
// =====================================================================

TEST(GetThreadPoolName, NoDigits) {
    // A name with no digits at all has no recognisable thread number
    // → the returned pool name is empty.
    EXPECT_EQ("", get_thread_pool_name("NoDigitsHere"));
}

TEST(GetThreadPoolName, StartsWithDigit) {
    // prefix_end == 0 → empty pool name
    EXPECT_EQ("", get_thread_pool_name("0worker"));
}

TEST(GetThreadPoolName, NoSeparator) {
    EXPECT_EQ("AuxIO", get_thread_pool_name("AuxIO0"));
    EXPECT_EQ("AuxIO", get_thread_pool_name("AuxIO3"));
}

TEST(GetThreadPoolName, ColonSeparator) {
    EXPECT_EQ("Thread", get_thread_pool_name("Thread:0"));
    EXPECT_EQ("NonioWorker", get_thread_pool_name("NonioWorker:3"));
}

TEST(GetThreadPoolName, UnderscoreSeparator) {
    EXPECT_EQ("Writer", get_thread_pool_name("Writer_0"));
    EXPECT_EQ("BgFetcher", get_thread_pool_name("BgFetcher_2"));
}

TEST(GetThreadPoolName, SeparatorInMiddleNotStripped) {
    // The separator is only stripped when it is the *last* character
    // before the digit; an embedded ':' or '_' in the pool name is kept.
    EXPECT_EQ("NonIO:pool", get_thread_pool_name("NonIO:pool:0"));
    EXPECT_EQ("ep_io", get_thread_pool_name("ep_io_0"));
}

TEST(GetThreadPoolName, FullyNumeric) {
    EXPECT_EQ("", get_thread_pool_name("0"));
}

// =====================================================================
// base64 round-trip
// =====================================================================

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

TEST(Base64, RoundTrip) {
    const std::string original = "Hello, World! \x01\x02\x03";
    EXPECT_EQ(original, base64_decode_value(base64_encode_value(original)));
}

TEST(Base64, RoundTripEmpty) {
    EXPECT_EQ("", base64_decode_value(base64_encode_value("")));
}
