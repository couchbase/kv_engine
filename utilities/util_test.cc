/*
 * Portions Copyright (c) 2010-Present Couchbase
 * Portions Copyright (c) 2009 Sun Microsystems
 *
 * Use of this software is governed by the Apache License, Version 2.0 and
 * BSD 3 Clause included in the files licenses/APL2.txt and
 * licenses/BSD-3-Clause-Sun-Microsystems.txt
 */

/*
 * Unit tests for util.c
 */

#include <platform/cb_malloc.h>
#include <platform/dirutils.h>

#include <memcached/util.h>
#include <memcached/config_parser.h>
#include "string_utilities.h"

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>

TEST(StringTest, safe_strtoul) {
    uint32_t val;
    EXPECT_TRUE(safe_strtoul("123", val));
    EXPECT_EQ(123u, val);
    EXPECT_TRUE(safe_strtoul("+123", val));
    EXPECT_EQ(123u, val);
    EXPECT_FALSE(safe_strtoul("", val));  /* empty */
    EXPECT_FALSE(safe_strtoul("123BOGUS", val));  /* non-numeric */
    /* Not sure what it does, but this works with ICC :/
       EXPECT_FALSE(safe_strtoul("92837498237498237498029383", val)); // out of range
    */

    /* extremes: */
    EXPECT_TRUE(safe_strtoul("4294967295", val)); /* 2**32 - 1 */
    EXPECT_EQ(4294967295L, val);
    /* This actually works on 64-bit ubuntu
       EXPECT_FALSE(safe_strtoul("4294967296", val)); 2**32
    */
    EXPECT_FALSE(safe_strtoul("-1", val));  /* negative */
}


TEST(StringTest, safe_strtoull) {
    uint64_t val;
    uint64_t exp = -1;
    EXPECT_TRUE(safe_strtoull("123", val));
    EXPECT_EQ(123u, val);
    EXPECT_TRUE(safe_strtoull("+123", val));
    EXPECT_EQ(123u, val);
    EXPECT_FALSE(safe_strtoull("", val));  /* empty */
    EXPECT_FALSE(safe_strtoull("123BOGUS", val));  /* non-numeric */
    EXPECT_FALSE(safe_strtoull("92837498237498237498029383", val)); /* out of range */

    /* extremes: */
    EXPECT_TRUE(safe_strtoull("18446744073709551615", val)); /* 2**64 - 1 */
    EXPECT_EQ(exp, val);
    EXPECT_FALSE(safe_strtoull("18446744073709551616", val)); /* 2**64 */
    EXPECT_FALSE(safe_strtoull("-1", val));  /* negative */
}

TEST(StringTest, safe_strtoll) {
    int64_t val;
    EXPECT_TRUE(safe_strtoll("123", val));
    EXPECT_EQ(123, val);
    EXPECT_TRUE(safe_strtoll("+123", val));
    EXPECT_EQ(123, val);
    EXPECT_TRUE(safe_strtoll("-123", val));
    EXPECT_EQ(-123, val);
    EXPECT_FALSE(safe_strtoll("", val));  /* empty */
    EXPECT_FALSE(safe_strtoll("123BOGUS", val));  /* non-numeric */
    EXPECT_FALSE(safe_strtoll("92837498237498237498029383", val)); /* out of range */

    /* extremes: */
    EXPECT_FALSE(safe_strtoll("18446744073709551615", val)); /* 2**64 - 1 */
    EXPECT_TRUE(safe_strtoll("9223372036854775807", val)); /* 2**63 - 1 */

    EXPECT_EQ(std::numeric_limits<int64_t>::max(),
              val); /* 9223372036854775807LL); */
    /*
      EXPECT_EQ(safe_strtoll("-9223372036854775808", val)); // -2**63
      EXPECT_EQ(val, -9223372036854775808LL);
    */
    EXPECT_FALSE(safe_strtoll("-9223372036854775809", val)); /* -2**63 - 1 */

    /* We'll allow space to terminate the string.  And leading space. */
    EXPECT_TRUE(safe_strtoll(" 123 foo", val));
    EXPECT_EQ(123, val);
}

TEST(StringTest, safe_strtol) {
    int32_t val;
    EXPECT_TRUE(safe_strtol("123", val));
    EXPECT_EQ(123, val);
    EXPECT_TRUE(safe_strtol("+123", val));
    EXPECT_EQ(123, val);
    EXPECT_TRUE(safe_strtol("-123", val));
    EXPECT_EQ(-123, val);
    EXPECT_FALSE(safe_strtol("", val));  /* empty */
    EXPECT_FALSE(safe_strtol("123BOGUS", val));  /* non-numeric */
    EXPECT_FALSE(safe_strtol("92837498237498237498029383", val)); /* out of range */

    /* extremes: */
    /* This actually works on 64-bit ubuntu
       EXPECT_FALSE(safe_strtol("2147483648", val)); // (expt 2.0 31.0)
    */
    EXPECT_TRUE(safe_strtol("2147483647", val)); /* (- (expt 2.0 31) 1) */
    EXPECT_EQ(2147483647L, val);
    /* This actually works on 64-bit ubuntu
       EXPECT_FALSE(safe_strtol("-2147483649", val)); // (- (expt -2.0 31) 1)
    */

    /* We'll allow space to terminate the string.  And leading space. */
    EXPECT_TRUE(safe_strtol(" 123 foo", val));
    EXPECT_EQ(123, val);
}

TEST(StringTest, safe_strtof) {
    float val;
    EXPECT_TRUE(safe_strtof("123", val));
    EXPECT_EQ(123.00f, val);
    EXPECT_TRUE(safe_strtof("+123", val));
    EXPECT_EQ(123.00f, val);
    EXPECT_TRUE(safe_strtof("-123", val));
    EXPECT_EQ(-123.00f, val);
    EXPECT_FALSE(safe_strtof("", val));  /* empty */
    EXPECT_FALSE(safe_strtof("123BOGUS", val));  /* non-numeric */

    /* We'll allow space to terminate the string.  And leading space. */
    EXPECT_TRUE(safe_strtof(" 123 foo", val));
    EXPECT_EQ(123.00f, val);

    EXPECT_TRUE(safe_strtof("123.23", val));
    EXPECT_EQ(123.23f, val);

    EXPECT_TRUE(safe_strtof("123.00", val));
    EXPECT_EQ(123.00f, val);
}

TEST(StringTest, split_string) {
    using namespace testing;

    EXPECT_THAT(split_string("123:456", ":"), ElementsAre("123", "456"));
    EXPECT_THAT(split_string("123::456", ":"), ElementsAre("123", "", "456"));
    EXPECT_THAT(split_string("123:456:", ":"), ElementsAre("123", "456", ""));
    EXPECT_THAT(split_string("123:456:789", ":", 1),
                ElementsAre("123", "456:789"));
    EXPECT_THAT(split_string("123:456:789", ":", 2),
                ElementsAre("123", "456", "789"));
    EXPECT_THAT(split_string("123::456", ":", 1),
                ElementsAre("123", ":456"));
    EXPECT_THAT(split_string(":", ":", 2),
                ElementsAre("", ""));
    EXPECT_THAT(split_string(":abcd", ":", 200),
                ElementsAre("", "abcd"));
    EXPECT_THAT(split_string("Hello, World!", ", ", 200),
                ElementsAre("Hello", "World!"));
    EXPECT_THAT(split_string("Hello<BOOM>World<BOOM>!", "<BOOM>", 200),
                ElementsAre("Hello", "World", "!"));
    EXPECT_THAT(split_string("Hello<BOOM>World<BOOM>!", "<BOOM>", 1),
                ElementsAre("Hello", "World<BOOM>!"));
}

TEST(StringTest, percent_decode) {
    // Test every character from 0x00->0xFF that they can be converted to
    // percent encoded strings and back again
    for (int i = 0; i < 255; ++i) {
        std::stringstream s, t;
        s << "%" << std::setfill('0') << std::setw(2) << std::hex << i;
        t << static_cast<char>(i);
        EXPECT_EQ(t.str(), percent_decode(s.str()));
    }

    EXPECT_EQ("abcdef!abcdef", percent_decode("abcdef%21abcdef"));
    EXPECT_EQ("!", percent_decode("%21"));
    EXPECT_EQ("!!", percent_decode("%21%21"));
    EXPECT_EQ("%21", percent_decode("%25%32%31"));

    EXPECT_THROW(percent_decode("%"), std::invalid_argument);
    EXPECT_THROW(percent_decode("%%"), std::invalid_argument);
    EXPECT_THROW(percent_decode("%3"), std::invalid_argument);
    EXPECT_THROW(percent_decode("%%%"), std::invalid_argument);
    EXPECT_THROW(percent_decode("%GG"), std::invalid_argument);
}

TEST(StringTest, decode_query) {
    using namespace testing;
    std::pair<std::string, StrToStrMap> request;

    request = decode_query("key?arg=val&arg2=val2&arg3=val?=");
    EXPECT_EQ("key", request.first);
    EXPECT_THAT(request.second, UnorderedElementsAre(Pair("arg", "val"),
                                                     Pair("arg2", "val2"),
                                                     Pair("arg3", "val?=")));

    request = decode_query("key");
    EXPECT_EQ("key", request.first);
    EXPECT_THAT(request.second, UnorderedElementsAre());

    request = decode_query("key?");
    EXPECT_EQ("key", request.first);
    EXPECT_THAT(request.second, UnorderedElementsAre());

    request = decode_query("key\?\?=?");
    EXPECT_EQ("key", request.first);
    EXPECT_THAT(request.second, UnorderedElementsAre(Pair("?", "?")));

    request = decode_query("key?%25=%26&%26=%25");
    EXPECT_EQ("key", request.first);
    EXPECT_THAT(request.second, UnorderedElementsAre(Pair("%", "&"),
                                                     Pair("&", "%")));

    EXPECT_THROW(decode_query("key?=&a=b"), std::invalid_argument);
    EXPECT_THROW(decode_query("key?a&a=b"), std::invalid_argument);
}
