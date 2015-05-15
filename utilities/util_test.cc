/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

/*
 * Unit tests for util.c
 */

#include <platform/platform.h>

#include <memcached/util.h>
#include <memcached/config_parser.h>

#include <gtest/gtest.h>

#define TMP_TEMPLATE "testapp_tmp_file.XXXXXXX"

TEST(StringTest, safe_strtoul) {
    uint32_t val;
    EXPECT_TRUE(safe_strtoul("123", &val));
    EXPECT_EQ(123u, val);
    EXPECT_TRUE(safe_strtoul("+123", &val));
    EXPECT_EQ(123u, val);
    EXPECT_FALSE(safe_strtoul("", &val));  /* empty */
    EXPECT_FALSE(safe_strtoul("123BOGUS", &val));  /* non-numeric */
    /* Not sure what it does, but this works with ICC :/
       EXPECT_FALSE(safe_strtoul("92837498237498237498029383", &val)); // out of range
    */

    /* extremes: */
    EXPECT_TRUE(safe_strtoul("4294967295", &val)); /* 2**32 - 1 */
    EXPECT_EQ(4294967295L, val);
    /* This actually works on 64-bit ubuntu
       EXPECT_FALSE(safe_strtoul("4294967296", &val)); 2**32
    */
    EXPECT_FALSE(safe_strtoul("-1", &val));  /* negative */
}


TEST(StringTest, safe_strtoull) {
    uint64_t val;
    uint64_t exp = -1;
    EXPECT_TRUE(safe_strtoull("123", &val));
    EXPECT_EQ(123u, val);
    EXPECT_TRUE(safe_strtoull("+123", &val));
    EXPECT_EQ(123u, val);
    EXPECT_FALSE(safe_strtoull("", &val));  /* empty */
    EXPECT_FALSE(safe_strtoull("123BOGUS", &val));  /* non-numeric */
    EXPECT_FALSE(safe_strtoull("92837498237498237498029383", &val)); /* out of range */

    /* extremes: */
    EXPECT_TRUE(safe_strtoull("18446744073709551615", &val)); /* 2**64 - 1 */
    EXPECT_EQ(exp, val);
    EXPECT_FALSE(safe_strtoull("18446744073709551616", &val)); /* 2**64 */
    EXPECT_FALSE(safe_strtoull("-1", &val));  /* negative */
}

TEST(StringTest, safe_strtoll) {
    int64_t val;
    int64_t exp = 1;
    exp <<= 63;
    exp -= 1;
    EXPECT_TRUE(safe_strtoll("123", &val));
    EXPECT_EQ(123, val);
    EXPECT_TRUE(safe_strtoll("+123", &val));
    EXPECT_EQ(123, val);
    EXPECT_TRUE(safe_strtoll("-123", &val));
    EXPECT_EQ(-123, val);
    EXPECT_FALSE(safe_strtoll("", &val));  /* empty */
    EXPECT_FALSE(safe_strtoll("123BOGUS", &val));  /* non-numeric */
    EXPECT_FALSE(safe_strtoll("92837498237498237498029383", &val)); /* out of range */

    /* extremes: */
    EXPECT_FALSE(safe_strtoll("18446744073709551615", &val)); /* 2**64 - 1 */
    EXPECT_TRUE(safe_strtoll("9223372036854775807", &val)); /* 2**63 - 1 */

    EXPECT_EQ(exp, val); /* 9223372036854775807LL); */
    /*
      EXPECT_EQ(safe_strtoll("-9223372036854775808", &val)); // -2**63
      EXPECT_EQ(val, -9223372036854775808LL);
    */
    EXPECT_FALSE(safe_strtoll("-9223372036854775809", &val)); /* -2**63 - 1 */

    /* We'll allow space to terminate the string.  And leading space. */
    EXPECT_TRUE(safe_strtoll(" 123 foo", &val));
    EXPECT_EQ(123, val);
}

TEST(StringTest, safe_strtol) {
    int32_t val;
    EXPECT_TRUE(safe_strtol("123", &val));
    EXPECT_EQ(123, val);
    EXPECT_TRUE(safe_strtol("+123", &val));
    EXPECT_EQ(123, val);
    EXPECT_TRUE(safe_strtol("-123", &val));
    EXPECT_EQ(-123, val);
    EXPECT_FALSE(safe_strtol("", &val));  /* empty */
    EXPECT_FALSE(safe_strtol("123BOGUS", &val));  /* non-numeric */
    EXPECT_FALSE(safe_strtol("92837498237498237498029383", &val)); /* out of range */

    /* extremes: */
    /* This actually works on 64-bit ubuntu
       EXPECT_FALSE(safe_strtol("2147483648", &val)); // (expt 2.0 31.0)
    */
    EXPECT_TRUE(safe_strtol("2147483647", &val)); /* (- (expt 2.0 31) 1) */
    EXPECT_EQ(2147483647L, val);
    /* This actually works on 64-bit ubuntu
       EXPECT_FALSE(safe_strtol("-2147483649", &val)); // (- (expt -2.0 31) 1)
    */

    /* We'll allow space to terminate the string.  And leading space. */
    EXPECT_TRUE(safe_strtol(" 123 foo", &val));
    EXPECT_EQ(123, val);
}

TEST(StringTest, safe_strtof) {
    float val;
    EXPECT_TRUE(safe_strtof("123", &val));
    EXPECT_EQ(123.00f, val);
    EXPECT_TRUE(safe_strtof("+123", &val));
    EXPECT_EQ(123.00f, val);
    EXPECT_TRUE(safe_strtof("-123", &val));
    EXPECT_EQ(-123.00f, val);
    EXPECT_FALSE(safe_strtof("", &val));  /* empty */
    EXPECT_FALSE(safe_strtof("123BOGUS", &val));  /* non-numeric */

    /* We'll allow space to terminate the string.  And leading space. */
    EXPECT_TRUE(safe_strtof(" 123 foo", &val));
    EXPECT_EQ(123.00f, val);

    EXPECT_TRUE(safe_strtof("123.23", &val));
    EXPECT_EQ(123.23f, val);

    EXPECT_TRUE(safe_strtof("123.00", &val));
    EXPECT_EQ(123.00f, val);
}

#ifndef WIN32
TEST(VperrorTest, A) {
    int rv = 0;
    int oldstderr = dup(STDERR_FILENO);
    char tmpl[sizeof(TMP_TEMPLATE)+1];
    int newfile;
    char buf[80] = {0};
    FILE *efile;
    char *prv;
    char expected[80] = {0};

    strncpy(tmpl, TMP_TEMPLATE, sizeof(TMP_TEMPLATE)+1);

    newfile = mkstemp(tmpl);
    ASSERT_GT(newfile, 0);
    rv = dup2(newfile, STDERR_FILENO);
    ASSERT_EQ(STDERR_FILENO, rv);
    rv = close(newfile);
    ASSERT_EQ(0, rv);

    errno = EIO;
    vperror("Old McDonald had a farm.  %s", "EI EIO");

    /* Restore stderr */
    rv = dup2(oldstderr, STDERR_FILENO);
    ASSERT_EQ(STDERR_FILENO, rv);


    /* Go read the file */
    efile = fopen(tmpl, "r");
    ASSERT_TRUE(efile);
    prv = fgets(buf, sizeof(buf), efile);
    EXPECT_TRUE(prv);
    fclose(efile);

    unlink(tmpl);

    snprintf(expected, sizeof(expected),
             "Old McDonald had a farm.  EI EIO: %s\n", strerror(EIO));

    /*
    fprintf(stderr,
            "\nExpected:  ``%s''"
            "\nGot:       ``%s''\n", expected, buf);
    */

    EXPECT_STREQ(expected, buf);
}
#endif // !WIN32

static char* trim(char* ptr) {
    char *start = ptr;
    char *end;

    while (isspace(*start)) {
        ++start;
    }
    end = start + strlen(start) - 1;
    if (end != start) {
        while (isspace(*end)) {
            *end = '\0';
            --end;
        }
    }
    return start;
}

TEST(ConfigParserTest, A) {
    bool bool_val = false;
    size_t size_val = 0;
    ssize_t ssize_val = 0;
    float float_val = 0;
    char *string_val = 0;
    int ii;
    char buffer[1024];
    FILE *cfg;
    char outfile[sizeof(TMP_TEMPLATE)+1];
    char cfgfile[sizeof(TMP_TEMPLATE)+1];
    FILE *error;

    /* Set up the different items I can handle */
    struct config_item items[7];
    memset(&items, 0, sizeof(items));
    ii = 0;
    items[ii].key = "bool";
    items[ii].datatype = DT_BOOL;
    items[ii].value.dt_bool = &bool_val;
    ++ii;

    items[ii].key = "size_t";
    items[ii].datatype = DT_SIZE;
    items[ii].value.dt_size = &size_val;
    ++ii;

    items[ii].key = "ssize_t";
    items[ii].datatype = DT_SSIZE;
    items[ii].value.dt_ssize = &ssize_val;
    ++ii;

    items[ii].key = "float";
    items[ii].datatype = DT_FLOAT;
    items[ii].value.dt_float = &float_val;
    ++ii;

    items[ii].key = "string";
    items[ii].datatype = DT_STRING;
    items[ii].value.dt_string = &string_val;
    ++ii;

    items[ii].key = "config_file";
    items[ii].datatype = DT_CONFIGFILE;
    ++ii;

    items[ii].key = NULL;
    ++ii;

    ASSERT_EQ(7, ii);
    strncpy(outfile, TMP_TEMPLATE, sizeof(TMP_TEMPLATE)+1);
    strncpy(cfgfile, TMP_TEMPLATE, sizeof(TMP_TEMPLATE)+1);

    ASSERT_NE(cb_mktemp(outfile), nullptr);
    error = fopen(outfile, "w");

    ASSERT_NE(error, nullptr);
    ASSERT_EQ(0, parse_config("", items, error));
    /* Nothing should be found */
    for (ii = 0; ii < 5; ++ii) {
        EXPECT_FALSE(items[0].found);
    }

    ASSERT_EQ(0, parse_config("bool=true", items, error));
    EXPECT_TRUE(bool_val);
    /* only bool should be found */
    EXPECT_TRUE(items[0].found);
    items[0].found = false;
    for (ii = 0; ii < 5; ++ii) {
        EXPECT_FALSE(items[0].found);
    }

    /* It should allow illegal keywords */
    ASSERT_EQ(1, parse_config("pacman=dead", items, error));
    /* and illegal values */
    ASSERT_EQ(-1, parse_config("bool=12", items, error));
    EXPECT_FALSE(items[0].found);
    /* and multiple occurences of the same value */
    ASSERT_EQ(0, parse_config("size_t=1; size_t=1024", items, error));
    EXPECT_TRUE(items[1].found);
    EXPECT_EQ(1024u, size_val);
    items[1].found = false;

    /* Empty string */
    /* XXX:  This test fails on Linux, but works on OS X.
    cb_assert(parse_config("string=", items, error) == 0);
    cb_assert(items[4].found);
    cb_assert(strcmp(string_val, "") == 0);
    items[4].found = false;
    */
    /* Plain string */
    ASSERT_EQ(0, parse_config("string=sval", items, error));
    EXPECT_TRUE(items[4].found);
    EXPECT_STREQ("sval", string_val);
    items[4].found = false;
    free(string_val);
    /* Leading space */
    ASSERT_EQ(0, parse_config("string= sval", items, error));
    EXPECT_TRUE(items[4].found);
    EXPECT_STREQ("sval", string_val);
    items[4].found = false;
    free(string_val);
    /* Escaped leading space */
    ASSERT_EQ(0, parse_config("string=\\ sval", items, error));
    EXPECT_TRUE(items[4].found);
    EXPECT_STREQ(" sval", string_val);
    items[4].found = false;
    free(string_val);
    /* trailing space */
    ASSERT_EQ(0, parse_config("string=sval ", items, error));
    EXPECT_TRUE(items[4].found);
    EXPECT_STREQ("sval", string_val);
    items[4].found = false;
    free(string_val);
    /* escaped trailing space */
    ASSERT_EQ(0, parse_config("string=sval\\ ", items, error));
    EXPECT_TRUE(items[4].found);
    EXPECT_STREQ("sval ", string_val);
    items[4].found = false;
    free(string_val);
    /* escaped stop char */
    ASSERT_EQ(0, parse_config("string=sval\\;blah=x", items, error));
    EXPECT_TRUE(items[4].found);
    EXPECT_STREQ("sval;blah=x", string_val);
    items[4].found = false;
    free(string_val);
    /* middle space */
    ASSERT_EQ(0, parse_config("string=s val", items, error));
    EXPECT_TRUE(items[4].found);
    EXPECT_STREQ("s val", string_val);
    items[4].found = false;
    free(string_val);

    /* And all of the variables */
    ASSERT_EQ(0, parse_config("bool=true;size_t=1024;float=12.5;string=somestr",
                              items, error));
    EXPECT_TRUE(bool_val);
    EXPECT_EQ(1024u, size_val);
    EXPECT_EQ(12.5f, float_val);
    EXPECT_STREQ("somestr", string_val);
    free(string_val);
    for (ii = 0; ii < 5; ++ii) {
        items[ii].found = false;
    }

    ASSERT_EQ(0, parse_config("size_t=1k", items, error));
    EXPECT_TRUE(items[1].found);
    EXPECT_EQ(1024u, size_val);
    items[1].found = false;
    ASSERT_EQ(0, parse_config("size_t=1m", items, error));
    EXPECT_TRUE(items[1].found);
    EXPECT_EQ(1024u * 1024u, size_val);
    items[1].found = false;
    ASSERT_EQ(0, parse_config("size_t=1g", items, error));
    EXPECT_TRUE(items[1].found);
    EXPECT_EQ(1024u * 1024u * 1024u ,size_val);
    items[1].found = false;
    ASSERT_EQ(0, parse_config("size_t=1K", items, error));
    EXPECT_TRUE(items[1].found);
    EXPECT_EQ(1024u, size_val);
    items[1].found = false;
    ASSERT_EQ(0, parse_config("size_t=1M", items, error));
    EXPECT_TRUE(items[1].found);
    EXPECT_EQ(1024u * 1024u, size_val);
    items[1].found = false;
    ASSERT_EQ(0, parse_config("size_t=1G", items, error));
    EXPECT_TRUE(items[1].found);
    EXPECT_EQ(1024u * 1024u * 1024u, size_val);
    items[1].found = false;

    ASSERT_NE(cb_mktemp(cfgfile), nullptr);
    cfg = fopen(cfgfile, "w");
    ASSERT_NE(cfg, nullptr);
    fprintf(cfg, "# This is a config file\nbool=true\nsize_t=1023\nfloat=12.4\n");
    fclose(cfg);
    sprintf(buffer, "config_file=%s", cfgfile);
    ASSERT_EQ(0, parse_config(buffer, items, error));
    EXPECT_TRUE(bool_val);
    EXPECT_EQ(1023u, size_val);
    EXPECT_EQ(12.4f, float_val);
    fclose(error);

    remove(cfgfile);
    /* Verify that I received the error messages ;-) */
    error = fopen(outfile, "r");
    ASSERT_TRUE(error);

    EXPECT_TRUE(fgets(buffer, sizeof(buffer), error));
    EXPECT_STREQ("Unsupported key: <pacman>", trim(buffer));
    EXPECT_TRUE(fgets(buffer, sizeof(buffer), error));
    EXPECT_STREQ("Invalid entry, Key: <bool> Value: <12>", trim(buffer));
    EXPECT_TRUE(fgets(buffer, sizeof(buffer), error));
    EXPECT_STREQ("WARNING: Found duplicate entry for \"size_t\"", trim(buffer));
    EXPECT_EQ(nullptr, fgets(buffer, sizeof(buffer), error));

    EXPECT_EQ(0, fclose(error));
    remove(outfile);
}
