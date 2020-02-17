/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include <folly/portability/GTest.h>
#include <xattr/key_validator.h>
#include <cctype>
#include <locale>

#include "daemon/subdocument_validators.h"
/**
 * Ensure that we don't accept empty keys
 */
TEST(XattrKeyValidator, Empty) {
    EXPECT_FALSE(is_valid_xattr_key({nullptr, 0}));
    EXPECT_FALSE(is_valid_xattr_key({".", 1}));
}

/**
 * Ensure that we accept keys without a dot (the path is empty)
 */
TEST(XattrKeyValidator, FullXattr) {
    std::string key = "mydata";
    EXPECT_TRUE(is_valid_xattr_key({key.data(), key.size()}));
}

/**
 * Ensure that we enforce the max limit
 */
TEST(XattrKeyValidator, KeyLengthWithoutPath) {
    std::string key = "The Three Strikes and You're Out";
    for (auto ii = key.length(); ii > 0; --ii) {
        if (ii >= SUBDOC_MAX_XATTR_LENGTH) {
            EXPECT_FALSE(is_valid_xattr_key({key.data(), ii}));
        } else {
            EXPECT_TRUE(is_valid_xattr_key({key.data(), ii}));
        }
    }
}

/**
 * Ensure that we enforce the max limit with a path element..
 */
TEST(XattrKeyValidator, KeyLengthWithPath) {
    std::string key = "The Three Strikes and You're Out";
    for (auto ii = key.length(); ii > 1; --ii) {
        // Just make a copy and inject a dot ;)
        std::string copy = key;
        const_cast<char*>(copy.data())[ii - 1] = '.';
        if (ii > SUBDOC_MAX_XATTR_LENGTH) {
            EXPECT_FALSE(is_valid_xattr_key({copy.data(), copy.size()}))
                    << "[" << copy << "]";
        } else {
            EXPECT_TRUE(is_valid_xattr_key({copy.data(), copy.size()}))
                    << "[" << copy << "]";
        }
    }
}

/**
 * Ensure that we accept keys with a path
 */
TEST(XattrKeyValidator, PartialXattr) {
    std::string key = "mydata.foobar";
    EXPECT_TRUE(is_valid_xattr_key({key.data(), key.size()}));
}

TEST(XattrKeyValidator, FullWithArrayIndex) {
    std::string key = "mydata[0]";
    EXPECT_TRUE(is_valid_xattr_key({key.data(), key.size()}));
}


/**
 * X-Keys starting with a leading underscore ('_', 0x5F) are considered system
 * Such keys must be at least two characters
 */
TEST(XattrKeyValidator, SystemXattr) {
    std::string key = "_sync";
    EXPECT_TRUE(is_valid_xattr_key({key.data(), key.size()}));

    key = "_";
    EXPECT_FALSE(is_valid_xattr_key({key.data(), key.size()}));
}

/**
 * X-Keys starting with a leading dollar sign ('$', 0x24) are considered
 * virtual xattrs
 */
TEST(XattrKeyValidator, VirtualXattr) {
    std::string key = "$document";
    EXPECT_TRUE(is_valid_xattr_key({key.data(), key.size()}));

    key = "$XTOC";
    EXPECT_TRUE(is_valid_xattr_key({key.data(), key.size()}));

    key = "$";
    EXPECT_FALSE(is_valid_xattr_key({key.data(), key.size()}));
}

/**
 * There is no restrictions on the characters that may be inside the xattr
 * key. It'll take way too long time to test all of the possible values, so
 * lets just validate with all 7 bit ASCII characters.
 */
TEST(XattrKeyValidator, AllCharachtersInXattr) {
    std::vector<char> key(2);
    key[0] = 'a';

    // 0 is not allowed according to (should be using the two byte encoding)
    key[1] = 0;
    EXPECT_FALSE(is_valid_xattr_key({key.data(), key.size()}));

    for (int ii = 1; ii < 0x80; ++ii) {
        key[1] = char(ii);
        EXPECT_TRUE(is_valid_xattr_key({key.data(), key.size()})) << ii;
    }
}

/**
 * X-Keys starting with the following characters are reserved and
 * cannot be used:
 *    * ispunct(), excluding underscore (!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~)
 *    * iscntrl()
 */
TEST(XattrKeyValidator, RestrictedXattrPrefix) {
    std::locale loc("C");
    std::vector<char> key(2);
    key[1] = 'b';

    for (int ii = 0; ii < 0x80; ++ii) { // values over 0x80 == multibyte UTF8
        key[0] = char(ii);
        if ((std::ispunct(key[0], loc) && (key[0] != '_' && key[0] != '$')) ||
            std::iscntrl(key[0], loc)) {
            EXPECT_FALSE(is_valid_xattr_key({key.data(), key.size()}));
        } else {
            EXPECT_TRUE(is_valid_xattr_key({key.data(), key.size()}));
        }
    }
}

/**
 * XATTRS should be UTF8
 */

static void testInvalidUtf(char magic, int nbytes) {
    std::vector<char> data;
    data.push_back(magic);

    for (int ii = 0; ii < nbytes; ++ii) {
        EXPECT_FALSE(is_valid_xattr_key({data.data(), data.size()}));
        data.push_back(char(0xbf));
    }
    EXPECT_TRUE(is_valid_xattr_key({data.data(), data.size()}));

    for (int ii = 1; ii < nbytes + 1; ++ii) {
        data[ii] = char(0xff);
        EXPECT_FALSE(is_valid_xattr_key({data.data(), data.size()})) << ii;
        data[ii] = char(0xbf);
        EXPECT_TRUE(is_valid_xattr_key({data.data(), data.size()})) << ii;
    }
}

TEST(XattrKeyValidator, InvalidUTF8_2Bytes) {
    testInvalidUtf(char(0xDF), 1);
}

TEST(XattrKeyValidator, InvalidUTF8_3Bytes) {
    testInvalidUtf(char(0xEF), 2);
}

TEST(XattrKeyValidator, InvalidUTF8_4Bytes) {
    testInvalidUtf(char(0xF7), 3);
}
