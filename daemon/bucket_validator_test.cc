/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "buckets.h"
#include "enginemap.h"

#include <folly/portability/GTest.h>
#include <platform/dirutils.h>
#include <cctype>
#include <stdexcept>

using BucketValidator::validateBucketName;

TEST(BucketNameValidatorTest, LengthRestrictions) {
    ASSERT_EQ("Name can't be empty", validateBucketName(""));
    std::string name;
    name.resize(MAX_BUCKET_NAME_LENGTH);
    std::fill(name.begin(), name.end(), 'a');
    ASSERT_EQ("", validateBucketName(name));
    // adding one more character should make it too long
    name.push_back('b');
    static_assert(MAX_BUCKET_NAME_LENGTH == 100, "Text needs to be updated");
    ASSERT_EQ("Name too long (exceeds 100)", validateBucketName(name));
}

TEST(BucketNameValidatorTest, TestInvalidCharacters) {
    std::string name("a ");

    for (int ii = 1; ii < 256; ++ii) {
        name.at(1) = char(ii);
        bool legal = true;

        // According to DOC-107:
        // "The bucket name can only contain characters in range A-Z, a-z, 0-9
        // as well as underscore, period, dash and percent symbols"
        if (!(isupper(ii) || islower(ii) || isdigit(ii))) {
            switch (ii) {
            case '_':
            case '-':
            case '.':
            case '%':
                break;
            default:
                legal = false;
            }
        }

        if (legal) {
            ASSERT_EQ("", validateBucketName(name));
        } else {
            ASSERT_EQ("Name contains invalid characters",
                      validateBucketName(name));
        }
    }
}

TEST(BucketTypeValidatorTest, Nobucket) {
    EXPECT_EQ(BucketType::NoBucket,
              module_to_bucket_type(
                      cb::io::sanitizePath("/foo/bar/nobucket.so")));
}

TEST(BucketTypeValidatorTest, Memcached) {
    EXPECT_EQ(BucketType::Memcached,
              module_to_bucket_type(
                      cb::io::sanitizePath("/foo/bar/default_engine.so")));
}

TEST(BucketTypeValidatorTest, Couchstore) {
    EXPECT_EQ(BucketType::Couchbase,
              module_to_bucket_type(cb::io::sanitizePath("/foo/bar/ep.so")));
}

TEST(BucketTypeValidatorTest, EWB) {
    EXPECT_EQ(BucketType::EWouldBlock,
              module_to_bucket_type(
                      cb::io::sanitizePath("/boo/bar/ewouldblock_engine.so")));
}

// we can't really verify all other possible module names, but just check that
// we get Unknown for some
TEST(BucketTypeValidatorTest, Unknown) {
    EXPECT_EQ(BucketType::Unknown,
              module_to_bucket_type(cb::io::sanitizePath("/foo/bar/foo.so")));
    EXPECT_EQ(BucketType::Unknown,
              module_to_bucket_type(cb::io::sanitizePath("/foo/bar/ep.so.1")));
}
