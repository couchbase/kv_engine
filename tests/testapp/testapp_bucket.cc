/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#include "testapp_bucket.h"

#include <algorithm>

#ifdef WIN32
// There is a pending bug trying to figure out why SSL fails..
INSTANTIATE_TEST_CASE_P(PlainOrSSL,
                        BucketTest,
                        ::testing::Values(Transport::Plain));
#else

INSTANTIATE_TEST_CASE_P(PlainOrSSL,
                        BucketTest,
                        ::testing::Values(Transport::Plain,
                                          Transport::SSL));
#endif

MemcachedConnection& BucketTest::getConnection(const Protocol& protocol) {
    switch (GetParam()) {
    case Transport::Plain:
        return connectionMap.getConnection(protocol,
                                           false, AF_INET);
    case Transport::SSL:
        return connectionMap.getConnection(protocol,
                                           true, AF_INET);
    }
    throw std::logic_error("Unknown transport");
}

TEST_P(BucketTest, TestNameTooLong) {
    auto& connection = getConnection(Protocol::Memcached);
    std::string name;
    name.resize(101);
    std::fill(name.begin(), name.end(), 'a');

    try {
        connection.createBucket(name, "", Greenstack::Bucket::Memcached);
        FAIL() << "Invalid bucket name is not refused";
    } catch (ConnectionError &error) {
        // @todo implement remap of the error codes
    }
}

TEST_P(BucketTest, TestMaxNameLength) {
    auto& connection = getConnection(Protocol::Memcached);
    std::string name;
    name.resize(100);
    std::fill(name.begin(), name.end(), 'a');

    EXPECT_NO_THROW(connection.createBucket(name, "", Greenstack::Bucket::Memcached));
    EXPECT_NO_THROW(connection.deleteBucket(name));
}

TEST_P(BucketTest, TestEmptyName) {
    auto& connection = getConnection(Protocol::Memcached);
    try {
        connection.createBucket("", "", Greenstack::Bucket::Memcached);
        FAIL() << "Empty bucket name is not refused";
    } catch (ConnectionError &error) {
        // @todo implement remap of the error codes
    }
}

TEST_P(BucketTest, TestInvalidCharacters) {
    auto& connection = getConnection(Protocol::Memcached);

    std::string name("a ");

    for (int ii = 1; ii < 256; ++ii) {
        name.at(1) = char(ii);
        bool legal = true;

        // According to DOC-107:
        // "The bucket name can only contain characters in range A-Z, a-z, 0-9 as well as
        // underscore, period, dash and percent symbols"
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
            EXPECT_NO_THROW(connection.createBucket(name, "", Greenstack::Bucket::Memcached));
            EXPECT_NO_THROW(connection.deleteBucket(name));
        } else {
            try {
                connection.createBucket(name, "", Greenstack::Bucket::Memcached);
                FAIL() << "I was able to create a bucket with character of value " << ii;
            } catch (ConnectionError& ex) {
                // @todo check the real error code
            }
        }
    }
}

TEST_P(BucketTest, TestMultipleBuckets) {
    auto& connection = getConnection(Protocol::Memcached);

    int ii;
    try {
        for (ii = 1; ii < COUCHBASE_MAX_NUM_BUCKETS; ++ii) {
            std::string name = "bucket-" + std::to_string(ii);
            connection.createBucket(name, "", Greenstack::Bucket::Memcached);
        }
    } catch (ConnectionError& ex) {
         FAIL() << "Failed to create more than " << ii << " buckets";
    }

    for (--ii; ii > 0; --ii) {
        std::string name = "bucket-" + std::to_string(ii);
        EXPECT_NO_THROW(connection.deleteBucket(name));
    }
}
