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
#include "testapp_greenstack_connection.h"

#include <algorithm>

#ifdef WIN32
// There is a pending bug trying to figure out why SSL fails..
INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        BucketTest,
                        ::testing::Values(TransportProtocols::PlainMcbp,
                                          TransportProtocols::PlainIpv6Mcbp
                                         ));

#else

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        BucketTest,
                        ::testing::Values(TransportProtocols::PlainMcbp,
                                          TransportProtocols::PlainIpv6Mcbp,
                                          TransportProtocols::SslMcbp,
                                          TransportProtocols::SslIpv6Mcbp
                                         ));
#endif


TEST_P(BucketTest, TestNameTooLong) {
    auto& connection = getConnection();
    std::string name;
    name.resize(101);
    std::fill(name.begin(), name.end(), 'a');

    try {
        connection.createBucket(name, "", Greenstack::BucketType::Memcached);
        FAIL() << "Invalid bucket name is not refused";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments()) << error.getReason();
    }
}

TEST_P(BucketTest, TestMaxNameLength) {
    auto& connection = getConnection();
    std::string name;
    name.resize(100);
    std::fill(name.begin(), name.end(), 'a');

    EXPECT_NO_THROW(connection.createBucket(name, "",
                                            Greenstack::BucketType::Memcached));
    EXPECT_NO_THROW(connection.deleteBucket(name));
}

TEST_P(BucketTest, TestEmptyName) {
    auto& connection = getConnection();

    if (connection.getProtocol() == Protocol::Greenstack) {
        // libgreenstack won't allow us to send such packets
        return;
    }

    try {
        connection.createBucket("", "", Greenstack::BucketType::Memcached);
        FAIL() << "Empty bucket name is not refused";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments()) << error.getReason();
    }
}

TEST_P(BucketTest, TestInvalidCharacters) {
    auto& connection = getConnection();

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
            EXPECT_NO_THROW(connection.createBucket(name, "",
                                                    Greenstack::BucketType::Memcached));
            EXPECT_NO_THROW(connection.deleteBucket(name));
        } else {
            try {
                connection.createBucket(name, "",
                                        Greenstack::BucketType::Memcached);
                FAIL() <<
                       "I was able to create a bucket with character of value " <<
                       ii;
            } catch (ConnectionError& error) {
                EXPECT_TRUE(error.isInvalidArguments()) << error.getReason();
            }
        }
    }
}

TEST_P(BucketTest, TestMultipleBuckets) {
    auto& connection = getConnection();

    int ii;
    try {
        for (ii = 1; ii < COUCHBASE_MAX_NUM_BUCKETS; ++ii) {
            std::string name = "bucket-" + std::to_string(ii);
            connection.createBucket(name, "", Greenstack::BucketType::Memcached);
        }
    } catch (ConnectionError& ex) {
        FAIL() << "Failed to create more than " << ii << " buckets";
    }

    for (--ii; ii > 0; --ii) {
        std::string name = "bucket-" + std::to_string(ii);
        EXPECT_NO_THROW(connection.deleteBucket(name));
    }
}

TEST_P(BucketTest, TestCreateBucketAlreadyExists) {
    auto& conn = getConnection();
    try {
        conn.createBucket("default", "", Greenstack::BucketType::Memcached);
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAlreadyExists()) << error.getReason();
    }
}

TEST_P(BucketTest, TestDeleteNonexistingBucket) {
    auto& conn = getConnection();
    try {
        conn.deleteBucket("ItWouldBeSadIfThisBucketExisted");
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound()) << error.getReason();
    }
}

TEST_P(BucketTest, TestListBucket) {
    auto& conn = getConnection();
    auto buckets = conn.listBuckets();
    EXPECT_EQ(1, buckets.size());
    EXPECT_EQ(std::string("default"), buckets[0]);
}


TEST_P(BucketTest, TestBucketIsolationBuckets)
{
    auto& connection = getConnection();

    for (int ii = 1; ii < COUCHBASE_MAX_NUM_BUCKETS; ++ii) {
        std::string name = "bucket-" + std::to_string(ii);
        connection.createBucket(name, "", Greenstack::BucketType::Memcached);
    }


    // I should be able to select each bucket and the same document..
    Document doc;
    doc.info.cas = Greenstack::CAS::Wildcard;
    doc.info.compression = Greenstack::Compression::None;
    doc.info.datatype = Greenstack::Datatype::Raw;
    doc.info.flags = 0xcaffee;
    doc.info.id = "TestBucketIsolationBuckets";
    char* ptr = cJSON_Print(memcached_cfg.get());
    std::copy(ptr, ptr + strlen(ptr), std::back_inserter(doc.value));
    cJSON_Free(ptr);

    for (int ii = 1; ii < COUCHBASE_MAX_NUM_BUCKETS; ++ii) {
        std::string name = "bucket-" + std::to_string(ii);
        EXPECT_NO_THROW(connection.selectBucket(name));
        EXPECT_NO_THROW(connection.mutate(doc, 0,
                                          Greenstack::MutationType::Add));
    }

    // Delete all buckets
    for (int ii = 1; ii < COUCHBASE_MAX_NUM_BUCKETS; ++ii) {
        std::string name = "bucket-" + std::to_string(ii);
        EXPECT_NO_THROW(connection.deleteBucket(name));
    }
}
