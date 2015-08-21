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
                                          TransportProtocols::PlainGreenstack,
                                          TransportProtocols::PlainIpv6Mcbp,
                                          TransportProtocols::PlainIpv6Greenstack
                                         ));
#else

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        BucketTest,
                        ::testing::Values(TransportProtocols::PlainMcbp,
                                          TransportProtocols::PlainGreenstack,
                                          TransportProtocols::PlainIpv6Mcbp,
                                          TransportProtocols::PlainIpv6Greenstack,
                                          TransportProtocols::SslMcbp,
                                          TransportProtocols::SslGreenstack,
                                          TransportProtocols::SslIpv6Mcbp,
                                          TransportProtocols::SslIpv6Greenstack
                                         ));
#endif

std::ostream& operator<<(std::ostream& os, const TransportProtocols& t) {
    os << to_string(t);
    return os;
}

const char* to_string(const TransportProtocols& transport) {
    switch (transport) {
    case TransportProtocols::PlainMcbp:
        return "Mcbp";
    case TransportProtocols::PlainGreenstack:
        return "Greenstack";
    case TransportProtocols::PlainIpv6Mcbp:
        return "Mcbp-Ipv6";
    case TransportProtocols::PlainIpv6Greenstack:
        return "Greenstack-Ipv6";
    case TransportProtocols::SslMcbp:
        return "Mcbp-Ssl";
    case TransportProtocols::SslGreenstack:
        return "Greenstack-Ssl";
    case TransportProtocols::SslIpv6Mcbp:
        return "Mcbp-Ipv6-Ssl";
    case TransportProtocols::SslIpv6Greenstack:
        return "Greenstack-Ipv6-Ssl";
    }
    throw std::logic_error("Unknown transport");
}

MemcachedConnection& BucketTest::prepare(MemcachedConnection& connection) {
    connection.reconnect();
    auto* c = dynamic_cast<MemcachedGreenstackConnection*>(&connection);
    if (c == nullptr) {
        // Currently no init is needed for MCBP
    } else {
        c->hello("memcached_testapp", "1,0", "BucketTest");
    }
    return connection;
}

MemcachedConnection& BucketTest::getConnection() {
    switch (GetParam()) {
    case TransportProtocols::PlainMcbp:
        return prepare(connectionMap.getConnection(Protocol::Memcached,
                                                   false, AF_INET));
    case TransportProtocols::PlainGreenstack:
        return prepare(connectionMap.getConnection(Protocol::Greenstack,
                                                   false, AF_INET));
    case TransportProtocols::PlainIpv6Mcbp:
        return prepare(connectionMap.getConnection(Protocol::Memcached,
                                                   false, AF_INET6));
    case TransportProtocols::PlainIpv6Greenstack:
        return prepare(connectionMap.getConnection(Protocol::Greenstack,
                                                   false, AF_INET6));
    case TransportProtocols::SslMcbp:
        return prepare(connectionMap.getConnection(Protocol::Memcached,
                                                   true, AF_INET));
    case TransportProtocols::SslGreenstack:
        return prepare(connectionMap.getConnection(Protocol::Greenstack,
                                                   true, AF_INET));
    case TransportProtocols::SslIpv6Mcbp:
        return prepare(connectionMap.getConnection(Protocol::Memcached,
                                                   true, AF_INET6));
    case TransportProtocols::SslIpv6Greenstack:
        return prepare(connectionMap.getConnection(Protocol::Greenstack,
                                                   true, AF_INET6));
    }
    throw std::logic_error("Unknown transport");
}

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
