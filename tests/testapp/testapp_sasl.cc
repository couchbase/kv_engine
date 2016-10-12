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
#include <cbcrypto/cbcrypto.h>
#include "testapp_sasl.h"

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        SaslTest,
                        ::testing::Values(TransportProtocols::McbpPlain,
                                          TransportProtocols::McbpIpv6Plain,
                                          TransportProtocols::McbpSsl,
                                          TransportProtocols::McbpIpv6Ssl
                                         ),
                        ::testing::PrintToStringParamName());

static const std::string bucket1("bucket-1");
static const std::string password1("1S|=,%#x1");
static const std::string bucket2("bucket-2");
static const std::string password2("secret");

TEST_P(SaslTest, SinglePLAIN) {
    MemcachedConnection& conn = getConnection();
    EXPECT_NO_THROW(conn.authenticate(bucket1, password1, "PLAIN"));
}

TEST_P(SaslTest, SingleSCRAM_SHA1) {
    if (cb::crypto::isSupported(cb::crypto::Algorithm::SHA1)) {
        MemcachedConnection& conn = getConnection();
        EXPECT_NO_THROW(conn.authenticate(bucket1, password1, "SCRAM-SHA1"));
    }
}

TEST_P(SaslTest, SingleSCRAM_SHA256) {
    if (cb::crypto::isSupported(cb::crypto::Algorithm::SHA256)) {
        MemcachedConnection& conn = getConnection();
        EXPECT_NO_THROW(conn.authenticate(bucket1, password1, "SCRAM-SHA256"));
    }
}

TEST_P(SaslTest, SingleSCRAM_SHA512) {
    if (cb::crypto::isSupported(cb::crypto::Algorithm::SHA512)) {
        MemcachedConnection& conn = getConnection();
        EXPECT_NO_THROW(conn.authenticate(bucket1, password1, "SCRAM-SHA512"));
    }
}

void SaslTest::testMixStartingFrom(const std::string& mechanism) {
    MemcachedConnection& conn = getConnection();

    for (const auto &mech : mechanisms) {
        conn.reconnect();
        EXPECT_NO_THROW(conn.authenticate(bucket1, password1, mechanism));
        EXPECT_NO_THROW(conn.authenticate(bucket2, password2, mech));
    }
}

TEST_P(SaslTest, TestSaslMixFrom_PLAIN) {
    testMixStartingFrom("PLAIN");
}

TEST_P(SaslTest, TestSaslMixFrom_SCRAM_SHA1) {
    if (cb::crypto::isSupported(cb::crypto::Algorithm::SHA1)) {
        testMixStartingFrom("SCRAM-SHA1");
    }
}

TEST_P(SaslTest, TestSaslMixFrom_SCRAM_SHA256) {
    if (cb::crypto::isSupported(cb::crypto::Algorithm::SHA256)) {
        testMixStartingFrom("SCRAM-SHA256");
    }
}

TEST_P(SaslTest, TestSaslMixFrom_SCRAM_SHA512) {
    if (cb::crypto::isSupported(cb::crypto::Algorithm::SHA512)) {
        testMixStartingFrom("SCRAM-SHA512");
    }
}


void SaslTest::SetUp() {
    auto& connection = getConnection();

    ASSERT_NO_THROW(connection.createBucket(bucket1, "",
                                            Greenstack::BucketType::Memcached));
    ASSERT_NO_THROW(connection.createBucket(bucket2, "",
                                            Greenstack::BucketType::Memcached));
}

void SaslTest::TearDown() {
    auto& connection = getConnection();
    ASSERT_NO_THROW(connection.deleteBucket(bucket1));
    ASSERT_NO_THROW(connection.deleteBucket(bucket2));
}

