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
#include "testapp_greenstack.h"
#include <libgreenstack/Greenstack.h>

#if 0

// Disabled while we're refactoring to bufferevents

#ifdef WIN32
// There is a pending bug trying to figure out why SSL fails..
INSTANTIATE_TEST_CASE_P(PlainOrSSL,
                        GreenstackTest,
                        ::testing::Values(Transport::Plain));
#else

INSTANTIATE_TEST_CASE_P(PlainOrSSL,
                        GreenstackTest,
                        ::testing::Values(Transport::Plain,
                                          Transport::PlainIpv6,
                                          Transport::SSL,
                                          Transport::SslIpv6));
#endif

#endif


MemcachedGreenstackConnection& GreenstackTest::getConnection() {
    switch (GetParam()) {
    case Transport::Plain:
        return dynamic_cast<MemcachedGreenstackConnection&>(
            connectionMap.getConnection(Protocol::Greenstack,
                                        false, AF_INET));
    case Transport::PlainIpv6:
        return dynamic_cast<MemcachedGreenstackConnection&>(
            connectionMap.getConnection(Protocol::Greenstack,
                                        false, AF_INET6));
    case Transport::SSL:
        return dynamic_cast<MemcachedGreenstackConnection&>(
            connectionMap.getConnection(Protocol::Greenstack,
                                        true, AF_INET));
    case Transport::SslIpv6:
        return dynamic_cast<MemcachedGreenstackConnection&>(
            connectionMap.getConnection(Protocol::Greenstack,
                                        true, AF_INET6));
    }
    throw std::logic_error("Unknown transport");
}

TEST_P(GreenstackTest, TestHello) {
    MemcachedGreenstackConnection& connection = getConnection();
    connection.hello("memcached_testapp", "1,0", "HelloTest");
    connection.reconnect();
}

TEST_P(GreenstackTest, TestSaslFail) {
    MemcachedGreenstackConnection& connection = getConnection();
    connection.hello("memcached_testapp", "1,0", "TestSaslFail");
    EXPECT_THROW(connection.authenticate("_admin", "foo",
                                         connection.getSaslMechanisms()),
                 std::runtime_error);
    connection.reconnect();
}

TEST_P(GreenstackTest, TestSaslPlain) {
    MemcachedGreenstackConnection& connection = getConnection();
    connection.hello("memcached_testapp", "1,0", "TestSaslPlain");
    connection.authenticate("_admin", "password", "PLAIN");
    connection.reconnect();
}

TEST_P(GreenstackTest, TestSaslCramMd5) {
    MemcachedGreenstackConnection& connection = getConnection();
    connection.hello("memcached_testapp", "1,0", "TestSaslCramMd5");
    connection.authenticate("_admin", "password", "CRAM-MD5");
    connection.reconnect();
}

TEST_P(GreenstackTest, TestSaslAutoSelectMechs) {
    MemcachedGreenstackConnection& connection = getConnection();
    connection.hello("memcached_testapp", "1,0", "TestSaslCramMd5");
    connection.authenticate("_admin", "password",
                            connection.getSaslMechanisms());
    connection.reconnect();
}

TEST_P(GreenstackTest, TestCreateDeleteBucket) {
    MemcachedGreenstackConnection& connection = getConnection();
    connection.hello("memcached_testapp", "1,0", "TestSaslCramMd5");
    connection.authenticate("_admin", "password",
                            connection.getSaslMechanisms());
    connection.createBucket(name, "", Greenstack::BucketType::Memcached);
    connection.deleteBucket(name);
}
