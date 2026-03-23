/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "hostname_utils.h"

#include <platform/socket.h>

#include <folly/portability/GTest.h>

class HostnameUtilsTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {
        cb::net::initialize();

        auto* serv = getservbyname("echo", "tcp");
        if (serv) {
            known_service_name = serv->s_name;
            known_service_port = serv->s_port;
#ifndef WIN32
        } else {
            // iterate through available services to find one we can use for
            // testing port resolution by name
            setservent(1);
            while ((serv = getservent())) {
                if (strcmp(serv->s_proto, "tcp") == 0) {
                    known_service_name = serv->s_name;
                    known_service_port = serv->s_port;
                    break;
                }
            }
            endservent();
#endif
        }
    }

    static std::string known_service_name;
    static int known_service_port;
};

std::string HostnameUtilsTest::known_service_name;
int HostnameUtilsTest::known_service_port = -1;

TEST_F(HostnameUtilsTest, PlainPortByNumber) {
    const auto [host, in_port, family] =
            cb::inet::parse_hostname("localhost", "11210");
    EXPECT_EQ("localhost", host);
    EXPECT_EQ(11210, in_port);
    EXPECT_EQ(AF_UNSPEC, family);
}

TEST_F(HostnameUtilsTest, PlainPortByName) {
    if (known_service_name.empty()) {
        GTEST_SKIP() << "port by name service not available on this node";
    }
    const auto [host, in_port, family] =
            cb::inet::parse_hostname("localhost", known_service_name);
    EXPECT_EQ("localhost", host);
    EXPECT_EQ(known_service_port, in_port);
    EXPECT_EQ(AF_UNSPEC, family);
}

TEST_F(HostnameUtilsTest, PlainInvalidPort) {
  EXPECT_THROW(cb::inet::parse_hostname("localhost", "11a"),
               std::runtime_error);
}

TEST_F(HostnameUtilsTest, IPv4PortByNumberInHost) {
    const auto [host, in_port, family] =
            cb::inet::parse_hostname("localhost:11210", "6666");
    EXPECT_EQ("localhost", host);
    EXPECT_EQ(11210, in_port);
    EXPECT_EQ(AF_INET, family);
}

TEST_F(HostnameUtilsTest, IPv4PortByNameInHost) {
    if (known_service_name.empty()) {
        GTEST_SKIP() << "port by name service not available on this node";
    }
    const auto [host, in_port, family] =
            cb::inet::parse_hostname("localhost:" + known_service_name, "6666");
    EXPECT_EQ("localhost", host);
    EXPECT_EQ(known_service_port, in_port);
    EXPECT_EQ(AF_INET, family);
}


TEST_F(HostnameUtilsTest, IPv6PortByNumberInHost) {
    const auto [host, in_port, family] =
            cb::inet::parse_hostname("[::1]:11210", "6666");
    EXPECT_EQ("::1", host);
    EXPECT_EQ(11210, in_port);
    EXPECT_EQ(AF_INET6, family);
}

TEST_F(HostnameUtilsTest, IPv6PortByNameInHost) {
    if (known_service_name.empty()) {
        GTEST_SKIP() << "port by name service not available on this node";
    }
    const auto [host, in_port, family] =
            cb::inet::parse_hostname("[::1]:" + known_service_name, "6666");
    EXPECT_EQ("::1", host);
    EXPECT_EQ(known_service_port, in_port);
    EXPECT_EQ(AF_INET6, family);
}

TEST_F(HostnameUtilsTest, InvalidIPv6PortSpec) {
    EXPECT_THROW(cb::inet::parse_hostname("::1[:11210]", ""),
                 std::runtime_error);
}

