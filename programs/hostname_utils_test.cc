/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
    }
};

TEST_F(HostnameUtilsTest, PlainPortByNumber) {
    std::string host;
    in_port_t in_port;
    sa_family_t family;

    std::tie(host, in_port, family) = cb::inet::parse_hostname("localhost",
                                                               "11210");
    EXPECT_EQ("localhost", host);
    EXPECT_EQ(11210, in_port);
    EXPECT_EQ(AF_UNSPEC, family);
}

TEST_F(HostnameUtilsTest, PlainPortByName) {
    std::string host;
    in_port_t in_port;
    sa_family_t family;

    std::tie(host, in_port, family) = cb::inet::parse_hostname("localhost",
                                                               "echo");
    EXPECT_EQ("localhost", host);
    EXPECT_EQ(7, ntohs(in_port));
    EXPECT_EQ(AF_UNSPEC, family);
}

TEST_F(HostnameUtilsTest, PlainInvalidPort) {
  EXPECT_THROW(cb::inet::parse_hostname("localhost", "11a"),
               std::runtime_error);
}

TEST_F(HostnameUtilsTest, IPv4PortByNumberInHost) {
    std::string host;
    in_port_t in_port;
    sa_family_t family;

    std::tie(host, in_port, family) = cb::inet::parse_hostname("localhost:11210",
                                                               "6666");
    EXPECT_EQ("localhost", host);
    EXPECT_EQ(11210, in_port);
    EXPECT_EQ(AF_INET, family);
}

TEST_F(HostnameUtilsTest, IPv4PortByNameInHost) {
    std::string host;
    in_port_t in_port;
    sa_family_t family;

    std::tie(host, in_port, family) = cb::inet::parse_hostname("localhost:echo",
                                                               "6666");
    EXPECT_EQ("localhost", host);
    EXPECT_EQ(7, ntohs(in_port));
    EXPECT_EQ(AF_INET, family);
}


TEST_F(HostnameUtilsTest, IPv6PortByNumberInHost) {
    std::string host;
    in_port_t in_port;
    sa_family_t family;

    std::tie(host, in_port, family) = cb::inet::parse_hostname("[::1]:11210",
                                                               "6666");
    EXPECT_EQ("::1", host);
    EXPECT_EQ(11210, in_port);
    EXPECT_EQ(AF_INET6, family);
}

TEST_F(HostnameUtilsTest, IPv6PortByNameInHost) {
    std::string host;
    in_port_t in_port;
    sa_family_t family;

    std::tie(host, in_port, family) = cb::inet::parse_hostname("[::1]:echo",
                                                               "6666");
    EXPECT_EQ("::1", host);
    EXPECT_EQ(7, ntohs(in_port));
    EXPECT_EQ(AF_INET6, family);
}

TEST_F(HostnameUtilsTest, InvalidIPv6PortSpec) {
    EXPECT_THROW(cb::inet::parse_hostname("::1[:11210]", ""),
                 std::runtime_error);
}

