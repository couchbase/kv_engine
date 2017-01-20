/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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
#include "config.h"
#include "hostname_utils.h"

#include <gtest/gtest.h>

class HostnameUtilsTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {
        cb_initialize_sockets();
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

