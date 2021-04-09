/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "testapp.h"
#include "testapp_client_test.h"

/*
 * This test batch verifies that the interface array in the server may
 * be dynamically changed.
 */

class InterfacesTest : public TestappClientTest {};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         InterfacesTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

TEST_P(InterfacesTest, AddRemoveInterface) {
    size_t total = 0;
    connectionMap.iterate([&total](const MemcachedConnection& c) { ++total; });
    auto interfaces = memcached_cfg["interfaces"];

    memcached_cfg["interfaces"][2] = {{"tag", "admin"},
                                      {"port", 0},
                                      {"ipv4", "required"},
                                      {"ipv6", "required"},
                                      {"host", "*"}};
    reconfigure();
    parse_portnumber_file();

    // Check that I have
    size_t current = 0;
    connectionMap.iterate(
            [&current](const MemcachedConnection& c) { ++current; });
    EXPECT_GT(current, total);

    // Remove the interface!
    memcached_cfg["interfaces"] = interfaces;
    reconfigure();
    parse_portnumber_file();

    // Check that I have
    current = 0;
    connectionMap.iterate(
            [&current](const MemcachedConnection& c) { ++current; });
    EXPECT_EQ(current, total);
}

TEST_P(InterfacesTest, DisableInAnyInterface) {
    size_t total = 0;
    connectionMap.iterate([&total](const MemcachedConnection& c) { ++total; });
    auto interfaces = memcached_cfg["interfaces"];

    memcached_cfg["interfaces"][2] = {{"tag", "DisableInAnyInterface"},
                                      {"port", 0},
                                      {"ipv4", "required"},
                                      {"ipv6", "off"},
                                      {"host", "*"}};
    reconfigure();
    parse_portnumber_file();

    // Find the port number it was assigned to so we can use that port
    // going forward
    in_port_t assignedPort = 0;
    connectionMap.iterate([&assignedPort](const MemcachedConnection& c) {
        if (c.getTag() == "DisableInAnyInterface") {
            assignedPort = c.getPort();
            ASSERT_EQ("0.0.0.0:" + std::to_string(assignedPort), c.getName());
        }
    });
    ASSERT_NE(0, assignedPort);

    // Check that we can go from ANY to localhost
    memcached_cfg["interfaces"][2] = {{"port", assignedPort},
                                      {"ipv4", "required"},
                                      {"ipv6", "off"},
                                      {"host", "127.0.0.1"}};

    reconfigure();
    parse_portnumber_file();
    bool ok = false;
    connectionMap.iterate([&assignedPort, &ok](const MemcachedConnection& c) {
        if (c.getPort() == assignedPort) {
            EXPECT_EQ("127.0.0.1:" + std::to_string(assignedPort), c.getName());
            ok = true;
        }
    });
    ASSERT_TRUE(ok) << "Did not locate the port entry";

    // Check that we can go back to ANY
    memcached_cfg["interfaces"][2] = {{"port", assignedPort},
                                      {"ipv4", "required"},
                                      {"ipv6", "off"},
                                      {"host", "*"}};
    reconfigure();
    parse_portnumber_file();
    ok = false;
    connectionMap.iterate([&assignedPort, &ok](const MemcachedConnection& c) {
        if (c.getPort() == assignedPort) {
            EXPECT_EQ("0.0.0.0:" + std::to_string(assignedPort), c.getName());
            ok = true;
        }
    });
    ASSERT_TRUE(ok) << "Did not locate the port entry";
    // restore the original interface array
    memcached_cfg["interfaces"] = interfaces;
    reconfigure();
    parse_portnumber_file();
}

TEST_P(InterfacesTest, AFamilyChangeInterface) {
    auto interfaces = memcached_cfg["interfaces"];

    memcached_cfg["interfaces"][2] = {{"tag", "AFamilyChangeInterface"},
                                      {"port", 0},
                                      {"ipv4", "required"},
                                      {"ipv6", "required"},
                                      {"host", "*"}};
    reconfigure();
    parse_portnumber_file();
    size_t total = 0;
    bool ipv4 = false;
    bool ipv6 = false;
    connectionMap.iterate([&total, &ipv4, &ipv6](const MemcachedConnection& c) {
        if (c.getTag() == "AFamilyChangeInterface") {
            sa_family_t afamily = c.getFamily();
            if (afamily == AF_INET) {
                ipv4 = true;
            } else if (afamily == AF_INET6) {
                ipv6 = true;
            }
            total++;
        }
    });
    ASSERT_TRUE(ipv4);
    ASSERT_TRUE(ipv6);

    // Check that Afamily change from both address family to one address
    // family results in desired interfaces.
    memcached_cfg["interfaces"][2] = {{"tag", "AFamilyChangeInterface"},
                                      {"port", 0},
                                      {"ipv4", "required"},
                                      {"ipv6", "off"},
                                      {"host", "*"}};

    reconfigure();
    parse_portnumber_file();
    size_t count = 0;
    connectionMap.iterate([&count](const MemcachedConnection& c) {
        if (c.getTag() == "AFamilyChangeInterface") {
            ASSERT_EQ(c.getFamily(), AF_INET);
            count++;
        }
    });
    EXPECT_GT(total, count);

    // Check that adding Afamily IPv6 causes desired interfaces.
    memcached_cfg["interfaces"][2] = {{"tag", "AFamilyChangeInterface"},
                                      {"port", 0},
                                      {"ipv4", "off"},
                                      {"ipv6", "required"},
                                      {"host", "*"}};
    memcached_cfg["interfaces"][3] = {{"tag", "AFamilyChangeInterface"},
                                      {"port", 0},
                                      {"ipv4", "required"},
                                      {"ipv6", "off"},
                                      {"host", "*"}};

    reconfigure();
    parse_portnumber_file();
    ipv4 = false;
    ipv6 = false;
    count = 0;
    connectionMap.iterate([&count, &ipv4, &ipv6](const MemcachedConnection& c) {
        if (c.getTag() == "AFamilyChangeInterface") {
            sa_family_t afamily = c.getFamily();
            if (afamily == AF_INET) {
                ipv4 = true;
            } else if (afamily == AF_INET6) {
                ipv6 = true;
            }
            count++;
        }
    });
    ASSERT_TRUE(ipv4);
    ASSERT_TRUE(ipv6);
    EXPECT_EQ(total, count);

    // restore the original interface array
    memcached_cfg["interfaces"] = interfaces;
    reconfigure();
    parse_portnumber_file();
}
