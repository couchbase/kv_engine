/*
 *     Copyright 2019 Couchbase, Inc.
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
#include "testapp.h"
#include "testapp_client_test.h"

/*
 * This test batch verifies that the interface array in the server may
 * be dynamically changed.
 */

class InterfacesTest : public TestappClientTest {
protected:
    void test_mb47707(bool whitelist_localhost_interface);
};

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        InterfacesTest,
                        ::testing::Values(TransportProtocols::McbpPlain),
                        ::testing::PrintToStringParamName());

TEST_P(InterfacesTest, AddRemoveInterface) {
    size_t total = 0;
    connectionMap.iterate([&total](MemcachedConnection& c) { ++total; });
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
    connectionMap.iterate([&current](MemcachedConnection& c) { ++current; });
    EXPECT_GT(current, total);

    // Remove the interface!
    memcached_cfg["interfaces"] = interfaces;
    reconfigure();
    parse_portnumber_file();

    // Check that I have
    current = 0;
    connectionMap.iterate([&current](MemcachedConnection& c) { ++current; });
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

void InterfacesTest::test_mb47707(bool whitelist_localhost_interface) {
    memcached_cfg["whitelist_localhost_interface"] =
            whitelist_localhost_interface;
    reconfigure();

    auto interfaces = memcached_cfg["interfaces"];
    memcached_cfg["interfaces"].emplace_back(
            nlohmann::json{{"tag", "MB-47707"},
                           {"port", 0},
                           {"ipv4", "required"},
                           {"ipv6", "off"},
                           {"host", "*"}});
    reconfigure();
    parse_portnumber_file();

    std::unique_ptr<MemcachedConnection> connection;
    connectionMap.iterate([&connection](const MemcachedConnection& c) {
        if (c.getTag() == "MB-47707") {
            connection = c.clone();
        }
    });
    ASSERT_TRUE(connection) << "Failed to locate the new connection";
    auto rsp = connection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::SaslListMechs});
    ASSERT_TRUE(rsp.isSuccess()) << "Status: " << to_string(rsp.getStatus())
                                 << " message " << rsp.getDataString();

    // Remove the interface!
    memcached_cfg["interfaces"] = interfaces;
    reconfigure();
    parse_portnumber_file();

    bool found = false;
    connectionMap.iterate([&found](const MemcachedConnection& c) {
        if (c.getTag() == "MB-47707") {
            found = true;
        }
    });
    ASSERT_FALSE(found) << "The port should have been gone";

    if (whitelist_localhost_interface) {
        // The connection should not be disconnected
        rsp = connection->execute(
                BinprotGenericCommand{cb::mcbp::ClientOpcode::SaslListMechs});
        ASSERT_TRUE(rsp.isSuccess()) << "Status: " << to_string(rsp.getStatus())
                                     << " message " << rsp.getDataString();
    } else {
        // The connection should be disconnected
        try {
            rsp = connection->execute(BinprotGenericCommand{
                    cb::mcbp::ClientOpcode::SaslListMechs});
            FAIL() << "Expected the connection to be disconnected.\n"
                   << "Status: " << to_string(rsp.getStatus())
                   << "\nmessage: " << rsp.getDataString();
        } catch (const std::system_error& error) {
            // we should probably have checked if the error code is
            // conn-reset, but then again that may be different on windows
            // mac and linux...
        }
    }
}

/// Verify that we don't disconnect localhost connections as part of
/// interface deletion if they're bound to localhost
TEST_P(InterfacesTest, MB_47707_LocalhostWhitelisted) {
    test_mb47707(true);
}

/// Verify that we disconnect localhost connections as part of
/// interface deletion even if they're bound to localhost
TEST_P(InterfacesTest, MB_47707_LocalhostNotWhitelisted) {
    test_mb47707(false);
}
