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

class InterfacesTest : public TestappClientTest {};

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
