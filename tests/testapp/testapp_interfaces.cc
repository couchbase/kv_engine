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

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
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
