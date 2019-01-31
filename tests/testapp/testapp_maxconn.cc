/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include "testapp_client_test.h"

static const int MAX_CONNECTIONS = 20;
static const int SYSTEM_CONNECTIONS = 10;

class MaxConnectionTest : public TestappClientTest {
protected:
    void SetUp() override {
        memcached_cfg["max_connections"] = MAX_CONNECTIONS;
        memcached_cfg["system_connections"] = SYSTEM_CONNECTIONS;
        reconfigure();
    }

    std::pair<int, int> getConnectionCounts(MemcachedConnection& connection) {
        int current = -1;
        int system = -1;
        connection.stats([&current, &system](const std::string& key,
                                             const std::string& value) -> void {
            if (key == "curr_connections") {
                current = std::stoi(value);
            } else if (key == "system_connections") {
                system = std::stoi(value);
            }
        });

        if (current == -1 || system == -1) {
            throw std::runtime_error(
                    R"(Failed to locate "current" or "system")");
        }

        return {current, system};
    }

    std::vector<std::unique_ptr<MemcachedConnection>> connections;
};

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        MaxConnectionTest,
                        ::testing::Values(TransportProtocols::McbpPlain,
                                          TransportProtocols::McbpSsl),
                        ::testing::PrintToStringParamName());

TEST_P(MaxConnectionTest, MaxConnection) {
    auto& connection = getAdminConnection();
    connection.selectBucket("default");
    auto current = getConnectionCounts(connection);
    while (current.first < MAX_CONNECTIONS) {
        connections.emplace_back(connection.clone());
        current = getConnectionCounts(connection);
    }

    ASSERT_ANY_THROW(auto c = connection.clone())
            << "All connections should be consumed so connecting one more "
               "should fail";
}

TEST_P(MaxConnectionTest, SystemConnection) {
    auto& connection = getAdminConnection();
    connection.selectBucket("default");
    auto current = getConnectionCounts(connection);
    while (current.second < SYSTEM_CONNECTIONS) {
        auto c = connection.clone();
        c->authenticate("@admin", "password", "PLAIN");
        connections.emplace_back(std::move(c));
        current = getConnectionCounts(connection);
    }

    ASSERT_ANY_THROW(auto c = connection.clone();
                     c->authenticate("@admin", "password", "PLAIN"))
            << "All connections should be consumed so connecting one more "
               "should fail";

    // But I should be able to create a normal connection
    auto c = connection.clone();
    c->authenticate("smith", "smithpassword", "PLAIN");
    EXPECT_TRUE(c->execute(BinprotGenericCommand{cb::mcbp::ClientOpcode::Noop})
                        .isSuccess())
            << "Client should have been disconnected";
}

TEST_P(MaxConnectionTest, OnlyAuthAllowed) {
    // When we're hit the limit of "user" connection (max - system) clients
    // gets disconnected unless they execute a command which is one of the
    // authentication commands
    auto& connection = getAdminConnection();
    connection.selectBucket("default");
    auto current = getConnectionCounts(connection);

    while (current.first < (MAX_CONNECTIONS - SYSTEM_CONNECTIONS)) {
        connections.emplace_back(connection.clone());
        current = getConnectionCounts(connection);
    }

    auto client = connection.clone();
    // running noop will fail
    ASSERT_ANY_THROW(
            client->execute(BinprotGenericCommand{cb::mcbp::ClientOpcode::Noop})
                    .isSuccess())
            << "Client should have been disconnected";

    client->connect();

    // Hello should work
    client->hello("Foo", "testapp", "1.0");
    client->authenticate("@admin", "password", "PLAIN");

    // I should also be allowed to authenticate as a different user
    auto client2 = connection.clone();
    client2->authenticate("smith", "smithpassword", "PLAIN");

    // And normal commands should work
    EXPECT_TRUE(client2->execute(BinprotGenericCommand{
                                         cb::mcbp::ClientOpcode::Noop})
                        .isSuccess());
}
