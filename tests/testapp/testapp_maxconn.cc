/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "testapp_client_test.h"

static const int USER_CONNECTIONS = 15;
static const int SYSTEM_CONNECTIONS = 10;

class MaxConnectionTest : public TestappTest {
protected:
    void SetUp() override {
        memcached_cfg["max_connections"] =
                USER_CONNECTIONS + SYSTEM_CONNECTIONS;
        memcached_cfg["system_connections"] = SYSTEM_CONNECTIONS;
        reconfigure();

        admin = connectionMap.getConnection().clone();
        admin->authenticate("@admin", "password", "PLAIN");
        admin->selectBucket(bucketName);
        user = connectionMap.getConnection("ssl").clone();
    }

    /// Return the number of "user" connections and "system" connections
    std::pair<int, int> getConnectionCounts() {
        int current = -1;
        int system = -1;
        admin->stats([&current, &system](const std::string& key,
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

        return {current - system, system};
    }

    std::unique_ptr<MemcachedConnection> admin;
    std::unique_ptr<MemcachedConnection> user;

    std::vector<std::unique_ptr<MemcachedConnection>> connections;
};

TEST_F(MaxConnectionTest, MaxUserConnectionsConnection) {
    auto current = getConnectionCounts();
    // Consume all of the user connections
    while (current.first < USER_CONNECTIONS) {
        connections.emplace_back(user->clone());
        connections.back()->getSaslMechanisms();
        current = getConnectionCounts();
    }

    try {
        auto c = user->clone();
        c->getSaslMechanisms();
        FAIL() << "All connections should be consumed so connecting one more "
                  "should fail";
    } catch (const std::exception&) {
    }

    // But I should be able to create a system connection
    auto c = admin->clone();
    c->authenticate("@admin", "password", "PLAIN");
}

TEST_F(MaxConnectionTest, SystemConnection) {
    auto current = getConnectionCounts();
    while (current.second < SYSTEM_CONNECTIONS) {
        connections.emplace_back(admin->clone());
        connections.back()->getSaslMechanisms();
        current = getConnectionCounts();
    }

    try {
        auto c = admin->clone();
        FAIL() << "All connections should be consumed so connecting one more "
                  "should fail";
    } catch (const std::exception&) {
    }

    // But I should be able to create a normal connection
    auto c = user->clone();
    c->getSaslMechanisms();
}
