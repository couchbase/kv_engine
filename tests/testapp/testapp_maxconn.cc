/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

static const int MAX_CONNECTIONS = 20;
static const int SYSTEM_CONNECTIONS = 10;

class MaxConnectionTest : public TestappTest {
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

TEST_F(MaxConnectionTest, MaxUserConnectionsConnection) {
    const auto limit = MAX_CONNECTIONS - SYSTEM_CONNECTIONS;
    auto& connection = getAdminConnection();
    connection.selectBucket("default");
    auto current = getConnectionCounts(connection);
    while (current.first < limit) {
        connections.emplace_back(connection.clone());
        current = getConnectionCounts(connection);
    }

    try {
        auto c = connection.clone();
        FAIL() << "All connections should be consumed so connecting one more "
                  "should fail";
    } catch (const std::exception&) {
    }

    // But I should be able to create a system connection
    auto& c = prepare(connectionMap.getConnection("ssl"));
    c.authenticate("@admin", "password", "PLAIN");
}

TEST_F(MaxConnectionTest, SystemConnection) {
    // Locate the interface tagged as admin
    auto& connection = prepare(connectionMap.getConnection("ssl"));
    connection.authenticate("@admin", "password", "PLAIN");

    connection.selectBucket("default");
    auto current = getConnectionCounts(connection);
    while (current.second < SYSTEM_CONNECTIONS) {
        connections.emplace_back(connection.clone());
        current = getConnectionCounts(connection);
    }

    try {
        auto c = connection.clone();
        FAIL() << "All connections should be consumed so connecting one more "
                  "should fail";
    } catch (const std::exception&) {
    }

    // But I should be able to create a normal connection
    auto& conn = getConnection();
    conn.getSaslMechanisms();
}
