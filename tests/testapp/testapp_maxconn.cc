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

#include <folly/ScopeGuard.h>
#include <platform/timeutils.h>

static constexpr int USER_CONNECTIONS = 15;
static constexpr int SYSTEM_CONNECTIONS = 10;

class MaxConnectionTest : public TestappTest {
protected:
    void SetUp() override {
        memcached_cfg["max_connections"] =
                USER_CONNECTIONS + SYSTEM_CONNECTIONS;
        memcached_cfg["system_connections"] = SYSTEM_CONNECTIONS;
        reconfigure();

        admin = connectionMap.getConnection().clone();
        admin->authenticate("@admin");
        admin->selectBucket(bucketName);
        user = connectionMap.getConnection("ssl").clone();
        const auto [uc, sc] = getConnectionCounts();
        idleUser = uc;
        idleSystem = sc;
    }

    /**
     * Close the connections and wait until the server successfully
     * closed the connections and updated its internal counters.
     */
    void TearDown() override {
        connections.clear();
        for (;;) {
            const auto [uc, sc] = getConnectionCounts();
            if (uc <= idleUser && sc <= idleSystem) {
                break;
            }
            // back off to avoid busyloop
            std::this_thread::sleep_for(std::chrono::milliseconds{50});
        }
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
    int idleUser;
    int idleSystem;
    std::vector<std::unique_ptr<MemcachedConnection>> connections;
};

TEST_F(MaxConnectionTest, MaxUserConnectionsConnection) {
    auto current = getConnectionCounts();
    // Consume all the user connections
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
    c->authenticate("@admin");
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

/**
 * MB-74480: A connection closed by the client while one of its commands is
 * throttled must be freed. It used to stay in pending_close (holding its socket
 * and a reference on the bucket) until the bucket was deleted, so clients
 * timing out and reconnecting while the bucket is throttled would eventually
 * use up all of the user connections.
 */
TEST_F(MaxConnectionTest, MB74480_ClosedWhileThrottledConnectionsAreFreed) {
    const std::string name = "bucket";
    memcached_cfg["throttle_enabled"] = true;
    reconfigure();
    // A hard limit of 0 throttles every command (Luke isn't Unthrottled)
    mcd_env->getTestBucket().createBucket(
            name,
            "throttle_reserved=0;throttle_hard_limit=0",
            *adminConnection);
    auto cleanup = folly::makeGuard([this, &name] {
        // Deleting the bucket resets the throttled commands, which frees
        // any leaked connections
        adminConnection->deleteBucket(name);
        memcached_cfg["throttle_enabled"] = false;
        reconfigure();
    });

    auto getNumThrottled = [this, &name]() {
        size_t throttled = 0;
        admin->stats(
                [&throttled](const auto&, const auto& value) {
                    throttled = nlohmann::json::parse(value)["num_throttled"];
                },
                "bucket_details " + name);
        return throttled;
    };

    // Each client sends a GET, which is throttled, and disconnects
    for (int ii = idleUser; ii < USER_CONNECTIONS; ++ii) {
        const auto throttled = getNumThrottled();
        auto conn = user->clone();
        conn->authenticate("Luke");
        conn->selectBucket(name);
        conn->sendCommand(BinprotGetCommand("key"));
        ASSERT_TRUE(cb::waitForPredicateUntil(
                [&getNumThrottled, throttled]() {
                    return getNumThrottled() != throttled;
                },
                std::chrono::seconds{5},
                std::chrono::milliseconds{10}))
                << "The GET on connection " << ii << " was not throttled";
    }

    // Wait for the server to close them
    EXPECT_TRUE(cb::waitForPredicateUntil(
            [this]() { return getConnectionCounts().first == idleUser; },
            std::chrono::seconds{5},
            std::chrono::milliseconds{50}))
            << "The connections closed while throttled were not freed. "
            << "User connections: " << getConnectionCounts().first
            << " expected: " << idleUser;

    try {
        auto conn = user->clone();
        conn->getSaslMechanisms();
    } catch (const std::exception& e) {
        ADD_FAILURE() << "A new user connection was refused: " << e.what();
    }
}
