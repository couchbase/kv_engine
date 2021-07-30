/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp.h"
#include "testapp_client_test.h"

class TenantTest : public TestappClientTest {
public:
    void SetUp() override {
        memcached_cfg["enforce_tenant_limits_enabled"] = true;
        reconfigure();
    }

    void TearDown() override {
        memcached_cfg["enforce_tenant_limits_enabled"] = false;
        reconfigure();
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         TenantTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

/// Disable the test as I've changed the way to enable/disable the tenant
/// tests by checking for a directory (which we don't have in testapp)
TEST_P(TenantTest, TenantStats) {
    auto& conn = getAdminConnection();
    // We should not have any tenants yet
    conn.stats(
            [](const std::string& key, const std::string& value) -> void {
                FAIL() << "We just enabled tenant stats so no one should "
                          "exist, but received: "
                       << std::endl
                       << key << " - " << value;
            },
            "tenants");

    conn.createBucket("rbac_test", "", BucketType::Memcached);

    auto clone = conn.clone();
    clone->authenticate("jones", "jonespassword", "PLAIN");
    bool found = false;
    conn.stats(
            [&found](const std::string& key, const std::string& value) -> void {
                EXPECT_EQ(R"({"domain":"local","user":"jones"})", key);

                auto json = nlohmann::json::parse(value);
                // We've not sent any commands after we authenticated
                EXPECT_EQ(0, json["ingress_bytes"].get<int>());

                // But we did send the reply to the AUTH so we should have
                // sent 1 mcbp response header
                EXPECT_EQ(sizeof(cb::mcbp::Header),
                          json["egress_bytes"].get<int>());

                EXPECT_EQ(1, json["connections"]["current"].get<int>());
                EXPECT_EQ(1, json["connections"]["total"].get<int>());
                found = true;
            },
            R"(tenants {"domain":"local","user":"jones"})");

    ASSERT_TRUE(found) << "Expected tenant data to be found for jones";
    found = false;
    clone->execute(BinprotGenericCommand{cb::mcbp::ClientOpcode::Noop});
    conn.stats(
            [&found](const std::string& key, const std::string& value) -> void {
                EXPECT_EQ(R"({"domain":"local","user":"jones"})", key);

                auto json = nlohmann::json::parse(value);
                EXPECT_EQ(sizeof(cb::mcbp::Header),
                          json["ingress_bytes"].get<int>());
                EXPECT_EQ(sizeof(cb::mcbp::Header) + sizeof(cb::mcbp::Header),
                          json["egress_bytes"].get<int>());
                EXPECT_EQ(1, json["connections"]["current"].get<int>());
                EXPECT_EQ(1, json["connections"]["total"].get<int>());
                found = true;
            },
            R"(tenants {"domain":"local","user":"jones"})");
    ASSERT_TRUE(found) << "Expected tenant data to be found for jones";
    found = false;

    // Reconnect and verify that we keep the correct # for total connections
    clone->reconnect();
    clone->authenticate("jones", "jonespassword", "PLAIN");
    conn.stats(
            [&found](const std::string& key, const std::string& value) -> void {
                EXPECT_EQ(R"({"domain":"local","user":"jones"})", key);

                auto json = nlohmann::json::parse(value);
                EXPECT_EQ(sizeof(cb::mcbp::Header),
                          json["ingress_bytes"].get<int>());
                EXPECT_EQ(3 * sizeof(cb::mcbp::Header),
                          json["egress_bytes"].get<int>());
                // We can't validate the "current" connection here, reconnect
                // could cause us to end up on a different front end thread
                // on memcached and we can't really predict the scheduling
                // so we could end up getting here _before_ the disconnect
                // logic happened on the other thread (we've seen this in
                // one CV unit test failure already)
                EXPECT_EQ(2, json["connections"]["total"].get<int>());
                found = true;
            },
            R"(tenants {"domain":"local","user":"jones"})");
    ASSERT_TRUE(found) << "Expected tenant data to be found for jones";
    found = false;

    // verify that we can request all tenants
    conn.stats(
            [&found](const std::string& key, const std::string& value) -> void {
                EXPECT_EQ("0", key);
                EXPECT_NE(std::string::npos,
                          value.find(
                                  R"("id":{"domain":"local","user":"jones"})"));
                found = true;
            },
            "tenants");
    ASSERT_TRUE(found) << "Expected tenant data to be found for jones";

    if (!folly::kIsSanitize) { // NOLINT
        // make sure we can rate limit. Hopefully the CV allows for 6000 noop/s
        bool error = false;
        while (!error) {
            auto rsp = clone->execute(
                    BinprotGenericCommand{cb::mcbp::ClientOpcode::Noop});
            if (!rsp.isSuccess()) {
                EXPECT_EQ(cb::mcbp::Status::RateLimitedMaxCommands,
                          rsp.getStatus())
                        << rsp.getDataString();
                error = true;
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds{1});
        auto rsp = clone->execute(
                BinprotGenericCommand{cb::mcbp::ClientOpcode::Noop});
        ASSERT_TRUE(rsp.isSuccess()) << to_string(rsp.getStatus());
    }

    // verify that we can't create as many connections that we want
    bool done = false;
    std::vector<std::unique_ptr<MemcachedConnection>> connections;
    while (!done) {
        // We should always be able to connect (as we don't know who
        // the tenant is so we can't be disconnected for that)
        connections.push_back(clone->clone());
        // We should always be able to authenticate (we don't want to "break"
        // clients by reporting authenticate error for a correct username
        // password combination, but that you're out of connections)
        connections.back()->authenticate("jones", "jonespassword", "PLAIN");
        // But we'll disconnect you upon the first command you try to
        // execute
        auto rsp = connections.back()->execute(
                BinprotGenericCommand(cb::mcbp::ClientOpcode::Noop));
        if (!rsp.isSuccess()) {
            // Command failed, so it should be rate limited
            ASSERT_EQ(cb::mcbp::Status::RateLimitedMaxConnections,
                      rsp.getStatus());
            done = true;
        }
    }

    // Verify that the stats recorded the rate limiting
    conn.stats(
            [&found](const std::string& key, const std::string& value) -> void {
                EXPECT_EQ(R"({"domain":"local","user":"jones"})", key);

                auto json = nlohmann::json::parse(value);
                const auto& limited = json["rate_limited"];
                EXPECT_EQ(1, limited["num_connections"].get<int>());
                if (!folly::kIsSanitize) { // NOLINT
                    EXPECT_EQ(1, limited["num_ops_per_min"].get<int>());
                }
                found = true;
            },
            R"(tenants {"domain":"local","user":"jones"})");
    ASSERT_TRUE(found) << "Expected tenant data to be found for jones";
    found = false;

    // Verify that the number of current connections is correct:
    conn.stats(
            [&found](const std::string& key, const std::string& value) -> void {
                EXPECT_EQ(R"({"domain":"local","user":"jones"})", key);

                auto json = nlohmann::json::parse(value);
                EXPECT_EQ(10, json["connections"]["current"].get<int>());
                found = true;
            },
            R"(tenants {"domain":"local","user":"jones"})");
    ASSERT_TRUE(found) << "Expected tenant data to be found for jones";
    found = false;

    // Close all of the connections, and verify that the current connection
    // counter should drop. Multiple threads are involved so we need to
    // run a loop and wait..
    connections.clear();
    auto timeout = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    int current = 0;
    do {
        conn.stats(
                [&found, &current](const std::string& key,
                                   const std::string& value) -> void {
                    EXPECT_EQ(R"({"domain":"local","user":"jones"})", key);
                    auto json = nlohmann::json::parse(value);
                    current = json["connections"]["current"].get<int>();
                    found = true;
                },
                R"(tenants {"domain":"local","user":"jones"})");
        ASSERT_TRUE(found) << "Expected tenant data to be found for jones";
        found = false;
        if (current != 1) {
            // back off to let the other threads run so we don't busy-wait
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    } while (current != 1 && std::chrono::steady_clock::now() < timeout);
    if (current != 1) {
        FAIL() << "Timed out waiting for current connections to drop to 1";
    }

    conn.deleteBucket("rbac_test");
}
