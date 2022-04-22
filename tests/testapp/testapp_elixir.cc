/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp.h"
#include "testapp_client_test.h"

#include <nlohmann/json.hpp>
#include <protocol/connection/frameinfo.h>

class ElixirTest : public TestappXattrClientTest {
public:
    /// Calculate the number of compute units the provided value should occupy
    std::size_t calc_num_cu(std::size_t val) {
        return val / 1024 + (val % 1024 ? 1 : 0);
    }

protected:
    std::pair<std::size_t, std::size_t> getComputeUnits() {
        std::size_t rcu = 0, wcu = 0;
        bool found = false;

        adminConnection->stats(
                [this, &rcu, &wcu, &found](const auto& k, const auto& v) {
                    if (!v.empty()) {
                        auto json = nlohmann::json::parse(v);
                        for (nlohmann::json& bucket : json["buckets"]) {
                            if (bucket["name"] == bucketName) {
                                rcu = bucket["rcu"].get<std::size_t>();
                                wcu = bucket["wcu"].get<std::size_t>();
                                found = true;
                            }
                        }
                    }
                },
                "bucket_details");
        if (found) {
            return {rcu, wcu};
        }
        throw std::runtime_error("getComputeUnits(): Bucket not found");
    }

    /// Execute a command and verify that RCU and WCU was updated to the
    /// correct values
    void execute(std::function<std::pair<std::size_t, std::size_t>()> command,
                 const char* message) {
        const auto [pre_rcu, pre_wcu] = getComputeUnits();
        auto [rcu, wcu] = command();
        const auto [post_rcu, post_wcu] = getComputeUnits();
        EXPECT_EQ(pre_rcu + rcu, post_rcu)
                << "Expected " << message << " to increase RCU with " << rcu;
        EXPECT_EQ(pre_wcu + wcu, post_wcu)
                << "Expected " << message << " to increase WCU with " << wcu;
    }
};

INSTANTIATE_TEST_SUITE_P(
        TransportProtocols,
        ElixirTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpSsl),
                           ::testing::Values(XattrSupport::Yes),
                           ::testing::Values(ClientJSONSupport::Yes),
                           ::testing::Values(ClientSnappySupport::Yes)),
        PrintToStringCombinedName());

TEST_P(ElixirTest, TestAddGet) {
    const auto ncu = calc_num_cu(document.value.size());
    execute(
            [ncu, this]() -> std::pair<std::size_t, std::size_t> {
                userConnection->mutate(document, Vbid{0}, MutationType::Add);
                return {0, ncu};
            },
            "ADD");

    execute(
            [ncu, this]() -> std::pair<std::size_t, std::size_t> {
                userConnection->get(document.info.id, Vbid{0});
                return {ncu, 0};
            },
            "GET");
}

TEST_P(ElixirTest, TestSetBucketComputeUnitThrottleLimitsPayload) {
    // set to 10000
    auto rsp = adminConnection->execute(
            SetBucketComputeUnitThrottleLimitCommand{bucketName, 10000});
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getDataString();
    // Disable
    rsp = adminConnection->execute(
            SetBucketComputeUnitThrottleLimitCommand{bucketName});
    ASSERT_TRUE(rsp.isSuccess());
    // Try a bucket which don't exist
    rsp = adminConnection->execute(SetBucketComputeUnitThrottleLimitCommand{
            "TestSetBucketComputeUnitThrottleLimitsPayload"});
    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());

    // And anormal user don't have the privilege
    rsp = userConnection->execute(
            SetBucketComputeUnitThrottleLimitCommand{bucketName, 100});
    ASSERT_EQ(cb::mcbp::Status::Eaccess, rsp.getStatus());
}

// @todo add more tests
