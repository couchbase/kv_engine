/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "testapp_client_test.h"
#include <folly/ScopeGuard.h>

class BucketConfigTest : public TestappClientTest {
public:
    void SetUp() override {
        TestappClientTest::SetUp();
    }

    void TearDown() override {
        TestappClientTest::TearDown();
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         BucketConfigTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

TEST_P(BucketConfigTest, ValidateSingleParameter) {
    using namespace std::string_view_literals;

    const auto testConfig = "bucket_type=persistent";
    auto response = adminConnection->validateBucketConfig(
            testConfig, BucketType::Couchbase);
    auto exitGuard = folly::makeGuard([response]() {
        if (HasFailure()) {
            FAIL() << response.dump(4);
        }
    });

    EXPECT_GT(response.size(), 10) << "Expected default values to be set";
    EXPECT_EQ(response["bucket_type"]["value"], "persistent");

    // Check that the default value is set.
    EXPECT_EQ(response["access_scanner_enabled"]["value"], true)
            << "Expected default value to be set";
}

TEST_P(BucketConfigTest, ValidateMultipleParameters) {
    using namespace std::string_view_literals;

    const auto testConfig =
            "bucket_type=persistent;item_eviction_policy=invalid;non_existent="
            "1234";
    auto response = adminConnection->validateBucketConfig(
            testConfig, BucketType::Couchbase);
    auto exitGuard = folly::makeGuard([response]() {
        if (HasFailure()) {
            FAIL() << response.dump(4);
        }
    });

    EXPECT_EQ(response.size(), 3)
            << "Did not expect default values to be set when validation fails";

    EXPECT_EQ(response["bucket_type"]["value"], "persistent");

    EXPECT_EQ(response["item_eviction_policy"]["error"], "invalid_arguments");
    EXPECT_EQ(response["item_eviction_policy"]["message"],
              "Invalid value for item_eviction_policy: invalid");

    EXPECT_EQ(response["non_existent"]["error"], "unsupported");
    EXPECT_EQ(response["non_existent"]["message"],
              "Parameter not supported by this bucket");
}

TEST_P(BucketConfigTest, ValidateInvalidBucketType) {
    BinprotValidateBucketConfigCommand command("invalid.so", "");
    auto response = adminConnection->execute(command);
    EXPECT_EQ(response.getStatus(), cb::mcbp::Status::Einval);
}

TEST_P(BucketConfigTest, ValidateInvalidTypeConfig) {
    using namespace std::string_view_literals;

    const auto testConfig =
            "access_scanner_enabled=bool;alog_sleep_time=int;checkpoint_memory_"
            "ratio=double";
    auto response = adminConnection->validateBucketConfig(
            testConfig, BucketType::Couchbase);
    auto exitGuard = folly::makeGuard([response]() {
        if (HasFailure()) {
            FAIL() << response.dump(4);
        }
    });

    EXPECT_EQ(response.size(), 3)
            << "Did not expect default values to be set when validation fails";

    EXPECT_EQ(response["access_scanner_enabled"]["error"], "invalid_arguments");
    EXPECT_EQ(response["alog_sleep_time"]["error"], "invalid_arguments");
    EXPECT_EQ(response["checkpoint_memory_ratio"]["error"],
              "invalid_arguments");
}

TEST_P(BucketConfigTest, ValidateConditionalParameters) {
    using namespace std::string_view_literals;

    const auto testConfig = "bucket_type=ephemeral";
    auto response = adminConnection->validateBucketConfig(
            testConfig, BucketType::Couchbase);

    // Make sure ephemeral params are present and public and persistent params
    // are not present. We should have the ephemeral_full_policy parameter and
    // not have item_eviction_policy parameter.
    EXPECT_EQ(response["ephemeral_full_policy"]["value"], "auto_delete");
    EXPECT_EQ(response.find("item_eviction_policy"), response.end());
}

TEST_P(BucketConfigTest, ValidateThrottleParameters) {
    using namespace std::string_view_literals;

    const auto testConfig =
            "bucket_type=persistent;throttle_reserved=1000;throttle_hard_limit="
            "2000";
    auto response = adminConnection->validateBucketConfig(
            testConfig, BucketType::Couchbase);
    EXPECT_EQ(response["throttle_reserved"]["value"], "1000");
    EXPECT_EQ(response["throttle_hard_limit"]["value"], "2000");
}

TEST_P(BucketConfigTest, ValidateInvalidThrottleParameters) {
    using namespace std::string_view_literals;

    const auto testConfig =
            "bucket_type=persistent;throttle_reserved=invalid;throttle_hard_"
            "limit=invalid";
    auto response = adminConnection->validateBucketConfig(
            testConfig, BucketType::Couchbase);
    EXPECT_EQ(response["throttle_reserved"]["error"], "invalid_arguments");
    EXPECT_EQ(response["throttle_hard_limit"]["error"], "invalid_arguments");
}
