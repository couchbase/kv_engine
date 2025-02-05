/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "config.h"
#include <folly/portability/GTest.h>
#include <unordered_set>

class ConfigTest : public ::testing::Test {};

TEST_F(ConfigTest, ToStringsWorks) {
    auto config = config::Config{{"bucket_type", "ephemeral"}};
    auto strings = config.toStrings();
    ASSERT_EQ(1, strings.size());
    ASSERT_EQ("bucket_type=ephemeral", strings[0]);
}

/**
 * toString() should sort configurations by key.
 */
TEST_F(ConfigTest, ToStringsIsSortedByKey) {
    auto config = config::Config{{"ephemeral_full_policy", "fail_new_data"},
                                 {"bucket_type", "ephemeral"}};
    auto strings = config.toStrings();
    ASSERT_EQ(1, strings.size());
    ASSERT_EQ("bucket_type=ephemeral:ephemeral_full_policy=fail_new_data",
              strings[0]);
}

/**
 * toString() should do a secondary sort by value. This ensures that the
 * order in which we perform operations on the Configs does not matter.
 */
TEST_F(ConfigTest, MultipleValuesAreSorted) {
    auto config = config::Config{
            {"ephemeral_full_policy", {"fail_new_data", "auto_new_data"}}};
    auto strings = config.toStrings();
    ASSERT_EQ(2, strings.size());
    ASSERT_EQ("ephemeral_full_policy=auto_new_data", strings[0]);
    ASSERT_EQ("ephemeral_full_policy=fail_new_data", strings[1]);
}

/**
 * The product of two Config objects is the Cartesian product of all of the
 * configurations of the two objects.
 */
TEST_F(ConfigTest, ProductOperatorWorks) {
    auto config = config::Config{{"bucket_type", "ephemeral"}} *
                  config::Config{{"ephemeral_full_policy",
                                  {"fail_new_data", "auto_new_data"}}};
    auto strings = config.toStrings();
    ASSERT_EQ(2, strings.size());
    ASSERT_EQ("bucket_type=ephemeral:ephemeral_full_policy=auto_new_data",
              strings[0]);
    ASSERT_EQ("bucket_type=ephemeral:ephemeral_full_policy=fail_new_data",
              strings[1]);
}

TEST_F(ConfigTest, JoinOperatorWorks) {
    auto config = config::Config{{"bucket_type", "ephemeral"}} |
                  config::Config{{"bucket_type", "persistent"}};
    auto strings = config.toStrings();
    ASSERT_EQ(2, strings.size());
    ASSERT_EQ("bucket_type=ephemeral", strings[0]);
    ASSERT_EQ("bucket_type=persistent", strings[1]);
}

/**
 * Using | to join configurations with the same values repeatedly should not
 * change the result.
 */
TEST_F(ConfigTest, JoinIsIdempotent) {
    auto config = config::Config{} |
                  config::Config{{"ephemeral_full_policy", "fail_new_data"}} |
                  config::Config{{"ephemeral_full_policy", "fail_new_data"}};
    auto strings = config.toStrings();
    ASSERT_EQ(1, strings.size());
    ASSERT_EQ("ephemeral_full_policy=fail_new_data", strings[0]);
}

/**
 * Calling .add() to a Config object containing multiple configurations
 * should add that parameter to all configurations.
 */
TEST_F(ConfigTest, AddAddsToAllConfigs) {
    auto config = config::Config{{"backend", {"couchstore", "magma"}}}.add(
            "ephemeral_full_policy", {"fail_new_data"});
    auto strings = config.toStrings();
    ASSERT_EQ(2, strings.size());
    ASSERT_EQ("backend=couchstore:ephemeral_full_policy=fail_new_data",
              strings[0]);
    ASSERT_EQ("backend=magma:ephemeral_full_policy=fail_new_data", strings[1]);
}

TEST_F(ConfigTest, AddToEmpty) {
    auto config = config::Config().add("ephemeral_full_policy",
                                       {"fail_new_data", "auto_delete"});
    auto strings = config.toStrings();
    ASSERT_EQ(2, strings.size());
    ASSERT_EQ("ephemeral_full_policy=auto_delete", strings[0]);
    ASSERT_EQ("ephemeral_full_policy=fail_new_data", strings[1]);
}

/**
 * Adding more parameters with the same value does not change the result.
 */
TEST_F(ConfigTest, AddIsIdempotent) {
    auto config = config::Config()
                          .add("ephemeral_full_policy",
                               {"fail_new_data", "auto_delete"})
                          .add("ephemeral_full_policy",
                               {"fail_new_data", "auto_delete"});
    auto strings = config.toStrings();
    ASSERT_EQ(2, strings.size());
    ASSERT_EQ("ephemeral_full_policy=auto_delete", strings[0]);
    ASSERT_EQ("ephemeral_full_policy=fail_new_data", strings[1]);
}

/**
 * Test that the we can generate all basic configurations for
 * ephemeral/couchstore/magma/nexus.
 */
TEST_F(ConfigTest, AllValuesConfigWorks) {
    using namespace config;
    using namespace std::string_literals;

    auto ephemeral = Config{
            {"bucket_type", {"ephemeral"}},
    };

    auto allEphemeralPolicies =
            Config{{"ephemeral_full_policy", {"fail_new_data", "auto_delete"}}};

    auto couchstore =
            Config{{"bucket_type", "persistent"}, {"backend", "couchstore"}};

    auto magma = Config{{"bucket_type", "persistent"}, {"backend", "magma"}};

    auto nexus = Config{
            {"bucket_type", "persistent"},
            {"backend", "nexus"},
            {"nexus_primary_backend", "couchstore"},
            {"nexus_secondary_backend", "magma"},
    };

    auto allEvictionModes =
            Config{{"item_eviction_policy", {"full_eviction", "value_only"}}};

    auto allEphemeral = ephemeral * allEphemeralPolicies;
    auto allPersistent = (couchstore | magma | nexus) * allEvictionModes;
    auto allValues = allEphemeral | allPersistent;

    std::unordered_set<std::string> expectedStrings{
            "bucket_type=ephemeral:"
            "ephemeral_full_policy=auto_delete"s,
            "bucket_type=ephemeral:"
            "ephemeral_full_policy=fail_new_data"s,
            "backend=couchstore:"
            "bucket_type=persistent:"
            "item_eviction_policy=value_only"s,
            "backend=couchstore:"
            "bucket_type=persistent:"
            "item_eviction_policy=full_eviction"s,
            "backend=nexus:"
            "bucket_type=persistent:"
            "item_eviction_policy=value_only:"
            "nexus_primary_backend=couchstore:"
            "nexus_secondary_backend=magma"s,
            "backend=nexus:"
            "bucket_type=persistent:"
            "item_eviction_policy=full_eviction:"
            "nexus_primary_backend=couchstore:"
            "nexus_secondary_backend=magma"s,
            "backend=magma:"
            "bucket_type=persistent:"
            "item_eviction_policy=value_only"s,
            "backend=magma:"
            "bucket_type=persistent:"
            "item_eviction_policy=full_eviction"s};

    auto configStrings = allValues.toStrings();
    ASSERT_EQ(expectedStrings.size(), configStrings.size());
    for (const auto& configString : configStrings) {
        ASSERT_TRUE(expectedStrings.contains(configString))
                << "Unexpected " << configString;
    }
}
