/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "filedescriptor_distribution.h"
#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>

using namespace cb::environment::filedescriptor;

/// Test that we return the absolute minimum distribution when we have
/// fewer file descriptors than the minimum pool sizes
TEST(Distribution, Limit_2000) {
    const auto expected = R"({
        "filedescriptor_limit": 2000,
        "reserved_core": 500,
        "reserved_ep_engine": 500,
        "reserved_magma": 500,
        "reserved_system_connections": 500,
        "reserved_user_connections": 500
    })"_json;
    auto distribution = getDistribution(2000);
    nlohmann::json json = distribution;
    EXPECT_EQ(expected, json);
}

TEST(Distribution, Limit_9999) {
    const auto expected = R"({
        "filedescriptor_limit": 9999,
        "reserved_core": 1357,
        "reserved_ep_engine": 2214,
        "reserved_magma": 2000,
        "reserved_system_connections": 1357,
        "reserved_user_connections": 3071
    })"_json;
    auto distribution = getDistribution(9999);
    nlohmann::json json = distribution;
    EXPECT_EQ(expected, json);
}

TEST(Distribution, Limit_10000) {
    const auto expected = R"({
        "filedescriptor_limit": 10000,
        "reserved_core": 1358,
        "reserved_ep_engine": 2214,
        "reserved_magma": 2000,
        "reserved_system_connections": 1357,
        "reserved_user_connections": 3071
    })"_json;
    auto distribution = getDistribution(10000);
    nlohmann::json json = distribution;
    EXPECT_EQ(expected, json);
}

TEST(Distribution, Limit_19999) {
    const auto expected = R"({
        "filedescriptor_limit": 19999,
        "reserved_core": 2000,
        "reserved_ep_engine": 5335,
        "reserved_magma": 2000,
        "reserved_system_connections": 2916,
        "reserved_user_connections": 7748
    })"_json;
    auto distribution = getDistribution(19999);
    nlohmann::json json = distribution;
    EXPECT_EQ(expected, json);
}

TEST(Distribution, Limit_20000) {
    const auto expected = R"({
        "filedescriptor_limit": 20000,
        "reserved_core": 2000,
        "reserved_ep_engine": 5336,
        "reserved_magma": 2000,
        "reserved_system_connections": 2916,
        "reserved_user_connections": 7748
    })"_json;
    auto distribution = getDistribution(20000);
    nlohmann::json json = distribution;
    EXPECT_EQ(expected, json);
}

TEST(Distribution, Limit_29999) {
    const auto expected = R"({
        "filedescriptor_limit": 29999,
        "reserved_core": 2000,
        "reserved_ep_engine": 8667,
        "reserved_magma": 2000,
        "reserved_system_connections": 4583,
        "reserved_user_connections": 12749
    })"_json;
    auto distribution = getDistribution(29999);
    nlohmann::json json = distribution;
    EXPECT_EQ(expected, json);
}

TEST(Distribution, Limit_30000) {
    const auto expected = R"({
        "filedescriptor_limit": 30000,
        "reserved_core": 2000,
        "reserved_ep_engine": 8668,
        "reserved_magma": 2000,
        "reserved_system_connections": 4583,
        "reserved_user_connections": 12749
    })"_json;
    auto distribution = getDistribution(30000);
    nlohmann::json json = distribution;
    EXPECT_EQ(expected, json);
}
TEST(Distribution, Limit_39999) {
    const auto expected = R"({
        "filedescriptor_limit": 39999,
        "reserved_core": 2000,
        "reserved_ep_engine": 12502,
        "reserved_magma": 2000,
        "reserved_system_connections": 5000,
        "reserved_user_connections": 18497
    })"_json;
    auto distribution = getDistribution(39999);
    nlohmann::json json = distribution;
    EXPECT_EQ(expected, json);
}

TEST(Distribution, Limit_40000) {
    const auto expected = R"({
        "filedescriptor_limit": 40000,
        "reserved_core": 2000,
        "reserved_ep_engine": 12500,
        "reserved_magma": 2000,
        "reserved_system_connections": 5000,
        "reserved_user_connections": 18500
    })"_json;
    auto distribution = getDistribution(40000);
    nlohmann::json json = distribution;
    EXPECT_EQ(expected, json);
}
TEST(Distribution, Limit_49999) {
    const auto expected = R"({
        "filedescriptor_limit": 49999,
        "reserved_core": 2000,
        "reserved_ep_engine": 16502,
        "reserved_magma": 2000,
        "reserved_system_connections": 5000,
        "reserved_user_connections": 24497
    })"_json;
    auto distribution = getDistribution(49999);
    nlohmann::json json = distribution;
    EXPECT_EQ(expected, json);
}

TEST(Distribution, Limit_50000) {
    const auto expected = R"({
        "filedescriptor_limit": 50000,
        "reserved_core": 2000,
        "reserved_ep_engine": 16500,
        "reserved_magma": 2000,
        "reserved_system_connections": 5000,
        "reserved_user_connections": 24500
    })"_json;
    auto distribution = getDistribution(50000);
    nlohmann::json json = distribution;
    EXPECT_EQ(expected, json);
}

TEST(Distribution, Limit_59999) {
    const auto expected = R"({
        "filedescriptor_limit": 59999,
        "reserved_core": 2000,
        "reserved_ep_engine": 20502,
        "reserved_magma": 2000,
        "reserved_system_connections": 5000,
        "reserved_user_connections": 30497
    })"_json;
    auto distribution = getDistribution(59999);
    nlohmann::json json = distribution;
    EXPECT_EQ(expected, json);
}

TEST(Distribution, Limit_60000) {
    const auto expected = R"({
        "filedescriptor_limit": 60000,
        "reserved_core": 2000,
        "reserved_ep_engine": 20500,
        "reserved_magma": 2000,
        "reserved_system_connections": 5000,
        "reserved_user_connections": 30500
    })"_json;
    auto distribution = getDistribution(60000);
    nlohmann::json json = distribution;
    EXPECT_EQ(expected, json);
}

TEST(Distribution, Limit_69999) {
    const auto expected = R"({
        "filedescriptor_limit": 69999,
        "reserved_core": 2000,
        "reserved_ep_engine": 24502,
        "reserved_magma": 2000,
        "reserved_system_connections": 5000,
        "reserved_user_connections": 36497
    })"_json;
    auto distribution = getDistribution(69999);
    nlohmann::json json = distribution;
    EXPECT_EQ(expected, json);
}

TEST(Distribution, Limit_70000) {
    const auto expected = R"({
        "filedescriptor_limit": 70000,
        "reserved_core": 2000,
        "reserved_ep_engine": 24500,
        "reserved_magma": 2000,
        "reserved_system_connections": 5000,
        "reserved_user_connections": 36500
    })"_json;
    auto distribution = getDistribution(70000);
    nlohmann::json json = distribution;
    EXPECT_EQ(expected, json);
}

TEST(Distribution, Limit_79999) {
    const auto expected = R"({
        "filedescriptor_limit": 79999,
        "reserved_core": 2000,
        "reserved_ep_engine": 28502,
        "reserved_magma": 2000,
        "reserved_system_connections": 5000,
        "reserved_user_connections": 42497
    })"_json;
    auto distribution = getDistribution(79999);
    nlohmann::json json = distribution;
    EXPECT_EQ(expected, json);
}

TEST(Distribution, Limit_80000) {
    const auto expected = R"({
        "filedescriptor_limit": 80000,
        "reserved_core": 2000,
        "reserved_ep_engine": 28500,
        "reserved_magma": 2000,
        "reserved_system_connections": 5000,
        "reserved_user_connections": 42500
    })"_json;
    auto distribution = getDistribution(80000);
    nlohmann::json json = distribution;
    EXPECT_EQ(expected, json);
}

TEST(Distribution, Limit_89999) {
    const auto expected = R"({
        "filedescriptor_limit": 89999,
        "reserved_core": 2000,
        "reserved_ep_engine": 30000,
        "reserved_magma": 2000,
        "reserved_system_connections": 5000,
        "reserved_user_connections": 50999
    })"_json;
    auto distribution = getDistribution(89999);
    nlohmann::json json = distribution;
    EXPECT_EQ(expected, json);
}

TEST(Distribution, Limit_90000) {
    const auto expected = R"({
        "filedescriptor_limit": 90000,
        "reserved_core": 2000,
        "reserved_ep_engine": 30000,
        "reserved_magma": 2000,
        "reserved_system_connections": 5000,
        "reserved_user_connections": 51000
    })"_json;
    auto distribution = getDistribution(90000);
    nlohmann::json json = distribution;
    EXPECT_EQ(expected, json);
}

TEST(Distribution, Limit_99999) {
    const auto expected = R"({
        "filedescriptor_limit": 99999,
        "reserved_core": 2000,
        "reserved_ep_engine": 30000,
        "reserved_magma": 2999,
        "reserved_system_connections": 5000,
        "reserved_user_connections": 60000
    })"_json;
    auto distribution = getDistribution(99999);
    nlohmann::json json = distribution;
    EXPECT_EQ(expected, json);
}

TEST(Distribution, Limit_100000) {
    const auto expected = R"({
        "filedescriptor_limit": 100000,
        "reserved_core": 2000,
        "reserved_ep_engine": 30000,
        "reserved_magma": 3000,
        "reserved_system_connections": 5000,
        "reserved_user_connections": 60000
    })"_json;
    auto distribution = getDistribution(100000);
    nlohmann::json json = distribution;
    EXPECT_EQ(expected, json);
}

TEST(Distribution, Limit_199999) {
    const auto expected = R"({
        "filedescriptor_limit": 199999,
        "reserved_core": 2000,
        "reserved_ep_engine": 30000,
        "reserved_magma": 102999,
        "reserved_system_connections": 5000,
        "reserved_user_connections": 60000
    })"_json;
    auto distribution = getDistribution(199999);
    nlohmann::json json = distribution;
    EXPECT_EQ(expected, json);
}

TEST(Distribution, DISABLED_GenerateDistributionTable) {
    std::cout << "<table>" << std::endl
              << "<tr><th>FD "
                 "Limit</th><th>Core</th><th>EpEngine</th><th>Magma</"
                 "th><th>System</th><th>User</th></tr>"
              << std::endl;
    for (int limit = 100; limit <= 9999; limit += 1000) {
        auto distribution = getDistribution(limit);
        std::cout << "<tr><td>" << distribution.filedescriptorLimit
                  << "</td><td>" << distribution.reservedCore << "</td><td>"
                  << distribution.reservedEpEngine << "</td><td>"
                  << distribution.reservedMagma << "</td><td>"
                  << distribution.reservedSystemConnections << "</td><td>"
                  << distribution.reservedUserConnections << "</td></tr>"
                  << std::endl;
    }
    for (int limit = 9999; limit <= 110000; limit += 10000) {
        auto distribution = getDistribution(limit);
        std::cout << "<tr><td>" << distribution.filedescriptorLimit
                  << "</td><td>" << distribution.reservedCore << "</td><td>"
                  << distribution.reservedEpEngine << "</td><td>"
                  << distribution.reservedMagma << "</td><td>"
                  << distribution.reservedSystemConnections << "</td><td>"
                  << distribution.reservedUserConnections << "</td></tr>"
                  << std::endl;
        distribution = getDistribution(limit + 1);
        std::cout << "<tr><td>" << distribution.filedescriptorLimit
                  << "</td><td>" << distribution.reservedCore << "</td><td>"
                  << distribution.reservedEpEngine << "</td><td>"
                  << distribution.reservedMagma << "</td><td>"
                  << distribution.reservedSystemConnections << "</td><td>"
                  << distribution.reservedUserConnections << "</td></tr>"
                  << std::endl;
    }
    std::cout << "</table>" << std::endl;
}
