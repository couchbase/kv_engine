/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "logger_config.h"
#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>

using namespace cb::logger;

/// Allow an "empty" JSON blob -> all default values
TEST(LoggerConfig, AllDefault) {
    auto config = nlohmann::json::parse("{}").get<Config>();
    EXPECT_TRUE(config.filename.empty());
    EXPECT_EQ(8192, config.buffersize);
    EXPECT_EQ(100_MiB, config.cyclesize);
    EXPECT_FALSE(config.unit_test);
    EXPECT_TRUE(config.console);
    EXPECT_EQ(spdlog::level::level_enum::info, config.log_level);
}

/// Test that we can dump to JSON, and it should be the same
/// when we parse it back
TEST(LoggerConfig, ToFromJson) {
    Config config;
    config.filename = "foo";
    config.buffersize = 2;
    config.cyclesize = 4;
    config.unit_test = true;
    config.console = false;
    config.max_aggregated_size = 100;

    const nlohmann::json json = config;
    EXPECT_EQ(config.filename, json["filename"].get<std::string>());
    EXPECT_EQ(config.buffersize, json["buffersize"].get<size_t>());
    EXPECT_EQ(config.cyclesize, json["cyclesize"].get<size_t>());
    EXPECT_EQ(config.unit_test, json["unit_test"].get<bool>());
    EXPECT_EQ(config.console, json["console"].get<bool>());
    EXPECT_EQ(config.max_aggregated_size,
              json["max_aggregated_size"].get<size_t>());
    const auto parsed = json.get<Config>();
    EXPECT_EQ(config, parsed);
}
