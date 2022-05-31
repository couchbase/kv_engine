/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <daemon/error_map_manager.h>
#include <folly/portability/GTest.h>
#include <mcbp/protocol/status.h>
#include <nlohmann/json.hpp>
#include <platform/string_hex.h>
#include <array>
#include <filesystem>
#include <iostream>

class ErrorMapManagerTest : public ::testing::Test {
public:
    static void SetUpTestCase() {
        auto dir = std::filesystem::path(SOURCE_ROOT) / "etc" / "couchbase" /
                   "kv" / "error_maps";
        try {
            ErrorMapManager::initialize(dir);
        } catch (const std::exception& exception) {
            std::cerr << exception.what() << std::endl;
            std::exit(EXIT_FAILURE);
        }
    }

    static void TearDownTestCase() {
        ErrorMapManager::shutdown();
    }

    void ensureAllStatusCodeExists(const nlohmann::json& map) {
        auto error = map["errors"];
        for (uint32_t ii = 0; ii < uint32_t(cb::mcbp::Status::COUNT); ++ii) {
            const auto status = cb::mcbp::Status(ii);
            if (cb::mcbp::is_known(status)) {
                std::array<char, 10> buffer;
                snprintf(buffer.data(), buffer.size(), "%x", ii);
                const auto iter = error.find(buffer.data());
                EXPECT_FALSE(iter == error.end())
                        << "Missing entry for Status code: "
                        << to_string(status, true) << " "
                        << cb::to_hex(uint16_t(ii));
            }
        }
    }
};

TEST_F(ErrorMapManagerTest, v0) {
    // V0 doesn't exist and is empty
    EXPECT_TRUE(ErrorMapManager::instance().getErrorMap(0).empty());
}

TEST_F(ErrorMapManagerTest, v1) {
    auto v1 = ErrorMapManager::instance().getErrorMap(1);
    EXPECT_FALSE(v1.empty());
    auto json = nlohmann::json::parse(v1);
    EXPECT_EQ(1, json["version"]);
    EXPECT_LE(5, json["revision"]);
    // We should not have a single reference to "rate-limit" in the v1
    // map. It is part of v2, so it should have been stripped off
    EXPECT_EQ(std::string_view::npos, v1.find("rate-limit"));
    ensureAllStatusCodeExists(json);
}

TEST_F(ErrorMapManagerTest, v2) {
    auto v2 = ErrorMapManager::instance().getErrorMap(2);
    EXPECT_FALSE(v2.empty());
    auto json = nlohmann::json::parse(v2);
    EXPECT_EQ(2, json["version"]);
    EXPECT_LE(1, json["revision"]);

    // We should have a reference to "rate-limit"
    EXPECT_NE(std::string_view::npos, v2.find("rate-limit"));
    ensureAllStatusCodeExists(json);
}

TEST_F(ErrorMapManagerTest, v3) {
    // V3 doesn't exist yet, so we should get version 2
    auto v3 = ErrorMapManager::instance().getErrorMap(3);
    EXPECT_FALSE(v3.empty());
    auto json = nlohmann::json::parse(v3);
    EXPECT_EQ(2, json["version"]);
    EXPECT_LE(1, json["revision"]);
}
