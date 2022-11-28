/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "breakpad_settings.h"
#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>
#include <filesystem>
#include <system_error>

using namespace cb::breakpad;

/// Allow an "empty" JSON blob -> disabled
TEST(BreakpadSettings, AllDefault) {
    auto settings = nlohmann::json::parse("{}").get<Settings>();
    EXPECT_FALSE(settings.enabled);
    EXPECT_TRUE(settings.minidump_dir.empty());
    EXPECT_EQ(Content::Default, settings.content);
}

TEST(BreakpadSettings, ContentDefaultValue) {
    auto settings = nlohmann::json{{"content", "default"}}.get<Settings>();
    EXPECT_FALSE(settings.enabled);
    EXPECT_TRUE(settings.minidump_dir.empty());
    EXPECT_EQ(Content::Default, settings.content);
}

TEST(BreakpadSettings, ContentMustBeDefault) {
    bool failed = false;
    try {
        nlohmann::json{{"content", "stack"}}.get<Settings>();
    } catch (const std::exception& e) {
        EXPECT_STREQ(R"("breakpad:content" settings must be set to "default")",
                     e.what());
        failed = true;
    }
    EXPECT_TRUE(failed) << "Failed to detect illegal value for content";
}

TEST(BreakpadSettings, ValidSpecificationSyntax) {
    Settings s;
    s.enabled = true;
    s.minidump_dir = std::filesystem::path("/").generic_string();
    nlohmann::json json = s;
    auto settings = json.get<Settings>();
    EXPECT_TRUE(settings.enabled);
    EXPECT_EQ(s.minidump_dir, settings.minidump_dir);
    EXPECT_EQ(s.content, settings.content);
}

TEST(BreakpadSettings, ValidateDisabled) {
    Settings s;
    s.enabled = false;
    s.minidump_dir = "";
    // As long as it is disabled minidump_dir may be empty
    s.validate();

    // or a non-existing file
    s.minidump_dir = "/foo";
    const auto path = std::filesystem::path(s.minidump_dir);
    EXPECT_FALSE(std::filesystem::is_directory(path));
    s.validate();
}

TEST(BreakpadSettings, ValidateEnabled) {
    Settings settings;
    settings.enabled = true;

    // Directory must be set
    bool failed = false;
    try {
        settings.validate();
    } catch (const std::exception& e) {
        failed = true;
        EXPECT_STREQ(R"("breakpad:minidump_dir" must be specified)", e.what());
    }
    EXPECT_TRUE(failed) << "Validate should detect missing minidump_dir";

    // And it must exist
    settings.minidump_dir =
            std::filesystem::path("/DirectoryShouldNotExists").generic_string();
    failed = false;
    try {
        settings.validate();
    } catch (const std::system_error& e) {
        failed = true;
        EXPECT_EQ(std::make_error_code(std::errc::no_such_file_or_directory),
                  e.code());
    }
    EXPECT_TRUE(failed) << "Validate should detect invalid directory";
}

TEST(BreakpadSettings, to_json) {
    Settings settings;
    settings.enabled = true;
    settings.minidump_dir = "/var/crash";
    const nlohmann::json json = settings;
    EXPECT_TRUE(json["enabled"].get<bool>());
    EXPECT_EQ(settings.minidump_dir, json["minidump_dir"].get<std::string>());
    EXPECT_EQ("default", json["content"].get<std::string>());
}
