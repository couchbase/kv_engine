/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "generator_event.h"
#include "generator_module.h"
#include "generator_utilities.h"

#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <fstream>

/// @todo Add extra unit tests to verify that we check for the JSON types

class ModuleListParseTest : public ::testing::Test {
protected:
    void SetUp() override {
        /**
         * This is a legal event identifier we can use to test that the parser
         * picks out the correct fields, and that it detects the errors it
         * should
         */
        const auto* input = R"(
{
  "modules": [
    {
      "name":"module1",
      "startid": 0,
      "file": "auditd/generator/tests/module1.json",
      "configurations": ["KV_CV","EE","CE"]
    },
    {
      "name": "module2",
      "startid": 4096,
      "file": "auditd/generator/tests/module2.json",
      "configurations": ["KV_CV","EE","CE"]
    }
  ]
})";
        json = nlohmann::json::parse(input);
        set_enterprise_edition(false);
    }

    nlohmann::json json;
};

TEST_F(ModuleListParseTest, LoadModules) {
    std::list<std::unique_ptr<Module>> modules;
    parse_module_descriptors(json, modules, SOURCE_ROOT, OBJECT_ROOT);
    EXPECT_EQ(2, modules.size());
}

class SingleModuleParseTest : public ::testing::Test {
protected:
    void SetUp() override {
        /**
         * This is a legal event identifier we can use to test that the parser
         * picks out the correct fields, and that it detects the errors it
         * should
         */
        const auto* input = R"(
{
  "name": "module1",
  "startid": 0,
  "file": "auditd/generator/tests/module1.json",
  "generate_macros": true,
  "configurations": ["KV_CV"]
})";
        json = nlohmann::json::parse(input);
        set_enterprise_edition(true);
    }

    nlohmann::json json;
};

/**
 * Verify that the members was set to whatever we had in the input
 */
TEST_F(SingleModuleParseTest, TestCorrectInput) {
    Module module(json);
    module.loadEventDescriptorFile(SOURCE_ROOT);
    EXPECT_EQ("module1", module.name);
    EXPECT_EQ(0, module.start);
    EXPECT_EQ(1, module.configurations.size());
    EXPECT_EQ("KV_CV", module.configurations[0]);
    EXPECT_EQ(3, module.events.size());
}

TEST_F(SingleModuleParseTest, MandatoryFields) {
    // For some stupid reason I'm getting bad_alloc exceptions
    // thrown from g++ with address sanitizer if I'm using:
    // for (const auto& tag :
    //      std::vector<std::string>{{"startid", "file"}}) {
    //
    std::vector<std::string> keywords;
    keywords.emplace_back("startid");
    keywords.emplace_back("file");
    for (const auto& tag : keywords) {
        auto removed = json.at(tag);
        json.erase(tag);
        try {
            Module module(json);
            module.loadEventDescriptorFile(SOURCE_ROOT);
            FAIL() << "Should not be able to construct modules without \""
                   << tag << "\"";
        } catch (const std::exception&) {
        }
        json[tag] = removed;
    }
}

/// Verify that the headerfile was successfully written
TEST_F(SingleModuleParseTest, HeaderfileGeneration) {
    Module module(json);
    module.loadEventDescriptorFile(SOURCE_ROOT);
    auto filename = cb::io::mktemp("myheader");
    std::ofstream output(filename);
    module.createMacros(output);
    output.close();
    auto content = cb::io::loadFile(filename);
    std::filesystem::remove(filename);

    // @todo add a better parser to check that we're not on a comment line etc
    EXPECT_NE(std::string::npos, content.find("MODULE1_AUDIT_EVENT_1 0"));
    EXPECT_NE(std::string::npos, content.find("MODULE1_AUDIT_EVENT_2 1"));
    EXPECT_NE(std::string::npos, content.find("MODULE1_AUDIT_EVENT_3 2"));
}
