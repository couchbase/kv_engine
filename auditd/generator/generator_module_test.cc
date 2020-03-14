/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include "generator_event.h"
#include "generator_module.h"
#include "generator_utilities.h"

#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>

/// @todo Add extra unit tests to verify that we check for the JSON types

class ModuleListParseTest : public ::testing::Test {
public:
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
      "module1": {
        "startid": 0,
        "file": "auditd/generator/tests/module1.json",
        "header": "auditd/generator/module1.h",
        "enterprise": false
      }
    },
    {
      "module2": {
        "startid": 4096,
        "file": "auditd/generator/tests/module2.json",
        "header": "auditd/generator/module2.h",
        "enterprise": false
      }
    },
    {
      "module3": {
        "startid": 8192,
        "file": "auditd/generator/no-such-file.json",
        "enterprise": true
      }
    }
  ]
})";
        json = nlohmann::json::parse(input);
        set_enterprise_edition(false);
    }

protected:
    nlohmann::json json;
};

TEST_F(ModuleListParseTest, LoadModules) {
    std::list<std::unique_ptr<Module>> modules;
    parse_module_descriptors(json, modules, SOURCE_ROOT, OBJECT_ROOT);

    // The file contains 3 entries, but the third entry is EE (with a
    // nonexisting file)
    EXPECT_EQ(2, modules.size());
}

TEST_F(ModuleListParseTest, LoadModulesNonexistingFile) {
    try {
        std::list<std::unique_ptr<Module>> modules;
        set_enterprise_edition(true);
        parse_module_descriptors(json, modules, SOURCE_ROOT, OBJECT_ROOT);
        FAIL() << "did not detect non-existing file";
    } catch (const std::system_error& e) {
        auto error = std::errc(e.code().value());
        EXPECT_TRUE(error == std::errc::no_such_file_or_directory ||
                    error == std::errc::operation_not_permitted);
    }
}

class SingleModuleParseTest : public ::testing::Test {
public:
protected:
    void SetUp() override {
        /**
         * This is a legal event identifier we can use to test that the parser
         * picks out the correct fields, and that it detects the errors it
         * should
         */
        const auto* input = R"(
{
  "module1": {
    "startid": 0,
    "file": "auditd/generator/tests/module1.json",
    "header": "auditd/generator/module_test.h",
    "enterprise": false
  }
})";
        json = nlohmann::json::parse(input);
        set_enterprise_edition(true);
    }

protected:
    nlohmann::json json;
};

/**
 * Verify that the members was set to whatever we had in the input
 */
TEST_F(SingleModuleParseTest, TestCorrectInput) {
    Module module(json, SOURCE_ROOT, OBJECT_ROOT);
    EXPECT_EQ("module1", module.name);
    EXPECT_EQ(0, module.start);
    auto expected = cb::io::sanitizePath(
            SOURCE_ROOT "/auditd/generator/tests/module1.json");
    EXPECT_EQ(expected, module.file);
    expected =
            cb::io::sanitizePath(OBJECT_ROOT "/auditd/generator/module_test.h");
    EXPECT_EQ(expected, module.header);
    EXPECT_FALSE(module.enterprise);
    EXPECT_EQ(3, module.events.size());
}

TEST_F(SingleModuleParseTest, MandatoryFields) {
    // For some stupid reason I'm getting bad_alloc exceptions
    // thrown from g++ with address sanitizer if I'm using:
    // for (const auto& tag :
    //      std::vector<std::string>{{"startid", "file"}}) {
    //
    std::vector<std::string> keywords;
    keywords.emplace_back(std::string{"startid"});
    keywords.emplace_back(std::string{"file"});
    for (const auto& tag : keywords) {
        auto removed = json["module1"].at(tag);
        json["module1"].erase(tag);
        try {
            Module module(json, SOURCE_ROOT, OBJECT_ROOT);
            FAIL() << "Should not be able to construct modules without \""
                   << tag << "\"";
        } catch (const nlohmann::json::exception&) {
        }
        json["module1"][tag] = removed;
    }
}

/// Verify that we can handle no header entry
TEST_F(SingleModuleParseTest, NoHeaderEntry) {
    json["module1"].erase("header");
    Module module(json, SOURCE_ROOT, OBJECT_ROOT);
    EXPECT_TRUE(module.header.empty());
}

/// Verify that we default to false if no enterprise attribute is set
TEST_F(SingleModuleParseTest, NoEnterprise) {
    json["module1"].erase("enterprise");
    Module module(json, SOURCE_ROOT, OBJECT_ROOT);
    EXPECT_FALSE(module.enterprise);
}

/// Verify that we can change the enterprise attribute to true
TEST_F(SingleModuleParseTest, Enterprise) {
    json["module1"]["enterprise"] = true;
    Module module(json, SOURCE_ROOT, OBJECT_ROOT);
    EXPECT_TRUE(module.enterprise);
}

/// Verify that we can change the enterprise attribute to true and that
/// the entry is skipped in CE build
TEST_F(SingleModuleParseTest, CeSkipEnterpriseEvents) {
    set_enterprise_edition(false);
    json["module1"]["enterprise"] = true;
    Module module(json, SOURCE_ROOT, OBJECT_ROOT);
    EXPECT_TRUE(module.enterprise);
    EXPECT_TRUE(module.events.empty());
}

/// Verify that the headerfile was successfully written
TEST_F(SingleModuleParseTest, HeaderfileGeneration) {
    Module module(json, SOURCE_ROOT, OBJECT_ROOT);
    EXPECT_FALSE(module.header.empty());
    if (cb::io::isFile(module.header)) {
        cb::io::rmrf(module.header);
    }
    module.createHeaderFile();
    EXPECT_TRUE(cb::io::isFile(module.header));
    auto content = cb::io::loadFile(module.header);

    // @todo add a better parser to check that we're not on a comment line etc
    EXPECT_NE(std::string::npos, content.find("MODULE1_AUDIT_EVENT_1 0"));
    EXPECT_NE(std::string::npos, content.find("MODULE1_AUDIT_EVENT_2 1"));
    EXPECT_NE(std::string::npos, content.find("MODULE1_AUDIT_EVENT_3 2"));
}
