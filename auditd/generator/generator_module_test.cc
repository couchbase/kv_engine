/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include <fstream>

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
        "enterprise": false
      }
    },
    {
      "module2": {
        "startid": 4096,
        "file": "auditd/generator/tests/module2.json",
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
    Module module(json, SOURCE_ROOT);
    EXPECT_EQ("module1", module.name);
    EXPECT_EQ(0, module.start);
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
            Module module(json, SOURCE_ROOT);
            FAIL() << "Should not be able to construct modules without \""
                   << tag << "\"";
        } catch (const nlohmann::json::exception&) {
        }
        json["module1"][tag] = removed;
    }
}

/// Verify that we default to false if no enterprise attribute is set
TEST_F(SingleModuleParseTest, NoEnterprise) {
    json["module1"].erase("enterprise");
    Module module(json, SOURCE_ROOT);
    EXPECT_FALSE(module.enterprise);
}

/// Verify that we can change the enterprise attribute to true
TEST_F(SingleModuleParseTest, Enterprise) {
    json["module1"]["enterprise"] = true;
    Module module(json, SOURCE_ROOT);
    EXPECT_TRUE(module.enterprise);
}

/// Verify that we can change the enterprise attribute to true and that
/// the entry is skipped in CE build
TEST_F(SingleModuleParseTest, CeSkipEnterpriseEvents) {
    set_enterprise_edition(false);
    json["module1"]["enterprise"] = true;
    Module module(json, SOURCE_ROOT);
    EXPECT_TRUE(module.enterprise);
    EXPECT_TRUE(module.events.empty());
}

/// Verify that the headerfile was successfully written
TEST_F(SingleModuleParseTest, HeaderfileGeneration) {
    Module module(json, SOURCE_ROOT);
    auto filename = cb::io::mktemp("myheader");
    std::ofstream output(filename);
    module.createHeaderFile(output);
    output.close();
    auto content = cb::io::loadFile(filename);
    cb::io::rmrf(filename);

    // @todo add a better parser to check that we're not on a comment line etc
    EXPECT_NE(std::string::npos, content.find("MODULE1_AUDIT_EVENT_1 0"));
    EXPECT_NE(std::string::npos, content.find("MODULE1_AUDIT_EVENT_2 1"));
    EXPECT_NE(std::string::npos, content.find("MODULE1_AUDIT_EVENT_3 2"));
}
