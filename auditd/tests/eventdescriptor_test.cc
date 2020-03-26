/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include "eventdescriptor.h"

#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>
#include <stdexcept>

class EventDescriptorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // We normally expect this to be an unsigned integer so we need to
        // explicitly pass a unsigned int to json[].
        size_t id = 1;
        json["id"] = id;
        json["name"] = "name";
        json["description"] = "description";
        json["sync"] = false;
        json["enabled"] = true;
    }

    nlohmann::json json;
};

TEST_F(EventDescriptorTest, ParseOk) {
    EXPECT_NO_THROW(EventDescriptor ptr(json));
}

TEST_F(EventDescriptorTest, VerifyGetters) {
    EventDescriptor ptr(json);
    EXPECT_EQ(1, ptr.getId());
    EXPECT_EQ("name", ptr.getName());
    EXPECT_EQ("description", ptr.getDescription());
    EXPECT_FALSE(ptr.isSync());
    EXPECT_TRUE(ptr.isEnabled());
}

TEST_F(EventDescriptorTest, UnknownTag) {
    json["foo"] = "foo";
    EXPECT_THROW(EventDescriptor ptr(json), std::invalid_argument);
}

TEST_F(EventDescriptorTest, MissingId) {
    json.erase(json.find("id"));
    EXPECT_THROW(EventDescriptor ptr(json), nlohmann::json::exception);
}

TEST_F(EventDescriptorTest, InvalidId) {
    json["id"] = "foo";
    EXPECT_THROW(EventDescriptor ptr(json), nlohmann::json::exception);
}

TEST_F(EventDescriptorTest, MissingName) {
    json.erase(json.find("name"));
    EXPECT_THROW(EventDescriptor ptr(json), nlohmann::json::exception);
}

TEST_F(EventDescriptorTest, InvalidName) {
    json["name"] = 5;
    EXPECT_THROW(EventDescriptor ptr(json), nlohmann::json::exception);
}

TEST_F(EventDescriptorTest, MissingDescription) {
    json.erase(json.find("description"));
    EXPECT_THROW(EventDescriptor ptr(json), nlohmann::json::exception);
}

TEST_F(EventDescriptorTest, InvalidDescription) {
    json["description"] = 5;
    EXPECT_THROW(EventDescriptor ptr(json), nlohmann::json::exception);
}

TEST_F(EventDescriptorTest, MissingSync) {
    json.erase(json.find("sync"));
    EXPECT_THROW(EventDescriptor ptr(json), nlohmann::json::exception);
}

TEST_F(EventDescriptorTest, InvalidSync) {
    json["sync"] = 5;
    EXPECT_THROW(EventDescriptor ptr(json), nlohmann::json::exception);
}

TEST_F(EventDescriptorTest, SyncTrue) {
    json["sync"] = true;
    EventDescriptor ptr(json);
    EXPECT_TRUE(ptr.isSync());
}

TEST_F(EventDescriptorTest, SyncFalse) {
    json["sync"] = false;
    EventDescriptor ptr(json);
    EXPECT_FALSE(ptr.isSync());
}

TEST_F(EventDescriptorTest, SyncSetter) {
    EventDescriptor ptr(json);
    ptr.setSync(true);
    EXPECT_TRUE(ptr.isSync());
    ptr.setSync(false);
    EXPECT_FALSE(ptr.isSync());
}

TEST_F(EventDescriptorTest, MissingEnabled) {
    json.erase(json.find("enabled"));
    EXPECT_THROW(EventDescriptor ptr(json), nlohmann::json::exception);
}

TEST_F(EventDescriptorTest, InvalidEnabled) {
    json["enabled"] = 5;
    EXPECT_THROW(EventDescriptor ptr(json), nlohmann::json::exception);
}

TEST_F(EventDescriptorTest, EnabledTrue) {
    json["enabled"] = true;
    EventDescriptor ptr(json);
    EXPECT_TRUE(ptr.isEnabled());
}

TEST_F(EventDescriptorTest, EnabledFalse) {
    json["enabled"] = false;
    EventDescriptor ptr(json);
    EXPECT_FALSE(ptr.isEnabled());
}

TEST_F(EventDescriptorTest, EnabledSetter) {
    EventDescriptor ptr(json);
    ptr.setEnabled(true);
    EXPECT_TRUE(ptr.isEnabled());
    ptr.setEnabled(false);
    EXPECT_FALSE(ptr.isEnabled());
}

TEST_F(EventDescriptorTest, HandleMandatoryFieldsObject) {
    json["mandatory_fields"] = nlohmann::json::object();
    EXPECT_NO_THROW(EventDescriptor ptr(json));
}

TEST_F(EventDescriptorTest, HandleMandatoryFieldsArray) {
    json["mandatory_fields"] = nlohmann::json::array();
    EXPECT_NO_THROW(EventDescriptor ptr(json));
}

TEST_F(EventDescriptorTest, HandleMandatoryFieldsInvalidType) {
    json["mandatory_fields"] = true;
    EXPECT_THROW(EventDescriptor ptr(json), std::invalid_argument);

    json["mandatory_fields"] = false;
    EXPECT_THROW(EventDescriptor ptr(json), std::invalid_argument);

    json["mandatory_fields"] = 5;
    EXPECT_THROW(EventDescriptor ptr(json), std::invalid_argument);

    json["mandatory_fields"] = "5";
    EXPECT_THROW(EventDescriptor ptr(json), std::invalid_argument);
}

TEST_F(EventDescriptorTest, HandleOptionalFieldsObject) {
    json["optional_fields"] = nlohmann::json::object();
    EXPECT_NO_THROW(EventDescriptor ptr(json));
}

TEST_F(EventDescriptorTest, HandleOptionalFieldsArray) {
    json["optional_fields"] = nlohmann::json::array();
    EXPECT_NO_THROW(EventDescriptor ptr(json));
}

TEST_F(EventDescriptorTest, HandleOptionalFieldsInvalidType) {
    json["optional_fields"] = true;
    EXPECT_THROW(EventDescriptor ptr(json), std::invalid_argument);

    json["optional_fields"] = false;
    EXPECT_THROW(EventDescriptor ptr(json), std::invalid_argument);

    json["optional_fields"] = 5;
    EXPECT_THROW(EventDescriptor ptr(json), std::invalid_argument);

    json["optional_fields"] = "5";
    EXPECT_THROW(EventDescriptor ptr(json), std::invalid_argument);
}

/// filtering_permitted is an optional parameter.  If it is not defined then
/// enquiring whether filtering is permitted should return false.
TEST_F(EventDescriptorTest, filterPermittedNotExist) {
    // It is not defined by default
    EventDescriptor ptr(json);
    EXPECT_FALSE(ptr.isFilteringPermitted());
}

TEST_F(EventDescriptorTest, filterPermittedFalse) {
    json["filtering_permitted"] = false;
    EventDescriptor ptr(json);
    EXPECT_FALSE(ptr.isFilteringPermitted());
}

TEST_F(EventDescriptorTest, filterPermittedTrue) {
    json["filtering_permitted"] = true;
    EventDescriptor ptr(json);
    EXPECT_TRUE(ptr.isFilteringPermitted());
}
