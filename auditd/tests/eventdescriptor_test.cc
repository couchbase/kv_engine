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

#include <gtest/gtest.h>
#include <cJSON_utils.h>
#include <stdexcept>
#include "eventdescriptor.h"

class EventDescriptorTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        json.reset(cJSON_CreateObject());
        ASSERT_NE(nullptr, json.get());
        cJSON* root = json.get();
        cJSON_AddNumberToObject(root, "id", 1);
        cJSON_AddStringToObject(root, "name", "name");
        cJSON_AddStringToObject(root, "description", "description");
        cJSON_AddFalseToObject(root, "sync");
        cJSON_AddTrueToObject(root, "enabled");
    }

    unique_cJSON_ptr json;
};

TEST_F(EventDescriptorTest, ParseOk) {
    EXPECT_NO_THROW(EventDescriptor(json.get()));
}

TEST_F(EventDescriptorTest, VerifyGetters) {
    EventDescriptor ptr(json.get());
    EXPECT_EQ(1, ptr.getId());
    EXPECT_EQ("name", ptr.getName());
    EXPECT_EQ("description", ptr.getDescription());
    EXPECT_FALSE(ptr.isSync());
    EXPECT_TRUE(ptr.isEnabled());
}

TEST_F(EventDescriptorTest, UnknownTag) {
    cJSON_AddStringToObject(json.get(), "foo", "foo");
    EXPECT_THROW(EventDescriptor ptr(json.get()),
                 std::logic_error);
}

TEST_F(EventDescriptorTest, MissingId) {
    cJSON_DeleteItemFromObject(json.get(), "id");
    EXPECT_THROW(EventDescriptor ptr(json.get()),
                 std::logic_error);
}

TEST_F(EventDescriptorTest, InvalidId) {
    cJSON_DeleteItemFromObject(json.get(), "id");
    cJSON_AddStringToObject(json.get(), "id", "foo");
    EXPECT_THROW(EventDescriptor ptr(json.get()),
                 std::logic_error);
}

TEST_F(EventDescriptorTest, MissingName) {
    cJSON_DeleteItemFromObject(json.get(), "name");
    EXPECT_THROW(EventDescriptor ptr(json.get()),
                 std::logic_error);
}

TEST_F(EventDescriptorTest, InvalidName) {
    cJSON_DeleteItemFromObject(json.get(), "name");
    cJSON_AddNumberToObject(json.get(), "name", 5);
    EXPECT_THROW(EventDescriptor ptr(json.get()),
                 std::logic_error);
}

TEST_F(EventDescriptorTest, MissingDescription) {
    cJSON_DeleteItemFromObject(json.get(), "description");
    EXPECT_THROW(EventDescriptor ptr(json.get()),
                 std::logic_error);
}

TEST_F(EventDescriptorTest, InvalidDescription) {
    cJSON_DeleteItemFromObject(json.get(), "description");
    cJSON_AddNumberToObject(json.get(), "description", 5);
    EXPECT_THROW(EventDescriptor ptr(json.get()),
                 std::logic_error);
}

TEST_F(EventDescriptorTest, MissingSync) {
    cJSON_DeleteItemFromObject(json.get(), "sync");
    EXPECT_THROW(EventDescriptor ptr(json.get()),
                 std::logic_error);
}

TEST_F(EventDescriptorTest, InvalidSync) {
    cJSON_DeleteItemFromObject(json.get(), "sync");
    cJSON_AddNumberToObject(json.get(), "sync", 5);
    EXPECT_THROW(EventDescriptor ptr(json.get()),
                 std::logic_error);
}

TEST_F(EventDescriptorTest, SyncTrue) {
    cJSON_DeleteItemFromObject(json.get(), "sync");
    cJSON_AddTrueToObject(json.get(), "sync");
    EventDescriptor ptr(json.get());
    EXPECT_TRUE(ptr.isSync());
}

TEST_F(EventDescriptorTest, SyncFalse) {
    cJSON_DeleteItemFromObject(json.get(), "sync");
    cJSON_AddFalseToObject(json.get(), "sync");
    EventDescriptor ptr(json.get());
    EXPECT_FALSE(ptr.isSync());
}

TEST_F(EventDescriptorTest, SyncSetter) {
    EventDescriptor ptr(json.get());
    ptr.setSync(true);
    EXPECT_TRUE(ptr.isSync());
    ptr.setSync(false);
    EXPECT_FALSE(ptr.isSync());
}

TEST_F(EventDescriptorTest, MissingEnabled) {
    cJSON_DeleteItemFromObject(json.get(), "enabled");
    EXPECT_THROW(EventDescriptor ptr(json.get()),
                 std::logic_error);
}

TEST_F(EventDescriptorTest, InvalidEnabled) {
    cJSON_DeleteItemFromObject(json.get(), "enabled");
    cJSON_AddNumberToObject(json.get(), "enabled", 5);
    EXPECT_THROW(EventDescriptor ptr(json.get()),
                 std::logic_error);
}

TEST_F(EventDescriptorTest, EnabledTrue) {
    cJSON_DeleteItemFromObject(json.get(), "enabled");
    cJSON_AddTrueToObject(json.get(), "enabled");
    EventDescriptor ptr(json.get());
    EXPECT_TRUE(ptr.isEnabled());
}

TEST_F(EventDescriptorTest, EnabledFalse) {
    cJSON_DeleteItemFromObject(json.get(), "enabled");
    cJSON_AddFalseToObject(json.get(), "enabled");
    EventDescriptor ptr(json.get());
    EXPECT_FALSE(ptr.isEnabled());
}

TEST_F(EventDescriptorTest, EnabledSetter) {
    EventDescriptor ptr(json.get());
    ptr.setEnabled(true);
    EXPECT_TRUE(ptr.isEnabled());
    ptr.setEnabled(false);
    EXPECT_FALSE(ptr.isEnabled());
}

TEST_F(EventDescriptorTest, HandleMandatoryFieldsObject) {
    cJSON_AddItemToObject(json.get(), "mandatory_fields", cJSON_CreateObject());
    EXPECT_NO_THROW(EventDescriptor(json.get()));
}

TEST_F(EventDescriptorTest, HandleMandatoryFieldsArray) {
    cJSON_AddItemToObject(json.get(), "mandatory_fields", cJSON_CreateObject());
    EXPECT_NO_THROW(EventDescriptor(json.get()));
}

TEST_F(EventDescriptorTest, HandleMandatoryFieldsInvalidType) {
    cJSON_AddItemToObject(json.get(), "mandatory_fields", cJSON_CreateTrue());
    EXPECT_THROW(EventDescriptor(json.get()), std::logic_error);

    cJSON_DeleteItemFromObject(json.get(), "mandatory_fields");
    cJSON_AddItemToObject(json.get(), "mandatory_fields", cJSON_CreateFalse());
    EXPECT_THROW(EventDescriptor(json.get()), std::logic_error);

    cJSON_DeleteItemFromObject(json.get(), "mandatory_fields");
    cJSON_AddItemToObject(json.get(), "mandatory_fields",
                          cJSON_CreateNumber(5));
    EXPECT_THROW(EventDescriptor(json.get()), std::logic_error);

    cJSON_DeleteItemFromObject(json.get(), "mandatory_fields");
    cJSON_AddItemToObject(json.get(), "mandatory_fields",
                          cJSON_CreateString("5"));
    EXPECT_THROW(EventDescriptor(json.get()), std::logic_error);
}

TEST_F(EventDescriptorTest, HandleOptionalFieldsObject) {
    cJSON_AddItemToObject(json.get(), "optional_fields", cJSON_CreateObject());
    EXPECT_NO_THROW(EventDescriptor(json.get()));
}

TEST_F(EventDescriptorTest, HandleOptionalFieldsArray) {
    cJSON_AddItemToObject(json.get(), "optional_fields", cJSON_CreateArray());
    EXPECT_NO_THROW(EventDescriptor(json.get()));
}

TEST_F(EventDescriptorTest, HandleOptionalFieldsInvalidType) {
    cJSON_AddItemToObject(json.get(), "optional_fields", cJSON_CreateTrue());
    EXPECT_THROW(EventDescriptor(json.get()), std::logic_error);

    cJSON_DeleteItemFromObject(json.get(), "optional_fields");
    cJSON_AddItemToObject(json.get(), "optional_fields", cJSON_CreateFalse());
    EXPECT_THROW(EventDescriptor(json.get()), std::logic_error);

    cJSON_DeleteItemFromObject(json.get(), "optional_fields");
    cJSON_AddItemToObject(json.get(), "optional_fields",
                          cJSON_CreateNumber(5));
    EXPECT_THROW(EventDescriptor(json.get()), std::logic_error);

    cJSON_DeleteItemFromObject(json.get(), "optional_fields");
    cJSON_AddItemToObject(json.get(), "optional_fields",
                          cJSON_CreateString("5"));
    EXPECT_THROW(EventDescriptor(json.get()), std::logic_error);
}
