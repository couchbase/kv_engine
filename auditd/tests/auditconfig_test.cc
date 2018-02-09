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
#include <map>
#include <cstdlib>
#include <iostream>
#include <platform/dirutils.h>
#include <platform/platform.h>

#include <cerrno>
#include <cstring>
#include "auditd.h"
#include "auditconfig.h"
#include "mock_auditconfig.h"

#include <gtest/gtest.h>

class AuditConfigTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {
        testdir = std::string("auditconfig-test-") +
            std::to_string(cb_getpid());
        cb::io::mkdirp(testdir);
        // Create the audit_events.json file needed by the configuration
        std::string fname = testdir + std::string("/audit_events.json");
        FILE* fd = fopen(fname.c_str(), "w");
        ASSERT_NE(nullptr, fd)
            << "Unable to open test file '" << fname << "' error: "
            << strerror(errno);
        ASSERT_EQ(0, fclose(fd))
            << "Failed to close test file '" << fname << "' error: "
            << strerror(errno);
    }

    static void TearDownTestCase() {
        cb::io::rmrf(testdir);
    }

    cJSON *json;
    AuditConfig config;
    static std::string testdir;

    virtual void SetUp() {
        json = createDefaultConfig();
    }

    virtual void TearDown() {
        cJSON_Delete(json);
    }

    cJSON *createDefaultConfig(void) {
        cJSON *root = cJSON_CreateObject();
        cJSON_AddNumberToObject(root, "version", 2);
        cJSON_AddNumberToObject(root, "rotate_size", 20*1024*1024);
        cJSON_AddNumberToObject(root, "rotate_interval", 900);
        cJSON_AddTrueToObject(root, "auditd_enabled");
        cJSON_AddTrueToObject(root, "buffered");
        cJSON_AddStringToObject(root, "log_path", testdir.c_str());
        cJSON_AddStringToObject(root, "descriptors_path", testdir.c_str());
        cJSON *sync = cJSON_CreateArray();
        cJSON_AddItemToObject(root, "sync", sync);
        cJSON *disabled = cJSON_CreateArray();
        cJSON_AddItemToObject(root, "disabled", disabled);
        cJSON *event_states = cJSON_CreateObject();
        cJSON_AddItemToObject(root, "event_states", event_states);
        cJSON* disabled_userids = cJSON_CreateArray();
        cJSON_AddItemToObject(root, "disabled_userids", disabled_userids);
        cJSON_AddTrueToObject(root, "filtering_enabled");
        cJSON_AddStringToObject(root, "uuid", "123456");

        return root;
    }
};

std::string AuditConfigTest::testdir;

TEST_F(AuditConfigTest, UnknownTag) {
    cJSON_AddNumberToObject(json, "foo", 5);
    EXPECT_THROW(config.initialize_config(json), std::string);
}

// version

TEST_F(AuditConfigTest, TestGetVersion) {
    ASSERT_NO_THROW(config.initialize_config(json));
    EXPECT_EQ(2, config.get_version());
}

TEST_F(AuditConfigTest, TestNoVersion) {
    cJSON *obj = cJSON_DetachItemFromObject(json, "version");
    cJSON_Delete(obj);
    EXPECT_THROW(config.initialize_config(json), std::string);
}

TEST_F(AuditConfigTest, TestIllegalDatatypeVersion) {
    cJSON_ReplaceItemInObject(json, "version",
                              cJSON_CreateString("foobar"));
    EXPECT_THROW(config.initialize_config(json), std::string);
}

TEST_F(AuditConfigTest, TestLegalVersion) {
    for (int version = -100; version < 100; ++version) {
        cJSON_ReplaceItemInObject(json, "version",
                                  cJSON_CreateNumber(version));
        if (version == 1) {
            cJSON* obj = cJSON_DetachItemFromObject(json, "filtering_enabled");
            cJSON_Delete(obj);
            obj = cJSON_DetachItemFromObject(json, "disabled_userids");
            cJSON_Delete(obj);
            obj = cJSON_DetachItemFromObject(json, "event_states");
            cJSON_Delete(obj);
            obj = cJSON_DetachItemFromObject(json, "uuid");
            cJSON_Delete(obj);
        }
        if (version == 2) {
            cJSON_AddTrueToObject(json, "filtering_enabled");
            cJSON* disabled_userids = cJSON_CreateArray();
            cJSON_AddItemToObject(json, "disabled_userids", disabled_userids);
            cJSON* event_states = cJSON_CreateObject();
            cJSON_AddItemToObject(json, "event_states", event_states);
            cJSON_AddStringToObject(json, "uuid", "123456");
        }
        if ((version == 1) || (version == 2)) {
            EXPECT_NO_THROW(config.initialize_config(json));
        } else {
            EXPECT_THROW(config.initialize_config(json), std::string);
        }
    }
}

// rotate_size

TEST_F(AuditConfigTest, TestNoRotateSize) {
    cJSON *obj = cJSON_DetachItemFromObject(json, "rotate_size");
    cJSON_Delete(obj);
    EXPECT_THROW(config.initialize_config(json), std::string);
}

TEST_F(AuditConfigTest, TestRotateSizeSetGet) {
    for (size_t ii = 0; ii < 100; ++ii) {
        config.set_rotate_size(ii);
        EXPECT_EQ(ii, config.get_rotate_size());
    }
}

TEST_F(AuditConfigTest, TestRotateSizeIllegalDatatype) {
    cJSON_ReplaceItemInObject(json, "rotate_size",
                              cJSON_CreateString("foobar"));
    EXPECT_THROW(config.initialize_config(json), std::string);
}

TEST_F(AuditConfigTest, TestRotateSizeLegalValue) {
    cJSON_ReplaceItemInObject(json, "rotate_size",
                              cJSON_CreateNumber(100));
    EXPECT_NO_THROW(config.initialize_config(json));
}

TEST_F(AuditConfigTest, TestRotateSizeIllegalValue) {
    cJSON_ReplaceItemInObject(json, "rotate_size",
                              cJSON_CreateNumber(-1));
    EXPECT_THROW(config.initialize_config(json), std::string);
}

// rotate_interval

TEST_F(AuditConfigTest, TestNoRotateInterval) {
    cJSON *obj = cJSON_DetachItemFromObject(json, "rotate_interval");
    cJSON_Delete(obj);
    EXPECT_THROW(config.initialize_config(json), std::string);
}

TEST_F(AuditConfigTest, TestRotateIntervalSetGet) {
    AuditConfig defaultvalue;
    const uint32_t min_file_rotation_time = defaultvalue.get_min_file_rotation_time();
    for (size_t ii = min_file_rotation_time;
         ii < min_file_rotation_time + 10;
         ++ii) {
        config.set_rotate_interval(uint32_t(ii));
        EXPECT_EQ(ii, config.get_rotate_interval());
    }
}

TEST_F(AuditConfigTest, TestRotateIntervalIllegalDatatype) {
    cJSON_ReplaceItemInObject(json, "rotate_interval",
                              cJSON_CreateString("foobar"));
    EXPECT_THROW(config.initialize_config(json), std::string);
}

TEST_F(AuditConfigTest, TestRotateIntervalLegalValue) {
    AuditConfig defaultvalue;
    const uint32_t min_file_rotation_time = defaultvalue.get_min_file_rotation_time();
    const uint32_t max_file_rotation_time = defaultvalue.get_max_file_rotation_time();

    for (size_t ii = min_file_rotation_time;
         ii < max_file_rotation_time;
         ii += 1000) {
        cJSON_ReplaceItemInObject(json, "rotate_interval",
                                  cJSON_CreateNumber(double(ii)));
        EXPECT_NO_THROW(config.initialize_config(json));
    }
}

TEST_F(AuditConfigTest, TestRotateIntervalIllegalValue) {
    AuditConfig defaultvalue;
    const uint32_t min_file_rotation_time = defaultvalue.get_min_file_rotation_time();
    const uint32_t max_file_rotation_time = defaultvalue.get_max_file_rotation_time();

    cJSON_ReplaceItemInObject(json, "rotate_interval",
                              cJSON_CreateNumber(min_file_rotation_time - 1));
    EXPECT_THROW(config.initialize_config(json), std::string);
    cJSON_ReplaceItemInObject(json, "rotate_interval",
                              cJSON_CreateNumber(max_file_rotation_time + 1));
    EXPECT_THROW(config.initialize_config(json), std::string);
}

// auditd_enabled

TEST_F(AuditConfigTest, TestNoAuditdEnabled) {
    cJSON *obj = cJSON_DetachItemFromObject(json, "auditd_enabled");
    cJSON_Delete(obj);
    EXPECT_THROW(config.initialize_config(json), std::string);
}

TEST_F(AuditConfigTest, TestGetSetAuditdEnabled) {
    config.set_auditd_enabled(true);
    EXPECT_TRUE(config.is_auditd_enabled());
    config.set_auditd_enabled(false);
    EXPECT_FALSE(config.is_auditd_enabled());
}

TEST_F(AuditConfigTest, TestIllegalDatatypeAuditdEnabled) {
    cJSON_ReplaceItemInObject(json, "auditd_enabled",
                              cJSON_CreateString("foobar"));
    EXPECT_THROW(config.initialize_config(json), std::string);
}

TEST_F(AuditConfigTest, TestLegalAuditdEnabled) {
    cJSON_ReplaceItemInObject(json, "auditd_enabled", cJSON_CreateTrue());
    EXPECT_NO_THROW(config.initialize_config(json));

    cJSON_ReplaceItemInObject(json, "auditd_enabled", cJSON_CreateFalse());
    EXPECT_NO_THROW(config.initialize_config(json));
}

// buffered

TEST_F(AuditConfigTest, TestNoBuffered) {
    // buffered is optional, and enabled unless explicitly disabled
    cJSON *obj = cJSON_DetachItemFromObject(json, "buffered");
    cJSON_Delete(obj);
    EXPECT_NO_THROW(config.initialize_config(json));
    EXPECT_TRUE(config.is_buffered());
}

TEST_F(AuditConfigTest, TestGetSetBuffered) {
    config.set_buffered(true);
    EXPECT_TRUE(config.is_buffered());
    config.set_buffered(false);
    EXPECT_FALSE(config.is_buffered());
}

TEST_F(AuditConfigTest, TestIllegalDatatypeBuffered) {
    cJSON_ReplaceItemInObject(json, "buffered",
                              cJSON_CreateString("foobar"));
    EXPECT_THROW(config.initialize_config(json), std::string);
}

TEST_F(AuditConfigTest, TestLegalBuffered) {
    cJSON_ReplaceItemInObject(json, "buffered", cJSON_CreateTrue());
    EXPECT_NO_THROW(config.initialize_config(json));

    cJSON_ReplaceItemInObject(json, "buffered", cJSON_CreateFalse());
    EXPECT_NO_THROW(config.initialize_config(json));
}

// log_path
TEST_F(AuditConfigTest, TestNoLogPath) {
    cJSON *obj = cJSON_DetachItemFromObject(json, "log_path");
    cJSON_Delete(obj);
    EXPECT_THROW(config.initialize_config(json), std::string);
}

TEST_F(AuditConfigTest, TestGetSetLogPath) {
    EXPECT_NO_THROW(config.set_log_directory(testdir));
    EXPECT_EQ(testdir, config.get_log_directory());
}

TEST_F(AuditConfigTest, TestGetSetSanitizeLogPath) {
    // Trim of trailing paths
    std::string path = testdir + std::string("/");
    EXPECT_NO_THROW(config.set_log_directory(path));
    EXPECT_EQ(testdir, config.get_log_directory());
}

#ifdef WIN32
TEST_F(AuditConfigTest, TestGetSetSanitizeLogPathMixedSeparators) {
    // Trim of trailing paths
    std::string path = testdir + std::string("/mydir\\baddir");
    EXPECT_NO_THROW(config.set_log_directory(path));
    EXPECT_EQ(testdir + "\\mydir\\baddir", config.get_log_directory());
    EXPECT_NO_THROW(cb::io::rmrf(config.get_log_directory()))
        << "Failed to remove: " << config.get_log_directory()
        << ": " << strerror(errno) << std::endl;
}
#endif

#ifndef WIN32
TEST_F(AuditConfigTest, TestFailToCreateDirLogPath) {
    cJSON_ReplaceItemInObject(json, "log_path",
                              cJSON_CreateString("/itwouldsuckifthisexists"));
    EXPECT_THROW(config.initialize_config(json), std::string);
}
#endif

TEST_F(AuditConfigTest, TestCreateDirLogPath) {
    std::string path = testdir + std::string("/mybar");
    cJSON_ReplaceItemInObject(json, "log_path",
                              cJSON_CreateString(path.c_str()));
    EXPECT_NO_THROW(config.initialize_config(json));
    EXPECT_NO_THROW(cb::io::rmrf(config.get_log_directory()))
        << "Failed to remove: " << config.get_log_directory()
        << ": " << strerror(errno) << std::endl;
}

// descriptors_path
TEST_F(AuditConfigTest, TestNoDescriptorsPath) {
    cJSON *obj = cJSON_DetachItemFromObject(json, "descriptors_path");
    cJSON_Delete(obj);
    EXPECT_THROW(config.initialize_config(json), std::string);
}

TEST_F(AuditConfigTest, TestGetSetDescriptorsPath) {
    EXPECT_NO_THROW(config.set_descriptors_path(testdir));
    EXPECT_EQ(testdir, config.get_descriptors_path());
}

TEST_F(AuditConfigTest, TestSetMissingEventDescrFileDescriptorsPath) {
    std::string path = testdir + std::string("/foo");
    cb::io::mkdirp(path);

    EXPECT_THROW(config.set_descriptors_path(path), std::string);
    EXPECT_NO_THROW(cb::io::rmrf(path))
        << "Failed to remove: " << path
        << ": " << strerror(errno) << std::endl;
}

// Sync
TEST_F(AuditConfigTest, TestNoSync) {
    cJSON *obj = cJSON_DetachItemFromObject(json, "sync");
    cJSON_Delete(obj);
    EXPECT_THROW(config.initialize_config(json), std::string);
}

TEST_F(AuditConfigTest, TestSpecifySync) {
    cJSON *array = cJSON_CreateArray();
    for (int ii = 0; ii < 10; ++ii) {
        cJSON_AddItemToArray(array, cJSON_CreateNumber(ii));
    }
    cJSON_ReplaceItemInObject(json, "sync", array);
    EXPECT_NO_THROW(config.initialize_config(json));

    for (uint32_t ii = 0; ii < 100; ++ii) {
        if (ii < 10) {
            EXPECT_TRUE(config.is_event_sync(ii));
        } else {
            EXPECT_FALSE(config.is_event_sync(ii));
        }
    }
}

// Disabled
TEST_F(AuditConfigTest, TestNoDisabled) {
    cJSON *obj = cJSON_DetachItemFromObject(json, "disabled");
    cJSON_Delete(obj);
    if (config.get_version() == 1) {
        EXPECT_THROW(config.initialize_config(json), std::string);
    } else {
        EXPECT_NO_THROW(config.initialize_config(json));
    }
}

TEST_F(AuditConfigTest, TestSpecifyDisabled) {
    cJSON *array = cJSON_CreateArray();
    for (int ii = 0; ii < 10; ++ii) {
        cJSON_AddItemToArray(array, cJSON_CreateNumber(ii));
    }
    cJSON_ReplaceItemInObject(json, "disabled", array);
    EXPECT_NO_THROW(config.initialize_config(json));

    for (uint32_t ii = 0; ii < 100; ++ii) {
        if (ii < 10 && config.get_version() == 1) {
            EXPECT_TRUE(config.is_event_disabled(ii));
        } else {
            EXPECT_FALSE(config.is_event_disabled(ii));
        }
    }
}

TEST_F(AuditConfigTest, TestSpecifyDisabledUsers) {
    cJSON *array = cJSON_CreateArray();
    for (uint16_t ii = 0; ii < 10; ++ii) {
        cJSON* userIdRoot = cJSON_CreateObject();
        if (userIdRoot == nullptr) {
            throw std::runtime_error(
                    "TestSpecifyDisabledUsers - Error "
                    "creating cJSON object");
        }
        cJSON_AddStringToObject(userIdRoot, "source", "internal");
        auto user = "user" + std::to_string(ii);
        cJSON_AddStringToObject(userIdRoot, "user", user.c_str());
        cJSON_AddItemToArray(array, userIdRoot);
    }
    cJSON_ReplaceItemInObject(json, "disabled_userids", array);
    EXPECT_NO_THROW(config.initialize_config(json));

    for (uint16_t ii = 0; ii < 100; ++ii) {
        const auto& source = "internal";
        const auto& user = "user" + std::to_string(ii);
        const auto& userid = std::make_pair(source, user);
        if (ii < 10) {
            EXPECT_TRUE(config.is_event_filtered(userid));
        } else {
            EXPECT_FALSE(config.is_event_filtered(userid));
        }
    }
}

/**
 * Tests that when converting a config containing a single disabled event it
 * translates to a single entry in the json "disabled" array and the json
 * "disabled_userids" array remains empty.
 */
TEST_F(AuditConfigTest, AuditConfigDisabled) {
    MockAuditConfig config;
    unique_cJSON_ptr disabled(cJSON_CreateObject());
    cJSON_AddItemToArray(disabled.get(), cJSON_CreateNumber(1234));
    config.public_set_disabled(disabled.get());
    unique_cJSON_ptr json { config.to_json() };
    auto disabledArray = MockAuditConfig::public_getObject(json.get(),
                                                           "disabled",
                                                           cJSON_Array);
    EXPECT_EQ(1, cJSON_GetArraySize(disabledArray));
    auto disabledUseridsArray = MockAuditConfig::public_getObject(
            json.get(), "disabled_userids", cJSON_Array);
    EXPECT_EQ(0, cJSON_GetArraySize(disabledUseridsArray));
}

/**
 * Tests that when converting a config containing a single disabled_user it
 * translates to a single entry in the json "disabled_userids" array and the
 * json "disabled" array remains empty.
 */
TEST_F(AuditConfigTest, AuditConfigDisabledUsers) {
    MockAuditConfig config;
    unique_cJSON_ptr disabledUserids(cJSON_CreateObject());

    cJSON* userIdRoot = cJSON_CreateObject();
    if (userIdRoot == nullptr) {
        throw std::bad_alloc();
    }
    cJSON_AddStringToObject(userIdRoot, "source", "internal");
    cJSON_AddStringToObject(userIdRoot, "user", "johndoe");
    cJSON_AddItemToArray(disabledUserids.get(), userIdRoot);

    config.public_set_disabled_userids(disabledUserids.get());
    unique_cJSON_ptr json { config.to_json() };
    auto disabledUseridsArray = MockAuditConfig::public_getObject(
            json.get(), "disabled_userids", cJSON_Array);
    EXPECT_EQ(1, cJSON_GetArraySize(disabledUseridsArray));
    auto disabledArray = MockAuditConfig::public_getObject(json.get(),
                                                           "disabled",
                                                           cJSON_Array);
    EXPECT_EQ(0, cJSON_GetArraySize(disabledArray));
}

// Test the filtering_enabled parameter

TEST_F(AuditConfigTest, TestNoFilteringEnabled) {
    cJSON *obj = cJSON_DetachItemFromObject(json, "filtering_enabled");
    cJSON_Delete(obj);
    EXPECT_THROW(config.initialize_config(json), std::string);
}

TEST_F(AuditConfigTest, TestGetSetFilteringEnabled) {
    config.set_filtering_enabled(true);
    EXPECT_TRUE(config.is_filtering_enabled());
    config.set_filtering_enabled(false);
    EXPECT_FALSE(config.is_filtering_enabled());
}

TEST_F(AuditConfigTest, TestIllegalDatatypeFilteringEnabled) {
    cJSON_ReplaceItemInObject(json, "filtering_enabled",
                              cJSON_CreateString("foobar"));
    EXPECT_THROW(config.initialize_config(json), std::string);
}

TEST_F(AuditConfigTest, TestLegalFilteringEnabled) {
    cJSON_ReplaceItemInObject(json, "filtering_enabled", cJSON_CreateTrue());
    EXPECT_NO_THROW(config.initialize_config(json));

    cJSON_ReplaceItemInObject(json, "filtering_enabled", cJSON_CreateFalse());
    EXPECT_NO_THROW(config.initialize_config(json));
}

// The event_states list is optional and therefore if it does not exist
// it should not throw an exception.
TEST_F(AuditConfigTest, TestNoEventStates) {
    cJSON* obj = cJSON_DetachItemFromObject(json, "event_states");
    cJSON_Delete(obj);
    EXPECT_NO_THROW(config.initialize_config(json));
}

// Test that with an event_states object consisting of "enabled" and "disabled"
// the states get converted into corresponding EventStates.  Also if an event
// is not in the event_states object it has an EventState of undefined.
TEST_F(AuditConfigTest, TestSpecifyEventStates) {
    cJSON* object = cJSON_CreateObject();
    for (int ii = 0; ii < 5; ++ii) {
        auto event = std::to_string(ii);
        cJSON_AddStringToObject(object, event.c_str(), "enabled");
    }
    for (int ii = 5; ii < 10; ++ii) {
        auto event = std::to_string(ii);
        cJSON_AddStringToObject(object, event.c_str(), "disabled");
    }
    cJSON_ReplaceItemInObject(json, "event_states", object);
    EXPECT_NO_THROW(config.initialize_config(json));

    for (uint32_t ii = 0; ii < 20; ++ii) {
        if (ii < 5) {
            EXPECT_EQ(AuditConfig::EventState::enabled,
                      config.get_event_state(ii));
        } else if (ii < 10) {
            EXPECT_EQ(AuditConfig::EventState::disabled,
                      config.get_event_state(ii));
        } else {
            EXPECT_EQ(AuditConfig::EventState::undefined,
                      config.get_event_state(ii));
        }
    }
}
