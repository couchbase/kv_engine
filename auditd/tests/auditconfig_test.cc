/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

#include <gtest/gtest.h>

class AuditConfigTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {
        testdir = std::string("auditconfig-test-") +
            std::to_string(cb_getpid());
        ASSERT_TRUE(CouchbaseDirectoryUtilities::mkdirp(testdir));
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
        CouchbaseDirectoryUtilities::rmrf(testdir);
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
        cJSON_AddNumberToObject(root, "version", 1);
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

        return root;
    }
};

std::string AuditConfigTest::testdir;

TEST_F(AuditConfigTest, UnknownTag) {
    cJSON_AddNumberToObject(json, "foo", 5);
    EXPECT_THROW(config.initialize_config(json), std::string);
}

// version

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
    for (size_t ii = AuditConfig::min_file_rotation_time;
         ii < AuditConfig::min_file_rotation_time+10;
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
    for (size_t ii = AuditConfig::min_file_rotation_time;
         ii < AuditConfig::max_file_rotation_time;
         ii += 1000) {
        cJSON_ReplaceItemInObject(json, "rotate_interval",
                                  cJSON_CreateNumber(double(ii)));
        EXPECT_NO_THROW(config.initialize_config(json));
    }
}

TEST_F(AuditConfigTest, TestRotateIntervalIllegalValue) {
    cJSON_ReplaceItemInObject(json, "rotate_interval",
                              cJSON_CreateNumber(AuditConfig::min_file_rotation_time-1));
    EXPECT_THROW(config.initialize_config(json), std::string);
    cJSON_ReplaceItemInObject(json, "rotate_interval",
                              cJSON_CreateNumber(AuditConfig::max_file_rotation_time+1));
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
    EXPECT_TRUE(CouchbaseDirectoryUtilities::rmrf(config.get_log_directory()))
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
    EXPECT_TRUE(CouchbaseDirectoryUtilities::rmrf(config.get_log_directory()))
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
    CouchbaseDirectoryUtilities::mkdirp(path);

    EXPECT_THROW(config.set_descriptors_path(path), std::string);
    EXPECT_TRUE(CouchbaseDirectoryUtilities::rmrf(path))
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
    EXPECT_THROW(config.initialize_config(json), std::string);
}

TEST_F(AuditConfigTest, TestSpecifyDisabled) {
    cJSON *array = cJSON_CreateArray();
    for (int ii = 0; ii < 10; ++ii) {
        cJSON_AddItemToArray(array, cJSON_CreateNumber(ii));
    }
    cJSON_ReplaceItemInObject(json, "disabled", array);
    EXPECT_NO_THROW(config.initialize_config(json));

    for (uint32_t ii = 0; ii < 100; ++ii) {
        if (ii < 10) {
            EXPECT_TRUE(config.is_event_disabled(ii));
        } else {
            EXPECT_FALSE(config.is_event_disabled(ii));
        }
    }
}
