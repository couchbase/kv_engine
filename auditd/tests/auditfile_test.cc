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
#include <platform/dirutils.h>

#include "auditfile.h"
#include <iostream>
#include <map>
#include <atomic>
#include <cstring>
#include <time.h>
#include <gtest/gtest.h>
#include <platform/platform.h>

using cb::io::findFilesWithPrefix;

class AuditFileTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {
    }

    AuditConfig config;
    std::string testdir;
    cJSON *event;

    virtual void SetUp() {
        testdir = std::string("auditfile-test-") + std::to_string(cb_getpid());
        config.set_log_directory(testdir);
        event = create_audit_event();
    }

    virtual void TearDown() {
        cb::io::rmrf(testdir);
        cJSON_Delete(event);
    }

    cJSON *create_audit_event() {
        cJSON *root = cJSON_CreateObject();
        cJSON_AddStringToObject(root, "timestamp",
                                "2015-03-13T02:36:00.000-07:00");
        cJSON_AddStringToObject(root, "peername", "127.0.0.1:666");
        cJSON_AddStringToObject(root, "sockname", "127.0.0.1:555");
        cJSON *source = cJSON_CreateObject();
        cJSON_AddStringToObject(source, "source", "memcached");
        cJSON_AddStringToObject(source, "user", "myuser");
        cJSON_AddItemToObject(root, "real_userid", source);
        return root;
    }
};

/**
 * Test that we can create the file and stash a number of events
 * in it.
 */
TEST_F(AuditFileTest, TestFileCreation) {
    AuditFile auditfile;
    auditfile.reconfigure(config);

    cJSON_AddStringToObject(event, "log_path", "fooo");

    for (int ii = 0; ii < 10; ++ii) {
        auditfile.ensure_open();
        auditfile.write_event_to_disk(event);
    }

    auditfile.close();

    auto files = findFilesWithPrefix(testdir + "/testing");
    EXPECT_EQ(1, files.size());
}

/**
 * Test that we can create a file, and as time flies by we rotate
 * to use the next file
 */
TEST_F(AuditFileTest, TestTimeRotate) {
    AuditConfig defaultvalue;
    config.set_rotate_interval(defaultvalue.get_min_file_rotation_time());
    config.set_rotate_size(1024*1024);

    AuditFile auditfile;
    auditfile.reconfigure(config);

    cJSON_AddStringToObject(event, "log_path", "fooo");

    for (int ii = 0; ii < 10; ++ii) {
        auditfile.ensure_open();
        auditfile.write_event_to_disk(event);
        cb_timeofday_timetravel(defaultvalue.get_min_file_rotation_time() + 1);
    }

    auditfile.close();

    auto files = findFilesWithPrefix(testdir + "/testing");
    EXPECT_EQ(10, files.size());
}

/**
 * Test that the we'll rotate to the next file as the content
 * of the file gets bigger.
 */
TEST_F(AuditFileTest, TestSizeRotate) {
    AuditConfig defaultvalue;
    config.set_rotate_interval(defaultvalue.get_max_file_rotation_time());
    config.set_rotate_size(100);

    AuditFile auditfile;
    auditfile.reconfigure(config);

    cJSON_AddStringToObject(event, "log_path", "fooo");

    for (int ii = 0; ii < 10; ++ii) {
        auditfile.ensure_open();
        auditfile.write_event_to_disk(event);
    }

    auditfile.close();

    auto files = findFilesWithPrefix(testdir + "/testing");
    EXPECT_EQ(10, files.size());
}

/**
 * Test that the time rollover starts from the time the file was
 * opened, and not from the instance was configured
 */
TEST_F(AuditFileTest, TestRollover) {
    AuditConfig defaultvalue;
    config.set_rotate_interval(defaultvalue.get_min_file_rotation_time());
    config.set_rotate_size(100);
    AuditFile auditfile;
    auditfile.reconfigure(config);

    uint32_t secs = auditfile.get_seconds_to_rotation();
    EXPECT_EQ(defaultvalue.get_min_file_rotation_time(), secs);

    cb_timeofday_timetravel(10);
    EXPECT_EQ(secs, auditfile.get_seconds_to_rotation())
        << "Secs to rotation should not change while file is closed";

    auditfile.ensure_open();

    secs = auditfile.get_seconds_to_rotation();
    EXPECT_TRUE(secs == defaultvalue.get_min_file_rotation_time() ||
                secs == (defaultvalue.get_min_file_rotation_time() - 1));

    cb_timeofday_timetravel(10);
    secs = auditfile.get_seconds_to_rotation();
    EXPECT_TRUE(secs == defaultvalue.get_min_file_rotation_time() - 10 ||
                secs == (defaultvalue.get_min_file_rotation_time() - 11));
}

TEST_F(AuditFileTest, TestSuccessfulCrashRecovery) {
    FILE *fp = fopen((testdir + "/audit.log").c_str(), "w");
    EXPECT_TRUE(fp != nullptr);

    char *content = cJSON_PrintUnformatted(event);
    fprintf(fp, "%s", content);
    fclose(fp);
    cJSON_Free(content);

    config.set_rotate_interval(3600);
    config.set_rotate_size(100);

    AuditFile auditfile;
    auditfile.reconfigure(config);

    EXPECT_NO_THROW(auditfile.cleanup_old_logfile(testdir));

    auto files = findFilesWithPrefix(testdir + "/testing-2015-03-13T02-36-00");
    EXPECT_EQ(1, files.size());
}

TEST_F(AuditFileTest, TestCrashRecoveryEmptyFile) {
    config.set_rotate_interval(3600);
    config.set_rotate_size(100);

    AuditFile auditfile;
    auditfile.reconfigure(config);

    FILE *fp = fopen((testdir + "/audit.log").c_str(), "w");
    EXPECT_TRUE(fp != nullptr);
    fclose(fp);

    EXPECT_NO_THROW(auditfile.cleanup_old_logfile(testdir));
    {
        // It should not have created any new files
        auto files = findFilesWithPrefix(testdir + "/testing");
        EXPECT_EQ(0, files.size());
    }
    {
        // File was empty and should just have been deleted
        auto files = findFilesWithPrefix(testdir + "/audit");
        EXPECT_EQ(0, files.size());
    }
}

TEST_F(AuditFileTest, TestCrashRecoveryNoTimestamp) {
    config.set_rotate_interval(3600);
    config.set_rotate_size(100);

    AuditFile auditfile;
    auditfile.reconfigure(config);


    FILE *fp = fopen((testdir + "/audit.log").c_str(), "w");
    EXPECT_TRUE(fp != nullptr);
    fprintf(fp, "{}");
    fclose(fp);

    EXPECT_THROW(auditfile.cleanup_old_logfile(testdir), std::string);
}

TEST_F(AuditFileTest, TestCrashRecoveryGarbeledDate) {
    config.set_rotate_interval(3600);
    config.set_rotate_size(100);

    AuditFile auditfile;
    auditfile.reconfigure(config);

    FILE *fp = fopen((testdir + "/audit.log").c_str(), "w");
    EXPECT_TRUE(fp != nullptr);

    char *content = cJSON_PrintUnformatted(event);
    char *ptr = strstr(content, "2015");
    *ptr = '\0';
    fprintf(fp, "%s", content);
    fclose(fp);
    cJSON_Free(content);

    EXPECT_THROW(auditfile.cleanup_old_logfile(testdir), std::string);
    {
        auto files = findFilesWithPrefix(testdir + "/testing");
        EXPECT_EQ(0, files.size());
    }
    // audit.log should still be present
    {
        auto files = findFilesWithPrefix(testdir + "/audit.log");
        EXPECT_EQ(1, files.size());
    }
}
