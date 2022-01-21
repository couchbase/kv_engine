/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <platform/dirutils.h>

#include "auditfile.h"
#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>
#include <platform/platform_time.h>
#include <time.h>
#include <atomic>
#include <cstring>
#include <iostream>
#include <map>

using cb::io::findFilesWithPrefix;

class AuditFileTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {
    }

    AuditConfig config;
    std::string testdir;
    nlohmann::json event;

    void SetUp() override {
        testdir = cb::io::mkdtemp("auditfile-test-");
        config.set_log_directory(testdir);
        event = create_audit_event();
    }

    void TearDown() override {
        cb::io::rmrf(testdir);
    }

    nlohmann::json create_audit_event() {
        nlohmann::json evt;
        evt["timestamp"] = "2015-03-13T02:36:00.000-07:00";
        evt["peername"] = "127.0.0.1:666";
        evt["sockname"] = "127.0.0.1:555";
        nlohmann::json source;
        source["source"] = "memcached";
        source["user"] = "myuser";
        evt["real_userid"] = source;
        return evt;
    }
};

/**
 * Test that we can create the file and stash a number of events
 * in it.
 */
TEST_F(AuditFileTest, TestFileCreation) {
    AuditFile auditfile("testing");
    auditfile.reconfigure(config);

    event["log_path"] = "fooo";

    for (int ii = 0; ii < 10; ++ii) {
        auditfile.ensure_open();
        auditfile.write_event_to_disk(event);
    }

    auditfile.close();

    auto files = findFilesWithPrefix(testdir + "/testing");
    EXPECT_EQ(1, files.size());
}

 /**
 * Test that an empty file is properly rotated using ensure_open()
 * Seen issues in the past such as MB-32232
 */
TEST_F(AuditFileTest, TestRotateEmptyFile) {
    AuditConfig defaultvalue;
    config.set_rotate_interval(defaultvalue.get_min_file_rotation_time());
    config.set_rotate_size(1024*1024);

    AuditFile auditfile("testing");
    auditfile.reconfigure(config);

    auditfile.ensure_open();
    cb_timeofday_timetravel(defaultvalue.get_min_file_rotation_time() + 1);
    auditfile.ensure_open();

    auditfile.close();

    auto files = findFilesWithPrefix(testdir + "/testing");
    EXPECT_EQ(0, files.size());
}

/**
 * Test that we can create a file, and as time flies by we rotate
 * to use the next file
 */
TEST_F(AuditFileTest, TestTimeRotate) {
    AuditConfig defaultvalue;
    config.set_rotate_interval(defaultvalue.get_min_file_rotation_time());
    config.set_rotate_size(1024*1024);

    AuditFile auditfile("testing");
    auditfile.reconfigure(config);

    event["log_path"] = "fooo";

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

    AuditFile auditfile("testing");
    auditfile.reconfigure(config);

    event["log_path"] = "fooo";

    for (int ii = 0; ii < 10; ++ii) {
        auditfile.ensure_open();
        auditfile.write_event_to_disk(event);
    }

    auditfile.close();

    auto files = findFilesWithPrefix(testdir + "/testing");
    EXPECT_EQ(10, files.size());
}

TEST_F(AuditFileTest, TestSizeRotateDisabled) {
    AuditConfig defaultvalue;
    config.set_rotate_interval(defaultvalue.get_max_file_rotation_time());
    config.set_rotate_size(0);

    AuditFile auditfile("testing");
    auditfile.reconfigure(config);

    event["log_path"] = "fooo";

    for (int ii = 0; ii < 10; ++ii) {
        auditfile.ensure_open();
        auditfile.write_event_to_disk(event);
    }

    auditfile.close();

    auto files = findFilesWithPrefix(testdir + "/testing");
    EXPECT_EQ(1, files.size());
}

/**
 * Test that the time rollover starts from the time the file was
 * opened, and not from the instance was configured
 */
TEST_F(AuditFileTest, TestRollover) {
    AuditConfig defaultvalue;
    config.set_rotate_interval(defaultvalue.get_min_file_rotation_time());
    config.set_rotate_size(100);
    AuditFile auditfile("testing");
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

    fprintf(fp, "%s", event.dump().c_str());
    fclose(fp);

    config.set_rotate_interval(3600);
    config.set_rotate_size(100);

    AuditFile auditfile("testing");
    auditfile.reconfigure(config);

    auditfile.cleanup_old_logfile(testdir);

    auto files = findFilesWithPrefix(testdir + "/testing-2015-03-13T02-36-00");
    EXPECT_EQ(1, files.size());
}

TEST_F(AuditFileTest, TestCrashRecoveryEmptyFile) {
    config.set_rotate_interval(3600);
    config.set_rotate_size(100);

    AuditFile auditfile("testing");
    auditfile.reconfigure(config);

    FILE *fp = fopen((testdir + "/audit.log").c_str(), "w");
    EXPECT_TRUE(fp != nullptr);
    fclose(fp);

    auditfile.cleanup_old_logfile(testdir);
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

    AuditFile auditfile("testing");
    auditfile.reconfigure(config);


    FILE *fp = fopen((testdir + "/audit.log").c_str(), "w");
    EXPECT_TRUE(fp != nullptr);
    fprintf(fp, "{}");
    fclose(fp);

    EXPECT_THROW(auditfile.cleanup_old_logfile(testdir), std::exception);
}

TEST_F(AuditFileTest, TestCrashRecoveryGarbeledDate) {
    config.set_rotate_interval(3600);
    config.set_rotate_size(100);

    AuditFile auditfile("testing");
    auditfile.reconfigure(config);

    FILE *fp = fopen((testdir + "/audit.log").c_str(), "w");
    EXPECT_TRUE(fp != nullptr);

    auto content = event.dump();
    auto idx = content.find("2015");
    ASSERT_NE(std::string::npos, idx);
    content.resize(idx);
    fprintf(fp, "%s", content.c_str());
    fclose(fp);

    EXPECT_THROW(auditfile.cleanup_old_logfile(testdir), std::exception);
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
