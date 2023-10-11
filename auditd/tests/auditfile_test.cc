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
#include <folly/FileUtil.h>
#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>
#include <platform/platform_time.h>
#include <fstream>
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
        std::filesystem::remove_all(testdir);
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

    void writeAuditFile(std::string_view content) {
        EXPECT_TRUE(
                folly::writeFile(content, (testdir + "/audit.log").c_str()));
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

static const uint32_t rotation_time = 100;

/**
 * Test that an empty file is properly rotated using ensure_open()
 * Seen issues in the past such as MB-32232
 */
TEST_F(AuditFileTest, TestRotateEmptyFile) {
    config.set_rotate_interval(rotation_time);
    config.set_rotate_size(1024*1024);

    AuditFile auditfile("testing");
    auditfile.reconfigure(config);

    auditfile.ensure_open();
    cb_timeofday_timetravel(rotation_time + 1);
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
    config.set_rotate_interval(rotation_time);
    config.set_rotate_size(1024*1024);

    AuditFile auditfile("testing");
    auditfile.reconfigure(config);

    event["log_path"] = "fooo";

    for (int ii = 0; ii < 10; ++ii) {
        auditfile.ensure_open();
        auditfile.write_event_to_disk(event);
        cb_timeofday_timetravel(rotation_time + 1);
    }

    auditfile.close();

    auto files = findFilesWithPrefix(testdir + "/testing");
    EXPECT_EQ(10, files.size());
}

/// Verify that it is possible to disable time based rotation
TEST_F(AuditFileTest, TestTimeRotateDisabled) {
    config.set_rotate_interval(0);
    config.set_rotate_size(0);

    AuditFile auditfile("testing");
    auditfile.reconfigure(config);
    event["log_path"] = "fooo";

    // Generate a few events while traveling in time
    for (int ii = 0; ii < 10; ++ii) {
        auditfile.ensure_open();
        auditfile.write_event_to_disk(event);
        cb_timeofday_timetravel(rotation_time + 1);
    }
    auditfile.close();

    auto files = findFilesWithPrefix(testdir + "/testing");
    EXPECT_EQ(1, files.size());
}

/**
 * Test that the we'll rotate to the next file as the content
 * of the file gets bigger.
 */
TEST_F(AuditFileTest, TestSizeRotate) {
    config.set_rotate_interval(0);
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
    config.set_rotate_interval(0);
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
    config.set_rotate_interval(rotation_time);
    config.set_rotate_size(100);
    AuditFile auditfile("testing");
    auditfile.reconfigure(config);

    uint32_t secs = auditfile.get_seconds_to_rotation();
    EXPECT_EQ(rotation_time, secs);

    cb_timeofday_timetravel(10);
    EXPECT_EQ(secs, auditfile.get_seconds_to_rotation())
        << "Secs to rotation should not change while file is closed";

    auditfile.ensure_open();

    secs = auditfile.get_seconds_to_rotation();
    EXPECT_TRUE(secs == rotation_time || secs == (rotation_time - 1));

    cb_timeofday_timetravel(10);
    secs = auditfile.get_seconds_to_rotation();
    EXPECT_TRUE(secs == rotation_time - 10 || secs == (rotation_time - 11));
}

/// The audit log contains a valid entry and the file gets renamed
/// to the correct filename
TEST_F(AuditFileTest, TestSuccessfulCrashRecovery) {
    writeAuditFile(event.dump());

    config.set_rotate_interval(3600);
    config.set_rotate_size(100);

    AuditFile auditfile("testing");
    auditfile.reconfigure(config);

    auditfile.cleanup_old_logfile(testdir);

    auto files = findFilesWithPrefix(testdir + "/testing-2015-03-13T02-36-00");
    EXPECT_EQ(1, files.size());
}

/// Verify that we don't crash if there isn't a file available
TEST_F(AuditFileTest, TestCrashRecoveryNoFile) {
    config.set_rotate_interval(3600);
    config.set_rotate_size(100);

    AuditFile auditfile("testing");
    auditfile.reconfigure(config);

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

/// Verify that we just remove the file if it doesn't contain any data
TEST_F(AuditFileTest, TestCrashRecoveryEmptyFile) {
    config.set_rotate_interval(3600);
    config.set_rotate_size(100);

    AuditFile auditfile("testing");
    auditfile.reconfigure(config);

    writeAuditFile({});
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

/// Verify that we don't crash if the the first line in the audit event
/// doesn't contain a timestamp field
TEST_F(AuditFileTest, TestCrashRecoveryNoTimestamp) {
    config.set_rotate_interval(3600);
    config.set_rotate_size(100);

    AuditFile auditfile("testing");
    auditfile.reconfigure(config);

    event.erase("timestamp");
    writeAuditFile(event.dump());

    auditfile.cleanup_old_logfile(testdir);

    {
        // The file should have been renamed (read the content and verify
        // that it is the same)
        auto files = findFilesWithPrefix(testdir + "/testing");
        ASSERT_EQ(1, files.size());
        auto content = cb::io::loadFile(files.front());
        EXPECT_EQ(event.dump(), content);
    }
    {
        // rename should make the old inaccessible
        auto files = findFilesWithPrefix(testdir + "/audit");
        EXPECT_EQ(0, files.size());
    }
}

/// Verify that we don't fail if the date isn't using the correct format
TEST_F(AuditFileTest, TestCrashRecoveryGarbeledDate) {
    config.set_rotate_interval(3600);
    config.set_rotate_size(100);

    AuditFile auditfile("testing");
    auditfile.reconfigure(config);

    event["timestamp"] = "This isn't a correct date";
    writeAuditFile(event.dump());

    auditfile.cleanup_old_logfile(testdir);

    {
        auto files = findFilesWithPrefix(testdir + "/testing");
        EXPECT_EQ(1, files.size());
        // Verify that we didn't use the invalid text in the filename
        EXPECT_EQ(std::string::npos, files.front().find("correct"));
        auto content = cb::io::loadFile(files.front());
        EXPECT_EQ(event.dump(), content);
    }

    {
        auto files = findFilesWithPrefix(testdir + "/audit.log");
        EXPECT_EQ(0, files.size());
    }
}

/// Verify that we don't crash if we fail to parse the JSON
TEST_F(AuditFileTest, TestCrashRecoveryGarbeledData) {
    config.set_rotate_interval(3600);
    config.set_rotate_size(100);

    AuditFile auditfile("testing");
    auditfile.reconfigure(config);

    const std::string_view content = R"({ "this is some partial JSON" : )";
    writeAuditFile(content);

    auditfile.cleanup_old_logfile(testdir);
    {
        auto files = findFilesWithPrefix(testdir + "/testing");
        EXPECT_EQ(1, files.size());
        EXPECT_EQ(content, cb::io::loadFile(files.front()));
    }

    {
        auto files = findFilesWithPrefix(testdir + "/audit.log");
        EXPECT_EQ(0, files.size());
    }
}

TEST_F(AuditFileTest, TestCrashRecoveryFileAlreadyExists) {
    writeAuditFile(event.dump());
    std::filesystem::path dir(testdir);

    std::ofstream of;
    of.open(dir / "testing-2015-03-13T02-36-00-audit.log", std::ios::binary);
    of << "hello" << std::endl;
    of.close();

    config.set_rotate_interval(3600);
    config.set_rotate_size(100);

    AuditFile auditfile("testing");
    auditfile.reconfigure(config);

    auditfile.cleanup_old_logfile(testdir);

    auto files = findFilesWithPrefix(testdir + "/testing-2015-03-13T02-36-00");
    EXPECT_EQ(2, files.size());
}

TEST_F(AuditFileTest, MB53282) {
    class MockAuditFile : public AuditFile {
    public:
        MockAuditFile() : AuditFile("testing"){};
        void test_mb53282() {
            ensure_open();
            close_and_rotate_log();
            ensure_open();
        }
    };

    MockAuditFile auditfile;
    auditfile.reconfigure(config);
    auditfile.test_mb53282();
}
