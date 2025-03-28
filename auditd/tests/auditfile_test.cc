/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "auditfile.h"

#include <dek/manager.h>
#include <fmt/format.h>
#include <folly/FileUtil.h>
#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <platform/platform_time.h>
#include <platform/strerror.h>
#include <platform/timeutils.h>
#include <deque>
#include <filesystem>
#include <fstream>

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

    auto files = findFilesWithPrefix(testdir, "testing");
    EXPECT_EQ(1, files.size());
}

static constexpr uint32_t rotation_time = 100;

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

    auto files = findFilesWithPrefix(testdir, "testing");
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

    auto files = findFilesWithPrefix(testdir, "testing");
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

    auto files = findFilesWithPrefix(testdir, "testing");
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

    auto files = findFilesWithPrefix(testdir, "testing");
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

    auto files = findFilesWithPrefix(testdir, "testing");
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

TEST_F(AuditFileTest, MB53282) {
    class MockAuditFile : public AuditFile {
    public:
        MockAuditFile() : AuditFile("testing") {
        }
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

TEST_F(AuditFileTest, PruneFiles) {
    class MockAuditFile : public AuditFile {
    public:
        explicit MockAuditFile(const std::filesystem::path& logdir)
            : AuditFile("PruneFiles") {
            set_log_directory(logdir.generic_string());
            for (int ii = 0; ii < 10; ++ii) {
                createAuditLogFile(
                        logdir, "PruneFiles", std::chrono::hours(ii));
            }
        }

        static void createAuditLogFile(const std::filesystem::path& logdir,
                                       std::string_view hostname,
                                       std::chrono::seconds seconds) {
            using namespace std::filesystem;
            auto ts = cb::time::timestamp(time(nullptr) - seconds.count())
                              .substr(0, 19);
            std::ranges::replace(ts, ':', '-');
            auto filename = fmt::format("{}-{}-audit.log", hostname, ts);
            auto path = logdir / fmt::format("{}-{}-audit.log", hostname, ts);
            FILE* fp = fopen(path.generic_string().c_str(), "w");
            if (!fp) {
                throw std::runtime_error(fmt::format(
                        "createAuditLogFile: Failed to create {}: {}",
                        path.generic_string(),
                        cb_strerror()));
            }
            fclose(fp);
            auto ftime = file_time_type::clock::now() - seconds;
            last_write_time(path, ftime);
        }

        void set_prune_age(std::chrono::seconds age) {
            using namespace std::chrono;
            prune_age = age;
            next_prune = steady_clock::now() - seconds(1);
        }

        std::deque<std::filesystem::path> get_log_files() {
            std::deque<std::filesystem::path> ret;
            for (const auto& p :
                 std::filesystem::directory_iterator(log_directory)) {
                if (is_regular_file(p.path())) {
                    ret.push_back(p.path());
                }
            }

            std::ranges::sort(ret);
            return ret;
        }
    };

    MockAuditFile auditfile(testdir);
    auto blueprint = auditfile.get_log_files();
    EXPECT_EQ(10, blueprint.size());

    // We don't have a prune time, so all files should be there
    auditfile.prune_old_audit_files();
    EXPECT_EQ(blueprint, auditfile.get_log_files());

    // If we set the prune time longer than all the files they should still
    // be there
    auditfile.set_prune_age(std::chrono::hours(9) + std::chrono::minutes{30});
    auditfile.prune_old_audit_files();
    EXPECT_EQ(blueprint, auditfile.get_log_files());

    // set the prune time between the two last files
    auditfile.set_prune_age(std::chrono::hours(8) + std::chrono::minutes{30});
    auditfile.prune_old_audit_files();
    blueprint.pop_front();
    EXPECT_EQ(blueprint, auditfile.get_log_files());

    // Verify that we nuke multiple old files by specifying a time
    // only leaving one file
    auditfile.set_prune_age(std::chrono::minutes{30});
    auditfile.prune_old_audit_files();
    while (blueprint.size() > 1) {
        blueprint.pop_front();
    }
    EXPECT_EQ(blueprint, auditfile.get_log_files());
}

TEST_F(AuditFileTest, TestDekRotation) {
    AuditFile auditfile("testing");
    auditfile.reconfigure(config);

    auto files = findFilesWithPrefix(testdir, "testing");
    EXPECT_EQ(0, files.size());

    for (int ii = 0; ii < 10; ++ii) {
        // Trigger an event to the audit dek configuration (which should
        // trigger the file to be rotated)
        cb::dek::Manager::instance().setActive(
                cb::dek::Entity::Audit,
                cb::crypto::DataEncryptionKey::generate());
        auditfile.ensure_open();
        auditfile.write_event_to_disk(event);
    }

    auditfile.close();

    files = findFilesWithPrefix(testdir, "testing");
    EXPECT_EQ(10, files.size());
}

/**
 * As part of configuration audit will try to create the log directory.
 * If this fails the "prune" path would crash due to an exception being
 * thrown as part of iterating the directory.
 */
TEST_F(AuditFileTest, MB65705_NonWritableLogDirectory) {
    std::filesystem::path logdir = std::filesystem::path(testdir) / "logdir";
    auto prune = folly::makeGuard([this] {
        std::filesystem::permissions(testdir,
                                     std::filesystem::perms::owner_all,
                                     std::filesystem::perm_options::replace);
    });

    class MockAuditFile : public AuditFile {
    public:
        explicit MockAuditFile(const std::filesystem::path& logdir)
            : AuditFile("PruneFiles") {
            set_log_directory(logdir.generic_string());
            prune_age = std::chrono::seconds{1};
            next_prune =
                    std::chrono::steady_clock::now() - std::chrono::seconds(1);
        };
    };

    // make the filesystem read only
    std::filesystem::permissions(testdir,
                                 std::filesystem::perms::owner_read,
                                 std::filesystem::perm_options::replace);

    // Ensure we don't crash when the log directory don't exists and can't
    // be created
    MockAuditFile auditfile(logdir);
    auditfile.prune_old_audit_files();
}
