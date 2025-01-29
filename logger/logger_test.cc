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

#include "logger_test_fixture.h"

#include <dek/manager.h>
#include <memcached/engine.h>
#include <valgrind/valgrind.h>

#ifndef WIN32
#include <sys/resource.h>
#endif

/**
 * Test that the new fmt-style formatting works
 */
TEST_F(SpdloggerTest, FmtStyleFormatting) {
    const uint32_t value = 0xdeadbeef;
    LOG_INFO("FmtStyleFormatting {:x}", value);
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1,
              countInFile(files.front(), "INFO FmtStyleFormatting deadbeef"));
}

/**
 * Test class for tests which wants to operate on multiple log files
 *
 * Initialize the logger with a 2k file rotation threshold
 */
class FileRotationTest : public SpdloggerTest {
protected:
    void SetUp() override {
        RemoveFiles();
        // Use a 2 k file size to make sure that we rotate :)
        config.log_level = spdlog::level::level_enum::debug;
        config.cyclesize = 2048;
        setUpLogger();
    }
};

/**
 * Log multiple messages, which will causes the files to rotate a few times.
 */
TEST_F(FileRotationTest, MultipleFilesTest) {
    cb::logger::shutdown();
    config.max_aggregated_size = 6 * 1024;
    setUpLogger();

    for (auto ii = 0; ii < 100; ii++) {
        LOG_DEBUG_RAW(
                "This is a textual log message that we want to repeat a "
                "number of times");
    }
    cb::logger::shutdown();

    files = cb::io::findFilesWithPrefix(config.filename);
    // sort the files so we know the order they appear
    std::sort(files.begin(), files.end());
    // This would have generated 6 files of log messages, but given that
    // we prune after 3 files we should have 3 files left (starting with
    // file id 3)
    ASSERT_EQ(3, files.size());
    for (std::size_t ii = 0; ii < files.size(); ++ii) {
        auto filename = std::filesystem::path(files[ii]).filename();
        EXPECT_EQ(fmt::format("spdlogger_test.00000{}.txt", 3 + ii),
                  filename.string());
    }
}

TEST_F(FileRotationTest, DekForceRotation) {
    auto* logger = cb::logger::get();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size());

    for (auto ii = 0; ii < 10; ii++) {
        logger->critical("This is a log messate: {}", ii);
        cb::dek::Manager::instance().setActive(cb::dek::Entity::Logs,
                                               cb::dek::SharedEncryptionKey{});
    }
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_LE(10, files.size());
}

#ifndef WIN32
/**
 * Test that it works as expected when running out of file
 * descriptors. This test won't run on Windows as they don't
 * have the same ulimit setting
 */
TEST_F(FileRotationTest, HandleOpenFileErrors) {
    if (RUNNING_ON_VALGRIND) {
        std::cerr << "Skipping test when running on valgrind" << std::endl;
        return;
    }

#ifdef UNDEFINED_SANITIZER
    // MB-28735: This test fails under UBSan, when spdlog fails to open a new
    // file (in custom_rotating_file_sink::_sink_it):
    //
    //     common.h:139:9: runtime error: member access within address <ADDR>
    //     which does not point to an object of type 'spdlog::spdlog_ex' <ADDR>:
    //     note: object has invalid vptr
    //
    // examing <ADDR> in a debugger indicates a valid object. Therefore skipping
    // this test under UBSan.
    std::cerr << "Skipping test when running on UBSan (MB-28735)\n";
    return;
#endif

    LOG_DEBUG_RAW("Hey, this is a test");
    cb::logger::flush();
    files = cb::io::findFilesWithPrefix(config.filename);
    EXPECT_EQ(1, files.size());

    // Bring down out open file limit to a more conservative level (to
    // save using up a huge number of user / system FDs (and speed up the test).
    rlimit rlim;
    ASSERT_EQ(0, getrlimit(RLIMIT_NOFILE, &rlim))
            << "Failed to get RLIMIT_NOFILE: " << strerror(errno);

    const auto current = rlim.rlim_cur;
    rlim.rlim_cur = 100;
    ASSERT_EQ(0, setrlimit(RLIMIT_NOFILE, &rlim))
            << "Failed to set RLIMIT_NOFILE: " << strerror(errno);

    // Eat up file descriptors
    std::vector<FILE*> fds;
    FILE* fp;
    while ((fp = fopen(files.front().c_str(), "r")) != nullptr) {
        fds.push_back(fp);
    }
    EXPECT_EQ(EMFILE, errno);

    // Keep on logging. This should cause the files to wrap
    for (auto ii = 0; ii < 100; ii++) {
        LOG_DEBUG_RAW(
                "This is a textual log message that we want to repeat a number "
                "of times");
    }

    LOG_DEBUG_RAW("HandleOpenFileErrors");
    cb::logger::flush();

    // We've just flushed the data to the file, so it should be possible
    // to find it in the file.
    char buffer[1024];
    bool found = false;
    while (fgets(buffer, sizeof(buffer), fds.front()) != nullptr) {
        if (strstr(buffer, "HandleOpenFileErrors") != nullptr) {
            found = true;
        }
    }

    EXPECT_TRUE(found) << files.front()
                       << " does not contain HandleOpenFileErrors";

    // close all of the file descriptors
    for (const auto& fp : fds) {
        fclose(fp);
    }
    fds.clear();

    // Verify that we didn't get a new file while we didn't have any
    // free file descriptors
    files = cb::io::findFilesWithPrefix(config.filename);
    EXPECT_EQ(1, files.size());

    // Add a log entry, and we should get a new file
    LOG_DEBUG_RAW("Logging to the next file");
    cb::logger::flush();

    files = cb::io::findFilesWithPrefix(config.filename);
    EXPECT_EQ(2, files.size());

    // Restore the filedescriptors
    rlim.rlim_cur = current;
    ASSERT_EQ(0, setrlimit(RLIMIT_NOFILE, &rlim))
            << "Failed to restore RLIMIT_NOFILE: " << strerror(errno);
}
#endif

/**
 * Test that the custom type Vbid (see memcached/vbucket.h) performs
 * its prefixing successfully whenever outputting a vBucket ID
 */
TEST_F(SpdloggerTest, VbidClassTest) {
    const Vbid value = Vbid(1023);
    LOG_INFO("VbidClassTest {}", value);
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "INFO VbidClassTest vb:1023"));
}

/**
 * MB-32688: Missing final log entries just before crash (or shutdown).
 *
 * Test that everything we attempt to log before we call shutdown is actually
 * flushed to a file.
 *
 * Test MAY pass if this race condition is present. Runtime is tuned for CV
 * machines which are generally much slower than dev machines. Anecdotal -
 * false positive rate on my dev machine (MB Pro 2017 - PCIe SSD) is ~1/1000.
 * This SHOULD be lower on CV machines as the flush command will take much
 * longer to execute (slower disks) which will back the file logger up more. Any
 * sporadic failures of this test likely mean a reintroduction of this race
 * condition and should be investigated.
 */
TEST_F(SpdloggerTest, ShutdownRace) {
    // We need the async logger for this test, shutdown the existing one and
    // create it.
    cb::logger::shutdown();
    RemoveFiles();
    config.unit_test = false;
    setUpLogger();

    // Back the file logger up with messages and flush commands.
    for (int i = 0; i < 100; i++) {
        // Post messages to the async logger - doesn't actually perform a flush,
        // but queues one on the async logger
        LOG_CRITICAL_RAW("a message");
        cb::logger::flush();
    }

    LOG_CRITICAL_RAW("We should see this msg");
    LOG_CRITICAL_RAW("and this one");
    // And this very long one
    auto str = std::string(50000, 'a');
    LOG_CRITICAL("{}", str);

    // Shutdown, process all messages in the queue, then return.
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "CRITICAL We should see this msg"));
    EXPECT_EQ(1, countInFile(files.front(), "CRITICAL and this one"));
    EXPECT_EQ(1, countInFile(files.front(), "CRITICAL " + str));
}
