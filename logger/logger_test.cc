/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "logger.h"

#include <gtest/gtest.h>
#include <memcached/engine.h>
#include <memcached/extension.h>
#include <platform/cbassert.h>
#include <platform/dirutils.h>
#include <platform/memorymap.h>
#include <valgrind/valgrind.h>

class SpdloggerTest : public ::testing::Test {
protected:
/*
 * Unset a few environment variables which affect how the logger works.
 * unsetenv() is not supported on Windows.
 */
#ifndef WIN32
    static void SetUpTestCase() {
        unsetenv("CB_MINIMIZE_LOGGER_SLEEPTIME");
        unsetenv("CB_MAXIMIZE_LOGGER_CYCLE_SIZE");
        unsetenv("CB_MAXIMIZE_LOGGER_BUFFER_SIZE");
    }
#endif

    void SetUp() override {
        RemoveFiles();

        // We don't want to deal with multiple files in the
        // basic tests. Set the file size to 20MB
        SetUpLogger(20 * 1024);
    }

    /**
     * Set up the logger
     *
     * @param cyclesize - the size to use before switching file
     */
    void SetUpLogger(size_t cyclesize) {
        cb::logger::Config config;
        config.filename = filename;
        config.cyclesize = cyclesize;
        config.buffersize = 8192;
        // Disable timebased flush
        config.sleeptime = 0;
        config.unit_test = true;
        config.console = false;

        const auto ret = cb::logger::initialize(config);
        EXPECT_FALSE(ret) << ret.get();
        cb::logger::get()->set_level(spdlog::level::level_enum::debug);
    }

    void RemoveFiles() {
        files = cb::io::findFilesWithPrefix(filename);
        for (const auto& file : files) {
            cb::io::rmrf(file);
        }
    }

    void TearDown() override {
        cb::logger::shutdown();
        RemoveFiles();
    }

    std::string getLogContents() {
        files = cb::io::findFilesWithPrefix(filename);
        std::ostringstream ret;

        for (const auto& file : files) {
            cb::MemoryMappedFile map(file.c_str(),
                                     cb::MemoryMappedFile::Mode::RDONLY);
            map.open();
            ret << std::string{reinterpret_cast<const char*>(map.getRoot()),
                               map.getSize()};
        }

        return ret.str();
    }

    std::vector<std::string> files;
    const std::string filename{"spdlogger_test"};
    const std::string openingHook = "---------- Opening logfile: ";
    const std::string closingHook = "---------- Closing logfile";
};

/**
 * Helper function - counts how many times a string appears in a file.
 *
 * @param file the name of the file
 * @param msg the message to searc hfor
 * @return the number of times we found the message in the file
 */
int countInFile(const std::string& file, const std::string& msg) {
    cb::MemoryMappedFile map(file.c_str(), cb::MemoryMappedFile::Mode::RDONLY);
    map.open();

    const auto* begin = reinterpret_cast<const char*>(map.getRoot());
    const auto* end = begin + map.getSize();

    int count = 0;
    while ((begin = std::search(begin, end, msg.begin(), msg.end())) != end) {
        ++count;
        begin += msg.size();
    }
    return count;
}

/**
 * Test that the printf-style of the logger still works
 */
TEST_F(SpdloggerTest, OldStylePrintf) {
    auto& logger = cb::logger::getLoggerDescriptor();
    const uint32_t value = 0xdeadbeef;
    logger.log(EXTENSION_LOG_INFO, nullptr, "OldStylePrintf %x", value);
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "INFO OldStylePrintf deadbeef"));
}

/**
 * Test that the new fmt-style formatting works
 */
TEST_F(SpdloggerTest, FmtStyleFormatting) {
    const uint32_t value = 0xdeadbeef;
    LOG_INFO("FmtStyleFormatting {:x}", value);
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1,
              countInFile(files.front(), "INFO FmtStyleFormatting deadbeef"));
}

/**
 * Tests writing the maximum allowed message to file. Messages are held in
 * a buffer of size 2048, which allows for a message of size 2047 characters
 * (excluding logger formatting and null terminator).
 *
 * (old printf style)
 */
TEST_F(SpdloggerTest, LargeMessageTest) {
    const std::string message(2047,
                              'x'); // max message size is 2047 + 1 for '\0'
    auto& logger = cb::logger::getLoggerDescriptor();
    logger.log(EXTENSION_LOG_DEBUG, nullptr, message.c_str());
    cb::logger::shutdown();

    files = cb::io::findFilesWithPrefix(filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), message));
}

/**
 * Tests the message cropping feature.
 * Crops a message which wouldn't fit in the message buffer.
 *
 * (old printf style)
 */
TEST_F(SpdloggerTest, LargeMessageWithCroppingTest) {
    const std::string message(2048, 'x'); // just 1 over max message size
    std::string cropped(2047 - strlen(" [cut]"), 'x');
    cropped.append(" [cut]");

    auto& logger = cb::logger::getLoggerDescriptor();
    logger.log(EXTENSION_LOG_DEBUG, nullptr, message.c_str());
    cb::logger::shutdown();

    files = cb::io::findFilesWithPrefix(filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), cropped));
}

/**
 * Most basic test. Open a logfile, write a log message, close the logfile and
 * check if the hooks appear in the file.
 */
TEST_F(SpdloggerTest, BasicHooksTest) {
    cb::logger::shutdown();

    files = cb::io::findFilesWithPrefix(filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), openingHook));
    EXPECT_EQ(1, countInFile(files.front(), closingHook));
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
        SetUpLogger(2048);
    }
};

/**
 * Log multiple messages, which will causes the files to rotate a few times.
 * Test if the hooks appear in each file.
 */
TEST_F(FileRotationTest, MultipleFilesTest) {
    const char* message =
            "This is a textual log message that we want to repeat a number of "
            "times: {}";
    for (auto ii = 0; ii < 100; ii++) {
        LOG_DEBUG(message, ii);
    }
    cb::logger::shutdown();

    files = cb::io::findFilesWithPrefix(filename);
    EXPECT_LT(1, files.size());
    for (auto& file : files) {
        EXPECT_EQ(1, countInFile(file, openingHook))
                << "Missing open hook in file: " << file;
        EXPECT_EQ(1, countInFile(file, closingHook))
                << "Missing closing hook in file: " << file;
    }
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

    LOG_DEBUG("Hey, this is a test");
    cb::logger::flush();
    files = cb::io::findFilesWithPrefix(filename);
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
    const char* message =
            "This is a textual log message that we want to repeat a number of "
            "times {}";
    for (auto ii = 0; ii < 100; ii++) {
        LOG_DEBUG(message, ii);
    }

    LOG_DEBUG("HandleOpenFileErrors");
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
    files = cb::io::findFilesWithPrefix(filename);
    EXPECT_EQ(1, files.size());

    // Add a log entry, and we should get a new file
    LOG_DEBUG("Logging to the next file");
    cb::logger::flush();

    files = cb::io::findFilesWithPrefix(filename);
    EXPECT_EQ(2, files.size());

    // Restore the filedescriptors
    rlim.rlim_cur = current;
    ASSERT_EQ(0, setrlimit(RLIMIT_NOFILE, &rlim))
            << "Failed to restore RLIMIT_NOFILE: " << strerror(errno);
}
#endif

class DedupeSinkTest : public SpdloggerTest {};

/**
 * Tests the functionality of the dedupe_sink by sending it the same message
 * 100 times. Once this is done, the log file should contain the string
 * "Message repeated 100 times".
 */
TEST_F(DedupeSinkTest, BasicTest) {
    const std::string message{"This message will be repeated 100 times!"};
    const std::string dedupeMessage{"Message repeated 100 times"};

    for (auto ii = 0; ii < 100; ii++) {
        LOG_WARNING(message);
    }
    cb::logger::shutdown();

    files = cb::io::findFilesWithPrefix(filename);
    ASSERT_EQ(1, files.size()) << "Did not expect log rotation to happen";
    EXPECT_EQ(1, countInFile(files.front(), message));
    EXPECT_EQ(1, countInFile(files.front(), dedupeMessage));
}

/**
 * No dedupe message should be printed if the message appeared only once
 */
TEST_F(DedupeSinkTest, MessageLoggedOnceTest) {
    const std::string message{"This message will be logged just once!"};
    const std::string dedupeMessage{"Message repeated"};

    LOG_WARNING(message);
    cb::logger::shutdown();

    files = cb::io::findFilesWithPrefix(filename);
    ASSERT_EQ(1, files.size()) << "Did not expect log rotation to happen";
    EXPECT_EQ(1, countInFile(files.front(), message));
    EXPECT_EQ(0, countInFile(files.front(), dedupeMessage));
}

/**
 * The dedupe message should trigger if the message appeared twice
 */
TEST_F(DedupeSinkTest, MessageLoggedTwiceTest) {
    const std::string message{"This message will be repeated twice!"};
    const std::string dedupeMessage{"Message repeated 2 times"};

    LOG_WARNING(message);
    LOG_WARNING(message);
    cb::logger::shutdown();

    files = cb::io::findFilesWithPrefix(filename);
    ASSERT_EQ(1, files.size()) << "Did not expect log rotation to happen";
    EXPECT_EQ(1, countInFile(files.front(), message))
            << "Log contents:" << std::endl
            << getLogContents();
    EXPECT_EQ(1, countInFile(files.front(), dedupeMessage))
            << "Log contents:" << std::endl
            << getLogContents();
}

/**
 * Flushing the buffer should "reset" the deduplication logic as we don't
 * want the log to look like:
 *
 *     message x repeated 10 times
 *     message x repeated 11 times
 *     message x repeated 15 times
 *
 * It makes it harder to read the logs.. Does it mean that it happened
 * 1 time or 11 times between the first two lines etc.
 */
TEST_F(DedupeSinkTest, MessageLoggedTwiceWithFlushTest) {
    const std::string message{"This message will be written and flushed!"};
    const std::string dedupeMessage{"Message repeated"};

    for (auto i = 0; i < 10; i++) {
        LOG_WARNING(message);
        cb::logger::flush();
    }

    files = cb::io::findFilesWithPrefix(filename);
    ASSERT_EQ(1, files.size()) << "Did not expect log rotation to happen";
    EXPECT_EQ(10, countInFile(files.front(), message))
            << "Log contents:" << std::endl
            << getLogContents();
    EXPECT_EQ(0, countInFile(files.front(), dedupeMessage))
            << "Log contents:" << std::endl
            << getLogContents();
}
