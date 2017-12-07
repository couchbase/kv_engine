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

#include "logger_test_common.h"

#include <extensions/loggers/custom_rotating_file_sink.h>
#include <gtest/gtest.h>
#include <platform/dirutils.h>
#include <spdlog/spdlog.h>
#include <fstream>

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

    virtual void SetUp() {
        files = cb::io::findFilesWithPrefix("spdlogger_test");
        if (!files.empty()) {
            remove_files(files);
        }

        LoggerConfig config;
        config.filename = "spdlogger_test";
        config.cyclesize = 2048;
        config.buffersize = 8192;
        config.sleeptime = 1;
        config.unit_test = true;

        ret = file_logger_initialize(config, get_server_api);
        EXPECT_EQ(EXTENSION_SUCCESS, ret);
    }
    EXTENSION_ERROR_CODE ret;
    std::vector<std::string> files;
};

/* Helper function - counts how many times a string appears in a file. */
int countInFile(const std::string& file, const std::string& msg) {
    std::ifstream inFile;

    inFile.open(file);
    if (!inFile) {
        return -1;
    }

    auto count = 0;
    std::string line;
    while (getline(inFile, line)) {
        if (line.find(msg, 0) != std::string::npos) {
            count++;
        }
    }
    inFile.close();
    return count;
}

/* Tests writing the maximum allowed message to file. Messages are held in
 * a buffer of size 2048, which allows for a message of size 2047 characters
 * (excluding logger formatting and null terminator).
 */
TEST_F(SpdloggerTest, LargeMessageTest) {
    std::string message(2047, 'x'); // max message size is 2047 + 1 for '\0'
    logger->log(EXTENSION_LOG_DETAIL, nullptr, message.c_str());
    logger->shutdown(false);

    files = cb::io::findFilesWithPrefix("spdlogger_test");

    auto found = false;
    for (auto& file : files) {
        auto messageCount = countInFile(file, message);
        if (messageCount == 1) {
            found = true;
            break;
        }
    }
    remove_files(files);
    EXPECT_EQ(true, found);
}

/* Tests the message cropping feature.
 * Crops a message which wouldn't fit in the message buffer.
 */
TEST_F(SpdloggerTest, LargeMessageWithCroppingTest) {
    std::string message(2048, 'x'); // just 1 over max message size
    std::string cropped(2047 - strlen(" [cut]"), 'x');
    cropped.append(" [cut]");

    logger->log(EXTENSION_LOG_DETAIL, nullptr, message.c_str());
    logger->shutdown(false);

    files = cb::io::findFilesWithPrefix("spdlogger_test");

    auto found = false;
    for (auto& file : files) {
        auto messageCount = countInFile(file, cropped);
        if (messageCount == 1) {
            found = true;
            break;
        }
    }
    remove_files(files);
    EXPECT_EQ(true, found);
}

class CustomRotatingFileSinkTest : public SpdloggerTest {
protected:
    void SetUp() override {
        files = cb::io::findFilesWithPrefix(filename);

        if (!files.empty()) {
            remove_files(files);
        }

        LoggerConfig config;
        config.filename = filename;
        config.cyclesize = 512;
        config.buffersize = 8192;

        try {
            auto test_sink = std::make_shared<custom_rotating_file_sink_mt>(
                    config.filename, config.cyclesize, 5, log_pattern);
            auto file_logger = spdlog::create_async(
                    loggername, test_sink, config.buffersize);
        } catch (const spdlog::spdlog_ex& ex) {
            std::cerr << "Log initialization failed: " << ex.what()
                      << std::endl;
        }
        spdlog::set_level(spdlog::level::trace);
        spdlog::set_pattern(log_pattern);
    }
    std::vector<std::string> files;
    const std::string filename = "test_file";
    const std::string loggername = "test_file_logger";
    const std::string openingHook = "---------- Opening logfile: ";
    const std::string closingHook = "---------- Closing logfile";
};

/* Most basic test. Open a logfile, write a log message, close the logfile and
 * check if the hooks appear in the file.
 */
TEST_F(CustomRotatingFileSinkTest, BasicHooksTest) {
    auto logger = spdlog::get(loggername);
    logger->info("This message should trigger the hooks!");
    spdlog::drop_all();
    logger.reset();

    files = cb::io::findFilesWithPrefix(filename);
    EXPECT_EQ(1, files.size());

    auto openingHookCount = countInFile(files.front(), openingHook);
    auto closingHookCount = countInFile(files.front(), closingHook);

    remove_files(files);
    EXPECT_EQ(1, openingHookCount);
    EXPECT_EQ(1, closingHookCount);
}

/* Log multiple messages, which will causes the files to rotate a few times.
 * Test if the hooks appear in each file.
 */
TEST_F(CustomRotatingFileSinkTest, MultipleFilesTest) {
    auto logger = spdlog::get(loggername);
    for (auto i = 0; i < 20; i++) {
        logger->info("Log this message a few times...");
    }
    spdlog::drop_all();
    logger.reset();

    files = cb::io::findFilesWithPrefix(filename);
    EXPECT_LT(1, files.size());
    for (auto file : files) {
        auto openingHookCount = countInFile(file, openingHook);
        auto closingHookCount = countInFile(file, closingHook);
        EXPECT_EQ(1, openingHookCount) << "Missing open hook in file: " << file;
        EXPECT_EQ(1, closingHookCount) << "Missing closing hook in file: "
                                       << file;
    }
    remove_files(files);
}
