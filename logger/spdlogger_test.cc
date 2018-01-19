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

#include <extensions/protocol_extension.h>
#include <gtest/gtest.h>
#include <memcached/extension.h>
#include <platform/cbassert.h>
#include <platform/dirutils.h>
#include <fstream>

static EXTENSION_LOGGER_DESCRIPTOR* logger;

static EXTENSION_LOG_LEVEL get_log_level(void) {
    return EXTENSION_LOG_DETAIL;
}

static bool register_extension(extension_type_t type, void* extension) {
    cb_assert(type == EXTENSION_LOGGER);
    logger = reinterpret_cast<EXTENSION_LOGGER_DESCRIPTOR*>(extension);
    return true;
}

static void register_callback(ENGINE_HANDLE* eh,
                              ENGINE_EVENT_TYPE type,
                              EVENT_CALLBACK cb,
                              const void* cb_data) {
}

static SERVER_HANDLE_V1* get_server_api(void) {
    static bool init = false;
    static SERVER_CORE_API core_api = {};
    static SERVER_COOKIE_API server_cookie_api = {};
    static SERVER_STAT_API server_stat_api = {};
    static SERVER_LOG_API server_log_api = {};
    static SERVER_EXTENSION_API extension_api = {};
    static SERVER_CALLBACK_API callback_api = {};
    static ALLOCATOR_HOOKS_API hooks_api = {};
    static SERVER_HANDLE_V1 rv;

    if (!init) {
        init = true;

        core_api.parse_config = parse_config;
        server_log_api.get_level = get_log_level;
        extension_api.register_extension = register_extension;
        callback_api.register_callback = register_callback;

        rv.interface = 1;
        rv.core = &core_api;
        rv.stat = &server_stat_api;
        rv.extension = &extension_api;
        rv.callback = &callback_api;
        rv.log = &server_log_api;
        rv.cookie = &server_cookie_api;
        rv.alloc_hooks = &hooks_api;
    }

    return &rv;
}

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

        cb::logger::Config config;
        config.filename = filename;
        config.cyclesize = 2048;
        config.buffersize = 8192;
        config.sleeptime = 1;
        config.unit_test = true;

        const auto ret = cb::logger::initialize(config, get_server_api);
        EXPECT_FALSE(ret) << ret.get();
    }

    void RemoveFiles() {
        files = cb::io::findFilesWithPrefix(filename);
        for (const auto file : files) {
            cb::io::rmrf(file);
        }
    }

    std::vector<std::string> files;
    const std::string filename{"spdlogger_test"};
    const std::string openingHook = "---------- Opening logfile: ";
    const std::string closingHook = "---------- Closing logfile";
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

/**
 * Tests writing the maximum allowed message to file. Messages are held in
 * a buffer of size 2048, which allows for a message of size 2047 characters
 * (excluding logger formatting and null terminator).
 */
TEST_F(SpdloggerTest, LargeMessageTest) {
    std::string message(2047, 'x'); // max message size is 2047 + 1 for '\0'
    logger->log(EXTENSION_LOG_DETAIL, nullptr, message.c_str());
    logger->shutdown(false);

    files = cb::io::findFilesWithPrefix(filename);

    auto found = false;
    for (auto& file : files) {
        auto messageCount = countInFile(file, message);
        if (messageCount == 1) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found);
}

/**
 * Tests the message cropping feature.
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

    EXPECT_TRUE(found);
}

/**
 * Most basic test. Open a logfile, write a log message, close the logfile and
 * check if the hooks appear in the file.
 */
TEST_F(SpdloggerTest, BasicHooksTest) {
    logger->shutdown(false);

    files = cb::io::findFilesWithPrefix(filename);
    EXPECT_EQ(1, files.size());

    auto openingHookCount = countInFile(files.front(), openingHook);
    auto closingHookCount = countInFile(files.front(), closingHook);

    EXPECT_EQ(1, openingHookCount);
    EXPECT_EQ(1, closingHookCount);
}

/**
 * Log multiple messages, which will causes the files to rotate a few times.
 * Test if the hooks appear in each file.
 */
TEST_F(SpdloggerTest, MultipleFilesTest) {
    const std::string message{
            "This is a textual log message that we want to repeat a number of "
            "times"};
    for (auto ii = 0; ii < 100; ii++) {
        logger->log(EXTENSION_LOG_DETAIL, nullptr, message.c_str());
    }
    logger->shutdown(false);

    files = cb::io::findFilesWithPrefix(filename);
    EXPECT_LT(1, files.size());
    for (auto file : files) {
        auto openingHookCount = countInFile(file, openingHook);
        auto closingHookCount = countInFile(file, closingHook);
        EXPECT_EQ(1, openingHookCount) << "Missing open hook in file: " << file;
        EXPECT_EQ(1, closingHookCount) << "Missing closing hook in file: "
                                       << file;
    }
}
