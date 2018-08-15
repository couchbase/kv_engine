/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

/**
 * Unit tests for BucketLogger class.
 */

#include "bucket_logger_test.h"

#include "bucket_logger.h"

#include <programs/engine_testapp/mock_server.h>

void BucketLoggerTest::SetUp() {
    // Store the oldLogLevel for tearDown
    oldLogLevel = globalBucketLogger->level();

    SpdloggerTest::SetUp();
    globalBucketLogger =
            BucketLogger::createBucketLogger(globalBucketLoggerName);
}

void BucketLoggerTest::TearDown() {
    SpdloggerTest::TearDown();

    // Create a new console logger
    // Set the log level back to the previous level, to respect verbosity
    // setting
    cb::logger::createConsoleLogger();
    get_mock_server_api()->log->set_level(oldLogLevel);
    get_mock_server_api()->log->get_spdlogger()->spdlogGetter()->set_level(
            oldLogLevel);
    globalBucketLogger =
            BucketLogger::createBucketLogger(globalBucketLoggerName);
    globalBucketLogger->set_level(oldLogLevel);
}

void BucketLoggerTest::setUpLogger(const spdlog::level::level_enum level,
                                   const size_t cyclesize) {
    SpdloggerTest::setUpLogger(level, cyclesize);
    globalBucketLogger =
            BucketLogger::createBucketLogger(globalBucketLoggerName);
    globalBucketLogger->set_level(level);
}

/**
 * Test that the new fmt-style formatting works correctly
 */
TEST_F(BucketLoggerTest, FmtStyleFormatting) {
    globalBucketLogger->log(
            spdlog::level::level_enum::info, "{}", "formattedtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "INFO (No Engine) formattedtext"));
}

/**
 * Test that the EP_LOG_TRACE Macro works correctly
 */
TEST_F(BucketLoggerTest, TraceMacro) {
    // Set up trace level logger
    cb::logger::shutdown();
    setUpLogger(spdlog::level::level_enum::trace);

    EP_LOG_TRACE("{}", "formattedtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "TRACE (No Engine) formattedtext"));
}

/**
 * Test that the EP_LOG_DEBUG Macro works correctly
 */
TEST_F(BucketLoggerTest, DebugMacro) {
    EP_LOG_DEBUG("{}", "formattedtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "DEBUG (No Engine) formattedtext"));
}

/**
 * Test that the EP_LOG_INFO Macro works correctly
 */
TEST_F(BucketLoggerTest, InfoMacro) {
    EP_LOG_INFO("{}", "formattedtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "INFO (No Engine) formattedtext"));
}

/**
 * Test that the EP_LOG_WARN Macro works correctly
 */
TEST_F(BucketLoggerTest, WarnMacro) {
    EP_LOG_WARN("{}", "formattedtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1,
              countInFile(files.front(), "WARNING (No Engine) formattedtext"));
}

/**
 * Test that the EP_LOG_CRITICAL Macro works correctly
 */
TEST_F(BucketLoggerTest, CriticalMacro) {
    EP_LOG_CRITICAL("{}", "formattedtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1,
              countInFile(files.front(), "CRITICAL (No Engine) formattedtext"));
}

/**
 * Test that the EP_LOG_TRACE Macro works correctly without formatting
 */
TEST_F(BucketLoggerTest, TraceRawMacro) {
    // Set up trace level logger
    cb::logger::shutdown();
    setUpLogger(spdlog::level::level_enum::trace);

    EP_LOG_TRACE("rawtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "TRACE (No Engine) rawtext"));
}

/**
 * Test that the EP_LOG_DEBUG Macro works correctly without formatting
 */
TEST_F(BucketLoggerTest, DebugRawMacro) {
    EP_LOG_DEBUG("rawtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "DEBUG (No Engine) rawtext"));
}

/**
 * Test that the EP_LOG_INFO Macro works correctly without formatting
 */
TEST_F(BucketLoggerTest, InfoRawMacro) {
    EP_LOG_INFO("rawtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "INFO (No Engine) rawtext"));
}

/**
 * Test that the EP_LOG_WARN Macro works correctly without formatting
 */
TEST_F(BucketLoggerTest, WarnRawMacro) {
    EP_LOG_WARN("rawtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "WARNING (No Engine) rawtext"));
}

/**
 * Test that the EP_LOG_CRITICAL Macro works correctly without formatting
 */
TEST_F(BucketLoggerTest, CriticalRawMacro) {
    EP_LOG_CRITICAL("rawtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "CRITICAL (No Engine) rawtext"));
}
