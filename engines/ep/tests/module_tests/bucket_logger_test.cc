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

/**
 * Unit tests for BucketLogger class.
 */

#include "bucket_logger_test.h"

#include "bucket_logger.h"
#include "thread_gate.h"

#include <programs/engine_testapp/mock_server.h>

BucketLoggerTest::BucketLoggerTest() {
    // Write to a different file in case other parent class fixtures are running
    // in parallel
    config.filename = "bucket_logger_test";
}

void BucketLoggerTest::SetUp() {
    // Store the oldLogLevel for tearDown
    oldLogLevel = getGlobalBucketLogger()->level();

    if (getGlobalBucketLogger()) {
        getGlobalBucketLogger()->unregister();
    }
    SpdloggerTest::SetUp();
    getGlobalBucketLogger() =
            BucketLogger::createBucketLogger(globalBucketLoggerName);
}

void BucketLoggerTest::TearDown() {
    SpdloggerTest::TearDown();

    // Create a new console logger
    // Set the log level back to the previous level, to respect verbosity
    // setting
    cb::logger::createConsoleLogger();
    cb::logger::get()->set_level(oldLogLevel);
    getGlobalBucketLogger() =
            BucketLogger::createBucketLogger(globalBucketLoggerName);
    getGlobalBucketLogger()->set_level(oldLogLevel);
}

void BucketLoggerTest::setUpLogger() {
    SpdloggerTest::setUpLogger();
    getGlobalBucketLogger() =
            BucketLogger::createBucketLogger(globalBucketLoggerName);
    getGlobalBucketLogger()->set_level(config.log_level);
}

/**
 * Test that the EP_LOG_TRACE Macro works correctly
 */
TEST_F(BucketLoggerTest, TraceMacro) {
    // Set up trace level logger
    cb::logger::shutdown();
    config.log_level = spdlog::level::level_enum::trace;
    setUpLogger();

    EP_LOG_TRACE("{}", "formattedtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "TRACE (No Engine) formattedtext"));
}

/**
 * Test that the EP_LOG_DEBUG Macro works correctly
 */
TEST_F(BucketLoggerTest, DebugMacro) {
    EP_LOG_DEBUG("{}", "formattedtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "DEBUG (No Engine) formattedtext"));
}

/**
 * Test that the EP_LOG_INFO Macro works correctly
 */
TEST_F(BucketLoggerTest, InfoMacro) {
    EP_LOG_INFO("{}", "formattedtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "INFO (No Engine) formattedtext"));
}

/**
 * Test that the EP_LOG_WARN Macro works correctly
 */
TEST_F(BucketLoggerTest, WarnMacro) {
    EP_LOG_WARN("{}", "formattedtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
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
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1,
              countInFile(files.front(), "CRITICAL (No Engine) formattedtext"));
}

/**
 * Test that the EP_LOG_TRACE_RAW macro works correctly without formatting
 */
TEST_F(BucketLoggerTest, TraceRawMacro) {
    // Set up trace level logger
    cb::logger::shutdown();
    config.log_level = spdlog::level::level_enum::trace;
    setUpLogger();

    EP_LOG_TRACE_RAW("rawtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "TRACE (No Engine) rawtext"));
}

/**
 * Test that the EP_LOG_DEBUG_RAW macro works correctly without formatting
 */
TEST_F(BucketLoggerTest, DebugRawMacro) {
    EP_LOG_DEBUG_RAW("rawtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "DEBUG (No Engine) rawtext"));
}

/**
 * Test that the EP_LOG_INFO macro works correctly without formatting
 */
TEST_F(BucketLoggerTest, InfoRawMacro) {
    EP_LOG_INFO_RAW("rawtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "INFO (No Engine) rawtext"));
}

/**
 * Test that the EP_LOG_WARN_RAW macro works correctly without formatting
 */
TEST_F(BucketLoggerTest, WarnRawMacro) {
    EP_LOG_WARN_RAW("rawtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "WARNING (No Engine) rawtext"));
}

/**
 * Test that the EP_LOG_CRITICAL_RAW macro works correctly without formatting
 */
TEST_F(BucketLoggerTest, CriticalRawMacro) {
    EP_LOG_CRITICAL_RAW("rawtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "CRITICAL (No Engine) rawtext"));
}

/**
 * Regression test for MB-46900 - don't error if the logger prefix contains
 * characters which could be interpreted as fmtlib format strings.
 */
TEST_F(BucketLoggerTest, FmtlibPrefix) {
    getGlobalBucketLogger()->prefix = "abc:{def}";
    EP_LOG_INFO("Test {}", "abc");
}

/**
 * Regression test for MB-46900 - don't error if the logger prefix contains
 * characters which is an invalid fmtlib format string.
 */
TEST_F(BucketLoggerTest, IllegalFmtlibPrefix) {
    getGlobalBucketLogger()->prefix = "abc:{def";
    EP_LOG_INFO("Test {}", "abc");
}

/**
 * Test class with ThreadGates that allows us to test the destruction of a
 * BucketLogger when it is owned solely by the background flushing thread.
 */
class FlushRaceLogger : public BucketLogger {
public:
    FlushRaceLogger(const std::string& name, ThreadGate& tg1, ThreadGate& tg2)
        : BucketLogger(name), tg1(tg1), tg2(tg2) {
    }

protected:
    void flush_() override {
        // Shared_ptr should still be owned in our test code block
        tg1.threadUp();

        // Wait until our test code block releases it's ownership of the
        // shared_ptr so that we can destruct this FlushRaceLogger from
        // spdlog::registry::flush_all
        tg2.threadUp();
    }
    ThreadGate& tg1;
    ThreadGate& tg2;
};

TEST_F(BucketLoggerTest, RaceInFlushDueToPtrOwnership) {
    // We need the async logger for this test, shutdown the existing one and
    // create it.
    cb::logger::shutdown();
    RemoveFiles();
    config.unit_test = false;
    setUpLogger();

    ThreadGate tg1(2);
    ThreadGate tg2(2);
    {
        auto sharedLogger =
                std::make_shared<FlushRaceLogger>("flushRaceLogger", tg1, tg2);
        // We hit the normal constructor of the BucketLogger via the
        // FlushRaceLogger so we need to manually register our logger
        cb::logger::registerSpdLogger(sharedLogger);

        // Wait for the background flushing thread to enter our FlushRaceLoggers
        // flush_ function.
        tg1.threadUp();
    }

    // We've just released our ownership of the shared pointer, signal the
    // FlushRaceLogger to continue and exit the flush_ function. This will then
    // trigger destruction of the FlushRaceLogger which will unregister the
    // logger via ~BucketLogger.
    tg2.threadUp();

    // Shutdown of the logger will keep the ThreadGates in scope until the flush
    // has finished as it locks on the logger_map_mutex_ in spdlog::registry
    cb::logger::shutdown();
}
