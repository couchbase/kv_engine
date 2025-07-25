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
        cb::logger::unregisterSpdLogger(getGlobalBucketLogger()->name());
    }
    getGlobalBucketLogger().reset();

    SpdloggerTest::SetUp();
}

void BucketLoggerTest::TearDown() {
    // If the test failed, print the actual log contents for debugging.
    if (HasFailure()) {
        for (auto& file : cb::io::findFilesWithPrefix(config.filename)) {
            FAIL() << "---" << file << "---\n"
                   << cb::io::loadFile(file) << "---END---\n";
        }
    }

    // Resetting the global bucket logger will drop the reference to the logger
    // in the core.
    if (getGlobalBucketLogger()) {
        cb::logger::unregisterSpdLogger(getGlobalBucketLogger()->name());
    }
    getGlobalBucketLogger().reset();

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
    getGlobalBucketLogger().reset();
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
    EXPECT_EQ(1, countInFile(files.front(), "TRACE formattedtext"));
}

/**
 * Test that the EP_LOG_DEBUG Macro works correctly
 */
TEST_F(BucketLoggerTest, DebugMacro) {
    EP_LOG_DEBUG("{}", "formattedtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "DEBUG formattedtext"));
}

/**
 * Test that the EP_LOG_INFO Macro works correctly
 */
TEST_F(BucketLoggerTest, InfoMacro) {
    EP_LOG_INFO("{}", "formattedtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "INFO formattedtext"));
}

/**
 * Test that the EP_LOG_WARN Macro works correctly
 */
TEST_F(BucketLoggerTest, WarnMacro) {
    EP_LOG_WARN("{}", "formattedtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "WARNING formattedtext"));
}

/**
 * Test that the EP_LOG_CRITICAL Macro works correctly
 */
TEST_F(BucketLoggerTest, CriticalMacro) {
    EP_LOG_CRITICAL("{}", "formattedtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "CRITICAL formattedtext"));
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
    EXPECT_EQ(1, countInFile(files.front(), "TRACE rawtext"));
}

/**
 * Test that the EP_LOG_DEBUG_RAW macro works correctly without formatting
 */
TEST_F(BucketLoggerTest, DebugRawMacro) {
    EP_LOG_DEBUG_RAW("rawtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "DEBUG rawtext"));
}

/**
 * Test that the EP_LOG_INFO macro works correctly without formatting
 */
TEST_F(BucketLoggerTest, InfoRawMacro) {
    EP_LOG_INFO_RAW("rawtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "INFO rawtext"));
}

/**
 * Test that the EP_LOG_WARN_RAW macro works correctly without formatting
 */
TEST_F(BucketLoggerTest, WarnRawMacro) {
    EP_LOG_WARN_RAW("rawtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "WARNING rawtext"));
}

/**
 * Test that the EP_LOG_CRITICAL_RAW macro works correctly without formatting
 */
TEST_F(BucketLoggerTest, CriticalRawMacro) {
    EP_LOG_CRITICAL_RAW("rawtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "CRITICAL rawtext"));
}

/**
 * Test that the EP_LOG_INFO_CTX macro works correctly
 */
TEST_F(BucketLoggerTest, InfoContextMacro) {
    EP_LOG_INFO_CTX("log message", {"a", 1});
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "INFO log message {\"a\":1}"));
}

/**
 * Test that the connection ID is inserted as the first element in the context,
 * with the correct key.
 */
TEST_F(BucketLoggerTest, InfoContextMacroWithConnId) {
    getGlobalBucketLogger()->setConnectionId(1);
    EP_LOG_INFO_CTX("log message", {"a", 1});
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1,
              countInFile(files.front(),
                          "INFO log message {\"conn_id\":1,\"a\":1}"))
            << "Not found in " << files.front();
}

/**
 * Test that the EP_LOG_INFO_CTX macro handles {} in the message.
 */
TEST_F(BucketLoggerTest, InfoContextMacroWithCurlyBraces) {
    EP_LOG_INFO_CTX("log {escape} message", {"a", 1});
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(
            1,
            countInFile(files.front(), "INFO log [escape] message {\"a\":1}"));
}

/**
 * Regression test for MB-46900 - don't error if the logger prefix contains
 * characters which could be interpreted as fmtlib format strings.
 */
TEST_F(BucketLoggerTest, FmtlibPrefix) {
    getGlobalBucketLogger()->setPrefix({}, "abc:{def}");
    EP_LOG_INFO("Test {}", "abc");
}

/**
 * Regression test for MB-46900 - don't error if the logger prefix contains
 * characters which is an invalid fmtlib format string.
 */
TEST_F(BucketLoggerTest, IllegalFmtlibPrefix) {
    getGlobalBucketLogger()->setPrefix({}, "abc:{def");
    EP_LOG_INFO("Test {}", "abc");
}

TEST_F(BucketLoggerTest, LogJsonInitialization) {
    cb::logger::Json json = {{"a", 0}};
    EP_LOG_INFO_CTX("log 1", {"a", 0});
    EP_LOG_INFO_CTX("log 2", json);
    EP_LOG_INFO_CTX("log 3", std::move(json));
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "INFO log 1 {\"a\":0}"))
            << "Not found in " << files.front();
    EXPECT_EQ(1, countInFile(files.front(), "INFO log 2 {\"a\":0}"))
            << "Not found in " << files.front();
    EXPECT_EQ(1, countInFile(files.front(), "INFO log 3 {\"a\":0}"))
            << "Not found in " << files.front();
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
    getGlobalBucketLogger().reset();
    shutdownLoggerAndRemoveFiles();
    config.unit_test = false;
    setUpLogger();

    ThreadGate tg1(2);
    ThreadGate tg2(2);
    {
        auto sharedLogger =
                std::make_shared<FlushRaceLogger>("flushRaceLogger", tg1, tg2);
        // We hit the normal constructor of the BucketLogger via the
        // FlushRaceLogger so we need to manually register our logger
        sharedLogger->tryRegister();

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

TEST_F(BucketLoggerTest, PrefixLogger) {
    auto prefixLogger = std::make_shared<cb::logger::PrefixLogger>(
            "prefix_logger", cb::logger::get());
    prefixLogger->setPrefix(cb::logger::Json({{"prefix", "test"}}),
                            "prefix=test");

    prefixLogger->log(spdlog::level::info, "my message");
    prefixLogger->logWithContext(
            spdlog::level::info, "my message", {{"key", "value"}});

    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(
            1,
            countInFile(files.front(), "INFO my message {\"prefix\":\"test\"}"))
            << getLogContents();
    EXPECT_EQ(
            1,
            countInFile(
                    files.front(),
                    "INFO my message {\"prefix\":\"test\",\"key\":\"value\"}"))
            << getLogContents();
}
