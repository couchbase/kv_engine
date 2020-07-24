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

#include "../mock/mock_dcp_conn_map.h"
#include "../mock/mock_dcp_producer.h"
#include "bucket_logger.h"
#include "dcp/producer.h"
#include "evp_store_single_threaded_test.h"
#include "thread_gate.h"

#include <programs/engine_testapp/mock_server.h>

#include "spdlog/spdlog.h"

void BucketLoggerTest::SetUp() {
    // Store the oldLogLevel for tearDown
    oldLogLevel = globalBucketLogger->level();

    if (globalBucketLogger) {
        globalBucketLogger->unregister();
    }
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
    get_mock_server_api()->log->get_spdlogger()->set_level(oldLogLevel);
    globalBucketLogger =
            BucketLogger::createBucketLogger(globalBucketLoggerName);
    globalBucketLogger->set_level(oldLogLevel);
}

void BucketLoggerTest::setUpLogger() {
    SpdloggerTest::setUpLogger();
    globalBucketLogger =
            BucketLogger::createBucketLogger(globalBucketLoggerName);
    globalBucketLogger->set_level(config.log_level);
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
 * Test that the EP_LOG_TRACE Macro works correctly without formatting
 */
TEST_F(BucketLoggerTest, TraceRawMacro) {
    // Set up trace level logger
    cb::logger::shutdown();
    config.log_level = spdlog::level::level_enum::trace;
    setUpLogger();

    EP_LOG_TRACE("rawtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "TRACE (No Engine) rawtext"));
}

/**
 * Test that the EP_LOG_DEBUG Macro works correctly without formatting
 */
TEST_F(BucketLoggerTest, DebugRawMacro) {
    EP_LOG_DEBUG("rawtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "DEBUG (No Engine) rawtext"));
}

/**
 * Test that the EP_LOG_INFO Macro works correctly without formatting
 */
TEST_F(BucketLoggerTest, InfoRawMacro) {
    EP_LOG_INFO("rawtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "INFO (No Engine) rawtext"));
}

/**
 * Test that the EP_LOG_WARN Macro works correctly without formatting
 */
TEST_F(BucketLoggerTest, WarnRawMacro) {
    EP_LOG_WARN("rawtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "WARNING (No Engine) rawtext"));
}

/**
 * Test that the EP_LOG_CRITICAL Macro works correctly without formatting
 */
TEST_F(BucketLoggerTest, CriticalRawMacro) {
    EP_LOG_CRITICAL("rawtext");
    cb::logger::shutdown();
    files = cb::io::findFilesWithPrefix(config.filename);
    ASSERT_EQ(1, files.size()) << "We should only have a single logfile";
    EXPECT_EQ(1, countInFile(files.front(), "CRITICAL (No Engine) rawtext"));
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

/**
 * Test that we adjust log levels correctly for FTS connections.
 */
class FtsLoggerTest : virtual public SingleThreadedKVBucketTest {};

TEST_F(FtsLoggerTest, testDynamicChanges) {
    // Set default log level to err instead of critical so that we can test
    // log level changes (as the fts default level is critical).
    get_mock_server_api()->log->set_level(spdlog::level::level_enum::err);

    auto startLevel = globalBucketLogger->level();
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Default state is to blacklist FTS connection logs
    ASSERT_TRUE(engine->getConfiguration().isDcpBlacklistFtsConnectionLogs());

    // Create our FTS Producer and manually add it to the DcpConnMap which is
    // responsible for passing down config changes. When we create the Producer
    // we create a logger for it. As the name of the producer implies it's an
    // FTS producer we set the log level and unregister it from the spdlog
    // registry.
    auto producer =
            std::make_shared<MockDcpProducer>(*engine,
                                              cookie,
                                              DcpProducer::ftsConnectionName,
                                              /*flags*/ 0);
    MockDcpConnMap& mockConnMap =
            static_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    mockConnMap.addConn(cookie, producer);

    // The logger should default to critical level
    EXPECT_EQ(spdlog::level::level_enum::critical,
              producer->getLogger().level());

    // Changing global log levels should not change that of the logger as it is
    // not registered.
    get_mock_server_api()->log->set_level(spdlog::level::level_enum::off);
    EXPECT_EQ(spdlog::level::level_enum::critical,
              producer->getLogger().level());
    get_mock_server_api()->log->set_level(startLevel);

    // Set our config which should update the log level and register the logger
    engine->getConfiguration().setDcpBlacklistFtsConnectionLogs(false);
    EXPECT_EQ(globalBucketLogger->level(), producer->getLogger().level());

    // Logger is registered so a log level change updates it's level.
    get_mock_server_api()->log->set_level(spdlog::level::level_enum::off);
    EXPECT_EQ(spdlog::level::level_enum::off, producer->getLogger().level());
    get_mock_server_api()->log->set_level(startLevel);
    EXPECT_EQ(startLevel, producer->getLogger().level());

    // Set the blacklist again which should set the level to critical and
    // unregsiter the logger.
    engine->getConfiguration().setDcpBlacklistFtsConnectionLogs(true);

    EXPECT_EQ(spdlog::level::level_enum::critical,
              producer->getLogger().level());

    // Changing global log levels should not change that of the logger as it is
    // not registered.
    get_mock_server_api()->log->set_level(spdlog::level::level_enum::off);
    EXPECT_EQ(spdlog::level::level_enum::critical,
              producer->getLogger().level());
    get_mock_server_api()->log->set_level(startLevel);

    // Needed to close connection references to prevent heap-use-after-free and
    // recreate our cookie after
    mockConnMap.disconnect(cookie);
    mockConnMap.manageConnections();
    cookie = create_mock_cookie();
}
