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
#include "logger/prefix_logger.h"

#include <gsl/gsl-lite.hpp>
#include <spdlog/spdlog.h>

SpdloggerTest::SpdloggerTest() {
    // Use default values from cb::logger::Config, apart from:
    config.log_level = spdlog::level::level_enum::debug;
    config.filename = "spdlogger_test";
    config.unit_test = true; // Enable unit test mode (synchronous logging)
    config.console = false; // Don't print to stderr
}

void SpdloggerTest::SetUp() {
    setUpLogger();
}

void SpdloggerTest::TearDown() {
    shutdownLoggerAndRemoveFiles();
}

void SpdloggerTest::RemoveFiles() {
    Expects(!config.filename.empty());
    files = cb::io::findFilesWithPrefix(config.filename);
    for (const auto& file : files) {
        std::filesystem::remove(file);
    }
}

void SpdloggerTest::setUpLogger() {
    shutdownLoggerAndRemoveFiles();

    const auto ret = cb::logger::initialize(config);
    EXPECT_FALSE(ret) << ret.value();

    cb::logger::get()->set_level(config.log_level);
}

static auto getLoggerWeakPtrs() {
    // May look contrived but it helps to detect if the logger is being
    // referenced elsewhere, which is important especially on Windows, where if
    // the logger is not destroyed, we cannot remove the log files since the
    // process has an open file handle, and that may fail the test.
    std::vector<std::weak_ptr<spdlog::logger>> weakLoggers;
    auto& registry = spdlog::details::registry::instance();
    registry.apply_all(
            [&weakLoggers](const auto& l) { weakLoggers.push_back(l); });
    return weakLoggers;
}

void SpdloggerTest::shutdownLoggerAndRemoveFiles() {
    auto weakLoggers = getLoggerWeakPtrs();

    cb::logger::shutdown();

    for (const auto& l : weakLoggers) {
        auto logger = l.lock();
        if (!logger) {
            continue;
        }
        FAIL() << "Logger '" << logger->name()
               << "' still exists with use count: " << (l.use_count() - 1);
    }

    RemoveFiles();
}

int SpdloggerTest::countInFile(const std::string& file,
                               const std::string& msg) {
    const auto content = cb::io::loadFile(file, {});

    const auto* begin = content.data();
    const auto* end = begin + content.size();

    int count = 0;
    while ((begin = std::search(begin, end, msg.begin(), msg.end())) != end) {
        ++count;
        begin += msg.size();
    }
    return count;
}

std::string SpdloggerTest::getLogContents() {
    files = cb::io::findFilesWithPrefix(config.filename);
    std::string ret;

    for (const auto& file : files) {
        ret.append(cb::io::loadFile(file, {}));
    }

    return ret;
}

TEST_F(SpdloggerTest, PrefixLogger) {
    auto prefixLogger = std::make_shared<cb::logger::PrefixLogger>(
            "prefix_logger", cb::logger::get());
    prefixLogger->setPrefix(cb::logger::BasicJsonType({{"prefix", "test"}}),
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
