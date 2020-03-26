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

#include "logger_test_fixture.h"

#include <gsl/gsl>

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
    cb::logger::shutdown();
    RemoveFiles();
}

void SpdloggerTest::RemoveFiles() {
    Expects(!config.filename.empty());
    files = cb::io::findFilesWithPrefix(config.filename);
    for (const auto& file : files) {
        cb::io::rmrf(file);
    }
}

void SpdloggerTest::setUpLogger() {
    RemoveFiles();

    const auto ret = cb::logger::initialize(config);
    EXPECT_FALSE(ret) << ret.value();

    cb::logger::get()->set_level(config.log_level);
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
