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

#include "logger_config.h"

void SpdloggerTest::SetUp() {
    setUpLogger(spdlog::level::level_enum::debug);
}

void SpdloggerTest::TearDown() {
    cb::logger::shutdown();
    RemoveFiles();
}

void SpdloggerTest::RemoveFiles() {
    files = cb::io::findFilesWithPrefix(filename);
    for (const auto& file : files) {
        cb::io::rmrf(file);
    }
}

void SpdloggerTest::setUpLogger(const spdlog::level::level_enum level,
                                const size_t cyclesize) {
    RemoveFiles();

    cb::logger::Config config;
    config.filename = filename;
    config.cyclesize = cyclesize;
    config.buffersize = 8192;
    config.unit_test = true;
    config.console = false;

    const auto ret = cb::logger::initialize(config);
    EXPECT_FALSE(ret) << ret.get();

    cb::logger::get()->set_level(level);
}

int SpdloggerTest::countInFile(const std::string& file,
                               const std::string& msg) {
    const auto content = cb::io::loadFile(file);

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
    files = cb::io::findFilesWithPrefix(filename);
    std::string ret;

    for (const auto& file : files) {
        ret.append(cb::io::loadFile(file));
    }

    return ret;
}
