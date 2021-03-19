/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include "testapp.h"

#include "logger/logger_test_fixture.h"

#include <folly/portability/GTest.h>

class LoggingTest : public TestappTest,
                    public ::testing::WithParamInterface<int> {
};

TEST_P(LoggingTest, ChangeVerbosity) {
    auto& conn = getAdminConnection();
    conn.selectBucket("default");

    BinprotVerbosityCommand cmd;
    cmd.setLevel(GetParam());
    auto rsp = conn.execute(cmd);
    // Fail this test if the connection does not return a successful
    // response
    ASSERT_TRUE(rsp.isSuccess());

    spdlog::level::level_enum level;
    switch (GetParam()) {
    case 0:
        level = spdlog::level::info;
        break;
    case 1:
        level = spdlog::level::debug;
        break;
    default:
        level = spdlog::level::trace;
        break;
    }

    rsp = conn.execute(BinprotEWBCommand{EWBEngineMode::CheckLogLevels,
                                         cb::engine_errc::success,
                                         uint32_t(level),
                                         "key"});
    ASSERT_TRUE(rsp.isSuccess()) << to_string(rsp.getStatus());
}

// Test with verbosity values 0, 1, 2
INSTANTIATE_TEST_SUITE_P(LoggingTests, LoggingTest, ::testing::Range(0, 3));
