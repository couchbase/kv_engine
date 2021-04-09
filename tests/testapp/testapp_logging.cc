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
