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

#include "bucket_logger_engine_test.h"

#include <engines/ep/src/bucket_logger.h>

void BucketLoggerEngineTest::SetUp() {
    // Override some logger config params before calling parent class SetUp():

    // 1. Write to a different file in case other parent class fixtures are
    // running in parallel
    config.filename = "spdlogger_engine_test";
    // 2. Set up the logger with a greater file size so logs are output to a
    // single file
    config.cyclesize = 100 * 1024;

    BucketLoggerTest::SetUp();
    EventuallyPersistentEngineTest::SetUp();
}

void BucketLoggerEngineTest::TearDown() {
    EventuallyPersistentEngineTest::TearDown();
    BucketLoggerTest::TearDown();
}

/**
 * Test that the bucket name is logged out at the start of the message
 */
TEST_F(BucketLoggerEngineTest, EngineTest) {
    // Flush the logger to ensure log file has required contents. Flushing
    // instead of shutting it down as the engine is still running
    cb::logger::flush();
    files = cb::io::findFilesWithPrefix(config.filename);
    EXPECT_EQ(1,
              countInFile(files.back(),
                          "INFO (default) EPEngine::initialize: using "
                          "configuration:"));
}
