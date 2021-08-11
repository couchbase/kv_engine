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

#pragma once

#include "logger/logger_test_fixture.h"

/**
 * Unit tests for the BucketLogger class
 *
 * Contains basic tests that check:
 *      - the spdlog stlye formatting ({})
 *      - the logging level prefixing
 *      - the prefixing of (No Engine) to non-engine threads
 */
class BucketLoggerTest : virtual public SpdloggerTest {
protected:
    BucketLoggerTest();
    void SetUp() override;
    void TearDown() override;
    void setUpLogger() override;

    // Old logging level so we can reset the logger in TearDown
    spdlog::level::level_enum oldLogLevel;
};
