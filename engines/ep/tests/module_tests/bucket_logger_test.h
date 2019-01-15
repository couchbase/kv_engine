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
    void SetUp() override;
    void TearDown() override;
    void setUpLogger() override;

    // Old logging level so we can reset the logger in TearDown
    spdlog::level::level_enum oldLogLevel;
};
