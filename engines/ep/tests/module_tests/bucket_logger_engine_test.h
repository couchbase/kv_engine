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

#include "bucket_logger_test.h"
#include "evp_engine_test.h"

/**
 * Unit tests for the BucketLogger class
 *
 * Contains engine related tests that check:
 *      - the prefixing of the engine (bucket name) to engine threads
 */
class BucketLoggerEngineTest : public BucketLoggerTest,
                               public EventuallyPersistentEngineTest {
protected:
    void SetUp() override;
    void TearDown() override;
};