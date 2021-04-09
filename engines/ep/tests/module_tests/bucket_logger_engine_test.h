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
