/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/**
 * Test fixtures for statistics tests.
 */

#pragma once

#include "evp_store_single_threaded_test.h"

class StatTest : public SingleThreadedEPBucketTest {
protected:
    void SetUp() override;

    /**
     * Requests the given statistics from the bucket.
     * @param statkey
     * @return Map of stat name to value
     */
    std::map<std::string, std::string> get_stat(const char* statkey = nullptr);

    /**
     * Test that calling get_stats with the provided key creates a background
     * task, but does not write any responses until called again from the
     * frontend (after blocking, and notifying IO complete).
     *
     * @param key stats group to request
     * @param expectedTask expected name of task which will be created
     */
    void test_BackgroundTasksDoNotWriteResponses(std::string_view key,
                                                 std::string_view expectedTask);
};
