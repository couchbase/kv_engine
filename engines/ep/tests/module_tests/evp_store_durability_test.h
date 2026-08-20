/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "evp_store_single_threaded_test.h"

// Test fixture for Durability-related KVBucket tests.
class DurabilityKVBucketTest : public STParameterizedBucketTest {
protected:
    void SetUp() override {
        STParameterizedBucketTest::SetUp();
        // Add an initial replication topology so we can accept SyncWrites.
        setVBucketToActiveWithValidTopology();
    }

    void setVBucketToActiveWithValidTopology(
            nlohmann::json topology = nlohmann::json::array({{"active",
                                                              "replica"}})) {
        setVBucketStateAndRunPersistTask(
                vbid, vbucket_state_active, {{"topology", topology}});
    }
};

/**
 * Test fixture for verifying that SyncWrite completion is sharded across
 * multiple DurabilityCompletionTasks.
 *
 * Parameterised on the number of DurabilityCompletionTasks
 * (durability_completion_task_count).
 */
class ShardedDurabilityCompletionTest
    : public SingleThreadedKVBucketTest,
      public ::testing::WithParamInterface<size_t> {
protected:
    void SetUp() override;
    void TearDown() override;

    /// Make the given vBucket active with a valid topology, but perform no
    /// SyncWrite on it.
    void makeActive(Vbid vbid);

    /// Make the given vBucket active and resolve one SyncWrite on it - which
    /// is what binds the vBucket to a DurabilityCompletionTask.
    void resolveSyncWrite(Vbid vbid);

    /// Cookies of the SyncWrites issued by resolveSyncWrite(), which must
    /// outlive the SyncWrites they belong to.
    std::vector<CookieIface*> cookies;
};
