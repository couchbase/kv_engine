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

#include "callbacks.h"
#include "checkpoint_config.h"
#include "configuration.h"
#include "evp_store_single_threaded_test.h"
#include "stats.h"
#include "vbucket_test.h"

#include <engines/ep/src/ephemeral_vb.h>
#include <folly/portability/GTest.h>

class MockCheckpointManager;

/**
 * Test fixture for Checkpoint tests.
 */
class CheckpointTest : public VBucketTest {
public:
    void SetUp() override;
    void TearDown() override;

protected:
    // Creates a new item with the given key and queues it into the checkpoint
    // manager.
    bool queueNewItem(const std::string& key);

    // Creates a new item with the given key@seqno and queues it into the
    // checkpoint manager.
    bool queueReplicatedItem(const std::string& key, int64_t seqno);

    void createManager(int64_t lastSeqno = 1000);

    void resetManager();

    // Owned by VBucket
    MockCheckpointManager* manager;
    // Owned by CheckpointManager
    CheckpointCursor* cursor;
};

/*
 * Test fixture for single-threaded Checkpoint tests
 */
class SingleThreadedCheckpointTest : public SingleThreadedKVBucketTest {
public:
    void closeReplicaCheckpointOnMemorySnapshotEnd(bool highMem,
                                                   uint32_t snapshotType);
};
