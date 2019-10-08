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

#include <folly/portability/GTest.h>

class MockCheckpointManager;

/**
 * Dummy callback to replace the flusher callback.
 */
class DummyCB : public Callback<Vbid> {
public:
    DummyCB() {
    }

    void callback(Vbid& dummy) {
        (void)dummy;
    }
};

/**
 * Test fixture for Checkpoint tests. Once constructed provides a checkpoint
 * manager and single vBucket (VBID 0).
 *
 *@tparam V The VBucket class to use for the vbucket object.
 */
template <typename V>
class CheckpointTest : public ::testing::Test {
protected:
    CheckpointTest();

    void createManager(int64_t last_seqno = 1000);

    // Creates a new item with the given key and queues it into the checkpoint
    // manager.
    bool queueNewItem(const std::string& key);

    // Creates a new item with the given key@seqno and queues it into the
    // checkpoint manager.
    bool queueReplicatedItem(const std::string& key, int64_t seqno);

    EPStats global_stats;
    CheckpointConfig checkpoint_config;
    Configuration config;
    std::shared_ptr<Callback<Vbid>> callback;
    std::unique_ptr<V> vbucket;
    std::unique_ptr<MockCheckpointManager> manager;
};

// Set of vBucket classes to test.
// @todo: Why isn't Ephemeral in this set?
class EPVBucket;
using VBucketTypes = ::testing::Types<EPVBucket>;

/*
 * Test fixture for single-threaded Checkpoint tests
 */
class SingleThreadedCheckpointTest : public SingleThreadedKVBucketTest {
public:
    void closeReplicaCheckpointOnMemorySnapshotEnd(bool highMem,
                                                   uint32_t snapshotType);
};
