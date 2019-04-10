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

#include "vbucket_test.h"

class MockCheckpointManager;
class MockDurabilityMonitor;

/*
 * VBucket unit tests related to durability.
 */
class VBucketDurabilityTest
    : public ::testing::TestWithParam<item_eviction_policy_t>,
      public VBucketTestBase {
public:
    VBucketDurabilityTest() : VBucketTestBase(GetParam()) {
    }

    void SetUp();

protected:
    /// Specification of a SyncWrite to store, as used by storeSyncWrites.
    struct SyncWriteSpec {
        explicit SyncWriteSpec(int64_t seqno) : seqno(seqno) {
        }
        SyncWriteSpec(int64_t seqno, bool deletion = false)
            : seqno(seqno), deletion(deletion) {
        }

        int64_t seqno;
        bool deletion = false;
    };

    /**
     * Store the given Sync mutations into VBucket
     *
     * @param writes the mutations to be added
     * @return the number of stored SyncWrites
     */
    size_t storeSyncWrites(const std::vector<SyncWriteSpec>& writes);

    /**
     * Tests the baseline progress of a set of SyncWrites in Vbucket:
     * 1) mutations added to VBucket
     * 2) mutations in state "pending" in both HashTable and CheckpointManager
     * 3) VBucket receives a SeqnoAck that satisfies the DurReqs for all SWs
     * 4) mutations in state "committed" in both HashTable and CheckpointManager
     *
     * @param writes the set of mutations to test
     */
    void testSyncWrites(const std::vector<SyncWriteSpec>& writes);

    // All owned by VBucket
    HashTable* ht;
    MockCheckpointManager* ckptMgr;
    MockDurabilityMonitor* monitor;

    const std::string active = "active";
    const std::string replica = "replica";
};
