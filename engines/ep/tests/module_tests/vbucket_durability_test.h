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
class ActiveDurabilityMonitor;

/*
 * VBucket unit tests related to durability.
 */
class VBucketDurabilityTest : public VBucketTest {
public:
    void SetUp();

protected:
    /// Specification of a SyncWrite to store, as used by storeSyncWrites.
    struct SyncWriteSpec {
        explicit SyncWriteSpec(int64_t seqno) : seqno(seqno) {
        }
        SyncWriteSpec(int64_t seqno, bool deletion = false)
            : seqno(seqno), deletion(deletion) {
        }
        SyncWriteSpec(int64_t seqno, bool deletion, cb::durability::Level level)
            : seqno(seqno), deletion(deletion), level(level) {
        }

        int64_t seqno;
        bool deletion = false;
        cb::durability::Level level = cb::durability::Level::Majority;
    };

    /**
     * Store the given Sync mutations into VBucket
     *
     * @param writes the mutations to be added
     */
    void storeSyncWrites(const std::vector<SyncWriteSpec>& writes);

    /**
     * Simulate the local (active) seqno acknowledgement.
     *
     * @param seqno The ack'ed seqno
     */
    void simulateLocalAck(uint64_t seqno);

    /**
     * Tests:
     * 1) mutations added to VBucket
     * 2) mutations in state "pending" in both HashTable and CheckpointManager
     *
     * @param writes the set of mutations to test
     */
    void testAddPrepare(const std::vector<SyncWriteSpec>& writes);

    /**
     * Tests the baseline progress of a set of SyncWrites in Vbucket:
     * 1) mutations added to VBucket
     * 2) mutations in state "pending" in both HashTable and CheckpointManager
     * 3) VBucket receives a SeqnoAck that satisfies the DurReqs for all SWs
     * 4) mutations in state "committed" in both HashTable and CheckpointManager
     *
     * @param writes the set of mutations to test
     */
    void testAddPrepareAndCommit(const std::vector<SyncWriteSpec>& writes);

    /**
     * Tests that the Replication Topology is cleared when a VBucket transitions
     * to the given state.
     *
     * @param state The new state for VBucket
     */
    void testSetVBucketState_ClearTopology(vbucket_state_t state);

    /**
     * Tests that the PassiveDM queues incoming Prepares correctly when the
     * owning VBucket is in the given state.
     *
     * @param state The state for VBucket
     * @param writes The Prepares to be queued
     */
    void testAddPrepareInPassiveDM(vbucket_state_t state,
                                   const std::vector<SyncWriteSpec>& writes);

    /**
     * Tests that the PassiveDM is correctly converted to ActiveDM when a
     * VBucket in the provided initial state transitions to vbstate-active when
     * there are in-flight SyncWrites.
     *
     * @param initialState The initial state for VBucket
     */
    void testConvertPassiveDMToActiveDM(vbucket_state_t initialState);

    /**
     * Tests that the PassiveDM is correctly converted to ActiveDM when a
     * vBucket in the provided initial state transitions to vbstate-active when
     * there are in-flight SyncWrites. This test mimics an actual takeover which
     * will do the following set of state transitions:
     * replica/pending->active with no topology->active with topology. Persists
     * up to seqno 2 regardless of the writes passed in.
     *
     * @param initialState The initial state for the vBucket
     * @param writes The prepares to be queued
     */
    void testConvertPDMToADMWithNullTopologySetup(
            vbucket_state_t initialState, std::vector<SyncWriteSpec>& writes);

    /**
     * Tests that the PassiveDM is correctly converted to ActiveDM when a
     * vBucket in the provided initial state transitions to vbstate-active when
     * there are in-flight SyncWrites. This test mimics an actual takeover which
     * will do the following set of state transitions:
     * replica/pending->active with no topology->active with topology
     *
     * @param initialState The initial state for the vBucket
     */
    void testConvertPDMToADMWithNullTopology(vbucket_state_t initialState);

    /**
     * Tests that the PassiveDM is correctly converted to ActiveDM when a
     * vBucket in the provided initial state transitions to vbstate-active when
     * there are in-flight SyncWrites. This test mimics an actual takeover which
     * will do the following set of state transitions:
     * replica/pending->active with no topology->active with topology. This test
     * tests that a PersistToMajority Prepare persisted after the final topology
     * change is done correctly.
     *
     * @param initialState The initial state for the vBucket
     */
    void testConvertPDMToADMWithNullTopologyPersistAfterTopologyChange(
            vbucket_state_t initialState);

    /**
     * Tests that the PassiveDM is correctly converted to ActiveDM when a
     * vBucket in the provided initial state transitions to vbstate-active when
     * there are in-flight SyncWrites. This test mimics an actual takeover which
     * will do the following set of state transitions:
     * replica/pending->active with no topology->active with topology. This test
     * tests that a PersistToMajority Prepare persisted between the null and
     * final topology change is done correctly.
     *
     * @param initialState The initial state for the vBucket
     */
    void testConvertPDMToADMWithNullTopologyPersistBeforeTopologyChange(
            vbucket_state_t initialState);

    /**
     * Tests that the PassiveDM is correctly converted to ActiveDM when a
     * vBucket in the provided initial state transitions to vbstate-active when
     * there are in-flight SyncWrites. This test mimics an actual takeover which
     * will do the following set of state transitions:
     * replica/pending->active with no topology->active with topology. This test
     * tests that a HPS that does not equal anything in trackedWrites is moved
     * over the the ADM and that a subsequent prepare and commit can be
     * performed successfully.
     *
     * @param initialState The initial state for the vBucket
     */
    void testConvertPDMToADMWithNullTopologyPostDiskSnap(
            vbucket_state_t initialState);

    /**
     * Tests that the PassiveDM is correctly converted to ActiveDM when a
     * vBucket in the provided initial state transitions to vbstate-active when
     * there is an in-flight PersistToMajority SyncWrite that has not yet been
     * persisted.
     *
     * @param initialState The initial state for the vBucket
     */
    void testConvertPassiveDMToActiveDMUnpersistedPrepare(
            vbucket_state_t initialState);

    /**
     * Test that the PassiveDM is correctly converted to ActiveDM when a
     * VBucket in the provided initial state transitions to vbstate-active when
     * there are no in-flight SyncWrites.
     *
     * @param initialState The initial state for VBucket
     */
    void testConvertPassiveDMToActiveDMNoPrepares(vbucket_state_t initialState);

    enum class Resolution { Commit, Abort };

    /**
     * Tests that the PassiveDM behaves correctly when VBucket notifies the
     * PassiveDM of Prepare completion (ie, Commit or Abort).
     *
     * @param initialState The initial state for VBucket
     * @param res The type of resolution, Commit/Abort
     */
    void testCompleteSWInPassiveDM(vbucket_state_t initialState,
                                   Resolution res);

    /**
     * Test a normal set followed by a pending SyncWrite; then committing the
     * pending SyncWrite which should replace the previous committed.
     */
    void testHTCommitExisting();

    /**
     * Add a pending SyncDelete to the vBucket for the given key.
     */
    void setupPendingDelete(StoredDocKey key);

    /**
     * Test a commit of a sync delete
     */
    void testHTSyncDeleteCommit();

    void doSyncWriteAndCommit();
    void doSyncDelete();

    // All owned by VBucket
    HashTable* ht;
    MockCheckpointManager* ckptMgr;

    const std::string active = "active";
    const std::string replica1 = "replica1";
    const std::string replica2 = "replica2";
    const std::string replica3 = "replica3";
};

class EPVBucketDurabilityTest : public VBucketDurabilityTest {};
class EphemeralVBucketDurabilityTest : public VBucketDurabilityTest {};
