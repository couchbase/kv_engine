/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#include "dcp_stream_test.h"

/*
 * ActiveStream tests for Durability. Single-threaded.
 */
class DurabilityActiveStreamTest
    : virtual public SingleThreadedActiveStreamTest {
public:
    void SetUp() override;
    void TearDown() override;

protected:
    /**
     * Does the DurabilityActiveStreamTest specific setup
     */
    void setUp(bool startCheckpointProcessorTask);

    /*
     * Queues a Prepare and verifies that the corresponding DCP_PREPARE
     * message has been queued into the ActiveStream::readyQ.
     */
    void testSendDcpPrepare();

    void setUpSendSetInsteadOfCommitTest();

    enum class Resolution { Commit, Abort };

    /*
     * Queues a Commit/Abort and verifies that the corresponding DCP meesage
     * has been queued into the ActiveStream::readyQ.
     *
     * @param res The type of resolution, Commit/Abort
     */
    void testSendCompleteSyncWrite(Resolution res);

    /**
     * Simulates a seqnoACK arriving at the ActiveStream.
     * - Calls ActiveStream::seqnoAck() with the given consumerName and
     *   prepared seqno.
     * - processes any SyncWrites which are now resolved (which would normally
     *   be done by the DurabilityCompletion background task.
     */
    ENGINE_ERROR_CODE simulateStreamSeqnoAck(const std::string& consumerName,
                                             uint64_t preparedSeqno);

    /**
     * Test that backfill for a stream which has not negotiated sync write
     * support sends a snapshot end seqno corresponding to an item which will
     * be sent - not the seqno of a prepare or abort.
     */
    void testBackfillNoSyncWriteSupport(DocumentState docState,
                                        cb::durability::Level level);

    /**
     * Test that backfill for a stream which has not negotiated sync write
     * does not send an empty snapshot if backfill finds only prepares/aborts
     * and the stream transitions to in-memory correctly.
     */
    void testEmptyBackfillNoSyncWriteSupport(DocumentState docState,
                                             cb::durability::Level level);

    /**
     * Test that backfill for a stream which has not negotiated sync write
     * does not send an empty snapshot if cursor dropping triggers backfill,
     * which finds only prepares/aborts, and the stream transitions back to
     * in-memory correctly and streams the expected items from memory.
     */
    void testEmptyBackfillAfterCursorDroppingNoSyncWriteSupport(
            DocumentState docState, cb::durability::Level level);

    void removeCheckpoint(VBucket& vb, int numItems);

    const std::string active = "active";
    const std::string replica = "replica";
};

/*
 * PassiveStream tests for Durability. Single-threaded.
 */
class DurabilityPassiveStreamTest
    : virtual public SingleThreadedPassiveStreamTest {
public:
    void SetUp() override;
    void TearDown() override;

protected:
    /**
     * Creates a DCP_PREPARE message (with a preceeding SNAPSHOT_MARKER), and
     * processes it on the DCP stream.
     * Returns the SyncWrite prepare item.
     */
    queued_item makeAndReceiveDcpPrepare(
            const StoredDocKey& key,
            uint64_t cas,
            uint64_t seqno,
            cb::durability::Level level = cb::durability::Level::Majority);

    /*
     * Simulates a Replica receiving a DCP_PREPARE and checks that it is
     * queued correctly for persistence.
     */
    void testReceiveDcpPrepare();

    /**
     * Simulates a Replica received a DCP_PREPARE followed by another after
     * disconnecting and re-connecting.
     */
    void testReceiveDuplicateDcpPrepare(uint64_t prepareSeqno);

    /**
     * Simulates a Replica receiving multiple DCP_PREPAREs followed by another
     * set of DCP_PREPAREs for the same keys after disconnecting and
     * re-connecting.
     */
    void testReceiveMultipleDuplicateDcpPrepares();

    /**
     * Simulates a Replica receiving a DCP_PREPARE followed by DCP_COMMIT and
     * checks they are queued correctly for persistence.
     */
    void testReceiveDcpPrepareCommit();

    /*
     * This test checks that a DCP Consumer receives and processes correctly a
     * DCP_ABORT message.
     */
    void testReceiveDcpAbort();

    /**
     * Test that a mutation or deletion sent instead of a commit is accepted by
     * the replica when backfilling from disk
     *
     * @param snapStart Of disk snapshot, also seqno of received PRE
     * @param snapEnd Of disk snapshot, also seqno of received (logical) CMT
     * @param docState Should we send a mutation or a deletion?
     * @param clearCM Whwther we should start the test from an empty CM
     */
    void testReceiveMutationOrDeletionInsteadOfCommitWhenStreamingFromDisk(
            uint64_t snapStart,
            uint64_t snapEnd,
            DocumentState docState,
            bool clearCM = true);

    /**
     * Test that a mutation or deletion sent instead of a commit is accepted by
     * the replica when backfilling from disk if it already has a mutation.
     *
     * @param docState Shoudl we send a mutation or a deletion?
     */
    void
    receiveMutationOrDeletionInsteadOfCommitWhenStreamingFromDiskMutationFirst(
            DocumentState docState);

    /**
     * Test that a mutaiton or deletion sent instead of a commit is accepted by
     * the replica when in the reconnect window for which a prepare may be
     * de-duped and that the state of the replica is correct afterwards.
     *
     * @param docState Should we send a mutation or a deletion?
     */
    void
    testReceiveMutationOrDeletionInsteadOfCommitForReconnectWindowWithPrepareLast(
            DocumentState docState);

    void setUpHandleSnapshotEndTest();

    /**
     * The resolution type of a SyncWrite.
     */
    enum class Resolution : uint8_t {
        Commit,
        Abort,
    };

    /**
     * The test checks that Replica is resilient to receiving Abort messages
     * for deduplicated Prepares in the case where a previous Prepare has been
     * already received and completed (Committed/Aborted) for the same key.
     *
     * MB-36735: The test covers also the specific case where the completed
     * Prepare is Level::PersistToMajority and the unprepared Abort us received
     * before the Prepare is locally-satisfied (ie, the flusher has not
     * persisted the entire snapshot containing the Prepare, which is still
     * tracked in PassiveDM).
     *
     * @param level The durability level of the Prepare under test
     * @param res Resolution type (Commit/Abort) of the Prepare under test
     * @param flush Whether the flusher has persisted the snapshot containing
     *     the Prepare. Used for covering MB-36735.
     */
    void testPrepareCompletedAtAbort(cb::durability::Level level,
                                     Resolution res,
                                     bool flush = false);
};

/**
 * PassiveStream tests for Durability against persistent buckets.
 * Single-threaded.
 */
class DurabilityPassiveStreamPersistentTest
    : public DurabilityPassiveStreamTest {
protected:
    /**
     * Test that hte HCS sent in a disk snapshot is persisted by sending sending
     * a disk snapshot containing a mutation instead of a commit.
     */
    void testDiskSnapshotHCSPersisted();

    /**
     * Read the highCompletedSeqno from disk.
     */
    uint64_t getPersistedHCS();
};

/**
 * Test fixture for tests that begin with an active vBucket with Producer and
 * ActiveStream and end with a replica vBucket with a Consumer and PassiveStream
 * to test replica promotion scenarios.
 */
class DurabilityPromotionStreamTest : public DurabilityActiveStreamTest,
                                      public DurabilityPassiveStreamTest {
public:
    void SetUp() override;
    void TearDown() override;

protected:
    /**
     * Test that Disk checkpoints received on a replica are streamed as Disk
     * snapshots when promoted to active.
     */
    void testDiskCheckpointStreamedAsDiskSnapshot();

    /**
     * Test that at snapshot transition (ie, ActiveStream streaming an alternate
     * sequence of Disk/Memory checkpoints) the MARKER_FLAG_CHK is always set in
     * the SnapshotMarker sent to Replica.
     */
    void testCheckpointMarkerAlwaysSetAtSnapTransition();

    /**
     * Test that Active always sends the HCS at streaming disk-snapshots from
     * memory.
     */
    void testActiveSendsHCSAtDiskSnapshotSentFromMemory();
};
