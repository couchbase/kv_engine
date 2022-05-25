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
    cb::engine_errc simulateStreamSeqnoAck(const std::string& consumerName,
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

class DurabilityActiveStreamEphemeralTest
    : virtual public DurabilityActiveStreamTest {};

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
     * Creates a SNAPSHOT_MARKER message with range [seqno,seqno] and
     * processes it on the DCP stream.
     */
    void makeAndProcessSnapshotMarker(
            uint32_t opaque,
            uint64_t seqno,
            uint64_t snapshotMarkerFlags = MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            const std::optional<uint64_t>& hcs = 0);

    /**
     * Creates a DCP_PREPARE message and processes it on the DCP stream.
     * Returns the SyncWrite prepare item.
     */
    queued_item makeAndReceiveDcpPrepare(
            const StoredDocKey& key,
            uint64_t cas,
            uint64_t seqno,
            cb::durability::Level level = cb::durability::Level::Majority);

    /**
     * Creates a DCP_PREPARE message (with a preceeding SNAPSHOT_MARKER), and
     * processes it on the DCP stream.
     * Returns the SyncWrite prepare item.
     */
    queued_item makeAndReceiveSnapMarkerAndDcpPrepare(
            const StoredDocKey& key,
            uint64_t cas,
            uint64_t seqno,
            cb::durability::Level level = cb::durability::Level::Majority,
            uint64_t snapshotMarkerFlags = MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            std::optional<uint64_t> hcs = 0);

    queued_item makeAndReceiveCommittedItem(
            const StoredDocKey& key,
            uint64_t cas,
            uint64_t seqno,
            std::optional<DeleteSource> deleted = {});

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

    /**
     * The test verifies that Replica repositions the HPS iterator correctly at
     * Prepare deduplication.
     *
     * @param level The level of the Prepares under testing
     */
    void testPrepareDeduplicationCorrectlyResetsHPS(
            cb::durability::Level level);
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
     * Tests that Replica skips completed Prepares when receiving a Disk
     * Checkpoint. Covers all the scenarios where a completed Prepare for <key>
     * may still be present in the PDM tracked list when Replica receives and
     * completes a new Prepare for the same key, ie:
     *
     * 1) Replica has received a first PersistTo Prepare in a Memory Checkpoint
     * 2) Replica has received a first (whatever level) Prepare in a Disk
     *   Checkpoint
     *
     * @param firstCkptType
     */
    void testCompletedPrepareSkippedAtOutOfOrderCompletion(
            CheckpointType firstCkptType);

    /**
     * Read the highCompletedSeqno from disk.
     */
    uint64_t getPersistedHCS();
};

class DurabilityPassiveStreamEphemeralTest
    : public DurabilityPassiveStreamTest {
protected:
    void testLogicalCommitCorrectTypeSetup();
    void testPrepareCommitedInDiskSnapshotCorrectState(
            std::optional<DeleteSource> = {});
    void testAbortCommitedInDiskSnapshotCorrectState(
            std::optional<DeleteSource> = {});
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
