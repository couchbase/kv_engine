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
class DurabilityActiveStreamTest : public SingleThreadedActiveStreamTest {
public:
    void SetUp() override;
    void TearDown() override;

protected:
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

    const std::string active = "active";
    const std::string replica = "replica";
};

/*
 * PassiveStream tests for Durability. Single-threaded.
 */
class DurabilityPassiveStreamTest : public SingleThreadedPassiveStreamTest {
public:
    void SetUp() override;
    void TearDown() override;

protected:
    /**
     * Creates a DCP_PREPARE message (with a preceeding SNAPSHOT_MARKER), and
     * processes it on the DCP stream.
     * Returns the SyncWrite prepare item.
     */
    queued_item makeAndReceiveDcpPrepare(const StoredDocKey& key,
                                         uint64_t cas,
                                         uint64_t seqno);

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
     * @param docState Should we send a mutation or a deletion?
     */
    void testReceiveMutationOrDeletionInsteadOfCommitWhenStreamingFromDisk(
            DocumentState docState);

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
};

/**
 * PassiveStream tests for Durability against persistent buckets.
 * Single-threaded.
 */
class DurabilityPassiveStreamPersistentTest
    : public DurabilityPassiveStreamTest {};

/**
 * ActiveStream tests for Durability against ephemeral buckets. Single-threaded.
 */
class DurabilityActiveStreamEphemeralTest : public DurabilityActiveStreamTest {
};
