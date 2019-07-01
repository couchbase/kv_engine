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

#include "../mock/gmock_dcp_msg_producers.h"
#include "checkpoint_manager.h"
#include "dcp/response.h"
#include "dcp_stream_test.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include "test_helpers.h"
#include "vbucket.h"
#include <engines/ep/tests/mock/mock_dcp.h>
#include <engines/ep/tests/mock/mock_dcp_producer.h>
#include <engines/ep/tests/mock/mock_stream.h>

/**
 * Tests if `arg` (an item*, aka void*) is equal to expected, excluding the
 * exptime field. This is helpful because over DCP, `exptime` is used to record
 * the deletion time, which differs for Items used in the frontend.
 */

MATCHER_P(ItemExcludingExptimeEq,
          expected,
          std::string(negation ? "isn't" : "is") +
                  " equal (excluding exptime) to " +
                  ::testing::PrintToString(expected)) {
    // Don't want to re-implment Item::operator==, so just take copies of
    // expected & actual and clear exptime.
    auto expectedWithoutExp = expected;
    auto actualWithoutExp = *static_cast<Item*>(arg);
    expectedWithoutExp.setExpTime(0);
    actualWithoutExp.setExpTime(0);
    return expectedWithoutExp == actualWithoutExp;
}

/// Check a DcpResponse is equal to the expected prepared Item.
void verifyDcpPrepare(const Item& expected, const DcpResponse& actual) {
    EXPECT_EQ(DcpResponse::Event::Prepare, actual.getEvent());
    const auto& dcpPrepare = dynamic_cast<const MutationResponse&>(actual);
    EXPECT_EQ(makeStoredDocKey("1"), dcpPrepare.getItem()->getKey());
    EXPECT_EQ(expected.getValue(), dcpPrepare.getItem()->getValue());
    EXPECT_EQ(expected.isDeleted(), dcpPrepare.getItem()->isDeleted());
    EXPECT_EQ(expected.getCas(), dcpPrepare.getItem()->getCas());
    EXPECT_EQ(expected.getDurabilityReqs().getLevel(),
              dcpPrepare.getItem()->getDurabilityReqs().getLevel());
}

/**
 * Test fixture for tests relating to DCP Streams and synchrnous replication.
 */
class DcpStreamSyncReplTest : public StreamTest {
protected:
    void SetUp() override {
        StreamTest::SetUp();
        // Need a valid replication chain to be able to perform SyncWrites
        engine->getKVBucket()->setVBucketState(
                vbid,
                vbucket_state_active,
                {{"topology", nlohmann::json::array({{"active", "replica"}})}});
    }

    /// Store a pending item of the given document state (alive / deleted).
    queued_item storePending(DocumentState docState,
                             std::string key,
                             std::string value,
                             cb::durability::Requirements reqs = {
                                     cb::durability::Level::Majority, {}}) {
        switch (docState) {
        case DocumentState::Alive:
            return store_pending_item(vbid, key, value, reqs);
        case DocumentState::Deleted:
            return store_pending_delete(vbid, key, reqs);
        }
        folly::assume_unreachable();
    }

    /**
     * Test that a pending SyncWrite is not sent to DCP consumer which doesn't
     * support sync replication.
     */
    void testNoPendingWithoutSyncReplica(DocumentState docState);

    /**
     * Test that if a mutation and pending SyncWrite is are present, a DCP
     * consumer which doesn't support sync replication only sees the mutation.
     */
    void testPendingAndMutationWithoutSyncReplica(DocumentState docState);

    /**
     * Test a mutation and a pending Sync Write against the same key sent to
     * DCP consumer which does not support sync replication.
     */
    void testMutationAndPendingWithoutSyncReplica(DocumentState docState);

    void testPendingItemWithSyncReplica(DocumentState docState);

    void testPendingAndMutationWithSyncReplica(DocumentState docState);

    /**
     * Test a mutation and a pending Sync Write against the same key sent to
     * DCP consumer which does support sync replication.
     */
    void testMutationAndPending2SnapshotsWithSyncReplica(
            DocumentState docState);

    /// Test that backfill of a prepared Write / Delete is handled correctly.
    void testBackfillPrepare(DocumentState docState);

    /**
     * Test that backfill of a prepared (Write or Delete) followed by a
     * committed item is handled correctly.
     */
    void testBackfillPrepareCommit(DocumentState docState);

    /**
     * Test that backfill of a prepared (Write or Delete) followed by a
     * aborted item is handled correctly.
     */
    void testBackfillPrepareAbort(DocumentState docState);
};

void DcpStreamSyncReplTest::testNoPendingWithoutSyncReplica(
        DocumentState docState) {
    // Setup: add a pending SyncWrite / Delete, store it and setup a DCP
    // stream to it.
    storePending(docState, "key", "value");
    setup_dcp_stream();
    ASSERT_EQ(ENGINE_SUCCESS, doStreamRequest(*producer).status);

    // For a non- sync replication stream we should not see any responses
    // if the only item is pending.
    GMockDcpMsgProducers producers; // no expectations set.

    prepareCheckpointItemsForStep(producers, *producer, *vb0);

    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(&producers));

    destroy_dcp_stream();
}

TEST_P(DcpStreamSyncReplTest, NoPendingWriteWithoutSyncReplica) {
    testNoPendingWithoutSyncReplica(DocumentState::Alive);
}

TEST_P(DcpStreamSyncReplTest, NoPendingDeleteWithoutSyncReplica) {
    testNoPendingWithoutSyncReplica(DocumentState::Deleted);
}

void DcpStreamSyncReplTest::testPendingAndMutationWithoutSyncReplica(
        DocumentState docState) {
    // Setup: add a mutation and a pending SyncWrite / Delete, store them
    // and setup a DCP stream.
    auto item = store_item(vbid, "normal", "XXX");
    storePending(docState, "pending", "YYY");
    setup_dcp_stream();
    ASSERT_EQ(ENGINE_SUCCESS, doStreamRequest(*producer).status);

    // For a non- sync replication stream we should see just one mutation.
    GMockDcpMsgProducers producers;
    {
        ::testing::InSequence dummy;
        using ::testing::_;
        using ::testing::Return;

        EXPECT_CALL(producers, marker(_, Vbid(0), 0, 1, _, _))
                .WillOnce(Return(ENGINE_SUCCESS));

        EXPECT_CALL(producers,
                    mutation(_,
                             ItemExcludingExptimeEq(*item),
                             Vbid(0),
                             item->getBySeqno(),
                             item->getRevSeqno(),
                             _,
                             _,
                             _,
                             _,
                             _))
                .WillOnce(Return(ENGINE_SUCCESS));
    }

    prepareCheckpointItemsForStep(producers, *producer, *vb0);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(&producers));

    destroy_dcp_stream();
}

TEST_P(DcpStreamSyncReplTest, PendingWriteAndMutationWithoutSyncReplica) {
    testPendingAndMutationWithoutSyncReplica(DocumentState::Alive);
}

TEST_P(DcpStreamSyncReplTest, PendingDeleteAndMutationWithoutSyncReplica) {
    testPendingAndMutationWithoutSyncReplica(DocumentState::Deleted);
}

void DcpStreamSyncReplTest::testMutationAndPendingWithoutSyncReplica(
        DocumentState docState) {
    // Setup: add a mutation and a pending SyncWrite, store them and setup a
    // DCP stream.
    auto item = store_item(vbid, "key", "XXX");
    /// Force a new checkpoint to avid de-duplication
    vb0->checkpointManager->createNewCheckpoint();
    storePending(docState, "key", "YYY");
    setup_dcp_stream();
    ASSERT_EQ(ENGINE_SUCCESS, doStreamRequest(*producer).status);

    // For a non- sync replication stream we should see just one mutation.
    GMockDcpMsgProducers producers;
    {
        ::testing::InSequence dummy;
        using ::testing::_;
        using ::testing::Return;

        EXPECT_CALL(producers, marker(_, Vbid(0), 0, 1, _, _))
                .WillOnce(Return(ENGINE_SUCCESS));

        EXPECT_CALL(producers,
                    mutation(_,
                             ItemExcludingExptimeEq(*item),
                             Vbid(0),
                             item->getBySeqno(),
                             item->getRevSeqno(),
                             _,
                             _,
                             _,
                             _,
                             _))
                .WillOnce(Return(ENGINE_SUCCESS));
    }

    prepareCheckpointItemsForStep(producers, *producer, *vb0);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(&producers));

    destroy_dcp_stream();
}

TEST_P(DcpStreamSyncReplTest, MutationAndPendingWriteWithoutSyncReplica) {
    testMutationAndPendingWithoutSyncReplica(DocumentState::Alive);
}

TEST_P(DcpStreamSyncReplTest, MutationAndPendingDeleteWithoutSyncReplica) {
    testMutationAndPendingWithoutSyncReplica(DocumentState::Deleted);
}

void DcpStreamSyncReplTest::testPendingItemWithSyncReplica(
        DocumentState docState) {
    // Setup: add a pending SyncWrite / Delete, store it and setup a DCP
    // stream to it.
    auto pending = storePending(docState, "key2", "XXX");
    setup_dcp_stream(0,
                     IncludeValue::Yes,
                     IncludeXattrs::Yes,
                     {{"enable_synchronous_replication", "true"}});
    ASSERT_EQ(ENGINE_SUCCESS, doStreamRequest(*producer).status);

    // For a sync replication stream we should see a snapshot marker
    // followed by one DcpPrepare.
    GMockDcpMsgProducers producers;
    {
        ::testing::InSequence dummy;
        using ::testing::_;
        using ::testing::Return;

        EXPECT_CALL(producers, marker(_, Vbid(0), 0, 1, _, _))
                .WillOnce(Return(ENGINE_SUCCESS));

        EXPECT_CALL(producers,
                    prepare(_,
                            ItemExcludingExptimeEq(*pending),
                            Vbid(0),
                            pending->getBySeqno(),
                            pending->getRevSeqno(),
                            _,
                            _,
                            docState,
                            _))
                .WillOnce(Return(ENGINE_SUCCESS));
    }

    // Drive the DcpMessageProducers
    prepareCheckpointItemsForStep(producers, *producer, *vb0);
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(&producers));

    destroy_dcp_stream();
}

TEST_P(DcpStreamSyncReplTest, PendingWriteWithSyncReplica) {
    testPendingItemWithSyncReplica(DocumentState::Alive);
}

TEST_P(DcpStreamSyncReplTest, PendingDeleteWithSyncReplica) {
    testPendingItemWithSyncReplica(DocumentState::Deleted);
}

void DcpStreamSyncReplTest::testPendingAndMutationWithSyncReplica(
        DocumentState docState) {
    auto mutation = store_item(vbid, "normal", "XXX");
    auto pending = storePending(docState, "pending", "YYY");
    setup_dcp_stream(0,
                     IncludeValue::Yes,
                     IncludeXattrs::Yes,
                     {{"enable_synchronous_replication", "true"}});
    ASSERT_EQ(ENGINE_SUCCESS, doStreamRequest(*producer).status);

    // For a sync replication stream we should see one mutation and one prepare.
    GMockDcpMsgProducers producers;
    {
        ::testing::InSequence dummy;
        using ::testing::_;
        using ::testing::Return;

        EXPECT_CALL(producers, marker(_, Vbid(0), 0, 2, _, _))
                .WillOnce(Return(ENGINE_SUCCESS));

        EXPECT_CALL(producers,
                    mutation(_,
                             ItemExcludingExptimeEq(*mutation),
                             Vbid(0),
                             mutation->getBySeqno(),
                             mutation->getRevSeqno(),
                             _,
                             _,
                             _,
                             _,
                             _))
                .WillOnce(Return(ENGINE_SUCCESS));

        EXPECT_CALL(producers,
                    prepare(_,
                            ItemExcludingExptimeEq(*pending),
                            Vbid(0),
                            pending->getBySeqno(),
                            pending->getRevSeqno(),
                            _,
                            _,
                            docState,
                            _))
                .WillOnce(Return(ENGINE_SUCCESS));
    }

    prepareCheckpointItemsForStep(producers, *producer, *vb0);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(&producers));

    destroy_dcp_stream();
}

TEST_P(DcpStreamSyncReplTest, PendingWriteAndMutationWithSyncReplica) {
    testPendingAndMutationWithSyncReplica(DocumentState::Alive);
}

TEST_P(DcpStreamSyncReplTest, PendingDeleteAndMutationWithSyncReplica) {
    testPendingAndMutationWithSyncReplica(DocumentState::Deleted);
}

void DcpStreamSyncReplTest::testMutationAndPending2SnapshotsWithSyncReplica(
        DocumentState docState) {
    // Setup: add a mutation and a pending SyncWrite, store them and setup a
    // DCP stream.
    auto mutation = store_item(vbid, "key", "XXX");
    /// Force a new checkpoint to avid de-duplication
    vb0->checkpointManager->createNewCheckpoint();
    auto pending = storePending(docState, "pending", "YYY");
    setup_dcp_stream(0,
                     IncludeValue::Yes,
                     IncludeXattrs::Yes,
                     {{"enable_synchronous_replication", "true"}});
    ASSERT_EQ(ENGINE_SUCCESS, doStreamRequest(*producer).status);

    // For a sync replication stream we should see one mutation and one prepare
    // each in their own snapshot.
    GMockDcpMsgProducers producers;
    {
        ::testing::InSequence dummy;
        using ::testing::_;
        using ::testing::Return;

        EXPECT_CALL(producers, marker(_, Vbid(0), 0, 1, _, _))
                .WillOnce(Return(ENGINE_SUCCESS));

        EXPECT_CALL(producers,
                    mutation(_,
                             ItemExcludingExptimeEq(*mutation),
                             Vbid(0),
                             mutation->getBySeqno(),
                             mutation->getRevSeqno(),
                             _,
                             _,
                             _,
                             _,
                             _))
                .WillOnce(Return(ENGINE_SUCCESS));

        EXPECT_CALL(producers, marker(_, Vbid(0), 2, 2, _, _))
                .WillOnce(Return(ENGINE_SUCCESS));

        EXPECT_CALL(producers,
                    prepare(_,
                            ItemExcludingExptimeEq(*pending),
                            Vbid(0),
                            pending->getBySeqno(),
                            pending->getRevSeqno(),
                            _,
                            _,
                            docState,
                            _))
                .WillOnce(Return(ENGINE_SUCCESS));
    }

    prepareCheckpointItemsForStep(producers, *producer, *vb0);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(&producers));

    destroy_dcp_stream();
}

TEST_P(DcpStreamSyncReplTest, MutationAndPendingWrite2SSWithSyncReplica) {
    testMutationAndPending2SnapshotsWithSyncReplica(DocumentState::Alive);
}

TEST_P(DcpStreamSyncReplTest, MutationAndPendingDelete2SSWithSyncReplica) {
    testMutationAndPending2SnapshotsWithSyncReplica(DocumentState::Deleted);
}

void DcpStreamSyncReplTest::testBackfillPrepare(DocumentState docState) {
    if (GetParam() == "ephemeral") {
        // @todo-durability: This test currently fails under ephmeral as
        // in-memory backfill doesn't fill in the durability requirements:
        //     std::logic_error: StoredValue::toItemImpl: attempted to create
        //     Item from Pending StoredValue without supplying durabilityReqs
        std::cerr << "NOTE: Skipped under Ephemeral\n";
        return;
    }

    // Store a pending item then remove the checkpoint to force backfill.
    using cb::durability::Level;
    auto prepared = storePending(
            docState, "1", "X", {Level::MajorityAndPersistOnMaster, {}});
    removeCheckpoint(1);

    // Create sync repl DCP stream
    setup_dcp_stream(0,
                     IncludeValue::Yes,
                     IncludeXattrs::Yes,
                     {{"enable_synchronous_replication", "true"}});

    MockDcpMessageProducers producers(engine);

    ExecutorPool::get()->setNumAuxIO(1);
    stream->transitionStateToBackfilling();

    // Wait for the backfill task to have pushed all items to the Stream::readyQ
    // Note: we expect 1 SnapshotMarker + numItems in the readyQ
    std::chrono::microseconds uSleepTime(128);
    while (stream->public_readyQSize() < 1 + 1) {
        uSleepTime = decayingSleep(uSleepTime);
    }

    auto item = stream->public_nextQueuedItem();
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, item->getEvent());
    auto snapMarker = dynamic_cast<SnapshotMarker&>(*item);
    EXPECT_EQ(0, snapMarker.getStartSeqno());
    EXPECT_EQ(1, snapMarker.getEndSeqno());

    item = stream->public_nextQueuedItem();
    verifyDcpPrepare(*prepared, *item);
}

TEST_P(DcpStreamSyncReplTest, BackfillPrepareWrite) {
    testBackfillPrepare(DocumentState::Alive);
}

TEST_P(DcpStreamSyncReplTest, BackfillPrepareDelete) {
    testBackfillPrepare(DocumentState::Deleted);
}

void DcpStreamSyncReplTest::testBackfillPrepareCommit(DocumentState docState) {
    if (GetParam() == "ephemeral") {
        // @todo-durability: This test currently fails under ephmeral as
        // in-memory backfill doesn't fill in the durability requirements:
        //     std::logic_error: StoredValue::toItemImpl: attempted to create
        //     Item from Pending StoredValue without supplying durabilityReqs
        std::cerr << "NOTE: Skipped under Ephemeral\n";
        return;
    }

    // Store a pending item, commit it and then remove the checkpoint to force
    // backfill.
    using cb::durability::Level;
    auto prepared = storePending(
            docState, "1", "X", {Level::MajorityAndPersistOnMaster, {}});
    ASSERT_EQ(ENGINE_SUCCESS,
              vb0->commit(prepared->getKey(),
                          prepared->getBySeqno(),
                          {},
                          vb0->lockCollections(prepared->getKey())));
    removeCheckpoint(2);

    // Create sync repl DCP stream
    setup_dcp_stream(0,
                     IncludeValue::Yes,
                     IncludeXattrs::Yes,
                     {{"enable_synchronous_replication", "true"}});

    MockDcpMessageProducers producers(engine);

    ExecutorPool::get()->setNumAuxIO(1);
    stream->transitionStateToBackfilling();

    // Wait for the backfill task to have pushed all items to the Stream::readyQ
    // Note: we expect 1 SnapshotMarker + numItems in the readyQ
    std::chrono::microseconds uSleepTime(128);
    while (stream->public_readyQSize() < 1 + 2) {
        uSleepTime = decayingSleep(uSleepTime);
    }

    auto item = stream->public_nextQueuedItem();
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, item->getEvent());
    auto& dcpSnapMarker = dynamic_cast<SnapshotMarker&>(*item);
    EXPECT_EQ(0, dcpSnapMarker.getStartSeqno());
    EXPECT_EQ(2, dcpSnapMarker.getEndSeqno());

    item = stream->public_nextQueuedItem();
    verifyDcpPrepare(*prepared, *item);

    item = stream->public_nextQueuedItem();
    // In general, a backfill from disk will send a mutation instead of a
    // commit as we may have de-duped the preceding prepare. The only case where
    // a backfill from disk will send a commit message is when the prepare seqno
    // is less than or equal to the requested stream start.
    auto expectedEvent = docState == DocumentState::Alive
                                 ? DcpResponse::Event::Mutation
                                 : DcpResponse::Event::Deletion;
    EXPECT_EQ(expectedEvent, item->getEvent());
    auto& mutation = dynamic_cast<MutationResponse&>(*item);
    EXPECT_EQ(makeStoredDocKey("1"), mutation.getItem()->getKey());
    EXPECT_EQ(prepared->getBySeqno() + 1, mutation.getItem()->getBySeqno());
}

TEST_P(DcpStreamSyncReplTest, BackfillPrepareWriteCommit) {
    testBackfillPrepareCommit(DocumentState::Alive);
}

TEST_P(DcpStreamSyncReplTest, BackfillPrepareDeleteCommit) {
    testBackfillPrepareCommit(DocumentState::Deleted);
}

void DcpStreamSyncReplTest::testBackfillPrepareAbort(DocumentState docState) {
    if (GetParam() == "ephemeral") {
        // @todo-durability: This test currently fails under ephmeral as
        // in-memory backfill doesn't fill in the durability requirements:
        //     std::logic_error: StoredValue::toItemImpl: attempted to
        //     create Item from Pending StoredValue without supplying
        //     durabilityReqs
        std::cerr << "NOTE: Skipped under Ephemeral\n";
        return;
    }

    // Store a pending item, commit it and then remove the checkpoint to force
    // backfill.
    using cb::durability::Level;
    auto prepared = storePending(
            docState, "1", "X", {Level::MajorityAndPersistOnMaster, {}});
    ASSERT_EQ(ENGINE_SUCCESS,
              vb0->abort(prepared->getKey(),
                         prepared->getBySeqno(),
                         {},
                         vb0->lockCollections(prepared->getKey())));
    removeCheckpoint(2);

    // Create sync repl DCP stream
    setup_dcp_stream(0,
                     IncludeValue::Yes,
                     IncludeXattrs::Yes,
                     {{"enable_synchronous_replication", "true"}});

    MockDcpMessageProducers producers(engine);

    ExecutorPool::get()->setNumAuxIO(1);
    stream->transitionStateToBackfilling();

    // Wait for the backfill task to have pushed all items to the Stream::readyQ
    // Note: we expect 1 SnapshotMarker + numItems in the readyQ
    std::chrono::microseconds uSleepTime(128);
    while (stream->public_readyQSize() < 1 + 1) {
        uSleepTime = decayingSleep(uSleepTime);
    }

    auto item = stream->public_nextQueuedItem();
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, item->getEvent());
    auto& dcpSnapMarker = dynamic_cast<SnapshotMarker&>(*item);
    EXPECT_EQ(0, dcpSnapMarker.getStartSeqno());
    EXPECT_EQ(2, dcpSnapMarker.getEndSeqno());

    item = stream->public_nextQueuedItem();
    EXPECT_EQ(DcpResponse::Event::Abort, item->getEvent());
    auto& dcpCommit = dynamic_cast<AbortSyncWrite&>(*item);
    EXPECT_EQ(makeStoredDocKey("1"), dcpCommit.getKey());
#if 0
    // @todo-durability: Once we move away from identifying abort by key,
    // the prepared seqno should be used (and should match).
    EXPECT_EQ(prepared->getBySeqno(), dcpCommit.getPreparedSeqno());
#endif
    EXPECT_EQ(prepared->getBySeqno() + 1, dcpCommit.getBySeqno());
}

TEST_P(DcpStreamSyncReplTest, BackfillPrepareWriteAbort) {
    testBackfillPrepareAbort(DocumentState::Alive);
}

TEST_P(DcpStreamSyncReplTest, BackfillPrepareDeleteAbort) {
    testBackfillPrepareAbort(DocumentState::Deleted);
}

// Test cases which run in both Full and Value eviction
INSTANTIATE_TEST_CASE_P(PersistentAndEphemeral,
                        DcpStreamSyncReplTest,
                        ::testing::Values("persistent", "ephemeral"),
                        [](const ::testing::TestParamInfo<std::string>& info) {
                            return info.param;
                        });
