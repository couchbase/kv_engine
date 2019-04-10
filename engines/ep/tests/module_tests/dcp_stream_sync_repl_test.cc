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

#include "checkpoint_manager.h"
#include "dcp_stream_test.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include "test_helpers.h"
#include "vbucket.h"
#include <engines/ep/tests/mock/mock_dcp.h>
#include <engines/ep/tests/mock/mock_dcp_producer.h>
#include <engines/ep/tests/mock/mock_stream.h>

/**
 * Test fixture for tests relating to DCP Streams and synchrnous replication.
 */
class DcpStreamSyncReplTest : public StreamTest {
    void SetUp() override {
        StreamTest::SetUp();
        // Need a valid replication chain to be able to perform SyncWrites
        engine->getKVBucket()->setVBucketState(
                vbid,
                vbucket_state_active,
                {{"topology", nlohmann::json::array({{"active", "replica"}})}});
    }
};

/// Test that a pending SyncWrite is not sent to DCP consumer which doesn't
/// support sync replication.
TEST_P(DcpStreamSyncReplTest, NoPendingItemWithoutSyncReplica) {
    // Setup: add a pending SyncWrite, store it and setup a DCP stream to it.
    store_pending_item(vbid, "key", "XXX");
    setup_dcp_stream();
    ASSERT_EQ(ENGINE_SUCCESS, doStreamRequest(*producer).status);

    // For a non- sync replication stream we should not see any responses
    // if the only item is pending.
    MockDcpMessageProducers producers(engine);
    prepareCheckpointItemsForStep(producers, *producer, *vb0);

    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(&producers));

    destroy_dcp_stream();
}

/// Test that if a mutation and pending SyncWrite is are present, a DCP
/// consumer which doesn't support sync replication only sees the mutation
TEST_P(DcpStreamSyncReplTest, PendingAndMutationWithoutSyncReplica) {
    // Setup: add a mutation and a pending SyncWrite, store them and setup a
    // DCP stream.
    store_item(vbid, "normal", "XXX");
    store_pending_item(vbid, "pending", "YYY");
    setup_dcp_stream();
    ASSERT_EQ(ENGINE_SUCCESS, doStreamRequest(*producer).status);

    // For a non- sync replication stream we should see only one mutation.
    MockDcpMessageProducers producers(engine);

    prepareCheckpointItemsForStep(producers, *producer, *vb0);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers.last_op);
    EXPECT_EQ(0, producers.last_snap_start_seqno);
    EXPECT_EQ(1, producers.last_snap_end_seqno);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers.last_op);
    EXPECT_EQ("normal", producers.last_key);
    EXPECT_EQ("XXX", producers.last_value);

    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(&producers));

    destroy_dcp_stream();
}

/// Test a mutation and a pending Sync Write against the same key sent to DCP
/// consumer which does not support sync replication.
TEST_P(DcpStreamSyncReplTest, MutationAndPendingWithoutSyncReplica) {
    // Setup: add a mutation and a pending SyncWrite, store them and setup a
    // DCP stream.
    store_item(vbid, "key", "XXX");
    /// Force a new checkpoint to avid de-duplication
    vb0->checkpointManager->createNewCheckpoint();
    store_pending_item(vbid, "key", "YYY");
    setup_dcp_stream();
    ASSERT_EQ(ENGINE_SUCCESS, doStreamRequest(*producer).status);

    // For a non- sync replication stream we should see just one mutation.
    MockDcpMessageProducers producers(engine);

    prepareCheckpointItemsForStep(producers, *producer, *vb0);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers.last_op);
    EXPECT_EQ(0, producers.last_snap_start_seqno);
    EXPECT_EQ(1, producers.last_snap_end_seqno);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers.last_op);
    EXPECT_EQ("key", producers.last_key);
    EXPECT_EQ("XXX", producers.last_value);

    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(&producers));

    destroy_dcp_stream();
}

/// Test that a pending SyncWrite is sent to DCP consumer which does support
/// sync replication.
TEST_P(DcpStreamSyncReplTest, PendingItemWithSyncReplica) {
    // Setup: add a pending SyncWrite, store it and setup a DCP stream to it.
    store_pending_item(vbid, "key2", "XXX");
    setup_dcp_stream(0,
                     IncludeValue::Yes,
                     IncludeXattrs::Yes,
                     {{"enable_synchronous_replication", "true"}});
    ASSERT_EQ(ENGINE_SUCCESS, doStreamRequest(*producer).status);

    // For a sync replication stream we should see a snapshot marker
    // followed by one DcpPrepare.
    MockDcpMessageProducers producers(engine);

    prepareCheckpointItemsForStep(producers, *producer, *vb0);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers.last_op);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpPrepare, producers.last_op);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(&producers));

    destroy_dcp_stream();
}

/// Test that if a mutation and pending SyncWrite is are present, a DCP
/// consumer which does support sync replication only sees the mutation
TEST_P(DcpStreamSyncReplTest, PendingAndMutationWithSyncReplica) {
    // Setup: add a mutation and a pending SyncWrite, store them and setup a
    // DCP stream.
    store_item(vbid, "normal", "XXX");
    store_pending_item(vbid, "pending", "YYY");
    setup_dcp_stream(0,
                     IncludeValue::Yes,
                     IncludeXattrs::Yes,
                     {{"enable_synchronous_replication", "true"}});
    ASSERT_EQ(ENGINE_SUCCESS, doStreamRequest(*producer).status);

    // For a sync replication stream we should see one mutation and one prepare.
    MockDcpMessageProducers producers(engine);

    prepareCheckpointItemsForStep(producers, *producer, *vb0);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers.last_op);
    EXPECT_EQ(0, producers.last_snap_start_seqno);
    EXPECT_EQ(2, producers.last_snap_end_seqno);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers.last_op);
    EXPECT_EQ("normal", producers.last_key);
    EXPECT_EQ("XXX", producers.last_value);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpPrepare, producers.last_op);
    EXPECT_EQ("pending", producers.last_key);
    EXPECT_EQ("YYY", producers.last_value);

    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(&producers));

    destroy_dcp_stream();
}

/// Test a mutation and a pending Sync Write against the same key sent to DCP
/// consumer which does support sync replication.
TEST_P(DcpStreamSyncReplTest, MutationAndPendingWithSyncReplica) {
    // Setup: add a mutation and a pending SyncWrite, store them and setup a
    // DCP stream.
    store_item(vbid, "key", "XXX");
    /// Force a new checkpoint to avid de-duplication
    vb0->checkpointManager->createNewCheckpoint();
    store_pending_item(vbid, "key", "YYY");
    setup_dcp_stream(0,
                     IncludeValue::Yes,
                     IncludeXattrs::Yes,
                     {{"enable_synchronous_replication", "true"}});
    ASSERT_EQ(ENGINE_SUCCESS, doStreamRequest(*producer).status);

    // For a sync replication stream we should see one mutation and one prepare.
    MockDcpMessageProducers producers(engine);

    prepareCheckpointItemsForStep(producers, *producer, *vb0);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers.last_op);
    EXPECT_EQ(0, producers.last_snap_start_seqno);
    EXPECT_EQ(1, producers.last_snap_end_seqno);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers.last_op);
    EXPECT_EQ("key", producers.last_key);
    EXPECT_EQ("XXX", producers.last_value);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers.last_op);
    EXPECT_EQ(2, producers.last_snap_start_seqno);
    EXPECT_EQ(2, producers.last_snap_end_seqno);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpPrepare, producers.last_op);
    EXPECT_EQ("key", producers.last_key);
    EXPECT_EQ("YYY", producers.last_value);

    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(&producers));

    destroy_dcp_stream();
}

/// Check a DcpResponse is equal to the expected prepared Item.
void verifyDcpPrepare(Item& expected, DcpResponse& actual) {
    EXPECT_EQ(DcpResponse::Event::Prepare, actual.getEvent());
    auto& dcpPrepare = dynamic_cast<MutationResponse&>(actual);
    EXPECT_EQ(makeStoredDocKey("1"), dcpPrepare.getItem()->getKey());
    EXPECT_EQ(expected.getValue(), dcpPrepare.getItem()->getValue());
    EXPECT_EQ(expected.getCas(), dcpPrepare.getItem()->getCas());
    EXPECT_EQ(expected.getDurabilityReqs().getLevel(),
              dcpPrepare.getItem()->getDurabilityReqs().getLevel());
}

/// Test that backfill of a prepared item is handled correctly.
TEST_P(DcpStreamSyncReplTest, BackfillPrepare) {
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
    auto prepared = store_pending_item(
            vbid, "1", "X", {Level::MajorityAndPersistOnMaster, 0});
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

/// Test that backfill of a prepared+committed item is handled correctly.
TEST_P(DcpStreamSyncReplTest, BackfillPrepareCommit) {
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
    auto prepared = store_pending_item(
            vbid, "1", "X", {Level::MajorityAndPersistOnMaster, 0});
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
    EXPECT_EQ(DcpResponse::Event::Commit, item->getEvent());
    auto& dcpCommit = dynamic_cast<CommitSyncWrite&>(*item);
    EXPECT_EQ(makeStoredDocKey("1"), dcpCommit.getKey());
#if 0
    // @todo-durability: Once we move away from identifying commit by key,
    // the prepared seqno should be used (and should match).
    EXPECT_EQ(prepared->getBySeqno(), dcpCommit.getPreparedSeqno());
#endif
    EXPECT_EQ(prepared->getBySeqno() + 1, dcpCommit.getBySeqno());
}

/// Test that backfill of a prepared+aborted item is handled correctly.
TEST_P(DcpStreamSyncReplTest, BackfillPrepareAbort) {
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
    auto prepared = store_pending_item(
            vbid, "1", "X", {Level::MajorityAndPersistOnMaster, 0});
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

// Test cases which run in both Full and Value eviction
INSTANTIATE_TEST_CASE_P(PersistentAndEphemeral,
                        DcpStreamSyncReplTest,
                        ::testing::Values("persistent", "ephemeral"),
                        [](const ::testing::TestParamInfo<std::string>& info) {
                            return info.param;
                        });
