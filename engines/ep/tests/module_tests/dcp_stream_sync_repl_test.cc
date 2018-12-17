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

#include "../mock/mock_stream.h"
#include "dcp_stream_test.h"
#include "ep_engine.h"
#include "test_helpers.h"
#include <engines/ep/tests/mock/mock_dcp.h>

#include "checkpoint_manager.h" // DEBUG
/**
 * Test fixture for tests relating to DCP Streams and synchrnous replication.
 */
class DcpStreamSyncReplTest : public StreamTest {};

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

// Test cases which run in both Full and Value eviction
INSTANTIATE_TEST_CASE_P(PersistentAndEphemeral,
                        DcpStreamSyncReplTest,
                        ::testing::Values("persistent", "ephemeral"),
                        [](const ::testing::TestParamInfo<std::string>& info) {
                            return info.param;
                        });
