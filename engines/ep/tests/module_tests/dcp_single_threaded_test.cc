/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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

#include "../mock/mock_dcp.h"
#include "../mock/mock_dcp_conn_map.h"
#include "../mock/mock_dcp_consumer.h"
#include "../mock/mock_dcp_producer.h"
#include "../mock/mock_stream.h"
#include "checkpoint_manager.h"
#include "dcp/consumer.h"
#include "dcp/response.h"
#include "dcp_utils.h"
#include "evp_store_single_threaded_test.h"
#include "kv_bucket.h"

#include <programs/engine_testapp/mock_cookie.h>

class STDcpTest : public STParameterizedBucketTest {
protected:
    /**
     * @param producerState Are we simulating a negotiation against a Producer
     *  that enables IncludeDeletedUserXattrs?
     */
    void testProducerNegotiatesIncludeDeletedUserXattrs(
            IncludeDeletedUserXattrs producerState);

    /**
     * @param producerState Are we simulating a negotiation against a Producer
     *  that enables IncludeDeletedUserXattrs?
     */
    void testConsumerNegotiatesIncludeDeletedUserXattrs(
            IncludeDeletedUserXattrs producerState);
};

/*
 * The following tests that when the disk_backfill_queue configuration is
 * set to false on receiving a snapshot marker it does not move into the
 * backfill phase and the open checkpoint id does not get set to zero.  Also
 * checks that on receiving a subsequent snapshot marker we do not create a
 * second checkpoint.
 */
TEST_P(STDcpTest, test_not_using_backfill_queue) {
    // Make vbucket replica so can add passive stream
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    const void* cookie = create_mock_cookie(engine.get());
    auto& connMap = engine->getDcpConnMap();
    auto* consumer = connMap.newConsumer(cookie, "test_consumer");

    // Add passive stream
    ASSERT_EQ(ENGINE_SUCCESS,
              consumer->addStream(0 /*opaque*/, vbid, 0 /*flags*/));

    // Get the checkpointManager
    auto& manager =
            *(engine->getKVBucket()->getVBucket(vbid)->checkpointManager);

    EXPECT_EQ(0, manager.getOpenCheckpointId());

    // Send a snapshotMarker
    consumer->snapshotMarker(1 /*opaque*/,
                             vbid,
                             0 /*start_seqno*/,
                             1 /*end_seqno*/,
                             MARKER_FLAG_DISK,
                             0 /*HCS*/,
                             {} /*maxVisibleSeqno*/);

    // Even without a checkpoint flag the first disk checkpoint will bump the ID
    EXPECT_EQ(1, manager.getOpenCheckpointId());

    EXPECT_TRUE(engine->getKVBucket()
                        ->getVBucket(vbid)
                        ->isReceivingInitialDiskSnapshot());

    // Create a producer outside of the ConnMap so we don't have to create a new
    // cookie. We'll never add a stream to it so we won't have any ConnMap/Store
    // TearDown issues.
    auto producer = std::make_shared<DcpProducer>(
            *engine, cookie, "test_producer", 0 /*flags*/, false /*startTask*/);

    /*
     * StreamRequest should tmp fail due to the associated vbucket receiving
     * a disk snapshot.
     */
    uint64_t rollbackSeqno = 0;
    auto err = producer->streamRequest(0 /*flags*/,
                                       0 /*opaque*/,
                                       vbid,
                                       0 /*start_seqno*/,
                                       0 /*end_seqno*/,
                                       0 /*vb_uuid*/,
                                       0 /*snap_start*/,
                                       0 /*snap_end*/,
                                       &rollbackSeqno,
                                       fakeDcpAddFailoverLog,
                                       {});

    EXPECT_EQ(ENGINE_TMPFAIL, err);

    // Open checkpoint Id should not be effected.
    EXPECT_EQ(1, manager.getOpenCheckpointId());

    /* Send a mutation */
    const DocKey docKey{nullptr, 0, DocKeyEncodesCollectionId::No};
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->mutation(1 /*opaque*/,
                                 docKey,
                                 {}, // value
                                 0, // priv bytes
                                 PROTOCOL_BINARY_RAW_BYTES,
                                 0, // cas
                                 vbid,
                                 0, // flags
                                 1, // bySeqno
                                 0, // rev seqno
                                 0, // exptime
                                 0, // locktime
                                 {}, // meta
                                 0)); // nru

    // Have received the mutation and so have snapshot end.
    EXPECT_FALSE(engine->getKVBucket()
                         ->getVBucket(vbid)
                         ->isReceivingInitialDiskSnapshot());
    EXPECT_EQ(1, manager.getOpenCheckpointId());

    consumer->snapshotMarker(1 /*opaque*/,
                             vbid,
                             0 /*start_seqno*/,
                             0 /*end_seqno*/,
                             0 /*flags*/,
                             {} /*HCS*/,
                             {} /*maxVisibleSeqno*/);

    // A new opencheckpoint should no be opened
    EXPECT_EQ(1, manager.getOpenCheckpointId());

    // Close stream
    ASSERT_EQ(ENGINE_SUCCESS, consumer->closeStream(0 /*opaque*/, vbid));

    connMap.disconnect(cookie);
    EXPECT_FALSE(connMap.isDeadConnectionsEmpty());
    connMap.manageConnections();
    EXPECT_TRUE(connMap.isDeadConnectionsEmpty());
}

/*
 * The following test has been adapted following the removal of vbucket DCP
 * backfill queue. It now demonstrates some of the 'new' behaviour of
 * DCP snapshot markers  on a replica checkpoint.
 * When snapshots arrive and no items exist, no new CP is created, only the
 * existing snapshot is updated
 */
TEST_P(STDcpTest, SnapshotsAndNoData) {
    // Make vbucket replica so can add passive stream
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    const void* cookie = create_mock_cookie(engine.get());
    auto& connMap = engine->getDcpConnMap();
    auto* consumer = connMap.newConsumer(cookie, "test_consumer");

    // Add passive stream
    ASSERT_EQ(ENGINE_SUCCESS,
              consumer->addStream(0 /*opaque*/, vbid, 0 /*flags*/));

    // Get the checkpointManager
    auto& manager =
            *(engine->getKVBucket()->getVBucket(vbid)->checkpointManager);

    EXPECT_EQ(0, manager.getOpenCheckpointId());

    // Send a snapshotMarker to move the vbucket into a backfilling state
    consumer->snapshotMarker(1 /*opaque*/,
                             vbid,
                             0 /*start_seqno*/,
                             0 /*end_seqno*/,
                             MARKER_FLAG_DISK,
                             0 /*HCS*/,
                             {} /*maxVisibleSeqno*/);

    // Even without a checkpoint flag the first disk checkpoint will bump the ID
    EXPECT_EQ(1, manager.getOpenCheckpointId());

    consumer->snapshotMarker(1 /*opaque*/,
                             vbid,
                             1 /*start_seqno*/,
                             2 /*end_seqno*/,
                             0 /*flags*/,
                             {} /*HCS*/,
                             {} /*maxVisibleSeqno*/);

    EXPECT_EQ(1, manager.getOpenCheckpointId());
    // Still cp:2 but the snap-end changes
    auto snapInfo = manager.getSnapshotInfo();
    EXPECT_EQ(0, snapInfo.range.getStart()); // no data so start is still 0
    EXPECT_EQ(2, snapInfo.range.getEnd());

    // Close stream
    ASSERT_EQ(ENGINE_SUCCESS, consumer->closeStream(/*opaque*/ 0, vbid));

    connMap.disconnect(cookie);
    EXPECT_FALSE(connMap.isDeadConnectionsEmpty());
    connMap.manageConnections();
    EXPECT_TRUE(connMap.isDeadConnectionsEmpty());
}

TEST_P(STDcpTest, AckCorrectPassiveStream) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    const void* cookie1 = create_mock_cookie(engine.get());
    Vbid vbid = Vbid(0);

    auto& connMap = engine->getDcpConnMap();
    auto& mockConnMap = static_cast<MockDcpConnMap&>(connMap);

    // Create our first consumer and enable SyncReplication so that we can ack
    auto consumer1 =
            std::make_shared<MockDcpConsumer>(*engine, cookie1, "consumer1");
    mockConnMap.addConn(cookie1, consumer1);
    consumer1->enableSyncReplication();

    // Add our stream and accept to mimic real scenario and ensure it is alive
    ASSERT_EQ(ENGINE_SUCCESS,
              consumer1->addStream(0 /*opaque*/, vbid, 0 /*flags*/));
    MockPassiveStream* stream1 = static_cast<MockPassiveStream*>(
            (consumer1->getVbucketStream(vbid)).get());
    stream1->acceptStream(cb::mcbp::Status::Success, 0 /*opaque*/);
    ASSERT_TRUE(stream1->isActive());

    ASSERT_EQ(2, stream1->public_readyQ().size());
    auto resp = stream1->public_popFromReadyQ();
    EXPECT_EQ(DcpResponse::Event::StreamReq, resp->getEvent());
    resp = stream1->public_popFromReadyQ();
    EXPECT_EQ(DcpResponse::Event::AddStream, resp->getEvent());
    EXPECT_EQ(0, stream1->public_readyQ().size());

    // Now close the stream to transition to dead
    consumer1->closeStream(0 /*opaque*/, vbid);
    ASSERT_FALSE(stream1->isActive());

    // Add a new consumer and new PassiveStream
    const void* cookie2 = create_mock_cookie(engine.get());
    auto consumer2 =
            std::make_shared<MockDcpConsumer>(*engine, cookie2, "consumer2");
    mockConnMap.addConn(cookie2, consumer2);
    consumer2->enableSyncReplication();
    ASSERT_EQ(ENGINE_SUCCESS,
              consumer2->addStream(1 /*opaque*/, vbid, 0 /*flags*/));
    MockPassiveStream* stream2 = static_cast<MockPassiveStream*>(
            (consumer2->getVbucketStream(vbid)).get());
    stream2->acceptStream(cb::mcbp::Status::Success, 1 /*opaque*/);
    ASSERT_TRUE(stream2->isActive());

    EXPECT_EQ(2, stream2->public_readyQ().size());
    resp = stream2->public_popFromReadyQ();
    EXPECT_EQ(DcpResponse::Event::StreamReq, resp->getEvent());
    resp = stream2->public_popFromReadyQ();
    EXPECT_EQ(DcpResponse::Event::AddStream, resp->getEvent());
    EXPECT_EQ(0, stream2->public_readyQ().size());

    // When we ack we should hit the active stream (stream2)
    engine->getDcpConnMap().seqnoAckVBPassiveStream(vbid, 1);
    EXPECT_EQ(0, stream1->public_readyQ().size());
    EXPECT_EQ(1, stream2->public_readyQ().size());

    ASSERT_EQ(ENGINE_SUCCESS, consumer2->closeStream(1 /*opaque*/, vbid));

    connMap.disconnect(cookie1);
    connMap.disconnect(cookie2);
    EXPECT_FALSE(connMap.isDeadConnectionsEmpty());
    connMap.manageConnections();
    EXPECT_TRUE(connMap.isDeadConnectionsEmpty());
}

void STDcpTest::testProducerNegotiatesIncludeDeletedUserXattrs(
        IncludeDeletedUserXattrs producerState) {
    const void* cookie = create_mock_cookie();

    uint32_t dcpOpenFlags;
    ENGINE_ERROR_CODE expectedControlResp;
    switch (producerState) {
    case IncludeDeletedUserXattrs::Yes: {
        dcpOpenFlags =
                cb::mcbp::request::DcpOpenPayload::IncludeDeletedUserXattrs;
        expectedControlResp = ENGINE_SUCCESS;
        break;
    }
    case IncludeDeletedUserXattrs::No: {
        dcpOpenFlags = 0;
        expectedControlResp = ENGINE_EINVAL;
        break;
    }
    }

    const auto producer = std::make_shared<MockDcpProducer>(
            *engine, cookie, "test_producer", dcpOpenFlags);
    EXPECT_EQ(producerState, producer->public_getIncludeDeletedUserXattrs());
    EXPECT_EQ(expectedControlResp,
              producer->control(0, "include_deleted_user_xattrs", "true"));

    destroy_mock_cookie(cookie);
}

TEST_P(STDcpTest,
       ProducerNegotiatesIncludeDeletedUserXattrs_DisabledAtProducer) {
    testProducerNegotiatesIncludeDeletedUserXattrs(
            IncludeDeletedUserXattrs::No);
}

TEST_P(STDcpTest,
       ProducerNegotiatesIncludeDeletedUserXattrs_EnabledAtProducer) {
    testProducerNegotiatesIncludeDeletedUserXattrs(
            IncludeDeletedUserXattrs::Yes);
}

void STDcpTest::testConsumerNegotiatesIncludeDeletedUserXattrs(
        IncludeDeletedUserXattrs producerState) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize();
    const void* cookie = create_mock_cookie();

    // Create a new Consumer, flag that we want it to support DeleteXattr
    auto& consumer = dynamic_cast<MockDcpConsumer&>(
            *connMap.newConsumer(cookie, "conn_name", "the_consumer_name"));

    using State = DcpConsumer::BlockingDcpControlNegotiation::State;
    auto syncReplNeg = consumer.public_getSyncReplNegotiation();
    ASSERT_EQ(State::PendingRequest, syncReplNeg.state);

    // Consumer::step performs multiple negotiation steps. Some of them are
    // "blocking" steps. So, move the Consumer to the point where all the
    // negotiation steps (but IncludeDeletedUserXattrs) have been completed and
    // verifying that the negotiation of DeletedUserXattrs flows as exptected.
    consumer.setPendingAddStream(false);
    MockDcpMessageProducers producers(engine.get());
    ENGINE_ERROR_CODE result;
    // Step and unblock (ie, simulate Producer response) up to completing the
    // SyncRepl negotiation, which is the last blocking step before the
    // DeletedUserXattrs negotiation
    do {
        result = consumer.step(&producers);
        handleProducerResponseIfStepBlocked(consumer, producers);
        syncReplNeg = consumer.public_getSyncReplNegotiation();
    } while (syncReplNeg.state != State::Completed);
    EXPECT_EQ(ENGINE_SUCCESS, result);

    // Skip over "send consumer name"
    EXPECT_EQ(ENGINE_SUCCESS, consumer.step(&producers));
    ASSERT_FALSE(consumer.public_getPendingSendConsumerName());

    // Check pre-negotiation state
    auto xattrNeg = consumer.public_getDeletedUserXattrsNegotiation();
    ASSERT_EQ(State::PendingRequest, xattrNeg.state);
    ASSERT_EQ(0, xattrNeg.opaque);

    // Start negotiation - consumer sends DcpControl
    EXPECT_EQ(ENGINE_SUCCESS, consumer.step(&producers));
    xattrNeg = consumer.public_getDeletedUserXattrsNegotiation();
    EXPECT_EQ(State::PendingResponse, xattrNeg.state);
    EXPECT_EQ("include_deleted_user_xattrs", producers.last_key);
    EXPECT_EQ("true", producers.last_value);
    EXPECT_GT(xattrNeg.opaque, 0);
    EXPECT_EQ(xattrNeg.opaque, producers.last_opaque);

    // Verify blocked - Consumer cannot proceed until negotiation completes
    EXPECT_EQ(ENGINE_EWOULDBLOCK, consumer.step(&producers));
    xattrNeg = consumer.public_getDeletedUserXattrsNegotiation();
    EXPECT_EQ(State::PendingResponse, xattrNeg.state);

    // Simulate Producer response
    cb::mcbp::Status respStatus =
            (producerState == IncludeDeletedUserXattrs::Yes
                     ? cb::mcbp::Status::Success
                     : cb::mcbp::Status::UnknownCommand);
    protocol_binary_response_header resp{};
    resp.response.setMagic(cb::mcbp::Magic::ClientResponse);
    resp.response.setOpcode(cb::mcbp::ClientOpcode::DcpControl);
    resp.response.setStatus(respStatus);
    resp.response.setOpaque(xattrNeg.opaque);
    consumer.handleResponse(&resp);

    // Verify final consumer state
    xattrNeg = consumer.public_getDeletedUserXattrsNegotiation();
    EXPECT_EQ(State::Completed, xattrNeg.state);
    EXPECT_EQ(producerState, consumer.public_getIncludeDeletedUserXattrs());

    destroy_mock_cookie(cookie);
}

TEST_P(STDcpTest, ConsumerNegotiatesDeletedUserXattrs_DisabledAtProducer) {
    testConsumerNegotiatesIncludeDeletedUserXattrs(
            IncludeDeletedUserXattrs::No);
}

TEST_P(STDcpTest, ConsumerNegotiatesDeletedUserXattrs_EnabledAtProducer) {
    testConsumerNegotiatesIncludeDeletedUserXattrs(
            IncludeDeletedUserXattrs::Yes);
}

INSTANTIATE_TEST_SUITE_P(PersistentAndEphemeral,
                         STDcpTest,
                         STParameterizedBucketTest::allConfigValues());
