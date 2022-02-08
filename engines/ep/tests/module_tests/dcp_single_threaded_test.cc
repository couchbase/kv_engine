/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "../mock/mock_dcp.h"
#include "../mock/mock_dcp_conn_map.h"
#include "../mock/mock_dcp_consumer.h"
#include "../mock/mock_dcp_producer.h"
#include "../mock/mock_stream.h"
#include "../mock/mock_synchronous_ep_engine.h"
#include "bucket_logger.h"
#include "checkpoint_manager.h"
#include "dcp/consumer.h"
#include "dcp/response.h"
#include "dcp_utils.h"
#include "evp_store_single_threaded_test.h"
#include "kv_bucket.h"
#include "vbucket.h"

#include <programs/engine_testapp/mock_cookie.h>
#include <spdlog/spdlog.h>

class STDcpTest : public STParameterizedBucketTest {
protected:
    struct StreamRequestResult {
        cb::engine_errc status;
        uint64_t rollbackSeqno;
    };

    // callbackCount needs to be static as its used inside of the static
    // function fakeDcpAddFailoverLog.
    // static int callbackCount;

    /*
     * Fake callback emulating dcp_add_failover_log
     */
    static cb::engine_errc fakeDcpAddFailoverLog(
            const std::vector<vbucket_failover_t>&) {
        // callbackCount++;
        return cb::engine_errc::success;
    }

    StreamRequestResult doStreamRequest(DcpProducer& producer,
                                        uint64_t startSeqno = 0,
                                        uint64_t endSeqno = ~0,
                                        uint64_t snapStart = 0,
                                        uint64_t snapEnd = ~0,
                                        uint64_t vbUUID = 0) {
        StreamRequestResult result;
        result.status = producer.streamRequest(/*flags*/ 0,
                                               /*opaque*/ 0,
                                               vbid,
                                               startSeqno,
                                               endSeqno,
                                               vbUUID,
                                               snapStart,
                                               snapEnd,
                                               &result.rollbackSeqno,
                                               fakeDcpAddFailoverLog,
                                               {});
        return result;
    }

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

    /**
     * Creates a consumer conn and makes the consumer processor task run with
     * memory usage near to replication threshold
     *
     * @param beyondThreshold indicates if the memory usage should above the
     *                        threshold or just below it
     */
    void processConsumerMutationsNearThreshold(bool beyondThreshold);

    /**
     * Creates a consumer conn and sends items on the conn with memory usage
     * near to replication threshold
     *
     * @param beyondThreshold indicates if the memory usage should above the
     *                        threshold or just below it
     */
    void sendConsumerMutationsNearThreshold(bool beyondThreshold);
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

    auto* cookie = create_mock_cookie(engine.get());
    auto& connMap = engine->getDcpConnMap();
    auto* consumer = connMap.newConsumer(cookie, "test_consumer");

    // Add passive stream
    ASSERT_EQ(cb::engine_errc::success,
              consumer->addStream(0 /*opaque*/, vbid, 0 /*flags*/));

    // Get the checkpointManager
    auto& manager =
            *(engine->getKVBucket()->getVBucket(vbid)->checkpointManager);

    EXPECT_EQ(1, manager.getOpenCheckpointId());

    // Send a snapshotMarker
    consumer->snapshotMarker(1 /*opaque*/,
                             vbid,
                             0 /*start_seqno*/,
                             1 /*end_seqno*/,
                             MARKER_FLAG_DISK,
                             0 /*HCS*/,
                             {} /*maxVisibleSeqno*/);

    // Even without a checkpoint flag the first disk checkpoint will bump the ID
    EXPECT_EQ(2, manager.getOpenCheckpointId());

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

    EXPECT_EQ(cb::engine_errc::temporary_failure, err);

    // Open checkpoint Id should not be effected.
    EXPECT_EQ(2, manager.getOpenCheckpointId());

    /* Send a mutation */
    const DocKey docKey{nullptr, 0, DocKeyEncodesCollectionId::No};
    EXPECT_EQ(cb::engine_errc::success,
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
    EXPECT_EQ(2, manager.getOpenCheckpointId());

    consumer->snapshotMarker(1 /*opaque*/,
                             vbid,
                             0 /*start_seqno*/,
                             0 /*end_seqno*/,
                             0 /*flags*/,
                             {} /*HCS*/,
                             {} /*maxVisibleSeqno*/);

    // A new opencheckpoint should no be opened
    EXPECT_EQ(2, manager.getOpenCheckpointId());

    // Close stream
    ASSERT_EQ(cb::engine_errc::success,
              consumer->closeStream(0 /*opaque*/, vbid));

    connMap.disconnect(cookie);
    EXPECT_FALSE(connMap.isDeadConnectionsEmpty());
    connMap.manageConnections();
    EXPECT_TRUE(connMap.isDeadConnectionsEmpty());

    destroy_mock_cookie(cookie);
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

    auto* cookie = create_mock_cookie(engine.get());
    auto& connMap = engine->getDcpConnMap();
    auto* consumer = connMap.newConsumer(cookie, "test_consumer");

    // Add passive stream
    ASSERT_EQ(cb::engine_errc::success,
              consumer->addStream(0 /*opaque*/, vbid, 0 /*flags*/));

    // Get the checkpointManager
    auto& manager =
            *(engine->getKVBucket()->getVBucket(vbid)->checkpointManager);

    EXPECT_EQ(1, manager.getOpenCheckpointId());

    // Send a snapshotMarker to move the vbucket into a backfilling state
    consumer->snapshotMarker(1 /*opaque*/,
                             vbid,
                             0 /*start_seqno*/,
                             1 /*end_seqno*/,
                             MARKER_FLAG_DISK,
                             0 /*HCS*/,
                             {} /*maxVisibleSeqno*/);

    // Even without a checkpoint flag the first disk checkpoint will bump the ID
    EXPECT_EQ(2, manager.getOpenCheckpointId());

    consumer->snapshotMarker(1 /*opaque*/,
                             vbid,
                             2 /*start_seqno*/,
                             2 /*end_seqno*/,
                             0 /*flags*/,
                             {} /*HCS*/,
                             {} /*maxVisibleSeqno*/);

    EXPECT_EQ(3, manager.getOpenCheckpointId());
    // Still cp:2 as checkpoint was empty so we have just re-used it.
    // Snap start/end have been updated to [1, 2], but for an empty checkpoint
    // we return [0, 0]
    auto snapInfo = manager.getSnapshotInfo();
    EXPECT_EQ(0, snapInfo.range.getStart());
    EXPECT_EQ(0, snapInfo.range.getEnd());

    // Close stream
    ASSERT_EQ(cb::engine_errc::success,
              consumer->closeStream(/*opaque*/ 0, vbid));

    connMap.disconnect(cookie);
    EXPECT_FALSE(connMap.isDeadConnectionsEmpty());
    connMap.manageConnections();
    EXPECT_TRUE(connMap.isDeadConnectionsEmpty());

    destroy_mock_cookie(cookie);
}

TEST_P(STDcpTest, AckCorrectPassiveStream) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    auto* cookie1 = create_mock_cookie(engine.get());
    Vbid vbid = Vbid(0);

    auto& connMap = engine->getDcpConnMap();
    auto& mockConnMap = static_cast<MockDcpConnMap&>(connMap);

    // Create our first consumer and enable SyncReplication so that we can ack
    auto consumer1 =
            std::make_shared<MockDcpConsumer>(*engine, cookie1, "consumer1");
    mockConnMap.addConn(cookie1, consumer1);
    consumer1->enableSyncReplication();

    // Add our stream and accept to mimic real scenario and ensure it is alive
    ASSERT_EQ(cb::engine_errc::success,
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
    auto* cookie2 = create_mock_cookie(engine.get());
    auto consumer2 =
            std::make_shared<MockDcpConsumer>(*engine, cookie2, "consumer2");
    mockConnMap.addConn(cookie2, consumer2);
    consumer2->enableSyncReplication();
    ASSERT_EQ(cb::engine_errc::success,
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

    ASSERT_EQ(cb::engine_errc::success,
              consumer2->closeStream(1 /*opaque*/, vbid));

    connMap.disconnect(cookie1);
    connMap.disconnect(cookie2);
    EXPECT_FALSE(connMap.isDeadConnectionsEmpty());
    connMap.manageConnections();
    EXPECT_TRUE(connMap.isDeadConnectionsEmpty());

    destroy_mock_cookie(cookie1);
    destroy_mock_cookie(cookie2);
}

void STDcpTest::testProducerNegotiatesIncludeDeletedUserXattrs(
        IncludeDeletedUserXattrs producerState) {
    auto* cookie = create_mock_cookie();

    uint32_t dcpOpenFlags;
    cb::engine_errc expectedControlResp;
    switch (producerState) {
    case IncludeDeletedUserXattrs::Yes: {
        dcpOpenFlags =
                cb::mcbp::request::DcpOpenPayload::IncludeDeletedUserXattrs;
        expectedControlResp = cb::engine_errc::success;
        break;
    }
    case IncludeDeletedUserXattrs::No: {
        dcpOpenFlags = 0;
        expectedControlResp = cb::engine_errc::invalid_arguments;
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
    auto* cookie = create_mock_cookie();

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
    MockDcpMessageProducers producers;
    cb::engine_errc result;
    // Step and unblock (ie, simulate Producer response) up to completing the
    // SyncRepl negotiation, which is the last blocking step before the
    // DeletedUserXattrs negotiation
    do {
        result = consumer.step(producers);
        handleProducerResponseIfStepBlocked(consumer, producers);
        syncReplNeg = consumer.public_getSyncReplNegotiation();
    } while (syncReplNeg.state != State::Completed);
    EXPECT_EQ(cb::engine_errc::success, result);

    // Skip over "send consumer name"
    EXPECT_EQ(cb::engine_errc::success, consumer.step(producers));
    ASSERT_FALSE(consumer.public_getPendingSendConsumerName());

    // Check pre-negotiation state
    auto xattrNeg = consumer.public_getDeletedUserXattrsNegotiation();
    ASSERT_EQ(State::PendingRequest, xattrNeg.state);
    ASSERT_EQ(0, xattrNeg.opaque);

    // Start negotiation - consumer sends DcpControl
    EXPECT_EQ(cb::engine_errc::success, consumer.step(producers));
    xattrNeg = consumer.public_getDeletedUserXattrsNegotiation();
    EXPECT_EQ(State::PendingResponse, xattrNeg.state);
    EXPECT_EQ("include_deleted_user_xattrs", producers.last_key);
    EXPECT_EQ("true", producers.last_value);
    EXPECT_GT(xattrNeg.opaque, 0);
    EXPECT_EQ(xattrNeg.opaque, producers.last_opaque);

    // Verify blocked - Consumer cannot proceed until negotiation completes
    EXPECT_EQ(cb::engine_errc::would_block, consumer.step(producers));
    xattrNeg = consumer.public_getDeletedUserXattrsNegotiation();
    EXPECT_EQ(State::PendingResponse, xattrNeg.state);

    // Simulate Producer response
    cb::mcbp::Status respStatus =
            (producerState == IncludeDeletedUserXattrs::Yes
                     ? cb::mcbp::Status::Success
                     : cb::mcbp::Status::UnknownCommand);
    cb::mcbp::Response response{};
    response.setMagic(cb::mcbp::Magic::ClientResponse);
    response.setOpcode(cb::mcbp::ClientOpcode::DcpControl);
    response.setStatus(respStatus);
    response.setOpaque(xattrNeg.opaque);
    consumer.handleResponse(response);

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

void STDcpTest::processConsumerMutationsNearThreshold(bool beyondThreshold) {
    auto* cookie = create_mock_cookie(engine.get());
    const uint32_t opaque = 1;
    const uint64_t snapStart = 1, snapEnd = 10;
    const uint64_t bySeqno = snapStart;

    /* Set up a consumer connection */
    auto& connMap = engine->getDcpConnMap();
    auto& mockConnMap = static_cast<MockDcpConnMap&>(connMap);
    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test_consumer");
    mockConnMap.addConn(cookie, consumer);

    /* Replica vbucket */
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    /* Passive stream */
    ASSERT_EQ(cb::engine_errc::success,
              consumer->addStream(/*opaque*/ 0,
                                  vbid,
                                  /*flags*/ 0));
    MockPassiveStream* stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    ASSERT_TRUE(stream->isActive());

    /* Send a snapshotMarker before sending items for replication */
    EXPECT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(opaque,
                                       vbid,
                                       snapStart,
                                       snapEnd,
                                       /* in-memory snapshot */ 0x1,
                                       /*HCS*/ {},
                                       /*maxVisibleSeqno*/ {}));

    /* Simulate a situation where adding a mutation temporarily fails
       and hence adds the mutation to a replication buffer. For that, we
       set vbucket::takeover_backed_up to true */
    engine->getKVBucket()->getVBucket(vbid)->setTakeoverBackedUpState(true);

    /* Send an item for replication and expect it to be buffered */
    const DocKey docKey{"mykey", DocKeyEncodesCollectionId::No};
    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(opaque,
                                 docKey,
                                 {}, // value
                                 0, // priv bytes
                                 PROTOCOL_BINARY_RAW_BYTES,
                                 0, // cas
                                 vbid,
                                 0, // flags
                                 bySeqno,
                                 0, // rev seqno
                                 0, // exptime
                                 0, // locktime
                                 {}, // meta
                                 0)); // nru
    EXPECT_EQ(1, stream->getNumBufferItems());

    /* Set back the vbucket::takeover_backed_up to false */
    engine->getKVBucket()->getVBucket(vbid)->setTakeoverBackedUpState(false);

    /* Set 'mem_used' beyond the 'replication threshold' */
    EPStats& stats = engine->getEpStats();
    if (beyondThreshold) {
        /* Actually setting it well above also, as there can be a drop in memory
           usage during testing */
        stats.setMaxDataSize(stats.getPreciseTotalMemoryUsed() / 4);
    } else {
        /* set max size to a value just over */
        stats.setMaxDataSize(stats.getPreciseTotalMemoryUsed() + 1);
        /* Simpler to set the replication threshold to 1 and test, rather than
           testing with maxData = (memUsed / replicationThrottleThreshold); that
           is, we are avoiding a division */
        engine->getConfiguration().setReplicationThrottleThreshold(100);
    }

    MockDcpMessageProducers producers;
    if ((engine->getConfiguration().getBucketType() == "ephemeral") &&
        (engine->getConfiguration().getEphemeralFullPolicy()) ==
                "fail_new_data") {
        /* Make a call to the function that would be called by the processor
           task here */
        EXPECT_EQ(stop_processing, consumer->processBufferedItems());

        /* Expect the connection to be notified */
        EXPECT_FALSE(consumer->isPaused());

        /* Expect disconnect signal in Ephemeral with "fail_new_data" policy */
        EXPECT_EQ(cb::engine_errc::disconnect, consumer->step(producers));
    } else {
        uint32_t backfoffs = consumer->getNumBackoffs();

        /* Make a call to the function that would be called by the processor
           task here */
        if (beyondThreshold) {
            EXPECT_EQ(more_to_process, consumer->processBufferedItems());
        } else {
            EXPECT_EQ(cannot_process, consumer->processBufferedItems());
        }

        EXPECT_EQ(backfoffs + 1, consumer->getNumBackoffs());

        /* In 'couchbase' buckets we buffer the replica items and indirectly
           throttle replication by not sending flow control acks to the
           producer. Hence we do not drop the connection here */
        EXPECT_EQ(cb::engine_errc::success, consumer->step(producers));

        /* Close stream before deleting the connection */
        EXPECT_EQ(cb::engine_errc::success,
                  consumer->closeStream(opaque, vbid));
    }

    connMap.disconnect(cookie);
    EXPECT_FALSE(connMap.isDeadConnectionsEmpty());
    connMap.manageConnections();
    EXPECT_TRUE(connMap.isDeadConnectionsEmpty());

    destroy_mock_cookie(cookie);
}

/* Here we test how the Processor task in DCP consumer handles the scenario
   where the memory usage is beyond the replication throttle threshold.
   In case of Ephemeral buckets with 'fail_new_data' policy it is expected to
   indicate close of the consumer conn and in other cases it is expected to
   just defer processing. */
TEST_P(STDcpTest, ProcessReplicationBufferAfterThrottleThreshold) {
    processConsumerMutationsNearThreshold(true);
}

/* Here we test how the Processor task in DCP consumer handles the scenario
   where the memory usage is just below the replication throttle threshold,
   but will go over the threshold when it adds the new mutation from the
   processor buffer to the hashtable.
   In case of Ephemeral buckets with 'fail_new_data' policy it is expected to
   indicate close of the consumer conn and in other cases it is expected to
   just defer processing. */
TEST_P(STDcpTest,
       DISABLED_ProcessReplicationBufferJustBeforeThrottleThreshold) {
    /* There are sporadic failures seen while testing this. The problem is
       we need to have a memory usage just below max_size, so we need to
       start at that point. But sometimes the memory usage goes further below
       resulting in the test failure (a hang). Hence commenting out the test.
       Can be run locally as and when needed. */
    processConsumerMutationsNearThreshold(false);
}

/* Checks that the DCP producer does an async stream close when the DCP client
   expects "DCP_STREAM_END" msg. */
TEST_P(STDcpTest, test_producer_stream_end_on_client_close_stream) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto* cookie = create_mock_cookie(engine.get());
    auto& mockConnMap = static_cast<MockDcpConnMap&>(engine->getDcpConnMap());

    /* Create a new Dcp producer */
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);
    mockConnMap.addConn(cookie, producer);
    EXPECT_TRUE(mockConnMap.doesConnHandlerExist("test_producer"));

    /* Send a control message to the producer indicating that the DCP client
       expects a "DCP_STREAM_END" upon stream close */
    const std::string sendStreamEndOnClientStreamCloseCtrlMsg(
            "send_stream_end_on_client_close_stream");
    const std::string sendStreamEndOnClientStreamCloseCtrlValue("true");
    EXPECT_EQ(cb::engine_errc::success,
              producer->control(0,
                                sendStreamEndOnClientStreamCloseCtrlMsg,
                                sendStreamEndOnClientStreamCloseCtrlValue));

    /* Open stream */
    EXPECT_EQ(cb::engine_errc::success, doStreamRequest(*producer).status);
    EXPECT_TRUE(mockConnMap.doesVbConnExist(vbid, "test_producer"));

    /* Close stream */
    EXPECT_EQ(cb::engine_errc::success, producer->closeStream(0, vbid));

    /* Expect a stream end message */
    MockDcpMessageProducers producers;
    EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpStreamEnd, producers.last_op);
    EXPECT_EQ(cb::mcbp::DcpStreamEndStatus::Closed, producers.last_end_status);

    /* Re-open stream for the same vbucket on the conn */
    EXPECT_EQ(cb::engine_errc::success, doStreamRequest(*producer).status);

    /* Check that the new stream is opened properly */
    auto stream = producer->findStream(vbid);
    auto* as = static_cast<ActiveStream*>(stream.get());
    EXPECT_TRUE(as->isInMemory());

    // MB-27769: Prior to the fix, this would fail here because we would skip
    // adding the connhandler into the connmap vbConns vector, causing the
    // stream to never get notified.
    EXPECT_TRUE(mockConnMap.doesVbConnExist(vbid, "test_producer"));

    mockConnMap.disconnect(cookie);
    EXPECT_FALSE(mockConnMap.doesVbConnExist(vbid, "test_producer"));
    mockConnMap.manageConnections();

    destroy_mock_cookie(cookie);
}

/* Checks that the DCP producer does a synchronous stream close when the DCP
   client does not expect "DCP_STREAM_END" msg. */
TEST_P(STDcpTest, test_producer_no_stream_end_on_client_close_stream) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto& connMap = engine->getDcpConnMap();
    auto* cookie = create_mock_cookie(engine.get());

    /* Create a new Dcp producer */
    DcpProducer* producer = connMap.newProducer(cookie,
                                                "test_producer",
                                                /*flags*/ 0);

    /* Open stream */
    EXPECT_EQ(cb::engine_errc::success, doStreamRequest(*producer).status);

    /* Close stream */
    EXPECT_EQ(cb::engine_errc::success, producer->closeStream(0, vbid));

    /* Don't expect a stream end message (or any other message as the stream is
       closed) */
    MockDcpMessageProducers producers;
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(producers));

    /* Check that the stream is not found in the producer's stream map */
    EXPECT_TRUE(producer->findStreams(vbid)->wlock().empty());

    /* Disconnect the producer connection */
    connMap.disconnect(cookie);
    /* Cleanup the deadConnections */
    connMap.manageConnections();

    destroy_mock_cookie(cookie);
}

TEST_P(STDcpTest, test_consumer_add_stream) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    auto& connMap = static_cast<MockDcpConnMap&>(engine->getDcpConnMap());

    auto* cookie = create_mock_cookie(engine.get());
    Vbid vbid = Vbid(0);

    /* Create a Mock Dcp consumer */
    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test_consumer");
    connMap.addConn(cookie, consumer);

    ASSERT_EQ(cb::engine_errc::success,
              consumer->addStream(/*opaque*/ 0,
                                  vbid,
                                  /*flags*/ 0));

    /* Set the passive to dead state. Note that we want to set the stream to
       dead state but not erase it from the streams map in the consumer
       connection*/
    MockPassiveStream* stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());

    stream->transitionStateToDead();

    /* Add a passive stream on the same vb */
    ASSERT_EQ(cb::engine_errc::success,
              consumer->addStream(/*opaque*/ 0,
                                  vbid,
                                  /*flags*/ 0));

    /* Expected the newly added stream to be in active state */
    stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    ASSERT_TRUE(stream->isActive());

    /* Close stream before deleting the connection */
    ASSERT_EQ(cb::engine_errc::success,
              consumer->closeStream(/*opaque*/ 0, vbid));

    connMap.disconnect(cookie);
    EXPECT_FALSE(connMap.isDeadConnectionsEmpty());
    connMap.manageConnections();
    EXPECT_TRUE(connMap.isDeadConnectionsEmpty());

    destroy_mock_cookie(cookie);
}

// Tests that the MutationResponse created for the deletion response is of the
// correct size.
TEST_P(STDcpTest, test_mb24424_deleteResponse) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    auto* cookie = create_mock_cookie(engine.get());
    Vbid vbid = Vbid(0);

    auto& connMap = static_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test_consumer");
    connMap.addConn(cookie, consumer);

    ASSERT_EQ(cb::engine_errc::success,
              consumer->addStream(/*opaque*/ 0,
                                  vbid,
                                  /*flags*/ 0));

    MockPassiveStream* stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    ASSERT_TRUE(stream->isActive());

    std::string key = "key";
    const DocKey docKey{reinterpret_cast<const uint8_t*>(key.data()),
                        key.size(),
                        DocKeyEncodesCollectionId::No};
    std::array<uint8_t, 1> extMeta;
    extMeta[0] = uint8_t(PROTOCOL_BINARY_DATATYPE_JSON);
    cb::const_byte_buffer meta{extMeta.data(), extMeta.size()};

    consumer->deletion(/*opaque*/ 1,
                       /*key*/ docKey,
                       /*value*/ {},
                       /*priv_bytes*/ 0,
                       /*datatype*/ PROTOCOL_BINARY_RAW_BYTES,
                       /*cas*/ 0,
                       /*vbucket*/ vbid,
                       /*bySeqno*/ 1,
                       /*revSeqno*/ 0,
                       /*meta*/ meta);

    auto messageSize = MutationResponse::deletionBaseMsgBytes + key.size() +
                       sizeof(extMeta);

    EXPECT_EQ(messageSize, consumer->getFlowControl().getFreedBytes());

    /* Close stream before deleting the connection */
    ASSERT_EQ(cb::engine_errc::success,
              consumer->closeStream(/*opaque*/ 0, vbid));

    connMap.disconnect(cookie);
    EXPECT_FALSE(connMap.isDeadConnectionsEmpty());
    connMap.manageConnections();
    EXPECT_TRUE(connMap.isDeadConnectionsEmpty());

    destroy_mock_cookie(cookie);
}

// Tests that the MutationResponse created for the mutation response is of the
// correct size.
TEST_P(STDcpTest, test_mb24424_mutationResponse) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    auto* cookie = create_mock_cookie(engine.get());
    Vbid vbid = Vbid(0);

    auto& connMap = static_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test_consumer");
    connMap.addConn(cookie, consumer);

    ASSERT_EQ(cb::engine_errc::success,
              consumer->addStream(/*opaque*/ 0,
                                  vbid,
                                  /*flags*/ 0));

    MockPassiveStream* stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    ASSERT_TRUE(stream->isActive());

    std::string key = "key";
    std::string data = R"({"json":"yes"})";
    const DocKey docKey{reinterpret_cast<const uint8_t*>(key.data()),
                        key.size(),
                        DocKeyEncodesCollectionId::No};
    cb::const_byte_buffer value{reinterpret_cast<const uint8_t*>(data.data()),
                                data.size()};
    std::array<uint8_t, 1> extMeta;
    extMeta[0] = uint8_t(PROTOCOL_BINARY_DATATYPE_JSON);
    cb::const_byte_buffer meta{extMeta.data(), extMeta.size()};

    consumer->mutation(/*opaque*/ 1,
                       /*key*/ docKey,
                       /*values*/ value,
                       /*priv_bytes*/ 0,
                       /*datatype*/ PROTOCOL_BINARY_DATATYPE_JSON,
                       /*cas*/ 0,
                       /*vbucket*/ vbid,
                       /*flags*/ 0,
                       /*bySeqno*/ 1,
                       /*revSeqno*/ 0,
                       /*exptime*/ 0,
                       /*lock_time*/ 0,
                       /*meta*/ meta,
                       /*nru*/ 0);

    auto messageSize = MutationResponse::mutationBaseMsgBytes + key.size() +
                       data.size() + sizeof(extMeta);

    EXPECT_EQ(messageSize, consumer->getFlowControl().getFreedBytes());

    /* Close stream before deleting the connection */
    ASSERT_EQ(cb::engine_errc::success,
              consumer->closeStream(/*opaque*/ 0, vbid));

    connMap.disconnect(cookie);
    EXPECT_FALSE(connMap.isDeadConnectionsEmpty());
    connMap.manageConnections();
    EXPECT_TRUE(connMap.isDeadConnectionsEmpty());

    destroy_mock_cookie(cookie);
}

void STDcpTest::sendConsumerMutationsNearThreshold(bool beyondThreshold) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    auto* cookie = create_mock_cookie(engine.get());
    const uint32_t opaque = 1;
    const uint64_t snapStart = 1;
    const uint64_t snapEnd = std::numeric_limits<uint64_t>::max();
    uint64_t bySeqno = snapStart;

    /* Set up a consumer connection */
    auto& connMap = static_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test_consumer");
    connMap.addConn(cookie, consumer);

    /* Passive stream */
    ASSERT_EQ(cb::engine_errc::success,
              consumer->addStream(/*opaque*/ 0,
                                  vbid,
                                  /*flags*/ 0));
    MockPassiveStream* stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    ASSERT_TRUE(stream->isActive());

    /* Send a snapshotMarker before sending items for replication */
    EXPECT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(opaque,
                                       vbid,
                                       snapStart,
                                       snapEnd,
                                       /* in-memory snapshot */ 0x1,
                                       {} /*HCS*/,
                                       {} /*maxVisibleSeq*/));

    /* Send an item for replication */
    const DocKey docKey{nullptr, 0, DocKeyEncodesCollectionId::No};
    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(opaque,
                                 docKey,
                                 {}, // value
                                 0, // priv bytes
                                 PROTOCOL_BINARY_RAW_BYTES,
                                 0, // cas
                                 vbid,
                                 0, // flags
                                 bySeqno,
                                 0, // rev seqno
                                 0, // exptime
                                 0, // locktime
                                 {}, // meta
                                 0)); // nru

    /* Set 'mem_used' beyond the 'replication threshold' */
    EPStats& stats = engine->getEpStats();
    if (beyondThreshold) {
        engine->setMaxDataSize(stats.getPreciseTotalMemoryUsed());
    } else {
        /* Set 'mem_used' just 1 byte less than the 'replication threshold'.
           That is we are below 'replication threshold', but not enough space
           for  the new item */
        engine->setMaxDataSize(stats.getPreciseTotalMemoryUsed() + 1);
        /* Simpler to set the replication threshold to 1 and test, rather than
           testing with maxData = (memUsed / replicationThrottleThreshold);
           that is, we are avoiding a division */
        engine->getConfiguration().setReplicationThrottleThreshold(100);
    }

    if ((engine->getConfiguration().getBucketType() == "ephemeral") &&
        (engine->getConfiguration().getEphemeralFullPolicy()) ==
                "fail_new_data") {
        /* Expect disconnect signal in Ephemeral with "fail_new_data" policy */
        while (true) {
            /* Keep sending items till the memory usage goes above the
               threshold and the connection is disconnected */
            if (cb::engine_errc::disconnect ==
                consumer->mutation(opaque,
                                   docKey,
                                   {}, // value
                                   0, // priv bytes
                                   PROTOCOL_BINARY_RAW_BYTES,
                                   0, // cas
                                   vbid,
                                   0, // flags
                                   ++bySeqno,
                                   0, // rev seqno
                                   0, // exptime
                                   0, // locktime
                                   {}, // meta
                                   0)) {
                break;
            }
        }
    } else {
        /* In 'couchbase' buckets we buffer the replica items and indirectly
           throttle replication by not sending flow control acks to the
           producer. Hence we do not drop the connection here */
        EXPECT_EQ(cb::engine_errc::success,
                  consumer->mutation(opaque,
                                     docKey,
                                     {}, // value
                                     0, // priv bytes
                                     PROTOCOL_BINARY_RAW_BYTES,
                                     0, // cas
                                     vbid,
                                     0, // flags
                                     bySeqno + 1,
                                     0, // rev seqno
                                     0, // exptime
                                     0, // locktime
                                     {}, // meta
                                     0)); // nru
    }

    /* Close stream before deleting the connection */
    EXPECT_EQ(cb::engine_errc::success, consumer->closeStream(opaque, vbid));

    connMap.disconnect(cookie);
    EXPECT_FALSE(connMap.isDeadConnectionsEmpty());
    connMap.manageConnections();
    EXPECT_TRUE(connMap.isDeadConnectionsEmpty());

    destroy_mock_cookie(cookie);
}

/* Here we test how the DCP consumer handles the scenario where the memory
   usage is beyond the replication throttle threshold.
   In case of Ephemeral buckets with 'fail_new_data' policy it is expected to
   indicate close of the consumer conn and in other cases it is expected to
   just defer processing. */
TEST_P(STDcpTest, ReplicateAfterThrottleThreshold) {
    sendConsumerMutationsNearThreshold(true);
}

/* Here we test how the DCP consumer handles the scenario where the memory
   usage is just below the replication throttle threshold, but will go over the
   threshold when it adds the new mutation from the processor buffer to the
   hashtable.
   In case of Ephemeral buckets with 'fail_new_data' policy it is expected to
   indicate close of the consumer conn and in other cases it is expected to
   just defer processing. */
TEST_P(STDcpTest, ReplicateJustBeforeThrottleThreshold) {
    sendConsumerMutationsNearThreshold(false);
}

// Test that reprocessing a stream-end is safe
TEST_P(STDcpTest, MB_45863) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto* cookie = create_mock_cookie(engine.get());
    auto& mockConnMap = static_cast<MockDcpConnMap&>(engine->getDcpConnMap());

    /* Create a new Dcp producer */
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);
    mockConnMap.addConn(cookie, producer);
    EXPECT_TRUE(mockConnMap.doesConnHandlerExist("test_producer"));

    /* Send a control message to the producer indicating that the DCP client
       expects a "DCP_STREAM_END" upon stream close */
    const std::string sendStreamEndOnClientStreamCloseCtrlMsg(
            "send_stream_end_on_client_close_stream");
    const std::string sendStreamEndOnClientStreamCloseCtrlValue("true");
    EXPECT_EQ(cb::engine_errc::success,
              producer->control(0,
                                sendStreamEndOnClientStreamCloseCtrlMsg,
                                sendStreamEndOnClientStreamCloseCtrlValue));

    /* Open stream */
    EXPECT_EQ(cb::engine_errc::success, doStreamRequest(*producer).status);
    EXPECT_TRUE(mockConnMap.doesVbConnExist(vbid, "test_producer"));

    /* Close stream */
    EXPECT_EQ(cb::engine_errc::success, producer->closeStream(0, vbid));

    // Expect a stream end message, process this using two steps, the first will
    // see 'too_big' and stash the response for the next call to step
    class ForceStreamEndFailure : public MockDcpMessageProducers {
    public:
        cb::engine_errc stream_end(uint32_t opaque,
                                   Vbid vbucket,
                                   cb::mcbp::DcpStreamEndStatus status,
                                   cb::mcbp::DcpStreamId sid) override {
            return cb::engine_errc::too_big;
        }
    } producers1;
    EXPECT_EQ(cb::engine_errc::too_big, producer->step(producers1));
    EXPECT_TRUE(producer->findStream(vbid));

    // Now expect a stream end message to send, this would throw before fixing
    // MB-45863
    MockDcpMessageProducers producers2;
    EXPECT_EQ(cb::engine_errc::success, producer->step(producers2));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpStreamEnd, producers2.last_op);
    EXPECT_EQ(cb::mcbp::DcpStreamEndStatus::Closed, producers2.last_end_status);
    EXPECT_FALSE(producer->findStream(vbid));
    mockConnMap.disconnect(cookie);
    mockConnMap.manageConnections();

    destroy_mock_cookie(cookie);
}

TEST_P(STDcpTest, ConnHandlerLoggerRegisterUnregister) {
    std::string loggerName;
    {
        auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                          cookie,
                                                          "test_producer",
                                                          /*flags*/ 0);
        loggerName = producer->getLogger().name();
        EXPECT_TRUE(spdlog::get(loggerName));
    }
    EXPECT_FALSE(spdlog::get(loggerName));
}

INSTANTIATE_TEST_SUITE_P(PersistentAndEphemeral,
                         STDcpTest,
                         STParameterizedBucketTest::allConfigValues());
