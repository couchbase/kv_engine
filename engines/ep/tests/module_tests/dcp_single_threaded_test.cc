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
        result.status = producer.streamRequest(cb::mcbp::DcpAddStreamFlag::None,
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
     * Creates a producer and consumer and checks if they are disconnected on
     * vbucket deletion
     *
     * @param state initial vbucket state
     * @param setDead whether to set vbucket state to dead before deletion
     */
    void testDeleteVBucketClosesConnections(vbucket_state_t state,
                                            bool setDead);

    /**
     * @param producerState Are we simulating a negotiation against a Producer
     *  that enables IncludeDeletedUserXattrs?
     */
    void testProducerNegotiatesIncludeDeletedUserXattrs(
            IncludeDeletedUserXattrs producerState);

    /**
     * Creates a consumer conn and sends items on the conn with memory usage
     * near to replication threshold
     *
     * @param beyondThreshold indicates if the memory usage should above the
     *                        threshold or just below it
     */
    void sendConsumerMutationsNearThreshold(bool beyondThreshold);
};

class STDcpConsumerTest : public STParameterizedBucketTest {
public:
    void SetUp() override {
        STParameterizedBucketTest::SetUp();
        connMap = std::make_unique<MockDcpConnMap>(*engine);
        connMap->initialize();
        cookie = create_mock_cookie();
        consumer = dynamic_cast<MockDcpConsumer*>(
                connMap->newConsumer(*cookie, "STDcpConsumerTest", "consumer"));
        EXPECT_TRUE(consumer);
    }

    void TearDown() override {
        destroy_mock_cookie(cookie);
        connMap.reset();
        STParameterizedBucketTest::TearDown();
    }

    void testConsumerNegotiates(
            cb::mcbp::Status producerStatus,
            std::string_view testControl,
            std::string_view value,
            std::function<bool(const MockDcpConsumer&)> validate);

    std::unique_ptr<MockDcpConnMap> connMap;
    MockDcpConsumer* consumer{nullptr};
    MockCookie* cookie;
};

class DcpStepper {
public:
    DcpStepper(CookieIface* cookie_, EventuallyPersistentEngine* engine_)
        : cookie{cookie_}, engine{engine_} {
    }

    void operator()() {
        cb::engine_errc ret;
        while (ret = engine->step(*cookie, false, producers),
               ret != cb::engine_errc::would_block) {
            EXPECT_EQ(cb::engine_errc::success, ret);
        }
    }

protected:
    CookieIface* cookie;
    EventuallyPersistentEngine* engine;
    MockDcpMessageProducers producers;
};

void STDcpTest::testDeleteVBucketClosesConnections(vbucket_state_t state,
                                                   bool setDead) {
    setVBucketStateAndRunPersistTask(vbid, state);

    auto& connMap = engine->getDcpConnMap();
    auto* cookie = create_mock_cookie(engine.get());
    ConnHandler* handler;

    if (state == vbucket_state_active) {
        auto* producer = connMap.newProducer(*cookie, "test_producer", {});
        // Open stream
        ASSERT_EQ(cb::engine_errc::success, doStreamRequest(*producer).status);
        handler = producer;
    } else if (state == vbucket_state_replica) {
        auto* consumer = connMap.newConsumer(*cookie, "test_consumer");
        // Add passive stream
        ASSERT_EQ(
                cb::engine_errc::success,
                consumer->addStream(
                        0 /*opaque*/, vbid, cb::mcbp::DcpAddStreamFlag::None));
        handler = consumer;
    } else {
        FAIL();
    }

    DcpStepper stepDcp{cookie, engine.get()};
    stepDcp();
    EXPECT_TRUE(connMap.vbConnectionExists(handler, vbid));

    if (setDead) {
        store->setVBucketState(vbid, vbucket_state_dead);
        stepDcp();
        if (state == vbucket_state_active) {
            // Producer connection is closed for any state change
            EXPECT_FALSE(connMap.vbConnectionExists(handler, vbid));
        } else {
            EXPECT_TRUE(connMap.vbConnectionExists(handler, vbid));
        }
    }

    store->deleteVBucket(vbid);
    stepDcp();
    EXPECT_FALSE(connMap.vbConnectionExists(handler, vbid));

    connMap.disconnect(cookie);
    connMap.manageConnections();
    destroy_mock_cookie(cookie);
}

TEST_P(STDcpTest, DeleteVBucketClosesConnections) {
    testDeleteVBucketClosesConnections(vbucket_state_active, false);
    testDeleteVBucketClosesConnections(vbucket_state_active, true);
    testDeleteVBucketClosesConnections(vbucket_state_replica, false);
    testDeleteVBucketClosesConnections(vbucket_state_replica, true);
}

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
    auto* consumer = connMap.newConsumer(*cookie, "test_consumer");

    // Add passive stream
    ASSERT_EQ(cb::engine_errc::success,
              consumer->addStream(
                      0 /*opaque*/, vbid, cb::mcbp::DcpAddStreamFlag::None));

    // Get the checkpointManager
    auto& manager =
            *(engine->getKVBucket()->getVBucket(vbid)->checkpointManager);

    EXPECT_EQ(1, manager.getOpenCheckpointId());

    // Send a snapshotMarker
    consumer->snapshotMarker(1 /*opaque*/,
                             vbid,
                             0 /*start_seqno*/,
                             1 /*end_seqno*/,
                             DcpSnapshotMarkerFlag::Disk,
                             0 /*HCS*/,
                             {} /*HPS*/,
                             {} /*maxVisibleSeqno*/,
                             {} /*purgeSeqno*/);

    // Even without a checkpoint flag the first disk checkpoint will bump the ID
    EXPECT_EQ(2, manager.getOpenCheckpointId());

    EXPECT_TRUE(engine->getKVBucket()
                        ->getVBucket(vbid)
                        ->isReceivingInitialDiskSnapshot());

    // Create a producer outside of the ConnMap so we don't have to create a new
    // cookie. We'll never add a stream to it so we won't have any ConnMap/Store
    // TearDown issues.
    auto producer = std::make_shared<DcpProducer>(*engine,
                                                  cookie,
                                                  "test_producer",
                                                  cb::mcbp::DcpOpenFlag::None,
                                                  false /*startTask*/);

    /*
     * StreamRequest should tmp fail due to the associated vbucket receiving
     * a disk snapshot.
     */
    uint64_t rollbackSeqno = 0;
    auto err = producer->streamRequest(cb::mcbp::DcpAddStreamFlag::None,
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
    const DocKeyView docKey{nullptr, 0, DocKeyEncodesCollectionId::No};
    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(1 /*opaque*/,
                                 docKey,
                                 {}, // value
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
                             {} /*flags*/,
                             {} /*HCS*/,
                             {} /*HPS*/,
                             {} /*maxVisibleSeqno*/,
                             {} /*purgeSeqno*/);

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
    auto* consumer = connMap.newConsumer(*cookie, "test_consumer");

    // Add passive stream
    ASSERT_EQ(cb::engine_errc::success,
              consumer->addStream(
                      0 /*opaque*/, vbid, cb::mcbp::DcpAddStreamFlag::None));

    // Get the checkpointManager
    auto& manager =
            *(engine->getKVBucket()->getVBucket(vbid)->checkpointManager);

    EXPECT_EQ(1, manager.getOpenCheckpointId());

    // Send a snapshotMarker to move the vbucket into a backfilling state
    consumer->snapshotMarker(1 /*opaque*/,
                             vbid,
                             0 /*start_seqno*/,
                             1 /*end_seqno*/,
                             DcpSnapshotMarkerFlag::Disk,
                             0 /*HCS*/,
                             {} /*HPS*/,
                             {} /*maxVisibleSeqno*/,
                             {} /*purgeSeqno*/);

    // Even without a checkpoint flag the first disk checkpoint will bump the ID
    EXPECT_EQ(2, manager.getOpenCheckpointId());

    consumer->snapshotMarker(1 /*opaque*/,
                             vbid,
                             2 /*start_seqno*/,
                             2 /*end_seqno*/,
                             {} /*flags*/,
                             {} /*HCS*/,
                             {} /*HPS*/,
                             {} /*maxVisibleSeqno*/,
                             {} /*purgeSeqno*/);

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
              consumer1->addStream(
                      0 /*opaque*/, vbid, cb::mcbp::DcpAddStreamFlag::None));
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
              consumer2->addStream(
                      1 /*opaque*/, vbid, cb::mcbp::DcpAddStreamFlag::None));
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

    cb::mcbp::DcpOpenFlag dcpOpenFlags = {};
    cb::engine_errc expectedControlResp;
    switch (producerState) {
    case IncludeDeletedUserXattrs::Yes:
        dcpOpenFlags = cb::mcbp::DcpOpenFlag::IncludeDeletedUserXattrs;
        expectedControlResp = cb::engine_errc::success;
        break;
    case IncludeDeletedUserXattrs::No:
        dcpOpenFlags = {};
        expectedControlResp = cb::engine_errc::invalid_arguments;
        break;
    }

    const auto producer = std::make_shared<MockDcpProducer>(
            *engine, cookie, "test_producer", dcpOpenFlags);
    EXPECT_EQ(producerState, producer->public_getIncludeDeletedUserXattrs());
    EXPECT_EQ(expectedControlResp,
              producer->control(
                      0, DcpControlKeys::IncludeDeletedUserXattrs, "true"));

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

// "generic" test that iteates through Consumer dcp control and validates
// consumer reflects negoiation result
void STDcpConsumerTest::testConsumerNegotiates(
        cb::mcbp::Status producerStatus,
        std::string_view testControl,
        std::string_view value,
        std::function<bool(const MockDcpConsumer&)> validate) {
    consumer->setPendingAddStream(false);
    MockDcpMessageProducers producers;

    // Keep stepping until the current control negotiation is the one the test
    // cares about.
    while (consumer->public_getCurrentControlNegotiation().key != testControl) {
        EXPECT_EQ(cb::engine_errc::success, consumer->step(false, producers));
        handleProducerResponseIfStepBlocked(*consumer, producers);
    }

    // Check pre-negotiation state
    {
        auto& control = consumer->public_getCurrentControlNegotiation();
        ASSERT_FALSE(control.opaque.has_value());
    }

    // Start negotiation - consumer sends DcpControl
    EXPECT_EQ(cb::engine_errc::success, consumer->step(false, producers));
    EXPECT_EQ(testControl, producers.last_key);
    EXPECT_EQ(value, producers.last_value);

    {
        // State changed and an opaque assigned.
        auto& control = consumer->public_getCurrentControlNegotiation();
        EXPECT_GT(control.opaque.value(), 0);
        EXPECT_EQ(control.opaque.value(), producers.last_opaque);
    }

    // Verify blocked - Consumer cannot proceed until negotiation completes
    EXPECT_EQ(cb::engine_errc::would_block, consumer->step(false, producers));
    {
        auto& control = consumer->public_getCurrentControlNegotiation();
        EXPECT_EQ(testControl, control.key);
    }

    // Producer response based on test input.
    cb::mcbp::Response response{};
    response.setMagic(cb::mcbp::Magic::ClientResponse);
    response.setOpcode(cb::mcbp::ClientOpcode::DcpControl);
    response.setStatus(producerStatus);
    EXPECT_TRUE(
            consumer->public_getCurrentControlNegotiation().opaque.has_value());
    response.setOpaque(*consumer->public_getCurrentControlNegotiation().opaque);
    consumer->handleResponse(response);

    EXPECT_TRUE(validate(*consumer));
}

// Here we test how the Processor task in DCP consumer handles the scenario
// where the memory usage is beyond the replication throttle threshold.
// In the case of Ephemeral/fail_new_data it is expected to trigger the consumer
// disconnection. In the other cases it is expected to just forcibly processing
// the item but deferring to acknowledge bytes to the producer.
TEST_P(STDcpTest, ProcessUnackedBytesAtReplicationOOM) {
    auto* cookie = create_mock_cookie(engine.get());
    const uint32_t opaque = 1;
    const uint64_t snapStart = 1, snapEnd = 10;
    const uint64_t seqno = snapStart;

    // Set up a consumer connection
    auto& connMap = engine->getDcpConnMap();
    auto& mockConnMap = static_cast<MockDcpConnMap&>(connMap);
    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test_consumer");
    mockConnMap.addConn(cookie, consumer);

    // Replica vbucket
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    // Passive stream
    ASSERT_EQ(cb::engine_errc::success,
              consumer->addStream(
                      /*opaque*/ 0, vbid, cb::mcbp::DcpAddStreamFlag::None));
    MockPassiveStream* stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    ASSERT_TRUE(stream->isActive());

    // Receive a marker
    EXPECT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(opaque,
                                       vbid,
                                       snapStart,
                                       snapEnd,
                                       DcpSnapshotMarkerFlag::Memory,
                                       /*HCS*/ {},
                                       /*HPS*/ {},
                                       /*maxVisibleSeqno*/ {},
                                       /*purgeSeqno*/ {}));

    // Simulate OOM on the node
    auto& config = engine->getConfiguration();
    config.setMutationMemRatio(0);

    auto& vb = *store->getVBucket(vbid);
    ASSERT_EQ(0, vb.getHighSeqno());
    ASSERT_EQ(0, stream->getUnackedBytes());

    // Receive an item. At OOM the item is queued into the checkpoint but not
    // acked back to the Producer.
    const auto res = consumer->mutation(opaque,
                                        {"key", DocKeyEncodesCollectionId::No},
                                        {}, // value
                                        PROTOCOL_BINARY_RAW_BYTES,
                                        0, // cas
                                        vbid,
                                        0, // flags
                                        seqno,
                                        0, // rev seqno
                                        0, // exptime
                                        0, // locktime
                                        {}, // meta
                                        0); // nru
    const auto unackedBytes = stream->getUnackedBytes();
    const auto backfoffs = consumer->getNumBackoffs();
    EXPECT_EQ(0, backfoffs);

    MockDcpMessageProducers producers;
    if (ephemeralFailNewData()) {
        // Expect disconnect signal in ephemeral/fail_new_data.
        EXPECT_EQ(cb::engine_errc::disconnect, res);
        EXPECT_EQ(0, vb.getHighSeqno());
        EXPECT_EQ(0, unackedBytes);

        // Expect the connection to be notified
        EXPECT_FALSE(consumer->isPaused());

        // Simulate the DcpConsumerTask - Nothing to process, we are just
        // disconnecting
        EXPECT_EQ(all_processed, consumer->processUnackedBytes());
    } else {
        // We process the replica items but indirectly throttle replication by
        // not sending flow control acks to the roducer. Hence we do not drop
        // the connection here.
        EXPECT_EQ(cb::engine_errc::success, res);
        EXPECT_EQ(1, vb.getHighSeqno());
        EXPECT_EQ(unackedBytes, stream->getUnackedBytes());

        // Simulate the DcpConsumerTask
        EXPECT_EQ(more_to_process, consumer->processUnackedBytes());

        // Still OOM
        EXPECT_EQ(backfoffs + 1, consumer->getNumBackoffs());
    }

    connMap.disconnect(cookie);
    EXPECT_FALSE(connMap.isDeadConnectionsEmpty());
    connMap.manageConnections();
    EXPECT_TRUE(connMap.isDeadConnectionsEmpty());

    destroy_mock_cookie(cookie);
}

/* Checks that the DCP producer does an async stream close when the DCP client
   expects "DCP_STREAM_END" msg. */
TEST_P(STDcpTest, test_producer_stream_end_on_client_close_stream) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto* cookie = create_mock_cookie(engine.get());
    auto& mockConnMap = static_cast<MockDcpConnMap&>(engine->getDcpConnMap());

    /* Create a new Dcp producer */
    auto producer = std::make_shared<MockDcpProducer>(
            *engine, cookie, "test_producer", cb::mcbp::DcpOpenFlag::None);
    mockConnMap.addConn(cookie, producer);
    EXPECT_TRUE(mockConnMap.doesConnHandlerExist("test_producer"));

    /* Send a control message to the producer indicating that the DCP client
       expects a "DCP_STREAM_END" upon stream close */
    EXPECT_EQ(
            cb::engine_errc::success,
            producer->control(0,
                              DcpControlKeys::SendStreamEndOnClientCloseStream,
                              "true"));

    /* Open stream */
    EXPECT_EQ(cb::engine_errc::success, doStreamRequest(*producer).status);
    EXPECT_TRUE(mockConnMap.doesVbConnExist(vbid, "test_producer"));

    /* Close stream */
    EXPECT_EQ(cb::engine_errc::success, producer->closeStream(0, vbid));

    /* Expect a stream end message */
    MockDcpMessageProducers producers;
    EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpStreamEnd, producers.last_op);
    EXPECT_EQ(cb::mcbp::DcpStreamEndStatus::Closed, producers.last_end_status);

    /* Re-open stream for the same vbucket on the conn */
    EXPECT_EQ(cb::engine_errc::success, doStreamRequest(*producer).status);

    /* Check that the new stream is opened properly */
    auto stream = producer->findStream(vbid);
    auto* as = stream.get();
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
    DcpProducer* producer = connMap.newProducer(*cookie,
                                                "test_producer",
                                                /*flags*/ {});

    /* Open stream */
    EXPECT_EQ(cb::engine_errc::success, doStreamRequest(*producer).status);

    /* Close stream */
    EXPECT_EQ(cb::engine_errc::success, producer->closeStream(0, vbid));

    /* Don't expect a stream end message (or any other message as the stream is
       closed) */
    MockDcpMessageProducers producers;
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(false, producers));

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
              consumer->addStream(
                      /*opaque*/ 0, vbid, cb::mcbp::DcpAddStreamFlag::None));

    /* Set the passive to dead state. Note that we want to set the stream to
       dead state but not erase it from the streams map in the consumer
       connection*/
    MockPassiveStream* stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());

    stream->transitionStateToDead();

    /* Add a passive stream on the same vb */
    ASSERT_EQ(cb::engine_errc::success,
              consumer->addStream(
                      /*opaque*/ 0, vbid, cb::mcbp::DcpAddStreamFlag::None));

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
              consumer->addStream(
                      /*opaque*/ 0, vbid, cb::mcbp::DcpAddStreamFlag::None));

    MockPassiveStream* stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    ASSERT_TRUE(stream->isActive());

    std::string key = "key";
    const DocKeyView docKey{reinterpret_cast<const uint8_t*>(key.data()),
                            key.size(),
                            DocKeyEncodesCollectionId::No};
    std::array<uint8_t, 1> extMeta;
    extMeta[0] = uint8_t(PROTOCOL_BINARY_DATATYPE_JSON);
    cb::const_byte_buffer meta{extMeta.data(), extMeta.size()};

    consumer->deletion(/*opaque*/ 1,
                       /*key*/ docKey,
                       /*value*/ {},
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
              consumer->addStream(
                      /*opaque*/ 0, vbid, cb::mcbp::DcpAddStreamFlag::None));

    MockPassiveStream* stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    ASSERT_TRUE(stream->isActive());

    std::string key = "key";
    std::string data = R"({"json":"yes"})";
    const DocKeyView docKey{reinterpret_cast<const uint8_t*>(key.data()),
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
              consumer->addStream(
                      /*opaque*/ 0, vbid, cb::mcbp::DcpAddStreamFlag::None));
    MockPassiveStream* stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    ASSERT_TRUE(stream->isActive());

    /* Send a snapshotMarker before sending items for replication */
    EXPECT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(opaque,
                                       vbid,
                                       snapStart,
                                       snapEnd,
                                       DcpSnapshotMarkerFlag::Memory,
                                       {} /*HCS*/,
                                       {} /*HPS*/,
                                       {} /*maxVisibleSeq*/,
                                       {} /*purgeSeqno*/));

    /* Send an item for replication */
    const DocKeyView docKey{nullptr, 0, DocKeyEncodesCollectionId::No};
    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(opaque,
                                 docKey,
                                 {}, // value
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
           testing with maxData = (memUsed * mutationMemRatio);
           that is, we are avoiding a division */
        engine->getConfiguration().setMutationMemRatio(1.0);
    }

    if (ephemeralFailNewData()) {
        /* Expect disconnect signal in Ephemeral with "fail_new_data" policy */
        size_t ii = 0;
        cb::engine_errc ret;
        do {
            const auto keyI = std::to_string(ii++);
            const StoredDocKey docKeyI{keyI, CollectionID::Default};
            /* Keep sending items till the memory usage goes above the
               threshold and the connection is disconnected */
            ret = consumer->mutation(opaque,
                                     docKeyI,
                                     {}, // value
                                     PROTOCOL_BINARY_RAW_BYTES,
                                     0, // cas
                                     vbid,
                                     0, // flags
                                     ++bySeqno,
                                     0, // rev seqno
                                     0, // exptime
                                     0, // locktime
                                     {}, // meta
                                     0);
        } while (ret == cb::engine_errc::success);
        EXPECT_EQ(cb::engine_errc::disconnect, ret);
    } else {
        /* In 'couchbase' buckets we buffer the replica items and indirectly
           throttle replication by not sending flow control acks to the
           producer. Hence we do not drop the connection here */
        EXPECT_EQ(cb::engine_errc::success,
                  consumer->mutation(opaque,
                                     docKey,
                                     {}, // value
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
    auto producer = std::make_shared<MockDcpProducer>(
            *engine, cookie, "test_producer", cb::mcbp::DcpOpenFlag::None);
    mockConnMap.addConn(cookie, producer);
    EXPECT_TRUE(mockConnMap.doesConnHandlerExist("test_producer"));

    /* Send a control message to the producer indicating that the DCP client
       expects a "DCP_STREAM_END" upon stream close */
    EXPECT_EQ(
            cb::engine_errc::success,
            producer->control(0,
                              DcpControlKeys::SendStreamEndOnClientCloseStream,
                              "true"));

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
    EXPECT_EQ(cb::engine_errc::too_big, producer->step(false, producers1));
    EXPECT_TRUE(producer->findStream(vbid));

    // Now expect a stream end message to send, this would throw before fixing
    // MB-45863
    MockDcpMessageProducers producers2;
    EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers2));
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
        auto producer = std::make_shared<MockDcpProducer>(
                *engine, cookie, "test_producer", cb::mcbp::DcpOpenFlag::None);
        loggerName = producer->getLogger().name();
        EXPECT_TRUE(spdlog::get(loggerName));
    }
    EXPECT_FALSE(spdlog::get(loggerName));
}

TEST_P(STDcpTest, ProducerNegotiatesFlatBuffers) {
    auto* cookie = create_mock_cookie();
    const auto producer = std::make_shared<MockDcpProducer>(
            *engine, cookie, "test_producer", cb::mcbp::DcpOpenFlag::None);
    // Disables by default
    ASSERT_FALSE(producer->areFlatBuffersSystemEventsEnabled());
    // DCP_CONTROL validation
    EXPECT_EQ(cb::engine_errc::invalid_arguments,
              producer->control(
                      0, DcpControlKeys::FlatBuffersSystemEvents, "not-true"));
    EXPECT_FALSE(producer->areFlatBuffersSystemEventsEnabled());
    // Enabled
    EXPECT_EQ(cb::engine_errc::success,
              producer->control(
                      0, DcpControlKeys::FlatBuffersSystemEvents, "true"));
    EXPECT_TRUE(producer->areFlatBuffersSystemEventsEnabled());
    destroy_mock_cookie(cookie);
}

TEST_P(STDcpTest, ProducerNegotiatesCDC) {
    if (!isMagma()) {
        GTEST_SKIP();
    }

    // @todo CDC: Can remove when magma has enabled history support
    replaceMagmaKVStore();

    auto* cookie = create_mock_cookie();
    const auto producer = std::make_shared<MockDcpProducer>(
            *engine, cookie, "test_producer", cb::mcbp::DcpOpenFlag::None);
    ASSERT_FALSE(producer->areChangeStreamsEnabled());

    // Value validation
    EXPECT_EQ(cb::engine_errc::invalid_arguments,
              producer->control(0, DcpControlKeys::ChangeStreams, "not-true"));
    EXPECT_FALSE(producer->areChangeStreamsEnabled());

    // Enabled
    EXPECT_EQ(cb::engine_errc::success,
              producer->control(0, DcpControlKeys::ChangeStreams, "true"));
    EXPECT_TRUE(producer->areChangeStreamsEnabled());

    destroy_mock_cookie(cookie);
}

TEST_P(STDcpTest, ProducerNegotiatesCDC_NotMagma) {
    if (isMagma()) {
        GTEST_SKIP();
    }

    auto* cookie = create_mock_cookie();
    const auto producer = std::make_shared<MockDcpProducer>(
            *engine, cookie, "test_producer", cb::mcbp::DcpOpenFlag::None);
    ASSERT_FALSE(producer->areChangeStreamsEnabled());

    EXPECT_EQ(cb::engine_errc::not_supported,
              producer->control(0, DcpControlKeys::ChangeStreams, "true"));
    EXPECT_FALSE(producer->areChangeStreamsEnabled());

    destroy_mock_cookie(cookie);
}

TEST_P(STDcpConsumerTest,
       ConsumerNegotiatesDeletedUserXattrs_DisabledAtProducer) {
    testConsumerNegotiates(
            cb::mcbp::Status::UnknownCommand,
            DcpControlKeys::IncludeDeletedUserXattrs,
            "true",
            [](const MockDcpConsumer& consumer) {
                return consumer.public_getIncludeDeletedUserXattrs() ==
                       IncludeDeletedUserXattrs::No;
            });
}

TEST_P(STDcpConsumerTest,
       ConsumerNegotiatesDeletedUserXattrs_EnabledAtProducer) {
    testConsumerNegotiates(
            cb::mcbp::Status::Success,
            DcpControlKeys::IncludeDeletedUserXattrs,
            "true",
            [](const MockDcpConsumer& consumer) {
                return consumer.public_getIncludeDeletedUserXattrs() ==
                       IncludeDeletedUserXattrs::Yes;
            });
}

TEST_P(STDcpConsumerTest, ConsumerNegotiatesCDC_DisabledAtProducer) {
    testConsumerNegotiates(cb::mcbp::Status::UnknownCommand,
                           DcpControlKeys::ChangeStreams,
                           "true",
                           [](const MockDcpConsumer& consumer) {
                               return !consumer.areChangeStreamsEnabled();
                           });
}

TEST_P(STDcpConsumerTest, ConsumerNegotiatesCDC_EnabledAtProducer) {
    testConsumerNegotiates(cb::mcbp::Status::Success,
                           DcpControlKeys::ChangeStreams,
                           "true",
                           [](const MockDcpConsumer& consumer) {
                               return consumer.areChangeStreamsEnabled();
                           });
}

TEST_P(STDcpConsumerTest, ConsumerNegotiatesFlatBuffers_DisabledAtProducer) {
    testConsumerNegotiates(
            cb::mcbp::Status::UnknownCommand,
            DcpControlKeys::FlatBuffersSystemEvents,
            "true",
            [](const MockDcpConsumer& consumer) {
                return !consumer.areFlatBuffersSystemEventsEnabled();
            });
}

TEST_P(STDcpConsumerTest, ConsumerNegotiatesFlatBuffers_EnabledAtProducer) {
    testConsumerNegotiates(
            cb::mcbp::Status::Success,
            DcpControlKeys::FlatBuffersSystemEvents,
            "true",
            [](const MockDcpConsumer& consumer) {
                return consumer.areFlatBuffersSystemEventsEnabled();
            });
}

TEST_P(STDcpConsumerTest, ConsumerNegotiatesV7StatusCodes_DisabledAtProducer) {
    testConsumerNegotiates(cb::mcbp::Status::UnknownCommand,
                           DcpControlKeys::V7DcpStatusCodes,
                           "true",
                           [](const MockDcpConsumer& consumer) {
                               return !consumer.useV7StatusCodes();
                           });
}

TEST_P(STDcpConsumerTest, ConsumerNegotiatesV7StatusCodes_EnabledAtProducer) {
    testConsumerNegotiates(cb::mcbp::Status::Success,
                           DcpControlKeys::V7DcpStatusCodes,
                           "true",
                           [](const MockDcpConsumer& consumer) {
                               return consumer.useV7StatusCodes();
                           });
}

TEST_P(STDcpConsumerTest, ConsumerNegotiatesNoopMillisecondsProducerAccepts) {
    // Validate that there is no seconds negotiation pending
    for (const auto& control : *consumer->public_getPendingControls().lock()) {
        if (control.key == DcpControlKeys::SetNoopInterval &&
            control.value == "1") {
            FAIL() << "Expected millisecond negotiation";
        }
    }

    testConsumerNegotiates(
            cb::mcbp::Status::Success,
            DcpControlKeys::SetNoopInterval,
            "0.100000",
            [](const MockDcpConsumer& consumer) {
                // validate that the consumer's pendingControl
                // does not include the seconds negotiate
                for (const auto& control :
                     *consumer.public_getPendingControls().lock()) {
                    if (control.key == DcpControlKeys::SetNoopInterval &&
                        control.value == "1") {
                        return false;
                    }
                }
                return true;
            });
}

TEST_P(STDcpConsumerTest, ConsumerNegotiatesNoopMillisecondsProducerFails) {
    // Validate that there is no seconds negotiation pending
    for (const auto& control : *consumer->public_getPendingControls().lock()) {
        if (control.key == DcpControlKeys::SetNoopInterval &&
            control.value == "1") {
            FAIL() << "Expected millisecond negotiation";
        }
    }

    testConsumerNegotiates(
            cb::mcbp::Status::Einval,
            DcpControlKeys::SetNoopInterval,
            "0.100000",
            [](const MockDcpConsumer& consumer) {
                // validate that the consumer's pendingControl
                // now includes the seconds negotiate
                for (const auto& control :
                     *consumer.public_getPendingControls().lock()) {
                    if (control.key == DcpControlKeys::SetNoopInterval &&
                        control.value == "1") {
                        return true;
                    }
                }
                return false;
            });

    // Now we should get the seconds negotiated
    testConsumerNegotiates(cb::mcbp::Status::Einval,
                           DcpControlKeys::SetNoopInterval,
                           "1",
                           [](const MockDcpConsumer& consumer) {
                               return true; // @todo: what can we check?
                           });
}

TEST_P(STDcpConsumerTest, ConsumerNegotiatesCacheTransfer) {
    testConsumerNegotiates(cb::mcbp::Status::Success,
                           DcpControlKeys::CacheTransfer,
                           "true",
                           [](const MockDcpConsumer& consumer) {
                               return consumer.isCacheTransferAvailable();
                           });
}

TEST_P(STDcpConsumerTest, ConsumerNegotiatesCacheTransferProducerDisabled) {
    testConsumerNegotiates(cb::mcbp::Status::NotSupported,
                           DcpControlKeys::CacheTransfer,
                           "true",
                           [](const MockDcpConsumer& consumer) {
                               return !consumer.isCacheTransferAvailable();
                           });
}

INSTANTIATE_TEST_SUITE_P(PersistentAndEphemeral,
                         STDcpTest,
                         STParameterizedBucketTest::allConfigValues());

INSTANTIATE_TEST_SUITE_P(PersistentAndEphemeral,
                         STDcpConsumerTest,
                         STParameterizedBucketTest::allConfigValues());