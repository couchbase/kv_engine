/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

/*
 * Unit test for DCP-related classes.
 *
 * Due to the way our classes are structured, most of the different DCP classes
 * need an instance of EventuallyPersistentStore & other related objects.
 */

#include "connmap.h"
#include "dcp-stream.h"
#include "dcp-response.h"
#include "evp_engine_test.h"
#include "programs/engine_testapp/mock_server.h"
#include "../mock/mock_dcp.h"
#include "../mock/mock_dcp_producer.h"
#include "../mock/mock_dcp_consumer.h"

#include <gtest/gtest.h>

// Mock of the ActiveStream class. Wraps the real ActiveStream, but exposes
// normally protected methods publically for test purposes.
class MockActiveStream : public ActiveStream {
public:
    MockActiveStream(EventuallyPersistentEngine* e, dcp_producer_t p,
                     const std::string &name, uint32_t flags, uint32_t opaque,
                     uint16_t vb, uint64_t st_seqno, uint64_t en_seqno,
                     uint64_t vb_uuid, uint64_t snap_start_seqno,
                     uint64_t snap_end_seqno)
    : ActiveStream(e, p, name, flags, opaque, vb, st_seqno, en_seqno, vb_uuid,
                   snap_start_seqno, snap_end_seqno) {}

    // Expose underlying protected ActiveStream methods as public
    void public_getOutstandingItems(RCPtr<VBucket> &vb,
                                    std::deque<queued_item> &items) {
        getOutstandingItems(vb, items);
    }

    void public_processItems(std::deque<queued_item>& items) {
        processItems(items);
    }

    bool public_nextCheckpointItem() {
        return nextCheckpointItem();
    }

    const std::queue<DcpResponse*>& public_readyQ() {
        return readyQ;
    }

    DcpResponse* public_nextQueuedItem() {
        return nextQueuedItem();
    }
};

/*
 * Mock of the DcpConnMap class.  Wraps the real DcpConnMap, but exposes
 * normally protected methods publically for test purposes.
 */
class MockDcpConnMap: public DcpConnMap {
public:
    MockDcpConnMap(EventuallyPersistentEngine &theEngine)
    : DcpConnMap(theEngine)
    {}

    size_t getNumberOfDeadConnections() {
        return deadConnections.size();
    }

    AtomicQueue<connection_t>& getPendingNotifications() {
        return pendingNotifications;
    }

    void initialize(conn_notifier_type ntype) {
        connNotifier_ = new ConnNotifier(ntype, *this);
        // We do not create a ConnNotifierCallback task
        // We do not create a ConnManager task
        // The ConnNotifier is deleted in the DcpConnMap
        // destructor
    }
};

class DCPTest : public EventuallyPersistentEngineTest {
protected:
    void SetUp() {
        EventuallyPersistentEngineTest::SetUp();

        // Set AuxIO threads to zero, so that the producer's
        // ActiveStreamCheckpointProcesserTask doesn't run.
        ExecutorPool::get()->setMaxAuxIO(0);
        // Set NonIO threads to zero, so the connManager
        // task does not run.
        ExecutorPool::get()->setMaxNonIO(0);
    }

    // Use TearDown from parent class
};

class StreamTest : public DCPTest {
protected:
    void TearDown() {
        producer->cancelCheckpointProcessorTask();
        // Destroy various engine objects
        vb0.reset();
        stream.reset();
        producer.reset();
        DCPTest::TearDown();
    }

    // Setup a DCP producer and attach a stream and cursor to it.
    void setup_dcp_stream() {
        producer = new DcpProducer(*engine, /*cookie*/NULL,
                                   "test_producer", /*notifyOnly*/false);
        stream = new MockActiveStream(engine, producer,
                                      producer->getName(), /*flags*/0,
                                      /*opaque*/0, vbid,
                                      /*st_seqno*/0,
                                      /*en_seqno*/~0,
                                      /*vb_uuid*/0xabcd,
                                      /*snap_start_seqno*/0,
                                      /*snap_end_seqno*/~0);
        vb0 = engine->getVBucket(0);
        EXPECT_TRUE(vb0) << "Failed to get valid VBucket object for id 0";
        EXPECT_FALSE(vb0->checkpointManager.registerTAPCursor(
                producer->getName()))
            << "Found an existing TAP cursor when attempting to register ours";
    }

    dcp_producer_t producer;
    stream_t stream;
    RCPtr<VBucket> vb0;
};

/* Regression test for MB-17766 - ensure that when an ActiveStream is preparing
 * queued items to be sent out via a DCP consumer, that nextCheckpointItem()
 * doesn't incorrectly return false (meaning that there are no more checkpoint
 * items to send).
 */
TEST_F(StreamTest, test_mb17766) {

    // Add an item.
    store_item(vbid, "key", "value");

    setup_dcp_stream();

    // Should start with nextCheckpointItem() returning true.
    MockActiveStream* mock_stream = static_cast<MockActiveStream*>(stream.get());
    EXPECT_TRUE(mock_stream->public_nextCheckpointItem())
        << "nextCheckpointItem() should initially be true.";

    std::deque<queued_item> items;

    // Get the set of outstanding items
    mock_stream->public_getOutstandingItems(vb0, items);

    // REGRESSION CHECK: nextCheckpointItem() should still return true
    EXPECT_TRUE(mock_stream->public_nextCheckpointItem())
        << "nextCheckpointItem() after getting outstanding items should be true.";

    // Process the set of items
    mock_stream->public_processItems(items);

    // Should finish with nextCheckpointItem() returning false.
    EXPECT_FALSE(mock_stream->public_nextCheckpointItem())
        << "nextCheckpointItem() after processing items should be false.";
}

//
// MB17653 test removed in 3.0.x backport, as MB not fixed in 3.0.x branch.
//

//
// test_mb18625 test removed in 3.0.x backport, as MB not fixed in 3.0.x branch.
//

class ConnectionTest : public DCPTest {};

ENGINE_ERROR_CODE mock_noop_return_engine_e2big(const void* cookie,uint32_t opaque) {
    return ENGINE_E2BIG;
}

TEST_F(ConnectionTest, test_maybesendnoop_buffer_full) {
    const void* cookie = create_mock_cookie();
    // Create a Mock Dcp producer
    MockDcpProducer producer(*engine, cookie, "test_producer", /*notifyOnly*/false);

    struct dcp_message_producers producers = {NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        mock_noop_return_engine_e2big, NULL, NULL};

    producer.setNoopEnabled(true);
    producer.setNoopSendTime(21);
    ENGINE_ERROR_CODE ret = producer.maybeSendNoop(&producers);
    EXPECT_EQ(ENGINE_E2BIG, ret)
    << "maybeSendNoop not returning ENGINE_E2BIG";
    EXPECT_FALSE(producer.getNoopPendingRecv())
    << "Waiting for noop acknowledgement";
    EXPECT_EQ(21, producer.getNoopSendTime())
    << "SendTime has been updated";
    destroy_mock_cookie(cookie);
}

TEST_F(ConnectionTest, test_maybesendnoop_send_noop) {
    const void* cookie = create_mock_cookie();
    // Create a Mock Dcp producer
    MockDcpProducer producer(*engine, cookie, "test_producer", /*notifyOnly*/false);

    dcp_message_producers* producers = get_dcp_producers();
    producer.setNoopEnabled(true);
    producer.setNoopSendTime(21);
    ENGINE_ERROR_CODE ret = producer.maybeSendNoop(producers);
    EXPECT_EQ(ENGINE_WANT_MORE, ret)
    << "maybeSendNoop not returning ENGINE_WANT_MORE";
    EXPECT_TRUE(producer.getNoopPendingRecv())
    << "Not waiting for noop acknowledgement";
    EXPECT_NE(21, producer.getNoopSendTime())
    << "SendTime has not been updated";
    destroy_mock_cookie(cookie);

    free(producers);
}

TEST_F(ConnectionTest, test_maybesendnoop_noop_already_pending) {
    const void* cookie = create_mock_cookie();
    // Create a Mock Dcp producer
    MockDcpProducer producer(*engine, cookie, "test_producer", /*notifyOnly*/false);

    dcp_message_producers* producers = get_dcp_producers();
    producer.setNoopEnabled(true);
    producer.setNoopSendTime(21);
    ENGINE_ERROR_CODE ret = producer.maybeSendNoop(producers);
    EXPECT_EQ(ENGINE_WANT_MORE, ret)
    << "maybeSendNoop not returning ENGINE_WANT_MORE";
    EXPECT_TRUE(producer.getNoopPendingRecv())
    << "Not awaiting noop acknowledgement";
    EXPECT_NE(21, producer.getNoopSendTime())
    << "SendTime has not been updated";
    producer.setNoopSendTime(21);
    ENGINE_ERROR_CODE ret2 = producer.maybeSendNoop(producers);
    EXPECT_EQ(ENGINE_DISCONNECT, ret2)
     << "maybeSendNoop not returning ENGINE_DISCONNECT";
    EXPECT_TRUE(producer.getNoopPendingRecv())
    << "Not waiting for noop acknowledgement";
    EXPECT_EQ(21, producer.getNoopSendTime())
    << "SendTime has been updated";
    destroy_mock_cookie(cookie);

    free(producers);
}

TEST_F(ConnectionTest, test_maybesendnoop_not_enabled) {
    const void* cookie = create_mock_cookie();
    // Create a Mock Dcp producer
    MockDcpProducer producer(*engine, cookie, "test_producer", /*notifyOnly*/false);

    dcp_message_producers* producers = get_dcp_producers();
    producer.setNoopEnabled(false);
    producer.setNoopSendTime(21);
    ENGINE_ERROR_CODE ret = producer.maybeSendNoop(producers);
    EXPECT_EQ(ENGINE_FAILED, ret)
    << "maybeSendNoop not returning ENGINE_FAILED";
    EXPECT_FALSE(producer.getNoopPendingRecv())
    << "Waiting for noop acknowledgement";
    EXPECT_EQ(21, producer.getNoopSendTime())
    << "SendTime has been updated";
    destroy_mock_cookie(cookie);

    free(producers);
}

TEST_F(ConnectionTest, test_maybesendnoop_not_sufficient_time_passed) {
    const void* cookie = create_mock_cookie();
    // Create a Mock Dcp producer
    MockDcpProducer producer(*engine, cookie, "test_producer", /*notifyOnly*/false);

    dcp_message_producers* producers = get_dcp_producers();
    producer.setNoopEnabled(true);
    rel_time_t current_time = ep_current_time();
    producer.setNoopSendTime(current_time);
    ENGINE_ERROR_CODE ret = producer.maybeSendNoop(producers);
    EXPECT_EQ(ENGINE_FAILED, ret)
    << "maybeSendNoop not returning ENGINE_FAILED";
    EXPECT_FALSE(producer.getNoopPendingRecv())
    << "Waiting for noop acknowledgement";
    EXPECT_EQ(current_time, producer.getNoopSendTime())
    << "SendTime has been incremented";
    destroy_mock_cookie(cookie);

    free(producers);
}

TEST_F(ConnectionTest, test_deadConnections) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize(DCP_CONN_NOTIFIER);
    const void *cookie = create_mock_cookie();
    // Create a new Dcp producer
    dcp_producer_t producer = connMap.newProducer(cookie, "test_producer",
                                    /*notifyOnly*/false);

    // Disconnect the producer connection
    connMap.disconnect(cookie);
    EXPECT_EQ(1, connMap.getNumberOfDeadConnections())
        << "Unexpected number of dead connections";
    connMap.manageConnections();
    // Should be zero deadConnections
    EXPECT_EQ(0, connMap.getNumberOfDeadConnections())
        << "Dead connections still remain";
}

//
// test_mb17042* removed in 3.0.x backport, as MB not fixed in 3.0.x
//

TEST_F(ConnectionTest, test_update_of_last_message_time_in_consumer) {
    const void* cookie = create_mock_cookie();
    // Create a Mock Dcp consumer
    MockDcpConsumer *consumer = new MockDcpConsumer(*engine, cookie, "test_consumer");
    consumer->setLastMessageTime(1234);
    consumer->addStream(/*opaque*/0, /*vbucket*/0, /*flags*/0);
    EXPECT_NE(1234, consumer->getLastMessageTime())
        << "lastMessagerTime not updated for addStream";
    consumer->setLastMessageTime(1234);
    consumer->closeStream(/*opaque*/0, /*vbucket*/0);
    EXPECT_NE(1234, consumer->getLastMessageTime())
        << "lastMessagerTime not updated for closeStream";
    consumer->setLastMessageTime(1234);
    consumer->streamEnd(/*opaque*/0, /*vbucket*/0, /*flags*/0);
    EXPECT_NE(1234, consumer->getLastMessageTime())
        << "lastMessagerTime not updated for streamEnd";
    consumer->mutation(/*opaque*/0,
                       /*key*/NULL,
                       /*nkey*/0,
                       /*value*/NULL,
                       /*nvalue*/0,
                       /*cas*/0,
                       /*vbucket*/0,
                       /*flags*/0,
                       /*datatype*/0,
                       /*locktime*/0,
                       /*bySeqno*/0,
                       /*revSeqno*/0,
                       /*exprtime*/0,
                       /*nru*/0,
                       /*meta*/NULL,
                       /*nmeta*/0);
    EXPECT_NE(1234, consumer->getLastMessageTime())
        << "lastMessagerTime not updated for mutation";
    consumer->setLastMessageTime(1234);
    consumer->deletion(/*opaque*/0,
                       /*key*/NULL,
                       /*nkey*/0,
                       /*cas*/0,
                       /*vbucket*/0,
                       /*bySeqno*/0,
                       /*revSeqno*/0,
                       /*meta*/NULL,
                       /*nmeta*/0);
    EXPECT_NE(1234, consumer->getLastMessageTime())
        << "lastMessagerTime not updated for deletion";
    consumer->setLastMessageTime(1234);
    consumer->expiration(/*opaque*/0,
                         /*key*/NULL,
                         /*nkey*/0,
                         /*cas*/0,
                         /*vbucket*/0,
                         /*bySeqno*/0,
                         /*revSeqno*/0,
                         /*meta*/NULL,
                         /*nmeta*/0);
    EXPECT_NE(1234, consumer->getLastMessageTime())
        << "lastMessagerTime not updated for expiration";
    consumer->setLastMessageTime(1234);
    consumer->snapshotMarker(/*opaque*/0,
                             /*vbucket*/0,
                             /*start_seqno*/0,
                             /*end_seqno*/0,
                             /*flags*/0);
    EXPECT_NE(1234, consumer->getLastMessageTime())
        << "lastMessagerTime not updated for snapshotMarker";
    consumer->setLastMessageTime(1234);
    consumer->noop(/*opaque*/0);
    EXPECT_NE(1234, consumer->getLastMessageTime())
        << "lastMessagerTime not updated for noop";
    consumer->setLastMessageTime(1234);
    consumer->flush(/*opaque*/0, /*vbucket*/0);
    EXPECT_NE(1234, consumer->getLastMessageTime())
        << "lastMessagerTime not updated for flush";
    consumer->setLastMessageTime(1234);
    consumer->setVBucketState(/*opaque*/0,
                              /*vbucket*/0,
                              /*state*/vbucket_state_active);
    EXPECT_NE(1234, consumer->getLastMessageTime())
        << "lastMessagerTime not updated for setVBucketState";
    destroy_mock_cookie(cookie);
}

class NotifyTest : public DCPTest {
protected:
    void SetUp() {
        // The test is going to replace a server API method, we must
        // be able to undo that
        sapi = *get_mock_server_api();
        scookie_api = *get_mock_server_api()->cookie;
        DCPTest::SetUp();
    }

    void TearDown() {
        // Reset the server_api for other tests
        *get_mock_server_api() = sapi;
        *get_mock_server_api()->cookie = scookie_api;
        DCPTest::TearDown();
    }

    SERVER_HANDLE_V1 sapi;
    SERVER_COOKIE_API scookie_api;
    dcp_producer_t producer;
    int callbacks;
};

class ConnMapNotifyTest {
public:
    ConnMapNotifyTest(EventuallyPersistentEngine& engine)
        : connMap(new MockDcpConnMap(engine)),
          callbacks(0) {
        connMap->initialize(DCP_CONN_NOTIFIER);

        // Use 'this' instead of a mock cookie
        producer = connMap->newProducer(static_cast<void*>(this),
                                        "test_producer",
                                        /*notifyOnly*/false);
    }

    ~ConnMapNotifyTest() {
        delete connMap;
    }

    void notify() {
        callbacks++;
        connMap->notifyPausedConnection(producer.get(), /*schedule*/true);
    }

    int getCallbacks() {
        return callbacks;
    }


    static void dcp_test_notify_io_complete(const void *cookie,
                                            ENGINE_ERROR_CODE status) {
        const ConnMapNotifyTest* notifyTest = reinterpret_cast<const ConnMapNotifyTest*>(cookie);
        // 3. Call notifyPausedConnection again. We're now interleaved inside
        //    of notifyAllPausedConnections, a second notification should occur.
        const_cast<ConnMapNotifyTest*>(notifyTest)->notify();
    }

    MockDcpConnMap* connMap;
    dcp_producer_t producer;

private:
    int callbacks;

};


TEST_F(NotifyTest, test_mb19503_connmap_notify) {
    ConnMapNotifyTest notifyTest(*engine);

    // Hook into notify_io_complete
    SERVER_COOKIE_API* scapi = get_mock_server_api()->cookie;
    scapi->notify_io_complete = ConnMapNotifyTest::dcp_test_notify_io_complete;

    // Should be 0 when we begin
    ASSERT_EQ(0, notifyTest.getCallbacks());
    ASSERT_TRUE(notifyTest.producer->isPaused());
    ASSERT_EQ(0, notifyTest.connMap->getPendingNotifications().size());

    // 1. Call notifyPausedConnection with schedule = true
    //    this will queue the producer
    notifyTest.connMap->notifyPausedConnection(notifyTest.producer.get(),
                                               /*schedule*/true);
    EXPECT_EQ(1, notifyTest.connMap->getPendingNotifications().size());

    // 2. Call notifyAllPausedConnections this will invoke notifyIOComplete
    //    which we've hooked into. For step 3 go to dcp_test_notify_io_complete
    notifyTest.connMap->notifyAllPausedConnections();

    // 2.1 One callback should of occurred, and we should still have one
    //     notification pending (see dcp_test_notify_io_complete).
    EXPECT_EQ(1, notifyTest.getCallbacks());
    EXPECT_EQ(1, notifyTest.connMap->getPendingNotifications().size());

    // 4. Call notifyAllPausedConnections again, is there a new connection?
    notifyTest.connMap->notifyAllPausedConnections();

    // 5. There should of been 2 callbacks
    EXPECT_EQ(2, notifyTest.getCallbacks());
}

// Variation on test_mb19503_connmap_notify - check that notification is correct
// when notifiable is not paused.
TEST_F(NotifyTest, test_mb19503_connmap_notify_paused) {
    ConnMapNotifyTest notifyTest(*engine);

    // Hook into notify_io_complete
    SERVER_COOKIE_API* scapi = get_mock_server_api()->cookie;
    scapi->notify_io_complete = ConnMapNotifyTest::dcp_test_notify_io_complete;

    // Should be 0 when we begin
    ASSERT_EQ(notifyTest.getCallbacks(), 0);
    ASSERT_TRUE(notifyTest.producer->isPaused());
    ASSERT_EQ(0, notifyTest.connMap->getPendingNotifications().size());

    // 1. Call notifyPausedConnection with schedule = true
    //    this will queue the producer
    notifyTest.connMap->notifyPausedConnection(notifyTest.producer.get(),
                                               /*schedule*/true);
    EXPECT_EQ(1, notifyTest.connMap->getPendingNotifications().size());

    // 2. Mark connection as not paused.
    notifyTest.producer->setPaused(false);

    // 3. Call notifyAllPausedConnections - as the connection is not paused
    // this should *not* invoke notifyIOComplete.
    notifyTest.connMap->notifyAllPausedConnections();

    // 3.1 Should have not had any callbacks.
    EXPECT_EQ(0, notifyTest.getCallbacks());
    // 3.2 Should have no pending notifications.
    EXPECT_EQ(0, notifyTest.connMap->getPendingNotifications().size());

    // 4. Now mark the connection as paused.
    ASSERT_FALSE(notifyTest.producer->isPaused());
    notifyTest.producer->setPaused(true);

    // 4. Add another notification - should queue the producer again.
    notifyTest.connMap->notifyPausedConnection(notifyTest.producer.get(),
                                               /*schedule*/true);
    EXPECT_EQ(1, notifyTest.connMap->getPendingNotifications().size());

    // 5. Call notifyAllPausedConnections a second time - as connection is
    //    paused this time we *should* get a callback.
    notifyTest.connMap->notifyAllPausedConnections();
    EXPECT_EQ(1, notifyTest.getCallbacks());
}
