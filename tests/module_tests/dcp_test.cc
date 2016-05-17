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

#include "connmap.h"
#include "dcp/producer.h"
#include "dcp/stream.h"
#include "programs/engine_testapp/mock_server.h"
#include "../mock/mock_dcp.h"

#include <platform/dirutils.h>

#include <gtest/gtest.h>

static const char test_dbname[] = "dcp_test_db";

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
                                    std::vector<queued_item> &items) {
        getOutstandingItems(vb, items);
    }

    void public_processItems(std::vector<queued_item>& items) {
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

/*
 * Mock of the DcpProducer class.  Wraps the real DcpProducer, but exposes
 * normally protected methods publically for test purposes.
 */
class MockDcpProducer: public DcpProducer {
public:
    MockDcpProducer(EventuallyPersistentEngine &theEngine, const void *cookie,
                    const std::string &name, bool isNotifier)
    : DcpProducer(theEngine, cookie, name, isNotifier)
    {}

    ENGINE_ERROR_CODE maybeSendNoop(struct dcp_message_producers* producers)
    {
        return DcpProducer::maybeSendNoop(producers);
    }

    void setNoopSendTime(const rel_time_t timeValue) {
        noopCtx.sendTime = timeValue;
    }

    rel_time_t getNoopSendTime() {
        return noopCtx.sendTime;
    }

    bool getNoopPendingRecv() {
        return noopCtx.pendingRecv;
    }

    void setNoopEnabled(const bool booleanValue) {
        noopCtx.enabled = booleanValue;
    }

    bool getNoopEnabled() {
        return noopCtx.enabled;
    }
};

/*
 * Mock of the DcpConsumer class.  Wraps the real DcpConsumer class
 * and provides get/set access to lastMessageTime.
 */
class MockDcpConsumer: public DcpConsumer {
public:
    MockDcpConsumer(EventuallyPersistentEngine &theEngine, const void *cookie,
                    const std::string &name)
    : DcpConsumer(theEngine, cookie, name)
    {}

    void setLastMessageTime(const rel_time_t timeValue) {
        lastMessageTime = timeValue;
    }

    rel_time_t getLastMessageTime() {
        return lastMessageTime;
    }
};

class DCPTest : public ::testing::Test {
protected:
    void SetUp() {
        // Paranoia - kill any existing files in case they are left over
        // from a previous run.
        CouchbaseDirectoryUtilities::rmrf(test_dbname);

        // Setup an engine with a single active vBucket.
        EXPECT_EQ(ENGINE_SUCCESS,
                  create_instance(1, get_mock_server_api, &handle))
            << "Failed to create ep engine instance";
        EXPECT_EQ(1, handle->interface) << "Unexpected engine handle version";
        engine_v1 = reinterpret_cast<ENGINE_HANDLE_V1*>(handle);

        engine = reinterpret_cast<EventuallyPersistentEngine*>(handle);
        ObjectRegistry::onSwitchThread(engine);
        std::string config = "dbname=" + std::string(test_dbname);
        EXPECT_EQ(ENGINE_SUCCESS, engine->initialize(config.c_str()))
            << "Failed to initialize engine.";

        engine->setVBucketState(vbid, vbucket_state_active, false);

        // Wait for warmup to complete.
        while (engine->getEpStore()->isWarmingUp()) {
            usleep(10);
        }

        // Set AuxIO threads to zero, so that the producer's
        // ActiveStreamCheckpointProcesserTask doesn't run.
        ExecutorPool::get()->setMaxAuxIO(0);
        // Set NonIO threads to zero, so the connManager
        // task does not run.
        ExecutorPool::get()->setMaxNonIO(0);
    }

    void TearDown() {
        // Need to force the destroy (i.e. pass true) because
        // we have disabled NonIO threads.
        engine_v1->destroy(handle, true);
        destroy_mock_event_callbacks();
        destroy_engine();
        // Cleanup any files we created.
        CouchbaseDirectoryUtilities::rmrf(test_dbname);
    }

    const uint16_t vbid = 0;

    ENGINE_HANDLE* handle;
    ENGINE_HANDLE_V1* engine_v1;
    EventuallyPersistentEngine* engine;
};

class StreamTest : public DCPTest {
protected:
    void TearDown() {
        producer->clearCheckpointProcessorTaskQueues();
        // Destroy various engine objects
        vb0.reset();
        stream.reset();
        producer.reset();
        DCPTest::TearDown();
    }

    // Setup a DCP producer and attach a stream and cursor to it.
    void setup_dcp_stream() {
        producer = new DcpProducer(*engine, /*cookie*/nullptr,
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
        EXPECT_FALSE(vb0->checkpointManager.registerCursor(
                                                           producer->getName(),
                                                           1, false,
                                                           MustSendCheckpointEnd::NO))
            << "Found an existing TAP cursor when attempting to register ours";
    }

    dcp_producer_t producer;
    stream_t stream;
    RCPtr<VBucket> vb0;
};

static void store_item(EventuallyPersistentEngine& engine,
                       const uint16_t vbid, const std::string& key,
                       const std::string& value) {
    Item item(key.c_str(), key.size(), /*flags*/0, /*exp*/0, value.c_str(),
              value.size());
    uint64_t cas;
    EXPECT_EQ(ENGINE_SUCCESS,
              engine.store(NULL, &item, &cas, OPERATION_SET, vbid));
}

/* Regression test for MB-17766 - ensure that when an ActiveStream is preparing
 * queued items to be sent out via a DCP consumer, that nextCheckpointItem()
 * doesn't incorrectly return false (meaning that there are no more checkpoint
 * items to send).
 */
TEST_F(StreamTest, test_mb17766) {

    // Add an item.
    store_item(*engine, vbid, "key", "value");

    setup_dcp_stream();

    // Should start with nextCheckpointItem() returning true.
    MockActiveStream* mock_stream = static_cast<MockActiveStream*>(stream.get());
    EXPECT_TRUE(mock_stream->public_nextCheckpointItem())
        << "nextCheckpointItem() should initially be true.";

    std::vector<queued_item> items;

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

// Check that the items remaining statistic is accurate and is unaffected
// by de-duplication.
TEST_F(StreamTest, MB17653_ItemsRemaining) {

    // Create 10 mutations to the same key which, while increasing the high
    // seqno by 10 will result in de-duplication and hence only one actual
    // mutation being added to the checkpoint items.
    const int set_op_count = 10;
    for (unsigned int ii = 0; ii < set_op_count; ii++) {
        store_item(*engine, vbid, "key", "value");
    }

    setup_dcp_stream();

    // Should start with one item remaining.
    MockActiveStream* mock_stream = static_cast<MockActiveStream*>(stream.get());

    EXPECT_EQ(1, mock_stream->getItemsRemaining())
        << "Unexpected initial stream item count";

    // Populate the streams' ready queue with items from the checkpoint,
    // advancing the streams' cursor. Should result in no change in items
    // remaining (they still haven't been send out of the stream).
    mock_stream->nextCheckpointItemTask();
    EXPECT_EQ(1, mock_stream->getItemsRemaining())
        << "Mismatch after moving items to ready queue";

    // Add another mutation. As we have already iterated over all checkpoint
    // items and put into the streams' ready queue, de-duplication of this new
    // mutation (from the point of view of the stream) isn't possible, so items
    // remaining should increase by one.
    store_item(*engine, vbid, "key", "value");
    EXPECT_EQ(2, mock_stream->getItemsRemaining())
        << "Mismatch after populating readyQ and storing 1 more item";

    // Now actually drain the items from the readyQ and see how many we received,
    // excluding meta items. This will result in all but one of the checkpoint
    // items (the one we added just above) being drained.
    std::unique_ptr<DcpResponse> response(mock_stream->public_nextQueuedItem());
    ASSERT_NE(nullptr, response);
    EXPECT_TRUE(response->isMetaEvent()) << "Expected 1st item to be meta";

    response.reset(mock_stream->public_nextQueuedItem());
    ASSERT_NE(nullptr, response);
    EXPECT_FALSE(response->isMetaEvent()) << "Expected 2nd item to be non-meta";

    response.reset(mock_stream->public_nextQueuedItem());
    EXPECT_EQ(nullptr, response) << "Expected there to not be a 3rd item.";

    EXPECT_EQ(1, mock_stream->getItemsRemaining())
        << "Expected to have 1 item remaining (in checkpoint) after draining readyQ";

    // Add another 10 mutations on a different key. This should only result in
    // us having one more item (not 10) due to de-duplication in
    // checkpoints.
    for (unsigned int ii = 0; ii < set_op_count; ii++) {
        store_item(*engine, vbid, "key_2", "value");
    }

    EXPECT_EQ(2, mock_stream->getItemsRemaining())
        << "Expected two items after adding 1 more to existing checkpoint";

    // Copy items into readyQ a second time, and drain readyQ so we should
    // have no items left.
    mock_stream->nextCheckpointItemTask();
    do {
        response.reset(mock_stream->public_nextQueuedItem());
    } while (response);
    EXPECT_EQ(0, mock_stream->getItemsRemaining())
        << "Should have 0 items remaining after advancing cursor and draining readyQ";
}

TEST_F(StreamTest, test_mb18625) {

    // Add an item.
    store_item(*engine, vbid, "key", "value");

    setup_dcp_stream();

    // Should start with nextCheckpointItem() returning true.
    MockActiveStream* mock_stream = static_cast<MockActiveStream*>(stream.get());
    EXPECT_TRUE(mock_stream->public_nextCheckpointItem())
        << "nextCheckpointItem() should initially be true.";

    std::vector<queued_item> items;

    // Get the set of outstanding items
    mock_stream->public_getOutstandingItems(vb0, items);

    // Set stream to DEAD to simulate a close stream request
    mock_stream->setDead(END_STREAM_CLOSED);

    // Process the set of items retrieved from checkpoint queues previously
    mock_stream->public_processItems(items);

    // Retrieve the next message in the stream's readyQ
    DcpResponse *op = mock_stream->public_nextQueuedItem();
    EXPECT_EQ(DCP_STREAM_END, op->getEvent())
        << "Expected the STREAM_END message";
    delete op;

    // Expect no other message to be queued after stream end message
    EXPECT_EQ(0, (mock_stream->public_readyQ()).size())
        << "Expected no more messages in the readyQ";
}

class ConnectionTest : public DCPTest {};

ENGINE_ERROR_CODE mock_noop_return_engine_e2big(const void* cookie,uint32_t opaque) {
    return ENGINE_E2BIG;
}

TEST_F(ConnectionTest, test_maybesendnoop_buffer_full) {
    const void* cookie = create_mock_cookie();
    // Create a Mock Dcp producer
    MockDcpProducer producer(*engine, cookie, "test_producer", /*notifyOnly*/false);

    struct dcp_message_producers producers = {nullptr, nullptr, nullptr, nullptr,
        nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
        mock_noop_return_engine_e2big, nullptr, nullptr};

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

    std::unique_ptr<dcp_message_producers> producers(get_dcp_producers(handle, engine_v1));
    producer.setNoopEnabled(true);
    producer.setNoopSendTime(21);
    ENGINE_ERROR_CODE ret = producer.maybeSendNoop(producers.get());
    EXPECT_EQ(ENGINE_WANT_MORE, ret)
    << "maybeSendNoop not returning ENGINE_WANT_MORE";
    EXPECT_TRUE(producer.getNoopPendingRecv())
    << "Not waiting for noop acknowledgement";
    EXPECT_NE(21, producer.getNoopSendTime())
    << "SendTime has not been updated";
    destroy_mock_cookie(cookie);
}

TEST_F(ConnectionTest, test_maybesendnoop_noop_already_pending) {
    const void* cookie = create_mock_cookie();
    // Create a Mock Dcp producer
    MockDcpProducer producer(*engine, cookie, "test_producer", /*notifyOnly*/false);

    std::unique_ptr<dcp_message_producers> producers(get_dcp_producers(handle, engine_v1));
    producer.setNoopEnabled(true);
    producer.setNoopSendTime(21);
    ENGINE_ERROR_CODE ret = producer.maybeSendNoop(producers.get());
    EXPECT_EQ(ENGINE_WANT_MORE, ret)
    << "maybeSendNoop not returning ENGINE_WANT_MORE";
    EXPECT_TRUE(producer.getNoopPendingRecv())
    << "Not awaiting noop acknowledgement";
    EXPECT_NE(21, producer.getNoopSendTime())
    << "SendTime has not been updated";
    producer.setNoopSendTime(21);
    ENGINE_ERROR_CODE ret2 = producer.maybeSendNoop(producers.get());
    EXPECT_EQ(ENGINE_DISCONNECT, ret2)
     << "maybeSendNoop not returning ENGINE_DISCONNECT";
    EXPECT_TRUE(producer.getNoopPendingRecv())
    << "Not waiting for noop acknowledgement";
    EXPECT_EQ(21, producer.getNoopSendTime())
    << "SendTime has been updated";
    destroy_mock_cookie(cookie);
}

TEST_F(ConnectionTest, test_maybesendnoop_not_enabled) {
    const void* cookie = create_mock_cookie();
    // Create a Mock Dcp producer
    MockDcpProducer producer(*engine, cookie, "test_producer", /*notifyOnly*/false);

    std::unique_ptr<dcp_message_producers> producers(get_dcp_producers(handle, engine_v1));
    producer.setNoopEnabled(false);
    producer.setNoopSendTime(21);
    ENGINE_ERROR_CODE ret = producer.maybeSendNoop(producers.get());
    EXPECT_EQ(ENGINE_FAILED, ret)
    << "maybeSendNoop not returning ENGINE_FAILED";
    EXPECT_FALSE(producer.getNoopPendingRecv())
    << "Waiting for noop acknowledgement";
    EXPECT_EQ(21, producer.getNoopSendTime())
    << "SendTime has been updated";
    destroy_mock_cookie(cookie);
}

TEST_F(ConnectionTest, test_maybesendnoop_not_sufficient_time_passed) {
    const void* cookie = create_mock_cookie();
    // Create a Mock Dcp producer
    MockDcpProducer producer(*engine, cookie, "test_producer", /*notifyOnly*/false);

    std::unique_ptr<dcp_message_producers> producers(get_dcp_producers(handle, engine_v1));
    producer.setNoopEnabled(true);
    rel_time_t current_time = ep_current_time();
    producer.setNoopSendTime(current_time);
    ENGINE_ERROR_CODE ret = producer.maybeSendNoop(producers.get());
    EXPECT_EQ(ENGINE_FAILED, ret)
    << "maybeSendNoop not returning ENGINE_FAILED";
    EXPECT_FALSE(producer.getNoopPendingRecv())
    << "Waiting for noop acknowledgement";
    EXPECT_EQ(current_time, producer.getNoopSendTime())
    << "SendTime has been incremented";
    destroy_mock_cookie(cookie);
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

TEST_F(ConnectionTest, test_mb17042_duplicate_name_producer_connections) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize(DCP_CONN_NOTIFIER);
    const void* cookie1 = create_mock_cookie();
    const void* cookie2 = create_mock_cookie();
    // Create a new Dcp producer
    dcp_producer_t producer = connMap.newProducer(cookie1, "test_producer",
                                                  /*notifyOnly*/false);
    EXPECT_NE(0, producer) << "producer is null";

    // Create a duplicate Dcp producer
    dcp_producer_t duplicateproducer = connMap.newProducer(cookie2, "test_producer",
                                                           /*notifyOnly*/false);
    EXPECT_TRUE(producer->doDisconnect()) << "producer doDisconnect == false";
    EXPECT_NE(0, duplicateproducer) << "duplicateproducer is null";

    // Disconnect the producer connection
    connMap.disconnect(cookie1);
    // Disconnect the duplicateproducer connection
    connMap.disconnect(cookie2);
    // Cleanup the deadConnections
    connMap.manageConnections();
    // Should be zero deadConnections
    EXPECT_EQ(0, connMap.getNumberOfDeadConnections())
        << "Dead connections still remain";
}

TEST_F(ConnectionTest, test_mb17042_duplicate_name_consumer_connections) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize(DCP_CONN_NOTIFIER);
    struct mock_connstruct* cookie1 = (struct mock_connstruct*)create_mock_cookie();
    struct mock_connstruct* cookie2 = (struct mock_connstruct*)create_mock_cookie();
    // Create a new Dcp consumer
    dcp_consumer_t consumer = connMap.newConsumer(cookie1, "test_consumer");
    EXPECT_NE(0, consumer) << "consumer is null";

    // Create a duplicate Dcp consumer
    dcp_consumer_t duplicateconsumer = connMap.newConsumer(cookie2, "test_consumer");
    EXPECT_TRUE(consumer->doDisconnect()) << "consumer doDisconnect == false";
    EXPECT_NE(0, duplicateconsumer) << "duplicateconsumer is null";

    // Disconnect the consumer connection
    connMap.disconnect(cookie1);
    // Disconnect the duplicateconsumer connection
    connMap.disconnect(cookie2);
    // Cleanup the deadConnections
    connMap.manageConnections();
    // Should be zero deadConnections
    EXPECT_EQ(0, connMap.getNumberOfDeadConnections())
        << "Dead connections still remain";
}

TEST_F(ConnectionTest, test_mb17042_duplicate_cookie_producer_connections) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize(DCP_CONN_NOTIFIER);
    const void* cookie = create_mock_cookie();
    // Create a new Dcp producer
    dcp_producer_t producer = connMap.newProducer(cookie, "test_producer1",
                                                   /*notifyOnly*/false);

    // Create a duplicate Dcp producer
    dcp_producer_t duplicateproducer = connMap.newProducer(cookie, "test_producer2",
                                                            /*notifyOnly*/false);
    EXPECT_TRUE(producer->doDisconnect()) << "producer doDisconnect == false";
    EXPECT_EQ(0, duplicateproducer) << "duplicateproducer is not null";

    // Disconnect the producer connection
    connMap.disconnect(cookie);
    // Cleanup the deadConnections
    connMap.manageConnections();
    // Should be zero deadConnections
    EXPECT_EQ(0, connMap.getNumberOfDeadConnections())
        << "Dead connections still remain";
}

TEST_F(ConnectionTest, test_mb17042_duplicate_cookie_consumer_connections) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize(DCP_CONN_NOTIFIER);
    const void* cookie = create_mock_cookie();
    // Create a new Dcp consumer
    dcp_consumer_t consumer = connMap.newConsumer(cookie, "test_consumer1");

    // Create a duplicate Dcp consumer
    dcp_consumer_t duplicateconsumer = connMap.newConsumer(cookie, "test_consumer2");
    EXPECT_TRUE(consumer->doDisconnect()) << "consumer doDisconnect == false";
    EXPECT_EQ(0, duplicateconsumer) << "duplicateconsumer is not null";

    // Disconnect the consumer connection
    connMap.disconnect(cookie);
    // Cleanup the deadConnections
    connMap.manageConnections();
    // Should be zero deadConnections
    EXPECT_EQ(0, connMap.getNumberOfDeadConnections())
        << "Dead connections still remain";
}

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
                       /*key*/nullptr,
                       /*nkey*/0,
                       /*value*/nullptr,
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
                       /*meta*/nullptr,
                       /*nmeta*/0);
    EXPECT_NE(1234, consumer->getLastMessageTime())
        << "lastMessagerTime not updated for mutation";
    consumer->setLastMessageTime(1234);
    consumer->deletion(/*opaque*/0,
                       /*key*/nullptr,
                       /*nkey*/0,
                       /*cas*/0,
                       /*vbucket*/0,
                       /*bySeqno*/0,
                       /*revSeqno*/0,
                       /*meta*/nullptr,
                       /*nmeta*/0);
    EXPECT_NE(1234, consumer->getLastMessageTime())
        << "lastMessagerTime not updated for deletion";
    consumer->setLastMessageTime(1234);
    consumer->expiration(/*opaque*/0,
                         /*key*/nullptr,
                         /*nkey*/0,
                         /*cas*/0,
                         /*vbucket*/0,
                         /*bySeqno*/0,
                         /*revSeqno*/0,
                         /*meta*/nullptr,
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


// Callback for dcp_add_failover_log
ENGINE_ERROR_CODE test_dcp_add_failover_log(vbucket_failover_t* entry,
                                            size_t nentries,
                                            const void *cookie) {
    return ENGINE_SUCCESS;
}

TEST_F(ConnectionTest, test_stream_request_for_dead_vbucket) {
    // Set vbucket state to dead
    engine->setVBucketState(vbid, vbucket_state_dead, false);

    MockDcpConnMap connMap(*engine);
    connMap.initialize(DCP_CONN_NOTIFIER);
    struct mock_connstruct* cookie = (struct mock_connstruct*)create_mock_cookie();

    // Create a new Dcp producer
    dcp_producer_t producer = connMap.newProducer(cookie, "test_producer",
                                                   /*notifyOnly*/false);

    uint64_t rollback_seqno;
    ENGINE_ERROR_CODE err = producer->streamRequest(/*flags*/0,
                                                    /*opaque*/0,
                                                    /*vbucket*/vbid,
                                                    /*start_seqno*/0,
                                                    /*end_seqno*/-1,
                                                    /*vb_uuid*/0xabcd,
                                                    /*snap_start*/0,
                                                    /*snap_end*/0,
                                                    /*rollback seqno*/&rollback_seqno,
                                                    test_dcp_add_failover_log);

    EXPECT_EQ(ENGINE_NOT_MY_VBUCKET, err) << "Unexpected error code";

    producer.reset();
    delete cookie;
}

class NotifyTest : public DCPTest {
protected:
    void SetUp() {
        // The test is going to replace a server API method, we must
        // be able to undo that
        sapi = *get_mock_server_api();
        DCPTest::SetUp();
    }

    void TearDown() {
        // Reset the server_api for other tests
        *get_mock_server_api() = sapi;
        DCPTest::TearDown();
    }

    SERVER_HANDLE_V1 sapi;
    std::unique_ptr<MockDcpConnMap> connMap;
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

    void notify() {
        callbacks++;
        connMap->notifyPausedConnection(producer.get(), /*schedule*/true);
    }

    int getCallbacks() {
        return callbacks;
    }


    static void dcp_test_notify_io_complete(const void *cookie,
                                            ENGINE_ERROR_CODE status) {
        auto notifyTest = reinterpret_cast<const ConnMapNotifyTest*>(cookie);
        // 3. Call notifyPausedConnection again. We're now interleaved inside
        //    of notifyAllPausedConnections, a second notification should occur.
        const_cast<ConnMapNotifyTest*>(notifyTest)->notify();
    }

    std::unique_ptr<MockDcpConnMap> connMap;
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


/* static storage for environment variable set by putenv(). */
static char allow_no_stats_env[] = "ALLOW_NO_STATS_UPDATE=yeah";

int main(int argc, char **argv) {
    (void)argc; (void)argv;

    putenv(allow_no_stats_env);
    init_mock_server(/*log to stderr*/false);

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
