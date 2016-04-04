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

    void initialize(conn_notifier_type ntype) {
        connNotifier_ = new ConnNotifier(ntype, *this);
        // We do not create a ConnNotifierCallback task
        // We do not create a ConnManager task
        // The ConnNotifier is deleted in the DcpConnMap
        // destructor
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

TEST_F(ConnectionTest, test_deadConnections) {
    MockDcpConnMap *connMap = new MockDcpConnMap(*engine);
    connMap->initialize(DCP_CONN_NOTIFIER);
    const void *cookie = create_mock_cookie();
    // Create a new Dcp producer
    dcp_producer_t producer = connMap->newProducer(cookie, "test_producer",
                                    /*notifyOnly*/false);

    // Disconnect the producer connection
    connMap->disconnect(cookie);
    EXPECT_EQ(1, (int)connMap->getNumberOfDeadConnections())
        << "Unexpected number of dead connections";
    connMap->manageConnections();
    // Should be zero deadConnections
    EXPECT_EQ(0, (int)connMap->getNumberOfDeadConnections())
        << "Dead connections still remain";

    delete connMap;
}

TEST_F(ConnectionTest, test_mb17042_duplicate_name_producer_connections) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize(DCP_CONN_NOTIFIER);
    struct mock_connstruct* cookie1 = (struct mock_connstruct*)create_mock_cookie();
    struct mock_connstruct* cookie2 = (struct mock_connstruct*)create_mock_cookie();
    // Create a new Dcp producer
    dcp_producer_t producer = connMap.newProducer(cookie1, "test_producer",
                                                  /*notifyOnly*/false);
    EXPECT_NE(0, (int)producer) << "producer is null";

    // Create a duplicate Dcp producer
    dcp_producer_t duplicateproducer = connMap.newProducer(cookie2, "test_producer",
                                                           /*notifyOnly*/false);
    EXPECT_NE(0, (int)duplicateproducer) << "duplicateproducer is null";

    producer.reset();
    duplicateproducer.reset();
    delete cookie1;
    delete cookie2;
}

TEST_F(ConnectionTest, test_mb17042_duplicate_name_consumer_connections) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize(DCP_CONN_NOTIFIER);
    struct mock_connstruct* cookie1 = (struct mock_connstruct*)create_mock_cookie();
    struct mock_connstruct* cookie2 = (struct mock_connstruct*)create_mock_cookie();
    // Create a new Dcp consumer
    dcp_consumer_t consumer = connMap.newConsumer(cookie1, "test_consumer");
    EXPECT_NE(0, (int)consumer) << "consumer is null";

    // Create a duplicate Dcp consumer
    dcp_consumer_t duplicateconsumer = connMap.newConsumer(cookie2, "test_consumer");
    EXPECT_NE(0, (int)duplicateconsumer) << "duplicateconsumer is null";

    consumer.reset();
    duplicateconsumer.reset();
    delete cookie1;
    delete cookie2;
}

TEST_F(ConnectionTest, test_mb17042_duplicate_cookie_producer_connections) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize(DCP_CONN_NOTIFIER);
    struct mock_connstruct* cookie = (struct mock_connstruct*)create_mock_cookie();
    // Create a new Dcp producer
    dcp_producer_t producer = connMap.newProducer(cookie, "test_producer1",
                                                   /*notifyOnly*/false);

    // Create a duplicate Dcp producer
    dcp_producer_t duplicateproducer = connMap.newProducer(cookie, "test_producer2",
                                                            /*notifyOnly*/false);
    EXPECT_EQ(0, (int)duplicateproducer) << "duplicateproducer is not null";

    producer.reset();
    delete cookie;
}

TEST_F(ConnectionTest, test_mb17042_duplicate_cookie_consumer_connections) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize(DCP_CONN_NOTIFIER);
    struct mock_connstruct* cookie = (struct mock_connstruct*)create_mock_cookie();
    // Create a new Dcp consumer
    dcp_consumer_t consumer = connMap.newConsumer(cookie, "test_consumer1");

    // Create a duplicate Dcp consumer
    dcp_consumer_t duplicateconsumer = connMap.newConsumer(cookie, "test_consumer2");
    EXPECT_EQ(0, (int)duplicateconsumer) << "duplicateconsumer is not null";

    consumer.reset();
    delete cookie;
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

/* static storage for environment variable set by putenv(). */
static char allow_no_stats_env[] = "ALLOW_NO_STATS_UPDATE=yeah";

int main(int argc, char **argv) {
    (void)argc; (void)argv;

    putenv(allow_no_stats_env);
    init_mock_server(/*log to stderr*/false);

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
