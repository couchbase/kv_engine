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
 * need an instance of EPBucket& other related objects.
 */

#include "../mock/mock_dcp.h"
#include "../mock/mock_dcp_conn_map.h"
#include "../mock/mock_dcp_consumer.h"
#include "../mock/mock_dcp_producer.h"
#include "../mock/mock_stream.h"
#include "connmap.h"
#include "dcp/backfill_disk.h"
#include "dcp/dcp-types.h"
#include "dcp/dcpconnmap.h"
#include "dcp/producer.h"
#include "dcp/stream.h"
#include "ep_time.h"
#include "evp_engine_test.h"
#include "test_helpers.h"

#include <dcp/backfill_memory.h>
#include <gtest/gtest.h>
#include <xattr/utils.h>

class DCPTest : public EventuallyPersistentEngineTest {
protected:
    void SetUp() override {
        EventuallyPersistentEngineTest::SetUp();

        // Set AuxIO threads to zero, so that the producer's
        // ActiveStreamCheckpointProcesserTask doesn't run.
        ExecutorPool::get()->setNumAuxIO(0);
        // Set NonIO threads to zero, so the connManager
        // task does not run.
        ExecutorPool::get()->setNumNonIO(0);
    }

    void TearDown() override {
        /* MB-22041 changes to dynamically stopping threads rather than having
         * the excess looping but not getting work. We now need to set the
         * AuxIO and NonIO back to 1 to allow dead tasks to be cleaned up
         */
        ExecutorPool::get()->setNumAuxIO(1);
        ExecutorPool::get()->setNumNonIO(1);

        EventuallyPersistentEngineTest::TearDown();
    }
};

class StreamTest : public DCPTest,
                   public ::testing::WithParamInterface<std::string> {
protected:
    void SetUp() override {
        bucketType = GetParam();
        DCPTest::SetUp();
        vb0 = engine->getVBucket(0);
        EXPECT_TRUE(vb0) << "Failed to get valid VBucket object for id 0";
    }

    void TearDown() override {
        if (producer) {
            producer->clearCheckpointProcessorTaskQueues();
        }
        // Destroy various engine objects
        vb0.reset();
        stream.reset();
        producer.reset();
        DCPTest::TearDown();
    }

    // Setup a DCP producer and attach a stream and cursor to it.
    void setup_dcp_stream(int flags = 0,
                          IncludeValue includeVal = IncludeValue::Yes,
                          IncludeXattrs includeXattrs = IncludeXattrs::Yes) {
        if (includeVal == IncludeValue::No) {
            flags |= DCP_OPEN_NO_VALUE;
        }
        if (includeXattrs == IncludeXattrs::Yes) {
            flags |= DCP_OPEN_INCLUDE_XATTRS;
        }
        producer = new MockDcpProducer(*engine,
                                       /*cookie*/ nullptr,
                                       "test_producer",
                                       flags,
                                       {/*no json*/},
                                       /*startTask*/ true);

        if (includeXattrs == IncludeXattrs::Yes) {
            producer->setNoopEnabled(true);
        }

        vb0 = engine->getVBucket(vbid);
        ASSERT_NE(nullptr, vb0.get());
        EXPECT_TRUE(vb0) << "Failed to get valid VBucket object for id 0";
        stream = new MockActiveStream(engine,
                                      producer,
                                      flags,
                                      /*opaque*/ 0,
                                      *vb0,
                                      /*st_seqno*/ 0,
                                      /*en_seqno*/ ~0,
                                      /*vb_uuid*/ 0xabcd,
                                      /*snap_start_seqno*/ 0,
                                      /*snap_end_seqno*/ ~0,
                                      includeVal,
                                      includeXattrs);

        EXPECT_FALSE(vb0->checkpointManager.registerCursor(
                                                           producer->getName(),
                                                           1, false,
                                                           MustSendCheckpointEnd::NO))
            << "Found an existing TAP cursor when attempting to register ours";
    }

    void destroy_dcp_stream() {
        producer.get()->closeStream(/*opaque*/ 0, vb0->getId());
    }

    /*
     * Creates an item with the key \"key\", containing json data and xattrs.
     * @return a unique_ptr to a newly created item.
     */
    std::unique_ptr<Item> makeItemWithXattrs() {
        std::string valueData = R"({"json":"yes"})";
        std::string data = createXattrValue(valueData);
        uint8_t ext_meta[EXT_META_LEN] = {PROTOCOL_BINARY_DATATYPE_JSON |
                                          PROTOCOL_BINARY_DATATYPE_XATTR};
        return std::make_unique<Item>(makeStoredDocKey("key"),
                                      /*flags*/0,
                                      /*exp*/0,
                                      data.c_str(),
                                      data.size(),
                                      ext_meta, sizeof(ext_meta));
    }

    /*
     * Creates an item with the key \"key\", containing json data and no xattrs.
     * @return a unique_ptr to a newly created item.
     */
    std::unique_ptr<Item> makeItemWithoutXattrs() {
            std::string valueData = R"({"json":"yes"})";
            uint8_t ext_meta[EXT_META_LEN] = {PROTOCOL_BINARY_DATATYPE_JSON};
            return std::make_unique<Item>(makeStoredDocKey("key"),
                                          /*flags*/0,
                                          /*exp*/0,
                                          valueData.c_str(),
                                          valueData.size(),
                                          ext_meta,
                                          sizeof(ext_meta));
    }

    /* Add items onto the vbucket and wait for the checkpoint to be removed */
    void addItemsAndRemoveCheckpoint(int numItems) {
        for (int i = 0; i < numItems; ++i) {
            std::string key("key" + std::to_string(i));
            store_item(vbid, key, "value");
        }

        /* Create new checkpoint so that we can remove the current checkpoint
         and force a backfill in the DCP stream */
        auto& ckpt_mgr = vb0->checkpointManager;
        ckpt_mgr.createNewCheckpoint();

        /* Wait for removal of the old checkpoint, this also would imply that
         the
         items are persisted (in case of persistent buckets) */
        {
            bool new_ckpt_created;
            std::chrono::microseconds uSleepTime(128);
            while (static_cast<size_t>(numItems) !=
                   ckpt_mgr.removeClosedUnrefCheckpoints(*vb0,
                                                         new_ckpt_created)) {
                uSleepTime = decayingSleep(uSleepTime);
            }
        }
    }

    /*
     * Fake callback emulating dcp_add_failover_log
     */
    static ENGINE_ERROR_CODE fakeDcpAddFailoverLog(vbucket_failover_t* entry,
                                                   size_t nentries,
                                                   const void *cookie) {
        return ENGINE_SUCCESS;
    }

    mock_dcp_producer_t producer;
    stream_t stream;
    VBucketPtr vb0;
};

/*
 * Test that when have a producer with IncludeValue and IncludeXattrs both set
 * to No an active stream created via a streamRequest returns true for
 * isKeyOnly.
 */
TEST_P(StreamTest, test_streamIsKeyOnlyTrue) {
    setup_dcp_stream(0, IncludeValue::No, IncludeXattrs::No);
    uint64_t rollbackSeqno;
    auto err = producer->streamRequest(/*flags*/0,
                                       /*opaque*/0,
                                       /*vbucket*/0,
                                       /*start_seqno*/0,
                                       /*end_seqno*/0,
                                       /*vb_uuid*/0,
                                       /*snap_start*/0,
                                       /*snap_end*/0,
                                       &rollbackSeqno,
                                       StreamTest::fakeDcpAddFailoverLog);
    ASSERT_EQ(ENGINE_SUCCESS, err)
        << "stream request did not return ENGINE_SUCCESS";

    stream = dynamic_cast<MockDcpProducer*>(producer.get())->findStream(0);
    EXPECT_TRUE(dynamic_cast<ActiveStream*>(stream.get())->isKeyOnly());
    destroy_dcp_stream();
}

/*
 * Test that when have a producer with IncludeValue set to Yes and IncludeXattrs
 * set to No an active stream created via a streamRequest returns false for
 * isKeyOnly.
 */
TEST_P(StreamTest, test_streamIsKeyOnlyFalseBecauseOfIncludeValue) {
    setup_dcp_stream(0, IncludeValue::Yes, IncludeXattrs::No);
    uint64_t rollbackSeqno;
    auto err = producer->streamRequest(/*flags*/0,
                                       /*opaque*/0,
                                       /*vbucket*/0,
                                       /*start_seqno*/0,
                                       /*end_seqno*/0,
                                       /*vb_uuid*/0,
                                       /*snap_start*/0,
                                       /*snap_end*/0,
                                       &rollbackSeqno,
                                       StreamTest::fakeDcpAddFailoverLog);
    ASSERT_EQ(ENGINE_SUCCESS, err)
        << "stream request did not return ENGINE_SUCCESS";

    stream = dynamic_cast<MockDcpProducer*>(producer.get())->findStream(0);
    EXPECT_FALSE(dynamic_cast<ActiveStream*>(stream.get())->isKeyOnly());
    destroy_dcp_stream();
}

/*
 * Test that when have a producer with IncludeValue set to No and IncludeXattrs
 * set to Yes an active stream created via a streamRequest returns false for
 * isKeyOnly.
 */
TEST_P(StreamTest, test_streamIsKeyOnlyFalseBecauseOfIncludeXattrs) {
    setup_dcp_stream(0, IncludeValue::No, IncludeXattrs::Yes);
    uint64_t rollbackSeqno;
    auto err = producer->streamRequest(/*flags*/0,
                                       /*opaque*/0,
                                       /*vbucket*/0,
                                       /*start_seqno*/0,
                                       /*end_seqno*/0,
                                       /*vb_uuid*/0,
                                       /*snap_start*/0,
                                       /*snap_end*/0,
                                       &rollbackSeqno,
                                       StreamTest::fakeDcpAddFailoverLog);
    ASSERT_EQ(ENGINE_SUCCESS, err)
        << "stream request did not return ENGINE_SUCCESS";

    stream = dynamic_cast<MockDcpProducer*>(producer.get())->findStream(0);
    EXPECT_FALSE(dynamic_cast<ActiveStream*>(stream.get())->isKeyOnly());
    destroy_dcp_stream();
}

/*
 * Test for a dcpResponse retrieved from a stream where IncludeValue and
 * IncludeXattrs are both No, that the message size does not include the size of
 * the body.
 */
TEST_P(StreamTest, test_keyOnlyMessageSize) {
    auto item = makeItemWithXattrs();
    auto keyOnlyMessageSize = MutationResponse::mutationBaseMsgBytes +
            item->getKey().size();
    queued_item qi(std::move(item));

    setup_dcp_stream(0, IncludeValue::No, IncludeXattrs::No);
    std::unique_ptr<DcpResponse> dcpResponse =
            dynamic_cast<MockActiveStream*>(
                    stream.get())->public_makeResponseFromItem(qi);

    EXPECT_EQ(keyOnlyMessageSize, dcpResponse->getMessageSize());
    destroy_dcp_stream();
}

/*
 * Test for a dcpResponse retrieved from a stream where IncludeValue and
 * IncludeXattrs are both Yes, that the message size includes the size of the
 * body.
 */
TEST_P(StreamTest, test_keyValueAndXattrsMessageSize) {
    auto item = makeItemWithXattrs();
    auto keyAndValueMessageSize = MutationResponse::mutationBaseMsgBytes +
            item->getKey().size() + item->getNBytes();
    queued_item qi(std::move(item));

    setup_dcp_stream(0, IncludeValue::Yes, IncludeXattrs::Yes);
    std::unique_ptr<DcpResponse> dcpResponse =
            dynamic_cast<MockActiveStream*>(
                    stream.get())->public_makeResponseFromItem(qi);

    EXPECT_EQ(keyAndValueMessageSize, dcpResponse->getMessageSize());
    destroy_dcp_stream();
}

/*
 * Test for a dcpResponse retrieved from a stream where IncludeValue and
 * IncludeXattrs are both Yes, however the document does not have any xattrs
 * and so the message size should equal the size of the value.
 */
TEST_P(StreamTest, test_keyAndValueMessageSize) {
    auto item = makeItemWithoutXattrs();
    auto keyAndValueMessageSize = MutationResponse::mutationBaseMsgBytes +
            item->getKey().size() + item->getNBytes();
    queued_item qi(std::move(item));

    setup_dcp_stream(0, IncludeValue::Yes, IncludeXattrs::Yes);
    std::unique_ptr<DcpResponse> dcpResponse =
            dynamic_cast<MockActiveStream*>(
                    stream.get())->public_makeResponseFromItem(qi);

    EXPECT_EQ(keyAndValueMessageSize, dcpResponse->getMessageSize());
    destroy_dcp_stream();
}

/*
 * Test for a dcpResponse retrieved from a stream where IncludeValue is Yes and
 * IncludeXattrs is No, that the message size includes the size of only the
 * value (excluding the xattrs).
 */
TEST_P(StreamTest, test_keyAndValueExcludingXattrsMessageSize) {
    auto item = makeItemWithXattrs();
    auto root = const_cast<char*>(item->getData());
    cb::byte_buffer buffer{(uint8_t*)root, item->getValue()->vlength()};
    auto sz = cb::xattr::get_body_offset({
           reinterpret_cast<char*>(buffer.buf), buffer.len});
    auto keyAndValueMessageSize = MutationResponse::mutationBaseMsgBytes +
            item->getKey().size() + item->getNBytes() - sz;
    queued_item qi(std::move(item));

    setup_dcp_stream(0, IncludeValue::Yes, IncludeXattrs::No);
    std::unique_ptr<DcpResponse> dcpResponse =
            dynamic_cast<MockActiveStream*>(
                    stream.get())->public_makeResponseFromItem(qi);

    EXPECT_EQ(keyAndValueMessageSize, dcpResponse->getMessageSize());
    destroy_dcp_stream();
}

/*
 * Test for a dcpResponse retrieved from a stream where IncludeValue is Yes and
 * IncludeXattrs are No, and the document does not have any xattrs.  So again
 * the message size should equal the size of the value.
 */
TEST_P(StreamTest,
       test_keyAndValueExcludingXattrsAndNotContainXattrMessageSize) {
    auto item = makeItemWithoutXattrs();
    auto keyAndValueMessageSize = MutationResponse::mutationBaseMsgBytes +
            item->getKey().size() + item->getNBytes();
    queued_item qi(std::move(item));

    setup_dcp_stream(0, IncludeValue::Yes, IncludeXattrs::No);
    std::unique_ptr<DcpResponse> dcpResponse =
            dynamic_cast<MockActiveStream*>(
                    stream.get())->public_makeResponseFromItem(qi);

    EXPECT_EQ(keyAndValueMessageSize, dcpResponse->getMessageSize());
    destroy_dcp_stream();
}

/*
 * Test for a dcpResponse retrieved from a stream where IncludeValue is No and
 * IncludeXattrs is Yes, that the message size includes the size of only the
 * xattrs (excluding the value).
 */
TEST_P(StreamTest, test_keyAndValueExcludingValueDataMessageSize) {
    auto item = makeItemWithXattrs();
    auto root = const_cast<char*>(item->getData());
    cb::byte_buffer buffer{(uint8_t*)root, item->getValue()->vlength()};
    auto sz = cb::xattr::get_body_offset({
           reinterpret_cast<char*>(buffer.buf), buffer.len});
    auto keyAndValueMessageSize = MutationResponse::mutationBaseMsgBytes +
            item->getKey().size() + sz;
    queued_item qi(std::move(item));

    setup_dcp_stream(0, IncludeValue::No, IncludeXattrs::Yes);
    std::unique_ptr<DcpResponse> dcpResponse =
            dynamic_cast<MockActiveStream*>(
                    stream.get())->public_makeResponseFromItem(qi);

    EXPECT_EQ(keyAndValueMessageSize, dcpResponse->getMessageSize());
    destroy_dcp_stream();
}

/* MB-24159 - Test to confirm a dcp stream backfill from an ephemeral bucket
 * over a range which includes /no/ items doesn't cause the producer to
 * segfault.
 */

TEST_P(StreamTest, backfillGetsNoItems) {
    if (engine->getConfiguration().getBucketType() == "ephemeral") {
        setup_dcp_stream(0, IncludeValue::No, IncludeXattrs::No);
        store_item(vbid, "key", "value1");
        store_item(vbid, "key", "value2");

        active_stream_t aStream = dynamic_cast<ActiveStream *>(stream.get());

        auto evb = std::shared_ptr<EphemeralVBucket>(
                std::dynamic_pointer_cast<EphemeralVBucket>(vb0));
        auto dcpbfm = DCPBackfillMemory(evb, aStream, 1, 1);
        dcpbfm.run();
        destroy_dcp_stream();
    }
}

/* Regression test for MB-17766 - ensure that when an ActiveStream is preparing
 * queued items to be sent out via a DCP consumer, that nextCheckpointItem()
 * doesn't incorrectly return false (meaning that there are no more checkpoint
 * items to send).
 */
TEST_P(StreamTest, test_mb17766) {
    // Add an item.
    store_item(vbid, "key", "value");

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
    destroy_dcp_stream();
}

// Check that the items remaining statistic is accurate and is unaffected
// by de-duplication.
TEST_P(StreamTest, MB17653_ItemsRemaining) {
    auto& manager = engine->getKVBucket()->getVBucket(vbid)->checkpointManager;

    ASSERT_EQ(0, manager.getNumOpenChkItems());

    // Create 10 mutations to the same key which, while increasing the high
    // seqno by 10 will result in de-duplication and hence only one actual
    // mutation being added to the checkpoint items.
    const int set_op_count = 10;
    for (unsigned int ii = 0; ii < set_op_count; ii++) {
        store_item(vbid, "key", "value");
    }

    ASSERT_EQ(1, manager.getNumOpenChkItems())
            << "Expected 1 items after population (set)";

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
    store_item(vbid, "key", "value");
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
        store_item(vbid, "key_2", "value");
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
    destroy_dcp_stream();
}

TEST_P(StreamTest, test_mb18625) {
    // Add an item.
    store_item(vbid, "key", "value");

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
    EXPECT_EQ(DcpResponse::Event::StreamEnd, op->getEvent())
        << "Expected the STREAM_END message";
    delete op;

    // Expect no other message to be queued after stream end message
    EXPECT_EQ(0, (mock_stream->public_readyQ()).size())
        << "Expected no more messages in the readyQ";
    destroy_dcp_stream();
}

/* Stream items from a DCP backfill */
TEST_P(StreamTest, BackfillOnly) {
    /* Add 3 items */
    int numItems = 3;
    addItemsAndRemoveCheckpoint(numItems);

    /* Set up a DCP stream for the backfill */
    setup_dcp_stream();
    MockActiveStream* mock_stream =
            static_cast<MockActiveStream*>(stream.get());

    /* We want the backfill task to run in a background thread */
    ExecutorPool::get()->setNumAuxIO(1);
    mock_stream->transitionStateToBackfilling();

    /* Wait for the backfill task to complete */
    {
        std::chrono::microseconds uSleepTime(128);
        while (numItems != mock_stream->getLastReadSeqno()) {
            uSleepTime = decayingSleep(uSleepTime);
        }
    }

    /* Verify that all items are read in the backfill */
    EXPECT_EQ(numItems, mock_stream->getNumBackfillItems());

    /* Since backfill items are sitting in the readyQ, check if the stat is
       updated correctly */
    EXPECT_EQ(numItems, mock_stream->getNumBackfillItemsRemaining());

    destroy_dcp_stream();
    /* [TODO]: Expand the testcase to check if snapshot marker, all individual
               items are read correctly */
}

/* Stream items from a DCP backfill with very small backfill buffer.
   However small the backfill buffer is, backfill must not stop, it must
   proceed to completion eventually */
TEST_P(StreamTest, BackfillSmallBuffer) {
    if (bucketType == "ephemeral") {
        /* Ephemeral buckets is not memory managed for now. Will be memory
           managed soon and then this test will be enabled */
        return;
    }

    /* Add 2 items */
    int numItems = 2;
    addItemsAndRemoveCheckpoint(numItems);

    /* Set up a DCP stream for the backfill */
    setup_dcp_stream();
    MockActiveStream* mock_stream =
            dynamic_cast<MockActiveStream*>(stream.get());

    /* set the DCP backfill buffer size to a value that is smaller than the
       size of a mutation */
    MockDcpProducer* mock_producer =
            dynamic_cast<MockDcpProducer*>(producer.get());
    mock_producer->setBackfillBufferSize(1);

    /* We want the backfill task to run in a background thread */
    ExecutorPool::get()->setNumAuxIO(1);
    mock_stream->transitionStateToBackfilling();

    /* Backfill can only read 1 as its buffer will become full after that */
    {
        std::chrono::microseconds uSleepTime(128);
        while ((numItems - 1) != mock_stream->getLastReadSeqno()) {
            uSleepTime = decayingSleep(uSleepTime);
        }
    }

    /* Consume the backfill item(s) */
    mock_stream->consumeBackfillItems(/*snapshot*/ 1 + /*mutation*/ 1);

    /* We should see that buffer full status must be false as we have read
       the item in the backfill buffer */
    EXPECT_FALSE(mock_producer->getBackfillBufferFullStatus());

    /* Finish up with the backilling of the remaining item */
    {
        std::chrono::microseconds uSleepTime(128);
        while (numItems != mock_stream->getLastReadSeqno()) {
            uSleepTime = decayingSleep(uSleepTime);
        }
    }

    /* Read the other item */
    mock_stream->consumeBackfillItems(1);
    destroy_dcp_stream();
}

TEST_P(StreamTest, CursorDroppingBasicBackfillState) {
    /* Add 2 items; we need this to keep stream in backfill state */
    const int numItems = 2;
    addItemsAndRemoveCheckpoint(numItems);

    /* Set up a DCP stream */
    setup_dcp_stream();
    MockActiveStream* mock_stream =
            dynamic_cast<MockActiveStream*>(stream.get());

    /* Transition stream to backfill state and expect cursor dropping call to
       succeed */
    mock_stream->transitionStateToBackfilling();
    EXPECT_TRUE(mock_stream->public_handleSlowStream());

    /* Run the backfill task in background thread to run so that it can
       complete/cancel itself */
    ExecutorPool::get()->setNumAuxIO(1);
    /* Finish up with the backilling of the remaining item */
    {
        std::chrono::microseconds uSleepTime(128);
        while (numItems != mock_stream->getLastReadSeqno()) {
            uSleepTime = decayingSleep(uSleepTime);
        }
    }
    destroy_dcp_stream();
}

TEST_P(StreamTest, CursorDroppingBasicInMemoryState) {
    /* Set up a DCP stream */
    setup_dcp_stream();
    MockActiveStream* mock_stream =
            dynamic_cast<MockActiveStream*>(stream.get());

    /* Transition stream to in-memory state and expect cursor dropping call to
       succeed */
    mock_stream->transitionStateToBackfilling();
    mock_stream->transitionStateToInMemory();
    EXPECT_TRUE(mock_stream->public_handleSlowStream());
    destroy_dcp_stream();
}

TEST_P(StreamTest, CursorDroppingBasicNotAllowedStates) {
    /* Set up a DCP stream */
    setup_dcp_stream(DCP_ADD_STREAM_FLAG_TAKEOVER);
    MockActiveStream* mock_stream =
            dynamic_cast<MockActiveStream*>(stream.get());

    /* Transition stream to takeoverSend state and expect cursor dropping call
       to fail */
    mock_stream->transitionStateToBackfilling();
    mock_stream->transitionStateToTakeoverSend();
    EXPECT_FALSE(mock_stream->public_handleSlowStream());

    /* Transition stream to takeoverWait state and expect cursor dropping call
       to fail */
    mock_stream->transitionStateToTakeoverWait();
    EXPECT_FALSE(mock_stream->public_handleSlowStream());

    /* Transition stream to takeoverSend state and expect cursor dropping call
       to fail */
    mock_stream->transitionStateToTakeoverDead();
    EXPECT_FALSE(mock_stream->public_handleSlowStream());
    destroy_dcp_stream();
}

class CacheCallbackTest : public StreamTest {
protected:
    void SetUp() override {
        StreamTest::SetUp();
        store_item(vbid, key, "value");

        /* Create new checkpoint so that we can remove the current checkpoint
         * and force a backfill in the DCP stream */
        CheckpointManager& ckpt_mgr = vb0->checkpointManager;
        ckpt_mgr.createNewCheckpoint();

        /* Wait for removal of the old checkpoint, this also would imply that
         * the items are persisted (in case of persistent buckets) */
        {
            bool new_ckpt_created;
            std::chrono::microseconds uSleepTime(128);
            while (numItems !=
                    ckpt_mgr.removeClosedUnrefCheckpoints(*vb0,
                                                          new_ckpt_created)) {
                uSleepTime = decayingSleep(uSleepTime);
            }
        }

        /* Set up a DCP stream for the backfill */
        setup_dcp_stream();
    }

    void TearDown() override {
        producer.get()->closeAllStreams();
        StreamTest::TearDown();
    }

    const size_t numItems = 1;
    const std::string key = "key";
    const DocKey docKey{key, DocNamespace::DefaultCollection};
};

/*
 * Tests the callback member function of the CacheCallback class.  This
 * particular test should result in the CacheCallback having a status of
 * ENGINE_KEY_EEXISTS.
 */
TEST_P(CacheCallbackTest, CacheCallback_key_eexists) {
    MockActiveStream* mockStream = static_cast<MockActiveStream*>(stream.get());
    active_stream_t activeStream(mockStream);
    CacheCallback callback(*engine, activeStream);

    mockStream->transitionStateToBackfilling();
    CacheLookup lookup(docKey, /*BySeqno*/ 1, vbid);
    callback.callback(lookup);

    /* Invoking callback should result in backfillReceived being called on
     * activeStream, which should return true and hence set the callback status
     * to ENGINE_KEY_EEXISTS.
     */
    EXPECT_EQ(ENGINE_KEY_EEXISTS, callback.getStatus());

    /* Verify that the item is read in the backfill */
    EXPECT_EQ(numItems, mockStream->getNumBackfillItems());

    /* Verify have the backfill item sitting in the readyQ */
    EXPECT_EQ(numItems, mockStream->public_readyQ().size());
}

/*
 * Tests the callback member function of the CacheCallback class.  This
 * particular test should result in the CacheCallback having a status of
 * ENGINE_SUCCESS.
 */
TEST_P(CacheCallbackTest, CacheCallback_engine_success) {
    MockActiveStream* mockStream = static_cast<MockActiveStream*>(stream.get());
    active_stream_t activeStream(mockStream);
    CacheCallback callback(*engine, activeStream);

    mockStream->transitionStateToBackfilling();
    // Passing in wrong BySeqno - should be 1, but passing in 0
    CacheLookup lookup(docKey, /*BySeqno*/ 0, vbid);
    callback.callback(lookup);

    /* Invoking callback should result in backfillReceived NOT being called on
     * activeStream, and hence the callback status should be set to
     * ENGINE_SUCCESS.
     */
    EXPECT_EQ(ENGINE_SUCCESS, callback.getStatus());

    /* Verify that the item is not read in the backfill */
    EXPECT_EQ(0, mockStream->getNumBackfillItems());

    /* Verify do not have the backfill item sitting in the readyQ */
    EXPECT_EQ(0, mockStream->public_readyQ().size());
}

/*
 * Tests the callback member function of the CacheCallback class.  This
 * particular test should result in the CacheCallback having a status of
 * ENGINE_ENOMEM.
 */
TEST_P(CacheCallbackTest, CacheCallback_engine_enomem) {
    /*
     * Ensure that DcpProducer::recordBackfillManagerBytesRead returns false
     * by setting the backfill buffer size to zero, and then setting bytes read
     * to one.
     */
    dynamic_cast<MockDcpProducer*>(producer.get())->setBackfillBufferSize(0);
    dynamic_cast<MockDcpProducer*>(producer.get())->bytesForceRead(1);

    MockActiveStream* mockStream = static_cast<MockActiveStream*>(stream.get());
    active_stream_t activeStream(mockStream);
    CacheCallback callback(*engine, activeStream);

    mockStream->transitionStateToBackfilling();
    CacheLookup lookup(docKey, /*BySeqno*/ 1, vbid);
    callback.callback(lookup);

    /* Invoking callback should result in backfillReceived being called on
     * activeStream, which should return false (due to
     * DcpProducer::recordBackfillManagerBytesRead returning false), and hence
     * set the callback status to ENGINE_ENOMEM.
     */
    EXPECT_EQ(ENGINE_ENOMEM, callback.getStatus());

    /* Verify that the item is not read in the backfill */
    EXPECT_EQ(0, mockStream->getNumBackfillItems());

    /* Verify do not have the backfill item sitting in the readyQ */
    EXPECT_EQ(0, mockStream->public_readyQ().size());
}

class ConnectionTest : public DCPTest,
                       public ::testing::WithParamInterface<
                               std::tuple<std::string, std::string>> {
protected:
    void SetUp() override {
        bucketType = std::get<0>(GetParam());
        DCPTest::SetUp();
        vbid = 0;
        if (bucketType == "ephemeral") {
            engine->getConfiguration().setEphemeralFullPolicy(
                    std::get<1>(GetParam()));
        }
    }

    ENGINE_ERROR_CODE set_vb_state(uint16_t vbid, vbucket_state_t state) {
        return engine->getKVBucket()->setVBucketState(vbid, state, true);
    }

    /**
     * Creates a consumer conn and sends items on the conn with memory usage
     * near to replication threshold
     *
     * @param beyondThreshold indicates if the memory usage should above the
     *                        threshold or just below it
     */
    void sendConsumerMutationsNearThreshold(bool beyondThreshold);

    /**
     * Creates a consumer conn and makes the consumer processor task run with
     * memory usage near to replication threshold
     *
     * @param beyondThreshold indicates if the memory usage should above the
     *                        threshold or just below it
     */
    void processConsumerMutationsNearThreshold(bool beyondThreshold);

    /* vbucket associated with this connection */
    uint16_t vbid;
};

ENGINE_ERROR_CODE mock_noop_return_engine_e2big(const void* cookie,uint32_t opaque) {
    return ENGINE_E2BIG;
}

/*
 * Test that the connection manager interval is a multiple of the value we
 * are setting the noop interval to.  This ensures we do not set the the noop
 * interval to a value that cannot be adhered to.  The reason is that if there
 * is no DCP traffic we snooze for the connection manager interval before
 * sending the noop.
 */
TEST_P(ConnectionTest, test_mb19955) {
    const void* cookie = create_mock_cookie();
    engine->getConfiguration().setConnectionManagerInterval(2);

    // Create a Mock Dcp producer
    MockDcpProducer producer(*engine,
                             cookie,
                             "test_producer",
                             /*flags*/ 0,
                             {/*no json*/});
    // "1" is not a multiple of "2" and so we should return ENGINE_EINVAL
    EXPECT_EQ(ENGINE_EINVAL, producer.control(0,
                                               "set_noop_interval",
                                               sizeof("set_noop_interval"),
                                               "1",
                                               sizeof("1")))
        << "Expected producer.control to return ENGINE_EINVAL";
    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, test_maybesendnoop_buffer_full) {
    const void* cookie = create_mock_cookie();
    // Create a Mock Dcp producer
    MockDcpProducer producer(
            *engine, cookie, "test_producer", /*flags*/ 0, {/*no json*/});

    struct dcp_message_producers producers = {nullptr, nullptr, nullptr, nullptr,
        nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
        mock_noop_return_engine_e2big, nullptr, nullptr};

    producer.setNoopEnabled(true);
    const auto send_time = ep_current_time() + 21;
    producer.setNoopSendTime(send_time);
    ENGINE_ERROR_CODE ret = producer.maybeSendNoop(&producers);
    EXPECT_EQ(ENGINE_E2BIG, ret)
    << "maybeSendNoop not returning ENGINE_E2BIG";
    EXPECT_FALSE(producer.getNoopPendingRecv())
    << "Waiting for noop acknowledgement";
    EXPECT_EQ(send_time, producer.getNoopSendTime())
    << "SendTime has been updated";
    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, test_maybesendnoop_send_noop) {
    const void* cookie = create_mock_cookie();
    // Create a Mock Dcp producer
    MockDcpProducer producer(
            *engine, cookie, "test_producer", /*flags*/ 0, {/*no json*/});

    std::unique_ptr<dcp_message_producers> producers(get_dcp_producers(handle, engine_v1));
    producer.setNoopEnabled(true);
    const auto send_time = ep_current_time() + 21;
    producer.setNoopSendTime(send_time);
    ENGINE_ERROR_CODE ret = producer.maybeSendNoop(producers.get());
    EXPECT_EQ(ENGINE_WANT_MORE, ret)
    << "maybeSendNoop not returning ENGINE_WANT_MORE";
    EXPECT_TRUE(producer.getNoopPendingRecv())
    << "Not waiting for noop acknowledgement";
    EXPECT_NE(send_time, producer.getNoopSendTime())
    << "SendTime has not been updated";
    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, test_maybesendnoop_noop_already_pending) {
    const void* cookie = create_mock_cookie();
    // Create a Mock Dcp producer
    MockDcpProducer producer(*engine,
                             cookie,
                             "test_producer",
                             /*flags*/ 0,
                             {/*no json*/});

    std::unique_ptr<dcp_message_producers> producers(
            get_dcp_producers(handle, engine_v1));
    const auto send_time = ep_current_time();
    TimeTraveller marty(engine->getConfiguration().getDcpIdleTimeout() + 1);
    producer.setNoopEnabled(true);
    producer.setNoopSendTime(send_time);
    ENGINE_ERROR_CODE ret = producer.maybeSendNoop(producers.get());
    // Check to see if a noop was sent i.e. returned ENGINE_WANT_MORE
    EXPECT_EQ(ENGINE_WANT_MORE, ret)
        << "maybeSendNoop not returning ENGINE_WANT_MORE";
    EXPECT_TRUE(producer.getNoopPendingRecv())
        << "Not awaiting noop acknowledgement";
    EXPECT_NE(send_time, producer.getNoopSendTime())
        << "SendTime has not been updated";
    ret = producer.maybeSendNoop(producers.get());
    // Check to see if a noop was not sent i.e. returned ENGINE_FAILED
    EXPECT_EQ(ENGINE_FAILED, ret)
        << "maybeSendNoop not returning ENGINE_FAILED";
    producer.setLastReceiveTime(send_time);
    ret = producer.maybeDisconnect();
    // Check to see if we want to disconnect i.e. returned ENGINE_DISCONNECT
    EXPECT_EQ(ENGINE_DISCONNECT, ret)
        << "maybeDisconnect not returning ENGINE_DISCONNECT";
    producer.setLastReceiveTime(send_time +
                                engine->getConfiguration().getDcpIdleTimeout() +
                                1);
    ret = producer.maybeDisconnect();
    // Check to see if we don't want to disconnect i.e. returned ENGINE_FAILED
    EXPECT_EQ(ENGINE_FAILED, ret)
        << "maybeDisconnect not returning ENGINE_FAILED";
    EXPECT_TRUE(producer.getNoopPendingRecv())
        << "Not waiting for noop acknowledgement";
    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, test_maybesendnoop_not_enabled) {
    const void* cookie = create_mock_cookie();
    // Create a Mock Dcp producer
    MockDcpProducer producer(
            *engine, cookie, "test_producer", /*flags*/ 0, {/*no json*/});

    std::unique_ptr<dcp_message_producers> producers(get_dcp_producers(handle, engine_v1));
    producer.setNoopEnabled(false);
    const auto send_time = ep_current_time() + 21;
    producer.setNoopSendTime(send_time);
    ENGINE_ERROR_CODE ret = producer.maybeSendNoop(producers.get());
    EXPECT_EQ(ENGINE_FAILED, ret)
    << "maybeSendNoop not returning ENGINE_FAILED";
    EXPECT_FALSE(producer.getNoopPendingRecv())
    << "Waiting for noop acknowledgement";
    EXPECT_EQ(send_time, producer.getNoopSendTime())
    << "SendTime has been updated";
    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, test_maybesendnoop_not_sufficient_time_passed) {
    const void* cookie = create_mock_cookie();
    // Create a Mock Dcp producer
    MockDcpProducer producer(
            *engine, cookie, "test_producer", /*flags*/ 0, {/*no json*/});

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

TEST_P(ConnectionTest, test_deadConnections) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize();
    const void *cookie = create_mock_cookie();
    // Create a new Dcp producer
    dcp_producer_t producer = connMap.newProducer(cookie,
                                                  "test_producer",
                                                  /*flags*/ 0,
                                                  {/*no json*/});

    // Disconnect the producer connection
    connMap.disconnect(cookie);
    EXPECT_EQ(1, connMap.getNumberOfDeadConnections())
        << "Unexpected number of dead connections";
    connMap.manageConnections();
    // Should be zero deadConnections
    EXPECT_EQ(0, connMap.getNumberOfDeadConnections())
        << "Dead connections still remain";
}

TEST_P(ConnectionTest, test_mb23637_findByNameWithConnectionDoDisconnect) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize();
    const void *cookie = create_mock_cookie();
    // Create a new Dcp producer
    dcp_producer_t producer = connMap.newProducer(cookie,
                                                  "test_producer",
                                                  /*flags*/ 0,
                                                  {/*nojson*/});
    // should be able to find the connection
    ASSERT_NE(connection_t(nullptr),
              connMap.findByName("eq_dcpq:test_producer"));
    // Disconnect the producer connection
    connMap.disconnect(cookie);
    ASSERT_EQ(1, connMap.getNumberOfDeadConnections())
        << "Unexpected number of dead connections";
    // should not be able to find because the connection has been marked as
    // wanting to disconnect
    EXPECT_EQ(connection_t(nullptr),
              connMap.findByName("eq_dcpq:test_producer"));
    connMap.manageConnections();
    // Should be zero deadConnections
    EXPECT_EQ(0, connMap.getNumberOfDeadConnections())
        << "Dead connections still remain";
}

TEST_P(ConnectionTest, test_mb23637_findByNameWithDuplicateConnections) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize();
    const void* cookie1 = create_mock_cookie();
    const void* cookie2 = create_mock_cookie();
    // Create a new Dcp producer
    dcp_producer_t producer = connMap.newProducer(cookie1,
                                                  "test_producer",
                                                  /*flags*/ 0,
                                                  {/*nojson*/});
    ASSERT_NE(0, producer) << "producer is null";
    // should be able to find the connection
    ASSERT_NE(connection_t(nullptr),
              connMap.findByName("eq_dcpq:test_producer"));

    // Create a duplicate Dcp producer
    dcp_producer_t duplicateproducer = connMap.newProducer(
            cookie2, "test_producer", /*flags*/ 0, {/*nojson*/});
    ASSERT_TRUE(producer->doDisconnect()) << "producer doDisconnect == false";
    ASSERT_NE(0, duplicateproducer) << "duplicateproducer is null";

    // should find the duplicateproducer as the first producer has been marked
    // as wanting to disconnect
    EXPECT_EQ(duplicateproducer,
              connMap.findByName("eq_dcpq:test_producer"));

    // Disconnect the producer connection
    connMap.disconnect(cookie1);
    // Disconnect the duplicateproducer connection
    connMap.disconnect(cookie2);
    EXPECT_EQ(2, connMap.getNumberOfDeadConnections())
        << "Unexpected number of dead connections";

    connMap.manageConnections();
    // Should be zero deadConnections
    EXPECT_EQ(0, connMap.getNumberOfDeadConnections())
        << "Dead connections still remain";
}


TEST_P(ConnectionTest, test_mb17042_duplicate_name_producer_connections) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize();
    const void* cookie1 = create_mock_cookie();
    const void* cookie2 = create_mock_cookie();
    // Create a new Dcp producer
    dcp_producer_t producer = connMap.newProducer(cookie1,
                                                  "test_producer",
                                                  /*flags*/ 0,
                                                  {/*nojson*/});
    EXPECT_NE(0, producer) << "producer is null";

    // Create a duplicate Dcp producer
    dcp_producer_t duplicateproducer = connMap.newProducer(cookie2,
                                                           "test_producer",
                                                           /*flags*/ 0,
                                                           {/*nojson*/});
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

TEST_P(ConnectionTest, test_mb17042_duplicate_name_consumer_connections) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize();
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

TEST_P(ConnectionTest, test_mb17042_duplicate_cookie_producer_connections) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize();
    const void* cookie = create_mock_cookie();
    // Create a new Dcp producer
    dcp_producer_t producer = connMap.newProducer(cookie,
                                                  "test_producer1",
                                                  /*flags*/ 0,
                                                  {/*nojson*/});

    // Create a duplicate Dcp producer
    dcp_producer_t duplicateproducer = connMap.newProducer(cookie,
                                                           "test_producer2",
                                                           /*flags*/ 0,
                                                           {/*nojson*/});

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

TEST_P(ConnectionTest, test_mb17042_duplicate_cookie_consumer_connections) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize();
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

TEST_P(ConnectionTest, test_update_of_last_message_time_in_consumer) {
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
    const DocKey docKey{ nullptr, 0, DocNamespace::DefaultCollection};
    consumer->mutation(0, // opaque
                       docKey,
                       {}, // value
                       0, // priv bytes
                       PROTOCOL_BINARY_RAW_BYTES,
                       0, // cas
                       0, // vbucket
                       0, // flags
                       0, // locktime
                       0, // by seqno
                       0, // rev seqno
                       0, // exptime
                       {}, // meta
                       0); // nru
    EXPECT_NE(1234, consumer->getLastMessageTime())
        << "lastMessagerTime not updated for mutation";
    consumer->setLastMessageTime(1234);
    consumer->deletion(0, // opaque
                       docKey,
                       {}, // value
                       0, // priv bytes
                       PROTOCOL_BINARY_RAW_BYTES,
                       0, // cas
                       0, // vbucket
                       0, // by seqno
                       0, // rev seqno
                       {}); // meta
    EXPECT_NE(1234, consumer->getLastMessageTime())
        << "lastMessagerTime not updated for deletion";
    consumer->setLastMessageTime(1234);
    consumer->expiration(0, // opaque
                         docKey,
                         {}, // value
                         0, // priv bytes
                         PROTOCOL_BINARY_RAW_BYTES,
                         0, // cas
                         0, // vbucket
                         0, // by seqno
                         0, // rev seqno
                         {}); // meta
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

TEST_P(ConnectionTest, test_consumer_add_stream) {
    const void* cookie = create_mock_cookie();
    uint16_t vbid = 0;

    /* Create a Mock Dcp consumer. Since child class subobj of MockDcpConsumer
       obj are accounted for by SingleThreadedRCPtr, use the same here */
    connection_t conn = new MockDcpConsumer(*engine, cookie, "test_consumer");
    MockDcpConsumer* consumer = dynamic_cast<MockDcpConsumer*>(conn.get());

    ASSERT_EQ(ENGINE_SUCCESS, set_vb_state(vbid, vbucket_state_replica));
    ASSERT_EQ(ENGINE_SUCCESS, consumer->addStream(/*opaque*/0, vbid,
                                                  /*flags*/0));

    /* Set the passive to dead state. Note that we want to set the stream to
       dead state but not erase it from the streams map in the consumer
       connection*/
    MockPassiveStream *stream = static_cast<MockPassiveStream*>
                                    ((consumer->getVbucketStream(vbid)).get());

    stream->transitionStateToDead();

    /* Add a passive stream on the same vb */
    ASSERT_EQ(ENGINE_SUCCESS, consumer->addStream(/*opaque*/0, vbid,
                                                  /*flags*/0));

    /* Expected the newly added stream to be in active state */
    stream = static_cast<MockPassiveStream*>
                                    ((consumer->getVbucketStream(vbid)).get());
    ASSERT_TRUE(stream->isActive());

    /* Close stream before deleting the connection */
    ASSERT_EQ(ENGINE_SUCCESS, consumer->closeStream(/*opaque*/0, vbid));

    destroy_mock_cookie(cookie);
}

// Regression test for MB 20645 - ensure that a call to addStats after a
// connection has been disconnected (and closeAllStreams called) doesn't crash.
TEST_P(ConnectionTest, test_mb20645_stats_after_closeAllStreams) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize();
    const void *cookie = create_mock_cookie();
    // Create a new Dcp producer
    dcp_producer_t producer = connMap.newProducer(cookie,
                                                  "test_producer",
                                                  /*flags*/ 0,
                                                  {/*no json*/});

    // Disconnect the producer connection
    connMap.disconnect(cookie);

    // Try to read stats. Shouldn't crash.
    producer->addStats([](const char *key, const uint16_t klen,
                          const char *val, const uint32_t vlen,
                          const void *cookie) {}, nullptr);

    destroy_mock_cookie(cookie);
}

// Verify that when a DELETE_BUCKET event occurs, we correctly notify any
// DCP connections which are currently in ewouldblock state, so the frontend
// can correctly close the connection.
// If we don't notify then front-end connections can hang for a long period of
// time).
TEST_P(ConnectionTest, test_mb20716_connmap_notify_on_delete) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize();
    const void *cookie = create_mock_cookie();
    // Create a new Dcp producer.
    dcp_producer_t producer = connMap.newProducer(cookie,
                                                  "mb_20716r",
                                                  /*flags*/ 0,
                                                  {/*nojson*/});

    // Check preconditions.
    EXPECT_TRUE(producer->isPaused());

    // Hook into notify_io_complete.
    // We (ab)use the engine_specific API to pass a pointer to a count of
    // how many times notify_io_complete has been called.
    size_t notify_count = 0;
    SERVER_COOKIE_API* scapi = get_mock_server_api()->cookie;
    scapi->store_engine_specific(cookie, &notify_count);
    auto orig_notify_io_complete = scapi->notify_io_complete;
    scapi->notify_io_complete = [](const void *cookie,
                                   ENGINE_ERROR_CODE status) {
        auto* notify_ptr = reinterpret_cast<size_t*>(
                get_mock_server_api()->cookie->get_engine_specific(cookie));
        (*notify_ptr)++;
    };

    // 0. Should start with no notifications.
    ASSERT_EQ(0, notify_count);

    // 1. Check that the periodic connNotifier (notifyAllPausedConnections)
    // isn't sufficient to notify (it shouldn't be, as our connection has
    // no notification pending).
    connMap.notifyAllPausedConnections();
    ASSERT_EQ(0, notify_count);

    // 1. Simulate a bucket deletion.
    connMap.shutdownAllConnections();

    // Can also get a second notify as part of manageConnections being called
    // in shutdownAllConnections().
    EXPECT_GE(notify_count, 1)
        << "expected at least one notify after shutting down all connections";

    // Restore notify_io_complete callback.
    scapi->notify_io_complete = orig_notify_io_complete;
    destroy_mock_cookie(cookie);
}

// Consumer variant of above test.
TEST_P(ConnectionTest, test_mb20716_connmap_notify_on_delete_consumer) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize();
    const void *cookie = create_mock_cookie();
    // Create a new Dcp producer
    dcp_consumer_t consumer = connMap.newConsumer(cookie,
                                                  "mb_20716_consumer");

    // Move consumer into paused state (aka EWOULDBLOCK).
    std::unique_ptr<dcp_message_producers> producers(
            get_dcp_producers(handle, engine_v1));
    ENGINE_ERROR_CODE result;
    do {
        result = consumer->step(producers.get());
    } while (result == ENGINE_WANT_MORE);
    EXPECT_EQ(ENGINE_SUCCESS, result);

    // Check preconditions.
    EXPECT_TRUE(consumer->isPaused());

    // Hook into notify_io_complete.
    // We (ab)use the engine_specific API to pass a pointer to a count of
    // how many times notify_io_complete has been called.
    size_t notify_count = 0;
    SERVER_COOKIE_API* scapi = get_mock_server_api()->cookie;
    scapi->store_engine_specific(cookie, &notify_count);
    auto orig_notify_io_complete = scapi->notify_io_complete;
    scapi->notify_io_complete = [](const void *cookie,
                                   ENGINE_ERROR_CODE status) {
        auto* notify_ptr = reinterpret_cast<size_t*>(
                get_mock_server_api()->cookie->get_engine_specific(cookie));
        (*notify_ptr)++;
    };

    // 0. Should start with no notifications.
    ASSERT_EQ(0, notify_count);

    // 1. Check that the periodic connNotifier (notifyAllPausedConnections)
    // isn't sufficient to notify (it shouldn't be, as our connection has
    // no notification pending).
    connMap.notifyAllPausedConnections();
    ASSERT_EQ(0, notify_count);

    // 2. Simulate a bucket deletion.
    connMap.shutdownAllConnections();

    // Can also get a second notify as part of manageConnections being called
    // in shutdownAllConnections().
    EXPECT_GE(notify_count, 1)
        << "expected at least one notify after shutting down all connections";

    // Restore notify_io_complete callback.
    scapi->notify_io_complete = orig_notify_io_complete;
    destroy_mock_cookie(cookie);
}

/*
 * The following tests that once a vbucket has been put into a backfillphase
 * the openCheckpointID is 0.  In addition it checks that a subsequent
 * snapshotMarker results in a new checkpoint being created.
 */
TEST_P(ConnectionTest, test_mb21784) {
    // Make vbucket replica so can add passive stream
    ASSERT_EQ(ENGINE_SUCCESS, set_vb_state(vbid, vbucket_state_replica));

    const void *cookie = create_mock_cookie();
    /*
     * Create a Mock Dcp consumer. Since child class subobj of MockDcpConsumer
     *  obj are accounted for by SingleThreadedRCPtr, use the same here
     */
    connection_t conn = new MockDcpConsumer(*engine, cookie, "test_consumer");
    MockDcpConsumer* consumer = static_cast<MockDcpConsumer*>(conn.get());

    // Add passive stream
    ASSERT_EQ(ENGINE_SUCCESS, consumer->addStream(/*opaque*/0, vbid,
                                                  /*flags*/0));
    // Get the checkpointManager
    auto& manager = engine->getKVBucket()->getVBucket(vbid)->checkpointManager;

    // Because the vbucket was previously active it will have an
    // openCheckpointId of 2
    EXPECT_EQ(2, manager.getOpenCheckpointId());

    // Send a snapshotMarker to move the vbucket into a backfilling state
    consumer->snapshotMarker(/*opaque*/1,
                             /*vbucket*/0,
                             /*start_seqno*/0,
                             /*end_seqno*/0,
                             /*flags set to MARKER_FLAG_DISK*/0x2);

    // A side effect of moving the vbucket into a backfill state is that
    // the openCheckpointId is set to 0
    EXPECT_EQ(0, manager.getOpenCheckpointId());

    consumer->snapshotMarker(/*opaque*/1,
                             /*vbucket*/0,
                             /*start_seqno*/0,
                             /*end_seqno*/0,
                             /*flags*/0);

    // Check that a new checkpoint was created, which means the
    // opencheckpointid increases to 1
    EXPECT_EQ(1, manager.getOpenCheckpointId());

    // Close stream
    ASSERT_EQ(ENGINE_SUCCESS, consumer->closeStream(/*opaque*/0, vbid));
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
    std::unique_ptr<MockDcpConnMap> connMap;
    dcp_producer_t producer;
    int callbacks;
};

class ConnMapNotifyTest {
public:
    ConnMapNotifyTest(EventuallyPersistentEngine& engine)
        : connMap(new MockDcpConnMap(engine)),
          callbacks(0) {
        connMap->initialize();

        // Use 'this' instead of a mock cookie
        producer = connMap->newProducer(static_cast<void*>(this),
                                        "test_producer",
                                        /*flags*/ 0,
                                        {/*no json*/});
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
    notifyTest.producer->unPause();

    // 3. Call notifyAllPausedConnections - as the connection is not paused
    // this should *not* invoke notifyIOComplete.
    notifyTest.connMap->notifyAllPausedConnections();

    // 3.1 Should have not had any callbacks.
    EXPECT_EQ(0, notifyTest.getCallbacks());
    // 3.2 Should have no pending notifications.
    EXPECT_EQ(0, notifyTest.connMap->getPendingNotifications().size());

    // 4. Now mark the connection as paused.
    ASSERT_FALSE(notifyTest.producer->isPaused());
    notifyTest.producer->pause();

    // 4. Add another notification - should queue the producer again.
    notifyTest.connMap->notifyPausedConnection(notifyTest.producer.get(),
                                               /*schedule*/true);
    EXPECT_EQ(1, notifyTest.connMap->getPendingNotifications().size());

    // 5. Call notifyAllPausedConnections a second time - as connection is
    //    paused this time we *should* get a callback.
    notifyTest.connMap->notifyAllPausedConnections();
    EXPECT_EQ(1, notifyTest.getCallbacks());
}

// Tests that the MutationResponse created for the deletion response is of the
// correct size.
TEST_P(ConnectionTest, test_mb24424_deleteResponse) {
    const void* cookie = create_mock_cookie();
    uint16_t vbid = 0;

    connection_t conn = new MockDcpConsumer(*engine, cookie, "test_consumer");
    MockDcpConsumer* consumer = dynamic_cast<MockDcpConsumer*>(conn.get());

    ASSERT_EQ(ENGINE_SUCCESS, set_vb_state(vbid, vbucket_state_replica));
    ASSERT_EQ(ENGINE_SUCCESS, consumer->addStream(/*opaque*/0, vbid,
                                                  /*flags*/0));

    MockPassiveStream *stream = static_cast<MockPassiveStream*>
                                       ((consumer->
                                               getVbucketStream(vbid)).get());
    ASSERT_TRUE(stream->isActive());

    std::string key = "key";
    std::string data = R"({"json":"yes"})";
    const DocKey docKey{ reinterpret_cast<const uint8_t*>(key.data()),
        key.size(),
        DocNamespace::DefaultCollection};
    cb::const_byte_buffer value{reinterpret_cast<const uint8_t*>(data.data()),
        data.size()};
    uint8_t extMeta[1] = {uint8_t(PROTOCOL_BINARY_DATATYPE_JSON)};
    cb::const_byte_buffer meta{extMeta, sizeof(uint8_t)};

    consumer->deletion(/*opaque*/1,
                       /*key*/docKey,
                       /*values*/value,
                       /*priv_bytes*/0,
                       /*datatype*/PROTOCOL_BINARY_DATATYPE_JSON,
                       /*cas*/0,
                       /*vbucket*/vbid,
                       /*bySeqno*/1,
                       /*revSeqno*/0,
                       /*meta*/meta);

    auto messageSize = MutationResponse::deletionBaseMsgBytes +
            key.size() + data.size() + sizeof(extMeta);

    EXPECT_EQ(messageSize, stream->responseMessageSize);

    /* Close stream before deleting the connection */
    ASSERT_EQ(ENGINE_SUCCESS, consumer->closeStream(/*opaque*/0, vbid));

    destroy_mock_cookie(cookie);
}

// Tests that the MutationResponse created for the mutation response is of the
// correct size.
TEST_P(ConnectionTest, test_mb24424_mutationResponse) {
    const void* cookie = create_mock_cookie();
    uint16_t vbid = 0;

    connection_t conn = new MockDcpConsumer(*engine, cookie, "test_consumer");
    MockDcpConsumer* consumer = dynamic_cast<MockDcpConsumer*>(conn.get());

    ASSERT_EQ(ENGINE_SUCCESS, set_vb_state(vbid, vbucket_state_replica));
    ASSERT_EQ(ENGINE_SUCCESS, consumer->addStream(/*opaque*/0, vbid,
                                                  /*flags*/0));

    MockPassiveStream *stream = static_cast<MockPassiveStream*>
                                       ((consumer->
                                               getVbucketStream(vbid)).get());
    ASSERT_TRUE(stream->isActive());

    std::string key = "key";
    std::string data = R"({"json":"yes"})";
    const DocKey docKey{ reinterpret_cast<const uint8_t*>(key.data()),
        key.size(),
        DocNamespace::DefaultCollection};
    cb::const_byte_buffer value{reinterpret_cast<const uint8_t*>(data.data()),
        data.size()};
    uint8_t extMeta[1] = {uint8_t(PROTOCOL_BINARY_DATATYPE_JSON)};
    cb::const_byte_buffer meta{extMeta, sizeof(uint8_t)};

    consumer->mutation(/*opaque*/1,
                       /*key*/docKey,
                       /*values*/value,
                       /*priv_bytes*/0,
                       /*datatype*/PROTOCOL_BINARY_DATATYPE_JSON,
                       /*cas*/0,
                       /*vbucket*/vbid,
                       /*flags*/0,
                       /*bySeqno*/1,
                       /*revSeqno*/0,
                       /*exptime*/0,
                       /*lock_time*/0,
                       /*meta*/meta,
                       /*nru*/0);

    auto messageSize = MutationResponse::mutationBaseMsgBytes +
            key.size() + data.size() + sizeof(extMeta);

    EXPECT_EQ(messageSize, stream->responseMessageSize);

    /* Close stream before deleting the connection */
    ASSERT_EQ(ENGINE_SUCCESS, consumer->closeStream(/*opaque*/0, vbid));

    destroy_mock_cookie(cookie);
}

void ConnectionTest::sendConsumerMutationsNearThreshold(bool beyondThreshold) {
    const void* cookie = create_mock_cookie();
    const uint32_t opaque = 1;
    const uint64_t snapStart = 1, snapEnd = 10;
    const uint64_t bySeqno = snapStart;

    /* Set up a consumer connection */
    connection_t conn = new MockDcpConsumer(*engine, cookie, "test_consumer");
    MockDcpConsumer* consumer = dynamic_cast<MockDcpConsumer*>(conn.get());

    /* Replica vbucket */
    ASSERT_EQ(ENGINE_SUCCESS, set_vb_state(vbid, vbucket_state_replica));

    /* Passive stream */
    ASSERT_EQ(ENGINE_SUCCESS,
              consumer->addStream(/*opaque*/ 0,
                                  vbid,
                                  /*flags*/ 0));
    MockPassiveStream* stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    ASSERT_TRUE(stream->isActive());

    /* Send a snapshotMarker before sending items for replication */
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->snapshotMarker(opaque,
                                       vbid,
                                       snapStart,
                                       snapEnd,
                                       /* in-memory snapshot */ 0x1));

    /* Send an item for replication */
    const DocKey docKey{nullptr, 0, DocNamespace::DefaultCollection};
    EXPECT_EQ(ENGINE_SUCCESS,
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
        stats.setMaxDataSize(stats.getTotalMemoryUsed());
    } else {
        /* Set 'mem_used' just 1 byte less than the 'replication threshold'.
           That is we are below 'replication threshold', but not enough space
           for  the new item */
        stats.setMaxDataSize(stats.getTotalMemoryUsed() + 1);
        /* Simpler to set the replication threshold to 1 and test, rather than
           testing with maxData = (memUsed / replicationThrottleThreshold);
           that is, we are avoiding a division */
        engine->getConfiguration().setReplicationThrottleThreshold(100);
    }

    /* Send another item for replication */
    auto ret = consumer->mutation(opaque,
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
                                  0); // nru

    if ((engine->getConfiguration().getBucketType() == "ephemeral") &&
        (engine->getConfiguration().getEphemeralFullPolicy()) ==
                "fail_new_data") {
        /* Expect disconnect signal in Ephemeral with "fail_new_data" policy */
        EXPECT_EQ(ENGINE_DISCONNECT, ret);
    } else {
        /* In 'couchbase' buckets we buffer the replica items and indirectly
           throttle replication by not sending flow control acks to the
           producer. Hence we do not drop the connection here */
        EXPECT_EQ(ENGINE_SUCCESS, ret);
    }

    /* Close stream before deleting the connection */
    EXPECT_EQ(ENGINE_SUCCESS, consumer->closeStream(opaque, vbid));

    destroy_mock_cookie(cookie);
}

/* Here we test how the DCP consumer handles the scenario where the memory
   usage is beyond the replication throttle threshold.
   In case of Ephemeral buckets with 'fail_new_data' policy it is expected to
   indicate close of the consumer conn and in other cases it is expected to
   just defer processing. */
TEST_P(ConnectionTest, ReplicateAfterThrottleThreshold) {
    sendConsumerMutationsNearThreshold(true);
}

/* Here we test how the DCP consumer handles the scenario where the memory
   usage is just below the replication throttle threshold, but will go over the
   threshold when it adds the new mutation from the processor buffer to the
   hashtable.
   In case of Ephemeral buckets with 'fail_new_data' policy it is expected to
   indicate close of the consumer conn and in other cases it is expected to
   just defer processing. */
TEST_P(ConnectionTest, ReplicateJustBeforeThrottleThreshold) {
    sendConsumerMutationsNearThreshold(false);
}

void ConnectionTest::processConsumerMutationsNearThreshold(
        bool beyondThreshold) {
    const void* cookie = create_mock_cookie();
    const uint32_t opaque = 1;
    const uint64_t snapStart = 1, snapEnd = 10;
    const uint64_t bySeqno = snapStart;

    /* Set up a consumer connection */
    connection_t conn = new MockDcpConsumer(*engine, cookie, "test_consumer");
    MockDcpConsumer* consumer = dynamic_cast<MockDcpConsumer*>(conn.get());

    /* Replica vbucket */
    ASSERT_EQ(ENGINE_SUCCESS, set_vb_state(vbid, vbucket_state_replica));

    /* Passive stream */
    ASSERT_EQ(ENGINE_SUCCESS,
              consumer->addStream(/*opaque*/ 0,
                                  vbid,
                                  /*flags*/ 0));
    MockPassiveStream* stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    ASSERT_TRUE(stream->isActive());

    /* Send a snapshotMarker before sending items for replication */
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->snapshotMarker(opaque,
                                       vbid,
                                       snapStart,
                                       snapEnd,
                                       /* in-memory snapshot */ 0x1));

    /* Simulate a situation where adding a mutation temporarily fails
       and hence adds the mutation to a replication buffer. For that, we
       set vbucket::takeover_backed_up to true */
    engine->getKVBucket()->getVBucket(vbid)->setTakeoverBackedUpState(true);

    /* Send an item for replication and expect it to be buffered */
    const DocKey docKey{"mykey", DocNamespace::DefaultCollection};
    EXPECT_EQ(ENGINE_SUCCESS,
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
        stats.setMaxDataSize(stats.getTotalMemoryUsed() / 4);
    } else {
        /* set max size to a value just over */
        stats.setMaxDataSize(stats.getTotalMemoryUsed() + 1);
        /* Simpler to set the replication threshold to 1 and test, rather than
           testing with maxData = (memUsed / replicationThrottleThreshold); that
           is, we are avoiding a division */
        engine->getConfiguration().setReplicationThrottleThreshold(100);
    }

    std::unique_ptr<dcp_message_producers> dcpStepProducers(
            get_dcp_producers(handle, engine_v1));
    if ((engine->getConfiguration().getBucketType() == "ephemeral") &&
        (engine->getConfiguration().getEphemeralFullPolicy()) ==
                "fail_new_data") {
        /* Make a call to the function that would be called by the processor
           task here */
        EXPECT_EQ(stop_processing, consumer->processBufferedItems());

        /* Expect the connection to be notified */
        EXPECT_FALSE(consumer->isPaused());

        /* Expect disconnect signal in Ephemeral with "fail_new_data" policy */
        EXPECT_EQ(ENGINE_DISCONNECT, consumer->step(dcpStepProducers.get()));
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
        EXPECT_EQ(ENGINE_WANT_MORE, consumer->step(dcpStepProducers.get()));

        /* Close stream before deleting the connection */
        EXPECT_EQ(ENGINE_SUCCESS, consumer->closeStream(opaque, vbid));
    }
    destroy_mock_cookie(cookie);
}

/* Here we test how the Processor task in DCP consumer handles the scenario
   where the memory usage is beyond the replication throttle threshold.
   In case of Ephemeral buckets with 'fail_new_data' policy it is expected to
   indicate close of the consumer conn and in other cases it is expected to
   just defer processing. */
TEST_P(ConnectionTest, ProcessReplicationBufferAfterThrottleThreshold) {
    processConsumerMutationsNearThreshold(true);
}

/* Here we test how the Processor task in DCP consumer handles the scenario
   where the memory usage is just below the replication throttle threshold,
   but will go over the threshold when it adds the new mutation from the
   processor buffer to the hashtable.
   In case of Ephemeral buckets with 'fail_new_data' policy it is expected to
   indicate close of the consumer conn and in other cases it is expected to
   just defer processing. */
TEST_P(ConnectionTest,
       DISABLED_ProcessReplicationBufferJustBeforeThrottleThreshold) {
    /* There are sporadic failures seen while testing this. The problem is
       we need to have a memory usage just below max_size, so we need to
       start at that point. But sometimes the memory usage goes further below
       resulting in the test failure (a hang). Hence commenting out the test.
       Can be run locally as and when needed. */
    processConsumerMutationsNearThreshold(false);
}

// Test cases which run in both Full and Value eviction
INSTANTIATE_TEST_CASE_P(PersistentAndEphemeral,
                        StreamTest,
                        ::testing::Values("persistent", "ephemeral"),
                        [](const ::testing::TestParamInfo<std::string>& info) {
                            return info.param;
                        });

// Test cases which run in both Full and Value eviction
INSTANTIATE_TEST_CASE_P(PersistentAndEphemeral,
                        CacheCallbackTest,
                        ::testing::Values("persistent", "ephemeral"),
                        [](const ::testing::TestParamInfo<std::string>& info) {
                            return info.param;
                        });

static auto allConfigValues = ::testing::Values(
        std::make_tuple(std::string("ephemeral"), std::string("auto_delete")),
        std::make_tuple(std::string("ephemeral"), std::string("fail_new_data")),
        std::make_tuple(std::string("persistent"), std::string{}));

INSTANTIATE_TEST_CASE_P(PersistentAndEphemeral,
                        ConnectionTest,
                        allConfigValues, );
