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

#include "dcp_stream_test.h"
#include "../mock/mock_dcp.h"
#include "../mock/mock_dcp_producer.h"
#include "../mock/mock_stream.h"
#include "checkpoint_manager.h"
#include "dcp/backfill_disk.h"
#include "dcp/backfill_memory.h"
#include "ep_engine.h"
#include "ephemeral_vb.h"
#include "failover-table.h"
#include "test_helpers.h"
#include <programs/engine_testapp/mock_server.h>
#include <xattr/utils.h>
#include <thread>

void StreamTest::SetUp() {
    bucketType = GetParam();
    DCPTest::SetUp();
    vb0 = engine->getVBucket(Vbid(0));
    EXPECT_TRUE(vb0) << "Failed to get valid VBucket object for id 0";
}

void StreamTest::TearDown() {
    if (producer) {
        producer->cancelCheckpointCreatorTask();
    }
    // Destroy various engine objects
    vb0.reset();
    stream.reset();
    producer.reset();
    DCPTest::TearDown();
}

/*
 * Test that when have a producer with IncludeValue and IncludeXattrs both set
 * to No an active stream created via a streamRequest returns true for
 * isKeyOnly.
 */
TEST_P(StreamTest, test_streamIsKeyOnlyTrue) {
    setup_dcp_stream(0, IncludeValue::No, IncludeXattrs::No);
    ASSERT_EQ(ENGINE_SUCCESS, doStreamRequest(*producer).status)
            << "stream request did not return ENGINE_SUCCESS";

    auto activeStream = std::dynamic_pointer_cast<ActiveStream>(
            producer->findStream(Vbid(0)));
    ASSERT_NE(nullptr, activeStream);
    EXPECT_TRUE(activeStream->isKeyOnly());
    destroy_dcp_stream();
}

// Test the compression control error case
TEST_P(StreamTest, validate_compression_control_message_denied) {
    setup_dcp_stream();
    std::string compressCtrlMsg("force_value_compression");
    std::string compressCtrlValue("true");
    EXPECT_FALSE(producer->isCompressionEnabled());

    // Sending a control message without actually enabling SNAPPY must fail
    EXPECT_EQ(ENGINE_EINVAL,
              producer->control(0, compressCtrlMsg, compressCtrlValue));
    destroy_dcp_stream();
}

/*
 * Test to verify the number of items, total bytes sent and total data size
 * by the producer when DCP compression is enabled
 */
TEST_P(StreamTest, test_verifyProducerCompressionStats) {
    VBucketPtr vb = engine->getKVBucket()->getVBucket(vbid);
    setup_dcp_stream();
    std::string compressibleValue(
            "{\"product\": \"car\",\"price\": \"100\"},"
            "{\"product\": \"bus\",\"price\": \"1000\"},"
            "{\"product\": \"Train\",\"price\": \"100000\"}");
    std::string regularValue("{\"product\": \"car\",\"price\": \"100\"}");

    std::string compressCtrlMsg("force_value_compression");
    std::string compressCtrlValue("true");

    mock_set_datatype_support(producer->getCookie(),
                              PROTOCOL_BINARY_DATATYPE_SNAPPY);

    ASSERT_EQ(ENGINE_SUCCESS,
              producer->control(0, compressCtrlMsg, compressCtrlValue));
    ASSERT_TRUE(producer->isForceValueCompressionEnabled());

    store_item(vbid, "key1", compressibleValue.c_str());
    store_item(vbid, "key2", regularValue.c_str());
    store_item(vbid, "key3", compressibleValue.c_str());

    MockDcpMessageProducers producers(engine);

    ASSERT_EQ(ENGINE_SUCCESS, doStreamRequest(*producer).status);

    prepareCheckpointItemsForStep(producers, *producer, *vb);

    /* Stream the snapshot marker first */
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(0, producer->getItemsSent());

    uint64_t totalBytesSent = producer->getTotalBytesSent();
    uint64_t totalUncompressedDataSize =
            producer->getTotalUncompressedDataSize();
    EXPECT_GT(totalBytesSent, 0);
    EXPECT_GT(totalUncompressedDataSize, 0);

    /* Stream the first mutation. This should increment the
     * number of items, total bytes sent and total data size.
     * Since this is a compressible document, the total bytes
     * sent should be incremented by a lesser value than the
     * total data size.
     */
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(1, producer->getItemsSent());
    EXPECT_GT(producer->getTotalBytesSent(), totalBytesSent);
    EXPECT_GT(producer->getTotalUncompressedDataSize(),
              totalUncompressedDataSize);
    EXPECT_LT(producer->getTotalBytesSent() - totalBytesSent,
              producer->getTotalUncompressedDataSize() -
                      totalUncompressedDataSize);

    totalBytesSent = producer->getTotalBytesSent();
    totalUncompressedDataSize = producer->getTotalUncompressedDataSize();

    /*
     * Now stream the second mutation. This should increment the
     * number of items and the total bytes sent. In this case,
     * the total data size should be incremented by exactly the
     * same amount as the total bytes sent
     */
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(2, producer->getItemsSent());
    EXPECT_GT(producer->getTotalBytesSent(), totalBytesSent);
    EXPECT_GT(producer->getTotalUncompressedDataSize(),
              totalUncompressedDataSize);
    EXPECT_EQ(producer->getTotalBytesSent() - totalBytesSent,
              producer->getTotalUncompressedDataSize() -
                      totalUncompressedDataSize);

    totalBytesSent = producer->getTotalBytesSent();
    totalUncompressedDataSize = producer->getTotalUncompressedDataSize();

    /*
     * Disable value compression on the producer side and stream a
     * compressible document. This should result in an increase in
     * total bytes. Even though the document is compressible, the
     * total data size and the total bytes sent would be incremented
     * by exactly the same amount
     */
    compressCtrlValue.assign("false");
    ASSERT_EQ(ENGINE_SUCCESS,
              producer->control(0, compressCtrlMsg, compressCtrlValue));
    mock_set_datatype_support(producer->getCookie(), PROTOCOL_BINARY_RAW_BYTES);

    ASSERT_FALSE(producer->isCompressionEnabled());
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(3, producer->getItemsSent());
    EXPECT_GT(producer->getTotalBytesSent(), totalBytesSent);
    EXPECT_GT(producer->getTotalUncompressedDataSize(),
              totalUncompressedDataSize);
    EXPECT_EQ(producer->getTotalBytesSent() - totalBytesSent,
              producer->getTotalUncompressedDataSize() -
                      totalUncompressedDataSize);

    destroy_dcp_stream();
}

/*
 * Test to verify the number of items and the total bytes sent
 * by the producer under normal and error conditions
 */
TEST_P(StreamTest, test_verifyProducerStats) {
    VBucketPtr vb = engine->getKVBucket()->getVBucket(vbid);
    setup_dcp_stream(0, IncludeValue::No, IncludeXattrs::No);
    store_item(vbid, "key1", "value1");
    store_item(vbid, "key2", "value2");

    MockDcpMessageProducers producers(engine);

    EXPECT_EQ(ENGINE_SUCCESS, doStreamRequest(*producer).status);

    prepareCheckpointItemsForStep(producers, *producer, *vb);

    /* Stream the snapshot marker first */
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(0, producer->getItemsSent());

    uint64_t totalBytes = producer->getTotalBytesSent();
    EXPECT_GT(totalBytes, 0);

    /* Stream the first mutation. This should increment the
     * number of items and the total bytes sent.
     */
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(1, producer->getItemsSent());
    EXPECT_GT(producer->getTotalBytesSent(), totalBytes);
    totalBytes = producer->getTotalBytesSent();

    /* Now simulate a failure while trying to stream the next
     * mutation.
     */
    producers.setMutationStatus(ENGINE_E2BIG);

    EXPECT_EQ(ENGINE_E2BIG, producer->step(&producers));

    /* The number of items total bytes sent should remain the same */
    EXPECT_EQ(1, producer->getItemsSent());
    EXPECT_EQ(producer->getTotalBytesSent(), totalBytes);
    totalBytes = producer->getTotalBytesSent();

    /* Now stream the mutation again and the stats should have incremented */
    producers.setMutationStatus(ENGINE_SUCCESS);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(2, producer->getItemsSent());
    EXPECT_GT(producer->getTotalBytesSent(), totalBytes);

    destroy_dcp_stream();
}

/*
 * Test that when have a producer with IncludeValue set to Yes and IncludeXattrs
 * set to No an active stream created via a streamRequest returns false for
 * isKeyOnly.
 */
TEST_P(StreamTest, test_streamIsKeyOnlyFalseBecauseOfIncludeValue) {
    setup_dcp_stream(0, IncludeValue::Yes, IncludeXattrs::No);
    ASSERT_EQ(ENGINE_SUCCESS, doStreamRequest(*producer).status)
            << "stream request did not return ENGINE_SUCCESS";

    auto activeStream = std::dynamic_pointer_cast<ActiveStream>(
            producer->findStream(Vbid(0)));
    ASSERT_NE(nullptr, activeStream);
    EXPECT_FALSE(activeStream->isKeyOnly());
    destroy_dcp_stream();
}

/*
 * Test that when have a producer with IncludeValue set to No and IncludeXattrs
 * set to Yes an active stream created via a streamRequest returns false for
 * isKeyOnly.
 */
TEST_P(StreamTest, test_streamIsKeyOnlyFalseBecauseOfIncludeXattrs) {
    setup_dcp_stream(0, IncludeValue::No, IncludeXattrs::Yes);
    ASSERT_EQ(ENGINE_SUCCESS, doStreamRequest(*producer).status)
            << "stream request did not return ENGINE_SUCCESS";

    auto activeStream = std::dynamic_pointer_cast<ActiveStream>(
            producer->findStream(Vbid(0)));
    ASSERT_NE(nullptr, activeStream);
    EXPECT_FALSE(activeStream->isKeyOnly());
    destroy_dcp_stream();
}

/*
 * Test for a dcpResponse retrieved from a stream where IncludeValue and
 * IncludeXattrs are both No, that the message size does not include the size of
 * the body.
 */
TEST_P(StreamTest, test_keyOnlyMessageSize) {
    auto item = makeItemWithXattrs();
    auto keyOnlyMessageSize =
            MutationResponse::mutationBaseMsgBytes +
            item->getKey().makeDocKeyWithoutCollectionID().size();
    queued_item qi(std::move(item));

    setup_dcp_stream(0, IncludeValue::No, IncludeXattrs::No);
    std::unique_ptr<DcpResponse> dcpResponse =
            stream->public_makeResponseFromItem(qi);

    /**
     * Create a DCP response and check that a new item is created
     */
    auto mutProdResponse = dynamic_cast<MutationResponse*>(dcpResponse.get());
    ASSERT_NE(qi.get(), mutProdResponse->getItem().get());

    EXPECT_EQ(keyOnlyMessageSize, dcpResponse->getMessageSize());
    destroy_dcp_stream();
}

/*
 * Test for a dcpResponse retrieved from a stream where
 * IncludeValue==NoWithUnderlyingDatatype and IncludeXattrs==No, that the
 * message size does not include the size of the body.
 */
TEST_P(StreamTest, test_keyOnlyMessageSizeUnderlyingDatatype) {
    auto item = makeItemWithXattrs();
    auto keyOnlyMessageSize =
            MutationResponse::mutationBaseMsgBytes +
            item->getKey().makeDocKeyWithoutCollectionID().size();
    queued_item qi(std::move(item));

    setup_dcp_stream(
            0, IncludeValue::NoWithUnderlyingDatatype, IncludeXattrs::No);
    std::unique_ptr<DcpResponse> dcpResponse =
            stream->public_makeResponseFromItem(qi);

    /**
     * Create a DCP response and check that a new item is created
     */
    auto mutProdResponse = dynamic_cast<MutationResponse*>(dcpResponse.get());
    ASSERT_NE(qi.get(), mutProdResponse->getItem().get());

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
    auto keyAndValueMessageSize =
            MutationResponse::mutationBaseMsgBytes +
            item->getKey().makeDocKeyWithoutCollectionID().size() +
            item->getNBytes();
    queued_item qi(std::move(item));

    setup_dcp_stream(0, IncludeValue::Yes, IncludeXattrs::Yes);
    std::unique_ptr<DcpResponse> dcpResponse =
            stream->public_makeResponseFromItem(qi);

    /**
     * Create a DCP response and check that a new item is not created
     */
    auto mutProdResponse = dynamic_cast<MutationResponse*>(dcpResponse.get());
    ASSERT_EQ(qi.get(), mutProdResponse->getItem().get());
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
    auto keyAndValueMessageSize =
            MutationResponse::mutationBaseMsgBytes +
            item->getKey().makeDocKeyWithoutCollectionID().size() +
            item->getNBytes();
    queued_item qi(std::move(item));

    setup_dcp_stream(0, IncludeValue::Yes, IncludeXattrs::Yes);
    std::unique_ptr<DcpResponse> dcpResponse =
            stream->public_makeResponseFromItem(qi);

    /**
     * Create a DCP response and check that a new item is not created
     */
    auto mutProdResponse = dynamic_cast<MutationResponse*>(dcpResponse.get());
    ASSERT_EQ(qi.get(), mutProdResponse->getItem().get());
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
    cb::byte_buffer buffer{(uint8_t*)root, item->getValue()->valueSize()};
    auto sz = cb::xattr::get_body_offset(
            {reinterpret_cast<char*>(buffer.buf), buffer.len});
    auto keyAndValueMessageSize =
            MutationResponse::mutationBaseMsgBytes +
            item->getKey().makeDocKeyWithoutCollectionID().size() +
            item->getNBytes() - sz;
    queued_item qi(std::move(item));

    setup_dcp_stream(0, IncludeValue::Yes, IncludeXattrs::No);
    std::unique_ptr<DcpResponse> dcpResponse =
            stream->public_makeResponseFromItem(qi);

    /**
     * Create a DCP response and check that a new item is created
     */
    auto mutProdResponse = dynamic_cast<MutationResponse*>(dcpResponse.get());
    ASSERT_NE(qi.get(), mutProdResponse->getItem().get());
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
    auto keyAndValueMessageSize =
            MutationResponse::mutationBaseMsgBytes +
            item->getKey().makeDocKeyWithoutCollectionID().size() +
            item->getNBytes();
    queued_item qi(std::move(item));

    setup_dcp_stream(0, IncludeValue::Yes, IncludeXattrs::No);
    std::unique_ptr<DcpResponse> dcpResponse =
            stream->public_makeResponseFromItem(qi);
    /**
     * Create a DCP response and check that a new item is not created
     */
    auto mutProdResponse = dynamic_cast<MutationResponse*>(dcpResponse.get());
    ASSERT_EQ(qi.get(), mutProdResponse->getItem().get());
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
    cb::byte_buffer buffer{(uint8_t*)root, item->getValue()->valueSize()};
    auto sz = cb::xattr::get_body_offset(
            {reinterpret_cast<char*>(buffer.buf), buffer.len});
    auto keyAndValueMessageSize =
            MutationResponse::mutationBaseMsgBytes +
            item->getKey().makeDocKeyWithoutCollectionID().size() + sz;
    queued_item qi(std::move(item));

    setup_dcp_stream(0, IncludeValue::No, IncludeXattrs::Yes);
    std::unique_ptr<DcpResponse> dcpResponse =
            stream->public_makeResponseFromItem(qi);

    /**
     * Create a DCP response and check that a new item is created
     */
    auto mutProdResponse = dynamic_cast<MutationResponse*>(dcpResponse.get());
    ASSERT_NE(qi.get(), mutProdResponse->getItem().get());
    EXPECT_EQ(keyAndValueMessageSize, dcpResponse->getMessageSize());
    destroy_dcp_stream();
}

/*
 * Test for a dcpResponse retrieved from a stream where IncludeValue is
 * NoWithUnderlyingDatatype and IncludeXattrs is Yes, that the message size
 * includes the size of only the xattrs (excluding the value), and the
 * datatype is the same as the original tiem.
 */
TEST_P(StreamTest, test_keyAndValueExcludingValueWithDatatype) {
    auto item = makeItemWithXattrs();
    auto root = const_cast<char*>(item->getData());
    cb::byte_buffer buffer{(uint8_t*)root, item->getValue()->valueSize()};
    auto sz = cb::xattr::get_body_offset(
            {reinterpret_cast<char*>(buffer.buf), buffer.len});
    auto keyAndValueMessageSize =
            MutationResponse::mutationBaseMsgBytes +
            item->getKey().makeDocKeyWithoutCollectionID().size() + sz;
    queued_item qi(std::move(item));

    setup_dcp_stream(
            0, IncludeValue::NoWithUnderlyingDatatype, IncludeXattrs::Yes);
    std::unique_ptr<DcpResponse> dcpResponse =
            stream->public_makeResponseFromItem(qi);

    /**
     * Create a DCP response and check that a new item is created
     */
    auto mutProdResponse = dynamic_cast<MutationResponse*>(dcpResponse.get());
    auto& responseItem = mutProdResponse->getItem();
    EXPECT_EQ(qi->getDataType(), responseItem->getDataType());
    EXPECT_EQ(keyAndValueMessageSize, dcpResponse->getMessageSize());
    destroy_dcp_stream();
}

/*
 * Test for a dcpResponse without XATTRS retrieved from a stream where
 * IncludeValue is NoWithUnderlyingDatatype and IncludeXattrs is Yes, that the
 * message size includes the size of only the key (excluding the value &
 * XATTRs), and the datatype is the same as the original item.
 */
TEST_P(StreamTest, test_keyAndValueWithoutXattrExcludingValueWithDatatype) {
    auto item = makeItemWithoutXattrs();
    auto root = const_cast<char*>(item->getData());
    cb::byte_buffer buffer{(uint8_t*)root, item->getValue()->valueSize()};
    auto keyAndValueMessageSize =
            MutationResponse::mutationBaseMsgBytes +
            item->getKey().makeDocKeyWithoutCollectionID().size();
    queued_item qi(std::move(item));

    setup_dcp_stream(
            0, IncludeValue::NoWithUnderlyingDatatype, IncludeXattrs::Yes);
    std::unique_ptr<DcpResponse> dcpResponse =
            stream->public_makeResponseFromItem(qi);

    /**
     * Create a DCP response and check that a new item is created
     */
    auto mutProdResponse = dynamic_cast<MutationResponse*>(dcpResponse.get());
    auto& responseItem = mutProdResponse->getItem();
    EXPECT_EQ(qi->getDataType(), responseItem->getDataType());
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

        auto evb = std::shared_ptr<EphemeralVBucket>(
                std::dynamic_pointer_cast<EphemeralVBucket>(vb0));
        auto dcpbfm = DCPBackfillMemory(evb, stream, 1, 1);
        dcpbfm.run();
        destroy_dcp_stream();
    }
}

TEST_P(StreamTest, bufferedMemoryBackfillPurgeGreaterThanStart) {
    if (engine->getConfiguration().getBucketType() == "ephemeral") {
        setup_dcp_stream(0, IncludeValue::No, IncludeXattrs::No);
        auto evb = std::shared_ptr<EphemeralVBucket>(
                std::dynamic_pointer_cast<EphemeralVBucket>(vb0));

        // Force the purgeSeqno because it's easier than creating and
        // deleting items
        evb->setPurgeSeqno(3);

        // Backfill with start != 1 and start != end and start < purge
        DCPBackfillMemoryBuffered dcpbfm(evb, stream, 2, 4);
        dcpbfm.run();
        EXPECT_TRUE(stream->isDead());
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
    EXPECT_TRUE(stream->public_nextCheckpointItem())
            << "nextCheckpointItem() should initially be true.";

    // Get the set of outstanding items
    auto items = stream->public_getOutstandingItems(*vb0);

    // REGRESSION CHECK: nextCheckpointItem() should still return true
    EXPECT_TRUE(stream->public_nextCheckpointItem())
            << "nextCheckpointItem() after getting outstanding items should be "
               "true.";

    // Process the set of items
    stream->public_processItems(items);

    // Should finish with nextCheckpointItem() returning false.
    EXPECT_FALSE(stream->public_nextCheckpointItem())
            << "nextCheckpointItem() after processing items should be false.";
    destroy_dcp_stream();
}

// Check that the items remaining statistic is accurate and is unaffected
// by de-duplication.
TEST_P(StreamTest, MB17653_ItemsRemaining) {
    auto& manager =
            *(engine->getKVBucket()->getVBucket(vbid)->checkpointManager);

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
    EXPECT_EQ(1, stream->getItemsRemaining())
            << "Unexpected initial stream item count";

    // Populate the streams' ready queue with items from the checkpoint,
    // advancing the streams' cursor. Should result in no change in items
    // remaining (they still haven't been send out of the stream).
    stream->nextCheckpointItemTask();
    EXPECT_EQ(1, stream->getItemsRemaining())
            << "Mismatch after moving items to ready queue";

    // Add another mutation. As we have already iterated over all checkpoint
    // items and put into the streams' ready queue, de-duplication of this new
    // mutation (from the point of view of the stream) isn't possible, so items
    // remaining should increase by one.
    store_item(vbid, "key", "value");
    EXPECT_EQ(2, stream->getItemsRemaining())
            << "Mismatch after populating readyQ and storing 1 more item";

    // Now actually drain the items from the readyQ and see how many we
    // received, excluding meta items. This will result in all but one of the
    // checkpoint items (the one we added just above) being drained.
    std::unique_ptr<DcpResponse> response(stream->public_nextQueuedItem());
    ASSERT_NE(nullptr, response);
    EXPECT_TRUE(response->isMetaEvent()) << "Expected 1st item to be meta";

    response = stream->public_nextQueuedItem();
    ASSERT_NE(nullptr, response);
    EXPECT_FALSE(response->isMetaEvent()) << "Expected 2nd item to be non-meta";

    response = stream->public_nextQueuedItem();
    EXPECT_EQ(nullptr, response) << "Expected there to not be a 3rd item.";

    EXPECT_EQ(1, stream->getItemsRemaining()) << "Expected to have 1 item "
                                                 "remaining (in checkpoint) "
                                                 "after draining readyQ";

    // Add another 10 mutations on a different key. This should only result in
    // us having one more item (not 10) due to de-duplication in
    // checkpoints.
    for (unsigned int ii = 0; ii < set_op_count; ii++) {
        store_item(vbid, "key_2", "value");
    }

    EXPECT_EQ(2, stream->getItemsRemaining())
            << "Expected two items after adding 1 more to existing checkpoint";

    // Copy items into readyQ a second time, and drain readyQ so we should
    // have no items left.
    stream->nextCheckpointItemTask();
    do {
        response = stream->public_nextQueuedItem();
    } while (response);
    EXPECT_EQ(0, stream->getItemsRemaining()) << "Should have 0 items "
                                                 "remaining after advancing "
                                                 "cursor and draining readyQ";
    destroy_dcp_stream();
}

/* Stream items from a DCP backfill */
TEST_P(StreamTest, BackfillOnly) {
    /* Add 3 items */
    const size_t numItems = 3;
    addItemsAndRemoveCheckpoint(numItems);

    /* Set up a DCP stream for the backfill */
    setup_dcp_stream();

    /* We want the backfill task to run in a background thread */
    ExecutorPool::get()->setNumAuxIO(1);
    stream->transitionStateToBackfilling();

    // MB-27199: Just stir things up by doing some front-end ops whilst
    // backfilling. This would trigger a number of TSAN warnings
    std::thread thr([this]() {
        int i = 0;
        while (i < 100) {
            engine->getAndTouchInner(cookie, makeStoredDocKey("key1"), vbid, i);
            i++;
        }
    });

    // Ensure all GATs are done before evaluating the stream below
    thr.join();

    // Wait for the backfill task to have pushed all items to the Stream::readyQ
    // Note: we expect 1 SnapshotMarker + numItems in the readyQ
    // Note: we need to access the readyQ under streamLock while the backfill
    //     task is running
    std::chrono::microseconds uSleepTime(128);
    while (stream->public_readyQSize() < numItems + 1) {
        uSleepTime = decayingSleep(uSleepTime);
    }

    // Check the content of readyQ
    auto front = stream->public_nextQueuedItem();
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, front->getEvent());
    auto snapMarker = dynamic_cast<SnapshotMarker&>(*front);
    while (stream->public_readyQSize() > 0) {
        auto item = stream->public_nextQueuedItem();
        EXPECT_EQ(DcpResponse::Event::Mutation, item->getEvent());
        auto seqno = item->getBySeqno().get();
        EXPECT_GE(seqno, snapMarker.getStartSeqno());
        EXPECT_LE(seqno, snapMarker.getEndSeqno());
    }

    // Check that backfill stats have been updated correctly
    EXPECT_EQ(numItems, stream->getNumBackfillItems());
    EXPECT_EQ(numItems, stream->getNumBackfillItemsRemaining());

    destroy_dcp_stream();
}

/* Negative test case that checks whether the stream gracefully goes to
   'dead' state upon disk backfill failure */
TEST_P(StreamTest, DiskBackfillFail) {
    if (bucketType == "ephemeral") {
        /* Ephemeral buckets don't do disk backfill */
        return;
    }

    /* Add 3 items */
    int numItems = 3;
    addItemsAndRemoveCheckpoint(numItems);

    /* Delete the vb file so that the backfill would fail */
    engine->getKVBucket()->getRWUnderlying(vbid)->delVBucket(vbid,
                                                             /* file rev */ 1);

    /* Set up a DCP stream for the backfill */
    setup_dcp_stream();

    /* Run the backfill task in a background thread */
    ExecutorPool::get()->setNumAuxIO(1);

    /* Wait for the backfill task to fail and stream to transition to dead
       state */
    {
        std::chrono::microseconds uSleepTime(128);
        while (stream->isActive()) {
            uSleepTime = decayingSleep(uSleepTime);
        }
    }

    destroy_dcp_stream();
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

    /* set the DCP backfill buffer size to a value that is smaller than the
       size of a mutation */
    producer->setBackfillBufferSize(1);

    /* We want the backfill task to run in a background thread */
    ExecutorPool::get()->setNumAuxIO(1);
    stream->transitionStateToBackfilling();

    /* Backfill can only read 1 as its buffer will become full after that */
    {
        std::chrono::microseconds uSleepTime(128);
        while ((numItems - 1) != stream->getLastReadSeqno()) {
            uSleepTime = decayingSleep(uSleepTime);
        }
    }

    /* Consume the backfill item(s) */
    stream->consumeBackfillItems(/*snapshot*/ 1 + /*mutation*/ 1);

    /* We should see that buffer full status must be false as we have read
       the item in the backfill buffer */
    EXPECT_FALSE(producer->getBackfillBufferFullStatus());

    /* Finish up with the backilling of the remaining item */
    {
        std::chrono::microseconds uSleepTime(128);
        while (numItems != stream->getLastReadSeqno()) {
            uSleepTime = decayingSleep(uSleepTime);
        }
    }

    /* Read the other item */
    stream->consumeBackfillItems(1);
    destroy_dcp_stream();
}

/* Checks that DCP backfill in Ephemeral buckets does not have duplicates in
 a snaphsot */
TEST_P(StreamTest, EphemeralBackfillSnapshotHasNoDuplicates) {
    if (bucketType != "ephemeral") {
        return;
    }
    EphemeralVBucket* evb = dynamic_cast<EphemeralVBucket*>(vb0.get());

    /* Add 4 items */
    const int numItems = 4;
    for (int i = 0; i < numItems; ++i) {
        std::string key("key" + std::to_string(i));
        store_item(vbid, key, "value");
    }

    /* Update "key1" before range read cursors are on vb */
    store_item(vbid, "key1", "value1");

    /* Add fake range read cursor on vb and update items */
    {
        auto itr = evb->makeRangeIterator(/*isBackfill*/ true);
        /* update 'key2' and 'key3' */
        store_item(vbid, "key2", "value1");
        store_item(vbid, "key3", "value1");
    }

    /* update key2 once again with a range iterator again so that it has 2 stale
     values */
    {
        auto itr = evb->makeRangeIterator(/*isBackfill*/ true);
        /* update 'key2' */
        store_item(vbid, "key2", "value1");
    }

    removeCheckpoint(numItems);

    /* Set up a DCP stream for the backfill */
    setup_dcp_stream();

    /* We want the backfill task to run in a background thread */
    ExecutorPool::get()->setNumAuxIO(1);
    stream->transitionStateToBackfilling();

    /* Wait for the backfill task to complete */
    {
        std::chrono::microseconds uSleepTime(128);
        uint64_t expLastReadSeqno = 4 /*numItems*/ + 4 /*num updates*/;
        while (expLastReadSeqno !=
               static_cast<uint64_t>(stream->getLastReadSeqno())) {
            uSleepTime = decayingSleep(uSleepTime);
        }
    }

    /* Verify that only 4 items are read in the backfill (no duplicates) */
    EXPECT_EQ(numItems, stream->getNumBackfillItems());

    destroy_dcp_stream();
}

TEST_P(StreamTest, CursorDroppingBasicBackfillState) {
    /* Add 2 items; we need this to keep stream in backfill state */
    const int numItems = 2;
    addItemsAndRemoveCheckpoint(numItems);

    /* Set up a DCP stream */
    setup_dcp_stream();

    /* Transition stream to backfill state and expect cursor dropping call to
       succeed */
    stream->transitionStateToBackfilling();
    EXPECT_TRUE(stream->public_handleSlowStream());

    /* Run the backfill task in background thread to run so that it can
       complete/cancel itself */
    ExecutorPool::get()->setNumAuxIO(1);
    /* Finish up with the backilling of the remaining item */
    {
        std::chrono::microseconds uSleepTime(128);
        while (numItems != stream->getLastReadSeqno()) {
            uSleepTime = decayingSleep(uSleepTime);
        }
    }
    destroy_dcp_stream();
}

/*
 * Tests that when a cursor is dropped the associated stream's pointer
 * to the cursor is set to nullptr.
 */
TEST_P(StreamTest, MB_32329CursorDroppingResetCursor) {
    /* Add 2 items; we need this to keep stream in backfill state */
    const int numItems = 2;
    addItemsAndRemoveCheckpoint(numItems);

    /* Set up a DCP stream */
    setup_dcp_stream();

    /* Transition stream to backfill state and expect cursor dropping call to
       succeed */
    stream->transitionStateToBackfilling();

    /*
     * Increase the use_count of the cursor shared pointer, this replicates
     * the behaviour of the ClosedUnrefCheckpointRemoverTask (see
     * cursorDroppingIfNeeded) which calls lock() on the cursor before
     * calling DcpConnMap::handleSlowStream.
     */
    auto cursorSP = stream->getCursor().lock();
    /*
     * The cursor shared_ptr has a reference count of 2. One is from the
     * reference from the cursor map, the other is the reference from taking
     * the lock (in the code above).
     */
    ASSERT_EQ(2, cursorSP.use_count());

    ASSERT_TRUE(stream->public_handleSlowStream());
    /*
     * The cursor should now be removed from the map and therefore the
     * reference count should have reduced to 1.
     */
    ASSERT_EQ(1, cursorSP.use_count());

    /*
     * Key part of the test to check that even though the cursor has a
     * reference count of 1, the dcp stream's pointer to the cursor has
     * now been set to nullptr, as it has been removed from the cursor map.
     */
    EXPECT_EQ(nullptr, stream->getCursor().lock());

    /* Run the backfill task in background thread to run so that it can
       complete/cancel itself */
    ExecutorPool::get()->setNumAuxIO(1);
    /* Finish up with the backilling of the remaining item */
    {
        std::chrono::microseconds uSleepTime(128);
        while (numItems != stream->getLastReadSeqno()) {
            uSleepTime = decayingSleep(uSleepTime);
        }
    }
    destroy_dcp_stream();
}

TEST_P(StreamTest, CursorDroppingBasicInMemoryState) {
    /* Set up a DCP stream */
    setup_dcp_stream();

    /* Transition stream to in-memory state and expect cursor dropping call to
       succeed */
    EXPECT_TRUE(stream->public_handleSlowStream());
    destroy_dcp_stream();
}

TEST_P(StreamTest, CursorDroppingBasicNotAllowedStates) {
    /* Set up a DCP stream */
    setup_dcp_stream(DCP_ADD_STREAM_FLAG_TAKEOVER);

    /* Transition stream to takeoverSend state and expect cursor dropping call
       to fail */
    stream->transitionStateToTakeoverSend();
    EXPECT_FALSE(stream->public_handleSlowStream());

    /* Transition stream to takeoverWait state and expect cursor dropping call
       to fail */
    stream->transitionStateToTakeoverWait();
    EXPECT_FALSE(stream->public_handleSlowStream());

    /* Transition stream to takeoverSend state and expect cursor dropping call
       to fail */
    stream->transitionStateToTakeoverDead();
    EXPECT_FALSE(stream->public_handleSlowStream());
    destroy_dcp_stream();
}

TEST_P(StreamTest, RollbackDueToPurge) {
    setup_dcp_stream(0, IncludeValue::No, IncludeXattrs::No);

    /* Store 4 items */
    const int numItems = 4;
    for (int i = 0; i <= numItems; ++i) {
        store_item(vbid, std::string("key" + std::to_string(i)), "value");
    }
    uint64_t vbUuid = vb0->failovers->getLatestUUID();
    auto result = doStreamRequest(*producer,
                                  numItems - 2,
                                  numItems,
                                  numItems - 2,
                                  numItems - 2,
                                  vbUuid);
    EXPECT_EQ(ENGINE_SUCCESS, result.status);
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->closeStream(/*opaque*/ 0, vb0->getId()));

    /* Set a start_seqno > purge_seqno > snap_start_seqno */
    engine->getKVBucket()->getLockedVBucket(vbid)->setPurgeSeqno(numItems - 3);

    /* We don't expect a rollback for this */
    result = doStreamRequest(
            *producer, numItems - 2, numItems, 0, numItems - 2, vbUuid);
    EXPECT_EQ(ENGINE_SUCCESS, result.status);
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->closeStream(/*opaque*/ 0, vb0->getId()));

    /* Set a purge_seqno > start_seqno */
    engine->getKVBucket()->getLockedVBucket(vbid)->setPurgeSeqno(numItems - 1);

    /* Now we expect a rollback to 0 */
    result = doStreamRequest(
            *producer, numItems - 2, numItems, 0, numItems - 2, vbUuid);
    EXPECT_EQ(ENGINE_ROLLBACK, result.status);
    EXPECT_EQ(0, result.rollbackSeqno);
    destroy_dcp_stream();
}

/*
 * Test to ensure that when a streamRequest is made to a dead vbucket, we
 * (1) return not my vbucket.
 * (2) do not invoke the callback function (which is passed as parameter).
 * The reason we don't want to invoke the callback function is that it will
 * invoke mcbp_response_handler and so generate a response (ENGINE_SUCCESS) and
 * then when we continue the execution of the streamRequest function we generate
 * a second response (ENGINE_NOT_MY_VBUCKET).
 */
TEST_P(StreamTest, MB_25820_callback_not_invoked_on_dead_vb_stream_request) {
    setup_dcp_stream(0, IncludeValue::No, IncludeXattrs::No);
    ASSERT_EQ(ENGINE_SUCCESS,
              engine->getKVBucket()->setVBucketState(
                      vbid, vbucket_state_dead, true));
    uint64_t vbUuid = vb0->failovers->getLatestUUID();
    // Given the vbucket state is dead we should return not my vbucket.
    EXPECT_EQ(ENGINE_NOT_MY_VBUCKET,
              doStreamRequest(*producer, 0, 0, 0, 0, vbUuid).status);
    // The callback function past to streamRequest should not be invoked.
    ASSERT_EQ(0, callbackCount);
}

// Test the compression control success case
TEST_P(StreamTest, validate_compression_control_message_allowed) {
    // For success enable the snappy datatype on the connection
    mock_set_datatype_support(cookie, PROTOCOL_BINARY_DATATYPE_SNAPPY);
    setup_dcp_stream();
    std::string compressCtrlMsg("force_value_compression");
    std::string compressCtrlValue("true");
    EXPECT_TRUE(producer->isCompressionEnabled());

    // Sending a control message after enabling SNAPPY should succeed
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->control(0, compressCtrlMsg, compressCtrlValue));
    destroy_dcp_stream();
}

class CacheCallbackTest : public StreamTest {
protected:
    void SetUp() override {
        StreamTest::SetUp();
        store_item(vbid, key, "value");

        /* Create new checkpoint so that we can remove the current checkpoint
         * and force a backfill in the DCP stream */
        CheckpointManager& ckpt_mgr = *vb0->checkpointManager;
        ckpt_mgr.createNewCheckpoint();

        /* Wait for removal of the old checkpoint, this also would imply that
         * the items are persisted (in case of persistent buckets) */
        {
            bool new_ckpt_created;
            std::chrono::microseconds uSleepTime(128);
            while (numItems != ckpt_mgr.removeClosedUnrefCheckpoints(
                                       *vb0, new_ckpt_created)) {
                uSleepTime = decayingSleep(uSleepTime);
            }
        }

        /* Set up a DCP stream for the backfill */
        setup_dcp_stream();
    }

    void TearDown() override {
        producer->closeAllStreams();
        StreamTest::TearDown();
    }

    const size_t numItems = 1;
    const std::string key = "key";
    const DocKey docKey{key, DocKeyEncodesCollectionId::No};
};

/*
 * Tests the callback member function of the CacheCallback class.  This
 * particular test should result in the CacheCallback having a status of
 * ENGINE_KEY_EEXISTS.
 */
TEST_P(CacheCallbackTest, CacheCallback_key_eexists) {
    CacheCallback callback(*engine, stream);

    stream->transitionStateToBackfilling();
    auto collectionsReadHandle = vb0->lockCollections(docKey);
    CacheLookup lookup(docKey, /*BySeqno*/ 1, vbid, collectionsReadHandle);
    callback.callback(lookup);

    /* Invoking callback should result in backfillReceived being called on
     * activeStream, which should return true and hence set the callback status
     * to ENGINE_KEY_EEXISTS.
     */
    EXPECT_EQ(ENGINE_KEY_EEXISTS, callback.getStatus());

    /* Verify that the item is read in the backfill */
    EXPECT_EQ(numItems, stream->getNumBackfillItems());

    /* Verify have the backfill item sitting in the readyQ */
    EXPECT_EQ(numItems, stream->public_readyQ().size());
}

/*
 * Tests the callback member function of the CacheCallback class.  This
 * particular test should result in the CacheCallback having a status of
 * ENGINE_SUCCESS.
 */
TEST_P(CacheCallbackTest, CacheCallback_engine_success) {
    CacheCallback callback(*engine, stream);

    stream->transitionStateToBackfilling();
    // Passing in wrong BySeqno - should be 1, but passing in 0
    auto collectionsReadHandle = vb0->lockCollections(docKey);
    CacheLookup lookup(docKey, /*BySeqno*/ 0, vbid, collectionsReadHandle);
    callback.callback(lookup);

    /* Invoking callback should result in backfillReceived NOT being called on
     * activeStream, and hence the callback status should be set to
     * ENGINE_SUCCESS.
     */
    EXPECT_EQ(ENGINE_SUCCESS, callback.getStatus());

    /* Verify that the item is not read in the backfill */
    EXPECT_EQ(0, stream->getNumBackfillItems());

    /* Verify do not have the backfill item sitting in the readyQ */
    EXPECT_EQ(0, stream->public_readyQ().size());
}

/*
 * Tests the callback member function of the CacheCallback class.  Due to the
 * key being evicted the test should result in the CacheCallback having a status
 * of ENGINE_SUCCESS.
 */
TEST_P(CacheCallbackTest, CacheCallback_engine_success_not_resident) {
    if (bucketType == "ephemeral") {
        /* The test relies on being able to evict a key from memory.
         * Eviction is not supported with empherial buckets.
         */
        return;
    }
    CacheCallback callback(*engine, stream);

    stream->transitionStateToBackfilling();
    auto collectionsReadHandle = vb0->lockCollections(docKey);
    CacheLookup lookup(docKey, /*BySeqno*/ 1, vbid, collectionsReadHandle);
    // Make the key non-resident by evicting the key
    const char* msg;
    engine->getKVBucket()->evictKey(docKey, vbid, &msg);
    callback.callback(lookup);

    /* With the key evicted, invoking callback should result in backfillReceived
     * NOT being called on activeStream, and hence the callback status should be
     * set to ENGINE_SUCCESS
     */
    EXPECT_EQ(ENGINE_SUCCESS, callback.getStatus());

    /* Verify that the item is not read in the backfill */
    EXPECT_EQ(0, stream->getNumBackfillItems());

    /* Verify do not have the backfill item sitting in the readyQ */
    EXPECT_EQ(0, stream->public_readyQ().size());
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
    producer->setBackfillBufferSize(0);
    producer->bytesForceRead(1);

    CacheCallback callback(*engine, stream);

    stream->transitionStateToBackfilling();
    auto collectionsReadHandle = vb0->lockCollections(docKey);
    CacheLookup lookup(docKey, /*BySeqno*/ 1, vbid, collectionsReadHandle);
    callback.callback(lookup);

    /* Invoking callback should result in backfillReceived being called on
     * activeStream, which should return false (due to
     * DcpProducer::recordBackfillManagerBytesRead returning false), and hence
     * set the callback status to ENGINE_ENOMEM.
     */
    EXPECT_EQ(ENGINE_ENOMEM, callback.getStatus());

    /* Verify that the item is not read in the backfill */
    EXPECT_EQ(0, stream->getNumBackfillItems());

    /* Verify do not have the backfill item sitting in the readyQ */
    EXPECT_EQ(0, stream->public_readyQ().size());
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
