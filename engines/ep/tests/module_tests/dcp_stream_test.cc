/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp_stream_test.h"

#include "checkpoint_manager.h"
#include "checkpoint_utils.h"
#include "collections/collections_test_helpers.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/backfill_disk.h"
#include "dcp/response.h"
#include "dcp_utils.h"
#include "ep_bucket.h"
#include "ep_time.h"
#include "failover-table.h"
#include "kv_bucket.h"
#include "kvstore/couch-kvstore/couch-kvstore-config.h"
#include "kvstore/kvstore.h"
#include "replicationthrottle.h"
#include "test_helpers.h"
#include "test_manifest.h"
#include "tests/test_fileops.h"
#include "thread_gate.h"
#include "vbucket.h"
#include "vbucket_state.h"
#include <executor/executorpool.h>

#include "../couchstore/src/internal.h"
#include "../mock/mock_checkpoint_manager.h"
#include "../mock/mock_dcp.h"
#include "../mock/mock_dcp_conn_map.h"
#include "../mock/mock_dcp_consumer.h"
#include "../mock/mock_dcp_producer.h"
#include "../mock/mock_kvstore.h"
#include "../mock/mock_stream.h"
#include "../mock/mock_synchronous_ep_engine.h"

#include <engines/ep/tests/mock/mock_dcp_backfill_mgr.h>
#include <folly/portability/GMock.h>
#include <programs/engine_testapp/mock_cookie.h>
#include <xattr/blob.h>
#include <xattr/utils.h>
#include <thread>

using FlushResult = EPBucket::FlushResult;
using MoreAvailable = EPBucket::MoreAvailable;

using namespace std::string_view_literals;

using ::testing::ElementsAre;

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
    ASSERT_EQ(cb::engine_errc::success, doStreamRequest(*producer).status)
            << "stream request did not return cb::engine_errc::success";

    auto activeStream = std::dynamic_pointer_cast<ActiveStream>(
            producer->findStream(Vbid(0)));
    ASSERT_NE(nullptr, activeStream);
    EXPECT_TRUE(activeStream->isKeyOnly());
    EXPECT_EQ(cb::engine_errc::success, destroy_dcp_stream());
}

// Test the compression control error case
TEST_P(StreamTest, validate_compression_control_message_denied) {
    setup_dcp_stream();
    std::string compressCtrlMsg("force_value_compression");
    std::string compressCtrlValue("true");
    EXPECT_FALSE(producer->isCompressionEnabled());

    // Sending a control message without actually enabling SNAPPY must fail
    EXPECT_EQ(cb::engine_errc::invalid_arguments,
              producer->control(0, compressCtrlMsg, compressCtrlValue));
    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
}

void StreamTest::setupProducerCompression() {
    VBucketPtr vb = engine->getKVBucket()->getVBucket(vbid);

    std::string compressibleValue(
            "{\"product\": \"car\",\"price\": \"100\"},"
            "{\"product\": \"bus\",\"price\": \"1000\"},"
            "{\"product\": \"Train\",\"price\": \"100000\"}");
    std::string regularValue(R"({"product": "car","price": "100"})");

    store_item(vbid, "key1", compressibleValue.c_str());
    store_item(vbid, "key2", regularValue.c_str());
}

void StreamTest::registerCursorAtCMStart() {
    auto vb = engine->getKVBucket()->getVBucket(vbid);
    ASSERT_TRUE(vb);
    auto& manager = static_cast<CheckpointManager&>(*vb->checkpointManager);
    const auto dcpCursor =
            manager.registerCursorBySeqno(
                           "a cursor", 0, CheckpointCursor::Droppable::Yes)
                    .cursor.lock();
    ASSERT_TRUE(dcpCursor);
}

/*
 * Test to verify the number of items, total bytes sent and total data size
 * by the producer when DCP compression is enabled
 */
TEST_P(StreamTest, test_verifyProducerCompressionStats) {
    setupProducerCompression();

    cookie->setDatatypeSupport(PROTOCOL_BINARY_DATATYPE_SNAPPY);
    setup_dcp_stream();

    ASSERT_EQ(cb::engine_errc::success,
              producer->control(0, "force_value_compression", "true"));
    ASSERT_TRUE(producer->isForceValueCompressionEnabled());

    ASSERT_EQ(cb::engine_errc::success, doStreamRequest(*producer).status);

    MockDcpMessageProducers producers;
    VBucketPtr vb = engine->getKVBucket()->getVBucket(vbid);
    prepareCheckpointItemsForStep(producers, *producer, *vb);

    /* Stream the snapshot marker first */
    EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
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
    EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
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
    EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
    EXPECT_EQ(2, producer->getItemsSent());
    EXPECT_GT(producer->getTotalBytesSent(), totalBytesSent);
    EXPECT_GT(producer->getTotalUncompressedDataSize(),
              totalUncompressedDataSize);
    EXPECT_EQ(producer->getTotalBytesSent() - totalBytesSent,
              producer->getTotalUncompressedDataSize() -
                      totalUncompressedDataSize);

    EXPECT_EQ(cb::engine_errc::success, destroy_dcp_stream());
}

/*
 * Test to verify the number of items, total bytes sent and total data size
 * by the producer when DCP compression is disabled
 */
TEST_P(StreamTest, test_verifyProducerCompressionDisabledStats) {
    setupProducerCompression();

    cookie->setDatatypeSupport(PROTOCOL_BINARY_RAW_BYTES);
    setup_dcp_stream();

    ASSERT_EQ(cb::engine_errc::success, doStreamRequest(*producer).status);

    MockDcpMessageProducers producers;
    VBucketPtr vb = engine->getKVBucket()->getVBucket(vbid);
    prepareCheckpointItemsForStep(producers, *producer, *vb);

    /* Stream the snapshot marker first */
    EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
    EXPECT_EQ(0, producer->getItemsSent());

    uint64_t totalBytesSent = producer->getTotalBytesSent();
    uint64_t totalUncompressedDataSize =
            producer->getTotalUncompressedDataSize();
    EXPECT_GT(totalBytesSent, 0);
    EXPECT_GT(totalUncompressedDataSize, 0);

    /*
     * With value compression on the producer side disabled, stream a
     * compressible document. This should result in an increase in
     * total bytes. Even though the document is compressible, the
     * total data size and the total bytes sent would be incremented
     * by exactly the same amount
     */
    EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
    EXPECT_EQ(1, producer->getItemsSent());
    EXPECT_GT(producer->getTotalBytesSent(), totalBytesSent);
    EXPECT_GT(producer->getTotalUncompressedDataSize(),
              totalUncompressedDataSize);
    EXPECT_EQ(producer->getTotalBytesSent() - totalBytesSent,
              producer->getTotalUncompressedDataSize() -
                      totalUncompressedDataSize);

    EXPECT_EQ(cb::engine_errc::success, destroy_dcp_stream());
}

/*
 * Test to verify the number of items and the total bytes sent by the producer
 * under normal and error conditions
 */
TEST_P(StreamTest, VerifyProducerStats) {
    // Prevent checkpoint removal for verifying a number of in-memory snapshots
    registerCursorAtCMStart();

    VBucketPtr vb = engine->getKVBucket()->getVBucket(vbid);
    nlohmann::json meta = {
            {"topology", nlohmann::json::array({{"active", "replica"}})}};
    vb->setState(vbucket_state_active, &meta);
    setup_dcp_stream(0,
                     IncludeValue::No,
                     IncludeXattrs::No,
                     {{"enable_sync_writes", "true"},
                      {"consumer_name", "test_consumer"}});
    store_item(vbid, "key1", "value1");
    store_item(vbid, "key2", "value2");
    using namespace cb::durability;
    auto reqs = Requirements{Level::Majority, Timeout()};
    auto prepareToCommit = store_pending_item(vbid, "pending1", "value3", reqs);

    ASSERT_EQ(cb::engine_errc::success,
              vb->commit(prepareToCommit->getKey(),
                         prepareToCommit->getBySeqno(),
                         {},
                         vb->lockCollections(prepareToCommit->getKey()),
                         cookie));

    // Clear our cookie, we don't actually care about the cas of the item but
    // this is necessary to allow us to enqueue our next abort (which uses the
    // same cookie)
    engine->storeEngineSpecific(cookie, nullptr);

    auto prepareToAbort = store_pending_item(vbid, "pending2", "value4", reqs);
    ASSERT_EQ(cb::engine_errc::success,
              vb->abort(prepareToAbort->getKey(),
                        prepareToAbort->getBySeqno(),
                        {},
                        vb->lockCollections(prepareToAbort->getKey())));

    MockDcpMessageProducers producers;

    EXPECT_EQ(cb::engine_errc::success, doStreamRequest(*producer).status);

    prepareCheckpointItemsForStep(producers, *producer, *vb);

    /* Stream the snapshot marker first */
    EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
    EXPECT_EQ(0, producer->getItemsSent());

    uint64_t totalBytes = producer->getTotalBytesSent();
    EXPECT_GT(totalBytes, 0);

    /* Stream the first mutation. This should increment the
     * number of items and the total bytes sent.
     */
    EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
    EXPECT_EQ(1, producer->getItemsSent());
    EXPECT_GT(producer->getTotalBytesSent(), totalBytes);
    totalBytes = producer->getTotalBytesSent();

    /* Now simulate a failure while trying to stream the next
     * mutation.
     */
    producers.setMutationStatus(cb::engine_errc::too_big);

    EXPECT_EQ(cb::engine_errc::too_big, producer->step(producers));

    /* The number of items total bytes sent should remain the same */
    EXPECT_EQ(1, producer->getItemsSent());
    EXPECT_EQ(producer->getTotalBytesSent(), totalBytes);
    totalBytes = producer->getTotalBytesSent();

    /* Now stream the mutation again and the stats should have incremented */
    producers.setMutationStatus(cb::engine_errc::success);

    EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
    EXPECT_EQ(2, producer->getItemsSent());
    EXPECT_GT(producer->getTotalBytesSent(), totalBytes);
    totalBytes = producer->getTotalBytesSent();

    // Prepare
    EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
    EXPECT_EQ(3, producer->getItemsSent());
    EXPECT_GT(producer->getTotalBytesSent(), totalBytes);
    totalBytes = producer->getTotalBytesSent();

    // Commit
    EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
    EXPECT_EQ(4, producer->getItemsSent());
    EXPECT_GT(producer->getTotalBytesSent(), totalBytes);
    totalBytes = producer->getTotalBytesSent();

    // Prepare
    EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
    EXPECT_EQ(5, producer->getItemsSent());
    EXPECT_GT(producer->getTotalBytesSent(), totalBytes);
    totalBytes = producer->getTotalBytesSent();

    // SnapshotMarker - doesn't bump items sent
    EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
    EXPECT_EQ(5, producer->getItemsSent());
    EXPECT_GT(producer->getTotalBytesSent(), totalBytes);
    totalBytes = producer->getTotalBytesSent();

    // Abort
    EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
    EXPECT_EQ(6, producer->getItemsSent());
    EXPECT_GT(producer->getTotalBytesSent(), totalBytes);

    EXPECT_EQ(cb::engine_errc::success, destroy_dcp_stream());
}

/*
 * Test that when have a producer with IncludeValue set to Yes and IncludeXattrs
 * set to No an active stream created via a streamRequest returns false for
 * isKeyOnly.
 */
TEST_P(StreamTest, test_streamIsKeyOnlyFalseBecauseOfIncludeValue) {
    setup_dcp_stream(0, IncludeValue::Yes, IncludeXattrs::No);
    ASSERT_EQ(cb::engine_errc::success, doStreamRequest(*producer).status)
            << "stream request did not return cb::engine_errc::success";

    auto activeStream = std::dynamic_pointer_cast<ActiveStream>(
            producer->findStream(Vbid(0)));
    ASSERT_NE(nullptr, activeStream);
    EXPECT_FALSE(activeStream->isKeyOnly());
    EXPECT_EQ(cb::engine_errc::success, destroy_dcp_stream());
}

/*
 * Test that when have a producer with IncludeValue set to No and IncludeXattrs
 * set to Yes an active stream created via a streamRequest returns false for
 * isKeyOnly.
 */
TEST_P(StreamTest, test_streamIsKeyOnlyFalseBecauseOfIncludeXattrs) {
    setup_dcp_stream(0, IncludeValue::No, IncludeXattrs::Yes);
    ASSERT_EQ(cb::engine_errc::success, doStreamRequest(*producer).status)
            << "stream request did not return cb::engine_errc::success";

    auto activeStream = std::dynamic_pointer_cast<ActiveStream>(
            producer->findStream(Vbid(0)));
    ASSERT_NE(nullptr, activeStream);
    EXPECT_FALSE(activeStream->isKeyOnly());
    EXPECT_EQ(cb::engine_errc::success, destroy_dcp_stream());
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
            stream->public_makeResponseFromItem(qi,
                                                SendCommitSyncWriteAs::Commit);

    /**
     * Create a DCP response and check that a new item is created
     */
    auto mutProdResponse = dynamic_cast<MutationResponse*>(dcpResponse.get());
    ASSERT_NE(qi.get(), mutProdResponse->getItem().get());

    EXPECT_EQ(keyOnlyMessageSize, dcpResponse->getMessageSize());
    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
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
            stream->public_makeResponseFromItem(qi,
                                                SendCommitSyncWriteAs::Commit);

    /**
     * Create a DCP response and check that a new item is created
     */
    auto mutProdResponse = dynamic_cast<MutationResponse*>(dcpResponse.get());
    ASSERT_NE(qi.get(), mutProdResponse->getItem().get());

    EXPECT_EQ(keyOnlyMessageSize, dcpResponse->getMessageSize());
    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
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
            stream->public_makeResponseFromItem(qi,
                                                SendCommitSyncWriteAs::Commit);

    /**
     * Create a DCP response and check that a new item is not created
     */
    auto mutProdResponse = dynamic_cast<MutationResponse*>(dcpResponse.get());
    ASSERT_EQ(qi.get(), mutProdResponse->getItem().get());
    EXPECT_EQ(keyAndValueMessageSize, dcpResponse->getMessageSize());
    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
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
            stream->public_makeResponseFromItem(qi,
                                                SendCommitSyncWriteAs::Commit);

    /**
     * Create a DCP response and check that a new item is not created
     */
    auto mutProdResponse = dynamic_cast<MutationResponse*>(dcpResponse.get());
    ASSERT_EQ(qi.get(), mutProdResponse->getItem().get());
    EXPECT_EQ(keyAndValueMessageSize, dcpResponse->getMessageSize());
    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
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
            {reinterpret_cast<char*>(buffer.data()), buffer.size()});
    auto keyAndValueMessageSize =
            MutationResponse::mutationBaseMsgBytes +
            item->getKey().makeDocKeyWithoutCollectionID().size() +
            item->getNBytes() - sz;
    queued_item qi(std::move(item));

    setup_dcp_stream(0, IncludeValue::Yes, IncludeXattrs::No);
    std::unique_ptr<DcpResponse> dcpResponse =
            stream->public_makeResponseFromItem(qi,
                                                SendCommitSyncWriteAs::Commit);

    /**
     * Create a DCP response and check that a new item is created
     */
    auto mutProdResponse = dynamic_cast<MutationResponse*>(dcpResponse.get());
    ASSERT_NE(qi.get(), mutProdResponse->getItem().get());
    EXPECT_EQ(keyAndValueMessageSize, dcpResponse->getMessageSize());
    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
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
            stream->public_makeResponseFromItem(qi,
                                                SendCommitSyncWriteAs::Commit);
    /**
     * Create a DCP response and check that a new item is not created
     */
    auto mutProdResponse = dynamic_cast<MutationResponse*>(dcpResponse.get());
    ASSERT_EQ(qi.get(), mutProdResponse->getItem().get());
    EXPECT_EQ(keyAndValueMessageSize, dcpResponse->getMessageSize());
    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
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
            {reinterpret_cast<char*>(buffer.data()), buffer.size()});
    auto keyAndValueMessageSize =
            MutationResponse::mutationBaseMsgBytes +
            item->getKey().makeDocKeyWithoutCollectionID().size() + sz;
    queued_item qi(std::move(item));

    setup_dcp_stream(0, IncludeValue::No, IncludeXattrs::Yes);
    std::unique_ptr<DcpResponse> dcpResponse =
            stream->public_makeResponseFromItem(qi,
                                                SendCommitSyncWriteAs::Commit);

    /**
     * Create a DCP response and check that a new item is created
     */
    auto mutProdResponse = dynamic_cast<MutationResponse*>(dcpResponse.get());
    ASSERT_NE(qi.get(), mutProdResponse->getItem().get());
    EXPECT_EQ(keyAndValueMessageSize, dcpResponse->getMessageSize());
    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
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
            {reinterpret_cast<char*>(buffer.data()), buffer.size()});
    auto keyAndValueMessageSize =
            MutationResponse::mutationBaseMsgBytes +
            item->getKey().makeDocKeyWithoutCollectionID().size() + sz;
    queued_item qi(std::move(item));

    setup_dcp_stream(
            0, IncludeValue::NoWithUnderlyingDatatype, IncludeXattrs::Yes);
    std::unique_ptr<DcpResponse> dcpResponse =
            stream->public_makeResponseFromItem(qi,
                                                SendCommitSyncWriteAs::Commit);

    /**
     * Create a DCP response and check that a new item is created
     */
    auto mutProdResponse = dynamic_cast<MutationResponse*>(dcpResponse.get());
    auto& responseItem = mutProdResponse->getItem();
    EXPECT_EQ(qi->getDataType(), responseItem->getDataType());
    EXPECT_EQ(keyAndValueMessageSize, dcpResponse->getMessageSize());
    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
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
            stream->public_makeResponseFromItem(qi,
                                                SendCommitSyncWriteAs::Commit);

    /**
     * Create a DCP response and check that a new item is created
     */
    auto mutProdResponse = dynamic_cast<MutationResponse*>(dcpResponse.get());
    auto& responseItem = mutProdResponse->getItem();
    EXPECT_EQ(qi->getDataType(), responseItem->getDataType());
    EXPECT_EQ(keyAndValueMessageSize, dcpResponse->getMessageSize());
    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
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
    EXPECT_TRUE(stream->public_nextCheckpointItem(*producer))
            << "nextCheckpointItem() should initially be true.";

    // Get the set of outstanding items
    auto items = stream->public_getOutstandingItems(*vb0);

    // REGRESSION CHECK: nextCheckpointItem() should still return true
    EXPECT_TRUE(stream->public_nextCheckpointItem(*producer))
            << "nextCheckpointItem() after getting outstanding items should be "
               "true.";

    // Process the set of items
    stream->public_processItems(items);

    // Should finish with nextCheckpointItem() returning false.
    EXPECT_FALSE(stream->public_nextCheckpointItem(*producer))
            << "nextCheckpointItem() after processing items should be false.";
    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
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

    ASSERT_TRUE(stream->isInMemory());

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
    std::unique_ptr<DcpResponse> response(
            stream->public_nextQueuedItem(*producer));
    ASSERT_NE(nullptr, response);
    EXPECT_TRUE(response->isMetaEvent()) << "Expected 1st item to be meta";

    response = stream->public_nextQueuedItem(*producer);
    ASSERT_NE(nullptr, response);
    EXPECT_FALSE(response->isMetaEvent()) << "Expected 2nd item to be non-meta";

    response = stream->public_nextQueuedItem(*producer);
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
        response = stream->public_nextQueuedItem(*producer);
    } while (response);
    EXPECT_EQ(0, stream->getItemsRemaining()) << "Should have 0 items "
                                                 "remaining after advancing "
                                                 "cursor and draining readyQ";
    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
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
    auto front = stream->public_nextQueuedItem(*producer);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, front->getEvent());
    auto snapMarker = dynamic_cast<SnapshotMarker&>(*front);
    while (stream->public_readyQSize() > 0) {
        auto item = stream->public_nextQueuedItem(*producer);
        EXPECT_EQ(DcpResponse::Event::Mutation, item->getEvent());
        auto seqno = item->getBySeqno().value();
        EXPECT_GE(seqno, snapMarker.getStartSeqno());
        EXPECT_LE(seqno, snapMarker.getEndSeqno());
    }

    // Check that backfill stats have been updated correctly. There will be at
    // least numItems but may be higher if those GAT mutations/expirations
    // got flushed and are in the backfill
    EXPECT_GE(numItems, stream->getNumBackfillItems());
    EXPECT_GE(numItems, *stream->getNumBackfillItemsRemaining());

    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
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
    engine->getKVBucket()->getRWUnderlying(vbid)->delVBucket(
            vbid,
            /* file rev */ std::make_unique<KVStoreRevision>(1));

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

    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
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
    uint64_t numItems = 2;
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
        while ((numItems - 1) != stream->getLastBackfilledSeqno()) {
            uSleepTime = decayingSleep(uSleepTime);
        }
    }

    /* Consume the backfill item(s) */
    stream->consumeBackfillItems(*producer, /*snapshot*/ 1 + /*mutation*/ 1);

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
    stream->consumeBackfillItems(*producer, 1);
    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
}

TEST_P(StreamTest, CursorDroppingBasicBackfillState) {
    /* Add 2 items; we need this to keep stream in backfill state */
    const uint64_t numItems = 2;
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
    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
}

/*
 * Tests that when a cursor is dropped the associated stream's pointer
 * to the cursor is set to nullptr.
 */
TEST_P(StreamTest, MB_32329CursorDroppingResetCursor) {
    /* Add 2 items; we need this to keep stream in backfill state */
    const uint64_t numItems = 2;
    addItemsAndRemoveCheckpoint(numItems);

    /* Set up a DCP stream */
    setup_dcp_stream();

    /* Transition stream to backfill state and expect cursor dropping call to
       succeed */
    stream->transitionStateToBackfilling();

    /*
     * Increase the use_count of the cursor shared pointer, this replicates
     * the behaviour of the CheckpointMemRecoveryTask (see
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
    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
}

TEST_P(StreamTest, CursorDroppingBasicInMemoryState) {
    /* Set up a DCP stream */
    setup_dcp_stream();

    /* Transition stream to in-memory state and expect cursor dropping call to
       succeed */
    EXPECT_TRUE(stream->public_handleSlowStream());
    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
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

    /* Transition stream to dead state and expect cursor dropping call
       to fail */
    stream->transitionStateToDead();
    EXPECT_FALSE(stream->public_handleSlowStream());
    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
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
    EXPECT_EQ(cb::engine_errc::success, result.status);
    EXPECT_EQ(cb::engine_errc::success,
              producer->closeStream(/*opaque*/ 0, vb0->getId()));

    /* Set a start_seqno > purge_seqno > snap_start_seqno */
    engine->getKVBucket()->getLockedVBucket(vbid)->setPurgeSeqno(numItems - 3);

    /* We don't expect a rollback for this */
    result = doStreamRequest(
            *producer, numItems - 2, numItems, 0, numItems - 2, vbUuid);
    EXPECT_EQ(cb::engine_errc::success, result.status);
    EXPECT_EQ(cb::engine_errc::success,
              producer->closeStream(/*opaque*/ 0, vb0->getId()));

    /* Set a purge_seqno > start_seqno */
    engine->getKVBucket()->getLockedVBucket(vbid)->setPurgeSeqno(numItems - 1);

    /* Now we expect a rollback to 0 */
    result = doStreamRequest(
            *producer, numItems - 2, numItems, 0, numItems - 2, vbUuid);
    EXPECT_EQ(cb::engine_errc::rollback, result.status);
    EXPECT_EQ(0, result.rollbackSeqno);
    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
}

/*
 * Test to ensure that when a streamRequest is made to a dead vbucket, we
 * (1) return not my vbucket.
 * (2) do not invoke the callback function (which is passed as parameter).
 * The reason we don't want to invoke the callback function is that it will
 * invoke mcbp_response_handler and so generate a response
 * (cb::engine_errc::success) and then when we continue the execution of the
 * streamRequest function we generate a second response
 * (cb::engine_errc::not_my_vbucket).
 */
TEST_P(StreamTest, MB_25820_callback_not_invoked_on_dead_vb_stream_request) {
    setup_dcp_stream(0, IncludeValue::No, IncludeXattrs::No);
    ASSERT_EQ(cb::engine_errc::success,
              engine->getKVBucket()->setVBucketState(
                      vbid, vbucket_state_dead, {}, TransferVB::Yes));
    uint64_t vbUuid = vb0->failovers->getLatestUUID();
    // Given the vbucket state is dead we should return not my vbucket.
    EXPECT_EQ(cb::engine_errc::not_my_vbucket,
              doStreamRequest(*producer, 0, 0, 0, 0, vbUuid).status);
    // The callback function past to streamRequest should not be invoked.
    ASSERT_EQ(0, callbackCount);
}

// Test the compression control success case
TEST_P(StreamTest, validate_compression_control_message_allowed) {
    // For success enable the snappy datatype on the connection
    cookie->setDatatypeSupport(PROTOCOL_BINARY_DATATYPE_SNAPPY);
    setup_dcp_stream();
    std::string compressCtrlMsg("force_value_compression");
    std::string compressCtrlValue("true");
    EXPECT_TRUE(producer->isCompressionEnabled());

    // Sending a control message after enabling SNAPPY should succeed
    EXPECT_EQ(cb::engine_errc::success,
              producer->control(0, compressCtrlMsg, compressCtrlValue));
    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
}

// Test that ActiveStream::processItems correctly encodes a Snapshot marker
// (with CHK flag set) when processItems() is called with a single
// checkpoint_start item.
TEST_P(StreamTest, ProcessItemsSingleCheckpointStart) {
    setup_dcp_stream();

    // Setup - put a single checkpoint_start item into a vector to be passed
    // to ActiveStream::processItems()
    ActiveStream::OutstandingItemsResult result;
    result.items.push_back(queued_item(new Item(makeStoredDocKey("start"),
                                                vbid,
                                                queue_op::checkpoint_start,
                                                2,
                                                1)));
    result.ranges.push_back({{0, 1}, {}, {}});

    // Test - call processItems() twice: once with a single checkpoint_start
    // item, then with a single mutation.
    // (We need the single mutation to actually cause a SnapshotMarker to be
    // generated, as SnapshotMarkers cannot represent an empty snapshot).
    stream->public_processItems(result);

    result.items.clear();
    auto mutation = makeCommittedItem(makeStoredDocKey("mutation"), "value");
    mutation->setBySeqno(2);
    result.items.push_back(mutation);
    stream->public_processItems(result);

    // Validate - check that we have two items in the readyQ (SnapshotMarker &
    // DcpMutation), and that the SnapshotMarker is correctly encoded (should
    // have CHK flag set).
    const auto& readyQ = stream->public_readyQ();
    ASSERT_EQ(2, readyQ.size());
    ASSERT_EQ(DcpResponse::Event::SnapshotMarker, readyQ.front()->getEvent());
    auto& snapMarker = dynamic_cast<SnapshotMarker&>(*readyQ.front());
    EXPECT_EQ(MARKER_FLAG_MEMORY | MARKER_FLAG_CHK, snapMarker.getFlags());

    EXPECT_EQ(DcpResponse::Event::Mutation, readyQ.back()->getEvent());
}

// Variation on ProcessItemsSingleCheckpointStart - test that
// ActiveStream::processItems correctly encodes a Snapshot marker (with CHK
// flag set) when processItems() is called with multiple items but
// checkpoint_start item is the last item in the batch.
TEST_P(StreamTest, ProcessItemsCheckpointStartIsLastItem) {
    setup_dcp_stream();

    // Setup - Create and discard the initial in-memory snapshot (it always has
    // the CKPT flag set, we just want to ignore this first one as are
    // testing behaviour of subsequent checkpoints).
    ActiveStream::OutstandingItemsResult result;
    result.items.emplace_back(new Item(
            makeStoredDocKey("start"), vbid, queue_op::checkpoint_start, 1, 1));
    auto dummy = makeCommittedItem(makeStoredDocKey("ignore"), "value");
    dummy->setBySeqno(9);
    result.items.push_back(dummy);
    result.items.emplace_back(new Item(
            makeStoredDocKey("end"), vbid, queue_op::checkpoint_end, 1, 10));
    result.ranges.push_back({{0, 9}, {}, {}});

    stream->public_processItems(result);
    result.items.clear();
    stream->public_popFromReadyQ();
    stream->public_popFromReadyQ();

    // Setup - call ActiveStream::processItems() with the end of one checkpoint
    // and the beginning of the next:
    //     muatation, checkpoint_end, checkpoint_start
    auto mutation1 = makeCommittedItem(makeStoredDocKey("M1"), "value");
    mutation1->setBySeqno(10);
    result.items.push_back(mutation1);
    result.items.push_back(queued_item(new Item(makeStoredDocKey("end"),
                                                vbid,
                                                queue_op::checkpoint_end,
                                                1,
                                                /*seqno*/ 11)));
    result.ranges.push_back({{10, 10}, {}, {}});
    result.items.push_back(queued_item(new Item(makeStoredDocKey("start"),
                                                vbid,
                                                queue_op::checkpoint_start,
                                                2,
                                                /*seqno*/ 11)));
    result.ranges.push_back({{11, 11}, {}, {}});

    // Test - call processItems() twice: once with the items above, then with
    // a single mutation.
    stream->public_processItems(result);

    result.items.clear();
    auto mutation2 = makeCommittedItem(makeStoredDocKey("M2"), "value");
    mutation2->setBySeqno(11);
    result.items.push_back(mutation2);
    stream->public_processItems(result);

    // Validate - check that we have four items in the readyQ with the correct
    // state:
    //    1. SnapshotMarker(10,10)
    //    2. Mutation(M1, 10)
    //    3. SnapshotMarker(11, 11, CHK)
    //    4. Mutation(M2, 11)
    const auto& readyQ = stream->public_readyQ();
    ASSERT_EQ(4, readyQ.size());

    // First snapshotMarker should be for seqno 10 and _not_ have the CHK flag
    // set.
    ASSERT_EQ(DcpResponse::Event::SnapshotMarker, readyQ.front()->getEvent());
    auto& snapMarker1 = dynamic_cast<SnapshotMarker&>(*readyQ.front());
    EXPECT_EQ(MARKER_FLAG_MEMORY, snapMarker1.getFlags());
    // Don't care about startSeqno for this snapshot...
    EXPECT_EQ(10, snapMarker1.getEndSeqno());

    stream->public_nextQueuedItem(*producer);
    EXPECT_EQ(DcpResponse::Event::Mutation, readyQ.front()->getEvent());

    // Second snapshotMarker should be for seqno 11 and have the CHK flag set.
    stream->public_nextQueuedItem(*producer);
    ASSERT_EQ(DcpResponse::Event::SnapshotMarker, readyQ.front()->getEvent());
    auto& snapMarker2 = dynamic_cast<SnapshotMarker&>(*readyQ.front());
    EXPECT_EQ(MARKER_FLAG_MEMORY | MARKER_FLAG_CHK, snapMarker2.getFlags());
    EXPECT_EQ(11, snapMarker2.getStartSeqno());
    EXPECT_EQ(11, snapMarker2.getEndSeqno());

    stream->public_nextQueuedItem(*producer);
    EXPECT_EQ(DcpResponse::Event::Mutation, readyQ.front()->getEvent());
}

TEST_P(StreamTest, ProducerReceivesSeqnoAckForErasedStream) {
    create_dcp_producer(0, /*flags*/
                        IncludeValue::Yes,
                        IncludeXattrs::Yes,
                        {{"send_stream_end_on_client_close_stream", "true"},
                         {"enable_sync_writes", "true"},
                         {"consumer_name", "replica1"}});

    // Need to do a stream request to put the stream in the producers map
    ASSERT_EQ(cb::engine_errc::success, doStreamRequest(*producer).status);

    // Close the stream to start the removal process
    EXPECT_EQ(cb::engine_errc::success,
              producer->closeStream(0 /*opaque*/, vbid));

    // Stream should still exist, but should be dead
    auto stream = producer->findStream(vbid);
    EXPECT_TRUE(stream);
    EXPECT_FALSE(stream->isActive());

    // Step the stream on, this should remove the stream from the producer's
    // StreamsMap
    MockDcpMessageProducers producers;
    EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpStreamEnd, producers.last_op);

    // Stream should no longer exist in the map
    EXPECT_FALSE(producer->findStream(vbid));

    EXPECT_EQ(cb::engine_errc::success,
              producer->seqno_acknowledged(
                      0 /*opaque*/, vbid, 1 /*prepareSeqno*/));
}

MATCHER_P(HasOperation, op, "") {
    return arg.getOperation() == op;
}

/**
 * Regression test for MB-38356 - if a DCP consumer sends a stream request for
 * a Vbid which it is already streaming, then the second request should fail,
 * leaving the first stream as it was.
 * (In the case of MB-38356 the first stream incorrectly lost it's cursor).
 */
TEST_P(StreamTest, MB38356_DuplicateStreamRequest) {
    setup_dcp_stream(0, IncludeValue::No, IncludeXattrs::No);
    ASSERT_EQ(cb::engine_errc::success, doStreamRequest(*producer).status);

    // Second request to same vbid should fail.
    EXPECT_EQ(cb::engine_errc::key_already_exists,
              doStreamRequest(*producer).status);

    // Original stream should still be established and allow items to be
    // streamed.
    auto stream = producer->findStream(vbid);
    ASSERT_TRUE(stream);
    const auto cursor = stream->getCursor();
    auto cursorPtr = cursor.lock();
    EXPECT_TRUE(cursorPtr);
    auto& vb = *engine->getVBucket(vbid);
    auto& cm = *vb.checkpointManager;
    std::vector<queued_item> qis;
    cm.getItemsForCursor(*cursorPtr, qis, std::numeric_limits<uint64_t>::max());
    // Copy to plain Item vector to aid in checking expected value.
    std::vector<Item> items;
    std::transform(qis.begin(),
                   qis.end(),
                   std::back_inserter(items),
                   [](const auto& rcptr) { return *rcptr; });

    EXPECT_THAT(items,
                ElementsAre(HasOperation(queue_op::checkpoint_start),
                            HasOperation(queue_op::set_vbucket_state)));

    EXPECT_EQ(cb::engine_errc::success, destroy_dcp_stream());
}

class CacheCallbackTest : public StreamTest {
protected:
    void SetUp() override {
        StreamTest::SetUp();
        store_item(vbid, key, "value");

        removeCheckpoint(numItems);

        /* Set up a DCP stream for the backfill */
        setup_dcp_stream();
    }

    void TearDown() override {
        producer->closeAllStreams();
        StreamTest::TearDown();
    }

    const size_t numItems = 1;
    const std::string key = "key";
    const DiskDocKey diskKey = makeDiskDocKey(key);
};

/*
 * Tests the callback member function of the CacheCallback class.  This
 * particular test should result in the CacheCallback having a status of
 * cb::engine_errc::key_already_exists.
 */
TEST_P(CacheCallbackTest, CacheCallback_key_eexists) {
    CacheCallback callback(*engine->getKVBucket(), stream);

    stream->transitionStateToBackfilling();
    CacheLookup lookup(diskKey, /*BySeqno*/ 1, vbid);
    callback.callback(lookup);

    /* Invoking callback should result in backfillReceived being called on
     * activeStream, which should return true and hence set the callback status
     * to cb::engine_errc::key_already_exists.
     */
    EXPECT_EQ(cb::engine_errc::key_already_exists, callback.getStatus());

    /* Verify that the item is read in the backfill */
    EXPECT_EQ(numItems, stream->getNumBackfillItems());

    /* Verify have the backfill item sitting in the readyQ */
    EXPECT_EQ(numItems, stream->public_readyQ().size());
}

/*
 * Tests the callback member function of the CacheCallback class.  This
 * particular test should result in the CacheCallback having a status of
 * cb::engine_errc::success.
 */
TEST_P(CacheCallbackTest, CacheCallback_engine_success) {
    CacheCallback callback(*engine->getKVBucket(), stream);

    stream->transitionStateToBackfilling();
    // Passing in wrong BySeqno - should be 1, but passing in 0
    CacheLookup lookup(diskKey, /*BySeqno*/ 0, vbid);
    callback.callback(lookup);

    /* Invoking callback should result in backfillReceived NOT being called on
     * activeStream, and hence the callback status should be set to
     * cb::engine_errc::success.
     */
    EXPECT_EQ(cb::engine_errc::success, callback.getStatus());

    /* Verify that the item is not read in the backfill */
    EXPECT_EQ(0, stream->getNumBackfillItems());

    /* Verify do not have the backfill item sitting in the readyQ */
    EXPECT_EQ(0, stream->public_readyQ().size());
}

/*
 * Tests the callback member function of the CacheCallback class.  Due to the
 * key being evicted the test should result in the CacheCallback having a status
 * of cb::engine_errc::success.
 */
TEST_P(CacheCallbackTest, CacheCallback_engine_success_not_resident) {
    if (bucketType == "ephemeral") {
        /* The test relies on being able to evict a key from memory.
         * Eviction is not supported with empherial buckets.
         */
        return;
    }
    CacheCallback callback(*engine->getKVBucket(), stream);

    stream->transitionStateToBackfilling();
    CacheLookup lookup(diskKey, /*BySeqno*/ 1, vbid);
    // Make the key non-resident by evicting the key
    const char* msg;
    engine->getKVBucket()->evictKey(diskKey.getDocKey(), vbid, &msg);
    callback.callback(lookup);

    /* With the key evicted, invoking callback should result in backfillReceived
     * NOT being called on activeStream, and hence the callback status should be
     * set to cb::engine_errc::success
     */
    EXPECT_EQ(cb::engine_errc::success, callback.getStatus());

    /* Verify that the item is not read in the backfill */
    EXPECT_EQ(0, stream->getNumBackfillItems());

    /* Verify do not have the backfill item sitting in the readyQ */
    EXPECT_EQ(0, stream->public_readyQ().size());
}

/*
 * Tests the callback member function of the CacheCallback class.  This
 * particular test should result in the CacheCallback having a status of
 * cb::engine_errc::temporary_failure (no memory available). This would then
 * normally cause backfill to yield
 */
TEST_P(CacheCallbackTest, CacheCallback_engine_enomem) {
    /*
     * Ensure that DcpProducer::recordBackfillManagerBytesRead returns false
     * by setting the backfill buffer size to zero, and then setting bytes read
     * to one.
     */
    producer->setBackfillBufferSize(0);
    producer->setBackfillBufferBytesRead(1);

    CacheCallback callback(*engine->getKVBucket(), stream);

    stream->transitionStateToBackfilling();
    CacheLookup lookup(diskKey, /*BySeqno*/ 1, vbid);
    callback.callback(lookup);

    /* Invoking callback should result in backfillReceived being called on
     * activeStream, which should return false (due to
     * DcpProducer::recordBackfillManagerBytesRead returning false), and hence
     * set the callback status to cb::engine_errc::temporary_failure.
     */
    EXPECT_EQ(cb::engine_errc::temporary_failure, callback.getStatus());

    // This is the same test
    EXPECT_TRUE(callback.shouldYield());

    /* Verify that the item is not read in the backfill */
    EXPECT_EQ(0, stream->getNumBackfillItems());

    /* Verify do not have the backfill item sitting in the readyQ */
    EXPECT_EQ(0, stream->public_readyQ().size());
}

// Test cases which run in both Full and Value eviction
INSTANTIATE_TEST_SUITE_P(PersistentAndEphemeral,
                         StreamTest,
                         ::testing::Values("persistent_couchstore",
                                           "ephemeral"),
                         [](const ::testing::TestParamInfo<std::string>& info) {
                             return info.param;
                         });

// Test cases which run in both Full and Value eviction
INSTANTIATE_TEST_SUITE_P(PersistentAndEphemeral,
                         CacheCallbackTest,
                         ::testing::Values("persistent_couchstore",
                                           "ephemeral"),
                         [](const ::testing::TestParamInfo<std::string>& info) {
                             return info.param;
                         });

void SingleThreadedActiveStreamTest::SetUp() {
    STParameterizedBucketTest::SetUp();
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    setupProducer();
}

void SingleThreadedActiveStreamTest::TearDown() {
    stream.reset();
    producer.reset();
    STParameterizedBucketTest::TearDown();
}

void SingleThreadedActiveStreamTest::startCheckpointTask() {
    if (!producer->getCheckpointSnapshotTask()) {
        producer->createCheckpointProcessorTask();
        producer->scheduleCheckpointProcessorTask();
    }
}

void SingleThreadedActiveStreamTest::setupProducer(
        const std::vector<std::pair<std::string, std::string>>& controls,
        bool startCheckpointProcessorTask) {
    uint32_t flags = 0;

    // We don't set the startTask flag here because we will create the task
    // manually. We do this because the producer actually creates the task on
    // StreamRequest which we do not do because we want a MockActiveStream.
    producer = std::make_shared<MockDcpProducer>(*engine,
                                                 cookie,
                                                 "test_producer->test_consumer",
                                                 flags,
                                                 false /*startTask*/);

    if (startCheckpointProcessorTask) {
        startCheckpointTask();
    }

    for (const auto& c : controls) {
        EXPECT_EQ(cb::engine_errc::success,
                  producer->control(0 /*opaque*/, c.first, c.second));
    }

    auto vb = engine->getVBucket(vbid);

    stream = std::make_shared<MockActiveStream>(
            engine.get(), producer, flags, 0 /*opaque*/, *vb);

    stream->setActive();
}

MutationStatus SingleThreadedActiveStreamTest::public_processSet(
        VBucket& vb, Item& item, const VBQueueItemCtx& ctx) {
    auto htRes = vb.ht.findForUpdate(item.getKey());
    auto* v = htRes.selectSVToModify(item);
    return vb
            .processSet(htRes,
                        v,
                        item,
                        0 /*cas*/,
                        true /*allowExisting*/,
                        false /*hasMetadata*/,
                        ctx,
                        {/*no predicate*/})
            .first;
}

void SingleThreadedActiveStreamTest::recreateProducerAndStream(
        VBucket& vb,
        uint32_t flags,
        std::optional<std::string_view> jsonFilter) {
    producer = std::make_shared<MockDcpProducer>(*engine,
                                                 cookie,
                                                 "test_producer->test_consumer",
                                                 flags,
                                                 false /*startTask*/);
    producer->setSyncReplication(SyncReplication::SyncReplication);
    recreateStream(vb, true /*enforceProducerFlags*/, jsonFilter);
}

void SingleThreadedActiveStreamTest::recreateStream(
        VBucket& vb,
        bool enforceProducerFlags,
        std::optional<std::string_view> jsonFilter) {
    if (enforceProducerFlags) {
        stream = producer->mockActiveStreamRequest(
                0 /*flags*/,
                0 /*opaque*/,
                vb,
                0 /*st_seqno*/,
                ~0 /*en_seqno*/,
                0x0 /*vb_uuid*/,
                0 /*snap_start_seqno*/,
                ~0 /*snap_end_seqno*/,
                producer->public_getIncludeValue(),
                producer->public_getIncludeXattrs(),
                producer->public_getIncludeDeletedUserXattrs(),
                jsonFilter);
    } else {
        stream = producer->mockActiveStreamRequest(0 /*flags*/,
                                                   0 /*opaque*/,
                                                   vb,
                                                   0 /*st_seqno*/,
                                                   ~0 /*en_seqno*/,
                                                   0x0 /*vb_uuid*/,
                                                   0 /*snap_start_seqno*/,
                                                   ~0 /*snap_end_seqno*/);
    }
}

void SingleThreadedPassiveStreamTest::SetUp() {
    STParameterizedBucketTest::SetUp();

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    setupConsumerAndPassiveStream();
}

void SingleThreadedPassiveStreamTest::TearDown() {
    ASSERT_NE(cb::engine_errc::disconnect,
              consumer->closeStream(0 /*opaque*/, vbid));
    consumer.reset();
    STParameterizedBucketTest::TearDown();
}

void SingleThreadedPassiveStreamTest::setupConsumerAndPassiveStream() {
    // In the normal DCP protocol flow, ns_server issues an AddStream request
    // to the DcpConsumer before DCP Control messages are necessarily
    // negotiated.
    // As such, create the PassiveStream *before* enabling SyncReplication
    // (normally done using DCP_CONTROL negotiation with the Producer) to
    // accurately reflect how these classes are used in the real flow.
    consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test_consumer");
    ASSERT_EQ(cb::engine_errc::success,
              consumer->addStream(0 /*opaque*/, vbid, 0 /*flags*/));
    stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    ASSERT_TRUE(stream->isActive());

    if (enableSyncReplication) {
        consumer->enableSyncReplication();
    }

    // Consume the StreamRequest message on the PassiveStreams' readyQ,
    // and simulate the producer responding to it.
    const auto& readyQ = stream->public_readyQ();
    ASSERT_EQ(1, readyQ.size());
    auto msg = stream->public_popFromReadyQ();
    ASSERT_TRUE(msg);
    ASSERT_EQ(DcpResponse::Event::StreamReq, msg->getEvent());
    stream->acceptStream(cb::mcbp::Status::Success, 0);
    ASSERT_TRUE(stream->isActive());

    // PassiveStream should have sent an AddStream response back to ns_server,
    // plus an optional SeqnoAcknowledgement (if SyncReplication enabled and
    // necessary to Ack back to producer).
    msg = stream->public_popFromReadyQ();
    ASSERT_EQ(DcpResponse::Event::AddStream, msg->getEvent());
    msg = stream->public_popFromReadyQ();
    if (msg) {
        ASSERT_EQ(DcpResponse::Event::SeqnoAcknowledgement, msg->getEvent());
    }
}

TEST_P(SingleThreadedActiveStreamTest, DiskSnapshotSendsChkMarker) {
    auto vb = engine->getVBucket(vbid);
    auto& ckptMgr = *vb->checkpointManager;
    // Get rid of set_vb_state and any other queue_op we are not interested in
    ckptMgr.clear(0 /*seqno*/);

    // Remove the initial stream, we want to force it to backfill.
    stream.reset();

    const auto key = makeStoredDocKey("key");
    const std::string value = "value";
    auto item = make_item(vbid, key, value);

    EXPECT_EQ(MutationStatus::WasClean,
              public_processSet(*vb, item, VBQueueItemCtx()));

    // Ensure mutation is on disk; no longer present in CheckpointManager.
    vb->checkpointManager->createNewCheckpoint();
    flushVBucketToDiskIfPersistent(vbid, 1);
    ASSERT_EQ(1, vb->checkpointManager->getNumCheckpoints());

    recreateStream(*vb);
    ASSERT_TRUE(stream->isBackfilling());

    // Run the backfill we scheduled when we transitioned to the backfilling
    // state. Only run the backfill task once because we only care about the
    // snapshot marker.
    auto& bfm = producer->getBFM();
    bfm.backfill();

    // No message processed, BufferLog empty
    ASSERT_EQ(0, producer->getBytesOutstanding());

    // readyQ must contain a SnapshotMarker
    ASSERT_GE(stream->public_readyQSize(), 1);
    auto resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());

    auto& marker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_TRUE(marker.getFlags() & MARKER_FLAG_CHK);
    EXPECT_TRUE(marker.getFlags() & MARKER_FLAG_DISK);
    EXPECT_FALSE(marker.getHighCompletedSeqno());

    producer->cancelCheckpointCreatorTask();
}

/// Test that disk backfill remaining isn't prematurely zero (before counts
/// read from disk by backfill task).
TEST_P(SingleThreadedActiveStreamTest, DiskBackfillInitializingItemsRemaining) {
    auto vb = engine->getVBucket(vbid);
    auto& manager = *vb->checkpointManager;

    // Delete initial stream (so we can re-create after items are only available
    // from disk.
    stream.reset();

    // Store 3 items (to check backfill remaining counts).
    // Add items, flush it to disk, then clear checkpoint to force backfill.
    store_item(vbid, makeStoredDocKey("key1"), "value");
    store_item(vbid, makeStoredDocKey("key2"), "value");
    store_item(vbid, makeStoredDocKey("key3"), "value");

    const auto openId = manager.getOpenCheckpointId();
    ASSERT_GT(manager.createNewCheckpoint(), openId);
    flushVBucketToDiskIfPersistent(vbid, 3 /*expected_num_flushed*/);
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(0, manager.getNumOpenChkItems());

    // Re-create producer now we have items only on disk.
    setupProducer();
    ASSERT_TRUE(stream->isBackfilling());

    // Should report empty itemsRemaining as that would mislead
    // ns_server if they asked for stats before the backfill task runs (they
    // would think backfill is complete).
    EXPECT_FALSE(stream->getNumBackfillItemsRemaining());

    bool statusFound = false;
    std::string expectedKey;
    auto checkStatusFn = [&statusFound, &expectedKey](std::string_view key,
                                                      std::string_view value,
                                                      const void* ctx) {
        if (key == "status"sv) {
            EXPECT_EQ(expectedKey, std::string(value.data(), value.size()));
            statusFound = true;
        }
    };

    // Should report status == "calculating_item_count" before backfill
    // scan has occurred.
    expectedKey = "calculating-item-count";
    stream->addTakeoverStats(checkStatusFn, nullptr, *vb);
    EXPECT_TRUE(statusFound);

    // Run the backfill we scheduled when we transitioned to the backfilling
    // state. Run the backfill task once to get initial item counts.
    auto& bfm = producer->getBFM();
    bfm.backfill();
    EXPECT_EQ(3, *stream->getNumBackfillItemsRemaining());
    // Should report status == "backfilling"
    statusFound = false;
    expectedKey = "backfilling";
    stream->addTakeoverStats(checkStatusFn, nullptr, *vb);
    EXPECT_TRUE(statusFound);

    // Run again to actually scan (items remaining unchanged).
    bfm.backfill();
    EXPECT_EQ(3, *stream->getNumBackfillItemsRemaining());
    statusFound = false;
    expectedKey = "backfilling";
    stream->addTakeoverStats(checkStatusFn, nullptr, *vb);
    EXPECT_TRUE(statusFound);

    // Finally run again to complete backfill (so it is shutdown in a clean
    // fashion).
    bfm.backfill();

    // Consume the items from backfill; should update items remaining.
    // Actually need to consume 4 items (snapshot_marker + 3x mutation).
    stream->consumeBackfillItems(*producer, 4);
    EXPECT_EQ(0, *stream->getNumBackfillItemsRemaining());
    statusFound = false;
    expectedKey = "in-memory";
    stream->addTakeoverStats(checkStatusFn, nullptr, *vb);
    EXPECT_TRUE(statusFound);
}

/// Test that backfill is correctly cancelled if the VBucket is deleted
/// part-way through the backfill.
TEST_P(SingleThreadedActiveStreamTest, BackfillDeletedVBucket) {
    {
        // Setup: Store items then remove them from checkpoint manager, forcing
        // DCP Producer to backfill from disk.
        // In own scope as we don't want to keep VBucketPtr alive
        // past when we call deleteVBucket.
        auto vb = engine->getVBucket(vbid);
        auto& manager = *vb->checkpointManager;

        // Delete initial stream (so we can re-create after items are only
        // available from disk.
        stream.reset();

        // Store some items, create new checkpoint and flush so we have
        // something to backfill from disk
        store_item(vbid, makeStoredDocKey("key1"), "value");
        store_item(vbid, makeStoredDocKey("key2"), "value");

        const auto openId = manager.getOpenCheckpointId();
        ASSERT_GT(manager.createNewCheckpoint(), openId);
        flushVBucketToDiskIfPersistent(vbid, 2 /*expected_num_flushed*/);
        ASSERT_EQ(1, manager.getNumCheckpoints());
        ASSERT_EQ(0, manager.getNumOpenChkItems());
    }

    auto* kvstore = engine->getKVBucket()->getRWUnderlying(vbid);
    if (persistent()) {
        ASSERT_TRUE(kvstore);
        // Sanity check - expected number of items are indeed on disk:
        ASSERT_EQ(2, kvstore->getItemCount(vbid));
    }

    // Re-create producer now we have items only on disk, setting a buffer which
    // can only hold 1 item (so backfill doesn't complete in one scan).
    setupProducer();
    producer->setBackfillBufferSize(1);
    ASSERT_TRUE(stream->isBackfilling());

    // Initialise the backfill of this VBucket (performs initial scan but
    // doesn't read any data yet).
    auto& bfm = producer->getBFM();
    ASSERT_EQ(backfill_success, bfm.backfill());
    ASSERT_EQ(2, *stream->getNumBackfillItemsRemaining());

    // Now delete the VBucket.
    ASSERT_EQ(cb::engine_errc::success,
              engine->getKVBucket()->deleteVBucket(vbid));

    // Normally done by DcpConnMap::vBucketStateChanged(), but the producer
    // isn't tracked in DcpConnMap here.
    stream->setDead(cb::mcbp::DcpStreamEndStatus::StateChanged);

    // Ensure background AuxIO task to actually delete VBucket from disk is run.
    if (persistent()) {
        auto& auxIoQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
        runNextTask(auxIoQ, "Removing (dead) vb:0 from memory and disk");

        // vBucket should be gone from disk - attempts to read should fail.
        EXPECT_THROW(kvstore->getItemCount(vbid), std::system_error);
    }

    // Test: run backfillMgr again to actually attempt to read items from disk.
    // Given vBucket has been deleted this should result in the backfill
    // finishing early instead of snoozing.
    ASSERT_EQ(1, bfm.getNumBackfills());
    EXPECT_EQ(backfill_success, bfm.backfill());
    EXPECT_EQ(0, bfm.getNumBackfills());
}

/// Test that backfills are scheduled in sequential order when
/// "stream_backfill_order" is set to "sequential"
TEST_P(SingleThreadedActiveStreamTest, BackfillSequential) {
    // Delete initial stream (so we can re-create after items are only available
    // from disk.
    stream.reset();

    // Create on-disk items for three vBuckets. These will be used to backfill
    // from below.
    for (auto vbid : {Vbid{0}, Vbid{1}, Vbid{2}}) {
        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
        store_item(vbid, makeStoredDocKey("key1"), "value");
        store_item(vbid, makeStoredDocKey("key2"), "value");
        auto vb = engine->getVBucket(vbid);
        auto& manager = *vb->checkpointManager;

        // To ensure that a backfill is required, must ensure items are no
        // longer present in CheckpointManager. Achieve this by creating a
        // new checkpoint, flushing the (now-closed) one and removing it.
        const auto openId = manager.getOpenCheckpointId();
        ASSERT_GT(manager.createNewCheckpoint(), openId);
        flushVBucketToDiskIfPersistent(vbid, 2 /*expected_num_flushed*/);
        ASSERT_EQ(1, manager.getNumCheckpoints());
        ASSERT_EQ(0, manager.getNumOpenChkItems());
    }

    // Re-create producer now we have items only on disk, setting a scan buffer
    // which can only hold 1 item (so backfill doesn't complete a VB in one
    // scan).
    setupProducer({{"backfill_order", "sequential"}});
    producer->public_getBackfillScanBuffer().maxItems = 1;

    // setupProducer creates a stream for vb0. Also need streams for vb1 and
    // vb2.
    auto stream1 =
            std::make_shared<MockActiveStream>(engine.get(),
                                               producer,
                                               0,
                                               0 /*opaque*/,
                                               *engine->getVBucket(Vbid{1}));
    auto stream2 =
            std::make_shared<MockActiveStream>(engine.get(),
                                               producer,
                                               0,
                                               0 /*opaque*/,
                                               *engine->getVBucket(Vbid{2}));
    stream1->setActive();
    stream2->setActive();

    ASSERT_TRUE(stream->isBackfilling());
    ASSERT_TRUE(stream1->isBackfilling());
    ASSERT_TRUE(stream2->isBackfilling());

    // Test - Drive the BackfillManager forward. We expect to see:
    // 1. The snapshot marker from each vbucket
    // 2. All of the mutations from vb0
    // 3. All of the mutations from vb1
    // 4. All of the mutations from vb2
    auto& bfm = producer->getBFM();
    ASSERT_EQ(3, bfm.getNumBackfills());

    // 1. snapshot markers
    auto& readyQ0 = stream->public_readyQ();
    auto& readyQ1 = stream1->public_readyQ();
    auto& readyQ2 = stream2->public_readyQ();
    ASSERT_EQ(backfill_success, bfm.backfill());
    ASSERT_EQ(backfill_success, bfm.backfill());
    ASSERT_EQ(backfill_success, bfm.backfill());

    EXPECT_EQ(1, readyQ0.size());
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, readyQ0.back()->getEvent());
    EXPECT_EQ(1, readyQ1.size());
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, readyQ1.back()->getEvent());
    EXPECT_EQ(1, readyQ2.size());
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, readyQ2.back()->getEvent());

    // To drive a single vBucket's backfill to completion requires
    // 3 steps (scan() * number of items, completed) for persistent
    // and 2 for ephemeral.
    const int backfillSteps = persistent() ? 3 : 2;
    for (int i = 0; i < backfillSteps; i++) {
        ASSERT_EQ(backfill_success, bfm.backfill());
    }

    // 2. Verify that all of the first VB has now backfilled.
    EXPECT_EQ(3, readyQ0.size());
    EXPECT_EQ(DcpResponse::Event::Mutation, readyQ0.back()->getEvent());
    EXPECT_EQ(1, readyQ1.size());
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, readyQ1.back()->getEvent());
    EXPECT_EQ(1, readyQ2.size());
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, readyQ2.back()->getEvent());

    for (int i = 0; i < backfillSteps; i++) {
        ASSERT_EQ(backfill_success, bfm.backfill());
    }

    // 3. Verify that all of the second VB has now been backfilled.
    EXPECT_EQ(3, readyQ0.size());
    EXPECT_EQ(DcpResponse::Event::Mutation, readyQ0.back()->getEvent());
    EXPECT_EQ(3, readyQ1.size());
    EXPECT_EQ(DcpResponse::Event::Mutation, readyQ1.back()->getEvent());
    EXPECT_EQ(1, readyQ2.size());
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, readyQ2.back()->getEvent());

    for (int i = 0; i < backfillSteps; i++) {
        ASSERT_EQ(backfill_success, bfm.backfill());
    }

    // 4. Verify that all 3 VBs have now been backfilled.
    EXPECT_EQ(3, readyQ0.size());
    EXPECT_EQ(DcpResponse::Event::Mutation, readyQ0.back()->getEvent());
    EXPECT_EQ(3, readyQ1.size());
    EXPECT_EQ(DcpResponse::Event::Mutation, readyQ1.back()->getEvent());
    EXPECT_EQ(3, readyQ2.size());
    EXPECT_EQ(DcpResponse::Event::Mutation, readyQ2.back()->getEvent());

    ASSERT_EQ(backfill_finished, bfm.backfill());
}

/**
 * Unit test for MB-36146 to ensure that CheckpointCursor do not try to
 * use the currentCheckpoint member variable if its not point to a valid
 * object.
 *
 * 1. Create an item
 * 2. Create a new open checkpoint
 * 3. For persistent vbuckets flush data to disk to move all cursors to the
 * next checkpoint.
 * 4. Create a lamda function that will allow use to mimic the race condition
 * 5. Transition stream state to dead which will call removeCheckpointCursor()
 * 6. Once the CheckpointManager has removed all cursors to the checkpoint
 * call removeClosedUnrefCheckpoints() to delete the checkpoint in memory
 * 7. call getNumItemsForCursor() using the cursor we removed and make sure
 * we don't access the deleted memory
 */
TEST_P(SingleThreadedActiveStreamTest, MB36146) {
    auto vb = engine->getVBucket(vbid);
    auto& ckptMgr = *vb->checkpointManager;

    const auto key = makeStoredDocKey("key");
    const std::string value = "value";
    auto item = make_item(vbid, key, value);

    {
        auto cHandle = vb->lockCollections(item.getKey());
        EXPECT_EQ(cb::engine_errc::success,
                  vb->set(item, cookie, *engine, {}, cHandle));
    }

    // Need to ensure that checkpoints are removed only at some specific point
    // in the test - see lambda below
    const auto extraCursor =
            vb->checkpointManager
                    ->registerCursorBySeqno(
                            "cursor", 0, CheckpointCursor::Droppable::Yes)
                    .cursor.lock();
    ASSERT_TRUE(extraCursor);

    EXPECT_EQ(2, ckptMgr.createNewCheckpoint());

    if (persistent()) {
        flush_vbucket_to_disk(vbid);
    }

    ckptMgr.runGetItemsHook = [this, &ckptMgr, &extraCursor](
                                      const CheckpointCursor& cursor,
                                      Vbid vbid) {
        ASSERT_EQ(2, ckptMgr.getNumCheckpoints());
        std::vector<queued_item> items;
        store->getVBucket(vbid)->checkpointManager->getNextItemsForCursor(
                *extraCursor, items);
        ASSERT_EQ(1, ckptMgr.getNumCheckpoints());

        size_t numberOfItemsInCursor = 0;
        EXPECT_NO_THROW(numberOfItemsInCursor =
                                ckptMgr.getNumItemsForCursor(&cursor));
        EXPECT_EQ(0, numberOfItemsInCursor);
    };

    stream->transitionStateToDead();
}

TEST_P(SingleThreadedActiveStreamTest, BackfillSkipsScanIfStreamInWrongState) {
    auto vb = engine->getVBucket(vbid);
    auto& ckptMgr = *vb->checkpointManager;

    const auto key = makeStoredDocKey("key");
    const std::string value = "value";
    auto item = make_item(vbid, key, value);

    {
        auto cHandle = vb->lockCollections(item.getKey());
        EXPECT_EQ(cb::engine_errc::success,
                  vb->set(item, cookie, *engine, {}, cHandle));
    }
    EXPECT_EQ(2, ckptMgr.createNewCheckpoint());

    if (persistent()) {
        flush_vbucket_to_disk(vbid);
    }
    producer->closeStream(stream->getOpaque(), vbid, stream->getStreamId());
    stream.reset();
    ASSERT_EQ(1, vb->checkpointManager->getNumCheckpoints());

    auto& bfm = dynamic_cast<MockDcpBackfillManager&>(producer->getBFM());
    // Normal flow if stream in correct state
    {
        // confirm no backfills scheduled
        EXPECT_EQ(0, bfm.getNumBackfills());

        // creating the stream will schedule backfill
        recreateStream(*vb);

        EXPECT_EQ(backfill_success, bfm.backfill()); // init
        EXPECT_EQ(backfill_success, bfm.backfill()); // scan
        if (persistent()) {
            // Persistent buckets need more calls for each step,
            EXPECT_EQ(backfill_success, bfm.backfill()); // done
            EXPECT_EQ(backfill_finished, bfm.backfill()); // nothing else to do
        }
        EXPECT_EQ(0, bfm.getNumBackfills());

        producer->closeStream(stream->getOpaque(), vbid, stream->getStreamId());
        stream.reset();
    }

    // Test stream *not* in expected backfill state when creating the backfill
    {
        // confirm no backfills scheduled
        EXPECT_EQ(0, bfm.getNumBackfills());

        // creating the stream will schedule backfill
        recreateStream(*vb);

        stream->transitionStateToInMemory();

        EXPECT_EQ(backfill_success, bfm.backfill()); // init
        // scan is skipped
        EXPECT_EQ(backfill_success, bfm.backfill()); // completing
        if (persistent()) {
            EXPECT_EQ(backfill_finished, bfm.backfill()); // nothing else to do
        }
        EXPECT_EQ(0, bfm.getNumBackfills());
    }
}

/*
 * MB-31410: In this test I simulate a DcpConsumer that receives messages
 * while previous messages have been buffered. This simulates the system
 * when Replication Throttling triggers.
 * The purpose is to check that the Consumer can /never/ process new incoming
 * messages /before/ the DcpConsumerTask processes buffered messages.
 * Note that, while I implement this test by using out-of-order mutations, the
 * test covers a generic scenario where we try to process any kind of
 * out-of-order messages (e.g., mutations and snapshot-markers).
 */
TEST_P(SingleThreadedPassiveStreamTest, MB31410) {
    const std::string value(1024 * 1024, 'x');
    const uint64_t snapStart = 1;
    const uint64_t snapEnd = 100;
    // Run with 4% replication throttle (see commit for this test)
    engine->getEpStats().replicationThrottleThreshold = 0.04;

    // The consumer receives the snapshot-marker
    uint32_t opaque = 0;
    SnapshotMarker snapshotMarker(opaque,
                                  vbid,
                                  snapStart,
                                  snapEnd,
                                  dcp_marker_flag_t::MARKER_FLAG_MEMORY,
                                  {} /*HCS*/,
                                  {} /*maxVisibleSeqno*/,
                                  {}, // timestamp
                                  {});
    stream->processMarker(&snapshotMarker);

    // The consumer receives mutations.
    // Here I want to create the scenario where we have hit the replication
    // threshold.
    size_t seqno = snapStart;
    for (; seqno <= snapEnd; seqno++) {
        auto ret = stream->messageReceived(
                makeMutationConsumerMessage(seqno, vbid, value, opaque));

        // We get cb::engine_errc::temporary_failure when we hit the replication
        // threshold. When it happens, we buffer the mutation for deferred
        // processing in the DcpConsumerTask.
        if (ret == cb::engine_errc::temporary_failure) {
            auto& epStats = engine->getEpStats();

            ASSERT_GT(epStats.getEstimatedTotalMemoryUsed(),
                      epStats.getMaxDataSize() *
                              epStats.replicationThrottleThreshold);
            ASSERT_EQ(1, stream->getNumBufferItems());
            auto& bufferedMessages = stream->getBufferMessages();
            auto& [dcpResponse, size] = bufferedMessages.at(0);
            ASSERT_EQ(seqno,
                      *dynamic_cast<MutationResponse&>(*dcpResponse)
                               .getBySeqno());

            // Simulate that we have recovered from OOM.
            // We need this for processing other items in the next steps.
            epStats.setMaxDataSize(epStats.getMaxDataSize() * 2);
            ASSERT_LT(epStats.getEstimatedTotalMemoryUsed(),
                      epStats.getMaxDataSize() *
                              epStats.replicationThrottleThreshold);

            break;
        } else {
            ASSERT_EQ(cb::engine_errc::success, ret);
        }
    }

    // At this point 'seqno' has been buffered. So in the following:
    //     - I start frontEndThread where I try to process 'seqno + 1'
    //     - I simulate the DcpConsumerTask in this_thread by calling
    //         PassiveStream::processBufferedMessages
    ThreadGate tg(2);

    // Used to simulate the scenario where frontEndThread executes while the
    // DcpConsumerTask is draining the message buffer.
    struct {
        std::mutex m;
        std::condition_variable cv;
        bool frontEndDone = false;
    } sync;

    auto nextFrontEndSeqno = seqno + 1;
    auto frontEndTask =
            [this, nextFrontEndSeqno, &value, opaque, &tg, &sync]() {
                tg.threadUp();
                // If the following check fails it is enough to assert that the
                // test has failed. But, I use EXPECT rather than ASSERT
                // because, in the case of failure, I want to trigger also the
                // ASSERT_NO_THROW below.
                EXPECT_EQ(cb::engine_errc::temporary_failure,
                          stream->messageReceived(makeMutationConsumerMessage(
                                  nextFrontEndSeqno, vbid, value, opaque)));
                // I cannot check the status of the buffer here because we have
                // released buffer.bufMutex and the DcpConsumerTask has started
                // draining. That would give TSan errors on CV. I do the check
                // in the DcpConsumerTask (below).

                // Unblock DcpConsumerTask
                {
                    std::lock_guard<std::mutex> lg(sync.m);
                    sync.frontEndDone = true;
                }
                sync.cv.notify_one();
            };
    // I need to run start frontEndThread before this_thread calls
    // PassiveStream::processBufferedMessages. That's because this_thread
    // would block forever in tg.threadUp() otherwise.
    std::thread frontEndThread(frontEndTask);

    // When this_thread goes to sleep in the hook function, frontEndThread
    // executes and tries to process the new incoming message.
    // If frontEndThread succeeds, then it means that we have processed new
    // messages /before/ the buffered ones.
    // In the specific case (where we are processing out-of-order mutations
    // and the new incoming message in frontEndThread is 'seqno + 1') it means
    // that we are trying to break the seqno-invariant.
    // When this_thread resumes its execution, it will process the mutations
    // previously buffered. So, if frontEndThread has got
    // cb::engine_errc::success above, then this_thread will throw an exception
    // (Monotonic<x> invariant failed).
    std::set<int64_t> processedBufferSeqnos;
    bool isFirstRun = true;
    std::function<void()> hook =
            [this, &tg, &isFirstRun, nextFrontEndSeqno, &sync]() {
                // If the test succeeds (i.e., the frontEndTask above sees
                // cb::engine_errc::temporary_failure) we will have 2 buffered
                // messages, so we will execute here twice. Calling tg.threadUp
                // again would lead to deadlock.
                if (!tg.isComplete()) {
                    tg.threadUp();
                }

                // Let the frontEndThread complete its execution.
                //
                // Note: There are many logic checks in this test that aim to
                //     both:
                //     1) ensuring that the test is valid
                //     2) ensuring that our logic works properly
                //     The problem is: if the test fails, then we are sure that
                //     our logic is broken; but, if the test doesn't fail we can
                //     assert that our logic is safe only if the test is valid.
                //     We may have a false negative otherwise.
                //     This test is valid only if frontEndThread has completed
                //     its execution at this point. Even if the logic checks
                //     seems enough to ensure that, the test is complex and I
                //     may have forgot something. Also, we are back-porting
                //     this patch to versions where logic conditions differ.
                //     So, here I enforce a strong sync-condition so that we are
                //     always sure that frontEndThread has completed before
                //     we proceed.
                {
                    std::unique_lock<std::mutex> ul(sync.m);
                    sync.cv.wait(ul, [&sync] { return sync.frontEndDone; });
                }

                // Check the status of the buffer before draining. Here the
                // state must be the one left by the frontEndThread. Note that
                // we have released buffer.bufMutex here. But, accessing the
                // buffer is safe as:
                // - test is designed so that we must have buffered 2 items
                // - no further front-end message will be processed/buffered
                //     at this point
                // - only this thread can remove messages from the buffer
                if (isFirstRun) {
                    auto numBufferedItems = stream->getNumBufferItems();
                    // Again, avoid that we fail with ASSERT_EQ or
                    // std::out_of_range so that this_thread proceeds and
                    // throws.
                    EXPECT_EQ(2, numBufferedItems);
                    if (numBufferedItems == 2) {
                        auto& bufferedMessages = stream->getBufferMessages();
                        auto& dcpResponse0 = bufferedMessages.at(0);
                        EXPECT_EQ(nullptr, dcpResponse0.first);
                        auto& dcpResponse1 = bufferedMessages.at(1);
                        EXPECT_EQ(nextFrontEndSeqno,
                                  *dynamic_cast<MutationResponse&>(
                                           *dcpResponse1.first)
                                           .getBySeqno());
                    }

                    isFirstRun = false;
                }
            };
    stream->setProcessBufferedMessages_postFront_Hook(hook);

    // If the seqno-invariant is broken, the next call throws:
    //     C++ exception with description "Monotonic<x> invariant failed:
    //     new value (<seqno>) breaks invariant on current value
    //     (<nextFrontEndSeqno>)" thrown in the test body.
    uint32_t bytesProcessed{0};
    ASSERT_NO_THROW(EXPECT_EQ(all_processed,
                              stream->processBufferedMessages(
                                      bytesProcessed, 100 /*batchSize*/)));
    EXPECT_GT(bytesProcessed, 0);

    frontEndThread.join();

    // Explicitly verify the order of mutations in the CheckpointManager.
    auto vb = store->getVBuckets().getBucket(vbid);
    auto* ckptMgr =
            static_cast<MockCheckpointManager*>(vb->checkpointManager.get());
    ASSERT_TRUE(ckptMgr);
    std::vector<queued_item> items;
    ckptMgr->getNextItemsForPersistence(items);
    // Expect two checkpoints one empty initial checkpoint which was closed due
    // to the snapshot being received and the open checkpoint
    EXPECT_EQ(1, ckptMgr->getNumCheckpoints());
    EXPECT_EQ(nextFrontEndSeqno, items.back()->getBySeqno());
    uint64_t prevSeqno = 0;
    for (auto& item : items) {
        if (item->isCheckPointMetaItem()) {
            continue;
        }
        ASSERT_EQ(queue_op::mutation, item->getOperation());
        EXPECT_GT(item->getBySeqno(), prevSeqno);
        prevSeqno = item->getBySeqno();
    }
}

// Main test code for MB-33773, see TEST_F for details of each mode.
// The test generally forces the consumer to buffer mutations and then
// interleaves various operations using ProcessBufferedMessages_postFront_Hook
void SingleThreadedPassiveStreamTest::mb_33773(
        SingleThreadedPassiveStreamTest::mb_33773Mode mode) {
    uint32_t opaque = 1;

    const uint64_t snapStart = 1;
    const uint64_t snapEnd = 100;

    // The consumer receives the snapshot-marker
    consumer->snapshotMarker(opaque,
                             vbid,
                             snapStart,
                             snapEnd,
                             dcp_marker_flag_t::MARKER_FLAG_MEMORY,
                             {} /*HCS*/,
                             {} /*maxVisibleSeqno*/);

    // This code is tricking the replication throttle into returning pause so
    // that the mutation's are buffered.
    auto& stats = engine->getEpStats();
    stats.replicationThrottleThreshold = 0;
    const size_t originalQuota = stats.getMaxDataSize();
    size_t expectedFlowControlBytes =
            SnapshotMarkerResponse::baseMsgBytes +
            sizeof(cb::mcbp::request::DcpSnapshotMarkerV1Payload);
    stats.setMaxDataSize(1);
    ASSERT_EQ(ReplicationThrottle::Status::Pause,
              engine->getReplicationThrottle().getStatus());

    // Push mutations
    EXPECT_EQ(0, stream->getNumBufferItems());
    for (size_t seqno = snapStart; seqno < snapEnd; seqno++) {
        auto key = makeStoredDocKey("k" + std::to_string(seqno));
        EXPECT_EQ(cb::engine_errc::success,
                  consumer->mutation(opaque,
                                     key,
                                     {},
                                     0,
                                     0,
                                     0,
                                     vbid,
                                     0,
                                     seqno,
                                     0,
                                     0,
                                     0,
                                     {},
                                     0));
        expectedFlowControlBytes +=
                MutationResponse::mutationBaseMsgBytes + key.size();
    }
    // and check they were buffered.
    ASSERT_EQ(snapEnd - snapStart, stream->getNumBufferItems());
    // Unblock consumer
    stats.replicationThrottleThreshold = 99;
    stats.setMaxDataSize(originalQuota);

    // We expect flowcontrol bytes to increase when the buffered items are
    // discarded.
    auto bytes = consumer->getFlowControl().getFreedBytes();
    auto backoffs = consumer->getNumBackoffs();
    size_t flowControlBytesFreed = 0; // this is used for one test only
    switch (mode) {
    case mb_33773Mode::closeStreamOnTask: {
        // Create and set a hook that will call setDead, the hook executes
        // just after an item has been taken from the buffer
        std::function<void()> hook = [this]() {
            consumer->closeStreamDueToVbStateChange(vbid, vbucket_state_active);
        };
        stream->setProcessBufferedMessages_postFront_Hook(hook);
        break;
    }
    case mb_33773Mode::closeStreamBeforeTask:
        consumer->closeStreamDueToVbStateChange(vbid, vbucket_state_active);
        break;
    case mb_33773Mode::noMemory: {
        // Fudge memory again so the task has to re-buffer the messages
        std::function<void()> hook = [this]() {
            engine->getEpStats().setMaxDataSize(1);
        };
        stream->setProcessBufferedMessages_postFront_Hook(hook);
        break;
    }
    case mb_33773Mode::noMemoryAndClosed: {
        // This hook will force quota to 1 so the processing fails.
        // But also closes the stream so that the messages queue is emptied.
        // We are testing that the item we've moved out of the queue is still
        // accounted in flow-control
        std::function<void()> hook = [this, &flowControlBytesFreed]() {
            engine->getEpStats().setMaxDataSize(1);
            consumer->closeStreamDueToVbStateChange(vbid, vbucket_state_active);
            // Capture flow control freed bytes which should now include all
            // buffered messages, except one (which was moved)
            flowControlBytesFreed = consumer->getFlowControl().getFreedBytes();
        };
        stream->setProcessBufferedMessages_postFront_Hook(hook);
        break;
    }
    }

    // Run the NonIO task. Without any fix (and in the interleaved test) the
    // task will grab a reference to an object which will be freed as a side
    // affect of calling closeStream. Crash/ASAN failure will occur.
    auto& nonIo = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
    runNextTask(nonIo);

    switch (mode) {
    case mb_33773Mode::closeStreamOnTask:
    case mb_33773Mode::closeStreamBeforeTask:
        // Expect that after running the task, which closed the stream via the
        // hook flow control freed increased to reflect the buffered items which
        // were discarded,
        EXPECT_GT(consumer->getFlowControl().getFreedBytes(), bytes);
        // All pushed items will have been 'acked'
        EXPECT_EQ(expectedFlowControlBytes,
                  consumer->getFlowControl().getFreedBytes());
        return;
    case mb_33773Mode::noMemory: {
        std::function<void()> hook = [] {};
        stream->setProcessBufferedMessages_postFront_Hook(hook);
        // fall through to next case
    }
    case mb_33773Mode::noMemoryAndClosed: {
        // Undo memory fudge for the rest of the test
        engine->getEpStats().setMaxDataSize(originalQuota);
        break;
    }
    }

    // NOTE: Only the noMemory test runs from here

    // backoffs should of increased
    EXPECT_GT(consumer->getNumBackoffs(), backoffs);

    if (mode == mb_33773Mode::noMemoryAndClosed) {
        // Check the hook updated this counter
        EXPECT_NE(0, flowControlBytesFreed);
        // And check that consumer flow control is even bigger now
        EXPECT_GT(consumer->getFlowControl().getFreedBytes(),
                  flowControlBytesFreed);
        // All pushed items will have been 'acked'
        EXPECT_EQ(expectedFlowControlBytes,
                  consumer->getFlowControl().getFreedBytes());
    } else {
        // The items are still buffered
        EXPECT_EQ(snapEnd - snapStart, stream->getNumBufferItems());
        // Run task again, it should of re-scheduled itself
        runNextTask(nonIo);
        // and all items now gone
        EXPECT_EQ(0, stream->getNumBufferItems());
    }
}

// MB-35061 - Check that closing a stream and opening a new one does not leave
// multiple entries for the same consumer in vbConns for a particular vb.
TEST_P(SingleThreadedPassiveStreamTest,
       ConsumerRemovedFromVBConnsWhenStreamReplaced) {
    auto& connMap = static_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    std::string streamName = "test_consumer";
    // consumer and stream created in SetUp
    ASSERT_TRUE(connMap.doesVbConnExist(vbid, streamName));

    // close stream
    EXPECT_EQ(cb::engine_errc::success, consumer->closeStream(0, vbid));

    EXPECT_TRUE(connMap.doesVbConnExist(vbid, streamName));

    // add new stream
    uint32_t opaque = 999;
    ASSERT_EQ(cb::engine_errc::success,
              consumer->addStream(opaque /*opaque*/, vbid, 0 /*flags*/));
    stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());

    ASSERT_TRUE(stream);
    EXPECT_TRUE(connMap.doesVbConnExist(vbid, streamName));

    // end the second stream
    EXPECT_EQ(cb::engine_errc::success,
              consumer->streamEnd(stream->getOpaque(),
                                  vbid,
                                  cb::mcbp::DcpStreamEndStatus::Ok));

    // expect the consumer is no longer in vbconns
    EXPECT_FALSE(connMap.doesVbConnExist(vbid, streamName));

    // re-add stream for teardown to close
    ASSERT_EQ(cb::engine_errc::success,
              consumer->addStream(opaque /*opaque*/, vbid, 0 /*flags*/));
}

// Do mb33773 with the close stream interleaved into the processBufferedMessages
// This is more reflective of the actual MB as this case would result in a fault
TEST_P(SingleThreadedPassiveStreamTest, MB_33773_interleaved) {
    mb_33773(mb_33773Mode::closeStreamOnTask);
}

// Do mb33773 with the close stream before processBufferedMessages. This is
// checking that flow-control is updated with the fix in place
TEST_P(SingleThreadedPassiveStreamTest, MB_33773) {
    mb_33773(mb_33773Mode::closeStreamBeforeTask);
}

// Test more of the changes in mb33773, this mode makes the processing fail
// because there's not enough memory, this makes us exercise the code that swaps
// a reponse back into the deque
TEST_P(SingleThreadedPassiveStreamTest, MB_33773_oom) {
    mb_33773(mb_33773Mode::noMemory);
}

// Test more of the changes in mb33773, this mode makes the processing fail
// because there's not enough memory, this makes us exercise the code that swaps
// a reponse back into the deque
TEST_P(SingleThreadedPassiveStreamTest, MB_33773_oom_close) {
    mb_33773(mb_33773Mode::noMemoryAndClosed);
}

void SingleThreadedPassiveStreamTest::
        testInitialDiskSnapshotFlagClearedOnTransitionToActive(
                vbucket_state_t initialState) {
    // Test that a vbucket changing state to active clears the initial disk
    // snapshot flag
    setVBucketStateAndRunPersistTask(vbid, initialState);

    // receive snapshot
    SnapshotMarker marker(0 /*opaque*/,
                          vbid,
                          1 /*snapStart*/,
                          100 /*snapEnd*/,
                          dcp_marker_flag_t::MARKER_FLAG_DISK | MARKER_FLAG_CHK,
                          0 /*HCS*/,
                          {} /*maxVisibleSeqno*/,
                          {}, // timestamp
                          {} /*streamId*/);

    stream->processMarker(&marker);

    auto vb = engine->getVBucket(vbid);
    ASSERT_TRUE(vb->isReceivingInitialDiskSnapshot());
    ASSERT_TRUE(stream->isActive());

    // set stream to dead - modelling stream being unexpectedly "disconnected"
    stream->setDead(cb::mcbp::DcpStreamEndStatus::Disconnected);
    ASSERT_FALSE(stream->isActive());

    // flag not cleared yet, the replica might reconnect to the active, don't
    // want to momentarily clear the flag
    EXPECT_TRUE(vb->isReceivingInitialDiskSnapshot());

    // change state
    setVBucketState(vbid, vbucket_state_active);
    flushVBucketToDiskIfPersistent(vbid, 0);

    // check that the initial disk snapshot flag was cleared
    EXPECT_FALSE(vb->isReceivingInitialDiskSnapshot());
}

TEST_P(SingleThreadedPassiveStreamTest,
       InitialDiskSnapshotFlagClearedOnStateTransition_Pending) {
    testInitialDiskSnapshotFlagClearedOnTransitionToActive(
            vbucket_state_pending);
}

TEST_P(SingleThreadedPassiveStreamTest,
       InitialDiskSnapshotFlagClearedOnStateTransition_Replica) {
    testInitialDiskSnapshotFlagClearedOnTransitionToActive(
            vbucket_state_replica);
}

/**
 * Note: this test does not cover any issue, it just shows what happens at
 * Replica if the Active misses to set the MARKER_FLAG_CHK in SnapshotMarker.
 */
TEST_P(SingleThreadedPassiveStreamTest, ReplicaNeverMergesDiskSnapshot) {
    auto vb = engine->getVBucket(vbid);
    ASSERT_TRUE(vb);
    auto& ckptMgr = static_cast<MockCheckpointManager&>(*vb->checkpointManager);
    ckptMgr.clear(0 /*seqno*/);
    ASSERT_EQ(1, ckptMgr.getNumCheckpoints());
    ASSERT_EQ(CheckpointType::Memory, ckptMgr.getOpenCheckpointType());

    const uint32_t opaque = 0;
    const auto receiveSnapshot =
            [this, opaque, &ckptMgr](
                    uint64_t snapStart,
                    uint64_t snapEnd,
                    uint32_t flags,
                    size_t expectedNumCheckpoint,
                    CheckpointType expectedOpenCkptType) -> void {
        cb::mcbp::DcpStreamId streamId{};
        SnapshotMarker marker(opaque,
                              vbid,
                              snapStart,
                              snapEnd,
                              flags,
                              0 /*HCS*/,
                              {} /*maxVisibleSeqno*/,
                              {}, // timestamp
                              streamId);
        stream->processMarker(&marker);

        auto item = makeCommittedItem(makeStoredDocKey("key"), "value");
        item->setBySeqno(snapStart);

        EXPECT_EQ(cb::engine_errc::success,
                  stream->messageReceived(
                          std::make_unique<MutationConsumerMessage>(
                                  std::move(item),
                                  opaque,
                                  IncludeValue::Yes,
                                  IncludeXattrs::Yes,
                                  IncludeDeleteTime::No,
                                  IncludeDeletedUserXattrs::Yes,
                                  DocKeyEncodesCollectionId::No,
                                  nullptr /*ext-metadata*/,
                                  streamId)));

        EXPECT_EQ(expectedNumCheckpoint, ckptMgr.getNumCheckpoints());
        EXPECT_EQ(expectedOpenCkptType, ckptMgr.getOpenCheckpointType());
    };
    auto initalNumberOfCheckpoints = vb->checkpointManager->getNumCheckpoints();
    {
        SCOPED_TRACE("");
        receiveSnapshot(1 /*snapStart*/,
                        1 /*snapEnd*/,
                        dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
                        initalNumberOfCheckpoints + 1 /*expectedNumCheckpoint*/,
                        CheckpointType::Memory /*expectedOpenCkptType*/);
    }

    // Merged with the previous snapshot
    {
        SCOPED_TRACE("");
        receiveSnapshot(2 /*snapStart*/,
                        2 /*snapEnd*/,
                        dcp_marker_flag_t::MARKER_FLAG_MEMORY,
                        initalNumberOfCheckpoints + 1 /*expectedNumCheckpoint*/,
                        CheckpointType::Memory /*expectedOpenCkptType*/);
    }

    // Disk + we miss the MARKER_FLAG_CHK, still not merged
    {
        SCOPED_TRACE("");
        receiveSnapshot(3 /*snapStart*/,
                        3 /*snapEnd*/,
                        dcp_marker_flag_t::MARKER_FLAG_DISK,
                        initalNumberOfCheckpoints + 2 /*expectedNumCheckpoint*/,
                        CheckpointType::Disk /*expectedOpenCkptType*/);
    }

    {
        SCOPED_TRACE("");
        receiveSnapshot(4 /*snapStart*/,
                        4 /*snapEnd*/,
                        dcp_marker_flag_t::MARKER_FLAG_DISK | MARKER_FLAG_CHK,
                        initalNumberOfCheckpoints + 3 /*expectedNumCheckpoint*/,
                        CheckpointType::Disk /*expectedOpenCkptType*/);
    }

    // From Disk to Disk + we miss the MARKER_FLAG_CHK, still not merged
    {
        SCOPED_TRACE("");
        receiveSnapshot(5 /*snapStart*/,
                        5 /*snapEnd*/,
                        dcp_marker_flag_t::MARKER_FLAG_DISK,
                        initalNumberOfCheckpoints + 4 /*expectedNumCheckpoint*/,
                        CheckpointType::Disk /*expectedOpenCkptType*/);
    }

    // Memory snap but previous snap is Disk -> no merge
    {
        SCOPED_TRACE("");
        receiveSnapshot(6 /*snapStart*/,
                        6 /*snapEnd*/,
                        dcp_marker_flag_t::MARKER_FLAG_MEMORY,
                        initalNumberOfCheckpoints + 5 /*expectedNumCheckpoint*/,
                        CheckpointType::Memory /*expectedOpenCkptType*/);
    }

    {
        SCOPED_TRACE("");
        receiveSnapshot(7 /*snapStart*/,
                        7 /*snapEnd*/,
                        dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
                        initalNumberOfCheckpoints + 6 /*expectedNumCheckpoint*/,
                        CheckpointType::Memory /*expectedOpenCkptType*/);
    }
}

void SingleThreadedPassiveStreamTest::testConsumerRejectsBodyInDeletion(
        const std::optional<cb::durability::Requirements>& durReqs) {
    auto& connMap = static_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    connMap.addConn(cookie, consumer);
    ASSERT_TRUE(consumer->isAllowSanitizeValueInDeletion());
    engine->getConfiguration().setAllowSanitizeValueInDeletion(false);
    ASSERT_FALSE(consumer->isAllowSanitizeValueInDeletion());

    consumer->public_setIncludeDeletedUserXattrs(IncludeDeletedUserXattrs::Yes);

    // Send deletion in a single seqno snapshot
    EXPECT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(1 /*opaque*/,
                                       vbid,
                                       1 /*startSeqno*/,
                                       1 /*endSeqno*/,
                                       MARKER_FLAG_CHK,
                                       {} /*HCS*/,
                                       {} /*maxVisibleSeqno*/));

    const auto verifyDCPFailure =
            [this, &durReqs](const cb::const_byte_buffer& value,
                             protocol_binary_datatype_t datatype) -> void {
        const uint32_t opaque = 1;
        int64_t bySeqno = 1;
        if (durReqs) {
            EXPECT_EQ(cb::engine_errc::invalid_arguments,
                      consumer->prepare(opaque,
                                        {"key", DocKeyEncodesCollectionId::No},
                                        value,
                                        0 /*priv_bytes*/,
                                        datatype,
                                        0 /*cas*/,
                                        vbid,
                                        0 /*flags*/,
                                        bySeqno,
                                        0 /*revSeqno*/,
                                        0 /*exp*/,
                                        0 /*lockTime*/,
                                        0 /*nru*/,
                                        DocumentState::Deleted,
                                        durReqs->getLevel()));
        } else {
            EXPECT_EQ(cb::engine_errc::invalid_arguments,
                      consumer->deletion(opaque,
                                         {"key", DocKeyEncodesCollectionId::No},
                                         value,
                                         0 /*priv_bytes*/,
                                         datatype,
                                         0 /*cas*/,
                                         vbid,
                                         bySeqno,
                                         0 /*revSeqno*/,
                                         {} /*meta*/));
        }
    };

    // Build up a value with just raw body and verify DCP failure
    const std::string body = "body";
    cb::const_byte_buffer value{reinterpret_cast<const uint8_t*>(body.data()),
                                body.size()};
    {
        SCOPED_TRACE("");
        verifyDCPFailure(value, PROTOCOL_BINARY_RAW_BYTES);
    }

    // Verify the same for body + xattrs
    const auto xattrValue = createXattrValue(body);
    value = {reinterpret_cast<const uint8_t*>(xattrValue.data()),
             xattrValue.size()};
    {
        SCOPED_TRACE("");
        verifyDCPFailure(
                value,
                PROTOCOL_BINARY_RAW_BYTES | PROTOCOL_BINARY_DATATYPE_XATTR);
    }

    connMap.removeConn(cookie);
}

TEST_P(SingleThreadedPassiveStreamTest, ConsumerRejectsBodyInDeletion) {
    testConsumerRejectsBodyInDeletion({});
}

TEST_P(SingleThreadedPassiveStreamTest, ConsumerRejectsBodyInSyncDeletion) {
    testConsumerRejectsBodyInDeletion(cb::durability::Requirements());
}

void SingleThreadedPassiveStreamTest::testConsumerSanitizesBodyInDeletion(
        const std::optional<cb::durability::Requirements>& durReqs) {
    ASSERT_TRUE(consumer->isAllowSanitizeValueInDeletion());
    consumer->public_setIncludeDeletedUserXattrs(IncludeDeletedUserXattrs::Yes);

    auto& vb = *store->getVBucket(vbid);
    ASSERT_EQ(0, vb.getHighSeqno());

    // If using durability, we'll send a second snapshot (as prepare's and
    // commits of the same key can't be in the same checkpoint) so just set this
    // snapshot's range for the prepare
    const uint64_t initialEndSeqno = durReqs ? 1 : 10;
    EXPECT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(1 /*opaque*/,
                                       vbid,
                                       1 /*startSeqno*/,
                                       initialEndSeqno,
                                       MARKER_FLAG_CHK,
                                       {} /*HCS*/,
                                       {} /*maxVisibleSeqno*/));

    const auto key = makeStoredDocKey("key");
    const auto verifyDCPSuccess = [this, &key, &durReqs](
                                          const cb::const_byte_buffer& value,
                                          protocol_binary_datatype_t datatype,
                                          int64_t bySeqno) -> void {
        const uint32_t opaque = 1;
        if (durReqs) {
            EXPECT_EQ(cb::engine_errc::success,
                      consumer->prepare(opaque,
                                        key,
                                        value,
                                        0 /*priv_bytes*/,
                                        datatype,
                                        0 /*cas*/,
                                        vbid,
                                        0 /*flags*/,
                                        bySeqno,
                                        0 /*revSeqno*/,
                                        0 /*exp*/,
                                        0 /*lockTime*/,
                                        0 /*nru*/,
                                        DocumentState::Deleted,
                                        durReqs->getLevel()));
        } else {
            EXPECT_EQ(cb::engine_errc::success,
                      consumer->deletion(opaque,
                                         key,
                                         value,
                                         0 /*priv_bytes*/,
                                         datatype,
                                         0 /*cas*/,
                                         vbid,
                                         bySeqno,
                                         0 /*revSeqno*/,
                                         {} /*meta*/));
        }
    };

    // Build up a value with just raw body and verify that DCP deletion succeeds
    // and the Body has been removed from the payload.
    const std::string body = "body";
    cb::const_byte_buffer value{reinterpret_cast<const uint8_t*>(body.data()),
                                body.size()};
    {
        SCOPED_TRACE("");
        verifyDCPSuccess(value, PROTOCOL_BINARY_RAW_BYTES, 1 /*bySeqno*/);
    }
    auto& ht = vb.ht;
    {
        auto res = ht.findForUpdate(key);
        const auto* sv = durReqs ? res.pending.getSV() : res.committed;
        EXPECT_TRUE(sv);
        EXPECT_EQ(1, sv->getBySeqno());
        if (durReqs) {
            EXPECT_EQ(0, sv->getValue()->valueSize());
        } else {
            // Note: Normal deletion with 0-size value goes through DelWithMeta
            // that sets the value to nullptr.
            EXPECT_FALSE(sv->getValue().get());
        }
    }

    int64_t nextSeqno = 2;
    if (durReqs) {
        // Need to commit the first prepare for queuing a new one in the next
        // steps.
        EXPECT_EQ(cb::engine_errc::success,
                  vb.commit(key, 1, {}, vb.lockCollections(key)));
        // Replica doesn't like 2 prepares for the same key into the same
        // checkpoint.
        const int64_t newStartSeqno = initialEndSeqno + 2;
        EXPECT_EQ(cb::engine_errc::success,
                  consumer->snapshotMarker(1 /*opaque*/,
                                           vbid,
                                           newStartSeqno,
                                           newStartSeqno + 10 /*endSeqno*/,
                                           MARKER_FLAG_CHK,
                                           {} /*HCS*/,
                                           {} /*maxVisibleSeqno*/));
        nextSeqno = newStartSeqno;
    }

    // Verify the same for body + user-xattrs + sys-xattrs
    const auto xattrValue = createXattrValue(body);
    value = {reinterpret_cast<const uint8_t*>(xattrValue.data()),
             xattrValue.size()};
    {
        SCOPED_TRACE("");
        verifyDCPSuccess(
                value,
                PROTOCOL_BINARY_RAW_BYTES | PROTOCOL_BINARY_DATATYPE_XATTR,
                nextSeqno);
    }
    {
        auto res = ht.findForUpdate(key);
        const auto* sv = durReqs ? res.pending.getSV() : res.committed;
        EXPECT_TRUE(sv);
        EXPECT_EQ(nextSeqno, sv->getBySeqno());
        EXPECT_TRUE(sv->getValue().get());
        EXPECT_LT(sv->getValue()->valueSize(), xattrValue.size());

        // No body
        const auto finalValue =
                std::string_view(const_cast<char*>(sv->getValue()->getData()),
                                 sv->getValue()->valueSize());
        EXPECT_EQ(0, cb::xattr::get_body_size(sv->getDatatype(), finalValue));

        // Must have user/sys xattrs (created at createXattrValue())
        const auto finalValueBuf =
                cb::char_buffer(const_cast<char*>(sv->getValue()->getData()),
                                sv->getValue()->valueSize());
        cb::xattr::Blob blob(finalValueBuf, false /*compressed*/);
        for (uint8_t i = 1; i <= 6; ++i) {
            EXPECT_FALSE(blob.get("ABCuser" + std::to_string(i)).empty());
        }
        EXPECT_FALSE(blob.get("meta").empty());
        EXPECT_FALSE(blob.get("_sync").empty());
    }
}

TEST_P(SingleThreadedPassiveStreamTest, ConsumerSanitizesBodyInDeletion) {
    testConsumerSanitizesBodyInDeletion({});
}

TEST_P(SingleThreadedPassiveStreamTest, ConsumerSanitizesBodyInSyncDeletion) {
    testConsumerSanitizesBodyInDeletion(cb::durability::Requirements());
}

void SingleThreadedPassiveStreamTest::testConsumerReceivesUserXattrsInDelete(
        bool sysXattrs,
        const std::optional<cb::durability::Requirements>& durReqs,
        bool compressed) {
    // UserXattrs in deletion are valid only for connections that enable it
    consumer->public_setIncludeDeletedUserXattrs(IncludeDeletedUserXattrs::Yes);

    // Send deletion in a single seqno snapshot
    const uint32_t opaque = 1;
    int64_t bySeqno = 1;
    EXPECT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(opaque,
                                       vbid,
                                       bySeqno,
                                       bySeqno,
                                       MARKER_FLAG_CHK,
                                       {} /*HCS*/,
                                       {} /*maxVisibleSeqno*/));

    // Build up a value composed of:
    // - no body
    // - some user-xattrs ("ABCUser[1..6]" + "meta")
    // - maybe the "_sync" sys-xattr
    auto value = createXattrValue("", sysXattrs, compressed);
    cb::const_byte_buffer valueBuf{
            reinterpret_cast<const uint8_t*>(value.data()), value.size()};
    auto datatype = PROTOCOL_BINARY_DATATYPE_XATTR;
    if (compressed) {
        datatype |= PROTOCOL_BINARY_DATATYPE_SNAPPY;
    }

    if (durReqs) {
        EXPECT_EQ(cb::engine_errc::success,
                  consumer->prepare(opaque,
                                    {"key", DocKeyEncodesCollectionId::No},
                                    valueBuf,
                                    0 /*priv_bytes*/,
                                    datatype,
                                    0 /*cas*/,
                                    vbid,
                                    0 /*flags*/,
                                    bySeqno,
                                    0 /*revSeqno*/,
                                    0 /*exp*/,
                                    0 /*lockTime*/,
                                    0 /*nru*/,
                                    DocumentState::Deleted,
                                    durReqs->getLevel()));
    } else {
        EXPECT_EQ(cb::engine_errc::success,
                  consumer->deletion(opaque,
                                     {"key", DocKeyEncodesCollectionId::No},
                                     valueBuf,
                                     0 /*priv_bytes*/,
                                     datatype,
                                     0 /*cas*/,
                                     vbid,
                                     bySeqno,
                                     0 /*revSeqno*/,
                                     {} /*meta*/));
    }

    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    EXPECT_EQ(FlushResult(MoreAvailable::No, 1), epBucket.flushVBucket(vbid));

    // Check item persisted

    auto& kvstore = *store->getRWUnderlying(vbid);
    const auto isPrepare = durReqs.has_value();
    auto doc = kvstore.get(makeDiskDocKey("key", isPrepare), vbid);
    EXPECT_EQ(cb::engine_errc::success, doc.getStatus());
    ASSERT_TRUE(doc.item);
    EXPECT_TRUE(doc.item->isDeleted());

    if (durReqs) {
        EXPECT_EQ(CommittedState::Pending, doc.item->getCommitted());
    } else {
        EXPECT_EQ(CommittedState::CommittedViaMutation,
                  doc.item->getCommitted());
    }

    ASSERT_EQ(datatype, doc.item->getDataType());
    const auto* data = doc.item->getData();
    const auto nBytes = doc.item->getNBytes();

    // Checkout on-disk value

    // No body
    ASSERT_EQ(0,
              cb::xattr::get_body_size(
                      datatype,
                      std::string_view(const_cast<char*>(data), nBytes)));

    // Must have user-xattrs
    cb::xattr::Blob blob(cb::char_buffer(const_cast<char*>(data), nBytes),
                         compressed);
    for (uint8_t i = 1; i <= 6; ++i) {
        EXPECT_FALSE(blob.get("ABCuser" + std::to_string(i)).empty());
    }
    EXPECT_FALSE(blob.get("meta").empty());

    if (sysXattrs) {
        EXPECT_FALSE(blob.get("_sync").empty());
    } else {
        EXPECT_TRUE(blob.get("_sync").empty());
    }
}

TEST_P(SingleThreadedPassiveStreamTest, ConsumerReceivesUserXattrsInDelete) {
    testConsumerReceivesUserXattrsInDelete(true, {});
}

TEST_P(SingleThreadedPassiveStreamTest,
       ConsumerReceivesUserXattrsInDelete_NoSysXattr) {
    testConsumerReceivesUserXattrsInDelete(false, {});
}

TEST_P(SingleThreadedPassiveStreamTest,
       ConsumerReceivesUserXattrsInSyncDelete) {
    testConsumerReceivesUserXattrsInDelete(true,
                                           cb::durability::Requirements());
}

TEST_P(SingleThreadedPassiveStreamTest,
       ConsumerReceivesUserXattrsInSyncDelete_NoSysXattr) {
    testConsumerReceivesUserXattrsInDelete(false,
                                           cb::durability::Requirements());
}

TEST_P(SingleThreadedPassiveStreamTest,
       ConsumerReceivesUserXattrsInDelete_Compressed) {
    testConsumerReceivesUserXattrsInDelete(true, {}, true);
}

TEST_P(SingleThreadedPassiveStreamTest,
       ConsumerReceivesUserXattrsInSyncDelete_Compressed) {
    testConsumerReceivesUserXattrsInDelete(
            true, cb::durability::Requirements(), true);
}

TEST_P(SingleThreadedPassiveStreamTest, ConsumerHandlesSeqnoAckResponse) {
    cb::mcbp::Response resp{};
    resp.setMagic(cb::mcbp::Magic::AltClientResponse);
    resp.setOpcode(cb::mcbp::ClientOpcode::DcpSeqnoAcknowledged);
    resp.setStatus(cb::mcbp::Status::NotMyVbucket);
    EXPECT_TRUE(consumer->handleResponse(resp));
}

/**
 * @todo: Spotted invalid during the work for MB-51295.
 * The test expects that, before the related fix, it fails as some queued items
 * is removed from checkpoints during a small time window between cursor-drop
 * and cursor re-registering when backfill is setup. That item removal never
 * happens in the test though.
 *
 * Convert to eager checkpoint removal (if possible) and re-enable.
 */
TEST_P(SingleThreadedActiveStreamTest,
       DISABLED_CursorReregisteredBeforeBackfillAfterCursorDrop) {
    // MB-37150: test that, after cursor dropping, cursors are registered before
    // checking whether to backfill. This ensures that checkpoints cannot be
    // removed/expelled from _after_ determining the backfill range, but before
    // registering the cursor.
    auto& vb = *engine->getVBucket(vbid);
    auto& cm = *vb.checkpointManager;

    producer->createCheckpointProcessorTask();

    stream = producer->mockActiveStreamRequest(0,
                                               /*opaque*/ 0,
                                               vb,
                                               /*st_seqno*/ 0,
                                               /*en_seqno*/ ~0,
                                               /*vb_uuid*/ 0xabcd,
                                               /*snap_start_seqno*/ 0,
                                               /*snap_end_seqno*/ ~0);

    auto key1 = makeStoredDocKey("key1");
    auto key2 = makeStoredDocKey("key2");
    // Store Mutation
    auto mutation = store_item(vbid, key1, "value");
    cm.createNewCheckpoint();
    auto mutation2 = store_item(vbid, key2, "value");

    // no items to backfill when created, stream will have transitioned to in
    // memory
    EXPECT_EQ(ActiveStream::StreamState::InMemory, stream->getState());

    stream->handleSlowStream();

    producer->setBeforeScheduleBackfillCB(
            [& stream = stream](uint64_t backfillEnd) {
                // check cursor exists before backfill is registered
                auto cursor = stream->getCursor().lock();
                EXPECT_TRUE(cursor);

                // check that the cursor was registered immediately after the
                // end of the backfill prior to MB-37150 this could fail as the
                // cursor would be _later_ than backfillEnd+1 as the checkpoint
                // has been removed.
                auto pos = CheckpointCursorIntrospector::getCurrentPos(*cursor);
                EXPECT_EQ(backfillEnd + 1, (*pos)->getBySeqno());
            });

    auto resp = stream->next(*producer);
    EXPECT_FALSE(resp);

    // backfill not needed
    EXPECT_EQ(ActiveStream::StreamState::InMemory, stream->getState());

    EXPECT_EQ(0, stream->public_readyQSize());

    MockDcpMessageProducers producers;
    runCheckpointProcessor(*producer, producers);

    EXPECT_EQ(4, stream->public_readyQSize());

    // NB: This first snapshot will actually be _skipped_ as the checkpoint was
    // removed but the active stream did not backfill to "catch up"
    // snap marker
    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_EQ(0, snapMarker.getStartSeqno());
    EXPECT_EQ(1, snapMarker.getEndSeqno());

    // receive mutation 1
    resp = stream->next(*producer);
    EXPECT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());

    {
        const auto& set = dynamic_cast<MutationResponse&>(*resp);
        EXPECT_EQ(key1, set.getItem()->getKey());
        EXPECT_EQ(1, set.getItem()->getBySeqno());
    }

    // snap marker
    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_EQ(2, snapMarker.getStartSeqno());
    EXPECT_EQ(2, snapMarker.getEndSeqno());

    // receive mutation 2
    resp = stream->next(*producer);
    EXPECT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    {
        const auto& set = dynamic_cast<MutationResponse&>(*resp);
        EXPECT_EQ(key2, set.getItem()->getKey());
        EXPECT_EQ(2, set.getItem()->getBySeqno());
    }

    EXPECT_EQ(ActiveStream::StreamState::InMemory, stream->getState());
}

// MB-37468: A stepping producer that has found no items (backfill fully
// processed can race with a completing backfill in such a way that we fail to
// notify the producer that the stream needs further processing. This causes us
// to fail to send a StreamEnd message. A similar case exists for transitioning
// state to TakeoverSend or InMemory.
TEST_P(SingleThreadedActiveStreamTest, CompleteBackfillRaceNoStreamEnd) {
    auto vb = engine->getVBucket(vbid);
    auto& ckptMgr = *vb->checkpointManager;

    // Delete initial stream (so we can re-create after items are available
    // from backing store).
    stream.reset();

    // Add items, flush it to disk, then clear checkpoint to force backfill.
    store_item(vbid, makeStoredDocKey("key1"), "value");

    const auto openId = ckptMgr.getOpenCheckpointId();
    ASSERT_GT(ckptMgr.createNewCheckpoint(), openId);
    flushVBucketToDiskIfPersistent(vbid, 1 /*expected_num_flushed*/);
    ASSERT_EQ(1, ckptMgr.getNumCheckpoints());
    ASSERT_EQ(0, ckptMgr.getNumOpenChkItems());

    // Re-create producer now we have items only on disk. We want to stream up
    // to seqno 1 (our only item) to test that we get the StreamEnd message.
    stream = producer->mockActiveStreamRequest(0 /*flags*/,
                                               0 /*opaque*/,
                                               *vb,
                                               0 /*st_seqno*/,
                                               1 /*en_seqno*/,
                                               0x0 /*vb_uuid*/,
                                               0 /*snap_start_seqno*/,
                                               ~0 /*snap_end_seqno*/);
    ASSERT_TRUE(stream->isBackfilling());

    MockDcpMessageProducers producers;

    // Step to schedule our backfill
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(producers));
    EXPECT_EQ(0, stream->public_readyQ().size());

    auto& bfm = producer->getBFM();

    // create the backfill
    bfm.backfill();

    ThreadGate tg1(2);
    ThreadGate tg2(2);
    std::thread t1;
    stream->setCompleteBackfillHook([this, &t1, &tg1, &tg2, &producers]() {
        // Step past our normal items to expose the race with backfill complete
        // and an empty readyQueue.

        EXPECT_EQ(1, *stream->getNumBackfillItemsRemaining());
        EXPECT_EQ(2, stream->public_readyQ().size());

        // Step snapshot marker
        EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers.last_op);

        // Step mutation
        EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers.last_op);

        stream->setNextHook([&tg1, &tg2]() {
            if (!tg1.isComplete()) {
                tg1.threadUp();

                // Wait for the completeBackfill thread to have attempted to
                // notify that the stream is ready before exiting the hook and
                // setting itemsReady.
                tg2.threadUp();
            }
        });

        // Run the step in a different thread
        t1 = std::thread{[this, &producers]() {
            // This step should produce the stream end
            EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
            EXPECT_EQ(cb::mcbp::ClientOpcode::DcpStreamEnd, producers.last_op);
        }};

        // Wait for the stepping thread to have reached the point at which it is
        // about to set itemsReady before we attempt to set itemsReady after we
        // exit this hook.
        tg1.threadUp();
    });

    // Complete the backfill to expose the race condition
    bfm.backfill();

    // Unblock the stepping thread to now find the stream end
    tg2.threadUp();

    t1.join();

    // Should have sent StreamEnd but vbucket still in queue
    EXPECT_FALSE(producer->findStream(vbid));
    EXPECT_FALSE(producer->getReadyQueue().empty());

    // Step to remove stream from queue
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(producers));
    EXPECT_FALSE(producer->findStream(vbid));
    EXPECT_TRUE(producer->getReadyQueue().empty());
}

void SingleThreadedActiveStreamTest::testProducerIncludesUserXattrsInDelete(
        const std::optional<cb::durability::Requirements>& durReqs) {
    using DcpOpenFlag = cb::mcbp::request::DcpOpenPayload;

    // Test is executed also for SyncDelete
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto vb = engine->getVBucket(vbid);
    // Note: we require IncludeXattr::Yes for IncludeDeletedUserXattrs::Yes
    recreateProducerAndStream(
            *vb,
            DcpOpenFlag::IncludeXattrs | DcpOpenFlag::IncludeDeletedUserXattrs);
    ASSERT_EQ(IncludeDeletedUserXattrs::Yes,
              producer->public_getIncludeDeletedUserXattrs());
    ASSERT_EQ(IncludeDeletedUserXattrs::Yes,
              stream->public_getIncludeDeletedUserXattrs());
    ASSERT_EQ(IncludeXattrs::Yes, producer->public_getIncludeXattrs());
    ASSERT_EQ(IncludeXattrs::Yes, stream->public_getIncludeXattrs());
    ASSERT_EQ(IncludeValue::Yes, producer->public_getIncludeValue());
    ASSERT_EQ(IncludeValue::Yes, stream->public_getIncludeValue());

    // Create a value that contains some user-xattrs + the "_sync" sys-xattr
    const auto value = createXattrValue("");

    const protocol_binary_datatype_t dtJsonXattr =
            PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR;

    auto* cookie = create_mock_cookie();

    // Store a Deleted doc
    auto item = makeCommittedItem(makeStoredDocKey("keyD"), value);
    item->setDataType(dtJsonXattr);
    uint64_t cas = 0;
    const auto expectedStoreRes =
            durReqs ? cb::engine_errc::would_block : cb::engine_errc::success;
    ASSERT_EQ(expectedStoreRes,
              engine->store(*cookie,
                            *item.get(),
                            cas,
                            StoreSemantics::Set,
                            durReqs,
                            DocumentState::Deleted,
                            false));

    if (persistent()) {
        // Flush and ensure docs on disk
        flush_vbucket_to_disk(vbid, 1 /*expectedNumFlushed*/);
        auto kvstore = store->getRWUnderlying(vbid);
        const auto isPrepare = durReqs.has_value();
        const auto doc = kvstore->get(makeDiskDocKey("keyD", isPrepare), vbid);
        EXPECT_EQ(cb::engine_errc::success, doc.getStatus());
        EXPECT_TRUE(doc.item->isDeleted());
        EXPECT_EQ(isPrepare, doc.item->isPending());
        // Check that we have persisted the expected value to disk
        ASSERT_TRUE(doc.item);
        ASSERT_GT(doc.item->getNBytes(), 0);
        EXPECT_EQ(std::string_view(value.c_str(), value.size()),
                  std::string_view(doc.item->getData(), doc.item->getNBytes()));
    }

    auto& readyQ = stream->public_readyQ();
    ASSERT_EQ(0, readyQ.size());

    // Push items to the readyQ and check what we get
    stream->nextCheckpointItemTask();
    ASSERT_EQ(2, readyQ.size());

    auto resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    ASSERT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());

    // Inspect payload for DCP deletion

    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);

    const auto& deletion = dynamic_cast<MutationResponse&>(*resp);
    if (durReqs) {
        ASSERT_EQ(DcpResponse::Event::Prepare, deletion.getEvent());
    } else {
        ASSERT_EQ(DcpResponse::Event::Deletion, deletion.getEvent());
    }

    ASSERT_TRUE(deletion.getItem()->isDeleted());
    ASSERT_EQ(IncludeValue::Yes, deletion.getIncludeValue());
    ASSERT_EQ(IncludeXattrs::Yes, deletion.getIncludeXattrs());
    ASSERT_EQ(IncludeDeletedUserXattrs::Yes,
              deletion.getIncludeDeletedUserXattrs());

    // The value must contain all xattrs (user+sys)
    ASSERT_EQ(dtJsonXattr, deletion.getItem()->getDataType());
    const auto* data = deletion.getItem()->getData();
    const auto nBytes = deletion.getItem()->getNBytes();

    const auto valueBuf = cb::char_buffer(const_cast<char*>(data), nBytes);

    // Check that we have no body (bodySize=0)
    std::string_view body{data, nBytes};
    body.remove_prefix(cb::xattr::get_body_offset(body));
    ASSERT_EQ(0, body.size());

    // Check that we have all the expected xattrs
    cb::xattr::Blob blob(valueBuf, false);
    // Must have user-xattrs
    for (uint8_t i = 1; i <= 6; ++i) {
        EXPECT_FALSE(blob.get("ABCuser" + std::to_string(i)).empty());
    }
    EXPECT_FALSE(blob.get("meta").empty());
    // Must have sys-xattr
    EXPECT_FALSE(blob.get("_sync").empty());

    destroy_mock_cookie(cookie);
}

TEST_P(SingleThreadedActiveStreamTest,
       ProducerIncludesUserXattrsInNormalDelete) {
    testProducerIncludesUserXattrsInDelete({});
}

TEST_P(SingleThreadedActiveStreamTest, ProducerIncludesUserXattrsInSyncDelete) {
    testProducerIncludesUserXattrsInDelete(cb::durability::Requirements());
}

void SingleThreadedActiveStreamTest::testProducerPrunesUserXattrsForDelete(
        uint32_t flags,
        const std::optional<cb::durability::Requirements>& durReqs) {
    using DcpOpenFlag = cb::mcbp::request::DcpOpenPayload;

    // Test is executed also for SyncDelete
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    // Check that we are testing a valid configuration: here we want to test
    // only configurations that trigger user-xattr pruning in deletes.
    ASSERT_TRUE((flags & DcpOpenFlag::IncludeDeletedUserXattrs) == 0);

    auto& vb = *engine->getVBucket(vbid);
    recreateProducerAndStream(vb, flags);

    const auto currIncDelUserXattr =
            (flags & DcpOpenFlag::IncludeDeletedUserXattrs) != 0
                    ? IncludeDeletedUserXattrs::Yes
                    : IncludeDeletedUserXattrs::No;
    ASSERT_EQ(currIncDelUserXattr,
              producer->public_getIncludeDeletedUserXattrs());
    ASSERT_EQ(currIncDelUserXattr,
              stream->public_getIncludeDeletedUserXattrs());

    const auto currIncXattr = (flags & DcpOpenFlag::IncludeXattrs) != 0
                                      ? IncludeXattrs::Yes
                                      : IncludeXattrs::No;
    ASSERT_EQ(currIncXattr, producer->public_getIncludeXattrs());
    ASSERT_EQ(currIncXattr, stream->public_getIncludeXattrs());

    ASSERT_EQ(IncludeValue::Yes, producer->public_getIncludeValue());
    ASSERT_EQ(IncludeValue::Yes, stream->public_getIncludeValue());

    // Create a value that contains some user-xattrs + the "_sync" sys-xattr
    const auto value = createXattrValue("");

    // Note: this body DT can be any type, but I set it to something != than RAW
    // to test that if we prune everything we end up with DT RAW. See below.
    const auto bodyType = PROTOCOL_BINARY_DATATYPE_JSON;

    auto* cookie = create_mock_cookie();

    struct Sizes {
        Sizes(const Item& item) {
            value = item.getNBytes();

            cb::char_buffer valBuf{const_cast<char*>(item.getData()),
                                   item.getNBytes()};
            cb::xattr::Blob xattrBlob(valBuf, false);
            xattrs = xattrBlob.size();
            userXattrs = xattrBlob.get_user_size();
            sysXattrs = xattrBlob.get_system_size();
            body = item.getNBytes() -
                   cb::xattr::get_body_offset({valBuf.data(), valBuf.size()});
        }

        size_t value;
        size_t xattrs;
        size_t userXattrs;
        size_t sysXattrs;
        size_t body;
    };

    // Make an item..
    auto item = makeCommittedItem(makeStoredDocKey("keyD"), value);
    item->setDataType(bodyType | PROTOCOL_BINARY_DATATYPE_XATTR);
    // .. and save the payload sizes for later checks.
    const auto originalValue = value;
    const auto originalSizes = Sizes(*item);

    // Store the item as deleted
    uint64_t cas = 0;
    const auto expectedStoreRes =
            durReqs ? cb::engine_errc::would_block : cb::engine_errc::success;
    ASSERT_EQ(expectedStoreRes,
              engine->store(*cookie,
                            *item.get(),
                            cas,
                            StoreSemantics::Set,
                            durReqs,
                            DocumentState::Deleted,
                            false));

    auto& readyQ = stream->public_readyQ();
    ASSERT_EQ(0, readyQ.size());

    // Verfies that the payload pointed by the item in CM is the same as the
    // original one
    const auto checkPayloadInCM =
            [&vb, &originalValue, &originalSizes, &durReqs]() -> void {
        const auto& manager = *vb.checkpointManager;
        const auto& ckptList =
                CheckpointManagerTestIntrospector::public_getCheckpointList(
                        manager);
        // 1 checkpoint
        ASSERT_EQ(1, ckptList.size());
        const auto* ckpt = ckptList.front().get();
        ASSERT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckpt->getState());
        // empty-item
        auto it = ckpt->begin();
        ASSERT_EQ(queue_op::empty, (*it)->getOperation());
        // 1 metaitem (checkpoint-start)
        it++;
        ASSERT_EQ(3, ckpt->getNumMetaItems());
        EXPECT_EQ(queue_op::checkpoint_start, (*it)->getOperation());
        it++;
        EXPECT_EQ(queue_op::set_vbucket_state, (*it)->getOperation());
        it++;
        EXPECT_EQ(queue_op::set_vbucket_state, (*it)->getOperation());
        // 1 non-metaitem is our deletion
        it++;
        ASSERT_EQ(1, ckpt->getNumItems());
        ASSERT_TRUE((*it)->isDeleted());
        const auto expectedOp =
                durReqs ? queue_op::pending_sync_write : queue_op::mutation;
        EXPECT_EQ(expectedOp, (*it)->getOperation());

        // Byte-by-byte comparison
        EXPECT_EQ(originalValue, (*it)->getValue()->to_s());

        // The latest check should already fail if even a single byte in the
        // payload has changed, but check also the sizes of the specific value
        // chunks.
        const auto cmSizes = Sizes(**it);
        EXPECT_EQ(originalSizes.value, cmSizes.value);
        EXPECT_EQ(originalSizes.xattrs, cmSizes.xattrs);
        EXPECT_EQ(originalSizes.userXattrs, cmSizes.userXattrs);
        EXPECT_EQ(originalSizes.sysXattrs, cmSizes.sysXattrs);
        ASSERT_EQ(originalSizes.body, cmSizes.body);
    };

    // Verify that the value of the item in CM has not changed
    {
        SCOPED_TRACE("");
        checkPayloadInCM();
    }

    // Push items to the readyQ and check what we get
    stream->nextCheckpointItemTask();
    ASSERT_EQ(2, readyQ.size());

    // MB-41944: The call to Stream::nextCheckpointItemTask() has removed
    // UserXattrs from the payload. Before the fix we modified the item's value
    // (which is a reference-counted object in memory) rather that a copy of it.
    // So here we check that the item's value in CM is still untouched.
    {
        SCOPED_TRACE("");
        checkPayloadInCM();
    }

    // Note: Doing this check after Stream::nextCheckpointItemTask() is another
    //  coverage for MB-41944, so I move it here.
    if (persistent()) {
        // Flush and ensure docs on disk
        flush_vbucket_to_disk(vbid, 1 /*expectedNumFlushed*/);
        auto kvstore = store->getRWUnderlying(vbid);
        const auto isPrepare = durReqs.has_value();
        const auto doc = kvstore->get(makeDiskDocKey("keyD", isPrepare), vbid);
        EXPECT_EQ(cb::engine_errc::success, doc.getStatus());
        EXPECT_TRUE(doc.item->isDeleted());
        // Check that we have persisted the expected value to disk
        ASSERT_TRUE(doc.item);
        ASSERT_GT(doc.item->getNBytes(), 0);
        EXPECT_EQ(value,
                  std::string_view(doc.item->getData(), doc.item->getNBytes()));
    }

    auto resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    ASSERT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());

    // Inspect payload for DCP deletion

    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);

    const auto& deletion = dynamic_cast<MutationResponse&>(*resp);
    if (durReqs) {
        ASSERT_EQ(DcpResponse::Event::Prepare, deletion.getEvent());
    } else {
        ASSERT_EQ(DcpResponse::Event::Deletion, deletion.getEvent());
    }

    ASSERT_TRUE(deletion.getItem()->isDeleted());
    ASSERT_EQ(IncludeValue::Yes, deletion.getIncludeValue());
    ASSERT_EQ(currIncXattr, deletion.getIncludeXattrs());
    ASSERT_EQ(currIncDelUserXattr, deletion.getIncludeDeletedUserXattrs());

    // Check that we stream the expected value.
    // What value we stream depends on the current configuration:
    // - if the test flags=0, then we want to prune everything, so no value
    // - else if the test flags=IncludeXattr, then we want only sys-xattrs as
    //   IncludeDeleteUserXattrs::No

    const auto* data = deletion.getItem()->getData();
    const auto nBytes = deletion.getItem()->getNBytes();

    const auto valueBuf = cb::char_buffer(const_cast<char*>(data), nBytes);

    // If we have a value..
    if (valueBuf.size() > 0) {
        // Check that we have no body (bodySize=0)
        std::string_view body{data, nBytes};
        body.remove_prefix(cb::xattr::get_body_offset(body));
        ASSERT_EQ(0, body.size());
    }

    // Check that we have the expected value
    if (flags == 0) {
        ASSERT_EQ(IncludeXattrs::No, deletion.getIncludeXattrs());
        ASSERT_EQ(IncludeDeletedUserXattrs::No,
                  deletion.getIncludeDeletedUserXattrs());
        // No value
        // Note: DT for no-value must be RAW
        ASSERT_EQ(PROTOCOL_BINARY_RAW_BYTES, deletion.getItem()->getDataType());
        // Note: I would expect valueBuf.data()==nullptr, but was not the case
        //  before my see Item::setData
        ASSERT_EQ(0, valueBuf.size());
    } else {
        ASSERT_EQ(IncludeXattrs::Yes, deletion.getIncludeXattrs());
        ASSERT_EQ(IncludeDeletedUserXattrs::No,
                  deletion.getIncludeDeletedUserXattrs());

        // Only xattrs in deletion, dt must be XATTR only
        ASSERT_EQ(PROTOCOL_BINARY_DATATYPE_XATTR,
                  deletion.getItem()->getDataType());

        cb::xattr::Blob blob(valueBuf, false);
        // Must have NO user-xattrs
        for (uint8_t i = 1; i <= 6; ++i) {
            EXPECT_TRUE(blob.get("ABCuser" + std::to_string(i)).empty());
        }
        EXPECT_TRUE(blob.get("meta").empty());
        // Must have sys-xattr
        EXPECT_FALSE(blob.get("_sync").empty());
    }

    destroy_mock_cookie(cookie);
}

TEST_P(SingleThreadedActiveStreamTest,
       ProducerPrunesUserXattrsForNormalDelete_NoDeleteUserXattrs) {
    testProducerPrunesUserXattrsForDelete(
            cb::mcbp::request::DcpOpenPayload::IncludeXattrs, {});
}

TEST_P(SingleThreadedActiveStreamTest,
       ProducerPrunesUserXattrsForSyncDelete_NoDeleteUserXattrs) {
    testProducerPrunesUserXattrsForDelete(
            cb::mcbp::request::DcpOpenPayload::IncludeXattrs,
            cb::durability::Requirements());
}

TEST_P(SingleThreadedActiveStreamTest,
       ProducerPrunesUserXattrsForNormalDelete_NoXattrs) {
    testProducerPrunesUserXattrsForDelete(0, {});
}

TEST_P(SingleThreadedActiveStreamTest,
       ProducerPrunesUserXattrsForSyncDelete_NoXattrs) {
    testProducerPrunesUserXattrsForDelete(0, cb::durability::Requirements());
}

void SingleThreadedActiveStreamTest::testExpirationRemovesBody(uint32_t flags,
                                                               Xattrs xattrs) {
    using DcpOpenFlag = cb::mcbp::request::DcpOpenPayload;

    auto& vb = *engine->getVBucket(vbid);
    recreateProducerAndStream(vb, DcpOpenFlag::IncludeXattrs | flags);

    const auto currIncDelUserXattr =
            (flags & DcpOpenFlag::IncludeDeletedUserXattrs) != 0
                    ? IncludeDeletedUserXattrs::Yes
                    : IncludeDeletedUserXattrs::No;
    ASSERT_EQ(currIncDelUserXattr,
              producer->public_getIncludeDeletedUserXattrs());
    ASSERT_EQ(currIncDelUserXattr,
              stream->public_getIncludeDeletedUserXattrs());
    ASSERT_EQ(IncludeXattrs::Yes, producer->public_getIncludeXattrs());
    ASSERT_EQ(IncludeXattrs::Yes, stream->public_getIncludeXattrs());
    ASSERT_EQ(IncludeValue::Yes, producer->public_getIncludeValue());
    ASSERT_EQ(IncludeValue::Yes, stream->public_getIncludeValue());

    std::string value;
    switch (xattrs) {
    case Xattrs::None:
        value = "body";
        break;
    case Xattrs::User:
        value = createXattrValue("body", false);
        break;
    case Xattrs::UserAndSys:
        value = createXattrValue("body", true);
        break;
    }

    auto datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    if (xattrs != Xattrs::None) {
        datatype |= PROTOCOL_BINARY_DATATYPE_XATTR;
    }

    // Store an item with exptime != 0
    const std::string key = "key";
    const auto docKey = DocKey{key, DocKeyEncodesCollectionId::No};
    store_item(vbid,
               docKey,
               value,
               ep_real_time() + 1 /*1 second TTL*/,
               {cb::engine_errc::success} /*expected*/,
               datatype);

    auto& manager = *vb.checkpointManager;
    const auto& list =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    manager);
    ASSERT_EQ(1, list.size());
    auto* ckpt = list.front().get();
    ASSERT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckpt->getState());
    ASSERT_EQ(2, ckpt->getNumMetaItems());
    ASSERT_EQ(1, ckpt->getNumItems());
    auto it = ckpt->begin(); // empty-item
    it++; // checkpoint-start
    it++; // set-vbstate
    it++;
    EXPECT_EQ(queue_op::mutation, (*it)->getOperation());
    EXPECT_FALSE((*it)->isDeleted());
    EXPECT_EQ(value, (*it)->getValue()->to_s());

    TimeTraveller tt(5000);

    manager.createNewCheckpoint();

    // Just need to access key for expiring
    GetValue gv = store->get(docKey, vbid, nullptr, get_options_t::NONE);
    EXPECT_EQ(cb::engine_errc::no_such_key, gv.getStatus());

    // MB-41989: Expiration removes UserXattrs (if any), but it must do that on
    // a copy of the payload that is then enqueued in the new expired item. So,
    // the payload of the original mutation must be untouched here.
    // Note: 'it' still points to the original mutation in the first checkpoint
    // in CM.
    EXPECT_EQ(queue_op::mutation, (*it)->getOperation());
    EXPECT_FALSE((*it)->isDeleted());
    EXPECT_EQ(std::string_view(value.c_str(), value.size()),
              std::string_view((*it)->getData(), (*it)->getNBytes()));

    ASSERT_EQ(2, list.size());
    ckpt = list.back().get();
    ASSERT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckpt->getState());
    it = ckpt->begin(); // empty-item
    it++; // checkpoint-start
    it++;
    EXPECT_EQ(queue_op::mutation, (*it)->getOperation());
    EXPECT_TRUE((*it)->isDeleted());

    // Note: I inspect the Expiration payload directly by looking at the message
    // in the ActiveStream::readyQ, see below

    auto& readyQ = stream->public_readyQ();
    ASSERT_EQ(0, readyQ.size());

    // Push items to the readyQ and check what we get
    stream->nextCheckpointItemTask();
    ASSERT_EQ(4, readyQ.size());

    auto resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    ASSERT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    auto* msg = dynamic_cast<MutationResponse*>(resp.get());
    ASSERT_TRUE(msg);
    ASSERT_EQ(DcpResponse::Event::Mutation, msg->getEvent());

    // Inspect payload for DCP Expiration
    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    ASSERT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    msg = dynamic_cast<MutationResponse*>(resp.get());
    ASSERT_TRUE(msg);
    ASSERT_EQ(DcpResponse::Event::Expiration, msg->getEvent());
    ASSERT_TRUE(msg->getItem()->isDeleted());
    ASSERT_EQ(currIncDelUserXattr, msg->getIncludeDeletedUserXattrs());

    const auto& item = *msg->getItem();
    const auto* data = item.getData();
    const auto nBytes = item.getNBytes();

    if (xattrs == Xattrs::UserAndSys) {
        // No body
        EXPECT_EQ(0,
                  cb::xattr::get_body_size(item.getDataType(), {data, nBytes}));

        // Only xattrs in deletion, dt must be XATTR only
        ASSERT_EQ(PROTOCOL_BINARY_DATATYPE_XATTR, item.getDataType());
        // We must keep only SysXa
        const auto valueBuf = cb::char_buffer(const_cast<char*>(data), nBytes);
        cb::xattr::Blob blob(valueBuf, false);
        EXPECT_EQ(blob.size(), blob.get_system_size());
        // Note: "_sync" sys-xattr created by createXattrValue()
        EXPECT_FALSE(blob.get("_sync").empty());
    } else {
        // We must remove everything
        // Note: DT for no-value must be RAW
        ASSERT_EQ(PROTOCOL_BINARY_RAW_BYTES, item.getDataType());
        EXPECT_EQ(0, nBytes);
    }
}

TEST_P(SingleThreadedActiveStreamTest, testExpirationRemovesBody_Pre66) {
    testExpirationRemovesBody(0, Xattrs::None);
}

TEST_P(SingleThreadedActiveStreamTest, testExpirationRemovesBody_Pre66_UserXa) {
    testExpirationRemovesBody(0, Xattrs::User);
}

TEST_P(SingleThreadedActiveStreamTest,
       testExpirationRemovesBody_Pre66_UserXa_SysXa) {
    testExpirationRemovesBody(0, Xattrs::UserAndSys);
}

TEST_P(SingleThreadedActiveStreamTest, testExpirationRemovesBody) {
    using DcpOpenFlag = cb::mcbp::request::DcpOpenPayload;
    testExpirationRemovesBody(DcpOpenFlag::IncludeDeletedUserXattrs,
                              Xattrs::None);
}

TEST_P(SingleThreadedActiveStreamTest, testExpirationRemovesBody_UserXa) {
    using DcpOpenFlag = cb::mcbp::request::DcpOpenPayload;
    testExpirationRemovesBody(DcpOpenFlag::IncludeDeletedUserXattrs,
                              Xattrs::User);
}

TEST_P(SingleThreadedActiveStreamTest, testExpirationRemovesBody_UserXa_SysXa) {
    using DcpOpenFlag = cb::mcbp::request::DcpOpenPayload;
    testExpirationRemovesBody(DcpOpenFlag::IncludeDeletedUserXattrs,
                              Xattrs::UserAndSys);
}

/**
 * Verifies that streams that set NO_VALUE still backfill the full payload for
 * SystemEvents and succeed in making the DCP message from that.
 */
TEST_P(SingleThreadedActiveStreamTest, NoValueStreamBackfillsFullSystemEvent) {
    // We need to re-create the stream in a condition that triggers a backfill
    stream.reset();
    producer.reset();

    auto& vb = *engine->getVBucket(vbid);
    ASSERT_EQ(0, vb.getHighSeqno());
    auto& manager = *vb.checkpointManager;
    const auto& list =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    manager);
    ASSERT_EQ(1, list.size());

    const auto key = makeStoredDocKey("key");
    const std::string value = "value";
    store_item(vbid, key, value);
    EXPECT_EQ(1, vb.getHighSeqno());

    CollectionsManifest cm;
    const auto& collection = CollectionEntry::fruit;
    cm.add(collection);
    vb.updateFromManifest(makeManifest(cm));
    EXPECT_EQ(2, vb.getHighSeqno());

    // Ensure backfill
    const auto openId = manager.getOpenCheckpointId();
    ASSERT_GT(manager.createNewCheckpoint(), openId);
    flushVBucketToDiskIfPersistent(vbid, 2 /*expected_num_flushed*/);
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(0, manager.getNumOpenChkItems());

    // Re-create producer and stream
    const std::string jsonFilter =
            fmt::format(R"({{"collections":["{}", "{}"]}})",
                        uint32_t(CollectionEntry::defaultC.getId()),
                        uint32_t(collection.getId()));
    const std::optional<std::string_view> json(jsonFilter);
    recreateProducerAndStream(
            vb, cb::mcbp::request::DcpOpenPayload::NoValue, json);
    ASSERT_TRUE(producer);
    producer->createCheckpointProcessorTask();
    ASSERT_TRUE(stream);
    ASSERT_TRUE(stream->isBackfilling());
    ASSERT_EQ(IncludeValue::No, stream->public_getIncludeValue());
    auto resp = stream->next(*producer);
    EXPECT_FALSE(resp);

    auto& bfm = producer->getBFM();
    EXPECT_EQ(1, bfm.getNumBackfills());
    EXPECT_EQ(backfill_success, bfm.backfill()); // create
    // Before the fix this step throws with:
    //     Collections::VB::Manifest::verifyFlatbuffersData: getCreateEventData
    //     data invalid, .., size:0"
    EXPECT_EQ(backfill_success, bfm.backfill()); // scan

    const auto& readyQ = stream->public_readyQ();
    ASSERT_EQ(3, readyQ.size());
    resp = stream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    ASSERT_EQ(2, readyQ.size());
    resp = stream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    ASSERT_EQ(1, readyQ.size());
    resp = stream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SystemEvent, resp->getEvent());
    const auto& event = dynamic_cast<CollectionCreateProducerMessage&>(*resp);
    EXPECT_GT(event.getEventData().size(), 0);
}

class SingleThreadedBackfillTest : public SingleThreadedActiveStreamTest {
protected:
    void testBackfill() {
        auto vb = engine->getVBucket(vbid);
        auto& ckptMgr = *vb->checkpointManager;

        // Delete initial stream (so we can re-create after items are only
        // available from disk.
        stream.reset();

        // Store 3 items (to check backfill remaining counts).
        // Add items, flush it to disk, then clear checkpoint to force backfill.
        store_item(vbid, makeStoredDocKey("key1"), "value");
        store_item(vbid, makeStoredDocKey("key2"), "value");
        store_item(vbid, makeStoredDocKey("key3"), "value");
        ckptMgr.createNewCheckpoint();

        const auto& stats = engine->getEpStats();
        if (isPersistent()) {
            ASSERT_EQ(0, stats.itemsRemovedFromCheckpoints);
            flushVBucketToDiskIfPersistent(vbid, 3);
        }
        ASSERT_EQ(3, stats.itemsRemovedFromCheckpoints);

        // Re-create the stream now we have items only on disk.
        stream = producer->mockActiveStreamRequest(0 /*flags*/,
                                                   0 /*opaque*/,
                                                   *vb,
                                                   0 /*st_seqno*/,
                                                   ~0 /*en_seqno*/,
                                                   0x0 /*vb_uuid*/,
                                                   0 /*snap_start_seqno*/,
                                                   ~0 /*snap_end_seqno*/);
        ASSERT_TRUE(stream->isBackfilling());

        // Should report empty itemsRemaining as that would mislead
        // ns_server if they asked for stats before the backfill task runs (they
        // would think backfill is complete).
        EXPECT_FALSE(stream->getNumBackfillItemsRemaining());

        // Run the backfill we scheduled when we transitioned to the backfilling
        // state.
        auto& bfm = producer->getBFM();

        // Persistent and Ephemeral backfill-create does not go straight to
        // scan, they both need an extra run
        EXPECT_EQ(backfill_status_t::backfill_success, bfm.backfill());

        // First item
        EXPECT_EQ(backfill_status_t::backfill_success, bfm.backfill());
        EXPECT_EQ(1, stream->getNumBackfillItems());

        // Step the snapshot marker and first mutation
        MockDcpMessageProducers producers;
        EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers.last_op);
        EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers.last_op);

        // Second item
        EXPECT_EQ(backfill_status_t::backfill_success, bfm.backfill());
        EXPECT_EQ(2, stream->getNumBackfillItems());

        // Step the second mutation
        EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers.last_op);

        // Third item
        EXPECT_EQ(backfill_status_t::backfill_success, bfm.backfill());
        EXPECT_EQ(3, stream->getNumBackfillItems());

        // Step the third mutation
        EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers.last_op);

        // Ephemeral backfill scan goes straight to complete but persistent
        // backfill scan does not so we need an extra run
        if (persistent()) {
            EXPECT_EQ(backfill_status_t::backfill_success, bfm.backfill());
        }

        // No more backfills
        EXPECT_EQ(backfill_status_t::backfill_finished, bfm.backfill());

        // Nothing more to step in the producer
        EXPECT_EQ(cb::engine_errc::would_block, producer->step(producers));
    }
};

class SingleThreadedBackfillScanBufferTest : public SingleThreadedBackfillTest {
public:
    void SetUp() override {
        config_string += "dcp_scan_byte_limit=100";
        SingleThreadedActiveStreamTest::SetUp();
    }

    void TearDown() override {
        SingleThreadedActiveStreamTest::TearDown();
    }
};

TEST_P(SingleThreadedBackfillScanBufferTest, SingleItemScanBuffer) {
    testBackfill();
}

class SingleThreadedBackfillBufferTest : public SingleThreadedBackfillTest {
public:
    void SetUp() override {
        config_string += "dcp_backfill_byte_limit=1";
        SingleThreadedActiveStreamTest::SetUp();
    }

    void TearDown() override {
        SingleThreadedActiveStreamTest::TearDown();
    }
};

TEST_P(SingleThreadedBackfillBufferTest, SingleItemBuffer) {
    testBackfill();
}

TEST_P(SingleThreadedPassiveStreamTest, MB42780_DiskToMemoryFromPre65) {
    // Note: We need at least one cursor in the replica checkpoint to hit the
    //  issue. Given that in Ephemeral (a) there is no persistence cursor and
    //  (b) we cannot have any outbound stream / DCP cursor from replica
    //  vbuckets, then we cannot hit the issue in Ephemeral.
    if (ephemeral()) {
        return;
    }

    auto& vb = *store->getVBucket(vbid);
    auto& manager = static_cast<MockCheckpointManager&>(*vb.checkpointManager);
    const auto& ckptList = manager.getCheckpointList();
    ASSERT_EQ(1, ckptList.size());
    ASSERT_EQ(CheckpointType::Memory, ckptList.front()->getCheckpointType());
    ASSERT_EQ(0, ckptList.front()->getSnapshotStartSeqno());
    ASSERT_EQ(0, ckptList.front()->getSnapshotEndSeqno());
    ASSERT_EQ(0, ckptList.front()->getNumItems());
    ASSERT_EQ(0, manager.getHighSeqno());

    // Replica receives a complete disk snapshot {keyA:1, keyB:2}
    const uint32_t opaque = 1;
    const uint64_t snapStart = 1;
    const uint64_t snapEnd = 2;
    EXPECT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(opaque,
                                       vbid,
                                       snapStart,
                                       snapEnd,
                                       MARKER_FLAG_DISK | MARKER_FLAG_CHK,
                                       {} /*HCS*/,
                                       {} /*maxVisibleSeqno*/));
    ASSERT_EQ(1, ckptList.size());
    ASSERT_TRUE(ckptList.front()->isDiskCheckpoint());
    ASSERT_EQ(1, ckptList.front()->getSnapshotStartSeqno());
    ASSERT_EQ(2, ckptList.front()->getSnapshotEndSeqno());
    ASSERT_EQ(0, ckptList.front()->getNumItems());
    ASSERT_EQ(0, manager.getHighSeqno());

    const auto keyA = makeStoredDocKey("keyA");
    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(opaque,
                                 keyA,
                                 {},
                                 0,
                                 0,
                                 0,
                                 vbid,
                                 0,
                                 snapStart,
                                 0,
                                 0,
                                 0,
                                 {},
                                 0));
    const auto keyB = makeStoredDocKey("keyB");
    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(opaque,
                                 keyB,
                                 {},
                                 0,
                                 0,
                                 0,
                                 vbid,
                                 0,
                                 snapEnd,
                                 0,
                                 0,
                                 0,
                                 {},
                                 0));

    ASSERT_EQ(1, ckptList.size());
    ASSERT_TRUE(ckptList.front()->isDiskCheckpoint());
    ASSERT_EQ(1, ckptList.front()->getSnapshotStartSeqno());
    ASSERT_EQ(2, ckptList.front()->getSnapshotEndSeqno());
    ASSERT_EQ(2, ckptList.front()->getNumItems());
    ASSERT_EQ(2, manager.getHighSeqno());

    // Move the persistence cursor to point to keyB:2.
    flush_vbucket_to_disk(vbid, 2 /*expected_num_flushed*/);
    const auto pCursorPos = manager.getPersistenceCursorPos();
    ASSERT_EQ(keyB, (*pCursorPos)->getKey());
    ASSERT_EQ(2, (*pCursorPos)->getBySeqno());

    auto openId = manager.getOpenCheckpointId();

    // Simulate a possible behaviour in pre-6.5: Producer may send a SnapMarker
    // and miss to set the MARKER_FLAG_CHK, eg MB-32862
    EXPECT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(opaque,
                                       vbid,
                                       3 /*snapStart*/,
                                       3 /*snapEnd*/,
                                       MARKER_FLAG_MEMORY,
                                       {} /*HCS*/,
                                       {} /*maxVisibleSeqno*/));

    // 6.6.1 PassiveStream is resilient to any Active misbehaviour with regard
    // to MARKER_FLAG_CHK. Even if Active missed to set the flag, Replica closes
    // the checkpoint and creates a new one for queueing the new Memory
    // snapshot.
    // This is an important step in the test. Essentially here we verify that
    // the fix eliminates one of the preconditions for hitting the issue:
    // snapshots cannot be merged into the same checkpoint if the merge involves
    // Disk snapshots.
    ASSERT_EQ(1, ckptList.size());
    ASSERT_GT(manager.getOpenCheckpointId(), openId);
    openId = manager.getOpenCheckpointId();
    ASSERT_EQ(CheckpointType::Memory, ckptList.front()->getCheckpointType());
    ASSERT_EQ(CHECKPOINT_OPEN, ckptList.front()->getState());
    ASSERT_EQ(3, ckptList.front()->getSnapshotStartSeqno());
    ASSERT_EQ(3, ckptList.front()->getSnapshotEndSeqno());
    ASSERT_EQ(0, ckptList.front()->getNumItems());
    ASSERT_EQ(2, manager.getHighSeqno());

    // Now replica receives a doc within the new Memory snapshot.
    // Note: This step queues keyC into the checkpoint. Given that it is a
    //  Memory checkpoint, then keyC is added to the keyIndex too.
    //  That is a precondition for executing the deduplication path that throws
    //  in Checkpoint::queueDirty before the fix.
    const auto keyC = makeStoredDocKey("keyC");
    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(opaque,
                                 keyC,
                                 {},
                                 0,
                                 0,
                                 0,
                                 vbid,
                                 0,
                                 3 /*seqno*/,
                                 0,
                                 0,
                                 0,
                                 {},
                                 0));

    ASSERT_EQ(1, ckptList.size());
    ASSERT_EQ(CheckpointType::Memory, ckptList.back()->getCheckpointType());
    ASSERT_EQ(CHECKPOINT_OPEN, ckptList.back()->getState());
    ASSERT_EQ(3, ckptList.back()->getSnapshotStartSeqno());
    ASSERT_EQ(3, ckptList.back()->getSnapshotEndSeqno());
    ASSERT_EQ(1, ckptList.back()->getNumItems());
    ASSERT_EQ(3, manager.getHighSeqno());

    // Another SnapMarker with no MARKER_FLAG_CHK
    // Note: This is not due to any pre-6.5 bug, this is legal also in 6.6.x and
    //  7.x. The active may generate multiple Memory snapshots from the same
    //  physical checkpoint. Those snapshots may contain duplicates of the same
    //  key.
    EXPECT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(opaque,
                                       vbid,
                                       4 /*snapStart*/,
                                       4 /*snapEnd*/,
                                       MARKER_FLAG_MEMORY,
                                       {} /*HCS*/,
                                       {} /*maxVisibleSeqno*/));
    // The new snapshot will be queued into the existing checkpoint.
    // Note: It is important that Memory snapshots still are queued into the
    //  same checkpoint if the active requires so. Otherwise, by generating
    //  many checkpoints we would probably hit again perf regressions already
    //  seen in the CheckpointManager.
    ASSERT_EQ(1, ckptList.size());
    ASSERT_EQ(openId, manager.getOpenCheckpointId());
    ASSERT_EQ(CheckpointType::Memory, ckptList.back()->getCheckpointType());
    ASSERT_EQ(CHECKPOINT_OPEN, ckptList.back()->getState());
    ASSERT_EQ(3, ckptList.back()->getSnapshotStartSeqno());
    ASSERT_EQ(4, ckptList.back()->getSnapshotEndSeqno());
    ASSERT_EQ(1, ckptList.back()->getNumItems());
    ASSERT_EQ(3, manager.getHighSeqno());

    // Now replica receives again keyC. KeyC is in the KeyIndex of the
    // open/Memory checkpoint and triggers deduplication checks. Note that dedup
    // checks involve accessing the key-entry in the KeyIndex for the mutation
    // pointed by cursors within that checkpoint.
    //
    // Before the fix, we merged a Disk snapshot and a Memory snapshot into the
    // same checkpoint. The persistence cursor points to a mutation that
    // was received within the Disk checkpoint, so there is no entry in the
    // KeyIndex for that mutation and we fail with:
    //
    //   libc++abi.dylib: terminating with uncaught exception of type
    //   std::logic_error: Checkpoint::queueDirty: Unable to find key in
    //   keyIndex with op:mutation seqno:2 for cursor:persistence in current
    //   checkpoint.
    //
    // At fix, the Memory snapshot is in its own checkpoint. The persistence
    // cursor is in the old (closed) checkpoint, so we don't even try to access
    // the KeyIndex for that cursor.
    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(opaque,
                                 keyC,
                                 {},
                                 0,
                                 0,
                                 0,
                                 vbid,
                                 0,
                                 4 /*seqno*/,
                                 0,
                                 0,
                                 0,
                                 {},
                                 0));

    // Check that we have executed the deduplication path, the test is invalid
    // otherwise.
    ASSERT_EQ(1, ckptList.size());
    ASSERT_EQ(openId, manager.getOpenCheckpointId());
    ASSERT_EQ(CheckpointType::Memory, ckptList.back()->getCheckpointType());
    ASSERT_EQ(CHECKPOINT_OPEN, ckptList.back()->getState());
    ASSERT_EQ(3, ckptList.back()->getSnapshotStartSeqno());
    ASSERT_EQ(4, ckptList.back()->getSnapshotEndSeqno());
    ASSERT_EQ(1, ckptList.back()->getNumItems());
    ASSERT_EQ(4, manager.getHighSeqno());
}

/**
 * MB-38444: We fix an Ephemeral-only bug, but test covers Persistent bucket too
 */
TEST_P(SingleThreadedActiveStreamTest, BackfillRangeCoversAllDataInTheStorage) {
    // We need to re-create the stream in a condition that triggers a backfill
    stream.reset();
    producer.reset();

    auto& vb = *engine->getVBucket(vbid);
    ASSERT_EQ(0, vb.getHighSeqno());
    auto& manager = *vb.checkpointManager;
    const auto& list =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    manager);
    ASSERT_EQ(1, list.size());

    const auto keyA = makeStoredDocKey("keyA");
    const std::string value = "value";
    store_item(vbid, keyA, value);
    EXPECT_EQ(1, vb.getHighSeqno());
    EXPECT_EQ(1, vb.getMaxVisibleSeqno());
    const auto keyB = makeStoredDocKey("keyB");
    store_item(vbid, keyB, value);
    EXPECT_EQ(2, vb.getHighSeqno());
    EXPECT_EQ(2, vb.getMaxVisibleSeqno());

    // Steps to ensure backfill when we re-create the stream in the following
    const auto openId = manager.getOpenCheckpointId();
    ASSERT_GT(manager.createNewCheckpoint(), openId);
    flushVBucketToDiskIfPersistent(vbid, 2 /*expected_num_flushed*/);
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(0, manager.getNumOpenChkItems());

    // Move high-seqno to 4
    const auto keyC = makeStoredDocKey("keyC");
    store_item(vbid, keyC, value);
    EXPECT_EQ(3, vb.getHighSeqno());
    EXPECT_EQ(3, vb.getMaxVisibleSeqno());
    const auto keyD = makeStoredDocKey("keyD");
    store_item(vbid, keyD, value);
    EXPECT_EQ(4, vb.getHighSeqno());
    EXPECT_EQ(4, vb.getMaxVisibleSeqno());

    // At this point we havehigh-seqno=4 but only seqnos 3 and 4 in the CM, so
    // we'll backfill.

    // Note: The aim here is to verify that Backfill picks up everything from
    // the storage even in the case where some seqnos are in the CM. So, we need
    // to ensure that all seqnos are on-disk for Persistent.
    flushVBucketToDiskIfPersistent(vbid, 2 /*expected_num_flushed*/);

    // Re-create producer and stream
    recreateProducerAndStream(vb, 0 /*flags*/);
    ASSERT_TRUE(producer);
    producer->createCheckpointProcessorTask();
    ASSERT_TRUE(stream);
    ASSERT_TRUE(stream->isBackfilling());
    ASSERT_TRUE(stream->public_supportSyncReplication());
    auto resp = stream->next(*producer);
    EXPECT_FALSE(resp);

    // Drive the backfill - execute
    auto& bfm = producer->getBFM();
    ASSERT_EQ(1, bfm.getNumBackfills());

    // Backfill::create
    // Before the fix this steps generates SnapMarker{start:0, end:2, mvs:4},
    // while we want SnapMarker{start:0, end:4, mvs:4}
    ASSERT_EQ(backfill_success, bfm.backfill());
    const auto& readyQ = stream->public_readyQ();
    ASSERT_EQ(1, readyQ.size());
    resp = stream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_EQ(0, snapMarker.getStartSeqno());
    EXPECT_EQ(4, snapMarker.getEndSeqno());
    EXPECT_EQ(4, *snapMarker.getMaxVisibleSeqno());

    // Verify that all seqnos are sent at Backfill::scan
    ASSERT_EQ(backfill_success, bfm.backfill());
    ASSERT_EQ(4, readyQ.size());
    resp = stream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(1, *resp->getBySeqno());
    ASSERT_EQ(3, readyQ.size());
    resp = stream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(2, *resp->getBySeqno());
    ASSERT_EQ(2, readyQ.size());
    resp = stream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(3, *resp->getBySeqno());
    ASSERT_EQ(1, readyQ.size());
    resp = stream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(4, *resp->getBySeqno());
    ASSERT_EQ(0, readyQ.size());
}

TEST_P(SingleThreadedActiveStreamTest, MB_45757) {
    auto& vb = *engine->getVBucket(vbid);
    ASSERT_EQ(0, vb.getHighSeqno());
    auto& manager = *vb.checkpointManager;
    const auto& list =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    manager);
    ASSERT_EQ(1, list.size());

    const auto key = makeStoredDocKey("key");
    const std::string value = "value";
    store_item(vbid, key, value);
    EXPECT_EQ(1, vb.getHighSeqno());

    // In the test we need to re-create streams in a condition that triggers
    // backfill. So get rid of the DCP cursor and move the persistence cursor to
    // a new checkpoint, then remove checkpoints.
    stream.reset();
    producer.reset();
    const auto openId = manager.getOpenCheckpointId();
    ASSERT_GT(manager.createNewCheckpoint(), openId);
    flushVBucketToDiskIfPersistent(vbid, 1 /*expected_num_flushed*/);
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(0, manager.getNumOpenChkItems());

    // Re-create the old producer and stream
    recreateProducerAndStream(vb, 0 /*flags*/);
    ASSERT_TRUE(producer);
    producer->createCheckpointProcessorTask();
    ASSERT_TRUE(stream);

    // Initiate disconnection of the old producer, but block it in
    // CM::removeCursor() just before it acquires the CM::lock.
    ThreadGate gate(2);
    stream->removeCursorPreLockHook = [&gate]() { gate.threadUp(); };
    auto threadCrash = std::thread(
            [stream = this->stream]() { stream->transitionStateToDead(); });

    // The same DCP client reconnects and creates a new producer, so same name
    // as the old producer.
    auto newProd = std::make_shared<MockDcpProducer>(*engine,
                                                     cookie,
                                                     producer->getName(),
                                                     0 /*flags*/,
                                                     false /*startTask*/);
    ASSERT_TRUE(newProd);
    newProd->createCheckpointProcessorTask();
    // StreamReq from the client on the same vbucket -> This stream has the same
    // name as the one that is transitioning to dead in threadCrash
    auto newStream = newProd->mockActiveStreamRequest(0 /*flags*/,
                                                      0 /*opaque*/,
                                                      vb,
                                                      0 /*st_seqno*/,
                                                      ~0 /*en_seqno*/,
                                                      0x0 /*vb_uuid*/,
                                                      0 /*snap_start_seqno*/,
                                                      ~0 /*snap_end_seqno*/);
    ASSERT_TRUE(newStream);
    ASSERT_TRUE(newStream->isBackfilling());

    // New connection backfills and registers cursor.
    // This step invalidates the old stream from the CM::cursors map as it has
    // the same name as the new stream.
    auto& bfm = newProd->getBFM();
    ASSERT_EQ(1, bfm.getNumBackfills());
    ASSERT_EQ(backfill_success, bfm.backfill());
    const auto& readyQ = newStream->public_readyQ();
    ASSERT_EQ(1, readyQ.size());
    const auto resp = newStream->next(*newProd);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());

    // Unblock threadCrash. It tries to invalidate the old stream that is
    // already invalid. This step throws before the fix.
    gate.threadUp();

    threadCrash.join();
}

INSTANTIATE_TEST_SUITE_P(AllBucketTypes,
                         SingleThreadedActiveStreamTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(
        AllBucketTypes,
        SingleThreadedPassiveStreamTest,
        STParameterizedBucketTest::persistentAllBackendsConfigValues(),
        STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(AllBucketTypes,
                         SingleThreadedBackfillScanBufferTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(AllBucketTypes,
                         SingleThreadedBackfillBufferTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

void STPassiveStreamPersistentTest::SetUp() {
    // Test class is not specific for SyncRepl, but some tests check SR
    // quantities too.
    enableSyncReplication = true;
    SingleThreadedPassiveStreamTest::SetUp();
    ASSERT_TRUE(consumer->isSyncReplicationEnabled());
}

#ifdef EP_USE_MAGMA
// Test that we issue Magma insert operations for items streamed in initial disk
// snapshot.
TEST_P(STPassiveStreamMagmaTest, InsertOpForInitialDiskSnapshot) {
    const std::string value("value");

    // Receive initial disk snapshot. Expect inserts for items in this snapshot.
    SnapshotMarker marker(0 /*opaque*/,
                          vbid,
                          0 /*snapStart*/,
                          3 /*snapEnd*/,
                          dcp_marker_flag_t::MARKER_FLAG_DISK | MARKER_FLAG_CHK,
                          0 /*HCS*/,
                          {} /*maxVisibleSeqno*/,
                          {}, // timestamp
                          {} /*streamId*/);

    stream->processMarker(&marker);
    auto vb = engine->getVBucket(vbid);
    ASSERT_TRUE(vb->checkpointManager->isOpenCheckpointInitialDisk());

    for (uint64_t seqno = 1; seqno <= 2; seqno++) {
        auto ret = stream->messageReceived(
                makeMutationConsumerMessage(seqno, vbid, value, 0));
        ASSERT_EQ(cb::engine_errc::success, ret);
    }

    flushVBucketToDiskIfPersistent(vbid, 2);
    EXPECT_EQ(vb->getNumItems(), 2);

    size_t inserts = 0;
    size_t upserts = 0;
    store->getKVStoreStat("magma_NInserts", inserts);
    store->getKVStoreStat("magma_NSets", upserts);
    EXPECT_EQ(inserts, 2);
    EXPECT_EQ(upserts, 0);

    // Simulate backfill interruption. Reconnect and receive a disk snapshot
    // with snap_start_seqno=0. However this is not an initial disk snapshot as
    // we've already received items before i.e. vb->getHighSeqno() > 0. Hence
    // expect upserts for items in this snapshot.
    stream->reconnectStream(vb, 0, 2);
    stream->processMarker(&marker);

    ASSERT_FALSE(vb->checkpointManager->isOpenCheckpointInitialDisk());
    auto ret = stream->messageReceived(
            makeMutationConsumerMessage(3, vbid, value, 0));
    ASSERT_EQ(cb::engine_errc::success, ret);

    flushVBucketToDiskIfPersistent(vbid, 1);
    EXPECT_EQ(vb->getNumItems(), 3);

    inserts = 0;
    upserts = 0;
    store->getKVStoreStat("magma_NInserts", inserts);
    store->getKVStoreStat("magma_NSets", upserts);
    EXPECT_EQ(inserts, 2);
    EXPECT_EQ(upserts, 1);

    // Receive next disk snapshot. Expect upserts for items in this snapshot.
    marker = SnapshotMarker(
            0 /*opaque*/,
            vbid,
            4 /*snapStart*/,
            6 /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_DISK | MARKER_FLAG_CHK,
            0 /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);

    stream->processMarker(&marker);
    ASSERT_FALSE(vb->checkpointManager->isOpenCheckpointInitialDisk());

    for (uint64_t seqno = 4; seqno <= 6; seqno++) {
        auto ret = stream->messageReceived(
                makeMutationConsumerMessage(seqno, vbid, value, 0));
        ASSERT_EQ(cb::engine_errc::success, ret);
    }

    flushVBucketToDiskIfPersistent(vbid, 3);
    EXPECT_EQ(vb->getNumItems(), 6);

    inserts = 0;
    upserts = 0;
    store->getKVStoreStat("magma_NInserts", inserts);
    store->getKVStoreStat("magma_NSets", upserts);
    EXPECT_EQ(inserts, 2);
    EXPECT_EQ(upserts, 4);
}
#endif /*EP_USE_MAGMA*/

/**
 * The test checks that we do not lose any SnapRange information when at Replica
 * we re-attempt the flush of Disk Snapshot after a storage failure.
 */
TEST_P(STPassiveStreamPersistentTest, VBStateNotLostAfterFlushFailure) {
    using namespace testing;
    // Gmock helps us with simulating a flush failure.
    // In the test we want that the first attempt to flush fails, while the
    // second attempts succeeds (forwards the call on to the real KVStore).
    // The purpose of the test is to check that we have stored all the SnapRange
    // info (together with items) when the second flush succeeds.
    auto& mockKVStore = MockKVStore::replaceRWKVStoreWithMock(*store, 0);
    EXPECT_CALL(mockKVStore, commit(_, _))
            .WillOnce(Return(false))
            .WillRepeatedly(DoDefault());

    // Replica receives Snap{{1, 3, Disk}, {PRE:1, M:2, D:3}}
    // Note that the shape of the snapshot is just functional to testing
    // that we write to disk all the required vbstate entries at flush

    // snapshot-marker [1, 3]
    uint32_t opaque = 0;
    SnapshotMarker snapshotMarker(opaque,
                                  vbid,
                                  1 /*snapStart*/,
                                  3 /*snapEnd*/,
                                  dcp_marker_flag_t::MARKER_FLAG_DISK,
                                  std::optional<uint64_t>(1) /*HCS*/,
                                  {} /*maxVisibleSeqno*/,
                                  {}, // timestamp
                                  {} /*streamId*/);
    stream->processMarker(&snapshotMarker);

    // PRE:1
    const std::string value("value");
    using namespace cb::durability;
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      1 /*seqno*/,
                      vbid,
                      value,
                      opaque,
                      Requirements(Level::Majority, Timeout::Infinity()))));

    // M:2 - Logic Commit for PRE:1
    // Note: implicit revSeqno=1
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      2 /*seqno*/, vbid, value, opaque)));

    // D:3
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(
                      makeMutationConsumerMessage(3 /*seqno*/,
                                                  vbid,
                                                  value,
                                                  opaque,
                                                  {} /*DurReqs*/,
                                                  true /*deletion*/,
                                                  2 /*revSeqno*/)));

    auto& kvStore = *store->getRWUnderlying(vbid);
    auto& vbs = *kvStore.getCachedVBucketState(vbid);
    // Check the vbstate entries that are set by SnapRange info
    const auto checkVBState = [&vbs](uint64_t lastSnapStart,
                                     uint64_t lastSnapEnd,
                                     CheckpointType type,
                                     uint64_t hps,
                                     uint64_t hcs,
                                     uint64_t maxDelRevSeqno) {
        EXPECT_EQ(lastSnapStart, vbs.lastSnapStart);
        EXPECT_EQ(lastSnapEnd, vbs.lastSnapEnd);
        EXPECT_EQ(type, getSuperCheckpointType(vbs.checkpointType));
        EXPECT_EQ(hps, vbs.highPreparedSeqno);
        EXPECT_EQ(hcs, vbs.persistedCompletedSeqno);
        EXPECT_EQ(maxDelRevSeqno, vbs.maxDeletedSeqno);
    };

    auto& vb = *store->getVBucket(vbid);
    EXPECT_EQ(3, vb.dirtyQueueSize);

    // This flush fails, we have not written HCS to disk
    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    EXPECT_EQ(FlushResult(MoreAvailable::Yes, 0), epBucket.flushVBucket(vbid));
    EXPECT_EQ(3, vb.dirtyQueueSize);
    {
        SCOPED_TRACE("");
        checkVBState(0 /*lastSnapStart*/,
                     0 /*lastSnapEnd*/,
                     CheckpointType::Memory,
                     0 /*HPS*/,
                     0 /*HCS*/,
                     0 /*maxDelRevSeqno*/);
    }

    // This flush succeeds, we must write all the expected SnapRange info in
    // vbstate on disk
    EXPECT_EQ(FlushResult(MoreAvailable::No, 3), epBucket.flushVBucket(vbid));
    EXPECT_EQ(0, vb.dirtyQueueSize);
    {
        SCOPED_TRACE("");
        // Notes:
        //   1) expected (snapStart = snapEnd) for complete snap flushed
        //   2) expected (HPS = snapEnd) for complete Disk snap flushed
        checkVBState(3 /*lastSnapStart*/,
                     3 /*lastSnapEnd*/,
                     CheckpointType::Disk,
                     3 /*HPS*/,
                     1 /*HCS*/,
                     2 /*maxDelRevSeqno*/);
    }

    // MB-41747: Make sure that we don't have a an on-disk-prepare as
    // part of the internal database handle used by the underlying storage
    // which will be written to disk (and purged as part of commit)
    auto res = store->getLockedVBucket(vbid);
    ASSERT_TRUE(res.owns_lock());
    auto& underlying = *store->getRWUnderlying(vbid);

    CompactionConfig cc;
    auto context =
            std::make_shared<CompactionContext>(store->getVBucket(vbid), cc, 1);
    underlying.compactDB(res.getLock(), context);
    EXPECT_EQ(0, underlying.getCachedVBucketState(vbid)->onDiskPrepares);
}

/**
 * MB-37948: Flusher wrongly computes the new persisted snapshot by using the
 * last persisted vbstate info.
 */
TEST_P(STPassiveStreamPersistentTest, MB_37948) {
    // Set vbucket active on disk
    // Note: TransferVB::Yes is just to prevent that the existing passive stream
    // is released. We sporadically segfault when we access this->stream below
    // otherwise.
    setVBucketStateAndRunPersistTask(
            vbid, vbucket_state_active, {}, TransferVB::Yes);

    // VBucket state changes to replica.
    // Note: The new state is not persisted yet.
    EXPECT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_replica));

    // Replica receives a partial Snap{1, 3, Memory}
    uint32_t opaque = 0;
    SnapshotMarker snapshotMarker(opaque,
                                  vbid,
                                  1 /*snapStart*/,
                                  3 /*snapEnd*/,
                                  dcp_marker_flag_t::MARKER_FLAG_MEMORY,
                                  {} /*HCS*/,
                                  {} /*maxVisibleSeqno*/,
                                  {}, // timestamp
                                  {} /*streamId*/);
    stream->processMarker(&snapshotMarker);
    // M:1
    const std::string value("value");
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      1 /*seqno*/, vbid, value, opaque)));
    // M:2
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      2 /*seqno*/, vbid, value, opaque)));
    // Note: snap is partial, seqno:3 not received yet

    auto& vb = *store->getVBucket(vbid);
    const auto checkPersistedSnapshot = [&vb](uint64_t lastSnapStart,
                                              uint64_t lastSnapEnd) {
        const auto snap = vb.getPersistedSnapshot();
        EXPECT_EQ(lastSnapStart, snap.getStart());
        EXPECT_EQ(lastSnapEnd, snap.getEnd());
    };

    // We have not persisted any item yet
    checkPersistedSnapshot(0, 0);

    // Flush. The new state=replica is not persisted yet; this is where
    // the flusher wrongly uses the state on disk (state=active) for computing
    // the new snapshot range to be persisted.
    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    EXPECT_EQ(FlushResult(MoreAvailable::No, 2), epBucket.flushVBucket(vbid));

    // Before the fix this fails because we have persisted snapEnd=2
    // Note: We have persisted a partial snapshot at replica, snapStart must
    //  still be 0
    checkPersistedSnapshot(1, 3);

    // The core of the test has been already executed, just check that
    // everything behaves as expected when the full snapshot is persisted.

    // M:3 (snap-end mutation)
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      3 /*seqno*/, vbid, value, opaque)));

    EXPECT_EQ(FlushResult(MoreAvailable::No, 1), epBucket.flushVBucket(vbid));

    checkPersistedSnapshot(3, 3);
}

// Check stream-id and sync-repl cannot be enabled
TEST_P(StreamTest, multi_stream_control_denied) {
    setup_dcp_stream();
    EXPECT_EQ(cb::engine_errc::success,
              producer->control(0, "enable_sync_writes", "true"));
    EXPECT_TRUE(producer->isSyncWritesEnabled());
    EXPECT_FALSE(producer->isMultipleStreamEnabled());

    EXPECT_EQ(cb::engine_errc::not_supported,
              producer->control(0, "enable_stream_id", "true"));
    EXPECT_TRUE(producer->isSyncWritesEnabled());
    EXPECT_FALSE(producer->isMultipleStreamEnabled());
    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
}

TEST_P(StreamTest, sync_writes_denied) {
    setup_dcp_stream();
    EXPECT_EQ(cb::engine_errc::success,
              producer->control(0, "enable_stream_id", "true"));
    EXPECT_FALSE(producer->isSyncWritesEnabled());
    EXPECT_TRUE(producer->isMultipleStreamEnabled());

    EXPECT_EQ(cb::engine_errc::not_supported,
              producer->control(0, "enable_sync_writes", "true"));
    EXPECT_FALSE(producer->isSyncWritesEnabled());
    EXPECT_TRUE(producer->isMultipleStreamEnabled());
    EXPECT_EQ(cb::engine_errc::dcp_streamid_invalid, destroy_dcp_stream());
}

/**
 * Test to ensure that V7 dcp status codes are returned when they have
 * been enabled.
 */
TEST_P(STPassiveStreamPersistentTest, enusre_extended_dcp_status_work) {
    uint32_t opaque = 0;
    const std::string keyStr("key");
    DocKey key(keyStr, DocKeyEncodesCollectionId::No);

    // check error code when stream isn't present for vbucket 99
    EXPECT_EQ(
            cb::engine_errc::no_such_key,
            consumer->mutation(
                    opaque, key, {}, 0, 0, 0, Vbid(99), 0, 1, 0, 0, 0, {}, 0));
    // check error code when using a non matching opaque
    opaque = 99999;
    EXPECT_EQ(cb::engine_errc::key_already_exists,
              consumer->mutation(
                      opaque, key, {}, 0, 0, 0, vbid, 0, 1, 0, 0, 0, {}, 0));

    // enable V7 dcp status codes
    consumer->enableV7DcpStatus();
    // check error code when stream isn't present for vbucket 99
    opaque = 0;
    EXPECT_EQ(
            cb::engine_errc::stream_not_found,
            consumer->mutation(
                    opaque, key, {}, 0, 0, 0, Vbid(99), 0, 1, 0, 0, 0, {}, 0));
    // check error code when using a non matching opaque
    opaque = 99999;
    EXPECT_EQ(cb::engine_errc::opaque_no_match,
              consumer->mutation(
                      opaque, key, {}, 0, 0, 0, vbid, 0, 1, 0, 0, 0, {}, 0));
}

void STPassiveStreamPersistentTest::checkVBState(uint64_t lastSnapStart,
                                                 uint64_t lastSnapEnd,
                                                 CheckpointType type,
                                                 uint64_t hps,
                                                 uint64_t hcs,
                                                 uint64_t maxDelRevSeqno) {
    auto& kvStore = *store->getRWUnderlying(vbid);
    auto& vbs = *kvStore.getCachedVBucketState(vbid);
    EXPECT_EQ(lastSnapStart, vbs.lastSnapStart);
    EXPECT_EQ(lastSnapEnd, vbs.lastSnapEnd);
    EXPECT_EQ(type, getSuperCheckpointType(vbs.checkpointType));
    EXPECT_EQ(hps, vbs.highPreparedSeqno);
    EXPECT_EQ(hcs, vbs.persistedCompletedSeqno);
    EXPECT_EQ(maxDelRevSeqno, vbs.maxDeletedSeqno);
}

/**
 * Test that the HPS value on disk is set appropriately when we receive a Disk
 * snapshot that does not contain a prepare.
 *
 * This tests the scenario as described in MB-51639, an active sending a disk
 * snapshot for the following items: [1:Pre, 2:Pre, 3:Commit, 4:Commit] which
 * is received as [3:Mutation, 4:Mutation] as completed prepares are not sent
 * and commits are stored as mutations on disk. Whilst the in-memory HPS value
 * is set to an appropriate value (4 - the snapshot end) the on disk HPS value
 * was left at 0 before the change for MB-51639 was made. This meant that after
 * a restart the HPS value was loaded from disk as 0, rather than 4, and it was
 * possible for ns_server to select a replica having only received [1:Pre as the
 * new active (resulting in a loss of committed prepare 2:Pre).
 */
TEST_P(STPassiveStreamPersistentTest, DiskSnapWithoutPrepareSetsDiskHPS) {
    uint32_t opaque = 0;
    SnapshotMarker snapshotMarker(opaque,
                                  vbid,
                                  1 /*snapStart*/,
                                  4 /*snapEnd*/,
                                  dcp_marker_flag_t::MARKER_FLAG_DISK,
                                  std::optional<uint64_t>(2) /*HCS*/,
                                  {} /*maxVisibleSeqno*/,
                                  {}, // timestamp
                                  {} /*streamId*/);
    stream->processMarker(&snapshotMarker);

    const std::string value("value");

    // M:3 - Logic Commit for PRE:1
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      3 /*seqno*/, vbid, value, opaque)));

    // M:4 - Logic Commit for PRE:2
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      4 /*seqno*/, vbid, value, opaque)));

    flushVBucketToDiskIfPersistent(vbid, 2);

    // Notes: HPS would ideally equal the highest logically completed prepare
    // (2) in this case, but that is not possible without a protocol change to
    // send it from the active. We instead move the HPS to the snapshot end
    // (both on disk and in memory) which is 4 in this case due to changes made
    // as part of MB-34873.
    checkVBState(4 /*lastSnapStart*/,
                 4 /*lastSnapEnd*/,
                 CheckpointType::Disk,
                 4 /*HPS*/,
                 2 /*HCS*/,
                 0 /*maxDelRevSeqno*/);

    EXPECT_EQ(4, store->getVBucket(vbid)->getHighPreparedSeqno());

    ASSERT_NE(cb::engine_errc::disconnect,
              consumer->closeStream(0 /*opaque*/, vbid));
    consumer.reset();

    resetEngineAndWarmup();
    setupConsumerAndPassiveStream();

    EXPECT_EQ(4, store->getVBucket(vbid)->getHighPreparedSeqno());
}

/**
 * Test that the HPS value on disk is set appropriately when we receive a Disk
 * snapshot that does contain a prepare.
 *
 * This tests the scenario:
 * [1:Pre, 2:Pre, 3:Commit, 4:Commit, 5:Pre, 6:Mutation]
 *
 * Whilst one might expect that that HPS value is set to 5, it is in fact set to
 * the snapshot end (6) due to the changes made for MB-34873.
 */
TEST_P(STPassiveStreamPersistentTest, DiskSnapWithPrepareSetsHPSToSnapEnd) {
    uint32_t opaque = 0;
    SnapshotMarker snapshotMarker(opaque,
                                  vbid,
                                  1 /*snapStart*/,
                                  6 /*snapEnd*/,
                                  dcp_marker_flag_t::MARKER_FLAG_DISK,
                                  std::optional<uint64_t>(2) /*HCS*/,
                                  {} /*maxVisibleSeqno*/,
                                  {}, // timestamp
                                  {} /*streamId*/);
    stream->processMarker(&snapshotMarker);

    const std::string value("value");

    // M:3 - Logic Commit for PRE:1
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      3 /*seqno*/, vbid, value, opaque)));

    // M:4 - Logic Commit for PRE:2
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      4 /*seqno*/, vbid, value, opaque)));

    // PRE:5
    using namespace cb::durability;
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      5 /*seqno*/,
                      vbid,
                      value,
                      opaque,
                      Requirements(Level::Majority, Timeout::Infinity()))));

    // M:6
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationConsumerMessage(
                      6 /*seqno*/, vbid, value, opaque)));

    flushVBucketToDiskIfPersistent(vbid, 4);

    checkVBState(6 /*lastSnapStart*/,
                 6 /*lastSnapEnd*/,
                 CheckpointType::Disk,
                 6 /*HPS*/,
                 2 /*HCS*/,
                 0 /*maxDelRevSeqno*/);
}

INSTANTIATE_TEST_SUITE_P(Persistent,
                         STPassiveStreamPersistentTest,
                         STParameterizedBucketTest::persistentConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

#ifdef EP_USE_MAGMA
INSTANTIATE_TEST_SUITE_P(Persistent,
                         STPassiveStreamMagmaTest,
                         STParameterizedBucketTest::magmaConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);
#endif /*EP_USE_MAGMA*/
