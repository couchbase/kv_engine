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
#include "collections/events_generated.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/active_stream_checkpoint_processor_task.h"
#include "dcp/backfill_disk.h"
#include "dcp/response.h"
#include "dcp_utils.h"
#include "ep_bucket.h"
#include "ep_time.h"
#include "failover-table.h"
#include "kv_bucket.h"
#include "kvstore/couch-kvstore/couch-kvstore-config.h"
#include "kvstore/kvstore.h"
#include "test_helpers.h"
#include "tests/test_fileops.h"
#include "thread_gate.h"
#include "vbucket.h"
#include "vbucket_state.h"
#include <executor/executorpool.h>

#include "../couchstore/src/internal.h"
#include "../mock/gmock_dcp_msg_producers.h"
#include "../mock/mock_checkpoint_manager.h"
#include "../mock/mock_dcp.h"
#include "../mock/mock_dcp_conn_map.h"
#include "../mock/mock_dcp_consumer.h"
#include "../mock/mock_dcp_producer.h"
#include "../mock/mock_kvstore.h"
#include "../mock/mock_paging_visitor.h"
#include "../mock/mock_stream.h"
#include "../mock/mock_synchronous_ep_engine.h"

#include <engines/ep/tests/mock/mock_dcp_backfill_mgr.h>
#include <folly/portability/GMock.h>
#include <folly/synchronization/Baton.h>
#include <memcached/dcp_stream_id.h>
#include <platform/json_log.h>
#include <platform/timeutils.h>
#include <programs/engine_testapp/mock_cookie.h>
#include <programs/engine_testapp/mock_server.h>
#include <xattr/blob.h>
#include <xattr/utils.h>
#include <memory>
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
    setup_dcp_stream(cb::mcbp::DcpAddStreamFlag::None,
                     IncludeValue::No,
                     IncludeXattrs::No);
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
    EXPECT_FALSE(producer->isCompressionEnabled());

    // Sending a control message without actually enabling SNAPPY must fail
    EXPECT_EQ(cb::engine_errc::invalid_arguments,
              producer->control(
                      0, DcpControlKeys::ForceValueCompression, "true"));
    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
}

void StreamTest::setupProducerCompression() {
    VBucketPtr vb = engine->getKVBucket()->getVBucket(vbid);

    std::string compressibleValue(
            "{\"product\": \"car\",\"price\": \"100\"},"
            "{\"product\": \"bus\",\"price\": \"1000\"},"
            "{\"product\": \"Train\",\"price\": \"100000\"}");
    std::string regularValue(R"({"product": "car","price": "100"})");

    store_item(vbid, "key1", compressibleValue);
    store_item(vbid, "key2", regularValue);
}

void StreamTest::registerCursorAtCMStart() {
    auto vb = engine->getKVBucket()->getVBucket(vbid);
    ASSERT_TRUE(vb);
    auto& manager = static_cast<CheckpointManager&>(*vb->checkpointManager);
    const auto dcpCursor =
            manager.registerCursorBySeqno(
                           "a cursor", 0, CheckpointCursor::Droppable::Yes)
                    .takeCursor()
                    .lock();
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
              producer->control(
                      0, DcpControlKeys::ForceValueCompression, "true"));
    ASSERT_TRUE(producer->isForceValueCompressionEnabled());

    ASSERT_EQ(cb::engine_errc::success, doStreamRequest(*producer).status);

    MockDcpMessageProducers producers;
    VBucketPtr vb = engine->getKVBucket()->getVBucket(vbid);
    prepareCheckpointItemsForStep(producers, *producer, *vb);

    /* Stream the snapshot marker first */
    EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));
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
    EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));
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
    EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));
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
    EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));
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
    EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));
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
    // MB-69484: Note expected_next_state has no intended affect on this test,
    // it is just sneaky check that this key can be added with topology without
    // creating a whole new test
    nlohmann::json meta = {
            {"topology", nlohmann::json::array({{"active", "replica"}})},
            {"expected_next_state", "active"}};
    vb->setState(vbucket_state_active, &meta);
    setup_dcp_stream(cb::mcbp::DcpAddStreamFlag::None,
                     IncludeValue::No,
                     IncludeXattrs::No,
                     {{DcpControlKeys::EnableSyncWrites, "true"},
                      {DcpControlKeys::ConsumerName, "test_consumer"}});
    store_item(vbid, "key1", "value1");
    store_item(vbid, "key2", "value2");
    using namespace cb::durability;
    auto reqs = Requirements{Level::Majority, Timeout()};
    auto prepareToCommit = store_pending_item(vbid, "pending1", "value3", reqs);

    {
        std::shared_lock rlh(vb->getStateLock());
        ASSERT_EQ(cb::engine_errc::success,
                  vb->commit(rlh,
                             prepareToCommit->getKey(),
                             prepareToCommit->getBySeqno(),
                             {},
                             CommitType::Majority,
                             vb->lockCollections(prepareToCommit->getKey()),
                             cookie));
    }

    // Clear our cookie, we don't actually care about the cas of the item but
    // this is necessary to allow us to enqueue our next abort (which uses the
    // same cookie)
    engine->clearEngineSpecific(*cookie);

    auto prepareToAbort = store_pending_item(vbid, "pending2", "value4", reqs);
    {
        std::shared_lock rlh(vb->getStateLock());
        ASSERT_EQ(cb::engine_errc::success,
                  vb->abort(rlh,
                            prepareToAbort->getKey(),
                            prepareToAbort->getBySeqno(),
                            {},
                            vb->lockCollections(prepareToAbort->getKey())));
    }

    MockDcpMessageProducers producers;

    EXPECT_EQ(cb::engine_errc::success, doStreamRequest(*producer).status);

    prepareCheckpointItemsForStep(producers, *producer, *vb);

    /* Stream the snapshot marker first */
    EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));
    EXPECT_EQ(0, producer->getItemsSent());

    uint64_t totalBytes = producer->getTotalBytesSent();
    EXPECT_GT(totalBytes, 0);

    /* Stream the first mutation. This should increment the
     * number of items and the total bytes sent.
     */
    EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));
    EXPECT_EQ(1, producer->getItemsSent());
    EXPECT_GT(producer->getTotalBytesSent(), totalBytes);
    totalBytes = producer->getTotalBytesSent();

    /* Now simulate a failure while trying to stream the next
     * mutation.
     */
    producers.setMutationStatus(cb::engine_errc::too_big);

    EXPECT_EQ(cb::engine_errc::too_big, producer->step(false, producers));

    /* The number of items total bytes sent should remain the same */
    EXPECT_EQ(1, producer->getItemsSent());
    EXPECT_EQ(producer->getTotalBytesSent(), totalBytes);
    totalBytes = producer->getTotalBytesSent();

    /* Now stream the mutation again and the stats should have incremented */
    producers.setMutationStatus(cb::engine_errc::success);

    EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));
    EXPECT_EQ(2, producer->getItemsSent());
    EXPECT_GT(producer->getTotalBytesSent(), totalBytes);
    totalBytes = producer->getTotalBytesSent();

    // Prepare
    EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));
    EXPECT_EQ(3, producer->getItemsSent());
    EXPECT_GT(producer->getTotalBytesSent(), totalBytes);
    totalBytes = producer->getTotalBytesSent();

    // Commit
    EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));
    EXPECT_EQ(4, producer->getItemsSent());
    EXPECT_GT(producer->getTotalBytesSent(), totalBytes);
    totalBytes = producer->getTotalBytesSent();

    // Prepare
    EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));
    EXPECT_EQ(5, producer->getItemsSent());
    EXPECT_GT(producer->getTotalBytesSent(), totalBytes);
    totalBytes = producer->getTotalBytesSent();

    // SnapshotMarker - doesn't bump items sent
    EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));
    EXPECT_EQ(5, producer->getItemsSent());
    EXPECT_GT(producer->getTotalBytesSent(), totalBytes);
    totalBytes = producer->getTotalBytesSent();

    // Abort
    EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));
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
    setup_dcp_stream(cb::mcbp::DcpAddStreamFlag::None,
                     IncludeValue::Yes,
                     IncludeXattrs::No);
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
    setup_dcp_stream(cb::mcbp::DcpAddStreamFlag::None,
                     IncludeValue::No,
                     IncludeXattrs::Yes);
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

    setup_dcp_stream(cb::mcbp::DcpAddStreamFlag::None,
                     IncludeValue::No,
                     IncludeXattrs::No);
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

    setup_dcp_stream(cb::mcbp::DcpAddStreamFlag::None,
                     IncludeValue::NoWithUnderlyingDatatype,
                     IncludeXattrs::No);
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

    setup_dcp_stream(cb::mcbp::DcpAddStreamFlag::None,
                     IncludeValue::Yes,
                     IncludeXattrs::Yes);
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

    setup_dcp_stream(cb::mcbp::DcpAddStreamFlag::None,
                     IncludeValue::Yes,
                     IncludeXattrs::Yes);
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

    setup_dcp_stream(cb::mcbp::DcpAddStreamFlag::None,
                     IncludeValue::Yes,
                     IncludeXattrs::No);
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

    setup_dcp_stream(cb::mcbp::DcpAddStreamFlag::None,
                     IncludeValue::Yes,
                     IncludeXattrs::No);
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

    setup_dcp_stream(cb::mcbp::DcpAddStreamFlag::None,
                     IncludeValue::No,
                     IncludeXattrs::Yes);
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

    setup_dcp_stream(cb::mcbp::DcpAddStreamFlag::None,
                     IncludeValue::NoWithUnderlyingDatatype,
                     IncludeXattrs::Yes);
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

    setup_dcp_stream(cb::mcbp::DcpAddStreamFlag::None,
                     IncludeValue::NoWithUnderlyingDatatype,
                     IncludeXattrs::Yes);
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
    // cs, vbs
    ASSERT_EQ(ephemeral() ? 1 : 2, manager.getNumOpenChkItems());

    // Create 10 mutations to the same key which, while increasing the high
    // seqno by 10 will result in de-duplication and hence only one actual
    // mutation being added to the checkpoint items.
    const int set_op_count = 10;
    for (unsigned int ii = 0; ii < set_op_count; ii++) {
        store_item(vbid, "key", "value");
    }

    ASSERT_EQ(ephemeral() ? 2 : 3, manager.getNumOpenChkItems())
            << "Incorrect items after population";

    setup_dcp_stream();

    ASSERT_TRUE(stream->isInMemory());

    EXPECT_EQ(ephemeral() ? 2 : 3, stream->getItemsRemaining())
            << "Unexpected initial stream item count";

    // Populate the streams' ready queue with items from the checkpoint,
    // advancing the streams' cursor. Should result in no change in items
    // remaining (they still haven't been send out of the stream).
    stream->nextCheckpointItemTask();
    EXPECT_EQ(2, stream->getItemsRemaining())
            << "Mismatch after moving items to ready queue";

    // Add another mutation. As we have already iterated over all checkpoint
    // items and put into the streams' ready queue, de-duplication of this new
    // mutation (from the point of view of the stream) isn't possible, so items
    // remaining should increase by one.
    store_item(vbid, "key", "value");
    EXPECT_EQ(3, stream->getItemsRemaining())
            << "Mismatch after populating readyQ and storing 1 more item";

    // Now actually drain the items from the readyQ and see how many we
    // received, excluding meta items. This will result in all but one of the
    // checkpoint items (the one we added just above) being drained.
    std::unique_ptr<DcpResponse> response(
            stream->public_nextQueuedItem(*producer));
    ASSERT_NE(nullptr, response);
    EXPECT_FALSE(response->isPersistedEvent())
            << "Expected 1st item to not be a persisted event";

    response = stream->public_nextQueuedItem(*producer);
    ASSERT_NE(nullptr, response);
    EXPECT_TRUE(response->isPersistedEvent())
            << "Expected 2nd item to be from persistence";

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
    ExecutorPool::get()->setNumAuxIO(ThreadPoolConfig::AuxIoThreadCount{1});
    stream->transitionStateToBackfilling();

    // MB-27199: Just stir things up by doing some front-end ops whilst
    // backfilling. This would trigger a number of TSAN warnings
    std::thread thr([this]() {
        int i = 0;
        while (i < 100) {
            engine->getAndTouchInner(
                    *cookie, makeStoredDocKey("key1"), vbid, i);
            i++;
        }
    });

    // Ensure all GATs are done before evaluating the stream below
    thr.join();

    // Wait for the backfill task to have pushed all items to the Stream::readyQ
    // Note: we expect 1 SnapshotMarker + numItems in the readyQ
    // Note: we need to access the readyQ under streamLock while the backfill
    //     task is running
    cb::waitForPredicate(
            [&] { return stream->public_readyQSize() == numItems + 1; });

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
    ExecutorPool::get()->setNumAuxIO(ThreadPoolConfig::AuxIoThreadCount{1});

    /* Wait for the backfill task to fail and stream to transition to dead
       state */
    cb::waitForPredicate([&] { return stream->isDead(); });

    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
}

/* Stream items from a DCP backfill with very small backfill buffer.
   However small the backfill buffer is, backfill must not stop, it must
   proceed to completion eventually */
TEST_P(StreamTest, BackfillSmallBuffer) {
    if (bucketType == "ephemeral") {
        /* Ephemeral buckets is not memory managed for now. Will be memory
           managed soon and then this test will be enabled */
        GTEST_SKIP();
    }

    /* Add 2 items */
    uint64_t numItems = 2;
    addItemsAndRemoveCheckpoint(static_cast<int>(numItems));

    /* Set up a DCP stream for the backfill */
    setup_dcp_stream();

    /* set the DCP backfill buffer size to a value that is smaller than the
       size of a mutation */
    producer->setBackfillBufferSize(1);

    ASSERT_TRUE(stream->isBackfilling());
    ASSERT_EQ(stream->getNumBackfillPauses(), 0);

    /* We want the backfill task to run in a background thread */
    ExecutorPool::get()->setNumAuxIO(ThreadPoolConfig::AuxIoThreadCount{1});

    /* Backfill can only read 1 as its buffer will become full after that */
    cb::waitForPredicate(
            [&] { return (numItems - 1) == stream->getLastBackfilledSeqno(); });

    /* Wait until backfill is paused to assert the pause count */
    cb::waitForPredicate([&] { return stream->getNumBackfillPauses() != 0; });

    EXPECT_EQ(stream->getNumBackfillPauses(), 1);

    /* Consume the backfill item(s) */
    stream->consumeBackfillItems(*producer, /*snapshot*/ 1 + /*mutation*/ 1);

    /* We should see that buffer full status must be false as we have read
       the item in the backfill buffer */
    EXPECT_FALSE(producer->getBackfillBufferFullStatus());

    /* Wait for the backfill to scan again */
    cb::waitForPredicate(
            [&] { return numItems == stream->getLastBackfilledSeqno(); });
    /* Read the other item */
    stream->consumeBackfillItems(*producer, 1);
    /* Finish up with the backilling of the remaining item */
    cb::waitForPredicate(
            [&] { return numItems == stream->getLastReadSeqno(); });

    EXPECT_EQ(stream->getNumBackfillPauses(), 2);

    // Ensure next backfill starts off with a 0 pause count.
    stream->handleSlowStream();
    stream->next(*producer);
    EXPECT_EQ(stream->getNumBackfillPauses(), 0);

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
    ExecutorPool::get()->setNumAuxIO(ThreadPoolConfig::AuxIoThreadCount{1});
    /* Finish up with the backilling of the remaining item */
    cb::waitForPredicate(
            [&] { return numItems == stream->getLastReadSeqno(); });
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
    ExecutorPool::get()->setNumAuxIO(ThreadPoolConfig::AuxIoThreadCount{1});
    /* Finish up with the backilling of the remaining item */
    cb::waitForPredicate(
            [&] { return numItems == stream->getLastReadSeqno(); });
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
    setup_dcp_stream(cb::mcbp::DcpAddStreamFlag::TakeOver);

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
    setup_dcp_stream(cb::mcbp::DcpAddStreamFlag::None,
                     IncludeValue::No,
                     IncludeXattrs::No,
                     {{DcpControlKeys::EnableNoop, "true"}});

    /* Store 4 items */
    const int numItems = 4;
    for (int i = 0; i <= numItems; ++i) {
        store_item(vbid, std::string("key" + std::to_string(i)), "value");
    }
    uint64_t vbUuid = vb0->failovers->getLatestUUID();
    auto result = doStreamRequest(*producer,
                                  vb0->getId(),
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
            *producer, vbid, numItems - 2, numItems, 0, numItems - 2, vbUuid);
    EXPECT_EQ(cb::engine_errc::success, result.status);
    EXPECT_EQ(cb::engine_errc::success,
              producer->closeStream(/*opaque*/ 0, vb0->getId()));

    /* Set a purge_seqno > start_seqno */
    engine->getKVBucket()->getLockedVBucket(vbid)->setPurgeSeqno(numItems - 1);

    /* Now we expect a rollback to 0 */
    result = doStreamRequest(
            *producer, vbid, numItems - 2, numItems, 0, numItems - 2, vbUuid);
    EXPECT_EQ(cb::engine_errc::rollback, result.status);
    EXPECT_EQ(0, result.rollbackSeqno);

    // local_purge_seqno > start_seqno, but remote_purge_seqno ==
    // local_purge_seqno. We don't expect a rollback.
    result = doStreamRequest(
            *producer,
            vbid,
            numItems - 2, // startSeqno
            numItems, // endSeqno
            0, // snapStartSeqno
            numItems - 2, // snapEndSeqno
            vbUuid,
            fmt::format(R"({{"purge_seqno":"{}"}})", numItems - 1));

    EXPECT_EQ(cb::engine_errc::success, result.status);
    EXPECT_EQ(cb::engine_errc::success,
              producer->closeStream(/*opaque*/ 0, vb0->getId()));

    // local_purge_seqno > start_seqno, but remote_purge_seqno !=
    // local_purge_seqno. We expect a rollback.
    result = doStreamRequest(
            *producer,
            vbid,
            numItems - 2, // startSeqno
            numItems, // endSeqno
            0, // snapStartSeqno
            numItems - 2, // snapEndSeqno
            vbUuid,
            fmt::format(R"({{"purge_seqno":"{}"}})", numItems - 2));

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
    setup_dcp_stream(cb::mcbp::DcpAddStreamFlag::None,
                     IncludeValue::No,
                     IncludeXattrs::No);
    ASSERT_EQ(cb::engine_errc::success,
              engine->getKVBucket()->setVBucketState(
                      vbid, vbucket_state_dead, {}, TransferVB::Yes));
    uint64_t vbUuid = vb0->failovers->getLatestUUID();
    // Given the vbucket state is dead we should return not my vbucket.
    EXPECT_EQ(cb::engine_errc::not_my_vbucket,
              doStreamRequest(*producer, vbid, 0, 0, 0, 0, vbUuid).status);
    // The callback function past to streamRequest should not be invoked.
    ASSERT_EQ(0, callbackCount);
}

// Test the compression control success case
TEST_P(StreamTest, validate_compression_control_message_allowed) {
    // For success enable the snappy datatype on the connection
    cookie->setDatatypeSupport(PROTOCOL_BINARY_DATATYPE_SNAPPY);
    setup_dcp_stream();
    EXPECT_TRUE(producer->isCompressionEnabled());

    // Sending a control message after enabling SNAPPY should succeed
    EXPECT_EQ(cb::engine_errc::success,
              producer->control(
                      0, DcpControlKeys::ForceValueCompression, "true"));
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
    EXPECT_EQ(DcpSnapshotMarkerFlag::Memory | DcpSnapshotMarkerFlag::Checkpoint,
              snapMarker.getFlags());

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
    EXPECT_EQ(DcpSnapshotMarkerFlag::Memory, snapMarker1.getFlags());
    // Don't care about startSeqno for this snapshot...
    EXPECT_EQ(10, snapMarker1.getEndSeqno());

    stream->public_nextQueuedItem(*producer);
    EXPECT_EQ(DcpResponse::Event::Mutation, readyQ.front()->getEvent());

    // Second snapshotMarker should be for seqno 11 and have the CHK flag set.
    stream->public_nextQueuedItem(*producer);
    ASSERT_EQ(DcpResponse::Event::SnapshotMarker, readyQ.front()->getEvent());
    auto& snapMarker2 = dynamic_cast<SnapshotMarker&>(*readyQ.front());
    EXPECT_EQ(DcpSnapshotMarkerFlag::Memory | DcpSnapshotMarkerFlag::Checkpoint,
              snapMarker2.getFlags());
    EXPECT_EQ(11, snapMarker2.getStartSeqno());
    EXPECT_EQ(11, snapMarker2.getEndSeqno());

    stream->public_nextQueuedItem(*producer);
    EXPECT_EQ(DcpResponse::Event::Mutation, readyQ.front()->getEvent());
}

TEST_P(StreamTest, ProducerReceivesSeqnoAckForErasedStream) {
    create_dcp_producer(
            cb::mcbp::DcpOpenFlag::None,
            IncludeValue::Yes,
            IncludeXattrs::Yes,
            {{DcpControlKeys::SendStreamEndOnClientCloseStream, "true"},
             {DcpControlKeys::EnableSyncWrites, "true"},
             {DcpControlKeys::ConsumerName, "replica1"}});

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
    EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));
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
    setup_dcp_stream(cb::mcbp::DcpAddStreamFlag::None,
                     IncludeValue::No,
                     IncludeXattrs::No);
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
    cm.getNextItemsForDcp(*cursorPtr, qis);
    // Copy to plain Item vector to aid in checking expected value.
    std::vector<Item> items;
    std::transform(qis.begin(),
                   qis.end(),
                   std::back_inserter(items),
                   [](const auto& rcptr) { return *rcptr; });

    if (ephemeral()) {
        EXPECT_THAT(items,
                    ElementsAre(HasOperation(queue_op::checkpoint_start)));
    } else {
        EXPECT_THAT(items,
                    ElementsAre(HasOperation(queue_op::checkpoint_start),
                                HasOperation(queue_op::set_vbucket_state)));
    }
    EXPECT_EQ(cb::engine_errc::success, destroy_dcp_stream());
}

// Test which demonstrates how a DcpProducer streams from multiple vBuckets.
// Test creates three vBuckets and adds two mutations to each vb, then creates
// a DcpProducer which streams all three vBuckets.
// When stepping the producer this results in all three ActiveStreams
// fetching items into their readyQs, and then items are returned in
// round-robin order (vb:0, vb:1, vb:2, vb:0, vb:1, ...)
//
// Note: There's an open question if this is the ideal behavior - we end up
// populating multiple readyQs with items which we don't have any way to
// recover that memory aside from the consumer reading it. If the consumer is
// slow to read compared to mutation rate; this can result in a significant
// amount of memory being consumed by readyQs.
// See also: MB-46740 (ActiveStream may queue items while above quota)
TEST_P(StreamTest, MultipleVBucketsRoundRobin) {
    // Setup DCP streams for vb:0, 1 and 2.
    engine->getKVBucket()->setVBucketState(Vbid{1}, vbucket_state_active);
    engine->getKVBucket()->setVBucketState(Vbid{2}, vbucket_state_active);
    create_dcp_producer();

    std::vector<VBucketPtr> vbs;
    for (auto vbid : {Vbid{0}, Vbid{1}, Vbid{2}}) {
        auto vb = engine->getVBucket(vbid);
        ASSERT_TRUE(vb);
        ASSERT_EQ(cb::engine_errc::success,
                  doStreamRequest(*producer, vb->getId()).status)
                << "stream request for " << to_string(vbid) << " failed";
        vbs.push_back(vb);
    }

    // Add two mutations to each VBucket.
    for (int i = 0; i < 2; ++i) {
        std::string key("key" + std::to_string(i));
        for (auto& vb : vbs) {
            store_item(vb->getId(), key, "value");
        }
    }

    EXPECT_EQ(3, producer->getReadyQueue().size());

    GMockDcpMsgProducers mockProducers;
    {
        // Setup expected DCP sequence.
        ::testing::InSequence sequence;
        using ::testing::_;

        EXPECT_CALL(mockProducers, marker(_, Vbid(0), _, _, _, _, _, _, _, _));
        EXPECT_CALL(mockProducers, marker(_, Vbid(1), _, _, _, _, _, _, _, _));
        EXPECT_CALL(mockProducers, marker(_, Vbid(2), _, _, _, _, _, _, _, _));

        EXPECT_CALL(mockProducers, mutation(_, _, Vbid(0), _, _, _, _, _));
        EXPECT_CALL(mockProducers, mutation(_, _, Vbid(1), _, _, _, _, _));
        EXPECT_CALL(mockProducers, mutation(_, _, Vbid(2), _, _, _, _, _));

        EXPECT_CALL(mockProducers, mutation(_, _, Vbid(0), _, _, _, _, _));
        EXPECT_CALL(mockProducers, mutation(_, _, Vbid(1), _, _, _, _, _));
        EXPECT_CALL(mockProducers, mutation(_, _, Vbid(2), _, _, _, _, _));
    }

    // Step the DCP producer (running ActiveStreamCheckpointProcessorTask)
    // until all items have been processed.

    EXPECT_EQ(cb::engine_errc::would_block,
              producer->step(false, mockProducers));
    EXPECT_EQ(3, producer->getCheckpointSnapshotTask()->queueSize());
    producer->getCheckpointSnapshotTask()->run();
    // After running ASCPT, expect all readyQs are populated with marker and
    // mutations
    for (const auto& vb : vbs) {
        auto stream = producer->findStream(vb->getId());
        ASSERT_TRUE(stream);
        EXPECT_EQ(3, stream->getItemsRemaining());
    }

    // 3x snapshot markers, 6x mutations.
    for (int items = 0; items < 9; items++) {
        EXPECT_EQ(cb::engine_errc::success,
                  producer->step(false, mockProducers));
    }

    // Should be nothing more.
    EXPECT_EQ(cb::engine_errc::would_block,
              producer->step(false, mockProducers));

    EXPECT_EQ(cb::engine_errc::success, destroy_dcp_stream());
}

class CacheCallbackTest : public StreamTest {
protected:
    void SetUp() override {
        StreamTest::SetUp();
        store_item(vbid, key, "value");

        removeCheckpoint();

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
    CacheCallback callback(*engine->getKVBucket(),
                           stream,
                           static_cast<std::chrono::milliseconds>(
                                   engine->getConfiguration()
                                           .getDcpBackfillRunDurationLimit()));
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
    CacheCallback callback(*engine->getKVBucket(),
                           stream,
                           static_cast<std::chrono::milliseconds>(
                                   engine->getConfiguration()
                                           .getDcpBackfillRunDurationLimit()));

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
    CacheCallback callback(*engine->getKVBucket(),
                           stream,
                           static_cast<std::chrono::milliseconds>(
                                   engine->getConfiguration()
                                           .getDcpBackfillRunDurationLimit()));

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

    CacheCallback callback(*engine->getKVBucket(),
                           stream,
                           static_cast<std::chrono::milliseconds>(
                                   engine->getConfiguration()
                                           .getDcpBackfillRunDurationLimit()));

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

    // Callback yields after processing the item
    EXPECT_EQ(1, stream->getNumBackfillItems());
    EXPECT_EQ(1, stream->public_readyQ().size());
}

// Test cases which run in both Full and Value eviction
INSTANTIATE_TEST_SUITE_P(
        PersistentAndEphemeral,
        StreamTest,
        ::testing::Values("persistent_couchstore", "ephemeral"),
        [](const ::testing::TestParamInfo<std::string>& testInfo) {
            return testInfo.param;
        });

// Test cases which run in both Full and Value eviction
INSTANTIATE_TEST_SUITE_P(
        PersistentAndEphemeral,
        CacheCallbackTest,
        ::testing::Values("persistent_couchstore", "ephemeral"),
        [](const ::testing::TestParamInfo<std::string>& testInfo) {
            return testInfo.param;
        });

void SingleThreadedActiveStreamTest::SetUp() {
    STParameterizedBucketTest::SetUp();
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    setupProducer();
    cookie_to_mock_cookie(cookie)->setCollectionsSupport(true);
}

void SingleThreadedActiveStreamTest::TearDown() {
    stream.reset();
    producer.reset();
    STParameterizedBucketTest::TearDown();
}

void SingleThreadedActiveStreamTest::setupProducer(
        const std::vector<std::pair<std::string_view, std::string_view>>&
                controls,
        cb::mcbp::DcpOpenFlag flags) {
    // We don't set the startTask flag here because we will create the task
    // manually. We do this because the producer actually creates the task on
    // StreamRequest which we do not do because we want a MockActiveStream.
    producer = std::make_shared<MockDcpProducer>(*engine,
                                                 cookie,
                                                 "test_producer->test_consumer",
                                                 flags,
                                                 false /*startTask*/);
    producer->createCheckpointProcessorTask();
    producer->scheduleCheckpointProcessorTask();

    for (const auto& [key, value] : controls) {
        EXPECT_EQ(cb::engine_errc::success,
                  producer->control(0 /*opaque*/, key, value));
    }

    auto vb = engine->getVBucket(vbid);

    stream =
            std::make_shared<MockActiveStream>(engine.get(),
                                               producer,
                                               cb::mcbp::DcpAddStreamFlag::None,
                                               0 /*opaque*/,
                                               *vb);

    stream->setActive();
}

void SingleThreadedActiveStreamTest::recreateProducer(
        VBucket& vb,
        cb::mcbp::DcpOpenFlag flags,
        const std::vector<std::pair<std::string_view, std::string_view>>&
                controls) {
    producer = std::make_shared<MockDcpProducer>(*engine,
                                                 cookie,
                                                 "test_producer->test_consumer",
                                                 flags,
                                                 false /*startTask*/);
    producer->createCheckpointProcessorTask();
    producer->setSyncReplication(SyncReplication::SyncReplication);

    for (const auto& [key, value] : controls) {
        EXPECT_EQ(cb::engine_errc::success,
                  producer->control(0 /*opaque*/, key, value))
                << key << "=" << value;
    }
}

void SingleThreadedActiveStreamTest::recreateProducerAndStream(
        VBucket& vb,
        cb::mcbp::DcpOpenFlag flags,
        std::optional<std::string_view> jsonFilter,
        const std::vector<std::pair<std::string_view, std::string_view>>&
                controls) {
    recreateProducer(vb, flags, controls);
    recreateStream(vb, true /*enforceProducerFlags*/, jsonFilter);
}

void SingleThreadedActiveStreamTest::recreateStream(
        VBucket& vb,
        bool enforceProducerFlags,
        std::optional<std::string_view> jsonFilter,
        cb::mcbp::DcpAddStreamFlag flags) {
    if (enforceProducerFlags) {
        stream = producer->addMockActiveStream(
                flags,
                0 /*opaque*/,
                vb,
                0 /*st_seqno*/,
                ~0ULL /*en_seqno*/,
                0x0 /*vb_uuid*/,
                0 /*snap_start_seqno*/,
                ~0ULL /*snap_end_seqno*/,
                producer->public_getIncludeValue(),
                producer->public_getIncludeXattrs(),
                producer->public_getIncludeDeletedUserXattrs(),
                producer->public_getMaxMarkerVersion(),
                jsonFilter);
    } else {
        stream = producer->addMockActiveStream(flags,
                                               0 /*opaque*/,
                                               vb,
                                               0 /*st_seqno*/,
                                               ~0ULL /*en_seqno*/,
                                               0x0 /*vb_uuid*/,
                                               0 /*snap_start_seqno*/,
                                               ~0ULL /*snap_end_seqno*/);
    }
}

void SingleThreadedPassiveStreamTest::SetUp() {
    STParameterizedBucketTest::SetUp();

    if (!startAsReplica) {
        return;
    }

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    setupConsumerAndPassiveStream();
}

void SingleThreadedPassiveStreamTest::TearDown() {
    ASSERT_NE(cb::engine_errc::disconnect,
              consumer->closeStream(0 /*opaque*/, vbid));
    consumer.reset();
    STParameterizedBucketTest::TearDown();
}

void SingleThreadedPassiveStreamTest::setupConsumer() {
    // In the normal DCP protocol flow, ns_server issues an AddStream request
    // to the DcpConsumer before DCP Control messages are necessarily
    // negotiated.
    // As such, create the PassiveStream *before* enabling SyncReplication
    // (normally done using DCP_CONTROL negotiation with the Producer) to
    // accurately reflect how these classes are used in the real flow.
    consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test_consumer");
    ASSERT_EQ(cb::engine_errc::success,
              consumer->addStream(0 /*opaque*/, vbid, {} /*flags*/));

    if (enableSyncReplication) {
        consumer->enableSyncReplication();
    }

    EXPECT_FALSE(static_cast<MockPassiveStream*>(
                         (consumer->getVbucketStream(vbid)).get())
                         ->public_areFlatBuffersSystemEventsEnabled());

    // Similar to sync-repl, FlatBuffer enablement follows the same pattern.
    // PassiveStream can be created before control negotiation completes, the
    // FlatBuffer setting at construction time could be wrong, it is corrected
    // when the PassiveStream processes the AddStream (::acceptStream)
    consumer->enableFlatBuffersSystemEvents();
}

void SingleThreadedPassiveStreamTest::setupPassiveStream() {
    stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    ASSERT_TRUE(stream->isActive());
}

void SingleThreadedPassiveStreamTest::consumePassiveStreamStreamReq() {
    // Consume the StreamRequest message on the PassiveStreams' readyQ,
    // and simulate the producer responding to it.
    const auto& readyQ = stream->public_readyQ();
    ASSERT_EQ(1, readyQ.size());
    auto msg = stream->public_popFromReadyQ();
    ASSERT_TRUE(msg);
    ASSERT_EQ(DcpResponse::Event::StreamReq, msg->getEvent());
    stream->acceptStream(cb::mcbp::Status::Success, 0);
    ASSERT_TRUE(stream->isActive());
    EXPECT_TRUE(stream->public_areFlatBuffersSystemEventsEnabled());
}

void SingleThreadedPassiveStreamTest::consumePassiveStreamAddStream() {
    auto msg = stream->public_popFromReadyQ();
    ASSERT_EQ(DcpResponse::Event::AddStream, msg->getEvent());
}

void SingleThreadedPassiveStreamTest::maybeConsumePassiveStreamSeqnoAck() {
    auto msg = stream->public_popFromReadyQ();
    if (msg) {
        ASSERT_EQ(DcpResponse::Event::SeqnoAcknowledgement, msg->getEvent());
    }
}

void SingleThreadedPassiveStreamTest::setupConsumerAndPassiveStream() {
    setupConsumer();
    setupPassiveStream();

    consumePassiveStreamStreamReq();

    // PassiveStream should have sent an AddStream response back to ns_server,
    // plus an optional SeqnoAcknowledgement (if SyncReplication enabled and
    // necessary to Ack back to producer).
    consumePassiveStreamAddStream();
    maybeConsumePassiveStreamSeqnoAck();
}

TEST_P(SingleThreadedPassiveStreamTest, StreamStats) {
    nlohmann::json stats;
    AddStatFn add_stat = [&stats](auto key, auto value, auto&) {
        stats[std::string(key)] = std::string(value);
    };
    stream->addStats(add_stat, *cookie);

    auto expectStreamStat = [&](auto& stat) {
        EXPECT_TRUE(stats.erase(stat)) << "Expected " << stat;
    };

    // PassiveStream stats
    expectStreamStat("unacked_bytes");
    expectStreamStat("cur_snapshot_prepare");
    expectStreamStat("cur_snapshot_type");
    expectStreamStat("flags");
    expectStreamStat("items_ready");
    expectStreamStat("last_received_seqno");
    expectStreamStat("opaque");
    expectStreamStat("readyQ_items");
    expectStreamStat("ready_queue_memory");
    expectStreamStat("snap_end_seqno");
    expectStreamStat("snap_start_seqno");
    expectStreamStat("start_seqno");
    expectStreamStat("state");
    expectStreamStat("request_value");
    expectStreamStat("vb_uuid");

    EXPECT_TRUE(stats.empty());
    if (HasFailure()) {
        std::cerr << "Unexpected stats " << stats.dump(2);
    }
}

TEST_P(SingleThreadedPassiveStreamTest, ConsumerStatsLegacy) {
    nlohmann::json stats;
    AddStatFn add_stat = [&stats](auto key, auto value, auto&) {
        stats[std::string(key)] = std::string(value);
    };

    // Collect per-stream stats.
    consumer->addStreamStats(
            add_stat, *cookie, ConnHandler::StreamStatsFormat::Legacy);

    EXPECT_TRUE(stats["test_consumer:stream_0_opaque"].is_string());

    if (HasFailure()) {
        std::cerr << "Unexpected stats " << stats.dump(2);
    }
}

TEST_P(SingleThreadedPassiveStreamTest, ConsumerStatsJson) {
    nlohmann::json stats;
    AddStatFn add_stat = [&stats](auto key, auto value, auto&) {
        stats[std::string(key)] = std::string(value);
    };

    // Collect per-stream stats.
    consumer->addStreamStats(
            add_stat, *cookie, ConnHandler::StreamStatsFormat::Json);

    ASSERT_EQ(1, stats.size());

    auto streamJson = stats["test_consumer:stream_0"].get<std::string>();

    // Make sure it parses fine.
    auto streamStats = nlohmann::json::parse(streamJson);
    EXPECT_TRUE(streamStats.contains("opaque"));

    if (HasFailure()) {
        std::cerr << "Unexpected stats " << stats.dump(2);
    }
}

TEST_P(SingleThreadedActiveStreamTest, StreamStats) {
    nlohmann::json stats;
    AddStatFn add_stat = [&stats](auto key, auto value, auto&) {
        stats[std::string(key)] = std::string(value);
    };
    stream->addStats(add_stat, *cookie);

    auto expectStreamStat = [&](auto& stat) {
        EXPECT_TRUE(stats.erase(stat)) << "Expected " << stat;
    };

    // ActiveStream stats
    expectStreamStat("backfill_buffer_bytes");
    expectStreamStat("backfill_buffer_items");
    expectStreamStat("backfill_disk_items");
    expectStreamStat("backfill_mem_items");
    expectStreamStat("backfill_sent");
    expectStreamStat("change_streams_enabled");
    expectStreamStat("cursor_registered");
    expectStreamStat("end_seqno");
    expectStreamStat("flags");
    expectStreamStat("items_ready");
    expectStreamStat("last_read_seqno");
    expectStreamStat("last_sent_seqno");
    expectStreamStat("last_sent_seqno_advance");
    expectStreamStat("last_sent_snap_end_seqno");
    expectStreamStat("memory_phase");
    expectStreamStat("opaque");
    expectStreamStat("readyQ_items");
    expectStreamStat("ready_queue_memory");
    expectStreamStat("snap_end_seqno");
    expectStreamStat("snap_start_seqno");
    expectStreamStat("start_seqno");
    expectStreamStat("state");
    expectStreamStat("vb_uuid");

    // Filter stats
    expectStreamStat("filter_cids");
    expectStreamStat("filter_default_allowed");
    expectStreamStat("filter_type");
    expectStreamStat("filter_sid");
    expectStreamStat("filter_size");

    EXPECT_TRUE(stats.empty());
    if (HasFailure()) {
        std::cerr << "Unexpected stats " << stats.dump(2);
    }
}

TEST_P(SingleThreadedActiveStreamTest,
       SkipMagmaBFilterWhenBackfillingFromDisk) {
    if (!isMagma()) {
        GTEST_SKIP() << "Magma specific test";
    }
    /* Test Setup - begin */

    // 1. Add an item.
    // 2. Flush the item to disk & clear all checkpoints to force the backfill
    //    from disk.
    // 3. Adjust the low-watermarks & force the item pager to run.

    auto vb = engine->getVBucket(vbid);
    auto& ckptMgr = *vb->checkpointManager;
    ckptMgr.clear(0 /*seqno*/);

    stream.reset();

    const auto key = makeStoredDocKey("key");
    const std::string value = "value";
    auto item = make_item(vbid, key, value);

    EXPECT_EQ(
            MutationStatus::WasClean,
            public_processSet(*vb, item, VBQueueItemCtx(CanDeduplicate::Yes)));

    // Ensure mutation is on disk; no longer present in CheckpointManager.
    vb->checkpointManager->createNewCheckpoint();
    flushVBucketToDiskIfPersistent(vbid, 1);
    ASSERT_EQ(1, vb->checkpointManager->getNumCheckpoints());

    vb->ht.clear();

    const auto numResidentItems =
            vb->getNumItems() - vb->getNumNonResidentItems();

    // Ensure there are no resident item.
    ASSERT_EQ(0, numResidentItems);
    /* Test Setup - end */

    //! Set the expectation keyMayExist is never called for Magma.
    using namespace ::testing;
    auto& mockKVStore = MockKVStore::replaceRWKVStoreWithMock(*store, 0);
    EXPECT_CALL(mockKVStore, keyMayExist(_, _)).Times(0);

    const std::string jsonFilter =
            fmt::format(R"({{"collections":["{}"]}})",
                        uint32_t(CollectionEntry::defaultC.getId()));

    recreateStream(*vb, true, jsonFilter);
    ASSERT_TRUE(stream->isBackfilling());

    auto& bfm = producer->getBFM();
    bfm.backfill();

    // Note the following assert just make sure the backfill scan indeed ran &
    // we received all the items we expect.

    // readyQ must contain a SnapshotMarker & a single mutation.
    ASSERT_EQ(stream->public_readyQSize(), 2);
    auto resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());

    resp = stream->public_nextQueuedItem(*producer);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
}

TEST_P(SingleThreadedActiveStreamTest, ProducerStatsLegacy) {
    nlohmann::json stats;
    AddStatFn add_stat = [&stats](auto key, auto value, auto&) {
        stats[std::string(key)] = std::string(value);
    };

    // Put the stream in the producer's stream map.
    auto streamPtr = std::static_pointer_cast<ActiveStream>(stream);
    producer->updateStreamsMap(Vbid(0),
                               cb::mcbp::DcpStreamId(1),
                               streamPtr,
                               DcpProducer::AllowSwapInStreamMap::No);
    // Collect per-stream stats.
    producer->addStreamStats(
            add_stat, *cookie, ConnHandler::StreamStatsFormat::Legacy);

    EXPECT_TRUE(
            stats["test_producer->test_consumer:stream_0_opaque"].is_string());

    if (HasFailure()) {
        std::cerr << "Unexpected stats " << stats.dump(2);
    }
}

TEST_P(SingleThreadedActiveStreamTest, ProducerStatsJson) {
    nlohmann::json stats;
    AddStatFn add_stat = [&stats](auto key, auto value, auto&) {
        stats[std::string(key)] = std::string(value);
    };

    // Put the stream in the producer's stream map.
    auto streamPtr = std::static_pointer_cast<ActiveStream>(stream);
    producer->updateStreamsMap(Vbid(0),
                               cb::mcbp::DcpStreamId(1),
                               streamPtr,
                               DcpProducer::AllowSwapInStreamMap::No);
    // Collect per-stream stats.
    producer->addStreamStats(
            add_stat, *cookie, ConnHandler::StreamStatsFormat::Json);
    ASSERT_EQ(1, stats.size());

    auto streamJson =
            stats["test_producer->test_consumer:stream_0"].get<std::string>();

    // Make sure it parses fine.
    auto streamStats = nlohmann::json::parse(streamJson);
    EXPECT_TRUE(streamStats.contains("opaque"));

    if (HasFailure()) {
        std::cerr << "Unexpected stats " << stats.dump(2);
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

    ASSERT_EQ(
            MutationStatus::WasClean,
            public_processSet(*vb, item, VBQueueItemCtx(CanDeduplicate::Yes)));

    // Update the collection stats. Frontend ops update the manifest stats
    // directly, but we are calling public_processSet above which call the
    // internal vbucket processSet, therefore manual update them.

    // Why update the collection stats?

    // Start seqno for the stream is 1 & collection high seqno is 0 (if not
    // updated). With the optimization in MB-62963, we check if a backfill can
    // be skipped if the start seqno in the stream request is greater than the
    // high seqno of the collections requested in a collection filtered stream.
    // Update the collection stats, to make sure backfill runs.
    {
        auto handle = vb->lockCollections();
        handle.incrementItemCount(CollectionID::Default);
        handle.setHighSeqno(CollectionID::Default,
                            1,
                            Collections::VB::HighSeqnoType::Committed);
    }
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
    EXPECT_TRUE(
            isFlagSet(marker.getFlags(), DcpSnapshotMarkerFlag::Checkpoint));
    EXPECT_TRUE(isFlagSet(marker.getFlags(), DcpSnapshotMarkerFlag::Disk));
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
    manager.createNewCheckpoint();
    ASSERT_GT(manager.getOpenCheckpointId(), openId);
    flushVBucketToDiskIfPersistent(vbid, 3 /*expected_num_flushed*/);
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getNumOpenChkItems());

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
                                                      CookieIface&) {
        if (key == "status"sv) {
            EXPECT_EQ(expectedKey, std::string(value.data(), value.size()));
            statusFound = true;
        }
    };

    // Should report status == "calculating_item_count" before backfill
    // scan has occurred.
    expectedKey = "calculating-item-count";
    auto* cookie = create_mock_cookie();
    stream->addTakeoverStats(checkStatusFn, *cookie, *vb);
    EXPECT_TRUE(statusFound);

    // Run the backfill we scheduled when we transitioned to the backfilling
    // state. Run the backfill task once to get initial item counts.
    auto& bfm = producer->getBFM();
    bfm.backfill();
    EXPECT_EQ(3, *stream->getNumBackfillItemsRemaining());
    // Should report status == "backfilling"
    statusFound = false;
    expectedKey = "backfilling";
    stream->addTakeoverStats(checkStatusFn, *cookie, *vb);
    EXPECT_TRUE(statusFound);

    // Run again to actually scan (items remaining unchanged).
    bfm.backfill();
    EXPECT_EQ(3, *stream->getNumBackfillItemsRemaining());
    statusFound = false;
    expectedKey = "backfilling";
    stream->addTakeoverStats(checkStatusFn, *cookie, *vb);
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
    stream->addTakeoverStats(checkStatusFn, *cookie, *vb);
    EXPECT_TRUE(statusFound);
    destroy_mock_cookie(cookie);
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
        manager.createNewCheckpoint();
        ASSERT_GT(manager.getOpenCheckpointId(), openId);
        flushVBucketToDiskIfPersistent(vbid, 2 /*expected_num_flushed*/);
        ASSERT_EQ(1, manager.getNumCheckpoints());
        ASSERT_EQ(1, manager.getNumOpenChkItems());
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
        auto& auxIoQ = *task_executor->getLpTaskQ(TaskType::AuxIO);
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

    // Set maximum number of in-progress backfills per connection to 3, so we
    // can have all of the backfills in progress at once.
    engine->getConfiguration().setDcpBackfillInProgressPerConnectionLimit(3);

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
        manager.createNewCheckpoint();
        ASSERT_GT(manager.getOpenCheckpointId(), openId);
        flushVBucketToDiskIfPersistent(vbid, 2 /*expected_num_flushed*/);
        ASSERT_EQ(1, manager.getNumCheckpoints());
        ASSERT_EQ(1, manager.getNumOpenChkItems());
    }

    // Re-create producer now we have items only on disk, setting a scan buffer
    // which can only hold 1 item (so backfill doesn't complete a VB in one
    // scan).
    setupProducer({{DcpControlKeys::BackfillOrder, "sequential"}});
    producer->public_getBackfillScanBuffer().maxItems = 1;

    // setupProducer creates a stream for vb0. Also need streams for vb1 and
    // vb2.
    auto stream1 =
            std::make_shared<MockActiveStream>(engine.get(),
                                               producer,
                                               cb::mcbp::DcpAddStreamFlag::None,
                                               0 /*opaque*/,
                                               *engine->getVBucket(Vbid{1}));
    auto stream2 =
            std::make_shared<MockActiveStream>(engine.get(),
                                               producer,
                                               cb::mcbp::DcpAddStreamFlag::None,
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

    // To drive a single vBucket's backfill to completion in this test requires
    // further 3 steps:
    // 1. For pushing the first item over the stream
    // 2. For pushing the second item over the stream
    // 3. For settling, as the backfill yields after (2)
    const int backfillSteps = 3;
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

    EXPECT_EQ(backfill_finished, bfm.backfill());
    EXPECT_EQ(backfill_finished, bfm.backfill());
    EXPECT_EQ(backfill_finished, bfm.backfill());
    EXPECT_EQ(backfill_finished, bfm.backfill());
    EXPECT_EQ(backfill_finished, bfm.backfill());
}

TEST_P(SingleThreadedActiveStreamTest, BackfillSkipsScanIfStreamInWrongState) {
    auto vb = engine->getVBucket(vbid);
    auto& ckptMgr = *vb->checkpointManager;

    const auto key = makeStoredDocKey("key");
    const std::string value = "value";
    auto item = make_item(vbid, key, value);

    {
        std::shared_lock rlh(vb->getStateLock());
        auto cHandle = vb->lockCollections(item.getKey());
        EXPECT_EQ(cb::engine_errc::success,
                  vb->set(rlh, item, cookie, *engine, {}, cHandle));
    }
    ckptMgr.createNewCheckpoint();
    EXPECT_EQ(2, ckptMgr.getOpenCheckpointId());

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
        EXPECT_EQ(backfill_success, bfm.backfill()); // create and scan
        EXPECT_EQ(backfill_finished, bfm.backfill()); // nothing else to do
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

        EXPECT_EQ(backfill_success, bfm.backfill()); // create -> complete
        EXPECT_EQ(backfill_finished, bfm.backfill()); // nothing else to do
        EXPECT_EQ(0, bfm.getNumBackfills());
    }
}

/**
 * Test to ensure that the ActiveStream doesn't see a seqno out of order
 * from the checkpoint manager.
 * To do that simulate the behaviour observed in MB-53100 when a takeover stream
 * created a checkpoint with only meta items in. Then try and register a cursor
 * against a seqno that matches the seqno of the meta items.
 */
TEST_P(SingleThreadedActiveStreamTest, MB_53100_Check_Monotonicity) {
    auto& vb = *store->getVBucket(vbid);
    auto& ckptMgr = *vb.checkpointManager;

    // Create a checkpoint that only contains meta items
    ASSERT_EQ(1, ckptMgr.getNumCheckpoints());
    setVBucketState(
            vbid, vbucket_state_pending, {}, TransferVB::Yes); // seqno: 1

    // Set the vbucket to active which will create a new checkpoint but not
    // change the high seqno
    setVBucketState(
            vbid, vbucket_state_active, {}, TransferVB::Yes); // seqno: 1
    ASSERT_EQ(2, ckptMgr.getNumCheckpoints());
    // Mimic takeover behaviour and set the topology of the active vbucket
    setVBucketState(
            vbid,
            vbucket_state_active,
            {{"topology",
              nlohmann::json::array({{"active", "replica"}})}}); // seqno: 1
    ASSERT_EQ(0, vb.getHighSeqno());
    // Write a few docs to the vbucket so we have some mutations to process
    store_item(vbid,
               makeStoredDocKey("keyA"),
               "value"); // seqno: 1 (Mutation makes seqno visible)
    auto keyASeqno = vb.getHighSeqno();
    // Ensure the high seqno has now changed
    ASSERT_EQ(1, vb.getHighSeqno());
    store_item(vbid, makeStoredDocKey("keyB"), "value"); // seqno: 2
    ASSERT_EQ(2, vb.getHighSeqno());

    // Create a stream from keyA's seqno, this will register the cursor in the
    // checkpoint
    producer->createCheckpointProcessorTask();
    auto newStream = std::make_shared<MockActiveStream>(
            engine.get(),
            producer,
            cb::mcbp::DcpAddStreamFlag::None,
            1 /*opaque*/,
            vb,
            keyASeqno,
            std::numeric_limits<uint64_t>::max(),
            0,
            keyASeqno,
            keyASeqno);
    newStream->setActive();
    // Now ask the stream to process any items in the checkpoint manager and
    // ensure we don't throw while processing them.
    auto items = newStream->public_getOutstandingItems(vb);
    EXPECT_NO_THROW(newStream->public_processItems(items));
}

TEST_P(SingleThreadedActiveStreamTest,
       MB_53100_RegisterCursorForFixLengthStream) {
    auto& vb = *store->getVBucket(vbid);
    auto& ckptMgr = *vb.checkpointManager;

    // Fill the current open checkpoint with meta items
    setVBucketState(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});
    setVBucketState(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica2"}})}});

    // Create a checkpoint, so we have a checkpoint with only meta items
    // e.g. id:1 [ e:1, cs:1, vbs:1, vbs:1, ce:1]
    ckptMgr.createNewCheckpoint();
    ASSERT_EQ(2, ckptMgr.getNumCheckpoints());

    // Then write two items to the new checkpoint so we have items that an
    // active stream can read
    store_item(vbid, makeStoredDocKey("keyA"), "value");
    store_item(vbid, makeStoredDocKey("keyB"), "value");
    ASSERT_EQ(2, vb.getHighSeqno());

    stream.reset();
    ASSERT_FALSE(stream);

    // Now create a stream from seqno 1 -> 2. Effectively asking
    // to just steam seqno:2. Streaming from seqno:1 will cause us to register a
    // cursor at seqno:1, this is important as we should register the cursor at
    // the mutation for keyA that makes seqno:1 visible and not at any of the
    // meta items that have their seqno set to 1 e.g. the set vbucket states.
    producer->createCheckpointProcessorTask();
    uint64_t rollbackSeqno = std::numeric_limits<uint64_t>::max();
    ASSERT_EQ(cb::engine_errc::success,
              producer->streamRequest({},
                                      2,
                                      vb.getId(),
                                      1,
                                      2,
                                      vb.failovers->getLatestUUID(),
                                      1,
                                      1,
                                      &rollbackSeqno,
                                      mock_dcp_add_failover_log,
                                      std::nullopt));

    MockDcpMessageProducers producers;
    notifyAndRunToCheckpoint(*producer, producers);
    // The stream should return snapshot: 1 -> 2, with keyB and then an end
    // stream
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(
                      producers, cb::mcbp::ClientOpcode::DcpSnapshotMarker));
    EXPECT_EQ(1, producers.last_snap_start_seqno);
    EXPECT_EQ(2, producers.last_snap_end_seqno);

    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(producers,
                                      cb::mcbp::ClientOpcode::DcpMutation));
    EXPECT_EQ(2, producers.last_byseqno);
    EXPECT_EQ("keyB", producers.last_key);

    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(producers,
                                      cb::mcbp::ClientOpcode::DcpStreamEnd));
    EXPECT_EQ(cb::mcbp::DcpStreamEndStatus::Ok, producers.last_end_status);
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
              consumer->addStream(opaque /*opaque*/, vbid, {} /*flags*/));
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
              consumer->addStream(opaque /*opaque*/, vbid, {} /*flags*/));
}

// This test covers a "race" where the streamEnd code would use the return value
// from removeStream - which was a nullptr. This test simulates that race by
// injecting a closeStream into the streamDead path.
TEST_P(SingleThreadedPassiveStreamTest, MB_56675) {
    // Create a hook which will remove the stream during the steamEnd code.
    // With MB-56675 this leads to a crash because the streamEnd code would
    // assume removeStream returns a valid stream.
    std::function<void()> hook = [this]() {
        consumer->closeStreamDueToVbStateChange(vbid, vbucket_state_active);
    };
    stream->setStreamDeadHook(hook);
    consumer->streamEnd(1, vbid, cb::mcbp::DcpStreamEndStatus::StateChanged);
    EXPECT_EQ(StreamEndResponse::getFlowControlSize(cb::mcbp::DcpStreamId{}),
              consumer->getFlowControl().getFreedBytes());
}

void SingleThreadedPassiveStreamTest::
        testInitialDiskSnapshotFlagClearedOnTransitionToActive(
                vbucket_state_t initialState) {
    // Test that a vbucket changing state to active clears the initial disk
    // snapshot flag
    setVBucketStateAndRunPersistTask(vbid, initialState);

    // receive snapshot
    SnapshotMarker marker(
            0 /*opaque*/,
            vbid,
            1 /*snapStart*/,
            100 /*snapEnd*/,
            DcpSnapshotMarkerFlag::Disk | DcpSnapshotMarkerFlag::Checkpoint,
            0 /*HCS*/,
            {},
            {} /*maxVisibleSeqno*/,
            std::nullopt,
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
 * Replica if the Active misses to set the
 * DcpSnapshotMarkerFlag::Checkpoint in SnapshotMarker.
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
                    DcpSnapshotMarkerFlag flags,
                    size_t expectedNumCheckpoint,
                    CheckpointType expectedOpenCkptType) -> void {
        cb::mcbp::DcpStreamId streamId{};
        SnapshotMarker marker(opaque,
                              vbid,
                              snapStart,
                              snapEnd,
                              flags,
                              0 /*HCS*/,
                              {},
                              {} /*maxVisibleSeqno*/,
                              std::nullopt,
                              streamId);
        stream->processMarker(&marker);

        auto item = makeCommittedItem(makeStoredDocKey("key"), "value");
        item->setBySeqno(snapStart);

        EXPECT_EQ(cb::engine_errc::success,
                  stream->messageReceived(std::make_unique<MutationResponse>(
                          std::move(item),
                          opaque,
                          IncludeDeleteTime::No,
                          DocKeyEncodesCollectionId::No,
                          EnableExpiryOutput::Yes,
                          streamId)));

        EXPECT_EQ(expectedNumCheckpoint, ckptMgr.getNumCheckpoints());
        EXPECT_EQ(expectedOpenCkptType, ckptMgr.getOpenCheckpointType());
    };
    auto initalNumberOfCheckpoints = vb->checkpointManager->getNumCheckpoints();
    {
        CB_SCOPED_TRACE("");
        receiveSnapshot(1 /*snapStart*/,
                        1 /*snapEnd*/,
                        DcpSnapshotMarkerFlag::Memory |
                                DcpSnapshotMarkerFlag::Checkpoint,
                        initalNumberOfCheckpoints + 1 /*expectedNumCheckpoint*/,
                        CheckpointType::Memory /*expectedOpenCkptType*/);
    }

    // Merged with the previous snapshot
    {
        CB_SCOPED_TRACE("");
        receiveSnapshot(2 /*snapStart*/,
                        2 /*snapEnd*/,
                        DcpSnapshotMarkerFlag::Memory,
                        initalNumberOfCheckpoints + 1 /*expectedNumCheckpoint*/,
                        CheckpointType::Memory /*expectedOpenCkptType*/);
    }

    // Disk + we miss the DcpSnapshotMarkerFlag::Checkpoint,
    // still not merged
    {
        CB_SCOPED_TRACE("");
        receiveSnapshot(3 /*snapStart*/,
                        3 /*snapEnd*/,
                        DcpSnapshotMarkerFlag::Disk,
                        initalNumberOfCheckpoints + 2 /*expectedNumCheckpoint*/,
                        CheckpointType::Disk /*expectedOpenCkptType*/);
    }

    {
        CB_SCOPED_TRACE("");
        receiveSnapshot(
                4 /*snapStart*/,
                4 /*snapEnd*/,
                DcpSnapshotMarkerFlag::Disk | DcpSnapshotMarkerFlag::Checkpoint,
                initalNumberOfCheckpoints + 3 /*expectedNumCheckpoint*/,
                CheckpointType::Disk /*expectedOpenCkptType*/);
    }

    // From Disk to Disk + we miss the
    // DcpSnapshotMarkerFlag::Checkpoint, still not merged
    {
        CB_SCOPED_TRACE("");
        receiveSnapshot(5 /*snapStart*/,
                        5 /*snapEnd*/,
                        DcpSnapshotMarkerFlag::Disk,
                        initalNumberOfCheckpoints + 4 /*expectedNumCheckpoint*/,
                        CheckpointType::Disk /*expectedOpenCkptType*/);
    }

    // Memory snap but previous snap is Disk -> no merge
    {
        CB_SCOPED_TRACE("");
        receiveSnapshot(6 /*snapStart*/,
                        6 /*snapEnd*/,
                        DcpSnapshotMarkerFlag::Memory,
                        initalNumberOfCheckpoints + 5 /*expectedNumCheckpoint*/,
                        CheckpointType::Memory /*expectedOpenCkptType*/);
    }

    {
        CB_SCOPED_TRACE("");
        receiveSnapshot(7 /*snapStart*/,
                        7 /*snapEnd*/,
                        DcpSnapshotMarkerFlag::Memory |
                                DcpSnapshotMarkerFlag::Checkpoint,
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
                                       DcpSnapshotMarkerFlag::Checkpoint,
                                       {} /*HCS*/,
                                       {} /*HPS*/,
                                       {} /*maxVisibleSeqno*/,
                                       {} /*purgeSeqno*/));

    const auto verifyDCPFailure =
            [this, &durReqs](const cb::const_byte_buffer& value,
                             protocol_binary_datatype_t datatype) -> void {
        const uint32_t opaque = 1;
        int64_t bySeqno = 1;
        const auto key = makeStoredDocKey("key");
        if (durReqs) {
            EXPECT_EQ(cb::engine_errc::invalid_arguments,
                      consumer->prepare(opaque,
                                        key,
                                        value,
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
                                         key,
                                         value,
                                         datatype,
                                         0 /*cas*/,
                                         vbid,
                                         bySeqno,
                                         0 /*revSeqno*/));
        }
    };

    // Build up a value with just raw body and verify DCP failure
    const std::string body = "body";
    cb::const_byte_buffer value{reinterpret_cast<const uint8_t*>(body.data()),
                                body.size()};
    {
        CB_SCOPED_TRACE("");
        verifyDCPFailure(value, PROTOCOL_BINARY_RAW_BYTES);
    }

    // Verify the same for body + xattrs
    const auto xattrValue = createXattrValue(body);
    value = {reinterpret_cast<const uint8_t*>(xattrValue.data()),
             xattrValue.size()};
    {
        CB_SCOPED_TRACE("");
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
                                       DcpSnapshotMarkerFlag::Checkpoint,
                                       {} /*HCS*/,
                                       {} /*HPS*/,
                                       {} /*maxVisibleSeqno*/,
                                       {} /*purgeSeqno*/));

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
                                         datatype,
                                         0 /*cas*/,
                                         vbid,
                                         bySeqno,
                                         0 /*revSeqno*/));
        }
    };

    // Build up a value with just raw body and verify that DCP deletion succeeds
    // and the Body has been removed from the payload.
    const std::string body = "body";
    cb::const_byte_buffer value{reinterpret_cast<const uint8_t*>(body.data()),
                                body.size()};
    {
        CB_SCOPED_TRACE("");
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
        {
            std::shared_lock rlh(vb.getStateLock());
            EXPECT_EQ(cb::engine_errc::success,
                      vb.commit(rlh,
                                key,
                                1,
                                {},
                                CommitType::Majority,
                                vb.lockCollections(key)));
        }
        // Replica doesn't like 2 prepares for the same key into the same
        // checkpoint.
        const int64_t newStartSeqno = initialEndSeqno + 2;
        EXPECT_EQ(cb::engine_errc::success,
                  consumer->snapshotMarker(1 /*opaque*/,
                                           vbid,
                                           newStartSeqno,
                                           newStartSeqno + 10 /*endSeqno*/,
                                           DcpSnapshotMarkerFlag::Checkpoint,
                                           {} /*HCS*/,
                                           {} /*HPS*/,
                                           {} /*maxVisibleSeqno*/,
                                           {} /*purgeSeqno*/));
        nextSeqno = newStartSeqno;
    }

    // Verify the same for body + user-xattrs + sys-xattrs
    const auto xattrValue = createXattrValue(body);
    value = {reinterpret_cast<const uint8_t*>(xattrValue.data()),
             xattrValue.size()};
    {
        CB_SCOPED_TRACE("");
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
                                       DcpSnapshotMarkerFlag::Checkpoint,
                                       {} /*HCS*/,
                                       {}, /*HPS*/
                                       {} /*maxVisibleSeqno*/,
                                       {} /*purgeSeqno*/));

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
    const auto key = makeStoredDocKey("key");
    if (durReqs) {
        EXPECT_EQ(cb::engine_errc::success,
                  consumer->prepare(opaque,
                                    key,
                                    valueBuf,
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
                                     valueBuf,
                                     datatype,
                                     0 /*cas*/,
                                     vbid,
                                     bySeqno,
                                     0 /*revSeqno*/));
    }

    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    EXPECT_EQ(FlushResult(MoreAvailable::No, 1), epBucket.flushVBucket(vbid));

    // Check item persisted

    auto& kvstore = *store->getRWUnderlying(vbid);
    const auto isPrepare = durReqs.has_value();

    // Test expects the datatype to remain the same.
    // Provide the correct filter to read back the item compressed/uncompressed
    // to match how it was stored.
    auto filter = compressed ? ValueFilter::VALUES_COMPRESSED
                             : ValueFilter::VALUES_DECOMPRESSED;
    auto doc = kvstore.get(makeDiskDocKey("key", isPrepare), vbid, filter);
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

TEST_P(SingleThreadedPassiveStreamTest, InvalidMarkerVisibleSnapEndThrows) {
    const uint64_t snapEnd = 10;
    const uint64_t visibleSnapEnd = snapEnd + 1;
    SnapshotMarker marker(0 /*opaque*/,
                          vbid,
                          1 /*snapStart*/,
                          snapEnd,
                          DcpSnapshotMarkerFlag::Memory,
                          0 /*HCS*/,
                          {},
                          visibleSnapEnd,
                          std::nullopt,
                          {} /*streamId*/);

    try {
        stream->processMarker(&marker);
    } catch (const std::logic_error& e) {
        const auto substring = "PassiveStream::processMarker: snapEnd:" +
                               std::to_string(snapEnd) + " < visibleSnapEnd:" +
                               std::to_string(visibleSnapEnd);
        EXPECT_THAT(e.what(), testing::HasSubstr(substring));
        return;
    }
    FAIL();
}

void SingleThreadedPassiveStreamTest::mutation(uint32_t opaque,
                                               const DocKeyView& key,
                                               Vbid vbid,
                                               uint64_t bySeqno) {
    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(
                      opaque, key, {}, 0, 1, vbid, 0, bySeqno, 0, 0, 0, 0));
}

void SingleThreadedPassiveStreamTest::deletion(uint32_t opaque,
                                               const DocKeyView& key,
                                               Vbid vbid,
                                               uint64_t bySeqno) {
    EXPECT_EQ(cb::engine_errc::success,
              consumer->deletionV2(opaque, key, {}, 0, 1, vbid, bySeqno, 0, 0));
}

// Test covers functionality of MB_63977, a disk snapshot which is received
// with a purge-seqno ensures that the disk snapshot is given that purge-seqno.
// The test writes a "sparse" snapshot in two flushes and checks that the purge
// seqno moves correctly.
void SingleThreadedPassiveStreamTest::purgeSeqnoReplicated(bool flushTwice) {
    // Setup a disk snapshot
    SnapshotMarker marker(0 /*opaque*/,
                          vbid,
                          1 /*snapStart*/,
                          10,
                          DcpSnapshotMarkerFlag::Disk,
                          0 /*HCS*/,
                          0, /*HPS*/
                          10, /*MVS*/
                          5, /* purge */
                          cb::mcbp::DcpStreamId{});
    stream->processMarker(&marker);

    // Write seqno 4.
    mutation(1, makeStoredDocKey("keyA"), vbid, 4);

    if (flushTwice) {
        flushVBucketToDiskIfPersistent(vbid, 1);

        // The first flush has only flushed seqno:4, the purgeSeqno of our
        // snapshot is 5. We must ensure the purgeSeqno only moved to the
        // high-seqno (4)
        EXPECT_EQ(4, store->getVBucket(vbid)->getPurgeSeqno());
        EXPECT_EQ(4, store->getRWUnderlying(vbid)->getPurgeSeqno(vbid));
    }

    // Write seqno 10.
    mutation(1, makeStoredDocKey("keyB"), vbid, 10);
    flushVBucketToDiskIfPersistent(vbid, flushTwice ? 1 : 2);

    // And flushed seqno:10, the purgeSeqno of the marker was 5. We must
    // ensure the purgeSeqno only moved to 5
    EXPECT_EQ(5, store->getVBucket(vbid)->getPurgeSeqno());
    EXPECT_EQ(5, store->getRWUnderlying(vbid)->getPurgeSeqno(vbid));
}

TEST_P(SingleThreadedPassiveStreamTest, purgeSeqnoReplicated) {
    purgeSeqnoReplicated(false);
}

TEST_P(SingleThreadedPassiveStreamTest, purgeSeqnoReplicatedFlushTwice) {
    purgeSeqnoReplicated(true);
}

// Test some cases where we drop a cursor (disk snapshot arrives)
// The active/replica purge independenly of each other, the replica cannot just
// accept an incoming purge-seqno without some checks.
TEST_P(SingleThreadedPassiveStreamTest, cursorDroppedPurgeSeqnoReplicated) {
    SnapshotMarker marker(0 /*opaque*/,
                          vbid,
                          1 /*snapStart*/,
                          10,
                          DcpSnapshotMarkerFlag::Disk,
                          0 /*HCS*/,
                          0 /*HPS*/,
                          10, /*MVS*/
                          0, /* purge */
                          cb::mcbp::DcpStreamId{});
    stream->processMarker(&marker);
    // We could imagine that key1@seq1 is deleted at seq2
    deletion(1, makeStoredDocKey("key1"), vbid, 2);
    deletion(1, makeStoredDocKey("key2"), vbid, 9);
    deletion(1, makeStoredDocKey("key3"), vbid, 10);
    flushVBucketToDiskIfPersistent(vbid, 3);
    EXPECT_EQ(0, store->getVBucket(vbid)->getPurgeSeqno());
    EXPECT_EQ(0, store->getRWUnderlying(vbid)->getPurgeSeqno(vbid));

    // Replica runs tombstone purge, cannot purge high-seqno. But seq2,9 gone
    purgeTombstonesBefore(10);
    EXPECT_EQ(9, store->getVBucket(vbid)->getPurgeSeqno());
    EXPECT_EQ(9, store->getRWUnderlying(vbid)->getPurgeSeqno(vbid));

    // Next interesting case is a drop cursor, a second disk checkpoint.
    // The active has indpendently purged, but the timing meant the active
    // drops 2 but retains 9 and 10, i.e. the incoming purge-seqno is 2, which
    // is lower than the replica purge - it must have no effect on the replica
    SnapshotMarker marker2(0 /*opaque*/,
                           vbid,
                           10 /*snapStart*/,
                           11,
                           DcpSnapshotMarkerFlag::Disk,
                           0 /*HCS*/,
                           0 /*HPS*/,
                           11, /*MVS*/
                           2, /* purge */
                           cb::mcbp::DcpStreamId{});
    stream->processMarker(&marker2);
    mutation(1, makeStoredDocKey("key4"), vbid, 11);
    flushVBucketToDiskIfPersistent(vbid, 1);
    // local purge is still 9.
    EXPECT_EQ(9, store->getVBucket(vbid)->getPurgeSeqno());
    EXPECT_EQ(9, store->getRWUnderlying(vbid)->getPurgeSeqno(vbid));

    // Next case is another disk snapshot, this time the active has purged.
    // It has actually purged ahead of the replica, but the replica still stores
    // the purged tombstone (seq:10). Because the purge-seqno is outside of the
    // range, we ignore it.
    SnapshotMarker marker3(0 /*opaque*/,
                           vbid,
                           12 /*snapStart*/,
                           13,
                           DcpSnapshotMarkerFlag::Disk,
                           0 /*HCS*/,
                           0 /*HPS*/,
                           13, /*MVS*/
                           10, /* purge */
                           cb::mcbp::DcpStreamId{});
    stream->processMarker(&marker3);
    mutation(1, makeStoredDocKey("key4"), vbid, 13);
    flushVBucketToDiskIfPersistent(vbid, 1);
    // local purge is still 9.
    EXPECT_EQ(9, store->getVBucket(vbid)->getPurgeSeqno());
    EXPECT_EQ(9, store->getRWUnderlying(vbid)->getPurgeSeqno(vbid));

    // Next case, new disk snapshot which has purge seqno within it. We must
    // use that purge seqno to avoid inconsitency problems with clients.
    SnapshotMarker marker4(0 /*opaque*/,
                           vbid,
                           14 /*snapStart*/,
                           20,
                           DcpSnapshotMarkerFlag::Disk,
                           0 /*HCS*/,
                           0 /*HPS*/,
                           20, /*MVS*/
                           14, /* purge */
                           cb::mcbp::DcpStreamId{});
    stream->processMarker(&marker4);
    mutation(1, makeStoredDocKey("key4"), vbid, 20);
    flushVBucketToDiskIfPersistent(vbid, 1);
    // local purge is now 14
    EXPECT_EQ(14, store->getVBucket(vbid)->getPurgeSeqno());
    EXPECT_EQ(14, store->getRWUnderlying(vbid)->getPurgeSeqno(vbid));
}

/**
 * Fixture for tests which start out as active and switch to replica to assert
 * we setup the PassiveStream correctly.
 */
class SingleThreadedActiveToPassiveStreamTest
    : public SingleThreadedPassiveStreamTest {
public:
    void SetUp() override {
        startAsReplica = false;
        SingleThreadedPassiveStreamTest::SetUp();
        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    }

    void TearDown() override {
        SingleThreadedPassiveStreamTest::TearDown();
    }

    std::unique_ptr<StreamRequest> startPassiveStream() {
        setupConsumer();
        setupPassiveStream();

        const auto& readyQ = stream->public_readyQ();
        EXPECT_EQ(1, readyQ.size());
        auto msg = stream->public_popFromReadyQ();
        EXPECT_TRUE(msg);
        EXPECT_EQ(DcpResponse::Event::StreamReq, msg->getEvent());

        // Cast the message pointer to the correct type.
        std::unique_ptr<StreamRequest> sr(
                dynamic_cast<StreamRequest*>(msg.get()));
        EXPECT_TRUE(sr);
        msg.release();

        stream->acceptStream(cb::mcbp::Status::Success, 0);
        EXPECT_TRUE(stream->isActive());

        return sr;
    }
};

/**
 * If the node changes to replica, and the last checkpoint only contains meta
 * items, check that the stream requests has startSeqno of the last mutation
 * (the current CheckpointManager::lastBySeqno).
 */
TEST_P(SingleThreadedActiveToPassiveStreamTest,
       ReplicaStreamRequestUsesLastMutationSeqno) {
    auto& vb = *store->getVBucket(vbid);
    auto& cm = *vb.checkpointManager;

    cm.registerCursorBySeqno("keep", 0, CheckpointCursor::Droppable::No)
            .takeCursor()
            .lock();

    store_item(vbid, makeStoredDocKey("item 1"), "value"); // seqno 1
    store_item(vbid, makeStoredDocKey("item 2"), "value"); // seqno 2
    flushVBucketToDiskIfPersistent(vbid, 2);

    // Flip back to replica, but make sure the set_vbucket_state is in its
    // own snapshot.
    cm.createNewCheckpoint();
    // The set_vbucket_state{replica} will have seqno 3.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    const auto& list =
            CheckpointManagerTestIntrospector::public_getCheckpointList(cm);
    EXPECT_EQ(2, list.size());

    auto& first = list.front();
    EXPECT_EQ(0, first->getSnapshotStartSeqno());
    EXPECT_EQ(2, first->getSnapshotEndSeqno());

    auto& second = list.back();
    EXPECT_EQ(3, second->getSnapshotStartSeqno());
    EXPECT_EQ(3, second->getSnapshotEndSeqno());

    // The StreamRequest on the PassiveStream's readyQ has a snapshot startSeqno
    // lower than the last checkpoint snapshot startSeqno.
    auto streamReq = startPassiveStream();
    EXPECT_EQ(2, streamReq->getStartSeqno());
    EXPECT_EQ(2, streamReq->getSnapStartSeqno());
}

/**
 * Check that the first snapshot marker returned for a stream has startSnapSeqno
 * equal to the one specified in the stream request.
 */
TEST_P(SingleThreadedActiveStreamTest,
       FirstSnapshotHasRequestedStartSnapSeqno) {
    // Replace initial stream with one registered with DCP producer.
    setupProducer({});

    auto& vb = *store->getVBucket(vbid);
    auto& cm = *vb.checkpointManager;
    // Prevent checkpoint destruction on ephemeral
    const auto keepCursor =
            cm.registerCursorBySeqno(
                      "cursor", 0, CheckpointCursor::Droppable::No)
                    .takeCursor()
                    .lock();

    store_item(vbid, makeStoredDocKey("item 1"), "value"); // seqno 1
    store_item(vbid, makeStoredDocKey("item 2"), "value"); // seqno 2
    cm.createNewCheckpoint();
    store_item(vbid, makeStoredDocKey("item 3"), "value"); // seqno 3
    store_item(vbid, makeStoredDocKey("item 4"), "value"); // seqno 4
    cm.createNewCheckpoint();

    stream = producer->addMockActiveStream(
            {},
            /*opaque*/ 0,
            vb,
            /*st_seqno*/ 2,
            /*en_seqno*/ ~0ULL,
            /*vb_uuid*/ vb.failovers->getFailoverLog().back().uuid,
            /*snap_start_seqno*/ 2,
            /*snap_end_seqno*/ 2);

    MockDcpMessageProducers producers;
    runCheckpointProcessor(*producer, producers);

    EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));

    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers.last_op);
    EXPECT_EQ(2, producers.last_snap_start_seqno);
    EXPECT_EQ(4, producers.last_snap_end_seqno);

    EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers.last_op);
    EXPECT_EQ(3, producers.last_byseqno);
}

/**
 * processItems should skip meta-only checkpoints and they should never
 * be seen by the ActiveStream.
 *
 * In MB-57767, we saw a crash because while the meta-only checkpoint is ignored
 * for replication and no SnapshotMarker is sent, we updated the nextSnapStart
 * during processItems, resulting in invariant breaking once we moved on
 * from the meta-only checkpoint.
 */
TEST_P(SingleThreadedActiveStreamTest, MetaOnlyCheckpointsSkipped) {
    // Replace initial stream with one registered with DCP producer.
    setupProducer({});

    auto& vb = *store->getVBucket(vbid);
    auto& cm = *vb.checkpointManager;

    store_item(vbid, makeStoredDocKey("item 1"), "value"); // seqno 1
    store_item(vbid, makeStoredDocKey("item 2"), "value"); // seqno 2

    cm.createNewCheckpoint();
    // CM: [1, 2]

    // Simulate set_vbucket_state{replica}.
    cm.queueSetVBState();
    // CM: [1, 2] [3, 3]

    // And a snapshot received from active.
    cm.createSnapshot(2, 3, {}, {}, CheckpointType::Memory, 3);
    // CM: [1, 2] [3, 3] [2, ]

    // CM: [1, 2] [3, 3] [2, 3]
    store_item(vbid, makeStoredDocKey("item 3"), "value"); // seqno 3

    auto items = stream->getOutstandingItems(vb);
    // getOutstandingItems will return 3 ranges, including the
    // [3, 3] meta-only snapshot. processItems should ignore it, as it doesn't
    // need to be replicated over DCP.
    ASSERT_EQ(3, items.ranges.size());
    ASSERT_EQ(0, items.ranges[0].getStart());
    ASSERT_EQ(3, items.ranges[1].getStart());
    ASSERT_EQ(2, items.ranges[2].getStart());

    // Make sure we don't crash here, processing [2, 3] after [3, 3].
    EXPECT_NO_THROW(stream->public_processItems(items));

    auto& readyQ = stream->public_readyQ();
    EXPECT_EQ(5, readyQ.size());

    // Ready queue shouldn't have a snapshot marker for the meta-only checkpoint
    // (with the set_vbucket_state).
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, readyQ.front()->getEvent());
    readyQ.pop();
    EXPECT_EQ(DcpResponse::Event::Mutation, readyQ.front()->getEvent());
    readyQ.pop();
    EXPECT_EQ(DcpResponse::Event::Mutation, readyQ.front()->getEvent());
    readyQ.pop();
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, readyQ.front()->getEvent());
    readyQ.pop();
    EXPECT_EQ(DcpResponse::Event::Mutation, readyQ.front()->getEvent());
    readyQ.pop();
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

    stream = producer->addMockActiveStream({},
                                           /*opaque*/ 0,
                                           vb,
                                           /*st_seqno*/ 0,
                                           /*en_seqno*/ ~0ULL,
                                           /*vb_uuid*/ 0xabcd,
                                           /*snap_start_seqno*/ 0,
                                           /*snap_end_seqno*/ ~0ULL);

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

// When the very first snapshot is to be filtered away (no items to be sent), we
// send a SnapshotMarker with the snapStartSeqno and snapEndSeqno of the
// StreamRequest + a SeqnoAdvanced to the snapEndSeqno. This completes the
// snapshot the consumer is streaming.
// However, since we apply this logic to the original requested snapshot range,
// we need to validate that this range does not extend beyond the vBucket
// highSeqno. The requested range may extend past the vb highSeqno only if the
// startSeqno and snapStartSeqno are the same (the consumer is a the begninning
// of a snapshot) in which case we don't rollback (as there is no data to
// rollback).
// The consumer may send a range which extends past the highSeqno if it saw a
// SnapshotMarker before a crash or failover, and the range that was going to be
// sent with that SnapshotMarker was lost. The consumer is allowed to reconnect
// with the received SnapshotMarker. This is the case where the consumer
// reconnects "in the middle" of a snapshot, only "the middle" is actually the
// start.
TEST_P(SingleThreadedActiveStreamTest, ConsumerSnapEndLimitedByHighSeqno) {
    // This only applies to connections supporting collections.
    cookie_to_mock_cookie(cookie)->setCollectionsSupport(true);
    setupProducer({});

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    stream.reset();
    auto& vb = *engine->getVBucket(vbid);
    auto& manager = *vb.checkpointManager;

    store_item(vbid, makeStoredDocKey("key1"), "value");
    manager.createNewCheckpoint();
    flushVBucketToDiskIfPersistent(vbid, 1 /*expected_num_flushed*/);

    const uint64_t highSeqno = manager.getHighSeqno();
    const uint64_t snapStartSeqno = highSeqno;
    const uint64_t snapEndSeqno = highSeqno + 1;
    uint64_t rollbackSeqno;

    // Sanity checks
    ASSERT_EQ(cb::engine_errc::out_of_range,
              producer->streamRequest(cb::mcbp::DcpAddStreamFlag::None,
                                      0,
                                      vbid,
                                      snapStartSeqno,
                                      ~0ULL,
                                      vb.failovers->getLatestUUID(),
                                      snapEndSeqno,
                                      snapEndSeqno,
                                      &rollbackSeqno,
                                      mock_dcp_add_failover_log,
                                      {}));
    ASSERT_EQ(cb::engine_errc::rollback,
              producer->streamRequest(cb::mcbp::DcpAddStreamFlag::None,
                                      0,
                                      vbid,
                                      snapEndSeqno,
                                      ~0ULL,
                                      vb.failovers->getLatestUUID(),
                                      snapEndSeqno,
                                      snapEndSeqno,
                                      &rollbackSeqno,
                                      mock_dcp_add_failover_log,
                                      {}));

    // Succeed with highSeqno == startSeqno == snapStartSeqno but snapEndSeqno
    // "in the future".
    EXPECT_EQ(cb::engine_errc::success,
              producer->streamRequest(cb::mcbp::DcpAddStreamFlag::None,
                                      0,
                                      vbid,
                                      highSeqno,
                                      ~0ULL,
                                      vb.failovers->getLatestUUID(),
                                      snapStartSeqno,
                                      snapEndSeqno,
                                      &rollbackSeqno,
                                      mock_dcp_add_failover_log,
                                      {}));

    auto stream = producer->findStream(vbid);
    EXPECT_EQ(highSeqno, stream->getSnapEndSeqno())
            << "Expected that the snapEndSeqno is ignored";
}

// We decided to not apply the logic tested by ConsumerSnapEndLimitedByHighSeqno
// to replication streams.
TEST_P(SingleThreadedActiveStreamTest,
       ConsumerSnapEndLimitedByHighSeqno_Replication) {
    cookie_to_mock_cookie(cookie)->setCollectionsSupport(false);
    setupProducer({});

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    stream.reset();
    auto& vb = *engine->getVBucket(vbid);
    auto& manager = *vb.checkpointManager;

    store_item(vbid, makeStoredDocKey("key1"), "value");
    manager.createNewCheckpoint();
    flushVBucketToDiskIfPersistent(vbid, 1 /*expected_num_flushed*/);

    const uint64_t highSeqno = manager.getHighSeqno();
    const uint64_t snapStartSeqno = highSeqno;
    const uint64_t snapEndSeqno = highSeqno + 1;
    uint64_t rollbackSeqno;

    // Succeed with highSeqno == startSeqno == snapStartSeqno but snapEndSeqno
    // "in the future".
    EXPECT_EQ(cb::engine_errc::success,
              producer->streamRequest(cb::mcbp::DcpAddStreamFlag::None,
                                      0,
                                      vbid,
                                      highSeqno,
                                      ~0ULL,
                                      vb.failovers->getLatestUUID(),
                                      snapStartSeqno,
                                      snapEndSeqno,
                                      &rollbackSeqno,
                                      mock_dcp_add_failover_log,
                                      {}));

    auto stream = producer->findStream(vbid);
    EXPECT_EQ(snapEndSeqno, stream->getSnapEndSeqno())
            << "Expected that the snapEndSeqno is preserved when SeqnoAdvanced "
               "is not available";
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
    ckptMgr.createNewCheckpoint();
    ASSERT_GT(ckptMgr.getOpenCheckpointId(), openId);
    flushVBucketToDiskIfPersistent(vbid, 1 /*expected_num_flushed*/);
    ASSERT_EQ(1, ckptMgr.getNumCheckpoints());
    ASSERT_EQ(1, ckptMgr.getNumOpenChkItems());

    // Re-create producer now we have items only on disk. We want to stream up
    // to seqno 1 (our only item) to test that we get the StreamEnd message.
    stream = producer->addMockActiveStream({} /*flags*/,
                                           0 /*opaque*/,
                                           *vb,
                                           0 /*st_seqno*/,
                                           1 /*en_seqno*/,
                                           0x0 /*vb_uuid*/,
                                           0 /*snap_start_seqno*/,
                                           ~0ULL /*snap_end_seqno*/);
    ASSERT_TRUE(stream->isBackfilling());

    MockDcpMessageProducers producers;

    // Step to schedule our backfill
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(false, producers));
    EXPECT_EQ(0, stream->public_readyQ().size());

    auto& bfm = producer->getBFM();

    ThreadGate tg1(2);
    ThreadGate tg2(2);
    std::thread t1;
    stream->setCompleteBackfillHook([this, &t1, &tg1, &tg2, &producers]() {
        // Step past our normal items to expose the race with backfill complete
        // and an empty readyQueue.

        EXPECT_EQ(1, *stream->getNumBackfillItemsRemaining());
        EXPECT_EQ(2, stream->public_readyQ().size());

        // Step snapshot marker
        EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers.last_op);

        // Step mutation
        EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers.last_op);

        stream->setNextHook([&tg1, &tg2](const DcpResponse* response) {
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
            EXPECT_EQ(cb::engine_errc::success,
                      producer->step(false, producers));
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
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(false, producers));
    EXPECT_FALSE(producer->findStream(vbid));
    EXPECT_TRUE(producer->getReadyQueue().empty());
}

// MB-54591: An ActiveStream can lose a notification of a new seqno if
// the notification occurs while the frontend DCP thread is finishing processing
// the previous item(s) via ActiveStream::next(). Specifically if
// notifyStreamReady() is called before itemsReady is cleared at the end of
// ActiveStream::next().
// This results in the DCP stream not waking and not sending out the affected
// seqno(s) until another mutation for that vBucket occurs.
TEST_P(SingleThreadedActiveStreamTest,
       RaceBetweenNotifyAndProcessingExistingItems) {
    auto vb = engine->getVBucket(vbid);
    stream = producer->addMockActiveStream({},
                                           /*opaque*/ 0,
                                           *vb,
                                           /*st_seqno*/ 0,
                                           /*en_seqno*/ ~0ULL,
                                           /*vb_uuid*/ 0xabcd,
                                           /*snap_start_seqno*/ 0,
                                           /*snap_end_seqno*/ ~0ULL);
    auto& connMap = static_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    connMap.addConn(cookie, producer);
    connMap.addVBConnByVBId(*producer, vbid);

    // Add an initial item which we will correctly process.
    store_item(vbid, makeStoredDocKey("key1"), "value");

    // step() the producer to schedule ActiveStreamCheckpointProcessorTask and
    // run it once to process the items from CkptManager into the Streams'
    // readyQ.
    GMockDcpMsgProducers producers;
    ASSERT_EQ(cb::engine_errc::would_block, producer->step(false, producers));
    auto& nonIO = *task_executor->getLpTaskQ(TaskType::NonIO);
    runNextTask(nonIO,
                "Process checkpoint(s) for DCP producer "
                "test_producer->test_consumer");

    // Setup Mock Producer expectations - we should see two snapshot
    // markers with one mutation each:
    {
        ::testing::InSequence dummy;
        using ::testing::_;
        using ::testing::Return;

        EXPECT_CALL(producers, marker(_, vbid, _, _, _, _, _, _, _, _))
                .WillOnce(Return(cb::engine_errc::success));

        EXPECT_CALL(producers, mutation(_, _, vbid, /*seqno*/ 1, _, _, _, _))
                .WillOnce(Return(cb::engine_errc::success));

        EXPECT_CALL(producers, marker(_, vbid, _, _, _, _, _, _, _, _))
                .WillOnce(Return(cb::engine_errc::success));

        EXPECT_CALL(producers, mutation(_, _, vbid, /*seqno*/ 2, _, _, _, _))
                .WillOnce(Return(cb::engine_errc::success));
    }

    // Step DCP producer twice to generate the initial snapshot marker and
    // mutation to "key1"
    ASSERT_EQ(cb::engine_errc::success, producer->step(false, producers));
    ASSERT_EQ(cb::engine_errc::success, producer->step(false, producers));

    // Step again - but this time configure a callback in ActiveStream::next()
    // which will perform another front-end store(). This _should_ result in
    // ActiveStream::notifyStreamReady() notifying the Producer via
    // Producer::notifyStreamReady() and waking up the front-end again - but in
    // the case of the bug this wakeup was missed.

    // Note we must perform the store() on a different thread (instead of
    // directly inside the hook) otherwise we will encounter lock inversions.
    folly::Baton baton;
    auto frontEndThread = std::thread([&] {
        baton.wait();
        store_item(vbid, makeStoredDocKey("key2"), "value");
    });

    bool extraStoreIssued = false;
    stream->setNextHook([&](const DcpResponse* response) {
        // Only want to add one extra mutation.
        if (extraStoreIssued) {
            return;
        }
        EXPECT_FALSE(response)
                << "ActiveStream::next() hook expected to only be called for "
                   "nullptr response when all previous items processed.";

        baton.post();
        frontEndThread.join();
        extraStoreIssued = true;
    });

    // Call step - this calls ActiveStream::next() which initially returns
    // nullptr as CkptManager has no more items, but our callback above adds
    // another mutation to CM at the end of ActiveStream::next(). With the bug
    // we miss the wakeup and producer->step() returns without scheduling
    // any more work - so runNextTask below fails.
    // With the bug fixed it will call ActiveStream::next() again, spot there's
    // a new item in CM and schedule the ActiveStreamCheckpointProcessorTask.
    ASSERT_EQ(cb::engine_errc::would_block, producer->step(false, producers));

    producer->getCheckpointSnapshotTask()->run();

    // Once the task has run then it should have notified the producer again.
    EXPECT_TRUE(producer->getReadyQueue().exists(vbid));

    // Step the producer to consume the second snapshot marker and key2.
    // Should finish with would_block to indicate no more data ready.
    EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));
    EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(false, producers));

    // Cleanup
    connMap.removeVBConnByVBId(cookie, vbid);
    connMap.removeConn(cookie);
}

void SingleThreadedActiveStreamTest::testProducerIncludesUserXattrsInDelete(
        const std::optional<cb::durability::Requirements>& durReqs) {
    using cb::mcbp::DcpOpenFlag;

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
    ASSERT_NE(0, deletion.getItem()->getNBytes());

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
        cb::mcbp::DcpOpenFlag flags,
        const std::optional<cb::durability::Requirements>& durReqs) {
    using DcpOpenFlag = cb::mcbp::DcpOpenFlag;

    // Test is executed also for SyncDelete
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    // Check that we are testing a valid configuration: here we want to test
    // only configurations that trigger user-xattr pruning in deletes.
    ASSERT_FALSE(isFlagSet(flags, DcpOpenFlag::IncludeDeletedUserXattrs));

    auto& vb = *engine->getVBucket(vbid);
    recreateProducerAndStream(vb, flags);

    const auto currIncDelUserXattr =
            isFlagSet(flags, DcpOpenFlag::IncludeDeletedUserXattrs)
                    ? IncludeDeletedUserXattrs::Yes
                    : IncludeDeletedUserXattrs::No;
    ASSERT_EQ(currIncDelUserXattr,
              producer->public_getIncludeDeletedUserXattrs());
    ASSERT_EQ(currIncDelUserXattr,
              stream->public_getIncludeDeletedUserXattrs());

    const auto currIncXattr = isFlagSet(flags, DcpOpenFlag::IncludeXattrs)
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
            [&vb, &originalValue, &originalSizes, &durReqs, this]() -> void {
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
        ASSERT_EQ(ephemeral() ? 2 : 4, ckpt->getNumItems());
        EXPECT_EQ(queue_op::checkpoint_start, (*it)->getOperation());
        if (!ephemeral()) {
            it++;
            EXPECT_EQ(queue_op::set_vbucket_state, (*it)->getOperation());
            it++;
            EXPECT_EQ(queue_op::set_vbucket_state, (*it)->getOperation());
        }
        // 1 non-metaitem is our deletion
        it++;
        ASSERT_TRUE((*it)->isDeleted());
        const auto expectedOp =
                durReqs ? queue_op::pending_sync_write : queue_op::mutation;
        EXPECT_EQ(expectedOp, (*it)->getOperation());

        // Byte-by-byte comparison
        EXPECT_EQ(originalValue, (*it)->getValue()->to_string_view());

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
        CB_SCOPED_TRACE("");
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
        CB_SCOPED_TRACE("");
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

    // Check that we stream the expected value.
    // What value we stream depends on the current configuration:
    // - if the test flags=0, then we want to prune everything, so no value
    // - else if the test flags=IncludeXattr, then we want only sys-xattrs as
    //   IncludeDeleteUserXattrs::No

    const auto* data = deletion.getItem()->getData();
    const auto nBytes = deletion.getItem()->getNBytes();

    const auto valueBuf = cb::char_buffer(const_cast<char*>(data), nBytes);

    // If we have a value..
    if (!valueBuf.empty()) {
        // Check that we have no body (bodySize=0)
        std::string_view body{data, nBytes};
        body.remove_prefix(cb::xattr::get_body_offset(body));
        ASSERT_EQ(0, body.size());
    }

    // Check that we have the expected value
    if (flags == cb::mcbp::DcpOpenFlag::None) {
        // No value
        // Note: DT for no-value must be RAW
        ASSERT_EQ(PROTOCOL_BINARY_RAW_BYTES, deletion.getItem()->getDataType());
        // Note: I would expect valueBuf.data()==nullptr, but was not the case
        //  before my see Item::setData
        ASSERT_EQ(0, valueBuf.size());
    } else {
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
    testProducerPrunesUserXattrsForDelete(cb::mcbp::DcpOpenFlag::IncludeXattrs,
                                          {});
}

TEST_P(SingleThreadedActiveStreamTest,
       ProducerPrunesUserXattrsForSyncDelete_NoDeleteUserXattrs) {
    testProducerPrunesUserXattrsForDelete(cb::mcbp::DcpOpenFlag::IncludeXattrs,
                                          cb::durability::Requirements());
}

TEST_P(SingleThreadedActiveStreamTest,
       ProducerPrunesUserXattrsForNormalDelete_NoXattrs) {
    testProducerPrunesUserXattrsForDelete(cb::mcbp::DcpOpenFlag::None, {});
}

TEST_P(SingleThreadedActiveStreamTest,
       ProducerPrunesUserXattrsForSyncDelete_NoXattrs) {
    testProducerPrunesUserXattrsForDelete(cb::mcbp::DcpOpenFlag::None,
                                          cb::durability::Requirements());
}

using cb::mcbp::DcpOpenFlag;

void SingleThreadedActiveStreamTest::testExpirationRemovesBody(
        DcpOpenFlag flags, Xattrs xattrs, ExpiryPath path) {
    auto& vb = *engine->getVBucket(vbid);
    recreateProducerAndStream(vb, DcpOpenFlag::IncludeXattrs | flags);

    const auto currIncDelUserXattr =
            isFlagSet(flags, DcpOpenFlag::IncludeDeletedUserXattrs)
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
    const auto docKey = DocKeyView{key, DocKeyEncodesCollectionId::No};
    store_item(vbid,
               docKey,
               value,
               ep_convert_to_expiry_time(1), /*1 second TTL*/
               {cb::engine_errc::success} /*expected*/,
               datatype);

    auto& manager = *vb.checkpointManager;
    const auto& list =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    manager);
    ASSERT_EQ(1, list.size());
    auto* ckpt = list.front().get();
    ASSERT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckpt->getState());
    ASSERT_EQ(ephemeral() ? 2 : 3, ckpt->getNumItems());
    auto it = ckpt->begin(); // empty-item
    it++; // checkpoint-start
    if (!ephemeral()) {
        ++it; // set-vbstate
    }
    it++;
    EXPECT_EQ(queue_op::mutation, (*it)->getOperation());
    EXPECT_FALSE((*it)->isDeleted());
    EXPECT_EQ(value, (*it)->getValue()->to_string_view());

    TimeTraveller tt(5000);

    manager.createNewCheckpoint();

    // Trigger the expiration
    switch (path) {
    case ExpiryPath::Access: {
        // Frontend access (cmd GET)
        GetValue gv = store->get(docKey, vbid, cookie, get_options_t::NONE);
        EXPECT_EQ(cb::engine_errc::no_such_key, gv.getStatus());
        break;
    }
    case ExpiryPath::Deletion: {
        // Explicit deletion (cmd DEL), which processes the TTL (if any) before
        // proceeding with the explicit deletion
        delete_item(vbid, docKey, cb::engine_errc::no_such_key);
        break;
    }
    }

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

TEST_P(SingleThreadedActiveStreamTest, ExpirationRemovesBody_Pre66) {
    testExpirationRemovesBody(
            DcpOpenFlag::None, Xattrs::None, ExpiryPath::Access);
}

TEST_P(SingleThreadedActiveStreamTest, ExpirationRemovesBody_Pre66_UserXa) {
    testExpirationRemovesBody(
            DcpOpenFlag::None, Xattrs::User, ExpiryPath::Access);
}

TEST_P(SingleThreadedActiveStreamTest,
       ExpirationRemovesBody_Pre66_UserXa_SysXa) {
    testExpirationRemovesBody(
            DcpOpenFlag::None, Xattrs::UserAndSys, ExpiryPath::Access);
}

TEST_P(SingleThreadedActiveStreamTest, ExpirationRemovesBody) {
    testExpirationRemovesBody(DcpOpenFlag::IncludeDeletedUserXattrs,
                              Xattrs::None,
                              ExpiryPath::Access);
}

TEST_P(SingleThreadedActiveStreamTest, ExpirationRemovesBody_UserXa) {
    testExpirationRemovesBody(DcpOpenFlag::IncludeDeletedUserXattrs,
                              Xattrs::User,
                              ExpiryPath::Access);
}

TEST_P(SingleThreadedActiveStreamTest, ExpirationRemovesBody_UserXa_SysXa) {
    testExpirationRemovesBody(DcpOpenFlag::IncludeDeletedUserXattrs,
                              Xattrs::UserAndSys,
                              ExpiryPath::Access);
}

TEST_P(SingleThreadedActiveStreamTest, ExpByDel_ExpirationRemovesBody) {
    testExpirationRemovesBody(DcpOpenFlag::IncludeDeletedUserXattrs,
                              Xattrs::None,
                              ExpiryPath::Deletion);
}

TEST_P(SingleThreadedActiveStreamTest, ExpByDel_ExpirationRemovesBody_UserXa) {
    testExpirationRemovesBody(DcpOpenFlag::IncludeDeletedUserXattrs,
                              Xattrs::User,
                              ExpiryPath::Deletion);
}

TEST_P(SingleThreadedActiveStreamTest,
       ExpByDel_ExpirationRemovesBody_UserXa_SysXa) {
    testExpirationRemovesBody(DcpOpenFlag::IncludeDeletedUserXattrs,
                              Xattrs::UserAndSys,
                              ExpiryPath::Deletion);
}

/**
 * This test covers starting a stream at seqno X from a checkpoint with start
 * seqno X-N, where the first N items have been expelled. In this case, since
 * the next mutation will be X+1 and that has not been expelled, we don't have
 * to backfill. The first snapshot marker should have a snap_start_seqno of X,
 * and not X-N (the start seqno of the checkpoint).
 */
TEST_P(SingleThreadedActiveStreamTest, SnapshotForCheckpointWithExpelledItems) {
    // We need to re-create the stream with a specific startSeqno
    stream.reset();

    auto& vb = *engine->getVBucket(vbid);
    auto& manager = *vb.checkpointManager;
    ASSERT_EQ(1, manager.getOpenCheckpointId());
    manager.createNewCheckpoint();

    const auto& checkpoint =
            CheckpointManagerTestIntrospector::public_getOpenCheckpoint(
                    manager);
    ASSERT_EQ(checkpoint.getNumberOfElements(), 2);

    store_item(vbid, makeStoredDocKey("seqno-1"), "");
    store_item(vbid, makeStoredDocKey("seqno-2"), "");
    store_item(vbid, makeStoredDocKey("seqno-3"), "");

    ASSERT_EQ(checkpoint.getHighSeqno(), 3);
    ASSERT_EQ(checkpoint.getNumberOfElements(), 5);

    // Unexpelled items at seqno:4
    store_item(vbid, makeStoredDocKey("seqno-4"), "");
    store_item(vbid, makeStoredDocKey("seqno-5"), "");

    flushVBucketToDiskIfPersistent(vbid, 5);

    const auto stopExpelCursor =
        manager.registerCursorBySeqno(
                        "test-cursor", 3, CheckpointCursor::Droppable::Yes)
                .takeCursor()
                .lock();
    EXPECT_EQ(2, manager.getNumItemsForCursor(*stopExpelCursor));
    // Expel up to seqno:3
    EXPECT_EQ(3, manager.expelUnreferencedCheckpointItems().count);
    EXPECT_EQ(2, manager.getNumItemsForCursor(*stopExpelCursor));

    ASSERT_EQ(checkpoint.getNumberOfElements(), 4);

    // Simulate client reconnecting at seqno:3.
    stream = producer->addMockActiveStream({} /*flags*/,
                                           0 /*opaque*/,
                                           vb,
                                           3 /*st_seqno*/,
                                           ~0ULL /*en_seqno*/,
                                           vb.failovers->getLatestUUID(),
                                           3 /*snap_start_seqno*/,
                                           3 /*snap_end_seqno*/);
    EXPECT_EQ(3, stream->getItemsRemaining());

    auto outstandingItems = stream->public_getOutstandingItems(vb);
    stream->public_processItems(outstandingItems);

    const auto& readyQ = stream->public_readyQ();
    ASSERT_EQ(3, readyQ.size());

    auto resp = stream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_EQ(3, snapMarker.getStartSeqno())
            << "Snapshot start seqno should be that of the first mutation";

    resp = stream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    auto mutation = dynamic_cast<MutationResponse&>(*resp);
    EXPECT_EQ(4, mutation.getBySeqno())
            << "Unexpected seqno for first mutation response";
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
    vb.updateFromManifest(
            std::shared_lock<folly::SharedMutex>(vb.getStateLock()),
            makeManifest(cm));
    EXPECT_EQ(2, vb.getHighSeqno());

    // Ensure backfill
    const auto openId = manager.getOpenCheckpointId();
    manager.createNewCheckpoint();
    ASSERT_GT(manager.getOpenCheckpointId(), openId);
    flushVBucketToDiskIfPersistent(vbid, 2 /*expected_num_flushed*/);
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getNumOpenChkItems());

    // Re-create producer and stream
    const std::string jsonFilter =
            fmt::format(R"({{"collections":["{}", "{}"]}})",
                        uint32_t(CollectionEntry::defaultC.getId()),
                        uint32_t(collection.getId()));
    const std::optional<std::string_view> json(jsonFilter);
    recreateProducerAndStream(vb, cb::mcbp::DcpOpenFlag::NoValue, json);
    ASSERT_TRUE(producer);
    producer->createCheckpointProcessorTask();
    ASSERT_TRUE(stream);
    ASSERT_TRUE(stream->isBackfilling());
    ASSERT_EQ(IncludeValue::No, stream->public_getIncludeValue());
    auto resp = stream->next(*producer);
    EXPECT_FALSE(resp);

    auto& bfm = producer->getBFM();
    EXPECT_EQ(1, bfm.getNumBackfills());
    // Before the fix this step throws with:
    //     Collections::VB::Manifest::verifyFlatbuffersData:
    //     getCollectionEventData data invalid, .., size:0"
    EXPECT_EQ(backfill_success, bfm.backfill()); // create->scan

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
        ASSERT_EQ(3, vb->getHighSeqno());
        ckptMgr.createNewCheckpoint();

        const auto& stats = engine->getEpStats();
        if (isPersistent()) {
            ASSERT_EQ(0, stats.itemsRemovedFromCheckpoints);
            flushVBucketToDiskIfPersistent(vbid, 3);
        }
        // cs, vbs, 3 mut(s), ce
        ASSERT_EQ(ephemeral() ? 5 : 6, stats.itemsRemovedFromCheckpoints);

        // Re-create the stream now we have items only on disk.
        stream = producer->addMockActiveStream({} /*flags*/,
                                               0 /*opaque*/,
                                               *vb,
                                               0 /*st_seqno*/,
                                               ~0ULL /*en_seqno*/,
                                               0x0 /*vb_uuid*/,
                                               0 /*snap_start_seqno*/,
                                               ~0ULL /*snap_end_seqno*/);
        ASSERT_TRUE(stream->isBackfilling());

        // Should report empty itemsRemaining as that would mislead
        // ns_server if they asked for stats before the backfill task runs (they
        // would think backfill is complete).
        EXPECT_FALSE(stream->getNumBackfillItemsRemaining());

        // Run the backfill we scheduled when we transitioned to the backfilling
        // state.
        auto& bfm = producer->getBFM();

        // First item
        EXPECT_EQ(backfill_status_t::backfill_success, bfm.backfill());
        EXPECT_EQ(1, stream->getNumBackfillItems());

        // Step the snapshot marker and first mutation
        MockDcpMessageProducers producers;
        EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers.last_op);
        EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers.last_op);
        EXPECT_EQ(1, producers.last_byseqno);

        // Second item
        EXPECT_EQ(backfill_status_t::backfill_success, bfm.backfill());
        EXPECT_EQ(2, stream->getNumBackfillItems());

        // Step the second mutation
        EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers.last_op);
        EXPECT_EQ(2, producers.last_byseqno);

        // Third item
        EXPECT_EQ(backfill_status_t::backfill_success, bfm.backfill());
        EXPECT_EQ(3, stream->getNumBackfillItems());

        // Step the third mutation
        EXPECT_EQ(cb::engine_errc::success, producer->step(false, producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers.last_op);
        EXPECT_EQ(3, producers.last_byseqno);

        // By MB-53806 backfill first pushes over the stream then yields. So,
        // we need an extra run after the last item for settling.
        EXPECT_EQ(backfill_status_t::backfill_success, bfm.backfill());

        // No more backfills
        EXPECT_EQ(backfill_status_t::backfill_finished, bfm.backfill());

        // Nothing more to step in the producer
        EXPECT_EQ(cb::engine_errc::would_block,
                  producer->step(false, producers));
    }
};

TEST_P(SingleThreadedActiveStreamTest,
       OSOBackfillResumesFromNextItemAfterPause) {
    if (ephemeral()) {
        GTEST_SKIP();
    }
    // require an OSO backfill for this test, using "auto" will skip and fail
    engine->getConfiguration().setDcpOsoBackfill("enabled");

    auto vb = engine->getVBucket(vbid);
    auto& ckptMgr = *vb->checkpointManager;
    // Get rid of set_vb_state and any other queue_op we are not interested in
    ckptMgr.clear(0 /*seqno*/);

    // Remove the initial stream, we want to force it to backfill.
    stream.reset();

    producer->setOutOfOrderSnapshots(OutOfOrderSnapshots::Yes);
    producer->setBackfillBufferSize(1);

    const auto keyA = makeStoredDocKey("keyA");
    const auto keyB = makeStoredDocKey("keyB");
    for (const auto& key : {keyA, keyB}) {
        auto item = make_item(vbid, key, "value");
        EXPECT_EQ(MutationStatus::WasClean,
                  public_processSet(
                          *vb, item, VBQueueItemCtx(CanDeduplicate::Yes)));
    }

    // Ensure mutation is on disk; no longer present in CheckpointManager.
    vb->checkpointManager->createNewCheckpoint();
    flushVBucketToDiskIfPersistent(vbid, 2);

    recreateStream(*vb);
    ASSERT_TRUE(stream->isBackfilling());

    // Run the backfill we scheduled when we transitioned to the backfilling
    // state. Only run the backfill task once because we only care about the
    // snapshot marker.
    auto& bfm = producer->getBFM();
    bfm.backfill();

    // No message processed, BufferLog empty
    ASSERT_EQ(0, producer->getBytesOutstanding());

    // readyQ must contain a OSOSnapshot message.
    // Plus, scan runs and pushes 1 item to the readyQ and yields by max scan
    // buffer full. That happens because (differently from Neo) in Trinity
    // backfill yields by OOM after processing the item at ::backfillReceived().
    ASSERT_EQ(stream->public_readyQSize(), 2);
    auto resp = stream->public_backfillPhase(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::OSOSnapshot, resp->getEvent());
    auto& marker = dynamic_cast<OSOSnapshot&>(*resp);
    EXPECT_TRUE(marker.isStart());

    ASSERT_EQ(stream->public_readyQSize(), 1);
    // Note: This call releases bytes from scan buffer. The next scan() will
    // push the next item to the readyQ
    resp = stream->public_backfillPhase(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    auto* mutation = dynamic_cast<MutationResponse*>(resp.get());
    EXPECT_EQ(keyA, mutation->getItem()->getKey());

    // Resume backfill - That must resume from the next item (keyB)
    // Before the fix for MB-57106, at this step scan() pushes keyA to readyQ
    // again.
    EXPECT_EQ(backfill_success, bfm.backfill());
    EXPECT_EQ(stream->public_readyQSize(), 1);
    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    mutation = dynamic_cast<MutationResponse*>(resp.get());
    EXPECT_EQ(keyB, mutation->getItem()->getKey());

    producer->cancelCheckpointCreatorTask();
}

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
    // cs, vbs
    ASSERT_EQ(2, ckptList.front()->getNumItems());
    ASSERT_EQ(0, manager.getHighSeqno());

    // Replica receives a complete disk snapshot {keyA:1, keyB:2}
    const uint32_t opaque = 1;
    const uint64_t snapStart = 1;
    const uint64_t snapEnd = 2;
    EXPECT_EQ(
            cb::engine_errc::success,
            consumer->snapshotMarker(opaque,
                                     vbid,
                                     snapStart,
                                     snapEnd,
                                     DcpSnapshotMarkerFlag::Disk |
                                             DcpSnapshotMarkerFlag::Checkpoint,
                                     {} /*HCS*/,
                                     {}, /*HPS*/
                                     {} /*maxVisibleSeqno*/,
                                     {} /*purgeSeqno*/));
    ASSERT_EQ(1, ckptList.size());
    ASSERT_TRUE(ckptList.front()->isDiskCheckpoint());
    ASSERT_EQ(1, ckptList.front()->getSnapshotStartSeqno());
    ASSERT_EQ(2, ckptList.front()->getSnapshotEndSeqno());
    // cs
    ASSERT_EQ(1, ckptList.front()->getNumItems());
    ASSERT_EQ(0, manager.getHighSeqno());

    const auto keyA = makeStoredDocKey("keyA");
    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(
                      opaque, keyA, {}, 0, 0, vbid, 0, snapStart, 0, 0, 0, 0));
    const auto keyB = makeStoredDocKey("keyB");
    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(
                      opaque, keyB, {}, 0, 0, vbid, 0, snapEnd, 0, 0, 0, 0));

    ASSERT_EQ(1, ckptList.size());
    ASSERT_TRUE(ckptList.front()->isDiskCheckpoint());
    ASSERT_EQ(1, ckptList.front()->getSnapshotStartSeqno());
    ASSERT_EQ(2, ckptList.front()->getSnapshotEndSeqno());
    ASSERT_EQ(3, ckptList.front()->getNumItems());
    ASSERT_EQ(2, manager.getHighSeqno());

    // Move the persistence cursor to point to keyB:2.
    flush_vbucket_to_disk(vbid, 2 /*expected_num_flushed*/);
    const auto pCursorPos = manager.getPersistenceCursorPos();
    ASSERT_EQ(keyB, (*pCursorPos)->getKey());
    ASSERT_EQ(2, (*pCursorPos)->getBySeqno());

    auto openId = manager.getOpenCheckpointId();

    // Simulate a possible behaviour in pre-6.5: Producer may send a SnapMarker
    // and miss to set the DcpSnapshotMarkerFlag::Checkpoint,
    // eg MB-32862
    EXPECT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(opaque,
                                       vbid,
                                       3 /*snapStart*/,
                                       3 /*snapEnd*/,
                                       DcpSnapshotMarkerFlag::Memory,
                                       {} /*HCS*/,
                                       {}, /*HPS*/
                                       {} /*maxVisibleSeqno*/,
                                       {} /*purgeSeqno*/));

    // 6.6.1 PassiveStream is resilient to any Active misbehaviour with regard
    // to DcpSnapshotMarkerFlag::Checkpoint. Even if Active
    // missed to set the flag, Replica closes the checkpoint and creates a new
    // one for queueing the new Memory snapshot. This is an important step in
    // the test. Essentially here we verify that the fix eliminates one of the
    // preconditions for hitting the issue: snapshots cannot be merged into the
    // same checkpoint if the merge involves Disk snapshots.
    ASSERT_EQ(1, ckptList.size());
    ASSERT_GT(manager.getOpenCheckpointId(), openId);
    openId = manager.getOpenCheckpointId();
    ASSERT_EQ(CheckpointType::Memory, ckptList.front()->getCheckpointType());
    ASSERT_EQ(CHECKPOINT_OPEN, ckptList.front()->getState());
    ASSERT_EQ(3, ckptList.front()->getSnapshotStartSeqno());
    ASSERT_EQ(3, ckptList.front()->getSnapshotEndSeqno());
    // cs
    ASSERT_EQ(1, ckptList.front()->getNumItems());
    ASSERT_EQ(2, manager.getHighSeqno());

    // Now replica receives a doc within the new Memory snapshot.
    // Note: This step queues keyC into the checkpoint. Given that it is a
    //  Memory checkpoint, then keyC is added to the keyIndex too.
    //  That is a precondition for executing the deduplication path that throws
    //  in Checkpoint::queueDirty before the fix.
    const auto keyC = makeStoredDocKey("keyC");
    EXPECT_EQ(
            cb::engine_errc::success,
            consumer->mutation(
                    opaque, keyC, {}, 0, 0, vbid, 0, 3 /*seqno*/, 0, 0, 0, 0));

    ASSERT_EQ(1, ckptList.size());
    ASSERT_EQ(CheckpointType::Memory, ckptList.back()->getCheckpointType());
    ASSERT_EQ(CHECKPOINT_OPEN, ckptList.back()->getState());
    ASSERT_EQ(3, ckptList.back()->getSnapshotStartSeqno());
    ASSERT_EQ(3, ckptList.back()->getSnapshotEndSeqno());
    ASSERT_EQ(2, ckptList.back()->getNumItems());
    ASSERT_EQ(3, manager.getHighSeqno());

    // Another SnapMarker with no
    // DcpSnapshotMarkerFlag::Checkpoint Note: This is not
    // due to any pre-6.5 bug, this is legal also in 6.6.x and
    //  7.x. The active may generate multiple Memory snapshots from the same
    //  physical checkpoint. Those snapshots may contain duplicates of the same
    //  key.
    EXPECT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(opaque,
                                       vbid,
                                       4 /*snapStart*/,
                                       4 /*snapEnd*/,
                                       DcpSnapshotMarkerFlag::Memory,
                                       {} /*HCS*/,
                                       {} /*HPS*/,
                                       {} /*maxVisibleSeqno*/,
                                       {} /*purgeSeqno*/));
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
    ASSERT_EQ(2, ckptList.back()->getNumItems());
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
    EXPECT_EQ(
            cb::engine_errc::success,
            consumer->mutation(
                    opaque, keyC, {}, 0, 0, vbid, 0, 4 /*seqno*/, 0, 0, 0, 0));

    // Check that we have executed the deduplication path, the test is invalid
    // otherwise.
    ASSERT_EQ(1, ckptList.size());
    ASSERT_EQ(openId, manager.getOpenCheckpointId());
    ASSERT_EQ(CheckpointType::Memory, ckptList.back()->getCheckpointType());
    ASSERT_EQ(CHECKPOINT_OPEN, ckptList.back()->getState());
    ASSERT_EQ(3, ckptList.back()->getSnapshotStartSeqno());
    ASSERT_EQ(4, ckptList.back()->getSnapshotEndSeqno());
    ASSERT_EQ(2, ckptList.back()->getNumItems());
    ASSERT_EQ(4, manager.getHighSeqno());
}

TEST_P(SingleThreadedPassiveStreamTest, GetSnapshotInfo) {
    auto& vb = *store->getVBucket(vbid);
    ASSERT_EQ(0, vb.getHighSeqno());
    auto& manager = static_cast<MockCheckpointManager&>(*vb.checkpointManager);
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(2, manager.getNumOpenChkItems()); // cs, vbs

    removeCheckpoint(vb);
    ASSERT_EQ(0, vb.getHighSeqno());
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getNumOpenChkItems()); // cs
    const auto& list = manager.getCheckpointList();
    ASSERT_FALSE(list.back()->modifiedByExpel());

    // The stream isn't in any snapshot (no data received)
    auto snapInfo = manager.getSnapshotInfo();
    EXPECT_EQ(0, snapInfo.start);
    EXPECT_EQ(snapshot_range_t(0, 0), snapInfo.range);

    const uint64_t opaque = 1;
    EXPECT_EQ(
            cb::engine_errc::success,
            consumer->snapshotMarker(opaque,
                                     vbid,
                                     1, // start
                                     2, // end
                                     DcpSnapshotMarkerFlag::Memory |
                                             DcpSnapshotMarkerFlag::Checkpoint,
                                     {},
                                     {},
                                     {},
                                     {}));

    const auto key = makeStoredDocKey("key");
    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(opaque,
                                 key,
                                 {},
                                 0,
                                 0,
                                 vbid,
                                 0,
                                 1, // seqno
                                 0,
                                 0,
                                 0,
                                 0));

    EXPECT_EQ(1, vb.getHighSeqno());
    EXPECT_EQ(2, manager.getNumOpenChkItems()); // cs, mut

    // The stream is in a partial snapshot here
    snapInfo = manager.getSnapshotInfo();
    EXPECT_EQ(1, snapInfo.start);
    EXPECT_EQ(snapshot_range_t(1, 2), snapInfo.range);

    // Move cursors to allow Expel
    flushVBucket(vbid);

    // Now Expel everything from the open checkpoint
    {
        EXPECT_EQ(0, manager.getNumItemsForPersistence());
        const auto res = manager.expelUnreferencedCheckpointItems();
        ASSERT_EQ(1, res.count); // mut
        ASSERT_TRUE(list.back()->modifiedByExpel());
        EXPECT_EQ(1, manager.getNumItems()); // 1 for checkpoint_start
        EXPECT_EQ(0, manager.getNumItemsForPersistence());
    }
    // Crucial test: We have expelled everything from the checkpoint and that
    // doesn't have to change the PassiveStream snapshot information.
    // Note: There were no production changes required for holding this
    // invariant.
    snapInfo = manager.getSnapshotInfo();
    EXPECT_EQ(1, snapInfo.start);
    EXPECT_EQ(snapshot_range_t(1, 2), snapInfo.range);

    // In the following we verify the snapshot-info behaviour in the case where
    // the open checkpoint contains only meta-items

    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(opaque,
                                 key,
                                 {},
                                 0,
                                 0,
                                 vbid,
                                 0,
                                 2, // seqno
                                 0,
                                 0,
                                 0,
                                 0));

    snapInfo = manager.getSnapshotInfo();
    EXPECT_EQ(2, snapInfo.start);
    EXPECT_EQ(snapshot_range_t(1, 2), snapInfo.range);
    EXPECT_EQ(2, manager.getNumItems()); // cs and mutation
    EXPECT_EQ(1, manager.getNumItemsForPersistence());

    EXPECT_EQ(
            cb::engine_errc::success,
            consumer->snapshotMarker(opaque,
                                     vbid,
                                     3, // start
                                     4, // end
                                     DcpSnapshotMarkerFlag::Memory |
                                             DcpSnapshotMarkerFlag::Checkpoint,
                                     {},
                                     {},
                                     {},
                                     {}));

    // No mutation in the new empty checkpoint, snapshot info is from the
    // previous checkpoint.
    EXPECT_EQ(2, manager.getNumCheckpoints());
    EXPECT_EQ(1, manager.getNumOpenChkItems()); // cs
    EXPECT_EQ(3, list.back()->getSnapshotStartSeqno());
    EXPECT_EQ(4, list.back()->getSnapshotEndSeqno());

    snapInfo = manager.getSnapshotInfo();
    EXPECT_EQ(2, snapInfo.start);
    EXPECT_EQ(snapshot_range_t(2, 2), snapInfo.range);

    // Now queue a meta-item
    consumer->setVBucketState(opaque, vbid, vbucket_state_pending);
    EXPECT_EQ(2, manager.getNumOpenChkItems()); // cs, vbs

    snapInfo = manager.getSnapshotInfo();
    EXPECT_EQ(2, snapInfo.start);
    EXPECT_EQ(snapshot_range_t(2, 2), snapInfo.range);

    // Try to expel the vbs meta-item.
    flushVBucket(vbid);

    {
        // MB-63341: SetVBState is expellable.
        const auto res = manager.expelUnreferencedCheckpointItems();
        EXPECT_EQ(1, res.count); // vbs gone
        EXPECT_TRUE(list.back()->modifiedByExpel());
    }

    snapInfo = manager.getSnapshotInfo();
    // Note: The open checkpoint range is (3, 4) but lastBySeqno=2 falls into
    // the previous checkpoint (which was the last that had stored a non-meta
    // item)
    EXPECT_EQ(2, snapInfo.start);
    EXPECT_EQ(snapshot_range_t(2, 2), snapInfo.range);
}

// Note: At the moment this test's purpose is to show how outbound DCP behaves
// at replica.
// @todo MB-59288
TEST_P(SingleThreadedPassiveStreamTest, BackfillSnapshotFromPartialReplica) {
    auto& vb = *store->getVBucket(vbid);
    ASSERT_EQ(0, vb.getHighSeqno());
    auto& manager = static_cast<MockCheckpointManager&>(*vb.checkpointManager);
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(2, manager.getNumOpenChkItems()); // cs, vbs
    removeCheckpoint(vb);
    ASSERT_EQ(0, vb.getHighSeqno());
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getNumOpenChkItems()); // cs
    const auto& list = manager.getCheckpointList();
    ASSERT_FALSE(list.back()->modifiedByExpel());

    // The stream isn't in any snapshot (no data received)
    auto snapInfo = manager.getSnapshotInfo();
    EXPECT_EQ(0, snapInfo.start);
    EXPECT_EQ(snapshot_range_t(0, 0), snapInfo.range);
    // Same on disk
    auto& underlying = *store->getRWUnderlying(vbid);
    ASSERT_EQ(0, underlying.getLastPersistedSeqno(vbid));
    auto vbstate = underlying.getPersistedVBucketState(vbid);
    ASSERT_EQ(0, vbstate.state.lastSnapStart);
    ASSERT_EQ(0, vbstate.state.lastSnapEnd);

    const uint64_t opaque = 1;
    EXPECT_EQ(
            cb::engine_errc::success,
            consumer->snapshotMarker(opaque,
                                     vbid,
                                     1, // start
                                     2, // end
                                     DcpSnapshotMarkerFlag::Memory |
                                             DcpSnapshotMarkerFlag::Checkpoint,
                                     {},
                                     {},
                                     {},
                                     {}));
    ASSERT_EQ(0, underlying.getLastPersistedSeqno(vbid));
    vbstate = underlying.getPersistedVBucketState(vbid);
    ASSERT_EQ(0, vbstate.state.lastSnapStart);
    ASSERT_EQ(0, vbstate.state.lastSnapEnd);
    const auto key = makeStoredDocKey("key");
    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(opaque,
                                 key,
                                 {},
                                 0,
                                 0,
                                 vbid,
                                 0,
                                 1, // seqno
                                 0,
                                 0,
                                 0,
                                 0));
    EXPECT_EQ(1, vb.getHighSeqno());
    EXPECT_EQ(2, manager.getNumOpenChkItems()); // cs, mut

    // The stream is in a partial snapshot here
    snapInfo = manager.getSnapshotInfo();
    EXPECT_EQ(1, snapInfo.start);
    EXPECT_EQ(snapshot_range_t(1, 2), snapInfo.range);
    // Disk too
    flushVBucketToDiskIfPersistent(vbid);
    ASSERT_EQ(1, underlying.getLastPersistedSeqno(vbid));
    vbstate = underlying.getPersistedVBucketState(vbid);
    ASSERT_EQ(0, vbstate.state.lastSnapStart);
    ASSERT_EQ(2, vbstate.state.lastSnapEnd);

    // Trigger an outbound backfill
    removeCheckpoint(vb);
    auto producer =
            std::make_shared<MockDcpProducer>(*engine,
                                              cookie,
                                              "test_producer->test_consumer",
                                              cb::mcbp::DcpOpenFlag::None,
                                              false);
    producer->createCheckpointProcessorTask();
    producer->scheduleCheckpointProcessorTask();
    auto activeStream = std::make_shared<MockActiveStream>(
            engine.get(), producer, cb::mcbp::DcpAddStreamFlag::None, 0, vb);
    activeStream->setActive();
    ASSERT_TRUE(activeStream->isBackfilling());
    auto& readyQ = activeStream->public_readyQ();
    ASSERT_EQ(0, readyQ.size());

    // Core test
    // Backfill generates a [0, 1] complete snapshot even if on disk replica is
    // in a partial [0, 2] snapshot.
    auto& lpAuxioQ = *task_executor->getLpTaskQ(TaskType::AuxIO);
    runNextTask(lpAuxioQ); // init + scan
    ASSERT_EQ(2, readyQ.size());
    // marker
    auto resp = activeStream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_EQ(0, snapMarker.getStartSeqno());
    EXPECT_EQ(1, snapMarker.getEndSeqno());
    // mutation
    resp = activeStream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(1, resp->getBySeqno());
}

// Note: At the moment this test's purpose is to show how outbound DCP behaves
// at replica.
// @todo MB-59288
TEST_P(SingleThreadedPassiveStreamTest, MemorySnapshotFromPartialReplica) {
    auto& vb = *store->getVBucket(vbid);
    ASSERT_EQ(0, vb.getHighSeqno());
    auto& manager = static_cast<MockCheckpointManager&>(*vb.checkpointManager);
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(2, manager.getNumOpenChkItems()); // cs, vbs

    removeCheckpoint(vb);
    ASSERT_EQ(0, vb.getHighSeqno());
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getNumOpenChkItems()); // cs
    const auto& list = manager.getCheckpointList();
    ASSERT_FALSE(list.back()->modifiedByExpel());

    // The stream isn't in any snapshot (no data received)
    auto snapInfo = manager.getSnapshotInfo();
    EXPECT_EQ(0, snapInfo.start);
    EXPECT_EQ(snapshot_range_t(0, 0), snapInfo.range);

    const uint64_t opaque = 1;
    EXPECT_EQ(
            cb::engine_errc::success,
            consumer->snapshotMarker(opaque,
                                     vbid,
                                     1, // start
                                     2, // end
                                     DcpSnapshotMarkerFlag::Memory |
                                             DcpSnapshotMarkerFlag::Checkpoint,
                                     {},
                                     {},
                                     {},
                                     {}));

    const auto key = makeStoredDocKey("key");
    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(opaque,
                                 key,
                                 {},
                                 0,
                                 0,
                                 vbid,
                                 0,
                                 1, // seqno
                                 0,
                                 0,
                                 0,
                                 0));

    EXPECT_EQ(1, vb.getHighSeqno());
    EXPECT_EQ(2, manager.getNumOpenChkItems()); // cs, mut

    // The stream is in a partial snapshot here
    snapInfo = manager.getSnapshotInfo();
    EXPECT_EQ(1, snapInfo.start);
    EXPECT_EQ(snapshot_range_t(1, 2), snapInfo.range);

    // Open an outbound stream from replica.
    auto producer =
            std::make_shared<MockDcpProducer>(*engine,
                                              cookie,
                                              "test_producer->test_consumer",
                                              cb::mcbp::DcpOpenFlag::None,
                                              false);
    producer->createCheckpointProcessorTask();
    producer->scheduleCheckpointProcessorTask();
    auto activeStream = std::make_shared<MockActiveStream>(
            engine.get(), producer, cb::mcbp::DcpAddStreamFlag::None, 0, vb);
    activeStream->setActive();
    ASSERT_TRUE(activeStream->isInMemory());
    auto& readyQ = activeStream->public_readyQ();
    ASSERT_EQ(0, readyQ.size());

    // Core test
    // I would expect that checkpoint generates a [0, 2] partial snapshot, only
    // seqno:1 in checkpoint.
    activeStream->nextCheckpointItemTask();
    ASSERT_EQ(2, readyQ.size());
    // marker
    auto resp = activeStream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto* snapMarker = dynamic_cast<SnapshotMarker*>(resp.get());
    EXPECT_EQ(0, snapMarker->getStartSeqno());
    EXPECT_EQ(1, snapMarker->getEndSeqno()); // @todo ??? I would expect 2
    // seqno:1
    resp = activeStream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    auto* mut = dynamic_cast<MutationResponse*>(resp.get());
    EXPECT_EQ(1, mut->getBySeqno());

    // Now replica receives the snapEnd mutation
    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(opaque,
                                 key,
                                 {},
                                 0,
                                 0,
                                 vbid,
                                 0,
                                 2, // seqno
                                 0,
                                 0,
                                 0,
                                 0));

    // Core test
    // There's only seqno:2 remaining for the stream in checkpoint. I would
    // expect that just that mutation is sent for completing the snapshot that
    // we have partially sent to the peer.
    activeStream->nextCheckpointItemTask();
    ASSERT_EQ(2, readyQ.size()); // @todo ??? I would expect 1
    // marker @todo ??? I would expect no marker
    resp = activeStream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    snapMarker = dynamic_cast<SnapshotMarker*>(resp.get());
    EXPECT_EQ(2, snapMarker->getStartSeqno());
    EXPECT_EQ(2, snapMarker->getEndSeqno());
    // seqno:2
    resp = activeStream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    mut = dynamic_cast<MutationResponse*>(resp.get());
    EXPECT_EQ(2, mut->getBySeqno());
}

TEST_P(SingleThreadedPassiveStreamTest,
       EmptyDiskSnapshotFromReplicaCheckpoint) {
    auto& vb = *store->getVBucket(vbid);
    ASSERT_EQ(0, vb.getHighSeqno());
    auto& manager = static_cast<MockCheckpointManager&>(*vb.checkpointManager);
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(2, manager.getNumOpenChkItems()); // cs, vbs

    removeCheckpoint(vb);
    ASSERT_EQ(0, vb.getHighSeqno());
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getNumOpenChkItems()); // cs
    const auto& list = manager.getCheckpointList();
    ASSERT_FALSE(list.back()->modifiedByExpel());

    // The stream isn't in any snapshot (no data received)
    auto snapInfo = manager.getSnapshotInfo();
    EXPECT_EQ(0, snapInfo.start);
    EXPECT_EQ(snapshot_range_t(0, 0), snapInfo.range);

    const uint64_t opaque = 1;
    EXPECT_EQ(
            cb::engine_errc::success,
            consumer->snapshotMarker(opaque,
                                     vbid,
                                     1, // start
                                     2, // end
                                     DcpSnapshotMarkerFlag::Disk |
                                             DcpSnapshotMarkerFlag::Checkpoint,
                                     {},
                                     {},
                                     {},
                                     {}));

    // Open an outbound stream from replica.
    auto producer =
            std::make_shared<MockDcpProducer>(*engine,
                                              cookie,
                                              "test_producer->test_consumer",
                                              cb::mcbp::DcpOpenFlag::None,
                                              false);
    producer->createCheckpointProcessorTask();
    producer->scheduleCheckpointProcessorTask();

    auto activeStream = producer->addMockActiveStream(
            {}, 0, vb, 0, ~0ULL, 0xabcd, 0, ~0ULL);

    ASSERT_TRUE(activeStream->isInMemory());
    auto& readyQ = activeStream->public_readyQ();
    ASSERT_EQ(0, readyQ.size());

    // Only checkpoint_start item processed
    activeStream->nextCheckpointItemTask();
    EXPECT_EQ(0, readyQ.size());

    // Extra task execution triggers an exception before the fix for MB-59310:
    //
    // libc++abi: terminating due to uncaught exception of type
    // std::logic_error: ActiveStream::getOutstandingItems:
    // stream:test_producer->test_consumer vb:0 processing checkpoint
    // type:InitialDisk, CheckpointHistorical::No, ranges:0, HCS:0, MVS:2,
    // items:0
    activeStream->nextCheckpointItemTask();

    EXPECT_FALSE(activeStream->isChkExtractionInProgress());
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
    manager.createNewCheckpoint();
    ASSERT_GT(manager.getOpenCheckpointId(), openId);
    flushVBucketToDiskIfPersistent(vbid, 2 /*expected_num_flushed*/);
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getNumOpenChkItems());

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
    recreateProducerAndStream(vb, cb::mcbp::DcpOpenFlag::None);
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

    // Before the fix this steps generates SnapMarker{start:0, end:2, mvs:4},
    // while we want SnapMarker{start:0, end:4, mvs:4}
    ASSERT_EQ(backfill_success, bfm.backfill());
    const auto& readyQ = stream->public_readyQ();
    ASSERT_EQ(5, readyQ.size());
    resp = stream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_EQ(0, snapMarker.getStartSeqno());
    EXPECT_EQ(4, snapMarker.getEndSeqno());
    EXPECT_EQ(4, *snapMarker.getMaxVisibleSeqno());

    // Verify that all seqnos are sent from the backfill
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
    manager.createNewCheckpoint();
    ASSERT_GT(manager.getOpenCheckpointId(), openId);
    flushVBucketToDiskIfPersistent(vbid, 1 /*expected_num_flushed*/);
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getNumOpenChkItems());

    // Re-create the old producer and stream
    recreateProducerAndStream(vb, cb::mcbp::DcpOpenFlag::None);
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
    auto newProd =
            std::make_shared<MockDcpProducer>(*engine,
                                              cookie,
                                              producer->getName(),
                                              cb::mcbp::DcpOpenFlag::None,
                                              false /*startTask*/);
    ASSERT_TRUE(newProd);
    newProd->createCheckpointProcessorTask();
    // StreamReq from the client on the same vbucket -> This stream has the same
    // name as the one that is transitioning to dead in threadCrash
    auto newStream = newProd->addMockActiveStream({} /*flags*/,
                                                  0 /*opaque*/,
                                                  vb,
                                                  0 /*st_seqno*/,
                                                  ~0ULL /*en_seqno*/,
                                                  0x0 /*vb_uuid*/,
                                                  0 /*snap_start_seqno*/,
                                                  ~0ULL /*snap_end_seqno*/);
    ASSERT_TRUE(newStream);
    ASSERT_TRUE(newStream->isBackfilling());

    // New connection backfills and registers cursor.
    // This step invalidates the old stream from the CM::cursors map as it has
    // the same name as the new stream.
    auto& bfm = newProd->getBFM();
    ASSERT_EQ(1, bfm.getNumBackfills());
    ASSERT_EQ(backfill_success, bfm.backfill());
    const auto& readyQ = newStream->public_readyQ();
    ASSERT_EQ(2, readyQ.size());
    const auto resp = newStream->next(*newProd);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());

    // Unblock threadCrash. It tries to invalidate the old stream that is
    // already invalid. This step throws before the fix.
    gate.threadUp();

    threadCrash.join();
}

TEST_P(SingleThreadedActiveStreamTest,
       StreamTaskMovesCursorToLastItemInEmptyCheckpoint) {
    auto& vb = *engine->getVBucket(vbid);
    ASSERT_EQ(0, vb.getHighSeqno());
    auto& manager = *vb.checkpointManager;
    const auto& list =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    manager);
    ASSERT_EQ(1, list.size());
    ASSERT_EQ(ephemeral() ? 1 : 2, manager.getNumOpenChkItems()); // cs, vbs

    // Note: The first in-memory snapshot is special with regard to the CHK
    // flag.. We force nextSnapshotIsCheckpoint=true at (some) state transition
    // in ActiveStream. Given that in this test we need to verify that the CHK
    // flag is set by processing checkpoint_start messages, then here I make the
    // stream process a first snapshot for clearing nextSnapshotIsCheckpoint
    // before proceeding.
    EXPECT_TRUE(stream->public_nextSnapshotIsCheckpoint());
    store_item(vbid, makeStoredDocKey("key"), "value");
    ASSERT_EQ(1, vb.getHighSeqno());
    // Push from checkpoint to readyQ
    stream->public_nextCheckpointItemTask();
    // Drain readyQ
    auto& readyQ = stream->public_readyQ();
    while (!readyQ.empty()) {
        readyQ.pop();
    }
    // We want just a new open checkpoint
    manager.createNewCheckpoint();
    if (isPersistent()) {
        flushVBucket(vbid);
    }
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getNumOpenChkItems());

    // Verify that the stream doesn't know of any checkpoint so far
    ASSERT_FALSE(stream->public_nextSnapshotIsCheckpoint());

    // Verify 1 items for cursor (checkpoint_start)
    auto cursor = stream->getCursor().lock();
    EXPECT_TRUE(manager.hasItemsForCursor(*cursor));
    EXPECT_EQ(1, cursor->getRemainingItemsInCurrentCheckpoint());

    // Run the StreamTask
    ASSERT_EQ(0, readyQ.size());
    stream->public_nextCheckpointItemTask();

    // Verify nothing in the stream readyQ yet, as no mutations in checkpoints
    ASSERT_EQ(0, readyQ.size());

    // But, cursor already moved to the last item in checkpoint (set-vbstate at
    // this point)
    EXPECT_FALSE(manager.hasItemsForCursor(*cursor));
    EXPECT_EQ(0, cursor->getRemainingItemsInCurrentCheckpoint());

    // Now we want to verify that ActiveStream has already processed the 2 meta
    // items. In particular, we need to ensure that the checkpoint_start item
    // has updates the ActiveStream state to make it ready for streaming a
    // proper SnapMarker later (ie, as soon as a mutation is queued and
    // streamed). Here "proper" means that the CHK_FLAG must be set in the
    // marker.

    // checkpoint_start already processed
    EXPECT_TRUE(stream->public_nextSnapshotIsCheckpoint());

    // So now queue some mutations
    store_item(vbid, makeStoredDocKey("key"), "value");
    store_item(vbid, makeStoredDocKey("other-key"), "value");
    EXPECT_EQ(3, vb.getHighSeqno());

    // Run the StreamTask again
    stream->public_nextCheckpointItemTask();

    // Verify that we have two items in the readyQ (SnapshotMarker and two
    // DcpMutations), and that the SnapshotMarker is correctly encoded (should
    // have CHK flag set and the expected seqno-range).
    ASSERT_EQ(3, readyQ.size());
    ASSERT_EQ(DcpResponse::Event::SnapshotMarker, readyQ.front()->getEvent());
    auto& snapMarker = dynamic_cast<SnapshotMarker&>(*readyQ.front());
    EXPECT_EQ(DcpSnapshotMarkerFlag::Memory | DcpSnapshotMarkerFlag::Checkpoint,
              snapMarker.getFlags());
    EXPECT_EQ(2, snapMarker.getStartSeqno());
    EXPECT_EQ(3, snapMarker.getEndSeqno());
    EXPECT_EQ(DcpResponse::Event::Mutation, readyQ.back()->getEvent());
}

TEST_P(SingleThreadedActiveStreamTest, MB_53806) {
    auto& vb = *engine->getVBucket(vbid);
    ASSERT_EQ(0, vb.getHighSeqno());

    const auto keyA = makeStoredDocKey("keyA");
    const auto keyB = makeStoredDocKey("keyB");
    const auto keyC = makeStoredDocKey("keyC");
    for (const auto& key : {keyA, keyB, keyC}) {
        store_item(vbid, key, "value");
    }
    ASSERT_EQ(3, vb.getHighSeqno());

    // In the test we need to re-create streams in a condition that triggers
    // backfill. So get rid of the DCP cursor and move the persistence cursor to
    // a new checkpoint, that removes the old checkpoint that contains items.
    stream.reset();
    producer.reset();
    auto& manager = *vb.checkpointManager;
    // cs, vbs, 3 muts
    ASSERT_EQ(ephemeral() ? 4 : 5, manager.getNumOpenChkItems());
    manager.createNewCheckpoint();
    flushVBucketToDiskIfPersistent(vbid, 3 /*expected_num_flushed*/);
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getNumOpenChkItems());

    // We need backfill flowing in the DiskCallback path, so ensure no cache hit
    // by ejecting everything from the HashTable
    if (persistent()) {
        for (const auto& key : {keyA, keyB, keyC}) {
            evict_key(vbid, key);
        }
    }

    // Re-create the old producer and stream
    recreateProducerAndStream(vb, cb::mcbp::DcpOpenFlag::None);
    ASSERT_TRUE(producer);
    producer->createCheckpointProcessorTask();
    ASSERT_TRUE(stream);
    ASSERT_TRUE(stream->isBackfilling());

    // Simulate backfill buffer full for triggering backfill-yield
    auto& bfm = dynamic_cast<MockDcpBackfillManager&>(producer->getBFM());
    ASSERT_EQ(1, bfm.getNumBackfills());
    bfm.setBackfillBufferSize(1);

    // Run backfill.
    // First call pushes the SnapMarker + Mut:1, then the backfill yields.
    // Note: Backfill buffer guarantees that the first item is always pushed to
    //   the readyQ. See BackfillManager for details.
    ASSERT_EQ(backfill_success, bfm.backfill());
    const auto& readyQ = stream->public_readyQ();
    ASSERT_EQ(2, readyQ.size());
    auto resp = stream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    EXPECT_EQ(1, readyQ.size());
    resp = stream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(1, resp->getBySeqno());
    EXPECT_EQ(0, readyQ.size());

    // Time to run again for the backfill.
    // We expect Mut:2 and Mut:3 pushed to the stream.
    // Before the fix we skip seqno:2 and proceed to seqno:3.
    bfm.setBackfillBufferSize(1_MiB);
    ASSERT_EQ(backfill_success, bfm.backfill());

    EXPECT_EQ(2, readyQ.size());
    resp = stream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(2, resp->getBySeqno());

    EXPECT_EQ(1, readyQ.size());
    resp = stream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(3, resp->getBySeqno());

    EXPECT_EQ(0, readyQ.size());

    EXPECT_EQ(backfill_finished, bfm.backfill());
}

TEST_P(SingleThreadedActiveStreamTest, ReadyQLimit) {
    auto& vb = *engine->getVBucket(vbid);
    ASSERT_EQ(0, vb.getHighSeqno());

    const size_t checkpointMaxSize = 1_MiB;
    engine->getCheckpointConfig().setCheckpointMaxSize(checkpointMaxSize);
    auto& manager = *vb.checkpointManager;
    ASSERT_EQ(checkpointMaxSize,
              manager.getCheckpointConfig().getCheckpointMaxSize());

    const auto value = std::string(checkpointMaxSize / 2, 'v');
    size_t numItems = 6;
    for (size_t i = 0; i < numItems; ++i) {
        store_item(vbid, makeStoredDocKey("key" + std::to_string(i)), value);
    }
    ASSERT_EQ(numItems, vb.getHighSeqno());
    const auto numCheckpoints = manager.getNumCheckpoints();
    ASSERT_EQ(numItems / 2, numCheckpoints);
    const size_t numMutationsPerCkpt = numItems / numCheckpoints;
    ASSERT_EQ(numMutationsPerCkpt + 1, manager.getNumOpenChkItems()); // cs + ..

    // Stream in memory and readYQ empty
    ASSERT_TRUE(stream->isInMemory());
    const auto& readyQ = stream->public_readyQ();
    ASSERT_EQ(0, readyQ.size());

    // Test - StreamTask pulls from CM and pushes into the readyQ
    // At each run the task is allowed to push checkpoint_max_size_bytes into
    // the readyQ. In the scenario under test that means 1 full checkpoint at a
    // time.
    for (size_t i = 0; i < numCheckpoints; ++i) {
        stream->nextCheckpointItemTask();

        ASSERT_EQ(3, readyQ.size());
        auto resp = stream->next(*producer);
        ASSERT_TRUE(resp);
        EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());

        ASSERT_EQ(2, readyQ.size());
        resp = stream->next(*producer);
        ASSERT_TRUE(resp);
        EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
        EXPECT_EQ(i * numMutationsPerCkpt + 1, resp->getBySeqno());

        ASSERT_EQ(1, readyQ.size());
        resp = stream->next(*producer);
        ASSERT_TRUE(resp);
        EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
        EXPECT_EQ(i * numMutationsPerCkpt + 2, resp->getBySeqno());

        ASSERT_EQ(0, readyQ.size());
    }

    // All streamed
    stream->nextCheckpointItemTask();
    ASSERT_EQ(0, readyQ.size());
}

/// Check handling of Checkpoint Cursors if a Cursor is not successfully
/// assigned to ActiveStream - e.g. due to an exception being thrown.
/// In the original bug this resulted in the cursor being orphaned - it existed
/// in CheckpointManager but was not associated with any Stream, and hence
/// was not advanced (via getItemsForCursor) - but also was not subject to
/// cursor-dropping so resulted in the CheckpointManager getting stuck at the
/// checkpoint high watermark.
TEST_P(SingleThreadedActiveStreamTest,
       MB55391_DcpStepExceptionShouldRemoveCursor) {
    // Setup to require backfill, which involves re-registering a cursor in
    // ActiveStream::scheduleBackfill_UNLOCKED. To do this we reset the
    // initially created producer and stream, store an item then flush and
    // create new checkpoint, then re-create the stream.

    stream.reset();
    producer->closeStream(0, vbid);

    auto& vb = *engine->getVBucket(vbid);
    const auto initialCursors = vb.checkpointManager->getNumCursors();

    // Ensure mutation is on disk and  no longer present in CheckpointManager so
    // stream will trigger backfill.
    store_item(vbid, makeStoredDocKey("key"), "value");
    vb.checkpointManager->createNewCheckpoint();
    flushVBucketToDiskIfPersistent(vbid, 1);
    ASSERT_EQ(1, vb.checkpointManager->getNumCheckpoints());

    // Re-create the stream, but using the preSetActiveHook to set a callback
    // to run in ActiveStream::scheduleBackfill after the cursor is registered.
    struct TestException : public std::invalid_argument {
        using std::invalid_argument::invalid_argument;
    };

    auto throwExceptionFn = []() {
        throw TestException("Injecting exception");
    };
    try {
        stream = producer->addMockActiveStream(
                {},
                0,
                vb,
                0,
                ~0ULL,
                0x0,
                0,
                ~0ULL,
                {},
                {},
                {},
                {},
                [&](auto& stream) {
                    stream.scheduleBackfillRegisterCursorHook =
                            throwExceptionFn;
                });
    } catch (TestException&) {
        // In full-stack, exception thown from DcpProducer::step() will tear
        // down the connection, including destroying the DcpProducer. Simulate
        // that here.
        stream.reset();
        producer.reset();

        // Test: We _should_ be back to the original number of cursors.
        EXPECT_EQ(initialCursors, vb.checkpointManager->getNumCursors());
        return;
    }

    FAIL() << "Expected TestException to be thrown during "
              "addMockActiveStream";
}

// Each cycle of stream create/stream closed (when the stream needs a backfill)
// creates a DCPBackfill and leaves it in the backfill manager. MB-57772 showed
// that there can be conditions (high memory) where the path to deleting the
// orphaned DCPBackfill (from the closed stream) is blocked. In the case where
// the backfill queues themselves are the dominant user of memory leaves the
// system live-locked and way way over quota. The fix is to have closeStream
// (or ~ActiveStream) directly free the DCPBackfill it created.
TEST_P(SingleThreadedActiveStreamTest, MB_57772) {
    store_item(vbid, makeStoredDocKey("key"), "value");
    flushVBucketToDiskIfPersistent(vbid);
    auto& vb = *engine->getVBucket(vbid);
    vb.checkpointManager->clear();

    auto& bfm = producer->getBFM();
    EXPECT_EQ(0, bfm.getNumBackfills());
    EXPECT_EQ(0, store->getKVStoreScanTracker().getNumRunningBackfills());
    stream.reset();
    recreateStream(vb);
    ASSERT_TRUE(stream->isBackfilling());

    EXPECT_EQ(1, bfm.getNumBackfills());
    // backfill makes it into the initializing list, classed as running
    EXPECT_EQ(1, store->getKVStoreScanTracker().getNumRunningBackfills());

    for (int cycles = 0; cycles < 2; ++cycles) {
        // In MB-57772 it doesn't look like a cycle of streamRequest/closeStream
        // grew the backfill queues (we don't actually know the true sequence),
        // but for this test closeStream reproduces an ever growing backfill
        // queue.
        stream.reset(); // we want the ActiveStream to destruct
        producer->closeStream(0, vbid);

        // closeStream has explicitly removed the backfill, no need to wait for
        // an I/O task to run.
        EXPECT_EQ(0, bfm.getNumBackfills());
        EXPECT_EQ(0, store->getKVStoreScanTracker().getNumRunningBackfills());

        recreateStream(vb);
        ASSERT_TRUE(stream->isBackfilling());

        // Now expect only 1 backfill in the queue (this would keep increasing
        // prior to fixing the issue).
        EXPECT_EQ(1, bfm.getNumBackfills());
        // backfill makes it into the initializing list, classed as running
        EXPECT_EQ(1, store->getKVStoreScanTracker().getNumRunningBackfills());
    }

    // Force close - now the stream will remove its backfill without the task
    // needing to run.
    stream.reset(); // we want the ActiveStream to destruct
    producer->closeStream(0, vbid);

    EXPECT_EQ(0, store->getKVStoreScanTracker().getNumRunningBackfills());
    EXPECT_EQ(0, bfm.getNumBackfills());
}

TEST_P(SingleThreadedActiveStreamTest, MB_58961) {
    // No OSOBackfill in Ephemeral
    if (ephemeral()) {
        GTEST_SKIP();
    }

    // Create a collection
    CollectionsManifest manifest;
    const auto& cFruit = CollectionEntry::fruit;
    manifest.add(
            cFruit, cb::NoExpiryLimit, true /*history*/, ScopeEntry::defaultS);
    auto& vb = *store->getVBucket(vbid);
    vb.updateFromManifest(
            std::shared_lock<folly::SharedMutex>(vb.getStateLock()),
            Collections::Manifest{std::string{manifest}});
    ASSERT_EQ(1, vb.getHighSeqno());

    // seqno:2 in cFruit
    const std::string value("value");
    store_item(vbid, makeStoredDocKey("keyF", cFruit), value);
    ASSERT_EQ(2, vb.getHighSeqno());

    // Add some data to the default collection
    const auto& cDefault = CollectionEntry::defaultC;
    store_item(vbid, makeStoredDocKey("keyD", cDefault), value);
    ASSERT_EQ(3, vb.getHighSeqno());

    // [e:1 cs:1 se:1 m(keyF):2 m(keyD):3)

    // Ensure new stream will backfill
    stream->public_getOutstandingItems(vb);
    removeCheckpoint(vb);

    // [e:4 cs:4)

    // Ensure OSOBackfill is triggered
    engine->getConfiguration().setDcpOsoBackfill("enabled");
    producer->setOutOfOrderSnapshots(OutOfOrderSnapshots::YesWithSeqnoAdvanced);

    // Stream filters on cFruit
    recreateStream(
            vb,
            true,
            fmt::format(R"({{"collections":["{:x}"]}})", uint32_t(cFruit.uid)));
    ASSERT_TRUE(stream);
    // Pushed to backfill
    ASSERT_TRUE(stream->isBackfilling());

    // [e:4 cs:4)
    //  ^

    auto& readyQ = stream->public_readyQ();
    ASSERT_EQ(0, readyQ.size());

    // Run the OSO backfill
    runBackfill(); // push markers and data
    ASSERT_EQ(5, readyQ.size());

    auto resp = stream->public_nextQueuedItem(*producer);
    EXPECT_EQ(DcpResponse::Event::OSOSnapshot, resp->getEvent());

    resp = stream->public_nextQueuedItem(*producer);
    EXPECT_EQ(DcpResponse::Event::SystemEvent, resp->getEvent());
    EXPECT_EQ(1, resp->getBySeqno());

    resp = stream->public_nextQueuedItem(*producer);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(2, resp->getBySeqno());

    // Note: seqno:3 is in cDefault, filtered out, SeqnoAdvance(3) sent
    resp = stream->public_nextQueuedItem(*producer);
    EXPECT_EQ(DcpResponse::Event::SeqnoAdvanced, resp->getEvent());
    EXPECT_EQ(3, resp->getBySeqno());

    resp = stream->public_nextQueuedItem(*producer);
    EXPECT_EQ(DcpResponse::Event::OSOSnapshot, resp->getEvent());

    EXPECT_EQ(0, stream->getLastSentSnapEndSeqno());
    EXPECT_EQ(2, stream->getLastBackfilledSeqno());
    EXPECT_EQ(3, stream->getLastSentSeqno());
    EXPECT_EQ(3, stream->getLastReadSeqno());
    EXPECT_EQ(4, stream->getCurChkSeqno());

    ASSERT_FALSE(stream->public_nextQueuedItem(*producer));
    GMockDcpMsgProducers producers;
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(false, producers));
    ASSERT_TRUE(stream->isInMemory());

    // [e:4 cs:4)
    //  ^

    store_item(vbid, makeStoredDocKey("keyD4", cDefault), value);
    ASSERT_EQ(4, vb.getHighSeqno());
    store_item(vbid, makeStoredDocKey("keyD5", cDefault), value);
    ASSERT_EQ(5, vb.getHighSeqno());

    // [e:4 cs:4 m(keyD4):4 m(keyD5):5)
    //  ^

    // Move inMemory stream.
    // Note: Before the fix we do move the cursor but we miss to advance the
    // stream
    ASSERT_EQ(0, readyQ.size());
    runCheckpointProcessor(*producer, producers);
    // Note: We don't send any snapshot when all items are filtrered out
    EXPECT_EQ(0, readyQ.size());

    // [e:4 cs:4 m(keyD4):4 m(keyD5):5)
    //                      ^

    EXPECT_EQ(0, stream->getLastSentSnapEndSeqno());
    EXPECT_EQ(2, stream->getLastBackfilledSeqno());
    EXPECT_EQ(3, stream->getLastSentSeqno());
    EXPECT_EQ(5, stream->getLastReadSeqno()); // Before the fix: 3
    EXPECT_EQ(5, stream->getCurChkSeqno());

    // CursorDrop + backfill
    ASSERT_TRUE(stream->handleSlowStream());
    ASSERT_TRUE(stream->isInMemory());

    // [e:4 cs:4 m(keyD4):4 m(keyD5):5)
    //                      x

    // Before the fix for MB-58961 this step triggers:
    //
    // libc++abi: terminating due to uncaught exception of type
    // boost::exception_detail::error_info_injector<std::logic_error>:Monotonic<y>
    // (ActiveStream(test_producer->test_consumer (vb:0))::curChkSeqno)
    // invariant failed: new value (4) breaks invariant on current value (5)
    //
    // The reason for the failure in MB-58961 is that we miss to update
    // AS::lastReadSeqno when we process the "empty-by-filter" snapshot before
    // CursorDrop.
    // By that:
    // (a) lastReadSeqno stays at 3
    // (b) In the subsequent AS::scheduleBackfill(lastReadSeqno:3) call we
    //     re-register the cursor at cs:4 and we try to reset curChkSeqno (5) by
    //     (4).
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(false, producers));
    EXPECT_FALSE(stream->isBackfilling());
}

TEST_P(SingleThreadedActiveStreamTest, StreamRequestMemoryQuota) {
    const auto& vb = *store->getVBucket(vbid);
    auto& config = engine->getConfiguration();
    const auto initVal = config.getBackfillMemThreshold();
    for (const size_t newVal : {size_t{0}, size_t{91}, initVal}) {
        config.setBackfillMemThreshold(newVal);
        ASSERT_EQ(newVal, config.getBackfillMemThreshold());
        ASSERT_FLOAT_EQ(newVal / 100.0f, store->getBackfillMemoryThreshold());
        uint64_t rollbackSeqno = std::numeric_limits<uint64_t>::max();
        auto ret = producer->streamRequest({},
                                           2,
                                           vbid,
                                           0,
                                           std::numeric_limits<uint64_t>::max(),
                                           vb.failovers->getLatestUUID(),
                                           0,
                                           0,
                                           &rollbackSeqno,
                                           mock_dcp_add_failover_log,
                                           std::nullopt);
        if (newVal == 0) {
            EXPECT_EQ(cb::engine_errc::no_memory, ret);
        } else {
            EXPECT_EQ(cb::engine_errc::success, ret);
        }
        auto activeStream = std::dynamic_pointer_cast<ActiveStream>(
                producer->findStream(vbid));
        if (activeStream) {
            activeStream->setDead(cb::mcbp::DcpStreamEndStatus::Closed);
        }
    }
}

TEST_P(SingleThreadedActiveStreamTest, PurgeSeqnoInSnapshotMarker_InMemory) {
    // Reset the stream created originally as part of the test setup.
    stream.reset();

    auto& vb = *engine->getVBucket(vbid);
    // Create Stream without MarkerVersion::V2_2.
    recreateProducerAndStream(vb, cb::mcbp::DcpOpenFlag::None);

    // Store a single item.
    store_item(vbid, makeStoredDocKey("foo"), "bar");

    ASSERT_TRUE(stream->isInMemory());
    EXPECT_EQ(0, stream->public_readyQSize());

    MockDcpMessageProducers producers;
    runCheckpointProcessor(*producer, producers);

    // One snapshot marker & one mutation.
    EXPECT_EQ(2, stream->public_readyQSize());

    auto resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_FALSE(snapMarker.getPurgeSeqno().has_value());

    recreateProducerAndStream(
            vb,
            cb::mcbp::DcpOpenFlag::None,
            {},
            {{DcpControlKeys::SnapshotMaxMarkerVersion, "2.2"}});

    ASSERT_TRUE(stream->isInMemory());
    EXPECT_EQ(0, stream->public_readyQSize());

    runCheckpointProcessor(*producer, producers);

    // One snapshot marker & one mutation.
    EXPECT_EQ(2, stream->public_readyQSize());

    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_FALSE(snapMarker.getPurgeSeqno().has_value());
}

TEST_P(SingleThreadedActiveStreamTest, PurgeSeqnoInSnapshotMarker_Backfill) {
    // Reset the stream created originally as part of the test setup.
    stream.reset();
    auto& vb = *engine->getVBucket(vbid);

    // Store a single item.
    store_item(vbid, makeStoredDocKey("foo"), "bar");

    // 1. Flush the items to disk.
    // 2. Recreate the producer & stream.
    // 3. Test purge-seqno not included.

    flushAndRemoveCheckpoints(vbid);

    //! 1. Recreate the stream without MarkerVersion::V2_2 & expect the
    //! purgeSeqno is not included.
    recreateProducerAndStream(
            vb,
            cb::mcbp::DcpOpenFlag::None,
            fmt::format(R"({{"collections":["{:x}"]}})",
                        uint32_t(CollectionEntry::defaultC.getId())));

    stream->transitionStateToBackfilling();
    ASSERT_TRUE(stream->isBackfilling());
    EXPECT_EQ(0, stream->public_readyQSize());

    // Perform the actual backfill - fills up the readyQ in the stream.
    producer->getBFM().backfill();

    // One snapshot marker & one mutation.
    EXPECT_EQ(2, stream->public_readyQSize());

    auto resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    //! The purgeSeqno shouldn't be present.
    EXPECT_FALSE(snapMarker.getPurgeSeqno().has_value());

    //! 2. Recreate the stream with max_marker_version=2.2 & expect the
    //! purgeSeqno is included.
    recreateProducerAndStream(
            vb,
            cb::mcbp::DcpOpenFlag::None,
            fmt::format(R"({{"collections":["{:x}"]}})",
                        uint32_t(CollectionEntry::defaultC.getId())),
            {{DcpControlKeys::SnapshotMaxMarkerVersion, "2.2"}});

    stream->transitionStateToBackfilling();
    ASSERT_TRUE(stream->isBackfilling());
    resp = stream->next(*producer);
    EXPECT_FALSE(resp);
    EXPECT_EQ(0, stream->public_readyQSize());

    // Perform the actual backfill - fills up the readyQ in the stream.
    producer->getBFM().backfill();

    // One snapshot marker & one mutation.
    EXPECT_EQ(2, stream->public_readyQSize());

    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    //! The purgeSeqno should be present.
    EXPECT_EQ(0, snapMarker.getPurgeSeqno());

    // 1. Perform a couple of other mutations.
    // - {"foo": "bar"} is already present.
    // - Add 2 more items & delete "foo".
    // 2. Flush the items & remove the checkpoints to force a bacfill.
    // 2. Purge till seqno 2.

    store_item(vbid, makeStoredDocKey("foo1"), "bar"); // Seqno: 2
    delete_item(vbid, makeStoredDocKey("foo")); // Seqno: 3
    store_item(vbid, makeStoredDocKey("foo2"), "bar"); // Seqno: 4

    flushAndRemoveCheckpoints(vbid);
    // Run compaction for persistent buckets & tombstone purger for ephemeral
    // buckets.
    purgeTombstonesBefore(3);
    const auto purgeSeqno = vb.getPurgeSeqno();
    EXPECT_EQ(3, purgeSeqno);

    //! 3. Recreate the stream with max_marker_version=2.2 & expect the
    //! purgeSeqno is included and it set to 3.
    recreateProducerAndStream(
            vb,
            cb::mcbp::DcpOpenFlag::None,
            fmt::format(R"({{"collections":["{:x}"]}})",
                        uint32_t(CollectionEntry::defaultC.getId())),
            {{DcpControlKeys::SnapshotMaxMarkerVersion, "2.2"}});

    stream->transitionStateToBackfilling();
    ASSERT_TRUE(stream->isBackfilling());
    EXPECT_EQ(0, stream->public_readyQSize());

    // Perform the actual backfill - fills up the readyQ in the stream.
    producer->getBFM().backfill();

    // One snapshot marker & 2 mutation (keys: foo1 & foo2).
    EXPECT_EQ(3, stream->public_readyQSize());

    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    snapMarker = dynamic_cast<SnapshotMarker&>(*resp);

    EXPECT_TRUE(snapMarker.getPurgeSeqno().has_value());
    EXPECT_EQ(purgeSeqno, snapMarker.getPurgeSeqno());

    // Pop the dcp mutations of the stream readyQ & check we indeed
    // received mutations.
    for (auto i = 0; i < 2; i++) {
        resp = stream->next(*producer);
        EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    }

    // Check the stream is in-memory state (should be the state we should
    // have dropped into at the end of a backfill).
    ASSERT_TRUE(stream->isInMemory());

    //! 4. Perform another mutation in memory & check we receive another
    //! snapshot-marker with the purgeSeqno: 3 (since no compaction has run
    //! again).
    store_item(vbid, makeStoredDocKey("foo3"), "bar"); // Seqno: 5
    MockDcpMessageProducers producers;
    runCheckpointProcessor(*producer, producers);

    // One snapshot marker & one mutation (key: "foo3").
    EXPECT_EQ(2, stream->public_readyQSize());

    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_FALSE(snapMarker.getPurgeSeqno().has_value());
}

TEST_P(SingleThreadedActiveStreamTest,
       PurgeSeqnoInSnapshotMarkerInFilteredStreams_InMemory) {
    // Reset the stream created originally as part of the test setup.
    stream.reset();
    auto& vb = *engine->getVBucket(vbid);

    // Create a collection
    CollectionsManifest manifest;
    const auto& cVegetable = CollectionEntry::vegetable;
    manifest.add(cVegetable,
                 cb::NoExpiryLimit,
                 true /*history*/,
                 ScopeEntry::defaultS);
    vb.updateFromManifest(
            std::shared_lock<folly::SharedMutex>(vb.getStateLock()),
            Collections::Manifest{std::string{manifest}});
    ASSERT_EQ(1, vb.getHighSeqno());

    store_item(vbid, makeStoredDocKey("potato", cVegetable), "value");

    //! 1. Test the snapshot marker does not include the purgeSeqno & advance
    //! seqno message is sent. All the mutations are filtered out.
    recreateProducer(vb, cb::mcbp::DcpOpenFlag::None);
    stream = producer->addMockActiveStream(
            cb::mcbp::DcpAddStreamFlag::None,
            0 /*opaque*/,
            vb,
            0 /*st_seqno*/,
            ~0ULL /*en_seqno*/,
            0x0 /*vb_uuid*/,
            0 /*snap_start_seqno*/,
            2 /*snap_end_seqno*/,
            producer->public_getIncludeValue(),
            producer->public_getIncludeXattrs(),
            producer->public_getIncludeDeletedUserXattrs(),
            producer->public_getMaxMarkerVersion(),
            fmt::format(R"({{"collections":["{:x}"]}})",
                        uint32_t(CollectionEntry::defaultC.getId())));
    ASSERT_TRUE(stream);

    ASSERT_TRUE(stream->isInMemory());
    EXPECT_EQ(0, stream->public_readyQSize());

    MockDcpMessageProducers producers;
    runCheckpointProcessor(*producer, producers);

    // One snapshot marker & one advance seqno message.
    EXPECT_EQ(2, stream->public_readyQSize());

    auto resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_FALSE(snapMarker.getPurgeSeqno().has_value());
    EXPECT_EQ(0, snapMarker.getStartSeqno());
    EXPECT_EQ(2, snapMarker.getEndSeqno());
    resp = stream->next(*producer);

    EXPECT_EQ(DcpResponse::Event::SeqnoAdvanced, resp->getEvent());

    auto seqnoAdvanced = dynamic_cast<SeqnoAdvanced&>(*resp);
    EXPECT_EQ(2, seqnoAdvanced.getBySeqno());

    //! 2. Test the snapshot marker includes the purgeSeqno & advance seqno
    //! message is sent. All the mutations are filtered out.
    recreateProducer(vb,
                     cb::mcbp::DcpOpenFlag::None,
                     {{DcpControlKeys::SnapshotMaxMarkerVersion, "2.2"}});
    stream = producer->addMockActiveStream(
            cb::mcbp::DcpAddStreamFlag::None,
            0 /*opaque*/,
            vb,
            0 /*st_seqno*/,
            ~0ULL /*en_seqno*/,
            0x0 /*vb_uuid*/,
            0 /*snap_start_seqno*/,
            2 /*snap_end_seqno*/,
            producer->public_getIncludeValue(),
            producer->public_getIncludeXattrs(),
            producer->public_getIncludeDeletedUserXattrs(),
            producer->public_getMaxMarkerVersion(),
            fmt::format(R"({{"collections":["{:x}"]}})",
                        uint32_t(CollectionEntry::defaultC.getId())));

    ASSERT_TRUE(stream->isInMemory());
    resp = stream->next(*producer);
    EXPECT_FALSE(resp);
    EXPECT_EQ(0, stream->public_readyQSize());

    runCheckpointProcessor(*producer, producers);

    // One snapshot marker & one advance seqno message.
    EXPECT_EQ(2, stream->public_readyQSize());

    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_FALSE(snapMarker.getPurgeSeqno().has_value());
    EXPECT_EQ(0, snapMarker.getStartSeqno());
    EXPECT_EQ(2, snapMarker.getEndSeqno());
    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SeqnoAdvanced, resp->getEvent());
    EXPECT_EQ(2, dynamic_cast<SeqnoAdvanced&>(*resp).getBySeqno());

    //! 3. Test the snapshot marker does not include the purgeSeqno & a single
    //! mutation meant for the collection-filtered stream is sent.
    recreateProducerAndStream(
            vb,
            cb::mcbp::DcpOpenFlag::None,
            fmt::format(R"({{"collections":["{:x}"]}})",
                        uint32_t(CollectionEntry::vegetable.getId())));

    ASSERT_TRUE(stream->isInMemory());
    EXPECT_EQ(0, stream->public_readyQSize());

    runCheckpointProcessor(*producer, producers);

    // One snapshot marker, one system-event (collection created) & one mutation
    // message.
    EXPECT_EQ(3, stream->public_readyQSize());

    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_FALSE(snapMarker.getPurgeSeqno().has_value());

    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SystemEvent, resp->getEvent());
    EXPECT_EQ(1, resp->getBySeqno());

    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    auto* mutation = dynamic_cast<MutationResponse*>(resp.get());
    EXPECT_EQ(2, mutation->getBySeqno());

    //! 4. Test the snapshot marker includes the purgeSeqno & a single mutation
    //! meant for the collection-filtered stream is sent.
    recreateProducerAndStream(
            vb,
            cb::mcbp::DcpOpenFlag::None,
            fmt::format(R"({{"collections":["{:x}"]}})",
                        uint32_t(CollectionEntry::vegetable.getId())),

            {{DcpControlKeys::SnapshotMaxMarkerVersion, "2.2"}});

    ASSERT_TRUE(stream->isInMemory());
    EXPECT_EQ(0, stream->public_readyQSize());

    runCheckpointProcessor(*producer, producers);

    // One snapshot marker, one system-event (collection created) & one mutation
    // message.
    EXPECT_EQ(3, stream->public_readyQSize());

    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_FALSE(snapMarker.getPurgeSeqno().has_value());

    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SystemEvent, resp->getEvent());
    EXPECT_EQ(1, resp->getBySeqno());

    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    mutation = dynamic_cast<MutationResponse*>(resp.get());
    EXPECT_EQ(2, mutation->getBySeqno());

    // Store an item in the default collection.
    store_item(vbid, makeStoredDocKey("foo"), "bar");

    //! 5. Test the snapshot marker does not include the purgeSeqno & a single
    //! mutation meant for the default-collection stream is sent.
    recreateProducerAndStream(
            vb,
            cb::mcbp::DcpOpenFlag::None,
            fmt::format(R"({{"collections":["{:x}"]}})",
                        uint32_t(CollectionEntry::defaultC.getId())));

    ASSERT_TRUE(stream->isInMemory());
    resp = stream->next(*producer);
    EXPECT_FALSE(resp);
    EXPECT_EQ(0, stream->public_readyQSize());

    runCheckpointProcessor(*producer, producers);

    // One snapshot marker & one mutation message.
    EXPECT_EQ(2, stream->public_readyQSize());

    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_FALSE(snapMarker.getPurgeSeqno().has_value());
    resp = stream->next(*producer);

    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    mutation = dynamic_cast<MutationResponse*>(resp.get());
    EXPECT_EQ(3, mutation->getBySeqno());

    //! 6. Test the snapshot marker does not include the purgeSeqno & a single
    //! mutation meant for the default-collection stream is sent.
    recreateProducerAndStream(
            vb,
            cb::mcbp::DcpOpenFlag::None,
            fmt::format(R"({{"collections":["{:x}"]}})",
                        uint32_t(CollectionEntry::defaultC.getId())),

            {{DcpControlKeys::SnapshotMaxMarkerVersion, "2.2"}});

    ASSERT_TRUE(stream->isInMemory());
    resp = stream->next(*producer);
    EXPECT_FALSE(resp);
    EXPECT_EQ(0, stream->public_readyQSize());

    runCheckpointProcessor(*producer, producers);

    // One snapshot marker & one mutation message.
    EXPECT_EQ(2, stream->public_readyQSize());

    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_FALSE(snapMarker.getPurgeSeqno().has_value());

    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    mutation = dynamic_cast<MutationResponse*>(resp.get());
    EXPECT_EQ(3, mutation->getBySeqno());
}

TEST_P(SingleThreadedActiveStreamTest,
       PurgeSeqnoInSnapshotMarkerInFilteredStreams_Backfill) {
    // Reset the stream created originally as part of the test setup.
    stream.reset();
    auto& vb = *engine->getVBucket(vbid);

    // Create a collection
    CollectionsManifest manifest;
    const auto& cVegetable = CollectionEntry::vegetable;
    manifest.add(cVegetable,
                 cb::NoExpiryLimit,
                 true /*history*/,
                 ScopeEntry::defaultS);
    vb.updateFromManifest(
            std::shared_lock<folly::SharedMutex>(vb.getStateLock()),
            Collections::Manifest{std::string{manifest}});
    ASSERT_EQ(1, vb.getHighSeqno());

    store_item(vbid, makeStoredDocKey("potato", cVegetable), "value");

    flushAndRemoveCheckpoints(vbid);

    //! 2. Test the snapshot marker does not include the purgeSeqno. All the
    //! mutations should be filtered out.
    recreateProducerAndStream(
            vb,
            cb::mcbp::DcpOpenFlag::None,
            fmt::format(R"({{"collections":["{:x}"]}})",
                        uint32_t(CollectionEntry::defaultC.getId())));

    stream->transitionStateToBackfilling();
    ASSERT_TRUE(stream->isBackfilling());
    EXPECT_EQ(0, stream->public_readyQSize());

    // Perform the actual backfill - fills up the readyQ in the stream.
    producer->getBFM().backfill();

    // All the documents are filtered out - we shouldn't have queue any
    // items.
    EXPECT_EQ(0, stream->public_readyQSize());

    //! 3. Test the snapshot marker does not include the purgeSeqno. Must
    //! receive the one mutation relevant to this stream.
    recreateProducerAndStream(
            vb,
            cb::mcbp::DcpOpenFlag::None,
            fmt::format(R"({{"collections":["{:x}"]}})",
                        uint32_t(CollectionEntry::vegetable.getId())));

    stream->transitionStateToBackfilling();
    ASSERT_TRUE(stream->isBackfilling());
    auto resp = stream->next(*producer);
    EXPECT_FALSE(resp);
    EXPECT_EQ(0, stream->public_readyQSize());

    // Perform the actual backfill - fills up the readyQ in the stream.
    producer->getBFM().backfill();

    // One snapshot marker, one system-event (collection-created), one mutation.
    EXPECT_EQ(3, stream->public_readyQSize());

    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    //! The purgeSeqno shouldn't be present.
    EXPECT_FALSE(snapMarker.getPurgeSeqno().has_value());
    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SystemEvent, resp->getEvent());
    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());

    //! 4. Test the snapshot marker includes the purgeSeqno. Must receive
    //! the one mutation relevant to this stream.
    recreateProducerAndStream(
            vb,
            cb::mcbp::DcpOpenFlag::None,
            fmt::format(R"({{"collections":["{:x}"]}})",
                        uint32_t(CollectionEntry::vegetable.getId())),

            {{DcpControlKeys::SnapshotMaxMarkerVersion, "2.2"}});

    stream->transitionStateToBackfilling();
    ASSERT_TRUE(stream->isBackfilling());
    EXPECT_EQ(0, stream->public_readyQSize());

    // Perform the actual backfill - fills up the readyQ in the stream.
    producer->getBFM().backfill();

    // One snapshot marker, one system-event (collection-created), one mutation.
    EXPECT_EQ(3, stream->public_readyQSize());

    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    //! The purgeSeqno should be present.
    EXPECT_EQ(0, snapMarker.getPurgeSeqno());
    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SystemEvent, resp->getEvent());
    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());

    // delete the item.
    delete_item(vbid, makeStoredDocKey("potato", cVegetable));

    //! Store another document to bump the hiseqno number, to force purging of
    //! the deleted document "vegetable:potato" (seqno: 3). For reasons unknown
    //! to me currently, we don't purge a tombstone who seqno is the hiseqno of
    //! the vbucket.
    store_item(vbid, makeStoredDocKey("foo"), "bar");
    flushAndRemoveCheckpoints(vbid);
    EXPECT_EQ(4, vb.getHighSeqno());
    purgeTombstonesBefore(4);

    const auto purgeSeqno = vb.getPurgeSeqno();
    EXPECT_EQ(3, purgeSeqno);

    //! 5. Test the snapshot marker includes the purgeSeqno is 3. All the
    //! mutations relevant to this stream are filtered out.
    recreateProducerAndStream(
            vb,
            cb::mcbp::DcpOpenFlag::None,
            fmt::format(R"({{"collections":["{:x}"]}})",
                        uint32_t(CollectionEntry::vegetable.getId())),

            {{DcpControlKeys::SnapshotMaxMarkerVersion, "2.2"}});

    stream->transitionStateToBackfilling();
    ASSERT_TRUE(stream->isBackfilling());
    EXPECT_EQ(0, stream->public_readyQSize());

    // Perform the actual backfill - fills up the readyQ in the stream.
    producer->getBFM().backfill();

    // One snapshot marker, one system-event (collection-created), one sequence
    // number advanced.
    EXPECT_EQ(3, stream->public_readyQSize());

    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    //! The purgeSeqno should be present.
    EXPECT_EQ(purgeSeqno, snapMarker.getPurgeSeqno());
    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SystemEvent, resp->getEvent());
    resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SeqnoAdvanced, resp->getEvent());
}

TEST_P(SingleThreadedActiveStreamTest,
       CollectionFilteredStreamEnds_StartSeqnoGreaterThanEndSeqno) {
    // Reset the stream created originally as part of the test setup.
    stream.reset();
    auto& vb = *engine->getVBucket(vbid);

    store_item(vbid, makeStoredDocKey("foo", CollectionEntry::defaultC), "bar");
    store_item(
            vbid, makeStoredDocKey("foo1", CollectionEntry::defaultC), "bar");

    // Create a collection
    CollectionsManifest manifest;
    const auto& cVegetable = CollectionEntry::vegetable;
    manifest.add(cVegetable,
                 cb::NoExpiryLimit,
                 true /*history*/,
                 ScopeEntry::defaultS);
    vb.updateFromManifest(
            std::shared_lock<folly::SharedMutex>(vb.getStateLock()),
            Collections::Manifest{std::string{manifest}});

    store_item(vbid, makeStoredDocKey("potato", cVegetable), "value");
    store_item(vbid, makeStoredDocKey("carrot", cVegetable), "value");

    ASSERT_EQ(5, vb.getHighSeqno());
    flushAndRemoveCheckpoints(vbid);

    // Request end seqno below what is relevant to the stream.
    stream = producer->addMockActiveStream(
            cb::mcbp::DcpAddStreamFlag::ActiveVbOnly,
            0 /*opaque*/,
            vb,
            0 /*st_seqno*/,
            2 /*en_seqno*/,
            0x0 /*vb_uuid*/,
            0 /*snap_start_seqno*/,
            ~0ULL /*snap_end_seqno*/,
            producer->public_getIncludeValue(),
            producer->public_getIncludeXattrs(),
            producer->public_getIncludeDeletedUserXattrs(),
            producer->public_getMaxMarkerVersion(),
            fmt::format(R"({{"collections":["{:x}"]}})",
                        uint32_t(cVegetable.getId())));

    auto resp = stream->next(*producer);
    ASSERT_EQ(DcpResponse::Event::StreamEnd, resp->getEvent());

    ASSERT_TRUE(stream->isDead());
}

// MB-65019: Collection filtered stream should not end immediately when
// start seqno is equal to collection's start seqno.
TEST_P(SingleThreadedActiveStreamTest,
       NoRollbackStartSeqnoEqualCollectionStartSeqno) {
    // Reset the stream created originally as part of the test setup
    stream.reset();
    auto& vb = *engine->getVBucket(vbid);

    // Store item in default collection
    store_item(vbid, makeStoredDocKey("foo"), "bar"); // seqno:1

    // Create a collection - seqno:2
    CollectionsManifest manifest;
    const auto& cVegetable = CollectionEntry::vegetable;
    manifest.add(cVegetable,
                 cb::NoExpiryLimit,
                 true /*history*/,
                 ScopeEntry::defaultS);
    vb.updateFromManifest(
            std::shared_lock<folly::SharedMutex>(vb.getStateLock()),
            Collections::Manifest{std::string{manifest}});

    store_item(
            vbid, makeStoredDocKey("potato", cVegetable), "value"); // seqno:3
    store_item(vbid, makeStoredDocKey("foo1"), "bar"); // seqno:4

    // Delete some items and add one to advance seqno
    delete_item(vbid, makeStoredDocKey("foo")); // seqno:5
    delete_item(vbid, makeStoredDocKey("potato", cVegetable)); // seqno:6
    store_item(
            vbid, makeStoredDocKey("lettuce", cVegetable), "value"); // seqno:7

    flushAndRemoveCheckpoints(vbid);

    // Run compaction/purge to advance purge_seqno past collection creation
    // seqno
    purgeTombstonesBefore(6);
    EXPECT_EQ(6, vb.getPurgeSeqno());

    // Request stream with start seqno 0 and collection filter for vegetable
    // Backfill startSeqno should be adjusted to 2 due to collection filter
    stream = producer->addMockActiveStream(
            cb::mcbp::DcpAddStreamFlag::ActiveVbOnly,
            0 /*opaque*/,
            vb,
            0 /*st_seqno*/,
            ~0ULL /*en_seqno*/,
            0x0 /*vb_uuid*/,
            0 /*snap_start_seqno*/,
            ~0ULL /*snap_end_seqno*/,
            producer->public_getIncludeValue(),
            producer->public_getIncludeXattrs(),
            producer->public_getIncludeDeletedUserXattrs(),
            producer->public_getMaxMarkerVersion(),
            fmt::format(R"({{"collections":["{:x}"]}})",
                        uint32_t(cVegetable.getId())));

    // Perform the backfill
    producer->getBFM().backfill();

    // Stream should not end immediately as items need to be sent starting at
    // seqno 2 onwards.
    auto resp = stream->next(*producer);
    ASSERT_NE(DcpResponse::Event::StreamEnd, resp->getEvent());
    ASSERT_FALSE(stream->isDead());
}

TEST_P(SingleThreadedActiveStreamTest, backfillYieldsAfterDurationLimit) {
    auto vb = engine->getVBucket(vbid);
    // Remove the initial stream, we want to force it to backfill.
    stream.reset();
    store_item(vbid, makeStoredDocKey("k0"), "v");
    store_item(vbid, makeStoredDocKey("k1"), "v");
    // Ensure mutation is on disk; no longer present in CheckpointManager.
    vb->checkpointManager->createNewCheckpoint();
    flushVBucketToDiskIfPersistent(vbid, 2);
    engine->getConfiguration().setDcpBackfillRunDurationLimit(0);
    recreateStream(*vb);
    ASSERT_TRUE(stream->isBackfilling());
    EXPECT_EQ(0, engine->getConfiguration().getDcpBackfillRunDurationLimit());

    auto& bfm = producer->getBFM();

    // first run will backfill the first item then yield
    EXPECT_EQ(backfill_success, bfm.backfill());
    EXPECT_EQ(1, stream->getNumBackfillItems());
    EXPECT_EQ(2, stream->public_readyQ().size());
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker,
              stream->public_nextQueuedItem(*producer)->getEvent());
    EXPECT_EQ(DcpResponse::Event::Mutation,
              stream->public_nextQueuedItem(*producer)->getEvent());
    EXPECT_EQ(1, stream->getNumBackfillPauses());
}

TEST_P(SingleThreadedPassiveStreamTest, PurgeSeqnoInDiskCheckpoint) {
    auto& vb = *store->getVBucket(vbid);
    auto& manager = static_cast<MockCheckpointManager&>(*vb.checkpointManager);
    const auto& ckptList = manager.getCheckpointList();
    ASSERT_EQ(1, ckptList.size());
    ASSERT_EQ(CheckpointType::Memory, ckptList.front()->getCheckpointType());
    ASSERT_EQ(0, ckptList.front()->getSnapshotStartSeqno());
    ASSERT_EQ(0, ckptList.front()->getSnapshotEndSeqno());
    // cs, vbs
    ASSERT_EQ(2, ckptList.front()->getNumItems());
    ASSERT_EQ(0, manager.getHighSeqno());

    // Replica receives a complete disk snapshot {keyA:1, keyB:2}
    const uint32_t opaque = 1;
    const uint64_t snapStart = 1;
    const uint64_t snapEnd = 3;
    EXPECT_EQ(
            cb::engine_errc::success,
            consumer->snapshotMarker(opaque,
                                     vbid,
                                     snapStart,
                                     snapEnd,
                                     DcpSnapshotMarkerFlag::Disk |
                                             DcpSnapshotMarkerFlag::Checkpoint,
                                     {} /*HCS*/,
                                     {} /*HPS*/,
                                     {} /*maxVisibleSeqno*/,
                                     2));

    const auto keyA = makeStoredDocKey("keyA");
    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(
                      opaque, keyA, {}, 0, 0, vbid, 0, snapStart, 0, 0, 0, 0));
    const auto keyB = makeStoredDocKey("keyB");
    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(
                      opaque, keyB, {}, 0, 0, vbid, 0, snapEnd, 0, 0, 0, 0));

    ASSERT_EQ(1, ckptList.size());
    ASSERT_TRUE(ckptList.front()->isDiskCheckpoint());
    ASSERT_EQ(1, ckptList.front()->getSnapshotStartSeqno());
    ASSERT_EQ(3, ckptList.front()->getSnapshotEndSeqno());
    ASSERT_EQ(3, ckptList.front()->getNumItems());
    ASSERT_EQ(3, manager.getHighSeqno());

    consumer->closeStream(opaque, vbid);

    setVBucketState(vbid, vbucket_state_active);
    auto* cookie1 = create_mock_cookie();
    cookie1->setCollectionsSupport(true);
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                 cookie1,
                                                 "test_producer->test_consumer",
                                                 cb::mcbp::DcpOpenFlag::None,
                                                 false /*startTask*/);

    EXPECT_EQ(cb::engine_errc::success,
              producer->control(0 /*opaque*/,
                                DcpControlKeys::SnapshotMaxMarkerVersion,
                                "2.2"));

    producer->createCheckpointProcessorTask();
    producer->scheduleCheckpointProcessorTask();

    const auto& activeStream = producer->addMockActiveStream(
            {},
            0 /*opaque*/,
            vb,
            0 /*st_seqno*/,
            ~0ULL /*en_seqno*/,
            0x0 /*vb_uuid*/,
            0 /*snap_start_seqno*/,
            ~0ULL /*snap_end_seqno*/,
            producer->public_getIncludeValue(),
            producer->public_getIncludeXattrs(),
            producer->public_getIncludeDeletedUserXattrs(),
            producer->public_getMaxMarkerVersion(),
            {});

    ASSERT_TRUE(activeStream->isInMemory());
    auto& readyQ = activeStream->public_readyQ();
    ASSERT_EQ(0, readyQ.size());

    MockDcpMessageProducers producers;
    runCheckpointProcessor(*producer, producers);

    // One snapshot marker & two mutation.
    EXPECT_EQ(3, activeStream->public_readyQSize());

    auto resp = activeStream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    // Expect to find the purgeSeqno set on the initial disk checkpoint.
    EXPECT_EQ(2, snapMarker.getPurgeSeqno());
    destroy_mock_cookie(cookie1);
}

TEST_P(SingleThreadedActiveStreamTest, backfillCompletesWithoutYielding) {
    auto vb = engine->getVBucket(vbid);
    // Remove the initial stream, we want to force it to backfill.
    stream.reset();
    store_item(vbid, makeStoredDocKey("k0"), "v");
    store_item(vbid, makeStoredDocKey("k1"), "v");
    // Ensure mutation is on disk; no longer present in CheckpointManager.
    vb->checkpointManager->createNewCheckpoint();
    flushVBucketToDiskIfPersistent(vbid, 2);

    std::chrono::milliseconds maxDuration = std::chrono::milliseconds::max();
    engine->getConfiguration().setDcpBackfillRunDurationLimit(
            maxDuration.count());
    recreateStream(*vb);
    ASSERT_TRUE(stream->isBackfilling());
    EXPECT_EQ(maxDuration.count(),
              engine->getConfiguration().getDcpBackfillRunDurationLimit());

    auto& bfm = producer->getBFM();

    // first run will backfill both items without yielding
    EXPECT_EQ(backfill_success, bfm.backfill());
    EXPECT_EQ(2, stream->getNumBackfillItems());
    EXPECT_EQ(3, stream->public_readyQ().size());
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker,
              stream->public_nextQueuedItem(*producer)->getEvent());
    EXPECT_EQ(DcpResponse::Event::Mutation,
              stream->public_nextQueuedItem(*producer)->getEvent());
    EXPECT_EQ(DcpResponse::Event::Mutation,
              stream->public_nextQueuedItem(*producer)->getEvent());
    EXPECT_EQ(0, stream->getNumBackfillPauses());
}

TEST_P(SingleThreadedActiveStreamTest,
       StreamRequestRollbackWhenPurgeSeqnoChanges) {
    if (ephemeral()) {
        GTEST_SKIP();
    }

    stream.reset();
    auto& vb = *engine->getVBucket(vbid);

    // Store & delete some items.
    store_item(vbid, makeStoredDocKey("foo"), "bar"); // Seqno: 1
    store_item(vbid, makeStoredDocKey("foo1"), "bar"); // Seqno: 2
    delete_item(vbid, makeStoredDocKey("foo")); // Seqno: 3
    store_item(vbid, makeStoredDocKey("foo2"), "bar"); // Seqno: 4

    flushAndRemoveCheckpoints(vbid);
    // Run compaction for persistent buckets & tombstone purger for ephemeral
    // buckets.
    purgeTombstonesBefore(3);
    auto purgeSeqno = vb.getPurgeSeqno();
    EXPECT_EQ(3, purgeSeqno);

    producer = std::make_shared<MockDcpProducer>(*engine,
                                                 cookie,
                                                 "test_producer->test_consumer",
                                                 cb::mcbp::DcpOpenFlag::None,
                                                 false /*startTask*/);

    producer->createCheckpointProcessorTask();
    producer->setSyncReplication(SyncReplication::SyncReplication);
    producer->setMaxMarkerVersion(MarkerVersion::V2_2);

    stream = producer->addMockActiveStream(
            {} /*flags*/,
            0 /*opaque*/,
            vb,
            2 /*st_seqno*/,
            ~0ULL /*en_seqno*/,
            0x0 /*vb_uuid*/,
            0 /*snap_start_seqno*/,
            ~0ULL /*snap_end_seqno*/,
            producer->public_getIncludeValue(),
            producer->public_getIncludeXattrs(),
            producer->public_getIncludeDeletedUserXattrs(),
            producer->public_getMaxMarkerVersion(),
            R"({"purge_seqno":"3"})",
            {});

    stream->transitionStateToBackfilling();
    ASSERT_TRUE(stream->isBackfilling());
    EXPECT_EQ(0, stream->public_readyQSize());

    // Perform the actual backfill - fills up the readyQ in the stream.
    producer->getBFM().backfill();

    // One snapshot marker & the mutation at seqno 4 (st_seqno requested above
    // was 2 & compaction removed seqno: 3.)
    EXPECT_EQ(2, stream->public_readyQSize());

    stream.reset();

    producer = std::make_shared<MockDcpProducer>(*engine,
                                                 cookie,
                                                 "test_producer->test_consumer",
                                                 cb::mcbp::DcpOpenFlag::None,
                                                 false /*startTask*/);

    producer->createCheckpointProcessorTask();
    producer->setSyncReplication(SyncReplication::SyncReplication);

    stream = producer->addMockActiveStream(
            {} /*flags*/,
            0 /*opaque*/,
            vb,
            2 /*st_seqno*/,
            ~0ULL /*en_seqno*/,
            0x0 /*vb_uuid*/,
            0 /*snap_start_seqno*/,
            ~0ULL /*snap_end_seqno*/,
            producer->public_getIncludeValue(),
            producer->public_getIncludeXattrs(),
            producer->public_getIncludeDeletedUserXattrs(),
            producer->public_getMaxMarkerVersion(),
            R"({"purge_seqno":"3"})",
            {});

    // Before the backfill runs & after the stream request has been processed,
    // process a few more mutations & run compaction. This would move the purge
    // seqno & send stream-end to the client.

    delete_item(vbid, makeStoredDocKey("foo1")); // Seqno: 5
    store_item(vbid, makeStoredDocKey("foo3"), "bar"); // Seqno: 6

    flushAndRemoveCheckpoints(vbid);
    // Run compaction for persistent buckets & tombstone purger for ephemeral
    // buckets.
    purgeTombstonesBefore(5);
    purgeSeqno = vb.getPurgeSeqno();
    EXPECT_EQ(5, purgeSeqno);

    // Perform the actual backfill - fills up the readyQ in the stream.
    producer->getBFM().backfill();

    // One Stream end message.
    // 1. The stream was created when the purge seqno was at 3 & a compaction
    //    ran and changed the purge seqno to 5, and therefore we should
    //    rollback.
    EXPECT_EQ(1, stream->public_readyQSize());

    auto resp = stream->next(*producer);
    EXPECT_EQ(DcpResponse::Event::StreamEnd, resp->getEvent());
}

TEST_P(SingleThreadedActiveStreamTest, MB_65581) {
    // Just start with a blank checkpoint
    stream.reset();
    auto& vb = *store->getVBucket(vbid);
    removeCheckpoint(vb);
    auto& manager = *vb.checkpointManager;
    ASSERT_EQ(1, manager.getNumCheckpoints());

    // Create a collection
    CollectionsManifest manifest;
    const auto& cFruit = CollectionEntry::fruit;
    manifest.add(
            cFruit, cb::NoExpiryLimit, true /*history*/, ScopeEntry::defaultS);
    vb.updateFromManifest(
            std::shared_lock<folly::SharedMutex>(vb.getStateLock()),
            Collections::Manifest{std::string{manifest}});
    ASSERT_EQ(1, manager.getHighSeqno());

    // seqno:2 in cFruit
    const std::string value("value");
    store_item(vbid, makeStoredDocKey("keyF", cFruit), value);
    ASSERT_EQ(2, manager.getHighSeqno());

    // Add some data to the default collection
    const auto& cDefault = CollectionEntry::defaultC;
    store_item(vbid, makeStoredDocKey("keyD", cDefault), value);
    ASSERT_EQ(3, manager.getHighSeqno());

    // [e:1 cs:1 se:1 m(keyF):2 m(keyD):3)

    // Other mutations queued in the same checkpoint
    store_item(vbid, makeStoredDocKey("keyDD", cDefault), value);
    ASSERT_EQ(4, manager.getHighSeqno());

    // [e:1 cs:1 se:1 m(keyF):2 m(keyD):3) m(keyDD):4)

    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(5, manager.getNumItems());

    // Note: Placing a dummy cursor in the first checkpoint on ephemeral for
    // preventing that closed checkpoints are removed during the test.
    // Not necessary for persistent bucket as we just don't move the persistence
    // cursor.
    std::shared_ptr<CheckpointCursor> dummyCursor;
    if (ephemeral()) {
        dummyCursor =
                manager.registerCursorBySeqno("dummy-cursor",
                                              0,
                                              CheckpointCursor::Droppable::Yes)
                        .takeCursor()
                        .lock();
    }

    // Other mutations queued in new checkpoint
    manager.createNewCheckpoint();
    ASSERT_EQ(2, manager.getNumCheckpoints());

    // [e:1 cs:1 se:1 m(keyF):2 m(keyD):3) m(keyDD):4 ce:5]
    //
    // [e:5 cs:5)

    store_item(vbid, makeStoredDocKey("keyFF", cFruit), value);
    ASSERT_EQ(2, manager.getNumCheckpoints());
    ASSERT_EQ(5, manager.getHighSeqno());

    // [e:1 cs:1 se:1 m(keyF):2 m(keyD):3) m(keyDD):4 ce:5]
    //
    // [e:5 cs:5 m(keyFF):5)

    store_item(vbid, makeStoredDocKey("keyDDD", cDefault), value);
    ASSERT_EQ(2, manager.getNumCheckpoints());
    ASSERT_EQ(6, manager.getHighSeqno());

    // [e:1 cs:1 se:1 m(keyF):2 m(keyD):3) m(keyDD):4 ce:5]
    //
    // [e:5 cs:5 m(keyFF):5 m(keyDDD):6)

    // Now we simulate all data persisted (ie up to s:6), s:1 expelled and a DCP
    // client that:
    // - connects for the first time
    // - filters on collection fruits
    // - start:0 -> backfill as s:1 expelled
    // - gets SnapMarker(Disk, 1, 6)
    // - gets up to seqno:2
    // - disconnect BEFORE getting SeqnoAdvance(3)
    //
    // At reconnecting that DCP client will issue a
    // StreamReq(start:2, snap:[1, 6])

    stream = producer->addMockActiveStream(
            cb::mcbp::DcpAddStreamFlag::None,
            0 /*opaque*/,
            vb,
            2 /*st_seqno*/,
            ~0ULL /*en_seqno*/,
            0x0 /*vb_uuid*/,
            1 /*snap_start_seqno*/,
            6 /*snap_end_seqno*/,
            producer->public_getIncludeValue(),
            producer->public_getIncludeXattrs(),
            producer->public_getIncludeDeletedUserXattrs(),
            fmt::format(R"({{"collections":["{:x}"]}})", uint32_t(cFruit.uid)));
    ASSERT_TRUE(stream);

    auto& cursor = stream->getCursor();
    EXPECT_EQ(2, (*cursor.lock()->getPos())->getBySeqno());
    EXPECT_EQ(6, manager.getNumItemsForCursor(*cursor.lock()));
    EXPECT_EQ(2, stream->getLastReadSeqno());
    EXPECT_EQ(6, stream->getSnapEndSeqno());
    // Note: At registerCursor this is set to the seqno of the next item for
    // cursor.
    EXPECT_EQ(3, stream->getCurChkSeqno());

    // [e:1 cs:1 se:1 m(keyF):2 m(keyD):3) m(keyDD):4 ce:5]
    //                        ^
    // [e:5 cs:5 m(keyFF):5 m(keyDDD):6)

    // Note: Purpose here is forcing a behaviour where DCP pulls 1 checkpoint
    // a time. That is for reproducing the real scenario where at reconnect DCP
    // manages to process a first checkpoint in isolation and sets lastReadSeqno
    // to some value. Then DCP processes a second checkpoint that triggers the
    // monotonic invariant on lastReadSeqno. See next steps for details.
    auto& config = engine->getConfiguration();
    config.setCheckpointMaxSize(1);
    ASSERT_EQ(1, manager.getCheckpointConfig().getCheckpointMaxSize());

    auto& readyQ = stream->public_readyQ();
    ASSERT_EQ(0, readyQ.size());

    // DCP gets only the first checkpoint
    stream->nextCheckpointItemTask();
    EXPECT_EQ(3, manager.getNumItemsForCursor(*cursor.lock()));
    EXPECT_EQ(queue_op::empty, (*cursor.lock()->getPos())->getOperation());
    EXPECT_EQ(5, (*cursor.lock()->getPos())->getBySeqno());

    // Before the fix: lastReadSeqno=6 as we sent SeqnoAdvance(6)
    // EXPECT_EQ(6, stream->getLastReadSeqno());
    // At fix the last nextCheckpointItemTask() hasn't produced any data:
    EXPECT_EQ(4, stream->getLastReadSeqno());

    // Checks on DCP readyQ
    //
    // Before the fix, at this point we have queued SnapMarker(1, 6) +
    // SeqnoAdvance(6).
    // While at fix:
    ASSERT_EQ(0, readyQ.size());

    // This is the current state in checkpoint:
    //
    // [e:1 cs:1 se:1 m(keyF):2 m(keyD):3) m(keyDD):4 ce:5]
    //
    // [e:5 cs:5 m(keyFF):5 m(keyDDD):6)
    //    ^

    // DCP moves and gets the second checkpoint.
    //
    // ActiveStream processes the snapshot and tries to update lastReadSeqno.
    //
    // Before the fix this call throws by monotonic invariant failure on
    // lastReadSeqno.
    stream->nextCheckpointItemTask();
    EXPECT_EQ(0, manager.getNumItemsForCursor(*cursor.lock()));

    // At fix:
    {
        ASSERT_EQ(2, readyQ.size());
        // SnapMarker(1, 5)
        auto resp = stream->public_nextQueuedItem(*producer);
        EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
        auto* marker = dynamic_cast<SnapshotMarker*>(resp.get());
        EXPECT_EQ(1, marker->getStartSeqno());
        EXPECT_EQ(5, marker->getEndSeqno());
        // Mutation(5)
        resp = stream->public_nextQueuedItem(*producer);
        EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
        EXPECT_EQ(5, resp->getBySeqno());
    }
}

void SingleThreadedActiveStreamTest::pushStreamToTakeoverBackupPhase() {
    // Note: The test runs a full takeover phase on an empty vbucket, no need
    // for data.

    // Prepare for recreating a ActiveStream that pushes the vbucket into a
    // takeover-backup state at the very first TakeoverSend phase
    auto& config = engine->getConfiguration();
    ASSERT_NE(0, config.getDcpTakeoverMaxTime());
    config.setDcpTakeoverMaxTime(0);
    ASSERT_EQ(0, config.getDcpTakeoverMaxTime());

    // Ensure ActiveStream::takeoverStart > 0 at stream creation. The stream
    // would miss to set the VBucket::takeover_backed_up flag in the subsequent
    // TakeoverSend phase otherwise.
    TimeTraveller t1(1);

    // Note: dcp_takeover_max_time applied to newly create streams
    EXPECT_NE(0, stream->getTakeoverSendMaxTime());
    auto& vb = *store->getVBucket(vbid);
    recreateStream(vb, true, {}, cb::mcbp::DcpAddStreamFlag::TakeOver);
    ASSERT_TRUE(stream);
    ASSERT_EQ(0, stream->getTakeoverSendMaxTime());
    ASSERT_TRUE(stream->isTakeoverStream());
    ASSERT_TRUE(stream->isTakeoverSend());

    // Vbucket clear, still accepting frontend traffic
    ASSERT_FALSE(vb.isTakeoverBackedUp());

    // Ensure that at least 1sec has past since takeoverStart. The stream might
    // miss to set the VBucket::takeover_backed_up flag in the subsequent
    // TakeoverSend phase otherwise.
    TimeTraveller t2(1);

    // Just move the stream's cursor at the end of checkpoint for avoiding some
    // additional TakeoverSend calls in the next steps.
    stream->nextCheckpointItemTask();

    // First TakeoverSend call. That just sends a SetVBState(pending) message to
    // the peer.
    auto resp = stream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SetVbucket, resp->getEvent());
    EXPECT_EQ(vbucket_state_pending,
              dynamic_cast<const SetVBucketState&>(*resp).getState());

    // The stream's TakeoverSend phase has already pushed the vbucket into a
    // takeover-backup state.
    EXPECT_TRUE(vb.isTakeoverBackedUp());
}

TEST_P(SingleThreadedActiveStreamTest, TakeoverBackupPhase) {
    pushStreamToTakeoverBackupPhase();

    // At TakeoverSend the stream has sent a SetVBState(pending) message to the
    // peer. Now the stream is in TakeoverWait, waiting for the SetVBState ACK
    // from the peer.
    EXPECT_TRUE(stream->isTakeoverWait());
    // The vbucket is still active
    auto& vb = *store->getVBucket(vbid);
    EXPECT_EQ(vbucket_state_active, vb.getState());

    // Ensure ActiveStream::takeoverStart is reset to > 0 when the stream
    // transitions back to TakeoverSend. The stream would miss to set the
    // VBucket::takeover_backed_up flag in the subsequent TakeoverSend phase
    // otherwise.
    TimeTraveller t1(1);

    // Now the stream received the ACK
    stream->setVBucketStateAckRecieved(*producer);
    // The stream has moved back into TakeoverSend
    ASSERT_TRUE(stream->isTakeoverSend());
    // The vbucket has been set to dead
    EXPECT_EQ(vbucket_state_dead, vb.getState());
    // VBucket::takeover_backed_up flag cleared as the vbucket has transitioned
    // to !active
    EXPECT_FALSE(vb.isTakeoverBackedUp());

    // Ensure that at least 1sec has past since takeoverStart. The stream might
    // miss to set the VBucket::takeover_backed_up flag in the subsequent
    // TakeoverSend phase otherwise.
    TimeTraveller t2(1);

    // Again, the vbstate transition has created a new checkpoint. Just move the
    // stream's cursor at the end of checkpoint for avoiding some additional
    // TakeoverSend calls in the next steps.
    stream->nextCheckpointItemTask();

    // TakeoverSend sends a SetVBState(active) to the peer
    const auto resp = stream->next(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SetVbucket, resp->getEvent());
    EXPECT_EQ(vbucket_state_active,
              dynamic_cast<const SetVBucketState&>(*resp).getState());

    // Stream back to TakeoverWait, waiting for the SetVBState ACK from the peer
    EXPECT_TRUE(stream->isTakeoverWait());
    // VBucket::takeover_backed_up flag set again
    EXPECT_TRUE(vb.isTakeoverBackedUp());

    // Now the stream received the ACK
    stream->setVBucketStateAckRecieved(*producer);
    // The stream has been set to dead
    EXPECT_TRUE(stream->isDead());
    // IMPORTANT:
    // The VBucket::takeover_backed_up flag must be cleared at the end of the
    // takeover procedure. Before MB-66609 we miss to do that and we leave the
    // vbucket in a endless takeover-backup state.
    EXPECT_FALSE(vb.isTakeoverBackedUp());
}

TEST_P(SingleThreadedActiveStreamTest, TakeoverBackupPhase_EarlyStreamEnd) {
    pushStreamToTakeoverBackupPhase();

    // Simulate disconnection
    producer->closeAllStreams();

    // The stream has been set to dead
    EXPECT_TRUE(stream->isDead());
    // IMPORTANT:
    // The VBucket::takeover_backed_up flag must be cleared at the end of the
    // takeover procedure. Before MB-66609 we miss to do that and we leave the
    // vbucket in a endless takeover-backup state.
    auto& vb = *store->getVBucket(vbid);
    EXPECT_FALSE(vb.isTakeoverBackedUp());
}

TEST_P(SingleThreadedActiveStreamTest,
       ProcessStreamsExceptionDisconnectsConnection) {
    // Set the producer to catch exceptions - new behaviour
    engine->getConfiguration().setDcpProducerCatchExceptions(true);

    // Recreate stream
    auto& vb = *store->getVBucket(vbid);
    MockDcpMessageProducers producers;
    recreateStream(vb, false);

    // Add an item
    store_item(vbid, makeStoredDocKey("key"), "value");

    // Confirm ActiveStream::processItems will throw an exception
    stream->processItemsHook = []() {
        throw std::runtime_error("processItemsHook throw");
    };
    auto items = stream->public_getOutstandingItems(vb);
    try {
        stream->public_processItems(items);
        FAIL() << "No exception thrown";
    } catch (const std::runtime_error& e) {
        EXPECT_EQ(std::string("processItemsHook throw"), e.what());
    }

    // Write an item so that the task will be scheduled by all bucket types
    store_item(vbid, makeStoredDocKey("key"), "value");

    // Call producer->step() to execute ActiveStream::nextCheckpointItemTask()
    // which will catch the exception thrown by processItems().
    ASSERT_EQ(cb::engine_errc::would_block, producer->step(false, producers));
    auto& nonIO = *task_executor->getLpTaskQ(TaskType::NonIO);
    EXPECT_NO_THROW(runNextTask(nonIO,
                                "Process checkpoint(s) for DCP producer "
                                "test_producer->test_consumer"));

    // Producer should disconnect connection now
    EXPECT_EQ(cb::engine_errc::disconnect, producer->step(false, producers));
}

TEST_P(SingleThreadedActiveStreamTest, ProcessStreamsExceptionCrashes) {
    // Set the producer to crash on failure - old behaviour
    engine->getConfiguration().setDcpProducerCatchExceptions(false);

    // Add an item
    store_item(vbid, makeStoredDocKey("key"), "value");

    // Create stream
    auto& vb = *store->getVBucket(vbid);
    MockDcpMessageProducers producers;
    recreateStream(vb, false);

    // Confirm ActiveStream::processItems will throw an exception
    stream->processItemsHook = []() {
        throw std::runtime_error("processItemsHook throw");
    };
    auto items = stream->public_getOutstandingItems(vb);
    try {
        stream->public_processItems(items);
        FAIL() << "No exception thrown";
    } catch (const std::runtime_error& e) {
        EXPECT_EQ(std::string("processItemsHook throw"), e.what());
    }

    // Call producer->step() to execute ActiveStream::nextCheckpointItemTask()
    // Exception will not be caught
    ASSERT_EQ(cb::engine_errc::would_block, producer->step(false, producers));
    auto& nonIO = *task_executor->getLpTaskQ(TaskType::NonIO);
    try {
        runNextTask(nonIO,
                    "Process checkpoint(s) for DCP producer "
                    "test_producer->test_consumer");
        FAIL() << "No exception thrown";
    } catch (const std::runtime_error& e) {
        EXPECT_EQ(std::string("processItemsHook throw"), e.what());
    }
    // Producer should NOT disconnect connection
    EXPECT_NE(cb::engine_errc::disconnect, producer->step(false, producers));
}

TEST_P(SingleThreadedActiveStreamTest,
       backfillReceivedExceptionDisconnectsConnection) {
    engine->getConfiguration().setDcpProducerCatchExceptions(true);
    stream.reset();

    // Add an item and flush to disk
    store_item(vbid, makeStoredDocKey("key"), "value");
    flushAndRemoveCheckpoints(vbid);

    // Create stream
    auto& vb = *store->getVBucket(vbid);
    MockDcpMessageProducers producers;
    recreateStream(vb, false);

    // Set the backfillReceivedHook to throw an exception
    stream->backfillReceivedHook = []() {
        throw std::runtime_error("backfillReceivedHook throw");
    };

    stream->transitionStateToBackfilling();
    ASSERT_TRUE(stream->isBackfilling());

    // Perform backfill - which will throw an exception
    // Exception will be caught and connection will be disconnected
    EXPECT_NO_THROW(producer->getBFM().backfill());

    // Producer should disconnect connection now
    EXPECT_EQ(cb::engine_errc::disconnect, producer->step(false, producers));
}

TEST_P(SingleThreadedActiveStreamTest, backfillReceivedExceptionCrashes) {
    // This test has a memory leak on couchstore - test only issue
    // AS::BackfillReceivedHook is called from backfillCallback()
    // When we EXPECT_THROW, we will continue running even after the exception
    // has not been handled and so couchstore has remaining memory allocated
    // that is not released. In GA memcached will crash and so this allocation
    // will not be the major issue...
    if (isCouchstore() || isNexus()) {
        GTEST_SKIP();
    }

    engine->getConfiguration().setDcpProducerCatchExceptions(false);
    stream.reset();

    // Add an item and flush to disk
    store_item(vbid, makeStoredDocKey("key"), "value");
    flushAndRemoveCheckpoints(vbid);

    // Create stream
    auto& vb = *store->getVBucket(vbid);
    MockDcpMessageProducers producers;
    recreateStream(vb, false);

    // Set the backfillReceivedHook to throw an exception
    stream->backfillReceivedHook = []() {
        throw std::runtime_error("backfillReceivedHook throw");
    };

    stream->transitionStateToBackfilling();
    ASSERT_TRUE(stream->isBackfilling());

    // Perform backfill - which will throw an exception
    EXPECT_THROW(producer->getBFM().backfill(), std::exception);

    // Producer will NOT disconnect connection
    EXPECT_NE(cb::engine_errc::disconnect, producer->step(false, producers));
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

INSTANTIATE_TEST_SUITE_P(
        AllBucketTypes,
        SingleThreadedActiveToPassiveStreamTest,
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
    ASSERT_TRUE(engine->isMagmaBlindWriteOptimisationEnabled());
    const std::string value("value");

    // Receive initial disk snapshot. Expect inserts for items in this snapshot.
    SnapshotMarker marker(
            0 /*opaque*/,
            vbid,
            0 /*snapStart*/,
            3 /*snapEnd*/,
            DcpSnapshotMarkerFlag::Disk | DcpSnapshotMarkerFlag::Checkpoint,
            0 /*HCS*/,
            {},
            {} /*maxVisibleSeqno*/,
            std::nullopt,
            {} /*streamId*/);

    stream->processMarker(&marker);
    auto vb = engine->getVBucket(vbid);
    ASSERT_TRUE(vb->checkpointManager->isOpenCheckpointInitialDisk());

    for (uint64_t seqno = 1; seqno <= 2; seqno++) {
        auto ret = stream->messageReceived(
                makeMutationResponse(seqno, vbid, value, 0));
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
    stream->reconnectStream(*vb, 0, 2);
    stream->processMarker(&marker);

    ASSERT_FALSE(vb->checkpointManager->isOpenCheckpointInitialDisk());
    auto ret = stream->messageReceived(makeMutationResponse(3, vbid, value, 0));
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
            DcpSnapshotMarkerFlag::Disk | DcpSnapshotMarkerFlag::Checkpoint,
            0 /*HCS*/,
            {},
            {} /*maxVisibleSeqno*/,
            std::nullopt,
            {} /*streamId*/);

    stream->processMarker(&marker);
    ASSERT_FALSE(vb->checkpointManager->isOpenCheckpointInitialDisk());

    for (uint64_t seqno = 4; seqno <= 6; seqno++) {
        const auto rv = stream->messageReceived(
                makeMutationResponse(seqno, vbid, value, 0));
        ASSERT_EQ(cb::engine_errc::success, rv);
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

// Test that we issue Magma insert operations for items streamed in initial disk
// snapshot.
TEST_P(STPassiveStreamMagmaTest, DisableBlindWriteOptimisation) {
    mock_set_magma_blind_write_optimisation_enabled(false);
    ASSERT_FALSE(engine->isMagmaBlindWriteOptimisationEnabled());

    const std::string value("value");
    // Receive initial disk snapshot. Expect inserts for items in this snapshot.
    SnapshotMarker marker(
            0 /*opaque*/,
            vbid,
            0 /*snapStart*/,
            3 /*snapEnd*/,
            DcpSnapshotMarkerFlag::Disk | DcpSnapshotMarkerFlag::Checkpoint,
            0 /*HCS*/,
            {}, /*highPreparedSeqno*/
            {}, /*maxVisibleSeqno*/
            {}, /*purgeSeqno*/
            {} /*streamId*/);

    stream->processMarker(&marker);
    auto vb = engine->getVBucket(vbid);
    ASSERT_TRUE(vb->checkpointManager->isOpenCheckpointInitialDisk());

    for (uint64_t seqno = 1; seqno <= 2; seqno++) {
        auto ret = stream->messageReceived(
                makeMutationResponse(seqno, vbid, value, 0));
        ASSERT_EQ(cb::engine_errc::success, ret);
    }

    flushVBucketToDiskIfPersistent(vbid, 2);
    EXPECT_EQ(vb->getNumItems(), 2);

    size_t inserts = 0;
    size_t upserts = 0;
    store->getKVStoreStat("magma_NInserts", inserts);
    store->getKVStoreStat("magma_NSets", upserts);
    EXPECT_EQ(inserts, 0);
    EXPECT_EQ(upserts, 2);
    mock_set_magma_blind_write_optimisation_enabled(true);
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
                                  DcpSnapshotMarkerFlag::Disk,
                                  std::optional<uint64_t>(1) /*HCS*/,
                                  {},
                                  {} /*maxVisibleSeqno*/,
                                  std::nullopt,
                                  {} /*streamId*/);
    stream->processMarker(&snapshotMarker);

    // PRE:1
    const std::string value("value");
    using namespace cb::durability;
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationResponse(
                      1 /*seqno*/,
                      vbid,
                      value,
                      opaque,
                      {},
                      Requirements(Level::Majority, Timeout::Infinity()))));

    // M:2 - Logic Commit for PRE:1
    // Note: implicit revSeqno=1
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(
                      makeMutationResponse(2 /*seqno*/, vbid, value, opaque)));

    // D:3
    ASSERT_EQ(
            cb::engine_errc::success,
            stream->messageReceived(makeMutationResponse(3 /*seqno*/,
                                                         vbid,
                                                         value,
                                                         opaque,
                                                         {},
                                                         {} /*DurReqs*/,
                                                         DeleteSource::Explicit,
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
        EXPECT_EQ(type, vbs.checkpointType);
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
        CB_SCOPED_TRACE("");
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
        CB_SCOPED_TRACE("");
        // Notes:
        //   1) expected (snapStart = snapEnd) for complete snap flushed
        //   2) expected (HPS = snapEnd) for complete Disk snap flushed
        checkVBState(3 /*lastSnapStart*/,
                     3 /*lastSnapEnd*/,
                     CheckpointType::InitialDisk,
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
                                  DcpSnapshotMarkerFlag::Memory,
                                  {} /*HCS*/,
                                  {},
                                  {} /*maxVisibleSeqno*/,
                                  std::nullopt,
                                  {} /*streamId*/);
    stream->processMarker(&snapshotMarker);
    // M:1
    const std::string value("value");
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(
                      makeMutationResponse(1 /*seqno*/, vbid, value, opaque)));
    // M:2
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(
                      makeMutationResponse(2 /*seqno*/, vbid, value, opaque)));
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
              stream->messageReceived(
                      makeMutationResponse(3 /*seqno*/, vbid, value, opaque)));

    EXPECT_EQ(FlushResult(MoreAvailable::No, 1), epBucket.flushVBucket(vbid));

    checkPersistedSnapshot(3, 3);
}

// Check stream-id and sync-repl cannot be enabled
TEST_P(StreamTest, multi_stream_control_denied) {
    setup_dcp_stream();
    EXPECT_EQ(cb::engine_errc::success,
              producer->control(0, DcpControlKeys::EnableSyncWrites, "true"));
    EXPECT_TRUE(producer->isSyncWritesEnabled());
    EXPECT_FALSE(producer->isMultipleStreamEnabled());

    EXPECT_EQ(cb::engine_errc::not_supported,
              producer->control(0, DcpControlKeys::EnableStreamId, "true"));
    EXPECT_TRUE(producer->isSyncWritesEnabled());
    EXPECT_FALSE(producer->isMultipleStreamEnabled());
    EXPECT_EQ(cb::engine_errc::no_such_key, destroy_dcp_stream());
}

TEST_P(StreamTest, sync_writes_denied) {
    setup_dcp_stream();
    EXPECT_EQ(cb::engine_errc::success,
              producer->control(0, DcpControlKeys::EnableStreamId, "true"));
    EXPECT_FALSE(producer->isSyncWritesEnabled());
    EXPECT_TRUE(producer->isMultipleStreamEnabled());

    EXPECT_EQ(cb::engine_errc::not_supported,
              producer->control(0, DcpControlKeys::EnableSyncWrites, "true"));
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
    const auto key = makeStoredDocKey("key");

    // check error code when stream isn't present for vbucket 99
    EXPECT_EQ(cb::engine_errc::no_such_key,
              consumer->mutation(
                      opaque, key, {}, 0, 0, Vbid(99), 0, 1, 0, 0, 0, 0));
    // check error code when using a non matching opaque
    opaque = 99999;
    EXPECT_EQ(
            cb::engine_errc::key_already_exists,
            consumer->mutation(opaque, key, {}, 0, 0, vbid, 0, 1, 0, 0, 0, 0));

    // enable V7 dcp status codes
    consumer->enableV7DcpStatus();
    // check error code when stream isn't present for vbucket 99
    opaque = 0;
    EXPECT_EQ(cb::engine_errc::stream_not_found,
              consumer->mutation(
                      opaque, key, {}, 0, 0, Vbid(99), 0, 1, 0, 0, 0, 0));
    // check error code when using a non matching opaque
    opaque = 99999;
    EXPECT_EQ(
            cb::engine_errc::opaque_no_match,
            consumer->mutation(opaque, key, {}, 0, 0, vbid, 0, 1, 0, 0, 0, 0));
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
    EXPECT_EQ(type, vbs.checkpointType);
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
                                  DcpSnapshotMarkerFlag::Disk,
                                  std::optional<uint64_t>(2) /*HCS*/,
                                  {} /*HPS*/,
                                  {} /*maxVisibleSeqno*/,
                                  std::nullopt,
                                  {} /*streamId*/);
    stream->processMarker(&snapshotMarker);

    const std::string value("value");

    // M:3 - Logic Commit for PRE:1
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(
                      makeMutationResponse(3 /*seqno*/, vbid, value, opaque)));

    // M:4 - Logic Commit for PRE:2
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(
                      makeMutationResponse(4 /*seqno*/, vbid, value, opaque)));

    flushVBucketToDiskIfPersistent(vbid, 2);

    // Notes: HPS would ideally equal the highest logically completed prepare
    // (2) in this case, but that is not possible without a protocol change to
    // send it from the active. We instead move the HPS to the snapshot end
    // (both on disk and in memory) which is 4 in this case due to changes made
    // as part of MB-34873.
    {
        CB_SCOPED_TRACE("");
        checkVBState(4 /*lastSnapStart*/,
                     4 /*lastSnapEnd*/,
                     CheckpointType::InitialDisk,
                     4 /*HPS*/,
                     2 /*HCS*/,
                     0 /*maxDelRevSeqno*/);
    }

    EXPECT_EQ(
            4,
            store->getVBucket(vbid)->acquireStateLockAndGetHighPreparedSeqno());

    ASSERT_NE(cb::engine_errc::disconnect,
              consumer->closeStream(0 /*opaque*/, vbid));
    consumer.reset();

    resetEngineAndWarmup();
    setupConsumerAndPassiveStream();

    EXPECT_EQ(
            4,
            store->getVBucket(vbid)->acquireStateLockAndGetHighPreparedSeqno());
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
                                  DcpSnapshotMarkerFlag::Disk,
                                  std::optional<uint64_t>(2) /*HCS*/,
                                  {},
                                  {} /*maxVisibleSeqno*/,
                                  std::nullopt,
                                  {} /*streamId*/);
    stream->processMarker(&snapshotMarker);

    const std::string value("value");

    // M:3 - Logic Commit for PRE:1
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(
                      makeMutationResponse(3 /*seqno*/, vbid, value, opaque)));

    // M:4 - Logic Commit for PRE:2
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(
                      makeMutationResponse(4 /*seqno*/, vbid, value, opaque)));

    // PRE:5
    using namespace cb::durability;
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(makeMutationResponse(
                      5 /*seqno*/,
                      vbid,
                      value,
                      opaque,
                      {},
                      Requirements(Level::Majority, Timeout::Infinity()))));

    // M:6
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(
                      makeMutationResponse(6 /*seqno*/, vbid, value, opaque)));

    flushVBucketToDiskIfPersistent(vbid, 4);

    {
        CB_SCOPED_TRACE("");
        checkVBState(6 /*lastSnapStart*/,
                     6 /*lastSnapEnd*/,
                     CheckpointType::InitialDisk,
                     6 /*HPS*/,
                     2 /*HCS*/,
                     0 /*maxDelRevSeqno*/);
    }
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

void CDCActiveStreamTest::SetUp() {
    if (!config_string.empty()) {
        config_string += ";";
    }
    // Enable history retention
    config_string +=
            "history_retention_bytes=10485760;history_retention_seconds=3600";
    STActiveStreamPersistentTest::SetUp();

    // @todo CDC: Can remove as soon as magma enables history
    replaceMagmaKVStore();

    manifest.add(CollectionEntry::historical,
                 cb::NoExpiryLimit,
                 true /*history*/,
                 ScopeEntry::defaultS);

    setVBucketState(vbid, vbucket_state_active);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(
            std::shared_lock<folly::SharedMutex>(vb->getStateLock()),
            Collections::Manifest{std::string{manifest}});
    ASSERT_EQ(1, vb->getHighSeqno());

    ASSERT_TRUE(producer);
    ASSERT_FALSE(producer->areChangeStreamsEnabled());
    ASSERT_EQ(cb::engine_errc::success,
              producer->control(0, DcpControlKeys::ChangeStreams, "true"));
    ASSERT_TRUE(producer->areChangeStreamsEnabled());
    producer->public_enableSyncReplication();

    recreateStream(*vb,
                   true,
                   fmt::format(R"({{"collections":["{:x}"]}})",
                               uint32_t(CollectionEntry::historical.uid)));
    ASSERT_TRUE(stream);
    ASSERT_TRUE(stream->areChangeStreamsEnabled());

    // Control data: Add some mutations into the non-CDC collection
    for (size_t i : {1, 2, 3}) {
        store_item(vbid,
                   makeStoredDocKey("key" + std::to_string(i),
                                    CollectionEntry::defaultC),
                   "value");
    }

    ASSERT_EQ(4, vb->getHighSeqno());
}

void CDCActiveStreamTest::clearCMAndPersistenceAndReplication() {
    auto& vb = *store->getVBucket(vbid);
    auto& manager = *vb.checkpointManager;
    manager.createNewCheckpoint();

    // Move the persistence and stream cursor to the end of the open checkpoint
    const auto& readyQ = stream->public_readyQ();
    const auto cursor = stream->getCursor().lock();
    while (manager.getNumCheckpoints() > 1) {
        stream->nextCheckpointItemTask();
        while (!readyQ.empty()) {
            stream->public_nextQueuedItem(*producer);
        }
        flushVBucket(vbid);
    }

    // Eager checkpoint removal ensures 1 empty checkpoint at this point
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getNumOpenChkItems()); // cs
}

TEST_P(CDCActiveStreamTest, CollectionNotDeduped_InMemory) {
    auto& vb = *store->getVBucket(vbid);
    const auto initHighSeqno = vb.getHighSeqno();
    ASSERT_GT(initHighSeqno, 0); // From SetUp

    // Mutate the same key some times into the CDC collection
    const auto key = makeStoredDocKey("key", CollectionEntry::historical);
    for (size_t i : {1, 2, 3}) {
        store_item(vbid, key, "value" + std::to_string(i));
    }
    ASSERT_EQ(initHighSeqno + 3, vb.getHighSeqno());

    // Stream must send 3 snapshots, 1 per mutation

    const auto& readyQ = stream->public_readyQ();
    ASSERT_EQ(0, readyQ.size());

    stream->nextCheckpointItemTask();
    ASSERT_EQ(7, readyQ.size());

    // Marker + Sysevent + Mutation
    auto resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto* marker = dynamic_cast<SnapshotMarker*>(resp.get());
    const auto expectedMarkerFlags = DcpSnapshotMarkerFlag::Memory |
                                     DcpSnapshotMarkerFlag::Checkpoint |
                                     DcpSnapshotMarkerFlag::History;
    EXPECT_EQ(expectedMarkerFlags, marker->getFlags());
    EXPECT_EQ(0, marker->getStartSeqno());
    EXPECT_EQ(initHighSeqno + 1, marker->getEndSeqno());

    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SystemEvent, resp->getEvent());
    EXPECT_EQ(1, resp->getBySeqno());

    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(initHighSeqno + 1, resp->getBySeqno());
    auto* mut = dynamic_cast<const MutationResponse*>(resp.get());
    EXPECT_EQ(key, mut->getItem()->getKey());

    // Marker + Mutation
    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    marker = dynamic_cast<SnapshotMarker*>(resp.get());
    EXPECT_EQ(expectedMarkerFlags, marker->getFlags());
    EXPECT_EQ(initHighSeqno + 2, marker->getStartSeqno());
    EXPECT_EQ(initHighSeqno + 2, marker->getEndSeqno());

    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(initHighSeqno + 2, resp->getBySeqno());
    mut = dynamic_cast<const MutationResponse*>(resp.get());
    EXPECT_EQ(key, mut->getItem()->getKey());

    // Marker + Mutation
    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    marker = dynamic_cast<SnapshotMarker*>(resp.get());
    EXPECT_EQ(expectedMarkerFlags, marker->getFlags());
    EXPECT_EQ(initHighSeqno + 3, marker->getStartSeqno());
    EXPECT_EQ(initHighSeqno + 3, marker->getEndSeqno());

    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(initHighSeqno + 3, resp->getBySeqno());
    mut = dynamic_cast<const MutationResponse*>(resp.get());
    EXPECT_EQ(key, mut->getItem()->getKey());

    EXPECT_EQ(0, readyQ.size());
}

TEST_P(CDCActiveStreamTest, MarkerHistoryFlagClearIfCheckpointNotHistorical) {
    auto& config = engine->getConfiguration();
    config.setHistoryRetentionBytes(0);
    config.setHistoryRetentionSeconds(0);
    ASSERT_FALSE(store->isHistoryRetentionEnabled());

    auto& vb = *store->getVBucket(vbid);
    const auto initHighSeqno = vb.getHighSeqno();
    ASSERT_GT(initHighSeqno, 0); // From SetUp

    // Move the stream cursor to the end of checkpoint
    clearCMAndPersistenceAndReplication();

    auto& manager = *vb.checkpointManager;
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getNumOpenChkItems()); // cs
    // Main precondition
    ASSERT_EQ(CheckpointHistorical::No, manager.getOpenCheckpointHistorical());

    // Write a doc into the historical collection
    const auto key = makeStoredDocKey("key", CollectionEntry::historical);
    store_item(vbid, key, "value");
    ASSERT_EQ(initHighSeqno + 1, vb.getHighSeqno());

    // Check the outbound stream
    const auto& readyQ = stream->public_readyQ();
    ASSERT_EQ(0, readyQ.size());

    stream->nextCheckpointItemTask();
    ASSERT_EQ(2, readyQ.size()); // Marker + Mutation

    auto resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto* marker = dynamic_cast<SnapshotMarker*>(resp.get());
    // Core of the test: DcpSnapshotMarkerFlag::History isn't
    // set in the marker
    const auto expectedMarkerFlags =
            DcpSnapshotMarkerFlag::Memory | DcpSnapshotMarkerFlag::Checkpoint;
    EXPECT_EQ(expectedMarkerFlags, marker->getFlags());
    EXPECT_EQ(initHighSeqno + 1, marker->getStartSeqno());
    EXPECT_EQ(initHighSeqno + 1, marker->getEndSeqno());

    // Verify the rest of the stream for completeness
    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(initHighSeqno + 1, resp->getBySeqno());
    auto* mut = dynamic_cast<const MutationResponse*>(resp.get());
    EXPECT_EQ(key, mut->getItem()->getKey());

    EXPECT_EQ(0, readyQ.size());
}

void CDCActiveStreamTest::testResilientToRetentionConfigChanges(
        HistoryRetentionMetric metric) {
    // SetUp enables history (both bytes/seconds > 0) and creates an historical
    // collection.
    // Here we need to test bytes/seconds selectively for ensuring the correct
    // behaviour of each.
    auto& config = engine->getConfiguration();
    switch (metric) {
    case HistoryRetentionMetric::BYTES: {
        ASSERT_GT(store->getHistoryRetentionBytes(), 0);
        config.setHistoryRetentionSeconds(0);
        break;
    }
    case HistoryRetentionMetric::SECONDS: {
        ASSERT_GT(store->getHistoryRetentionSeconds().count(), 0);
        config.setHistoryRetentionBytes(0);
        break;
    }
    }
    ASSERT_TRUE(store->isHistoryRetentionEnabled());

    clearCMAndPersistenceAndReplication();

    // Now we write a doc into the historical collection
    auto& vb = *store->getVBucket(vbid);
    const auto initHighSeqno = vb.getHighSeqno();
    ASSERT_GT(initHighSeqno, 0); // From SetUp
    const auto key = makeStoredDocKey("key", CollectionEntry::historical);
    const auto value = "value";
    store_item(vbid, key, value);
    ASSERT_EQ(initHighSeqno + 1, vb.getHighSeqno());
    // 1 historical checkpoint contains the mutation
    const auto& manager = *vb.checkpointManager;
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(2, manager.getNumOpenChkItems()); // cs, m
    ASSERT_EQ(CheckpointHistorical::Yes, manager.getOpenCheckpointHistorical());
    // Save the current checkpoint id for later in the test
    const auto prevCkptId = manager.getOpenCheckpointId();
    // The the stream makes a History snapshot from that checkpoint
    const auto& readyQ = stream->public_readyQ();
    ASSERT_EQ(0, readyQ.size());
    stream->nextCheckpointItemTask();
    ASSERT_EQ(2, readyQ.size()); // Marker + Mutation
    // Marker
    auto resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto* marker = dynamic_cast<SnapshotMarker*>(resp.get());
    // Core test: DcpSnapshotMarkerFlag::History is set in
    // the marker
    EXPECT_EQ(DcpSnapshotMarkerFlag::Memory |
                      DcpSnapshotMarkerFlag::Checkpoint |
                      DcpSnapshotMarkerFlag::History,
              marker->getFlags());
    EXPECT_EQ(initHighSeqno + 1, marker->getStartSeqno());
    EXPECT_EQ(initHighSeqno + 1, marker->getEndSeqno());
    // Mutation
    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(initHighSeqno + 1, resp->getBySeqno());
    auto* mut = dynamic_cast<const MutationResponse*>(resp.get());
    EXPECT_EQ(key, mut->getItem()->getKey());
    // readyQ drained
    ASSERT_EQ(0, readyQ.size());

    // Move persistence to the end of checkpoint
    flushVBucket(vbid);

    // Now the user disables history retention
    switch (metric) {
    case HistoryRetentionMetric::BYTES: {
        config.setHistoryRetentionBytes(0);
        break;
    }
    case HistoryRetentionMetric::SECONDS: {
        config.setHistoryRetentionSeconds(0);
        break;
    }
    }
    ASSERT_FALSE(store->isHistoryRetentionEnabled());

    // Core test: The existing historical checkpoint is closed and a new open
    // non-historical checkpoint must be created
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_GT(manager.getOpenCheckpointId(), prevCkptId);
    ASSERT_EQ(1, manager.getNumOpenChkItems());
    ASSERT_EQ(CheckpointHistorical::No, manager.getOpenCheckpointHistorical());

    // Now store another mutation into the historical collection
    store_item(vbid, key, value);

    // This time DCP produces a non-historical snapshot from the non-historical
    // checkpoint
    ASSERT_EQ(0, readyQ.size());
    stream->nextCheckpointItemTask();
    ASSERT_EQ(2, readyQ.size()); // Marker + Mutation
    // Marker
    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    marker = dynamic_cast<SnapshotMarker*>(resp.get());
    // Core test: DcpSnapshotMarkerFlag::History isn't set in
    // the marker
    EXPECT_EQ(DcpSnapshotMarkerFlag::Memory | DcpSnapshotMarkerFlag::Checkpoint,
              marker->getFlags());
    EXPECT_EQ(initHighSeqno + 2, marker->getStartSeqno());
    EXPECT_EQ(initHighSeqno + 2, marker->getEndSeqno());
    // Mutation
    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(initHighSeqno + 2, resp->getBySeqno());
    mut = dynamic_cast<const MutationResponse*>(resp.get());
    EXPECT_EQ(key, mut->getItem()->getKey());
    // readyQ drained
    ASSERT_EQ(0, readyQ.size());
}

TEST_P(CDCActiveStreamTest, ResilientToRetentionConfigChanges_Bytes) {
    testResilientToRetentionConfigChanges(HistoryRetentionMetric::BYTES);
}

TEST_P(CDCActiveStreamTest, ResilientToRetentionConfigChanges_Seconds) {
    testResilientToRetentionConfigChanges(HistoryRetentionMetric::SECONDS);
}

TEST_P(CDCActiveStreamTest, SnapshotAndSeqnoAdvanceCorrectHistoryFlag) {
    ASSERT_TRUE(store->isHistoryRetentionEnabled());

    // The SetUp step creates the historical collection and stores a bunch of
    // items into the default collection. In this particular test we just need
    // to start from a clean CM and stream.
    manifest.remove(CollectionEntry::historical);
    auto& vb = *store->getVBucket(vbid);
    vb.updateFromManifest(
            std::shared_lock<folly::SharedMutex>(vb.getStateLock()),
            Collections::Manifest{std::string{manifest}});
    clearCMAndPersistenceAndReplication();

    producer->closeStream(0, vbid, stream->getStreamId());

    // Open a stream directly in a in-memory state
    const auto highSeqno = vb.getHighSeqno();
    stream = producer->addMockActiveStream(
            {} /*flags*/,
            0 /*opaque*/,
            vb,
            highSeqno /*start_seqno*/,
            ~0ULL /*end_seqno*/,
            0x0 /*vb_uuid*/,
            highSeqno /*snap_start_seqno*/,
            ~0ULL /*snap_end_seqno*/,
            producer->public_getIncludeValue(),
            producer->public_getIncludeXattrs(),
            producer->public_getIncludeDeletedUserXattrs(),
            std::string_view{} /*jsonFilter*/);
    ASSERT_TRUE(stream->isInMemory());

    // FlatBuffers disabled makes the test flow in the path that is under
    // verification in this test. Ie, at some point a SysEvent(modify) is
    // generated, but the stream receives a SeqnoAdvance in place of the
    // SysEvent.
    ASSERT_TRUE(stream->areChangeStreamsEnabled());
    ASSERT_FALSE(stream->isFlatBuffersSystemEventEnabled());

    // Add a collection
    manifest.add(CollectionEntry::historical,
                 cb::NoExpiryLimit,
                 true /*history*/,
                 ScopeEntry::defaultS);
    vb.updateFromManifest(
            std::shared_lock<folly::SharedMutex>(vb.getStateLock()),
            Collections::Manifest{std::string{manifest}});

    const auto& readyQ = stream->public_readyQ();
    ASSERT_EQ(0, readyQ.size());
    stream->nextCheckpointItemTask();
    ASSERT_EQ(2, readyQ.size()); // SnapshotMarker + SysEvent

    auto resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto* marker = dynamic_cast<SnapshotMarker*>(resp.get());
    // DcpSnapshotMarkerFlag::History is set in the marker
    EXPECT_EQ(DcpSnapshotMarkerFlag::Memory |
                      DcpSnapshotMarkerFlag::Checkpoint |
                      DcpSnapshotMarkerFlag::History,
              marker->getFlags());
    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SystemEvent, resp->getEvent());
    EXPECT_EQ(vb.getHighSeqno(), resp->getBySeqno());

    EXPECT_EQ(0, readyQ.size());

    // Modify the collection
    manifest.update(CollectionEntry::historical,
                    cb::NoExpiryLimit,
                    {} /*history*/,
                    ScopeEntry::defaultS);
    vb.updateFromManifest(
            std::shared_lock<folly::SharedMutex>(vb.getStateLock()),
            Collections::Manifest{std::string{manifest}});

    stream->nextCheckpointItemTask();
    ASSERT_EQ(2, readyQ.size()); // SnapshotMarker + SeqnoAdvance

    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    marker = dynamic_cast<SnapshotMarker*>(resp.get());
    // Core of the test: DcpSnapshotMarkerFlag::History is
    // set in the marker
    EXPECT_EQ(DcpSnapshotMarkerFlag::Memory | DcpSnapshotMarkerFlag::History,
              marker->getFlags());
    // Verify the string representation of marker's flags
    EXPECT_EQ(R"(["Memory","History"])", format_as(marker->getFlags()));
    // SeqnoAdvance in place of SysEvent(modify), as flatbuffers sys-events are
    // disabled
    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SeqnoAdvanced, resp->getEvent());
    EXPECT_EQ(vb.getHighSeqno(), resp->getBySeqno());

    EXPECT_EQ(0, readyQ.size());
}

TEST_P(CDCActiveStreamTest, DeduplicationDisabledForAbort) {
    ASSERT_TRUE(store->isHistoryRetentionEnabled());
    ASSERT_TRUE(stream->public_supportSyncReplication());

    setVBucketState(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *store->getVBucket(vbid);
    const auto initHighSeqno = vb.getHighSeqno();
    ASSERT_GT(initHighSeqno, 0); // From SetUp

    clearCMAndPersistenceAndReplication();
    // Note: stream filters on historical collection, so only the CreateColl
    // sysevent has been processed so far. See SetUp for details.
    ASSERT_EQ(1, stream->getLastSentSeqno());

    // Sequence of pre, abr, pre, cmt for the same key
    const auto key = makeStoredDocKey("key", CollectionEntry::historical);

    // PRE
    using namespace cb::durability;
    const auto reqs = Requirements{Level::Majority, Timeout()};
    const auto pre1 = store_item(vbid,
                                 key,
                                 "value_p1",
                                 0,
                                 {cb::engine_errc::sync_write_pending},
                                 PROTOCOL_BINARY_RAW_BYTES,
                                 reqs);
    EXPECT_EQ(initHighSeqno + 1, vb.getHighSeqno());

    // ABORT
    {
        std::shared_lock rlh(vb.getStateLock());
        ASSERT_EQ(cb::engine_errc::success,
                  vb.abort(rlh,
                           pre1.getKey(),
                           pre1.getBySeqno(),
                           {},
                           vb.lockCollections(pre1.getKey()),
                           nullptr));
    }
    EXPECT_EQ(initHighSeqno + 2, vb.getHighSeqno());

    // PRE
    const auto pre2 = store_item(vbid,
                                 key,
                                 "value_p2",
                                 0,
                                 {cb::engine_errc::sync_write_pending},
                                 PROTOCOL_BINARY_RAW_BYTES,
                                 reqs);
    EXPECT_EQ(initHighSeqno + 3, vb.getHighSeqno());

    // COMMIT
    {
        std::shared_lock rlh(vb.getStateLock());
        ASSERT_EQ(cb::engine_errc::success,
                  vb.commit(rlh,
                            pre2.getKey(),
                            pre2.getBySeqno(),
                            {},
                            CommitType::Majority,
                            vb.lockCollections(pre2.getKey()),
                            nullptr));
    }
    EXPECT_EQ(initHighSeqno + 4, vb.getHighSeqno());

    // Note: First step where checks begin to fail before the fix for MB-55919.
    // Before the fix, only 3 items are persisted as the Abort is wrongly
    // deduplicated.
    flush_vbucket_to_disk(vbid, 4);

    // Force stream to backfilling from disk
    stream->handleSlowStream();
    GMockDcpMsgProducers producers;
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(false, producers));
    EXPECT_TRUE(stream->isBackfilling());

    // Note this test was added for MB-55919, but has been modified to account
    // for changes made by /MB-56654, there is still value in this test so it
    // remains but with modified expectations.
    //
    // Must see 1 historical snapshot with [abr, cmt]
    // Before the fix for MB-55919 we get only [pre, pre, cmt].
    const auto& readyQ = stream->public_readyQ();
    EXPECT_EQ(0, readyQ.size());
    runBackfill();
    EXPECT_EQ(3, readyQ.size());

    auto resp = stream->public_nextQueuedItem(*producer);
    EXPECT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto* marker = dynamic_cast<SnapshotMarker*>(resp.get());
    const auto expectedMarkerFlags =
            DcpSnapshotMarkerFlag::Disk | DcpSnapshotMarkerFlag::Checkpoint |
            DcpSnapshotMarkerFlag::History |
            DcpSnapshotMarkerFlag::MayContainDuplicates;
    EXPECT_EQ(expectedMarkerFlags, marker->getFlags());
    EXPECT_EQ(initHighSeqno + 1, marker->getStartSeqno());
    EXPECT_EQ(initHighSeqno + 4, marker->getEndSeqno());

    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Abort, resp->getEvent());
    EXPECT_EQ(initHighSeqno + 2, resp->getBySeqno());

    resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(initHighSeqno + 4, resp->getBySeqno());
}

INSTANTIATE_TEST_SUITE_P(Persistent,
                         CDCActiveStreamTest,
                         STParameterizedBucketTest::magmaConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

void CDCPassiveStreamTest::SetUp() {
    if (!config_string.empty()) {
        config_string += ";";
    }
    // Enable history retention
    config_string += "history_retention_bytes=10485760";

    STPassiveStreamPersistentTest::SetUp();
}

void CDCPassiveStreamTest::createHistoricalCollection(CheckpointType snapType,
                                                      uint64_t snapStart,
                                                      uint64_t snapEnd) {
    const auto snapSource = snapType == CheckpointType::Memory
                                    ? DcpSnapshotMarkerFlag::Memory
                                    : DcpSnapshotMarkerFlag::Disk;

    const uint32_t opaque = 1;
    ASSERT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(
                      opaque,
                      vbid,
                      snapStart,
                      snapEnd,
                      snapSource | DcpSnapshotMarkerFlag::Checkpoint |
                              DcpSnapshotMarkerFlag::History,
                      0,
                      {},
                      {},
                      {}));

    const auto& vb = *store->getVBucket(vbid);
    auto& manager = *vb.checkpointManager;
    ASSERT_EQ(CheckpointHistorical::Yes, manager.getOpenCheckpointHistorical());
    switch (snapType) {
    case CheckpointType::Memory:
        ASSERT_EQ(snapType, manager.getOpenCheckpointType());
        break;
    case CheckpointType::Disk:
    case CheckpointType::InitialDisk:
        ASSERT_EQ((vb.getHighSeqno() == 0 ? CheckpointType::InitialDisk
                                          : CheckpointType::Disk),
                  manager.getOpenCheckpointType());
        break;
    }

    flatbuffers::FlatBufferBuilder fb;
    Collections::ManifestUid manifestUid;
    const auto collection = CollectionEntry::historical;
    auto fbPayload = Collections::VB::CreateCollection(
            fb,
            manifestUid,
            uint32_t(ScopeEntry::defaultS.getId()),
            uint32_t(collection.getId()),
            false,
            0,
            fb.CreateString(collection.name.data(), collection.name.size()),
            true /*history*/);
    fb.Finish(fbPayload);
    ASSERT_EQ(cb::engine_errc::success,
              consumer->systemEvent(
                      1 /*opaque*/,
                      vbid,
                      mcbp::systemevent::id::BeginCollection,
                      snapStart,
                      mcbp::systemevent::version::version2,
                      {reinterpret_cast<const uint8_t*>(collection.name.data()),
                       collection.name.size()},
                      {fb.GetBufferPointer(), fb.GetSize()}));

    ASSERT_EQ(0, vb.getNumTotalItems());
    ASSERT_EQ(snapStart, vb.getHighSeqno());
    ASSERT_EQ(2, manager.getNumOpenChkItems());
}

/*
 * HistorySnapshotReceived tests verify (on different scenarios) that:
 * - replica stream is resilient to duplicates on disk snapshot
 * - duplicates are queued in checkpoint (ie, not deduplicated)
 * - duplicates are persisted on disk (again, not deduplicated)
 * - stats (eg item-count) are updated correctly
 */

TEST_P(CDCPassiveStreamTest, HistorySnapshotReceived_Disk) {
    // Replica receives Snap{start, end, Disk|History}, with start->end
    // mutations for the same key.

    createHistoricalCollection(CheckpointType::Disk, 1, 1);
    flush_vbucket_to_disk(vbid, 1);

    // Clear CM
    const auto& vb = *store->getVBucket(vbid);
    const auto initialHighSeqno = vb.getHighSeqno();
    ASSERT_EQ(1, initialHighSeqno);
    auto& manager = *vb.checkpointManager;
    manager.createNewCheckpoint();
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getNumOpenChkItems());

    const uint32_t opaque = 1;
    SnapshotMarker snapshotMarker(opaque,
                                  vbid,
                                  initialHighSeqno + 1 /*start*/,
                                  initialHighSeqno + 3 /*end*/,
                                  DcpSnapshotMarkerFlag::Checkpoint |
                                          DcpSnapshotMarkerFlag::Disk |
                                          DcpSnapshotMarkerFlag::History,
                                  std::optional<uint64_t>(0), /*HCS*/
                                  {},
                                  {}, /*maxVisibleSeqno*/
                                  std::nullopt,
                                  {} /*streamId*/);
    stream->processMarker(&snapshotMarker);
    ASSERT_EQ(2, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getNumOpenChkItems());
    ASSERT_EQ(CheckpointType::Disk, manager.getOpenCheckpointType());
    ASSERT_EQ(CheckpointHistorical::Yes, manager.getOpenCheckpointHistorical());

    const auto collection = CollectionEntry::historical;
    const std::string key("key");
    const std::string value("value");
    const size_t numItems = 3;
    for (size_t seqno = initialHighSeqno + 1;
         seqno <= initialHighSeqno + numItems;
         ++seqno) {
        EXPECT_EQ(
                cb::engine_errc::success,
                stream->messageReceived(makeMutationResponse(
                        opaque, seqno, vbid, value, key, collection.getId())));
    }

    EXPECT_EQ(initialHighSeqno + numItems, vb.getHighSeqno());
    EXPECT_EQ(2, manager.getNumCheckpoints());
    EXPECT_EQ(1 + numItems, manager.getNumOpenChkItems());

    // All duplicates persisted
    flush_vbucket_to_disk(vbid, numItems);

    // Item count doesn't account for historical revisions
    EXPECT_EQ(1, vb.getNumTotalItems());
}

TEST_P(CDCPassiveStreamTest, HistorySnapshotReceived_InitialDisk) {
    // Replica receives Snap{start, end, InitialDisk|History}, with start->end
    // mutations for the same key.

    createHistoricalCollection(CheckpointType::Disk, 1, 10);
    flush_vbucket_to_disk(vbid, 1);

    const auto& vb = *store->getVBucket(vbid);
    const auto initialHighSeqno = vb.getHighSeqno();
    ASSERT_EQ(1, initialHighSeqno);
    auto& manager = *vb.checkpointManager;
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(2, manager.getNumOpenChkItems());

    // Historical items received within the same snapshot
    const auto collection = CollectionEntry::historical;
    const std::string key("key");
    const std::string value("value");
    const size_t numItems = 3;
    for (size_t seqno = initialHighSeqno + 1;
         seqno <= initialHighSeqno + numItems;
         ++seqno) {
        EXPECT_EQ(cb::engine_errc::success,
                  stream->messageReceived(
                          makeMutationResponse(1 /*opaque*/,
                                               seqno,
                                               vbid,
                                               value,
                                               key,
                                               collection.getId())));
    }

    // Important: In this scenario historical mutations are queued into the
    // Initial disk checkpoint
    ASSERT_EQ(CheckpointType::InitialDisk, manager.getOpenCheckpointType());
    ASSERT_EQ(CheckpointHistorical::Yes, manager.getOpenCheckpointHistorical());

    EXPECT_EQ(initialHighSeqno + numItems, vb.getHighSeqno());
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(1 + initialHighSeqno + numItems, manager.getNumOpenChkItems());

    // All duplicates persisted
    flush_vbucket_to_disk(vbid, numItems);

    // Item count doesn't account for historical revisions
    EXPECT_EQ(1, vb.getNumTotalItems());
}

TEST_P(CDCPassiveStreamTest, MemorySnapshotTransitionToHistory) {
    const auto& vb = *store->getVBucket(vbid);
    ASSERT_EQ(0, vb.getHighSeqno());
    auto& manager = *vb.checkpointManager;
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(2, manager.getNumOpenChkItems()); // cs + vbs
    ASSERT_EQ(CheckpointType::Memory, manager.getOpenCheckpointType());
    // Note: 7.2 vbucket sets itself to History mode when created.
    ASSERT_EQ(CheckpointHistorical::Yes, manager.getOpenCheckpointHistorical());

    // Replica receives a Snap{Memory}.
    const uint32_t opaque = 1;
    SnapshotMarker snapshotMarker(
            opaque,
            vbid,
            1 /*start*/,
            1 /*end*/,
            DcpSnapshotMarkerFlag::Checkpoint | DcpSnapshotMarkerFlag::Memory,
            std::optional<uint64_t>(0), /*HCS*/
            {},
            {}, /*maxVisibleSeqno*/
            std::nullopt,
            {} /*streamId*/);
    stream->processMarker(&snapshotMarker);
    ASSERT_EQ(cb::engine_errc::success,
              stream->messageReceived(
                      makeMutationResponse(opaque,
                                           1 /*seqno*/,
                                           vbid,
                                           "some-value",
                                           "some-key",
                                           CollectionEntry::defaultC.getId())));
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(2, manager.getNumOpenChkItems());
    ASSERT_EQ(CheckpointType::Memory, manager.getOpenCheckpointType());
    ASSERT_EQ(CheckpointHistorical::No, manager.getOpenCheckpointHistorical());
    ASSERT_EQ(1, vb.getHighSeqno());

    // Replica receives a Snap{Memory|History}.
    createHistoricalCollection(CheckpointType::Memory, 2, 10);
    ASSERT_EQ(2, manager.getNumCheckpoints());
    ASSERT_EQ(2, manager.getNumOpenChkItems()); // cs + sysevent
    ASSERT_EQ(CheckpointType::Memory, manager.getOpenCheckpointType());
    ASSERT_EQ(CheckpointHistorical::Yes, manager.getOpenCheckpointHistorical());
    ASSERT_EQ(2, vb.getHighSeqno()); // sysevent bumped the seqno

    // Historical items received within the same snapshot
    const auto collection = CollectionEntry::historical;
    EXPECT_EQ(
            cb::engine_errc::success,
            stream->messageReceived(makeMutationResponse(opaque,
                                                         3 /*seqno*/,
                                                         vbid,
                                                         "value",
                                                         "key",
                                                         collection.getId())));

    EXPECT_EQ(3, vb.getHighSeqno());
    EXPECT_EQ(3, manager.getNumOpenChkItems());
    EXPECT_EQ(2, manager.getNumCheckpoints());

    // Test: The flusher must process one checkpoint at a time, as we can't
    // merge checkpoints with different history configuration
    std::vector<queued_item> items;
    auto res =
            manager.getItemsForPersistence(items,
                                           std::numeric_limits<size_t>::max(),
                                           std::numeric_limits<size_t>::max());
    EXPECT_EQ(1, res.ranges.size());

    // Coverage for MB-55337 from now on.

    // Note: The previous step simulates the flusher in the middle of execution,
    // the backup cursor is still registered into the first (closed) checkpoint.
    EXPECT_EQ(CHECKPOINT_CLOSED,
              (*manager.getBackupPersistenceCursor()->getCheckpoint())
                      ->getState());
    EXPECT_EQ(2, manager.getNumCheckpoints());

    // Simulate a new DCP Producer connection
    const auto outboundDcpCursor =
            manager.registerCursorBySeqno(
                           "dcp-cursor", 0, CheckpointCursor::Droppable::Yes)
                    .takeCursor()
                    .lock();
    // Registered into the first/closed checkpoint
    EXPECT_EQ(CHECKPOINT_CLOSED,
              (*outboundDcpCursor->getCheckpoint())->getState());

    // Flusher completes and removes the backup cursor from the first
    res.flushHandle.reset();

    // State here is
    // [ .. ] [ .. )
    // ^      ^
    // dcp    persistence

    // Now move the dcp cursor.
    // Before the fix for MB-55337:
    //  1. The DCP cursor is moved to the open checkpoint
    //  2. The closed checkpoint becomes unreferenced and it's removed (detached
    //     to the CheckpointDestroyer.
    //  3. Our code tries to access the removed checkpoint by
    //     std::prev(CM::checkpointList::begin()), which is undefined behaviour.
    items.clear();
    manager.getItemsForCursor(*outboundDcpCursor, items, 1000, 1000);
}

TEST_P(CDCPassiveStreamTest, TouchedByExpelCheckpointNotReused) {
    auto& vb = *store->getVBucket(vbid);
    ASSERT_EQ(0, vb.getHighSeqno());
    auto& manager = static_cast<MockCheckpointManager&>(*vb.checkpointManager);
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(2, manager.getNumOpenChkItems()); // cs, vbs
    const auto& list = manager.getCheckpointList();
    ASSERT_FALSE(list.back()->modifiedByExpel());

    createHistoricalCollection(CheckpointType::Memory, 1, 1);
    ASSERT_EQ(2, manager.getNumOpenChkItems()); // cs, sys
    ASSERT_EQ(1, vb.getHighSeqno());
    removeCheckpoint(vb);
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getNumOpenChkItems()); // cs

    const auto initialId = manager.getOpenCheckpointId();

    const uint64_t opaque = 1;
    EXPECT_EQ(
            cb::engine_errc::success,
            consumer->snapshotMarker(opaque,
                                     vbid,
                                     2, // start
                                     2, // end
                                     DcpSnapshotMarkerFlag::Memory |
                                             DcpSnapshotMarkerFlag::Checkpoint |
                                             DcpSnapshotMarkerFlag::History,
                                     {},
                                     {},
                                     {},
                                     {}));
    // We never reuse a checkpoint.
    // Note that at this point the open checkpoint is empty as it stores only
    // the checkpoint_start meta-item
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(initialId + 1, manager.getOpenCheckpointId());

    const auto key = makeStoredDocKey("key", CollectionEntry::historical);
    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(opaque,
                                 key,
                                 {},
                                 0,
                                 0,
                                 vbid,
                                 0,
                                 2, // seqno
                                 0,
                                 0,
                                 0,
                                 0));

    EXPECT_EQ(2, manager.getNumOpenChkItems()); // cs, mut

    // Move cursors to allow Expel
    if (isPersistent()) {
        flushVBucket(vbid);
    }

    // Now Expel everything from the open checkpoint
    {
        const auto res = manager.expelUnreferencedCheckpointItems();
        ASSERT_EQ(1, res.count); // mut
        ASSERT_TRUE(list.back()->modifiedByExpel());
    }

    EXPECT_EQ(
            cb::engine_errc::success,
            consumer->snapshotMarker(opaque,
                                     vbid,
                                     3, // start
                                     3, // end
                                     DcpSnapshotMarkerFlag::Memory |
                                             DcpSnapshotMarkerFlag::Checkpoint |
                                             DcpSnapshotMarkerFlag::History,
                                     {},
                                     {},
                                     {},
                                     {}));
    // Again, we can never reuse a checkpoint. That applies to touched-by-expel
    // checkpoints too.
    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(initialId + 2, manager.getOpenCheckpointId());

    // Note: This was a fix in the Neo branch. At the time of merging, Trinity
    // already resilient to this.
    //
    // Before the fix this step throws an exception:
    //
    // libc++abi: terminating due to uncaught exception of type
    // std::logic_error: CheckpointManager::queueDirty: Got
    // status:failure:duplicate item when vb:0 is non-active:2,
    // item:[op:mutation, seqno:3, key:<ud>cid:0xf:key</ud>], lastBySeqno:2,
    // openCkpt:[start:3, end:3]
    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(opaque,
                                 key,
                                 {},
                                 0,
                                 0,
                                 vbid,
                                 0,
                                 3, // seqno
                                 0,
                                 0,
                                 0,
                                 0));
}

INSTANTIATE_TEST_SUITE_P(Persistent,
                         CDCPassiveStreamTest,
                         STParameterizedBucketTest::magmaConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

#endif /*EP_USE_MAGMA*/

void SingleThreadedPassiveStreamTest::testProcessMessageBypassMemCheck(
        DcpResponse::Event event, bool hasValue, OOMLevel oomLevel) {
    auto& vb = *store->getVBucket(vbid);
    ASSERT_EQ(0, vb.getHighSeqno());
    auto& manager = *vb.checkpointManager;
    removeCheckpoint(vb);
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getNumOpenChkItems()); // cs

    // Force OOM
    switch (oomLevel) {
    case OOMLevel::Bucket:
        engine->getConfiguration().setMutationMemRatio(0);
        break;
    case OOMLevel::Checkpoint:
        engine->getConfiguration().setCheckpointMemoryRatio(0);
        break;
    }

    const uint32_t opaque = 1;
    const size_t seqno = 1;
    SnapshotMarker snapshotMarker(opaque,
                                  vbid,
                                  seqno,
                                  seqno,
                                  DcpSnapshotMarkerFlag::Memory,
                                  std::optional<uint64_t>(0),
                                  {},
                                  {},
                                  std::nullopt,
                                  {});
    stream->processMarker(&snapshotMarker);

    const std::string key = "key";
    const auto value = hasValue ? std::string(1_MiB, 'v') : std::string();
    size_t messageBytes = key.size() + value.size();

    using namespace cb::durability;
    std::optional<Requirements> reqs;
    std::optional<DeleteSource> deletion;
    switch (event) {
    case DcpResponse::Event::Mutation:
        messageBytes += MutationResponse::mutationBaseMsgBytes;
        break;
    case DcpResponse::Event::Prepare: {
        reqs = Requirements(Level::Majority, Timeout::Infinity());
        messageBytes += MutationResponse::prepareBaseMsgBytes;
        break;
    }
    case DcpResponse::Event::Deletion: {
        deletion = DeleteSource::Explicit;
        messageBytes += MutationResponse::deletionBaseMsgBytes;
        break;
    }
    case DcpResponse::Event::Expiration: {
        deletion = DeleteSource::TTL;
        messageBytes += MutationResponse::expirationBaseMsgBytes;
        break;
    }
    default:
        GTEST_FAIL();
    }

    ASSERT_EQ(0, stream->getUnackedBytes());

    auto message =
            makeMutationResponse(1, vbid, value, opaque, key, reqs, deletion);
    const auto res = stream->messageReceived(std::move(message));

    EXPECT_EQ(cb::engine_errc::temporary_failure, res);
    EXPECT_EQ(vb.getHighSeqno(), 1);
    EXPECT_EQ(messageBytes, stream->getUnackedBytes());

    // Still OOM, DcpConsumerTask can't process unacked bytes
    uint32_t processedBytes = 0;
    EXPECT_EQ(more_to_process, stream->processUnackedBytes(processedBytes));
    EXPECT_EQ(0, processedBytes);
    EXPECT_EQ(messageBytes, stream->getUnackedBytes());

    // System recovers from OOM
    switch (oomLevel) {
    case OOMLevel::Bucket:
        engine->getConfiguration().setMutationMemRatio(1);
        break;
    case OOMLevel::Checkpoint:
        engine->getConfiguration().setCheckpointMemoryRatio(0.5);
        break;
    }
    EXPECT_EQ(all_processed, stream->processUnackedBytes(processedBytes));
    EXPECT_EQ(messageBytes, processedBytes);
    EXPECT_EQ(0, stream->getUnackedBytes());
}

TEST_P(SingleThreadedPassiveStreamTest, ProcessMessageBypassMemCheck_Mutation) {
    testProcessMessageBypassMemCheck(
            DcpResponse::Event::Mutation, true, OOMLevel::Bucket);
}

TEST_P(SingleThreadedPassiveStreamTest, ProcessMessageBypassMemCheck_Prepare) {
    testProcessMessageBypassMemCheck(
            DcpResponse::Event::Prepare, true, OOMLevel::Bucket);
}

TEST_P(SingleThreadedPassiveStreamTest,
       ProcessMessageBypassMemCheck_Deletion_WithValue) {
    testProcessMessageBypassMemCheck(
            DcpResponse::Event::Deletion, true, OOMLevel::Bucket);
}

TEST_P(SingleThreadedPassiveStreamTest,
       ProcessMessageBypassMemCheck_Deletion_NoValue) {
    testProcessMessageBypassMemCheck(
            DcpResponse::Event::Deletion, false, OOMLevel::Bucket);
}

TEST_P(SingleThreadedPassiveStreamTest,
       ProcessMessageBypassMemCheck_Expiration_WithValue) {
    testProcessMessageBypassMemCheck(
            DcpResponse::Event::Expiration, true, OOMLevel::Bucket);
}

TEST_P(SingleThreadedPassiveStreamTest,
       ProcessMessageBypassMemCheck_Expiration_NoValue) {
    testProcessMessageBypassMemCheck(
            DcpResponse::Event::Expiration, false, OOMLevel::Bucket);
}

TEST_P(SingleThreadedPassiveStreamTest,
       ProcessMessageBypassMemCheck_Mutation_CheckppintOOM) {
    testProcessMessageBypassMemCheck(
            DcpResponse::Event::Mutation, true, OOMLevel::Checkpoint);
}

TEST_P(SingleThreadedPassiveStreamTest,
       ProcessMessageBypassMemCheck_Prepare_CheckppintOOM) {
    testProcessMessageBypassMemCheck(
            DcpResponse::Event::Prepare, true, OOMLevel::Checkpoint);
}

TEST_P(SingleThreadedPassiveStreamTest,
       ProcessMessageBypassMemCheck_Deletion_WithValue_CheckppintOOM) {
    testProcessMessageBypassMemCheck(
            DcpResponse::Event::Deletion, true, OOMLevel::Checkpoint);
}

TEST_P(SingleThreadedPassiveStreamTest,
       ProcessMessageBypassMemCheck_Deletion_NoValue_CheckppintOOM) {
    testProcessMessageBypassMemCheck(
            DcpResponse::Event::Deletion, false, OOMLevel::Checkpoint);
}

TEST_P(SingleThreadedPassiveStreamTest,
       ProcessMessageBypassMemCheck_Expiration_WithValue_CheckppintOOM) {
    testProcessMessageBypassMemCheck(
            DcpResponse::Event::Expiration, true, OOMLevel::Checkpoint);
}

TEST_P(SingleThreadedPassiveStreamTest,
       ProcessMessageBypassMemCheck_Expiration_NoValue_CheckppintOOM) {
    testProcessMessageBypassMemCheck(
            DcpResponse::Event::Expiration, false, OOMLevel::Checkpoint);
}

TEST_P(SingleThreadedPassiveStreamTest, ProcessUnackedBytes_StreamEnd) {
    auto& vb = *store->getVBucket(vbid);
    ASSERT_EQ(0, vb.getHighSeqno());
    auto& manager = *vb.checkpointManager;
    removeCheckpoint(vb);
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getNumOpenChkItems()); // cs

    // Force OOM
    engine->getConfiguration().setMutationMemRatio(0);

    const uint32_t opaque = 1;
    const size_t seqno = 1;
    SnapshotMarker snapshotMarker(opaque,
                                  vbid,
                                  seqno,
                                  seqno,
                                  DcpSnapshotMarkerFlag::Memory,
                                  std::optional<uint64_t>(0),
                                  {},
                                  {},
                                  std::nullopt,
                                  {});
    stream->processMarker(&snapshotMarker);

    const std::string key = "key";
    const auto value = std::string(1_MiB, 'v');
    const auto messageBytes =
            MutationResponse::mutationBaseMsgBytes + key.size() + value.size();

    ASSERT_EQ(0, stream->getUnackedBytes());

    queued_item qi(makeCommittedItem(makeStoredDocKey(key), value));
    qi->setBySeqno(seqno);
    const auto docKey = qi->getDocKey();
    const auto res = consumer->public_processMutationOrPrepare(
            vbid, opaque, docKey, std::move(qi), messageBytes);

    // Note: PassiveStream returns temporary_failure but DcpConsumer turns it
    // into success.
    EXPECT_EQ(cb::engine_errc::success, res);
    EXPECT_EQ(vb.getHighSeqno(), 1);
    // Evidence of executing the OOM path is given by counters though
    EXPECT_EQ(messageBytes, stream->getUnackedBytes());

    auto& control = consumer->getFlowControl();
    ASSERT_EQ(0, control.getFreedBytes());

    // Still OOM, DcpConsumerTask can't process unacked bytes
    EXPECT_EQ(more_to_process, consumer->processUnackedBytes());
    EXPECT_EQ(0, control.getFreedBytes());
    EXPECT_EQ(messageBytes, stream->getUnackedBytes());

    std::function<void()> hook = [this, &control]() {
        // Close the stream. That removes the stream from the Consumer map.
        // Any unprocessed unacked bytes for that stream isn't added to
        // FlowControl counters yet
        consumer->closeStreamDueToVbStateChange(vbid, vbucket_state_active);
        EXPECT_EQ(0, control.getFreedBytes());
    };
    stream->setProcessUnackedBytes_TestHook(hook);

    // System recovers from OOM
    engine->getConfiguration().setMutationMemRatio(1);
    EXPECT_EQ(all_processed, consumer->processUnackedBytes());
    // The last call into processUnackedBytes() releases the last reference to
    // the stream, so the stream is destroyed.
    EXPECT_FALSE(consumer->public_findStream(vbid));
    // At stream destruction the pending unacked bytes are notified to the
    // Consumer's FlowControl.
    EXPECT_EQ(messageBytes, control.getFreedBytes());
}

TEST_P(SingleThreadedPassiveStreamTest, MB_63439) {
    auto& vb = *store->getVBucket(vbid);
    ASSERT_EQ(0, vb.getHighSeqno());
    auto& manager = *vb.checkpointManager;
    removeCheckpoint(vb);
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getNumOpenChkItems()); // cs

    // Force OOM
    engine->getConfiguration().setMutationMemRatio(0);

    const uint32_t opaque = 1;
    const size_t seqno = 1;
    SnapshotMarker snapshotMarker(opaque,
                                  vbid,
                                  seqno,
                                  seqno,
                                  DcpSnapshotMarkerFlag::Memory,
                                  std::optional<uint64_t>(0),
                                  {},
                                  {},
                                  {},
                                  {});
    stream->processMarker(&snapshotMarker);

    const std::string key = "key";
    const auto value = std::string(1_MiB, 'v');
    const auto messageBytes =
            MutationResponse::mutationBaseMsgBytes + key.size() + value.size();

    ASSERT_EQ(0, stream->getUnackedBytes());

    queued_item qi(makeCommittedItem(makeStoredDocKey(key), value));
    qi->setBySeqno(seqno);
    const auto docKey = qi->getDocKey();
    const auto res = consumer->public_processMutationOrPrepare(
            vbid, opaque, docKey, std::move(qi), messageBytes);

    // Note: PassiveStream returns temporary_failure but DcpConsumer turns it
    // into success.
    EXPECT_EQ(cb::engine_errc::success, res);
    EXPECT_EQ(vb.getHighSeqno(), 1);
    // Evidence of executing the OOM path is given by counters though
    EXPECT_EQ(messageBytes, stream->getUnackedBytes());

    auto& control = consumer->getFlowControl();
    ASSERT_EQ(0, control.getFreedBytes());

    // Still OOM, DcpConsumerTask can't process unacked bytes
    EXPECT_EQ(more_to_process, consumer->processUnackedBytes());
    EXPECT_EQ(0, control.getFreedBytes());
    EXPECT_EQ(messageBytes, stream->getUnackedBytes());

    // System recovers from OOM..
    engine->getConfiguration().setMutationMemRatio(1);
    // .. but re-enters a OOM phase during the unacked bytes processing
    std::function<void()> hook = [this]() {
        engine->getConfiguration().setMutationMemRatio(0);
    };
    stream->setProcessUnackedBytes_TestHook(hook);

    EXPECT_EQ(more_to_process, consumer->processUnackedBytes());
    EXPECT_EQ(messageBytes, stream->getUnackedBytes());
    EXPECT_EQ(0, control.getFreedBytes());

    // Finally the system recovers from OOM
    engine->getConfiguration().setMutationMemRatio(1);
    stream->setProcessUnackedBytes_TestHook({});

    // Before the fix for MB-63439, in the previous call into
    // processUnackedBytes() the Consumer has missed to add the vbid back into
    // the Consumer::bufferedVBQueue, so at the next call the Consumer doesn't
    // find any vb to process.
    EXPECT_EQ(all_processed, consumer->processUnackedBytes());
    EXPECT_EQ(0, stream->getUnackedBytes());
    EXPECT_EQ(messageBytes, control.getFreedBytes());
}

TEST_P(SingleThreadedPassiveStreamTest, MB_63611) {
    auto& vb0 = *store->getVBucket(vbid);
    ASSERT_EQ(0, vb0.getHighSeqno());
    auto& manager = *vb0.checkpointManager;
    removeCheckpoint(vb0);
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getNumOpenChkItems()); // cs

    // Test connection and stream already exist
    ASSERT_TRUE(consumer);
    ASSERT_TRUE(stream);
    // Setup a second stream under the same connection for a different vbucket
    const auto vbid1 = Vbid(1);
    setVBucketStateAndRunPersistTask(vbid1, vbucket_state_replica);
    ASSERT_EQ(cb::engine_errc::success, consumer->addStream(0, vbid1, {}));
    auto* stream1 = static_cast<MockPassiveStream*>(
            consumer->getVbucketStream(vbid1).get());
    ASSERT_TRUE(stream1->isActive());

    // Force OOM
    engine->getConfiguration().setMutationMemRatio(0);

    const size_t seqno = 1;
    SnapshotMarker snapshotMarker(0, // opaque
                                  vbid,
                                  seqno,
                                  seqno,
                                  DcpSnapshotMarkerFlag::Memory,
                                  std::optional<uint64_t>(0),
                                  {},
                                  {},
                                  {},
                                  {});
    stream->processMarker(&snapshotMarker);
    stream1->processMarker(&snapshotMarker);

    ASSERT_EQ(0, stream->getUnackedBytes());
    ASSERT_EQ(0, stream1->getUnackedBytes());

    const std::string key = "key";
    const auto value = std::string(1_MiB, 'v');
    const auto messageBytes =
            MutationResponse::mutationBaseMsgBytes + key.size() + value.size();

    for (const auto vb : {vbid, vbid1}) {
        queued_item qi(makeCommittedItem(makeStoredDocKey(key), value));
        qi->setBySeqno(seqno);
        qi->setVBucketId(vb);
        const auto docKey = qi->getDocKey();
        // Note: PassiveStream returns temporary_failure but DcpConsumer turns
        // it into success.
        EXPECT_EQ(cb::engine_errc::success,
                  consumer->public_processMutationOrPrepare(
                          vb,
                          (vb == Vbid(0) ? 1 : 2), // opaque
                          docKey,
                          std::move(qi),
                          messageBytes));
        // Note: At OOM we force items into checkpoints
        EXPECT_EQ(store->getVBucket(vb)->getHighSeqno(), 1);
    }
    // Evidence of executing the OOM path is given by counters though
    EXPECT_EQ(messageBytes, stream->getUnackedBytes());
    EXPECT_EQ(messageBytes, stream1->getUnackedBytes());

    auto& control = consumer->getFlowControl();
    ASSERT_EQ(0, control.getFreedBytes());

    // Still OOM, DcpConsumerTask can't process unacked bytes
    EXPECT_EQ(more_to_process, consumer->processUnackedBytes());
    EXPECT_EQ(messageBytes, stream->getUnackedBytes());
    EXPECT_EQ(messageBytes, stream1->getUnackedBytes());
    EXPECT_EQ(0, control.getFreedBytes());

    // The system recovers from OOM
    engine->getConfiguration().setMutationMemRatio(1);

    // Before the fix for MB-63611, the next call processes all unacked bytes
    // for one stream but returns all_processed, which put the DcpConsumerTask
    // to sleep
    EXPECT_EQ(more_to_process, consumer->processUnackedBytes());
    EXPECT_EQ(messageBytes, stream->getUnackedBytes());
    EXPECT_EQ(0, stream1->getUnackedBytes());
    EXPECT_EQ(messageBytes, control.getFreedBytes());

    // Extra paranoid check: The next DcpConsumerTask successfully processes
    // the other stream's bytes
    EXPECT_EQ(all_processed, consumer->processUnackedBytes());
    EXPECT_EQ(0, stream->getUnackedBytes());
    EXPECT_EQ(0, stream1->getUnackedBytes());
    EXPECT_EQ(messageBytes * 2, control.getFreedBytes());
}

TEST_P(SingleThreadedPassiveStreamTest, MB_64246) {
    auto& vb = *store->getVBucket(vbid);
    ASSERT_EQ(vbucket_state_replica, vb.getState());
    ASSERT_EQ(0, vb.getHighSeqno());
    auto& manager = *vb.checkpointManager;
    removeCheckpoint(vb);
    ASSERT_EQ(1, manager.getNumCheckpoints());
    ASSERT_EQ(1, manager.getNumOpenChkItems()); // cs
    // Save the initial open checkpointId for sanity checks
    const auto ckptId = manager.getOpenCheckpointId();

    ASSERT_TRUE(consumer);
    ASSERT_TRUE(stream);
    ASSERT_TRUE(stream->isActive());

    // Stream receives the full [1, 10] snapshot
    const size_t snapStart = 1;
    const size_t snapEnd = 10;
    SnapshotMarker snapshotMarker(0,
                                  vbid,
                                  snapStart,
                                  snapEnd,
                                  DcpSnapshotMarkerFlag::Memory,
                                  std::optional<uint64_t>(0),
                                  {},
                                  {},
                                  {},
                                  {});
    stream->processMarker(&snapshotMarker);

    const auto value = std::string("value");
    for (size_t seqno = snapStart; seqno <= snapEnd; ++seqno) {
        const auto key = "key" + std::to_string(seqno);
        const auto messageBytes = MutationResponse::mutationBaseMsgBytes +
                                  key.size() + value.size();
        queued_item qi(makeCommittedItem(makeStoredDocKey(key), value));
        qi->setBySeqno(seqno);
        qi->setVBucketId(vbid);
        const auto docKey = qi->getDocKey();
        EXPECT_EQ(cb::engine_errc::success,
                  consumer->public_processMutationOrPrepare(vbid,
                                                            1, // opaque
                                                            docKey,
                                                            std::move(qi),
                                                            messageBytes));
        EXPECT_EQ(seqno, vb.getHighSeqno());
    }
    ASSERT_EQ(10, vb.getHighSeqno());
    flush_vbucket_to_disk(vbid, 10);

    EXPECT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(ckptId + 1, manager.getOpenCheckpointId());
    EXPECT_EQ(11, manager.getNumOpenChkItems()); // cs + [1, 10] snapshot
    EXPECT_EQ(10, manager.getHighSeqno());

    auto checkpointSnap = manager.getSnapshotInfo();
    EXPECT_EQ(1, checkpointSnap.range.getStart());
    EXPECT_EQ(10, checkpointSnap.range.getEnd());
    EXPECT_EQ(10, checkpointSnap.start);

    // Recreate stream
    ASSERT_EQ(cb::engine_errc::success, consumer->closeStream(0, vbid));
    ASSERT_EQ(cb::engine_errc::success, consumer->addStream(0, vbid, {}));
    stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    ASSERT_TRUE(stream);

    EXPECT_EQ(10, stream->getSnapStartSeqno());
    EXPECT_EQ(10, stream->getSnapEndSeqno());
    EXPECT_EQ(10, stream->getStartSeqno());

    // Simulate vbstate changes and stream recreation as observed in MB-64246
    setVBucketState(vbid, vbucket_state_pending);
    setVBucketState(vbid, vbucket_state_active);
    // Note: Persisting for moving the cursor and allowing ItemExpel in the
    // next steps
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    // Vbstate bumps triggered new checkpoint creation (note: prev checkpoint
    // removed by cursor move)
    ASSERT_EQ(1, manager.getNumCheckpoints());
    EXPECT_EQ(ckptId + 2, manager.getOpenCheckpointId());
    // cs + vbs(a) + vbs(r)
    EXPECT_EQ(3, manager.getNumOpenChkItems());
    EXPECT_EQ(10, manager.getHighSeqno());
    checkpointSnap = manager.getSnapshotInfo();
    EXPECT_EQ(10, checkpointSnap.range.getStart());
    EXPECT_EQ(10, checkpointSnap.range.getEnd());
    EXPECT_EQ(10, checkpointSnap.start);

    // Item Expel is the precondition for triggering the broken code path at
    // CheckpointManager::getSnapshotInfo() which is used for making the
    // StreamRequest in DcpConsumer::addStream
    EXPECT_EQ(2, manager.expelUnreferencedCheckpointItems().count);

    // Before the fix getSnapshotInfo() would expose an invalid snapshot
    // (snapStart:11, snapEnd:11, start:10), so these need to NOT vary from
    // the previous values
    checkpointSnap = manager.getSnapshotInfo();
    EXPECT_EQ(10, checkpointSnap.range.getStart());
    EXPECT_EQ(10, checkpointSnap.range.getEnd());
    EXPECT_EQ(10, checkpointSnap.start);

    // Stream closes and reconnects
    ASSERT_EQ(cb::engine_errc::success, consumer->closeStream(0, vbid));
    ASSERT_EQ(cb::engine_errc::success, consumer->addStream(0, vbid, {}));
    stream = static_cast<MockPassiveStream*>(
            consumer->getVbucketStream(vbid).get());
    ASSERT_TRUE(stream->isActive());

    // Before the fix we would observe (snapStart:11, snapEnd:11, start:10)
    EXPECT_EQ(10, stream->getSnapStartSeqno());
    EXPECT_EQ(10, stream->getSnapEndSeqno());
    EXPECT_EQ(10, stream->getStartSeqno());

    // Verify snap seqnos in the StreamRequest queued in readyQ
    const auto& readyQ = stream->public_readyQ();
    ASSERT_EQ(1, readyQ.size());
    auto msg = stream->public_popFromReadyQ();
    ASSERT_TRUE(msg);
    ASSERT_EQ(DcpResponse::Event::StreamReq, msg->getEvent());
    auto& streamReq = dynamic_cast<StreamRequest&>(*msg);
    // Before the fix we would observe (snapStart:11, snapEnd:11, start:10)
    EXPECT_EQ(10, streamReq.getSnapStartSeqno());
    EXPECT_EQ(10, streamReq.getSnapEndSeqno());
    EXPECT_EQ(10, streamReq.getStartSeqno());
}

// MB-62847
TEST_P(STParameterizedBucketTest, EmptySnapshotMustNotTriggerSeqnoAdvance) {
    if (ephemeral()) {
        // Skip this test for ephemeral buckets as the vbucket state is not
        // created on ephemeral buckets.
        return;
    }
    // Begin with an active vbucket and do some work to ensure that DCP will
    // backfill when we start it.
    store->setVBucketState(vbid, vbucket_state_active);
    VBucketPtr vb = store->getVBucket(vbid);
    store_item(vbid, makeStoredDocKey("k1"), "v1");
    flushVBucketToDiskIfPersistent(vbid, 1);
    // Using expel to clear memory item to force backfill.
    vb->checkpointManager->expelUnreferencedCheckpointItems();

    // Critical step - ensure replica and ensure a set_vbucket_state meta-item
    // is in the checkpoint
    store->setVBucketState(vbid, vbucket_state_replica);

    ASSERT_EQ(1, vb->checkpointManager->getSnapshotInfo().range.getEnd());
    // Next extend the open-checkpoint and verify it is returned as the end of
    // the range. DCP backfill will Merge and return a range 0, 2
    vb->checkpointManager->extendOpenCheckpoint(2, 2);
    ASSERT_EQ(2, vb->checkpointManager->getSnapshotInfo().range.getEnd());

    // Now begin DCP. This is a bucket stream which does not enable sync-repl
    // this means it will enable SeqnoAdvance.
    auto cookie = create_mock_cookie(engine.get());
    auto producer = std::make_shared<MockDcpProducer>(
            *engine, cookie, "MB_62847", DcpOpenFlag::None);
    producer->setSyncReplication(SyncReplication::No);
    producer->setNoopEnabled(MockDcpProducer::NoopMode::EnabledButNeverSent);
    MockDcpMessageProducers producers;

    // Create with an empty config, which enables seqno-advance
    createDcpStream(*producer, vbid, std::string_view{});

    using namespace cb::mcbp;

    // Step DCP from disk, backfill runs and creates a merged snapshot 0:2
    notifyAndStepToCheckpoint(
            *producer, producers, ClientOpcode::DcpSnapshotMarker, false);
    EXPECT_EQ(0, producers.last_snap_start_seqno);
    EXPECT_EQ(2, producers.last_snap_end_seqno);
    // Drain readyQ which will schedule in-memory processing
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(producers, ClientOpcode::DcpMutation));

    // Step, with bug this processes the set_vbucket_state and incorrectly
    // triggered SeqnoAdvance. Without bug nothing happens
    notifyAndStepToCheckpoint(*producer, producers, ClientOpcode::Invalid);

    // Now along comes the actual seqno:2 mutation
    writeDocToReplica(vbid, makeStoredDocKey("k1"), 2, false);

    // With MB-62847 this next step throws Monotonic exception.
    notifyAndStepToCheckpoint(*producer, producers, ClientOpcode::DcpMutation);
    destroy_mock_cookie(cookie);
}

class SlowBackfillTest : public SingleThreadedActiveStreamTest {
protected:
    /// Sets up a stream in the backfilling state and sets the idle timeout such
    /// that the backfill is considered idle and can be closed with status Slow.
    void setupIdleBackfill(cb::mcbp::DcpAddStreamFlag flags = {});
    void validateStream();
};

void SlowBackfillTest::setupIdleBackfill(cb::mcbp::DcpAddStreamFlag flags) {
    auto vb = engine->getVBucket(vbid);
    // Remove the initial stream, we want to force it to backfill.
    stream.reset();
    store_item(vbid, makeStoredDocKey("k0"), "v");
    store_item(vbid, makeStoredDocKey("k1"), "v");
    // Ensure mutation is no longer present in CheckpointManager.
    vb->checkpointManager->createNewCheckpoint();
    flushVBucketToDiskIfPersistent(vbid, 2);
    producer->setBackfillBufferSize(1);
    // Force idle protection on for testing all bucket types
    engine->getConfiguration().setDcpBackfillIdleProtectionEnabled(true);
    engine->getConfiguration().setDcpBackfillIdleLimitSeconds(0);
    engine->getConfiguration().setDcpBackfillIdleDiskThreshold(0.0);

    recreateStream(*vb, false, {}, flags);
    ASSERT_TRUE(stream->isBackfilling());

    auto& bfm = producer->getBFM();
    // first run will introduce the pause, the buffer is now full
    EXPECT_FALSE(producer->getBackfillBufferFullStatus());
    bfm.backfill();
    EXPECT_TRUE(producer->getBackfillBufferFullStatus());

    // Next attempt will enter shouldCancel checking because the buffer is
    // full. This first attempt will setup Position tracking.
    EXPECT_EQ(backfill_snooze, bfm.backfill());
    EXPECT_EQ(1, bfm.getNumBackfills());
}

void SlowBackfillTest::validateStream() {
    ASSERT_EQ(2, stream->public_readyQSize());
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker,
              stream->public_nextQueuedItem(*producer)->getEvent());
    EXPECT_EQ(DcpResponse::Event::Mutation,
              stream->public_nextQueuedItem(*producer)->getEvent());
}

TEST_P(SlowBackfillTest, BackfillCompletesWithProgress) {
    setupIdleBackfill();
    // This test skip validateStream because it wants to drain the stream via
    // DcpProducer::step which will free up buffer space allowing the scan to
    // progress.

    MockDcpMessageProducers producers;
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(
                      producers, cb::mcbp::ClientOpcode::DcpSnapshotMarker));
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(producers,
                                      cb::mcbp::ClientOpcode::DcpMutation));
    EXPECT_EQ("k0", producers.last_key);

    // producer drained
    EXPECT_FALSE(producer->getBackfillBufferFullStatus());

    auto& bfm = producer->getBFM();

    // Now step backfill, it can load k1 and returns yield (puts backfill on
    // snooze list)
    EXPECT_EQ(backfill_success, bfm.backfill());
    EXPECT_TRUE(producer->getBackfillBufferFullStatus());

    // Stepping backfill whilst buffer full and when on snooze list result in a
    // call to shouldCancel - which will return false because progress was made
    EXPECT_EQ(backfill_snooze, bfm.backfill());

    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(producers,
                                      cb::mcbp::ClientOpcode::DcpMutation));
    EXPECT_FALSE(producer->getBackfillBufferFullStatus());

    EXPECT_EQ("k1", producers.last_key);

    EXPECT_EQ(backfill_success, bfm.backfill());
    EXPECT_EQ(0, bfm.getNumBackfills());
    EXPECT_EQ(backfill_finished, bfm.backfill());

    // Stream is set to dead, so has all messages cleared and then 1 StreamEnd
    // will be queued
    ASSERT_FALSE(stream->isDead());
}

// Test related to MB-62703, check stream ends and backfill cancels if no
// progress can be made.
TEST_P(SlowBackfillTest, BackfillCancelsWhenNoProgress) {
    setupIdleBackfill();
    validateStream();

    // Run compaction so disk space to free is not 0
    runCompaction(vbid);

    auto& bfm = producer->getBFM();
    EXPECT_EQ(1, bfm.getNumBackfills());

    // Next attempt will see no change in Position within the time (0s).
    // backfill cancels and stream ends.
    EXPECT_EQ(backfill_success, bfm.backfill());
    EXPECT_EQ(0, bfm.getNumBackfills());
    // Stream is set to dead, so has all messages cleared and then 1 StreamEnd
    // will be queued
    ASSERT_TRUE(stream->isDead());
    ASSERT_EQ(1, stream->public_readyQSize());
    auto resp = stream->public_nextQueuedItem(*producer);
    ASSERT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::StreamEnd, resp->getEvent());
    auto& endStream = dynamic_cast<StreamEndResponse&>(*resp);
    EXPECT_EQ(cb::mcbp::DcpStreamEndStatus::Slow, endStream.getFlags());
}

// Test related to MB-62703, check takeover streams cannot be ended even when
// slow accoriding to the idle timeout.
TEST_P(SlowBackfillTest, TakeoverBackfillDoesNotCancelWhenNoProgress) {
    setupIdleBackfill(cb::mcbp::DcpAddStreamFlag::TakeOver);
    validateStream();

    auto& bfm = producer->getBFM();
    // Next attempt process one item and snooze. It should not be cancelled.
    EXPECT_EQ(backfill_snooze, bfm.backfill());
    EXPECT_EQ(1, bfm.getNumBackfills());
    ASSERT_FALSE(stream->isDead())
            << "Expected stream to still be alive, but it was marked dead";
}

INSTANTIATE_TEST_SUITE_P(AllBucketTypes,
                         SlowBackfillTest,
                         STParameterizedBucketTest::allConfigValuesNoNexus(),
                         STParameterizedBucketTest::PrintToStringParamName);

TEST_P(SingleThreadedPassiveStreamTest,
       SkipBloomFilterWhenProcessingDcpMutation) {
    auto& vb = *store->getVBucket(vbid);
    ASSERT_EQ(0, vb.getHighSeqno());

    const uint64_t opaque = 1;
    EXPECT_EQ(
            cb::engine_errc::success,
            consumer->snapshotMarker(opaque,
                                     vbid,
                                     1, // start
                                     2, // end
                                     DcpSnapshotMarkerFlag::Memory |
                                             DcpSnapshotMarkerFlag::Checkpoint,
                                     {},
                                     {},
                                     {},
                                     {}));

    const auto key = makeStoredDocKey("key");
    // Set the expectation keyMayExist is never called for Magma.
    using namespace ::testing;
    auto& mockKVStore = MockKVStore::replaceRWKVStoreWithMock(*store, 0);
    EXPECT_CALL(mockKVStore, keyMayExist(_, _)).Times(0);

    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(opaque,
                                 key,
                                 {},
                                 0,
                                 0,
                                 vbid,
                                 0,
                                 1, // seqno
                                 0,
                                 0,
                                 0,
                                 0));
    EXPECT_EQ(1, vb.getHighSeqno());
}
class TestStuckProducer : public SingleThreadedActiveStreamTest {
public:
    void SetUp() override {
        SingleThreadedActiveStreamTest::SetUp();
        // Set the timeout to 0 and regex to only match a specific name
        mock_set_dcp_disconnect_when_stuck_timeout(std::chrono::seconds{0});
        mock_set_dcp_disconnect_when_stuck_name_regex(".*:disconnect-me:.*");

        store_item(vbid, makeStoredDocKey("keyA"), "value");
        store_item(vbid, makeStoredDocKey("keyB"), "value");
    }
};

TEST_P(TestStuckProducer, producerDisconnected) {
    auto& vb = *store->getVBucket(vbid);
    // The name of this producer will match the configured regex.
    auto producer =
            std::make_shared<MockDcpProducer>(*engine,
                                              cookie,
                                              "dcp:disconnect-me:p->c",
                                              cb::mcbp::DcpOpenFlag::None,
                                              false);
    producer->createCheckpointProcessorTask();
    producer->scheduleCheckpointProcessorTask();

    // Set a small connection buffer size to force the producer to pause
    // The snapshot and first mutation will fill the buffer, the 2nd mutation
    // (final step) will then trigger the producer to disconnect when the flow
    // control is seen to be in the paused state with no change for 0 seconds
    EXPECT_EQ(
            cb::engine_errc::success,
            producer->control(
                    0 /*opaque*/, DcpControlKeys::ConnectionBufferSize, "100"));

    producer->addMockActiveStream(cb::mcbp::DcpAddStreamFlag::None,
                                  0,
                                  vb,
                                  0,
                                  ~0ULL,
                                  0x0,
                                  0,
                                  ~0ULL)
            ->setActive();

    MockDcpMessageProducers producers;
    notifyAndRunToCheckpoint(*producer, producers);
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(
                      producers, cb::mcbp::ClientOpcode::DcpSnapshotMarker));
    EXPECT_EQ(0, producers.last_snap_start_seqno);
    EXPECT_EQ(2, producers.last_snap_end_seqno);

    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(producers,
                                      cb::mcbp::ClientOpcode::DcpMutation));
    EXPECT_EQ(1, producers.last_byseqno);
    EXPECT_EQ("keyA", producers.last_key);

    EXPECT_EQ(cb::engine_errc::disconnect, producer->step(false, producers));
}

TEST_P(TestStuckProducer, producerNotDisconnected) {
    auto& vb = *store->getVBucket(vbid);
    // The name of this producer will not match the configured regex.
    auto producer =
            std::make_shared<MockDcpProducer>(*engine,
                                              cookie,
                                              "dcp:connected:p->c",
                                              cb::mcbp::DcpOpenFlag::None,
                                              false);
    producer->createCheckpointProcessorTask();
    producer->scheduleCheckpointProcessorTask();

    // Set a small connection buffer size to force the producer to pause
    // The snapshot and first mutation will fill the buffer, the 2nd mutation
    // (final step) will then trigger the producer to disconnect when the flow
    // control is seen to be in the paused state with no change for 0 seconds
    EXPECT_EQ(
            cb::engine_errc::success,
            producer->control(
                    0 /*opaque*/, DcpControlKeys::ConnectionBufferSize, "100"));

    producer->addMockActiveStream(cb::mcbp::DcpAddStreamFlag::None,
                                  0,
                                  vb,
                                  0,
                                  ~0ULL,
                                  0x0,
                                  0,
                                  ~0ULL)
            ->setActive();

    MockDcpMessageProducers producers;
    notifyAndRunToCheckpoint(*producer, producers);
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(
                      producers, cb::mcbp::ClientOpcode::DcpSnapshotMarker));
    EXPECT_EQ(0, producers.last_snap_start_seqno);
    EXPECT_EQ(2, producers.last_snap_end_seqno);

    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(producers,
                                      cb::mcbp::ClientOpcode::DcpMutation));
    EXPECT_EQ(1, producers.last_byseqno);
    EXPECT_EQ("keyA", producers.last_key);

    // No data, but stays connected
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(false, producers));
}

// This is a variation of the disconnect test - here we ACK some bytes and avoid
// a disconnect!
TEST_P(TestStuckProducer, producerNotDisconnectedClientAcked) {
    auto& vb = *store->getVBucket(vbid);
    // The name of this producer will match the configured regex.
    auto producer =
            std::make_shared<MockDcpProducer>(*engine,
                                              cookie,
                                              "dcp:disconnect-me:p->c",
                                              cb::mcbp::DcpOpenFlag::None,
                                              false);
    producer->createCheckpointProcessorTask();
    producer->scheduleCheckpointProcessorTask();

    // Set a small connection buffer size to force the producer to pause
    // The snapshot and first mutation will fill the buffer, the 2nd mutation
    // (final step) will then trigger the producer to disconnect when the flow
    // control is seen to be in the paused state with no change for 0 seconds
    EXPECT_EQ(
            cb::engine_errc::success,
            producer->control(
                    0 /*opaque*/, DcpControlKeys::ConnectionBufferSize, "100"));

    producer->addMockActiveStream(cb::mcbp::DcpAddStreamFlag::None,
                                  0,
                                  vb,
                                  0,
                                  ~0ULL,
                                  0x0,
                                  0,
                                  ~0ULL)
            ->setActive();

    MockDcpMessageProducers producers;
    notifyAndRunToCheckpoint(*producer, producers);
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(
                      producers, cb::mcbp::ClientOpcode::DcpSnapshotMarker));
    EXPECT_EQ(0, producers.last_snap_start_seqno);
    EXPECT_EQ(2, producers.last_snap_end_seqno);

    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(producers,
                                      cb::mcbp::ClientOpcode::DcpMutation));
    EXPECT_EQ(1, producers.last_byseqno);
    EXPECT_EQ("keyA", producers.last_key);

    // Acknowledge some bytes so the next step doesn't disconnect
    producer->ackBytesOutstanding(100);

    // Unpaused, no disconnect and the next mutation is processed
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(producers,
                                      cb::mcbp::ClientOpcode::DcpMutation));
    EXPECT_EQ(2, producers.last_byseqno);
    EXPECT_EQ("keyB", producers.last_key);
}

INSTANTIATE_TEST_SUITE_P(AllBucketTypes,
                         TestStuckProducer,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

class TestDcpConsumerMaxMarkerVersion : public SingleThreadedPassiveStreamTest {
public:
    void SetUp() override {
        // Don't start as replica initially, we'll set up the consumer manually
        startAsReplica = false;
        SingleThreadedPassiveStreamTest::SetUp();
    }

    void setupConsumerWithMaxMarker(double maxMarkerVersion) {
        mock_set_dcp_consumer_max_marker_version(maxMarkerVersion);
        setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
        setupConsumerAndPassiveStream();
    }

    void verifySnapshotMarkerVersionControl(bool expected,
                                            std::string_view maxMarkerVersion) {
        auto controls = consumer->public_getPendingControls().lock();

        auto maxMarkerVersionControl = std::find_if(
                controls->begin(), controls->end(), [](const auto& control) {
                    return control.key ==
                           DcpControlKeys::SnapshotMaxMarkerVersion;
                });

        bool foundSnapshotMarkerVersion =
                (maxMarkerVersionControl != controls->end());

        EXPECT_EQ(expected, foundSnapshotMarkerVersion)
                << "Snapshot marker version control should "
                << (expected ? "be" : "not be") << " present";

        if (foundSnapshotMarkerVersion) {
            EXPECT_EQ(maxMarkerVersion, maxMarkerVersionControl->value)
                    << "max_marker_version control value should be '"
                    << maxMarkerVersion << "'";
        }
    }
};

TEST_P(TestDcpConsumerMaxMarkerVersion, MaxMarkerVersionEnabled) {
    setupConsumerWithMaxMarker(2.2);
    verifySnapshotMarkerVersionControl(true, "2.2");
}

TEST_P(TestDcpConsumerMaxMarkerVersion, MaxMarkerVersionDisabled) {
    setupConsumerWithMaxMarker(0.0);
    verifySnapshotMarkerVersionControl(false, "");
}

INSTANTIATE_TEST_SUITE_P(AllBucketTypes,
                         TestDcpConsumerMaxMarkerVersion,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

class TestDcpSnapshotMarkerFeatures : public SingleThreadedActiveStreamTest {
public:
    void SetUp() override {
        SingleThreadedActiveStreamTest::SetUp();
        store_item(vbid, makeStoredDocKey("foo"), "bar");
        flushAndRemoveCheckpoints(vbid);
    }

    void TearDown() override {
        // Reset the DCP snapshot marker feature settings
        mock_set_dcp_snapshot_marker_purge_seqno_enabled(true);
        mock_set_dcp_snapshot_marker_hps_enabled(true);
        SingleThreadedActiveStreamTest::TearDown();
    }

    void testDcpSnapshotMarkerHPSEnabled(bool enabled) {
        auto& vb = *store->getVBucket(vbid);
        stream.reset();

        // Enable/disable HPS in snapshot marker
        mock_set_dcp_snapshot_marker_hps_enabled(enabled);

        // Create producer and stream
        recreateProducer(vb,
                         cb::mcbp::DcpOpenFlag::None,
                         {{DcpControlKeys::SnapshotMaxMarkerVersion, "2.2"}});
        producer->createCheckpointProcessorTask();
        // Requirement for HPS to be included in snapshot marker
        producer->setSyncReplication(SyncReplication::SyncReplication);
        producer->setMaxMarkerVersion(MarkerVersion::V2_2);
        recreateStream(vb, true /* enforceProducerFlags */);

        stream->transitionStateToBackfilling();
        EXPECT_EQ(0, stream->public_readyQSize());
        // Perform the actual backfill - fills up the readyQ in the stream.
        producer->getBFM().backfill();

        // Verify HPS presence/absence depending on the setting
        auto resp = stream->public_nextQueuedItem(*producer);
        EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
        auto* marker = dynamic_cast<SnapshotMarker*>(resp.get());
        EXPECT_EQ(enabled, marker->getHighPreparedSeqno().has_value());
    }

    void testDcpSnapshotMarkerPurgeSeqnoEnabled(bool enabled) {
        auto& vb = *store->getVBucket(vbid);
        stream.reset();

        // Enable/disable purge seqno in snapshot marker
        mock_set_dcp_snapshot_marker_purge_seqno_enabled(enabled);

        // Create producer and stream
        recreateProducer(vb,
                         cb::mcbp::DcpOpenFlag::None,
                         {{DcpControlKeys::SnapshotMaxMarkerVersion, "2.2"}});
        producer->createCheckpointProcessorTask();
        // Requirement for purge seqno to be included in snapshot marker
        producer->setMaxMarkerVersion(MarkerVersion::V2_2);
        recreateStream(vb, true /* enforceProducerFlags */);

        stream->transitionStateToBackfilling();
        EXPECT_EQ(0, stream->public_readyQSize());
        // Perform the actual backfill - fills up the readyQ in the stream.
        producer->getBFM().backfill();

        // Verify purge seqno presence/absence depending on the setting
        auto resp = stream->public_nextQueuedItem(*producer);
        EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
        auto* marker = dynamic_cast<SnapshotMarker*>(resp.get());
        EXPECT_EQ(enabled, marker->getPurgeSeqno().has_value());
    }
};

TEST_P(TestDcpSnapshotMarkerFeatures, TestDcpSnapshotMarkerHPSEnabledTrue) {
    testDcpSnapshotMarkerHPSEnabled(true);
}

TEST_P(TestDcpSnapshotMarkerFeatures, TestDcpSnapshotMarkerHPSEnabledFalse) {
    testDcpSnapshotMarkerHPSEnabled(false);
}

TEST_P(TestDcpSnapshotMarkerFeatures,
       TestDcpSnapshotMarkerPurgeSeqnoEnabledTrue) {
    testDcpSnapshotMarkerPurgeSeqnoEnabled(true);
}

TEST_P(TestDcpSnapshotMarkerFeatures,
       TestDcpSnapshotMarkerPurgeSeqnoEnabledFalse) {
    testDcpSnapshotMarkerPurgeSeqnoEnabled(false);
}

INSTANTIATE_TEST_SUITE_P(AllBucketTypes,
                         TestDcpSnapshotMarkerFeatures,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);
