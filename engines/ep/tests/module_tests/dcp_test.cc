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
#include "../mock/mock_synchronous_ep_engine.h"
#include "checkpoint.h"
#include "checkpoint_utils.h"
#include "connmap.h"
#include "dcp/backfill_disk.h"
#include "dcp/dcp-types.h"
#include "dcp/dcpconnmap.h"
#include "dcp/producer.h"
#include "dcp/stream.h"
#include "dcp_utils.h"
#include "ep_time.h"
#include "evp_engine_test.h"
#include "evp_store_single_threaded_test.h"
#include "failover-table.h"
#include "memory_tracker.h"
#include "objectregistry.h"
#include "replicationthrottle.h"
#include "test_helpers.h"
#include "thread_gate.h"

#include <dcp/backfill_memory.h>
#include <gtest/gtest.h>
#include <platform/compress.h>
#include <xattr/utils.h>

#include <thread>

extern uint8_t dcp_last_op;
extern uint32_t dcp_last_flags;

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
        callbackCount = 0;

#if defined(HAVE_JEMALLOC)
        // MB-28370: Run with memory tracking for all alloc/deallocs when built
        // with jemalloc.
        MemoryTracker::getInstance(*get_mock_server_api()->alloc_hooks);
        engine->getEpStats().memoryTrackerEnabled.store(true);
#endif
    }

    void TearDown() override {
        /* MB-22041 changes to dynamically stopping threads rather than having
         * the excess looping but not getting work. We now need to set the
         * AuxIO and NonIO back to 1 to allow dead tasks to be cleaned up
         */
        ExecutorPool::get()->setNumAuxIO(1);
        ExecutorPool::get()->setNumNonIO(1);

        EventuallyPersistentEngineTest::TearDown();

        MemoryTracker::destroyInstance();
    }

    // Setup a DCP producer and attach a stream and cursor to it.
    void setup_dcp_stream(
            int flags = 0,
            IncludeValue includeVal = IncludeValue::Yes,
            IncludeXattrs includeXattrs = IncludeXattrs::Yes,
            std::vector<std::pair<std::string, std::string>> controls = {}) {
        if (includeVal == IncludeValue::No) {
            flags |= DCP_OPEN_NO_VALUE;
        }
        if (includeVal == IncludeValue::NoWithUnderlyingDatatype) {
            flags |= DCP_OPEN_NO_VALUE_WITH_UNDERLYING_DATATYPE;
        }
        if (includeXattrs == IncludeXattrs::Yes) {
            flags |= DCP_OPEN_INCLUDE_XATTRS;
        }
        producer = std::make_shared<MockDcpProducer>(
                *engine,
                cookie,
                "test_producer",
                flags,
                cb::const_byte_buffer() /*no json*/,
                /*startTask*/ true);

        if (includeXattrs == IncludeXattrs::Yes) {
            producer->setNoopEnabled(true);
        }

        // Since we are creating a mock active stream outside of
        // DcpProducer::streamRequest(), and we want the checkpt processor task,
        // create it explicitly here
        producer->createCheckpointProcessorTask();
        producer->scheduleCheckpointProcessorTask();

        // Now set any controls before creating any streams
        for (const auto& control : controls) {
            EXPECT_EQ(ENGINE_SUCCESS,
                      producer->control(0,
                                        control.first.c_str(),
                                        control.first.size(),
                                        control.second.c_str(),
                                        control.second.size()));
        }

        vb0 = engine->getVBucket(vbid);
        ASSERT_NE(nullptr, vb0.get());
        EXPECT_TRUE(vb0) << "Failed to get valid VBucket object for id 0";
        stream = std::make_shared<MockActiveStream>(engine,
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

        EXPECT_FALSE(vb0->checkpointManager->registerCursor(
                stream->getCursorName(), 1, false, MustSendCheckpointEnd::NO))
                << "Found an existing TAP cursor when attempting to register "
                   "ours";
        stream->setActive();
    }

    void destroy_dcp_stream() {
        producer->closeStream(/*opaque*/ 0, vb0->getId());
    }

    /*
     * Creates an item with the key \"key\", containing json data and xattrs.
     * @return a unique_ptr to a newly created item.
     */
    std::unique_ptr<Item> makeItemWithXattrs() {
        std::string valueData = R"({"json":"yes"})";
        std::string data = createXattrValue(valueData);
        protocol_binary_datatype_t datatype = (PROTOCOL_BINARY_DATATYPE_JSON |
                                               PROTOCOL_BINARY_DATATYPE_XATTR);
        return std::make_unique<Item>(makeStoredDocKey("key"),
                                      /*flags*/0,
                                      /*exp*/0,
                                      data.c_str(),
                                      data.size(),
                                      datatype);
    }

    /*
     * Creates an item with the key \"key\", containing json data and no xattrs.
     * @return a unique_ptr to a newly created item.
     */
    std::unique_ptr<Item> makeItemWithoutXattrs() {
            std::string valueData = R"({"json":"yes"})";
            protocol_binary_datatype_t datatype = PROTOCOL_BINARY_DATATYPE_JSON;
            return std::make_unique<Item>(makeStoredDocKey("key"),
                                          /*flags*/0,
                                          /*exp*/0,
                                          valueData.c_str(),
                                          valueData.size(),
                                          datatype);
    }

    /* Add items onto the vbucket and wait for the checkpoint to be removed */
    void addItemsAndRemoveCheckpoint(int numItems) {
        for (int i = 0; i < numItems; ++i) {
            std::string key("key" + std::to_string(i));
            store_item(vbid, key, "value");
        }
        removeCheckpoint(numItems);
    }

    void removeCheckpoint(int numItems) {
        /* Create new checkpoint so that we can remove the current checkpoint
           and force a backfill in the DCP stream */
        auto& ckpt_mgr = *vb0->checkpointManager;
        ckpt_mgr.createNewCheckpoint();

        /* Wait for removal of the old checkpoint, this also would imply that
           the items are persisted (in case of persistent buckets) */
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

    std::shared_ptr<MockDcpProducer> producer;
    std::shared_ptr<MockActiveStream> stream;
    VBucketPtr vb0;

    /*
     * Fake callback emulating dcp_add_failover_log
     */
    static ENGINE_ERROR_CODE fakeDcpAddFailoverLog(
            vbucket_failover_t* entry,
            size_t nentries,
            gsl::not_null<const void*> cookie) {
        callbackCount++;
        return ENGINE_SUCCESS;
    }

    // callbackCount needs to be static as its used inside of the static
    // function fakeDcpAddFailoverLog.
    static int callbackCount;
};
int DCPTest::callbackCount = 0;

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
            producer->cancelCheckpointCreatorTask();
        }
        // Destroy various engine objects
        vb0.reset();
        stream.reset();
        producer.reset();
        DCPTest::TearDown();
    }
};

/*
 * Test that when have a producer with IncludeValue and IncludeXattrs both set
 * to No an active stream created via a streamRequest returns true for
 * isKeyOnly.
 */
TEST_P(StreamTest, test_streamIsKeyOnlyTrue) {
    setup_dcp_stream(0, IncludeValue::No, IncludeXattrs::No);
    uint64_t rollbackSeqno;
    auto err = producer->streamRequest(/*flags*/ 0,
                                       /*opaque*/ 0,
                                       /*vbucket*/ 0,
                                       /*start_seqno*/ 0,
                                       /*end_seqno*/ 0,
                                       /*vb_uuid*/ 0,
                                       /*snap_start*/ 0,
                                       /*snap_end*/ 0,
                                       &rollbackSeqno,
                                       DCPTest::fakeDcpAddFailoverLog);
    ASSERT_EQ(ENGINE_SUCCESS, err)
        << "stream request did not return ENGINE_SUCCESS";

    auto activeStream =
            std::dynamic_pointer_cast<ActiveStream>(producer->findStream(0));
    ASSERT_NE(nullptr, activeStream);
    EXPECT_TRUE(activeStream->isKeyOnly());
    destroy_dcp_stream();
}

ENGINE_ERROR_CODE mock_mutation_return_engine_e2big(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        item* itm,
        uint16_t vbucket,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        uint32_t lock_time,
        const void* meta,
        uint16_t nmeta,
        uint8_t nru,
        uint8_t collection_len) {
    Item* item = reinterpret_cast<Item*>(itm);
    delete item;
    return ENGINE_E2BIG;
}

std::string decompressValue(std::string compressedValue) {
    cb::compression::Buffer buffer;
    if (!cb::compression::inflate(cb::compression::Algorithm::Snappy,
                                  compressedValue, buffer)) {
        return {};
    }

    return std::string(buffer.data(), buffer.size());
}

extern std::string dcp_last_value;
extern uint32_t dcp_last_packet_size;
extern protocol_binary_datatype_t dcp_last_datatype;

class CompressionStreamTest : public DCPTest,
                              public ::testing::WithParamInterface<
                                      ::testing::tuple<std::string, bool>> {
public:
    void SetUp() override {
        bucketType = ::testing::get<0>(GetParam());
        DCPTest::SetUp();
        vb0 = engine->getVBucket(0);
        EXPECT_TRUE(vb0) << "Failed to get valid VBucket object for id 0";
    }

    void TearDown() override {
        if (producer) {
            producer->cancelCheckpointCreatorTask();
        }
        // Destroy various engine objects
        vb0.reset();
        stream.reset();
        producer.reset();
        DCPTest::TearDown();
    }

    bool isXattr() const {
        return ::testing::get<1>(GetParam());
    }

    size_t getItemSize(Item& item) {
        if (isXattr()) {
            return MutationResponse::mutationBaseMsgBytes +
                   item.getKey().size() +
                   // DCP won't recompress the pruned document
                   getXattrSize(false);
        }
        return MutationResponse::mutationBaseMsgBytes + item.getKey().size() +
               item.getNBytes();
    }

    size_t getXattrSize(bool compressed) const {
        return createXattrValue({}, true, compressed).size();
    }
};

// Test the compression control error case
TEST_P(StreamTest, validate_compression_control_message_denied) {
    setup_dcp_stream();
    std::string compressCtrlMsg("force_value_compression");
    std::string compressCtrlValue("true");
    EXPECT_FALSE(producer->isCompressionEnabled());

    // Sending a control message without actually enabling SNAPPY must fail
    EXPECT_EQ(ENGINE_EINVAL,
              producer->control(0,
                                compressCtrlMsg.c_str(),
                                compressCtrlMsg.size(),
                                compressCtrlValue.c_str(),
                                compressCtrlValue.size()));
    destroy_dcp_stream();
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
              producer->control(0,
                                compressCtrlMsg.c_str(),
                                compressCtrlMsg.size(),
                                compressCtrlValue.c_str(),
                                compressCtrlValue.size()));
    destroy_dcp_stream();
}

/**
 * Test to verify DCP compression/decompression. There are 4 cases that are being
 * tested
 *
 * 1. Add a compressed item and stream a compressed item
 * 2. Add an uncompressed item and stream a compressed item
 * 3. Add a compressed item and stream an uncompressed item
 * 4. Add an uncompressed item and stream an uncompressed item
 */

/**
 * There are 2 cases that are
 * being tested in this test. This test uses a producer/connection without
 * compression enabled
 *
 * 1. Add a compressed item and expect to stream an uncompressed item
 * 2. Add an uncompressed item and expect to stream an uncompressed item
 *
 */
TEST_P(CompressionStreamTest, compression_not_enabled) {
    VBucketPtr vb = engine->getKVBucket()->getVBucket(vbid);
    std::string valueData("{\"product\": \"car\",\"price\": \"100\"},"
                          "{\"product\": \"bus\",\"price\": \"1000\"},"
                          "{\"product\": \"Train\",\"price\": \"100000\"}");
    auto item1 = makeCompressibleItem(vbid,
                                      makeStoredDocKey("key1"),
                                      valueData,
                                      PROTOCOL_BINARY_DATATYPE_JSON,
                                      true, // compressed
                                      isXattr());
    auto item2 = makeCompressibleItem(vbid,
                                      makeStoredDocKey("key2"),
                                      valueData,
                                      PROTOCOL_BINARY_DATATYPE_JSON,
                                      false, // uncompressed
                                      isXattr());

    auto includeValue = isXattr() ? IncludeValue::No : IncludeValue::Yes;
    setup_dcp_stream(0, includeValue, IncludeXattrs::Yes);

    /**
     * Ensure that compression is disabled
     */
    ASSERT_FALSE(producer->isCompressionEnabled());

    auto producers = get_dcp_producers(reinterpret_cast<ENGINE_HANDLE*>(engine),
                                       reinterpret_cast<ENGINE_HANDLE_V1*>(engine));

    // Now, add 2 items
    EXPECT_EQ(ENGINE_SUCCESS, engine->getKVBucket()->set(*item1, cookie));
    EXPECT_EQ(ENGINE_SUCCESS, engine->getKVBucket()->set(*item2, cookie));

    auto keyAndSnappyValueMessageSize = getItemSize(*item1);

    /**
     * Create a DCP response and check that a new item isn't created and that
     * the size of the response message is greater than the size of the original
     * message (or equal for xattr stream)
     */
    queued_item qi(std::move(item1));
    std::unique_ptr<DcpResponse> dcpResponse = stream->public_makeResponseFromItem(qi);
    auto mutProdResponse = dynamic_cast<MutationProducerResponse*>(dcpResponse.get());
    ASSERT_NE(qi.get(), mutProdResponse->getItem().get());
    if (isXattr()) {
        // The same sizes. makeResponseFromItem will have inflated and not
        // compressed as part of the value pruning
        EXPECT_EQ(keyAndSnappyValueMessageSize, dcpResponse->getMessageSize());
    } else {
        EXPECT_LT(keyAndSnappyValueMessageSize, dcpResponse->getMessageSize());
    }

    uint64_t rollbackSeqno;
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(/*flags*/ 0,
                                      /*opaque*/ 0,
                                      /*vbucket*/ 0,
                                      /*start_seqno*/ 0,
                                      /*end_seqno*/ ~0,
                                      /*vb_uuid*/ 0,
                                      /*snap_start*/ 0,
                                      /*snap_end*/ ~0,
                                      &rollbackSeqno,
                                      DCPTest::fakeDcpAddFailoverLog));

    producer->notifySeqnoAvailable(vbid, vb->getHighSeqno());
    ASSERT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    ASSERT_EQ(1, producer->getCheckpointSnapshotTask().queueSize());
    producer->getCheckpointSnapshotTask().run();

    /* Stream the snapshot marker first */
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));
    EXPECT_EQ(0, producer->getItemsSent());

    /* Stream the first mutation */
    protocol_binary_datatype_t expectedDataType =
            isXattr() ? PROTOCOL_BINARY_DATATYPE_XATTR
                      : PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));
    std::string value(qi->getValue()->getData(), qi->getValue()->valueSize());
    EXPECT_STREQ(dcp_last_value.c_str(), decompressValue(value).c_str());

    if (isXattr()) {
        // The pruned packet won't be recompressed
        EXPECT_EQ(dcp_last_packet_size, keyAndSnappyValueMessageSize);
    } else {
        EXPECT_GT(dcp_last_packet_size, keyAndSnappyValueMessageSize);
    }

    EXPECT_FALSE(mcbp::datatype::is_snappy(dcp_last_datatype));
    EXPECT_EQ(expectedDataType, dcp_last_datatype);

    /**
     * Create a DCP response and check that a new item is created and
     * the message size is less than the size of original item
     */
    uint32_t keyAndValueMessageSize = getItemSize(*item2);
    qi.reset(std::move(item2));
    dcpResponse = stream->public_makeResponseFromItem(qi);
    mutProdResponse = dynamic_cast<MutationProducerResponse*>(dcpResponse.get());

    // A new pruned item will always be generated
    if (!isXattr()) {
        ASSERT_EQ(qi.get(), mutProdResponse->getItem().get());
    }
    EXPECT_EQ(dcpResponse->getMessageSize(), keyAndValueMessageSize);

    /* Stream the second mutation */
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));

    value.assign(qi->getValue()->getData(), qi->getValue()->valueSize());
    EXPECT_STREQ(value.c_str(), dcp_last_value.c_str());
    EXPECT_EQ(dcp_last_packet_size, keyAndValueMessageSize);

    EXPECT_FALSE(mcbp::datatype::is_snappy(dcp_last_datatype));
    EXPECT_EQ(expectedDataType, dcp_last_datatype);
}

/**
 * Test to verify DCP compression, this test has client snappy enabled
 *
 *  - Add a compressed item and expect we stream a compressed item
 *
 * Note when the test is running xattr-only DCP, expect we stream an
 * uncompressed item
 */
TEST_P(CompressionStreamTest, connection_snappy_enabled) {
    VBucketPtr vb = engine->getKVBucket()->getVBucket(vbid);
    std::string valueData(
            "{\"product\": \"car\",\"price\": \"100\"},"
            "{\"product\": \"bus\",\"price\": \"1000\"},"
            "{\"product\": \"Train\",\"price\": \"100000\"}");

    auto item = makeCompressibleItem(vbid,
                                     makeStoredDocKey("key"),
                                     valueData,
                                     PROTOCOL_BINARY_DATATYPE_JSON,
                                     true, // compressed
                                     isXattr());

    //Enable the snappy datatype on the connection
    mock_set_datatype_support(cookie, PROTOCOL_BINARY_DATATYPE_SNAPPY);

    auto includeValue = isXattr() ? IncludeValue::No : IncludeValue::Yes;
    setup_dcp_stream(0, includeValue, IncludeXattrs::Yes);

    uint64_t rollbackSeqno;
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(/*flags*/ 0,
                                      /*opaque*/ 0,
                                      /*vbucket*/ 0,
                                      /*start_seqno*/ 0,
                                      /*end_seqno*/ ~0,
                                      /*vb_uuid*/ 0,
                                      /*snap_start*/ 0,
                                      /*snap_end*/ ~0,
                                      &rollbackSeqno,
                                      DCPTest::fakeDcpAddFailoverLog));

    auto producers =
            get_dcp_producers(reinterpret_cast<ENGINE_HANDLE*>(engine),
                              reinterpret_cast<ENGINE_HANDLE_V1*>(engine));
    ASSERT_TRUE(producer->isCompressionEnabled());

    // Now, add the 3rd item. This item should be compressed
    EXPECT_EQ(ENGINE_SUCCESS, engine->getKVBucket()->set(*item, cookie));

    producer->notifySeqnoAvailable(vbid, vb->getHighSeqno());
    ASSERT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    ASSERT_EQ(1, producer->getCheckpointSnapshotTask().queueSize());
    producer->getCheckpointSnapshotTask().run();

    /* Stream the snapshot marker */
    ASSERT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));

    /* Stream the 3rd mutation */
    ASSERT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));

    /**
     * Create a DCP response and check that a new item is created and
     * the message size is greater than the size of original item
     */
    auto keyAndSnappyValueMessageSize = getItemSize(*item);
    queued_item qi = std::move(item);
    auto dcpResponse = stream->public_makeResponseFromItem(qi);
    auto* mutProdResponse =
            dynamic_cast<MutationProducerResponse*>(dcpResponse.get());
    std::string value;
    if (!isXattr()) {
        ASSERT_EQ(qi.get(), mutProdResponse->getItem().get());
        value.assign(qi->getValue()->getData(), qi->getValue()->valueSize());
    }

    EXPECT_STREQ(dcp_last_value.c_str(), value.c_str());
    EXPECT_EQ(dcpResponse->getMessageSize(), keyAndSnappyValueMessageSize);

    EXPECT_EQ(dcp_last_packet_size, keyAndSnappyValueMessageSize);

    // If xattr-only enabled on DCP, we won't re-compress (after we've
    // decompressed the document and split out the xattrs)
    protocol_binary_datatype_t snappy =
            isXattr() ? 0 : PROTOCOL_BINARY_DATATYPE_SNAPPY;
    protocol_binary_datatype_t expectedDataType =
            isXattr() ? PROTOCOL_BINARY_DATATYPE_XATTR
                      : PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ((expectedDataType | snappy), dcp_last_datatype);
}

/**
 * Test to verify DCP compression, this test has client snappy enabled
 *
 *  - Add an uncompressed item and expect we stream a compressed item
 */
TEST_P(CompressionStreamTest, force_value_compression_enabled) {
    VBucketPtr vb = engine->getKVBucket()->getVBucket(vbid);
    std::string valueData(
            "{\"product\": \"car\",\"price\": \"100\"},"
            "{\"product\": \"bus\",\"price\": \"1000\"},"
            "{\"product\": \"Train\",\"price\": \"100000\"}");

    auto item = makeCompressibleItem(vbid,
                                     makeStoredDocKey("key"),
                                     valueData,
                                     PROTOCOL_BINARY_DATATYPE_JSON,
                                     false, // not compressed
                                     isXattr());

    // Enable the snappy datatype on the connection
    mock_set_datatype_support(cookie, PROTOCOL_BINARY_DATATYPE_SNAPPY);
    auto includeValue = isXattr() ? IncludeValue::No : IncludeValue::Yes;

    // Setup the producer/stream and request force_value_compression
    setup_dcp_stream(0,
                     includeValue,
                     IncludeXattrs::Yes,
                     {{"force_value_compression", "true"}});

    uint64_t rollbackSeqno;
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(/*flags*/ 0,
                                      /*opaque*/ 0,
                                      /*vbucket*/ 0,
                                      /*start_seqno*/ 0,
                                      /*end_seqno*/ ~0,
                                      /*vb_uuid*/ 0,
                                      /*snap_start*/ 0,
                                      /*snap_end*/ ~0,
                                      &rollbackSeqno,
                                      DCPTest::fakeDcpAddFailoverLog));
    auto producers =
            get_dcp_producers(reinterpret_cast<ENGINE_HANDLE*>(engine),
                              reinterpret_cast<ENGINE_HANDLE_V1*>(engine));

    ASSERT_TRUE(producer->isForceValueCompressionEnabled());

    // Now, add the 4th item, which is not compressed
    EXPECT_EQ(ENGINE_SUCCESS, engine->getKVBucket()->set(*item, cookie));
    /**
     * Create a DCP response and check that a new item is created and
     * the message size is less than the size of the original item
     */
    auto keyAndValueMessageSize = getItemSize(*item);
    queued_item qi = std::move(item);
    auto dcpResponse = stream->public_makeResponseFromItem(qi);
    auto* mutProdResponse =
            dynamic_cast<MutationProducerResponse*>(dcpResponse.get());
    ASSERT_NE(qi.get(), mutProdResponse->getItem().get());
    EXPECT_LT(dcpResponse->getMessageSize(), keyAndValueMessageSize);

    producer->notifySeqnoAvailable(vbid, vb->getHighSeqno());
    ASSERT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    ASSERT_EQ(1, producer->getCheckpointSnapshotTask().queueSize());
    producer->getCheckpointSnapshotTask().run();

    /* Stream the snapshot marker */
    ASSERT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));

    /* Stream the mutation */
    ASSERT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));
    std::string value(qi->getValue()->getData(), qi->getValue()->valueSize());
    EXPECT_STREQ(decompressValue(dcp_last_value).c_str(), value.c_str());
    EXPECT_LT(dcp_last_packet_size, keyAndValueMessageSize);

    protocol_binary_datatype_t expectedDataType =
            isXattr() ? PROTOCOL_BINARY_DATATYPE_XATTR
                      : PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ((expectedDataType | PROTOCOL_BINARY_DATATYPE_SNAPPY),
              dcp_last_datatype);

    destroy_dcp_stream();
}

/*
 * Test to verify the number of items, total bytes sent and total data size
 * by the producer when DCP compression is enabled
 */
TEST_P(StreamTest, test_verifyProducerCompressionStats) {
    VBucketPtr vb = engine->getKVBucket()->getVBucket(vbid);
    setup_dcp_stream();
    std::string compressibleValue("{\"product\": \"car\",\"price\": \"100\"},"
                                  "{\"product\": \"bus\",\"price\": \"1000\"},"
                                  "{\"product\": \"Train\",\"price\": \"100000\"}");
    std::string regularValue("{\"product\": \"car\",\"price\": \"100\"}");

    std::string compressCtrlMsg("force_value_compression");
    std::string compressCtrlValue("true");

    mock_set_datatype_support(producer->getCookie(), PROTOCOL_BINARY_DATATYPE_SNAPPY);

    ASSERT_EQ(ENGINE_SUCCESS, producer->control(0, compressCtrlMsg.c_str(),
                                                compressCtrlMsg.size(),
                                                compressCtrlValue.c_str(),
                                                compressCtrlValue.size()));
    ASSERT_TRUE(producer->isForceValueCompressionEnabled());

    store_item(vbid, "key1", compressibleValue.c_str());
    store_item(vbid, "key2", regularValue.c_str());
    store_item(vbid, "key3", compressibleValue.c_str());

    auto producers = get_dcp_producers(reinterpret_cast<ENGINE_HANDLE*>(engine),
                                       reinterpret_cast<ENGINE_HANDLE_V1*>(engine));

    uint64_t rollbackSeqno;
    auto err = producer->streamRequest(/*flags*/ 0,
                                       /*opaque*/ 0,
                                       /*vbucket*/ 0,
                                       /*start_seqno*/ 0,
                                       /*end_seqno*/ ~0,
                                       /*vb_uuid*/ 0,
                                       /*snap_start*/ 0,
                                       /*snap_end*/ ~0,
                                       &rollbackSeqno,
                                       DCPTest::fakeDcpAddFailoverLog);

    ASSERT_EQ(ENGINE_SUCCESS, err);
    producer->notifySeqnoAvailable(vbid, vb->getHighSeqno());

    ASSERT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    ASSERT_EQ(1, producer->getCheckpointSnapshotTask().queueSize());
    producer->getCheckpointSnapshotTask().run();

    /* Stream the snapshot marker first */
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));
    EXPECT_EQ(0, producer->getItemsSent());

    uint64_t totalBytesSent = producer->getTotalBytesSent();
    uint64_t totalUncompressedDataSize = producer->getTotalUncompressedDataSize();
    EXPECT_GT(totalBytesSent, 0);
    EXPECT_GT(totalUncompressedDataSize, 0);

    /* Stream the first mutation. This should increment the
     * number of items, total bytes sent and total data size.
     * Since this is a compressible document, the total bytes
     * sent should be incremented by a lesser value than the
     * total data size.
     */
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));
    EXPECT_EQ(1, producer->getItemsSent());
    EXPECT_GT(producer->getTotalBytesSent(), totalBytesSent);
    EXPECT_GT(producer->getTotalUncompressedDataSize(), totalUncompressedDataSize);
    EXPECT_LT(producer->getTotalBytesSent() - totalBytesSent,
              producer->getTotalUncompressedDataSize() - totalUncompressedDataSize);

    totalBytesSent = producer->getTotalBytesSent();
    totalUncompressedDataSize = producer->getTotalUncompressedDataSize();

    /*
     * Now stream the second mutation. This should increment the
     * number of items and the total bytes sent. In this case,
     * the total data size should be incremented by exactly the
     * same amount as the total bytes sent
     */
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));
    EXPECT_EQ(2, producer->getItemsSent());
    EXPECT_GT(producer->getTotalBytesSent(), totalBytesSent);
    EXPECT_GT(producer->getTotalUncompressedDataSize(), totalUncompressedDataSize);
    EXPECT_EQ(producer->getTotalBytesSent() - totalBytesSent,
              producer->getTotalUncompressedDataSize() - totalUncompressedDataSize);

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
    ASSERT_EQ(ENGINE_SUCCESS, producer->control(0, compressCtrlMsg.c_str(),
                                                compressCtrlMsg.size(),
                                                compressCtrlValue.c_str(),
                                                compressCtrlValue.size()));
    mock_set_datatype_support(producer->getCookie(),
                              PROTOCOL_BINARY_RAW_BYTES);

    ASSERT_FALSE(producer->isCompressionEnabled());
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));
    EXPECT_EQ(3, producer->getItemsSent());
    EXPECT_GT(producer->getTotalBytesSent(), totalBytesSent);
    EXPECT_GT(producer->getTotalUncompressedDataSize(), totalUncompressedDataSize);
    EXPECT_EQ(producer->getTotalBytesSent() - totalBytesSent,
              producer->getTotalUncompressedDataSize() - totalUncompressedDataSize);

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
    auto producers = get_dcp_producers(reinterpret_cast<ENGINE_HANDLE*>(engine),
                                       reinterpret_cast<ENGINE_HANDLE_V1*>(engine));
    uint64_t rollbackSeqno;
    auto err = producer->streamRequest(/*flags*/ 0,
                                       /*opaque*/ 0,
                                       /*vbucket*/ 0,
                                       /*start_seqno*/ 0,
                                       /*end_seqno*/ ~0,
                                       /*vb_uuid*/ 0,
                                       /*snap_start*/ 0,
                                       /*snap_end*/ ~0,
                                       &rollbackSeqno,
                                       DCPTest::fakeDcpAddFailoverLog);

    EXPECT_EQ(ENGINE_SUCCESS, err);
    producer->notifySeqnoAvailable(vbid, vb->getHighSeqno());
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));

    EXPECT_EQ(1, producer->getCheckpointSnapshotTask().queueSize());

    producer->getCheckpointSnapshotTask().run();

    /* Stream the snapshot marker first */
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));
    EXPECT_EQ(0, producer->getItemsSent());

    uint64_t totalBytes = producer->getTotalBytesSent();
    EXPECT_GT(totalBytes, 0);

    /* Stream the first mutation. This should increment the
     * number of items and the total bytes sent.
     */
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));
    EXPECT_EQ(1, producer->getItemsSent());
    EXPECT_GT(producer->getTotalBytesSent(), totalBytes);
    totalBytes = producer->getTotalBytesSent();

    /* Now simulate a failure while trying to stream the next
     * mutation.
     */
    auto mutation_callback = producers->mutation;
    producers->mutation = mock_mutation_return_engine_e2big;

    EXPECT_EQ(ENGINE_E2BIG, producer->step(producers.get()));

    /* The number of items total bytes sent should remain the same */
    EXPECT_EQ(1, producer->getItemsSent());
    EXPECT_EQ(producer->getTotalBytesSent(), totalBytes);
    totalBytes = producer->getTotalBytesSent();

    /* Now stream the mutation again and the stats should have incremented */
    producers->mutation = mutation_callback;

    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));
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
    uint64_t rollbackSeqno;
    auto err = producer->streamRequest(/*flags*/ 0,
                                       /*opaque*/ 0,
                                       /*vbucket*/ 0,
                                       /*start_seqno*/ 0,
                                       /*end_seqno*/ 0,
                                       /*vb_uuid*/ 0,
                                       /*snap_start*/ 0,
                                       /*snap_end*/ 0,
                                       &rollbackSeqno,
                                       DCPTest::fakeDcpAddFailoverLog);
    ASSERT_EQ(ENGINE_SUCCESS, err)
        << "stream request did not return ENGINE_SUCCESS";

    auto activeStream =
            std::dynamic_pointer_cast<ActiveStream>(producer->findStream(0));
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
    uint64_t rollbackSeqno;
    auto err = producer->streamRequest(/*flags*/ 0,
                                       /*opaque*/ 0,
                                       /*vbucket*/ 0,
                                       /*start_seqno*/ 0,
                                       /*end_seqno*/ 0,
                                       /*vb_uuid*/ 0,
                                       /*snap_start*/ 0,
                                       /*snap_end*/ 0,
                                       &rollbackSeqno,
                                       DCPTest::fakeDcpAddFailoverLog);
    ASSERT_EQ(ENGINE_SUCCESS, err)
        << "stream request did not return ENGINE_SUCCESS";

    auto activeStream =
            std::dynamic_pointer_cast<ActiveStream>(producer->findStream(0));
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
    auto keyOnlyMessageSize = MutationResponse::mutationBaseMsgBytes +
            item->getKey().size();
    queued_item qi(std::move(item));

    setup_dcp_stream(0, IncludeValue::No, IncludeXattrs::No);
    std::unique_ptr<DcpResponse> dcpResponse =
            stream->public_makeResponseFromItem(qi);

    /**
     * Create a DCP response and check that a new item is created
     */
    auto mutProdResponse = dynamic_cast<MutationProducerResponse*>(dcpResponse.get());
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
            MutationResponse::mutationBaseMsgBytes + item->getKey().size();
    queued_item qi(std::move(item));

    setup_dcp_stream(
            0, IncludeValue::NoWithUnderlyingDatatype, IncludeXattrs::No);
    std::unique_ptr<DcpResponse> dcpResponse =
            stream->public_makeResponseFromItem(qi);

    /**
     * Create a DCP response and check that a new item is created
     */
    auto mutProdResponse =
            dynamic_cast<MutationProducerResponse*>(dcpResponse.get());
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
    auto keyAndValueMessageSize = MutationResponse::mutationBaseMsgBytes +
            item->getKey().size() + item->getNBytes();
    queued_item qi(std::move(item));

    setup_dcp_stream(0, IncludeValue::Yes, IncludeXattrs::Yes);
    std::unique_ptr<DcpResponse> dcpResponse =
            stream->public_makeResponseFromItem(qi);

    /**
     * Create a DCP response and check that a new item is not created
     */
    auto mutProdResponse = dynamic_cast<MutationProducerResponse*>(dcpResponse.get());
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
    auto keyAndValueMessageSize = MutationResponse::mutationBaseMsgBytes +
            item->getKey().size() + item->getNBytes();
    queued_item qi(std::move(item));

    setup_dcp_stream(0, IncludeValue::Yes, IncludeXattrs::Yes);
    std::unique_ptr<DcpResponse> dcpResponse =
            stream->public_makeResponseFromItem(qi);

    /**
     * Create a DCP response and check that a new item is not created
     */
    auto mutProdResponse = dynamic_cast<MutationProducerResponse*>(dcpResponse.get());
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
    auto sz = cb::xattr::get_body_offset({
           reinterpret_cast<char*>(buffer.buf), buffer.len});
    auto keyAndValueMessageSize = MutationResponse::mutationBaseMsgBytes +
            item->getKey().size() + item->getNBytes() - sz;
    queued_item qi(std::move(item));

    setup_dcp_stream(0, IncludeValue::Yes, IncludeXattrs::No);
    std::unique_ptr<DcpResponse> dcpResponse =
            stream->public_makeResponseFromItem(qi);

    /**
     * Create a DCP response and check that a new item is created
     */
    auto mutProdResponse = dynamic_cast<MutationProducerResponse*>(dcpResponse.get());
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
    auto keyAndValueMessageSize = MutationResponse::mutationBaseMsgBytes +
            item->getKey().size() + item->getNBytes();
    queued_item qi(std::move(item));

    setup_dcp_stream(0, IncludeValue::Yes, IncludeXattrs::No);
    std::unique_ptr<DcpResponse> dcpResponse =
            stream->public_makeResponseFromItem(qi);
    /**
     * Create a DCP response and check that a new item is not created
     */
    auto mutProdResponse = dynamic_cast<MutationProducerResponse*>(dcpResponse.get());
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
    auto sz = cb::xattr::get_body_offset({
           reinterpret_cast<char*>(buffer.buf), buffer.len});
    auto keyAndValueMessageSize = MutationResponse::mutationBaseMsgBytes +
            item->getKey().size() + sz;
    queued_item qi(std::move(item));

    setup_dcp_stream(0, IncludeValue::No, IncludeXattrs::Yes);
    std::unique_ptr<DcpResponse> dcpResponse =
            stream->public_makeResponseFromItem(qi);

    /**
     * Create a DCP response and check that a new item is created
     */
    auto mutProdResponse = dynamic_cast<MutationProducerResponse*>(dcpResponse.get());
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
            MutationResponse::mutationBaseMsgBytes + item->getKey().size() + sz;
    queued_item qi(std::move(item));

    setup_dcp_stream(
            0, IncludeValue::NoWithUnderlyingDatatype, IncludeXattrs::Yes);
    std::unique_ptr<DcpResponse> dcpResponse =
            stream->public_makeResponseFromItem(qi);

    /**
     * Create a DCP response and check that a new item is created
     */
    auto mutProdResponse =
            dynamic_cast<MutationProducerResponse*>(dcpResponse.get());
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
            MutationResponse::mutationBaseMsgBytes + item->getKey().size();
    queued_item qi(std::move(item));

    setup_dcp_stream(
            0, IncludeValue::NoWithUnderlyingDatatype, IncludeXattrs::Yes);
    std::unique_ptr<DcpResponse> dcpResponse =
            stream->public_makeResponseFromItem(qi);

    /**
     * Create a DCP response and check that a new item is created
     */
    auto mutProdResponse =
            dynamic_cast<MutationProducerResponse*>(dcpResponse.get());
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
        DCPBackfillMemoryBuffered dcpbfm (evb, stream, 2, 4);
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

    // Now actually drain the items from the readyQ and see how many we received,
    // excluding meta items. This will result in all but one of the checkpoint
    // items (the one we added just above) being drained.
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
    int numItems = 3;
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
            engine->get_and_touch(cookie, makeStoredDocKey("key1"), vbid, i);
            i++;
        }
    });

    // Ensure all GATs are done before evaluating the stream below
    thr.join();

    /* Wait for the backfill task to complete */
    {
        std::chrono::microseconds uSleepTime(128);
        while (stream->getLastReadSeqno() < numItems) {
            uSleepTime = decayingSleep(uSleepTime);
        }
    }

    /* Verify that all items are read in the backfill */
    EXPECT_EQ(numItems, stream->getNumBackfillItems());

    /* Since backfill items are sitting in the readyQ, check if the stat is
       updated correctly */
    EXPECT_EQ(numItems, *stream->getNumBackfillItemsRemaining());

    destroy_dcp_stream();
    /* [TODO]: Expand the testcase to check if snapshot marker, all individual
               items are read correctly */
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

TEST_P(StreamTest, CursorDroppingBasicInMemoryState) {
    /* Set up a DCP stream */
    setup_dcp_stream();

    /* Transition stream to in-memory state and expect cursor dropping call to
       succeed */
    stream->transitionStateToBackfilling();
    stream->transitionStateToInMemory();
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
    uint64_t rollbackSeqno;
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(/*flags*/ 0,
                                      /*opaque*/ 0,
                                      /*vbucket*/ 0,
                                      /*start_seqno*/ numItems - 2,
                                      /*end_seqno*/ numItems,
                                      vbUuid,
                                      /*snap_start*/ numItems - 2,
                                      /*snap_end*/ numItems - 2,
                                      &rollbackSeqno,
                                      DCPTest::fakeDcpAddFailoverLog));
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->closeStream(/*opaque*/ 0, vb0->getId()));

    /* Set a start_seqno > purge_seqno > snap_start_seqno */
    engine->getKVBucket()->getLockedVBucket(vbid)->setPurgeSeqno(numItems - 3);

    /* We don't expect a rollback for this */
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(/*flags*/ 0,
                                      /*opaque*/ 0,
                                      /*vbucket*/ 0,
                                      /*start_seqno*/ numItems - 2,
                                      /*end_seqno*/ numItems,
                                      vbUuid,
                                      /*snap_start*/ 0,
                                      /*snap_end*/ numItems - 2,
                                      &rollbackSeqno,
                                      DCPTest::fakeDcpAddFailoverLog));
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->closeStream(/*opaque*/ 0, vb0->getId()));

    /* Set a purge_seqno > start_seqno */
    engine->getKVBucket()->getLockedVBucket(vbid)->setPurgeSeqno(numItems - 1);

    /* Now we expect a rollback to 0 */
    EXPECT_EQ(ENGINE_ROLLBACK,
              producer->streamRequest(/*flags*/ 0,
                                      /*opaque*/ 0,
                                      /*vbucket*/ 0,
                                      /*start_seqno*/ numItems - 2,
                                      /*end_seqno*/ numItems,
                                      vbUuid,
                                      /*snap_start*/ numItems - 2,
                                      /*snap_end*/ numItems - 2,
                                      &rollbackSeqno,
                                      DCPTest::fakeDcpAddFailoverLog));
    EXPECT_EQ(0, rollbackSeqno);
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
              engine->getKVBucket()->setVBucketState(vbid,
                                                     vbucket_state_dead,
                                                     true));
    uint64_t vbUuid = vb0->failovers->getLatestUUID();
    uint64_t rollbackSeqno;
    // Given the vbucket state is dead we should return not my vbucket.
    EXPECT_EQ(ENGINE_NOT_MY_VBUCKET,
              producer->streamRequest(/*flags*/ 0,
                                      /*opaque*/ 0,
                                      /*vbucket*/ 0,
                                      /*start_seqno*/ 0,
                                      /*end_seqno*/ 0,
                                      vbUuid,
                                      /*snap_start*/ 0,
                                      /*snap_end*/ 0,
                                      &rollbackSeqno,
                                      DCPTest::fakeDcpAddFailoverLog));
    // The callback function past to streamRequest should not be invoked.
    ASSERT_EQ(0, callbackCount);
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
        producer->closeAllStreams();
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
    CacheCallback callback(*engine, stream);

    stream->transitionStateToBackfilling();
    CacheLookup lookup(docKey, /*BySeqno*/ 1, vbid, {});
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
    CacheLookup lookup(docKey, /*BySeqno*/ 0, vbid, {});
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
    CacheLookup lookup(docKey, /*BySeqno*/ 1, vbid, {});
    // Make the key non-resident by evicting the key
    const char* msg;
    engine->evictKey(docKey, vbid, &msg);
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
    CacheLookup lookup(docKey, /*BySeqno*/ 1, vbid, {});
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

ENGINE_ERROR_CODE mock_noop_return_engine_e2big(
        gsl::not_null<const void*> cookie, uint32_t opaque) {
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
    auto producer = std::make_shared<MockDcpProducer>(
            *engine,
            cookie,
            "test_producer",
            /*flags*/ 0,
            cb::const_byte_buffer() /*no json*/);
    // "1" is not a multiple of "2" and so we should return ENGINE_EINVAL
    EXPECT_EQ(ENGINE_EINVAL,
              producer->control(0,
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
    auto producer = std::make_shared<MockDcpProducer>(
            *engine,
            cookie,
            "test_producer",
            /*flags*/ 0,
            cb::const_byte_buffer() /*no json*/);

    struct dcp_message_producers producers = {nullptr,
                                              nullptr,
                                              nullptr,
                                              nullptr,
                                              nullptr,
                                              nullptr,
                                              nullptr,
                                              nullptr,
                                              nullptr,
                                              nullptr,
                                              nullptr,
                                              nullptr,
                                              nullptr,
                                              mock_noop_return_engine_e2big,
                                              nullptr,
                                              nullptr};

    producer->setNoopEnabled(true);
    const auto send_time = ep_current_time() + 21;
    producer->setNoopSendTime(send_time);
    ENGINE_ERROR_CODE ret = producer->maybeSendNoop(&producers);
    EXPECT_EQ(ENGINE_E2BIG, ret)
    << "maybeSendNoop not returning ENGINE_E2BIG";
    EXPECT_FALSE(producer->getNoopPendingRecv())
            << "Waiting for noop acknowledgement";
    EXPECT_EQ(send_time, producer->getNoopSendTime())
            << "SendTime has been updated";
    producer->cancelCheckpointCreatorTask();
    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, test_maybesendnoop_send_noop) {
    const void* cookie = create_mock_cookie();
    // Create a Mock Dcp producer
    auto producer = std::make_shared<MockDcpProducer>(
            *engine,
            cookie,
            "test_producer",
            /*flags*/ 0,
            cb::const_byte_buffer() /*no json*/);

    std::unique_ptr<dcp_message_producers> producers(get_dcp_producers(handle, engine_v1));
    producer->setNoopEnabled(true);
    const auto send_time = ep_current_time() + 21;
    producer->setNoopSendTime(send_time);
    ENGINE_ERROR_CODE ret = producer->maybeSendNoop(producers.get());
    EXPECT_EQ(ENGINE_WANT_MORE, ret)
    << "maybeSendNoop not returning ENGINE_WANT_MORE";
    EXPECT_TRUE(producer->getNoopPendingRecv())
            << "Not waiting for noop acknowledgement";
    EXPECT_NE(send_time, producer->getNoopSendTime())
            << "SendTime has not been updated";
    producer->cancelCheckpointCreatorTask();
    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, test_maybesendnoop_noop_already_pending) {
    const void* cookie = create_mock_cookie();
    // Create a Mock Dcp producer
    auto producer = std::make_shared<MockDcpProducer>(
            *engine,
            cookie,
            "test_producer",
            /*flags*/ 0,
            cb::const_byte_buffer() /*no json*/);

    std::unique_ptr<dcp_message_producers> producers(
            get_dcp_producers(handle, engine_v1));
    const auto send_time = ep_current_time();
    TimeTraveller marty(engine->getConfiguration().getDcpIdleTimeout() + 1);
    producer->setNoopEnabled(true);
    producer->setNoopSendTime(send_time);
    ENGINE_ERROR_CODE ret = producer->maybeSendNoop(producers.get());
    // Check to see if a noop was sent i.e. returned ENGINE_WANT_MORE
    EXPECT_EQ(ENGINE_WANT_MORE, ret)
        << "maybeSendNoop not returning ENGINE_WANT_MORE";
    EXPECT_TRUE(producer->getNoopPendingRecv())
            << "Not awaiting noop acknowledgement";
    EXPECT_NE(send_time, producer->getNoopSendTime())
            << "SendTime has not been updated";
    ret = producer->maybeSendNoop(producers.get());
    // Check to see if a noop was not sent i.e. returned ENGINE_FAILED
    EXPECT_EQ(ENGINE_FAILED, ret)
        << "maybeSendNoop not returning ENGINE_FAILED";
    producer->setLastReceiveTime(send_time);
    ret = producer->maybeDisconnect();
    // Check to see if we want to disconnect i.e. returned ENGINE_DISCONNECT
    EXPECT_EQ(ENGINE_DISCONNECT, ret)
        << "maybeDisconnect not returning ENGINE_DISCONNECT";
    producer->setLastReceiveTime(
            send_time + engine->getConfiguration().getDcpIdleTimeout() + 1);
    ret = producer->maybeDisconnect();
    // Check to see if we don't want to disconnect i.e. returned ENGINE_FAILED
    EXPECT_EQ(ENGINE_FAILED, ret)
        << "maybeDisconnect not returning ENGINE_FAILED";
    EXPECT_TRUE(producer->getNoopPendingRecv())
            << "Not waiting for noop acknowledgement";
    producer->cancelCheckpointCreatorTask();
    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, test_maybesendnoop_not_enabled) {
    const void* cookie = create_mock_cookie();
    // Create a Mock Dcp producer
    auto producer = std::make_shared<MockDcpProducer>(
            *engine,
            cookie,
            "test_producer",
            /*flags*/ 0,
            cb::const_byte_buffer() /*no json*/);

    std::unique_ptr<dcp_message_producers> producers(get_dcp_producers(handle, engine_v1));
    producer->setNoopEnabled(false);
    const auto send_time = ep_current_time() + 21;
    producer->setNoopSendTime(send_time);
    ENGINE_ERROR_CODE ret = producer->maybeSendNoop(producers.get());
    EXPECT_EQ(ENGINE_FAILED, ret)
    << "maybeSendNoop not returning ENGINE_FAILED";
    EXPECT_FALSE(producer->getNoopPendingRecv())
            << "Waiting for noop acknowledgement";
    EXPECT_EQ(send_time, producer->getNoopSendTime())
            << "SendTime has been updated";
    producer->cancelCheckpointCreatorTask();
    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, test_maybesendnoop_not_sufficient_time_passed) {
    const void* cookie = create_mock_cookie();
    // Create a Mock Dcp producer
    auto producer = std::make_shared<MockDcpProducer>(
            *engine,
            cookie,
            "test_producer",
            /*flags*/ 0,
            cb::const_byte_buffer() /*no json*/);

    std::unique_ptr<dcp_message_producers> producers(get_dcp_producers(handle, engine_v1));
    producer->setNoopEnabled(true);
    rel_time_t current_time = ep_current_time();
    producer->setNoopSendTime(current_time);
    ENGINE_ERROR_CODE ret = producer->maybeSendNoop(producers.get());
    EXPECT_EQ(ENGINE_FAILED, ret)
    << "maybeSendNoop not returning ENGINE_FAILED";
    EXPECT_FALSE(producer->getNoopPendingRecv())
            << "Waiting for noop acknowledgement";
    EXPECT_EQ(current_time, producer->getNoopSendTime())
            << "SendTime has been incremented";
    producer->cancelCheckpointCreatorTask();
    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, test_deadConnections) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize();
    const void *cookie = create_mock_cookie();
    // Create a new Dcp producer
    connMap.newProducer(cookie,
                        "test_producer",
                        /*flags*/ 0);

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
    connMap.newProducer(cookie,
                        "test_producer",
                        /*flags*/ 0);
    // should be able to find the connection
    ASSERT_NE(nullptr, connMap.findByName("eq_dcpq:test_producer"));
    // Disconnect the producer connection
    connMap.disconnect(cookie);
    ASSERT_EQ(1, connMap.getNumberOfDeadConnections())
        << "Unexpected number of dead connections";
    // should not be able to find because the connection has been marked as
    // wanting to disconnect
    EXPECT_EQ(nullptr, connMap.findByName("eq_dcpq:test_producer"));
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
    DcpProducer* producer = connMap.newProducer(cookie1,
                                                "test_producer",
                                                /*flags*/ 0);
    ASSERT_NE(nullptr, producer) << "producer is null";
    // should be able to find the connection
    ASSERT_NE(nullptr, connMap.findByName("eq_dcpq:test_producer"));

    // Create a duplicate Dcp producer
    DcpProducer* duplicateproducer =
            connMap.newProducer(cookie2, "test_producer", /*flags*/ 0);
    ASSERT_TRUE(producer->doDisconnect()) << "producer doDisconnect == false";
    ASSERT_NE(nullptr, duplicateproducer) << "duplicateproducer is null";

    // should find the duplicateproducer as the first producer has been marked
    // as wanting to disconnect
    EXPECT_EQ(duplicateproducer,
              connMap.findByName("eq_dcpq:test_producer").get());

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
    DcpProducer* producer = connMap.newProducer(cookie1,
                                                "test_producer",
                                                /*flags*/ 0);
    EXPECT_NE(nullptr, producer) << "producer is null";

    // Create a duplicate Dcp producer
    DcpProducer* duplicateproducer = connMap.newProducer(cookie2,
                                                         "test_producer",
                                                         /*flags*/ 0);
    EXPECT_TRUE(producer->doDisconnect()) << "producer doDisconnect == false";
    EXPECT_NE(nullptr, duplicateproducer) << "duplicateproducer is null";

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
    DcpConsumer* consumer = connMap.newConsumer(cookie1, "test_consumer");
    EXPECT_NE(nullptr, consumer) << "consumer is null";

    // Create a duplicate Dcp consumer
    DcpConsumer* duplicateconsumer =
            connMap.newConsumer(cookie2, "test_consumer");
    EXPECT_TRUE(consumer->doDisconnect()) << "consumer doDisconnect == false";
    EXPECT_NE(nullptr, duplicateconsumer) << "duplicateconsumer is null";

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
    DcpProducer* producer = connMap.newProducer(cookie,
                                                "test_producer1",
                                                /*flags*/ 0);

    // Create a duplicate Dcp producer
    DcpProducer* duplicateproducer = connMap.newProducer(cookie,
                                                         "test_producer2",
                                                         /*flags*/ 0);

    EXPECT_TRUE(producer->doDisconnect()) << "producer doDisconnect == false";
    EXPECT_EQ(nullptr, duplicateproducer) << "duplicateproducer is not null";

    // Disconnect the producer connection
    connMap.disconnect(cookie);
    // Cleanup the deadConnections
    connMap.manageConnections();
    // Should be zero deadConnections
    EXPECT_EQ(0, connMap.getNumberOfDeadConnections())
        << "Dead connections still remain";
}

/* Checks that the DCP producer does an async stream close when the DCP client
   expects "DCP_STREAM_END" msg. */
TEST_P(ConnectionTest, test_producer_stream_end_on_client_close_stream) {
#ifdef UNDEFINED_SANITIZER
    // See below MB-28739 comment for why this is skipped.
    std::cerr << "MB-28739[UBsan] skipping test\n";
    return;
#endif
    const void* cookie = create_mock_cookie();
    /* Create a new Dcp producer */
    auto producer = std::make_shared<MockDcpProducer>(
            *engine,
            cookie,
            "test_producer",
            /*flags*/ 0,
            cb::const_byte_buffer() /*no json*/);

    /* Send a control message to the producer indicating that the DCP client
       expects a "DCP_STREAM_END" upon stream close */
    const std::string sendStreamEndOnClientStreamCloseCtrlMsg(
            "send_stream_end_on_client_close_stream");
    const std::string sendStreamEndOnClientStreamCloseCtrlValue("true");
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->control(
                      0,
                      sendStreamEndOnClientStreamCloseCtrlMsg.c_str(),
                      sendStreamEndOnClientStreamCloseCtrlMsg.size(),
                      sendStreamEndOnClientStreamCloseCtrlValue.c_str(),
                      sendStreamEndOnClientStreamCloseCtrlValue.size()));

    /* Open stream */
    uint64_t rollbackSeqno = 0;
    uint32_t opaque = 0;
    const uint16_t vbid = 0;
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(/*flags*/ 0,
                                      opaque,
                                      vbid,
                                      /*start_seqno*/ 0,
                                      /*end_seqno*/ ~0,
                                      /*vb_uuid*/ 0,
                                      /*snap_start*/ 0,
                                      /*snap_end*/ 0,
                                      &rollbackSeqno,
                                      DCPTest::fakeDcpAddFailoverLog));

    // MB-28739[UBSan]: The following cast is undefined behaviour - the DCP
    // connection map object is of type DcpConnMap; so it's undefined to cast
    // to MockDcpConnMap.
    // However, in this instance MockDcpConnMap has identical member variables
    // to DcpConnMap - the mock just exposes normally private data - and so
    // this /seems/ ok.
    // As such allow it in general, but skip this test under UBSan.
    MockDcpConnMap& mockConnMap =
            static_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    mockConnMap.addConn(cookie, producer);
    EXPECT_TRUE(mockConnMap.doesConnHandlerExist(vbid, "test_producer"));

    /* Close stream */
    EXPECT_EQ(ENGINE_SUCCESS, producer->closeStream(opaque, vbid));

    /* Expect a stream end message */
    std::unique_ptr<dcp_message_producers> fakeProducers(
            get_dcp_producers(handle, engine_v1));
    EXPECT_EQ(ENGINE_WANT_MORE, producer->step(fakeProducers.get()));
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_STREAM_END, dcp_last_op);
    EXPECT_EQ(END_STREAM_CLOSED, dcp_last_flags);

    /* Re-open stream for the same vbucket on the conn */
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(/*flags*/ 0,
                                      opaque,
                                      vbid,
                                      /*start_seqno*/ 0,
                                      /*end_seqno*/ ~0,
                                      /*vb_uuid*/ 0,
                                      /*snap_start*/ 0,
                                      /*snap_end*/ 0,
                                      &rollbackSeqno,
                                      DCPTest::fakeDcpAddFailoverLog));

    /* Check that the new stream is opened properly */
    auto stream = producer->findStream(vbid);
    EXPECT_TRUE(stream->isInMemory());

    // MB-27769: Prior to the fix, this would fail here because we would skip
    // adding the connhandler into the connmap vbConns vector, causing the
    // stream to never get notified.
    EXPECT_TRUE(mockConnMap.doesConnHandlerExist(vbid, "test_producer"));

    mockConnMap.disconnect(cookie);
    EXPECT_FALSE(mockConnMap.doesConnHandlerExist(vbid, "test_producer"));
    mockConnMap.manageConnections();
}

/* Checks that the DCP producer does a synchronous stream close when the DCP
   client does not expect "DCP_STREAM_END" msg. */
TEST_P(ConnectionTest, test_producer_no_stream_end_on_client_close_stream) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize();
    const void* cookie = create_mock_cookie();

    /* Create a new Dcp producer */
    DcpProducer* producer = connMap.newProducer(cookie,
                                                "test_producer",
                                                /*flags*/ 0);

    /* Open stream */
    uint64_t rollbackSeqno = 0;
    uint32_t opaque = 0;
    const uint16_t vbid = 0;
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(/*flags*/ 0,
                                      opaque,
                                      vbid,
                                      /*start_seqno*/ 0,
                                      /*end_seqno*/ ~0,
                                      /*vb_uuid*/ 0,
                                      /*snap_start*/ 0,
                                      /*snap_end*/ 0,
                                      &rollbackSeqno,
                                      DCPTest::fakeDcpAddFailoverLog));

    /* Close stream */
    EXPECT_EQ(ENGINE_SUCCESS, producer->closeStream(opaque, vbid));

    /* Don't expect a stream end message (or any other message as the stream is
       closed) */
    std::unique_ptr<dcp_message_producers> fakeProducers(
            get_dcp_producers(handle, engine_v1));
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(fakeProducers.get()));

    /* Check that the stream is not found in the producer's stream map */
    EXPECT_FALSE(producer->findStream(vbid));

    /* Disconnect the producer connection */
    connMap.disconnect(cookie);
    /* Cleanup the deadConnections */
    connMap.manageConnections();
}

TEST_P(ConnectionTest, test_producer_unknown_ctrl_msg) {
    const void* cookie = create_mock_cookie();
    /* Create a new Dcp producer */
    auto producer = std::make_shared<MockDcpProducer>(
            *engine,
            cookie,
            "test_producer",
            /*flags*/ 0,
            cb::const_byte_buffer() /*no json*/);

    /* Send an unkown control message to the producer and expect an error code
       of "ENGINE_EINVAL" */
    const std::string unkownCtrlMsg("unknown");
    const std::string unkownCtrlValue("blah");
    EXPECT_EQ(ENGINE_EINVAL,
              producer->control(0,
                                unkownCtrlMsg.c_str(),
                                unkownCtrlMsg.size(),
                                unkownCtrlValue.c_str(),
                                unkownCtrlValue.size()));
    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, test_mb17042_duplicate_cookie_consumer_connections) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize();
    const void* cookie = create_mock_cookie();
    // Create a new Dcp consumer
    DcpConsumer* consumer = connMap.newConsumer(cookie, "test_consumer1");

    // Create a duplicate Dcp consumer
    DcpConsumer* duplicateconsumer =
            connMap.newConsumer(cookie, "test_consumer2");
    EXPECT_TRUE(consumer->doDisconnect()) << "consumer doDisconnect == false";
    EXPECT_EQ(nullptr, duplicateconsumer) << "duplicateconsumer is not null";

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
    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test_consumer");
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

    /* Create a Mock Dcp consumer */
    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test_consumer");

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

TEST_P(ConnectionTest, consumer_get_error_map) {
    // We want to test that the Consumer processes the GetErrorMap negotiation
    // with the Producer correctly. I.e., the Consumer must check the
    // Producer's version and set internal flags accordingly.
    // Note: we test both the cases of pre-5.0.0 and post-5.0.0 Producer
    for (auto prodIsV5orHigher : {true, false}) {
        const void* cookie = create_mock_cookie();
        // GetErrorMap negotiation performed only if NOOP is enabled
        engine->getConfiguration().setDcpEnableNoop(true);
        auto producers = get_dcp_producers(handle, engine_v1);

        // Create a mock DcpConsumer
        MockDcpConsumer consumer(*engine, cookie, "test_consumer");
        ASSERT_EQ(1 /*PendingRequest*/,
                  static_cast<uint8_t>(consumer.getGetErrorMapState()));
        ASSERT_EQ(false, consumer.getProducerIsVersion5orHigher());

        // If a Flow Control Policy is enabled, then the first call to step()
        // will handle the Flow Control negotiation. We do not want to test that
        // here, so this is just to let the test to work with all EP
        // configurations.
        if (engine->getConfiguration().getDcpFlowControlPolicy() != "none") {
            ASSERT_EQ(ENGINE_WANT_MORE, consumer.step(producers.get()));
        }

        // The next call to step() is expected to start the GetErrorMap
        // negotiation
        ASSERT_EQ(ENGINE_WANT_MORE, consumer.step(producers.get()));
        ASSERT_EQ(2 /*PendingResponse*/,
                  static_cast<uint8_t>(consumer.getGetErrorMapState()));

        // At this point the consumer is waiting for a response from the
        // producer. I simulate the producer's response with a call to
        // handleResponse()
        protocol_binary_response_header resp{};
        resp.response.opcode = PROTOCOL_BINARY_CMD_GET_ERROR_MAP;
        resp.response.status =
                prodIsV5orHigher
                        ? htons(PROTOCOL_BINARY_RESPONSE_SUCCESS)
                        : htons(PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND);
        ASSERT_TRUE(consumer.handleResponse(&resp));
        ASSERT_EQ(0 /*Skip*/,
                  static_cast<uint8_t>(consumer.getGetErrorMapState()));
        ASSERT_EQ(prodIsV5orHigher ? true : false,
                  consumer.getProducerIsVersion5orHigher());

        destroy_mock_cookie(cookie);
    }
}

// Regression test for MB 20645 - ensure that a call to addStats after a
// connection has been disconnected (and closeAllStreams called) doesn't crash.
TEST_P(ConnectionTest, test_mb20645_stats_after_closeAllStreams) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize();
    const void *cookie = create_mock_cookie();
    // Create a new Dcp producer
    DcpProducer* producer = connMap.newProducer(cookie,
                                                "test_producer",
                                                /*flags*/ 0);

    // Disconnect the producer connection
    connMap.disconnect(cookie);

    // Try to read stats. Shouldn't crash.
    producer->addStats([](const char* key,
                          const uint16_t klen,
                          const char* val,
                          const uint32_t vlen,
                          gsl::not_null<const void*> cookie) {},
                       // Cookie is not being used in the callback, but the
                       // API requires it. Pass in the producer as cookie
                       static_cast<const void*>(producer));

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
    DcpProducer* producer = connMap.newProducer(cookie,
                                                "mb_20716r",
                                                /*flags*/ 0);

    // Check preconditions.
    EXPECT_TRUE(producer->isPaused());

    // Hook into notify_io_complete.
    // We (ab)use the engine_specific API to pass a pointer to a count of
    // how many times notify_io_complete has been called.
    size_t notify_count = 0;
    SERVER_COOKIE_API* scapi = get_mock_server_api()->cookie;
    scapi->store_engine_specific(cookie, &notify_count);
    auto orig_notify_io_complete = scapi->notify_io_complete;
    scapi->notify_io_complete = [](gsl::not_null<const void*> cookie,
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
    // Create a new Dcp consumer
    DcpConsumer* consumer = connMap.newConsumer(cookie, "mb_20716_consumer");

    // Move consumer into paused state (aka EWOULDBLOCK).
    std::unique_ptr<dcp_message_producers> producers(
            get_dcp_producers(handle, engine_v1));
    ENGINE_ERROR_CODE result;
    do {
        result = consumer->step(producers.get());
        handleProducerResponseIfStepBlocked(*consumer);
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
    scapi->notify_io_complete = [](gsl::not_null<const void*> cookie,
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
    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test_consumer");

    // Add passive stream
    ASSERT_EQ(ENGINE_SUCCESS, consumer->addStream(/*opaque*/0, vbid,
                                                  /*flags*/0));
    // Get the checkpointManager
    auto& manager =
            *(engine->getKVBucket()->getVBucket(vbid)->checkpointManager);

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

class DcpConnMapTest : public ::testing::Test {
protected:
    void SetUp() override {
        /* Set up the bare minimum stuff needed by the 'SynchronousEPEngine'
           (mock engine) */
        ObjectRegistry::onSwitchThread(&engine);
        engine.setKVBucket(engine.public_makeBucket(engine.getConfiguration()));
        engine.public_initializeEngineCallbacks();
        initialize_time_functions(get_mock_server_api()->core);

        /* Set up one vbucket in the bucket */
        engine.getKVBucket()->setVBucketState(
                vbid, vbucket_state_active, false);
    }

    void TearDown() override {
        destroy_mock_event_callbacks();
        ObjectRegistry::onSwitchThread(nullptr);
    }

    /**
     * Fake callback emulating dcp_add_failover_log
     */
    static ENGINE_ERROR_CODE fakeDcpAddFailoverLog(
            vbucket_failover_t* entry,
            size_t nentries,
            gsl::not_null<const void*> cookie) {
        return ENGINE_SUCCESS;
    }

    SynchronousEPEngine engine;
    const uint16_t vbid = 0;
};

/* Tests that there is no memory loss due to cyclic reference between connection
 * and other objects (like dcp streams). It is possible that connections are
 * deleted from the dcp connmap when dcp connmap is deleted due to abrupt
 * deletion of 'EventuallyPersistentEngine' obj.
 * This test simulates the abrupt deletion of dcp connmap object
 */
TEST_F(DcpConnMapTest, DeleteProducerOnUncleanDCPConnMapDelete) {
    /* Create a new Dcp producer */
    const void* dummyMockCookie = create_mock_cookie();
    DcpProducer* producer = engine.getDcpConnMap().newProducer(dummyMockCookie,
                                                               "test_producer",
                                                               /*flags*/ 0);
    /* Open stream */
    uint64_t rollbackSeqno = 0;
    uint32_t opaque = 0;
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(/*flags*/ 0,
                                      opaque,
                                      vbid,
                                      /*start_seqno*/ 0,
                                      /*end_seqno*/ ~0,
                                      /*vb_uuid*/ 0,
                                      /*snap_start*/ 0,
                                      /*snap_end*/ 0,
                                      &rollbackSeqno,
                                      fakeDcpAddFailoverLog));

    destroy_mock_cookie(dummyMockCookie);

    /* Delete the connmap, connection should be deleted as the owner of
       the connection (connmap) is deleted. Checks that there is no cyclic
       reference between conn (producer) and stream or any other object */
    engine.setDcpConnMap(nullptr);
}

/* Tests that there is no memory loss due to cyclic reference between a
 * notifier connection and a notifier stream.
 */
TEST_F(DcpConnMapTest, DeleteNotifierConnOnUncleanDCPConnMapDelete) {
    /* Create a new Dcp producer */
    const void* dummyMockCookie = create_mock_cookie();
    DcpProducer* producer = engine.getDcpConnMap().newProducer(
            dummyMockCookie, "test_producer", DCP_OPEN_NOTIFIER);
    /* Open notifier stream */
    uint64_t rollbackSeqno = 0;
    uint32_t opaque = 0;
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(/*flags*/ 0,
                                      opaque,
                                      vbid,
                                      /*start_seqno*/ 0,
                                      /*end_seqno*/ ~0,
                                      /*vb_uuid*/ 0,
                                      /*snap_start*/ 0,
                                      /*snap_end*/ 0,
                                      &rollbackSeqno,
                                      fakeDcpAddFailoverLog));

    destroy_mock_cookie(dummyMockCookie);

    /* Delete the connmap, connection should be deleted as the owner of
       the connection (connmap) is deleted. Checks that there is no cyclic
       reference between conn (producer) and stream or any other object */
    engine.setDcpConnMap(nullptr);
}

/* Tests that there is no memory loss due to cyclic reference between a
 * consumer connection and a passive stream.
 */
TEST_F(DcpConnMapTest, DeleteConsumerConnOnUncleanDCPConnMapDelete) {
    /* Consumer stream needs a replica vbucket */
    engine.getKVBucket()->setVBucketState(vbid, vbucket_state_replica, false);

    /* Create a new Dcp consumer */
    const void* dummyMockCookie = create_mock_cookie();
    DcpConsumer* consumer = engine.getDcpConnMap().newConsumer(dummyMockCookie,
                                                               "test_consumer");

    /* Add passive stream */
    ASSERT_EQ(ENGINE_SUCCESS,
              consumer->addStream(/*opaque*/ 0,
                                  vbid,
                                  /*flags*/ 0));

    destroy_mock_cookie(dummyMockCookie);

    /* Delete the connmap, connection should be deleted as the owner of
       the connection (connmap) is deleted. Checks that there is no cyclic
       reference between conn (consumer) and stream or any other object */
    engine.setDcpConnMap(nullptr);
}

class NotifyTest : public DCPTest {
protected:
    void SetUp() {
        // The test is going to replace a server cookie API method, we must
        // be able to undo that
        scookie_api = *get_mock_server_api()->cookie;
        DCPTest::SetUp();
    }

    void TearDown() {
        // Reset the server cookie api for other tests
        *get_mock_server_api()->cookie = scookie_api;
        DCPTest::TearDown();
    }

    SERVER_COOKIE_API scookie_api;
    std::unique_ptr<MockDcpConnMap> connMap;
    DcpProducer* producer;
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
                                        /*flags*/ 0);
    }

    void notify() {
        callbacks++;
        connMap->notifyPausedConnection(producer->shared_from_this(),
                                        /*schedule*/ true);
    }

    int getCallbacks() {
        return callbacks;
    }

    static void dcp_test_notify_io_complete(gsl::not_null<const void*> cookie,
                                            ENGINE_ERROR_CODE status) {
        const auto* notifyTest =
                reinterpret_cast<const ConnMapNotifyTest*>(cookie.get());
        // 3. Call notifyPausedConnection again. We're now interleaved inside
        //    of notifyAllPausedConnections, a second notification should occur.
        const_cast<ConnMapNotifyTest*>(notifyTest)->notify();
    }

    std::unique_ptr<MockDcpConnMap> connMap;
    DcpProducer* producer;

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
    notifyTest.connMap->notifyPausedConnection(
            notifyTest.producer->shared_from_this(),
            /*schedule*/ true);
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
    notifyTest.connMap->notifyPausedConnection(
            notifyTest.producer->shared_from_this(),
            /*schedule*/ true);
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
    notifyTest.connMap->notifyPausedConnection(
            notifyTest.producer->shared_from_this(),
            /*schedule*/ true);
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

    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test_consumer");

    ASSERT_EQ(ENGINE_SUCCESS, set_vb_state(vbid, vbucket_state_replica));
    ASSERT_EQ(ENGINE_SUCCESS, consumer->addStream(/*opaque*/0, vbid,
                                                  /*flags*/0));

    MockPassiveStream *stream = static_cast<MockPassiveStream*>
                                       ((consumer->
                                               getVbucketStream(vbid)).get());
    ASSERT_TRUE(stream->isActive());

    std::string key = "key";
    const DocKey docKey{ reinterpret_cast<const uint8_t*>(key.data()),
        key.size(),
        DocNamespace::DefaultCollection};
    uint8_t extMeta[1] = {uint8_t(PROTOCOL_BINARY_DATATYPE_JSON)};
    cb::const_byte_buffer meta{extMeta, sizeof(uint8_t)};

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

    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test_consumer");

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
    const uint64_t snapStart = 1;
    const uint64_t snapEnd = std::numeric_limits<uint64_t>::max();
    uint64_t bySeqno = snapStart;

    /* Set up a consumer connection */
    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test_consumer");

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
        stats.setMaxDataSize(stats.getPreciseTotalMemoryUsed());
    } else {
        /* Set 'mem_used' just 1 byte less than the 'replication threshold'.
           That is we are below 'replication threshold', but not enough space
           for  the new item */
        stats.setMaxDataSize(stats.getPreciseTotalMemoryUsed() + 1);
        /* Simpler to set the replication threshold to 1 and test, rather than
           testing with maxData = (memUsed / replicationThrottleThreshold);
           that is, we are avoiding a division */
        engine->getConfiguration().setReplicationThrottleThreshold(100);
    }

    if ((engine->getConfiguration().getBucketType() == "ephemeral") &&
        (engine->getConfiguration().getEphemeralFullPolicy()) ==
                "fail_new_data") {
        /* Expect disconnect signal in Ephemeral with "fail_new_data" policy */
        while (1) {
            /* Keep sending items till the memory usage goes above the
               threshold and the connection is disconnected */
            if (ENGINE_DISCONNECT ==
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
        EXPECT_EQ(ENGINE_SUCCESS,
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
    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test_consumer");

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
        stats.setMaxDataSize(stats.getEstimatedTotalMemoryUsed() / 4);
    } else {
        /* set max size to a value just over */
        stats.setMaxDataSize(stats.getEstimatedTotalMemoryUsed() + 1);
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

class ActiveStreamChkptProcessorTaskTest : public SingleThreadedKVBucketTest {
public:
    ActiveStreamChkptProcessorTaskTest() : cookie(create_mock_cookie()) {
    }

    void SetUp() override {
        SingleThreadedKVBucketTest::SetUp();

        /* Start an active vb and add 3 items */
        store->setVBucketState(vbid, vbucket_state_active, false);
        addItems(3);

        producers = get_dcp_producers(
                reinterpret_cast<ENGINE_HANDLE*>(engine.get()),
                reinterpret_cast<ENGINE_HANDLE_V1*>(engine.get()));
        producer = std::make_shared<MockDcpProducer>(
                *engine,
                cookie,
                "test_producer",
                0 /*flags*/,
                cb::const_byte_buffer() /*no json*/,
                false /*startTask*/);

        /* Create the checkpoint processor task object, but don't schedule */
        producer->createCheckpointProcessorTask();
    }

    void TearDown() override {
        producer->cancelCheckpointCreatorTask();
        producer->closeAllStreams();
        destroy_mock_cookie(cookie);
        SingleThreadedKVBucketTest::TearDown();
    }

    void addItems(int numItems) {
        for (int i = 0; i < numItems; ++i) {
            std::string key("key" + std::to_string(i));
            store_item(vbid, makeStoredDocKey(key), "value");
        }
    }

    /*
     * Fake callback emulating dcp_add_failover_log
     */
    static ENGINE_ERROR_CODE fakeDcpAddFailoverLog(
            vbucket_failover_t* entry,
            size_t nentries,
            gsl::not_null<const void*> cookie) {
        return ENGINE_SUCCESS;
    }

    void notifyAndStepToCheckpoint() {
        SingleThreadedKVBucketTest::notifyAndStepToCheckpoint(*producer,
                                                              *producers);
    }

    const void* cookie;
    std::unique_ptr<dcp_message_producers> producers;
    std::shared_ptr<MockDcpProducer> producer;
    const int vbid = 0;
};

TEST_F(ActiveStreamChkptProcessorTaskTest, DeleteDeadStreamEntry) {
    uint64_t rollbackSeqno;
    uint32_t opaque = 1;
    ASSERT_EQ(
            ENGINE_SUCCESS,
            producer->streamRequest(
                    0, // flags
                    opaque,
                    vbid,
                    0, // start_seqno
                    ~0ull, // end_seqno
                    0, // vbucket_uuid,
                    0, // snap_start_seqno,
                    0, // snap_end_seqno,
                    &rollbackSeqno,
                    ActiveStreamChkptProcessorTaskTest::fakeDcpAddFailoverLog));
    /* Checkpoint task processor Q will already have any entry for the stream */
    EXPECT_EQ(1, producer->getCheckpointSnapshotTask().queueSize());

    /* Close and open the stream without clearing the checkpoint task processor
     Q */
    producer->closeStream(opaque, vbid);
    ASSERT_EQ(
            ENGINE_SUCCESS,
            producer->streamRequest(
                    0, // flags
                    opaque,
                    vbid,
                    0, // start_seqno
                    ~0ull, // end_seqno
                    0, // vbucket_uuid,
                    0, // snap_start_seqno,
                    0, // snap_end_seqno,
                    &rollbackSeqno,
                    ActiveStreamChkptProcessorTaskTest::fakeDcpAddFailoverLog));

    /* The checkpoint processor Q should be processed with the new stream
     getting the item(s) */
    notifyAndStepToCheckpoint();
}

// Test cases which run in both Full and Value eviction
INSTANTIATE_TEST_CASE_P(PersistentAndEphemeral,
                        StreamTest,
                        ::testing::Values("persistent", "ephemeral"),
                        [](const ::testing::TestParamInfo<std::string>& info) {
                            return info.param;
                        });

struct PrintToStringCombinedNameXattrOnOff {
    std::string operator()(
            const ::testing::TestParamInfo<::testing::tuple<std::string, bool>>&
                    info) const {
        if (::testing::get<1>(info.param)) {
            return ::testing::get<0>(info.param) + "_xattr";
        }
        return ::testing::get<0>(info.param);
    }
};

// Test cases which run in both Full and Value eviction
INSTANTIATE_TEST_CASE_P(CompressionStreamTest,
                        CompressionStreamTest,
                        ::testing::Combine(::testing::Values("persistent",
                                                             "ephemeral"),
                                           ::testing::Bool()),
                        PrintToStringCombinedNameXattrOnOff());

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

/*
 * Test fixture for single-threaded Stream tests
 */
class SingleThreadedStreamTest : public SingleThreadedEPBucketTest {
public:
    void SetUp() override {
        // Bucket Quota 100MB, Replication Threshold 10%
        config_string += "max_size=104857600;replication_throttle_threshold=4";
        SingleThreadedEPBucketTest::SetUp();
    }

    enum class mb_33773Mode {
        closeStreamOnTask,
        closeStreamBeforeTask,
        noMemory,
        noMemoryAndClosed
    };
    void mb_33773(mb_33773Mode mode);
};

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
TEST_F(SingleThreadedStreamTest, MB31410) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test_consumer");

    uint32_t opaque = 0;

    ASSERT_EQ(ENGINE_SUCCESS, consumer->addStream(opaque, vbid, 0 /*flags*/));

    auto* passiveStream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    ASSERT_TRUE(passiveStream->isActive());

    const std::string value(1024 * 1024, 'x');
    const uint64_t snapStart = 1;
    const uint64_t snapEnd = 100;

    // The consumer receives the snapshot-marker
    SnapshotMarker snapshotMarker(opaque,
                                  vbid,
                                  snapStart,
                                  snapEnd,
                                  dcp_marker_flag_t::MARKER_FLAG_MEMORY);
    passiveStream->processMarker(&snapshotMarker);

    // The consumer receives mutations.
    // Here I want to create the scenario where we have hit the replication
    // threshold.
    size_t seqno = snapStart;
    for (; seqno <= snapEnd; seqno++) {
        auto ret = passiveStream->messageReceived(
                makeMutation(seqno, vbid, value, opaque));

        // We get ENGINE_TMPFAIL when we hit the replication threshold.
        // When it happens, we buffer the mutation for deferred processing
        // in the DcpConsumerTask.
        if (ret == ENGINE_TMPFAIL) {
            auto& epStats = engine->getEpStats();

            ASSERT_GT(epStats.getEstimatedTotalMemoryUsed(),
                      epStats.getMaxDataSize() *
                              epStats.replicationThrottleThreshold);
            ASSERT_EQ(1, passiveStream->getNumBufferItems());
            auto& bufferedMessages = passiveStream->getBufferMessages();
            auto* dcpResponse = bufferedMessages.at(0).get();
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
            ASSERT_EQ(ENGINE_SUCCESS, ret);
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
    auto frontEndTask = [this,
                         passiveStream,
                         nextFrontEndSeqno,
                         &value,
                         opaque,
                         &tg,
                         &sync]() {
        tg.threadUp();
        // If the following check fails it is enough to assert that the test
        // has failed. But, I use EXPECT rather than ASSERT  because, in the
        // case of failure, I want to trigger also the ASSERT_NO_THROW below.
        EXPECT_EQ(ENGINE_TMPFAIL,
                  passiveStream->messageReceived(makeMutation(
                          nextFrontEndSeqno, vbid, value, opaque)));
        // I cannot check the status of the buffer here because we have released
        // buffer.bufMutex and the DcpConsumerTask has started draining.
        // That would give TSan errors on CV. I do the check in the
        // DcpConsumerTask (below).

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
    // previously buffered. So, if frontEndThread has got ENGINE_SUCCESS above,
    // then this_thread will throw an exception (Monotonic<x> invariant failed).
    std::set<int64_t> processedBufferSeqnos;
    bool isFirstRun = true;
    std::function<void()> hook = [&tg,
                                  passiveStream,
                                  &isFirstRun,
                                  nextFrontEndSeqno,
                                  &sync]() {
        // If the test succeeds (i.e., the frontEndTask above sees
        // ENGINE_TMPFAIL) we will have 2 buffered messages, so we will
        // execute here twice. Calling tg.threadUp again would lead to
        // deadlock.
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
            auto numBufferedItems = passiveStream->getNumBufferItems();
            // Again, avoid that we fail with ASSERT_EQ or
            // std::out_of_range so that this_thread proceeds and
            // throws.
            EXPECT_EQ(2, numBufferedItems);
            if (numBufferedItems == 2) {
                auto& bufferedMessages = passiveStream->getBufferMessages();
                auto* dcpResponse = bufferedMessages.at(0).get();
                EXPECT_EQ(nullptr, dcpResponse);
                dcpResponse = bufferedMessages.at(1).get();
                EXPECT_EQ(nextFrontEndSeqno,
                          *dynamic_cast<MutationResponse&>(*dcpResponse)
                                   .getBySeqno());
            }

            isFirstRun = false;
        }
    };
    passiveStream->setProcessBufferedMessages_postFront_Hook(hook);

    // If the seqno-invariant is broken, the next call throws:
    //     C++ exception with description "Monotonic<x> invariant failed:
    //     new value (<seqno>) breaks invariant on current value
    //     (<nextFrontEndSeqno>)" thrown in the test body.
    uint32_t bytesProcessed{0};
    ASSERT_NO_THROW(EXPECT_EQ(all_processed,
                              passiveStream->processBufferedMessages(
                                      bytesProcessed, 100 /*batchSize*/)));
    EXPECT_GT(bytesProcessed, 0);

    frontEndThread.join();

    // Explicitly verify the order of mutations in the CheckpointManager.
    auto vb = store->getVBuckets().getBucket(vbid);
    auto* ckptMgr = vb->checkpointManager.get();
    ASSERT_TRUE(ckptMgr);
    std::vector<queued_item> items;
    ckptMgr->getItemsForCursor(
            CheckpointManager::pCursorName, items, 100 /*limit*/);
    // Note: I expect only items (no metaitems) because we have  only 1
    // checkpoint and the cursor was at checkpoint-start before moving
    EXPECT_EQ(1, ckptMgr->getNumCheckpoints());
    EXPECT_EQ(nextFrontEndSeqno, items.size());
    uint64_t prevSeqno = 0;
    for (auto& item : items) {
        ASSERT_EQ(queue_op::mutation, item->getOperation());
        EXPECT_GT(item->getBySeqno(), prevSeqno);
        prevSeqno = item->getBySeqno();
    }

    // Cleanup
    ASSERT_EQ(ENGINE_SUCCESS, consumer->closeStream(opaque, vbid));
}

// Main test code for MB-33773, see TEST_F for details of each mode.
// The test generally forces the consumer to buffer mutations and then
// interleaves various operations using ProcessBufferedMessages_postFront_Hook
void SingleThreadedStreamTest::mb_33773(
        SingleThreadedStreamTest::mb_33773Mode mode) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "MB_33773");

    uint32_t opaque = 0;

    ASSERT_EQ(ENGINE_SUCCESS, consumer->addStream(opaque, vbid, 0 /*flags*/));
    opaque++;

    auto* passiveStream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    ASSERT_TRUE(passiveStream->isActive());

    const uint64_t snapStart = 1;
    const uint64_t snapEnd = 100;

    // The consumer receives the snapshot-marker
    SnapshotMarker snapshotMarker(opaque,
                                  vbid,
                                  snapStart,
                                  snapEnd,
                                  dcp_marker_flag_t::MARKER_FLAG_MEMORY);
    passiveStream->processMarker(&snapshotMarker);

    // This code is tricking the replication throttle into returning pause so
    // that the mutation's are buffered.
    engine->getReplicationThrottle().adjustWriteQueueCap(0);
    const size_t size = engine->getEpStats().getMaxDataSize();
    engine->getEpStats().setMaxDataSize(1);
    ASSERT_EQ(ReplicationThrottle::Status::Pause,
              engine->getReplicationThrottle().getStatus());

    // Push mutations
    EXPECT_EQ(0, passiveStream->getNumBufferItems());
    for (size_t seqno = snapStart; seqno < snapEnd; seqno++) {
        EXPECT_EQ(ENGINE_SUCCESS,
                  consumer->mutation(
                          opaque,
                          makeStoredDocKey("k" + std::to_string(seqno)),
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
    }
    // and check they were buffered.
    ASSERT_EQ(snapEnd - snapStart, passiveStream->getNumBufferItems());
    engine->getEpStats().setMaxDataSize(size); // undo the quota adjustment

    // We expect flowcontrol bytes to increase when the buffered items are
    // discarded.
    auto bytes = consumer->getFlowControl().getFreedBytes();
    auto backoffs = consumer->getNumBackoffs();
    size_t flowControlBytesFreed = 0; // this is used for one test only
    switch (mode) {
    case mb_33773Mode::closeStreamOnTask:{
        // Create and set a hook that will call setDead, the hook executes
        // just after an item has been taken from the buffer
        std::function<void()> hook = [this, consumer]() {
            consumer->closeStreamDueToVbStateChange(vbid, vbucket_state_active);
        };
        passiveStream->setProcessBufferedMessages_postFront_Hook(hook);
        break;
    }
    case mb_33773Mode::closeStreamBeforeTask:
        consumer->closeStreamDueToVbStateChange(vbid, vbucket_state_active);
        break;
    case mb_33773Mode::noMemory: {
        // Fudge memory again so the task has to rebuffer the messages
        std::function<void()> hook = [this]() {
            engine->getEpStats().setMaxDataSize(1);
        };
        passiveStream->setProcessBufferedMessages_postFront_Hook(hook);
        break;
    }
    case mb_33773Mode::noMemoryAndClosed: {
        // This hook will force quota to 1 so the processing fails.
        // But also closes the stream so that the messages queue is emptied.
        // We are testing that the item we've moved out of the queue is still
        // accounted in flow-control
        std::function<void()> hook = [this,
                                      consumer,
                                      &flowControlBytesFreed]() {
            engine->getEpStats().setMaxDataSize(1);
            consumer->closeStreamDueToVbStateChange(vbid, vbucket_state_active);
            // Capture flow control freed bytes which should now include all
            // buffered messages, except one (which was moved)
            flowControlBytesFreed = consumer->getFlowControl().getFreedBytes();
        };
        passiveStream->setProcessBufferedMessages_postFront_Hook(hook);
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
        return;
    case mb_33773Mode::noMemory: {
        std::function<void()> hook = [] {};
        passiveStream->setProcessBufferedMessages_postFront_Hook(hook);
        // fall through to next case
    }
    case mb_33773Mode::noMemoryAndClosed: {
        // Undo memory fudge for the rest of the test
        engine->getEpStats().setMaxDataSize(size);
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
    } else {
        // The items are still buffered
        EXPECT_EQ(snapEnd - snapStart, passiveStream->getNumBufferItems());
        // Run task again, it should of re-sheduled itself
        runNextTask(nonIo);
        // and all items now gone
        EXPECT_EQ(0, passiveStream->getNumBufferItems());
    }
}

// Do mb33773 with the close stream interleaved into the processBufferedMessages
// This is more reflective of the actual MB as this case would result in a fault
TEST_F(SingleThreadedStreamTest, MB_33773_interleaved) {
    mb_33773(mb_33773Mode::closeStreamOnTask);
}

// Do mb33773 with the close stream before processBufferedMessages. This is
// checking that flow-control is updated with the fix in place
TEST_F(SingleThreadedStreamTest, MB_33773) {
    mb_33773(mb_33773Mode::closeStreamBeforeTask);
}

// Test more of the changes in mb33773, this mode makes the processing fail
// because there's not enough memory, this makes us exercise the code that swaps
// a reponse back into the deque
TEST_F(SingleThreadedStreamTest, MB_33773_oom) {
    mb_33773(mb_33773Mode::noMemory);
}

// Test more of the changes in mb33773, this mode makes the processing fail
// because there's not enough memory, this makes us exercise the code that swaps
// a reponse back into the deque
TEST_F(SingleThreadedStreamTest, MB_33773_oom_close) {
    mb_33773(mb_33773Mode::noMemoryAndClosed);
}

/*
 * Test fixture for single-threaded ActiveStream tests.
 *
 * Instantiated for both Persistent and Ephemeral buckets.
 */
class SingleThreadedActiveStreamTest : public STParameterizedBucketTest {
protected:
    void SetUp() override;
    void TearDown() override;

    void setupProducer(const std::vector<std::pair<std::string, std::string>>&
                               controls = {});

    std::shared_ptr<MockDcpProducer> producer;
    std::shared_ptr<MockActiveStream> stream;
};

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

void SingleThreadedActiveStreamTest::setupProducer(
        const std::vector<std::pair<std::string, std::string>>& controls) {
    uint32_t flags = 0;

    // We don't set the startTask flag here because we will create the task
    // manually. We do this because the producer actually creates the task on
    // StreamRequest which we do not do because we want a MockActiveStream.
    producer = std::make_shared<MockDcpProducer>(
            *engine,
            cookie,
            "test_producer->test_consumer",
            flags,
            cb::const_byte_buffer() /*no json*/,
            false /*startTask*/);

    for (const auto& c : controls) {
        EXPECT_EQ(ENGINE_SUCCESS,
                  producer->control(0 /*opaque*/,
                                    c.first.c_str(),
                                    c.first.size(),
                                    c.second.c_str(),
                                    c.second.size()));
    }

    auto vb = engine->getVBucket(vbid);

    stream = std::make_shared<MockActiveStream>(engine.get(),
                                                producer,
                                                flags,
                                                0 /*opaque*/,
                                                *vb,
                                                0 /*st_seqno*/,
                                                ~0 /*en_seqno*/,
                                                0x0 /*vb_uuid*/,
                                                0 /*snap_start_seqno*/,
                                                ~0 /*snap_end_seqno*/);

    stream->setActive();
}

/// Test that disk backfill remaining isn't prematurely zero (before counts
/// read from disk by backfill task).
TEST_P(SingleThreadedActiveStreamTest, DiskBackfillInitializingItemsRemaining) {
    auto vb = engine->getVBucket(vbid);
    auto& ckptMgr = *vb->checkpointManager;

    // Delete initial stream (so we can re-create after items are only available
    // from disk.
    stream.reset();

    // Store 3 items (to check backfill remaining counts).
    // Add items, flush it to disk, then clear checkpoint to force backfill.
    store_item(vbid, makeStoredDocKey("key1"), "value");
    store_item(vbid, makeStoredDocKey("key2"), "value");
    store_item(vbid, makeStoredDocKey("key3"), "value");
    ckptMgr.createNewCheckpoint();

    flushVBucketToDiskIfPersistent(vbid, 3);

    bool newCKptCreated;
    ASSERT_EQ(3, ckptMgr.removeClosedUnrefCheckpoints(*vb, newCKptCreated));

    // Re-create producer now we have items only on disk.
    setupProducer();
    ASSERT_TRUE(stream->isBackfilling());

    // Should report empty itemsRemaining as that would mislead
    // ns_server if they asked for stats before the backfill task runs (they
    // would think backfill is complete).
    EXPECT_FALSE(stream->getNumBackfillItemsRemaining());

    struct Cookie : public cb::tracing::Traceable {
        bool statusFound = false;
        std::string str;
    } state;

    auto checkStatusFn = [](const char* key,
                            const uint16_t klen,
                            const char* val,
                            const uint32_t vlen,
                            gsl::not_null<const void*> cookie) {
        if (std::string(key, klen) == "status") {
            Cookie& state =
                    *reinterpret_cast<Cookie*>(const_cast<void*>(cookie.get()));
            EXPECT_EQ(state.str, std::string(val, vlen));
            state.statusFound = true;
        }
    };

    // Should report status == "calculating_item_count" before backfill
    // scan has occurred.
    state.str = "calculating-item-count";
    stream->addTakeoverStats(checkStatusFn, &state, *vb);
    EXPECT_TRUE(state.statusFound);

    // Run the backfill we scheduled when we transitioned to the backfilling
    // state. Run the backfill task once to get initial item counts.
    auto& bfm = producer->getBFM();
    bfm.backfill();
    EXPECT_EQ(3, *stream->getNumBackfillItemsRemaining());
    // Should report status == "backfilling"
    state.str = "backfilling";
    state.statusFound = false;
    stream->addTakeoverStats(checkStatusFn, &state, *vb);
    EXPECT_TRUE(state.statusFound);

    // Run again to actually scan (items remaining unchanged).
    bfm.backfill();
    EXPECT_EQ(3, *stream->getNumBackfillItemsRemaining());
    state.statusFound = false;
    stream->addTakeoverStats(checkStatusFn, &state, *vb);
    EXPECT_TRUE(state.statusFound);

    // Finally run again to complete backfill (so it is shutdown in a clean
    // fashion).
    bfm.backfill();

    // Consume the items from backfill; should update items remaining.
    // Actually need to consume 4 items (snapshot_marker + 3x mutation).
    stream->consumeBackfillItems(4);
    EXPECT_EQ(0, *stream->getNumBackfillItemsRemaining());
    state.str = "in-memory";
    state.statusFound = false;
    stream->addTakeoverStats(checkStatusFn, &state, *vb);
    EXPECT_TRUE(state.statusFound);
}

TEST_P(SingleThreadedActiveStreamTest,
       CursorReregisteredBeforeBackfillAfterCursorDrop) {
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

    producer->setBeforeScheduleBackfillCB([this, &cm](uint64_t backfillEnd) {
        const auto cursorName = this->stream->getCursorName();
        // check cursor exists before backfill is registered
        auto ckptID = cm.getCheckpointIdForCursor(cursorName);
        EXPECT_TRUE(ckptID);

        // check that the cursor was registered immediately after the
        // end of the backfill prior to MB-37150 this could fail as the
        // cursor would be _later_ than backfillEnd+1 as the checkpoint
        // has been removed.
        auto pos = CheckpointCursorIntrospector::getCurrentPos(cm, cursorName);
        EXPECT_EQ(backfillEnd + 1, (*std::next(pos))->getBySeqno());
    });

    auto resp = stream->next();
    EXPECT_FALSE(resp);

    // backfill not needed
    EXPECT_EQ(ActiveStream::StreamState::InMemory, stream->getState());

    EXPECT_EQ(0, stream->public_readyQ().size());

    auto producers = get_dcp_producers(
            reinterpret_cast<ENGINE_HANDLE*>(engine.get()),
            reinterpret_cast<ENGINE_HANDLE_V1*>(engine.get()));
    runCheckpointProcessor(*producer, *producers);

    EXPECT_EQ(4, stream->public_readyQ().size());

    // NB: This first snapshot will actually be _skipped_ as the checkpoint was
    // removed but the active stream did not backfill to "catch up"
    // snap marker
    resp = stream->next();
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    auto snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_EQ(0, snapMarker.getStartSeqno());
    EXPECT_EQ(1, snapMarker.getEndSeqno());

    // receive mutation 1
    resp = stream->next();
    EXPECT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());

    {
        auto& set = dynamic_cast<MutationResponse&>(*resp);
        EXPECT_EQ(key1, set.getItem()->getKey());
        EXPECT_EQ(1, set.getItem()->getBySeqno());
    }

    // snap marker
    resp = stream->next();
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());
    snapMarker = dynamic_cast<SnapshotMarker&>(*resp);
    EXPECT_EQ(2, snapMarker.getStartSeqno());
    EXPECT_EQ(2, snapMarker.getEndSeqno());

    // receive mutation 2
    resp = stream->next();
    EXPECT_TRUE(resp);
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    {
        auto& set = dynamic_cast<MutationResponse&>(*resp);
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
    ckptMgr.createNewCheckpoint();

    flushVBucketToDiskIfPersistent(vbid, 1);
    bool newCKptCreated;
    ASSERT_EQ(1, ckptMgr.removeClosedUnrefCheckpoints(*vb, newCKptCreated));

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

    auto producers = get_dcp_producers(
            reinterpret_cast<ENGINE_HANDLE*>(engine.get()),
            reinterpret_cast<ENGINE_HANDLE_V1*>(engine.get()));

    // Step to schedule our backfill
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(0, stream->public_readyQ().size());

    auto& bfm = producer->getBFM();

    // Ephemeral has a single stage backfill and we only compare about the
    // complete stage so skip over create and scan for persistent buckets
    if (persistent()) {
        bfm.backfill();
        bfm.backfill();
    }

    ThreadGate tg1 (2);
    ThreadGate tg2 (2);
    std::thread t1;
    stream->setCompleteBackfillHook([this, &t1, &tg1, &tg2, &producers]() {
        // Step past our normal items to expose the race with backfill complete
        // and an empty readyQueue.

        EXPECT_EQ(1, *stream->getNumBackfillItemsRemaining());
        EXPECT_EQ(2, stream->public_readyQ().size());

        // Step snapshot marker
        EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));
        EXPECT_EQ((uint8_t)cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                dcp_last_op);

        // Step mutation
        EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));
        EXPECT_EQ((uint8_t)cb::mcbp::ClientOpcode::DcpMutation, dcp_last_op);

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
            EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));
            EXPECT_EQ((uint8_t)cb::mcbp::ClientOpcode::DcpStreamEnd, dcp_last_op);
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

    // Should have sent StreamEnd but stream still in queue
    EXPECT_FALSE(producer->findStream(vbid)->isActive());
    EXPECT_FALSE(producer->getReadyQueue().empty());

    // Step to remove stream from queue
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_FALSE(producer->findStream(vbid)->isActive());
    EXPECT_TRUE(producer->getReadyQueue().empty());
}

INSTANTIATE_TEST_CASE_P(AllBucketTypes,
                        SingleThreadedActiveStreamTest,
                        allConfigValues, );
