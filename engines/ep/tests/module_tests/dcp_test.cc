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

#include "dcp_test.h"
#include "../mock/mock_checkpoint_manager.h"
#include "../mock/mock_dcp.h"
#include "../mock/mock_dcp_conn_map.h"
#include "../mock/mock_dcp_consumer.h"
#include "../mock/mock_dcp_producer.h"
#include "../mock/mock_stream.h"
#include "../mock/mock_synchronous_ep_engine.h"
#include "checkpoint.h"
#include "checkpoint_manager.h"
#include "checkpoint_utils.h"
#include "dcp/active_stream_checkpoint_processor_task.h"
#include "dcp/dcp-types.h"
#include "dcp/dcpconnmap.h"
#include "dcp/producer.h"
#include "dcp/response.h"
#include "dcp/stream.h"
#include "dcp_utils.h"
#include "ep_time.h"
#include "evp_engine_test.h"
#include "kv_bucket.h"
#include "memory_tracker.h"
#include "objectregistry.h"
#include "test_helpers.h"
#include "vbucket.h"
#include "warmup.h"

#include <folly/portability/GTest.h>
#include <memcached/server_cookie_iface.h>
#include <platform/cbassert.h>
#include <platform/compress.h>
#include <programs/engine_testapp/mock_server.h>

#include <thread>

/**
 * The DCP tests wants to mock around with the notify_io_complete
 * method. Previously we copied in a new notify_io_complete method, but
 * we can't do that as the cookie interface contains virtual pointers.
 * An easier approach is to create a class which just wraps the server
 * API and we may subclass this class to override whatever method we want
 *
 * The constructor installs itself as the mock server cookie interface,
 * and the destructor reinstalls the original server cookie interfa
 */
class WrappedServerCookieIface : public ServerCookieIface {
public:
    WrappedServerCookieIface() : wrapped(get_mock_server_api()->cookie) {
        get_mock_server_api()->cookie = this;
    }

    ~WrappedServerCookieIface() override {
        get_mock_server_api()->cookie = wrapped;
    }

    void store_engine_specific(gsl::not_null<const void*> cookie,
                               void* engine_data) override {
        wrapped->store_engine_specific(cookie, engine_data);
    }
    void* get_engine_specific(gsl::not_null<const void*> cookie) override {
        return wrapped->get_engine_specific(cookie);
    }
    bool is_datatype_supported(gsl::not_null<const void*> cookie,
                               protocol_binary_datatype_t datatype) override {
        return wrapped->is_datatype_supported(cookie, datatype);
    }
    bool is_mutation_extras_supported(
            gsl::not_null<const void*> cookie) override {
        return wrapped->is_mutation_extras_supported(cookie);
    }
    bool is_collections_supported(gsl::not_null<const void*> cookie) override {
        return wrapped->is_collections_supported(cookie);
    }
    cb::mcbp::ClientOpcode get_opcode_if_ewouldblock_set(
            gsl::not_null<const void*> cookie) override {
        return wrapped->get_opcode_if_ewouldblock_set(cookie);
    }
    bool validate_session_cas(uint64_t cas) override {
        return wrapped->validate_session_cas(cas);
    }
    void decrement_session_ctr() override {
        return wrapped->decrement_session_ctr();
    }
    void notify_io_complete(gsl::not_null<const void*> cookie,
                            ENGINE_ERROR_CODE status) override {
        return wrapped->notify_io_complete(cookie, status);
    }
    ENGINE_ERROR_CODE reserve(gsl::not_null<const void*> cookie) override {
        return wrapped->reserve(cookie);
    }
    ENGINE_ERROR_CODE release(gsl::not_null<const void*> cookie) override {
        return wrapped->release(cookie);
    }
    void set_priority(gsl::not_null<const void*> cookie,
                      CONN_PRIORITY priority) override {
        return wrapped->set_priority(cookie, priority);
    }
    CONN_PRIORITY get_priority(gsl::not_null<const void*> cookie) override {
        return wrapped->get_priority(cookie);
    }
    bucket_id_t get_bucket_id(gsl::not_null<const void*> cookie) override {
        return wrapped->get_bucket_id(cookie);
    }
    uint64_t get_connection_id(gsl::not_null<const void*> cookie) override {
        return wrapped->get_connection_id(cookie);
    }
    cb::rbac::PrivilegeAccess check_privilege(
            gsl::not_null<const void*> cookie,
            cb::rbac::Privilege privilege) override {
        return wrapped->check_privilege(cookie, privilege);
    }
    cb::mcbp::Status engine_error2mcbp(gsl::not_null<const void*> cookie,
                                       ENGINE_ERROR_CODE code) override {
        return wrapped->engine_error2mcbp(cookie, code);
    }
    std::pair<uint32_t, std::string> get_log_info(
            gsl::not_null<const void*> cookie) override {
        return wrapped->get_log_info(cookie);
    }
    std::string get_authenticated_user(
            gsl::not_null<const void*> cookie) override {
        return wrapped->get_authenticated_user(cookie);
    }
    in_port_t get_connected_port(gsl::not_null<const void*> cookie) override {
        return wrapped->get_connected_port(cookie);
    }
    void set_error_context(gsl::not_null<void*> cookie,
                           cb::const_char_buffer message) override {
        wrapped->set_error_context(cookie, message);
    }
    void set_error_json_extras(gsl::not_null<void*> cookie,
                               const nlohmann::json& json) override {
        wrapped->set_error_json_extras(cookie, json);
    }

protected:
    ServerCookieIface* wrapped;
};

void DCPTest::SetUp() {
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

void DCPTest::TearDown() {
    /* MB-22041 changes to dynamically stopping threads rather than having
     * the excess looping but not getting work. We now need to set the
     * AuxIO and NonIO back to 1 to allow dead tasks to be cleaned up
     */
    ExecutorPool::get()->setNumAuxIO(1);
    ExecutorPool::get()->setNumNonIO(1);

    EventuallyPersistentEngineTest::TearDown();

    MemoryTracker::destroyInstance();
}

void DCPTest::create_dcp_producer(
        int flags,
        IncludeValue includeVal,
        IncludeXattrs includeXattrs,
        std::vector<std::pair<std::string, std::string>> controls) {
    if (includeVal == IncludeValue::No) {
        flags |= cb::mcbp::request::DcpOpenPayload::NoValue;
    }
    if (includeVal == IncludeValue::NoWithUnderlyingDatatype) {
        flags |= cb::mcbp::request::DcpOpenPayload::
                NoValueWithUnderlyingDatatype;
    }
    if (includeXattrs == IncludeXattrs::Yes) {
        flags |= cb::mcbp::request::DcpOpenPayload::IncludeXattrs;
    }
    producer = std::make_shared<MockDcpProducer>(*engine,
                                                 cookie,
                                                 "test_producer",
                                                 flags,
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
                  producer->control(0, control.first, control.second));
    }
}

void DCPTest::setup_dcp_stream(
        int flags,
        IncludeValue includeVal,
        IncludeXattrs includeXattrs,
        std::vector<std::pair<std::string, std::string>> controls) {
    create_dcp_producer(flags, includeVal, includeXattrs, controls);

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

    stream->public_registerCursor(
            *vb0->checkpointManager, producer->getName(), 0);
    stream->setActive();
}

void DCPTest::destroy_dcp_stream() {
    producer->closeStream(/*opaque*/ 0, vb0->getId());
}

DCPTest::StreamRequestResult DCPTest::doStreamRequest(DcpProducer& producer,
                                                      uint64_t startSeqno,
                                                      uint64_t endSeqno,
                                                      uint64_t snapStart,
                                                      uint64_t snapEnd,
                                                      uint64_t vbUUID) {
    DCPTest::StreamRequestResult result;
    result.status = producer.streamRequest(/*flags*/ 0,
                                           /*opaque*/ 0,
                                           Vbid(0),
                                           startSeqno,
                                           endSeqno,
                                           vbUUID,
                                           snapStart,
                                           snapEnd,
                                           &result.rollbackSeqno,
                                           DCPTest::fakeDcpAddFailoverLog,
                                           {});
    return result;
}

void DCPTest::prepareCheckpointItemsForStep(dcp_message_producers& msgProducers,
                                            MockDcpProducer& producer,
                                            VBucket& vb) {
    producer.notifySeqnoAvailable(
            vb.getId(), vb.getHighSeqno(), SyncWriteOperation::Yes);
    ASSERT_EQ(ENGINE_EWOULDBLOCK, producer.step(&msgProducers));
    ASSERT_EQ(1, producer.getCheckpointSnapshotTask()->queueSize());
    producer.getCheckpointSnapshotTask()->run();
}

std::unique_ptr<Item> DCPTest::makeItemWithXattrs() {
    std::string valueData = R"({"json":"yes"})";
    std::string data = createXattrValue(valueData);
    protocol_binary_datatype_t datatype =
            (PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR);
    return std::make_unique<Item>(makeStoredDocKey("key"),
                                  /*flags*/ 0,
                                  /*exp*/ 0,
                                  data.c_str(),
                                  data.size(),
                                  datatype);
}

std::unique_ptr<Item> DCPTest::makeItemWithoutXattrs() {
    std::string valueData = R"({"json":"yes"})";
    protocol_binary_datatype_t datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    return std::make_unique<Item>(makeStoredDocKey("key"),
                                  /*flags*/ 0,
                                  /*exp*/ 0,
                                  valueData.c_str(),
                                  valueData.size(),
                                  datatype);
}

void DCPTest::addItemsAndRemoveCheckpoint(int numItems) {
    for (int i = 0; i < numItems; ++i) {
        std::string key("key" + std::to_string(i));
        store_item(vbid, key, "value");
    }
    removeCheckpoint(numItems);
}

void DCPTest::removeCheckpoint(int numItems) {
    /* Create new checkpoint so that we can remove the current checkpoint
       and force a backfill in the DCP stream */
    auto& ckpt_mgr = *vb0->checkpointManager;
    ckpt_mgr.createNewCheckpoint();

    /* Wait for removal of the old checkpoint, this also would imply that
       the items are persisted (in case of persistent buckets) */
    bool new_ckpt_created;
    std::chrono::microseconds uSleepTime(128);
    int itemsRemoved = 0;
    while (true) {
        itemsRemoved +=
                ckpt_mgr.removeClosedUnrefCheckpoints(*vb0, new_ckpt_created);
        if (itemsRemoved >= numItems) {
            break;
        }
        uSleepTime = decayingSleep(uSleepTime);
    };
    EXPECT_EQ(numItems, itemsRemoved);
}
int DCPTest::callbackCount = 0;

void DCPTest::runCheckpointProcessor(dcp_message_producers& producers) {
    // Step which will notify the snapshot task
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(&producers));

    EXPECT_EQ(1, producer->getCheckpointSnapshotTask()->queueSize());

    // Now call run on the snapshot task to move checkpoint into DCP
    // stream
    producer->getCheckpointSnapshotTask()->run();
}

/*
 * MB-30189: Test that addStats() on the DcpProducer object doesn't
 * attempt to dereference the cookie passed in (as it's not it's
 * object).  Check that no invalid memory accesses occur; requires
 * ASan for maximum accuracy in testing.
 */
TEST_F(DCPTest, MB30189_addStats) {
    create_dcp_producer();
    class MockStats {
    } mockStats;
    producer->addStats(
            [](const char* key,
               const uint16_t klen,
               const char* val,
               const uint32_t vlen,
               gsl::not_null<const void*> cookie) {
                // do nothing
            },
            &mockStats);
}

std::string decompressValue(std::string compressedValue) {
    cb::compression::Buffer buffer;
    if (!cb::compression::inflate(cb::compression::Algorithm::Snappy,
                                  compressedValue, buffer)) {
        return {};
    }

    return std::string(buffer.data(), buffer.size());
}

class CompressionStreamTest : public DCPTest,
                              public ::testing::WithParamInterface<
                                      ::testing::tuple<std::string, bool>> {
public:
    void SetUp() override {
        bucketType = ::testing::get<0>(GetParam());
        DCPTest::SetUp();
        vb0 = engine->getVBucket(Vbid(0));
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

    size_t getItemSize(const Item& item) {
        size_t base = MutationResponse::mutationBaseMsgBytes +
                      item.getKey().makeDocKeyWithoutCollectionID().size();
        if (isXattr()) {
            // DCP won't recompress the pruned document
            return base + getXattrSize(false);
        }
        return base + item.getNBytes();
    }

    size_t getXattrSize(bool compressed) const {
        return createXattrValue({}, true, compressed).size();
    }
};

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

    MockDcpMessageProducers producers(engine);

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
    std::unique_ptr<DcpResponse> dcpResponse =
            stream->public_makeResponseFromItem(qi,
                                                SendCommitSyncWriteAs::Commit);
    auto mutProdResponse = dynamic_cast<MutationResponse*>(dcpResponse.get());
    ASSERT_NE(qi.get(), mutProdResponse->getItem().get());
    if (isXattr()) {
        // The same sizes. makeResponseFromItem will have inflated and not
        // compressed as part of the value pruning
        EXPECT_EQ(keyAndSnappyValueMessageSize, dcpResponse->getMessageSize());
    } else {
        EXPECT_LT(keyAndSnappyValueMessageSize, dcpResponse->getMessageSize());
    }

    EXPECT_EQ(ENGINE_SUCCESS, doStreamRequest(*producer).status);

    prepareCheckpointItemsForStep(producers, *producer, *vb);

    /* Stream the snapshot marker first */
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(0, producer->getItemsSent());

    /* Stream the first mutation */
    protocol_binary_datatype_t expectedDataType =
            isXattr() ? PROTOCOL_BINARY_DATATYPE_XATTR
                      : PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    std::string value(qi->getValue()->getData(), qi->getValue()->valueSize());
    EXPECT_STREQ(producers.last_value.c_str(), decompressValue(value).c_str());

    if (isXattr()) {
        // The pruned packet won't be recompressed
        EXPECT_EQ(producers.last_packet_size, keyAndSnappyValueMessageSize);
    } else {
        EXPECT_GT(producers.last_packet_size, keyAndSnappyValueMessageSize);
    }

    EXPECT_FALSE(mcbp::datatype::is_snappy(producers.last_datatype));
    EXPECT_EQ(expectedDataType, producers.last_datatype);

    /**
     * Create a DCP response and check that a new item is created and
     * the message size is less than the size of original item
     */
    uint32_t keyAndValueMessageSize = getItemSize(*item2);
    qi.reset(std::move(item2));
    dcpResponse = stream->public_makeResponseFromItem(
            qi, SendCommitSyncWriteAs::Commit);
    mutProdResponse = dynamic_cast<MutationResponse*>(dcpResponse.get());

    // A new pruned item will always be generated
    if (!isXattr()) {
        ASSERT_EQ(qi.get(), mutProdResponse->getItem().get());
    }
    EXPECT_EQ(dcpResponse->getMessageSize(), keyAndValueMessageSize);

    /* Stream the second mutation */
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));

    value.assign(qi->getValue()->getData(), qi->getValue()->valueSize());
    EXPECT_STREQ(value.c_str(), producers.last_value.c_str());
    EXPECT_EQ(producers.last_packet_size, keyAndValueMessageSize);

    EXPECT_FALSE(mcbp::datatype::is_snappy(producers.last_datatype));
    EXPECT_EQ(expectedDataType, producers.last_datatype);
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

    // Enable the snappy datatype on the connection
    mock_set_datatype_support(cookie, PROTOCOL_BINARY_DATATYPE_SNAPPY);

    auto includeValue = isXattr() ? IncludeValue::No : IncludeValue::Yes;
    setup_dcp_stream(0, includeValue, IncludeXattrs::Yes);

    EXPECT_EQ(ENGINE_SUCCESS, doStreamRequest(*producer).status);
    MockDcpMessageProducers producers(engine);
    ASSERT_TRUE(producer->isCompressionEnabled());

    // Now, add the 3rd item. This item should be compressed
    EXPECT_EQ(ENGINE_SUCCESS, engine->getKVBucket()->set(*item, cookie));

    prepareCheckpointItemsForStep(producers, *producer, *vb);

    /* Stream the snapshot marker */
    ASSERT_EQ(ENGINE_SUCCESS, producer->step(&producers));

    /* Stream the 3rd mutation */
    ASSERT_EQ(ENGINE_SUCCESS, producer->step(&producers));

    /**
     * Create a DCP response and check that a new item is created and
     * the message size is greater than the size of original item
     */
    auto keyAndSnappyValueMessageSize = getItemSize(*item);
    queued_item qi = std::move(item);
    auto dcpResponse = stream->public_makeResponseFromItem(
            qi, SendCommitSyncWriteAs::Commit);
    auto* mutProdResponse = dynamic_cast<MutationResponse*>(dcpResponse.get());
    std::string value;
    if (!isXattr()) {
        ASSERT_EQ(qi.get(), mutProdResponse->getItem().get());
        value.assign(qi->getValue()->getData(), qi->getValue()->valueSize());
    }

    EXPECT_STREQ(producers.last_value.c_str(), value.c_str());
    EXPECT_EQ(dcpResponse->getMessageSize(), keyAndSnappyValueMessageSize);

    EXPECT_EQ(producers.last_packet_size, keyAndSnappyValueMessageSize);

    // If xattr-only enabled on DCP, we won't re-compress (after we've
    // decompressed the document and split out the xattrs)
    protocol_binary_datatype_t snappy =
            isXattr() ? 0 : PROTOCOL_BINARY_DATATYPE_SNAPPY;
    protocol_binary_datatype_t expectedDataType =
            isXattr() ? PROTOCOL_BINARY_DATATYPE_XATTR
                      : PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ((expectedDataType | snappy), producers.last_datatype);
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

    EXPECT_EQ(ENGINE_SUCCESS, doStreamRequest(*producer).status);
    MockDcpMessageProducers producers(engine);

    ASSERT_TRUE(producer->isForceValueCompressionEnabled());

    // Now, add the 4th item, which is not compressed
    EXPECT_EQ(ENGINE_SUCCESS, engine->getKVBucket()->set(*item, cookie));
    /**
     * Create a DCP response and check that a new item is created and
     * the message size is less than the size of the original item
     */
    auto keyAndValueMessageSize = getItemSize(*item);
    queued_item qi = std::move(item);
    auto dcpResponse = stream->public_makeResponseFromItem(
            qi, SendCommitSyncWriteAs::Commit);
    auto* mutProdResponse = dynamic_cast<MutationResponse*>(dcpResponse.get());
    ASSERT_NE(qi.get(), mutProdResponse->getItem().get());
    EXPECT_LT(dcpResponse->getMessageSize(), keyAndValueMessageSize);

    prepareCheckpointItemsForStep(producers, *producer, *vb);

    /* Stream the snapshot marker */
    ASSERT_EQ(ENGINE_SUCCESS, producer->step(&producers));

    /* Stream the mutation */
    ASSERT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    std::string value(qi->getValue()->getData(), qi->getValue()->valueSize());
    EXPECT_STREQ(decompressValue(producers.last_value).c_str(), value.c_str());
    EXPECT_LT(producers.last_packet_size, keyAndValueMessageSize);

    protocol_binary_datatype_t expectedDataType =
            isXattr() ? PROTOCOL_BINARY_DATATYPE_XATTR
                      : PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ((expectedDataType | PROTOCOL_BINARY_DATATYPE_SNAPPY),
              producers.last_datatype);

    destroy_dcp_stream();
}

class ConnectionTest : public DCPTest,
                       public ::testing::WithParamInterface<
                               std::tuple<std::string, std::string>> {
protected:
    void SetUp() override {
        bucketType = std::get<0>(GetParam());
        DCPTest::SetUp();
        vbid = Vbid(0);
        if (bucketType == "ephemeral") {
            engine->getConfiguration().setEphemeralFullPolicy(
                    std::get<1>(GetParam()));
        }
    }

    ENGINE_ERROR_CODE set_vb_state(Vbid vbid, vbucket_state_t state) {
        return engine->getKVBucket()->setVBucketState(
                vbid, state, {}, TransferVB::Yes);
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
    Vbid vbid;
};

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
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);
    // "1" is not a multiple of "2" and so we should return ENGINE_EINVAL
    EXPECT_EQ(ENGINE_EINVAL, producer->control(0, "set_noop_interval", "1"))
            << "Expected producer.control to return ENGINE_EINVAL";
    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, test_maybesendnoop_buffer_full) {
    const void* cookie = create_mock_cookie();
    // Create a Mock Dcp producer
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);

    class MockE2BigMessageProducers : public MockDcpMessageProducers {
    public:
        ENGINE_ERROR_CODE noop(uint32_t) override {
            return ENGINE_E2BIG;
        }

    } producers;

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
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);

    MockDcpMessageProducers producers(handle);
    producer->setNoopEnabled(true);
    const auto send_time = ep_current_time() + 21;
    producer->setNoopSendTime(send_time);
    ENGINE_ERROR_CODE ret = producer->maybeSendNoop(&producers);
    EXPECT_EQ(ENGINE_SUCCESS, ret)
            << "maybeSendNoop not returning ENGINE_SUCCESS";
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
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);

    MockDcpMessageProducers producers(engine);
    const auto send_time = ep_current_time();
    TimeTraveller marty(engine->getConfiguration().getDcpIdleTimeout() + 1);
    producer->setNoopEnabled(true);
    producer->setNoopSendTime(send_time);
    ENGINE_ERROR_CODE ret = producer->maybeSendNoop(&producers);
    // Check to see if a noop was sent i.e. returned ENGINE_SUCCESS
    EXPECT_EQ(ENGINE_SUCCESS, ret)
            << "maybeSendNoop not returning ENGINE_SUCCESS";
    EXPECT_TRUE(producer->getNoopPendingRecv())
            << "Not awaiting noop acknowledgement";
    EXPECT_NE(send_time, producer->getNoopSendTime())
            << "SendTime has not been updated";
    ret = producer->maybeSendNoop(&producers);
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
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);

    MockDcpMessageProducers producers(handle);
    producer->setNoopEnabled(false);
    const auto send_time = ep_current_time() + 21;
    producer->setNoopSendTime(send_time);
    ENGINE_ERROR_CODE ret = producer->maybeSendNoop(&producers);
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
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);

    MockDcpMessageProducers producers(handle);
    producer->setNoopEnabled(true);
    rel_time_t current_time = ep_current_time();
    producer->setNoopSendTime(current_time);
    ENGINE_ERROR_CODE ret = producer->maybeSendNoop(&producers);
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
    auto* cookie1 = (struct MockCookie*)create_mock_cookie();
    auto* cookie2 = (struct MockCookie*)create_mock_cookie();
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
    producer = std::make_shared<MockDcpProducer>(*engine,
                                                 cookie,
                                                 "test_producer",
                                                 /*flags*/ 0);

    /* Send a control message to the producer indicating that the DCP client
       expects a "DCP_STREAM_END" upon stream close */
    const std::string sendStreamEndOnClientStreamCloseCtrlMsg(
            "send_stream_end_on_client_close_stream");
    const std::string sendStreamEndOnClientStreamCloseCtrlValue("true");
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->control(0,
                                sendStreamEndOnClientStreamCloseCtrlMsg,
                                sendStreamEndOnClientStreamCloseCtrlValue));

    /* Open stream */
    EXPECT_EQ(ENGINE_SUCCESS, doStreamRequest(*producer).status);

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
    EXPECT_EQ(ENGINE_SUCCESS, producer->closeStream(0, vbid));

    /* Expect a stream end message */
    MockDcpMessageProducers producers(handle);
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpStreamEnd, producers.last_op);
    EXPECT_EQ(END_STREAM_CLOSED, producers.last_flags);

    /* Re-open stream for the same vbucket on the conn */
    EXPECT_EQ(ENGINE_SUCCESS, doStreamRequest(*producer).status);

    /* Check that the new stream is opened properly */
    auto stream = producer->findStream(vbid);
    auto* as = static_cast<ActiveStream*>(stream.get());
    EXPECT_TRUE(as->isInMemory());

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
    EXPECT_EQ(ENGINE_SUCCESS, doStreamRequest(*producer).status);

    /* Close stream */
    EXPECT_EQ(ENGINE_SUCCESS, producer->closeStream(0, vbid));

    /* Don't expect a stream end message (or any other message as the stream is
       closed) */
    MockDcpMessageProducers producers(handle);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(&producers));

    /* Check that the stream is not found in the producer's stream map */
    EXPECT_TRUE(producer->findStreams(vbid)->wlock().empty());

    /* Disconnect the producer connection */
    connMap.disconnect(cookie);
    /* Cleanup the deadConnections */
    connMap.manageConnections();
}

TEST_P(ConnectionTest, test_producer_unknown_ctrl_msg) {
    const void* cookie = create_mock_cookie();
    /* Create a new Dcp producer */
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);

    /* Send an unkown control message to the producer and expect an error code
       of "ENGINE_EINVAL" */
    const std::string unkownCtrlMsg("unknown");
    const std::string unkownCtrlValue("blah");
    EXPECT_EQ(ENGINE_EINVAL,
              producer->control(0, unkownCtrlMsg, unkownCtrlValue));
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
    Vbid vbid(0);
    // Create a Mock Dcp consumer
    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test_consumer");
    consumer->setLastMessageTime(1234);
    consumer->addStream(/*opaque*/ 0, vbid, /*flags*/ 0);
    EXPECT_NE(1234, consumer->getLastMessageTime())
        << "lastMessagerTime not updated for addStream";
    consumer->setLastMessageTime(1234);
    consumer->closeStream(/*opaque*/ 0, vbid);
    EXPECT_NE(1234, consumer->getLastMessageTime())
        << "lastMessagerTime not updated for closeStream";
    consumer->setLastMessageTime(1234);
    consumer->streamEnd(/*opaque*/ 0, vbid, /*flags*/ 0);
    EXPECT_NE(1234, consumer->getLastMessageTime())
        << "lastMessagerTime not updated for streamEnd";
    const DocKey docKey{nullptr, 0, DocKeyEncodesCollectionId::No};
    consumer->mutation(0, // opaque
                       docKey,
                       {}, // value
                       0, // priv bytes
                       PROTOCOL_BINARY_RAW_BYTES,
                       0, // cas
                       vbid, // vbucket
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
                       vbid, // vbucket
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
                         vbid, // vbucket
                         0, // by seqno
                         0, // rev seqno
                         {}); // meta
    EXPECT_NE(1234, consumer->getLastMessageTime())
        << "lastMessagerTime not updated for expiration";
    consumer->setLastMessageTime(1234);
    consumer->snapshotMarker(/*opaque*/ 0,
                             vbid,
                             /*start_seqno*/ 0,
                             /*end_seqno*/ 0,
                             /*flags*/ 0,
                             /*HCS*/ {},
                             /*maxVisibleSeqno*/ {});
    EXPECT_NE(1234, consumer->getLastMessageTime())
        << "lastMessagerTime not updated for snapshotMarker";
    consumer->setLastMessageTime(1234);
    consumer->noop(/*opaque*/0);
    EXPECT_NE(1234, consumer->getLastMessageTime())
        << "lastMessagerTime not updated for noop";
    consumer->setLastMessageTime(1234);
    consumer->setVBucketState(/*opaque*/ 0,
                              vbid,
                              /*state*/ vbucket_state_active);
    EXPECT_NE(1234, consumer->getLastMessageTime())
        << "lastMessagerTime not updated for setVBucketState";
    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, test_consumer_add_stream) {
    const void* cookie = create_mock_cookie();
    Vbid vbid = Vbid(0);

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

TEST_P(ConnectionTest, consumer_waits_for_add_stream) {
    const void* cookie = create_mock_cookie();
    MockDcpMessageProducers producers(engine);
    MockDcpConsumer consumer(*engine, cookie, "test_consumer");
    ASSERT_EQ(ENGINE_EWOULDBLOCK, consumer.step(&producers));
    // fake that we received add stream
    consumer.setPendingAddStream(false);
    ASSERT_EQ(ENGINE_SUCCESS, consumer.step(&producers));

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
        MockDcpMessageProducers producers(engine);

        // Create a mock DcpConsumer
        MockDcpConsumer consumer(*engine, cookie, "test_consumer");
        consumer.setPendingAddStream(false);
        ASSERT_EQ(1 /*PendingRequest*/,
                  static_cast<uint8_t>(consumer.getGetErrorMapState()));
        ASSERT_EQ(false, consumer.getProducerIsVersion5orHigher());

        // If a Flow Control Policy is enabled, then the first call to step()
        // will handle the Flow Control negotiation. We do not want to test that
        // here, so this is just to let the test to work with all EP
        // configurations.
        if (engine->getConfiguration().getDcpFlowControlPolicy() != "none") {
            ASSERT_EQ(ENGINE_SUCCESS, consumer.step(&producers));
        }

        // The next call to step() is expected to start the GetErrorMap
        // negotiation
        ASSERT_EQ(ENGINE_SUCCESS, consumer.step(&producers));
        ASSERT_EQ(2 /*PendingResponse*/,
                  static_cast<uint8_t>(consumer.getGetErrorMapState()));

        // At this point the consumer is waiting for a response from the
        // producer. I simulate the producer's response with a call to
        // handleResponse()
        protocol_binary_response_header resp{};
        resp.response.setMagic(cb::mcbp::Magic::ClientResponse);
        resp.response.setOpcode(cb::mcbp::ClientOpcode::GetErrorMap);
        resp.response.setStatus(
                prodIsV5orHigher
                        ? cb::mcbp::Status::Success
                        : cb::mcbp::Status::UnknownCommand);
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
    class MockServerCookieApi : public WrappedServerCookieIface {
    public:
        void notify_io_complete(gsl::not_null<const void*> cookie,
                                ENGINE_ERROR_CODE status) override {
            auto* notify_ptr = reinterpret_cast<size_t*>(
                    wrapped->get_engine_specific(cookie));
            (*notify_ptr)++;
        }
    } scapi;

    scapi.store_engine_specific(cookie, &notify_count);

    // 0. Should start with no notifications.
    ASSERT_EQ(0, notify_count);

    // 1. Check that the periodic connNotifier (processPendingNotifications)
    // isn't sufficient to notify (it shouldn't be, as our connection has
    // no notification pending).
    connMap.processPendingNotifications();
    ASSERT_EQ(0, notify_count);

    // 1. Simulate a bucket deletion.
    connMap.shutdownAllConnections();

    // Can also get a second notify as part of manageConnections being called
    // in shutdownAllConnections().
    EXPECT_GE(notify_count, 1)
        << "expected at least one notify after shutting down all connections";

    // Restore notify_io_complete callback.
    destroy_mock_cookie(cookie);
}

// Consumer variant of above test.
TEST_P(ConnectionTest, test_mb20716_connmap_notify_on_delete_consumer) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize();
    const void *cookie = create_mock_cookie();
    // Create a new Dcp consumer
    auto& consumer = dynamic_cast<MockDcpConsumer&>(
            *connMap.newConsumer(cookie, "mb_20716_consumer"));
    consumer.setPendingAddStream(false);

    // Move consumer into paused state (aka EWOULDBLOCK).
    MockDcpMessageProducers producers(handle);
    ENGINE_ERROR_CODE result;
    do {
        result = consumer.step(&producers);
        handleProducerResponseIfStepBlocked(consumer, producers);
    } while (result == ENGINE_SUCCESS);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, result);

    // Check preconditions.
    EXPECT_TRUE(consumer.isPaused());

    // Hook into notify_io_complete.
    // We (ab)use the engine_specific API to pass a pointer to a count of
    // how many times notify_io_complete has been called.
    size_t notify_count = 0;

    class MockServerCookieApi : public WrappedServerCookieIface {
    public:
        void notify_io_complete(gsl::not_null<const void*> cookie,
                                ENGINE_ERROR_CODE status) override {
            auto* notify_ptr = reinterpret_cast<size_t*>(
                    get_mock_server_api()->cookie->get_engine_specific(cookie));
            (*notify_ptr)++;
        }
    } scapi;

    scapi.store_engine_specific(cookie, &notify_count);

    // 0. Should start with no notifications.
    ASSERT_EQ(0, notify_count);

    // 1. Check that the periodic connNotifier (processPendingNotifications)
    // isn't sufficient to notify (it shouldn't be, as our connection has
    // no notification pending).
    connMap.processPendingNotifications();
    ASSERT_EQ(0, notify_count);

    // 2. Simulate a bucket deletion.
    connMap.shutdownAllConnections();

    // Can also get a second notify as part of manageConnections being called
    // in shutdownAllConnections().
    EXPECT_GE(notify_count, 1)
        << "expected at least one notify after shutting down all connections";

    // Restore notify_io_complete callback.
    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, ConsumerWithoutConsumerNameDoesNotEnableSyncRepl) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize();
    const void* cookie = create_mock_cookie();
    // Create a new Dcp consumer
    auto& consumer = dynamic_cast<MockDcpConsumer&>(
            *connMap.newConsumer(cookie, "consumer"));
    consumer.setPendingAddStream(false);
    EXPECT_FALSE(consumer.isSyncReplicationEnabled());
    EXPECT_EQ(DcpConsumer::SyncReplNegotiation::State::Completed,
              consumer.public_getSyncReplNegotiation().state);

    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, ConsumerWithConsumerNameEnablesSyncRepl) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize();
    const void* cookie = create_mock_cookie();
    // Create a new Dcp consumer
    auto& consumer = dynamic_cast<MockDcpConsumer&>(
            *connMap.newConsumer(cookie, "consumer", "replica1"));
    consumer.setPendingAddStream(false);
    EXPECT_FALSE(consumer.isSyncReplicationEnabled());
    EXPECT_EQ(DcpConsumer::SyncReplNegotiation::State::PendingRequest,
              consumer.public_getSyncReplNegotiation().state);

    // Move consumer into paused state (aka EWOULDBLOCK) by stepping through the
    // DCP_CONTROL logic.
    MockDcpMessageProducers producers(handle);
    ENGINE_ERROR_CODE result;
    do {
        result = consumer.step(&producers);
        handleProducerResponseIfStepBlocked(consumer, producers);
    } while (result == ENGINE_SUCCESS);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, result);

    // SyncReplication negotiation is now completed, SyncReplication is enabled
    // on this consumer, and we have sent the consumer name to the producer.
    EXPECT_EQ(DcpConsumer::SyncReplNegotiation::State::Completed,
              consumer.public_getSyncReplNegotiation().state);
    EXPECT_TRUE(consumer.isSyncReplicationEnabled());

    // The last thing that we sent was the key
    EXPECT_EQ("replica1", producers.last_value);

    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, AckCorrectPassiveStream) {
    ASSERT_EQ(ENGINE_SUCCESS, set_vb_state(vbid, vbucket_state_replica));

    const void* cookie1 = create_mock_cookie();
    Vbid vbid = Vbid(0);

    // Create our first consumer and enable SyncReplication so that we can ack
    auto consumer1 =
            std::make_shared<MockDcpConsumer>(*engine, cookie1, "consumer1");
    consumer1->enableSyncReplication();

    // Add our stream and accept to mimic real scenario and ensure it is alive
    ASSERT_EQ(ENGINE_SUCCESS,
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
    const void* cookie2 = create_mock_cookie();
    auto consumer2 =
            std::make_shared<MockDcpConsumer>(*engine, cookie2, "consumer2");
    consumer2->enableSyncReplication();
    ASSERT_EQ(ENGINE_SUCCESS,
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

    ASSERT_EQ(ENGINE_SUCCESS, consumer2->closeStream(1 /*opaque*/, vbid));
    destroy_mock_cookie(cookie1);
    destroy_mock_cookie(cookie2);
}

/*
 * The following tests that when the disk_backfill_queue configuration is
 * set to false on receiving a snapshot marker it does not move into the
 * backfill phase and the open checkpoint id does not get set to zero.  Also
 * checks that on receiving a subsequent snapshot marker we do not create a
 * second checkpoint.
 */
TEST_P(ConnectionTest, test_not_using_backfill_queue) {
    // Make vbucket replica so can add passive stream
    ASSERT_EQ(ENGINE_SUCCESS, set_vb_state(vbid, vbucket_state_replica));

    const void* cookie = create_mock_cookie();
    /*
     * Create a Mock Dcp consumer. Since child class subobj of MockDcpConsumer
     *  obj are accounted for by SingleThreadedRCPtr, use the same here
     */
    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test_consumer");

    // Add passive stream
    ASSERT_EQ(ENGINE_SUCCESS,
              consumer->addStream(/*opaque*/ 0,
                                  vbid,
                                  /*flags*/ 0));
    // Get the checkpointManager
    auto& manager =
            *(engine->getKVBucket()->getVBucket(vbid)->checkpointManager);

    // Because the vbucket was previously active it will have an
    // openCheckpointId of 2
    EXPECT_EQ(2, manager.getOpenCheckpointId());

    // Send a snapshotMarker
    consumer->snapshotMarker(/*opaque*/ 1,
                             Vbid(0),
                             /*start_seqno*/ 0,
                             /*end_seqno*/ 1,
                             /*flags set to MARKER_FLAG_DISK*/ 0x2,
                             /*HCS*/ {},
                             /*maxVisibleSeqno*/ {});

    EXPECT_TRUE(engine->getKVBucket()
                        ->getVBucket(vbid)
                        ->isReceivingInitialDiskSnapshot());

    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);

    /*
     * StreamRequest should tmp fail due to the associated vbucket receiving
     * a disk snapshot.
     */
    uint64_t rollbackSeqno = 0;
    auto err = producer->streamRequest(/*flags*/ 0,
                                       /*opaque*/ 0,
                                       vbid,
                                       /*start_seqno*/ 0,
                                       /*end_seqno*/ 0,
                                       /*vb_uuid*/ 0,
                                       /*snap_start*/ 0,
                                       /*snap_end*/ 0,
                                       &rollbackSeqno,
                                       fakeDcpAddFailoverLog,
                                       {});

    EXPECT_EQ(ENGINE_TMPFAIL, err);

    // Open checkpoint Id should not be effected.
    EXPECT_EQ(2, manager.getOpenCheckpointId());

    /* Send a mutation */
    const DocKey docKey{nullptr, 0, DocKeyEncodesCollectionId::No};
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->mutation(/*opaque*/ 1,
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

    consumer->snapshotMarker(/*opaque*/ 1,
                             Vbid(0),
                             /*start_seqno*/ 0,
                             /*end_seqno*/ 0,
                             /*flags*/ 0,
                             /*HCS*/ {},
                             /*maxVisibleSeqno*/ {});

    // A new opencheckpoint should no be opened
    EXPECT_EQ(2, manager.getOpenCheckpointId());

    // Close stream
    ASSERT_EQ(ENGINE_SUCCESS, consumer->closeStream(/*opaque*/ 0, vbid));
    destroy_mock_cookie(cookie);
}

/*
 * The following test has been adapted following the removal of vbucket DCP
 * backfill queue. It now demonstrates some of the 'new' behaviour of
 * DCP snapshot markers  on a replica checkpoint.
 * When snapshots arrive and no items exist, no new CP is created, only the
 * existing snapshot is updated
 */
TEST_P(ConnectionTest, SnapshotsAndNoData) {
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
    consumer->snapshotMarker(/*opaque*/ 1,
                             Vbid(0),
                             /*start_seqno*/ 0,
                             /*end_seqno*/ 0,
                             /*flags set to MARKER_FLAG_DISK*/ 0x2,
                             /*HCS*/ {},
                             /*maxVisibleSeqno*/ {});

    EXPECT_EQ(2, manager.getOpenCheckpointId());

    consumer->snapshotMarker(/*opaque*/ 1,
                             Vbid(0),
                             /*start_seqno*/ 1,
                             /*end_seqno*/ 2,
                             /*flags*/ 0,
                             /*HCS*/ {},
                             /*maxVisibleSeqno*/ {});

    EXPECT_EQ(2, manager.getOpenCheckpointId());
    // Still cp:2 but the snap-end changes
    auto snapInfo = manager.getSnapshotInfo();
    EXPECT_EQ(0, snapInfo.range.getStart()); // no data so start is still 0
    EXPECT_EQ(2, snapInfo.range.getEnd());

    // Close stream
    ASSERT_EQ(ENGINE_SUCCESS, consumer->closeStream(/*opaque*/0, vbid));
    destroy_mock_cookie(cookie);
}

class DcpConnMapTest : public ::testing::Test {
protected:
    void SetUp() override {
        engine = SynchronousEPEngine::build({});

        initialize_time_functions(get_mock_server_api()->core);

        /* Set up one vbucket in the bucket */
        engine->getKVBucket()->setVBucketState(vbid, vbucket_state_active);
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

    enum class ConnExistsBy : uint8_t { Cookie, Name };

    /**
     * MB-36915: With a recent change, we unconditionally acquire an exclusive
     * lock to vbstate in KVBucket::setVBucketState. But, the new lock
     * introduces a potential deadlock by lock-inversion on connLock and
     * vbstateLock in EPE::dcpOpen if a connection with the same cookie or name
     * already exists in conn-map. TSAN easily spots the issue as soon as we
     * have an execution where two threads run in parallel and execute the code
     * responsible for the potential deadlock, which is what this test achieves.
     */
    void testLockInversionInSetVBucketStateAndNewProducer(ConnExistsBy by);

    SynchronousEPEngineUniquePtr engine;
    const Vbid vbid = Vbid(0);
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
    DcpProducer* producer = engine->getDcpConnMap().newProducer(dummyMockCookie,
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
                                      fakeDcpAddFailoverLog,
                                      {}));

    destroy_mock_cookie(dummyMockCookie);

    /* Delete the connmap, connection should be deleted as the owner of
       the connection (connmap) is deleted. Checks that there is no cyclic
       reference between conn (producer) and stream or any other object */
    engine->setDcpConnMap(nullptr);
}

/* Tests that there is no memory loss due to cyclic reference between a
 * notifier connection and a notifier stream.
 */
TEST_F(DcpConnMapTest, DeleteNotifierConnOnUncleanDCPConnMapDelete) {
    /* Create a new Dcp producer */
    const void* dummyMockCookie = create_mock_cookie();
    DcpProducer* producer = engine->getDcpConnMap().newProducer(
            dummyMockCookie,
            "test_producer",
            cb::mcbp::request::DcpOpenPayload::Notifier);
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
                                      fakeDcpAddFailoverLog,
                                      {}));

    destroy_mock_cookie(dummyMockCookie);

    /* Delete the connmap, connection should be deleted as the owner of
       the connection (connmap) is deleted. Checks that there is no cyclic
       reference between conn (producer) and stream or any other object */
    engine->setDcpConnMap(nullptr);
}

/* Tests that there is no memory loss due to cyclic reference between a
 * consumer connection and a passive stream.
 */
TEST_F(DcpConnMapTest, DeleteConsumerConnOnUncleanDCPConnMapDelete) {
    /* Consumer stream needs a replica vbucket */
    engine->getKVBucket()->setVBucketState(vbid, vbucket_state_replica);

    /* Create a new Dcp consumer */
    const void* dummyMockCookie = create_mock_cookie();
    DcpConsumer* consumer = engine->getDcpConnMap().newConsumer(
            dummyMockCookie, "test_consumer");

    /* Add passive stream */
    ASSERT_EQ(ENGINE_SUCCESS,
              consumer->addStream(/*opaque*/ 0,
                                  vbid,
                                  /*flags*/ 0));

    destroy_mock_cookie(dummyMockCookie);

    /* Delete the connmap, connection should be deleted as the owner of
       the connection (connmap) is deleted. Checks that there is no cyclic
       reference between conn (consumer) and stream or any other object */
    engine->setDcpConnMap(nullptr);
}

TEST_F(DcpConnMapTest, TestCorrectConnHandlerRemoved) {
    auto connMapPtr = std::make_unique<MockDcpConnMap>(*engine);
    MockDcpConnMap& connMap = *connMapPtr;
    engine->setDcpConnMap(std::move(connMapPtr));

    const void* cookieA = create_mock_cookie();
    const void* cookieB = create_mock_cookie();

    ASSERT_EQ(ENGINE_SUCCESS,
              engine->getKVBucket()->setVBucketState(
                      vbid, vbucket_state_replica, {}, TransferVB::Yes));

    DcpConsumer* consumerA =
            connMap.newConsumer(cookieA, "test_consumerA", "test_consumerA");
    EXPECT_FALSE(connMap.doesConnHandlerExist(vbid, "eq_dcpq:test_consumerA"));
    consumerA->addStream(0xdead, vbid, 0);
    EXPECT_TRUE(connMap.doesConnHandlerExist(vbid, "eq_dcpq:test_consumerA"));
    // destroys the first consumer, leaving a weakptr in the vbConn map
    EXPECT_TRUE(connMap.removeConn(cookieA));

    // Create a new consumer, with a stream for the same VB
    DcpConsumer* consumerB =
            connMap.newConsumer(cookieB, "test_consumerB", "test_consumerB");
    EXPECT_FALSE(connMap.doesConnHandlerExist(vbid, "eq_dcpq:test_consumerB"));
    consumerB->addStream(0xbeef, vbid, 0);
    EXPECT_TRUE(connMap.doesConnHandlerExist(vbid, "eq_dcpq:test_consumerB"));

    // Here the ConnHandler added to connMap.vbConns in addStream should be
    // removed
    connMap.disconnect(cookieB);

    // Consumer B should not be in the vbConns any more
    EXPECT_FALSE(connMap.doesConnHandlerExist(vbid, "eq_dcpq:test_consumerB"));

    /* Cleanup the deadConnections */
    connMap.manageConnections();
    destroy_mock_cookie(cookieA);
}

// MB-35061 - Test to ensure the Producer ConnHandler is removed
// when a producer stream ends, and does not linger and become confused
// with a later Producer stream.
TEST_F(DcpConnMapTest, TestCorrectRemovedOnStreamEnd) {
    auto connMapPtr = std::make_unique<MockDcpConnMap>(*engine);
    MockDcpConnMap& connMap = *connMapPtr;
    engine->setDcpConnMap(std::move(connMapPtr));

    // create cookies
    const void* producerCookie = create_mock_cookie();
    const void* consumerCookie = create_mock_cookie();

    MockDcpMessageProducers producers(engine.get());

    // create a producer (We are currently active)
    auto producer = engine->getDcpConnMap().newProducer(producerCookie,
                                                        "producerA",
                                                        /*flags*/ 0);

    producer->control(0xdead, "send_stream_end_on_client_close_stream", "true");

    uint64_t rollbackSeqno = 0;

    ASSERT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(0, // flags
                                      0xdead,
                                      vbid,
                                      0, // start_seqno
                                      ~0ull, // end_seqno
                                      0, // vbucket_uuid,
                                      0, // snap_start_seqno,
                                      0, // snap_end_seqno,
                                      &rollbackSeqno,
                                      fakeDcpAddFailoverLog,
                                      {}));

    EXPECT_TRUE(connMap.doesConnHandlerExist(vbid, "eq_dcpq:producerA"));

    // Close the stream. This will not remove the ConnHandler from
    // ConnMap.vbConns because we are waiting to send streamEnd.
    ASSERT_EQ(ENGINE_SUCCESS, producer->closeStream(0xdead, vbid));
    // Step to send the streamEnd, and remove the ConnHandler
    ASSERT_EQ(ENGINE_SUCCESS, producer->step(&producers));

    // Move to replica
    ASSERT_EQ(ENGINE_SUCCESS,
              engine->getKVBucket()->setVBucketState(
                      vbid, vbucket_state_replica, {}, TransferVB::Yes));

    // confirm the ConnHandler was removed
    EXPECT_FALSE(connMap.doesConnHandlerExist(vbid, "eq_dcpq:producerA"));

    // Create a consumer (we are now a replica)
    DcpConsumer* consumer = connMap.newConsumer(
            consumerCookie, "test_consumerA", "test_consumerA");

    EXPECT_FALSE(connMap.doesConnHandlerExist(vbid, "eq_dcpq:test_consumerA"));

    // add a stream for the same VB as before
    ASSERT_EQ(ENGINE_SUCCESS, consumer->addStream(0xbeef, vbid, 0));
    EXPECT_TRUE(connMap.doesConnHandlerExist(vbid, "eq_dcpq:test_consumerA"));

    // End the stream. This should remove the Consumer ConnHandler from vbConns
    auto streamOpaque =
            static_cast<MockDcpConsumer*>(consumer)->getStreamOpaque(0xbeef);
    ASSERT_TRUE(streamOpaque);
    ASSERT_EQ(ENGINE_SUCCESS,
              consumer->streamEnd(*streamOpaque, vbid, /* flags */ 0));

    // expect neither ConnHandler remains in vbConns
    EXPECT_FALSE(connMap.doesConnHandlerExist(vbid, "eq_dcpq:producerA"));
    EXPECT_FALSE(connMap.doesConnHandlerExist(vbid, "eq_dcpq:test_consumerA"));

    /* Cleanup the deadConnections */
    connMap.manageConnections();
    destroy_mock_cookie(producerCookie);
    destroy_mock_cookie(consumerCookie);
}

/**
 * MB-36637: With a recent change, we unconditionally acquire an exclusive lock
 * to vbstate in KVBucket::setVBucketState. But, deep down in the call hierarchy
 * (ActiveStream::setDead) we may lock again on the same mutex. That happens
 * if we are closing streams that support SyncReplication. So in this test we:
 * 1) create a Producer and enable SyncReplication
 * 2) create an ActiveStream (which implicitly supports SyncReplication)
 * 3) issue a KVBucket::setVBucketState, with newState != oldState
 * Step (3) deadlocks before this fix.
 */
TEST_F(DcpConnMapTest, AvoidDoubleLockToVBStateAtSetVBucketState) {
    const void* cookie = create_mock_cookie();
    const uint32_t flags = 0;
    auto& connMap = dynamic_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    auto* producer = connMap.newProducer(cookie, "producer", flags);

    const uint32_t opaque = 0xdead;
    // Vbstate lock acquired in ActiveStream::setDead (executed by
    // DcpConnMap::disconnect) only if SyncRepl is enabled
    producer->control(opaque, "enable_sync_writes", "true");
    producer->control(opaque, "consumer_name", "consumer");

    uint64_t rollbackSeqno = 0;
    ASSERT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(flags,
                                      opaque,
                                      vbid,
                                      0, // start_seqno
                                      ~0ull, // end_seqno
                                      0, // vbucket_uuid,
                                      0, // snap_start_seqno,
                                      0, // snap_end_seqno,
                                      &rollbackSeqno,
                                      fakeDcpAddFailoverLog,
                                      {} /*collection_filter*/));

    EXPECT_TRUE(connMap.doesConnHandlerExist(vbid, "eq_dcpq:producer"));

    engine->getKVBucket()->setVBucketState(
            vbid,
            vbucket_state_t::vbucket_state_replica,
            {} /*meta*/,
            TransferVB::No);

    // Cleanup
    connMap.manageConnections();
    destroy_mock_cookie(cookie);
}

/**
 * MB-36557: With a recent change, we unconditionally acquire an exclusive lock
 * to vbstate in KVBucket::setVBucketState. But, the new lock introduces a
 * potential deadlock by lock inversion with EPE::handleDisconnect on connLock
 * and vbstateLock.
 * TSAN easily spots the issue as soon as we have an execution where two threads
 * run in parallel and execute the code responsible for the potential deadlock,
 * which is what this test achieves.
 */
TEST_F(DcpConnMapTest,
       AvoidLockInversionInSetVBucketStateAndConnMapDisconnect) {
    const void* cookie = create_mock_cookie();
    const uint32_t flags = 0;
    auto& connMap = dynamic_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    auto* producer = connMap.newProducer(cookie, "producer", flags);

    const uint32_t opaque = 0xdead;
    // Vbstate lock acquired in ActiveStream::setDead (executed by
    // DcpConnMap::disconnect) only if SyncRepl is enabled
    producer->control(opaque, "enable_sync_writes", "true");
    producer->control(opaque, "consumer_name", "consumer");

    uint64_t rollbackSeqno = 0;
    ASSERT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(flags,
                                      opaque,
                                      vbid,
                                      0, // start_seqno
                                      ~0ull, // end_seqno
                                      0, // vbucket_uuid,
                                      0, // snap_start_seqno,
                                      0, // snap_end_seqno,
                                      &rollbackSeqno,
                                      fakeDcpAddFailoverLog,
                                      {} /*collection_filter*/));

    EXPECT_TRUE(connMap.doesConnHandlerExist(vbid, "eq_dcpq:producer"));

    std::thread t1 = std::thread([this]() -> void {
        engine->getKVBucket()->setVBucketState(
                vbid,
                vbucket_state_t::vbucket_state_replica,
                {} /*meta*/,
                TransferVB::No);
    });

    // Disconnect in this thread
    connMap.disconnect(cookie);

    t1.join();

    // Check that streams have been shutdown at disconnect
    EXPECT_FALSE(connMap.doesConnHandlerExist(vbid, "eq_dcpq:producer"));

    // Cleanup
    connMap.manageConnections();
}

void DcpConnMapTest::testLockInversionInSetVBucketStateAndNewProducer(
        ConnExistsBy by) {
    auto& connMap = dynamic_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    const auto* cookie = create_mock_cookie();
    const std::string connName = "producer";
    const uint32_t flags = 0;
    auto* producer = connMap.newProducer(cookie, connName, flags);

    const uint32_t opaque = 0;
    // Vbstate lock acquired in ActiveStream::setDead (executed by
    // DcpConnMap::newProducer) only if SyncRepl is enabled
    producer->control(opaque, "enable_sync_writes", "true");
    producer->control(opaque, "consumer_name", "consumer");

    const auto streamRequest =
            [this, flags, opaque](DcpProducer& producer) -> void {
        uint64_t rollbackSeqno = 0;
        ASSERT_EQ(ENGINE_SUCCESS,
                  producer.streamRequest(flags,
                                         opaque,
                                         vbid,
                                         0, // start_seqno
                                         ~0ull, // end_seqno
                                         0, // vbucket_uuid,
                                         0, // snap_start_seqno,
                                         0, // snap_end_seqno,
                                         &rollbackSeqno,
                                         fakeDcpAddFailoverLog,
                                         {} /*collection_filter*/));
    };

    // Check that the conne has been created and exists in vbConns at stream-req
    {
        SCOPED_TRACE("");
        streamRequest(*producer);
    }
    EXPECT_TRUE(connMap.doesConnHandlerExist(vbid, "eq_dcpq:" + connName));

    std::thread t1 = std::thread([this]() -> void {
        engine->getKVBucket()->setVBucketState(
                vbid,
                vbucket_state_t::vbucket_state_replica,
                {} /*meta*/,
                TransferVB::No);
    });

    // New producer in this thread.
    // Note: ActiveStream::setDead executed only if re-creating the same
    // connection (ie, same cookie or connection name).
    const auto* cookie2 = create_mock_cookie();
    switch (by) {
    case ConnExistsBy::Cookie: {
        const std::string newConnName = "newName";
        EXPECT_FALSE(connMap.newProducer(cookie, newConnName, flags));
        // Note: Connection flagged as disconnected but not yet removed
        // from conn-map. See comments in ConnMap::newProducer.
        EXPECT_TRUE(connMap.doesConnHandlerExist(vbid, "eq_dcpq:" + connName));
        break;
    }
    case ConnExistsBy ::Name: {
        producer = connMap.newProducer(cookie2, connName, flags);
        ASSERT_TRUE(producer);
        // Check that the connection has been re-created with the same name
        // and exists in vbConns at stream-req
        {
            SCOPED_TRACE("");
            streamRequest(*producer);
        }
        EXPECT_TRUE(connMap.doesConnHandlerExist(vbid, "eq_dcpq:" + connName));
        break;
    }
    }

    t1.join();

    // Cleanup
    connMap.manageConnections();
    destroy_mock_cookie(cookie);
    destroy_mock_cookie(cookie2);
}

TEST_F(DcpConnMapTest,
       AvoidLockInversionInSetVBucketStateAndNewProducerExistingCookie) {
    testLockInversionInSetVBucketStateAndNewProducer(ConnExistsBy::Cookie);
}

TEST_F(DcpConnMapTest,
       AvoidLockInversionInSetVBucketStateAndNewProducerExistingName) {
    testLockInversionInSetVBucketStateAndNewProducer(ConnExistsBy::Name);
}

class NotifyTest : public DCPTest {
protected:
    std::unique_ptr<MockDcpConnMap> connMap;
    DcpProducer* producer = nullptr;
    int callbacks = 0;
};

class ConnMapNotifyTest {
public:
    ConnMapNotifyTest(EventuallyPersistentEngine& engine)
        : connMap(new MockDcpConnMap(engine)),
          callbacks(0),
          cookie(create_mock_cookie()) {
        connMap->initialize();

        // Save `this` in server-specific so we can retrieve it from
        // dcp_test_notify_io_complete below:
        get_mock_server_api()->cookie->store_engine_specific(cookie, this);

        producer = connMap->newProducer(cookie,
                                        "test_producer",
                                        /*flags*/ 0);
    }

    ~ConnMapNotifyTest() {
        destroy_mock_cookie(cookie);
    }

    void notify() {
        callbacks++;
        connMap->addConnectionToPending(producer->shared_from_this());
    }

    int getCallbacks() {
        return callbacks;
    }

    static void dcp_test_notify_io_complete(gsl::not_null<const void*> cookie,
                                            ENGINE_ERROR_CODE status) {
        const auto* notifyTest = reinterpret_cast<const ConnMapNotifyTest*>(
                get_mock_server_api()->cookie->get_engine_specific(
                        cookie.get()));
        cb_assert(notifyTest != nullptr);
        const_cast<ConnMapNotifyTest*>(notifyTest)->notify();
    }

    std::unique_ptr<MockDcpConnMap> connMap;
    DcpProducer* producer;

private:
    int callbacks;
    const void* cookie = nullptr;
};


TEST_F(NotifyTest, test_mb19503_connmap_notify) {
    ConnMapNotifyTest notifyTest(*engine);

    // Hook into notify_io_complete
    class MockServerCookieApi : public WrappedServerCookieIface {
    public:
        void notify_io_complete(gsl::not_null<const void*> cookie,
                                ENGINE_ERROR_CODE status) override {
            ConnMapNotifyTest::dcp_test_notify_io_complete(cookie, status);
        }
    } scapi;

    // Should be 0 when we begin
    ASSERT_EQ(0, notifyTest.getCallbacks());
    ASSERT_TRUE(notifyTest.producer->isPaused());
    ASSERT_EQ(0, notifyTest.connMap->getPendingNotifications().size());

    // 1. Call addConnectionToPending - this will queue the producer
    notifyTest.connMap->addConnectionToPending(
            notifyTest.producer->shared_from_this());
    EXPECT_EQ(1, notifyTest.connMap->getPendingNotifications().size());

    // 2. Call processPendingNotifications this will invoke notifyIOComplete
    //    which we've hooked into. For step 3 go to dcp_test_notify_io_complete
    notifyTest.connMap->processPendingNotifications();

    // 2.1 One callback should of occurred, and we should still have one
    //     notification pending (see dcp_test_notify_io_complete).
    EXPECT_EQ(1, notifyTest.getCallbacks());
    EXPECT_EQ(1, notifyTest.connMap->getPendingNotifications().size());

    // 4. Call processPendingNotifications again, is there a new connection?
    notifyTest.connMap->processPendingNotifications();

    // 5. There should of been 2 callbacks
    EXPECT_EQ(2, notifyTest.getCallbacks());
}

// Variation on test_mb19503_connmap_notify - check that notification is correct
// when notifiable is not paused.
TEST_F(NotifyTest, test_mb19503_connmap_notify_paused) {
    ConnMapNotifyTest notifyTest(*engine);

    // Hook into notify_io_complete
    class MockServerCookieApi : public WrappedServerCookieIface {
    public:
        void notify_io_complete(gsl::not_null<const void*> cookie,
                                ENGINE_ERROR_CODE status) override {
            ConnMapNotifyTest::dcp_test_notify_io_complete(cookie, status);
        }
    } scapi;

    // Should be 0 when we begin
    ASSERT_EQ(notifyTest.getCallbacks(), 0);
    ASSERT_TRUE(notifyTest.producer->isPaused());
    ASSERT_EQ(0, notifyTest.connMap->getPendingNotifications().size());

    // 1. Call addConnectionToPending - this will queue the producer
    notifyTest.connMap->addConnectionToPending(
            notifyTest.producer->shared_from_this());
    EXPECT_EQ(1, notifyTest.connMap->getPendingNotifications().size());

    // 2. Mark connection as not paused.
    notifyTest.producer->unPause();

    // 3. Call processPendingNotifications - as the connection is not paused
    // this should *not* invoke notifyIOComplete.
    notifyTest.connMap->processPendingNotifications();

    // 3.1 Should have not had any callbacks.
    EXPECT_EQ(0, notifyTest.getCallbacks());
    // 3.2 Should have no pending notifications.
    EXPECT_EQ(0, notifyTest.connMap->getPendingNotifications().size());

    // 4. Now mark the connection as paused.
    ASSERT_FALSE(notifyTest.producer->isPaused());
    notifyTest.producer->pause();

    // 4. Add another notification - should queue the producer again.
    notifyTest.connMap->addConnectionToPending(
            notifyTest.producer->shared_from_this());
    EXPECT_EQ(1, notifyTest.connMap->getPendingNotifications().size());

    // 5. Call processPendingNotifications a second time - as connection is
    //    paused this time we *should* get a callback.
    notifyTest.connMap->processPendingNotifications();
    EXPECT_EQ(1, notifyTest.getCallbacks());
}

// Tests that the MutationResponse created for the deletion response is of the
// correct size.
TEST_P(ConnectionTest, test_mb24424_deleteResponse) {
    const void* cookie = create_mock_cookie();
    Vbid vbid = Vbid(0);

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
    const DocKey docKey{reinterpret_cast<const uint8_t*>(key.data()),
                        key.size(),
                        DocKeyEncodesCollectionId::No};
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
    Vbid vbid = Vbid(0);

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
    const DocKey docKey{reinterpret_cast<const uint8_t*>(key.data()),
                        key.size(),
                        DocKeyEncodesCollectionId::No};
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
                                       /* in-memory snapshot */ 0x1,
                                       {} /*HCS*/,
                                       {} /*maxVisibleSeq*/));

    /* Send an item for replication */
    const DocKey docKey{nullptr, 0, DocKeyEncodesCollectionId::No};
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
                                       /* in-memory snapshot */ 0x1,
                                       /*HCS*/ {},
                                       /*maxVisibleSeqno*/ {}));

    /* Simulate a situation where adding a mutation temporarily fails
       and hence adds the mutation to a replication buffer. For that, we
       set vbucket::takeover_backed_up to true */
    engine->getKVBucket()->getVBucket(vbid)->setTakeoverBackedUpState(true);

    /* Send an item for replication and expect it to be buffered */
    const DocKey docKey{"mykey", DocKeyEncodesCollectionId::No};
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

    MockDcpMessageProducers producers(handle);
    if ((engine->getConfiguration().getBucketType() == "ephemeral") &&
        (engine->getConfiguration().getEphemeralFullPolicy()) ==
                "fail_new_data") {
        /* Make a call to the function that would be called by the processor
           task here */
        EXPECT_EQ(stop_processing, consumer->processBufferedItems());

        /* Expect the connection to be notified */
        EXPECT_FALSE(consumer->isPaused());

        /* Expect disconnect signal in Ephemeral with "fail_new_data" policy */
        EXPECT_EQ(ENGINE_DISCONNECT, consumer->step(&producers));
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
        EXPECT_EQ(ENGINE_SUCCESS, consumer->step(&producers));

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
        store->setVBucketState(vbid, vbucket_state_active);
        addItems(3);

        producers = std::make_unique<MockDcpMessageProducers>(engine.get());
        producer = std::make_shared<MockDcpProducer>(
                *engine,
                cookie,
                "test_producer",
                0 /*flags*/,
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
    std::unique_ptr<MockDcpMessageProducers> producers;
    std::shared_ptr<MockDcpProducer> producer;
    const Vbid vbid = Vbid(0);
};

TEST_F(ActiveStreamChkptProcessorTaskTest, DeleteDeadStreamEntry) {
    uint64_t rollbackSeqno;
    uint32_t opaque = 1;
    ASSERT_EQ(ENGINE_SUCCESS,
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
                      ActiveStreamChkptProcessorTaskTest::fakeDcpAddFailoverLog,
                      {}));
    /* Checkpoint task processor Q will already have any entry for the stream */
    EXPECT_EQ(1, producer->getCheckpointSnapshotTask()->queueSize());

    /* Close and open the stream without clearing the checkpoint task processor
     Q */
    producer->closeStream(opaque, vbid);
    ASSERT_EQ(ENGINE_SUCCESS,
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
                      ActiveStreamChkptProcessorTaskTest::fakeDcpAddFailoverLog,
                      {}));

    /* The checkpoint processor Q should be processed with the new stream
     getting the item(s) */
    notifyAndStepToCheckpoint();
}

// Test handleResponse accepts opcodes that the producer can send
TEST_F(SingleThreadedKVBucketTest, ProducerHandleResponse) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "ProducerHandleResponse",
                                                      /*flags*/ 0);

    MockDcpMessageProducers producers(engine.get());

    for (auto status : {cb::mcbp::Status::Success,
                        cb::mcbp::Status::Etmpfail,
                        cb::mcbp::Status::Enomem}) {
        protocol_binary_response_header message{};
        message.response.setMagic(cb::mcbp::Magic::ClientResponse);
        message.response.setStatus(status);
        for (auto op : {cb::mcbp::ClientOpcode::DcpMutation,
                        cb::mcbp::ClientOpcode::DcpDeletion,
                        cb::mcbp::ClientOpcode::DcpExpiration,
                        cb::mcbp::ClientOpcode::DcpStreamEnd,
                        cb::mcbp::ClientOpcode::DcpSystemEvent,
                        cb::mcbp::ClientOpcode::DcpCommit,
                        cb::mcbp::ClientOpcode::DcpPrepare,
                        cb::mcbp::ClientOpcode::DcpAbort}) {
            message.response.setOpcode(op);
            EXPECT_TRUE(producer->handleResponse(&message));
        }
    }
}

TEST_F(SingleThreadedKVBucketTest, ProducerHandleResponseDuribilityErrorCodes) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto producer = std::make_shared<MockDcpProducer>(
            *engine, cookie, "ProducerHandleResponceEinVal", 0);
    MockDcpMessageProducers producers(engine.get());

    protocol_binary_response_header message{};
    message.response.setMagic(cb::mcbp::Magic::ClientResponse);
    // check we return false from DcpProducer::handleResponse when receiving
    // an EINVAL and KeyEnoent for Commit, Prepare and abort
    for (auto errorCode :
         {cb::mcbp::Status::KeyEnoent, cb::mcbp::Status::Einval}) {
        message.response.setStatus(errorCode);
        for (auto op : {cb::mcbp::ClientOpcode::DcpCommit,
                        cb::mcbp::ClientOpcode::DcpPrepare,
                        cb::mcbp::ClientOpcode::DcpAbort}) {
            message.response.setOpcode(op);
            EXPECT_FALSE(producer->handleResponse(&message));
        }
        // for the other ops check that we don't return false
        for (auto op : {cb::mcbp::ClientOpcode::DcpMutation,
                        cb::mcbp::ClientOpcode::DcpDeletion,
                        cb::mcbp::ClientOpcode::DcpExpiration,
                        cb::mcbp::ClientOpcode::DcpStreamEnd,
                        cb::mcbp::ClientOpcode::DcpSystemEvent}) {
            message.response.setOpcode(op);
            EXPECT_TRUE(producer->handleResponse(&message));
        }
    }
}

TEST_F(SingleThreadedKVBucketTest, ConsumerIdleTimeoutUpdatedOnConfigChange) {
    engine->getConfiguration().setDcpIdleTimeout(100);

    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test_consumer");
    ASSERT_EQ(std::chrono::seconds(100), consumer->getIdleTimeout());

    // Need to put our consumer in the ConnMap or we won't know to change the
    // value when the config is updated.
    auto& connMap = static_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    connMap.addConn(cookie, consumer);
    ASSERT_TRUE(connMap.findByName("test_consumer"));

    engine->getConfiguration().setDcpIdleTimeout(200);

    EXPECT_EQ(std::chrono::seconds(200), consumer->getIdleTimeout());

    connMap.removeConn(cookie);
}

TEST_F(SingleThreadedKVBucketTest, ProducerIdleTimeoutUpdatedOnConfigChange) {
    engine->getConfiguration().setDcpIdleTimeout(100);

    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);
    ASSERT_EQ(std::chrono::seconds(100), producer->getIdleTimeout());

    // Need to put our producer in the ConnMap or we won't know to change the
    // value when the config is updated.
    auto& connMap = static_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    connMap.addConn(cookie, producer);
    ASSERT_TRUE(connMap.findByName("test_producer"));

    engine->getConfiguration().setDcpIdleTimeout(200);

    EXPECT_EQ(std::chrono::seconds(200), producer->getIdleTimeout());

    connMap.removeConn(cookie);
}

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

INSTANTIATE_TEST_CASE_P(PersistentAndEphemeral,
                        ConnectionTest,
                        STParameterizedBucketTest::allConfigValues(), );
