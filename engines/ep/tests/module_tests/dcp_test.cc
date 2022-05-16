/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * Unit test for DCP-related classes.
 *
 * Due to the way our classes are structured, most of the different DCP classes
 * need an instance of EPBucket& other related objects.
 */

#include "dcp_test.h"
#include "../mock/mock_bucket_logger.h"
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
#include "objectregistry.h"
#include "test_helpers.h"
#include "vbucket.h"
#include "warmup.h"

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <memcached/server_cookie_iface.h>
#include <platform/cbassert.h>
#include <platform/compress.h>
#include <platform/dirutils.h>
#include <programs/engine_testapp/mock_cookie.h>
#include <programs/engine_testapp/mock_server.h>
#include <statistics/cbstat_collector.h>
#include <statistics/labelled_collector.h>

#include <thread>
#include <unordered_map>

// For some reason g++ 10.2.0 fails building googletest 1.11.0 due to not
// finding the ostream operator... workaround that for now..
namespace std::chrono {
void PrintTo(const std::chrono::duration<long int>& val, ostream* os) {
    *os << val.count();
}
} // namespace std::chrono

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
    void setDcpConnHandler(const CookieIface& cookie,
                           DcpConnHandlerIface* handler) override {
        wrapped->setDcpConnHandler(cookie, handler);
    }
    DcpConnHandlerIface* getDcpConnHandler(const CookieIface& cookie) override {
        return wrapped->getDcpConnHandler(cookie);
    }
    void setDcpFlowControlBufferSize(const CookieIface& cookie,
                                     std::size_t size) override {
        wrapped->setDcpFlowControlBufferSize(cookie, size);
    }
    void notify_io_complete(const CookieIface& cookie,
                            cb::engine_errc status) override {
        return wrapped->notify_io_complete(cookie, status);
    }
    void scheduleDcpStep(const CookieIface& cookie) override {
        wrapped->scheduleDcpStep(cookie);
    }
    void reserve(const CookieIface& cookie) override {
        wrapped->reserve(cookie);
    }
    void release(const CookieIface& cookie) override {
        wrapped->release(cookie);
    }
    void set_priority(const CookieIface& cookie,
                      ConnectionPriority priority) override {
        return wrapped->set_priority(cookie, priority);
    }
    ConnectionPriority get_priority(const CookieIface& cookie) override {
        return wrapped->get_priority(cookie);
    }
    uint64_t get_connection_id(const CookieIface& cookie) override {
        return wrapped->get_connection_id(cookie);
    }
    cb::rbac::PrivilegeAccess check_privilege(
            const CookieIface& cookie,
            cb::rbac::Privilege privilege,
            std::optional<ScopeID> sid,
            std::optional<CollectionID> cid) override {
        return wrapped->check_privilege(cookie, privilege, sid, cid);
    }
    cb::rbac::PrivilegeAccess check_for_privilege_at_least_in_one_collection(
            const CookieIface& cookie, cb::rbac::Privilege privilege) override {
        return wrapped->check_for_privilege_at_least_in_one_collection(
                cookie, privilege);
    }
    uint32_t get_privilege_context_revision(
            const CookieIface& cookie) override {
        return wrapped->get_privilege_context_revision(cookie);
    }
    cb::mcbp::Status engine_error2mcbp(const CookieIface& cookie,
                                       cb::engine_errc code) override {
        return wrapped->engine_error2mcbp(cookie, code);
    }
    std::pair<uint32_t, std::string> get_log_info(
            const CookieIface& cookie) override {
        return wrapped->get_log_info(cookie);
    }
    std::string get_authenticated_user(const CookieIface& cookie) override {
        return wrapped->get_authenticated_user(cookie);
    }
    in_port_t get_connected_port(const CookieIface& cookie) override {
        return wrapped->get_connected_port(cookie);
    }
    void set_error_context(CookieIface& cookie,
                           std::string_view message) override {
        wrapped->set_error_context(cookie, message);
    }
    void set_error_json_extras(CookieIface& cookie,
                               const nlohmann::json& json) override {
        wrapped->set_error_json_extras(cookie, json);
    }
    void set_unknown_collection_error_context(CookieIface& cookie,
                                              uint64_t manifestUid) override {
        wrapped->set_unknown_collection_error_context(cookie, manifestUid);
    }
    bool is_valid_json(CookieIface& cookie, std::string_view view) override {
        return wrapped->is_valid_json(cookie, view);
    }
    void send_response(const CookieIface& cookie,
                       cb::engine_errc status,
                       std::string_view view) override {
        wrapped->send_response(cookie, status, view);
    }
    void execution_complete(const CookieIface& cookie) override {
        wrapped->execution_complete(cookie);
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
}

void DCPTest::TearDown() {
    /* MB-22041 changes to dynamically stopping threads rather than having
     * the excess looping but not getting work. We now need to set the
     * AuxIO and NonIO back to 1 to allow dead tasks to be cleaned up
     */
    ExecutorPool::get()->setNumAuxIO(1);
    ExecutorPool::get()->setNumNonIO(1);

    stream.reset();
    producer.reset();

    EventuallyPersistentEngineTest::TearDown();
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
        EXPECT_EQ(cb::engine_errc::success,
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

cb::engine_errc DCPTest::destroy_dcp_stream() {
    return producer->closeStream(/*opaque*/ 0, vb0->getId());
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

void DCPTest::prepareCheckpointItemsForStep(
        DcpMessageProducersIface& msgProducers,
        MockDcpProducer& producer,
        VBucket& vb) {
    producer.notifySeqnoAvailable(
            vb.getId(), vb.getHighSeqno(), SyncWriteOperation::Yes);
    ASSERT_EQ(cb::engine_errc::would_block, producer.step(msgProducers));
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
    auto datatype = PROTOCOL_BINARY_DATATYPE_JSON;
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
    std::chrono::microseconds uSleepTime(128);

    // When checkpoints become unreferenced, they will be immediately
    // removed. This will be driven by the persistence cursor moving
    // out of the checkpoint.
    // Making expectations about the number of items removed is likely
    // to be racy - all the checkpoints may have been removed by persistence
    // before this method was called. Instead, just wait while the only
    // checkpoint left is the checkpoint just created.
    while (ckpt_mgr.getNumCheckpoints() > 1) {
        uSleepTime = decayingSleep(uSleepTime);
    };
}
int DCPTest::callbackCount = 0;

void DCPTest::runCheckpointProcessor(DcpMessageProducersIface& producers) {
    // Step which will notify the snapshot task
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(producers));

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
    producer->addStats(
            [](std::string_view key, std::string_view val, const void* ctx) {
                // do nothing
            },
            nullptr);
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

    MockDcpMessageProducers producers;

    // Now, add 2 items
    EXPECT_EQ(cb::engine_errc::success,
              engine->getKVBucket()->set(*item1, cookie));
    EXPECT_EQ(cb::engine_errc::success,
              engine->getKVBucket()->set(*item2, cookie));

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

    EXPECT_EQ(cb::engine_errc::success, doStreamRequest(*producer).status);

    prepareCheckpointItemsForStep(producers, *producer, *vb);

    /* Stream the snapshot marker first */
    EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
    EXPECT_EQ(0, producer->getItemsSent());

    /* Stream the first mutation */
    protocol_binary_datatype_t expectedDataType =
            isXattr() ? PROTOCOL_BINARY_DATATYPE_XATTR
                      : PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
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
    qi = queued_item(std::move(item2));
    dcpResponse = stream->public_makeResponseFromItem(
            qi, SendCommitSyncWriteAs::Commit);
    mutProdResponse = dynamic_cast<MutationResponse*>(dcpResponse.get());

    // A new pruned item will always be generated
    if (!isXattr()) {
        ASSERT_EQ(qi.get(), mutProdResponse->getItem().get());
    }
    EXPECT_EQ(dcpResponse->getMessageSize(), keyAndValueMessageSize);

    /* Stream the second mutation */
    EXPECT_EQ(cb::engine_errc::success, producer->step(producers));

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
    cookie->setDatatypeSupport(PROTOCOL_BINARY_DATATYPE_SNAPPY);

    auto includeValue = isXattr() ? IncludeValue::No : IncludeValue::Yes;
    setup_dcp_stream(0, includeValue, IncludeXattrs::Yes);

    EXPECT_EQ(cb::engine_errc::success, doStreamRequest(*producer).status);
    MockDcpMessageProducers producers;
    ASSERT_TRUE(producer->isCompressionEnabled());

    // Now, add the 3rd item. This item should be compressed
    EXPECT_EQ(cb::engine_errc::success,
              engine->getKVBucket()->set(*item, cookie));

    prepareCheckpointItemsForStep(producers, *producer, *vb);

    /* Stream the snapshot marker */
    ASSERT_EQ(cb::engine_errc::success, producer->step(producers));

    /* Stream the 3rd mutation */
    ASSERT_EQ(cb::engine_errc::success, producer->step(producers));

    /**
     * Create a DCP response and check that a new item is created and
     * the message size is greater than the size of original item
     */
    auto keyAndSnappyValueMessageSize = getItemSize(*item);
    queued_item qi(std::move(item));
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
    cookie->setDatatypeSupport(PROTOCOL_BINARY_DATATYPE_SNAPPY);
    auto includeValue = isXattr() ? IncludeValue::No : IncludeValue::Yes;

    // Setup the producer/stream and request force_value_compression
    setup_dcp_stream(0,
                     includeValue,
                     IncludeXattrs::Yes,
                     {{"force_value_compression", "true"}});

    EXPECT_EQ(cb::engine_errc::success, doStreamRequest(*producer).status);
    MockDcpMessageProducers producers;

    ASSERT_TRUE(producer->isForceValueCompressionEnabled());

    // Now, add the 4th item, which is not compressed
    EXPECT_EQ(cb::engine_errc::success,
              engine->getKVBucket()->set(*item, cookie));
    /**
     * Create a DCP response and check that a new item is created and
     * the message size is less than the size of the original item
     */
    auto keyAndValueMessageSize = getItemSize(*item);
    queued_item qi(std::move(item));
    auto dcpResponse = stream->public_makeResponseFromItem(
            qi, SendCommitSyncWriteAs::Commit);
    auto* mutProdResponse = dynamic_cast<MutationResponse*>(dcpResponse.get());
    ASSERT_NE(qi.get(), mutProdResponse->getItem().get());
    EXPECT_LT(dcpResponse->getMessageSize(), keyAndValueMessageSize);

    prepareCheckpointItemsForStep(producers, *producer, *vb);

    /* Stream the snapshot marker */
    ASSERT_EQ(cb::engine_errc::success, producer->step(producers));

    /* Stream the mutation */
    ASSERT_EQ(cb::engine_errc::success, producer->step(producers));
    std::string value(qi->getValue()->getData(), qi->getValue()->valueSize());
    EXPECT_STREQ(decompressValue(producers.last_value).c_str(), value.c_str());
    EXPECT_LT(producers.last_packet_size, keyAndValueMessageSize);

    protocol_binary_datatype_t expectedDataType =
            isXattr() ? PROTOCOL_BINARY_DATATYPE_XATTR
                      : PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ((expectedDataType | PROTOCOL_BINARY_DATATYPE_SNAPPY),
              producers.last_datatype);

    EXPECT_EQ(cb::engine_errc::success, destroy_dcp_stream());
}

TEST_P(CompressionStreamTest,
       NoWithUnderlyingDatatype_CompressionDisabled_ItemCompressed) {
    setup_dcp_stream(
            0, IncludeValue::NoWithUnderlyingDatatype, IncludeXattrs::Yes);
    ASSERT_FALSE(producer->isCompressionEnabled());
    ASSERT_EQ(IncludeValue::NoWithUnderlyingDatatype,
              stream->public_getIncludeValue());
    ASSERT_EQ(IncludeXattrs::Yes, stream->public_getIncludeXattrs());

    // Create a compressed item
    auto item = makeCompressibleItem(vbid,
                                     makeStoredDocKey("key"),
                                     "body000000000000000000000000000000000000",
                                     isXattr() ? PROTOCOL_BINARY_DATATYPE_JSON
                                               : PROTOCOL_BINARY_RAW_BYTES,
                                     true, // compressed
                                     isXattr());

    // ActiveStream::makeResponseFromItem is where we modify the item value (if
    // necessary) before pushing items into the Stream::readyQ. Here we just
    // pass the item in input to the function and check that we get the expected
    // DcpResponse.

    queued_item originalItem(std::move(item));
    const auto resp = stream->public_makeResponseFromItem(
            originalItem, SendCommitSyncWriteAs::Commit);

    const auto* mut = dynamic_cast<MutationResponse*>(resp.get());
    ASSERT_TRUE(mut);

    // Expecting a modified item, new allocation occurred.
    ASSERT_NE(originalItem.get(), mut->getItem().get());

    const auto originalValueSize = originalItem->getNBytes();
    ASSERT_GT(originalValueSize, 0);
    const auto onTheWireValueSize = mut->getItem()->getNBytes();

    if (isXattr()) {
        // Stream::makeResponseFromItem will have inflated the value for
        // removing Xattrs, and then not re-compressed as passive compression
        // is disabled.
        EXPECT_GT(onTheWireValueSize, originalValueSize);
    } else {
        // Body only, which must have been removed.
        EXPECT_EQ(0, onTheWireValueSize);
    }
}

TEST_P(CompressionStreamTest,
       NoWithUnderlyingDatatype_CompressionEnabled_ItemCompressed) {
    // Enable the snappy and passive compression on the connection.
    cookie->setDatatypeSupport(PROTOCOL_BINARY_DATATYPE_SNAPPY);
    setup_dcp_stream(0,
                     IncludeValue::NoWithUnderlyingDatatype,
                     IncludeXattrs::Yes,
                     {{"force_value_compression", "true"}});

    ASSERT_TRUE(producer->isSnappyEnabled());
    ASSERT_TRUE(producer->isCompressionEnabled());
    ASSERT_TRUE(producer->isForceValueCompressionEnabled());
    ASSERT_TRUE(stream->isSnappyEnabled());
    ASSERT_TRUE(stream->isCompressionEnabled());
    ASSERT_TRUE(stream->isForceValueCompressionEnabled());

    ASSERT_EQ(IncludeValue::NoWithUnderlyingDatatype,
              stream->public_getIncludeValue());
    ASSERT_EQ(IncludeXattrs::Yes, stream->public_getIncludeXattrs());

    const auto body = "body000000000000000000000000000000000000";
    const auto key = makeStoredDocKey("key");

    // Create a compressed item
    auto item = makeCompressibleItem(vbid,
                                     key,
                                     body,
                                     isXattr() ? PROTOCOL_BINARY_DATATYPE_JSON
                                               : PROTOCOL_BINARY_RAW_BYTES,
                                     true, // compressed
                                     isXattr());

    // ActiveStream::makeResponseFromItem is where we modify the item value (if
    // necessary) before pushing items into the Stream::readyQ. Here we just
    // pass the item in input to the function and check that we get the expected
    // DcpResponse.

    queued_item originalItem(std::move(item));
    const auto resp = stream->public_makeResponseFromItem(
            originalItem, SendCommitSyncWriteAs::Commit);

    const auto* mut = dynamic_cast<MutationResponse*>(resp.get());
    ASSERT_TRUE(mut);

    // Expecting a modified item, new allocation occurred.
    ASSERT_NE(originalItem.get(), mut->getItem().get());

    const auto originalValueSize = originalItem->getNBytes();
    ASSERT_GT(originalValueSize, 0);
    const auto onTheWireValueSize = mut->getItem()->getNBytes();

    if (isXattr()) {
        // Some extra validation for the Xattr case, for ensuring that the test
        // is valid.
        // During the test, the value of the compressed item is uncompressed /
        // modified / re-compressed, and we make assumptions on sizes for
        // understanding if the final value is compressed. Note that we cannot
        // use the datatype for that, as here we are dealing with
        // IncludeValue::NoWithUnderlyingDatatype, so by definition the datatype
        // is inconsistent with the underlying value.
        // It's very easy to invalidate this test by using a wrong payload. Eg,
        // is the final value-size smaller than the original value-size because
        // we have successfully re-compressed the final value (which is what we
        // want) or because the final value is wrongly uncompressed but still
        // smaller than the original (compressed) payload?
        // The latter may happen if you have Body+Xattr and you remove the Body,
        // which is exactly what we do on IncludeValue::NoWithUnderlyingDatatype

        // Ensure that the uncompressed Xattr block is bigger than the original
        // compressed payload.
        const auto uncompressedXattrSize =
                makeCompressibleItem(vbid,
                                     key,
                                     "" /*body*/,
                                     PROTOCOL_BINARY_DATATYPE_JSON,
                                     false, // compressed
                                     true /*xattrs*/)
                        ->getNBytes();
        ASSERT_GT(uncompressedXattrSize, originalValueSize);

        // Stream::makeResponseFromItem will have inflated the value for
        // removing the Body, and then re-compressed as passive compression is
        // enabled. Before the fix this fails because we miss to re-compress the
        // final value.
        // Note: This is where the test may be invalid if using a wrong payload.
        EXPECT_LT(onTheWireValueSize, originalValueSize);
    } else {
        // Body only, which must have been removed.
        EXPECT_EQ(0, onTheWireValueSize);
    }
}

/**
 * The test verifies that we don't even attempt compression if an item has no
 * value. We would produce and stream a size-1 Snappy value otherwise.
 */
TEST_P(CompressionStreamTest, CompressionEnabled_NoValue) {
    // Enable the snappy and passive compression on the connection.
    cookie->setDatatypeSupport(PROTOCOL_BINARY_DATATYPE_SNAPPY);

    // Note: Whatever input, we want to stream a size-0 value
    setup_dcp_stream(0,
                     IncludeValue::No,
                     IncludeXattrs::No,
                     {{"force_value_compression", "true"}});

    ASSERT_TRUE(producer->isSnappyEnabled());
    ASSERT_TRUE(producer->isCompressionEnabled());
    ASSERT_TRUE(producer->isForceValueCompressionEnabled());
    ASSERT_TRUE(stream->isSnappyEnabled());
    ASSERT_TRUE(stream->isCompressionEnabled());
    ASSERT_TRUE(stream->isForceValueCompressionEnabled());

    ASSERT_EQ(IncludeValue::No, stream->public_getIncludeValue());
    ASSERT_EQ(IncludeXattrs::No, stream->public_getIncludeXattrs());

    // Create a compressed item
    auto item = makeCompressibleItem(vbid,
                                     makeStoredDocKey("key"),
                                     "body000000000000000000000000000000000000",
                                     isXattr() ? PROTOCOL_BINARY_DATATYPE_JSON
                                               : PROTOCOL_BINARY_RAW_BYTES,
                                     false, // compressed
                                     isXattr());
    ASSERT_GT(item->getNBytes(), 0);

    // ActiveStream::makeResponseFromItem is where we modify the item value (if
    // necessary) before pushing items into the Stream::readyQ. Here we just
    // pass the item in input to the function and check that we get the expected
    // DcpResponse.

    queued_item originalItem(std::move(item));
    const auto resp = stream->public_makeResponseFromItem(
            originalItem, SendCommitSyncWriteAs::Commit);

    const auto* mut = dynamic_cast<MutationResponse*>(resp.get());
    ASSERT_TRUE(mut);

    // Expecting a modified item, new allocation occurred.
    ASSERT_NE(originalItem.get(), mut->getItem().get());

    // We did compress but discarded the final value as it is larger than the
    // input, we stream no value as expected.
    EXPECT_EQ(0, mut->getItem()->getNBytes());
}

class ConnectionTest : public DCPTest,
                       public ::testing::WithParamInterface<std::string> {
protected:
    void SetUp() override {
        if (!config_string.empty()) {
            config_string += ";";
        }

        config_string += sanitizeTestParamConfigString(GetParam());

        DCPTest::SetUp();
        vbid = Vbid(0);
    }

    cb::engine_errc set_vb_state(Vbid vbid, vbucket_state_t state) {
        return engine->getKVBucket()->setVBucketState(
                vbid, state, {}, TransferVB::Yes);
    }

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
    auto* cookie = create_mock_cookie(engine);
    engine->getConfiguration().setConnectionManagerInterval(2);

    // Create a Mock Dcp producer
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);
    // "1" is not a multiple of "2" and so we should return
    // cb::engine_errc::invalid_arguments
    EXPECT_EQ(cb::engine_errc::invalid_arguments,
              producer->control(0, "set_noop_interval", "1"))
            << "Expected producer.control to return "
               "cb::engine_errc::invalid_arguments";
    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, test_maybesendnoop_buffer_full) {
    auto* cookie = create_mock_cookie(engine);
    // Create a Mock Dcp producer
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);

    class MockE2BigMessageProducers : public MockDcpMessageProducers {
    public:
        cb::engine_errc noop(uint32_t) override {
            return cb::engine_errc::too_big;
        }

    } producers;

    producer->setNoopEnabled(true);
    const auto send_time = ep_current_time() + 21;
    producer->setNoopSendTime(send_time);
    cb::engine_errc ret = producer->maybeSendNoop(producers);
    EXPECT_EQ(cb::engine_errc::too_big, ret)
            << "maybeSendNoop not returning cb::engine_errc::too_big";
    EXPECT_FALSE(producer->getNoopPendingRecv())
            << "Waiting for noop acknowledgement";
    EXPECT_EQ(send_time, producer->getNoopSendTime())
            << "SendTime has been updated";
    producer->cancelCheckpointCreatorTask();
    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, test_maybesendnoop_send_noop) {
    auto* cookie = create_mock_cookie(engine);
    // Create a Mock Dcp producer
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);

    MockDcpMessageProducers producers;
    producer->setNoopEnabled(true);
    const auto send_time = ep_current_time() + 21;
    producer->setNoopSendTime(send_time);
    cb::engine_errc ret = producer->maybeSendNoop(producers);
    EXPECT_EQ(cb::engine_errc::success, ret)
            << "maybeSendNoop not returning cb::engine_errc::success";
    EXPECT_TRUE(producer->getNoopPendingRecv())
            << "Not waiting for noop acknowledgement";
    EXPECT_NE(send_time, producer->getNoopSendTime())
            << "SendTime has not been updated";
    producer->cancelCheckpointCreatorTask();
    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, test_maybesendnoop_noop_already_pending) {
    auto* cookie = create_mock_cookie(engine);
    // Create a Mock Dcp producer
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);

    MockDcpMessageProducers producers;
    const auto send_time = ep_current_time();
    TimeTraveller marty(engine->getConfiguration().getDcpIdleTimeout() + 1);
    producer->setNoopEnabled(true);
    producer->setNoopSendTime(send_time);
    cb::engine_errc ret = producer->maybeSendNoop(producers);
    // Check to see if a noop was sent i.e. returned cb::engine_errc::success
    EXPECT_EQ(cb::engine_errc::success, ret)
            << "maybeSendNoop not returning cb::engine_errc::success";
    EXPECT_TRUE(producer->getNoopPendingRecv())
            << "Not awaiting noop acknowledgement";
    EXPECT_NE(send_time, producer->getNoopSendTime())
            << "SendTime has not been updated";
    ret = producer->maybeSendNoop(producers);
    // Check to see if a noop was not sent i.e. returned cb::engine_errc::failed
    EXPECT_EQ(cb::engine_errc::failed, ret)
            << "maybeSendNoop not returning cb::engine_errc::failed";
    producer->setLastReceiveTime(send_time);
    ret = producer->maybeDisconnect();
    // Check to see if we want to disconnect i.e. returned
    // cb::engine_errc::disconnect
    EXPECT_EQ(cb::engine_errc::disconnect, ret)
            << "maybeDisconnect not returning cb::engine_errc::disconnect";
    producer->setLastReceiveTime(
            send_time + engine->getConfiguration().getDcpIdleTimeout() + 1);
    ret = producer->maybeDisconnect();
    // Check to see if we don't want to disconnect i.e. returned
    // cb::engine_errc::failed
    EXPECT_EQ(cb::engine_errc::failed, ret)
            << "maybeDisconnect not returning cb::engine_errc::failed";
    EXPECT_TRUE(producer->getNoopPendingRecv())
            << "Not waiting for noop acknowledgement";
    producer->cancelCheckpointCreatorTask();
    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, test_maybesendnoop_not_enabled) {
    auto* cookie = create_mock_cookie(engine);
    // Create a Mock Dcp producer
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);

    MockDcpMessageProducers producers;
    producer->setNoopEnabled(false);
    const auto send_time = ep_current_time() + 21;
    producer->setNoopSendTime(send_time);
    cb::engine_errc ret = producer->maybeSendNoop(producers);
    EXPECT_EQ(cb::engine_errc::failed, ret)
            << "maybeSendNoop not returning cb::engine_errc::failed";
    EXPECT_FALSE(producer->getNoopPendingRecv())
            << "Waiting for noop acknowledgement";
    EXPECT_EQ(send_time, producer->getNoopSendTime())
            << "SendTime has been updated";
    producer->cancelCheckpointCreatorTask();
    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, test_maybesendnoop_not_sufficient_time_passed) {
    auto* cookie = create_mock_cookie(engine);
    // Create a Mock Dcp producer
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);

    MockDcpMessageProducers producers;
    producer->setNoopEnabled(true);
    rel_time_t current_time = ep_current_time();
    producer->setNoopSendTime(current_time);
    cb::engine_errc ret = producer->maybeSendNoop(producers);
    EXPECT_EQ(cb::engine_errc::failed, ret)
            << "maybeSendNoop not returning cb::engine_errc::failed";
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
    auto* cookie = create_mock_cookie(engine);
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

    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, test_mb23637_findByNameWithConnectionDoDisconnect) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize();
    auto* cookie = create_mock_cookie(engine);
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

    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, test_mb23637_findByNameWithDuplicateConnections) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize();
    auto* cookie1 = create_mock_cookie(engine);
    auto* cookie2 = create_mock_cookie(engine);
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

    destroy_mock_cookie(cookie1);
    destroy_mock_cookie(cookie2);
}


TEST_P(ConnectionTest, test_mb17042_duplicate_name_producer_connections) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize();
    auto* cookie1 = create_mock_cookie(engine);
    auto* cookie2 = create_mock_cookie(engine);
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

    destroy_mock_cookie(cookie1);
    destroy_mock_cookie(cookie2);
}

TEST_P(ConnectionTest, test_mb17042_duplicate_name_consumer_connections) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize();
    auto* cookie1 = create_mock_cookie(engine);
    auto* cookie2 = create_mock_cookie(engine);
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

    destroy_mock_cookie(cookie1);
    destroy_mock_cookie(cookie2);
}

TEST_P(ConnectionTest, test_producer_unknown_ctrl_msg) {
    auto* cookie = create_mock_cookie(engine);
    /* Create a new Dcp producer */
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);

    /* Send an unknown control message to the producer and expect an error code
       of "cb::engine_errc::invalid_arguments" */
    const std::string unknownCtrlMsg("unknown");
    const std::string unknownCtrlValue("blah");
    EXPECT_EQ(cb::engine_errc::invalid_arguments,
              producer->control(0, unknownCtrlMsg, unknownCtrlValue));
    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, test_update_of_last_message_time_in_consumer) {
    auto* cookie = create_mock_cookie(engine);
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
    consumer->streamEnd(/*opaque*/ 0, vbid, cb::mcbp::DcpStreamEndStatus::Ok);
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



TEST_P(ConnectionTest, consumer_waits_for_add_stream) {
    auto* cookie = create_mock_cookie(engine);
    MockDcpMessageProducers producers;
    MockDcpConsumer consumer(*engine, cookie, "test_consumer");
    ASSERT_EQ(cb::engine_errc::would_block, consumer.step(producers));
    // fake that we received add stream
    consumer.setPendingAddStream(false);
    ASSERT_EQ(cb::engine_errc::success, consumer.step(producers));

    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, consumer_get_error_map) {
    // We want to test that the Consumer processes the GetErrorMap negotiation
    // with the Producer correctly. I.e., the Consumer must check the
    // Producer's version and set internal flags accordingly.
    // Note: we test both the cases of pre-5.0.0 and post-5.0.0 Producer
    for (auto prodIsV5orHigher : {true, false}) {
        auto* cookie = create_mock_cookie(engine);
        // GetErrorMap negotiation performed only if NOOP is enabled
        engine->getConfiguration().setDcpEnableNoop(true);
        MockDcpMessageProducers producers;

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
            ASSERT_EQ(cb::engine_errc::success, consumer.step(producers));
        }

        // The next call to step() is expected to start the GetErrorMap
        // negotiation
        ASSERT_EQ(cb::engine_errc::success, consumer.step(producers));
        ASSERT_EQ(2 /*PendingResponse*/,
                  static_cast<uint8_t>(consumer.getGetErrorMapState()));

        // At this point the consumer is waiting for a response from the
        // producer. I simulate the producer's response with a call to
        // handleResponse()
        cb::mcbp::Response resp{};
        resp.setMagic(cb::mcbp::Magic::ClientResponse);
        resp.setOpcode(cb::mcbp::ClientOpcode::GetErrorMap);
        resp.setStatus(prodIsV5orHigher ? cb::mcbp::Status::Success
                                        : cb::mcbp::Status::UnknownCommand);
        ASSERT_TRUE(consumer.handleResponse(resp));
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
    auto* cookie = create_mock_cookie(engine);
    // Create a new Dcp producer
    DcpProducer* producer = connMap.newProducer(cookie,
                                                "test_producer",
                                                /*flags*/ 0);

    // Disconnect the producer connection
    connMap.disconnect(cookie);

    // Try to read stats. Shouldn't crash.
    producer->addStats(
            [](std::string_view key, std::string_view value, const void* ctx) {
            },
            nullptr);

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
    auto* cookie = create_mock_cookie(engine);
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
        void scheduleDcpStep(const CookieIface& cookie) override {
            auto* notify_ptr =
                    reinterpret_cast<size_t*>(cookie.getEngineStorage());
            (*notify_ptr)++;
        }
    } scapi;

    cookie->setEngineStorage(&notify_count);

    // 0. Should start with no notifications.
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
    auto* cookie = create_mock_cookie(engine);
    // Create a new Dcp consumer
    auto& consumer = dynamic_cast<MockDcpConsumer&>(
            *connMap.newConsumer(cookie, "mb_20716_consumer"));
    consumer.setPendingAddStream(false);

    // Move consumer into paused state (aka EWOULDBLOCK).
    MockDcpMessageProducers producers;
    cb::engine_errc result;
    do {
        result = consumer.step(producers);
        handleProducerResponseIfStepBlocked(consumer, producers);
    } while (result == cb::engine_errc::success);
    EXPECT_EQ(cb::engine_errc::would_block, result);

    // Check preconditions.
    EXPECT_TRUE(consumer.isPaused());

    // Hook into notify_io_complete.
    // We (ab)use the engine_specific API to pass a pointer to a count of
    // how many times notify_io_complete has been called.
    size_t notify_count = 0;

    class MockServerCookieApi : public WrappedServerCookieIface {
    public:
        void scheduleDcpStep(const CookieIface& cookie) override {
            auto* notify_ptr =
                    reinterpret_cast<size_t*>(cookie.getEngineStorage());
            (*notify_ptr)++;
        }
    } scapi;

    cookie->setEngineStorage(&notify_count);

    // 0. Should start with no notifications.
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

TEST_P(ConnectionTest, ConsumerWithoutConsumerNameDoesNotEnableSyncRepl) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize();
    auto* cookie = create_mock_cookie(engine);
    // Create a new Dcp consumer
    auto& consumer = dynamic_cast<MockDcpConsumer&>(
            *connMap.newConsumer(cookie, "consumer"));
    consumer.setPendingAddStream(false);
    EXPECT_FALSE(consumer.isSyncReplicationEnabled());
    EXPECT_EQ(DcpConsumer::BlockingDcpControlNegotiation::State::Completed,
              consumer.public_getSyncReplNegotiation().state);

    destroy_mock_cookie(cookie);
}

TEST_P(ConnectionTest, ConsumerWithConsumerNameEnablesSyncRepl) {
    MockDcpConnMap connMap(*engine);
    connMap.initialize();
    auto* cookie = create_mock_cookie(engine);
    // Create a new Dcp consumer
    auto& consumer = dynamic_cast<MockDcpConsumer&>(
            *connMap.newConsumer(cookie, "consumer", "replica1"));
    consumer.setPendingAddStream(false);
    EXPECT_FALSE(consumer.isSyncReplicationEnabled());
    using State = DcpConsumer::BlockingDcpControlNegotiation::State;
    auto syncReplNeg = consumer.public_getSyncReplNegotiation();
    EXPECT_EQ(State::PendingRequest, syncReplNeg.state);

    // Move consumer into paused state (aka EWOULDBLOCK) by stepping through the
    // DCP_CONTROL logic.
    MockDcpMessageProducers producers;
    cb::engine_errc result;
    do {
        result = consumer.step(producers);
        handleProducerResponseIfStepBlocked(consumer, producers);
        syncReplNeg = consumer.public_getSyncReplNegotiation();
    } while (syncReplNeg.state != State::Completed);
    EXPECT_EQ(cb::engine_errc::success, result);

    // Last step - send the consumer name
    ASSERT_TRUE(consumer.public_getPendingSendConsumerName());
    EXPECT_EQ(cb::engine_errc::success, consumer.step(producers));

    // SyncReplication negotiation is now completed, SyncReplication is enabled
    // on this consumer, and we have sent the consumer name to the producer.
    EXPECT_TRUE(consumer.isSyncReplicationEnabled());
    EXPECT_FALSE(consumer.public_getPendingSendConsumerName());
    EXPECT_EQ("replica1", producers.last_value);

    destroy_mock_cookie(cookie);
}

class DcpConnMapTest : public ::testing::Test {
protected:
    void SetUp() override {
        ExecutorPool::create();

        const auto dbname = dbnameFromCurrentGTestInfo();
        removePathIfExists(dbname);

        const auto extraConfig = "dbname=" + dbname;
        engine = SynchronousEPEngine::build(extraConfig);

        initialize_time_functions(get_mock_server_api()->core);

        /* Set up one vbucket in the bucket */
        engine->getKVBucket()->setVBucketState(vbid, vbucket_state_active);
    }

    void TearDown() override {
        ExecutorPool::get()->unregisterTaskable(engine->getTaskable(),
                                                false /*force*/);
        engine.reset();
        ObjectRegistry::onSwitchThread(nullptr);
        ExecutorPool::shutdown();
        removePathIfExists(dbnameFromCurrentGTestInfo());
    }

    /**
     * Fake callback emulating dcp_add_failover_log
     */
    static cb::engine_errc fakeDcpAddFailoverLog(
            const std::vector<vbucket_failover_t>&) {
        return cb::engine_errc::success;
    }

    enum class ConnExistsBy : uint8_t { Cookie, Name };

    /**
     * MB-36915: With a recent change, we unconditionally acquire an exclusive
     * lock to vbstate in KVBucket::setVBucketState. But, the new lock
     * introduces a potential deadlock by lock-inversion on connLock and
     * vbstateLock in EPE::dcpOpen if a connection with the same name
     * already exists in conn-map. TSAN easily spots the issue as soon as we
     * have an execution where two threads run in parallel and execute the code
     * responsible for the potential deadlock, which is what this test achieves.
     */
    void testLockInversionInSetVBucketStateAndNewProducer();

    SynchronousEPEngineUniquePtr engine;
    const Vbid vbid = Vbid(0);
};

// MB-33873: Test that we do not store stale references to a ConnHandler in the
// ConnMap. This could cause a seg fault if we don't check them before use.
TEST_F(DcpConnMapTest, StaleConnMapReferences) {
    {
        // We can put a MockDcpConnMap in the engine, but we have to move it
        // (inheritance with unique pointers is a pain).
        // We can just get it back out later if we jump through a couple of
        // hoops.
        auto mockConnMap = std::make_unique<MockDcpConnMap>(*engine);
        engine->setDcpConnMap(std::move(mockConnMap));
    }
    auto& connMap = engine->getDcpConnMap();
    auto& mockConnMap = dynamic_cast<MockDcpConnMap&>(connMap);

    auto* cookie = create_mock_cookie();
    // Create a new Dcp producer
    auto* producer = connMap.newProducer(cookie, "test_producer", 0 /*flags*/);

    // Bit of a test hack; when we close the stream we will only remove the
    // ConnHandler reference from the vbToConns map if we do not set
    // "send_stream_end_on_client_close_stream". We can purposefully leave it
    // in by setting this control flag.
    producer->control(
            0 /*opaque*/, "send_stream_end_on_client_close_stream", "true");

    // Create a stream
    uint64_t rollbackSeqno;
    producer->streamRequest(0 /*flags*/,
                            0 /*opaque*/,
                            Vbid(0),
                            0 /*startSeqno*/,
                            ~0 /*endSeqno*/,
                            0 /*vbUUID*/,
                            0 /*snapStart*/,
                            0 /*snapEnd*/,
                            &rollbackSeqno,
                            mock_dcp_add_failover_log,
                            {});

    // The ConnMap will add the "ep_dcpq" name prefix to our name
    ASSERT_TRUE(mockConnMap.doesVbConnExist(Vbid(0), "eq_dcpq:test_producer"));

    // Close it, the connection should still exist in the vbToConns map
    producer->closeStream(0, Vbid(0));
    ASSERT_TRUE(mockConnMap.doesVbConnExist(Vbid(0), "eq_dcpq:test_producer"));

    // Remove the connection, we should clean up the references in the
    // vbToConns map now
    connMap.disconnect(cookie);
    EXPECT_FALSE(mockConnMap.doesVbConnExist(Vbid(0), "eq_dcpq:test_producer"));

    destroy_mock_cookie(cookie);
}

/* Tests that there is no memory loss due to cyclic reference between connection
 * and other objects (like dcp streams). It is possible that connections are
 * deleted from the dcp connmap when dcp connmap is deleted due to abrupt
 * deletion of 'EventuallyPersistentEngine' obj.
 * This test simulates the abrupt deletion of dcp connmap object
 */
TEST_F(DcpConnMapTest, DeleteProducerOnUncleanDCPConnMapDelete) {
    /* Create a new Dcp producer */
    auto* dummyMockCookie = create_mock_cookie(engine.get());
    DcpProducer* producer = engine->getDcpConnMap().newProducer(dummyMockCookie,
                                                                "test_producer",
                                                                /*flags*/ 0);
    /* Open stream */
    uint64_t rollbackSeqno = 0;
    uint32_t opaque = 0;
    EXPECT_EQ(cb::engine_errc::success,
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
    auto* dummyMockCookie = create_mock_cookie(engine.get());
    DcpConsumer* consumer = engine->getDcpConnMap().newConsumer(
            dummyMockCookie, "test_consumer");

    /* Add passive stream */
    ASSERT_EQ(cb::engine_errc::success,
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

    auto* cookieA = create_mock_cookie(engine.get());
    auto* cookieB = create_mock_cookie(engine.get());

    ASSERT_EQ(cb::engine_errc::success,
              engine->getKVBucket()->setVBucketState(
                      vbid, vbucket_state_replica, {}, TransferVB::Yes));

    DcpConsumer* consumerA =
            connMap.newConsumer(cookieA, "test_consumerA", "test_consumerA");
    EXPECT_FALSE(connMap.doesVbConnExist(vbid, "eq_dcpq:test_consumerA"));
    consumerA->addStream(0xdead, vbid, 0);
    EXPECT_TRUE(connMap.doesVbConnExist(vbid, "eq_dcpq:test_consumerA"));
    // destroys the first consumer, leaving a weakptr in the vbConn map
    EXPECT_TRUE(connMap.removeConn(cookieA));

    // Create a new consumer, with a stream for the same VB
    DcpConsumer* consumerB =
            connMap.newConsumer(cookieB, "test_consumerB", "test_consumerB");
    EXPECT_FALSE(connMap.doesVbConnExist(vbid, "eq_dcpq:test_consumerB"));
    consumerB->addStream(0xbeef, vbid, 0);
    EXPECT_TRUE(connMap.doesVbConnExist(vbid, "eq_dcpq:test_consumerB"));

    // Here the ConnHandler added to connMap.vbConns in addStream should be
    // removed
    connMap.disconnect(cookieB);

    // Consumer B should not be in the vbConns any more
    EXPECT_FALSE(connMap.doesVbConnExist(vbid, "eq_dcpq:test_consumerB"));

    /* Cleanup the deadConnections */
    connMap.manageConnections();
    destroy_mock_cookie(cookieA);
    destroy_mock_cookie(cookieB);
}

// MB-35061 - Test to ensure the Producer ConnHandler is removed
// when a producer stream ends, and does not linger and become confused
// with a later Producer stream.
TEST_F(DcpConnMapTest, TestCorrectRemovedOnStreamEnd) {
    auto connMapPtr = std::make_unique<MockDcpConnMap>(*engine);
    MockDcpConnMap& connMap = *connMapPtr;
    engine->setDcpConnMap(std::move(connMapPtr));

    // create cookies
    auto* producerCookie = create_mock_cookie(engine.get());
    auto* consumerCookie = create_mock_cookie(engine.get());

    MockDcpMessageProducers producers;

    // create a producer (We are currently active)
    auto producer = engine->getDcpConnMap().newProducer(producerCookie,
                                                        "producerA",
                                                        /*flags*/ 0);

    producer->control(0xdead, "send_stream_end_on_client_close_stream", "true");

    uint64_t rollbackSeqno = 0;

    ASSERT_EQ(cb::engine_errc::success,
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

    EXPECT_TRUE(connMap.doesVbConnExist(vbid, "eq_dcpq:producerA"));

    // Close the stream. This will not remove the ConnHandler from
    // ConnMap.vbConns because we are waiting to send streamEnd.
    ASSERT_EQ(cb::engine_errc::success, producer->closeStream(0xdead, vbid));
    // Step to send the streamEnd, and remove the ConnHandler
    ASSERT_EQ(cb::engine_errc::success, producer->step(producers));

    // Move to replica
    ASSERT_EQ(cb::engine_errc::success,
              engine->getKVBucket()->setVBucketState(
                      vbid, vbucket_state_replica, {}, TransferVB::Yes));

    // confirm the ConnHandler was removed
    EXPECT_FALSE(connMap.doesVbConnExist(vbid, "eq_dcpq:producerA"));

    // Create a consumer (we are now a replica)
    DcpConsumer* consumer = connMap.newConsumer(
            consumerCookie, "test_consumerA", "test_consumerA");

    EXPECT_FALSE(connMap.doesVbConnExist(vbid, "eq_dcpq:test_consumerA"));

    // add a stream for the same VB as before
    ASSERT_EQ(cb::engine_errc::success, consumer->addStream(0xbeef, vbid, 0));
    EXPECT_TRUE(connMap.doesVbConnExist(vbid, "eq_dcpq:test_consumerA"));

    // End the stream. This should remove the Consumer ConnHandler from vbConns
    auto streamOpaque =
            static_cast<MockDcpConsumer*>(consumer)->getStreamOpaque(0xbeef);
    ASSERT_TRUE(streamOpaque);
    ASSERT_EQ(cb::engine_errc::success,
              consumer->streamEnd(
                      *streamOpaque, vbid, cb::mcbp::DcpStreamEndStatus::Ok));

    // expect neither ConnHandler remains in vbConns
    EXPECT_FALSE(connMap.doesVbConnExist(vbid, "eq_dcpq:producerA"));
    EXPECT_FALSE(connMap.doesVbConnExist(vbid, "eq_dcpq:test_consumerA"));

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
    auto* cookie = create_mock_cookie(engine.get());
    const uint32_t flags = 0;
    auto& connMap = dynamic_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    auto* producer = connMap.newProducer(cookie, "producer", flags);

    const uint32_t opaque = 0xdead;
    // Vbstate lock acquired in ActiveStream::setDead (executed by
    // DcpConnMap::disconnect) only if SyncRepl is enabled
    producer->control(opaque, "enable_sync_writes", "true");
    producer->control(opaque, "consumer_name", "consumer");

    uint64_t rollbackSeqno = 0;
    ASSERT_EQ(cb::engine_errc::success,
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

    EXPECT_TRUE(connMap.doesVbConnExist(vbid, "eq_dcpq:producer"));

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
    auto* cookie = create_mock_cookie(engine.get());
    const uint32_t flags = 0;
    auto& connMap = dynamic_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    auto* producer = connMap.newProducer(cookie, "producer", flags);

    const uint32_t opaque = 0xdead;
    // Vbstate lock acquired in ActiveStream::setDead (executed by
    // DcpConnMap::disconnect) only if SyncRepl is enabled
    producer->control(opaque, "enable_sync_writes", "true");
    producer->control(opaque, "consumer_name", "consumer");

    uint64_t rollbackSeqno = 0;
    ASSERT_EQ(cb::engine_errc::success,
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

    EXPECT_TRUE(connMap.doesVbConnExist(vbid, "eq_dcpq:producer"));

    std::thread t1 = std::thread([this]() -> void {
        engine->getKVBucket()->setVBucketState(
                vbid,
                vbucket_state_t::vbucket_state_replica,
                {} /*meta*/,
                TransferVB::No);
    });

    // Disconnect in this thread
    connMap.disconnect(cookie);
    destroy_mock_cookie(cookie);

    t1.join();

    // Check that streams have been shutdown at disconnect
    EXPECT_FALSE(connMap.doesVbConnExist(vbid, "eq_dcpq:producer"));

    // Cleanup
    connMap.manageConnections();
}

TEST_F(DcpConnMapTest, ConnAggStats) {
    // Test that ConnAggStats correctly aggregates stats by connection
    // "type" (taken from the connection name).
    auto connMapPtr = std::make_unique<MockDcpConnMap>(*engine);
    MockDcpConnMap& connMap = *connMapPtr;
    engine->setDcpConnMap(std::move(connMapPtr));

    // create cookies
    auto* producerCookie1 = create_mock_cookie(engine.get());
    auto* producerCookie2 = create_mock_cookie(engine.get());
    auto* consumerCookie = create_mock_cookie(engine.get());
    auto* statsCookie = create_mock_cookie(engine.get());

    // create producers
    // Producer for conn type "fts" - the type is not special, it's just a
    // string which will be extracted from the conn name.
    auto producer1 = std::make_shared<MockDcpProducer>(*engine,
                                                       producerCookie1,
                                                       "eq_dcpq:fts:foo",
                                                       /*flags*/ 0);

    // Oroducer for "views"
    auto producer2 = std::make_shared<MockDcpProducer>(*engine,
                                                       producerCookie2,
                                                       "eq_dcpq:views:bar",
                                                       /*flags*/ 0);
    // Create a consumer for conn type "replication"
    auto consumer = std::make_shared<MockDcpConsumer>(*engine,
                                                      consumerCookie,
                                                      "eq_dcpq:replication:baz",
                                                      "test_consumerA");

    // add conns to map
    connMap.addConn(producerCookie1, producer1);
    connMap.addConn(producerCookie2, producer2);
    connMap.addConn(consumerCookie, consumer);

    // manufacture specific stats to test they are aggregated
    // correctly
    auto producerBytes1 = 1234;
    auto producerBytes2 = 4321;
    auto consumerBackoffs = 1991;

    producer1->setTotalBtyesSent(producerBytes1);
    producer2->setTotalBtyesSent(producerBytes2);
    consumer->setNumBackoffs(consumerBackoffs);

    std::unordered_map<std::string, std::string> statsOutput;

    auto addStat = [&statsOutput](std::string_view key,
                                  std::string_view value,
                                  const void* ctx) {
        statsOutput.emplace(std::string(key), std::string(value));
    };

    // get the conn aggregated stats
    engine->doConnAggStats(
            CBStatCollector(addStat, statsCookie).forBucket("default"), ":");

    // expect output for each of the connection "types" and
    // a total output.
    std::unordered_map<std::string, std::string> expected{
            {"replication:total_bytes", "0"},
            {"fts:total_bytes", std::to_string(producerBytes1)},
            {"views:total_bytes", std::to_string(producerBytes2)},
            {":total:total_bytes",
             std::to_string(producerBytes1 + producerBytes2)},

            {"replication:producer_count", "0"},
            {"fts:producer_count", "1"},
            {"views:producer_count", "1"},
            {":total:producer_count", "2"},

            {"replication:count", "1"},
            {"fts:count", "1"},
            {"views:count", "1"},
            {":total:count", "3"},

            {"replication:backoff", std::to_string(consumerBackoffs)},
            {"fts:backoff", "0"},
            {"views:backoff", "0"},
            {":total:backoff", std::to_string(consumerBackoffs)},
    };

    for (const auto& [key, value] : expected) {
        auto itr = statsOutput.find(key);
        if (itr == statsOutput.end()) {
            FAIL() << "Stat \"" << key << "\" missing from output";
        }
        EXPECT_EQ(value, itr->second);
    }

    destroy_mock_cookie(producerCookie1);
    destroy_mock_cookie(producerCookie2);
    destroy_mock_cookie(consumerCookie);
    destroy_mock_cookie(statsCookie);
}

void DcpConnMapTest::testLockInversionInSetVBucketStateAndNewProducer() {
    auto& connMap = dynamic_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    auto* cookie = create_mock_cookie(engine.get());
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
        ASSERT_EQ(cb::engine_errc::success,
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
    EXPECT_TRUE(connMap.doesVbConnExist(vbid, "eq_dcpq:" + connName));

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
    auto* cookie2 = create_mock_cookie(engine.get());
    producer = connMap.newProducer(cookie2, connName, flags);
    ASSERT_TRUE(producer);
    // Check that the connection has been re-created with the same name
    // and exists in vbConns at stream-req
    {
        SCOPED_TRACE("");
        streamRequest(*producer);
    }
    EXPECT_TRUE(connMap.doesVbConnExist(vbid, "eq_dcpq:" + connName));

    t1.join();

    // Cleanup
    connMap.manageConnections();
    destroy_mock_cookie(cookie);
    destroy_mock_cookie(cookie2);
}

TEST_F(DcpConnMapTest,
       AvoidLockInversionInSetVBucketStateAndNewProducerExistingName) {
    testLockInversionInSetVBucketStateAndNewProducer();
}

class NotifyTest : public DCPTest {
};

class ConnMapNotifyTest {
public:
    explicit ConnMapNotifyTest(EventuallyPersistentEngine& engine)
        : connMap(new MockDcpConnMap(engine)),
          callbacks(0),
          cookie(create_mock_cookie(&engine)) {
        connMap->initialize();

        // Save `this` in server-specific so we can retrieve it from
        // dcp_test_notify_io_complete below:
        cookie->setEngineStorage(this);

        producer = connMap->newProducer(cookie,
                                        "test_producer",
                                        /*flags*/ 0);
    }

    ~ConnMapNotifyTest() {
        destroy_mock_cookie(cookie);
    }

    void notify() {
        callbacks++;
    }

    int getCallbacks() {
        return callbacks;
    }

    static void dcp_test_notify_io_complete(const CookieIface& cookie,
                                            cb::engine_errc status) {
        const auto* notifyTest = reinterpret_cast<const ConnMapNotifyTest*>(
                cookie.getEngineStorage());
        cb_assert(notifyTest != nullptr);
        const_cast<ConnMapNotifyTest*>(notifyTest)->notify();
    }

    std::unique_ptr<MockDcpConnMap> connMap;
    DcpProducer* producer;

private:
    int callbacks;
    CookieIface* cookie = nullptr;
};


TEST_F(NotifyTest, test_mb19503_connmap_notify) {
    ConnMapNotifyTest notifyTest(*engine);

    // Hook into notify_io_complete
    class MockServerCookieApi : public WrappedServerCookieIface {
    public:
        void notify_io_complete(const CookieIface& cookie,
                                cb::engine_errc status) override {
            ConnMapNotifyTest::dcp_test_notify_io_complete(cookie, status);
        }
        void scheduleDcpStep(const CookieIface& cookie) override {
            ConnMapNotifyTest::dcp_test_notify_io_complete(
                    cookie, cb::engine_errc::success);
        }
    } scapi;

    // Should be 0 when we begin
    ASSERT_EQ(0, notifyTest.getCallbacks());
    ASSERT_TRUE(notifyTest.producer->isPaused());

    // 1. notify the producer
    notifyTest.producer->scheduleNotify();

    // 2 One callback should've occurred
    EXPECT_EQ(1, notifyTest.getCallbacks());

    // notify the producer again
    notifyTest.producer->scheduleNotify();

    // 5. There should've been 2 callbacks
    EXPECT_EQ(2, notifyTest.getCallbacks());
}

// Variation on test_mb19503_connmap_notify - check that notification is correct
// when notifiable is not paused.
TEST_F(NotifyTest, test_mb19503_connmap_notify_paused) {
    ConnMapNotifyTest notifyTest(*engine);

    // Hook into notify_io_complete
    class MockServerCookieApi : public WrappedServerCookieIface {
    public:
        void notify_io_complete(const CookieIface& cookie,
                                cb::engine_errc status) override {
            ConnMapNotifyTest::dcp_test_notify_io_complete(cookie, status);
        }
        void scheduleDcpStep(const CookieIface& cookie) override {
            ConnMapNotifyTest::dcp_test_notify_io_complete(
                    cookie, cb::engine_errc::success);
        }
    } scapi;

    // Should be 0 when we begin
    ASSERT_EQ(notifyTest.getCallbacks(), 0);
    ASSERT_TRUE(notifyTest.producer->isPaused());

    // 1. Mark connection as not paused.
    notifyTest.producer->unPause();

    // 2. notify the connection - as the connection is not paused
    // this should *not* invoke notifyIOComplete.
    notifyTest.producer->scheduleNotify();

    // 3.1 Should have not had any callbacks.
    EXPECT_EQ(0, notifyTest.getCallbacks());

    // 4. Now mark the connection as paused.
    ASSERT_FALSE(notifyTest.producer->isPaused());
    notifyTest.producer->pause();

    // 4. notify the connection - as connection is
    //    //    paused this time we *should* get a callback.
    notifyTest.producer->scheduleNotify();
    EXPECT_EQ(1, notifyTest.getCallbacks());
}

TEST_P(ConnectionTest, ProducerEnablesDeleteXattr) {
    auto* cookie = create_mock_cookie();

    uint32_t flags = 0;
    {
        const auto producer = std::make_shared<MockDcpProducer>(
                *engine, cookie, "test_producer", flags);
        EXPECT_EQ(IncludeDeletedUserXattrs::No,
                  producer->public_getIncludeDeletedUserXattrs());
    }

    flags = cb::mcbp::request::DcpOpenPayload::IncludeDeletedUserXattrs;
    const auto producer = std::make_shared<MockDcpProducer>(
            *engine, cookie, "test_producer", flags);
    EXPECT_EQ(IncludeDeletedUserXattrs::Yes,
              producer->public_getIncludeDeletedUserXattrs());

    destroy_mock_cookie(cookie);
}

class ActiveStreamChkptProcessorTaskTest : public SingleThreadedKVBucketTest {
public:
    ActiveStreamChkptProcessorTaskTest()
        : cookie(create_mock_cookie(engine.get())) {
    }

    void SetUp() override {
        SingleThreadedKVBucketTest::SetUp();

        /* Start an active vb and add 3 items */
        store->setVBucketState(vbid, vbucket_state_active);
        addItems(3);

        producers = std::make_unique<MockDcpMessageProducers>();
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
        producer.reset();
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
    static cb::engine_errc fakeDcpAddFailoverLog(
            const std::vector<vbucket_failover_t>&) {
        return cb::engine_errc::success;
    }

    void notifyAndStepToCheckpoint() {
        SingleThreadedKVBucketTest::notifyAndStepToCheckpoint(*producer,
                                                              *producers);
    }

    CookieIface* cookie;
    std::unique_ptr<MockDcpMessageProducers> producers;
    std::shared_ptr<MockDcpProducer> producer;
    const Vbid vbid = Vbid(0);
};

TEST_F(ActiveStreamChkptProcessorTaskTest, DeleteDeadStreamEntry) {
    uint64_t rollbackSeqno;
    uint32_t opaque = 1;
    ASSERT_EQ(cb::engine_errc::success,
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
    ASSERT_EQ(cb::engine_errc::success,
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

    MockDcpMessageProducers producers;

    cb::mcbp::Response message{};
    message.setMagic(cb::mcbp::Magic::ClientResponse);
    for (auto status : {cb::mcbp::Status::NotMyVbucket,
                        cb::mcbp::Status::KeyEexists,
                        cb::mcbp::Status::KeyEnoent,
                        cb::mcbp::Status::DcpStreamNotFound,
                        cb::mcbp::Status::OpaqueNoMatch,
                        cb::mcbp::Status::Success}) {
        message.setStatus(status);
        for (auto op : {cb::mcbp::ClientOpcode::DcpOpen,
                        cb::mcbp::ClientOpcode::DcpAddStream,
                        cb::mcbp::ClientOpcode::DcpCloseStream,
                        cb::mcbp::ClientOpcode::DcpStreamReq,
                        cb::mcbp::ClientOpcode::DcpGetFailoverLog,
                        cb::mcbp::ClientOpcode::DcpMutation,
                        cb::mcbp::ClientOpcode::DcpDeletion,
                        cb::mcbp::ClientOpcode::DcpExpiration,
                        cb::mcbp::ClientOpcode::DcpBufferAcknowledgement,
                        cb::mcbp::ClientOpcode::DcpControl,
                        cb::mcbp::ClientOpcode::DcpSystemEvent,
                        cb::mcbp::ClientOpcode::GetErrorMap,
                        cb::mcbp::ClientOpcode::DcpPrepare}) {
            message.setOpcode(op);
            EXPECT_TRUE(producer->handleResponse(message));
        }
    }
    // We should disconnect when we see cb::mcbp::Status::KeyEnoent for
    // a durability DCP op
    for (auto op : {cb::mcbp::ClientOpcode::DcpCommit,
                    cb::mcbp::ClientOpcode::DcpAbort}) {
        message.setOpcode(op);

        for (auto status : {cb::mcbp::Status::KeyEexists,
                            cb::mcbp::Status::NotMyVbucket,
                            cb::mcbp::Status::DcpStreamNotFound,
                            cb::mcbp::Status::OpaqueNoMatch,
                            cb::mcbp::Status::Success}) {
            message.setStatus(status);

            EXPECT_TRUE(producer->handleResponse(message));
        }
    }
}

// Test that we disconnect whe we receive a non success status code for the
// majority of Dcp Opcodes.
TEST_F(SingleThreadedKVBucketTest, ProducerHandleResponseDisconnect) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto producer = std::make_shared<MockDcpProducer>(
            *engine, cookie, "ProducerHandleResponceDiscconnect", 0);
    MockDcpMessageProducers producers;

    cb::mcbp::Response message{};
    message.setMagic(cb::mcbp::Magic::ClientResponse);
    for (auto errorCode : {cb::mcbp::Status::E2big,
                           cb::mcbp::Status::Einval,
                           cb::mcbp::Status::Enomem,
                           cb::mcbp::Status::Erange,
                           cb::mcbp::Status::Etmpfail,
                           cb::mcbp::Status::Locked,
                           cb::mcbp::Status::SyncWriteAmbiguous,
                           cb::mcbp::Status::SyncWriteInProgress,
                           cb::mcbp::Status::SyncWriteReCommitInProgress,
                           cb::mcbp::Status::UnknownCollection}) {
        message.setStatus(errorCode);
        for (auto op : {cb::mcbp::ClientOpcode::DcpOpen,
                        cb::mcbp::ClientOpcode::DcpAddStream,
                        cb::mcbp::ClientOpcode::DcpCloseStream,
                        cb::mcbp::ClientOpcode::DcpStreamReq,
                        cb::mcbp::ClientOpcode::DcpGetFailoverLog,
                        cb::mcbp::ClientOpcode::DcpMutation,
                        cb::mcbp::ClientOpcode::DcpDeletion,
                        cb::mcbp::ClientOpcode::DcpExpiration,
                        cb::mcbp::ClientOpcode::DcpBufferAcknowledgement,
                        cb::mcbp::ClientOpcode::DcpControl,
                        cb::mcbp::ClientOpcode::DcpSystemEvent,
                        cb::mcbp::ClientOpcode::GetErrorMap,
                        cb::mcbp::ClientOpcode::DcpPrepare,
                        cb::mcbp::ClientOpcode::DcpCommit,
                        cb::mcbp::ClientOpcode::DcpAbort}) {
            message.setOpcode(op);
            EXPECT_FALSE(producer->handleResponse(message));
        }
    }
    message.setStatus(cb::mcbp::Status::KeyEnoent);
    for (auto op : {cb::mcbp::ClientOpcode::DcpCommit,
                    cb::mcbp::ClientOpcode::DcpAbort}) {
        message.setOpcode(op);
        EXPECT_FALSE(producer->handleResponse(message));
    }
}

// Test how we handle DcpStreamEnd responses from a consumer
TEST_F(SingleThreadedKVBucketTest, ProducerHandleResponseStreamEnd) {
    auto producer = std::make_shared<MockDcpProducer>(
            *engine, cookie, "ProducerHandleResponceStreamEnd", 0);
    MockDcpMessageProducers producers;

    cb::mcbp::Response message{};
    message.setMagic(cb::mcbp::Magic::ClientResponse);
    message.setOpcode(cb::mcbp::ClientOpcode::DcpStreamEnd);
    for (auto errorCode : {cb::mcbp::Status::KeyEnoent,
                           cb::mcbp::Status::KeyEexists,
                           cb::mcbp::Status::DcpStreamNotFound,
                           cb::mcbp::Status::OpaqueNoMatch,
                           cb::mcbp::Status::NotMyVbucket,
                           cb::mcbp::Status::Success}) {
        message.setStatus(errorCode);
        EXPECT_TRUE(producer->handleResponse(message));
    }
    for (auto errorCode : {cb::mcbp::Status::E2big,
                           cb::mcbp::Status::Einval,
                           cb::mcbp::Status::Enomem,
                           cb::mcbp::Status::Erange,
                           cb::mcbp::Status::Etmpfail,
                           cb::mcbp::Status::Locked,
                           cb::mcbp::Status::SyncWriteAmbiguous,
                           cb::mcbp::Status::SyncWriteInProgress,
                           cb::mcbp::Status::SyncWriteReCommitInProgress,
                           cb::mcbp::Status::UnknownCollection}) {
        message.setStatus(errorCode);
        EXPECT_FALSE(producer->handleResponse(message));
    }
}

// Test how we handle DcpNoop responses from a consumer
TEST_F(SingleThreadedKVBucketTest, ProducerHandleResponseNoop) {
    auto producer = std::make_shared<MockDcpProducer>(
            *engine, cookie, "ProducerHandleResponceNoop", 0);
    MockDcpMessageProducers producers;

    cb::mcbp::Response message{};
    message.setMagic(cb::mcbp::Magic::ClientResponse);
    message.setOpcode(cb::mcbp::ClientOpcode::DcpNoop);

    for (auto errorCode : {cb::mcbp::Status::E2big,
                           cb::mcbp::Status::Einval,
                           cb::mcbp::Status::Enomem,
                           cb::mcbp::Status::Erange,
                           cb::mcbp::Status::Etmpfail,
                           cb::mcbp::Status::Locked,
                           cb::mcbp::Status::Success,
                           cb::mcbp::Status::SyncWriteAmbiguous,
                           cb::mcbp::Status::SyncWriteInProgress,
                           cb::mcbp::Status::SyncWriteReCommitInProgress,
                           cb::mcbp::Status::UnknownCollection}) {
        message.setStatus(errorCode);
        // Test DcpNoop when the opaque is the default opaque value
        message.setOpaque(10000000);
        EXPECT_TRUE(producer->handleResponse(message));
        for (uint32_t Opaque : {123, 0}) {
            message.setOpaque(Opaque);
            EXPECT_FALSE(producer->handleResponse(message));
        }
    }

    for (auto errorCode : {cb::mcbp::Status::NotMyVbucket,
                           cb::mcbp::Status::KeyEexists,
                           cb::mcbp::Status::KeyEnoent,
                           cb::mcbp::Status::DcpStreamNotFound,
                           cb::mcbp::Status::OpaqueNoMatch}) {
        message.setStatus(errorCode);
        // Test DcpNoop when the opaque is the default opaque value
        message.setOpaque(10000000);
        EXPECT_TRUE(producer->handleResponse(message));
        for (uint32_t Opaque : {123, 0}) {
            message.setOpaque(Opaque);
            EXPECT_TRUE(producer->handleResponse(message));
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

void FlowControlTest::SetUp() {
    flowControlEnabled = GetParam();
    const std::string policy = flowControlEnabled ? "aggressive" : "none";
    config_string = "dcp_flow_control_policy=" + policy;
    KVBucketTest::SetUp();
}

TEST_P(FlowControlTest, NotifyConsumerOnlyIfFlowControlEnabled) {
    uint32_t opaque = 0;
    engine->getKVBucket()->setVBucketState(vbid, vbucket_state_replica);

    const auto connName = "test_consumer";
    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, connName);
    ASSERT_EQ(flowControlEnabled, consumer->public_flowControl().isEnabled());

    // Add to consumer to ConnMap so that we can test whether the connection
    // is scheduled for notifying by checking if it is added to the 'pending
    // notifications' in the ConnMap itself
    auto& connMap = static_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    connMap.addConn(cookie, consumer);
    ASSERT_TRUE(connMap.findByName(connName));

    // If FlowControl is enabled, connection are added for notification only if
    // the buffer is sufficiently drained. Setting the buffer size to 0 makes
    // the buffer sufficiently drained at any received DCP message.
    if (flowControlEnabled) {
        consumer->public_flowControl().setFlowControlBufSize(0);
    }

    // Setup the stream
    ASSERT_EQ(cb::engine_errc::success,
              consumer->addStream(opaque, vbid, 0 /*flags*/));
    opaque += 1;
    ASSERT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(opaque,
                                       vbid,
                                       1,
                                       10,
                                       0x1 /* in-memory snapshot */,
                                       {} /*HCS*/,
                                       {} /*maxVisibleSeq*/));
    const DocKey docKey{nullptr, 0, DocKeyEncodesCollectionId::No};

    // Receive a mutation
    // Note: Only paused connections are added to the pending notifications
    consumer->pause();
    ASSERT_EQ(cb::engine_errc::success,
              consumer->mutation(opaque,
                                 {"key", DocKeyEncodesCollectionId::No},
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

    // Before the fix the consumer would be notified even when Flow Control is
    // disabled.
    EXPECT_EQ(flowControlEnabled,
              mock_cookie_notified(cookie_to_mock_cookie(cookie)));

    connMap.removeConn(cookie);
}

INSTANTIATE_TEST_SUITE_P(FlowControl,
                         FlowControlTest,
                         ::testing::ValuesIn({false, true}));

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
INSTANTIATE_TEST_SUITE_P(
        CompressionStreamTest,
        CompressionStreamTest,
        ::testing::Combine(EPEngineParamTest::allConfigValues(),
                           ::testing::Bool()),
        PrintToStringCombinedNameXattrOnOff());

INSTANTIATE_TEST_SUITE_P(PersistentAndEphemeral,
                         ConnectionTest,
                         STParameterizedBucketTest::allConfigValues());
