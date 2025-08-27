/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp/response.h"
#include "evp_store_single_threaded_test.h"
#include "failover-table.h"
#include "item.h"
#include "tests/mock/mock_cache_transfer_stream.h"
#include "tests/mock/mock_dcp.h"
#include "tests/mock/mock_dcp_producer.h"
#include "tests/module_tests/test_helpers.h"
#include "vbucket.h"
#include <utilities/test_manifest.h>

#include <unordered_set>

class DcpCacheTransferTest : public STParameterizedBucketTest {
public:
    void SetUp() override {
        SingleThreadedKVBucketTest::SetUp();
        setVBucketStateAndRunPersistTask(Vbid(0), vbucket_state_active);
        producer = createDcpProducer(
                cookie, IncludeDeleteTime::Yes, true, "DcpCacheTransferTest");

        // Insert one item that should be found by all tests
        expectedItems.insert(store_item(Vbid(0), makeStoredDocKey("1"), "1"));
    }

    void TearDown() override {
        producer.reset();
        SingleThreadedKVBucketTest::TearDown();
    }

    std::shared_ptr<MockDcpProducer> producer;

    std::shared_ptr<MockCacheTransferStream> createStream(
            MockDcpProducer& producer,
            uint32_t opaque,
            Vbid vbid,
            uint64_t maxSeqno,
            IncludeValue includeValue) {
        Collections::VB::Filter f(
                "", store->getVBucket(vbid)->getManifest(), *cookie, *engine);
        auto stream =
                producer.mockCacheTransferStreamRequest(1,
                                                        Vbid(0),
                                                        maxSeqno,
                                                        0 /*uuid not used*/,
                                                        includeValue,
                                                        std::move(f));
        EXPECT_TRUE(stream->isActive());
        EXPECT_EQ(0, stream->getItemsRemaining());
        EXPECT_FALSE(stream->next(producer));
        stream->setActive();
        return stream;
    }

    void runCacheTransferTask() {
        auto& nonioQueue = *task_executor->getLpTaskQ(TaskType::NonIO);
        runNextTask(nonioQueue,
                    "DCP (Producer) DcpCacheTransferTest - CacheTransferTask "
                    "for vb:0");
    }

    std::unordered_set<Item> expectedItems;
};

TEST_P(DcpCacheTransferTest, basic_stream) {
    expectedItems.insert(store_item(Vbid(0), makeStoredDocKey("2"), "2"));
    auto stream = createStream(*producer,
                               1,
                               Vbid(0),
                               store->getVBucket(vbid)->getHighSeqno(),
                               IncludeValue::Yes);
    runCacheTransferTask();
    // Should find 2 items and 1 stream-end
    ASSERT_EQ(3, stream->getItemsRemaining());
    EXPECT_TRUE(stream->validateNextResponse(expectedItems));
    EXPECT_TRUE(stream->validateNextResponse(expectedItems));
    EXPECT_TRUE(stream->validateNextResponseIsEnd());
}

TEST_P(DcpCacheTransferTest, basic_stream2) {
    store_item(Vbid(0), makeStoredDocKey("2"), "2");
    auto stream = createStream(*producer, 1, Vbid(0), 1, IncludeValue::Yes);
    runCacheTransferTask();
    // Should find 1 items and 1 stream-end
    ASSERT_EQ(2, stream->getItemsRemaining());
    EXPECT_TRUE(stream->validateNextResponse(expectedItems));
    EXPECT_TRUE(stream->validateNextResponseIsEnd());
}

TEST_P(DcpCacheTransferTest, ignore_deleted_items) {
    store_item(Vbid(0), makeStoredDocKey("2"), "2");
    delete_item(Vbid(0), makeStoredDocKey("2"));

    // Must not find the deleted item
    auto stream = createStream(*producer,
                               1,
                               Vbid(0),
                               store->getVBucket(vbid)->getHighSeqno(),
                               IncludeValue::Yes);
    runCacheTransferTask();
    // Should find 1 items and 1 stream-end
    EXPECT_EQ(2, stream->getItemsRemaining());
    EXPECT_TRUE(stream->validateNextResponse(expectedItems));
    EXPECT_TRUE(stream->validateNextResponseIsEnd());
}

TEST_P(DcpCacheTransferTest, ignore_evicted_items) {
    store_item(Vbid(0), makeStoredDocKey("2"), "2");
    flushVBucketToDiskIfPersistent(vbid, 2); // must flush so we can evict
    evict_key(Vbid(0), makeStoredDocKey("2"));

    // Must not find the evicted item
    auto stream = createStream(*producer,
                               1,
                               Vbid(0),
                               store->getVBucket(vbid)->getHighSeqno(),
                               IncludeValue::Yes);
    runCacheTransferTask();
    // Should find 1 items and 1 stream-end
    ASSERT_EQ(2, stream->getItemsRemaining());
    EXPECT_TRUE(stream->validateNextResponse(expectedItems));
    EXPECT_TRUE(stream->validateNextResponseIsEnd());
}

TEST_P(DcpCacheTransferTest, ignore_prepares) {
    setVBucketState(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});
    store_pending_item(Vbid(0), makeStoredDocKey("2"), "2");

    // Must not find the prepare
    auto stream = createStream(*producer,
                               1,
                               Vbid(0),
                               store->getVBucket(vbid)->getHighSeqno(),
                               IncludeValue::Yes);
    runCacheTransferTask();
    // Should find 1 items and 1 stream-end
    ASSERT_EQ(2, stream->getItemsRemaining());
    EXPECT_TRUE(stream->validateNextResponse(expectedItems));
    EXPECT_TRUE(stream->validateNextResponseIsEnd());
}

TEST_P(DcpCacheTransferTest, dropped_collection) {
    CollectionsManifest cm{};
    setCollections(cookie, cm.add(CollectionEntry::fruit));
    store_item(Vbid(0), makeStoredDocKey("2", CollectionEntry::fruit), "2");
    setCollections(cookie, cm.remove(CollectionEntry::fruit));

    // Must not find the prepare
    auto stream = createStream(*producer,
                               1,
                               Vbid(0),
                               store->getVBucket(vbid)->getHighSeqno(),
                               IncludeValue::Yes);
    runCacheTransferTask();
    // Should find 1 items and 1 stream-end
    ASSERT_EQ(2, stream->getItemsRemaining());
    EXPECT_TRUE(stream->validateNextResponse(expectedItems));
    EXPECT_TRUE(stream->validateNextResponseIsEnd());
}

TEST_P(DcpCacheTransferTest, oom) {
    // Need static clock for this test so we can run the sleeping task
    cb::time::steady_clock::use_chrono = false;
    auto scopeGuard = folly::makeGuard(
            []() { cb::time::steady_clock::use_chrono = true; });

    expectedItems.insert(store_item(Vbid(0), makeStoredDocKey("2"), "2"));

    // Must not find the deleted item
    auto stream = createStream(*producer,
                               1,
                               Vbid(0),
                               store->getVBucket(vbid)->getHighSeqno(),
                               IncludeValue::Yes);
    int callbacks = 0;
    stream->preQueueCallback = [this, &stream, &callbacks](const auto&) {
        ++callbacks;
        stream->memoryUsedOffset = 0;
        if (callbacks == 1) {
            // After queueing the first item, this will force the OOM check to
            // yield the task.
            stream->memoryUsedOffset = engine->getEpStats().getMaxDataSize();
        }
    };
    // Runs, one item can transfer, then OOM
    runCacheTransferTask();
    ASSERT_EQ(1, stream->getItemsRemaining());
    // Task currently yields for 0.5 second during OOM.
    cb::time::steady_clock::advance(std::chrono::seconds(1));
    // Should find 2 items and 1 stream-end
    runCacheTransferTask();
    ASSERT_EQ(3, stream->getItemsRemaining());
    EXPECT_TRUE(stream->validateNextResponse(expectedItems));
    EXPECT_TRUE(stream->validateNextResponse(expectedItems));
    EXPECT_TRUE(stream->validateNextResponseIsEnd());
}

TEST_P(DcpCacheTransferTest, skip_expired_items) {
    // Need static clock for this test
    cb::time::steady_clock::use_chrono = false;
    auto scopeGuard = folly::makeGuard(
            []() { cb::time::steady_clock::use_chrono = true; });

    store_item(Vbid(0), makeStoredDocKey("2"), "2", 200);

    // Must not find the deleted item
    auto stream = createStream(*producer,
                               1,
                               Vbid(0),
                               store->getVBucket(vbid)->getHighSeqno(),
                               IncludeValue::Yes);

    // Force expiry by moving the clock forward
    cb::time::steady_clock::advance(std::chrono::seconds(300));
    runCacheTransferTask();
    // Should find 1 item and 1 stream-end
    ASSERT_EQ(2, stream->getItemsRemaining());
    EXPECT_TRUE(stream->validateNextResponse(expectedItems));
    EXPECT_TRUE(stream->validateNextResponseIsEnd());
    ASSERT_TRUE(expectedItems.empty());
}

// Validate CacheTranfer via DcpProducer::streamRequest
// The test requests a cache transfer upto and including the high seqno.
TEST_P(DcpCacheTransferTest, viaStreamRequest) {
    store_item(Vbid(0), makeStoredDocKey("k2"), "2");

    EXPECT_EQ(cb::engine_errc::success,
              producer->streamRequest(
                      cb::mcbp::DcpAddStreamFlag::CacheTransfer,
                      1,
                      Vbid(0),
                      store->getVBucket(vbid)->getHighSeqno(),
                      store->getVBucket(vbid)->getHighSeqno(),
                      store->getVBucket(vbid)->failovers->getLatestUUID(),
                      0,
                      store->getVBucket(vbid)->getHighSeqno(),
                      nullptr,
                      mock_dcp_add_failover_log,
                      std::nullopt));

    runCacheTransferTask();

    MockDcpMessageProducers producers;

    // There is no order... check two keys and values are found, no more, no
    // less.
    std::unordered_map<std::string, std::pair<std::string, uint64_t>>
            expectedValues = {{"1", {"1", 1}}, {"k2", {"2", 2}}};

    while (!expectedValues.empty()) {
        EXPECT_EQ(cb::engine_errc::success,
                  producer->stepAndExpect(producers,
                                          cb::mcbp::ClientOpcode::DcpMutation));
        // Check that the key is one of the expected keys
        auto keyIt = expectedValues.find(producers.last_key);
        ASSERT_NE(keyIt, expectedValues.end())
                << "Unexpected key: " << producers.last_key;
        // Check that the value matches the key
        EXPECT_EQ(expectedValues[producers.last_key].first,
                  producers.last_value);
        EXPECT_EQ(expectedValues[producers.last_key].second,
                  producers.last_byseqno);
        // Remove the key so we don't see it again
        expectedValues.erase(keyIt);
    }

    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(producers,
                                      cb::mcbp::ClientOpcode::DcpStreamEnd));
    EXPECT_EQ(cb::mcbp::DcpStreamEndStatus::Ok, producers.last_end_status);
}

TEST_P(DcpCacheTransferTest, viaStreamRequest_with_filter) {
    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::vegetable));
    store_item(Vbid(0), makeStoredDocKey("veg", CollectionUid::vegetable), "v");

    EXPECT_EQ(cb::engine_errc::success,
              producer->streamRequest(
                      cb::mcbp::DcpAddStreamFlag::CacheTransfer,
                      1,
                      Vbid(0),
                      store->getVBucket(vbid)->getHighSeqno(),
                      store->getVBucket(vbid)->getHighSeqno(),
                      store->getVBucket(vbid)->failovers->getLatestUUID(),
                      0,
                      store->getVBucket(vbid)->getHighSeqno(),
                      nullptr,
                      mock_dcp_add_failover_log,
                      R"({"collections":["a"]})"));

    runCacheTransferTask();

    MockDcpMessageProducers producers;
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(producers,
                                      cb::mcbp::ClientOpcode::DcpMutation));
    // seq is 3 as create-veg added a system-event
    EXPECT_EQ(3, producers.last_byseqno);
    EXPECT_EQ(CollectionUid::vegetable, producers.last_collection_id);
    EXPECT_EQ("veg", producers.last_key);
    EXPECT_EQ("v", producers.last_value);

    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(producers,
                                      cb::mcbp::ClientOpcode::DcpStreamEnd));
    EXPECT_EQ(cb::mcbp::DcpStreamEndStatus::Ok, producers.last_end_status);
}

INSTANTIATE_TEST_SUITE_P(DcpCacheTransferTest,
                         DcpCacheTransferTest,
                         STParameterizedBucketTest::allConfigValuesNoNexus(),
                         STParameterizedBucketTest::PrintToStringParamName);
