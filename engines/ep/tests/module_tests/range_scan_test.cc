/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "ep_bucket.h"
#include "ep_vb.h"
#include "kvstore/kvstore.h"
#include "range_scans/range_scan.h"
#include "range_scans/range_scan_callbacks.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"
#include "tests/module_tests/test_helpers.h"
#include "vbucket.h"

#include <programs/engine_testapp/mock_server.h>
#include <utilities/test_manifest.h>

#include <unordered_set>
#include <vector>

// A handler implementation that just stores the scan key/items in vectors
class TestRangeScanHandler : public RangeScanDataHandlerIFace {
public:
    void handleKey(DocKey key) override {
        scannedKeys.emplace_back(key);
        testHook(scannedKeys.size());
    }

    void handleItem(std::unique_ptr<Item> item) override {
        scannedItems.emplace_back(std::move(item));
        testHook(scannedItems.size());
    }

    void handleStatus(cb::engine_errc status) override {
        EXPECT_TRUE(validateStatus(status));
        this->status = status;
    }

    void validateKeyScan(const std::unordered_set<StoredDocKey>& expectedKeys);
    void validateItemScan(const std::unordered_set<StoredDocKey>& expectedKeys);

    // check for allowed/expected status code
    static bool validateStatus(cb::engine_errc code);

    std::function<void(size_t)> testHook = [](size_t) {};
    std::vector<std::unique_ptr<Item>> scannedItems;
    std::vector<StoredDocKey> scannedKeys;
    // default to some status RangeScan won't use
    cb::engine_errc status{cb::engine_errc::sync_write_ambiguous};
};

class RangeScanTest
    : public SingleThreadedEPBucketTest,
      public ::testing::WithParamInterface<
              std::tuple<std::string, std::string, std::string>> {
public:
    void SetUp() override {
        config_string += generateBackendConfig(std::get<0>(GetParam()));
        config_string += ";item_eviction_policy=" + getEvictionMode();
#ifdef EP_USE_MAGMA
        config_string += ";" + magmaRollbackConfig;
#endif
        SingleThreadedEPBucketTest::SetUp();

        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

        // Setup collections and store keys
        CollectionsManifest cm;
        cm.add(CollectionEntry::vegetable);
        cm.add(CollectionEntry::fruit);
        cm.add(CollectionEntry::dairy);
        setCollections(cookie, cm);
        flush_vbucket_to_disk(vbid, 3);
        storeTestKeys();
    }

    static std::string PrintToStringParamName(
            const ::testing::TestParamInfo<ParamType>& info) {
        return std::get<0>(info.param) + "_" + std::get<1>(info.param) + "_" +
               std::get<2>(info.param);
    }

    std::string getEvictionMode() const {
        return std::get<1>(GetParam());
    }

    bool isKeyOnly() const {
        return std::get<2>(GetParam()) == "key_scan";
    }

    cb::rangescan::KeyOnly getScanType() const {
        return std::get<2>(GetParam()) == "key_scan"
                       ? cb::rangescan::KeyOnly::Yes
                       : cb::rangescan::KeyOnly::No;
    }

    cb::rangescan::Id createScan(CollectionID cid,
                                 cb::rangescan::KeyView start,
                                 cb::rangescan::KeyView end);

    const std::unordered_set<StoredDocKey> getUserKeys() {
        // Create a number of user prefixed collections and place them in the
        // collection that we will scan.
        std::unordered_set<StoredDocKey> keys;
        for (const auto& k :
             {"user-alan", "useralan", "user.claire", "user::zoe", "users"}) {
            keys.emplace(makeStoredDocKey(k, scanCollection));
        }
        return keys;
    }

    /**
     * generate a vector containing all of the keys which will be stored before
     * the test runs. Tests can then scan for these using various start/end
     * patterns
     */
    const std::vector<StoredDocKey> generateTestKeys() {
        std::vector<StoredDocKey> keys;

        for (const auto& k : getUserKeys()) {
            keys.push_back(k);
            keys.push_back(makeStoredDocKey(k.c_str(), collection2));
            keys.push_back(makeStoredDocKey(k.c_str(), collection3));
        }

        // Add some other keys, one above and below user and then some at
        // further ends of the alphabet
        for (const auto& k : {"useq", "uses", "abcd", "uuu", "uuuu", "xyz"}) {
            keys.push_back(makeStoredDocKey(k, scanCollection));
            keys.push_back(makeStoredDocKey(k, collection2));
            keys.push_back(makeStoredDocKey(k, collection3));
        }
        // Some stuff in other collections, no real meaning to this, just other
        // data we should never hit in the scan
        for (const auto& k : {"1000", "718", "ZOOM", "U", "@@@@"}) {
            keys.push_back(makeStoredDocKey(k, collection2));
            keys.push_back(makeStoredDocKey(k, collection3));
        }
        return keys;
    }

    void storeTestKeys() {
        for (const auto& key : generateTestKeys()) {
            // store one key with xattrs to check it gets stripped
            if (key == makeStoredDocKey("users", scanCollection)) {
                store_item(vbid,
                           key,
                           createXattrValue(key.to_string()),
                           0,
                           {cb::engine_errc::success},
                           PROTOCOL_BINARY_DATATYPE_XATTR);
            } else {
                // Store key with StoredDocKey::to_string as the value
                store_item(vbid, key, key.to_string());
            }
        }
        flushVBucket(vbid);
    }

    // Run a scan using the relatively low level pieces
    void testRangeScan(const std::unordered_set<StoredDocKey>& expectedKeys,
                       CollectionID cid,
                       cb::rangescan::KeyView start,
                       cb::rangescan::KeyView end);

    void testLessThan(std::string key);

    // Tests all scan against the following collection
    const CollectionID scanCollection = CollectionEntry::vegetable.getId();
    // Tests also have data in these collections, and these deliberately enclose
    // the vegetable collection
    const CollectionID collection2 = CollectionEntry::fruit.getId();
    const CollectionID collection3 = CollectionEntry::dairy.getId();

    std::unique_ptr<TestRangeScanHandler> handler{
            std::make_unique<TestRangeScanHandler>()};
};

void TestRangeScanHandler::validateKeyScan(
        const std::unordered_set<StoredDocKey>& expectedKeys) {
    EXPECT_TRUE(scannedItems.empty());
    EXPECT_EQ(expectedKeys.size(), scannedKeys.size());
    for (const auto& key : scannedKeys) {
        // Expect to find the key
        EXPECT_EQ(1, expectedKeys.count(key));
    }
}

void TestRangeScanHandler::validateItemScan(
        const std::unordered_set<StoredDocKey>& expectedKeys) {
    EXPECT_TRUE(scannedKeys.empty());
    EXPECT_EQ(expectedKeys.size(), scannedItems.size());
    for (const auto& scanItem : scannedItems) {
        auto itr = expectedKeys.find(scanItem->getKey());
        // Expect to find the key
        EXPECT_NE(itr, expectedKeys.end());
        // And the value of StoredDocKey::to_string should equal the value
        EXPECT_EQ(itr->to_string(), scanItem->getValueView());
    }
}

cb::rangescan::Id RangeScanTest::createScan(CollectionID cid,
                                            cb::rangescan::KeyView start,
                                            cb::rangescan::KeyView end) {
    auto vb = store->getVBucket(vbid);
    // Create a new RangeScan object and give it a handler we can inspect.
    EXPECT_EQ(cb::engine_errc::would_block,
              vb->createRangeScan(
                      cid, start, end, *handler, *cookie, getScanType()));

    // Now run via auxio task
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "RangeScanCreateTask");

    EXPECT_EQ(cb::engine_errc::success, mock_waitfor_cookie(cookie));

    // Next frontend will add the uuid/scan, client can be informed of the uuid
    auto& epVb = dynamic_cast<EPVBucket&>(*vb);
    auto status = epVb.createRangeScanComplete(*cookie);
    EXPECT_EQ(cb::engine_errc::success, status.first);

    auto scan = epVb.getRangeScan(status.second);
    EXPECT_TRUE(scan);
    return scan->getUuid();
}

// This method drives a range scan through create/continue/cancel for the given
// range. The test drives a range scan serially and the comments indicate where
// a frontend thread would be executing and where a background I/O task would.
void RangeScanTest::testRangeScan(
        const std::unordered_set<StoredDocKey>& expectedKeys,
        CollectionID cid,
        cb::rangescan::KeyView start,
        cb::rangescan::KeyView end) {
    // 1) create a RangeScan to scan the user prefixed keys.
    auto uuid = createScan(cid, start, end);

    auto vb = store->getVBucket(vbid);

    // 2) Continue a RangeScan
    // 2.1) Frontend thread would call this method using clients uuid
    EXPECT_EQ(cb::engine_errc::would_block,
              vb->continueRangeScan(uuid, *cookie));

    // 2.2) An I/O task now reads data from disk
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "RangeScanContinueTask");

    // 2.3) All expected keys must have been read from disk (no limits yet)
    if (isKeyOnly()) {
        handler->validateKeyScan(expectedKeys);
    } else {
        handler->validateItemScan(expectedKeys);
    }
    // status was set to success
    EXPECT_EQ(cb::engine_errc::success, handler->status);

    // In this case the scan finished and cleaned up

    // Check scan is gone, cannot be cancelled again
    EXPECT_EQ(cb::engine_errc::no_such_key, vb->cancelRangeScan(uuid, true));

    // Or continued, uuid is unknown
    EXPECT_EQ(cb::engine_errc::no_such_key,
              vb->continueRangeScan(uuid, *cookie));
}

// Scan for the user prefixed keys
TEST_P(RangeScanTest, user_prefix) {
    testRangeScan(getUserKeys(), scanCollection, {"user"}, {"user\xFF"});
}

// Test ensures callbacks cover disk read case
TEST_P(RangeScanTest, user_prefix_evicted) {
    for (const auto& key : generateTestKeys()) {
        evict_key(vbid, key);
    }
    testRangeScan(getUserKeys(), scanCollection, {"user"}, {"user\xFF"});
}

// Run a >= user scan by setting the keys to user and the end (255)
TEST_P(RangeScanTest, greater_than_or_equal) {
    auto expectedKeys = getUserKeys();
    auto rangeStart = makeStoredDocKey("user", scanCollection);
    for (const auto& key : generateTestKeys()) {
        // to_string returns a debug "cid:key", but >= will select the
        // correct keys for this text
        if (key.getCollectionID() == scanCollection &&
            key.to_string() >= rangeStart.to_string()) {
            expectedKeys.emplace(key);
        }
    }

    testRangeScan(expectedKeys, scanCollection, {"user"}, {"\xFF"});
}

// Run a <= user scan y setting the keys to 0 and user\xFF
TEST_P(RangeScanTest, less_than_or_equal) {
    auto expectedKeys = getUserKeys();
    auto rangeEnd = makeStoredDocKey("user\xFF", scanCollection);
    for (const auto& key : generateTestKeys()) {
        // to_string returns a debug "cid:key", but <= will select the
        // correct keys for this text
        if (key.getCollectionID() == scanCollection &&
            key.to_string() <= rangeEnd.to_string()) {
            expectedKeys.emplace(key);
        }
    }

    // note: start is ensuring the key is byte 0 with a length of 1
    testRangeScan(expectedKeys, scanCollection, {"\0", 1}, {"user\xFF"});
}

// Perform > uuu, this simulates a request for an exclusive start range-scan
TEST_P(RangeScanTest, greater_than) {
    // Here the client could of specified "aaa" and flag to set exclusive-start
    // so we set the start to "skip" aaa and start from the next key

    // This test kind of walks through how a client may be resuming after the
    // scan being destroyed for some reason (restart/rebalance).
    // key "uuu" is the last received key, so they'd like to receive in the next
    // scan all keys greater then uuu, but not uuu itself (exclusive start or >)
    std::string key = "uuu";

    // In this case the client requests exclusive start and we manipulate the
    // key to achieve exactly that by appending the value of 0
    key += char(0);
    auto rangeStart = makeStoredDocKey(key, scanCollection);

    // Let's also store rangeStart as if a client had written such a key (it's)
    // possible.
    store_item(vbid, rangeStart, rangeStart.to_string());
    flushVBucket(vbid);

    // So now generate the expected keys. rangeStart is logically greater than
    // uuu so >= here will select all keys we expect to see in the result
    std::unordered_set<StoredDocKey> expectedKeys;
    for (const auto& k : generateTestKeys()) {
        if (k.getCollectionID() == scanCollection &&
            k.to_string() >= rangeStart.to_string()) {
            expectedKeys.emplace(k);
        }
    }
    expectedKeys.emplace(rangeStart);

    testRangeScan(expectedKeys, scanCollection, {key}, {"\xFF"});
}

// scan for less than key
void RangeScanTest::testLessThan(std::string key) {
    // In this case the client requests an exclusive end and we manipulate the
    // key by changing the suffix character, pop or subtract based on value
    if (key.back() == char(0)) {
        key.pop_back();
    } else {
        key.back()--;
    }

    auto rangeEnd = makeStoredDocKey(key, scanCollection);

    // Let's also store rangeEnd as if a client had written such a key (it's)
    // possible.
    store_item(vbid, rangeEnd, rangeEnd.to_string());
    flushVBucket(vbid);

    // So now generate the expected keys. rangeEnd is logically less than
    // the input key  so <=here will select all keys we expect to see in the
    // result
    std::unordered_set<StoredDocKey> expectedKeys;
    for (const auto& k : generateTestKeys()) {
        if (k.getCollectionID() == scanCollection &&
            k.to_string() <= rangeEnd.to_string()) {
            expectedKeys.emplace(k);
        }
    }
    expectedKeys.emplace(rangeEnd);

    // note: start is ensuring the key is byte 0 with a length of 1
    testRangeScan(expectedKeys, scanCollection, {"\0", 1}, {key});
}

TEST_P(RangeScanTest, less_than) {
    testLessThan("uuu");
}

TEST_P(RangeScanTest, less_than_with_zero_suffix) {
    std::string key = "uuu";
    key += char(0);
    testLessThan(key);
}

// Test that we reject continue whilst a scan is already being continued
TEST_P(RangeScanTest, continue_must_be_serialised) {
    auto uuid = createScan(scanCollection, {"a"}, {"b"});
    auto vb = store->getVBucket(vbid);

    EXPECT_EQ(cb::engine_errc::would_block,
              vb->continueRangeScan(uuid, *cookie));
    auto& epVb = dynamic_cast<EPVBucket&>(*vb);
    EXPECT_TRUE(epVb.getRangeScan(uuid)->isContinuing());

    // Cannot continue again
    EXPECT_EQ(cb::engine_errc::too_busy, vb->continueRangeScan(uuid, *cookie));

    // But can cancel
    EXPECT_EQ(cb::engine_errc::success, vb->cancelRangeScan(uuid, true));
}

// Create and then straight to cancel
TEST_P(RangeScanTest, create_cancel) {
    auto uuid = createScan(scanCollection, {"user"}, {"user\xFF"});
    auto vb = store->getVBucket(vbid);
    EXPECT_EQ(cb::engine_errc::success, vb->cancelRangeScan(uuid, true));
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "RangeScanContinueTask");

    // Nothing read
    EXPECT_TRUE(handler->scannedKeys.empty());
    EXPECT_TRUE(handler->scannedItems.empty());
}

TEST_P(RangeScanTest, create_cancel_no_data) {
    // this scan will generate no callbacks internally, but must still safely
    // cancel
    auto uuid = createScan(scanCollection, {"0"}, {"0\xFF"});
    auto vb = store->getVBucket(vbid);
    EXPECT_EQ(cb::engine_errc::success, vb->cancelRangeScan(uuid, true));
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "RangeScanContinueTask");

    // Nothing read
    EXPECT_TRUE(handler->scannedKeys.empty());
    EXPECT_TRUE(handler->scannedItems.empty());
}

// Check that if a scan has been continue (but is waiting to run), it can be
// cancelled. When the task runs the scan cancels.
TEST_P(RangeScanTest, create_continue_is_cancelled) {
    auto uuid = createScan(scanCollection, {"user"}, {"user\xFF"});
    auto vb = store->getVBucket(vbid);

    EXPECT_EQ(cb::engine_errc::would_block,
              vb->continueRangeScan(uuid, *cookie));

    // Cancel
    EXPECT_EQ(cb::engine_errc::success, vb->cancelRangeScan(uuid, true));

    // Note that at the moment continue and cancel are creating new tasks.
    // run them both and future changes will clean this up.
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "RangeScanContinueTask");
    // First task cancels, nothing was read
    EXPECT_TRUE(handler->scannedKeys.empty());
    EXPECT_TRUE(handler->scannedItems.empty());
    // and handler was notified of the cancel status
    EXPECT_EQ(cb::engine_errc::range_scan_cancelled, handler->status);

    // set to some status we don't use
    handler->status = cb::engine_errc::sync_write_pending;

    // second task runs, but won't do anything
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "RangeScanContinueTask");

    // no change
    EXPECT_EQ(cb::engine_errc::sync_write_pending, handler->status);
}

// Test that a scan doesn't keep on reading if a cancel occurs during the I/O
// task run
TEST_P(RangeScanTest, create_continue_is_cancelled_2) {
    auto uuid = createScan(scanCollection, {"user"}, {"user\xFF"});
    auto vb = store->getVBucket(vbid);

    EXPECT_EQ(cb::engine_errc::would_block,
              vb->continueRangeScan(uuid, *cookie));

    // Set a hook which will cancel when the 2nd key is read
    handler->testHook = [&vb, uuid](size_t count) {
        EXPECT_LT(count, 3); // never reach third key
        if (count == 2) {
            EXPECT_EQ(cb::engine_errc::success,
                      vb->cancelRangeScan(uuid, true));
        }
    };

    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "RangeScanContinueTask");

    EXPECT_EQ(cb::engine_errc::range_scan_cancelled, handler->status);

    // Check scan is gone, cannot be cancelled again
    EXPECT_EQ(cb::engine_errc::no_such_key, vb->cancelRangeScan(uuid, true));

    // Or continued, uuid is unknown
    EXPECT_EQ(cb::engine_errc::no_such_key,
              vb->continueRangeScan(uuid, *cookie));

    // Scan only read 2 of the possible keys
    if (isKeyOnly()) {
        EXPECT_EQ(2, handler->scannedKeys.size());
    } else {
        EXPECT_EQ(2, handler->scannedItems.size());
    }
    // And set our status to cancelled
    EXPECT_EQ(cb::engine_errc::range_scan_cancelled, handler->status);
}

bool TestRangeScanHandler::validateStatus(cb::engine_errc code) {
    switch (code) {
    case cb::engine_errc::success:
    case cb::engine_errc::not_my_vbucket:
    case cb::engine_errc::unknown_collection:
    case cb::engine_errc::range_scan_cancelled:
    case cb::engine_errc::range_scan_more:
    case cb::engine_errc::failed:
        return true;
    case cb::engine_errc::no_such_key:
    case cb::engine_errc::key_already_exists:
    case cb::engine_errc::no_memory:
    case cb::engine_errc::not_stored:
    case cb::engine_errc::invalid_arguments:
    case cb::engine_errc::not_supported:
    case cb::engine_errc::would_block:
    case cb::engine_errc::too_big:
    case cb::engine_errc::too_many_connections:
    case cb::engine_errc::disconnect:
    case cb::engine_errc::no_access:
    case cb::engine_errc::temporary_failure:
    case cb::engine_errc::out_of_range:
    case cb::engine_errc::rollback:
    case cb::engine_errc::no_bucket:
    case cb::engine_errc::too_busy:
    case cb::engine_errc::authentication_stale:
    case cb::engine_errc::delta_badval:
    case cb::engine_errc::locked:
    case cb::engine_errc::locked_tmpfail:
    case cb::engine_errc::predicate_failed:
    case cb::engine_errc::cannot_apply_collections_manifest:
    case cb::engine_errc::unknown_scope:
    case cb::engine_errc::durability_impossible:
    case cb::engine_errc::sync_write_in_progress:
    case cb::engine_errc::sync_write_ambiguous:
    case cb::engine_errc::dcp_streamid_invalid:
    case cb::engine_errc::durability_invalid_level:
    case cb::engine_errc::sync_write_re_commit_in_progress:
    case cb::engine_errc::sync_write_pending:
    case cb::engine_errc::stream_not_found:
    case cb::engine_errc::opaque_no_match:
    case cb::engine_errc::scope_size_limit_exceeded:
        return false;
    };
    throw std::invalid_argument(
            "TestRangeScanHandler::validateStatus: code does not represent a "
            "legal error code: " +
            std::to_string(int(code)));
}

auto scanConfigValues = ::testing::Combine(
        // Run for couchstore only until MB-49816 is resolved
        ::testing::Values("persistent_couchdb"),
        ::testing::Values("value_only", "full_eviction"),
        ::testing::Values("key_scan", "value_scan"));

INSTANTIATE_TEST_SUITE_P(RangeScanFullAndValueEviction,
                         RangeScanTest,
                         scanConfigValues,
                         RangeScanTest::PrintToStringParamName);
