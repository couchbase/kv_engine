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

#include <utilities/test_manifest.h>

#include <unordered_set>
#include <vector>

// A handler implementation that just stores the scan key/items in vectors
class TestRangeScanHandler : public RangeScanDataHandlerIFace {
public:
    void handleKey(DocKey key) override {
        scannedKeys.emplace_back(key);
    }

    void handleItem(std::unique_ptr<Item> item) override {
        scannedItems.emplace_back(std::move(item));
    }

    void validateKeyScan(const std::unordered_set<StoredDocKey>& expectedKeys);
    void validateItemScan(const std::unordered_set<StoredDocKey>& expectedKeys);

    std::vector<std::unique_ptr<Item>> scannedItems;
    std::vector<StoredDocKey> scannedKeys;
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

    std::pair<cb::rangescan::Id, std::shared_ptr<RangeScan>> createScan(
            CollectionID cid,
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
            // Store key with StoredDocKey::to_string as the value
            store_item(vbid, key, key.to_string());
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

std::pair<cb::rangescan::Id, std::shared_ptr<RangeScan>>
RangeScanTest::createScan(CollectionID cid,
                          cb::rangescan::KeyView start,
                          cb::rangescan::KeyView end) {
    // Create a new RangeScan object, this would be done on an I/O task as it
    // opens the file and generates a UUID
    auto scan = std::make_shared<RangeScan>(dynamic_cast<EPBucket&>(*store),
                                            *store->getVBucket(vbid),
                                            cid,
                                            start,
                                            end,
                                            *handler,
                                            cookie,
                                            getScanType());

    // Next frontend will add the uuid/scan, client can be informed of the uuid
    auto& epVb = dynamic_cast<EPVBucket&>(*store->getVBucket(vbid));
    EXPECT_EQ(cb::engine_errc::success, epVb.addNewRangeScan(scan));

    return {scan->getUuid(), scan};
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
    auto [uuid, scan] = createScan(cid, start, end);

    auto vb = store->getVBucket(vbid);

    // 2) Continue a RangeScan
    // 2.1) Frontend thread would call this method using clients uuid
    EXPECT_EQ(cb::engine_errc::success, vb->continueRangeScan(uuid));
    EXPECT_TRUE(scan->isContinuing());

    // 2.2) An I/O task now calls continueScan which will read data from disk
    EXPECT_EQ(cb::engine_errc::success,
              scan->continueScan(*store->getRWUnderlying(vbid)));

    // 2.3) All expected keys must have been read from disk (no limits yet)
    if (isKeyOnly()) {
        handler->validateKeyScan(expectedKeys);
    } else {
        handler->validateItemScan(expectedKeys);
    }

    // 3) Cancel a RangeScan
    // In this case the scan did technically finish, but no code yet exists to
    // tidy up, i.e. a completed scan will remove itself from the vbucket.
    // For now run cancel explicitly
    EXPECT_EQ(cb::engine_errc::success, vb->cancelRangeScan(uuid));

    // If the task were running, an I/O task would check for this state and stop
    // scanning
    EXPECT_TRUE(scan->isCancelled());

    // Check scan is gone, cannot be cancelled again
    EXPECT_EQ(cb::engine_errc::no_such_key, vb->cancelRangeScan(uuid));

    // Or continued, uuid is unknown
    EXPECT_EQ(cb::engine_errc::no_such_key, vb->continueRangeScan(uuid));

    // clean up (force kvstore close here)
    scan.reset();
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
    auto [uuid, scan] = createScan(scanCollection, {"a"}, {"b"});
    auto vb = store->getVBucket(vbid);

    EXPECT_EQ(cb::engine_errc::success, vb->continueRangeScan(uuid));
    EXPECT_TRUE(scan->isContinuing());

    // Cannot continue again
    EXPECT_EQ(cb::engine_errc::too_busy, vb->continueRangeScan(uuid));

    // But can cancel
    EXPECT_EQ(cb::engine_errc::success, vb->cancelRangeScan(uuid));
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
