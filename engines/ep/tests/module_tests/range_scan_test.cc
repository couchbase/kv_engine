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

#include "collections/collection_persisted_stats.h"
#include "ep_bucket.h"
#include "ep_vb.h"
#include "failover-table.h"
#include "kvstore/kvstore.h"
#include "mcbp/protocol/datatype.h"
#include "memcached/engine_error.h"
#include "memcached/range_scan.h"
#include "range_scans/range_scan.h"
#include "range_scans/range_scan_callbacks.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"
#include "tests/module_tests/test_helpers.h"
#include "vbucket.h"

#include <boost/uuid/name_generator.hpp>
#include <folly/portability/GTest.h>
#include <memcached/range_scan_optional_configuration.h>
#include <programs/engine_testapp/mock_cookie.h>
#include <programs/engine_testapp/mock_server.h>
#include <utilities/test_manifest.h>

#include <chrono>
#include <unordered_set>
#include <vector>

using namespace std::chrono_literals;

// A handler implementation that just stores the scan key/items in vectors
class TestRangeScanHandler : public RangeScanDataHandlerIFace {
public:
    // As the handler gets moved into the RangeScan, keep references to the
    // containers/status needed for validation
    TestRangeScanHandler(std::vector<std::unique_ptr<Item>>& items,
                         std::vector<StoredDocKey>& keys,
                         std::function<Status(size_t)>& hook)
        : scannedItems(items),
          scannedKeys(keys),
          testHook(hook) {
    }

    Status handleKey(DocKeyView key) override {
        checkKeyIsUnique(key);
        scannedKeys.emplace_back(key);
        return testHook(scannedKeys.size());
    }

    Status handleItem(std::unique_ptr<Item> item) override {
        checkKeyIsUnique(item->getKey());
        scannedItems.emplace_back(std::move(item));
        return testHook(scannedItems.size());
    }

    std::unique_ptr<RangeScanContinueResult> continuePartialOnFrontendThread()
            override {
        // no-op
        return {};
    }

    std::unique_ptr<RangeScanContinueResult> continueMoreOnFrontendThread()
            override {
        // no-op
        return {};
    }

    std::unique_ptr<RangeScanContinueResult> completeOnFrontendThread()
            override {
        // no-op
        return {};
    }

    std::unique_ptr<RangeScanContinueResult> cancelOnFrontendThread() override {
        // no-op
        return {};
    }

    void addStats(std::string_view prefix,
                  const StatCollector& collector) override {
        // none
    }

    void checkKeyIsUnique(DocKeyView key) {
        auto [itr, emplaced] = allKeys.emplace(key);
        EXPECT_TRUE(emplaced) << "Duplicate key returned " << key.to_string();
    }

    // return true if this code is an expected code from a continue
    static bool validateContinueStatus(cb::engine_errc code);

    std::vector<std::unique_ptr<Item>>& scannedItems;
    std::vector<StoredDocKey>& scannedKeys;
    std::unordered_set<StoredDocKey> allKeys;
    std::function<Status(size_t)>& testHook;
};

class RangeScanTest
    : public SingleThreadedEPBucketTest,
      public ::testing::WithParamInterface<
              std::tuple<std::string, std::string, std::string>> {
public:
    void SetUp() override {
        setupConfig();
        SingleThreadedEPBucketTest::SetUp();

        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

        // Setup collections and store keys
        cm.add(CollectionEntry::vegetable);
        cm.add(CollectionEntry::fruit);
        cm.add(CollectionEntry::dairy);
        setCollections(cookie, cm);
        flush_vbucket_to_disk(vbid, 3);
        storeTestKeys();
    }

    void TearDown() override {
        MockCookie::setCheckPrivilegeFunction({});
        mock_set_privilege_context_revision(0);
        RangeScan::resetClockFunction();
        SingleThreadedEPBucketTest::TearDown();
    }

    bool isTimeoutTaskScheduled() const {
        return task_executor->isTaskScheduled(
                TaskType::NonIO,
                fmt::format("RangeScanTimeoutTask for {}", vbid));
    }

    bool isTimeoutTaskSnoozed() const {
        return task_executor->isTask(
                TaskType::NonIO,
                fmt::format("RangeScanTimeoutTask for {}", vbid),
                [](GlobalTask& t) { return t.getState() == TASK_SNOOZED; });
    }

    bool isTimeoutTaskDead() const {
        return task_executor->isTask(
                TaskType::NonIO,
                fmt::format("RangeScanTimeoutTask for {}", vbid),
                [](GlobalTask& t) { return t.getState() == TASK_DEAD; });
    }

    bool isTimeoutTaskWakeupTime(
            std::chrono::steady_clock::time_point time) const {
        return task_executor->isTask(
                TaskType::NonIO,
                fmt::format("RangeScanTimeoutTask for {}", vbid),
                [time](GlobalTask& t) { return t.getWaketime() == time; });
    }

    static std::string PrintToStringParamName(
            const ::testing::TestParamInfo<ParamType>& info) {
        return std::get<0>(info.param) + "_" + std::get<1>(info.param) + "_" +
               std::get<2>(info.param);
    }

    void setupConfig() {
        config_string += generateBackendConfig(std::get<0>(GetParam()));
        config_string += ";item_eviction_policy=" + getEvictionMode();
#ifdef EP_USE_MAGMA
        config_string += ";" + magmaRollbackConfig;
#endif
    }

    std::string getEvictionMode() const {
        return std::get<1>(GetParam());
    }

    bool isKeyOnly() const {
        return std::get<2>(GetParam()) == "key_scan";
    }

    bool isIncludeXattr() const {
        return std::get<2>(GetParam()) == "value_scan_include_xattrs";
    }

    cb::rangescan::KeyOnly getScanType() const {
        return std::get<2>(GetParam()) == "key_scan"
                       ? cb::rangescan::KeyOnly::Yes
                       : cb::rangescan::KeyOnly::No;
    }

    cb::rangescan::IncludeXattrs getIncludeXattrs() const {
        return std::get<2>(GetParam()) == "value_scan_include_xattrs"
                       ? cb::rangescan::IncludeXattrs::Yes
                       : cb::rangescan::IncludeXattrs::No;
    }

    cb::rangescan::Id createScan(
            CollectionID cid,
            cb::rangescan::KeyView start,
            cb::rangescan::KeyView end,
            std::optional<cb::rangescan::SnapshotRequirements> snapshotReqs =
                    std::nullopt,
            std::optional<cb::rangescan::SamplingConfiguration> samplingConfig =
                    std::nullopt,
            cb::engine_errc expectedStatus = cb::engine_errc::success,
            std::unique_ptr<RangeScanDataHandlerIFace> optionalHandler =
                    nullptr);

    /**
     * Helper function to continue a scan, handles the EWB pattern
     */
    void continueRangeScan(cb::rangescan::Id id,
                           size_t itemLimit,
                           std::chrono::milliseconds timeLimit,
                           size_t byteLimit,
                           cb::engine_errc status);

    [[nodiscard]] std::vector<std::string> getUserStrings() const {
        return {"user-alan",
                "useralan",
                "user.claire",
                "user::\0",
                "user::zoe",
                // 2-byte code unit sequence in UTF-8; U+0416 = Cyrillic Capital
                // Letter Zhe Interesting case because first byte is outside
                // ASCII range.
                "user::\xD0\x96",
                "user::\xFF",
                "users"};
    }

    /**
     * @return a set of collection aware user-prefixed keys, all in the
     *         collection that the tests will scan
     */
    [[nodiscard]] std::unordered_set<StoredDocKey> getUserKeys() const {
        // Create a number of user prefixed collections and place them in the
        // collection that we will scan.
        std::unordered_set<StoredDocKey> keys;
        for (const auto& k : getUserStrings()) {
            keys.emplace(makeStoredDocKey(k, scanCollection));
        }
        return keys;
    }

    /**
     * generate a vector containing all of the keys which will be stored before
     * the test runs. Tests can then scan for these using various start/end
     * patterns
     */
    [[nodiscard]] std::vector<StoredDocKey> generateTestKeys() {
        std::vector<StoredDocKey> keys;

        for (const auto& k : getUserKeys()) {
            keys.push_back(k);
        }

        // Create the same keys in different collections
        for (const auto& k : getUserStrings()) {
            keys.push_back(makeStoredDocKey(k, collection2));
            keys.push_back(makeStoredDocKey(k, collection3));
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
            // store one key with xattrs to test when includeXattrs=Yes
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
                       cb::rangescan::KeyView end,
                       size_t itemLimit = 0,
                       std::chrono::milliseconds timeLimit = 0ms,
                       size_t byteLimit = 0,
                       size_t extraContinues = 0);

    void testLessThan(std::string key);

    void validateKeyScan(const std::unordered_set<StoredDocKey>& expectedKeys);
    void validateItemScan(const std::unordered_set<StoredDocKey>& expectedKeys);
    std::vector<std::pair<cb::rangescan::Id, MockCookie*>>
    setupConcurrencyMaxxed();

    void setNoAccess(CollectionID noaccess) {
        MockCookie::setCheckPrivilegeFunction(
                [noaccess](const CookieIface&,
                           cb::rbac::Privilege priv,
                           std::optional<ScopeID> sid,
                           std::optional<CollectionID> cid)
                        -> cb::rbac::PrivilegeAccess {
                    if (cid && *cid == noaccess) {
                        return cb::rbac::PrivilegeAccessFail;
                    }
                    return cb::rbac::PrivilegeAccessOk;
                });
    }

    // Tests all scan against the following collection
    const CollectionID scanCollection = CollectionEntry::vegetable.getId();
    // Tests also have data in these collections, and these deliberately enclose
    // the vegetable collection
    const CollectionID collection2 = CollectionEntry::fruit.getId();
    const CollectionID collection3 = CollectionEntry::dairy.getId();

    std::vector<std::unique_ptr<Item>> scannedItems;
    std::vector<StoredDocKey> scannedKeys;

    // callback counter used by DummyRangeScanHandler
    size_t dummyCallbackCounter{0};

    std::function<TestRangeScanHandler::Status(size_t)> testHook = [](size_t) {
        return TestRangeScanHandler::Status::OK;
    };
    std::unique_ptr<TestRangeScanHandler> handler{
            std::make_unique<TestRangeScanHandler>(
                    scannedItems, scannedKeys, testHook)};
    CollectionsManifest cm;
};

void RangeScanTest::validateKeyScan(
        const std::unordered_set<StoredDocKey>& expectedKeys) {
    EXPECT_TRUE(scannedItems.empty());
    EXPECT_EQ(expectedKeys.size(), scannedKeys.size());
    for (const auto& key : scannedKeys) {
        // Expect to find the key
        EXPECT_EQ(1, expectedKeys.count(key));
    }
}

void RangeScanTest::validateItemScan(
        const std::unordered_set<StoredDocKey>& expectedKeys) {
    EXPECT_TRUE(scannedKeys.empty());
    EXPECT_EQ(expectedKeys.size(), scannedItems.size());
    for (const auto& scanItem : scannedItems) {
        auto itr = expectedKeys.find(scanItem->getKey());
        // Expect to find the key
        EXPECT_NE(itr, expectedKeys.end());

        if (isIncludeXattr() &&
            (scanItem->getDataType() & PROTOCOL_BINARY_DATATYPE_XATTR)) {
            // Scan should include xattrs
            EXPECT_EQ(createXattrValue(itr->to_string()),
                      scanItem->getValueView());
        } else {
            // The value of StoredDocKey::to_string should equal the value
            EXPECT_EQ(itr->to_string(), scanItem->getValueView());
        }
    }
}

cb::rangescan::Id RangeScanTest::createScan(
        CollectionID cid,
        cb::rangescan::KeyView start,
        cb::rangescan::KeyView end,
        std::optional<cb::rangescan::SnapshotRequirements> snapshotReqs,
        std::optional<cb::rangescan::SamplingConfiguration> samplingConfig,
        cb::engine_errc expectedStatus,
        std::unique_ptr<RangeScanDataHandlerIFace> optionalHandler) {
    // Create a new RangeScan object and give it a handler we can inspect.
    EXPECT_EQ(cb::engine_errc::would_block,
              store->createRangeScan(
                           *cookie,
                           optionalHandler ? std::move(optionalHandler)
                                           : std::move(handler),
                           cb::rangescan::CreateParameters{vbid,
                                                           cid,
                                                           start,
                                                           end,
                                                           getScanType(),
                                                           getIncludeXattrs(),
                                                           snapshotReqs,
                                                           samplingConfig})
                      .first);
    // Now run via auxio task
    runNextTask(*task_executor->getLpTaskQ(TaskType::AuxIO),
                "RangeScanCreateTask");

    EXPECT_EQ(expectedStatus, mock_waitfor_cookie(cookie));

    if (expectedStatus != cb::engine_errc::success) {
        return {};
    }

    // Next frontend will add the uuid/scan, client can be informed of the uuid
    auto status = store->createRangeScan(
            *cookie,
            nullptr,
            cb::rangescan::CreateParameters{vbid,
                                            cid,
                                            start,
                                            end,
                                            getScanType(),
                                            getIncludeXattrs(),
                                            snapshotReqs,
                                            samplingConfig});
    EXPECT_EQ(cb::engine_errc::success, status.first);

    auto vb = store->getVBucket(vbid);
    EXPECT_TRUE(vb);
    auto& epVb = dynamic_cast<EPVBucket&>(*vb);
    auto scan = epVb.getRangeScan(status.second);
    EXPECT_TRUE(scan);
    return scan->getUuid();
}

void RangeScanTest::continueRangeScan(cb::rangescan::Id id,
                                      size_t itemLimit,
                                      std::chrono::milliseconds timeLimit,
                                      size_t byteLimit,
                                      cb::engine_errc status) {
    auto vb = store->getVBucket(vbid);
    EXPECT_TRUE(vb);
    cb::rangescan::ContinueParameters params{
            vbid, id, itemLimit, timeLimit, byteLimit, status};
    EXPECT_EQ(cb::engine_errc::would_block,
              vb->continueRangeScan(*cookie, params))
            << "Failure for scheduling";

    runNextTask(*task_executor->getLpTaskQ(TaskType::AuxIO),
                "RangeScanContinueTask");

    auto ioCompleteStatus = mock_waitfor_cookie(cookie);
    EXPECT_EQ(status, ioCompleteStatus);
    EXPECT_EQ(status, vb->continueRangeScan(*cookie, params))
            << "Failure for IO complete";

    // Only a subset of engine_errc are expected
    EXPECT_TRUE(TestRangeScanHandler::validateContinueStatus(ioCompleteStatus))
            << ioCompleteStatus;
}

// This method drives a range scan through create/continue/cancel for the given
// range. The test drives a range scan serially and the comments indicate where
// a frontend thread would be executing and where a background I/O task would.
void RangeScanTest::testRangeScan(
        const std::unordered_set<StoredDocKey>& expectedKeys,
        CollectionID cid,
        cb::rangescan::KeyView start,
        cb::rangescan::KeyView end,
        size_t itemLimit,
        std::chrono::milliseconds timeLimit,
        size_t byteLimit,
        size_t extraContinues) {
    // Not smart enough to test both limits yet
    EXPECT_TRUE(!(itemLimit && timeLimit.count()));

    // 1) create a RangeScan to scan the user prefixed keys.
    auto uuid = createScan(cid, start, end);

    // 2) Continue a RangeScan
    auto ioCompleteStatus = extraContinues
                                    ? cb::engine_errc::range_scan_more
                                    : cb::engine_errc::range_scan_complete;
    continueRangeScan(uuid, itemLimit, timeLimit, byteLimit, ioCompleteStatus);

    auto vb = store->getVBucket(vbid);
    auto& epVb = dynamic_cast<EPVBucket&>(*vb);

    // Tests will need more continues if a limit is in-play
    for (size_t count = 0; count < extraContinues; count++) {
        // Last continue should reach complete
        auto ioCompleteStatus = (count < extraContinues - 1)
                                        ? cb::engine_errc::range_scan_more
                                        : cb::engine_errc::range_scan_complete;
        EXPECT_EQ(1 + count, epVb.getRangeScan(uuid)->getContinueCount());
        continueRangeScan(
                uuid, itemLimit, timeLimit, byteLimit, ioCompleteStatus);
    }

    // 2.3) All expected keys must have been read from disk
    if (isKeyOnly()) {
        validateKeyScan(expectedKeys);
    } else {
        validateItemScan(expectedKeys);
    }

    // In this case the scan finished and cleaned up
    // Check scan is gone, cannot be cancelled again
    EXPECT_EQ(cb::engine_errc::no_such_key,
              store->cancelRangeScan(vbid, uuid, *cookie));

    // Or continued, uuid is unknown
    EXPECT_EQ(
            cb::engine_errc::no_such_key,
            store->continueRangeScan(
                    *cookie,
                    cb::rangescan::ContinueParameters{
                            vbid, uuid, 0, 0ms, 0, cb::engine_errc::success}));

    // Once the continue reached complete, the RangeScan object is pushed to
    // the IO task once more so that it can destruct on an IO task - this is
    // because the destruct closes the KVStore objects (maybe doing IO). Run
    // this task now so we don't get blocked in magma shutdown
    runNextTask(*task_executor->getLpTaskQ(TaskType::AuxIO),
                "RangeScanContinueTask");
}

class RangeScanCreateAndContinueTest : public RangeScanTest {
public:
    void TearDown() override {
        runNextTask(*task_executor->getLpTaskQ(TaskType::AuxIO),
                    "RangeScanContinueTask");
        RangeScanTest::TearDown();
    }
};

class RangeScanCreateTest : public RangeScanTest {
public:
    void TearDown() override {
        RangeScanTest::TearDown();
    }
};

// Scan for the user prefixed keys
TEST_P(RangeScanCreateAndContinueTest, user_prefix) {
    testRangeScan(getUserKeys(), scanCollection, {"user"}, {"user\xFF"});
}

TEST_P(RangeScanCreateAndContinueTest, exclusive_start) {
    auto expectedKeys = getUserKeys();
    expectedKeys.erase(makeStoredDocKey("user-alan", scanCollection));
    testRangeScan(expectedKeys,
                  scanCollection,
                  {"user-alan", cb::rangescan::KeyType::Exclusive},
                  {"user\xFF"});
}

TEST_P(RangeScanCreateAndContinueTest, exclusive_end) {
    auto expectedKeys = getUserKeys();
    expectedKeys.erase(makeStoredDocKey("users", scanCollection));
    testRangeScan(expectedKeys,
                  scanCollection,
                  {"user"},
                  {"users", cb::rangescan::KeyType::Exclusive});
}

TEST_P(RangeScanCreateAndContinueTest, exclusive_end_2) {
    // Check this zero suffixed key isn't included if it's set as the end
    // of an exclusive range
    store_item(vbid, makeStoredDocKey("users\0", scanCollection), "value");
    flushVBucket(vbid);

    auto expectedKeys = getUserKeys();
    expectedKeys.erase(makeStoredDocKey("users", scanCollection));
    testRangeScan(expectedKeys,
                  scanCollection,
                  {"user"},
                  {"users\0", cb::rangescan::KeyType::Exclusive});
}

TEST_P(RangeScanCreateAndContinueTest, exclusive_end_non_ascii) {
    auto expectedKeys = getUserKeys();
    expectedKeys.erase(makeStoredDocKey("users", scanCollection));
    expectedKeys.erase(makeStoredDocKey("useralan", scanCollection));
    testRangeScan(expectedKeys,
                  scanCollection,
                  {"user"},
                  {"user:;", cb::rangescan::KeyType::Exclusive});
}

TEST_P(RangeScanCreateAndContinueTest, exclusive_range) {
    auto expectedKeys = getUserKeys();
    expectedKeys.erase(makeStoredDocKey("users", scanCollection));
    expectedKeys.erase(makeStoredDocKey("user-alan", scanCollection));

    testRangeScan(expectedKeys,
                  scanCollection,
                  {"user-alan", cb::rangescan::KeyType::Exclusive},
                  {"users", cb::rangescan::KeyType::Exclusive});
}

TEST_P(RangeScanCreateAndContinueTest, user_prefix_with_item_limit_1) {
    auto expectedKeys = getUserKeys();
    testRangeScan(expectedKeys,
                  scanCollection,
                  {"user"},
                  {"user\xFF"},
                  1,
                  0ms,
                  0,
                  expectedKeys.size());
}

TEST_P(RangeScanCreateAndContinueTest, user_prefix_with_item_limit_2) {
    auto expectedKeys = getUserKeys();
    testRangeScan(expectedKeys,
                  scanCollection,
                  {"user"},
                  {"user\xFF"},
                  2,
                  0ms,
                  0,
                  expectedKeys.size() / 2);
}

// Test has a smaller byte-limit which must take affect before the item count
TEST_P(RangeScanCreateAndContinueTest, user_prefix_with_two_limits) {
    auto expectedKeys = getUserKeys();
    testRangeScan(expectedKeys,
                  scanCollection,
                  {"user"},
                  {"user\xFF"},
                  2, // 2 items
                  0ms,
                  1, // 1 byte
                  expectedKeys.size());
}

TEST_P(RangeScanCreateAndContinueTest, user_prefix_with_time_limit) {
    // Replace time with a function that ticks per call, forcing the scan to
    // yield for every item
    RangeScan::setClockFunction([]() {
        static auto now = cb::time::steady_clock::now();
        now += std::chrono::milliseconds(100);
        return now;
    });
    auto expectedKeys = getUserKeys();
    testRangeScan(expectedKeys,
                  scanCollection,
                  {"user"},
                  {"user\xFF"},
                  0,
                  std::chrono::milliseconds(10),
                  0,
                  expectedKeys.size());
}

TEST_P(RangeScanCreateAndContinueTest, user_prefix_with_byte_limit) {
    auto expectedKeys = getUserKeys();
    testRangeScan(expectedKeys,
                  scanCollection,
                  {"user"},
                  {"user\xFF"},
                  0,
                  0ms,
                  1, // 1 byte forces 1 key or document per continue
                  expectedKeys.size());
}

// Test ensures callbacks cover disk read case
TEST_P(RangeScanCreateAndContinueTest, user_prefix_evicted) {
    for (const auto& key : generateTestKeys()) {
        evict_key(vbid, key);
    }
    auto expectedKeys = getUserKeys();
    testRangeScan(expectedKeys,
                  scanCollection,
                  {"user"},
                  {"user\xFF"},
                  2,
                  0ms,
                  0,
                  expectedKeys.size() / 2);
}

TEST_P(RangeScanCreateAndContinueTest, scan_is_throttled) {
    testHook = [](size_t) { return TestRangeScanHandler::Status::Throttle; };
    // Scan with no continue limits, but the scan will yield for every key
    // as the testHook returns true meaning "throttle"
    auto expectedKeys = getUserKeys();
    testRangeScan(expectedKeys,
                  scanCollection,
                  {"user"},
                  {"user\xFF"},
                  0,
                  0ms,
                  0,
                  expectedKeys.size());
}

// Run a >= user scan by setting the keys to user and the end (255)
TEST_P(RangeScanCreateAndContinueTest, greater_than_or_equal) {
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
TEST_P(RangeScanCreateAndContinueTest, less_than_or_equal) {
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
TEST_P(RangeScanCreateAndContinueTest, greater_than) {
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

    testRangeScan(
            expectedKeys, scanCollection, std::string_view{key}, {"\xFF"});
}

// Run a > "user" scan using the KeyType
TEST_P(RangeScanCreateAndContinueTest, greater_than_using_KeyType) {
    auto expectedKeys = getUserKeys();
    auto rangeStart = makeStoredDocKey("user", scanCollection);
    expectedKeys.erase(rangeStart);
    for (const auto& key : generateTestKeys()) {
        // to_string returns a debug "cid:key", but > will select the
        // correct keys for this text
        if (key.getCollectionID() == scanCollection &&
            key.to_string() > rangeStart.to_string()) {
            expectedKeys.emplace(key);
        }
    }

    testRangeScan(expectedKeys,
                  scanCollection,
                  {"user", cb::rangescan::KeyType::Exclusive},
                  {"\xFF"});
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
    std::string_view start{"\0", 1};
    testRangeScan(expectedKeys, scanCollection, start, std::string_view{key});
}

TEST_P(RangeScanCreateAndContinueTest, less_than) {
    testLessThan("uuu");
}

TEST_P(RangeScanCreateAndContinueTest, less_than_with_zero_suffix) {
    std::string key = "uuu";
    key += char(0);
    testLessThan(key);
}

// Run a < "users" scan using the KeyType
TEST_P(RangeScanCreateAndContinueTest, less_than_using_KeyType) {
    auto expectedKeys = getUserKeys();
    auto rangeEnd = makeStoredDocKey("users", scanCollection);
    expectedKeys.erase(rangeEnd);
    for (const auto& key : generateTestKeys()) {
        // to_string returns a debug "cid:key", but >= will select the
        // correct keys for this text
        if (key.getCollectionID() == scanCollection &&
            key.to_string() < rangeEnd.to_string()) {
            expectedKeys.emplace(key);
        }
    }

    testRangeScan(expectedKeys,
                  scanCollection,
                  {"\0"},
                  {"users", cb::rangescan::KeyType::Exclusive});
}

// Test that we reject continue whilst a scan is already being continued
TEST_P(RangeScanCreateAndContinueTest, continue_must_be_serialised) {
    auto uuid = createScan(scanCollection, {"a"}, {"b"});
    auto vb = store->getVBucket(vbid);

    auto continueParams = cb::rangescan::ContinueParameters{
            vbid, uuid, 0, 0ms, 0, cb::engine_errc::success};

    EXPECT_EQ(cb::engine_errc::would_block,
              vb->continueRangeScan(*cookie, continueParams));
    auto& epVb = dynamic_cast<EPVBucket&>(*vb);
    EXPECT_TRUE(epVb.getRangeScan(uuid)->isContinuing());

    // Cannot continue again
    auto* cookie2 = create_mock_cookie();
    EXPECT_EQ(cb::engine_errc::too_busy,
              vb->continueRangeScan(*cookie2, continueParams));
    destroy_mock_cookie(cookie2);

    // But can cancel
    EXPECT_EQ(cb::engine_errc::success, vb->cancelRangeScan(uuid, cookie));
}

// Create and then straight to cancel
TEST_P(RangeScanCreateAndContinueTest, create_cancel) {
    auto uuid = createScan(scanCollection, {"user"}, {"user\xFF"});
    auto vb = store->getVBucket(vbid);
    EXPECT_EQ(cb::engine_errc::success, vb->cancelRangeScan(uuid, cookie));
    runNextTask(*task_executor->getLpTaskQ(TaskType::AuxIO),
                "RangeScanContinueTask");

    // Nothing read
    EXPECT_TRUE(scannedKeys.empty());
    EXPECT_TRUE(scannedItems.empty());
}

TEST_P(RangeScanCreateTest, create_no_data) {
    // create will fail if no data is in range
    createScan(scanCollection,
               {"0"},
               {"0\xFF"},
               {},
               {/* no sampling config*/},
               cb::engine_errc::no_such_key);
}

// Check that if a scan has been continue (but is waiting to run), it can be
// cancelled. When the task runs the scan cancels.
TEST_P(RangeScanCreateAndContinueTest, create_continue_is_cancelled) {
    auto uuid = createScan(scanCollection, {"user"}, {"user\xFF"});
    auto vb = store->getVBucket(vbid);

    auto continueParams = cb::rangescan::ContinueParameters{
            vbid, uuid, 0, 0ms, 0, cb::engine_errc::success};
    EXPECT_EQ(cb::engine_errc::would_block,
              vb->continueRangeScan(*cookie, continueParams));

    // Cancel
    EXPECT_EQ(cb::engine_errc::success, vb->cancelRangeScan(uuid, cookie));

    // Task will run and cancel the scan, it will reschedule until nothing is
    // found in the ReadyScans queue (so 1 reschedule for this test).
    runNextTask(*task_executor->getLpTaskQ(TaskType::AuxIO),
                "RangeScanContinueTask");

    // Scan cancels. No keys/items read and the status isn't seen as cancelled
    EXPECT_TRUE(scannedKeys.empty());
    EXPECT_TRUE(scannedItems.empty());
    // and handler was notified of the cancel status
    EXPECT_EQ(cb::engine_errc::range_scan_cancelled,
              mock_waitfor_cookie(cookie));
}

// Test that a scan doesn't keep on reading if a cancel occurs during the I/O
// task run
TEST_P(RangeScanCreateAndContinueTest, create_continue_is_cancelled_2) {
    auto uuid = createScan(scanCollection, {"user"}, {"user\xFF"});
    auto vb = store->getVBucket(vbid);

    // Set a hook which will cancel when the 2nd key is read
    testHook = [&vb, uuid, this](size_t count) {
        EXPECT_LT(count, 3); // never reach third key
        if (count == 2) {
            // cancel with no cookie so there is no privilege check. This avoids
            // a double lock of the collection manifest which is locked by the
            // scan loop
            EXPECT_EQ(cb::engine_errc::success,
                      vb->cancelRangeScan(uuid, nullptr));
        }
        return TestRangeScanHandler::Status::OK;
    };

    continueRangeScan(uuid, 0, 0ms, 0, cb::engine_errc::range_scan_cancelled);

    // Check scan is gone, cannot be cancelled again
    EXPECT_EQ(cb::engine_errc::no_such_key, vb->cancelRangeScan(uuid, cookie));

    // Or continued, uuid is unknown
    EXPECT_EQ(
            cb::engine_errc::no_such_key,
            vb->continueRangeScan(
                    *cookie,
                    cb::rangescan::ContinueParameters{
                            vbid, uuid, 0, 0ms, 0, cb::engine_errc::success}));

    // Scan only read 2 of the possible keys
    if (isKeyOnly()) {
        EXPECT_EQ(2, scannedKeys.size());
    } else {
        EXPECT_EQ(2, scannedItems.size());
    }
}

TEST_P(RangeScanCreateTest, snapshot_does_not_contain_seqno_0) {
    auto vb = store->getVBucket(vbid);
    // require persisted upto 0 and something found at 0
    cb::rangescan::SnapshotRequirements reqs{
            vb->failovers->getLatestUUID(),
            0, /* persieted up to 0 */
            std::nullopt,
            true /* something must exist at seqno 0*/};
    createScan(scanCollection,
               {"user"},
               {"user\xFF"},
               reqs,
               {/* no sampling config*/},
               cb::engine_errc::not_stored);
}

TEST_P(RangeScanCreateTest, snapshot_does_not_contain_seqno) {
    auto vb = store->getVBucket(vbid);
    // Store, capture high-seqno and update so it's gone from the snapshot
    store_item(vbid, StoredDocKey("update_me", scanCollection), "1");
    cb::rangescan::SnapshotRequirements reqs{vb->failovers->getLatestUUID(),
                                             uint64_t(vb->getHighSeqno()),
                                             std::nullopt,
                                             true};
    store_item(vbid, StoredDocKey("update_me", scanCollection), "2");
    flushVBucket(vbid);
    createScan(scanCollection,
               {"user"},
               {"user\xFF"},
               reqs,
               {/* no sampling config*/},
               cb::engine_errc::not_stored);
}

TEST_P(RangeScanCreateAndContinueTest, snapshot_upto_seqno) {
    auto vb = store->getVBucket(vbid);
    cb::rangescan::SnapshotRequirements reqs{vb->failovers->getLatestUUID(),
                                             uint64_t(vb->getHighSeqno()),
                                             std::nullopt,
                                             false};
    auto uuid = createScan(scanCollection,
                           {"user"},
                           {"user\xFF"},
                           reqs,
                           {/* no sampling config*/},
                           cb::engine_errc::success);
    EXPECT_EQ(cb::engine_errc::success, vb->cancelRangeScan(uuid, cookie));
}

TEST_P(RangeScanCreateAndContinueTest, snapshot_contains_seqno) {
    auto vb = store->getVBucket(vbid);
    cb::rangescan::SnapshotRequirements reqs{vb->failovers->getLatestUUID(),
                                             uint64_t(vb->getHighSeqno()),
                                             std::nullopt,
                                             true};
    auto uuid = createScan(scanCollection,
                           {"user"},
                           {"user\xFF"},
                           reqs,
                           {/* no sampling config*/},
                           cb::engine_errc::success);
    EXPECT_EQ(cb::engine_errc::success, vb->cancelRangeScan(uuid, cookie));
}

// There is no wait option, so a future seqno is a failure
TEST_P(RangeScanCreateTest, future_seqno_fails) {
    auto vb = store->getVBucket(vbid);
    cb::rangescan::SnapshotRequirements reqs{vb->failovers->getLatestUUID(),
                                             uint64_t(vb->getHighSeqno() + 1),
                                             std::nullopt,
                                             false};
    // This error is detected on first invocation, no need for ewouldblock
    // this seqno check occurs in EPBucket, so don't test via VBucket
    EXPECT_EQ(cb::engine_errc::temporary_failure,
              store->createRangeScan(*cookie,
                                     std::move(handler),
                                     cb::rangescan::CreateParameters{
                                             vbid,
                                             scanCollection,
                                             {"user"},
                                             {"user\xFF"},
                                             getScanType(),
                                             getIncludeXattrs(),
                                             reqs,
                                             {/* no sampling config*/}})
                      .first);
}

TEST_P(RangeScanCreateTest, vb_uuid_check) {
    auto vb = store->getVBucket(vbid);
    cb::rangescan::SnapshotRequirements reqs{
            1, uint64_t(vb->getHighSeqno()), std::nullopt, false};
    // This error is detected on first invocation, no need for ewouldblock
    // uuid check occurs in EPBucket, so don't test via VBucket
    EXPECT_EQ(cb::engine_errc::vbuuid_not_equal,
              store->createRangeScan(*cookie,
                                     std::move(handler),
                                     cb::rangescan::CreateParameters{
                                             vbid,
                                             scanCollection,
                                             {"user"},
                                             {"user\xFF"},
                                             getScanType(),
                                             getIncludeXattrs(),
                                             reqs,
                                             {/* no sampling config*/}})
                      .first);
}

TEST_P(RangeScanCreateAndContinueTest, random_sample_less_keys_than_samples) {
    auto stats = getCollectionStats(vbid, {scanCollection});
    // Request more samples than keys. Everything gets returned
    auto sampleSize = stats[scanCollection].itemCount + 1;
    auto uuid = createScan(scanCollection,
                           {"\0", 1},
                           {"\xFF"},
                           {/* no snapshot requirements */},
                           cb::rangescan::SamplingConfiguration{sampleSize, 0});
    continueRangeScan(uuid, 0, 0ms, 0, cb::engine_errc::range_scan_complete);

    if (isKeyOnly()) {
        EXPECT_EQ(stats[scanCollection].itemCount, scannedKeys.size());
    } else {
        EXPECT_EQ(stats[scanCollection].itemCount, scannedItems.size());
    }
}

// MB-54543: Test covers updated sampling behaviour where we can return more
// samples than requested
TEST_P(RangeScanCreateAndContinueTest,
       random_sample_return_more_keys_than_samples) {
    auto stats = getCollectionStats(vbid, {scanCollection});
    // Request nearly the whole collection
    auto sampleSize = stats[scanCollection].itemCount - 1;

    auto uuid = createScan(scanCollection,
                           {"\0", 1},
                           {"\xFF"},
                           {/* no snapshot requirements */},
                           cb::rangescan::SamplingConfiguration{sampleSize, 0});
    continueRangeScan(uuid, 0, 0ms, 0, cb::engine_errc::range_scan_complete);

    // The scans both return all keys
    if (isKeyOnly()) {
        EXPECT_EQ(stats[scanCollection].itemCount, scannedKeys.size());
    } else {
        EXPECT_EQ(stats[scanCollection].itemCount, scannedItems.size());
    }
}

TEST_P(RangeScanCreateAndContinueTest, random_sample_keys_equal_samples) {
    auto stats = getCollectionStats(vbid, {scanCollection});
    // Request samples == keys. Everything gets returned
    auto sampleSize = stats[scanCollection].itemCount;
    auto uuid = createScan(scanCollection,
                           {"\0", 1},
                           {"\xFF"},
                           {/* no snapshot requirements */},
                           cb::rangescan::SamplingConfiguration{sampleSize, 0});
    continueRangeScan(uuid, 0, 0ms, 0, cb::engine_errc::range_scan_complete);
    if (isKeyOnly()) {
        EXPECT_EQ(stats[scanCollection].itemCount, scannedKeys.size());
    } else {
        EXPECT_EQ(stats[scanCollection].itemCount, scannedItems.size());
    }
}

TEST_P(RangeScanCreateAndContinueTest, random_sample) {
    auto stats = getCollectionStats(vbid, {scanCollection});
    // We'll sample up to 1/2 of the keys from the collection, we may not get
    // exactly 50% of the keys returned though.
    auto sampleSize = stats[scanCollection].itemCount / 2;

    // key ranges covers all keys in scanCollection, kv_engine will do this
    // not the client. Note the seed chosen here ensures a reasonable number
    // of keys are returned. Given that the unit tests only store a small number
    // of keys, it's possible to find a seed which returned nothing.
    auto uuid = createScan(scanCollection,
                           {"\0", 1},
                           {"\xFF"},
                           {/* no snapshot requirements */},
                           cb::rangescan::SamplingConfiguration{sampleSize, 1});

    // run the scan with a limit, this gives more coverage of checks for
    // sampleSize. This loop also runs the scan one extra time to ensure we
    // enter the continue code with the isTotalLimitReached() condition true
    for (size_t ii = 0; ii <= sampleSize; ii++) {
        auto status = ii == sampleSize ? cb::engine_errc::range_scan_complete
                                       : cb::engine_errc::range_scan_more;
        continueRangeScan(uuid, 1, 0ms, 0, status);
    }

    // the chosen seed, results in sampleSize keys
    if (isKeyOnly()) {
        EXPECT_EQ(sampleSize, scannedKeys.size());
    } else {
        EXPECT_EQ(sampleSize, scannedItems.size());
    }
}

TEST_P(RangeScanCreateAndContinueTest, random_sample_with_limit_1) {
    auto stats = getCollectionStats(vbid, {scanCollection});
    // We'll sample 1/2 of the keys from the collection
    auto sampleSize = (stats[scanCollection].itemCount / 2) + 1;

    // key ranges covers all keys in scanCollection, kv_engine will do this
    // not the client
    auto uuid = createScan(scanCollection,
                           {"\0", 1},
                           {"\xFF"},
                           {/* no snapshot requirements */},
                           cb::rangescan::SamplingConfiguration{sampleSize, 0});

    auto vb = store->getVBucket(vbid);

    // 1 key returned per continue (limit=1)
    // + 1 extra continue will bring it to 'self cancel' via complete
    for (size_t ii = 0; ii < sampleSize - 1; ii++) {
        continueRangeScan(uuid, 1, 0ms, 0, cb::engine_errc::range_scan_more);
    }

    continueRangeScan(uuid, 1, 0ms, 0, cb::engine_errc::range_scan_complete);

    // See comments RangeScanTest::random_sample regarding sampleSize adjustment
    if (isKeyOnly()) {
        EXPECT_EQ(sampleSize - 1, scannedKeys.size());
    } else {
        EXPECT_EQ(sampleSize - 1, scannedItems.size());
    }
}

TEST_P(RangeScanCreateTest, not_my_vbucket) {
    EXPECT_EQ(cb::engine_errc::not_my_vbucket,
              store->createRangeScan(
                           *cookie,
                           std::move(handler),
                           cb::rangescan::CreateParameters{Vbid(4),
                                                           scanCollection,
                                                           {"\0", 1},
                                                           {"\xFF"},
                                                           getScanType(),
                                                           getIncludeXattrs(),
                                                           {},
                                                           {}})
                      .first);
}

TEST_P(RangeScanCreateTest, unknown_collection) {
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              store->createRangeScan(*cookie,
                                     std::move(handler),
                                     cb::rangescan::CreateParameters{
                                             vbid,
                                             CollectionEntry::meat.getId(),
                                             {"\0", 1},
                                             {"\xFF"},
                                             getScanType(),
                                             getIncludeXattrs(),
                                             {},
                                             {}})
                      .first);
}

// Test that the collection going away after part 1 of create, cleans up
TEST_P(RangeScanCreateAndContinueTest, scan_cancels_after_create) {
    EXPECT_EQ(cb::engine_errc::would_block,
              store->createRangeScan(
                           *cookie,
                           std::move(handler),
                           cb::rangescan::CreateParameters{vbid,
                                                           scanCollection,
                                                           {"user"},
                                                           {"user\xFF"},
                                                           getScanType(),
                                                           getIncludeXattrs(),
                                                           {},
                                                           {}})
                      .first);
    // Now run via auxio task
    runNextTask(*task_executor->getLpTaskQ(TaskType::AuxIO),
                "RangeScanCreateTask");

    EXPECT_EQ(cb::engine_errc::success, mock_waitfor_cookie(cookie));

    // Drop scanCollection
    EXPECT_EQ(scanCollection, CollectionEntry::vegetable.getId());
    auto* cookie2 = create_mock_cookie();
    setCollections(cookie2, cm.remove(CollectionEntry::vegetable));

    // Second part of create runs and fails
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              store->createRangeScan(
                           *cookie,
                           nullptr,
                           cb::rangescan::CreateParameters{vbid,
                                                           scanCollection,
                                                           {"user"},
                                                           {"user\xFF"},
                                                           getScanType(),
                                                           getIncludeXattrs(),
                                                           {},
                                                           {}})
                      .first);
    destroy_mock_cookie(cookie2);
}

// Test that if the vbucket changes after create, but before we run the create
// task, it is detected when snapshot_requirements are in use
TEST_P(RangeScanCreateTest, scan_detects_vbucket_change) {
    auto vb = store->getVBucket(vbid);
    cb::rangescan::SnapshotRequirements reqs{vb->failovers->getLatestUUID(),
                                             uint64_t(vb->getHighSeqno()),
                                             std::nullopt,
                                             false};
    vb.reset();

    EXPECT_EQ(cb::engine_errc::would_block,
              store->createRangeScan(
                           *cookie,
                           std::move(handler),
                           cb::rangescan::CreateParameters{vbid,
                                                           scanCollection,
                                                           {"user"},
                                                           {"user\xFF"},
                                                           getScanType(),
                                                           getIncludeXattrs(),
                                                           reqs,
                                                           {}})
                      .first);

    // destroy and create the vbucket - a new uuid will be created
    EXPECT_TRUE(store->resetVBucket(vbid));

    // Need to poke a few functions to get the create ready to run.
    runNextTask(*task_executor->getLpTaskQ(TaskType::AuxIO),
                "Removing (dead) vb:0 from memory and disk");

    // Force a state change to active, generating a new UUID. As the collection
    // state is not "epoch", this new vbucket will pick up 3 mutations, so we
    // use flushVBucket directly and not setVBucketStateAndRunPersistTask as
    // the latter expects 0 items flushed.
    setVBucketState(vbid, vbucket_state_active);
    flushVBucket(vbid); // ensures vb on disk has the new uuid

    runNextTask(*task_executor->getLpTaskQ(TaskType::AuxIO),
                "RangeScanCreateTask");

    // task detected the vbucket has changed and aborted
    EXPECT_EQ(cb::engine_errc::vbuuid_not_equal, mock_waitfor_cookie(cookie));
}

TEST_P(RangeScanCreateTest, create_on_replica) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    // create only allowed on active
    EXPECT_EQ(cb::engine_errc::not_my_vbucket,
              store->createRangeScan(
                           *cookie,
                           std::move(handler),
                           cb::rangescan::CreateParameters{vbid,
                                                           scanCollection,
                                                           {"user"},
                                                           {"user\xFF"},
                                                           getScanType(),
                                                           getIncludeXattrs(),
                                                           {},
                                                           {}})
                      .first);
}

// Test that if the vbucket changes during the continue phase, the scan
// stops. In theory we could still keep scanning as we have the correct
// snapshot open, but some event has occurred that the scan client should be
// aware of, the scan would also need to ignore any in-memory values, simpler
// to just end the scan and report the vbucket change to the client.
// Note for this test, no snapshot requirements are needed.
TEST_P(RangeScanCreateAndContinueTest,
       scan_detects_vbucket_change_during_continue) {
    // Create the scan
    auto uuid = createScan(scanCollection,
                           {"user"},
                           {"user\xFF"},
                           {/* no snapshot requirements */},
                           {/* no sampling*/});

    // Continue
    auto continueParams = cb::rangescan::ContinueParameters{
            vbid, uuid, 0, 0ms, 0, cb::engine_errc::success};
    EXPECT_EQ(cb::engine_errc::would_block,
              store->continueRangeScan(*cookie, continueParams));

    // destroy and create the vbucket - a new uuid will be created
    EXPECT_TRUE(store->resetVBucket(vbid));

    // Need to poke a few functions to get the create ready to run.
    runNextTask(*task_executor->getLpTaskQ(TaskType::AuxIO),
                "Removing (dead) vb:0 from memory and disk");

    // Force a state change to active, generating a new Vbucket UUID.
    setVBucketState(vbid, vbucket_state_active);

    // No need to flush in this test as only in memory VB can be inspected for
    // the uuid change
    runNextTask(*task_executor->getLpTaskQ(TaskType::AuxIO),
                "RangeScanContinueTask");

    // The scan task detected that it was cancelled
    EXPECT_EQ(cb::engine_errc::range_scan_cancelled,
              mock_waitfor_cookie(cookie));
    EXPECT_EQ(cb::engine_errc::range_scan_cancelled,
              store->continueRangeScan(*cookie, continueParams));
    // And it's gone
    EXPECT_EQ(cb::engine_errc::no_such_key,
              store->continueRangeScan(*cookie, continueParams));
}

TEST_P(RangeScanCreateAndContinueTest, wait_for_persistence_success) {
    auto vb = store->getVBucket(vbid);

    // Create a scan that requires +1 from high-seqno. We are willing to wait
    cb::rangescan::SnapshotRequirements reqs{vb->failovers->getLatestUUID(),
                                             uint64_t(vb->getHighSeqno() + 1),
                                             std::chrono::milliseconds(100),
                                             false};

    EXPECT_EQ(cb::engine_errc::would_block,
              store->createRangeScan(
                           *cookie,
                           nullptr,
                           cb::rangescan::CreateParameters{vbid,
                                                           scanCollection,
                                                           {"user"},
                                                           {"user\xFF"},
                                                           getScanType(),
                                                           getIncludeXattrs(),
                                                           reqs,
                                                           {}})
                      .first);

    // store our item and flush (so the waitForPersistence is notified)
    store_item(vbid, StoredDocKey("waiting", scanCollection), "");
    EXPECT_EQ(1, vb->getHighPriorityChkSize());
    flushVBucket(vbid);
    EXPECT_EQ(cb::engine_errc::success, mock_waitfor_cookie(cookie));
    EXPECT_EQ(0, vb->getHighPriorityChkSize());

    // Now the task will move to create, we can drive the scan using our wrapper
    // it will do the next ewouldblock phase finally creating the scan
    auto uuid = createScan(scanCollection,
                           {"user"},
                           {"user\xFF"},
                           reqs,
                           {/* no sampling config*/},
                           cb::engine_errc::success);

    // Close the scan
    EXPECT_EQ(cb::engine_errc::success, vb->cancelRangeScan(uuid, cookie));
}

TEST_P(RangeScanCreateTest, wait_for_persistence_fails) {
    auto vb = store->getVBucket(vbid);

    // Create a scan that requires +1 from high-seqno. No timeout so fails on
    // the first crack of the whip
    cb::rangescan::SnapshotRequirements reqs{vb->failovers->getLatestUUID(),
                                             uint64_t(vb->getHighSeqno() + 1),
                                             std::nullopt,
                                             false};

    EXPECT_EQ(cb::engine_errc::temporary_failure,
              store->createRangeScan(
                           *cookie,
                           std::move(handler),
                           cb::rangescan::CreateParameters{vbid,
                                                           scanCollection,
                                                           {"user"},
                                                           {"user\xFF"},
                                                           getScanType(),
                                                           getIncludeXattrs(),
                                                           reqs,
                                                           {}})
                      .first);
}

TEST_P(RangeScanCreateTest, wait_for_persistence_timeout) {
    auto vb = store->getVBucket(vbid);

    // Create a scan that requires +1 from high-seqno. We are willing to wait
    // set the timeout to 0, so first flush will expire
    cb::rangescan::SnapshotRequirements reqs{vb->failovers->getLatestUUID(),
                                             uint64_t(vb->getHighSeqno() + 2),
                                             0ms,
                                             false};

    EXPECT_EQ(cb::engine_errc::would_block,
              store->createRangeScan(
                           *cookie,
                           std::move(handler),
                           cb::rangescan::CreateParameters{vbid,
                                                           scanCollection,
                                                           {"user"},
                                                           {"user\xFF"},
                                                           getScanType(),
                                                           getIncludeXattrs(),
                                                           reqs,
                                                           {}})
                      .first);

    // store an item and flush (so the waitForPersistence is notified and
    // expired)
    store_item(vbid, StoredDocKey("waiting", scanCollection), "");
    flushVBucket(vbid);
    EXPECT_EQ(cb::engine_errc::temporary_failure, mock_waitfor_cookie(cookie));
}

// Prior to this test, a cancel as the scan attempts to yield could hit an
// exception in the state change code, this test covers that path
TEST_P(RangeScanCreateAndContinueTest, cancel_when_yielding) {
    auto uuid = createScan(scanCollection, {"user"}, {"user\xFF"});
    auto vb = store->getVBucket(vbid);

    // Scan with a limit so we enter the yield path
    auto continueParams = cb::rangescan::ContinueParameters{
            vbid, uuid, 1, 0ms, 0, cb::engine_errc::success};
    EXPECT_EQ(cb::engine_errc::would_block,
              vb->continueRangeScan(*cookie, continueParams));

    // Set a hook which will cancel when the first key is read and the scan
    // would yield
    testHook = [&vb, uuid, this](size_t count) {
        // Cancel after the first key has been read
        EXPECT_EQ(cb::engine_errc::success, vb->cancelRangeScan(uuid, nullptr));
        return TestRangeScanHandler::Status::OK;
    };

    // scan!
    runNextTask(*task_executor->getLpTaskQ(TaskType::AuxIO),
                "RangeScanContinueTask");

    // The cancel is detected yet, so scan yields
    EXPECT_EQ(cb::engine_errc::range_scan_more, mock_waitfor_cookie(cookie));

    // Now the cancel is picked up by the IO complete run of the request
    continueParams.currentStatus = cb::engine_errc::range_scan_more;
    EXPECT_EQ(cb::engine_errc::range_scan_cancelled,
              vb->continueRangeScan(*cookie, continueParams));
}

class DummyRangeScanHandler : public RangeScanDataHandlerIFace {
public:
    DummyRangeScanHandler(size_t& callbackCounter)
        : callbackCounter(callbackCounter) {
    }

    Status handleKey(DocKeyView key) override {
        ++callbackCounter;
        return Status::OK;
    }

    Status handleItem(std::unique_ptr<Item>) override {
        ++callbackCounter;
        return Status::OK;
    }

    std::unique_ptr<RangeScanContinueResult> continuePartialOnFrontendThread()
            override {
        // no-op
        return {};
    }

    std::unique_ptr<RangeScanContinueResult> continueMoreOnFrontendThread()
            override {
        // no-op
        return {};
    }

    std::unique_ptr<RangeScanContinueResult> completeOnFrontendThread()
            override {
        // no-op
        return {};
    }

    std::unique_ptr<RangeScanContinueResult> cancelOnFrontendThread() override {
        // no-op
        return {};
    }

    void addStats(std::string_view prefix,
                  const StatCollector& collector) override {
        // none
    }

    size_t& callbackCounter;
};

std::vector<std::pair<cb::rangescan::Id, MockCookie*>>
RangeScanTest::setupConcurrencyMaxxed() {
    auto vb = store->getVBucket(vbid);

    std::vector<std::pair<cb::rangescan::Id, MockCookie*>> scans;
    // At time of writing, getNumAuxIO was 8
    EXPECT_GT(task_executor->getNumAuxIO(), 2);
    const size_t numScans = task_executor->getNumAuxIO() + 1;
    for (size_t ii = 0; ii < numScans; ii++) {
        // Create the scan and a cookie
        scans.emplace_back(createScan(scanCollection,
                                      {"user"},
                                      {"user\xFF"},
                                      {/* no snapshot requirements */},
                                      {/* no sampling*/},
                                      cb::engine_errc::success,
                                      std::make_unique<DummyRangeScanHandler>(
                                              dummyCallbackCounter)),
                           create_mock_cookie());
    }

    for (const auto& scan : scans) {
        auto continueParams = cb::rangescan::ContinueParameters{
                vbid, scan.first, 0, 0ms, 0, cb::engine_errc::success};
        EXPECT_EQ(cb::engine_errc::would_block,
                  store->continueRangeScan(*scan.second, continueParams));
    }
    // Check only AUXIO - 1 tasks were scheduled to handle the scans
    auto* q = task_executor->getLpTaskQ(TaskType::AuxIO);
    EXPECT_EQ(task_executor->getNumAuxIO() - 1, q->getFutureQueueSize());
    EXPECT_EQ(0, q->getReadyQueueSize());
    return scans;
}

// Create lots and lots of scans, continue them all and check only max tasks
// are scheduled
TEST_P(RangeScanCreateAndContinueTest, concurrency_maxxed) {
    auto scans = setupConcurrencyMaxxed();

    // Now clean-up
    for (const auto& scan : scans) {
        EXPECT_EQ(cb::engine_errc::success,
                  store->cancelRangeScan(vbid, scan.first, *cookie));
    }

    // Still -1 tasks, yet everything can run (tasks will reschedule to ensure
    // all scans get picked up)
    auto* q = task_executor->getLpTaskQ(TaskType::AuxIO);
    EXPECT_EQ(task_executor->getNumAuxIO() - 1, q->getFutureQueueSize());
    for (const auto& scan : scans) {
        runNextTask(*q, "RangeScanContinueTask");
        // every cookie returns cancelled
        EXPECT_EQ(cb::engine_errc::range_scan_cancelled,
                  mock_waitfor_cookie(scan.second));
        destroy_mock_cookie(scan.second);
    }
}

TEST_P(RangeScanCreateAndContinueTest, concurrency_maxxed_cancel_only) {
    auto vb = store->getVBucket(vbid);

    std::vector<cb::rangescan::Id> scans;
    // At time of writing, getNumAuxIO was 8
    ASSERT_GT(task_executor->getNumAuxIO(), 2);
    const size_t numScans = task_executor->getNumAuxIO() + 1;
    for (size_t ii = 0; ii < numScans; ii++) {
        // Create the scan
        scans.emplace_back(createScan(scanCollection,
                                      {"user"},
                                      {"user\xFF"},
                                      {/* no snapshot requirements */},
                                      {/* no sampling*/}));
    }

    for (const auto& scan : scans) {
        EXPECT_EQ(cb::engine_errc::success,
                  store->cancelRangeScan(vbid, scan, *cookie));
    }

    auto* q = task_executor->getLpTaskQ(TaskType::AuxIO);
    EXPECT_EQ(task_executor->getNumAuxIO() - 1, q->getFutureQueueSize());

    for (const auto& scan : scans) {
        (void)scan;
        runNextTask(*q, "RangeScanContinueTask");
    }
}

// Create enough scans to max out the default concurrency and then reduce the
// concurrent limit. We should observe that tasks don't pickup scans, but
// instead exit until only the correct number of tasks remain.
TEST_P(RangeScanCreateAndContinueTest, concurrency_maxxed_and_reduce) {
    ASSERT_NE(1, task_executor->getNumAuxIO());

    auto scans = setupConcurrencyMaxxed();

    // Now reduce the internal limit and begin to run scans. expect that the
    // first few tasks don't run a scan, they terminate until only the correct
    // number of tasks remain, those tasks will run the scans.
    getEPBucket().getReadyRangeScans()->setConcurrentTaskLimit(1);

    // Run n - 1 tasks, none of these will scan, all will exit
    auto* q = task_executor->getLpTaskQ(TaskType::AuxIO);
    for (size_t ii = 0, iterations = q->getFutureQueueSize() - 1;
         ii < iterations;
         ii++) {
        runNextTask(*q, "RangeScanContinueTask");
        EXPECT_EQ(0, dummyCallbackCounter);
    }

    auto counter = dummyCallbackCounter;
    EXPECT_EQ(0, q->getFutureQueueSize());
    EXPECT_EQ(1, q->getReadyQueueSize());

    // The final task will run all of the scans
    for (const auto& scan : scans) {
        (void)scan;
        runNextTask(*q, "RangeScanContinueTask");
        EXPECT_EQ(0, q->getReadyQueueSize());
        EXPECT_EQ(1, q->getFutureQueueSize());
        EXPECT_GT(dummyCallbackCounter, counter);
        counter = dummyCallbackCounter;
        destroy_mock_cookie(scan.second);
    }
}

// Dropped collection detection is noted by privilege check when we process a
// range scan continue (different to detection whilst a scan is continuing)
TEST_P(RangeScanCreateAndContinueTest, dropped_collection_for_continue) {
    // Create the scan
    auto uuid1 = createScan(scanCollection,
                            {"user"},
                            {"user\xFF"},
                            {/* no snapshot requirements */},
                            {/* no sampling*/});

    auto uuid2 = createScan(scanCollection,
                            {"user"},
                            {"user\xFF"},
                            {/* no snapshot requirements */},
                            {/* no sampling*/});

    cm.remove(CollectionEntry::vegetable);
    setCollections(cookie, cm);

    // Continue spots the collection has gone and fails the request
    auto continueParams = cb::rangescan::ContinueParameters{
            vbid, uuid1, 0, 0ms, 0, cb::engine_errc::success};
    EXPECT_EQ(cb::engine_errc::unknown_collection,
              store->continueRangeScan(*cookie, continueParams));

    // Scan was removed from the vbucket
    EXPECT_EQ(cb::engine_errc::no_such_key,
              store->cancelRangeScan(vbid, uuid1, *cookie));

    // And a task was still scheduled because the scan has been cancelled as
    // a side affect
    runNextTask(*task_executor->getLpTaskQ(TaskType::AuxIO),
                "RangeScanContinueTask");

    // uuid2 - we just attempt to cancel. The collection has gone, but that's
    // fine - the scan still cancels
    EXPECT_EQ(cb::engine_errc::success,
              store->cancelRangeScan(vbid, uuid2, *cookie));

    // And the second scan cancels on the AUXIO task
    runNextTask(*task_executor->getLpTaskQ(TaskType::AuxIO),
                "RangeScanContinueTask");
}

// Lose access to a collection after create
TEST_P(RangeScanCreateAndContinueTest, lose_access_to_scan) {
    // Create the scan
    auto uuid = createScan(scanCollection,
                           {"user"},
                           {"user\xFF"},
                           {/* no snapshot requirements */},
                           {/* no sampling*/});

    setNoAccess(scanCollection);
    auto continueParams = cb::rangescan::ContinueParameters{
            vbid, uuid, 0, 0ms, 0, cb::engine_errc::success};
    EXPECT_EQ(cb::engine_errc::no_access,
              store->continueRangeScan(*cookie, continueParams));

    EXPECT_EQ(cb::engine_errc::no_access,
              store->cancelRangeScan(vbid, uuid, *cookie));

    EXPECT_EQ(0,
              task_executor->getLpTaskQ(TaskType::AuxIO)->getFutureQueueSize());

    // validate scan still exists and runs
    MockCookie::setCheckPrivilegeFunction({});
    mock_set_privilege_context_revision(2);
    EXPECT_EQ(cb::engine_errc::would_block,
              store->continueRangeScan(*cookie, continueParams));

    runNextTask(*task_executor->getLpTaskQ(TaskType::AuxIO),
                "RangeScanContinueTask");

    if (isKeyOnly()) {
        validateKeyScan(getUserKeys());
    } else {
        validateItemScan(getUserKeys());
    }
}

TEST_P(RangeScanCreateAndContinueTest, cancel_scan_due_to_time_limit) {
    // Create the scan
    auto uuid = createScan(scanCollection,
                           {"user"},
                           {"user\xFF"},
                           {/* no snapshot requirements */},
                           {/* no sampling*/});

    auto vb = store->getVBucket(vbid);

    // Force cancel by time-limit path (0 limit)
    auto& epVb = dynamic_cast<EPVBucket&>(*vb);
    EXPECT_FALSE(epVb.cancelRangeScansExceedingDuration(std::chrono::seconds(0))
                         .has_value());
    auto continueParams = cb::rangescan::ContinueParameters{
            vbid, uuid, 0, 0ms, 0, cb::engine_errc::success};
    EXPECT_EQ(cb::engine_errc::no_such_key,
              store->continueRangeScan(*cookie, continueParams));
}

TEST_P(RangeScanCreateAndContinueTest, cancel_scans_due_to_time_limit) {
    cb::time::steady_clock::use_chrono = false;
    auto scopeGuard = folly::makeGuard(
            []() { cb::time::steady_clock::use_chrono = true; });

    // e.g. 60 seconds
    std::chrono::seconds lifetime{
            store->getConfiguration().getRangeScanMaxLifetime()};
    // e.g. 30 seconds advance at various parts of the test
    auto tick = lifetime / 2;

    auto vb = store->getVBucket(vbid);

    // Create the scans
    auto uuid1 = createScan(
            scanCollection,
            {"user"},
            {"user\xFF"},
            {/* no snapshot requirements */},
            {/* no sampling*/},
            cb::engine_errc::success,
            std::make_unique<DummyRangeScanHandler>(dummyCallbackCounter));

    EXPECT_TRUE(isTimeoutTaskScheduled());
    auto startTime = cb::time::steady_clock::now(); // e.g. 15:00.00
    EXPECT_TRUE(isTimeoutTaskWakeupTime(startTime + lifetime)); // 15:01.00

    using namespace std::chrono_literals;

    // advance time so next scan has a different creation time
    cb::time::steady_clock::advance(tick); // now + 30s = 15:00.30

    // uuid2 is created at 15:00.30 and expires at 15:01.30
    auto uuid2 = createScan(
            scanCollection,
            {"user"},
            {"user\xFF"},
            {/* no snapshot requirements */},
            {/* no sampling*/},
            cb::engine_errc::success,
            std::make_unique<DummyRangeScanHandler>(dummyCallbackCounter));

    // Task schedule and still due for the time of when we need uuid1 to be
    // done by.
    EXPECT_TRUE(isTimeoutTaskScheduled());
    EXPECT_TRUE(isTimeoutTaskWakeupTime(startTime + lifetime)); // 15:01.00

    // // RangeScanOwnerTest::cancelRangeScansExceedingDuration gives more
    // details
    // // to the flow of time (or calls to now())
    // tick = std::chrono::seconds(0);

    // Move time so task runs and we shall see that uuid1 cancels
    cb::time::steady_clock::advance(tick); // now + 30s = 15:01.00
    runNextTask(TaskType::NonIO,
                fmt::format("RangeScanTimeoutTask for {}", vbid));
    // Task runbs and sets the waketime to be the expiry of the next scan
    // And the scan is gone.
    auto continueParams = cb::rangescan::ContinueParameters{
            vbid, uuid1, 0, 0ms, 0, cb::engine_errc::success};
    EXPECT_EQ(cb::engine_errc::no_such_key,
              store->continueRangeScan(*cookie, continueParams));

    // We can start uuid2
    auto continueParams2 = cb::rangescan::ContinueParameters{
            vbid, uuid2, 0, 0ms, 0, cb::engine_errc::success};
    EXPECT_EQ(cb::engine_errc::would_block,
              store->continueRangeScan(*cookie, continueParams2));

    // And expect the task will now wake at the limit of uuid2's lifetime
    EXPECT_TRUE(isTimeoutTaskScheduled());
    EXPECT_TRUE(isTimeoutTaskWakeupTime(startTime + lifetime +
                                        (lifetime / 2))); // 15:01.30

    cb::time::steady_clock::advance(tick); // now + 30s = 15:01.30
    // Task should run again and cancel uuid2, whilst it's runnable though
    runNextTask(TaskType::NonIO,
                fmt::format("RangeScanTimeoutTask for {}", vbid));

    // Task does not reschedule as no scans remain.
    EXPECT_FALSE(isTimeoutTaskScheduled());

    // Scan has been removed from tracking, no_such_key
    EXPECT_EQ(cb::engine_errc::no_such_key,
              store->cancelRangeScan(vbid, uuid2, *cookie));

    // Finally must run the AuxIO to drain out the tasks which got scheduled.
    // When a RangeScan is cancelled an AuxIO task does the work to free any
    // disk resources, hence we have AuxIO tasks to run.
    runNextTask(*task_executor->getLpTaskQ(TaskType::AuxIO),
                "RangeScanContinueTask");
    runNextTask(*task_executor->getLpTaskQ(TaskType::AuxIO),
                "RangeScanContinueTask");
    // In this test 3 tasks were scheduled - 1 for the continue and 2 for cancel
    runNextTask(*task_executor->getLpTaskQ(TaskType::AuxIO),
                "RangeScanContinueTask");
}

TEST_P(RangeScanCreateAndContinueTest, rangeScanWithXattrs) {
    auto keyXattr = makeStoredDocKey("xattrsIncluded", scanCollection);
    auto keyNoXattr = makeStoredDocKey("xattrsExcluded", scanCollection);

    // Store one document with xattrs
    store_item(vbid,
               keyXattr,
               createXattrValue(keyXattr.to_string()),
               0,
               {cb::engine_errc::success},
               PROTOCOL_BINARY_DATATYPE_XATTR);
    // And another without xattrs
    store_item(vbid, keyNoXattr, keyNoXattr.to_string());
    flushVBucket(vbid);

    std::unordered_set<StoredDocKey> expectedKeys;
    expectedKeys.emplace(keyXattr);
    expectedKeys.emplace(keyNoXattr);

    // Run scan - will also validate xattrs included/excluded respectively
    testRangeScan(expectedKeys, scanCollection, {"xattrs"}, {"xattrs\xFF"});
}

TEST_P(RangeScanCreateAndContinueTest, randomSampleWithXattrs) {
    // Empty collection
    setCollections(cookie, cm.remove(CollectionEntry::vegetable));
    setCollections(cookie, cm.add(CollectionEntry::vegetable));

    auto keyXattr = makeStoredDocKey("xattrsIncluded", scanCollection);
    auto keyNoXattr = makeStoredDocKey("xattrsExcluded", scanCollection);

    // Store one document with xattrs
    store_item(vbid,
               keyXattr,
               createXattrValue(keyXattr.to_string()),
               0,
               {cb::engine_errc::success},
               PROTOCOL_BINARY_DATATYPE_XATTR);
    // Add another without xattrs
    store_item(vbid, keyNoXattr, keyNoXattr.to_string());
    flushVBucket(vbid);

    std::unordered_set<StoredDocKey> expectedKeys;
    expectedKeys.emplace(keyXattr);
    expectedKeys.emplace(keyNoXattr);

    // Request whole collection
    auto sampleSize = expectedKeys.size();
    auto uuid = createScan(scanCollection,
                           {"\0", 1},
                           {"\xFF"},
                           {/* no snapshot requirements */},
                           cb::rangescan::SamplingConfiguration{sampleSize, 0});
    continueRangeScan(uuid, 0, 0ms, 0, cb::engine_errc::range_scan_complete);

    if (isKeyOnly()) {
        validateKeyScan(expectedKeys);
    } else {
        // Will also validate xattrs included/excluded
        validateItemScan(expectedKeys);
    }
}

bool TestRangeScanHandler::validateContinueStatus(cb::engine_errc code) {
    switch (code) {
    case cb::engine_errc::range_scan_more:
    case cb::engine_errc::range_scan_complete:
    case cb::engine_errc::range_scan_cancelled:
    case cb::engine_errc::failed: // scan() failure
    case cb::engine_errc::not_my_vbucket:
    case cb::engine_errc::unknown_collection:
        // A RangeScan continue can end with any of the above status codes
        return true;
    case cb::engine_errc::success: // more/complete are used instead of success
    case cb::engine_errc::no_such_key:
    case cb::engine_errc::key_already_exists:
    case cb::engine_errc::no_memory:
    case cb::engine_errc::not_stored:
    case cb::engine_errc::invalid_arguments:
    case cb::engine_errc::not_supported:
    case cb::engine_errc::throttled:
    case cb::engine_errc::would_block:
    case cb::engine_errc::too_much_data_in_output_buffer:
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
    case cb::engine_errc::not_locked:
    case cb::engine_errc::cas_value_invalid:
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
    case cb::engine_errc::vbuuid_not_equal:
    case cb::engine_errc::cancelled:
    case cb::engine_errc::bucket_paused:
    case cb::engine_errc::encryption_key_not_available:
    case cb::engine_errc::checksum_mismatch:
        return false;
    }
    throw std::invalid_argument(
            "TestRangeScanHandler::validateContinueStatus: code does not "
            "represent a legal error code: " +
            std::to_string(int(code)));
}

// This test case uses a simpler data set as the "broader" test data of
// RangeScanTest::Setup is not required and just adds unnecessary noise
class RangeScanTestSimple : public RangeScanTest {
public:
    void SetUp() override {
        setupConfig();
        SingleThreadedEPBucketTest::SetUp();
        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

        // Setup one collection - the tests will decide their keys
        cm.add(CollectionEntry::vegetable);
        setCollections(cookie, cm);
        flush_vbucket_to_disk(vbid, 1);
    }
};

// SDK team noted exclusive end was incorrect, this test covers the failure.
TEST_P(RangeScanTestSimple, MB_53184) {
    std::unordered_set<StoredDocKey> expectedKeys;
    for (const auto& k : {"a-11", "a-12", "b-11"}) {
        auto key = makeStoredDocKey(k, scanCollection);
        store_item(vbid, key, k);
        expectedKeys.emplace(key);
    }
    // and store this extra "c" prefixed key
    store_item(vbid, makeStoredDocKey("c-12", scanCollection), "value");
    flushVBucket(vbid);

    // Expect: a-11, a-12 and b-11
    // Note: this is the actual MB
    testRangeScan(expectedKeys,
                  scanCollection,
                  {"\0", 1}, // from min
                  {"c", cb::rangescan::KeyType::Exclusive});

    runNextTask(*task_executor->getLpTaskQ(TaskType::AuxIO),
                "RangeScanContinueTask");

    // Run a second exclusive end similar to the MB, this test-case was fine
    scannedKeys.clear();
    handler = std::make_unique<TestRangeScanHandler>(
            scannedItems, scannedKeys, testHook);
    // Expect: a-11, a-12 and b-11
    testRangeScan(expectedKeys,
                  scanCollection,
                  {"\0", 1}, // from min
                  {"c-12", cb::rangescan::KeyType::Exclusive});
}

class RangeScanOwnerTest : public SingleThreadedEPBucketTest {
public:
    void SetUp() override {
        SingleThreadedEPBucketTest::SetUp();
        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    }

    void TearDown() override {
        SingleThreadedEPBucketTest::TearDown();
    }

    cb::rangescan::Id addScan(VB::RangeScanOwner& rangeScans) {
        static int scanId = 0;
        boost::uuids::name_generator_sha1 gen(boost::uuids::ns::dns());
        boost::uuids::uuid udoc = gen(std::to_string(++scanId));
        EXPECT_EQ(cb::engine_errc::success,
                  rangeScans.addNewScan(
                          std::make_shared<RangeScan>(
                                  udoc, store->getKVStoreScanTracker()),
                          getEPVBucket(),
                          store->getEPEngine()));
        return udoc;
    }

    EPVBucket& getEPVBucket() {
        auto vb = store->getVBucket(vbid);
        EXPECT_TRUE(vb);
        return static_cast<EPVBucket&>(*vb);
    }
};

TEST_F(RangeScanOwnerTest, cancelRangeScansExceedingDuration) {
    VB::RangeScanOwner& rangeScans = getEPVBucket().getRangeScans();

    // RangeScan::now will tick 5 seconds per call
    static std::chrono::seconds tick = std::chrono::seconds(5);
    RangeScan::setClockFunction([]() {
        static auto now = cb::time::steady_clock::now();
        now += tick;
        return now;
    });
    auto* nonIOQueue = task_executor->getLpTaskQ(TaskType::NonIO);
    auto initialQueueSize = nonIOQueue->getFutureQueueSize();

    auto t5 = addScan(rangeScans); // now() = T5
    // first scan schedules the task
    EXPECT_EQ(initialQueueSize + 1, nonIOQueue->getFutureQueueSize());
    auto t10 = addScan(rangeScans); // now() = T10
    // no changes expected
    EXPECT_EQ(initialQueueSize + 1, nonIOQueue->getFutureQueueSize());

    // Freeze the flow of time. cancelRangeScansExceedingDuration will call
    // now() once for each scan (and there's no known order in which our T5 or
    // T10 scan will be visited). Keeping time frozen allows the test to check
    // for an outcome irrespective of the order the scans are tested.
    tick = std::chrono::seconds(0);

    // Use 9 seconds as the timeLimit parameter, i.e. any scan which has existed
    // for longer than 9 seconds should be cancelled. Time is frozen at T10,
    // thus both scans are under the 9 second limit. The function returns though
    // how many seconds until a scan would need cancelling (for use in setting
    // the sleep of our watchdog). The T5 scan will have existed for 9 seconds
    // at T14 and now() is T10, thus the expected return value is 4.
    auto rv = rangeScans.cancelAllExceedingDuration(getEPBucket(),
                                                    std::chrono::seconds(9));
    EXPECT_EQ(2, rangeScans.size());
    ASSERT_TRUE(rv.has_value());
    EXPECT_EQ(std::chrono::seconds(4), rv.value());

    // With time still frozen, call again. But this time setting a much smaller
    // limit of 3 seconds. With now() being T10, this means the first scan (T5)
    // has existed too long and is cancelled. The return value of 3 is when the
    // T10 scan should be cancelled.
    rv = rangeScans.cancelAllExceedingDuration(getEPBucket(),
                                               std::chrono::seconds(3));
    EXPECT_EQ(1, rangeScans.size()); // 1 as the first scan was removed
    ASSERT_TRUE(rv.has_value());
    EXPECT_EQ(std::chrono::seconds(3), rv.value());
    // Check the t10 scan remains
    ASSERT_TRUE(rangeScans.getScan(t10));
    EXPECT_FALSE(rangeScans.getScan(t5));
    EXPECT_EQ(t10, rangeScans.getScan(t10)->getUuid());

    // Now force cancel of T10, unfreeze time
    tick = std::chrono::seconds(5);
    rv = rangeScans.cancelAllExceedingDuration(getEPBucket(),
                                               std::chrono::seconds(1));
    EXPECT_EQ(0, rangeScans.size());
    // task still queued, but is dead (was cancelled)
    EXPECT_EQ(initialQueueSize + 1, nonIOQueue->getFutureQueueSize());
    CheckedExecutor executor(task_executor,
                             *task_executor->getLpTaskQ(TaskType::NonIO));
    EXPECT_TRUE(executor.getCurrentTask()->isdead());
    EXPECT_EQ("RangeScanTimeoutTask for vb:0",
              executor.getCurrentTask()->getDescription());

    // The return value has no value, which would be interpreted by the watch
    // dog task as sleep forever
    ASSERT_FALSE(rv.has_value());
}

// Test that RangeScan create fails once the limit is reached
TEST_P(RangeScanTestSimple, limitRangeScans) {
    // Override the KVStoreScanTracker and set that 1 RangeScan can be created
    store->getKVStoreScanTracker().setMaxRunningScans(1, 1, 1);
    EXPECT_EQ(1, store->getKVStoreScanTracker().getMaxRunningRangeScans());
    EXPECT_EQ(0, store->getKVStoreScanTracker().getNumRunningRangeScans());

    // 1 key is required for the scan to create successfully
    store_item(vbid, makeStoredDocKey("key", scanCollection), "value");
    flushVBucket(vbid);
    auto uuid1 = createScan(scanCollection,
                            {"\0"},
                            {"\xFF"},
                            {/* no snapshot requirements */},
                            {/* no sampling*/});

    EXPECT_EQ(0, store->getKVStoreScanTracker().getNumRunningBackfills());
    EXPECT_EQ(1, store->getKVStoreScanTracker().getNumRunningRangeScans());

    // Now create gets too_busy
    EXPECT_EQ(cb::engine_errc::too_busy,
              store->createRangeScan(*cookie,
                                     std::move(handler),
                                     cb::rangescan::CreateParameters{
                                             vbid,
                                             CollectionID::Default,
                                             {"\0"},
                                             {"\xff"},
                                             getScanType(),
                                             getIncludeXattrs(),
                                             {},
                                             {}})
                      .first);

    EXPECT_EQ(cb::engine_errc::success,
              store->cancelRangeScan(vbid, uuid1, *cookie));

    EXPECT_EQ(1, store->getKVStoreScanTracker().getNumRunningRangeScans());

    runNextTask(*task_executor->getLpTaskQ(TaskType::AuxIO),
                "RangeScanContinueTask");
    EXPECT_EQ(0, store->getKVStoreScanTracker().getNumRunningRangeScans());
}

TEST_P(RangeScanTestSimple, beforeFlush) {
    Vbid testvb = Vbid(1);
    setVBucketState(testvb, vbucket_state_active);

    // The vbucket is in "creating" state, i.e. nothing flushed (for couchstore
    // there is no 1.couch.1 file)
    EXPECT_EQ(cb::engine_errc::temporary_failure,
              store->createRangeScan(*cookie,
                                     std::move(handler),
                                     cb::rangescan::CreateParameters{
                                             testvb,
                                             CollectionID::Default,
                                             {"\0"},
                                             {"\xff"},
                                             getScanType(),
                                             getIncludeXattrs(),
                                             {},
                                             {}})
                      .first);
}

// A handler which will execute a callback as part of handleStatus
class MB_54053Handler : public RangeScanDataHandlerIFace {
public:
    MB_54053Handler(std::function<void(cb::engine_errc)> cb)
        : callback(std::move(cb)) {
    }

    Status handleKey(DocKeyView key) override {
        return Status::OK;
    }

    Status handleItem(std::unique_ptr<Item>) override {
        return Status::OK;
    }

    std::unique_ptr<RangeScanContinueResult> continuePartialOnFrontendThread()
            override {
        // no-op
        return {};
    }

    std::unique_ptr<RangeScanContinueResult> continueMoreOnFrontendThread()
            override {
        // no-op
        return {};
    }

    std::unique_ptr<RangeScanContinueResult> completeOnFrontendThread()
            override {
        // no-op
        return {};
    }

    std::unique_ptr<RangeScanContinueResult> cancelOnFrontendThread() override {
        // no-op
        return {};
    }

    void addStats(std::string_view prefix,
                  const StatCollector& collector) override {
    }
    std::function<void(cb::engine_errc)> callback;
};

// This test 'weaves' the continue of a single scan with some overlap where a
// race condition lead to an exception
TEST_P(RangeScanTestSimple, MB_54053) {
    // 2 keys required
    auto k1 = makeStoredDocKey("key1", scanCollection);
    auto k2 = makeStoredDocKey("key2", scanCollection);

    store_item(vbid, k1, "value");
    store_item(vbid, k2, "value");
    flushVBucket(vbid);

    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);

    cb::rangescan::Id scanId;
    auto* kvs = store->getRWUnderlying(vbid);
    ASSERT_TRUE(kvs);

    // Setup a callback function which will move the scan to the continue state.
    // This is called when the first continuation sets back to idle and signals
    // to the first request the status.
    std::shared_ptr<RangeScan> scan2;
    auto callback = [this, &scanId, &scan2](cb::engine_errc status) {
        if (status == cb::engine_errc::range_scan_more) {
            scan2->setStateContinuing(
                    *cookie, 1, std::chrono::milliseconds{0}, 0);
            scan2->prepareToRunOnContinueTask();
        }
    };

    // 1 scan is required which is manually created so we can drive it forward
    auto scan1 = std::make_shared<RangeScan>(
            epBucket,
            *vb,
            DiskDocKey{k1},
            DiskDocKey{k2},
            std::make_unique<MB_54053Handler>(std::move(callback)),
            *cookie,
            cb::rangescan::KeyOnly::Yes,
            cb::rangescan::IncludeXattrs::No,
            std::optional<cb::rangescan::SnapshotRequirements>{},
            std::optional<cb::rangescan::SamplingConfiguration>{},
            std::string{});
    scanId = scan1->getUuid();
    // Set a second reference on the scan to demonstrate the original bug
    scan2 = scan1;

    scan1->setStateContinuing(*cookie, 1, std::chrono::milliseconds{0}, 0);
    auto result1 = scan1->prepareToRunOnContinueTask();
    EXPECT_TRUE(result1.cookie);
    EXPECT_EQ(result1.status, cb::engine_errc::range_scan_more);
    scan1->continueOnIOThread(*kvs);

    // MB-56855: updates this test, no longer can prepare be called twice whilst
    // the scan is in the continue state, that violates the expectations of how
    // a single RangeScan is queued by RangeScanOwner (queued once) and how
    // state changes are made. So cancel the scan and check the prepareToRun
    // function.
    scan2->setStateCancelled(cb::engine_errc::not_my_vbucket);
    auto result2 = scan2->prepareToRunOnContinueTask();
    EXPECT_EQ(result2.status, cb::engine_errc::not_my_vbucket);

    // The cookie is null as it can only be used once
    EXPECT_FALSE(result2.cookie);
}

auto valueScanConfig = ::testing::Combine(
        ::testing::Values("persistent_couchdb"
#ifdef EP_USE_MAGMA
                          ,
                          "persistent_magma",
                          "persistent_nexus_couchstore_magma"
#endif
                          ),
        ::testing::Values("value_only", "full_eviction"),
        ::testing::Values("value_scan", "value_scan_include_xattrs"));

auto keyScanConfig =
        ::testing::Combine(::testing::Values("persistent_couchdb"
#ifdef EP_USE_MAGMA
                                             ,
                                             "persistent_magma",
                                             "persistent_nexus_couchstore_magma"
#endif
                                             ),
                           ::testing::Values("full_eviction"),
                           ::testing::Values("key_scan"));

// Test that only attempt to create a scan have no need to run in key and value
// variations. Use key only
INSTANTIATE_TEST_SUITE_P(RangeScanCreateKeyScan,
                         RangeScanCreateTest,
                         keyScanConfig,
                         RangeScanTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(RangeScanValueScan,
                         RangeScanCreateAndContinueTest,
                         valueScanConfig,
                         RangeScanTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(RangeScanKeyScan,
                         RangeScanCreateAndContinueTest,
                         keyScanConfig,
                         RangeScanTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(RangeScanTestSimpleKeyScan,
                         RangeScanTestSimple,
                         keyScanConfig,
                         RangeScanTest::PrintToStringParamName);
