/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "ep_bucket.h"
#include "evp_store_single_threaded_test.h"
#include "item.h"
#include "test_helpers.h"
#include "vbucket.h"
#include "warmup.h"

#include <platform/dirutils.h>

class AccessLogTest : public STParamPersistentBucketTest {
public:
    void SetUp() override {
        setupPrimaryWarmupOnly();
        config_string +=
                "alog_resident_ratio_threshold=100;alog_max_stored_items=10;"
                "alog_path=" +
                test_dbname + cb::io::DirectorySeparator + "access.log";

        STParamPersistentBucketTest::SetUp();
        // Run warmup (empty) but this will ensure access scanner is runnable
        // in the test body
        static_cast<EPBucket*>(engine->getKVBucket())->initializeWarmupTask();
        static_cast<EPBucket*>(engine->getKVBucket())->startWarmupTask();
        runReadersUntilWarmedUp();

        ASSERT_TRUE(store->isAccessScannerEnabled());
        // Two vbs and these land in different shards
        setVBucketStateAndRunPersistTask(vbid0, vbucket_state_active);
        setVBucketStateAndRunPersistTask(vbid1, vbucket_state_active);
    }
    Vbid vbid0{0};
    Vbid vbid1{1};

    void generateAccessLog() {
        // Generate the access logs
        ASSERT_TRUE(store->runAccessScannerTask());
        auto& auxioQueue = *task_executor->getLpTaskQ(TaskType::AuxIO);
        runNextTask(auxioQueue);
        runNextTask(auxioQueue);
        runNextTask(auxioQueue);
    }
};

// Test will create two vbuckets and populate with 2 items in each vb. The
// assumption is that if 1 item from each VB is evicted prior to generating
// the access log then warmup (with a configuration that cannot load all items)
// should return the cache to pre-warmup state.
TEST_P(AccessLogTest, WarmupWithAccessLog) {
    auto key1 = makeStoredDocKey("key1");
    auto key2 = makeStoredDocKey("key2");
    auto key3 = makeStoredDocKey("key3");
    auto key4 = makeStoredDocKey("key4");

    store_item(vbid0, key1, "value");
    store_item(vbid0, key2, "value");
    store_item(vbid1, key3, "value");
    store_item(vbid1, key4, "value");

    flush_vbucket_to_disk(vbid0, 2);
    flush_vbucket_to_disk(vbid1, 2);

    // Now evict the keys we don't want to see post warmup. Deliberately evict
    // the lowest seqno/first written.
    evict_key(vbid0, key1);
    evict_key(vbid1, key3);

    auto check = [this, &key1, &key2, &key3, &key4]() {
        // What is resident?
        auto vb0 = store->getVBuckets().getBucket(vbid0);
        auto vb1 = store->getVBuckets().getBucket(vbid1);
        ASSERT_TRUE(vb0 && vb1);

        auto r1 = vb0->ht.findForRead(key1);
        auto r2 = vb0->ht.findForRead(key2);
        auto r3 = vb1->ht.findForRead(key3);
        auto r4 = vb1->ht.findForRead(key4);

        if (r1.storedValue) {
            // Must be non-resident value
            EXPECT_FALSE(r1.storedValue->isResident());
        }
        if (r3.storedValue) {
            // Must be non-resident value
            EXPECT_FALSE(r3.storedValue->isResident());
        }

        ASSERT_TRUE(r2.storedValue && r4.storedValue);
        EXPECT_TRUE(r2.storedValue->isResident()) << *r2.storedValue;
        EXPECT_TRUE(r4.storedValue->isResident()) << *r4.storedValue;
    };

    check();

    generateAccessLog();

    // Reset and set a new config to ensure 1/2 items are loaded.
    // This will mean that after the LoadingAccessLog warmup phase has loaded
    // the two items, warmup will not attempt LoadingData and the final expects
    // will be met.
    const auto config = buildNewWarmupConfig(
            "warmup_behavior=use_config;"
            "primary_warmup_min_items_threshold=50;"
            "primary_warmup_min_memory_threshold=100;"
            "secondary_warmup_min_items_threshold=0;"
            "secondary_warmup_min_memory_threshold=0;"
            "bfilter_enabled=false");
    resetEngineAndWarmup(config);
    // Post-warmup should pass the same check as pre-warmup.
    // MB-59262 fails here as key1/key3 are resident and key2/key4 are not
    check();

    // Check  the estimated key/valueitem count.
    const auto* primary = getEPBucket().getPrimaryWarmup();
    // secondary should of cloned the estimated item count, which is initialised
    // in a warm-up step that is skipped by Secondary.
    EXPECT_EQ(4, primary->getEstimatedKeyCount());
    EXPECT_EQ(4, primary->getEstimatedValueCount());

    // 2 of 4 values are loaded
    EXPECT_EQ(2, engine->getEpStats().warmedUpValues);

    // For keys, value vs full eviction differ. All the keys were loaded in
    // value mode.
    if (fullEviction()) {
        EXPECT_EQ(2, engine->getEpStats().warmedUpKeys); // 50% (2/4)
    } else {
        EXPECT_EQ(4, engine->getEpStats().warmedUpKeys); // 100% (4/4)
    }
}

class MutationLogApplyTest : public SingleThreadedKVBucketTest {
public:
    Vbid vbid0{0};
    Vbid vbid1{1};
};

static bool loadFn(Vbid vb, const std::set<StoredDocKey>& keys, void* arg) {
    auto& map = *reinterpret_cast<
            std::unordered_map<Vbid, std::unordered_set<StoredDocKey>>*>(arg);
    for (const auto& key : keys) {
        map[vb].insert(key);
    }
    return true;
}

// Test is checking MutationLogHarvester::apply with both
// removeNonExistentKeys=true and removeNonExistentKeys=false
TEST_F(MutationLogApplyTest, Apply) {
    auto logName = test_dbname + cb::io::DirectorySeparator + "access.log";
    setVBucketStateAndRunPersistTask(vbid0, vbucket_state_active);
    setVBucketStateAndRunPersistTask(vbid1, vbucket_state_active);
    std::unordered_set<StoredDocKey> vb0Keys, vb1Keys;

    {
        MutationLog ml(logName);
        ml.open();

        // Place vb0 items in the log
        for (size_t ii = 0; ii < 3; ii++) {
            auto key = makeStoredDocKey("vb0_key" + std::to_string(ii));
            ml.newItem(vbid0, key);
            vb0Keys.insert(key);
        }
        // Place vb1 items in the hash table and the log
        for (size_t ii = 0; ii < 3; ii++) {
            auto key = makeStoredDocKey("vb1_key" + std::to_string(ii));
            store_item(vbid1, key, "value");
            ml.newItem(vbid1, key);
            vb1Keys.insert(key);
        }
        ml.commit1();
        ml.commit2();

        EXPECT_EQ(6, ml.itemsLogged[int(MutationLogType::New)]);
        EXPECT_EQ(1, ml.itemsLogged[int(MutationLogType::Commit1)]);
        EXPECT_EQ(1, ml.itemsLogged[int(MutationLogType::Commit2)]);
    }

    // Apply the log and ask for non-existent keys to get removed
    {
        MutationLog ml(logName);
        ml.open();
        MutationLogHarvester h(ml, engine.get());
        h.setVBucket(vbid0);
        h.setVBucket(vbid1);

        h.loadBatch(6);
        std::unordered_map<Vbid, std::unordered_set<StoredDocKey>> map;
        h.apply(&map, loadFn, true);
        EXPECT_EQ(0, map.count(vbid0));
        ASSERT_EQ(1, map.count(vbid1));

        EXPECT_EQ(vb1Keys.size(), map[vbid1].size());
        for (const auto& key : vb1Keys) {
            EXPECT_EQ(1, map[vbid1].count(key));
        }
    }

    // Apply the log and all keys should be applied
    {
        MutationLog ml(logName);
        ml.open();
        MutationLogHarvester h(ml, engine.get());
        h.setVBucket(vbid0);
        h.setVBucket(vbid1);
        h.loadBatch(6);

        std::unordered_map<Vbid, std::unordered_set<StoredDocKey>> map;
        h.apply(&map, loadFn, false);
        ASSERT_EQ(1, map.count(vbid0));
        ASSERT_EQ(1, map.count(vbid1));

        EXPECT_EQ(vb0Keys.size(), map[vbid0].size());
        for (const auto& key : vb0Keys) {
            EXPECT_EQ(1, map[vbid0].count(key));
        }
        EXPECT_EQ(vb1Keys.size(), map[vbid1].size());
        for (const auto& key : vb1Keys) {
            EXPECT_EQ(1, map[vbid1].count(key));
        }
    }
}

// Test provides coverage over co-operative access log warm-up, checking that
// a reader thread isn't blocked for the entire duration of access log loading.
TEST_P(AccessLogTest, ReadAndWarmup) {
    // Test requires to bgfetch via reader tasks
    ASSERT_TRUE(getEPBucket().startBgFetcher());
    std::vector<std::pair<Vbid, StoredDocKey>> keys;
    keys.emplace_back(vbid0, makeStoredDocKey("key0"));
    keys.emplace_back(vbid0, makeStoredDocKey("key1"));
    keys.emplace_back(vbid1, makeStoredDocKey("key2"));
    keys.emplace_back(vbid1, makeStoredDocKey("key3"));

    for (const auto& key : keys) {
        store_item(key.first, key.second, "value");
    }

    flush_vbucket_to_disk(vbid0, 2);
    flush_vbucket_to_disk(vbid1, 2);

    generateAccessLog();

    // Warmup and set the "chunk" to 0, this means that each key loaded will
    // yield and avoids any need to hack around with time.
    const auto config =
            buildNewWarmupConfig(
                    "warmup_accesslog_load_duration=0;warmup_batch_size=1") +
            ";" + "bfilter_enabled=false";
    resetEngineAndEnableWarmup(config);
    ASSERT_TRUE(getEPBucket().startBgFetcher());

    // Step warmup until LoadingAccessLog
    auto& readerQueue = *task_executor->getLpTaskQ(TaskType::Reader);
    auto* warmup = store->getPrimaryWarmup();
    ASSERT_TRUE(warmup);
    while (warmup->getWarmupState() != WarmupState::State::LoadingAccessLog) {
        runNextTask(readerQueue);
    }

    // Reading a key will require a bg-fetch at this point, the test will check
    // it becomes interleaved with the access-log loading (which can also
    // read the key and populate the cache).
    auto options = QUEUE_BG_FETCH;

    for (const auto& key : keys) {
        auto gv = store->get(key.second, key.first, cookie, options);
        ASSERT_EQ(cb::engine_errc::would_block, gv.getStatus());
    }

    const auto& stats = engine->getEpStats();
    auto fetched = stats.bg_fetched;
    int loaded = 0;
    while (warmup->getWarmupState() == WarmupState::State::LoadingAccessLog) {
        // Run a mixture of BgFetch and Warmup
        runNextTask(readerQueue);
        if (fetched != stats.bg_fetched) {
            ++loaded;
        }
    }

    // Possible that 3 of 4 keys were loaded and the next reader is going to
    // load the 4th, so just check 3 were loaded.
    ASSERT_GE(loaded, 3);
}

// The test config here only wants to cover persistent and both eviction modes
// running all backends is wasted effort
INSTANTIATE_TEST_SUITE_P(AccessLogTest,
                         AccessLogTest,
                         STParameterizedBucketTest::couchstoreConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);