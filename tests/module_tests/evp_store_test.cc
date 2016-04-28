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
 * Unit tests for the EventuallyPersistentStore class.
 *
 * Note that these test do *not* have the normal Tasks running (BGFetcher,
 * flusher etc) as we do not initialise EPEngine. This means that such tasks
 * need to be manually run. This can be very helpful as it essentially gives us
 * synchronous control of EPStore.
 */

#include "evp_store_test.h"

#include "bgfetcher.h"
#include "checkpoint.h"
#include "dcp/dcpconnmap.h"
#include "ep_engine.h"
#include "flusher.h"
#include "tapconnmap.h"

#include "programs/engine_testapp/mock_server.h"
#include <platform/dirutils.h>

#include <thread>

SynchronousEPEngine::SynchronousEPEngine(const std::string& extra_config)
    : EventuallyPersistentEngine(get_mock_server_api) {
    maxFailoverEntries = 1;

    // Merge any extra config into the main configuration.
    if (extra_config.size() > 0) {
        if (!configuration.parseConfiguration(extra_config.c_str(),
                                              serverApi)) {
            throw std::invalid_argument("Unable to parse config string: " +
                                        extra_config);
        }
    }

    // workload is needed by EPStore's constructor (to construct the
    // VBucketMap).
    workload = new WorkLoadPolicy(/*workers*/1, /*shards*/1);

    // dcpConnMap_ is needed by EPStore's constructor.
    dcpConnMap_ = new DcpConnMap(*this);

    // tapConnMap is needed by queueDirty.
    tapConnMap = new TapConnMap(*this);

    // checkpointConfig is needed by CheckpointManager (via EPStore).
    checkpointConfig = new CheckpointConfig(*this);
}

void SynchronousEPEngine::setEPStore(EventuallyPersistentStore* store) {
    cb_assert(epstore == nullptr);
    epstore = store;
}

MockEPStore::MockEPStore(EventuallyPersistentEngine &theEngine)
    : EventuallyPersistentStore(theEngine) {}

VBucketMap& MockEPStore::getVbMap() {
    return vbMap;
}

/* Mock Task class. Doesn't actually run() or snooze() - they both do nothing.
 */
class MockGlobalTask : public GlobalTask {
public:
    MockGlobalTask(Taskable& t, const Priority &p)
        : GlobalTask(t, p) {}

    bool run() override { return false; }
    std::string getDescription() override { return "MockGlobalTask"; }

    void snooze(const double secs) override {}
};

void EventuallyPersistentStoreTest::SetUp() {
    // Paranoia - kill any existing files in case they are left over
    // from a previous run.
    CouchbaseDirectoryUtilities::rmrf(test_dbname);

    engine.reset(new SynchronousEPEngine(config_string));
    ObjectRegistry::onSwitchThread(engine.get());

    store = new MockEPStore(*engine);
    engine->setEPStore(store);

    // Need to initialize ep_real_time and friends.
    initialize_time_functions(get_mock_server_api()->core);

    cookie = create_mock_cookie();
}

void EventuallyPersistentStoreTest::TearDown() {
    destroy_mock_cookie(cookie);
    destroy_mock_event_callbacks();
    ObjectRegistry::onSwitchThread(nullptr);
    engine.reset();

    // Shutdown the ExecutorPool singleton (initialized when we create
    // an EventuallyPersistentStore object). Must happen after engine
    // has been destroyed (to allow the tasks the engine has
    // registered a chance to be unregistered).
    ExecutorPool::shutdown();
}

Item EventuallyPersistentStoreTest::make_item(uint16_t vbid,
                                              const std::string& key,
                                              const std::string& value) {
    Item item(key.c_str(), key.size(), /*flags*/0, /*exp*/0, value.c_str(),
              value.size());
    item.setVBucketId(vbid);
    return item;
}

void EventuallyPersistentStoreTest::store_item(uint16_t vbid,
                                               const std::string& key,
                                               const std::string& value) {
    auto item = make_item(vbid, key, value);
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, nullptr));
}

void EventuallyPersistentStoreTest::flush_vbucket_to_disk(uint16_t vbid) {
    int result;
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(5);

    // Need to retry as warmup may not have completed.
    do {
        result = store->flushVBucket(vbid);
        if (result != RETRY_FLUSH_VBUCKET) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    } while (std::chrono::steady_clock::now() < deadline);

    ASSERT_EQ(1, result) << "Failed to flush the one item we have stored.";
}

void EventuallyPersistentStoreTest::delete_item(uint16_t vbid,
                                                const std::string& key) {
    uint64_t cas = 0;
    mutation_descr_t mut_info;
    EXPECT_EQ(ENGINE_SUCCESS,
              store->deleteItem(key, &cas, vbid, cookie, /*force*/false,
                                /*itemMeta*/nullptr, &mut_info));
}

void EventuallyPersistentStoreTest::evict_key(uint16_t vbid,
                                              const std::string& key) {
    const char* msg;
    size_t msg_size{sizeof(msg)};
    EXPECT_EQ(ENGINE_SUCCESS, store->evictKey(key, vbid, &msg, &msg_size));
    EXPECT_EQ("Ejected.", msg);
}

class EPStoreEvictionTest : public EventuallyPersistentStoreTest,
                             public ::testing::WithParamInterface<std::string> {
    void SetUp() override {
        config_string += std::string{"item_eviction_policy="} + GetParam();
        EventuallyPersistentStoreTest::SetUp();

        // Have all the objects, activate vBucket zero so we can store data.
        store->setVBucketState(vbid, vbucket_state_active, false);

    }
};

// getKeyStats tests //////////////////////////////////////////////////////////

// Check that keystats on resident items works correctly.
TEST_P(EPStoreEvictionTest, GetKeyStatsResident) {
    key_stats kstats;

    // Should start with key not existing.
    EXPECT_EQ(ENGINE_KEY_ENOENT,
              store->getKeyStats("key", 0, cookie, kstats,
                                 /*wantsDeleted*/false));

    store_item(0, "key", "value");
    EXPECT_EQ(ENGINE_SUCCESS,
              store->getKeyStats("key", 0, cookie, kstats,
                                 /*wantsDeleted*/false))
        << "Expected to get key stats on existing item";
    EXPECT_EQ(vbucket_state_active, kstats.vb_state);
    EXPECT_FALSE(kstats.logically_deleted);
}

// Check that keystats on ejected items. When ejected should return ewouldblock
// until bgfetch completes.
TEST_P(EPStoreEvictionTest, GetKeyStatsEjected) {
    key_stats kstats;

    // Store then eject an item. Note we cannot forcefully evict as we have
    // to ensure it's own disk so we can later bg fetch from there :)
    store_item(vbid, "key", "value");

    // Trigger a flush to disk.
    flush_vbucket_to_disk(vbid);

    /**
     * Although a flushVBucket writes the item to the underlying store,
     * the item is not marked clean until an explicit commit is called
     * If the underlying store is couchstore, a commit is called with
     * a flushVBucket but in the case of forestdb, a commit is not
     * always called, hence call an explicit commit.
     */
    uint16_t numShards = store->getVbMap().getNumShards();

    store->commit(vbid % numShards);

    evict_key(vbid, "key");

    // Setup a lambda for how we want to call getKeyStats (saves repeating the
    // same arguments for each instance below).
    auto do_getKeyStats = [this, &kstats]() {
        return store->getKeyStats("key", vbid, cookie, kstats,
                                  /*wantsDeleted*/false);
    };

    if (GetParam() == "value_only") {
        EXPECT_EQ(ENGINE_SUCCESS, do_getKeyStats())
            << "Expected to get key stats on evicted item";

    } else if (GetParam() == "full_eviction") {

        // Try to get key stats. This should return EWOULDBLOCK (as the whole
        // item is no longer resident). As we arn't running the full EPEngine
        // task system, then no BGFetch task will be automatically run, we'll
        // manually run it.

        EXPECT_EQ(ENGINE_EWOULDBLOCK, do_getKeyStats())
            << "Expected to need to go to disk to get key stats on fully evicted item";

        // Try a second time - this should detect the already-created temp
        // item, and re-schedule the bgfetch.
        EXPECT_EQ(ENGINE_EWOULDBLOCK, do_getKeyStats())
            << "Expected to need to go to disk to get key stats on fully evicted item (try 2)";

        // Manually run the BGFetcher task; to fetch the two outstanding
        // requests (for the same key).
        MockGlobalTask mockTask(engine->getTaskable(),
                                Priority::BgFetcherPriority);
        store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);

        EXPECT_EQ(ENGINE_SUCCESS, do_getKeyStats())
            << "Expected to get key stats on evicted item after notify_IO_complete";

    } else {
        FAIL() << "Unhandled GetParam() value:" << GetParam();
    }
}

// Create then delete an item, checking we get keyStats reporting the item as
// deleted.
TEST_P(EPStoreEvictionTest, GetKeyStatsDeleted) {
    auto& epstore = *engine->getEpStore();
    key_stats kstats;

    store_item(0, "key", "value");
    delete_item(vbid, "key");

    // Should get ENOENT if we don't ask for deleted items.
    EXPECT_EQ(ENGINE_KEY_ENOENT,
              epstore.getKeyStats("key", 0, cookie, kstats,
                                  /*wantsDeleted*/false));

    // Should get success (and item flagged as deleted) if we ask for deleted
    // items.
    EXPECT_EQ(ENGINE_SUCCESS,
              epstore.getKeyStats("key", 0, cookie, kstats,
                                  /*wantsDeleted*/true));
    EXPECT_EQ(vbucket_state_active, kstats.vb_state);
    EXPECT_TRUE(kstats.logically_deleted);
}

// Check incorrect vbucket returns not-my-vbucket.
TEST_P(EPStoreEvictionTest, GetKeyStatsNMVB) {
    auto& epstore = *engine->getEpStore();
    key_stats kstats;

    EXPECT_EQ(ENGINE_NOT_MY_VBUCKET,
              epstore.getKeyStats("key", 1, cookie, kstats,
                                  /*wantsDeleted*/false));
}

// Replace tests //////////////////////////////////////////////////////////////

// Test replace against a non-existent key.
TEST_P(EPStoreEvictionTest, ReplaceENOENT) {
    // Should start with key not existing (and hence cannot replace).
    auto item = make_item(vbid, "key", "value");
    EXPECT_EQ(ENGINE_KEY_ENOENT, store->replace(item, cookie));
}

// Test replace against an ejected key.
TEST_P(EPStoreEvictionTest, ReplaceEExists) {

    // Store then eject an item.
    store_item(vbid, "key", "value");
    flush_vbucket_to_disk(vbid);
    evict_key(vbid, "key");

    // Setup a lambda for how we want to call replace (saves repeating the
    // same arguments for each instance below).
    auto do_replace = [this]() {
        auto item = make_item(vbid, "key", "value2");
        return store->replace(item, cookie);
    };

    if (GetParam() == "value_only") {
        // Should be able to replace as still have metadata resident.
        EXPECT_EQ(ENGINE_SUCCESS, do_replace());

    } else if (GetParam() == "full_eviction") {
        // Should get EWOULDBLOCK as need to go to disk to get metadata.
        EXPECT_EQ(ENGINE_EWOULDBLOCK, do_replace());

        // A second request should also get EWOULDBLOCK and add to the
        // existing pending BGFetch
        EXPECT_EQ(ENGINE_EWOULDBLOCK, do_replace());

        // Manually run the BGFetcher task; to fetch the two outstanding
        // requests (for the same key).
        MockGlobalTask mockTask(engine->getTaskable(),
                                Priority::BgFetcherPriority);
        store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);

        EXPECT_EQ(ENGINE_SUCCESS, do_replace())
            << "Expected to replace on evicted item after notify_IO_complete";

    } else {
        FAIL() << "Unhandled GetParam() value:" << GetParam();
    }
}

// Create then delete an item, checking replace reports ENOENT.
TEST_P(EPStoreEvictionTest, ReplaceDeleted) {
    store_item(vbid, "key", "value");
    delete_item(vbid, "key");

    // Replace should fail.
    auto item = make_item(vbid, "key", "value2");
    EXPECT_EQ(ENGINE_KEY_ENOENT, store->replace(item, cookie));
}

// Check incorrect vbucket returns not-my-vbucket.
TEST_P(EPStoreEvictionTest, ReplaceNMVB) {
    auto item = make_item(vbid + 1, "key", "value2");
    EXPECT_EQ(ENGINE_NOT_MY_VBUCKET, store->replace(item, cookie));
}

// Check pending vbucket returns EWOULDBLOCK.
TEST_P(EPStoreEvictionTest, ReplacePendingVB) {
    store->setVBucketState(vbid, vbucket_state_pending, false);
    auto item = make_item(vbid, "key", "value2");
    EXPECT_EQ(ENGINE_EWOULDBLOCK, store->replace(item, cookie));
}

// Set tests //////////////////////////////////////////////////////////////////

// Test set against an ejected key.
TEST_P(EPStoreEvictionTest, SetEExists) {

    // Store an item, then eject it.
    auto item = make_item(vbid, "key", "value");
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, nullptr));
    flush_vbucket_to_disk(vbid);
    evict_key(item.getVBucketId(), item.getKey());

    if (GetParam() == "value_only") {
        // Should be able to set (with same cas as previously)
        // as still have metadata resident.
        ASSERT_NE(0, item.getCas());
        EXPECT_EQ(ENGINE_SUCCESS, store->set(item, cookie));

    } else if (GetParam() == "full_eviction") {
        // Should get EWOULDBLOCK as need to go to disk to get metadata.
        EXPECT_EQ(ENGINE_EWOULDBLOCK, store->set(item, cookie));

        // A second request should also get EWOULDBLOCK and add to the
        // existing pending BGFetch
        EXPECT_EQ(ENGINE_EWOULDBLOCK, store->set(item, cookie));

        // Manually run the BGFetcher task; to fetch the two outstanding
        // requests (for the same key).
        MockGlobalTask mockTask(engine->getTaskable(),
                                Priority::BgFetcherPriority);
        store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);

        EXPECT_EQ(ENGINE_SUCCESS, store->set(item, cookie))
            << "Expected to set on evicted item after notify_IO_complete";

    } else {
        FAIL() << "Unhandled GetParam() value:" << GetParam();
    }
}

// Test CAS set against a non-existent key
TEST_P(EPStoreEvictionTest, SetCASNonExistent) {
    // Create an item with a non-zero CAS.
    auto item = make_item(vbid, "key", "value");
    item.setCas();
    ASSERT_NE(0, item.getCas());

    // Should get ENOENT as we should immediately know (either from metadata
    // being resident, or by bloomfilter) that key doesn't exist.
    EXPECT_EQ(ENGINE_KEY_ENOENT, store->set(item, cookie));
}

// Add tests //////////////////////////////////////////////////////////////////

// Test successful add
TEST_P(EPStoreEvictionTest, Add) {
    auto item = make_item(vbid, "key", "value");
    EXPECT_EQ(ENGINE_SUCCESS, store->add(item, nullptr));
}

// Test add against an ejected key.
TEST_P(EPStoreEvictionTest, AddEExists) {

    // Store an item, then eject it.
    auto item = make_item(vbid, "key", "value");
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, nullptr));
    flush_vbucket_to_disk(vbid);
    evict_key(item.getVBucketId(), item.getKey());

    // Setup a lambda for how we want to call add (saves repeating the
    // same arguments for each instance below).
    auto do_add = [this]() {
        auto item = make_item(vbid, "key", "value2");
        return store->add(item, cookie);
    };

    if (GetParam() == "value_only") {
        // Should immediately return NOT_STORED (as metadata is still resident).
        EXPECT_EQ(ENGINE_NOT_STORED, do_add());

    } else if (GetParam() == "full_eviction") {
        // Should get EWOULDBLOCK as need to go to disk to get metadata.
        EXPECT_EQ(ENGINE_EWOULDBLOCK, do_add());

        // A second request should also get EWOULDBLOCK and add to the
        // existing pending BGFetch
        EXPECT_EQ(ENGINE_EWOULDBLOCK, do_add());

        // Manually run the BGFetcher task; to fetch the two outstanding
        // requests (for the same key).
        MockGlobalTask mockTask(engine->getTaskable(),
                                Priority::BgFetcherPriority);
        store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);

        EXPECT_EQ(ENGINE_NOT_STORED, do_add())
            << "Expected to fail to add on evicted item after notify_IO_complete";

    } else {
        FAIL() << "Unhandled GetParam() value:" << GetParam();
    }
}

// Check incorrect vbucket returns not-my-vbucket.
TEST_P(EPStoreEvictionTest, AddNMVB) {
    auto item = make_item(vbid + 1, "key", "value2");
    EXPECT_EQ(ENGINE_NOT_MY_VBUCKET, store->add(item, cookie));
}


// Test cases which run in both Full and Value eviction
INSTANTIATE_TEST_CASE_P(FullAndValueEviction,
                        EPStoreEvictionTest,
                        ::testing::Values("value_only", "full_eviction"),
                        [] (const ::testing::TestParamInfo<std::string>& info) {
                            return info.param;
                        });


const char EventuallyPersistentStoreTest::test_dbname[] = "ep_engine_ep_unit_tests_db";
