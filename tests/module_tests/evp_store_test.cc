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
 * Unit tests for the EPBucket class.
 *
 * Note that these test do *not* have the normal Tasks running (BGFetcher,
 * flusher etc) as we do not initialise EPEngine. This means that such tasks
 * need to be manually run. This can be very helpful as it essentially gives us
 * synchronous control of EPStore.
 */

#include "evp_store_test.h"

#include "bgfetcher.h"
#include "checkpoint.h"
#include "../mock/mock_dcp_producer.h"
#include "checkpoint_remover.h"
#include "dcp/dcpconnmap.h"
#include "dcp/flow-control-manager.h"
#include "ep_engine.h"
#include "flusher.h"
#include "makestoreddockey.h"
#include "replicationthrottle.h"
#include "tapconnmap.h"
#include "tasks.h"
#include "vbucketmemorydeletiontask.h"

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

    dcpFlowControlManager_ = new DcpFlowControlManager(*this);

    replicationThrottle = new ReplicationThrottle(configuration, stats);

    tapConfig = new TapConfig(*this);
}

void SynchronousEPEngine::setEPStore(KVBucket* store) {
    cb_assert(kvBucket == nullptr);
    kvBucket = store;
}

void SynchronousEPEngine::initializeConnmaps() {
    dcpConnMap_->initialize(DCP_CONN_NOTIFIER);
    tapConnMap->initialize(TAP_CONN_NOTIFIER);
}

MockEPStore::MockEPStore(EventuallyPersistentEngine& theEngine)
    : EPBucket(theEngine) {
    // Perform a limited set of setup (normally done by EPStore::initialize) -
    // enough such that objects which are assumed to exist are present.

    // Create the closed checkpoint removed task. Note we do _not_ schedule
    // it, unlike EPStore::initialize
    chkTask = new ClosedUnrefCheckpointRemoverTask
            (&engine, stats, theEngine.getConfiguration().getChkRemoverStime());
}

VBucketMap& MockEPStore::getVbMap() {
    return vbMap;
}

/* Mock Task class. Doesn't actually run() or snooze() - they both do nothing.
 */
class MockGlobalTask : public GlobalTask {
public:
    MockGlobalTask(Taskable& t, TaskId id)
        : GlobalTask(t, id) {}

    bool run() override { return false; }
    std::string getDescription() override { return "MockGlobalTask"; }

    void snooze(const double secs) override {}
};

void EPBucketTest::SetUp() {
    // Paranoia - kill any existing files in case they are left over
    // from a previous run.
    cb::io::rmrf(test_dbname);

    // Add dbname to config string.
    std::string config = config_string;
    if (config.size() > 0) {
        config += ";";
    }
    config += "dbname=" + std::string(test_dbname);

    engine.reset(new SynchronousEPEngine(config));
    ObjectRegistry::onSwitchThread(engine.get());

    store = new MockEPStore(*engine);
    engine->setEPStore(store);

    // Ensure that EPEngine is hold about necessary server callbacks
    // (client disconnect, bucket delete).
    engine->public_initializeEngineCallbacks();

    // Need to initialize ep_real_time and friends.
    initialize_time_functions(get_mock_server_api()->core);

    cookie = create_mock_cookie();
}

void EPBucketTest::TearDown() {
    destroy_mock_cookie(cookie);
    destroy_mock_event_callbacks();
    engine->getDcpConnMap().manageConnections();
    ObjectRegistry::onSwitchThread(nullptr);
    engine.reset();

    // Shutdown the ExecutorPool singleton (initialized when we create
    // an EPBucket object). Must happen after engine
    // has been destroyed (to allow the tasks the engine has
    // registered a chance to be unregistered).
    ExecutorPool::shutdown();
}

Item EPBucketTest::make_item(uint16_t vbid,
                             const StoredDocKey& key,
                             const std::string& value,
                             uint32_t exptime,
                             protocol_binary_datatype_t datatype) {
    uint8_t ext_meta[EXT_META_LEN] = {datatype};
    Item item(key, /*flags*/0, /*exp*/exptime, value.c_str(), value.size(),
              ext_meta, sizeof(ext_meta));
    item.setVBucketId(vbid);
    return item;
}

Item EPBucketTest::store_item(uint16_t vbid,
                              const StoredDocKey& key,
                              const std::string& value,
                              uint32_t exptime,
                              protocol_binary_datatype_t datatype) {
    auto item = make_item(vbid, key, value, exptime, datatype);
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, nullptr));

    return item;
}

void EPBucketTest::flush_vbucket_to_disk(uint16_t vbid) {
    int result;
    const auto time_limit = std::chrono::seconds(10);
    const auto deadline = std::chrono::steady_clock::now() + time_limit;

    // Need to retry as warmup may not have completed.
    bool flush_successful = false;
    do {
        result = store->flushVBucket(vbid);
        if (result != RETRY_FLUSH_VBUCKET) {
            flush_successful = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    } while (std::chrono::steady_clock::now() < deadline);

    ASSERT_TRUE(flush_successful)
        << "Hit timeout (" << time_limit.count() << " seconds) waiting for "
           "warmup to complete while flushing VBucket.";

    ASSERT_EQ(1, result) << "Failed to flush the one item we have stored.";

    /**
     * Although a flushVBucket writes the item to the underlying store,
     * the item is not marked clean until an explicit commit is called
     * If the underlying store is couchstore, a commit is called with
     * a flushVBucket but in the case of forestdb, a commit is not
     * always called, hence call an explicit commit.
     */
    uint16_t numShards = store->getVbMap().getNumShards();

    store->commit(vbid % numShards);
}

void EPBucketTest::delete_item(uint16_t vbid, const StoredDocKey& key) {
    uint64_t cas = 0;
    EXPECT_EQ(ENGINE_SUCCESS,
              store->deleteItem(key,
                                cas,
                                vbid,
                                cookie,
                                /*force*/ false,
                                /*Item*/ nullptr,
                                /*itemMeta*/ nullptr,
                                /*mutation_descr_t*/ nullptr));
}

void EPBucketTest::evict_key(uint16_t vbid, const StoredDocKey& key) {
    const char* msg;
    size_t msg_size{sizeof(msg)};
    EXPECT_EQ(ENGINE_SUCCESS, store->evictKey(key, vbid, &msg, &msg_size));
    EXPECT_STREQ("Ejected.", msg);
}

// Verify that when handling a bucket delete with open DCP
// connections, we don't deadlock when notifying the front-end
// connection.
// This is a potential issue because notify_IO_complete
// needs to lock the worker thread mutex the connection is assigned
// to, to update the event list for that connection, which the worker
// thread itself will have locked while it is running. Normally
// deadlock is avoided by using a background thread (ConnNotifier),
// which only calls notify_IO_complete and isnt' involved with any
// other mutexes, however we cannot use that task as it gets shut down
// during shutdownAllConnections.
// This test requires ThreadSanitizer or similar to validate;
// there's no guarantee we'll actually deadlock on any given run.
TEST_F(EPBucketTest, test_mb20751_deadlock_on_disconnect_delete) {

    // Create a new Dcp producer, reserving its cookie.
    get_mock_server_api()->cookie->reserve(cookie);
    dcp_producer_t producer = engine->getDcpConnMap().newProducer(
        cookie, "mb_20716r", /*notifyOnly*/false);

    // Check preconditions.
    EXPECT_TRUE(producer->isPaused());

    // 1. To check that there's no potential data-race with the
    //    concurrent connection disconnect on another thread
    //    (simulating a front-end thread).
    std::thread frontend_thread_handling_disconnect{[this](){
            // Frontend thread always runs with the cookie locked, so
            // lock here to match.
            lock_mock_cookie(cookie);
            engine->handleDisconnect(cookie);
            unlock_mock_cookie(cookie);
        }};

    // 2. Trigger a bucket deletion.
    engine->handleDeleteBucket(cookie);

    frontend_thread_handling_disconnect.join();
}

class EPStoreEvictionTest : public EPBucketTest,
                             public ::testing::WithParamInterface<std::string> {
    void SetUp() override {
        config_string += std::string{"item_eviction_policy="} + GetParam();
        EPBucketTest::SetUp();

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
              store->getKeyStats(makeStoredDocKey("key"), 0, cookie, kstats,
                                 /*wantsDeleted*/false));

    store_item(0, makeStoredDocKey("key"), "value");
    EXPECT_EQ(ENGINE_SUCCESS,
              store->getKeyStats(makeStoredDocKey("key"), 0, cookie, kstats,
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
    store_item(vbid, makeStoredDocKey("key"), "value");

    // Trigger a flush to disk.
    flush_vbucket_to_disk(vbid);

    evict_key(vbid, makeStoredDocKey("key"));

    // Setup a lambda for how we want to call getKeyStats (saves repeating the
    // same arguments for each instance below).
    auto do_getKeyStats = [this, &kstats]() {
        return store->getKeyStats(makeStoredDocKey("key"), vbid, cookie, kstats,
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
                                TaskId::MultiBGFetcherTask);
        store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);

        ASSERT_EQ(ENGINE_SUCCESS, do_getKeyStats())
            << "Expected to get key stats on evicted item after notify_IO_complete";

    } else {
        FAIL() << "Unhandled GetParam() value:" << GetParam();
    }
}

// Create then delete an item, checking we get keyStats reporting the item as
// deleted.
TEST_P(EPStoreEvictionTest, GetKeyStatsDeleted) {
    auto& kvbucket = *engine->getKVBucket();
    key_stats kstats;

    store_item(0, makeStoredDocKey("key"), "value");
    delete_item(vbid, makeStoredDocKey("key"));

    // Should get ENOENT if we don't ask for deleted items.
    EXPECT_EQ(ENGINE_KEY_ENOENT,
              kvbucket.getKeyStats(makeStoredDocKey("key"), 0, cookie,
                                   kstats, /*wantsDeleted*/false));

    // Should get success (and item flagged as deleted) if we ask for deleted
    // items.
    EXPECT_EQ(ENGINE_SUCCESS,
              kvbucket.getKeyStats(makeStoredDocKey("key"), 0, cookie,
                                  kstats, /*wantsDeleted*/true));
    EXPECT_EQ(vbucket_state_active, kstats.vb_state);
    EXPECT_TRUE(kstats.logically_deleted);
}

// Check incorrect vbucket returns not-my-vbucket.
TEST_P(EPStoreEvictionTest, GetKeyStatsNMVB) {
    auto& kvbucket = *engine->getKVBucket();
    key_stats kstats;

    EXPECT_EQ(ENGINE_NOT_MY_VBUCKET,
              kvbucket.getKeyStats(makeStoredDocKey("key"), 1, cookie,
                                   kstats, /*wantsDeleted*/false));
}

// Replace tests //////////////////////////////////////////////////////////////

// Test replace against a non-existent key.
TEST_P(EPStoreEvictionTest, ReplaceENOENT) {
    // Should start with key not existing (and hence cannot replace).
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(ENGINE_KEY_ENOENT, store->replace(item, cookie));
}

// Test replace against an ejected key.
TEST_P(EPStoreEvictionTest, ReplaceEExists) {

    // Store then eject an item.
    store_item(vbid, makeStoredDocKey("key"), "value");
    flush_vbucket_to_disk(vbid);
    evict_key(vbid, makeStoredDocKey("key"));

    // Setup a lambda for how we want to call replace (saves repeating the
    // same arguments for each instance below).
    auto do_replace = [this]() {
        auto item = make_item(vbid, makeStoredDocKey("key"), "value2");
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
                                TaskId::MultiBGFetcherTask);
        store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);

        EXPECT_EQ(ENGINE_SUCCESS, do_replace())
            << "Expected to replace on evicted item after notify_IO_complete";

    } else {
        FAIL() << "Unhandled GetParam() value:" << GetParam();
    }
}

// Create then delete an item, checking replace reports ENOENT.
TEST_P(EPStoreEvictionTest, ReplaceDeleted) {
    store_item(vbid, makeStoredDocKey("key"), "value");
    delete_item(vbid, makeStoredDocKey("key"));

    // Replace should fail.
    auto item = make_item(vbid, makeStoredDocKey("key"), "value2");
    EXPECT_EQ(ENGINE_KEY_ENOENT, store->replace(item, cookie));
}

// Check incorrect vbucket returns not-my-vbucket.
TEST_P(EPStoreEvictionTest, ReplaceNMVB) {
    auto item = make_item(vbid + 1, makeStoredDocKey("key"), "value2");
    EXPECT_EQ(ENGINE_NOT_MY_VBUCKET, store->replace(item, cookie));
}

// Check pending vbucket returns EWOULDBLOCK.
TEST_P(EPStoreEvictionTest, ReplacePendingVB) {
    store->setVBucketState(vbid, vbucket_state_pending, false);
    auto item = make_item(vbid, makeStoredDocKey("key"), "value2");
    EXPECT_EQ(ENGINE_EWOULDBLOCK, store->replace(item, cookie));
}

// Set tests //////////////////////////////////////////////////////////////////

// Test set against an ejected key.
TEST_P(EPStoreEvictionTest, SetEExists) {

    // Store an item, then eject it.
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
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
                                TaskId::MultiBGFetcherTask);
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
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    item.setCas();
    ASSERT_NE(0, item.getCas());

    // Should get ENOENT as we should immediately know (either from metadata
    // being resident, or by bloomfilter) that key doesn't exist.
    EXPECT_EQ(ENGINE_KEY_ENOENT, store->set(item, cookie));
}

// Add tests //////////////////////////////////////////////////////////////////

// Test successful add
TEST_P(EPStoreEvictionTest, Add) {
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(ENGINE_SUCCESS, store->add(item, nullptr));
}

// Test add against an ejected key.
TEST_P(EPStoreEvictionTest, AddEExists) {

    // Store an item, then eject it.
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, nullptr));
    flush_vbucket_to_disk(vbid);
    evict_key(item.getVBucketId(), item.getKey());

    // Setup a lambda for how we want to call add (saves repeating the
    // same arguments for each instance below).
    auto do_add = [this]() {
        auto item = make_item(vbid, makeStoredDocKey("key"), "value2");
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
                                TaskId::MultiBGFetcherTask);
        store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);

        EXPECT_EQ(ENGINE_NOT_STORED, do_add())
            << "Expected to fail to add on evicted item after notify_IO_complete";

    } else {
        FAIL() << "Unhandled GetParam() value:" << GetParam();
    }
}

// Check incorrect vbucket returns not-my-vbucket.
TEST_P(EPStoreEvictionTest, AddNMVB) {
    auto item = make_item(vbid + 1, makeStoredDocKey("key"), "value2");
    EXPECT_EQ(ENGINE_NOT_MY_VBUCKET, store->add(item, cookie));
}

// SetWithMeta tests //////////////////////////////////////////////////////////

// Test basic setWithMeta
TEST_P(EPStoreEvictionTest, SetWithMeta) {
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    item.setCas();
    uint64_t seqno;
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setWithMeta(item, 0, &seqno, cookie, /*force*/false,
                                 /*allowExisting*/false));
}

// Test setWithMeta with a conflict with an existing item.
TEST_P(EPStoreEvictionTest, SetWithMeta_Conflicted) {
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, nullptr));

    uint64_t seqno;
    // Attempt to set with the same rev Seqno - should get EEXISTS.
    EXPECT_EQ(ENGINE_KEY_EEXISTS,
              store->setWithMeta(item, item.getCas(), &seqno, cookie,
                                 /*force*/false, /*allowExisting*/true));
}

// Test setWithMeta replacing existing item
TEST_P(EPStoreEvictionTest, SetWithMeta_Replace) {
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, nullptr));

    // Increase revSeqno so conflict resolution doesn't fail.
    item.setRevSeqno(item.getRevSeqno() + 1);
    uint64_t seqno;
    // Should get EEXISTS if we don't force (and use wrong CAS).
    EXPECT_EQ(ENGINE_KEY_EEXISTS,
              store->setWithMeta(item, item.getCas() + 1, &seqno, cookie,
                                 /*force*/false, /*allowExisting*/true));

    // Should succeed with correct CAS, and different RevSeqno.
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setWithMeta(item, item.getCas(), &seqno, cookie,
                                 /*force*/false, /*allowExisting*/true));
}

// Test setWithMeta replacing an existing, non-resident item
TEST_P(EPStoreEvictionTest, SetWithMeta_ReplaceNonResident) {
    // Store an item, then evict it.
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, nullptr));
    flush_vbucket_to_disk(vbid);
    evict_key(item.getVBucketId(), item.getKey());

    // Increase revSeqno so conflict resolution doesn't fail.
    item.setRevSeqno(item.getRevSeqno() + 1);

    // Setup a lambda for how we want to call setWithMeta (saves repeating the
    // same arguments for each instance below).
    auto do_setWithMeta = [this, item]() mutable {
        uint64_t seqno;
        return store->setWithMeta(std::ref(item), item.getCas(), &seqno,
                           cookie, /*force*/false, /*allowExisting*/true);
    };

    if (GetParam() == "value_only") {
        // Should succeed as the metadata is still resident.
        EXPECT_EQ(ENGINE_SUCCESS, do_setWithMeta());

    } else if (GetParam() == "full_eviction") {
        // Should get EWOULDBLOCK as need to go to disk to get metadata.
        EXPECT_EQ(ENGINE_EWOULDBLOCK, do_setWithMeta());

        // A second request should also get EWOULDBLOCK and add to the
        // existing pending BGFetch
        EXPECT_EQ(ENGINE_EWOULDBLOCK, do_setWithMeta());

        // Manually run the BGFetcher task; to fetch the two outstanding
        // requests (for the same key).
        MockGlobalTask mockTask(engine->getTaskable(),
                                TaskId::MultiBGFetcherTask);
        store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);

        ASSERT_EQ(ENGINE_SUCCESS, do_setWithMeta())
            << "Expected to setWithMeta on evicted item after notify_IO_complete";

    } else {
        FAIL() << "Unhandled GetParam() value:" << GetParam();
    }
}

// Test forced setWithMeta
TEST_P(EPStoreEvictionTest, SetWithMeta_Forced) {
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    item.setCas();
    uint64_t seqno;
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setWithMeta(item, 0, &seqno, cookie, /*force*/true,
                                 /*allowExisting*/false));
}

// Test to ensure all pendingBGfetches are deleted when the
// VBucketMemoryDeletionTask is run
TEST_P(EPStoreEvictionTest, MB_21976) {
    // Store an item, then eject it.
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, nullptr));
    flush_vbucket_to_disk(vbid);
    evict_key(item.getVBucketId(), item.getKey());

    // Perform a get, which should EWOULDBLOCK
    get_options_t options = static_cast<get_options_t>(QUEUE_BG_FETCH |
                                                       HONOR_STATES |
                                                       TRACK_REFERENCE |
                                                       DELETE_TEMP |
                                                       HIDE_LOCKED_CAS |
                                                       TRACK_STATISTICS);
    GetValue gv = store->get(makeStoredDocKey("key"), vbid, cookie, options);
    EXPECT_EQ(ENGINE_EWOULDBLOCK,gv.getStatus());

    // Mark the status of the cookie so that we can see if notify is called
    lock_mock_cookie(cookie);
    struct mock_connstruct* c = (struct mock_connstruct *)cookie;
    c->status = ENGINE_E2BIG;
    unlock_mock_cookie(cookie);

    // Manually run the VBucketMemoryDeletionTask task
    RCPtr<VBucket> vb = store->getVBucket(vbid);
    VBucketMemoryDeletionTask deletionTask(*engine, vb, /*delay*/0.0);
    deletionTask.run();

    // Check the status of the cookie to see if the cookie status has changed
    // to ENGINE_NOT_MY_VBUCKET, which means the notify was sent
    EXPECT_EQ(ENGINE_NOT_MY_VBUCKET, c->status);
}

TEST_P(EPStoreEvictionTest, TouchCmdDuringBgFetch) {
    const DocKey dockey("key", DocNamespace::DefaultCollection);
    const int numTouchCmds = 2, expiryTime = (time(NULL) + 1000);

    // Store an item
    store_item(vbid, dockey, "value");

    // Trigger a flush to disk.
    flush_vbucket_to_disk(vbid);

    // Evict the item
    evict_key(vbid, dockey);

    // Issue 2 touch commands
    for (int i = 0; i < numTouchCmds; ++i) {
        GetValue gv = store->getAndUpdateTtl(dockey, vbid, cookie,
                                             (i + 1) * expiryTime);
        EXPECT_EQ(ENGINE_EWOULDBLOCK, gv.getStatus());
    }

    // Manually run the BGFetcher task; to fetch the two outstanding
    // requests (for the same key).
    MockGlobalTask mockTask(engine->getTaskable(), TaskId::MultiBGFetcherTask);
    store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);

    // Issue 2 touch commands again to mock actions post notify from bgFetch
    for (int i = 0; i < numTouchCmds; ++i) {
        GetValue gv = store->getAndUpdateTtl(dockey, vbid, cookie,
                                             (i + 1) * (expiryTime));
        EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
        delete gv.getValue();
    }
    EXPECT_EQ(numTouchCmds + 1 /* Initial item store */,
              store->getVBucket(vbid)->getHighSeqno());
}

// Test cases which run in both Full and Value eviction
INSTANTIATE_TEST_CASE_P(FullAndValueEviction,
                        EPStoreEvictionTest,
                        ::testing::Values("value_only", "full_eviction"),
                        [] (const ::testing::TestParamInfo<std::string>& info) {
                            return info.param;
                        });


const char EPBucketTest::test_dbname[] = "ep_engine_ep_unit_tests_db";
