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

#include "../mock/mock_dcp_producer.h"
#include "bgfetcher.h"
#include "checkpoint_remover.h"
#include "dcp/dcpconnmap.h"
#include "flusher.h"
#include "tests/mock/mock_global_task.h"
#include "tests/module_tests/test_helpers.h"
#include "vbucketdeletiontask.h"
#include "warmup.h"

#include <string_utilities.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

#include <thread>

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
    DcpProducer* producer = engine->getDcpConnMap().newProducer(
            cookie, "mb_20716r", /*flags*/ 0);

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
        return store->getKeyStats(makeStoredDocKey("key"),
                                  vbid,
                                  cookie,
                                  kstats,
                                  WantsDeleted::No);
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
        runBGFetcherTask();

        ASSERT_EQ(ENGINE_SUCCESS, do_getKeyStats())
            << "Expected to get key stats on evicted item after notify_IO_complete";

    } else {
        FAIL() << "Unhandled GetParam() value:" << GetParam();
    }
}

// Replace tests //////////////////////////////////////////////////////////////

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
        runBGFetcherTask();

        EXPECT_EQ(ENGINE_SUCCESS, do_replace())
            << "Expected to replace on evicted item after notify_IO_complete";

    } else {
        FAIL() << "Unhandled GetParam() value:" << GetParam();
    }
}

// Set tests //////////////////////////////////////////////////////////////////

// Test set against an ejected key.
TEST_P(EPStoreEvictionTest, SetEExists) {

    // Store an item, then eject it.
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, cookie));
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
        runBGFetcherTask();

        EXPECT_EQ(ENGINE_SUCCESS, store->set(item, cookie))
            << "Expected to set on evicted item after notify_IO_complete";

    } else {
        FAIL() << "Unhandled GetParam() value:" << GetParam();
    }
}

// Add tests //////////////////////////////////////////////////////////////////

// Test add against an ejected key.
TEST_P(EPStoreEvictionTest, AddEExists) {

    // Store an item, then eject it.
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, cookie));
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
        runBGFetcherTask();

        EXPECT_EQ(ENGINE_NOT_STORED, do_add())
            << "Expected to fail to add on evicted item after notify_IO_complete";

    } else {
        FAIL() << "Unhandled GetParam() value:" << GetParam();
    }
}

// SetWithMeta tests //////////////////////////////////////////////////////////

// Test setWithMeta replacing an existing, non-resident item
TEST_P(EPStoreEvictionTest, SetWithMeta_ReplaceNonResident) {
    // Store an item, then evict it.
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, cookie));
    flush_vbucket_to_disk(vbid);
    evict_key(item.getVBucketId(), item.getKey());

    // Increase revSeqno so conflict resolution doesn't fail.
    item.setRevSeqno(item.getRevSeqno() + 1);

    // Setup a lambda for how we want to call setWithMeta (saves repeating the
    // same arguments for each instance below).
    auto do_setWithMeta = [this, item]() mutable {
        uint64_t seqno;
        return store->setWithMeta(std::ref(item),
                                  item.getCas(),
                                  &seqno,
                                  cookie,
                                  {vbucket_state_active},
                                  CheckConflicts::No,
                                  /*allowExisting*/ true);
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
        runBGFetcherTask();

        ASSERT_EQ(ENGINE_SUCCESS, do_setWithMeta())
            << "Expected to setWithMeta on evicted item after notify_IO_complete";

    } else {
        FAIL() << "Unhandled GetParam() value:" << GetParam();
    }
}

// Deleted-with-Value Tests ///////////////////////////////////////////////////

TEST_P(EPStoreEvictionTest, DeletedValue) {
    // Create a deleted item which has a value, then evict it.
    auto key = makeStoredDocKey("key");
    auto item = make_item(vbid, key, "deleted value");
    item.setDeleted();
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, cookie));

    // The act of flushing will remove the item from the HashTable.
    flush_vbucket_to_disk(vbid);
    EXPECT_EQ(0, store->getVBucket(vbid)->getNumItems());

    // Setup a lambda for how we want to call get (saves repeating the
    // same arguments for each instance below).
    auto do_get = [this, &key]() {
        auto options = get_options_t(GET_DELETED_VALUE | QUEUE_BG_FETCH);
        return store->get(key, vbid, cookie, options);
    };

    // Try to get the Deleted-with-value key. This should return EWOULDBLOCK
    // (as the Deleted value is not resident).
    EXPECT_EQ(ENGINE_EWOULDBLOCK, do_get().getStatus())
            << "Expected to need to go to disk to get Deleted-with-value key";

    // Try a second time - this should detect the already-created temp
    // item, and re-schedule the bgfetch.
    EXPECT_EQ(ENGINE_EWOULDBLOCK, do_get().getStatus())
            << "Expected to need to go to disk to get Deleted-with-value key "
               "(try 2)";

    // Manually run the BGFetcher task; to fetch the two outstanding
    // requests (for the same key).
    MockGlobalTask mockTask(engine->getTaskable(), TaskId::MultiBGFetcherTask);
    store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);

    auto result = do_get();
    EXPECT_EQ(ENGINE_SUCCESS, result.getStatus())
            << "Expected to get Deleted-with-value for evicted key after "
               "notify_IO_complete";
    EXPECT_EQ("deleted value", result.item->getValue()->to_s());
}

// Test to ensure all pendingBGfetches are deleted when the
// VBucketMemoryDeletionTask is run
TEST_P(EPStoreEvictionTest, MB_21976) {
    // Store an item, then eject it.
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, cookie));
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
    EXPECT_EQ(ENGINE_EWOULDBLOCK, gv.getStatus());

    // Mark the status of the cookie so that we can see if notify is called
    lock_mock_cookie(cookie);
    struct mock_connstruct* c = (struct mock_connstruct *)cookie;
    c->status = ENGINE_E2BIG;
    unlock_mock_cookie(cookie);

    const void* deleteCookie = create_mock_cookie();

    // lock the cookie, waitfor will release and enter the internal cond-var
    lock_mock_cookie(deleteCookie);
    store->deleteVBucket(vbid, deleteCookie);
    waitfor_mock_cookie(deleteCookie);
    unlock_mock_cookie(deleteCookie);

    // Check the status of the cookie to see if the cookie status has changed
    // to ENGINE_NOT_MY_VBUCKET, which means the notify was sent
    EXPECT_EQ(ENGINE_NOT_MY_VBUCKET, c->status);

    destroy_mock_cookie(deleteCookie);
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
    runBGFetcherTask();

    // Issue 2 touch commands again to mock actions post notify from bgFetch
    for (int i = 0; i < numTouchCmds; ++i) {
        GetValue gv = store->getAndUpdateTtl(dockey, vbid, cookie,
                                             (i + 1) * (expiryTime));
        EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    }
    EXPECT_EQ(numTouchCmds + 1 /* Initial item store */,
              store->getVBucket(vbid)->getHighSeqno());
}

TEST_P(EPStoreEvictionTest, checkIfResidentAfterBgFetch) {
    const DocKey dockey("key", DocNamespace::DefaultCollection);

    //Store an item
    store_item(vbid, dockey, "value");

    //Trigger a flush to disk
    flush_vbucket_to_disk(vbid);

    //Now, delete the item
    uint64_t cas = 0;
    mutation_descr_t mutation_descr;
    ASSERT_EQ(ENGINE_SUCCESS,
              store->deleteItem(dockey,
                                cas,
                                vbid,
                                /*cookie*/ cookie,
                                /*itemMeta*/ nullptr,
                                mutation_descr));

    flush_vbucket_to_disk(vbid);

    get_options_t options = static_cast<get_options_t>(QUEUE_BG_FETCH |
                                                       HONOR_STATES   |
                                                       TRACK_REFERENCE |
                                                       DELETE_TEMP |
                                                       HIDE_LOCKED_CAS |
                                                       TRACK_STATISTICS |
                                                       GET_DELETED_VALUE);

    GetValue gv = store->get(makeStoredDocKey("key"), vbid, cookie, options);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, gv.getStatus());

    runBGFetcherTask();

    // The Get should succeed in this case
    gv = store->get(makeStoredDocKey("key"), vbid, cookie, options);
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());

    VBucketPtr vb = store->getVBucket(vbid);

    auto hbl = vb->ht.getLockedBucket(dockey);
    StoredValue* v = vb->ht.unlocked_find(dockey,
                                          hbl.getBucketNum(),
                                          WantsDeleted::Yes,
                                          TrackReference::No);

    EXPECT_TRUE(v->isResident());
}

TEST_P(EPStoreEvictionTest, xattrExpiryOnFullyEvictedItem) {
    if (GetParam() == "value_only") {
        return;
    }

    cb::xattr::Blob builder;

    //Add a few values
    builder.set("_meta", "{\"rev\":10}");
    builder.set("foo", "{\"blob\":true}");

    auto blob = builder.finalize();
    auto blob_data = to_string(blob);
    auto itm = store_item(vbid,
                          makeStoredDocKey("key"),
                          blob_data,
                          0,
                          {cb::engine_errc::success},
                          (PROTOCOL_BINARY_DATATYPE_JSON |
                           PROTOCOL_BINARY_DATATYPE_XATTR));

    GetValue gv = store->getAndUpdateTtl(makeStoredDocKey("key"), vbid, cookie,
                                         time(NULL) + 120);
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    std::unique_ptr<Item> get_itm(std::move(gv.item));

    flush_vbucket_to_disk(vbid);
    evict_key(vbid, makeStoredDocKey("key"));
    store->deleteExpiredItem(itm, time(NULL) + 121, ExpireBy::Compactor);

    get_options_t options = static_cast<get_options_t>(QUEUE_BG_FETCH |
                                                       HONOR_STATES |
                                                       TRACK_REFERENCE |
                                                       DELETE_TEMP |
                                                       HIDE_LOCKED_CAS |
                                                       TRACK_STATISTICS |
                                                       GET_DELETED_VALUE);

    gv = store->get(makeStoredDocKey("key"), vbid, cookie, options);
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());

    get_itm = std::move(gv.item);
    auto get_data = const_cast<char*>(get_itm->getData());
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_XATTR, get_itm->getDataType())
              << "Unexpected Datatype";

    cb::char_buffer value_buf{get_data, get_itm->getNBytes()};
    cb::xattr::Blob new_blob(value_buf);

    const std::string& rev_str{"{\"rev\":10}"};
    const std::string& meta_str = to_string(new_blob.get("_meta"));

    EXPECT_EQ(rev_str, meta_str) << "Unexpected system xattrs";
    EXPECT_TRUE(new_blob.get("foo").empty())
            << "The foo attribute should be gone";
}

/**
 * Verify that when getIf is used it only fetches the metdata from disk for
 * the filter, and not the complete document.
 * Negative case where filter doesn't match.
**/
TEST_P(EPStoreEvictionTest, getIfOnlyFetchesMetaForFilterNegative) {
    // Store an item, then eject it.
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, cookie));
    flush_vbucket_to_disk(vbid);
    evict_key(item.getVBucketId(), item.getKey());

    // Setup a lambda for how we want to call get_if() - filter always returns
    // false.
    auto do_getIf = [this]() {
        return engine->get_if(cookie,
                              makeStoredDocKey("key"),
                              vbid,
                              [](const item_info& info) { return false; });
    };

    auto& stats = engine->getEpStats();
    ASSERT_EQ(0, stats.bg_fetched);
    ASSERT_EQ(0, stats.bg_meta_fetched);

    if (GetParam() == "value_only") {
        // Value-only should reject (via filter) on first attempt (no need to
        // go to disk).
        auto res = do_getIf();
        EXPECT_EQ(cb::engine_errc::success, res.first);
        EXPECT_EQ(nullptr, res.second.get());

    } else if (GetParam() == "full_eviction") {

        // First attempt should return EWOULDBLOCK (as the item has been evicted
        // and we need to fetch).
        auto res = do_getIf();
        EXPECT_EQ(cb::engine_errc::would_block, res.first);

        // Manually run the BGFetcher task; to fetch the outstanding meta fetch.
        // requests (for the same key).
        runBGFetcherTask();
        EXPECT_EQ(0, stats.bg_fetched);
        EXPECT_EQ(1, stats.bg_meta_fetched);

        // Second attempt - should succeed this time, without a match.
        res = do_getIf();
        EXPECT_EQ(cb::engine_errc::success, res.first);
        EXPECT_EQ(nullptr, res.second.get());

    } else {
        FAIL() << "Unhandled GetParam() value:" << GetParam();
    }
}

/**
 * Verify that when getIf is used it only fetches the metdata from disk for
 * the filter, and not the complete document.
 * Positive case where filter does match.
**/
TEST_P(EPStoreEvictionTest, getIfOnlyFetchesMetaForFilterPositive) {
    // Store an item, then eject it.
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, cookie));
    flush_vbucket_to_disk(vbid);
    evict_key(item.getVBucketId(), item.getKey());

    // Setup a lambda for how we want to call get_if() - filter always returns
    // true.
    auto do_getIf = [this]() {
        return engine->get_if(cookie,
                              makeStoredDocKey("key"),
                              vbid,
                              [](const item_info& info) { return true; });
    };

    auto& stats = engine->getEpStats();
    ASSERT_EQ(0, stats.bg_fetched);
    ASSERT_EQ(0, stats.bg_meta_fetched);

    if (GetParam() == "value_only") {
        // Value-only should match filter on first attempt, and then return
        // bgfetch to get the body.
        auto res = do_getIf();
        EXPECT_EQ(cb::engine_errc::would_block, res.first);
        EXPECT_EQ(nullptr, res.second.get());

        // Manually run the BGFetcher task; to fetch the outstanding body.
        runBGFetcherTask();
        EXPECT_EQ(1, stats.bg_fetched);
        ASSERT_EQ(0, stats.bg_meta_fetched);

        res = do_getIf();
        EXPECT_EQ(cb::engine_errc::success, res.first);
        ASSERT_NE(nullptr, res.second.get());
        Item* epItem = static_cast<Item*>(res.second.get());
        ASSERT_NE(nullptr, epItem->getValue().get().get());
        EXPECT_EQ("value", epItem->getValue()->to_s());

    } else if (GetParam() == "full_eviction") {

        // First attempt should return would_block (as the item has been evicted
        // and we need to fetch).
        auto res = do_getIf();
        EXPECT_EQ(cb::engine_errc::would_block, res.first);

        // Manually run the BGFetcher task; to fetch the outstanding meta fetch.
        runBGFetcherTask();
        EXPECT_EQ(0, stats.bg_fetched);
        EXPECT_EQ(1, stats.bg_meta_fetched);

        // Second attempt - should get as far as applying the filter, but
        // will need to go to disk a second time for the body.
        res = do_getIf();
        EXPECT_EQ(cb::engine_errc::would_block, res.first);

        // Manually run the BGFetcher task; this time to fetch the body.
        runBGFetcherTask();
        EXPECT_EQ(1, stats.bg_fetched);
        EXPECT_EQ(1, stats.bg_meta_fetched);

        // Third call to getIf - should have result now.
        res = do_getIf();
        EXPECT_EQ(cb::engine_errc::success, res.first);
        ASSERT_NE(nullptr, res.second.get());
        Item* epItem = static_cast<Item*>(res.second.get());
        ASSERT_NE(nullptr, epItem->getValue().get().get());
        EXPECT_EQ("value", epItem->getValue()->to_s());

    } else {
        FAIL() << "Unhandled GetParam() value:" << GetParam();
    }
}

/**
 * Verify that a get of a deleted item with no value successfully
 * returns an item
 */
TEST_P(EPStoreEvictionTest, getDeletedItemWithNoValue) {
    const DocKey dockey("key", DocNamespace::DefaultCollection);

    // Store an item
    store_item(vbid, dockey, "value");

    // Trigger a flush to disk
    flush_vbucket_to_disk(vbid);

    uint64_t cas = 0;
    mutation_descr_t mutation_descr;
    ASSERT_EQ(ENGINE_SUCCESS,
              store->deleteItem(dockey,
                                cas,
                                vbid,
                                /*cookie*/ cookie,
                                /*itemMeta*/ nullptr,
                                mutation_descr));

    // Ensure that the delete has been persisted
    flush_vbucket_to_disk(vbid);

    get_options_t options = static_cast<get_options_t>(QUEUE_BG_FETCH |
                                                       HONOR_STATES |
                                                       TRACK_REFERENCE |
                                                       DELETE_TEMP |
                                                       HIDE_LOCKED_CAS |
                                                       TRACK_STATISTICS |
                                                       GET_DELETED_VALUE);

    GetValue gv = store->get(makeStoredDocKey("key"), vbid, cookie, options);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, gv.getStatus());

    runBGFetcherTask();

    // The Get should succeed in this case
    gv = store->get(makeStoredDocKey("key"), vbid, cookie, options);
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());

    // Ensure that the item is deleted and the value length is zero
    Item* itm = gv.item.get();
    value_t value = itm->getValue();
    EXPECT_EQ(0, value->valueSize());
    EXPECT_TRUE(itm->isDeleted());
}

/**
 * Verify that a get of a deleted item with value successfully
 * returns an item
 */
TEST_P(EPStoreEvictionTest, getDeletedItemWithValue) {
    const DocKey dockey("key", DocNamespace::DefaultCollection);

    // Store an item
    store_item(vbid, dockey, "value");

    // Trigger a flush to disk
    flush_vbucket_to_disk(vbid);

    auto item = make_item(vbid, dockey, "deletedvalue");
    item.setDeleted();
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, cookie));
    flush_vbucket_to_disk(vbid);

    //Perform a get
    get_options_t options = static_cast<get_options_t>(QUEUE_BG_FETCH |
                                                       HONOR_STATES |
                                                       TRACK_REFERENCE |
                                                       DELETE_TEMP |
                                                       HIDE_LOCKED_CAS |
                                                       TRACK_STATISTICS |
                                                       GET_DELETED_VALUE);

    GetValue gv = store->get(dockey, vbid, cookie, options);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, gv.getStatus());

    runBGFetcherTask();

    // The Get should succeed in this case
    gv = store->get(dockey, vbid, cookie, options);
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());

    // Ensure that the item is deleted and the value matches
    Item* itm = gv.item.get();
    EXPECT_EQ("deletedvalue", itm->getValue()->to_s());
    EXPECT_TRUE(itm->isDeleted());
}

//Test to verify the behavior in the condition where
//memOverhead is greater than the bucket quota
TEST_P(EPStoreEvictionTest, memOverheadMemoryCondition) {
    //Limit the bucket quota to 200K
    Configuration& config = engine->getConfiguration();
    config.setMaxSize(204800);
    config.setMemHighWat(0.8 * 204800);
    config.setMemLowWat(0.6 * 204800);

    //Ensure the memOverhead is greater than the bucket quota
    auto& stats = engine->getEpStats();
    stats.memOverhead->store(config.getMaxSize() + 1);

    // Fill bucket until we hit ENOMEM - note storing via external
    // API (epstore) so we trigger the memoryCondition() code in the event of
    // ENGINE_ENOMEM.
    size_t count = 0;
    const std::string value(512, 'x'); // 512B value to use for documents.
    ENGINE_ERROR_CODE result;
    auto dummyCookie = std::make_unique<mock_connstruct>();
    for (result = ENGINE_SUCCESS; result == ENGINE_SUCCESS; count++) {
        auto item = make_item(vbid,
                              makeStoredDocKey("key_" + std::to_string(count)),
                              value);
        uint64_t cas;
        result = engine->store(dummyCookie.get(), &item, cas, OPERATION_SET);
    }

    if (GetParam() == "value_only") {
        ASSERT_EQ(ENGINE_ENOMEM, result);
    } else {
        ASSERT_EQ(ENGINE_TMPFAIL, result);
    }
}

class EPStoreEvictionBloomOnOffTest
        : public EPBucketTest,
          public ::testing::WithParamInterface<
                  ::testing::tuple<std::string, bool>> {
public:
    void SetUp() override {
        config_string += std::string{"item_eviction_policy="} +
                         ::testing::get<0>(GetParam());

        if (::testing::get<1>(GetParam())) {
            config_string += ";bfilter_enabled=true";
        } else {
            config_string += ";bfilter_enabled=false";
        }

        EPBucketTest::SetUp();

        // Have all the objects, activate vBucket zero so we can store data.
        store->setVBucketState(vbid, vbucket_state_active, false);
    }
};

TEST_P(EPStoreEvictionBloomOnOffTest, store_if_throws) {
    // You can't keep returning GetItemInfo
    cb::StoreIfPredicate predicate = [](
            const boost::optional<item_info>& existing,
            cb::vbucket_info vb) -> cb::StoreIfStatus {
        return cb::StoreIfStatus::GetItemInfo;
    };

    auto key = makeStoredDocKey("key");
    auto item = make_item(vbid, key, "value2");

    store_item(vbid, key, "value1");
    flush_vbucket_to_disk(vbid);
    evict_key(vbid, key);

    if (::testing::get<0>(GetParam()) == "full_eviction") {
        EXPECT_NO_THROW(engine->store_if(
                cookie, item, 0 /*cas*/, OPERATION_SET, predicate));
        runBGFetcherTask();
    }

    // If the itemInfo exists, you can't ask for it again - so expect throw
    EXPECT_THROW(
            engine->store_if(cookie, item, 0 /*cas*/, OPERATION_SET, predicate),
            std::logic_error);
}

TEST_P(EPStoreEvictionBloomOnOffTest, store_if) {
    struct TestData {
        StoredDocKey key;
        cb::StoreIfPredicate predicate;
        cb::engine_errc expectedVEStatus;
        cb::engine_errc expectedFEStatus;
        cb::engine_errc actualStatus;
    };

    std::vector<TestData> testData;
    cb::StoreIfPredicate predicate1 = [](
            const boost::optional<item_info>& existing,
            cb::vbucket_info vb) -> cb::StoreIfStatus {
        return cb::StoreIfStatus::Continue;
    };
    cb::StoreIfPredicate predicate2 = [](
            const boost::optional<item_info>& existing,
            cb::vbucket_info vb) -> cb::StoreIfStatus {
        return cb::StoreIfStatus::Fail;
    };
    cb::StoreIfPredicate predicate3 = [](
            const boost::optional<item_info>& existing,
            cb::vbucket_info vb) -> cb::StoreIfStatus {
        if (existing.is_initialized()) {
            return cb::StoreIfStatus::Continue;
        }
        return cb::StoreIfStatus::GetItemInfo;
    };
    cb::StoreIfPredicate predicate4 = [](
            const boost::optional<item_info>& existing,
            cb::vbucket_info vb) -> cb::StoreIfStatus {
        if (existing.is_initialized()) {
            return cb::StoreIfStatus::Fail;
        }
        return cb::StoreIfStatus::GetItemInfo;
    };

    testData.push_back({makeStoredDocKey("key1"),
                        predicate1,
                        cb::engine_errc::success,
                        cb::engine_errc::success});
    testData.push_back({makeStoredDocKey("key2"),
                        predicate2,
                        cb::engine_errc::predicate_failed,
                        cb::engine_errc::predicate_failed});
    testData.push_back({makeStoredDocKey("key3"),
                        predicate3,
                        cb::engine_errc::success,
                        cb::engine_errc::would_block});
    testData.push_back({makeStoredDocKey("key4"),
                        predicate4,
                        cb::engine_errc::predicate_failed,
                        cb::engine_errc::would_block});

    for (auto& test : testData) {
        store_item(vbid, test.key, "value");
        flush_vbucket_to_disk(vbid);
        evict_key(vbid, test.key);
        auto item = make_item(vbid, test.key, "new_value");
        test.actualStatus = engine->store_if(cookie,
                                             item,
                                             0 /*cas*/,
                                             OPERATION_SET,
                                             test.predicate)
                                    .status;
        if (test.actualStatus == cb::engine_errc::success) {
            flush_vbucket_to_disk(vbid);
        }
    }

    for (size_t i = 0; i < testData.size(); i++) {
        if (::testing::get<0>(GetParam()) == "value_only") {
            EXPECT_EQ(testData[i].expectedVEStatus, testData[i].actualStatus)
                    << "Failed value_only iteration " + std::to_string(i);
        } else if (::testing::get<0>(GetParam()) == "full_eviction") {
            EXPECT_EQ(testData[i].expectedFEStatus, testData[i].actualStatus)
                    << "Failed full_eviction iteration " + std::to_string(i);
        } else {
            FAIL() << "Unhandled GetParam() value:"
                   << ::testing::get<0>(GetParam());
        }
    }

    if (::testing::get<0>(GetParam()) == "full_eviction") {
        runBGFetcherTask();
        for (size_t i = 0; i < testData.size(); i++) {
            if (testData[i].actualStatus == cb::engine_errc::would_block) {
                auto item = make_item(vbid, testData[i].key, "new_value");
                auto status = engine->store_if(cookie,
                                               item,
                                               0 /*cas*/,
                                               OPERATION_SET,
                                               testData[i].predicate);
                // The second run should result the same as VE
                EXPECT_EQ(testData[i].expectedVEStatus, status.status);
            }
        }
    }
}

TEST_P(EPStoreEvictionBloomOnOffTest, store_if_fe_interleave) {
    if (::testing::get<0>(GetParam()) != "full_eviction") {
        return;
    }

    cb::StoreIfPredicate predicate = [](
            const boost::optional<item_info>& existing,
            cb::vbucket_info vb) -> cb::StoreIfStatus {
        if (existing.is_initialized()) {
            return cb::StoreIfStatus::Continue;
        }
        return cb::StoreIfStatus::GetItemInfo;
    };

    auto key = makeStoredDocKey("key");
    auto item = make_item(vbid, key, "value2");

    store_item(vbid, key, "value1");
    flush_vbucket_to_disk(vbid);
    evict_key(vbid, key);

    EXPECT_EQ(
            cb::engine_errc::would_block,
            engine->store_if(cookie, item, 0 /*cas*/, OPERATION_SET, predicate)
                    .status);

    // expect another store to the same key to be told the same, even though the
    // first store has populated the store with a temp item
    EXPECT_EQ(
            cb::engine_errc::would_block,
            engine->store_if(cookie, item, 0 /*cas*/, OPERATION_SET, predicate)
                    .status);

    runBGFetcherTask();
    EXPECT_EQ(
            cb::engine_errc::success,
            engine->store_if(cookie, item, 0 /*cas*/, OPERATION_SET, predicate)
                    .status);
}

struct PrintToStringCombinedName {
    std::string operator()(
            const ::testing::TestParamInfo<::testing::tuple<std::string, bool>>&
                    info) const {
        std::string bfilter = "_bfilter_enabled";
        if (!::testing::get<1>(info.param)) {
            bfilter = "_bfilter_disabled";
        }
        return ::testing::get<0>(info.param) + bfilter;
    }
};

// Test cases which run in both Full and Value eviction, and with bloomfilter
// on and off.
INSTANTIATE_TEST_CASE_P(FullAndValueEvictionBloomOnOff,
                        EPStoreEvictionBloomOnOffTest,
                        ::testing::Combine(::testing::Values("value_only",
                                                             "full_eviction"),
                                           ::testing::Bool()),
                        PrintToStringCombinedName());

// Test cases which run in both Full and Value eviction
INSTANTIATE_TEST_CASE_P(FullAndValueEviction,
                        EPStoreEvictionTest,
                        ::testing::Values("value_only", "full_eviction"),
                        [] (const ::testing::TestParamInfo<std::string>& info) {
                            return info.param;
                        });
