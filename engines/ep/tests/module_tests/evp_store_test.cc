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
#include "tapconnmap.h"
#include "tests/mock/mock_global_task.h"
#include "tests/module_tests/test_helpers.h"
#include "vbucketdeletiontask.h"
#include "warmup.h"

#include <string_utilities.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

#include <thread>

void EPBucketTest::runBGFetcherTask() {
    MockGlobalTask mockTask(engine->getTaskable(),
                            TaskId::MultiBGFetcherTask);
    store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);
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
            cookie, "mb_20716r", /*flags*/ 0, {/*no json*/});

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
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, nullptr));
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
    ASSERT_EQ(ENGINE_SUCCESS,
              store->deleteItem(dockey, cas, vbid,
                                /*cookie*/ cookie,
                                /*itemMeta*/ nullptr,
                                /*mutation_descr_t*/ nullptr));

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

    // Run the BGFetcher task
    MockGlobalTask mockTask(engine->getTaskable(), TaskId::MultiBGFetcherTask);
    store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);

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
    builder.set(to_const_byte_buffer("_meta"),
                to_const_byte_buffer("{\"rev\":10}"));
    builder.set(to_const_byte_buffer("foo"),
                to_const_byte_buffer("{\"blob\":true}"));

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

    cb::byte_buffer value_buf{reinterpret_cast<uint8_t*>(get_data),
                              get_itm->getNBytes()};
    cb::xattr::Blob new_blob(value_buf);

    const std::string& rev_str{"{\"rev\":10}"};
    const std::string& meta_str = to_string(new_blob.get(to_const_byte_buffer("_meta")));

    EXPECT_EQ(rev_str, meta_str) << "Unexpected system xattrs";
    EXPECT_TRUE(new_blob.get(to_const_byte_buffer("foo")).empty()) <<
                "The foo attribute should be gone";
}

/**
 * Verify that when getIf is used it only fetches the metdata from disk for
 * the filter, and not the complete document.
 * Negative case where filter doesn't match.
**/
TEST_P(EPStoreEvictionTest, getIfOnlyFetchesMetaForFilterNegative) {
    // Store an item, then eject it.
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, nullptr));
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
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, nullptr));
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
        ASSERT_NE(nullptr, epItem->getValue().get());
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
        ASSERT_NE(nullptr, epItem->getValue().get());
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
    ASSERT_EQ(ENGINE_SUCCESS,
              store->deleteItem(dockey, cas, vbid,
                                /*cookie*/ cookie,
                                /*itemMeta*/ nullptr,
                                /*mutation_descr_t*/ nullptr));

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

    // Run the BGFetcher task
    MockGlobalTask mockTask(engine->getTaskable(), TaskId::MultiBGFetcherTask);
    store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);

    // The Get should succeed in this case
    gv = store->get(makeStoredDocKey("key"), vbid, cookie, options);
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());

    // Ensure that the item is deleted and the value length is zero
    Item* itm = gv.item.get();
    value_t value = itm->getValue();
    EXPECT_EQ(0, value->vlength());
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

    auto item = make_item(vbid, makeStoredDocKey("key"),
                          "deletedvalue");
    item.setDeleted();
    EXPECT_EQ(ENGINE_SUCCESS, store->set(item, nullptr));
    flush_vbucket_to_disk(vbid);

    //Perform a get
    get_options_t options = static_cast<get_options_t>(QUEUE_BG_FETCH |
                                                       HONOR_STATES |
                                                       TRACK_REFERENCE |
                                                       DELETE_TEMP |
                                                       HIDE_LOCKED_CAS |
                                                       TRACK_STATISTICS |
                                                       GET_DELETED_VALUE);

    GetValue gv = store->get(makeStoredDocKey("key"), vbid, cookie, options);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, gv.getStatus());

    // Run the BGFetcher task
    MockGlobalTask mockTask(engine->getTaskable(), TaskId::MultiBGFetcherTask);
    store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);

    // The Get should succeed in this case
    gv = store->get(makeStoredDocKey("key"), vbid, cookie, options);
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

    //Set warmup complete to ensure that we don't hit
    //degraded mode
    engine->getKVBucket()->getWarmup()->setComplete();
    // Fill bucket until we hit ENOMEM - note storing via external
    // API (epstore) so we trigger the memoryCondition() code in the event of
    // ENGINE_ENOMEM.
    size_t count = 0;
    const std::string value(512, 'x'); // 512B value to use for documents.
    ENGINE_ERROR_CODE result;
    for (result = ENGINE_SUCCESS; result == ENGINE_SUCCESS; count++) {
        auto item = make_item(vbid,
                              makeStoredDocKey("key_" + std::to_string(count)),
                              value);
        uint64_t cas;
        result = engine->store(nullptr, &item, &cas, OPERATION_SET);
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

TEST_P(EPStoreEvictionBloomOnOffTest, store_if) {
    engine->getKVBucket()->getWarmup()->setComplete();

    // Store 3 items.
    // key1 will be stored with a always true predicate
    // key2 will be stored with a predicate which is expected to fail
    // key3 will be store with an empty predicate
    auto key1 = makeStoredDocKey("key1");
    auto key2 = makeStoredDocKey("key2");
    auto key3 = makeStoredDocKey("key3");

    store_item(vbid, key1, "value1");
    store_item(vbid, key2, "value1");
    store_item(vbid, key3, "value1");
    flush_vbucket_to_disk(vbid, 3);

    // Evict the keys so the FE mode will bgFetch and not be optimised away by
    // the bloomfilter
    evict_key(vbid, key1);
    evict_key(vbid, key2);
    evict_key(vbid, key3);

    uint64_t cas = 0;
    auto predicate1 = [](const item_info& existing) { return true; };

    auto predicate2 = [](const item_info& existing) {
        return existing.datatype == PROTOCOL_BINARY_RAW_BYTES;
    };

    // Now store_if and check the return codes
    auto item1 = make_item(vbid, key1, "value2");
    auto result1 =
            engine->store_if(nullptr, item1, cas, OPERATION_SET, predicate1);

    auto item2 = make_item(vbid, key2, "value2");
    auto result2 =
            engine->store_if(nullptr, item2, cas, OPERATION_SET, predicate2);

    auto item3 = make_item(vbid, key3, "value2");
    auto result3 = engine->store_if(nullptr, item3, cas, OPERATION_SET, {});

    if (::testing::get<0>(GetParam()) == "value_only") {
        EXPECT_EQ(cb::engine_errc::success, result1.status)
                << "Expected store_if(key1) to succeed in VE mode";
        EXPECT_EQ(cb::engine_errc::predicate_failed, result2.status)
                << "Expected store_if(key2) to return predicate_failed in VE "
                   "mode";
        EXPECT_EQ(cb::engine_errc::success, result3.status)
                << "Expected store_if(key3) to succeed in VE mode";

    } else if (::testing::get<0>(GetParam()) == "full_eviction") {
        // The stores have to go to disk so that the predicate can be ran
        ASSERT_EQ(cb::engine_errc::would_block, result1.status)
                << "Expected store_if(key1) to block in FE mode";

        ASSERT_EQ(cb::engine_errc::would_block, result2.status)
                << "Expected store_if(key2) to block in FE mode";

        EXPECT_EQ(cb::engine_errc::success, result3.status)
                << "Expected store_if(key3) to succeed in FE mode because "
                   "there's no predicate";

        runBGFetcherTask();

        // key1/key2 will need store_if running again.

        result1 = engine->store_if(
                nullptr, item1, cas, OPERATION_SET, predicate1);

        EXPECT_EQ(cb::engine_errc::success, result1.status)
                << "Expected store_if to succeed";

        result2 = engine->store_if(
                nullptr, item2, cas, OPERATION_SET, predicate2);

        EXPECT_EQ(cb::engine_errc::predicate_failed, result2.status)
                << "Expected store_if to return predicate_failed in FE mode";

    } else {
        FAIL() << "Unhandled GetParam() value:"
               << ::testing::get<0>(GetParam());
    }
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

// Test cases which run in both Full and Value eviction
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
