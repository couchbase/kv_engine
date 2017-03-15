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
        MockGlobalTask mockTask(engine->getTaskable(),
                                TaskId::MultiBGFetcherTask);
        store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);

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
        MockGlobalTask mockTask(engine->getTaskable(),
                                TaskId::MultiBGFetcherTask);
        store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);

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
        MockGlobalTask mockTask(engine->getTaskable(),
                                TaskId::MultiBGFetcherTask);
        store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);

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
        MockGlobalTask mockTask(engine->getTaskable(),
                                TaskId::MultiBGFetcherTask);
        store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);

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
    std::unique_ptr<Item> get_itm(gv.getValue());

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

    get_itm.reset(gv.getValue());
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

// Test cases which run in both Full and Value eviction
INSTANTIATE_TEST_CASE_P(FullAndValueEviction,
                        EPStoreEvictionTest,
                        ::testing::Values("value_only", "full_eviction"),
                        [] (const ::testing::TestParamInfo<std::string>& info) {
                            return info.param;
                        });
