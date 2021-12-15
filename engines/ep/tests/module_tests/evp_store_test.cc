/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
#include "checkpoint_manager.h"
#include "checkpoint_remover.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/dcpconnmap.h"
#include "ep_bucket.h"
#include "ep_time.h"
#include "flusher.h"
#include "item_eviction.h"
#include "kvstore/kvstore.h"
#include "tasks.h"
#include "test_manifest.h"
#include "tests/mock/mock_ep_bucket.h"
#include "tests/mock/mock_global_task.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/test_helpers.h"
#include "vbucket_bgfetch_item.h"
#include "vbucketdeletiontask.h"
#include "warmup.h"

#include <memcached/server_cookie_iface.h>
#include <programs/engine_testapp/mock_cookie.h>
#include <programs/engine_testapp/mock_server.h>
#include <string_utilities.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

#include <thread>
#include <utility>

void EPBucketTest::SetUp() {
    STParameterizedBucketTest::SetUp();

    // Have all the objects, activate vBucket zero so we can store data.
    store->setVBucketState(vbid, vbucket_state_active);
}

EPBucket& EPBucketTest::getEPBucket() {
    return dynamic_cast<EPBucket&>(*store);
}

void EPBucketFullEvictionNoBloomFilterTest::SetUp() {
    config_string += "bfilter_enabled=false";
    EPBucketFullEvictionTest::SetUp();
}

void EPBucketBloomFilterParameterizedTest::SetUp() {
    auto bucketType = std::get<0>(GetParam());
    config_string += generateBucketTypeConfig(bucketType);

    config_string +=
            std::string{";item_eviction_policy="} + std::get<1>(GetParam());

    if (bloomFiltersEnabled()) {
        config_string += ";bfilter_enabled=true";
    } else {
        config_string += ";bfilter_enabled=false";
    }

    SingleThreadedKVBucketTest::SetUp();

    // Have all the objects, activate vBucket zero so we can store data.
    store->setVBucketState(vbid, vbucket_state_active);
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
TEST_P(EPBucketTest, test_mb20751_deadlock_on_disconnect_delete) {
    // Create a new Dcp producer, reserving its cookie.
    get_mock_server_api()->cookie->reserve(*cookie);
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
            auto* mockCookie = cookie_to_mock_cookie(cookie);
            Expects(cookie);
            mockCookie->lock();
            engine->handleDisconnect(cookie);
            mockCookie->unlock();
        }};

    // 2. Trigger a bucket deletion.
    engine->initiate_shutdown();

    frontend_thread_handling_disconnect.join();
}

/**
 * MB-30015: Test to check the config parameter "retain_erroneous_tombstones"
 */
TEST_P(EPBucketTest, testRetainErroneousTombstonesConfig) {
    Configuration& config = engine->getConfiguration();

    config.setRetainErroneousTombstones(true);
    auto& store = getEPBucket();
    EXPECT_TRUE(store.isRetainErroneousTombstones());

    config.setRetainErroneousTombstones(false);
    EXPECT_FALSE(store.isRetainErroneousTombstones());

    std::string msg;
    ASSERT_EQ(
            cb::engine_errc::success,
            engine->setFlushParam("retain_erroneous_tombstones", "true", msg));
    EXPECT_TRUE(store.isRetainErroneousTombstones());

    ASSERT_EQ(
            cb::engine_errc::success,
            engine->setFlushParam("retain_erroneous_tombstones", "false", msg));
    EXPECT_FALSE(store.isRetainErroneousTombstones());
}

// getKeyStats tests //////////////////////////////////////////////////////////

// Check that keystats on ejected items. When ejected should return ewouldblock
// until bgfetch completes.
TEST_P(EPBucketTest, GetKeyStatsEjected) {
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

    if (!fullEviction()) {
        EXPECT_EQ(cb::engine_errc::success, do_getKeyStats())
                << "Expected to get key stats on evicted item";

    } else {
        // Try to get key stats. This should return EWOULDBLOCK (as the whole
        // item is no longer resident). As we arn't running the full EPEngine
        // task system, then no BGFetch task will be automatically run, we'll
        // manually run it.

        EXPECT_EQ(cb::engine_errc::would_block, do_getKeyStats())
                << "Expected to need to go to disk to get key stats on fully "
                   "evicted item";

        // Try a second time - this should detect the already-created temp
        // item, and re-schedule the bgfetch.
        EXPECT_EQ(cb::engine_errc::would_block, do_getKeyStats())
                << "Expected to need to go to disk to get key stats on fully "
                   "evicted item (try 2)";

        // Manually run the BGFetcher task; to fetch the two outstanding
        // requests (for the same key).
        runBGFetcherTask();

        ASSERT_EQ(cb::engine_errc::success, do_getKeyStats())
                << "Expected to get key stats on evicted item after "
                   "notify_IO_complete";
    }
}

// Replace tests //////////////////////////////////////////////////////////////

// Test replace against an ejected key.
TEST_P(EPBucketTest, ReplaceEExists) {
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

    if (!fullEviction()) {
        // Should be able to replace as still have metadata resident.
        EXPECT_EQ(cb::engine_errc::success, do_replace());

    } else {
        // Should get EWOULDBLOCK as need to go to disk to get metadata.
        EXPECT_EQ(cb::engine_errc::would_block, do_replace());

        // A second request should also get EWOULDBLOCK and add to the
        // existing pending BGFetch
        EXPECT_EQ(cb::engine_errc::would_block, do_replace());

        // Manually run the BGFetcher task; to fetch the two outstanding
        // requests (for the same key).
        runBGFetcherTask();

        EXPECT_EQ(cb::engine_errc::success, do_replace())
                << "Expected to replace on evicted item after "
                   "notify_IO_complete";
    }
}

// Set tests //////////////////////////////////////////////////////////////////

// Test set against an ejected key.
TEST_P(EPBucketTest, SetEExists) {
    // Store an item, then eject it.
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(cb::engine_errc::success, store->set(item, cookie));
    flush_vbucket_to_disk(vbid);
    evict_key(item.getVBucketId(), item.getKey());

    if (!fullEviction()) {
        // Should be able to set (with same cas as previously)
        // as still have metadata resident.
        ASSERT_NE(0, item.getCas());
        EXPECT_EQ(cb::engine_errc::success, store->set(item, cookie));

    } else {
        // Should get EWOULDBLOCK as need to go to disk to get metadata.
        EXPECT_EQ(cb::engine_errc::would_block, store->set(item, cookie));

        // A second request should also get EWOULDBLOCK and add to the
        // existing pending BGFetch
        EXPECT_EQ(cb::engine_errc::would_block, store->set(item, cookie));

        // Manually run the BGFetcher task; to fetch the two outstanding
        // requests (for the same key).
        runBGFetcherTask();

        EXPECT_EQ(cb::engine_errc::success, store->set(item, cookie))
                << "Expected to set on evicted item after notify_IO_complete";
    }
}

// Check performing a mutation to an existing document does not reset the
// frequency count
TEST_P(EPBucketTest, FreqCountTest) {
    const uint8_t initialFreqCount = Item::initialFreqCount;
    StoredDocKey a = makeStoredDocKey("a");
    auto item_v1 = store_item(vbid, a, "old");

    // Perform one or more gets to increase the frequency count
    auto options = static_cast<get_options_t>(TRACK_REFERENCE);
    GetValue v = store->get(a, vbid, nullptr, options);
    while (v.item->getFreqCounterValue() == initialFreqCount) {
        v = store->get(a, vbid, nullptr, options);
    }

    // Update the document with a new value
    auto item_v2 = store_item(vbid, a, "new");

    // Check that the frequency counter of the document has not been reset
    auto result = store->get(a, vbid, nullptr, {});
    EXPECT_NE(initialFreqCount, result.item->getFreqCounterValue());
}

// Check that performing mutations increases an item's frequency count
TEST_P(EPBucketTest, FreqCountOnlyMutationsTest) {
    StoredDocKey k = makeStoredDocKey("key");
    auto freqCount = Item::initialFreqCount;
    GetValue result;

    // We cannot guarantee the counter will increase after performing a single
    // update, therefore iterate multiple times until the counter changes.
    // A limit of 1000000 times is applied.
    int ii = 0;
    const int limit = 1000000;
    do {
        std::string str = "value" + std::to_string(ii);
        store_item(vbid, k, str);

        // Do an internal get (does not increment frequency count)
        result = store->get(k, vbid, nullptr, {});
        ++ii;
    } while (freqCount == result.item->getFreqCounterValue() && ii < limit);

    // Check that the frequency counter of the document has increased
    EXPECT_LT(freqCount, result.item->getFreqCounterValue());
    freqCount = result.item->getFreqCounterValue();

    ii = 0;
    do {
        std::string str = "value" + std::to_string(ii);
        auto item = make_item(vbid, k, str);
        store->replace(item, cookie);

        // Do an internal get (does not increment frequency count)
        result = store->get(k, vbid, nullptr, {});
        ++ii;
    } while (freqCount == result.item->getFreqCounterValue() && ii < limit);

    // Check that the frequency counter of the document has increased
    EXPECT_LT(freqCount, result.item->getFreqCounterValue());
}

// Add tests //////////////////////////////////////////////////////////////////

// Test add against an ejected key.
TEST_P(EPBucketTest, AddEExists) {
    // Store an item, then eject it.
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(cb::engine_errc::success, store->set(item, cookie));
    flush_vbucket_to_disk(vbid);
    evict_key(item.getVBucketId(), item.getKey());

    // Setup a lambda for how we want to call add (saves repeating the
    // same arguments for each instance below).
    auto do_add = [this]() {
        auto item = make_item(vbid, makeStoredDocKey("key"), "value2");
        return store->add(item, cookie);
    };

    if (!fullEviction()) {
        // Should immediately return NOT_STORED (as metadata is still resident).
        EXPECT_EQ(cb::engine_errc::not_stored, do_add());

    } else {
        // Should get EWOULDBLOCK as need to go to disk to get metadata.
        EXPECT_EQ(cb::engine_errc::would_block, do_add());

        // A second request should also get EWOULDBLOCK and add to the
        // existing pending BGFetch
        EXPECT_EQ(cb::engine_errc::would_block, do_add());

        // Manually run the BGFetcher task; to fetch the two outstanding
        // requests (for the same key).
        runBGFetcherTask();

        EXPECT_EQ(cb::engine_errc::not_stored, do_add())
                << "Expected to fail to add on evicted item after "
                   "notify_IO_complete";
    }
}

// SetWithMeta tests //////////////////////////////////////////////////////////

// Test setWithMeta replacing an existing, non-resident item
TEST_P(EPBucketTest, SetWithMeta_ReplaceNonResident) {
    // Store an item, then evict it.
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(cb::engine_errc::success, store->set(item, cookie));
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

    if (!fullEviction()) {
        // Should succeed as the metadata is still resident.
        EXPECT_EQ(cb::engine_errc::success, do_setWithMeta());

    } else {
        // Should get EWOULDBLOCK as need to go to disk to get metadata.
        EXPECT_EQ(cb::engine_errc::would_block, do_setWithMeta());

        // A second request should also get EWOULDBLOCK and add to the
        // existing pending BGFetch
        EXPECT_EQ(cb::engine_errc::would_block, do_setWithMeta());

        // Manually run the BGFetcher task; to fetch the two outstanding
        // requests (for the same key).
        runBGFetcherTask();

        ASSERT_EQ(cb::engine_errc::success, do_setWithMeta())
                << "Expected to setWithMeta on evicted item after "
                   "notify_IO_complete";
    }
}

// Deleted-with-Value Tests ///////////////////////////////////////////////////

TEST_P(EPBucketTest, DeletedValue) {
    // Create a deleted item which has a value, then evict it.
    auto key = makeStoredDocKey("key");
    auto item = make_item(vbid, key, "deleted value");
    item.setDeleted();
    EXPECT_EQ(cb::engine_errc::success, store->set(item, cookie));

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
    EXPECT_EQ(cb::engine_errc::would_block, do_get().getStatus())
            << "Expected to need to go to disk to get Deleted-with-value key";

    // Try a second time - this should detect the already-created temp
    // item, and re-schedule the bgfetch.
    EXPECT_EQ(cb::engine_errc::would_block, do_get().getStatus())
            << "Expected to need to go to disk to get Deleted-with-value key "
               "(try 2)";

    // Manually run the BGFetcher task; to fetch the two outstanding
    // requests (for the same key).
    runBGFetcherTask();

    auto result = do_get();
    EXPECT_EQ(cb::engine_errc::success, result.getStatus())
            << "Expected to get Deleted-with-value for evicted key after "
               "notify_IO_complete";
    EXPECT_EQ("deleted value", result.item->getValue()->to_s());
}

// Test to ensure all pendingBGfetches are deleted when the
// VBucketMemoryDeletionTask is run
TEST_P(EPBucketTest, MB_21976) {
    // Store an item, then eject it.
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(cb::engine_errc::success, store->set(item, cookie));
    flush_vbucket_to_disk(vbid);
    evict_key(item.getVBucketId(), item.getKey());

    // Perform a get, which should EWOULDBLOCK
    auto options = static_cast<get_options_t>(QUEUE_BG_FETCH |
                                                       HONOR_STATES |
                                                       TRACK_REFERENCE |
                                                       DELETE_TEMP |
                                                       HIDE_LOCKED_CAS |
                                                       TRACK_STATISTICS);
    GetValue gv = store->get(makeStoredDocKey("key"), vbid, cookie, options);
    EXPECT_EQ(cb::engine_errc::would_block, gv.getStatus());

    auto* deleteCookie = create_mock_cookie(engine.get());

    EXPECT_EQ(cb::engine_errc::would_block,
              store->deleteVBucket(vbid, deleteCookie));

    auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
    runNextTask(lpAuxioQ, "Removing (dead) vb:0 from memory and disk");

    // Cookie should be notified with not_my_vbucket.
    EXPECT_EQ(cb::engine_errc::not_my_vbucket, mock_waitfor_cookie(cookie));

    destroy_mock_cookie(deleteCookie);
}

TEST_P(EPBucketTest, TouchCmdDuringBgFetch) {
    const DocKey dockey("key", DocKeyEncodesCollectionId::No);
    const int numTouchCmds = 2;
    auto expiryTime = time(nullptr) + 1000;

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
        EXPECT_EQ(cb::engine_errc::would_block, gv.getStatus());
    }

    // Manually run the BGFetcher task; to fetch the two outstanding
    // requests (for the same key).
    runBGFetcherTask();

    // Issue 2 touch commands again to mock actions post notify from bgFetch
    for (int i = 0; i < numTouchCmds; ++i) {
        GetValue gv = store->getAndUpdateTtl(dockey, vbid, cookie,
                                             (i + 1) * (expiryTime));
        EXPECT_EQ(cb::engine_errc::success, gv.getStatus());
    }
    EXPECT_EQ(numTouchCmds + 1 /* Initial item store */,
              store->getVBucket(vbid)->getHighSeqno());
}

TEST_P(EPBucketTest, checkIfResidentAfterBgFetch) {
    const DocKey dockey("key", DocKeyEncodesCollectionId::No);

    //Store an item
    store_item(vbid, dockey, "value");

    //Trigger a flush to disk
    flush_vbucket_to_disk(vbid);

    //Now, delete the item
    uint64_t cas = 0;
    mutation_descr_t mutation_descr;
    ASSERT_EQ(cb::engine_errc::success,
              store->deleteItem(dockey,
                                cas,
                                vbid,
                                /*cookie*/ cookie,
                                {},
                                /*itemMeta*/ nullptr,
                                mutation_descr));

    flush_vbucket_to_disk(vbid);

    auto options = static_cast<get_options_t>(QUEUE_BG_FETCH |
                                                       HONOR_STATES   |
                                                       TRACK_REFERENCE |
                                                       DELETE_TEMP |
                                                       HIDE_LOCKED_CAS |
                                                       TRACK_STATISTICS |
                                                       GET_DELETED_VALUE);

    GetValue gv = store->get(makeStoredDocKey("key"), vbid, cookie, options);
    EXPECT_EQ(cb::engine_errc::would_block, gv.getStatus());

    runBGFetcherTask();

    // The Get should succeed in this case
    gv = store->get(makeStoredDocKey("key"), vbid, cookie, options);
    EXPECT_EQ(cb::engine_errc::success, gv.getStatus());

    VBucketPtr vb = store->getVBucket(vbid);

    auto result =
            vb->ht.findForRead(dockey, TrackReference::No, WantsDeleted::Yes);
    ASSERT_TRUE(result.storedValue);
    EXPECT_TRUE(result.storedValue->isResident());
}

TEST_P(EPBucketFullEvictionTest, xattrExpiryOnFullyEvictedItem) {
    cb::xattr::Blob builder;

    //Add a few values
    builder.set("_meta", "{\"rev\":10}");
    builder.set("foo", "{\"blob\":true}");

    std::string blob_data{builder.finalize()};
    auto itm = store_item(vbid,
                          makeStoredDocKey("key"),
                          blob_data,
                          0,
                          {cb::engine_errc::success},
                          (PROTOCOL_BINARY_DATATYPE_JSON |
                           PROTOCOL_BINARY_DATATYPE_XATTR));

    GetValue gv = store->getAndUpdateTtl(makeStoredDocKey("key"), vbid, cookie,
                                         time(NULL) + 120);
    EXPECT_EQ(cb::engine_errc::success, gv.getStatus());
    std::unique_ptr<Item> get_itm(std::move(gv.item));

    flush_vbucket_to_disk(vbid);
    evict_key(vbid, makeStoredDocKey("key"));
    store->processExpiredItem(
            *get_itm, time(nullptr) + 121, ExpireBy::Compactor);

    auto options = static_cast<get_options_t>(QUEUE_BG_FETCH |
                                                       HONOR_STATES |
                                                       TRACK_REFERENCE |
                                                       DELETE_TEMP |
                                                       HIDE_LOCKED_CAS |
                                                       TRACK_STATISTICS |
                                                       GET_DELETED_VALUE);

    // Compactions are not interlocked with writes so the expiration when driven
    // by compaction will need to bg fetch the item from disk if it is not
    // resident to ensure that we don't "expire" old versions of items.
    runBGFetcherTask();

    gv = store->get(makeStoredDocKey("key"), vbid, cookie, options);
    ASSERT_EQ(cb::engine_errc::success, gv.getStatus());

    get_itm = std::move(gv.item);
    auto get_data = const_cast<char*>(get_itm->getData());
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_XATTR, get_itm->getDataType())
              << "Unexpected Datatype";

    cb::char_buffer value_buf{get_data, get_itm->getNBytes()};
    cb::xattr::Blob new_blob(value_buf, /*compressed?*/ false);

    const std::string& rev_str{"{\"rev\":10}"};
    const std::string& meta_str = to_string(new_blob.get("_meta"));

    EXPECT_EQ(rev_str, meta_str) << "Unexpected system xattrs";
    EXPECT_TRUE(new_blob.get("foo").empty())
            << "The foo attribute should be gone";
}

TEST_P(EPBucketFullEvictionTest, ExpiryFindNonResidentItem) {
    EXPECT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_active, {}));

    // 1) Store item
    auto key = makeStoredDocKey("a");
    store_item(vbid, key, "v1");
    flushVBucketToDiskIfPersistent(vbid, 1);

    // 2) Grab item from disk just like the compactor would
    vb_bgfetch_queue_t q;
    vb_bgfetch_item_ctx_t ctx;
    ctx.addBgFetch(std::make_unique<FrontEndBGFetchItem>(
            nullptr, ValueFilter::VALUES_DECOMPRESSED, 0));
    auto diskDocKey = makeDiskDocKey("a");
    q[diskDocKey] = std::move(ctx);
    store->getRWUnderlying(vbid)->getMulti(vbid, q);
    EXPECT_EQ(cb::engine_errc::success, q[diskDocKey].value.getStatus());
    EXPECT_EQ("v1", q[diskDocKey].value.item->getValue()->to_s());

    // 3) Evict item
    evict_key(vbid, key);

    auto vb = store->getVBucket(vbid);
    ASSERT_EQ(0, vb->numExpiredItems);

    // 4) Callback from the "pager" with the item.
    vb->processExpiredItem(*q[diskDocKey].value.item, 0, ExpireBy::Pager);
    EXPECT_EQ(1, vb->numExpiredItems);

    flushVBucketToDiskIfPersistent(vbid, 1);
}

void EPBucketFullEvictionTest::compactionFindsNonResidentItem(
        bool dropCollection, bool switchToReplica) {
    EXPECT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_active, {}));

    // 1) Store Av1 and persist
    auto key = makeStoredDocKey("a");
    store_item(vbid, key, "v1");
    flushVBucketToDiskIfPersistent(vbid, 1);

    // 2) Grab Av1 item from disk just like the compactor would
    vb_bgfetch_queue_t q;
    vb_bgfetch_item_ctx_t ctx;
    ctx.addBgFetch(std::make_unique<FrontEndBGFetchItem>(
            nullptr, ValueFilter::VALUES_DECOMPRESSED, 0));
    auto diskDocKey = makeDiskDocKey("a");
    q[diskDocKey] = std::move(ctx);
    store->getRWUnderlying(vbid)->getMulti(vbid, q);
    EXPECT_EQ(cb::engine_errc::success, q[diskDocKey].value.getStatus());
    EXPECT_EQ("v1", q[diskDocKey].value.item->getValue()->to_s());

    // 3) Evict Av1
    evict_key(vbid, key);

    auto vb = store->getVBucket(vbid);
    ASSERT_EQ(0, vb->numExpiredItems);

    // 4) Callback from the "compactor" with Av1
    vb->processExpiredItem(*q[diskDocKey].value.item, 0, ExpireBy::Compactor);

    int expectedExpiredItems = 1;
    if (dropCollection) {
        CollectionsManifest cm;
        cm.remove(CollectionEntry::defaultC);
        EXPECT_EQ(setCollections(cookie, cm), cb::engine_errc::success);
        expectedExpiredItems = 0;
        flushVBucketToDiskIfPersistent(vbid, 1);
    } else {
        // We should not have deleted the item and should not flush anything
        flushVBucketToDiskIfPersistent(vbid, 0);
    }

    EXPECT_EQ(0, vb->numExpiredItems);

    // We should have queued a BGFetch for the item
    EXPECT_EQ(1, vb->getNumItems());
    ASSERT_TRUE(vb->hasPendingBGFetchItems());

    auto highSeqno = vb->getHighSeqno();
    if (switchToReplica) {
        EXPECT_EQ(cb::engine_errc::success,
                  store->setVBucketState(vbid, vbucket_state_replica, {}));
    }

    runBGFetcherTask();

    if (!switchToReplica) {
        EXPECT_FALSE(vb->hasPendingBGFetchItems());

        // We should have expired the item
        EXPECT_EQ(expectedExpiredItems, vb->numExpiredItems);

        // But it still exists on disk until we flush
        EXPECT_EQ(1, vb->getNumItems());

        auto expectedItems = 0;
        if (isRocksDB() || dropCollection) {
            // RocksDB doesn't know if we insert or update so item counts are
            // not correct. Or if we drop the collection, no expiry. Item count
            // won't be updated until collections are purged, so 1 is correct.
            expectedItems = 1;
            if (dropCollection) {
                EXPECT_EQ(highSeqno, vb->getHighSeqno());
            }
        } else {
            EXPECT_LT(highSeqno, vb->getHighSeqno());
            flushVBucketToDiskIfPersistent(vbid, 1);
        }
        EXPECT_EQ(expectedItems, vb->getNumItems());
    } else {
        EXPECT_EQ(0, vb->numExpiredItems);
        EXPECT_EQ(highSeqno, vb->getHighSeqno());
    }
}

TEST_P(EPBucketFullEvictionTest, CompactionFindsNonResidentItem) {
    compactionFindsNonResidentItem(false, false);
}

TEST_P(EPBucketFullEvictionTest, MB_42295_dropCollectionBeforeExpiry) {
    compactionFindsNonResidentItem(true, false);
}

TEST_P(EPBucketFullEvictionTest, MB_48841_switchToReplica) {
    // Test will switch from active to replica, bgfetch runs whilst replica and
    // all pending expiries must not take affect.
    compactionFindsNonResidentItem(false, true);
}

/**
 * This is a valid test case (for at least magma) where the following can
 * happen:
 *
 * 1) Document "a" comes in and gets persisted (henceforth referred to as Av1)
 * 2) Update of document a happens but is not yet persisted (henceforth referred
 *    to as Av2)
 * 3) (Magma) background compaction starts and finds Av1 and calls back up to
 *    the engine to expire it but does not yet process it
 * 4) Av2 is persisted and evicted by the ItemPager to reduce memory usage
 * 5) Expiry callback (of Av1) continues and finds no document in the HashTable
 *
 * Given that we interlock flushing and compaction for couchstore buckets this
 * should not be possible for them.
 */
TEST_P(EPBucketFullEvictionTest, CompactionFindsNonResidentSupersededItem) {
    EXPECT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_active, {}));

    // 1) Store Av1 and persist
    auto key = makeStoredDocKey("a");
    store_item(vbid, key, "v1");
    flushVBucketToDiskIfPersistent(vbid, 1);

    // 2) Store Av2
    store_item(vbid, key, "v2");

    // 3) Grab Av1 item from disk just like the compactor would
    vb_bgfetch_queue_t q;
    vb_bgfetch_item_ctx_t ctx;
    ctx.addBgFetch(std::make_unique<FrontEndBGFetchItem>(
            nullptr, ValueFilter::VALUES_DECOMPRESSED, 0));
    auto diskDocKey = makeDiskDocKey("a");
    q[diskDocKey] = std::move(ctx);
    store->getRWUnderlying(vbid)->getMulti(vbid, q);
    EXPECT_EQ(cb::engine_errc::success, q[diskDocKey].value.getStatus());
    EXPECT_EQ("v1", q[diskDocKey].value.item->getValue()->to_s());

    // 4) Flush and evict Av2
    flushVBucketToDiskIfPersistent(vbid, 1);
    evict_key(vbid, key);

    auto vb = store->getVBucket(vbid);
    ASSERT_EQ(0, vb->numExpiredItems);

    // 5) Callback from the "compactor" with Av1
    vb->processExpiredItem(*q[diskDocKey].value.item, 0, ExpireBy::Compactor);

    EXPECT_EQ(0, vb->numExpiredItems);

    // We should not have deleted the item and should not flush anything
    flushVBucketToDiskIfPersistent(vbid, 0);

    // We should have queued a BGFetch for the item
    ASSERT_TRUE(vb->hasPendingBGFetchItems());
    runBGFetcherTask();
    EXPECT_FALSE(vb->hasPendingBGFetchItems());

    // BGFetch runs and does not expire the item as the item has been superseded
    // by a newer version (Av2)
    EXPECT_EQ(0, vb->numExpiredItems);

    // Nothing to flush
    flushVBucketToDiskIfPersistent(vbid, 0);

    auto expectedItems = 1;
    if (isRocksDB()) {
        // RocksDB doesn't know if we insert or update so item counts are not
        // correct
        expectedItems = 2;
    }
    EXPECT_EQ(expectedItems, vb->getNumItems());
}

TEST_P(EPBucketFullEvictionTest, CompactionBGExpiryFindsTempItem) {
    EXPECT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_active, {}));

    // 1) Store Av1 and persist
    auto key = makeStoredDocKey("a");
    store_item(vbid, key, "v1");
    flushVBucketToDiskIfPersistent(vbid, 1);

    // 2) Grab Av1 item from disk just like the compactor would
    vb_bgfetch_queue_t q;
    vb_bgfetch_item_ctx_t ctx;
    ctx.addBgFetch(std::make_unique<FrontEndBGFetchItem>(
            nullptr, ValueFilter::VALUES_DECOMPRESSED, 0));
    auto diskDocKey = makeDiskDocKey("a");
    q[diskDocKey] = std::move(ctx);
    store->getRWUnderlying(vbid)->getMulti(vbid, q);
    EXPECT_EQ(cb::engine_errc::success, q[diskDocKey].value.getStatus());
    EXPECT_EQ("v1", q[diskDocKey].value.item->getValue()->to_s());

    // 3) Evict Av1
    evict_key(vbid, key);

    auto vb = store->getVBucket(vbid);
    ASSERT_EQ(0, vb->numExpiredItems);

    // 4) Callback from the "compactor" with Av1
    vb->processExpiredItem(*q[diskDocKey].value.item, 0, ExpireBy::Compactor);

    EXPECT_EQ(0, vb->numExpiredItems);

    // We should not have deleted the item and should not flush anything
    flushVBucketToDiskIfPersistent(vbid, 0);

    // We should have queued a BGFetch for the item
    EXPECT_EQ(1, vb->getNumItems());
    ASSERT_TRUE(vb->hasPendingBGFetchItems());

    // 5) Do another get that should BGFetch to ensure that we expire the item
    // correctly
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS | GET_DELETED_VALUE);
    auto gv = store->get(key, vbid, cookie, options);
    EXPECT_EQ(cb::engine_errc::would_block, gv.getStatus());

    runBGFetcherTask();
    EXPECT_FALSE(vb->hasPendingBGFetchItems());

    // We should have expired the item
    EXPECT_EQ(1, vb->numExpiredItems);

    // But it still exists on disk until we flush
    EXPECT_EQ(1, vb->getNumItems());
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto expectedItems = 0;
    if (isRocksDB()) {
        // RocksDB doesn't know if we insert or update so item counts are not
        // correct
        expectedItems = 1;
    }
    EXPECT_EQ(expectedItems, vb->getNumItems());

    EXPECT_EQ(cb::engine_errc::success, mock_waitfor_cookie(cookie));
}

TEST_P(EPBucketFullEvictionTest, ExpiryFindsPrepareWithSameCas) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    // 1) Store prepare with expiry
    auto key = makeStoredDocKey("a");
    using namespace cb::durability;
    auto pre = makePendingItem(key, "value", Requirements{Level::Majority, {}});
    pre->setVBucketId(vbid);
    pre->setExpTime(1);
    EXPECT_EQ(cb::engine_errc::sync_write_pending, setItem(*pre, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto vb = store->getVBucket(vbid);

    // 2) Set vbucket on a disk snapshot so that when we warmup we scan the
    // entire snapshot for prepares (i.e. incomplete disk snapshot)
    vb->checkpointManager->createSnapshot(2, 3, 0, CheckpointType::Disk, 3);

    // 3) Seqno ack and commit the prepare
    vb->seqnoAcknowledged(folly::SharedMutex::ReadHolder(vb->getStateLock()),
                          "replica",
                          1 /*prepareSeqno*/);
    vb->processResolvedSyncWrites();
    flushVBucketToDiskIfPersistent(vbid, 1);

    // 4) Restart and warmup
    vb.reset();
    resetEngineAndWarmup();
    vb = store->getVBucket(vbid);

    {
        // Verify that the prepare is there and it's "MaybeVisible"
        auto ret = vb->ht.findForUpdate(key);
        ASSERT_TRUE(ret.pending);
        ASSERT_TRUE(ret.pending->isPreparedMaybeVisible());

        // And that the commit is there too
        ASSERT_TRUE(ret.committed);
    }

    // 5) Grab the item from disk just like the compactor would
    vb_bgfetch_queue_t q;
    vb_bgfetch_item_ctx_t ctx;
    auto diskDocKey = makeDiskDocKey("a");
    q[diskDocKey] = std::move(ctx);
    store->getRWUnderlying(vbid)->getMulti(vbid, q);
    EXPECT_EQ(cb::engine_errc::success, q[diskDocKey].value.getStatus());

    // 6) Callback from the "compactor" with the item to try and expire it. We
    //    could also pretend to be the pager here.
    ASSERT_EQ(0, vb->numExpiredItems);
    vb->processExpiredItem(*q[diskDocKey].value.item, 2, ExpireBy::Compactor);

    // Item expiry cannot take place if the MaybeVisible prepare exists.
    EXPECT_EQ(0, vb->numExpiredItems);
    {
        // Verify that the prepare is there and it's "MaybeVisible". Before the
        // fix processExpiredItem would select and replace the prepare which is
        // incorrect and causes us to have two committed items in the HashTable.
        auto ret = vb->ht.findForUpdate(key);
        ASSERT_TRUE(ret.pending);
        ASSERT_TRUE(ret.pending->isPreparedMaybeVisible());

        // And that the commit is there too
        ASSERT_TRUE(ret.committed);
    }
}

/**
 * MB-49207:
 *
 * Test that if we "pause" a bg fetch after reading the item(s) from disk but
 * before restoring them to the HashTable and update an item in this window then
 * then BgFetcher does not restore the now "old" version of the item back into
 * the HashTable.
 *
 * This particular variant tests what happens when we bg fetch to decide if we
 * should expire an item during compaction and no item was found
 */
TEST_P(EPBucketFullEvictionTest, CompactionBGExpiryNewGenerationNoItem) {
    ASSERT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_active, {}));

    // 1) Store Av1 and persist
    auto key = makeStoredDocKey("a");
    store_item(vbid, key, "v1");
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto vb = store->getVBucket(vbid);

    // 2) Grab Av1 item from disk just like the compactor would
    vb_bgfetch_queue_t q;
    vb_bgfetch_item_ctx_t ctx;
    ctx.addBgFetch(std::make_unique<FrontEndBGFetchItem>(
            nullptr, ValueFilter::VALUES_DECOMPRESSED, 0));
    auto diskDocKey = makeDiskDocKey("a");
    q[diskDocKey] = std::move(ctx);
    store->getRWUnderlying(vbid)->getMulti(vbid, q);
    ASSERT_EQ(cb::engine_errc::success, q[diskDocKey].value.getStatus());
    ASSERT_EQ("v1", q[diskDocKey].value.item->getValue()->to_s());

    // 3) Evict Av1
    evict_key(vbid, key);
    ASSERT_EQ(0, vb->numExpiredItems);

    // 4) Callback from the "compactor" with Av1
    vb->processExpiredItem(*q[diskDocKey].value.item, 0, ExpireBy::Compactor);

    ASSERT_EQ(0, vb->numExpiredItems);

    // We should not have deleted the item and should not flush anything
    flushVBucketToDiskIfPersistent(vbid, 0);

    // We should have queued a BGFetch for the item
    EXPECT_EQ(1, vb->getNumItems());
    ASSERT_TRUE(vb->hasPendingBGFetchItems());

    // 5a) Start a fetch and read Av1 from disk, but don't check the HT result
    // yet
    auto* bucket = dynamic_cast<MockEPBucket*>(engine->getKVBucket());
    auto& bgFetcher = bucket->getBgFetcher(vbid);

    bgFetcher.preCompleteHook = [this, &key]() {
        // 5b) Create and evict Av2 (2nd generation of this item)
        auto key = makeStoredDocKey("a");
        store_item(vbid, key, "v2");
        flushVBucketToDiskIfPersistent(vbid, 1);
        evict_key(vbid, key);
    };
    runBGFetcherTask();

    EXPECT_EQ(0, vb->numExpiredItems);
}

/**
 * MB-49207:
 *
 * Test that if we "pause" a bg fetch after reading the item(s) from disk but
 * before restoring them to the HashTable and update an item in this window then
 * then BgFetcher does not restore the now "old" version of the item back into
 * the HashTable.
 *
 * This particular variant tests what happens when we bg fetch to decide if we
 * should expire an item during compaction and a temp item was found
 */
TEST_P(EPBucketFullEvictionTest, CompactionBGExpiryNewGenerationTempItem) {
    ASSERT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_active, {}));

    // 1) Store Av1 and persist
    auto key = makeStoredDocKey("a");
    store_item(vbid, key, "v1");
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto vb = store->getVBucket(vbid);

    // 2) Grab Av1 item from disk just like the compactor would
    vb_bgfetch_queue_t q;
    vb_bgfetch_item_ctx_t ctx;
    ctx.addBgFetch(std::make_unique<FrontEndBGFetchItem>(
            nullptr, ValueFilter::VALUES_DECOMPRESSED, 0));
    auto diskDocKey = makeDiskDocKey("a");
    q[diskDocKey] = std::move(ctx);
    store->getRWUnderlying(vbid)->getMulti(vbid, q);
    ASSERT_EQ(cb::engine_errc::success, q[diskDocKey].value.getStatus());
    ASSERT_EQ("v1", q[diskDocKey].value.item->getValue()->to_s());

    // 3) Evict Av1
    evict_key(vbid, key);
    ASSERT_EQ(0, vb->numExpiredItems);

    // 4) Callback from the "compactor" with Av1
    vb->processExpiredItem(*q[diskDocKey].value.item, 0, ExpireBy::Compactor);

    ASSERT_EQ(0, vb->numExpiredItems);

    // We should not have deleted the item and should not flush anything
    flushVBucketToDiskIfPersistent(vbid, 0);

    // We should have queued a BGFetch for the item
    ASSERT_EQ(1, vb->getNumItems());
    ASSERT_TRUE(vb->hasPendingBGFetchItems());

    // 5a) Start a fetch and read Av1 from disk, but don't check the HT result
    // yet
    auto* bucket = dynamic_cast<MockEPBucket*>(engine->getKVBucket());
    auto& bgFetcher = bucket->getBgFetcher(vbid);

    bgFetcher.preCompleteHook = [this, &key]() {
        // 5b) Create and evict Av2 (2nd generation of this item)
        auto key = makeStoredDocKey("a");
        store_item(vbid, key, "v2");
        flushVBucketToDiskIfPersistent(vbid, 1);
        evict_key(vbid, key);

        // 5c) Another get to bring our temp item back
        auto options = static_cast<get_options_t>(
                QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
                HIDE_LOCKED_CAS | TRACK_STATISTICS | GET_DELETED_VALUE);

        auto gv = store->get(key, vbid, cookie, options);

        auto vb = store->getVBucket(vbid);
        ASSERT_TRUE(vb);

        auto res = vb->ht.findForUpdate(key);
        ASSERT_TRUE(res.committed);
        ASSERT_TRUE(res.committed->isTempInitialItem());
    };
    runBGFetcherTask();

    auto res = vb->ht.findForUpdate(key);
    ASSERT_TRUE(res.committed);
    EXPECT_FALSE(res.committed->isDeleted());
}

TEST_P(EPBucketFullEvictionTest, UnDelWithPrepare) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    // 1) Store, delete, and persist the item. We need to do this to ensure that
    //    the bloom filter tells us to go to disk when we try gets if there is
    //    no delete in the HashTable.
    auto key = makeStoredDocKey("key");
    storeAndDeleteItem(vbid, key, "value");
    auto vb = store->getVBucket(vbid);
    EXPECT_EQ(0, vb->getNumItems());

    // 1) Store the new item and persist
    store_item(vbid, key, "value");
    flushVBucketToDiskIfPersistent(vbid, 1);
    EXPECT_EQ(1, vb->getNumItems());

    // 2) Store the new item but don't persist it yet. We want to test what
    //    happens when it's dirty.
    delete_item(vbid, key);

    // 1 because we haven't persisted the delete yet
    EXPECT_EQ(1, vb->getNumItems());

    // 3) Get now returns not found
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);
    auto gv = getInternal(key, vbid, cookie, ForGetReplicaOp::No, options);
    EXPECT_EQ(cb::engine_errc::no_such_key, gv.getStatus());

    // 4) Add prepare
    auto prepare = makePendingItem(key, "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending, addItem(*prepare, cookie));

    // 1 because we haven't persisted the delete yet
    EXPECT_EQ(1, vb->getNumItems());

    // 5) Check that the HashTable state is now correct
    {
        auto htRes = vb->ht.findForUpdate(key);
        ASSERT_TRUE(htRes.committed);
        EXPECT_TRUE(htRes.committed->isDeleted());
        EXPECT_TRUE(htRes.pending);
    }

    // @TODO RDB: Rocks item counting is broken and overcounts assuming
    // everything is a new item
    if (!isRocksDB()) {
        flushVBucketToDiskIfPersistent(vbid, 2);
        EXPECT_EQ(0, vb->getNumItems());
    }
}

TEST_P(EPBucketFullEvictionTest, RaceyFetchingMetaBgFetch) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto key = makeStoredDocKey("key");
    store_item(vbid,
               key,
               "ohno",
               0 /*exptime*/,
               {cb::engine_errc::success} /*expected*/,
               PROTOCOL_BINARY_RAW_BYTES);
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);

    auto oldCas = vb->ht.findForUpdate(key).committed->getCas();

    const char* msg;
    store->evictKey(key, vbid, &msg);

    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS | GET_DELETED_VALUE);

    ItemMetaData itemMeta;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    ASSERT_EQ(
            cb::engine_errc::would_block,
            store->getMetaData(key, vbid, cookie, itemMeta, deleted, datatype));

    auto* bucket = dynamic_cast<MockEPBucket*>(engine->getKVBucket());
    auto& bgFetcher = bucket->getBgFetcher(vbid);

    bgFetcher.preCompleteHook = [this, &key, &options]() {
        store_item(vbid,
                   key,
                   "value",
                   0 /*exptime*/,
                   {cb::engine_errc::success} /*expected*/,
                   PROTOCOL_BINARY_RAW_BYTES);
        flushVBucketToDiskIfPersistent(vbid, 1);
        const char* msg;
        store->evictKey(key, vbid, &msg);

        if (isFullEviction()) {
            // Need to make the item "temp" for the bg fetcher to consider
            // completing this fetch
            auto gv = store->get(key, vbid, cookie, options);
        } else {
            auto vb = store->getVBucket(vbid);
            ASSERT_TRUE(vb);

            auto res = vb->ht.findForUpdate(key);
            ASSERT_TRUE(res.committed);
            ASSERT_FALSE(res.committed->getValue());
        }
    };

    runBGFetcherTask();

    auto res = vb->ht.findForUpdate(key);
    ASSERT_TRUE(res.committed);
    EXPECT_FALSE(res.committed->isResident());
    EXPECT_NE(oldCas, res.committed->getCas());
}

/**
 * Verify that when getIf is used it only fetches the metdata from disk for
 * the filter, and not the complete document.
 * Negative case where filter doesn't match.
**/
TEST_P(EPBucketTest, getIfOnlyFetchesMetaForFilterNegative) {
    // Store an item, then eject it.
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(cb::engine_errc::success, store->set(item, cookie));
    flush_vbucket_to_disk(vbid);
    evict_key(item.getVBucketId(), item.getKey());

    // Setup a lambda for how we want to call get_if() - filter always returns
    // false.
    auto do_getIf = [this]() {
        return engine->getIfInner(cookie,
                                  makeStoredDocKey("key"),
                                  vbid,
                                  [](const item_info& info) { return false; });
    };

    auto& stats = engine->getEpStats();
    ASSERT_EQ(0, stats.bg_fetched);
    ASSERT_EQ(0, stats.bg_meta_fetched);

    if (!fullEviction()) {
        // Value-only should reject (via filter) on first attempt (no need to
        // go to disk).
        auto res = do_getIf();
        EXPECT_EQ(cb::engine_errc::success, res.first);
        EXPECT_EQ(nullptr, res.second.get());

    } else {
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
    }
}

/**
 * Verify that when getIf is used it only fetches the metdata from disk for
 * the filter, and not the complete document.
 * Positive case where filter does match.
**/
TEST_P(EPBucketTest, getIfOnlyFetchesMetaForFilterPositive) {
    // Store an item, then eject it.
    auto item = make_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(cb::engine_errc::success, store->set(item, cookie));
    flush_vbucket_to_disk(vbid);
    evict_key(item.getVBucketId(), item.getKey());

    // Setup a lambda for how we want to call get_if() - filter always returns
    // true.
    auto do_getIf = [this]() {
        return engine->getIfInner(cookie,
                                  makeStoredDocKey("key"),
                                  vbid,
                                  [](const item_info& info) { return true; });
    };

    auto& stats = engine->getEpStats();
    ASSERT_EQ(0, stats.bg_fetched);
    ASSERT_EQ(0, stats.bg_meta_fetched);

    if (!fullEviction()) {
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

    } else {
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
    }
}

/**
 * Verify that a get of a deleted item with no value successfully
 * returns an item
 */
TEST_P(EPBucketTest, getDeletedItemWithNoValue) {
    const DocKey dockey("key", DocKeyEncodesCollectionId::No);

    // Store an item
    store_item(vbid, dockey, "value");

    // Trigger a flush to disk
    flush_vbucket_to_disk(vbid);

    uint64_t cas = 0;
    mutation_descr_t mutation_descr;
    ASSERT_EQ(cb::engine_errc::success,
              store->deleteItem(dockey,
                                cas,
                                vbid,
                                /*cookie*/ cookie,
                                {},
                                /*itemMeta*/ nullptr,
                                mutation_descr));

    // Ensure that the delete has been persisted
    flush_vbucket_to_disk(vbid);

    auto options = static_cast<get_options_t>(QUEUE_BG_FETCH |
                                                       HONOR_STATES |
                                                       TRACK_REFERENCE |
                                                       DELETE_TEMP |
                                                       HIDE_LOCKED_CAS |
                                                       TRACK_STATISTICS |
                                                       GET_DELETED_VALUE);

    GetValue gv = store->get(makeStoredDocKey("key"), vbid, cookie, options);
    EXPECT_EQ(cb::engine_errc::would_block, gv.getStatus());

    runBGFetcherTask();

    // The Get should succeed in this case
    gv = store->get(makeStoredDocKey("key"), vbid, cookie, options);
    EXPECT_EQ(cb::engine_errc::success, gv.getStatus());

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
TEST_P(EPBucketTest, getDeletedItemWithValue) {
    const DocKey dockey("key", DocKeyEncodesCollectionId::No);

    // Store an item
    store_item(vbid, dockey, "value");

    // Trigger a flush to disk
    flush_vbucket_to_disk(vbid);

    auto item = make_item(vbid, dockey, "deletedvalue");
    item.setDeleted();
    EXPECT_EQ(cb::engine_errc::success, store->set(item, cookie));
    flush_vbucket_to_disk(vbid);

    //Perform a get
    auto options = static_cast<get_options_t>(QUEUE_BG_FETCH |
                                                       HONOR_STATES |
                                                       TRACK_REFERENCE |
                                                       DELETE_TEMP |
                                                       HIDE_LOCKED_CAS |
                                                       TRACK_STATISTICS |
                                                       GET_DELETED_VALUE);

    GetValue gv = store->get(dockey, vbid, cookie, options);
    EXPECT_EQ(cb::engine_errc::would_block, gv.getStatus());

    runBGFetcherTask();

    // The Get should succeed in this case
    gv = store->get(dockey, vbid, cookie, options);
    EXPECT_EQ(cb::engine_errc::success, gv.getStatus());

    // Ensure that the item is deleted and the value matches
    Item* itm = gv.item.get();
    EXPECT_EQ("deletedvalue", itm->getValue()->to_s());
    EXPECT_TRUE(itm->isDeleted());
}

/**
 * Verify that a get of a non-resident item is returned compressed to client
 * iff client supports Snappy.
 */
TEST_P(EPBucketTest, GetNonResidentCompressed) {
    // Setup: Change bucket to passive compression.
    engine->setCompressionMode("passive");

    // Setup: Store item then evict.
    const DocKey dockey("key", DocKeyEncodesCollectionId::No);
    store_item(vbid,
               dockey,
               "\"A JSON value which repeated strings so will compress "
               "compress compress compress.\"");
    flush_vbucket_to_disk(vbid);

    auto doGet = [&] {
        evict_key(vbid, dockey);
        auto options = static_cast<get_options_t>(
                QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
                HIDE_LOCKED_CAS | TRACK_STATISTICS | GET_DELETED_VALUE);
        GetValue gv = store->get(dockey, vbid, cookie, options);
        EXPECT_EQ(cb::engine_errc::would_block, gv.getStatus());
        runBGFetcherTask();

        // The Get should succeed in this case
        gv = store->get(dockey, vbid, cookie, options);
        EXPECT_EQ(cb::engine_errc::success, gv.getStatus());
        return std::move(gv.item);
    };

    // Test 1: perform a get when snappy is supported.
    // Check the item is returned compressed (if supported by KVStore)
    const auto Json = PROTOCOL_BINARY_DATATYPE_JSON;
    const auto JsonSnappy = Json | PROTOCOL_BINARY_DATATYPE_SNAPPY;
    cookie_to_mock_cookie(cookie)->setDatatypeSupport(JsonSnappy);

    auto item = doGet();
    if (supportsFetchingAsSnappy()) {
        EXPECT_EQ(JsonSnappy, item->getDataType());
    } else {
        EXPECT_EQ(Json, item->getDataType());
    }

    // Test 2: perform a get when snappy is not supported.
    // Check the item is returned uncompressed.
    cookie_to_mock_cookie(cookie)->setDatatypeSupport(Json);
    item = doGet();
    EXPECT_EQ(Json, item->getDataType());
}

//Test to verify the behavior in the condition where
//memOverhead is greater than the bucket quota
TEST_P(EPBucketTest, memOverheadMemoryCondition) {
    //Limit the bucket quota to 200K
    Configuration& config = engine->getConfiguration();
    config.setMaxSize(204800);
    config.setMemHighWat(0.8 * 204800);
    config.setMemLowWat(0.6 * 204800);

    //Ensure the memOverhead is greater than the bucket quota
    auto& stats = engine->getEpStats();
    stats.coreLocal.get()->memOverhead.store(config.getMaxSize() + 1);

    // Fill bucket until we hit ENOMEM - note storing via external
    // API (epstore) so we trigger the memoryCondition() code in the event of
    // cb::engine_errc::no_memory.
    size_t count = 0;
    const std::string value(512, 'x'); // 512B value to use for documents.
    cb::engine_errc result;
    auto dummyCookie = std::make_unique<MockCookie>(engine.get());
    for (result = cb::engine_errc::success; result == cb::engine_errc::success;
         count++) {
        auto item = make_item(vbid,
                              makeStoredDocKey("key_" + std::to_string(count)),
                              value);
        uint64_t cas;
        result = engine->storeInner(
                dummyCookie.get(), item, cas, StoreSemantics::Set, false);
    }

    ASSERT_EQ(cb::engine_errc::no_memory, result);
}

// MB-26907: Test that the item count is properly updated upon an expiry for
//           both value and full eviction.
TEST_P(EPBucketTest, expiredItemCount) {
    // Create a item with an expiry time
    auto expiryTime = time(nullptr) + 290;
    auto key = makeStoredDocKey("key");
    auto item = make_item(vbid, key, "expire value", expiryTime);
    ASSERT_EQ(cb::engine_errc::success, store->set(item, cookie));

    flush_vbucket_to_disk(vbid);
    ASSERT_EQ(1, store->getVBucket(vbid)->getNumItems());
    ASSERT_EQ(0, store->getVBucket(vbid)->numExpiredItems);
    // Travel past expiry time
    TimeTraveller missy(64000);

    // Trigger expiry on a GET
    auto gv = store->get(key, vbid, cookie, NONE);
    EXPECT_EQ(cb::engine_errc::no_such_key, gv.getStatus());

    flush_vbucket_to_disk(vbid);

    // @TODO RDB: Fix when correcting item count. Counts correct for value
    // eviction as they are done in memory
    if (!fullEviction() || !isRocksDB()) {
        EXPECT_EQ(0, store->getVBucket(vbid)->getNumItems());
    } else {
        EXPECT_EQ(1, store->getVBucket(vbid)->getNumItems());
    }
    EXPECT_EQ(1, store->getVBucket(vbid)->numExpiredItems);
}

// MB-48577, Replace operations are blocked until traffic has been enabled.
TEST_P(EPBucketTest, replaceRequiresEnabledTraffic) {
    auto key = makeStoredDocKey("key");
    auto item = make_item(vbid, key, "value2");
    store_item(vbid, key, "value1");
    flush_vbucket_to_disk(vbid);
    engine->public_enableTraffic(false);
    EXPECT_EQ(cb::engine_errc::temporary_failure,
              engine->storeIfInner(
                            cookie, item, 0, StoreSemantics::Replace, {}, false)
                      .status);
    engine->public_enableTraffic(true);
    EXPECT_EQ(cb::engine_errc::success,
              engine->storeIfInner(
                            cookie, item, 0, StoreSemantics::Replace, {}, false)
                      .status);
}

TEST_P(EPBucketBloomFilterParameterizedTest, store_if_throws) {
    // You can't keep returning GetItemInfo
    cb::StoreIfPredicate predicate =
            [](const std::optional<item_info>& existing,
               cb::vbucket_info vb) -> cb::StoreIfStatus {
        return cb::StoreIfStatus::GetItemInfo;
    };

    auto key = makeStoredDocKey("key");
    auto item = make_item(vbid, key, "value2");

    store_item(vbid, key, "value1");
    flush_vbucket_to_disk(vbid);
    evict_key(vbid, key);

    if (fullEviction()) {
        EXPECT_NO_THROW(engine->storeIfInner(cookie,
                                             item,
                                             0 /*cas*/,
                                             StoreSemantics::Set,
                                             predicate,
                                             false));
        runBGFetcherTask();
    }

    // If the itemInfo exists, you can't ask for it again - so expect throw
    EXPECT_THROW(engine->storeIfInner(cookie,
                                      item,
                                      0 /*cas*/,
                                      StoreSemantics::Set,
                                      predicate,
                                      false),
                 std::logic_error);
}

TEST_P(EPBucketBloomFilterParameterizedTest, store_if) {
    struct TestData {
        TestData(StoredDocKey key,
                 cb::StoreIfPredicate predicate,
                 cb::engine_errc expectedVEStatus,
                 cb::engine_errc expectedFEStatus)
            : key(std::move(key)),
              predicate(std::move(predicate)),
              expectedVEStatus(expectedVEStatus),
              expectedFEStatus(expectedFEStatus) {
        }

        const StoredDocKey key;
        const cb::StoreIfPredicate predicate;
        const cb::engine_errc expectedVEStatus;
        const cb::engine_errc expectedFEStatus;
        cb::engine_errc actualStatus;
    };

    std::vector<TestData> testData;
    cb::StoreIfPredicate predicate1 =
            [](const std::optional<item_info>& existing,
               cb::vbucket_info vb) -> cb::StoreIfStatus {
        return cb::StoreIfStatus::Continue;
    };
    cb::StoreIfPredicate predicate2 =
            [](const std::optional<item_info>& existing,
               cb::vbucket_info vb) -> cb::StoreIfStatus {
        return cb::StoreIfStatus::Fail;
    };
    cb::StoreIfPredicate predicate3 =
            [](const std::optional<item_info>& existing,
               cb::vbucket_info vb) -> cb::StoreIfStatus {
        if (existing.has_value()) {
            return cb::StoreIfStatus::Continue;
        }
        return cb::StoreIfStatus::GetItemInfo;
    };
    cb::StoreIfPredicate predicate4 =
            [](const std::optional<item_info>& existing,
               cb::vbucket_info vb) -> cb::StoreIfStatus {
        if (existing.has_value()) {
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
        test.actualStatus = engine->storeIfInner(cookie,
                                                 item,
                                                 0 /*cas*/,
                                                 StoreSemantics::Set,
                                                 test.predicate,
                                                 false)
                                    .status;
        if (test.actualStatus == cb::engine_errc::success) {
            flush_vbucket_to_disk(vbid);
        }
    }

    for (size_t i = 0; i < testData.size(); i++) {
        if (!fullEviction()) {
            EXPECT_EQ(testData[i].expectedVEStatus, testData[i].actualStatus)
                    << "Failed value_only iteration " + std::to_string(i);
        } else {
            EXPECT_EQ(testData[i].expectedFEStatus, testData[i].actualStatus)
                    << "Failed full_eviction iteration " + std::to_string(i);
        }
    }

    if (fullEviction()) {
        runBGFetcherTask();
        for (auto& i : testData) {
            if (i.actualStatus == cb::engine_errc::would_block) {
                auto item = make_item(vbid, i.key, "new_value");
                auto status = engine->storeIfInner(cookie,
                                                   item,
                                                   0 /*cas*/,
                                                   StoreSemantics::Set,
                                                   i.predicate,
                                                   false);
                // The second run should result the same as VE
                EXPECT_EQ(i.expectedVEStatus, status.status);
            }
        }
    }
}

TEST_P(EPBucketBloomFilterParameterizedTest, store_if_fe_interleave) {
    if (!fullEviction()) {
        return;
    }

    cb::StoreIfPredicate predicate =
            [](const std::optional<item_info>& existing,
               cb::vbucket_info vb) -> cb::StoreIfStatus {
        if (existing.has_value()) {
            return cb::StoreIfStatus::Continue;
        }
        return cb::StoreIfStatus::GetItemInfo;
    };

    auto key = makeStoredDocKey("key");
    auto item = make_item(vbid, key, "value2");

    store_item(vbid, key, "value1");
    flush_vbucket_to_disk(vbid);
    evict_key(vbid, key);

    EXPECT_EQ(cb::engine_errc::would_block,
              engine->storeIfInner(cookie,
                                   item,
                                   0 /*cas*/,
                                   StoreSemantics::Set,
                                   predicate,
                                   false)
                      .status);

    // expect another store to the same key to be told the same, even though the
    // first store has populated the store with a temp item
    EXPECT_EQ(cb::engine_errc::would_block,
              engine->storeIfInner(cookie,
                                   item,
                                   0 /*cas*/,
                                   StoreSemantics::Set,
                                   predicate,
                                   false)
                      .status);

    runBGFetcherTask();
    EXPECT_EQ(cb::engine_errc::success,
              engine->storeIfInner(cookie,
                                   item,
                                   0 /*cas*/,
                                   StoreSemantics::Set,
                                   predicate,
                                   false)
                      .status);
}

// Demonstrate the couchstore issue affects get - if we have multiple gets in
// one batch and the keys are crafted in such a way, we will skip out the get
// of the one key which really does exist.
TEST_P(EPBucketFullEvictionNoBloomFilterTest, MB_29816) {
    auto key = makeStoredDocKey("005");
    store_item(vbid, key, "value");
    flush_vbucket_to_disk(vbid);
    evict_key(vbid, key);

    auto key2 = makeStoredDocKey("004");
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);
    auto gv = store->get(key, vbid, cookie, options);
    EXPECT_EQ(cb::engine_errc::would_block, gv.getStatus());
    gv = store->get(key2, vbid, cookie, options);
    EXPECT_EQ(cb::engine_errc::would_block, gv.getStatus());

    runBGFetcherTask();

    // Get the keys again
    gv = store->get(key, vbid, cookie, options);
    ASSERT_EQ(cb::engine_errc::success, gv.getStatus())
            << "key:005 should have been found";

    gv = store->get(key2, vbid, cookie, options);
    ASSERT_EQ(cb::engine_errc::no_such_key, gv.getStatus());
}

/**
 * MB-49207:
 *
 * Test that if we "pause" a bg fetch after reading the item(s) from disk but
 * before restoring them to the HashTable and update an item in this window then
 * then BgFetcher does not restore the now "old" version of the item back into
 * the HashTable.
 *
 * This particular variant tests what happens when we restore deleted metadata
 */
TEST_P(EPBucketFullEvictionNoBloomFilterTest, RaceyFetchingDeletedMetaBgFetch) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto key = makeStoredDocKey("key");

    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);

    const char* msg;
    store->evictKey(key, vbid, &msg);

    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS | GET_DELETED_VALUE);

    ItemMetaData itemMeta;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    ASSERT_EQ(
            cb::engine_errc::would_block,
            store->getMetaData(key, vbid, cookie, itemMeta, deleted, datatype));

    auto* bucket = dynamic_cast<MockEPBucket*>(engine->getKVBucket());
    auto& bgFetcher = bucket->getBgFetcher(vbid);

    bgFetcher.preCompleteHook = [this, &key, &options]() {
        store_item(vbid,
                   key,
                   "value",
                   0 /*exptime*/,
                   {cb::engine_errc::success} /*expected*/,
                   PROTOCOL_BINARY_RAW_BYTES);
        flushVBucketToDiskIfPersistent(vbid, 1);

        const char* msg;
        store->evictKey(key, vbid, &msg);

        // Need to make the item "temp" for the bg fetcher to consider
        // completing the bgfetch
        auto gv = store->get(key, vbid, cookie, options);

        auto vb = store->getVBucket(vbid);
        ASSERT_TRUE(vb);

        auto res = vb->ht.findForUpdate(key);
        ASSERT_TRUE(res.committed);
        ASSERT_TRUE(res.committed->isTempInitialItem());
    };

    runBGFetcherTask();

    auto res = vb->ht.findForUpdate(key);
    ASSERT_TRUE(res.committed);
    EXPECT_FALSE(res.committed->isResident());
    EXPECT_FALSE(res.committed->isTempNonExistentItem());
}

class EPBucketTestNoRocksDb : public EPBucketTest {
public:
    void SetUp() override {
        EPBucketTest::SetUp();
    }
    void TearDown() override {
        EPBucketTest::TearDown();
    }
};

// Test that scheduling compaction means the current task gets the new config
TEST_P(EPBucketTestNoRocksDb, ScheduleCompactionWithNewConfig) {
    // Store something so the compaction will be success when ran
    store_item(vbid, makeStoredDocKey("key"), "value");
    flushVBucketToDiskIfPersistent(vbid, 1);
    auto* mockEPBucket = dynamic_cast<MockEPBucket*>(engine->getKVBucket());
    auto task = mockEPBucket->getCompactionTask(vbid);
    EXPECT_FALSE(task);

    CompactionConfig c;
    EXPECT_EQ(cb::engine_errc::would_block,
              mockEPBucket->scheduleCompaction(
                      vbid, c, nullptr, std::chrono::seconds(0)));
    task = mockEPBucket->getCompactionTask(vbid);
    ASSERT_TRUE(task);
    EXPECT_EQ(c, task->getCurrentConfig());

    c.purge_before_ts = 100;
    EXPECT_EQ(cb::engine_errc::would_block,
              mockEPBucket->scheduleCompaction(
                      vbid, c, nullptr, std::chrono::seconds(0)));
    EXPECT_EQ(c, task->getCurrentConfig());

    EXPECT_EQ(cb::engine_errc::would_block,
              mockEPBucket->scheduleCompaction(
                      vbid, c, nullptr, std::chrono::seconds(0)));

    // Now schedule via the 'no config' method, the task's config now takes on
    // the 'internally_requested' flag
    EXPECT_EQ(cb::engine_errc::would_block,
              mockEPBucket->scheduleCompaction(vbid, std::chrono::seconds(0)));
    c.internally_requested = true;
    EXPECT_EQ(c, task->getCurrentConfig());

    // no reschedule needed
    EXPECT_FALSE(task->run());

    task = mockEPBucket->getCompactionTask(vbid);
    EXPECT_FALSE(task);
}

// Test that scheduling compaction means the task which runs, runs with a merged
// configuration that meets all requests.
TEST_P(EPBucketTestNoRocksDb, ScheduleCompactionAndMergeNewConfig) {
    auto* mockEPBucket = dynamic_cast<MockEPBucket*>(engine->getKVBucket());
    // Array of configs to use for each call to schedule, it should result
    // in a config for the run which is the 'merge of all'.
    std::array<CompactionConfig, 5> configs = {{{0, 0, false, false},
                                                {0, 1000, false, false},
                                                {1000, 0, false, false},
                                                {9, 900, false, true},
                                                {9, 900, true, false}}};

    for (const auto& config : configs) {
        EXPECT_EQ(cb::engine_errc::would_block,
                  mockEPBucket->scheduleCompaction(
                          vbid, config, nullptr, std::chrono::seconds(0)));
    }
    auto task = mockEPBucket->getCompactionTask(vbid);
    ASSERT_TRUE(task);
    auto finalConfig = task->getCurrentConfig();

    // Merged values, max for 'purge_before_' and true for the bools
    EXPECT_EQ(1000, finalConfig.purge_before_ts);
    EXPECT_EQ(1000, finalConfig.purge_before_seq);
    EXPECT_TRUE(finalConfig.drop_deletes);
    EXPECT_TRUE(finalConfig.retain_erroneous_tombstones);

    // no reschedule needed
    EXPECT_FALSE(task->run());

    task = mockEPBucket->getCompactionTask(vbid);
    EXPECT_FALSE(task);
}

// Test that scheduling compaction when a task is already running the task
// will reschedule *and* the reschedule picks up the new config.
TEST_P(EPBucketTestNoRocksDb, ScheduleCompactionReschedules) {
    auto* mockEPBucket = dynamic_cast<MockEPBucket*>(engine->getKVBucket());
    auto task = mockEPBucket->getCompactionTask(vbid);
    EXPECT_FALSE(task);

    CompactionConfig config1{100, 1, true, true};
    EXPECT_EQ(cb::engine_errc::would_block,
              mockEPBucket->scheduleCompaction(
                      vbid, config1, nullptr, std::chrono::seconds(0)));
    task = mockEPBucket->getCompactionTask(vbid);
    ASSERT_TRUE(task);
    EXPECT_EQ(config1, task->getCurrentConfig());
    // Now we will manually call run, task has no need to reschedule
    EXPECT_FALSE(task->run());
    task = mockEPBucket->getCompactionTask(vbid);
    EXPECT_FALSE(task); // no task anymore

    // Schedule again
    CompactionConfig config2{200, 2, false, true};
    EXPECT_EQ(cb::engine_errc::would_block,
              mockEPBucket->scheduleCompaction(
                      vbid, config2, nullptr, std::chrono::seconds(0)));
    task = mockEPBucket->getCompactionTask(vbid);
    ASSERT_TRUE(task);
    EXPECT_EQ(config2, task->getCurrentConfig());

    // Set our trigger function - this is invoked in the middle of run after
    // the task has copied the config and logically compaction is running.
    CompactionConfig config3{300, 3, false, false};
    task->setRunningCallback([this, &config3, mockEPBucket]() {
        EXPECT_EQ(cb::engine_errc::would_block,
                  mockEPBucket->scheduleCompaction(
                          vbid, config3, cookie, std::chrono::seconds(0)));
    });

    // Now we will manually call run, returns true means executor to run again.
    EXPECT_TRUE(task->run());

    // Compaction for the cookie not yet executed
    EXPECT_FALSE(mock_cookie_notified(cookie));

    // config3 is now the current config
    EXPECT_EQ(config3, task->getCurrentConfig());

    task->setRunningCallback({});

    // task is now done
    EXPECT_FALSE(task->run());
    EXPECT_FALSE(mockEPBucket->getCompactionTask(vbid));
    // Check that compaction run due to the status code changing, but this won't
    // be success as we haven't compacted anything
    EXPECT_EQ(cb::engine_errc::failed, mock_waitfor_cookie(cookie));
}

class EPBucketTestCouchstore : public EPBucketTest {
public:
    void SetUp() override {
        EPBucketTest::SetUp();
    }
    void TearDown() override {
        EPBucketTest::TearDown();
    }
};

// Relates to MB-43242 where we need to be sure we can trigger compaction
// with arbitrary settings. This test is only functional with couchstore
TEST_P(EPBucketTestCouchstore, CompactionWithPurgeOptions) {
    storeAndDeleteItem(vbid, makeStoredDocKey("key1"), "value");
    storeAndDeleteItem(vbid, makeStoredDocKey("key2"), "value");
    flush_vbucket_to_disk(vbid, 0);
    std::array<CompactionConfig, 3> configs;

    auto vb = store->getVBucket(vbid);

    // purge_before_seq only takes affect if purge_before_ts is set
    configs[0].purge_before_seq = vb->getHighSeqno();
    configs[0].purge_before_ts = ep_real_time() + 86400; // now + 1 day

    configs[1].purge_before_ts = ep_real_time() + 86400; // now + 1 day

    configs[2].drop_deletes = true;

    int ii = 0;
    for (const auto& c : configs) {
        EXPECT_EQ(2, vb->getNumPersistedDeletes());
        engine->scheduleCompaction(vbid, c, cookie);
        auto* mockEPBucket = dynamic_cast<MockEPBucket*>(engine->getKVBucket());
        auto task = mockEPBucket->getCompactionTask(vbid);
        ASSERT_TRUE(task);
        EXPECT_FALSE(task->run());
        // Expect 1 to remain as compaction cannot purge the high-seqno
        EXPECT_EQ(1, vb->getNumPersistedDeletes());

        // Store/delete a new key ready for next test
        storeAndDeleteItem(
                vbid, makeStoredDocKey(std::to_string(ii++)), "value");
        flush_vbucket_to_disk(vbid, 0);
    }
}

struct BFilterPrintToStringCombinedName {
    std::string
    operator()(const ::testing::TestParamInfo<
               ::testing::tuple<std::string, std::string, bool>>& info) const {
        std::string bfilter = "_bfilter_enabled";
        if (!std::get<2>(info.param)) {
            bfilter = "_bfilter_disabled";
        }
        return std::get<0>(info.param) + "_" + std::get<1>(info.param) +
               bfilter;
    }
};

// Test cases which run in both Full and Value eviction
INSTANTIATE_TEST_SUITE_P(
        FullAndvalueEviction,
        EPBucketTest,
        STParameterizedBucketTest::persistentAllBackendsConfigValues(),
        STParameterizedBucketTest::PrintToStringParamName);

// Test cases which run only for Full eviction
INSTANTIATE_TEST_SUITE_P(
        FullEviction,
        EPBucketFullEvictionTest,
        STParameterizedBucketTest::fullEvictionAllBackendsConfigValues(),
        STParameterizedBucketTest::PrintToStringParamName);

// Test cases which run only for Full eviction with bloom filters disabled
INSTANTIATE_TEST_SUITE_P(
        FullEviction,
        EPBucketFullEvictionNoBloomFilterTest,
        STParameterizedBucketTest::fullEvictionAllBackendsConfigValues(),
        STParameterizedBucketTest::PrintToStringParamName);

// Test cases which run in both Full and Value eviction, and with bloomfilter
// on and off.
INSTANTIATE_TEST_SUITE_P(
        FullAndValueEvictionBloomOnOff,
        EPBucketBloomFilterParameterizedTest,
        EPBucketBloomFilterParameterizedTest::allConfigValues(),
        BFilterPrintToStringCombinedName());

INSTANTIATE_TEST_SUITE_P(EPBucketTestNoRocksDb,
                         EPBucketTestNoRocksDb,
                         STParameterizedBucketTest::persistentConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(EPBucketTestCouchstore,
                         EPBucketTestCouchstore,
                         STParameterizedBucketTest::couchstoreConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);
