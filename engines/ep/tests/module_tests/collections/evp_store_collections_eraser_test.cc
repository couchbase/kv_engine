/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "checkpoint_manager.h"
#include "collections/collection_persisted_stats.h"
#include "collections/vbucket_manifest_handles.h"
#include "durability/active_durability_monitor.h"
#include "ephemeral_tombstone_purger.h"
#include "ephemeral_vb.h"
#include "item.h"
#include "kvstore/kvstore.h"
#include "programs/engine_testapp/mock_cookie.h"
#include "programs/engine_testapp/mock_server.h"
#include "tests/mock/mock_couch_kvstore.h"
#include "tests/mock/mock_ep_bucket.h"
#include "tests/mock/mock_magma_kvstore.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/collections/collections_test_helpers.h"
#include "tests/module_tests/collections/stat_checker.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"
#include "tests/module_tests/test_helpers.h"
#include "tests/module_tests/vbucket_utils.h"
#include "vbucket_state.h"
#include <utilities/test_manifest.h>

class CollectionsEraserTest : public STParameterizedBucketTest {
public:
    void SetUp() override {
        // A few of the tests in this test suite (in particular the ones that
        // care about document counting) require
        // "magma_sync_every_batch" to be set to true. In this
        // particular case it ensures that we visit older (stale) values that
        // may still exist during compaction.
#ifdef EP_USE_MAGMA
        config_string += magmaRollbackConfig;
#endif

        STParameterizedBucketTest::SetUp();
        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
        vb = store->getVBucket(vbid);
    }

    void TearDown() override {
        vb.reset();
        SingleThreadedKVBucketTest::TearDown();
    }

    /// Override so we recreate the vbucket for ephemeral tests
    void resetEngineAndWarmup() {
        SingleThreadedKVBucketTest::resetEngineAndWarmup();
        if (!persistent()) {
            // Persistent will recreate the VB from the disk metadata so for
            // ephemeral do an explicit set state.
            EXPECT_EQ(cb::engine_errc::success,
                      store->setVBucketState(vbid, vbucket_state_active));
        }
    }

    /**
     * Test that we track the collection purged items stat correctly when we
     * purge a collection
     *
     * @param cid CollectionID to drop
     * @param seqnoOffset seqno to offset expectations by
     */
    void testCollectionPurgedItemsCorrectAfterDrop(
            CollectionsManifest& cm,
            CollectionEntry::Entry collection,
            int64_t seqnoOffset = 0);

    /**
     * Test that we track the scope purged items stat correctly when we
     * purge a scope
     * @param cm reference to the CollectionsManifest of the vbucket
     * @param scope ScopeId to drop
     * @param seqnoOffset seqno to offset expectations by
     */
    void testScopePurgedItemsCorrectAfterDrop(CollectionsManifest& cm,
                                              ScopeEntry::Entry scope,
                                              CollectionEntry::Entry collection,
                                              int64_t seqnoOffset = 0);

    /**
     * Setup for expiry resurrection tests that persists the item given to an
     * old generation collection then resurrects the collection (same cid)
     *
     * @param item Item to persist (should have expiry set by caller)
     */
    void expiryResurrectionTestSetup(Item& item);

    VBucketPtr vb;
};

// Small numbers of items for easier debug
TEST_P(CollectionsEraserTest, basic) {
    // add a collection
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm));

    flushVBucketToDiskIfPersistent(vbid, 1 /* 1 x system */);

    // add some items
    store_item(vbid, StoredDocKey{"milk", CollectionEntry::dairy}, "nice");
    store_item(vbid, StoredDocKey{"butter", CollectionEntry::dairy}, "lovely");

    // Additional checks relating to stat fixes for MB-41321. Prior to flushing,
    // the getStatsForFlush function shall find the collection, it is has no
    // items (no flush yet), it does have some disk size as the system event was
    // flushed. Note ephemeral can also make this call because the dairy
    // collection is open (in KV only the flusher thread calls this)
    auto stats =
            vb->lockCollections().getStatsForFlush(CollectionEntry::dairy, 1);
    if (persistent()) {
        EXPECT_EQ(0, stats.itemCount);
        EXPECT_NE(0, stats.diskSize);
    } else {
        EXPECT_EQ(2, stats.itemCount);
        EXPECT_EQ(0, stats.diskSize);
    }
    size_t diskSize = stats.diskSize;

    flushVBucketToDiskIfPersistent(vbid, 2 /* 2 x items */);

    if (persistent()) {
        // Now the flusher will have calculated the items/diskSize
        auto flushStats = vb->lockCollections().getStatsForFlush(
                CollectionEntry::dairy, 1);
        EXPECT_EQ(2, flushStats.itemCount);
        EXPECT_GT(flushStats.diskSize, diskSize);
        diskSize = flushStats.diskSize;
    }

    EXPECT_EQ(2, vb->getNumItems());

    // Evict one of the keys, we should still erase it
    if (persistent()) {
        evict_key(vbid, StoredDocKey{"butter", CollectionEntry::dairy});
    }

    // Also store an item into the same batch as the drop
    store_item(vbid, StoredDocKey{"cheese", CollectionEntry::dairy}, "blue");

    // delete the collection
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::dairy)));

    if (!persistent()) {
        EXPECT_EQ(1, vb->getNumSystemItems());
    }

    if (persistent()) {
        // Now the collection is dropped, we should still be able to obtain the
        // stats the flusher can continue flushing any items the proceed the
        // drop and still make correct updates
        auto flushStats = vb->lockCollections().getStatsForFlush(
                CollectionEntry::dairy, 1);
        EXPECT_EQ(2, flushStats.itemCount);
        EXPECT_EQ(diskSize, flushStats.diskSize);
    }

    flushVBucketToDiskIfPersistent(vbid, 2 /* 1 x system, 1 x item */);

    // Now the drop is persisted the stats have gone
    EXPECT_THROW(
            vb->lockCollections().getStatsForFlush(CollectionEntry::dairy, 1),
            std::logic_error);

    // Collection is deleted
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));

    // And compaction was triggered (throws if not)
    runCollectionsEraser(vbid);

    EXPECT_EQ(0, vb->getNumItems());

    // @todo MB-26334: persistent buckets don't track the system event counts
    if (!persistent()) {
        // The system event still exists as a tombstone and will reside in the
        // system until tombstone purging removes it.
        EXPECT_EQ(1, vb->getNumSystemItems());
    }
}

TEST_P(CollectionsEraserTest, basic_2_collections) {
    // add two collections
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::fruit)));

    flushVBucketToDiskIfPersistent(vbid, 2 /* 2 x system */);

    // add some items
    store_item(
            vbid, StoredDocKey{"dairy:milk", CollectionEntry::dairy}, "nice");
    store_item(vbid,
               StoredDocKey{"dairy:butter", CollectionEntry::dairy},
               "lovely");
    store_item(
            vbid, StoredDocKey{"fruit:apple", CollectionEntry::fruit}, "nice");
    store_item(vbid,
               StoredDocKey{"fruit:apricot", CollectionEntry::fruit},
               "lovely");

    flushVBucketToDiskIfPersistent(vbid, 4);

    EXPECT_EQ(4, vb->getNumItems());

    // delete the collections
    vb->updateFromManifest(makeManifest(
            cm.remove(CollectionEntry::dairy).remove(CollectionEntry::fruit)));

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));

    flushVBucketToDiskIfPersistent(vbid, 2 /* 2 x system */);

    runCollectionsEraser(vbid);

    EXPECT_EQ(0, vb->getNumItems());

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));
}

TEST_P(CollectionsEraserTest, basic_3_collections) {
    // Add two collections
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::fruit)));

    flushVBucketToDiskIfPersistent(vbid, 2 /* 1x system */);

    // add some items
    store_item(
            vbid, StoredDocKey{"dairy:milk", CollectionEntry::dairy}, "nice");
    store_item(vbid,
               StoredDocKey{"dairy:butter", CollectionEntry::dairy},
               "lovely");
    store_item(
            vbid, StoredDocKey{"fruit:apple", CollectionEntry::fruit}, "nice");
    store_item(vbid,
               StoredDocKey{"fruit:apricot", CollectionEntry::fruit},
               "lovely");

    flushVBucketToDiskIfPersistent(vbid, 4 /* 2x items */);

    EXPECT_EQ(4, vb->getNumItems());

    // delete one of the 3 collections
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::fruit)));

    EXPECT_TRUE(vb->lockCollections().exists(CollectionEntry::dairy));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));

    flushVBucketToDiskIfPersistent(vbid, 1 /* 1 x system */);

    runCollectionsEraser(vbid);

    EXPECT_EQ(2, vb->getNumItems());

    EXPECT_TRUE(vb->lockCollections().exists(CollectionEntry::dairy));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));
}

TEST_P(CollectionsEraserTest, basic_4_collections) {
    // Add two collections
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::fruit)));

    flushVBucketToDiskIfPersistent(vbid, 2 /* 1x system */);

    // add some items
    store_item(
            vbid, StoredDocKey{"dairy:milk", CollectionEntry::dairy}, "nice");
    store_item(vbid,
               StoredDocKey{"dairy:butter", CollectionEntry::dairy},
               "lovely");
    store_item(
            vbid, StoredDocKey{"fruit:apple", CollectionEntry::fruit}, "nice");
    store_item(vbid,
               StoredDocKey{"fruit:apricot", CollectionEntry::fruit},
               "lovely");

    flushVBucketToDiskIfPersistent(vbid, 4 /* 2x items */);

    // delete the collections and re-add a new dairy
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::fruit)
                                                .remove(CollectionEntry::dairy)
                                                .add(CollectionEntry::dairy2)));

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));
    EXPECT_TRUE(vb->lockCollections().exists(CollectionEntry::dairy2));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));

    flushVBucketToDiskIfPersistent(vbid,
                                   3 /* 3x system (2 deletes, 1 create) */);

    runCollectionsEraser(vbid);

    EXPECT_EQ(0, vb->getNumItems());

    EXPECT_TRUE(vb->lockCollections().exists(CollectionEntry::dairy2));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));
}

TEST_P(CollectionsEraserTest, default_Destroy) {
    // add some items
    store_item(vbid,
               StoredDocKey{"dairy:milk", CollectionEntry::defaultC},
               "nice");
    store_item(vbid,
               StoredDocKey{"dairy:butter", CollectionEntry::defaultC},
               "lovely");
    store_item(vbid,
               StoredDocKey{"fruit:apple", CollectionEntry::defaultC},
               "nice");
    store_item(vbid,
               StoredDocKey{"fruit:apricot", CollectionEntry::defaultC},
               "lovely");

    flushVBucketToDiskIfPersistent(vbid, 4);

    EXPECT_EQ(4, vb->getNumItems());

    // delete the default collection
    CollectionsManifest cm;
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::defaultC)));

    flushVBucketToDiskIfPersistent(vbid, 1 /* 1 x system */);

    runCollectionsEraser(vbid);

    EXPECT_EQ(0, vb->getNumItems());

    // Add default back - so we don't get collection unknown errors
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::defaultC)));

    auto key1 = makeStoredDocKey("dairy:milk", CollectionEntry::defaultC);
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);

    EXPECT_EQ(cb::engine_errc::no_such_key,
              checkKeyExists(key1, vbid, options));
}

// Test that following a full drop (compaction completes the deletion), warmup
// reloads the VB::Manifest and the dropped collection stays dropped.
TEST_P(CollectionsEraserTest, erase_and_reset) {
    // Add two collections
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::fruit)));

    flushVBucketToDiskIfPersistent(vbid, 2 /* 1x system */);

    // add some items
    store_item(
            vbid, StoredDocKey{"dairy:milk", CollectionEntry::dairy}, "nice");
    store_item(vbid,
               StoredDocKey{"dairy:butter", CollectionEntry::dairy},
               "lovely");
    store_item(
            vbid, StoredDocKey{"fruit:apple", CollectionEntry::fruit}, "nice");
    store_item(vbid,
               StoredDocKey{"fruit:apricot", CollectionEntry::fruit},
               "lovely");

    flushVBucketToDiskIfPersistent(vbid, 4 /* 2x items */);

    // delete the collections and re-add a new dairy
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::fruit)
                                                .remove(CollectionEntry::dairy)
                                                .add(CollectionEntry::dairy2)));

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));
    EXPECT_TRUE(vb->lockCollections().exists(CollectionEntry::dairy2));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));

    flushVBucketToDiskIfPersistent(vbid,
                                   3 /* 3x system (2 deletes, 1 create) */);

    runCollectionsEraser(vbid);

    EXPECT_EQ(0, vb->getNumItems());

    EXPECT_TRUE(vb->lockCollections().exists(CollectionEntry::dairy2));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));

    vb.reset();
    resetEngineAndWarmup();

    if (!persistent()) {
        // The final expects only apply to persistent buckets that will remember
        // the collection state
        return;
    }

    // Now reset and warmup and expect the manifest to come back with the same
    // correct view of collections
    vb = store->getVBucket(vbid);
    EXPECT_TRUE(vb->lockCollections().exists(CollectionEntry::dairy2));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));
}

// Small numbers of items for easier debug
TEST_P(CollectionsEraserTest, basic_deleted_items) {
    // add a collection
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm));

    flushVBucketToDiskIfPersistent(vbid, 1 /* 1 x system */);

    // add some items
    store_item(
            vbid, StoredDocKey{"dairy:milk", CollectionEntry::dairy}, "nice");
    store_item(vbid,
               StoredDocKey{"dairy:butter", CollectionEntry::dairy},
               "lovely");
    delete_item(vbid, StoredDocKey{"dairy:butter", CollectionEntry::dairy});

    flushVBucketToDiskIfPersistent(vbid, 2 /* 2 x items */);

    EXPECT_EQ(1, vb->getNumItems());

    // delete the collection
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::dairy)));

    // @todo MB-26334: persistent buckets don't track the system event counts
    if (!persistent()) {
        EXPECT_EQ(1, vb->getNumSystemItems());
    }

    flushVBucketToDiskIfPersistent(vbid, 1 /* 1 x system */);

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));

    runCollectionsEraser(vbid);

    EXPECT_EQ(0, vb->getNumItems());

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));

    // @todo MB-26334: persistent buckets don't track the system event counts
    if (!persistent()) {
        // The system event still exists as a tombstone and will reside in the
        // system until tombstone purging removes it.
        EXPECT_EQ(1, vb->getNumSystemItems());
    }
}

// Small numbers of items for easier debug
TEST_P(CollectionsEraserTest, tombstone_cleaner) {
    // add a collection
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm));

    flushVBucketToDiskIfPersistent(vbid, 1 /* 1 x system */);

    // add some items
    store_item(
            vbid, StoredDocKey{"dairy:milk", CollectionEntry::dairy}, "nice");
    store_item(vbid,
               StoredDocKey{"dairy:butter", CollectionEntry::dairy},
               "lovely");

    flushVBucketToDiskIfPersistent(vbid, 2 /* 2 x items */);

    EXPECT_EQ(2, vb->getNumItems());

    // delete the collection
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::dairy)));

    // @todo MB-26334: persistent buckets don't track the system event counts
    if (!persistent()) {
        EXPECT_EQ(1, vb->getNumSystemItems());
    }

    flushVBucketToDiskIfPersistent(vbid, 1 /* 1 x system */);

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));

    runCollectionsEraser(vbid);

    EXPECT_EQ(0, vb->getNumItems());

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));

    // @todo MB-26334: persistent buckets don't track the system event counts
    if (!persistent()) {
        // The system event still exists as a tombstone and will reside in the
        // system until tombstone purging removes it.
        EXPECT_EQ(1, vb->getNumSystemItems());
    }

    // We're gonna have to kick ephemeral a bit to mark the collection tombstone
    // as stale. Travel forward in time then run the HTTombstonePurger.
    TimeTraveller c(10000000);
    if (!persistent()) {
        EphemeralVBucket::HTTombstonePurger purger(0);
        auto vbptr = store->getVBucket(vbid);
        auto* evb = dynamic_cast<EphemeralVBucket*>(vbptr.get());
        purger.setCurrentVBucket(*evb);
        evb->ht.visit(purger);
    }

    // Now that we've run the tasks, we won't have any system events in the hash
    // table. The collection drop system event should still exist in the backing
    // store because it's the last item, but it won't be accounted for in the
    // NumSystemItems stat that looks at the hash table.
    EXPECT_EQ(0, vb->getNumSystemItems());
}

// Test that a collection erase "resumes" after a restart/warmup
TEST_P(CollectionsEraserTest, erase_after_warmup) {
    if (!persistent()) {
        return;
    }

    // add a collection
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm));

    flushVBucketToDiskIfPersistent(vbid, 1 /* 1 x system */);

    // add some items
    store_item(
            vbid, StoredDocKey{"dairy:milk", CollectionEntry::dairy}, "nice");
    store_item(vbid,
               StoredDocKey{"dairy:butter", CollectionEntry::dairy},
               "lovely");

    flushVBucketToDiskIfPersistent(vbid, 2 /* 2 x items */);

    EXPECT_EQ(2, vb->getNumItems());

    // Evict one of the keys, we should still erase it
    evict_key(vbid, StoredDocKey{"dairy:butter", CollectionEntry::dairy});

    // delete the collection
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::dairy)));

    flushVBucketToDiskIfPersistent(vbid, 1 /* 1 x system */);
    vb.reset();

    store->cancelCompaction(vbid);
    resetEngineAndWarmup();

    auto [status, dropped] = store->getVBucket(vbid)
                                     ->getShard()
                                     ->getRWUnderlying()
                                     ->getDroppedCollections(vbid);
    ASSERT_TRUE(status);
    EXPECT_FALSE(dropped.empty());

    // Now the eraser should ready to run, warmup will have noticed a dropped
    // collection in the manifest and schedule the eraser
    runCollectionsEraser(vbid);
    vb = store->getVBucket(vbid);
    EXPECT_EQ(0, vb->getNumItems());
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));
}

/*
 * Test to ensure we don't self assign highestPurgedDeletedSeqno when we purge a
 * seqno less than highestPurgedDeletedSeqno in BasicLinkedList::purgeListElem()
 * as this would break the monotonic property of highestPurgedDeletedSeqno.
 *
 * To do this, this test performs the following steps:
 * 1. Create collection 1
 * 2. Create items for collection 1
 * 3. Create collection 2
 * 4. Create items for collection 2
 * 5. Mark some items of collection 2 as deleted
 * 6. Mark some items of collection 1 as deleted
 * 7. Remove collection 1 from the manifest
 * 8. Purge all items from collection 1
 * 9. Remove collection 2 from the manifest
 * 10. Purge all items from collection 2
 * At this point we would expect the monotonic to throw as we will purge an
 * item with a seqno less than the highest seqno seen in the first purge run.
 */
TEST_P(CollectionsEraserTest, MB_39113) {
    // This is an ephemeral only test.
    if (!ephemeral()) {
        return;
    }
    // add two collections
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm));

    // add some items
    store_item(
            vbid, StoredDocKey{"dairy:milk", CollectionEntry::dairy}, "nice");
    store_item(vbid,
               StoredDocKey{"dairy:butter", CollectionEntry::dairy},
               "lovely");

    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::fruit)));
    store_item(
            vbid, StoredDocKey{"fruit:apple", CollectionEntry::fruit}, "nice");
    store_item(vbid,
               StoredDocKey{"fruit:apricot", CollectionEntry::fruit},
               "lovely");

    EXPECT_EQ(4, vb->getNumItems());

    delete_item(vbid, StoredDocKey{"fruit:apple", CollectionEntry::fruit});
    delete_item(vbid, StoredDocKey{"fruit:apricot", CollectionEntry::fruit});

    delete_item(vbid, StoredDocKey{"dairy:milk", CollectionEntry::dairy});
    delete_item(vbid, StoredDocKey{"dairy:butter", CollectionEntry::dairy});

    EXPECT_EQ(0, vb->getNumItems());

    // delete the collections
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::dairy)));

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));
    EXPECT_TRUE(vb->lockCollections().exists(CollectionEntry::fruit));

    // Purge deleted items with higher seqnos
    EXPECT_NO_THROW(runCollectionsEraser(vbid););

    EXPECT_EQ(0, vb->getNumItems());

    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::fruit)));

    // Purge deleted items with lower seqnos
    EXPECT_NO_THROW(runCollectionsEraser(vbid););

    EXPECT_EQ(0, vb->getNumItems());

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));
}

// MB found that if compaction is scheduled for a collection drop and the
// vbucket is remove, a gsl not_null exception occurs as we enter some code
// that expects a cookie, but the collection purge trigger has no cookie.
TEST_P(CollectionsEraserTest, MB_38313) {
    if (!persistent()) {
        return;
    }
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm));

    // add some items
    store_item(vbid, StoredDocKey{"milk", CollectionEntry::dairy}, "nice");

    flushVBucketToDiskIfPersistent(vbid, 2 /* 1x system 2x items */);

    // delete the collection
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::dairy)));
    flushVBucketToDiskIfPersistent(vbid, 1 /* 1 x system */);

    // A drop sets compaction with a future run time, poke it forwards
    store->scheduleCompaction(vbid, {}, nullptr, std::chrono::seconds(0));

    // Remove vbucket
    store->deleteVBucket(vbid, nullptr);

    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "Compact DB file 0"); // would fault (gsl exception)
}

// Test reproduces an issue seen in MB-50747 where MagmaKVStore skipped updating
// the item count when the dairy collection drops
TEST_P(CollectionsEraserTest, MB_50747_ItemCountOvercount) {
    // Ephemeral doesn't have comparable compaction so not valid to test here
    if (!persistent()) {
        return;
    }

    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto* kvStore = store->getRWUnderlying(vbid);
    EXPECT_EQ(1, kvStore->getItemCount(vbid));

    // Do one SyncWrite and one normal
    auto key = StoredDocKey{"butter1", CollectionEntry::dairy};
    auto item = makePendingItem(key, "nice");
    EXPECT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));
    store_item(vbid, StoredDocKey{"butter2", CollectionEntry::dairy}, "nice");
    flushVBucketToDiskIfPersistent(vbid, 2);

    // Magma doesn't count prepares
    size_t prepareCount = isMagma() ? 0 : 1;

    // system event + key
    EXPECT_EQ(2 + prepareCount, kvStore->getItemCount(vbid));

    // This will reproduce the conditions when magma is the backend. Both keys
    // will end up in the same sstable and compaction would purge both keys
    // from the prepare namespace purge, meaning the explicit purge of dairy
    // skipped an item count update
    runCompaction(vbid);

    if (isMagma()) {
        std::array<std::string_view, 1> keys = {{"magma_KeyIndexNTableFiles"}};
        auto storeStats = kvStore->getStats(keys);
        ASSERT_EQ(1, storeStats.count("magma_KeyIndexNTableFiles"));
        EXPECT_EQ(1, storeStats["magma_KeyIndexNTableFiles"]);
    }

    // Drop the collection
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::dairy)));
    flushVBucketToDiskIfPersistent(vbid, 1 /* 1 x system */);

    runCollectionsEraser(vbid);

    // Item count should now be zero
    EXPECT_EQ(0, kvStore->getItemCount(vbid));
}

TEST_P(CollectionsEraserTest, PrepareCountCorrectAfterErase) {
    // Ephemeral doesn't have comparable compaction so not valid to test here
    if (!persistent()) {
        return;
    }

    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto* kvStore = store->getRWUnderlying(vbid);
    EXPECT_EQ(1, kvStore->getItemCount(vbid));

    // Do our SyncWrite
    auto key = StoredDocKey{"milk", CollectionEntry::dairy};
    auto item = makePendingItem(key, "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    if (isMagma()) {
        // Magma does not track prepares in doc count
        EXPECT_EQ(1, kvStore->getItemCount(vbid));
    } else {
        EXPECT_EQ(2, kvStore->getItemCount(vbid));
    }

    // And commit it
    EXPECT_EQ(cb::engine_errc::success,
              vb->commit(key,
                         2 /*prepareSeqno*/,
                         {} /*commitSeqno*/,
                         vb->lockCollections(key)));
    flushVBucketToDiskIfPersistent(vbid, 1);

    if (isMagma()) {
        // Magma does not track prepares in doc count
        EXPECT_EQ(2, kvStore->getItemCount(vbid));
    } else {
        EXPECT_EQ(3, kvStore->getItemCount(vbid));
    }

    // Do our SyncWrite
    item = makePendingItem(key, "value2");
    EXPECT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    if (isMagma()) {
        EXPECT_EQ(2, kvStore->getItemCount(vbid));
    } else {
        EXPECT_EQ(3, kvStore->getItemCount(vbid));
    }

    // And commit it
    EXPECT_EQ(cb::engine_errc::success,
              vb->commit(key,
                         4 /*prepareSeqno*/,
                         {} /*commitSeqno*/,
                         vb->lockCollections(key)));
    flushVBucketToDiskIfPersistent(vbid, 1);

    if (isMagma()) {
        EXPECT_EQ(2, kvStore->getItemCount(vbid));
    } else {
        EXPECT_EQ(3, kvStore->getItemCount(vbid));
    }

    if (isMagma()) {
        EXPECT_EQ(2, kvStore->getItemCount(vbid));
    } else {
        EXPECT_EQ(3, kvStore->getItemCount(vbid));
    }

    // delete the collection
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::dairy)));
    flushVBucketToDiskIfPersistent(vbid, 1 /* 1 x system */);

    runCollectionsEraser(vbid);

    // Check doc and prepare counts
    EXPECT_EQ(0, kvStore->getItemCount(vbid));

    auto state = kvStore->getCachedVBucketState(vbid);
    ASSERT_TRUE(state);
    EXPECT_EQ(0, state->onDiskPrepares);

    vb.reset();

    // Warmup would have triggered an underflow when we set the VBucket doc
    // count before we stopped tracking number of prepares for magma.
    resetEngineAndWarmup();
}

void CollectionsEraserTest::testCollectionPurgedItemsCorrectAfterDrop(
        CollectionsManifest& cm,
        CollectionEntry::Entry collection,
        int64_t seqnoOffset) {
    auto key = StoredDocKey{"milk", collection};
    store_item(vbid, key, "nice");
    flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(vbid, key, "nice2");
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Need to update manifest uid so that we don't fail to flush due to going
    // backwards
    cm.updateUid(4);
    vb->updateFromManifest(makeManifest(cm.remove(collection)));
    flushVBucketToDiskIfPersistent(vbid, 1 /* 1 x system */);

    if (isPersistent()) {
        auto* bucket = dynamic_cast<MockEPBucket*>(engine->getKVBucket());
        using namespace testing;

        // Magma will make two calls here. One for the first value that
        // logically does not exist anymore, and one for the updated value.
        if (isMagma()) {
            EXPECT_CALL(*bucket,
                        dropKey(Ref(*bucket->getVBucket(vbid)),
                                DiskDocKey(key),
                                1 + seqnoOffset,
                                false /*isAbort*/,
                                0 /*PCS*/))
                    .RetiresOnSaturation();
        }

        EXPECT_CALL(*bucket,
                    dropKey(Ref(*bucket->getVBucket(vbid)),
                            DiskDocKey(key),
                            2 + seqnoOffset,
                            false /*isAbort*/,
                            0 /*PCS*/))
                .RetiresOnSaturation();
    }
    EXPECT_EQ(1, vb->getNumItems());
    EXPECT_EQ(0, vb->getPurgeSeqno());

    runCollectionsEraser(vbid);

    EXPECT_EQ(0, vb->getNumItems());
    EXPECT_EQ(0, vb->getPurgeSeqno());

    // The warmup will definitely fail if this isn't true
    auto kvStore = store->getRWUnderlying(vbid);

    if (isPersistent()) {
        ASSERT_EQ(0, kvStore->getItemCount(vbid));
    }

    vb.reset();

    resetEngineAndWarmup();

    // Reset the vBucket as the ptr will now be bad
    vb = store->getVBucket(vbid);
}

TEST_P(CollectionsEraserTest, CollectionPurgedItemsCorrectAfterDropDefaultC) {
    CollectionsManifest cm;
    testCollectionPurgedItemsCorrectAfterDrop(cm, CollectionEntry::defaultC);
}

TEST_P(CollectionsEraserTest, CollectionPurgedItemsCorrectAfterDrop) {
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 1);

    testCollectionPurgedItemsCorrectAfterDrop(
            cm, CollectionEntry::dairy, 1 /*seqnoOffset*/);
}

TEST_P(CollectionsEraserTest, DropEmptyCollection) {
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 1);

    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::dairy)));
    flushVBucketToDiskIfPersistent(vbid, 1 /* 1 x system */);

    if (persistent()) {
        // Empty collection will not schedule compaction
        EXPECT_EQ(0, getFutureQueueSize(AUXIO_TASK_IDX));
        EXPECT_EQ(0, getReadyQueueSize(AUXIO_TASK_IDX));
    } else {
        runCollectionsEraser(vbid);
    }
}

void CollectionsEraserTest::testScopePurgedItemsCorrectAfterDrop(
        CollectionsManifest& cm,
        ScopeEntry::Entry scope,
        CollectionEntry::Entry collection,
        int64_t seqnoOffset) {
    auto key = StoredDocKey{"milk", collection};
    store_item(vbid, key, "nice");
    flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(vbid, key, "nice2");
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Need to update manifest uid so that we don't fail to flush due to going
    // backwards
    vb->updateFromManifest(makeManifest(cm.remove(scope)));
    flushVBucketToDiskIfPersistent(
            vbid, 2 /* 1 x scope drop and 1 x collection drop */);

    if (isPersistent()) {
        auto* bucket = dynamic_cast<MockEPBucket*>(engine->getKVBucket());
        using namespace testing;

        // Magma will make two calls here. One for the first value that
        // logically does not exist anymore, and one for the updated value.
        if (isMagma()) {
            EXPECT_CALL(*bucket,
                        dropKey(Ref(*bucket->getVBucket(vbid)),
                                DiskDocKey(key),
                                1 + seqnoOffset,
                                false /*isAbort*/,
                                0 /*PCS*/))
                    .RetiresOnSaturation();
        }

        EXPECT_CALL(*bucket,
                    dropKey(Ref(*bucket->getVBucket(vbid)),
                            DiskDocKey(key),
                            2 + seqnoOffset,
                            false /*isAbort*/,
                            0 /*PCS*/))
                .RetiresOnSaturation();
    }
    EXPECT_EQ(1, vb->getNumItems());
    EXPECT_EQ(0, vb->getPurgeSeqno());

    runCollectionsEraser(vbid);

    EXPECT_EQ(0, vb->getNumItems());
    EXPECT_EQ(0, vb->getPurgeSeqno());

    // The warmup will definitely fail if this isn't true
    auto kvStore = store->getRWUnderlying(vbid);

    if (isPersistent()) {
        ASSERT_EQ(0, kvStore->getItemCount(vbid));
    }

    vb.reset();

    resetEngineAndWarmup();

    // Reset the vBucket as the ptr will now be bad
    vb = store->getVBucket(vbid);
}

TEST_P(CollectionsEraserTest, ScopePurgedItemsCorrectAfterDrop) {
    CollectionsManifest cm;
    cm.add(ScopeEntry::shop1);
    cm.add(CollectionEntry::dairy, cb::ExpiryLimit{0}, ScopeEntry::shop1);
    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 2);

    testScopePurgedItemsCorrectAfterDrop(
            cm, ScopeEntry::shop1, CollectionEntry::dairy, 2 /*seqnoOffset*/);
}

void CollectionsEraserTest::expiryResurrectionTestSetup(Item& item) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Add the item that we want to expire
    auto key = makeStoredDocKey("key", CollectionEntry::dairy);
    auto storedItem = store_item(vbid, key, "nice", 10 /*expiryTime*/);
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Drop the old gen collection
    cm.remove(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Add new gen
    cm.add(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 1);
}

TEST_P(CollectionsEraserTest, FetchValidValueExpiryResurrectionTest) {
    auto key = makeStoredDocKey("key", CollectionEntry::dairy);
    auto item = make_item(vbid, key, "nice", 10 /*expiryTime*/);
    expiryResurrectionTestSetup(item);

    auto expectedHighSeqno = vb->getHighSeqno();
    EXPECT_EQ(0, vb->numExpiredItems);

    // And run the expiry which should not generate a new seqno
    {
        auto cHandle = vb->lockCollections(key);
        auto result = vb->fetchValidValue(WantsDeleted::Yes,
                                          TrackReference::Yes,
                                          cHandle);
        EXPECT_FALSE(result.storedValue);
    }

    EXPECT_EQ(expectedHighSeqno, vb->getHighSeqno());
    EXPECT_EQ(0, vb->numExpiredItems);
}

TEST_P(CollectionsEraserTest, DeleteExpiryResurrectionTest) {
    // Add the item that we want to expire
    auto key = makeStoredDocKey("key", CollectionEntry::dairy);
    auto item = make_item(vbid, key, "nice", 10 /*expiryTime*/);
    expiryResurrectionTestSetup(item);

    auto expectedHighSeqno = vb->getHighSeqno();

    // And run the expiry which should not generate a new seqno. Importantly it
    // should return ENOENT as the item it found belongs to a logically deleted
    // collection and it should NOT try to expire it.
    uint64_t cas = item.getCas();
    mutation_descr_t mutation_descr;
    ASSERT_EQ(cb::engine_errc::no_such_key,
              store->deleteItem(key,
                                cas,
                                vbid,
                                /*cookie*/ nullptr,
                                {},
                                /*itemMeta*/ nullptr,
                                mutation_descr));
    EXPECT_EQ(expectedHighSeqno, vb->getHighSeqno());
}

class CollectionsEraserSyncWriteTest : public CollectionsEraserTest {
public:
    void SetUp() override {
        CollectionsEraserTest::SetUp();
        setVBucketStateAndRunPersistTask(
                vbid,
                vbucket_state_active,
                {{"topology", nlohmann::json::array({{"active", "replica"}})}});
    }

    void TearDown() override {
        CollectionsEraserTest::TearDown();
    }

    void dropEraseAndVerify(uint64_t expectedTrackedWrites,
                            uint64_t expectedHPS,
                            uint64_t expectedHCS) {
        auto& adm = VBucketTestIntrospector::public_getActiveDM(*vb);

        dropCollection();
        EXPECT_EQ(1, adm.getNumTracked());

        runCollectionsEraser(vbid);

        EXPECT_EQ(0, adm.getNumTracked());
        EXPECT_EQ(expectedHPS, adm.getHighPreparedSeqno());
        EXPECT_EQ(expectedHCS, adm.getHighCompletedSeqno());

        // At the end of the test expect for persistent to have nothing in the
        // hash-table, except for ephemeral which leaves the tombstone of the
        // collection system event
        if (!persistent()) {
            EXPECT_EQ(1, vb->ht.getNumInMemoryItems());
        } else {
            EXPECT_EQ(0, vb->ht.getNumInMemoryItems());
        }

        // No items remain for all bucket types
        EXPECT_EQ(0, vb->getNumItems());
        if (persistent()) {
            // Collection should be purged from KVStore
            auto [status, manifest] = store->getVBucket(vbid)
                                              ->getShard()
                                              ->getRWUnderlying()
                                              ->getCollectionsManifest(vbid);
            ASSERT_TRUE(status);
            auto itr = manifest.collections.begin();
            for (; itr != manifest.collections.end(); itr++) {
                if (itr->metaData.cid == CollectionEntry::dairy.getId()) {
                    break;
                }
            }
            EXPECT_EQ(itr, manifest.collections.end());
        }
    }

    /**
     * Run a test which drops a collection that has a sync write in it
     */
    void basicDropWithSyncWrite();

protected:
    void addCollection() {
        cm.add(CollectionEntry::dairy);
        vb->updateFromManifest(makeManifest(cm));
        flushVBucketToDiskIfPersistent(vbid, 1);
    }

    void dropCollection() {
        cm.remove(CollectionEntry::dairy);
        vb->updateFromManifest(makeManifest(cm));
        flushVBucketToDiskIfPersistent(vbid, 1);
    }

    void createPendingWrite(bool deleted = false) {
        auto persistedHighSeqno = vb->lockCollections().getPersistedHighSeqno(
                key.getCollectionID());
        auto getOnDiskPrepares = [this]() {
            auto* vbstate =
                    store->getOneRWUnderlying()->getCachedVBucketState(vbid);
            return vbstate->onDiskPrepares;
        };

        int initPendingDocOnDisk = 0;
        if (persistent() && !isMagma()) {
            initPendingDocOnDisk = getOnDiskPrepares();
        }

        if (deleted) {
            auto item = makeCommittedItem(key, "value");
            EXPECT_EQ(cb::engine_errc::success, store->set(*item, cookie));
            flushVBucketToDiskIfPersistent(vbid, 1);
            // Delete changed the stats
            persistedHighSeqno = vb->lockCollections().getPersistedHighSeqno(
                    key.getCollectionID());

            mutation_descr_t delInfo;
            uint64_t cas = item->getCas();
            using namespace cb::durability;
            EXPECT_EQ(cb::engine_errc::sync_write_pending,
                      store->deleteItem(key,
                                        cas,
                                        vbid,
                                        cookie,
                                        Requirements(Level::Majority, {}),
                                        nullptr,
                                        delInfo));
        } else {
            auto item = makePendingItem(key, "value");
            EXPECT_EQ(cb::engine_errc::sync_write_pending,
                      store->set(*item, cookie));
        }
        flushVBucketToDiskIfPersistent(vbid, 1);

        if (persistent() && !isMagma()) {
            EXPECT_EQ(initPendingDocOnDisk + 1, getOnDiskPrepares());
        }

        if (persistent()) {
            // Validate the collection high-persisted-seqno changed for the
            // pending item
            EXPECT_EQ(persistedHighSeqno + 1,
                      vb->lockCollections().getPersistedHighSeqno(
                              key.getCollectionID()));
        }
    }

    void processAck(uint64_t prepareSeqno) {
        vb->seqnoAcknowledged(
                folly::SharedMutex::ReadHolder(vb->getStateLock()),
                "replica",
                prepareSeqno);
    }

    void processResolvedSyncWrites() {
        vb->processResolvedSyncWrites();
    }

    void commit() {
        EXPECT_EQ(cb::engine_errc::success,
                  vb->commit(key,
                             2 /*prepareSeqno*/,
                             {} /*commitSeqno*/,
                             vb->lockCollections(key)));
        flushVBucketToDiskIfPersistent(vbid, 1);

        if (!persistent()) {
            auto pending = vb->ht.findForUpdate(key).pending;
            ASSERT_TRUE(pending);
            EXPECT_EQ(pending->getCommitted(),
                      CommittedState::PrepareCommitted);
        }
    }

    void abort() {
        EXPECT_EQ(cb::engine_errc::success,
                  vb->abort(key,
                            2 /*prepareSeqno*/,
                            {} /*commitSeqno*/,
                            vb->lockCollections(key)));

        flushVBucketToDiskIfPersistent(vbid, 1);
    }

    CollectionsManifest cm{};
    StoredDocKey key = makeStoredDocKey("key", CollectionEntry::dairy);
};

void CollectionsEraserSyncWriteTest::basicDropWithSyncWrite() {
    addCollection();
    createPendingWrite();

    if (!persistent()) {
        auto pending = vb->ht.findForUpdate(key).pending;
        ASSERT_TRUE(pending);
    }

    dropEraseAndVerify(0, 2, 0);

    if (persistent()) {
        // Check prepares and disk count
        auto kvstore = store->getRWUnderlying(vbid);
        ASSERT_TRUE(kvstore);
        EXPECT_EQ(0, kvstore->getItemCount(vbid));

        auto vbstate = kvstore->getCachedVBucketState(vbid);
        ASSERT_TRUE(vbstate);
        EXPECT_EQ(0, vbstate->onDiskPrepares);
    }
}

TEST_P(CollectionsEraserSyncWriteTest, BasicDropWithPendingSyncWrite) {
    basicDropWithSyncWrite();
}

TEST_P(CollectionsEraserSyncWriteTest, BasicDropWithDeletedPendingSyncWrite) {
    addCollection();
    createPendingWrite(true);

    {
        auto pending = vb->ht.findForUpdate(key).pending;
        ASSERT_TRUE(pending);
    }

    dropEraseAndVerify(0, 3, 0);

    if (persistent()) {
        // Check prepares and disk count
        auto kvstore = store->getRWUnderlying(vbid);
        ASSERT_TRUE(kvstore);
        EXPECT_EQ(0, kvstore->getItemCount(vbid));

        auto vbstate = kvstore->getCachedVBucketState(vbid);
        ASSERT_TRUE(vbstate);
        EXPECT_EQ(0, vbstate->onDiskPrepares);
    }
}

TEST_P(CollectionsEraserSyncWriteTest, DropAfterCommit) {
    addCollection();
    createPendingWrite();
    commit();
    dropCollection();

    // This is a dodgy way of testing things but in this test the only prepares
    // we will be dropping are completed and so we should not call into the DM.
    // If we tried to then we'd segfault
    VBucketTestIntrospector::destroyDM(*vb.get());

    runCollectionsEraser(vbid);
}

TEST_P(CollectionsEraserSyncWriteTest, DropPrepareWhileDead) {
    // Test that dropping a prepare does not attempt to use the PDM of a dead
    // vbucket.

    addCollection();
    createPendingWrite();

    dropCollection();

    store->setVBucketState(vb->getId(), vbucket_state_dead);

    auto& lpAuxioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
    runNextTask(lpAuxioQ, "Notify clients of Sync Write Ambiguous vb:0");

    // `Expects` in getPassiveDM() would fail during this call
    runCollectionsEraser(vbid);
}

TEST_P(CollectionsEraserSyncWriteTest, DropAfterAbort) {
    addCollection();
    auto emptySz = vb->getManifest().lock(key.getCollectionID()).getDiskSize();
    if (isPersistent()) {
        EXPECT_NE(0, emptySz); // system event data is counted
    }
    {
        std::function<bool(size_t, size_t)> gt = std::greater<>{};
        std::function<bool(size_t, size_t)> eq = std::equal_to<>{};
        // Expect collection disk to increase because aborts use storage.
        // ephemeral no change expected and magma (MB-45185)
        DiskChecker dc(vb,
                       CollectionEntry::dairy,
                       isPersistent() && !isMagma() ? gt : eq);
        createPendingWrite();
        abort();
    }
    dropCollection();

    // This is a dodgy way of testing things but in this test the only prepares
    // we will be dropping are completed and so we should not call into the DM.
    // If we tried to then we'd segfault
    VBucketTestIntrospector::destroyDM(*vb.get());

    if (isPersistent()) {
        // Expect compaction task to be scheduled as the collections as we've
        // added an abort to the collection before it was dropped.
        EXPECT_EQ(1, getFutureQueueSize(AUXIO_TASK_IDX));
        EXPECT_EQ(0, getReadyQueueSize(AUXIO_TASK_IDX));
    }
}

TEST_P(CollectionsEraserSyncWriteTest,
       CommitDifferentItemAfterDropBeforeErase) {
    addCollection();
    createPendingWrite();
    dropCollection();

    // Add new write
    auto newKey = makeStoredDocKey("defaultCKey");
    auto item = makePendingItem(newKey, "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Seqno ack to start a commit
    vb->seqnoAcknowledged(folly::SharedMutex::ReadHolder(vb->getStateLock()),
                          "replica",
                          4 /*prepareSeqno*/);

    // Should have moved the prepare to the resolvedQ but not run the task.
    // Try to process the queue and we should commit the new prepare and skip
    // the one for the dropped collection (after notifying the client with an
    // ambiguous response). Flushing 1 item verifies that we don't commit both.
    vb->processResolvedSyncWrites();
    flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(4, vb->getHighCompletedSeqno());
}

TEST_P(CollectionsEraserSyncWriteTest, AbortAfterDropBeforeErase) {
    addCollection();
    createPendingWrite();
    dropCollection();

    vb->processDurabilityTimeout(std::chrono::steady_clock::now() +
                                 std::chrono::seconds(1000));

    // Should have moved the prepare to the resolvedQ but not run the task.
    // Try to process the queue and we should commit the new prepare and skip
    // the one for the dropped collection (after notifying the client with an
    // ambiguous response).
    vb->processResolvedSyncWrites();

    EXPECT_EQ(0, vb->getHighCompletedSeqno());
}

TEST_P(CollectionsEraserSyncWriteTest, ResurrectionTestDontCommitOldPrepare) {
    {
        SCOPED_TRACE("First generation");
        addCollection();

        // Do a prepare
        createPendingWrite();
        EXPECT_EQ(2, vb->getHighSeqno());
        EXPECT_EQ(0, vb->getHighCompletedSeqno());

        // Ack it, don't commit it yet
        processAck(2 /*prepareSeqno*/);
        EXPECT_EQ(2, vb->getHighSeqno());
        EXPECT_EQ(0, vb->getHighCompletedSeqno());

        // Now drop this version of the collection
        dropCollection();
        EXPECT_EQ(3, vb->getHighSeqno());
        EXPECT_EQ(0, vb->getHighCompletedSeqno());
    }

    {
        SCOPED_TRACE("Second generation");
        addCollection();
        EXPECT_EQ(4, vb->getHighSeqno());
        EXPECT_EQ(0, vb->getHighCompletedSeqno());

        // Processing the resolved queue should NOT commit the prepare that we
        // created against the old collection
        processResolvedSyncWrites();
        EXPECT_EQ(4, vb->getHighSeqno());
        EXPECT_EQ(0, vb->getHighCompletedSeqno());
    }
}

TEST_P(CollectionsEraserSyncWriteTest, ResurrectionTestDontAbortOldPrepare) {
    {
        SCOPED_TRACE("First generation");
        addCollection();

        // Do a prepare
        createPendingWrite();
        EXPECT_EQ(2, vb->getHighSeqno());
        EXPECT_EQ(0, vb->getHighCompletedSeqno());

        // Ack it, don't commit it yet
        vb->processDurabilityTimeout(std::chrono::steady_clock::now() +
                                     std::chrono::seconds(70));

        EXPECT_EQ(2, vb->getHighSeqno());
        EXPECT_EQ(0, vb->getHighCompletedSeqno());

        // Now drop this version of the collection
        dropCollection();
        EXPECT_EQ(3, vb->getHighSeqno());
        EXPECT_EQ(0, vb->getHighCompletedSeqno());
    }

    {
        SCOPED_TRACE("Second generation");
        addCollection();
        EXPECT_EQ(4, vb->getHighSeqno());
        EXPECT_EQ(0, vb->getHighCompletedSeqno());

        // Processing the resolved queue should NOT commit the prepare that we
        // created against the old collection
        processResolvedSyncWrites();
        EXPECT_EQ(4, vb->getHighSeqno());
        EXPECT_EQ(0, vb->getHighCompletedSeqno());
    }
}

TEST_P(CollectionsEraserSyncWriteTest, ErasePendingPrepare) {
    auto& vb = *store->getVBucket(vbid);
    auto& ht = vb.ht;
    ASSERT_EQ(0, ht.getNumItems());
    auto& dm = vb.getDurabilityMonitor();
    ASSERT_EQ(0, dm.getNumTracked());

    // Note: All operations against the same collection
    addCollection();
    createPendingWrite();
    dropCollection();

    EXPECT_EQ(3, vb.getHighSeqno());

    // Prepare still in HT and DM
    {
        auto res = ht.findForUpdate(key);
        ASSERT_TRUE(res.pending);
        ASSERT_EQ(CommittedState::Pending, res.pending->getCommitted());
        EXPECT_EQ(2, res.pending->getBySeqno());
        ASSERT_FALSE(res.committed);
    }
    ASSERT_EQ(1, dm.getNumTracked());

    scheduleAndRunCollectionsEraser(vbid);

    // Prepare removed from HT and DM
    {
        auto res = ht.findForUpdate(key);
        ASSERT_FALSE(res.pending);
        ASSERT_FALSE(res.committed);
    }
    ASSERT_EQ(0, dm.getNumTracked());
}

TEST_P(CollectionsEraserSyncWriteTest, EraserFindsPrepares) {
    // Ephemeral doesn't have comparable compaction so not valid to test here
    if (!persistent()) {
        GTEST_SKIP();
    }
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Do our SyncWrite
    auto key = makeStoredDocKey("syncwrite");
    auto item = makePendingItem(key, "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // And commit it
    EXPECT_EQ(cb::engine_errc::success,
              vb->commit(key,
                         2 /*prepareSeqno*/,
                         {} /*commitSeqno*/,
                         vb->lockCollections(key)));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // add some items
    store_item(vbid, StoredDocKey{"milk", CollectionEntry::dairy}, "nice");
    flushVBucketToDiskIfPersistent(vbid, 1 /* 1x items */);

    // delete the collection
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::dairy)));
    flushVBucketToDiskIfPersistent(vbid, 1 /* 1 x system */);

    runCollectionsEraser(vbid);

    // We expect the prepare to have been visited (and dropped due to
    // completion) during this compaction as the compaction must iterate over
    // the prepare namespace
    auto state = store->getRWUnderlying(vbid)->getCachedVBucketState(vbid);
    ASSERT_TRUE(state);
    EXPECT_EQ(0, state->onDiskPrepares);
}

class CollectionsEraserPersistentOnly : public CollectionsEraserTest {
public:
    void testEmptyCollections(bool flushInTheMiddle);
    void testEmptyCollectionsWithPending(bool flushInTheMiddle,
                                         bool warmupInTheMiddle);
};

// Test that we schedule compaction after dropping a collection that only
// contains prepares
void CollectionsEraserPersistentOnly::testEmptyCollectionsWithPending(
        bool flushInTheMiddle, bool warmupInTheMiddle) {
    auto getOnDiskPrepares = [this]() {
        auto* vbstate =
                store->getOneRWUnderlying()->getCachedVBucketState(vbid);
        return vbstate->onDiskPrepares;
    };
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    // Create two collections
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::fruit)));

    // 2x collection creates
    int waitingForFlush = 2;

    // The flusher will see the drop as a separate event or in the same batch
    // as the create
    if (flushInTheMiddle) {
        flushVBucketToDiskIfPersistent(vbid, waitingForFlush);
        waitingForFlush = 0;
        auto handle = vb->lockCollections();
        EXPECT_EQ(1, handle.getHighSeqno(CollectionEntry::dairy));
        EXPECT_EQ(1, handle.getPersistedHighSeqno(CollectionEntry::dairy));
        EXPECT_EQ(2, handle.getHighSeqno(CollectionEntry::fruit));
        EXPECT_EQ(2, handle.getPersistedHighSeqno(CollectionEntry::fruit));
    }

    auto item = makePendingItem(StoredDocKey{"orange", CollectionEntry::fruit},
                                "v");
    EXPECT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));
    item = makePendingItem(StoredDocKey{"cheese", CollectionEntry::dairy},
                           "vv");
    EXPECT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));

    // 2x prepares
    waitingForFlush += 2;

    if (warmupInTheMiddle) {
        // Persist the prepares and do a warmup
        flushVBucketToDiskIfPersistent(vbid, waitingForFlush);
        waitingForFlush = 0;
        vb.reset();
        resetEngineAndWarmup();
        vb = store->getVBucket(vbid);
    } else if (!flushInTheMiddle) {
        // The delete collection events will cancel out the creates.
        waitingForFlush -= 2;
    }

    // Delete the collections
    vb->updateFromManifest(makeManifest(
            cm.remove(CollectionEntry::dairy).remove(CollectionEntry::fruit)));
    waitingForFlush += 2;

    auto flusherDedupe = !store->getOneROUnderlying()
                                  ->getStorageProperties()
                                  .hasAutomaticDeduplication();
    if (!warmupInTheMiddle && !flushInTheMiddle && !flusherDedupe) {
        waitingForFlush += 2;
    }

    flushVBucketToDiskIfPersistent(vbid, waitingForFlush);

    if (isPersistent() && !isMagma()) {
        EXPECT_EQ(2, getOnDiskPrepares());
    }

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));

    runCollectionsEraser(vbid);

    if (isPersistent() && !isMagma()) {
        EXPECT_EQ(0, getOnDiskPrepares());
    }

    EXPECT_EQ(0, vb->getNumItems());

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));
}

TEST_P(CollectionsEraserPersistentOnly,
       logically_empty_with_flush_with_warmup) {
    testEmptyCollectionsWithPending(true, true);
}
TEST_P(CollectionsEraserPersistentOnly, logically_empty_no_flush_with_warmup) {
    testEmptyCollectionsWithPending(false, true);
}
TEST_P(CollectionsEraserPersistentOnly, logically_empty_with_flush) {
    testEmptyCollectionsWithPending(true, false);
}
TEST_P(CollectionsEraserPersistentOnly, logically_empty_no_flush) {
    testEmptyCollectionsWithPending(false, false);
}

// Test that empty collections don't lead to a compaction trigger
void CollectionsEraserPersistentOnly::testEmptyCollections(
        bool flushInTheMiddle) {
    // Create two collections
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::fruit)));
    auto& kvs = *vb->getShard()->getRWUnderlying();

    // The flusher will see the drop as a separate event or in the same batch
    // as the create
    if (flushInTheMiddle) {
        flushVBucketToDiskIfPersistent(vbid, 2 /* 2 x system */);
        auto handle = vb->lockCollections();
        EXPECT_EQ(1, handle.getHighSeqno(CollectionEntry::dairy));
        EXPECT_EQ(1, handle.getPersistedHighSeqno(CollectionEntry::dairy));
        EXPECT_EQ(2, handle.getHighSeqno(CollectionEntry::fruit));
        EXPECT_EQ(2, handle.getPersistedHighSeqno(CollectionEntry::fruit));

        auto stats = kvs.getCollectionStats(vbid, CollectionEntry::dairy);
        EXPECT_EQ(KVStore::GetCollectionStatsStatus::Success, stats.first);
        EXPECT_EQ(0, stats.second.itemCount);
        EXPECT_EQ(vb->getHighSeqno() - 1, stats.second.highSeqno);
        EXPECT_NE(0, stats.second.diskSize);
        stats = kvs.getCollectionStats(vbid, CollectionEntry::fruit);
        EXPECT_EQ(KVStore::GetCollectionStatsStatus::Success, stats.first);
        EXPECT_EQ(0, stats.second.itemCount);
        EXPECT_EQ(vb->getHighSeqno(), stats.second.highSeqno);
        EXPECT_NE(0, stats.second.diskSize);
    } else {
        auto handle = vb->lockCollections();
        EXPECT_EQ(vb->getHighSeqno() - 1,
                  handle.getHighSeqno(CollectionEntry::dairy));
        EXPECT_EQ(0, handle.getPersistedHighSeqno(CollectionEntry::dairy));
        EXPECT_EQ(vb->getHighSeqno(),
                  handle.getHighSeqno(CollectionEntry::fruit));
        EXPECT_EQ(0, handle.getPersistedHighSeqno(CollectionEntry::fruit));
    }

    EXPECT_EQ(0, vb->getNumItems());

    // Drop the collections
    vb->updateFromManifest(makeManifest(
            cm.remove(CollectionEntry::dairy).remove(CollectionEntry::fruit)));

    auto expected = 2; // 2 system
    auto flusherDedupe = !store->getOneROUnderlying()
                                  ->getStorageProperties()
                                  .hasAutomaticDeduplication();
    if (!flushInTheMiddle && !flusherDedupe) {
        expected += 2;
    }

    flushVBucketToDiskIfPersistent(vbid, expected);

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));

    // Expect that the eraser task is not scheduled
    EXPECT_EQ(0, getFutureQueueSize(AUXIO_TASK_IDX));
    EXPECT_EQ(0, getReadyQueueSize(AUXIO_TASK_IDX));

    EXPECT_EQ(0, vb->getNumItems());

    if (!isMagma()) {
        // magma keeps the disk stats until next compaction completes
        auto stats = kvs.getCollectionStats(vbid, CollectionEntry::fruit);
        EXPECT_EQ(KVStore::GetCollectionStatsStatus::NotFound, stats.first);
        EXPECT_EQ(0, stats.second.itemCount);
        EXPECT_EQ(0, stats.second.highSeqno);
        EXPECT_EQ(0, stats.second.diskSize);
        stats = kvs.getCollectionStats(vbid, CollectionEntry::dairy);
        EXPECT_EQ(KVStore::GetCollectionStatsStatus::NotFound, stats.first);
        EXPECT_EQ(0, stats.second.itemCount);
        EXPECT_EQ(0, stats.second.highSeqno);
        EXPECT_EQ(0, stats.second.diskSize);
    }
}

TEST_P(CollectionsEraserPersistentOnly, empty_collections_with_flush) {
    testEmptyCollections(true);
}

TEST_P(CollectionsEraserPersistentOnly, empty_collections_no_flush) {
    testEmptyCollections(false);
}

// Create and store into multiple collections, drop all of the collections and
// validate that only 1 task is ready to run (previously 1 per drop/flush)
TEST_P(CollectionsEraserPersistentOnly, DropManyCompactOnce) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    cm.add(CollectionEntry::vegetable);
    vb->updateFromManifest(makeManifest(cm));
    store_item(vbid, StoredDocKey{"apple", CollectionEntry::fruit}, "red");
    store_item(vbid, StoredDocKey{"sprout", CollectionEntry::vegetable}, "yum");
    flushVBucketToDiskIfPersistent(vbid, 4);

    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::fruit)));
    flushVBucketToDiskIfPersistent(vbid, 1);

    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::vegetable)));
    flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(1,
              (*task_executor->getLpTaskQ()[AUXIO_TASK_IDX])
                      .getFutureQueueSize());

    // Test extended to cover MB-43199
    const int nCookies = 3;
    std::vector<CookieIface*> cookies;
    for (int ii = 0; ii < nCookies; ii++) {
        cookies.push_back(create_mock_cookie());
        // Now schedule as if a command had requested (i.e. set a cookie)
        store->scheduleCompaction(
                vbid, {}, cookies.back(), std::chrono::seconds(0));
    }

    std::string taskDescription =
            "Compact DB file " + std::to_string(vbid.get());
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX],
                "Compact DB file " + std::to_string(vbid.get()));

    for (auto* cookie : cookies) {
        EXPECT_EQ(cb::engine_errc::success, mock_waitfor_cookie(cookie));
        destroy_mock_cookie(cookie);
    }
}

TEST_P(CollectionsEraserPersistentOnly, DocAliveCollRecreateDocAliveCollPurge) {
    // Flush the initial create
    CollectionsManifest cm;
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::fruit)));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // DocAlive
    store_item(vbid, StoredDocKey{"apple", CollectionEntry::fruit}, "red");
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Collection Drop
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::fruit)));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Collection Create
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::fruit)));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // DocAlive
    store_item(vbid, StoredDocKey{"apple", CollectionEntry::fruit}, "green");
    flushVBucketToDiskIfPersistent(vbid, 1);

    if (isMagma() && isFullEviction()) {
        // Magma full eviction (relies on magma doc count) overcounts here as
        // we have logically inserted an item into a collection (written a new
        // version of it to a new alive generation of a collection but it exists
        // in the old generation). See MB-50061 for more details.
        EXPECT_EQ(2, vb->getNumItems());
    } else {
        EXPECT_EQ(1, vb->getNumItems());
    }

    // Collection Purge
    EXPECT_EQ(1,
              (*task_executor->getLpTaskQ()[AUXIO_TASK_IDX])
                      .getFutureQueueSize());
    runCollectionsEraser(vbid);
    EXPECT_EQ(1, vb->getNumItems());
}

TEST_P(CollectionsEraserPersistentOnly,
       DocAliveCollRecreateDocDeleteCollPurge) {
    // Flush the create
    CollectionsManifest cm;
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::fruit)));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // DocAlive
    store_item(vbid, StoredDocKey{"apple", CollectionEntry::fruit}, "red");
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Collection Drop
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::fruit)));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Collection Create
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::fruit)));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // DocDelete
    store_item(vbid,
               StoredDocKey{"apple", CollectionEntry::fruit},
               "green",
               0,
               {cb::engine_errc::success},
               PROTOCOL_BINARY_DATATYPE_JSON,
               {},
               true);
    flushVBucketToDiskIfPersistent(vbid, 1);

    if (isMagma() && isFullEviction()) {
        EXPECT_EQ(1, vb->getNumItems());
    } else {
        EXPECT_EQ(0, vb->getNumItems());
    }

    // Collection Purge
    EXPECT_EQ(1,
              (*task_executor->getLpTaskQ()[AUXIO_TASK_IDX])
                      .getFutureQueueSize());
    runCollectionsEraser(vbid);
    EXPECT_EQ(0, vb->getNumItems());
}

TEST_P(CollectionsEraserPersistentOnly,
       DocDeleteCollRecreateDocAliveCollPurge) {
    // Flush the create
    CollectionsManifest cm;
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::fruit)));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // DocDelete
    store_item(vbid,
               StoredDocKey{"apple", CollectionEntry::fruit},
               "red",
               0,
               {cb::engine_errc::success},
               PROTOCOL_BINARY_DATATYPE_JSON,
               {},
               true);
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Collection Drop
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::fruit)));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Collection Create
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::fruit)));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // DocAlive
    store_item(vbid, StoredDocKey{"apple", CollectionEntry::fruit}, "green");
    flushVBucketToDiskIfPersistent(vbid, 1);
    EXPECT_EQ(1, vb->getNumItems());

    // Collection Purge
    EXPECT_EQ(1,
              (*task_executor->getLpTaskQ()[AUXIO_TASK_IDX])
                      .getFutureQueueSize());
    runCollectionsEraser(vbid);
    EXPECT_EQ(1, vb->getNumItems());
}

TEST_P(CollectionsEraserPersistentOnly,
       DocDeleteCollRecreateDocDeleteCollPurge) {
    // Flush the create
    CollectionsManifest cm;
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::fruit)));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // DocDelete
    store_item(vbid,
               StoredDocKey{"apple", CollectionEntry::fruit},
               "red",
               0,
               {cb::engine_errc::success},
               PROTOCOL_BINARY_DATATYPE_JSON,
               {},
               true);
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Collection Drop
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::fruit)));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Collection Create
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::fruit)));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // DocDelete
    store_item(vbid,
               StoredDocKey{"apple", CollectionEntry::fruit},
               "green",
               0,
               {cb::engine_errc::success},
               PROTOCOL_BINARY_DATATYPE_JSON,
               {},
               true);
    flushVBucketToDiskIfPersistent(vbid, 1);
    EXPECT_EQ(0, vb->getNumItems());

    auto [status, manifest] =
            store->getRWUnderlying(vbid)->getCollectionsManifest(vbid);
    ASSERT_TRUE(status);
    EXPECT_EQ(3, manifest.manifestUid);

    // Collection Purge
    EXPECT_EQ(1,
              (*task_executor->getLpTaskQ()[AUXIO_TASK_IDX])
                      .getFutureQueueSize());
    runCollectionsEraser(vbid);
    EXPECT_EQ(0, vb->getNumItems());
}

/**
 * Regression test for MB-43460, where the VBucket-level num_items stat was
 * incorrect after dropping a collection.
 */
TEST_P(CollectionsEraserPersistentOnly, DropDuringFlush) {
    // Setup a replica vBucket containing one collection.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    vb->checkpointManager->createSnapshot(0, 3, 0, CheckpointType::Disk, 3);
    uint64_t uid = 0;
    vb->replicaCreateCollection(Collections::ManifestUid(uid++),
                                {ScopeID::Default, CollectionEntry::dairy},
                                "dairy",
                                {},
                                1);
    ASSERT_EQ(0, vb->getNumItems());

    // Test: Store two more items to the collection, open a new memory snapshot,
    // then drop the collection, and flush to disk.
    // Importantly, we trigger the flush when only the first item is queued
    // in the CheckpointManager; and perform the second mutation and collection
    // drop while the first flush is running via setSaveDocsPostWriteHook.
    // A second flush is then triggered to write the second mutation and
    // collection drop to disk.
    //
    // We expect that during the second flush, the number of items in the
    // collection (2) is correctly calculated.
    // However, with the bug from MB-43460, the number of items in the
    // collection is mis-accounted - during the second flush it fails to read
    // the existing collection count (1) and incorrectly writes 1 (0+1) instead
    // of 2 (1+2). As a consequence, when the collection is cleaned up during
    // compaction, only '1' is subtracted from the vBucket item count instead
    // of 2.
    //
    // Note: The new snapshot is critical here, as we need to have the
    // dropCollection applied to the in-memory manifest, but _not_ flushed to
    // disk in the same batch. A dropCollection SystemEvent is normally be
    // placed into its own Checkpoint, and this in conjunction with the logic
    // in CheckpointManager::getItemsForCursor() which does not flush different
    // Checkpoint types (memory / disk) in the same flush batch, will trigger
    // the problematic scenario.
    writeDocToReplica(
            vbid, StoredDocKey{"cheese", CollectionEntry::dairy}, 2, false);

    auto* kvstore = store->getRWUnderlying(vbid);
    kvstore->setSaveDocsPostWriteDocsHook([this, kvstore, &uid] {
        writeDocToReplica(vbid,
                          StoredDocKey{"yoghurt", CollectionEntry::dairy},
                          3,
                          false);

        vb->checkpointManager->createSnapshot(
                4, 4, 0, CheckpointType::Memory, 4);
        this->vb->replicaDropCollection(
                Collections::ManifestUid(uid++), CollectionEntry::dairy, 4);

        // Clear this hook - don't want to add extra calls in subsequent
        // flushes.
        kvstore->setSaveDocsPostWriteDocsHook({});
    });

    // Flush vBucket
    ASSERT_EQ(2, flushVBucket(vbid))
            << "Expected to see 1 create collection and 1 mutation flushed";

    auto [status, persistedStats] =
            kvstore->getCollectionStats(vbid, CollectionEntry::dairy);
    ASSERT_EQ(KVStoreIface::GetCollectionStatsStatus::Success, status);
    EXPECT_EQ(1, persistedStats.itemCount);

    // Flush to write the second mutation and drop added since the
    // previous flush. Note that as the drop is in a different snapshot
    // whose type differs from first, this is done as two separate flush
    // batches in the KVStore of 1 item each.
    //
    // First flush; only up until end of first checkpoint. Collection
    // should still be alive and have 2 items afterwards.
    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    auto res = epBucket.flushVBucket(vbid);
    EXPECT_EQ(EPBucket::MoreAvailable::Yes, res.moreAvailable);
    EXPECT_EQ(1, res.numFlushed);
    std::tie(status, persistedStats) =
            kvstore->getCollectionStats(vbid, CollectionEntry::dairy);
    ASSERT_EQ(KVStoreIface::GetCollectionStatsStatus::Success, status);
    EXPECT_EQ(2, persistedStats.itemCount);
    EXPECT_EQ(3, persistedStats.highSeqno);

    // Complete flushing the drop of the collection. After this collection
    // should be NotFound.
    res = epBucket.flushVBucket(vbid);
    EXPECT_EQ(EPBucket::MoreAvailable::No, res.moreAvailable);
    EXPECT_EQ(1, res.numFlushed);
    EXPECT_EQ(EPBucket::WakeCkptRemover::Yes, res.wakeupCkptRemover);

    std::tie(status, persistedStats) =
            kvstore->getCollectionStats(vbid, CollectionEntry::dairy);
    ASSERT_EQ(KVStoreIface::GetCollectionStatsStatus::NotFound, status);
    EXPECT_EQ(0, persistedStats.itemCount);

    // Note in the original MB we _do_ get the correct count here, as this
    // is in-memory accounting.
    EXPECT_EQ(2, vb->getNumItems())
            << "Incorrect VBucket item count after dropping (but not yet "
               "cleaning up via compaction) a collection";

    // Compaction should have been scheduled to clean up the dropped
    // collection; run it now.
    runCollectionsEraser(vbid);

    EXPECT_EQ(0, vb->getNumItems())
            << "VBucket should have zero items after collection has been "
               "deleted and compacted";
    EXPECT_EQ(0, vb->getNumTotalItems())
            << "VBucket should have zero total (on-disk) items after "
               "collection has been deleted and compacted";
}

class CollectionsEraserPersistentWithFailure
    : public CollectionsEraserPersistentOnly {
public:
    void SetUp() override {
        CollectionsEraserPersistentOnly::SetUp();
        if (isMagma()) {
            replaceMagmaKVStore();
        } else {
            replaceCouchKVStoreWithMock();
        }
    }

    void runAndFailCompaction() {
        auto* kvstore = store->getRWUnderlying(vbid);
        ASSERT_TRUE(kvstore);

        if (isMagma()) {
#if EP_USE_MAGMA
            dynamic_cast<MockMagmaKVStore&>(*kvstore).setCompactionStatusHook(
                    [](magma::Status& status) {
                        throw std::runtime_error(
                                "CollectionsEraserPersistentWithFailure "
                                "forcing compaction to fail");
                    });
#else
            FAIL() << "isMagma() true but EP_USE_MAGMA is not defined";
#endif
        } else {
            dynamic_cast<MockCouchKVStore&>(*kvstore)
                    .setConcurrentCompactionPreLockHook(
                            [](auto& compactionKey) {
                                throw std::runtime_error(
                                        "CollectionsEraserPersistentWithFailure"
                                        " forcing compaction to fail");
                            });
        }

        runCollectionsEraser(vbid, false /*expectSuccess*/);

        if (isMagma()) {
#if EP_USE_MAGMA
            dynamic_cast<MockMagmaKVStore&>(*kvstore).setCompactionStatusHook(
                    [](magma::Status& status){});
#else
            FAIL() << "isMagma() true but EP_USE_MAGMA is not defined";
#endif
        } else {
            dynamic_cast<MockCouchKVStore&>(*kvstore)
                    .setConcurrentCompactionPreLockHook(
                            [](auto& compactionKey) {});
        }
    }
};

TEST_P(CollectionsEraserPersistentWithFailure, FailAndRetry) {
    // Flush the create
    CollectionsManifest cm;
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::fruit)));
    flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(vbid, StoredDocKey{"apple", CollectionEntry::fruit}, "red");
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Collection Drop
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::fruit)));
    flushVBucketToDiskIfPersistent(vbid, 1);

    size_t preFailure = 0;
    EXPECT_TRUE(store->getKVStoreStat("failure_compaction", preFailure));

    // Inject failure
    runAndFailCompaction();

    size_t postFailure = 0;
    EXPECT_TRUE(store->getKVStoreStat("failure_compaction", postFailure));
    EXPECT_GT(postFailure, preFailure);

    // Rescheduled and should run again
    runCollectionsEraser(vbid);

    size_t postSuccess = 0;
    EXPECT_TRUE(store->getKVStoreStat("failure_compaction", postSuccess));
    EXPECT_EQ(postFailure, postSuccess);
}

// Delete an item in the same flush batch as the drop of its collection.
// MB-50747 would mean the vbucket item count was decremented when it should
// remain unchanged, this lead to a negative count
TEST_P(CollectionsEraserTest, MB_50747_delete_and_drop) {
    // add a collection
    CollectionsManifest cm(CollectionEntry::fruit);
    vb->updateFromManifest(makeManifest(cm));
    auto key = makeStoredDocKey("peach", CollectionEntry::fruit);
    store_item(vbid, key, "value");
    flushVBucketToDiskIfPersistent(vbid, 2);

    delete_item(vbid, key);
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::fruit)));

    flushVBucketToDiskIfPersistent(vbid, 2);
    runCollectionsEraser(vbid);
    if (persistent()) {
        // Check prepares and disk count
        auto kvstore = store->getRWUnderlying(vbid);
        ASSERT_TRUE(kvstore);
        EXPECT_EQ(0, kvstore->getItemCount(vbid));
    }

    EXPECT_EQ(0, vb->getNumItems());
}

// Test cases which run for persistent and ephemeral buckets
INSTANTIATE_TEST_SUITE_P(CollectionsEraserTests,
                         CollectionsEraserTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(CollectionsEraserSyncWriteTests,
                         CollectionsEraserSyncWriteTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(CollectionsEraserPersistentOnly,
                         CollectionsEraserPersistentOnly,
                         STParameterizedBucketTest::persistentConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(
        CollectionsEraserPersistentWithFailure,
        CollectionsEraserPersistentWithFailure,
        STParameterizedBucketTest::persistentNoNexusConfigValues(),
        STParameterizedBucketTest::PrintToStringParamName);
