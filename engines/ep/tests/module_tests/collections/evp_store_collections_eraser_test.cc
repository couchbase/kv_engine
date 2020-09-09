/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "collections/vbucket_manifest_handles.h"
#include "durability/active_durability_monitor.h"
#include "ephemeral_tombstone_purger.h"
#include "ephemeral_vb.h"
#include "item.h"
#include "kv_bucket.h"
#include "kvstore.h"
#include "tests/mock/mock_ep_bucket.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/collections/collections_test_helpers.h"
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
        // "magma_commit_point_every_batch" to be set to true. In this
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

    void runEraser() {
        if (persistent()) {
            runCompaction();
        } else {
            auto* evb = dynamic_cast<EphemeralVBucket*>(vb.get());

            evb->purgeStaleItems();
        }
    }

    void flush_vbucket_to_disk(Vbid vbid, size_t expected = 1) {
        flushVBucketToDiskIfPersistent(vbid, expected);
    }

    /// Override so we recreate the vbucket for ephemeral tests
    void resetEngineAndWarmup() {
        SingleThreadedKVBucketTest::resetEngineAndWarmup();
        if (!persistent()) {
            // Persistent will recreate the VB from the disk metadata so for
            // ephemeral do an explicit set state.
            EXPECT_EQ(ENGINE_SUCCESS,
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

    VBucketPtr vb;
};

// Small numbers of items for easier debug
TEST_P(CollectionsEraserTest, basic) {
    // add a collection
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm));

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    // add some items
    store_item(
            vbid, StoredDocKey{"dairy:milk", CollectionEntry::dairy}, "nice");
    store_item(vbid,
               StoredDocKey{"dairy:butter", CollectionEntry::dairy},
               "lovely");

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

    flush_vbucket_to_disk(vbid, 2 /* 2 x items */);

    if (persistent()) {
        // Now the flusher will have calculated the items/diskSize
        auto stats = vb->lockCollections().getStatsForFlush(
                CollectionEntry::dairy, 1);
        EXPECT_EQ(2, stats.itemCount);
        EXPECT_GT(stats.diskSize, diskSize);
        diskSize = stats.diskSize;
    }

    EXPECT_EQ(2, vb->getNumItems());

    // Evict one of the keys, we should still erase it
    if (persistent()) {
        evict_key(vbid, StoredDocKey{"dairy:butter", CollectionEntry::dairy});
    }
    // delete the collection
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::dairy)));

    if (!persistent()) {
        EXPECT_EQ(1, vb->getNumSystemItems());
    }

    if (persistent()) {
        // Now the collection is dropped, we should still be able to obtain the
        // stats the flusher can continue flushing any items the proceed the
        // drop and still make correct updates
        auto stats = vb->lockCollections().getStatsForFlush(
                CollectionEntry::dairy, 1);
        EXPECT_EQ(2, stats.itemCount);
        EXPECT_EQ(diskSize, stats.diskSize);
    }

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    // Now the drop is persisted the stats have gone

    EXPECT_THROW(
            vb->lockCollections().getStatsForFlush(CollectionEntry::dairy, 1),
            std::logic_error);

    // Deleted
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));

    runCollectionsEraser();

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

    flush_vbucket_to_disk(vbid, 2 /* 2 x system */);

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

    flush_vbucket_to_disk(vbid, 4);

    EXPECT_EQ(4, vb->getNumItems());

    // delete the collections
    vb->updateFromManifest(makeManifest(
            cm.remove(CollectionEntry::dairy).remove(CollectionEntry::fruit)));

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));

    flush_vbucket_to_disk(vbid, 2 /* 2 x system */);

    runCollectionsEraser();

    EXPECT_EQ(0, vb->getNumItems());

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));
}

TEST_P(CollectionsEraserTest, basic_3_collections) {
    // Add two collections
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::fruit)));

    flush_vbucket_to_disk(vbid, 2 /* 1x system */);

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

    flush_vbucket_to_disk(vbid, 4 /* 2x items */);

    EXPECT_EQ(4, vb->getNumItems());

    // delete one of the 3 collections
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::fruit)));

    EXPECT_TRUE(vb->lockCollections().exists(CollectionEntry::dairy));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    runCollectionsEraser();

    EXPECT_EQ(2, vb->getNumItems());

    EXPECT_TRUE(vb->lockCollections().exists(CollectionEntry::dairy));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));
}

TEST_P(CollectionsEraserTest, basic_4_collections) {
    // Add two collections
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::fruit)));

    flush_vbucket_to_disk(vbid, 2 /* 1x system */);

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

    flush_vbucket_to_disk(vbid, 4 /* 2x items */);

    // delete the collections and re-add a new dairy
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::fruit)
                                                .remove(CollectionEntry::dairy)
                                                .add(CollectionEntry::dairy2)));

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));
    EXPECT_TRUE(vb->lockCollections().exists(CollectionEntry::dairy2));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));

    flush_vbucket_to_disk(vbid, 3 /* 3x system (2 deletes, 1 create) */);

    runCollectionsEraser();

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

    flush_vbucket_to_disk(vbid, 4);

    EXPECT_EQ(4, vb->getNumItems());

    // delete the default collection
    CollectionsManifest cm;
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::defaultC)));

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    runCollectionsEraser();

    EXPECT_EQ(0, vb->getNumItems());

    // Add default back - so we don't get collection unknown errors
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::defaultC)));

    auto key1 = makeStoredDocKey("dairy:milk", CollectionEntry::defaultC);
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);

    EXPECT_EQ(ENGINE_KEY_ENOENT, checkKeyExists(key1, vbid, options));
}

// Test that following a full drop (compaction completes the deletion), warmup
// reloads the VB::Manifest and the dropped collection stays dropped.
TEST_P(CollectionsEraserTest, erase_and_reset) {
    // Add two collections
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::fruit)));

    flush_vbucket_to_disk(vbid, 2 /* 1x system */);

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

    flush_vbucket_to_disk(vbid, 4 /* 2x items */);

    // delete the collections and re-add a new dairy
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::fruit)
                                                .remove(CollectionEntry::dairy)
                                                .add(CollectionEntry::dairy2)));

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));
    EXPECT_TRUE(vb->lockCollections().exists(CollectionEntry::dairy2));
    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::fruit));

    flush_vbucket_to_disk(vbid, 3 /* 3x system (2 deletes, 1 create) */);

    runCollectionsEraser();

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

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    // add some items
    store_item(
            vbid, StoredDocKey{"dairy:milk", CollectionEntry::dairy}, "nice");
    store_item(vbid,
               StoredDocKey{"dairy:butter", CollectionEntry::dairy},
               "lovely");
    delete_item(vbid, StoredDocKey{"dairy:butter", CollectionEntry::dairy});

    flush_vbucket_to_disk(vbid, 2 /* 2 x items */);

    EXPECT_EQ(1, vb->getNumItems());

    // delete the collection
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::dairy)));

    // @todo MB-26334: persistent buckets don't track the system event counts
    if (!persistent()) {
        EXPECT_EQ(1, vb->getNumSystemItems());
    }

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));

    runCollectionsEraser();

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

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    // add some items
    store_item(
            vbid, StoredDocKey{"dairy:milk", CollectionEntry::dairy}, "nice");
    store_item(vbid,
               StoredDocKey{"dairy:butter", CollectionEntry::dairy},
               "lovely");

    flush_vbucket_to_disk(vbid, 2 /* 2 x items */);

    EXPECT_EQ(2, vb->getNumItems());

    // delete the collection
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::dairy)));

    // @todo MB-26334: persistent buckets don't track the system event counts
    if (!persistent()) {
        EXPECT_EQ(1, vb->getNumSystemItems());
    }

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));

    runCollectionsEraser();

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

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    // add some items
    store_item(
            vbid, StoredDocKey{"dairy:milk", CollectionEntry::dairy}, "nice");
    store_item(vbid,
               StoredDocKey{"dairy:butter", CollectionEntry::dairy},
               "lovely");

    flush_vbucket_to_disk(vbid, 2 /* 2 x items */);

    EXPECT_EQ(2, vb->getNumItems());

    // Evict one of the keys, we should still erase it
    evict_key(vbid, StoredDocKey{"dairy:butter", CollectionEntry::dairy});

    // delete the collection
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::dairy)));

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);
    vb.reset();

    store->cancelCompaction(vbid);
    resetEngineAndWarmup();

    EXPECT_FALSE(store->getVBucket(vbid)
                         ->getShard()
                         ->getRWUnderlying()
                         ->getDroppedCollections(vbid)
                         .empty());

    // Now the eraser should ready to run, warmup will have noticed a dropped
    // collection in the manifest and schedule the eraser
    runCollectionsEraser();
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
    EXPECT_NO_THROW(runCollectionsEraser());

    EXPECT_EQ(0, vb->getNumItems());

    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::fruit)));

    // Purge deleted items with lower seqnos
    EXPECT_NO_THROW(runCollectionsEraser());

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

    flush_vbucket_to_disk(vbid, 2 /* 1x system 2x items */);

    // delete the collection
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::dairy)));
    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    // Remove vbucket
    store->deleteVBucket(vbid, nullptr);
    runNextTask(*task_executor->getLpTaskQ()[WRITER_TASK_IDX],
                "Compact DB file 0"); // would fault (gsl exception)
}

// @TODO move to CollectionsEraserSyncWriteTest when we run it for all backends
TEST_P(CollectionsEraserTest, EraserFindsPrepares) {
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

    // Do our SyncWrite
    auto key = makeStoredDocKey("syncwrite");
    auto item = makePendingItem(key, "value");
    EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*item, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // And commit it
    EXPECT_EQ(ENGINE_SUCCESS,
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

    runEraser();

    // We expect the prepare to have been visited (and dropped due to
    // completion) during this compaction as the compaction must iterate over
    // the prepare namespace
    auto state = store->getRWUnderlying(vbid)->getVBucketState(vbid);
    ASSERT_TRUE(state);
    EXPECT_EQ(0, state->onDiskPrepares);
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
    EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*item, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    if (isMagma()) {
        // Magma does not track prepares in doc count
        EXPECT_EQ(1, kvStore->getItemCount(vbid));
    } else {
        EXPECT_EQ(2, kvStore->getItemCount(vbid));
    }

    // And commit it
    EXPECT_EQ(ENGINE_SUCCESS,
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
    EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*item, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    if (isMagma()) {
        EXPECT_EQ(2, kvStore->getItemCount(vbid));
    } else {
        EXPECT_EQ(3, kvStore->getItemCount(vbid));
    }

    // And commit it
    EXPECT_EQ(ENGINE_SUCCESS,
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

    runEraser();

    // Check doc and prepare counts
    EXPECT_EQ(0, kvStore->getItemCount(vbid));

    auto state = kvStore->getVBucketState(vbid);
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

        // Magma will make two calls here. One for the first value that
        // logically does not exist anymore, and one for the updated value.
        if (isMagma()) {
            EXPECT_CALL(*bucket,
                        dropKey(vbid,
                                DiskDocKey(key),
                                1 + seqnoOffset,
                                false /*isAbort*/,
                                0 /*PCS*/))
                    .RetiresOnSaturation();
        }

        EXPECT_CALL(*bucket,
                    dropKey(vbid,
                            DiskDocKey(key),
                            2 + seqnoOffset,
                            false /*isAbort*/,
                            0 /*PCS*/))
                .RetiresOnSaturation();
    }

    runEraser();

    EXPECT_EQ(0, vb->getNumItems());

    // The warmup will definitely fail if this isn't true
    auto kvStore = store->getRWUnderlying(vbid);
    ASSERT_EQ(0, kvStore->getItemCount(vbid));

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

    runEraser();
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

        CollectionsEraserTest::runCollectionsEraser();
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
            auto manifest = store->getVBucket(vbid)
                                    ->getShard()
                                    ->getRWUnderlying()
                                    ->getCollectionsManifest(vbid);
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
        if (deleted) {
            auto item = makeCommittedItem(key, "value");
            EXPECT_EQ(ENGINE_SUCCESS, store->set(*item, cookie));
            flushVBucketToDiskIfPersistent(vbid, 1);

            mutation_descr_t delInfo;
            uint64_t cas = item->getCas();
            using namespace cb::durability;
            EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING,
                      store->deleteItem(key,
                                        cas,
                                        vbid,
                                        cookie,
                                        Requirements(Level::Majority, {}),
                                        nullptr,
                                        delInfo));
        } else {
            auto item = makePendingItem(key, "value");
            EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*item, cookie));
        }
        flushVBucketToDiskIfPersistent(vbid, 1);
    }

    void commit() {
        EXPECT_EQ(ENGINE_SUCCESS,
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
        EXPECT_EQ(ENGINE_SUCCESS,
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

        auto vbstate = kvstore->getVBucketState(vbid);
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

        auto vbstate = kvstore->getVBucketState(vbid);
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

    runCollectionsEraser();
}

TEST_P(CollectionsEraserSyncWriteTest, DropAfterAbort) {
    addCollection();
    createPendingWrite();
    abort();
    dropCollection();

    // This is a dodgy way of testing things but in this test the only prepares
    // we will be dropping are completed and so we should not call into the DM.
    // If we tried to then we'd segfault
    VBucketTestIntrospector::destroyDM(*vb.get());

    runCollectionsEraser();
}

TEST_P(CollectionsEraserSyncWriteTest, CommitAfterDropBeforeErase) {
    addCollection();
    createPendingWrite();
    dropCollection();

    // Add new write
    auto newKey = makeStoredDocKey("defaultCKey");
    auto item = makePendingItem(newKey, "value");
    EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*item, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Seqno ack to start a commit
    vb->seqnoAcknowledged(folly::SharedMutex::ReadHolder(vb->getStateLock()),
                          "replica",
                          4 /*prepareSeqno*/);

    // Should have moved the prepare to the resolvedQ but not run the task.
    // Try to process the queue and we should commit the new prepare and skip
    // the one for the dropped collection (after notifying the client with an
    // ambiguous response).
    vb->processResolvedSyncWrites();
    flushVBucketToDiskIfPersistent(vbid, 1);
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
