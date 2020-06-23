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

#include "ephemeral_tombstone_purger.h"
#include "ephemeral_vb.h"
#include "item.h"
#include "kv_bucket.h"
#include "kvstore.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/collections/test_manifest.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"
#include "tests/module_tests/test_helpers.h"
#include "vbucket_state.h"

class CollectionsEraserTest : public STParameterizedBucketTest {
public:
    void SetUp() override {
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

    VBucketPtr vb;
};

// Small numbers of items for easier debug
TEST_P(CollectionsEraserTest, basic) {
    // add a collection
    CollectionsManifest cm(CollectionEntry::dairy);
    vb->updateFromManifest({cm});

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
    if (persistent()) {
        evict_key(vbid, StoredDocKey{"dairy:butter", CollectionEntry::dairy});
    }
    // delete the collection
    vb->updateFromManifest({cm.remove(CollectionEntry::dairy)});

    // @todo MB-26334: persistent buckets don't track the system event counts
    if (!persistent()) {
        EXPECT_EQ(1, vb->getNumSystemItems());
    }

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

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
    vb->updateFromManifest({cm.add(CollectionEntry::fruit)});

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
    vb->updateFromManifest(
            {cm.remove(CollectionEntry::dairy).remove(CollectionEntry::fruit)});

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
    vb->updateFromManifest({cm.add(CollectionEntry::fruit)});

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
    vb->updateFromManifest({cm.remove(CollectionEntry::fruit)});

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
    vb->updateFromManifest({cm.add(CollectionEntry::fruit)});

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
    vb->updateFromManifest({cm.remove(CollectionEntry::fruit)
                                    .remove(CollectionEntry::dairy)
                                    .add(CollectionEntry::dairy2)});

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
    vb->updateFromManifest({cm.remove(CollectionEntry::defaultC)});

    flush_vbucket_to_disk(vbid, 1 /* 1 x system */);

    runCollectionsEraser();

    EXPECT_EQ(0, vb->getNumItems());

    // Add default back - so we don't get collection unknown errors
    vb->updateFromManifest({cm.add(CollectionEntry::defaultC)});

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
    vb->updateFromManifest({cm.add(CollectionEntry::fruit)});

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
    vb->updateFromManifest({cm.remove(CollectionEntry::fruit)
                                    .remove(CollectionEntry::dairy)
                                    .add(CollectionEntry::dairy2)});

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
    vb->updateFromManifest({cm});

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
    vb->updateFromManifest({cm.remove(CollectionEntry::dairy)});

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
    vb->updateFromManifest({cm});

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
    vb->updateFromManifest({cm.remove(CollectionEntry::dairy)});

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
    vb->updateFromManifest({cm});

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
    vb->updateFromManifest({cm.remove(CollectionEntry::dairy)});

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
    vb->updateFromManifest({cm});

    // add some items
    store_item(
            vbid, StoredDocKey{"dairy:milk", CollectionEntry::dairy}, "nice");
    store_item(vbid,
               StoredDocKey{"dairy:butter", CollectionEntry::dairy},
               "lovely");

    vb->updateFromManifest({cm.add(CollectionEntry::fruit)});
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
    vb->updateFromManifest({cm.remove(CollectionEntry::dairy)});

    EXPECT_FALSE(vb->lockCollections().exists(CollectionEntry::dairy));
    EXPECT_TRUE(vb->lockCollections().exists(CollectionEntry::fruit));

    // Purge deleted items with higher seqnos
    EXPECT_NO_THROW(runCollectionsEraser());

    EXPECT_EQ(0, vb->getNumItems());

    vb->updateFromManifest({cm.remove(CollectionEntry::fruit)});

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
    vb->updateFromManifest({cm});

    // add some items
    store_item(vbid, StoredDocKey{"milk", CollectionEntry::dairy}, "nice");

    flush_vbucket_to_disk(vbid, 2 /* 1x system 2x items */);

    // delete the collection
    vb->updateFromManifest({cm.remove(CollectionEntry::dairy)});
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
    vb->updateFromManifest({cm});
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
    vb->updateFromManifest({cm.remove(CollectionEntry::dairy)});
    flushVBucketToDiskIfPersistent(vbid, 1 /* 1 x system */);

    runEraser();

    // We expect the prepare to have been visited (and dropped due to
    // completion) during this compaction as the compaction must iterate over
    // the prepare namespace
    auto state = store->getRWUnderlying(vbid)->getVBucketState(vbid);
    ASSERT_TRUE(state);
    EXPECT_EQ(0, state->onDiskPrepares);
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
        EXPECT_EQ(expectedItemsInHashTable, vb->ht.getNumInMemoryItems());
        EXPECT_EQ(expectedItemsInVBucket, vb->getNumItems());
        CollectionsEraserTest::TearDown();
    }

    enum class TestMode { Abort, Pending, Commit };

    /**
     * Run a test which drops a collection that has a sync write in it
     * @param commit true if the test should commit the sync-write
     */
    void basicDropWithSyncWrite(bool commit);

protected:
    void addCollection() {
        cm.add(CollectionEntry::dairy);
        vb->updateFromManifest({cm});
        flushVBucketToDiskIfPersistent(vbid, 1);
        if (!persistent()) {
            expectedItemsInHashTable++;
        }
        EXPECT_EQ(expectedItemsInHashTable, vb->ht.getNumInMemoryItems());
        EXPECT_EQ(expectedItemsInVBucket, vb->getNumItems());
    }

    void dropCollection() {
        cm.remove(CollectionEntry::dairy);
        vb->updateFromManifest({cm});
        flushVBucketToDiskIfPersistent(vbid, 1);
        EXPECT_EQ(expectedItemsInHashTable, vb->ht.getNumInMemoryItems());
        EXPECT_EQ(expectedItemsInVBucket, vb->getNumItems());
    }

    void createPendingWrite() {
        auto item = makePendingItem(key, "value");
        EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*item, cookie));
        flushVBucketToDiskIfPersistent(vbid, 1);
        expectedItemsInHashTable++;
        EXPECT_EQ(expectedItemsInHashTable, vb->ht.getNumInMemoryItems());
        EXPECT_EQ(expectedItemsInVBucket, vb->getNumItems());
    }

    void commit() {
        EXPECT_EQ(ENGINE_SUCCESS,
                  vb->commit(key,
                             2 /*prepareSeqno*/,
                             {} /*commitSeqno*/,
                             vb->lockCollections(key)));

        if (!persistent()) {
            // ep bucket will replace, ephmeral keeps a second item (tested
            // below)
            expectedItemsInHashTable++;
        }
        expectedItemsInVBucket++;
        EXPECT_EQ(expectedItemsInHashTable, vb->ht.getNumInMemoryItems());
        EXPECT_EQ(expectedItemsInVBucket, vb->getNumItems());
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
        EXPECT_EQ(expectedItemsInHashTable, vb->ht.getNumInMemoryItems());
        EXPECT_EQ(expectedItemsInVBucket, vb->getNumItems());
    }

    void runCollectionsEraser(bool commitWrite) {
        CollectionsEraserTest::runCollectionsEraser();

        if (commitWrite) {
            expectedItemsInVBucket--;
            expectedItemsInHashTable -= 2;
        } else {
            expectedItemsInHashTable--;
        }
    }

    CollectionsManifest cm{};
    StoredDocKey key = makeStoredDocKey("key", CollectionEntry::dairy);
    int expectedItemsInHashTable = 0;
    int expectedItemsInVBucket = 0;
};

void CollectionsEraserSyncWriteTest::basicDropWithSyncWrite(bool commitWrite) {
    // For now only run this test on ephemeral - it was written to exercise an
    // ephemeral path that was crashing (MB-38856). Overall sync-writes and
    // collection drop don't work yet (MB-34217) and if we run this test with
    // persistence a few other checks fail (and one exception is seen)
    if (persistent()) {
        return;
    }

    addCollection();
    createPendingWrite();
    if (commitWrite) {
        commit();
    }

    if (!persistent()) {
        auto pending = vb->ht.findForUpdate(key).pending;
        ASSERT_TRUE(pending);
        EXPECT_EQ(commitWrite, pending->isCompleted());
    }

    if (commitWrite) {
        EXPECT_EQ(1, vb->getNumItems());
    }

    dropCollection();

    // persistent won't purge until drop is flushed
    runCollectionsEraser(commitWrite);
}

TEST_P(CollectionsEraserSyncWriteTest, BasicDropWithCommittedSyncWrite) {
    basicDropWithSyncWrite(true /*commit*/);
}

TEST_P(CollectionsEraserSyncWriteTest, BasicDropWithPendingSyncWrite) {
    basicDropWithSyncWrite(false /*commit*/);
}

TEST_P(CollectionsEraserSyncWriteTest, DropBeforeAbort) {
    addCollection();
    createPendingWrite();
    dropCollection();
    abort(); // MB-38979: would of thrown an exception
    expectedItemsInHashTable++; // MB-34217: vb->abort put one item in the ht
    runCollectionsEraser(false);
}

TEST_P(CollectionsEraserSyncWriteTest, DropAfterAbort) {
    addCollection();
    createPendingWrite();
    abort();
    dropCollection();
    runCollectionsEraser(false);
}

// Test cases which run for persistent and ephemeral buckets
INSTANTIATE_TEST_SUITE_P(CollectionsEraserTests,
                         CollectionsEraserTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(CollectionsEraserSyncWriteTests,
                         CollectionsEraserSyncWriteTest,
                         // @todo: run with persistence - disabled persistence
                         // tests to save on wasted start/stop time
                         STParameterizedBucketTest::ephConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);
