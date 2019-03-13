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

/**
 * Tests for Rollback functionality in EPStore.
 */

#include "checkpoint_manager.h"
#include "dcp/active_stream_checkpoint_processor_task.h"
#include "dcp/dcpconnmap.h"
#include "dcp/producer.h"
#include "dcp/stream.h"
#include "dcp_utils.h"
#include "evp_store_single_threaded_test.h"
#include "evp_store_test.h"
#include "failover-table.h"
#include "programs/engine_testapp/mock_server.h"
#include "tests/mock/mock_checkpoint_manager.h"
#include "tests/mock/mock_dcp.h"
#include "tests/mock/mock_dcp_consumer.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/collections/test_manifest.h"
#include "tests/module_tests/test_helpers.h"
#include <engines/ep/tests/mock/mock_dcp_conn_map.h>

#include <mcbp/protocol/framebuilder.h>
#include <memcached/server_cookie_iface.h>

class RollbackTest : public SingleThreadedEPBucketTest,
                     public ::testing::WithParamInterface<
                             std::tuple<std::string, std::string>> {
    void SetUp() override {
        config_string += "item_eviction_policy=" + std::get<0>(GetParam());
        SingleThreadedEPBucketTest::SetUp();
        if (std::get<1>(GetParam()) == "pending") {
            vbStateAtRollback = vbucket_state_pending;
        } else {
            vbStateAtRollback = vbucket_state_replica;
        }

        // Start vbucket as active to allow us to store items directly to it.
        store->setVBucketState(vbid, vbucket_state_active);

        // For any rollback tests which actually want to rollback, we need
        // to ensure that we don't rollback more than 50% of the seqno count
        // as then the VBucket is just cleared (it'll instead expect a resync
        // from zero.
        // Therefore create 10 dummy items which we don't otherwise care
        // about (most of the Rollback test only work with a couple of
        // "active" items.
        const auto dummy_elements = size_t{5};
        for (size_t ii = 1; ii <= dummy_elements; ii++) {
            auto res = store_item(vbid,
                                  makeStoredDocKey("dummy" + std::to_string(ii)),
                                  "dummy");
            ASSERT_EQ(ii, res.getBySeqno());
        }
        ASSERT_EQ(std::make_pair(false, dummy_elements),
                  getEPBucket().flushVBucket(vbid));
        initial_seqno = dummy_elements;
    }

protected:
    /*
     * Fake callback emulating dcp_add_failover_log
     */
    static ENGINE_ERROR_CODE fakeDcpAddFailoverLog(
            vbucket_failover_t* entry,
            size_t nentries,
            gsl::not_null<const void*> cookie) {
        return ENGINE_SUCCESS;
    }

public:
    /**
     * Test rollback after deleting an item.
     * @param flush_before_rollback: Should the vbucket be flushed to disk just
     *        before the rollback (i.e. guaranteeing the in-memory state is in
     *        sync with disk).
     * @param expire_item: Instead of deleting the item, expire the item using
     *        an expiry time and triggering it via a get.
     */
    void rollback_after_deletion_test(bool flush_before_rollback,
                                      bool expire_item) {
        // Setup: Store an item then flush the vBucket (creating a checkpoint);
        // then delete/expire the item and create a second checkpoint.
        StoredDocKey a = makeStoredDocKey("key");
        auto expiryTime = 0;
        if (expire_item) {
            expiryTime = 10;
        }
        auto item_v1 = store_item(vbid, a, "1", expiryTime);
        ASSERT_EQ(initial_seqno + 1, item_v1.getBySeqno());
        ASSERT_EQ(std::make_pair(false, size_t(1)),
                  getEPBucket().flushVBucket(vbid));
        if (expire_item) {
            // Move time forward and trigger expiry on a get.
            TimeTraveller arron(expiryTime * 2);
            ASSERT_EQ(
                    ENGINE_KEY_ENOENT,
                    store->get(a, vbid, /*cookie*/ nullptr, get_options_t::NONE)
                            .getStatus());
        } else {
            uint64_t cas = item_v1.getCas();
            mutation_descr_t mutation_descr;
            ASSERT_EQ(ENGINE_SUCCESS,
                      store->deleteItem(a,
                                        cas,
                                        vbid,
                                        /*cookie*/ nullptr,
                                        {},
                                        /*itemMeta*/ nullptr,
                                        mutation_descr));
        }
        if (flush_before_rollback) {
            ASSERT_EQ(std::make_pair(false, size_t(1)),
                      getEPBucket().flushVBucket(vbid));
        }

        // Sanity-check - item should no longer exist.
        // If we're in value_only or have not flushed in full_eviction then
        // we don't need to do a bg fetch
        if (std::get<0>(GetParam()) == "value_only" || !flush_before_rollback) {
            EXPECT_EQ(ENGINE_KEY_ENOENT,
                      store->get(a, vbid, nullptr, {}).getStatus());
        } else {
            EXPECT_EQ(ENGINE_EWOULDBLOCK,
                      store->get(a, vbid, nullptr, QUEUE_BG_FETCH).getStatus());
            // Manually run the bgfetch task.
            runBGFetcherTask();
            EXPECT_EQ(ENGINE_KEY_ENOENT,
                      store->get(a, vbid, nullptr, QUEUE_BG_FETCH).getStatus());
        }

        // Test - rollback to seqno of item_v1 and verify that the previous value
        // of the item has been restored.
        store->setVBucketState(vbid, vbStateAtRollback);
        ASSERT_EQ(TaskStatus::Complete,
                  store->rollback(vbid, item_v1.getBySeqno()));
        auto result =
                getInternal(a, vbid, /*cookie*/ nullptr, vbStateAtRollback, {});
        ASSERT_EQ(ENGINE_SUCCESS, result.getStatus());
        EXPECT_EQ(item_v1, *result.item)
                << "Fetched item after rollback should match item_v1";

        if (!flush_before_rollback) {
            EXPECT_EQ(std::make_pair(false, size_t(0)),
                      getEPBucket().flushVBucket(vbid));
        }
    }

    // Test rollback after modifying an item.
    void rollback_after_mutation_test(bool flush_before_rollback) {
        // Setup: Store an item then flush the vBucket (creating a checkpoint);
        // then update the item with a new value and create a second checkpoint.
        StoredDocKey a = makeStoredDocKey("a");
        auto item_v1 = store_item(vbid, a, "old");
        ASSERT_EQ(initial_seqno + 1, item_v1.getBySeqno());
        ASSERT_EQ(std::make_pair(false, size_t(1)),
                  getEPBucket().flushVBucket(vbid));

        auto item2 = store_item(vbid, a, "new");
        ASSERT_EQ(initial_seqno + 2, item2.getBySeqno());

        StoredDocKey key = makeStoredDocKey("key");
        store_item(vbid, key, "meh");

        if (flush_before_rollback) {
            EXPECT_EQ(std::make_pair(false, size_t(2)),
                      getEPBucket().flushVBucket(vbid));
        }

        // Test - rollback to seqno of item_v1 and verify that the previous
        // value of the item has been restored.
        store->setVBucketState(vbid, vbStateAtRollback);
        ASSERT_EQ(TaskStatus::Complete,
                  store->rollback(vbid, item_v1.getBySeqno()));
        ASSERT_EQ(item_v1.getBySeqno(),
                  store->getVBucket(vbid)->getHighSeqno());

        // a should have the value of 'old'
        {
            auto result = store->get(a, vbid, nullptr, {});
            ASSERT_EQ(ENGINE_SUCCESS, result.getStatus());
            EXPECT_EQ(item_v1, *result.item)
                    << "Fetched item after rollback should match item_v1";
        }

        // key should be gone
        {
            auto result = store->get(key, vbid, nullptr, {});
            EXPECT_EQ(ENGINE_KEY_ENOENT, result.getStatus())
                    << "A key set after the rollback point was found";
        }

        if (!flush_before_rollback) {
            // The rollback should of wiped out any keys waiting for persistence
            EXPECT_EQ(std::make_pair(false, size_t(0)),
                      getEPBucket().flushVBucket(vbid));
        }
    }

protected:
    /**
     * Test what happens in scenarios where we delete and rollback a
     * document that only existed post rollback seqno.
     *
     * @param deleteLast - should the last operation be the deletion of the
     *        document
     * @param flushOnce - should we only flush once before the rollback (or
     *        should we flush after each operation)
     */
    void rollback_after_creation_and_deletion_test(bool deleteLast,
                                                   bool flushOnce) {
        auto rbSeqno = store->getVBucket(vbid)->getHighSeqno();

        // Setup
        StoredDocKey a = makeStoredDocKey("a");
        if (deleteLast) {
            store_item(vbid, a, "new");
            if (!flushOnce) {
                ASSERT_EQ(std::make_pair(false, size_t(1)),
                          getEPBucket().flushVBucket(vbid));
            }
            delete_item(vbid, a);
            if (!flushOnce) {
                ASSERT_EQ(std::make_pair(false, size_t(1)),
                          getEPBucket().flushVBucket(vbid));
            }
        } else {
            // Make sure we have something to delete
            store_item(vbid, a, "new");
            if (!flushOnce) {
                ASSERT_EQ(std::make_pair(false, size_t(1)),
                          getEPBucket().flushVBucket(vbid));
            }
            delete_item(vbid, a);
            if (!flushOnce) {
                ASSERT_EQ(std::make_pair(false, size_t(1)),
                          getEPBucket().flushVBucket(vbid));
            }
            store_item(vbid, a, "new");
            if (!flushOnce) {
                ASSERT_EQ(std::make_pair(false, size_t(1)),
                          getEPBucket().flushVBucket(vbid));
            }
        }

        if (flushOnce) {
            ASSERT_EQ(std::make_pair(false, size_t(1)),
                      getEPBucket().flushVBucket(vbid));
        }

        // Test - rollback to seqno before this test
        store->setVBucketState(vbid, vbStateAtRollback);
        ASSERT_EQ(TaskStatus::Complete, store->rollback(vbid, rbSeqno));
        ASSERT_EQ(rbSeqno, store->getVBucket(vbid)->getHighSeqno());

        if (std::get<0>(GetParam()) == "value_only") {
            EXPECT_EQ(ENGINE_KEY_ENOENT,
                      store->get(a, vbid, nullptr, {}).getStatus());
        } else {
            GetValue gcb = store->get(a, vbid, nullptr, QUEUE_BG_FETCH);

            if (flushOnce && !deleteLast) {
                EXPECT_EQ(ENGINE_KEY_ENOENT, gcb.getStatus());
            } else {
                // We only need to bg fetch the document if we deduplicate a
                // create + delete + create which is flushed in one go
                EXPECT_EQ(ENGINE_EWOULDBLOCK, gcb.getStatus());
                // Manually run the bgfetch task.
                runBGFetcherTask();
                EXPECT_EQ(ENGINE_KEY_ENOENT,
                          store->get(a, vbid, nullptr, QUEUE_BG_FETCH)
                                  .getStatus());
            }
        }
    }

    // Check the stats after rolling back
    void rollback_stat_test(int expectedDifference,
                            std::function<void()> test) {
        // Everything will be in the default collection
        auto vb = store->getVBucket(vbid);

        // Get the starting item count
        auto startDefaultCollectionCount =
                vb->getManifest().lock().getItemCount(CollectionID::Default);
        auto startVBCount = vb->getNumItems();

        auto startHighSeqno = vb->getManifest().lock().getPersistedHighSeqno(
                CollectionID::Default);

        test();

        EXPECT_EQ(startDefaultCollectionCount + expectedDifference,
                  vb->getManifest().lock().getItemCount(CollectionID::Default));
        EXPECT_EQ(startVBCount + expectedDifference, vb->getNumItems());

        EXPECT_EQ(startHighSeqno + expectedDifference,
                  vb->getManifest().lock().getPersistedHighSeqno(
                          CollectionID::Default));
    }

    // Check the stats after rolling back a creation and deletion
    void rollback_create_delete_stat_test(bool deleteLast,
                                          bool flushOnce,
                                          int expectedDifference) {
        // Everything will be in the default collection
        auto vb = store->getVBucket(vbid);

        // Get the starting item count
        auto startDefaultCollectionCount =
                vb->getManifest().lock().getItemCount(CollectionID::Default);
        auto startVBCount = vb->getNumItems();

        auto startPHighSeqno = vb->getManifest().lock().getPersistedHighSeqno(
                CollectionID::Default);

        auto startHighSeqno =
                vb->getManifest().lock().getHighSeqno(CollectionID::Default);

        rollback_after_creation_and_deletion_test(deleteLast, flushOnce);

        // Item counts should be the start + the expectedDifference
        EXPECT_EQ(startDefaultCollectionCount + expectedDifference,
                  vb->getManifest().lock().getItemCount(CollectionID::Default));
        EXPECT_EQ(startVBCount + expectedDifference, vb->getNumItems());

        EXPECT_EQ(startPHighSeqno + expectedDifference,
                  vb->getManifest().lock().getPersistedHighSeqno(
                          CollectionID::Default));

        EXPECT_EQ(startHighSeqno + expectedDifference,
                  vb->getManifest().lock().getHighSeqno(CollectionID::Default));
    }

    void rollback_to_middle_test(bool flush_before_rollback,
                                 bool rollbackCollectionCreate = false) {
        // create some more checkpoints just to see a few iterations
        // of parts of the rollback function.

        // need to store a certain number of keys because rollback
        // 'bails' if the rollback is too much.
        for (int i = 0; i < 7; i++) {
            store_item(vbid, makeStoredDocKey("key_" + std::to_string(i)), "dontcare");
        }

        // rollbackCollectionCreate==true and the roll back function will
        // rewind disk to here, with dairy collection unknown
        auto seqno1 = store->getVBucket(vbid)->getHighSeqno();
        ASSERT_EQ(std::make_pair(false, size_t(7)),
                  getEPBucket().flushVBucket(vbid));

        auto vb = store->getVBucket(vbid);
        CollectionsManifest cm;
        // the roll back function will rewind disk to this collection state
        vb->updateFromManifest({cm.add(CollectionEntry::dairy)});

        // rollbackCollectionCreate==false and the roll back function will
        // rewind disk to key7, with dairy collection open
        auto rollback_item =
                store_item(vbid,
                           makeStoredDocKey("key7", CollectionEntry::dairy),
                           "key7 in the dairy collection");
        ASSERT_EQ(std::make_pair(false, size_t(2)),
                  getEPBucket().flushVBucket(vbid));

        // every key past this point will be lost from disk in a mid-point.
        auto item_v1 = store_item(vbid, makeStoredDocKey("rollback-cp-1"), "keep-me");
        auto item_v2 = store_item(vbid, makeStoredDocKey("rollback-cp-2"), "rollback to me");
        store_item(vbid, makeStoredDocKey("rollback-cp-3"), "i'm gone");

        // Select the rollback seqno
        auto rollback = rollbackCollectionCreate
                                ? rollback_item.getBySeqno() - 1
                                : item_v2.getBySeqno();
        ASSERT_EQ(std::make_pair(false, size_t(3)),
                  getEPBucket().flushVBucket(vbid));

        cm.remove(CollectionEntry::dairy);
        vb->updateFromManifest({cm});

        // Expect failure to store
        store_item(vbid,
                   makeStoredDocKey("fail", CollectionEntry::dairy),
                   "unknown collection",
                   0,
                   {cb::engine_errc::unknown_collection});

        for (int i = 0; i < 2; i++) {
            store_item(vbid,
                       makeStoredDocKey("anotherkey_" + std::to_string(i)),
                       "default collection");
        }

        if (flush_before_rollback) {
            ASSERT_EQ(std::make_pair(false, size_t(3)),
                      getEPBucket().flushVBucket(vbid));
        }

        // Rollback should succeed, but rollback to 0
        store->setVBucketState(vbid, vbStateAtRollback);
        EXPECT_EQ(TaskStatus::Complete, store->rollback(vbid, rollback));

        // These keys should be gone after the rollback
        for (int i = 0; i < 3; i++) {
            auto result = store->get(
                    makeStoredDocKey("rollback-cp-" + std::to_string(i)),
                    vbid,
                    cookie,
                    {});
            EXPECT_EQ(ENGINE_KEY_ENOENT, result.getStatus())
                << "A key set after the rollback point was found";
        }

        // These keys should be gone after the rollback
        for (int i = 0; i < 2; i++) {
            auto result = store->get(
                    makeStoredDocKey("anotherkey_" + std::to_string(i)),
                    vbid,
                    cookie,
                    {});
            EXPECT_EQ(ENGINE_KEY_ENOENT, result.getStatus())
                << "A key set after the rollback point was found";
        }

        if (rollbackCollectionCreate) {
            // Dairy collection should of been rolled away
            auto result =
                    store->get(makeStoredDocKey("key7", CollectionEntry::dairy),
                               vbid,
                               cookie,
                               {});
            EXPECT_EQ(ENGINE_UNKNOWN_COLLECTION, result.getStatus());
            // Rolled back to the previous checkpoint before dairy
            EXPECT_EQ(seqno1, store->getVBucket(vbid)->getHighSeqno());
        } else {
            // And key7 from dairy collection, can I GET it?
            auto result =
                    store->get(makeStoredDocKey("key7", CollectionEntry::dairy),
                               vbid,
                               cookie,
                               {});
            EXPECT_EQ(ENGINE_SUCCESS, result.getStatus())
                    << "Failed to find key7"; // Yes you can!
            // Rolled back to the previous checkpoint
            EXPECT_EQ(rollback_item.getBySeqno(),
                      store->getVBucket(vbid)->getHighSeqno());
        }
        EXPECT_EQ(0, store->getVBucket(vbid)->ht.getNumSystemItems());
    }

protected:
    int64_t initial_seqno;
    vbucket_state_t vbStateAtRollback;
};

TEST_P(RollbackTest, RollbackAfterMutation) {
    rollback_after_mutation_test(/*flush_before_rollbaack*/true);
}

TEST_P(RollbackTest, RollbackAfterMutationNoFlush) {
    rollback_after_mutation_test(/*flush_before_rollback*/false);
}

TEST_P(RollbackTest, RollbackAfterDeletion) {
    rollback_after_deletion_test(/*flush_before_rollback*/ true,
                                 /*expire_item*/ false);
}

TEST_P(RollbackTest, RollbackAfterDeletionNoFlush) {
    rollback_after_deletion_test(/*flush_before_rollback*/ false,
                                 /*expire_item*/ false);
}

TEST_P(RollbackTest, RollbackAfterExpiration) {
    rollback_after_deletion_test(/*flush_before_rollback*/ true,
                                 /*expire_item*/ true);
}

TEST_P(RollbackTest, RollbackAfterExpirationNoFlush) {
    rollback_after_deletion_test(/*flush_before_rollback*/ false,
                                 /*expire_item*/ true);
}

TEST_P(RollbackTest, RollbackToMiddleOfAPersistedSnapshot) {
    rollback_to_middle_test(true);
}

TEST_P(RollbackTest, RollbackToMiddleOfAPersistedSnapshotNoFlush) {
    rollback_to_middle_test(false);
}

TEST_P(RollbackTest, RollbackCollectionCreate1) {
    // run the middle test but request the rollback is to before a collection
    // create
    rollback_to_middle_test(true, true);
}

TEST_P(RollbackTest, RollbackCollectionCreate2) {
    // run the middle test but request the rollback is to before a collection
    // create
    rollback_to_middle_test(false, true);
}

// Test what happens when we rollback the creation and the mutation of
// different documents that are persisted
TEST_P(RollbackTest, RollbackMutationDocCounts) {
    rollback_stat_test(
            1,
            std::bind(&RollbackTest::rollback_after_mutation_test, this, true));
}

// Test what happens when we rollback the creation and the mutation of
// different documents that are not persisted
TEST_P(RollbackTest, RollbackMutationDocCountsNoFlush) {
    rollback_stat_test(
            1,
            std::bind(
                    &RollbackTest::rollback_after_mutation_test, this, false));
}

// Test what happens when we rollback a deletion of a document that existed
// before rollback that has been persisted
TEST_P(RollbackTest, RollbackDeletionDocCounts) {
    rollback_stat_test(1,
                       std::bind(&RollbackTest::rollback_after_deletion_test,
                                 this,
                                 true,
                                 false));
}

// Test what happens when we rollback a deletion of a document that existed
// before rollback that has not been persisted
TEST_P(RollbackTest, RollbackDeletionDocCountsNoFlush) {
    rollback_stat_test(1,
                       std::bind(&RollbackTest::rollback_after_deletion_test,
                                 this,
                                 false,
                                 false));
}

// Test what happens if we rollback the creation and deletion of a document
// when we flush them separately
TEST_P(RollbackTest, RollbackCreationAndDeletionDocCountsSeparateFlushes) {
    // Doc count should not change
    rollback_create_delete_stat_test(true, false, 0);
}

// Test what happens if we rollback the creation and deletion of a document
// when we flush them in one go
TEST_P(RollbackTest, RollbackCreationAndDeletionDocCountsOneFlush) {
    // Doc count should not change
    rollback_create_delete_stat_test(true, true, 0);
}

// Test what happens if we rollback the creation + deletion + creation of a
// document when we flush them separately
TEST_P(RollbackTest, RollbackDeletionAndCreationDocCountsSeparateFlushes) {
    // Doc count should not change
    rollback_create_delete_stat_test(false, false, 0);
}

// Test what happens if we rollback the creation + deletion + creation of a
// document when we flush them in one go
TEST_P(RollbackTest, RollbackDeletionAndCreationDocCountsOneFlush) {
    // Doc count should not change
    rollback_create_delete_stat_test(false, true, 0);
}

// Test what happens to the doc counts if we rollback the vBucket completely
TEST_P(RollbackTest, RollbackFromZeroDocCounts) {
    // Trigger a rollback to zero by rolling back to a seqno just before the
    // halfway point between start and end (2 in this case)
    store->setVBucketState(vbid, vbStateAtRollback);
    auto docKey = makeStoredDocKey("dummy1");
    ASSERT_EQ(
            TaskStatus::Complete,
            store->rollback(
                    vbid,
                    store->get(docKey, vbid, nullptr, {}).item->getBySeqno()));

    // No items at all
    ASSERT_EQ(0, store->getVBucket(vbid)->getHighSeqno());
    EXPECT_EQ(0,
              store->getVBucket(vbid)->getManifest().lock().getItemCount(
                      CollectionID::Default));
    EXPECT_EQ(
            0,
            store->getVBucket(vbid)->getManifest().lock().getPersistedHighSeqno(
                    CollectionID::Default));
    EXPECT_EQ(0, store->getVBucket(vbid)->getNumItems());
}

TEST_P(RollbackTest, RollbackToMiddleOfAnUnPersistedSnapshot) {
    /* need to store a certain number of keys because rollback
       'bails (rolls back to 0)' if the rollback is too much. */
    const size_t numItems = 10;
    for (size_t i = 0; i < numItems; i++) {
        store_item(vbid,
                   makeStoredDocKey("key_" + std::to_string(i)),
                   "not rolled back");
    }

    /* the roll back function will rewind disk to key11. */
    auto rollback_item =
            store_item(vbid, makeStoredDocKey("key11"), "rollback pt");

    ASSERT_EQ(std::make_pair(false, numItems + 1),
              getEPBucket().flushVBucket(vbid));

    /* Keys to be lost in rollback */
    auto item_v1 = store_item(
            vbid, makeStoredDocKey("rollback-cp-1"), "hope to keep till here");
    /* ask to rollback to here; this item is in a checkpoint and
       is not persisted */
    auto rollbackReqSeqno = item_v1.getBySeqno();

    auto item_v2 = store_item(vbid, makeStoredDocKey("rollback-cp-2"), "gone");

    /* do rollback */
    store->setVBucketState(vbid, vbStateAtRollback);
    EXPECT_EQ(TaskStatus::Complete, store->rollback(vbid, rollbackReqSeqno));

    /* confirm that we have rolled back to the disk snapshot */
    EXPECT_EQ(rollback_item.getBySeqno(),
              store->getVBucket(vbid)->getHighSeqno());

    /* since we rely only on disk snapshots currently, we must lose the items in
       the checkpoints */
    for (int i = 0; i < 2; i++) {
        auto result =
                store->get(makeStoredDocKey("rollback-cp-" + std::to_string(i)),
                           vbid,
                           nullptr,
                           {});
        EXPECT_EQ(ENGINE_KEY_ENOENT, result.getStatus())
                << "A key set after the rollback point was found";
    }
}

/*
 * The opencheckpointid of a bucket is one after a rollback and
 * the receivingInitialDiskSnapshot flag is false.
 */
TEST_P(RollbackTest, MB21784) {
    // Make the vbucket a replica
    store->setVBucketState(vbid, vbucket_state_replica);
    // Perform a rollback
    EXPECT_EQ(TaskStatus::Complete, store->rollback(vbid, initial_seqno))
            << "rollback did not return success";

    // Assert the checkpointmanager clear function (called during rollback)
    // has set the opencheckpointid to one
    auto vb = store->getVBucket(vbid);
    auto& ckpt_mgr = *vb->checkpointManager;
    EXPECT_EQ(1, ckpt_mgr.getOpenCheckpointId()) << "opencheckpointId not one";
    EXPECT_FALSE(vb->isReceivingInitialDiskSnapshot())
            << "receivingInitialDiskSnapshot is true";

    // Create a new Dcp producer, reserving its cookie.
    get_mock_server_api()->cookie->reserve(cookie);
    DcpProducer* producer = engine->getDcpConnMap().newProducer(
            cookie, "test_producer", /*flags*/ 0);

    uint64_t rollbackSeqno;
    auto err = producer->streamRequest(/*flags*/ 0,
                                       /*opaque*/ 0,
                                       /*vbucket*/ vbid,
                                       /*start_seqno*/ 0,
                                       /*end_seqno*/ 0,
                                       /*vb_uuid*/ 0,
                                       /*snap_start*/ 0,
                                       /*snap_end*/ 0,
                                       &rollbackSeqno,
                                       RollbackTest::fakeDcpAddFailoverLog,
                                       {});
    EXPECT_EQ(ENGINE_SUCCESS, err)
        << "stream request did not return ENGINE_SUCCESS";
    // Close stream
    ASSERT_EQ(ENGINE_SUCCESS, producer->closeStream(/*opaque*/0, vbid));
    engine->handleDisconnect(cookie);
}

TEST_P(RollbackTest, RollbackOnActive) {
    /* Store 3 items */
    const int numItems = 3;
    for (int i = 0; i < numItems; i++) {
        store_item(vbid,
                   makeStoredDocKey("key_" + std::to_string(i)),
                   "not rolled back");
    }

    /* Try to rollback on active (default state) vbucket */
    EXPECT_EQ(TaskStatus::Abort,
              store->rollback(vbid, numItems - 1 /*rollbackReqSeqno*/));

    EXPECT_EQ(TaskStatus::Abort, store->rollback(vbid, 0 /*rollbackReqSeqno*/));
}

class RollbackDcpTest : public SingleThreadedEPBucketTest,
                        public ::testing::WithParamInterface<
                                std::tuple<std::string, std::string>> {
public:
    // Mock implementation of dcp_message_producers which ... TODO
    class DcpProducers;

    RollbackDcpTest() {
    }

    void SetUp() override {
        config_string += "item_eviction_policy=" + std::get<0>(GetParam());
        SingleThreadedEPBucketTest::SetUp();
        if (std::get<1>(GetParam()) == "pending") {
            vbStateAtRollback = vbucket_state_pending;
        } else {
            vbStateAtRollback = vbucket_state_replica;
        }

        store->setVBucketState(vbid, vbucket_state_active);
        consumer = std::make_shared<MockDcpConsumer>(
                *engine, cookie, "test_consumer");
        vb = store->getVBucket(vbid);
    }

    void TearDown() override {
        consumer->closeAllStreams();
        consumer.reset();
        vb.reset();
        SingleThreadedEPBucketTest::TearDown();
    }

    // build a rollback response command
    std::unique_ptr<char[]> getRollbackResponse(uint32_t opaque,
                                                uint64_t rollbackSeq) const {
        auto msg = std::make_unique<char[]>(
                sizeof(protocol_binary_response_header) + sizeof(uint64_t));
        cb::mcbp::ResponseBuilder builder(
                {reinterpret_cast<uint8_t*>(msg.get()),
                 sizeof(protocol_binary_response_header) + sizeof(uint64_t)});

        builder.setMagic(cb::mcbp::Magic::ClientResponse);
        builder.setOpcode(cb::mcbp::ClientOpcode::DcpStreamReq);
        builder.setStatus(cb::mcbp::Status::Rollback);
        builder.setOpaque(opaque);
        uint64_t encoded = htonll(rollbackSeq);
        builder.setValue(
                {reinterpret_cast<const uint8_t*>(&encoded), sizeof(encoded)});
        return msg;
    }

    static struct StreamRequestData {
        bool called;
        uint32_t opaque;
        Vbid vbucket;
        uint32_t flags;
        uint64_t start_seqno;
        uint64_t end_seqno;
        uint64_t vbucket_uuid;
        uint64_t snap_start_seqno;
        uint64_t snap_end_seqno;
    } streamRequestData;

    class DcpProducers : public MockDcpMessageProducers {
    public:
        ENGINE_ERROR_CODE stream_req(
                uint32_t opaque,
                Vbid vbucket,
                uint32_t flags,
                uint64_t start_seqno,
                uint64_t end_seqno,
                uint64_t vbucket_uuid,
                uint64_t snap_start_seqno,
                uint64_t snap_end_seqno,
                const std::string& vb_manifest_uid) override {
            streamRequestData = {true,
                                 opaque,
                                 vbucket,
                                 flags,
                                 start_seqno,
                                 end_seqno,
                                 vbucket_uuid,
                                 snap_start_seqno,
                                 snap_end_seqno};

            return ENGINE_SUCCESS;
        }
    };

    void stepForStreamRequest(uint64_t startSeqno, uint64_t vbUUID) {
        while (consumer->step(&producers) == ENGINE_SUCCESS) {
            handleProducerResponseIfStepBlocked(*consumer, producers);
        }
        EXPECT_TRUE(streamRequestData.called);
        EXPECT_EQ(startSeqno, streamRequestData.start_seqno);
        EXPECT_EQ(startSeqno ? vbUUID : 0, streamRequestData.vbucket_uuid);
        streamRequestData = {};
    }

    void createItems(int items, int flushes) {
        // Flush multiple checkpoints of unique keys
        for (int ii = 0; ii < flushes; ii++) {
            std::string key = "anykey_" + std::to_string(ii) + "_";
            EXPECT_TRUE(store_items(items,
                                    vbid,
                                    {key, DocKeyEncodesCollectionId::No},
                                    "value"));
            flush_vbucket_to_disk(vbid, items);
            // Add an entry for this seqno
            vb->failovers->createEntry(items * (ii + 1));
        }

        store->setVBucketState(vbid, vbStateAtRollback);
    }

    uint64_t addStream(int nitems) {
        consumer->addStream(/*opaque*/ 0, vbid, /*flags*/ 0);
        // Step consumer to retrieve the first stream request.
        uint64_t vbUUID = vb->failovers->getLatestEntry().vb_uuid;
        stepForStreamRequest(nitems, vbUUID);
        return vbUUID;
    }

    void responseNoRollback(int nitems,
                            uint64_t rollbackSeq,
                            uint64_t previousUUID) {
        // Now push a reponse to the consumer, saying rollback to 0.
        // The consumer must ignore the 0 rollback and retry a stream-request
        // with the next failover entry.
        auto msg = getRollbackResponse(1 /*opaque*/, rollbackSeq);
        EXPECT_TRUE(consumer->handleResponse(
                reinterpret_cast<protocol_binary_response_header*>(msg.get())));

        // Consumer should of added a StreamRequest with a different vbuuid
        EXPECT_NE(previousUUID, vb->failovers->getLatestEntry().vb_uuid);

        stepForStreamRequest(nitems, vb->failovers->getLatestEntry().vb_uuid);
    }

    void responseRollback(uint64_t rollbackSeq) {
        // Now push a reponse to the consumer, saying rollback to 0.
        // The consumer must ignore the 0 rollback and retry a stream-request
        // with the next failover entry.
        auto msg = getRollbackResponse(1 /*opaque*/, rollbackSeq);
        EXPECT_TRUE(consumer->handleResponse(
                reinterpret_cast<protocol_binary_response_header*>(msg.get())));

        // consumer must of scheduled a RollbackTask (writer task)
        auto& lpWriteQ = *task_executor->getLpTaskQ()[WRITER_TASK_IDX];
        ASSERT_EQ(1, lpWriteQ.getFutureQueueSize());
        runNextTask(lpWriteQ);
    }

    std::shared_ptr<MockDcpConsumer> consumer;
    DcpProducers producers;
    VBucketPtr vb;
    vbucket_state_t vbStateAtRollback;
};

RollbackDcpTest::StreamRequestData RollbackDcpTest::streamRequestData = {};

/**
 * Push stream responses to a consumer and test
 * 1. The first rollback to 0 response is ignored, the consumer requests again
 *    with new data.
 * 2. The second rollback to 0 response triggers a rollback to 0.
 */
TEST_P(RollbackDcpTest, test_rollback_zero) {
    const int items = 40;
    const int flushes = 1;
    const int nitems = items * flushes;
    const int rollbackPoint = 0; // expect final rollback to be to 0

    // Test will create anykey_0_{0..items-1}
    createItems(items, flushes);

    auto uuid = addStream(nitems);

    responseNoRollback(nitems, 0, uuid);

    // All keys available
    for (int ii = 0; ii < items; ii++) {
        std::string key = "anykey_0_" + std::to_string(ii);
        auto result = store->get(
                {key, DocKeyEncodesCollectionId::No}, vbid, nullptr, {});
        EXPECT_EQ(ENGINE_SUCCESS, result.getStatus()) << "Problem with " << key;
    }

    responseRollback(rollbackPoint);

    // All keys now gone
    for (int ii = 0; ii < items; ii++) {
        std::string key = "anykey_0_" + std::to_string(ii);
        auto result = store->get(
                {key, DocKeyEncodesCollectionId::No}, vbid, nullptr, {});
        EXPECT_EQ(ENGINE_KEY_ENOENT, result.getStatus()) << "Problem with "
                                                         << key;
    }

    // Expected a rollback to 0 which is a VB reset, so discard the now dead
    // vb and obtain replacement
    vb = store->getVBucket(vbid);

    // Rollback complete and will have posted a new StreamRequest
    stepForStreamRequest(rollbackPoint,
                         vb->failovers->getLatestEntry().vb_uuid);
    EXPECT_EQ(rollbackPoint, vb->getHighSeqno()) << "VB hasn't rolled back to "
                                                 << rollbackPoint;
}

/**
 * Push stream responses to a consumer and test
 * 1. The first rollback to 0 response is ignored, the consumer requests again
 *    with new data.
 * 2. The second rollback response is non-zero, and the consumer accepts that
 *    and rolls back to the rollbackPoint and requests a stream for it.
 */
TEST_P(RollbackDcpTest, test_rollback_nonzero) {
    const int items = 10;
    const int flushes = 4;
    const int nitems = items * flushes;
    const int rollbackPoint = 3 * items; // rollback to 3/4

    // Test will create anykey_{0..flushes-1}_{0..items-1}
    createItems(items, flushes);

    auto uuid = addStream(nitems);

    responseNoRollback(nitems, 0, uuid);

    // All keys available
    for (int ii = 0; ii < items; ii++) {
        for (int ff = 0; ff < flushes; ff++) {
            std::string key =
                    "anykey_" + std::to_string(ff) + "_" + std::to_string(ii);
            auto result = store->get(
                    {key, DocKeyEncodesCollectionId::No}, vbid, nullptr, {});
            EXPECT_EQ(ENGINE_SUCCESS, result.getStatus()) << "Expected to find "
                                                          << key;
        }
    }

    responseRollback(rollbackPoint);

    // 3/4 keys available
    for (int ii = 0; ii < items; ii++) {
        for (int ff = 0; ff < 3; ff++) {
            std::string key =
                    "anykey_" + std::to_string(ff) + "_" + std::to_string(ii);
            auto result = store->get(
                    {key, DocKeyEncodesCollectionId::No}, vbid, nullptr, {});
            EXPECT_EQ(ENGINE_SUCCESS, result.getStatus()) << "Expected to find "
                                                          << key;
        }
    }

    // Final 1/4 were discarded by the rollback
    for (int ii = 0; ii < items; ii++) {
        std::string key = "anykey_3_" + std::to_string(ii);
        auto result = store->get(
                {key, DocKeyEncodesCollectionId::No}, vbid, nullptr, {});
        EXPECT_EQ(ENGINE_KEY_ENOENT, result.getStatus()) << "Problem with "
                                                         << key;
    }

    // Rollback complete and will have posted a new StreamRequest
    stepForStreamRequest(rollbackPoint,
                         vb->failovers->getLatestEntry().vb_uuid);
    EXPECT_EQ(rollbackPoint, vb->getHighSeqno()) << "VB hasn't rolled back to "
                                                 << rollbackPoint;
}

class ReplicaRollbackDcpTest : public SingleThreadedEPBucketTest {
public:
    ReplicaRollbackDcpTest() {
    }

    void SetUp() override {
        SingleThreadedEPBucketTest::SetUp();
        store->setVBucketState(vbid, vbucket_state_active);
        vb = store->getVBucket(vbid);
        producers = std::make_unique<MockDcpMessageProducers>(engine.get());
        engine->setDcpConnMap(std::make_unique<MockDcpConnMap>(*engine));
    }

    void TearDown() override {
        vb.reset();
        SingleThreadedEPBucketTest::TearDown();
    }

    std::unique_ptr<MockDcpMessageProducers> producers;
    VBucketPtr vb;
};

TEST_F(ReplicaRollbackDcpTest, ReplicaRollbackClosesStreams) {
    /* MB-21682: Confirm that producer DCP streams from a replica VB are closed
     * if the VB does rollback to be consistent with the active. If we didn't
     * do this, streams from replica VBs could see the seqno go backwards
     * as we would continue to stream from the rollback point
     * */
    store_item(vbid, makeStoredDocKey("key"), "value");

    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));

    auto& ckpt_mgr =
            *(static_cast<MockCheckpointManager*>(vb->checkpointManager.get()));
    ckpt_mgr.createNewCheckpoint();
    EXPECT_EQ(2, ckpt_mgr.getNumCheckpoints());
    EXPECT_EQ(1, ckpt_mgr.getNumOfCursors());

    // Now remove the earlier checkpoint
    bool new_ckpt_created;
    EXPECT_EQ(0, ckpt_mgr.removeClosedUnrefCheckpoints(*vb, new_ckpt_created));

    store->setVBucketState(vbid, vbucket_state_replica);

    get_mock_server_api()->cookie->reserve(cookie);

    // Create a Mock Dcp producer
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      /*cookie*/ cookie,
                                                      "MB-21682",
                                                      0);

    MockDcpConnMap& mockConnMap =
            static_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    mockConnMap.addConn(cookie, producer);

    uint64_t rollbackSeqno;
    ASSERT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(
                      /*flags*/ 0,
                      /*opaque*/ 0,
                      /*vbucket*/ vbid,
                      /*start_seqno*/ 0,
                      /*end_seqno*/ ~0,
                      /*vb_uuid*/ 0,
                      /*snap_start*/ 0,
                      /*snap_end*/ ~0,
                      &rollbackSeqno,
                      [](vbucket_failover_t* entry,
                         size_t nentries,
                         gsl::not_null<const void*> cookie) {
                          return ENGINE_SUCCESS;
                      },
                      {}));

    auto stream = producer->findStream(vbid);
    ASSERT_TRUE(stream->isActive());

    producer->notifySeqnoAvailable(vb->getId(), vb->getHighSeqno());

    // Step which will notify the snapshot task
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(producers.get()));
    EXPECT_EQ(1, producer->getCheckpointSnapshotTask().queueSize());

    // Now call run on the snapshot task to move checkpoint into DCP stream
    producer->getCheckpointSnapshotTask().run();

    // snapshot marker
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers->last_op);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);

    auto kvb = engine->getKVBucket();

    // Perform the rollback
    EXPECT_EQ(TaskStatus::Complete, kvb->rollback(vbid, 0));

    // The stream should now be dead
    EXPECT_FALSE(stream->isActive()) << "Stream should be dead";

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpStreamEnd, producers->last_op)
            << "stream should have received a STREAM_END";

    // Stop Producer checkpoint processor task
    producer->cancelCheckpointCreatorTask();
}

auto allConfigValues =
        ::testing::Combine(::testing::Values("value_only", "full_eviction"),
                           ::testing::Values("replica", "pending"));

// Test cases which run in both Full and Value eviction on replica and pending
// vbucket states
INSTANTIATE_TEST_CASE_P(FullAndValueEvictionOnReplicaAndPending,
                        RollbackTest,
                        allConfigValues, );

// Test cases which run in both Full and Value eviction on replica and pending
// vbucket states
INSTANTIATE_TEST_CASE_P(FullAndValueEvictionOnReplicaAndPending,
                        RollbackDcpTest,
                        allConfigValues, );
