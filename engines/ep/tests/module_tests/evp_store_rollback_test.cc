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

#include "checkpoint_utils.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/active_stream_checkpoint_processor_task.h"
#include "dcp/producer.h"
#include "dcp/response.h"
#include "dcp_utils.h"
#include "durability/passive_durability_monitor.h"
#include "ep_bucket.h"
#include "ep_time.h"
#include "evp_store_single_threaded_test.h"
#include "failover-table.h"
#include "programs/engine_testapp/mock_server.h"
#include "tests/mock/mock_checkpoint_manager.h"
#include "tests/mock/mock_dcp.h"
#include "tests/mock/mock_dcp_consumer.h"
#include "tests/mock/mock_dcp_producer.h"
#include "tests/mock/mock_stream.h"
#include "tests/module_tests/collections/collections_test_helpers.h"
#include "tests/module_tests/test_helpers.h"
#include <engines/ep/tests/mock/mock_dcp_conn_map.h>
#include <utilities/test_manifest.h>

#include <mcbp/protocol/framebuilder.h>
#include <memcached/server_cookie_iface.h>

using FlushResult = EPBucket::FlushResult;
using MoreAvailable = EPBucket::MoreAvailable;
using WakeCkptRemover = EPBucket::WakeCkptRemover;

/**
 * Test fixture for rollback tests. Parameterized on:
 * 1. storage backend (couchstore / magma)
 * 2. eviction policy (full / value)
 * 3. vBucket state at rollback (pending or replica).
 */
class RollbackTest
    : public SingleThreadedEPBucketTest,
      public ::testing::WithParamInterface<
              std::tuple<std::string, std::string, std::string>> {
    void SetUp() override {
        config_string += "backend=" + getBackend();
        config_string += ";item_eviction_policy=" + getEvictionMode();
#ifdef EP_USE_MAGMA
        config_string += ";" + magmaRollbackConfig;
#endif
        SingleThreadedEPBucketTest::SetUp();
        if (getVBStateAtRollback() == "pending") {
            vbStateAtRollback = vbucket_state_pending;
        } else {
            vbStateAtRollback = vbucket_state_replica;
        }

        // Start vbucket as active to allow us to store items directly to it.
        store->setVBucketState(vbid, vbucket_state_active);

        // Write a single item so that we have a valid seqno/key/value to
        // rollback to. Rollback to 0 is a special case and results in
        // recreation of a vBucket so.
        const auto dummy_elements = size_t{1};
        auto res = store_item(vbid, makeStoredDocKey("dummy1"), "dummy");
        ASSERT_EQ(1, res.getBySeqno());
        ASSERT_EQ(
                FlushResult(
                        MoreAvailable::No, dummy_elements, WakeCkptRemover::No),
                getEPBucket().flushVBucket(vbid));
        initial_seqno = dummy_elements;
    }

protected:
    /*
     * Fake callback emulating dcp_add_failover_log
     */
    static ENGINE_ERROR_CODE fakeDcpAddFailoverLog(
            const std::vector<vbucket_failover_t>&) {
        return ENGINE_SUCCESS;
    }

    /**
     * Returns the json returned from HashTable::dumpStoredValuesAsJson without
     * the dirty field as this may not be the same post-rollback. Also removes
     * the exp time field as it will be different post rollback for any tests
     * that test deleted items.
     */
    nlohmann::json getHtState() {
        auto json = store->getVBucket(vbid)->ht.dumpStoredValuesAsJson();
        for (auto& sv : json) {
            sv.erase("dirty");
            sv.erase("exp time");
        }
        return json;
    }

public:
    std::string getBackend() const {
        return std::get<0>(GetParam());
    }

    std::string getEvictionMode() const {
        return std::get<1>(GetParam());
    }

    std::string getVBStateAtRollback() const {
        return std::get<2>(GetParam());
    }

    static std::string PrintToStringParamName(
            const ::testing::TestParamInfo<ParamType>& info) {
        return std::get<0>(info.param) + "_" + std::get<1>(info.param) + "_" +
               std::get<2>(info.param);
    }

    /**
     * Test rollback after deleting an item.
     * @param flush_before_rollback: Should the vbucket be flushed to disk just
     *        before the rollback (i.e. guaranteeing the in-memory state is in
     *        sync with disk).
     */
    void rollback_after_deletion_test(bool flush_before_rollback) {
        // Setup: Store an item then flush the vBucket (creating a checkpoint);
        // then delete the item and create a second checkpoint.
        StoredDocKey a = makeStoredDocKey("key");
        auto item_v1 = store_item(vbid, a, "1");
        ASSERT_EQ(initial_seqno + 1, item_v1.getBySeqno());
        ASSERT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::No),
                  getEPBucket().flushVBucket(vbid));
        auto expectedItems = store->getVBucket(vbid)->getNumTotalItems();
        // Save the pre-rollback HashTable state for later comparison
        auto htState = getHtState();
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
        if (flush_before_rollback) {
            const auto& vb = *store->getVBucket(vbid);
            const auto& ckptList =
                    CheckpointManagerTestIntrospector::public_getCheckpointList(
                            *vb.checkpointManager);
            const auto expectedWakeCktpRem =
                    (ckptList.size() > 1 ? WakeCkptRemover::Yes
                                         : WakeCkptRemover::No);
            ASSERT_EQ(FlushResult(MoreAvailable::No, 1, expectedWakeCktpRem),
                      getEPBucket().flushVBucket(vbid));
        }

        // Sanity-check - item should no longer exist.
        // If we're in value_only or have not flushed in full_eviction then
        // we don't need to do a bg fetch
        if (getEvictionMode() == "value_only" || !flush_before_rollback) {
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
        ForGetReplicaOp getReplicaItem =
                vbStateAtRollback == vbucket_state_replica
                        ? ForGetReplicaOp::Yes
                        : ForGetReplicaOp::No;
        auto result =
                getInternal(a, vbid, /*cookie*/ nullptr, getReplicaItem, {});
        ASSERT_EQ(ENGINE_SUCCESS, result.getStatus());

        EXPECT_EQ(htState.dump(0), getHtState().dump(0));
        EXPECT_EQ(item_v1, *result.item)
                << "Fetched item after rollback should match item_v1";

        if (!flush_before_rollback) {
            EXPECT_EQ(FlushResult(MoreAvailable::No, 0, WakeCkptRemover::No),
                      getEPBucket().flushVBucket(vbid));
        }

        EXPECT_EQ(expectedItems, store->getVBucket(vbid)->getNumItems());
        EXPECT_EQ(expectedItems, store->getVBucket(vbid)->getNumTotalItems());
    }

    // Test rollback after modifying an item.
    void rollback_after_mutation_test(bool flush_before_rollback) {
        // Setup: Store an item then flush the vBucket (creating a checkpoint);
        // then update the item with a new value and create a second checkpoint.
        StoredDocKey a = makeStoredDocKey("a");
        auto item_v1 = store_item(vbid, a, "old");
        ASSERT_EQ(initial_seqno + 1, item_v1.getBySeqno());
        ASSERT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::No),
                  getEPBucket().flushVBucket(vbid));

        // Save the pre-rollback HashTable state for later comparison
        auto htState = getHtState();
        auto expectedItems = store->getVBucket(vbid)->getNumTotalItems();

        auto item2 = store_item(vbid, a, "new");
        ASSERT_EQ(initial_seqno + 2, item2.getBySeqno());

        StoredDocKey key = makeStoredDocKey("key");
        store_item(vbid, key, "meh");

        if (flush_before_rollback) {
            EXPECT_EQ(FlushResult(MoreAvailable::No, 2, WakeCkptRemover::No),
                      getEPBucket().flushVBucket(vbid));
        }

        // Test - rollback to seqno of item_v1 and verify that the previous
        // value of the item has been restored.
        store->setVBucketState(vbid, vbStateAtRollback);
        ASSERT_EQ(TaskStatus::Complete,
                  store->rollback(vbid, item_v1.getBySeqno()));
        ASSERT_EQ(item_v1.getBySeqno(),
                  store->getVBucket(vbid)->getHighSeqno());

        EXPECT_EQ(htState.dump(0), getHtState().dump(0));

        // a should have the value of 'old'
        {
            auto result = store->get(a, vbid, nullptr, {});
            ASSERT_EQ(ENGINE_SUCCESS, result.getStatus());
            EXPECT_EQ(item_v1, *result.item)
                    << "Fetched item after rollback should match item_v1";
        }

        // key should be gone
        {
            auto result = store->get(key, vbid, nullptr, needsBGFetchQueued());
            if (needsBGFetch(result.getStatus())) {
                runBGFetcherTask();
                result = store->get(key, vbid, nullptr, {});
            }
            EXPECT_EQ(ENGINE_KEY_ENOENT, result.getStatus())
                    << "A key set after the rollback point was found";
        }

        if (!flush_before_rollback) {
            // The rollback should of wiped out any keys waiting for persistence
            EXPECT_EQ(FlushResult(MoreAvailable::No, 0, WakeCkptRemover::No),
                      getEPBucket().flushVBucket(vbid));
        }
        EXPECT_EQ(expectedItems, store->getVBucket(vbid)->getNumItems());
        EXPECT_EQ(expectedItems, store->getVBucket(vbid)->getNumTotalItems());
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

        // Save the pre-rollback HashTable state for later comparison
        auto htState = getHtState();

        // Setup
        StoredDocKey a = makeStoredDocKey("a");
        EPBucket::FlushResult res =
                FlushResult(MoreAvailable::No, 1, WakeCkptRemover::No);
        if (deleteLast) {
            store_item(vbid, a, "new");
            if (!flushOnce) {
                ASSERT_EQ(res, getEPBucket().flushVBucket(vbid));
            }
            delete_item(vbid, a);
            if (!flushOnce) {
                ASSERT_EQ(res, getEPBucket().flushVBucket(vbid));
            }
        } else {
            // Make sure we have something to delete
            store_item(vbid, a, "new");
            if (!flushOnce) {
                ASSERT_EQ(res, getEPBucket().flushVBucket(vbid));
            }
            delete_item(vbid, a);
            if (!flushOnce) {
                ASSERT_EQ(res, getEPBucket().flushVBucket(vbid));
            }
            store_item(vbid, a, "new");
            if (!flushOnce) {
                ASSERT_EQ(res, getEPBucket().flushVBucket(vbid));
            }
        }

        if (flushOnce) {
            ASSERT_EQ(res, getEPBucket().flushVBucket(vbid));
        }

        // Test - rollback to seqno before this test
        store->setVBucketState(vbid, vbStateAtRollback);
        ASSERT_EQ(TaskStatus::Complete, store->rollback(vbid, rbSeqno));
        ASSERT_EQ(rbSeqno, store->getVBucket(vbid)->getHighSeqno());

        EXPECT_EQ(htState.dump(0), getHtState().dump(0));

        if (getEvictionMode() == "value_only") {
            EXPECT_EQ(ENGINE_KEY_ENOENT,
                      store->get(a, vbid, nullptr, {}).getStatus());
        } else {
            auto gcb = store->get(a, vbid, nullptr, QUEUE_BG_FETCH);

            if (flushOnce && !deleteLast) {
                if (needsBGFetch(gcb.getStatus())) {
                    runBGFetcherTask();
                    gcb = store->get(a, vbid, nullptr, {});
                }
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
        EXPECT_EQ(1, store->getVBucket(vbid)->getNumItems());
        EXPECT_EQ(1, store->getVBucket(vbid)->getNumTotalItems());
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
        EXPECT_EQ(startVBCount + expectedDifference, vb->getNumTotalItems());

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
        EXPECT_EQ(startVBCount + expectedDifference, vb->getNumTotalItems());

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

        // rollbackCollectionCreate==true and the roll back function will
        // rewind disk to here, with dairy collection unknown
        auto seqno1 = store->getVBucket(vbid)->getHighSeqno();
        auto itemCount = store->getVBucket(vbid)->getNumItems();

        auto vb = store->getVBucket(vbid);
        CollectionsManifest cm;
        // the roll back function will rewind disk to this collection state.
        // note: we add 'meat' and keep it empty, this reproduces MB-37940
        vb->updateFromManifest(makeManifest(
                cm.add(CollectionEntry::dairy).add(CollectionEntry::meat)));

        nlohmann::json htState;
        if (rollbackCollectionCreate) {
            // Save the pre-rollback HashTable state for later comparison
            htState = getHtState();
        }

        // rollbackCollectionCreate==false and the roll back function will
        // rewind disk to key7, with dairy collection open
        auto rollback_item =
                store_item(vbid,
                           makeStoredDocKey("key7", CollectionEntry::dairy),
                           "key7 in the dairy collection");

        if (htState.empty()) {
            // Save the pre-rollback HashTable state for later comparison
            htState = getHtState();
        }

        ASSERT_EQ(FlushResult(MoreAvailable::No, 3, WakeCkptRemover::Yes),
                  getEPBucket().flushVBucket(vbid));

        // every key past this point will be lost from disk in a mid-point.
        auto item_v1 = store_item(vbid, makeStoredDocKey("rollback-cp-1"), "keep-me");
        auto item_v2 = store_item(vbid, makeStoredDocKey("rollback-cp-2"), "rollback to me");
        store_item(vbid, makeStoredDocKey("rollback-cp-3"), "i'm gone");

        // Select the rollback seqno
        auto rollback = rollbackCollectionCreate
                                ? rollback_item.getBySeqno() - 1
                                : item_v2.getBySeqno();
        ASSERT_EQ(FlushResult(MoreAvailable::No, 3, WakeCkptRemover::Yes),
                  getEPBucket().flushVBucket(vbid));

        cm.remove(CollectionEntry::dairy);
        vb->updateFromManifest(makeManifest(cm));

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
            ASSERT_EQ(FlushResult(MoreAvailable::No, 3, WakeCkptRemover::Yes),
                      getEPBucket().flushVBucket(vbid));
        }

        // Rollback should succeed, but rollback to 0
        store->setVBucketState(vbid, vbStateAtRollback);
        EXPECT_EQ(TaskStatus::Complete, store->rollback(vbid, rollback));

        EXPECT_EQ(htState.dump(0), getHtState().dump(0));

        // These keys should be gone after the rollback
        for (int i = 0; i < 3; i++) {
            auto key = makeStoredDocKey("rollback-cp-" + std::to_string(i));
            auto result = store->get(key, vbid, cookie, needsBGFetchQueued());
            if (needsBGFetch(result.getStatus())) {
                runBGFetcherTask();
                result = store->get(key, vbid, cookie, {});
            }

            EXPECT_EQ(ENGINE_KEY_ENOENT, result.getStatus())
                    << "A key set after the rollback point was found";
        }

        // These keys should be gone after the rollback
        for (int i = 0; i < 2; i++) {
            auto key = makeStoredDocKey("anotherkey_" + std::to_string(i));
            auto result = store->get(key, vbid, cookie, needsBGFetchQueued());
            if (needsBGFetch(result.getStatus())) {
                runBGFetcherTask();
                result = store->get(key, vbid, cookie, {});
            }

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
            EXPECT_EQ(itemCount, store->getVBucket(vbid)->getNumItems());
            EXPECT_EQ(itemCount, store->getVBucket(vbid)->getNumTotalItems());
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
            EXPECT_EQ(itemCount + 1, store->getVBucket(vbid)->getNumItems());
            EXPECT_EQ(itemCount + 1,
                      store->getVBucket(vbid)->getNumTotalItems());
        }
        EXPECT_EQ(0, store->getVBucket(vbid)->ht.getNumSystemItems());
        EXPECT_EQ(store->getVBucket(vbid)->getHighSeqno(),
                  store->getVBucket(vbid)->getPersistenceSeqno());
    }

    void rollback_to_zero(int64_t rollbackSeqno) {
        const auto minItemsToRollbackTo = 11;
        auto vbHighSeqno = store->getVBucket(vbid)->getHighSeqno();

        // Write some more dummy items so that we have a midpoint to rollback to
        for (size_t ii = vbHighSeqno + 1; ii <= minItemsToRollbackTo; ii++) {
            auto res =
                    store_item(vbid,
                               makeStoredDocKey("dummy" + std::to_string(ii)),
                               "dummy");
            ASSERT_EQ(ii, res.getBySeqno());
        }
        ASSERT_EQ(FlushResult(MoreAvailable::No,
                              minItemsToRollbackTo - vbHighSeqno,
                              WakeCkptRemover::No),
                  getEPBucket().flushVBucket(vbid));

        // Trigger a rollback to zero by rolling back to a seqno just before the
        // halfway point between start and end (2 in this case)
        store->setVBucketState(vbid, vbStateAtRollback);
        ASSERT_EQ(TaskStatus::Complete, store->rollback(vbid, rollbackSeqno));

        // No items at all
        auto vbptr = store->getVBucket(vbid);
        ASSERT_TRUE(vbptr);
        EXPECT_EQ(0, store->getVBucket(vbid)->getHighSeqno());
        EXPECT_EQ(0,
                  store->getVBucket(vbid)->getManifest().lock().getItemCount(
                          CollectionID::Default));
        EXPECT_EQ(0,
                  store->getVBucket(vbid)
                          ->getManifest()
                          .lock()
                          .getPersistedHighSeqno(CollectionID::Default));
        EXPECT_EQ(0, store->getVBucket(vbid)->getNumItems());
        EXPECT_EQ(0, store->getVBucket(vbid)->getNumTotalItems());
        EXPECT_EQ(0, store->getVBucket(vbid)->getPersistenceSeqno());
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
    rollback_after_deletion_test(/*flush_before_rollback*/ true);
}

TEST_P(RollbackTest, RollbackAfterDeletionNoFlush) {
    rollback_after_deletion_test(/*flush_before_rollback*/ false);
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
    rollback_stat_test(1, [this] { rollback_after_mutation_test(true); });
}

// Test what happens when we rollback the creation and the mutation of
// different documents that are not persisted
TEST_P(RollbackTest, RollbackMutationDocCountsNoFlush) {
    rollback_stat_test(1, [this] { rollback_after_mutation_test(false); });
}

// Test what happens when we rollback a deletion of a document that existed
// before rollback that has been persisted
TEST_P(RollbackTest, RollbackDeletionDocCounts) {
    rollback_stat_test(1, [this] { rollback_after_deletion_test(true); });
}

// Test what happens when we rollback a deletion of a document that existed
// before rollback that has not been persisted
TEST_P(RollbackTest, RollbackDeletionDocCountsNoFlush) {
    rollback_stat_test(1, [this] { rollback_after_deletion_test(false); });
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

TEST_P(RollbackTest, RollbackToZeroExplicitlyDocCounts) {
    rollback_to_zero(0);
}

TEST_P(RollbackTest, RollbackToZeroLTMidpointDocCounts) {
    // Here we are testing that we rollback to 0 if we have a rollback point
    // that would require us to discard more than half of our seqnos. Skipped
    // for magma as we don't make this "optimization".
    // @TODO magma: investigate if we should make the above "optimization".
    if (getBackend() == "magma") {
        return;
    }
    rollback_to_zero(4);
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

    // Save the pre-rollback HashTable state for later comparison
    auto htState = getHtState();

    ASSERT_EQ(FlushResult(MoreAvailable::No, numItems + 1, WakeCkptRemover::No),
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

    EXPECT_EQ(htState.dump(0), getHtState().dump(0));

    /* confirm that we have rolled back to the disk snapshot */
    EXPECT_EQ(rollback_item.getBySeqno(),
              store->getVBucket(vbid)->getHighSeqno());

    /* since we rely only on disk snapshots currently, we must lose the items in
       the checkpoints */
    for (int i = 0; i < 2; i++) {
        auto key = makeStoredDocKey("rollback-cp-" + std::to_string(i));
        auto result = store->get(key, vbid, nullptr, needsBGFetchQueued());
        if (needsBGFetch(result.getStatus())) {
            runBGFetcherTask();
            result = store->get(key, vbid, nullptr, {});
        }
        EXPECT_EQ(ENGINE_KEY_ENOENT, result.getStatus())
                << "A key set after the rollback point was found";
    }
    // numItems + dummy + key11
    EXPECT_EQ(numItems + 2, store->getVBucket(vbid)->getNumItems());
    EXPECT_EQ(numItems + 2, store->getVBucket(vbid)->getNumTotalItems());
}

/*
 * The opencheckpointid of a bucket is one after a rollback and
 * the receivingInitialDiskSnapshot flag is false.
 */
TEST_P(RollbackTest, MB21784) {
    // Make the vbucket a replica
    store->setVBucketState(vbid, vbucket_state_replica);

    // Save the pre-rollback HashTable state for later comparison
    auto htState = getHtState();

    // Perform a rollback
    EXPECT_EQ(TaskStatus::Complete, store->rollback(vbid, initial_seqno))
            << "rollback did not return success";

    EXPECT_EQ(htState.dump(0), getHtState().dump(0));

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

    // Save the pre-rollback HashTable state for later comparison
    auto htState = getHtState();

    /* Try to rollback on active (default state) vbucket */
    EXPECT_EQ(TaskStatus::Abort,
              store->rollback(vbid, numItems - 1 /*rollbackReqSeqno*/));
    EXPECT_EQ(htState.dump(0), getHtState().dump(0));

    EXPECT_EQ(TaskStatus::Abort, store->rollback(vbid, 0 /*rollbackReqSeqno*/));
    EXPECT_EQ(htState.dump(0), getHtState().dump(0));
}

TEST_P(RollbackTest, RollbackUnpersistedItemsFromCheckpointsOfDifferentType) {
    // Stay as active until the rollback as it's easier to store things
    // Store 1 item as our base (and post-rollback) state
    const int numItems = 1;
    for (int i = 0; i < numItems; i++) {
        store_item(vbid,
                   makeStoredDocKey("key_" + std::to_string(i)),
                   "not rolled back");
    }

    flushVBucketToDiskIfPersistent(vbid, 1);
    auto htState = getHtState();
    auto vb = store->getVBucket(vbid);
    auto rollbackSeqno = vb->getHighSeqno();

    // Put two unpersisted items into the checkpoint manager in two different
    // checkpoints of different type
    ASSERT_FALSE(vb->checkpointManager->isOpenCheckpointDisk());
    ASSERT_EQ(2, vb->checkpointManager->getOpenCheckpointId());
    store_item(vbid, makeStoredDocKey("key_memory"), "not rolled back");

    vb->checkpointManager->createSnapshot(vb->getHighSeqno() + 1,
                                          vb->getHighSeqno() + 2,
                                          0 /*highCompletedSeqno*/,
                                          CheckpointType::Disk,
                                          vb->getHighSeqno() + 1);
    ASSERT_TRUE(vb->checkpointManager->isOpenCheckpointDisk());
    ASSERT_EQ(3, vb->checkpointManager->getOpenCheckpointId());
    store_item(vbid, makeStoredDocKey("key_disk"), "not rolled back");

    // Flip to replica and rollback
    store->setVBucketState(vbid, vbucket_state_replica);
    EXPECT_EQ(TaskStatus::Complete, store->rollback(vbid, rollbackSeqno));
    EXPECT_EQ(rollbackSeqno, vb->getHighSeqno());

    EXPECT_EQ(htState.dump(0), getHtState().dump(0));
    EXPECT_EQ(numItems + 1, store->getVBucket(vbid)->getNumItems());
    EXPECT_EQ(numItems + 1, store->getVBucket(vbid)->getNumTotalItems());
}

TEST_P(RollbackTest, RollbackBeforeFirstFailoverTableEntry) {
    auto maxFailoverEntries =
            engine->getConfiguration().getMaxFailoverEntries();

    // Stay as active until the rollback as it's easier to store things.
    for (size_t i = 0; i < maxFailoverEntries + 1; i++) {
        store_item(vbid,
                   makeStoredDocKey("key_" + std::to_string(i)),
                   "not rolled back");
    }

    flushVBucketToDiskIfPersistent(vbid, maxFailoverEntries + 1);
    auto htState = getHtState();
    auto vb = store->getVBucket(vbid);
    auto rollbackSeqno = vb->getHighSeqno();

    for (size_t i = 0; i < maxFailoverEntries; i++) {
        store_item(vbid,
                   makeStoredDocKey("rollback_" + std::to_string(i)),
                   "rolled back");
        vb->failovers->createEntry(vb->getHighSeqno());
    }

    EXPECT_EQ(maxFailoverEntries, vb->failovers->getNumEntries());

    // Flip to replica and rollback
    store->setVBucketState(vbid, vbucket_state_replica);
    EXPECT_EQ(TaskStatus::Complete, store->rollback(vbid, rollbackSeqno));
    EXPECT_EQ(rollbackSeqno, vb->getHighSeqno());

    EXPECT_EQ(htState.dump(0), getHtState().dump(0));

    EXPECT_EQ(1, vb->failovers->getNumEntries());
    auto entry = vb->failovers->getLatestEntry();
    EXPECT_EQ(rollbackSeqno, entry.by_seqno);
    EXPECT_NE(0, entry.vb_uuid);
    // +2 includes the dummy key stored by SetUp
    EXPECT_EQ(maxFailoverEntries + 2, vb->getNumItems());
    EXPECT_EQ(maxFailoverEntries + 2, vb->getNumTotalItems());
}

class RollbackDcpTest : public RollbackTest {
public:
    // Mock implementation of DcpMessageProducersIface which ... TODO
    class DcpProducers;

    RollbackDcpTest() = default;

    void SetUp() override {
        config_string += "item_eviction_policy=" + getEvictionMode();
        SingleThreadedEPBucketTest::SetUp();
        if (getVBStateAtRollback() == "pending") {
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
    std::vector<char> getRollbackResponse(uint32_t opaque,
                                          uint64_t rollbackSeq) const {
        std::vector<char> msg(sizeof(protocol_binary_response_header) +
                              sizeof(uint64_t));
        cb::mcbp::ResponseBuilder builder(
                {reinterpret_cast<uint8_t*>(msg.data()),
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
        while (consumer->step(producers) == ENGINE_SUCCESS) {
            handleProducerResponseIfStepBlocked(*consumer, producers);
        }
        EXPECT_TRUE(streamRequestData.called);
        EXPECT_EQ(startSeqno, streamRequestData.start_seqno);
        EXPECT_EQ(startSeqno ? vbUUID : 0, streamRequestData.vbucket_uuid);
        streamRequestData = {};
    }

    void createItems(int items, int flushes, int start = 0) {
        // Flush multiple checkpoints of unique keys
        for (int ii = start; ii < flushes; ii++) {
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
                *reinterpret_cast<cb::mcbp::Response*>(msg.data())));

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
                *reinterpret_cast<cb::mcbp::Response*>(msg.data())));

        // consumer must of scheduled a RollbackTask (writer task)
        auto& lpWriteQ = *task_executor->getLpTaskQ()[WRITER_TASK_IDX];
        ASSERT_EQ(1, lpWriteQ.getFutureQueueSize());
        runNextTask(lpWriteQ);
    }

    /**
     * Writes items (default of 1). At least 1 item needed to set up stream
     * and for rollback to occur.
     */
    void writeBaseItems(int items = 1);

    /**
     * Receive and flush a DCP prepare
     *
     * @param key Key to use for the Prepare.
     * @param value Value to use for the Prepare.
     * @param syncDelete is this a SyncWrite or SyncDelete
     * @param flush flush the prepare to disk
     */
    void doPrepare(StoredDocKey key,
                   std::string value,
                   bool syncDelete = false,
                   bool flush = true);

    /**
     * Receive and fulsh a DCP commit
     *
     * @param key Key to use for the Commit.
     */
    void doCommit(StoredDocKey key);

    /**
     * Receive and flush a DCP prepare followed by a DCP commit
     *
     * @param key Key to use for the Prepare and Commit.
     * @param value Value to use for the Prepare.
     * @param syncDelete is this a SyncWrite or SyncDelete
     */
    void doPrepareAndCommit(StoredDocKey key,
                            std::string value,
                            bool syncDelete = false);
    /**
     * Receive and flush a DCP abort
     *
     * @param key Key to use for the Abort.
     * @param flush whether to flush the abort to disk
     */
    void doAbort(StoredDocKey key, bool flush = true);

    /**
     * Consume a delete
     * @param key Key to delete
     * @param flush run the flusher (or not)
     */
    void doDelete(StoredDocKey key, bool flush = true);

    /**
     * Receive and flush a DCP prepare followed by a DCP abort
     *
     * @param key Key to use for the Prepare and Abort.
     * @param value Value to use for the Prepare.
     * @param syncDelete is this a SyncWrite or SyncDelete
     * @param flush whether to flush after each op
     */
    void doPrepareAndAbort(StoredDocKey key,
                           std::string value,
                           bool syncDeleted = false,
                           bool flush = true);

    void rollbackPrepare(bool deleted);
    void rollbackPrepareOnTopOfSyncWrite(bool syncDelete, bool deletedPrepare);
    void rollbackSyncWrite(bool deleted);
    void rollbackAbortedSyncWrite(bool deleted);
    void rollbackSyncWriteOnTopOfSyncWrite(bool syncDeleteFirst,
                                           bool syncDeleteSecond);
    void rollbackSyncWriteOnTopOfAbortedSyncWrite(bool syncDeleteFirst,
                                                  bool syncDeleteSecond);
    void rollbackToZeroWithSyncWrite(bool deleted);
    void rollbackCommit(bool deleted);
    void rollbackCommitOnTopOfSyncWrite(bool syncDeleteFirst,
                                        bool syncDeleteSecond,
                                        bool lowMemoryRollback = false);
    void rollbackCommitOnTopOfAbortedSyncWrite(bool syncDeleteFirst,
                                               bool syncDeleteSecond);
    void rollbackAbort(bool deleted);
    void rollbackAbortOnTopOfSyncWrite(bool syncDeleteFirst,
                                       bool syncDeleteSecond);
    void rollbackAbortOnTopOfAbortedSyncWrite(bool syncDeleteFirst,
                                              bool syncDeleteSecond);

    void rollbackUnpersistedPrepareOnTopOfSyncWrite(bool syncDelete,
                                                    bool deletedPrepare);

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

    // Save the pre-rollback HashTable state for later comparison
    auto htState = getHtState();

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

    EXPECT_EQ(htState.dump(0), getHtState().dump(0));

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

    // +2 includes the dummy key stored by SetUp
    EXPECT_EQ(0, vb->getNumItems());
    EXPECT_EQ(0, vb->getNumTotalItems());
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
    createItems(items, 3);

    // Save the pre-rollback HashTable state for later comparison after doing 3
    // flushes
    auto htState = getHtState();

    // Flush the final set of items (requires setting vBucket back to active)
    store->setVBucketState(vbid, vbucket_state_active);
    createItems(items, flushes, 3 /*startFlush*/);

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

    EXPECT_EQ(htState.dump(0), getHtState().dump(0));

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

    EXPECT_EQ(items * 3, vb->getNumItems());
    EXPECT_EQ(items * 3, vb->getNumTotalItems());
}

void RollbackDcpTest::writeBaseItems(int items) {
    const int flushes = 1;
    createItems(items, flushes);

    addStream(items);
    auto stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    ASSERT_TRUE(stream);
}

void RollbackDcpTest::doPrepare(StoredDocKey key,
                                std::string value,
                                bool syncDelete,
                                bool flush) {
    auto stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    auto startSeqno = vb->getHighSeqno();
    auto prepareSeqno = startSeqno + 1;
    SnapshotMarker marker(
            0 /*opaque*/,
            vbid,
            prepareSeqno /*snapStart*/,
            prepareSeqno /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    stream->processMarker(&marker);

    auto prepare = makePendingItem(key, value);

    // The producer would normally do this for us
    prepare->setCas(999);
    prepare->setBySeqno(prepareSeqno);
    using namespace cb::durability;
    prepare->setPendingSyncWrite(
            Requirements{Level::Majority, Timeout::Infinity()});

    // The consumer would normally do this for us
    prepare->setPreparedMaybeVisible();
    if (syncDelete) {
        prepare->setDeleted();
    }

    ASSERT_EQ(ENGINE_SUCCESS, store->prepare(*prepare, cookie));
    if (flush) {
        flush_vbucket_to_disk(vbid, 1);
    }
}

void RollbackDcpTest::doCommit(StoredDocKey key) {
    auto stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());

    auto prepareSeqno = vb->getHighSeqno();
    auto commitSeqno = prepareSeqno + 1;
    SnapshotMarker marker(
            0 /*opaque*/,
            vbid,
            commitSeqno /*snapStart*/,
            commitSeqno /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamId*/);
    stream->processMarker(&marker);

    ASSERT_EQ(ENGINE_SUCCESS,
              vb->commit(key,
                         prepareSeqno,
                         {commitSeqno},
                         vb->lockCollections(key)));

    flush_vbucket_to_disk(vbid, 1);

    auto& passiveDm = static_cast<const PassiveDurabilityMonitor&>(
            vb->getDurabilityMonitor());

    // To set the HPS we need to receive the snapshot end - just hit the PDM
    // function to simulate this
    const_cast<PassiveDurabilityMonitor&>(passiveDm).notifySnapshotEndReceived(
            commitSeqno);
    ASSERT_EQ(0, passiveDm.getNumTracked());
    ASSERT_EQ(prepareSeqno, passiveDm.getHighPreparedSeqno());
    ASSERT_EQ(prepareSeqno, passiveDm.getHighCompletedSeqno());
}

void RollbackDcpTest::doPrepareAndCommit(StoredDocKey key,
                                         std::string value,
                                         bool syncDelete) {
    doPrepare(key, value, syncDelete);
    doCommit(key);
}

void RollbackDcpTest::doAbort(StoredDocKey key, bool flush) {
    auto stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());

    auto prepareSeqno = vb->getHighSeqno();
    auto abortSeqno = prepareSeqno + 1;
    SnapshotMarker marker(
            0 /*opaque*/,
            vbid,
            abortSeqno /*snapStart*/,
            abortSeqno /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamID*/);
    stream->processMarker(&marker);

    ASSERT_EQ(
            ENGINE_SUCCESS,
            vb->abort(
                    key, prepareSeqno, {abortSeqno}, vb->lockCollections(key)));
    if (flush) {
        flush_vbucket_to_disk(vbid, 1);
    }

    auto& passiveDm = static_cast<const PassiveDurabilityMonitor&>(
            vb->getDurabilityMonitor());

    // To set teh HPS we need to receive the snapshot end - just hit the PDM
    // function to simulate this
    const_cast<PassiveDurabilityMonitor&>(passiveDm).notifySnapshotEndReceived(
            abortSeqno);
    ASSERT_EQ(0, passiveDm.getNumTracked());
    ASSERT_EQ(prepareSeqno, passiveDm.getHighPreparedSeqno());
    ASSERT_EQ(prepareSeqno, passiveDm.getHighCompletedSeqno());
}

void RollbackDcpTest::doDelete(StoredDocKey key, bool flush) {
    auto stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());

    auto delSeqno = vb->getHighSeqno() + 1;
    SnapshotMarker marker(
            0 /*opaque*/,
            vbid,
            delSeqno /*snapStart*/,
            delSeqno /*snapEnd*/,
            dcp_marker_flag_t::MARKER_FLAG_MEMORY | MARKER_FLAG_CHK,
            {} /*HCS*/,
            {} /*maxVisibleSeqno*/,
            {}, // timestamp
            {} /*streamID*/);
    stream->processMarker(&marker);
    ItemMetaData meta;
    uint64_t cas = 0;
    meta.cas = 99;
    ASSERT_EQ(ENGINE_SUCCESS,
              vb->deleteWithMeta(cas,
                                 nullptr,
                                 cookie,
                                 store->getEPEngine(),
                                 CheckConflicts::No,
                                 meta,
                                 GenerateBySeqno::No,
                                 GenerateCas::No,
                                 delSeqno,
                                 vb->lockCollections(key),
                                 DeleteSource::Explicit));

    if (flush) {
        flush_vbucket_to_disk(vbid, 1);
    }
}
void RollbackDcpTest::doPrepareAndAbort(StoredDocKey key,
                                        std::string value,
                                        bool syncDelete,
                                        bool flush) {
    doPrepare(key, value, syncDelete, flush);
    doAbort(key, flush);
}

/**
 * Checks the state of the HashTable and PassiveDM post-rollback
 *
 * Pre-rollback our sequence of events should be:
 *  - 1 base item
 *  - 1 prepare
 *
 * Post-rollback our sequence of events should be:
 *  - 1 base item
 */
void RollbackDcpTest::rollbackPrepare(bool deleted) {
    writeBaseItems();
    auto baseItems = vb->getNumTotalItems();
    auto rollbackSeqno = vb->getHighSeqno();
    auto key = makeStoredDocKey("anykey_0_0");

    // Save the pre-rollback HashTable state for later comparison
    auto htState = getHtState();

    doPrepare(key, "value2", deleted);

    store->setVBucketState(vbid, vbStateAtRollback);
    EXPECT_EQ(TaskStatus::Complete, store->rollback(vbid, rollbackSeqno));
    EXPECT_EQ(rollbackSeqno, store->getVBucket(vbid)->getHighSeqno());

    auto& passiveDm = static_cast<const PassiveDurabilityMonitor&>(
            vb->getDurabilityMonitor());
    EXPECT_EQ(0, passiveDm.getNumTracked());
    EXPECT_EQ(0, passiveDm.getHighPreparedSeqno());
    EXPECT_EQ(0, passiveDm.getHighCompletedSeqno());
    EXPECT_EQ(baseItems, vb->getNumItems());
    EXPECT_EQ(baseItems, vb->getNumTotalItems());
    EXPECT_EQ(htState.dump(0), getHtState().dump(0));
}

TEST_P(RollbackDcpTest, RollbackPrepare) {
    rollbackPrepare(false);
}

TEST_P(RollbackDcpTest, RollbackDeletedPrepare) {
    rollbackPrepare(true);
}

/**
 * Checks the state of the HashTable and PassiveDM post-rollback
 *
 * Pre-rollback our sequence of events should be:
 *  - 1 base item
 *  - 1 prepare("anykey_0_0")
 *  - 1 commit("anykey_0_0")
 *  - 1 prepare("anykey_0_0")
 *
 * Post-rollback our sequence of events should be:
 *  - 1 base item
 *  - 1 prepare("anykey_0_0")
 *  - 1 commit("anykey_0_0")
 */
void RollbackDcpTest::rollbackPrepareOnTopOfSyncWrite(bool syncDelete,
                                                      bool deletedPrepare) {
    writeBaseItems();
    auto highCompletedAndPreparedSeqno = vb->getHighSeqno() + 1;
    auto key = makeStoredDocKey("anykey_0_0");
    doPrepareAndCommit(key, "value2", syncDelete);
    auto baseItems = vb->getNumTotalItems();
    auto rollbackSeqno = vb->getHighSeqno();

    // Save the pre-rollback HashTable state for later comparison
    auto htState = getHtState();

    doPrepare(key, "value3", deletedPrepare);

    store->setVBucketState(vbid, vbStateAtRollback);
    EXPECT_EQ(TaskStatus::Complete, store->rollback(vbid, rollbackSeqno));
    EXPECT_EQ(rollbackSeqno, store->getVBucket(vbid)->getHighSeqno());

    auto& passiveDm = static_cast<const PassiveDurabilityMonitor&>(
            vb->getDurabilityMonitor());
    EXPECT_EQ(0, passiveDm.getNumTracked());
    EXPECT_EQ(highCompletedAndPreparedSeqno, passiveDm.getHighPreparedSeqno());
    EXPECT_EQ(highCompletedAndPreparedSeqno, passiveDm.getHighCompletedSeqno());
    EXPECT_EQ(baseItems, vb->getNumItems());
    EXPECT_EQ(baseItems, vb->getNumTotalItems());
    EXPECT_EQ(htState.dump(0), getHtState().dump(0));
}

TEST_P(RollbackDcpTest, RollbackPrepareOnTopOfSyncWrite) {
    rollbackPrepareOnTopOfSyncWrite(false, false);
}

TEST_P(RollbackDcpTest, RollbackDeletedPrepareOnTopOfSyncWrite) {
    rollbackPrepareOnTopOfSyncWrite(false, true);
}

TEST_P(RollbackDcpTest, RollbackPrepareOnTopOfSyncDelete) {
    rollbackPrepareOnTopOfSyncWrite(true, false);
}

TEST_P(RollbackDcpTest, RollbackDeletedPrepareOnTopOfSyncDelete) {
    rollbackPrepareOnTopOfSyncWrite(true, true);
}

void RollbackDcpTest::rollbackUnpersistedPrepareOnTopOfSyncWrite(
        bool syncDelete, bool deletedPrepare) {
    writeBaseItems();
    auto highCompletedAndPreparedSeqno = vb->getHighSeqno() + 1;
    auto key = makeStoredDocKey("anykey_0_0");
    doPrepareAndCommit(key, "value2", syncDelete);
    auto baseItems = vb->getNumTotalItems();
    auto rollbackSeqno = vb->getHighSeqno();

    // Save the pre-rollback HashTable state for later comparison
    auto htState = getHtState();

    doPrepare(key, "value3", deletedPrepare, false /*noflush*/);

    store->setVBucketState(vbid, vbStateAtRollback);
    EXPECT_EQ(TaskStatus::Complete, store->rollback(vbid, rollbackSeqno));
    EXPECT_EQ(rollbackSeqno, store->getVBucket(vbid)->getHighSeqno());

    auto& passiveDm = static_cast<const PassiveDurabilityMonitor&>(
            vb->getDurabilityMonitor());
    EXPECT_EQ(0, passiveDm.getNumTracked());
    EXPECT_EQ(highCompletedAndPreparedSeqno, passiveDm.getHighPreparedSeqno());
    EXPECT_EQ(highCompletedAndPreparedSeqno, passiveDm.getHighCompletedSeqno());
    EXPECT_EQ(baseItems, vb->getNumItems());
    EXPECT_EQ(baseItems, vb->getNumTotalItems());
    EXPECT_EQ(htState.dump(0), getHtState().dump(0));
    EXPECT_EQ(0, vb->ht.getNumPreparedSyncWrites());
}

TEST_P(RollbackDcpTest, RollbackUnpersistedPrepareOnTopOfSyncWrite) {
    rollbackUnpersistedPrepareOnTopOfSyncWrite(false, false);
}

TEST_P(RollbackDcpTest, RollbackUnpersistedDeletedPrepareOnTopOfSyncWrite) {
    rollbackUnpersistedPrepareOnTopOfSyncWrite(false, true);
}

TEST_P(RollbackDcpTest, RollbackUnpersistedPrepareOnTopOfSyncDelete) {
    rollbackUnpersistedPrepareOnTopOfSyncWrite(true, false);
}

TEST_P(RollbackDcpTest, RollbackUnpersistedDeletedPrepareOnTopOfSyncDelete) {
    rollbackUnpersistedPrepareOnTopOfSyncWrite(true, true);
}

/**
 * Checks the state of the HashTable and PassiveDM post-rollback
 *
 * Pre-rollback our sequence of events should be:
 *  - 1 base item
 *  - 1 prepare
 *  - 1 commit
 *
 * Post-rollback our sequence of events should be:
 *  - 1 base item
 */
void RollbackDcpTest::rollbackSyncWrite(bool deleted) {
    writeBaseItems();
    auto baseItems = vb->getNumTotalItems();
    auto rollbackSeqno = vb->getHighSeqno();

    // Save the pre-rollback HashTable state for later comparison
    auto htState = getHtState();

    doPrepareAndCommit(makeStoredDocKey("anykey_0_0"), "value2", deleted);

    store->setVBucketState(vbid, vbStateAtRollback);
    EXPECT_EQ(TaskStatus::Complete, store->rollback(vbid, rollbackSeqno));
    EXPECT_EQ(rollbackSeqno, store->getVBucket(vbid)->getHighSeqno());

    auto& passiveDm = static_cast<const PassiveDurabilityMonitor&>(
            vb->getDurabilityMonitor());
    EXPECT_EQ(0, passiveDm.getNumTracked());
    EXPECT_EQ(0, passiveDm.getHighPreparedSeqno());
    EXPECT_EQ(0, passiveDm.getHighCompletedSeqno());
    EXPECT_EQ(baseItems, vb->getNumItems());
    EXPECT_EQ(baseItems, vb->getNumTotalItems());
    EXPECT_EQ(htState.dump(0), getHtState().dump(0));
}

TEST_P(RollbackDcpTest, RollbackSyncWrite) {
    rollbackSyncWrite(false);
}

TEST_P(RollbackDcpTest, RollbackSyncDelete) {
    rollbackSyncWrite(true);
}

/**
 * Checks the state of the HashTable and PassiveDM post-rollback
 *
 * Pre-rollback our sequence of events should be:
 *  - 1 base item
 *  - 1 prepare("anykey_0_0")
 *  - 1 abort("anykey_0_0")
 *
 * Post-rollback our sequence of events should be:
 *  - 1 base item
 */
void RollbackDcpTest::rollbackAbortedSyncWrite(bool deleted) {
    writeBaseItems();
    auto baseItems = vb->getNumTotalItems();
    auto rollbackSeqno = vb->getHighSeqno();

    // Save the pre-rollback HashTable state for later comparison
    auto htState = getHtState();

    auto key = makeStoredDocKey("anykey_0_0");
    doPrepareAndAbort(key, "value2", deleted);

    store->setVBucketState(vbid, vbStateAtRollback);
    EXPECT_EQ(TaskStatus::Complete, store->rollback(vbid, rollbackSeqno));
    EXPECT_EQ(rollbackSeqno, store->getVBucket(vbid)->getHighSeqno());

    auto& passiveDm = static_cast<const PassiveDurabilityMonitor&>(
            vb->getDurabilityMonitor());
    EXPECT_EQ(0, passiveDm.getNumTracked());
    EXPECT_EQ(0, passiveDm.getHighPreparedSeqno());
    EXPECT_EQ(0, passiveDm.getHighCompletedSeqno());
    EXPECT_EQ(baseItems, vb->getNumItems());
    EXPECT_EQ(baseItems, vb->getNumTotalItems());
    EXPECT_EQ(htState.dump(0), getHtState().dump(0));
}

TEST_P(RollbackDcpTest, RollbackAbortedSyncWrite) {
    rollbackAbortedSyncWrite(false);
}

TEST_P(RollbackDcpTest, RollbackAbortedSyncDelete) {
    rollbackAbortedSyncWrite(true);
}

/**
 * Checks the state of the HashTable and PassiveDM post-rollback
 *
 * Pre-rollback our sequence of events should be:
 *  - 1 base item
 *  - 1 prepare
 *  - 1 commit
 *  - 1 prepare
 *  - 1 commit
 *
 * Post-rollback our sequence of events should be:
 *  - 1 base item
 *  - 1 prepare
 *  - 1 commit
 */
void RollbackDcpTest::rollbackSyncWriteOnTopOfSyncWrite(bool syncDeleteFirst,
                                                        bool syncDeleteSecond) {
    writeBaseItems();
    auto highCompletedAndPreparedSeqno = vb->getHighSeqno() + 1;
    auto key = makeStoredDocKey("anykey_0_0");
    doPrepareAndCommit(key, "value2", syncDeleteFirst);
    auto baseItems = vb->getNumTotalItems();
    auto rollbackSeqno = vb->getHighSeqno();

    // Save the pre-rollback HashTable state for later comparison
    auto htState = getHtState();

    doPrepareAndCommit(key, "value3", syncDeleteSecond);

    store->setVBucketState(vbid, vbStateAtRollback);
    EXPECT_EQ(TaskStatus::Complete, store->rollback(vbid, rollbackSeqno));
    EXPECT_EQ(rollbackSeqno, store->getVBucket(vbid)->getHighSeqno());

    auto& passiveDm = static_cast<const PassiveDurabilityMonitor&>(
            vb->getDurabilityMonitor());
    EXPECT_EQ(0, passiveDm.getNumTracked());
    EXPECT_EQ(highCompletedAndPreparedSeqno, passiveDm.getHighPreparedSeqno());
    EXPECT_EQ(highCompletedAndPreparedSeqno, passiveDm.getHighCompletedSeqno());
    EXPECT_EQ(baseItems, vb->getNumItems());
    EXPECT_EQ(baseItems, vb->getNumTotalItems());
    EXPECT_EQ(htState.dump(0), getHtState().dump(0));
}

TEST_P(RollbackDcpTest, RollbackSyncWriteOnTopOfSyncWrite) {
    rollbackSyncWriteOnTopOfSyncWrite(false, false);
}

TEST_P(RollbackDcpTest, RollbackSyncDeleteOnTopOfSyncWrite) {
    rollbackSyncWriteOnTopOfSyncWrite(false, true);
}

TEST_P(RollbackDcpTest, RollbackSyncWriteOnTopOfSyncDelete) {
    rollbackSyncWriteOnTopOfSyncWrite(true, false);
}

TEST_P(RollbackDcpTest, RollbackSyncDeleteOnTopOfSyncDelete) {
    rollbackSyncWriteOnTopOfSyncWrite(true, true);
}

/**
 * Checks the state of the HashTable and PassiveDM post-rollback
 *
 * Pre-rollback our sequence of events should be:
 *  - 1 base item
 *  - 1 prepare("anykey_0_0")
 *  - 1 abort("anykey_0_0")
 *  - 1 prepare("anykey_0_0")
 *  - 1 commit("anykey_0_0")
 *
 * Post-rollback our sequence of events should be:
 *  - 1 base item
 *  - 1 prepare("anykey_0_0")
 *  - 1 abort("anykey_0_0")
 */
void RollbackDcpTest::rollbackSyncWriteOnTopOfAbortedSyncWrite(
        bool syncDeleteFirst, bool syncDeleteSecond) {
    writeBaseItems();
    auto baseItems = vb->getNumTotalItems();
    auto highCompletedAndPreparedSeqno = vb->getHighSeqno() + 1;
    auto key = makeStoredDocKey("anykey_0_0");
    doPrepareAndAbort(key, "value2", syncDeleteFirst);
    auto rollbackSeqno = vb->getHighSeqno();

    // Save the pre-rollback HashTable state for later comparison
    auto htState = getHtState();

    doPrepareAndCommit(key, "value3", syncDeleteSecond);

    store->setVBucketState(vbid, vbStateAtRollback);
    EXPECT_EQ(TaskStatus::Complete, store->rollback(vbid, rollbackSeqno));
    EXPECT_EQ(rollbackSeqno, store->getVBucket(vbid)->getHighSeqno());

    auto& passiveDm = static_cast<const PassiveDurabilityMonitor&>(
            vb->getDurabilityMonitor());
    EXPECT_EQ(0, passiveDm.getNumTracked());
    EXPECT_EQ(highCompletedAndPreparedSeqno, passiveDm.getHighPreparedSeqno());
    EXPECT_EQ(highCompletedAndPreparedSeqno, passiveDm.getHighCompletedSeqno());
    EXPECT_EQ(baseItems, vb->getNumItems());
    EXPECT_EQ(baseItems, vb->getNumTotalItems());
    EXPECT_EQ(htState.dump(0), getHtState().dump(0));
}

TEST_P(RollbackDcpTest, RollbackSyncWriteOnTopOfAbortedSyncWrite) {
    rollbackSyncWriteOnTopOfAbortedSyncWrite(false, false);
}

TEST_P(RollbackDcpTest, RollbackSyncDeleteOnTopOfAbortedSyncWrite) {
    rollbackSyncWriteOnTopOfAbortedSyncWrite(false, true);
}

TEST_P(RollbackDcpTest, RollbackSyncWriteOnTopOfAbortedSyncDelete) {
    rollbackSyncWriteOnTopOfAbortedSyncWrite(true, false);
}

TEST_P(RollbackDcpTest, RollbackSyncDeleteOnTopOfAbortedSyncDelete) {
    rollbackSyncWriteOnTopOfAbortedSyncWrite(true, true);
}

void RollbackDcpTest::rollbackToZeroWithSyncWrite(bool deleted) {
    // Save the pre-rollback HashTable state for later comparison
    auto htState = getHtState();

    // Need to write more than 10 items to ensure a rollback
    createItems(11 /*numItems*/, 1 /*numFlushes*/);
    addStream(11 /*numItems*/);
    auto stream = static_cast<MockPassiveStream*>(
            (consumer->getVbucketStream(vbid)).get());
    ASSERT_TRUE(stream);

    auto highCompletedAndPreparedSeqno = vb->getHighSeqno() + 1;
    auto key = makeStoredDocKey("anykey_0_0");
    doPrepareAndCommit(key, "value2", deleted);

    // Rollback everything (because the rollback seqno is > seqno / 2)
    store->setVBucketState(vbid, vbStateAtRollback);
    auto rollbackSeqno = 1;
    EXPECT_EQ(TaskStatus::Complete, store->rollback(vbid, rollbackSeqno));
    EXPECT_EQ(0, store->getVBucket(vbid)->getHighSeqno());

    auto& passiveDm = static_cast<const PassiveDurabilityMonitor&>(
            vb->getDurabilityMonitor());
    EXPECT_EQ(0, passiveDm.getNumTracked());
    EXPECT_EQ(highCompletedAndPreparedSeqno, passiveDm.getHighPreparedSeqno());
    EXPECT_EQ(highCompletedAndPreparedSeqno, passiveDm.getHighCompletedSeqno());

    auto newVb = store->getVBucket(vbid);
    EXPECT_EQ(0, newVb->getNumItems());
    EXPECT_EQ(0, newVb->getNumTotalItems());
    auto& newPassiveDm = static_cast<const PassiveDurabilityMonitor&>(
            newVb->getDurabilityMonitor());
    EXPECT_EQ(0, newPassiveDm.getNumTracked());
    EXPECT_EQ(0, newPassiveDm.getHighPreparedSeqno());
    EXPECT_EQ(0, newPassiveDm.getHighCompletedSeqno());
    EXPECT_EQ(htState.dump(0), getHtState().dump(0));
}

TEST_P(RollbackDcpTest, RollbackToZeroWithSyncWrite) {
    rollbackToZeroWithSyncWrite(false);
}

TEST_P(RollbackDcpTest, RollbackToZeroWithSyncDelete) {
    rollbackToZeroWithSyncWrite(true);
}

/**
 * Checks the state of the HashTable and PassiveDM post-rollback
 *
 * Pre-rollback our sequence of events should be:
 *  - 1 base item
 *  - 1 prepare
 *  - 1 commit
 *
 * Post-rollback our sequence of events should be:
 *  - 1 base item
 *  - 1 prepare
 */
void RollbackDcpTest::rollbackCommit(bool deleted) {
    writeBaseItems();
    // Rollback only the commit
    auto baseItems = vb->getNumTotalItems();
    auto rollbackSeqno = vb->getHighSeqno() + 1;

    auto key = makeStoredDocKey("anykey_0_0");
    doPrepare(key, "value2", deleted);

    // Save the pre-rollback HashTable state for later comparison
    auto htState = getHtState();
    doCommit(key);

    store->setVBucketState(vbid, vbStateAtRollback);
    EXPECT_EQ(TaskStatus::Complete, store->rollback(vbid, rollbackSeqno));
    EXPECT_EQ(rollbackSeqno, store->getVBucket(vbid)->getHighSeqno());

    auto& passiveDm = static_cast<const PassiveDurabilityMonitor&>(
            vb->getDurabilityMonitor());
    EXPECT_EQ(1, passiveDm.getNumTracked());
    EXPECT_EQ(rollbackSeqno, passiveDm.getHighPreparedSeqno());
    EXPECT_EQ(0, passiveDm.getHighCompletedSeqno());
    EXPECT_EQ(baseItems, vb->getNumItems());
    EXPECT_EQ(baseItems, vb->getNumTotalItems());
    EXPECT_EQ(htState.dump(0), getHtState().dump(0));
}

TEST_P(RollbackDcpTest, RollbackCommit) {
    rollbackCommit(false);
}

TEST_P(RollbackDcpTest, RollbackSyncDeleteCommit) {
    rollbackCommit(true);
}

/**
 * Checks the state of the HashTable and PassiveDM post-rollback
 *
 * Pre-rollback our sequence of events should be:
 *  - 1 base item
 *  - 1 prepare
 *  - 1 commit
 *  - 1 prepare
 *  - 1 commit
 *
 * Post-rollback our sequence of events should be:
 *  - 1 base item
 *  - 1 prepare
 *  - 1 commit
 *  - 1 prepare
 */
void RollbackDcpTest::rollbackCommitOnTopOfSyncWrite(bool syncDeleteFirst,
                                                     bool syncDeleteSecond,
                                                     bool lowMemoryRollback) {
    writeBaseItems();
    auto highCompletedAndPreparedSeqno = vb->getHighSeqno() + 1;
    auto key = makeStoredDocKey("anykey_0_0");

    doPrepareAndCommit(key, "value2", syncDeleteFirst);
    // Rollback only the second commit.
    auto rollbackSeqno = vb->getHighSeqno() + 1;

    doPrepare(key, "value3", syncDeleteSecond);

    // Save the pre-rollback HashTable state for later comparison
    auto htState = getHtState();
    doCommit(key);

    if (lowMemoryRollback) {
        // Force low-memory condition by adjusting the quota.
        store->getEPEngine().getEpStats().setMaxDataSize(1);
    }

    store->setVBucketState(vbid, vbStateAtRollback);
    EXPECT_EQ(TaskStatus::Complete, store->rollback(vbid, rollbackSeqno));
    EXPECT_EQ(rollbackSeqno, store->getVBucket(vbid)->getHighSeqno());

    auto& passiveDm = static_cast<const PassiveDurabilityMonitor&>(
            vb->getDurabilityMonitor());
    EXPECT_EQ(1, passiveDm.getNumTracked());
    EXPECT_EQ(rollbackSeqno, passiveDm.getHighPreparedSeqno());
    EXPECT_EQ(highCompletedAndPreparedSeqno, passiveDm.getHighCompletedSeqno());
    EXPECT_EQ(htState.dump(0), getHtState().dump(0));
    auto expectedItems = syncDeleteFirst ? 0 : 1;
    EXPECT_EQ(expectedItems, vb->getNumItems());
    EXPECT_EQ(expectedItems, vb->getNumTotalItems());
}

TEST_P(RollbackDcpTest, RollbackCommitOnTopOfSyncWrite) {
    rollbackCommitOnTopOfSyncWrite(false, false);
}

TEST_P(RollbackDcpTest, RollbackSyncDeleteCommitOnTopOfSyncWrite) {
    rollbackCommitOnTopOfSyncWrite(false, true);
}

TEST_P(RollbackDcpTest, RollbackCommitOnTopOfSyncDelete) {
    rollbackCommitOnTopOfSyncWrite(true, false);
}

TEST_P(RollbackDcpTest, RollbackCommitOnTopOfSyncDelete_LowMemory) {
    rollbackCommitOnTopOfSyncWrite(true, false, true);
}

TEST_P(RollbackDcpTest, RollbackSyncDeleteCommitOnTopOfSyncDelete) {
    rollbackCommitOnTopOfSyncWrite(true, true);
}

/**
 * Checks the state of the HashTable and PassiveDM post-rollback
 *
 * Pre-rollback our sequence of events should be:
 *  - 1 base item
 *  - 1 prepare
 *  - 1 abort
 *  - 1 prepare
 *  - 1 commit
 *
 * Post-rollback our sequence of events should be:
 *  - 1 base item
 *  - 1 prepare
 *  - 1 abort
 *  - 1 prepare
 */
void RollbackDcpTest::rollbackCommitOnTopOfAbortedSyncWrite(
        bool syncDeleteFirst, bool syncDeleteSecond) {
    writeBaseItems();
    auto highCompletedAndPreparedSeqno = vb->getHighSeqno() + 1;
    auto key = makeStoredDocKey("anykey_0_0");
    doPrepareAndAbort(key, "value2", syncDeleteFirst);
    // Rollback only the commit
    auto rollbackSeqno = vb->getHighSeqno() + 1;

    doPrepare(key, "value3", syncDeleteSecond);

    // Save the pre-rollback HashTable state for later comparison
    auto htState = getHtState();
    doCommit(key);

    store->setVBucketState(vbid, vbStateAtRollback);
    EXPECT_EQ(TaskStatus::Complete, store->rollback(vbid, rollbackSeqno));
    EXPECT_EQ(rollbackSeqno, store->getVBucket(vbid)->getHighSeqno());

    auto& passiveDm = static_cast<const PassiveDurabilityMonitor&>(
            vb->getDurabilityMonitor());
    EXPECT_EQ(1, passiveDm.getNumTracked());
    EXPECT_EQ(rollbackSeqno, passiveDm.getHighPreparedSeqno());
    EXPECT_EQ(highCompletedAndPreparedSeqno, passiveDm.getHighCompletedSeqno());
    EXPECT_EQ(htState.dump(0), getHtState().dump(0));
    EXPECT_EQ(1, vb->getNumItems());
    EXPECT_EQ(1, vb->getNumTotalItems());
}

TEST_P(RollbackDcpTest, RollbackCommitOnTopOfAbortedSyncWrite) {
    rollbackCommitOnTopOfAbortedSyncWrite(false, false);
}

TEST_P(RollbackDcpTest, RollbackSyncDeleteCommitOnTopOfAbortedSyncWrite) {
    rollbackCommitOnTopOfAbortedSyncWrite(false, true);
}

TEST_P(RollbackDcpTest, RollbackCommitOnTopOfAbortedSyncDelete) {
    rollbackCommitOnTopOfAbortedSyncWrite(true, false);
}

TEST_P(RollbackDcpTest, RollbackSyncDeleteCommitOnTopOfAbortedSyncDelete) {
    rollbackCommitOnTopOfAbortedSyncWrite(true, true);
}

// Related to MB-42093 check the collection item counts are correct following
// a rollback
TEST_P(RollbackDcpTest, RollbackCollectionCounts) {
    writeBaseItems(2);
    // rollback the deletes and replay them
    auto rollbackSeqno = vb->getHighSeqno();
    auto key1 = makeStoredDocKey("anykey_0_0");
    auto key2 = makeStoredDocKey("anykey_0_1");

    auto vb = store->getVBucket(vbid);

    auto check = [&vb](int expected) {
        auto handle = vb->lockCollections();
        EXPECT_EQ(expected, handle.getItemCount(CollectionID::Default));
        EXPECT_EQ(expected, vb->getNumItems());
        EXPECT_EQ(expected, vb->getNumTotalItems());
    };

    check(2);
    doDelete(key1);
    check(1);
    doDelete(key2);
    check(0);

    store->setVBucketState(vbid, vbStateAtRollback);
    EXPECT_EQ(TaskStatus::Complete, store->rollback(vbid, rollbackSeqno));
    EXPECT_EQ(rollbackSeqno, vb->getHighSeqno());

    check(2);
    doDelete(key1);
    check(1);
    doDelete(key2);
    check(0);
}

/**
 * Checks the state of the HashTable and PassiveDM post-rollback
 *
 * Pre-rollback our sequence of events should be:
 *  - 1 base item
 *  - 1 prepare
 *  - 1 abort
 *
 * Post-rollback our sequence of events should be:
 *  - 1 base item
 *  - 1 prepare
 */
void RollbackDcpTest::rollbackAbort(bool deleted) {
    writeBaseItems();
    // Rollback only the abort
    auto rollbackSeqno = vb->getHighSeqno() + 1;

    auto key = makeStoredDocKey("anykey_0_0");
    doPrepare(key, "value2", deleted);

    // Save the pre-rollback HashTable state for later comparison
    auto htState = getHtState();
    doAbort(key);

    store->setVBucketState(vbid, vbStateAtRollback);
    EXPECT_EQ(TaskStatus::Complete, store->rollback(vbid, rollbackSeqno));

    auto& passiveDm = static_cast<const PassiveDurabilityMonitor&>(
            vb->getDurabilityMonitor());
    EXPECT_EQ(1, passiveDm.getNumTracked());
    EXPECT_EQ(rollbackSeqno, passiveDm.getHighPreparedSeqno());
    EXPECT_EQ(0, passiveDm.getHighCompletedSeqno());
    EXPECT_EQ(htState.dump(0), getHtState().dump(0));
    EXPECT_EQ(1, vb->getNumItems());
    EXPECT_EQ(1, vb->getNumTotalItems());
}

TEST_P(RollbackDcpTest, RollbackAbort) {
    rollbackAbort(false);
}

TEST_P(RollbackDcpTest, RollbackSyncDeleteAbort) {
    rollbackAbort(true);
}

/**
 * Checks the state of the HashTable and PassiveDM post-rollback
 *
 * Pre-rollback our sequence of events should be:
 *  - 1 base item
 *  - 1 prepare
 *  - 1 commit
 *  - 1 prepare
 *  - 1 abort
 *
 * Post-rollback our sequence of events should be:
 *  - 1 base item
 *  - 1 prepare
 *  - 1 commit
 *  - 1 prepare
 */
void RollbackDcpTest::rollbackAbortOnTopOfSyncWrite(bool syncDeleteFirst,
                                                    bool syncDeleteSecond) {
    writeBaseItems();
    auto highCompletedAndPreparedSeqno = vb->getHighSeqno() + 1;
    auto key = makeStoredDocKey("anykey_0_0");

    doPrepareAndCommit(key, "value2", syncDeleteFirst);

    // Rollback only the abort
    auto rollbackSeqno = vb->getHighSeqno() + 1;

    doPrepare(key, "value3", syncDeleteSecond);

    // Save the pre-rollback HashTable state for later comparison
    auto htState = getHtState();
    doAbort(key);

    store->setVBucketState(vbid, vbStateAtRollback);
    EXPECT_EQ(TaskStatus::Complete, store->rollback(vbid, rollbackSeqno));
    EXPECT_EQ(rollbackSeqno, store->getVBucket(vbid)->getHighSeqno());

    auto& passiveDm = static_cast<const PassiveDurabilityMonitor&>(
            vb->getDurabilityMonitor());
    EXPECT_EQ(1, passiveDm.getNumTracked());
    EXPECT_EQ(rollbackSeqno, passiveDm.getHighPreparedSeqno());
    EXPECT_EQ(highCompletedAndPreparedSeqno, passiveDm.getHighCompletedSeqno());

    EXPECT_EQ(htState.dump(0), getHtState().dump(0));
    auto expectedItems = syncDeleteFirst ? 0 : 1;
    EXPECT_EQ(expectedItems, vb->getNumItems());
    EXPECT_EQ(expectedItems, vb->getNumTotalItems());
}

TEST_P(RollbackDcpTest, RollbackAbortOnTopOfSyncWrite) {
    rollbackAbortOnTopOfSyncWrite(false, false);
}

TEST_P(RollbackDcpTest, RollbackSyncDeleteAbortOnTopOfSyncWrite) {
    rollbackAbortOnTopOfSyncWrite(false, true);
}

TEST_P(RollbackDcpTest, RollbackAbortOnTopOfSyncDelete) {
    rollbackAbortOnTopOfSyncWrite(true, false);
}

TEST_P(RollbackDcpTest, RollbackSyncDeleteAbortOnTopOfSyncDelete) {
    rollbackAbortOnTopOfSyncWrite(true, true);
}

/**
 * Checks the state of the HashTable and PassiveDM post-rollback
 *
 * Pre-rollback our sequence of events should be:
 *  - 1 base item
 *  - 1 prepare
 *  - 1 abort
 *  - 1 prepare
 *  - 1 abort
 *
 * Post-rollback our sequence of events should be:
 *  - 1 base item
 *  - 1 prepare
 *  - 1 abort
 *  - 1 prepare
 */
void RollbackDcpTest::rollbackAbortOnTopOfAbortedSyncWrite(
        bool syncDeleteFirst, bool syncDeleteSecond) {
    writeBaseItems();
    auto highCompletedAndPreparedSeqno = vb->getHighSeqno() + 1;
    auto key = makeStoredDocKey("anykey_0_0");
    doPrepareAndAbort(key, "value2", syncDeleteFirst);
    // Rollback only the abort
    auto rollbackSeqno = vb->getHighSeqno() + 1;

    doPrepare(key, "value3", syncDeleteSecond);

    // Save the pre-rollback HashTable state for later comparison
    auto htState = getHtState();
    doAbort(key);

    store->setVBucketState(vbid, vbStateAtRollback);
    EXPECT_EQ(TaskStatus::Complete, store->rollback(vbid, rollbackSeqno));
    EXPECT_EQ(rollbackSeqno, store->getVBucket(vbid)->getHighSeqno());

    auto& passiveDm = static_cast<const PassiveDurabilityMonitor&>(
            vb->getDurabilityMonitor());
    EXPECT_EQ(1, passiveDm.getNumTracked());
    EXPECT_EQ(rollbackSeqno, passiveDm.getHighPreparedSeqno());
    EXPECT_EQ(highCompletedAndPreparedSeqno, passiveDm.getHighCompletedSeqno());

    EXPECT_EQ(htState.dump(0), getHtState().dump(0));
    EXPECT_EQ(1, vb->getNumItems());
    EXPECT_EQ(1, vb->getNumTotalItems());
}

TEST_P(RollbackDcpTest, RollbackAbortOnTopOfAbortedSyncWrite) {
    rollbackAbortOnTopOfAbortedSyncWrite(false, false);
}

TEST_P(RollbackDcpTest, RollbackSyncDeleteAbortOnTopOfAbortedSyncWrite) {
    rollbackAbortOnTopOfAbortedSyncWrite(false, true);
}

TEST_P(RollbackDcpTest, RollbackAbortOnTopOfAbortedSyncDelete) {
    rollbackAbortOnTopOfAbortedSyncWrite(true, false);
}

TEST_P(RollbackDcpTest, RollbackSyncDeleteAbortOnTopOfAbortedSyncDelete) {
    rollbackAbortOnTopOfAbortedSyncWrite(true, true);
}

TEST_P(RollbackDcpTest, RollbackUnpersistedAbortDoesNotLoadOlderPrepare) {
    // MB-39333: Rolling back an unpersisted abort should not attempt
    // to load an earlier version from disk - if the prepare needs to be
    // reloaded, it will be done as part of loadPreparedSyncWrite

    store->setVBucketState(vbid, vbStateAtRollback);
    consumer->addStream(/*opaque*/ 0, vbid, /*flags*/ 0);

    auto key = makeStoredDocKey("key");
    // persist a prepare and commit (both are flushed immediately)
    doPrepareAndCommit(key, "value", /*not a syncdelete */ false);

    auto rollbackSeqno = vb->getHighSeqno();

    // Save the pre-rollback HashTable state for later comparison
    auto htState = getHtState();

    // store but _do not_ flush a prepare and an abort
    doPrepareAndAbort(key,
                      "value2",
                      /*not a syncdelete */ false,
                      /* don't flush */ false);

    EXPECT_EQ(TaskStatus::Complete, store->rollback(vbid, rollbackSeqno));
    EXPECT_EQ(rollbackSeqno, store->getVBucket(vbid)->getHighSeqno());

    auto& passiveDm = static_cast<const PassiveDurabilityMonitor&>(
            vb->getDurabilityMonitor());
    EXPECT_EQ(0, passiveDm.getNumTracked());
    EXPECT_EQ(1, passiveDm.getHighPreparedSeqno());
    EXPECT_EQ(1, passiveDm.getHighCompletedSeqno());
    // expect only the one committed item to be left
    EXPECT_EQ(1, vb->getNumTotalItems());
    EXPECT_EQ(1, vb->getNumItems());

    // MB-39333: Rolling back the abort would erroneously reload the first
    // prepare (seqno 2 here) into the ht.
    EXPECT_FALSE(vb->ht.findOnlyPrepared(key).storedValue);

    EXPECT_EQ(htState.dump(), getHtState().dump());
}

class ReplicaRollbackDcpTest : public SingleThreadedEPBucketTest {
public:
    ReplicaRollbackDcpTest() = default;

    void SetUp() override {
        SingleThreadedEPBucketTest::SetUp();
        store->setVBucketState(vbid, vbucket_state_active);
        vb = store->getVBucket(vbid);
        producers = std::make_unique<MockDcpMessageProducers>();
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

    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::No),
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

    auto& mockConnMap =
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
                      [](const std::vector<vbucket_failover_t>&) {
                          return ENGINE_SUCCESS;
                      },
                      {}));

    auto stream = producer->findStream(vbid);
    ASSERT_TRUE(stream->isActive());

    producer->notifySeqnoAvailable(
            vb->getId(), vb->getHighSeqno(), SyncWriteOperation::No);

    // Step which will notify the snapshot task
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(*producers));
    EXPECT_EQ(1, producer->getCheckpointSnapshotTask()->queueSize());

    // Now call run on the snapshot task to move checkpoint into DCP stream
    producer->getCheckpointSnapshotTask()->run();

    // snapshot marker
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers->last_op);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);

    auto kvb = engine->getKVBucket();

    // Perform the rollback
    EXPECT_EQ(TaskStatus::Complete, kvb->rollback(vbid, 0));

    // The stream should now be dead
    EXPECT_FALSE(stream->isActive()) << "Stream should be dead";

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpStreamEnd, producers->last_op)
            << "stream should have received a STREAM_END";

    // Stop Producer checkpoint processor task
    producer->cancelCheckpointCreatorTask();
}

auto allConfigValues =
        ::testing::Combine(::testing::Values("couchdb"
#ifdef EP_USE_MAGMA
                                             ,
                                             "magma"
#endif
                                             ),
                           ::testing::Values("value_only", "full_eviction"),
                           ::testing::Values("replica", "pending"));

// Test cases which run in both Full and Value eviction on replica and pending
// vbucket states
INSTANTIATE_TEST_SUITE_P(FullAndValueEvictionOnReplicaAndPending,
                         RollbackTest,
                         allConfigValues,
                         RollbackTest::PrintToStringParamName);

// Test cases which run in both Full and Value eviction on replica and pending
// vbucket states
INSTANTIATE_TEST_SUITE_P(FullAndValueEvictionOnReplicaAndPending,
                         RollbackDcpTest,
                         allConfigValues,
                         RollbackTest::PrintToStringParamName);
