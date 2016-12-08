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

#include "dcp/dcpconnmap.h"
#include "evp_store_test.h"
#include "makestoreddockey.h"
#include "programs/engine_testapp/mock_server.h"

class RollbackTest : public EPBucketTest,
                     public ::testing::WithParamInterface<std::string>
{
    void SetUp() override {
        EPBucketTest::SetUp();
        // Start vbucket as active to allow us to store items directly to it.
        store->setVBucketState(vbid, vbucket_state_active, false);

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
        ASSERT_EQ(dummy_elements, store->flushVBucket(vbid));
        initial_seqno = dummy_elements;
    }

protected:
    /*
     * Fake callback emulating dcp_add_failover_log
     */
    static ENGINE_ERROR_CODE fakeDcpAddFailoverLog(vbucket_failover_t* entry,
                                                   size_t nentries,
                                                   const void *cookie) {
        return ENGINE_SUCCESS;
    }

    /**
     * Test rollback after deleting an item.
     * @param flush_before_rollback: Should the vbuckt be flushed to disk just
     *        before the rollback (i.e. guaranteeing the in-memory state is in sync
     *        with disk).
     */
    void rollback_after_deletion_test(bool flush_before_rollback) {
        // Setup: Store an item then flush the vBucket (creating a checkpoint);
        // then delete the item and create a second checkpoint.
        StoredDocKey a = makeStoredDocKey("key");
        auto item_v1 = store_item(vbid, a, "1");
        ASSERT_EQ(initial_seqno + 1, item_v1.getBySeqno());
        ASSERT_EQ(1, store->flushVBucket(vbid));
        uint64_t cas = item_v1.getCas();
        mutation_descr_t mut_info;
        ASSERT_EQ(ENGINE_SUCCESS,
                  store->deleteItem(a, &cas, vbid, /*cookie*/nullptr,
                                    /*force*/false, /*itemMeta*/nullptr,
                                    &mut_info));
        if (flush_before_rollback) {
            ASSERT_EQ(1, store->flushVBucket(vbid));
        }
        // Sanity-check - item should no longer exist.
        EXPECT_EQ(ENGINE_KEY_ENOENT,
                  store->get(a, vbid, nullptr, {}).getStatus());

        // Test - rollback to seqno of item_v1 and verify that the previous value
        // of the item has been restored.
        store->setVBucketState(vbid, vbucket_state_replica, false);
        ASSERT_EQ(ENGINE_SUCCESS, store->rollback(vbid, item_v1.getBySeqno()));
        auto result = store->public_getInternal(a, vbid, /*cookie*/nullptr,
                                                vbucket_state_replica, {});
        ASSERT_EQ(ENGINE_SUCCESS, result.getStatus());
        EXPECT_EQ(item_v1, *result.getValue())
            << "Fetched item after rollback should match item_v1";
        delete result.getValue();

        if (!flush_before_rollback) {
            EXPECT_EQ(0, store->flushVBucket(vbid));
        }
    }

    // Test rollback after modifying an item.
    void rollback_after_mutation_test(bool flush_before_rollback) {
        // Setup: Store an item then flush the vBucket (creating a checkpoint);
        // then update the item with a new value and create a second checkpoint.
        StoredDocKey a = makeStoredDocKey("a");
        auto item_v1 = store_item(vbid, a, "old");
        ASSERT_EQ(initial_seqno + 1, item_v1.getBySeqno());
        ASSERT_EQ(1, store->flushVBucket(vbid));

        auto item2 = store_item(vbid, a, "new");
        ASSERT_EQ(initial_seqno + 2, item2.getBySeqno());

        StoredDocKey key = makeStoredDocKey("key");
        store_item(vbid, key, "meh");

        if (flush_before_rollback) {
            EXPECT_EQ(2, store->flushVBucket(vbid));
        }

        // Test - rollback to seqno of item_v1 and verify that the previous value
        // of the item has been restored.
        store->setVBucketState(vbid, vbucket_state_replica, false);
        ASSERT_EQ(ENGINE_SUCCESS, store->rollback(vbid, item_v1.getBySeqno()));
        ASSERT_EQ(item_v1.getBySeqno(), store->getVBucket(vbid)->getHighSeqno());

        // a should have the value of 'old'
        {
            auto result = store->get(a, vbid, nullptr, {});
            ASSERT_EQ(ENGINE_SUCCESS, result.getStatus());
            EXPECT_EQ(item_v1, *result.getValue())
                << "Fetched item after rollback should match item_v1";
            delete result.getValue();
        }

        // key should be gone
        {
            auto result = store->get(key, vbid, nullptr, {});
            EXPECT_EQ(ENGINE_KEY_ENOENT, result.getStatus())
                << "A key set after the rollback point was found";
        }

        if (!flush_before_rollback) {
            // The rollback should of wiped out any keys waiting for persistence
            EXPECT_EQ(0, store->flushVBucket(vbid));
        }
    }

// This test triggers MSVC 'cl' to assert, a lot of time has been spent trying
// to tweak the code so it compiles, but no solution yet. Disabled for VS 2013
#if !defined(_MSC_VER) || _MSC_VER != 1800
    void rollback_to_middle_test(bool flush_before_rollback) {
        // create some more checkpoints just to see a few iterations
        // of parts of the rollback function.

        // need to store a certain number of keys because rollback
        // 'bails' if the rollback is too much.
        for (int i = 0; i < 6; i++) {
            store_item(vbid, makeStoredDocKey("key_" + std::to_string(i)), "dontcare");
        }
        // the roll back function will rewind disk to key7.
        auto rollback_item = store_item(vbid, makeStoredDocKey("key7"), "dontcare");
        ASSERT_EQ(7, store->flushVBucket(vbid));

        // every key past this point will be lost from disk in a mid-point.
        auto item_v1 = store_item(vbid, makeStoredDocKey("rollback-cp-1"), "keep-me");
        auto item_v2 = store_item(vbid, makeStoredDocKey("rollback-cp-2"), "rollback to me");
        store_item(vbid, makeStoredDocKey("rollback-cp-3"), "i'm gone");
        auto rollback = item_v2.getBySeqno(); // ask to rollback to here.
        ASSERT_EQ(3, store->flushVBucket(vbid));

        for (int i = 0; i < 3; i++) {
            store_item(vbid, makeStoredDocKey("anotherkey_" + std::to_string(i)), "dontcare");
        }

        if (flush_before_rollback) {
            ASSERT_EQ(3, store->flushVBucket(vbid));
        }


        // Rollback should succeed, but rollback to 0
        store->setVBucketState(vbid, vbucket_state_replica, false);
        EXPECT_EQ(ENGINE_SUCCESS, store->rollback(vbid, rollback));

        // These keys should be gone after the rollback
        for (int i = 0; i < 3; i++) {
            auto result = store->get(makeStoredDocKey("rollback-cp-" + std::to_string(i)), vbid, nullptr, {});
            EXPECT_EQ(ENGINE_KEY_ENOENT, result.getStatus())
                << "A key set after the rollback point was found";
        }

        // These keys should be gone after the rollback
        for (int i = 0; i < 3; i++) {
            auto result = store->get(makeStoredDocKey("anotherkey_" + std::to_string(i)), vbid, nullptr, {});
            EXPECT_EQ(ENGINE_KEY_ENOENT, result.getStatus())
                << "A key set after the rollback point was found";
        }

        // Rolled back to the previous checkpoint
        EXPECT_EQ(rollback_item.getBySeqno(),
                  store->getVBucket(vbid)->getHighSeqno());
    }
#endif

protected:
    int64_t initial_seqno;
};

TEST_P(RollbackTest, RollbackAfterMutation) {
    rollback_after_mutation_test(/*flush_before_rollbaack*/true);
}

TEST_P(RollbackTest, RollbackAfterMutationNoFlush) {
    rollback_after_mutation_test(/*flush_before_rollback*/false);
}

TEST_P(RollbackTest, RollbackAfterDeletion) {
    rollback_after_deletion_test(/*flush_before_rollback*/true);
}

TEST_P(RollbackTest, RollbackAfterDeletionNoFlush) {
    rollback_after_deletion_test(/*flush_before_rollback*/false);
}

#if !defined(_MSC_VER) || _MSC_VER != 1800
TEST_P(RollbackTest, RollbackToMiddleOfACheckpoint) {
    rollback_to_middle_test(true);
}

TEST_P(RollbackTest, RollbackToMiddleOfACheckpointNoFlush) {
    rollback_to_middle_test(false);
}
#endif

/*
 * The opencheckpointid of a bucket can be zero after a rollback.
 * From MB21784 if an opencheckpointid was zero it was assumed that the
 * vbucket was in backfilling state.  This caused the producer stream
 * request to be stuck waiting for backfilling to complete.
 */
TEST_P(RollbackTest, MB21784) {
    // Make the vbucket a replica
    store->setVBucketState(vbid, vbucket_state_replica, false);
    // Perform a rollback
    EXPECT_EQ(ENGINE_SUCCESS, store->rollback(vbid, initial_seqno))
        << "rollback did not return ENGINE_SUCCESS";

    // Assert the checkpointmanager clear function (called during rollback)
    // has set the opencheckpointid to zero
    auto vb = store->getVbMap().getBucket(vbid);
    auto& ckpt_mgr = vb->checkpointManager;
    EXPECT_EQ(0, ckpt_mgr.getOpenCheckpointId()) << "opencheckpointId not zero";

    // Create a new Dcp producer, reserving its cookie.
    get_mock_server_api()->cookie->reserve(cookie);
    dcp_producer_t producer = engine->getDcpConnMap().newProducer(
            cookie, "test_producer", /*notifyOnly*/false);

    uint64_t rollbackSeqno;
    auto err = producer->streamRequest(/*flags*/0,
                                       /*opaque*/0,
                                       /*vbucket*/vbid,
                                       /*start_seqno*/0,
                                       /*end_seqno*/0,
                                       /*vb_uuid*/0,
                                       /*snap_start*/0,
                                       /*snap_end*/0,
                                       &rollbackSeqno,
                                       RollbackTest::fakeDcpAddFailoverLog);
    EXPECT_EQ(ENGINE_SUCCESS, err)
        << "stream request did not return ENGINE_SUCCESS";
    // Close stream
    ASSERT_EQ(ENGINE_SUCCESS, producer->closeStream(/*opaque*/0, vbid));
    engine->handleDisconnect(cookie);
}

// Test cases which run in both Full and Value eviction
INSTANTIATE_TEST_CASE_P(FullAndValueEviction,
                        RollbackTest,
                        ::testing::Values("value_only", "full_eviction"),
                        [] (const ::testing::TestParamInfo<std::string>& info) {
                            return info.param;
                        });
