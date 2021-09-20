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

/**
 * Unit tests for Item Paging / Expiration.
 */

#include "../mock/mock_global_task.h"
#include "../mock/mock_paging_visitor.h"
#include "../mock/mock_stat_collector.h"
#include "bgfetcher.h"
#include "checkpoint_manager.h"
#include "checkpoint_utils.h"
#include "ep_bucket.h"
#include "ep_time.h"
#include "evp_store_single_threaded_test.h"
#include "item.h"
#include "item_eviction.h"
#include "kv_bucket.h"
#include "kvstore/kvstore.h"
#include "test_helpers.h"
#include "tests/mock/mock_synchronous_ep_engine.h"

#include <folly/portability/GTest.h>
#include <platform/cb_arena_malloc.h>
#include <platform/semaphore.h>
#include <programs/engine_testapp/mock_server.h>
#include <statistics/labelled_collector.h>
#include <string_utilities.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

using namespace std::string_literals;

using FlushResult = EPBucket::FlushResult;
using MoreAvailable = EPBucket::MoreAvailable;
using WakeCkptRemover = EPBucket::WakeCkptRemover;

/**
 * Test fixture for bucket quota tests.
 *
 * NOTE: All the tests using this (including subclasses) require memory
 * tracking to be enabled.
 */
class STBucketQuotaTest : public STParameterizedBucketTest {
protected:
    void SetUp() override {
        if (!config_string.empty()) {
            config_string += ";";
        }
        config_string +=
                "ht_size=47;"
                "magma_checkpoint_interval=0;"
                "magma_checkpoint_every_batch=true";

        STParameterizedBucketTest::SetUp();

        ObjectRegistry::onSwitchThread(engine.get());

        // By setting the magma config params above, it causes
        // magma to flush with every batch. This will release memory
        // being held by streaming sstables (~600KB). Otherwise, we would
        // have to bump up the quota for magma.
        increaseQuota(800 * 1024);

        // How many nonIO tasks we expect initially
        // - 0 for persistent.
        // - 1 for Ephemeral (EphTombstoneHTCleaner).
        if (std::get<0>(GetParam()) == "ephemeral") {
            ++initialNonIoTasks;
        }

        // Sanity check - need memory tracker to be able to check our memory
        // usage.
        ASSERT_TRUE(cb::ArenaMalloc::canTrackAllocations())
                << "Memory tracker not enabled - cannot continue";

        store->setVBucketState(vbid, vbucket_state_active);

        // Sanity check - to ensure memory usage doesn't increase without us
        // noticing.
        ASSERT_EQ(47, store->getVBucket(vbid)->ht.getSize())
                << "Expected to have a HashTable of size 47 (mem calculations "
                   "based on this).";
    }

    void increaseQuota(size_t val) {
        // Setup the bucket from the entry state, which can be polluted from
        // previous tests, i.e. not guaranteed to be '0'. The worst offender is
        // if a background thread is still around, it could have data in the
        // arena inflating it - e.g. rocksdb threads
        auto& stats = engine->getEpStats();
        const size_t expectedStart = stats.getPreciseTotalMemoryUsed();

        const size_t quota = expectedStart + val;
        engine->getConfiguration().setMaxSize(quota);

        // Set the water marks to 70% and 85%
        engine->getConfiguration().setMemLowWat(quota * 0.7);
        engine->getConfiguration().setMemHighWat(quota * 0.85);
    }

    cb::engine_errc storeItem(Item& item) {
        uint64_t cas = 0;
        return engine->storeInner(
                cookie, item, cas, StoreSemantics::Set, false);
    }

    /**
     * Write documents to the bucket until they fail with TMP_FAIL.
     * Note this stores via external API (epstore) so we trigger the
     * memoryCondition() code in the event of cb::engine_errc::no_memory.
     *
     * @param vbid vBucket to write items to.
     * @param expiry value for items. 0 == no TTL.
     * @return number of documents written.
     */
    size_t populateUntilTmpFail(Vbid vbid, rel_time_t ttl = 0) {
        size_t count = 0;
        const std::string value(1024, 'x'); // 1024B value to use for documents.
        cb::engine_errc result;
        const auto expiry =
                (ttl != 0) ? ep_abs_time(ep_reltime(ttl)) : time_t(0);
        for (result = cb::engine_errc::success;
             result == cb::engine_errc::success;
             count++) {
            auto key = makeStoredDocKey("xxx_" + std::to_string(count));
            auto item = make_item(vbid, key, value, expiry);
            // Set freqCount to 0 so will be a candidate for paging out straight
            // away.
            item.setFreqCounterValue(0);
            result = storeItem(item);
        }
        EXPECT_EQ(cb::engine_errc::temporary_failure, result);
        // Fixup count for last loop iteration.
        --count;

        auto& stats = engine->getEpStats();
        EXPECT_GT(stats.getEstimatedTotalMemoryUsed(),
                  stats.mem_high_wat.load())
                << "Expected to exceed high watermark after hitting TMPFAIL";
        EXPECT_GT(stats.getEstimatedTotalMemoryUsed(), stats.mem_low_wat.load())
                << "Expected to exceed low watermark after hitting TMPFAIL";

        // To ensure the Blobs can actually be removed from memory, they must have
        // a ref-count of 1. This will not be the case if there's any open
        // checkpoints hanging onto Items. Therefore force the creation of a new
        // checkpoint.
        store->getVBucket(vbid)->checkpointManager->createNewCheckpoint();

        // Ensure items are flushed to disk (so we can evict them).
        flushDirectlyIfPersistent(vbid);

#ifdef EP_USE_MAGMA
        // Magma... we need to run explicit compaction to make sure no
        // implicit compactions run that might use memory.
        if (hasMagma()) {
            auto kvstore = store->getRWUnderlyingByShard(0);
            // Force a compaction here to make sure there are no implicit
            // compactions to consume any memory
            CompactionConfig compactionConfig;
            auto cctx = std::make_shared<CompactionContext>(
                    vbid, compactionConfig, 0);
            auto vb = store->getLockedVBucket(vbid);
            EXPECT_TRUE(kvstore->compactDB(vb.getLock(), cctx));
        }
#endif
        return count;
    }

    int populateUntilAboveHighWaterMark(Vbid vbid, uint8_t freqCount = 0) {
        bool populate = true;
        int count = 0;
        auto& stats = engine->getEpStats();
        while (populate) {
            auto key = makeStoredDocKey("key_" + std::to_string(count++));
            auto item = make_item(vbid, key, {"x", 128}, 0 /*ttl*/);
            // By default, set freqCount to 0 so will be a candidate for paging
            // out straight away. Some callers may need to set a different value
            item.setFreqCounterValue(freqCount);
            EXPECT_EQ(cb::engine_errc::success, storeItem(item));
            populate = stats.getEstimatedTotalMemoryUsed() <=
                       stats.mem_high_wat.load();
        }
        return count;
    }

    /**
     * Store items evenly into the provided vbuckets until the given
     * predicate is met.
     * @return the number of items stored into each vbucket NB: may be off by 1
     *         for some vbuckets, dependent on when the predicate is satisfied
     */
    size_t populateVbsUntil(const std::vector<Vbid>& vbids,
                            const std::function<bool()>& predicate,
                            std::string_view keyPrefix = "key_",
                            size_t itemSize = 5000) {
        bool populate = true;
        int count = 0;
        while (populate) {
            for (auto vbid : vbids) {
                // scope to ensure everything is destroyed before checking the
                // predicate (in case the predicate depends on memory usage)
                {
                    auto key = makeStoredDocKey(std::string(keyPrefix) +
                                                std::to_string(count++));
                    auto item =
                            make_item(vbid, key, std::string(itemSize, 'x'));
                    // make the items have a range of MFU values
                    // without this, no matter what percent of items should be
                    // evicted, either all of them or none of them would be
                    // evicted as they are evicted/kept based on an mfu
                    // threshold
                    auto& ht = store->getVBucket(vbid)->ht;
                    auto mfu = item.getFreqCounterValue();
                    for (int i = 0; i < count % 256; ++i) {
                        // probabilistically update the MFU
                        mfu = ht.generateFreqValue(mfu);
                    }
                    item.setFreqCounterValue(mfu);
                    if (storeItem(item) != cb::engine_errc::success) {
                        ADD_FAILURE() << "Failed storing an item before the "
                                         "predicate returned true";
                        return count;
                    }

                    // Some ItemPager tests want to store up to the HWM, which
                    // is prevented by MB-46827 (bounded CM mem-usage). To allow
                    // that, just move the cursor and expel from checkpoints.
                    //
                    // Note: Flushing and expelling at every mutation is indeed
                    // expensive but it seems that we can't avoid that at the
                    // time of writing. Problem is that some ItemPager tests
                    // wants to verify that the ItemPager is able to push the
                    // mem-usage down to the LWM. And that is obviously based on
                    // the assumption that there's enough allocation in the HT
                    // for the pager to free-up. So, if (eg) I perform the
                    // flush+expel only periodically, then we do get to HWM but
                    // the allocation in the HT doesn't hit the required level
                    // for verifying the pager assumptions.
                    // For avoiding the runtime increase of those tests, I've
                    // just increased the value-size of stored items, so we
                    // manage to hit the same mem-conditions with less work.
                    flushAndExpelFromCheckpoints(vbid);
                }
                populate = !predicate();
                if (!populate) {
                    break;
                }
            }
        }
        return count;
    }

    /**
     * Directly flush the given vBucket (instead of calling a unit test
     * function) because we don't know/care how many items are going to be
     * flushed.
     */
    void flushDirectlyIfPersistent(Vbid vb) {
        if (persistent()) {
            auto& bucket = dynamic_cast<EPBucket&>(*store);
            bucket.flushVBucket(vb);
        }
    }

    /**
     * Directly flush the given vBucket (instead of calling a unit test
     * function) and test the output of the flush function.
     */
    void flushDirectlyIfPersistent(Vbid vb, const FlushResult& expected) {
        if (persistent()) {
            const auto res = dynamic_cast<EPBucket&>(*store).flushVBucket(vb);
            EXPECT_EQ(expected, res);
        }
    }

    /// Count of nonIO tasks we should initially have.
    size_t initialNonIoTasks = 0;
};

/**
 * Test fixture for item pager tests - enables the Item Pager (in addition to
 * what the parent class does).
 */
class STItemPagerTest : virtual public STBucketQuotaTest {
protected:
    void SetUp() override {
        if (!config_string.empty()) {
            config_string += ";";
        }
        config_string +=
                "concurrent_pagers=" + std::to_string(getNumConcurrentPagers());
        STBucketQuotaTest::SetUp();

        // For Ephemeral fail_new_data buckets we have no item pager, instead
        // the Expiry pager is used.
        if (std::get<1>(GetParam()) == "fail_new_data") {
            initializeExpiryPager();
            ++initialNonIoTasks;
        } else {
            // Everyone else uses the ItemPager.
            scheduleItemPager();
            ++initialNonIoTasks;
            itemPagerScheduled = true;
        }

        // Sanity check - should be no nonIO tasks ready to run,
        // and expected number in futureQ.
        auto& lpNonioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
        EXPECT_EQ(0, lpNonioQ.getReadyQueueSize());
        EXPECT_EQ(initialNonIoTasks, lpNonioQ.getFutureQueueSize());

        // We shouldn't be able to schedule the Item Pager task yet as it's not
        // ready.
        try {
            SCOPED_TRACE("");
            runNextTask(lpNonioQ, "Paging out items.");
            FAIL() << "Unexpectedly managed to run Item Pager";
        } catch (std::logic_error&) {
        }
    }

    /**
     * Run the pager which is scheduled when the high watermark is reached
     * (memoryCondition). This is either the ItemPager (for buckets where
     * items can be paged out - Persistent or Ephemeral-auto_delete), or
     * the Expiry pager (Ephemeral-fail_new_data).
     */
    void runHighMemoryPager() {
        auto& lpNonioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
        ASSERT_EQ(0, lpNonioQ.getReadyQueueSize());
        ASSERT_EQ(initialNonIoTasks, lpNonioQ.getFutureQueueSize());

        if (itemPagerScheduled) {
            // Item pager consists of two Tasks - the parent ItemPager task,
            // and then a task (via VCVBAdapter) to process each vBucket (which
            // there is just one of as we only have one vBucket online).
            runNextTask(lpNonioQ, "Paging out items.");
            ASSERT_EQ(0, lpNonioQ.getReadyQueueSize());
            ASSERT_EQ(initialNonIoTasks + 1, lpNonioQ.getFutureQueueSize());
            runNextTask(lpNonioQ, "Item pager no vbucket assigned");
        } else {
            runNextTask(lpNonioQ, "Paging expired items.");
            // Multiple vBuckets are processed in a single task run.
            runNextTask(lpNonioQ, "Expired item remover no vbucket assigned");
        }
        // Once complete, should have the same number of tasks we initially
        // had.
        ASSERT_EQ(0, lpNonioQ.getReadyQueueSize());
        ASSERT_EQ(initialNonIoTasks, lpNonioQ.getFutureQueueSize());

        // Ensure any deletes are flushed to disk (so item counts are accurate).
        flushDirectlyIfPersistent(vbid);
    }

    // get the resident ratio of the provided vbucket, expressed as a percentage
    static size_t getRRPercent(VBucket& vb) {
        size_t numItems = vb.getNumItems();
        size_t numResident = numItems - vb.getNumNonResidentItems();
        return (numItems != 0) ? size_t((numResident * 100.0) / numItems) : 100;
    }

    size_t getNumConcurrentPagers() const {
        return numConcurrentPagers;
    }

    /**
     * Set the configured number of PagingVisitors to run concurrently.
     *
     * Should be set before calling STItemPagerTest::SetUp()
     */
    void setNumConcurrentPagers(size_t num) {
        numConcurrentPagers = num;
    }

    size_t numConcurrentPagers = 1;
    /// Has the item pager been scheduled to run?
    bool itemPagerScheduled = false;
};

// Test that the ItemPager is scheduled when the Server Quota is reached, and
// that items are successfully paged out.
TEST_P(STItemPagerTest, ServerQuotaReached) {
    size_t count = populateUntilTmpFail(vbid);
    ASSERT_GE(count, 50) << "Too few documents stored";

    runHighMemoryPager();

    // For all configurations except ephemeral fail_new_data, memory usage
    // should have dropped.
    auto& stats = engine->getEpStats();
    auto vb = engine->getVBucket(vbid);
    if (std::get<1>(GetParam()) == "fail_new_data") {
        EXPECT_GT(stats.getPreciseTotalMemoryUsed(), stats.mem_low_wat.load())
                << "Expected still to exceed low watermark after hitting "
                   "TMPFAIL with fail_new_data bucket";
        EXPECT_EQ(count, vb->getNumItems());
    } else {
        size_t val{0};
        if (hasMagma()) {
            auto kvstore = store->getROUnderlyingByShard(0);
            kvstore->getStat("storage_mem_used", val);
        }
        EXPECT_LT(stats.getPreciseTotalMemoryUsed() - val,
                  stats.mem_low_wat.load())
                << "Expected to be below low watermark after running item "
                   "pager";
        const auto numResidentItems =
                vb->getNumItems() - vb->getNumNonResidentItems();
        EXPECT_LT(numResidentItems, count);
    }
}

TEST_P(STItemPagerTest, HighWaterMarkTriggersPager) {
    // Fill to just over HWM
    populateUntilAboveHighWaterMark(vbid);
    // Success if the pager is now ready
    runHighMemoryPager();
}

TEST_P(STItemPagerTest, PagerEvictsSomething) {
    // Pager can't run in fail_new_data policy so test is invalid
    if ((std::get<1>(GetParam()) == "fail_new_data")) {
        return;
    }

    // Fill to just over HWM
    populateUntilAboveHighWaterMark(vbid);

    // Flush so items are eligible for ejection
    flushDirectlyIfPersistent(vbid);

    auto vb = store->getVBucket(vbid);
    auto items = vb->getNumItems();

    // Success if the pager is now ready
    runHighMemoryPager();

    if (persistent()) {
        // Should have evicted something
        EXPECT_LT(0, store->getVBucket(vbid)->getNumNonResidentItems());
        EXPECT_EQ(vb->getNumItems(), items);

        // Bump up max size and HWM so we don't encounter memory issues during
        // get phase
        auto quota = engine->getEpStats().getPreciseTotalMemoryUsed() * 2;
        engine->getConfiguration().setMaxSize(quota);
        engine->getConfiguration().setMemHighWat(quota * 0.85);

        // Read all of our items to verify that they are still there. Some will
        // be on disk
        for (size_t i = 0; i < items; i++) {
            auto key = makeStoredDocKey("key_" + std::to_string(i));
            auto gv = getInternal(key,
                                  vbid,
                                  cookie,
                                  ForGetReplicaOp::No,
                                  get_options_t::QUEUE_BG_FETCH);
            switch (gv.getStatus()) {
            case cb::engine_errc::success:
                break;
            case cb::engine_errc::would_block: {
                ASSERT_TRUE(vb->hasPendingBGFetchItems());
                runBGFetcherTask();
                gv = getInternal(key,
                                 vbid,
                                 cookie,
                                 ForGetReplicaOp::No,
                                 get_options_t::NONE);
                EXPECT_EQ(cb::engine_errc::success, gv.getStatus());
                break;
            }
            default:
                FAIL() << "Unexpected status";
            }
        }
    } else {
        EXPECT_LT(vb->getNumItems(), items);
    }
}

// Tests that for the hifi_mfu eviction algorithm we visit replica vbuckets
// first.
TEST_P(STItemPagerTest, ReplicaItemsVisitedFirst) {
    // For the Expiry Pager we do not enforce the visiting of replica buckets
    // first.
    if ((std::get<1>(GetParam()) == "fail_new_data")) {
        return;
    }
    auto& lpNonioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];

    const Vbid activeVB = Vbid(0);
    const Vbid pendingVB = Vbid(1);
    const Vbid replicaVB = Vbid(2);

    // Set pendingVB online, initially as active (so we can populate it).
    store->setVBucketState(pendingVB, vbucket_state_active);
    // Set replicaVB online, initially as active (so we can populate it).
    store->setVBucketState(replicaVB, vbucket_state_active);

    // Add a document to both the active and pending vbucket.
    const std::string value(512, 'x'); // 512B value to use for documents.
    // add sufficient items to the active vb to trigger eviction even for
    // ephemeral.
    for (int ii = 0; ii < 40; ii++) {
        auto key = makeStoredDocKey("key_" + std::to_string(ii));
        auto activeItem = make_item(activeVB, key, value);
        auto pendingItem = make_item(pendingVB, key, value);
        ASSERT_EQ(cb::engine_errc::success, storeItem(activeItem));
        ASSERT_EQ(cb::engine_errc::success, storeItem(pendingItem));
    }

    store->setVBucketState(pendingVB, vbucket_state_pending);

    auto count = populateUntilTmpFail(replicaVB);
    store->setVBucketState(replicaVB, vbucket_state_replica);

    runNextTask(lpNonioQ, "Paging out items.");
    runNextTask(lpNonioQ, "Item pager no vbucket assigned");

    if (std::get<0>(GetParam()) == "ephemeral") {
        // We should have not evicted from replica vbuckets
        EXPECT_EQ(count, store->getVBucket(replicaVB)->getNumItems());
        // We should have evicted from the active/pending vbuckets
        auto activeAndPendingItems =
                store->getVBucket(activeVB)->getNumItems() +
                store->getVBucket(pendingVB)->getNumItems();
        EXPECT_NE(20, activeAndPendingItems);

    } else {
        // We should have evicted from replica vbuckets
        EXPECT_NE(0, store->getVBucket(replicaVB)->getNumNonResidentItems());
        auto evictedActiveAndPendingItems =
                store->getVBucket(activeVB)->getNumNonResidentItems() +
                store->getVBucket(pendingVB)->getNumNonResidentItems();
        // We should not have evicted from active or pending vbuckets
        EXPECT_EQ(0, evictedActiveAndPendingItems);
    }
    ASSERT_EQ(initialNonIoTasks, lpNonioQ.getFutureQueueSize());
}

// Test that when the server quota is reached, we delete items which have
// expired before any other items.
TEST_P(STItemPagerTest, ExpiredItemsDeletedFirst) {
    // Test only works for the now removed 2-bit LRU eviction algorithm
    // @todo Investigate converting the test to work with the new hifi_mfu
    // eviction algorithm.
    return;

    // Populate bucket with non-expiring items until we reach the low
    // watermark.
    size_t countA = 0;
    const std::string value(512, 'x'); // 512B value to use for documents.
    auto& stats = engine->getEpStats();
    do {
        auto key = makeStoredDocKey("key_" + std::to_string(countA));
        auto item = make_item(vbid, key, value);
        ASSERT_EQ(cb::engine_errc::success, storeItem(item));
        countA++;
    } while (stats.getEstimatedTotalMemoryUsed() < stats.mem_low_wat.load());

    ASSERT_GE(countA, 10)
            << "Expected at least 10 items before hitting low watermark";

    // Fill bucket with items with a TTL of 1s until we hit ENOMEM. When
    // we run the pager, we expect these items to be deleted first.
    auto countB = populateUntilTmpFail(vbid, 1);

    ASSERT_GE(countB, 50)
        << "Expected at least 50 documents total before hitting high watermark";

    // Advance time so when the pager runs it will find expired items.
    TimeTraveller billSPrestonEsq(2);

    EXPECT_EQ(countA + countB, store->getVBucket(vbid)->getNumItems());

    runHighMemoryPager();

    // Ensure deletes are flushed to disk (so any temp items removed from
    // HashTable).
    flushVBucketToDiskIfPersistent(vbid);

    // Check which items remain. We should have deleted all of the items with
    // a TTL, as they should have been considered first).

    // Initial documents should still exist. Note we need to use getMetaData
    // here as get() would expire the item on access.
    for (size_t ii = 0; ii < countA; ii++) {
        auto key = makeStoredDocKey("key_" + std::to_string(ii));
        auto result = store->get(key, vbid, cookie, get_options_t());
        EXPECT_EQ(cb::engine_errc::success, result.getStatus())
                << "For key:" << key;
    }

    // Documents which had a TTL should be deleted. Note it's hard to check
    // the specific keys without triggering an expire-on-access (and hence
    // doing the item pager's job for it). Therefore just check the count of
    // items still existing (should have decreased by the number of items
    // with TTLs) and expiry statistics.
    EXPECT_EQ(countA, store->getVBucket(vbid)->getNumItems());
    EXPECT_EQ(countB, stats.expired_pager);
    EXPECT_EQ(0, stats.expired_access);
    EXPECT_EQ(0, stats.expired_compactor);
}

// Test migrated and mutated from from ep_testsuite_basic so that it's less
// racey
TEST_P(STItemPagerTest, test_memory_limit) {
    // Test only works for the now removed 2-bit LRU eviction algorithm
    // @todo Investigate converting the test to work with the new hifi_mfu
    // eviction algorithm.
    return;

    // Now set max_size to be 10MiB
    std::string msg;
    EXPECT_EQ(
            cb::engine_errc::success,
            engine->setFlushParam(
                    "max_size", std::to_string(10 * 1024 * 1204).c_str(), msg));

    // Store a large document 4MiB
    std::string value(4 * 1024 * 1204, 'a');
    {
        auto item =
                make_item(vbid, {"key", DocKeyEncodesCollectionId::No}, value);
        // Set freqCount to 0 so will be a candidate for paging out straight
        // away.
        item.setFreqCounterValue(0);
        ASSERT_EQ(cb::engine_errc::success, storeItem(item));
    }

    if (std::get<0>(GetParam()) == "persistent") {
        // flush so the HT item becomes clean
        flush_vbucket_to_disk(vbid);

        // Now do some steps which will remove the checkpoint, all of these
        // steps are needed
        auto vb = engine->getVBucket(vbid);

        // Force close the current checkpoint
        vb->checkpointManager->createNewCheckpoint();
        // Reflush
        flush_vbucket_to_disk(vbid);
        EXPECT_EQ(1,
                  vb->checkpointManager->removeClosedUnrefCheckpoints().count);
    }

    // Now set max_size to be mem_used + 10% (we need some headroom)
    auto& stats = engine->getEpStats();
    EXPECT_EQ(cb::engine_errc::success,
              engine->setFlushParam(
                      "max_size",
                      std::to_string(stats.getEstimatedTotalMemoryUsed() * 1.10)
                              .c_str(),
                      msg));

    // The next tests use itemAllocate (as per a real SET)
    {
        auto [status, item] =
                engine->itemAllocate({"key2", DocKeyEncodesCollectionId::No},
                                     value.size(),
                                     0,
                                     0,
                                     0,
                                     0,
                                     vbid);
        EXPECT_EQ(cb::engine_errc::temporary_failure, status);
        EXPECT_FALSE(item);
    }

    // item_pager should be notified and ready to run
    runHighMemoryPager();

    if (std::get<0>(GetParam()) == "persistent") {
        EXPECT_EQ(1, stats.numValueEjects);
    }

    if (std::get<1>(GetParam()) != "fail_new_data") {
        // Enough should of been freed so itemAllocate can succeed
        auto [status, item] =
                engine->itemAllocate({"key2", DocKeyEncodesCollectionId::No},
                                     value.size(),
                                     0,
                                     0,
                                     0,
                                     0,
                                     vbid);
        EXPECT_EQ(cb::engine_errc::success, status);
        EXPECT_TRUE(item);
    }
}

/**
 * MB-29236: Test that if an item is eligible to be evicted but exceeding the
 * eviction threshold we do not add the maximum value (255) to the
 * ItemEviction histogram.
 */
TEST_P(STItemPagerTest, isEligible) {
    populateUntilTmpFail(vbid);

    EventuallyPersistentEngine* epe =
            ObjectRegistry::onSwitchThread(nullptr, true);
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);

    for (int ii = 0; ii < 10; ii++) {
        auto key = makeStoredDocKey("xxx_0");
        store->get(key, vbid, cookie, options);
        ObjectRegistry::onSwitchThread(epe);
    }
    auto pagerSemaphore = std::make_shared<cb::Semaphore>();
    Configuration& cfg = engine->getConfiguration();
    std::unique_ptr<MockPagingVisitor> pv = std::make_unique<MockPagingVisitor>(
            *engine->getKVBucket(),
            engine->getEpStats(),
            EvictionRatios{0.0 /* active&pending */,
                           0.0 /* replica */}, // evict nothing
            pagerSemaphore,
            ITEM_PAGER,
            false,
            VBucketFilter(),
            cfg.getItemEvictionAgePercentage(),
            cfg.getItemEvictionFreqCounterAgeThreshold());

    VBucketPtr vb = store->getVBucket(vbid);
    pv->visitBucket(vb);
    auto initialCount = Item::initialFreqCount;
    EXPECT_NE(initialCount,
              pv->getItemEviction().getThresholds(100.0, 0.0).first);
    EXPECT_NE(255, pv->getItemEviction().getThresholds(100.0, 0.0).first);
}

/**
 * MB-29333:  Test that if a vbucket contains a single document with an
 * execution frequency of Item::initialFreqCount, the document will
 * be evicted if the paging visitor is run a sufficient number of times.
 */
TEST_P(STItemPagerTest, decayByOne) {
    const std::string value(512, 'x'); // 512B value to use for documents.
    auto key = makeStoredDocKey("xxx_0");
    auto item = make_item(vbid, key, value, time_t(0));
    storeItem(item);

    auto pagerSemaphore = std::make_shared<cb::Semaphore>();
    Configuration& cfg = engine->getConfiguration();
    std::unique_ptr<MockPagingVisitor> pv = std::make_unique<MockPagingVisitor>(
            *engine->getKVBucket(),
            engine->getEpStats(),
            EvictionRatios{1.0 /* active&pending */,
                           1.0 /* replica */}, // try evict everything
            pagerSemaphore,
            ITEM_PAGER,
            false,
            VBucketFilter(),
            cfg.getItemEvictionAgePercentage(),
            cfg.getItemEvictionFreqCounterAgeThreshold());

    pv->setCurrentBucket(engine->getKVBucket()->getVBucket(vbid));
    flushVBucketToDiskIfPersistent(vbid);
    int iterationCount = 0;
    while ((pv->getEjected() == 0) &&
           iterationCount <= Item::initialFreqCount) {
        pv->setFreqCounterThreshold(0);
        VBucketPtr vb = store->getVBucket(vbid);
        vb->ht.visit(*pv);
        iterationCount++;
    }
    EXPECT_EQ(1, pv->getEjected());
}

/**
 * MB-29333:  Test that if a vbucket contains a single document with an
 * execution frequency of Item::initialFreqCount, but the document
 * is not eligible for eviction (due to being replica in ephemeral case and
 * not flushed in the persistent case) check that its frequency count is not
 * decremented.
 */
TEST_P(STItemPagerTest, doNotDecayIfCannotEvict) {
    const std::string value(512, 'x'); // 512B value to use for documents.
    auto key = makeStoredDocKey("xxx_0");
    auto item = make_item(vbid, key, value, time_t(0));
    storeItem(item);

    auto pagerSemaphore = std::make_shared<cb::Semaphore>();
    Configuration& cfg = engine->getConfiguration();
    std::unique_ptr<MockPagingVisitor> pv = std::make_unique<MockPagingVisitor>(
            *engine->getKVBucket(),
            engine->getEpStats(),
            EvictionRatios{1.0 /* active&pending */,
                           1.0 /* replica */}, // try evict everything
            pagerSemaphore,
            ITEM_PAGER,
            false,
            VBucketFilter(),
            cfg.getItemEvictionAgePercentage(),
            cfg.getItemEvictionFreqCounterAgeThreshold());

    pv->setCurrentBucket(engine->getKVBucket()->getVBucket(vbid));
    store->setVBucketState(vbid, vbucket_state_replica);
    for (int ii = 0; ii <= Item::initialFreqCount; ii++) {
        pv->setFreqCounterThreshold(0);
        pv->getItemEviction().reset();
        VBucketPtr vb = store->getVBucket(vbid);
        vb->ht.visit(*pv);
    }

    // Now make the document eligible for eviction.
    store->setVBucketState(vbid, vbucket_state_active);
    flushVBucketToDiskIfPersistent(vbid);

    // Check still not be able to evict, because the frequency count is still
    // at Item::initialFreqCount
    pv->setFreqCounterThreshold(0);
    pv->getItemEviction().reset();
    VBucketPtr vb = store->getVBucket(vbid);
    vb->ht.visit(*pv);
    auto initialFreqCount = Item::initialFreqCount;
    EXPECT_EQ(initialFreqCount,
              pv->getItemEviction().getThresholds(100.0, 0.0).first);
    EXPECT_EQ(0, pv->getEjected());

}

/**
 * MB-38315 found that when we BG Fetch a deleted item it can end up "stuck" in
 * the HashTable until the compactor removes the tombstone for it. This
 * increases memory pressure and could cause the system to lock up if the pager
 * finds no items eligible for eviction (e.g. BGFetched an entirely deleted data
 * set). Test that the item pager can page out a clean deleted item.
 */
TEST_P(STItemPagerTest, EvictBGFetchedDeletedItem) {
    // Only relevant to full eviction as Value Eviction will:
    //   1: Delete deleted items from the HashTable when they are persisted
    //      (also true for Full Eviction)
    //   2: Not allow BGFetches
    //
    // This means that there is no way for us to get a deleted item into the
    // HashTable that is not dirty. As such, this test can't work for Value
    // eviction.
    if (!fullEviction()) {
        return;
    }

    // 1) Write our document
    auto key = makeStoredDocKey("key");
    store_item(vbid, key, "value");
    flushVBucketToDiskIfPersistent(vbid, 1);

    delete_item(vbid, key);
    flushVBucketToDiskIfPersistent(vbid, 1);

    // 2) Evict it
    auto vb = store->getVBucket(vbid);

    const char* msg;
    vb->evictKey(&msg, vb->lockCollections(key));

    // 3) Get to schedule our BGFetch
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS | GET_DELETED_VALUE);
    auto res = store->get(key, vbid, cookie, options);
    EXPECT_EQ(cb::engine_errc::would_block, res.getStatus());

    EXPECT_EQ(0, vb->getNumItems());
    EXPECT_EQ(1, vb->getNumTempItems());

    // 4) Fetch it back into the HashTable
    runBGFetcherTask();

    // Should have replaced the temp item, but the item in the HT will be
    // deleted and not counted in NumItems
    EXPECT_EQ(0, vb->getNumItems());
    EXPECT_EQ(0, vb->getNumTempItems());

    auto checkItemStatePostFetch = [&](bool expected) {
        auto val = vb->fetchValidValue(WantsDeleted::Yes,
                                       TrackReference::No,
                                       QueueExpired::No,
                                       vb->lockCollections(key));
        if (expected) {
            EXPECT_TRUE(val.storedValue);
            EXPECT_TRUE(val.storedValue->isDeleted());
            EXPECT_FALSE(val.storedValue->isDirty());
            EXPECT_FALSE(val.storedValue->isTempItem());
        } else {
            EXPECT_FALSE(val.storedValue);
        }
    };

    // Check that the item is actually in the HT
    checkItemStatePostFetch(true /*expect item to exist*/);

    // 5) Re-run our get after BG Fetch (proving that it does not affect the
    // item). Returns success as we requested deleted values.
    res = store->get(key, vbid, cookie, options);
    EXPECT_EQ(cb::engine_errc::success, res.getStatus());

    EXPECT_EQ(0, vb->getNumItems());
    EXPECT_EQ(0, vb->getNumTempItems());

    // Check that the item is still in the HT
    checkItemStatePostFetch(true /*expect item to exist*/);

    // 6) Fill to just over HWM then run the pager
    // set the frequency counter of the stored items to the max value,
    // to make it very likely that the above test item will be evicted
    // (as it is much "colder")
    populateUntilAboveHighWaterMark(
            vbid, /* MFU */ std::numeric_limits<uint8_t>::max());
    // flush the vb, only items which are eligible for eviction are used to
    // determine the MFU threshold for evicting items.
    flushDirectlyIfPersistent(vbid);
    runHighMemoryPager();

    // Finally, check our item again. It should now have been evicted
    checkItemStatePostFetch(false /*expect item to not exist*/);
}

TEST_P(STItemPagerTest, ReplicaEvictedBeforeActive) {
    // MB-40531 test that the item pager does not evict from active vbuckets
    // if evicting from replicas can reclaim enough memory to reach the low
    // water mark (determined when the item pager first runs).
    // Test populates replicas to have _just_ enough pageable memory usage
    // so that eviction can bring memory usage below the low watermark
    // entirely by evicting from replicas, to confirm active vbuckets are _not_
    // evicted from.

    if (ephemeral()) {
        // Ephemeral does not evict from replicas
        GTEST_SKIP();
    }
    if (hasMagma()) {
        // Magma's memory usage makes tests relying on memory usage maths
        // difficult, and probably quite fragile/sensitive to magma changes.
        // The behaviour being tested is _not_ dependent on the backend,
        // so testing with couchstore _should_ be sufficient.
        GTEST_SKIP();
    }

    std::vector<Vbid> activeVBs = {Vbid(0), Vbid(1)};
    std::vector<Vbid> replicaVBs = {Vbid(2), Vbid(3)};

    setVBucketStateAndRunPersistTask(activeVBs[0], vbucket_state_active);
    setVBucketStateAndRunPersistTask(activeVBs[1], vbucket_state_active);

    // Set replicaVBs as active initially (so we can populate them easily).
    setVBucketStateAndRunPersistTask(replicaVBs[0], vbucket_state_active);
    setVBucketStateAndRunPersistTask(replicaVBs[1], vbucket_state_active);

    // bump the quota up. We need the evictable data stored in replicas to be
    // greater than the gap between high and low watermarks. With the default
    // quota, there's not enough "headroom" left after base memory usage to
    // populate vbs as desired.
    increaseQuota(2500000);

    auto& stats = engine->getEpStats();

    auto watermarkDiff = stats.mem_high_wat.load() - stats.mem_low_wat.load();

    // starting above the low water mark would mean that the replica pageable
    // memory will definitely not exceed the watermarkdiff when the high
    // watermark is reached.
    ASSERT_LT(stats.getPreciseTotalMemoryUsed(), stats.mem_low_wat.load());

    // populate the replica vbs until its evictable memory _exceeds_ the gap
    // between the high and low water mark. When the mem usage later passes the
    // high water mark, evicting from the replica should reclaim enough memory.
    auto replicaItemCount = populateVbsUntil(
            replicaVBs, [&stats, &store = store, replicaVBs, watermarkDiff] {
                // sanity check - if this fails the quota is too low -
                EXPECT_LT(stats.getPreciseTotalMemoryUsed(),
                          stats.mem_high_wat.load());

                size_t pageableMem = 0;
                for (const auto vbid : replicaVBs) {
                    pageableMem +=
                            store->getVBucket(vbid)->getPageableMemUsage();
                }
                // When the actives are populated, mem used will go _over_
                // the high water mark by a small amount. To evict only from
                // replicas, their total pageable memory must be at least the
                // watermark diff + a small excess to account for the final
                // inserted item _passing_ the high watermark.
                return pageableMem > watermarkDiff + 10000;
            });
    flushAndRemoveCheckpoints(replicaVBs[0]);
    flushAndRemoveCheckpoints(replicaVBs[1]);

    // We don't care exactly how many were stored, just that it is a
    // "large enough" number for percentages to be somewhat useful
    EXPECT_GT(replicaItemCount, 100);

    // Now fill the active VB until the high watermark is reached
    auto activeItemCount =
            populateVbsUntil(activeVBs, [&stats, &store = store] {
                return stats.getPreciseTotalMemoryUsed() >
                       stats.mem_high_wat.load();
            });

    flushAndRemoveCheckpoints(activeVBs[0]);
    flushAndRemoveCheckpoints(activeVBs[1]);

    // Flushing and removing checkpoints frees some memory, "top up" the
    // active vbuckets back to the high watermark.
    activeItemCount += populateVbsUntil(
            activeVBs,
            [this, &stats, &activeVBs] {
                flushAndRemoveCheckpoints(activeVBs[0]);
                flushAndRemoveCheckpoints(activeVBs[1]);
                bool readyToEvict = stats.getPreciseTotalMemoryUsed() >
                                    stats.mem_high_wat.load();
                if (readyToEvict) {
                    // This would be normally triggered by the _next_ item
                    // stored. Done within this predicate, before any
                    // deallocations take the memory usage below the watermark
                    // again
                    store->attemptToFreeMemory();
                }
                return readyToEvict;
            },
            "topup_keys");

    EXPECT_GT(activeItemCount, 100);

    setVBucketStateAndRunPersistTask(replicaVBs[0], vbucket_state_replica);
    setVBucketStateAndRunPersistTask(replicaVBs[1], vbucket_state_replica);

    // check all vbuckets are fully resident to start
    for (const auto vb : {0, 1, 2, 3}) {
        Vbid vbid(vb);
        ASSERT_EQ(100, getRRPercent(*store->getVBucket(vbid)))
                << vbid << " not fully resident before eviction";
    }

    // Run the item pager
    auto& lpNonioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
    // run the item pager. This creates the paging visitor
    runNextTask(lpNonioQ, "Paging out items.");

    runNextTask(lpNonioQ, "Item pager no vbucket assigned");

    // nothing left to do
    EXPECT_EQ(0, lpNonioQ.getReadyQueueSize());

    for (const auto vbid : activeVBs) {
        EXPECT_EQ(100, getRRPercent(*store->getVBucket(vbid)))
                << vbid << " not fully resident after eviction";
    }

    // Confirm the replica RR is lower that active RR
    for (const auto vbid : replicaVBs) {
        EXPECT_LT(getRRPercent(*store->getVBucket(vbid)), 100)
                << vbid << " has residency higher than expected";
    }
}

TEST_P(STItemPagerTest, ActiveEvictedIfReplicaEvictionInsufficient) {
    // MB-40531 test that the item pager _does_ evict from active vbuckets
    // if evicting from replicas can _not_ reclaim enough memory to reach the
    // low water mark (determined when the item pager first runs)

    if (ephemeral()) {
        // Ephemeral does not evict from replicas
        GTEST_SKIP();
    }
    if (hasMagma()) {
        // Magma's memory usage makes tests relying on memory usage maths
        // difficult, and probably quite fragile/sensitive to magma changes.
        // The behaviour being tested is _not_ dependent on the backend,
        // so testing with couchstore _should_ be sufficient.
        GTEST_SKIP();
    }

    auto activeVB = Vbid(0);
    auto replicaVB = Vbid(1);

    setVBucketStateAndRunPersistTask(activeVB, vbucket_state_active);

    // Set replicaVB as active initially (so we can populate it).
    setVBucketStateAndRunPersistTask(replicaVB, vbucket_state_active);

    // bump the quota up. We need total evictable data to comfortably exceed
    // the gap between high and low watermarks. With the default
    // quota, there's not enough "headroom" left after base memory usage to
    // populate vbs as desired.
    increaseQuota(20000000);

    auto& stats = engine->getEpStats();

    auto watermarkDiff = stats.mem_high_wat.load() - stats.mem_low_wat.load();

    // starting above the low water mark would mean that the replica pageable
    // memory will definitely not exceed the watermarkdiff when the high
    // watermark is reached.
    ASSERT_LT(stats.getPreciseTotalMemoryUsed(), stats.mem_low_wat.load());

    // store items in the replica vb such that evicting everything in the
    // replica when at the high watermark will _not_ reach the low watermark
    auto replicaItemCount = populateVbsUntil(
            {replicaVB}, [&stats, &store = store, replicaVB, watermarkDiff] {
                // sanity check - if this fails the quota is too low -
                EXPECT_LT(stats.getPreciseTotalMemoryUsed(),
                          stats.mem_high_wat.load());

                size_t pageableMem =
                        store->getVBucket(replicaVB)->getPageableMemUsage();

                return pageableMem > watermarkDiff * 0.9;
            });
    flushAndRemoveCheckpoints(replicaVB);

    setVBucketStateAndRunPersistTask(replicaVB, vbucket_state_replica);

    // We don't care exactly how many were stored, just that it is a
    // "large enough" number for percentages to be somewhat useful
    EXPECT_GT(replicaItemCount, 100);

    // Now fill the active VB until the high watermark is reached
    auto activeItemCount =
            populateVbsUntil({activeVB}, [&stats, &store = store] {
                return stats.getPreciseTotalMemoryUsed() >
                       stats.mem_high_wat.load();
            });

    flushAndRemoveCheckpoints(activeVB);

    // Flushing and removing checkpoints frees some memory, "top up" the
    // active vbuckets back to the high watermark.
    // Note, populateUntilTmpFail flushes each time an item is stored,
    // using this without the above pre-population would be quite slow in CV
    activeItemCount += populateUntilTmpFail(activeVB);

    EXPECT_GT(activeItemCount, 100);

    size_t activeRR = getRRPercent(*store->getVBucket(activeVB));
    size_t replicaRR = getRRPercent(*store->getVBucket(replicaVB));

    ASSERT_EQ(100, activeRR);
    ASSERT_EQ(100, replicaRR);

    auto& lpNonioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
    // run the item pager. This creates the paging visitor
    runNextTask(lpNonioQ, "Paging out items.");

    runNextTask(lpNonioQ, "Item pager no vbucket assigned");

    // nothing left to do
    EXPECT_EQ(0, lpNonioQ.getReadyQueueSize());

    activeRR = getRRPercent(*store->getVBucket(activeVB));
    replicaRR = getRRPercent(*store->getVBucket(replicaVB));

    EXPECT_LT(activeRR, 100);
    EXPECT_GT(activeRR, 60);

    EXPECT_LT(replicaRR, 5);
}

MATCHER_P(VBPtrVbidMatcher,
          vbid,
          "Check the provided VBucket pointer points to a vbucket with the "
          "given vbid") {
    return arg && arg->getId() == vbid;
}

TEST_P(STItemPagerTest, ItemPagerEvictionOrder) {
    // MB-40531: Test that the paging visitor visits vbuckets ordered by:
    // * replica vbuckets before active/pending
    // * highest pageableMemUsage first

    std::vector<Vbid> activeVBs = {Vbid(0), Vbid(1)};
    std::vector<Vbid> replicaVBs = {Vbid(2), Vbid(3)};
    std::vector<Vbid> allVBs = {Vbid(0), Vbid(1), Vbid(2), Vbid(3)};

    // Set all vbs, including replicaVBs, as active initially (so we can
    // populate them easily).
    for (const auto& vbid : allVBs) {
        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    }

    // populate the vbuckets with different numbers of equal sized items.
    // Pageable mem usage should be proportional to the number of items.
    // The visitor should therefore visit them in the commented order
    for (const auto& setup : std::vector<std::pair<Vbid, int>>{
                 {activeVBs[0], 30}, // 4th
                 {activeVBs[1], 40}, // 3rd
                 {replicaVBs[0], 20}, // 1st
                 {replicaVBs[1], 10}, // 2nd
         }) {
        Vbid vbid;
        int itemCount;
        std::tie(vbid, itemCount) = setup;
        for (int i = 0; i < itemCount; ++i) {
            auto key = makeStoredDocKey("key-" + std::to_string(i));
            auto item = make_item(vbid, key, std::string(100, 'x'));
            ASSERT_EQ(cb::engine_errc::success, storeItem(item));
        }
    }

    // flush all vbs
    for (const auto& vbid : allVBs) {
        flushDirectlyIfPersistent(Vbid(vbid));
    }

    // flip the replica vbs to the correct state
    setVBucketStateAndRunPersistTask(replicaVBs[0], vbucket_state_replica);
    setVBucketStateAndRunPersistTask(replicaVBs[1], vbucket_state_replica);

    auto& stats = engine->getEpStats();
    auto pagerSemaphore = std::make_shared<cb::Semaphore>();
    auto& config = engine->getConfiguration();

    auto pv = std::make_unique<MockPagingVisitor>(
            *store,
            stats,
            EvictionRatios{1.0 /*active*/, 1.0 /*replica*/},
            pagerSemaphore,
            ITEM_PAGER,
            false,
            VBucketFilter(ephemeral() ? activeVBs : allVBs),
            config.getItemEvictionAgePercentage(),
            config.getItemEvictionFreqCounterAgeThreshold());

    using namespace testing;
    InSequence s;

    // set up expectations _before_ moving the pv into the VBCBAdaptor
    if (!ephemeral()) {
        // ephemeral cannot evict from replicas, so the pager visitor won't
        // visit them at all.
        EXPECT_CALL(*pv, visitBucket(VBPtrVbidMatcher(replicaVBs[0])));
        EXPECT_CALL(*pv, visitBucket(VBPtrVbidMatcher(replicaVBs[1])));
    }
    EXPECT_CALL(*pv, visitBucket(VBPtrVbidMatcher(activeVBs[1])));
    EXPECT_CALL(*pv, visitBucket(VBPtrVbidMatcher(activeVBs[0])));

    auto label = "Item pager";
    auto taskid = TaskId::ItemPagerVisitor;
    VBCBAdaptor task(store,
                     taskid,
                     std::move(pv),
                     label,
                     /*shutdown*/ false);

    // call the run method repeatedly until it returns false, indicating the
    // visitor is definitely done, rather than paused
    while (task.run())
        ;
}

TEST_P(STItemPagerTest, MB43559_EvictionWithoutReplicasReachesLWM) {
    // MB-43559: Test that eviction does not stop once memory usage drops
    // below the high watermark if there are no replica vbuckets

    if (!persistent()) {
        // ephemeral buckets are never less than 100% resident and are not
        // vulnerable to the bug (see MB)
        return;
    }

    // The test makes assumptions on the memory state of the system at storing
    // items. In particular, we load some items until we hit the HWM for
    // preparing for HT ejection.
    // Now that we introduce a limit on checkpoint mem-usage (MB-47386), when
    // loading data we need to simulate checkpoint mem-recovery, or we would
    // never reach the HWM otherwise as we'll hit the checkpoint quota first.
    // All fine so far, under couchstore all behaves as expected.
    // Problem under magma is that it allocates a good chunk of the bucket quota
    // when we persist for allowing chekpoint-mem-recovery. We hit the HWM by
    // magma allocations that seem to be intermittently freed (maybe in magma
    // threads?) as soon as we return from the load-data function. Thus pushing
    // the system back below the HWM and invalidating the test pre-requirements.
    // At the time of writing magma is also facing some major internal memory
    // usage and tracking issues.
    // For all that, I'm disabling the test for magma for now.
    // @todo: fix and re-enable, tracked in MB-47481
    if (hasMagma()) {
        GTEST_SKIP();
    }

    // Magma's memory usage makes calculations around recovering memory
    // through eviction difficult. Increase the quota to allow eviction
    // to actually reach the low watermark despite magma's memory usage
    if (hasMagma()) {
        increaseQuota(2 * 1024 * 1024);
    }

    // issue involves eviction skipping vbuckets after mem_used < hwm,
    // so multiple vbuckets are needed
    std::vector<Vbid> vbids = {Vbid(0), Vbid(1), Vbid(2), Vbid(3)};

    // set vbs active
    for (const auto& vb : vbids) {
        setVBucketStateAndRunPersistTask(vb, vbucket_state_active);
    }

    auto& stats = engine->getEpStats();

    // check that baseline memory usage is definitely below the LWM
    ASSERT_LT(stats.getPreciseTotalMemoryUsed(), stats.mem_low_wat.load());

    auto aboveHWM = [&stats]() {
        return stats.getPreciseTotalMemoryUsed() > stats.mem_high_wat.load();
    };

    auto itemCount = 0;
    auto i = 0;
    // items need to be persisted to be evicted, but flushing after each item
    // would be slow. Load to the HWM, then flush, then repeat if we dropped
    // below the HWM
    while (!aboveHWM()) {
        // Note: the populate function persists and expels from checkpoints, so
        // here we'll hit the HWM with most of the allocations in the HT and all
        // items clean
        itemCount += populateVbsUntil(vbids,
                                      aboveHWM,
                                      "keys_" + std::to_string(i++) + "_",
                                      2048 /*valSize*/);
    }

    // confirm we are above the high watermark, and can test the item pager
    // behaviour
    ASSERT_GT(stats.getEstimatedTotalMemoryUsed(), stats.mem_high_wat.load());

    // Note: Any more precise expectation on item-count is just a guess, as
    // different storage have different mem-usage baselines, so the num of items
    // that we manage to store may vary considerably.
    EXPECT_GT(itemCount, 0);

    // Note: prior to the fix for this MB, eviction relied upon cached
    // residency ratios _which were only updated when gathering stats_.
    // This is no longer the case post-fix, but to ensure the test _would_
    // fail prior to the fix, this is done here.
    auto refreshCachedResidentRatios = [& store = store]() {
        // call getAggregatedVBucketStats to updated cached resident ratios (as
        // returned by `store->get*ResidentRatio()`)
        testing::NiceMock<MockStatCollector> collector;
        store->getAggregatedVBucketStats(collector.forBucket("foobar"));
    };
    refreshCachedResidentRatios();

    // there are no replica buckets
    ASSERT_EQ(0, store->getNumOfVBucketsInState(vbucket_state_replica));
    // the replica resident ratio defaults to 100 as there are no replica items
    ASSERT_EQ(100, store->getReplicaResidentRatio());

    // age is not relevant to this test, increase the mfu threshold under which
    // age is ignored
    auto& config = engine->getConfiguration();
    config.setItemEvictionFreqCounterAgeThreshold(255);

    // to make sure this isn't sensitive to small changes in memory usage,
    // try to evict _everything_ (eviction ratio 1.0). If mem_used still hasn't
    // gone below the lwm then something is definitely wrong.
    auto pagerSemaphore = std::make_shared<cb::Semaphore>();

    auto pv = std::make_unique<MockPagingVisitor>(
            *store,
            stats,
            EvictionRatios{1.0 /* active&pending */, 1.0 /* replica */},
            pagerSemaphore,
            ITEM_PAGER,
            false,
            VBucketFilter(vbids),
            config.getItemEvictionAgePercentage(),
            config.getItemEvictionFreqCounterAgeThreshold());

    auto label = "Item pager";
    auto taskid = TaskId::ItemPagerVisitor;
    VBCBAdaptor task(store,
                     taskid,
                     std::move(pv),
                     label,
                     /*shutdown*/ false);

    // call the run method repeatedly until it returns false, indicating the
    // visitor is definitely done, rather than paused
    while (task.run()) {
        // need to refresh the resident ratio so it reflects active RR < 100
        // after some items have been evicted. This wouldn't need to happen
        // _during_ eviction in a full repro, just _once_ ever after active RR
        // drops.
        refreshCachedResidentRatios();
    }

    // confirm that memory usage is now below the low watermark
    EXPECT_LT(stats.getPreciseTotalMemoryUsed(), stats.mem_low_wat.load());
}

/**
 * Test fixture for Ephemeral-only item pager tests.
 */
class STEphemeralItemPagerTest : public STItemPagerTest {
};

// For Ephemeral buckets, replica items should not be paged out (deleted) -
// as that would cause the replica to have a diverging history from the active.
TEST_P(STEphemeralItemPagerTest, ReplicaNotPaged) {
    const Vbid active_vb = Vbid(0);
    const Vbid replica_vb = Vbid(1);
    // Set vBucket 1 online, initially as active (so we can populate it).
    store->setVBucketState(replica_vb, vbucket_state_active);

    auto& stats = engine->getEpStats();
    ASSERT_LT(stats.getEstimatedTotalMemoryUsed(), stats.mem_low_wat.load())
            << "Expected to start below low watermark";

    // Populate vbid 0 (active) until we reach the low watermark.
    size_t active_count = 0;
    const std::string value(1024, 'x'); // 1KB value to use for documents.
    do {
        auto key = makeStoredDocKey("key_" + std::to_string(active_count));
        auto item = make_item(active_vb, key, value);
        // Set freqCount to 0 so will be a candidate for paging out straight
        // away.
        item.setFreqCounterValue(0);
        ASSERT_EQ(cb::engine_errc::success, storeItem(item));
        active_count++;
    } while (stats.getEstimatedTotalMemoryUsed() < stats.mem_low_wat.load());

    ASSERT_GE(active_count, 10)
            << "Expected at least 10 active items before hitting low watermark";

    // Populate vbid 1 (replica) until we reach the high watermark.
    size_t replica_count = populateUntilTmpFail(replica_vb);
    ASSERT_GE(replica_count, 10)
        << "Expected at least 10 replica items before hitting high watermark";

    // Flip vb 1 to be a replica (and hence should not be a candidate for
    // any paging out.
    store->setVBucketState(replica_vb, vbucket_state_replica);
    runHighMemoryPager();

    // Expected active vb behaviour depends on the full policy:
    if (std::get<1>(GetParam()) == "fail_new_data") {
        EXPECT_EQ(store->getVBucket(replica_vb)->getNumItems(), replica_count)
                << "Expected replica count to remain equal";
        EXPECT_EQ(store->getVBucket(active_vb)->getNumItems(), active_count)
                << "Active count should be the same after Item Pager "
                   "(fail_new_data)";
    } else {
        EXPECT_EQ(store->getVBucket(replica_vb)->getNumItems(), replica_count)
                << "Expected replica count to remain equal";
        EXPECT_LT(store->getVBucket(active_vb)->getNumItems(), active_count)
                << "Active count should have decreased after Item Pager";
    }
}

/**
 * Test fixture for expiry pager tests - enables the Expiry Pager (in addition
 * to what the parent class does).
 */
class STExpiryPagerTest : virtual public STBucketQuotaTest {
protected:
    void SetUp() override {
        STBucketQuotaTest::SetUp();

        // Setup expiry pager - this adds one to the number of nonIO tasks
        initializeExpiryPager();
        ++initialNonIoTasks;

        // Sanity check - should be no nonIO tasks ready to run, and initial
        // count in futureQ.
        auto& lpNonioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
        EXPECT_EQ(0, lpNonioQ.getReadyQueueSize());
        EXPECT_EQ(initialNonIoTasks, lpNonioQ.getFutureQueueSize());
    }

    void wakeUpExpiryPager() {
        store->wakeUpExpiryPager();
        // Expiry pager consists of two Tasks - the parent ExpiryPager task,
        // and then a per-vBucket task (via VCVBAdapter) - which there is
        // just one of as we only have one vBucket online.
        // Trigger expiry pager - note the main task just spawns individual
        // tasks per vBucket - we also need to execute one of them.
        auto& lpNonioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
        runNextTask(lpNonioQ, "Paging expired items.");
        EXPECT_EQ(0, lpNonioQ.getReadyQueueSize());
        EXPECT_EQ(initialNonIoTasks + 1, lpNonioQ.getFutureQueueSize());
        runNextTask(lpNonioQ, "Expired item remover no vbucket assigned");
        EXPECT_EQ(0, lpNonioQ.getReadyQueueSize());
        EXPECT_EQ(initialNonIoTasks, lpNonioQ.getFutureQueueSize());
    }

    void expiredItemsDeleted();
};

void STExpiryPagerTest::expiredItemsDeleted() {
    // Populate bucket with three documents - one with no expiry, one with an
    // expiry in 10 seconds, and one with an expiry in 20 seconds.
    std::string value = createXattrValue("body");
    for (size_t ii = 0; ii < 3; ii++) {
        auto key = makeStoredDocKey("key_" + std::to_string(ii));
        const uint32_t expiry =
                ii > 0 ? ep_abs_time(ep_current_time() + ii * 10) : 0;
        auto item = make_item(
                vbid,
                key,
                value,
                expiry,
                PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR);
        ASSERT_EQ(cb::engine_errc::success, storeItem(item));
    }

    flushDirectlyIfPersistent(vbid,
                              {MoreAvailable::No, 3, WakeCkptRemover::No});

    // Sanity check - should have not hit high watermark (otherwise the
    // item pager will run automatically and aggressively delete items).
    auto& stats = engine->getEpStats();
    EXPECT_LE(stats.getEstimatedTotalMemoryUsed(), stats.getMaxDataSize() * 0.8)
            << "Expected to not have exceeded 80% of bucket quota";

    // Move time forward by 11s, so key_1 should be expired.
    TimeTraveller tedTheodoreLogan(11);

    // Sanity check - should still have all items present in VBucket.
    ASSERT_EQ(3, engine->getVBucket(vbid)->getNumItems());

    wakeUpExpiryPager();
    flushDirectlyIfPersistent(vbid,
                              {MoreAvailable::No, 1, WakeCkptRemover::No});

    EXPECT_EQ(2, engine->getVBucket(vbid)->getNumItems())
        << "Should only have 2 items after running expiry pager";

    // Check our items.
    auto key_0 = makeStoredDocKey("key_0");
    auto getKeyFn = [this](const StoredDocKey& key) {
        return store->get(key, vbid, cookie, QUEUE_BG_FETCH).getStatus();
    };
    EXPECT_EQ(cb::engine_errc::success, getKeyFn(key_0))
            << "Key without TTL should still exist.";

    auto key_1 = makeStoredDocKey("key_1");

    if (::testing::get<1>(GetParam()) == "full_eviction") {
        // Need an extra get() to trigger EWOULDBLOCK / bgfetch.
        EXPECT_EQ(cb::engine_errc::would_block, getKeyFn(key_1));
        runBGFetcherTask();
    }
    EXPECT_EQ(cb::engine_errc::no_such_key, getKeyFn(key_1))
            << "Key with TTL:10 should be removed.";

    auto key_2 = makeStoredDocKey("key_2");
    EXPECT_EQ(cb::engine_errc::success, getKeyFn(key_2))
            << "Key with TTL:20 should still exist.";

    // Move time forward by +10s, so key_2 should also be expired.
    TimeTraveller philConners(10);

    // Sanity check - should still have 2 items present in VBucket.
    ASSERT_EQ(2, engine->getVBucket(vbid)->getNumItems())
        << "Should still have 2 items after time-travelling";

    wakeUpExpiryPager();
    flushDirectlyIfPersistent(vbid,
                              {MoreAvailable::No, 1, WakeCkptRemover::No});

    // Should only be 1 item remaining.
    EXPECT_EQ(1, engine->getVBucket(vbid)->getNumItems());

    // Check our items.
    EXPECT_EQ(cb::engine_errc::success, getKeyFn(key_0))
            << "Key without TTL should still exist.";

    EXPECT_EQ(cb::engine_errc::no_such_key, getKeyFn(key_1))
            << "Key with TTL:10 should be removed.";

    if (::testing::get<1>(GetParam()) == "full_eviction") {
        // Need an extra get() to trigger EWOULDBLOCK / bgfetch.
        EXPECT_EQ(cb::engine_errc::would_block, getKeyFn(key_2));
        runBGFetcherTask();
    }
    EXPECT_EQ(cb::engine_errc::no_such_key, getKeyFn(key_2))
            << "Key with TTL:20 should be removed.";
}

// Test that when the expiry pager runs, all expired items are deleted.
TEST_P(STExpiryPagerTest, ExpiredItemsDeleted) {
    expiredItemsDeleted();
}

// Test that when an expired system-xattr document is fetched with getMeta
// it can be successfully expired again
TEST_P(STExpiryPagerTest, MB_25650) {
    expiredItemsDeleted();

    auto vb = store->getVBucket(Vbid(0));

    auto key_1 = makeStoredDocKey("key_1");
    ItemMetaData metadata;
    uint32_t deleted;
    uint8_t datatype;

    // Ephemeral doesn't bgfetch, persistent full-eviction already had to
    // perform a bgfetch to check key_1 no longer exists in the above
    // expiredItemsDeleted().
    const cb::engine_errc err =
            persistent() && std::get<1>(GetParam()) == "value_only"
                    ? cb::engine_errc::would_block
                    : cb::engine_errc::success;

    // Bring document meta back into memory and run expiry on it
    EXPECT_EQ(err,
              store->getMetaData(
                      key_1, vbid, cookie, metadata, deleted, datatype));
    if (persistent()) {
        runBGFetcherTask();
        EXPECT_EQ(cb::engine_errc::success,
                  store->getMetaData(
                          key_1, vbid, cookie, metadata, deleted, datatype));
    }

    // Original bug is that we would segfault running the pager here
    wakeUpExpiryPager();

    auto options =
            static_cast<get_options_t>(QUEUE_BG_FETCH | GET_DELETED_VALUE);
    EXPECT_EQ(err, store->get(key_1, vbid, cookie, options).getStatus())
            << "Key with TTL:10 should be removed.";

    // Verify that the xattr body still exists.
    if (persistent()) {
        runBGFetcherTask();
    }
    auto item = store->get(key_1, vbid, cookie, GET_DELETED_VALUE);

    ASSERT_EQ(cb::engine_errc::success, item.getStatus());
    EXPECT_TRUE(mcbp::datatype::is_xattr(item.item->getDataType()));
    ASSERT_NE(0, item.item->getNBytes());
    cb::xattr::Blob blob(
            {const_cast<char*>(item.item->getData()), item.item->getNBytes()},
            false);

    EXPECT_EQ(0, blob.get("user").size());
    EXPECT_EQ(0, blob.get("meta").size());
    ASSERT_NE(0, blob.get("_sync").size());
    EXPECT_STREQ("{\"cas\":\"0xdeadbeefcafefeed\"}",
                 reinterpret_cast<char*>(blob.get("_sync").data()));
}

// Test that when an expired system-xattr document is fetched with getMeta
// deleteWithMeta can be successfully invoked
TEST_P(STExpiryPagerTest, MB_25671) {
    expiredItemsDeleted();
    auto vb = store->getVBucket(vbid);

    // key_1 has been expired
    auto key_1 = makeStoredDocKey("key_1");
    ItemMetaData metadata;
    uint32_t deleted = 0;
    uint8_t datatype = 0;

    // Ephemeral doesn't bgfetch, persistent full-eviction already had to
    // perform a bgfetch to check key_1 no longer exists in the above
    // expiredItemsDeleted().
    const cb::engine_errc err =
            persistent() && std::get<1>(GetParam()) == "value_only"
                    ? cb::engine_errc::would_block
                    : cb::engine_errc::success;

    // Bring the deleted key back with a getMeta call
    EXPECT_EQ(err,
              store->getMetaData(
                      key_1, vbid, cookie, metadata, deleted, datatype));
    if (persistent()) {
        runBGFetcherTask();
        EXPECT_EQ(cb::engine_errc::success,
                  store->getMetaData(
                          key_1, vbid, cookie, metadata, deleted, datatype));
    }

    uint64_t cas = -1;
    metadata.flags = 0xf00f0088;
    metadata.cas = 0xbeeff00dcafe1234ull;
    metadata.revSeqno = 0xdad;
    metadata.exptime = 0xfeedface;
    PermittedVBStates vbstates(vbucket_state_active);
    auto deleteWithMeta =
            [this, key_1, &cas, vbstates, metadata]() -> cb::engine_errc {
        return store->deleteWithMeta(key_1,
                                     cas,
                                     nullptr,
                                     vbid,
                                     cookie,
                                     vbstates,
                                     CheckConflicts::No,
                                     metadata,
                                     GenerateBySeqno::No,
                                     GenerateCas::No,
                                     0,
                                     nullptr,
                                     DeleteSource::Explicit);
    };
    // Prior to the MB fix - this would crash.
    EXPECT_EQ(err, deleteWithMeta());

    auto options =
            static_cast<get_options_t>(QUEUE_BG_FETCH | GET_DELETED_VALUE);
    if (persistent()) {
        runBGFetcherTask();
        EXPECT_EQ(cb::engine_errc::success, deleteWithMeta());
    }

    auto item = store->get(key_1, vbid, cookie, options);
    ASSERT_EQ(cb::engine_errc::success, item.getStatus());
    EXPECT_TRUE(item.item->isDeleted()) << "Not deleted " << *item.item;
    ASSERT_NE(0, item.item->getNBytes()) << "No value " << *item.item;

    cb::xattr::Blob blob(
            {const_cast<char*>(item.item->getData()), item.item->getNBytes()},
            false);

    EXPECT_EQ(0, blob.get("user").size());
    EXPECT_EQ(0, blob.get("meta").size());
    ASSERT_NE(0, blob.get("_sync").size());
    EXPECT_STREQ("{\"cas\":\"0xdeadbeefcafefeed\"}",
                 reinterpret_cast<char*>(blob.get("_sync").data()));
    EXPECT_EQ(metadata.flags, item.item->getFlags());
    EXPECT_EQ(metadata.exptime, item.item->getExptime());
    EXPECT_EQ(metadata.cas, item.item->getCas());
    EXPECT_EQ(metadata.revSeqno, item.item->getRevSeqno());
}

TEST_P(STItemPagerTest, ItemPagerEvictionOrderIsSafe) {
    // MB-42688: Test that the ordering of the paging visitor comparator is
    // fixed even if the amount of pageable memory or the vbucket state changes.
    // This is required to meet the strict weak ordering requirement of
    // std::sort

    // 17 is the minimum number of vbs found to demonstrate a segfault
    // with an unsuitable comparator on linux. This is likely impl dependent.
    // 18 gives 6 vbs per active/replica/pending
    if (hasMagma()) {
        // Magma does not appreciate having max_vbuckets updated
        // In relation to this test, magma should not behave differently to
        // couchstore buckets anyway
        GTEST_SKIP();
    }
    resetEngineAndWarmup("max_vbuckets=18");
    std::vector<Vbid> allVBs(18);

    for (int i = 0; i < 18; i++) {
        allVBs[i] = Vbid(i);
    }

    for (int i = 0; i < 6; i++) {
        setVBucketStateAndRunPersistTask(Vbid(i), vbucket_state_active);
        setVBucketStateAndRunPersistTask(Vbid(i + 6), vbucket_state_pending);
        setVBucketStateAndRunPersistTask(Vbid(i + 12), vbucket_state_replica);
    }

    if (engine->getConfiguration().getBucketType() == "persistent") {
        // flush all vbs
        for (const auto& vbid : allVBs) {
            dynamic_cast<EPBucket&>(*store).flushVBucket(vbid);
        }
    }

    auto& stats = engine->getEpStats();
    auto pagerSemaphore = std::make_shared<cb::Semaphore>();
    auto& config = engine->getConfiguration();

    auto pv = std::make_unique<MockPagingVisitor>(
            *store,
            stats,
            EvictionRatios{1.0 /*active*/, 1.0 /*replica*/},
            pagerSemaphore,
            ITEM_PAGER,
            false,
            VBucketFilter(allVBs),
            config.getItemEvictionAgePercentage(),
            config.getItemEvictionFreqCounterAgeThreshold());

    // now test that even with state changes, the comparator sorts the vbuckets
    // acceptably
    auto innerComparator = pv->getVBucketComparator();

    // wrap the comparator to allow insertion of state changes mid-sort
    auto comparator = [&innerComparator, this](const Vbid& a, const Vbid& b) {
        // run the actual comparator
        auto res = innerComparator(a, b);
        // now, to _intentionally_ try to break the strict weak ordering, change
        // the state of one of the vbuckets. If the vbucket comparator is
        // checking the state each time, this can cause a crash in std::sort
        for (const auto& vbid : {a, b}) {
            auto state = store->getVBucket(vbid)->getState();
            EXPECT_EQ(cb::engine_errc::success,
                      store->setVBucketState(vbid,
                                             state == vbucket_state_replica
                                                     ? vbucket_state_active
                                                     : vbucket_state_replica));
        }

        return res;
    };

    // if the vbucket comparator is "safe" and checks the state once, this
    // sort will work as expected - otherwise it _may_ segfault.
    // Note: this is not a robust test, variations in the impl of sort
    // may mean this does _not_ crash even with an unacceptable comparator
    std::sort(allVBs.begin(), allVBs.end(), comparator);

    // as a secondary check, directly test the requirements of a strict
    // weak ordering on the comparator while actively changing
    // the state of vbuckets. This will fail if the comparator checks
    // the bucket state on every call.

    // irreflexivity
    for (const auto& x : allVBs) {
        EXPECT_FALSE(comparator(x, x));
    }

    // asymmetry
    for (const auto& x : allVBs) {
        for (const auto& y : allVBs) {
            EXPECT_FALSE(comparator(x, y) && comparator(y, x));
        }
    }

    // transitivity
    for (const auto& x : allVBs) {
        for (const auto& y : allVBs) {
            for (const auto& z : allVBs) {
                // x < y && y < z => x < z
                // equivalent to
                // !(x < y && y < z) || x < z
                // if x < y and y < z, it must be true that x < z
                EXPECT_TRUE(!(comparator(x, y) && comparator(y, z)) ||
                            comparator(x, z));
            }
        }
    }
}

TEST_P(STItemPagerTest, MB43055_MemUsedDropDoesNotBreakEviction) {
    // MB-43055: Test that having current memory usage drop below the low
    // watermark before the item pager runs does not prevent future item
    // pager runs.

    if (std::get<1>(GetParam()) == "fail_new_data") {
        // items are not auto-deleted, so the ItemPager does not run.
        return;
    }

    // Need a slightly higher quota here
    increaseQuota(800 * 1024);

    // Magma has been failing this test consistently after more memory was
    // allocated to histograms so bump the quota accordingly. If others start
    // to fail then we should consider moving this to affect all magma tests.
    if (hasMagma()) {
        increaseQuota(1024 * 1024);
    }

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // this triggers eviction, scheduling the ItemPager task
    auto itemCount = populateUntilAboveHighWaterMark(vbid);
    EXPECT_LT(0, itemCount);

    // now delete some items to lower memory usage
    for (int i = 0; i < itemCount; i++) {
        auto key = makeStoredDocKey("key_" + std::to_string(i));
        uint64_t cas = 0;
        mutation_descr_t mutation_descr;
        EXPECT_EQ(cb::engine_errc::success,
                  store->deleteItem(key,
                                    cas,
                                    vbid,
                                    cookie,
                                    {},
                                    /*itemMeta*/ nullptr,
                                    mutation_descr));
    }

    // Deleting items doesn't free much memory and we'd need a big quota to be
    // able to reclaim the difference between low and high watermarks just by
    // deleting items. We can also flush persistent VBuckets and reduce the
    // checkpoint memory usage by freeing closed checkpoints. Doing this allows
    // us to use a significantly lower quota which reduces test time
    // significantly
    auto vb = store->getVBucket(vbid);
    vb->checkpointManager->createNewCheckpoint(true);
    flushVBucketToDiskIfPersistent(vbid, itemCount);
    vb->checkpointManager->removeClosedUnrefCheckpoints();

    auto& stats = engine->getEpStats();
    // confirm we are now below the low watermark, and can test the item pager
    // behaviour
    ASSERT_LT(stats.getEstimatedTotalMemoryUsed(), stats.mem_low_wat.load());

    auto& lpNonioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
    // run the item pager. It should _not_ create and schedule a PagingVisitor
    runNextTask(lpNonioQ, "Paging out items.");

    EXPECT_EQ(0, lpNonioQ.getReadyQueueSize());
    EXPECT_EQ(initialNonIoTasks, lpNonioQ.getFutureQueueSize());

    // populate again, and confirm this time that the item pager does shedule
    // a paging visitor
    populateUntilAboveHighWaterMark(vbid);

    runNextTask(lpNonioQ, "Paging out items.");
    runNextTask(lpNonioQ, "Item pager no vbucket assigned");
}

/**
 * Subclass for expiry tests only applicable to Value eviction persistent
 * buckets.
 */
class STValueEvictionExpiryPagerTest : public STExpiryPagerTest {
public:
    static auto configValues() {
        return ::testing::Values(
#ifdef EP_USE_MAGMA
                std::make_tuple("persistent_magma"s, "value_only"s),
#endif
                std::make_tuple("persistent_couchstore"s, "value_only"s));
    }
};

// Test that when a xattr value is ejected, we can still expire it. Previous
// to the fix we crash because the item has no value in memory.
//
// (Not applicable to full-eviction as relies on in-memory expiry pager
//  expiring a non-resident item; with full-eviction the item is completely
//  evicted and hence ExpiryPager won't find it.)
TEST_P(STValueEvictionExpiryPagerTest, MB_25931) {
    std::string value = createXattrValue("body");
    auto key = makeStoredDocKey("key_1");
    auto item = make_item(
            vbid,
            key,
            value,
            ep_abs_time(ep_current_time() + 10),
            PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR);
    ASSERT_EQ(cb::engine_errc::success, storeItem(item));

    flushDirectlyIfPersistent(vbid,
                              {MoreAvailable::No, 1, WakeCkptRemover::No});

    const char* msg;
    EXPECT_EQ(cb::mcbp::Status::Success, store->evictKey(key, vbid, &msg));
    EXPECT_STREQ("Ejected.", msg);

    runBGFetcherTask();

    TimeTraveller docBrown(15);

    wakeUpExpiryPager();
    flushDirectlyIfPersistent(vbid,
                              {MoreAvailable::No, 1, WakeCkptRemover::No});
}

// Test that expiring a non-resident item works (and item counts are correct).
//
// (Not applicable to full-eviction as relies on in-memory expiry pager
//  expiring a non-resident item; with full-eviction the item is completely
//  evicted and hence ExpiryPager won't find it.)
TEST_P(STValueEvictionExpiryPagerTest, MB_25991_ExpiryNonResident) {
    // Populate bucket with a TTL'd document, and then evict that document.
    auto key = makeStoredDocKey("key");
    auto expiry = ep_abs_time(ep_current_time() + 5);
    auto item = make_item(vbid, key, "value", expiry);
    ASSERT_EQ(cb::engine_errc::success, storeItem(item));

    flushDirectlyIfPersistent(vbid,
                              {MoreAvailable::No, 1, WakeCkptRemover::No});

    // Sanity check - should have not hit high watermark (otherwise the
    // item pager will run automatically and aggressively delete items).
    auto& stats = engine->getEpStats();
    EXPECT_LE(stats.getEstimatedTotalMemoryUsed(), stats.getMaxDataSize() * 0.8)
            << "Expected to not have exceeded 80% of bucket quota";

    // Evict key so it is no longer resident.
    evict_key(vbid, key);

    // Move time forward by 11s, so key should be expired.
    TimeTraveller tedTheodoreLogan(11);

    // Sanity check - should still have item present (and non-resident)
    // in VBucket.
    ASSERT_EQ(1, engine->getVBucket(vbid)->getNumItems());
    ASSERT_EQ(1, engine->getVBucket(vbid)->getNumNonResidentItems());

    wakeUpExpiryPager();
    flushDirectlyIfPersistent(vbid,
                              {MoreAvailable::No, 1, WakeCkptRemover::No});

    EXPECT_EQ(0, engine->getVBucket(vbid)->getNumItems())
            << "Should have 0 items after running expiry pager";
    EXPECT_EQ(0, engine->getVBucket(vbid)->getNumNonResidentItems())
            << "Should have 0 non-resident items after running expiry pager";

    // Check our item - should not exist.
    auto result = store->get(key, vbid, cookie, get_options_t());
    EXPECT_EQ(cb::engine_errc::no_such_key, result.getStatus());
}

class MB_32669 : public STValueEvictionExpiryPagerTest {
public:
    void SetUp() override {
        if (!config_string.empty()) {
            config_string += ";";
        }
        config_string += "compression_mode=active";
        STValueEvictionExpiryPagerTest::SetUp();
        store->enableItemCompressor();
        initialNonIoTasks++;
    }

    void runItemCompressor() {
        auto& lpNonioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
        runNextTask(lpNonioQ, "Item Compressor");
    }
};

// Test that an xattr value which is compressed, evicted and then expired
// doesn't trigger an exception
TEST_P(MB_32669, expire_a_compressed_and_evicted_xattr_document) {
    // 1) Add bucket a TTL'd xattr document
    auto key = makeStoredDocKey("key");
    auto expiry = ep_abs_time(ep_current_time() + 5);
    auto value = createXattrValue(std::string(100, 'a'), true /*sys xattrs*/);
    auto item =
            make_item(vbid, key, value, expiry, PROTOCOL_BINARY_DATATYPE_XATTR);
    ASSERT_EQ(cb::engine_errc::success, storeItem(item));

    flushDirectlyIfPersistent(vbid,
                              {MoreAvailable::No, 1, WakeCkptRemover::No});

    // Sanity check - should have not hit high watermark (otherwise the
    // item pager will run automatically and aggressively delete items).
    auto& stats = engine->getEpStats();
    ASSERT_LE(stats.getEstimatedTotalMemoryUsed(), stats.getMaxDataSize() * 0.8)
            << "Expected to not have exceeded 80% of bucket quota";

    // 2) Run the compressor
    runItemCompressor();

    // 2.1) And validate the document is now snappy
    ItemMetaData metadata;
    uint32_t deleted;
    uint8_t datatype;

    EXPECT_EQ(
            cb::engine_errc::success,
            store->getMetaData(key, vbid, cookie, metadata, deleted, datatype));
    ASSERT_EQ(PROTOCOL_BINARY_DATATYPE_SNAPPY,
              datatype & PROTOCOL_BINARY_DATATYPE_SNAPPY);

    // 3) Evict key so it is no longer resident.
    evict_key(vbid, key);

    // 4) Move time forward by 11s, so key should be expired.
    TimeTraveller wyldStallyns(11);

    // Sanity check - should still have item present (and non-resident)
    // in VBucket.
    ASSERT_EQ(1, engine->getVBucket(vbid)->getNumItems());
    ASSERT_EQ(1, engine->getVBucket(vbid)->getNumNonResidentItems());

    wakeUpExpiryPager();

    flushDirectlyIfPersistent(vbid,
                              {MoreAvailable::No, 1, WakeCkptRemover::No});

    EXPECT_EQ(0, engine->getVBucket(vbid)->getNumItems())
            << "Should have 0 items after running expiry pager";
    EXPECT_EQ(0, engine->getVBucket(vbid)->getNumNonResidentItems())
            << "Should have 0 non-resident items after running expiry pager";

    // Check our item has been deleted and the xattrs pruned
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS | GET_DELETED_VALUE);
    GetValue gv = store->get(key, vbid, cookie, options);
    EXPECT_EQ(cb::engine_errc::would_block, gv.getStatus());

    runBGFetcherTask();
    gv = store->get(key, vbid, cookie, options);
    ASSERT_EQ(cb::engine_errc::success, gv.getStatus());

    EXPECT_TRUE(gv.item->isDeleted());
    auto get_itm = gv.item.get();
    auto get_data = const_cast<char*>(get_itm->getData());

    cb::char_buffer value_buf{get_data, get_itm->getNBytes()};
    cb::xattr::Blob new_blob(value_buf, false);

    // expect sys attributes to remain
    const std::string& cas_str{R"({"cas":"0xdeadbeefcafefeed"})"};
    const std::string& sync_str = to_string(new_blob.get("_sync"));

    EXPECT_EQ(cas_str, sync_str) << "Unexpected system xattrs";
    EXPECT_TRUE(new_blob.get("user").empty())
            << "The user attribute should be gone";
    EXPECT_TRUE(new_blob.get("meta").empty())
            << "The meta attribute should be gone";
}

class MB_36087 : public STParameterizedBucketTest {
public:
};

// Test for MB-36087 - simply check that an evicted xattr item doesn't crash
// when a winning del-with-meta arrives.
TEST_P(MB_36087, DelWithMeta_EvictedKey) {
    store->setVBucketState(vbid, vbucket_state_active);
    if (!persistent()) {
        return;
    }
    ASSERT_TRUE(persistent());
    std::string value = createXattrValue("body");
    auto key = makeStoredDocKey("k1");
    auto item = make_item(
            vbid,
            key,
            value,
            0,
            PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR);
    uint64_t cas = 0;
    ASSERT_EQ(
            cb::engine_errc::success,
            engine->storeInner(cookie, item, cas, StoreSemantics::Set, false));

    auto& bucket = dynamic_cast<EPBucket&>(*store);
    EXPECT_EQ(1, bucket.flushVBucket(vbid).numFlushed);

    // 1) Store k1
    auto vb = store->getVBucket(vbid);

    // 2) Evict k1
    evict_key(vbid, key);

    // 3) A winning delWithMeta - system must bgFetch and not crash...
    ItemMetaData metadata;

    cas = -1;
    metadata.flags = 0xf00f0088;
    metadata.cas = 0xbeeff00dcafe1234ull;
    metadata.revSeqno = 0xdad;
    metadata.exptime = 0xfeedface;
    PermittedVBStates vbstates(vbucket_state_active);

    auto deleteWithMeta =
            [this, key, &cas, vbstates, metadata]() -> cb::engine_errc {
        return store->deleteWithMeta(key,
                                     cas,
                                     nullptr,
                                     vbid,
                                     cookie,
                                     vbstates,
                                     CheckConflicts::Yes,
                                     metadata,
                                     GenerateBySeqno::Yes,
                                     GenerateCas::No,
                                     0,
                                     nullptr,
                                     DeleteSource::Explicit);
    };
    // A bgfetch is required for full or value eviction because we need the
    // xattr value
    EXPECT_EQ(cb::engine_errc::would_block, deleteWithMeta());
    runBGFetcherTask();

    // Full eviction first did a meta-fetch, now has todo a full fetch
    auto err = std::get<1>(GetParam()) == "full_eviction"
                       ? cb::engine_errc::would_block
                       : cb::engine_errc::success;
    EXPECT_EQ(err, deleteWithMeta());

    if (std::get<1>(GetParam()) == "full_eviction") {
        runBGFetcherTask();
        EXPECT_EQ(cb::engine_errc::success, deleteWithMeta());
    }

    auto options =
            static_cast<get_options_t>(QUEUE_BG_FETCH | GET_DELETED_VALUE);
    auto gv = store->get(key, vbid, cookie, options);
    ASSERT_EQ(cb::engine_errc::success, gv.getStatus());
    EXPECT_TRUE(gv.item->isDeleted()) << "Not deleted " << *gv.item;
    ASSERT_NE(0, gv.item->getNBytes()) << "No value " << *gv.item;

    cb::xattr::Blob blob(
            {const_cast<char*>(gv.item->getData()), gv.item->getNBytes()},
            false);

    EXPECT_EQ(0, blob.get("user").size());
    EXPECT_EQ(0, blob.get("meta").size());
    ASSERT_NE(0, blob.get("_sync").size());
    EXPECT_STREQ("{\"cas\":\"0xdeadbeefcafefeed\"}",
                 reinterpret_cast<char*>(blob.get("_sync").data()));
    EXPECT_EQ(metadata.flags, gv.item->getFlags());
    EXPECT_EQ(metadata.exptime, gv.item->getExptime());
    EXPECT_EQ(metadata.cas, gv.item->getCas());
    EXPECT_EQ(metadata.revSeqno, gv.item->getRevSeqno());
}

/**
 * Fixture similar to STItemPagerTest, but with 4 paging visitors configured.
 */
class MultiPagingVisitorTest : public STItemPagerTest {
protected:
    void SetUp() override {
        setNumConcurrentPagers(4);
        STItemPagerTest::SetUp();
    }
};

/**
 * Test that the ItemPager correctly creates multiple PagingVisitors
 * as configured.
 */
TEST_P(MultiPagingVisitorTest, ItemPagerCreatesMultiplePagers) {
    if (!persistent()) {
        GTEST_SKIP();
    }
    std::vector<Vbid> vbids{Vbid(0), Vbid(1), Vbid(2), Vbid(3)};

    for (const auto& vbid : vbids) {
        store->setVBucketState(vbid, vbucket_state_active);
    }

    auto& stats = engine->getEpStats();
    // populate until above hwm
    auto aboveHWM = [&stats]() {
        return stats.getPreciseTotalMemoryUsed() > stats.mem_high_wat.load();
    };

    auto itemCount = 0;
    auto i = 0;
    // items need to be persisted to be evicted, but flushing after each item
    // would be slow. Load to the HWM, then flush, then repeat if we dropped
    // below the HWM
    while (!aboveHWM()) {
        itemCount += populateVbsUntil(
                vbids, aboveHWM, "keys_" + std::to_string(i++) + "_", 500);
        for (const auto& vb : vbids) {
            // flush and remove checkpoints as eviction will not touch
            // dirty items
            flushAndRemoveCheckpoints(vb);
        }
    }

    // make sure the item pager has been notified while we're above the HWM
    store->attemptToFreeMemory();

    auto& lpNonioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
    ASSERT_EQ(0, lpNonioQ.getReadyQueueSize());
    ASSERT_EQ(initialNonIoTasks, lpNonioQ.getFutureQueueSize());

    // Run the parent ItemPager task, and then N PagingVisitor tasks
    runNextTask(lpNonioQ, "Paging out items.");
    ASSERT_EQ(0, lpNonioQ.getReadyQueueSize());

    auto numConcurrentPagers = engine->getConfiguration().getConcurrentPagers();
    ASSERT_EQ(initialNonIoTasks + numConcurrentPagers,
              lpNonioQ.getFutureQueueSize());

    for (size_t i = 0; i < numConcurrentPagers; ++i) {
        runNextTask(lpNonioQ, "Item pager no vbucket assigned");
    }
}

// Confirm that splitting a filter into several disjoint filters works as
// expected. Used when creating multiple PagingVisitors
TEST(VbucketFilterTest, Split) {
    const VBucketFilter filter(
            std::vector<Vbid>{Vbid(0), Vbid(1), Vbid(2), Vbid(3)});

    using namespace testing;
    {
        SCOPED_TRACE("Identity");
        auto filters = filter.split(1);
        EXPECT_THAT(filters, SizeIs(1));
        EXPECT_TRUE(filter == filters.at(0));
    }

    {
        SCOPED_TRACE("Split N");
        // Expected: {0}, {1}, {2}, {3}
        auto filters = filter.split(4);
        EXPECT_THAT(filters, SizeIs(4));
        for (int i = 0; i < 4; ++i) {
            EXPECT_THAT(filters.at(i), SizeIs(1));
            EXPECT_TRUE(filters.at(i)(Vbid(i)));
        }
    }

    {
        SCOPED_TRACE("Split >N");
        // Expected: {0}, {1}, {2}, {3}, {}
        auto filters = filter.split(5);
        EXPECT_THAT(filters, SizeIs(5));
        for (int i = 0; i < 4; ++i) {
            EXPECT_THAT(filters.at(i), SizeIs(1));
            EXPECT_TRUE(filters.at(i)(Vbid(i)));
        }
        // last filter is empty
        EXPECT_THAT(filters.at(4), SizeIs(0));
    }

    {
        SCOPED_TRACE("Split <N");
        // Expected: {0, 3}, {1}, {2}
        auto filters = filter.split(3);
        EXPECT_THAT(filters, SizeIs(3));
        // round robin means first filter has more items

        EXPECT_THAT(filters.at(0), SizeIs(2));
        EXPECT_TRUE(filters.at(0)(Vbid(0)));
        EXPECT_TRUE(filters.at(0)(Vbid(3)));

        for (int i = 1; i < 3; ++i) {
            EXPECT_THAT(filters.at(i), SizeIs(1));
            EXPECT_TRUE(filters.at(i)(Vbid(i)));
        }
    }
}

/**
 * Test fixture for full eviction buckets with bloom filter disabled. Inherits
 * from both STExpiryPagerTest and STItemPagerTest so that we can re-use the
 * code that wrangles the ItemPager in both modes.
 */
class STFullEvictionNoBloomFilterPagerTest : virtual public STExpiryPagerTest,
                                             virtual public STItemPagerTest {
public:
    void SetUp() override {
        if (!config_string.empty()) {
            config_string += ";";
        }
        config_string += "bfilter_enabled=false";
        config_string += ";concurrent_pagers=" +
                         std::to_string(getNumConcurrentPagers());

        STExpiryPagerTest::SetUp();

        // Manually create item pager as in STItemPagerTest::SetUp()
        scheduleItemPager();
        ++initialNonIoTasks;
        itemPagerScheduled = true;
    }

    void testTempItemEvictableButNotExpirable(StoredDocKey key);
};

void STFullEvictionNoBloomFilterPagerTest::testTempItemEvictableButNotExpirable(
        StoredDocKey key) {
    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);

    // Attempt to expire the fetched (temp non-existent) item. Time travel 1
    // second to ensure that the item is expired (by time)
    TimeTraveller tt(1);
    wakeUpExpiryPager();

    auto& stats = engine->getEpStats();
    ASSERT_EQ(1, stats.expiryPagerRuns);
    EXPECT_EQ(0, stats.expired_pager);
    EXPECT_EQ(0, vb->numExpiredItems);

    {
        auto htRes = vb->ht.findForUpdate(key);
        ASSERT_TRUE(htRes.committed);

        // Poke the freq counter so that this item will definitely get evicted
        htRes.committed->setFreqCounterValue(0);

        EXPECT_FALSE(htRes.pending);
    }

    // Run item pager to test that we can actually get rid of this item.
    // We're going to set this vBucket to replica and write ourselves over the
    // HWM against some other vBucket to ensure that this vBucket is visited
    // first by the PagingVisitor to ensure that we try to evict our item.
    store->setVBucketState(vbid, vbucket_state_replica);

    auto dummyVb = Vbid(1);
    store->setVBucketState(dummyVb, vbucket_state_active);
    populateUntilAboveHighWaterMark(dummyVb);
    runHighMemoryPager();

    {
        // Item is gone, all is good
        auto htRes = vb->ht.findForUpdate(key);
        EXPECT_FALSE(htRes.committed);
        EXPECT_FALSE(htRes.pending);
    }
}

TEST_P(STFullEvictionNoBloomFilterPagerTest, TempNonResidentNotExpired) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Store the item to trigger a metadata bg fetch, running it will create a
    // TempNonExistent item
    auto key = makeStoredDocKey("key");
    auto item = make_item(vbid, key, "v1");
    EXPECT_EQ(cb::engine_errc::would_block, store->add(item, cookie));
    runBGFetcherTask();

    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);

    // Verify that the item is TempNonExistent
    {
        auto htRes = vb->ht.findForUpdate(key);
        ASSERT_TRUE(htRes.committed);
        ASSERT_TRUE(htRes.committed->isTempNonExistentItem());

        EXPECT_FALSE(htRes.pending);
    }

    // And proceed with the test
    testTempItemEvictableButNotExpirable(key);
}

TEST_P(STFullEvictionNoBloomFilterPagerTest, TempDeletedNotExpired) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Store a deleted item so that an add driven bg fetch will restore
    // deleted meta (setting the previously TempInitial item to TempDeleted)
    auto key = makeStoredDocKey("key");
    auto item = makeDeletedItem(key);
    EXPECT_EQ(cb::engine_errc::success, store->set(*item, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);

    // Add to drive a bg fetch
    auto itemToAdd = make_item(vbid, key, "v1");
    EXPECT_EQ(cb::engine_errc::would_block, store->add(itemToAdd, cookie));
    runBGFetcherTask();

    // Verify that the item is TempDeleted
    {
        auto htRes = vb->ht.findForUpdate(key);
        ASSERT_TRUE(htRes.committed);
        ASSERT_TRUE(htRes.committed->isTempDeletedItem());

        EXPECT_FALSE(htRes.pending);
    }

    // And proceed with the test
    testTempItemEvictableButNotExpirable(key);
}

// TODO: Ideally all of these tests should run with or without jemalloc,
// however we currently rely on jemalloc for accurate memory tracking; and
// hence it is required currently.
#if defined(HAVE_JEMALLOC)

INSTANTIATE_TEST_SUITE_P(EphemeralOrPersistent,
                         STItemPagerTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(EphemeralOrPersistent,
                         MultiPagingVisitorTest,
                         STParameterizedBucketTest::persistentConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(EphemeralOrPersistent,
                         STExpiryPagerTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(
        PersistentFullEviciton,
        STFullEvictionNoBloomFilterPagerTest,
        STParameterizedBucketTest::fullEvictionAllBackendsConfigValues(),
        STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(ValueOnly,
                         STValueEvictionExpiryPagerTest,
                         STValueEvictionExpiryPagerTest::configValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(Persistent,
                         MB_32669,
                         STValueEvictionExpiryPagerTest::configValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(Ephemeral,
                         STEphemeralItemPagerTest,
                         STParameterizedBucketTest::ephConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(PersistentFullValue,
                         MB_36087,
                         STParameterizedBucketTest::persistentConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

#else
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(STItemPagerTest);
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(MultiPagingVisitorTest);
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(STExpiryPagerTest);
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(STFullEvictionNoBloomFilterPagerTest);
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(STValueEvictionExpiryPagerTest);
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(MB_32669);
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(STEphemeralItemPagerTest);
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(MB_36087);
#endif
