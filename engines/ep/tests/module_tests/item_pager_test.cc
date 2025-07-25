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
#include "bgfetcher.h"
#include "checkpoint_manager.h"
#include "checkpoint_utils.h"
#include "ep_bucket.h"
#include "ep_time.h"
#include "ephemeral_bucket.h"
#include "ephemeral_mem_recovery.h"
#include "evp_store_single_threaded_test.h"
#include "item.h"
#include "item_pager.h"
#include "kv_bucket.h"
#include "kvstore/kvstore.h"
#include "learning_age_and_mfu_based_eviction.h"
#include "test_helpers.h"
#include "tests/mock/mock_ep_bucket.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/collections/collections_test_helpers.h"
#include "vb_adapters.h"
#include "vbucket.h"
#include "vbucket_utils.h"
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <platform/cb_arena_malloc.h>
#include <platform/semaphore.h>
#include <programs/engine_testapp/mock_server.h>
#include <statistics/labelled_collector.h>
#include <statistics/tests/mock/mock_stat_collector.h>
#include <utilities/string_utilities.h>
#include <utilities/test_manifest.h>
#include <xattr/blob.h>
#include <xattr/utils.h>
#include <chrono>

using namespace std::string_literals;

using FlushResult = EPBucket::FlushResult;
using MoreAvailable = EPBucket::MoreAvailable;

// Comparing durations as ints gives better GTest output
template <typename T>
static constexpr auto countMilliseconds(T x) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(x).count();
}

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
                "magma_min_checkpoint_interval=0;"
                "magma_sync_every_batch=true";

        STParameterizedBucketTest::SetUp();

        ObjectRegistry::onSwitchThread(engine.get());

        // By setting the magma config params above, it causes
        // magma to flush with every batch. This will release memory
        // being held by streaming sstables (~600KB). Otherwise, we would
        // have to bump up the quota for magma.
        increaseQuota(800_KiB);

        // How many nonIO tasks we expect initially
        // - 0 for persistent.
        // - 1 for Ephemeral (EphTombstoneHTCleaner).
        if (engine->getConfiguration().getBucketTypeString() == "ephemeral") {
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
        engine->setMaxDataSize(quota);
    }

    cb::engine_errc storeItem(Item& item) {
        uint64_t cas = 0;
        return engine->storeInner(
                *cookie, item, cas, StoreSemantics::Set, false);
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
    size_t populateUntilOOM(Vbid vbid,
                            rel_time_t ttl = 0,
                            std::string keyPrefix = "xxx_",
                            size_t valueSize = 15000) {
        size_t count = 0;
        const auto& stats = engine->getEpStats();
        EXPECT_LT(stats.getEstimatedTotalMemoryUsed(), stats.mem_high_wat);
        const std::string value(valueSize, 'x');

        cb::engine_errc result = cb::engine_errc::success;
        while (result == cb::engine_errc::success) {
            auto key = makeStoredDocKey(fmt::format("{}{}", keyPrefix, count));
            auto item =
                    make_item(vbid, key, value, ep_convert_to_expiry_time(ttl));
            // Set freqCount to 0 so will be a candidate for paging out straight
            // away.
            item.setFreqCounterValue(0);
            result = storeItem(item);

            if (result == cb::engine_errc::success) {
                ++count;
            }

            // This prevents flaky load post conditions where mem-usage doesn't
            // stay over the HWM. By expelling every item we ensure that memory
            // is mostly in the HT when we exit this loop.
            flushAndExpelFromCheckpoints(vbid);
        }
        // result depends on whether we hit the bucket quota or just the HWM
        EXPECT_THAT(result,
                    testing::AnyOf(cb::engine_errc::temporary_failure,
                                   cb::engine_errc::no_memory));

        EXPECT_GT(stats.getEstimatedTotalMemoryUsed(),
                  stats.mem_high_wat.load())
                << "Expected to exceed high watermark after hitting TMPFAIL";
        EXPECT_GT(stats.getEstimatedTotalMemoryUsed(), stats.mem_low_wat.load())
                << "Expected to exceed low watermark after hitting TMPFAIL";

        // To ensure the Blobs can actually be removed from memory, they must
        // have a ref-count of 1. This will not be the case if there's any open
        // checkpoints hanging onto Items. Therefore force the creation of a new
        // checkpoint that will also remove the closed ones.
        auto& cm = *store->getVBucket(vbid)->checkpointManager;
        cm.createNewCheckpoint();

        // Ensure items are flushed to disk (so we can evict them).
        flushDirectlyIfPersistent(vbid);

        // manually drive checkpoint destruction to recover memory.
        runCheckpointDestroyer(vbid);

#ifdef EP_USE_MAGMA
        // Magma... we need to run explicit compaction to make sure no
        // implicit compactions run that might use memory.
        if (hasMagma()) {
            auto kvstore = store->getRWUnderlyingByShard(0);
            // Force a compaction here to make sure there are no implicit
            // compactions to consume any memory
            CompactionConfig compactionConfig;
            auto cctx = dynamic_cast<EPBucket*>(store)->makeCompactionContext(
                    vbid, compactionConfig, 0);
            auto vb = store->getLockedVBucket(vbid);
            EXPECT_EQ(CompactDBStatus::Success,
                      kvstore->compactDB(vb.getLock(), cctx));
        }
#endif
        return count;
    }

    int populateUntilAboveHighWaterMark(Vbid vbid, uint8_t freqCount = 0) {
        int count = 0;

        const auto& stats = engine->getEpStats();
        EXPECT_LT(stats.getEstimatedTotalMemoryUsed(), stats.mem_high_wat);
        // Load ~ 20 documents
        const size_t valueSize =
                (stats.mem_high_wat - stats.getEstimatedTotalMemoryUsed()) / 20;
        const std::string value(valueSize, 'x');

        cb::engine_errc result = cb::engine_errc::success;
        while (result == cb::engine_errc::success) {
            auto key = makeStoredDocKey("key_" + std::to_string(count));
            auto item = make_item(vbid, key, value, 0 /*ttl*/);
            // By default, set freqCount to 0 so will be a candidate for paging
            // out straight away. Some callers may need to set a different value
            item.setFreqCounterValue(freqCount);
            result = storeItem(item);

            if (result == cb::engine_errc::success) {
                ++count;
            }

            // Purpose is to hit the HWM by HT allocation
            flushAndExpelFromCheckpoints(vbid);
        }

        EXPECT_GT(stats.getEstimatedTotalMemoryUsed(), stats.mem_high_wat)
                << "Expected to exceed high watermark after hitting TMPFAIL";

        return count;
    }

    /**
     * Store items evenly into the provided vbuckets until the given
     * predicate is met.
     *
     * NOTE: If you rely on super-precise memory usage measurements within the
     * predicate, be careful with how the arguments passed into this function
     * are constructed. Constructing the std::vector (and std::function)
     * as temporaries, will mean the memory usage for the buffers for these
     * structures will be included in the engine's memory usage (if they have to
     * allocate memory).
     *
     * @return the number of items stored into each vbucket NB: may be off by 1
     *         for some vbuckets, dependent on when the predicate is satisfied
     */
    size_t populateVbsUntil(const std::vector<Vbid>& vbids,
                            const std::function<bool()>& predicate,
                            std::string_view keyPrefix = "key_",
                            size_t itemSize = 5000,
                            size_t batchSize = 1) {
        bool populate = true;
        int count = 0;
        while (populate) {
            for (auto vbid : vbids) {
                // scope to ensure everything is destroyed before checking the
                // predicate (in case the predicate depends on memory usage)
                {
                    for (size_t i = 0; i < batchSize; i++) {
                        auto key = makeStoredDocKey(std::string(keyPrefix) +
                                                    std::to_string(count++));
                        auto item = make_item(
                                vbid, key, std::string(itemSize, 'x'));
                        // Set the frequency counter to the same value for all items
                        // to avoid potential probabilistic failures around not
                        // evicting to below the LWM in one pass
                        item.setFreqCounterValue(0);
                        const auto status = storeItem(item);
                        if (status != cb::engine_errc::success) {
                            ADD_FAILURE()
                                    << "Failed (cb::engine_errc: " << status
                                    << ") storing an item before the "
                                       "predicate returned true";
                            return count;
                        }
                    }

                    // Some ItemPager tests want to store up to the HWM, which
                    // is prevented by MB-46827 (bounded CM mem-usage). To allow
                    // that, just move the cursor and expel from checkpoints.
                    //
                    // Note: Flushing and expelling at every mutation is indeed
                    // expensive but it seems that we can't avoid that for some
                    // tests. Problem is that some ItemPager tests want to
                    // verify that the ItemPager is able to push the mem-usage
                    // down to the LWM. And that is obviously based on the
                    // assumption that there's enough allocation in the HT
                    // for the pager to free-up. So, if (eg) I perform the
                    // flush+expel only periodically, then we do get to HWM but
                    // the allocation in the HT doesn't hit the required level
                    // for verifying the pager assumptions.
                    //
                    // For avoiding that, we use larger value sizes and a batch
                    // size of 1 as default. Other tests can override this if
                    // they require different (less strict) conditions.
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
class STItemPagerTest : public STBucketQuotaTest {
public:
    static auto allConfigValuesAllEvictionStrategiesNoNexus() {
        auto evictionStrategies =
                config::Config{{"item_eviction_strategy",
                                {"upfront_mfu_only", "learning_age_and_mfu"}}};
        return allConfigValuesNoNexus() * evictionStrategies;
    }

protected:
    void SetUp() override {
        if (!config_string.empty()) {
            config_string += ";";
        }
        // Disable pausing mid-vbucket by default
        config_string += "paging_visitor_pause_check_count=1;";
        config_string +=
                "concurrent_pagers=" + std::to_string(getNumConcurrentPagers());
        config_string += ";expiry_pager_concurrency=" +
                         std::to_string(getNumConcurrentExpiryPagers());
        STBucketQuotaTest::SetUp();

        if (isEphemeralMemRecoveryEnabled()) {
            if (isCrossBucketHtQuotaSharing()) {
                GTEST_SKIP()
                        << "EphemeralMemRecovery and cross-bucket HT quota "
                           "sharing are not compatible";
            }
            // Stop periodic execution of ItemPager
            updateItemPagerSleepTime(std::chrono::milliseconds(INT_MAX));
            scheduleEphemeralMemRecovery();
            // EphMemRec utilises checkpoint remover task
            scheduleCheckpointRemoverTask();
            initialNonIoTasks += 2;
            ephemeralMemRecoveryScheduled = true;
        }

        // For Ephemeral fail_new_data buckets we have no item pager, instead
        // the Expiry pager is used.
        if (engine->getConfiguration().getEphemeralFullPolicyString() ==
            "fail_new_data") {
            // change the config and allow the listener to enable the task
            // (ensures the config is consistent with reality, rather than
            // directly enabling the task).
            engine->getConfiguration().parseConfiguration(
                    "exp_pager_enabled=true");
            ++initialNonIoTasks;
        } else {
            // Everyone else uses the ItemPager.
            scheduleItemPager();
            ++initialNonIoTasks;
            itemPagerScheduled = true;
        }

        // Sanity check - should be no nonIO tasks ready to run,
        // and expected number in futureQ.
        auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);
        EXPECT_EQ(0, lpNonioQ.getReadyQueueSize());
        EXPECT_EQ(initialNonIoTasks, lpNonioQ.getFutureQueueSize());

        // We shouldn't be able to schedule the Item Pager task yet as it's not
        // ready.
        try {
            CB_SCOPED_TRACE("");
            runNextTask(lpNonioQ, itemPagerTaskName());
            FAIL() << "Unexpectedly managed to run Item Pager";
        } catch (std::logic_error&) {
        }

        // Set up mock pager that is ready to evict.
        auto pagerSemaphore = std::make_shared<cb::Semaphore>();
        mockVisitor = std::make_unique<MockItemPagingVisitor>(
                *store,
                engine->getEpStats(),
                ItemEvictionStrategy::evict_everything(),
                pagerSemaphore,
                false,
                VBucketFilter());
        mockVisitor->setCurrentBucket(*engine->getKVBucket()->getVBucket(vbid));
    }

    bool isCrossBucketHtQuotaSharing() {
        return config_string.find("cross_bucket_ht_quota_sharing=true") !=
               std::string::npos;
    }

    std::string itemPagerTaskName() {
        if (isCrossBucketHtQuotaSharing()) {
            return "Paging out items (quota sharing).";
        }
        return "Paging out items.";
    }

    void runPagingAdapterTask() {
        auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);
        if (isCrossBucketHtQuotaSharing()) {
            // The cross-bucket adapter runs one task per vBucket so we schedule
            // it until completed
            while (initialNonIoTasks < lpNonioQ.getFutureQueueSize()) {
                try {
                    runNextTask(lpNonioQ,
                                "Item pager (quota sharing) "
                                "(SynchronousEPEngine:default)");
                } catch (const std::runtime_error&) {
                    break;
                }
            }
        } else {
            runNextTask(lpNonioQ, "Item pager no vbucket assigned");
        }
    }

    /**
     * Run the pager which is scheduled when the high watermark is reached
     * (memoryCondition). This is either the ItemPager (for buckets where
     * items can be paged out - Persistent or Ephemeral-auto_delete), or
     * the Expiry pager (Ephemeral-fail_new_data).
     */
    void runHighMemoryPager() {
        auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);
        ASSERT_EQ(0, lpNonioQ.getReadyQueueSize());
        ASSERT_EQ(initialNonIoTasks, lpNonioQ.getFutureQueueSize());

        if (ephemeralMemRecoveryScheduled) {
            // Ephemeral MemRecovery wakes up checkpoint remover first,
            // it then wakes up item pager for auto-delete buckets.
            runNextTask(lpNonioQ, "Ephemeral Memory Recovery");

            if (itemPagerScheduled) {
                // Only auto-delete bucket has itemPager scheduled
                runNextTask(lpNonioQ, "CheckpointMemRecoveryTask:0");
                runNextTask(lpNonioQ, "Ephemeral Memory Recovery");
                // item pager is run as final stage of EphemeralMemRecovery
                runNextTask(lpNonioQ, itemPagerTaskName());
                ASSERT_EQ(0, lpNonioQ.getReadyQueueSize());
                ASSERT_EQ(initialNonIoTasks + 1, lpNonioQ.getFutureQueueSize());
                runPagingAdapterTask();
            } else {
                // fail_new_data bucket schedules the expiry pager as well
                runNextTask(lpNonioQ, "Paging expired items.");
                runNextTask(lpNonioQ, "CheckpointMemRecoveryTask:0");
                runNextTask(lpNonioQ,
                            "Expired item remover no vbucket assigned");
                runNextTask(lpNonioQ, "Ephemeral Memory Recovery");
            }
        } else if (itemPagerScheduled) {
            // Item pager consists of two Tasks - the parent ItemPager task,
            // and then a task (via VCVBAdapter) to process each vBucket (which
            // there is just one of as we only have one vBucket online).
            runNextTask(lpNonioQ, itemPagerTaskName());
            ASSERT_EQ(0, lpNonioQ.getReadyQueueSize());
            ASSERT_EQ(initialNonIoTasks + 1, lpNonioQ.getFutureQueueSize());
            runPagingAdapterTask();
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

    /**
     * Describes the result of a paging visitor visit.
     */
    struct PagerResult {
        /// Has the pager expired the key?
        bool expired = false;
        /// Has the pager evicted the key?
        bool evicted = false;
        /// Has the pager removed the key (as result of eviction/expiration)?
        bool unlinked = false;
    };

    /**
     * Visits the value in the HT under that key using the PagingVisitor.
     */
    PagerResult mockVisitKey(const DocKeyView& key) {
        auto& ht = store->getVBucket(vbid)->ht;

        auto preExpired = engine->getEpStats().expired_pager;
        auto preEvicted = ht.getNumEjects();

        {
            auto [sv, hbl] =
                    ht.findForRead(key, TrackReference::No, WantsDeleted::Yes);
            if (!sv) {
                throw std::logic_error(
                        fmt::format("Expected to find key {}",
                                    static_cast<std::string_view>(key)));
            }

            EXPECT_TRUE(mockVisitor->setUpHashBucketVisit());
            mockVisitor->visit(hbl, const_cast<StoredValue&>(*sv));
            mockVisitor->tearDownHashBucketVisit();
        }
        mockVisitor->update();

        return PagerResult{
                preExpired < engine->getEpStats().expired_pager,
                preEvicted < ht.getNumEjects(),
                !ht.findForRead(key, TrackReference::No, WantsDeleted::Yes)
                         .storedValue,
        };
    }

    /**
     * Inserts a copy of the SV in the HT and visits it with the PagingVisitor.
     * @returns PagerResult which describes the action that was taken.
     */
    PagerResult mockVisitSV(const StoredValue& testSV) {
        auto& ht = store->getVBucket(vbid)->ht;
        forceInsert(ht, ht.getLockedBucket(testSV.getKey()), testSV);
        auto result = mockVisitKey(testSV.getKey());
        removeIfExists(
                ht, ht.getLockedBucket(testSV.getKey()), testSV.getKey());
        return result;
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

    size_t getNumConcurrentExpiryPagers() const {
        return numConcurrentExpiryPagers;
    }

    /**
     * Set the configured number of expiry PagingVisitors to run concurrently.
     *
     * Should be set before calling STItemPagerTest::SetUp()
     */
    void setNumConcurrentExpiryPagers(size_t num) {
        numConcurrentExpiryPagers = num;
    }

    void pagerEvictsSomething(bool dropCollection);

    size_t numConcurrentPagers = 1;
    size_t numConcurrentExpiryPagers = 1;
    /// Has the item pager been scheduled to run?
    bool itemPagerScheduled = false;
    /// Paging visitor set up to visit VBucket(`vbid`).
    std::unique_ptr<MockItemPagingVisitor> mockVisitor;
    bool ephemeralMemRecoveryScheduled = false;
};

TEST_P(STItemPagerTest, MaxVisitorDuration) {
    auto& stats = engine->getEpStats();
    StrictQuotaItemPager pager(*engine, stats, 1);

    stats.setMaxDataSize(0);
    EXPECT_EQ(125, countMilliseconds(pager.maxExpectedVisitorDuration()));

    stats.setMaxDataSize(10_GiB);
    EXPECT_EQ(125, countMilliseconds(pager.maxExpectedVisitorDuration()));

    stats.setMaxDataSize(100_GiB);
    EXPECT_EQ(575, countMilliseconds(pager.maxExpectedVisitorDuration()));
}

/**
 * Simulates a VBCBAdaptor and counts the number of vBucket visits.
 * @param pv PagingVisitor to drive
 * @param vb vBucket to visit
 * @return number of times the vBucket was visited
 */
static size_t countPagingVisitorVisits(PagingVisitor& pv, VBucket& vb) {
    size_t numVisits = 0;
    pv.begin();
    do {
        pv.visitBucket(vb);
        ++numVisits;
    } while (pv.needsToRevisitLast() != NeedsRevisit::No);
    pv.complete();
    return numVisits;
}

TEST_P(STItemPagerTest, VisitorPausesMidVBucket) {
    // Define mock visitors that adjust the time so the time condition for
    // pausing is always true, and count the number of times HT visits continued
    // or were paused.

    class MockExpiredPagingVisitor : public ExpiredPagingVisitor {
    public:
        using ExpiredPagingVisitor::ExpiredPagingVisitor;

        ExecutionState shouldInterrupt() override {
            return ExecutionState::Pause;
        }

        bool visit(const HashTable::HashBucketLock& lh,
                   StoredValue& v) override {
            bool ret = ExpiredPagingVisitor::visit(lh, v);
            if (ret) {
                ++numContinue;
            } else {
                ++numPause;
            }
            return ret;
        }

        size_t numPause = 0;
        size_t numContinue = 0;
    };

    class MockItemPagingVisitor : public ItemPagingVisitor {
    public:
        using ItemPagingVisitor::ItemPagingVisitor;

        ExecutionState shouldInterrupt() override {
            return ExecutionState::Pause;
        }

        bool visit(const HashTable::HashBucketLock& lh,
                   StoredValue& v) override {
            bool ret = ItemPagingVisitor::visit(lh, v);
            if (ret) {
                ++numContinue;
            } else {
                ++numPause;
            }
            return ret;
        }

        size_t numPause = 0;
        size_t numContinue = 0;
    };

    if (!isPersistent()) {
        GTEST_SKIP();
    }

    auto& config = engine->getConfiguration();
    // Enable pausing mid-vbucket, but reduce the count from the config default,
    // so that we don't check the pause condition every time
    config.setPagingVisitorPauseCheckCount(5);
    // Age is not relevant to this test, increase the MFU threshold
    // under which age is ignored
    config.setItemEvictionFreqCounterAgeThreshold(255);

    auto expirySemaphore = std::make_shared<cb::Semaphore>();
    MockExpiredPagingVisitor expiryVisitor(*store,
                                           engine->getEpStats(),
                                           expirySemaphore,
                                           true,
                                           VBucketFilter());

    auto pagingSemaphore = std::make_shared<cb::Semaphore>();
    MockItemPagingVisitor pagingVisitor(
            *store,
            engine->getEpStats(),
            ItemEvictionStrategy::evict_everything(),
            pagingSemaphore,
            true,
            VBucketFilter());

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    auto& stats = engine->getEpStats();
    const auto mem_high_wat = stats.mem_high_wat.load();

    size_t itemCount = 0;
    std::string value(200, 'x');
    do {
        // Populate in batches of 50
        for (int ii = 0; ii < 50; ++ii) {
            auto key = makeStoredDocKey(std::to_string(itemCount++));
            auto item = make_item(vbid, key, value);
            // Set the frequency counter to the same value for all items
            // to avoid potential probabilistic failures around not
            // evicting to below the LWM in one pass
            item.setFreqCounterValue(0);
            ASSERT_EQ(cb::engine_errc::success, storeItem(item));
        }
        flushAndExpelFromCheckpoints(vbid); // Free memory from checkpoints
        vb->ht.resizeInOneStep(); // Keep chains short
    } while (stats.getPreciseTotalMemoryUsed() <= mem_high_wat);

    // We expect to pause at least once, so the vBucket will need to be visited
    // more than once. We also limit how often we check the pause condition,
    // so we expect the number of visits to be less than the HT size. This also
    // means that not every HT visit will indicate a pause.

    size_t numVisits = countPagingVisitorVisits(expiryVisitor, *vb);
    EXPECT_GT(numVisits, 1);
    EXPECT_LT(numVisits, vb->ht.getSize());
    EXPECT_GT(expiryVisitor.numPause, 0);
    EXPECT_GT(expiryVisitor.numContinue, 0);

    numVisits = countPagingVisitorVisits(pagingVisitor, *vb);
    EXPECT_GT(numVisits, 1);
    EXPECT_LT(numVisits, vb->ht.getSize());
    EXPECT_GT(pagingVisitor.numPause, 0);
    EXPECT_GT(pagingVisitor.numContinue, 0);
}

TEST_P(STItemPagerTest, MB_50423_ItemPagerCleansUpDeletedStoredValues) {
    // MB-50423:
    //  A request for a deleted value (e.g., to read system xattrs) bgfetches a
    //  delete back into memory. Under full eviction, this value can eventually
    //  be evicted again. For value eviction, while the _value_ may be evicted,
    //  nothing cleaned up the metadata, leaving it in memory until it is
    //  overwritten. Verify that the ItemPager now evicts deleted item metadata
    //  even under value eviction.

    if (ephemeral()) {
        // Ephemeral deletes remain in memory until the purge interval elapses,
        // and there's no backing disk for the deletes to be bgfetched from.
        GTEST_SKIP();
    }
    auto key = makeStoredDocKey("key");
    // get a deleted item on disk
    storeAndDeleteItem(vbid, key, "value");

    auto vb = store->getVBucket(vbid);
    auto& ht = vb->ht;

    // confirm directly that the item is _not_ present in the HT
    {
        const auto* sv =
                ht.findForRead(key, TrackReference::No, WantsDeleted::Yes)
                        .storedValue;

        ASSERT_FALSE(sv);
    }

    // now request with GET_DELETED_VALUE - should trigger a bgfetch of the item
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS | GET_DELETED_VALUE);

    auto gv = store->get(key, vbid, cookie, options);
    ASSERT_EQ(cb::engine_errc::would_block, gv.getStatus());

    runBGFetcherTask();

    // successfully read the deleted item
    gv = store->get(key, vbid, cookie, options);
    ASSERT_EQ(cb::engine_errc::success, gv.getStatus());

    // confirm directly that the item _is_ present in the HT
    {
        const auto* sv =
                ht.findForRead(key, TrackReference::No, WantsDeleted::Yes)
                        .storedValue;

        ASSERT_TRUE(sv);
    }

    // Setup complete - there is now a deleted, non-resident item in the HT
    // MB-50423 : the metadata for this item would not be routinely cleaned up,
    // and could stay in memory forever if not overwritten

    // attempt to evict every possible item
    auto pagerSemaphore = std::make_shared<cb::Semaphore>();
    pagerSemaphore->try_acquire(1);
    auto pv = std::make_unique<MockItemPagingVisitor>(
            *engine->getKVBucket(),
            engine->getEpStats(),
            ItemEvictionStrategy::evict_everything(),
            pagerSemaphore,
            false,
            VBucketFilter());

    // Drop the lwn ensure memory usage is above it so paging proceeds.
    auto& config = engine->getConfiguration();
    auto origLowWatermark = config.getMemLowWatPercent();
    config.setMemLowWatPercent(0);

    // Drop the MFU of the item to let it be evicted now
    ht.findOnlyCommitted(key).storedValue->setFreqCounterValue(0);

    pv->visitBucket(*vb);

    config.setMemLowWatPercent(origLowWatermark);

    // Expect that the stored value is no longer present - even though this
    // is a value eviction bucket.
    {
        const auto* sv =
                ht.findForRead(key, TrackReference::No, WantsDeleted::Yes)
                        .storedValue;

        EXPECT_FALSE(sv);
    }
}

// Test that the ItemPager is scheduled when the Server Quota is reached, and
// that items are successfully paged out.
TEST_P(STItemPagerTest, ServerQuotaReached) {
    size_t count = populateUntilOOM(vbid);
    // Make sure we have atleast one document saved, so that we can check at the
    // end 'count' number of documents were paged out.
    ASSERT_GT(count, 0) << "No Documents stored";

    auto& stats = engine->getEpStats();
    auto currMem = stats.getPreciseTotalMemoryUsed();

    // Set the HWM 1 byte lower than the currMem usage, to force all the items
    // to be paged out.

    stats.setHighWaterMark(currMem - 1);

    runHighMemoryPager();

    // For all configurations except ephemeral fail_new_data, memory usage
    // should have dropped.
    auto vb = engine->getVBucket(vbid);
    if (ephemeralFailNewData()) {
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

// Test that the ItemPager is scheduled periodically (pager_sleep_time_ms),
// even if a "memoryConditon" is not detected. Regression test for MB-59287.
TEST_P(STItemPagerTest, ItemPagerRunPeriodically) {
    if (ephemeralFailNewData()) {
        // EphemeralFailNewData doesn't enable the ItemPager.
        GTEST_SKIP();
    }
    if (isEphemeralMemRecoveryEnabled()) {
        // Periodic execution disabled if EphemeralMemRecovery is enabled.
        GTEST_SKIP();
    }

    // Check initial sleep time of ItemPager - advance time by item pager
    // sleep period, and run the next NonIO task.
    // This should be the initial item pager run.
    auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);
    EXPECT_EQ(0, lpNonioQ.getReadyQueueSize());
    EXPECT_GE(lpNonioQ.getFutureQueueSize(), 1);
    // Run the next task, advancing time by just over the item pager period,
    // so we expect it to be run.
    const auto timeAdvance = std::chrono::milliseconds{
            engine->getConfiguration().getPagerSleepTimeMs() + 1};

    const auto expectedTask = isCrossBucketHtQuotaSharing()
                                      ? "Paging out items (quota sharing)."
                                      : "Paging out items.";
    runNextTask(lpNonioQ, expectedTask, timeAdvance);

    // Check subsequent runs - again advance time by item pager sleep period,
    // and run the next NonIO task.
    // This should be the periodic item pager run.
    runNextTask(lpNonioQ, expectedTask, timeAdvance * 2);
}

TEST_P(STItemPagerTest, ItemPagerUpdateSleepTime) {
    if (ephemeralFailNewData()) {
        // EphemeralFailNewData doesn't enable the ItemPager.
        GTEST_SKIP();
    }
    if (isCrossBucketHtQuotaSharing()) {
        // QuotaSharingItemPager does not support updating sleep time.
        GTEST_SKIP();
    }
    if (isEphemeralMemRecoveryEnabled()) {
        // Periodic execution disabled if EphemeralMemRecovery is enabled.
        GTEST_SKIP();
    }

    // Check initial sleep time of ItemPager - advance time by item pager
    // sleep period, and run the next NonIO task.
    // This should be the initial item pager run.
    auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);
    EXPECT_EQ(0, lpNonioQ.getReadyQueueSize());
    EXPECT_GE(lpNonioQ.getFutureQueueSize(), 1);

    // Run the next task, advancing time by just over the item pager period,
    // so we expect it to be run.
    auto timeAdvance = std::chrono::milliseconds{
            engine->getConfiguration().getPagerSleepTimeMs() + 1};
    const auto itemPagerTask = "Paging out items.";
    runNextTask(lpNonioQ, itemPagerTask, timeAdvance);

    // Change sleep time and check it is utilised.
    updateItemPagerSleepTime(1000ms);
    timeAdvance = 1001ms;
    runNextTask(lpNonioQ, itemPagerTask, timeAdvance);

    // Change and check again
    updateItemPagerSleepTime(2000ms);
    timeAdvance = 2001ms;
    runNextTask(lpNonioQ, itemPagerTask, timeAdvance);
}

TEST_P(STItemPagerTest, HighWaterMarkTriggersPager) {
    // Fill to just over HWM
    populateUntilAboveHighWaterMark(vbid);
    // Success if the pager is now ready
    runHighMemoryPager();
}

void STItemPagerTest::pagerEvictsSomething(bool dropCollection) {
    // Pager can't run in fail_new_data policy so test is invalid
    if (ephemeralFailNewData()) {
        return;
    }

    // Fill to just over HWM
    // When dropCollection is true, set the freqCount to be high so items are
    // not eligible for paging by MFU, but instead get dropped
    populateUntilAboveHighWaterMark(vbid, dropCollection ? 255 : 0);

    // Flush so items are eligible for ejection
    flushDirectlyIfPersistent(vbid);

    auto vb = store->getVBucket(vbid);
    auto items = vb->getNumItems();

    if (dropCollection) {
        CollectionsManifest cm(NoDefault{});
        // Add 1 scope which reproduces MB-58333
        cm.add(ScopeEntry::shop1);
        vb->updateFromManifest(
                std::shared_lock<folly::SharedMutex>(vb->getStateLock()),
                makeManifest(cm));
    }

    auto memUsed = engine->getEpStats().getPreciseTotalMemoryUsed();

    // Success if the pager is now ready
    runHighMemoryPager();

    if (persistent()) {
        // Should have evicted something
        if (dropCollection) {
            if (isFullEviction()) {
                // FE uses the 'disk' item count and disk still stores the item.
                EXPECT_EQ(items, vb->getNumItems());
                EXPECT_EQ(items,
                          store->getVBucket(vbid)->getNumNonResidentItems());

            } else {
                // VE the count goes down
                EXPECT_EQ(0, vb->getNumItems());
                EXPECT_EQ(0, store->getVBucket(vbid)->getNumNonResidentItems());
            }

        } else {
            EXPECT_LT(0, store->getVBucket(vbid)->getNumNonResidentItems());
            EXPECT_EQ(items, vb->getNumItems());
        }

        // Check memory went down before reading data back
        EXPECT_LE(engine->getEpStats().getPreciseTotalMemoryUsed(), memUsed);

        // Cannot read the keys if dropColection==true
        if (!dropCollection) {
            // Bump up max size and HWM so we don't encounter memory issues
            // during get phase
            auto quota = engine->getEpStats().getPreciseTotalMemoryUsed() * 2;
            increaseQuota(quota);

            // Read all of our items to verify that they are still there. Some
            // will be on disk
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
                    FAIL() << "Unexpected status:" << gv.getStatus();
                }
            }
        }
    } else {
        if (dropCollection) {
            EXPECT_EQ(0, vb->getNumItems());
        } else {
            EXPECT_LT(vb->getNumItems(), items);
        }
    }
}

TEST_P(STItemPagerTest, PagerEvictsSomething) {
    pagerEvictsSomething(false);
}

TEST_P(STItemPagerTest, PagerDropsCollectionData) {
    pagerEvictsSomething(true);
}

// Tests that for the hifi_mfu eviction algorithm we visit replica vbuckets
// first.
TEST_P(STItemPagerTest, ReplicaItemsVisitedFirst) {
    // For the Expiry Pager we do not enforce the visiting of replica buckets
    // first.
    if (ephemeralFailNewData()) {
        GTEST_SKIP();
    }
    // EphemeralMemRecovery utilises ItemPager for eviction - test not required
    if (isEphemeralMemRecoveryEnabled()) {
        GTEST_SKIP();
    }
    auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);

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

    auto count = populateUntilOOM(replicaVB);
    store->setVBucketState(replicaVB, vbucket_state_replica);

    runNextTask(lpNonioQ, itemPagerTaskName());
    runPagingAdapterTask();

    if (ephemeral()) {
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
    auto countB = populateUntilOOM(vbid, 1);

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
    EXPECT_EQ(cb::engine_errc::success,
              engine->setFlushParam("max_size", std::to_string(10_MiB), msg));

    // Store a large document 4MiB
    std::string value(4_MiB, 'a');
    {
        auto item =
                make_item(vbid, {"key", DocKeyEncodesCollectionId::No}, value);
        // Set freqCount to 0 so will be a candidate for paging out straight
        // away.
        item.setFreqCounterValue(0);
        ASSERT_EQ(cb::engine_errc::success, storeItem(item));
    }

    if (persistent()) {
        // flush so the HT item becomes clean
        flush_vbucket_to_disk(vbid);

        // Now do some steps which will remove the checkpoint, all of these
        // steps are needed
        auto vb = engine->getVBucket(vbid);

        // Force close the current checkpoint
        vb->checkpointManager->createNewCheckpoint();
        // Reflush
        flush_vbucket_to_disk(vbid);
        // Closed checkpoints removed at this point
    }

    // Now set max_size to be mem_used + 10% (we need some headroom)
    auto& stats = engine->getEpStats();
    EXPECT_EQ(
            cb::engine_errc::success,
            engine->setFlushParam(
                    "max_size",
                    std::to_string(stats.getEstimatedTotalMemoryUsed() * 1.10),
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

    if (persistent()) {
        EXPECT_EQ(1, stats.numValueEjects);
    }

    if (ephemeralFailNewData()) {
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
    populateUntilOOM(vbid);

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
    auto strategyPtr = std::make_unique<LearningAgeAndMFUBasedEviction>(
            EvictionRatios{0.0 /* active&pending */,
                           0.0 /* replica */}, // evict nothing
            cfg.getItemEvictionAgePercentage(),
            cfg.getItemEvictionFreqCounterAgeThreshold(),
            nullptr /* epstats */);
    auto& strategy = *strategyPtr;
    std::unique_ptr<MockItemPagingVisitor> pv =
            std::make_unique<MockItemPagingVisitor>(*engine->getKVBucket(),
                                                    engine->getEpStats(),
                                                    std::move(strategyPtr),
                                                    pagerSemaphore,
                                                    false,
                                                    VBucketFilter());

    VBucketPtr vb = store->getVBucket(vbid);
    pv->visitBucket(*vb);
    auto initialCount = Item::initialFreqCount;
    EXPECT_NE(initialCount, strategy.getThresholds(100.0, 0.0).first);
    EXPECT_NE(255, strategy.getThresholds(100.0, 0.0).first);
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
    auto pv = std::make_unique<MockItemPagingVisitor>(
            *engine->getKVBucket(),
            engine->getEpStats(),
            // try evict everything, hotness/age eviction behaviour is not
            // under test here.
            ItemEvictionStrategy::evict_everything(),
            pagerSemaphore,
            false,
            VBucketFilter());

    pv->setCurrentBucket(*engine->getKVBucket()->getVBucket(vbid));
    flushVBucketToDiskIfPersistent(vbid);
    int iterationCount = 0;
    while ((pv->getEjected() == 0) &&
           iterationCount <= Item::initialFreqCount) {
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
    auto pv = std::make_unique<MockItemPagingVisitor>(
            *engine->getKVBucket(),
            engine->getEpStats(),
            // try evict everything, hotness/age eviction behaviour is not
            // under test here.
            ItemEvictionStrategy::evict_everything(),
            pagerSemaphore,
            false,
            VBucketFilter());

    pv->setCurrentBucket(*engine->getKVBucket()->getVBucket(vbid));
    store->setVBucketState(vbid, vbucket_state_replica);

    VBucketPtr vb = store->getVBucket(vbid);
    for (int ii = 0; ii <= Item::initialFreqCount; ii++) {
        vb->ht.visit(*pv);
    }

    // no items were ejected.
    EXPECT_EQ(0, pv->getEjected());

    {
        const auto* sv =
                vb->ht.findForRead(key, TrackReference::No, WantsDeleted::Yes)
                        .storedValue;

        // stored value should still be in the HT
        EXPECT_TRUE(sv);
        // freq counter value was not decayed, despite being visited
        EXPECT_EQ(Item::initialFreqCount, sv->getFreqCounterValue());
    }
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
    auto key = makeStoredDocKey("key1");
    store_item(vbid, key, "value");
    flushVBucketToDiskIfPersistent(vbid, 1);

    delete_item(vbid, key);
    flushVBucketToDiskIfPersistent(vbid, 1);

    // 2) Evict it
    auto vb = store->getVBucket(vbid);

    const char* msg;
    std::shared_lock rlh(vb->getStateLock());
    vb->evictKey(&msg, rlh, vb->lockCollections(key));

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
        auto val = vb->fetchValidValue(rlh,
                                       WantsDeleted::Yes,
                                       TrackReference::No,
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
    // (as it is much "colder"). Note that this is still dependent on
    // the exact position in the hash-table, hence the key may need to
    // be tweaked when changing the hash-table implementation.
    populateUntilAboveHighWaterMark(
            vbid, /* MFU */ std::numeric_limits<uint8_t>::max());
    // flush the vb, only items which are eligible for eviction are used to
    // determine the MFU threshold for evicting items.
    flushDirectlyIfPersistent(vbid);
    runHighMemoryPager();

    // Finally, check our item again. It should now have been evicted
    checkItemStatePostFetch(false /*expect item to not exist*/);
}

// Previously disabled due to consistent failures (MB-51958)
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

    const Vbid activeVb(0);
    const Vbid replicaVb(1);

    setVBucketStateAndRunPersistTask(activeVb, vbucket_state_active);
    // Set replicaVb as active initially so we can populate it easily
    setVBucketStateAndRunPersistTask(replicaVb, vbucket_state_active);

    auto& stats = engine->getEpStats();

    const auto watermarkDiff = stats.mem_high_wat - stats.mem_low_wat;

    // starting above the low water mark would mean that the replica pageable
    // memory will definitely not exceed the watermarkdiff when the high
    // watermark is reached.
    ASSERT_LT(stats.getPreciseTotalMemoryUsed(), stats.mem_low_wat);

    // populate the replica vbs until its evictable memory _exceeds_ the gap
    // between the high and low water mark. When the mem usage later passes the
    // high water mark, evicting from the replica should reclaim enough memory.
    std::vector<Vbid> replicaVbsToPopulate{replicaVb};
    auto replicaItemCount = populateVbsUntil(
            replicaVbsToPopulate,
            [&stats, &store = store, replicaVb, watermarkDiff] {
                // sanity check - if this fails the quota is too low -
                EXPECT_LT(stats.getPreciseTotalMemoryUsed(),
                          stats.mem_high_wat);

                return store->getVBucket(replicaVb)->getPageableMemUsage() >
                       watermarkDiff;
            });
    EXPECT_GT(replicaItemCount, 10);

    // Now fill the active VB until the high watermark is reached
    std::vector<Vbid> activeVbsToPopulate{activeVb};
    auto activeItemCount = populateVbsUntil(activeVbsToPopulate, [&stats] {
        return stats.getPreciseTotalMemoryUsed() > stats.mem_high_wat;
    });
    store->attemptToFreeMemory();

    EXPECT_GT(activeItemCount, 10);

    setVBucketStateAndRunPersistTask(replicaVb, vbucket_state_replica);

    // check all vbuckets are fully resident to start
    ASSERT_EQ(100, getRRPercent(*store->getVBucket(activeVb)));
    ASSERT_EQ(100, getRRPercent(*store->getVBucket(replicaVb)));

    // PreciseTotalMemoryUsed will be above the high watermark by some amount
    // after population. Because the item pager evicts down to the low
    // watermark, this would cause us to evict > watermarkDiff, forcing
    // actives to be evicted as well.
    // Additionally, setting the replica vBuckets to replica state may change
    // the memory used. Set the low watermark so this doesn't affect the test.
    // Note we cannot exactly predict where preciseMemoryUsed will be when
    // the pager runs (it may be slightly higher due to heap allocaitons
    // made by the pager itself), so we cannot exactly set the low watermark
    // to:
    //     current_precise_mem_used - replicaPageable
    //
    // as if precise_mem_used_when_pager is slightly higher, we'll need to
    // evict more than just replica items. As such, set low watermark based on
    // 75% of replica pagable, giving us a margin of error.
    stats.setLowWaterMark(
            stats.getPreciseTotalMemoryUsed() -
            ((store->getVBucket(replicaVb)->getPageableMemUsage() * 3) / 4));

    // Run the item pager
    auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);
    // This creates the paging visitor
    runNextTask(lpNonioQ, itemPagerTaskName());
    runPagingAdapterTask();

    // nothing left to do
    EXPECT_EQ(0, lpNonioQ.getReadyQueueSize());

    // Nothing should be evicted from active
    EXPECT_EQ(100, getRRPercent(*store->getVBucket(activeVb)));
    // Confirm the replica RR is lower than active RR
    EXPECT_LT(getRRPercent(*store->getVBucket(replicaVb)), 100);
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

    // populating items with a default MFU of 0 allows all items to be evicted
    // in one pass. Without this, some items may not be evicted because:
    //  * learning item eviction defaults to an MFU threshold of 0 until
    //    _after_ an item has been visited
    //  * item age is considered to protect the lowest X% of items. an MFU of
    //    0 is below the default value of
    //    item_eviction_freq_counter_age_threshold
    //    so age will be ignored.
    // As mfu distribution learning and age are not under test here, this
    // simplifies this test and makes it less brittle.
    store->setInitialMFU(0);

    auto activeVB = Vbid(0);
    auto replicaVB = Vbid(1);

    setVBucketStateAndRunPersistTask(activeVB, vbucket_state_active);
    // Set replicaVB as active initially (so we can populate it).
    setVBucketStateAndRunPersistTask(replicaVB, vbucket_state_active);

    auto& stats = engine->getEpStats();
    auto watermarkDiff = stats.mem_high_wat - stats.mem_low_wat;

    // starting above the low water mark would mean that the replica pageable
    // memory will definitely not exceed the watermarkdiff when the high
    // watermark is reached.
    ASSERT_LT(stats.getPreciseTotalMemoryUsed(), stats.mem_low_wat);

    // store items in the replica vb such that evicting everything in the
    // replica when at the high watermark will _not_ reach the low watermark
    std::vector<Vbid> replicaVbsToPopulate = {replicaVB};
    auto replicaItemCount = populateVbsUntil(
            replicaVbsToPopulate,
            [&stats, &store = store, replicaVB, watermarkDiff] {
                // sanity check - if this fails the quota is too low -
                EXPECT_LT(stats.getPreciseTotalMemoryUsed(),
                          stats.mem_high_wat);

                return store->getVBucket(replicaVB)->getPageableMemUsage() >
                       watermarkDiff * 0.5;
            });

    // We don't care exactly how many were stored, just that it is a
    // "large enough" number for percentages to be somewhat useful
    EXPECT_GT(replicaItemCount, 0);

    setVBucketStateAndRunPersistTask(replicaVB, vbucket_state_replica);

    // Now fill the active VB until the high watermark is reached
    std::vector<Vbid> activeVbsToPopulate = {activeVB};
    auto activeItemCount = populateVbsUntil(activeVbsToPopulate, [&stats] {
        return stats.getPreciseTotalMemoryUsed() > stats.mem_high_wat;
    });
    store->attemptToFreeMemory();

    EXPECT_GT(activeItemCount, 0);

    size_t activeRR = getRRPercent(*store->getVBucket(activeVB));
    size_t replicaRR = getRRPercent(*store->getVBucket(replicaVB));

    ASSERT_EQ(100, activeRR);
    ASSERT_EQ(100, replicaRR);

    ASSERT_GT(stats.getPreciseTotalMemoryUsed(), stats.mem_high_wat.load());

    auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);

    // run the item pager. This creates the paging visitor
    ASSERT_NO_THROW(runNextTask(lpNonioQ, itemPagerTaskName()));
    ASSERT_NO_THROW(runPagingAdapterTask());

    // nothing left to do
    EXPECT_EQ(0, lpNonioQ.getReadyQueueSize());

    activeRR = getRRPercent(*store->getVBucket(activeVB));
    replicaRR = getRRPercent(*store->getVBucket(replicaVB));

    EXPECT_EQ(activeRR, 0);
    EXPECT_EQ(replicaRR, 0);
}

MATCHER_P(VBPtrVbidMatcher,
          vbid,
          "Check the provided VBucket pointer points to a vbucket with the "
          "given vbid") {
    return arg.getId() == vbid;
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
    auto pv = std::make_unique<MockItemPagingVisitor>(
            *store,
            stats,
            ItemEvictionStrategy::evict_everything(),
            pagerSemaphore,
            false,
            VBucketFilter(ephemeral() ? activeVBs : allVBs));

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
    // preparing for HT eviction. We then test that post-eviction the memory
    // usage is under the LWM. This test is not trivially runnable for magma
    // backends as magma has background threads that may allocate and
    // de-allocate memory during the course of the test. In particular, this
    // test encounters issues when being run under magma when a magma thread
    // allocates memory for something after paging which puts us back above
    // the LWM. These allocates are small for the main server (~kBs) but are a
    // significate portion of the test memory usage. These allocations appear to
    // scale to some extent with data size and we'd have to increase total
    // memory quota of this test by an unacceptable amount to make this work
    // consistently. Given that this test tests no KVStore specific
    // functionality, just skip the test for magma.
    if (hasMagma()) {
        GTEST_SKIP();
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

    auto pv = std::make_unique<MockItemPagingVisitor>(
            *store,
            stats,
            ItemEvictionStrategy::evict_everything(),
            pagerSemaphore,
            false,
            VBucketFilter(vbids));

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

TEST_P(STItemPagerTest, ItemPagerUpdatesMFUHistogram) {
    // TODO: decouple paging visitor from KVBucket, and move this test
    //       to vbucket_test.cc
    // verify that the paging visitor task correctly updates the MFU
    // histogram if it decreases the MFU of an item after visiting but
    // _not_ evicting it.

    // prepare for the test by storing an item
    auto& vbucket = *store->getVBucket(vbid);
    auto& ht = vbucket.ht;
    const auto& hist = ht.getEvictableMFUHistogram();
    ASSERT_TRUE(hist.empty());

    StoredDocKey key = makeStoredDocKey("key");
    auto item = make_item(vbid, key, "value");

    // Set a specific MFU for ease of testing
    auto startMFU = uint8_t(123);
    store->setInitialMFU(startMFU);
    ASSERT_EQ(cb::engine_errc::success, storeItem(item));
    flushAndExpelFromCheckpoints(vbid);

    // check that the item is indeed reflected in the mfu hist before
    // proceeding with this test
    ASSERT_EQ(1, hist[startMFU]);

    // set up a paging visitor
    auto pagerSemaphore = std::make_shared<cb::Semaphore>();
    pagerSemaphore->try_acquire(1);
    MockItemPagingVisitor visitor(
            *engine->getKVBucket(),
            engine->getEpStats(),
            // testing items which are visited but _not_ evicted so have their
            // MFU decreased - use a dummy strategy which does not try to
            // evict anything to simulate all items being over the target
            // MFU threshold.
            ItemEvictionStrategy::evict_nothing(),
            pagerSemaphore,
            false,
            VBucketFilter());

    visitor.setCurrentBucket(vbucket);

    // visit everthing in the HT
    HashTable::Position pos;
    while (pos != ht.endPosition()) {
        // keep visiting until done
        pos = ht.pauseResumeVisit(visitor, pos);
    }

    auto expectedMFU = uint8_t(startMFU - 1);

    // check that the MFU was decremented.
    EXPECT_EQ(0, hist[startMFU]);
    EXPECT_EQ(1, hist[expectedMFU]);
    EXPECT_EQ(1, hist.getNumberOfSamples());
}

TEST_P(STItemPagerTest, EligibleForEvictionAndExpiration) {
    if (!isPersistent()) {
        // Does not run under ephemeral -- OSVs are linked and that means we
        // cannot just force them into the HT, which this test does.
        GTEST_SKIP();
    }

    for (auto& sv : cb::testing::sv::createAll(makeStoredDocKey("key"))) {
        const auto now = gsl::narrow<uint32_t>(ep_real_time());
        // We do not expire deleted or temp items.
        bool expectToExpire =
                !sv->isDeleted() && !sv->isTempItem() && sv->isExpired(now);
        // We cannot evict and expire. We cannot evict dirty SVs.
        bool expectToEvict =
                !expectToExpire && !sv->isDirty() &&
                (!sv->isLocked(now) || (sv->isLocked(now) && sv->isResident()));
        // Under value eviction, we can only evict the value.
        // Deletes can also be removed entirely.
        if (store->getItemEvictionPolicy() == EvictionPolicy::Value) {
            expectToEvict &= sv->isResident() || sv->isDeleted();
        }
        // We only remove the SV from the HT during eviction if:
        // - it is a temp deleted or non-existent item
        // - under full-eviction, as part of eviction, but not if locked
        // - under value-eviction, if it is a delete
        bool expectToUnlink =
                sv->isTempDeletedItem() || sv->isTempNonExistentItem() ||
                (store->getItemEvictionPolicy() == EvictionPolicy::Value &&
                 expectToEvict && sv->isDeleted()) ||
                (store->getItemEvictionPolicy() == EvictionPolicy::Full &&
                 expectToEvict && !sv->isLocked(now));

        auto [expired, evicted, unlinked] = mockVisitSV(*sv);
        EXPECT_EQ(expectToExpire, expired) << *sv;
        EXPECT_EQ(expectToEvict, evicted) << *sv;
        EXPECT_EQ(expectToUnlink, unlinked) << *sv;
    }
}

TEST_P(STItemPagerTest, ItemPagerCannotRemoveKeyDuringWarmup) {
    if (!isPersistent()) {
        // Warmup only on persistent buckets.
        GTEST_SKIP();
    }

    using namespace ::testing;

    auto& epBucket = dynamic_cast<MockEPBucket&>(*engine->getKVBucket());
    EXPECT_CALL(epBucket, isWarmupLoadingData()).WillRepeatedly(Return(true));

    for (auto& sv : cb::testing::sv::createAll(makeStoredDocKey("key"))) {
        // We do not expire deleted or temp items.
        bool expectToExpire = !sv->isDeleted() && !sv->isTempItem() &&
                              sv->isExpired(ep_real_time());
        // We cannot evict and expire. We cannot evict dirty SVs.
        // We cannot evict non-resident items under full-eviction (metadata).
        bool expectToEvict =
                !expectToExpire && !sv->isDirty() && sv->isResident();
        // Under value eviction, we can only evict the value.
        // Deletes can also be removed entirely.
        if (store->getItemEvictionPolicy() == EvictionPolicy::Value) {
            expectToEvict &= sv->isResident() || sv->isDeleted();
        }
        // During warmup, we can only remove temp items from the HT. The
        // metadata for everything else must stay.
        bool expectToUnlink =
                sv->isTempDeletedItem() || sv->isTempNonExistentItem();

        auto [expired, evicted, unlinked] = mockVisitSV(*sv);
        EXPECT_EQ(expectToExpire, expired) << *sv;
        EXPECT_EQ(expectToEvict, evicted) << *sv;
        EXPECT_EQ(expectToUnlink, unlinked) << *sv;
    }
}

// Item pager should correctly handle all vbuckets being dead and should
// remain functional when there are non-dead vbuckets to evict from.
TEST_P(STItemPagerTest, MB65468_ItemPagerWithAllDeadVBuckets) {
    if (ephemeralFailNewData()) {
        // items are not evicted, so the ItemPager does not run.
        GTEST_SKIP();
    }
    if (isCrossBucketHtQuotaSharing()) {
        // Only applies to StrictItemPager
        GTEST_SKIP();
    }
    if (isEphemeralMemRecoveryEnabled()) {
        // Only test ItemPager directly
        GTEST_SKIP();
    }

    // Make active and populate above HWM so the ItemPager can
    // schedule it's paging visitor
    auto vbid0 = Vbid(0);
    auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);

    auto& stats = engine->getEpStats();
    auto aboveHWM = [&stats]() {
        return stats.getPreciseTotalMemoryUsed() > stats.mem_high_wat.load();
    };
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    populateVbsUntil(
            {vbid0}, aboveHWM, "key_", 256 /*valSize*/, 50 /*batchSize*/);

    // Make vbucket dead and run item pager
    // We cannot evict from a dead vbucket so the filter will be empty
    // and we should return without scheduling paging visitor
    setVBucketStateAndRunPersistTask(vbid0, vbucket_state_dead);
    store->wakeUpStrictItemPager(); // set manuallyNotified to true
    runNextTask(lpNonioQ, itemPagerTaskName());

    // Check that no paging visitor task/s are scheduled
    EXPECT_EQ(0, lpNonioQ.getReadyQueueSize());
    EXPECT_EQ(initialNonIoTasks, lpNonioQ.getFutureQueueSize())
            << "No paging visitor tasks should be scheduled when all vbuckets "
               "are dead";

    // Put vbucket back to active and wake the item pager
    setVBucketStateAndRunPersistTask(vbid0, vbucket_state_active);
    store->wakeUpStrictItemPager(); // set manuallyNotified to true
    runNextTask(lpNonioQ, itemPagerTaskName());

    // Memory usage is still above HWM - so paging visitor should be scheduled
    // Without the release of the semaphore, the paging visitor task will
    // not be created and scheduled and so we should throw here.
    ASSERT_NO_THROW(runPagingAdapterTask());
    // Back to initial task count
    EXPECT_EQ(initialNonIoTasks, lpNonioQ.getFutureQueueSize());
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
    size_t replica_count = populateUntilOOM(replica_vb);
    ASSERT_GE(replica_count, 10)
        << "Expected at least 10 replica items before hitting high watermark";

    // Flip vb 1 to be a replica (and hence should not be a candidate for
    // any paging out.
    store->setVBucketState(replica_vb, vbucket_state_replica);
    runHighMemoryPager();

    // Expected active vb behaviour depends on the full policy:
    if (engine->getConfiguration().getEphemeralFullPolicyString() ==
        "fail_new_data") {
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
 * Test fixture for Ephemeral item pager tests with
 * ephemeral_full_policy=auto_delete.
 */
class STEphemeralAutoDeleteItemPagerTest : public STItemPagerTest {};

// It is important that we hold the vbucket state lock during pageOut because
// otherwise we could end up deleting items from replicas in case of a poorly
// timed change in the vbucket state.
TEST_P(STEphemeralAutoDeleteItemPagerTest, PageOutHoldsVBStateLock) {
    // Populate the vbucket until we reach the high watermark.
    populateUntilAboveHighWaterMark(vbid);

    auto& vb = *store->getVBucket(vbid);
    bool softDeleteCalled{false};
    VBucketTestIntrospector::setSoftDeleteStoredValueHook(
            vb, {[&softDeleteCalled](folly::SharedMutex& vbStateLock) {
                softDeleteCalled = true;

                bool didManageToLock = vbStateLock.try_lock();
                if (didManageToLock) {
                    vbStateLock.unlock();
                }
                EXPECT_FALSE(didManageToLock);
            }});

    // Run the pager and check that items got deleted.
    runHighMemoryPager();
    EXPECT_TRUE(softDeleteCalled);
}

// MB-59368 was an issue where memory reached the threshold to trigger the
// pager (which will delete data), but code in the paging visitor was checking
// different values and stopped the pager. The pager would then just keep
// getting triggered leading to noticeable CPU increase and no memory reduction.
// Unfortunately the calculation of pageable watermarks is flawed and items may
// be deleted with plenty of spare memory (MB-64008). This test checks that we
// don't delete under the simple LWM when pageable mem used is above the
// pageable LWM.
TEST_P(STEphemeralAutoDeleteItemPagerTest, MB_64008) {
    // EphemeralMemRecovery utilises the ItemPager to delete items, so
    // running this test just on ItemPager alone is enough.
    if (isEphemeralMemRecoveryEnabled()) {
        GTEST_SKIP();
    }

    ASSERT_TRUE(itemPagerScheduled);

    // Need two vbuckets and one must be replica so that we account the replica
    // memory usage which will affect the result of getPageableMemCurrent
    const Vbid activeVbid = vbid;
    const Vbid replicaVbid(1);

    setVBucketState(replicaVbid, vbucket_state_replica);

    auto setReplica = [this](Vbid id, int count) {
        auto key = makeStoredDocKey("replica_" + std::to_string(count));
        auto item = make_item(id, key, "value");
        item.setCas(1 + count);
        uint64_t seqno;
        ASSERT_EQ(cb::engine_errc::success,
                  store->setWithMeta(item,
                                     0,
                                     &seqno,
                                     cookie,
                                     {vbucket_state_replica},
                                     CheckConflicts::No,
                                     true,
                                     GenerateBySeqno::Yes,
                                     GenerateCas::Yes));
    };

    auto setActive = [this](Vbid id, int count) {
        auto key = makeStoredDocKey("active_" + std::to_string(count));
        auto item = make_item(id, key, "value");
        storeItem(item);
    };

    auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);
    auto checkNotScheduling = [this, &lpNonioQ]() {
        ASSERT_TRUE(itemPagerScheduled);
        ASSERT_EQ(0, lpNonioQ.getReadyQueueSize());
        ASSERT_EQ(initialNonIoTasks, lpNonioQ.getFutureQueueSize());
        try {
            runNextTask(lpNonioQ, "Paging out items.");
            FAIL() << "ItemPager did run";
        } catch (const std::logic_error& ex) {
            ASSERT_STREQ("CheckedExecutor failed fetchNextTask", ex.what());
        }
    };
    auto checkNotDeleting = [this, &lpNonioQ]() {
        ASSERT_TRUE(itemPagerScheduled);
        ASSERT_EQ(0, lpNonioQ.getReadyQueueSize());
        ASSERT_EQ(initialNonIoTasks, lpNonioQ.getFutureQueueSize());
        // Item pager consists of two Tasks - the parent ItemPager task,
        // and then a task (via VCVBAdapter) to process each vBucket (which
        // there is just one of as we only have one vBucket online).
        runNextTask(lpNonioQ, "Paging out items.");
        ASSERT_EQ(0, lpNonioQ.getReadyQueueSize());
        ASSERT_EQ(initialNonIoTasks, lpNonioQ.getFutureQueueSize())
                << "VBucket visitor scheduled";
    };

    auto& stats = engine->getEpStats();
    int count = 0;
    do {
        setReplica(replicaVbid, count);
        setActive(activeVbid, count);
        ++count;
    } while (store->getPageableMemCurrent() <=
             store->getPageableMemHighWatermark());
    // This is the condition which produced the issue in MB-59368,
    // but we actually don't want to delete under LWM as in MB-64008.
    ASSERT_LE(stats.getEstimatedTotalMemoryUsed(), stats.mem_low_wat);
    checkNotScheduling();
    store->wakeItemPager();
    checkNotDeleting();

    do {
        setReplica(replicaVbid, count);
        setActive(activeVbid, count);
        ++count;
    } while (stats.getEstimatedTotalMemoryUsed() <= stats.mem_low_wat ||
             store->getPageableMemCurrent() <=
                     store->getPageableMemHighWatermark());
    checkNotScheduling();
    store->wakeItemPager();
    checkNotDeleting();

    auto& vb = *store->getVBucket(activeVbid);
    bool softDeleteCalled{false};
    VBucketTestIntrospector::setSoftDeleteStoredValueHook(
            vb, {[&softDeleteCalled](folly::SharedMutex& vbStateLock) {
                softDeleteCalled = true;
            }});

    do {
        // Write the active last as the path it executes will do the memory
        // check that schedules the pager.
        setReplica(replicaVbid, count);
        setActive(activeVbid, count);
        ++count;
    } while (stats.getEstimatedTotalMemoryUsed() <= stats.mem_high_wat);
    // Run the pager and check that items got deleted.
    runHighMemoryPager();
    EXPECT_TRUE(softDeleteCalled) << "Nothing was deleted";
}

// Dead vBuckets should not be counted as pageable.
TEST_P(STEphemeralAutoDeleteItemPagerTest, MB_64092) {
    setVBucketState(Vbid(1), vbucket_state_active);
    double prevPageableHWM = store->getPageableMemHighWatermark();
    double prevPageableLWM = store->getPageableMemLowWatermark();
    setVBucketState(Vbid(1), vbucket_state_dead);
    EXPECT_LT(store->getPageableMemHighWatermark() / prevPageableHWM, 0.6);
    EXPECT_LT(store->getPageableMemLowWatermark() / prevPageableLWM, 0.6);
}

// Test covers MB-60046. A pending and committed key must not find itself auto
// deleted, else the delete of the commit will be rejected by the DCP replica
// and cause disconnects.
TEST_P(STEphemeralAutoDeleteItemPagerTest, MB_60046) {
    setVBucketState(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    // Register a cursor now to hold data in checkpoints for validation
    auto& vb = *store->getVBucket(vbid);
    auto cursorResult = vb.checkpointManager->registerCursorBySeqno(
            "test", 1, CheckpointCursor::Droppable::No);
    ASSERT_FALSE(cursorResult.tryBackfill) << *vb.checkpointManager;
    auto cursor = cursorResult.takeCursor().lock();

    // Two functions for writing active and replica keys.
    auto setActive = [this](int count) {
        auto item =
                make_item(vbid,
                          makeStoredDocKey("active_" + std::to_string(count)),
                          "value");
        item.setFreqCounterValue(0);
        storeItem(item);
    };

    int count = 0;
    setActive(count);
    auto key = makeStoredDocKey("active_" + std::to_string(count));
    // Now make the key written also pending
    ASSERT_EQ(cb::engine_errc::would_block,
              storeItem(*makePendingItem(key, "pending-value")));
    // One more item
    setActive(++count);
    EXPECT_GE(vb.getNumItems(), 2);

    // Process the current queue before we generate the deletes
    std::vector<queued_item> items;
    vb.checkpointManager->getItemsForCursor(*cursor,
                                            items,
                                            std::numeric_limits<size_t>::max(),
                                            std::numeric_limits<size_t>::max());
    EXPECT_NE(0, items.size()) << *vb.checkpointManager;
    bool pendingFound{false};
    bool committedFound{false};
    for (const auto& item : items) {
        if (item->getKey() == key) {
            if (item->isPending()) {
                ASSERT_FALSE(pendingFound);
                pendingFound = true;
            } else {
                ASSERT_FALSE(committedFound);
                committedFound = true;
            }
        }
    }
    // Both pending and committed must be seen before we trigger auto-delete
    ASSERT_TRUE(pendingFound);
    ASSERT_TRUE(committedFound);
    items.clear();

    // Now trigger the auto-delete paging. Lower the quota and try to write 1
    // more key.
    engine->setMaxDataSize(engine->getEpStats().getPreciseTotalMemoryUsed());
    setActive(++count);
    runHighMemoryPager();

    // Iterate through the checkpoint and check all deletes.
    vb.checkpointManager->getItemsForCursor(*cursor,
                                            items,
                                            std::numeric_limits<size_t>::max(),
                                            std::numeric_limits<size_t>::max());
    // Should be some new mutations (deletes)
    EXPECT_NE(0, items.size()) << *vb.checkpointManager;
    int deleteCount{0};
    for (const auto& item : items) {
        if (item->getKey() == key) {
            // The key should not be found in the new batch of items. The
            // original commit+pending were in the first batch and not expected
            // to see again. Prior to the fix, a delete(key) was seen here.
            FAIL() << *item;
        }
        if (item->isDeleted()) {
            ++deleteCount;
        }
    }
    // We certainly should see deletes.
    ASSERT_NE(0, deleteCount);
}

/**
 * Test fixture for expiry pager tests - enables the Expiry Pager (in addition
 * to what the parent class does).
 */
class STExpiryPagerTest : public STBucketQuotaTest {
protected:
    void SetUp() override {
        if (!config_string.empty()) {
            config_string += ";";
        }
        // Disable pausing mid-vbucket by default
        // enable expiry pager - this adds one to the expected number of nonIO
        // tasks
        config_string +=
                "paging_visitor_pause_check_count=1;"
                "exp_pager_enabled=true";
        ++initialNonIoTasks;
        STBucketQuotaTest::SetUp();

        // Sanity check - should be no nonIO tasks ready to run, and initial
        // count in futureQ.
        auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);
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
        auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);
        runNextTask(lpNonioQ, "Paging expired items.");
        EXPECT_EQ(0, lpNonioQ.getReadyQueueSize());
        auto pagers = std::min(
                engine->getKVBucket()->getVBuckets().getBuckets().size(),
                engine->getConfiguration().getExpiryPagerConcurrency());
        EXPECT_EQ(initialNonIoTasks + pagers, lpNonioQ.getFutureQueueSize());
        for (size_t i = 0; i < pagers; ++i) {
            runNextTask(lpNonioQ, "Expired item remover no vbucket assigned");
        }
        EXPECT_EQ(0, lpNonioQ.getReadyQueueSize());
        EXPECT_EQ(initialNonIoTasks, lpNonioQ.getFutureQueueSize());
    }

    void expiredItemsDeleted();
};

void STExpiryPagerTest::expiredItemsDeleted() {
    // Populate bucket with three documents - one with no expiry, one with an
    // expiry in 10 seconds, and one with an expiry in 20 seconds.
    std::string value = createXattrValue("body");
    for (uint32_t ii = 0; ii < 3; ii++) {
        auto key = makeStoredDocKey("key_" + std::to_string(ii));
        auto item = make_item(
                vbid,
                key,
                value,
                ii > 0 ? ep_convert_to_expiry_time(ii * 10) : 0,
                PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR);
        ASSERT_EQ(cb::engine_errc::success, storeItem(item));
    }

    flushDirectlyIfPersistent(vbid, {MoreAvailable::No, 3});

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
    flushDirectlyIfPersistent(vbid, {MoreAvailable::No, 1});

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

    if (fullEviction()) {
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
    flushDirectlyIfPersistent(vbid, {MoreAvailable::No, 1});

    // Should only be 1 item remaining.
    EXPECT_EQ(1, engine->getVBucket(vbid)->getNumItems());

    // Check our items.
    EXPECT_EQ(cb::engine_errc::success, getKeyFn(key_0))
            << "Key without TTL should still exist.";

    EXPECT_EQ(cb::engine_errc::no_such_key, getKeyFn(key_1))
            << "Key with TTL:10 should be removed.";

    if (fullEviction()) {
        // Need an extra get() to trigger EWOULDBLOCK / bgfetch.
        EXPECT_EQ(cb::engine_errc::would_block, getKeyFn(key_2));
        runBGFetcherTask();
    }
    EXPECT_EQ(cb::engine_errc::no_such_key, getKeyFn(key_2))
            << "Key with TTL:20 should be removed.";
}

TEST_P(STExpiryPagerTest, MaxVisitorDuration) {
    auto& stats = engine->getEpStats();
    ExpiredItemPager pager(*engine, stats, 0, 0, 1);

    stats.setMaxDataSize(0);
    EXPECT_EQ(55, countMilliseconds(pager.maxExpectedVisitorDuration()));

    stats.setMaxDataSize(10_GiB);
    EXPECT_EQ(55, countMilliseconds(pager.maxExpectedVisitorDuration()));

    stats.setMaxDataSize(100_GiB);
    EXPECT_EQ(235, countMilliseconds(pager.maxExpectedVisitorDuration()));
}

// Test that when the expiry pager runs, all expired items are deleted.
TEST_P(STExpiryPagerTest, ExpiredItemsDeleted) {
    expiredItemsDeleted();
}

// Test that when an expired system-xattr document is fetched with getMeta
// it can be successfully expired again
TEST_P(STExpiryPagerTest, MB_25650) {
    if (isNexus()) {
        // TODO MB-53859: Re-enable under nexus once couchstore datatype
        // on KEYS_ONLY is consistent with magma
        GTEST_SKIP();
    }
    expiredItemsDeleted();

    auto vb = store->getVBucket(Vbid(0));

    auto key_1 = makeStoredDocKey("key_1");
    ItemMetaData metadata;
    uint32_t deleted;
    uint8_t datatype;

    // Ephemeral doesn't bgfetch, persistent full-eviction already had to
    // perform a bgfetch to check key_1 no longer exists in the above
    // expiredItemsDeleted().
    const cb::engine_errc err = persistent() && !fullEviction()
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
    EXPECT_TRUE(cb::mcbp::datatype::is_xattr(item.item->getDataType()));
    ASSERT_NE(0, item.item->getNBytes());
    cb::xattr::Blob blob(
            {const_cast<char*>(item.item->getData()), item.item->getNBytes()},
            false);

    EXPECT_EQ(0, blob.get("user").size());
    EXPECT_EQ(0, blob.get("meta").size());
    ASSERT_NE(0, blob.get("_sync").size());
    EXPECT_EQ("{\"cas\":\"0xdeadbeefcafefeed\"}", blob.get("_sync"));
}

// Test that when an expired system-xattr document is fetched with getMeta
// deleteWithMeta can be successfully invoked
TEST_P(STExpiryPagerTest, MB_25671) {
    if (isNexus()) {
        // TODO MB-53859: Re-enable under nexus once couchstore datatype
        // on KEYS_ONLY is consistent with magma
        GTEST_SKIP();
    }
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
    const cb::engine_errc err = persistent() && !fullEviction()
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

    uint64_t cas = 0;
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
                                     DeleteSource::Explicit,
                                     EnforceMemCheck::Yes);
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
    EXPECT_EQ("{\"cas\":\"0xdeadbeefcafefeed\"}", blob.get("_sync"));
    EXPECT_EQ(metadata.flags, item.item->getFlags());
    EXPECT_EQ(metadata.exptime, item.item->getExptime());
    EXPECT_EQ(metadata.cas, item.item->getCas());
    EXPECT_EQ(metadata.revSeqno, item.item->getRevSeqno());
}

TEST_P(STExpiryPagerTest, ProcessExpiredListBeforeVisitingHashTable) {
    const auto quota = engine->getEpStats().getMaxDataSize();
    increaseQuota(quota);

    auto& config = engine->getConfiguration();
    // Force immediate yield by setting duration limits to 0
    config.setExpiryVisitorItemsOnlyDurationMs(0);
    config.setExpiryVisitorExpireAfterVisitDurationMs(0);

    // Store items with an expiry time of now + 10s
    const auto expiry = ep_convert_to_expiry_time(10);
    for (size_t i = 0; i < 102; i++) {
        auto key = makeStoredDocKey("key_" + std::to_string(i + 1));
        auto item = make_item(
                vbid,
                key,
                createXattrValue("value_" + std::to_string(i + 1)),
                expiry,
                PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR);
        ASSERT_EQ(cb::engine_errc::success, storeItem(item));
    }
    flushDirectlyIfPersistent(vbid, {MoreAvailable::No, 102});
    EXPECT_EQ(102, engine->getVBucket(vbid)->getNumItems())
            << "Should begin with 102 items in the bucket";

    // Time-travel and initialise the visitor, this will pick up a future time
    // ahead of the item expiry
    TimeTraveller tedTheodoreLogan(100);
    auto expirySemaphore = std::make_shared<cb::Semaphore>();
    MockExpiredPagingVisitor expiryVisitor(*store,
                                           engine->getEpStats(),
                                           expirySemaphore,
                                           true,
                                           VBucketFilter());
    EXPECT_CALL(expiryVisitor, visitBucket(testing::_)).Times(3);

    // First run of expiry pager should accumulate expired items and pause after
    // expiring 101 items. This is because we have
    // "visit_bucket_and_expire_items_duration_limit=0" and the ProgressTracker
    // required a minimum of 100 + 1 items visited before it will consider
    // yielding.
    auto vbucket = store->getVBucket(vbid);
    expiryVisitor.visitBucket(*vbucket);
    EXPECT_EQ(102, expiryVisitor.getExpiredItems().size());
    expiryVisitor.complete();
    flushDirectlyIfPersistent(vbid, {MoreAvailable::No, 101});

    // verify 1 key is left
    EXPECT_EQ(1, expiryVisitor.getExpiredItems().size());
    EXPECT_EQ(1, engine->getVBucket(vbid)->getNumItems());

    // Add a new key to the bucket that needs expiring
    auto key = makeStoredDocKey("key_103");
    auto value = createXattrValue("value_103");
    auto item = make_item(
            vbid,
            key,
            value,
            expiry,
            PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR);
    ASSERT_EQ(cb::engine_errc::success, storeItem(item));
    flushDirectlyIfPersistent(vbid, {MoreAvailable::No, 1});
    EXPECT_EQ(2, engine->getVBucket(vbid)->getNumItems());

    // This time when we wake up the expiry pager, because the expired list
    // already contain an item, it should only process this expiry and not visit
    // the hashtable to find key_103
    expiryVisitor.visitBucket(*vbucket);
    EXPECT_TRUE(expiryVisitor.getExpiredItems().empty());
    expiryVisitor.complete();

    flushDirectlyIfPersistent(vbid, {MoreAvailable::No, 1});
    EXPECT_EQ(1, engine->getVBucket(vbid)->getNumItems())
            << "key_103 should not be expired";

    // The final run will visit the hashtable and expire key_103
    expiryVisitor.visitBucket(*vbucket);
    EXPECT_EQ(1, expiryVisitor.getExpiredItems().size());

    expiryVisitor.complete();
    flushDirectlyIfPersistent(vbid, {MoreAvailable::No, 1});
    EXPECT_TRUE(expiryVisitor.getExpiredItems().empty());
    EXPECT_EQ(0, engine->getVBucket(vbid)->getNumItems());
}

// Test that when the expired list is not empty and the visitor has to sleep
// and then wake up, it will process/delete the expired items.
// This also tests that the expired list is still processed even if there are
// no more hash table visits required.
TEST_P(STExpiryPagerTest, MB_67191) {
    const auto quota = engine->getEpStats().getMaxDataSize();
    increaseQuota(quota);

    auto& config = engine->getConfiguration();
    // Force immediate yield by setting duration limits to 0
    config.setExpiryVisitorItemsOnlyDurationMs(0);
    config.setExpiryVisitorExpireAfterVisitDurationMs(0);

    // Store 203 items with an expiry time
    const auto expiry = ep_convert_to_expiry_time(10);
    for (size_t i = 0; i < 203; i++) {
        auto key = makeStoredDocKey("key_" + std::to_string(i + 1));
        auto item = make_item(
                vbid,
                key,
                createXattrValue("value_" + std::to_string(i + 1)),
                expiry,
                PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR);
        ASSERT_EQ(cb::engine_errc::success, storeItem(item));
    }
    flushDirectlyIfPersistent(vbid, {MoreAvailable::No, 203});
    EXPECT_EQ(203, engine->getVBucket(vbid)->getNumItems())
            << "Should begin with 203 items in the bucket";
    TimeTraveller tedTheodoreLogan(100);
    store->wakeUpExpiryPager();
    auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);
    runNextTask(lpNonioQ, "Paging expired items.");
    EXPECT_EQ(0, lpNonioQ.getReadyQueueSize());
    auto pagers = 1;
    EXPECT_EQ(initialNonIoTasks + pagers, lpNonioQ.getFutureQueueSize());

    // First run of ExpiryPagingVisitor should accumulate expired items and
    // pause after expiring 101 items. This is because we have
    // "visit_bucket_and_expire_items_duration_limit=0" and the ProgressTracker
    // required a minimum of 100 + 1 items visited before it will consider
    // yielding.
    runNextTask(lpNonioQ, "Expired item remover no vbucket assigned");
    flushDirectlyIfPersistent(vbid, {MoreAvailable::No, 101});
    // Verify 102 items are left
    EXPECT_EQ(102, engine->getVBucket(vbid)->getNumItems());
    // Visitor reschedules itself
    EXPECT_EQ(initialNonIoTasks + 1, lpNonioQ.getFutureQueueSize());

    // Second run of ExpiryPagingVisitor should also expire 101
    runNextTask(lpNonioQ, "Expired item remover on vb:0");
    flushDirectlyIfPersistent(vbid, {MoreAvailable::No, 101});
    EXPECT_EQ(1, engine->getVBucket(vbid)->getNumItems()); // 1 item left
    // Visitor reschedules itself
    EXPECT_EQ(initialNonIoTasks + 1, lpNonioQ.getFutureQueueSize());

    // Last run of ExpiryPagingVisitor should expire the last item
    runNextTask(lpNonioQ, "Expired item remover on vb:0");
    flushDirectlyIfPersistent(vbid, {MoreAvailable::No, 1});

    // No more tasks scheduled to run
    EXPECT_EQ(0, lpNonioQ.getReadyQueueSize());
    EXPECT_EQ(initialNonIoTasks, lpNonioQ.getFutureQueueSize());
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

    for (Vbid::id_type i = 0; i < 18; i++) {
        allVBs[i] = Vbid(i);
    }

    for (Vbid::id_type i = 0; i < 6; i++) {
        setVBucketStateAndRunPersistTask(Vbid(i), vbucket_state_active);
        setVBucketStateAndRunPersistTask(Vbid(i + 6), vbucket_state_pending);
        setVBucketStateAndRunPersistTask(Vbid(i + 12), vbucket_state_replica);
    }

    if (engine->getConfiguration().getBucketTypeString() == "persistent") {
        // flush all vbs
        for (const auto& vbid : allVBs) {
            dynamic_cast<EPBucket&>(*store).flushVBucket(vbid);
        }
    }

    auto& stats = engine->getEpStats();
    auto pagerSemaphore = std::make_shared<cb::Semaphore>();

    auto pv = std::make_unique<MockItemPagingVisitor>(
            *store,
            stats,
            ItemEvictionStrategy::evict_everything(),
            pagerSemaphore,
            false,
            VBucketFilter(allVBs));

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
                                                     : vbucket_state_replica,
                                             {},
                                             TransferVB::Yes));
        }

        return res;
    };

    // if the vbucket comparator is "safe" and checks the state once, this
    // sort will work as expected - otherwise it _may_ segfault.
    // Note: this is not a robust test, variations in the impl of sort
    // may mean this does _not_ crash even with an unacceptable comparator
    std::ranges::sort(allVBs, comparator);

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

    if (ephemeralFailNewData()) {
        // items are not auto-deleted, so the ItemPager does not run.
        GTEST_SKIP();
    }
    if (isEphemeralMemRecoveryEnabled()) {
        // The test is not applicable when EphemeralMemRecovery enabled as
        // only ItemPager was affected (which is utilised by EphMemRec).
        GTEST_SKIP();
    }

    // No need to run under magma/nexus:
    //   - The test has been spotted quite fragile multiple times (eg, magma).
    //     It makes assumptions on the mem state of the system after persistence
    //     but that state varies depending on the storage
    //   - The test covers in-memory behaviour and doesn't care about the
    //     particular storage used, so we can just keep the couchstore/magma
    //     versions for persistence, plus ephemeral.
    if (hasMagma()) {
        GTEST_SKIP();
    }

    // Need a slightly higher quota here
    increaseQuota(800_KiB);

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // this triggers eviction, scheduling the ItemPager task
    auto itemCount = populateUntilAboveHighWaterMark(vbid);
    EXPECT_LT(0, itemCount);

    if (isPersistent()) {
        flushVBucket(vbid);
    }

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

        // Deleting items doesn't free much memory and we'd need a big quota to
        // be able to reclaim the difference between low and high watermarks
        // just by deleting items. We can also flush persistent VBuckets and
        // reduce the checkpoint memory usage by freeing closed checkpoints.
        // Doing this allows us to use a significantly lower quota which reduces
        // test time significantly.
        //
        // Note: Doing this step within the loop for avoiding deletions being
        // failed by high CM mem-usage
        auto vb = store->getVBucket(vbid);
        vb->checkpointManager->createNewCheckpoint();
        flushVBucketToDiskIfPersistent(vbid, 1);
        runCheckpointDestroyer(vbid);
    }

    auto& stats = engine->getEpStats();
    // confirm we are now below the low watermark, and can test the item pager
    // behaviour
    ASSERT_LT(stats.getEstimatedTotalMemoryUsed(), stats.mem_low_wat.load());

    auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);
    // run the item pager. It should _not_ create and schedule a PagingVisitor
    runNextTask(lpNonioQ, itemPagerTaskName());

    EXPECT_EQ(0, lpNonioQ.getReadyQueueSize());
    EXPECT_EQ(initialNonIoTasks, lpNonioQ.getFutureQueueSize());

    // populate again, and confirm this time that the item pager does shedule
    // a paging visitor
    populateUntilAboveHighWaterMark(vbid);

    runNextTask(lpNonioQ, itemPagerTaskName());
    runPagingAdapterTask();
}

/**
 * Subclass for expiry tests only applicable to Value eviction persistent
 * buckets.
 */
class STValueEvictionExpiryPagerTest : public STExpiryPagerTest {
public:
    static auto configValues() {
#ifdef EP_USE_MAGMA
        return (magmaBucket() | couchstoreBucket()) * valueOnlyEvictionPolicy();
#else
        return couchstoreBucket() * valueOnlyEvictionPolicy();
#endif
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
            ep_convert_to_expiry_time(10),
            PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR);
    ASSERT_EQ(cb::engine_errc::success, storeItem(item));

    flushDirectlyIfPersistent(vbid, {MoreAvailable::No, 1});

    const char* msg;
    EXPECT_EQ(cb::engine_errc::success, store->evictKey(key, vbid, &msg));
    EXPECT_STREQ("Ejected.", msg);

    runBGFetcherTask();

    TimeTraveller docBrown(15);

    wakeUpExpiryPager();
    flushDirectlyIfPersistent(vbid, {MoreAvailable::No, 1});
}

// Test that expiring a non-resident item works (and item counts are correct).
//
// (Not applicable to full-eviction as relies on in-memory expiry pager
//  expiring a non-resident item; with full-eviction the item is completely
//  evicted and hence ExpiryPager won't find it.)
TEST_P(STValueEvictionExpiryPagerTest, MB_25991_ExpiryNonResident) {
    // Populate bucket with a TTL'd document, and then evict that document.
    auto key = makeStoredDocKey("key");
    auto item = make_item(vbid, key, "value", ep_convert_to_expiry_time(5));
    ASSERT_EQ(cb::engine_errc::success, storeItem(item));

    flushDirectlyIfPersistent(vbid, {MoreAvailable::No, 1});

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
    flushDirectlyIfPersistent(vbid, {MoreAvailable::No, 1});

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
        store->processCompressionModeChange();
        initialNonIoTasks++;
    }

    void runItemCompressor() {
        auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);
        runNextTask(lpNonioQ, "Item Compressor");
    }
};

// Test that an xattr value which is compressed, evicted and then expired
// doesn't trigger an exception
TEST_P(MB_32669, expire_a_compressed_and_evicted_xattr_document) {
    // 1) Add bucket a TTL'd xattr document
    auto key = makeStoredDocKey("key");
    auto expiry = ep_convert_to_expiry_time(5);
    auto value = createXattrValue(std::string(100, 'a'), true /*sys xattrs*/);
    auto item =
            make_item(vbid, key, value, expiry, PROTOCOL_BINARY_DATATYPE_XATTR);
    ASSERT_EQ(cb::engine_errc::success, storeItem(item));

    flushDirectlyIfPersistent(vbid, {MoreAvailable::No, 1});

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

    flushDirectlyIfPersistent(vbid, {MoreAvailable::No, 1});

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
    using namespace std::string_view_literals;
    const auto cas_str = R"({"cas":"0xdeadbeefcafefeed"})"sv;
    const auto sync_str = new_blob.get("_sync");

    EXPECT_EQ(cas_str, sync_str) << "Unexpected system xattrs";
    EXPECT_TRUE(new_blob.get("user").empty())
            << "The user attribute should be gone";
    EXPECT_TRUE(new_blob.get("meta").empty())
            << "The meta attribute should be gone";
}

class MB_36087 : public STParameterizedBucketTest {};

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
            engine->storeInner(*cookie, item, cas, StoreSemantics::Set, false));

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
                                     DeleteSource::Explicit,
                                     EnforceMemCheck::Yes);
    };
    // A bgfetch is required for full or value eviction because we need the
    // xattr value
    EXPECT_EQ(cb::engine_errc::would_block, deleteWithMeta());
    runBGFetcherTask();

    // Full eviction first did a meta-fetch, now has todo a full fetch
    auto err = fullEviction() ? cb::engine_errc::would_block
                              : cb::engine_errc::success;
    EXPECT_EQ(err, deleteWithMeta());

    if (fullEviction()) {
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
    EXPECT_EQ("{\"cas\":\"0xdeadbeefcafefeed\"}", blob.get("_sync"));
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
        setNumConcurrentExpiryPagers(4);
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

    auto i = 0;
    // items need to be persisted to be evicted, but flushing after each item
    // would be slow. Load to the HWM, then flush, then repeat if we dropped
    // below the HWM
    while (!aboveHWM()) {
        populateVbsUntil(
                vbids, aboveHWM, "keys_" + std::to_string(i++) + "_", 500);
        // populateVbsUntil flushes and removes checkpoints which is required
        // as eviction will not touch dirty items
    }

    // make sure the item pager has been notified while we're above the HWM
    store->attemptToFreeMemory();

    auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);
    ASSERT_EQ(0, lpNonioQ.getReadyQueueSize());
    ASSERT_EQ(initialNonIoTasks, lpNonioQ.getFutureQueueSize());

    // Run the parent ItemPager task, and then N PagingVisitor tasks
    runNextTask(lpNonioQ, itemPagerTaskName());
    ASSERT_EQ(0, lpNonioQ.getReadyQueueSize());

    auto numConcurrentPagers = engine->getConfiguration().getConcurrentPagers();
    ASSERT_EQ(initialNonIoTasks + numConcurrentPagers,
              lpNonioQ.getFutureQueueSize());

    for (size_t p = 0; p < numConcurrentPagers; ++p) {
        runPagingAdapterTask();
    }
}

/**
 * Test that the ExpiryPager correctly creates multiple PagingVisitors
 * as configured.
 */
TEST_P(MultiPagingVisitorTest, ExpiryPagerCreatesMultiplePagers) {
    std::vector<Vbid> vbids{Vbid(0), Vbid(1), Vbid(2), Vbid(3)};

    for (const auto& vbid : vbids) {
        store->setVBucketState(vbid, vbucket_state_active);
    }

    auto& stats = engine->getEpStats();
    // expiry in the past
    const auto expiry = ep_convert_to_expiry_time(10);
    std::string value = "foobar";

    for (Vbid::id_type i = 0; i < 4; ++i) {
        auto key = makeStoredDocKey("key_" + std::to_string(i));
        auto item = make_item(Vbid(i), key, value, expiry);
        ASSERT_EQ(cb::engine_errc::success, storeItem(item));
    }

    TimeTraveller tedTheodoreLogan(100);
    store->enableExpiryPager();
    store->wakeUpExpiryPager();

    // enabling the expiry pager schedules another task

    initialNonIoTasks++;

    auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);
    ASSERT_EQ(0, lpNonioQ.getReadyQueueSize());
    ASSERT_EQ(initialNonIoTasks, lpNonioQ.getFutureQueueSize());

    // Run the parent ItemPager task, and then N PagingVisitor tasks
    runNextTask(lpNonioQ, "Paging expired items.");
    ASSERT_EQ(0, lpNonioQ.getReadyQueueSize());

    auto numConcurrentPagers =
            engine->getConfiguration().getExpiryPagerConcurrency();
    EXPECT_EQ(initialNonIoTasks + numConcurrentExpiryPagers,
              lpNonioQ.getFutureQueueSize());

    for (size_t i = 0; i < numConcurrentPagers; ++i) {
        runNextTask(lpNonioQ, "Expired item remover no vbucket assigned");
        // each task should only visit one of the vbuckets, and there's
        // one expired item per vbucket - expired count should increase by one
        EXPECT_EQ(i + 1, stats.expired_pager);
    }
}

/**
 * Test fixture with bloom filter disabled.
 */
class STNoBloomFilterExpiryPagerTest : public STExpiryPagerTest {
public:
    void SetUp() override {
        if (!config_string.empty()) {
            config_string += ";";
        }
        config_string += "bfilter_enabled=false";
        STExpiryPagerTest::SetUp();
    }
    void testTempItemCleanUp(StoredDocKey key, bool expectCleanup);
};

void STNoBloomFilterExpiryPagerTest::testTempItemCleanUp(StoredDocKey key,
                                                         bool expectCleanup) {
    auto vb = store->getVBucket(vbid);
    ASSERT_EQ(1, vb->getNumTempItems());
    ASSERT_TRUE(vb);
    wakeUpExpiryPager();
    auto& stats = engine->getEpStats();
    ASSERT_EQ(1, stats.expiryPagerRuns);
    EXPECT_EQ(0, stats.expired_pager);
    EXPECT_EQ(0, vb->numExpiredItems);

    if (expectCleanup) {
        EXPECT_EQ(0, vb->getNumTempItems());
        // Item is gone, all is good
        auto htRes = vb->ht.findForUpdate(key);
        EXPECT_FALSE(htRes.committed);
        EXPECT_FALSE(htRes.pending);
    } else {
        EXPECT_EQ(1, vb->getNumTempItems());
        // Item is still in the HT
        auto htRes = vb->ht.findForUpdate(key);
        EXPECT_TRUE(htRes.committed);
        EXPECT_FALSE(htRes.pending);
    }
}

TEST_P(STNoBloomFilterExpiryPagerTest, TempInitialNotCleanUpNotExpired) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto key = makeStoredDocKey("key");

    ItemMetaData metadata;
    uint32_t deleted;
    uint8_t datatype;
    // GetMeta for a non-existent document
    EXPECT_EQ(
            cb::engine_errc::would_block,
            store->getMetaData(key, vbid, cookie, metadata, deleted, datatype));

    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    // Verify that the item is TempInitial (bgfetch incomplete)
    {
        auto htRes = vb->ht.findForUpdate(key);
        ASSERT_TRUE(htRes.committed);
        ASSERT_TRUE(htRes.committed->isTempInitialItem());
        EXPECT_FALSE(htRes.pending);
    }
    // And proceed with the test
    testTempItemCleanUp(key, false);
}

TEST_P(STNoBloomFilterExpiryPagerTest, TempNonExistentCleanedUpNotExpired) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    // Store the item to trigger a metadata bg fetch, running it will create a
    // TempNonExistent item
    auto key = makeStoredDocKey("key");

    ItemMetaData metadata;
    uint32_t deleted;
    uint8_t datatype;
    // GetMeta for a non-existent document
    EXPECT_EQ(
            cb::engine_errc::would_block,
            store->getMetaData(key, vbid, cookie, metadata, deleted, datatype));
    // Allow the BGFetch to complete
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
    testTempItemCleanUp(key, true);
}

TEST_P(STNoBloomFilterExpiryPagerTest, TempDeletedCleanedUpNotExpired) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    // Store a deleted item so that an add driven bg fetch will restore
    // deleted meta (setting the previously TempInitial item to TempDeleted)
    auto key = makeStoredDocKey("key");
    auto item = makeDeletedItem(key);
    EXPECT_EQ(cb::engine_errc::success, store->set(*item, cookie));
    flushVBucketToDiskIfPersistent(vbid, 1);
    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);

    ItemMetaData metadata;
    uint32_t deleted;
    uint8_t datatype;
    EXPECT_EQ(
            cb::engine_errc::would_block,
            store->getMetaData(key, vbid, cookie, metadata, deleted, datatype));
    runBGFetcherTask();
    // Verify that the item is TempDeleted
    {
        auto htRes = vb->ht.findForUpdate(key);
        ASSERT_TRUE(htRes.committed);
        ASSERT_TRUE(htRes.committed->isTempDeletedItem());
        EXPECT_FALSE(htRes.pending);
    }
    // And proceed with the test
    testTempItemCleanUp(key, true);
}

// TODO: Ideally all of these tests should run with or without jemalloc,
// however we currently rely on jemalloc for accurate memory tracking; and
// hence it is required currently.
#if defined(HAVE_JEMALLOC)

INSTANTIATE_TEST_SUITE_P(
        EphemeralOrPersistent,
        STItemPagerTest,
        STItemPagerTest::allConfigValuesAllEvictionStrategiesNoNexus(),
        STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(EphemeralOrPersistent,
                         MultiPagingVisitorTest,
                         STParameterizedBucketTest::persistentConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(EphemeralOrPersistent,
                         STExpiryPagerTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(Persistent,
                         STNoBloomFilterExpiryPagerTest,
                         STParameterizedBucketTest::persistentConfigValues(),
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

INSTANTIATE_TEST_SUITE_P(EphemeralAutoDelete,
                         STEphemeralAutoDeleteItemPagerTest,
                         STParameterizedBucketTest::ephAutoDeleteConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(PersistentFullValue,
                         MB_36087,
                         STParameterizedBucketTest::persistentConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

#else
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(STItemPagerTest);
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(MultiPagingVisitorTest);
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(STExpiryPagerTest);
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(STNoBloomFilterExpiryPagerTest);
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(STValueEvictionExpiryPagerTest);
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(MB_32669);
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(STEphemeralItemPagerTest);
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(STEphemeralAutoDeleteItemPagerTest);
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(MB_36087);
#endif
