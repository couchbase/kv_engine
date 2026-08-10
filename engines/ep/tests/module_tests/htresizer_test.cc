/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "evp_store_single_threaded_test.h"

#include "htresizer.h"
#include "kv_bucket.h"
#include "vbucket.h"

#include <folly/portability/GTest.h>

#include <algorithm>
#include <chrono>

/**
 * Tests for HashtableResizerTask and the per-vBucket ResizingVisitor it drives
 * through VBCBAdaptor, covering what happens when a resize is deferred because
 * something holds the HashTable's visiting lock - as a CacheTransferTask does
 * for the whole duration of a transfer.
 */
class HashtableResizerTaskTest : public SingleThreadedKVBucketTest {
protected:
    /// @return the resize visitor task, while it is still scheduled.
    ExTask findResizeVisitor() {
        for (const auto& [id, entry] : task_executor->getTaskLocator()) {
            if (entry.second->getQueueType() == TaskType::NonIO &&
                entry.first->getDescription().starts_with(
                        "Hashtable resizer")) {
                return entry.first;
            }
        }
        return {};
    }

    /**
     * Run the resize visitor for as long as it remains immediately runnable,
     * i.e. until it completes or backs off to a future waketime.
     * @return how many times it ran.
     */
    int runResizeVisitorWhileRunnable() {
        auto& nonIoQ = *task_executor->getLpTaskQ(TaskType::NonIO);
        int runs = 0;
        while (runs < maxRuns) {
            auto task = findResizeVisitor();
            if (!task || task->getWaketime() > cb::time::steady_clock::now()) {
                break;
            }
            const auto description = task->getDescription();
            runNextTask(nonIoQ, description);
            ++runs;
        }
        return runs;
    }

    /// Run the resize visitor to completion, skipping over any backoff.
    void drainResizeVisitor() {
        auto& nonIoQ = *task_executor->getLpTaskQ(TaskType::NonIO);
        for (int i = 0; i < maxRuns; ++i) {
            auto task = findResizeVisitor();
            if (!task) {
                return;
            }
            const auto advance = std::max(
                    task->getWaketime() - cb::time::steady_clock::now() +
                            std::chrono::milliseconds(1),
                    cb::time::steady_clock::duration::zero());
            const auto description = task->getDescription();
            runNextTask(nonIoQ, description, advance);
        }
    }

    /// Shrink vb's HashTable so that the resizer wants to grow it back.
    void shrinkHashTable(VBucket& vb) {
        ASSERT_EQ(NeedsRevisit::No, vb.ht.resizeInOneStep(shrunkSize));
        ASSERT_EQ(shrunkSize, vb.ht.getSize());
        ASSERT_NE(shrunkSize, vb.ht.getPreferredSize(std::chrono::seconds(0)));
    }

    /// Small enough that ht_size (HashTable::minimumSize) exceeds it, so
    /// getPreferredSize() always wants to grow back from here.
    static constexpr size_t shrunkSize = 3;
    /// Bound on visitor runs, so a spin fails the test rather than hanging it.
    static constexpr int maxRuns = 20;
};

/**
 * A deferred resize must not leave the visitor immediately runnable. While the
 * visiting lock is held nothing can change, so re-running straight away is a
 * busy-spin that lasts for as long as the lock holder does.
 */
TEST_F(HashtableResizerTaskTest, BlockedResizeDoesNotSpin) {
    setVBucketState(Vbid(0), vbucket_state_active);
    auto vb = store->getVBucket(Vbid(0));
    ASSERT_TRUE(vb);
    shrinkHashTable(*vb);
    const auto preferredSize = vb->ht.getPreferredSize(std::chrono::seconds(0));

    // Stand in for the CacheTransferTask's long-lived visiting lock.
    auto visitHold = vb->ht.tryAcquireVisitingLock();
    ASSERT_TRUE(visitHold.owns_lock());

    HashtableResizerTask resizer(*store, 0);
    ASSERT_TRUE(resizer.run());
    ASSERT_TRUE(findResizeVisitor()) << "resize visitor was not scheduled";

    // One run is expected - the visitor has to attempt the resize once to
    // discover that it is deferred. It must not then be runnable again.
    EXPECT_EQ(1, runResizeVisitorWhileRunnable())
            << "resize visitor was re-run without making progress; while the "
               "visiting lock is held it can never progress, so an immediate "
               "re-run is a busy-spin";

    // The resize really was deferred, rather than being unnecessary.
    EXPECT_EQ(shrunkSize, vb->ht.getSize());

    // Once the lock is released a subsequent pass resizes, confirming the
    // deferral above was caused by the lock and not by something else.
    visitHold.unlock();
    ASSERT_TRUE(resizer.run());
    drainResizeVisitor();
    EXPECT_EQ(preferredSize, vb->ht.getSize());
}

/**
 * A vBucket is only visited once per resizer pass, so a visitor kept alive
 * waiting on a blocked vBucket would stop every other vBucket from being
 * re-evaluated for as long as the block lasts. Deferring must therefore end
 * the pass for that vBucket rather than retry within it.
 */
TEST_F(HashtableResizerTaskTest, BlockedVBucketDoesNotStallOthers) {
    setVBucketState(Vbid(0), vbucket_state_active);
    setVBucketState(Vbid(1), vbucket_state_active);
    auto blocked = store->getVBucket(Vbid(0));
    auto other = store->getVBucket(Vbid(1));
    shrinkHashTable(*blocked);
    shrinkHashTable(*other);

    auto visitHold = blocked->ht.tryAcquireVisitingLock();
    ASSERT_TRUE(visitHold.owns_lock());

    HashtableResizerTask resizer(*store, 0);
    ASSERT_TRUE(resizer.run());
    runResizeVisitorWhileRunnable();
    ASSERT_NE(shrunkSize, other->ht.getSize()) << "vb:1 resized on first pass";
    ASSERT_EQ(shrunkSize, blocked->ht.getSize()) << "vb:0 deferred";

    // vb:1 needs resizing again, as it would under continuing write load.
    shrinkHashTable(*other);

    // Further passes, as HashtableResizerTask makes every ht_resize_interval,
    // while vb:0 remains blocked.
    for (int pass = 0; pass < 5; ++pass) {
        ASSERT_TRUE(resizer.run());
        runResizeVisitorWhileRunnable();
    }
    EXPECT_NE(shrunkSize, other->ht.getSize())
            << "vb:1 was never re-resized while vb:0 stayed blocked";

    // And vb:0 is still picked up once it is no longer blocked.
    visitHold.unlock();
    ASSERT_TRUE(resizer.run());
    drainResizeVisitor();
    EXPECT_NE(shrunkSize, blocked->ht.getSize());
}
