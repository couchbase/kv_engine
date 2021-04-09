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

#include "ephemeral_tombstone_purger.h"

#include "atomic.h"
#include "bucket_logger.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "ephemeral_bucket.h"
#include "ephemeral_vb.h"
#include "executorpool.h"
#include "seqlist.h"

#include <climits>

EphemeralVBucket::HTTombstonePurger::HTTombstonePurger(rel_time_t purgeAge)
    : now(ep_real_time()), purgeAge(purgeAge), numPurgedItems(0) {
}

void EphemeralVBucket::HTTombstonePurger::setDeadline(
        std::chrono::steady_clock::time_point deadline) {
    progressTracker.setDeadline(deadline);
}

void EphemeralVBucket::HTTombstonePurger::setCurrentVBucket(VBucket& vb) {
    vbucket = &dynamic_cast<EphemeralVBucket&>(vb);
}

bool EphemeralVBucket::HTTombstonePurger::visit(
        const HashTable::HashBucketLock& hbl, StoredValue& v) {
    // MB-41089: Never remove Pending Prepares, that would break SyncDelete.
    if (!v.isPending()) {
        auto* osv = v.toOrderedStoredValue();

        // MB-31175: Item must have been deleted before this task starts to
        // ensure that we do not get a -ve value when we check if the time
        // difference is >= purgeAge. This is preferable to updating the task
        // start time for every visit and has little impact as this task runs
        // frequently.
        if ((osv->isDeleted() || osv->isPrepareCompleted()) &&
            (now >= osv->getCompletedOrDeletedTime()) &&
            (now - osv->getCompletedOrDeletedTime() >= purgeAge)) {
            // This item should be purged. Remove from the HashTable and move
            // over to being owned by the sequence list. Remove by pointer (not
            // by key) so that we do not remove any committed/prepared
            // StoredValues for which there may be two with the same key.
            auto ownedSV = vbucket->ht.unlocked_release(hbl, osv);
            {
                std::lock_guard<std::mutex> listWriteLg(
                        vbucket->seqList->getListWriteLock());
                // Mark the item stale, with no replacement item
                vbucket->seqList->markItemStale(
                        listWriteLg, std::move(ownedSV), nullptr);
            }
            ++vbucket->htDeletedPurgeCount;
            ++numPurgedItems;
        }
    }
    ++numVisitedItems;

    // See if we have done enough work for this chunk. If so
    // stop visiting (for now).
    return progressTracker.shouldContinueVisiting(numVisitedItems);
}

void EphemeralVBucket::HTTombstonePurger::clearStats() {
    numVisitedItems = 0;
    numPurgedItems = 0;
}

EphTombstoneHTCleaner::EphTombstoneHTCleaner(EventuallyPersistentEngine* e,
                                             EphemeralBucket& bucket)
    : GlobalTask(e,
                 TaskId::EphTombstoneHTCleaner,
                 e->getConfiguration().getEphemeralMetadataPurgeInterval(),
                 false),
      bucket(bucket),
      bucketPosition(bucket.endPosition()),
      staleItemDeleterTask(
              std::make_shared<EphTombstoneStaleItemDeleter>(e, bucket)) {
    staleItemDeleterTaskId =
            ExecutorPool::get()->schedule(staleItemDeleterTask);
}

bool EphTombstoneHTCleaner::run() {
    // Get our pause/resume visitor. If we didn't finish the previous pass,
    // then resume from where we last were, otherwise create a new visitor
    // starting from the beginning.
    if (bucketPosition == bucket.endPosition()) {
        prAdapter = std::make_unique<PauseResumeVBAdapter>(
                std::make_unique<EphemeralVBucket::HTTombstonePurger>(
                        getDeletedPurgeAge()));
        bucketPosition = bucket.startPosition();

        EP_LOG_DEBUG("{} starting with purge age:{}s",
                     getDescription(),
                     uint64_t(getDeletedPurgeAge()));
    }

    // Prepare the underlying visitor.
    auto& visitor = getPurgerVisitor();
    visitor.setDeadline(std::chrono::steady_clock::now() + getChunkDuration());
    visitor.clearStats();

    // (re)start visiting.
    auto start = std::chrono::steady_clock::now();
    bucketPosition = bucket.pauseResumeVisit(*prAdapter, bucketPosition);
    auto end = std::chrono::steady_clock::now();

    // Check if the visitor completed a full pass.
    bool completed = (bucketPosition == bucket.endPosition());

    auto duration_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    if (!completed) {
        // Schedule to run again asap - note this still yields to the scheduler
        // if there are any higher priority tasks which want to run.
        return true;
    }

    // Completed a full pass. Sleep ourselves, and wakeup the StaleItemDeleter
    // task to complete the purge.
    EP_LOG_DEBUG(
            "{} {}. Took {} ms. Visited {} items, marked {} items as stale. "
            "Sleeping for {} seconds.",
            getDescription(),
            completed ? "completed" : "paused",
            uint64_t(duration_ms.count()),
            uint64_t(visitor.getVisitedCount()),
            uint64_t(visitor.getNumItemsMarkedStale()),
            uint64_t(getSleepTime()));

    snooze(getSleepTime());
    ExecutorPool::get()->wake(staleItemDeleterTaskId);
    return true;
}

std::string EphTombstoneHTCleaner::getDescription() const {
    return "Eph tombstone hashtable cleaner";
}

std::chrono::microseconds EphTombstoneHTCleaner::maxExpectedDuration() const {
    // Tombstone HT cleaner processes items in chunks, with each chunk
    // constrained by a ChunkDuration runtime, so we expect to only take that
    // long. However, the ProgressTracker used estimates the time remaining, so
    // apply some headroom to that figure so we don't get inundated with
    // spurious "slow tasks" which only just exceed the limit.
    return getChunkDuration() * 10;
}

std::chrono::milliseconds EphTombstoneHTCleaner::getChunkDuration() const {
    return std::chrono::milliseconds(
            engine->getConfiguration()
                    .getEphemeralMetadataMarkStaleChunkDuration());
}

size_t EphTombstoneHTCleaner::getSleepTime() const {
    return engine->getConfiguration().getEphemeralMetadataPurgeInterval();
}

size_t EphTombstoneHTCleaner::getDeletedPurgeAge() const {
    return engine->getConfiguration().getEphemeralMetadataPurgeAge();
}

EphemeralVBucket::HTTombstonePurger&
EphTombstoneHTCleaner::getPurgerVisitor() {
    return dynamic_cast<EphemeralVBucket::HTTombstonePurger&>(
            prAdapter->getHTVisitor());
}

/**
 * Ephemeral VBucket Sequence stale item deleter
 *
 * Visitor which is responsible for scanning sequence list for stale items
 * and deleting them.
 */
class EphemeralVBucket::StaleItemDeleter : public PauseResumeVBVisitor {
public:
    explicit StaleItemDeleter(EphemeralBucket& bucket) : bucket(bucket) {
    }

    bool visit(VBucket& vb) override {
        auto* vbucket = dynamic_cast<EphemeralVBucket*>(&vb);
        if (!vbucket) {
            throw std::invalid_argument(
                    "StaleItemDeleter::visitBucket: Called with a "
                    "non-Ephemeral bucket");
        }

        /// The lambda function passed indicates if the "StaleItemDeleter"
        /// should be paused. It can be called by the module(s) implementing the
        /// purge at the desired granularity
        numItemsDeleted += vbucket->purgeStaleItems(

                [this]() {
                    shouldContinueVisiting =
                            progressTracker.shouldContinueVisiting(
                                    numVisitedItems++);
                    return !(shouldContinueVisiting);
                });
        return shouldContinueVisiting;
    }

    size_t getNumItemsDeleted() const {
        return numItemsDeleted;
    }

    void setDeadline(std::chrono::steady_clock::time_point deadline) {
        progressTracker.setDeadline(deadline);
    }

    void clearStats() {
        numItemsDeleted = 0;
        numVisitedItems = 0;
        shouldContinueVisiting = true;
    }

protected:
    /// The bucket we are associated with.
    EphemeralBucket& bucket;

    /// Count of how many items have been deleted for all visited vBuckets.
    size_t numItemsDeleted = 0;

    /// Estimates how far we have got, and when we should pause.
    ProgressTracker progressTracker;

    /// Count of how many items have been visited.
    size_t numVisitedItems = 0;

    /// Indicates if the VB visitor should continue visiting other vbuckets in
    /// the current run
    bool shouldContinueVisiting = true;
};

EphTombstoneStaleItemDeleter::EphTombstoneStaleItemDeleter(
        EventuallyPersistentEngine* e, EphemeralBucket& bucket)
    : GlobalTask(e, TaskId::EphTombstoneStaleItemDeleter, INT_MAX, false),
      bucket(bucket),
      bucketPosition(bucket.endPosition()) {
}

bool EphTombstoneStaleItemDeleter::run() {
    // Get our pause/resume visitor. If we didn't finish the previous pass,
    // then resume from where we last were, otherwise create a new visitor
    // starting from the beginning.
    if (bucketPosition == bucket.endPosition()) {
        staleItemDeleteVbVisitor =
                std::make_unique<EphemeralVBucket::StaleItemDeleter>(bucket);
        bucketPosition = bucket.startPosition();

        EP_LOG_DEBUG("{} starting", getDescription());
    }

    // Create a StaleItemDeleter, and run across all VBuckets.
    staleItemDeleteVbVisitor->setDeadline(std::chrono::steady_clock::now() +
                                          getChunkDuration());
    staleItemDeleteVbVisitor->clearStats();

    auto start = std::chrono::steady_clock::now();
    bucketPosition =
            bucket.pauseResumeVisit(*staleItemDeleteVbVisitor, bucketPosition);
    auto end = std::chrono::steady_clock::now();

    // Check if the visitor completed a full pass.
    bool completed = (bucketPosition == bucket.endPosition());

    auto duration_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    if (!completed) {
        // Schedule to run again asap - note this still yields to the scheduler
        // if there are any higher priority tasks which want to run.
        return true;
    }

    EP_LOG_DEBUG("{} {}. Deleted {} items. Took {}ms.",
                 getDescription(),
                 completed ? "completed" : "paused",
                 staleItemDeleteVbVisitor->getNumItemsDeleted(),
                 duration_ms.count());

    // Completed a full pass, sleep forever - rely on the HTCleaner task to
    // wake us.
    snooze(INT_MAX);
    return true;
}

std::string EphTombstoneStaleItemDeleter::getDescription() const {
    return "Eph tombstone stale item deleter";
}

std::chrono::microseconds EphTombstoneStaleItemDeleter::maxExpectedDuration()
        const {
    // Stale item deleter purges tombstone items in chunks, with each chunk
    // constrained by a ChunkDuration runtime, so we expect to only take that
    // long. However, the ProgressTracker used estimates the time remaining, so
    // apply some headroom to that figure so we don't get inundated with
    // spurious "slow tasks" which only just exceed the limit.
    return getChunkDuration() * 10;
}

std::chrono::milliseconds EphTombstoneStaleItemDeleter::getChunkDuration()
        const {
    return std::chrono::milliseconds(
            engine->getConfiguration()
                    .getEphemeralMetadataPurgeStaleChunkDuration());
}
