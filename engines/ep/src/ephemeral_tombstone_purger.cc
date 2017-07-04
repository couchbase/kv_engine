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

#include "atomic.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "ephemeral_bucket.h"
#include "ephemeral_vb.h"
#include "seqlist.h"

#include <climits>

EphemeralVBucket::HTTombstonePurger::HTTombstonePurger(rel_time_t purgeAge)
    : now(ep_current_time()), purgeAge(purgeAge), numPurgedItems(0) {
}

void EphemeralVBucket::HTTombstonePurger::setDeadline(
        ProcessClock::time_point deadline) {
    progressTracker.setDeadline(deadline);
}

void EphemeralVBucket::HTTombstonePurger::setCurrentVBucket(VBucket& vb) {
    vbucket = &dynamic_cast<EphemeralVBucket&>(vb);
}

bool EphemeralVBucket::HTTombstonePurger::visit(
        const HashTable::HashBucketLock& hbl, StoredValue& v) {
    auto* osv = v.toOrderedStoredValue();

    if (osv->isDeleted() && (now - osv->getDeletedTime() >= purgeAge)) {
        // This item should be purged. Remove from the HashTable and move over
        // to being owned by the sequence list.
        auto ownedSV = vbucket->ht.unlocked_release(hbl, v.getKey());
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
      staleItemDeleterTask(std::make_shared<EphTombstoneStaleItemDeleter>(e)) {
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

        LOG(EXTENSION_LOG_NOTICE /*INFO*/,
            "%s starting with purge age:%" PRIu64 "s",
            getDescription().data(),
            uint64_t(getDeletedPurgeAge()));
    }

    // Prepare the underlying visitor.
    auto& visitor = getPurgerVisitor();
    visitor.setDeadline(ProcessClock::now() + getChunkDuration());
    visitor.clearStats();

    // (re)start visiting.
    auto start = ProcessClock::now();
    bucketPosition = bucket.pauseResumeVisit(*prAdapter, bucketPosition);
    auto end = ProcessClock::now();

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
    LOG(EXTENSION_LOG_NOTICE /*INFO*/,
        "%s %s. Took %" PRIu64 " ms. Visited %" PRIu64 " items, marked %" PRIu64
        " items as stale. Sleeping for %" PRIu64 " seconds.",
        getDescription().data(),
        completed ? "completed" : "paused",
        uint64_t(duration_ms.count()),
        uint64_t(visitor.getVisitedCount()),
        uint64_t(visitor.getNumItemsMarkedStale()),
        uint64_t(getSleepTime()));

    snooze(getSleepTime());
    staleItemDeleterTask->wakeUp();
    return true;
}

cb::const_char_buffer EphTombstoneHTCleaner::getDescription() {
    return "Eph tombstone hashtable cleaner";
}

std::chrono::milliseconds EphTombstoneHTCleaner::getChunkDuration() const {
    return std::chrono::milliseconds(
            engine->getConfiguration().getEphemeralMetadataPurgeChunkDuration());
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
class EphemeralVBucket::StaleItemDeleter : public VBucketVisitor {
public:
    StaleItemDeleter() {
    }

    void visitBucket(VBucketPtr& vb) override {
        auto* vbucket = dynamic_cast<EphemeralVBucket*>(vb.get());
        if (!vbucket) {
            throw std::invalid_argument(
                    "StaleItemDeleter::visitBucket: Called with a "
                    "non-Ephemeral bucket");
        }
        numItemsDeleted += vbucket->purgeStaleItems();
    }

    size_t getNumItemsDeleted() const {
        return numItemsDeleted;
    }

protected:
    /// Count of how many items have been deleted for all visited vBuckets.
    size_t numItemsDeleted = 0;
};

EphTombstoneStaleItemDeleter::EphTombstoneStaleItemDeleter(
        EventuallyPersistentEngine* e)
    : GlobalTask(e, TaskId::EphTombstoneStaleItemDeleter, INT_MAX, false) {
}

bool EphTombstoneStaleItemDeleter::run() {
    LOG(EXTENSION_LOG_INFO, "%s starting", getDescription().data());

    // Create a StaleItemDeleter, and run across all VBuckets.
    auto start = ProcessClock::now();
    EphemeralVBucket::StaleItemDeleter deleter;
    engine->getKVBucket()->visit(deleter);
    auto end = ProcessClock::now();

    auto duration_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    LOG(EXTENSION_LOG_INFO,
        "%s completed. Deleted %" PRIu64 " items. Took %" PRIu64 "ms.",
        getDescription().data(),
        uint64_t(deleter.getNumItemsDeleted()),
        uint64_t(duration_ms.count()));

    // Sleep forever - rely on the HTCleaner task to wake us.
    snooze(INT_MAX);
    return true;
}

cb::const_char_buffer EphTombstoneStaleItemDeleter::getDescription() {
    return "Eph tombstone stale item deleter";
}
