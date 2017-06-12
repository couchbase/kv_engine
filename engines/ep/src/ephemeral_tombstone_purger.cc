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
#include "ephemeral_vb.h"
#include "seqlist.h"

#include <climits>

EphemeralVBucket::HTTombstonePurger::HTTombstonePurger(
        EphemeralVBucket& vbucket, rel_time_t purgeAge)
    : vbucket(vbucket),
      now(ep_current_time()),
      purgeAge(purgeAge),
      numPurgedItems(0) {
}

bool EphemeralVBucket::HTTombstonePurger::visit(
        const HashTable::HashBucketLock& hbl, StoredValue& v) {
    auto* osv = v.toOrderedStoredValue();

    if (!osv->isDeleted()) {
        return true;
    }

    // Skip if deleted item is too young.
    if (now - osv->getDeletedTime() < purgeAge) {
        return true;
    }

    // This item should be purged. Remove from the HashTable and move over to
    // being owned by the sequence list.
    auto ownedSV = vbucket.ht.unlocked_release(hbl, v.getKey());
    {
        std::lock_guard<std::mutex> listWriteLg(
                vbucket.seqList->getListWriteLock());
        // Mark the item stale, with no replacement item
        vbucket.seqList->markItemStale(listWriteLg, std::move(ownedSV), nullptr);
    }
    ++numPurgedItems;

    return true;
}

EphemeralVBucket::HTCleaner::HTCleaner(rel_time_t purgeAge)
    : purgeAge(purgeAge), numItemsMarkedStale(0) {
}

void EphemeralVBucket::HTCleaner::visitBucket(VBucketPtr& vb) {
    auto vbucket = dynamic_cast<EphemeralVBucket*>(vb.get());
    if (!vbucket) {
        throw std::invalid_argument(
                "VBTombstonePurger::visitBucket: Called with a non-Ephemeral "
                "bucket");
    }
    numItemsMarkedStale += vbucket->markOldTombstonesStale(purgeAge);
}

EphTombstoneHTCleaner::EphTombstoneHTCleaner(EventuallyPersistentEngine* e)
    : GlobalTask(e,
                 TaskId::EphTombstoneHTCleaner,
                 e->getConfiguration().getEphemeralMetadataPurgeInterval(),
                 false),
      staleItemDeleterTask(std::make_shared<EphTombstoneStaleItemDeleter>(e)) {
    ExecutorPool::get()->schedule(staleItemDeleterTask);
}

bool EphTombstoneHTCleaner::run() {
    LOG(EXTENSION_LOG_INFO,
        "%s starting with purge age:%" PRIu64,
        getDescription().data(),
        uint64_t(getDeletedPurgeAge()));

    // Create a VB purger, and run across all VBuckets.
    auto start = ProcessClock::now();
    EphemeralVBucket::HTCleaner purger(getDeletedPurgeAge());
    engine->getKVBucket()->visit(purger);
    auto end = ProcessClock::now();

    auto duration_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    LOG(EXTENSION_LOG_INFO,
        "%s completed. Marked %" PRIu64 " items as stale. Took %" PRIu64
        "ms. Sleeping for %" PRIu64 " seconds.",
        getDescription().data(),
        uint64_t(purger.getNumItemsMarkedStale()),
        uint64_t(duration_ms.count()),
        uint64_t(getSleepTime()));

    // Sleep ourselves, and wakeup the StaleItemDeleter task to complete the
    // purge.
    snooze(getSleepTime());
    staleItemDeleterTask->wakeUp();

    return true;
}

cb::const_char_buffer EphTombstoneHTCleaner::getDescription() {
    return "Eph tombstone hashtable cleaner";
}

size_t EphTombstoneHTCleaner::getSleepTime() const {
    return engine->getConfiguration().getEphemeralMetadataPurgeInterval();
}

size_t EphTombstoneHTCleaner::getDeletedPurgeAge() const {
    return engine->getConfiguration().getEphemeralMetadataPurgeAge();
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
