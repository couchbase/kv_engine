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
#include "ephemeral_vb.h"
#include "seqlist.h"

EphemeralVBucket::HTTombstonePurger::HTTombstonePurger(
        EphemeralVBucket& vbucket, rel_time_t purgeAge)
    : vbucket(vbucket),
      now(ep_current_time()),
      purgeAge(purgeAge),
      numPurgedItems(0) {
}

void EphemeralVBucket::HTTombstonePurger::visit(
        const HashTable::HashBucketLock& hbl, StoredValue* v) {
    auto* osv = v->toOrderedStoredValue();

    if (!osv->isDeleted()) {
        return;
    }

    // Skip if deleted item is too young.
    if (now - osv->getDeletedTime() < purgeAge) {
        return;
    }

    // This item should be purged. Remove from the HashTable and move over to
    // being owned by the sequence list.
    auto ownedSV = vbucket.ht.unlocked_release(hbl, v->getKey());
    {
        std::lock_guard<std::mutex> listWriteLg(
                vbucket.seqList->getListWriteLock());
        vbucket.seqList->markItemStale(listWriteLg, std::move(ownedSV));
    }
    ++numPurgedItems;
}

EphemeralVBucket::VBTombstonePurger::VBTombstonePurger(rel_time_t purgeAge)
    : purgeAge(purgeAge), numPurgedItems(0) {
}

void EphemeralVBucket::VBTombstonePurger::visitBucket(VBucketPtr& vb) {
    auto vbucket = dynamic_cast<EphemeralVBucket*>(vb.get());
    if (!vbucket) {
        throw std::invalid_argument(
                "VBTombstonePurger::visitBucket: Called with a non-Ephemeral "
                "bucket");
    }
    numPurgedItems += vbucket->purgeTombstones(purgeAge);
}

EphTombstonePurgerTask::EphTombstonePurgerTask(EventuallyPersistentEngine* e,
                                               EPStats& stats_)
    : GlobalTask(e, TaskId::EphTombstonePurgerTask, 0, false) {
}

bool EphTombstonePurgerTask::run() {
    if (engine->getEpStats().isShutdown) {
        return false;
    }

    LOG(EXTENSION_LOG_NOTICE,
        "%s starting with purge age:%" PRIu64,
        to_string(getDescription()).c_str(),
        uint64_t(getDeletedPurgeAge()));

    // Create a VB purger, and run across all VBuckets.
    auto start = ProcessClock::now();
    EphemeralVBucket::VBTombstonePurger purger(getDeletedPurgeAge());
    engine->getKVBucket()->visit(purger);
    auto end = ProcessClock::now();

    auto duration_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    LOG(EXTENSION_LOG_NOTICE,
        "%s completed. Purged %" PRIu64 " items. Took %" PRIu64
        "ms. Sleeping for %" PRIu64 " seconds.",
        to_string(getDescription()).c_str(),
        uint64_t(purger.getNumPurgedItems()),
        uint64_t(duration_ms.count()),
        uint64_t(getSleepTime()));

    snooze(getSleepTime());
    return true;
}

cb::const_char_buffer EphTombstonePurgerTask::getDescription() {
    return "Ephemeral Tombstone Purger";
}

size_t EphTombstonePurgerTask::getSleepTime() const {
    return engine->getConfiguration().getEphemeralMetadataPurgeInterval();
}

size_t EphTombstonePurgerTask::getDeletedPurgeAge() const {
    return engine->getConfiguration().getEphemeralMetadataPurgeAge();
}
