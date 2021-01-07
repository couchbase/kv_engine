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

/**
 * Ephemeral Bucket Tombstone Purger tasks
 *
 * Ephemeral buckets need to store tombstones (records of deleted documents) in
 * memory (unlike EP buckets which can store on disk). Such tombstones have a
 * finite lifetime, so we don't end up filling up all of RAM with them.
 * To handle this, there are a set of background tasks which run
 * periodically to purge tombstones which have reached a certain age.
 *
 * The high level process is simple - identify tombstones which are older than
 * ephemeral_metadata_purge_age, and remove them from memory. However,
 * the implementation is a little more complicated, due to the interaction
 * between the HashTable and SequenceList which are used to access
 * OrderedStoredValues:
 *
 * To purge OSVs we must remove them from both data-structures. For a HashTable
 * alone this would be straightforward - iterate across it identifying
 * tombstones we wish to purge, and remove from the HashTable (under the HTLock
 * for that particular item).
 * The SequenceList complicates things - the SeqList is non-owning (it only
 * holds ptrs to OSVs), and a range read may be in progress by
 * another actor. As such, we cannot actually delete (from HashTable) items
 * which are within an in-flight range read, as that would break DCP
 * invariants (if we've already told a downstream client that we have items in
 * range [A,Z], we cannot delete item M before it has been read).
 *
 * Therefore, purging is handled with a two-phase approach, with each phase
 * done by a different Task:
 *
 * 1. EphTombstoneHTCleaner - visit the HashTable for deleted items exceeding
 *    ephemeral_metadata_purge_age. For such items, unlink from the HashTable
 *    (but don't delete the object), and mark the item as stale.
 *    Such item can no longer be located via the HashTable, but are still in
 *    the SequenceList, hence in-progress range reads are safe to continue.
 *
 * 2. EphTombstoneStaleItemDeleter - iterate the SequenceList in order
 *    looking for stale OSVs. For such items unlink from the SequenceList and
 *    delete the OSV.
 *
 * Note that items can also become stale if they have been replaced with a newer
 * revision - this occurs when an item needs to be modified but the existing
 * revision is being read by a rangeRead and hence we cannot simply update the
 * existing item. As such, EphTombstoneStaleItemDeleter task deletes stale
 * items created in both situations, and isn't strictly limited to purging
 * tombstones.
 */
#pragma once

#include "ephemeral_vb.h"
#include "kv_bucket_iface.h"
#include "progress_tracker.h"
#include "vb_visitors.h"

class EphemeralBucket;
class EphTombstoneStaleItemDeleter;

/**
 * HashTable Tombstone Purger visitor
 *
 * Visitor which is responsible for removing deleted items from the HashTable
 * which are past their permitted lifetime.
 *
 * Ownership of such items is transferred to the SequenceList as 'stale' items;
 * cleanup of the SequenceList is handled seperately (see
 * SequenceList::purgeTombstones).
*/
class EphemeralVBucket::HTTombstonePurger : public VBucketAwareHTVisitor {
public:
    explicit HTTombstonePurger(rel_time_t purgeAge);

    // Set the deadline at which point the visitor will pause visiting.
    void setDeadline(std::chrono::steady_clock::time_point deadline);

    void setCurrentVBucket(VBucket& vb) override;

    bool visit(const HashTable::HashBucketLock& lh, StoredValue& v) override;

    /// Return the number of items visited in the HashTable.
    size_t getVisitedCount() const {
        return numVisitedItems;
    }

    /// Return the number of items purged from the HashTable.
    size_t getNumItemsMarkedStale() const {
        return numPurgedItems;
    }

    void clearStats();

protected:
    /// VBucket being visited.
    EphemeralVBucket* vbucket;

    /// Time point the purge is running at. Set to ep_real_time in object
    /// creation.
    const time_t now;

    /// Items older than this age are purged. "Age" is defined as:
    ///    now - delete_time.
    const rel_time_t purgeAge;

    /// Estimates how far we have got, and when we should pause.
    ProgressTracker progressTracker;

    /// Count of how many items have been visited.
    size_t numVisitedItems = 0;

    /// Count of how many items have been purged.
    size_t numPurgedItems;
};

/**
 * Task responsible for identifying tombstones (deleted item markers) which
 * are too old, and removing from the Ephemeral buckets' HashTable.
 */
class EphTombstoneHTCleaner : public GlobalTask {
public:
    EphTombstoneHTCleaner(EventuallyPersistentEngine* e,
                          EphemeralBucket& bucket);

    bool run() noexcept override;

    std::string getDescription() override;

    std::chrono::microseconds maxExpectedDuration() override;

private:
    /// How long should each chunk of HT cleaning run for?
    std::chrono::milliseconds getChunkDuration() const;

    /// Duration (in seconds) task should sleep for between runs.
    size_t getSleepTime() const;

    /// Age (in seconds) which deleted items will be purged after.
    size_t getDeletedPurgeAge() const;

    /// Returns the underlying VBTombstonePurger instance.
    EphemeralVBucket::HTTombstonePurger& getPurgerVisitor();

    /// The bucket we are associated with.
    EphemeralBucket& bucket;

    /// Opaque marker indicating how far through the KVBucket we have visited.
    KVBucketIface::Position bucketPosition;

    /**
     * Visitor adapter which supports pausing & resuming (records how far
     * though a VBucket is has got). unique_ptr as we re-create it for each
     * complete pass.
     */
    std::unique_ptr<PauseResumeVBAdapter> prAdapter;

    /// Second paired task which deletes stale items from the sequenceList.
    std::shared_ptr<EphTombstoneStaleItemDeleter> staleItemDeleterTask;

    /// TaskId for StaleItemDeleter registered with executorpool.
    size_t staleItemDeleterTaskId;
};

/**
 * Task responsible for deleting stale items from Ephemeral buckets'
 * SequenceLists.
 *
 * Works in conjunction with EphTombstoneCleanupHashTable.
 */
class EphTombstoneStaleItemDeleter : public GlobalTask {
public:
    EphTombstoneStaleItemDeleter(EventuallyPersistentEngine* e,
                                 EphemeralBucket& bucket);

    bool run() noexcept override;

    std::string getDescription() override;

    std::chrono::microseconds maxExpectedDuration() override;

private:
    /// How long should each chunk of stale item deleter run for?
    std::chrono::milliseconds getChunkDuration() const;

    /// The bucket we are associated with.
    EphemeralBucket& bucket;

    /// Opaque marker indicating how far through the KVBucket we have visited.
    KVBucketIface::Position bucketPosition;

    /// Vb visitor instance that deletes the stale items by visiting all
    /// vbuckets one by one
    std::unique_ptr<EphemeralVBucket::StaleItemDeleter>
            staleItemDeleteVbVisitor;
};
