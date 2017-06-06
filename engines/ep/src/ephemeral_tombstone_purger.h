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

#pragma once

#include "config.h"

#include "ephemeral_vb.h"
#include "kv_bucket_iface.h"

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
class EphemeralVBucket::HTTombstonePurger : public HashTableVisitor {
public:
    HTTombstonePurger(EphemeralVBucket& vbucket, rel_time_t purgeAge);

    bool visit(const HashTable::HashBucketLock& lh, StoredValue& v) override;

    /// Return the number of items purged from the HashTable.
    size_t getNumPurged() const {
        return numPurgedItems;
    }

protected:
    /// VBucket being visited.
    EphemeralVBucket& vbucket;

    /// Time point the purge is running at. Set to ep_current_time in object
    /// creation.
    const rel_time_t now;

    /// Items older than this age are purged. "Age" is defined as:
    ///    now - delete_time.
    const rel_time_t purgeAge;

    /// Count of how many items have been purged.
    size_t numPurgedItems;
};

/**
 * VBucket Tombstone Purger visitor
 *
 * Visitor which is responsible for removing deleted items from each VBucket.
 * Mostly delegates to HTTombstonePurger for the 'real' work.
 */
class EphemeralVBucket::VBTombstonePurger : public VBucketVisitor {
public:
    VBTombstonePurger(rel_time_t purgeAge);

    void visitBucket(VBucketPtr& vb) override;

    size_t getNumPurgedItems() const {
        return numPurgedItems;
    }

protected:
    /// Items older than this age are purged.
    const rel_time_t purgeAge;

    /// Count of how many items have been purged for all visited vBuckets.
    size_t numPurgedItems;
};

/**
 * Task responsible for purging tombstones from an Ephemeral Bucket.
 */
class EphTombstonePurgerTask : public GlobalTask {
public:
    EphTombstonePurgerTask(EventuallyPersistentEngine* e, EPStats& stats_);

    bool run() override;

    cb::const_char_buffer getDescription() override;

private:
    /// Duration (in seconds) task should sleep for between runs.
    size_t getSleepTime() const;

    /// Age (in seconds) which deleted items will be purged after.
    size_t getDeletedPurgeAge() const;
};
