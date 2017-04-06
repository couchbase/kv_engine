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

    void visit(const HashTable::HashBucketLock& lh, StoredValue* v) override;

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
