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
    vbucket.seqList->markItemStale(std::move(ownedSV));
    ++numPurgedItems;
}
