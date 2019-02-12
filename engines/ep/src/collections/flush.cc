/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "collections/flush.h"
#include "collections/collection_persisted_stats.h"
#include "collections/vbucket_manifest.h"
#include "ep_bucket.h"
#include "item.h"

void Collections::VB::Flush::saveCollectionStats(
        std::function<void(CollectionID, PersistedStats)> cb) const {
    for (const auto c : mutated) {
        PersistedStats stats;
        {
            auto lock = manifest.lock(c);
            if (!lock.valid()) {
                // Can be flushing for a dropped collection (no longer in the
                // manifest)
                continue;
            }
            stats = lock.getPersistedStats();
        }
        cb(c, stats);
    }
}

void Collections::VB::Flush::incrementDiskCount(const DocKey& key) {
    if (key.getCollectionID() != CollectionID::System) {
        mutated.insert(key.getCollectionID());
        manifest.lock(key).incrementDiskCount();
    }
}

void Collections::VB::Flush::decrementDiskCount(const DocKey& key) {
    if (key.getCollectionID() != CollectionID::System) {
        mutated.insert(key.getCollectionID());
        manifest.lock(key).decrementDiskCount();
    }
}

void Collections::VB::Flush::setPersistedHighSeqno(const DocKey& key,
                                                   uint64_t value,
                                                   bool deleted) {
    if (key.getCollectionID() == CollectionID::System) {
        CollectionID cid;
        try {
            // Pass in the Collections specific SystemEvent prefix. We should
            // throw an exception if this isn't a Collections SystemEvent as
            // we shouldn't try to lookup and update a collection using say a
            // ScopeID
            cid = getCollectionIDFromKey(key,
                    Collections::CollectionEventPrefixWithSeparator);
        } catch (std::invalid_argument&) {
            // Not a collection system event, do nothing
            return;
        }

        // If this system event is a deletion, then it may be the case that we
        // are the replica and we know nothing about the previous state of this
        // collection. In this case, we do not want to throw if we cannot find
        // the collection, we should simply do nothing.
        manifest.lock().setPersistedHighSeqno(cid, value, deleted);
    }
    else {
        mutated.insert(key.getCollectionID());
        manifest.lock(key).setPersistedHighSeqno(value);
    }
}

void Collections::VB::Flush::checkAndTriggerPurge(Vbid vbid,
                                                  KVBucket& bucket) const {
    if (needsPurge) {
        triggerPurge(vbid, bucket);
    }
}

void Collections::VB::Flush::triggerPurge(Vbid vbid, KVBucket& bucket) {
    CompactionConfig config;
    config.db_file_id = vbid;
    bucket.scheduleCompaction(vbid, config, nullptr);
}