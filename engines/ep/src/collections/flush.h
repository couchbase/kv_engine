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

#pragma once

#include "collections/collections_types.h"
#include "item.h"

#include <unordered_set>
#include <vector>

class KVBucket;

namespace Collections {
namespace VB {

class Manifest;
struct PersistedStats;

/**
 * The Collections::VB::Flush object maintains data used in a single run of the
 * disk flusher for 1) Collection item counting and 2) persisted metadata
 * updates (when the flusher is flushing collection config changes).
 */
class Flush {
public:
    Flush(Manifest& manifest) : manifest(manifest) {
    }

    Manifest& getManifest() const {
        return manifest;
    }

    void processManifestChange(const queued_item& item);

    /**
     * @return the item which contains data for a persisted metadata update. Can
     *         be null when there is no metadata update happening.
     */
    const Item* getCollectionsManifestItem() const {
        return collectionManifestItem.get();
    }

    /**
     * @return the data to be persisted when a collection event is part of the
     *         flusher's batch of Items.
     */
    PersistedManifest getManifestData() const;

    /**
     * Run the specified callback against the list of collections which were
     * deleted (dropped) by the run of the flusher. Currently this is used to
     * remove the item count data for the collection from disk.
     */
    void saveDeletes(std::function<void(CollectionID)> callback) const;

    /**
     * Run the specified callback against the set of collections which have
     * changed their item count during the run of the flusher.
     */
    void saveCollectionStats(
            std::function<void(CollectionID, PersistedStats)> cb) const;

    /**
     * Increment the 'disk' count for the collection associated with the key
     */
    void incrementDiskCount(const DocKey& key);

    /**
     * Decrement the 'disk' count for the collection associated with the key
     */
    void decrementDiskCount(const DocKey& key);

    /**
     * Set the highest seqno that needs to be persisted for this collection
     *
     * @param key Key of the document being flushed
     * @param value New seqno value to set
     * @param deleted Is this document deleted? If it is, and it is a system
     *        event, then we should not throw an exception if we cannot find
     *        the collection we wish to update.
     */
    void setPersistedHighSeqno(const DocKey& key, uint64_t value, bool deleted);

    /**
     * Check to see if this flush should trigger a collection purge which if
     * true schedules a task which will iterate the vbucket's documents removing
     * those of any dropped collections. The actual task currently scheduled is
     * compaction.
     */
    void checkAndTriggerPurge(Vbid vbid, KVBucket& bucket) const;

    static void triggerPurge(Vbid vbid, KVBucket& bucket);

    void setNeedsPurge() {
        needsPurge = true;
    }

private:
    bool needsPurge = false;

    /**
     * Keep track of only the collections that have had a insert/delete in
     * this run of the flusher so we can flush only those collections whose
     * item count may have changed.
     */
    std::unordered_set<CollectionID> mutated;

    /**
     * A list of collections which have been deleted in the run of the flusher
     */
    std::vector<CollectionID> deletedCollections;

    /**
     * Shared pointer to an Item which holds collections manifest data that
     * maybe needed by the flush::commit (the item represents a collection
     * create/drop/flush)
     */
    queued_item collectionManifestItem;

    /// ref to the 'parent' manifest for this VB::Flusher
    Manifest& manifest;
};

} // end namespace VB
} // end namespace Collections
