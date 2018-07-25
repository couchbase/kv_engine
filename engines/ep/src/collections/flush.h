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

#include "item.h"

#include <unordered_set>
#include <vector>

namespace Collections {
namespace VB {

class Manifest;

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
     * @return the JSON data to be persisted if a collection create/drop is part
     *         of the flushers items
     */
    std::string getJsonForFlush() const;

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
    void saveItemCounts(std::function<void(CollectionID, uint64_t)> cb) const;

    /**
     * Increment the 'disk' count for the collection associated with the key
     */
    void incrementDiskCount(const DocKey& key);

    /**
     * Decrement the 'disk' count for the collection associated with the key
     */
    void decrementDiskCount(const DocKey& key);

private:
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
