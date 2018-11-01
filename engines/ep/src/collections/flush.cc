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
#include "collections/vbucket_manifest.h"
#include "item.h"

void Collections::VB::Flush::processManifestChange(const queued_item& item) {
    // If we already have a manifest item stored see if the incoming item
    // has a higher seqno. During the flush of the items, we only want to update
    // the disk metadata once using the highest seqno manifest item.
    if ((collectionManifestItem &&
         item->getBySeqno() > collectionManifestItem->getBySeqno()) ||
        !collectionManifestItem) {
        collectionManifestItem = item;
    }

    // Save the collection-ID of every collection delete
    if (item->isDeleted()) {
        deletedCollections.push_back(
                VB::Manifest::getCollectionIDFromKey(item->getKey()));
    }
}

void Collections::VB::Flush::saveDeletes(
        std::function<void(CollectionID)> callback) const {
    for (const auto c : deletedCollections) {
        callback(c);
    }
}

void Collections::VB::Flush::saveItemCounts(
        std::function<void(CollectionID, uint64_t)> cb) const {
    for (const auto c : mutated) {
        cb(c, manifest.lock().getItemCount(c));
    }
}

Collections::VB::PersistedManifest Collections::VB::Flush::getManifestData()
        const {
    return Collections::VB::Manifest::getPersistedManifest(
            *getCollectionsManifestItem());
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