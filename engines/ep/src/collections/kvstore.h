/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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
 * This file contains collection data structures that KVStore must maintain.
 *  - Manifest is a structure that KVStore returns (from getCollectionsManifest)
 *  - CommitMetaData is a structure that KVStore maintains in response to system
 *    events being stored.
 */

#pragma once

#include "collections/collections_types.h"
#include "item.h"

namespace Collections {
namespace KVStore {

/**
 * KVStore will return the start-seqno of the collection and the meta-data of
 * the collection.
 */
struct OpenCollection {
    int64_t startSeqno;
    CollectionMetaData metaData;
};

/**
 * Data that KVStore is required return to from KVStore::getCollectionsManifest
 * This data can be used to construct a Collections::VB::Manifest
 */
struct Manifest {
    /// The uid of the last manifest to change the collection state
    ManifestUid manifestUid{0};

    /// A vector of collections that are available
    std::vector<OpenCollection> collections;

    /// A vector of scopes that are available
    std::vector<ScopeID> scopes;

    /**
     * true if KVStore has collection data belonging to dropped collections.
     * i.e. a collection was dropped but has not yet been 100% erased from
     * storage. KVBucket can decide to schedule purging based on this bool
     */
    bool droppedCollectionsExist;
};

/**
 * A dropped collection stores the seqno range it spans and the collection-ID
 */
struct DroppedCollection {
    int64_t startSeqno;
    int64_t endSeqno;
    CollectionID collectionId;
};

/**
 * Data that KVStore will maintain as the EPBucket flusher writes system events
 * The underlying implementation of KVStore can optionally persist this data
 * in any format to allow for a simple implementation of:
 *   -  KVStore::getCollectionsManifest
 *   -  KVStore::getDroppedCollections
 */
struct CommitMetaData {
    void clear() {
        needsCommit = false;
        collections.clear();
        scopes.clear();
        droppedCollections.clear();
        droppedScopes.clear();
        manifestUid.reset(0);
    }

    void setUid(ManifestUid in) {
        manifestUid = std::max<ManifestUid>(manifestUid, in);
    }

    /**
     * The most recent manifest committed, if needsCommit is true this value
     * must be stored by the underlying KVStore.
     */
    ManifestUid manifestUid{0};

    /**
     * The following vectors store any items that are creating or dropping
     * scopes and collections. The underlying KVStore must store the contents
     * of these containers if needsCommit is true.
     */
    std::vector<OpenCollection> collections;
    std::vector<ScopeID> scopes;

    std::vector<DroppedCollection> droppedCollections;
    std::vector<ScopeID> droppedScopes;

    /**
     * Set to true when any of the fields in this structure have data which
     * should be saved in the KVStore update/commit. The underlying KVStore
     * reads this data and stores it in any suitable format (e.g. flatbuffers).
     */
    bool needsCommit{false};
};

} // end namespace KVStore
} // end namespace Collections