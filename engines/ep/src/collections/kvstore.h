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
#include <flatbuffers/flatbuffers.h>
#include <unordered_map>
#include <vector>

class DiskDocKey;
class Item;

namespace Collections {
namespace KVStore {

/**
 * KVStore will store the start-seqno of the collection and the meta-data of
 * the collection.
 */
struct OpenCollection {
    uint64_t startSeqno;
    CollectionMetaData metaData;
    bool operator==(const OpenCollection& other) const;
    bool operator!=(const OpenCollection& other) const {
        return !(*this == other);
    }
};

/**
 * KVStore will store the start-seqno of the scope and the meta-data of
 * the scope.
 */
struct OpenScope {
    uint64_t startSeqno;
    ScopeMetaData metaData;
    bool operator==(const OpenScope& other) const;
    bool operator!=(const OpenScope& other) const {
        return !(*this == other);
    }
};

/**
 * Data that KVStore is required return to from KVStore::getCollectionsManifest
 * This data can be used to construct a Collections::VB::Manifest
 *
 * Default construction will result in the "default" manifest
 * - Default collection exists (since the beginning of time)
 * - Default Scope exists
 * - manifest UID of 0
 * - no dropped collections
 */
struct Manifest {
    struct Default {};
    struct Empty {};

    /**
     * Default results in the "default" manifest
     * - Default collection exists (since the beginning of time)
     * - Default Scope exists (since the beginning of time)
     * - manifest UID of 0
     * - no dropped collections
     */
    explicit Manifest(Default)
        : collections{{CollectionID::Default, {0, {}}}},
          scopes{{ScopeID::Default, {0, {}}}} {
    }

    /**
     * Empty results in
     * - no collections
     * - no scopes
     * - manifest UID of 0
     * - no dropped collections
     */
    explicit Manifest(Empty) {
    }

    ~Manifest(){};

    /**
     * compare Manifest - note that there is no sorting applied to the vectors
     * so may compare all elements
     */
    bool operator==(const Manifest& other) const;
    bool operator!=(const Manifest& other) const {
        return !(*this == other);
    }

    /// The uid of the last manifest to change the collection state
    ManifestUid manifestUid{0};

    /// A vector of collections that are available
    std::vector<OpenCollection> collections;

    /// A vector of scopes that are available
    std::vector<OpenScope> scopes;

    /**
     * true if KVStore has collection data belonging to dropped collections.
     * i.e. a collection was dropped but has not yet been 100% erased from
     * storage. KVBucket can decide to schedule purging based on this bool
     */
    bool droppedCollectionsExist{false};
};

/**
 * A dropped collection stores the seqno range it spans and the collection-ID
 */
struct DroppedCollection {
    uint64_t startSeqno;
    uint64_t endSeqno;
    CollectionID collectionId;
    bool operator==(const DroppedCollection& other) const;
    bool operator!=(const DroppedCollection& other) const {
        return !(*this == other);
    }
};


/**
 * Decode the buffers from the local doc store into the collections
 * data structures.
 * @param manifest buffer containing manifest meta data
 * @param collections buffer containing open collections
 * @param scopes buffer containing open scopes
 * @param dropped buffer containing dropped collections
 */
Manifest decodeManifest(cb::const_byte_buffer manifest,
                        cb::const_byte_buffer collections,
                        cb::const_byte_buffer scopes,
                        cb::const_byte_buffer dropped);

/**
 * Decode the local doc buffer into the dropped collections data structure.
 * @param dc buffer containing dropped collections
 */
std::vector<Collections::KVStore::DroppedCollection> decodeDroppedCollections(
        cb::const_byte_buffer dc);

template <class T>
void verifyFlatbuffersData(cb::const_byte_buffer buf,
                           const std::string& caller);

/**
 * Callback to inform KV-engine that KVStore dropped key@seqno and whether or
 * not it is an abort. Also passes the PCS (Persisted Completed Seqno) which is
 * used to avoid calling into the DM to drop keys that won't exist in the DM.
 */
/**
 * Callback to inform kv_engine that KVStore dropped key@seqno.
 *
 * @param key - the key
 * @param seqno - the seqno
 * @param aborted - if the key is for an aborted SyncWrite
 * @param pcs - the Persisted Completed Seqno in the compaction context. Used
 *              to avoid calling into the DM to drop keys that won't exist.
 */
using DroppedCb = std::function<void(
        const DiskDocKey& key, int64_t seqno, bool aborted, int64_t pcs)>;

} // end namespace KVStore
} // end namespace Collections
