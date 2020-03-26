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
#include <vector>

class DiskDocKey;

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
     * - Default Scope exists
     * - manifest UID of 0
     * - no dropped collections
     */
    explicit Manifest(Default)
        : collections{{0, {}}}, scopes{{ScopeID::Default}} {
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
    bool droppedCollectionsExist{false};
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

/**
 * Encode the manifest commit meta data into a flatbuffer
 * @param meta manifest commit meta data
 */
flatbuffers::DetachedBuffer encodeManifestUid(
        Collections::KVStore::CommitMetaData& meta);

/**
 * Encode the open collections list into a flatbuffer. Includes merging
 * with what was read off disk.
 * @param droppedCollections dropped collections list
 * @param collectionsMeta manifest commit meta data
 * @param collections collections buffer from local data store
 */
flatbuffers::DetachedBuffer encodeOpenCollections(
        std::vector<Collections::KVStore::DroppedCollection>&
                droppedCollections,
        Collections::KVStore::CommitMetaData& collectionsMeta,
        cb::const_byte_buffer collections);

/**
 * Encode the dropped collection list into a flatbuffer.
 * @param collectionsMeta manifest commit meta data
 * @param dropped dropped collections list
 */
flatbuffers::DetachedBuffer encodeDroppedCollections(
        Collections::KVStore::CommitMetaData& collectionsMeta,
        const std::vector<Collections::KVStore::DroppedCollection>& dropped);

/**
 * Encode open scopes list into flat buffer.
 * @param collectionsMeta manifest commit meta data
 * @param scopes open scopes list
 */
flatbuffers::DetachedBuffer encodeScopes(
        Collections::KVStore::CommitMetaData& collectionsMeta,
        cb::const_byte_buffer scopes);

/// callback to inform KV-engine that KVStore dropped key@seqno
using DroppedCb = std::function<void(const DiskDocKey&, int64_t)>;

} // end namespace KVStore
} // end namespace Collections
