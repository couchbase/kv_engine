/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/**
 * This file contains collection data structures that KVStore must maintain.
 *  - Manifest is a structure that KVStore returns (from getCollectionsManifest)
 *  - CommitMetaData is a structure that KVStore maintains in response to system
 *    events being stored.
 */

#pragma once

#include "collections/collections_types.h"
#include <fmt/ostream.h>
#include <cstdint>
#include <utility>
#include <vector>

class DiskDocKey;
class Item;

namespace Collections::KVStore {

/**
 * KVStore will store the start-seqno of the collection and the meta-data of
 * the collection.
 */
struct OpenCollection {
    OpenCollection(uint64_t startSeqno, CollectionMetaData metaData)
        : startSeqno(startSeqno), metaData(std::move(metaData)) {
    }
    uint64_t startSeqno;
    CollectionMetaData metaData;
    bool operator==(const OpenCollection& other) const;
    bool operator!=(const OpenCollection& other) const {
        return !(*this == other);
    }
};

std::ostream& operator<<(std::ostream& os, const Collections::KVStore::OpenCollection& collection);

/**
 * KVStore will store the start-seqno of the scope and the meta-data of
 * the scope.
 */
struct OpenScope {
    OpenScope(uint64_t startSeqno, ScopeMetaData metaData)
        : startSeqno(startSeqno), metaData(std::move(metaData)) {
    }
    uint64_t startSeqno;
    ScopeMetaData metaData;
    bool operator==(const OpenScope& other) const;
    bool operator!=(const OpenScope& other) const {
        return !(*this == other);
    }
};

std::ostream& operator<<(std::ostream& os, const Collections::KVStore::OpenScope& scope);

/**
 * Data that KVStore is required return to from KVStore::getCollectionsManifest
 * This data can be used to construct a Collections::VB::Manifest
 *
 * Default construction will result in the "default" manifest
 * - Default collection exists (since the beginning of time)
 * - Default Scope exists
 * - manifest UID of 0
 * - no dropped collections
 * - erase seqno of 0 (no data awaits erasing)
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
     * - erase seqno of 0 (no data awaits erasing)
     */
    explicit Manifest(Default)
        : collections{{0 /*seqno*/, CollectionMetaData{}}},
          scopes{{0 /*seqno*/, ScopeMetaData{}}} {
    }

    /**
     * Empty results in
     * - no collections
     * - no scopes
     * - manifest UID of 0
     * - no dropped collections
     * - erase seqno of 0 (no data awaits erasing)
     */
    explicit Manifest(Empty) {
    }

    ~Manifest() = default;

    /**
     * compare Manifest - note that there is no sorting applied to the vectors
     * so may compare all elements
     */
    bool operator==(const Manifest& other) const;
    bool operator!=(const Manifest& other) const {
        return !(*this == other);
    }

    /// Returns true if collections are logically equivalent
    bool compareCollections(const Manifest& other) const;

    /// Returns true if scopes are logically equivalent
    bool compareScopes(const Manifest& other) const;

    /// The uid of the last manifest to change the collection state
    ManifestUid manifestUid{0};

    /// A vector of collections that are available
    std::vector<OpenCollection> collections;

    /// A vector of scopes that are available
    std::vector<OpenScope> scopes;

    /**
     * true if KVStore has collection data belonging to dropped collections.
     * i.e. a collection was dropped but has not yet been 100% erased from
     * storage. Seeds VB::Manifest::dropInProgress; warmup and rollback
     * schedule a purge from it. Set from stored erase seqnos or a non-empty
     * dropped collections list (a list written before the upgrade has no
     * erase seqnos).
     */
    bool droppedCollectionsExist{false};
};

std::ostream& operator<<(std::ostream& os, const Collections::KVStore::Manifest& manifest);

/**
 * A dropped collection stores the seqno range it spans and the collection-ID
 */
struct DroppedCollection {
    DroppedCollection(uint64_t startSeqno,
                      uint64_t endSeqno,
                      CollectionID collectionId)
        : startSeqno(startSeqno),
          endSeqno(endSeqno),
          collectionId(collectionId) {
    }
    uint64_t startSeqno;
    uint64_t endSeqno;
    CollectionID collectionId;
    bool operator==(const DroppedCollection& other) const;
    bool operator!=(const DroppedCollection& other) const {
        return !(*this == other);
    }
};

/**
 * Decode the manifest buffer from the local doc store into a ManifestUID
 * @param manifest buffer containing manifest meta data
 */
ManifestUid decodeManifestUid(cb::const_byte_buffer manifest);

/**
 * Decode the buffers from the local doc store into the collections
 * data structures.
 * @param manifest buffer containing manifest meta data
 * @param collections buffer containing open collections
 * @param scopes buffer containing open scopes
 * @param dropped buffer containing dropped collections
 * @param erase buffer containing the erase seqno
 */
Manifest decodeManifest(cb::const_byte_buffer manifest,
                        cb::const_byte_buffer collections,
                        cb::const_byte_buffer scopes,
                        cb::const_byte_buffer dropped,
                        cb::const_byte_buffer erase);

/**
 * Decode the local doc buffer into an open collections data structure.
 * @param data buffer containing open collections (FlatBuffers)
 */
std::vector<Collections::KVStore::OpenCollection> decodeOpenCollections(
        cb::const_byte_buffer data);

/**
 * Decode the local doc buffer into the dropped collections data structure.
 * @param dc buffer containing dropped collections
 */
std::vector<Collections::KVStore::DroppedCollection> decodeDroppedCollections(
        cb::const_byte_buffer dc);

/**
 * The bounded set of erase seqnos: the seqnos of non-empty collection drops
 * (and collection flushes) whose data may remain in storage. All of a dropped
 * generation's data is below its drop (or flush) seqno.
 *
 * The set holds at most a configured number of seqnos. Adding beyond that
 * merges the two closest seqnos into the greater, so a seqno may stand for
 * earlier ones. A merged seqno is still the seqno of a drop/flush with data to
 * erase, merging only delays when a purge may be scheduled. The greatest seqno
 * is the erase seqno.
 */
class EraseSeqnos {
public:
    EraseSeqnos() = default;

    /**
     * @param seqnos must be ascending and unique
     */
    explicit EraseSeqnos(std::vector<uint64_t> seqnos);

    bool operator==(const EraseSeqnos&) const = default;

    /**
     * @return true if no data awaits erasing
     */
    bool empty() const {
        return seqnos.empty();
    }

    /**
     * @return the greatest seqno (the erase seqno), 0 if empty
     */
    uint64_t getEraseSeqno() const {
        return seqnos.empty() ? 0 : seqnos.back();
    }

    /**
     * @return the smallest seqno, 0 if empty
     */
    uint64_t getOldestSeqno() const {
        return seqnos.empty() ? 0 : seqnos.front();
    }

    /**
     * @return the seqnos, ascending
     */
    const std::vector<uint64_t>& getSeqnos() const {
        return seqnos;
    }

    /**
     * Add seqnos, then merge until at most maxSize remain.
     * @param newSeqnos seqnos to add (any order, 0 is ignored)
     * @param maxSize the maximum number of seqnos, at least 1
     */
    void add(const std::vector<uint64_t>& newSeqnos, size_t maxSize);

    /**
     * Remove the seqnos at or below seqno, their data has been erased.
     * @return true if any seqno was removed
     */
    bool removeUpTo(uint64_t seqno);

private:
    /**
     * Merge the two closest seqnos (into the greater) until within maxSize. A
     * tie merges the lesser pair, keeping the most recent seqnos precise.
     */
    void merge(size_t maxSize);

    /**
     * Ascending and unique
     */
    std::vector<uint64_t> seqnos;
};

std::ostream& operator<<(std::ostream& os, const EraseSeqnos& eraseSeqnos);

/**
 * Decode the local doc buffer into the erase seqnos.
 * @param data buffer containing EraseSeqno (FlatBuffers)
 * @return the erase seqnos, empty when data is empty (nothing to erase).
 *         Seqnos which are 0, repeated or out of order are corrected rather
 *         than rejected, the flusher and the history monitor read this
 *         document and must not fail on it.
 */
[[nodiscard]] EraseSeqnos decodeEraseSeqnos(cb::const_byte_buffer data);

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

} // namespace Collections::KVStore

template <>
struct fmt::formatter<Collections::KVStore::Manifest> : ostream_formatter {};
template <>
struct fmt::formatter<Collections::KVStore::OpenScope> : ostream_formatter {};
template <>
struct fmt::formatter<Collections::KVStore::OpenCollection>
    : ostream_formatter {};
