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

#include "collections/collection_persisted_stats.h"
#include "collections/collections_types.h"
#include "collections/kvstore.h"

#include <flatbuffers/flatbuffers.h>
#include <unordered_map>

class KVBucket;

namespace Collections::VB {

class Manifest;

/**
 * The Collections::VB::Flush object maintains data used in a single run of the
 * disk flusher for 1) Collection item counting and 2) persisted metadata
 * updates (when the flusher is flushing collection config changes).
 */
class Flush {
public:
    explicit Flush(Manifest& manifest) : manifest(manifest) {
    }

    const Manifest& getManifest() const {
        return manifest;
    }

    /**
     * KVStore implementations call this function and specific a callback.
     * This object will call cb for all collections that were flushed in the
     * run of the flusher and pass a PersistedStats object which the KVStore
     * should persist.
     *
     * @param a function to callback
     */
    void saveCollectionStats(
            std::function<void(CollectionID, const PersistedStats&)> cb) const;

    /**
     * KVStore implementations must call this function once they have
     * successfully committed all of the PersistedStats provided by the
     * saveCollectionStats call. This function will make the persisted changes
     * visible (i.e. cmd_stats will be able to return the new statistics).
     */
    void postCommitMakeStatsVisible();

    /**
     * Update collection stats from the flusher for a insert only operation.
     * We can be inserting a delete or a live document.
     *
     * @param key The key of the item flushed
     * @param seqno The seqno of the item flushed
     * @param isCommitted the prepare/commit state of the item flushed
     * @param isDelete alive/delete state of the item flushed
     * @param size bytes used on disk of the item flushed
     */
    void updateStats(const DocKey& key,
                     uint64_t seqno,
                     bool isCommitted,
                     bool isDelete,
                     size_t size);

    /**
     * Update collection stats from the flusher when an old 'version' of the
     * item already exists. This covers updates or deletes of items
     *
     * @param key The key of the item flushed
     * @param seqno The seqno of the item flushed
     * @param isCommitted the prepare/commit state of the item flushed
     * @param isDelete alive/delete stats of the item flushed
     * @param size bytes used on disk of the item flushed
     * @param oldSeqno The seqno of the old 'version' of the item
     * @param oldIsDelete alive/delete state of the old 'version' of the item
     * @param oldSize bytes used on disk of the old 'version' of the item
     */
    void updateStats(const DocKey& key,
                     uint64_t seqno,
                     bool isCommitted,
                     bool isDelete,
                     size_t size,
                     uint64_t oldSeqno,
                     bool oldIsDelete,
                     size_t oldSize);

    /**
     * Method for KVStore implementation to call before flushing a batch of
     * items - tells this Flush object about the collections that have been
     * dropped (and to be purged). Note if a KVStore knows there are no dropped
     * collections in storage, they can omit the call.
     */
    void setDroppedCollectionsForStore(
            const std::vector<Collections::KVStore::DroppedCollection>& v);

    /**
     * Called after a flush was successful so that purging can be triggered and
     * statistic changes applied.
     */
    void flushSuccess(Vbid vbid, KVBucket& bucket);

    /**
     * Trigger a purge of the given vbucket/bucket
     */
    static void triggerPurge(Vbid vbid, KVBucket& bucket);

    /**
     * Set that the KVStore needs to commit the data held in this object.
     */
    void setReadyForCommit() {
        this->needsMetaCommit = true;
    }

    bool isReadyForCommit() const {
        return needsMetaCommit;
    }

    // @return if the set of open collections is changing
    bool isOpenCollectionsChanged() const {
        return !collections.empty() || isDroppedCollectionsChanged();
    }

    // @return if the set of dropped collections is changing
    bool isDroppedCollectionsChanged() const {
        return !droppedCollections.empty();
    }

    // @return if the set of open scopes is changing
    bool isScopesChanged() const {
        return !scopes.empty() || isDroppedScopesChanged();
    }

    // @return if the set of dropped scopes is changing
    bool isDroppedScopesChanged() const {
        return !droppedScopes.empty();
    }

    // @return const reference to the map of dropped collections
    const std::unordered_map<CollectionID, KVStore::DroppedCollection>&
    getDroppedCollections() const {
        return droppedCollections;
    }

    void recordSystemEvent(const Item& item);

    /**
     * Record that a create collection was present in a commit batch
     */
    void recordCreateCollection(const Item& item);

    /**
     * Record that a drop collection was present in a commit batch
     */
    void recordDropCollection(const Item& item);

    /**
     * Record that a create scope was present in a commit batch
     */
    void recordCreateScope(const Item& item);

    /**
     * Record that a drop scope was present in a commit batch
     */
    void recordDropScope(const Item& item);

    /**
     * Encode the manifest commit meta data into a flatbuffer
     */
    flatbuffers::DetachedBuffer encodeManifestUid();

    /**
     * Encode the open collections list into a flatbuffer. Includes merging
     * with what was read off disk.
     * @param collections existing flatbuffer data for open collections
     */
    flatbuffers::DetachedBuffer encodeOpenCollections(
            cb::const_byte_buffer collections);

    /**
     * Encode the dropped collection list as flatbuffer.
     *
     * @param dropped list of collections that are already dropped (read from
     *        storage)
     * @return The dropped list (as a flatbuffer type)
     */
    flatbuffers::DetachedBuffer encodeDroppedCollections(
            std::vector<Collections::KVStore::DroppedCollection>&
                    existingDropped);

    /**
     * Encode open scopes list into flat buffer format.
     * @param scopes open scopes list
     */
    flatbuffers::DetachedBuffer encodeOpenScopes(cb::const_byte_buffer scopes);

private:
    /**
     * Set the ManifestUid from the create/drop events (but only the greatest
     * observed).
     */
    void setManifestUid(ManifestUid in);

    /**
     * Function determines if the collection @ seqno is dropped, but only
     * in the current uncommitted flush batch. E.g. if cid:0, seqno:100 and
     * the function returns true it means that this flush batch contains a
     * collection drop event for collection 0 with a seqno greater than 100.
     *
     * @param cid Collection to look-up.
     * @param seqno A seqno which was affected by the cid.
     */
    bool isLogicallyDeleted(CollectionID cid, uint64_t seqno) const;

    /**
     * Function determines if the collection @ seqno is dropped, but only
     * in the current store we are flushing too, i.e. the committed data.
     * E.g. if cid=0, seqno=100 and this function returns true it means that the
     * committed store has a collection drop event for collection 0 with a
     * seqno greater than 100.
     *
     * @param cid Collection to look-up.
     * @param seqno A seqno which was affected by the cid.
     */
    bool isLogicallyDeletedInStore(CollectionID cid, uint64_t seqno) const;

    // Helper class for doing collection stat updates
    class StatisticsUpdate {
    public:
        explicit StatisticsUpdate(uint64_t highSeqno)
            : persistedHighSeqno(highSeqno) {
        }

        /**
         * Set the persistedHighSeqno only if seqno is > than
         * this->persistedHighSeqno
         * @param seqno to use if it's greater than current value
         */
        void maybeSetPersistedHighSeqno(uint64_t seqno);

        /**
         * Process an insert into the collection
         * @param isSystem true if a system event is inserted
         * @param isDelete true if a deleted item is inserted (tombstone
         *        creation)
         * @param diskSize size in bytes 'inserted' into disk. Should be
         *        representative of the bytes used by each document, but does
         *        not need to be exact.
         */
        void insert(bool isSystem,
                    bool isDelete,
                    ssize_t diskSize);

        /**
         * Process an update into the collection
         * @param diskSizeDelta size in bytes difference. Should be
         *        representative of the difference between existing and new
         *        documents, but does not need to be exact.
         */
        void update(ssize_t diskSizeDelta);

        /**
         * Process a remove from the collection (store of a delete)
         * @param isSystem true if a system event is removed
         * @param diskSizeDelta size in bytes difference. Should be
         *        representative of the difference between existing and new
         *        documents, but does not need to be exact.
         */
        void remove(bool isSystem, ssize_t diskSizeDelta);

        /// @returns the highest persisted seqno recorded by the Flush object
        uint64_t getPersistedHighSeqno() const {
            return persistedHighSeqno;
        }

        /// @returns the items flushed (can be negative due to deletes)
        ssize_t getItemCount() const {
            return itemCount;
        }

        /// @returns the size of disk changes flushed (can be negative due to
        ///          deletes or replacements shrinking documents)
        ssize_t getDiskSize() const {
            return diskSize;
        }

    private:
        void incrementItemCount();

        void decrementItemCount();

        void updateDiskSize(ssize_t delta);

        uint64_t persistedHighSeqno{0};
        ssize_t itemCount{0};
        ssize_t diskSize{0};
    };

    /**
     * Obtain a Stats reference so insert/update/remove can be tracked.
     * The function may also update the persisted high-seqno of the collection
     * if the given seqno is greater than the currently recorded one.
     */
    StatisticsUpdate& getStatsAndMaybeSetPersistedHighSeqno(CollectionID cid,
                                                            uint64_t seqno);

    /**
     * Iterate through the 'droppedCollections' container and call a function
     * on the VB:Manifest to let it know the drop/seqno was persisted. This is
     * only done from flushSuccess.
     */
    void notifyManifestOfAnyDroppedCollections();

    /**
     * Called from the path of a successful flush.
     * Check to see if a collection purge is needed, if so schedules a task
     * which will iterate the vbucket's documents removing those of any dropped
     * collections. The actual task scheduled is compaction
     */
    void checkAndTriggerPurge(Vbid vbid, KVBucket& bucket) const;

    /**
     * A map of collections that have had items flushed and the statistics
     * gathered. E.g. the delta of disk and item count changes and the high
     * seqno.
     */
    std::unordered_map<CollectionID, StatisticsUpdate> stats;

    /**
     * A map of collections that are currently dropped (and persisted) in the
     * vbucket we are flushing to. As documents are flushed as 'updates' we need
     * to know if the old document is logically deleted.
     */
    std::unordered_map<CollectionID, KVStore::DroppedCollection> droppedInStore;

    /**
     * For each collection created in the batch, we record meta data of the
     * first and last (high/low by-seqno). If the collection was created once,
     * both entries are the same.
     */
    struct CollectionSpan {
        KVStore::OpenCollection low;
        KVStore::OpenCollection high;
    };
    std::unordered_map<CollectionID, CollectionSpan> collections;

    /**
     * For each scope created in the batch, we record meta data for the greatest
     * by-seqno.
     */
    std::unordered_map<ScopeID, KVStore::OpenScope> scopes;

    /**
     * For each collection dropped in the batch, we record the metadata of the
     * greatest
     */
    std::unordered_map<CollectionID, KVStore::DroppedCollection>
            droppedCollections;

    /**
     * For each scope dropped in the batch, we record the greatest seqno
     */
    std::unordered_map<ScopeID, uint64_t> droppedScopes;

    /**
     * The most recent manifest committed, if needsMetaCommit is true this value
     * must be stored by the underlying KVStore.
     */
    ManifestUid manifestUid{0};

    /**
     * ref to the 'parent' manifest for this VB::Flusher, this will receive item
     * count updates
     */
    Manifest& manifest;

    /**
     * Set to true when any of the fields in this structure have data which
     * should be saved in the KVStore update/commit. The underlying KVStore
     * reads this data and stores it in any suitable format (e.g. flatbuffers).
     */
    bool needsMetaCommit{false};
};

} // end namespace Collections::VB
