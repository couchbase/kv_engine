/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "collections/collections_types.h"
#include "collections/flush_accounting.h"
#include "collections/kvstore.h"
#include "ep_types.h"

#include <flatbuffers/flatbuffers.h>
#include <unordered_map>
#include <unordered_set>

class EPBucket;

namespace Collections::VB {

class Manifest;
struct PersistedStats;

/**
 * The Collections::VB::Flush object maintains data used in a single run of the
 * disk flusher for 1) Collection item counting and 2) persisted metadata
 * updates (when the flusher is flushing collection config changes).
 */
class Flush {
public:
    explicit Flush(Manifest& manifest) : manifest(manifest) {
    }

    /**
     * KVStore implementations call this function and specific a callback.
     * This object will call cb for all collections that were flushed in the
     * run of the flusher and pass a PersistedStats object which the KVStore
     * should persist.
     *
     * @param vbid vbucket flushing
     * @param a function to callback
     */
    void saveCollectionStats(
            Vbid vbid,
            std::function<void(CollectionID, const PersistedStats&)> cb);

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
     * @param compactionCallbacks For which items does the store invoke the
     *                            compaction callbacks?
     * @return if the collection disk size stat now reflects the new item size
     */
    bool updateStats(const DocKeyView& key,
                     uint64_t seqno,
                     IsCommitted isCommitted,
                     IsDeleted isDelete,
                     size_t size,
                     CompactionCallbacks compactionCallbacks =
                             CompactionCallbacks::LatestRevision);

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
     * @param compactionCallbacks For which items does the store invoke the
     *                            compaction callbacks?
     *
     * @return true if this is a logical insert (the item previously belonged to
     *         and old generation of the collection).
     */
    FlushAccounting::UpdateStatsResult updateStats(
            const DocKeyView& key,
            uint64_t seqno,
            IsCommitted isCommitted,
            IsDeleted isDelete,
            size_t size,
            uint64_t oldSeqno,
            IsDeleted oldIsDelete,
            size_t oldSize,
            CompactionCallbacks compactionCallbacks =
                    CompactionCallbacks::LatestRevision);

    /**
     * Update the collection high-seqno (only if the flushed item is higher)
     *
     * @param key The key of the item flushed
     * @param seqno The seqno of the item flushed
     * @param isDelete alive/delete stats of the item flushed
     */
    void maybeUpdatePersistedHighSeqno(const DocKeyView& key,
                                       uint64_t seqno,
                                       bool isDelete) {
        flushAccounting.maybeUpdatePersistedHighSeqno(key, seqno, isDelete);
    }

    /**
     * Method for KVStore implementation to call before flushing a batch of
     * items - tells this Flush object about the collections that have been
     * dropped (and to be purged). Note if a KVStore knows there are no dropped
     * collections in storage, they can omit the call.
     */
    void setDroppedCollectionsForStore(
            const std::vector<Collections::KVStore::DroppedCollection>& v);

    /**
     * Set the ManifestUid from the create/drop events (but only the greatest
     * observed). May also be called by the KVStore should a flush require a
     * stats update that doesn't add/remove a collection to set the current
     * manifest uid.
     */
    void setManifestUid(ManifestUid uid);

    ManifestUid getManifestUid() const {
        return manifestUid;
    }

    /**
     * Set the manifest to the given manifest. Required by the NexusKVStore to
     * generate a VB::Commit/Collections::Flush to track collections stats
     * for the secondary KVStore without altering the stats tracked in the
     * primary VB::Manifest.
     */
    void setManifest(Manifest& newManifest);

    /// Returns the VB::Manifest currently in use
    VB::Manifest& getManifest() const;

    /**
     * Called after a flush was successful so that purging can be triggered and
     * statistic changes applied.
     */
    void flushSuccess(Vbid vbid, EPBucket& bucket);

    /**
     * Call the given callback for each collection that is 100% dropped in this
     * flush batch. Collections which are dropped and never recreated in the
     * batch will have the callback invoked, collections dropped and then
     * created again are not included.
     *
     * @param a function to call with the ID of each dropped collection
     */
    void forEachDroppedCollection(std::function<void(CollectionID)> cb) const;

    /**
     * Trigger a purge of the given vbucket/bucket
     */
    static void triggerPurge(Vbid vbid, EPBucket& bucket);

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
        return !collections.empty() || !collectionMods.empty() ||
               isDroppedCollectionsChanged();
    }

    // @return if the set of dropped collections is changing or a collection was
    //         flushed.
    bool isDroppedCollectionsChanged() const {
        return !flushAccounting.getDroppedCollections().empty() ||
               !flushAccounting.getFlushedCollections().empty();
    }

    // @return if the set of dropped stats is changing
    bool isDroppedStatsChanged() const {
        return !flushAccounting.getDroppedStats().empty();
    }

    // @return if the set of open scopes is changing
    bool isScopesChanged() const {
        return !scopes.empty() || isDroppedScopesChanged();
    }

    // @return if the set of dropped scopes is changing
    bool isDroppedScopesChanged() const {
        return !droppedScopes.empty();
    }

    /// @return const reference to the map of dropped collections
    const std::unordered_map<CollectionID, KVStore::DroppedCollection>&
    getDroppedCollections() const {
        return flushAccounting.getDroppedCollections();
    }

    /// @return the stats for a given dropped collection
    FlushAccounting::StatisticsUpdate getDroppedStats(CollectionID cid);

    /// @return reference to the FlushAccounting droppedStats map
    const FlushAccounting::StatsMap& getDroppedStats() const;

    void recordSystemEvent(const Item& item);

    /**
     * Record that a create collection was present in a commit batch
     */
    void recordCreateCollection(const Item& item);

    /**
     * Record that a collection was modified in a commit batch
     */
    void recordModifyCollection(const Item& item);

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
     * Method is used by compaction, but works with data that flush maintains
     * i.e. the output of encodeDroppedCollections.
     *
     * This method encodes a new dropped collections 'list'. The new list is the
     * relative complement of the parameters:
     * A (droppedCollections) and B (idsToRemove).
     *
     * E.g. if A[0, 1, 2] and B[0, 1] the function returns as flatbuffer data
     * set[2]
     *
     * If the output is an empty set, the returned DetachedBuffer has no data.
     * This allows the caller to determine the output is the empty set and
     * delete the document which may own the DroppedCollections data.
     *
     * @param droppedCollection vector of known dropped collections constructed
     *        from the post compaction snapshot
     * @param postCompactionDropped the set of IDs that are known to be dropped
     *        in the pre compaction snapshot.
     * @param endSeqno the end-seqno (greatest collection end) from the pre
     *        compaction snapshot
     *
     * @return A new set of dropped collections as per the description above.
     *         This is flatbuffer encoding of DroppedCollections (kvstore.fbs)
     */
    static flatbuffers::DetachedBuffer
    encodeRelativeComplementOfDroppedCollections(
            const std::vector<Collections::KVStore::DroppedCollection>&
                    droppedCollections,
            const std::unordered_set<CollectionID>& idsToRemove,
            uint64_t endSeqno);

    /**
     * Encode open scopes list into flat buffer format.
     * @param scopes open scopes list
     */
    flatbuffers::DetachedBuffer encodeOpenScopes(cb::const_byte_buffer scopes);

    /**
     * @return if dropped collections exist in storage
     */
    bool droppedCollectionsExists() const;

    // @return was the collection opened in this batch?
    bool isOpen(CollectionID cid) const;

private:
    /**
     * After all flushed items and system events have been processed this
     * function counts how many non-empty collections were dropped.
     *
     * non-empty is defined as a collection with 1 or more *committed* items.
     * Detection of non-empty requires comparing the start-seqno with the
     * collection's high-seqno.
     *
     * @return the number of non-empty collections that were dropped.
     */
    uint32_t countNonEmptyDroppedCollections() const;


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
    void checkAndTriggerPurge(Vbid vbid, EPBucket& bucket) const;

    /**
     * Return the collection metadata relevant to a collection created @ seqno.
     * The returned value either returns the input metadata or if a modification
     * event is in the flush batch, it will return the modified metadata. This
     * data can then be pushed to the collection "vbstate" metadata as the
     * correct state of the collection.
     *
     * @param cid The collection created (and maybe modified)
     * @param seqno The sequence number of the collection create
     * @oaram orginalMeta The original collection metadata, which is returned
     *        if there is no flushed modification
     * @return The metadata to use in writing out the collection state
     */
    CollectionMetaData getMaybeModifiedCollectionMetaData(
            CollectionID cid,
            uint64_t seqno,
            const CollectionMetaData& orginalMeta) const;

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
     * For each collection modified in the batch we record the meta data for the
     * greatest by-seqno
     */
    std::unordered_map<CollectionID, KVStore::OpenCollection> collectionMods;

    /**
     * For each scope created in the batch, we record meta data for the greatest
     * by-seqno.
     */
    std::unordered_map<ScopeID, KVStore::OpenScope> scopes;

    /**
     * For each scope dropped in the batch, we record the greatest seqno
     */
    std::unordered_map<ScopeID, uint64_t> droppedScopes;

    /**
     * Used for accounting items, disk-size and high-seqno during flushing
     */
    FlushAccounting flushAccounting{};

    /**
     * The most recent manifest committed, if needsMetaCommit is true this value
     * must be stored by the underlying KVStore.
     */
    ManifestUid manifestUid{0};

    /**
     * ref to the 'parent' manifest for this VB::Flusher, this will receive item
     * count updates
     */
    std::reference_wrapper<Manifest> manifest;

    /**
     * Flushing counts how many non-empty collections were committed and uses
     * this for triggering (or not) a purge.
     */
    uint32_t nonEmptyDroppedCollections{0};

    /**
     * Set to true when any of the fields in this structure have data which
     * should be saved in the KVStore update/commit. The underlying KVStore
     * reads this data and stores it in any suitable format (e.g. flatbuffers).
     */
    bool needsMetaCommit{false};
};

} // end namespace Collections::VB
