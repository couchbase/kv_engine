/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "collections/kvstore.h"
#include "ep_types.h"

#include <unordered_map>
#include <vector>

namespace Collections::VB {

struct PersistedStats;

using DroppedMap = std::unordered_map<CollectionID, KVStore::DroppedCollection>;

/**
 * The Collections::VB::FlushStats object provides code and data for accounting
 * a collection disk-size, item count and persisted high-seqno as data moves
 * from memory to persistent storage, i.e. via flushing. However the code is
 * equally required for concurrent compaction as we copy chunks of data from
 * an old file to a new file, new disk-sizes are calculated and the accounting
 * logic is identical to that of Flush
 */
class FlushAccounting {
public:
    FlushAccounting() = default;

    /**
     * Construct the FlushAccounting object and directly give the class a set
     * of collections that a KVStore says are dropped. The dropped collection
     * set is used for determining if updateStats should consider keys as
     * dropped
     */
    FlushAccounting(
            const std::vector<Collections::KVStore::DroppedCollection>& v);
    /**
     * preset the statistics of the collection. Subsequent updateStats calls
     * then account against preset values.
     *
     * @param cid Collection associated with the stats
     * @param stats The collections persisted stats
     */
    void presetStats(CollectionID cid, const PersistedStats& stats);

    /**
     * Update collection stats from for a insert only operation.
     * We can be inserting a delete or a live document.
     *
     * @param key The key of the item flushed
     * @param seqno The seqno of the item flushed
     * @param isCommitted the prepare/commit state of the item flushed
     * @param isDelete alive/delete state of the item flushed
     * @param size bytes used on disk of the item flushed
     * @param isCompaction Yes if the call originates from compaction replay
     * @param wantsDropped should we update dropped stats?
     */
    void updateStats(const DocKey& key,
                     uint64_t seqno,
                     IsCommitted isCommitted,
                     IsDeleted isDelete,
                     size_t size,
                     IsCompaction isCompaction = IsCompaction::No,
                     WantsDropped wantsDropped = WantsDropped::No);

    /**
     * Update collection stats when an old 'version' of the item already exists.
     * This covers updates or deletes of items
     *
     * @param key The key of the item flushed
     * @param seqno The seqno of the item flushed
     * @param isCommitted the prepare/commit state of the item flushed
     * @param isDelete alive/delete stats of the item flushed
     * @param size bytes used on disk of the item flushed
     * @param oldSeqno The seqno of the old 'version' of the item
     * @param oldIsDelete alive/delete state of the old 'version' of the item
     * @param oldSize bytes used on disk of the old 'version' of the item
     * @param isCompaction Yes if the call originates from compaction replay
     * @param wantsDropped should we update dropped stats?
     * @return bool should collections meta be updated due to stats change?
     */
    bool updateStats(const DocKey& key,
                     uint64_t seqno,
                     IsCommitted isCommitted,
                     IsDeleted isDelete,
                     size_t size,
                     uint64_t oldSeqno,
                     IsDeleted oldIsDelete,
                     size_t oldSize,
                     IsCompaction isCompaction = IsCompaction::No,
                     WantsDropped wantsDropped = WantsDropped::No);

    /**
     * Update the collection high-seqno (only if the flushed item is higher)
     *
     * @param key The key of the item flushed
     * @param seqno The seqno of the item flushed
     * @param isDelete alive/delete stats of the item flushed
     */
    void maybeUpdatePersistedHighSeqno(const DocKey& key,
                                       uint64_t seqno,
                                       bool isDelete);

    /**
     * Method for KVStore implementation to call before flushing a batch of
     * items - tells this Flush object about the collections that have been
     * dropped (and to be purged). Note if a KVStore knows there are no dropped
     * collections in storage, they can omit the call.
     * @param v vector of DroppedCollection objects representing persisted
     *        dropped collections.
     */
    void setDroppedCollectionsForStore(
            const std::vector<Collections::KVStore::DroppedCollection>&);

    /**
     * For each collection that we have stats tracked for, call the given
     * callback with the collection id and current stats.
     *
     * @param cb callback to invoke for each tracked collection
     */
    void forEachCollection(
            std::function<void(CollectionID, const PersistedStats&)> cb) const;

    /**
     * @return reference to the map of collections dropped by the flusher
     */
    const DroppedMap& getDroppedCollections() const {
        return droppedCollections;
    }

    /**
     * @return reference to the map of collections dropped by the flusher
     */
    DroppedMap& getDroppedCollections() {
        return droppedCollections;
    }

    // Helper class for doing collection stat updates
    class StatisticsUpdate {
    public:
        explicit StatisticsUpdate(uint64_t seqno) : persistedHighSeqno(seqno) {
        }

        explicit StatisticsUpdate(const PersistedStats& stats);

        /**
         * Set the persistedHighSeqno iff seqno is > than the current value
         *
         * @param seqno to use if it's greater than current value
         */
        void maybeSetPersistedHighSeqno(uint64_t seqno);

        /**
         * Process an insert into the collection
         * @param isSystem true if a system event is inserted
         * @param isDelete true if a deleted item is inserted (tombstone
         *        creation)
         * @param isCommitted does the item belong to the committed namespace?
         * @param diskSize size in bytes 'inserted' into disk. Should be
         *        representative of the bytes used by each document, but does
         *        not need to be exact.
         */
        void insert(IsSystem isSystem,
                    IsDeleted isDelete,
                    IsCommitted isCommitted,
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
         * @param isDelete true if a deleted item is inserted (tombstone
         *        creation)
         * @param isCommitted does the item belong to the committed namespace?
         * @param diskSizeDelta difference between new and old item
         */
        void remove(IsSystem isSystem,
                    IsDeleted isDelete,
                    IsCommitted isCommitted,
                    ssize_t diskSizeDelta);

        /**
         * @return the highest persisted seqno recorded by the Flush object.
         *         this includes prepare/abort and committed items
         */
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

        /// @returns true if a non-meta item has been persisted for this
        ///          collection in this flush batch
        bool itemInBatch() const {
            return flushedItem;
        }

    private:
        void incrementItemCount();

        void decrementItemCount();

        void updateDiskSize(ssize_t delta);

        uint64_t persistedHighSeqno{0};
        ssize_t itemCount{0};
        ssize_t diskSize{0};
        bool flushedItem{false};
    };

    using StatsMap = std::unordered_map<CollectionID, StatisticsUpdate>;
    const StatsMap& getStats() const {
        return stats;
    }

    const StatsMap& getDroppedStats() const {
        return droppedStats;
    }

private:
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

    /**
     * Obtain a Stats reference so insert/update/remove can be tracked.
     * The function may also update the persisted high-seqno of the collection
     * if the given seqno is greater than the currently recorded one.
     *
     * @param stats StatsMap in which to look for our stats
     * @param cid CollectionID
     * @param seqno New high seqno to potentially update the persisted one
     * @return Stats reference
     */
    StatisticsUpdate& getStatsAndMaybeSetPersistedHighSeqno(StatsMap& stats,
                                                            CollectionID cid,
                                                            uint64_t seqno);

    /**
     * Obtain a Stats reference so insert/update/remove can be tracked.
     * The function may also update the persisted high-seqno of the collection
     * if the given seqno is greater than the currently recorded one.
     *
     * @param cid CollectionID
     * @param seqno New high seqno to potentially update the persisted one
     * @param wantsDropped Wants the stats of dropped collections
     * @return Stats reference
     */
    StatisticsUpdate& getStatsAndMaybeSetPersistedHighSeqno(
            CollectionID cid,
            uint64_t seqno,
            WantsDropped wantsDropped = WantsDropped::No);

    /**
     * Helper for updateStats
     * Processes the collection create or drop event that was seen by
     * updateStats and applies changes to the stats maps.
     * @param cid The collection associated with the event
     * @param isDelete Yes/No - drop vs create collection
     * @param isCompaction Yes if called from compaction replay
     * @return true if isDelete is Yes (i.e. drop collection)
     */
    bool processSystemEvent(CollectionID cid,
                            IsDeleted isDelete,
                            IsCompaction isCompaction);

    /**
     * A map of collections that have had items flushed and the statistics
     * gathered. E.g. the delta of disk and item count changes and the high
     * seqno.
     */
    StatsMap stats;

    /**
     * Similar to stats, but these collections have been dropped and now are
     * logicallyDeleted. We don't track these under "stats" as some backends
     * don't keep around stats for deleted collections
     */
    StatsMap droppedStats;

    /**
     * For each collection dropped in the batch, we record the metadata of the
     * greatest
     */
    DroppedMap droppedCollections;

    /**
     * A map of collections that are currently dropped (and persisted) in the
     * vbucket we are flushing to. As documents are flushed as 'updates' we need
     * to know if the old document is logically deleted.
     */
    DroppedMap droppedInStore;
};
} // namespace Collections::VB