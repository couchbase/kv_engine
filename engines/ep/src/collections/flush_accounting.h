/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021 Couchbase, Inc
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

#include "collections/kvstore.h"
#include "ep_types.h"

#include <unordered_map>
#include <vector>

namespace Collections::VB {

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
    /**
     * Update collection stats from for a insert only operation.
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
                     IsCommitted isCommitted,
                     IsDeleted isDelete,
                     size_t size);

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
     */
    void updateStats(const DocKey& key,
                     uint64_t seqno,
                     IsCommitted isCommitted,
                     IsDeleted isDelete,
                     size_t size,
                     uint64_t oldSeqno,
                     IsDeleted oldIsDelete,
                     size_t oldSize);

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
     */
    void setDroppedCollectionsForStore(
            const std::vector<Collections::KVStore::DroppedCollection>& v);

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

    private:
        void incrementItemCount();

        void decrementItemCount();

        void updateDiskSize(ssize_t delta);

        uint64_t persistedHighSeqno{0};
        ssize_t itemCount{0};
        ssize_t diskSize{0};
    };

    using StatsMap = std::unordered_map<CollectionID, StatisticsUpdate>;
    const StatsMap& getStats() const {
        return stats;
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
     * @param cid CollectionID
     * @param seqno New high seqno to potentially update the persisted one
     * @return Stats reference
     */
    StatisticsUpdate& getStatsAndMaybeSetPersistedHighSeqno(CollectionID cid,
                                                            uint64_t seqno);

    /**
     * A map of collections that have had items flushed and the statistics
     * gathered. E.g. the delta of disk and item count changes and the high
     * seqno.
     */
    StatsMap stats;

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