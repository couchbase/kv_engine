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

#include "collections/flush_accounting.h"
#include "collections/collection_persisted_stats.h"
#include "systemevent_factory.h"

namespace Collections::VB {

static std::pair<bool, std::optional<CollectionID>> getCollectionID(
        const DocKey& key) {
    bool isSystemEvent = key.isInSystemCollection();
    CollectionID cid;
    if (isSystemEvent) {
        auto [event, id] = SystemEventFactory::getTypeAndID(key);
        switch (event) {
        case SystemEvent::Collection: {
            cid = CollectionID(id);
            break;
        }
        case SystemEvent::Scope:
            return {true, {}};
        }
    } else {
        cid = key.getCollectionID();
    }
    return {isSystemEvent, cid};
}

FlushAccounting::StatisticsUpdate&
FlushAccounting::getStatsAndMaybeSetPersistedHighSeqno(
        CollectionID cid, uint64_t seqno, WantsDropped wantsDropped) {
    if (isLogicallyDeleted(cid, seqno) && wantsDropped == WantsDropped::Yes) {
        getStatsAndMaybeSetPersistedHighSeqno(stats, cid, seqno);

        return getStatsAndMaybeSetPersistedHighSeqno(droppedStats, cid, seqno);
    }

    return getStatsAndMaybeSetPersistedHighSeqno(stats, cid, seqno);
}

FlushAccounting::StatisticsUpdate&
FlushAccounting::getStatsAndMaybeSetPersistedHighSeqno(StatsMap& stats,
                                                       CollectionID cid,
                                                       uint64_t seqno) {
    auto [itr, inserted] = stats.try_emplace(cid, StatisticsUpdate{seqno});
    auto& [key, value] = *itr;
    (void)key;
    if (!inserted) {
        value.maybeSetPersistedHighSeqno(seqno);
    }

    return value;
}

FlushAccounting::StatisticsUpdate::StatisticsUpdate(const PersistedStats& stats)
    : persistedHighSeqno(stats.highSeqno),
      itemCount(stats.itemCount),
      diskSize(stats.diskSize) {
}

void FlushAccounting::StatisticsUpdate::maybeSetPersistedHighSeqno(
        uint64_t seqno) {
    if (seqno > persistedHighSeqno) {
        persistedHighSeqno = seqno;
    }
}

void FlushAccounting::StatisticsUpdate::incrementItemCount() {
    itemCount++;
}

void FlushAccounting::StatisticsUpdate::decrementItemCount() {
    itemCount--;
}

void FlushAccounting::StatisticsUpdate::updateDiskSize(ssize_t delta) {
    diskSize += delta;
}

void FlushAccounting::StatisticsUpdate::insert(IsSystem isSystem,
                                               IsDeleted isDelete,
                                               IsCommitted isCommitted,
                                               ssize_t diskSizeDelta) {
    if (isSystem == IsSystem::No && isDelete == IsDeleted::No &&
        isCommitted == IsCommitted::Yes) {
        incrementItemCount();
    }

    if (isSystem == IsSystem::No) {
        flushedItem = true;
    }

    // else inserting a collection-start/prepare/tombstone/abort:
    // no item increment but account for the disk size change
    updateDiskSize(diskSizeDelta);
}

void FlushAccounting::StatisticsUpdate::update(ssize_t diskSizeDelta) {
    // System events don't get updated so just set flushedItem to true
    flushedItem = true;

    updateDiskSize(diskSizeDelta);
}

void FlushAccounting::StatisticsUpdate::remove(IsSystem isSystem,
                                               IsDeleted isDelete,
                                               IsCommitted isCommitted,
                                               ssize_t diskSizeDelta) {
    if (isSystem == IsSystem::No && isCommitted == IsCommitted::Yes) {
        decrementItemCount();
    }

    if (isSystem == IsSystem::No) {
        flushedItem = true;
    }

    updateDiskSize(diskSizeDelta);
}

FlushAccounting::FlushAccounting(
        const std::vector<Collections::KVStore::DroppedCollection>& v,
        IsCompaction isCompaction) {
    setDroppedCollectionsForStore(v, isCompaction);
}

void FlushAccounting::presetStats(CollectionID cid,
                                  const PersistedStats& preStats) {
    auto [itr, inserted] = stats.try_emplace(cid, StatisticsUpdate{preStats});
    (void)itr;

    // This function is used from a path where this should not be true, where
    // only unique keys should be found.
    Expects(inserted && "presetStats must insert unique collections");
}

bool FlushAccounting::processSystemEvent(CollectionID cid,
                                         IsDeleted isDelete,
                                         IsCompaction isCompaction) {
    // If the update comes from compaction (where replay is copying data) then
    // unconditionally remove the collection from the stats map. A create or
    // or drop collection is the start or end of the collection - in both cases
    // we don't need what is in the map. For create collection if this does
    // remove an entry, then we have a resurrect case and must account following
    // items against 0. For drop, we will be removing the stats from storage.
    if (isCompaction == IsCompaction::Yes) {
        stats.erase(cid);
    }

    return isDelete == IsDeleted::Yes;
}

void FlushAccounting::updateStats(const DocKey& key,
                                  uint64_t seqno,
                                  IsCommitted isCommitted,
                                  IsDeleted isDelete,
                                  size_t size,
                                  IsCompaction isCompaction,
                                  WantsDropped wantsDropped) {
    auto [isSystemEvent, cid] = getCollectionID(key);

    if (!cid) {
        // The key is not for a collection (e.g. a scope event).
        return;
    }

    // System events have extra handling and may terminate the stat update
    if (isSystemEvent && processSystemEvent(*cid, isDelete, isCompaction)) {
        // This was a drop collection event, we don't update the collection
        // stats for this case.
        return;
    }

    // Track high-seqno for the item
    auto& collsFlushStats = getStatsAndMaybeSetPersistedHighSeqno(
            cid.value(), seqno, wantsDropped);

    // If we want the dropped stats then getStatsAndMaybeSetPersistedHighSeqno
    // would have returned a reference to stats in droppedCollections.
    // if we did the empty collection  detection will fail because the
    // high-seqno of the collection will change to be equal to the drop-event's
    // seqno. Empty collection detection relies on start-seqno == high-seqno.
    if (!isLogicallyDeleted(cid.value(), seqno) ||
        wantsDropped == WantsDropped::Yes) {
        collsFlushStats.insert(isSystemEvent ? IsSystem::Yes : IsSystem::No,
                               isDelete,
                               isCommitted,
                               size);
    }
}

bool FlushAccounting::updateStats(const DocKey& key,
                                  uint64_t seqno,
                                  IsCommitted isCommitted,
                                  IsDeleted isDelete,
                                  size_t size,
                                  uint64_t oldSeqno,
                                  IsDeleted oldIsDelete,
                                  size_t oldSize,
                                  IsCompaction isCompaction,
                                  WantsDropped wantsDropped) {
    bool updateMeta = false;

    // Same logic (and comments) apply as per the above updateStats function.
    auto [systemEvent, cid] = getCollectionID(key);
    auto isSystemEvent = systemEvent ? IsSystem::Yes : IsSystem::No;
    if (!cid) {
        return false;
    }

    // System events have extra handling and may terminate the stat update
    if (isSystemEvent == IsSystem::Yes &&
        processSystemEvent(*cid, isDelete, isCompaction)) {
        return false;
    }

    auto& collsFlushStats = getStatsAndMaybeSetPersistedHighSeqno(
            cid.value(), seqno, wantsDropped);

    // Logically deleted items don't update item-count/disk-size
    if (isLogicallyDeleted(cid.value(), seqno) &&
        wantsDropped == WantsDropped::No) {
        return false;
    }

    if (isSystemEvent == IsSystem::No && wantsDropped == WantsDropped::Yes &&
        (isLogicallyDeleted(cid.value(), oldSeqno) ||
         isLogicallyDeletedInStore(cid.value(), oldSeqno))) {
        // When we resurrect a collection and update a stat in it in this flush
        // batch (i.e. the key previously existed on disk for the old generation
        // of the collection) we need to decrement our dropped stats by that
        // value as the key will still exist and isn't going to go away now.
        auto& dropped = getStatsAndMaybeSetPersistedHighSeqno(
                droppedStats, cid.value(), seqno);

        // Item used to contribute towards the item count, and we're now
        // updating (meaning that it belongs to a new generation of the
        // collection). If we're here then we haven't purged the original
        // (dropped) collection yet so we need to "forget" about the old
        // versions contributions towards the item count.
        if (oldIsDelete == IsDeleted::No) {
            dropped.remove(IsSystem::No,
                           IsDeleted::No,
                           IsCommitted::Yes,
                           size - oldSize);
        }

        // May not have dropped the collection we're concerned with in this
        // batch but we need to make sure that the stat update makes it to disk,
        // setting updateMeta does this by telling Collections::Flush that some
        // metadata must be updated at the end of this flush batch.
        updateMeta = true;
    }

    // Of interest next is the state of old vs new. An update can become an
    // insert or remove.
    //
    // The following defines our expected old and new states. Note that only
    // committed items actually increment the item count (and that is handled
    // in FlushAccounting::StatisticsUpdate::insert/remove).
    //
    // The old key can be live, deleted or dropped.
    // The new key can be live or deleted.
    //
    // new key is live:
    //   * old key dropped: Key is an insert => items += 1, diskSize += size
    //   * old key deleted: Key is an insert => items += 1, diskSize += delta
    //   * old key live: Key is an update => diskSize += delta
    //
    // new key is deleted
    //   * old key is dropped: Key is an update => diskSize += size
    //   * old key is deleted: Key is an update => diskSize += delta
    //   * old key is live: Key is a remove => items -= 1, diskSize += delta
    //
    // Note that old can be both dropped and deleted (a dropped tombstone). In
    // that case we process as dropped.
    const bool oldIsDropped =
            (isLogicallyDeleted(cid.value(), oldSeqno) ||
             isLogicallyDeletedInStore(cid.value(), oldSeqno));

    if (isDelete == IsDeleted::No) {
        // new key is live
        if (oldIsDropped) {
            // insert with the new size
            collsFlushStats.insert(isSystemEvent, isDelete, isCommitted, size);
        } else if (oldIsDelete == IsDeleted::Yes) {
            // insert with the delta of old/new
            collsFlushStats.insert(
                    isSystemEvent, isDelete, isCommitted, size - oldSize);
        } else {
            // update with the delta
            collsFlushStats.update(size - oldSize);
        }

    } else {
        // new key is delete
        if (oldIsDropped) {
            // update with the size of the new tombstone
            collsFlushStats.update(size);
        } else if (oldIsDelete == IsDeleted::Yes) {
            // update with the delta of old/new
            collsFlushStats.update(size - oldSize);
        } else {
            // remove
            collsFlushStats.remove(
                    isSystemEvent, isDelete, isCommitted, size - oldSize);
        }
    }

    return updateMeta;
}

void FlushAccounting::maybeUpdatePersistedHighSeqno(const DocKey& key,
                                                    uint64_t seqno,
                                                    bool isDelete) {
    // Same logic and comment as updateStats.
    auto [isSystemEvent, cid] = getCollectionID(key);

    if (!cid) {
        return;
    }

    if (isDelete && isSystemEvent) {
        return;
    }
    // don't care for the return value, just update the persisted high seqno
    getStatsAndMaybeSetPersistedHighSeqno(cid.value(), seqno);
}

bool FlushAccounting::isLogicallyDeleted(CollectionID cid,
                                         uint64_t seqno) const {
    auto itr = droppedCollections.find(cid);
    if (itr != droppedCollections.end()) {
        return seqno <= itr->second.endSeqno;
    }
    return false;
}

bool FlushAccounting::isLogicallyDeletedInStore(CollectionID cid,
                                                uint64_t seqno) const {
    auto itr = droppedInStore.find(cid);
    if (itr != droppedInStore.end()) {
        return seqno <= itr->second.endSeqno;
    }
    return false;
}

void FlushAccounting::setDroppedCollectionsForStore(
        const std::vector<Collections::KVStore::DroppedCollection>& v,
        IsCompaction isCompaction) {
    for (const auto& c : v) {
        droppedInStore.emplace(c.collectionId, c);

        if (isCompaction == IsCompaction::Yes) {
            // Update the dropped map now as compaction knows ahead of the
            // replay what collections are dropped.
            droppedCollections.emplace(c.collectionId, c);
        }
    }
}

// Called from KVStore during flush or compaction replay
// This method iterates through the statistics gathered by the Flush/replay and
// uses the std::function callback to have the KVStore implementation write them
// to storage, e.g. a local document.
void FlushAccounting::forEachCollection(
        std::function<void(CollectionID, const PersistedStats&)> cb) const {
    // For each collection modified in the flush run ask the VBM for the
    // current stats (using the high-seqno so we find the correct generation
    // of stats)
    for (const auto& [cid, flushStats] : stats) {
        // Don't generate an update of the statistics for a dropped collection.
        // 1) it's wasted effort
        // 2) the kvstore may not be able handle an update of a 'local' doc
        //    and a delete of the same in one flush (couchstore doesn't)
        auto itr = droppedInStore.find(cid);
        if (itr != droppedInStore.end() &&
            flushStats.getPersistedHighSeqno() < itr->second.endSeqno) {
            continue;
        }

        // Generate new stats, add the deltas from this flush batch for count
        // and size and set the high-seqno (which includes prepares)
        PersistedStats ps(flushStats.getItemCount(),
                          flushStats.getPersistedHighSeqno(),
                          flushStats.getDiskSize());
        cb(cid, ps);
    }
}

void FlushAccounting::forEachDroppedCollection(
        std::function<void(CollectionID)> cb) const {
    // To invoke the callback only for dropped collections iterate the dropped
    // map and then check in the 'stats' map (and if found do an ordering check)
    for (const auto& [cid, dropped] : getDroppedCollections()) {
        auto itr = getStats().find(cid);
        if (itr == getStats().end() ||
            dropped.endSeqno > itr->second.getPersistedHighSeqno()) {
            // collection's endSeqno exceeds the persistedHighSeqno. The drop
            // is the greatest event against the collection in the batch, we
            // will invoke the callback.
            cb(cid);
        }
    }
}

} // namespace Collections::VB
