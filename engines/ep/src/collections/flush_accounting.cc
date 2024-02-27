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

/**
 * From a DocKey extract CollectionID and SystemEvent data.
 * The function returns two optionals because depending on the key there could
 * be no SystemEvent (regular mutation) or no CollectionID (Scope event) - or we
 * could have both (e.g. Collection SystemEvent).
 *
 * @param key a DocKey from a flushed Item
 * @return a pair of optional. first (SystemEvent) is initialised if the key
 *         belongs to the SystemCollection (collection 1) - the value is the
 *         type of event that the key represents. The second (CollectionID) is
 *         initialised with a CollectionID where relevant. E.g. CreateScope
 *         leaves this as std::nullopt but CreateCollection will set it to the
 *         collection's ID. All non system keys will also initialise second with
 *         the CollectionID.
 */
static std::pair<std::optional<SystemEvent>, std::optional<CollectionID>>
getCollectionEventAndCollectionID(const DocKey& key) {
    if (key.isInSystemEventCollection()) {
        auto [event, id] = SystemEventFactory::getTypeAndID(key);
        switch (event) {
        case SystemEvent::ModifyCollection:
        case SystemEvent::Collection: {
            return {event, CollectionID(id)};
        }
        case SystemEvent::Scope:
            return {std::nullopt, std::nullopt};
        }
    }
    // No event, but we do have a collection id
    return {std::nullopt, key.getCollectionID()};
}

FlushAccounting::StatisticsUpdate&
FlushAccounting::getStatsAndMaybeSetPersistedHighSeqno(
        CollectionID cid,
        uint64_t seqno,
        CompactionCallbacks compactionCallbacks) {
    if (isLogicallyDeleted(cid, seqno) &&
        compactionCallbacks == CompactionCallbacks::AnyRevision) {
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

bool FlushAccounting::StatisticsUpdate::insert(
        IsSystem isSystem,
        IsDeleted isDelete,
        IsCommitted isCommitted,
        CompactionCallbacks compactionCallbacks,
        ssize_t diskSizeDelta) {
    if (isSystem == IsSystem::No && isDelete == IsDeleted::No &&
        isCommitted == IsCommitted::Yes) {
        incrementItemCount();
    }

    if (isSystem == IsSystem::No) {
        flushedItem = true;
    }

    if (isDelete == IsDeleted::Yes &&
        compactionCallbacks == CompactionCallbacks::AnyRevision) {
        // Not tracking tombstones in the disk size for magma because we can't
        // decrement by the correct amount when purging them as we may purge
        // stale versions
        return false;
    }

    // else inserting a collection{start,modify}/prepare/tombstone/abort:
    // no item increment but account for the disk size change
    updateDiskSize(diskSizeDelta);
    return true;
}

void FlushAccounting::StatisticsUpdate::update(ssize_t diskSizeDelta) {
    // System events don't get updated so just set flushedItem to true
    flushedItem = true;

    updateDiskSize(diskSizeDelta);
}

bool FlushAccounting::StatisticsUpdate::remove(
        IsSystem isSystem,
        IsDeleted isDelete,
        IsCommitted isCommitted,
        CompactionCallbacks compactionCallbacks,
        size_t oldSize,
        size_t newSize) {
    if (isSystem == IsSystem::No && isCommitted == IsCommitted::Yes) {
        decrementItemCount();
    }

    if (isSystem == IsSystem::No) {
        flushedItem = true;
    }

    if (compactionCallbacks == CompactionCallbacks::AnyRevision &&
        isDelete == IsDeleted::Yes) {
        updateDiskSize(-oldSize);
        return false;
    }

    updateDiskSize(newSize - oldSize);
    return true;
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

    if (isPiTR == IsPiTR::No) {
        // This function is used from a path where this should not be true,
        // where only unique keys should be found.
        Expects(inserted && "presetStats must insert unique collections");
    }
}

bool FlushAccounting::checkAndMaybeProcessSystemEvent(
        SystemEvent event,
        CollectionID cid,
        IsDeleted isDelete,
        IsCompaction isCompaction) {
    switch (event) {
    case SystemEvent::ModifyCollection:
        // Modify event - no processing
        return false;
    case SystemEvent::Collection:
        // Collection create/drop - break and process
        break;
    default:
        // E.g. Scope
        throw std::logic_error(
                "checkAndMaybeProcessSystemEvent unexpected event" +
                std::to_string(int(event)));
    }

    // If the update comes from compaction (where replay is copying data) then
    // unconditionally remove the collection from the stats map. A create or
    // or drop collection is the start or end of the collection - in both cases
    // we don't need what is in the map. For create collection if this does
    // remove an entry, then we have a resurrect case and must account following
    // items against 0. For drop, we will be removing the stats from storage.
    if (isCompaction == IsCompaction::Yes) {
        stats.erase(cid);
    }

    // caller stops processing if this function returns true - DropCollection
    // stops the processing.
    return isDelete == IsDeleted::Yes;
}

bool FlushAccounting::updateStats(const DocKey& key,
                                  uint64_t seqno,
                                  IsCommitted isCommitted,
                                  IsDeleted isDelete,
                                  size_t size,
                                  IsCompaction isCompaction,
                                  CompactionCallbacks compactionCallbacks) {
    const auto [event, cid] = getCollectionEventAndCollectionID(key);

    if (!cid) {
        // If the key is not associated with a Collection then abandon
        // processing. This must mean the key is a Scope event and as such does
        // not change collection stats.
        return false;
    }

    if (event &&
        checkAndMaybeProcessSystemEvent(*event, *cid, isDelete, isCompaction)) {
        // This case is reached for a DropCollection event. No collection stats
        // are to be changed.
        return false;
    }

    // At this point the key is a normal mutation/delete so will for example
    // increment or decrement the collection item count.
    // Or the key is a SystemEvent Create/Modify collection, which needs to
    // change the collection disk size and high-seqno.

    // Track high-seqno for the item
    auto& collsFlushStats = getStatsAndMaybeSetPersistedHighSeqno(
            cid.value(), seqno, compactionCallbacks);

    // If we want the dropped stats then getStatsAndMaybeSetPersistedHighSeqno
    // would have returned a reference to stats in droppedCollections.
    // if we did the empty collection  detection will fail because the
    // high-seqno of the collection will change to be equal to the drop-event's
    // seqno. Empty collection detection relies on start-seqno == high-seqno.
    if (!isLogicallyDeleted(cid.value(), seqno) ||
        compactionCallbacks == CompactionCallbacks::AnyRevision) {
        return collsFlushStats.insert(event ? IsSystem::Yes : IsSystem::No,
                                      isDelete,
                                      isCommitted,
                                      compactionCallbacks,
                                      size);
    }
    return false;
}

FlushAccounting::UpdateStatsResult FlushAccounting::updateStats(
        const DocKey& key,
        uint64_t seqno,
        IsCommitted isCommitted,
        IsDeleted isDelete,
        size_t size,
        uint64_t oldSeqno,
        IsDeleted oldIsDelete,
        size_t oldSize,
        IsCompaction isCompaction,
        CompactionCallbacks compactionCallbacks) {
    // Same logic (and comments) apply as per the above updateStats function.
    const auto [event, cid] = getCollectionEventAndCollectionID(key);
    if (!cid) {
        return {};
    }

    if (event &&
        checkAndMaybeProcessSystemEvent(*event, *cid, isDelete, isCompaction)) {
        return {};
    }

    // At this point the key is a normal mutation/delete so will for example
    // increment or decrement the collection item count.
    // Or the key is a SystemEvent Create/Modify collection, which needs to
    // change the collection disk size and high-seqno.

    auto& collsFlushStats = getStatsAndMaybeSetPersistedHighSeqno(
            cid.value(), seqno, compactionCallbacks);

    // Logically deleted items don't update item-count/disk-size
    if (isLogicallyDeleted(cid.value(), seqno) &&
        compactionCallbacks == CompactionCallbacks::LatestRevision) {
        return {};
    }

    UpdateStatsResult result;
    const auto isSystemEvent = event ? IsSystem::Yes : IsSystem::No;
    if (isSystemEvent == IsSystem::No &&
        compactionCallbacks == CompactionCallbacks::AnyRevision &&
        (isLogicallyDeleted(cid.value(), oldSeqno) ||
         isLogicallyDeletedInStore(cid.value(), oldSeqno))) {
        // We are logically inserting an item into a collection (i.e. it existed
        // before in an older generation of the collection and we are now
        // adding it into this one). Backends that may see any revision of an
        // item during compaction have to count the vBucket item count by
        // relying on a dropped collection item count stat that is persisted
        // until the collection is fully purged. We cannot update that stat at
        // this point as it would race with a concurrent purge so we instead
        // update the vBucket stat to reflect this being a logical insert rather
        // than an update.
        result.logicalInsert = true;
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
            result.newDocReflectedInDiskSize =
                    collsFlushStats.insert(isSystemEvent,
                                           isDelete,
                                           isCommitted,
                                           compactionCallbacks,
                                           size);
        } else if (oldIsDelete == IsDeleted::Yes) {
            // insert with the delta of old/new
            auto sizeUpdate = size - oldSize;
            if (compactionCallbacks == CompactionCallbacks::AnyRevision) {
                // Magma doesn't track tombstones in the disk size, need to
                // increment by size rather than delta as we're undeleting
                sizeUpdate = size;
            }
            result.newDocReflectedInDiskSize =
                    collsFlushStats.insert(isSystemEvent,
                                           isDelete,
                                           isCommitted,
                                           compactionCallbacks,
                                           sizeUpdate);
        } else {
            // update with the delta
            collsFlushStats.update(size - oldSize);
            result.newDocReflectedInDiskSize = true;
        }

    } else {
        // new key is delete
        if (oldIsDropped) {
            // update with the size of the new tombstone
            if (compactionCallbacks == CompactionCallbacks::LatestRevision) {
                // Magma doesn't track tombstones in the disk size
                collsFlushStats.update(size);
                result.newDocReflectedInDiskSize = true;
            }
        } else if (oldIsDelete == IsDeleted::Yes) {
            // update with the delta of old/new
            if (compactionCallbacks == CompactionCallbacks::LatestRevision) {
                // Magma doesn't track tombstones in the disk size
                collsFlushStats.update(size - oldSize);
                result.newDocReflectedInDiskSize = true;
            }
        } else {
            // remove
            result.newDocReflectedInDiskSize =
                    collsFlushStats.remove(isSystemEvent,
                                           isDelete,
                                           isCommitted,
                                           compactionCallbacks,
                                           oldSize,
                                           size);
        }
    }

    return result;
}

void FlushAccounting::maybeUpdatePersistedHighSeqno(const DocKey& key,
                                                    uint64_t seqno,
                                                    bool isDelete) {
    const auto [event, cid] = getCollectionEventAndCollectionID(key);

    if (!cid) {
        // A scope event - no high-seqno update.
        return;
    }

    if (isDelete && event) {
        // A system event, but deleted (drop collection) - no high-seqno update
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
