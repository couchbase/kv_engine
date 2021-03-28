/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021 Couchbase, Inc.
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

#include "collections/flush_accounting.h"
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
FlushAccounting::getStatsAndMaybeSetPersistedHighSeqno(CollectionID cid,
                                                       uint64_t seqno) {
    auto [itr, inserted] = stats.try_emplace(cid, StatisticsUpdate{seqno});
    auto& [key, value] = *itr;
    (void)key;
    if (!inserted) {
        value.maybeSetPersistedHighSeqno(seqno);
    }

    return value;
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
    // else inserting a collection-start/prepare/tombstone/abort:
    // no item increment but account for the disk size change
    updateDiskSize(diskSizeDelta);
}

void FlushAccounting::StatisticsUpdate::update(ssize_t diskSizeDelta) {
    updateDiskSize(diskSizeDelta);
}

void FlushAccounting::StatisticsUpdate::remove(IsSystem isSystem,
                                               IsDeleted isDelete,
                                               IsCommitted isCommitted,
                                               ssize_t diskSizeDelta) {
    if (isSystem == IsSystem::No && isCommitted == IsCommitted::Yes) {
        decrementItemCount();
    }
    updateDiskSize(diskSizeDelta);
}

void FlushAccounting::updateStats(const DocKey& key,
                                  uint64_t seqno,
                                  IsCommitted isCommitted,
                                  IsDeleted isDelete,
                                  size_t size) {
    auto [isSystemEvent, cid] = getCollectionID(key);

    if (!cid) {
        // The key is not for a collection (e.g. a scope event).
        return;
    }

    // Skip tracking the 'stats' of the delete collection event, if we did the
    // empty collection detection will fail because the high-seqno of the
    // collection will change to be equal to the drop-event's seqno. Empty
    // collection detection relies on start-seqno == high-seqno.
    if (isDelete == IsDeleted::Yes && isSystemEvent) {
        // Delete collection event - no tracking
        return;
    }

    // Track high-seqno for the item
    auto& collsFlushStats =
            getStatsAndMaybeSetPersistedHighSeqno(cid.value(), seqno);

    // but don't track any changes if the item is logically deleted. Why?
    // A flush batch could of recreated the collection, the stats tracking code
    // only has stats stored for the most recent collection, we cannot then
    // call insert for a dropped collection otherwise the stats will be
    // incorrect. Note this relates to an issue for MB-42272, magma assumes the
    // stats item count will include *everything*, but isn't.
    if (!isLogicallyDeleted(cid.value(), seqno)) {
        collsFlushStats.insert(isSystemEvent ? IsSystem::Yes : IsSystem::No,
                               isDelete,
                               isCommitted,
                               size);
    }
}

void FlushAccounting::updateStats(const DocKey& key,
                                  uint64_t seqno,
                                  IsCommitted isCommitted,
                                  IsDeleted isDelete,
                                  size_t size,
                                  uint64_t oldSeqno,
                                  IsDeleted oldIsDelete,
                                  size_t oldSize) {
    // Same logic and comment as updateStats above.
    auto [systemEvent, cid] = getCollectionID(key);
    auto isSystemEvent = systemEvent ? IsSystem::Yes : IsSystem::No;
    if (!cid) {
        return;
    }

    if (isDelete == IsDeleted::Yes && isSystemEvent == IsSystem::Yes) {
        return;
    }

    auto& collsFlushStats =
            getStatsAndMaybeSetPersistedHighSeqno(cid.value(), seqno);

    // As per comment in the other updateStats, logically deleted items don't
    // update item-count/disk-size
    if (isLogicallyDeleted(cid.value(), seqno)) {
        return;
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
        const std::vector<Collections::KVStore::DroppedCollection>& v) {
    for (const auto& c : v) {
        droppedInStore.emplace(c.collectionId, c);
    }
}

} // namespace Collections::VB
