/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <gsl/gsl-lite.hpp>
#include <platform/checked_snprintf.h>
#include <string>
#include <utility>

#include "bucket_logger.h"
#include "checkpoint.h"
#include "checkpoint_manager.h"
#include "checkpoint_remover.h"
#include "ep_time.h"
#include "stats.h"

#include <statistics/cbstat_collector.h>

class CookieIface;

const char* to_string(enum checkpoint_state s) {
    switch (s) {
    case CHECKPOINT_OPEN:
        return "Open";
    case CHECKPOINT_CLOSED:
        return "Closed";
    }
    return "<unknown>";
}

std::string to_string(QueueDirtyStatus value) {
    switch (value) {
    case QueueDirtyStatus::SuccessExistingItem:
        return "exitsting item";
    case QueueDirtyStatus::SuccessPersistAgain:
        return "persist again";
    case QueueDirtyStatus::SuccessNewItem:
        return "new item";
    case QueueDirtyStatus::FailureDuplicateItem:
        return "failure:duplicate item";
    }

    throw std::invalid_argument("to_string(QueueDirtyStatus): Invalid value: " +
                                std::to_string(int(value)));
}

CheckpointCursor::CheckpointCursor(std::string n,
                                   CheckpointList::iterator checkpoint,
                                   ChkptQueueIterator pos)
    : name(std::move(n)),
      currentCheckpoint(checkpoint),
      currentPos(pos),
      numVisits(0) {
    (*currentCheckpoint)->incNumOfCursorsInCheckpoint();
}

CheckpointCursor::CheckpointCursor(const CheckpointCursor& other,
                                   std::string name)
    : name(std::move(name)),
      currentCheckpoint(other.currentCheckpoint),
      currentPos(other.currentPos),
      numVisits(other.numVisits.load()),
      isValid(other.isValid) {
    if (isValid) {
        (*currentCheckpoint)->incNumOfCursorsInCheckpoint();
    }
}

CheckpointCursor::~CheckpointCursor() {
    if (isValid) {
        (*currentCheckpoint)->decNumOfCursorsInCheckpoint();
    }
}

void CheckpointCursor::invalidate() {
    (*currentCheckpoint)->decNumOfCursorsInCheckpoint();
    isValid = false;
}

void CheckpointCursor::decrPos() {
    if (currentPos != (*currentCheckpoint)->begin()) {
        --currentPos;
    }
}

void CheckpointCursor::incrPos() {
    if (currentPos != (*currentCheckpoint)->end()) {
        ++currentPos;
    }
}

uint64_t CheckpointCursor::getId() const {
    return (*currentCheckpoint)->getId();
}

size_t CheckpointCursor::getRemainingItemsCount() const {
    size_t remaining = 0;
    ChkptQueueIterator itr = currentPos;
    // Start counting from the next item
    if (itr != (*currentCheckpoint)->end()) {
        ++itr;
    }
    while (itr != (*currentCheckpoint)->end()) {
        if (!(*itr)->isCheckPointMetaItem()) {
            ++remaining;
        }
        ++itr;
    }
    return remaining;
}

CheckpointType CheckpointCursor::getCheckpointType() const {
    return (*currentCheckpoint)->getCheckpointType();
}

bool operator<(const CheckpointCursor& a, const CheckpointCursor& b) {
    // Compare currentCheckpoint, bySeqno, and finally distance from start of
    // currentCheckpoint.
    // Given the underlying iterator (CheckpointCursor::currentPos) is a
    // std::list iterator, it is O(N) to compare iterators directly.
    // Therefore bySeqno (integer) initially, only falling back to iterator
    // comparison if two CheckpointCursors have the same bySeqno.
    const auto a_id = (*a.currentCheckpoint)->getId();
    const auto b_id = (*b.currentCheckpoint)->getId();
    if (a_id < b_id) {
        return true;
    }
    if (a_id > b_id) {
        return false;
    }

    // Same checkpoint; check bySeqno
    const auto a_bySeqno = (*a.currentPos)->getBySeqno();
    const auto b_bySeqno = (*b.currentPos)->getBySeqno();
    if (a_bySeqno < b_bySeqno) {
        return true;
    }
    if (a_bySeqno > b_bySeqno) {
        return false;
    }

    // Same checkpoint and seqno, measure distance from start of checkpoint.
    const auto a_distance =
            std::distance((*a.currentCheckpoint)->begin(), a.currentPos);
    const auto b_distance =
            std::distance((*b.currentCheckpoint)->begin(), b.currentPos);
    return a_distance < b_distance;
}

std::ostream& operator<<(std::ostream& os, const CheckpointCursor& c) {
    os << "CheckpointCursor[" << &c << "] with"
       << " name:" << c.name
       << " currentCkpt:{id:" << (*c.currentCheckpoint)->getId()
       << " state:" << to_string((*c.currentCheckpoint)->getState())
       << "} currentSeq:" << (*c.currentPos)->getBySeqno() << " distance:"
       << std::distance((*c.currentCheckpoint)->begin(), c.currentPos);
    return os;
}

Checkpoint::Checkpoint(CheckpointManager& manager,
                       EPStats& st,
                       uint64_t id,
                       uint64_t snapStart,
                       uint64_t snapEnd,
                       uint64_t visibleSnapEnd,
                       std::optional<uint64_t> highCompletedSeqno,
                       Vbid vbid,
                       CheckpointType checkpointType)
    : manager(&manager),
      stats(st),
      checkpointId(id),
      snapStartSeqno(snapStart),
      snapEndSeqno(snapEnd),
      visibleSnapEndSeqno(visibleSnapEnd),
      vbucketId(vbid),
      creationTime(ep_real_time()),
      checkpointState(CHECKPOINT_OPEN),
      numItems(0),
      numMetaItems(0),
      toWrite(trackingAllocator),
      committedKeyIndex(keyIndexTrackingAllocator),
      preparedKeyIndex(keyIndexTrackingAllocator),
      keyIndexMemUsage(st, &manager.estimatedMemUsage),
      queuedItemsMemUsage(st, &manager.estimatedMemUsage),
      checkpointType(checkpointType),
      highCompletedSeqno(std::move(highCompletedSeqno)) {
    auto& core = stats.coreLocal.get();
    core->memOverhead.fetch_add(sizeof(Checkpoint));
    core->numCheckpoints++;

    // the overheadChangedCallback uses the accurately tracked overhead
    // from trackingAllocator. The above memOverhead stat is "manually"
    // accounted in queueDirty, and approximates the overhead based on
    // key sizes and the size of queued_item and index_entry.
    manager.overheadChangedCallback(getMemoryOverhead());
}

Checkpoint::~Checkpoint() {
    EP_LOG_DEBUG("Checkpoint {} for {} is purged from memory",
                 checkpointId,
                 vbucketId);
    /**
     * Calculate as best we can the overhead associated with the queue
     * (toWrite). This is approximated to sizeof(queued_item) * number
     * of queued_items in the checkpoint.
     */
    auto queueMemOverhead = sizeof(queued_item) * toWrite.size();
    auto& core = stats.coreLocal.get();
    core->memOverhead.fetch_sub(sizeof(Checkpoint) + keyIndexMemUsage +
                                queueMemOverhead);
    core->numCheckpoints--;

    if (manager) {
        // If this checkpoint is still associated with a manger, detach it
        // now to ensure all manager stats are updated before the checkpoint
        // goes away.
        detachFromManager();
    }
}

QueueDirtyResult Checkpoint::queueDirty(const queued_item& qi) {
    if (getState() != CHECKPOINT_OPEN) {
        throw std::logic_error(
                "Checkpoint::queueDirty: checkpointState "
                "(which is" +
                std::to_string(getState()) + ") is not OPEN");
    }

    Expects(manager);
    QueueDirtyResult rv;
    // trigger the overheadChangedCallback if the overhead is different
    // when this helper is destroyed
    auto overheadCheck = gsl::finally([pre = getMemoryOverhead(), this]() {
        auto post = getMemoryOverhead();
        if (pre != post) {
            manager->overheadChangedCallback(post - pre);
        }
    });

    // Check if the item is a meta item
    if (qi->isCheckPointMetaItem()) {
        // We will just queue the item
        rv.status = QueueDirtyStatus::SuccessNewItem;
        addItemToCheckpoint(qi);
    } else {
        // Check in the appropriate key index if an item already exists.
        auto& keyIndex =
                qi->isCommitted() ? committedKeyIndex : preparedKeyIndex;
        auto it = keyIndex.find(makeIndexKey(qi));

        // Before de-duplication could discard a delete, store the largest
        // "rev-seqno" encountered
        if (qi->isDeleted() &&
            qi->getRevSeqno() > maxDeletedRevSeqno.value_or(0)) {
            maxDeletedRevSeqno = qi->getRevSeqno();
        }

        if (it != keyIndex.end()) {
            // Case: key is in the index, need to execute the de-dup path

            const auto& indexEntry = it->second;

            if (indexEntry.getPosition() == toWrite.begin() ||
                qi->getOperation() == queue_op::commit_sync_write) {
                // Case: sync mutation expelled or new item is a Commit

                // If the previous op was a syncWrite and we hit this code
                // then we know that the new op (regardless of what it is)
                // must be placed in a new checkpoint (as it is for the same
                // key).
                //
                // If the new op is a commit (which would typically de-dupe
                // a mutation) then we must also place the op in a new
                // checkpoint.
                return {QueueDirtyStatus::FailureDuplicateItem, 0};
            } else if (indexEntry.getPosition() == toWrite.end()) {
                // Case: normal mutation expelled

                // Always return PersistAgain because if the old item has been
                // expelled so all cursors must have passed it.
                rv.status = QueueDirtyStatus::SuccessPersistAgain;
                addItemToCheckpoint(qi);
            } else {
                // Case: item not expelled, normal path

                // Note: In this case the index entry points to a valid position
                // in toWrite, so we can make our de-dup checks.
                const auto existingSeqno =
                        (*indexEntry.getPosition())->getBySeqno();
                Expects(highestExpelledSeqno < existingSeqno);

                const auto oldPos = it->second.getPosition();
                const auto& oldItem = *oldPos;
                if (!(canDedup(oldItem, qi))) {
                    return {QueueDirtyStatus::FailureDuplicateItem, 0};
                }

                rv.status = QueueDirtyStatus::SuccessExistingItem;

                // In the following loop we perform various operations in
                // preparation for removing the item being dedup'ed:
                //
                // 1. We avoid invalid cursors by repositioning any cursor that
                //    points to the item being removed.
                // 2. Specifically and only for the Persistence cursor, we need
                //    to do some computation for correct stats update at caller.
                for (auto& cursor : manager->cursors) {
                    if ((*(cursor.second->currentCheckpoint)).get() != this) {
                        // Cursor is in another checkpoint, doesn't need
                        // updating here
                        continue;
                    }

                    // Save the original cursor pos before the cursor is
                    // possibly repositioned
                    const auto originalCursorPos = cursor.second->currentPos;

                    // Reposition the cursor to the previous item if it points
                    // to the item being dedup'ed. Done for all cursors.
                    if (originalCursorPos.getUnderlyingIterator() == oldPos) {
                        // Note: We never deduplicate meta-items
                        Expects(!(*originalCursorPos)->isCheckPointMetaItem());
                        cursor.second->decrPos();
                    }

                    // The logic below is specific to the Persistence cursor,
                    // so skip it for any other cursor.
                    if (cursor.second->name != CheckpointManager::pCursorName) {
                        continue;
                    }

                    // Code path executed only for Persistence cursor

                    // If the cursor item is non-meta, then we need to return
                    // persist again if the existing item is either before or on
                    // the cursor - as the cursor points to the "last processed"
                    // item. However if the cursor item is meta, then we only
                    // need to return persist again if the existing item is
                    // strictly less than the cursor, as meta-items can share a
                    // seqno with a non-meta item but are logically before them.
                    //
                    // Note: For correct computation we need to use the original
                    // cursor seqno.
                    const auto originalCursorSeqno =
                            (*originalCursorPos)->isCheckPointMetaItem()
                                    ? (*originalCursorPos)->getBySeqno() - 1
                                    : (*originalCursorPos)->getBySeqno();

                    if (existingSeqno > originalCursorSeqno) {
                        // Old mutation comes after the cursor, nothing else to
                        // do here
                        continue;
                    }

                    // Cursor has already processed the previous value for this
                    // key so need to persist again.
                    rv.status = QueueDirtyStatus::SuccessPersistAgain;

                    // When we overwrite a persisted item again we need to
                    // consider if we are currently mid-flush. If we return
                    // SuccessPersistAgain and update stats accordingly but the
                    // flush fails then we'll have double incremented a stat for
                    // a single item (we de-dupe below). Track this in an
                    // AggregatedFlushStats in CheckpointManager so that we can
                    // undo these stat updates if the flush fails.
                    const auto backupPCursor = manager->cursors.find(
                            CheckpointManager::backupPCursorName);

                    if (backupPCursor == manager->cursors.end()) {
                        // We're not mid-flush, don't need to adjust any stats
                        continue;
                    }

                    const auto backupPCursorSeqno =
                            (*(*backupPCursor->second).currentPos)
                                    ->getBySeqno();
                    if (backupPCursorSeqno <= existingSeqno) {
                        // Pass the oldItem in. When we return and update
                        // the stats we'll use the new item and the flush
                        // will pick up the new item too so we have to match
                        // the original (oldItem) increment with a decrement
                        manager->persistenceFailureStatOvercounts.accountItem(
                                *oldItem);
                    }
                }

                if (rv.status == QueueDirtyStatus::SuccessExistingItem) {
                    // Set the queuedTime of the item to the original queued
                    // time. We must do this to ensure that the dirtyQueueAge
                    // is tracked correctly when this item is persisted. If we
                    // get PersistAgain from the above code then we'd just
                    // increment/decrement the stat again so no adjustment is
                    // necessary.
                    qi->setQueuedTime(oldItem->getQueuedTime());

                    // If we're changing the item size we need to pass that back
                    // to update the dirtyQueuePendingWrites size also
                    rv.successExistingByteDiff = qi->size() - oldItem->size();
                }

                addItemToCheckpoint(qi);

                // Reduce the size of the checkpoint by the size of the
                // item being removed.
                queuedItemsMemUsage -= oldItem->size();
                // Remove the existing item for the same key from the list.
                toWrite.erase(
                        ChkptQueueIterator::const_underlying_iterator{oldPos});
            }

            // Reduce the number of items because addItemToCheckpoint will
            // increase the number by one.
            --numItems;
        } else {
            // Case: key is not in the index, just queue the new item.

            rv.status = QueueDirtyStatus::SuccessNewItem;
            addItemToCheckpoint(qi);
        }
    }

    if (rv.status == QueueDirtyStatus::SuccessNewItem) {
        stats.coreLocal.get()->memOverhead.fetch_add(sizeof(queued_item));
    }

    /**
     * We only add keys to the indexes of Memory Checkpoints. We don't add them
     * to the indexes of Disk Checkpoints as these grow at a O(n) rate and this
     * is unsustainable for heavy DGM use cases. A Disk Checkpoint should also
     * never contain more than one instance of any given key as we should only
     * be keeping the latest copy of each key on disk. A Memory Checkpoint can
     * have multiple of the same key in some circumstances and the keyIndexes
     * allow us to perform de-duplication correctly on the active node and check
     * on the replica node that we have received a valid Checkpoint.
     */
    if (!qi->isCheckPointMetaItem() && qi->getKey().size() > 0 &&
        !isDiskCheckpoint()) {
        // --toWrite.end() is okay as the list is not empty now.
        const auto entry = IndexEntry(--toWrite.end());
        // Set the index of the key to the new item that is pushed back into
        // the list.
        auto& keyIndex =
                qi->isCommitted() ? committedKeyIndex : preparedKeyIndex;
        auto result = keyIndex.emplace(makeIndexKey(qi), entry);
        if (!result.second) {
            // Did not manage to insert - so update the value directly
            result.first->second = entry;
        }

        if (rv.status == QueueDirtyStatus::SuccessNewItem) {
            const auto indexKeyUsage = qi->getKey().size() + sizeof(IndexEntry);
            stats.coreLocal.get()->memOverhead.fetch_add(indexKeyUsage);
            // Update the total keyIndex memory usage which is used when the
            // checkpoint is destructed to manually account for the freed mem.
            keyIndexMemUsage += indexKeyUsage;
        }
    }

    // track the highest prepare seqno present in the checkpoint
    if (qi->getOperation() == queue_op::pending_sync_write) {
        setHighPreparedSeqno(qi->getBySeqno());
    }

    // Notify flusher if in case queued item is a checkpoint meta item or
    // vbpersist state.
    if (qi->getOperation() == queue_op::checkpoint_start ||
        qi->getOperation() == queue_op::checkpoint_end ||
        qi->getOperation() == queue_op::set_vbucket_state) {
        manager->notifyFlusher();
    }

    return rv;
}

bool Checkpoint::canDedup(const queued_item& existing,
                          const queued_item& in) const {
    auto isDurabilityOp = [](const queued_item& qi_) -> bool {
        const auto op = qi_->getOperation();
        return op == queue_op::pending_sync_write ||
               op == queue_op::commit_sync_write ||
               op == queue_op::abort_sync_write;
    };
    return !(isDurabilityOp(existing) || isDurabilityOp(in));
}

uint64_t Checkpoint::getMinimumCursorSeqno() const {
    auto pos = begin();
    Expects((*pos)->isEmptyItem());
    const auto seqno = (*pos)->getBySeqno();
    ++pos;
    Expects((*pos)->isCheckpointStart());
    Expects(seqno == (*pos)->getBySeqno());

    if (highestExpelledSeqno == 0) {
        // Old path for the pre-expel behaviour.
        // Expel has never modified this checkpoint, so any seqno-gap was
        // generated by normal de-duplication.
        //
        // Note: This path ensures that we don't trigger useless backfills where
        // backfilling is not really necessary.
        return seqno;
    }

    // Expel has run and modified the checkpoint, we must have at least one
    // item as expel would not remove high-seqno.
    Expects(numItems > 0);

    // Seek to the first item after checkpoint start
    ++pos;
    return (*pos)->getBySeqno();
}

void Checkpoint::addItemToCheckpoint(const queued_item& qi) {
    toWrite.push_back(qi);
    // Increase the size of the checkpoint by the item being added
    queuedItemsMemUsage += qi->size();

    if (qi->isCheckPointMetaItem()) {
        // empty items act only as a dummy element for the start of the
        // checkpoint (and are not read by clients), we do not include them
        // in numMetaItems.
        if (qi->isNonEmptyCheckpointMetaItem()) {
            ++numMetaItems;
        }
    } else {
        // Not a meta item
        ++numItems;
    }
}

std::pair<CheckpointQueue, size_t> Checkpoint::expelItems(
        const ChkptQueueIterator& last) {
    CheckpointQueue expelledItems(toWrite.get_allocator());

    // Expel from the the first item after the checkpoint_start item (included)
    // to 'last' (included).
    const auto dummy = begin();
    Expects((*dummy)->isEmptyItem());
    auto first = std::next(dummy);
    Expects((*first)->isCheckpointStart());
    // This function expects that there is at least one item to expel, caller is
    // responsible to ensure that.
    ++first;
    if (first == end()) {
        throw std::logic_error(
                "Checkpoint::expelItems: Called on an empty checkpoint");
    }
    // The last item to be expelled is not expected to be a meta-item.
    Expects(!(*last)->isCheckPointMetaItem());

    // Record the seqno of the last item to be expelled.
    highestExpelledSeqno = (*last)->getBySeqno();

    expelledItems.splice(
            ChkptQueueIterator::const_underlying_iterator{
                    expelledItems.begin()},
            toWrite,
            ChkptQueueIterator::const_underlying_iterator{first},
            ChkptQueueIterator::const_underlying_iterator{std::next(last)});

    size_t itemsMemory = 0;

    // Note: No key-index in disk checkpoints
    if (getState() == CHECKPOINT_OPEN && !isDiskCheckpoint()) {
        // If the checkpoint is open, for every expelled the corresponding
        // keyIndex entry must be invalidated.
        for (const auto& expelled : expelledItems) {
            if (!expelled->isCheckPointMetaItem()) {
                auto& keyIndex = expelled->isCommitted() ? committedKeyIndex
                                                         : preparedKeyIndex;

                auto it = keyIndex.find(makeIndexKey(expelled));
                Expects(it != keyIndex.end());

                // An IndexEntry is invalidated by placing the underlying
                // iterator to one of the following special positions:
                // - toWrite::end(), if the expelled item is a normal mutation
                // - toWrite::begin(), if the expelled item is sync mutation
                it->second.invalidate(expelled->isAnySyncWriteOp()
                                              ? toWrite.begin()
                                              : toWrite.end());
            }

            itemsMemory += expelled->size();
        }
    } else {
        /*
         * Reduce the queuedItems memory usage by the size of the items
         * being expelled from memory.
         */
        const auto addSize = [](size_t a, queued_item qi) {
            return a + qi->size();
        };
        itemsMemory = std::accumulate(
                expelledItems.begin(), expelledItems.end(), 0, addSize);
    }

    queuedItemsMemUsage -= itemsMemory;

    return {std::move(expelledItems), itemsMemory};
}

CheckpointIndexKeyType Checkpoint::makeIndexKey(const queued_item& item) const {
    return CheckpointIndexKeyType(item->getKey(), keyIndexKeyTrackingAllocator);
}

void Checkpoint::addStats(const AddStatFn& add_stat,
                          const CookieIface* cookie) {
    std::array<char, 256> buf;

    checked_snprintf(buf.data(),
                     buf.size(),
                     "vb_%d:id_%" PRIu64 ":queued_items_mem_usage",
                     vbucketId.get(),
                     getId());
    add_casted_stat(buf.data(), getQueuedItemsMemUsage(), add_stat, cookie);

    checked_snprintf(buf.data(),
                     buf.size(),
                     "vb_%d:id_%" PRIu64 ":key_index_allocator_bytes",
                     vbucketId.get(),
                     getId());
    add_casted_stat(buf.data(), getKeyIndexAllocatorBytes(), add_stat, cookie);

    checked_snprintf(buf.data(),
                     buf.size(),
                     "vb_%d:id_%" PRIu64 ":to_write_allocator_bytes",
                     vbucketId.get(),
                     getId());
    add_casted_stat(
            buf.data(), getWriteQueueAllocatorBytes(), add_stat, cookie);

    checked_snprintf(buf.data(),
                     buf.size(),
                     "vb_%d:id_%" PRIu64 ":state",
                     vbucketId.get(),
                     getId());
    add_casted_stat(buf.data(), to_string(getState()), add_stat, cookie);

    checked_snprintf(buf.data(),
                     buf.size(),
                     "vb_%d:id_%" PRIu64 ":type",
                     vbucketId.get(),
                     getId());
    add_casted_stat(
            buf.data(), to_string(getCheckpointType()), add_stat, cookie);

    checked_snprintf(buf.data(),
                     buf.size(),
                     "vb_%d:id_%" PRIu64 ":snap_start",
                     vbucketId.get(),
                     getId());
    add_casted_stat(buf.data(), getSnapshotStartSeqno(), add_stat, cookie);

    checked_snprintf(buf.data(),
                     buf.size(),
                     "vb_%d:id_%" PRIu64 ":snap_end",
                     vbucketId.get(),
                     getId());
    add_casted_stat(buf.data(), getSnapshotEndSeqno(), add_stat, cookie);

    checked_snprintf(buf.data(),
                     buf.size(),
                     "vb_%d:id_%" PRIu64 ":visible_snap_end",
                     vbucketId.get(),
                     getId());
    add_casted_stat(buf.data(), getVisibleSnapshotEndSeqno(), add_stat, cookie);
}

void Checkpoint::detachFromManager() {
    Expects(manager);
    // decrease the manager memory overhead by the total amount of this
    // checkpoint
    manager->overheadChangedCallback(-getMemoryOverhead());
    manager = nullptr;

    // stop tracking MemoryCounters against the CM, this also decreases the
    // "parent" value by the values for this Checkpoint.
    setMemoryTracker(nullptr);
}

void Checkpoint::setMemoryTracker(
        cb::NonNegativeCounter<size_t>* newMemoryUsageTracker) {
    // This checkpoint is being removed from the Manager, decrease the memory
    // usage accounted against the Manager, and instead track it against the
    // new owner (destroyer task).
    queuedItemsMemUsage.changeParent(newMemoryUsageTracker);
    keyIndexMemUsage.changeParent(newMemoryUsageTracker);
}

std::ostream& operator <<(std::ostream& os, const Checkpoint& c) {
    os << "Checkpoint[" << &c << "] with"
       << " id:" << c.checkpointId << " seqno:{" << c.getMinimumCursorSeqno()
       << "," << c.getHighSeqno() << "}"
       << " snap:{" << c.getSnapshotStartSeqno() << ","
       << c.getSnapshotEndSeqno()
       << ", visible:" << c.getVisibleSnapshotEndSeqno() << "}"
       << " state:" << to_string(c.getState())
       << " numCursors:" << c.getNumCursorsInCheckpoint()
       << " type:" << to_string(c.getCheckpointType());
    const auto hcs = c.getHighCompletedSeqno();
    os << " hcs:" << (hcs ? std::to_string(hcs.value()) : "none ") << " items:["
       << std::endl;
    for (const auto& e : c.toWrite) {
        os << "\t{" << e->getBySeqno() << "," << to_string(e->getOperation());
        e->isDeleted() ? os << "[d]," : os << ",";
        os << e->getKey() << "," << e->size() << ",";
        e->isCheckPointMetaItem() ? os << "[m]}" : os << "}";
        os << std::endl;
    }
    os << "]";
    return os;
}

Checkpoint::MemoryCounter::~MemoryCounter() {
    if (parentUsage) {
        *parentUsage -= local;
    }
    stats.coreLocal.get()->estimatedCheckpointMemUsage.fetch_sub(local);
}

Checkpoint::MemoryCounter& Checkpoint::MemoryCounter::operator+=(size_t size) {
    local += size;
    if (parentUsage) {
        *parentUsage += size;
    }
    stats.coreLocal.get()->estimatedCheckpointMemUsage.fetch_add(size);
    return *this;
}

Checkpoint::MemoryCounter& Checkpoint::MemoryCounter::operator-=(size_t size) {
    local -= size;
    if (parentUsage) {
        *parentUsage -= size;
    }
    stats.coreLocal.get()->estimatedCheckpointMemUsage.fetch_sub(size);
    return *this;
}

void Checkpoint::MemoryCounter::changeParent(
        cb::NonNegativeCounter<size_t>* newParent) {
    if (parentUsage) {
        *parentUsage -= local;
    }
    parentUsage = newParent;
    if (parentUsage) {
        *parentUsage += local;
    }
}