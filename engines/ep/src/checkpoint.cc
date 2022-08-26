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

Checkpoint::Checkpoint(CheckpointManager& manager,
                       EPStats& st,
                       uint64_t id,
                       uint64_t snapStart,
                       uint64_t snapEnd,
                       uint64_t visibleSnapEnd,
                       std::optional<uint64_t> highCompletedSeqno,
                       uint64_t highPreparedSeqno,
                       Vbid vbid,
                       CheckpointType checkpointType)
    : manager(&manager),
      stats(st),
      checkpointId(id),
      snapStartSeqno(snapStart),
      snapEndSeqno(snapEnd, {*this}),
      visibleSnapEndSeqno(visibleSnapEnd),
      highestExpelledSeqno(0, {*this}),
      vbucketId(vbid),
      checkpointState(CHECKPOINT_OPEN),
      toWrite(queueAllocator),
      committedKeyIndex(keyIndexAllocator),
      preparedKeyIndex(keyIndexAllocator),
      keyIndexMemUsage(st, &manager.memOverheadIndex),
      queuedItemsMemUsage(st, &manager.queuedItemsMemUsage),
      queueMemOverhead(st, &manager.memOverheadQueue),
      checkpointType(checkpointType),
      highCompletedSeqno(std::move(highCompletedSeqno)) {
    Expects(snapStart <= snapEnd);
    Expects(visibleSnapEnd <= snapEnd);

    auto& core = stats.coreLocal.get();
    core->memOverhead.fetch_add(sizeof(Checkpoint));
    core->numCheckpoints++;
    core->checkpointManagerEstimatedMemUsage.fetch_add(sizeof(Checkpoint));

    this->highPreparedSeqno.reset(highPreparedSeqno);

    // the overheadChangedCallback uses the accurately tracked overhead
    // from queueAllocator. The above memOverhead stat is "manually"
    // accounted in queueDirty, and approximates the overhead based on
    // key sizes and the size of queued_item and index_entry.
    manager.overheadChangedCallback(getMemOverheadAllocatorBytes());
}

Checkpoint::~Checkpoint() {
    EP_LOG_DEBUG("Checkpoint {} for {} is purged from memory",
                 checkpointId,
                 vbucketId);
    auto& core = stats.coreLocal.get();
    core->memOverhead.fetch_sub(getMemOverhead());
    core->numCheckpoints--;
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
    auto overheadCheck =
            gsl::finally([pre = getMemOverheadAllocatorBytes(), this]() {
                auto post = getMemOverheadAllocatorBytes();
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

                int64_t initialBackupPCursorSeqno = 0;
                auto initialBackupPCursor = manager->cursors.end();
                // Check that a backup cursor can exist
                if (manager->getPersistenceCursor()) {
                    initialBackupPCursor = manager->cursors.find(
                            CheckpointManager::backupPCursorName);
                    if (initialBackupPCursor != manager->cursors.end()) {
                        auto backupCursorItem =
                                *initialBackupPCursor->second->getPos();
                        if (backupCursorItem) {
                            initialBackupPCursorSeqno =
                                    backupCursorItem->getBySeqno();
                            // If the backup cursor is pointing to a meta item
                            // then move the backup cursor seqno back one to if
                            // it was point to a no meta item
                            if (backupCursorItem->isCheckPointMetaItem()) {
                                initialBackupPCursorSeqno--;
                            }
                        }
                    }
                }

                // In the following loop we perform various operations in
                // preparation for removing the item being dedup'ed:
                //
                // 1. We avoid invalid cursors by repositioning any cursor that
                //    points to the item being removed.
                // 2. Specifically and only for the Persistence cursor, we need
                //    to do some computation for correct stats update at caller.
                for (auto& [_, cursor] : manager->cursors) {
                    if ((*cursor->getCheckpoint()).get() != this) {
                        // Cursor is in another checkpoint, doesn't need
                        // updating here
                        continue;
                    }

                    // Save the original cursor pos before the cursor is
                    // possibly repositioned
                    const auto originalCursorPos = cursor->getPos();

                    // Reposition the cursor to the previous item if it points
                    // to the item being dedup'ed. That also decrements the
                    // cursor's distance accordingly.
                    // Or, just update the cursor's distance if necessary.
                    // See CheckpointCursor::distance for details.
                    // Done for all cursors.
                    if (originalCursorPos.getUnderlyingIterator() == oldPos) {
                        // Note: We never deduplicate meta-items
                        Expects(!(*originalCursorPos)->isCheckPointMetaItem());
                        cursor->decrPos();
                    } else {
                        // The cursor is not being repositioned, so the only
                        // thing that we need to do (if necessary) case is
                        // decrementing the cursor's distance.
                        //
                        // Note: We don't care here whether the cursor points
                        // to a meta or non-meta item here. Eg, imagine that m:1
                        // is being dedup'ed and cursor_seqno is 2:
                        //
                        // [e:0 cs:0 vbs:1 m:1 vbs:2 m:2)
                        //
                        // .. then we need to decrement the cursor's distance
                        // regardless of whether cursor points to vbs:2 or m:2.
                        if (existingSeqno <
                            (*originalCursorPos)->getBySeqno()) {
                            cursor->decrDistance();
                        }
                    }

                    // The logic below is specific to the Persistence cursor,
                    // so skip it for any other cursor.
                    if (cursor->getName() != CheckpointManager::pCursorName) {
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
                    if (initialBackupPCursor == manager->cursors.end()) {
                        // We're not mid-flush, don't need to adjust any stats
                        continue;
                    }

                    if (existingSeqno > initialBackupPCursorSeqno) {
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

                // Queue the new item and remove the dedup'ed one
                addItemToCheckpoint(qi);
                removeItemFromCheckpoint(oldPos);
            }
        } else {
            // Case: key is not in the index, just queue the new item.

            rv.status = QueueDirtyStatus::SuccessNewItem;
            addItemToCheckpoint(qi);
        }
    }

    if (rv.status == QueueDirtyStatus::SuccessNewItem) {
        stats.coreLocal.get()->memOverhead.fetch_add(per_item_queue_overhead);
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
    if (isEmptyByExpel()) {
        // No mutation in this checkpoint and we can't know exactly whether it
        // will store any further mutation, nothing to pick for cursors.
        return 0;
    }

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
        // Eg, the following might happen:
        //
        // a. [e:1 cs:1 m:1(A) )
        // b. [e:1 cs:1 x      m:2(A) )
        //
        // After (b) this function returns 1, while the first seqno picked by a
        // cursor would be 2.
        // The semantic here is: a cursor wouldn't pick seqno:1, but it will
        // pick seqno:2 that is a new revision for the same key. Logically that
        // is equivalent to:
        // - m:1 still queued when the cursor is registered
        // - no cursor move yet
        // - m:2 queued deduplicates m:1
        // - cursor moves and picks only m:2
        // , which is all legal and expected behaviour for in-memory dedup.
        //
        // Note: This path ensures that we don't trigger useless backfills where
        // backfilling is not really necessary. Ie, if we remove this path then
        // a cursor registered after dedup (ie m:1 not in checkpoint) might
        // trigger a backfill just for the fact that m:1 has been dedup'ed by
        // m:2
        return seqno;
    }

    // Seek to the first item after checkpoint start
    ++pos;
    return (*pos)->getBySeqno();
}

uint64_t Checkpoint::getHighSeqno() const {
    if (isEmptyByExpel()) {
        // No mutation in this checkpoint and we can't know exactly whether it
        // will store any further mutation.
        return 0;
    }

    auto pos = end();
    --pos;

    // We bump the seqno for meta items, so we need to locate the high-seqno of
    // the last non-meta item.
    //
    // Note: Theoretically this code is O(N) in toWrite.size(). In practice the
    // only way for showing a O(N) behaviour is having a long sequence of
    // set_vbucket_state items queued at the end of the checkpoint, which never
    // happens.
    while ((*pos)->isCheckPointMetaItem() && !(*pos)->isCheckpointStart()) {
        --pos;
    }

    return (*pos)->getBySeqno();
}

void Checkpoint::addItemToCheckpoint(const queued_item& qi) {
    toWrite.push_back(qi);
    queueMemOverhead += per_item_queue_overhead;
    // Increase the size of the checkpoint by the item being added
    queuedItemsMemUsage += qi->size();
}

void Checkpoint::removeItemFromCheckpoint(CheckpointQueue::const_iterator it) {
    // Note: Metaitems are logically immutable and not removable, we would break
    // the checkpoint otherwise.
    Expects(!(*it)->isCheckPointMetaItem());
    const auto itemSize = (*it)->size();
    toWrite.erase(it);
    queueMemOverhead -= per_item_queue_overhead;
    queuedItemsMemUsage -= itemSize;
}

CheckpointQueue Checkpoint::expelItems(const ChkptQueueIterator& last,
                                       size_t distance) {
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

    // Note: If reaching here the logic ensures that we have at least one item
    // to expel. Thus, the lowest expected position for 'last' is at the item
    // next to checkpoint_start, which has distance=2.
    Expects(distance >= 2);
    // 'distance' is the distance toWrite.begin->last and we need the distance
    // first->std::next(last). So:
    // -2 as 'first' is 2 hops from toWrite.begin()
    // +1 as our range-end is std::next(last)
    distance = distance - 2 + 1;

    const auto _first = ChkptQueueIterator::const_underlying_iterator{first};
    const auto _last =
            ChkptQueueIterator::const_underlying_iterator{std::next(last)};
#if CB_DEVELOPMENT_ASSERTS
    Expects(distance == size_t(std::distance(_first, _last)));
#endif
    expelledItems.splice(
            ChkptQueueIterator::const_underlying_iterator{
                    expelledItems.begin()},
            toWrite,
            _first,
            _last,
            distance);

    // Note: No key-index in disk checkpoints
    if (getState() == CHECKPOINT_OPEN && isMemoryCheckpoint()) {
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
        }
    }

    // 'distance' is === expelledItems.size()
    // I avoid to make the size() call as it's O(N). See CheckpointQueue type
    // for details.
    const auto overhead = distance * per_item_queue_overhead;
    queueMemOverhead -= overhead;
    stats.coreLocal.get()->memOverhead.fetch_sub(overhead);

    return expelledItems;
}

CheckpointIndexKeyType Checkpoint::makeIndexKey(const queued_item& item) const {
    return {item->getKey(), keyIndexKeyAllocator};
}

void Checkpoint::addStats(const AddStatFn& add_stat,
                          const CookieIface* cookie) {
    std::array<char, 256> buf;

    checked_snprintf(buf.data(),
                     buf.size(),
                     "vb_%d:id_%" PRIu64 ":mem_usage_queued_items",
                     vbucketId.get(),
                     getId());
    add_casted_stat(buf.data(), getQueuedItemsMemUsage(), add_stat, cookie);

    checked_snprintf(buf.data(),
                     buf.size(),
                     "vb_%d:id_%" PRIu64 ":mem_usage_queue_overhead",
                     vbucketId.get(),
                     getId());
    add_casted_stat(buf.data(), getMemOverheadQueue(), add_stat, cookie);

    checked_snprintf(buf.data(),
                     buf.size(),
                     "vb_%d:id_%" PRIu64 ":mem_usage_key_index_overhead",
                     vbucketId.get(),
                     getId());
    add_casted_stat(buf.data(), getMemOverheadIndex(), add_stat, cookie);

    checked_snprintf(buf.data(),
                     buf.size(),
                     "vb_%d:id_%" PRIu64 ":key_index_allocator_bytes",
                     vbucketId.get(),
                     getId());
    add_casted_stat(buf.data(), getKeyIndexAllocatorBytes(), add_stat, cookie);

    checked_snprintf(buf.data(),
                     buf.size(),
                     "vb_%d:id_%" PRIu64 ":queue_allocator_bytes",
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

    checked_snprintf(buf.data(),
                     buf.size(),
                     "vb_%d:id_%" PRIu64 ":highest_expelled_seqno",
                     vbucketId.get(),
                     getId());
    add_casted_stat(buf.data(), highestExpelledSeqno, add_stat, cookie);
}

void Checkpoint::detachFromManager() {
    Expects(manager);
    manager = nullptr;

    // In EPStats we track the mem used by checkpoints owned by CM, so we need
    // to decrease that we detach a checkpoint from the CM.
    auto& cmMemUsage =
            stats.coreLocal.get()->checkpointManagerEstimatedMemUsage;
    cmMemUsage.fetch_sub(queuedItemsMemUsage);
    cmMemUsage.fetch_sub(keyIndexMemUsage);
    cmMemUsage.fetch_sub(queueMemOverhead);
    cmMemUsage.fetch_sub(sizeof(Checkpoint));

    // stop tracking MemoryCounters against the CM
    queuedItemsMemUsage.detachFromManager();
    queueMemOverhead.detachFromManager();
    keyIndexMemUsage.detachFromManager();
}

void Checkpoint::applyQueuedItemsMemUsageDecrement(size_t size) {
    queuedItemsMemUsage -= size;
}

bool Checkpoint::hasNonMetaItems() const {
    // Note: Function theoretically O(N) but O(1) in practice, as O(N) behaviour
    // only in the case of many subsequent set_vbstate items.
    auto pos = begin();
    Expects((*pos)->isEmptyItem());
    while (++pos != end()) {
        if (!(*pos)->isCheckPointMetaItem()) {
            return true;
        }
    }
    return false;
}

bool Checkpoint::isEmptyByExpel() const {
    auto pos = begin();
    Expects((*pos)->isEmptyItem());
    const auto seqno = (*pos)->getBySeqno();
    ++pos;
    Expects((*pos)->isCheckpointStart());
    Expects(seqno == (*pos)->getBySeqno());

    return modifiedByExpel() && !hasNonMetaItems();
}

bool Checkpoint::modifiedByExpel() const {
    return highestExpelledSeqno > 0;
}

bool Checkpoint::isOpen() const {
    return getState() == CHECKPOINT_OPEN;
}

size_t Checkpoint::getHighestExpelledSeqno() const {
    return highestExpelledSeqno;
}

std::ostream& operator<<(std::ostream& os, const Checkpoint& c) {
    os << "Checkpoint[" << &c << "] with"
       << " id:" << c.checkpointId << " seqno:{" << c.getMinimumCursorSeqno()
       << "," << c.getHighSeqno() << "}"
       << " highestExpelledSeqno:" << c.getHighestExpelledSeqno()
       << " snap:{" << c.getSnapshotStartSeqno() << ","
       << c.getSnapshotEndSeqno()
       << ", visible:" << c.getVisibleSnapshotEndSeqno() << "}"
       << " state:" << to_string(c.getState())
       << " numCursors:" << c.getNumCursorsInCheckpoint()
       << " type:" << to_string(c.getCheckpointType());
    const auto hps = c.getHighPreparedSeqno();
    os << " hps:" << (hps ? std::to_string(hps.value()) : "none ");
    const auto hcs = c.getHighCompletedSeqno();
    os << " hcs:" << (hcs ? std::to_string(hcs.value()) : "none ") << " items:["
       << std::endl;
    for (const auto& e : c.toWrite) {
        os << "\t{" << e->getBySeqno() << "," << to_string(e->getOperation());
        e->isDeleted() ? os << "[d]," : os << ",";
        os << e->getKey() << "," << e->size();
        e->isCheckPointMetaItem() ? os << ",[m]" : os << "";
        if (e->getOperation() == queue_op::set_vbucket_state) {
            os << ",value:" << e->getValueView();
        }
        os << "}" << std::endl;
    }
    os << "]";
    return os;
}

Checkpoint::MemoryCounter& Checkpoint::MemoryCounter::operator+=(size_t size) {
    local += size;

    Expects(managerUsage);
    *managerUsage += size;

    stats.coreLocal.get()->checkpointManagerEstimatedMemUsage.fetch_add(size);
    return *this;
}

Checkpoint::MemoryCounter& Checkpoint::MemoryCounter::operator-=(size_t size) {
    local -= size;

    Expects(managerUsage);
    *managerUsage -= size;

    stats.coreLocal.get()->checkpointManagerEstimatedMemUsage.fetch_sub(size);
    return *this;
}

void Checkpoint::MemoryCounter::detachFromManager() {
    Expects(managerUsage);
    *managerUsage -= local;
    managerUsage = nullptr;
}

std::string Checkpoint::Labeller::getLabel(const char* name) const {
    return fmt::format("Checkpoint({} ckptId:{} type:{} snapStartSeqno:{})::{}",
                       c.vbucketId,
                       c.getId(),
                       to_string(c.getCheckpointType()),
                       c.getSnapshotStartSeqno(),
                       name);
}