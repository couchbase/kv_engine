/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011 Couchbase, Inc
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

const char* to_string(enum checkpoint_state s) {
    switch (s) {
        case CHECKPOINT_OPEN: return "CHECKPOINT_OPEN";
        case CHECKPOINT_CLOSED: return "CHECKPOINT_CLOSED";
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

CheckpointCursor::CheckpointCursor(const CheckpointCursor& other)
    : name(other.name),
      currentCheckpoint(other.currentCheckpoint),
      currentPos(other.currentPos),
      numVisits(other.numVisits.load()),
      isValid(other.isValid) {
    if (isValid) {
        (*currentCheckpoint)->incNumOfCursorsInCheckpoint();
    }
}

CheckpointCursor::CheckpointCursor(const CheckpointCursor& other,
                                   const std::string& name)
    : CheckpointCursor(other) {
    this->name = name;
}

CheckpointCursor::~CheckpointCursor() {
    if (isValid) {
        (*currentCheckpoint)->decNumOfCursorsInCheckpoint();
    }
}

CheckpointCursor& CheckpointCursor::operator=(const CheckpointCursor& other) {
    name.assign(other.name);
    currentCheckpoint = other.currentCheckpoint;
    currentPos = other.currentPos;
    numVisits = other.numVisits.load();
    isValid = other.isValid;
    if (isValid) {
        (*currentCheckpoint)->incNumOfCursorsInCheckpoint();
    }
    return *this;
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

Checkpoint::Checkpoint(
        EPStats& st,
        uint64_t id,
        uint64_t snapStart,
        uint64_t snapEnd,
        uint64_t visibleSnapEnd,
        std::optional<uint64_t> highCompletedSeqno,
        Vbid vbid,
        CheckpointType checkpointType,
        const std::function<void(int64_t)>& memOverheadChangedCallback)
    : stats(st),
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
      metaKeyIndex(keyIndexTrackingAllocator),
      keyIndexMemUsage(0),
      queuedItemsMemUsage(0),
      checkpointType(checkpointType),
      highCompletedSeqno(std::move(highCompletedSeqno)),
      memOverheadChangedCallback(memOverheadChangedCallback) {
    stats.coreLocal.get()->memOverhead.fetch_add(sizeof(Checkpoint));
    // the memOverheadChangedCallback uses the accurately tracked overhead
    // from trackingAllocator. The above memOverhead stat is "manually"
    // accounted in queueDirty, and approximates the overhead based on
    // key sizes and the size of queued_item and index_entry.
    memOverheadChangedCallback(getMemoryOverhead());
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
    stats.coreLocal.get()->memOverhead.fetch_sub(
            sizeof(Checkpoint) + keyIndexMemUsage + queueMemOverhead);
    memOverheadChangedCallback(-getMemoryOverhead());
}

QueueDirtyStatus Checkpoint::queueDirty(const queued_item& qi,
                                        CheckpointManager* checkpointManager) {
    if (getState() != CHECKPOINT_OPEN) {
        throw std::logic_error(
                "Checkpoint::queueDirty: checkpointState "
                "(which is" +
                std::to_string(getState()) + ") is not OPEN");
    }

    QueueDirtyStatus rv;
    // trigger the memOverheadChangedCallback if the overhead is different
    // when this helper is destroyed
    auto overheadCheck = gsl::finally([pre = getMemoryOverhead(), this]() {
        auto post = getMemoryOverhead();
        if (pre != post) {
            memOverheadChangedCallback(post - pre);
        }
    });

    // Check if the item is a meta item
    if (qi->isCheckPointMetaItem()) {
        // We will just queue the item
        rv = QueueDirtyStatus::SuccessNewItem;
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

        // Check if this checkpoint already has an item for the same key
        // and the item has not been expelled.
        if (it != keyIndex.end()) {
            if (it->second.mutation_id > highestExpelledSeqno) {
                // Normal path - we haven't expelled the item. We have a valid
                // cursor position to read the item and make our de-dupe checks.
                const auto oldPos = it->second.position;
                const auto& oldItem = (*it->second.position);
                if (!(canDedup(oldItem, qi))) {
                    return QueueDirtyStatus::FailureDuplicateItem;
                }

                rv = QueueDirtyStatus::SuccessExistingItem;
                const int64_t currMutationId{it->second.mutation_id};

                // Given the key already exists, need to check all cursors in
                // this Checkpoint and see if the existing item for this key is
                // to the "left" of the cursor (i.e. has already been
                // processed).
                for (auto& cursor : checkpointManager->cursors) {
                    if ((*(cursor.second->currentCheckpoint)).get() == this) {
                        if (cursor.second->name ==
                            CheckpointManager::pCursorName) {
                            int64_t cursor_mutation_id =
                                    getMutationId(*cursor.second);
                            queued_item& cursor_item =
                                    *(cursor.second->currentPos);
                            // If the cursor item is non-meta, then we need to
                            // return persist again if the existing item is
                            // either before or on the cursor - as the cursor
                            // points to the "last processed" item.
                            // However if the cursor item is meta, then we only
                            // need to return persist again if the existing item
                            // is strictly less than the cursor, as meta-items
                            // can share a seqno with a non-meta item but are
                            // logically before them.
                            if (cursor_item->isCheckPointMetaItem()) {
                                --cursor_mutation_id;
                            }
                            if (currMutationId <= cursor_mutation_id) {
                                // Cursor has already processed the previous
                                // value for this key so need to persist again.
                                rv = QueueDirtyStatus::SuccessPersistAgain;

                                // When we overwrite a persisted item again
                                // we need to consider if we are currently
                                // mid-flush. If we return SuccessPersistAgain
                                // and update stats accordingly but the flush
                                // fails then we'll have double incremented a
                                // stat for a single item (we de-dupe below).
                                // Track this in an AggregatedFlushStats in
                                // CheckpointManager so that we can undo these
                                // stat updates if the flush fails.
                                auto backupPCursor =
                                        checkpointManager->cursors.find(
                                                CheckpointManager::
                                                        backupPCursorName);
                                if (backupPCursor !=
                                    checkpointManager->cursors.end()) {
                                    // We'd normally use "mutation_id" here
                                    // which is basically the seqno but the
                                    // backupPCursor may be in a different
                                    // checkpoint and we'd fail a bunch of
                                    // sanity checks trying to read it.
                                    auto backupPCursorSeqno =
                                            (*(*backupPCursor->second)
                                                      .currentPos)
                                                    ->getBySeqno();
                                    if (backupPCursorSeqno <= currMutationId) {
                                        // Pass the old queueTime in. When we
                                        // return and update the stats we'll use
                                        // the new queueTime and the flush will
                                        // pick up the new queueTime too so we
                                        // need to patch the increment of the
                                        // original stat update/queueTime
                                        checkpointManager
                                                ->persistenceFailureStatOvercounts
                                                .accountItem(
                                                        *qi,
                                                        oldItem->getQueuedTime());
                                    }
                                }
                            }
                        }
                        /* If a cursor points to the existing item for the same
                           key, shift it left by 1 */
                        if (cursor.second->currentPos == oldPos) {
                            cursor.second->decrPos();
                        }
                    }
                }

                if (rv == QueueDirtyStatus::SuccessExistingItem) {
                    // Set the queuedTime of the item to the original queued
                    // time. We must do this to ensure that the dirtyQueueAge
                    // is tracked correctly when this item is persisted. If we
                    // get PersistAgain from the above code then we'd just
                    // increment/decrement the stat again so no adjustment is
                    // necessary.
                    qi->setQueuedTime((*it->second.position)->getQueuedTime());
                }

                addItemToCheckpoint(qi);

                // Reduce the size of the checkpoint by the size of the
                // item being removed.
                queuedItemsMemUsage -= oldItem->size();
                // Remove the existing item for the same key from the list.
                toWrite.erase(
                        ChkptQueueIterator::const_underlying_iterator{oldPos});
            } else {
                // The old item has been expelled, but we can continue to use
                // this checkpoint in most cases. If the previous op was a
                // syncWrite and we hit this code then we know that the new op
                // (regardless of what it is) must be placed in a new
                // checkpoint (as it is for the same key). If the new op is a
                // commit (which would typically de-dupe a mutation) then we
                // must also place the op in a new checkpoint. We can't use the
                // cursor position as we normally would as we have expelled the
                // queued_item and freed the memory. The index_entry has the
                // information we need though to tell us if this item was a
                // SyncWrite.
                if (it->second.isSyncWrite() ||
                    qi->getOperation() == queue_op::commit_sync_write) {
                    return QueueDirtyStatus::FailureDuplicateItem;
                }

                // Always return PersistAgain because if the old item has been
                // expelled then the persistence cursor MUST have passed it.
                rv = QueueDirtyStatus::SuccessPersistAgain;

                addItemToCheckpoint(qi);
            }

            // Reduce the number of items because addItemToCheckpoint will
            // increase the number by one.
            --numItems;
        } else {
            rv = QueueDirtyStatus::SuccessNewItem;
            addItemToCheckpoint(qi);
        }
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
    if (qi->getKey().size() > 0 && !isDiskCheckpoint()) {
        ChkptQueueIterator last = end();
        // --last is okay as the list is not empty now.
        index_entry entry = {--last, qi->getBySeqno()};
        // Set the index of the key to the new item that is pushed back into
        // the list.
        if (qi->isCheckPointMetaItem()) {
            // Insert the new entry into the metaKeyIndex
            auto result = metaKeyIndex.emplace(makeIndexKey(qi), entry);
            if (!result.second) {
                // Did not manage to insert - so update the value directly
                result.first->second = entry;
            }
        } else {
            // Insert the new entry into the keyIndex
            auto& keyIndex =
                    qi->isCommitted() ? committedKeyIndex : preparedKeyIndex;
            auto result = keyIndex.emplace(makeIndexKey(qi), entry);
            if (!result.second) {
                // Did not manage to insert - so update the value directly
                result.first->second = entry;
            }
        }

        if (rv == QueueDirtyStatus::SuccessNewItem) {
            auto indexKeyUsage = qi->getKey().size() + sizeof(index_entry);
            /**
             * Calculate as best we can the memory overhead of adding the new
             * item to the queue (toWrite).  This is approximated to the
             * addition to metaKeyIndex / keyIndex plus sizeof(queued_item).
             */
            stats.coreLocal.get()->memOverhead.fetch_add(indexKeyUsage +
                                                         sizeof(queued_item));
            /**
             *  Update the total metaKeyIndex / keyIndex memory usage which is
             *  used when the checkpoint is destructed to manually account
             *  for the freed memory.
             */
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
        checkpointManager->notifyFlusher();
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

void Checkpoint::addItemToCheckpoint(const queued_item& qi) {
    toWrite.push_back(qi);
    // Increase the size of the checkpoint by the item being added
    queuedItemsMemUsage += (qi->size());

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

CheckpointQueue Checkpoint::expelItems(
        CheckpointCursor& expelUpToAndIncluding) {
    CheckpointQueue expelledItems(toWrite.get_allocator());

    ChkptQueueIterator iterator = expelUpToAndIncluding.currentPos;

    // Record the seqno of the last item to be expelled.
    highestExpelledSeqno =
            (*iterator)->getBySeqno();

    auto firstItemToExpel = std::next(begin());
    auto lastItemToExpel = iterator;

    if (getState() == CHECKPOINT_OPEN && !isDiskCheckpoint()) {
        // If the checkpoint is open, for every item which will be expelled
        // the corresponding keyIndex entry must be invalidated. The items
        // to expel start /after/ the dummy item, and /include/ the item
        // pointed to by expelUpToAndIncluding.
        // NB: This must occur *before* swapping the dummy value to its
        // new position, as invalidate(...) dereferences the entry's
        // iterator and reads the value it finds. Swapping the dummy first
        // would lead to it reading the dummy value instead.
        for (auto expelItr = firstItemToExpel;
             expelItr != std::next(lastItemToExpel);
             ++expelItr) {
            const auto& toExpel = *expelItr;

            // We don't put keys in the indexes for disk checkpoints so we:
            //     a) can't test that the key is in the index
            //     b) can't invalidate the index entry as it does not exist
            if (!toExpel->isCheckPointMetaItem()) {
                auto& keyIndex = toExpel->isCommitted() ? committedKeyIndex
                                                        : preparedKeyIndex;

                auto itr = keyIndex.find(makeIndexKey(toExpel));
                Expects(itr != keyIndex.end());
                Expects(itr->second.position == expelItr);
                itr->second.invalidate(end());

                Ensures(toExpel->isAnySyncWriteOp() ==
                        itr->second.isSyncWrite());
            }

            queuedItemsMemUsage -= toExpel->size();
        }
    } else {
        /*
         * Reduce the queuedItems memory usage by the size of the items
         * being expelled from memory.
         */
        const auto addSize = [](size_t a, queued_item qi) {
            return a + qi->size();
        };
        queuedItemsMemUsage -= std::accumulate(
                firstItemToExpel, std::next(lastItemToExpel), 0, addSize);
    }

    // The item to be swapped with the dummy is not expected to be a
    // meta-data item.
    Expects(!(*iterator)->isCheckPointMetaItem());

    // Swap the item pointed to by our iterator with the dummy item
    auto dummy = begin();
    iterator->swap(*dummy);

    /*
     * Move from (and including) the first item in the checkpoint queue upto
     * (but not including) the item pointed to by iterator.  The item pointed
     * to by iterator is now the new dummy item for the checkpoint queue.
     */
    expelledItems.splice(
            ChkptQueueIterator::const_underlying_iterator{
                    expelledItems.begin()},
            toWrite,
            ChkptQueueIterator::const_underlying_iterator{begin()},
            ChkptQueueIterator::const_underlying_iterator{iterator});

    // Return the items that have been expelled in a separate queue.
    return expelledItems;
}

CheckpointIndexKeyType Checkpoint::makeIndexKey(const queued_item& item) const {
    return CheckpointIndexKeyType(item->getKey(), keyIndexKeyTrackingAllocator);
}

int64_t Checkpoint::getMutationId(const CheckpointCursor& cursor) const {
    if ((*cursor.currentPos)->isCheckPointMetaItem()) {
        auto cursor_item_idx =
                metaKeyIndex.find(makeIndexKey(*cursor.currentPos));
        if (cursor_item_idx == metaKeyIndex.end()) {
            throw std::logic_error(
                    "Checkpoint::getMutationId: Unable "
                    "to find key in metaKeyIndex with op:" +
                    to_string((*cursor.currentPos)->getOperation()) +
                    " seqno:" +
                    std::to_string((*cursor.currentPos)->getBySeqno()) +
                    "for cursor:" + cursor.name + " in current checkpoint.");
        }
        return cursor_item_idx->second.mutation_id;
    }

    auto& keyIndex = (*cursor.currentPos)->isCommitted() ? committedKeyIndex
                                                         : preparedKeyIndex;
    auto cursor_item_idx = keyIndex.find(makeIndexKey(*cursor.currentPos));
    if (cursor_item_idx == keyIndex.end()) {
        throw std::logic_error(
                "Checkpoint::getMutationId: Unable "
                "to find key in keyIndex with op:" +
                to_string((*cursor.currentPos)->getOperation()) +
                " seqno:" + std::to_string((*cursor.currentPos)->getBySeqno()) +
                "for cursor:" + cursor.name + " in current checkpoint.");
    }
    return cursor_item_idx->second.mutation_id;
}

void Checkpoint::addStats(const AddStatFn& add_stat, const void* cookie) {
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

std::ostream& operator <<(std::ostream& os, const Checkpoint& c) {
    os << "Checkpoint[" << &c << "] with"
       << " id:" << c.checkpointId << " seqno:{" << c.getLowSeqno() << ","
       << c.getHighSeqno() << "}"
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
