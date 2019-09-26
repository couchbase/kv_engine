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

#include <boost/optional/optional_io.hpp>
#include <gsl.h>
#include <platform/checked_snprintf.h>
#include <string>
#include <utility>
#include <vector>

#include "bucket_logger.h"
#include "checkpoint.h"
#include "checkpoint_manager.h"
#include "ep_time.h"
#include "stats.h"
#include "statwriter.h"

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

std::pair<int64_t, uint64_t> CheckpointCursor::getCkptIdAndSeqno() const {
    return {(*currentCheckpoint)->getId(), (*currentPos)->getBySeqno()};
}

CheckpointType CheckpointCursor::getCheckpointType() const {
    return (*currentCheckpoint)->getCheckpointType();
}

std::ostream& operator<<(std::ostream& os, const CheckpointCursor& c) {
    os << "CheckpointCursor[" << &c << "] with"
       << " name:" << c.name
       << " currentCkpt:{id:" << (*c.currentCheckpoint)->getId()
       << " state:" << to_string((*c.currentCheckpoint)->getState())
       << "} currentPos:" << (*c.currentPos)->getBySeqno();
    return os;
}

Checkpoint::Checkpoint(EPStats& st,
                       uint64_t id,
                       uint64_t snapStart,
                       uint64_t snapEnd,
                       boost::optional<uint64_t> highCompletedSeqno,
                       Vbid vbid,
                       CheckpointType checkpointType)
    : stats(st),
      checkpointId(id),
      snapStartSeqno(snapStart),
      snapEndSeqno(snapEnd),
      vbucketId(vbid),
      creationTime(ep_real_time()),
      checkpointState(CHECKPOINT_OPEN),
      numItems(0),
      numMetaItems(0),
      toWrite(trackingAllocator),
      keyIndex(keyIndexTrackingAllocator),
      metaKeyIndex(keyIndexTrackingAllocator),
      queuedItemsMemUsage(0),
      checkpointType(checkpointType),
      highCompletedSeqno(highCompletedSeqno) {
    stats.coreLocal.get()->memOverhead.fetch_add(sizeof(Checkpoint) +
                                                 getKeyIndexAllocatorBytes() +
                                                 getWriteQueueAllocatorBytes());
}

Checkpoint::~Checkpoint() {
    EP_LOG_DEBUG("Checkpoint {} for {} is purged from memory",
                 checkpointId,
                 vbucketId);
    stats.coreLocal.get()->memOverhead.fetch_sub(sizeof(Checkpoint) +
                                                 getKeyIndexAllocatorBytes() +
                                                 getWriteQueueAllocatorBytes());
}

QueueDirtyStatus Checkpoint::queueDirty(const queued_item& qi,
                                        CheckpointManager* checkpointManager) {
    if (getState() != CHECKPOINT_OPEN) {
        throw std::logic_error(
                "Checkpoint::queueDirty: checkpointState "
                "(which is" +
                std::to_string(getState()) + ") is not OPEN");
    }
    TrackOverhead trackOverhead(*this);
    QueueDirtyStatus rv;

    // Check if the item is a meta item
    if (qi->isCheckPointMetaItem()) {
        rv = QueueDirtyStatus::SuccessNewItem;
        addItemToCheckpoint(qi);
    } else {
        checkpoint_index::iterator it = keyIndex.find(
                {qi->getKey(),
                 qi->isCommitted() ? CheckpointIndexKeyNamespace::Committed
                                   : CheckpointIndexKeyNamespace::Prepared});

        // Before de-duplication could discard a delete, store the largest
        // "rev-seqno" encountered
        if (qi->isDeleted() &&
            qi->getRevSeqno() > maxDeletedRevSeqno.value_or(0)) {
            maxDeletedRevSeqno = qi->getRevSeqno();
        }

        // Check if this checkpoint already has an item for the same key
        // and the item has not been expelled.
        if (it != keyIndex.end() &&
            (it->second.mutation_id > highestExpelledSeqno)) {
            const auto currPos = it->second.position;
            if (!(canDedup(*currPos, qi))) {
                return QueueDirtyStatus::FailureDuplicateItem;
            }

            rv = QueueDirtyStatus::SuccessExistingItem;
            const int64_t currMutationId{it->second.mutation_id};

            // Given the key already exists, need to check all cursors in this
            // Checkpoint and see if the existing item for this key is to
            // the "left" of the cursor (i.e. has already been processed).
            for (auto& cursor : checkpointManager->connCursors) {
                if ((*(cursor.second->currentCheckpoint)).get() == this) {
                    if (cursor.second->name == CheckpointManager::pCursorName) {
                        int64_t cursor_mutation_id =
                                getMutationId(*cursor.second);
                        queued_item& cursor_item = *(cursor.second->currentPos);
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
                        }
                    }
                    /* If a cursor points to the existing item for the same
                       key, shift it left by 1 */
                    if (cursor.second->currentPos == currPos) {
                        cursor.second->decrPos();
                    }
                }
            }

            addItemToCheckpoint(qi);

            // Reduce the size of the checkpoint by the size of the
            // item being removed.
            queuedItemsMemUsage -= ((*currPos)->size());
            // Remove the existing item for the same key from the list.
            toWrite.erase(currPos.getUnderlyingIterator());

            // Reduce the number of items because addItemToCheckpoint
            // increases the number by one.
            --numItems;
        } else {
            rv = QueueDirtyStatus::SuccessNewItem;
            addItemToCheckpoint(qi);
        }
    }

    if (qi->getKey().size() > 0) {
        ChkptQueueIterator last = end();
        // --last is okay as the list is not empty now.
        index_entry entry = {--last, qi->getBySeqno()};
        // Set the index of the key to the new item that is pushed back into
        // the list.
        if (qi->isCheckPointMetaItem()) {
            // Insert the new entry into the metaKeyIndex
            auto result = metaKeyIndex.emplace(qi->getKey(), entry);
            if (!result.second) {
                // Did not manage to insert - so update the value directly
                result.first->second = entry;
            }
        } else {
            // Insert the new entry into the keyIndex
            auto result = keyIndex.emplace(
                    CheckpointIndexKey(
                            qi->getKey(),
                            qi->isCommitted()
                                    ? CheckpointIndexKeyNamespace::Committed
                                    : CheckpointIndexKeyNamespace::Prepared),
                    entry);
            if (!result.second) {
                // Did not manage to insert - so update the value directly
                result.first->second = entry;
            }
        }
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

ExpelResult Checkpoint::expelItems(CheckpointCursor& expelUpToAndIncluding) {
    TrackOverhead trackOverhead(*this);
    ExpelResult expelResult;
    // Perform the expel inside this inner scope, this ensures that the expelled
    // items are deallocated within this function allowing memOverhead to be
    // tracked.
    {
        CheckpointQueue expelledItems(trackingAllocator);

        ChkptQueueIterator iterator = expelUpToAndIncluding.currentPos;

        // Record the seqno of the last item to be expelled.
        highestExpelledSeqno =
                iterator.getUnderlyingIterator()->get()->getBySeqno();

        // The item to be swapped with the dummy is not expected to be a
        // meta-data item.
        Expects(!iterator.getUnderlyingIterator()
                         ->get()
                         ->isCheckPointMetaItem());

        // Swap the item pointed to by our iterator with the dummy item
        auto dummy = begin().getUnderlyingIterator();
        iterator.getUnderlyingIterator()->swap(*dummy);

        /*
         * Move from (and including) the first item in the checkpoint queue upto
         * (but not including) the item pointed to by iterator.  The item
         * pointed to by iterator is now the new dummy item for the checkpoint
         * queue.
         */
        expelledItems.splice(expelledItems.begin(),
                             toWrite,
                             begin().getUnderlyingIterator(),
                             iterator.getUnderlyingIterator());

        if (getState() == CHECKPOINT_OPEN) {
            // Whilst cp is open, erase the expelled items from the indexes
            for (const auto& expelled : expelledItems) {
                size_t erased = 0;
                if (expelled->isCheckPointMetaItem()) {
                    erased = metaKeyIndex.erase(expelled->getKey());
                } else {
                    erased = keyIndex.erase(
                            {expelled->getKey(),
                             expelled->isCommitted()
                                     ? CheckpointIndexKeyNamespace::Committed
                                     : CheckpointIndexKeyNamespace::Prepared});
                }

                if (erased == 0) {
                    std::stringstream ss;
                    ss << *expelled;
                    throw std::logic_error(
                            "Checkpoint::expelItem: not found in index " +
                            ss.str());
                }
            }
            // Ask the hash-table's to rehash to fit the new number of elements
            metaKeyIndex.reserve(metaKeyIndex.size());
            keyIndex.reserve(keyIndex.size());
        }

        /*
         * Reduce the queuedItems memory usage by the size of the items
         * being expelled from memory, and record the expelled amount in the
         * result.
         */
        const auto addSize = [](size_t a, queued_item qi) {
            return a + qi->size();
        };

        expelResult.estimateOfFreeMemory = std::accumulate(
                expelledItems.begin(), expelledItems.end(), 0, addSize);
        queuedItemsMemUsage -= expelResult.estimateOfFreeMemory;
        expelResult.expelCount = expelledItems.size();
    }

    expelResult.estimateOfFreeMemory +=
            std::abs(trackOverhead.getToWriteDifference());
    return expelResult;
}

int64_t Checkpoint::getMutationId(const CheckpointCursor& cursor) const {
    if ((*cursor.currentPos)->isCheckPointMetaItem()) {
        auto cursor_item_idx =
                metaKeyIndex.find((*cursor.currentPos)->getKey());
        if (cursor_item_idx == metaKeyIndex.end()) {
            throw std::logic_error(
                    "Checkpoint::queueDirty: Unable "
                    "to find key in metaKeyIndex with op:" +
                    to_string((*cursor.currentPos)->getOperation()) +
                    " seqno:" +
                    std::to_string((*cursor.currentPos)->getBySeqno()) +
                    "for cursor:" + cursor.name + " in current checkpoint.");
        }
        return cursor_item_idx->second.mutation_id;
    }

    auto cursor_item_idx =
            keyIndex.find({(*cursor.currentPos)->getKey(),
                           (*cursor.currentPos)->isCommitted()
                                   ? CheckpointIndexKeyNamespace::Committed
                                   : CheckpointIndexKeyNamespace::Prepared});
    if (cursor_item_idx == keyIndex.end()) {
        throw std::logic_error(
                "Checkpoint::queueDirty: Unable "
                "to find key in keyIndex with op:" +
                to_string((*cursor.currentPos)->getOperation()) +
                " seqno:" + std::to_string((*cursor.currentPos)->getBySeqno()) +
                "for cursor:" + cursor.name + " in current checkpoint.");
    }
    return cursor_item_idx->second.mutation_id;
}

Checkpoint::TrackOverhead::~TrackOverhead() {
    // Add in the allocated difference to the memOverhead
    cp.stats.coreLocal.get()->memOverhead.fetch_add(getToWriteDifference() +
                                                    getKeyIndexDifference());
}

ssize_t Checkpoint::TrackOverhead::getToWriteDifference() const {
    return (cp.getWriteQueueAllocatorBytes() - toWriteAllocated);
}

ssize_t Checkpoint::TrackOverhead::getKeyIndexDifference() const {
    return (cp.getKeyIndexAllocatorBytes() - keyIndexAllocated);
}

void Checkpoint::setState(checkpoint_state state) {
    *checkpointState.wlock() = state;

    if (state == CHECKPOINT_CLOSED) {
        // Now closed, the indexes have no use and can be completely erased
        TrackOverhead trackOverhead(*this);
        keyIndex.clear();
        keyIndex.reserve(0);
        metaKeyIndex.clear();
        metaKeyIndex.reserve(0);
    }
}

void Checkpoint::addStats(const AddStatFn& add_stat, const void* cookie) {
    char buf[256];

    checked_snprintf(buf,
                     sizeof(buf),
                     "vb_%d:id_%" PRIu64 ":queued_items_mem_usage",
                     vbucketId.get(),
                     getId());
    add_casted_stat(buf, getQueuedItemsMemUsage(), add_stat, cookie);

    checked_snprintf(buf,
                     sizeof(buf),
                     "vb_%d:id_%" PRIu64 ":key_index_allocator_bytes",
                     vbucketId.get(),
                     getId());
    add_casted_stat(buf, getKeyIndexAllocatorBytes(), add_stat, cookie);

    checked_snprintf(buf,
                     sizeof(buf),
                     "vb_%d:id_%" PRIu64 ":to_write_allocator_bytes",
                     vbucketId.get(),
                     getId());
    add_casted_stat(buf, getWriteQueueAllocatorBytes(), add_stat, cookie);

    checked_snprintf(buf,
                     sizeof(buf),
                     "vb_%d:id_%" PRIu64 ":state",
                     vbucketId.get(),
                     getId());
    add_casted_stat(buf, to_string(getState()), add_stat, cookie);

    checked_snprintf(buf,
                     sizeof(buf),
                     "vb_%d:id_%" PRIu64 ":type",
                     vbucketId.get(),
                     getId());
    add_casted_stat(buf, to_string(getCheckpointType()), add_stat, cookie);
}

std::ostream& operator <<(std::ostream& os, const Checkpoint& c) {
    os << "Checkpoint[" << &c << "] with"
       << " id:" << c.checkpointId << " seqno:{" << c.getLowSeqno() << ","
       << c.getHighSeqno() << "}"
       << " snap:{" << c.getSnapshotStartSeqno() << ","
       << c.getSnapshotEndSeqno() << "}"
       << " state:" << to_string(c.getState())
       << " type:" << to_string(c.getCheckpointType())
       << " toWrite:" << c.getWriteQueueAllocatorBytes()
       << " keyIndex:" << c.getKeyIndexAllocatorBytes()
       << " hcs:" << c.getHighCompletedSeqno() << " items:[" << std::endl;
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
