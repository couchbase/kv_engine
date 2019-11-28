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

CheckpointCursor::CheckpointCursor(const std::string& n,
                                   CheckpointList::iterator checkpoint,
                                   ChkptQueueIterator pos)
    : name(n), currentCheckpoint(checkpoint), currentPos(pos), numVisits(0) {
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
                       uint64_t visibleSnapEnd,
                       boost::optional<uint64_t> highCompletedSeqno,
                       Vbid vbid,
                       CheckpointType checkpointType)
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
      keyIndex(keyIndexTrackingAllocator),
      metaKeyIndex(keyIndexTrackingAllocator),
      keyIndexMemUsage(0),
      queuedItemsMemUsage(0),
      checkpointType(checkpointType),
      highCompletedSeqno(highCompletedSeqno) {
    stats.coreLocal.get()->memOverhead.fetch_add(sizeof(Checkpoint));
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

    // Check if the item is a meta item
    if (qi->isCheckPointMetaItem()) {
        // We will just queue the item
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
        if (it != keyIndex.end()) {
            if (it->second.mutation_id > highestExpelledSeqno) {
                // Normal path - we haven't expelled the item. We have a valid
                // cursor position to read the item and make our de-dupe checks.
                const auto currPos = it->second.position;
                if (!(canDedup(*currPos, qi))) {
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
                toWrite.erase(currPos);
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

        if (rv == QueueDirtyStatus::SuccessNewItem) {
            auto indexKeyUsage = qi->getKey().size() + sizeof(index_entry);
            if (!qi->isCheckPointMetaItem()) {
                indexKeyUsage += sizeof(CheckpointIndexKeyNamespace);
            }
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

    if (getState() == CHECKPOINT_OPEN) {
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

            if (!toExpel->isCheckPointMetaItem()) {
                auto itr = keyIndex.find(
                        {toExpel->getKey(),
                         toExpel->isCommitted()
                         ? CheckpointIndexKeyNamespace::Committed
                         : CheckpointIndexKeyNamespace::Prepared});
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
    expelledItems.splice(expelledItems.begin(),
                         toWrite,
                         begin(),
                         iterator);

    // Return the items that have been expelled in a separate queue.
    return expelledItems;
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

    checked_snprintf(buf,
                     sizeof(buf),
                     "vb_%d:id_%" PRIu64 ":snap_start",
                     vbucketId.get(),
                     getId());
    add_casted_stat(buf, getSnapshotStartSeqno(), add_stat, cookie);

    checked_snprintf(buf,
                     sizeof(buf),
                     "vb_%d:id_%" PRIu64 ":snap_end",
                     vbucketId.get(),
                     getId());
    add_casted_stat(buf, getSnapshotEndSeqno(), add_stat, cookie);

    checked_snprintf(buf,
                     sizeof(buf),
                     "vb_%d:id_%" PRIu64 ":visible_snap_end",
                     vbucketId.get(),
                     getId());
    add_casted_stat(buf, getVisibleSnapshotEndSeqno(), add_stat, cookie);
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
       << " type:" << to_string(c.getCheckpointType())
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
