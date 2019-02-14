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

#include "config.h"

#include <platform/checked_snprintf.h>
#include <string>
#include <utility>
#include <vector>

#include "bucket_logger.h"
#include "checkpoint.h"
#include "checkpoint_manager.h"
#include "ep_time.h"

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
                       Vbid vbid)
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
      keyIndexMemUsage(0),
      queuedItemsMemUsage(0) {
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
    if (checkpointState != CHECKPOINT_OPEN) {
        throw std::logic_error("Checkpoint::queueDirty: checkpointState "
                        "(which is" + std::to_string(checkpointState) +
                        ") is not OPEN");
    }

    QueueDirtyStatus rv;

    // Check if the item is a meta item
    if (qi->isCheckPointMetaItem()) {
        rv = QueueDirtyStatus::SuccessNewItem;
        addItemToCheckpoint(qi);
    } else {
        checkpoint_index::iterator it = keyIndex.find(qi->getKey());
        // Check if this checkpoint already has an item for the same key
        // and the item has not been expelled.
        if (it != keyIndex.end() &&
            (it->second.mutation_id > highestExpelledSeqno)) {
            const auto currPos = it->second.position;
            if ((*currPos)->getCommitted() !=
                        CommittedState::CommittedViaMutation ||
                qi->getCommitted() != CommittedState::CommittedViaMutation) {
                // Cannot de-duplicate SyncWrite items (either Pending
                // or Committed SyncWrites).
                return QueueDirtyStatus::FailureDuplicateItem;
            }

            rv = QueueDirtyStatus::SuccessExistingItem;
            const int64_t currMutationId{it->second.mutation_id};

            // Given the key already exists, need to check all cursors in this
            // Checkpoint and see if the existing item for this key is to
            // the "left" of the cursor (i.e. has already been processed).
            for (auto& cursor : checkpointManager->connCursors) {
                if ((*(cursor.second->currentCheckpoint)).get() == this) {
                    queued_item& cursor_item = *(cursor.second->currentPos);

                    auto& index =
                            cursor_item->isCheckPointMetaItem() ? metaKeyIndex
                                                                : keyIndex;

                    auto cursor_item_idx = index.find(cursor_item->getKey());
                    if (cursor_item_idx == index.end()) {
                        throw std::logic_error(
                                "Checkpoint::queueDirty: Unable "
                                "to find key with"
                                " op:" +
                                to_string(cursor_item->getOperation()) +
                                " seqno:" +
                                std::to_string(cursor_item->getBySeqno()) +
                                "for cursor:" + cursor.second->name +
                                " in current checkpoint.");
                    }

                    if (cursor.second->name == CheckpointManager::pCursorName) {
                        int64_t cursor_mutation_id{
                                cursor_item_idx->second.mutation_id};

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
            auto result = keyIndex.emplace(qi->getKey(), entry);
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

    // Notify flusher if in case queued item is a checkpoint meta item or
    // vbpersist state.
    if (qi->getOperation() == queue_op::checkpoint_start ||
        qi->getOperation() == queue_op::checkpoint_end ||
        qi->getOperation() == queue_op::set_vbucket_state) {
        checkpointManager->notifyFlusher();
    }

    return rv;
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
            iterator.getUnderlyingIterator()->get()->getBySeqno();

    // Add the item pointed to by the iterator to the expelledItems.
    expelledItems.push_back(*(iterator.getUnderlyingIterator()));

    // Update the item pointed to by the iterator to become the new dummy.
    auto dummy = begin().getUnderlyingIterator();
    iterator.getUnderlyingIterator()->reset(*dummy);
    /*
     * Move from (and including) the first item in the checkpoint queue upto
     * (but not including) the item pointed to by iterator.  The item pointed
     * to by iterator is now the new dummy item for the checkpoint queue.
     */
    expelledItems.splice(++(expelledItems.begin()),
                         toWrite,
                         begin().getUnderlyingIterator(),
                         iterator.getUnderlyingIterator());

    // Return the items that have been expelled in a separate queue.
    return expelledItems;
}

std::ostream& operator <<(std::ostream& os, const Checkpoint& c) {
    os << "Checkpoint[" << &c << "] with"
       << " seqno:{" << c.getLowSeqno() << "," << c.getHighSeqno() << "}"
       << " state:" << to_string(c.getState())
       << " items:[" << std::endl;
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
