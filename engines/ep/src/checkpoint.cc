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

std::string to_string(queue_dirty_t value) {
    switch (value) {
    case queue_dirty_t::EXISTING_ITEM:
        return "exitsting item";
    case queue_dirty_t::PERSIST_AGAIN:
        return "persist again";
    case queue_dirty_t::NEW_ITEM:
        return "new item";
    }

    throw std::invalid_argument("to_string(queue_dirty_t): Invalid value: " +
                                std::to_string(int(value)));
}

void CheckpointCursor::decrOffset(size_t decr) {
    if (offset >= decr) {
        offset.fetch_sub(decr);
    } else {
        offset = 0;
        EP_LOG_DEBUG("{} cursor offset is negative. Reset it to 0.", name);
    }
}

void CheckpointCursor::decrPos() {
    if (currentPos != (*currentCheckpoint)->begin()) {
        --currentPos;
    }
}

uint64_t CheckpointCursor::getId() const {
    return (*currentCheckpoint)->getId();
}

MustSendCheckpointEnd CheckpointCursor::shouldSendCheckpointEndMetaItem() const
{
    return sendCheckpointEndMetaItem;
}

size_t CheckpointCursor::getCurrentCkptMetaItemsRead() const {
    return ckptMetaItemsRead;
}

std::ostream& operator<<(std::ostream& os, const CheckpointCursor& c) {
    os << "CheckpointCursor[" << &c << "] with"
       << " name:" << c.name
       << " currentCkpt:{id:" << (*c.currentCheckpoint)->getId()
       << " state:" << to_string((*c.currentCheckpoint)->getState())
       << "} currentPos:" << (*c.currentPos)->getBySeqno()
       << " offset:" << c.offset.load()
       << " ckptMetaItemsRead:" << c.getCurrentCkptMetaItemsRead();
    return os;
}

Checkpoint::Checkpoint(EPStats& st,
                       uint64_t id,
                       uint64_t snapStart,
                       uint64_t snapEnd,
                       uint16_t vbid)
    : stats(st),
      checkpointId(id),
      snapStartSeqno(snapStart),
      snapEndSeqno(snapEnd),
      vbucketId(vbid),
      creationTime(ep_real_time()),
      checkpointState(CHECKPOINT_OPEN),
      numItems(0),
      numMetaItems(0),
      memOverhead(0),
      effectiveMemUsage(0) {
    stats.coreLocal.get()->memOverhead.fetch_add(memorySize());
}

Checkpoint::~Checkpoint() {
    EP_LOG_DEBUG("Checkpoint {} for vb:{} is purged from memory",
                 checkpointId,
                 vbucketId);
    stats.coreLocal.get()->memOverhead.fetch_sub(memorySize());
}

size_t Checkpoint::getNumMetaItems() const {
    return numMetaItems;
}

void Checkpoint::setState(checkpoint_state state) {
    LockHolder lh(lock);
    setState_UNLOCKED(state);
}

void Checkpoint::setState_UNLOCKED(checkpoint_state state) {
    checkpointState = state;
}

bool Checkpoint::keyExists(const DocKey& key) {
    return keyIndex.find(key) != keyIndex.end();
}

queue_dirty_t Checkpoint::queueDirty(const queued_item &qi,
                                     CheckpointManager *checkpointManager) {
    if (checkpointState != CHECKPOINT_OPEN) {
        throw std::logic_error("Checkpoint::queueDirty: checkpointState "
                        "(which is" + std::to_string(checkpointState) +
                        ") is not OPEN");
    }
    queue_dirty_t rv;
    checkpoint_index::iterator it = keyIndex.find(qi->getKey());
    // Check if the item is a meta item
    if (qi->isCheckPointMetaItem()) {
        // empty items act only as a dummy element for the start of the
        // checkpoint (and are not read by clients), we do not include them in
        // numMetaItems.
        if (qi->isNonEmptyCheckpointMetaItem()) {
            ++numMetaItems;
        }
        rv = queue_dirty_t::NEW_ITEM;
        toWrite.push_back(qi);
    } else {
        // Check if this checkpoint already had an item for the same key
        if (it != keyIndex.end()) {
            rv = queue_dirty_t::EXISTING_ITEM;
            CheckpointQueue::iterator currPos = it->second.position;
            const int64_t currMutationId{it->second.mutation_id};

            // Given the key already exists, need to check all cursors in this
            // Checkpoint and see if the existing item for this key is to
            // the "left" of the cursor (i.e. has already been processed) - in
            // which case we need to adjust the cursor's offset to ensure that
            // we correctly account for the updated item which will need to be
            // iterated over.
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

                    // If the cursor item is non-meta, then we need to decrement
                    // offset if existing item is either before or on the cursor
                    // - as the cursor points to the "last processed" item.
                    // However if the cursor item is meta, then we only
                    // decrement if the the existing item is strictly less than
                    // the cursor, as meta-items can share a seqno with
                    // a non-meta item but are logically before them.
                    int64_t cursor_mutation_id{cursor_item_idx->second.mutation_id};
                    if (cursor_item->isCheckPointMetaItem()) {
                        --cursor_mutation_id;
                    }
                    if (currMutationId <= cursor_mutation_id) {
                        // Cursor has already processed the previous value for
                        // this key - need to logically move the cursor
                        // backwards one so it will pick up the new value for
                        // this key.
                        cursor.second->decrOffset(1);
                        if (cursor.second->name ==
                            CheckpointManager::pCursorName) {
                            rv = queue_dirty_t::PERSIST_AGAIN;
                        }
                    }
                    /* If an TAP cursor points to the existing item for the same
                       key, shift it left by 1 */
                    if (cursor.second->currentPos == currPos) {
                        cursor.second->decrPos();
                    }
                }
            }

            toWrite.push_back(qi);
            // Remove the existing item for the same key from the list.
            toWrite.erase(currPos);
        } else {
            ++numItems;
            rv = queue_dirty_t::NEW_ITEM;
            // Push the new item into the list
            toWrite.push_back(qi);
        }
    }

    if (qi->getKey().size() > 0) {
        CheckpointQueue::iterator last = toWrite.end();
        // --last is okay as the list is not empty now.
        index_entry entry = {--last, qi->getBySeqno()};
        // Set the index of the key to the new item that is pushed back into
        // the list.
        if (qi->isCheckPointMetaItem()) {
            // We add a meta item only once to a checkpoint
            metaKeyIndex[qi->getKey()] = entry;
        } else {
            keyIndex[qi->getKey()] = entry;
        }
        if (rv == queue_dirty_t::NEW_ITEM) {
            size_t newEntrySize = qi->getKey().size() +
                                  sizeof(index_entry) + sizeof(queued_item);
            memOverhead += newEntrySize;
            stats.coreLocal.get()->memOverhead.fetch_add(newEntrySize);
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

const StoredDocKey Checkpoint::DummyKey("dummy_key", DocNamespace::System);
const StoredDocKey Checkpoint::CheckpointStartKey("checkpoint_start", DocNamespace::System);
const StoredDocKey Checkpoint::CheckpointEndKey("checkpoint_end", DocNamespace::System);
const StoredDocKey Checkpoint::SetVBucketStateKey("set_vbucket_state", DocNamespace::System);

uint64_t Checkpoint::getMutationIdForKey(const DocKey& key, bool isMeta) {
    uint64_t mid = 0;
    checkpoint_index& chkIdx = isMeta ? metaKeyIndex : keyIndex;

    checkpoint_index::iterator it = chkIdx.find(key);
    if (it != chkIdx.end()) {
        mid = it->second.mutation_id;
    } else {
        throw std::invalid_argument("key{" +
                                    std::string(reinterpret_cast<const char*>(key.data())) +
                                    "} not found in " +
                                    std::string(isMeta ? "meta" : "key") +
                                    " index");
    }
    return mid;
}

bool Checkpoint::isEligibleToBeUnreferenced() {
    const std::set<std::string> &cursors = getCursorNameList();
    std::set<std::string>::const_iterator cit = cursors.begin();
    for (; cit != cursors.end(); ++cit) {
        if ((*cit).compare(CheckpointManager::pCursorName) == 0) {
            // Persistence cursor is on current checkpoint
            return false;
        }
    }
    return true;
}

std::ostream& operator <<(std::ostream& os, const Checkpoint& c) {
    os << "Checkpoint[" << &c << "] with"
       << " seqno:{" << c.getLowSeqno() << "," << c.getHighSeqno() << "}"
       << " state:" << to_string(c.getState())
       << " items:[" << std::endl;
    for (const auto& e : c.toWrite) {
        os << "\t{" << e->getBySeqno() << "," << to_string(e->getOperation());
        e->isDeleted() ? os << "[d]," : os << ",";
        os << e->getKey().c_str() << "," << e->size() << ",";
        e->isCheckPointMetaItem() ? os << "[m]}" : os << "}";
        os << std::endl;
    }
    os << "]";
    return os;
}
