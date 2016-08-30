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

#include "checkpoint.h"
#include "ep_engine.h"
#define STATWRITER_NAMESPACE checkpoint
#include "statwriter.h"
#undef STATWRITER_NAMESPACE
#include "vbucket.h"

const std::string CheckpointManager::pCursorName("persistence");

/**
 * A listener class to update checkpoint related configs at runtime.
 */
class CheckpointConfigChangeListener : public ValueChangedListener {
public:
    CheckpointConfigChangeListener(CheckpointConfig &c) : config(c) { }
    virtual ~CheckpointConfigChangeListener() { }

    virtual void sizeValueChanged(const std::string &key, size_t value) {
        if (key.compare("chk_period") == 0) {
            config.setCheckpointPeriod(value);
        } else if (key.compare("chk_max_items") == 0) {
            config.setCheckpointMaxItems(value);
        } else if (key.compare("max_checkpoints") == 0) {
            config.setMaxCheckpoints(value);
        }
    }

    virtual void booleanValueChanged(const std::string &key, bool value) {
        if (key.compare("item_num_based_new_chk") == 0) {
            config.allowItemNumBasedNewCheckpoint(value);
        } else if (key.compare("keep_closed_chks") == 0) {
            config.allowKeepClosedCheckpoints(value);
        } else if (key.compare("enable_chk_merge") == 0) {
            config.allowCheckpointMerge(value);
        }
    }

private:
    CheckpointConfig &config;
};

void CheckpointCursor::decrOffset(size_t decr) {
    if (offset >= decr) {
        offset.fetch_sub(decr);
    } else {
        offset = 0;
        LOG(EXTENSION_LOG_INFO, "%s cursor offset is negative. Reset it to 0.",
            name.c_str());
    }
}

void CheckpointCursor::decrPos() {
    if (currentPos != (*currentCheckpoint)->begin()) {
        --currentPos;
    }
}

MustSendCheckpointEnd CheckpointCursor::shouldSendCheckpointEndMetaItem() const
{
    return sendCheckpointEndMetaItem;
}

Checkpoint::~Checkpoint() {
    LOG(EXTENSION_LOG_INFO,
        "Checkpoint %" PRIu64 " for vbucket %d is purged from memory",
        checkpointId, vbucketId);
    stats.memOverhead.fetch_sub(memorySize());
    if (stats.memOverhead.load() >= GIGANTOR) {
        LOG(EXTENSION_LOG_WARNING,
            "Checkpoint::~Checkpoint: stats.memOverhead (which is %" PRId64
            ") is greater than %" PRId64, uint64_t(stats.memOverhead.load()),
            uint64_t(GIGANTOR));
    }
}

void Checkpoint::setState(checkpoint_state state) {
    checkpointState = state;
}

void Checkpoint::popBackCheckpointEndItem() {
    if (!toWrite.empty() &&
        toWrite.back()->getOperation() == queue_op_checkpoint_end) {
        metaKeyIndex.erase(toWrite.back()->getKey());
        toWrite.pop_back();
    }
}

bool Checkpoint::keyExists(const std::string &key) {
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
        rv = NEW_ITEM;
        toWrite.push_back(qi);
    } else {
        // Check if this checkpoint already had an item for the same key
        if (it != keyIndex.end()) {
            rv = EXISTING_ITEM;
            CheckpointQueue::iterator currPos = it->second.position;
            uint64_t currMutationId = it->second.mutation_id;

            cursor_index::iterator map_it =
                                        checkpointManager->connCursors.begin();
            for (; map_it != checkpointManager->connCursors.end(); ++map_it) {

                if (*(map_it->second.currentCheckpoint) == this) {
                    queued_item &tqi = *(map_it->second.currentPos);
                    const std::string &key = tqi->getKey();
                    checkpoint_index::iterator ita = keyIndex.find(key);
                    if (ita != keyIndex.end() && (!tqi->isCheckPointMetaItem()))
                    {
                        uint64_t mutationId = ita->second.mutation_id;
                        if (currMutationId <= mutationId) {
                            map_it->second.decrOffset(1);
                            if (map_it->second.name.compare(CheckpointManager::pCursorName)
                                == 0) {
                                rv = PERSIST_AGAIN;
                            }
                        }
                    }
                    /* If an TAP cursor points to the existing item for the same
                       key, shift it left by 1 */
                    if (map_it->second.currentPos == currPos) {
                        map_it->second.decrPos();
                    }
                }
            }

            toWrite.push_back(qi);
            // Remove the existing item for the same key from the list.
            toWrite.erase(currPos);
        } else {
            ++numItems;
            rv = NEW_ITEM;
            // Push the new item into the list
            toWrite.push_back(qi);
        }
    }

    if (qi->getNKey() > 0) {
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
        if (rv == NEW_ITEM) {
            size_t newEntrySize = qi->getNKey() + sizeof(index_entry) +
                                  sizeof(queued_item);
            memOverhead += newEntrySize;
            stats.memOverhead.fetch_add(newEntrySize);
            if (stats.memOverhead.load() >= GIGANTOR) {
                LOG(EXTENSION_LOG_WARNING,
                    "Checkpoint::queueDirty: stats.memOverhead (which is %" PRId64
                    ") is greater than %" PRId64, uint64_t(stats.memOverhead.load()),
                    uint64_t(GIGANTOR));
            }
        }
    }

    // Notify flusher if in case queued item is a checkpoint meta item
    if (qi->getOperation() == queue_op_checkpoint_start ||
        qi->getOperation() == queue_op_checkpoint_end) {
        checkpointManager->notifyFlusher();
    }

    return rv;
}

size_t Checkpoint::mergePrevCheckpoint(Checkpoint *pPrevCheckpoint) {
    size_t numNewItems = 0;
    size_t newEntryMemOverhead = 0;
    CheckpointQueue::reverse_iterator rit = pPrevCheckpoint->rbegin();

    LOG(EXTENSION_LOG_INFO,
        "Collapse the checkpoint %" PRIu64 " into the checkpoint %" PRIu64
        " for vbucket %d",
        pPrevCheckpoint->getId(), checkpointId, vbucketId);

    CheckpointQueue::iterator itr = toWrite.begin();
    uint64_t seqno = pPrevCheckpoint->getMutationIdForKey("dummy_key", true);
    metaKeyIndex["dummy_key"].mutation_id = seqno;
    (*itr)->setBySeqno(seqno);

    seqno = pPrevCheckpoint->getMutationIdForKey("checkpoint_start", true);
    metaKeyIndex["checkpoint_start"].mutation_id = seqno;
    ++itr;
    (*itr)->setBySeqno(seqno);

    for (; rit != pPrevCheckpoint->rend(); ++rit) {
        const std::string &key = (*rit)->getKey();
        if ((*rit)->getOperation() != queue_op_del &&
            (*rit)->getOperation() != queue_op_set) {
            continue;
        }
        checkpoint_index::iterator it = keyIndex.find(key);
        if (it == keyIndex.end()) {
            CheckpointQueue::iterator pos = toWrite.begin();
            // Skip the first two meta items
            ++pos; ++pos;
            toWrite.insert(pos, *rit);
            index_entry entry = {--pos, static_cast<int64_t>(pPrevCheckpoint->
                                            getMutationIdForKey(key, false))};
            keyIndex[key] = entry;
            newEntryMemOverhead += key.size() + sizeof(index_entry);
            ++numItems;
            ++numNewItems;

            // Update new checkpoint's memory usage
            incrementMemConsumption((*rit)->size());
        }
    }

    /**
     * Update snapshot start of current checkpoint to the first
     * item's sequence number, after merge completed, as items
     * from the previous checkpoint will be inserted into this
     * checkpoint.
     */
    setSnapshotStartSeqno(getLowSeqno());

    memOverhead += newEntryMemOverhead;
    stats.memOverhead.fetch_add(newEntryMemOverhead);
    LOG(EXTENSION_LOG_WARNING,
        "Checkpoint::mergePrevCheckpoint: stats.memOverhead (which is %" PRId64
        ") is greater than %" PRId64, uint64_t(stats.memOverhead.load()),
        uint64_t(GIGANTOR));
    return numNewItems;
}

uint64_t Checkpoint::getMutationIdForKey(const std::string &key, bool isMeta)
{
    uint64_t mid = 0;
    checkpoint_index& chkIdx = isMeta ? metaKeyIndex : keyIndex;

    checkpoint_index::iterator it = chkIdx.find(key);
    if (it != chkIdx.end()) {
        mid = it->second.mutation_id;
    } else {
        LOG(EXTENSION_LOG_WARNING, "%s not found in chk index", key.c_str());
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
    os << "Checkpoint[" << &c << "] with "
       << "seqno:{" << c.getLowSeqno() << "," << c.getHighSeqno() << "} "
       << "state:" << c.getState();
    return os;
}

CheckpointManager::~CheckpointManager() {
    std::list<Checkpoint*>::iterator it = checkpointList.begin();
    while(it != checkpointList.end()) {
        delete *it;
        ++it;
    }
}

uint64_t CheckpointManager::getOpenCheckpointId_UNLOCKED() {
    if (checkpointList.empty()) {
        return 0;
    }

    uint64_t id = checkpointList.back()->getId();
    return checkpointList.back()->getState() == CHECKPOINT_OPEN ? id : id + 1;
}

uint64_t CheckpointManager::getOpenCheckpointId() {
    LockHolder lh(queueLock);
    return getOpenCheckpointId_UNLOCKED();
}

uint64_t CheckpointManager::getLastClosedCheckpointId_UNLOCKED() {
    if (!isCollapsedCheckpoint) {
        uint64_t id = getOpenCheckpointId_UNLOCKED();
        lastClosedCheckpointId = id > 0 ? (id - 1) : 0;
    }
    return lastClosedCheckpointId;
}

uint64_t CheckpointManager::getLastClosedCheckpointId() {
    LockHolder lh(queueLock);
    return getLastClosedCheckpointId_UNLOCKED();
}

void CheckpointManager::setOpenCheckpointId_UNLOCKED(uint64_t id) {
    if (!checkpointList.empty()) {
        // Update the checkpoint_start item with the new Id.
        CheckpointQueue::iterator it =
            ++(checkpointList.back()->begin());
        (*it)->setRevSeqno(id);
        if (checkpointList.back()->getId() == 0) {
            (*it)->setBySeqno(lastBySeqno + 1);
            checkpointList.back()->setSnapshotStartSeqno(lastBySeqno);
            checkpointList.back()->setSnapshotEndSeqno(lastBySeqno);
        }

        checkpointList.back()->setId(id);
        LOG(EXTENSION_LOG_INFO, "Set the current open checkpoint id to %" PRIu64
            " for vbucket %d, bySeqno is %" PRId64 ", max is %" PRId64,
            id, vbucketId, (*it)->getBySeqno(), lastBySeqno);
    }
}

bool CheckpointManager::addNewCheckpoint_UNLOCKED(uint64_t id) {
    return addNewCheckpoint_UNLOCKED(id, lastBySeqno, lastBySeqno);
}

bool CheckpointManager::addNewCheckpoint_UNLOCKED(uint64_t id,
                                                  uint64_t snapStartSeqno,
                                                  uint64_t snapEndSeqno) {
    // This is just for making sure that the current checkpoint should be
    // closed.
    if (!checkpointList.empty() &&
        checkpointList.back()->getState() == CHECKPOINT_OPEN) {
        closeOpenCheckpoint_UNLOCKED();
    }

    LOG(EXTENSION_LOG_INFO, "Create a new open checkpoint %" PRIu64
        " for vbucket %" PRIu16 " at seqno:%" PRIu64,
        id, vbucketId, snapStartSeqno);

    bool was_empty = checkpointList.empty() ? true : false;
    Checkpoint *checkpoint = new Checkpoint(stats, id, snapStartSeqno,
                                            snapEndSeqno, vbucketId);
    // Add a dummy item into the new checkpoint, so that any cursor referring
    // to the actual first
    // item in this new checkpoint can be safely shifted left by 1 if the
    // first item is removed
    // and pushed into the tail.
    queued_item qi = createCheckpointItem(0, 0xffff, queue_op_empty);
    checkpoint->queueDirty(qi, this);

    // This item represents the start of the new checkpoint and is also sent to the slave node.
    qi = createCheckpointItem(id, vbucketId, queue_op_checkpoint_start);
    checkpoint->queueDirty(qi, this);
    ++numItems;
    checkpointList.push_back(checkpoint);

    if (was_empty) {
        return true;
    }

    /* If cursors reached to the end of its current checkpoint, move it to the
       next checkpoint. DCP and Persistence cursors can skip a "checkpoint end"
       meta item, but TAP cursors cannot. This is needed so that the checkpoint
       remover can remove the closed checkpoints and hence reduce the memory
       usage */
    for (auto& cur_it : connCursors) {
        CheckpointCursor &cursor = cur_it.second;
        ++(cursor.currentPos);
        if ((cursor.shouldSendCheckpointEndMetaItem() ==
             MustSendCheckpointEnd::NO) &&
            cursor.currentPos != (*(cursor.currentCheckpoint))->end() &&
            (*(cursor.currentPos))->getOperation() == queue_op_checkpoint_end) {
            /* checkpoint_end meta item is expected by TAP cursors. Hence skip
               it only for persitence and DCP cursors */
            ++(cursor.offset);
            ++(cursor.currentPos); // cursor now reaches to the checkpoint end
        }

        if (cursor.currentPos == (*(cursor.currentCheckpoint))->end()) {
           if ((*(cursor.currentCheckpoint))->getState() == CHECKPOINT_CLOSED) {
               if (!moveCursorToNextCheckpoint(cursor)) {
                   --(cursor.currentPos);
               }
           } else {
               // The replication cursor is already reached to the end of
               // the open checkpoint.
               --(cursor.currentPos);
           }
        } else {
            --(cursor.currentPos);
        }
    }

    return true;
}

bool CheckpointManager::closeOpenCheckpoint_UNLOCKED() {
    if (checkpointList.empty()) {
        return false;
    }
    if (checkpointList.back()->getState() == CHECKPOINT_CLOSED) {
        return true;
    }

    auto& cur_ckpt = checkpointList.back();
    LOG(EXTENSION_LOG_INFO, "Close the open checkpoint %" PRIu64
        " for vbucket:%" PRIu16 " seqnos:{%" PRIu64 ",%" PRIu64 "}",
        cur_ckpt->getId(), vbucketId, cur_ckpt->getLowSeqno(),
        cur_ckpt->getHighSeqno());

    // This item represents the end of the current open checkpoint and is sent
    // to the slave node.
    queued_item qi = createCheckpointItem(cur_ckpt->getId(), vbucketId,
                                          queue_op_checkpoint_end);

    checkpointList.back()->queueDirty(qi, this);
    ++numItems;
    checkpointList.back()->setState(CHECKPOINT_CLOSED);
    lastClosedChkBySeqno = checkpointList.back()->getHighSeqno();
    return true;
}

bool CheckpointManager::closeOpenCheckpoint() {
    LockHolder lh(queueLock);
    return closeOpenCheckpoint_UNLOCKED();
}

bool CheckpointManager::registerCursor(
                            const std::string& name,
                            uint64_t checkpointId,
                            bool alwaysFromBeginning,
                            MustSendCheckpointEnd needsCheckpointEndMetaItem) {
    LockHolder lh(queueLock);
    return registerCursor_UNLOCKED(name, checkpointId, alwaysFromBeginning,
                                   needsCheckpointEndMetaItem);
}

CursorRegResult CheckpointManager::registerCursorBySeqno(
                            const std::string &name,
                            uint64_t startBySeqno,
                            MustSendCheckpointEnd needsCheckPointEndMetaItem) {
    LockHolder lh(queueLock);
    if (checkpointList.empty()) {
        throw std::logic_error("CheckpointManager::registerCursorBySeqno: "
                        "checkpointList is empty");
    }
    if (checkpointList.back()->getHighSeqno() < startBySeqno) {
        throw std::invalid_argument("CheckpointManager::registerCursorBySeqno:"
                        " startBySeqno (which is " +
                        std::to_string(startBySeqno) + ") is less than last "
                        "checkpoint highSeqno (which is " +
                        std::to_string(checkpointList.back()->getHighSeqno()) +
                        ")");
    }

    removeCursor_UNLOCKED(name);

    size_t skipped = 0;
    CursorRegResult result;
    result.first = std::numeric_limits<uint64_t>::max();
    result.second = false;

    std::list<Checkpoint*>::iterator itr = checkpointList.begin();
    for (; itr != checkpointList.end(); ++itr) {
        uint64_t en = (*itr)->getHighSeqno();
        uint64_t st = (*itr)->getLowSeqno();

        if (startBySeqno < st) {
            connCursors[name] = CheckpointCursor(name, itr, (*itr)->begin(),
                                                 skipped, false,
                                                 needsCheckPointEndMetaItem);
            (*itr)->registerCursorName(name);
            result.first = (*itr)->getLowSeqno();
            break;
        } else if (startBySeqno <= en) {
            CheckpointQueue::iterator iitr = (*itr)->begin();
            while (++iitr != (*itr)->end() &&
                    (startBySeqno >=
                     static_cast<uint64_t>((*iitr)->getBySeqno()))) {
                skipped++;
            }

            if (iitr == (*itr)->end()) {
                --iitr;
                result.first = static_cast<uint64_t>((*iitr)->getBySeqno()) + 1;
            } else {
                result.first = static_cast<uint64_t>((*iitr)->getBySeqno());
                --iitr;
            }

            connCursors[name] = CheckpointCursor(name, itr, iitr, skipped,
                                                 false,
                                                 needsCheckPointEndMetaItem);
            (*itr)->registerCursorName(name);
            break;
        } else {
            skipped += (*itr)->getNumItems() + 2;
        }
    }

    result.second = (result.first == checkpointList.front()->getLowSeqno()) ?
                    true : false;

    if (result.first == std::numeric_limits<uint64_t>::max()) {
        /*
         * We should never get here since this would mean that the sequence
         * number we are looking for is higher than anything currently assigned
         *  and there is already an assert above for this case.
         */
        LOG(EXTENSION_LOG_WARNING, "Cursor not registered into vb %d "
            " for stream '%s' because seqno %" PRIu64 " is too high",
            vbucketId, name.c_str(), startBySeqno);
    }
    return result;
}

bool CheckpointManager::registerCursor_UNLOCKED(
                            const std::string &name,
                            uint64_t checkpointId,
                            bool alwaysFromBeginning,
                            MustSendCheckpointEnd needsCheckpointEndMetaItem)
{
    if (checkpointList.empty()) {
        throw std::logic_error("CheckpointManager::registerCursor_UNLOCKED: "
                        "checkpointList is empty");
    }

    bool resetOnCollapse = true;
    if (name.compare(pCursorName) == 0) {
        resetOnCollapse = false;
    }

    bool found = false;
    std::list<Checkpoint*>::iterator it = checkpointList.begin();
    for (; it != checkpointList.end(); ++it) {
        if (checkpointId == (*it)->getId()) {
            found = true;
            break;
        }
    }

    LOG(EXTENSION_LOG_INFO,
        "Register the cursor with name \"%s\" for vbucket %d",
        name.c_str(), vbucketId);

    // If the cursor exists, remove its name from the checkpoint that is
    // currently referenced by it.
    cursor_index::iterator map_it = connCursors.find(name);
    if (map_it != connCursors.end()) {
        (*(map_it->second.currentCheckpoint))->removeCursorName(name);
    }

    if (!found) {
        for (it = checkpointList.begin(); it != checkpointList.end(); ++it) {
            if (pCursorPreCheckpointId < (*it)->getId() ||
                pCursorPreCheckpointId == 0) {
                break;
            }
        }

        LOG(EXTENSION_LOG_DEBUG,
            "Checkpoint %" PRIu64 " for vbucket %d doesn't exist in memory. "
            "Set the cursor with the name \"%s\" to checkpoint %" PRIu64 ".\n",
            checkpointId, vbucketId, name.c_str(), (*it)->getId());

        if (it == checkpointList.end()) {
            throw std::logic_error("CheckpointManager::registerCursor_UNLOCKED: "
                            "failed to find checkpoint with "
                            "Id >= pCursorPreCheckpointId (which is" +
                            std::to_string(pCursorPreCheckpointId) + ")");
        }

        size_t offset = 0;
        std::list<Checkpoint*>::iterator pos = checkpointList.begin();
        for (; pos != it; ++pos) {
            offset += (*pos)->getNumItems() + 2;
        }

        connCursors[name] = CheckpointCursor(name, it, (*it)->begin(), offset,
                                             resetOnCollapse,
                                             needsCheckpointEndMetaItem);
        (*it)->registerCursorName(name);
    } else {
        size_t offset = 0;
        CheckpointQueue::iterator curr;

        LOG(EXTENSION_LOG_DEBUG,
            "Checkpoint %" PRIu64 " for vbucket %d exists in memory. "
            "Set the cursor with the name \"%s\" to the checkpoint %" PRIu64,
            checkpointId, vbucketId, name.c_str(), checkpointId);

        if (!alwaysFromBeginning &&
            map_it != connCursors.end() &&
            (*(map_it->second.currentCheckpoint))->getId() == (*it)->getId()) {
            // If the cursor is currently in the checkpoint to start with,
            // simply start from
            // its current position.
            curr = map_it->second.currentPos;
            offset = map_it->second.offset;
        } else {
            // Set the cursor's position to the begining of the checkpoint to
            // start with
            curr = (*it)->begin();
            std::list<Checkpoint*>::iterator pos = checkpointList.begin();
            for (; pos != it; ++pos) {
                offset += (*pos)->getNumItems() + 2;
                // 2 is for checkpoint start and end items.
            }
        }

        connCursors[name] = CheckpointCursor(name, it, curr, offset,
                                             resetOnCollapse,
                                             needsCheckpointEndMetaItem);
        // Register the cursor's name to the checkpoint.
        (*it)->registerCursorName(name);
    }

    return found;
}

bool CheckpointManager::removeCursor(const std::string &name) {
    LockHolder lh(queueLock);
    return removeCursor_UNLOCKED(name);
}

bool CheckpointManager::removeCursor_UNLOCKED(const std::string &name) {
    cursor_index::iterator it = connCursors.find(name);
    if (it == connCursors.end()) {
        return false;
    }

    LOG(EXTENSION_LOG_INFO,
        "Remove the checkpoint cursor with the name \"%s\" from vbucket %d",
        name.c_str(), vbucketId);

    // We can simply remove the cursor's name from the checkpoint to which it
    // currently belongs,
    // by calling
    // (*(it->second.currentCheckpoint))->removeCursorName(name);
    // However, we just want to do more sanity checks by looking at each
    // checkpoint. This won't
    // cause much overhead because the max number of checkpoints allowed per
    // vbucket is small.
    std::list<Checkpoint*>::iterator cit = checkpointList.begin();
    for (; cit != checkpointList.end(); ++cit) {
        (*cit)->removeCursorName(name);
    }

    connCursors.erase(it);
    return true;
}

uint64_t CheckpointManager::getCheckpointIdForCursor(const std::string &name) {
    LockHolder lh(queueLock);
    cursor_index::iterator it = connCursors.find(name);
    if (it == connCursors.end()) {
        return 0;
    }

    return (*(it->second.currentCheckpoint))->getId();
}

size_t CheckpointManager::getNumOfCursors() {
    LockHolder lh(queueLock);
    return connCursors.size();
}

size_t CheckpointManager::getNumCheckpoints() const {
    LockHolder lh(queueLock);
    return checkpointList.size();
}

checkpointCursorInfoList CheckpointManager::getAllCursors() {
    LockHolder lh(queueLock);
    checkpointCursorInfoList cursorInfo;
    for (auto& cur_it : connCursors) {
        cursorInfo.push_back(std::make_pair(
                        (cur_it.first),
                        (cur_it.second.shouldSendCheckpointEndMetaItem())));
    }
    return cursorInfo;
}

bool CheckpointManager::isCheckpointCreationForHighMemUsage(
                                              const RCPtr<VBucket> &vbucket) {
    bool forceCreation = false;
    double memoryUsed = static_cast<double>(stats.getTotalMemoryUsed());
    // pesistence and conn cursors are all currently in the open checkpoint?
    bool allCursorsInOpenCheckpoint =
        (connCursors.size() + 1) == checkpointList.back()->getNumberOfCursors();

    if (memoryUsed > stats.mem_high_wat &&
        allCursorsInOpenCheckpoint &&
        (checkpointList.back()->getNumItems() >= MIN_CHECKPOINT_ITEMS ||
         checkpointList.back()->getNumItems() == vbucket->ht.getNumInMemoryItems())) {
        forceCreation = true;
    }
    return forceCreation;
}

size_t CheckpointManager::removeClosedUnrefCheckpoints(
                                              const RCPtr<VBucket> &vbucket,
                                              bool &newOpenCheckpointCreated) {

    // This function is executed periodically by the non-IO dispatcher.
    LockHolder lh(queueLock);
    if (!vbucket) {
        throw std::invalid_argument("CheckpointManager::removeCloseUnrefCheckpoints:"
                        " vbucket must be non-NULL");
    }
    uint64_t oldCheckpointId = 0;
    bool canCreateNewCheckpoint = false;
    if (checkpointList.size() < checkpointConfig.getMaxCheckpoints() ||
        (checkpointList.size() == checkpointConfig.getMaxCheckpoints() &&
         checkpointList.front()->getNumberOfCursors() == 0)) {
        canCreateNewCheckpoint = true;
    }
    if (vbucket->getState() == vbucket_state_active &&
        canCreateNewCheckpoint) {

        bool forceCreation = isCheckpointCreationForHighMemUsage(vbucket);
        // Check if this master active vbucket needs to create a new open
        // checkpoint.
        oldCheckpointId = checkOpenCheckpoint_UNLOCKED(forceCreation, true);
    }
    newOpenCheckpointCreated = oldCheckpointId > 0;

    if (checkpointConfig.canKeepClosedCheckpoints()) {
        double memoryUsed = static_cast<double>(stats.getTotalMemoryUsed());
        if (memoryUsed < stats.mem_high_wat &&
            checkpointList.size() <= checkpointConfig.getMaxCheckpoints()) {
            return 0;
        }
    }

    size_t numUnrefItems = 0;
    size_t numMetaItems = 0;
    size_t numCheckpointsRemoved = 0;
    std::list<Checkpoint*> unrefCheckpointList;
    std::list<Checkpoint*>::iterator it = checkpointList.begin();
    for (; it != checkpointList.end(); ++it) {
        removeInvalidCursorsOnCheckpoint(*it);
        if ((*it)->getNumberOfCursors() > 0 ||
            (*it)->getId() > pCursorPreCheckpointId) {
            break;
        } else {
            numUnrefItems += (*it)->getNumItems();
            numMetaItems +=  2; // 2 is for checkpoint start and end items.
            ++numCheckpointsRemoved;
            if (checkpointConfig.canKeepClosedCheckpoints() &&
                (checkpointList.size() - numCheckpointsRemoved) <=
                 checkpointConfig.getMaxCheckpoints()) {
                // Collect unreferenced closed checkpoints until the number
                // of checkpoints is
                // equal to the number of max checkpoints allowed.
                ++it;
                break;
            }
        }
    }
    size_t total_items = numUnrefItems + numMetaItems;
    numItems.fetch_sub(total_items);
    if (total_items > 0) {
        cursor_index::iterator map_it = connCursors.begin();
        for (; map_it != connCursors.end(); ++map_it) {
            map_it->second.decrOffset(total_items);
        }
    }
    unrefCheckpointList.splice(unrefCheckpointList.begin(), checkpointList,
                               checkpointList.begin(), it);

    // If any cursor on a replica vbucket or downstream active vbucket
    // receiving checkpoints from
    // the upstream master is very slow and causes more closed checkpoints in
    // memory, collapse those closed checkpoints into a single one to reduce
    // the memory overhead.
    if (checkpointConfig.isCheckpointMergeSupported() &&
        !checkpointConfig.canKeepClosedCheckpoints() &&
        vbucket->getState() == vbucket_state_replica)
    {
        size_t curr_remains = getNumItemsForCursor_UNLOCKED(pCursorName);
        collapseClosedCheckpoints(unrefCheckpointList);
        size_t new_remains = getNumItemsForCursor_UNLOCKED(pCursorName);
        if (curr_remains > new_remains) {
            size_t diff = curr_remains - new_remains;
            stats.decrDiskQueueSize(diff);
            vbucket->decrDirtyQueueSize(diff);
        } else if (curr_remains < new_remains) {
            size_t diff = new_remains - curr_remains;
            stats.diskQueueSize.fetch_add(diff);
            vbucket->dirtyQueueSize.fetch_add(diff);
        }
    }
    lh.unlock();

    std::list<Checkpoint*>::iterator chkpoint_it = unrefCheckpointList.begin();
    for (; chkpoint_it != unrefCheckpointList.end(); ++chkpoint_it) {
        delete *chkpoint_it;
    }

    return numUnrefItems;
}

void CheckpointManager::removeInvalidCursorsOnCheckpoint(
                                                     Checkpoint *pCheckpoint) {
    std::list<std::string> invalidCursorNames;
    const std::set<std::string> &cursors = pCheckpoint->getCursorNameList();
    std::set<std::string>::const_iterator cit = cursors.begin();
    for (; cit != cursors.end(); ++cit) {
        cursor_index::iterator mit = connCursors.find(*cit);
        if (mit == connCursors.end() ||
            pCheckpoint != *(mit->second.currentCheckpoint)) {
            invalidCursorNames.push_back(*cit);
        }
    }

    std::list<std::string>::iterator it = invalidCursorNames.begin();
    for (; it != invalidCursorNames.end(); ++it) {
        pCheckpoint->removeCursorName(*it);
    }
}

void CheckpointManager::collapseClosedCheckpoints(
                                      std::list<Checkpoint*> &collapsedChks) {
    // If there are one open checkpoint and more than one closed checkpoint,
    // collapse those
    // closed checkpoints into one checkpoint to reduce the memory overhead.
    if (checkpointList.size() > 2) {
        std::map<std::string, std::pair<uint64_t, bool> > slowCursors;
        std::set<std::string> fastCursors;
        std::list<Checkpoint*>::iterator lastClosedChk = checkpointList.end();
        --lastClosedChk; --lastClosedChk; // Move to the last closed chkpt.
        std::set<std::string>::iterator nitr =
                (*lastClosedChk)->getCursorNameList().begin();
        // Check if there are any cursors in the last closed checkpoint, which
        // haven't yet visited any regular items belonging to the last closed
        // checkpoint. If so, then we should skip collapsing checkpoints until
        // those cursors move to the first regular item. Otherwise, those cursors will
        // visit old items from collapsed checkpoints again.
        for (; nitr != (*lastClosedChk)->getCursorNameList().end(); ++nitr) {
            cursor_index::iterator cc = connCursors.find(*nitr);
            if (cc == connCursors.end()) {
                continue;
            }
            enum queue_operation qop = (*(cc->second.currentPos))->getOperation();
            if (qop ==  queue_op_empty || qop == queue_op_checkpoint_start) {
                return;
            }
        }

        fastCursors.insert((*lastClosedChk)->getCursorNameList().begin(),
                           (*lastClosedChk)->getCursorNameList().end());
        std::list<Checkpoint*>::reverse_iterator rit = checkpointList.rbegin();
        ++rit; ++rit; //Move to the second last closed checkpoint.
        size_t numDuplicatedItems = 0, numMetaItems = 0;
        for (; rit != checkpointList.rend(); ++rit) {
            size_t numAddedItems = (*lastClosedChk)->mergePrevCheckpoint(*rit);
            numDuplicatedItems += ((*rit)->getNumItems() - numAddedItems);
            numMetaItems += 2; // checkpoint start and end meta items

            std::set<std::string>::iterator nameItr =
                (*rit)->getCursorNameList().begin();
            for (; nameItr != (*rit)->getCursorNameList().end(); ++nameItr) {
                cursor_index::iterator cc = connCursors.find(*nameItr);
                const std::string& key = (*(cc->second.currentPos))->getKey();
                bool isMetaItem =
                            (*(cc->second.currentPos))->isCheckPointMetaItem();
                bool cursor_on_chk_start = false;
                if ((*(cc->second.currentPos))->getOperation() ==
                    queue_op_checkpoint_start) {
                    cursor_on_chk_start = true;
                }
                slowCursors[*nameItr] =
                    std::make_pair((*rit)->getMutationIdForKey(key, isMetaItem),
                                   cursor_on_chk_start);
            }
        }
        putCursorsInCollapsedChk(slowCursors, lastClosedChk);

        size_t total_items = numDuplicatedItems + numMetaItems;
        numItems.fetch_sub(total_items);
        Checkpoint *pOpenCheckpoint = checkpointList.back();
        const std::set<std::string> &openCheckpointCursors =
                                    pOpenCheckpoint->getCursorNameList();
        fastCursors.insert(openCheckpointCursors.begin(),
                           openCheckpointCursors.end());
        std::set<std::string>::const_iterator cit = fastCursors.begin();
        // Update the offset of each fast cursor.
        for (; cit != fastCursors.end(); ++cit) {
            cursor_index::iterator mit = connCursors.find(*cit);
            if (mit != connCursors.end()) {
                mit->second.decrOffset(total_items);
            }
        }
        collapsedChks.splice(collapsedChks.end(), checkpointList,
                             checkpointList.begin(),  lastClosedChk);
    }
}

std::vector<std::string> CheckpointManager::getListOfCursorsToDrop() {
    LockHolder lh(queueLock);

    // List of cursor names whose streams will be closed
    std::vector<std::string> cursorsToDrop;

    size_t num_checkpoints_to_unref;
    if (checkpointList.size() == 1) {
        num_checkpoints_to_unref = 0;
    } else if (checkpointList.size() <=
              (DEFAULT_MAX_CHECKPOINTS + MAX_CHECKPOINTS_UPPER_BOUND) / 2) {
        num_checkpoints_to_unref = 1;
    } else {
        num_checkpoints_to_unref = 2;
    }

    std::list<Checkpoint*>::const_iterator it = checkpointList.begin();
    while (num_checkpoints_to_unref != 0 && it != checkpointList.end()) {
        if ((*it)->isEligibleToBeUnreferenced()) {
            const std::set<std::string> &cursors = (*it)->getCursorNameList();
            cursorsToDrop.insert(cursorsToDrop.end(), cursors.begin(), cursors.end());
        } else {
            break;
        }
        --num_checkpoints_to_unref;
        ++it;
    }
    return cursorsToDrop;
}

bool CheckpointManager::queueDirty(const RCPtr<VBucket> &vb, queued_item& qi,
                                   bool genSeqno) {
    LockHolder lh(queueLock);
    if (!vb) {
        throw std::invalid_argument("CheckpointManager::queueDirty: vb must "
                        "be non-NULL");
    }
    bool canCreateNewCheckpoint = false;
    if (checkpointList.size() < checkpointConfig.getMaxCheckpoints() ||
        (checkpointList.size() == checkpointConfig.getMaxCheckpoints() &&
         checkpointList.front()->getNumberOfCursors() == 0)) {
        canCreateNewCheckpoint = true;
    }

    if (vb->getState() == vbucket_state_active && canCreateNewCheckpoint) {
        // Only the master active vbucket can create a next open checkpoint.
        checkOpenCheckpoint_UNLOCKED(false, true);
    }

    if (checkpointList.back()->getState() == CHECKPOINT_CLOSED) {
        if (vb->getState() == vbucket_state_active) {
            addNewCheckpoint_UNLOCKED(checkpointList.back()->getId() + 1);
        } else {
            throw std::logic_error("CheckpointManager::queueDirty: vBucket "
                    "state (which is " +
                    std::string(VBucket::toString(vb->getState())) +
                    ") is not active. This is not expected. vb:" +
                    std::to_string(vb->getId()) +
                    " lastBySeqno:" + std::to_string(lastBySeqno) +
                    " genSeqno:" + std::to_string(genSeqno));
        }
    }

    if (checkpointList.back()->getState() != CHECKPOINT_OPEN) {
        throw std::logic_error(
                "Checkpoint::queueDirty: current checkpointState (which is" +
                std::to_string(checkpointList.back()->getState()) +
                ") is not OPEN");
    }

    if (genSeqno) {
        qi->setBySeqno(++lastBySeqno);
        checkpointList.back()->setSnapshotEndSeqno(lastBySeqno);
    } else {
        lastBySeqno = qi->getBySeqno();
    }
    uint64_t st = checkpointList.back()->getSnapshotStartSeqno();
    uint64_t en = checkpointList.back()->getSnapshotEndSeqno();
    if (!(st <= static_cast<uint64_t>(lastBySeqno) &&
          static_cast<uint64_t>(lastBySeqno) <= en)) {
        throw std::logic_error("CheckpointManager::queueDirty: lastBySeqno "
                "not in snapshot range. vb:" + std::to_string(vb->getId()) +
                " state:" + std::string(VBucket::toString(vb->getState())) +
                " snapshotStart:" + std::to_string(st) +
                " lastBySeqno:" + std::to_string(lastBySeqno) +
                " snapshotEnd:" + std::to_string(en) +
                " genSeqno:" + (genSeqno ? "True" : "False"));
    }

    queue_dirty_t result = checkpointList.back()->queueDirty(qi, this);

    if (result == NEW_ITEM) {
        ++numItems;
    }

    if (result != EXISTING_ITEM) {
        ++stats.totalEnqueued;
        ++stats.diskQueueSize;
        vb->doStatsForQueueing(*qi, qi->size());

        // Update the checkpoint's memory usage
        checkpointList.back()->incrementMemConsumption(qi->size());
    }

    return result != EXISTING_ITEM;
}

snapshot_range_t CheckpointManager::getAllItemsForCursor(
                                             const std::string& name,
                                             std::vector<queued_item> &items) {
    LockHolder lh(queueLock);
    snapshot_range_t range;
    cursor_index::iterator it = connCursors.find(name);
    if (it == connCursors.end()) {
        range.start = 0;
        range.end = 0;
        return range;
    }

    bool moreItems;
    range.start = (*it->second.currentCheckpoint)->getSnapshotStartSeqno();
    range.end = (*it->second.currentCheckpoint)->getSnapshotEndSeqno();
    while ((moreItems = incrCursor(it->second))) {
        queued_item& qi = *(it->second.currentPos);
        items.push_back(qi);

        if (qi->getOperation() == queue_op_checkpoint_end) {
            range.end = (*it->second.currentCheckpoint)->getSnapshotEndSeqno();
            moveCursorToNextCheckpoint(it->second);
        }
    }

    if (!moreItems) {
        range.end = (*it->second.currentCheckpoint)->getSnapshotEndSeqno();
    }

    it->second.numVisits++;

    return range;
}

queued_item CheckpointManager::nextItem(const std::string &name,
                                        bool &isLastMutationItem) {
    LockHolder lh(queueLock);
    cursor_index::iterator it = connCursors.find(name);
    if (it == connCursors.end()) {
        LOG(EXTENSION_LOG_WARNING,
        "The cursor with name \"%s\" is not found in the checkpoint of vbucket"
        "%d.\n", name.c_str(), vbucketId);
        queued_item qi(new Item(std::string(""), 0xffff,
                                queue_op_empty, 0, 0));
        return qi;
    }
    if (checkpointList.back()->getId() == 0) {
        LOG(EXTENSION_LOG_INFO,
            "VBucket %d is still in backfill phase that doesn't allow "
            " the cursor to fetch an item from it's current checkpoint",
            vbucketId);
        queued_item qi(new Item(std::string(""), 0xffff,
                                queue_op_empty, 0, 0));
        return qi;
    }

    CheckpointCursor &cursor = it->second;
    if (incrCursor(cursor)) {
        isLastMutationItem = isLastMutationItemInCheckpoint(cursor);
        return *(cursor.currentPos);
    } else {
        isLastMutationItem = false;
        queued_item qi(new Item(std::string(""), 0xffff,
                                queue_op_empty, 0, 0));
        return qi;
    }
}

bool CheckpointManager::incrCursor(CheckpointCursor &cursor) {
    if (++(cursor.currentPos) != (*(cursor.currentCheckpoint))->end()) {
        ++(cursor.offset);
        return true;
    } else if (!moveCursorToNextCheckpoint(cursor)) {
        --(cursor.currentPos);
        return false;
    }
    return incrCursor(cursor);
}

void CheckpointManager::clear(RCPtr<VBucket> &vb, uint64_t seqno) {
    LockHolder lh(queueLock);
    clear_UNLOCKED(vb->getState(), seqno);

    // Reset the disk write queue size stat for the vbucket
    size_t currentDqSize = vb->dirtyQueueSize.load();
    vb->decrDirtyQueueSize(currentDqSize);
    stats.decrDiskQueueSize(currentDqSize);
}

void CheckpointManager::clear_UNLOCKED(vbucket_state_t vbState, uint64_t seqno) {
    std::list<Checkpoint*>::iterator it = checkpointList.begin();
    // Remove all the checkpoints.
    while(it != checkpointList.end()) {
        delete *it;
        ++it;
    }
    checkpointList.clear();
    numItems = 0;
    lastBySeqno = seqno;
    pCursorPreCheckpointId = 0;

    uint64_t checkpointId = vbState == vbucket_state_active ? 1 : 0;
    // Add a new open checkpoint.
    addNewCheckpoint_UNLOCKED(checkpointId);
    resetCursors();
}

void CheckpointManager::resetCursors(bool resetPersistenceCursor) {
    cursor_index::iterator cit = connCursors.begin();
    for (; cit != connCursors.end(); ++cit) {
        if (cit->second.name.compare(pCursorName) == 0) {
            if (!resetPersistenceCursor) {
                continue;
            } else {
                uint64_t chkid = checkpointList.front()->getId();
                pCursorPreCheckpointId = chkid ? chkid - 1 : 0;
            }
        }
        cit->second.currentCheckpoint = checkpointList.begin();
        cit->second.currentPos = checkpointList.front()->begin();
        cit->second.offset = 0;
        checkpointList.front()->registerCursorName(cit->second.name);
    }
}

void CheckpointManager::resetCursors(checkpointCursorInfoList &cursors) {
    LockHolder lh(queueLock);

    for (auto& it : cursors) {
        registerCursor_UNLOCKED(it.first, getOpenCheckpointId_UNLOCKED(), true,
                                it.second);
    }
}

bool CheckpointManager::moveCursorToNextCheckpoint(CheckpointCursor &cursor) {
    if ((*(cursor.currentCheckpoint))->getState() == CHECKPOINT_OPEN) {
        return false;
    } else if ((*(cursor.currentCheckpoint))->getState() ==
                                                           CHECKPOINT_CLOSED) {
        std::list<Checkpoint*>::iterator currCheckpoint =
                                                      cursor.currentCheckpoint;
        if (++currCheckpoint == checkpointList.end()) {
            return false;
        }
    }

    // Remove the cursor's name from its current checkpoint.
    (*(cursor.currentCheckpoint))->removeCursorName(cursor.name);
    // Move the cursor to the next checkpoint.
    ++(cursor.currentCheckpoint);
    cursor.currentPos = (*(cursor.currentCheckpoint))->begin();
    // Register the cursor's name to its new current checkpoint.
    (*(cursor.currentCheckpoint))->registerCursorName(cursor.name);
    return true;
}

size_t CheckpointManager::getNumOpenChkItems() {
    LockHolder lh(queueLock);
    if (checkpointList.empty()) {
        return 0;
    }
    return checkpointList.back()->getNumItems() + 1;
}

uint64_t CheckpointManager::checkOpenCheckpoint_UNLOCKED(bool forceCreation,
                                                         bool timeBound) {
    int checkpoint_id = 0;

    timeBound = timeBound &&
                (ep_real_time() - checkpointList.back()->getCreationTime()) >=
                checkpointConfig.getCheckpointPeriod();
    // Create the new open checkpoint if any of the following conditions is
    // satisfied:
    // (1) force creation due to online update or high memory usage
    // (2) current checkpoint is reached to the max number of items allowed.
    // (3) time elapsed since the creation of the current checkpoint is greater
    //     than the threshold
    if (forceCreation ||
        (checkpointConfig.isItemNumBasedNewCheckpoint() &&
         checkpointList.back()->getNumItems() >=
         checkpointConfig.getCheckpointMaxItems()) ||
        (checkpointList.back()->getNumItems() > 0 && timeBound)) {

        checkpoint_id = checkpointList.back()->getId();
        addNewCheckpoint_UNLOCKED(checkpoint_id + 1);
    }
    return checkpoint_id;
}

size_t CheckpointManager::getNumItemsForCursor(const std::string &name) {
    LockHolder lh(queueLock);
    return getNumItemsForCursor_UNLOCKED(name);
}

size_t CheckpointManager::getNumItemsForCursor_UNLOCKED(const std::string &name) {
    size_t remains = 0;
    cursor_index::iterator it = connCursors.find(name);
    if (it != connCursors.end()) {
        size_t offset = it->second.offset + getNumOfMetaItemsFromCursor(it->second);
        remains = (numItems > offset) ? numItems - offset : 0;
    }
    return remains;
}

size_t CheckpointManager::getNumOfMetaItemsFromCursor(CheckpointCursor &cursor) {
    // Get the number of meta items that can be skipped by a given cursor.
    size_t meta_items = 0;
    std::list<Checkpoint*>::iterator curr_chk = cursor.currentCheckpoint;
    for (; curr_chk != checkpointList.end(); ++curr_chk) {
        if (curr_chk == cursor.currentCheckpoint) {
            CheckpointQueue::iterator curr_pos = cursor.currentPos;
            ++curr_pos;
            if (curr_pos == (*curr_chk)->end()) {
                continue;
            }
            if ((*curr_pos)->getOperation() == queue_op_checkpoint_start) {
                if ((*curr_chk)->getState() == CHECKPOINT_CLOSED) {
                    meta_items += 2;
                } else {
                    ++meta_items;
                }
            } else {
                if ((*curr_chk)->getState() == CHECKPOINT_CLOSED) {
                    ++meta_items;
                }
            }
        } else {
            if ((*curr_chk)->getState() == CHECKPOINT_CLOSED) {
                meta_items += 2;
            } else {
                ++meta_items;
            }
        }
    }
    return meta_items;
}

void CheckpointManager::decrCursorFromCheckpointEnd(const std::string &name) {
    LockHolder lh(queueLock);
    cursor_index::iterator it = connCursors.find(name);
    if (it != connCursors.end() &&
        (*(it->second.currentPos))->getOperation() ==
        queue_op_checkpoint_end) {
        it->second.decrPos();
    }
}

bool CheckpointManager::isLastMutationItemInCheckpoint(
                                                   CheckpointCursor &cursor) {
    CheckpointQueue::iterator it = cursor.currentPos;
    ++it;
    if (it == (*(cursor.currentCheckpoint))->end() ||
        (*it)->getOperation() == queue_op_checkpoint_end) {
        return true;
    }
    return false;
}

void CheckpointManager::setBackfillPhase(uint64_t start, uint64_t end) {
    LockHolder lh(queueLock);
    setOpenCheckpointId_UNLOCKED(0);
    checkpointList.back()->setSnapshotStartSeqno(start);
    checkpointList.back()->setSnapshotEndSeqno(end);
}

void CheckpointManager::createSnapshot(uint64_t snapStartSeqno,
                                       uint64_t snapEndSeqno) {
    LockHolder lh(queueLock);
    if (checkpointList.empty()) {
        throw std::logic_error("CheckpointManager::createSnapshot: "
                        "checkpointList is empty");
    }

    size_t lastChkId = checkpointList.back()->getId();

    if (checkpointList.back()->getState() == CHECKPOINT_OPEN &&
        checkpointList.back()->getNumItems() == 0) {
        if (lastChkId == 0) {
            setOpenCheckpointId_UNLOCKED(lastChkId + 1);
            resetCursors(false);
        }
        checkpointList.back()->setSnapshotStartSeqno(snapStartSeqno);
        checkpointList.back()->setSnapshotEndSeqno(snapEndSeqno);
        return;
    }

    addNewCheckpoint_UNLOCKED(lastChkId + 1, snapStartSeqno, snapEndSeqno);
}

void CheckpointManager::resetSnapshotRange() {
    LockHolder lh(queueLock);
    if (checkpointList.empty()) {
        throw std::logic_error("CheckpointManager::resetSnapshotRange: "
                        "checkpointList is empty");
    }

    // Update snapshot_start and snapshot_end only if the open
    // checkpoint has no items, otherwise just set the
    // snapshot_end to the high_seqno.
    if (checkpointList.back()->getState() == CHECKPOINT_OPEN &&
        checkpointList.back()->getNumItems() == 0) {
        checkpointList.back()->setSnapshotStartSeqno(
                                        static_cast<uint64_t>(lastBySeqno + 1));
        checkpointList.back()->setSnapshotEndSeqno(
                                        static_cast<uint64_t>(lastBySeqno + 1));

    } else {
        checkpointList.back()->setSnapshotEndSeqno(
                                        static_cast<uint64_t>(lastBySeqno));
    }
}

snapshot_info_t CheckpointManager::getSnapshotInfo() {
    LockHolder lh(queueLock);
    if (checkpointList.empty()) {
        throw std::logic_error("CheckpointManager::getSnapshotInfo: "
                        "checkpointList is empty");
    }

    snapshot_info_t info;
    info.range.start = checkpointList.back()->getSnapshotStartSeqno();
    info.start = lastBySeqno;
    info.range.end = checkpointList.back()->getSnapshotEndSeqno();

    // If there are no items in the open checkpoint then we need to resume by
    // using that sequence numbers of the last closed snapshot. The exception is
    // if we are in a partial snapshot which can be detected by checking if the
    // snapshot start sequence number is greater than the start sequence number
    // Also, since the last closed snapshot may not be in the checkpoint manager
    // we should just use the last by sequence number. The open checkpoint will
    // be overwritten once the next snapshot marker is received since there are
    // no items in it.
    if (checkpointList.back()->getNumItems() == 0 &&
        static_cast<uint64_t>(lastBySeqno) < info.range.start) {
        info.range.start = lastBySeqno;
        info.range.end = lastBySeqno;
    }

    return info;
}

void CheckpointManager::checkAndAddNewCheckpoint(uint64_t id,
                                               const RCPtr<VBucket> &vbucket) {
    LockHolder lh(queueLock);

    // Ignore CHECKPOINT_START message with ID 0 as 0 is reserved for
    // representing backfill.
    if (id == 0) {
        return;
    }
    // If the replica receives a checkpoint start message right after backfill
    // completion, simply set the current open checkpoint id to the one
    // received from the active vbucket.
    if (checkpointList.back()->getId() == 0) {
        setOpenCheckpointId_UNLOCKED(id);
        resetCursors(false);
        return;
    }

    std::list<Checkpoint*>::iterator it = checkpointList.begin();
    // Check if a checkpoint exists with ID >= id.
    while (it != checkpointList.end()) {
        if (id <= (*it)->getId()) {
            break;
        }
        ++it;
    }

    if (it == checkpointList.end()) {
        if ((checkpointList.back()->getId() + 1) < id) {
            isCollapsedCheckpoint = true;
            uint64_t oid = getOpenCheckpointId_UNLOCKED();
            lastClosedCheckpointId = oid > 0 ? (oid - 1) : 0;
        } else if ((checkpointList.back()->getId() + 1) == id) {
            isCollapsedCheckpoint = false;
        }
        if (checkpointList.back()->getState() == CHECKPOINT_OPEN &&
            checkpointList.back()->getNumItems() == 0) {
            // If the current open checkpoint doesn't have any items, simply
            // set its id to
            // the one from the master node.
            setOpenCheckpointId_UNLOCKED(id);
            // Reposition all the cursors in the open checkpoint to the
            // begining position so that a checkpoint_start message can be
            // sent again with the correct id.
            const std::set<std::string> &cursors = checkpointList.back()->
                                                   getCursorNameList();
            std::set<std::string>::const_iterator cit = cursors.begin();
            for (; cit != cursors.end(); ++cit) {
                if ((*cit).compare(pCursorName) == 0) {
                    // Persistence cursor
                    continue;
                } else { // Dcp/Tap cursors
                    cursor_index::iterator mit = connCursors.find(*cit);
                    mit->second.currentPos = checkpointList.back()->begin();
                }
            }
        } else {
            addNewCheckpoint_UNLOCKED(id);
        }
    } else {
        size_t curr_remains = getNumItemsForCursor_UNLOCKED(pCursorName);
        collapseCheckpoints(id);
        size_t new_remains = getNumItemsForCursor_UNLOCKED(pCursorName);
        if (curr_remains > new_remains) {
            size_t diff = curr_remains - new_remains;
            stats.decrDiskQueueSize(diff);
            vbucket->decrDirtyQueueSize(diff);
        } else if (curr_remains < new_remains) {
            size_t diff = new_remains - curr_remains;
            stats.diskQueueSize.fetch_add(diff);
            vbucket->dirtyQueueSize.fetch_add(diff);
        }
    }
}

void CheckpointManager::collapseCheckpoints(uint64_t id) {
    if (checkpointList.empty()) {
        throw std::logic_error("CheckpointManager::collapseCheckpoints: "
                        "checkpointList is empty");
    }

    std::map<std::string, std::pair<uint64_t, bool> > cursorMap;
    cursor_index::iterator itr;
    for (itr = connCursors.begin(); itr != connCursors.end(); itr++) {
        Checkpoint* chk = *(itr->second.currentCheckpoint);
        const std::string& key = (*(itr->second.currentPos))->getKey();
        bool isMetaItem = (*(itr->second.currentPos))->isCheckPointMetaItem();
        bool cursor_on_chk_start = false;
        if ((*(itr->second.currentPos))->getOperation() == queue_op_checkpoint_start) {
            cursor_on_chk_start = true;
        }
        cursorMap[itr->first.c_str()] =
            std::make_pair(chk->getMutationIdForKey(key, isMetaItem),
                           cursor_on_chk_start);
    }

    setOpenCheckpointId_UNLOCKED(id);

    std::list<Checkpoint*>::reverse_iterator rit = checkpointList.rbegin();
    ++rit; // Move to the last closed checkpoint.
    size_t numDuplicatedItems = 0, numMetaItems = 0;
    // Collapse all checkpoints.
    for (; rit != checkpointList.rend(); ++rit) {
        size_t numAddedItems = checkpointList.back()->
                               mergePrevCheckpoint(*rit);
        numDuplicatedItems += ((*rit)->getNumItems() - numAddedItems);
        numMetaItems += 2; // checkpoint start and end meta items
        delete *rit;
    }
    numItems.fetch_sub(numDuplicatedItems + numMetaItems);

    if (checkpointList.size() > 1) {
        checkpointList.erase(checkpointList.begin(), --checkpointList.end());
    }

    if (checkpointList.size() != 1) {
        throw std::logic_error("CheckpointManager::collapseCheckpoints: "
                "checkpointList.size (which is" +
                std::to_string(checkpointList.size()) + " is not 1");
    }

    if (checkpointList.back()->getState() == CHECKPOINT_CLOSED) {
        checkpointList.back()->popBackCheckpointEndItem();
        --numItems;
        checkpointList.back()->setState(CHECKPOINT_OPEN);
    }
    putCursorsInCollapsedChk(cursorMap, checkpointList.begin());
}

void CheckpointManager::
putCursorsInCollapsedChk(std::map<std::string, std::pair<uint64_t, bool> > &cursors,
                         std::list<Checkpoint*>::iterator chkItr) {
    size_t i;
    Checkpoint *chk = *chkItr;
    CheckpointQueue::iterator cit = chk->begin();
    CheckpointQueue::iterator last = chk->begin();
    for (i = 0; cit != chk->end(); ++i, ++cit) {
        uint64_t id = chk->getMutationIdForKey((*cit)->getKey(),
                                               (*cit)->isCheckPointMetaItem());
        std::map<std::string, std::pair<uint64_t, bool> >::iterator mit = cursors.begin();
        while (mit != cursors.end()) {
            std::pair<uint64_t, bool> val = mit->second;
            if (val.first < id || (val.first == id && val.second &&
                (*last)->getOperation() == queue_op_checkpoint_start)) {

                cursor_index::iterator cc = connCursors.find(mit->first);
                if (cc == connCursors.end() ||
                    cc->second.fromBeginningOnChkCollapse) {
                    ++mit;
                    continue;
                }
                cc->second.currentCheckpoint = chkItr;
                cc->second.currentPos = last;
                cc->second.offset = (i > 0) ? i - 1 : 0;
                chk->registerCursorName(cc->second.name);
                cursors.erase(mit++);
            } else {
                ++mit;
            }
        }

        last = cit;
        if (cursors.empty()) {
            break;
        }
    }

    std::map<std::string, std::pair<uint64_t, bool> >::iterator mit = cursors.begin();
    for (; mit != cursors.end(); ++mit) {
        cursor_index::iterator cc = connCursors.find(mit->first);
        if (cc == connCursors.end()) {
            continue;
        }
        cc->second.currentCheckpoint = chkItr;
        if (cc->second.fromBeginningOnChkCollapse) {
            cc->second.currentPos = chk->begin();
            cc->second.offset = 0;
        } else {
            cc->second.currentPos = last;
            cc->second.offset = (i > 0) ? i - 1 : 0;
        }
        chk->registerCursorName(cc->second.name);
    }
}

bool CheckpointManager::hasNext(const std::string &name) {
    LockHolder lh(queueLock);
    cursor_index::iterator it = connCursors.find(name);
    if (it == connCursors.end() || getOpenCheckpointId_UNLOCKED() == 0) {
        return false;
    }

    bool hasMore = true;
    CheckpointQueue::iterator curr = it->second.currentPos;
    ++curr;
    if (curr == (*(it->second.currentCheckpoint))->end() &&
        (*(it->second.currentCheckpoint)) == checkpointList.back()) {
        hasMore = false;
    }
    return hasMore;
}

queued_item CheckpointManager::createCheckpointItem(uint64_t id, uint16_t vbid,
                                          enum queue_operation checkpoint_op) {
    uint64_t bySeqno;
    std::string key;

    switch (checkpoint_op) {
    case queue_op_checkpoint_start:
        key = "checkpoint_start";
        bySeqno = lastBySeqno + 1;
        break;
    case queue_op_checkpoint_end:
        key = "checkpoint_end";
        bySeqno = lastBySeqno;
        break;
    case queue_op_empty:
        key = "dummy_key";
        bySeqno = lastBySeqno;
        break;
    default:
        throw std::invalid_argument("CheckpointManager::createCheckpointItem:"
                        "checkpoint_op (which is " +
                        std::to_string(checkpoint_op) +
                        ") is not a valid item to create");
    }

    queued_item qi(new Item(key, vbid, checkpoint_op, id, bySeqno));
    return qi;
}

uint64_t CheckpointManager::createNewCheckpoint() {
    LockHolder lh(queueLock);
    if (checkpointList.back()->getNumItems() > 0) {
        uint64_t chk_id = checkpointList.back()->getId();
        addNewCheckpoint_UNLOCKED(chk_id + 1);
    }
    return checkpointList.back()->getId();
}

uint64_t CheckpointManager::getPersistenceCursorPreChkId() {
    LockHolder lh(queueLock);
    return pCursorPreCheckpointId;
}

void CheckpointManager::itemsPersisted() {
    LockHolder lh(queueLock);
    CheckpointCursor& persistenceCursor = connCursors[pCursorName];
    std::list<Checkpoint*>::iterator itr = persistenceCursor.currentCheckpoint;
    pCursorPreCheckpointId = ((*itr)->getId() > 0) ? (*itr)->getId() - 1 : 0;
}

size_t CheckpointManager::getMemoryUsage_UNLOCKED() {
    if (checkpointList.empty()) {
        return 0;
    }

    size_t memUsage = 0;
    std::list<Checkpoint*>::const_iterator it = checkpointList.begin();
    for (; it != checkpointList.end(); ++it) {
        memUsage += (*it)->getMemConsumption();
    }
    return memUsage;
}

size_t CheckpointManager::getMemoryUsage() {
    LockHolder lh(queueLock);
    return getMemoryUsage_UNLOCKED();
}

size_t CheckpointManager::getMemoryUsageOfUnrefCheckpoints() {
    LockHolder lh(queueLock);

    if (checkpointList.empty()) {
        return 0;
    }

    size_t memUsage = 0;
    std::list<Checkpoint*>::const_iterator it = checkpointList.begin();
    for (; it != checkpointList.end(); ++it) {
        if ((*it)->getNumberOfCursors() == 0) {
            memUsage += (*it)->getMemConsumption();
        } else {
            break;
        }
    }
    return memUsage;
}

void CheckpointConfig::addConfigChangeListener(
                                         EventuallyPersistentEngine &engine) {
    Configuration &configuration = engine.getConfiguration();
    configuration.addValueChangedListener("chk_period",
             new CheckpointConfigChangeListener(engine.getCheckpointConfig()));
    configuration.addValueChangedListener("chk_max_items",
             new CheckpointConfigChangeListener(engine.getCheckpointConfig()));
    configuration.addValueChangedListener("max_checkpoints",
             new CheckpointConfigChangeListener(engine.getCheckpointConfig()));
    configuration.addValueChangedListener("item_num_based_new_chk",
             new CheckpointConfigChangeListener(engine.getCheckpointConfig()));
    configuration.addValueChangedListener("keep_closed_chks",
             new CheckpointConfigChangeListener(engine.getCheckpointConfig()));
    configuration.addValueChangedListener("enable_chk_merge",
             new CheckpointConfigChangeListener(engine.getCheckpointConfig()));
}

CheckpointConfig::CheckpointConfig(EventuallyPersistentEngine &e) {
    Configuration &config = e.getConfiguration();
    checkpointPeriod = config.getChkPeriod();
    checkpointMaxItems = config.getChkMaxItems();
    maxCheckpoints = config.getMaxCheckpoints();
    itemNumBasedNewCheckpoint = config.isItemNumBasedNewChk();
    keepClosedCheckpoints = config.isKeepClosedChks();
    enableChkMerge = config.isEnableChkMerge();
}

bool CheckpointConfig::validateCheckpointMaxItemsParam(size_t
                                                       checkpoint_max_items) {
    if (checkpoint_max_items < MIN_CHECKPOINT_ITEMS ||
        checkpoint_max_items > MAX_CHECKPOINT_ITEMS) {
        std::stringstream ss;
        ss << "New checkpoint_max_items param value " << checkpoint_max_items
           << " is not ranged between the min allowed value " <<
           MIN_CHECKPOINT_ITEMS
           << " and max value " << MAX_CHECKPOINT_ITEMS;
        LOG(EXTENSION_LOG_WARNING, "%s", ss.str().c_str());
        return false;
    }
    return true;
}

bool CheckpointConfig::validateCheckpointPeriodParam(
                                                   size_t checkpoint_period) {
    if (checkpoint_period < MIN_CHECKPOINT_PERIOD ||
        checkpoint_period > MAX_CHECKPOINT_PERIOD) {
        std::stringstream ss;
        ss << "New checkpoint_period param value " << checkpoint_period
           << " is not ranged between the min allowed value " <<
              MIN_CHECKPOINT_PERIOD
           << " and max value " << MAX_CHECKPOINT_PERIOD;
        LOG(EXTENSION_LOG_WARNING, "%s\n", ss.str().c_str());
        return false;
    }
    return true;
}

bool CheckpointConfig::validateMaxCheckpointsParam(size_t max_checkpoints) {
    if (max_checkpoints < DEFAULT_MAX_CHECKPOINTS ||
        max_checkpoints > MAX_CHECKPOINTS_UPPER_BOUND) {
        std::stringstream ss;
        ss << "New max_checkpoints param value " << max_checkpoints
           << " is not ranged between the min allowed value " <<
              DEFAULT_MAX_CHECKPOINTS
           << " and max value " << MAX_CHECKPOINTS_UPPER_BOUND;
        LOG(EXTENSION_LOG_WARNING, "%s\n", ss.str().c_str());
        return false;
    }
    return true;
}

void CheckpointConfig::setCheckpointPeriod(size_t value) {
    if (!validateCheckpointPeriodParam(value)) {
        value = DEFAULT_CHECKPOINT_PERIOD;
    }
    checkpointPeriod = static_cast<rel_time_t>(value);
}

void CheckpointConfig::setCheckpointMaxItems(size_t value) {
    if (!validateCheckpointMaxItemsParam(value)) {
        value = DEFAULT_CHECKPOINT_ITEMS;
    }
    checkpointMaxItems = value;
}

void CheckpointConfig::setMaxCheckpoints(size_t value) {
    if (!validateMaxCheckpointsParam(value)) {
        value = DEFAULT_MAX_CHECKPOINTS;
    }
    maxCheckpoints = value;
}

void CheckpointManager::addStats(ADD_STAT add_stat, const void *cookie) {
    LockHolder lh(queueLock);
    char buf[256];

    try {
        checked_snprintf(buf, sizeof(buf), "vb_%d:open_checkpoint_id",
                         vbucketId);
        add_casted_stat(buf, getOpenCheckpointId_UNLOCKED(), add_stat, cookie);
        checked_snprintf(buf, sizeof(buf), "vb_%d:last_closed_checkpoint_id",
                         vbucketId);
        add_casted_stat(buf, getLastClosedCheckpointId_UNLOCKED(),
                        add_stat, cookie);
        checked_snprintf(buf, sizeof(buf), "vb_%d:num_conn_cursors", vbucketId);
        add_casted_stat(buf, connCursors.size(), add_stat, cookie);
        checked_snprintf(buf, sizeof(buf), "vb_%d:num_checkpoint_items",
                         vbucketId);
        add_casted_stat(buf, numItems, add_stat, cookie);
        checked_snprintf(buf, sizeof(buf), "vb_%d:num_open_checkpoint_items",
                         vbucketId);
        add_casted_stat(buf, checkpointList.empty() ? 0 :
                             checkpointList.back()->getNumItems(),
                        add_stat, cookie);
        checked_snprintf(buf, sizeof(buf), "vb_%d:num_checkpoints", vbucketId);
        add_casted_stat(buf, checkpointList.size(), add_stat, cookie);
        checked_snprintf(buf, sizeof(buf), "vb_%d:num_items_for_persistence",
                         vbucketId);
        add_casted_stat(buf, getNumItemsForCursor_UNLOCKED(pCursorName),
                        add_stat, cookie);
        checked_snprintf(buf, sizeof(buf), "vb_%d:mem_usage", vbucketId);
        add_casted_stat(buf, getMemoryUsage_UNLOCKED(), add_stat, cookie);

        cursor_index::iterator cur_it = connCursors.begin();
        for (; cur_it != connCursors.end(); ++cur_it) {
            checked_snprintf(buf, sizeof(buf),
                             "vb_%d:%s:cursor_checkpoint_id", vbucketId,
                             cur_it->first.c_str());
            add_casted_stat(buf, (*(cur_it->second.currentCheckpoint))->getId(),
                            add_stat, cookie);
            checked_snprintf(buf, sizeof(buf), "vb_%d:%s:cursor_seqno",
                             vbucketId,
                             cur_it->first.c_str());
            add_casted_stat(buf, (*(cur_it->second.currentPos))->getBySeqno(),
                            add_stat, cookie);
            checked_snprintf(buf, sizeof(buf), "vb_%d:%s:num_visits",
                             vbucketId,
                             cur_it->first.c_str());
            add_casted_stat(buf, cur_it->second.numVisits.load(),
                            add_stat, cookie);
        }
    } catch (std::exception& error) {
        LOG(EXTENSION_LOG_WARNING,
            "CheckpointManager::addStats: An error occurred while adding stats: %s",
            error.what());
    }
}

std::ostream& operator <<(std::ostream& os, const CheckpointManager& m) {
    os << "CheckpointManager[" << &m << "] with "
       << m.getNumCheckpoints() << " checkpoints, " << m.getNumItems() << " items." << std::endl;
    for (auto* c : m.checkpointList) {
        os << "    " << *c << std::endl;
    }
    return os;
}
