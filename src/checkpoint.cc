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

Checkpoint::~Checkpoint() {
    LOG(EXTENSION_LOG_INFO,
        "Checkpoint %llu for vbucket %d is purged from memory",
        checkpointId, vbucketId);
    stats.memOverhead.fetch_sub(memorySize());
    cb_assert(stats.memOverhead.load() < GIGANTOR);
}

void Checkpoint::setState(checkpoint_state state) {
    checkpointState = state;
}

void Checkpoint::popBackCheckpointEndItem() {
    if (!toWrite.empty() &&
        toWrite.back()->getOperation() == queue_op_checkpoint_end) {
        keyIndex.erase(toWrite.back()->getKey());
        toWrite.pop_back();
    }
}

bool Checkpoint::keyExists(const std::string &key) {
    return keyIndex.find(key) != keyIndex.end();
}

queue_dirty_t Checkpoint::queueDirty(const queued_item &qi,
                                     CheckpointManager *checkpointManager) {
    assert (checkpointState == CHECKPOINT_OPEN);
    queue_dirty_t rv;

    checkpoint_index::iterator it = keyIndex.find(qi->getKey());
    // Check if this checkpoint already had an item for the same key.
    if (it != keyIndex.end()) {
        rv = EXISTING_ITEM;
        std::list<queued_item>::iterator currPos = it->second.position;
        uint64_t currMutationId = it->second.mutation_id;

        cursor_index::iterator map_it = checkpointManager->tapCursors.begin();
        for (; map_it != checkpointManager->tapCursors.end(); ++map_it) {

            if (*(map_it->second.currentCheckpoint) == this) {
                queued_item &tqi = *(map_it->second.currentPos);
                const std::string &key = tqi->getKey();
                checkpoint_index::iterator ita = keyIndex.find(key);
                if (ita != keyIndex.end()) {
                    uint64_t mutationId = ita->second.mutation_id;
                    if (currMutationId <= mutationId &&
                        tqi->getOperation() != queue_op_checkpoint_start) {
                        checkpointManager->
                                  decrCursorOffset_UNLOCKED(map_it->second, 1);
                        if (map_it->second.name.compare(CheckpointManager::pCursorName) == 0) {
                            rv = PERSIST_AGAIN;
                        }
                    }
                }
                // If an TAP cursor points to the existing item for the same
                // key, shift it left by 1
                if (map_it->second.currentPos == currPos) {
                    checkpointManager->decrCursorPos_UNLOCKED(map_it->second);
                }
            }
        }

        toWrite.push_back(qi);
        // Remove the existing item for the same key from the list.
        toWrite.erase(currPos);
    } else {
        if (qi->getOperation() == queue_op_set ||
            qi->getOperation() == queue_op_del) {
            ++numItems;
        }
        rv = NEW_ITEM;
        // Push the new item into the list
        toWrite.push_back(qi);
    }

    if (qi->getNKey() > 0) {
        std::list<queued_item>::iterator last = toWrite.end();
        // --last is okay as the list is not empty now.
        index_entry entry = {--last, qi->getBySeqno()};
        // Set the index of the key to the new item that is pushed back into
        // the list.
        keyIndex[qi->getKey()] = entry;
        if (rv == NEW_ITEM) {
            size_t newEntrySize = qi->getNKey() + sizeof(index_entry) +
                                  sizeof(queued_item);
            memOverhead += newEntrySize;
            stats.memOverhead.fetch_add(newEntrySize);
            cb_assert(stats.memOverhead.load() < GIGANTOR);
        }
    }
    return rv;
}

size_t Checkpoint::mergePrevCheckpoint(Checkpoint *pPrevCheckpoint) {
    size_t numNewItems = 0;
    size_t newEntryMemOverhead = 0;
    std::list<queued_item>::reverse_iterator rit = pPrevCheckpoint->rbegin();

    LOG(EXTENSION_LOG_INFO,
        "Collapse the checkpoint %llu into the checkpoint %llu for vbucket %d",
        pPrevCheckpoint->getId(), checkpointId, vbucketId);

    std::list<queued_item>::iterator itr = toWrite.begin();
    uint64_t seqno = pPrevCheckpoint->getMutationIdForKey("dummy_key");
    keyIndex["dummy_key"].mutation_id = seqno;
    (*itr)->setBySeqno(seqno);

    seqno = pPrevCheckpoint->getMutationIdForKey("checkpoint_start");
    keyIndex["checkpoint_start"].mutation_id = seqno;
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
            std::list<queued_item>::iterator pos = toWrite.begin();
            // Skip the first two meta items
            ++pos; ++pos;
            toWrite.insert(pos, *rit);
            index_entry entry = {--pos, static_cast<int64_t>(pPrevCheckpoint->
                                                    getMutationIdForKey(key))};
            keyIndex[key] = entry;
            newEntryMemOverhead += key.size() + sizeof(index_entry);
            ++numItems;
            ++numNewItems;
        }
    }
    memOverhead += newEntryMemOverhead;
    stats.memOverhead.fetch_add(newEntryMemOverhead);
    cb_assert(stats.memOverhead.load() < GIGANTOR);
    return numNewItems;
}

uint64_t Checkpoint::getMutationIdForKey(const std::string &key) {
    uint64_t mid = 0;
    checkpoint_index::iterator it = keyIndex.find(key);
    if (it != keyIndex.end()) {
        mid = it->second.mutation_id;
    }
    return mid;
}

CheckpointManager::~CheckpointManager() {
    LockHolder lh(queueLock);
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
        std::list<queued_item>::iterator it =
            ++(checkpointList.back()->begin());
        (*it)->setRevSeqno(id);
        if (checkpointList.back()->getId() == 0) {
            (*it)->setBySeqno(lastBySeqno + 1);
        }
        checkpointList.back()->setId(id);
        LOG(EXTENSION_LOG_INFO, "Set the current open checkpoint id to %llu "
            "for vbucket %d, bySeqno is %llu, max is %llu", id, vbucketId,
            (*it)->getBySeqno(), lastBySeqno);

    }
}

bool CheckpointManager::addNewCheckpoint_UNLOCKED(uint64_t id) {
    // This is just for making sure that the current checkpoint should be
    // closed.
    if (!checkpointList.empty() &&
        checkpointList.back()->getState() == CHECKPOINT_OPEN) {
        closeOpenCheckpoint_UNLOCKED(checkpointList.back()->getId());
    }

    LOG(EXTENSION_LOG_INFO, "Create a new open checkpoint %llu for vbucket %d",
        id, vbucketId);

    bool was_empty = checkpointList.empty() ? true : false;
    Checkpoint *checkpoint = new Checkpoint(stats, id, vbucketId,
                                            CHECKPOINT_OPEN);
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

    CheckpointCursor& persistenceCursor = tapCursors[pCursorName];
    // Move the persistence cursor to the next checkpoint if it already reached to
    // the end of its current checkpoint.
    ++(persistenceCursor.currentPos);
    if (persistenceCursor.currentPos != (*(persistenceCursor.currentCheckpoint))->end()) {
        if ((*(persistenceCursor.currentPos))->getOperation() == queue_op_checkpoint_end) {
            // Skip checkpoint_end meta item that is only used by replication cursors.
            ++(persistenceCursor.offset);
            ++(persistenceCursor.currentPos); // cursor now reaches to the checkpoint end.
        }
    }
    if (persistenceCursor.currentPos == (*(persistenceCursor.currentCheckpoint))->end()) {
        if ((*(persistenceCursor.currentCheckpoint))->getState() == CHECKPOINT_CLOSED) {
            if (!moveCursorToNextCheckpoint(persistenceCursor)) {
                --(persistenceCursor.currentPos);
            }
        } else {
            // The persistence cursor is already reached to the end of the open checkpoint.
            --(persistenceCursor.currentPos);
        }
    } else {
        --(persistenceCursor.currentPos);
    }

    // If any of replication cursors reached to the end of its current checkpoint, move it to
    // the next checkpoint. Note that the replication cursors cannot skip a checkpoint_end
    // meta item.
    std::map<const std::string, CheckpointCursor>::iterator tap_it = tapCursors.begin();
    for (; tap_it != tapCursors.end(); ++tap_it) {
        CheckpointCursor &cursor = tap_it->second;
        if (++(cursor.currentPos) == (*(cursor.currentCheckpoint))->end()) {
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

bool CheckpointManager::addNewCheckpoint(uint64_t id) {
    LockHolder lh(queueLock);
    return addNewCheckpoint_UNLOCKED(id);
}

bool CheckpointManager::closeOpenCheckpoint_UNLOCKED(uint64_t id) {
    if (checkpointList.empty()) {
        return false;
    }
    if (id != checkpointList.back()->getId() ||
        checkpointList.back()->getState() == CHECKPOINT_CLOSED) {
        return true;
    }

    LOG(EXTENSION_LOG_INFO, "Close the open checkpoint %llu for vbucket %d",
        id, vbucketId);

    // This item represents the end of the current open checkpoint and is sent to the slave node.
    queued_item qi = createCheckpointItem(id, vbucketId,
                                          queue_op_checkpoint_end);
    checkpointList.back()->queueDirty(qi, this);
    ++numItems;
    checkpointList.back()->setState(CHECKPOINT_CLOSED);
    lastClosedChkBySeqno = checkpointList.back()->getHighSeqno();
    return true;
}

bool CheckpointManager::closeOpenCheckpoint(uint64_t id) {
    LockHolder lh(queueLock);
    return closeOpenCheckpoint_UNLOCKED(id);
}

bool CheckpointManager::registerTAPCursor(const std::string &name,
                                          uint64_t checkpointId,
                                          bool alwaysFromBeginning) {
    LockHolder lh(queueLock);
    return registerTAPCursor_UNLOCKED(name,
                                      checkpointId,
                                      alwaysFromBeginning);
}

CursorRegResult CheckpointManager::registerTAPCursorBySeqno(const std::string &name,
                                                            uint64_t startBySeqno) {
    LockHolder lh(queueLock);
    cb_assert(!checkpointList.empty());
    cb_assert(checkpointList.back()->getHighSeqno() >= startBySeqno);

    removeTAPCursor_UNLOCKED(name);

    size_t skipped = 0;
    CursorRegResult result;
    result.first = std::numeric_limits<uint64_t>::max();
    result.second = false;

    std::list<Checkpoint*>::iterator itr = checkpointList.begin();
    for (; itr != checkpointList.end(); ++itr) {
        uint64_t en = (*itr)->getHighSeqno();
        uint64_t st = (*itr)->getLowSeqno();

        if (startBySeqno < st) {
            tapCursors[name] = CheckpointCursor(name, itr, (*itr)->begin(),
                                                skipped, false);
            (*itr)->registerCursorName(name);
            result.first = (*itr)->getLowSeqno();
            break;
        } else if (startBySeqno <= en) {
            std::list<queued_item>::iterator iitr = (*itr)->begin();
            while (++iitr != (*itr)->end() &&
                    startBySeqno >= static_cast<uint64_t>((*iitr)->getBySeqno())) {
                skipped++;
            }

            if (iitr == (*itr)->end()) {
                --iitr;
                result.first = static_cast<uint64_t>((*iitr)->getBySeqno()) + 1;
            } else {
                result.first = static_cast<uint64_t>((*iitr)->getBySeqno());
                --iitr;
            }

            tapCursors[name] = CheckpointCursor(name, itr, iitr, skipped,
                                                false);
            (*itr)->registerCursorName(name);
            break;
        } else {
            skipped += (*itr)->getNumItems() + 2;
        }
    }

    result.second = (result.first == checkpointList.front()->getLowSeqno()) ? true : false;

    if (result.first == std::numeric_limits<uint64_t>::max()) {
        /*
         * We should never get here since this would mean that the sequence number
         * we are looking for is higher than anything currently assigned and there
         * is already an assert above for this case.
         */
        LOG(EXTENSION_LOG_WARNING, "Cursor not registered into vb %d "
            " for stream '%s' because seqno %llu is too high",
            vbucketId, name.c_str(), startBySeqno);
    }
    return result;
}

bool CheckpointManager::registerTAPCursor_UNLOCKED(const std::string &name,
                                                   uint64_t checkpointId,
                                                   bool alwaysFromBeginning) {
    cb_assert(!checkpointList.empty());

    bool found = false;
    std::list<Checkpoint*>::iterator it = checkpointList.begin();
    for (; it != checkpointList.end(); ++it) {
        if (checkpointId == (*it)->getId()) {
            found = true;
            break;
        }
    }

    LOG(EXTENSION_LOG_INFO,
        "Register the tap cursor with the name \"%s\" for vbucket %d",
        name.c_str(), vbucketId);

    // If the tap cursor exists, remove its name from the checkpoint that is
    // currently referenced by the tap cursor.
    cursor_index::iterator map_it = tapCursors.find(name);
    if (map_it != tapCursors.end()) {
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
            "Checkpoint %llu for vbucket %d doesn't exist in memory. "
            "Set the cursor with the name \"%s\" to checkpoint %d.\n",
            checkpointId, vbucketId, name.c_str(), (*it)->getId());

        cb_assert(it != checkpointList.end());

        size_t offset = 0;
        std::list<Checkpoint*>::iterator pos = checkpointList.begin();
        for (; pos != it; ++pos) {
            offset += (*pos)->getNumItems() + 2;
        }

        tapCursors[name] = CheckpointCursor(name, it, (*it)->begin(), offset,
                                            true);
        (*it)->registerCursorName(name);
    } else {
        size_t offset = 0;
        std::list<queued_item>::iterator curr;

        LOG(EXTENSION_LOG_DEBUG,
            "Checkpoint %llu for vbucket %d exists in memory. "
            "Set the cursor with the name \"%s\" to the checkpoint %llu\n",
            checkpointId, vbucketId, name.c_str(), checkpointId);

        if (!alwaysFromBeginning &&
            map_it != tapCursors.end() &&
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

        tapCursors[name] = CheckpointCursor(name, it, curr, offset, true);
        // Register the tap cursor's name to the checkpoint.
        (*it)->registerCursorName(name);
    }

    return found;
}

bool CheckpointManager::removeTAPCursor(const std::string &name) {
    LockHolder lh(queueLock);
    return removeTAPCursor_UNLOCKED(name);
}

bool CheckpointManager::removeTAPCursor_UNLOCKED(const std::string &name) {
    cursor_index::iterator it = tapCursors.find(name);
    if (it == tapCursors.end()) {
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

    tapCursors.erase(it);
    return true;
}

uint64_t CheckpointManager::getCheckpointIdForTAPCursor(
                                                     const std::string &name) {
    LockHolder lh(queueLock);
    cursor_index::iterator it = tapCursors.find(name);
    if (it == tapCursors.end()) {
        return 0;
    }

    return (*(it->second.currentCheckpoint))->getId();
}

size_t CheckpointManager::getNumOfTAPCursors() {
    LockHolder lh(queueLock);
    return tapCursors.size();
}

size_t CheckpointManager::getNumCheckpoints() {
    LockHolder lh(queueLock);
    return checkpointList.size();
}

std::list<std::string> CheckpointManager::getTAPCursorNames() {
    LockHolder lh(queueLock);
    std::list<std::string> cursor_names;
    cursor_index::iterator tap_it = tapCursors.begin();
        for (; tap_it != tapCursors.end(); ++tap_it) {
        cursor_names.push_back((tap_it->first));
    }
    return cursor_names;
}

bool CheckpointManager::isCheckpointCreationForHighMemUsage(
                                              const RCPtr<VBucket> &vbucket) {
    bool forceCreation = false;
    double memoryUsed = static_cast<double>(stats.getTotalMemoryUsed());
    // pesistence and tap cursors are all currently in the open checkpoint?
    bool allCursorsInOpenCheckpoint =
        (tapCursors.size() + 1) == checkpointList.back()->getNumberOfCursors();

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
    cb_assert(vbucket);
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
        cursor_index::iterator map_it = tapCursors.begin();
        for (; map_it != tapCursors.end(); ++map_it) {
            decrCursorOffset_UNLOCKED(map_it->second, total_items);
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
        size_t curr_remains = getNumItemsForPersistence_UNLOCKED();
        collapseClosedCheckpoints(unrefCheckpointList);
        size_t new_remains = getNumItemsForPersistence_UNLOCKED();
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
        cursor_index::iterator mit = tapCursors.find(*cit);
        if (mit == tapCursors.end() ||
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
            cursor_index::iterator cc = tapCursors.find(*nitr);
            if (cc == tapCursors.end()) {
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
                cursor_index::iterator cc = tapCursors.find(*nameItr);
                const std::string& key = (*(cc->second.currentPos))->getKey();
                bool cursor_on_chk_start = false;
                if ((*(cc->second.currentPos))->getOperation() ==
                    queue_op_checkpoint_start) {
                    cursor_on_chk_start = true;
                }
                slowCursors[*nameItr] =
                    std::make_pair((*rit)->getMutationIdForKey(key), cursor_on_chk_start);
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
            cursor_index::iterator mit = tapCursors.find(*cit);
            if (mit != tapCursors.end()) {
                decrCursorOffset_UNLOCKED(mit->second, total_items);
            }
        }
        collapsedChks.splice(collapsedChks.end(), checkpointList,
                             checkpointList.begin(),  lastClosedChk);
    }
}

bool CheckpointManager::queueDirty(const RCPtr<VBucket> &vb, queued_item& qi,
                                   bool genSeqno) {
    LockHolder lh(queueLock);

    cb_assert(vb);
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
        addNewCheckpoint_UNLOCKED(checkpointList.back()->getId() + 1);
    }

    cb_assert(checkpointList.back()->getState() == CHECKPOINT_OPEN);

    if (genSeqno) {
        qi->setBySeqno(nextBySeqno());
    } else {
        lastBySeqno = qi->getBySeqno();
    }

    queue_dirty_t result = checkpointList.back()->queueDirty(qi, this);
    if (result == NEW_ITEM) {
        ++numItems;
    }

    if (result != EXISTING_ITEM) {
        ++stats.totalEnqueued;
        ++stats.diskQueueSize;
        vb->doStatsForQueueing(*qi, qi->size());
    }

    return result != EXISTING_ITEM;
}

void CheckpointManager::getAllItemsForCursor(const std::string& name,
                                             std::list<queued_item> &items) {
    LockHolder lh(queueLock);
    cursor_index::iterator it = tapCursors.find(name);
    if (it == tapCursors.end()) {
        return;
    }

    while (incrCursor(it->second)) {
        queued_item& qi = *(it->second.currentPos);
        items.push_back(qi);

        if (qi->getOperation() == queue_op_checkpoint_end) {
            moveCursorToNextCheckpoint(it->second);
            break;
        }
    }
}

void CheckpointManager::getAllItemsForPersistence(
                                             std::vector<queued_item> &items) {
    LockHolder lh(queueLock);
    // Get all the items up to the end of the current open checkpoint.
    CheckpointCursor& persistenceCursor = tapCursors[pCursorName];
    while (incrCursor(persistenceCursor)) {
        items.push_back(*(persistenceCursor.currentPos));
    }

    LOG(EXTENSION_LOG_DEBUG,
        "Grab %ld items through the persistence cursor from vbucket %d",
        items.size(), vbucketId);
}

queued_item CheckpointManager::nextItem(const std::string &name,
                                        bool &isLastMutationItem,
                                        uint64_t &endSeqno) {
    LockHolder lh(queueLock);
    cursor_index::iterator it = tapCursors.find(name);
    if (it == tapCursors.end()) {
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
            " the tap cursor to fetch an item from it's current checkpoint",
            vbucketId);
        queued_item qi(new Item(std::string(""), 0xffff,
                                queue_op_empty, 0, 0));
        return qi;
    }

    CheckpointCursor &cursor = it->second;
    if (incrCursor(cursor)) {
        isLastMutationItem = isLastMutationItemInCheckpoint(cursor);
        if ((*(cursor.currentCheckpoint))->getState() == CHECKPOINT_CLOSED) {
            endSeqno = (*(cursor.currentCheckpoint))->getHighSeqno();
        } else {
            endSeqno = -1;
        }
        return *(cursor.currentPos);
    } else {
        isLastMutationItem = false;
        if ((*(cursor.currentCheckpoint))->getState() == CHECKPOINT_CLOSED) {
            endSeqno = (*(cursor.currentCheckpoint))->getHighSeqno();
        } else {
            endSeqno = -1;
        }
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

void CheckpointManager::clear(vbucket_state_t vbState) {
    LockHolder lh(queueLock);
    std::list<Checkpoint*>::iterator it = checkpointList.begin();
    // Remove all the checkpoints.
    while(it != checkpointList.end()) {
        delete *it;
        ++it;
    }
    checkpointList.clear();
    numItems = 0;
    lastBySeqno = 0;
    pCursorPreCheckpointId = 0;

    uint64_t checkpointId = vbState == vbucket_state_active ? 1 : 0;
    // Add a new open checkpoint.
    addNewCheckpoint_UNLOCKED(checkpointId);
    resetCursors();
}

void CheckpointManager::resetCursors(bool resetPersistenceCursor) {
    cursor_index::iterator cit = tapCursors.begin();
    for (; cit != tapCursors.end(); ++cit) {
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

void CheckpointManager::resetTAPCursors(const std::list<std::string> &cursors){
    LockHolder lh(queueLock);
    std::list<std::string>::const_iterator it = cursors.begin();
    for (; it != cursors.end(); ++it) {
        registerTAPCursor_UNLOCKED(*it, getOpenCheckpointId_UNLOCKED(), true);
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
        closeOpenCheckpoint_UNLOCKED(checkpoint_id);
        addNewCheckpoint_UNLOCKED(checkpoint_id + 1);
    }
    return checkpoint_id;
}

bool CheckpointManager::eligibleForEviction(const std::string &key) {
    LockHolder lh(queueLock);
    uint64_t smallest_mid = std::numeric_limits<uint64_t>::max();

    if (tapCursors.empty()) {
        return true;
    }

    // Get the mutation id of the item pointed by the slowest cursor.
    // This won't cause much overhead as the number of cursors per vbucket is
    // usually bounded to 3 (persistence cursor + 2 replicas).
    cursor_index::iterator mit = tapCursors.begin();
    for (; mit != tapCursors.end(); ++mit) {
        const std::string &tkey = (*(mit->second.currentPos))->getKey();
        uint64_t mid = (*(mit->second.currentCheckpoint))->
                                                     getMutationIdForKey(tkey);
        if (mid < smallest_mid) {
            smallest_mid = mid;
        }
    }

    bool can_evict = true;
    std::list<Checkpoint*>::reverse_iterator it = checkpointList.rbegin();
    for (; it != checkpointList.rend(); ++it) {
        uint64_t mid = (*it)->getMutationIdForKey(key);
        if (mid == 0) { // key doesn't exist in a checkpoint.
            continue;
        }
        if (smallest_mid < mid) { // The slowest cursor is still
            can_evict = false;    //sitting behind a given key.
            break;
        }
    }

    return can_evict;
}

size_t CheckpointManager::getNumItemsForTAPConnection(const std::string &name) {
    LockHolder lh(queueLock);
    size_t remains = 0;
    cursor_index::iterator it = tapCursors.find(name);
    if (it != tapCursors.end()) {
        size_t offset = it->second.offset + getNumOfMetaItemsFromCursor(it->second);
        remains = (numItems > offset) ?
                   numItems - offset : 0;
    }
    return remains;
}

size_t CheckpointManager::getNumItemsForPersistence_UNLOCKED() {
    size_t num_items = numItems;
    CheckpointCursor& persistenceCursor = tapCursors[pCursorName];
    size_t offset = persistenceCursor.offset +
                    getNumOfMetaItemsFromCursor(persistenceCursor);
    return num_items > offset ? num_items - offset : 0;
}

size_t CheckpointManager::getNumOfMetaItemsFromCursor(CheckpointCursor &cursor) {
    // Get the number of meta items that can be skipped by a given cursor.
    size_t meta_items = 0;
    std::list<Checkpoint*>::iterator curr_chk = cursor.currentCheckpoint;
    for (; curr_chk != checkpointList.end(); ++curr_chk) {
        if (curr_chk == cursor.currentCheckpoint) {
            std::list<queued_item>::iterator curr_pos = cursor.currentPos;
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

void CheckpointManager::decrTapCursorFromCheckpointEnd(
                                                    const std::string &name) {
    LockHolder lh(queueLock);
    cursor_index::iterator it = tapCursors.find(name);
    if (it != tapCursors.end() &&
        (*(it->second.currentPos))->getOperation() ==
        queue_op_checkpoint_end) {
        decrCursorPos_UNLOCKED(it->second);
    }
}

uint64_t CheckpointManager::getMutationIdForKey(uint64_t chk_id,
                                                std::string key) {
    std::list<Checkpoint*>::iterator itr = checkpointList.begin();
    for (; itr != checkpointList.end(); ++itr) {
        if (chk_id == (*itr)->getId()) {
            return (*itr)->getMutationIdForKey(key);
        }
    }
    return 0;
}

bool CheckpointManager::isLastMutationItemInCheckpoint(
                                                   CheckpointCursor &cursor) {
    std::list<queued_item>::iterator it = cursor.currentPos;
    ++it;
    if (it == (*(cursor.currentCheckpoint))->end() ||
        (*it)->getOperation() == queue_op_checkpoint_end) {
        return true;
    }
    return false;
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
                } else { // TAP cursors
                    cursor_index::iterator mit = tapCursors.find(*cit);
                    mit->second.currentPos = checkpointList.back()->begin();
                }
            }
        } else {
            closeOpenCheckpoint_UNLOCKED(checkpointList.back()->getId());
            addNewCheckpoint_UNLOCKED(id);
        }
    } else {
        size_t curr_remains = getNumItemsForPersistence_UNLOCKED();
        collapseCheckpoints(id);
        size_t new_remains = getNumItemsForPersistence_UNLOCKED();
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
    cb_assert(!checkpointList.empty());

    std::map<std::string, std::pair<uint64_t, bool> > cursorMap;
    cursor_index::iterator itr;
    for (itr = tapCursors.begin(); itr != tapCursors.end(); itr++) {
        Checkpoint* chk = *(itr->second.currentCheckpoint);
        const std::string& key = (*(itr->second.currentPos))->getKey();
        bool cursor_on_chk_start = false;
        if ((*(itr->second.currentPos))->getOperation() == queue_op_checkpoint_start) {
            cursor_on_chk_start = true;
        }
        cursorMap[itr->first.c_str()] =
            std::make_pair(chk->getMutationIdForKey(key), cursor_on_chk_start);
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
    cb_assert(checkpointList.size() == 1);

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
    std::list<queued_item>::iterator cit = chk->begin();
    std::list<queued_item>::iterator last = chk->begin();
    for (i = 0; cit != chk->end(); ++i, ++cit) {
        uint64_t id = chk->getMutationIdForKey((*cit)->getKey());
        std::map<std::string, std::pair<uint64_t, bool> >::iterator mit = cursors.begin();
        while (mit != cursors.end()) {
            std::pair<uint64_t, bool> val = mit->second;
            if (val.first < id || (val.first == id && val.second &&
                (*last)->getOperation() == queue_op_checkpoint_start)) {

                cursor_index::iterator cc = tapCursors.find(mit->first);
                if (cc == tapCursors.end() ||
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
        cursor_index::iterator cc = tapCursors.find(mit->first);
        if (cc == tapCursors.end()) {
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
    cursor_index::iterator it = tapCursors.find(name);
    if (it == tapCursors.end() || getOpenCheckpointId_UNLOCKED() == 0) {
        return false;
    }

    bool hasMore = true;
    std::list<queued_item>::iterator curr = it->second.currentPos;
    ++curr;
    if (curr == (*(it->second.currentCheckpoint))->end() &&
        (*(it->second.currentCheckpoint)) == checkpointList.back()) {
        hasMore = false;
    }
    return hasMore;
}

queued_item CheckpointManager::createCheckpointItem(uint64_t id, uint16_t vbid,
                                          enum queue_operation checkpoint_op) {
    cb_assert(checkpoint_op == queue_op_checkpoint_start ||
           checkpoint_op == queue_op_checkpoint_end ||
           checkpoint_op == queue_op_empty);

    uint64_t bySeqno;
    std::stringstream key;
    if (checkpoint_op == queue_op_checkpoint_start) {
        key << "checkpoint_start";
        bySeqno = lastBySeqno + 1;
    } else if (checkpoint_op == queue_op_empty) {
        key << "dummy_key";
        bySeqno = lastBySeqno;
    } else {
        key << "checkpoint_end";
        bySeqno = lastBySeqno;
    }
    queued_item qi(new Item(key.str(), vbid, checkpoint_op, id, bySeqno));
    return qi;
}

bool CheckpointManager::hasNextForPersistence() {
    LockHolder lh(queueLock);
    bool hasMore = true;
    CheckpointCursor& persistenceCursor = tapCursors[pCursorName];
    std::list<queued_item>::iterator curr = persistenceCursor.currentPos;
    ++curr;
    if (curr == (*(persistenceCursor.currentCheckpoint))->end() &&
        (*(persistenceCursor.currentCheckpoint)) == checkpointList.back()) {
        hasMore = false;
    }
    return hasMore;
}

uint64_t CheckpointManager::createNewCheckpoint() {
    LockHolder lh(queueLock);
    if (checkpointList.back()->getNumItems() > 0) {
        uint64_t chk_id = checkpointList.back()->getId();
        closeOpenCheckpoint_UNLOCKED(chk_id);
        addNewCheckpoint_UNLOCKED(chk_id + 1);
    }
    return checkpointList.back()->getId();
}

void CheckpointManager::decrCursorOffset_UNLOCKED(CheckpointCursor &cursor,
                                                  size_t decr) {
    if (cursor.offset >= decr) {
        cursor.offset.fetch_sub(decr);
    } else {
        cursor.offset = 0;
        LOG(EXTENSION_LOG_INFO,
            "%s cursor offset is negative. Reset it to 0.",
            cursor.name.c_str());
    }
}

void CheckpointManager::decrCursorPos_UNLOCKED(CheckpointCursor &cursor) {
    if (cursor.currentPos != (*(cursor.currentCheckpoint))->begin()) {
        --(cursor.currentPos);
    }
}

uint64_t CheckpointManager::getPersistenceCursorPreChkId() {
    LockHolder lh(queueLock);
    return pCursorPreCheckpointId;
}

void CheckpointManager::itemsPersisted() {
    LockHolder lh(queueLock);
    CheckpointCursor& persistenceCursor = tapCursors[pCursorName];
    std::list<Checkpoint*>::iterator itr = persistenceCursor.currentCheckpoint;
    pCursorPreCheckpointId = ((*itr)->getId() > 0) ? (*itr)->getId() - 1 : 0;
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

    snprintf(buf, sizeof(buf), "vb_%d:open_checkpoint_id", vbucketId);
    add_casted_stat(buf, getOpenCheckpointId_UNLOCKED(), add_stat, cookie);
    snprintf(buf, sizeof(buf), "vb_%d:last_closed_checkpoint_id", vbucketId);
    add_casted_stat(buf, getLastClosedCheckpointId_UNLOCKED(),
    add_stat, cookie);
    snprintf(buf, sizeof(buf), "vb_%d:num_tap_cursors", vbucketId);
    add_casted_stat(buf, tapCursors.size(), add_stat, cookie);
    snprintf(buf, sizeof(buf), "vb_%d:num_checkpoint_items", vbucketId);
    add_casted_stat(buf, numItems, add_stat, cookie);
    snprintf(buf, sizeof(buf), "vb_%d:num_open_checkpoint_items", vbucketId);
    add_casted_stat(buf, checkpointList.empty() ? 0 :
                    checkpointList.back()->getNumItems(),
                    add_stat, cookie);
    snprintf(buf, sizeof(buf), "vb_%d:num_checkpoints", vbucketId);
    add_casted_stat(buf, checkpointList.size(), add_stat, cookie);
    snprintf(buf, sizeof(buf), "vb_%d:num_items_for_persistence", vbucketId);
    add_casted_stat(buf, getNumItemsForPersistence_UNLOCKED(),
                    add_stat, cookie);

    cursor_index::iterator tap_it = tapCursors.begin();
    for (; tap_it != tapCursors.end(); ++tap_it) {
        snprintf(buf, sizeof(buf),
                 "vb_%d:%s:cursor_checkpoint_id", vbucketId,
                 tap_it->first.c_str());
        add_casted_stat(buf, (*(tap_it->second.currentCheckpoint))->getId(),
                        add_stat, cookie);
        snprintf(buf, sizeof(buf), "vb_%d:%s:cursor_seqno", vbucketId,
                 tap_it->first.c_str());
        add_casted_stat(buf, (*(tap_it->second.currentPos))->getBySeqno(),
                        add_stat, cookie);
    }
}
