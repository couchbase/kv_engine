/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "vbucket.hh"
#include "checkpoint.hh"
#include "ep_engine.h"

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
        if (key.compare("inconsistent_slave_chk") == 0) {
            config.allowInconsistentSlaveCheckpoint(value);
        } else if (key.compare("item_num_based_new_chk") == 0) {
            config.allowItemNumBasedNewCheckpoint(value);
        } else if (key.compare("keep_closed_chks") == 0) {
            config.allowKeepClosedCheckpoints(value);
        } else if (key.compare("chk_meta_items_only") == 0) {
            config.allowMetaItemsOnly(value);
        }
    }

private:
    CheckpointConfig &config;
};

void Checkpoint::setState(checkpoint_state state) {
    checkpointState = state;
}

void Checkpoint::popBackCheckpointEndItem() {
    if (toWrite.size() > 0 && toWrite.back()->getOperation() == queue_op_checkpoint_end) {
        toWrite.pop_back();
    }
}

uint64_t Checkpoint::getCasForKey(const std::string &key) {
    uint64_t cas = 0;
    checkpoint_index::iterator it = keyIndex.find(key);
    if (it != keyIndex.end()) {
        std::list<queued_item>::iterator currPos = it->second.position;
        cas = (*(currPos))->getCas();
    }
    return cas;
}

queue_dirty_t Checkpoint::queueDirty(const queued_item &qi, CheckpointManager *checkpointManager) {
    assert (checkpointState == opened);

    uint64_t newMutationId = checkpointManager->nextMutationId();
    queue_dirty_t rv;

    checkpoint_index::iterator it = keyIndex.find(qi->getKey());
    // Check if this checkpoint already had an item for the same key.
    if (it != keyIndex.end()) {
        std::list<queued_item>::iterator currPos = it->second.position;
        uint64_t currMutationId = it->second.mutation_id;

        if (*(checkpointManager->persistenceCursor.currentCheckpoint) == this) {
            // If the existing item is in the left-hand side of the item pointed by the
            // persistence cursor, decrease the persistence cursor's offset by 1.
            const std::string &key = (*(checkpointManager->persistenceCursor.currentPos))->getKey();
            checkpoint_index::iterator ita = keyIndex.find(key);
            if (ita != keyIndex.end()) {
                uint64_t mutationId = ita->second.mutation_id;
                if (currMutationId <= mutationId) {
                    checkpointManager->decrPersistenceCursorOffset(1);
                }
            }
            // If the persistence cursor points to the existing item for the same key,
            // shift the cursor left by 1.
            if (checkpointManager->persistenceCursor.currentPos == currPos) {
                checkpointManager->decrPersistenceCursorPos_UNLOCKED();
            }
        }

        std::map<const std::string, CheckpointCursor>::iterator map_it;
        for (map_it = checkpointManager->tapCursors.begin();
             map_it != checkpointManager->tapCursors.end(); map_it++) {

            if (*(map_it->second.currentCheckpoint) == this) {
                const std::string &key = (*(map_it->second.currentPos))->getKey();
                checkpoint_index::iterator ita = keyIndex.find(key);
                if (ita != keyIndex.end()) {
                    uint64_t mutationId = ita->second.mutation_id;
                    if (currMutationId <= mutationId) {
                        --(map_it->second.offset);
                    }
                }
                // If an TAP cursor points to the existing item for the same key, shift it left by 1
                if (map_it->second.currentPos == currPos) {
                    --(map_it->second.currentPos);
                }
            }
        }
        // Copy the queued time of the existing item to the new one.
        qi->setQueuedTime((*currPos)->getQueuedTime());
        // Remove the existing item for the same key from the list.
        toWrite.erase(currPos);
        rv = EXISTING_ITEM;
    } else {
        if (qi->getKey().size() > 0) {
            ++numItems;
        }
        rv = NEW_ITEM;
    }
    // Push the new item into the list
    toWrite.push_back(qi);

    if (qi->getKey().size() > 0) {
        std::list<queued_item>::iterator last = toWrite.end();
        // --last is okay as the list is not empty now.
        index_entry entry = {--last, newMutationId};
        // Set the index of the key to the new item that is pushed back into the list.
        keyIndex[qi->getKey()] = entry;
        if (rv == NEW_ITEM) {
            size_t newEntrySize = qi->getKey().size() + sizeof(index_entry) + sizeof(queued_item);
            memOverhead += newEntrySize;
            stats.memOverhead.incr(newEntrySize);
            assert(stats.memOverhead.get() < GIGANTOR);
        }
    }
    return rv;
}

size_t Checkpoint::mergePrevCheckpoint(Checkpoint *pPrevCheckpoint) {
    size_t numNewItems = 0;
    size_t newEntryMemOverhead = 0;
    std::list<queued_item>::reverse_iterator rit = pPrevCheckpoint->rbegin();
    for (; rit != pPrevCheckpoint->rend(); ++rit) {
        const std::string &key = (*rit)->getKey();
        if (key.size() == 0) {
            continue;
        }
        checkpoint_index::iterator it = keyIndex.find(key);
        if (it == keyIndex.end()) {
            // Skip the first two meta items
            std::list<queued_item>::iterator pos = toWrite.begin();
            for (; pos != toWrite.end(); ++pos) {
                if ((*pos)->getKey().compare("") != 0) {
                    break;
                }
            }
            toWrite.insert(pos, *rit);
            index_entry entry = {--pos, pPrevCheckpoint->getMutationIdForKey(key)};
            keyIndex[key] = entry;
            newEntryMemOverhead += key.size() + sizeof(index_entry);
            ++numItems;
            ++numNewItems;
        }
    }
    memOverhead += newEntryMemOverhead;
    stats.memOverhead.incr(newEntryMemOverhead);
    assert(stats.memOverhead.get() < GIGANTOR);
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
    if (checkpointList.size() == 0) {
        return 0;
    }

    uint64_t id = checkpointList.back()->getId();
    return checkpointList.back()->getState() == opened ? id : id + 1;
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
    if (checkpointList.size() > 0) {
        checkpointList.back()->setId(id);
        // Update the checkpoint_start item with the new Id.
        queued_item qi = createCheckpointItem(id, vbucketId, queue_op_checkpoint_start);
        std::list<queued_item>::iterator it = ++(checkpointList.back()->begin());
        *it = qi;
    }
}

bool CheckpointManager::addNewCheckpoint_UNLOCKED(uint64_t id) {
    // This is just for making sure that the current checkpoint should be closed.
    if (checkpointList.size() > 0 && checkpointList.back()->getState() == opened) {
        closeOpenCheckpoint_UNLOCKED(checkpointList.back()->getId());
    }

    Checkpoint *checkpoint = new Checkpoint(stats, id, opened);
    // Add a dummy item into the new checkpoint, so that any cursor referring to the actual first
    // item in this new checkpoint can be safely shifted left by 1 if the first item is removed
    // and pushed into the tail.
    queued_item dummyItem(new QueuedItem("", 0xffff, queue_op_empty));
    checkpoint->queueDirty(dummyItem, this);

    // This item represents the start of the new checkpoint and is also sent to the slave node.
    queued_item qi = createCheckpointItem(id, vbucketId, queue_op_checkpoint_start);
    checkpoint->queueDirty(qi, this);
    ++numItems;
    checkpointList.push_back(checkpoint);
    return true;
}

bool CheckpointManager::addNewCheckpoint(uint64_t id) {
    LockHolder lh(queueLock);
    return addNewCheckpoint_UNLOCKED(id);
}

bool CheckpointManager::closeOpenCheckpoint_UNLOCKED(uint64_t id) {
    if (checkpointList.size() == 0) {
        return false;
    }
    if (id != checkpointList.back()->getId() || checkpointList.back()->getState() == closed) {
        return true;
    }

    // This item represents the end of the current open checkpoint and is sent to the slave node.
    queued_item qi = createCheckpointItem(id, vbucketId, queue_op_checkpoint_end);
    checkpointList.back()->queueDirty(qi, this);
    ++numItems;
    checkpointList.back()->setState(closed);
    return true;
}

bool CheckpointManager::closeOpenCheckpoint(uint64_t id) {
    LockHolder lh(queueLock);
    return closeOpenCheckpoint_UNLOCKED(id);
}

void CheckpointManager::registerPersistenceCursor() {
    LockHolder lh(queueLock);
    assert(checkpointList.size() > 0);
    persistenceCursor.currentCheckpoint = checkpointList.begin();
    persistenceCursor.currentPos = checkpointList.front()->begin();
    checkpointList.front()->registerCursorName(persistenceCursor.name);
}

protocol_binary_response_status CheckpointManager::startOnlineUpdate() {
    LockHolder lh(queueLock);
    assert(checkpointList.size() > 0);

    if (doOnlineUpdate) {
        return PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED;
    }

    // close the current checkpoint and create a new checkpoint
    checkOpenCheckpoint_UNLOCKED(true, true);

    // This item represents the start of online update and is also sent to the slave node..
    queued_item dummyItem(new QueuedItem("", vbucketId, queue_op_online_update_start));
    checkpointList.back()->queueDirty(dummyItem, this);
    ++numItems;

    onlineUpdateCursor.currentCheckpoint = checkpointList.end();
    --(onlineUpdateCursor.currentCheckpoint);

    onlineUpdateCursor.currentPos = checkpointList.back()->begin();
    (*(onlineUpdateCursor.currentCheckpoint))->registerCursorName(onlineUpdateCursor.name);

    doOnlineUpdate = true;
    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

protocol_binary_response_status CheckpointManager::stopOnlineUpdate() {
    LockHolder lh(queueLock);

    if ( !doOnlineUpdate ) {
        return PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED;
    }

    // This item represents the end of online update and is also sent to the slave node..
    queued_item dummyItem(new QueuedItem("", vbucketId, queue_op_online_update_end));
    checkpointList.back()->queueDirty(dummyItem, this);
    ++numItems;

    //close the current checkpoint and create a new checkpoint
    checkOpenCheckpoint_UNLOCKED(true, true);

    (*(onlineUpdateCursor.currentCheckpoint))->removeCursorName(onlineUpdateCursor.name);

    // Adjust for onlineupdate start and end items
    numItems -= 2;
    decrPersistenceCursorOffset(2);
    std::map<const std::string, CheckpointCursor>::iterator map_it = tapCursors.begin();
    for (; map_it != tapCursors.end(); ++map_it) {
        map_it->second.offset -= 2;
    }

    doOnlineUpdate = false;
    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

protocol_binary_response_status CheckpointManager::beginHotReload() {
    LockHolder lh(queueLock);

    if (!doOnlineUpdate) {
         getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                          "Not in online update phase, just return.");
         return PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED;
    }
    if (doHotReload) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "In online update revert phase already, just return.");
        return PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED;
    }

    doHotReload = true;

    assert(persistenceCursor.currentCheckpoint == onlineUpdateCursor.currentCheckpoint);

    // This item represents the end of online update and is also sent to the slave node..
    queued_item dummyItem(new QueuedItem("", vbucketId, queue_op_online_update_revert));
    checkpointList.back()->queueDirty(dummyItem, this);
    ++numItems;

    //close the current checkpoint and create a new checkpoint
    checkOpenCheckpoint_UNLOCKED(true, true);

    //Update persistence cursor due to hotReload
    (*(persistenceCursor.currentCheckpoint))->removeCursorName(persistenceCursor.name);
    persistenceCursor.currentCheckpoint = --(checkpointList.end());
    persistenceCursor.currentPos = checkpointList.back()->begin();

    (*(persistenceCursor.currentCheckpoint))->registerCursorName(persistenceCursor.name);

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

protocol_binary_response_status CheckpointManager::endHotReload(uint64_t total)  {
    LockHolder lh(queueLock);

    if (!doHotReload) {
        return PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED;
    }

    persistenceCursor.offset += total-1;    // Should ignore the first dummy item

    (*(onlineUpdateCursor.currentCheckpoint))->removeCursorName(onlineUpdateCursor.name);
    doHotReload = false;
    doOnlineUpdate = false;

    // Adjust for onlineupdate start and end items
    numItems -= 2;
    decrPersistenceCursorOffset(2);
    std::map<const std::string, CheckpointCursor>::iterator map_it = tapCursors.begin();
    for (; map_it != tapCursors.end(); ++map_it) {
        map_it->second.offset -= 2;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

bool CheckpointManager::registerTAPCursor(const std::string &name, uint64_t checkpointId,
                                          bool closedCheckpointOnly, bool alwaysFromBeginning) {
    LockHolder lh(queueLock);
    assert(checkpointList.size() > 0);

    bool found = false;
    std::list<Checkpoint*>::iterator it = checkpointList.begin();
    for (; it != checkpointList.end(); it++) {
        if (checkpointId == (*it)->getId()) {
            found = true;
            break;
        }
    }

    // Get the current open_checkpoint_id. The cursor that grabs items from closed checkpoints
    // only walks the checkpoint datastructure until it reaches to the beginning of the
    // checkpoint with open_checkpoint_id. One of the typical use cases is the cursor for the
    // incremental backup client.
    uint64_t open_chk_id = getOpenCheckpointId_UNLOCKED();

    // If the tap cursor exists, remove its name from the checkpoint that is
    // currently referenced by the tap cursor.
    std::map<const std::string, CheckpointCursor>::iterator map_it = tapCursors.find(name);
    if (map_it != tapCursors.end()) {
        (*(map_it->second.currentCheckpoint))->removeCursorName(name);
    }

    if (!found) {
        // If the checkpoint to start with is not found, set the TAP cursor to the current
        // open checkpoint. This case requires the full materialization through backfill.
        it = --(checkpointList.end());
        CheckpointCursor cursor(name, it, (*it)->begin(),
                            numItems - ((*it)->getNumItems() + 1), // 1 is for checkpoint start item
                            closedCheckpointOnly, open_chk_id);
        tapCursors[name] = cursor;
        (*it)->registerCursorName(name);
    } else {
        size_t offset = 0;
        std::list<queued_item>::iterator curr;
        if (!alwaysFromBeginning &&
            map_it != tapCursors.end() &&
            (*(map_it->second.currentCheckpoint))->getId() == (*it)->getId()) {
            // If the cursor is currently in the checkpoint to start with, simply start from
            // its current position.
            curr = map_it->second.currentPos;
            offset = map_it->second.offset;
        } else {
            // Set the cursor's position to the begining of the checkpoint to start with
            curr = (*it)->begin();
            std::list<Checkpoint*>::iterator pos = checkpointList.begin();
            for (; pos != it; ++pos) {
                offset += (*pos)->getNumItems() + 2; // 2 is for checkpoint start and end items.
            }
        }

        CheckpointCursor cursor(name, it, curr, offset, closedCheckpointOnly, open_chk_id);
        tapCursors[name] = cursor;
        // Register the tap cursor's name to the checkpoint.
        (*it)->registerCursorName(name);
    }

    return found;
}

bool CheckpointManager::removeTAPCursor(const std::string &name) {
    LockHolder lh(queueLock);
    std::map<const std::string, CheckpointCursor>::iterator it = tapCursors.find(name);
    if (it == tapCursors.end()) {
        return false;
    }

    // We can simply remove the cursor's name from the checkpoint to which it currently belongs,
    // by calling
    // (*(it->second.currentCheckpoint))->removeCursorName(name);
    // However, we just want to do more sanity checks by looking at each checkpoint. This won't
    // cause much overhead because the max number of checkpoints allowed per vbucket is small.
    std::list<Checkpoint*>::iterator cit = checkpointList.begin();
    for (; cit != checkpointList.end(); cit++) {
        (*cit)->removeCursorName(name);
    }

    tapCursors.erase(it);
    return true;
}

uint64_t CheckpointManager::getCheckpointIdForTAPCursor(const std::string &name) {
    LockHolder lh(queueLock);
    std::map<const std::string, CheckpointCursor>::iterator it = tapCursors.find(name);
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
    std::map<const std::string, CheckpointCursor>::iterator tap_it = tapCursors.begin();
        for (; tap_it != tapCursors.end(); ++tap_it) {
        cursor_names.push_back((tap_it->first));
    }
    return cursor_names;
}

bool CheckpointManager::isCheckpointCreationForHighMemUsage(const RCPtr<VBucket> &vbucket) {
    bool forceCreation = false;
    double current = static_cast<double>(stats.currentSize.get() + stats.memOverhead.get());
    // pesistence and tap cursors are all currently in the open checkpoint?
    bool allCursorsInOpenCheckpoint =
        (1 + tapCursors.size()) == checkpointList.back()->getNumberOfCursors() ? true : false;

    if (current > stats.mem_high_wat &&
        allCursorsInOpenCheckpoint &&
        (checkpointList.back()->getNumItems() >= MIN_CHECKPOINT_ITEMS ||
         checkpointList.back()->getNumItems() == vbucket->ht.getNumItems())) {
        forceCreation = true;
    }
    return forceCreation;
}

size_t CheckpointManager::removeClosedUnrefCheckpoints(const RCPtr<VBucket> &vbucket,
                                                       bool &newOpenCheckpointCreated) {

    // This function is executed periodically by the non-IO dispatcher.
    LockHolder lh(queueLock);
    assert(vbucket);
    uint64_t oldCheckpointId = 0;
    bool canCreateNewCheckpoint = false;
    if (checkpointList.size() < checkpointConfig.getMaxCheckpoints() ||
        (checkpointList.size() == checkpointConfig.getMaxCheckpoints() &&
         checkpointList.front()->getNumberOfCursors() == 0)) {
        canCreateNewCheckpoint = true;
    }
    if (vbucket->getState() == vbucket_state_active &&
        !checkpointConfig.isInconsistentSlaveCheckpoint() &&
        canCreateNewCheckpoint) {

        bool forceCreation = isCheckpointCreationForHighMemUsage(vbucket);
        // Check if this master active vbucket needs to create a new open checkpoint.
        oldCheckpointId = checkOpenCheckpoint_UNLOCKED(forceCreation, true);
    }
    newOpenCheckpointCreated = oldCheckpointId > 0 ? true : false;
    if (oldCheckpointId > 0) {
        // If the persistence cursor reached to the end of the old open checkpoint, move it to
        // the new open checkpoint.
        if ((*(persistenceCursor.currentCheckpoint))->getId() == oldCheckpointId) {
            if (++(persistenceCursor.currentPos) ==
                (*(persistenceCursor.currentCheckpoint))->end()) {
                moveCursorToNextCheckpoint(persistenceCursor);
            } else {
                --(persistenceCursor.currentPos);
            }
        }
        // If any of TAP cursors reached to the end of the old open checkpoint, move them to
        // the new open checkpoint.
        std::map<const std::string, CheckpointCursor>::iterator tap_it = tapCursors.begin();
        for (; tap_it != tapCursors.end(); ++tap_it) {
            CheckpointCursor &cursor = tap_it->second;
            if ((*(cursor.currentCheckpoint))->getId() == oldCheckpointId) {
                if (++(cursor.currentPos) == (*(cursor.currentCheckpoint))->end()) {
                    moveCursorToNextCheckpoint(cursor);
                } else {
                    --(cursor.currentPos);
                }
            }
        }
    }

    if (checkpointConfig.canKeepClosedCheckpoints()) {
        double current = static_cast<double>(stats.currentSize.get() + stats.memOverhead.get());
        if (current < stats.mem_high_wat &&
            checkpointList.size() <= checkpointConfig.getMaxCheckpoints()) {
            return 0;
        }
    }

    size_t numUnrefItems = 0;
    size_t numCheckpointsRemoved = 0;
    std::list<Checkpoint*> unrefCheckpointList;
    std::list<Checkpoint*>::iterator it = checkpointList.begin();
    for (; it != checkpointList.end(); it++) {
        removeInvalidCursorsOnCheckpoint(*it);
        if ((*it)->getNumberOfCursors() > 0) {
            break;
        } else {
            numUnrefItems += (*it)->getNumItems() + 2; // 2 is for checkpoint start and end items.
            ++numCheckpointsRemoved;
            if (checkpointConfig.canKeepClosedCheckpoints() &&
                (checkpointList.size() - numCheckpointsRemoved) <=
                 checkpointConfig.getMaxCheckpoints()) {
                // Collect unreferenced closed checkpoints until the number of checkpoints is
                // equal to the number of max checkpoints allowed.
                ++it;
                break;
            }
        }
    }
    if (numUnrefItems > 0) {
        numItems -= numUnrefItems;
        decrPersistenceCursorOffset(numUnrefItems);
        std::map<const std::string, CheckpointCursor>::iterator map_it = tapCursors.begin();
        for (; map_it != tapCursors.end(); ++map_it) {
            map_it->second.offset -= numUnrefItems;
        }
    }
    unrefCheckpointList.splice(unrefCheckpointList.begin(), checkpointList,
                               checkpointList.begin(), it);
    // If any cursor on a replica vbucket or downstream active vbucket receiving checkpoints from
    // the upstream master is very slow and causes more closed checkpoints in memory,
    // collapse those closed checkpoints into a single one to reduce the memory overhead.
    if (!checkpointConfig.canKeepClosedCheckpoints() &&
        (vbucket->getState() == vbucket_state_replica ||
         (vbucket->getState() == vbucket_state_active &&
          checkpointConfig.isInconsistentSlaveCheckpoint()))) {
        collapseClosedCheckpoints(unrefCheckpointList);
    }
    lh.unlock();

    std::list<Checkpoint*>::iterator chkpoint_it = unrefCheckpointList.begin();
    for (; chkpoint_it != unrefCheckpointList.end(); chkpoint_it++) {
        delete *chkpoint_it;
    }

    return numUnrefItems;
}

void CheckpointManager::removeInvalidCursorsOnCheckpoint(Checkpoint *pCheckpoint) {
    std::list<std::string> invalidCursorNames;
    const std::set<std::string> &cursors = pCheckpoint->getCursorNameList();
    std::set<std::string>::const_iterator cit = cursors.begin();
    for (; cit != cursors.end(); ++cit) {
        // Check it with persistence cursor
        if ((*cit).compare(persistenceCursor.name) == 0) {
            if (pCheckpoint != *(persistenceCursor.currentCheckpoint)) {
                invalidCursorNames.push_back(*cit);
            }
        } else if ((*cit).compare(onlineUpdateCursor.name) == 0) { // OnlineUpdate cursor
            if (pCheckpoint != *(onlineUpdateCursor.currentCheckpoint)) {
                invalidCursorNames.push_back(*cit);
            }
        } else { // Check it with tap cursors
            std::map<const std::string, CheckpointCursor>::iterator mit = tapCursors.find(*cit);
            if (mit == tapCursors.end() || pCheckpoint != *(mit->second.currentCheckpoint)) {
                invalidCursorNames.push_back(*cit);
            }
        }
    }

    std::list<std::string>::iterator it = invalidCursorNames.begin();
    for (; it != invalidCursorNames.end(); ++it) {
        pCheckpoint->removeCursorName(*it);
    }
}

void CheckpointManager::collapseClosedCheckpoints(std::list<Checkpoint*> &collapsedChks) {
    // If there are one open checkpoint and more than one closed checkpoint, collapse those
    // closed checkpoints into one checkpoint to reduce the memory overhead.
    if (checkpointList.size() > 2) {
        std::set<std::string> slowCursors;
        std::set<std::string> fastCursors;
        std::list<Checkpoint*>::iterator lastClosedChk = checkpointList.end();
        --lastClosedChk; --lastClosedChk; // Move to the lastest closed checkpoint.
        fastCursors.insert((*lastClosedChk)->getCursorNameList().begin(),
                           (*lastClosedChk)->getCursorNameList().end());
        std::list<Checkpoint*>::reverse_iterator rit = checkpointList.rbegin();
        ++rit; ++rit;// Move to the second lastest closed checkpoint.
        size_t numDuplicatedItems = 0, numMetaItems = 0;
        for (; rit != checkpointList.rend(); ++rit) {
            size_t numAddedItems = (*lastClosedChk)->mergePrevCheckpoint(*rit);
            numDuplicatedItems += ((*rit)->getNumItems() - numAddedItems);
            numMetaItems += 2; // checkpoint start and end meta items
            slowCursors.insert((*rit)->getCursorNameList().begin(),
                              (*rit)->getCursorNameList().end());
        }
        // Reposition the slow cursors to the beginning of the last closed checkpoint.
        std::set<std::string>::iterator sit = slowCursors.begin();
        for (; sit != slowCursors.end(); ++sit) {
            if ((*sit).compare(persistenceCursor.name) == 0) { // Reposition persistence cursor
                persistenceCursor.currentCheckpoint = lastClosedChk;
                persistenceCursor.currentPos =  (*lastClosedChk)->begin();
                persistenceCursor.offset = 0;
                (*lastClosedChk)->registerCursorName(persistenceCursor.name);
            } else if ((*sit).compare(onlineUpdateCursor.name) == 0) { // onlineUpdate cursor
                onlineUpdateCursor.currentCheckpoint = lastClosedChk;
                onlineUpdateCursor.currentPos =  (*lastClosedChk)->begin();
                onlineUpdateCursor.offset = 0;
                (*lastClosedChk)->registerCursorName(onlineUpdateCursor.name);
            } else { // Reposition tap cursors
                std::map<const std::string, CheckpointCursor>::iterator mit = tapCursors.find(*sit);
                if (mit != tapCursors.end()) {
                    mit->second.currentCheckpoint = lastClosedChk;
                    mit->second.currentPos =  (*lastClosedChk)->begin();
                    mit->second.offset = 0;
                    (*lastClosedChk)->registerCursorName(mit->second.name);
                }
            }
        }

        numItems -= (numDuplicatedItems + numMetaItems);
        Checkpoint *pOpenCheckpoint = checkpointList.back();
        const std::set<std::string> &openCheckpointCursors = pOpenCheckpoint->getCursorNameList();
        fastCursors.insert(openCheckpointCursors.begin(), openCheckpointCursors.end());
        std::set<std::string>::const_iterator cit = fastCursors.begin();
        // Update the offset of each fast cursor.
        for (; cit != fastCursors.end(); ++cit) {
            if ((*cit).compare(persistenceCursor.name) == 0) {
                decrPersistenceCursorOffset(numDuplicatedItems + numMetaItems);
            } else if ((*cit).compare(onlineUpdateCursor.name) == 0) {
                onlineUpdateCursor.offset -= (numDuplicatedItems + numMetaItems);
            } else {
                std::map<const std::string, CheckpointCursor>::iterator mit = tapCursors.find(*cit);
                if (mit != tapCursors.end()) {
                    mit->second.offset -= (numDuplicatedItems + numMetaItems);
                }
            }
        }
        collapsedChks.splice(collapsedChks.end(), checkpointList,
                             checkpointList.begin(),  lastClosedChk);
    }
}

bool CheckpointManager::queueDirty(const queued_item &qi, const RCPtr<VBucket> &vbucket) {
    LockHolder lh(queueLock);
    if (vbucket->getState() != vbucket_state_active &&
        checkpointList.back()->getState() == closed) {
        // Replica vbucket might receive items from the master even if the current open checkpoint
        // has been already closed, because some items from the backfill with an invalid token
        // are on the wire even after that backfill thread is closed. Simply ignore those items.
        return false;
    }

    // The current open checkpoint should be always the last one in the checkpoint list.
    assert(checkpointList.back()->getState() == opened);
    size_t numItemsBefore = getNumItemsForPersistence_UNLOCKED();
    if (checkpointList.back()->queueDirty(qi, this) == NEW_ITEM) {
        ++numItems;
    }
    size_t numItemsAfter = getNumItemsForPersistence_UNLOCKED();

    assert(vbucket);
    bool canCreateNewCheckpoint = false;
    if (checkpointList.size() < checkpointConfig.getMaxCheckpoints() ||
        (checkpointList.size() == checkpointConfig.getMaxCheckpoints() &&
         checkpointList.front()->getNumberOfCursors() == 0)) {
        canCreateNewCheckpoint = true;
    }
    if (vbucket->getState() == vbucket_state_active &&
        !checkpointConfig.isInconsistentSlaveCheckpoint() &&
        canCreateNewCheckpoint) {
        // Only the master active vbucket can create a next open checkpoint.
        checkOpenCheckpoint_UNLOCKED(false, true);
    }
    // Note that the creation of a new checkpoint on the replica vbucket will be controlled by TAP
    // mutation messages from the active vbucket, which contain the checkpoint Ids.

    return (numItemsAfter - numItemsBefore) > 0 ? true : false;
}

uint64_t CheckpointManager::getAllItemsFromCurrentPosition(CheckpointCursor &cursor,
                                                           uint64_t barrier,
                                                           std::vector<queued_item> &items) {
    while (true) {
        if ( barrier > 0 )  {
            if ((*(cursor.currentCheckpoint))->getId() >= barrier) {
                break;
            }
        }
        while (++(cursor.currentPos) != (*(cursor.currentCheckpoint))->end()) {
            items.push_back(*(cursor.currentPos));
        }
        if ((*(cursor.currentCheckpoint))->getState() == closed) {
            if (!moveCursorToNextCheckpoint(cursor)) {
                --(cursor.currentPos);
                break;
            }
        } else { // The cursor is currently in the open checkpoint and reached to
                 // the end() of the open checkpoint.
            --(cursor.currentPos);
            break;
        }
    }

    uint64_t checkpointId = 0;
    // Get the last closed checkpoint Id.
    if(checkpointList.size() > 0) {
        uint64_t id = (*(cursor.currentCheckpoint))->getId();
        checkpointId = id > 0 ? id - 1 : 0;
    }

    return checkpointId;
}

uint64_t CheckpointManager::getAllItemsForPersistence(std::vector<queued_item> &items) {
    LockHolder lh(queueLock);
    uint64_t checkpointId;
    if (doOnlineUpdate) {
        // Get all the items up to the start of the onlineUpdate cursor.
        uint64_t barrier = (*(onlineUpdateCursor.currentCheckpoint))->getId();
        checkpointId = getAllItemsFromCurrentPosition(persistenceCursor, barrier, items);
        persistenceCursor.offset += items.size();
    } else {
        // Get all the items up to the end of the current open checkpoint.
        checkpointId = getAllItemsFromCurrentPosition(persistenceCursor, 0, items);
        persistenceCursor.offset = numItems;
    }
    return checkpointId;
}

uint64_t CheckpointManager::getAllItemsForTAPConnection(const std::string &name,
                                                    std::vector<queued_item> &items) {
    LockHolder lh(queueLock);
    std::map<const std::string, CheckpointCursor>::iterator it = tapCursors.find(name);
    if (it == tapCursors.end()) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "The cursor for TAP connection \"%s\" is not found in the checkpoint.\n",
                         name.c_str());
        return 0;
    }
    uint64_t checkpointId = getAllItemsFromCurrentPosition(it->second, 0, items);
    it->second.offset = numItems;
    return checkpointId;
}

uint64_t CheckpointManager::getAllItemsForOnlineUpdate(std::vector<queued_item> &items) {
    LockHolder lh(queueLock);
    uint64_t checkpointId = 0;
    if (doOnlineUpdate) {
        // Get all the items up to the end of the current open checkpoint
        checkpointId = getAllItemsFromCurrentPosition(onlineUpdateCursor, 0, items);
        onlineUpdateCursor.offset += items.size();
    }

    return checkpointId;
}

queued_item CheckpointManager::nextItem(const std::string &name, bool &isLastMutationItem) {
    LockHolder lh(queueLock);
    isLastMutationItem = false;
    std::map<const std::string, CheckpointCursor>::iterator it = tapCursors.find(name);
    if (it == tapCursors.end()) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "The cursor for TAP connection \"%s\" is not found in the checkpoint.\n",
                         name.c_str());
        queued_item qi(new QueuedItem("", 0xffff, queue_op_empty));
        return qi;
    }
    if (checkpointList.back()->getId() == 0) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "VBucket %d CheckpointManager: Wait for backfill completion...\n",
                         vbucketId);
        queued_item qi(new QueuedItem("", 0xffff, queue_op_empty));
        return qi;
    }

    CheckpointCursor &cursor = it->second;
    if ((*(it->second.currentCheckpoint))->getState() == closed) {
        return nextItemFromClosedCheckpoint(cursor, isLastMutationItem);
    } else {
        return nextItemFromOpenedCheckpoint(cursor, isLastMutationItem);
    }
}

queued_item CheckpointManager::nextItemFromClosedCheckpoint(CheckpointCursor &cursor,
                                                            bool &isLastMutationItem) {
    // The cursor already reached to the beginning of the checkpoint that had "open" state
    // when registered. Simply return an empty item so that the corresponding TAP client
    // can close the connection.
    if (cursor.closedCheckpointOnly &&
        cursor.openChkIdAtRegistration <= (*(cursor.currentCheckpoint))->getId()) {
        queued_item qi(new QueuedItem("", vbucketId, queue_op_empty));
        return qi;
    }

    ++(cursor.currentPos);
    if (cursor.currentPos != (*(cursor.currentCheckpoint))->end()) {
        ++(cursor.offset);
        isLastMutationItem = isLastMutationItemInCheckpoint(cursor);
        return *(cursor.currentPos);
    } else {
        if (!moveCursorToNextCheckpoint(cursor)) {
            --(cursor.currentPos);
            queued_item qi(new QueuedItem("", 0xffff, queue_op_empty));
            return qi;
        }
        if ((*(cursor.currentCheckpoint))->getState() == closed) { // the close checkpoint.
            ++(cursor.currentPos); // Move the cursor to point to the actual first item.
            ++(cursor.offset);
            isLastMutationItem = isLastMutationItemInCheckpoint(cursor);
            return *(cursor.currentPos);
        } else { // the open checkpoint.
            return nextItemFromOpenedCheckpoint(cursor, isLastMutationItem);
        }
    }
}

queued_item CheckpointManager::nextItemFromOpenedCheckpoint(CheckpointCursor &cursor,
                                                            bool &isLastMutationItem) {
    if (cursor.closedCheckpointOnly) {
        queued_item qi(new QueuedItem("", vbucketId, queue_op_empty));
        return qi;
    }

    ++(cursor.currentPos);
    if (cursor.currentPos != (*(cursor.currentCheckpoint))->end()) {
        ++(cursor.offset);
        isLastMutationItem = isLastMutationItemInCheckpoint(cursor);
        return *(cursor.currentPos);
    } else {
        --(cursor.currentPos);
        queued_item qi(new QueuedItem("", 0xffff, queue_op_empty));
        return qi;
    }
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
    mutationCounter = 0;

    uint64_t checkpointId = vbState == vbucket_state_active ? 1 : 0;
    // Add a new open checkpoint.
    addNewCheckpoint_UNLOCKED(checkpointId);
    resetCursors();
}

void CheckpointManager::resetCursors() {
    // Reset the persistence cursor.
    persistenceCursor.currentCheckpoint = checkpointList.begin();
    persistenceCursor.currentPos = checkpointList.front()->begin();
    persistenceCursor.offset = 0;
    checkpointList.front()->registerCursorName(persistenceCursor.name);

    // Reset all the TAP cursors.
    std::map<const std::string, CheckpointCursor>::iterator cit = tapCursors.begin();
    for (; cit != tapCursors.end(); ++cit) {
        cit->second.currentCheckpoint = checkpointList.begin();
        cit->second.currentPos = checkpointList.front()->begin();
        cit->second.offset = 0;
        checkpointList.front()->registerCursorName(cit->second.name);
    }
}

bool CheckpointManager::moveCursorToNextCheckpoint(CheckpointCursor &cursor) {
    if ((*(cursor.currentCheckpoint))->getState() == opened) {
        return false;
    } else if ((*(cursor.currentCheckpoint))->getState() == closed) {
        std::list<Checkpoint*>::iterator currCheckpoint = cursor.currentCheckpoint;
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

uint64_t CheckpointManager::checkOpenCheckpoint_UNLOCKED(bool forceCreation, bool timeBound) {
    int checkpointId = 0;
    timeBound = timeBound &&
                (ep_real_time() - checkpointList.back()->getCreationTime()) >=
                checkpointConfig.getCheckpointPeriod();
    // Create the new open checkpoint if any of the following conditions is satisfied:
    // (1) force creation due to online update or high memory usage
    // (2) current checkpoint is reached to the max number of items allowed.
    // (3) time elapsed since the creation of the current checkpoint is greater than the threshold
    if (forceCreation ||
        (checkpointConfig.isItemNumBasedNewCheckpoint() &&
         checkpointList.back()->getNumItems() >= checkpointConfig.getCheckpointMaxItems()) ||
        (checkpointList.back()->getNumItems() > 0 && timeBound)) {

        checkpointId = checkpointList.back()->getId();
        closeOpenCheckpoint_UNLOCKED(checkpointId);
        addNewCheckpoint_UNLOCKED(checkpointId + 1);
    }
    return checkpointId;
}

bool CheckpointManager::isKeyResidentInCheckpoints(const std::string &key, uint64_t cas) {
    LockHolder lh(queueLock);

    std::list<Checkpoint*>::iterator it = checkpointList.begin();
    // Find the first checkpoint that is referenced by any cursor.
    for (; it != checkpointList.end(); ++it) {
        if ((*it)->getNumberOfCursors() > 0) {
            break;
        }
    }

    uint64_t cas_from_checkpoint;
    bool found = false;
    // Check if a given key with its CAS value exists in any subsequent checkpoints.
    for (; it != checkpointList.end(); ++it) {
        cas_from_checkpoint = (*it)->getCasForKey(key);
        if (cas == cas_from_checkpoint) {
            found = true;
            break;
        } else if (cas < cas_from_checkpoint) {
            break; // if a key's CAS value is less than the one from the checkpoint, we do not
                   // have to look at any following checkpoints.
        }
    }

    lh.unlock();
    return found;
}

size_t CheckpointManager::getNumItemsForTAPConnection(const std::string &name) {
    LockHolder lh(queueLock);
    size_t remains = 0;
    std::map<const std::string, CheckpointCursor>::iterator it = tapCursors.find(name);
    if (it != tapCursors.end()) {
        remains = (numItems >= it->second.offset) ? numItems - it->second.offset : 0;
    }
    return remains;
}

void CheckpointManager::decrTapCursorFromCheckpointEnd(const std::string &name) {
    LockHolder lh(queueLock);
    std::map<const std::string, CheckpointCursor>::iterator it = tapCursors.find(name);
    if (it != tapCursors.end() &&
        (*(it->second.currentPos))->getOperation() == queue_op_checkpoint_end) {
        --(it->second.offset);
        --(it->second.currentPos);
    }
}

bool CheckpointManager::isLastMutationItemInCheckpoint(CheckpointCursor &cursor) {
    std::list<queued_item>::iterator it = cursor.currentPos;
    ++it;
    if (it == (*(cursor.currentCheckpoint))->end() ||
        (*it)->getOperation() == queue_op_checkpoint_end) {
        return true;
    }
    return false;
}

bool CheckpointManager::checkAndAddNewCheckpoint(uint64_t id, bool &pCursorRepositioned) {
    LockHolder lh(queueLock);

    // Ignore CHECKPOINT_START message with ID 0 as 0 is reserved for representing backfill.
    if (id == 0) {
        pCursorRepositioned = false;
        return true;
    }
    // If the replica receives a checkpoint start message right after backfill completion,
    // simply set the current open checkpoint id to the one received from the active vbucket.
    if (checkpointList.back()->getId() == 0) {
        setOpenCheckpointId_UNLOCKED(id);
        pCursorRepositioned = id > 0 ? true : false;
        return true;
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
        pCursorRepositioned = false;
        if (checkpointList.back()->getState() == opened &&
            checkpointList.back()->getNumItems() == 0) {
            // If the current open checkpoint doesn't have any items, simply set its id to
            // the one from the master node.
            setOpenCheckpointId_UNLOCKED(id);
            // Reposition all the cursors in the open checkpoint to the begining position
            // so that a checkpoint_start message can be sent again with the correct id.
            const std::set<std::string> &cursors = checkpointList.back()->getCursorNameList();
            std::set<std::string>::const_iterator cit = cursors.begin();
            for (; cit != cursors.end(); ++cit) {
                if ((*cit).compare(persistenceCursor.name) == 0) { // Persistence cursor
                    persistenceCursor.currentPos = checkpointList.back()->begin();
                } else if ((*cit).compare(onlineUpdateCursor.name) == 0) { // OnlineUpdate cursor
                    onlineUpdateCursor.currentPos = checkpointList.back()->begin();
                } else { // TAP cursors
                    std::map<const std::string, CheckpointCursor>::iterator mit =
                        tapCursors.find(*cit);
                    mit->second.currentPos = checkpointList.back()->begin();
                }
            }
            return true;
        } else {
            closeOpenCheckpoint_UNLOCKED(checkpointList.back()->getId());
            return addNewCheckpoint_UNLOCKED(id);
        }
    } else {
        assert(checkpointList.size() > 0);
        std::list<Checkpoint*>::reverse_iterator rit = checkpointList.rbegin();
        ++rit; // Move to the last closed checkpoint.
        size_t numDuplicatedItems = 0, numMetaItems = 0;
        // Collapse all checkpoints.
        for (; rit != checkpointList.rend(); ++rit) {
            size_t numAddedItems = checkpointList.back()->mergePrevCheckpoint(*rit);
            numDuplicatedItems += ((*rit)->getNumItems() - numAddedItems);
            numMetaItems += 2; // checkpoint start and end meta items
            delete *rit;
        }
        numItems -= (numDuplicatedItems + numMetaItems);

        if (checkpointList.size() > 1) {
            checkpointList.erase(checkpointList.begin(), --checkpointList.end());
        }
        assert(checkpointList.size() == 1);

        if (checkpointList.back()->getState() == closed) {
            checkpointList.back()->popBackCheckpointEndItem();
            --numItems;
            checkpointList.back()->setState(opened);
        }
        setOpenCheckpointId_UNLOCKED(id);
        resetCursors();

        pCursorRepositioned = true;
        return true;
    }
}

bool CheckpointManager::hasNext(const std::string &name) {
    LockHolder lh(queueLock);
    std::map<const std::string, CheckpointCursor>::iterator it = tapCursors.find(name);
    if (it == tapCursors.end() || getOpenCheckpointId_UNLOCKED() == 0) {
        return false;
    }

    bool hasMore = true;
    std::list<queued_item>::iterator curr = it->second.currentPos;
    ++curr;
    if (curr == (*(it->second.currentCheckpoint))->end() &&
        (*(it->second.currentCheckpoint))->getState() == opened) {
        hasMore = false;
    }
    return hasMore;
}

queued_item CheckpointManager::createCheckpointItem(uint64_t id,
                                                    uint16_t vbid,
                                                    enum queue_operation checkpoint_op) {
    assert(checkpoint_op == queue_op_checkpoint_start || checkpoint_op == queue_op_checkpoint_end);
    uint64_t cid = htonll(id);
    RCPtr<Blob> vblob(Blob::New((const char*)&cid, sizeof(cid)));
    queued_item qi(new QueuedItem("", vblob, vbid, checkpoint_op));
    return qi;
}

bool CheckpointManager::hasNextForPersistence() {
    LockHolder lh(queueLock);
    bool hasMore = true;
    std::list<queued_item>::iterator curr = persistenceCursor.currentPos;
    ++curr;
    if (curr == (*(persistenceCursor.currentCheckpoint))->end() &&
        (*(persistenceCursor.currentCheckpoint))->getState() == opened) {
        hasMore = false;
    }
    return hasMore;
}

void CheckpointConfig::addConfigChangeListener(EventuallyPersistentEngine &engine) {
    Configuration &configuration = engine.getConfiguration();
    configuration.addValueChangedListener("chk_period",
                              new CheckpointConfigChangeListener(engine.getCheckpointConfig()));
    configuration.addValueChangedListener("chk_max_items",
                              new CheckpointConfigChangeListener(engine.getCheckpointConfig()));
    configuration.addValueChangedListener("max_checkpoints",
                              new CheckpointConfigChangeListener(engine.getCheckpointConfig()));
    configuration.addValueChangedListener("inconsistent_slave_chk",
                              new CheckpointConfigChangeListener(engine.getCheckpointConfig()));
    configuration.addValueChangedListener("item_num_based_new_chk",
                              new CheckpointConfigChangeListener(engine.getCheckpointConfig()));
    configuration.addValueChangedListener("keep_closed_chks",
                              new CheckpointConfigChangeListener(engine.getCheckpointConfig()));
    configuration.addValueChangedListener("chk_meta_items_only",
                              new CheckpointConfigChangeListener(engine.getCheckpointConfig()));
}

CheckpointConfig::CheckpointConfig(EventuallyPersistentEngine &e) {
    Configuration &config = e.getConfiguration();
    checkpointPeriod = config.getChkPeriod();
    checkpointMaxItems = config.getChkMaxItems();
    maxCheckpoints = config.getMaxCheckpoints();
    inconsistentSlaveCheckpoint = config.isInconsistentSlaveChk();
    itemNumBasedNewCheckpoint = config.isItemNumBasedNewChk();
    keepClosedCheckpoints = config.isKeepClosedChks();
    metaItemsOnly = config.isChkMetaItemsOnly();
}

bool CheckpointConfig::validateCheckpointMaxItemsParam(size_t checkpoint_max_items) {
    if (checkpoint_max_items < MIN_CHECKPOINT_ITEMS ||
        checkpoint_max_items > MAX_CHECKPOINT_ITEMS) {
        std::stringstream ss;
        ss << "New checkpoint_max_items param value " << checkpoint_max_items
           << " is not ranged between the min allowed value " << MIN_CHECKPOINT_ITEMS
           << " and max value " << MAX_CHECKPOINT_ITEMS;
        getLogger()->log(EXTENSION_LOG_WARNING, NULL, ss.str().c_str());
        return false;
    }
    return true;
}

bool CheckpointConfig::validateCheckpointPeriodParam(size_t checkpoint_period) {
    if (checkpoint_period < MIN_CHECKPOINT_PERIOD ||
        checkpoint_period > MAX_CHECKPOINT_PERIOD) {
        std::stringstream ss;
        ss << "New checkpoint_period param value " << checkpoint_period
           << " is not ranged between the min allowed value " << MIN_CHECKPOINT_PERIOD
           << " and max value " << MAX_CHECKPOINT_PERIOD;
        getLogger()->log(EXTENSION_LOG_WARNING, NULL, ss.str().c_str());
        return false;
    }
    return true;
}

bool CheckpointConfig::validateMaxCheckpointsParam(size_t max_checkpoints) {
    if (max_checkpoints < DEFAULT_MAX_CHECKPOINTS ||
        max_checkpoints > MAX_CHECKPOINTS_UPPER_BOUND) {
        std::stringstream ss;
        ss << "New max_checkpoints param value " << max_checkpoints
           << " is not ranged between the min allowed value " << DEFAULT_MAX_CHECKPOINTS
           << " and max value " << MAX_CHECKPOINTS_UPPER_BOUND;
        getLogger()->log(EXTENSION_LOG_WARNING, NULL, ss.str().c_str());
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
