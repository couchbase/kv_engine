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
        }
    }

    virtual void booleanValueChanged(const std::string &key, bool value) {
        if (key.compare("inconsistent_slave_chk") == 0) {
            config.allowInconsistentSlaveCheckpoint(value);
        } else if (key.compare("item_num_based_new_chk") == 0) {
            config.allowItemNumBasedNewCheckpoint(value);
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
            std::string key = (*(checkpointManager->persistenceCursor.currentPos))->getKey();
            checkpoint_index::iterator ita = keyIndex.find(key);
            if (ita != keyIndex.end()) {
                uint64_t mutationId = ita->second.mutation_id;
                if (currMutationId <= mutationId) {
                    checkpointManager->decrPersistenceCursorOffset();
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
                std::string key = (*(map_it->second.currentPos))->getKey();
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
    checkpointList.front()->incrReferenceCounter();
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
    (*(onlineUpdateCursor.currentCheckpoint))->incrReferenceCounter();

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

    (*(onlineUpdateCursor.currentCheckpoint))->decrReferenceCounter();

    // Adjust for onlineupdate start and end items
    numItems -= 2;
    persistenceCursor.offset -= 2;
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
    (*(persistenceCursor.currentCheckpoint))->decrReferenceCounter();
    persistenceCursor.currentCheckpoint = --(checkpointList.end());
    persistenceCursor.currentPos = checkpointList.back()->begin();

    (*(persistenceCursor.currentCheckpoint))->incrReferenceCounter();

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

protocol_binary_response_status CheckpointManager::endHotReload(uint64_t total)  {
    LockHolder lh(queueLock);

    if (!doHotReload) {
        return PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED;
    }

    persistenceCursor.offset += total-1;    // Should ignore the first dummy item

    (*(onlineUpdateCursor.currentCheckpoint))->decrReferenceCounter();
    doHotReload = false;
    doOnlineUpdate = false;

    // Adjust for onlineupdate start and end items
    numItems -= 2;
    persistenceCursor.offset -= 2;
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

    // If the tap cursor exists, decrease the reference counter of the checkpoint that is
    // currently referenced by the tap cursor.
    std::map<const std::string, CheckpointCursor>::iterator map_it = tapCursors.find(name);
    if (map_it != tapCursors.end()) {
        (*(map_it->second.currentCheckpoint))->decrReferenceCounter();
    }

    if (!found) {
        // If the checkpoint to start with is not found, set the TAP cursor to the current
        // open checkpoint. This case requires the full materialization through backfill.
        it = --(checkpointList.end());
        CheckpointCursor cursor(it, (*it)->begin(),
                            numItems - ((*it)->getNumItems() + 1), // 1 is for checkpoint start item
                            closedCheckpointOnly);
        tapCursors[name] = cursor;
        (*it)->incrReferenceCounter();
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

        CheckpointCursor cursor(it, curr, offset, closedCheckpointOnly);
        tapCursors[name] = cursor;
        // Increase the reference counter of the checkpoint that is newly referenced
        // by the tap cursor.
        (*it)->incrReferenceCounter();
    }

    return found;
}

bool CheckpointManager::removeTAPCursor(const std::string &name) {
    LockHolder lh(queueLock);
    std::map<const std::string, CheckpointCursor>::iterator it = tapCursors.find(name);
    if (it == tapCursors.end()) {
        return false;
    }
    (*(it->second.currentCheckpoint))->decrReferenceCounter();

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

bool CheckpointManager::isCheckpointCreationForHighMemUsage(const RCPtr<VBucket> &vbucket) {
    bool forceCreation = false;
    double current = static_cast<double>(stats.currentSize.get() + stats.memOverhead.get());
    // pesistence and tap cursors are all currently in the open checkpoint?
    bool allCursorsInOpenCheckpoint =
        (1 + tapCursors.size()) == checkpointList.back()->getReferenceCounter() ? true : false;

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
    if (vbucket->getState() == vbucket_state_active &&
        !checkpointConfig.isInconsistentSlaveCheckpoint() &&
        checkpointList.size() < checkpointConfig.getMaxCheckpoints()) {

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

    size_t numUnrefItems = 0;
    std::list<Checkpoint*> unrefCheckpointList;
    std::list<Checkpoint*>::iterator it = checkpointList.begin();
    for (; it != checkpointList.end(); it++) {
        if ((*it)->getReferenceCounter() > 0) {
            break;
        } else {
            numUnrefItems += (*it)->getNumItems() + 2; // 2 is for checkpoint start and end items.
        }
    }
    if (numUnrefItems > 0) {
        numItems -= numUnrefItems;
        persistenceCursor.offset -= numUnrefItems;
        std::map<const std::string, CheckpointCursor>::iterator map_it = tapCursors.begin();
        for (; map_it != tapCursors.end(); ++map_it) {
            map_it->second.offset -= numUnrefItems;
        }
    }
    unrefCheckpointList.splice(unrefCheckpointList.begin(), checkpointList,
                               checkpointList.begin(), it);
    lh.unlock();

    std::list<Checkpoint*>::iterator chkpoint_it = unrefCheckpointList.begin();
    for (; chkpoint_it != unrefCheckpointList.end(); chkpoint_it++) {
        delete *chkpoint_it;
    }

    return numUnrefItems;
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
    if (vbucket->getState() == vbucket_state_active &&
        !checkpointConfig.isInconsistentSlaveCheckpoint() &&
        checkpointList.size() < checkpointConfig.getMaxCheckpoints()) {
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
    } else {
        // Get all the items up to the end of the current open checkpoint.
        checkpointId = getAllItemsFromCurrentPosition(persistenceCursor, 0, items);
    }
    persistenceCursor.offset += items.size();
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
    it->second.offset += items.size();
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

    // Reset the persistence cursor.
    persistenceCursor.currentCheckpoint = checkpointList.begin();
    persistenceCursor.currentPos = checkpointList.front()->begin();
    persistenceCursor.offset = 0;
    checkpointList.front()->incrReferenceCounter();

    // Reset all the TAP cursors.
    std::map<const std::string, CheckpointCursor>::iterator cit = tapCursors.begin();
    for (; cit != tapCursors.end(); ++cit) {
        cit->second.currentCheckpoint = checkpointList.begin();
        cit->second.currentPos = checkpointList.front()->begin();
        cit->second.offset = 0;
        checkpointList.front()->incrReferenceCounter();
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

    // decr the reference counter for the current checkpoint by 1.
    (*(cursor.currentCheckpoint))->decrReferenceCounter();
    // Move the cursor to the next checkpoint.
    ++(cursor.currentCheckpoint);
    cursor.currentPos = (*(cursor.currentCheckpoint))->begin();
    // incr the reference counter for the next checkpoint by 1.
    (*(cursor.currentCheckpoint))->incrReferenceCounter();
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
        if ((*it)->getReferenceCounter() > 0) {
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
        if (checkpointList.back()->getState() == opened) {
            closeOpenCheckpoint_UNLOCKED(checkpointList.back()->getId());
        }
        pCursorRepositioned = false;
        return addNewCheckpoint_UNLOCKED(id);
    } else {
        bool ret = true;
        bool persistenceCursorReposition = false;
        std::set<std::string> tapClients;
        std::list<Checkpoint*>::iterator curr = it;
        for (; curr != checkpointList.end(); ++curr) {
            if (*(persistenceCursor.currentCheckpoint) == *curr) {
                persistenceCursorReposition = true;
            }
            std::map<const std::string, CheckpointCursor>::iterator map_it = tapCursors.begin();
            for (; map_it != tapCursors.end(); ++map_it) {
                if (*(map_it->second.currentCheckpoint) == *curr) {
                    tapClients.insert(map_it->first);
                }
            }
            if ((*curr)->getState() == closed) {
                numItems -= ((*curr)->getNumItems() + 2); // 2 is for checkpoint start and end items.
            } else if ((*curr)->getState() == opened) {
                numItems -= ((*curr)->getNumItems() + 1); // 1 is for checkpoint start.
            }
            delete *curr;
        }
        checkpointList.erase(it, checkpointList.end());

        ret = addNewCheckpoint_UNLOCKED(id);
        if (ret) {
            if (checkpointList.back()->getState() == closed) {
                checkpointList.back()->popBackCheckpointEndItem();
                checkpointList.back()->setState(opened);
            }
            if (persistenceCursorReposition) {
                persistenceCursor.currentCheckpoint = --(checkpointList.end());
                persistenceCursor.currentPos = checkpointList.back()->begin();
                persistenceCursor.offset = numItems - 1;
                checkpointList.back()->incrReferenceCounter();
            }
            std::set<std::string>::iterator set_it = tapClients.begin();
            for (; set_it != tapClients.end(); ++set_it) {
                std::map<const std::string, CheckpointCursor>::iterator map_it =
                    tapCursors.find(*set_it);
                map_it->second.currentCheckpoint = --(checkpointList.end());
                map_it->second.currentPos = checkpointList.back()->begin();
                map_it->second.offset = numItems - 1;
                checkpointList.back()->incrReferenceCounter();
            }
        }

        pCursorRepositioned = persistenceCursorReposition;
        return ret;
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
}

CheckpointConfig::CheckpointConfig(EventuallyPersistentEngine &e) {
    Configuration &config = e.getConfiguration();
    checkpointPeriod = config.getChkPeriod();
    checkpointMaxItems = config.getChkMaxItems();
    maxCheckpoints = config.getMaxCheckpoints();
    inconsistentSlaveCheckpoint = config.isInconsistentSlaveChk();
    itemNumBasedNewCheckpoint = config.isItemNumBasedNewChk();
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
