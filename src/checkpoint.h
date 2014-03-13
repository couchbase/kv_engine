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

#ifndef SRC_CHECKPOINT_H_
#define SRC_CHECKPOINT_H_ 1

#include "config.h"

#include <assert.h>

#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "atomic.h"
#include "common.h"
#include "item.h"
#include "locks.h"
#include "stats.h"

#define MIN_CHECKPOINT_ITEMS 100
#define MAX_CHECKPOINT_ITEMS 500000
#define DEFAULT_CHECKPOINT_ITEMS 5000

#define MIN_CHECKPOINT_PERIOD 60 // 1 min.
#define MAX_CHECKPOINT_PERIOD 28800 // 8 hours.
#define DEFAULT_CHECKPOINT_PERIOD 1800 // 30 min.

#define DEFAULT_MAX_CHECKPOINTS 2
#define MAX_CHECKPOINTS_UPPER_BOUND 5

/**
 * The state of a given checkpoint.
 */
typedef enum {
    CHECKPOINT_OPEN, //!< The checkpoint is open.
    CHECKPOINT_CLOSED  //!< The checkpoint is not open.
} checkpoint_state;

/**
 * A checkpoint index entry.
 */
struct index_entry {
    std::list<queued_item>::iterator position;
    int64_t mutation_id;
};

/**
 * The checkpoint index maps a key to a checkpoint index_entry.
 */
typedef unordered_map<std::string, index_entry> checkpoint_index;

class Checkpoint;
class CheckpointManager;
class CheckpointConfig;
class VBucket;

/**
 * A checkpoint cursor
 */
class CheckpointCursor {
    friend class CheckpointManager;
    friend class Checkpoint;
public:

    CheckpointCursor(const std::string &n)
        : name(n),
          currentCheckpoint(),
          currentPos(),
          offset(0) { }

    CheckpointCursor(const std::string &n,
                     std::list<Checkpoint*>::iterator checkpoint,
                     std::list<queued_item>::iterator pos,
                     size_t os = 0) :
        name(n), currentCheckpoint(checkpoint), currentPos(pos), offset(os) { }

    // We need to define the copy construct explicitly due to the fact
    // that std::atomic implicitly deleted the assignment operator
    CheckpointCursor(const CheckpointCursor &other) :
        name(other.name), currentCheckpoint(other.currentCheckpoint),
        currentPos(other.currentPos), offset(other.offset.load()) { }

private:
    std::string                      name;
    std::list<Checkpoint*>::iterator currentCheckpoint;
    std::list<queued_item>::iterator currentPos;
    AtomicValue<size_t>              offset;
};

/**
 * The cursor index maps checkpoint cursor names to checkpoint cursors
 */
typedef std::map<const std::string, CheckpointCursor> cursor_index;

/**
 * Result from invoking queueDirty in the current open checkpoint.
 */
typedef enum {
    /*
     * The item exists on the right hand side of the persistence cursor. The
     * item will be deduplicated and doesn't change the size of the checkpoint.
     */
    EXISTING_ITEM,

    /**
     * The item exists on the left hand side of the persistence cursor. It will
     * be dedeuplicated and moved the to right hand side, but the item needs
     * to be re-persisted.
     */
    PERSIST_AGAIN,

    /**
     * The item doesn't exist yet in the checkpoint. Adding this item will
     * increase the size of the checkpoint.
     */
    NEW_ITEM
} queue_dirty_t;

/**
 * Representation of a checkpoint used in the unified queue for persistence and tap.
 */
class Checkpoint {
public:
    Checkpoint(EPStats &st, uint64_t id, uint16_t vbid,
               checkpoint_state state = CHECKPOINT_OPEN) :
        stats(st), checkpointId(id), vbucketId(vbid), creationTime(ep_real_time()),
        checkpointState(state), numItems(0), memOverhead(0) {
        stats.memOverhead.fetch_add(memorySize());
        assert(stats.memOverhead.load() < GIGANTOR);
    }

    ~Checkpoint();

    /**
     * Return the checkpoint Id
     */
    uint64_t getId() const {
        return checkpointId;
    }

    /**
     * Set the checkpoint Id
     * @param id the checkpoint Id to be set.
     */
    void setId(uint64_t id) {
        checkpointId = id;
    }

    /**
     * Return the creation timestamp of this checkpoint in sec.
     */
    rel_time_t getCreationTime() {
        return creationTime;
    }

    /**
     * Return the number of items belonging to this checkpoint.
     */
    size_t getNumItems() const {
        return numItems;
    }

    /**
     * Return the current state of this checkpoint.
     */
    checkpoint_state getState() const {
        return checkpointState;
    }

    /**
     * Set the current state of this checkpoint.
     * @param state the checkpoint's new state
     */
    void setState(checkpoint_state state);

    void popBackCheckpointEndItem();

    /**
     * Return the number of cursors that are currently walking through this checkpoint.
     */
    size_t getNumberOfCursors() const {
        return cursors.size();
    }

    /**
     * Register a cursor's name to this checkpoint
     */
    void registerCursorName(const std::string &name) {
        cursors.insert(name);
    }

    /**
     * Remove a cursor's name from this checkpoint
     */
    void removeCursorName(const std::string &name) {
        cursors.erase(name);
    }

    /**
     * Return true if the cursor with a given name exists in this checkpoint
     */
    bool hasCursorName(const std::string &name) const {
        return cursors.find(name) != cursors.end();
    }

    /**
     * Return the list of all cursor names in this checkpoint
     */
    const std::set<std::string> &getCursorNameList() const {
        return cursors;
    }

    /**
     * Queue an item to be written to persistent layer.
     * @param item the item to be persisted
     * @param checkpointManager the checkpoint manager to which this checkpoint belongs
     * @param bySeqno the by sequence number assigned to this mutation
     * @return a result indicating the status of the operation.
     */
    queue_dirty_t queueDirty(const queued_item &qi,
                             CheckpointManager *checkpointManager);

    uint64_t getLowSeqno() {
        if (numItems == 0) {
            return -1;
        }
        std::list<queued_item>::iterator pos = toWrite.begin();
        pos++; pos++;
        return (*pos)->getBySeqno();
    }

    uint64_t getHighSeqno() {
        if (numItems == 0) {
            return -1;
        }
        std::list<queued_item>::reverse_iterator pos = toWrite.rbegin();
        if (checkpointState != CHECKPOINT_OPEN) {
            ++pos;
        }
        return (*pos)->getBySeqno();
    }

    std::list<queued_item>::iterator begin() {
        return toWrite.begin();
    }

    std::list<queued_item>::iterator end() {
        return toWrite.end();
    }

    std::list<queued_item>::reverse_iterator rbegin() {
        return toWrite.rbegin();
    }

    std::list<queued_item>::reverse_iterator rend() {
        return toWrite.rend();
    }

    bool keyExists(const std::string &key);

    /**
     * Return the memory overhead of this checkpoint instance, except for the memory used by
     * all the items belonging to this checkpoint. The memory overhead of those items is
     * accounted separately in "ep_kv_size" stat.
     * @return memory overhead of this checkpoint instance.
     */
    size_t memorySize() {
        return sizeof(Checkpoint) + memOverhead;
    }

    /**
     * Merge the previous checkpoint into the this checkpoint by adding the items from
     * the previous checkpoint, which don't exist in this checkpoint.
     * @param pPrevCheckpoint pointer to the previous checkpoint.
     * @return the number of items added from the previous checkpoint.
     */
    size_t mergePrevCheckpoint(Checkpoint *pPrevCheckpoint);

    /**
     * Get the mutation id for a given key in this checkpoint
     * @param key a key to retrieve its mutation id
     * @return the mutation id for a given key
     */
    uint64_t getMutationIdForKey(const std::string &key);

private:
    EPStats                       &stats;
    uint64_t                       checkpointId;
    uint16_t                       vbucketId;
    rel_time_t                     creationTime;
    checkpoint_state               checkpointState;
    size_t                         numItems;
    std::set<std::string>          cursors; // List of cursors with their unique names.
    // List is used for queueing mutations as vector incurs shift operations for deduplication.
    std::list<queued_item>         toWrite;
    checkpoint_index               keyIndex;
    size_t                         memOverhead;
};

/**
 * Representation of a checkpoint manager that maintains the list of checkpoints
 * for each vbucket.
 */
class CheckpointManager {
    friend class Checkpoint;
    friend class EventuallyPersistentEngine;
    friend class Consumer;
    friend class TapConsumer;
    friend class UprConsumer;
public:

    CheckpointManager(EPStats &st, uint16_t vbucket, CheckpointConfig &config,
                      int64_t lastSeqno, uint64_t checkpointId = 1) :
        stats(st), checkpointConfig(config), vbucketId(vbucket), numItems(0),
        lastBySeqNo(lastSeqno), persistenceCursor("persistence"),
        isCollapsedCheckpoint(false),
        pCursorPreCheckpointId(0),
        pCursorSeqno(lastSeqno) {
        addNewCheckpoint(checkpointId);
        registerPersistenceCursor();
    }

    ~CheckpointManager();

    uint64_t getOpenCheckpointId_UNLOCKED();
    uint64_t getOpenCheckpointId();

    uint64_t getLastClosedCheckpointId_UNLOCKED();
    uint64_t getLastClosedCheckpointId();

    void setOpenCheckpointId_UNLOCKED(uint64_t id);

    void setOpenCheckpointId(uint64_t id) {
        LockHolder lh(queueLock);
        setOpenCheckpointId_UNLOCKED(id);
    }

    /**
     * Remove closed unreferenced checkpoints and return them through the vector.
     * @param vbucket the vbucket that this checkpoint manager belongs to.
     * @param newOpenCheckpointCreated the flag indicating if the new open checkpoint was created
     * as a result of running this function.
     * @return the number of items that are purged from checkpoint
     */
    size_t removeClosedUnrefCheckpoints(const RCPtr<VBucket> &vbucket,
                                        bool &newOpenCheckpointCreated);

    /**
     * Register the cursor for getting items whose bySeqno values are between
     * startBySeqno and endBySeqno, and close the open checkpoint if endBySeqno
     * belongs to the open checkpoint.
     * @param startBySeqno start bySeqno.
     * @param endBySeqno end bySeqno.
     * @return the bySeqno with which the cursor can start.
     */
    uint64_t registerTAPCursorBySeqno(const std::string &name,
                                      uint64_t startBySeqno,
                                      uint64_t endBySeqno);

    /**
     * Register the new cursor for a given TAP connection
     * @param name the name of a given TAP connection
     * @param checkpointId the checkpoint Id to start with.
     * @param alwaysFromBeginning the flag indicating if a cursor should be set to the beginning of
     * checkpoint to start with, even if the cursor is currently in that checkpoint.
     * @return true if the checkpoint to start with exists in the queue.
     */
    bool registerTAPCursor(const std::string &name, uint64_t checkpointId = 1,
                           bool alwaysFromBeginning = false);

    /**
     * Remove the cursor for a given TAP connection.
     * @param name the name of a given TAP connection
     * @return true if the TAP cursor is removed successfully.
     */
    bool removeTAPCursor(const std::string &name);

    /**
     * Get the Id of the checkpoint where the given TAP connection's cursor is currently located.
     * If the cursor is not found, return 0 as a checkpoint Id.
     * @param name the name of a given TAP connection
     * @return the checkpoint Id for a given TAP connection's cursor.
     */
    uint64_t getCheckpointIdForTAPCursor(const std::string &name);

    size_t getNumOfTAPCursors();

    std::list<std::string> getTAPCursorNames();

    /**
     * Queue an item to be written to persistent layer.
     * @param item the item to be persisted.
     * @param vbucket the vbucket that a new item is pushed into.
     * @param bySeqno the sequence number assigned to this mutation
     * @return true if an item queued increases the size of persistence queue by 1.
     */
    bool queueDirty(const RCPtr<VBucket> &vb, queued_item& qi, bool genSeqno);

    /**
     * Return the next item to be sent to a given TAP connection
     * @param name the name of a given TAP connection
     * @param isLastMutationItem flag indicating if the item to be returned is the last mutation one
     * in the closed checkpoint.
     * @return the next item to be sent to a given TAP connection.
     */
    queued_item nextItem(const std::string &name, bool &isLastMutationItem);

    /**
     * Return the list of items, which needs to be persisted, to the flusher.
     * @param items the array that will contain the list of items to be persisted and
     * be pushed into the flusher's outgoing queue where the further IO optimization is performed.
     */
    void getAllItemsForPersistence(std::vector<queued_item> &items);

    /**
     * Return the total number of items that belong to this checkpoint manager.
     */
    size_t getNumItems() {
        return numItems;
    }

    size_t getNumOpenChkItems();

    size_t getNumCheckpoints();

    /**
     * Return the total number of remaining items that should be visited by the persistence cursor.
     */
    size_t getNumItemsForPersistence_UNLOCKED();

    size_t getNumItemsForPersistence() {
        LockHolder lh(queueLock);
        return getNumItemsForPersistence_UNLOCKED();
    }

    size_t getNumItemsForTAPConnection(const std::string &name);

    /**
     * Return true if a given key was already visited by all the cursors
     * and is eligible for eviction.
     */
    bool eligibleForEviction(const std::string &key);

    /**
     * Clear all the checkpoints managed by this checkpoint manager.
     */
    void clear(vbucket_state_t vbState);

    /**
     * If a given TAP cursor currently points to the checkpoint_end dummy item,
     * decrease its current position by 1. This function is mainly used for checkpoint
     * synchronization between the master and slave nodes.
     * @param name the name of a given TAP connection
     */
    void decrTapCursorFromCheckpointEnd(const std::string &name);

    bool hasNext(const std::string &name);

    bool hasNextForPersistence();

    const CheckpointConfig &getCheckpointConfig() const {
        return checkpointConfig;
    }

    void addStats(ADD_STAT add_stat, const void *cookie);

    /**
     * Create a new open checkpoint by force.
     * @return the new open checkpoint id
     */
    uint64_t createNewCheckpoint();

    void resetTAPCursors(const std::list<std::string> &cursors);

    /**
     * Get id of the previous checkpoint that is followed by the checkpoint
     * where the persistence cursor is currently walking.
     */
    uint64_t getPersistenceCursorPreChkId();

    /**
     * Get seqno for the item that the persistence cursor is currently pointing to.
     */
    uint64_t getPersistenceCursorSeqno();

    /**
     * This method performs the following steps for creating a new checkpoint with a given ID i1:
     * 1) Check if the checkpoint manager contains any checkpoints with IDs >= i1.
     * 2) If exists, collapse all checkpoints and set the open checkpoint id to a given ID.
     * 3) Otherwise, simply create a new open checkpoint with a given ID.
     * This method is mainly for dealing with rollback events from a TAP producer.
     * @param id the id of a checkpoint to be created.
     * @param vbucket vbucket of the checkpoint.
     */
    void checkAndAddNewCheckpoint(uint64_t id, const RCPtr<VBucket> &vbucket);

    /**
     * Gets the mutation id for a given checkpoint item.
     * @param The checkpoint to look for the key in
     * @param The key to get the mutation id for
     * @return The mutation id or 0 if not found
     */
    uint64_t getMutationIdForKey(uint64_t chk_id, std::string key);

    bool incrCursor(CheckpointCursor &cursor);

    void itemsPersisted();

    int64_t getHighSeqno() {
        return lastBySeqNo;
    }

    int64_t nextBySeqno() {
        return ++lastBySeqNo;
    }

private:

    bool registerTAPCursor_UNLOCKED(const std::string &name,
                                    uint64_t checkpointId = 1,
                                    bool alwaysFromBeginning = false);

    void registerPersistenceCursor();

    /**
     * Create a new open checkpoint and add it to the checkpoint list.
     * The lock should be acquired before calling this function.
     * @param id the id of a checkpoint to be created.
     */
    bool addNewCheckpoint_UNLOCKED(uint64_t id);

    void removeInvalidCursorsOnCheckpoint(Checkpoint *pCheckpoint);

    /**
     * Create a new open checkpoint and add it to the checkpoint list.
     * @param id the id of a checkpoint to be created.
     */
    bool addNewCheckpoint(uint64_t id);

    bool moveCursorToNextCheckpoint(CheckpointCursor &cursor);

    /**
     * Check the current open checkpoint to see if we need to create the new open checkpoint.
     * @param forceCreation is to indicate if a new checkpoint is created due to online update or
     * high memory usage.
     * @param timeBound is to indicate if time bound should be considered in creating a new
     * checkpoint.
     * @return the previous open checkpoint Id if we create the new open checkpoint. Otherwise
     * return 0.
     */
    uint64_t checkOpenCheckpoint_UNLOCKED(bool forceCreation, bool timeBound);

    uint64_t checkOpenCheckpoint(bool forceCreation, bool timeBound) {
        LockHolder lh(queueLock);
        return checkOpenCheckpoint_UNLOCKED(forceCreation, timeBound);
    }

    bool closeOpenCheckpoint_UNLOCKED(uint64_t id);
    bool closeOpenCheckpoint(uint64_t id);

    void decrCursorOffset_UNLOCKED(CheckpointCursor &cursor, size_t decr);

    void decrCursorPos_UNLOCKED(CheckpointCursor &cursor);

    bool isLastMutationItemInCheckpoint(CheckpointCursor &cursor);

    bool isCheckpointCreationForHighMemUsage(const RCPtr<VBucket> &vbucket);

    void collapseClosedCheckpoints(std::list<Checkpoint*> &collapsedChks);

    void collapseCheckpoints(uint64_t id);

    void resetCursors(bool resetPersistenceCursor = true);

    void putCursorsInChk(std::map<std::string, uint64_t> &cursors,
                         std::list<Checkpoint*>::iterator chkItr);

    queued_item createCheckpointItem(uint64_t id, uint16_t vbid,
                                     enum queue_operation checkpoint_op);

    EPStats                 &stats;
    CheckpointConfig        &checkpointConfig;
    Mutex                    queueLock;
    uint16_t                 vbucketId;
    AtomicValue<size_t>      numItems;
    int64_t                  lastBySeqNo;
    std::list<Checkpoint*>   checkpointList;
    CheckpointCursor         persistenceCursor;
    bool                     isCollapsedCheckpoint;
    uint64_t                 lastClosedCheckpointId;
    uint64_t                 pCursorPreCheckpointId;
    uint64_t                 pCursorSeqno;
    cursor_index             tapCursors;
};

/**
 * A class containing the config parameters for checkpoint.
 */

class CheckpointConfig {
public:
    CheckpointConfig()
        : checkpointPeriod(DEFAULT_CHECKPOINT_PERIOD),
          checkpointMaxItems(DEFAULT_CHECKPOINT_ITEMS),
          maxCheckpoints(DEFAULT_MAX_CHECKPOINTS),
          itemNumBasedNewCheckpoint(true),
          keepClosedCheckpoints(false)
    { /* empty */ }

    CheckpointConfig(EventuallyPersistentEngine &e);

    rel_time_t getCheckpointPeriod() const {
        return checkpointPeriod;
    }

    size_t getCheckpointMaxItems() const {
        return checkpointMaxItems;
    }

    size_t getMaxCheckpoints() const {
        return maxCheckpoints;
    }

    bool isItemNumBasedNewCheckpoint() const {
        return itemNumBasedNewCheckpoint;
    }

    bool canKeepClosedCheckpoints() const {
        return keepClosedCheckpoints;
    }

protected:
    friend class CheckpointConfigChangeListener;
    friend class EventuallyPersistentEngine;

    bool validateCheckpointMaxItemsParam(size_t checkpoint_max_items);
    bool validateCheckpointPeriodParam(size_t checkpoint_period);
    bool validateMaxCheckpointsParam(size_t max_checkpoints);

    void setCheckpointPeriod(size_t value);
    void setCheckpointMaxItems(size_t value);
    void setMaxCheckpoints(size_t value);

    void allowItemNumBasedNewCheckpoint(bool value) {
        itemNumBasedNewCheckpoint = value;
    }

    void allowKeepClosedCheckpoints(bool value) {
        keepClosedCheckpoints = value;
    }

    static void addConfigChangeListener(EventuallyPersistentEngine &engine);

private:
    // Period of a checkpoint in terms of time in sec
    rel_time_t checkpointPeriod;
    // Number of max items allowed in each checkpoint
    size_t checkpointMaxItems;
    // Number of max checkpoints allowed
    size_t     maxCheckpoints;
    // Flag indicating if a new checkpoint is created once the number of items in the current
    // checkpoint is greater than the max number allowed.
    bool itemNumBasedNewCheckpoint;
    // Flag indicating if closed checkpoints should be kept in memory if the current memory usage
    // below the high water mark.
    bool keepClosedCheckpoints;
};

#endif  // SRC_CHECKPOINT_H_
