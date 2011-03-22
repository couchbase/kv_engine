/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef CHECKPOINT_HH
#define CHECKPOINT_HH 1

#include <assert.h>
#include <list>
#include <map>
#include <set>

#include "common.hh"
#include "atomic.hh"
#include "locks.hh"
#include "queueditem.hh"
#include "stats.hh"

#define MAX_CHECKPOINT_ITEMS 500000
#define MAX_CHECKPOINT_PERIOD 3600

typedef enum {
    opened,
    closed
} checkpoint_state;

struct index_entry {
    std::list<queued_item>::iterator position;
    uint64_t mutation_id;
};

typedef std::map<std::string, index_entry> checkpoint_index;

class Checkpoint;
class CheckpointManager;
class VBucket;

/**
 * A checkpoint cursor
 */
class CheckpointCursor {
    friend class CheckpointManager;
    friend class Checkpoint;
public:
    CheckpointCursor() { }

    CheckpointCursor(std::list<Checkpoint*>::iterator checkpoint,
                     std::list<queued_item>::iterator pos,
                     size_t os = 0) :
        currentCheckpoint(checkpoint), currentPos(pos), offset(os) { }

private:
    std::list<Checkpoint*>::iterator currentCheckpoint;
    std::list<queued_item>::iterator currentPos;
    Atomic<size_t>                   offset;
};

/**
 * Result from invoking queueDirty in the current open checkpoint.
 */
typedef enum {
    EXISTING_ITEM,     //!< The item was already in the checkpoint and pushed back to the tail.
    NEW_ITEM          //!< The item is newly added to the tail.
} queue_dirty_t;

/**
 * Representation of a checkpoint used in the unified queue for persistence and tap.
 */
class Checkpoint {
public:
    Checkpoint(uint64_t id, checkpoint_state state = opened) :
        checkpointId(id), creationTime(ep_real_time()),
        checkpointState(state), referenceCounter(0), numItems(0) { }

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
    time_t getCreationTime() {
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

    /**
     * Return the number of cursors that are currently walking through this checkpoint.
     */
    size_t getReferenceCounter() const {
        return referenceCounter;
    }

    /**
     * Increase the reference counter by 1.
     */
    void incrReferenceCounter() {
        ++referenceCounter;
    }

    /**
     * Decrease the reference counter by 1.
     */
    void decrReferenceCounter() {
        if (referenceCounter == 0) {
            return;
        }
        --referenceCounter;
    }

    /**
     * Queue an item to be written to persistent layer.
     * @param item the item to be persisted
     * @param checkpointManager the checkpoint manager to which this checkpoint belongs
     * @return a result indicating the status of the operation.
     */
    queue_dirty_t queueDirty(const queued_item &item, CheckpointManager *checkpointManager);


    std::list<queued_item>::iterator begin() {
        return toWrite.begin();
    }

    std::list<queued_item>::iterator end() {
        return toWrite.end();
    }

    uint64_t getCasForKey(const std::string &key);

private:
    uint64_t                       checkpointId;
    time_t                     creationTime;
    Atomic<checkpoint_state>       checkpointState;
    Atomic<size_t>                 referenceCounter;
    Atomic<size_t>                 numItems;
    // List is used for queueing mutations as vector incurs shift operations for deduplication.
    std::list<queued_item>         toWrite;
    checkpoint_index               keyIndex;
};

/**
 * Representation of a checkpoint manager that maintains the list of checkpoints
 * for each vbucket.
 */
class CheckpointManager {
    friend class Checkpoint;
    friend class EventuallyPersistentEngine;
    friend class TapConsumer;
public:

    CheckpointManager(EPStats &st, uint16_t vbucket, uint64_t checkpointId = 1) :
        stats(st), vbucketId(vbucket), nextCheckpointId(checkpointId), numItems(0),
        mutationCounter(0) {

        addNewCheckpoint(nextCheckpointId++);
        registerPersistenceCursor();
    }

    ~CheckpointManager();

    uint64_t getOpenCheckpointId();

    void setOpenCheckpointId(uint64_t id);

    /**
     * Remove closed unreferenced checkpoints and return them through the vector.
     * @param vbucket the vbucket that this checkpoint manager belongs to.
     * @param items the set that will contains the list of items in the collapsed
     * @param newOpenCheckpointCreated the flag indicating if the new open checkpoint was created
     * as a result of running this function.
     * unreferenced checkpoints.
     * @return the last checkpoint Id in the unreferenced checkpoints.
     */
    uint64_t removeClosedUnrefCheckpoints(const RCPtr<VBucket> &vbucket,
                                          std::set<queued_item, CompareQueuedItemsByKey> &items,
                                          bool &newOpenCheckpointCreated);

    /**
     * Register the new cursor for a given TAP connection
     * @param name the name of a given TAP connection
     * @param checkpointId the checkpoint Id to start with.
     * @return true if the checkpoint to start with exists in the queue.
     */
    bool registerTAPCursor(const std::string &name, uint64_t checkpointId = 1);

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

    /**
     * Queue an item to be written to persistent layer.
     * @param item the item to be persisted.
     * @param vbucket the vbucket that a new item is pushed into.
     * @return true if an item queued increases the size of persistence queue by 1.
     */
    bool queueDirty(const queued_item &item, const RCPtr<VBucket> &vbucket);

    /**
     * Return the next item to be sent to a given TAP connection
     * @param name the name of a given TAP connection
     * @return the next item to be sent to a given TAP connection.
     */
    queued_item nextItem(const std::string &name);

    /**
     * Return the list of items, which needs to be persisted, to the flusher.
     * @param items the array that will contain the list of items to be persisted and
     * be pushed into the flusher's outgoing queue where the further IO optimization is performed.
     * @return the last closed checkpoint Id.
     */
    uint64_t getAllItemsForPersistence(std::vector<queued_item> &items);

    /**
     * Return the list of all the items to a given TAP cursor since its current position.
     * @param name the name of a given TAP connection.
     * @param items the array that will contain the list of items to be returned
     * to the TAP connection.
     * @return the last closed checkpoint Id.
     */
    uint64_t getAllItemsForTAPConnection(const std::string &name, std::vector<queued_item> &items);

    /**
     * Return the total number of items that belong to this checkpoint manager.
     */
    size_t getNumItems() {
        return numItems;
    }

    /**
     * Return the total number of remaining items that should be visited by the persistence cursor.
     */
    size_t getNumItemsForPersistence() {
        return numItems - persistenceCursor.offset;
    }

    size_t getNumItemsForTAPConnection(const std::string &name);

    /**
     * Return true if a given key with its CAS value exists in the open or
     * closed referenced checkpoints. This function is invoked by the item pager to determine
     * if a given key's value can be evicted from memory hashtable.
     */
    bool isKeyResidentInCheckpoints(const std::string &key, uint64_t cas);

    /**
     * Clear all the checkpoints managed by this checkpoint manager.
     */
    void clear();

    static void initializeCheckpointConfig(size_t checkpoint_period,
                                           size_t checkpoint_max_items) {
        checkpointPeriod = checkpoint_period;
        checkpointMaxItems = checkpoint_max_items;
    }

    static void setCheckpointPeriod(size_t checkpoint_period) {
        checkpointPeriod = checkpoint_period;
    }

    static void setCheckpointMaxItems(size_t checkpoint_max_items) {
        checkpointMaxItems = checkpoint_max_items;
    }

private:

    void registerPersistenceCursor();

    /**
     * Create a new open checkpoint and add it to the checkpoint list.
     * The lock should be acquired before calling this function.
     * @param id the id of a checkpoint to be created.
     */
    bool addNewCheckpoint_UNLOCKED(uint64_t id);

    /**
     * Create a new open checkpoint and add it to the checkpoint list.
     * @param id the id of a checkpoint to be created.
     */
    bool addNewCheckpoint(uint64_t id);

    queued_item nextItemFromClosedCheckpoint(CheckpointCursor &cursor);

    queued_item nextItemFromOpenedCheckpoint(CheckpointCursor &cursor);

    uint64_t getAllItemsFromCurrentPosition(CheckpointCursor &cursor,
                                            std::vector<queued_item> &items);

    bool moveCursorToNextCheckpoint(CheckpointCursor &cursor);

    /**
     * Check the current open checkpoint to see if we need to create the new open checkpoint.
     * @return the previous open checkpoint Id if we create the new open checkpoint. Otherwise
     * return 0.
     */
    uint64_t checkOpenCheckpoint_UNLOCKED();

    uint64_t checkOpenCheckpoint() {
        LockHolder lh(queueLock);
        return checkOpenCheckpoint_UNLOCKED();
    }

    bool closeOpenCheckpoint_UNLOCKED(uint64_t id);
    bool closeOpenCheckpoint(uint64_t id);

    /**
     * This method performs the following steps for creating a new checkpoint with a given Id:
     * 1) Check if the checkpoint manager already contains the checkpoint with the same id.
     * 2) If exists, remove the existing checkpoint and all of its following checkpoints, and
     *    then create a new checkpoint with a given Id and reposition all the cursors appropriately.
     * 3) Otherwise, simply create a new open checkpoint with a given Id.
     * This method is mainly for dealing with rollback events from a TAP producer.
     * @param id the id of a checkpoint to be created.
     */
    bool checkAndAddNewCheckpoint(uint64_t id);

    uint64_t nextMutationId() {
        return ++mutationCounter;
    }

    void decrPersistenceCursorOffset() {
        if (persistenceCursor.offset > 0) {
            --(persistenceCursor.offset);
        }
    }

    void decrPersistenceCursorPos_UNLOCKED() {
        if (persistenceCursor.currentPos != (*(persistenceCursor.currentCheckpoint))->begin()) {
            --(persistenceCursor.currentPos);
        }
    }

    EPStats                 &stats;
    Mutex                    queueLock;
    uint16_t                 vbucketId;
    Atomic<uint64_t>         nextCheckpointId;
    Atomic<size_t>           numItems;
    uint64_t                 mutationCounter;
    std::list<Checkpoint*>   checkpointList;
    CheckpointCursor         persistenceCursor;
    std::map<const std::string, CheckpointCursor> tapCursors;

    // Period of a checkpoint in terms of time in sec
    static Atomic<rel_time_t> checkpointPeriod;
    // Number of max items allowed in each checkpoint
    static Atomic<size_t>     checkpointMaxItems;
};

#endif /* CHECKPOINT_HH */
