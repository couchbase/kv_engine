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
                     std::list<queued_item>::iterator pos) :
        currentCheckpoint(checkpoint), currentPos(pos) { }

private:
    std::list<Checkpoint*>::iterator currentCheckpoint;
    std::list<queued_item>::iterator currentPos;
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
        checkpointId(id), creationTime(ep_current_time()),
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
    rel_time_t getCreationTime() {
        return creationTime;
    }

    /**
     * Return the number of items belonging to this checkpoint.
     */
    size_t getNumItems() const {
        return numItems == 0 ? 0 : numItems - 1; // Exclude the dummy item.
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
    queue_dirty_t queueDirty(queued_item item, CheckpointManager *checkpointManager);


    std::list<queued_item>::iterator begin() {
        return toWrite.begin();
    }

    std::list<queued_item>::iterator end() {
        return toWrite.end();
    }

private:
    uint64_t                       checkpointId;
    rel_time_t                     creationTime;
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
public:

    CheckpointManager(EPStats &st, uint64_t checkpointId = 1) :
        stats(st), nextCheckpointId(checkpointId), numItems(0),
        persistenceCursorOffset(0), mutationCounter(0) {

        addNewCheckpoint(nextCheckpointId++);
        registerPersistenceCursor();
    }

    ~CheckpointManager();

    uint64_t getOpenCheckpointId();

    void setOpenCheckpointId(uint64_t id);

    /**
     * Remove closed unreferenced checkpoints and return them through the vector.
     * @param items the set that will contains the list of items in the collapsed
     * unreferenced checkpoints.
     * @return the last checkpoint Id in the unreferenced checkpoints.
     */
    uint64_t removeClosedUnrefCheckpoints(std::set<queued_item, CompareQueuedItemsByKey> &items);

    /**
     * Register the new cursor for a given TAP connection
     * @param name the name of a given TAP connection
     * @param checkpointId the checkpoint Id to start with.
     * @return true if the TAP cursor is registered successfully.
     */
    bool registerTAPCursor(const std::string &name, uint64_t checkpointId = 1);

    /**
     * Remove the cursor for a given TAP connection.
     * @param name the name of a given TAP connection
     * @return true if the TAP cursor is removed successfully.
     */
    bool removeTAPCursor(const std::string &name);

    size_t getNumOfTAPCursors();

    /**
     * Queue an item to be written to persistent layer.
     * @param item the item to be persisted.
     * @param vbucket the vbucket that a new item is pushed into.
     * @return true if an item queued increases the size of persistence queue by 1.
     */
    bool queueDirty(queued_item item, const RCPtr<VBucket> &vbucket);

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
        return numItems - persistenceCursorOffset;
    }

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
    void addNewCheckpoint_UNLOCKED(uint64_t id);

    /**
     * Create a new open checkpoint and add it to the checkpoint list.
     * @param id the id of a checkpoint to be created.
     */
    void addNewCheckpoint(uint64_t id);

    queued_item nextItemFromClosedCheckpoint(CheckpointCursor &cursor);

    queued_item nextItemFromOpenedCheckpoint(CheckpointCursor &cursor);

    uint64_t getAllItemsFromCurrentPosition(CheckpointCursor &cursor,
                                            std::vector<queued_item> &items);

    void moveCursorToNextCheckpoint(CheckpointCursor &cursor);

    /**
     * Check the current open checkpoint to see if we need to create the new open checkpoint.
     * @return the previous open checkpoint Id if we create the new open checkpoint. Otherwise
     * return 0.
     */
    uint64_t checkOpenCheckpoint();


    EPStats                 &stats;
    Mutex                    queueLock;
    Atomic<uint64_t>         nextCheckpointId;
    Atomic<size_t>           numItems;
    Atomic<size_t>           persistenceCursorOffset;
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
