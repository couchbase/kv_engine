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

#define MIN_CHECKPOINT_ITEMS 500
#define MAX_CHECKPOINT_ITEMS 500000
#define DEFAULT_CHECKPOINT_ITEMS 5000

#define MIN_CHECKPOINT_PERIOD 60
#define MAX_CHECKPOINT_PERIOD 3600
#define DEFAULT_CHECKPOINT_PERIOD 600

typedef enum {
    opened,
    closed
} checkpoint_state;

struct index_entry {
    std::list<queued_item>::iterator position;
    uint64_t mutation_id;
};

typedef unordered_map<std::string, index_entry> checkpoint_index;

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
                     size_t os = 0, bool isClosedCheckpointOnly = false) :
        currentCheckpoint(checkpoint), currentPos(pos),
        offset(os), closedCheckpointOnly(isClosedCheckpointOnly) { }

private:
    std::list<Checkpoint*>::iterator currentCheckpoint;
    std::list<queued_item>::iterator currentPos;
    Atomic<size_t>                   offset;
    bool                             closedCheckpointOnly;
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
    Checkpoint(EPStats &st, uint64_t id, checkpoint_state state = opened) :
        stats(st), checkpointId(id), creationTime(ep_real_time()),
        checkpointState(state), referenceCounter(0), numItems(0), indexMemOverhead(0) {
        stats.memOverhead.incr(memorySize());
        assert(stats.memOverhead.get() < GIGANTOR);
    }

    ~Checkpoint() {
        stats.memOverhead.decr(memorySize());
        assert(stats.memOverhead.get() < GIGANTOR);
    }

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

    /**
     * Return the memory overhead of this checkpoint instance, except for the memory used by
     * all the items belonging to this checkpoint. The memory overhead of those items is
     * accounted separately in "ep_kv_size" stat.
     * @return memory overhead of this checkpoint instance.
     */
    size_t memorySize() {
        return sizeof(Checkpoint) + indexMemOverhead;
    }

private:
    EPStats                       &stats;
    uint64_t                       checkpointId;
    rel_time_t                     creationTime;
    checkpoint_state               checkpointState;
    size_t                         referenceCounter;
    size_t                         numItems;
    // List is used for queueing mutations as vector incurs shift operations for deduplication.
    std::list<queued_item>         toWrite;
    checkpoint_index               keyIndex;
    size_t                         indexMemOverhead;
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
        mutationCounter(0), doOnlineUpdate(false), doHotReload(false) {

        addNewCheckpoint(nextCheckpointId++);
        registerPersistenceCursor();
    }

    ~CheckpointManager();

    uint64_t getOpenCheckpointId();

    uint64_t getLastClosedCheckpointId() {
        uint64_t id = getOpenCheckpointId();
        return id > 0 ? (id - 1) : 0;
    }

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
     * @param closedCheckpointOnly the flag indicating if a cursor is only for closed checkpoints.
     * @param alwaysFromBeginning the flag indicating if a cursor should be set to the beginning of
     * checkpoint to start with, even if the cursor is currently in that checkpoint.
     * @return true if the checkpoint to start with exists in the queue.
     */
    bool registerTAPCursor(const std::string &name, uint64_t checkpointId = 1,
                           bool closedCheckpointOnly = false, bool alwaysFromBeginning = false);

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
     * Start onlineupdate - stop persisting mutation to disk
     * @return :
     *    PROTOCOL_BINARY_RESPONSE_SUCCESS if the online update mode is enabled
     *    PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED if server is in online update mode.
     */
    protocol_binary_response_status startOnlineUpdate();

    /**
     * Stop onlineupdate and continue persisting mutations
     * @return :
     *    PROTOCOL_BINARY_RESPONSE_SUCCESS if the online update process finishes.
     *    PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED if server is noT in online update mode
     */
    protocol_binary_response_status stopOnlineUpdate();

    /**
    *  Start hot reload process to create a hotreload event for taps and
    *  move persistenceCursor to skip all mutations during onlineupgrade period.
    *  @return :
    *     PROTOCOL_BINARY_RESPONSE_SUCCESS if successfully to start hot reload process
    *     PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED if the server is NOT in online update mode
    */
    protocol_binary_response_status beginHotReload();

    /**
    *  Finish hot reload process
    * @total total items that are reverted
    * @return
    *    PROTOCOL_BINARY_RESPONSE_SUCCESS - finish hot reload process
    */
    protocol_binary_response_status endHotReload(uint64_t total);

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
     * @param isLastMutationItem flag indicating if the item to be returned is the last mutation one
     * in the closed checkpoint.
     * @return the next item to be sent to a given TAP connection.
     */
    queued_item nextItem(const std::string &name, bool &isLastMutationItem);

    /**
     * Return the list of items, which needs to be persisted, to the flusher.
     * @param items the array that will contain the list of items to be persisted and
     * be pushed into the flusher's outgoing queue where the further IO optimization is performed.
     * @return the last closed checkpoint Id.
     */
    uint64_t getAllItemsForPersistence(std::vector<queued_item> &items);
    /**
     * Return the list of items, which needs to be updated from backgrond fetcher.
     * @param items the array that will contain the list of items to be fetched and
     * be updated to the hashtable.
     * @return the last closed checkpoint Id.
     */
    uint64_t getAllItemsForOnlineUpdate(std::vector<queued_item> &items);

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

    size_t getNumCheckpoints();

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

    bool isOnlineUpdate() { return doOnlineUpdate; }
    void setOnlineUpdate(bool olupdate) { doOnlineUpdate = olupdate; }

    bool isHotReload() { return doHotReload; }

    /**
     * If a given TAP cursor currently points to the checkpoint_end dummy item,
     * decrease its current position by 1. This function is mainly used for checkpoint
     * synchronization between the master and slave nodes.
     * @param name the name of a given TAP connection
     */
    void decrTapCursorFromCheckpointEnd(const std::string &name);

    bool hasNext(const std::string &name);

    static void initializeCheckpointConfig(size_t checkpoint_period,
                                           size_t checkpoint_max_items,
                                           bool allow_inconsistency = false) {
        if (!validateCheckpointMaxItemsParam(checkpoint_max_items) ||
            !validateCheckpointPeriodParam(checkpoint_period)) {
            return;
        }
        checkpointPeriod = checkpoint_period;
        checkpointMaxItems = checkpoint_max_items;
        inconsistentSlaveCheckpoint = allow_inconsistency;
    }

    static void setCheckpointPeriod(size_t checkpoint_period) {
        if (!validateCheckpointPeriodParam(checkpoint_period)) {
            return;
        }
        checkpointPeriod = checkpoint_period;
    }

    static void setCheckpointMaxItems(size_t checkpoint_max_items) {
        if (!validateCheckpointMaxItemsParam(checkpoint_max_items)) {
            return;
        }
        checkpointMaxItems = checkpoint_max_items;
    }

    static void allowInconsistentSlaveCheckpoint(bool allow_inconsistency) {
        inconsistentSlaveCheckpoint = allow_inconsistency;
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

    queued_item nextItemFromClosedCheckpoint(CheckpointCursor &cursor, bool &isLastMutationItem);

    queued_item nextItemFromOpenedCheckpoint(CheckpointCursor &cursor, bool &isLastMutationItem);

    uint64_t getAllItemsFromCurrentPosition(CheckpointCursor &cursor,
                                            uint64_t barrier,
                                            std::vector<queued_item> &items);

    bool moveCursorToNextCheckpoint(CheckpointCursor &cursor);

    /**
     * Check the current open checkpoint to see if we need to create the new open checkpoint.
     * @param forceCreation is to indicate if a new checkpoint is created due to online update or
     * high memory usage
     * @return the previous open checkpoint Id if we create the new open checkpoint. Otherwise
     * return 0.
     */
    uint64_t checkOpenCheckpoint_UNLOCKED(bool forceCreation);

    uint64_t checkOpenCheckpoint(bool forceCreation=false) {
        LockHolder lh(queueLock);
        return checkOpenCheckpoint_UNLOCKED(forceCreation);
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

    bool isLastMutationItemInCheckpoint(CheckpointCursor &cursor);

    bool isCheckpointCreationForHighMemUsage(const RCPtr<VBucket> &vbucket);

    static bool validateCheckpointMaxItemsParam(size_t checkpoint_max_items);
    static bool validateCheckpointPeriodParam(size_t checkpoint_period);
    static queued_item createCheckpointItem(uint64_t id, uint16_t vbid,
                                            enum queue_operation checkpoint_op);

    EPStats                 &stats;
    Mutex                    queueLock;
    uint16_t                 vbucketId;
    Atomic<uint64_t>         nextCheckpointId;
    Atomic<size_t>           numItems;
    uint64_t                 mutationCounter;
    std::list<Checkpoint*>   checkpointList;
    CheckpointCursor         persistenceCursor;
    CheckpointCursor         onlineUpdateCursor;
    std::map<const std::string, CheckpointCursor> tapCursors;

    // Period of a checkpoint in terms of time in sec
    static Atomic<rel_time_t> checkpointPeriod;
    // Number of max items allowed in each checkpoint
    static Atomic<size_t>     checkpointMaxItems;
    // Flag indicating if a downstream active vbucket is allowed to receive checkpoint start/end
    // messages from the master active vbucket.
    static bool               inconsistentSlaveCheckpoint;

    Atomic<bool>              doOnlineUpdate;
    Atomic<bool>              doHotReload;
};

#endif /* CHECKPOINT_HH */
