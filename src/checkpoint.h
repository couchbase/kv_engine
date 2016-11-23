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

#include <list>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include <atomic>
#include "ep_types.h"
#include "item.h"
#include "locks.h"
#include "stats.h"

#define GIGANTOR ((size_t)1<<(sizeof(size_t)*8-1))

#define MIN_CHECKPOINT_ITEMS 10
#define MAX_CHECKPOINT_ITEMS 50000
#define DEFAULT_CHECKPOINT_ITEMS 500

#define MIN_CHECKPOINT_PERIOD 1 //  1 sec.
#define MAX_CHECKPOINT_PERIOD 3600 // 3600 sec.
#define DEFAULT_CHECKPOINT_PERIOD 5 // 5 sec.

#define DEFAULT_MAX_CHECKPOINTS 2
#define MAX_CHECKPOINTS_UPPER_BOUND 5

/**
 * The state of a given checkpoint.
 */
enum checkpoint_state {
    CHECKPOINT_OPEN, //!< The checkpoint is open.
    CHECKPOINT_CLOSED  //!< The checkpoint is not open.
};

const char* to_string(enum checkpoint_state);

// List is used for queueing mutations as vector incurs shift operations for
// deduplication.
typedef std::list<queued_item> CheckpointQueue;

/**
 * A checkpoint index entry.
 */
struct index_entry {
    CheckpointQueue::iterator position;
    int64_t mutation_id;
};

typedef struct {
    uint64_t start;
    uint64_t end;
} snapshot_range_t;

typedef struct {
    uint64_t start;
    snapshot_range_t range;
} snapshot_info_t;

/**
 * Flag indicating that we must send checkpoint end meta item for the cursor
 */
enum class MustSendCheckpointEnd {
    NO,
    YES
};

/**
 * The checkpoint index maps a key to a checkpoint index_entry.
 */
typedef std::unordered_map<StoredDocKey, index_entry> checkpoint_index;

/**
 * List of pairs containing checkpoint cursor name and corresponding flag
 * indicating whether we must send checkpoint end meta item for the cursor
 */
typedef std::list<std::pair<std::string, MustSendCheckpointEnd>>
                                                    checkpointCursorInfoList;

class Checkpoint;
class CheckpointManager;
class CheckpointConfig;
class VBucket;

/**
 * A checkpoint cursor, representing the current position in a Checkpoint
 * series.
 *
 * CheckpointCursors are similar to STL-style iterators but for Checkpoints.
 * A consumer (DCP, TAP, persistence) will have one CheckpointCursor, initially
 * positioned at the first item they want. As they read items from the
 * Checkpoint the Cursor is advanced, allowing them to continue from where
 * they left off when they next attempt to read items.
 *
 * A CheckpointCursor has two main pieces of state:
 *
 * - currentCheckpoint - The current Checkpoint the cursor is operating on.
 * - currentPos - the position with the current Checkpoint.
 *
 * When a CheckpointCursor reaches the end of Checkpoint, the CheckpointManager
 * will move it to the next Checkpoint.
 *
 * To assist in accounting how many items remain in a Checkpoint series, a
 * cursor also records its `offset` - the count of items (non-meta and meta) it
 * has already 'consumed' from the Checkpoint series. Note that this `offset`
 * count is not cumulative - when the CheckpointManager removes checkpoints
 * the offset will be decremented. To put it another way - the number of items
 * a CheckpointCursor has left to consume can be calcuated as
 * `CheckpointManager::numItems - CheckpointCursor::offset`.
 */
class CheckpointCursor {
    friend class CheckpointManager;
    friend class Checkpoint;
public:

    CheckpointCursor() { }

    CheckpointCursor(const std::string &n)
        : name(n),
          currentCheckpoint(),
          currentPos(),
          offset(0),
          ckptMetaItemsRead(0),
          fromBeginningOnChkCollapse(false),
          sendCheckpointEndMetaItem(MustSendCheckpointEnd::YES) { }

    /**
     * @param offset_ Count of items (normal+meta) already read for *all*
     *                checkpoints in the series.
     * @param meta_items_read Count of meta_items already read for the
     *                        given checkpoint.
     */
    CheckpointCursor(const std::string &n,
                     std::list<Checkpoint*>::iterator checkpoint,
                     CheckpointQueue::iterator pos,
                     size_t offset_,
                     size_t meta_items_read,
                     bool beginningOnChkCollapse,
                     MustSendCheckpointEnd needsCheckpointEndMetaItem) :
        name(n), currentCheckpoint(checkpoint), currentPos(pos), numVisits(0),
        offset(offset_),
        ckptMetaItemsRead(meta_items_read),
        fromBeginningOnChkCollapse(beginningOnChkCollapse),
        sendCheckpointEndMetaItem(needsCheckpointEndMetaItem) { }

    // We need to define the copy construct explicitly due to the fact
    // that std::atomic implicitly deleted the assignment operator
    CheckpointCursor(const CheckpointCursor &other) :
        name(other.name), currentCheckpoint(other.currentCheckpoint),
        currentPos(other.currentPos), numVisits(other.numVisits.load()),
        offset(other.offset.load()),
        ckptMetaItemsRead(other.ckptMetaItemsRead),
        fromBeginningOnChkCollapse(other.fromBeginningOnChkCollapse),
        sendCheckpointEndMetaItem(other.sendCheckpointEndMetaItem) { }

    CheckpointCursor &operator=(const CheckpointCursor &other) {
        name.assign(other.name);
        currentCheckpoint = other.currentCheckpoint;
        currentPos = other.currentPos;
        numVisits = other.numVisits.load();
        offset.store(other.offset.load());
        setMetaItemOffset(other.ckptMetaItemsRead);
        fromBeginningOnChkCollapse = other.fromBeginningOnChkCollapse;
        sendCheckpointEndMetaItem = other.sendCheckpointEndMetaItem;
        return *this;
    }

    /**
     * Decrement the offsets for this cursor.
     * @param items Count of all items (meta and non-meta) to decrement by.
     */
    void decrOffset(size_t decr);

    void decrPos();

    /* Indicates whether we must send checkpoint end meta item for the cursor.
       Currently TAP cursors require checkpoint end meta item to be sent to
       the consumer. DCP cursors don't have this constraint */
    MustSendCheckpointEnd shouldSendCheckpointEndMetaItem() const;

    /**
     * Return the count of meta items processed (i.e. moved past) for the
     * current checkpoint.
     * This value is reset to zero when a new checkpoint
     * is entered.
     */
    size_t getCurrentCkptMetaItemsRead() const;

protected:
    void incrMetaItemOffset(size_t incr) {
        ckptMetaItemsRead += incr;
    }

    void setMetaItemOffset(size_t val) {
        ckptMetaItemsRead = val;
    }

private:
    std::string                      name;
    std::list<Checkpoint*>::iterator currentCheckpoint;
    CheckpointQueue::iterator currentPos;

    // Number of times a cursor has been moved or processed.
    std::atomic<size_t>              numVisits;

    // The offset (in terms of items) this cursor is from the start of the
    // checkpoint list. Includes meta and non-meta items. Used to calculate
    // how many items this cursor has remaining by subtracting
    // offset from CheckpointManager::numItems.
    std::atomic<size_t>              offset;
    // Count of the number of meta items which have been read (processed) for
    // the *current* checkpoint.
    size_t ckptMetaItemsRead;
    bool                             fromBeginningOnChkCollapse;
    MustSendCheckpointEnd            sendCheckpointEndMetaItem;

    friend std::ostream& operator<<(std::ostream& os, const CheckpointCursor& c);
};

std::ostream& operator<<(std::ostream& os, const CheckpointCursor& c);

/**
 * The cursor index maps checkpoint cursor names to checkpoint cursors
 */
typedef std::map<const std::string, CheckpointCursor> cursor_index;

/**
 * Result from invoking queueDirty in the current open checkpoint.
 */
enum queue_dirty_t {
    /*
     * The item exists on the right hand side of the persistence cursor - i.e.
     * the persistence cursor has not yet processed this key.
     * The item will be deduplicated and doesn't change the size of the checkpoint.
     */
    EXISTING_ITEM,

    /**
     * The item exists on the left hand side of the persistence cursor - i.e.
     * the persistence cursor has already processed one (or more) instances of
     * this key.
     * It will be dedeuplicated and moved the to right hand side, but the item
     * needs to be re-persisted.
     */
    PERSIST_AGAIN,

    /**
     * The item doesn't exist yet in the checkpoint. Adding this item will
     * increase the size of the checkpoint.
     */
    NEW_ITEM
};

/**
 * Representation of a checkpoint used in the unified queue for persistence and
 * replication.
 *
 * Each Checkpoint consists of an ordered series of queued_item items, each
 * of which either represents a 'real' user operation
 * (queue_op::set & queue_op::del), or one of a range of meta-items
 * (queue_op::checkpoint_start, queue_op::checkpoint_end, ...).
 *
 * A checkpoint may either be Open or Closed. Open checkpoints can still have
 * new items appended to them, whereas Closed checkpoints cannot (and are
 * logically immutable). A checkpoint begins life as an Open checkpoint, will
 * have items added to it (including de-duplication if a key is added which
 * already exists), and then once large/old enough it will be marked as Closed,
 * and a new Open checkpoint created for new items.
 *
 * Consumers read items from Checkpoints by creating a CheckpointCursor
 * (similar to an STL iterator), which they use to mark how far along the
 * Checkpoint they are.
 *
 *
 *     Checkpoint (closed)
 *                               numItems: 5 (1x start, 2x set, 1x del, 1x end)
 *
 *              +-------+-------+-------+-------+-------+-------+
 *              | empty | Start |  Set  |  Set  |  Del  |  End  |
 *              +-------+-------+-------+-------+-------+-------+
 *         seqno    0       1       1       2       3       3
 *
 *                  ^
 *                  |
 *                  |
 *            CheckpointCursor
 *             (initial pos)
 *
 *     Checkpoint (open)
 *                               numItems: 4 (1x start, 1x set, 2x set)
 *
 *              +-------+-------+-------+-------+-------+
 *              | empty | Start |  Del  |  Set  |  Set
 *              +-------+-------+-------+-------+-------+
 *         seqno    3       4       4       5       6
 *
 * A Checkpoint starts with an empty item, followed by a checkpoint_start,
 * and then 0...N set and del items, finally finishing with a checkpoint_end if
 * the Checkpoint is closed.
 * The empty item exists because Checkpoints are structured such that
 * CheckpointCursors are advanced _before_ dereferencing them, not _after_
 * (this differs from STL iterators which are typically incremented after
 * dereferencing them) - i.e. the pseudo-code for reading elements is:
 *
 *     getElements(CheckpointCursor& cur) {
 *         std::vector<...> result;
 *         while (incrCursorAndCheckValid(cur) {
 *             result.push_back(*cur);
 *         };
 *         return result;
 *     }
 *
 * As such we need to have a dummy element at the start of each Checkpoint, so
 * after the first call to CheckpointManager::incrCursor() the cursor
 * dereferences to the checkpoint_start element.
 *
 * Note that sequence numbers are only unique for normal operations (set & del) -
 * for meta-items like checkpoint start/end they share the same sequence number
 * as the associated op - for checkpoint_start that is the ID of the following
 * op, for checkpoint_end the ID of the proceeding op.
 */
class Checkpoint {
public:
    Checkpoint(EPStats &st, uint64_t id, uint64_t snapStart, uint64_t snapEnd,
               uint16_t vbid) :
        stats(st), checkpointId(id), snapStartSeqno(snapStart),
        snapEndSeqno(snapEnd), vbucketId(vbid), creationTime(ep_real_time()),
        checkpointState(CHECKPOINT_OPEN),
        numItems(0),
        numMetaItems(0),
        memOverhead(0),
        effectiveMemUsage(0) {
        stats.memOverhead->fetch_add(memorySize());
        if (stats.memOverhead->load() >= GIGANTOR) {
            LOG(EXTENSION_LOG_WARNING,
                "Checkpoint::Checkpoint: stats.memOverhead (which is %" PRId64
                ") is greater than %" PRId64, uint64_t(stats.memOverhead->load()),
                uint64_t(GIGANTOR));
        }
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
     * Return the number of non-meta items belonging to this checkpoint.
     */
    size_t getNumItems() const {
        return numItems;
    }

    /**
     * Return the number of meta items (as defined by Item::isNonEmptyCheckpointMetaItem)
     * in this checkpoint.
     */
    size_t getNumMetaItems() const;

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

    uint64_t getLowSeqno() const {
        auto pos = toWrite.begin();
        pos++;
        return (*pos)->getBySeqno();
    }

    uint64_t getHighSeqno() const {
        auto pos = toWrite.rbegin();
        return (*pos)->getBySeqno();
    }

    uint64_t getSnapshotStartSeqno() {
        return snapStartSeqno;
    }

    void setSnapshotStartSeqno(uint64_t seqno) {
        snapStartSeqno = seqno;
    }

    uint64_t getSnapshotEndSeqno() {
        return snapEndSeqno;
    }

    void setSnapshotEndSeqno(uint64_t seqno) {
        snapEndSeqno = seqno;
    }

    CheckpointQueue::iterator begin() {
        return toWrite.begin();
    }

    CheckpointQueue::const_iterator begin() const {
        return toWrite.begin();
    }

    CheckpointQueue::iterator end() {
        return toWrite.end();
    }

    CheckpointQueue::const_iterator end() const {
        return toWrite.end();
    }

    CheckpointQueue::reverse_iterator rbegin() {
        return toWrite.rbegin();
    }

    CheckpointQueue::reverse_iterator rend() {
        return toWrite.rend();
    }

    bool keyExists(const DocKey& key);

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
     * @param isMetaKey indicates if the key is a checkpoint meta item
     * @return the mutation id for a given key
     */
    uint64_t getMutationIdForKey(const DocKey& key, bool isMetaKey);

    /**
     * Function invoked by the cursor-dropper which checks if the
     * peristence cursor is currently in the given checkpoint, in
     * which case returns false, otherwise true.
     */
    bool isEligibleToBeUnreferenced();

    /**
     * Invoked by the checkpoint manager whenever an item is queued
     * into the given checkpoint.
     * @param Amount of memory being added to current usage
     */
    void incrementMemConsumption(size_t by) {
        effectiveMemUsage += by;
    }

    /**
     * Returns the memory held by all the queued items which includes
     * key, metadata and the blob.
     */
    size_t getMemConsumption() {
        return effectiveMemUsage;
    }

    static const StoredDocKey DummyKey;
    static const StoredDocKey CheckpointStartKey;
    static const StoredDocKey CheckpointEndKey;
    static const StoredDocKey SetVBucketStateKey;

private:
    EPStats                       &stats;
    uint64_t                       checkpointId;
    uint64_t                       snapStartSeqno;
    uint64_t                       snapEndSeqno;
    uint16_t                       vbucketId;
    rel_time_t                     creationTime;
    checkpoint_state               checkpointState;
    /// Number of non-meta items (see Item::isCheckPointMetaItem).
    size_t                         numItems;
    /// Number of meta items (see Item::isCheckPointMetaItem).
    size_t numMetaItems;
    std::set<std::string>          cursors; // List of cursors with their unique names.
    CheckpointQueue                toWrite;
    checkpoint_index               keyIndex;
    /* Index for meta keys like "dummy_key" */
    checkpoint_index               metaKeyIndex;
    size_t                         memOverhead;

    // The following stat is to contain the memory consumption of all
    // the queued items in the given checkpoint.
    size_t                         effectiveMemUsage;

    friend std::ostream& operator <<(std::ostream& os, const Checkpoint& m);
};

typedef std::pair<uint64_t, bool> CursorRegResult;

/**
 * Representation of a checkpoint manager that maintains the list of checkpoints
 * for a given vbucket.
 */
class CheckpointManager {
    friend class Checkpoint;
    friend class EventuallyPersistentEngine;
    friend class Consumer;
    friend class TapConsumer;
public:

    typedef std::shared_ptr<Callback<uint16_t> > FlusherCallback;

    CheckpointManager(EPStats& st,
                      uint16_t vbucket,
                      CheckpointConfig& config,
                      int64_t lastSeqno,
                      uint64_t lastSnapStart,
                      uint64_t lastSnapEnd,
                      FlusherCallback cb);

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
    size_t removeClosedUnrefCheckpoints(VBucket& vbucket,
                                        bool& newOpenCheckpointCreated);

    /**
     * Register the cursor for getting items whose bySeqno values are between
     * startBySeqno and endBySeqno, and close the open checkpoint if endBySeqno
     * belongs to the open checkpoint.
     * @param startBySeqno start bySeqno.
     * @param needsCheckpointEndMetaItem indicates the CheckpointEndMetaItem
     *        must not be skipped for the cursor.
     * @return Cursor registration result which consists of (1) the bySeqno with
     * which the cursor can start and (2) flag indicating if the cursor starts
     * with the first item on a checkpoint.
     */
    CursorRegResult registerCursorBySeqno(
                            const std::string &name,
                            uint64_t startBySeqno,
                            MustSendCheckpointEnd needsCheckpointEndMetaItem);

    /**
     * Register the new cursor for a given connection
     * @param name the name of a given connection
     * @param checkpointId the checkpoint Id to start with.
     * @param alwaysFromBeginning the flag indicating if a cursor should be set
     *        to the beginning of checkpoint to start with, even if the cursor
     *        is currently in that checkpoint.
     * @param needsCheckpointEndMetaItem indicates the CheckpointEndMetaItem
     *        must not be skipped for the cursor.
     * @return true if the checkpoint to start with exists in the queue.
     */
    bool registerCursor(const std::string &name, uint64_t checkpointId,
                        bool alwaysFromBeginning,
                        MustSendCheckpointEnd needsCheckpointEndMetaItem);

    /**
     * Remove the cursor for a given connection.
     * @param name the name of a given connection
     * @return true if the cursor is removed successfully.
     */
    bool removeCursor(const std::string &name);

    /**
     * Get the Id of the checkpoint where the given connections cursor is currently located.
     * If the cursor is not found, return 0 as a checkpoint Id.
     * @param name the name of a given connection
     * @return the checkpoint Id for a given connections cursor.
     */
    uint64_t getCheckpointIdForCursor(const std::string &name);

    size_t getNumOfCursors();

    /**
     * Get info about all the cursors in this checkpoint manager.
     * Cursor names and corresponding MustSendCheckpointEnd flag are returned
     * as a list.
     * Note that return of info by copy is intended because after this call the
     * the chkpt manager can be deleted or reset
     *
     * @return std list of pair (name, MustSendCheckpointEnd)
     */
    checkpointCursorInfoList getAllCursors();

    /**
     * Queue an item to be written to persistent layer.
     * @param vb the vbucket that a new item is pushed into.
     * @param qi item to be persisted.
     * @param generateBySeqno yes/no generate the seqno for the item
     * @return true if an item queued increases the size of persistence queue by 1.
     */
    bool queueDirty(VBucket& vb, queued_item& qi,
                    const GenerateBySeqno generateBySeqno,
                    const GenerateCas generateCas);

    /*
     * Queue writing of the VBucket's state to persistent layer.
     * @param vb the vbucket that a new item is pushed into.
     */
    void queueSetVBState(VBucket& vb);

    /**
     * Return the next item to be sent to a given connection
     * @param name the name of a given connection
     * @param isLastMutationItem flag indicating if the item to be returned is
     * the last mutation one in the closed checkpoint.
     * @return the next item to be sent to a given connection.
     */
    queued_item nextItem(const std::string &name, bool &isLastMutationItem);

    snapshot_range_t getAllItemsForCursor(const std::string& name,
                                          std::vector<queued_item> &items);

    /**
     * Return the total number of items (including meta items) that belong to
     * this checkpoint manager.
     */
    size_t getNumItems() const {
        return numItems;
    }

    size_t getNumOpenChkItems();

    size_t getNumCheckpoints() const;

    /* Returns the count of Items (excluding meta items) that the given cursor
     * has yet to process (i.e. between the cursor's current position and the
     * end of the last checkpoint).
     */
    size_t getNumItemsForCursor(const std::string &name) const;

    void clear(vbucket_state_t vbState) {
        LockHolder lh(queueLock);
        clear_UNLOCKED(vbState, lastBySeqno);
    }

    /**
     * Clear all the checkpoints managed by this checkpoint manager.
     */
    void clear(RCPtr<VBucket> &vb, uint64_t seqno);

    /**
     * If a given cursor currently points to the checkpoint_end dummy item,
     * decrease its current position by 1. This function is mainly used for
     * checkpoint synchronization between the master and slave nodes.
     * @param name the name of a given connection
     */
    void decrCursorFromCheckpointEnd(const std::string &name);

    bool hasNext(const std::string &name);

    const CheckpointConfig &getCheckpointConfig() const {
        return checkpointConfig;
    }

    void addStats(ADD_STAT add_stat, const void *cookie);

    /**
     * Create a new open checkpoint by force.
     * @return the new open checkpoint id
     */
    uint64_t createNewCheckpoint();

    void resetCursors(checkpointCursorInfoList &cursors);

    /**
     * Get id of the previous checkpoint that is followed by the checkpoint
     * where the persistence cursor is currently walking.
     */
    uint64_t getPersistenceCursorPreChkId();

    /**
     * Update the checkpoint manager persistence cursor checkpoint offset
     */
    void itemsPersisted();

    /**
     * Return memory consumption of all the checkpoints managed
     */
    size_t getMemoryUsage_UNLOCKED();

    size_t getMemoryUsage();

    /**
     * Return memory consumption of unreferenced checkpoints
     */
    size_t getMemoryUsageOfUnrefCheckpoints();

    /**
     * Function returns a list of cursors to drop so as to unreference
     * certain checkpoints within the manager, invoked by the cursor-dropper.
     */
    std::vector<std::string> getListOfCursorsToDrop();

    /**
     * This method performs the following steps for creating a new checkpoint with a given ID i1:
     * 1) Check if the checkpoint manager contains any checkpoints with IDs >= i1.
     * 2) If exists, collapse all checkpoints and set the open checkpoint id to a given ID.
     * 3) Otherwise, simply create a new open checkpoint with a given ID.
     * This method is mainly for dealing with rollback events from a producer.
     * @param id the id of a checkpoint to be created.
     * @param vbucket vbucket of the checkpoint.
     */
    void checkAndAddNewCheckpoint(uint64_t id, VBucket& vbucket);

    bool closeOpenCheckpoint();

    void setBackfillPhase(uint64_t start, uint64_t end);

    void createSnapshot(uint64_t snapStartSeqno, uint64_t snapEndSeqno);

    void resetSnapshotRange();

    void updateCurrentSnapshotEnd(uint64_t snapEnd) {
        LockHolder lh(queueLock);
        checkpointList.back()->setSnapshotEndSeqno(snapEnd);
    }

    snapshot_info_t getSnapshotInfo();

    bool incrCursor(CheckpointCursor &cursor);

    void notifyFlusher() {
        if (flusherCB) {
            uint16_t vbid = vbucketId;
            flusherCB->callback(vbid);
        }
    }

    void setBySeqno(int64_t seqno) {
        LockHolder lh(queueLock);
        lastBySeqno = seqno;
    }

    int64_t getHighSeqno() const {
        LockHolder lh(queueLock);
        return lastBySeqno;
    }

    int64_t getLastClosedChkBySeqno() {
        LockHolder lh(queueLock);
        return lastClosedChkBySeqno;
    }

    int64_t nextBySeqno() {
        LockHolder lh(queueLock);
        return ++lastBySeqno;
    }

    void dump() const;

    static const std::string pCursorName;

protected:

    // Helper method for queueing methods - update the global and per-VBucket
    // stats after queueing a new item to a checkpoint.
    // Must be called with queueLock held (LockHolder passed in as argument to
    // 'prove' this).
    void updateStatsForNewQueuedItem_UNLOCKED(const LockHolder&,
                                     VBucket& vb, const queued_item& qi);

    /**
     * Helper method to update disk queue stats after (maybe) changing the
     * number of items remaining for the persistence cursor (for example after
     * adding / removing checkpoints.
     * @param vbucket VBucket whose stats should be updated.
     * @param curr_remains The number of items previously remaining for the
     *                     persistence cursor.
     * @param new_remains The number of items now remaining for the
     *                    persistence cursor.
     */
    void updateDiskQueueStats(VBucket& vbucket, size_t curr_remains,
                              size_t new_remains);

private:

    // Pair of {sequence number, cursor at checkpoint start} used when
    // updating cursor positions when collapsing checkpoints.
    struct CursorPosition {
        uint64_t seqno;
        bool onCpktStart;
    };

    // Map of cursor name to position. Used when updating cursor positions
    // when collapsing checkpoints.
    using CursorIdToPositionMap = std::map<std::string, CursorPosition>;

    bool removeCursor_UNLOCKED(const std::string &name);

    bool registerCursor_UNLOCKED(
                            const std::string &name,
                            uint64_t checkpointId,
                            bool alwaysFromBeginning,
                            MustSendCheckpointEnd needsCheckpointEndMetaItem);

    size_t getNumItemsForCursor_UNLOCKED(const std::string &name) const;

    void clear_UNLOCKED(vbucket_state_t vbState, uint64_t seqno);

    /**
     * Create a new open checkpoint and add it to the checkpoint list.
     * The lock should be acquired before calling this function.
     * @param id the id of a checkpoint to be created.
     */
    bool addNewCheckpoint_UNLOCKED(uint64_t id);

    bool addNewCheckpoint_UNLOCKED(uint64_t id,
                                   uint64_t snapStartSeqno,
                                   uint64_t snapEndSeqno);

    void removeInvalidCursorsOnCheckpoint(Checkpoint *pCheckpoint);

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

    bool closeOpenCheckpoint_UNLOCKED();

    bool isLastMutationItemInCheckpoint(CheckpointCursor &cursor);

    bool isCheckpointCreationForHighMemUsage(const VBucket& vbucket);

    void collapseClosedCheckpoints(std::list<Checkpoint*> &collapsedChks);

    void collapseCheckpoints(uint64_t id);

    void resetCursors(bool resetPersistenceCursor = true);

    void putCursorsInCollapsedChk(CursorIdToPositionMap& cursors,
                                  const std::list<Checkpoint*>::iterator chkItr);

    queued_item createCheckpointItem(uint64_t id, uint16_t vbid,
                                     queue_op checkpoint_op);

    /**
     * Return the number of meta Items remaining for this cursor.
     */
    size_t getNumOfMetaItemsFromCursor(const CheckpointCursor &cursor) const;

    EPStats                 &stats;
    CheckpointConfig        &checkpointConfig;
    mutable std::mutex       queueLock;
    const uint16_t           vbucketId;

    // Total number of items (including meta items) in /all/ checkpoints managed
    // by this object.
    std::atomic<size_t>      numItems;
    int64_t                  lastBySeqno;
    int64_t                  lastClosedChkBySeqno;
    std::list<Checkpoint*>   checkpointList;
    bool                     isCollapsedCheckpoint;
    uint64_t                 lastClosedCheckpointId;
    uint64_t                 pCursorPreCheckpointId;
    cursor_index             connCursors;

    FlusherCallback          flusherCB;

    friend std::ostream& operator<<(std::ostream& os, const CheckpointManager& m);
};

// Outputs a textual description of the CheckpointManager.
std::ostream& operator<<(std::ostream& os, const CheckpointManager& m);

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
          keepClosedCheckpoints(false),
          enableChkMerge(false),
          persistenceEnabled(true)
    { /* empty */ }

    CheckpointConfig(rel_time_t period, size_t max_items, size_t max_ckpts,
                     bool item_based_new_ckpt, bool keep_closed_ckpts,
                     bool enable_ckpt_merge, bool persistence_enabled)
        : checkpointPeriod(period),
          checkpointMaxItems(max_items),
          maxCheckpoints(max_ckpts),
          itemNumBasedNewCheckpoint(item_based_new_ckpt),
          keepClosedCheckpoints(keep_closed_ckpts),
          enableChkMerge(enable_ckpt_merge),
          persistenceEnabled(persistence_enabled) {}

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

    bool isCheckpointMergeSupported() const {
        return enableChkMerge;
    }

    bool isPersistenceEnabled() const {
        return persistenceEnabled;
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

    void allowCheckpointMerge(bool value) {
        enableChkMerge = value;
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
    // Flag indicating if merging closed checkpoints is enabled or not.
    bool enableChkMerge;

    // Flag indicating if persistence is enabled.
    bool persistenceEnabled;
};

#endif  // SRC_CHECKPOINT_H_
