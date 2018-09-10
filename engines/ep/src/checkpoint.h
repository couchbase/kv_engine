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

#pragma once

#include "config.h"

#include "ep_types.h"
#include "item.h"
#include "stats.h"

#include <list>
#include <map>
#include <set>
#include <unordered_map>

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

/**
 * The checkpoint index maps a key to a checkpoint index_entry.
 */
typedef std::unordered_map<StoredDocKey, index_entry> checkpoint_index;

class Checkpoint;
class CheckpointManager;
class CheckpointConfig;
class PreLinkDocumentContext;
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

    CheckpointCursor(const std::string& n)
        : name(n),
          currentCheckpoint(),
          currentPos(),
          offset(0),
          ckptMetaItemsRead(0),
          fromBeginningOnChkCollapse(false) {
    }

    /**
     * @param offset_ Count of items (normal+meta) already read for *all*
     *                checkpoints in the series.
     * @param meta_items_read Count of meta_items already read for the
     *                        given checkpoint.
     */
    CheckpointCursor(const std::string& n,
                     CheckpointList::iterator checkpoint,
                     CheckpointQueue::iterator pos,
                     size_t offset_,
                     size_t meta_items_read,
                     bool beginningOnChkCollapse)
        : name(n),
          currentCheckpoint(checkpoint),
          currentPos(pos),
          numVisits(0),
          offset(offset_),
          ckptMetaItemsRead(meta_items_read),
          fromBeginningOnChkCollapse(beginningOnChkCollapse) {
    }

    // We need to define the copy construct explicitly due to the fact
    // that std::atomic implicitly deleted the assignment operator
    CheckpointCursor(const CheckpointCursor& other)
        : name(other.name),
          currentCheckpoint(other.currentCheckpoint),
          currentPos(other.currentPos),
          numVisits(other.numVisits.load()),
          offset(other.offset.load()),
          ckptMetaItemsRead(other.ckptMetaItemsRead),
          fromBeginningOnChkCollapse(other.fromBeginningOnChkCollapse) {
    }

    CheckpointCursor &operator=(const CheckpointCursor &other) {
        name.assign(other.name);
        currentCheckpoint = other.currentCheckpoint;
        currentPos = other.currentPos;
        numVisits = other.numVisits.load();
        offset.store(other.offset.load());
        setMetaItemOffset(other.ckptMetaItemsRead);
        fromBeginningOnChkCollapse = other.fromBeginningOnChkCollapse;
        return *this;
    }

    /**
     * Decrement the offsets for this cursor.
     * @param items Count of all items (meta and non-meta) to decrement by.
     */
    void decrOffset(size_t decr);

    void decrPos();

    /**
     * Return the count of meta items processed (i.e. moved past) for the
     * current checkpoint.
     * This value is reset to zero when a new checkpoint
     * is entered.
     */
    size_t getCurrentCkptMetaItemsRead() const;

    /// @returns the id of the current checkpoint the cursor is on
    uint64_t getId() const;

protected:
    void incrMetaItemOffset(size_t incr) {
        ckptMetaItemsRead += incr;
    }

    void setMetaItemOffset(size_t val) {
        ckptMetaItemsRead = val;
    }

private:
    std::string                      name;
    CheckpointList::iterator currentCheckpoint;
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

    friend std::ostream& operator<<(std::ostream& os, const CheckpointCursor& c);
};

std::ostream& operator<<(std::ostream& os, const CheckpointCursor& c);

/**
 * Result from invoking queueDirty in the current open checkpoint.
 */
enum class queue_dirty_t {
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

std::string to_string(queue_dirty_t value);

/**
 * Representation of a checkpoint used in the unified queue for persistence and
 * replication.
 *
 * Each Checkpoint consists of an ordered series of queued_item items, each
 * of which either represents a 'real' user operation
 * (queue_op::mutation), or one of a range of meta-items
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
 * Note that sequence numbers are only unique for normal operations (mutation)
 * and system events - for meta-items like checkpoint start/end they share the
 * same sequence number as the associated op - for checkpoint_start that is the
 * ID of the following op, for checkpoint_end the ID of the proceeding op.
 */
class Checkpoint {
public:
    Checkpoint(EPStats& st,
               uint64_t id,
               uint64_t snapStart,
               uint64_t snapEnd,
               Vbid vbid);

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
    rel_time_t getCreationTime() const {
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
        LockHolder lh(lock);
        return getState_UNLOCKED();
    }

    checkpoint_state getState_UNLOCKED() const {
        return checkpointState;
    }

    /**
     * Set the current state of this checkpoint.
     * @param state the checkpoint's new state
     */
    void setState(checkpoint_state state);

    /**
     * Set the current state of this checkpoint.
     * @param state the checkpoint's new state
     */
    void setState_UNLOCKED(checkpoint_state state);

    /**
     * Return the number of cursors that are currently walking through this checkpoint.
     */
    size_t getNumberOfCursors() const {
        LockHolder lh(lock);
        return cursors.size();
    }

    /**
     * Register a cursor's name to this checkpoint
     */
    void registerCursorName(const std::string &name) {
        LockHolder lh(lock);
        cursors.insert(name);
    }

    /**
     * Remove a cursor's name from this checkpoint
     */
    void removeCursorName(const std::string &name) {
        LockHolder lh(lock);
        cursors.erase(name);
    }

    /**
     * Return true if the cursor with a given name exists in this checkpoint
     */
    bool hasCursorName(const std::string &name) const {
        LockHolder lh(lock);
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

    uint64_t getSnapshotStartSeqno() const {
        return snapStartSeqno;
    }

    void setSnapshotStartSeqno(uint64_t seqno) {
        snapStartSeqno = seqno;
    }

    uint64_t getSnapshotEndSeqno() const {
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
    size_t getMemConsumption() const {
        return effectiveMemUsage;
    }

    /**
     * Returns the overhead of the checkpoint. For each item in the checkpoint,
     * this is the sum of key size + sizeof(index_entry) + sizeof(queued_item).
     * When it comes to cursor dropping, this is the theoretical guaranteed
     * memory which can be freed, as the checkpoint contains the only references
     * to them.
     */
    size_t getMemoryOverhead() const {
        return memOverhead;
    }

private:
    EPStats                       &stats;
    uint64_t                       checkpointId;
    uint64_t                       snapStartSeqno;
    uint64_t                       snapEndSeqno;
    Vbid vbucketId;
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
    mutable std::mutex lock;

    friend std::ostream& operator <<(std::ostream& os, const Checkpoint& m);
};
