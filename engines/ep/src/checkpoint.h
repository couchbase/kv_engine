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

#include "checkpoint_iterator.h"
#include "checkpoint_types.h"
#include "ep_types.h"
#include "item.h"
#include "monotonic.h"
#include "stats.h"

#include <platform/non_negative_counter.h>
#include <utilities/memory_tracking_allocator.h>

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
// de-duplication.  We template the list on a queued_item and our own
// memory allocator which allows memory usage to be tracked.
typedef std::list<queued_item, MemoryTrackingAllocator<queued_item>>
        CheckpointQueue;

// Iterator for the Checkpoint queue.  The iterator is templated on the
// queue type (CheckpointQueue).
using ChkptQueueIterator = CheckpointIterator<CheckpointQueue>;

/**
 * A checkpoint index entry.
 */
struct index_entry {
    ChkptQueueIterator position;
    int64_t mutation_id;
};

/**
 * The checkpoint index maps a key to a checkpoint index_entry.
 */
using checkpoint_index = std::unordered_map<
        StoredDocKey,
        index_entry,
        std::hash<StoredDocKey>,
        std::equal_to<StoredDocKey>,
        MemoryTrackingAllocator<std::pair<const StoredDocKey, index_entry>>>;

class Checkpoint;
class CheckpointManager;
class CheckpointConfig;
class Cursor;
class PreLinkDocumentContext;
class VBucket;

/**
 * A checkpoint cursor, representing the current position in a Checkpoint
 * series.
 *
 * CheckpointCursors are similar to STL-style iterators but for Checkpoints.
 * A consumer (DCP or persistence) will have one CheckpointCursor, initially
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
 */
class CheckpointCursor {
    friend class CheckpointManager;
    friend class Checkpoint;
public:

    CheckpointCursor(const std::string& n,
                     CheckpointList::iterator checkpoint,
                     ChkptQueueIterator pos)
        : name(n),
          currentCheckpoint(checkpoint),
          currentPos(pos),
          numVisits(0) {
    }

    // We need to define the copy construct explicitly due to the fact
    // that std::atomic implicitly deleted the assignment operator
    CheckpointCursor(const CheckpointCursor& other)
        : name(other.name),
          currentCheckpoint(other.currentCheckpoint),
          currentPos(other.currentPos),
          numVisits(other.numVisits.load()) {
    }

    CheckpointCursor &operator=(const CheckpointCursor &other) {
        name.assign(other.name);
        currentCheckpoint = other.currentCheckpoint;
        currentPos = other.currentPos;
        numVisits = other.numVisits.load();
        return *this;
    }

    /// @returns the id of the current checkpoint the cursor is on
    uint64_t getId() const;

private:
    /**
     * Move the cursor's iterator back one if it is not currently pointing to
     * begin.  If pointing to begin then do nothing.
     */
    void decrPos();

    /*
     * Calculate the number of items (excluding meta-items) remaining to be
     * processed in the checkpoint the cursor is currently in.
     *
     * @return number of items remaining to be processed.
     */
    size_t getRemainingItemsCount() const;

    std::string                      name;
    CheckpointList::iterator currentCheckpoint;

    // Specify the current position in the checkpoint
    ChkptQueueIterator currentPos;

    // Number of times a cursor has been moved or processed.
    std::atomic<size_t>              numVisits;

    friend std::ostream& operator<<(std::ostream& os, const CheckpointCursor& c);
};

std::ostream& operator<<(std::ostream& os, const CheckpointCursor& c);

/**
 * Result from invoking queueDirty in the current open checkpoint.
 */
enum class QueueDirtyStatus {
    /*
     * The item exists on the right hand side of the persistence cursor - i.e.
     * the persistence cursor has not yet processed this key.
     * The item will be deduplicated and doesn't change the size of the
     * checkpoint.
     */
    SuccessExistingItem,

    /**
     * The item exists on the left hand side of the persistence cursor - i.e.
     * the persistence cursor has already processed one (or more) instances of
     * this key.
     * It will be dedeuplicated and moved the to right hand side, but the item
     * needs to be re-persisted.
     */
    SuccessPersistAgain,

    /**
     * The item doesn't exist yet in the checkpoint. Adding this item will
     * increase the size of the checkpoint.
     */
    SuccessNewItem,

    /**
     * queueDirty failed - an item exists with the same key which cannot be
     * de-duplicated (for example a SyncWrite).
     */
    FailureDuplicateItem,
};

std::string to_string(QueueDirtyStatus value);

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
 *
 * Expelling
 * =========
 *
 * Items can be expelled (ejected from memory) from the oldest checkpoint that
 * still has cursors in it.  This can include the open checkpoint.
 *
 * Items are expelled items from checkpoints to reduce memory requirements.
 * This is achieved by identifying the oldest checkpoint that still has cursors
 * in it.  Then identifying where the first cursor is located. For example in
 * the diagram below it is seqno 1004.
 *
 *                 low                                high
 *          dummy seqno                              seqno
 *            |     |                                  |
 *            |     |                                  |
 *           \./   \./                                \./
 *        --------------------------------------------------
 *        | 1000 | 1001 | 1002 | 1003 | 1004 | 1005 | 1006 |
 *        --------------------------------------------------
 *                                      /.\           /.\
 *                                       |             |
 *                                       |             |
 *                                    cursor 1      cursor 2
 *
 * It then expels items from the start of the checkpoint upto and including
 * where the first cursor is located.  The cursor points to the location
 * that was last processed and therefore it is safe for the item pointed to
 * by the cursor to be expelled.
 *
 *
 * In the diagram this means expelling 1000, 1001, 1002, 1003 and 1004.
 * A new dummy is created at the position of where the last cursor pointed
 * and is assigned the same seqno as the previous dummy.
 *
 * After the expel operation the checkpoint in memory is as follows.
 *
 *           new   low     high
 *          dummy seqno   seqno
 *            |      |      |
 *            |      |      |
 *           \./    \./    \./
 *         ---------------------
 *         | 1000 | 1005 | 1006 |
 *         ---------------------
 *           /.\           /.\
 *            |             |
 *            |             |
 *         cursor 1      cursor 2
 *
 *
 * The number of items (queue_op::mutation) in the checkpoint remains unchanged
 * after expelling.  In the above example it means the checkpoint still contains
 * the original six items, as shown below:
 *
 *        -------------------------------------------
 *        | 1001 | 1002 | 1003 | 1004 | 1005 | 1006 |
 *        -------------------------------------------
 *                                /.\           /.\
 *                                 |             |
 *                                 |             |
 *                              cursor 1      cursor 2
 *
 * If a checkpoint contains a single mutation then it is not expelled.  Also
 * if the cursor is pointing to a meta-item the position to expel from is moved
 * backwards until either a mutation item or the dummy item is reached.
 *
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
    size_t getNumMetaItems() const {
        return numMetaItems;
    }

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
    void setState(checkpoint_state state) {
        LockHolder lh(lock);
        setState_UNLOCKED(state);
    }

    /**
     * Set the current state of this checkpoint.
     * @param state the checkpoint's new state
     */
    void setState_UNLOCKED(checkpoint_state state) {
        checkpointState = state;
    }

    void incNumOfCursorsInCheckpoint() {
        ++numOfCursorsInCheckpoint;
    }

    void decNumOfCursorsInCheckpoint() {
        --numOfCursorsInCheckpoint;
    }

    bool isNoCursorsInCheckpoint() const {
        return (numOfCursorsInCheckpoint == 0);
    }

    size_t getNumCursorsInCheckpoint() const {
        return numOfCursorsInCheckpoint;
    }

    /**
     * Queue an item to be written to persistent layer.
     * @param item the item to be persisted
     * @param checkpointManager the checkpoint manager to which this checkpoint belongs
     * @param bySeqno the by sequence number assigned to this mutation
     * @return a result indicating the status of the operation.
     */
    QueueDirtyStatus queueDirty(const queued_item& qi,
                                CheckpointManager* checkpointManager);

    /**
     * Returns the seqno of a non-expelled (and non-dummy) item that has the
     * lowest seqno.
     */
    uint64_t getLowSeqno() const {
        auto pos = begin();
        // Skip passed the dummy item
        ++pos;
        return (*pos)->getBySeqno();
    }

    uint64_t getHighSeqno() const {
        auto pos = end();
        --pos;
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

    /**
     * Returns an iterator pointing to the beginning of the CheckpointQueue,
     * toWrite.
     */
    ChkptQueueIterator begin() const {
        return ChkptQueueIterator(const_cast<CheckpointQueue&>(toWrite),
                                  ChkptQueueIterator::Position::begin);
    }

    /**
     * Returns an iterator pointing to the 'end' of the CheckpointQueue,
     * toWrite.
     */
    ChkptQueueIterator end() const {
        return ChkptQueueIterator(const_cast<CheckpointQueue&>(toWrite),
                                  ChkptQueueIterator::Position::end);
    }

    /**
     * Returns the memory held by the checkpoint, which is the sum of the
     * memory used by all items held in the checkpoint plus the checkpoint
     * overhead.
     */
    size_t getMemConsumption() const {
        return queuedItemsMemUsage + getMemoryOverhead();
    }

    /**
     * Returns the overhead of the checkpoint.
     * This is comprised of three components:
     * 1) The size of the Checkpoint object
     * 2) The keyIndex / metaKeyIndex usage
     * 3) The size of the ref-counted pointer instances (queued_item) in the
     *    container.
     *
     * When it comes to cursor dropping, this is the theoretical guaranteed
     * memory which can be freed, as the checkpoint contains the only
     * references to them.
     */
    size_t getMemoryOverhead() const {
        // The keyIndex and metaKeyIndex share the same allocator and therefore
        // getting the bytes allocated for the keyIndex map will also include
        // the metaKeyIndex map.
        return sizeof(Checkpoint) +
               *(keyIndex.get_allocator().getBytesAllocated()) +
               *(toWrite.get_allocator().getBytesAllocated());
    }

    /**
     * Adds a queued_item to the checkpoint and updates the checkpoint stats
     * accordingly.
     * @param qi  The queued_iem to be added to the checkpoint.
     */
    void addItemToCheckpoint(const queued_item& qi);

    /**
     * Expel those items in the checkpoint where all cursors have passed.
     * @param expelUpToAndIncluding  a cursor pointing where we will expel
     *                               upto and including.
     * @return  a CheckpointQueue of items that have been expelled.
     */
    CheckpointQueue expelItems(CheckpointCursor& expelUpToAndIncluding);

private:
    EPStats                       &stats;
    uint64_t                       checkpointId;
    uint64_t                       snapStartSeqno;
    uint64_t                       snapEndSeqno;
    /// The seqno of the highest expelled item.
    Monotonic<int64_t> highestExpelledSeqno = 0;
    Vbid vbucketId;
    rel_time_t                     creationTime;
    checkpoint_state               checkpointState;
    /// Number of non-meta items (see Item::isCheckPointMetaItem).
    size_t                         numItems;
    /// Number of meta items (see Item::isCheckPointMetaItem).
    size_t numMetaItems;

    // Count of the number of cursors that reside in the checkpoint
    cb::NonNegativeCounter<size_t> numOfCursorsInCheckpoint = 0;

    // Allocator used for tracking memory used by the CheckpointQueue
    MemoryTrackingAllocator<queued_item> trackingAllocator;
    // Allocator used for tracking memory used by the CheckpointQueue
    checkpoint_index::allocator_type keyIndexTrackingAllocator;
    CheckpointQueue toWrite;
    checkpoint_index               keyIndex;
    /* Index for meta keys like "dummy_key" */
    checkpoint_index               metaKeyIndex;

    // Record the memory overhead of maintaining the keyIndex and metaKeyIndex.
    // This includes each item's key size and sizeof(index_entry).
    cb::NonNegativeCounter<size_t> keyIndexMemUsage;
    // Records the memory consumption of all items in the checkpoint.
    // This includes each item's key, metadata and the blob.
    cb::NonNegativeCounter<size_t> queuedItemsMemUsage;
    mutable std::mutex lock;

    friend std::ostream& operator <<(std::ostream& os, const Checkpoint& m);
};
