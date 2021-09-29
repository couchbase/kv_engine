/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "checkpoint_iterator.h"
#include "checkpoint_types.h"
#include "ep_types.h"
#include "item.h"
#include <platform/monotonic.h>

#include <folly/Synchronized.h>
#include <folly/container/F14Map.h>
#include <memcached/engine_common.h>
#include <platform/non_negative_counter.h>
#include <optional>

#include <list>
#include <map>
#include <set>
#include <unordered_map>

#define MIN_CHECKPOINT_ITEMS 10
#define MAX_CHECKPOINT_ITEMS 50000
#define DEFAULT_CHECKPOINT_ITEMS 500

#define MIN_CHECKPOINT_PERIOD 1 //  1 sec.
#define MAX_CHECKPOINT_PERIOD 3600 // 3600 sec.
#define DEFAULT_CHECKPOINT_PERIOD 5 // 5 sec.

/**
 * The state of a given checkpoint.
 */
enum checkpoint_state {
    CHECKPOINT_OPEN, //!< The checkpoint is open.
    CHECKPOINT_CLOSED  //!< The checkpoint is not open.
};

const char* to_string(enum checkpoint_state);

// Iterator for the Checkpoint queue.  The iterator is templated on the
// queue type (CheckpointQueue).
using ChkptQueueIterator = CheckpointIterator<CheckpointQueue>;

class IndexEntry {
public:
    IndexEntry(const CheckpointQueue::iterator& it) : position(it) {
    }

    /**
     * Invalidate this index entry (as part of expelling) by pointing the
     * internal iterator to some special position provided by the user.
     *
     * @param it The new position pointed by this index entry.
     */
    void invalidate(const CheckpointQueue::iterator& it) {
        position = it;
    }

    CheckpointQueue::iterator getPosition() const {
        return position;
    }

private:
    CheckpointQueue::iterator position;
};

/**
 * The checkpoint index maps a key to a checkpoint index_entry.
 */
using CheckpointIndexKeyType = StoredDocKeyT<MemoryTrackingAllocator>;
using CheckpointIndexValueType =
        std::pair<const CheckpointIndexKeyType, IndexEntry>;

using checkpoint_index =
        folly::F14NodeMap<CheckpointIndexKeyType,
                          IndexEntry,
                          std::hash<CheckpointIndexKeyType>,
                          std::equal_to<>,
                          MemoryTrackingAllocator<CheckpointIndexValueType>>;

class Checkpoint;
class CheckpointManager;
class CheckpointConfig;
class CheckpointCursorIntrospector;
class CookieIface;
class Cursor;
class EPStats;
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
    friend class MockCheckpointManager;
    friend class CheckpointCursorIntrospector;

public:
    CheckpointCursor(std::string n,
                     CheckpointList::iterator checkpoint,
                     ChkptQueueIterator pos);

    // The implicitly generated copy-ctor would miss to increment the
    // checkpoint-cursor count, so we make sure that nobody can accidentally
    // invoke it.
    CheckpointCursor(const CheckpointCursor& other) = delete;
    CheckpointCursor& operator=(const CheckpointCursor& other) = delete;

    /**
     * Construct by copy and assign the new name.
     *
     * @param other
     * @param name The new name
     */
    CheckpointCursor(const CheckpointCursor& other, std::string name);

    ~CheckpointCursor();

    /// @returns the id of the current checkpoint the cursor is on
    uint64_t getId() const;

    /// @returns the type of the Checkpoint that the cursor is in
    CheckpointType getCheckpointType() const;

    /**
     * Invalidates this cursor. After invalidating this cursor it should not be
     * used.
     */
    void invalidate();

    bool valid() const {
        return isValid;
    }

    const StoredDocKey& getKey() const {
        return (*currentPos)->getKey();
    }

private:
    /**
     * Move the cursor's iterator back one if it is not currently pointing to
     * begin.  If pointing to begin then do nothing.
     */
    void decrPos();

    /**
     * Move the cursor's iterator forward. NOP if pointing to end.
     */
    void incrPos();

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

    /**
     * Is the cursor pointing to a valid checkpoint
     */
    bool isValid = true;

    friend bool operator<(const CheckpointCursor& a, const CheckpointCursor& b);
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

struct QueueDirtyResult {
    // Status of operation
    QueueDirtyStatus status;

    // Difference in bytes, of the queued_item sizes, if status is
    // SuccessExistingItem. Note, size includes value + key etc.
    ssize_t successExistingByteDiff{0};
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
 * and a new Open checkpoint created for new items. A Checkpoint may have a type
 * of Disk if it is created by a non-active vBucket when it receives a DCP Disk
 * snapshot; otherwise the Checkpoint has a type of Memory.
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
 *         seqno    0       1       1       2       3       4
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
 *         seqno    4       4       4       5       6
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
 * same sequence number as the associated op - for all meta operations this is
 * the ID of the following op.
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
 *        | 1001 | 1001 | 1002 | 1003 | 1004 | 1005 | 1006 |
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
 *         | 1001 | 1005 | 1006 |
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
 * Checkpoints call the provided memOverheadChangedCallback on any action that
 * changes the memory overhead of the checkpoint - that is, the memory required
 * _beyond_ that of the Items the Checkpoint holds. This occurs at
 * creation/destruction or when queuing new items.
 */
class Checkpoint {
    friend class CheckpointManagerTestIntrospector;

public:
    Checkpoint(CheckpointManager& manager,
               EPStats& st,
               uint64_t id,
               uint64_t snapStart,
               uint64_t snapEnd,
               uint64_t visibleSnapEnd,
               std::optional<uint64_t> highCompletedSeqno,
               Vbid vbid,
               CheckpointType checkpointType);

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
        return *checkpointState.rlock();
    }

    /**
     * Set the current state of this checkpoint.
     * @param state the checkpoint's new state
     */
    void setState(checkpoint_state state) {
        *checkpointState.wlock() = state;
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

    /**
     * @return The number of all cursors (ie, persistence and DCP) that reside
     *  in this Checkpoint
     */
    size_t getNumCursorsInCheckpoint() const {
        return numOfCursorsInCheckpoint;
    }

    /**
     * Queue an item to be written to persistent layer.
     * @param item the item to be persisted
     * @return a result indicating the status of the operation.
     */
    QueueDirtyResult queueDirty(const queued_item& qi);

    /**
     * @return true if the item can be de-duplicated, false otherwise
     */
    bool canDedup(const queued_item& existing, const queued_item& in) const;

    /**
     * Returns the first seqno available in this checkpoint for a cursor to pick
     * up. Used for registering cursors at the right position.
     * Logically the returned seqno is a different quantity depending on whether
     * expelling has modified the checkpoint queue:
     *
     * 1. Expel hasn't run -> that's the seqno of checkpoint_start
     * 2. Expel has run -> that's the seqno of the first item after the
     *    checkpoint_start
     */
    uint64_t getMinimumCursorSeqno() const;

    uint64_t getHighSeqno() const {
        auto pos = end();
        --pos;

        if ((*pos)->getOperation() == queue_op::checkpoint_end) {
            // We bump the seqno for checkpoint_end items so return the high
            // seqno of the last non-meta item (i.e. one less)
            return (*pos)->getBySeqno() - 1;
        }

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

    uint64_t getVisibleSnapshotEndSeqno() const {
        return visibleSnapEndSeqno;
    }

    void setSnapshotEndSeqno(uint64_t seqno, uint64_t visibleSnapEnd) {
        snapEndSeqno = seqno;
        visibleSnapEndSeqno = visibleSnapEnd;
    }

    void setCheckpointType(CheckpointType type) {
        checkpointType = type;
    }

    void setHighCompletedSeqno(std::optional<uint64_t> seqno) {
        highCompletedSeqno = seqno;
    }

    std::optional<uint64_t> getHighCompletedSeqno() const {
        return highCompletedSeqno;
    }

    /**
     * Tracks the seqno of the latest prepare queued.
     * @param seqno the seqno of the prepare which has been queued
     */
    void setHighPreparedSeqno(uint64_t seqno) {
        // assignment checks monotonicity
        highPreparedSeqno = seqno;
    }

    /**
     * Returns the seqno of the last prepare queued in the checkpoint.
     */
    std::optional<uint64_t> getHighPreparedSeqno() const {
        if (highPreparedSeqno != 0) {
            return highPreparedSeqno;
        }
        return std::nullopt;
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
        // All 3 indexes (preparedKey, committedKey and metaKey) share the
        // same allocator and therefore getting the bytes allocated for the
        // one will include the others.
        return sizeof(Checkpoint) +
               (committedKeyIndex.get_allocator().getBytesAllocated()) +
               (toWrite.get_allocator().getBytesAllocated()) +
               (keyIndexKeyTrackingAllocator).getBytesAllocated();
    }

    /**
     * Adds a queued_item to the checkpoint and updates the checkpoint stats
     * accordingly.
     * @param qi  The queued_iem to be added to the checkpoint.
     */
    void addItemToCheckpoint(const queued_item& qi);

    /**
     * Expel those items in the checkpoint where all cursors have passed.
     *
     * @param lastToExpel Iterator to the last item to expel (inclusive)
     * @return the expelled items and an estimate of the amount of memory
     *  released
     */
    std::pair<CheckpointQueue, size_t> expelItems(
            const ChkptQueueIterator& lastToExpel);

    /// @return true if this is a disk checkpoint (replica streaming from disk)
    bool isDiskCheckpoint() const {
        return checkpointType == CheckpointType::Disk;
    }

    CheckpointType getCheckpointType() const {
        return checkpointType;
    }

    /// @return the maximum 'deleted' rev-seq for this checkpoint (can be none)
    std::optional<uint64_t> getMaxDeletedRevSeqno() const {
        return maxDeletedRevSeqno;
    }

    /// @return bytes allocated to keyIndex/metaKeyIndex as a signed type
    ssize_t getKeyIndexAllocatorBytes() const {
        return ssize_t(keyIndexTrackingAllocator.getBytesAllocated());
    }

    /// @return bytes allocated to the toWrite as a signed type
    ssize_t getWriteQueueAllocatorBytes() const {
        return trackingAllocator.getBytesAllocated();
    }

    // see member variable definition for info
    size_t getQueuedItemsMemUsage() const {
        return queuedItemsMemUsage;
    }

    // see member variable definition for info
    size_t getKeyIndexMemUsage() const {
        return keyIndexMemUsage;
    }

    void addStats(const AddStatFn& add_stat, const CookieIface* cookie);

    /**
     * Remove association with a CheckpointManager.
     *
     * After this is called, stats will no longer be accounted against
     * the checkpoint manager, and the Checkpoint shall not have any further
     * items queued.
     */
    void detachFromManager();

    /**
     * Change where the memory usage of the keyIndex and queued items is
     * accounted against.
     *
     * The locally tracked values are unchanged, but the counters for the
     * previous owner (the CheckpointManager) are decreased by this Checkpoint's
     * usage, and the new counter is increased by the same value.
     *
     * Upon Checkpoint destruction, the new counter will be decreased, rather
     * than the old one.
     *
     * A null argument sets "no parent"; local stat updates will not be
     * reflected in a parent counter, until a subsequent non-null parent is
     * set.
     */
    void setMemoryTracker(
            cb::NonNegativeCounter<size_t>* newMemoryUsageTracker);

private:
    /**
     * Make a CheckpointIndexKey for inserting items into or finding items in
     * the key index(es).
     */
    CheckpointIndexKeyType makeIndexKey(const queued_item& item) const;

    // Pointer to the CheckpointManager that owns this Checkpoint.
    // Will be made null when removing the checkpoint from the manager.
    CheckpointManager* manager = nullptr;

    EPStats& stats;
    uint64_t                       checkpointId;
    uint64_t                       snapStartSeqno;
    uint64_t                       snapEndSeqno;

    /// The maximum visible snapshot end (hides prepare/abort), this could be
    /// a WeaklyMonotonic, but many unit tests will violate that.
    uint64_t visibleSnapEndSeqno = 0;
    /// The seqno of the highest expelled item.
    Monotonic<int64_t> highestExpelledSeqno{0};
    Vbid vbucketId;
    rel_time_t                     creationTime;
    folly::Synchronized<checkpoint_state> checkpointState;
    /// Number of non-meta items (see Item::isCheckPointMetaItem).
    size_t                         numItems;
    /// Number of meta items (see Item::isCheckPointMetaItem).
    size_t numMetaItems;

    // Count of the number of all cursors (ie persistence and DCP) that reside
    // in the checkpoint
    cb::NonNegativeCounter<size_t> numOfCursorsInCheckpoint = 0;

    // Allocator used for tracking memory used by toWrite
    MemoryTrackingAllocator<queued_item> trackingAllocator;
    // Allocator used for tracking memory used by keyIndex and metaKeyIndex
    checkpoint_index::allocator_type keyIndexTrackingAllocator;

    // Allocator used for tracking memory that we allocate for the keys of the
    // key and metaKey keyIndexes
    checkpoint_index::key_type::allocator_type keyIndexKeyTrackingAllocator;

    CheckpointQueue toWrite;

    /**
     * We want to allow prepares and commits to exist in the same checkpoint as
     * this simplifies replica code. This is because disk based snapshots can
     * contain both a prepare and a committed item against the same key. For
     * consistency, in memory snapshots should be able to do the same.
     * To do this, maintain two key indexes - one for prepared items and
     * one for committed items, allowing at most one of each type in a
     * single checkpoint.
     * Currently an abort exists in the same namespace as a prepare so we will
     * mimic that here and not allow prepares and aborts in the same checkpoint.
     */
    checkpoint_index committedKeyIndex;
    checkpoint_index preparedKeyIndex;

    /**
     * Helper class for local memory-counters that need to reflect their updates
     * on bucket-level EPStats.
     */
    class MemoryCounter {
    public:
        MemoryCounter(EPStats& stats,
                      cb::NonNegativeCounter<size_t>* parentUsage)
            : local(0), stats(stats), parentUsage(parentUsage) {
        }
        ~MemoryCounter();
        MemoryCounter& operator+=(size_t size);
        MemoryCounter& operator-=(size_t size);

        /**
         * Change where the local counter is aggregated.
         *
         * Checkpoints are owned by a CheckpointManager until they are
         * destroyed, so the parent will currently never need changing.
         *
         * TODO: MB-47462 Introduction of eager checkpoint removal will mean
         * checkpoints can be detached from a checkpoint manager.
         * changeParent can then be used to account the checkpoint memory
         * against a "detatched checkpoint" memory usage counter
         *
         */
        void changeParent(cb::NonNegativeCounter<size_t>* newParent);

        operator size_t() const {
            return local;
        }
    private:
        // Stores this checkpoint mem-usage
        cb::NonNegativeCounter<size_t> local;
        // Used to update the "global" bucket counter in EPStats
        EPStats& stats;
        // Pointer to a parent counter which needs updating when the
        // local value changes. Null indicates "no parent"
        cb::NonNegativeCounter<size_t>* parentUsage;
    };

    // Record the memory overhead of maintaining the keyIndex and metaKeyIndex.
    // This includes each item's key size and sizeof(index_entry).
    MemoryCounter keyIndexMemUsage;

    // Records the memory consumption of all items in the checkpoint queue.
    // For every item we include key, metadata and blob sizes.
    MemoryCounter queuedItemsMemUsage;

    // Is this a checkpoint created by a replica from a received disk snapshot?
    CheckpointType checkpointType;

    // The SyncRep HCS for this checkpoint. Used to ensure that we flush a
    // correct HCS at the end of a snapshot to disk. This is optional as it is
    // only necessary for Disk snapshot (due to de-dupe) and the way we retrieve
    // items from the CheckpointManager for memory snapshots makes it
    // non-trivial to send the HCS in memory snapshot markers.
    std::optional<uint64_t> highCompletedSeqno;

    // Tracks the seqno of the most recently queued prepare. Once this entire
    // checkpoint has been persisted, the state on disk definitely has a
    // state which could be warmed up and validly have this seqno as the
    // high prepared seqno.
    Monotonic<uint64_t> highPreparedSeqno{0};

    // queueDirty inspects each queued_item looking for isDeleted():true
    // this value tracks the largest rev seqno of those deleted items,
    // and allows the flusher to get the max value irrespective of
    // de-duplication.
    std::optional<uint64_t> maxDeletedRevSeqno;

    friend std::ostream& operator <<(std::ostream& os, const Checkpoint& m);
};
