/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "checkpoint_cursor.h"
#include "checkpoint_types.h"
#include "cursor.h"
#include "ep_types.h"
#include "queue_op.h"
#include "stats.h"
#include "utilities/testing_hook.h"
#include "vbucket_types.h"
#include <platform/monotonic.h>

#include <memcached/engine_common.h>
#include <memcached/vbucket.h>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>

class Checkpoint;
class CheckpointConfig;
class CheckpointCursor;
class EPStats;
class PreLinkDocumentContext;
class VBucket;

template <typename... RV>
class Callback;

/**
 * snapshot_range_t + a HCS for flushing to disk from Disk checkpoints which
 * is required as we can't work out a correct PCS on a replica due to de-dupe.
 */
struct CheckpointSnapshotRange {
    // Getters for start and end to allow us to use this in the same way as a
    // normal snapshot_range_t
    uint64_t getStart() const {
        return range.getStart();
    }
    uint64_t getEnd() const {
        return range.getEnd();
    }

    snapshot_range_t range;

    // HCS that should be flushed. Currently should only be set for Disk
    // Checkpoint runs.
    std::optional<uint64_t> highCompletedSeqno = {};
    // HPS that should be flushed when the entire range has been persisted.
    // This is the seqno of the latest prepare in this checkpoint.
    std::optional<uint64_t> highPreparedSeqno = {};
};

/**
 * Representation of a checkpoint manager that maintains the list of checkpoints
 * for a given vbucket.
 */
class CheckpointManager {
    friend class Checkpoint;
    friend class CheckpointBench;
    friend class CheckpointManagerTestIntrospector;
    friend class CheckpointTest;
    friend class Consumer;
    friend class EventuallyPersistentEngine;

public:
    using FlusherCallback = std::shared_ptr<Callback<Vbid>>;

    /// Return type of getNextItemsForCursor()
    struct ItemsForCursor {
        ItemsForCursor() = default;
        ItemsForCursor(CheckpointType checkpointType,
                       std::optional<uint64_t> maxDeletedRevSeqno,
                       std::optional<uint64_t> highCompletedSeqno,
                       uint64_t visibleSeqno)
            : checkpointType(checkpointType),
              maxDeletedRevSeqno(maxDeletedRevSeqno),
              highCompletedSeqno(highCompletedSeqno),
              visibleSeqno(visibleSeqno) {
        }
        std::vector<CheckpointSnapshotRange> ranges;
        bool moreAvailable = {false};

        /**
         * The natural place for this is CheckpointSnapshotRange, as this is per
         * snapshot. Originally placed here as CM::getNextItemsForCursor() never
         * returns multiple snapshots of different types.
         */
        CheckpointType checkpointType = CheckpointType::Memory;

        std::optional<uint64_t> maxDeletedRevSeqno = {};

        /**
         * HCS that must be sent to Replica when the Active is streaming a
         * Disk Checkpoint. The same as checkpoint-type, the natural place for
         * this is CheckpointSnapshotRange, where we already have it.
         *
         * I am not re-using the member in ranges for:
         * 1) keeping this change smaller, as the code in ActiveStream would
         *   require changes for dealing with it
         * 2) highlighing the fact that here we expect a *single* range when we
         *   use this member
         *
         * The correctness of the latter is ensured by the fact that
         * CM::getNextItemsForCursor() never returns multiple Disk Checkpoints.
         *
         * @todo: This member should be removed (and the one in SnapRange used)
         * as soon as we refactor the DCP stream code in CheckpointManager and
         * ActiveStream.
         */
        std::optional<uint64_t> highCompletedSeqno;

        /**
         * The max visible seqno for first Checkpoint returned, e.g. if multiple
         * Checkpoints are returned as follows:
         *   cp1[mutation:1, prepare:2] cp2[mutation:3,mutation:4]
         * This value would be 1
         */
        uint64_t visibleSeqno;

        /// Set only for persistence cursor, resets the CM state after flush.
        UniqueFlushHandle flushHandle;
    };

    // Used as return type of functions responsible for memory releasing.
    struct ReleaseResult {
        // Used to store the number of elements released
        size_t count = {0};
        // Used to store the number of bytes released
        size_t memory = {0};
    };

    /**
     * Construct a Checkpoint manager, tracking items queued for Vbucket @p vb.
     *
     *
     * @param st EPStats instance to record checkpoint stats in
     * @param vb the vbucket this manager is to be associated with
     * @param config checkpoint config, possibly built from the engine config
     * @param lastSeqno highest existing seqno, new queued items will start
     *                  at lastSeqno + 1
     * @param lastSnapStart most recent snapshot start seqno
     * @param lastSnapEnd most recent snapshot end seqnp
     * @param maxVisibleSeqno highest seqno of a committed item at the time
     *                        of construction
     * @param maxPrepareSeqno highest seqno of a prepare at time of
     *                        construction, set from PPS at warmup.
     * @param cb flusher callback, used to trigger the flusher after items have
     *           been queued
     */
    CheckpointManager(EPStats& st,
                      VBucket& vb,
                      CheckpointConfig& config,
                      int64_t lastSeqno,
                      uint64_t lastSnapStart,
                      uint64_t lastSnapEnd,
                      uint64_t maxVisibleSeqno,
                      uint64_t maxPrepareSeqno,
                      FlusherCallback cb);

    virtual ~CheckpointManager();

    uint64_t getOpenCheckpointId() const;

    CheckpointType getOpenCheckpointType() const;

    /**
     * Attempt to expel (i.e. eject from memory) items in the oldest checkpoint
     * that still has cursor registered in it.  This is to help avoid very large
     * checkpoints which consume a large amount of memory.
     *
     * @return ReleaseResult, with the number of items expelled and an estimate
     *  of released memory
     */
    ReleaseResult expelUnreferencedCheckpointItems();

    /**
     * Register the cursor for getting items whose bySeqno values are between
     * startBySeqno and endBySeqno, and close the open checkpoint if endBySeqno
     * belongs to the open checkpoint.
     *
     * @param name of the new cursor.
     * @param startBySeqno The seqno where to place the cursor
     * @param droppable Whether the new cursor can be removed by cursor-dropping
     * @return CursorRegResult, see struct for details
     */
    CursorRegResult registerCursorBySeqno(
            const std::string& name,
            uint64_t startBySeqno,
            CheckpointCursor::Droppable droppable);

    /**
     * Remove the cursor for a given connection.
     * @param cursor, ref to the clients cursor to be removed from the
     * CheckpointManager and so currentCheckpoint member can be set to
     * checkpointList.end() to prevent further use of currentCheckpoint
     * iterator.
     * @return true if the cursor is removed successfully.
     */
    bool removeCursor(CheckpointCursor& cursor);

    /**
     * Removes the backup persistence cursor created at getItemsForCursor().
     */
    void removeBackupPersistenceCursor();

    /**
     * Moves the pcursor back to the backup cursor.
     * Note that:
     *  1) it is logical move, the function has constant complexity
     *  2) the backup cursor is logically removed (as it becomes the new
     *     pcursor)
     * @return aggregated flush stats to roll back the VBucket counters by
     */
    AggregatedFlushStats resetPersistenceCursor();

    /**
     * Queue an item to be written to persistent layer.
     *
     * @param qi item to be persisted.
     * @param generateBySeqno yes/no generate the seqno for the item
     * @param preLinkDocumentContext A context object needed for the
     *        pre link document API in the server API. It is notified
     *        with the generated CAS before the object is made available
     *        for other threads. May be nullptr if the document originates
     *        from a context where the document shouldn't be updated.
     * @param assignedSeqnoCallback a function that is called with the seqno
     *        of the item. This is called with the queueLock held.
     * @return true if an item queued increases the size of persistence queue
     *        by 1.
     */
    bool queueDirty(queued_item& qi,
                    const GenerateBySeqno generateBySeqno,
                    const GenerateCas generateCas,
                    PreLinkDocumentContext* preLinkDocumentContext,
                    std::function<void(int64_t)> assignedSeqnoCallback = {});

    /*
     * Queue writing of the VBucket's state to persistent layer.
     */
    void queueSetVBState();

    /**
     * Add all outstanding items for the given cursor name to the vector. Only
     * fetches items for contiguous Checkpoints of the same type.
     *
     * @param cursor CheckpointCursor to read items from and advance
     * @param items container which items will be appended to.
     * @return The low/high sequence number added to `items` on success,
     *         or (0,0) if no items were added.
     */
    CheckpointManager::ItemsForCursor getNextItemsForCursor(
            CheckpointCursor& cursor, std::vector<queued_item>& items);

    /**
     * Add all outstanding items for persistence to the vector. Only fetches
     * items for contiguous Checkpoints of the same type.
     *
     * @param items container which items will be appended to.
     * @return The low/high sequence number added to `items` on success,
     *         or (0,0) if no items were added.
     */
    CheckpointManager::ItemsForCursor getNextItemsForPersistence(
            std::vector<queued_item>& items) {
        Expects(persistenceCursor != nullptr);
        return getNextItemsForCursor(*persistenceCursor, items);
    }

    /**
     * Add items for the given cursor to the vector, stopping on a checkpoint
     * boundary which is greater or equal to `approxLimit`. The cursor is
     * advanced to point after the items fetched.
     *
     * Can fetch items of contiguous Memory Checkpoints.
     * Never fetches (1) items of contiguous Disk checkpoints or (2) items of
     * checkpoints of different types.
     *
     * Note: It is only valid to fetch complete checkpoints; as such we cannot
     * limit to a precise number of items.
     *
     * @param cursor CheckpointCursor to read items from and advance
     * @param[in/out] items container which items will be appended to.
     * @param approxLimit Approximate number of items to add.
     * @return An ItemsForCursor object containing:
     * range: the low/high sequence number of the checkpoints(s) added to
     * `items`;
     * moreAvailable: true if there are still items available for this
     * checkpoint (i.e. the limit was hit).
     * maxDeletedRevSeqno for the items returned
     * highCompletedSeqno for the items returned
     * maxVisibleSeqno initialised to that of the first checkpoint, this works
     *                 for the ActiveStream use-case who just needs a single
     *                 value to seed it's snapshot loop.
     */
    ItemsForCursor getItemsForCursor(CheckpointCursor& cursor,
                                     std::vector<queued_item>& items,
                                     size_t approxLimit);

    /**
     * Add items for persistence to the vector, stopping on a checkpoint
     * boundary which is greater or equal to `approxLimit`. The persistence
     * cursor is advanced to point after the items fetched. Only fetches
     * items for contiguous Checkpoints of the same type.
     *
     * Note: It is only valid to fetch complete checkpoints; as such we cannot
     * limit to a precise number of items.
     *
     * @param[in/out] items container which items will be appended to.
     * @param approxLimit Approximate number of items to add.
     * @return An ItemsForCursor object containing:
     * range: the low/high sequence number of the checkpoints(s) added to
     * `items`;
     * moreAvailable: true if there are still items available for this
     * checkpoint (i.e. the limit was hit).
     */
    ItemsForCursor getItemsForPersistence(std::vector<queued_item>& items,
                                          size_t approxLimit) {
        Expects(persistenceCursor);
        return getItemsForCursor(*persistenceCursor, items, approxLimit);
    }

    /**
     * Return the total number of items (including meta items) that belong to
     * this checkpoint manager.
     */
    size_t getNumItems() const {
        return numItems;
    }

    /**
     * Returns the number of non-meta items in the currently open checkpoint.
     */
    size_t getNumOpenChkItems() const;

    /* WARNING! This method can return inaccurate counts - see MB-28431. It
     * at *least* can suffer from overcounting by at least 1 (in scenarios as
     * yet not clear).
     * As such it is *not* safe to use when a precise count of remaining
     * items is needed.
     *
     * Returns the count of Items (excluding meta items) that the given cursor
     * has yet to process (i.e. between the cursor's current position and the
     * end of the last checkpoint).
     */
    size_t getNumItemsForCursor(const CheckpointCursor* cursor) const;

    /* WARNING! This method can return inaccurate counts - see MB-28431. It
     * at *least* can suffer from overcounting by at least 1 (in scenarios as
     * yet not clear).
     * As such it is *not* safe to use when a precise count of remaining
     * items is needed.
     *
     * Returns the count of Items (excluding meta items) that the persistence
     * cursor has yet to process (i.e. between the cursor's current position and
     * the end of the last checkpoint).
     */
    size_t getNumItemsForPersistence() const {
        return getNumItemsForCursor(persistenceCursor);
    }

    /**
     * Clears all the checkpoints managed by this checkpoint manager and reset
     * seqnos to the given seqno.
     * If the optional seqno arg is omitted, then CM::lastBySeqno is used for
     * resetting seqnos.
     *
     * @param seqno (optional) The high-seqno to set for the cleared CM.
     */
    void clear(std::optional<uint64_t> seqno = {});

    const CheckpointConfig &getCheckpointConfig() const {
        return checkpointConfig;
    }

    void addStats(const AddStatFn& add_stat, const CookieIface* cookie);

    /**
     * Create a new open checkpoint by force.
     *
     * @return the new open checkpoint id
     */
    uint64_t createNewCheckpoint();

    /**
     * @return the memory usage of all the checkpoints managed
     */
    size_t getMemUsage() const;

    /**
     * Return the mem usage of all queued items in all checkpoints
     */
    size_t getQueuedItemsMemUsage() const;

    /**
     * Return memory overhead of all the checkpoints managed, computed by
     * allocators
     */
    size_t getMemOverheadAllocatorBytes() const;

    /**
     * Return the mem overhead of this CM checkpoints' queue struct, computed by
     * allocator
     */
    size_t getMemOverheadAllocatorBytesQueue() const;

    /**
     * Return the mem overhead of this CM checkpoints' index struct, computed by
     * allocator
     */
    size_t getMemOverheadAllocatorBytesIndex() const;

    /**
     * Return memory overhead of all the checkpoints managed, computed by
     * internal counters
     */
    size_t getMemOverhead() const;

    /**
     * Return the mem overhead of this CM checkpoints' queue struct, computed by
     * internal counters
     */
    size_t getMemOverheadQueue() const;

    /**
     * Return of the memory overhead of the key index in all checkpoints. That
     * includes both internal index struct and allocs for keys in the index.
     */
    size_t getMemOverheadIndex() const;

    /**
     * Function returns a list of cursors to drop so as to unreference
     * certain checkpoints within the manager, invoked by the cursor-dropper.
     * @return a container of weak_ptr to cursors
     */
    std::vector<Cursor> getListOfCursorsToDrop();

    void createSnapshot(uint64_t snapStartSeqno,
                        uint64_t snapEndSeqno,
                        std::optional<uint64_t> highCompletedSeqno,
                        CheckpointType checkpointType,
                        uint64_t maxVisibleSnapEnd);

    /**
     * Extend the open checkpoint to contain more mutations. Allowed only for
     * Memory checkpoints.
     * Note:
     * 1) We forbid merging of checkpoints of different type for multiple
     * reasons (eg, MB-42780).
     * 2) Extending a Disk checkpoint would be theoretically possible, but the
     * function doesn't support it (eg, we would need to update other quantities
     * like the HCS). Adding support for that doesn't seem necessary. The
     * original idea behind "extending a checkpoint" is that under load the
     * active may send many/tiny snapshots. Creating a checkpoint for every
     * snapshot would be unnecessarily expensive at runtime and also we would
     * end up quickly with a huge CheckpointList, which would degrade the
     * performance of some code-paths in the CM (eg, checkpoint removal).
     *
     * @param snapEnd
     * @param maxVisibleSnapEnd
     * @throws std::logic_error If the user tries to extend a Disk checkpoint
     */
    void extendOpenCheckpoint(uint64_t snapEnd, uint64_t maxVisibleSnapEnd);

    snapshot_info_t getSnapshotInfo();

    uint64_t getOpenSnapshotStartSeqno() const;

    /**
     * Return the visible end seqno for the current snapshot. This logically
     * matches the end which would be returned by getSnapshotInfo, but for the
     * visible end.
     *
     * @return The end seqno for the current snapshot. For replication, if only
     *          a marker has been received, the value returned is for the prev
     *          complete snapshot.
     */
    uint64_t getVisibleSnapshotEndSeqno() const;

    void notifyFlusher();

    int64_t getHighSeqno() const;

    uint64_t getMaxVisibleSeqno() const;

    /// @return the persistence cursor which can be null
    CheckpointCursor* getPersistenceCursor() const {
        return persistenceCursor;
    }

    /// @return the backup-pcursor
    std::shared_ptr<CheckpointCursor> getBackupPersistenceCursor();

    void dump() const;

    /**
     * Take the cursors from another checkpoint manager and reset them in the
     * process - used as part of vbucket reset.
     * @param other the manager we are taking cursors from
     */
    void takeAndResetCursors(CheckpointManager& other);

    /// @return true if the current open checkpoint is a DiskCheckpoint
    bool isOpenCheckpointDisk();

    /// @return true if the current open checkpoint is an InitialDisk
    /// checkpoint.
    bool isOpenCheckpointInitialDisk();

    void updateStatsForStateChange(vbucket_state_t from, vbucket_state_t to);

    /**
     * Sets the callback to be invoked whenever memory usage changes due to a
     * new queued item or checkpoint removal (or checkpoint expelling, in
     * versions this is implemented in). This allows changes in checkpoint
     * memory usage to be monitored.
     */
    void setOverheadChangedCallback(
            std::function<void(int64_t delta)> callback);

    /**
     * Gets the callback to be invoked whenever memory usage changes due to a
     * new queued item or checkpoint removal (or checkpoint expelling, in
     * versions this is implemented in).
     */
    std::function<void(int64_t delta)> getOverheadChangedCallback() const;

    /**
     * @return The number of checkpoints currently managed by this CM.
     */
    size_t getNumCheckpoints() const;

    /**
     * Returns whether the is some non-meta item to process for the given
     * cursor.
     *
     * Note: Function non-const as it potentially creates a temporary cursor
     * and moves it by re-using existing/non-const functions.
     *
     * @param cursor
     */
    bool hasNonMetaItemsForCursor(const CheckpointCursor& cursor);

    // @return the number of cursors registered in all checkpoints
    size_t getNumCursors() const;

    // @return the amount of memory (in bytes) released by ItemExpel
    size_t getMemFreedByItemExpel() const;

    size_t getMemFreedByCheckpointRemoval() const;

    /**
     * Checks if the CM state pre-conditions for creating a new checkpoint are
     * met and proceeds trying creating a new checkpoint.
     */
    void maybeCreateNewCheckpoint();

protected:
    /**
     * @param lh, the queueLock held
     * @return The number of checkpoints currently managed by this CM.
     */
    size_t getNumCheckpoints(std::lock_guard<std::mutex>& lh) const;

    /**
     * @param lh, the queueLock held
     * @return memory overhead of all the checkpoints managed, computed by
     * internal counters
     */
    size_t getMemOverhead(std::lock_guard<std::mutex>& lh) const;

    /**
     * @param lh, the queueLock currently held
     * @return the memory usage of all the checkpoints managed
     */
    size_t getMemUsage(std::lock_guard<std::mutex>& lh) const;

    /**
     * Checks if the given checkpoint is:
     *  * the oldest checkpoint
     *  * closed
     *  * unreferenced
     */
    bool isEligibleForRemoval(const Checkpoint& checkpoint) const;

    /**
     * Checks if the provided checkpoint is eligible for removal (see
     * isEligibleForRemoval(...)) and if so, removes it and schedules it for
     * destruction on a background task.
     */
    void maybeScheduleDestruction(Checkpoint& checkpoint);

    /**
     * Schedules the provided checkpoints for destruction on a background task.
     */
    void scheduleDestruction(CheckpointList&& toRemove);

    /**
     * Advance the given cursor. Protected as it's valid to call this from
     * getItemsForCursor but not from anywhere else (as it will return an entire
     * checkpoint and never leave a cursor placed at the checkpoint_end).
     *
     * Note: This function skips empty items. If the cursor moves into a new
     * checkpoint, then after this call it will point to the checkpoint_start
     * item into the new checkpoint.
     *
     * @return true if advanced, false otherwise
     */
    bool incrCursor(CheckpointCursor& cursor);

    /**
     * @param lh Lock to CM::queueLock
     * @return the id of the open checkpoint
     */
    uint64_t getOpenCheckpointId(const std::lock_guard<std::mutex>& lh) const;

    /**
     * Helper method for queueing methods - update the global and per-VBucket
     * stats after queueing a new item to a checkpoint.
     *
     * @param lh Lock to CM::queueLock
     * @param qi
     */
    void updateStatsForNewQueuedItem(const std::lock_guard<std::mutex>& lh,
                                     const queued_item& qi);

    /**
     * function to invoke whenever memory usage changes due to a new
     * queued item or checkpoint removal (or checkpoint expelling, in versions
     * this ins implemented in).
     * Must be declared before checkpointList to ensure it still exists
     * when any Checkpoints within the list are destroyed during destruction
     * of this CheckpointManager.
     */
    std::function<void(int64_t delta)> overheadChangedCallback{[](int64_t) {}};

    /**
     * Return type of removeCursor().
     *  - bool: true is the cursor was removed, false otherwise
     *  - CheckpointList: checkpoints possibly made unreferenced and removed
     */
    struct RemoveCursorResult {
        bool success;
        CheckpointList removed;
    };

    /**
     * Marks the given cursor invalid and removes it from the internal
     * cursor-map.
     *
     * @param lh lock guard holding the queueLock
     * @param cursor
     * @return RemoveCursorResult, see struct definition
     */
    RemoveCursorResult removeCursor(const std::lock_guard<std::mutex>& lh,
                                    CheckpointCursor& cursor);

    /**
     * Register a cursor within the checkpoint.
     *
     * @param lh lock guard holding the queueLock
     * @param name of the new cursor
     * @param startBySeqno The seqno where to place the cursor
     * @param droppable Whether the new cursor can be removed by cursor-dropping
     * @return CursorRegResult, see struct for details
     */
    CursorRegResult registerCursorBySeqno(
            const std::lock_guard<std::mutex>& lh,
            const std::string& name,
            uint64_t startBySeqno,
            CheckpointCursor::Droppable droppable);

    /**
     * Called by getItemsForCursor() for registering a copy of the persistence
     * cursor before pcursor moves.
     * The copy is used for resetting the pcursor to the backup position (in
     * the case of flush failure) for re-attempting the flush.
     *
     * The function forbids to overwrite an existing backup pcursor.
     *
     * @param lh Lock to CM::queueLock
     * @throws logic_error if the user attempts to overwrite the backup pcursor
     */
    void registerBackupPersistenceCursor(const std::lock_guard<std::mutex>& lh);

    /**
     * Returns the sum of the total number of items to be processed from the
     * current checkpoint and all subsequent checkpoints
     *
     * @param lh Lock to CM::queueLock
     * @param cursor
     * @return number of items to be processed
     */
    size_t getNumItemsForCursor(const std::lock_guard<std::mutex>& lh,
                                const CheckpointCursor* cursor) const;

    /**
     * Clears this CM, effectively removing all checkpoints in the list and
     * resetting seqnos.
     *
     * @param lh Lock to CM::queueLock
     * @param seqno The high-seqno to set for the cleared CM
     */
    void clear(const std::lock_guard<std::mutex>& lh, uint64_t seqno);

    /**
     *
     * @param lh Lock to CM::queueLock
     * @return a reference to the open checkpoint
     */
    Checkpoint& getOpenCheckpoint(const std::lock_guard<std::mutex>& lh) const;

    /**
     * Closes the current open checkpoint and adds a new open checkpoint to
     * the checkpointList.
     *
     * @param lh Lock to CM::queueLock
     * @param snapStartSeqno for the new checkpoint
     * @param snapEndSeqno for the new checkpoint
     * @param visibleSnapEnd for the new checkpoint
     * @param highCompletedSeqno optional SyncRep HCS to be flushed to disk
     * @param checkpointType is the checkpoint created from a replica receiving
     *                       a disk snapshot?
     */
    void addNewCheckpoint(const std::lock_guard<std::mutex>& lh,
                          uint64_t snapStartSeqno,
                          uint64_t snapEndSeqno,
                          uint64_t visibleSnapEnd,
                          std::optional<uint64_t> highCompletedSeqno,
                          CheckpointType checkpointType);

    /**
     * Closes the current open checkpoint and adds a new open checkpoint to
     * the checkpointList.
     *
     * Note: The function sets snapStart and snapEnd to 'lastBySeqno' for the
     *       new checkpoint.
     *
     * @param lh Lock to CM::queueLock
     */
    void addNewCheckpoint(const std::lock_guard<std::mutex>& lh);

    /*
     * Add an open checkpoint to the checkpointList.
     *
     * @param snapStartSeqno for the new checkpoint
     * @param snapEndSeqno for the new checkpoint
     * @param highCompletedSeqno the SyncRepl HCS to be flushed to disk
     * @param checkpointType is the checkpoint created from a replica receiving
     *                       a disk snapshot?
     */
    void addOpenCheckpoint(uint64_t snapStart,
                           uint64_t snapEnd,
                           uint64_t visibleSnapEnd,
                           std::optional<uint64_t> highCompletedSeqno,
                           uint64_t highPreparedSeqno,
                           CheckpointType checkpointType);

    /**
     * Moves the cursor to the empty item into the next checkpoint (if any).
     *
     * @param cursor
     * @return true if the cursor has moved, false otherwise
     */
    bool moveCursorToNextCheckpoint(CheckpointCursor &cursor);

    bool isLastMutationItemInCheckpoint(CheckpointCursor &cursor);

    void resetCursors();

    /**
     * Create a checkpoint meta-item.
     *
     * @param checkpointId
     * @param op
     * @return The queued_item
     * @throw std::invalid_argument if a non-meta queue_op is requested
     */
    queued_item createCheckpointMetaItem(uint64_t checkpointId, queue_op op);

    /**
     * Checks if the CM state pre-conditions for creating a new checkpoint are
     * met and proceeds trying creating a new checkpoint.
     *
     * @param lh Lock to CM mutex
     */
    void maybeCreateNewCheckpoint(const std::lock_guard<std::mutex>& lh);

    /**
     * Remove all the closed/unref checkpoints (only the ones already processed
     * by all cursors) from the checkpoint-list and return the removed chuck to
     * the caller.
     *
     * @param lh Lock to CM mutex
     * @return the set of closed/unref checkpoints removed from the CM list
     */
    CheckpointList extractClosedUnrefCheckpoints(
            const std::lock_guard<std::mutex>& lh);

    /**
     * Returns the earliest cursor in checkpoints.
     *
     * @param lh Lock to CM mutex
     * @return the lowest cursor, or an empty pointer if there are zero cursors
     * registered.
     */
    std::shared_ptr<CheckpointCursor> getLowestCursor(
            const std::lock_guard<std::mutex>& lh);

    /// Return type of extractItemsToExpel().
    class ExtractItemsResult {
    public:
        ExtractItemsResult();
        ExtractItemsResult(CheckpointQueue&& items,
                           CheckpointManager* manager,
                           std::shared_ptr<CheckpointCursor> expelCursor,
                           Checkpoint* checkpoint);

        ~ExtractItemsResult();

        ExtractItemsResult(const ExtractItemsResult&) = delete;
        ExtractItemsResult& operator=(const ExtractItemsResult&) = delete;

        ExtractItemsResult(ExtractItemsResult&& other);
        ExtractItemsResult& operator=(ExtractItemsResult&& other);

        size_t getNumItems() const;
        const CheckpointCursor& getExpelCursor() const;

        /**
         * Erases all the owned items.
         * Note: This is a O(N) deallocation.
         *
         * @return an estimate of memory released
         */
        size_t deleteItems();

        Checkpoint* getCheckpoint() const;

    private:
        // Container of expelled items
        CheckpointQueue items;

        // Ref to the CM. Used at destruction for removing the expel-cursor.
        CheckpointManager* manager{nullptr};

        // This cursor is registered at expel for ensuring that the checkpoint
        // touched is not removed between locked CM calls. That allows to
        // logically split ItemExpel in multiple locked calls into the
        // checkpoint.
        // Cursor always registered at Checkpoint::begin. The cursor is removed
        // when expel has completed all its logical steps.
        std::shared_ptr<CheckpointCursor> expelCursor{nullptr};

        // Pointer to the checkpoint on which expel has operated.
        Checkpoint* checkpoint{nullptr};
    };

    /**
     * Extracts all the items in the oldest checkpoint that still has cursors
     * registered in it and returns the removed chuck to the caller.
     * This function includes all the ItemExpel logic tha needs to execute under
     * CM::queueLock.
     *
     * @param lh Lock to CM mutex
     * @return ExtractItemsResult, see struct definition
     */
    ExtractItemsResult extractItemsToExpel(
            const std::lock_guard<std::mutex>& lh);

    /**
     * Return memory overhead of all the checkpoints managed, computed by
     * allocators
     *
     * @param lh Lock to CM::queueLock
     */
    size_t getMemOverheadAllocatorBytes(
            const std::lock_guard<std::mutex>& lh) const;

    CheckpointList checkpointList;
    EPStats                 &stats;
    CheckpointConfig        &checkpointConfig;
    mutable std::mutex       queueLock;

    // Ref to the owning vbucket.
    // Non-const as required by some usage that ideally we would remove. @todo
    VBucket& vb;

    // Total number of items (including meta items) in /all/ checkpoints managed
    // by this object.
    std::atomic<size_t>      numItems;

    struct Labeller {
        std::string getLabel(const char* name) const;
        const Vbid vbid;
    };
    ATOMIC_MONOTONIC3(int64_t, lastBySeqno, Labeller);
    /**
     * The highest seqno of all items that are visible, i.e. normal mutations or
     * mutations which have been prepared->committed. The main use of this value
     * is to give clients that don't support sync-replication a view of the
     * vbucket which they can receive (via dcp), i.e this value would not change
     * to the seqno of a prepare.
     */
    MONOTONIC3(int64_t, maxVisibleSeqno, Labeller);

    /**
     * cursors: stores all known CheckpointCursor objects which are held via
     * shared_ptr. When a client creates a cursor we store the shared_ptr and
     * give out a weak_ptr allowing cursors to be simply de-registered. We use
     * the client's chosen name as the key
     */
    using cursor_index =
            std::unordered_map<std::string, std::shared_ptr<CheckpointCursor>>;
    cursor_index cursors;

    const FlusherCallback flusherCB;

    static constexpr const char* pCursorName = "persistence";
    Cursor pCursor;
    CheckpointCursor* persistenceCursor = nullptr;

    // Only for persistence, we register a copy of the cursor before the cursor
    // moves. Then:
    //  1) if flush succeeds, we just remove the copy
    //  2) if flush fails, we reset the pcursor to the copy
    //
    // That allows to rely entirely on the CM for re-attemping the flush after
    // failure.
    static constexpr const char* backupPCursorName = "persistence-backup";

    /**
     * Flush stats that are accounted when we persist an item between the
     * backup and persistence cursors. Should the flush fail we need to undo
     * the stat updates or we'll overcount them.
     */
    AggregatedFlushStats persistenceFailureStatOvercounts;

    /**
     * The memory overhead of the Checkpoint::toWrite structures.
     */
    cb::AtomicNonNegativeCounter<size_t> memOverheadQueue{0};

    /**
     * The memory overhead of maintaining the keyIndex, including each item's
     * key size and sizeof(index_entry), for all checkpoints in this CM.
     */
    cb::AtomicNonNegativeCounter<size_t> memOverheadIndex{0};

    /**
     * The memory consumption of all items in all checkpoint queues managed by
     * this CM. For every item we include key, metadata and blob sizes.
     */
    cb::AtomicNonNegativeCounter<size_t> queuedItemsMemUsage{0};

    /**
     * Helper class for local counters that need to reflect their updates on
     * bucket-level EPStats.
     */
    class Counter {
    public:
        Counter(EPStats::Counter& global) : local(0), global(global) {
        }
        Counter& operator+=(size_t size);
        Counter& operator-=(size_t size);

        operator size_t() const {
            return local;
        }

    private:
        cb::AtomicNonNegativeCounter<size_t> local;
        EPStats::Counter& global;
    };

    // Memory released by item expel in this CM
    Counter memFreedByExpel;

    // Memory released by checkpoint removal in this CM
    Counter memFreedByCheckpointRemoval;

    friend std::ostream& operator<<(std::ostream& os, const CheckpointManager& m);
};

// Outputs a textual description of the CheckpointManager.
std::ostream& operator<<(std::ostream& os, const CheckpointManager& m);
