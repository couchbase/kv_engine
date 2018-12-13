/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "callbacks.h"
#include "cursor.h"
#include "ep_types.h"
#include "item.h"
#include "monotonic.h"
#include "vbucket.h"

#include <map>
#include <memory>

class Checkpoint;
class CheckpointConfig;
class CheckpointCursor;
class EPStats;
class PreLinkDocumentContext;
class VBucket;

/**
 * Representation of a checkpoint manager that maintains the list of checkpoints
 * for a given vbucket.
 */
class CheckpointManager {
    friend class Checkpoint;
    friend class EventuallyPersistentEngine;
    friend class Consumer;
    friend class CheckpointManagerTestIntrospector;

public:
    typedef std::shared_ptr<Callback<Vbid>> FlusherCallback;

    /// Return type of getItemsForCursor()
    struct ItemsForCursor {
        snapshot_range_t range = {0, 0};
        bool moreAvailable = {false};
    };

    CheckpointManager(EPStats& st,
                      Vbid vbucket,
                      CheckpointConfig& config,
                      int64_t lastSeqno,
                      uint64_t lastSnapStart,
                      uint64_t lastSnapEnd,
                      FlusherCallback cb);

    uint64_t getOpenCheckpointId();

    uint64_t getLastClosedCheckpointId();

    void setOpenCheckpointId(uint64_t id) {
        LockHolder lh(queueLock);
        setOpenCheckpointId_UNLOCKED(lh, id);
    }

    /**
     * Remove closed unreferenced checkpoints and return them through the
     * vector.
     * @param vbucket the vbucket that this checkpoint manager belongs to.
     * @param newOpenCheckpointCreated the flag indicating if the new open
     * checkpoint was created as a result of running this function.
     * @return the number of items that are purged from checkpoint
     * @param limit Max number of checkpoint that can be removed.
     *     No limit by default, overidden only for testing.
     */
    size_t removeClosedUnrefCheckpoints(
            VBucket& vbucket,
            bool& newOpenCheckpointCreated,
            size_t limit = std::numeric_limits<size_t>::max());

    /**
     * Register the cursor for getting items whose bySeqno values are between
     * startBySeqno and endBySeqno, and close the open checkpoint if endBySeqno
     * belongs to the open checkpoint.
     * @param startBySeqno start bySeqno.
     * @return Cursor registration result which consists of (1) the bySeqno with
     * which the cursor can start and (2) flag indicating if the cursor starts
     * with the first item on a checkpoint.
     */
    CursorRegResult registerCursorBySeqno(const std::string& name,
                                          uint64_t startBySeqno);

    /**
     * Remove the cursor for a given connection.
     * @param const pointer to the clients cursor, can be null
     * @return true if the cursor is removed successfully.
     */
    bool removeCursor(const CheckpointCursor* cursor);

    size_t getNumOfCursors();

    /**
     * Queue an item to be written to persistent layer.
     * @param vb the vbucket that a new item is pushed into.
     * @param qi item to be persisted.
     * @param generateBySeqno yes/no generate the seqno for the item
     * @param preLinkDocumentContext A context object needed for the
     *        pre link document API in the server API. It is notified
     *        with the generated CAS before the object is made available
     *        for other threads. May be nullptr if the document originates
     *        from a context where the document shouldn't be updated.
     * @return true if an item queued increases the size of persistence queue by 1.
     */
    bool queueDirty(VBucket& vb,
                    queued_item& qi,
                    const GenerateBySeqno generateBySeqno,
                    const GenerateCas generateCas,
                    PreLinkDocumentContext* preLinkDocumentContext);

    /*
     * Queue writing of the VBucket's state to persistent layer.
     * @param vb the vbucket that a new item is pushed into.
     */
    void queueSetVBState(VBucket& vb);

    /**
     * Return the next item to be sent to a given connection
     *
     * *** currently only in use by test code ***
     *
     * @param const pointer to the clients cursor, can be null
     * @param isLastMutationItem flag indicating if the item to be returned is
     * the last mutation one in the closed checkpoint.
     * @return the next item to be sent to a given connection.
     */
    queued_item nextItem(CheckpointCursor* cursor, bool& isLastMutationItem);

    /**
     * Add all outstanding items for the given cursor name to the vector.
     *
     * @param cursor CheckpointCursor to read items from and advance
     * @param items container which items will be appended to.
     * @return The low/high sequence number added to `items` on success,
     *         or (0,0) if no items were added.
     */
    snapshot_range_t getAllItemsForCursor(CheckpointCursor* cursor,
                                          std::vector<queued_item>& items);

    /**
     * Add all outstanding items for persistence to the vector
     *
     * @param items container which items will be appended to.
     * @return The low/high sequence number added to `items` on success,
     *         or (0,0) if no items were added.
     */
    snapshot_range_t getAllItemsForPersistence(std::vector<queued_item>& items);

    /**
     * Add items for the given cursor to the vector, stopping on a checkpoint
     * boundary which is greater or equal to `approxLimit`. The cursor is
     * advanced to point after the items fetched.
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
     */
    ItemsForCursor getItemsForCursor(CheckpointCursor* cursor,
                                     std::vector<queued_item>& items,
                                     size_t approxLimit);

    /**
     * Add items for persistence to the vector, stopping on a checkpoint
     * boundary which is greater or equal to `approxLimit`. The persistence
     * cursor is advanced to point after the items fetched.
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
        return getItemsForCursor(persistenceCursor, items, approxLimit);
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

    size_t getNumCheckpoints() const;

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

    void clear(vbucket_state_t vbState) {
        LockHolder lh(queueLock);
        clear_UNLOCKED(vbState, lastBySeqno);
    }

    /**
     * Clear all the checkpoints managed by this checkpoint manager.
     */
    void clear(VBucket& vb, uint64_t seqno);

    const CheckpointConfig &getCheckpointConfig() const {
        return checkpointConfig;
    }

    void addStats(ADD_STAT add_stat, const void *cookie);

    /**
     * Create a new open checkpoint by force.
     * @return the new open checkpoint id
     */
    uint64_t createNewCheckpoint();

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
    size_t getMemoryUsage_UNLOCKED() const;

    size_t getMemoryUsage() const;

    /**
     * Return memory overhead of all the checkpoints managed
     */
    size_t getMemoryOverhead_UNLOCKED() const;

    /**
     * Return memory overhead of all the checkpoints managed
     */
    size_t getMemoryOverhead() const;

    /**
     * Return memory consumption of unreferenced checkpoints
     */
    size_t getMemoryUsageOfUnrefCheckpoints() const;

    /**
     * Function returns a list of cursors to drop so as to unreference
     * certain checkpoints within the manager, invoked by the cursor-dropper.
     * @return a container of weak_ptr to cursors
     */
    std::vector<Cursor> getListOfCursorsToDrop();

    /**
     * @return True if at least one checkpoint is unreferenced and can
     * be removed.
     */
    bool hasClosedCheckpointWhichCanBeRemoved() const;

    /*
     * Closes the current open checkpoint and creates a new open one if the id
     * of the open checkpoint is > 0 (i.e., if the vbucket isn't in backfill
     * state). Just updates the open checkpoint id otherwise.
     */
    void checkAndAddNewCheckpoint();

    void setBackfillPhase(uint64_t start, uint64_t end);

    void createSnapshot(uint64_t snapStartSeqno, uint64_t snapEndSeqno);

    void resetSnapshotRange();

    void updateCurrentSnapshotEnd(uint64_t snapEnd);

    snapshot_info_t getSnapshotInfo();

    bool incrCursor(CheckpointCursor &cursor);

    void notifyFlusher() {
        if (flusherCB) {
            Vbid vbid = vbucketId;
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

    int64_t nextBySeqno() {
        LockHolder lh(queueLock);
        return ++lastBySeqno;
    }

    /// @return the persistence cursor which can be null
    CheckpointCursor* getPersistenceCursor() const {
        return persistenceCursor;
    }

    void dump() const;

    /**
     * Take the cursors from another checkpoint manager and reset them in the
     * process - used as part of vbucket reset.
     * @param other the manager we are taking cursors from
     */
    void takeAndResetCursors(CheckpointManager& other);

protected:
    uint64_t getOpenCheckpointId_UNLOCKED(const LockHolder& lh);

    uint64_t getLastClosedCheckpointId_UNLOCKED(const LockHolder& lh);

    void setOpenCheckpointId_UNLOCKED(const LockHolder& lh, uint64_t id);

    // Helper method for queueing methods - update the global and per-VBucket
    // stats after queueing a new item to a checkpoint.
    // Must be called with queueLock held (LockHolder passed in as argument to
    // 'prove' this).
    void updateStatsForNewQueuedItem_UNLOCKED(const LockHolder& lh,
                                              VBucket& vb,
                                              const queued_item& qi);

    CheckpointList checkpointList;

    // Pair of {sequence number, cursor at checkpoint start} used when
    // updating cursor positions when collapsing checkpoints.
    struct CursorPosition {
        uint64_t seqno;
        bool onCpktStart;
    };

    bool removeCursor_UNLOCKED(const CheckpointCursor* cursor);

    CursorRegResult registerCursorBySeqno_UNLOCKED(const LockHolder& lh,
                                                   const std::string& name,
                                                   uint64_t startBySeqno);

    size_t getNumItemsForCursor_UNLOCKED(const CheckpointCursor* cursor) const;

    void clear_UNLOCKED(vbucket_state_t vbState, uint64_t seqno);

    /*
     * @return a reference to the open checkpoint
     */
    Checkpoint& getOpenCheckpoint_UNLOCKED(const LockHolder& lh) const;

    /*
     * Closes the current open checkpoint and adds a new open checkpoint to
     * the checkpointList.
     *
     * @param id for the new checkpoint
     * @param snapStartSeqno for the new checkpoint
     * @param snapEndSeqno for the new checkpoint
     */
    void addNewCheckpoint_UNLOCKED(uint64_t id,
                                   uint64_t snapStartSeqno,
                                   uint64_t snapEndSeqno);

    /*
     * Closes the current open checkpoint and adds a new open checkpoint to
     * the checkpointList.
     * Note: the function sets snapStart and snapEnd to 'lastBySeqno' for the
     *     new checkpoint.
     *
     * @param id for the new checkpoint
     */
    void addNewCheckpoint_UNLOCKED(uint64_t id);

    /*
     * Add an open checkpoint to the checkpointList.
     *
     * @param id for the new checkpoint
     * @param snapStartSeqno for the new checkpoint
     * @param snapEndSeqno for the new checkpoint
     */
    void addOpenCheckpoint(uint64_t id, uint64_t snapStart, uint64_t snapEnd);

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
    uint64_t checkOpenCheckpoint_UNLOCKED(const LockHolder& lh,
                                          bool forceCreation,
                                          bool timeBound);

    bool isLastMutationItemInCheckpoint(CheckpointCursor &cursor);

    bool isCheckpointCreationForHighMemUsage_UNLOCKED(const LockHolder& lh,
                                                      const VBucket& vbucket);

    void resetCursors(bool resetPersistenceCursor = true);

    queued_item createCheckpointItem(uint64_t id,
                                     Vbid vbid,
                                     queue_op checkpoint_op);

    EPStats                 &stats;
    CheckpointConfig        &checkpointConfig;
    mutable std::mutex       queueLock;
    const Vbid vbucketId;

    // Total number of items (including meta items) in /all/ checkpoints managed
    // by this object.
    std::atomic<size_t>      numItems;
    Monotonic<int64_t>       lastBySeqno;
    uint64_t                 pCursorPreCheckpointId;

    /**
     * connCursors: stores all known CheckpointCursor objects which are held via
     * shared_ptr. When a client creates a cursor we store the shared_ptr and
     * give out a weak_ptr allowing cursors to be simply de-registered. We use
     * the client's chosen name as the key
     */
    using cursor_index =
            std::unordered_map<std::string, std::shared_ptr<CheckpointCursor>>;
    cursor_index connCursors;

    const FlusherCallback flusherCB;

    static constexpr const char* pCursorName = "persistence";
    Cursor pCursor;
    CheckpointCursor* persistenceCursor = nullptr;

    friend std::ostream& operator<<(std::ostream& os, const CheckpointManager& m);
};

// Outputs a textual description of the CheckpointManager.
std::ostream& operator<<(std::ostream& os, const CheckpointManager& m);
