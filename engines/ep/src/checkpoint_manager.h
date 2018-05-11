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
#include "ep_types.h"
#include "item.h"
#include "locks.h"
#include "monotonic.h"
#include "stats.h"

#include <memcached/engine_common.h>

#include <map>

class Checkpoint;
class CheckpointConfig;
class CheckpointCursor;
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

    typedef std::shared_ptr<Callback<uint16_t> > FlusherCallback;

    /// Return type of getItemsForCursor()
    struct ItemsForCursor {
        snapshot_range_t range = {0, 0};
        bool moreAvailable = {false};
    };

    CheckpointManager(EPStats& st,
                      uint16_t vbucket,
                      CheckpointConfig& config,
                      int64_t lastSeqno,
                      uint64_t lastSnapStart,
                      uint64_t lastSnapEnd,
                      FlusherCallback cb);

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
     * @param name the name of a given connection
     * @param isLastMutationItem flag indicating if the item to be returned is
     * the last mutation one in the closed checkpoint.
     * @return the next item to be sent to a given connection.
     */
    queued_item nextItem(const std::string &name, bool &isLastMutationItem);

    /**
     * Add all outstanding items for the given cursor name to the vector.
     *
     * @param name Cursor to advance.
     * @param items container which items will be appended to.
     * @return The low/high sequence number added to `items` on success,
     *         or (0,0) if no items were added.
     */
    snapshot_range_t getAllItemsForCursor(const std::string& name,
                                          std::vector<queued_item>& items);

    /**
     * Add items for the given cursor to the vector, stopping on a checkpoint
     * boundary which is greater or equal to `approxLimit`. The cursor is
     * advanced to point after the items fetched.
     *
     * Note: It is only valid to fetch complete checkpoints; as such we cannot
     * limit to a precise number of items.
     *
     * @param name Cursor to advance.
     * @param[in/out] items container which items will be appended to.
     * @param approxLimit Approximate number of items to add.
     * @return An ItemsForCursor object containing:
     * range: the low/high sequence number of the checkpoints(s) added to
     * `items`;
     * moreAvailable: true if there are still items available for this
     * checkpoint (i.e. the limit was hit).
     */
    ItemsForCursor getItemsForCursor(const std::string& name,
                                     std::vector<queued_item>& items,
                                     size_t approxLimit);

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
    size_t getNumItemsForCursor(const std::string &name) const;

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

    void setBackfillPhase(uint64_t start, uint64_t end);

    void createSnapshot(uint64_t snapStartSeqno, uint64_t snapEndSeqno);

    void resetSnapshotRange();

    void updateCurrentSnapshotEnd(uint64_t snapEnd);

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

    CheckpointList checkpointList;

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

    bool closeOpenCheckpoint_UNLOCKED();

    bool isLastMutationItemInCheckpoint(CheckpointCursor &cursor);

    bool isCheckpointCreationForHighMemUsage(const VBucket& vbucket);

    void collapseClosedCheckpoints(CheckpointList& collapsedChks);

    void collapseCheckpoints(uint64_t id);

    void resetCursors(bool resetPersistenceCursor = true);

    void putCursorsInCollapsedChk(CursorIdToPositionMap& cursors,
                                  const CheckpointList::iterator chkItr);

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
    Monotonic<int64_t>       lastBySeqno;
    bool                     isCollapsedCheckpoint;
    uint64_t                 lastClosedCheckpointId;
    uint64_t                 pCursorPreCheckpointId;
    cursor_index             connCursors;

    FlusherCallback          flusherCB;

    friend std::ostream& operator<<(std::ostream& os, const CheckpointManager& m);
};

// Outputs a textual description of the CheckpointManager.
std::ostream& operator<<(std::ostream& os, const CheckpointManager& m);