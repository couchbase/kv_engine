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

#include "checkpoint_manager.h"

#include "bucket_logger.h"
#include "checkpoint.h"
#include "ep_time.h"
#include "pre_link_document_context.h"
#include "statwriter.h"
#include "vbucket.h"

#include <gsl.h>

CheckpointManager::CheckpointManager(EPStats& st,
                                     Vbid vbucket,
                                     CheckpointConfig& config,
                                     int64_t lastSeqno,
                                     uint64_t lastSnapStart,
                                     uint64_t lastSnapEnd,
                                     FlusherCallback cb)
    : stats(st),
      checkpointConfig(config),
      vbucketId(vbucket),
      numItems(0),
      lastBySeqno(lastSeqno),
      pCursorPreCheckpointId(0),
      flusherCB(cb) {
    LockHolder lh(queueLock);

    // Note: this is the last moment in the CheckpointManager lifetime
    //     when the checkpointList is empty.
    //     Only in CheckpointManager::clear_UNLOCKED, the checkpointList
    //     is temporarily cleared and a new open checkpoint added immediately.
    addOpenCheckpoint(1, lastSnapStart, lastSnapEnd);

    if (checkpointConfig.isPersistenceEnabled()) {
        // Register the persistence cursor
        pCursor = registerCursorBySeqno_UNLOCKED(lh, pCursorName, lastBySeqno)
                          .cursor;
        EP_LOG_INFO("CheckpointManager::registering persistence cursor for {}",
                    vbucketId);
        persistenceCursor = pCursor.lock().get();
    }
}

uint64_t CheckpointManager::getOpenCheckpointId_UNLOCKED(const LockHolder& lh) {
    return getOpenCheckpoint_UNLOCKED(lh).getId();
}

uint64_t CheckpointManager::getOpenCheckpointId() {
    LockHolder lh(queueLock);
    return getOpenCheckpointId_UNLOCKED(lh);
}

uint64_t CheckpointManager::getLastClosedCheckpointId_UNLOCKED(
        const LockHolder& lh) {
    auto id = getOpenCheckpointId_UNLOCKED(lh);
    return id > 0 ? (id - 1) : 0;
}

uint64_t CheckpointManager::getLastClosedCheckpointId() {
    LockHolder lh(queueLock);
    return getLastClosedCheckpointId_UNLOCKED(lh);
}

void CheckpointManager::setOpenCheckpointId_UNLOCKED(const LockHolder& lh,
                                                     uint64_t id) {
    auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);

    // Update the checkpoint_start item with the new Id.
    const auto ckpt_start = ++(openCkpt.begin());
    (*ckpt_start)->setRevSeqno(id);
    if (openCkpt.getId() == 0) {
        (*ckpt_start)->setBySeqno(lastBySeqno + 1);
        openCkpt.setSnapshotStartSeqno(lastBySeqno);
        openCkpt.setSnapshotEndSeqno(lastBySeqno);
    }

    // Update any set_vbstate items to have the same seqno as the
    // checkpoint_start.
    const auto ckpt_start_seqno = (*ckpt_start)->getBySeqno();
    for (auto item = std::next(ckpt_start); item != openCkpt.end(); item++) {
        if ((*item)->getOperation() == queue_op::set_vbucket_state) {
            (*item)->setBySeqno(ckpt_start_seqno);
        }
    }

    openCkpt.setId(id);
    EP_LOG_DEBUG(
            "Set the current open checkpoint id to {} for {} bySeqno is "
            "{}, max is {}",
            id,
            vbucketId,
            (*ckpt_start)->getBySeqno(),
            lastBySeqno);
}

Checkpoint& CheckpointManager::getOpenCheckpoint_UNLOCKED(
        const LockHolder&) const {
    // During its lifetime, the checkpointList can only be in one of the
    // following states:
    //
    //     - 1 open checkpoint, after the execution of:
    //         - CheckpointManager::CheckpointManager
    //         - CheckpointManager::clear_UNLOCKED
    //     - [1, N] closed checkpoints + 1 open checkpoint, after the execution
    //         of CheckpointManager::closeOpenCheckpointAndAddNew_UNLOCKED
    //
    // Thus, by definition checkpointList.back() is the open checkpoint and the
    // checkpointList is never empty.
    return *checkpointList.back();
}

void CheckpointManager::addNewCheckpoint_UNLOCKED(uint64_t id) {
    addNewCheckpoint_UNLOCKED(id, lastBySeqno, lastBySeqno);
}

void CheckpointManager::addNewCheckpoint_UNLOCKED(uint64_t id,
                                                  uint64_t snapStartSeqno,
                                                  uint64_t snapEndSeqno) {
    // First, we must close the open checkpoint.
    auto& oldOpenCkpt = *checkpointList.back();
    EP_LOG_DEBUG(
            "CheckpointManager::addNewCheckpoint_UNLOCKED: Close "
            "the current open checkpoint: [{}, id:{}, snapStart:{}, "
            "snapEnd:{}]",
            vbucketId,
            oldOpenCkpt.getId(),
            oldOpenCkpt.getLowSeqno(),
            oldOpenCkpt.getHighSeqno());
    queued_item qi = createCheckpointItem(
            oldOpenCkpt.getId(), vbucketId, queue_op::checkpoint_end);
    oldOpenCkpt.queueDirty(qi, this);
    ++numItems;
    oldOpenCkpt.setState(CHECKPOINT_CLOSED);

    // Now, we can create the new open checkpoint
    EP_LOG_DEBUG(
            "CheckpointManager::addNewCheckpoint_UNLOCKED: Create "
            "a new open checkpoint: [{}, id:{}, snapStart:{}, snapEnd:{}]",
            vbucketId,
            id,
            snapStartSeqno,
            snapEndSeqno);
    addOpenCheckpoint(id, snapStartSeqno, snapEndSeqno);

    /* If cursors reached to the end of its current checkpoint, move it to the
       next checkpoint. DCP and Persistence cursors can skip a "checkpoint end"
       meta item. This is needed so that the checkpoint remover can remove the
       closed checkpoints and hence reduce the memory usage */
    for (auto& cur_it : connCursors) {
        CheckpointCursor& cursor = *cur_it.second;
        ++(cursor.currentPos);
        if (cursor.currentPos != (*(cursor.currentCheckpoint))->end() &&
            (*(cursor.currentPos))->getOperation() ==
                    queue_op::checkpoint_end) {
            /* checkpoint_end meta item is skipped for persistence and
             * DCP cursors */
            ++(cursor.currentPos); // cursor now reaches to the checkpoint end
        }

        if (cursor.currentPos == (*(cursor.currentCheckpoint))->end()) {
           if ((*(cursor.currentCheckpoint))->getState() == CHECKPOINT_CLOSED) {
               if (!moveCursorToNextCheckpoint(cursor)) {
                   --(cursor.currentPos);
               }
           } else {
               // The replication cursor is already reached to the end of
               // the open checkpoint.
               --(cursor.currentPos);
           }
        } else {
            --(cursor.currentPos);
        }
    }
}

void CheckpointManager::addOpenCheckpoint(uint64_t id,
                                          uint64_t snapStart,
                                          uint64_t snapEnd) {
    Expects(checkpointList.empty() ||
            checkpointList.back()->getState() ==
                    checkpoint_state::CHECKPOINT_CLOSED);

    auto ckpt = std::make_unique<Checkpoint>(
            stats, id, snapStart, snapEnd, vbucketId);
    // Add an empty-item into the new checkpoint.
    // We need this because every CheckpointCursor will point to this empty-item
    // at creation. So, the cursor will point at the first actual non-meta item
    // after the first cursor-increment.
    queued_item qi = createCheckpointItem(0, Vbid(0xffff), queue_op::empty);
    ckpt->queueDirty(qi, this);
    // Note: We don't include the empty-item in 'numItems'

    // This item represents the start of the new checkpoint
    qi = createCheckpointItem(id, vbucketId, queue_op::checkpoint_start);
    ckpt->queueDirty(qi, this);
    ++numItems;

    checkpointList.push_back(std::move(ckpt));
    Ensures(!checkpointList.empty());
    Ensures(checkpointList.back()->getState() ==
            checkpoint_state::CHECKPOINT_OPEN);
}

CursorRegResult CheckpointManager::registerCursorBySeqno(
        const std::string& name, uint64_t startBySeqno) {
    LockHolder lh(queueLock);
    return registerCursorBySeqno_UNLOCKED(lh, name, startBySeqno);
}

CursorRegResult CheckpointManager::registerCursorBySeqno_UNLOCKED(
        const LockHolder& lh, const std::string& name, uint64_t startBySeqno) {
    const auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);
    if (openCkpt.getHighSeqno() < startBySeqno) {
        throw std::invalid_argument(
                "CheckpointManager::registerCursorBySeqno:"
                " startBySeqno (which is " +
                std::to_string(startBySeqno) +
                ") is less than last "
                "checkpoint highSeqno (which is " +
                std::to_string(openCkpt.getHighSeqno()) + ")");
    }

    // If cursor exists with the same name as the one being created, then
    // remove it.
    for (const auto& cursor : connCursors) {
        if (cursor.first == name) {
            EP_LOG_WARN(
                    "Remove the duplicate cursor with the name \"{}\" from {}",
                    name,
                    vbucketId);
            removeCursor_UNLOCKED(cursor.second.get());
            break;
        }
    }

    CursorRegResult result;
    result.seqno = std::numeric_limits<uint64_t>::max();
    result.tryBackfill = false;

    auto itr = checkpointList.begin();
    for (; itr != checkpointList.end(); ++itr) {
        uint64_t en = (*itr)->getHighSeqno();
        uint64_t st = (*itr)->getLowSeqno();

        if (startBySeqno < st) {
            // Requested sequence number is before the start of this
            // checkpoint, position cursor at the checkpoint start.
            auto cursor = std::make_shared<CheckpointCursor>(name,
                                                             itr,
                                                             (*itr)->begin());
            connCursors[name] = cursor;
            (*itr)->incNumOfCursorsInCheckpoint();
            result.seqno = (*itr)->getLowSeqno();
            result.cursor.setCursor(cursor);
            break;
        } else if (startBySeqno <= en) {
            // Requested sequence number lies within this checkpoint.
            // Calculate which item to position the cursor at.
            CheckpointQueue::iterator iitr = (*itr)->begin();
            while (++iitr != (*itr)->end() &&
                    (startBySeqno >=
                     static_cast<uint64_t>((*iitr)->getBySeqno()))) {
            }

            if (iitr == (*itr)->end()) {
                --iitr;
                result.seqno = static_cast<uint64_t>((*iitr)->getBySeqno()) + 1;
            } else {
                result.seqno = static_cast<uint64_t>((*iitr)->getBySeqno());
                --iitr;
            }

            auto cursor =
                    std::make_shared<CheckpointCursor>(name, itr, iitr);
            connCursors[name] = cursor;
            (*itr)->incNumOfCursorsInCheckpoint();
            result.cursor.setCursor(cursor);
            break;
        }
    }

    result.tryBackfill = (result.seqno == checkpointList.front()->getLowSeqno())
                                 ? true
                                 : false;

    if (result.seqno == std::numeric_limits<uint64_t>::max()) {
        /*
         * We should never get here since this would mean that the sequence
         * number we are looking for is higher than anything currently assigned
         *  and there is already an assert above for this case.
         */
        throw std::logic_error(
                "CheckpointManager::registerCursorBySeqno the sequences number "
                "is higher than anything currently assigned");
    }
    return result;
}

bool CheckpointManager::removeCursor(const CheckpointCursor* cursor) {
    LockHolder lh(queueLock);
    return removeCursor_UNLOCKED(cursor);
}

bool CheckpointManager::removeCursor_UNLOCKED(const CheckpointCursor* cursor) {
    if (!cursor) {
        return false;
    }

    EP_LOG_WARN("Remove the checkpoint cursor with the name \"{}\" from {}",
                cursor->name,
                vbucketId);

    (*cursor->currentCheckpoint)->decNumOfCursorsInCheckpoint();

    if (connCursors.erase(cursor->name) == 0) {
        throw std::logic_error(
                "CheckpointManager::removeCursor_UNLOCKED failed to remove "
                "name:" +
                cursor->name);
    }
    return true;
}

size_t CheckpointManager::getNumOfCursors() {
    LockHolder lh(queueLock);
    return connCursors.size();
}

size_t CheckpointManager::getNumCheckpoints() const {
    LockHolder lh(queueLock);
    return checkpointList.size();
}

bool CheckpointManager::isCheckpointCreationForHighMemUsage_UNLOCKED(
        const LockHolder& lh, const VBucket& vbucket) {
    bool forceCreation = false;
    double memoryUsed =
            static_cast<double>(stats.getEstimatedTotalMemoryUsed());

    const auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);

    // pesistence and conn cursors are all currently in the open checkpoint?
    bool allCursorsInOpenCheckpoint =
            (connCursors.size() + 1) == openCkpt.getNumCursorsInCheckpoint();

    if (memoryUsed > stats.mem_high_wat && allCursorsInOpenCheckpoint &&
        (openCkpt.getNumItems() >= MIN_CHECKPOINT_ITEMS ||
         openCkpt.getNumItems() == vbucket.ht.getNumInMemoryItems())) {
        forceCreation = true;
    }
    return forceCreation;
}

size_t CheckpointManager::removeClosedUnrefCheckpoints(
        VBucket& vbucket, bool& newOpenCheckpointCreated, size_t limit) {
    // This function is executed periodically by the non-IO dispatcher.
    size_t numUnrefItems = 0;
    // Checkpoints in the `unrefCheckpointList` have to be deleted before we
    // return from this function. With smart pointers, deletion happens when
    // `unrefCheckpointList` goes out of scope (i.e., when this function
    // returns).
    CheckpointList unrefCheckpointList;
    {
        LockHolder lh(queueLock);
        uint64_t oldCheckpointId = 0;
        bool canCreateNewCheckpoint = false;
        if (checkpointList.size() < checkpointConfig.getMaxCheckpoints() ||
            (checkpointList.size() == checkpointConfig.getMaxCheckpoints() &&
             checkpointList.front()->isNoCursorsInCheckpoint())) {
            canCreateNewCheckpoint = true;
        }
        if (vbucket.getState() == vbucket_state_active &&
            canCreateNewCheckpoint) {
            bool forceCreation =
                    isCheckpointCreationForHighMemUsage_UNLOCKED(lh, vbucket);
            // Check if this master active vbucket needs to create a new open
            // checkpoint.
            oldCheckpointId =
                    checkOpenCheckpoint_UNLOCKED(lh, forceCreation, true);
        }
        newOpenCheckpointCreated = oldCheckpointId > 0;

        if (checkpointConfig.canKeepClosedCheckpoints()) {
            double memoryUsed =
                    static_cast<double>(stats.getEstimatedTotalMemoryUsed());
            if (memoryUsed < stats.mem_high_wat &&
                checkpointList.size() <= checkpointConfig.getMaxCheckpoints()) {
                return 0;
            }
        }

        size_t numMetaItems = 0;
        size_t numCheckpointsRemoved = 0;
        // Iterate through the current checkpoints (from oldest to newest),
        // checking if the checkpoint can be removed.
        auto it = checkpointList.begin();
        // Note terminating condition - we stop at one before the last
        // checkpoint - we must leave at least one checkpoint in existence.
        for (; it != checkpointList.end() &&
               std::next(it) != checkpointList.end();
             ++it) {

            // When we encounter the first checkpoint which has cursor(s) in it,
            // or if the persistence cursor is still operating, stop.
            if ((*it)->getNumCursorsInCheckpoint() > 0 ||
                (checkpointConfig.isPersistenceEnabled() &&
                 (*it)->getId() > pCursorPreCheckpointId)) {
                break;
            } else {
                numUnrefItems += (*it)->getNumItems();
                numMetaItems += (*it)->getNumMetaItems();
                ++numCheckpointsRemoved;

                if (numCheckpointsRemoved >= limit) {
                    break;
                }

                if (checkpointConfig.canKeepClosedCheckpoints() &&
                    (checkpointList.size() - numCheckpointsRemoved) <=
                            checkpointConfig.getMaxCheckpoints()) {
                    // Collect unreferenced closed checkpoints until the number
                    // of checkpoints is
                    // equal to the number of max checkpoints allowed.
                    ++it;
                    break;
                }
            }
        }
        size_t total_items = numUnrefItems + numMetaItems;
        numItems.fetch_sub(total_items);
        unrefCheckpointList.splice(unrefCheckpointList.begin(),
                                   checkpointList,
                                   checkpointList.begin(),
                                   it);
    }
    // Here we have released the lock and unrefCheckpointList is not yet
    // out-of-scope (it will be when this function returns).
    // Thus, checkpoint memory freeing doen't happen under lock.
    // That is very important as releasing objects is an expensive operation, so
    // it would have a relevant impact on front-end operations.
    // Also note that this function is O(N), with N being checkpointList.size().

    return numUnrefItems;
}

std::vector<Cursor> CheckpointManager::getListOfCursorsToDrop() {
    LockHolder lh(queueLock);

    Checkpoint* persistentCheckpoint =
            (persistenceCursor == nullptr)
                    ? nullptr
                    : persistenceCursor->currentCheckpoint->get();
    /*
     * Iterate through the list of checkpoints and add the checkpoint to
     * a set of valid checkpoints until we reach either an open checkpoint
     * or a checkpoint that contains the persistence cursor.
     */
    std::unordered_set<Checkpoint*> validChkpts;
    for (const auto& chkpt : checkpointList) {
        if (persistentCheckpoint == chkpt.get() ||
            chkpt->getState() == CHECKPOINT_OPEN) {
            break;
        } else {
            validChkpts.insert(chkpt.get());
        }
    }

    /*
     * If we cannot find any valid checkpoints to remove cursors from
     * then just return an empty vector.
     */
    if (validChkpts.empty()) {
        return {};
    }

    /*
     * Iterate through all cursors and if the cursor resides in one of the
     * valid checkpoints (i.e. a checkpoint that cursors can be deleted
     * from) then add the cursor to the cursorsToDrop vector.
     */
    std::vector<Cursor> cursorsToDrop;
    for (const auto& cursor : connCursors) {
        if (validChkpts.count(cursor.second->currentCheckpoint->get()) > 0) {
            cursorsToDrop.emplace_back(cursor.second);
        }
    }
    return cursorsToDrop;
}

bool CheckpointManager::hasClosedCheckpointWhichCanBeRemoved() const {
    LockHolder lh(queueLock);
    // Check oldest checkpoint; if closed and contains no cursors then
    // we can remove it (and possibly additional old-but-not-oldest
    // checkpoints).
    const auto& oldestCkpt = checkpointList.front();
    return (oldestCkpt->getState() == CHECKPOINT_CLOSED) &&
           (oldestCkpt->isNoCursorsInCheckpoint());
}

void CheckpointManager::updateStatsForNewQueuedItem_UNLOCKED(
        const LockHolder& lh, VBucket& vb, const queued_item& qi) {
    ++stats.totalEnqueued;
    if (checkpointConfig.isPersistenceEnabled()) {
        ++stats.diskQueueSize;
        vb.doStatsForQueueing(*qi, qi->size());
    }
}

bool CheckpointManager::queueDirty(
        VBucket& vb,
        queued_item& qi,
        const GenerateBySeqno generateBySeqno,
        const GenerateCas generateCas,
        PreLinkDocumentContext* preLinkDocumentContext) {
    LockHolder lh(queueLock);

    bool canCreateNewCheckpoint = false;
    if (checkpointList.size() < checkpointConfig.getMaxCheckpoints() ||
        (checkpointList.size() == checkpointConfig.getMaxCheckpoints() &&
         checkpointList.front()->isNoCursorsInCheckpoint())) {
        canCreateNewCheckpoint = true;
    }

    if (vb.getState() == vbucket_state_active && canCreateNewCheckpoint) {
        // Only the master active vbucket can create a next open checkpoint.
        checkOpenCheckpoint_UNLOCKED(lh, false, true);
    }

    auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);

    if (GenerateBySeqno::Yes == generateBySeqno) {
        qi->setBySeqno(++lastBySeqno);
        openCkpt.setSnapshotEndSeqno(lastBySeqno);
    } else {
        lastBySeqno = qi->getBySeqno();
    }

    // MB-20798: Allow the HLC to be created 'atomically' with the seqno as
    // we're holding the ::queueLock.
    if (GenerateCas::Yes == generateCas) {
        auto cas = vb.nextHLCCas();
        qi->setCas(cas);
        if (preLinkDocumentContext != nullptr) {
            preLinkDocumentContext->preLink(cas, lastBySeqno);
        }
    }

    auto snapStart = openCkpt.getSnapshotStartSeqno();
    auto snapEnd = openCkpt.getSnapshotEndSeqno();
    if (!(snapStart <= static_cast<uint64_t>(lastBySeqno) &&
          static_cast<uint64_t>(lastBySeqno) <= snapEnd)) {
        throw std::logic_error(
                "CheckpointManager::queueDirty: lastBySeqno "
                "not in snapshot range. " +
                vb.getId().to_string() +
                " state:" + std::string(VBucket::toString(vb.getState())) +
                " snapshotStart:" + std::to_string(snapStart) +
                " lastBySeqno:" + std::to_string(lastBySeqno) +
                " snapshotEnd:" + std::to_string(snapEnd) + " genSeqno:" +
                to_string(generateBySeqno) + " checkpointList.size():" +
                std::to_string(checkpointList.size()));
    }

    queue_dirty_t result = openCkpt.queueDirty(qi, this);

    if (result == queue_dirty_t::NEW_ITEM) {
        ++numItems;
    }

    switch (result) {
        case queue_dirty_t::EXISTING_ITEM:
            ++stats.totalDeduplicated;
            return false;
        case queue_dirty_t::PERSIST_AGAIN:
        case queue_dirty_t::NEW_ITEM:
            updateStatsForNewQueuedItem_UNLOCKED(lh, vb, qi);
            return true;
    }
    throw std::logic_error("queueDirty: Invalid value for queue_dirty_t: " +
                           std::to_string(int(result)));
}

void CheckpointManager::queueSetVBState(VBucket& vb) {
    // Take lock to serialize use of {lastBySeqno} and to queue op.
    LockHolder lh(queueLock);

    // Create the setVBState operation, and enqueue it.
    queued_item item = createCheckpointItem(/*id*/0, vbucketId,
                                            queue_op::set_vbucket_state);

    auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);
    const auto result = openCkpt.queueDirty(item, this);

    if (result == queue_dirty_t::NEW_ITEM) {
        ++numItems;
        updateStatsForNewQueuedItem_UNLOCKED(lh, vb, item);
    } else {
        throw std::logic_error(
                "CheckpointManager::queueSetVBState: "
                "expected: NEW_ITEM, got:" +
                to_string(result) + "after queueDirty. " +
                vbucketId.to_string());
    }
}

snapshot_range_t CheckpointManager::getAllItemsForPersistence(
        std::vector<queued_item>& items) {
    if (persistenceCursor == nullptr) {
        EP_LOG_WARN("getAllItemsForPersistence(): persistenceCusror is null {}",
                    vbucketId);
    }
    return getAllItemsForCursor(persistenceCursor, items);
}

snapshot_range_t CheckpointManager::getAllItemsForCursor(
        CheckpointCursor* cursor, std::vector<queued_item>& items) {
    auto result = getItemsForCursor(
            cursor, items, std::numeric_limits<size_t>::max());
    return result.range;
}

CheckpointManager::ItemsForCursor CheckpointManager::getItemsForCursor(
        CheckpointCursor* cursorPtr,
        std::vector<queued_item>& items,
        size_t approxLimit) {
    LockHolder lh(queueLock);
    if (!cursorPtr) {
        EP_LOG_WARN("getAllItemsForCursor(): Caller had a null cursor {}",
                    vbucketId);
        return {};
    }

    auto& cursor = *cursorPtr;

    // Fetch whole checkpoints; as long as we don't exceed the approx item
    // limit.
    ItemsForCursor result;
    result.range.start = (*cursor.currentCheckpoint)->getSnapshotStartSeqno();
    result.range.end = (*cursor.currentCheckpoint)->getSnapshotEndSeqno();
    size_t itemCount = 0;
    while ((result.moreAvailable = incrCursor(cursor))) {
        queued_item& qi = *(cursor.currentPos);
        items.push_back(qi);
        itemCount++;

        if (qi->getOperation() == queue_op::checkpoint_end) {
            // Reached the end of a checkpoint; check if we have exceeded
            // our limit.
            if (itemCount >= approxLimit) {
                // Reached our limit - don't want any more items.
                result.range.end =
                        (*cursor.currentCheckpoint)->getSnapshotEndSeqno();

                // However, we *do* want to move the cursor into the next
                // checkpoint if possible; as that means the checkpoint we just
                // completed has one less cursor in it (and could potentially be
                // freed).
                moveCursorToNextCheckpoint(cursor);
                break;
            }
        }
        // May have moved into a new checkpoint - update range.end.
        result.range.end = (*cursor.currentCheckpoint)->getSnapshotEndSeqno();
    }

    EP_LOG_DEBUG(
            "CheckpointManager::getAllItemsForCursor() "
            "cursor:{} result:{{#items:{} range:{{{}, {}}} "
            "moreAvailable:{}}}",
            cursor.name,
            uint64_t(itemCount),
            result.range.start,
            result.range.end,
            result.moreAvailable ? "true" : "false");

    cursor.numVisits++;

    return result;
}

queued_item CheckpointManager::nextItem(CheckpointCursor* constCursor,
                                        bool& isLastMutationItem) {
    LockHolder lh(queueLock);
    static StoredDocKey emptyKey("", CollectionID::System);
    if (!constCursor) {
        EP_LOG_WARN(
                "CheckpointManager::nextItemForPersistence called with a null "
                "cursor for {}",
                vbucketId);
        queued_item qi(new Item(emptyKey, Vbid(0xffff), queue_op::empty, 0, 0));
        return qi;
    }
    if (getOpenCheckpointId_UNLOCKED(lh) == 0) {
        EP_LOG_DEBUG(
                "{} is still in backfill phase that doesn't allow "
                " the cursor to fetch an item from it's current checkpoint",
                vbucketId);
        queued_item qi(new Item(emptyKey, Vbid(0xffff), queue_op::empty, 0, 0));
        return qi;
    }

    // obtain the non-const cursor so we can advance it
    CheckpointCursor& cursor = *constCursor;
    if (incrCursor(cursor)) {
        isLastMutationItem = isLastMutationItemInCheckpoint(cursor);
        return *(cursor.currentPos);
    } else {
        isLastMutationItem = false;
        queued_item qi(new Item(emptyKey, Vbid(0xffff), queue_op::empty, 0, 0));
        return qi;
    }
}

bool CheckpointManager::incrCursor(CheckpointCursor &cursor) {
    if (++(cursor.currentPos) != (*(cursor.currentCheckpoint))->end()) {
        return true;
    }
    if (!moveCursorToNextCheckpoint(cursor)) {
        --(cursor.currentPos);
        return false;
    }
    return incrCursor(cursor);
}

void CheckpointManager::dump() const {
    std::cerr << *this << std::endl;
}

void CheckpointManager::clear(VBucket& vb, uint64_t seqno) {
    LockHolder lh(queueLock);
    clear_UNLOCKED(vb.getState(), seqno);

    // Reset the disk write queue size stat for the vbucket
    if (checkpointConfig.isPersistenceEnabled()) {
        size_t currentDqSize = vb.dirtyQueueSize.load();
        vb.dirtyQueueSize.fetch_sub(currentDqSize);
        stats.diskQueueSize.fetch_sub(currentDqSize);
    }
}

void CheckpointManager::clear_UNLOCKED(vbucket_state_t vbState, uint64_t seqno) {
    checkpointList.clear();
    numItems = 0;
    lastBySeqno.reset(seqno);
    pCursorPreCheckpointId = 0;
    addOpenCheckpoint(
            vbucket_state_active ? 1 : 0 /* id */, lastBySeqno, lastBySeqno);
    resetCursors();
}

void CheckpointManager::resetCursors(bool resetPersistenceCursor) {
    for (auto& cit : connCursors) {
        if (cit.second->name == pCursorName) {
            if (!resetPersistenceCursor) {
                continue;
            } else {
                uint64_t chkid = checkpointList.front()->getId();
                pCursorPreCheckpointId = chkid ? chkid - 1 : 0;
            }
        }
        cit.second->currentCheckpoint = checkpointList.begin();
        cit.second->currentPos = checkpointList.front()->begin();
        checkpointList.front()->incNumOfCursorsInCheckpoint();
    }
}

bool CheckpointManager::moveCursorToNextCheckpoint(CheckpointCursor &cursor) {
    auto& it = cursor.currentCheckpoint;
    if ((*it)->getState() == CHECKPOINT_OPEN) {
        return false;
    } else if ((*it)->getState() == CHECKPOINT_CLOSED) {
        if (std::next(it) == checkpointList.end()) {
            return false;
        }
    }

    // Remove cursor from its current checkpoint.
    (*it)->decNumOfCursorsInCheckpoint();

    // Move the cursor to the next checkpoint.
    ++it;
    cursor.currentPos = (*it)->begin();
    // Add cursor to its new current checkpoint.
    (*it)->incNumOfCursorsInCheckpoint();

    return true;
}

size_t CheckpointManager::getNumOpenChkItems() const {
    LockHolder lh(queueLock);
    return getOpenCheckpoint_UNLOCKED(lh).getNumItems();
}

uint64_t CheckpointManager::checkOpenCheckpoint_UNLOCKED(const LockHolder& lh,
                                                         bool forceCreation,
                                                         bool timeBound) {
    int checkpoint_id = 0;

    const auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);

    timeBound = timeBound && (ep_real_time() - openCkpt.getCreationTime()) >=
                                     checkpointConfig.getCheckpointPeriod();
    // Create the new open checkpoint if any of the following conditions is
    // satisfied:
    // (1) force creation due to online update or high memory usage
    // (2) current checkpoint is reached to the max number of items allowed.
    // (3) time elapsed since the creation of the current checkpoint is greater
    //     than the threshold
    if (forceCreation ||
        (checkpointConfig.isItemNumBasedNewCheckpoint() &&
         openCkpt.getNumItems() >= checkpointConfig.getCheckpointMaxItems()) ||
        (openCkpt.getNumItems() > 0 && timeBound)) {
        checkpoint_id = openCkpt.getId();
        addNewCheckpoint_UNLOCKED(checkpoint_id + 1);
    }
    return checkpoint_id;
}

size_t CheckpointManager::getNumItemsForCursor(
        const CheckpointCursor* cursor) const {
    LockHolder lh(queueLock);
    return getNumItemsForCursor_UNLOCKED(cursor);
}

size_t CheckpointManager::getNumItemsForCursor_UNLOCKED(
        const CheckpointCursor* cursor) const {
    if (cursor) {
        size_t items = cursor->getRemainingItemsCount();
        CheckpointList::const_iterator chkptIterator =
                cursor->currentCheckpoint;
        if (chkptIterator != checkpointList.end()) {
            ++chkptIterator;
        }

        // Now add the item counts for all the subsequent checkpoints
        auto result = std::accumulate(
                chkptIterator,
                checkpointList.end(),
                items,
                [](size_t a, const std::unique_ptr<Checkpoint>& b) {
                    return a + b->getNumItems();
                });
        return result;
    }
    return 0;
}

bool CheckpointManager::isLastMutationItemInCheckpoint(
                                                   CheckpointCursor &cursor) {
    CheckpointQueue::iterator it = cursor.currentPos;
    ++it;
    if (it == (*(cursor.currentCheckpoint))->end() ||
        (*it)->getOperation() == queue_op::checkpoint_end) {
        return true;
    }
    return false;
}

void CheckpointManager::setBackfillPhase(uint64_t start, uint64_t end) {
    LockHolder lh(queueLock);
    setOpenCheckpointId_UNLOCKED(lh, 0);
    auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);
    openCkpt.setSnapshotStartSeqno(start);
    openCkpt.setSnapshotEndSeqno(end);
}

void CheckpointManager::createSnapshot(uint64_t snapStartSeqno,
                                       uint64_t snapEndSeqno) {
    LockHolder lh(queueLock);

    auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);
    const auto openCkptId = openCkpt.getId();

    if (openCkpt.getNumItems() == 0) {
        if (openCkptId == 0) {
            setOpenCheckpointId_UNLOCKED(lh, openCkptId + 1);
            resetCursors(false);
        }
        openCkpt.setSnapshotStartSeqno(snapStartSeqno);
        openCkpt.setSnapshotEndSeqno(snapEndSeqno);
        return;
    }

    addNewCheckpoint_UNLOCKED(openCkptId + 1, snapStartSeqno, snapEndSeqno);
}

void CheckpointManager::resetSnapshotRange() {
    LockHolder lh(queueLock);

    auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);

    // Update snapStart and snapEnd only if the open checkpoint has no items,
    // just set (snapEnd = lastBySeqno) otherwise.
    if (openCkpt.getNumItems() == 0) {
        openCkpt.setSnapshotStartSeqno(static_cast<uint64_t>(lastBySeqno + 1));
        openCkpt.setSnapshotEndSeqno(static_cast<uint64_t>(lastBySeqno + 1));
    } else {
        openCkpt.setSnapshotEndSeqno(static_cast<uint64_t>(lastBySeqno));
    }
}

void CheckpointManager::updateCurrentSnapshotEnd(uint64_t snapEnd) {
    LockHolder lh(queueLock);
    getOpenCheckpoint_UNLOCKED(lh).setSnapshotEndSeqno(snapEnd);
}

snapshot_info_t CheckpointManager::getSnapshotInfo() {
    LockHolder lh(queueLock);

    const auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);

    snapshot_info_t info;
    info.range.start = openCkpt.getSnapshotStartSeqno();
    info.start = lastBySeqno;
    info.range.end = openCkpt.getSnapshotEndSeqno();

    // If there are no items in the open checkpoint then we need to resume by
    // using that sequence numbers of the last closed snapshot. The exception is
    // if we are in a partial snapshot which can be detected by checking if the
    // snapshot start sequence number is greater than the start sequence number
    // Also, since the last closed snapshot may not be in the checkpoint manager
    // we should just use the last by sequence number. The open checkpoint will
    // be overwritten once the next snapshot marker is received since there are
    // no items in it.
    if (openCkpt.getNumItems() == 0 &&
        static_cast<uint64_t>(lastBySeqno) < info.range.start) {
        info.range.start = lastBySeqno;
        info.range.end = lastBySeqno;
    }

    return info;
}

void CheckpointManager::checkAndAddNewCheckpoint() {
    LockHolder lh(queueLock);
    const auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);
    const auto openCkptId = openCkpt.getId();

    // This function is executed only on a DCP Consumer at snapshot-end
    // mutation. So, by logic a non-backfill open checkpoint cannot be empty.
    Expects(openCkpt.getNumItems() > 0 || openCkptId == 0);

    // If the open checkpoint is the backfill-snapshot (checkpoint-id=0),
    // then we just update the id of the existing checkpoint and we update
    // cursors.
    // Notes:
    //     - we need this because the (checkpoint-id = 0) is reserved for the
    //         backfill phase, and any attempt of stream-request to a
    //         replica-vbucket (e.g., View-Engine) fails if
    //         (current-checkpoint-id = 0). There are also some PassiveStream
    //         tests relying on that.
    //     - an alternative to this is closing the checkpoint and adding a
    //         new one.
    //     - the backfill checkpoint is empty by definition
    if (openCkptId == 0) {
        setOpenCheckpointId_UNLOCKED(lh, openCkptId + 1);
        resetCursors(false);
        return;
    }

    addNewCheckpoint_UNLOCKED(openCkptId + 1);
}

queued_item CheckpointManager::createCheckpointItem(uint64_t id,
                                                    Vbid vbid,
                                                    queue_op checkpoint_op) {
    uint64_t bySeqno;
    StoredDocKey key(to_string(checkpoint_op), CollectionID::System);

    switch (checkpoint_op) {
    case queue_op::checkpoint_start:
        bySeqno = lastBySeqno + 1;
        break;
    case queue_op::checkpoint_end:
        bySeqno = lastBySeqno;
        break;
    case queue_op::empty:
        bySeqno = lastBySeqno;
        break;
    case queue_op::set_vbucket_state:
        bySeqno = lastBySeqno + 1;
        break;

    default:
        throw std::invalid_argument("CheckpointManager::createCheckpointItem:"
                        "checkpoint_op (which is " +
                        std::to_string(static_cast<std::underlying_type<queue_op>::type>(checkpoint_op)) +
                        ") is not a valid item to create");
    }

    queued_item qi(new Item(key, vbid, checkpoint_op, id, bySeqno));
    return qi;
}

uint64_t CheckpointManager::createNewCheckpoint() {
    LockHolder lh(queueLock);

    const auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);

    if (openCkpt.getNumItems() == 0) {
        return openCkpt.getId();
    }

    addNewCheckpoint_UNLOCKED(openCkpt.getId() + 1);
    return getOpenCheckpointId_UNLOCKED(lh);
}

uint64_t CheckpointManager::getPersistenceCursorPreChkId() {
    LockHolder lh(queueLock);
    return pCursorPreCheckpointId;
}

void CheckpointManager::itemsPersisted() {
    LockHolder lh(queueLock);
    auto itr = persistenceCursor->currentCheckpoint;
    pCursorPreCheckpointId = ((*itr)->getId() > 0) ? (*itr)->getId() - 1 : 0;
}

size_t CheckpointManager::getMemoryUsage_UNLOCKED() const {
    size_t memUsage = 0;
    for (const auto& checkpoint : checkpointList) {
        memUsage += checkpoint->getMemConsumption();
    }
    return memUsage;
}

size_t CheckpointManager::getMemoryOverhead_UNLOCKED() const {
    size_t memUsage = 0;
    for (const auto& checkpoint : checkpointList) {
        memUsage += checkpoint->getMemoryOverhead();
    }
    return memUsage;
}

size_t CheckpointManager::getMemoryUsage() const {
    LockHolder lh(queueLock);
    return getMemoryUsage_UNLOCKED();
}

size_t CheckpointManager::getMemoryUsageOfUnrefCheckpoints() const {
    LockHolder lh(queueLock);

    size_t memUsage = 0;
    for (const auto& checkpoint : checkpointList) {
        if (checkpoint->isNoCursorsInCheckpoint()) {
            memUsage += checkpoint->getMemConsumption();
        } else {
            break;
        }
    }
    return memUsage;
}

size_t CheckpointManager::getMemoryOverhead() const {
    LockHolder lh(queueLock);
    return getMemoryOverhead_UNLOCKED();
}

void CheckpointManager::addStats(ADD_STAT add_stat, const void *cookie) {
    LockHolder lh(queueLock);
    char buf[256];

    try {
        checked_snprintf(
                buf, sizeof(buf), "vb_%d:open_checkpoint_id", vbucketId.get());
        add_casted_stat(
                buf, getOpenCheckpointId_UNLOCKED(lh), add_stat, cookie);
        checked_snprintf(buf,
                         sizeof(buf),
                         "vb_%d:last_closed_checkpoint_id",
                         vbucketId.get());
        add_casted_stat(
                buf, getLastClosedCheckpointId_UNLOCKED(lh), add_stat, cookie);
        checked_snprintf(
                buf, sizeof(buf), "vb_%d:num_conn_cursors", vbucketId.get());
        add_casted_stat(buf, connCursors.size(), add_stat, cookie);
        checked_snprintf(buf,
                         sizeof(buf),
                         "vb_%d:num_checkpoint_items",
                         vbucketId.get());
        add_casted_stat(buf, numItems, add_stat, cookie);
        checked_snprintf(buf,
                         sizeof(buf),
                         "vb_%d:num_open_checkpoint_items",
                         vbucketId.get());
        add_casted_stat(buf,
                        getOpenCheckpoint_UNLOCKED(lh).getNumItems(),
                        add_stat,
                        cookie);
        checked_snprintf(
                buf, sizeof(buf), "vb_%d:num_checkpoints", vbucketId.get());
        add_casted_stat(buf, checkpointList.size(), add_stat, cookie);

        if (persistenceCursor) {
            checked_snprintf(buf,
                             sizeof(buf),
                             "vb_%d:num_items_for_persistence",
                             vbucketId.get());
            add_casted_stat(buf,
                            getNumItemsForCursor_UNLOCKED(persistenceCursor),
                            add_stat,
                            cookie);
        }
        checked_snprintf(buf, sizeof(buf), "vb_%d:mem_usage", vbucketId.get());
        add_casted_stat(buf, getMemoryUsage_UNLOCKED(), add_stat, cookie);

        for (const auto& cursor : connCursors) {
            checked_snprintf(buf,
                             sizeof(buf),
                             "vb_%d:%s:cursor_checkpoint_id",
                             vbucketId.get(),
                             cursor.second->name.c_str());
            add_casted_stat(buf,
                            (*(cursor.second->currentCheckpoint))->getId(),
                            add_stat,
                            cookie);
            checked_snprintf(buf,
                             sizeof(buf),
                             "vb_%d:%s:cursor_seqno",
                             vbucketId.get(),
                             cursor.second->name.c_str());
            add_casted_stat(buf,
                            (*(cursor.second->currentPos))->getBySeqno(),
                            add_stat,
                            cookie);
            checked_snprintf(buf,
                             sizeof(buf),
                             "vb_%d:%s:num_visits",
                             vbucketId.get(),
                             cursor.second->name.c_str());
            add_casted_stat(
                    buf, cursor.second->numVisits.load(), add_stat, cookie);
            if (cursor.second.get() != persistenceCursor) {
                checked_snprintf(buf,
                                 sizeof(buf),
                                 "vb_%d:%s:num_items_for_cursor",
                                 vbucketId.get(),
                                 cursor.second->name.c_str());
                add_casted_stat(
                        buf,
                        getNumItemsForCursor_UNLOCKED(cursor.second.get()),
                        add_stat,
                        cookie);
            }
        }
    } catch (std::exception& error) {
        EP_LOG_WARN(
                "CheckpointManager::addStats: An error occurred while adding "
                "stats: {}",
                error.what());
    }
}

void CheckpointManager::takeAndResetCursors(CheckpointManager& other) {
    pCursor = other.pCursor;
    persistenceCursor = pCursor.lock().get();
    for (auto& cursor : other.connCursors) {
        connCursors[cursor.second->name] = cursor.second;
    }
    other.connCursors.clear();

    resetCursors(true /*reset persistence*/);
}

std::ostream& operator <<(std::ostream& os, const CheckpointManager& m) {
    os << "CheckpointManager[" << &m << "] with numItems:"
       << m.getNumItems() << " checkpoints:" << m.checkpointList.size()
       << std::endl;
    for (const auto& c : m.checkpointList) {
        os << "    " << *c << std::endl;
    }
    os << "    connCursors:[" << std::endl;
    for (const auto cur : m.connCursors) {
        os << "        " << cur.first << ": " << *cur.second << std::endl;
    }
    os << "    ]" << std::endl;
    return os;
}
