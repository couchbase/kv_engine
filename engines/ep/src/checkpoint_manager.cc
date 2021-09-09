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

#include "checkpoint_manager.h"

#include "bucket_logger.h"
#include "checkpoint.h"
#include "checkpoint_config.h"
#include "ep_time.h"
#include "pre_link_document_context.h"
#include "stats.h"
#include "vbucket.h"
#include "vbucket_state.h"

#include <utility>

#include <gsl/gsl-lite.hpp>
#include <platform/optional.h>
#include <statistics/cbstat_collector.h>

constexpr const char* CheckpointManager::pCursorName;
constexpr const char* CheckpointManager::backupPCursorName;

CheckpointManager::CheckpointManager(EPStats& st,
                                     Vbid vbucket,
                                     CheckpointConfig& config,
                                     int64_t lastSeqno,
                                     uint64_t lastSnapStart,
                                     uint64_t lastSnapEnd,
                                     uint64_t maxVisibleSeqno,
                                     FlusherCallback cb)
    : stats(st),
      checkpointConfig(config),
      vbucketId(vbucket),
      numItems(0),
      lastBySeqno(lastSeqno),
      maxVisibleSeqno(maxVisibleSeqno),
      flusherCB(std::move(cb)) {
    std::lock_guard<std::mutex> lh(queueLock);

    lastBySeqno.setLabel("CheckpointManager(" + vbucketId.to_string() +
                         ")::lastBySeqno");

    // Note: this is the last moment in the CheckpointManager lifetime
    //     when the checkpointList is empty.
    //     Only in CheckpointManager::clear_UNLOCKED, the checkpointList
    //     is temporarily cleared and a new open checkpoint added immediately.
    addOpenCheckpoint(lastSnapStart,
                      lastSnapEnd,
                      maxVisibleSeqno,
                      {},
                      CheckpointType::Memory);

    if (checkpointConfig.isPersistenceEnabled()) {
        // Register the persistence cursor
        pCursor = registerCursorBySeqno_UNLOCKED(lh, pCursorName, lastBySeqno)
                          .cursor;
        persistenceCursor = pCursor.lock().get();
    }
}

uint64_t CheckpointManager::getOpenCheckpointId(
        const std::lock_guard<std::mutex>& lh) {
    return getOpenCheckpoint_UNLOCKED(lh).getId();
}

uint64_t CheckpointManager::getOpenCheckpointId() {
    std::lock_guard<std::mutex> lh(queueLock);
    return getOpenCheckpointId(lh);
}

uint64_t CheckpointManager::getLastClosedCheckpointId_UNLOCKED(
        const std::lock_guard<std::mutex>& lh) {
    auto id = getOpenCheckpointId(lh);
    return id > 0 ? (id - 1) : 0;
}

uint64_t CheckpointManager::getLastClosedCheckpointId() {
    std::lock_guard<std::mutex> lh(queueLock);
    return getLastClosedCheckpointId_UNLOCKED(lh);
}

Checkpoint& CheckpointManager::getOpenCheckpoint_UNLOCKED(
        const std::lock_guard<std::mutex>&) const {
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

void CheckpointManager::addNewCheckpoint_UNLOCKED() {
    addNewCheckpoint_UNLOCKED(lastBySeqno,
                              lastBySeqno,
                              maxVisibleSeqno,
                              {},
                              CheckpointType::Memory);
}

void CheckpointManager::addNewCheckpoint_UNLOCKED(
        uint64_t snapStartSeqno,
        uint64_t snapEndSeqno,
        uint64_t visibleSnapEnd,
        std::optional<uint64_t> highCompletedSeqno,
        CheckpointType checkpointType) {
    // First, we must close the open checkpoint.
    auto& oldOpenCkpt = *checkpointList.back();
    EP_LOG_DEBUG(
            "CheckpointManager::addNewCheckpoint_UNLOCKED: Close "
            "the current open checkpoint: [{}, id:{}, snapStart:{}, "
            "snapEnd:{}]",
            vbucketId,
            oldOpenCkpt.getId(),
            oldOpenCkpt.getMinimumCursorSeqno(),
            oldOpenCkpt.getHighSeqno());
    queued_item qi = createCheckpointItem(
            oldOpenCkpt.getId(), vbucketId, queue_op::checkpoint_end);
    oldOpenCkpt.queueDirty(qi);
    ++numItems;
    oldOpenCkpt.setState(CHECKPOINT_CLOSED);

    addOpenCheckpoint(snapStartSeqno,
                      snapEndSeqno,
                      visibleSnapEnd,
                      highCompletedSeqno,
                      checkpointType);

    /* If cursors reached to the end of its current checkpoint, move it to the
       next checkpoint. DCP cursors can skip a "checkpoint end" meta item.
       This is needed so that the checkpoint remover can remove the
       closed checkpoints and hence reduce the memory usage and so to ensure
       that we do not leave a cursor at the checkpoint_end. This also ensures
       that where possible we will drop a checkpoint instead of running
       expelling which is faster.*/
    for (auto& cur_it : cursors) {
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

void CheckpointManager::addOpenCheckpoint(
        uint64_t snapStart,
        uint64_t snapEnd,
        uint64_t visibleSnapEnd,
        std::optional<uint64_t> highCompletedSeqno,
        CheckpointType checkpointType) {
    Expects(checkpointList.empty() ||
            checkpointList.back()->getState() ==
                    checkpoint_state::CHECKPOINT_CLOSED);

    const uint64_t id =
            checkpointList.empty() ? 1 : checkpointList.back()->getId() + 1;

    EP_LOG_DEBUG(
            "CheckpointManager::addOpenCheckpoint: Create "
            "a new open checkpoint: [{}, id:{}, snapStart:{}, snapEnd:{}, "
            "visibleSnapEnd:{}, hcs:{}, type:{}]",
            vbucketId,
            id,
            snapStart,
            snapEnd,
            visibleSnapEnd,
            to_string_or_none(highCompletedSeqno),
            to_string(checkpointType));

    auto ckpt = std::make_unique<Checkpoint>(*this,
                                             stats,
                                             id,
                                             snapStart,
                                             snapEnd,
                                             visibleSnapEnd,
                                             highCompletedSeqno,
                                             vbucketId,
                                             checkpointType,
                                             overheadChangedCallback);
    // Add an empty-item into the new checkpoint.
    // We need this because every CheckpointCursor will point to this empty-item
    // at creation. So, the cursor will point at the first actual non-meta item
    // after the first cursor-increment.
    queued_item qi = createCheckpointItem(0, Vbid(0xffff), queue_op::empty);
    ckpt->queueDirty(qi);
    // Note: We don't include the empty-item in 'numItems'

    // This item represents the start of the new checkpoint
    qi = createCheckpointItem(id, vbucketId, queue_op::checkpoint_start);
    ckpt->queueDirty(qi);
    ++numItems;

    checkpointList.push_back(std::move(ckpt));
    Ensures(!checkpointList.empty());
    Ensures(checkpointList.back()->getState() ==
            checkpoint_state::CHECKPOINT_OPEN);
}

CursorRegResult CheckpointManager::registerCursorBySeqno(
        const std::string& name, uint64_t startBySeqno) {
    std::lock_guard<std::mutex> lh(queueLock);
    return registerCursorBySeqno_UNLOCKED(lh, name, startBySeqno);
}

CursorRegResult CheckpointManager::registerCursorBySeqno_UNLOCKED(
        const std::lock_guard<std::mutex>& lh,
        const std::string& name,
        uint64_t startBySeqno) {
    const auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);
    if (openCkpt.getHighSeqno() < startBySeqno) {
        throw std::invalid_argument(
                "CheckpointManager::registerCursorBySeqno_UNLOCKED:"
                " startBySeqno (which is " +
                std::to_string(startBySeqno) +
                ") is less than last "
                "checkpoint highSeqno (which is " +
                std::to_string(openCkpt.getHighSeqno()) + ")");
    }

    // If cursor exists with the same name as the one being created, then
    // remove it.
    for (const auto& cursor : cursors) {
        if (cursor.first == name) {
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
        uint64_t st = (*itr)->getMinimumCursorSeqno();

        if (startBySeqno < st) {
            // Requested sequence number is before the start of this
            // checkpoint, position cursor at the checkpoint start.
            auto cursor = std::make_shared<CheckpointCursor>(name,
                                                             itr,
                                                             (*itr)->begin());
            cursors[name] = cursor;
            result.seqno = st;
            result.cursor.setCursor(cursor);
            result.tryBackfill = true;
            break;
        } else if (startBySeqno <= en) {
            // MB-47551 Skip this checkpoint if it is closed and the requested
            // start is the high seqno. The cursor should go to an open
            // checkpoint ready for new mutations.
            if ((*itr)->getState() == CHECKPOINT_CLOSED &&
                startBySeqno == uint64_t(lastBySeqno.load())) {
                continue;
            }

            // Requested sequence number lies within this checkpoint.
            // Calculate which item to position the cursor at.
            ChkptQueueIterator iitr = (*itr)->begin();
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
            cursors[name] = cursor;
            result.cursor.setCursor(cursor);
            break;
        }
    }

    if (result.seqno == std::numeric_limits<uint64_t>::max()) {
        /*
         * We should never get here since this would mean that the sequence
         * number we are looking for is higher than anything currently assigned
         *  and there is already an assert above for this case.
         */
        throw std::logic_error(
                "CheckpointManager::registerCursorBySeqno_UNLOCKED the "
                "sequences number "
                "is higher than anything currently assigned");
    }
    return result;
}

void CheckpointManager::registerBackupPersistenceCursor(
        const std::lock_guard<std::mutex>& lh) {
    // Preconditions: pCursor exists and copy does not
    Expects(persistenceCursor);
    if (cursors.find(backupPCursorName) != cursors.end()) {
        throw std::logic_error(
                "CheckpointManager::registerBackupPersistenceCursor: Backup "
                "cursor "
                "already exists");
    }

    // Note: We want to make an exact copy, only the name differs
    const auto pCursorCopy = std::make_shared<CheckpointCursor>(
            *persistenceCursor, backupPCursorName);
    cursors[backupPCursorName] = std::move(pCursorCopy);
}

bool CheckpointManager::removeCursor(CheckpointCursor* cursor) {
    removeCursorPreLockHook();

    std::lock_guard<std::mutex> lh(queueLock);
    return removeCursor_UNLOCKED(cursor);
}

void CheckpointManager::removeBackupPersistenceCursor() {
    std::lock_guard<std::mutex> lh(queueLock);
    const auto res = removeCursor_UNLOCKED(cursors.at(backupPCursorName).get());
    Expects(res);

    // Reset (recreate) the potential stats overcounts as our flush was
    // successful
    persistenceFailureStatOvercounts = VBucket::AggregatedFlushStats();
}

VBucket::AggregatedFlushStats CheckpointManager::resetPersistenceCursor() {
    std::lock_guard<std::mutex> lh(queueLock);

    // Note: the logic here relies on the existing cursor copy-ctor and
    //  CM::removeCursor function for getting the checkpoint num-cursors
    //  computation right

    // 1) Remove the existing pcursor
    auto remResult = removeCursor_UNLOCKED(persistenceCursor);
    Expects(remResult);
    pCursor = Cursor();
    persistenceCursor = nullptr;

    // 2) Make the new pcursor from the backup copy, assign the correct name
    // and register it
    auto* backup = cursors.at(backupPCursorName).get();
    const auto newPCursor =
            std::make_shared<CheckpointCursor>(*backup, pCursorName);
    cursors[pCursorName] = newPCursor;
    pCursor.setCursor(newPCursor);
    persistenceCursor = pCursor.lock().get();

    // 3) Remove old backup
    remResult = removeCursor_UNLOCKED(backup);
    Expects(remResult);

    // Swap the stat counts to reset them for the next flush - return the
    // one we accumulated for the caller to adjust the VBucket stats
    VBucket::AggregatedFlushStats ret;
    std::swap(ret, persistenceFailureStatOvercounts);

    return ret;
}

bool CheckpointManager::removeCursor_UNLOCKED(CheckpointCursor* cursor) {
    if (!cursor) {
        return false;
    }

    // We have logic "race conditions" that may lead to legally executing here
    // when the cursor has already been marked invalid, so we just return if
    // that is the case. See MB-45757 for details.
    if (!cursor->valid()) {
        return false;
    }

    EP_LOG_DEBUG("Remove the checkpoint cursor with the name \"{}\" from {}",
                 cursor->name,
                 vbucketId);

    cursor->invalidate();

    if (cursors.erase(cursor->name) == 0) {
        throw std::logic_error("CheckpointManager::removeCursor_UNLOCKED: " +
                               to_string(vbucketId) +
                               " Failed to remove cursor: " + cursor->name);
    }

    /**
     * The code bellow is for unit test purposes only and is designed to inject
     * code to simulate a race condition with the destruction of a cursor. See
     * for more information MB-36146
     */
    if (runGetItemsHook) {
        queueLock.unlock();
        runGetItemsHook(cursor, vbucketId);
        queueLock.lock();
    }

    return true;
}

bool CheckpointManager::isCheckpointCreationForHighMemUsage(
        const std::lock_guard<std::mutex>& lh, const VBucket& vbucket) {
    bool forceCreation = false;
    auto memoryUsed = static_cast<double>(stats.getEstimatedTotalMemoryUsed());

    const auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);

    // @todo: This function is broken as CM::cursor tracks all cursors, the
    // persistence (and backup) one included. So:
    // -> allCursorsInOpenCheckpoint = false, always
    // -> forceCreation = false, always
    // This will be fixed within MB-47386.
    //
    // pesistence and conn cursors are all currently in the open checkpoint?
    bool allCursorsInOpenCheckpoint =
            (cursors.size() + 1) == openCkpt.getNumCursorsInCheckpoint();

    if (memoryUsed > stats.mem_high_wat && allCursorsInOpenCheckpoint &&
        (openCkpt.getNumItems() >= MIN_CHECKPOINT_ITEMS ||
         openCkpt.getNumItems() == vbucket.ht.getNumInMemoryItems())) {
        forceCreation = true;
    }
    return forceCreation;
}

CheckpointManager::ReleaseResult
CheckpointManager::removeClosedUnrefCheckpoints(VBucket& vb) {
    // This function is executed periodically by the non-IO dispatcher.

    // We need to acquire the CM lock for extracting checkpoints from the
    // CheckpointList. But the actual deallocation must be lock-free, as it is
    // an expensive operation that has been already proven to degrade frontend
    // throughput if performed under lock.
    CheckpointList toRelease;
    {
        std::lock_guard<std::mutex> lh(queueLock);
        maybeCreateNewCheckpoint(lh, vb);
        toRelease = extractClosedUnrefCheckpoints(lh);
    }
    // CM lock released here

    if (toRelease.empty()) {
        return {0, 0};
    }

    // Update stats and compute return value
    size_t numNonMetaItemsRemoved = 0;
    size_t numMetaItemsRemoved = 0;
    size_t memoryReleased = 0;
    for (const auto& checkpoint : toRelease) {
        numNonMetaItemsRemoved += checkpoint->getNumItems();
        numMetaItemsRemoved += checkpoint->getNumMetaItems();
        memoryReleased += checkpoint->getMemConsumption();
    }
    numItems.fetch_sub(numNonMetaItemsRemoved + numMetaItemsRemoved);
    stats.itemsRemovedFromCheckpoints.fetch_add(numNonMetaItemsRemoved);
    stats.memFreedByCheckpointRemoval += memoryReleased;

    EP_LOG_DEBUG(
            "CheckpointManager::removeClosedUnrefCheckpoints: Removed {} "
            "checkpoints, {} meta-items, {} non-meta-items, {} bytes from {}",
            toRelease.size(),
            numMetaItemsRemoved,
            numNonMetaItemsRemoved,
            memoryReleased,
            vb.getId());

    return {numNonMetaItemsRemoved, memoryReleased};
}

CheckpointManager::ReleaseResult
CheckpointManager::expelUnreferencedCheckpointItems() {
    // trigger the overheadChangedCallback if the overhead is different
    // when this helper is destroyed - which occurs _after_ the destruction
    // of expelledItems (declared below)
    auto overheadCheck = gsl::finally([pre = getMemoryOverhead(), this]() {
        auto post = getMemoryOverhead();
        if (pre != post) {
            overheadChangedCallback(post - pre);
        }
    });

    CheckpointQueue expelledItems;
    size_t estimatedMemRecovered{0};
    {
        std::lock_guard<std::mutex> lh(queueLock);

        Checkpoint* oldestCheckpoint = checkpointList.front().get();

        if (oldestCheckpoint->getNumCursorsInCheckpoint() == 0) {
            // The oldest checkpoint is unreferenced, and may be deleted
            // as a whole by the ClosedUnrefCheckpointRemoverTask,
            // expelling everything from it one by one would be a waste of time.
            // Cannot expel from checkpoints which are not the oldest without
            // leaving gaps in the items a cursor would read.
            return {};
        }

        if (oldestCheckpoint->getNumItems() == 0) {
            // There are no mutation items in the checkpoint to expel.
            return {};
        }

        const auto lowestCursorEntry =
                std::min_element(cursors.begin(),
                                 cursors.end(),
                                 [](const auto& a, const auto& b) {
                                     // Compare by CheckpointCursor.
                                     return *a.second < *b.second;
                                 });
        const auto lowestCursor = lowestCursorEntry->second;

        // Sanity check - if the oldest checkpoint is referenced, the cursor
        // with the lowest seqno should be in that checkpoint.
        if (lowestCursor->currentCheckpoint->get() != oldestCheckpoint) {
            std::stringstream ss;
            ss << "CheckpointManager::expelUnreferencedCheckpointItems: ("
               << vbucketId
               << ") lowest found cursor is not in the oldest "
                  "checkpoint. Oldest checkpoint ID: "
               << oldestCheckpoint->getId()
               << " lowSeqno: " << oldestCheckpoint->getMinimumCursorSeqno()
               << " highSeqno: " << oldestCheckpoint->getHighSeqno()
               << " snapStart: " << oldestCheckpoint->getSnapshotStartSeqno()
               << " snapEnd: " << oldestCheckpoint->getSnapshotEndSeqno()
               << ". Lowest cursor: " << lowestCursor->name
               << " seqno: " << (*lowestCursor->currentPos)->getBySeqno()
               << " ckptID: " << lowestCursor->getId();
            throw std::logic_error(ss.str());
        }

        // Note: Important check as this avoid to decrement the begin() iterator
        // in the following steps.
        if (lowestCursor->currentPos == oldestCheckpoint->begin()) {
            // Lowest cursor is at the checkpoint empty item, nothing to expel
            return {};
        }

        // Never expel items pointed by cursor.
        auto iterator = std::prev(lowestCursor->currentPos);

        /*
         * Walk backwards over the checkpoint if not yet reached the dummy item,
         * and pointing to an item that either:
         * 1. has a seqno equal to the checkpoint's high seqno, or
         * 2. has a subsequent entry with the same seqno (i.e. we don't want
         *    to expel some items but not others with the same seqno), or
         * 3. is pointing to a metadata item.
         */
        while ((iterator != oldestCheckpoint->begin()) &&
               (((*iterator)->getBySeqno() ==
                 int64_t(oldestCheckpoint->getHighSeqno())) ||
                (std::next(iterator) != oldestCheckpoint->end() &&
                 (*iterator)->getBySeqno() ==
                         (*std::next(iterator))->getBySeqno()) ||
                ((*iterator)->isCheckPointMetaItem()))) {
            --iterator;
        }

        // If pointing to the dummy item then cannot expel anything and so just
        // return.
        if (iterator == oldestCheckpoint->begin()) {
            return {};
        }

        /*
         * Now have the checkpoint and the expelUpToAndIncluding
         * cursor we can expel the relevant checkpoint items.  The
         * method returns the expelled items in the expelledItems
         * queue thereby ensuring they still have a reference whilst
         * the queuelock is being held.
         */
        std::tie(expelledItems, estimatedMemRecovered) =
                oldestCheckpoint->expelItems(iterator);
    }

    // If called currentCheckpoint->expelItems but did not manage to expel
    // anything then just return.
    if (expelledItems.empty()) {
        return {};
    }

    stats.itemsExpelledFromCheckpoints.fetch_add(expelledItems.size());

    /*
     * The estimate of the amount of memory recovered by expel is comprised of
     * two parts:
     * 1. Memory used by each item to be expelled.  For each item this
     *    is calculated as the sizeof(Item) + key size + value size.
     * 2. Memory used to hold the items in the checkpoint list.
     *    The checkpoint list will be shorter by expelledItems.size().
     *    This saving is equal to the memory allocated by the
     *    expelledItems list.  Note: On Windows this is not strictly
     *    true as we allocate space for size + 1.  However as its
     *    an estimate we do not need to adjust.
     *
     * It is an optimistic estimate as it assumes that each queued_item
     * is not referenced by anyone else (e.g. a DCP stream) and therefore
     * its reference count will drop to zero on exiting the function
     * allowing the memory to be freed.
     */
    estimatedMemRecovered += expelledItems.get_allocator().getBytesAllocated();

    stats.memFreedByCheckpointItemExpel += estimatedMemRecovered;

    /*
     * We are now outside of the queueLock when the method exits,
     * expelledItems will go out of scope and so the reference count
     * of expelled items will go to zero and hence will be deleted
     * outside of the queuelock.
     */
    return {expelledItems.size(), estimatedMemRecovered};
}

std::vector<Cursor> CheckpointManager::getListOfCursorsToDrop() {
    std::lock_guard<std::mutex> lh(queueLock);

    std::vector<Cursor> cursorsToDrop;

    if (persistenceCursor) {
        // EP
        // By logic:
        // 1. We can't drop the persistence cursor
        // 2. We can't drop the backup-persistence cursor
        // , so surely we can never remove the checkpoint where the
        // special-cursor min(pcursor, backup-pcursor) resides and all
        // checkpoints after that.
        // So in the end it comes by logic that here we want remove only the
        // cursors that reside in the checkpoints up to the last one before the
        // checkpoint pointed by special-cursor.
        // Note that the invariant applies that (backup-pcursor <= pcursor), if
        // the backup cursor exists. So that can be exploited to simplify
        // the logic further here.

        const auto backupExists =
                cursors.find(backupPCursorName) != cursors.end();
        const auto& specialCursor = backupExists
                                            ? *cursors.at(backupPCursorName)
                                            : *persistenceCursor;

        for (const auto& pair : cursors) {
            const auto cursor = pair.second;
            // Note: Strict condition here.
            // Historically the primary reason for dropping cursors has been
            // making closed checkpoints eligible for removal. But with expel it
            // would make sense to drop cursors that reside within the same
            // checkpoint as pcursor/backup-pcursor, as that may make some items
            // eligible for expel.
            // At the time of writing that kind of change is out of scope, so
            // making that a @todo for now.
            if (cursor->getId() < specialCursor.getId()) {
                cursorsToDrop.emplace_back(cursor);
            }
        }
    } else {
        // Ephemeral
        // There's no persistence cursor, so we want just to remove all cursors
        // that reside in the closed checkpoints.

        const auto id = getOpenCheckpointId(lh);
        for (const auto& pair : cursors) {
            const auto cursor = pair.second;
            if (cursor->getId() < id) {
                cursorsToDrop.emplace_back(cursor);
            }
        }
    }

    return cursorsToDrop;
}

bool CheckpointManager::hasClosedCheckpointWhichCanBeRemoved() const {
    std::lock_guard<std::mutex> lh(queueLock);
    // Check oldest checkpoint; if closed and contains no cursors then
    // we can remove it (and possibly additional old-but-not-oldest
    // checkpoints).
    const auto& oldestCkpt = checkpointList.front();
    return (oldestCkpt->getState() == CHECKPOINT_CLOSED) &&
           (oldestCkpt->isNoCursorsInCheckpoint());
}

bool CheckpointManager::isEligibleForCheckpointRemovalAfterPersistence() const {
    std::lock_guard<std::mutex> lh(queueLock);

    const auto& oldestCkpt = checkpointList.front();

    // Just 1 (open) checkpoint in CM
    if (oldestCkpt->getState() == CHECKPOINT_OPEN) {
        Expects(checkpointList.size() == 1);
        return false;
    }
    Expects(checkpointList.size() > 1);

    // Is the oldest checkpoint closed and unreferenced?
    const auto numCursors = oldestCkpt->getNumCursorsInCheckpoint();
    if (numCursors == 0) {
        return true;
    }

    // Some cursors in oldest checkpoint

    // If more than 1 cursor, then no checkpoint is eligible for removal
    if (numCursors > 1) {
        return false;
    }

    // Just 1 cursor in oldest checkpoint, is it the backup pcursor?
    const auto backupIt = cursors.find(backupPCursorName);
    if (backupIt != cursors.end() &&
        backupIt->second->currentCheckpoint->get() == oldestCkpt.get()) {
        // Backup cursor in oldest checkpoint, checkpoint(s) will be eligible
        // for removal after backup cursor has gone
        return true;
    }
    // No backup cursor in CM, some other cursor is in oldest checkpoint
    return false;
}

void CheckpointManager::updateStatsForNewQueuedItem_UNLOCKED(
        const std::lock_guard<std::mutex>& lh,
        VBucket& vb,
        const queued_item& qi) {
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
        PreLinkDocumentContext* preLinkDocumentContext,
        std::function<void(int64_t)> assignedSeqnoCallback) {
    std::lock_guard<std::mutex> lh(queueLock);

    bool canCreateNewCheckpoint = false;
    if (checkpointList.size() < checkpointConfig.getMaxCheckpoints() ||
        (checkpointList.size() == checkpointConfig.getMaxCheckpoints() &&
         checkpointList.front()->isNoCursorsInCheckpoint())) {
        canCreateNewCheckpoint = true;
    }

    if (vb.getState() == vbucket_state_active && canCreateNewCheckpoint) {
        // Only the master active vbucket can create a next open checkpoint.
        checkOpenCheckpoint(lh, false);
    }

    auto* openCkpt = &getOpenCheckpoint_UNLOCKED(lh);

    if (GenerateBySeqno::Yes == generateBySeqno) {
        qi->setBySeqno(lastBySeqno + 1);
    }

    const auto newLastBySeqno = qi->getBySeqno();

    if (assignedSeqnoCallback) {
        assignedSeqnoCallback(newLastBySeqno);
    }

    // MB-20798: Allow the HLC to be created 'atomically' with the seqno as
    // we're holding the ::queueLock.
    if (GenerateCas::Yes == generateCas) {
        auto cas = vb.nextHLCCas();
        qi->setCas(cas);
        if (preLinkDocumentContext != nullptr) {
            preLinkDocumentContext->preLink(cas, newLastBySeqno);
        }
    }

    QueueDirtyResult result = openCkpt->queueDirty(qi);

    if (result.status == QueueDirtyStatus::FailureDuplicateItem) {
        // Could not queue into the current checkpoint as it already has a
        // duplicate item (and not permitted to de-dupe this item).
        if (vb.getState() != vbucket_state_active) {
            // We shouldn't see this for non-active vBuckets; given the
            // original (active) vBucket on some other node should not have
            // put duplicate mutations in the same Checkpoint.
            throw std::logic_error(
                    "CheckpointManager::queueDirty(" + vbucketId.to_string() +
                    ") - got Ckpt::queueDirty() status:" +
                    to_string(result.status) + " when vbstate is non-active:" +
                    std::to_string(vb.getState()));
        }

        // To process this item, create a new (empty) checkpoint which we can
        // then re-attempt the enqueuing.
        // Note this uses the lastBySeqno for snapStart / End.
        checkOpenCheckpoint(lh, /*force*/ true);
        openCkpt = &getOpenCheckpoint_UNLOCKED(lh);
        result = openCkpt->queueDirty(qi);
        if (result.status != QueueDirtyStatus::SuccessNewItem) {
            throw std::logic_error("CheckpointManager::queueDirty(vb:" +
                                   vbucketId.to_string() +
                                   ") - got Ckpt::queueDirty() status:" +
                                   to_string(result.status) +
                                   " even after creating a new Checkpoint.");
        }
    }

    lastBySeqno = newLastBySeqno;
    if (qi->isVisible()) {
        maxVisibleSeqno = newLastBySeqno;
    }
    if (GenerateBySeqno::Yes == generateBySeqno) {
        // Now the item has been queued, update snapshotEndSeqno.
        openCkpt->setSnapshotEndSeqno(lastBySeqno, maxVisibleSeqno);
    }

    // Sanity check that the last seqno is within the open Checkpoint extent.
    auto snapStart = openCkpt->getSnapshotStartSeqno();
    auto snapEnd = openCkpt->getSnapshotEndSeqno();
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

    switch (result.status) {
    case QueueDirtyStatus::SuccessExistingItem:
        ++stats.totalDeduplicated;
        if (checkpointConfig.isPersistenceEnabled()) {
            vb.dirtyQueuePendingWrites += result.successExistingByteDiff;
        }
        return false;
    case QueueDirtyStatus::SuccessNewItem:
        ++numItems;
        // FALLTHROUGH
    case QueueDirtyStatus::SuccessPersistAgain:
        updateStatsForNewQueuedItem_UNLOCKED(lh, vb, qi);
        return true;
    case QueueDirtyStatus::FailureDuplicateItem:
        throw std::logic_error(
                "CheckpointManager::queueDirty: Got invalid "
                "result:FailureDuplicateItem - should have been handled with "
                "retry.");
    }
    folly::assume_unreachable();
}

void CheckpointManager::queueSetVBState(VBucket& vb) {
    // Grab the vbstate before the queueLock (avoid a lock inversion)
    auto vbstate = vb.getTransitionState();

    // Take lock to serialize use of {lastBySeqno} and to queue op.
    std::lock_guard<std::mutex> lh(queueLock);

    // Create the setVBState operation, and enqueue it.
    queued_item item = createCheckpointItem(/*id*/0, vbucketId,
                                            queue_op::set_vbucket_state);

    // We need to set the cas of the item as two subsequent set_vbucket_state
    // items will have the same seqno and the flusher needs a way to determine
    // which is the latest so that we persist the correct state.
    // We do this 'atomically' as we are holding the ::queueLock.
    item->setCas(vb.nextHLCCas());

    // MB-43528: To ensure that we have a reasonable queue_age stat we need to
    // set the queue time here.
    item->setQueuedTime();

    // Store a JSON version of the vbucket transition data in the value
    vbstate.toItem(*item);

    auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);
    const auto result = openCkpt.queueDirty(item);

    if (result.status == QueueDirtyStatus::SuccessNewItem) {
        ++numItems;
        updateStatsForNewQueuedItem_UNLOCKED(lh, vb, item);
    } else {
        auto msg = fmt::format(
                "CheckpointManager::queueSetVBState: {} "
                "expected: SuccessNewItem, got: {} with byte "
                "diff of: {} after queueDirty.",
                vbucketId.to_string(),
                to_string(result.status),
                result.successExistingByteDiff);
        throw std::logic_error(msg);
    }
}

CheckpointManager::ItemsForCursor CheckpointManager::getNextItemsForCursor(
        CheckpointCursor* cursor, std::vector<queued_item>& items) {
    return getItemsForCursor(cursor, items, std::numeric_limits<size_t>::max());
}

CheckpointManager::ItemsForCursor CheckpointManager::getItemsForCursor(
        CheckpointCursor* cursorPtr,
        std::vector<queued_item>& items,
        size_t approxLimit) {
    Expects(approxLimit > 0);

    std::lock_guard<std::mutex> lh(queueLock);
    if (!cursorPtr) {
        EP_LOG_WARN("getItemsForCursor(): Caller had a null cursor {}",
                    vbucketId);
        return {};
    }

    auto& cursor = *cursorPtr;

    // Fetch whole checkpoints; as long as we don't exceed the approx item
    // limit.
    ItemsForCursor result(
            (*cursor.currentCheckpoint)->getCheckpointType(),
            (*cursor.currentCheckpoint)->getMaxDeletedRevSeqno(),
            (*cursor.currentCheckpoint)->getHighCompletedSeqno(),
            (*cursor.currentCheckpoint)->getVisibleSnapshotEndSeqno());

    // Only enforce a hard limit for Disk Checkpoints (i.e backfill). This will
    // prevent huge memory growth due to flushing vBuckets on replicas during a
    // rebalance. Memory checkpoints can still grow unbounded due to max number
    // of checkpoints constraint, but that should be solved by reducing
    // Checkpoint size and increasing max number.
    bool hardLimit = (*cursor.currentCheckpoint)->getCheckpointType() ==
                             CheckpointType::Disk &&
                     cursor.name == pCursorName;

    // For persistence, we register a backup pcursor for resetting the pcursor
    // to the backup position if persistence fails.
    if (cursorPtr == persistenceCursor) {
        registerBackupPersistenceCursor(lh);
        result.flushHandle = std::make_unique<FlushHandle>(*this);
    }

    size_t itemCount = 0;
    bool enteredNewCp = true;
    while ((!hardLimit || itemCount < approxLimit) &&
           (result.moreAvailable = incrCursor(cursor))) {
        if (enteredNewCp) {
            result.checkpointType =
                    (*cursor.currentCheckpoint)->getCheckpointType();
            result.ranges.push_back(
                    {{(*cursor.currentCheckpoint)->getSnapshotStartSeqno(),
                      (*cursor.currentCheckpoint)->getSnapshotEndSeqno()},
                     (*cursor.currentCheckpoint)->getHighCompletedSeqno(),
                     (*cursor.currentCheckpoint)->getHighPreparedSeqno()});
            enteredNewCp = false;

            // As we cross into new checkpoints, update the maxDeletedRevSeqno
            // iff the new checkpoint has one recorded, and it's larger than the
            // previous value.
            if ((*cursor.currentCheckpoint)
                        ->getMaxDeletedRevSeqno()
                        .value_or(0) > result.maxDeletedRevSeqno.value_or(0)) {
                result.maxDeletedRevSeqno =
                        (*cursor.currentCheckpoint)->getMaxDeletedRevSeqno();
            }
        }

        queued_item& qi = *(cursor.currentPos);
        items.push_back(qi);
        itemCount++;

        if (qi->getOperation() == queue_op::checkpoint_end) {
            enteredNewCp = true; // the next incrCuror will move to a new CP

            // Reached the end of a checkpoint; check if we have exceeded
            // our limit (soft limit check only returns complete checkpoints).
            if (itemCount >= approxLimit) {
                // Reached our limit - don't want any more items.

                // However, we *do* want to move the cursor into the next
                // checkpoint if possible; as that means the checkpoint we just
                // completed has one less cursor in it (and could potentially be
                // freed).
                moveCursorToNextCheckpoint(cursor);
                break;
            }

            // MB-36971: In the following do *not* call CM::incrCursor(), we
            // may skip a checkpoint_start item at the next run.
            // Use CM::moveCursorToNextCheckpoint() instead, which moves the
            // cursor to the empty item in the next checkpoint (if any).

            // MB-36971: We never want to return (1) multiple Disk checkpoints
            // or (2) checkpoints of different type. So, break if we have just
            // finished with processing a Disk Checkpoint, regardless of what
            // comes next.
            if ((*cursor.currentCheckpoint)->getCheckpointType() ==
                CheckpointType::Disk) {
                // Moving the cursor to the next checkpoint potentially allows
                // the CheckpointRemover to free the checkpoint that we are
                // leaving.
                moveCursorToNextCheckpoint(cursor);
                break;
            }

            // We only want to return items from contiguous checkpoints with the
            // same type. We should not return Memory checkpoint items followed
            // by Disk checkpoint items or vice versa. This is due to
            // ActiveStream needing to send Disk checkpoint items as Disk
            // snapshots to the replica.
            if (moveCursorToNextCheckpoint(cursor)) {
                if ((*cursor.currentCheckpoint)->getCheckpointType() !=
                    result.checkpointType) {
                    break;
                }
            }
        }
    }

    if (getGlobalBucketLogger()->should_log(spdlog::level::debug)) {
        std::stringstream ranges;
        for (const auto& range : result.ranges) {
            const auto hcs = range.highCompletedSeqno;
            ranges << "{" << range.range.getStart() << ","
                   << range.range.getEnd()
                   << "} with HCS:" << to_string_or_none(hcs);
        }
        EP_LOG_DEBUG(
                "CheckpointManager::getItemsForCursor() "
                "cursor:{} result:{{#items:{} ranges:size:{} {} "
                "moreAvailable:{}}}",
                cursor.name,
                uint64_t(itemCount),
                result.ranges.size(),
                ranges.str(),
                result.moreAvailable ? "true" : "false");
    }

    cursor.numVisits++;

    return result;
}

bool CheckpointManager::incrCursor(CheckpointCursor &cursor) {
    if (!cursor.valid()) {
        return false;
    }

    if (++(cursor.currentPos) != (*(cursor.currentCheckpoint))->end()) {
        return true;
    }
    if (!moveCursorToNextCheckpoint(cursor)) {
        --(cursor.currentPos);
        return false;
    }
    return incrCursor(cursor);
}

void CheckpointManager::notifyFlusher() {
    if (flusherCB) {
        Vbid vbid = vbucketId;
        flusherCB->callback(vbid);
    }
}

int64_t CheckpointManager::getHighSeqno() const {
    std::lock_guard<std::mutex> lh(queueLock);
    return lastBySeqno;
}

uint64_t CheckpointManager::getMaxVisibleSeqno() const {
    std::lock_guard<std::mutex> lh(queueLock);
    return maxVisibleSeqno;
}

std::shared_ptr<CheckpointCursor>
CheckpointManager::getBackupPersistenceCursor() {
    std::lock_guard<std::mutex> lh(queueLock);
    const auto exists = cursors.find(backupPCursorName) != cursors.end();
    return exists ? cursors[backupPCursorName] : nullptr;
}

void CheckpointManager::dump() const {
    std::cerr << *this << std::endl;
}

void CheckpointManager::clear(const std::lock_guard<std::mutex>& lh,
                              uint64_t seqno) {
    // Swap our checkpoint list for a new one so that we can clear everything
    // and addOpenCheckpoint will create the new checkpoint in our new list.
    // This also keeps our cursors pointing to valid checkpoints which is
    // necessary as we will dereference them in resetCursors to decrement the
    // counts of the old checkpoints.
    CheckpointList newCheckpointList;
    checkpointList.swap(newCheckpointList);

    numItems = 0;
    lastBySeqno.reset(seqno);
    maxVisibleSeqno.reset(seqno);

    Expects(checkpointList.empty());

    addOpenCheckpoint(lastBySeqno,
                      lastBySeqno,
                      maxVisibleSeqno,
                      {},
                      CheckpointType::Memory);
    resetCursors();
}

void CheckpointManager::resetCursors() {
    for (auto& cit : cursors) {
        // Remove this cursor from the accounting of it's old checkpoint.
        (*cit.second->currentCheckpoint)->decNumOfCursorsInCheckpoint();

        cit.second->currentCheckpoint = checkpointList.begin();
        cit.second->currentPos = checkpointList.front()->begin();
        checkpointList.front()->incNumOfCursorsInCheckpoint();
    }
}

bool CheckpointManager::moveCursorToNextCheckpoint(CheckpointCursor &cursor) {
    if (!cursor.valid()) {
        return false;
    }

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

    Expects((*cursor.currentPos)->getOperation() == queue_op::empty);

    return true;
}

size_t CheckpointManager::getNumOpenChkItems() const {
    std::lock_guard<std::mutex> lh(queueLock);
    return getOpenCheckpoint_UNLOCKED(lh).getNumItems();
}

void CheckpointManager::checkOpenCheckpoint(
        const std::lock_guard<std::mutex>& lh, bool forceCreation) {
    const auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);

    // Create the new open checkpoint if any of the following conditions is
    // satisfied:
    // (1) force creation due to online update, high memory usage, or enqueueing
    //     an op we cannot de-dupe (i.e. an abort, commit, or prepare)
    // (2) current open checkpoint has reached the max number of items allowed
    // (3) the age of the current open checkpoint is greater than the threshold
    //     @todo MB-48038: allow disabling the time-based trigger via config
    const auto numItemsTrigger =
            checkpointConfig.isItemNumBasedNewCheckpoint() &&
            openCkpt.getNumItems() >= checkpointConfig.getCheckpointMaxItems();
    const auto openCkptAge = ep_real_time() - openCkpt.getCreationTime();
    const auto timeTrigger =
            (openCkpt.getNumItems() > 0) &&
            (openCkptAge >= checkpointConfig.getCheckpointPeriod());

    if (forceCreation || numItemsTrigger || timeTrigger) {
        addNewCheckpoint_UNLOCKED();
    }
}

size_t CheckpointManager::getNumItemsForCursor(
        const CheckpointCursor* cursor) const {
    std::lock_guard<std::mutex> lh(queueLock);
    return getNumItemsForCursor_UNLOCKED(cursor);
}

size_t CheckpointManager::getNumItemsForCursor_UNLOCKED(
        const CheckpointCursor* cursor) const {
    if (cursor && cursor->valid()) {
        size_t items = cursor->getRemainingItemsCount();
        CheckpointList::const_iterator chkptIterator(cursor->currentCheckpoint);
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

void CheckpointManager::clear(std::optional<uint64_t> seqno) {
    std::lock_guard<std::mutex> lh(queueLock);
    clear(lh, seqno ? *seqno : lastBySeqno);
}

bool CheckpointManager::isLastMutationItemInCheckpoint(
                                                   CheckpointCursor &cursor) {
    if (!cursor.valid()) {
        throw std::logic_error(
                "CheckpointManager::isLastMutationItemInCheckpoint() cursor "
                "is not valid, it has been removed");
    }

    ChkptQueueIterator it = cursor.currentPos;
    ++it;
    return it == (*(cursor.currentCheckpoint))->end() ||
           (*it)->getOperation() == queue_op::checkpoint_end;
}

void CheckpointManager::createSnapshot(
        uint64_t snapStartSeqno,
        uint64_t snapEndSeqno,
        std::optional<uint64_t> highCompletedSeqno,
        CheckpointType checkpointType,
        uint64_t visibleSnapEnd) {
    if (checkpointType == CheckpointType::Disk) {
        Expects(highCompletedSeqno.has_value());
    }

    std::lock_guard<std::mutex> lh(queueLock);

    auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);

    if (openCkpt.getNumItems() == 0) {
        openCkpt.setSnapshotStartSeqno(snapStartSeqno);
        openCkpt.setSnapshotEndSeqno(snapEndSeqno, visibleSnapEnd);
        openCkpt.setCheckpointType(checkpointType);
        openCkpt.setHighCompletedSeqno(highCompletedSeqno);
        return;
    }

    addNewCheckpoint_UNLOCKED(snapStartSeqno,
                              snapEndSeqno,
                              visibleSnapEnd,
                              highCompletedSeqno,
                              checkpointType);
}

void CheckpointManager::extendOpenCheckpoint(uint64_t snapEnd,
                                             uint64_t visibleSnapEnd) {
    std::lock_guard<std::mutex> lh(queueLock);
    auto& ckpt = getOpenCheckpoint_UNLOCKED(lh);

    if (ckpt.getCheckpointType() == CheckpointType::Disk) {
        throw std::logic_error(
                "CheckpointManager::extendOpenCheckpoint: Cannot extend a Disk "
                "checkpoint");
    }

    ckpt.setSnapshotEndSeqno(snapEnd, visibleSnapEnd);
}

snapshot_info_t CheckpointManager::getSnapshotInfo() {
    std::lock_guard<std::mutex> lh(queueLock);

    const auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);

    snapshot_info_t info(
            lastBySeqno,
            {openCkpt.getSnapshotStartSeqno(), openCkpt.getSnapshotEndSeqno()});

    // If there are no items in the open checkpoint then we need to resume by
    // using that sequence numbers of the last closed snapshot. The exception is
    // if we are in a partial snapshot which can be detected by checking if the
    // snapshot start sequence number is greater than the start sequence number
    // Also, since the last closed snapshot may not be in the checkpoint manager
    // we should just use the last by sequence number. The open checkpoint will
    // be overwritten once the next snapshot marker is received since there are
    // no items in it.
    if (openCkpt.getNumItems() == 0 &&
        static_cast<uint64_t>(lastBySeqno) < info.range.getStart()) {
        info.range = snapshot_range_t(lastBySeqno, lastBySeqno);
    }

    return info;
}

uint64_t CheckpointManager::getOpenSnapshotStartSeqno() const {
    std::lock_guard<std::mutex> lh(queueLock);
    const auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);

    return openCkpt.getSnapshotStartSeqno();
}

uint64_t CheckpointManager::getVisibleSnapshotEndSeqno() const {
    // Follow what getSnapshotInfo does, but only for visible end-seqno
    std::lock_guard<std::mutex> lh(queueLock);
    const auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);

    // This clause is also in getSnapshotInfo, if we have no items for the open
    // checkpoint, return the "end" as maxVisible
    if (openCkpt.getNumItems() == 0 &&
        static_cast<uint64_t>(lastBySeqno) < openCkpt.getSnapshotStartSeqno()) {
        return maxVisibleSeqno;
    }

    return openCkpt.getVisibleSnapshotEndSeqno();
}

queued_item CheckpointManager::createCheckpointItem(uint64_t id,
                                                    Vbid vbid,
                                                    queue_op checkpoint_op) {
    // It's not valid to actually increment lastBySeqno for any meta op as this
    // may be called independently on the replica to the active (i.e. for a
    // failover table change as part of set_vbucket_state) so the seqnos would
    // differ to those on the active.
    //
    // We enqueue all meta ops with lastBySeqno + 1 though to ensure that they
    // are weakly monotonic. If we used different seqnos for different meta ops
    // then they may not be. The next normal op will be enqueued after bumping
    // lastBySeqno so we may see the following seqnos across checkpoints
    // [1, 1, 1, 2, 3, 3] [3, 3, 4] [4, 4, ...]. This means that checkpoint end
    // seqnos are exclusive of any seqno of a normal mutation in the checkpoint,
    // whilst checkpoint starts should be inclusive. Checkpoint ends may share a
    // seqno with a preceding setVBucketState though.
    uint64_t bySeqno = lastBySeqno + 1;
    StoredDocKey key(to_string(checkpoint_op), CollectionID::System);

    switch (checkpoint_op) {
    case queue_op::checkpoint_start:
    case queue_op::checkpoint_end:
    case queue_op::empty:
    case queue_op::set_vbucket_state:
        break;

    default:
        throw std::invalid_argument(
                "CheckpointManager::createCheckpointItem:"
                "checkpoint_op (which is " +
                std::to_string(
                        static_cast<std::underlying_type<queue_op>::type>(
                                checkpoint_op)) +
                ") is not a valid item to create");
    }

    queued_item qi(new Item(key, vbid, checkpoint_op, id, bySeqno));
    return qi;
}

uint64_t CheckpointManager::createNewCheckpoint(bool force) {
    std::lock_guard<std::mutex> lh(queueLock);

    const auto& openCkpt = getOpenCheckpoint_UNLOCKED(lh);

    if (openCkpt.getNumItems() == 0 && !force) {
        return openCkpt.getId();
    }

    addNewCheckpoint_UNLOCKED();
    return getOpenCheckpointId(lh);
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
    std::lock_guard<std::mutex> lh(queueLock);
    return getMemoryUsage_UNLOCKED();
}

size_t CheckpointManager::getEstimatedMemUsage() const {
    // Atomic, don't need to acquire the CM lock
    return estimatedMemUsage;
}

size_t CheckpointManager::getMemoryUsageOfUnrefCheckpoints() const {
    std::lock_guard<std::mutex> lh(queueLock);

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
    std::lock_guard<std::mutex> lh(queueLock);
    return getMemoryOverhead_UNLOCKED();
}

void CheckpointManager::addStats(const AddStatFn& add_stat,
                                 const CookieIface* cookie) {
    std::lock_guard<std::mutex> lh(queueLock);
    std::array<char, 256> buf;

    try {
        checked_snprintf(buf.data(),
                         buf.size(),
                         "vb_%d:open_checkpoint_id",
                         vbucketId.get());
        add_casted_stat(buf.data(), getOpenCheckpointId(lh), add_stat, cookie);
        checked_snprintf(buf.data(),
                         buf.size(),
                         "vb_%d:last_closed_checkpoint_id",
                         vbucketId.get());
        add_casted_stat(buf.data(),
                        getLastClosedCheckpointId_UNLOCKED(lh),
                        add_stat,
                        cookie);
        checked_snprintf(buf.data(),
                         buf.size(),
                         "vb_%d:num_conn_cursors",
                         vbucketId.get());
        add_casted_stat(buf.data(), cursors.size(), add_stat, cookie);
        checked_snprintf(buf.data(),
                         buf.size(),
                         "vb_%d:num_checkpoint_items",
                         vbucketId.get());
        add_casted_stat(buf.data(), numItems, add_stat, cookie);
        checked_snprintf(buf.data(),
                         buf.size(),
                         "vb_%d:num_open_checkpoint_items",
                         vbucketId.get());
        add_casted_stat(buf.data(),
                        getOpenCheckpoint_UNLOCKED(lh).getNumItems(),
                        add_stat,
                        cookie);
        checked_snprintf(buf.data(),
                         buf.size(),
                         "vb_%d:num_checkpoints",
                         vbucketId.get());
        add_casted_stat(buf.data(), checkpointList.size(), add_stat, cookie);

        if (persistenceCursor) {
            checked_snprintf(buf.data(),
                             buf.size(),
                             "vb_%d:num_items_for_persistence",
                             vbucketId.get());
            add_casted_stat(buf.data(),
                            getNumItemsForCursor_UNLOCKED(persistenceCursor),
                            add_stat,
                            cookie);
        }
        checked_snprintf(
                buf.data(), buf.size(), "vb_%d:mem_usage", vbucketId.get());
        add_casted_stat(
                buf.data(), getMemoryUsage_UNLOCKED(), add_stat, cookie);

        for (const auto& cursor : cursors) {
            checked_snprintf(buf.data(),
                             buf.size(),
                             "vb_%d:%s:cursor_checkpoint_id",
                             vbucketId.get(),
                             cursor.second->name.c_str());
            add_casted_stat(buf.data(),
                            (*(cursor.second->currentCheckpoint))->getId(),
                            add_stat,
                            cookie);
            checked_snprintf(buf.data(),
                             buf.size(),
                             "vb_%d:%s:cursor_seqno",
                             vbucketId.get(),
                             cursor.second->name.c_str());
            add_casted_stat(buf.data(),
                            (*(cursor.second->currentPos))->getBySeqno(),
                            add_stat,
                            cookie);
            checked_snprintf(buf.data(),
                             buf.size(),
                             "vb_%d:%s:num_visits",
                             vbucketId.get(),
                             cursor.second->name.c_str());
            add_casted_stat(buf.data(),
                            cursor.second->numVisits.load(),
                            add_stat,
                            cookie);
            if (cursor.second.get() != persistenceCursor) {
                checked_snprintf(buf.data(),
                                 buf.size(),
                                 "vb_%d:%s:num_items_for_cursor",
                                 vbucketId.get(),
                                 cursor.second->name.c_str());
                add_casted_stat(
                        buf.data(),
                        getNumItemsForCursor_UNLOCKED(cursor.second.get()),
                        add_stat,
                        cookie);
            }
        }

        // Iterate all checkpoints and dump usages
        for (const auto& c : checkpointList) {
            c->addStats(add_stat, cookie);
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
    for (auto& cursor : other.cursors) {
        cursors[cursor.second->name] = cursor.second;
    }
    other.cursors.clear();

    resetCursors();
}

bool CheckpointManager::isOpenCheckpointDisk() {
    std::lock_guard<std::mutex> lh(queueLock);
    return checkpointList.back()->isDiskCheckpoint();
}

void CheckpointManager::updateStatsForStateChange(vbucket_state_t from,
                                                  vbucket_state_t to) {
    std::lock_guard<std::mutex> lh(queueLock);
    if (from == vbucket_state_replica && to != vbucket_state_replica) {
        // vbucket is changing state away from replica, it's memory usage
        // should no longer be accounted for as a replica.
        stats.replicaCheckpointOverhead -= getMemoryOverhead_UNLOCKED();
    } else if (from != vbucket_state_replica && to == vbucket_state_replica) {
        // vbucket is changing state to _become_ a replica, it's memory usage
        // _should_ be accounted for as a replica.
        stats.replicaCheckpointOverhead += getMemoryOverhead_UNLOCKED();
    }
}

void CheckpointManager::setOverheadChangedCallback(
        std::function<void(int64_t delta)> callback) {
    std::lock_guard<std::mutex> lh(queueLock);
    overheadChangedCallback = std::move(callback);

    overheadChangedCallback(getMemoryOverhead_UNLOCKED());
}

std::function<void(int64_t delta)>
CheckpointManager::getOverheadChangedCallback() const {
    std::lock_guard<std::mutex> lh(queueLock);
    return overheadChangedCallback;
}

size_t CheckpointManager::getNumCheckpoints() const {
    std::lock_guard<std::mutex> lh(queueLock);
    return checkpointList.size();
}

bool CheckpointManager::hasNonMetaItemsForCursor(
        const CheckpointCursor& cursor) {
    std::lock_guard<std::mutex> lh(queueLock);

    if (!cursor.valid()) {
        return false;
    }

    // Note: using "mutation" === "non-meta item" in the following.
    // Point of the function is to tell the user if there are mutations
    // available for the cursor to process.
    // CM lastBySeqno is bumped only for mutations, so we can exploit that here.
    const auto seqno = (*cursor.currentPos)->getBySeqno();
    if (seqno < lastBySeqno) {
        // Surely there's at least another mutation to process
        return true;
    }

    if (seqno > lastBySeqno) {
        // Note: A cursor's seqno can be higher than lastBySeqno as meta items
        // in CM can be queued after the last mutation and will get (lastBySeqno
        // + 1). If that's the case, then cursor points to something that comes
        // surely after the last queued mutation, so nothing else to process
        return false;
    }

    // Cursor is at lastBySeqno, which could be shared across a mutation and
    // multiple meta items. Eg:
    //
    // CheckpointManager[0x10f1d8380] with numItems:2 checkpoints:1
    //    Checkpoint[0x10f1d8540] with id:4 seqno:{4,4} snap:{3,4, visible:4}
    //      state:CHECKPOINT_OPEN numCursors:2 type:Memory hcs:none  items:[
    //        {4,empty,cid:0x1:empty,118,[m]}
    //        {4,checkpoint_start,cid:0x1:checkpoint_start,129,[m]}
    //        {4,mutation,cid:0x0:key,130,}
    //    ]

    if (!(*cursor.currentPos)->isCheckPointMetaItem()) {
        // If at mutation, then we can definitely state that there's nothing
        // to process as:
        // - another mutation would bump lastBySeqno to (lastBySeqno + 1), which
        //   can't be the case here
        // - another meta-item would get (lastBySeqno + 1) but we are not
        //   interested in meta-items here
        return false;
    }

    // If at meta-items, we need to check if there's any mutation to be
    // processed at the same seqno.
    //
    // Unfortunately in general the state can be more complex than what shown
    // above, and the check can cross multiple checkpoints. Eg:
    //
    // C1:{e:4 cs:4 vbs:4 ce:4}  C2:{e:4 cs:4 m:4}
    //              ^
    //
    // Theoretically this scenario can end up in traversing many meta items
    // until we reach a mutation or the end of the queue, so introducing another
    // O(N) procedure. In practice that's all very unlikely, as normally
    // meta-items are a tiny percentage of all queued items. The only real way
    // for degrading this logic is having hundreds of consecutive set-vbstate
    // items.
    auto c = CheckpointCursor(cursor, "");
    while (incrCursor(c)) {
        if (!(*c.currentPos)->isCheckPointMetaItem()) {
            return true;
        }
    }
    return false;
}

std::ostream& operator <<(std::ostream& os, const CheckpointManager& m) {
    os << "CheckpointManager[" << &m << "] with numItems:"
       << m.getNumItems() << " checkpoints:" << m.checkpointList.size()
       << std::endl;
    for (const auto& c : m.checkpointList) {
        os << "    " << *c << std::endl;
    }
    os << "    cursors:[" << std::endl;
    for (const auto& cur : m.cursors) {
        os << "        " << cur.first << ": " << *cur.second << std::endl;
    }
    os << "    ]" << std::endl;
    return os;
}

FlushHandle::~FlushHandle() {
    if (failed) {
        Expects(vbucket);
        auto statUpdates = manager.resetPersistenceCursor();
        vbucket->doAggregatedFlushStats(statUpdates);
        return;
    }
    // Flush-success path
    manager.removeBackupPersistenceCursor();
}

void CheckpointManager::maybeCreateNewCheckpoint(
        const std::lock_guard<std::mutex>& lh, VBucket& vb) {
    // Only the active can shape the CheckpointList
    if (vb.getState() != vbucket_state_active) {
        return;
    }

    if (checkpointList.size() < checkpointConfig.getMaxCheckpoints() ||
        (checkpointList.size() == checkpointConfig.getMaxCheckpoints() &&
         checkpointList.front()->isNoCursorsInCheckpoint())) {
        // CM state pre-conditions allow creating a new checkpoint.

        // Create a new checkpoint if required.
        const auto forceCreation = isCheckpointCreationForHighMemUsage(lh, vb);
        checkOpenCheckpoint(lh, forceCreation);
    }
}

CheckpointList CheckpointManager::extractClosedUnrefCheckpoints(
        const std::lock_guard<std::mutex>& lh) {
    if (checkpointList.size() < 2) {
        // Only an open checkpoint in the list, nothing to remove.
        return {};
    }

    CheckpointList::iterator it;
    if (cursors.empty()) {
        // No cursors, can remove everything but the open checkpoint
        it = std::prev(checkpointList.end());
    } else {
        it = getLowestCursor(lh)->currentCheckpoint;

        if (it == checkpointList.begin()) {
            // Lowest cursor is in the first checkpoint, nothing to remove.
            return {};
        }
    }

    // Checkpoints eligible for removal are by definition the ones in
    // [list.begin(), lowestCursorCheckpoint - 1]
    CheckpointList ret;
    const auto begin = checkpointList.begin();
    Expects((*begin)->getId() < (*it)->getId());
    const auto distance = (*it)->getId() - (*begin)->getId();
    // Note: Same as for the STL container, the overload of the splice function
    // that doesn't require the distance is O(N) in the size of the input list,
    // while this is a O(1) operation.
    ret.splice(ret.begin(), checkpointList, begin, it, distance);

    return ret;
}

std::shared_ptr<CheckpointCursor> CheckpointManager::getLowestCursor(
        const std::lock_guard<std::mutex>& lh) {
    // Note: This function is called at checkpoint expel/removal and executes.
    // under CM lock.
    // At the time of writing (MB-47386) the purpose is to get rid of any code
    // that is O(N = checkpoint-list-size), so scanning the cursors-map is
    // better than scanning the checkpoint-list. But, this is still a O(N)
    // procedure, where N this time is the cursor-map-size.
    // In particular with collections, the number of cursors can increase
    // considerably compared with the pre-7.0 releases. So we should be smarted
    // than just std::std::unordered_map on cursors. Replacing that with an
    // ordered container (ordered by cursor seqno) would give us O(1) at scan as
    // the lower cursor will be simply the first element in the container.
    // But, we would lose the constant complexity at accessing elements by key.
    // Not sure what would be the performance impact of that, but probably in
    // the end we may need to keep the existing container for fast access, plus
    // an additional ordered container for fast access of the lowest element.
    // An other alternative is keeping track of the lowest cursor by recomputing
    // it at every cursor-move in CM. Ideally that is being a cheap operation at
    // cursor-move and would also make the code here O(1).

    const auto entry = std::min_element(
            cursors.begin(), cursors.end(), [](const auto& a, const auto& b) {
                // Compare by CheckpointCursor.
                return *a.second < *b.second;
            });
    return entry->second;
}