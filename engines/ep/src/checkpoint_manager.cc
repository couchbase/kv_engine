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
#include "checkpoint_cursor.h"
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
                                     VBucket& vb,
                                     CheckpointConfig& config,
                                     int64_t lastSeqno,
                                     uint64_t lastSnapStart,
                                     uint64_t lastSnapEnd,
                                     uint64_t maxVisibleSeqno,
                                     uint64_t maxPrepareSeqno,
                                     FlusherCallback cb,
                                     CheckpointDisposer checkpointDisposer)
    : stats(st),
      checkpointConfig(config),
      vb(vb),
      numItems(0),
      lastBySeqno(lastSeqno),
      maxVisibleSeqno(maxVisibleSeqno),
      flusherCB(std::move(cb)),
      checkpointDisposer(std::move(checkpointDisposer)),
      memFreedByExpel(stats.memFreedByCheckpointItemExpel),
      memFreedByCheckpointRemoval(stats.memFreedByCheckpointRemoval) {
    std::lock_guard<std::mutex> lh(queueLock);

    lastBySeqno.setLabel("CheckpointManager(" + vb.getId().to_string() +
                         ")::lastBySeqno");

    // Note: this is the last moment in the CheckpointManager lifetime
    //     when the checkpointList is empty.
    //     Only in CheckpointManager::clear_UNLOCKED, the checkpointList
    //     is temporarily cleared and a new open checkpoint added immediately.
    addOpenCheckpoint(lastSnapStart,
                      lastSnapEnd,
                      maxVisibleSeqno,
                      {},
                      maxPrepareSeqno,
                      CheckpointType::Memory,
                      vb.isHistoryRetentionEnabled()
                              ? CheckpointHistorical::Yes
                              : CheckpointHistorical::No);

    if (checkpointConfig.isPersistenceEnabled()) {
        // Register the persistence cursor
        pCursor = registerCursorBySeqno(lh,
                                        pCursorName,
                                        lastBySeqno,
                                        CheckpointCursor::Droppable::No)
                          .cursor;
        persistenceCursor = pCursor.lock().get();
    }
}

CheckpointManager::~CheckpointManager() = default;

uint64_t CheckpointManager::getOpenCheckpointId(
        const std::lock_guard<std::mutex>& lh) const {
    return getOpenCheckpoint(lh).getId();
}

uint64_t CheckpointManager::getOpenCheckpointId() const {
    std::lock_guard<std::mutex> lh(queueLock);
    return getOpenCheckpointId(lh);
}

uint64_t CheckpointManager::getLastClosedCheckpointId(
        const std::lock_guard<std::mutex>& lh) const {
    auto id = getOpenCheckpointId(lh);
    return id > 0 ? (id - 1) : 0;
}

uint64_t CheckpointManager::getLastClosedCheckpointId() const {
    std::lock_guard<std::mutex> lh(queueLock);
    return getLastClosedCheckpointId(lh);
}

CheckpointType CheckpointManager::getOpenCheckpointType() const {
    std::lock_guard<std::mutex> lh(queueLock);
    return getOpenCheckpoint(lh).getCheckpointType();
}

CheckpointHistorical CheckpointManager::getOpenCheckpointHistorical() const {
    std::lock_guard<std::mutex> lh(queueLock);
    return getOpenCheckpoint(lh).getHistorical();
}

Checkpoint& CheckpointManager::getOpenCheckpoint(
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

void CheckpointManager::addNewCheckpoint(
        const std::lock_guard<std::mutex>& lh) {
    // Use lastBySeqno + 1 as that will be the seqno of the first item belonging
    // to this checkpoint
    addNewCheckpoint(lh,
                     lastBySeqno + 1,
                     lastBySeqno + 1,
                     maxVisibleSeqno,
                     {},
                     CheckpointType::Memory,
                     vb.isHistoryRetentionEnabled() ? CheckpointHistorical::Yes
                                                    : CheckpointHistorical::No);
}

void CheckpointManager::addNewCheckpoint(
        const std::lock_guard<std::mutex>& lh,
        uint64_t snapStartSeqno,
        uint64_t snapEndSeqno,
        uint64_t visibleSnapEnd,
        std::optional<uint64_t> highCompletedSeqno,
        CheckpointType checkpointType,
        CheckpointHistorical historical) {
    if (isDiskCheckpointType(checkpointType) && !highCompletedSeqno) {
        const auto msg = fmt::format(
                "CheckpointManager::addNewCheckpoint: {}, snapStart:{}, "
                "snapEnd:{}, visibleSnapEnd:{}, HCS:{}, type:{}, {} - "
                "missing HCS",
                vb.getId(),
                snapStartSeqno,
                snapEndSeqno,
                visibleSnapEnd,
                to_string_or_none(highCompletedSeqno),
                ::to_string(checkpointType),
                ::to_string(historical));
        throw std::logic_error(msg);
    }

    // First, we must close the open checkpoint.
    auto* const oldOpenCkptPtr = checkpointList.back().get();
    auto& oldOpenCkpt = *oldOpenCkptPtr;
    EP_LOG_DEBUG(
            "CheckpointManager::addNewCheckpoint: Close "
            "the current open checkpoint: [{}, id:{}, snapStart:{}, "
            "snapEnd:{}]",
            vb.getId(),
            oldOpenCkpt.getId(),
            oldOpenCkpt.getMinimumCursorSeqno(),
            oldOpenCkpt.getHighSeqno());
    queued_item qi = createCheckpointMetaItem(oldOpenCkpt.getId(),
                                              queue_op::checkpoint_end);
    oldOpenCkpt.queueDirty(qi);
    ++numItems;
    oldOpenCkpt.setState(CHECKPOINT_CLOSED);

    // Inherit the HPS from the previous Checkpoint. Should we de-dupe in the
    // Flusher some item at a snapshot end then we must ensure that the HPS of
    // any given Checkpoint is persisted. Consider the following Checkpoints:
    // [1:Pre, 2:Mutation][3:Mutation] in which seqnos 2 and 3 are the same key.
    // To ensure that the flusher persists the HPS for 1:Pre without modifying
    // the Flusher we can inherit the HPS value from the previous Checkpoint
    // such that the Checkpoint [3:Mutation] also has HPS = 1.
    auto hps = oldOpenCkpt.getHighPreparedSeqno()
                       ? *oldOpenCkpt.getHighPreparedSeqno()
                       : 0;

    addOpenCheckpoint(snapStartSeqno,
                      snapEndSeqno,
                      visibleSnapEnd,
                      highCompletedSeqno,
                      hps,
                      checkpointType,
                      historical);

    // If cursors reached to the end of its current checkpoint, move it to the
    // next checkpoint. That is done to help in making checkpoints eligible for
    // removal/expel, so reducing the overall checkpoint mem-usage.
    //
    // Note: The expel-cursor (if registered) is always placed at checkpoint
    //  begin, so it's logically not possible to touch it here.
    //  @todo: It would be good to enforce some check on expel-cursor here,
    //    better to be paranoid in this area.
    //
    // @todo MB-48681: Skipping the checkpoint_end item on DCP cursors is
    //   unexpected, see CM::getItemsForCursor for how that item is used
    for (auto& pair : cursors) {
        auto& cursor = *pair.second;
        const auto& checkpoint = *(*(cursor.getCheckpoint()));
        if (checkpoint.getState() == CHECKPOINT_OPEN) {
            // Cursor in the open (ie last) checkpoint, nothing to move
            continue;
        }

        // Cursor is in a closed checkpoint, we can move it to the open
        // checkpoint if it's at any of the following positions:
        //
        // [empty  ckpt_start  ..  mutation  ..  mutation  ckpt_end  end()]
        //                                       ^         ^         ^
        auto pos = cursor.getPos();
        if (pos == checkpoint.end() ||
            (*pos)->getOperation() == queue_op::checkpoint_end ||
            (*std::next(pos))->getOperation() == queue_op::checkpoint_end) {
            moveCursorToNextCheckpoint(cursor);
        }
    }

    // if the old open checkpoint had no cursors, it is now both closed and
    // unreferenced. If eager checkpoint removal is enabled, it may be possible
    // to remove it immediately, if it is now the oldest checkpoint.
    maybeScheduleDestruction(*oldOpenCkptPtr);
}

void CheckpointManager::addOpenCheckpoint(
        uint64_t snapStart,
        uint64_t snapEnd,
        uint64_t visibleSnapEnd,
        std::optional<uint64_t> highCompletedSeqno,
        uint64_t highPreparedSeqno,
        CheckpointType checkpointType,
        CheckpointHistorical historical) {
    Expects(checkpointList.empty() ||
            checkpointList.back()->getState() ==
                    checkpoint_state::CHECKPOINT_CLOSED);

    const uint64_t id =
            checkpointList.empty() ? 1 : checkpointList.back()->getId() + 1;

    EP_LOG_DEBUG(
            "CheckpointManager::addOpenCheckpoint: Create "
            "a new open checkpoint: [{}, id:{}, snapStart:{}, snapEnd:{}, "
            "visibleSnapEnd:{}, hcs:{}, hps:{} type:{}]",
            vb.getId(),
            id,
            snapStart,
            snapEnd,
            visibleSnapEnd,
            to_string_or_none(highCompletedSeqno),
            highPreparedSeqno,
            to_string(checkpointType));

    auto ckpt = std::make_unique<Checkpoint>(*this,
                                             stats,
                                             id,
                                             snapStart,
                                             snapEnd,
                                             visibleSnapEnd,
                                             highCompletedSeqno,
                                             highPreparedSeqno,
                                             vb.getId(),
                                             checkpointType,
                                             historical);
    // Add an empty-item into the new checkpoint.
    // We need this because every CheckpointCursor will point to this empty-item
    // at creation. So, the cursor will point at the first actual non-meta item
    // after the first cursor-increment.
    queued_item qi = createCheckpointMetaItem(0, queue_op::empty);
    ckpt->queueDirty(qi);
    // Note: We don't include the empty-item in 'numItems'

    // This item represents the start of the new checkpoint
    qi = createCheckpointMetaItem(id, queue_op::checkpoint_start);
    ckpt->queueDirty(qi);
    ++numItems;

    checkpointList.push_back(std::move(ckpt));
    Ensures(!checkpointList.empty());
    Ensures(checkpointList.back()->getState() ==
            checkpoint_state::CHECKPOINT_OPEN);
}

CursorRegResult CheckpointManager::registerCursorBySeqno(
        const std::string& name,
        uint64_t startBySeqno,
        CheckpointCursor::Droppable droppable) {
    std::lock_guard<std::mutex> lh(queueLock);
    return registerCursorBySeqno(lh, name, startBySeqno, droppable);
}

CursorRegResult CheckpointManager::registerCursorBySeqno(
        const std::lock_guard<std::mutex>& lh,
        const std::string& name,
        uint64_t lastProcessedSeqno,
        CheckpointCursor::Droppable droppable) {
    std::shared_ptr<CheckpointCursor> newCursor;

    CursorRegResult result;
    result.nextSeqno = std::numeric_limits<uint64_t>::max();
    result.tryBackfill = false;

    auto ckptIt = checkpointList.begin();

    if ((*ckptIt)->isOpen() && (*ckptIt)->isEmptyByExpel()) {
        // If:
        // - there is only 1 checkpoint in CM
        // - and, ItemExpel removed all mutations from that checkpoint
        // (MB-39344) then we register a cursor at checkpoint begin.
        //
        // Note: The legacy logic (below) for cursor-registering is based on
        // Checkpoint::getMinimumCursorSeqno()/getHighSeqno() that are
        // meaningless if all mutations have been expelled.
        auto pos = (*ckptIt)->begin();
        newCursor = std::make_shared<CheckpointCursor>(
                name, ckptIt, pos, droppable, 0);
        result.nextSeqno = lastBySeqno + 1;
        result.cursor.setCursor(newCursor);
        result.position = *pos;

        // Trigger backfill only if there's a gap between lastProcessedSeqno and
        // nextSeqnoAvailableFromCheckpoint
        const uint64_t nextSeqnoAvailableFromCheckpoint = lastBySeqno + 1;
        result.tryBackfill =
                (nextSeqnoAvailableFromCheckpoint != lastProcessedSeqno + 1);
    } else {
        // Path here handles all scenarios but the new one introduced in
        // MB-39344 (ie one single open checkpoint has been emptied by
        // ItemExpel).

        const auto& openCkpt = getOpenCheckpoint(lh);
        if (openCkpt.getHighSeqno() < lastProcessedSeqno) {
            throw std::invalid_argument(
                    "CheckpointManager::registerCursorBySeqno: "
                    "lastProcessedSeqno (which is " +
                    std::to_string(lastProcessedSeqno) +
                    ") is less than last checkpoint highSeqno (which is " +
                    std::to_string(openCkpt.getHighSeqno()) + ")");
        }

        for (; ckptIt != checkpointList.end(); ++ckptIt) {
            // Some sanity check
            if (ckptIt != checkpointList.begin()) {
                // ItemExpel is expected to touch only the oldest checkpoint.
                Expects(!(*ckptIt)->modifiedByExpel());
            }

            uint64_t en = (*ckptIt)->getHighSeqno();
            uint64_t st = (*ckptIt)->getMinimumCursorSeqno();

            if (lastProcessedSeqno < st) {
                // Requested sequence number is before the start of this
                // checkpoint, position cursor at the checkpoint begin.
                auto pos = (*ckptIt)->begin();
                newCursor = std::make_shared<CheckpointCursor>(
                        name, ckptIt, pos, droppable, 0);
                result.nextSeqno = st;
                result.cursor.setCursor(newCursor);
                result.position = *pos;
                // Set tryBackfill:true only if lastProcessedSeqno to st is non
                // contiguous. Note this is likely the fix for MB-53616/MB-58302
                // which were noted in master only, and likely backported to the
                // neo branch via MB-39344
                result.tryBackfill = st - lastProcessedSeqno > 1;
                break;
            } else if (lastProcessedSeqno <= en) {
                // checkpoint was empty by expel, so we don't know the real
                // start, backfill is needed whilst we continue the search for
                // a checkpoint to best satisfy the start. MB-58261
                if (st == 0) {
                    result.tryBackfill = true;
                }

                // MB-47551 Skip this checkpoint if it is closed and the
                // requested start is the high seqno. The cursor should go to an
                // open checkpoint ready for new mutations.
                if ((*ckptIt)->getState() == CHECKPOINT_CLOSED &&
                    lastProcessedSeqno == uint64_t(lastBySeqno.load())) {
                    continue;
                }

                // MB-58321: Skip this checkpoint if the requested start is on
                // the end (and if there are more checkpoints).
                const bool isLastCkpt =
                        std::next(ckptIt) == checkpointList.end();
                if (lastProcessedSeqno == en && !isLastCkpt) {
                    continue;
                }

                // Requested sequence number lies within this checkpoint.
                // Calculate the position/distance to place the cursor, plus the
                // information for the caller on what's the next seqno available
                // in checkpoint.
                auto pos = (*ckptIt)->begin();
                size_t distance = 0;
                while (true) {
                    auto next = std::next(pos);
                    if (next == (*ckptIt)->end()) {
                        result.nextSeqno = uint64_t((*pos)->getBySeqno() + 1);
                        break;
                    }
                    const auto nextSeqno = uint64_t((*next)->getBySeqno());
                    if (lastProcessedSeqno < nextSeqno) {
                        result.nextSeqno = nextSeqno;
                        break;
                    }
                    ++pos;
                    ++distance;
                }

                newCursor = std::make_shared<CheckpointCursor>(
                        name, ckptIt, pos, droppable, distance);
                result.cursor.setCursor(newCursor);
                result.position = *pos;
                break;
            }
        }

        if (result.nextSeqno == std::numeric_limits<uint64_t>::max()) {
            /*
             * We should never get here since this would mean that the sequence
             * number we are looking for is higher than anything currently
             * assigned and there is already an assert above for this case.
             */
            throw std::logic_error(
                    "CheckpointManager::registerCursorBySeqno the sequences "
                    "number is higher than anything currently assigned");
        }
    }

    Expects(newCursor);

    // If a cursor with the same name exists remove it before adding the new one
    auto itr = cursors.find(name);
    if (itr != cursors.end()) {
        // Place a temporary entry in the map for the new cursor. This ensures
        // that the checkpoint of the cursor cannot become eligible for eager
        // removal (triggering a use-after-free situation)
        const auto tempName = "temp" + name;
        cursors[tempName] = newCursor;
        removeCursor(lh, itr->second.get());
        cursors.erase(tempName);
    }

    // Finally save the newCursor
    cursors[name] = newCursor;

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
    return removeCursor(lh, cursor);
}

void CheckpointManager::removeBackupPersistenceCursor() {
    std::lock_guard<std::mutex> lh(queueLock);
    const auto res = removeCursor(lh, cursors.at(backupPCursorName).get());
    Expects(res);

    // Reset (recreate) the potential stats overcounts as our flush was
    // successful
    persistenceFailureStatOvercounts = AggregatedFlushStats();
}

AggregatedFlushStats CheckpointManager::resetPersistenceCursor() {
    std::lock_guard<std::mutex> lh(queueLock);

    // Note: the logic here relies on the existing cursor copy-ctor and
    //  CM::removeCursor function for getting the checkpoint num-cursors
    //  computation right

    // 1) Remove the existing pcursor
    auto remResult = removeCursor(lh, persistenceCursor);
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
    remResult = removeCursor(lh, backup);
    Expects(remResult);

    // Swap the stat counts to reset them for the next flush - return the
    // one we accumulated for the caller to adjust the VBucket stats
    AggregatedFlushStats ret;
    std::swap(ret, persistenceFailureStatOvercounts);

    return ret;
}

bool CheckpointManager::removeCursor(const std::lock_guard<std::mutex>& lh,
                                     CheckpointCursor* cursor) {
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
                 cursor->getName(),
                 vb.getId());

    cursor->invalidate();

    // find the current checkpoint before erasing the cursor, if there
    // are no other owners of the cursor it may be destroyed.
    auto* checkpoint = cursor->getCheckpoint()->get();

    if (cursors.erase(cursor->getName()) == 0) {
        throw std::logic_error(
                "CheckpointManager::removeCursor: " + to_string(vb.getId()) +
                " Failed to remove cursor: " + cursor->getName());
    }

    if (isEligibleForEagerRemoval(*checkpoint)) {
        // after removing `cursor`, perhaps the oldest checkpoint is now
        // unreferenced, and can be removed. BUT - this cursor has been
        // removed, not just moved to the next checkpoint. Therefore,
        // multiple checkpoints may be eligible for removal. Check for multiple,
        // not just the oldest.
        scheduleDestruction(extractClosedUnrefCheckpoints(lh));
    }

    /**
     * The code bellow is for unit test purposes only and is designed to inject
     * code to simulate a race condition with the destruction of a cursor. See
     * for more information MB-36146
     */
    if (runGetItemsHook) {
        queueLock.unlock();
        runGetItemsHook(cursor, vb.getId());
        queueLock.lock();
    }

    return true;
}

bool CheckpointManager::isEligibleForEagerRemoval(
        const Checkpoint& checkpoint) const {
    return checkpointConfig.isEagerCheckpointRemoval() &&
           &checkpoint == checkpointList.front().get() &&
           checkpoint.isNoCursorsInCheckpoint() &&
           checkpoint.getState() == checkpoint_state::CHECKPOINT_CLOSED;
}

void CheckpointManager::maybeScheduleDestruction(const Checkpoint& checkpoint) {
    if (isEligibleForEagerRemoval(checkpoint)) {
        CheckpointList forDestruction;

        // checkpoints must be removed in order, only the oldest is eligible
        // when removing checkpoints one at a time.
        // When cursors are removed, multiple checkpoints may unreffed and
        // can be removed together, but that is handled in removeCursor()

        // using O(1) overload of splice which takes a distance.
        forDestruction.splice(forDestruction.begin(),
                              checkpointList,
                              checkpointList.begin(),
                              std::next(checkpointList.begin()),
                              1 /* distance */);

        scheduleDestruction(std::move(forDestruction));
    }
}

void CheckpointManager::scheduleDestruction(CheckpointList&& toRemove) {
    if (toRemove.empty()) {
        return;
    }

    updateStatsForCheckpointRemoval(toRemove);
    checkpointDisposer(std::move(toRemove), vb.getId());
}

CheckpointManager::ReleaseResult
CheckpointManager::removeClosedUnrefCheckpoints() {
    // This function is executed periodically by the non-IO dispatcher.

    // We need to acquire the CM lock for extracting checkpoints from the
    // CheckpointList. But the actual deallocation must be lock-free, as it is
    // an expensive operation that has been already proven to degrade frontend
    // throughput if performed under lock.
    CheckpointList toRelease;
    {
        std::lock_guard<std::mutex> lh(queueLock);
        maybeCreateNewCheckpoint(lh);
        toRelease = extractClosedUnrefCheckpoints(lh);
    }
    // CM lock released here

    if (toRelease.empty()) {
        return {0, 0};
    }

    auto released = updateStatsForCheckpointRemoval(toRelease);

    // the provided disposer may queue checkpoints for destruction in a
    // background task, or may do nothing - in that case toRelease will be
    // destroyed when it goes out of scope.
    //
    // Note: The current behaviour in production is that checkpoints are queued
    // for destruction into the DestroyerTask. The in-place deallocation happens
    // only in some test code currently.
    checkpointDisposer(std::move(toRelease), vb.getId());

    return released;
}

CheckpointManager::ReleaseResult
CheckpointManager::updateStatsForCheckpointRemoval(
        const CheckpointList& toRemove) {
    // Update stats and compute return value
    size_t numItemsRemoved = 0;
    size_t memoryReleased = 0;
    for (const auto& checkpoint : toRemove) {
        numItemsRemoved += checkpoint->getNumItems();
        memoryReleased += checkpoint->getMemUsage();
        checkpoint->detachFromManager();
    }
    numItems.fetch_sub(numItemsRemoved);
    stats.itemsRemovedFromCheckpoints.fetch_add(numItemsRemoved);
    memFreedByCheckpointRemoval += memoryReleased;

    EP_LOG_DEBUG(
            "CheckpointManager::updateStatsForCheckpointRemoval: Removed {} "
            "checkpoints, {} items, {} bytes from {}",
            toRemove.size(),
            numItemsRemoved,
            memoryReleased,
            vb.getId());

    return {numItemsRemoved, memoryReleased};
}

CheckpointManager::ReleaseResult
CheckpointManager::expelUnreferencedCheckpointItems() {
    // trigger the overheadChangedCallback if the overhead is different
    // when this helper is destroyed - which occurs _after_ the destruction
    // of expelledItems (declared below)
    auto overheadCheck =
            gsl::finally([pre = getMemOverheadAllocatorBytes(), this]() {
                auto post = getMemOverheadAllocatorBytes();
                if (pre != post) {
                    overheadChangedCallback(post - pre);
                }
            });

    ExtractItemsResult extractRes;
    {
        std::lock_guard<std::mutex> lh(queueLock);

        // The method returns the expelled items in the expelledItems queue
        // thereby ensuring they still have a reference whilst the queuelock is
        // being held.
        extractRes = extractItemsToExpel(lh);
    }

    const auto numItemsExpelled = extractRes.getNumItems();
    if (numItemsExpelled == 0) {
        // Nothing expelled, done
        return {};
    }

    stats.itemsExpelledFromCheckpoints.fetch_add(numItemsExpelled);
    numItems.fetch_sub(numItemsExpelled);

    // queueLock already released here, O(N) deallocation is lock-free
    const auto queuedItemsMemReleased = extractRes.deleteItems();

    // Test hook that executes before CM::lock is re-acquired
    expelHook();

    {
        // Acquire the queueLock just for the very short time necessary for
        // updating the checkpoint's queued-items mem-usage.
        //
        // Note that the presence of the expel-cursor at this step ensures that
        // the checkpoint is still in the CheckpointList; unless the VBucket has
        // rolled-back (see the following).
        // Expel-cursor is released once extractRes is destroyed at caller.
        std::lock_guard<std::mutex> lh(queueLock);
        auto* checkpoint = extractRes.getCheckpoint();
        Expects(checkpoint);

        // Expel always touches the oldest checkpoint in the list.
        // The checkpoint touched by Expel might not exist anymore if the
        // VBucket has rolled-back. Just give up in that case.
        if (checkpoint != checkpointList.begin()->get()) {
            return {0, 0};
        }

        Expects(extractRes.getExpelCursor().getCheckpoint()->get() ==
                checkpoint);
        checkpoint->applyQueuedItemsMemUsageDecrement(queuedItemsMemReleased);
    }

    // Mem-usage estimation of expelled items is the sum of:
    // 1. Memory used by all items. For each item this is calculated as
    //    sizeof(Item) + key size + value size.
    // 2. Memory used to hold the items in the checkpoint list.
    //
    // The latter is computed as 3 pointers per element in the list, ie forward,
    // backwards and element pointers per each list node.
    // Note: Although quite accurate, that is still an estimation. For example,
    // debugging on Windows shows that space for 1 extra element is allocated
    // for an empty list, plus extra 16 bytes on Debug CRT.
    //
    // This is an optimistic estimate as it assumes that the reference count of
    // each queued_item drops to zero as soon as the 'expelledItems' container
    // goes out of scope, thus allowing memory to be freed. Which might not be
    // immediately the case if any component (eg, a DCP stream) still references
    // the queued items.
    const auto estimatedMemRecovered =
            queuedItemsMemReleased +
            (Checkpoint::per_item_queue_overhead * numItemsExpelled);

    memFreedByExpel += estimatedMemRecovered;

    return {numItemsExpelled, estimatedMemRecovered};
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
        // checkpoint pointed by special-cursor. Which also implies by logic
        // that we never drop cursors in the open checkpoint.
        // Note that the invariant applies that (backup-pcursor <= pcursor), if
        // the backup cursor exists. So that can be exploited to simplify
        // the logic further here.

        const auto backupExists =
                cursors.find(backupPCursorName) != cursors.end();
        const auto& specialCursor = backupExists
                                            ? *cursors.at(backupPCursorName)
                                            : *persistenceCursor;

        getListOfCursorsToDropHook();

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
            if (cursor->isDroppable() &&
                (*cursor->getCheckpoint())->getId() <
                        (*specialCursor.getCheckpoint())->getId()) {
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
            if (cursor->isDroppable() &&
                (*cursor->getCheckpoint())->getId() < id) {
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
        backupIt->second->getCheckpoint()->get() == oldestCkpt.get()) {
        // Backup cursor in oldest checkpoint, checkpoint(s) will be eligible
        // for removal after backup cursor has gone
        return true;
    }
    // No backup cursor in CM, some other cursor is in oldest checkpoint
    return false;
}

void CheckpointManager::updateStatsForNewQueuedItem(
        const std::lock_guard<std::mutex>& lh, const queued_item& qi) {
    ++stats.totalEnqueued;
    if (checkpointConfig.isPersistenceEnabled()) {
        ++stats.diskQueueSize;
        vb.doStatsForQueueing(*qi, qi->size());
    }
}

bool CheckpointManager::queueDirty(
        queued_item& qi,
        const GenerateBySeqno generateBySeqno,
        const GenerateCas generateCas,
        PreLinkDocumentContext* preLinkDocumentContext,
        std::function<void(int64_t)> assignedSeqnoCallback) {
    std::lock_guard<std::mutex> lh(queueLock);

    maybeCreateNewCheckpoint(lh);

    auto* openCkpt = &getOpenCheckpoint(lh);

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
            const auto msg = fmt::format(
                    "CheckpointManager::queueDirty: Got status:{} when {} is "
                    "non-active:{}, item:[op:{}, seqno:{}, key:<ud>{}</ud>], "
                    "lastBySeqno:{}, openCkpt:[start:{}, end:{}]",
                    to_string(result.status),
                    vb.getId(),
                    std::to_string(vb.getState()),
                    ::to_string(qi->getOperation()),
                    qi->getBySeqno(),
                    qi->getKey(),
                    lastBySeqno,
                    openCkpt->getSnapshotStartSeqno(),
                    openCkpt->getSnapshotEndSeqno());
            throw std::logic_error(msg);
        }

        // To process this item, create a new (empty) checkpoint which we can
        // then re-attempt the enqueuing.
        // Note this uses the lastBySeqno for snapStart / End.
        addNewCheckpoint(lh);
        openCkpt = &getOpenCheckpoint(lh);
        result = openCkpt->queueDirty(qi);
        if (result.status != QueueDirtyStatus::SuccessNewItem) {
            throw std::logic_error("CheckpointManager::queueDirty(vb:" +
                                   vb.getId().to_string() +
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
        updateStatsForNewQueuedItem(lh, qi);
        return true;
    case QueueDirtyStatus::FailureDuplicateItem:
        throw std::logic_error(
                "CheckpointManager::queueDirty: Got invalid "
                "result:FailureDuplicateItem - should have been handled with "
                "retry.");
    }
    folly::assume_unreachable();
}

void CheckpointManager::queueSetVBState() {
    // Grab the vbstate before the queueLock (avoid a lock inversion)
    auto vbstate = vb.getTransitionState();

    {
        // Take lock to serialize use of {lastBySeqno} and to queue op.
        std::lock_guard<std::mutex> lh(queueLock);

        // Create the setVBState operation, and enqueue it.
        queued_item item =
                createCheckpointMetaItem(0, queue_op::set_vbucket_state);

        // We need to set the cas of the item as two subsequent
        // set_vbucket_state items will have the same seqno and the flusher
        // needs a way to determine which is the latest so that we persist the
        // correct state. We do this 'atomically' as we are holding the
        // ::queueLock.
        item->setCas(vb.nextHLCCas());

        // MB-43528: To ensure that we have a reasonable queue_age stat we need
        // to set the queue time here.
        item->setQueuedTime();

        // Store a JSON version of the vbucket transition data in the value
        vbstate.toItem(*item);

        auto& openCkpt = getOpenCheckpoint(lh);
        const auto result = openCkpt.queueDirty(item);

        if (result.status == QueueDirtyStatus::SuccessNewItem) {
            ++numItems;
            updateStatsForNewQueuedItem(lh, item);
        } else {
            auto msg = fmt::format(
                    "CheckpointManager::queueSetVBState: {} "
                    "expected: SuccessNewItem, got: {} with byte "
                    "diff of: {} after queueDirty.",
                    vb.getId().to_string(),
                    to_string(result.status),
                    result.successExistingByteDiff);
            throw std::logic_error(msg);
        }
    }

    // IMPORTANT: queueLock already released when we called into ActiveStream.
    // We would introduce potential deadlock-by-lock-inversion with the many
    // code paths that acquire streamLock->queueLock otherwise.
    //
    // @todo MB-53778: Currently we potentially notify unnecessary streams.
    //   The side effect isn't expected to cause any major issue. An idle
    //   DCP Producer might be unnecessarily woken up, but immediately put
    //   again to sleep at the first step(). Less of a problem for busy DCP
    //   Producers: they are in their step() loop anyway, so any attempt of
    //   notification is actually a NOP.
    vb.notifyReplication();
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
                    vb.getId());
        return {};
    }

    auto& cursor = *cursorPtr;

    // Fetch whole checkpoints; as long as we don't exceed the approx item
    // limit.
    const auto& checkpoint = **cursor.getCheckpoint();
    ItemsForCursor result(checkpoint.getCheckpointType(),
                          checkpoint.getMaxDeletedRevSeqno(),
                          checkpoint.getHighCompletedSeqno(),
                          checkpoint.getVisibleSnapshotEndSeqno(),
                          checkpoint.getHistorical());

    // Only enforce a hard limit for Disk Checkpoints (i.e backfill). This will
    // prevent huge memory growth due to flushing vBuckets on replicas during a
    // rebalance. Memory checkpoints can still grow unbounded due to max number
    // of checkpoints constraint, but that should be solved by reducing
    // Checkpoint size and increasing max number.
    bool hardLimit = (*cursor.getCheckpoint())->isDiskCheckpoint() &&
                     cursor.getName() == pCursorName;

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
            const auto& checkpoint = **cursor.getCheckpoint();
            result.checkpointType = checkpoint.getCheckpointType();
            result.ranges.push_back({{checkpoint.getSnapshotStartSeqno(),
                                      checkpoint.getSnapshotEndSeqno()},
                                     checkpoint.getHighCompletedSeqno(),
                                     checkpoint.getHighPreparedSeqno()});
            enteredNewCp = false;

            // As we cross into new checkpoints, update the maxDeletedRevSeqno
            // iff the new checkpoint has one recorded, and it's larger than the
            // previous value.
            if (checkpoint.getMaxDeletedRevSeqno().value_or(0) >
                result.maxDeletedRevSeqno.value_or(0)) {
                result.maxDeletedRevSeqno = checkpoint.getMaxDeletedRevSeqno();
            }
        }

        queued_item& qi = *(cursor.getPos());
        items.push_back(qi);
        itemCount++;

        if (qi->isSetVBState()) {
            // A set-vbucket-state overrides the maxCas - this ensures that in
            // the seqno order any prior "poisoned" Items are ignored if the
            // HLC was reset by a forceMaxCas
            result.maxCas = qi->getCas();
        } else {
            result.maxCas = std::max(result.maxCas, qi->getCas());
        }

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

            // MB-36971: We never want to return multiple Disk checkpoints, So,
            // break if we have just finished processing a Disk Checkpoint,
            // regardless of what comes next.
            if ((*cursor.getCheckpoint())->isDiskCheckpoint()) {
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
            if (canMoveCursorToNextCheckpoint(cursor)) {
                // We are now moving the cursor to the next checkpoint.
                // Given that that operation might make the old cursor's
                // checkpoint unreferenced, it might be removed as soon as the
                // cursors jumps (eager checkpoint removal). So, we need to
                // make our checkpoint-merge checks before performing the move.

                const auto current = cursor.getCheckpoint();
                const auto next = std::next(current);
                Expects(next != checkpointList.end());
                const auto canMerge = canBeMerged(lh, **current, **next);

                const auto moved = moveCursorToNextCheckpoint(cursor);
                Expects(moved);

                if (!canMerge) {
                    // The new checkpoint onto which cursor moved to is
                    // incompatible with the previous checkpoint, so just break
                    // the loop and return.
                    break;
                }
            }
        }
    }

    if (getGlobalBucketLogger()->should_log(spdlog::level::debug)) {
        std::string ranges;
        for (const auto& range : result.ranges) {
            fmt::format_to(std::back_inserter(ranges),
                           "[{{{},{}}} HCS:{} HPS:{}],",
                           range.range.getStart(),
                           range.range.getEnd(),
                           to_string_or_none(range.highCompletedSeqno),
                           to_string_or_none(range.highPreparedSeqno));
        }
        if (!ranges.empty()) {
            ranges.pop_back();
        }

        EP_LOG_DEBUG(
                "CheckpointManager::getItemsForCursor() "
                "cursor:{} result:{{items:{} ranges:size:{} {} "
                "moreAvailable:{}}}",
                cursor.getName(),
                uint64_t(itemCount),
                result.ranges.size(),
                ranges,
                result.moreAvailable);
    }

    cursor.incrNumVisit();

    return result;
}

bool CheckpointManager::incrCursor(CheckpointCursor &cursor) {
    if (!cursor.valid()) {
        return false;
    }

    // Move forward
    cursor.incrPos();

    if (cursor.getPos() != (*cursor.getCheckpoint())->end()) {
        return true;
    }

    if (!moveCursorToNextCheckpoint(cursor)) {
        // There is no further checkpoint to move the cursor to, reset it to the
        // original position
        cursor.decrPos();
        return false;
    }

    return incrCursor(cursor);
}

void CheckpointManager::notifyFlusher() {
    if (flusherCB) {
        Vbid vbid = vb.getId();
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
    std::lock_guard<std::mutex> lh(queueLock);
    dump(lh);
}

void CheckpointManager::dump(const std::lock_guard<std::mutex>& lh) const {
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

    // Use lastBySeqno + 1 as that will be the seqno of the first item belonging
    // to this checkpoint
    addOpenCheckpoint(lastBySeqno + 1,
                      lastBySeqno + 1,
                      maxVisibleSeqno,
                      {},
                      0, // HPS=0 because we have correct val on disk and in PDM
                      CheckpointType::Memory,
                      vb.isHistoryRetentionEnabled()
                              ? CheckpointHistorical::Yes
                              : CheckpointHistorical::No);
    resetCursors();
}

void CheckpointManager::resetCursors() {
    for (auto& pair : cursors) {
        // Reset the cursor to the very begin of the checkpoint list, ie first
        // item in the first checkpoint
        (*pair.second).repositionAtCheckpointBegin(checkpointList.begin());
    }
}

bool CheckpointManager::canMoveCursorToNextCheckpoint(
        const CheckpointCursor& cursor) const {
    if (!cursor.valid()) {
        return false;
    }

    if ((*cursor.getCheckpoint())->getState() == CHECKPOINT_OPEN) {
        return false;
    }

    return true;
}

bool CheckpointManager::moveCursorToNextCheckpoint(CheckpointCursor& cursor) {
    if (!canMoveCursorToNextCheckpoint(cursor)) {
        return false;
    }

    const auto prev = cursor.getCheckpoint();
    Expects((*prev)->getState() == CHECKPOINT_CLOSED);
    const auto next = std::next(prev);
    // There must be at least an open checkpoint
    Expects(next != checkpointList.end());

    // Move the cursor to the next checkpoint.
    // Note: This also updates the cursor accounting for both old/new checkpoint
    cursor.repositionAtCheckpointBegin(next);

    Expects((*cursor.getPos())->getOperation() == queue_op::empty);

    // by advancing the cursor, the previous checkpoint became unreferenced,
    // and may be removable now.
    // only act if the unreffed checkpoint is the oldest closed checkpoint.
    maybeScheduleDestruction(**prev);

    return true;
}

size_t CheckpointManager::getNumOpenChkItems() const {
    std::lock_guard<std::mutex> lh(queueLock);
    return getOpenCheckpoint(lh).getNumItems();
}

void CheckpointManager::checkOpenCheckpoint(
        const std::lock_guard<std::mutex>& lh) {
    const auto& openCkpt = getOpenCheckpoint(lh);

    // Create the new open checkpoint if any of the following conditions is
    // satisfied:
    // (1) force creation due to online update, high memory usage, or enqueueing
    //     an op we cannot de-dupe (i.e. an abort, commit, or prepare)
    // (2) current open checkpoint has reached the max number of items allowed
    // (3) the age of the current open checkpoint is greater than the threshold
    //     @todo MB-48038: allow disabling the time-based trigger via config
    // (4) current open checkpoint has reached its max size (in bytes)

    // Note on the backport for MB-52276.
    // In the original Trinity code change here I replaced all the usages of
    //     openCkpt.getNumItems() > 0
    // by
    //     openCkpt.hasNonMetaItems()
    //
    // The reason is that Checkpoint::numItems is "broken" by that ItemExpel
    // doesn't update it (which is what we fix in MB-52276), so we don't want
    // to rely on it anymore.
    // Now, in Trinity we don't have any "num-item trigger" when we make that
    // change, so here we have to keep using the checkpoint's numItems for that.
    //
    // Not a big problem though. As soon as the backport of MB-52276 is complete
    // numItems will be fixed and so will be the "num-item trigger". The only
    // side effect will be that numItems will account for both meta and non-meta
    // items, so the "num-item trigger" semantic will change slightly by that.
    //
    // Note: Removing the "num-item trigger" was done in Trinity as part of
    // MB-50984 - Different MB, we are trying to limit the scope of the backport

    const auto numItems = openCkpt.getNumItems();
    const auto numItemsTrigger =
            checkpointConfig.isItemNumBasedNewCheckpoint() &&
            numItems >= checkpointConfig.getCheckpointMaxItems();

    const auto openCkptAge = ep_real_time() - openCkpt.getCreationTime();
    const auto timeTrigger =
            openCkpt.hasNonMetaItems() &&
            (openCkptAge >= checkpointConfig.getCheckpointPeriod());

    // Note: The condition ensures that we always allow at least 1 non-meta item
    //  in the open checkpoint, regardless of any setting.
    const auto memTrigger =
            (openCkpt.getMemUsage() >= vb.getCheckpointMaxSize()) &&
            openCkpt.hasNonMetaItems();

    if (numItemsTrigger || timeTrigger || memTrigger) {
        addNewCheckpoint(lh);
    }
}

size_t CheckpointManager::getNumItemsForCursor(
        const CheckpointCursor* cursor) const {
    std::lock_guard<std::mutex> lh(queueLock);
    return getNumItemsForCursor(lh, cursor);
}

size_t CheckpointManager::getNumItemsForCursor(
        const std::lock_guard<std::mutex>& lh,
        const CheckpointCursor* cursor) const {
    if (cursor && cursor->valid()) {
        size_t items = cursor->getRemainingItemsInCurrentCheckpoint();
        CheckpointList::const_iterator chkptIterator(cursor->getCheckpoint());
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

    auto it = std::next(cursor.getPos());
    return it == (*(cursor.getCheckpoint()))->end() ||
           (*it)->getOperation() == queue_op::checkpoint_end;
}

void CheckpointManager::createSnapshot(
        uint64_t snapStartSeqno,
        uint64_t snapEndSeqno,
        std::optional<uint64_t> highCompletedSeqno,
        CheckpointType checkpointType,
        uint64_t visibleSnapEnd,
        CheckpointHistorical historical) {
    if (isDiskCheckpointType(checkpointType)) {
        Expects(highCompletedSeqno.has_value());
    }

    std::lock_guard<std::mutex> lh(queueLock);

    auto& openCkpt = getOpenCheckpoint(lh);

    if (!openCkpt.modifiedByExpel() && !openCkpt.hasNonMetaItems()) {
        openCkpt.setSnapshotStartSeqno(snapStartSeqno);
        openCkpt.setSnapshotEndSeqno(snapEndSeqno, visibleSnapEnd);
        openCkpt.setCheckpointType(checkpointType);
        openCkpt.setHistorical(historical);
        openCkpt.setHighCompletedSeqno(highCompletedSeqno);
        return;
    }

    addNewCheckpoint(lh,
                     snapStartSeqno,
                     snapEndSeqno,
                     visibleSnapEnd,
                     highCompletedSeqno,
                     checkpointType,
                     historical);
}

void CheckpointManager::extendOpenCheckpoint(uint64_t snapEnd,
                                             uint64_t visibleSnapEnd) {
    std::lock_guard<std::mutex> lh(queueLock);
    auto& ckpt = getOpenCheckpoint(lh);

    if (ckpt.isDiskCheckpoint()) {
        throw std::logic_error(
                "CheckpointManager::extendOpenCheckpoint: Cannot extend a Disk "
                "checkpoint");
    }

    ckpt.setSnapshotEndSeqno(snapEnd, visibleSnapEnd);
}

snapshot_info_t CheckpointManager::getSnapshotInfo() {
    std::lock_guard<std::mutex> lh(queueLock);

    const auto& openCkpt = getOpenCheckpoint(lh);

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
    //
    // Note: Condition on "modifiedByExpel" added in MB-39344 for ensuring that
    // the semantic here doesn't change by the new ItemExpel semantic.
    // Actually new unit tests cover this code path and prove that there is no
    // semantic change here caused by MB-39344. But, the additional condition
    // covers us by any unexpected (and uncaught in unit tests) behaviour.
    if (!openCkpt.modifiedByExpel() && !openCkpt.hasNonMetaItems() &&
        static_cast<uint64_t>(lastBySeqno) < info.range.getStart()) {
        info.range = snapshot_range_t(lastBySeqno, lastBySeqno);
    }

    return info;
}

uint64_t CheckpointManager::getOpenSnapshotStartSeqno() const {
    std::lock_guard<std::mutex> lh(queueLock);
    const auto& openCkpt = getOpenCheckpoint(lh);

    return openCkpt.getSnapshotStartSeqno();
}

uint64_t CheckpointManager::getVisibleSnapshotEndSeqno() const {
    // Follow what getSnapshotInfo does, but only for visible end-seqno
    std::lock_guard<std::mutex> lh(queueLock);
    const auto& openCkpt = getOpenCheckpoint(lh);

    // This clause is also in getSnapshotInfo, if we have no items for the open
    // checkpoint, return the "end" as maxVisible
    //
    // Note: Condition on "modifiedByExpel" added in MB-39344 for ensuring that
    // the logic here doesn't change by the new ItemExpel semantic.
    if (!openCkpt.modifiedByExpel() && !openCkpt.hasNonMetaItems() &&
        static_cast<uint64_t>(lastBySeqno) < openCkpt.getSnapshotStartSeqno()) {
        return maxVisibleSeqno;
    }

    return openCkpt.getVisibleSnapshotEndSeqno();
}

queued_item CheckpointManager::createCheckpointMetaItem(uint64_t checkpointId,
                                                        queue_op op) {
    if (!isMetaQueueOp(op)) {
        throw std::invalid_argument(
                "CheckpointManager::createCheckpointMetaItem: op " +
                to_string(op) + " is non-meta");
    }

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
    uint64_t seqno = lastBySeqno + 1;
    StoredDocKey key(to_string(op), CollectionID::System);

    return queued_item(new Item(key, vb.getId(), op, checkpointId, seqno));
}

uint64_t CheckpointManager::createNewCheckpoint() {
    std::lock_guard<std::mutex> lh(queueLock);
    addNewCheckpoint(lh);
    return getOpenCheckpointId(lh);
}

size_t CheckpointManager::getMemOverheadAllocatorBytes(
        const std::lock_guard<std::mutex>& lh) const {
    size_t memUsage = 0;
    for (const auto& checkpoint : checkpointList) {
        memUsage += checkpoint->getMemOverheadAllocatorBytes();
    }
    return memUsage;
}

size_t CheckpointManager::getMemUsage() const {
    // Atomic, don't need to acquire the CM lock
    return memUsage;
}

size_t CheckpointManager::getQueuedItemsMemUsage() const {
    std::lock_guard<std::mutex> lh(queueLock);
    size_t usage = 0;
    for (const auto& checkpoint : checkpointList) {
        usage += checkpoint->getQueuedItemsMemUsage();
    }
    return usage;
}

size_t CheckpointManager::getMemoryUsageOfUnrefCheckpoints() const {
    std::lock_guard<std::mutex> lh(queueLock);

    size_t memUsage = 0;
    for (const auto& checkpoint : checkpointList) {
        if (checkpoint->isNoCursorsInCheckpoint()) {
            memUsage += checkpoint->getMemUsage();
        } else {
            break;
        }
    }
    return memUsage;
}

// @todo MB-48587: Suboptimal O(N) implementation for all mem-overhead functions
//  below, optimized in a dedicated patch.

size_t CheckpointManager::getMemOverheadAllocatorBytes() const {
    std::lock_guard<std::mutex> lh(queueLock);
    return getMemOverheadAllocatorBytes(lh);
}

size_t CheckpointManager::getMemOverheadAllocatorBytesQueue() const {
    std::lock_guard<std::mutex> lh(queueLock);
    size_t usage = 0;
    for (const auto& checkpoint : checkpointList) {
        usage += checkpoint->getWriteQueueAllocatorBytes();
    }
    return usage;
}

size_t CheckpointManager::getMemOverheadAllocatorBytesIndex() const {
    std::lock_guard<std::mutex> lh(queueLock);
    size_t usage = 0;
    for (const auto& checkpoint : checkpointList) {
        usage += checkpoint->getKeyIndexAllocatorBytes();
    }
    return usage;
}

size_t CheckpointManager::getMemOverheadAllocatorBytesIndexKey() const {
    std::lock_guard<std::mutex> lh(queueLock);
    size_t usage = 0;
    for (const auto& checkpoint : checkpointList) {
        usage += checkpoint->getKeyIndexKeyAllocatorBytes();
    }
    return usage;
}

size_t CheckpointManager::getMemOverhead() const {
    std::lock_guard<std::mutex> lh(queueLock);
    size_t usage = 0;
    for (const auto& checkpoint : checkpointList) {
        usage += checkpoint->getMemOverhead();
    }
    return usage;
}

size_t CheckpointManager::getMemOverheadQueue() const {
    std::lock_guard<std::mutex> lh(queueLock);
    size_t usage = 0;
    for (const auto& checkpoint : checkpointList) {
        usage += checkpoint->getMemOverheadQueue();
    }
    return usage;
}

size_t CheckpointManager::getMemOverheadIndex() const {
    std::lock_guard<std::mutex> lh(queueLock);
    size_t usage = 0;
    for (const auto& checkpoint : checkpointList) {
        usage += checkpoint->getMemOverheadIndex();
    }
    return usage;
}

void CheckpointManager::addStats(const AddStatFn& add_stat,
                                 const CookieIface* cookie) {
    std::lock_guard<std::mutex> lh(queueLock);
    std::array<char, 256> buf;

    try {
        const auto vbucketId = vb.getId();
        checked_snprintf(buf.data(),
                         buf.size(),
                         "vb_%d:open_checkpoint_id",
                         vb.getId().get());
        add_casted_stat(buf.data(), getOpenCheckpointId(lh), add_stat, cookie);
        checked_snprintf(buf.data(),
                         buf.size(),
                         "vb_%d:last_closed_checkpoint_id",
                         vbucketId.get());
        add_casted_stat(
                buf.data(), getLastClosedCheckpointId(lh), add_stat, cookie);
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
                        getOpenCheckpoint(lh).getNumItems(),
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
                            getNumItemsForCursor(lh, persistenceCursor),
                            add_stat,
                            cookie);
        }
        checked_snprintf(
                buf.data(), buf.size(), "vb_%d:mem_usage", vbucketId.get());
        add_casted_stat(buf.data(), memUsage, add_stat, cookie);

        for (const auto& cursor : cursors) {
            checked_snprintf(buf.data(),
                             buf.size(),
                             "vb_%d:%s:cursor_checkpoint_id",
                             vbucketId.get(),
                             cursor.second->getName().c_str());
            add_casted_stat(buf.data(),
                            (*(cursor.second->getCheckpoint()))->getId(),
                            add_stat,
                            cookie);

            checked_snprintf(buf.data(),
                             buf.size(),
                             "vb_%d:%s:cursor_seqno",
                             vbucketId.get(),
                             cursor.second->getName().c_str());
            add_casted_stat(buf.data(),
                            (*(cursor.second->getPos()))->getBySeqno(),
                            add_stat,
                            cookie);

            checked_snprintf(buf.data(),
                             buf.size(),
                             "vb_%d:%s:cursor_op",
                             vbucketId.get(),
                             cursor.second->getName().c_str());
            add_casted_stat(
                    buf.data(),
                    to_string((*(cursor.second->getPos()))->getOperation()),
                    add_stat,
                    cookie);

            checked_snprintf(buf.data(),
                             buf.size(),
                             "vb_%d:%s:cursor_distance",
                             vbucketId.get(),
                             cursor.second->getName().c_str());
            add_casted_stat(
                    buf.data(), cursor.second->getDistance(), add_stat, cookie);

            checked_snprintf(buf.data(),
                             buf.size(),
                             "vb_%d:%s:num_visits",
                             vbucketId.get(),
                             cursor.second->getName().c_str());
            add_casted_stat(
                    buf.data(), cursor.second->getNumVisit(), add_stat, cookie);

            if (cursor.second.get() != persistenceCursor) {
                checked_snprintf(buf.data(),
                                 buf.size(),
                                 "vb_%d:%s:num_items_for_cursor",
                                 vbucketId.get(),
                                 cursor.second->getName().c_str());
                add_casted_stat(buf.data(),
                                getNumItemsForCursor(lh, cursor.second.get()),
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
    other.takeAndResetCursorsHook();

    Cursor otherPCursor;
    cursor_index otherCursors;
    {
        std::lock_guard<std::mutex> otherLH(other.queueLock);
        otherPCursor = other.pCursor;
        otherCursors = std::move(other.cursors);
        other.cursors.clear();
    }

    std::lock_guard<std::mutex> lh(queueLock);
    pCursor = std::move(otherPCursor);
    persistenceCursor = pCursor.lock().get();
    for (auto& cursor : otherCursors) {
        cursors[cursor.second->getName()] = std::move(cursor.second);
    }
    resetCursors();
}

bool CheckpointManager::isOpenCheckpointDisk() {
    std::lock_guard<std::mutex> lh(queueLock);
    return checkpointList.back()->isDiskCheckpoint();
}

bool CheckpointManager::isOpenCheckpointInitialDisk() {
    std::lock_guard<std::mutex> lh(queueLock);
    return checkpointList.back()->isInitialDiskCheckpoint();
}

void CheckpointManager::updateStatsForStateChange(vbucket_state_t from,
                                                  vbucket_state_t to) {
    std::lock_guard<std::mutex> lh(queueLock);
    if (from == vbucket_state_replica && to != vbucket_state_replica) {
        // vbucket is changing state away from replica, it's memory usage
        // should no longer be accounted for as a replica.
        stats.replicaCheckpointOverhead -= getMemOverheadAllocatorBytes(lh);
    } else if (from != vbucket_state_replica && to == vbucket_state_replica) {
        // vbucket is changing state to _become_ a replica, it's memory usage
        // _should_ be accounted for as a replica.
        stats.replicaCheckpointOverhead += getMemOverheadAllocatorBytes(lh);
    }
}

void CheckpointManager::setOverheadChangedCallback(
        std::function<void(int64_t delta)> callback) {
    std::lock_guard<std::mutex> lh(queueLock);
    overheadChangedCallback = std::move(callback);

    overheadChangedCallback(getMemOverheadAllocatorBytes(lh));
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

bool CheckpointManager::hasItemsForCursor(
        const CheckpointCursor& cursor) const {
    std::lock_guard<std::mutex> lh(queueLock);

    if (!cursor.valid()) {
        return false;
    }

    // Cursor not in the last checkpoint? Surely at least items to process in
    // the open checkpoint
    if ((*cursor.getCheckpoint())->getState() == CHECKPOINT_CLOSED) {
        return true;
    }

    // Cursor in the open checkpoint
    return cursor.getRemainingItemsInCurrentCheckpoint() > 0;
}

size_t CheckpointManager::getNumCursors() const {
    std::lock_guard<std::mutex> lh(queueLock);
    return cursors.size();
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
        const std::lock_guard<std::mutex>& lh) {
    // Only the active can shape the CheckpointList
    if (vb.getState() != vbucket_state_active) {
        return;
    }

    if (checkpointList.size() < checkpointConfig.getMaxCheckpoints() ||
        (checkpointList.size() == checkpointConfig.getMaxCheckpoints() &&
         checkpointList.front()->isNoCursorsInCheckpoint())) {
        // CM state pre-conditions allow creating a new checkpoint.

        // Create a new checkpoint if required.
        checkOpenCheckpoint(lh);
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
        it = getLowestCursor(lh)->getCheckpoint();
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
    if (entry == cursors.end()) {
        return {};
    }
    return entry->second;
}

CheckpointManager::ExtractItemsResult::ExtractItemsResult() = default;

CheckpointManager::ExtractItemsResult::ExtractItemsResult(
        CheckpointQueue&& items,
        CheckpointManager* manager,
        std::shared_ptr<CheckpointCursor> expelCursor,
        Checkpoint* checkpoint)
    : items(std::move(items)),
      manager(manager),
      expelCursor(std::move(expelCursor)),
      checkpoint(checkpoint) {
}

CheckpointManager::ExtractItemsResult::~ExtractItemsResult() {
    if (manager) {
        manager->removeCursor(expelCursor.get());
    }
}

CheckpointManager::ExtractItemsResult::ExtractItemsResult(
        CheckpointManager::ExtractItemsResult&& other) {
    *this = std::move(other);
}

CheckpointManager::ExtractItemsResult&
CheckpointManager::ExtractItemsResult::operator=(ExtractItemsResult&& other) {
    items = std::move(other.items);
    manager = other.manager;
    other.manager = nullptr;
    expelCursor = std::move(other.expelCursor);
    checkpoint = other.checkpoint;
    other.checkpoint = nullptr;
    return *this;
}

size_t CheckpointManager::ExtractItemsResult::getNumItems() const {
    return items.size();
}

size_t CheckpointManager::ExtractItemsResult::deleteItems() {
    size_t memReleased = 0;
    for (auto it = items.begin(); it != items.end();) {
        memReleased += (*it)->size();
        it = items.erase(it);
    }
    return memReleased;
}

Checkpoint* CheckpointManager::ExtractItemsResult::getCheckpoint() const {
    return checkpoint;
}

const CheckpointCursor& CheckpointManager::ExtractItemsResult::getExpelCursor()
        const {
    return *expelCursor;
}

CheckpointManager::ExtractItemsResult CheckpointManager::extractItemsToExpel(
        const std::lock_guard<std::mutex>& lh) {
    const auto oldestCkptIterator = checkpointList.begin();
    Checkpoint* const oldestCheckpoint = oldestCkptIterator->get();

    if ((oldestCheckpoint->getNumCursorsInCheckpoint() == 0) &&
        oldestCheckpoint->getState() == checkpoint_state::CHECKPOINT_CLOSED) {
        // The oldest checkpoint is unreferenced and closed, therefore may be
        // deleted as a whole by the CheckpointMemRecoveryTask, expelling
        // everything from it one by one would be a waste of time. Cannot expel
        // from checkpoints which are not the oldest without leaving gaps in the
        // items a cursor would read.
        return {};
    }

    if (!oldestCheckpoint->hasNonMetaItems()) {
        // There are no mutation items in the checkpoint to expel.
        return {};
    }

    const auto lowestCursor = getLowestCursor(lh);

    if (lowestCursor) {
        // Sanity check - if the oldest checkpoint is referenced, the cursor
        // with the lowest seqno should be in that checkpoint.
        if (lowestCursor->getCheckpoint()->get() != oldestCheckpoint) {
            std::stringstream ss;
            ss << "CheckpointManager::extractItemsToExpel: (" << vb.getId()
               << ") lowest found cursor is not in the oldest "
                  "checkpoint. Oldest checkpoint ID: "
               << oldestCheckpoint->getId()
               << " lowSeqno: " << oldestCheckpoint->getMinimumCursorSeqno()
               << " highSeqno: " << oldestCheckpoint->getHighSeqno()
               << " snapStart: " << oldestCheckpoint->getSnapshotStartSeqno()
               << " snapEnd: " << oldestCheckpoint->getSnapshotEndSeqno()
               << ". Lowest cursor: " << lowestCursor->getName()
               << " seqno: " << (*lowestCursor->getPos())->getBySeqno()
               << " ckptID: " << (*lowestCursor->getCheckpoint())->getId();
            throw std::logic_error(ss.str());
        }

        // Note: Important check as this avoids decrementing the begin()
        // iterator in the following steps.
        if (lowestCursor->getPos() == oldestCheckpoint->begin()) {
            // Lowest cursor is at the checkpoint empty item, nothing to expel
            return {};
        }
    }

    // Calculate the extent of the items to expel based on the lowest cursor,
    // or in the case of no cursor - use the end of the checkpoint.
    // This position is then adjusted backwards to ensure we expel up to
    // a consistent seqno - i.e we should expel all or none of a given seqno.
    //
    // Note: distance is logically std::distance(begin, pos). If we have a
    // cursor then that's precalculated. Else, that's
    // std::distance(begin, std::prev(end)), which is (numElements - 1).
    auto iterator = lowestCursor ? lowestCursor->getPos()
                                 : std::prev(oldestCheckpoint->end());
    auto distance = lowestCursor ? lowestCursor->getDistance()
                                 : oldestCheckpoint->getNumberOfElements() - 1;

    // Note: If reached here iterator points to some position > begin()
    Expects(distance > 0);

    /*
     * Walk backwards over the checkpoint if not yet reached the dummy item,
     * and pointing to an item that either:
     * 1. has a subsequent entry with the same seqno (i.e. we don't want
     *    to expel some items but not others with the same seqno), or
     * 2. is pointing to a metadata item.
     */
    while ((iterator != oldestCheckpoint->begin()) &&
           ((std::next(iterator) != oldestCheckpoint->end() &&
             (*iterator)->getBySeqno() ==
                     (*std::next(iterator))->getBySeqno()) ||
            ((*iterator)->isCheckPointMetaItem()))) {
        --iterator;
        Expects(distance > 0);
        --distance;
    }

    // If pointing to the dummy item then cannot expel anything and so just
    // return.
    if (iterator == oldestCheckpoint->begin()) {
        return {};
    }

    // We allow expelling also the item pointed by cursor. For avoiding
    // invalid cursors, we need to reposition all the cursors that point to the
    // same item to a valid position. Given that we are expelling
    // the [checkpoint_start + 1, iterator] range, then the correct new
    // position for those cursors is checkpoint_start.
    //
    // Note 1: iterator <= lowestCursor. If in the '<' case, then we won't
    //  reposition anything
    //
    // Note 2: Repositioning at Checkpoint::begin() would be wrong as a cursor
    //  should never process a checkpoint_start multiple times
    for (auto& entry : cursors) {
        auto& cursor = entry.second;
        if (cursor->getPos() == iterator) {
            cursor->repositionAtCheckpointStart(cursor->getCheckpoint());
        }
    }

    auto expelledItems = oldestCheckpoint->expelItems(iterator, distance);

    // Re-compute the distance for all cursors that reside in the touched
    // checkpoint
    const auto numExpelledItems = expelledItems.size();
    // Note: Logic ensures that we have done some useful work if reached here
    Expects(numExpelledItems > 0);
    for (auto& it : cursors) {
        // Nothing to do for cursors that reside in other checkpoints
        auto& cursor = *it.second;
        if (cursor.getCheckpoint()->get() != oldestCheckpoint) {
            continue;
        }

        // Nothing to do for cursors placed at empty/checkpoint_start, they are
        // not affected by expel.
        const auto op = (*cursor.getPos())->getOperation();
        if (op == queue_op::empty || op == queue_op::checkpoint_start) {
            continue;
        }

        const auto oldDistance = cursor.getDistance();
        Expects(numExpelledItems < oldDistance);
        cursor.setDistance(oldDistance - numExpelledItems);
    }

    // Register the expel-cursor at checkpoint begin. That is for preventing
    // that the checkpoint is removed in the middle of an expel run when the
    // CM::queueLock is released.
    // Note: Previous validation ensures that lowestCursor points to the oldest
    //  checkpoint at this point
    const auto name = "expel-cursor";
    Expects(cursors.find(name) == cursors.end());
    const auto cursor =
            std::make_shared<CheckpointCursor>(name,
                                               oldestCkptIterator,
                                               oldestCheckpoint->begin(),
                                               CheckpointCursor::Droppable::No,
                                               0);
    cursors[name] = cursor;

    return {std::move(expelledItems),
            this,
            std::move(cursor),
            oldestCheckpoint};
}

CheckpointManager::Counter& CheckpointManager::Counter::operator+=(
        size_t size) {
    local += size;
    global.fetch_add(size);
    return *this;
}

CheckpointManager::Counter& CheckpointManager::Counter::operator-=(
        size_t size) {
    local -= size;
    global.fetch_sub(size);
    return *this;
}

size_t CheckpointManager::getMemFreedByItemExpel() const {
    return memFreedByExpel;
}

size_t CheckpointManager::getMemFreedByCheckpointRemoval() const {
    return memFreedByCheckpointRemoval;
}

bool CheckpointManager::canBeMerged(const std::lock_guard<std::mutex>& lh,
                                    const Checkpoint& first,
                                    const Checkpoint& second) const {
    // MB-36971: We never want to return checkpoints of different type.
    if (first.getCheckpointType() != second.getCheckpointType()) {
        return false;
    }
    // CDC: The history flag that we pass within the flush-batch to magma is
    // expected to be a per-snapshot flag. Thus, merging checkpoints with
    // different History characteristic would be incorrect.
    if (first.getHistorical() != second.getHistorical()) {
        return false;
    }
    return true;
}
