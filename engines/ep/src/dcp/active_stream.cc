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

#include "active_stream_impl.h"

#include "checkpoint_manager.h"
#include "dcp/producer.h"
#include "ep_time.h"
#include "kv_bucket.h"

const std::string activeStreamLoggingPrefix =
        "DCP (Producer): **Deleted conn**";

ActiveStream::ActiveStream(EventuallyPersistentEngine* e,
                           std::shared_ptr<DcpProducer> p,
                           const std::string& n,
                           uint32_t flags,
                           uint32_t opaque,
                           VBucket& vbucket,
                           uint64_t st_seqno,
                           uint64_t en_seqno,
                           uint64_t vb_uuid,
                           uint64_t snap_start_seqno,
                           uint64_t snap_end_seqno,
                           IncludeValue includeVal,
                           IncludeXattrs includeXattrs,
                           IncludeDeleteTime includeDeleteTime,
                           const Collections::Filter& filter,
                           const Collections::VB::Manifest& manifest)
    : Stream(n,
             flags,
             opaque,
             vbucket.getId(),
             st_seqno,
             en_seqno,
             vb_uuid,
             snap_start_seqno,
             snap_end_seqno,
             Type::Active),
      isBackfillTaskRunning(false),
      pendingBackfill(false),
      lastReadSeqno(st_seqno),
      backfillRemaining(0),
      lastReadSeqnoUnSnapshotted(st_seqno),
      lastSentSeqno(st_seqno),
      curChkSeqno(st_seqno),
      takeoverState(vbucket_state_pending),
      itemsFromMemoryPhase(0),
      firstMarkerSent(false),
      waitForSnapshot(0),
      engine(e),
      producerPtr(p),
      lastSentSnapEndSeqno(0),
      chkptItemsExtractionInProgress(false),
      includeValue(includeVal),
      includeXattributes(includeXattrs),
      includeDeleteTime(includeDeleteTime),
      // @todo MB-26618 stream-request is to be changed and the
      // Collections::Filter is going away, calculating includeCollectionID will
      // be made more succinct during that update.
      includeCollectionID(
              (!filter.isPassthrough() && filter.allowDefaultCollection())
                      ? DocKeyEncodesCollectionId::No
                      : DocKeyEncodesCollectionId::Yes),
      snappyEnabled(p->isSnappyEnabled() ? SnappyEnabled::Yes
                                         : SnappyEnabled::No),
      forceValueCompression(p->isForceValueCompressionEnabled()
                                    ? ForceValueCompression::Yes
                                    : ForceValueCompression::No),
      filter(filter, manifest) {
    const char* type = "";
    if (flags_ & DCP_ADD_STREAM_FLAG_TAKEOVER) {
        type = "takeover ";
        end_seqno_ = dcpMaxSeqno;
    }

    ReaderLockHolder rlh(vbucket.getStateLock());
    if (vbucket.getState() == vbucket_state_replica) {
        snapshot_info_t info = vbucket.checkpointManager->getSnapshotInfo();
        if (info.range.end > en_seqno) {
            end_seqno_ = info.range.end;
        }
    }

    p->getLogger().log(
            spdlog::level::info,
            "(vb:{}) Creating {}stream with start seqno {} and end seqno {}; "
            "requested end seqno was {}",
            vbucket.getId(),
            type,
            st_seqno,
            end_seqno_,
            en_seqno);

    backfillItems.memory = 0;
    backfillItems.disk = 0;
    backfillItems.sent = 0;

    bufferedBackfill.bytes = 0;
    bufferedBackfill.items = 0;

    takeoverStart = 0;
    takeoverSendMaxTime = engine->getConfiguration().getDcpTakeoverMaxTime();

    if (start_seqno_ >= end_seqno_) {
        /* streamMutex lock needs to be acquired because endStream
         * potentially makes call to pushToReadyQueue.
         */
        LockHolder lh(streamMutex);
        endStream(END_STREAM_OK);
        itemsReady.store(true);
        // lock is released on leaving the scope
    }
}

ActiveStream::~ActiveStream() {
    if (state_ != StreamState::Dead) {
        removeCheckpointCursor();
    }
}

BucketLogger* ActiveStream::getBucketLogger() {
    static std::shared_ptr<BucketLogger> instance =
            BucketLogger::createBucketLogger("globalActiveStreamBucketLogger",
                                             activeStreamLoggingPrefix);
    return instance.get();
}

std::unique_ptr<DcpResponse> ActiveStream::next() {
    std::lock_guard<std::mutex> lh(streamMutex);
    return next(lh);
}

std::unique_ptr<DcpResponse> ActiveStream::next(
        std::lock_guard<std::mutex>& lh) {
    std::unique_ptr<DcpResponse> response;

    switch (state_.load()) {
    case StreamState::Pending:
        break;
    case StreamState::Backfilling:
        response = backfillPhase(lh);
        break;
    case StreamState::InMemory:
        response = inMemoryPhase();
        break;
    case StreamState::TakeoverSend:
        response = takeoverSendPhase();
        break;
    case StreamState::TakeoverWait:
        response = takeoverWaitPhase();
        break;
    case StreamState::Reading:
        // Not valid for an active stream.
        {
            auto producer = producerPtr.lock();
            std::string connHeader =
                    producer ? producer->logHeader()
                             : "DCP (Producer): **Deleted conn**";
            throw std::logic_error(
                    "ActiveStream::next: Invalid state "
                    "StreamReading for stream " +
                    connHeader + " vb " + std::to_string(vb_));
        }
        break;
    case StreamState::Dead:
        response = deadPhase();
        break;
    }

    itemsReady.store(response ? true : false);
    return response;
}

void ActiveStream::registerCursor(CheckpointManager& chkptmgr,
                                  uint64_t lastProcessedSeqno) {
    try {
        CursorRegResult result = chkptmgr.registerCursorBySeqno(
                name_, lastProcessedSeqno, MustSendCheckpointEnd::NO);

        /*
         * MB-22960:  Due to cursor dropping we re-register the replication
         * cursor only during backfill when we mark the disk snapshot.  However
         * by this point it is possible that the CheckpointManager no longer
         * contains the next sequence number the replication stream requires
         * (i.e. next one after the backfill seqnos).
         *
         * To avoid this data loss when we register the cursor we check to see
         * if the result is greater than the lastProcessedSeqno + 1.
         * If so we know we may have missed some items and may need to perform
         * another backfill.
         *
         * We actually only need to do another backfill if the result is greater
         * than the lastProcessedSeqno + 1 and registerCursorBySeqno returns
         * true, indicating that the resulting seqno starts with the first item
         * on a checkpoint.
         */
        const uint64_t nextRequiredSeqno = lastProcessedSeqno + 1;
        if (result.seqno > nextRequiredSeqno && result.tryBackfill) {
            pendingBackfill = true;
        }
        curChkSeqno = result.seqno;
        cursor = result.cursor;
    } catch (std::exception& error) {
        log(spdlog::level::level_enum::warn,
            "(vb:{}) Failed to register cursor: {}",
            vb_,
            error.what());
        endStream(END_STREAM_STATE);
    }
}

void ActiveStream::markDiskSnapshot(uint64_t startSeqno, uint64_t endSeqno) {
    {
        LockHolder lh(streamMutex);
        uint64_t chkCursorSeqno = endSeqno;

        if (!isBackfilling()) {
            log(spdlog::level::level_enum::warn,
                "(vb:{}) ActiveStream::"
                "markDiskSnapshot: Unexpected state_:{}",
                vb_,
                to_string(state_.load()));
            return;
        }

        /* We need to send the requested 'snap_start_seqno_' as the snapshot
           start when we are sending the first snapshot because the first
           snapshot could be resumption of a previous snapshot */
        if (!firstMarkerSent) {
            startSeqno = std::min(snap_start_seqno_, startSeqno);
            firstMarkerSent = true;
        }

        VBucketPtr vb = engine->getVBucket(vb_);
        if (!vb) {
            log(spdlog::level::level_enum::warn,
                "(vb:{}) "
                "ActiveStream::markDiskSnapshot, vbucket "
                "does not exist",
                vb_);
            return;
        }
        // An atomic read of vbucket state without acquiring the
        // reader lock for state should suffice here.
        if (vb->getState() == vbucket_state_replica) {
            if (end_seqno_ > endSeqno) {
                /* We possibly have items in the open checkpoint
                   (incomplete snapshot) */
                snapshot_info_t info = vb->checkpointManager->getSnapshotInfo();
                log(spdlog::level::level_enum::info,
                    "(vb:{}) Merging backfill and memory snapshot for a "
                    "replica vbucket, backfill start seqno {}, "
                    "backfill end seqno {}, "
                    "snapshot end seqno after merge {}",
                    vb_,
                    startSeqno,
                    endSeqno,
                    info.range.end);
                endSeqno = info.range.end;
            }
        }

        log(spdlog::level::level_enum::info,
            "(vb:{}) Sending disk snapshot with start seqno {} and end "
            "seqno {}",
            vb_,
            startSeqno,
            endSeqno);
        pushToReadyQ(std::make_unique<SnapshotMarker>(
                opaque_, vb_, startSeqno, endSeqno, MARKER_FLAG_DISK));
        lastSentSnapEndSeqno.store(endSeqno, std::memory_order_relaxed);

        if (!(flags_ & DCP_ADD_STREAM_FLAG_DISKONLY)) {
            // Only re-register the cursor if we still need to get memory
            // snapshots
            registerCursor(*vb->checkpointManager, chkCursorSeqno);
        }
    }
    notifyStreamReady();
}

bool ActiveStream::backfillReceived(std::unique_ptr<Item> itm,
                                    backfill_source_t backfill_source,
                                    bool force) {
    if (!itm) {
        return false;
    }

    if (itm->shouldReplicate()) {
        std::unique_lock<std::mutex> lh(streamMutex);
        if (isBackfilling() && filter.checkAndUpdate(*itm)) {
            queued_item qi(std::move(itm));
            std::unique_ptr<DcpResponse> resp(makeResponseFromItem(qi));
            auto producer = producerPtr.lock();
            if (!producer || !producer->recordBackfillManagerBytesRead(
                                     resp->getApproximateSize(), force)) {
                // Deleting resp may also delete itm (which is owned by
                // resp)
                resp.reset();
                return false;
            }

            bufferedBackfill.bytes.fetch_add(resp->getApproximateSize());
            bufferedBackfill.items++;
            lastReadSeqno.store(uint64_t(*resp->getBySeqno()));

            pushToReadyQ(std::move(resp));

            lh.unlock();
            notifyStreamReady();

            if (backfill_source == BACKFILL_FROM_MEMORY) {
                backfillItems.memory++;
            } else {
                backfillItems.disk++;
            }
        }
    }

    return true;
}

void ActiveStream::completeBackfill() {
    {
        LockHolder lh(streamMutex);
        if (isBackfilling()) {
            log(spdlog::level::level_enum::info,
                "(vb:{}) Backfill complete, {}"
                " items "
                "read from disk, {}"
                " from memory, last seqno read: "
                "{}, pendingBackfill : {}",
                vb_,
                backfillItems.disk.load(),
                backfillItems.memory.load(),
                lastReadSeqno.load(),
                pendingBackfill ? "True" : "False");
        } else {
            log(spdlog::level::level_enum::warn,
                "(vb:{}) ActiveStream::completeBackfill: "
                "Unexpected state_:{}",
                vb_,
                to_string(state_.load()));
        }
    }

    bool inverse = true;
    isBackfillTaskRunning.compare_exchange_strong(inverse, false);
    notifyStreamReady();
}

void ActiveStream::snapshotMarkerAckReceived() {
    if (--waitForSnapshot == 0) {
        notifyStreamReady();
    }
}

void ActiveStream::setVBucketStateAckRecieved() {
    VBucketPtr vbucket = engine->getVBucket(vb_);
    if (!vbucket) {
        log(spdlog::level::level_enum::warn,
            "(vb:{}) not present during ack for set "
            "vbucket during takeover",
            vb_);
        return;
    }

    {
        /* Order in which the below 3 locks are acquired is important to avoid
           any potential lock inversion problems */
        std::unique_lock<std::mutex> epVbSetLh(
                engine->getKVBucket()->getVbSetMutexLock());
        WriterLockHolder vbStateLh(vbucket->getStateLock());
        std::unique_lock<std::mutex> lh(streamMutex);
        if (isTakeoverWait()) {
            if (takeoverState == vbucket_state_pending) {
                log(spdlog::level::level_enum::debug,
                    "(vb:{}) Receive ack for set vbucket state to "
                    "pending message",
                    vb_);

                takeoverState = vbucket_state_active;
                transitionState(StreamState::TakeoverSend);

                engine->getKVBucket()->setVBucketState_UNLOCKED(
                        vb_,
                        vbucket_state_dead,
                        false /* transfer */,
                        false /* notify_dcp */,
                        epVbSetLh,
                        &vbStateLh);

                log(spdlog::level::level_enum::info,
                    "(vb:{}) Vbucket marked as dead, last sent "
                    "seqno: {}, high seqno: {}",
                    vb_,
                    lastSentSeqno.load(),
                    vbucket->getHighSeqno());
            } else {
                log(spdlog::level::level_enum::info,
                    "(vb:{}) Receive ack for set vbucket state to "
                    "active message",
                    vb_);
                endStream(END_STREAM_OK);
            }
        } else {
            log(spdlog::level::level_enum::warn,
                "(vb:{}) Unexpected ack for set vbucket op on "
                "stream '{}' state '{}'",
                vb_,
                name_,
                to_string(state_.load()));
            return;
        }
    }

    notifyStreamReady();
}

std::unique_ptr<DcpResponse> ActiveStream::backfillPhase(
        std::lock_guard<std::mutex>& lh) {
    auto resp = nextQueuedItem();

    if (resp) {
        /* It is ok to have recordBackfillManagerBytesSent() and
           bufferedBackfill.bytes.fetch_sub() for all events because
           resp->getApproximateSize() is non zero for only certain resp types.
           (MB-24905 is open to make the accounting cleaner) */
        auto producer = producerPtr.lock();
        if (!producer) {
            throw std::logic_error("(vb:" + std::to_string(vb_) +
                                   " )Producer reference null in the stream in "
                                   "backfillPhase(). This should not happen as "
                                   "the function is called from the producer "
                                   "object");
        }

        producer->recordBackfillManagerBytesSent(resp->getApproximateSize());
        bufferedBackfill.bytes.fetch_sub(resp->getApproximateSize());
        if (!resp->isMetaEvent() || resp->isSystemEvent()) {
            bufferedBackfill.items--;
        }

        // Only DcpResponse objects representing items from "disk" have a size
        // so only update backfillRemaining when non-zero
        if (resp->getApproximateSize()) {
            if (backfillRemaining.load(std::memory_order_relaxed) > 0) {
                backfillRemaining.fetch_sub(1, std::memory_order_relaxed);
            }
        }
    }

    if (!isBackfillTaskRunning && readyQ.empty()) {
        // Given readyQ.empty() is True resp will be NULL
        backfillRemaining.store(0, std::memory_order_relaxed);
        // The previous backfill has completed.  Check to see if another
        // backfill needs to be scheduled.
        if (pendingBackfill) {
            scheduleBackfill_UNLOCKED(true);
            pendingBackfill = false;
        } else {
            if (lastReadSeqno.load() >= end_seqno_) {
                endStream(END_STREAM_OK);
            } else if (flags_ & DCP_ADD_STREAM_FLAG_TAKEOVER) {
                transitionState(StreamState::TakeoverSend);
            } else if (flags_ & DCP_ADD_STREAM_FLAG_DISKONLY) {
                endStream(END_STREAM_OK);
            } else {
                transitionState(StreamState::InMemory);
            }

            if (!resp) {
                resp = nextQueuedItem();
            }
        }
    }

    return resp;
}

std::unique_ptr<DcpResponse> ActiveStream::inMemoryPhase() {
    if (lastSentSeqno.load() >= end_seqno_) {
        endStream(END_STREAM_OK);
    } else if (readyQ.empty()) {
        if (pendingBackfill) {
            // Moving the state from InMemory to Backfilling will result in a
            // backfill being scheduled
            transitionState(StreamState::Backfilling);
            pendingBackfill = false;
            return NULL;
        } else if (nextCheckpointItem()) {
            return NULL;
        }
    }
    return nextQueuedItem();
}

std::unique_ptr<DcpResponse> ActiveStream::takeoverSendPhase() {
    VBucketPtr vb = engine->getVBucket(vb_);
    if (vb && takeoverStart != 0 && !vb->isTakeoverBackedUp() &&
        (ep_current_time() - takeoverStart) > takeoverSendMaxTime) {
        vb->setTakeoverBackedUpState(true);
    }

    if (!readyQ.empty()) {
        return nextQueuedItem();
    } else {
        if (nextCheckpointItem()) {
            return NULL;
        }
    }

    if (waitForSnapshot != 0) {
        return NULL;
    }

    if (vb) {
        vb->setTakeoverBackedUpState(false);
        takeoverStart = 0;
    }

    auto producer = producerPtr.lock();
    if (producer) {
        if (producer->bufferLogInsert(SetVBucketState::baseMsgBytes)) {
            transitionState(StreamState::TakeoverWait);
            return std::make_unique<SetVBucketState>(
                    opaque_, vb_, takeoverState);
        }
    }
    return nullptr;
}

std::unique_ptr<DcpResponse> ActiveStream::takeoverWaitPhase() {
    return nextQueuedItem();
}

std::unique_ptr<DcpResponse> ActiveStream::deadPhase() {
    auto resp = nextQueuedItem();
    if (!resp) {
        log(spdlog::level::level_enum::info,
            "(vb:{}) Stream closed, "
            "{} items sent from backfill phase, "
            "{} items sent from memory phase, "
            "{} was last seqno sent",
            vb_,
            backfillItems.sent.load(),
            itemsFromMemoryPhase.load(),
            lastSentSeqno.load());
    }
    return resp;
}

bool ActiveStream::isCompressionEnabled() {
    auto producer = producerPtr.lock();
    if (producer) {
        return producer->isCompressionEnabled();
    }
    /* If the 'producer' is deleted, what we return doesn't matter */
    return false;
}

void ActiveStream::addStats(ADD_STAT add_stat, const void* c) {
    Stream::addStats(add_stat, c);

    try {
        const int bsize = 1024;
        char buffer[bsize];
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_backfill_disk_items",
                         name_.c_str(),
                         vb_);
        add_casted_stat(buffer, backfillItems.disk, add_stat, c);
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_backfill_mem_items",
                         name_.c_str(),
                         vb_);
        add_casted_stat(buffer, backfillItems.memory, add_stat, c);
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_backfill_sent",
                         name_.c_str(),
                         vb_);
        add_casted_stat(buffer, backfillItems.sent, add_stat, c);
        checked_snprintf(
                buffer, bsize, "%s:stream_%d_memory_phase", name_.c_str(), vb_);
        add_casted_stat(buffer, itemsFromMemoryPhase.load(), add_stat, c);
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_last_sent_seqno",
                         name_.c_str(),
                         vb_);
        add_casted_stat(buffer, lastSentSeqno.load(), add_stat, c);
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_last_sent_snap_end_seqno",
                         name_.c_str(),
                         vb_);
        add_casted_stat(buffer,
                        lastSentSnapEndSeqno.load(std::memory_order_relaxed),
                        add_stat,
                        c);
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_last_read_seqno",
                         name_.c_str(),
                         vb_);
        add_casted_stat(buffer, lastReadSeqno.load(), add_stat, c);
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_last_read_seqno_unsnapshotted",
                         name_.c_str(),
                         vb_);
        add_casted_stat(buffer, lastReadSeqnoUnSnapshotted.load(), add_stat, c);
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_ready_queue_memory",
                         name_.c_str(),
                         vb_);
        add_casted_stat(buffer, getReadyQueueMemory(), add_stat, c);
        checked_snprintf(
                buffer, bsize, "%s:stream_%d_items_ready", name_.c_str(), vb_);
        add_casted_stat(
                buffer, itemsReady.load() ? "true" : "false", add_stat, c);
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_backfill_buffer_bytes",
                         name_.c_str(),
                         vb_);
        add_casted_stat(buffer, bufferedBackfill.bytes, add_stat, c);
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_backfill_buffer_items",
                         name_.c_str(),
                         vb_);
        add_casted_stat(buffer, bufferedBackfill.items, add_stat, c);

        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_cursor_registered",
                         name_.c_str(),
                         vb_);
        add_casted_stat(buffer, cursor.lock() != nullptr, add_stat, c);

        if (isTakeoverSend() && takeoverStart != 0) {
            checked_snprintf(buffer,
                             bsize,
                             "%s:stream_%d_takeover_since",
                             name_.c_str(),
                             vb_);
            add_casted_stat(
                    buffer, ep_current_time() - takeoverStart, add_stat, c);
        }
    } catch (std::exception& error) {
        EP_LOG_WARN("ActiveStream::addStats: Failed to build stats: {}",
                    error.what());
    }

    filter.addStats(add_stat, c, name_, vb_);
}

void ActiveStream::addTakeoverStats(ADD_STAT add_stat,
                                    const void* cookie,
                                    const VBucket& vb) {
    LockHolder lh(streamMutex);

    add_casted_stat("name", name_, add_stat, cookie);
    if (!isActive()) {
        log(spdlog::level::level_enum::warn,
            "(vb:{}) "
            "ActiveStream::addTakeoverStats: Stream has "
            "status StreamDead",
            vb_);
        // Return status of does_not_exist to ensure rebalance does not hang.
        add_casted_stat("status", "does_not_exist", add_stat, cookie);
        add_casted_stat("estimate", 0, add_stat, cookie);
        add_casted_stat("backfillRemaining", 0, add_stat, cookie);
        return;
    }

    size_t total = backfillRemaining.load(std::memory_order_relaxed);
    if (isBackfilling()) {
        add_casted_stat("status", "backfilling", add_stat, cookie);
    } else {
        add_casted_stat("status", "in-memory", add_stat, cookie);
    }
    add_casted_stat("backfillRemaining",
                    backfillRemaining.load(std::memory_order_relaxed),
                    add_stat,
                    cookie);

    size_t vb_items = vb.getNumItems();
    size_t chk_items = 0;
    auto sp = cursor.lock();
    if (vb_items > 0 && sp) {
        chk_items = vb.checkpointManager->getNumItemsForCursor(sp.get());
    }

    size_t del_items = 0;
    try {
        del_items = engine->getKVBucket()->getNumPersistedDeletes(vb_);
    } catch (std::runtime_error& e) {
        log(spdlog::level::level_enum::warn,
            "ActiveStream:addTakeoverStats: exception while getting num "
            "persisted "
            "deletes for vbucket:{}"
            " - treating as 0 deletes. "
            "Details: {}",
            vb_,
            e.what());
    }

    if (end_seqno_ < curChkSeqno) {
        chk_items = 0;
    } else if ((end_seqno_ - curChkSeqno) < chk_items) {
        chk_items = end_seqno_ - curChkSeqno + 1;
    }
    total += chk_items;

    add_casted_stat("estimate", total, add_stat, cookie);
    add_casted_stat("chk_items", chk_items, add_stat, cookie);
    add_casted_stat("vb_items", vb_items, add_stat, cookie);
    add_casted_stat("on_disk_deletes", del_items, add_stat, cookie);
}

std::unique_ptr<DcpResponse> ActiveStream::nextQueuedItem() {
    if (!readyQ.empty()) {
        auto& response = readyQ.front();
        auto producer = producerPtr.lock();
        if (!producer) {
            return nullptr;
        }
        if (producer->bufferLogInsert(response->getMessageSize())) {
            auto seqno = response->getBySeqno();
            if (seqno) {
                lastSentSeqno.store(*seqno);

                if (isBackfilling()) {
                    backfillItems.sent++;
                } else {
                    itemsFromMemoryPhase++;
                }
            }

            return popFromReadyQ();
        }
    }
    return nullptr;
}

bool ActiveStream::nextCheckpointItem() {
    VBucketPtr vbucket = engine->getVBucket(vb_);
    if (vbucket && vbucket->checkpointManager->getNumItemsForCursor(
                           cursor.lock().get()) > 0) {
        // schedule this stream to build the next checkpoint
        auto producer = producerPtr.lock();
        if (!producer) {
            return false;
        }
        producer->scheduleCheckpointProcessorTask(shared_from_this());
        return true;
    } else if (chkptItemsExtractionInProgress) {
        return true;
    }
    return false;
}

void ActiveStream::nextCheckpointItemTask() {
    // MB-29369: Obtain stream mutex here
    LockHolder lh(streamMutex);
    nextCheckpointItemTask(lh);
}

void ActiveStream::nextCheckpointItemTask(const LockHolder& streamMutex) {
    VBucketPtr vbucket = engine->getVBucket(vb_);
    if (vbucket) {
        // MB-29369: only run the task's work if the stream is in an in-memory
        // phase (of which takeover is a variant).
        if (isInMemory() || isTakeoverSend()) {
            auto items = getOutstandingItems(*vbucket);
            processItems(items, streamMutex);
        }
    } else {
        /* The entity deleting the vbucket must set stream to dead,
           calling setDead(END_STREAM_STATE) will cause deadlock because
           it will try to grab streamMutex which is already acquired at this
           point here */
        return;
    }
}

std::vector<queued_item> ActiveStream::getOutstandingItems(VBucket& vb) {
    std::vector<queued_item> items;
    // Commencing item processing - set guard flag.
    chkptItemsExtractionInProgress.store(true);

    auto _begin_ = ProcessClock::now();
    vb.checkpointManager->getAllItemsForCursor(cursor.lock().get(), items);
    engine->getEpStats().dcpCursorsGetItemsHisto.add(
            std::chrono::duration_cast<std::chrono::microseconds>(
                    ProcessClock::now() - _begin_));

    if (vb.checkpointManager->hasClosedCheckpointWhichCanBeRemoved()) {
        engine->getKVBucket()->wakeUpCheckpointRemover();
    }
    return items;
}

/**
 * This function is used to find out if a given item's value
 * needs to be changed
 */
static bool shouldModifyItem(const queued_item& item,
                             IncludeValue includeValue,
                             IncludeXattrs includeXattrs,
                             bool isForceValueCompressionEnabled,
                             bool isSnappyEnabled) {
    // If there is no value, no modification needs to be done
    if (item->getValue()) {
        /**
         * If value needs to be included
         */
        if (includeValue == IncludeValue::No) {
            return true;
        }

        /**
         * Check if value needs to be compressed or decompressed
         * If yes, then then value definitely needs modification
         */
        if (isSnappyEnabled) {
            if (isForceValueCompressionEnabled) {
                if (!mcbp::datatype::is_snappy(item->getDataType())) {
                    return true;
                }
            }
        } else {
            if (mcbp::datatype::is_snappy(item->getDataType())) {
                return true;
            }
        }

        /**
         * If the value doesn't have to be compressed, then
         * check if xattrs need to be pruned. If not, then
         * value needs no modification
         */
        if (includeXattrs == IncludeXattrs::No &&
            mcbp::datatype::is_xattr(item->getDataType())) {
            return true;
        }
    }

    return false;
}

std::unique_ptr<DcpResponse> ActiveStream::makeResponseFromItem(
        const queued_item& item) {
    // Note: This function is hot - it is called for every item to be
    // sent over the DCP connection.
    if (item->getOperation() != queue_op::system_event) {
        if (shouldModifyItem(item,
                             includeValue,
                             includeXattributes,
                             isForceValueCompressionEnabled(),
                             isSnappyEnabled())) {
            auto finalItem = std::make_unique<Item>(*item);
            finalItem->pruneValueAndOrXattrs(includeValue, includeXattributes);

            if (isSnappyEnabled()) {
                if (isForceValueCompressionEnabled()) {
                    if (!mcbp::datatype::is_snappy(finalItem->getDataType())) {
                        if (!finalItem->compressValue()) {
                            EP_LOG_WARN(
                                    "Failed to snappy compress an uncompressed "
                                    "value");
                        }
                    }
                }
            } else {
                if (mcbp::datatype::is_snappy(finalItem->getDataType())) {
                    if (!finalItem->decompressValue()) {
                        EP_LOG_WARN(
                                "Failed to snappy uncompress a compressed "
                                "value");
                    }
                }
            }

            /**
             * Create a mutation response to be placed in the ready queue.
             */
            return std::make_unique<MutationResponse>(std::move(finalItem),
                                                      opaque_,
                                                      includeValue,
                                                      includeXattributes,
                                                      includeDeleteTime,
                                                      includeCollectionID);
        }

        // Item unmodified - construct response from original.
        return std::make_unique<MutationResponse>(item,
                                                  opaque_,
                                                  includeValue,
                                                  includeXattributes,
                                                  includeDeleteTime,
                                                  includeCollectionID);
    }
    return SystemEventProducerMessage::make(opaque_, item);
}

void ActiveStream::processItems(std::vector<queued_item>& items,
                                const LockHolder& streamMutex) {
    if (!items.empty()) {
        bool mark = false;
        if (items.front()->getOperation() == queue_op::checkpoint_start) {
            mark = true;
        }

        std::deque<std::unique_ptr<DcpResponse>> mutations;
        for (auto& qi : items) {
            if (SystemEventReplicate::process(*qi) == ProcessStatus::Continue) {
                curChkSeqno = qi->getBySeqno();
                lastReadSeqnoUnSnapshotted = qi->getBySeqno();
                // Check if the item is allowed on the stream, note the filter
                // updates itself for collection deletion events
                if (filter.checkAndUpdate(*qi)) {
                    mutations.push_back(makeResponseFromItem(qi));
                }

            } else if (qi->getOperation() == queue_op::checkpoint_start) {
                /* if there are already other mutations, then they belong to the
                   previous checkpoint and hence we must create a snapshot and
                   put them onto readyQ */
                if (!mutations.empty()) {
                    snapshot(mutations, mark);
                    /* clear out all the mutations since they are already put
                       onto the readyQ */
                    mutations.clear();
                }
                /* mark true as it indicates a new checkpoint snapshot */
                mark = true;
            }
        }

        if (mutations.empty()) {
            // If we only got checkpoint start or ends check to see if there are
            // any more snapshots before pausing the stream.
            nextCheckpointItemTask(streamMutex);
        } else {
            snapshot(mutations, mark);
        }
    }

    // After the snapshot has been processed, check if the filter is now empty
    // a stream with an empty filter does nothing but self close
    if (filter.empty()) {
        // Filter is now empty empty, so endStream
        endStream(END_STREAM_FILTER_EMPTY);
    }

    // Completed item processing - clear guard flag and notify producer.
    chkptItemsExtractionInProgress.store(false);
    notifyStreamReady(true);
}

void ActiveStream::snapshot(std::deque<std::unique_ptr<DcpResponse>>& items,
                            bool mark) {
    if (items.empty()) {
        return;
    }

    /* This assumes that all items in the "items deque" is put onto readyQ */
    lastReadSeqno.store(lastReadSeqnoUnSnapshotted);

    if (isCurrentSnapshotCompleted()) {
        uint32_t flags = MARKER_FLAG_MEMORY;

        // Get OptionalSeqnos which for the items list types should have values
        auto seqnoStart = items.front()->getBySeqno();
        auto seqnoEnd = items.back()->getBySeqno();
        if (!seqnoStart || !seqnoEnd) {
            throw std::logic_error(
                    "ActiveStream::snapshot incorrect DcpEvent, missing a "
                    "seqno " +
                    std::string(items.front()->to_string()) + " " +
                    std::string(items.back()->to_string()));
        }

        uint64_t snapStart = *seqnoStart;
        uint64_t snapEnd = *seqnoEnd;

        if (mark) {
            flags |= MARKER_FLAG_CHK;
        }

        if (isTakeoverSend()) {
            waitForSnapshot++;
            flags |= MARKER_FLAG_ACK;
        }

        /* We need to send the requested 'snap_start_seqno_' as the snapshot
           start when we are sending the first snapshot because the first
           snapshot could be resumption of a previous snapshot */
        if (!firstMarkerSent) {
            snapStart = std::min(snap_start_seqno_, snapStart);
            firstMarkerSent = true;
        }
        pushToReadyQ(std::make_unique<SnapshotMarker>(
                opaque_, vb_, snapStart, snapEnd, flags));
        lastSentSnapEndSeqno.store(snapEnd, std::memory_order_relaxed);
    }

    for (auto& item : items) {
        pushToReadyQ(std::move(item));
    }
}

uint32_t ActiveStream::setDead(end_stream_status_t status) {
    {
        LockHolder lh(streamMutex);
        endStream(status);
    }

    if (status != END_STREAM_DISCONNECTED) {
        notifyStreamReady();
    }
    return 0;
}

void ActiveStream::notifySeqnoAvailable(uint64_t seqno) {
    if (isActive()) {
        notifyStreamReady();
    }
}

void ActiveStream::endStream(end_stream_status_t reason) {
    if (isActive()) {
        pendingBackfill = false;
        if (isBackfilling()) {
            // If Stream were in Backfilling state, clear out the
            // backfilled items to clear up the backfill buffer.
            clear_UNLOCKED();
            auto producer = producerPtr.lock();
            if (producer) {
                producer->recordBackfillManagerBytesSent(
                        bufferedBackfill.bytes);
            }
            bufferedBackfill.bytes = 0;
            bufferedBackfill.items = 0;
        }
        transitionState(StreamState::Dead);
        if (reason != END_STREAM_DISCONNECTED) {
            pushToReadyQ(
                    std::make_unique<StreamEndResponse>(opaque_, reason, vb_));
        }
        VBucketPtr vb = engine->getVBucket(vb_);
        log(spdlog::level::level_enum::warn,
            "(vb:{}) Stream closing, "
            "sent until seqno {} "
            "remaining items {}, "
            "reason: {}",
            vb_,
            lastSentSeqno.load(),
            readyQ_non_meta_items.load(),
            getEndStreamStatusStr(reason));
        log(spdlog::level::level_enum::warn,
            "(vb:{}) Stream closing, sent until seqno {} remaining items "
            "{}, reason: {}",
            vb_,
            lastSentSeqno.load(),
            readyQ_non_meta_items.load(),
            getEndStreamStatusStr(reason).c_str());
    }
}

void ActiveStream::scheduleBackfill_UNLOCKED(bool reschedule) {
    if (isBackfillTaskRunning) {
        log(spdlog::level::level_enum::info,
            "(vb:{}) Skipping "
            "scheduleBackfill_UNLOCKED; "
            "lastReadSeqno {}"
            ", reschedule flag "
            ": {}",
            vb_,
            lastReadSeqno.load(),
            reschedule ? "True" : "False");
        return;
    }

    VBucketPtr vbucket = engine->getVBucket(vb_);
    if (!vbucket) {
        log(spdlog::level::level_enum::warn,
            "(vb:{}) Failed to schedule "
            "backfill as unable to get vbucket; "
            "lastReadSeqno : {}"
            ", "
            "reschedule : {}",
            vb_,
            lastReadSeqno.load(),
            reschedule ? "True" : "False");
        return;
    }

    auto producer = producerPtr.lock();
    if (!producer) {
        log(spdlog::level::level_enum::warn,
            "(vb:{}) Aborting scheduleBackfill_UNLOCKED() "
            "as the producer conn is deleted; "
            "lastReadSeqno : {}"
            ", "
            "reschedule : {}",
            vb_,
            lastReadSeqno.load(),
            reschedule ? "True" : "False");
        return;
    }

    uint64_t backfillStart = lastReadSeqno.load() + 1;
    uint64_t backfillEnd = 0;
    bool tryBackfill = false;

    if ((flags_ & DCP_ADD_STREAM_FLAG_DISKONLY) || reschedule) {
        uint64_t vbHighSeqno = static_cast<uint64_t>(vbucket->getHighSeqno());
        if (lastReadSeqno.load() > vbHighSeqno) {
            throw std::logic_error(
                    "ActiveStream::scheduleBackfill_UNLOCKED: "
                    "lastReadSeqno (which is " +
                    std::to_string(lastReadSeqno.load()) +
                    " ) is greater than vbHighSeqno (which is " +
                    std::to_string(vbHighSeqno) + " ). " + "for stream " +
                    producer->logHeader() + "; vb " + std::to_string(vb_));
        }
        if (reschedule) {
            /* We need to do this for reschedule because in case of
               DCP_ADD_STREAM_FLAG_DISKONLY (the else part), end_seqno_ is
               set to last persisted seqno befor calling
               scheduleBackfill_UNLOCKED() */
            backfillEnd = engine->getKVBucket()->getLastPersistedSeqno(vb_);
        } else {
            backfillEnd = end_seqno_;
        }
        tryBackfill = true;
    } else {
        try {
            auto registerResult =
                    vbucket->checkpointManager->registerCursorBySeqno(
                            name_,
                            lastReadSeqno.load(),
                            MustSendCheckpointEnd::NO);
            curChkSeqno = registerResult.seqno;
            tryBackfill = registerResult.tryBackfill;
            cursor = registerResult.cursor;
        } catch (std::exception& error) {
            log(spdlog::level::level_enum::warn,
                "(vb:{}) Failed to register "
                "cursor: {}",
                vb_,
                error.what());
            endStream(END_STREAM_STATE);
        }

        if (lastReadSeqno.load() > curChkSeqno) {
            throw std::logic_error(
                    "ActiveStream::scheduleBackfill_UNLOCKED: "
                    "lastReadSeqno (which is " +
                    std::to_string(lastReadSeqno.load()) +
                    " ) is greater than curChkSeqno (which is " +
                    std::to_string(curChkSeqno) + " ). " + "for stream " +
                    producer->logHeader() + "; vb " + std::to_string(vb_));
        }

        /* We need to find the minimum seqno that needs to be backfilled in
         * order to make sure that we don't miss anything when transitioning
         * to a memory snapshot. The backfill task will always make sure that
         * the backfill end seqno is contained in the backfill.
         */
        if (backfillStart < curChkSeqno) {
            if (curChkSeqno > end_seqno_) {
                /* Backfill only is enough */
                backfillEnd = end_seqno_;
            } else {
                /* Backfill + in-memory streaming */
                backfillEnd = curChkSeqno - 1;
            }
        }
    }

    if (backfillStart <= backfillEnd && tryBackfill) {
        log(spdlog::level::level_enum::info,
            "(vb:{}) Scheduling backfill "
            "from {} to {}, reschedule "
            "flag : {}",
            vb_,
            backfillStart,
            backfillEnd,
            reschedule ? "True" : "False");
        producer->scheduleBackfillManager(
                *vbucket, shared_from_this(), backfillStart, backfillEnd);
        isBackfillTaskRunning.store(true);
    } else {
        if (reschedule) {
            // Infrequent code path, see comment below.
            log(spdlog::level::level_enum::info,
                "(vb:{}) Did not schedule "
                "backfill with reschedule : True, "
                "tryBackfill : True; "
                "backfillStart : {}"
                ", "
                "backfillEnd : {}"
                ", "
                "flags_ : {}"
                ", "
                "start_seqno_ : {}"
                ", "
                "end_seqno_ : {}"
                ", "
                "lastReadSeqno : {}"
                ", "
                "lastSentSeqno : {}"
                ", "
                "curChkSeqno : {}"
                ", "
                "itemsReady : {}",
                vb_,
                backfillStart,
                backfillEnd,
                flags_,
                start_seqno_,
                end_seqno_,
                lastReadSeqno.load(),
                lastSentSeqno.load(),
                curChkSeqno.load(),
                itemsReady ? "True" : "False");

            /* Cursor was dropped, but we will not do backfill.
             * This may happen in a corner case where, the memory usage is high
             * due to other vbuckets and persistence cursor moves ahead of
             * replication cursor to new checkpoint open but does not persist
             * items yet.
             *
             * Because we dropped the cursor but did not do a backfill (and
             * therefore did not re-register a cursor in markDiskSnapshot) we
             * must re-register the cursor here.
             */
            try {
                CursorRegResult result =
                        vbucket->checkpointManager->registerCursorBySeqno(
                                name_,
                                lastReadSeqno.load(),
                                MustSendCheckpointEnd::NO);
                curChkSeqno = result.seqno;
            } catch (std::exception& error) {
                log(spdlog::level::level_enum::warn,
                    "(vb:{}) Failed to register "
                    "cursor: {}",
                    vb_,
                    error.what());
                endStream(END_STREAM_STATE);
            }
        }
        if (flags_ & DCP_ADD_STREAM_FLAG_DISKONLY) {
            endStream(END_STREAM_OK);
        } else if (flags_ & DCP_ADD_STREAM_FLAG_TAKEOVER) {
            transitionState(StreamState::TakeoverSend);
        } else {
            transitionState(StreamState::InMemory);
        }
        if (reschedule) {
            /*
             * It is not absolutely necessary to notify immediately as conn
             * manager or an incoming item will cause a notification eventually,
             * but wouldn't hurt to do so.
             *
             * Note: must not notify when we schedule a backfill for the first
             * time (i.e. when reschedule is false) because the stream is not
             * yet in producer conn list of streams.
             */
            notifyStreamReady();
        }
    }
}

bool ActiveStream::handleSlowStream() {
    LockHolder lh(streamMutex);
    log(spdlog::level::level_enum::info,
        "(vb:{}) Handling slow stream; "
        "state_ : {}, "
        "lastReadSeqno : {}"
        ", "
        "lastSentSeqno : {}"
        ", "
        "vBucketHighSeqno : {}"
        ", "
        "isBackfillTaskRunning : {}",
        vb_,
        to_string(state_.load()).c_str(),
        lastReadSeqno.load(),
        lastSentSeqno.load(),
        engine->getVBucket(vb_)->getHighSeqno(),
        isBackfillTaskRunning.load() ? "True" : "False");

    bool status = false;
    switch (state_.load()) {
    case StreamState::Backfilling:
    case StreamState::InMemory:
        /* Drop the existing cursor and set pending backfill */
        status = dropCheckpointCursor_UNLOCKED();
        pendingBackfill = true;
        return status;
    case StreamState::TakeoverSend:
    /* To be handled later if needed */
    case StreamState::TakeoverWait:
    /* To be handled later if needed */
    case StreamState::Dead:
        /* To be handled later if needed */
        return false;
    case StreamState::Pending:
    case StreamState::Reading: {
        auto producer = producerPtr.lock();
        std::string connHeader = producer ? producer->logHeader()
                                          : "DCP (Producer): **Deleted conn**";
        throw std::logic_error(
                "ActiveStream::handleSlowStream: "
                "called with state " +
                to_string(state_.load()) +
                " "
                "for stream " +
                connHeader + "; vb " + std::to_string(vb_));
    }
    }
    return false;
}

std::string ActiveStream::getEndStreamStatusStr(end_stream_status_t status) {
    switch (status) {
    case END_STREAM_OK:
        return "The stream ended due to all items being streamed";
    case END_STREAM_CLOSED:
        return "The stream closed early due to a close stream message";
    case END_STREAM_STATE:
        return "The stream closed early because the vbucket state changed";
    case END_STREAM_DISCONNECTED:
        return "The stream closed early because the conn was disconnected";
    case END_STREAM_SLOW:
        return "The stream was closed early because it was too slow";
    case END_STREAM_BACKFILL_FAIL:
        return "The stream closed early due to backfill failure";
    case END_STREAM_ROLLBACK:
        return "The stream closed early because the vbucket rollback'ed";
    case END_STREAM_FILTER_EMPTY:
        return "The stream closed because all of the filtered collections "
               "were deleted";
    }
    return std::string{"Status unknown: " + std::to_string(status) +
                       "; this should not have happened!"};
}

void ActiveStream::transitionState(StreamState newState) {
    if (state_ == newState) {
        return;
    }

    auto logLevel = getTransitionStateLogLevel(state_, newState);
    log(logLevel,
        "ActiveStream::transitionState: (vb:{}) "
        "Transitioning from {} to {}",
        vb_,
        to_string(state_.load()),
        to_string(newState));

    bool validTransition = false;
    switch (state_.load()) {
    case StreamState::Pending:
        if (newState == StreamState::Backfilling ||
            newState == StreamState::Dead) {
            validTransition = true;
        }
        break;
    case StreamState::Backfilling:
        if (newState == StreamState::InMemory ||
            newState == StreamState::TakeoverSend ||
            newState == StreamState::Dead) {
            validTransition = true;
        }
        break;
    case StreamState::InMemory:
        if (newState == StreamState::Backfilling ||
            newState == StreamState::Dead) {
            validTransition = true;
        }
        break;
    case StreamState::TakeoverSend:
        if (newState == StreamState::TakeoverWait ||
            newState == StreamState::Dead) {
            validTransition = true;
        }
        break;
    case StreamState::TakeoverWait:
        if (newState == StreamState::TakeoverSend ||
            newState == StreamState::Dead) {
            validTransition = true;
        }
        break;
    case StreamState::Reading:
        // Active stream should never be in READING state.
        validTransition = false;
        break;
    case StreamState::Dead:
        // Once DEAD, no other transitions should occur.
        validTransition = false;
        break;
    }

    if (!validTransition) {
        throw std::invalid_argument(
                "ActiveStream::transitionState:"
                " newState (which is " +
                to_string(newState) +
                ") is not valid for current state (which is " +
                to_string(state_.load()) + ")");
    }

    StreamState oldState = state_.load();
    state_ = newState;

    switch (newState) {
    case StreamState::Backfilling:
        if (StreamState::Pending == oldState) {
            scheduleBackfill_UNLOCKED(false /* reschedule */);
        } else if (StreamState::InMemory == oldState) {
            scheduleBackfill_UNLOCKED(true /* reschedule */);
        }
        break;
    case StreamState::InMemory:
        // Check if the producer has sent up till the last requested
        // sequence number already, if not - move checkpoint items into
        // the ready queue.
        if (lastSentSeqno.load() >= end_seqno_) {
            // Stream transitioning to DEAD state
            endStream(END_STREAM_OK);
            notifyStreamReady();
        } else {
            nextCheckpointItem();
        }
        break;
    case StreamState::TakeoverSend:
        takeoverStart = ep_current_time();
        if (!nextCheckpointItem()) {
            notifyStreamReady(true);
        }
        break;
    case StreamState::Dead:
        removeCheckpointCursor();
        break;
    case StreamState::TakeoverWait:
    case StreamState::Pending:
        break;
    case StreamState::Reading:
        throw std::logic_error(
                "ActiveStream::transitionState:"
                " newState can't be " +
                to_string(newState) + "!");
    }
}

size_t ActiveStream::getItemsRemaining() {
    VBucketPtr vbucket = engine->getVBucket(vb_);

    if (!vbucket || !isActive()) {
        return 0;
    }

    // Items remaining is the sum of:
    // (a) Items outstanding in checkpoints
    // (b) Items pending in our readyQ, excluding any meta items.
    size_t ckptItems = 0;
    if (auto sp = cursor.lock()) {
        ckptItems = vbucket->checkpointManager->getNumItemsForCursor(sp.get());
    }
    return ckptItems + readyQ_non_meta_items;
}

uint64_t ActiveStream::getLastReadSeqno() const {
    return lastReadSeqno.load();
}

uint64_t ActiveStream::getLastSentSeqno() const {
    return lastSentSeqno.load();
}

bool ActiveStream::isCurrentSnapshotCompleted() const {
    VBucketPtr vbucket = engine->getVBucket(vb_);
    // An atomic read of vbucket state without acquiring the
    // reader lock for state should suffice here.
    if (vbucket && vbucket->getState() == vbucket_state_replica) {
        if (lastSentSnapEndSeqno.load(std::memory_order_relaxed) >=
            lastReadSeqno) {
            return false;
        }
    }
    return true;
}

bool ActiveStream::dropCheckpointCursor_UNLOCKED() {
    VBucketPtr vbucket = engine->getVBucket(vb_);
    if (!vbucket) {
        endStream(END_STREAM_STATE);
        notifyStreamReady();
    }
    return vbucket->checkpointManager->removeCursor(cursor.lock().get());
}

spdlog::level::level_enum ActiveStream::getTransitionStateLogLevel(
        StreamState currState, StreamState newState) {
    if ((currState == StreamState::Pending) ||
        (newState == StreamState::Dead)) {
        return spdlog::level::level_enum::debug;
    }
    return spdlog::level::level_enum::info;
}

void ActiveStream::notifyStreamReady(bool force) {
    auto producer = producerPtr.lock();
    if (!producer) {
        return;
    }

    bool inverse = false;
    if (force || itemsReady.compare_exchange_strong(inverse, true)) {
        producer->notifyStreamReady(vb_);
    }
}

void ActiveStream::removeCheckpointCursor() {
    VBucketPtr vb = engine->getVBucket(vb_);
    if (vb) {
        vb->checkpointManager->removeCursor(cursor.lock().get());
    }
}

std::shared_ptr<BucketLogger> globalActiveStreamBucketLogger;
