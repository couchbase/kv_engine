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

#include "checkpoint.h"
#include "checkpoint_manager.h"
#include "dcp/producer.h"
#include "dcp/response.h"
#include "ep_time.h"
#include "kv_bucket.h"
#include "statwriter.h"

#include <boost/optional/optional_io.hpp>
#include <memcached/protocol_binary.h>

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
                           Collections::VB::Filter f)
    : Stream(n,
             flags,
             opaque,
             vbucket.getId(),
             st_seqno,
             en_seqno,
             vb_uuid,
             snap_start_seqno,
             snap_end_seqno),
      isBackfillTaskRunning(false),
      pendingBackfill(false),
      lastReadSeqno(st_seqno),
      backfillRemaining(),
      lastReadSeqnoUnSnapshotted(st_seqno),
      lastSentSeqno(st_seqno),
      curChkSeqno(st_seqno),
      takeoverState(vbucket_state_pending),
      itemsFromMemoryPhase(0),
      firstMarkerSent(false),
      waitForSnapshot(0),
      engine(e),
      producerPtr(p),
      takeoverSendMaxTime(e->getConfiguration().getDcpTakeoverMaxTime()),
      lastSentSnapEndSeqno(0),
      chkptItemsExtractionInProgress(false),
      includeValue(includeVal),
      includeXattributes(includeXattrs),
      includeDeleteTime(includeDeleteTime),
      includeCollectionID(f.isLegacyFilter() ? DocKeyEncodesCollectionId::No
                                             : DocKeyEncodesCollectionId::Yes),
      enableExpiryOutput(p->isDCPExpiryEnabled() ? EnableExpiryOutput::Yes
                                                 : EnableExpiryOutput::No),
      snappyEnabled(p->isSnappyEnabled() ? SnappyEnabled::Yes
                                         : SnappyEnabled::No),
      forceValueCompression(p->isForceValueCompressionEnabled()
                                    ? ForceValueCompression::Yes
                                    : ForceValueCompression::No),
      syncReplication(p->getSyncReplSupport()),
      filter(std::move(f)),
      sid(filter.getStreamId()) {
    const char* type = "";
    if (flags_ & DCP_ADD_STREAM_FLAG_TAKEOVER) {
        type = "takeover ";
        end_seqno_ = dcpMaxSeqno;
    }

    folly::SharedMutex::ReadHolder rlh(vbucket.getStateLock());
    if (vbucket.getState() == vbucket_state_replica) {
        snapshot_info_t info = vbucket.checkpointManager->getSnapshotInfo();
        if (info.range.getEnd() > en_seqno) {
            end_seqno_ = info.range.getEnd();
        }
    }

    logPrefix = "(" + vbucket.getId().to_string() + ")";
    if (sid) {
        // name must be unique to ensure we get our own cursor
        name_ += sid.to_string();
        logPrefix += " (" + sid.to_string() + ")";
    }
    lastReadSeqno.setLabel("ActiveStream(" + vbucket.getId().to_string() +
                           ")::lastReadSeqno");

    log(spdlog::level::info,
        "{} Creating {}stream with start seqno {} and end seqno {}; "
        "requested end seqno was {}, collections-manifest uid:{}, {}",
        logPrefix,
        type,
        st_seqno,
        end_seqno_,
        en_seqno,
        filter.getUid(),
        sid);

    backfillItems.memory = 0;
    backfillItems.disk = 0;
    backfillItems.sent = 0;

    bufferedBackfill.bytes = 0;
    bufferedBackfill.items = 0;

    takeoverStart = 0;

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
    case StreamState::Dead:
        response = deadPhase();
        break;
    }

    if (nextHook) {
        nextHook();
    }

    itemsReady.store(response ? true : false);
    return response;
}

bool ActiveStream::isActive() const {
    return state_.load() != StreamState::Dead;
}

bool ActiveStream::isBackfilling() const {
    return state_.load() == StreamState::Backfilling;
}

bool ActiveStream::isInMemory() const {
    return state_.load() == StreamState::InMemory;
}

bool ActiveStream::isPending() const {
    return state_.load() == StreamState::Pending;
}

bool ActiveStream::isTakeoverSend() const {
    return state_.load() == StreamState::TakeoverSend;
}

bool ActiveStream::isTakeoverWait() const {
    return state_.load() == StreamState::TakeoverWait;
}

void ActiveStream::registerCursor(CheckpointManager& chkptmgr,
                                  uint64_t lastProcessedSeqno) {
    try {
        CursorRegResult result =
                chkptmgr.registerCursorBySeqno(name_, lastProcessedSeqno);

        log(spdlog::level::level_enum::info,
            "{} ActiveStream::registerCursor name \"{}\", backfill:{}, "
            "seqno:{}",
            logPrefix,
            name_,
            result.tryBackfill,
            result.seqno);

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
            "{} Failed to register cursor: {}",
            logPrefix,
            error.what());
        endStream(END_STREAM_STATE);
    }
}

bool ActiveStream::markDiskSnapshot(
        uint64_t startSeqno,
        uint64_t endSeqno,
        boost::optional<uint64_t> highCompletedSeqno,
        uint64_t maxVisibleSeqno) {
    {
        LockHolder lh(streamMutex);
        uint64_t chkCursorSeqno = endSeqno;

        if (!isBackfilling()) {
            log(spdlog::level::level_enum::warn,
                "{} ActiveStream::"
                "markDiskSnapshot: Unexpected state_:{}",
                logPrefix,
                to_string(state_.load()));
            return false;
        }

        if (!supportSyncWrites()) {
            // the connection does not support sync writes, so the
            // snapshot end must be set to the seqno of a visible item.
            // Items after the MVS will not be sent.
            endSeqno = maxVisibleSeqno;
            if (endSeqno < startSeqno) {
                // no visible items in backfill, should not send
                // a snapshot marker at all (no data will be sent)
                log(spdlog::level::level_enum::info,
                    "{} "
                    "ActiveStream::markDiskSnapshot not sending snapshot "
                    "because"
                    "it contains no visible items",
                    logPrefix);
                // reregister cursor at original end seqno
                notifyEmptyBackfill_UNLOCKED(chkCursorSeqno);
                return false;
            }
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
                "{} "
                "ActiveStream::markDiskSnapshot, vbucket "
                "does not exist",
                logPrefix);
            return false;
        }
        // An atomic read of vbucket state without acquiring the
        // reader lock for state should suffice here.
        if (vb->getState() == vbucket_state_replica) {
            if (end_seqno_ > endSeqno) {
                /* We possibly have items in the open checkpoint
                   (incomplete snapshot) */
                snapshot_info_t info = vb->checkpointManager->getSnapshotInfo();
                log(spdlog::level::level_enum::info,
                    "{} Merging backfill and memory snapshot for a "
                    "replica vbucket, backfill start seqno {}, "
                    "backfill end seqno {}, "
                    "snapshot end seqno after merge {}",
                    logPrefix,
                    startSeqno,
                    endSeqno,
                    info.range.getEnd());
                endSeqno = info.range.getEnd();
            }
        }

        // If the stream supports SyncRep then send the HCS in the
        // SnapshotMarker if it is not 0
        auto sendHCS = supportSyncReplication() && highCompletedSeqno;
        auto hcsToSend = sendHCS ? highCompletedSeqno : boost::none;
        log(spdlog::level::level_enum::info,
            "{} ActiveStream::markDiskSnapshot: Sending disk snapshot with "
            "start {}, end {}, and high completed {}, max visible {}",
            logPrefix,
            startSeqno,
            endSeqno,
            hcsToSend,
            maxVisibleSeqno);
        pushToReadyQ(std::make_unique<SnapshotMarker>(
                opaque_,
                vb_,
                startSeqno,
                endSeqno,
                MARKER_FLAG_DISK | MARKER_FLAG_CHK,
                hcsToSend,
                boost::optional<uint64_t>{supportSyncReplication(),
                                          maxVisibleSeqno},
                sid));
        lastSentSnapEndSeqno.store(endSeqno, std::memory_order_relaxed);

        if (!(flags_ & DCP_ADD_STREAM_FLAG_DISKONLY)) {
            // Only re-register the cursor if we still need to get memory
            // snapshots
            registerCursor(*vb->checkpointManager, chkCursorSeqno);
        }
    }
    notifyStreamReady();
    return true;
}

bool ActiveStream::markOSODiskSnapshot() {
    pushToReadyQ(std::make_unique<OSOSnapshot>(opaque_, vb_, sid));
    notifyStreamReady();
    return true;
}

bool ActiveStream::backfillReceived(std::unique_ptr<Item> itm,
                                    backfill_source_t backfill_source,
                                    bool force) {
    if (!itm) {
        return false;
    }

    // Should the item replicate?
    if (!shouldProcessItem(*itm)) {
        return true; // skipped, but return true as it's not a failure
    }

    std::unique_lock<std::mutex> lh(streamMutex);
    if (isBackfilling() && filter.checkAndUpdate(*itm)) {
        queued_item qi(std::move(itm));
        // We need to send a mutation instead of a commit if this Item is a
        // commit as we may have de-duped the preceding prepare and the replica
        // needs to know what to commit.
        std::unique_ptr<DcpResponse> resp(
                makeResponseFromItem(qi, SendCommitSyncWriteAs::Mutation));
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
        lastBackfilledSeqno = std::max<uint64_t>(lastBackfilledSeqno,
                                                 uint64_t(*resp->getBySeqno()));

        pushToReadyQ(std::move(resp));

        lh.unlock();
        notifyStreamReady();

        if (backfill_source == BACKFILL_FROM_MEMORY) {
            backfillItems.memory++;
        } else {
            backfillItems.disk++;
        }
    }

    return true;
}

void ActiveStream::completeBackfill() {
    {
        LockHolder lh(streamMutex);

        // backfills can be scheduled and return nothing. lastReadSeqno is
        // monotonic (and assigned in many places). In this backfill assignment
        // we will only assign when not equal
        if (lastReadSeqno != lastBackfilledSeqno) {
            lastReadSeqno.store(lastBackfilledSeqno);
        }

        if (isBackfilling()) {
            log(spdlog::level::level_enum::info,
                "{} Backfill complete, {}"
                " items "
                "read from disk, {}"
                " from memory, last seqno read: "
                "{}, pendingBackfill : {}",
                logPrefix,
                backfillItems.disk.load(),
                backfillItems.memory.load(),
                lastReadSeqno.load(),
                pendingBackfill ? "True" : "False");
        } else {
            log(spdlog::level::level_enum::warn,
                "{} ActiveStream::completeBackfill: "
                "Unexpected state_:{}",
                logPrefix,
                to_string(state_.load()));
        }
    }

    if (completeBackfillHook) {
        completeBackfillHook();
    }

    bool inverse = true;
    isBackfillTaskRunning.compare_exchange_strong(inverse, false);

    // MB-37468: Items may not be ready, but we need to notify the stream
    // regardless as a racing stepping producer that had just finished
    // processing all items and found an empty ready queue could clear the flag
    // immediately after we call notifyStreamReady (which does not notify as
    // itemsReady is true). This would then result in us not notifying the
    // stream and not putting it back in the producer's readyQueue. A similar
    // case exists for transitioning state to TakeoverSend or InMemory.
    notifyStreamReady(true);
}

void ActiveStream::completeOSOBackfill() {
    {
        LockHolder lh(streamMutex);
        lastReadSeqno.store(lastBackfilledSeqno);
        if (isBackfilling()) {
            log(spdlog::level::level_enum::info,
                "{} OSO Backfill complete, {} items read from disk, {}"
                " from memory, lastReadSeqno:{}, pendingBackfill:{}",
                logPrefix,
                backfillItems.disk.load(),
                backfillItems.memory.load(),
                lastReadSeqno.load(),
                pendingBackfill ? "True" : "False");
        } else {
            log(spdlog::level::level_enum::warn,
                "{} ActiveStream::completeOSOBackfill: Unexpected state_:{}",
                logPrefix,
                to_string(state_.load()));
        }
    }

    if (completeBackfillHook) {
        completeBackfillHook();
    }

    bool inverse = true;
    isBackfillTaskRunning.compare_exchange_strong(inverse, false);

    pushToReadyQ(std::make_unique<OSOSnapshot>(
            opaque_, vb_, sid, OSOSnapshot::End{}));

    notifyStreamReady(true);
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
            "{} not present during ack for set "
            "vbucket during takeover",
            logPrefix);
        return;
    }

    {
        /* Order in which the below 3 locks are acquired is important to avoid
           any potential lock inversion problems */
        std::unique_lock<std::mutex> epVbSetLh(
                engine->getKVBucket()->getVbSetMutexLock());
        folly::SharedMutex::WriteHolder vbStateLh(vbucket->getStateLock());
        std::unique_lock<std::mutex> lh(streamMutex);
        if (isTakeoverWait()) {
            if (takeoverState == vbucket_state_pending) {
                log(spdlog::level::level_enum::debug,
                    "{} Receive ack for set vbucket state to "
                    "pending message",
                    logPrefix);

                takeoverState = vbucket_state_active;
                transitionState(StreamState::TakeoverSend);

                engine->getKVBucket()->setVBucketState_UNLOCKED(
                        vbucket,
                        vbucket_state_dead,
                        {},
                        TransferVB::No,
                        false /* notify_dcp */,
                        epVbSetLh,
                        vbStateLh);

                log(spdlog::level::level_enum::info,
                    "{} Vbucket marked as dead, last sent "
                    "seqno: {}, high seqno: {}",
                    logPrefix,
                    lastSentSeqno.load(),
                    vbucket->getHighSeqno());
            } else {
                log(spdlog::level::level_enum::info,
                    "{} Receive ack for set vbucket state to "
                    "active message",
                    logPrefix);
                endStream(END_STREAM_OK);
            }
        } else {
            log(spdlog::level::level_enum::warn,
                "{} Unexpected ack for set vbucket op on "
                "stream '{}' state '{}'",
                logPrefix,
                name_,
                to_string(state_.load()));
            return;
        }
    }

    notifyStreamReady();
}

void ActiveStream::setBackfillRemaining(size_t value) {
    std::lock_guard<std::mutex> guard(streamMutex);
    setBackfillRemaining_UNLOCKED(value);
}

void ActiveStream::setBackfillRemaining_UNLOCKED(size_t value) {
    backfillRemaining = value;
}

void ActiveStream::clearBackfillRemaining() {
    std::lock_guard<std::mutex> guard(streamMutex);
    backfillRemaining.reset();
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
            throw std::logic_error(
                    "ActiveStream::backfillPhase: Producer reference null. "
                    "This should not happen as "
                    "the function is called from the producer "
                    "object. " +
                    logPrefix);
        }

        producer->recordBackfillManagerBytesSent(resp->getApproximateSize());
        bufferedBackfill.bytes.fetch_sub(resp->getApproximateSize());
        if (!resp->isMetaEvent() || resp->isSystemEvent()) {
            bufferedBackfill.items--;
        }

        // Only DcpResponse objects representing items from "disk" have a size
        // so only update backfillRemaining when non-zero
        if (resp->getApproximateSize() && backfillRemaining.is_initialized()) {
            (*backfillRemaining)--;
        }
    }

    if (!isBackfillTaskRunning && readyQ.empty()) {
        // Given readyQ.empty() is True resp will be NULL
        // The previous backfill has completed.  Check to see if another
        // backfill needs to be scheduled.
        if (pendingBackfill) {
            scheduleBackfill_UNLOCKED(true);
            pendingBackfill = false;
            // After scheduling a backfill we may now have items in readyQ -
            // so re-check if we didn't already have a response.
            if (!resp) {
                resp = nextQueuedItem();
            }
        } else {
            if (lastReadSeqno.load() >= end_seqno_) {
                endStream(END_STREAM_OK);
            } else if (flags_ & DCP_ADD_STREAM_FLAG_TAKEOVER) {
                transitionState(StreamState::TakeoverSend);
            } else if (flags_ & DCP_ADD_STREAM_FLAG_DISKONLY) {
                endStream(END_STREAM_OK);
            } else {
                if (backfillRemaining && *backfillRemaining != 0) {
                    /* No more items will be received from the backfill at this
                     * point but backfill remaining count may be an overestimate
                     * if the stream is not sync write aware.
                     * This is an expected situation.
                     */
                    log(spdlog::level::level_enum::debug,
                        "{} ActiveStream::completeBackfill: "
                        "Backfill complete with items remaining:{}",
                        logPrefix,
                        *backfillRemaining);

                    // reset value to zero just in case.
                    setBackfillRemaining_UNLOCKED(0);
                }
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
                    opaque_, vb_, takeoverState, sid);
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
            "{} Stream closed, "
            "{} items sent from backfill phase, "
            "{} items sent from memory phase, "
            "{} was last seqno sent",
            logPrefix,
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

void ActiveStream::addStats(const AddStatFn& add_stat, const void* c) {
    Stream::addStats(add_stat, c);

    try {
        const int bsize = 1024;
        char buffer[bsize];
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_backfill_disk_items",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(buffer, backfillItems.disk, add_stat, c);
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_backfill_mem_items",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(buffer, backfillItems.memory, add_stat, c);
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_backfill_sent",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(buffer, backfillItems.sent, add_stat, c);
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_memory_phase",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(buffer, itemsFromMemoryPhase.load(), add_stat, c);
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_last_sent_seqno",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(buffer, lastSentSeqno.load(), add_stat, c);
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_last_sent_snap_end_seqno",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(buffer,
                        lastSentSnapEndSeqno.load(std::memory_order_relaxed),
                        add_stat,
                        c);
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_last_read_seqno",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(buffer, lastReadSeqno.load(), add_stat, c);
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_last_read_seqno_unsnapshotted",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(buffer, lastReadSeqnoUnSnapshotted.load(), add_stat, c);
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_ready_queue_memory",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(buffer, getReadyQueueMemory(), add_stat, c);

        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_backfill_buffer_bytes",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(buffer, bufferedBackfill.bytes, add_stat, c);
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_backfill_buffer_items",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(buffer, bufferedBackfill.items, add_stat, c);

        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_cursor_registered",
                         name_.c_str(),
                         vb_.get());
        add_casted_stat(buffer, cursor.lock() != nullptr, add_stat, c);

        if (isTakeoverSend() && takeoverStart != 0) {
            checked_snprintf(buffer,
                             bsize,
                             "%s:stream_%d_takeover_since",
                             name_.c_str(),
                             vb_.get());
            add_casted_stat(
                    buffer, ep_current_time() - takeoverStart, add_stat, c);
        }
    } catch (std::exception& error) {
        log(spdlog::level::level_enum::warn,
            "{} ActiveStream::addStats: Failed to build stats: {}",
            logPrefix,
            error.what());
    }

    filter.addStats(add_stat, c, name_, vb_);
}

void ActiveStream::addTakeoverStats(const AddStatFn& add_stat,
                                    const void* cookie,
                                    const VBucket& vb) {
    LockHolder lh(streamMutex);

    add_casted_stat("name", name_, add_stat, cookie);
    if (!isActive()) {
        log(spdlog::level::level_enum::warn,
            "{} "
            "ActiveStream::addTakeoverStats: Stream has "
            "status StreamDead",
            logPrefix);
        // Return status of does_not_exist to ensure rebalance does not hang.
        add_casted_stat("status", "does_not_exist", add_stat, cookie);
        add_casted_stat("estimate", 0, add_stat, cookie);
        add_casted_stat("backfillRemaining", 0, add_stat, cookie);
        return;
    }

    size_t total = 0;
    const char* status = nullptr;
    if (isBackfilling()) {
        if (backfillRemaining) {
            status = "backfilling";
            total += *backfillRemaining;
        } else {
            status = "calculating-item-count";
        }
    } else {
        status = "in-memory";
    }
    add_casted_stat("status", status, add_stat, cookie);

    if (backfillRemaining) {
        add_casted_stat(
                "backfillRemaining", *backfillRemaining, add_stat, cookie);
    }

    size_t vb_items = vb.getNumItems();
    size_t chk_items = 0;
    auto sp = cursor.lock();
    if (vb_items > 0 && sp) {
        chk_items = vb.checkpointManager->getNumItemsForCursor(sp.get());
    }

    size_t del_items = 0;
    try {
        del_items = vb.getNumPersistedDeletes();
    } catch (std::runtime_error& e) {
        log(spdlog::level::level_enum::warn,
            "{} ActiveStream:addTakeoverStats: exception while getting num "
            "persisted "
            "deletes"
            " - treating as 0 deletes. "
            "Details: {}",
            logPrefix,
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
            auto res = getOutstandingItems(*vbucket);
            processItems(res, streamMutex);
        }
    } else {
        /* The entity deleting the vbucket must set stream to dead,
           calling setDead(END_STREAM_STATE) will cause deadlock because
           it will try to grab streamMutex which is already acquired at this
           point here */
        return;
    }
}

ActiveStream::OutstandingItemsResult ActiveStream::getOutstandingItems(
        VBucket& vb) {
    OutstandingItemsResult result;
    // Commencing item processing - set guard flag.
    chkptItemsExtractionInProgress.store(true);

    auto _begin_ = std::chrono::steady_clock::now();
    const auto itemsForCursor = vb.checkpointManager->getNextItemsForCursor(
            cursor.lock().get(), result.items);
    engine->getEpStats().dcpCursorsGetItemsHisto.add(
            std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - _begin_));

    result.checkpointType = itemsForCursor.checkpointType;
    result.highCompletedSeqno = itemsForCursor.highCompletedSeqno;
    result.visibleSeqno = itemsForCursor.visibleSeqno;
    if (vb.checkpointManager->hasClosedCheckpointWhichCanBeRemoved()) {
        engine->getKVBucket()->wakeUpCheckpointRemover();
    }
    return result;
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
        if ((includeValue == IncludeValue::No) ||
            (includeValue == IncludeValue::NoWithUnderlyingDatatype)) {
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
        const queued_item& item, SendCommitSyncWriteAs sendCommitSyncWriteAs) {
    // Note: This function is hot - it is called for every item to be
    // sent over the DCP connection.

    // If this Stream supports SyncReplication then we may send a
    // CommitSyncWrite. If this Stream does not support SyncReplication then we
    // will only send Mutation messages.
    //
    // We will send a CommitSyncWrite when streaming from Checkpoints, and a
    // Mutation instead of a Commit (as this contains the full value) when
    // streaming from disk/backfill. If we have a Disk Checkpoint then we will
    // send Mutations as all of our Items will be mutations (streamed to us by
    // and old active as a Mutation).
    if ((item->getOperation() == queue_op::commit_sync_write) &&
        (supportSyncWrites()) &&
        sendCommitSyncWriteAs == SendCommitSyncWriteAs::Commit) {
        return std::make_unique<CommitSyncWrite>(opaque_,
                                                 item->getVBucketId(),
                                                 item->getPrepareSeqno(),
                                                 item->getBySeqno(),
                                                 item->getKey());
    }

    if (item->getOperation() == queue_op::abort_sync_write) {
        return std::make_unique<AbortSyncWrite>(
                opaque_,
                item->getVBucketId(),
                item->getKey(),
                item->getPrepareSeqno(),
                item->getBySeqno() /*abortSeqno*/);
    }

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
                            log(spdlog::level::level_enum::warn,
                                "{} Failed to snappy compress an uncompressed "
                                "value",
                                logPrefix);
                        }
                    }
                }
            } else {
                if (mcbp::datatype::is_snappy(finalItem->getDataType())) {
                    if (!finalItem->decompressValue()) {
                        log(spdlog::level::level_enum::warn,

                            "{} Failed to snappy uncompress a compressed "
                            "value",
                            logPrefix);
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
                                                      includeCollectionID,
                                                      enableExpiryOutput,
                                                      sid);
        }

        // Item unmodified - construct response from original.
        return std::make_unique<MutationResponse>(item,
                                                  opaque_,
                                                  includeValue,
                                                  includeXattributes,
                                                  includeDeleteTime,
                                                  includeCollectionID,
                                                  enableExpiryOutput,
                                                  sid);
    }
    return SystemEventProducerMessage::make(opaque_, item, sid);
}

void ActiveStream::processItems(OutstandingItemsResult& outstandingItemsResult,
                                const LockHolder& streamMutex) {
    if (!outstandingItemsResult.items.empty()) {
        // Transform the sequence of items from the CheckpointManager into
        // a sequence of DCP messages which this stream should receive. There
        // are a couple of sublties to watch out for here:
        //
        // 1. Unlike CheckpointManager, In DCP there are no individual 'start' /
        // end messages book-ending mutations - instead we prefix a sequence of
        // mutations with a snapshot_marker{start, end, flags}. However, we do
        // not know the end seqno until we get to the end of the checkpoint. To
        // handle this we accumulate the set of mutations which will make up a
        // snapshot into 'mutations', and when we encounter the next
        // checkpoint_start message we call snapshot() on our mutations to
        // prepend the snapshot_marker; followed by the mutations it contains.
        //
        // 2. For each checkpoint_start item we need to create a snapshot with
        // the MARKER_FLAG_CHK set - so the destination knows this represents
        // a consistent point and should create it's own checkpoint on this
        // boundary.
        // However, a snapshot marker must contain at least 1
        // (non-snapshot_start) item, but if the last item in `items` is a
        // checkpoint_marker then it is not possible to create a valid snapshot
        // (yet). We must instead defer calling snapshot() until we have at
        // least one item - i.e on a later call to processItems.
        // Therefore we record the pending MARKER_FLAG_CHK as part of the
        // object's state in nextSnapshotIsCheckpoint. When we subsequently
        // receive at least one more mutation (and hence can enqueue a
        // SnapshotMarker), we can use nextSnapshotIsCheckpoint to snapshot
        // it correctly.
        std::deque<std::unique_ptr<DcpResponse>> mutations;

        // Initialise to the first visibleSeqno of the batch of items
        uint64_t visibleSeqno = outstandingItemsResult.visibleSeqno;
        for (auto& qi : outstandingItemsResult.items) {
            if (shouldProcessItem(*qi)) {
                curChkSeqno = qi->getBySeqno();
                lastReadSeqnoUnSnapshotted = qi->getBySeqno();
                // Check if the item is allowed on the stream, note the filter
                // updates itself for collection deletion events
                if (filter.checkAndUpdate(*qi)) {
                    if (qi->isVisible()) {
                        visibleSeqno = qi->getBySeqno();
                    }
                    mutations.push_back(makeResponseFromItem(
                            qi, SendCommitSyncWriteAs::Commit));
                }

            } else if (qi->getOperation() == queue_op::checkpoint_start) {
                /* if there are already other mutations, then they belong to the
                   previous checkpoint and hence we must create a snapshot and
                   put them onto readyQ */
                if (!mutations.empty()) {
                    snapshot(outstandingItemsResult.checkpointType,
                             mutations,
                             outstandingItemsResult.highCompletedSeqno,
                             visibleSeqno);
                    /* clear out all the mutations since they are already put
                       onto the readyQ */
                    mutations.clear();
                }
                /* mark true as it indicates a new checkpoint snapshot */
                nextSnapshotIsCheckpoint = true;
            }
        }

        if (!mutations.empty()) {
            snapshot(outstandingItemsResult.checkpointType,
                     mutations,
                     outstandingItemsResult.highCompletedSeqno,
                     visibleSeqno);
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

bool ActiveStream::shouldProcessItem(const Item& item) {
    if (!item.shouldReplicate(supportSyncWrites())) {
        return false;
    }

    if (item.getOperation() == queue_op::system_event) {
        switch (SystemEvent(item.getFlags())) {
        case SystemEvent::Collection:
        case SystemEvent::Scope:
            return true;
        }
        return false;
    }
    return true;
}

void ActiveStream::snapshot(CheckpointType checkpointType,
                            std::deque<std::unique_ptr<DcpResponse>>& items,
                            boost::optional<uint64_t> highCompletedSeqno,
                            uint64_t maxVisibleSeqno) {
    if (items.empty()) {
        return;
    }

    /* This assumes that all items in the "items deque" is put onto readyQ */
    lastReadSeqno.store(lastReadSeqnoUnSnapshotted);

    if (isCurrentSnapshotCompleted()) {
        const auto isCkptTypeDisk = checkpointType == CheckpointType::Disk;
        uint32_t flags = isCkptTypeDisk ? MARKER_FLAG_DISK : MARKER_FLAG_MEMORY;

        // Get OptionalSeqnos which for the items list types should have values
        auto seqnoStart = items.front()->getBySeqno();
        auto seqnoEnd = items.back()->getBySeqno();
        if (!seqnoStart || !seqnoEnd) {
            throw std::logic_error(
                    logPrefix +
                    "ActiveStream::snapshot incorrect DcpEvent, missing a "
                    "seqno " +
                    std::string(items.front()->to_string()) + " " +
                    std::string(items.back()->to_string()) + " " + logPrefix);
        }

        uint64_t snapStart = *seqnoStart;
        uint64_t snapEnd = *seqnoEnd;

        if (nextSnapshotIsCheckpoint) {
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

        // If the stream supports SyncRep then send the HCS for CktpType::disk
        const auto sendHCS = supportSyncReplication() && isCkptTypeDisk;
        const auto hcsToSend = sendHCS ? highCompletedSeqno : boost::none;
        if (sendHCS) {
            Expects(hcsToSend.is_initialized());
            log(spdlog::level::level_enum::info,
                "{} ActiveStream::snapshot: Sending disk snapshot with start "
                "seqno {}, end seqno {}, and"
                " high completed seqno {}",
                logPrefix,
                snapStart,
                snapEnd,
                hcsToSend);
        }

        pushToReadyQ(std::make_unique<SnapshotMarker>(
                opaque_,
                vb_,
                snapStart,
                snapEnd,
                flags,
                hcsToSend,
                boost::optional<uint64_t>{supportSyncReplication(),
                                          maxVisibleSeqno},
                sid));
        lastSentSnapEndSeqno.store(snapEnd, std::memory_order_relaxed);

        // Here we can just clear this flag as it is set every time we process
        // a checkpoint_start item in ActiveStream::processItems.
        nextSnapshotIsCheckpoint = false;
    }

    for (auto& item : items) {
        pushToReadyQ(std::move(item));
    }
}

void ActiveStream::setDeadInner(end_stream_status_t status) {
    {
        LockHolder lh(streamMutex);
        endStream(status);
    }

    if (status != END_STREAM_DISCONNECTED) {
        notifyStreamReady();
    }
}

uint32_t ActiveStream::setDead(end_stream_status_t status) {
    setDeadInner(status);
    removeAcksFromDM();
    return 0;
}

void ActiveStream::setDead(end_stream_status_t status,
                           folly::SharedMutex::WriteHolder& vbstateLock) {
    setDeadInner(status);
    removeAcksFromDM(vbstateLock);
}

void ActiveStream::removeAcksFromDM(
        boost::optional<folly::SharedMutex::WriteHolder&> vbstateLock) {
    // Remove any unknown acks for the stream. Why here and not on
    // destruction of the object? We could be replacing an existing
    // DcpProducer with another. This old ActiveStream may then live on
    // (owned by a backfill) and clear a seqno ack from a new ActiveStream.
    if (supportSyncReplication()) {
        auto vb = engine->getVBucket(vb_);
        if (!vb) {
            return;
        }

        // Get the consumer name from the producer so that we can clear the
        // correct ack
        std::string consumerName;
        {
            auto p = producerPtr.lock();
            if (!p) {
                log(spdlog::level::warn,
                    "Producer could not be locked when"
                    "attempting to clear queued seqno acks");
                return;
            }
            consumerName = p->getConsumerName();
        }

        if (consumerName.empty()) {
            log(spdlog::level::warn,
                "Consumer name not found for producer when"
                "attempting to clear queued seqno acks");
            return;
        }

        if (vbstateLock) {
            vb->removeAcksFromADM(consumerName, *vbstateLock);
        } else {
            vb->removeAcksFromADM(
                    consumerName,
                    folly::SharedMutex::ReadHolder(vb->getStateLock()));
        }
    }
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
            pushToReadyQ(std::make_unique<StreamEndResponse>(
                    opaque_, reason, vb_, sid));
        }

        // If we ended normally then print at info level to prevent views
        // from spamming our logs
        auto level = reason == END_STREAM_OK ? spdlog::level::level_enum::info
                                             : spdlog::level::level_enum::warn;
        log(level,
            "{} Stream closing, sent until seqno {} remaining items "
            "{}, reason: {}",
            logPrefix,
            lastSentSeqno.load(),
            readyQ_non_meta_items.load(),
            getEndStreamStatusStr(reason).c_str());
    }
}

void ActiveStream::scheduleBackfill_UNLOCKED(bool reschedule) {
    if (isBackfillTaskRunning) {
        log(spdlog::level::level_enum::info,
            "{} Skipping "
            "scheduleBackfill_UNLOCKED; "
            "lastReadSeqno {}"
            ", reschedule flag "
            ": {}",
            logPrefix,
            lastReadSeqno.load(),
            reschedule ? "True" : "False");
        return;
    }

    VBucketPtr vbucket = engine->getVBucket(vb_);
    if (!vbucket) {
        log(spdlog::level::level_enum::warn,
            "{} Failed to schedule "
            "backfill as unable to get vbucket; "
            "lastReadSeqno : {}"
            ", "
            "reschedule : {}",
            logPrefix,
            lastReadSeqno.load(),
            reschedule ? "True" : "False");
        return;
    }

    auto producer = producerPtr.lock();
    if (!producer) {
        log(spdlog::level::level_enum::warn,
            "{} Aborting scheduleBackfill_UNLOCKED() "
            "as the producer conn is deleted; "
            "lastReadSeqno : {}"
            ", "
            "reschedule : {}",
            logPrefix,
            lastReadSeqno.load(),
            reschedule ? "True" : "False");
        return;
    }

    uint64_t backfillStart = lastReadSeqno.load() + 1;
    uint64_t backfillEnd;
    bool tryBackfill;

    if (flags_ & static_cast<uint64_t>(DCP_ADD_STREAM_FLAG_DISKONLY)) {
        // if disk only, always backfill to the requested end seqno
        backfillEnd = end_seqno_;
        tryBackfill = true;
    } else {
        /* not disk only - stream may require backfill but will transition to
         * in-memory afterward; register the cursor now.
         * There are two expected cases:
         *  1: registerResult.tryBackfill=true, which means
         *     - Cursor at start of first checkpoint
         *     - CheckpointManager can't provide all the items needed
         *       so a backfill may be required before moving to
         *       in-memory streaming.
         *  2: registerResult.tryBackfill=false
         *     - The CheckpointManager contains the required items
         *     - No backfill needed
         */

        CursorRegResult registerResult;
        try {
            registerResult = vbucket->checkpointManager->registerCursorBySeqno(
                    name_, lastReadSeqno.load());
        } catch (std::exception& error) {
            log(spdlog::level::level_enum::warn,
                "{} Failed to register "
                "cursor: {}",
                logPrefix,
                error.what());
            endStream(END_STREAM_STATE);
            return;
        }

        log(spdlog::level::level_enum::info,
            "{} ActiveStream::scheduleBackfill_UNLOCKED register cursor "
            "with "
            "name \"{}\" backfill:{}, seqno:{}",
            logPrefix,
            name_,
            registerResult.tryBackfill,
            registerResult.seqno);

        curChkSeqno = registerResult.seqno;
        tryBackfill = registerResult.tryBackfill;
        cursor = registerResult.cursor;

        if (lastReadSeqno.load() > curChkSeqno) {
            // something went wrong registering the cursor - it is too early
            // and could read items this stream has already sent.
            throw std::logic_error(
                    "ActiveStream::scheduleBackfill_UNLOCKED: "
                    "lastReadSeqno (which is " +
                    std::to_string(lastReadSeqno.load()) +
                    " ) is greater than curChkSeqno (which is " +
                    std::to_string(curChkSeqno) + " ). " + "for stream " +
                    producer->logHeader() + "; " + logPrefix);
        }

        // _if_ a backfill is required, it should end either at the
        // requested stream end seqno OR the seqno immediately
        // before what the checkpoint manager can provide
        // - whichever is lower.
        backfillEnd = std::min(end_seqno_, curChkSeqno - 1);
    }

    if (tryBackfill && tryAndScheduleOSOBackfill(*producer, *vbucket)) {
        return;
    } else if (tryBackfill &&
               producer->scheduleBackfillManager(*vbucket,
                                                 shared_from_this(),
                                                 backfillStart,
                                                 backfillEnd)) {
        // backfill will be needed to catch up to the items in the
        // CheckpointManager
        log(spdlog::level::level_enum::info,
            "{} Scheduling backfill "
            "from {} to {}, reschedule "
            "flag : {}",
            logPrefix,
            backfillStart,
            backfillEnd,
            reschedule ? "True" : "False");

        isBackfillTaskRunning.store(true);
        /// Number of backfill items is unknown until the Backfill task
        /// completes the scan phase - reset backfillRemaining counter.
        backfillRemaining.reset();
    } else {
        // backfill not needed
        if (flags_ & static_cast<uint64_t>(DCP_ADD_STREAM_FLAG_DISKONLY)) {
            endStream(END_STREAM_OK);
        } else if (flags_ &
                   static_cast<uint64_t>(DCP_ADD_STREAM_FLAG_TAKEOVER)) {
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

bool ActiveStream::tryAndScheduleOSOBackfill(DcpProducer& producer,
                                             VBucket& vb) {
    // OSO only allowed:
    // if the filter is set to a single collection.
    // if this is the initial backfill request
    // if the client has enabled OSO
    if (filter.singleCollection() && lastReadSeqno.load() == 0 &&
        curChkSeqno.load() > lastReadSeqno.load() + 1 &&
        producer.isOutOfOrderSnapshotsEnabled()) {
        CollectionID cid = filter.front();

        // OSO possible - engage.
        producer.scheduleBackfillManager(vb, shared_from_this(), cid);
        // backfill will be needed to catch up to the items in the
        // CheckpointManager
        log(spdlog::level::level_enum::info,
            "{} Scheduling OSO backfill "
            "for cid:{} lastReadSeqno:{} curChkSeqno:{}",
            logPrefix,
            cid.to_string(),
            lastReadSeqno.load(),
            curChkSeqno.load());

        isBackfillTaskRunning.store(true);
        /// Number of backfill items is unknown until the Backfill task
        /// completes the scan phase - reset backfillRemaining counter.
        backfillRemaining.reset();
        return true;
    }
    return false;
}

void ActiveStream::notifyEmptyBackfill(uint64_t lastSeenSeqno) {
    LockHolder lh(streamMutex);
    notifyEmptyBackfill_UNLOCKED(lastSeenSeqno);
}

void ActiveStream::notifyEmptyBackfill_UNLOCKED(uint64_t lastSeenSeqno) {
    setBackfillRemaining_UNLOCKED(0);
    auto vbucket = engine->getVBucket(vb_);
    if (!cursor.lock()) {
        try {
            CursorRegResult result =
                    vbucket->checkpointManager->registerCursorBySeqno(
                            name_, lastSeenSeqno);
            log(spdlog::level::level_enum::info,
                "{} ActiveStream::notifyEmptyBackfill "
                "Re-registering dropped cursor with name \"{}\", "
                "backfill:{}, seqno:{}",
                logPrefix,
                name_,
                result.tryBackfill,
                result.seqno);
            curChkSeqno = result.seqno;
            cursor = result.cursor;
        } catch (std::exception& error) {
            log(spdlog::level::level_enum::warn,
                "{} Failed to register "
                "cursor: {}",
                logPrefix,
                error.what());
            endStream(END_STREAM_STATE);
        }
    }
}

bool ActiveStream::handleSlowStream() {
    LockHolder lh(streamMutex);
    log(spdlog::level::level_enum::info,
        "{} Handling slow stream; "
        "state_ : {}, "
        "lastReadSeqno : {}"
        ", "
        "lastSentSeqno : {}"
        ", "
        "vBucketHighSeqno : {}"
        ", "
        "isBackfillTaskRunning : {}",
        logPrefix,
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
    case StreamState::Pending: {
        auto producer = producerPtr.lock();
        std::string connHeader = producer ? producer->logHeader()
                                          : "DCP (Producer): **Deleted conn**";
        throw std::logic_error(
                "ActiveStream::handleSlowStream: "
                "called with state " +
                to_string(state_.load()) +
                " "
                "for stream " +
                connHeader + "; " + logPrefix);
    }
    }
    return false;
}

std::string ActiveStream::getStreamTypeName() const {
    return "Active";
}

std::string ActiveStream::getStateName() const {
    return to_string(state_);
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
        "{} ActiveStream::transitionState: "
        "Transitioning from {} to {}",
        logPrefix,
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
                to_string(state_.load()) + ") " + logPrefix);
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
            // Starting a new in-memory snapshot which could contain duplicate
            // keys compared to the previous backfill snapshot. Therefore set
            // the Checkpoint flag on the next snapshot so the Consumer will
            // know to create a new Checkpoint.
            nextSnapshotIsCheckpoint = true;
            nextCheckpointItem();
        }
        break;
    case StreamState::TakeoverSend:
        takeoverStart = ep_current_time();

        // Starting a new in-memory (takeover) snapshot which could contain
        // duplicate keys compared to the previous Backfill snapshot. Therefore
        // set the Checkpoint flag on the next snapshot so the Consumer will
        // know to create a new Checkpoint.
        nextSnapshotIsCheckpoint = true;

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
    return removeCheckpointCursor();
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
    bool inverse = false;
    if (force || itemsReady.compare_exchange_strong(inverse, true)) {
        auto producer = producerPtr.lock();
        if (!producer) {
            return;
        }
        producer->notifyStreamReady(vb_);
    }
}

bool ActiveStream::removeCheckpointCursor() {
    VBucketPtr vb = engine->getVBucket(vb_);
    if (vb) {
        if (vb->checkpointManager->removeCursor(cursor.lock().get())) {
            /*
             * Although the cursor has been removed from the cursor map
             * the underlying shared_ptr can still be valid due to other
             * uses of the cursor not yet going out of scope
             * (e.g. ClosedUnrefCheckpointRemoverTask).  Therefore
             * cursor.lock().get() may not return the nullptr, so reset the
             * cursor to ensure that it is not used.
             */
            cursor.reset();
            return true;
        }
    }
    return false;
}

ENGINE_ERROR_CODE ActiveStream::seqnoAck(const std::string& consumerName,
                                         uint64_t preparedSeqno) {
    VBucketPtr vb = engine->getVBucket(vb_);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    // Take the vb state lock so that we don't change the state of
    // this vb. Done before the streamMutex is acquired to prevent a lock order
    // inversion.
    {
        folly::SharedMutex::ReadHolder vbStateLh(vb->getStateLock());

        // Locked with the streamMutex to ensure that we cannot race with a
        // stream end
        {
            LockHolder lh(streamMutex);

            // We cannot ack something on a dead stream.
            if (!isActive()) {
                return ENGINE_SUCCESS;
            }

            if (preparedSeqno > getLastSentSeqno()) {
                throw std::logic_error(
                        vb_.to_string() + " replica \"" + consumerName +
                        "\" acked seqno:" + std::to_string(preparedSeqno) +
                        " which is greater than last sent seqno:" +
                        std::to_string(getLastSentSeqno()));
            }

            return vb->seqnoAcknowledged(
                    vbStateLh, consumerName, preparedSeqno);
        } // end stream mutex lock scope
    } // end vb state lock scope
}

std::string ActiveStream::to_string(StreamState st) {
    switch (st) {
    case StreamState::Pending:
        return "pending";
    case StreamState::Backfilling:
        return "backfilling";
    case StreamState::InMemory:
        return "in-memory";
    case StreamState::TakeoverSend:
        return "takeover-send";
    case StreamState::TakeoverWait:
        return "takeover-wait";
    case StreamState::Dead:
        return "dead";
    }
    throw std::invalid_argument("ActiveStream::to_string(StreamState): " +
                                std::to_string(int(st)));
}

bool ActiveStream::collectionAllowed(CollectionID cid) const {
    return filter.check(cid);
}
