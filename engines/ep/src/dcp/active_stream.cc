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

#include "active_stream_impl.h"

#include "checkpoint.h"
#include "checkpoint_manager.h"
#include "dcp/producer.h"
#include "dcp/response.h"
#include "ep_time.h"
#include "kv_bucket.h"
#include "vbucket.h"

#include <fmt/chrono.h>
#include <memcached/protocol_binary.h>
#include <platform/optional.h>
#include <platform/timeutils.h>
#include <statistics/cbstat_collector.h>

// OutstandingItemsResult ctor and dtor required to be defined out of line to
// allow us to forward declare CheckpointSnapshotRange
ActiveStream::OutstandingItemsResult::OutstandingItemsResult() = default;
ActiveStream::OutstandingItemsResult::~OutstandingItemsResult() = default;

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
                           IncludeDeletedUserXattrs includeDeletedUserXattrs,
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
      includeValue(includeVal),
      includeXattributes(includeXattrs),
      includeDeletedUserXattrs(includeDeletedUserXattrs),
      lastReadSeqnoUnSnapshotted(st_seqno),
      lastSentSeqno(st_seqno),
      lastSentSeqnoAdvance(0),
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
      includeDeleteTime(includeDeleteTime),
      pitrEnabled(p->isPointInTimeEnabled()),
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
      flatBuffersSystemEventsEnabled(p->areFlatBuffersSystemEventsEnabled()),
      filter(std::move(f)),
      sid(filter.getStreamId()),
      changeStreamsEnabled(p->areChangeStreamsEnabled()) {
    const char* type = "";
    if (isTakeoverStream()) {
        type = "takeover ";
        end_seqno_ = dcpMaxSeqno;
    } else if (pitrEnabled == PointInTimeEnabled::Yes) {
        type = "PiTR ";
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
        "requested end seqno was {}, snapshot:{{{},{}}} "
        "collections-filter-size:{} {}",
        logPrefix,
        type,
        st_seqno,
        end_seqno_,
        en_seqno,
        snap_start_seqno,
        snap_end_seqno,
        filter.size(),
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
        std::lock_guard<std::mutex> lh(streamMutex);
        endStream(cb::mcbp::DcpStreamEndStatus::Ok);
        itemsReady.store(true);
        // lock is released on leaving the scope
    }
    auto streamInfo = "ActiveStream " + name_ + " " + logPrefix;
    nextSnapStart.setLabel(streamInfo + " nextSnapStart");
}

ActiveStream::~ActiveStream() {
    if (state_ != StreamState::Dead) {
        removeCheckpointCursor();
    }
}

std::unique_ptr<DcpResponse> ActiveStream::next(DcpProducer& producer) {
    std::lock_guard<std::mutex> lh(streamMutex);

    // Clear notification flag before checking for a response, as if there was
    // nothing available when we checked, we want to be notified again when
    // more items are available. We do this to avoid a lost wake-up, in the
    // event we are notified about a new seqno just after we have found
    // no response is ready.
    // Note however this does mean we can get spurious wakeups between here
    // and when we set itemsReady at the end of this function.
    itemsReady.store(false);

    std::unique_ptr<DcpResponse> response;
    switch (state_.load()) {
    case StreamState::Pending:
        break;
    case StreamState::Backfilling:
        response = backfillPhase(producer, lh);
        break;
    case StreamState::InMemory:
        response = inMemoryPhase(producer);
        break;
    case StreamState::TakeoverSend:
        response = takeoverSendPhase(producer);
        break;
    case StreamState::TakeoverWait:
        response = takeoverWaitPhase(producer);
        break;
    case StreamState::Dead:
        response = deadPhase(producer);
        break;
    }

    if (nextHook) {
        nextHook(response.get());
    }

    // We have at least one response, and hence will call next() at least one
    // more time (a null response is used to indicate the Stream has no items
    // currently available) - as such set the itemsReady flag to avoid
    // unnecessary notifications - we know we need to check again.
    if (response) {
        itemsReady.store(true);
    }
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
        CursorRegResult result = chkptmgr.registerCursorBySeqno(
                name_, lastProcessedSeqno, CheckpointCursor::Droppable::Yes);

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
        endStream(cb::mcbp::DcpStreamEndStatus::StateChanged);
    }
}

bool ActiveStream::markDiskSnapshot(uint64_t startSeqno,
                                    uint64_t endSeqno,
                                    std::optional<uint64_t> highCompletedSeqno,
                                    uint64_t maxVisibleSeqno,
                                    std::optional<uint64_t> timestamp,
                                    SnapshotSource source) {
    {
        std::unique_lock<std::mutex> lh(streamMutex);

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
            if (!isCollectionEnabledStream()) {
                /* the connection does not support sync writes or collections,
                 * so the snapshot end must be set to the seqno of a visible
                 * item. Thus, items after the MVS will not be sent. As we are
                 * the client can not process non-visible items nor can we
                 * inform it that a seqno has moved to the end of the snapshot
                 * using a SeqnoAdvanced op.
                 */
                endSeqno = maxVisibleSeqno;
            }
            if (endSeqno < startSeqno) {
                // no visible items in backfill, should not send
                // a snapshot marker at all (no data will be sent)
                log(spdlog::level::level_enum::info,
                    "{} "
                    "ActiveStream::markDiskSnapshot not sending snapshot "
                    "because it contains no visible items",
                    logPrefix);
                // reregister cursor at original end seqno
                notifyEmptyBackfill_UNLOCKED(chkCursorSeqno);
                return false;
            }
        }

        /* We may need to send the requested 'snap_start_seqno_' as the snapshot
           start when we are sending the first snapshot because the first
           snapshot could be resumption of a previous snapshot */
        startSeqno = adjustStartIfFirstSnapshot(
                startSeqno, source != SnapshotSource::NoHistoryPrologue);

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
                    "snapshot end seqno after merge s:{} - e:{}",
                    logPrefix,
                    startSeqno,
                    endSeqno,
                    info.range.getStart(),
                    info.range.getEnd());
                endSeqno = info.range.getEnd();
            }
        }

        // If the stream supports SyncRep then send the HCS in the
        // SnapshotMarker if it is not 0
        auto sendHCS = supportSyncReplication() && highCompletedSeqno;
        auto hcsToSend = sendHCS ? highCompletedSeqno : std::nullopt;
        auto mvsToSend = supportSyncReplication()
                                 ? std::make_optional(maxVisibleSeqno)
                                 : std::nullopt;

        auto flags = MARKER_FLAG_DISK | MARKER_FLAG_CHK;

        if (source == SnapshotSource::History) {
            flags |= (MARKER_FLAG_HISTORY |
                      MARKER_FLAG_MAY_CONTAIN_DUPLICATE_KEYS);
        }

        log(spdlog::level::level_enum::info,
            "{} ActiveStream::markDiskSnapshot: Sending disk snapshot with "
            "start:{}, end:{}, flags:0x{:x}, hcs:{}, mvs:{}",
            logPrefix,
            startSeqno,
            endSeqno,
            flags,
            to_string_or_none(hcsToSend),
            to_string_or_none(mvsToSend));
        pushToReadyQ(std::make_unique<SnapshotMarker>(opaque_,
                                                      vb_,
                                                      startSeqno,
                                                      endSeqno,
                                                      flags,
                                                      hcsToSend,
                                                      mvsToSend,
                                                      timestamp,
                                                      sid));
        lastSentSnapEndSeqno.store(endSeqno, std::memory_order_relaxed);

        if (!isDiskOnly()) {
            // Only re-register the cursor if we still need to get memory
            // snapshots
            registerCursor(*vb->checkpointManager, chkCursorSeqno);
        }
    }
    notifyStreamReady();
    return true;
}

bool ActiveStream::markOSODiskSnapshot(uint64_t endSeqno) {
    {
        std::unique_lock<std::mutex> lh(streamMutex);

        if (!isBackfilling()) {
            log(spdlog::level::level_enum::warn,
                "{} ActiveStream::"
                "markOSODiskSnapshot: Unexpected state_:{}",
                logPrefix,
                to_string(state_.load()));
            return false;
        }

        if (!isDiskOnly()) {
            VBucketPtr vb = engine->getVBucket(vb_);
            if (!vb) {
                log(spdlog::level::level_enum::warn,
                    "{} "
                    "ActiveStream::markOSODiskSnapshot, vbucket "
                    "does not exist",
                    logPrefix);
                return false;
            }
            registerCursor(*vb->checkpointManager, endSeqno);
            log(spdlog::level::level_enum::info,
                "{} ActiveStream::markOSODiskSnapshot: Sent snapshot "
                "begin marker, cursor requested:{} curChkSeqno:{}",
                logPrefix,
                endSeqno,
                curChkSeqno.load());
        } else {
            log(spdlog::level::level_enum::info,
                "{} ActiveStream::markOSODiskSnapshot: Sent snapshot "
                "begin marker",
                logPrefix);
        }
        pushToReadyQ(std::make_unique<OSOSnapshot>(opaque_, vb_, sid));
    }
    notifyStreamReady();
    return true;
}

bool ActiveStream::backfillReceived(std::unique_ptr<Item> itm,
                                    backfill_source_t backfill_source) {
    if (!itm) {
        return false;
    }

    // Should the item replicate?
    if (!shouldProcessItem(*itm)) {
        return true; // skipped, but return true as it's not a failure
    }

    // Is the item accepted by the stream filter (e.g matching collection?)
    if (!filter.checkAndUpdate(*itm)) {
        // Skip this item, but continue backfill at next item.
        return true;
    }

    queued_item qi(std::move(itm));
    // We need to send a mutation instead of a commit if this Item is a
    // commit as we may have de-duped the preceding prepare and the replica
    // needs to know what to commit.
    auto resp = makeResponseFromItem(qi, SendCommitSyncWriteAs::Mutation);

    auto producer = producerPtr.lock();
    if (!producer) {
        // Producer no longer valid (e.g. DCP connection closed), return false
        // to stop backfill task.
        return false;
    }

    {
        // Locked scope for ActiveStream state reads / writes. Note
        // streamMutex is heavily contended - frontend thread must acquire it
        // to consume data from ActiveStream::readyQ so try to minimise work
        // under lock.
        std::unique_lock<std::mutex> lh(streamMutex);

        // isBackfilling reads ActiveStream::state hence requires streamMutex.
        if (!isBackfilling()) {
            // Stream no longer backfilling; return false to stop backfill
            // task.
            return false;
        }

        // recordBackfillManagerBytesRead requires a valid backillMgr hence
        // must occur after isBackfilling check (and hence must be in locked
        // region) :(
        if (!producer->recordBackfillManagerBytesRead(
                    resp->getApproximateSize())) {
            return false;
        }

        // Passed all checks, item will be added to ready queue now.
        bufferedBackfill.bytes.fetch_add(resp->getApproximateSize());
        bufferedBackfill.items++;
        lastBackfilledSeqno = std::max<uint64_t>(lastBackfilledSeqno,
                                                 uint64_t(*resp->getBySeqno()));
        pushToReadyQ(std::move(resp));
    }

    notifyStreamReady(false /*force*/, producer.get());

    if (backfill_source == BACKFILL_FROM_MEMORY) {
        backfillItems.memory++;
    } else {
        backfillItems.disk++;
    }

    return true;
}

void ActiveStream::completeBackfill(std::chrono::steady_clock::duration runtime,
                                    size_t diskBytesRead) {
    // maxSeqno is not needed for InOrder completion
    completeBackfillInner(
            BackfillType::InOrder, 0 /*maxSeqno*/, runtime, diskBytesRead);
}

void ActiveStream::completeOSOBackfill(
        uint64_t maxSeqno,
        std::chrono::steady_clock::duration runtime,
        size_t diskBytesRead) {
    completeBackfillInner(
            BackfillType::OutOfSequenceOrder, maxSeqno, runtime, diskBytesRead);
}

void ActiveStream::snapshotMarkerAckReceived() {
    if (--waitForSnapshot == 0) {
        notifyStreamReady();
    }
}

void ActiveStream::setVBucketStateAckRecieved(DcpProducer& producer) {
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
                endStream(cb::mcbp::DcpStreamEndStatus::Ok);
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

    notifyStreamReady(false /*force*/, &producer);
}

void ActiveStream::setBackfillRemaining(size_t value) {
    std::lock_guard<std::mutex> guard(streamMutex);
    setBackfillRemaining_UNLOCKED(value);
}

void ActiveStream::setBackfillRemaining_UNLOCKED(size_t value) {
    backfillRemaining = value;
}

std::unique_ptr<DcpResponse> ActiveStream::backfillPhase(
        DcpProducer& producer, std::lock_guard<std::mutex>& lh) {
    auto resp = nextQueuedItem(producer);

    if (resp) {
        producer.recordBackfillManagerBytesSent(resp->getApproximateSize());
        bufferedBackfill.bytes.fetch_sub(resp->getApproximateSize());
        if (!resp->isMetaEvent() || resp->isSystemEvent()) {
            bufferedBackfill.items--;
        }

        // Only DcpResponse objects representing items from "disk" have a size
        // so only update backfillRemaining when non-zero
        if (resp->getApproximateSize() && backfillRemaining.has_value()) {
            (*backfillRemaining)--;
        }
    }

    if (!isBackfillTaskRunning && readyQ.empty()) {
        // Given readyQ.empty() is True resp will be NULL
        // The previous backfill has completed.  Check to see if another
        // backfill needs to be scheduled.
        if (pendingBackfill) {
            scheduleBackfill_UNLOCKED(producer, true);
            pendingBackfill = false;
            // After scheduling a backfill we may now have items in readyQ -
            // so re-check if we didn't already have a response.
            if (!resp) {
                resp = nextQueuedItem(producer);
            }
        } else {
            if (lastReadSeqno.load() >= end_seqno_) {
                endStream(cb::mcbp::DcpStreamEndStatus::Ok);
            } else if (isTakeoverStream()) {
                transitionState(StreamState::TakeoverSend);
            } else if (isDiskOnly()) {
                endStream(cb::mcbp::DcpStreamEndStatus::Ok);
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
                resp = nextQueuedItem(producer);
            }
        }
    }

    return resp;
}

std::unique_ptr<DcpResponse> ActiveStream::inMemoryPhase(
        DcpProducer& producer) {
    if (readyQ.empty()) {
        if (pendingBackfill) {
            // Moving the state from InMemory to Backfilling will result in a
            // backfill being scheduled
            transitionState(StreamState::Backfilling);
            pendingBackfill = false;
            return {};
        } else if (nextCheckpointItem(producer)) {
            return {};
        }
    }

    return nextQueuedItem(producer);
}

std::unique_ptr<DcpResponse> ActiveStream::takeoverSendPhase(
        DcpProducer& producer) {
    VBucketPtr vb = engine->getVBucket(vb_);
    if (vb && takeoverStart != 0 && !vb->isTakeoverBackedUp() &&
        (ep_current_time() - takeoverStart) > takeoverSendMaxTime) {
        vb->setTakeoverBackedUpState(true);
    }

    if (!readyQ.empty()) {
        return nextQueuedItem(producer);
    } else {
        if (nextCheckpointItem(producer)) {
            return {};
        }
    }

    if (waitForSnapshot != 0) {
        return {};
    }

    takeoverSendPhaseHook();

    if (producer.bufferLogInsert(SetVBucketState::baseMsgBytes)) {
        transitionState(StreamState::TakeoverWait);
        return std::make_unique<SetVBucketState>(opaque_, vb_, takeoverState);
    } else {
        // Force notification of the stream, with no new mutations we might get
        // stuck otherwise as returning no item doesn't add this vBucket back to
        // the producer's readyQueue
        notifyStreamReady(true, &producer);
    }

    return {};
}

std::unique_ptr<DcpResponse> ActiveStream::takeoverWaitPhase(
        DcpProducer& producer) {
    return nextQueuedItem(producer);
}

std::unique_ptr<DcpResponse> ActiveStream::deadPhase(DcpProducer& producer) {
    auto resp = nextQueuedItem(producer);
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

bool ActiveStream::isCompressionEnabled() const {
    auto producer = producerPtr.lock();
    if (producer) {
        return producer->isCompressionEnabled();
    }
    /* If the 'producer' is deleted, what we return doesn't matter */
    return false;
}

void ActiveStream::addStats(const AddStatFn& add_stat, const CookieIface* c) {
    Stream::addStats(add_stat, c);

    try {
        fmt::memory_buffer keyBuff;
        fmt::format_to(keyBuff, "{}:stream_{}_", name_, vb_.get());
        const auto prefixLen = keyBuff.size();
        const auto addStat = [&keyBuff, prefixLen, add_stat, c](
                                     const auto& statKey, auto statValue) {
            keyBuff.resize(prefixLen);
            fmt::format_to(keyBuff, "{}", statKey);
            add_casted_stat(
                    {keyBuff.data(), keyBuff.size()}, statValue, add_stat, c);
        };
        addStat("backfill_disk_items", backfillItems.disk.load());
        addStat("backfill_mem_items", backfillItems.memory.load());
        addStat("backfill_sent", backfillItems.sent.load());
        addStat("memory_phase", itemsFromMemoryPhase.load());
        addStat("last_sent_seqno", lastSentSeqno.load());
        addStat("last_sent_seqno_advance", lastSentSeqnoAdvance.load());
        addStat("last_sent_snap_end_seqno",
                lastSentSnapEndSeqno.load(std::memory_order_relaxed));
        addStat("last_read_seqno", lastReadSeqno.load());
        addStat("last_read_seqno_unsnapshotted",
                lastReadSeqnoUnSnapshotted.load());
        addStat("ready_queue_memory", getReadyQueueMemory());
        addStat("backfill_buffer_bytes", bufferedBackfill.bytes.load());
        addStat("backfill_buffer_items", bufferedBackfill.items.load());
        addStat("cursor_registered", cursor.lock() != nullptr);
        addStat("change_streams_enabled", changeStreamsEnabled);

        if (isTakeoverSend() && takeoverStart != 0) {
            addStat("takeover_since", ep_current_time() - takeoverStart);
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
    std::lock_guard<std::mutex> lh(streamMutex);

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

std::unique_ptr<DcpResponse> ActiveStream::nextQueuedItem(
        DcpProducer& producer) {
    if (!readyQ.empty()) {
        auto& response = readyQ.front();
        if (producer.bufferLogInsert(response->getMessageSize())) {
            auto seqno = response->getBySeqno();
            if (seqno) {
                // When OSO is enabled, and we're backfilling lastSentSeqno
                // isn't monotonic so just reset() to set it.
                if (producer.isOutOfOrderSnapshotsEnabled() &&
                    isBackfilling()) {
                    lastSentSeqno.reset(*seqno);
                } else {
                    lastSentSeqno.store(*seqno);
                }

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

bool ActiveStream::nextCheckpointItem(DcpProducer& producer) {
    auto vb = engine->getVBucket(vb_);
    if (vb) {
        const auto curs = cursor.lock();
        if (curs && vb->checkpointManager->hasNonMetaItemsForCursor(*curs)) {
            // Schedule the stream-processor for pulling items from checkpoints
            // and pushing them into the stream readyQ
            producer.scheduleCheckpointProcessorTask(shared_from_this());
            return true;
        }
    }

    return chkptItemsExtractionInProgress;
}

void ActiveStream::nextCheckpointItemTask() {
    // MB-29369: Obtain stream mutex here
    std::lock_guard<std::mutex> lh(streamMutex);
    nextCheckpointItemTask(lh);
}

void ActiveStream::nextCheckpointItemTask(
        const std::lock_guard<std::mutex>& streamMutex) {
    VBucketPtr vbucket = engine->getVBucket(vb_);
    if (vbucket) {
        auto producer = producerPtr.lock();
        if (!producer) {
            return;
        }

        // MB-29369: only run the task's work if the stream is in an in-memory
        // phase (of which takeover is a variant).
        if (isInMemory() || isTakeoverSend()) {
            auto res = getOutstandingItems(*vbucket);
            processItems(res, streamMutex);
        }
    } else {
        /* The entity deleting the vbucket must set stream to dead,
           calling setDead(cb::mcbp::DcpStreamEndStatus::StateChanged) will
           cause deadlock because it will try to grab streamMutex which is
           already acquired at this point here */
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
    result.ranges = itemsForCursor.ranges;
    if (isDiskCheckpointType(result.checkpointType)) {
        result.diskCheckpointState =
                OutstandingItemsResult::DiskCheckpointState();
        Expects(itemsForCursor.highCompletedSeqno);
        result.diskCheckpointState->highCompletedSeqno =
                *itemsForCursor.highCompletedSeqno;
    }

    result.visibleSeqno = itemsForCursor.visibleSeqno;
    if (vb.checkpointManager->hasClosedCheckpointWhichCanBeRemoved()) {
        engine->getKVBucket()->wakeUpCheckpointMemRecoveryTask();
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
                             IncludeDeletedUserXattrs includeDeletedUserXattrs,
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
        if (mcbp::datatype::is_xattr(item->getDataType())) {
            // Do we want to strip all xattrs regardless of whether the item is
            // a mutation or deletion?
            if (includeXattrs == IncludeXattrs::No) {
                return true;
            }

            // Do we want to strip user-xattrs for deletions?
            if (includeDeletedUserXattrs == IncludeDeletedUserXattrs::No &&
                item->isDeleted()) {
                return true;
            }
        }
    }

    return false;
}

std::unique_ptr<DcpResponse> ActiveStream::makeResponseFromItem(
        queued_item& item, SendCommitSyncWriteAs sendCommitSyncWriteAs) {
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
                                                 item->getKey(),
                                                 includeCollectionID);
    }

    if (item->getOperation() == queue_op::abort_sync_write) {
        return std::make_unique<AbortSyncWrite>(
                opaque_,
                item->getVBucketId(),
                item->getKey(),
                item->getPrepareSeqno(),
                item->getBySeqno() /*abortSeqno*/,
                includeCollectionID);
    }

    if (item->getOperation() != queue_op::system_event) {
        if (shouldModifyItem(item,
                             includeValue,
                             includeXattributes,
                             includeDeletedUserXattrs,
                             isForceValueCompressionEnabled(),
                             isSnappyEnabled())) {
            auto finalItem = make_STRCPtr<Item>(*item);
            const auto wasInflated = finalItem->removeBodyAndOrXattrs(
                    includeValue, includeXattributes, includeDeletedUserXattrs);

            if (isSnappyEnabled()) {
                if (isForceValueCompressionEnabled()) {
                    if (finalItem->getNBytes() > 0) {
                        bool compressionFailed = false;

                        if (!mcbp::datatype::is_snappy(
                                    finalItem->getDataType())) {
                            compressionFailed = !finalItem->compressValue();
                        } else if (wasInflated == Item::WasValueInflated::Yes) {
                            // MB-40493: IncludeValue::NoWithUnderlyingDatatype
                            // may reset the datatype to Snappy and leave an
                            // inflated Xattr chunk that requires compression.
                            // We would miss to compress here if we check just
                            // the datatype.
                            compressionFailed =
                                    !finalItem->compressValue(true /*force*/);
                        }

                        if (compressionFailed) {
                            log(spdlog::level::level_enum::warn,
                                "{} Failed to snappy compress an uncompressed "
                                "value",
                                logPrefix);
                        }
                    }
                }
            } else {
                // The purpose of this block is to uncompress compressed items
                // as they are being streamed over a connection that doesn't
                // support compression.
                //
                // MB-40493: IncludeValue::NoWithUnderlyingDatatype may reset
                //  datatype to SNAPPY, even if the value has been already
                //  decompressed (eg, the original value contained Body+Xattr
                //  and Body have been removed) or if there is no value at all
                //  (eg, the original value contained only a Body, now removed).
                //  We need to avoid the call to Item::decompress in both cases,
                //  we log an unnecessary warning otherwise.
                if (mcbp::datatype::is_snappy(finalItem->getDataType()) &&
                    (wasInflated == Item::WasValueInflated::No) &&
                    (finalItem->getNBytes() > 0)) {
                    if (!finalItem->decompressValue()) {
                        log(spdlog::level::level_enum::warn,
                            "{} Failed to snappy uncompress a compressed value",
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
                                                      includeDeletedUserXattrs,
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
                                                  includeDeletedUserXattrs,
                                                  includeCollectionID,
                                                  enableExpiryOutput,
                                                  sid);
    }

    if (flatBuffersSystemEventsEnabled) {
        return SystemEventProducerMessage::makeWithFlatBuffersValue(
                opaque_, item, sid);
    }
    return SystemEventProducerMessage::make(opaque_, item, sid);
}

void ActiveStream::processItems(
        OutstandingItemsResult& outstandingItemsResult,
        const std::lock_guard<std::mutex>& streamMutex) {
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
        /*
         * highNonVisibleSeqno is used to track the current seqno of non visible
         * seqno of a snapshot before we filter them out. This is only used when
         * collections is enabled on a stream and sync write support is not.
         * This allows us to inform the consumer of the high seqno of a
         * collection regardless if it is committed or not. By sending a
         * SeqnoAdvanced op. This solves the problem where a snapshot would be
         * sent to a non sync write aware client with the last mutation of the
         * snapshot was a prepare or abort and the final seqno would never be
         * sent meaning the snapshot was never completed.
         */
        std::optional<uint64_t> highNonVisibleSeqno;
        for (auto& qi : outstandingItemsResult.items) {
            if (qi->getOperation() == queue_op::checkpoint_end) {
                // At the end of each checkpoint remove its snapshot range, so
                // we don't use it to set nextSnapStart for the next checkpoint.
                // We can just erase the range at the head of ranges as every
                // time as CheckpointManager::getItemsForCursor() will always
                // ensure there is a snapshot range for if there is a
                // queue_op::checkpoint_end in the items it returns.
                auto rangeItr = outstandingItemsResult.ranges.begin();
                outstandingItemsResult.ranges.erase(rangeItr);
            }

            if (qi->getOperation() == queue_op::checkpoint_start) {
                /* if there are already other mutations, then they belong to the
                   previous checkpoint and hence we must create a snapshot and
                   put them onto readyQ */
                if (!mutations.empty()) {
                    snapshot(outstandingItemsResult.checkpointType,
                             mutations,
                             outstandingItemsResult.diskCheckpointState,
                             visibleSeqno,
                             highNonVisibleSeqno);
                    /* clear out all the mutations since they are already put
                       onto the readyQ */
                    mutations.clear();
                    highNonVisibleSeqno = std::nullopt;
                }
                /* mark true as it indicates a new checkpoint snapshot */
                nextSnapshotIsCheckpoint = true;

                if (outstandingItemsResult.ranges.empty()) {
                    throw std::logic_error(
                            "ActiveStream::processItems: found "
                            "no snapshot ranges but we have a "
                            "checkpoint start with seqno:" +
                            std::to_string(qi->getBySeqno()));
                }

                nextSnapStart =
                        outstandingItemsResult.ranges.begin()->getStart();

                continue;
            }

            if (!qi->isCheckPointMetaItem()) {
                curChkSeqno = qi->getBySeqno();
            }

            if (shouldProcessItem(*qi)) {
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
            } else if (isSeqnoAdvancedEnabled() &&
                       !qi->isCheckPointMetaItem() &&
                       filter.checkAndUpdate(*qi)) {
                // Can replace with SeqnoAdvance and the item is for this stream
                highNonVisibleSeqno = qi->getBySeqno();
            }
        }

        if (!mutations.empty()) {
            snapshot(outstandingItemsResult.checkpointType,
                     mutations,
                     outstandingItemsResult.diskCheckpointState,
                     visibleSeqno,
                     highNonVisibleSeqno);
        } else if (isSeqnoAdvancedEnabled()) {
            // Note that we cannot enter this case if supportSyncReplication()
            // returns true (see isSeqnoAdvancedEnabled). This means that we
            // do not need to set the HCS/MVS or timestamp parameters of the
            // snapshot marker. MB-47877 tracks enabling sync-writes+filtering
            if (!firstMarkerSent && lastReadSeqno < snap_end_seqno_) {
                // MB-47009: This first snapshot has been completely filtered
                // away. The remaining items must not of been for this client.
                // We must still send a snapshot marker so that the client is
                // moved to their end seqno - so a snapshot + seqno advance is
                // needed.
                sendSnapshotAndSeqnoAdvanced(
                        outstandingItemsResult.checkpointType,
                        snap_start_seqno_,
                        snap_end_seqno_);
                firstMarkerSent = true;
            } else if (isSeqnoGapAtEndOfSnapshot(curChkSeqno)) {
                auto vb = engine->getVBucket(getVBucket());
                if (vb) {
                    if (vb->getState() == vbucket_state_replica) {
                        /*
                         * If this is a collection stream and we're not sending
                         * any mutations from memory and we haven't queued a
                         * snapshot and we're a replica. Then our snapshot
                         * covers backfill and in memory. So we have one
                         * snapshot marker for both items on disk and in memory.
                         * Thus, we need to send a SeqnoAdvanced to push the
                         * consumer's seqno to the end of the snapshot. This is
                         * needed when no items for the collection we're
                         * streaming are present in memory.
                         */
                        queueSeqnoAdvanced();
                    }
                } else {
                    log(spdlog::level::level_enum::warn,
                        "{} processItems() for vbucket which does not "
                        "exist",
                        logPrefix);
                }
            } else if (highNonVisibleSeqno &&
                       curChkSeqno >= highNonVisibleSeqno.value()) {
                // MB-48368: Nothing directly available for the stream, but a
                // non-visible item was available - bring the client up-to-date
                sendSnapshotAndSeqnoAdvanced(
                        outstandingItemsResult.checkpointType,
                        highNonVisibleSeqno.value(),
                        highNonVisibleSeqno.value());
            }
        }
        // if we've processed past the stream's end seqno then transition to the
        // stream to the dead state and add a stream end to the ready queue
        if (curChkSeqno >= getEndSeqno()) {
            endStream(cb::mcbp::DcpStreamEndStatus::Ok);
        }
    }

    // After the snapshot has been processed, check if the filter is now empty
    // a stream with an empty filter does nothing but self close
    if (filter.empty()) {
        // Filter is now empty empty, so endStream
        endStream(cb::mcbp::DcpStreamEndStatus::FilterEmpty);
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
        case SystemEvent::ModifyCollection:
            // Modify cannot be transmitted unless opted-in
            return flatBuffersSystemEventsEnabled;
        }
        return false;
    }
    return true;
}

void ActiveStream::snapshot(
        CheckpointType checkpointType,
        std::deque<std::unique_ptr<DcpResponse>>& items,
        std::optional<OutstandingItemsResult::DiskCheckpointState>
                diskCheckpointState,
        uint64_t maxVisibleSeqno,
        std::optional<uint64_t> highNonVisibleSeqno) {
    if (items.empty()) {
        return;
    }

    /* This assumes that all items in the "items deque" is put onto readyQ */
    lastReadSeqno.store(lastReadSeqnoUnSnapshotted);

    if (isCurrentSnapshotCompleted()) {
        const auto isCkptTypeDisk = isDiskCheckpointType(checkpointType);
        uint32_t flags = isCkptTypeDisk ? MARKER_FLAG_DISK : MARKER_FLAG_MEMORY;

        if (changeStreamsEnabled) {
            flags |= MARKER_FLAG_HISTORY;
        }

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

        // Pin the snapshot start seqno to the checkpoint's start seqno if this
        // a checkpoint snapshot. But only do this if there's a gap in the seqno
        // range between the last snapshot's endSeqno and this snapshot's
        // startSeqno
        if (nextSnapshotIsCheckpoint && nextSnapStart > lastSentSnapEndSeqno) {
            snapStart = nextSnapStart;
        }

        /*
         * If the highNonVisibleSeqno has been set and it higher than the snap
         * end of the filtered mutations it means that the last item in the snap
         * shot is not visible i.e. a prepare or abort. Thus we need need to
         * extend the snapshot end to this value and then send a SeqnoAdvanced
         * at the end of the snapshot to inform the client of this.
         */
        if (highNonVisibleSeqno.has_value() &&
            highNonVisibleSeqno.value() > snapEnd) {
            snapEnd = highNonVisibleSeqno.value();
        }

        if (nextSnapshotIsCheckpoint) {
            flags |= MARKER_FLAG_CHK;
        }

        if (isTakeoverSend()) {
            waitForSnapshot++;
            flags |= MARKER_FLAG_ACK;
        }

        // If the stream supports SyncRep then send the HCS for CktpType::disk
        const auto sendHCS = supportSyncReplication() && isCkptTypeDisk;
        std::optional<uint64_t> hcsToSend;
        if (sendHCS) {
            Expects(diskCheckpointState);
            hcsToSend = diskCheckpointState->highCompletedSeqno;
            log(spdlog::level::level_enum::info,
                "{} ActiveStream::snapshot: Sending disk snapshot with start "
                "seqno {}, end seqno {}, and"
                " high completed seqno {}",
                logPrefix,
                snapStart,
                snapEnd,
                diskCheckpointState->highCompletedSeqno);
        }

        /* We need to send the requested 'snap_start_seqno_' as the snapshot
           start when we are sending the first snapshot because the first
           snapshot could be resumption of a previous snapshot */
        if (!firstMarkerSent) {
            snapStart = std::min(snap_start_seqno_, snapStart);
            firstMarkerSent = true;
        }

        const auto mvsToSend = supportSyncReplication()
                                       ? std::make_optional(maxVisibleSeqno)
                                       : std::nullopt;

        pushToReadyQ(std::make_unique<SnapshotMarker>(
                opaque_,
                vb_,
                snapStart,
                snapEnd,
                flags,
                hcsToSend,
                mvsToSend,
                std::optional<uint64_t>{}, // @todo MB-37319
                sid));
        lastSentSnapEndSeqno.store(snapEnd, std::memory_order_relaxed);

        // Here we can just clear this flag as it is set every time we process
        // a checkpoint_start item in ActiveStream::processItems.
        nextSnapshotIsCheckpoint = false;
    }

    for (auto& item : items) {
        pushToReadyQ(std::move(item));
    }

    if (isSeqnoAdvancedEnabled() && isSeqnoGapAtEndOfSnapshot(curChkSeqno)) {
        queueSeqnoAdvanced();
    }
}

void ActiveStream::setDeadInner(cb::mcbp::DcpStreamEndStatus status) {
    {
        std::lock_guard<std::mutex> lh(streamMutex);
        endStream(status);
    }

    if (status != cb::mcbp::DcpStreamEndStatus::Disconnected) {
        notifyStreamReady();
    }
}

uint32_t ActiveStream::setDead(cb::mcbp::DcpStreamEndStatus status) {
    setDeadInner(status);
    removeAcksFromDM();
    return 0;
}

void ActiveStream::setDead(cb::mcbp::DcpStreamEndStatus status,
                           folly::SharedMutex::WriteHolder& vbstateLock) {
    setDeadInner(status);
    removeAcksFromDM(&vbstateLock);
}

void ActiveStream::removeAcksFromDM(
        folly::SharedMutex::WriteHolder* vbstateLock) {
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
                    "({}) Producer could not be locked when"
                    "attempting to clear queued seqno acks",
                    vb_);
                return;
            }
            consumerName = p->getConsumerName();
        }

        if (consumerName.empty()) {
            log(spdlog::level::warn,
                "({}) Consumer name not found for producer when"
                "attempting to clear queued seqno acks",
                vb_);
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

void ActiveStream::notifySeqnoAvailable(DcpProducer& producer) {
    if (isActive()) {
        notifyStreamReady(false /*force*/, &producer);
    }
}

void ActiveStream::endStream(cb::mcbp::DcpStreamEndStatus reason) {
    if (isActive()) {
        // Cache the number remaining items as if we call clear_UNLOCKED() then
        // readyQ_non_meta_items will be reset to 0.
        auto cachedRemainingItems = readyQ_non_meta_items.load();
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
        if (reason != cb::mcbp::DcpStreamEndStatus::Disconnected) {
            pushToReadyQ(std::make_unique<StreamEndResponse>(
                    opaque_, reason, vb_, sid));
        }

        // If we ended normally then print at info level. Normally covers the
        // expected reasons for ending a stream, such as vbucket state changed
        // or a change in privileges, they are operationally quite normal.
        auto level = spdlog::level::level_enum::info;

        switch (reason) {
        case cb::mcbp::DcpStreamEndStatus::Ok:
        case cb::mcbp::DcpStreamEndStatus::Closed:
        case cb::mcbp::DcpStreamEndStatus::StateChanged:
        case cb::mcbp::DcpStreamEndStatus::LostPrivileges:
        case cb::mcbp::DcpStreamEndStatus::FilterEmpty:
            break;
        // A disconnect is abnormal
        case cb::mcbp::DcpStreamEndStatus::Disconnected:
        // A slow client is abnormal (never sent)
        case cb::mcbp::DcpStreamEndStatus::Slow:
        // A failing backfill is not good
        case cb::mcbp::DcpStreamEndStatus::BackfillFail:
        // Rollback indicates a failure/failover may of occurred
        case cb::mcbp::DcpStreamEndStatus::Rollback:
            level = spdlog::level::level_enum::warn;
        }
        log(level,
            "{} Stream closing, sent until seqno {} remaining items "
            "{}, reason: {}",
            logPrefix,
            lastSentSeqno.load(),
            cachedRemainingItems,
            cb::mcbp::to_string(reason));
    }
}

void ActiveStream::scheduleBackfill_UNLOCKED(DcpProducer& producer,
                                             bool reschedule) {
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

    uint64_t backfillStart = lastReadSeqno.load() + 1;
    uint64_t backfillEnd;
    bool tryBackfill;

    if (isDiskOnly()) {
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
                    name_,
                    lastReadSeqno.load(),
                    CheckpointCursor::Droppable::Yes);
        } catch (std::exception& error) {
            log(spdlog::level::level_enum::warn,
                "{} Failed to register "
                "cursor: {}",
                logPrefix,
                error.what());
            endStream(cb::mcbp::DcpStreamEndStatus::StateChanged);
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
                    producer.logHeader() + "; " + logPrefix);
        }

        // _if_ a backfill is required, it should end either at the
        // requested stream end seqno OR the seqno immediately
        // before what the checkpoint manager can provide
        // - whichever is lower.
        backfillEnd = std::min(end_seqno_, curChkSeqno - 1);
    }

    if (tryBackfill && tryAndScheduleOSOBackfill(producer, *vbucket)) {
        return;
    } else if (tryBackfill &&
               producer.scheduleBackfillManager(*vbucket,
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
        if (isDiskOnly()) {
            endStream(cb::mcbp::DcpStreamEndStatus::Ok);
        } else if (isTakeoverStream()) {
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
            notifyStreamReady(false /*force*/, &producer);
        }
    }
}

bool ActiveStream::tryAndScheduleOSOBackfill(DcpProducer& producer,
                                             VBucket& vb) {
    // OSO only allowed:
    // if the filter is set to a single collection.
    // if this is the initial backfill request
    // if the client has enabled OSO
    if (producer.isOutOfOrderSnapshotsEnabled() && filter.singleCollection() &&
        lastReadSeqno.load() == 0 &&
        ((curChkSeqno.load() > lastReadSeqno.load() + 1) || (isDiskOnly()))) {
        CollectionID cid = filter.front();

        // OSO possible - engage.
        producer.scheduleBackfillManager(vb, shared_from_this(), cid);
        // backfill will be needed to catch up to the items in the
        // CheckpointManager
        log(spdlog::level::level_enum::info,
            "{} Scheduling OSO backfill "
            "for cid:{} diskOnly:{} lastReadSeqno:{} curChkSeqno:{}",
            logPrefix,
            cid.to_string(),
            isDiskOnly(),
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

void ActiveStream::completeBackfillInner(
        BackfillType backfillType,
        uint64_t maxSeqno,
        std::chrono::steady_clock::duration runtime,
        size_t diskBytesRead) {
    {
        std::lock_guard<std::mutex> lh(streamMutex);

        // backfills can be scheduled and return nothing, leaving
        // lastBackfilledSeqno to be behind lastReadSeqno. Only update when
        // greater.
        if (lastBackfilledSeqno > lastReadSeqno) {
            lastReadSeqno.store(lastBackfilledSeqno);
        }

        if (backfillType == BackfillType::InOrder) {
            // In-order backfills may require a seqno-advanced message if
            // there is a stream filter present (e.g. only streaming a single
            // collection).
            if (isSeqnoAdvancedNeededBackFill()) {
                queueSeqnoAdvanced();
            }
            // reset last seqno seen by backfill
            maxScanSeqno = 0;
        }

        if (isBackfilling()) {
            const auto diskItemsRead = backfillItems.disk.load();
            const auto runtimeSecs =
                    std::chrono::duration<double>(runtime).count();

            log(spdlog::level::level_enum::info,
                "{} {}Backfill complete. {} items consisting of {} bytes read "
                "from disk, "
                "{} items from memory, lastReadSeqno:{} "
                "lastSentSeqnoAdvance:{}, pendingBackfill:{}. Total runtime {} "
                "({} item/s, {} MB/s)",
                logPrefix,
                backfillType == BackfillType::OutOfSequenceOrder ? "OSO " : "",
                diskItemsRead,
                diskBytesRead,
                backfillItems.memory.load(),
                lastReadSeqno.load(),
                lastSentSeqnoAdvance.load(),
                pendingBackfill ? "True" : "False",
                cb::time2text(runtime),
                diskItemsRead ? int(diskItemsRead / runtimeSecs) : 0,
                diskBytesRead
                        ? int((diskBytesRead / runtimeSecs) / (1024 * 1024))
                        : 0);
        } else {
            log(spdlog::level::level_enum::warn,
                "{} ActiveStream::completeBackfillInner: "
                "Unexpected state_:{}",
                logPrefix,
                to_string(state_.load()));
        }

        if (backfillType == BackfillType::OutOfSequenceOrder) {
            auto producer = producerPtr.lock();

            if (!producer) {
                log(spdlog::level::level_enum::warn,
                    "{} ActiveStream::completeBackfillInner: producer "
                    "unavailable",
                    logPrefix);
            } else if (
                    producer->isOutOfOrderSnapshotsEnabledWithSeqnoAdvanced() &&
                    maxSeqno != lastBackfilledSeqno) {
                pushToReadyQ(std::make_unique<SeqnoAdvanced>(
                        opaque_, vb_, sid, maxSeqno));
                lastSentSeqnoAdvance.store(maxSeqno);
            }

            pushToReadyQ(std::make_unique<OSOSnapshot>(
                    opaque_, vb_, sid, OSOSnapshot::End{}));
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

void ActiveStream::clear_UNLOCKED() {
    while (!readyQ.empty()) {
        popFromReadyQ();
    }
}

void ActiveStream::notifyEmptyBackfill_UNLOCKED(uint64_t lastSeenSeqno) {
    setBackfillRemaining_UNLOCKED(0);
    auto vbucket = engine->getVBucket(vb_);
    if (!cursor.lock()) {
        try {
            CursorRegResult result =
                    vbucket->checkpointManager->registerCursorBySeqno(
                            name_,
                            lastSeenSeqno,
                            CheckpointCursor::Droppable::Yes);
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
            endStream(cb::mcbp::DcpStreamEndStatus::StateChanged);
        }
    }
}

bool ActiveStream::handleSlowStream() {
    std::lock_guard<std::mutex> lh(streamMutex);
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
    case StreamState::Backfilling: {
        auto producer = producerPtr.lock();
        if (producer) {
            if (StreamState::Pending == oldState) {
                scheduleBackfill_UNLOCKED(*producer, false /* reschedule */);
            } else if (StreamState::InMemory == oldState) {
                scheduleBackfill_UNLOCKED(*producer, true /* reschedule */);
            }
        }
        break;
    }
    case StreamState::InMemory:
        // Check if the producer has sent up till the last requested
        // sequence number already, if not - move checkpoint items into
        // the ready queue.
        if (lastSentSeqno.load() >= end_seqno_) {
            // Stream transitioning to DEAD state
            endStream(cb::mcbp::DcpStreamEndStatus::Ok);
            notifyStreamReady();
        } else {
            // Starting a new in-memory snapshot which could contain duplicate
            // keys compared to the previous backfill snapshot. Therefore set
            // the Checkpoint flag on the next snapshot so the Consumer will
            // know to create a new Checkpoint.
            nextSnapshotIsCheckpoint = true;

            auto producer = producerPtr.lock();
            if (producer) {
                nextCheckpointItem(*producer);
            }
        }
        break;
    case StreamState::TakeoverSend: {
        takeoverStart = ep_current_time();

        // Starting a new in-memory (takeover) snapshot which could contain
        // duplicate keys compared to the previous Backfill snapshot. Therefore
        // set the Checkpoint flag on the next snapshot so the Consumer will
        // know to create a new Checkpoint.
        nextSnapshotIsCheckpoint = true;

        auto producer = producerPtr.lock();
        if (producer && !nextCheckpointItem(*producer)) {
            notifyStreamReady(true);
        }
    } break;
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
        endStream(cb::mcbp::DcpStreamEndStatus::StateChanged);
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

void ActiveStream::notifyStreamReady(bool force, DcpProducer* producer) {
    bool inverse = false;
    if (force || itemsReady.compare_exchange_strong(inverse, true)) {
        /**
         * The below block of code exists to reduce the amount of times that we
         * have to promote the producerPtr (weak_ptr<DcpProducer>). Callers that
         * have already done so can supply a raw ptr for us to use instead.
         */
        if (producer) {
            // Caller supplied a producer to call this on, use that
            producer->notifyStreamReady(vb_);
            return;
        }

        // No producer supplied, promote the weak_ptr and use that
        auto lkProducer = producerPtr.lock();
        if (!lkProducer) {
            return;
        }
        lkProducer->notifyStreamReady(vb_);
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
             * (e.g. CheckpointMemRecoveryTask).  Therefore
             * cursor.lock().get() may not return the nullptr, so reset the
             * cursor to ensure that it is not used.
             */
            cursor.reset();
            return true;
        }
    }
    return false;
}

cb::engine_errc ActiveStream::seqnoAck(const std::string& consumerName,
                                       uint64_t preparedSeqno) {
    VBucketPtr vb = engine->getVBucket(vb_);
    if (!vb) {
        return cb::engine_errc::not_my_vbucket;
    }

    // Take the vb state lock so that we don't change the state of
    // this vb. Done before the streamMutex is acquired to prevent a lock order
    // inversion.
    {
        folly::SharedMutex::ReadHolder vbStateLh(vb->getStateLock());

        // Locked with the streamMutex to ensure that we cannot race with a
        // stream end
        {
            std::lock_guard<std::mutex> lh(streamMutex);

            // We cannot ack something on a dead stream.
            if (!isActive()) {
                return cb::engine_errc::success;
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

bool ActiveStream::collectionAllowed(DocKey key) const {
    return filter.check(key);
}

bool ActiveStream::endIfRequiredPrivilegesLost(DcpProducer& producer) {
    // Does this stream still have the appropriate privileges to operate?
    if (filter.checkPrivileges(*producer.getCookie(), *engine) !=
        cb::engine_errc::success) {
        std::unique_lock lh(streamMutex);
        endStream(cb::mcbp::DcpStreamEndStatus::LostPrivileges);
        lh.unlock();
        notifyStreamReady();
        return true;
    }
    return false;
}

std::unique_ptr<DcpResponse> ActiveStream::makeEndStreamResponse(
        cb::mcbp::DcpStreamEndStatus reason) {
    return std::make_unique<StreamEndResponse>(opaque_, reason, vb_, sid);
}

void ActiveStream::queueSeqnoAdvanced() {
    const auto seqno = lastSentSnapEndSeqno.load();
    pushToReadyQ(std::make_unique<SeqnoAdvanced>(opaque_, vb_, sid, seqno));
    lastSentSeqnoAdvance.store(seqno);

    // MB-47009 and MB-47534
    // Set the lastReadSeqno to be the seqno-advance.
    // We have read and then discarded something - setting this value to be
    // where the stream has read to ensures we don't send further seqno
    // advances unless a new snapshot is generated.
    // This is conditional as at least one path already manages lastReadSeqno
    // before getting here (lastReadSeqno is a Monotonic type).
    if (lastReadSeqno.load() < seqno) {
        lastReadSeqno.store(seqno);
    }
}

bool ActiveStream::isDiskOnly() const {
    return flags_ & DCP_ADD_STREAM_FLAG_DISKONLY;
}

bool ActiveStream::isTakeoverStream() const {
    return flags_ & DCP_ADD_STREAM_FLAG_TAKEOVER;
}

bool ActiveStream::isSeqnoAdvancedEnabled() const {
    // SeqnoAdvance can only be sent if collections enabled as that's what added
    // the message.
    // Then we only require SeqnoAdvance for streams which don't enable:
    // sync-writes - so we can replace abort/prepare with seqno-advance
    // FlatBuffers - so we can replace ModifyCollection with seqno-advance
    return isCollectionEnabledStream() &&
           (!supportSyncReplication() || !flatBuffersSystemEventsEnabled);
}

bool ActiveStream::isSeqnoAdvancedNeededBackFill() const {
    if (!isSeqnoAdvancedEnabled() || !isSeqnoGapAtEndOfSnapshot(maxScanSeqno)) {
        return false;
    }
    /**
     * In most cases we want to send a SeqnoAdvanced op if we have not sent
     * the final seqno in the snapshot at the end of backfill. However,
     * replica vbucket may transition their snapshot from backfill to
     * streaming from memory without sending another snapshot. Thus, in this
     * case we do not want to send a SeqnoAdvanced at the end of backfill.
     * So check that we don't have an in memory range to stream from.
     */
    auto vb = engine->getVBucket(vb_);
    if (vb) {
        if (vb->getState() == vbucket_state_replica) {
            return maxScanSeqno > lastBackfilledSeqno &&
                   maxScanSeqno == lastSentSnapEndSeqno.load();
        }
    } else {
        log(spdlog::level::level_enum::warn,
            "{} isSeqnoAdvancedNeededBackFill() for vbucket which does not "
            "exist",
            logPrefix);
    }
    return isCurrentSnapshotCompleted();
}

bool ActiveStream::isSeqnoGapAtEndOfSnapshot(uint64_t streamSeqno) const {
    return (lastSentSnapEndSeqno.load() > lastReadSeqno.load()) &&
           lastSentSnapEndSeqno.load() == streamSeqno;
}

void ActiveStream::sendSnapshotAndSeqnoAdvanced(CheckpointType checkpointType,
                                                uint64_t start,
                                                uint64_t end) {
    start = adjustStartIfFirstSnapshot(start, true);

    const auto isCkptTypeDisk = isDiskCheckpointType(checkpointType);
    uint32_t flags = isCkptTypeDisk ? MARKER_FLAG_DISK : MARKER_FLAG_MEMORY;

    pushToReadyQ(std::make_unique<SnapshotMarker>(opaque_,
                                                  vb_,
                                                  start,
                                                  end,
                                                  flags,
                                                  std::nullopt,
                                                  std::nullopt,
                                                  std::nullopt,
                                                  sid));

    lastSentSnapEndSeqno.store(end, std::memory_order_relaxed);
    nextSnapshotIsCheckpoint = false;

    queueSeqnoAdvanced();
}

uint64_t ActiveStream::adjustStartIfFirstSnapshot(uint64_t start,
                                                  bool isCompleteSnapshot) {
    if (!firstMarkerSent) {
        if (isCompleteSnapshot) {
            firstMarkerSent = true;
        }
        return std::min(snap_start_seqno_, start);
    }
    return start;
}

ValueFilter ActiveStream::getValueFilter() const {
    ValueFilter valFilter = ValueFilter::VALUES_DECOMPRESSED;
    if (isKeyOnly()) {
        valFilter = ValueFilter::KEYS_ONLY;
    } else if (isCompressionEnabled()) {
        valFilter = ValueFilter::VALUES_COMPRESSED;
    }
    return valFilter;
}

void ActiveStream::setEndSeqno(uint64_t seqno) {
    end_seqno_ = seqno;
}

bool ActiveStream::areChangeStreamsEnabled() const {
    return changeStreamsEnabled;
}
