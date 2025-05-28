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
#include "configuration.h"
#include "dcp/backfill-manager.h"
#include "dcp/producer.h"
#include "dcp/response.h"
#include "ep_time.h"
#include "kv_bucket.h"
#include "kvstore/kvstore_iface.h"
#include "queue_op.h"
#include "vbucket.h"

#include <collections/vbucket_manifest_handles.h>
#include <fmt/chrono.h>
#include <logger/logger.h>
#include <memcached/protocol_binary.h>
#include <platform/backtrace.h>
#include <platform/exceptions.h>
#include <platform/optional.h>
#include <platform/timeutils.h>
#include <statistics/cbstat_collector.h>

// OutstandingItemsResult ctor and dtor required to be defined out of
// line to allow us to forward declare CheckpointSnapshotRange
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
      lastReadSeqno(st_seqno, {*this}),
      backfillRemaining(),
      includeValue(includeVal),
      includeXattributes(includeXattrs),
      includeDeletedUserXattrs(includeDeletedUserXattrs),
      lastSentSeqno(st_seqno, {*this}),
      lastSentSeqnoAdvance(0, {*this}),
      curChkSeqno(st_seqno, {*this}),
      nextSnapStart(0),
      takeoverState(vbucket_state_pending),
      itemsFromMemoryPhase(0),
      firstMarkerSent(false),
      waitForSnapshot(0),
      engine(e),
      producerPtr(p),
      takeoverSendMaxTime(e->getConfiguration().getDcpTakeoverMaxTime()),
      lastSentSnapStartSeqno(0, {*this}),
      lastSentSnapEndSeqno(0, {*this}),
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

    log(spdlog::level::info,
        "{} Creating {}stream with start seqno {} and end seqno {}; "
        "requested end seqno was {}, flags:{:x}, snapshot:{{{},{}}} "
        "{}, {}",
        logPrefix,
        type,
        st_seqno,
        end_seqno_,
        en_seqno,
        flags,
        snap_start_seqno,
        snap_end_seqno,
        filter.summary(),
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
}

ActiveStream::~ActiveStream() {
    if (state_ != StreamState::Dead) {
        removeCheckpointCursor();
    }

    std::shared_ptr<DcpProducer> producer;
    if (backfillUID && (producer = producerPtr.lock())) {
        if (!producer->removeBackfill(backfillUID)) {
            // Note: if this object is being destructed from the backfill itself
            // then we will fail to remove the backfill object (as it is not in
            // the backfill queues when running). Just log as debug as there
            // could be many "safe" reasons why the DCPBackfill object is not
            // removed by this call.
            log(spdlog::level::debug,
                "{} ~ActiveStream expected to remove backfillUID:{}",
                logPrefix,
                backfillUID);
        }
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

        if (result.tryBackfill) {
            pendingBackfill = true;
        }
        curChkSeqno = result.nextSeqno;
        cursor = result.takeCursor();

        Expects(result.position);
        log(spdlog::level::level_enum::info,
            "{} ActiveStream::registerCursor name \"{}\", "
            "lastProcessedSeqno:{}, pendingBackfill:{}, "
            "result{{tryBackfill:{}, op:{}, seqno:{}, nextSeqno:{}}}",
            logPrefix,
            name_,
            lastProcessedSeqno,
            pendingBackfill,
            result.tryBackfill,
            ::to_string(result.position->getOperation()),
            result.position->getBySeqno(),
            result.nextSeqno);
    } catch (std::exception& error) {
        log(spdlog::level::level_enum::warn,
            "{} Failed to register cursor: {}",
            logPrefix,
            error.what());
        endStream(cb::mcbp::DcpStreamEndStatus::StateChanged);
    }
}

// Helper function for setting the lastSentSnapEndSeqno (which is Monotonic)
//.The History snapshot can follow a NoHistory snapshot but both are the same
// underlying disk-snapshot. The endSeqno from each phase is equal. Here we
// avoid a Monotonic exception for that case without switching to a weak
// monotonic type or incorrectly using reset
static bool mustAssignEndSeqno(SnapshotType source,
                               uint64_t newEndSeqno,
                               uint64_t currentEndSeqno) {
    switch (source) {
    case SnapshotType::NoHistory:
    case SnapshotType::NoHistoryPrecedingHistory:
        // Always attempt assignment for these sources so we catch every
        // monotonic violation.
        return true;
    case SnapshotType::History:
    case SnapshotType::HistoryFollowingNoHistory:
        // tolerate newEndSeqno == currentEndSeqno, so only assign if they
        // are different, which catches every other monotonic violation.
        return newEndSeqno != currentEndSeqno;
    }

    folly::assume_unreachable();
}

bool ActiveStream::markDiskSnapshot(uint64_t startSeqno,
                                    uint64_t endSeqno,
                                    std::optional<uint64_t> highCompletedSeqno,
                                    uint64_t maxVisibleSeqno,
                                    std::optional<uint64_t> timestamp,
                                    SnapshotType snapshotType) {
    {
        std::unique_lock<std::mutex> lh(streamMutex);

        const auto originalEndSeqno = endSeqno;

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
                notifyEmptyBackfill_UNLOCKED(originalEndSeqno);
                return false;
            }
        }

        /* We may need to send the requested 'snap_start_seqno_' as the snapshot
           start when we are sending the first snapshot because the first
           snapshot could be resumption of a previous snapshot */
        const bool wasFirst = !firstMarkerSent;
        startSeqno = adjustStartIfFirstSnapshot(
                startSeqno,
                snapshotType != SnapshotType::NoHistoryPrecedingHistory);

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

        if (snapshotType == SnapshotType::History ||
            snapshotType == SnapshotType::HistoryFollowingNoHistory) {
            flags |= (MARKER_FLAG_HISTORY |
                      MARKER_FLAG_MAY_CONTAIN_DUPLICATE_KEYS);
        }

        if (snapshotType == SnapshotType::NoHistoryPrecedingHistory) {
            // When the source is the prologue to history, don't send the marker
            // but stash it until the backfill definitely returns data.
            // backfillRecevied can send it if it exists.
            pendingDiskMarker = std::make_unique<SnapshotMarker>(opaque_,
                                                                 vb_,
                                                                 startSeqno,
                                                                 endSeqno,
                                                                 flags,
                                                                 hcsToSend,
                                                                 mvsToSend,
                                                                 timestamp,
                                                                 sid);
        } else {
            log(spdlog::level::level_enum::info,
                "{} ActiveStream::markDiskSnapshot: Sending disk snapshot with "
                "start:{}, end:{}, flags:0x{:x}, flagsDecoded:{}, hcs:{}, "
                "mvs:{}, lastBackfilledSeqno:{}",
                logPrefix,
                startSeqno,
                endSeqno,
                flags,
                dcpMarkerFlagsToString(flags),
                to_string_or_none(hcsToSend),
                to_string_or_none(mvsToSend),
                lastBackfilledSeqno);
            // Clear the pending marker, it's no longer needed.
            // Note: Missing to reset the pending marker here would lead to
            // sending it unnecessarily in the case where the stream has an
            // empty NonHistory prologue.
            pendingDiskMarker.reset();
            pushToReadyQ(std::make_unique<SnapshotMarker>(opaque_,
                                                          vb_,
                                                          startSeqno,
                                                          endSeqno,
                                                          flags,
                                                          hcsToSend,
                                                          mvsToSend,
                                                          timestamp,
                                                          sid));
            // Update the last start seqno seen but handle base case as
            // lastSentSnapStartSeqno is initial zero
            if (startSeqno > 0) {
                lastSentSnapStartSeqno = startSeqno;
            }

            // Only compare last sent start with last sent end if end has
            // already been set (note ignore 0 == 0). When an OSO snapshot comes
            // before a history seqno-ordered snapshot, wasFirst is false, yet
            // the snapshot variables are still zero and we would hit this
            // exception.
            // Also, any subsequent backfill phase (eg, by CursorDrop) can
            // generate double markers with same seqno range for CDC
            // NonHistoryPrologue->History jumps, so we this check is invalid in
            // that case.
            if (!wasFirst && lastSentSnapStartSeqno <= lastSentSnapEndSeqno &&
                lastSentSnapStartSeqno && lastSentSnapEndSeqno &&
                snapshotType != SnapshotType::HistoryFollowingNoHistory) {
                auto msg = fmt::format(
                        "ActiveStream::markDiskSnapshot:"
                        "sent snapshot marker to client with snap start <= "
                        "previous snap end "
                        "{} "
                        "lastSentSnapStart:{} "
                        "lastSentSnapEnd:{} "
                        "snapStart:{} "
                        "snapEnd:{} "
                        "sid:{} "
                        "producer name:{} "
                        "lastReadSeqno:{} "
                        "curChkSeqno:{}",
                        vb_,
                        lastSentSnapStartSeqno.load(),
                        lastSentSnapEndSeqno.load(),
                        startSeqno,
                        endSeqno,
                        sid,
                        getName(),
                        lastReadSeqno.load(),
                        curChkSeqno.load());
                throw std::logic_error(msg);
            }

            // CDC: There are cases where there's no need to assign the seqno
            // again
            if (mustAssignEndSeqno(
                        snapshotType, endSeqno, lastSentSnapEndSeqno)) {
                lastSentSnapEndSeqno.store(endSeqno, std::memory_order_relaxed);
            }
        }

        if (!isDiskOnly() &&
            snapshotType != SnapshotType::HistoryFollowingNoHistory) {
            // Only re-register the cursor if we still need to get memory
            // snapshots and this is not the second markDiskSnapshot of a
            // combined CDC snapshot
            registerCursor(*vb->checkpointManager, originalEndSeqno);
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

bool ActiveStream::backfillReceived(std::unique_ptr<Item> item,
                                    backfill_source_t backfill_source) {
    if (!item) {
        return false;
    }

    auto producer = producerPtr.lock();
    if (!producer) {
        // Producer no longer valid (e.g. DCP connection closed), return false
        // to stop backfill task.
        return false;
    }

    try {
        backfillReceivedHook();
        // Should the item replicate?
        // Is the item accepted by the stream filter (e.g matching collection) ?
        if (!shouldProcessItem(*item) || !filter.checkAndUpdate(*item)) {
            // Skip this item, but continue backfill at next item.
            return true;
        }

        queued_item qi(std::move(item));
        // We need to send a mutation instead of a commit if this Item is a
        // commit as we may have de-duped the preceding prepare and the replica
        // needs to know what to commit.
        auto resp = makeResponseFromItem(qi, SendCommitSyncWriteAs::Mutation);

        bool buffersFull = false;
        {
            // Locked scope for ActiveStream state reads / writes. Note
            // streamMutex is heavily contended - frontend thread must acquire
            // it to consume data from ActiveStream::readyQ so try to minimise
            // work under lock.
            std::unique_lock<std::mutex> lh(streamMutex);

            // isBackfilling reads ActiveStream::state hence requires
            // streamMutex.
            if (!isBackfilling()) {
                // Stream no longer backfilling; return false to stop backfill
                // task.
                return false;
            }

            // Note: ActiveStream and Producer/BackfillManager buffer bytes
            // counters need to be both updated under streamMutex. That's
            // because the end-stream path uses stream counters for updating
            // prod/bm counters, so they need to be consistent.
            if (pendingDiskMarker) {
                // There is a marker, move it to the readyQ
                log(spdlog::level::level_enum::info,
                    "{} ActiveStream::backfillReceived(seqno:{}): Sending "
                    "pending disk snapshot with start:{}, end:{}, "
                    "flags:0x{:x}, flagsDecoded:{}, hcs:{}, mvs:{}",
                    logPrefix,
                    *resp->getBySeqno(),
                    pendingDiskMarker->getStartSeqno(),
                    pendingDiskMarker->getEndSeqno(),
                    pendingDiskMarker->getFlags(),
                    dcpMarkerFlagsToString(pendingDiskMarker->getFlags()),
                    to_string_or_none(
                            pendingDiskMarker->getHighCompletedSeqno()),
                    to_string_or_none(pendingDiskMarker->getMaxVisibleSeqno()));

                // Note: The presence of a pending disk marker means that we
                // were at SnapshotType::NoHistoryPrecedingHistory before this
                // point and now we have moved to SnapshotType::History. See
                // detail in the SnapshotType enum.
                if (mustAssignEndSeqno(SnapshotType::History,
                                    pendingDiskMarker->getEndSeqno(),
                                    lastSentSnapEndSeqno)) {
                    lastSentSnapEndSeqno.store(pendingDiskMarker->getEndSeqno(),
                                            std::memory_order_relaxed);
                }
                pushToReadyQ(std::move(pendingDiskMarker));
            }

            // Passed all checks, item will be added to ready queue now.
            const auto respSize = resp->getApproximateSize();
            bufferedBackfill.bytes.fetch_add(respSize);
            bufferedBackfill.items++;
            lastBackfilledSeqno = std::max<uint64_t>(
                    lastBackfilledSeqno, uint64_t(*resp->getBySeqno()));
            pushToReadyQ(std::move(resp));

            // Note: recordBackfillManagerBytesRead requires a valid backillMgr
            // hence must occur after isBackfilling check (and hence must be in
            // locked region) :(
            buffersFull = !producer->recordBackfillManagerBytesRead(respSize);
        }

        // Note: The call locks on streamMutex, so this needs to be executed
        // without holding the lock.
        notifyStreamReady(false /*force*/, producer.get());

        if (backfill_source == BACKFILL_FROM_MEMORY) {
            backfillItems.memory++;
        } else {
            backfillItems.disk++;
        }

        // We have processed this item but now the backfill buffers are full.
        // We need to inform the caller that this backfill has to yield.
        return !buffersFull;
    } catch (const std::exception& e) {
        handleDcpProducerException(e);
        return false;
    }
}

void ActiveStream::completeBackfill(uint64_t maxScanSeqno,
                                    std::chrono::steady_clock::duration runtime,
                                    size_t diskBytesRead) {
    completeBackfillInner(
            BackfillType::InOrder, maxScanSeqno, runtime, diskBytesRead);
}

void ActiveStream::completeOSOBackfill(
        uint64_t maxScanSeqno,
        std::chrono::steady_clock::duration runtime,
        size_t diskBytesRead) {
    completeBackfillInner(BackfillType::OutOfSequenceOrder,
                          maxScanSeqno,
                          runtime,
                          diskBytesRead);
    firstMarkerSent = true;
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
        // Order in which the below 3 locks are acquired is important to avoid
        // any potential lock inversion problems.
        //
        // Plus, CheckpointManager::queueSetVBState() notifies streams. We need
        // to make that call after releasing the streamMutex, we might deadlock
        // by lock-inversion or double-lock otherwise.
        std::unique_lock<std::mutex> epVbSetLh(
                engine->getKVBucket()->getVbSetMutexLock());
        folly::SharedMutex::WriteHolder vbStateLh(vbucket->getStateLock());

        bool needToSetVbState = false;
        {
            std::unique_lock<std::mutex> lh(streamMutex);
            if (!isTakeoverWait()) {
                log(spdlog::level::level_enum::warn,
                    "{} Unexpected ack for set vbucket op on "
                    "stream '{}' state '{}'",
                    logPrefix,
                    name_,
                    to_string(state_.load()));
                return;
            }

            if (takeoverState == vbucket_state_pending) {
                log(spdlog::level::level_enum::debug,
                    "{} Receive ack for set vbucket state to "
                    "pending message",
                    logPrefix);
                takeoverState = vbucket_state_active;
                transitionState(StreamState::TakeoverSend);
                needToSetVbState = true;
            } else {
                log(spdlog::level::level_enum::info,
                    "{} Receive ack for set vbucket state to "
                    "active message",
                    logPrefix);
                endStream(cb::mcbp::DcpStreamEndStatus::Ok);
            }
        }

        if (needToSetVbState) {
            // Note: streamMutex released when making the call
            engine->getKVBucket()->setVBucketState_UNLOCKED(
                    vbucket,
                    vbucket_state_dead,
                    false /* deleteVB */,
                    nullptr /* meta */,
                    TransferVB::No,
                    false /* notifyDcp */,
                    epVbSetLh,
                    vbStateLh);
            log(spdlog::level::level_enum::info,
                "{} Vbucket marked as dead, last sent "
                "seqno: {}, high seqno: {}",
                logPrefix,
                lastSentSeqno.load(),
                vbucket->getHighSeqno());
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

void ActiveStream::addStats(const AddStatFn& add_stat, CookieIface& c) {
    Stream::addStats(add_stat, c);

    try {
        const auto addStat = [&add_stat, &c](const auto& statKey,
                                             auto statValue) {
            add_casted_stat(statKey, statValue, add_stat, c);
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

    auto addFilterStat = [&add_stat](auto k, auto v, auto& c) {
        add_stat(fmt::format("filter_{}", k), v, c);
    };
    filter.addStats(addFilterStat, c, vb_);
}

void ActiveStream::addTakeoverStats(const AddStatFn& add_stat,
                                    CookieIface& cookie,
                                    const VBucket& vb) {
    std::lock_guard<std::mutex> lh(streamMutex);

    add_casted_stat("name", name_, add_stat, cookie);
    if (!isActive()) {
        log(spdlog::level::level_enum::warn,
            "{} "
            "ActiveStream::addTakeoverStats: Stream has "
            "status StreamDead",
            logPrefix);
        add_casted_stat("status", "stream_is_dead", add_stat, cookie);
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
        chk_items = vb.checkpointManager->getNumItemsForCursor(*sp);
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
        if (curs && vb->checkpointManager->hasItemsForCursor(*curs)) {
            // Schedule the stream-processor for pulling items from checkpoints
            // and pushing them into the stream readyQ
            producer.scheduleCheckpointProcessorTask(shared_from_this());
            return true;
        }
    }

    return chkptItemsExtractionInProgress;
}

void ActiveStream::nextCheckpointItemTask() {
    try {
        // MB-29369: Obtain stream mutex here
        std::lock_guard<std::mutex> lh(streamMutex);
        nextCheckpointItemTask(lh);
    } catch (const std::exception& e) {
        handleDcpProducerException(e);
    }
}

void ActiveStream::nextCheckpointItemTask(
        const std::lock_guard<std::mutex>& streamMutex) {
    auto vbucket = engine->getVBucket(vb_);
    if (!vbucket) {
        // The entity deleting the vbucket must set stream to dead,
        // calling setDead(cb::mcbp::DcpStreamEndStatus::StateChanged) will
        // cause deadlock because it will try to grab streamMutex which is
        // already acquired at this point here
        return;
    }

    if (!producerPtr.lock()) {
        // Nothing to do, the connection is being shut down
        return;
    }

    // MB-29369: only run the task's work if the stream is in an in-memory
    // phase (of which takeover is a variant).
    if (!(isInMemory() || isTakeoverSend())) {
        return;
    }

    auto res = getOutstandingItems(*vbucket);
    processItems(streamMutex, res);
}

ActiveStream::OutstandingItemsResult ActiveStream::getOutstandingItems(
        VBucket& vb) {
    OutstandingItemsResult result;
    // Commencing item processing - set guard flag.
    chkptItemsExtractionInProgress.store(true);

    auto _begin_ = std::chrono::steady_clock::now();
    CheckpointManager::ItemsForCursor itemsForCursor{};
    auto cursorPtr = cursor.lock();
    if (cursorPtr) {
        itemsForCursor = vb.checkpointManager->getNextItemsForDcp(*cursorPtr,
                                                                  result.items);
    }
    engine->getEpStats().dcpCursorsGetItemsHisto.add(
            std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - _begin_));

    result.checkpointType = itemsForCursor.checkpointType;
    result.ranges = itemsForCursor.ranges;
    result.historical = itemsForCursor.historical;
    result.visibleSeqno = itemsForCursor.visibleSeqno;

    if (itemsForCursor.ranges.empty()) {
        Expects(result.items.empty());
        log(spdlog::level::level_enum::debug,
            "{} ActiveStream::getOutstandingItems: no data from checkpoints"
            "- type:{}, {}, ranges:{}, HCS:{}, MVS:{}, items:{}",
            logPrefix,
            ::to_string(itemsForCursor.checkpointType),
            ::to_string(itemsForCursor.historical),
            itemsForCursor.ranges.size(),
            ::to_string_or_none(itemsForCursor.highCompletedSeqno),
            itemsForCursor.visibleSeqno,
            result.items.size());
        return result;
    }

    if (isDiskCheckpointType(result.checkpointType)) {
        // CM can never return multiple disk checkpoints or checkpoints of
        // different types. So if Disk, then one range.

        if (itemsForCursor.ranges.size() != 1) {
            const auto msg = fmt::format(
                    "ActiveStream::getOutstandingItems: stream:{} {} "
                    "processing checkpoint type:{}, {}, ranges:{}, HCS:{}, "
                    "MVS:{}, items:{}",
                    name_,
                    vb_,
                    ::to_string(itemsForCursor.checkpointType),
                    ::to_string(itemsForCursor.historical),
                    itemsForCursor.ranges.size(),
                    ::to_string_or_none(itemsForCursor.highCompletedSeqno),
                    itemsForCursor.visibleSeqno,
                    result.items.size());
            cb::throwWithTrace(std::logic_error(msg));
        }

        const auto& range = itemsForCursor.ranges.front();
        if (!itemsForCursor.highCompletedSeqno) {
            const auto msg = fmt::format(
                    "ActiveStream::getOutstandingItems: stream:{} {} processing"
                    " checkpoint type:{}, {}, snapStart:{}, snapEnd:{} -"
                    " missing HCS",
                    name_,
                    vb_,
                    ::to_string(itemsForCursor.checkpointType),
                    ::to_string(itemsForCursor.historical),
                    range.getStart(),
                    range.getEnd());
            cb::throwWithTrace(std::logic_error(msg));
        }

        result.diskCheckpointState =
                OutstandingItemsResult::DiskCheckpointState();
        result.diskCheckpointState->highCompletedSeqno =
                *itemsForCursor.highCompletedSeqno;
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
                if (!cb::mcbp::datatype::is_snappy(item->getDataType())) {
                    return true;
                }
            }
        } else {
            if (cb::mcbp::datatype::is_snappy(item->getDataType())) {
                return true;
            }
        }

        /**
         * If the value doesn't have to be compressed, then
         * check if xattrs need to be pruned. If not, then
         * value needs no modification
         */
        if (cb::mcbp::datatype::is_xattr(item->getDataType())) {
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

                        if (!cb::mcbp::datatype::is_snappy(
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
                if (cb::mcbp::datatype::is_snappy(finalItem->getDataType()) &&
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

void ActiveStream::processItemsInner(
        const std::lock_guard<std::mutex>& lg,
        OutstandingItemsResult& outstandingItemsResult) {
    if (outstandingItemsResult.items.empty()) {
        return;
    }

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
    uint64_t newLastReadSeqno = 0;

    // Record each seqno of all items which *could* replicate (they may not
    // due to configuration). This value is needed for SeqnoAdvance trigger.
    // initialised as 0 so nothing happens if no replicate-able items are seen.
    uint64_t lastReplicateableSeqno = 0;

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
                snapshot(outstandingItemsResult,
                         mutations,
                         visibleSeqno,
                         highNonVisibleSeqno,
                         newLastReadSeqno);
                /* clear out all the mutations since they are already put
                   onto the readyQ */
                mutations.clear();
                highNonVisibleSeqno = std::nullopt;
            }

            if (outstandingItemsResult.ranges.empty()) {
                cb::throwWithTrace(
                        std::logic_error("ActiveStream::processItems: found "
                                         "no snapshot ranges but we have a "
                                         "checkpoint start with seqno:" +
                                         std::to_string(qi->getBySeqno())));
            }

            /* mark true as it indicates a new checkpoint snapshot */
            nextSnapshotIsCheckpoint = true;
            /* and record the checkpoint's snapshot start, as we use it to
             * override the snapshot_marker start seqno to guard against things
             * like MB-50543 */
            nextSnapStart = outstandingItemsResult.ranges.begin()->getStart();

            continue;
        }

        if (!qi->isCheckPointMetaItem()) {
            // Set curChkSeqno to the seqno we have visited and record the
            // seqno of this item for use in seqno-advance decision
            curChkSeqno = lastReplicateableSeqno = qi->getBySeqno();
        }

        if (shouldProcessItem(*qi)) {
            newLastReadSeqno = qi->getBySeqno();

            // Check if the item is allowed on the stream, note the filter
            // updates itself for collection deletion events
            if (filter.checkAndUpdate(*qi)) {
                if (qi->isVisible()) {
                    visibleSeqno = qi->getBySeqno();
                }
                mutations.push_back(makeResponseFromItem(
                        qi, SendCommitSyncWriteAs::Commit));
            }

        } else if (isSeqnoAdvancedEnabled() && !qi->isCheckPointMetaItem() &&
                   filter.checkAndUpdate(*qi)) {
            // Can replace with SeqnoAdvance and the item is for this stream
            highNonVisibleSeqno = qi->getBySeqno();
        }
    }

    if (!mutations.empty()) {
        // We have a snapshot with mutations to send. Push it into the ready
        // queue, all done then.
        snapshot(outstandingItemsResult,
                 mutations,
                 visibleSeqno,
                 highNonVisibleSeqno,
                 newLastReadSeqno);
        return;
    }

    // No mutations to send but we might need to send a SeqnoAdvanced
    if (!isSeqnoAdvancedEnabled()) {
        return;
    }
    // Note that we cannot enter this case if supportSyncReplication()
    // returns true (see isSeqnoAdvancedEnabled). This means that we
    // do not need to set the HCS/MVS or timestamp parameters of the
    // snapshot marker. MB-47877 tracks enabling sync-writes+filtering
    //
    // MB-65581 introduces the (curChkSeqno >= snap_end_seqno_) condition.
    // Meaning: We must not send any SnapMarker+SeqnoAdvance in the case where a
    // stream (that got a partial snapshot) reconnects and registers it cursor
    // in a checkpoint that overlaps with the previous partial snapshot (ie
    // curChkSeqno < snap_end_seqno_ by logic).
    // If we do send that SnapMarker+SeqnoAdvance in that case, then we might
    // prematurely update lastReadSeqno. Then a subsequent ActiveStreamProcessor
    // run might try to reset lastReadSeqno to the same (or lower) value, thus
    // breaking lastReadSeqno's monotonicity invariant.
    // So to address MB-65581:
    //  1. We skip this block (ie the current ActiveStreamProcessor run doesn't
    //     push any data over the stream). By that we avoid a premature update
    //     on lastReadSeqno;
    //  2. Then, a subsequent ActiveStreamProcessor run will move the stream
    //     by processing remaining checkpoint items. That will set lastReadSeqno
    //     successfully.
    // The covered scenario is quite complex; details in MB-65581 and in the
    // unit test included with the fix.
    // MB-66612 switches from curChkSeqno to lastReplicateableSeqno as there are
    // cases when curChkSeqno represents the next seqno (not assigned to any
    // item yet). lastReplicateableSeqno will mean we enter this code block only
    // when a non-meta item has been processed.
    if (!firstMarkerSent && lastReadSeqno < snap_end_seqno_ &&
        lastReplicateableSeqno >= snap_end_seqno_) {
        // MB-47009: This first snapshot has been completely filtered
        // away. The remaining items must not of been for this client.
        // We must still send a snapshot marker so that the client is
        // moved to their end seqno - so a snapshot + seqno advance is
        // needed.
        sendSnapshotAndSeqnoAdvanced(
                outstandingItemsResult, start_seqno_, lastReplicateableSeqno);
        firstMarkerSent = true;
    } else if (isSeqnoGapAtEndOfSnapshot(lastReplicateableSeqno)) {
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
        sendSnapshotAndSeqnoAdvanced(outstandingItemsResult,
                                     highNonVisibleSeqno.value(),
                                     highNonVisibleSeqno.value());
    } else if (lastReadSeqno < curChkSeqno) {
        // So here we are in the case where:
        // - We have moved the DCP cursor and pulled some items
        // - There was some non-meta items (as curChkSeqno has been
        //   bumped)
        // - We have filtered out some item (lastReadSeqno not aligned
        //   to curChkSeqno)
        // - Actually we have filtered all the items and we have skipped
        //   the call to snapshot().
        //
        // We need to bump lastReadSeqno.
        // The local newLastReadSeqno variable is updated with all
        // seqnos that belong the stream, regardless of whether they are
        // filtered out by the stream filter. That's the quantity that
        // we normally use in the snapshot() golden-path for updating
        // AS::lastReadSeqno. Used here with the same semantic.
        if (lastReadSeqno < newLastReadSeqno) {
            lastReadSeqno = newLastReadSeqno;
        }
    }
}

void ActiveStream::processItems(
        const std::lock_guard<std::mutex>& lg,
        OutstandingItemsResult& outstandingItemsResult) {
    processItemsHook();
    processItemsInner(lg, outstandingItemsResult);

    // If we've processed past the stream's end seqno then transition to the
    // stream to the dead state and add a stream end to the ready queue
    if (curChkSeqno >= getEndSeqno()) {
        endStream(cb::mcbp::DcpStreamEndStatus::Ok);
    }

    // After the snapshot has been processed, check if the filter is now
    // empty. A stream with an empty filter does nothing but self close.
    if (filter.empty()) {
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

void ActiveStream::snapshot(const OutstandingItemsResult& meta,
                            std::deque<std::unique_ptr<DcpResponse>>& items,
                            uint64_t maxVisibleSeqno,
                            std::optional<uint64_t> highNonVisibleSeqno,
                            uint64_t newLastReadSeqno) {
    if (items.empty()) {
        return;
    }

    lastReadSeqno.store(newLastReadSeqno);

    // Note: ActiveStream is in a complete snapshot
    // - Always, on active vbuckets
    // - If we have sent up to the last seqno in the last marker range, for
    //   non-active vbuckets
    //
    // * Update on the above*
    // At the time of writing I have introduced the
    // MemorySnapshotFromPartialReplica DCP test. Test stresses the behaviour
    // here for replica vbuckets. The test proves that the condition that we
    // enforce here isn't enough for ensuring that replica vbuckets stream
    // consistent snapshots to the peer. See that test for details.
    // @todo: MB-59288
    const auto isReplicaSnapshotComplete =
            lastSentSnapEndSeqno.load(std::memory_order_relaxed) <
            lastReadSeqno;

    // Note: Here we consider "!replica" rather than "active", but I believe
    // that streaming from "pending|dead" is just illegal, so we should change
    // this.

    const auto vb = engine->getVBucket(vb_);
    if (vb && (vb->getState() != vbucket_state_replica ||
               isReplicaSnapshotComplete)) {
        // Get OptionalSeqnos which for the items list types should have values
        auto seqnoStart = items.front()->getBySeqno();
        auto seqnoEnd = items.back()->getBySeqno();
        if (!seqnoStart || !seqnoEnd) {
            cb::throwWithTrace(std::logic_error(
                    logPrefix +
                    "ActiveStream::snapshot incorrect DcpEvent, missing a "
                    "seqno " +
                    std::string(items.front()->to_string()) + " " +
                    std::string(items.back()->to_string()) + " " + logPrefix));
        }

        uint64_t snapStart = *seqnoStart;
        uint64_t snapEnd = *seqnoEnd;

        // MB-50333: Pin the snapshot start seqno to the start seqno of the
        // checkpoint that the items we are processing belong to.
        // MB-50543: Since AS::snapshot() can pick up items from a previous
        // checkpoint, starting after the start seqno, and also items from the
        // next checkpoint in one go, we need to check whether we've already
        // sent the snapshot marker for the previous checkpoint, which would end
        // at a higher seqno.
        if (nextSnapshotIsCheckpoint && nextSnapStart > lastSentSnapEndSeqno) {
            // MB-59759: We should consider the requested snap_start_seqno_, as
            // when a stream starts in the middle of a checkpoint and all items
            // up to that point have been deduped or expelled, the checkpoint
            // start seqno can be before the snapshot at the consumer, causing
            // failures in the consumer (see monotonics in
            // PassiveStream::reconnectStream).
            snapStart = std::max(snap_start_seqno_, nextSnapStart);
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

        auto flags = getMarkerFlags(meta);
        if (isTakeoverSend()) {
            waitForSnapshot++;
            flags |= MARKER_FLAG_ACK;
        }

        // If the stream supports SyncRep then send the HCS for CktpType::disk
        const auto sendHCS = supportSyncReplication() &&
                             isDiskCheckpointType(meta.checkpointType);
        std::optional<uint64_t> hcsToSend;
        if (sendHCS) {
            Expects(meta.diskCheckpointState);
            hcsToSend = meta.diskCheckpointState->highCompletedSeqno;
            log(spdlog::level::level_enum::info,
                "{} ActiveStream::snapshot: Sending disk snapshot with start "
                "seqno {}, end seqno {}, and"
                " high completed seqno {}",
                logPrefix,
                snapStart,
                snapEnd,
                *hcsToSend);
        }

        /* We need to send the requested 'snap_start_seqno_' as the snapshot
           start when we are sending the first snapshot because the first
           snapshot could be resumption of a previous snapshot */
        const bool wasFirst = !firstMarkerSent;
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
        // Update the last start seqno seen but handle base case as
        // lastSentSnapStartSeqno is initial zero
        if (snapStart > 0) {
            lastSentSnapStartSeqno = snapStart;
        }

        // We only consider failing here if the vBucket state is active. If this
        // is a replica vBucket (and as such this must be a view stream) then it
        // is possible for us to send a snap start < previous snap end as we
        // attempt to merge disk and memory snapshots.
        if (!wasFirst && lastSentSnapStartSeqno <= lastSentSnapEndSeqno &&
            engine->getVBucket(vb_)->getState() == vbucket_state_active) {
            auto msg = fmt::format(
                    "ActiveStream::snapshot: sent "
                    "snapshot marker to client with snap start <= previous "
                    "snap end "
                    "{} "
                    "lastSentSnapStart:{} "
                    "lastSentSnapEnd:{} "
                    "snapStart:{} "
                    "snapEnd:{} "
                    "flags:{} "
                    "sid:{} "
                    "producer name:{} "
                    "lastReadSeqno:{} "
                    "curChkSeqno:{}",
                    vb_,
                    lastSentSnapStartSeqno.load(),
                    lastSentSnapEndSeqno.load(),
                    snapStart,
                    snapEnd,
                    flags,
                    sid,
                    getName(),
                    lastReadSeqno.load(),
                    curChkSeqno.load());
            cb::throwWithTrace(std::logic_error(msg));
        }
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
        if (status == cb::mcbp::DcpStreamEndStatus::BackfillFail) {
            // This case is called from the IO task doing the backfill - it has
            // failed and the DCPBackfill object will be removed by the task
            backfillUID = 0;
        }
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

        // Reset the vbucket takeover state
        if (isTakeoverStream()) {
            auto vb = engine->getVBucket(vb_);
            if (vb) {
                vb->setTakeoverBackedUpState(false);
            }
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
            readyQ.size(),
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

        scheduleBackfillRegisterCursorHook();

        curChkSeqno = registerResult.nextSeqno;
        tryBackfill = registerResult.tryBackfill;
        cursor = registerResult.takeCursor();

        log(spdlog::level::level_enum::info,
            "{} ActiveStream::scheduleBackfill_UNLOCKED register cursor with "
            "name \"{}\" lastProcessedSeqno:{}, result{{tryBackfill:{}, op:{}, "
            "seqno:{}, nextSeqno:{}}}",
            logPrefix,
            name_,
            lastReadSeqno.load(),
            registerResult.tryBackfill,
            ::to_string(registerResult.position->getOperation()),
            registerResult.position->getBySeqno(),
            registerResult.nextSeqno);

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

    numBackfillPauses = 0;

    if (tryBackfill && tryAndScheduleOSOBackfill(producer, *vbucket)) {
        return;
    } else if (tryBackfill && (backfillUID = producer.scheduleBackfillManager(
                                       *vbucket,
                                       shared_from_this(),
                                       backfillStart,
                                       backfillEnd))) {
        // Expect a non zero UID. 0 is reserved for "no backfill to remove"
        Expects(backfillUID);
        // backfill will be needed to catch up to the items in the
        // CheckpointManager
        log(spdlog::level::level_enum::info,
            "{} Scheduling backfill "
            "from {} to {}, uid:{}, reschedule "
            "flag : {}",
            logPrefix,
            backfillStart,
            backfillEnd,
            backfillUID,
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
    // OSO only _allowed_ (but may not be chosen):
    // if the client has enabled OSO
    // if the size of the collection filter fits the current configured max
    // if this is the initial backfill request (diskonly or not)
    const auto& config = engine->getConfiguration();

    if (producer.isOutOfOrderSnapshotsEnabled() &&
        filter.isOsoSuitable(config.getDcpOsoMaxCollectionsPerBackfill()) &&
        lastReadSeqno.load() == 0 &&
        ((curChkSeqno.load() > lastReadSeqno.load() + 1) || (isDiskOnly()))) {
        // however OSO is only _used_ if:
        // - dcp_oso_backfill is set to enabled,
        // - dcp_oso_backfill is set to "auto", and OSO is predicted to be
        //   faster for this backfill.
        const auto osoBackfill = config.getDcpOsoBackfill();
        if (osoBackfill == "disabled") {
            return false;
        }
        if (osoBackfill == "auto") {
            auto [colItemCount, colDiskSize] =
                    filter.getSizeStats(vb.getManifest());

            const auto vbItemCount = vb.getNumItems();
            if (!isOSOPreferredForCollectionBackfill(
                        config, colItemCount, colDiskSize, vbItemCount)) {
                log(spdlog::level::level_enum::info,
                    "{} Skipping OSO backfill of size:{} total collection item "
                    "count ({}) is too large a percentage of the vBucket item "
                    "count ({}) - ({:.2f}%)",
                    logPrefix,
                    filter.size(),
                    colItemCount,
                    vbItemCount,
                    (float(colItemCount) * 100) / vbItemCount);
                return false;
            }
        }

        // OSO possible - engage.
        backfillUID = producer.scheduleBackfillManager(vb, shared_from_this());

        // Expect a non zero UID. 0 is reserved to mean "no backfill to remove"
        Expects(backfillUID);

        // backfill will be needed to catch up to the items in the
        // CheckpointManager
        log(spdlog::level::level_enum::info,
            "{} Scheduling OSO backfill "
            "for size:{} diskOnly:{} lastReadSeqno:{} curChkSeqno:{}",
            logPrefix,
            filter.size(),
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
        uint64_t maxScanSeqno,
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
            const auto vb = engine->getVBucket(vb_);
            if (!vb) {
                log(spdlog::level::level_enum::warn,
                    "{} ActiveStream::completeBackfillInner(): Vbucket "
                    "does not exist",
                    logPrefix);
                return;
            }

            // In-order backfills may require a seqno-advanced message if
            // there is a stream filter present (e.g. only streaming a single
            // collection).
            if (isSeqnoAdvancedEnabled() &&
                isSeqnoGapAtEndOfSnapshot(maxScanSeqno)) {
                // Active:  We must send a SeqnoAdvanced to bump the DCP
                //          client's seqno to snap-end.
                // Replica: Vbucket may transition backfill->memory without
                //          sending another snapshot. Thus, in this case we
                //          do not want to send a SeqnoAdvanced at the end
                //          of backfill. So check that we don't have an in
                //          memory range to stream from.
                const auto replicaVucketSeqnoAdvance =
                        maxScanSeqno > lastBackfilledSeqno &&
                        maxScanSeqno == lastSentSnapEndSeqno;
                if (vb->getState() != vbucket_state_replica ||
                    replicaVucketSeqnoAdvance) {
                    queueSeqnoAdvanced();
                }
            }

            // Client does not support collections, so we cannot send a
            // seqno-advanced message to tell them that the last streamed seqno
            // is below the snap_end. However, we should still move the
            // lastReadSeqno as we do when we send a SeqnoAdvanced, as
            // otherwise, if there is a backfill pending, we'd start that
            // backfill from the last seqno we sent, no the last seqno we read.
            // In this case, if the purgeSeqno has advanced past the
            // lastReadSeqno, we'd fail with reason=Rollback, and could get in a
            // perpetual cycle of backfill from zero, reschedule, fail
            // (MB-56084).
            if (!isCollectionEnabledStream() && maxScanSeqno > lastReadSeqno) {
                lastReadSeqno.store(maxScanSeqno);
            }
        }

        if (isBackfilling()) {
            const auto diskItemsRead = backfillItems.disk.load();
            const auto runtimeSecs =
                    std::chrono::duration<double>(runtime).count();
            log(spdlog::level::level_enum::info,
                "{} {}Backfill complete. {} items consisting of {} bytes read "
                "from disk, "
                "{} items from memory, lastReadSeqno:{}, "
                "lastSentSeqnoAdvance:{}, lastSentSnapStartSeqno:{}, "
                "lastSentSnapEndSeqno:{}, pendingBackfill:{}, "
                "numBackfillPauses:{}. Total "
                "runtime {} "
                "({:.0f} item/s, {:.0f} MB/s)",
                logPrefix,
                backfillType == BackfillType::OutOfSequenceOrder ? "OSO " : "",
                diskItemsRead,
                diskBytesRead,
                backfillItems.memory.load(),
                lastReadSeqno.load(),
                lastSentSeqnoAdvance.load(),
                lastSentSnapStartSeqno.load(),
                lastSentSnapEndSeqno.load(),
                pendingBackfill ? "True" : "False",
                numBackfillPauses.load(),
                cb::time2text(runtime),
                diskItemsRead ? (diskItemsRead / runtimeSecs) : 0.0,
                diskBytesRead ? ((diskBytesRead / runtimeSecs) / (1024 * 1024))
                              : 0.0);
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
                    maxScanSeqno != lastBackfilledSeqno) {
                pushToReadyQ(std::make_unique<SeqnoAdvanced>(
                        opaque_, vb_, sid, maxScanSeqno));
                lastSentSeqnoAdvance.store(maxScanSeqno);
            }

            // Now that the OSO backfill has ended, we can tweak
            // lastReadSeqno so that it reflects the end of the snapshot
            // we've just processed. This ensures any pending backfill which
            // follows continues from maxSeqno and not the max seqno of the
            // collection(s) in the OSO scan, which could be way less.
            if (maxScanSeqno > lastReadSeqno) {
                lastReadSeqno = maxScanSeqno;
            }

            pushToReadyQ(std::make_unique<OSOSnapshot>(
                    opaque_, vb_, sid, OSOSnapshot::End{}));
        }

        // Set back to 0 so we disable explicit clean-up.
        backfillUID = 0;
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
                "{} ActiveStream::notifyEmptyBackfill: "
                "Re-registering dropped cursor with name \"{}\", "
                "lastSeenSeqno:{}, backfill:{}, nextSeqno:{}",
                logPrefix,
                name_,
                lastSeenSeqno,
                result.tryBackfill,
                result.nextSeqno);
            curChkSeqno = result.nextSeqno;
            cursor = result.takeCursor();
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
        "curChkSeqno: {}, "
        "isBackfillTaskRunning : {}",
        logPrefix,
        to_string(state_.load()).c_str(),
        lastReadSeqno.load(),
        lastSentSeqno.load(),
        engine->getVBucket(vb_)->getHighSeqno(),
        curChkSeqno.load(),
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

void ActiveStream::handleDcpProducerException(const std::exception& exception) {
    auto callstack = nlohmann::json::array();
    if (const auto* backtrace = cb::getBacktrace(exception)) {
        print_backtrace_frames(*backtrace, [&callstack](const char* frame) {
            callstack.emplace_back(frame);
        });
        log(spdlog::level::debug,
            "{} ActiveStream::handleDcpProducerException: Caught exception "
            "last_sent_seqno: {} error: {}, backtrace: {}",
            logPrefix,
            getLastSentSeqno(),
            exception.what(),
            callstack.dump());
    } else {
        log(spdlog::level::debug,
            "{} ActiveStream::handleDcpProducerException: Caught exception "
            "last_sent_seqno: {} error: {}",
            logPrefix,
            getLastSentSeqno(),
            exception.what());
    }

    if (engine->getConfiguration().isDcpProducerCatchExceptions()) {
        setDead(cb::mcbp::DcpStreamEndStatus::Disconnected);
        // Disconnect the connection
        auto producer = producerPtr.lock();
        if (producer) {
            producer->flagDisconnect();
            // Notify producer to close front-end connection and
            // remaining streams.
            producer->scheduleNotify();
        }
    } else {
        throw;
    }
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
    // (b) Items pending in our readyQ
    size_t ckptItems = 0;
    if (auto sp = cursor.lock()) {
        ckptItems = vbucket->checkpointManager->getNumItemsForCursor(*sp);
    }

    // Note: concurrent access to readyQ guarded by streamMutex
    std::lock_guard<std::mutex> lh(streamMutex);
    return ckptItems + readyQ.size();
}

size_t ActiveStream::getBackfillItemsDisk() const {
    return backfillItems.disk;
}

size_t ActiveStream::getBackfillItemsMemory() const {
    return backfillItems.memory;
}

uint64_t ActiveStream::getLastReadSeqno() const {
    return lastReadSeqno.load();
}

uint64_t ActiveStream::getLastSentSeqno() const {
    return lastSentSeqno.load();
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
        removeCursorPreLockHook();
        auto lockedCursor = cursor.lock();
        if (lockedCursor &&
            vb->checkpointManager->removeCursor(*lockedCursor)) {
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

void ActiveStream::incrementNumBackfillPauses() {
    numBackfillPauses++;
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

bool ActiveStream::isIgnoringPurgedTombstones() const {
    return flags_ & DCP_ADD_STREAM_FLAG_IGNORE_PURGED_TOMBSTONES;
}

bool ActiveStream::isSeqnoAdvancedEnabled() const {
    // SeqnoAdvance can only be sent if collections enabled as that's what added
    // the message.
    // Then we only require SeqnoAdvance for streams which don't enable:
    // sync-writes - so we can replace abort/prepare with seqno-advance
    // FlatBuffers - so we can replace ModifyCollection with seqno-advance
    return isCollectionEnabledStream() &&
           (syncReplication == SyncReplication::No ||
            !flatBuffersSystemEventsEnabled);
}

bool ActiveStream::isSeqnoGapAtEndOfSnapshot(uint64_t streamSeqno) const {
    return (lastSentSnapEndSeqno.load() > lastReadSeqno.load()) &&
           lastSentSnapEndSeqno.load() == streamSeqno;
}

void ActiveStream::sendSnapshotAndSeqnoAdvanced(
        const OutstandingItemsResult& meta, uint64_t start, uint64_t end) {
    const bool wasFirst = !firstMarkerSent;

    start = adjustStartIfFirstSnapshot(start, true);
    const auto flags = getMarkerFlags(meta);
    pushToReadyQ(std::make_unique<SnapshotMarker>(opaque_,
                                                  vb_,
                                                  start,
                                                  end,
                                                  flags,
                                                  std::nullopt,
                                                  std::nullopt,
                                                  std::nullopt,
                                                  sid));
    // Update the last start seqno seen but handle base case as
    // lastSentSnapStartSeqno is initial zero
    if (start > 0) {
        lastSentSnapStartSeqno = start;
    }

    // Only compare last sent start with last sent end if end has already
    // been set
    if (!wasFirst && lastSentSnapStartSeqno <= lastSentSnapEndSeqno) {
        auto msg = fmt::format(
                "ActiveStream::sendSnapshotAndSeqnoAdvanced: sent snapshot "
                "marker to client with snap start <= previous snap end "
                "{} "
                "lastSentSnapStart:{} "
                "lastSentSnapEnd:{} "
                "snapStart:{} "
                "snapEnd:{} "
                "flags:{} "
                "sid:{} "
                "producer name:{} "
                "lastReadSeqno:{} "
                "curChkSeqno:{}",
                vb_,
                lastSentSnapStartSeqno.load(),
                lastSentSnapEndSeqno.load(),
                start,
                end,
                flags,
                sid,
                getName(),
                lastReadSeqno.load(),
                curChkSeqno.load());
        cb::throwWithTrace(std::logic_error(msg));
    }

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

uint32_t ActiveStream::getMarkerFlags(
        const OutstandingItemsResult& meta) const {
    uint32_t flags = isDiskCheckpointType(meta.checkpointType)
                             ? MARKER_FLAG_DISK
                             : MARKER_FLAG_MEMORY;

    if (changeStreamsEnabled &&
        (meta.historical == CheckpointHistorical::Yes)) {
        flags |= MARKER_FLAG_HISTORY;
    }

    if (nextSnapshotIsCheckpoint) {
        flags |= MARKER_FLAG_CHK;
    }

    return flags;
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

std::string ActiveStream::Labeller::getLabel(const char* name) const {
    return fmt::format("ActiveStream({} {})::{}",
                       stream.getName(),
                       stream.getLogPrefix(),
                       name);
}

bool ActiveStream::areChangeStreamsEnabled() const {
    return changeStreamsEnabled;
}

bool ActiveStream::isFlatBuffersSystemEventEnabled() const {
    return flatBuffersSystemEventsEnabled;
}

bool ActiveStream::isOSOPreferredForCollectionBackfill(
        const Configuration& config,
        uint64_t collectionItems,
        uint64_t collectionDiskSize,
        uint64_t totalItems) {
    // Determine if OSO backfill or seqno backfill should be used for this
    // collection - which is expected to be faster?
    //
    // === Background ===
    //
    // In the abstract, OSO backfill _should_ always be faster than seqno as it
    // only has to scan the specific keys which are part of the collection,
    // whereas seqno backfill must scan _all_ items in the vbucket, only
    // fetching the values which belong to the requested collection.
    // However, in practice this isn't actually the case :-
    // - Couchstore arranges values in seqno order, so iterating in seqno
    //   order results in less random IO and more sequential IO.
    // - Magma places values next to their seqno, which means that iterating
    //   by seqno will find the value immediately next to the seqno without
    //   any extra IO, whereas iterating by key requires a random IO per
    //   fetched document.
    //
    // As such, performing a seqno scan requires less random IO and hence a
    // "brute force" approach of iterating over all seqnos but only sending
    // the ones which match the collection can be *much* faster.
    //
    // For example, performing backfills of a collection which contains 1.5%
    // of all 511M items from the Bucket, with a value size of 232B, using a
    // cloud EBS-style disk, the runtime of OSO & seqno backfill for Magma and
    // Couchstore is:
    //
    //    Magma OSO:        202 seconds
    //    Magma seqno:       63 seconds
    //    Couchstore OSO:   286 seconds
    //    Couchstore seqno: 185 seconds
    //
    // In other words, it's 3.2x (Magma) or ~1.5x (Couchstore) faster to use
    // a seqno scan instead of an OSO scan in this setup. Thedifference is
    // more significant the larger the collections are - speedups of over 20x
    // have been observed when the collection is the entire bucket.
    //
    // === Experimental Results ===
    //
    // Various experiments have been performed to measure backfill runtimes
    // under different configs (see MB-56346), and based on those, OSO vs seqno
    // ratio function can be estimated by a power function with a negative
    // coefficient:
    //
    //     oso/seqno ratio = Mx ^ E
    //
    //  where:
    //     'x' is the size of the collection as a fraction of the
    //         bucket (0, 1.0).
    //     'M' has been observed to range from 20 to 350.
    //     'E' has been observed to range from -0.9 to -0.6.
    //
    // The coefficient and exponent vary somewhat based on the value size,
    // storage engine (Couchstore vs Magma), disk type and concurrency being
    // driven - and as such the break-even point collection size has been
    // observed to range from ~0.2% to ~6% of the bucket items.
    //
    // Generating an accurate model (where we can determine the coefficient and
    // exponent to use for a given backfill) is difficult - those values appear
    // to depend on (at least):
    //
    // a) Properties of the data (collection size as a percentage of bucket
    //    items, collection item sizes).
    // b) Which storage engine is used.
    // c) Properties of the environment (disk sequential IO vs random IO,
    //    disk throughput at different IO sizes, ...).
    //
    // While (a) and (b) can be measured reasonably easily inside ep-engine,
    // (c) is much more difficult, can vary significantly, and has a very
    // big impact on the performance of each method. For example, the exact
    // same backfill setup above but performed on a local NVMe disk shows
    // the following runtimes:
    //
    //    Magma NVMe OSO:    87 seconds
    //    Magma NVMe seqno:  61 seconds
    //
    // i.e. seqno scan time is 1.03x faster than what it was with an EBS-style
    // disk, however OSO is 2.3x faster - which moves where the break-even point
    // between the two approaches is.
    //
    // === Chosen model ===
    //
    // Given the challenge in determining the "correct" coefficient & exponent
    // values all of the above, we use a pretty simple model whose primary
    // aim is to avoid any pathological behaviour :-
    //
    //   - If the average item size is "small", then use OSO if collection size
    //     is less than dcp_oso_backfill_small_value_ratio (e.g. 0.5% of
    //     bucket).
    //   - If the average item size is "large", then use OSO if collection
    //     size is less than dcp_oso_backfill_large_value_ratio (e.g. 4% of
    //     bucket).
    //   - The threshold for considering average item size of small vs large
    //     is determined by dcp_oso_backfill_small_vs_large_item_size_threshold.
    //
    // The rationale for this approach is based on the fact that the largest
    // collection size which OSO is beneficial (across all experiments) is only
    // 6% - i.e. even in the best case, OSO is only beneficial to a very small
    // range of collection sizes. Given we can only test so many environments,
    // it would be dangerous to try to "overfit" a power series to the limited
    // experimental evidence we have. Additionally, a simple model like the one
    // chosen is easy to understand and explain.

    // If the collection is empty then we should always use OSO - a key index
    // scan should be able to efficiently find "all" items in the collection.
    // This also avoids a potential div-by-zero case below.
    if (collectionItems == 0) {
        return true;
    }

    const auto collectionRatio = double(collectionItems) / totalItems;
    const auto meanCollectionItemSize = collectionDiskSize / collectionItems;
    const auto maxCollectionRatioForOso =
            meanCollectionItemSize <
                            config.getDcpOsoBackfillSmallItemSizeThreshold()
                    ? config.getDcpOsoBackfillSmallValueRatio()
                    : config.getDcpOsoBackfillLargeValueRatio();

    return collectionRatio < maxCollectionRatioForOso;
}

void ActiveStream::removeBackfill(BackfillManager& bfm) {
    uint64_t removeThis{0};
    {
        std::lock_guard<std::mutex> lh(streamMutex);
        removeThis = std::exchange(backfillUID, 0);
    }

    if (removeThis) {
        bfm.removeBackfill(removeThis);
    }
}
