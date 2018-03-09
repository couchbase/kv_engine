/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include "config.h"

#include "checkpoint.h"
#include "collections/vbucket_filter.h"
#include "dcp/backfill-manager.h"
#include "dcp/backfill.h"
#include "dcp/consumer.h"
#include "dcp/producer.h"
#include "dcp/response.h"
#include "dcp/stream.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "executorpool.h"
#include "failover-table.h"
#include "kv_bucket.h"
#include "kvstore.h"
#include "replicationthrottle.h"
#include "statwriter.h"

#include <platform/checked_snprintf.h>

#include <memory>


const char* to_string(Stream::Snapshot type) {
    switch (type) {
    case Stream::Snapshot::None:
        return "none";
    case Stream::Snapshot::Disk:
        return "disk";
    case Stream::Snapshot::Memory:
        return "memory";
    }
    throw std::logic_error("to_string(Stream::Snapshot): called with invalid "
            "Snapshot type:" + std::to_string(int(type)));
}

const std::string to_string(Stream::Type type) {
    switch (type) {
    case Stream::Type::Active:
        return "Active";
    case Stream::Type::Notifier:
        return "Notifier";
    case Stream::Type::Passive:
        return "Passive";
    }
    throw std::logic_error("to_string(Stream::Type): called with invalid "
            "type:" + std::to_string(int(type)));
}

const uint64_t Stream::dcpMaxSeqno = std::numeric_limits<uint64_t>::max();

Stream::Stream(const std::string &name, uint32_t flags, uint32_t opaque,
               uint16_t vb, uint64_t start_seqno, uint64_t end_seqno,
               uint64_t vb_uuid, uint64_t snap_start_seqno,
               uint64_t snap_end_seqno, Type type)
    : name_(name),
      flags_(flags),
      opaque_(opaque),
      vb_(vb),
      start_seqno_(start_seqno),
      end_seqno_(end_seqno),
      vb_uuid_(vb_uuid),
      snap_start_seqno_(snap_start_seqno),
      snap_end_seqno_(snap_end_seqno),
      state_(StreamState::Pending),
      type_(type),
      itemsReady(false),
      readyQ_non_meta_items(0),
      readyQueueMemory(0) {
}

Stream::~Stream() {
    // NB: reusing the "unlocked" method without a lock because we're
    // destructing and should not take any locks.
    clear_UNLOCKED();
}

const std::string Stream::to_string(Stream::StreamState st) {
    switch(st) {
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
    case StreamState::Reading:
        return "reading";
    case StreamState::Dead:
        return "dead";
    }
    throw std::invalid_argument(
        "Stream::to_string(StreamState): " + std::to_string(int(st)));
}

bool Stream::isTypeActive() const {
    return type_ == Type::Active;
}

bool Stream::isActive() const {
    return state_.load() != StreamState::Dead;
}

bool Stream::isBackfilling() const {
    return state_.load() == StreamState::Backfilling;
}

bool Stream::isInMemory() const {
    return state_.load() == StreamState::InMemory;
}

bool Stream::isPending() const {
    return state_.load() == StreamState::Pending;
}

bool Stream::isTakeoverSend() const {
    return state_.load() == StreamState::TakeoverSend;
}

bool Stream::isTakeoverWait() const {
    return state_.load() == StreamState::TakeoverWait;
}

void Stream::clear_UNLOCKED() {
    while (!readyQ.empty()) {
        popFromReadyQ();
    }
}

void Stream::pushToReadyQ(std::unique_ptr<DcpResponse> resp) {
    /* expect streamMutex.ownsLock() == true */
    if (resp) {
        if (!resp->isMetaEvent()) {
            readyQ_non_meta_items++;
        }
        readyQueueMemory.fetch_add(resp->getMessageSize(),
                                   std::memory_order_relaxed);
        readyQ.push(std::move(resp));
    }
}

std::unique_ptr<DcpResponse> Stream::popFromReadyQ(void) {
    /* expect streamMutex.ownsLock() == true */
    if (!readyQ.empty()) {
        auto front = std::move(readyQ.front());
        readyQ.pop();

        if (!front->isMetaEvent()) {
            readyQ_non_meta_items--;
        }
        const uint32_t respSize = front->getMessageSize();

        /* Decrement the readyQ size */
        if (respSize <= readyQueueMemory.load(std::memory_order_relaxed)) {
            readyQueueMemory.fetch_sub(respSize, std::memory_order_relaxed);
        } else {
            LOG(EXTENSION_LOG_DEBUG, "readyQ size for stream %s (vb %d)"
                "underflow, likely wrong stat calculation! curr size: %" PRIu64
                "; new size: %d",
                name_.c_str(), getVBucket(),
                readyQueueMemory.load(std::memory_order_relaxed), respSize);
            readyQueueMemory.store(0, std::memory_order_relaxed);
        }

        return front;
    }

    return nullptr;
}

uint64_t Stream::getReadyQueueMemory() {
    return readyQueueMemory.load(std::memory_order_relaxed);
}

void Stream::addStats(ADD_STAT add_stat, const void *c) {
    try {
        const int bsize = 1024;
        char buffer[bsize];
        checked_snprintf(buffer, bsize, "%s:stream_%d_flags", name_.c_str(),
                         vb_);
        add_casted_stat(buffer, flags_, add_stat, c);
        checked_snprintf(buffer, bsize, "%s:stream_%d_opaque", name_.c_str(),
                         vb_);
        add_casted_stat(buffer, opaque_, add_stat, c);
        checked_snprintf(buffer, bsize, "%s:stream_%d_start_seqno",
                         name_.c_str(), vb_);
        add_casted_stat(buffer, start_seqno_, add_stat, c);
        checked_snprintf(buffer, bsize, "%s:stream_%d_end_seqno", name_.c_str(),
                         vb_);
        add_casted_stat(buffer, end_seqno_, add_stat, c);
        checked_snprintf(buffer, bsize, "%s:stream_%d_vb_uuid", name_.c_str(),
                         vb_);
        add_casted_stat(buffer, vb_uuid_, add_stat, c);
        checked_snprintf(buffer, bsize, "%s:stream_%d_snap_start_seqno",
                         name_.c_str(), vb_);
        add_casted_stat(buffer, snap_start_seqno_, add_stat, c);
        checked_snprintf(buffer, bsize, "%s:stream_%d_snap_end_seqno",
                         name_.c_str(), vb_);
        add_casted_stat(buffer, snap_end_seqno_, add_stat, c);
        checked_snprintf(buffer, bsize, "%s:stream_%d_state", name_.c_str(),
                         vb_);
        add_casted_stat(buffer, to_string(state_.load()), add_stat, c);
    } catch (std::exception& error) {
        LOG(EXTENSION_LOG_WARNING,
            "Stream::addStats: Failed to build stats: %s", error.what());
    }
}

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
            EXTENSION_LOG_NOTICE,
            "(vb %" PRIu16 ") Creating %sstream with start seqno %" PRIu64
            " and end seqno %" PRIu64 "; requested end seqno was %" PRIu64,
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

    // Finally obtain a copy of the current separator
    currentSeparator = vbucket.getManifest().lock().getSeparator();
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
        if (result.first > nextRequiredSeqno && result.second) {
            pendingBackfill = true;
        }
        curChkSeqno = result.first;
    } catch(std::exception& error) {
        log(EXTENSION_LOG_WARNING,
            "(vb %" PRIu16 ") Failed to register cursor: %s",
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
            log(EXTENSION_LOG_WARNING,
                "(vb %" PRIu16
                ") ActiveStream::"
                "markDiskSnapshot: Unexpected state_:%s",
                vb_,
                to_string(state_.load()).c_str());
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
            log(EXTENSION_LOG_WARNING,
                "(vb %" PRIu16
                ") "
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
                log(EXTENSION_LOG_NOTICE,
                    "(vb %" PRIu16
                    ") Merging backfill and memory snapshot for a "
                    "replica vbucket, backfill start seqno %" PRIu64
                    ", "
                    "backfill end seqno %" PRIu64
                    ", "
                    "snapshot end seqno after merge %" PRIu64,
                    vb_,
                    startSeqno,
                    endSeqno,
                    info.range.end);
                endSeqno = info.range.end;
            }
        }

        log(EXTENSION_LOG_NOTICE,
            "(vb %" PRIu16 ") Sending disk snapshot with start seqno %" PRIu64
            " and end seqno %" PRIu64,
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
            if (!producer ||
                !producer->recordBackfillManagerBytesRead(
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
            log(EXTENSION_LOG_NOTICE,
                "(vb %" PRIu16 ") Backfill complete, %" PRIu64
                " items "
                "read from disk, %" PRIu64
                " from memory, last seqno read: "
                "%" PRIu64 ", pendingBackfill : %s",
                vb_,
                uint64_t(backfillItems.disk.load()),
                uint64_t(backfillItems.memory.load()),
                lastReadSeqno.load(),
                pendingBackfill ? "True" : "False");
        } else {
            log(EXTENSION_LOG_WARNING,
                "(vb %" PRIu16
                ") ActiveStream::completeBackfill: "
                "Unexpected state_:%s",
                vb_,
                to_string(state_.load()).c_str());
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
        log(EXTENSION_LOG_WARNING,
            "(vb %" PRIu16
            ") not present during ack for set "
            "vbucket during takeover",
            vb_);
        return;
    }

    {
        /* Order in which the below 3 locks are acquired is important to avoid
           any potential lock inversion problems */
        std::unique_lock<std::mutex> epVbSetLh(engine->getKVBucket()->getVbSetMutexLock());
        WriterLockHolder vbStateLh(vbucket->getStateLock());
        std::unique_lock<std::mutex> lh(streamMutex);
        if (isTakeoverWait()) {
            if (takeoverState == vbucket_state_pending) {
                log(EXTENSION_LOG_INFO,
                    "(vb %" PRIu16
                    ") Receive ack for set vbucket state to "
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

                log(EXTENSION_LOG_NOTICE,
                    "(vb %" PRIu16
                    ") Vbucket marked as dead, last sent "
                    "seqno: %" PRIu64 ", high seqno: %" PRIu64,
                    vb_,
                    lastSentSeqno.load(),
                    vbucket->getHighSeqno());
            } else {
                log(EXTENSION_LOG_NOTICE,
                    "(vb %" PRIu16
                    ") Receive ack for set vbucket state to "
                    "active message",
                    vb_);
                endStream(END_STREAM_OK);
            }
        } else {
            log(EXTENSION_LOG_WARNING,
                "(vb %" PRIu16
                ") Unexpected ack for set vbucket op on "
                "stream '%s' state '%s'",
                vb_,
                name_.c_str(),
                to_string(state_.load()).c_str());
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
    if (vb && takeoverStart != 0 &&
        !vb->isTakeoverBackedUp() &&
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
        log(EXTENSION_LOG_NOTICE,
            "(vb %" PRIu16
            ") Stream closed, "
            "%" PRIu64
            " items sent from backfill phase, "
            "%" PRIu64
            " items sent from memory phase, "
            "%" PRIu64 " was last seqno sent",
            vb_,
            uint64_t(backfillItems.sent.load()),
            uint64_t(itemsFromMemoryPhase.load()),
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

bool ActiveStream::isForceValueCompressionEnabled() {
    auto producer = producerPtr.lock();
    if (producer) {
        return producer->isForceValueCompressionEnabled();
    }
    /* If the 'producer' is deleted, what we return doesn't matter */
    return false;
}

bool ActiveStream::isSnappyEnabled() {
    auto producer = producerPtr.lock();
    if (producer) {
        return producer->isSnappyEnabled();
    }
    /* If the 'producer' is deleted, what we return doesn't matter */
    return false;
}

void ActiveStream::addStats(ADD_STAT add_stat, const void *c) {
    Stream::addStats(add_stat, c);

    try {
        const int bsize = 1024;
        char buffer[bsize];
        checked_snprintf(buffer, bsize, "%s:stream_%d_backfill_disk_items",
                         name_.c_str(), vb_);
        add_casted_stat(buffer, backfillItems.disk, add_stat, c);
        checked_snprintf(buffer, bsize, "%s:stream_%d_backfill_mem_items",
                         name_.c_str(), vb_);
        add_casted_stat(buffer, backfillItems.memory, add_stat, c);
        checked_snprintf(buffer, bsize, "%s:stream_%d_backfill_sent",
                         name_.c_str(), vb_);
        add_casted_stat(buffer, backfillItems.sent, add_stat, c);
        checked_snprintf(buffer, bsize, "%s:stream_%d_memory_phase",
                         name_.c_str(), vb_);
        add_casted_stat(buffer, itemsFromMemoryPhase.load(), add_stat, c);
        checked_snprintf(buffer, bsize, "%s:stream_%d_last_sent_seqno",
                         name_.c_str(), vb_);
        add_casted_stat(buffer, lastSentSeqno.load(), add_stat, c);
        checked_snprintf(buffer, bsize, "%s:stream_%d_last_sent_snap_end_seqno",
                         name_.c_str(), vb_);
        add_casted_stat(buffer,
                        lastSentSnapEndSeqno.load(std::memory_order_relaxed),
                        add_stat, c);
        checked_snprintf(buffer, bsize, "%s:stream_%d_last_read_seqno",
                         name_.c_str(), vb_);
        add_casted_stat(buffer, lastReadSeqno.load(), add_stat, c);
        checked_snprintf(buffer,
                         bsize,
                         "%s:stream_%d_last_read_seqno_unsnapshotted",
                         name_.c_str(),
                         vb_);
        add_casted_stat(buffer, lastReadSeqnoUnSnapshotted.load(), add_stat, c);
        checked_snprintf(buffer, bsize, "%s:stream_%d_ready_queue_memory",
                         name_.c_str(), vb_);
        add_casted_stat(buffer, getReadyQueueMemory(), add_stat, c);
        checked_snprintf(buffer, bsize, "%s:stream_%d_items_ready",
                         name_.c_str(), vb_);
        add_casted_stat(buffer, itemsReady.load() ? "true" : "false", add_stat,
                        c);
        checked_snprintf(buffer, bsize, "%s:stream_%d_backfill_buffer_bytes",
                         name_.c_str(), vb_);
        add_casted_stat(buffer, bufferedBackfill.bytes, add_stat, c);
        checked_snprintf(buffer, bsize, "%s:stream_%d_backfill_buffer_items",
                         name_.c_str(), vb_);
        add_casted_stat(buffer, bufferedBackfill.items, add_stat, c);

        if (isTakeoverSend() && takeoverStart != 0) {
            checked_snprintf(buffer, bsize, "%s:stream_%d_takeover_since",
                             name_.c_str(), vb_);
            add_casted_stat(buffer, ep_current_time() - takeoverStart, add_stat,
                            c);
        }
    } catch (std::exception& error) {
        LOG(EXTENSION_LOG_WARNING,
            "ActiveStream::addStats: Failed to build stats: %s", error.what());
    }

    filter.addStats(add_stat, c, name_, vb_);
}

void ActiveStream::addTakeoverStats(ADD_STAT add_stat, const void *cookie,
                                    const VBucket& vb) {
    LockHolder lh(streamMutex);

    add_casted_stat("name", name_, add_stat, cookie);
    if (!isActive()) {
        log(EXTENSION_LOG_WARNING,
            "(vb %" PRIu16
            ") "
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
                    add_stat, cookie);

    size_t vb_items = vb.getNumItems();
    size_t chk_items =
            vb_items > 0 ? vb.checkpointManager->getNumItemsForCursor(name_)
                         : 0;

    size_t del_items = 0;
    try {
        del_items = engine->getKVBucket()->getNumPersistedDeletes(vb_);
    } catch (std::runtime_error& e) {
        log(EXTENSION_LOG_WARNING,
            "ActiveStream:addTakeoverStats: exception while getting num "
            "persisted "
            "deletes for vbucket:%" PRIu16
            " - treating as 0 deletes. "
            "Details: %s",
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

            // See if the response is a system-event
            processSystemEvent(response.get());
            return popFromReadyQ();
        }
    }
    return nullptr;
}

bool ActiveStream::nextCheckpointItem() {
    VBucketPtr vbucket = engine->getVBucket(vb_);
    if (vbucket &&
        vbucket->checkpointManager->getNumItemsForCursor(name_) > 0) {
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

ActiveStreamCheckpointProcessorTask::ActiveStreamCheckpointProcessorTask(
        EventuallyPersistentEngine& e, std::shared_ptr<DcpProducer> p)
    : GlobalTask(
              &e, TaskId::ActiveStreamCheckpointProcessorTask, INT_MAX, false),
      description("Process checkpoint(s) for DCP producer " + p->getName()),
      notified(false),
      iterationsBeforeYield(
              e.getConfiguration().getDcpProducerSnapshotMarkerYieldLimit()),
      producerPtr(p) {
}

bool ActiveStreamCheckpointProcessorTask::run() {
    if (engine->getEpStats().isShutdown) {
        return false;
    }

    // Setup that we will sleep forever when done.
    snooze(INT_MAX);

    // Clear the notfification flag
    notified.store(false);

    size_t iterations = 0;
    do {
        std::shared_ptr<ActiveStream> stream = queuePop();

        if (stream) {
            stream->nextCheckpointItemTask();
        } else {
            break;
        }
        iterations++;
    } while(!queueEmpty()
            && iterations < iterationsBeforeYield);

    // Now check if we were re-notified or there are still checkpoints
    bool expected = true;
    if (notified.compare_exchange_strong(expected, false)
        || !queueEmpty()) {
        // wakeUp, essentially yielding and allowing other tasks a go
        wakeUp();
    }

    return true;
}

void ActiveStreamCheckpointProcessorTask::wakeup() {
    ExecutorPool::get()->wake(getId());
}

void ActiveStreamCheckpointProcessorTask::schedule(
        std::shared_ptr<ActiveStream> stream) {
    pushUnique(stream->getVBucket());

    bool expected = false;
    if (notified.compare_exchange_strong(expected, true)) {
        wakeup();
    }
}

void ActiveStreamCheckpointProcessorTask::cancelTask() {
    LockHolder lh(workQueueLock);
    while (!queue.empty()) {
        queue.pop();
    }
    queuedVbuckets.clear();
}

void ActiveStreamCheckpointProcessorTask::addStats(const std::string& name,
                                                   ADD_STAT add_stat,
                                                   const void* c) const {
    // Take a copy of the queue data under lock; then format it to stats.
    std::queue<VBucket::id_type> qCopy;
    std::unordered_set<VBucket::id_type> qMapCopy;
    {
        LockHolder lh(workQueueLock);
        qCopy = queue;
        qMapCopy = queuedVbuckets;
    }

    auto prefix = name + ":ckpt_processor_";
    add_casted_stat((prefix + "queue_size").c_str(), qCopy.size(), add_stat, c);
    add_casted_stat(
            (prefix + "queue_map_size").c_str(), qMapCopy.size(), add_stat, c);

    // Form a comma-separated string of the queue's contents.
    std::string contents;
    while (!qCopy.empty()) {
        contents += std::to_string(qCopy.front()) + ",";
        qCopy.pop();
    }
    if (!contents.empty()) {
        contents.pop_back();
    }
    add_casted_stat(
            (prefix + "queue_contents").c_str(), contents.c_str(), add_stat, c);

    // Form a comma-separated string of the queue map's contents.
    std::string qMapContents;
    for (auto& vbid : qMapCopy) {
        qMapContents += std::to_string(vbid) + ",";
    }
    if (!qMapContents.empty()) {
        qMapContents.pop_back();
    }
    add_casted_stat((prefix + "queue_map_contents").c_str(),
                    qMapContents.c_str(),
                    add_stat,
                    c);

    add_casted_stat((prefix + "notified").c_str(), notified, add_stat, c);
}

void ActiveStream::nextCheckpointItemTask() {
    VBucketPtr vbucket = engine->getVBucket(vb_);
    if (vbucket) {
        auto items = getOutstandingItems(*vbucket);
        processItems(items);
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
    vb.checkpointManager->getAllItemsForCursor(name_, items);
    engine->getEpStats().dcpCursorsGetItemsHisto.add(
            std::chrono::duration_cast<std::chrono::microseconds>(
                    ProcessClock::now() - _begin_));

    if (vb.checkpointManager->getNumCheckpoints() > 1) {
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
    value_t value = item->getValue();
    /**
     * If there is no value, no modification needs to be done
     */
    if (value) {
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
        queued_item& item) {
    if (item->getOperation() != queue_op::system_event) {
        auto cKey = Collections::DocKey::make(item->getKey(), currentSeparator);
        queued_item finalQueuedItem(item);
        if (shouldModifyItem(item, includeValue, includeXattributes,
                             isForceValueCompressionEnabled(),
                             isSnappyEnabled())) {
            auto finalItem = std::make_unique<Item>(*item);
            finalItem->pruneValueAndOrXattrs(includeValue, includeXattributes);

            if (isSnappyEnabled()) {
                if (isForceValueCompressionEnabled()) {
                    if (!mcbp::datatype::is_snappy(finalItem->getDataType())) {
                        if (!finalItem->compressValue()) {
                            LOG(EXTENSION_LOG_WARNING,
                                "Failed to snappy compress an uncompressed value");
                        }
                    }
                }
            } else {
                if (mcbp::datatype::is_snappy(finalItem->getDataType())) {
                    if (!finalItem->decompressValue()) {
                        LOG(EXTENSION_LOG_WARNING,
                            "Failed to snappy uncompress a compressed value");
                    }
                }
            }

            finalQueuedItem = std::move(finalItem);
        }

        /**
         * Create a mutation response to be placed in the ready queue.
         * Note that once an item has been placed in the ready queue
         * as compressed, it will be sent out as compressed even if the
         * client explicitly changes the setting to request uncompressed
         * values.
         */
        return std::make_unique<MutationProducerResponse>(
                finalQueuedItem,
                opaque_,
                includeValue,
                includeXattributes,
                includeDeleteTime,
                cKey.getCollectionLen());
    } else {
        return SystemEventProducerMessage::make(opaque_, item);
    }
}

void ActiveStream::processItems(std::vector<queued_item>& items) {
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
            nextCheckpointItemTask();
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

    LockHolder lh(streamMutex);

    if (!isActive() || isBackfilling()) {
        // If stream was closed forcefully by the time the checkpoint items
        // retriever task completed, or if we decided to switch the stream to
        // backfill state from in-memory state, none of the acquired mutations
        // should be added on the stream's readyQ. We must drop items in case
        // we switch state from in-memory to backfill because we schedule
        // backfill from lastReadSeqno + 1
        items.clear();
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
        log(EXTENSION_LOG_NOTICE,
            "(vb %" PRIu16
            ") Stream closing, "
            "sent until seqno %" PRIu64
            " "
            "remaining items %" PRIu64
            ", "
            "reason: %s",
            vb_,
            lastSentSeqno.load(),
            uint64_t(readyQ_non_meta_items.load()),
            getEndStreamStatusStr(reason).c_str());
    }
}

void ActiveStream::scheduleBackfill_UNLOCKED(bool reschedule) {
    if (isBackfillTaskRunning) {
        log(EXTENSION_LOG_NOTICE,
            "(vb %" PRIu16
            ") Skipping "
            "scheduleBackfill_UNLOCKED; "
            "lastReadSeqno %" PRIu64
            ", reschedule flag "
            ": %s",
            vb_,
            lastReadSeqno.load(),
            reschedule ? "True" : "False");
        return;
    }

    VBucketPtr vbucket = engine->getVBucket(vb_);
    if (!vbucket) {
        log(EXTENSION_LOG_WARNING,
            "(vb %" PRIu16
            ") Failed to schedule "
            "backfill as unable to get vbucket; "
            "lastReadSeqno : %" PRIu64
            ", "
            "reschedule : %s",
            vb_,
            lastReadSeqno.load(),
            reschedule ? "True" : "False");
        return;
    }

    auto producer = producerPtr.lock();
    if (!producer) {
        log(EXTENSION_LOG_WARNING,
            "(vb %" PRIu16
            ") Aborting scheduleBackfill_UNLOCKED() "
            "as the producer conn is deleted; "
            "lastReadSeqno : %" PRIu64
            ", "
            "reschedule : %s",
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
            throw std::logic_error("ActiveStream::scheduleBackfill_UNLOCKED: "
                                   "lastReadSeqno (which is " +
                                   std::to_string(lastReadSeqno.load()) +
                                   " ) is greater than vbHighSeqno (which is " +
                                   std::to_string(vbHighSeqno) + " ). " +
                                   "for stream " + producer->logHeader() +
                                   "; vb " + std::to_string(vb_));
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
            std::tie(curChkSeqno, tryBackfill) =
                    vbucket->checkpointManager->registerCursorBySeqno(
                            name_,
                            lastReadSeqno.load(),
                            MustSendCheckpointEnd::NO);
        } catch(std::exception& error) {
            log(EXTENSION_LOG_WARNING,
                "(vb %" PRIu16
                ") Failed to register "
                "cursor: %s",
                vb_,
                error.what());
            endStream(END_STREAM_STATE);
        }

        if (lastReadSeqno.load() > curChkSeqno) {
            throw std::logic_error("ActiveStream::scheduleBackfill_UNLOCKED: "
                                   "lastReadSeqno (which is " +
                                   std::to_string(lastReadSeqno.load()) +
                                   " ) is greater than curChkSeqno (which is " +
                                   std::to_string(curChkSeqno) + " ). " +
                                   "for stream " + producer->logHeader() +
                                   "; vb " + std::to_string(vb_));
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
        log(EXTENSION_LOG_NOTICE,
            "(vb %" PRIu16
            ") Scheduling backfill "
            "from %" PRIu64 " to %" PRIu64
            ", reschedule "
            "flag : %s",
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
            log(EXTENSION_LOG_NOTICE,
                "(vb %" PRIu16
                ") Did not schedule "
                "backfill with reschedule : True, "
                "tryBackfill : True; "
                "backfillStart : %" PRIu64
                ", "
                "backfillEnd : %" PRIu64
                ", "
                "flags_ : %" PRIu32
                ", "
                "start_seqno_ : %" PRIu64
                ", "
                "end_seqno_ : %" PRIu64
                ", "
                "lastReadSeqno : %" PRIu64
                ", "
                "lastSentSeqno : %" PRIu64
                ", "
                "curChkSeqno : %" PRIu64
                ", "
                "itemsReady : %s",
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

                curChkSeqno = result.first;
            } catch (std::exception& error) {
                log(EXTENSION_LOG_WARNING,
                    "(vb %" PRIu16
                    ") Failed to register "
                    "cursor: %s",
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
    log(EXTENSION_LOG_NOTICE,
        "(vb %" PRIu16
        ") Handling slow stream; "
        "state_ : %s, "
        "lastReadSeqno : %" PRIu64
        ", "
        "lastSentSeqno : %" PRIu64
        ", "
        "isBackfillTaskRunning : %s",
        vb_,
        to_string(state_.load()).c_str(),
        lastReadSeqno.load(),
        lastSentSeqno.load(),
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
            std::string connHeader =
                    producer ? producer->logHeader()
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

    EXTENSION_LOG_LEVEL logLevel = getTransitionStateLogLevel(state_, newState);
    log(logLevel,
        "ActiveStream::transitionState: (vb %d) "
        "Transitioning from %s to %s",
        vb_,
        to_string(state_.load()).c_str(),
        to_string(newState).c_str());

    bool validTransition = false;
    switch (state_.load()) {
        case StreamState::Pending:
            if (newState == StreamState::Backfilling ||
                    newState == StreamState::Dead) {
                validTransition = true;
            }
            break;
        case StreamState::Backfilling:
            if(newState == StreamState::InMemory ||
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
        throw std::invalid_argument("ActiveStream::transitionState:"
                " newState (which is " + to_string(newState) +
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
            nextCheckpointItem();
            break;
        case StreamState::Dead:
            removeCheckpointCursor();
            break;
        case StreamState::TakeoverWait:
        case StreamState::Pending:
            break;
        case StreamState::Reading:
            throw std::logic_error("ActiveStream::transitionState:"
                    " newState can't be " + to_string(newState) +
                    "!");
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
    return vbucket->checkpointManager->getNumItemsForCursor(name_) +
           readyQ_non_meta_items;
}

uint64_t ActiveStream::getLastReadSeqno() const {
    return lastReadSeqno.load();
}

uint64_t ActiveStream::getLastSentSeqno() const {
    return lastSentSeqno.load();
}

void ActiveStream::log(EXTENSION_LOG_LEVEL severity,
                       const char* fmt,
                       ...) const {
    va_list va;
    va_start(va, fmt);
    auto producer = producerPtr.lock();
    if (producer) {
        producer->getLogger().vlog(severity, fmt, va);
    } else {
        static Logger defaultLogger =
                Logger("DCP (Producer): **Deleted conn**");
        defaultLogger.vlog(severity, fmt, va);
    }
    va_end(va);
}

bool ActiveStream::isCurrentSnapshotCompleted() const
{
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
    /* Drop the existing cursor */
    return vbucket->checkpointManager->removeCursor(name_);
}

EXTENSION_LOG_LEVEL ActiveStream::getTransitionStateLogLevel(
        StreamState currState, StreamState newState) {
    if ((currState == StreamState::Pending) ||
        (newState == StreamState::Dead)) {
        return EXTENSION_LOG_INFO;
    }
    return EXTENSION_LOG_NOTICE;
}

void ActiveStream::processSystemEvent(DcpResponse* response) {
    if (response->getEvent() == DcpResponse::Event::SystemEvent) {
        auto se = static_cast<SystemEventProducerMessage*>(response);
        if (se->getSystemEvent() == mcbp::systemevent::id::CollectionsSeparatorChanged) {
            currentSeparator =
                    std::string(se->getKey().data(), se->getKey().size());
            // filter needs new separator?
        }
    }
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
        vb->checkpointManager->removeCursor(name_);
    }
}

NotifierStream::NotifierStream(EventuallyPersistentEngine* e,
                               std::shared_ptr<DcpProducer> p,
                               const std::string& name,
                               uint32_t flags,
                               uint32_t opaque,
                               uint16_t vb,
                               uint64_t st_seqno,
                               uint64_t en_seqno,
                               uint64_t vb_uuid,
                               uint64_t snap_start_seqno,
                               uint64_t snap_end_seqno)
    : Stream(name,
             flags,
             opaque,
             vb,
             st_seqno,
             en_seqno,
             vb_uuid,
             snap_start_seqno,
             snap_end_seqno,
             Type::Notifier),
      producerPtr(p) {
    LockHolder lh(streamMutex);
    VBucketPtr vbucket = e->getVBucket(vb_);
    if (vbucket && static_cast<uint64_t>(vbucket->getHighSeqno()) > st_seqno) {
        pushToReadyQ(std::make_unique<StreamEndResponse>(
                opaque_, END_STREAM_OK, vb_));
        transitionState(StreamState::Dead);
        itemsReady.store(true);
    }

    p->getLogger().log(EXTENSION_LOG_NOTICE,
        "(vb %d) stream created with start seqno %" PRIu64 " and end seqno %"
        PRIu64, vb, st_seqno, en_seqno);
}

uint32_t NotifierStream::setDead(end_stream_status_t status) {
    std::unique_lock<std::mutex> lh(streamMutex);
    if (isActive()) {
        transitionState(StreamState::Dead);
        if (status != END_STREAM_DISCONNECTED) {
            pushToReadyQ(
                    std::make_unique<StreamEndResponse>(opaque_, status, vb_));
            lh.unlock();
            notifyStreamReady();
        }
    }
    return 0;
}

void NotifierStream::notifySeqnoAvailable(uint64_t seqno) {
    std::unique_lock<std::mutex> lh(streamMutex);
    if (isActive() && start_seqno_ < seqno) {
        pushToReadyQ(std::make_unique<StreamEndResponse>(
                opaque_, END_STREAM_OK, vb_));
        transitionState(StreamState::Dead);
        lh.unlock();
        notifyStreamReady();
    }
}

std::unique_ptr<DcpResponse> NotifierStream::next() {
    LockHolder lh(streamMutex);

    if (readyQ.empty()) {
        itemsReady.store(false);
        return nullptr;
    }

    auto& response = readyQ.front();
    auto producer = producerPtr.lock();
    if (producer && producer->bufferLogInsert(response->getMessageSize())) {
        return popFromReadyQ();
    }
    return nullptr;
}

void NotifierStream::transitionState(StreamState newState) {
    log(EXTENSION_LOG_INFO,
        "NotifierStream::transitionState: (vb %d) "
        "Transitioning from %s to %s",
        vb_,
        to_string(state_.load()).c_str(),
        to_string(newState).c_str());

    if (state_ == newState) {
        return;
    }

    bool validTransition = false;
    switch (state_.load()) {
        case StreamState::Pending:
            if (newState == StreamState::Dead) {
                validTransition = true;
            }
            break;

        case StreamState::Backfilling:
        case StreamState::InMemory:
        case StreamState::TakeoverSend:
        case StreamState::TakeoverWait:
        case StreamState::Reading:
        case StreamState::Dead:
            // No other state transitions are valid for a notifier stream.
            break;
    }

    if (!validTransition) {
        throw std::invalid_argument("NotifierStream::transitionState:"
                " newState (which is " + to_string(newState) +
                ") is not valid for current state (which is " +
                to_string(state_.load()) + ")");
    }
    state_ = newState;
}

void NotifierStream::addStats(ADD_STAT add_stat, const void* c) {
    Stream::addStats(add_stat, c);
}

void NotifierStream::log(EXTENSION_LOG_LEVEL severity,
                         const char* fmt,
                         ...) const {
    va_list va;
    va_start(va, fmt);
    auto producer = producerPtr.lock();
    if (producer) {
        producer->getLogger().vlog(severity, fmt, va);
    } else {
        static Logger defaultLogger =
                Logger("DCP (Notifier): **Deleted conn**");
        defaultLogger.vlog(severity, fmt, va);
    }
    va_end(va);
}

void NotifierStream::notifyStreamReady() {
    auto producer = producerPtr.lock();
    if (!producer) {
        return;
    }

    bool inverse = false;
    if (itemsReady.compare_exchange_strong(inverse, true)) {
        producer->notifyStreamReady(vb_);
    }
}

PassiveStream::PassiveStream(EventuallyPersistentEngine* e,
                             std::shared_ptr<DcpConsumer> c,
                             const std::string& name,
                             uint32_t flags,
                             uint32_t opaque,
                             uint16_t vb,
                             uint64_t st_seqno,
                             uint64_t en_seqno,
                             uint64_t vb_uuid,
                             uint64_t snap_start_seqno,
                             uint64_t snap_end_seqno,
                             uint64_t vb_high_seqno)
    : Stream(name,
             flags,
             opaque,
             vb,
             st_seqno,
             en_seqno,
             vb_uuid,
             snap_start_seqno,
             snap_end_seqno,
             Type::Passive),
      engine(e),
      consumerPtr(c),
      last_seqno(vb_high_seqno),
      cur_snapshot_start(0),
      cur_snapshot_end(0),
      cur_snapshot_type(Snapshot::None),
      cur_snapshot_ack(false) {
    LockHolder lh(streamMutex);
    streamRequest_UNLOCKED(vb_uuid);
    itemsReady.store(true);
}

PassiveStream::~PassiveStream() {
    uint32_t unackedBytes = clearBuffer_UNLOCKED();
    if (state_ != StreamState::Dead) {
        // Destructed a "live" stream, log it.
        log(EXTENSION_LOG_NOTICE,
            "(vb %" PRId16
            ") Destructing stream."
            " last_seqno is %" PRIu64 ", unAckedBytes is %" PRIu32 ".",
            vb_,
            last_seqno.load(),
            unackedBytes);
    }
}

void PassiveStream::streamRequest(uint64_t vb_uuid) {
    {
        std::unique_lock<std::mutex> lh(streamMutex);
        streamRequest_UNLOCKED(vb_uuid);
    }
    notifyStreamReady();
}

void PassiveStream::streamRequest_UNLOCKED(uint64_t vb_uuid) {
    /* the stream should send a don't care vb_uuid if start_seqno is 0 */
    pushToReadyQ(std::make_unique<StreamRequest>(vb_,
                                                 opaque_,
                                                 flags_,
                                                 start_seqno_,
                                                 end_seqno_,
                                                 start_seqno_ ? vb_uuid : 0,
                                                 snap_start_seqno_,
                                                 snap_end_seqno_));

    const char* type = (flags_ & DCP_ADD_STREAM_FLAG_TAKEOVER)
        ? "takeover stream" : "stream";
    log(EXTENSION_LOG_NOTICE,
        "(vb %" PRId16 ") Attempting to add %s: opaque_:%" PRIu32
        ", "
        "start_seqno_:%" PRIu64 ", end_seqno_:%" PRIu64
        ", "
        "vb_uuid:%" PRIu64 ", snap_start_seqno_:%" PRIu64
        ", "
        "snap_end_seqno_:%" PRIu64 ", last_seqno:%" PRIu64,
        vb_,
        type,
        opaque_,
        start_seqno_,
        end_seqno_,
        vb_uuid,
        snap_start_seqno_,
        snap_end_seqno_,
        last_seqno.load());
}

uint32_t PassiveStream::setDead(end_stream_status_t status) {
    /* Hold buffer lock so that we clear out all items before we set the stream
       to dead state. We do not want to add any new message to the buffer or
       process any items in the buffer once we set the stream state to dead. */
    std::unique_lock<std::mutex> lg(buffer.bufMutex);
    uint32_t unackedBytes = clearBuffer_UNLOCKED();
    bool killed = false;

    LockHolder slh(streamMutex);
    if (transitionState(StreamState::Dead)) {
        killed = true;
    }

    if (killed) {
        EXTENSION_LOG_LEVEL logLevel = EXTENSION_LOG_NOTICE;
        if (END_STREAM_DISCONNECTED == status) {
            logLevel = EXTENSION_LOG_WARNING;
        }
        log(logLevel,
            "(vb %" PRId16
            ") Setting stream to dead state, last_seqno is %" PRIu64
            ", unAckedBytes is %" PRIu32 ", status is %s",
            vb_,
            last_seqno.load(),
            unackedBytes,
            getEndStreamStatusStr(status).c_str());
    }
    return unackedBytes;
}

void PassiveStream::acceptStream(uint16_t status, uint32_t add_opaque) {
    std::unique_lock<std::mutex> lh(streamMutex);
    if (isPending()) {
        if (status == ENGINE_SUCCESS) {
            transitionState(StreamState::Reading);
        } else {
            transitionState(StreamState::Dead);
        }
        pushToReadyQ(std::make_unique<AddStreamResponse>(
                add_opaque, opaque_, status));
        lh.unlock();
        notifyStreamReady();
    }
}

void PassiveStream::reconnectStream(VBucketPtr &vb,
                                    uint32_t new_opaque,
                                    uint64_t start_seqno) {
    /* the stream should send a don't care vb_uuid if start_seqno is 0 */
    vb_uuid_ = start_seqno ? vb->failovers->getLatestEntry().vb_uuid : 0;

    snapshot_info_t info = vb->checkpointManager->getSnapshotInfo();
    if (info.range.end == info.start) {
        info.range.start = info.start;
    }

    snap_start_seqno_ = info.range.start;
    start_seqno_ = info.start;
    snap_end_seqno_ = info.range.end;

    log(EXTENSION_LOG_NOTICE,
        "(vb %d) Attempting to reconnect stream with opaque %" PRIu32
        ", start seq no %" PRIu64 ", end seq no %" PRIu64
        ", snap start seqno %" PRIu64 ", and snap end seqno %" PRIu64,
        vb_,
        new_opaque,
        start_seqno,
        end_seqno_,
        snap_start_seqno_,
        snap_end_seqno_);
    {
        LockHolder lh(streamMutex);
        last_seqno.store(start_seqno);
        pushToReadyQ(std::make_unique<StreamRequest>(vb_,
                                                     new_opaque,
                                                     flags_,
                                                     start_seqno,
                                                     end_seqno_,
                                                     vb_uuid_,
                                                     snap_start_seqno_,
                                                     snap_end_seqno_));
    }
    notifyStreamReady();
}

ENGINE_ERROR_CODE PassiveStream::messageReceived(std::unique_ptr<DcpResponse> dcpResponse) {
    if (!dcpResponse) {
        return ENGINE_EINVAL;
    }

    if (!isActive()) {
        return ENGINE_KEY_ENOENT;
    }

    auto seqno = dcpResponse->getBySeqno();
    if (seqno) {
        if (uint64_t(*seqno) <= last_seqno.load()) {
            log(EXTENSION_LOG_WARNING,
                "(vb %d) Erroneous (out of sequence) message (%s) received, "
                "with opaque: %" PRIu32 ", its seqno (%" PRIu64
                ") is not "
                "greater than last received seqno (%" PRIu64
                "); "
                "Dropping mutation!",
                vb_,
                dcpResponse->to_string(),
                opaque_,
                *seqno,
                last_seqno.load());
            return ENGINE_ERANGE;
        }
        last_seqno.store(*seqno);
    } else if(dcpResponse->getEvent() == DcpResponse::Event::SnapshotMarker) {
        auto s = static_cast<SnapshotMarker*>(dcpResponse.get());
        uint64_t snapStart = s->getStartSeqno();
        uint64_t snapEnd = s->getEndSeqno();
        if (snapStart < last_seqno.load() && snapEnd <= last_seqno.load()) {
            log(EXTENSION_LOG_WARNING,
                "(vb %d) Erroneous snapshot marker received, with "
                "opaque: %" PRIu32
                ", its start "
                "(%" PRIu64 "), and end (%" PRIu64
                ") are less than last "
                "received seqno (%" PRIu64 "); Dropping marker!",
                vb_,
                opaque_,
                snapStart,
                snapEnd,
                last_seqno.load());
            return ENGINE_ERANGE;
        }
    }

    switch (engine->getReplicationThrottle().getStatus()) {
    case ReplicationThrottle::Status::Disconnect:
        log(EXTENSION_LOG_WARNING,
            "vb:%" PRIu16
            " Disconnecting the connection as there is "
            "no memory to complete replication",
            vb_);
        return ENGINE_DISCONNECT;
    case ReplicationThrottle::Status::Process:
        if (buffer.empty()) {
            /* Process the response here itself rather than buffering it */
            ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
            switch (dcpResponse->getEvent()) {
            case DcpResponse::Event::Mutation:
                ret = processMutation(
                        static_cast<MutationResponse*>(dcpResponse.get()));
                break;
            case DcpResponse::Event::Deletion:
            case DcpResponse::Event::Expiration:
                ret = processDeletion(
                        static_cast<MutationResponse*>(dcpResponse.get()));
                break;
            case DcpResponse::Event::SnapshotMarker:
                processMarker(static_cast<SnapshotMarker*>(dcpResponse.get()));
                break;
            case DcpResponse::Event::SetVbucket:
                processSetVBucketState(
                        static_cast<SetVBucketState*>(dcpResponse.get()));
                break;
            case DcpResponse::Event::StreamEnd: {
                LockHolder lh(streamMutex);
                transitionState(StreamState::Dead);
            } break;
            case DcpResponse::Event::SystemEvent: {
                ret = processSystemEvent(
                        *static_cast<SystemEventMessage*>(dcpResponse.get()));
                break;
            }
            default:
                log(EXTENSION_LOG_WARNING,
                    "(vb %d) Unknown event:%d, opaque:%" PRIu32,
                    vb_,
                    int(dcpResponse->getEvent()),
                    opaque_);
                return ENGINE_DISCONNECT;
            }

            if (ret == ENGINE_ENOMEM) {
                if (engine->getReplicationThrottle().doDisconnectOnNoMem()) {
                    log(EXTENSION_LOG_WARNING,
                        "vb:%" PRIu16
                        " Disconnecting the connection as there is no "
                        "memory to complete replication; process dcp "
                        "event returned no memory",
                        vb_);
                    return ENGINE_DISCONNECT;
                }
            }

            if (ret != ENGINE_TMPFAIL && ret != ENGINE_ENOMEM) {
                return ret;
            }
        }
        break;
    case ReplicationThrottle::Status::Pause:
        /* Do nothing specific here, we buffer item for this case and
           other cases below */
        break;
    }

    // Only buffer if the stream is not dead
    if (isActive()) {
        buffer.push(std::move(dcpResponse));
    }
    return ENGINE_TMPFAIL;
}

process_items_error_t PassiveStream::processBufferedMessages(uint32_t& processed_bytes,
                                                             size_t batchSize) {
    std::unique_lock<std::mutex> lh(buffer.bufMutex);
    uint32_t count = 0;
    uint32_t message_bytes = 0;
    uint32_t total_bytes_processed = 0;
    bool failed = false, noMem = false;

    while (count < batchSize && !buffer.messages.empty()) {
        ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
        /* If the stream is in dead state we should not process any remaining
           items in the buffer, we should rather clear them */
        if (!isActive()) {
            total_bytes_processed += clearBuffer_UNLOCKED();
            processed_bytes = total_bytes_processed;
            return all_processed;
        }

        std::unique_ptr<DcpResponse> response = buffer.pop_front(lh);

        // Release bufMutex whilst we attempt to process the message
        // a lock inversion exists with connManager if we hold this.
        lh.unlock();

        message_bytes = response->getMessageSize();

        switch (response->getEvent()) {
            case DcpResponse::Event::Mutation:
                ret = processMutation(static_cast<MutationResponse*>(response.get()));
                break;
            case DcpResponse::Event::Deletion:
            case DcpResponse::Event::Expiration:
                ret = processDeletion(static_cast<MutationResponse*>(response.get()));
                break;
            case DcpResponse::Event::SnapshotMarker:
                processMarker(static_cast<SnapshotMarker*>(response.get()));
                break;
            case DcpResponse::Event::SetVbucket:
                processSetVBucketState(static_cast<SetVBucketState*>(response.get()));
                break;
            case DcpResponse::Event::StreamEnd:
                {
                    LockHolder lh(streamMutex);
                    transitionState(StreamState::Dead);
                }
                break;
            case DcpResponse::Event::SystemEvent: {
                    ret = processSystemEvent(
                            *static_cast<SystemEventMessage*>(response.get()));
                    break;
                }
            default:
                log(EXTENSION_LOG_WARNING,
                    "PassiveStream::processBufferedMessages:"
                    "(vb %" PRIu16
                    ") PassiveStream ignoring "
                    "unknown message type %s",
                    vb_,
                    response->to_string());
                continue;
        }

        if (ret == ENGINE_TMPFAIL || ret == ENGINE_ENOMEM) {
            failed = true;
            if (ret == ENGINE_ENOMEM) {
                noMem = true;
            }
        }

        // Re-acquire bufMutex so that
        // 1) we can update the buffer
        // 2) safely re-check the while conditional statement
        lh.lock();

        // If we failed and the stream is not dead, stash the DcpResponse at the
        // front of the queue and break the loop.
        if (failed && isActive()) {
            buffer.push_front(std::move(response), lh);
            break;
        }

        count++;
        if (ret != ENGINE_ERANGE) {
            total_bytes_processed += message_bytes;
        }
    }

    processed_bytes = total_bytes_processed;

    if (failed) {
        if (noMem && engine->getReplicationThrottle().doDisconnectOnNoMem()) {
            log(EXTENSION_LOG_WARNING,
                "vb:%" PRIu16
                " Processor task indicating disconnection as "
                "there is no memory to complete replication; process dcp "
                "event returned no memory ",
                vb_);
            return stop_processing;
        }
        return cannot_process;
    }

    return all_processed;
}

ENGINE_ERROR_CODE PassiveStream::processMutation(MutationResponse* mutation) {
    VBucketPtr vb = engine->getVBucket(vb_);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    auto consumer = consumerPtr.lock();
    if (!consumer) {
        return ENGINE_DISCONNECT;
    }

    if (uint64_t(*mutation->getBySeqno()) < cur_snapshot_start.load() ||
        uint64_t(*mutation->getBySeqno()) > cur_snapshot_end.load()) {
        log(EXTENSION_LOG_WARNING,
            "(vb %d) Erroneous mutation [sequence "
            "number does not fall in the expected snapshot range : "
            "{snapshot_start (%" PRIu64 ") <= seq_no (%" PRIu64
            ") <= "
            "snapshot_end (%" PRIu64 ")]; Dropping the mutation!",
            vb_,
            cur_snapshot_start.load(),
            *mutation->getBySeqno(),
            cur_snapshot_end.load());
        return ENGINE_ERANGE;
    }

    // MB-17517: Check for the incoming item's CAS validity. We /shouldn't/
    // receive anything without a valid CAS, however given that versions without
    // this check may send us "bad" CAS values, we should regenerate them (which
    // is better than rejecting the data entirely).
    if (!Item::isValidCas(mutation->getItem()->getCas())) {
        log(EXTENSION_LOG_WARNING,
            "Invalid CAS (0x%" PRIx64 ") received for mutation {vb:%" PRIu16
            ", seqno:%" PRId64 "}. Regenerating new CAS",
            mutation->getItem()->getCas(),
            vb_,
            mutation->getItem()->getBySeqno());
        mutation->getItem()->setCas();
    }

    ENGINE_ERROR_CODE ret;
    if (vb->isBackfillPhase()) {
        ret = engine->getKVBucket()->addBackfillItem(
                *mutation->getItem(),
                GenerateBySeqno::No,
                mutation->getExtMetaData());
    } else {
        ret = engine->getKVBucket()->setWithMeta(*mutation->getItem(),
                                                 0,
                                                 NULL,
                                                 consumer->getCookie(),
                                                 {vbucket_state_active,
                                                  vbucket_state_replica,
                                                  vbucket_state_pending},
                                                 CheckConflicts::No,
                                                 true,
                                                 GenerateBySeqno::No,
                                                 GenerateCas::No,
                                                 mutation->getExtMetaData(),
                                                 true);
    }

    if (ret != ENGINE_SUCCESS) {
        log(EXTENSION_LOG_WARNING,
            "vb:%" PRIu16
            " Got error '%s' while trying to process "
            "mutation with seqno:%" PRId64,
            vb_,
            cb::to_string(cb::to_engine_errc(ret)).c_str(),
            mutation->getItem()->getBySeqno());
    } else {
        handleSnapshotEnd(vb, *mutation->getBySeqno());
    }

    return ret;
}

ENGINE_ERROR_CODE PassiveStream::processDeletion(MutationResponse* deletion) {
    VBucketPtr vb = engine->getVBucket(vb_);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    auto consumer = consumerPtr.lock();
    if (!consumer) {
        return ENGINE_DISCONNECT;
    }

    if (uint64_t(*deletion->getBySeqno()) < cur_snapshot_start.load() ||
        uint64_t(*deletion->getBySeqno()) > cur_snapshot_end.load()) {
        log(EXTENSION_LOG_WARNING,
            "(vb %d) Erroneous deletion [sequence "
            "number does not fall in the expected snapshot range : "
            "{snapshot_start (%" PRIu64 ") <= seq_no (%" PRIu64
            ") <= "
            "snapshot_end (%" PRIu64 ")]; Dropping the deletion!",
            vb_,
            cur_snapshot_start.load(),
            *deletion->getBySeqno(),
            cur_snapshot_end.load());
        return ENGINE_ERANGE;
    }

    // The deleted value has a body, send it through the mutation path so we
    // set the deleted item with a value
    if (deletion->getItem()->getNBytes()) {
        return processMutation(deletion);
    }

    uint64_t delCas = 0;
    ENGINE_ERROR_CODE ret;
    ItemMetaData meta = deletion->getItem()->getMetaData();

    // MB-17517: Check for the incoming item's CAS validity.
    if (!Item::isValidCas(meta.cas)) {
        log(EXTENSION_LOG_WARNING,
            "Invalid CAS (0x%" PRIx64 ") received for deletion {vb:%" PRIu16
            ", seqno:%" PRId64 "}. Regenerating new CAS",
            meta.cas,
            vb_,
            *deletion->getBySeqno());
        meta.cas = Item::nextCas();
    }

    ret = engine->getKVBucket()->deleteWithMeta(deletion->getItem()->getKey(),
                                                delCas,
                                                nullptr,
                                                deletion->getVBucket(),
                                                consumer->getCookie(),
                                                {vbucket_state_active,
                                                 vbucket_state_replica,
                                                 vbucket_state_pending},
                                                CheckConflicts::No,
                                                meta,
                                                vb->isBackfillPhase(),
                                                GenerateBySeqno::No,
                                                GenerateCas::No,
                                                *deletion->getBySeqno(),
                                                deletion->getExtMetaData(),
                                                true);
    if (ret == ENGINE_KEY_ENOENT) {
        ret = ENGINE_SUCCESS;
    }

    if (ret != ENGINE_SUCCESS) {
        log(EXTENSION_LOG_WARNING,
            "vb:%" PRIu16
            " Got error '%s' while trying to process "
            "deletion with seqno:%" PRId64,
            vb_,
            cb::to_string(cb::to_engine_errc(ret)).c_str(),
            *deletion->getBySeqno());
    } else {
        handleSnapshotEnd(vb, *deletion->getBySeqno());
    }

    return ret;
}

ENGINE_ERROR_CODE PassiveStream::processSystemEvent(
        const SystemEventMessage& event) {
    VBucketPtr vb = engine->getVBucket(vb_);

    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    // Depending on the event, extras is different and key may even be empty
    // The specific handler will know how to interpret.
    switch (event.getSystemEvent()) {
    case mcbp::systemevent::id::CreateCollection: {
        rv = processCreateCollection(*vb, {event});
        break;
    }
    case mcbp::systemevent::id::DeleteCollection: {
        rv = processBeginDeleteCollection(*vb, {event});
        break;
    }
    case mcbp::systemevent::id::CollectionsSeparatorChanged: {
        rv = processSeparatorChanged(*vb, {event});
        break;
    }
    default: {
        rv = ENGINE_EINVAL;
        break;
    }
    }

    if (rv != ENGINE_SUCCESS) {
        log(EXTENSION_LOG_WARNING,
            "vb:%" PRIu16
            " Got error '%s' while trying to process "
            "system event",
            vb_,
            cb::to_string(cb::to_engine_errc(rv)).c_str());
    } else {
        handleSnapshotEnd(vb, *event.getBySeqno());
    }

    return rv;
}

ENGINE_ERROR_CODE PassiveStream::processCreateCollection(
        VBucket& vb, const CreateOrDeleteCollectionEvent& event) {
    try {
        vb.replicaAddCollection(event.getManifestUid(),
                                event.getCollection(),
                                event.getBySeqno());
    } catch (std::exception& e) {
        LOG(EXTENSION_LOG_WARNING,
            "PassiveStream::processCreateCollection exception %s",
            e.what());
        return ENGINE_EINVAL;
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE PassiveStream::processBeginDeleteCollection(
        VBucket& vb, const CreateOrDeleteCollectionEvent& event) {
    try {
        vb.replicaBeginDeleteCollection(event.getManifestUid(),
                                        event.getCollection(),
                                        event.getBySeqno());
    } catch (std::exception& e) {
        LOG(EXTENSION_LOG_WARNING,
            "PassiveStream::processBeginDeleteCollection exception %s",
            e.what());
        return ENGINE_EINVAL;
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE PassiveStream::processSeparatorChanged(
        VBucket& vb, const ChangeSeparatorCollectionEvent& event) {
    try {
        vb.replicaChangeCollectionSeparator(event.getManifestUid(),
                                            event.getSeparator(),
                                            event.getBySeqno());
    } catch (std::exception& e) {
        LOG(EXTENSION_LOG_WARNING,
            "PassiveStream::processSeparatorChanged exception %s",
            e.what());
        return ENGINE_EINVAL;
    }
    return ENGINE_SUCCESS;
}

void PassiveStream::processMarker(SnapshotMarker* marker) {
    VBucketPtr vb = engine->getVBucket(vb_);

    cur_snapshot_start.store(marker->getStartSeqno());
    cur_snapshot_end.store(marker->getEndSeqno());
    cur_snapshot_type.store((marker->getFlags() & MARKER_FLAG_DISK) ?
            Snapshot::Disk : Snapshot::Memory);

    if (vb) {
        auto& ckptMgr = *vb->checkpointManager;
        if (marker->getFlags() & MARKER_FLAG_DISK && vb->getHighSeqno() == 0) {
            vb->setBackfillPhase(true);
            // calling setBackfillPhase sets the openCheckpointId to zero.
            ckptMgr.setBackfillPhase(cur_snapshot_start.load(),
                                     cur_snapshot_end.load());
        } else {
            if (marker->getFlags() & MARKER_FLAG_CHK ||
                vb->checkpointManager->getOpenCheckpointId() == 0) {
                ckptMgr.createSnapshot(cur_snapshot_start.load(),
                                       cur_snapshot_end.load());
            } else {
                ckptMgr.updateCurrentSnapshotEnd(cur_snapshot_end.load());
            }
            vb->setBackfillPhase(false);
        }

        if (marker->getFlags() & MARKER_FLAG_ACK) {
            cur_snapshot_ack = true;
        }
    }
}

void PassiveStream::processSetVBucketState(SetVBucketState* state) {
    engine->getKVBucket()->setVBucketState(vb_, state->getState(), true);
    {
        LockHolder lh (streamMutex);
        pushToReadyQ(std::make_unique<SetVBucketStateResponse>(opaque_,
                                                               ENGINE_SUCCESS));
    }
    notifyStreamReady();
}

void PassiveStream::handleSnapshotEnd(VBucketPtr& vb, uint64_t byseqno) {
    if (byseqno == cur_snapshot_end.load()) {
        auto& ckptMgr = *vb->checkpointManager;
        if (cur_snapshot_type.load() == Snapshot::Disk &&
                vb->isBackfillPhase()) {
            vb->setBackfillPhase(false);
            const auto id = ckptMgr.getOpenCheckpointId() + 1;
            ckptMgr.checkAndAddNewCheckpoint(id, *vb);
        } else {
            size_t mem_threshold = engine->getEpStats().mem_high_wat.load();
            size_t mem_used =
                    engine->getEpStats().getEstimatedTotalMemoryUsed();
            /* We want to add a new replica checkpoint if the mem usage is above
               high watermark (85%) */
            if (mem_threshold < mem_used) {
                const auto id = ckptMgr.getOpenCheckpointId() + 1;
                ckptMgr.checkAndAddNewCheckpoint(id, *vb);
            }
        }

        if (cur_snapshot_ack) {
            {
                LockHolder lh(streamMutex);
                pushToReadyQ(std::make_unique<SnapshotMarkerResponse>(
                        opaque_, ENGINE_SUCCESS));
            }
            notifyStreamReady();
            cur_snapshot_ack = false;
        }
        cur_snapshot_type.store(Snapshot::None);
    }
}

void PassiveStream::addStats(ADD_STAT add_stat, const void *c) {
    Stream::addStats(add_stat, c);

    try {
        const int bsize = 1024;
        char buf[bsize];
        size_t bufferItems = 0;
        size_t bufferBytes = 0;
        {
            std::lock_guard<std::mutex> lg(buffer.bufMutex);
            bufferItems = buffer.messages.size();
            bufferBytes = buffer.bytes;
        }
        checked_snprintf(buf, bsize, "%s:stream_%d_buffer_items", name_.c_str(),
                         vb_);
        add_casted_stat(buf, bufferItems, add_stat, c);
        checked_snprintf(buf, bsize, "%s:stream_%d_buffer_bytes", name_.c_str(),
                         vb_);
        add_casted_stat(buf, bufferBytes, add_stat, c);
        checked_snprintf(buf, bsize, "%s:stream_%d_items_ready", name_.c_str(),
                         vb_);
        add_casted_stat(buf, itemsReady.load() ? "true" : "false", add_stat, c);
        checked_snprintf(buf, bsize, "%s:stream_%d_last_received_seqno",
                         name_.c_str(), vb_);
        add_casted_stat(buf, last_seqno.load(), add_stat, c);
        checked_snprintf(buf, bsize, "%s:stream_%d_ready_queue_memory",
                         name_.c_str(), vb_);
        add_casted_stat(buf, getReadyQueueMemory(), add_stat, c);

        checked_snprintf(buf, bsize, "%s:stream_%d_cur_snapshot_type",
                         name_.c_str(), vb_);
        add_casted_stat(buf, ::to_string(cur_snapshot_type.load()),
                        add_stat, c);

        if (cur_snapshot_type.load() != Snapshot::None) {
            checked_snprintf(buf, bsize, "%s:stream_%d_cur_snapshot_start",
                             name_.c_str(), vb_);
            add_casted_stat(buf, cur_snapshot_start.load(), add_stat, c);
            checked_snprintf(buf, bsize, "%s:stream_%d_cur_snapshot_end",
                             name_.c_str(), vb_);
            add_casted_stat(buf, cur_snapshot_end.load(), add_stat, c);
        }
    } catch (std::exception& error) {
        LOG(EXTENSION_LOG_WARNING,
            "PassiveStream::addStats: Failed to build stats: %s", error.what());
    }
}

std::unique_ptr<DcpResponse> PassiveStream::next() {
    LockHolder lh(streamMutex);

    if (readyQ.empty()) {
        itemsReady.store(false);
        return NULL;
    }

    return popFromReadyQ();
}

uint32_t PassiveStream::clearBuffer_UNLOCKED() {
    uint32_t unackedBytes = buffer.bytes;
    buffer.messages.clear();
    buffer.bytes = 0;
    return unackedBytes;
}

bool PassiveStream::transitionState(StreamState newState) {
    log(EXTENSION_LOG_INFO,
        "PassiveStream::transitionState: (vb %d) "
        "Transitioning from %s to %s",
        vb_,
        to_string(state_.load()).c_str(),
        to_string(newState).c_str());

    if (state_ == newState) {
        return false;
    }

    bool validTransition = false;
    switch (state_.load()) {
        case StreamState::Pending:
            if (newState == StreamState::Reading ||
                    newState == StreamState::Dead) {
                validTransition = true;
            }
            break;

        case StreamState::Backfilling:
        case StreamState::InMemory:
        case StreamState::TakeoverSend:
        case StreamState::TakeoverWait:
            // Not valid for passive streams
            break;

        case StreamState::Reading:
            if (newState == StreamState::Pending ||
                    newState == StreamState::Dead) {
                validTransition = true;
            }
            break;

        case StreamState::Dead:
            // Once 'dead' shouldn't transition away from it.
            break;
    }

    if (!validTransition) {
        throw std::invalid_argument("PassiveStream::transitionState:"
                " newState (which is" + to_string(newState) +
                ") is not valid for current state (which is " +
                to_string(state_.load()) + ")");
    }

    state_ = newState;
    return true;
}

std::string PassiveStream::getEndStreamStatusStr(end_stream_status_t status) {
    switch (status) {
        case END_STREAM_OK:
            return "The stream closed as part of normal operation";
        case END_STREAM_CLOSED:
            return "The stream closed due to a close stream message";
        case END_STREAM_DISCONNECTED:
            return "The stream closed early because the conn was disconnected";
        case END_STREAM_STATE:
            return "The stream closed early because the vbucket state changed";
        default:
            break;
    }
    return std::string{"Status unknown: " + std::to_string(status) +
                       "; this should not have happened!"};
}

void PassiveStream::log(EXTENSION_LOG_LEVEL severity,
                        const char* fmt,
                        ...) const {
    va_list va;
    va_start(va, fmt);
    auto consumer = consumerPtr.lock();
    if (consumer) {
        consumer->getLogger().vlog(severity, fmt, va);
    } else {
        static Logger defaultLogger =
                Logger("DCP (Consumer): **Deleted conn**");
        defaultLogger.vlog(severity, fmt, va);
    }
    va_end(va);
}

void PassiveStream::notifyStreamReady() {
    auto consumer = consumerPtr.lock();
    if (!consumer) {
        return;
    }

    bool inverse = false;
    if (itemsReady.compare_exchange_strong(inverse, true)) {
        consumer->notifyStreamReady(vb_);
    }
}
