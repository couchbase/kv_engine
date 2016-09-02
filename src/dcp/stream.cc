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

#include <platform/checked_snprintf.h>

#include "ep_engine.h"
#include "failover-table.h"
#include "kvstore.h"
#include "statwriter.h"
#include "dcp/backfill-manager.h"
#include "dcp/backfill.h"
#include "dcp/consumer.h"
#include "dcp/producer.h"
#include "dcp/response.h"
#include "dcp/stream.h"
#include "replicationthrottle.h"

#include <memory>

static const char* snapshotTypeToString(snapshot_type_t type) {
    static const char * const snapshotTypes[] = { "none", "disk", "memory" };
    if (type < none || type > memory) {
        throw std::invalid_argument("snapshotTypeToString: type (which is " +
                                    std::to_string(type) +
                                    ") is not a valid snapshot_type_t");
    }
    return snapshotTypes[type];
}

const uint64_t Stream::dcpMaxSeqno = std::numeric_limits<uint64_t>::max();

Stream::Stream(const std::string &name, uint32_t flags, uint32_t opaque,
               uint16_t vb, uint64_t start_seqno, uint64_t end_seqno,
               uint64_t vb_uuid, uint64_t snap_start_seqno,
               uint64_t snap_end_seqno)
    : name_(name), flags_(flags), opaque_(opaque), vb_(vb),
      start_seqno_(start_seqno), end_seqno_(end_seqno), vb_uuid_(vb_uuid),
      snap_start_seqno_(snap_start_seqno),
      snap_end_seqno_(snap_end_seqno),
      state_(STREAM_PENDING), itemsReady(false),
      readyQ_non_meta_items(0),
      readyQueueMemory(0) {
}

Stream::~Stream() {
    // NB: reusing the "unlocked" method without a lock because we're
    // destructing and should not take any locks.
    clear_UNLOCKED();
}

void Stream::clear_UNLOCKED() {
    while (!readyQ.empty()) {
        DcpResponse* resp = readyQ.front();
        popFromReadyQ();
        delete resp;
    }
}

void Stream::pushToReadyQ(DcpResponse* resp)
{
   /* expect streamMutex.ownsLock() == true */
    if (resp) {
        readyQ.push(resp);
        if (!resp->isMetaEvent()) {
            readyQ_non_meta_items++;
        }
        readyQueueMemory.fetch_add(resp->getMessageSize(),
                                   std::memory_order_relaxed);
    }
}

void Stream::popFromReadyQ(void)
{
    /* expect streamMutex.ownsLock() == true */
    if (!readyQ.empty()) {
        const auto& front = readyQ.front();
        if (!front->isMetaEvent()) {
            readyQ_non_meta_items--;
        }
        const uint32_t respSize = front->getMessageSize();
        readyQ.pop();

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
    }
}

uint64_t Stream::getReadyQueueMemory() {
    return readyQueueMemory.load(std::memory_order_relaxed);
}

const char * Stream::stateName(stream_state_t st) const {
    static const char * const stateNames[] = {
        "pending", "backfilling", "in-memory", "takeover-send", "takeover-wait",
        "reading", "dead"
    };
    if (st < STREAM_PENDING || st > STREAM_DEAD) {
        throw std::invalid_argument("Stream::stateName: st (which is " +
                                        std::to_string(st) +
                                        ") is not a valid stream_state_t");
    }
    return stateNames[st];
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
        add_casted_stat(buffer, stateName(state_), add_stat, c);
    } catch (std::exception& error) {
        LOG(EXTENSION_LOG_WARNING,
            "Stream::addStats: Failed to build stats: %s", error.what());
    }
}

ActiveStream::ActiveStream(EventuallyPersistentEngine* e, dcp_producer_t p,
                           const std::string &n, uint32_t flags,
                           uint32_t opaque, uint16_t vb, uint64_t st_seqno,
                           uint64_t en_seqno, uint64_t vb_uuid,
                           uint64_t snap_start_seqno, uint64_t snap_end_seqno)
    :  Stream(n, flags, opaque, vb, st_seqno, en_seqno, vb_uuid,
              snap_start_seqno, snap_end_seqno),
       lastReadSeqnoUnSnapshotted(st_seqno), lastReadSeqno(st_seqno),
       lastSentSeqno(st_seqno), curChkSeqno(st_seqno),
       takeoverState(vbucket_state_pending), backfillRemaining(0),
       itemsFromMemoryPhase(0), firstMarkerSent(false), waitForSnapshot(0),
       engine(e), producer(p), isBackfillTaskRunning(false),
       pendingBackfill(false),
       payloadType((flags & DCP_ADD_STREAM_FLAG_NO_VALUE) ? KEY_ONLY :
                                                            KEY_VALUE),
       lastSentSnapEndSeqno(0), chkptItemsExtractionInProgress(false) {

    const char* type = "";
    if (flags_ & DCP_ADD_STREAM_FLAG_TAKEOVER) {
        type = "takeover ";
        end_seqno_ = dcpMaxSeqno;
    }

    RCPtr<VBucket> vbucket = engine->getVBucket(vb);
    if (vbucket) {
        ReaderLockHolder rlh(vbucket->getStateLock());
        if (vbucket->getState() == vbucket_state_replica) {
            snapshot_info_t info = vbucket->checkpointManager.getSnapshotInfo();
            if (info.range.end > en_seqno) {
                end_seqno_ = info.range.end;
            }
        }
    }

    producer->getLogger().log(EXTENSION_LOG_NOTICE,
        "(vb %" PRIu16 ") Creating %sstream with start seqno %" PRIu64
        " and end seqno %" PRIu64, vb, type, st_seqno, en_seqno);

    backfillItems.memory = 0;
    backfillItems.disk = 0;
    backfillItems.sent = 0;

    type_ = STREAM_ACTIVE;

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
    transitionState(STREAM_DEAD);
}

DcpResponse* ActiveStream::next() {
    LockHolder lh(streamMutex);

    stream_state_t initState = state_;

    DcpResponse* response = NULL;

    bool validTransition = false;
    switch (initState) {
        case STREAM_PENDING:
            validTransition = true;
            break;
        case STREAM_BACKFILLING:
            validTransition = true;
            response = backfillPhase();
            break;
        case STREAM_IN_MEMORY:
            validTransition = true;
            response = inMemoryPhase();
            break;
        case STREAM_TAKEOVER_SEND:
            validTransition = true;
            response = takeoverSendPhase();
            break;
        case STREAM_TAKEOVER_WAIT:
            validTransition = true;
            response = takeoverWaitPhase();
            break;
        case STREAM_READING:
            // Not valid for an active stream.
            break;
        case STREAM_DEAD:
            validTransition = true;
            response = deadPhase();
            break;
    }

    if (!validTransition) {
        throw std::invalid_argument("ActiveStream::transitionState:"
                " invalid state " + std::to_string(state_) + " for stream " +
                producer->logHeader() + " vb " + std::to_string(vb_));
    }

    stream_state_t newState = state_;

    if (newState != STREAM_DEAD && newState != state_ && !response) {
        lh.unlock();
        return next();
    }

    itemsReady.store(response ? true : false);
    return response;
}

void ActiveStream::markDiskSnapshot(uint64_t startSeqno, uint64_t endSeqno) {
    LockHolder lh(streamMutex);
    uint64_t chkCursorSeqno = endSeqno;

    if (state_ != STREAM_BACKFILLING) {
        return;
    }

    startSeqno = std::min(snap_start_seqno_, startSeqno);
    firstMarkerSent = true;

    RCPtr<VBucket> vb = engine->getVBucket(vb_);
    // An atomic read of vbucket state without acquiring the
    // reader lock for state should suffice here.
    if (vb && vb->getState() == vbucket_state_replica) {
        if (end_seqno_ > endSeqno) {
            /* We possibly have items in the open checkpoint
               (incomplete snapshot) */
            snapshot_info_t info = vb->checkpointManager.getSnapshotInfo();
            producer->getLogger().log(EXTENSION_LOG_NOTICE,
                "(vb %" PRIu16 ") Merging backfill and memory snapshot for a "
                "replica vbucket, backfill start seqno %" PRIu64 ", "
                "backfill end seqno %" PRIu64 ", "
                "snapshot end seqno after merge %" PRIu64,
                vb_, startSeqno, endSeqno, info.range.end);
            endSeqno = info.range.end;
        }
    }

    producer->getLogger().log(EXTENSION_LOG_NOTICE,
        "(vb %" PRIu16 ") Sending disk snapshot with start seqno %" PRIu64
        " and end seqno %" PRIu64, vb_, startSeqno, endSeqno);
    pushToReadyQ(new SnapshotMarker(opaque_, vb_, startSeqno, endSeqno,
                                    MARKER_FLAG_DISK));
    lastSentSnapEndSeqno.store(endSeqno, std::memory_order_relaxed);

    if (!vb) {
        endStream(END_STREAM_STATE);
    } else if (!(flags_ & DCP_ADD_STREAM_FLAG_DISKONLY)) {
        // Only re-register the cursor if we still need to get memory snapshots
        CursorRegResult result =
            vb->checkpointManager.registerCursorBySeqno(
                                                name_, chkCursorSeqno,
                                                MustSendCheckpointEnd::NO);
        curChkSeqno = result.first;
    }

    lh.unlock();
    bool inverse = false;
    if (itemsReady.compare_exchange_strong(inverse, true)) {
        producer->notifyStreamReady(vb_, false);
    }
}

bool ActiveStream::backfillReceived(Item* itm, backfill_source_t backfill_source) {
    if (nullptr == itm) {
        return false;
    }
    LockHolder lh(streamMutex);
    if (state_ == STREAM_BACKFILLING) {
        if (!producer->recordBackfillManagerBytesRead(itm->size())) {
            delete itm;
            return false;
        }

        bufferedBackfill.bytes.fetch_add(itm->size());
        bufferedBackfill.items++;

        pushToReadyQ(new MutationResponse(itm, opaque_,
                          prepareExtendedMetaData(itm->getVBucketId(),
                                                  itm->getConflictResMode())));

        lastReadSeqno.store(itm->getBySeqno());
        lh.unlock();
        bool inverse = false;
        if (itemsReady.compare_exchange_strong(inverse, true)) {
            producer->notifyStreamReady(vb_, false);
        }

        if (backfill_source == BACKFILL_FROM_MEMORY) {
            backfillItems.memory++;
        } else {
            backfillItems.disk++;
        }
    } else {
        delete itm;
    }

    return true;
}

void ActiveStream::completeBackfill() {
    {
        LockHolder lh(streamMutex);
        if (state_ == STREAM_BACKFILLING) {
            producer->getLogger().log(EXTENSION_LOG_NOTICE,
                    "(vb %" PRIu16 ") Backfill complete, %" PRIu64 " items "
                    "read from disk, %" PRIu64 " from memory, last seqno read: "
                    "%" PRIu64 "\n", vb_, uint64_t(backfillItems.disk.load()),
                    uint64_t(backfillItems.memory.load()),
                    lastReadSeqno.load());

            isBackfillTaskRunning = false;
            if (pendingBackfill) {
                scheduleBackfill_UNLOCKED(true);
                pendingBackfill = false;
            }
        }
    }

    bool inverse = true;
    isBackfillTaskRunning.compare_exchange_strong(inverse, false);
    inverse = false;
    if (itemsReady.compare_exchange_strong(inverse, true)) {
        producer->notifyStreamReady(vb_, false);
    }
}

void ActiveStream::snapshotMarkerAckReceived() {
    bool inverse = false;
    if (--waitForSnapshot == 0 &&
        itemsReady.compare_exchange_strong(inverse, true)) {
        producer->notifyStreamReady(vb_, true);
    }
}

void ActiveStream::setVBucketStateAckRecieved() {
    LockHolder lh(streamMutex);
    if (state_ == STREAM_TAKEOVER_WAIT) {
        if (takeoverState == vbucket_state_pending) {
            producer->getLogger().log(EXTENSION_LOG_INFO,
                "(vb %" PRIu16 ") Receive ack for set vbucket state to pending "
                "message", vb_);

            takeoverState = vbucket_state_active;
            transitionState(STREAM_TAKEOVER_SEND);
            lh.unlock();

            engine->getEpStore()->setVBucketState(vb_, vbucket_state_dead,
                                                  false, false);
            RCPtr<VBucket> vbucket = engine->getVBucket(vb_);
            producer->getLogger().log(EXTENSION_LOG_NOTICE,
                "(vb %" PRIu16 ") Vbucket marked as dead, last sent seqno: %"
                PRIu64 ", high seqno: %" PRIu64,
                vb_, lastSentSeqno.load(), vbucket->getHighSeqno());
        } else {
            producer->getLogger().log(EXTENSION_LOG_INFO,
                "(vb %" PRIu16 ") Receive ack for set vbucket state to active "
                "message", vb_);
            endStream(END_STREAM_OK);
            lh.unlock();
        }

        bool inverse = false;
        if (itemsReady.compare_exchange_strong(inverse, true)) {
            producer->notifyStreamReady(vb_, true);
        }
    } else {
        producer->getLogger().log(EXTENSION_LOG_WARNING,
            "(vb %" PRIu16 ") Unexpected ack for set vbucket op on stream '%s' "
            "state '%s'", vb_, name_.c_str(), stateName(state_));
    }

}

DcpResponse* ActiveStream::backfillPhase() {
    DcpResponse* resp = nextQueuedItem();

    if (resp && (resp->getEvent() == DCP_MUTATION ||
         resp->getEvent() == DCP_DELETION ||
         resp->getEvent() == DCP_EXPIRATION)) {
        MutationResponse* m = static_cast<MutationResponse*>(resp);
        producer->recordBackfillManagerBytesSent(m->getItem()->size());
        bufferedBackfill.bytes.fetch_sub(m->getItem()->size());
        bufferedBackfill.items--;
        if (backfillRemaining.load(std::memory_order_relaxed) > 0) {
            backfillRemaining.fetch_sub(1, std::memory_order_relaxed);
        }
    }

    if (!isBackfillTaskRunning && readyQ.empty()) {
        backfillRemaining.store(0, std::memory_order_relaxed);
        if (lastReadSeqno.load() >= end_seqno_) {
            endStream(END_STREAM_OK);
        } else if (flags_ & DCP_ADD_STREAM_FLAG_TAKEOVER) {
            transitionState(STREAM_TAKEOVER_SEND);
        } else if (flags_ & DCP_ADD_STREAM_FLAG_DISKONLY) {
            endStream(END_STREAM_OK);
        } else {
            transitionState(STREAM_IN_MEMORY);
        }

        if (!resp) {
            resp = nextQueuedItem();
        }
    }

    return resp;
}

DcpResponse* ActiveStream::inMemoryPhase() {
    if (lastSentSeqno.load() >= end_seqno_) {
        endStream(END_STREAM_OK);
    } else if (readyQ.empty()) {
        if (pendingBackfill) {
            transitionState(STREAM_BACKFILLING);
            pendingBackfill = false;
            return NULL;
        } else if (nextCheckpointItem()) {
            return NULL;
        }
    }

    return nextQueuedItem();
}

DcpResponse* ActiveStream::takeoverSendPhase() {

    RCPtr<VBucket> vb = engine->getVBucket(vb_);
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

    DcpResponse* resp = NULL;
    if (producer->bufferLogInsert(SetVBucketState::baseMsgBytes)) {
        resp = new SetVBucketState(opaque_, vb_, takeoverState);
        transitionState(STREAM_TAKEOVER_WAIT);
    }
    return resp;
}

DcpResponse* ActiveStream::takeoverWaitPhase() {
    return nextQueuedItem();
}

DcpResponse* ActiveStream::deadPhase() {
    DcpResponse* resp = nextQueuedItem();
    if (!resp) {
        producer->getLogger().log(EXTENSION_LOG_NOTICE,
                                  "(vb %" PRIu16 ") Stream closed, "
                                  "%" PRIu64 " items sent from backfill phase, "
                                  "%" PRIu64 " items sent from memory phase, "
                                  "%" PRIu64 " was last seqno sent",
                                  vb_,
                                  uint64_t(backfillItems.sent.load()),
                                  uint64_t(itemsFromMemoryPhase.load()),
                                  lastSentSeqno.load());
    }
    return resp;
}

bool ActiveStream::isCompressionEnabled() {
    return producer->isValueCompressionEnabled();
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

        if ((state_ == STREAM_TAKEOVER_SEND) && takeoverStart != 0) {
            checked_snprintf(buffer, bsize, "%s:stream_%d_takeover_since",
                             name_.c_str(), vb_);
            add_casted_stat(buffer, ep_current_time() - takeoverStart, add_stat,
                            c);
        }
    } catch (std::exception& error) {
        LOG(EXTENSION_LOG_WARNING,
            "ActiveStream::addStats: Failed to build stats: %s", error.what());
    }
}

void ActiveStream::addTakeoverStats(ADD_STAT add_stat, const void *cookie) {
    LockHolder lh(streamMutex);

    RCPtr<VBucket> vb = engine->getVBucket(vb_);
    add_casted_stat("name", name_, add_stat, cookie);
    if (!vb || state_ == STREAM_DEAD) {
        add_casted_stat("status", "completed", add_stat, cookie);
        add_casted_stat("estimate", 0, add_stat, cookie);
        add_casted_stat("backfillRemaining", 0, add_stat, cookie);
        return;
    }

    size_t total = backfillRemaining.load(std::memory_order_relaxed);
    if (state_ == STREAM_BACKFILLING) {
        add_casted_stat("status", "backfilling", add_stat, cookie);
    } else {
        add_casted_stat("status", "in-memory", add_stat, cookie);
    }
    add_casted_stat("backfillRemaining",
                    backfillRemaining.load(std::memory_order_relaxed),
                    add_stat, cookie);

    item_eviction_policy_t iep = engine->getEpStore()->getItemEvictionPolicy();
    size_t vb_items = vb->getNumItems(iep);
    size_t chk_items = vb_items > 0 ?
                vb->checkpointManager.getNumItemsForCursor(name_) : 0;

    size_t del_items = 0;
    try {
        del_items = engine->getEpStore()->getRWUnderlying(vb_)->
                                                        getNumPersistedDeletes(vb_);
    } catch (std::runtime_error& e) {
        producer->getLogger().log(EXTENSION_LOG_WARNING,
            "ActiveStream:addTakeoverStats: exception while getting num persisted "
            "deletes for vbucket:%" PRIu16 " - treating as 0 deletes. "
            "Details: %s", vb_, e.what());
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

DcpResponse* ActiveStream::nextQueuedItem() {
    if (!readyQ.empty()) {
        DcpResponse* response = readyQ.front();
        if (producer->bufferLogInsert(response->getMessageSize())) {
            if (response->getEvent() == DCP_MUTATION ||
                    response->getEvent() == DCP_DELETION ||
                    response->getEvent() == DCP_EXPIRATION) {
                lastSentSeqno.store(
                        dynamic_cast<MutationResponse*>(response)->getBySeqno());

                if (state_ == STREAM_BACKFILLING) {
                    backfillItems.sent++;
                } else {
                    itemsFromMemoryPhase++;
                }
            }
            popFromReadyQ();
            return response;
        }
    }
    return NULL;
}

bool ActiveStream::nextCheckpointItem() {
    RCPtr<VBucket> vbucket = engine->getVBucket(vb_);
    if (vbucket && vbucket->checkpointManager.getNumItemsForCursor(name_) > 0) {
        // schedule this stream to build the next checkpoint
        producer->scheduleCheckpointProcessorTask(this);
        return true;
    } else if (chkptItemsExtractionInProgress) {
        return true;
    }
    return false;
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
        stream_t nextStream = queuePop();
        ActiveStream* stream = static_cast<ActiveStream*>(nextStream.get());

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
        // snooze for 0, essentially yielding and allowing other tasks a go
        snooze(0.0);
    }

    return true;
}

void ActiveStreamCheckpointProcessorTask::wakeup() {
    ExecutorPool::get()->wake(getId());
}

void ActiveStreamCheckpointProcessorTask::schedule(stream_t stream) {
    pushUnique(stream);

    bool expected = false;
    if (notified.compare_exchange_strong(expected, true)) {
        wakeup();
    }
}

void ActiveStreamCheckpointProcessorTask::clearQueues() {
    LockHolder lh(workQueueLock);
    while (!queue.empty()) {
        queue.pop();
    }
    queuedVbuckets.clear();
}

void ActiveStream::nextCheckpointItemTask() {
    RCPtr<VBucket> vbucket = engine->getVBucket(vb_);
    if (vbucket) {
        std::vector<queued_item> items;
        getOutstandingItems(vbucket, items);
        processItems(items);
    } else {
        /* The entity deleting the vbucket must set stream to dead,
           calling setDead(END_STREAM_STATE) will cause deadlock because
           it will try to grab streamMutex which is already acquired at this
           point here */
        return;
    }
}

void ActiveStream::getOutstandingItems(RCPtr<VBucket> &vb,
                                       std::vector<queued_item> &items) {
    // Commencing item processing - set guard flag.
    chkptItemsExtractionInProgress.store(true);

    hrtime_t _begin_ = gethrtime();
    vb->checkpointManager.getAllItemsForCursor(name_, items);
    engine->getEpStats().dcpCursorsGetItemsHisto.add(
                                            (gethrtime() - _begin_) / 1000);

    if (vb->checkpointManager.getNumCheckpoints() > 1) {
        engine->getEpStore()->wakeUpCheckpointRemover();
    }
}

void ActiveStream::processItems(std::vector<queued_item>& items) {
    if (!items.empty()) {
        bool mark = false;
        if (items.front()->getOperation() == queue_op_checkpoint_start) {
            mark = true;
        }

        std::deque<MutationResponse*> mutations;
        std::vector<queued_item>::iterator itr = items.begin();
        for (; itr != items.end(); ++itr) {
            queued_item& qi = *itr;

            if (qi->getOperation() == queue_op_set ||
                qi->getOperation() == queue_op_del) {
                curChkSeqno = qi->getBySeqno();
                lastReadSeqnoUnSnapshotted = qi->getBySeqno();

                mutations.push_back(new MutationResponse(qi, opaque_,
                            prepareExtendedMetaData(qi->getVBucketId(),
                                                    qi->getConflictResMode()),
                            isSendMutationKeyOnlyEnabled() ? KEY_ONLY :
                                                             KEY_VALUE));
            } else if (qi->getOperation() == queue_op_checkpoint_start) {
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

    // Completed item processing - clear guard flag and notify producer.
    chkptItemsExtractionInProgress.store(false);
    producer->notifyStreamReady(vb_, true);
}

void ActiveStream::snapshot(std::deque<MutationResponse*>& items, bool mark) {
    if (items.empty()) {
        return;
    }

    LockHolder lh(streamMutex);

    if ((state_ == STREAM_DEAD) || (state_ == STREAM_BACKFILLING)) {
        // If stream was closed forcefully by the time the checkpoint items
        // retriever task completed, or if we decided to switch the stream to
        // backfill state from in-memory state, none of the acquired mutations
        // should be added on the stream's readyQ. We must drop items in case
        // we switch state from in-memory to backfill because we schedule
        // backfill from lastReadSeqno + 1
        std::deque<MutationResponse *>::iterator itr = items.begin();
        for (; itr != items.end(); ++itr) {
            delete *itr;
        }
        items.clear();
        return;
    }

    /* This assumes that all items in the "items deque" is put onto readyQ */
    lastReadSeqno.store(lastReadSeqnoUnSnapshotted);

    if (isCurrentSnapshotCompleted()) {
        uint32_t flags = MARKER_FLAG_MEMORY;
        uint64_t snapStart = items.front()->getBySeqno();
        uint64_t snapEnd = items.back()->getBySeqno();

        if (mark) {
            flags |= MARKER_FLAG_CHK;
        }

        if (state_ == STREAM_TAKEOVER_SEND) {
            waitForSnapshot++;
            flags |= MARKER_FLAG_ACK;
        }

        if (!firstMarkerSent) {
            snapStart = std::min(snap_start_seqno_, snapStart);
            firstMarkerSent = true;
        }
        pushToReadyQ(new SnapshotMarker(opaque_, vb_, snapStart, snapEnd,
                                        flags));
        lastSentSnapEndSeqno.store(snapEnd, std::memory_order_relaxed);
    }

    std::deque<MutationResponse*>::iterator itemItr;
    for (itemItr = items.begin(); itemItr != items.end(); itemItr++) {
        pushToReadyQ(*itemItr);
    }
}

uint32_t ActiveStream::setDead(end_stream_status_t status) {
    {
        LockHolder lh(streamMutex);
        endStream(status);
    }

    bool inverse = false;
    if (status != END_STREAM_DISCONNECTED &&
        itemsReady.compare_exchange_strong(inverse, true)) {
        producer->notifyStreamReady(vb_, true);
    }
    return 0;
}

void ActiveStream::notifySeqnoAvailable(uint64_t seqno) {
    if (state_ != STREAM_DEAD) {
        bool inverse = false;
        if (itemsReady.compare_exchange_strong(inverse, true)) {
            producer->notifyStreamReady(vb_, true);
        }
    }
}

void ActiveStream::endStream(end_stream_status_t reason) {
    if (state_ != STREAM_DEAD) {
        pendingBackfill = false;
        if (state_ == STREAM_BACKFILLING) {
            // If Stream were in Backfilling state, clear out the
            // backfilled items to clear up the backfill buffer.
            clear_UNLOCKED();
            producer->recordBackfillManagerBytesSent(bufferedBackfill.bytes);
            bufferedBackfill.bytes = 0;
            bufferedBackfill.items = 0;
        }
        transitionState(STREAM_DEAD);
        if (reason != END_STREAM_DISCONNECTED) {
            pushToReadyQ(new StreamEndResponse(opaque_, reason, vb_));
        }
        producer->getLogger().log(EXTENSION_LOG_NOTICE,
                                  "(vb %" PRIu16 ") Stream closing, "
                                  "sent until seqno %" PRIu64 " "
                                  "remaining items %" PRIu64 ", "
                                  "reason: %s",
                                  vb_,
                                  lastSentSeqno.load(),
                                  uint64_t(readyQ_non_meta_items.load()),
                                  getEndStreamStatusStr(reason));
    }
}

void ActiveStream::scheduleBackfill_UNLOCKED(bool reschedule) {
    if (isBackfillTaskRunning) {
        producer->getLogger().log(EXTENSION_LOG_NOTICE,
                                  "(vb %" PRIu16 ") Skipping "
                                  "scheduleBackfill_UNLOCKED; "
                                  "lastReadSeqno %" PRIu64 ", reschedule flag "
                                  ": %s", vb_, lastReadSeqno.load(),
                                  reschedule ? "True" : "False");
        return;
    }

    RCPtr<VBucket> vbucket = engine->getVBucket(vb_);
    if (!vbucket) {
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
            backfillEnd = engine->getEpStore()->getLastPersistedSeqno(vb_);
        } else {
            backfillEnd = end_seqno_;
        }
        tryBackfill = true;
    } else {
        CursorRegResult result =
            vbucket->checkpointManager.registerCursorBySeqno(
                                                name_,
                                                lastReadSeqno.load(),
                                                MustSendCheckpointEnd::NO);
        curChkSeqno = result.first;
        tryBackfill = result.second;

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
        producer->getLogger().log(EXTENSION_LOG_NOTICE,
                                  "(vb %" PRIu16 ") Scheduling backfill "
                                  "from %" PRIu64 " to %" PRIu64 ", reschedule "
                                  "flag : %s", vb_, backfillStart, backfillEnd,
                                  reschedule ? "True" : "False");
        producer->scheduleBackfillManager(this, backfillStart, backfillEnd);
        isBackfillTaskRunning.store(true);
    } else {
        if (flags_ & DCP_ADD_STREAM_FLAG_DISKONLY) {
            endStream(END_STREAM_OK);
        } else if (flags_ & DCP_ADD_STREAM_FLAG_TAKEOVER) {
            transitionState(STREAM_TAKEOVER_SEND);
        } else {
            transitionState(STREAM_IN_MEMORY);
        }
        itemsReady.store(true);
    }
}

void ActiveStream::handleSlowStream()
{
    LockHolder lh(streamMutex);
    switch (state_.load()) {
        case STREAM_BACKFILLING:
            if (isBackfillTaskRunning.load()) {
                /* Drop the existing cursor and set pending backfill */
                dropCheckpointCursor_UNLOCKED();
                pendingBackfill = true;
            } else {
                scheduleBackfill_UNLOCKED(true);
            }
            break;
        case STREAM_IN_MEMORY:
            /* Drop the existing cursor and set pending backfill */
            dropCheckpointCursor_UNLOCKED();
            pendingBackfill = true;
            break;
        case STREAM_TAKEOVER_SEND:
            /* To be handled later if needed */
        case STREAM_TAKEOVER_WAIT:
            /* To be handled later if needed */
        case STREAM_DEAD:
            /* To be handled later if needed */
            break;
        case STREAM_PENDING:
        case STREAM_READING:
            throw std::logic_error("ActiveStream::handleSlowStream: "
                                   "called with state " +
                                   std::to_string(state_.load()) + " . " +
                                   "for stream " + producer->logHeader() +
                                   "; vb " + std::to_string(vb_));
    }
}

const char* ActiveStream::getEndStreamStatusStr(end_stream_status_t status)
{
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
    }
    std::string msg("Status unknown: " + std::to_string(status) +
                    "; this should not have happened!");
    return msg.c_str();
}

void ActiveStream::transitionState(stream_state_t newState) {
    producer->getLogger().log(EXTENSION_LOG_DEBUG,
                              "(vb %d) Transitioning from %s to %s",
                              vb_, stateName(state_), stateName(newState));

    if (state_ == newState) {
        return;
    }

    bool validTransition = false;
    switch (state_.load()) {
        case STREAM_PENDING:
            if (newState == STREAM_BACKFILLING || newState == STREAM_DEAD) {
                validTransition = true;
            }
            break;
        case STREAM_BACKFILLING:
            if(newState == STREAM_IN_MEMORY ||
               newState == STREAM_TAKEOVER_SEND ||
               newState == STREAM_DEAD) {
                validTransition = true;
            }
            break;
        case STREAM_IN_MEMORY:
            if (newState == STREAM_BACKFILLING || newState == STREAM_DEAD) {
                validTransition = true;
            }
            break;
        case STREAM_TAKEOVER_SEND:
            if (newState == STREAM_TAKEOVER_WAIT || newState == STREAM_DEAD) {
                validTransition = true;
            }
            break;
        case STREAM_TAKEOVER_WAIT:
            if (newState == STREAM_TAKEOVER_SEND || newState == STREAM_DEAD) {
                validTransition = true;
            }
            break;
        case STREAM_READING:
            // Active stream should never be in READING state.
            validTransition = false;
            break;
        case STREAM_DEAD:
            // Once DEAD, no other transitions should occur.
            validTransition = false;
            break;
    }

    if (!validTransition) {
        throw std::invalid_argument("ActiveStream::transitionState:"
                " newState (which is " + std::to_string(newState) +
                ") is not valid for current state (which is " +
                std::to_string(state_) + ")");
    }

    stream_state_t oldState = state_.load();
    state_ = newState;

    switch (newState) {
        case STREAM_BACKFILLING:
            if (STREAM_PENDING == oldState) {
                scheduleBackfill_UNLOCKED(false /* reschedule */);
            } else if (STREAM_IN_MEMORY == oldState) {
                scheduleBackfill_UNLOCKED(true /* reschedule */);
            }
            break;
        case STREAM_IN_MEMORY:
            // Check if the producer has sent up till the last requested
            // sequence number already, if not - move checkpoint items into
            // the ready queue.
            if (lastSentSeqno.load() >= end_seqno_) {
                // Stream transitioning to DEAD state
                endStream(END_STREAM_OK);
                bool inverse = false;
                if (itemsReady.compare_exchange_strong(inverse, true)) {
                    producer->notifyStreamReady(vb_, false);
                }
            } else {
                nextCheckpointItem();
            }
            break;
        case STREAM_TAKEOVER_SEND:
            takeoverStart = ep_current_time();
            nextCheckpointItem();
            break;
        case STREAM_DEAD:
            {
                RCPtr<VBucket> vb = engine->getVBucket(vb_);
                if (vb) {
                    vb->checkpointManager.removeCursor(name_);
                }
                break;
            }
        case STREAM_TAKEOVER_WAIT:
        case STREAM_PENDING:
            break;
        case STREAM_READING:
            throw std::logic_error("ActiveStream::transitionState:"
                    " newState can't be " + std::to_string(newState) + "!");
    }
}

size_t ActiveStream::getItemsRemaining() {
    RCPtr<VBucket> vbucket = engine->getVBucket(vb_);

    if (!vbucket || state_ == STREAM_DEAD) {
        return 0;
    }

    // Items remaining is the sum of:
    // (a) Items outstanding in checkpoints
    // (b) Items pending in our readyQ, excluding any meta items.
    return vbucket->checkpointManager.getNumItemsForCursor(name_) +
            readyQ_non_meta_items;
}

uint64_t ActiveStream::getLastSentSeqno() {
    return lastSentSeqno.load();
}

ExtendedMetaData* ActiveStream::prepareExtendedMetaData(uint16_t vBucketId,
                                                        uint8_t conflictResMode)
{
    ExtendedMetaData *emd = NULL;
    if (producer->isExtMetaDataEnabled()) {
        RCPtr<VBucket> vb = engine->getVBucket(vBucketId);
        if (vb && vb->getTimeSyncConfig() == time_sync_t::ENABLED_WITH_DRIFT) {
            int64_t adjustedTime = gethrtime() + vb->getDriftCounter();
            emd = new ExtendedMetaData(adjustedTime, conflictResMode);
        } else {
            emd = new ExtendedMetaData(conflictResMode);
        }
    }
    return emd;
}

const Logger& ActiveStream::getLogger() const
{
    return producer->getLogger();
}

bool ActiveStream::isSendMutationKeyOnlyEnabled() const
{
    return (KEY_ONLY == payloadType);
}

bool ActiveStream::isCurrentSnapshotCompleted() const
{
    RCPtr<VBucket> vbucket = engine->getVBucket(vb_);
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

void ActiveStream::dropCheckpointCursor_UNLOCKED()
{
    RCPtr<VBucket> vbucket = engine->getVBucket(vb_);
    if (!vbucket) {
        endStream(END_STREAM_STATE);
    }
    /* Drop the existing cursor */
    vbucket->checkpointManager.removeCursor(name_);
}

NotifierStream::NotifierStream(EventuallyPersistentEngine* e, dcp_producer_t p,
                               const std::string &name, uint32_t flags,
                               uint32_t opaque, uint16_t vb, uint64_t st_seqno,
                               uint64_t en_seqno, uint64_t vb_uuid,
                               uint64_t snap_start_seqno,
                               uint64_t snap_end_seqno)
    : Stream(name, flags, opaque, vb, st_seqno, en_seqno, vb_uuid,
             snap_start_seqno, snap_end_seqno),
      producer(p) {
    LockHolder lh(streamMutex);
    RCPtr<VBucket> vbucket = e->getVBucket(vb_);
    if (vbucket && static_cast<uint64_t>(vbucket->getHighSeqno()) > st_seqno) {
        pushToReadyQ(new StreamEndResponse(opaque_, END_STREAM_OK, vb_));
        transitionState(STREAM_DEAD);
        itemsReady.store(true);
    }

    type_ = STREAM_NOTIFIER;

    producer->getLogger().log(EXTENSION_LOG_NOTICE,
        "(vb %d) stream created with start seqno %" PRIu64 " and end seqno %"
        PRIu64, vb, st_seqno, en_seqno);
}

uint32_t NotifierStream::setDead(end_stream_status_t status) {
    LockHolder lh(streamMutex);
    if (state_ != STREAM_DEAD) {
        transitionState(STREAM_DEAD);
        if (status != END_STREAM_DISCONNECTED) {
            pushToReadyQ(new StreamEndResponse(opaque_, status, vb_));
            lh.unlock();
            bool inverse = false;
            if (itemsReady.compare_exchange_strong(inverse, true)) {
                producer->notifyStreamReady(vb_, true);
            }
        }
    }
    return 0;
}

void NotifierStream::notifySeqnoAvailable(uint64_t seqno) {
    LockHolder lh(streamMutex);
    if (state_ != STREAM_DEAD && start_seqno_ < seqno) {
        pushToReadyQ(new StreamEndResponse(opaque_, END_STREAM_OK, vb_));
        transitionState(STREAM_DEAD);
        lh.unlock();
        bool inverse = false;
        if (itemsReady.compare_exchange_strong(inverse, true)) {
            producer->notifyStreamReady(vb_, true);
        }
    }
}

DcpResponse* NotifierStream::next() {
    LockHolder lh(streamMutex);

    if (readyQ.empty()) {
        itemsReady.store(false);
        return NULL;
    }

    DcpResponse* response = readyQ.front();
    if (producer->bufferLogInsert(response->getMessageSize())) {
        popFromReadyQ();
    } else {
        response = NULL;
    }

    return response;
}

void NotifierStream::transitionState(stream_state_t newState) {
    producer->getLogger().log(EXTENSION_LOG_DEBUG,
        "(vb %d) Transitioning from %s to %s", vb_,
        stateName(state_), stateName(newState));

    if (state_ == newState) {
        return;
    }

    bool validTransition = false;
    switch (state_.load()) {
        case STREAM_PENDING:
            if (newState == STREAM_DEAD) {
                validTransition = true;
            }
            break;

        case STREAM_BACKFILLING:
        case STREAM_IN_MEMORY:
        case STREAM_TAKEOVER_SEND:
        case STREAM_TAKEOVER_WAIT:
        case STREAM_READING:
        case STREAM_DEAD:
            // No other state transitions are valid for a notifier stream.
            break;
    }

    if (!validTransition) {
        throw std::invalid_argument("NotifierStream::transitionState:"
                " newState (which is " + std::to_string(newState) +
                ") is not valid for current state (which is " +
                std::to_string(state_) + ")");
    }
    state_ = newState;
}

PassiveStream::PassiveStream(EventuallyPersistentEngine* e, dcp_consumer_t c,
                             const std::string &name, uint32_t flags,
                             uint32_t opaque, uint16_t vb, uint64_t st_seqno,
                             uint64_t en_seqno, uint64_t vb_uuid,
                             uint64_t snap_start_seqno, uint64_t snap_end_seqno,
                             uint64_t vb_high_seqno)
    : Stream(name, flags, opaque, vb, st_seqno, en_seqno, vb_uuid,
             snap_start_seqno, snap_end_seqno),
      engine(e), consumer(c), last_seqno(vb_high_seqno), cur_snapshot_start(0),
      cur_snapshot_end(0), cur_snapshot_type(none), cur_snapshot_ack(false) {
    LockHolder lh(streamMutex);
    pushToReadyQ(new StreamRequest(vb, opaque, flags, st_seqno, en_seqno,
                                  vb_uuid, snap_start_seqno, snap_end_seqno));
    itemsReady.store(true);
    type_ = STREAM_PASSIVE;

    const char* type = (flags & DCP_ADD_STREAM_FLAG_TAKEOVER) ? "takeover" : "";
    consumer->getLogger().log(EXTENSION_LOG_NOTICE,
        "(vb %" PRId16 ") Attempting to add %s stream"
        " with start seqno %" PRIu64 ", end seqno %" PRIu64 ","
        " vbucket uuid %" PRIu64 ", snap start seqno %" PRIu64 ","
        " snap end seqno %" PRIu64 ", and vb_high_seqno %" PRIu64 "",
        vb, type, st_seqno, en_seqno, vb_uuid,
        snap_start_seqno, snap_end_seqno, vb_high_seqno);
}

PassiveStream::~PassiveStream() {
    uint32_t unackedBytes = clearBuffer_UNLOCKED();
    if (transitionState(STREAM_DEAD)) {
        // Destructed a "live" stream, log it.
        consumer->getLogger().log(EXTENSION_LOG_NOTICE,
            "(vb %" PRId16 ") Destructing stream."
            " last_seqno is %" PRIu64 ", unAckedBytes is %" PRIu32 ".",
            vb_, last_seqno.load(), unackedBytes);
    }
}

uint32_t PassiveStream::setDead(end_stream_status_t status) {
    /* Hold buffer lock so that we clear out all items before we set the stream
       to dead state. We do not want to add any new message to the buffer or
       process any items in the buffer once we set the stream state to dead. */
    std::lock_guard<std::mutex> lg(buffer.bufMutex);
    uint32_t unackedBytes = clearBuffer_UNLOCKED();
    bool killed = false;

    LockHolder slh(streamMutex);
    if (transitionState(STREAM_DEAD)) {
        killed = true;
    }

    if (killed) {
        EXTENSION_LOG_LEVEL logLevel = EXTENSION_LOG_NOTICE;
        if (END_STREAM_DISCONNECTED == status) {
            logLevel = EXTENSION_LOG_WARNING;
        }
        consumer->getLogger().log(logLevel,
            "(vb %" PRId16 ") Setting stream to dead state, last_seqno is %"
            PRIu64 ", unAckedBytes is %" PRIu32 ", status is %s",
            vb_, last_seqno.load(), unackedBytes, getEndStreamStatusStr(status));
    }
    return unackedBytes;
}

void PassiveStream::acceptStream(uint16_t status, uint32_t add_opaque) {
    LockHolder lh(streamMutex);
    if (state_ == STREAM_PENDING) {
        if (status == ENGINE_SUCCESS) {
            transitionState(STREAM_READING);
        } else {
            transitionState(STREAM_DEAD);
        }
        pushToReadyQ(new AddStreamResponse(add_opaque, opaque_, status));
        lh.unlock();
        bool inverse = false;
        if (itemsReady.compare_exchange_strong(inverse, true)) {
            consumer->notifyStreamReady(vb_);
        }
    }
}

void PassiveStream::reconnectStream(RCPtr<VBucket> &vb,
                                    uint32_t new_opaque,
                                    uint64_t start_seqno) {
    vb_uuid_ = vb->failovers->getLatestEntry().vb_uuid;

    snapshot_info_t info = vb->checkpointManager.getSnapshotInfo();
    if (info.range.end == info.start) {
        info.range.start = info.start;
    }

    snap_start_seqno_ = info.range.start;
    start_seqno_ = info.start;
    snap_end_seqno_ = info.range.end;

    consumer->getLogger().log(EXTENSION_LOG_NOTICE,
        "(vb %d) Attempting to reconnect stream with opaque %" PRIu32
        ", start seq no %" PRIu64 ", end seq no %" PRIu64
        ", snap start seqno %" PRIu64 ", and snap end seqno %" PRIu64,
        vb_, new_opaque, start_seqno, end_seqno_,
        snap_start_seqno_, snap_end_seqno_);

    LockHolder lh(streamMutex);
    last_seqno.store(start_seqno);
    pushToReadyQ(new StreamRequest(vb_, new_opaque, flags_, start_seqno,
                                  end_seqno_, vb_uuid_, snap_start_seqno_,
                                  snap_end_seqno_));
    lh.unlock();
    bool inverse = false;
    if (itemsReady.compare_exchange_strong(inverse, true)) {
        consumer->notifyStreamReady(vb_);
    }
}

ENGINE_ERROR_CODE PassiveStream::messageReceived(std::unique_ptr<DcpResponse> dcpResponse) {
    if (!dcpResponse) {
        return ENGINE_EINVAL;
    }

    if (state_ == STREAM_DEAD) {
        return ENGINE_KEY_ENOENT;
    }

    switch (dcpResponse->getEvent()) {
        case DCP_MUTATION:
        case DCP_DELETION:
        case DCP_EXPIRATION:
        {
            uint64_t bySeqno =
                static_cast<MutationResponse*>(dcpResponse.get())->getBySeqno();
            if (bySeqno <= last_seqno.load()) {
                consumer->getLogger().log(EXTENSION_LOG_WARNING,
                    "(vb %d) Erroneous (out of sequence) mutation received, "
                    "with opaque: %" PRIu32 ", its seqno (%" PRIu64 ") is not "
                    "greater than last received seqno (%" PRIu64 "); "
                    "Dropping mutation!",
                    vb_, opaque_, bySeqno, last_seqno.load());
                return ENGINE_ERANGE;
            }
            last_seqno.store(bySeqno);
            break;
        }
        case DCP_SNAPSHOT_MARKER:
        {
            auto s = static_cast<SnapshotMarker*>(dcpResponse.get());
            uint64_t snapStart = s->getStartSeqno();
            uint64_t snapEnd = s->getEndSeqno();
            if (snapStart < last_seqno.load() && snapEnd <= last_seqno.load()) {
                consumer->getLogger().log(EXTENSION_LOG_WARNING,
                    "(vb %d) Erroneous snapshot marker received, with "
                    "opaque: %" PRIu32 ", its start "
                    "(%" PRIu64 "), and end (%" PRIu64 ") are less than last "
                    "received seqno (%" PRIu64 "); Dropping marker!",
                    vb_, opaque_, snapStart, snapEnd, last_seqno.load());
                return ENGINE_ERANGE;
            }
            break;
        }
        case DCP_SET_VBUCKET:
        case DCP_STREAM_END:
        {
            /* No validations necessary */
            break;
        }
        default:
        {
            consumer->getLogger().log(EXTENSION_LOG_WARNING,
                "(vb %d) Unknown DCP op received: %d; Disconnecting connection..",
                vb_, dcpResponse->getEvent());
            return ENGINE_DISCONNECT;
        }
    }

    if (engine->getReplicationThrottle().shouldProcess() && buffer.empty()) {
        /* Process the response here itself rather than buffering it */
        ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
        switch (dcpResponse->getEvent()) {
            case DCP_MUTATION:
                ret = processMutation(static_cast<MutationResponse*>(dcpResponse.get()));
                break;
            case DCP_DELETION:
            case DCP_EXPIRATION:
                ret = processDeletion(static_cast<MutationResponse*>(dcpResponse.get()));
                break;
            case DCP_SNAPSHOT_MARKER:
                processMarker(static_cast<SnapshotMarker*>(dcpResponse.get()));
                break;
            case DCP_SET_VBUCKET:
                processSetVBucketState(static_cast<SetVBucketState*>(dcpResponse.get()));
                break;
            case DCP_STREAM_END:
                {
                    LockHolder lh(streamMutex);
                    transitionState(STREAM_DEAD);
                }
                break;
            default:
                // Above switch should've returned DISCONNECT, throw an exception
                throw std::logic_error("PassiveStream::messageReceived: (vb " +
                                       std::to_string(vb_) +
                                       ") received unknown message type " +
                                       std::to_string(dcpResponse->getEvent()));
        }
        if (ret != ENGINE_TMPFAIL && ret != ENGINE_ENOMEM) {
            return ret;
        }
    }

    // Only buffer if the stream is not dead
    if (state_.load() != STREAM_DEAD) {
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
    bool failed = false;
    while (count < batchSize && !buffer.messages.empty()) {
        ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
        /* If the stream is in dead state we should not process any remaining
           items in the buffer, we should rather clear them */
        if (state_ == STREAM_DEAD) {
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
            case DCP_MUTATION:
                ret = processMutation(static_cast<MutationResponse*>(response.get()));
                break;
            case DCP_DELETION:
            case DCP_EXPIRATION:
                ret = processDeletion(static_cast<MutationResponse*>(response.get()));
                break;
            case DCP_SNAPSHOT_MARKER:
                processMarker(static_cast<SnapshotMarker*>(response.get()));
                break;
            case DCP_SET_VBUCKET:
                processSetVBucketState(static_cast<SetVBucketState*>(response.get()));
                break;
            case DCP_STREAM_END:
                {
                    LockHolder lh(streamMutex);
                    transitionState(STREAM_DEAD);
                }
                break;
            default:
                consumer->getLogger().log(EXTENSION_LOG_WARNING,
                                          "PassiveStream::processBufferedMessages:"
                                          "(vb %" PRIu16 ") PassiveStream ignoring "
                                          "unknown message type %d",
                                          vb_, response->getEvent());
                continue;
        }

        if (ret == ENGINE_TMPFAIL || ret == ENGINE_ENOMEM) {
            failed = true;
        }

        // Re-acquire bufMutex so that
        // 1) we can update the buffer
        // 2) safely re-check the while conditional statement
        lh.lock();

        // If we failed and the stream is not dead, stash the DcpResponse at the
        // front of the queue and break the loop.
        if (failed && state_.load() != STREAM_DEAD) {
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
        return cannot_process;
    }

    return all_processed;
}

ENGINE_ERROR_CODE PassiveStream::processMutation(MutationResponse* mutation) {
    RCPtr<VBucket> vb = engine->getVBucket(vb_);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    if (mutation->getBySeqno() < cur_snapshot_start.load() ||
        mutation->getBySeqno() > cur_snapshot_end.load()) {
        consumer->getLogger().log(EXTENSION_LOG_WARNING,
            "(vb %d) Erroneous mutation [sequence "
            "number does not fall in the expected snapshot range : "
            "{snapshot_start (%" PRIu64 ") <= seq_no (%" PRIu64 ") <= "
            "snapshot_end (%" PRIu64 ")]; Dropping the mutation!",
            vb_, cur_snapshot_start.load(),
            mutation->getBySeqno(), cur_snapshot_end.load());
        return ENGINE_ERANGE;
    }

    // MB-17517: Check for the incoming item's CAS validity. We /shouldn't/
    // receive anything without a valid CAS, however given that versions without
    // this check may send us "bad" CAS values, we should regenerate them (which
    // is better than rejecting the data entirely).
    if (!Item::isValidCas(mutation->getItem()->getCas())) {
        LOG(EXTENSION_LOG_WARNING,
            "%s Invalid CAS (0x%" PRIx64 ") received for mutation {vb:%" PRIu16
            ", seqno:%" PRId64 "}. Regenerating new CAS",
            consumer->logHeader(),
            mutation->getItem()->getCas(), vb_,
            mutation->getItem()->getBySeqno());
        mutation->getItem()->setCas();
    }

    ENGINE_ERROR_CODE ret;
    if (vb->isBackfillPhase()) {
        ret = engine->getEpStore()->addTAPBackfillItem(*mutation->getItem(),
                                                       false,
                                                       mutation->getExtMetaData());
    } else {
        ret = engine->getEpStore()->setWithMeta(*mutation->getItem(), 0, NULL,
                                                consumer->getCookie(), true,
                                                true, false,
                                                mutation->getExtMetaData(),
                                                true);
    }

    if (ret != ENGINE_SUCCESS) {
        consumer->getLogger().log(EXTENSION_LOG_WARNING,
            "Got an error code %d while trying to process mutation", ret);
    } else {
        handleSnapshotEnd(vb, mutation->getBySeqno());
    }

    return ret;
}

ENGINE_ERROR_CODE PassiveStream::processDeletion(MutationResponse* deletion) {
    RCPtr<VBucket> vb = engine->getVBucket(vb_);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    if (deletion->getBySeqno() < cur_snapshot_start.load() ||
        deletion->getBySeqno() > cur_snapshot_end.load()) {
        consumer->getLogger().log(EXTENSION_LOG_WARNING,
            "(vb %d) Erroneous deletion [sequence "
            "number does not fall in the expected snapshot range : "
            "{snapshot_start (%" PRIu64 ") <= seq_no (%" PRIu64 ") <= "
            "snapshot_end (%" PRIu64 ")]; Dropping the deletion!",
            vb_, cur_snapshot_start.load(),
            deletion->getBySeqno(), cur_snapshot_end.load());
        return ENGINE_ERANGE;
    }

    uint64_t delCas = 0;
    ENGINE_ERROR_CODE ret;
    ItemMetaData meta = deletion->getItem()->getMetaData();

    // MB-17517: Check for the incoming item's CAS validity.
    if (!Item::isValidCas(meta.cas)) {
        LOG(EXTENSION_LOG_WARNING,
            "%s Invalid CAS (0x%" PRIx64 ") received for deletion {vb:%" PRIu16
            ", seqno:%" PRId64 "}. Regenerating new CAS",
            consumer->logHeader(), meta.cas, vb_, deletion->getBySeqno());
        meta.cas = Item::nextCas();
    }

    ret = engine->getEpStore()->deleteWithMeta(deletion->getItem()->getKey(),
                                               &delCas, NULL, deletion->getVBucket(),
                                               consumer->getCookie(), true,
                                               &meta, vb->isBackfillPhase(),
                                               false, deletion->getBySeqno(),
                                               deletion->getExtMetaData(),
                                               true);
    if (ret == ENGINE_KEY_ENOENT) {
        ret = ENGINE_SUCCESS;
    }

    if (ret != ENGINE_SUCCESS) {
        consumer->getLogger().log(EXTENSION_LOG_WARNING,
            "Got an error code %d while trying to process deletion", ret);
    } else {
        handleSnapshotEnd(vb, deletion->getBySeqno());
    }

    return ret;
}

void PassiveStream::processMarker(SnapshotMarker* marker) {
    RCPtr<VBucket> vb = engine->getVBucket(vb_);

    cur_snapshot_start.store(marker->getStartSeqno());
    cur_snapshot_end.store(marker->getEndSeqno());
    cur_snapshot_type.store((marker->getFlags() & MARKER_FLAG_DISK) ? disk : memory);

    if (vb) {
        if (marker->getFlags() & MARKER_FLAG_DISK && vb->getHighSeqno() == 0) {
            vb->setBackfillPhase(true);
            vb->checkpointManager.setBackfillPhase(cur_snapshot_start.load(),
                                                   cur_snapshot_end.load());
        } else {
            if (marker->getFlags() & MARKER_FLAG_CHK ||
                vb->checkpointManager.getOpenCheckpointId() == 0) {
                vb->checkpointManager.createSnapshot(cur_snapshot_start.load(),
                                                     cur_snapshot_end.load());
            } else {
                vb->checkpointManager.updateCurrentSnapshotEnd(cur_snapshot_end.load());
            }
            vb->setBackfillPhase(false);
        }

        if (marker->getFlags() & MARKER_FLAG_ACK) {
            cur_snapshot_ack = true;
        }
    }
}

void PassiveStream::processSetVBucketState(SetVBucketState* state) {
    engine->getEpStore()->setVBucketState(vb_, state->getState(), true);

    LockHolder lh (streamMutex);
    pushToReadyQ(new SetVBucketStateResponse(opaque_, ENGINE_SUCCESS));
    lh.unlock();
    bool inverse = false;
    if (itemsReady.compare_exchange_strong(inverse, true)) {
        consumer->notifyStreamReady(vb_);
    }
}

void PassiveStream::handleSnapshotEnd(RCPtr<VBucket>& vb, uint64_t byseqno) {
    if (byseqno == cur_snapshot_end.load()) {
        if (cur_snapshot_type.load() == disk && vb->isBackfillPhase()) {
            vb->setBackfillPhase(false);
            uint64_t id = vb->checkpointManager.getOpenCheckpointId() + 1;
            vb->checkpointManager.checkAndAddNewCheckpoint(id, vb);
        } else {
            size_t mem_threshold = engine->getEpStats().mem_high_wat.load();
            size_t mem_used = engine->getEpStats().getTotalMemoryUsed();
            /* We want to add a new replica checkpoint if the mem usage is above
               high watermark (85%) */
            if (mem_threshold < mem_used) {
                uint64_t id = vb->checkpointManager.getOpenCheckpointId() + 1;
                vb->checkpointManager.checkAndAddNewCheckpoint(id, vb);
            }
        }

        if (cur_snapshot_ack) {
            LockHolder lh(streamMutex);
            pushToReadyQ(new SnapshotMarkerResponse(opaque_, ENGINE_SUCCESS));
            lh.unlock();
            bool inverse = false;
            if (itemsReady.compare_exchange_strong(inverse, true)) {
                consumer->notifyStreamReady(vb_);
            }
            cur_snapshot_ack = false;
        }
        cur_snapshot_type.store(none);
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
        add_casted_stat(buf, snapshotTypeToString(cur_snapshot_type.load()),
                        add_stat, c);

        if (cur_snapshot_type.load() != none) {
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

DcpResponse* PassiveStream::next() {
    LockHolder lh(streamMutex);

    if (readyQ.empty()) {
        itemsReady.store(false);
        return NULL;
    }

    DcpResponse* response = readyQ.front();
    popFromReadyQ();
    return response;
}

uint32_t PassiveStream::clearBuffer_UNLOCKED() {
    uint32_t unackedBytes = buffer.bytes;
    buffer.messages.clear();
    buffer.bytes = 0;
    return unackedBytes;
}

bool PassiveStream::transitionState(stream_state_t newState) {
    consumer->getLogger().log(EXTENSION_LOG_DEBUG,
        "(vb %d) Transitioning from %s to %s",
        vb_, stateName(state_), stateName(newState));

    if (state_ == newState) {
        return false;
    }

    bool validTransition = false;
    switch (state_.load()) {
        case STREAM_PENDING:
            if (newState == STREAM_READING || newState == STREAM_DEAD) {
                validTransition = true;
            }
            break;

        case STREAM_BACKFILLING:
        case STREAM_IN_MEMORY:
        case STREAM_TAKEOVER_SEND:
        case STREAM_TAKEOVER_WAIT:
            // Not valid for passive streams
            break;

        case STREAM_READING:
            if (newState == STREAM_PENDING || newState == STREAM_DEAD) {
                validTransition = true;
            }
            break;

        case STREAM_DEAD:
            // Once 'dead' shouldn't transition away from it.
            break;
    }

    if (!validTransition) {
        throw std::invalid_argument("PassiveStream::transitionState:"
                " newState (which is" + std::to_string(newState) +
                ") is not valid for current state (which is " +
                std::to_string(state_) + ")");
    }

    state_ = newState;
    return true;
}

const char* PassiveStream::getEndStreamStatusStr(end_stream_status_t status)
{
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
    std::string msg("Status unknown: " + std::to_string(status) +
                    "; this should not have happened!");
    return msg.c_str();
}
