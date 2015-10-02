/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
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

#include "ep_engine.h"
#include "failover-table.h"
#include "kvstore.h"
#include "statwriter.h"
#include "dcp-backfill-manager.h"
#include "dcp-backfill.h"
#include "dcp-consumer.h"
#include "dcp-producer.h"
#include "dcp-response.h"
#include "dcp-stream.h"

static const char* snapshotTypeToString(snapshot_type_t type) {
    static const char * const snapshotTypes[] = { "none", "disk", "memory" };
    cb_assert(type >= none && type <= memory);
    return snapshotTypes[type];
}

const uint64_t Stream::dcpMaxSeqno = std::numeric_limits<uint64_t>::max();
const size_t PassiveStream::batchSize = 10;

Stream::Stream(const std::string &name, uint32_t flags, uint32_t opaque,
               uint16_t vb, uint64_t start_seqno, uint64_t end_seqno,
               uint64_t vb_uuid, uint64_t snap_start_seqno,
               uint64_t snap_end_seqno)
    : name_(name), flags_(flags), opaque_(opaque), vb_(vb),
      start_seqno_(start_seqno), end_seqno_(end_seqno), vb_uuid_(vb_uuid),
      snap_start_seqno_(snap_start_seqno),
      snap_end_seqno_(snap_end_seqno),
      state_(STREAM_PENDING), itemsReady(false), readyQueueMemory(0) {
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
    if (resp) {
        readyQ.push(resp);
        readyQueueMemory += resp->getMessageSize();
    }
}

void Stream::popFromReadyQ(void)
{
    if (!readyQ.empty()) {
        uint32_t respSize = readyQ.front()->getMessageSize();
        readyQ.pop();
        /* Decrement the readyQ size */
        if ((readyQueueMemory - respSize) <= readyQueueMemory) {
            readyQueueMemory -= respSize;
        } else {
            LOG(EXTENSION_LOG_DEBUG, "readyQ size for stream %s (vb %d)"
                "underflow, likely wrong stat calculation! curr size: %llu;"
                "new size: %d", name_.c_str(), getVBucket(), readyQueueMemory,
                respSize);
            readyQueueMemory = 0;
        }
    }
}

uint64_t Stream::getReadyQueueMemory() {
    return readyQueueMemory;
}

const char * Stream::stateName(stream_state_t st) const {
    static const char * const stateNames[] = {
        "pending", "backfilling", "in-memory", "takeover-send", "takeover-wait",
        "reading", "dead"
    };
    cb_assert(st >= STREAM_PENDING && st <= STREAM_DEAD);
    return stateNames[st];
}

void Stream::addStats(ADD_STAT add_stat, const void *c) {
    const int bsize = 1024;
    char buffer[bsize];
    snprintf(buffer, bsize, "%s:stream_%d_flags", name_.c_str(), vb_);
    add_casted_stat(buffer, flags_, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_opaque", name_.c_str(), vb_);
    add_casted_stat(buffer, opaque_, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_start_seqno", name_.c_str(), vb_);
    add_casted_stat(buffer, start_seqno_, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_end_seqno", name_.c_str(), vb_);
    add_casted_stat(buffer, end_seqno_, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_vb_uuid", name_.c_str(), vb_);
    add_casted_stat(buffer, vb_uuid_, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_snap_start_seqno", name_.c_str(), vb_);
    add_casted_stat(buffer, snap_start_seqno_, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_snap_end_seqno", name_.c_str(), vb_);
    add_casted_stat(buffer, snap_end_seqno_, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_state", name_.c_str(), vb_);
    add_casted_stat(buffer, stateName(state_), add_stat, c);
}

ActiveStream::ActiveStream(EventuallyPersistentEngine* e, DcpProducer* p,
                           const std::string &n, uint32_t flags,
                           uint32_t opaque, uint16_t vb, uint64_t st_seqno,
                           uint64_t en_seqno, uint64_t vb_uuid,
                           uint64_t snap_start_seqno, uint64_t snap_end_seqno)
    :  Stream(n, flags, opaque, vb, st_seqno, en_seqno, vb_uuid,
              snap_start_seqno, snap_end_seqno),
       lastReadSeqno(st_seqno), lastSentSeqno(st_seqno), curChkSeqno(st_seqno),
       takeoverState(vbucket_state_pending), backfillRemaining(0),
       itemsFromMemoryPhase(0), firstMarkerSent(false), waitForSnapshot(0),
       engine(e), producer(p), isBackfillTaskRunning(false) {

    const char* type = "";
    if (flags_ & DCP_ADD_STREAM_FLAG_TAKEOVER) {
        type = "takeover ";
        end_seqno_ = dcpMaxSeqno;
    }

    if (start_seqno_ >= end_seqno_) {
        endStream(END_STREAM_OK);
        itemsReady = true;
    }

    backfillItems.memory = 0;
    backfillItems.disk = 0;
    backfillItems.sent = 0;

    type_ = STREAM_ACTIVE;

    bufferedBackfill.bytes = 0;
    bufferedBackfill.items = 0;

    LOG(EXTENSION_LOG_WARNING, "%s (vb %d) %sstream created with start seqno "
        "%llu and end seqno %llu", producer->logHeader(), vb, type, st_seqno,
        en_seqno);
}

ActiveStream::~ActiveStream() {
    LockHolder lh(streamMutex);
    transitionState(STREAM_DEAD);
    clear_UNLOCKED();
    producer->getBackfillManager()->bytesSent(bufferedBackfill.bytes);
    bufferedBackfill.bytes = 0;
    bufferedBackfill.items = 0;
}

DcpResponse* ActiveStream::next() {
    LockHolder lh(streamMutex);

    stream_state_t initState = state_;

    DcpResponse* response = NULL;

    switch (state_) {
        case STREAM_PENDING:
            break;
        case STREAM_BACKFILLING:
            response = backfillPhase();
            break;
        case STREAM_IN_MEMORY:
            response = inMemoryPhase();
            break;
        case STREAM_TAKEOVER_SEND:
            response = takeoverSendPhase();
            break;
        case STREAM_TAKEOVER_WAIT:
            response = takeoverWaitPhase();
            break;
        case STREAM_DEAD:
            response = deadPhase();
            break;
        default:
            LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Invalid state '%s'",
                producer->logHeader(), vb_, stateName(state_));
            abort();
    }

    if (state_ != STREAM_DEAD && initState != state_ && !response) {
        lh.unlock();
        return next();
    }

    itemsReady = response ? true : false;
    return response;
}

void ActiveStream::markDiskSnapshot(uint64_t startSeqno, uint64_t endSeqno) {
    LockHolder lh(streamMutex);
    if (state_ != STREAM_BACKFILLING) {
        return;
    }

    startSeqno = std::min(snap_start_seqno_, startSeqno);
    firstMarkerSent = true;

    LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Sending disk snapshot with start "
        "seqno %llu and end seqno %llu", producer->logHeader(), vb_, startSeqno,
        endSeqno);
    pushToReadyQ(new SnapshotMarker(opaque_, vb_, startSeqno, endSeqno,
                                   MARKER_FLAG_DISK));
    RCPtr<VBucket> vb = engine->getVBucket(vb_);
    if (!vb) {
        endStream(END_STREAM_STATE);
    } else {
        if (endSeqno > end_seqno_) {
            endSeqno = end_seqno_;
        }
        // Only re-register the cursor if we still need to get memory snapshots
        CursorRegResult result =
            vb->checkpointManager.registerCursorBySeqno(name_, endSeqno);
        curChkSeqno = result.first;
    }

    if (!itemsReady) {
        itemsReady = true;
        lh.unlock();
        producer->notifyStreamReady(vb_, false);
    }
}

bool ActiveStream::backfillReceived(Item* itm, backfill_source_t backfill_source) {
    LockHolder lh(streamMutex);
    if (state_ == STREAM_BACKFILLING) {
        if (!producer->getBackfillManager()->bytesRead(itm->size())) {
            delete itm;
            return false;
        }

        bufferedBackfill.bytes.fetch_add(itm->size());
        bufferedBackfill.items++;

        pushToReadyQ(new MutationResponse(itm, opaque_,
                          prepareExtendedMetaData(itm->getVBucketId(),
                                                  itm->getConflictResMode())));
        lastReadSeqno = itm->getBySeqno();

        if (!itemsReady) {
            itemsReady = true;
            lh.unlock();
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
    LockHolder lh(streamMutex);

    if (state_ == STREAM_BACKFILLING) {
        isBackfillTaskRunning = false;
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Backfill complete, %d items read"
            " from disk %d from memory, last seqno read: %llu",
            producer->logHeader(), vb_, backfillItems.disk.load(),
            backfillItems.memory.load(), lastReadSeqno);

        if (!itemsReady) {
            itemsReady = true;
            lh.unlock();
            producer->notifyStreamReady(vb_, false);
        }
    }
}

void ActiveStream::snapshotMarkerAckReceived() {
    LockHolder lh (streamMutex);
    waitForSnapshot--;

    if (!itemsReady && waitForSnapshot == 0) {
        itemsReady = true;
        lh.unlock();
        producer->notifyStreamReady(vb_, true);
    }
}

void ActiveStream::setVBucketStateAckRecieved() {
    LockHolder lh(streamMutex);
    if (state_ == STREAM_TAKEOVER_WAIT) {
        if (takeoverState == vbucket_state_pending) {
            RCPtr<VBucket> vbucket = engine->getVBucket(vb_);
            engine->getEpStore()->setVBucketState(vb_, vbucket_state_dead,
                                                  false, false);
            takeoverState = vbucket_state_active;
            transitionState(STREAM_TAKEOVER_SEND);
            LOG(EXTENSION_LOG_INFO, "%s (vb %d) Receive ack for set vbucket "
                "state to pending message", producer->logHeader(), vb_);
        } else {
            LOG(EXTENSION_LOG_INFO, "%s (vb %d) Receive ack for set vbucket "
                "state to active message", producer->logHeader(), vb_);
            endStream(END_STREAM_OK);
        }

        if (!itemsReady) {
            itemsReady = true;
            lh.unlock();
            producer->notifyStreamReady(vb_, true);
        }
    } else {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Unexpected ack for set vbucket "
            "op on stream '%s' state '%s'", producer->logHeader(), vb_,
            name_.c_str(), stateName(state_));
    }
}

DcpResponse* ActiveStream::backfillPhase() {
    DcpResponse* resp = nextQueuedItem();

    if (resp && (resp->getEvent() == DCP_MUTATION ||
         resp->getEvent() == DCP_DELETION ||
         resp->getEvent() == DCP_EXPIRATION)) {
        MutationResponse* m = static_cast<MutationResponse*>(resp);
        producer->getBackfillManager()->bytesSent(m->getItem()->size());
        bufferedBackfill.bytes.fetch_sub(m->getItem()->size());
        bufferedBackfill.items--;
        if (backfillRemaining > 0) {
            backfillRemaining--;
        }
    }

    if (!isBackfillTaskRunning && readyQ.empty()) {
        backfillRemaining = 0;
        if (lastReadSeqno >= end_seqno_) {
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
    if (!readyQ.empty()) {
        return nextQueuedItem();
    }

    if (lastSentSeqno >= end_seqno_) {
        endStream(END_STREAM_OK);
    } else {
        nextCheckpointItem();
    }

    return nextQueuedItem();
}

DcpResponse* ActiveStream::takeoverSendPhase() {
    if (!readyQ.empty()) {
        return nextQueuedItem();
    } else {
        nextCheckpointItem();
        if (!readyQ.empty()) {
            return nextQueuedItem();
        }
    }

    if (waitForSnapshot != 0) {
        return NULL;
    }

    DcpResponse* resp = new SetVBucketState(opaque_, vb_, takeoverState);
    transitionState(STREAM_TAKEOVER_WAIT);
    return resp;
}

DcpResponse* ActiveStream::takeoverWaitPhase() {
    return nextQueuedItem();
}

DcpResponse* ActiveStream::deadPhase() {
    return nextQueuedItem();
}

void ActiveStream::addStats(ADD_STAT add_stat, const void *c) {
    Stream::addStats(add_stat, c);

    const int bsize = 1024;
    char buffer[bsize];
    snprintf(buffer, bsize, "%s:stream_%d_backfill_disk_items",
             name_.c_str(), vb_);
    add_casted_stat(buffer, backfillItems.disk, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_backfill_mem_items",
             name_.c_str(), vb_);
    add_casted_stat(buffer, backfillItems.memory, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_backfill_sent", name_.c_str(), vb_);
    add_casted_stat(buffer, backfillItems.sent, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_memory_phase", name_.c_str(), vb_);
    add_casted_stat(buffer, itemsFromMemoryPhase, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_last_sent_seqno", name_.c_str(), vb_);
    add_casted_stat(buffer, lastSentSeqno, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_last_read_seqno", name_.c_str(), vb_);
    add_casted_stat(buffer, lastReadSeqno, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_ready_queue_memory", name_.c_str(), vb_);
    add_casted_stat(buffer, getReadyQueueMemory(), add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_items_ready", name_.c_str(), vb_);
    add_casted_stat(buffer, itemsReady ? "true" : "false", add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_backfill_buffer_bytes", name_.c_str(), vb_);
    add_casted_stat(buffer, bufferedBackfill.bytes, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_backfill_buffer_items", name_.c_str(), vb_);
    add_casted_stat(buffer, bufferedBackfill.items, add_stat, c);
}

void ActiveStream::addTakeoverStats(ADD_STAT add_stat, const void *cookie) {
    LockHolder lh(streamMutex);

    RCPtr<VBucket> vb = engine->getVBucket(vb_);
    add_casted_stat("name", name_, add_stat, cookie);
    if (!vb || state_ == STREAM_DEAD) {
        add_casted_stat("status", "completed", add_stat, cookie);
        add_casted_stat("estimate", 0, add_stat, cookie);
        add_casted_stat("backfillRemaining", 0, add_stat, cookie);
        add_casted_stat("estimate", 0, add_stat, cookie);
        return;
    }

    size_t total = backfillRemaining;
    if (state_ == STREAM_BACKFILLING) {
        add_casted_stat("status", "backfilling", add_stat, cookie);
    } else {
        add_casted_stat("status", "in-memory", add_stat, cookie);
    }
    add_casted_stat("backfillRemaining", backfillRemaining, add_stat, cookie);

    item_eviction_policy_t iep = engine->getEpStore()->getItemEvictionPolicy();
    size_t vb_items = vb->getNumItems(iep);
    size_t chk_items = vb_items > 0 ?
                vb->checkpointManager.getNumItemsForCursor(name_) : 0;
    size_t del_items = engine->getEpStore()->getRWUnderlying(vb_)->
                                                    getNumPersistedDeletes(vb_);

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
        if (response->getEvent() == DCP_MUTATION ||
            response->getEvent() == DCP_DELETION ||
            response->getEvent() == DCP_EXPIRATION) {
            lastSentSeqno = dynamic_cast<MutationResponse*>(response)->getBySeqno();

            if (state_ == STREAM_BACKFILLING) {
                backfillItems.sent++;
            } else {
                itemsFromMemoryPhase++;
            }
        }
        popFromReadyQ();
        return response;
    }
    return NULL;
}

void ActiveStream::nextCheckpointItem() {
    RCPtr<VBucket> vbucket = engine->getVBucket(vb_);
    if (!vbucket) {
        /* The entity deleting the vbucket must set stream to dead,
           calling setDead(END_STREAM_STATE) will cause deadlock because
           it will try to grab streamMutex which is already acquired at this
           point here */
        return;
    }

    bool mark = false;
    std::vector<queued_item> items;
    std::list<MutationResponse*> mutations;
    vbucket->checkpointManager.getAllItemsForCursor(name_, items);
    if (vbucket->checkpointManager.getNumCheckpoints() > 1) {
        engine->getEpStore()->wakeUpCheckpointRemover();
    }

    if (items.empty()) {
        return;
    }

    if (items.front()->getOperation() == queue_op_checkpoint_start) {
        mark = true;
    }

    std::vector<queued_item>::iterator itr = items.begin();
    for (; itr != items.end(); ++itr) {
        queued_item& qi = *itr;

        if (qi->getOperation() == queue_op_set ||
            qi->getOperation() == queue_op_del) {
            curChkSeqno = qi->getBySeqno();
            lastReadSeqno = qi->getBySeqno();

            mutations.push_back(new MutationResponse(qi, opaque_,
                           prepareExtendedMetaData(qi->getVBucketId(),
                                                   qi->getConflictResMode())));
        } else if (qi->getOperation() == queue_op_checkpoint_start) {
            cb_assert(mutations.empty());
            mark = true;
        }
    }

    if (mutations.empty()) {
        // If we only got checkpoint start or ends check to see if there are
        // any more snapshots before pausing the stream.
        nextCheckpointItem();
    } else {
        snapshot(mutations, mark);
    }
}

void ActiveStream::snapshot(std::list<MutationResponse*>& items, bool mark) {
    if (items.empty()) {
        return;
    }

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

    pushToReadyQ(new SnapshotMarker(opaque_, vb_, snapStart, snapEnd, flags));
    while(!items.empty()) {
        pushToReadyQ(items.front());
        items.pop_front();
    }
}

uint32_t ActiveStream::setDead(end_stream_status_t status) {
    LockHolder lh(streamMutex);
    endStream(status);

    if (!itemsReady && status != END_STREAM_DISCONNECTED) {
        itemsReady = true;
        lh.unlock();
        producer->notifyStreamReady(vb_, true);
    }
    return 0;
}

void ActiveStream::notifySeqnoAvailable(uint64_t seqno) {
    LockHolder lh(streamMutex);
    if (state_ != STREAM_DEAD) {
        if (!itemsReady) {
            itemsReady = true;
            lh.unlock();
            producer->notifyStreamReady(vb_, true);
        }
    }
}

void ActiveStream::endStream(end_stream_status_t reason) {
    if (state_ != STREAM_DEAD) {
        if (state_ == STREAM_BACKFILLING) {
            // If Stream were in Backfilling state, clear out the
            // backfilled items to clear up the backfill buffer.
            clear_UNLOCKED();
            producer->getBackfillManager()->bytesSent(bufferedBackfill.bytes);
            bufferedBackfill.bytes = 0;
            bufferedBackfill.items = 0;
        }
        if (reason != END_STREAM_DISCONNECTED) {
            pushToReadyQ(new StreamEndResponse(opaque_, reason, vb_));
        }
        transitionState(STREAM_DEAD);
        LOG(EXTENSION_LOG_WARNING, "%s (vb %" PRId16 ") Stream closing, "
            "%" PRIu64 " items sent from backfill phase, %" PRIu64 " items "
            "sent from memory phase, %" PRIu64 " was last seqno sent, "
            "reason: %s", producer->logHeader(), vb_,
            uint64_t(backfillItems.sent.load()),
            uint64_t(itemsFromMemoryPhase), lastSentSeqno,
            getEndStreamStatusStr(reason));
    }
}

void ActiveStream::scheduleBackfill() {
    if (!isBackfillTaskRunning) {
        RCPtr<VBucket> vbucket = engine->getVBucket(vb_);
        if (!vbucket) {
            return;
        }

        CursorRegResult result =
            vbucket->checkpointManager.registerCursorBySeqno(name_,
                                                             lastReadSeqno);
        curChkSeqno = result.first;
        bool isFirstItem = result.second;

        cb_assert(lastReadSeqno <= curChkSeqno);
        uint64_t backfillStart = lastReadSeqno + 1;

        /* We need to find the minimum seqno that needs to be backfilled in
         * order to make sure that we don't miss anything when transitioning
         * to a memory snapshot. The backfill task will always make sure that
         * the backfill end seqno is contained in the backfill.
         */
        uint64_t backfillEnd = 0;
        if (flags_ & DCP_ADD_STREAM_FLAG_DISKONLY) { // disk backfill only
            backfillEnd = end_seqno_;
        } else { // disk backfill + in-memory streaming
            if (backfillStart < curChkSeqno) {
                if (curChkSeqno > end_seqno_) {
                    backfillEnd = end_seqno_;
                } else {
                    backfillEnd = curChkSeqno - 1;
                }
            }
        }

        bool tryBackfill = isFirstItem || flags_ & DCP_ADD_STREAM_FLAG_DISKONLY;

        if (backfillStart <= backfillEnd && tryBackfill) {
            BackfillManager* backfillMgr = producer->getBackfillManager();
            backfillMgr->schedule(this, backfillStart, backfillEnd);
            isBackfillTaskRunning = true;
        } else {
            if (flags_ & DCP_ADD_STREAM_FLAG_DISKONLY) {
                endStream(END_STREAM_OK);
            } else if (flags_ & DCP_ADD_STREAM_FLAG_TAKEOVER) {
                transitionState(STREAM_TAKEOVER_SEND);
            } else {
                transitionState(STREAM_IN_MEMORY);
            }
            itemsReady = true;
        }
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
    default:
        break;
    }
    return "Status unknown; this should not happen";
}

void ActiveStream::transitionState(stream_state_t newState) {
    LOG(EXTENSION_LOG_DEBUG, "%s (vb %d) Transitioning from %s to %s",
        producer->logHeader(), vb_, stateName(state_), stateName(newState));

    if (state_ == newState) {
        return;
    }

    switch (state_) {
        case STREAM_PENDING:
            cb_assert(newState == STREAM_BACKFILLING || newState == STREAM_DEAD);
            break;
        case STREAM_BACKFILLING:
            cb_assert(newState == STREAM_IN_MEMORY ||
                   newState == STREAM_TAKEOVER_SEND ||
                   newState == STREAM_DEAD);
            break;
        case STREAM_IN_MEMORY:
            cb_assert(newState == STREAM_BACKFILLING || newState == STREAM_DEAD);
            break;
        case STREAM_TAKEOVER_SEND:
            cb_assert(newState == STREAM_TAKEOVER_WAIT || newState == STREAM_DEAD);
            break;
        case STREAM_TAKEOVER_WAIT:
            cb_assert(newState == STREAM_TAKEOVER_SEND || newState == STREAM_DEAD);
            break;
        default:
            LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Invalid Transition from %s "
                "to %s", producer->logHeader(), vb_, stateName(state_),
                stateName(newState));
            abort();
    }

    state_ = newState;

    if (newState == STREAM_BACKFILLING) {
        scheduleBackfill();
    } else if (newState == STREAM_TAKEOVER_SEND) {
        nextCheckpointItem();
    } else if (newState == STREAM_DEAD) {
        RCPtr<VBucket> vb = engine->getVBucket(vb_);
        if (vb) {
            vb->checkpointManager.removeCursor(name_);
        }
    }
}

size_t ActiveStream::getItemsRemaining() {
    RCPtr<VBucket> vbucket = engine->getVBucket(vb_);

    if (!vbucket || state_ == STREAM_DEAD) {
        return 0;
    }

    uint64_t high_seqno = vbucket->getHighSeqno();

    if (end_seqno_ < high_seqno) {
        if (end_seqno_ > lastSentSeqno) {
            return (end_seqno_ - lastSentSeqno);
        }
    } else {
        if (high_seqno > lastSentSeqno) {
            return (high_seqno - lastSentSeqno);
        }
    }

    return 0;
}

ExtendedMetaData* ActiveStream::prepareExtendedMetaData(uint16_t vBucketId,
                                                        uint8_t conflictResMode)
{
    ExtendedMetaData *emd = NULL;
    if (producer->isExtMetaDataEnabled()) {
        RCPtr<VBucket> vb = engine->getVBucket(vBucketId);
        if (vb && vb->isTimeSyncEnabled()) {
            int64_t adjustedTime = gethrtime() + vb->getDriftCounter();
            emd = new ExtendedMetaData(adjustedTime, conflictResMode);
        } else {
            emd = new ExtendedMetaData(conflictResMode);
        }
    }
    return emd;
}

const char* ActiveStream::logHeader()
{
    return producer->logHeader();
}

NotifierStream::NotifierStream(EventuallyPersistentEngine* e, DcpProducer* p,
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
        itemsReady = true;
    }

    type_ = STREAM_NOTIFIER;

    LOG(EXTENSION_LOG_WARNING, "%s (vb %d) stream created with start seqno "
        "%llu and end seqno %llu", producer->logHeader(), vb, st_seqno,
        en_seqno);
}

uint32_t NotifierStream::setDead(end_stream_status_t status) {
    LockHolder lh(streamMutex);
    if (state_ != STREAM_DEAD) {
        transitionState(STREAM_DEAD);
        if (status != END_STREAM_DISCONNECTED) {
            pushToReadyQ(new StreamEndResponse(opaque_, status, vb_));
            if (!itemsReady) {
                itemsReady = true;
                lh.unlock();
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
        if (!itemsReady) {
            itemsReady = true;
            lh.unlock();
            producer->notifyStreamReady(vb_, true);
        }
    }
}

DcpResponse* NotifierStream::next() {
    LockHolder lh(streamMutex);

    if (readyQ.empty()) {
        itemsReady = false;
        return NULL;
    }

    DcpResponse* response = readyQ.front();
    popFromReadyQ();

    return response;
}

void NotifierStream::transitionState(stream_state_t newState) {
    LOG(EXTENSION_LOG_DEBUG, "%s (vb %d) Transitioning from %s to %s",
        producer->logHeader(), vb_, stateName(state_), stateName(newState));

    if (state_ == newState) {
        return;
    }

    switch (state_) {
        case STREAM_PENDING:
            cb_assert(newState == STREAM_DEAD);
            break;
        default:
            LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Invalid Transition from %s "
                "to %s", producer->logHeader(), vb_, stateName(state_),
                stateName(newState));
            abort();
    }

    state_ = newState;
}

PassiveStream::PassiveStream(EventuallyPersistentEngine* e, DcpConsumer* c,
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
    itemsReady = true;
    type_ = STREAM_PASSIVE;

    const char* type = (flags & DCP_ADD_STREAM_FLAG_TAKEOVER) ? "takeover" : "";
    LOG(EXTENSION_LOG_WARNING, "%s (vb %" PRId16 ") Attempting to add %s stream"
        " with start seqno %" PRIu64 ", end seqno %" PRIu64 ","
        " vbucket uuid %" PRIu64 ", snap start seqno %" PRIu64 ","
        " snap end seqno %" PRIu64 ", and vb_high_seqno %" PRIu64 "",
        consumer->logHeader(), vb, type, st_seqno, en_seqno, vb_uuid,
        snap_start_seqno, snap_end_seqno, vb_high_seqno);
}

PassiveStream::~PassiveStream() {
    LockHolder lh(streamMutex);
    clear_UNLOCKED();
    if (state_ != STREAM_DEAD) {
        setDead_UNLOCKED(END_STREAM_OK, &lh);
    }
}

uint32_t PassiveStream::setDead_UNLOCKED(end_stream_status_t status,
                                         LockHolder *slh) {
    transitionState(STREAM_DEAD);
    slh->unlock();
    uint32_t unackedBytes = clearBuffer();
    LOG(EXTENSION_LOG_WARNING, "%s (vb %" PRId16 ") Setting stream to dead"
        " state, last_seqno is %" PRIu64 ", unackedBytes is %" PRIu32 ","
        " status is %s",
        consumer->logHeader(), vb_, last_seqno, unackedBytes,
        getEndStreamStatusStr(status));
    return unackedBytes;
}

uint32_t PassiveStream::setDead(end_stream_status_t status) {
    LockHolder lh(streamMutex);
    return setDead_UNLOCKED(status, &lh);
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
        if (!itemsReady) {
            itemsReady = true;
            lh.unlock();
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

    LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Attempting to reconnect stream "
        "with opaque %u, start seq no %llu, end seq no %llu, snap start seqno "
        "%llu, and snap end seqno %llu", consumer->logHeader(), vb_, new_opaque,
        start_seqno, end_seqno_, snap_start_seqno_, snap_end_seqno_);

    LockHolder lh(streamMutex);
    last_seqno = start_seqno;
    pushToReadyQ(new StreamRequest(vb_, new_opaque, flags_, start_seqno,
                                  end_seqno_, vb_uuid_, snap_start_seqno_,
                                  snap_end_seqno_));
    if (!itemsReady) {
        itemsReady = true;
        lh.unlock();
        consumer->notifyStreamReady(vb_);
    }
}

ENGINE_ERROR_CODE PassiveStream::messageReceived(DcpResponse* resp) {
    if(nullptr == resp) {
        return ENGINE_EINVAL;
    }

    LockHolder lh(buffer.bufMutex);

    if (state_ == STREAM_DEAD) {
        delete resp;
        return ENGINE_KEY_ENOENT;
    }

    switch (resp->getEvent()) {
        case DCP_MUTATION:
        case DCP_DELETION:
        case DCP_EXPIRATION:
        {
            MutationResponse* m = static_cast<MutationResponse*>(resp);
            uint64_t bySeqno = m->getBySeqno();
            if (bySeqno <= last_seqno) {
                LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Erroneous (out of "
                    "sequence) mutation received, with opaque: %u, its "
                    "seqno (%llu) is not greater than last received seqno "
                    "(%llu); Dropping mutation!", consumer->logHeader(),
                    vb_, opaque_, bySeqno, last_seqno);
                delete m;
                return ENGINE_ERANGE;
            }
            last_seqno = bySeqno;
            break;
        }
        case DCP_SNAPSHOT_MARKER:
        {
            SnapshotMarker* s = static_cast<SnapshotMarker*>(resp);
            uint64_t snapStart = s->getStartSeqno();
            uint64_t snapEnd = s->getEndSeqno();
            if (snapStart < last_seqno && snapEnd <= last_seqno) {
                LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Erroneous snapshot "
                    "marker received, with opaque: %u, its start (%llu), and"
                    "end (%llu) are less than last received seqno (%llu); "
                    "Dropping marker!", consumer->logHeader(), vb_, opaque_,
                    snapStart, snapEnd, last_seqno);
                delete s;
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
            LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Unknown DCP op received: %d;"
                " Disconnecting connection..",
                consumer->logHeader(), vb_, resp->getEvent());
            return ENGINE_DISCONNECT;
        }
    }

    buffer.messages.push(resp);
    buffer.items++;
    buffer.bytes += resp->getMessageSize();

    return ENGINE_SUCCESS;
}

process_items_error_t PassiveStream::processBufferedMessages(uint32_t& processed_bytes) {
    LockHolder lh(buffer.bufMutex);
    uint32_t count = 0;
    uint32_t message_bytes = 0;
    uint32_t total_bytes_processed = 0;
    bool failed = false;

    if (buffer.messages.empty()) {
        return all_processed;
    }

    while (count < PassiveStream::batchSize && !buffer.messages.empty()) {
        ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
        DcpResponse *response = buffer.messages.front();
        message_bytes = response->getMessageSize();

        switch (response->getEvent()) {
            case DCP_MUTATION:
                ret = processMutation(static_cast<MutationResponse*>(response));
                break;
            case DCP_DELETION:
            case DCP_EXPIRATION:
                ret = processDeletion(static_cast<MutationResponse*>(response));
                break;
            case DCP_SNAPSHOT_MARKER:
                processMarker(static_cast<SnapshotMarker*>(response));
                break;
            case DCP_SET_VBUCKET:
                processSetVBucketState(static_cast<SetVBucketState*>(response));
                break;
            case DCP_STREAM_END:
                transitionState(STREAM_DEAD);
                delete response;
                break;
            default:
                abort();
        }

        if (ret == ENGINE_TMPFAIL || ret == ENGINE_ENOMEM) {
            failed = true;
            break;
        }

        buffer.messages.pop();
        buffer.items--;
        buffer.bytes -= message_bytes;
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

    if (mutation->getBySeqno() > cur_snapshot_end) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Erroneous mutation [sequence "
            "number (%llu) greater than current snapshot end seqno (%llu)] "
            "being processed; Dropping the mutation!", consumer->logHeader(),
            vb_, mutation->getBySeqno(), cur_snapshot_end);
        delete mutation;
        return ENGINE_ERANGE;
    }

    ENGINE_ERROR_CODE ret;
    if (vb->isBackfillPhase()) {
        ret = engine->getEpStore()->addTAPBackfillItem(*mutation->getItem(),
                                                    INITIAL_NRU_VALUE,
                                                    false,
                                                    mutation->getExtMetaData());
    } else {
        ret = engine->getEpStore()->setWithMeta(*mutation->getItem(), 0, NULL,
                                                consumer->getCookie(), true,
                                                true, INITIAL_NRU_VALUE, false,
                                                mutation->getExtMetaData(),
                                                true);
    }

    // We should probably handle these error codes in a better way, but since
    // the producer side doesn't do anything with them anyways let's just log
    // them for now until we come up with a better solution.
    if (ret != ENGINE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING, "%s Got an error code %d while trying to "
            "process  mutation", consumer->logHeader(), ret);
    } else {
        handleSnapshotEnd(vb, mutation->getBySeqno());
    }

    if (ret != ENGINE_TMPFAIL && ret != ENGINE_ENOMEM) {
        delete mutation;
    }

    return ret;
}

ENGINE_ERROR_CODE PassiveStream::processDeletion(MutationResponse* deletion) {
    RCPtr<VBucket> vb = engine->getVBucket(vb_);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    if (deletion->getBySeqno() > cur_snapshot_end) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Erroneous deletion [sequence "
            "number (%llu) greater than current snapshot end seqno (%llu)] "
            "being processed; Dropping the deletion!", consumer->logHeader(),
            vb_, deletion->getBySeqno(), cur_snapshot_end);
        delete deletion;
        return ENGINE_ERANGE;
    }

    uint64_t delCas = 0;
    ENGINE_ERROR_CODE ret;
    ItemMetaData meta = deletion->getItem()->getMetaData();
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

    // We should probably handle these error codes in a better way, but since
    // the producer side doesn't do anything with them anyways let's just log
    // them for now until we come up with a better solution.
    if (ret != ENGINE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING, "%s Got an error code %d while trying to "
            "process  deletion", consumer->logHeader(), ret);
    } else {
        handleSnapshotEnd(vb, deletion->getBySeqno());
    }

    if (ret != ENGINE_TMPFAIL && ret != ENGINE_ENOMEM) {
        delete deletion;
    }

    return ret;
}

void PassiveStream::processMarker(SnapshotMarker* marker) {
    RCPtr<VBucket> vb = engine->getVBucket(vb_);

    cur_snapshot_start = marker->getStartSeqno();
    cur_snapshot_end = marker->getEndSeqno();
    cur_snapshot_type = (marker->getFlags() & MARKER_FLAG_DISK) ? disk : memory;

    if (vb) {
        if (marker->getFlags() & MARKER_FLAG_DISK && vb->getHighSeqno() == 0) {
            vb->setBackfillPhase(true);
            vb->checkpointManager.setBackfillPhase(cur_snapshot_start,
                                                   cur_snapshot_end);
        } else {
            if (marker->getFlags() & MARKER_FLAG_CHK ||
                vb->checkpointManager.getOpenCheckpointId() == 0) {
                vb->checkpointManager.createSnapshot(cur_snapshot_start,
                                                     cur_snapshot_end);
            } else {
                vb->checkpointManager.updateCurrentSnapshotEnd(cur_snapshot_end);
            }
            vb->setBackfillPhase(false);
        }

        if (marker->getFlags() & MARKER_FLAG_ACK) {
            cur_snapshot_ack = true;
        }
    }
    delete marker;
}

void PassiveStream::processSetVBucketState(SetVBucketState* state) {
    engine->getEpStore()->setVBucketState(vb_, state->getState(), true);
    delete state;

    LockHolder lh (streamMutex);
    pushToReadyQ(new SetVBucketStateResponse(opaque_, ENGINE_SUCCESS));
    if (!itemsReady) {
        itemsReady = true;
        lh.unlock();
        consumer->notifyStreamReady(vb_);
    }
}

void PassiveStream::handleSnapshotEnd(RCPtr<VBucket>& vb, uint64_t byseqno) {
    if (byseqno == cur_snapshot_end) {
        if (cur_snapshot_type == disk && vb->isBackfillPhase()) {
            vb->setBackfillPhase(false);
            uint64_t id = vb->checkpointManager.getOpenCheckpointId() + 1;
            vb->checkpointManager.checkAndAddNewCheckpoint(id, vb);
        } else {
            double maxSize = static_cast<double>(engine->getEpStats().getMaxDataSize());
            double mem_threshold = StoredValue::getMutationMemThreshold();
            double mem_used = static_cast<double>(engine->getEpStats().getTotalMemoryUsed());
            if (maxSize * mem_threshold < mem_used) {
                uint64_t id = vb->checkpointManager.getOpenCheckpointId() + 1;
                vb->checkpointManager.checkAndAddNewCheckpoint(id, vb);
            }
        }

        if (cur_snapshot_ack) {
            LockHolder lh(streamMutex);
            pushToReadyQ(new SnapshotMarkerResponse(opaque_, ENGINE_SUCCESS));
            if (!itemsReady) {
                itemsReady = true;
                lh.unlock();
                consumer->notifyStreamReady(vb_);
            }
            cur_snapshot_ack = false;
        }
        cur_snapshot_type = none;
    }
}

void PassiveStream::addStats(ADD_STAT add_stat, const void *c) {
    Stream::addStats(add_stat, c);

    const int bsize = 1024;
    char buf[bsize];
    snprintf(buf, bsize, "%s:stream_%d_buffer_items", name_.c_str(), vb_);
    add_casted_stat(buf, buffer.items, add_stat, c);
    snprintf(buf, bsize, "%s:stream_%d_buffer_bytes", name_.c_str(), vb_);
    add_casted_stat(buf, buffer.bytes, add_stat, c);
    snprintf(buf, bsize, "%s:stream_%d_items_ready", name_.c_str(), vb_);
    add_casted_stat(buf, itemsReady ? "true" : "false", add_stat, c);
    snprintf(buf, bsize, "%s:stream_%d_last_received_seqno", name_.c_str(), vb_);
    add_casted_stat(buf, last_seqno, add_stat, c);
    snprintf(buf, bsize, "%s:stream_%d_ready_queue_memory", name_.c_str(), vb_);
    add_casted_stat(buf, getReadyQueueMemory(), add_stat, c);

    snprintf(buf, bsize, "%s:stream_%d_cur_snapshot_type", name_.c_str(), vb_);
    add_casted_stat(buf, snapshotTypeToString(cur_snapshot_type), add_stat, c);

    if (cur_snapshot_type != none) {
        snprintf(buf, bsize, "%s:stream_%d_cur_snapshot_start", name_.c_str(), vb_);
        add_casted_stat(buf, cur_snapshot_start, add_stat, c);
        snprintf(buf, bsize, "%s:stream_%d_cur_snapshot_end", name_.c_str(), vb_);
        add_casted_stat(buf, cur_snapshot_end, add_stat, c);
    }
}

DcpResponse* PassiveStream::next() {
    LockHolder lh(streamMutex);

    if (readyQ.empty()) {
        itemsReady = false;
        return NULL;
    }

    DcpResponse* response = readyQ.front();
    popFromReadyQ();
    return response;
}

uint32_t PassiveStream::clearBuffer() {
    LockHolder lh(buffer.bufMutex);
    uint32_t unackedBytes = buffer.bytes;

    while (!buffer.messages.empty()) {
        DcpResponse* resp = buffer.messages.front();
        buffer.messages.pop();
        delete resp;
    }

    buffer.bytes = 0;
    buffer.items = 0;
    return unackedBytes;
}

void PassiveStream::transitionState(stream_state_t newState) {
    LOG(EXTENSION_LOG_DEBUG, "%s (vb %d) Transitioning from %s to %s",
        consumer->logHeader(), vb_, stateName(state_), stateName(newState));

    if (state_ == newState) {
        return;
    }

    switch (state_) {
        case STREAM_PENDING:
            cb_assert(newState == STREAM_READING || newState == STREAM_DEAD);
            break;
        case STREAM_READING:
            cb_assert(newState == STREAM_PENDING || newState == STREAM_DEAD);
            break;
        default:
            LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Invalid Transition from %s "
                "to %s", consumer->logHeader(), vb_, stateName(state_),
                stateName(newState));
            abort();
    }

    state_ = newState;
}

const char* PassiveStream::getEndStreamStatusStr(end_stream_status_t status)
{
    switch (status) {
        case END_STREAM_CLOSED:
            return "The stream closed due to a close stream message";
        case END_STREAM_DISCONNECTED:
            return "The stream closed early because the conn was disconnected";
        default:
            break;
    }
    return "Status unknown; this should not happen";
}
