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
#include "dcp-stream.h"
#include "dcp-consumer.h"
#include "dcp-producer.h"
#include "dcp-response.h"

#define DCP_BACKFILL_SLEEP_TIME 2

static const char* snapshotTypeToString(snapshot_type_t type) {
    static const char * const snapshotTypes[] = { "none", "disk", "memory" };
    cb_assert(type >= none && type <= memory);
    return snapshotTypes[type];
}

const uint64_t Stream::dcpMaxSeqno = std::numeric_limits<uint64_t>::max();
const size_t PassiveStream::batchSize = 10;

class SnapshotMarkerCallback : public Callback<SeqnoRange> {
public:
    SnapshotMarkerCallback(stream_t s)
        : stream(s) {
        cb_assert(s->getType() == STREAM_ACTIVE);
    }

    void callback(SeqnoRange &range) {
        uint64_t st = range.getStartSeqno();
        uint64_t en = range.getEndSeqno();
        static_cast<ActiveStream*>(stream.get())->markDiskSnapshot(st, en);
    }

private:
    stream_t stream;
};

class CacheCallback : public Callback<CacheLookup> {
public:
    CacheCallback(EventuallyPersistentEngine* e, stream_t &s)
        : engine_(e), stream_(s) {
        Stream *str = stream_.get();
        if (str) {
            cb_assert(str->getType() == STREAM_ACTIVE);
        }
    }

    void callback(CacheLookup &lookup);

private:
    EventuallyPersistentEngine* engine_;
    stream_t stream_;
};

void CacheCallback::callback(CacheLookup &lookup) {
    RCPtr<VBucket> vb = engine_->getEpStore()->getVBucket(lookup.getVBucketId());
    if (!vb) {
        setStatus(ENGINE_SUCCESS);
        return;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(lookup.getKey(), &bucket_num);
    StoredValue *v = vb->ht.unlocked_find(lookup.getKey(), bucket_num, false, false);
    if (v && v->isResident() && v->getBySeqno() == lookup.getBySeqno()) {
        Item* it = v->toItem(false, lookup.getVBucketId());
        lh.unlock();
        static_cast<ActiveStream*>(stream_.get())->backfillReceived(it);
        setStatus(ENGINE_KEY_EEXISTS);
    } else {
        setStatus(ENGINE_SUCCESS);
    }
}

class DiskCallback : public Callback<GetValue> {
public:
    DiskCallback(stream_t &s)
        : stream_(s) {
        Stream *str = stream_.get();
        if (str) {
            cb_assert(str->getType() == STREAM_ACTIVE);
        }
    }

    void callback(GetValue &val) {
        cb_assert(val.getValue());
        ActiveStream* active_stream = static_cast<ActiveStream*>(stream_.get());
        active_stream->backfillReceived(val.getValue());
    }

private:
    stream_t stream_;
};

class DCPBackfill : public GlobalTask {
public:
    DCPBackfill(EventuallyPersistentEngine* e, stream_t s,
                uint64_t start_seqno, uint64_t end_seqno, const Priority &p,
                double sleeptime = 0, bool shutdown = false)
        : GlobalTask(e, p, sleeptime, shutdown), engine(e), stream(s),
          startSeqno(start_seqno), endSeqno(end_seqno) {
        cb_assert(stream->getType() == STREAM_ACTIVE);
    }

    bool run();

    std::string getDescription();

private:
    EventuallyPersistentEngine *engine;
    stream_t                    stream;
    uint64_t                    startSeqno;
    uint64_t                    endSeqno;
};

bool DCPBackfill::run() {
    uint16_t vbid = stream->getVBucket();

    if (engine->getEpStore()->isMemoryUsageTooHigh()) {
        LOG(EXTENSION_LOG_WARNING, "VBucket %d dcp backfill task temporarily "
                "suspended  because the current memory usage is too high",
                vbid);
        snooze(DCP_BACKFILL_SLEEP_TIME);
        return true;
    }

    uint64_t lastPersistedSeqno =
        engine->getEpStore()->getLastPersistedSeqno(vbid);
    uint64_t diskSeqno =
        engine->getEpStore()->getRWUnderlying(vbid)->getLastPersistedSeqno(vbid);

    if (lastPersistedSeqno < endSeqno) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Rescheduling backfill "
            "because backfill up to seqno %llu is needed but only up to "
            "%llu is persisted (disk %llu)",
            static_cast<ActiveStream*>(stream.get())->logHeader(), vbid,
            endSeqno, lastPersistedSeqno, diskSeqno);
        snooze(DCP_BACKFILL_SLEEP_TIME);
        return true;
    }

    KVStore* kvstore = engine->getEpStore()->getROUnderlying(vbid);
    size_t numItems = kvstore->getNumItems(vbid, startSeqno,
                                           std::numeric_limits<uint64_t>::max());
    static_cast<ActiveStream*>(stream.get())->incrBackfillRemaining(numItems);

    shared_ptr<Callback<GetValue> > cb(new DiskCallback(stream));
    shared_ptr<Callback<CacheLookup> > cl(new CacheCallback(engine, stream));
    shared_ptr<Callback<SeqnoRange> > sr(new SnapshotMarkerCallback(stream));
    kvstore->dump(vbid, startSeqno, cb, cl, sr);

    static_cast<ActiveStream*>(stream.get())->completeBackfill();

    LOG(EXTENSION_LOG_WARNING, "%s (vb %" PRIu16 ") "
        "Backfill task (%" PRIu64 " to %" PRIu64 ") "
        "finished. disk seqno %" PRIu64 " memory seqno %" PRIu64 "",
        static_cast<ActiveStream*>(stream.get())->logHeader(), vbid,
        startSeqno, endSeqno, diskSeqno, lastPersistedSeqno);

    return false;
}

std::string DCPBackfill::getDescription() {
    std::stringstream ss;
    ss << "DCP backfill for vbucket " << stream->getVBucket();
    return ss.str();
}

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
    const int bsize = 128;
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

ActiveStream::ActiveStream(EventuallyPersistentEngine* e, dcp_producer_t p,
                           const std::string &n, uint32_t flags,
                           uint32_t opaque, uint16_t vb, uint64_t st_seqno,
                           uint64_t en_seqno, uint64_t vb_uuid,
                           uint64_t snap_start_seqno, uint64_t snap_end_seqno)
    :  Stream(n, flags, opaque, vb, st_seqno, en_seqno, vb_uuid,
              snap_start_seqno, snap_end_seqno),
       lastReadSeqno(st_seqno), lastSentSeqno(st_seqno), curChkSeqno(st_seqno),
       takeoverState(vbucket_state_pending), backfillRemaining(0),
       itemsFromBackfill(0), itemsFromMemory(0), firstMarkerSent(false),
       waitForSnapshot(0), engine(e), producer(p),
       isBackfillTaskRunning(false), lastSentSnapEndSeqno(0),
       chkptItemsExtractionInProgress(false) {

    const char* type = "";
    if (flags_ & DCP_ADD_STREAM_FLAG_TAKEOVER) {
        type = "takeover ";
        end_seqno_ = dcpMaxSeqno;
    }

    RCPtr<VBucket> vbucket = engine->getVBucket(vb);
    if (vbucket) {
        ReaderLockHolder rlh(vbucket->getStateLock());
        if (vbucket->getState() == vbucket_state_replica) {
            uint64_t snapshot_start, snapshot_end;
            vbucket->getCurrentSnapshot(snapshot_start, snapshot_end);
            if (snapshot_end > en_seqno) {
                end_seqno_ = snapshot_end;
            }
        }
    }

    if (start_seqno_ >= end_seqno_) {
        endStream(END_STREAM_OK);
        itemsReady.store(true);
    }

    type_ = STREAM_ACTIVE;

    LOG(EXTENSION_LOG_WARNING, "%s (vb %d) %sstream created with start seqno "
        "%llu and end seqno %llu", producer->logHeader(), vb, type, st_seqno,
        en_seqno);
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
            LOG(EXTENSION_LOG_WARNING, "%s (vb %" PRIu16 ") Merging backfill"
                " and memory snapshot for a replica vbucket, start seqno "
                "%" PRIu64 " and end seqno %" PRIu64 "",
                producer->logHeader(), vb_, startSeqno, endSeqno);
            endSeqno = end_seqno_;
        }
    }

    LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Sending disk snapshot with start "
        "seqno %llu and end seqno %llu", producer->logHeader(), vb_, startSeqno,
        endSeqno);
    pushToReadyQ(new SnapshotMarker(opaque_, vb_, startSeqno, endSeqno,
                                    MARKER_FLAG_DISK));
    lastSentSnapEndSeqno = endSeqno;

    if (!vb) {
        endStream(END_STREAM_STATE);
    } else {
        if (endSeqno > end_seqno_) {
            chkCursorSeqno = end_seqno_;
        }
        // Only re-register the cursor if we still need to get memory snapshots
        CursorRegResult result =
            vb->checkpointManager.registerTAPCursorBySeqno(name_,
                                                           chkCursorSeqno);
        curChkSeqno = result.first;
    }

    lh.unlock();
    bool inverse = false;
    if (itemsReady.compare_exchange_strong(inverse, true)) {
        producer->notifyStreamReady(vb_, false);
    }
}

void ActiveStream::backfillReceived(Item* itm) {
    LockHolder lh(streamMutex);
    if (state_ == STREAM_BACKFILLING) {
        pushToReadyQ(new MutationResponse(itm, opaque_));
        lastReadSeqno = itm->getBySeqno();
        lh.unlock();
        bool inverse = false;
        if (itemsReady.compare_exchange_strong(inverse, true)) {
            producer->notifyStreamReady(vb_, false);
        }
    } else {
        delete itm;
    }
}

void ActiveStream::completeBackfill() {
    {
        LockHolder lh(streamMutex);
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Backfill complete, %d items read"
            " from disk, last seqno read: %ld", producer->logHeader(), vb_,
            itemsFromBackfill, lastReadSeqno);
    }

    isBackfillTaskRunning.store(false);
    bool inverse = false;
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
            LOG(EXTENSION_LOG_INFO, "%s (vb %" PRIu16 ") Receive ack for set "
                "vbucket state to pending message", producer->logHeader(), vb_);

            takeoverState = vbucket_state_active;
            transitionState(STREAM_TAKEOVER_SEND);
            lh.unlock();

            engine->getEpStore()->setVBucketState(vb_, vbucket_state_dead,
                                                  false, false);
            RCPtr<VBucket> vbucket = engine->getVBucket(vb_);
            LOG(EXTENSION_LOG_WARNING, "%s (vb %" PRIu16 ") Vbucket marked as "
                "dead, last sent seqno: %" PRIu64 ", high seqno: %" PRIu64 "",
                producer->logHeader(), vb_, lastSentSeqno,
                vbucket->getHighSeqno());
        } else {
            LOG(EXTENSION_LOG_INFO, "%s (vb %" PRIu16 ") Receive ack for set "
                "vbucket state to active message", producer->logHeader(), vb_);
            endStream(END_STREAM_OK);
            lh.unlock();
        }

        bool inverse = false;
        if (itemsReady.compare_exchange_strong(inverse, true)) {
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

    if (resp && backfillRemaining > 0 &&
        (resp->getEvent() == DCP_MUTATION ||
         resp->getEvent() == DCP_DELETION ||
         resp->getEvent() == DCP_EXPIRATION)) {
        backfillRemaining--;
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
    if (lastSentSeqno >= end_seqno_) {
        endStream(END_STREAM_OK);
    } else if (readyQ.empty()) {
        if (nextCheckpointItem()) {
            return NULL;
        }
    }

    return nextQueuedItem();
}

DcpResponse* ActiveStream::takeoverSendPhase() {
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
    return nextQueuedItem();
}

void ActiveStream::addStats(ADD_STAT add_stat, const void *c) {
    Stream::addStats(add_stat, c);

    const int bsize = 128;
    char buffer[bsize];
    snprintf(buffer, bsize, "%s:stream_%d_backfilled", name_.c_str(), vb_);
    add_casted_stat(buffer, itemsFromBackfill, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_memory", name_.c_str(), vb_);
    add_casted_stat(buffer, itemsFromMemory, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_last_sent_seqno", name_.c_str(), vb_);
    add_casted_stat(buffer, lastSentSeqno, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_last_read_seqno", name_.c_str(), vb_);
    add_casted_stat(buffer, lastReadSeqno, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_ready_queue_memory", name_.c_str(), vb_);
    add_casted_stat(buffer, getReadyQueueMemory(), add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_items_ready", name_.c_str(), vb_);
    add_casted_stat(buffer, itemsReady.load() ? "true" : "false", add_stat, c);
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
                vb->checkpointManager.getNumItemsForTAPConnection(name_) : 0;
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
        if (producer->bufferLogInsert(response->getMessageSize())) {
            if (response->getEvent() == DCP_MUTATION ||
                response->getEvent() == DCP_DELETION ||
                response->getEvent() == DCP_EXPIRATION) {
                lastSentSeqno = dynamic_cast<MutationResponse*>(response)->getBySeqno();

                if (state_ == STREAM_BACKFILLING) {
                    itemsFromBackfill++;
                } else {
                    itemsFromMemory++;
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
    if (vbucket && vbucket->checkpointManager.getNumItemsForTAPConnection(name_) > 0) {
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
        std::deque<queued_item> items;
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
                                       std::deque<queued_item> &items) {
    // Commencing item processing - set guard flag.
    chkptItemsExtractionInProgress.store(true);

    vb->checkpointManager.getAllItemsForCursor(name_, items);
    if (vb->checkpointManager.getNumCheckpoints() > 1) {
        engine->getEpStore()->wakeUpCheckpointRemover();
    }
}


void ActiveStream::processItems(std::deque<queued_item>& items) {
    if (!items.empty()) {
        bool mark = false;
        if (items.front()->getOperation() == queue_op_checkpoint_start) {
            mark = true;
        }

        std::deque<MutationResponse*> mutations;
        std::deque<queued_item>::iterator itemItr;
        for (itemItr = items.begin(); itemItr != items.end(); itemItr++) {
            queued_item& qi = *itemItr;

            if (qi->getOperation() == queue_op_set ||
                qi->getOperation() == queue_op_del) {
                curChkSeqno = qi->getBySeqno();
                lastReadSeqno = qi->getBySeqno();

                mutations.push_back(new MutationResponse(qi, opaque_));
            } else if (qi->getOperation() == queue_op_checkpoint_start) {
                snapshot(mutations, mark);
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
        lastSentSnapEndSeqno = snapEnd;
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
        if (reason != END_STREAM_DISCONNECTED) {
            pushToReadyQ(new StreamEndResponse(opaque_, reason, vb_));
        }
        transitionState(STREAM_DEAD);
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Stream closing, %llu items sent"
            " from disk, %llu items sent from memory, %llu was last seqno sent"
            " %s is the reason", producer->logHeader(), vb_, itemsFromBackfill,
            itemsFromMemory, lastSentSeqno, getEndStreamStatusStr(reason));
    }
}

void ActiveStream::scheduleBackfill() {
    if (!isBackfillTaskRunning) {
        RCPtr<VBucket> vbucket = engine->getVBucket(vb_);
        if (!vbucket) {
            return;
        }

        CursorRegResult result =
            vbucket->checkpointManager.registerTAPCursorBySeqno(name_,
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
            ExTask task = new DCPBackfill(engine, this, backfillStart, backfillEnd,
                                          Priority::TapBgFetcherPriority, 0, false);
            ExecutorPool::get()->schedule(task, AUXIO_TASK_IDX);
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

    switch (newState) {
        case STREAM_BACKFILLING:
            scheduleBackfill();
            break;
        case STREAM_IN_MEMORY:
            // Check if the producer has sent up till the last requested
            // sequence number already, if not - move checkpoint items into
            // the ready queue.
            if (lastSentSeqno >= end_seqno_) {
                // Stream transitioning to DEAD state
                endStream(END_STREAM_OK);
            } else {
                nextCheckpointItem();
            }
            break;
        case STREAM_TAKEOVER_SEND:
            nextCheckpointItem();
            break;
        case STREAM_DEAD:
            {
                RCPtr<VBucket> vb = engine->getVBucket(vb_);
                if (vb) {
                    vb->checkpointManager.removeTAPCursor(name_);
                }
                break;
            }
        case STREAM_TAKEOVER_WAIT:
        case STREAM_PENDING:
            break;
        case STREAM_READING:
            LOG(EXTENSION_LOG_WARNING,
                "ActiveStream::transitionState: newState can't be "
                "STREAM_READING!");
            break;
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

const char* ActiveStream::logHeader()
{
    return producer->logHeader();
}

bool ActiveStream::isCurrentSnapshotCompleted() const
{
    RCPtr<VBucket> vbucket = engine->getVBucket(vb_);
    // An atomic read of vbucket state without acquiring the
    // reader lock for state should suffice here.
    if (vbucket && vbucket->getState() == vbucket_state_replica) {
        if (lastSentSnapEndSeqno >= lastReadSeqno) {
            return false;
        }
    }
    return true;
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

PassiveStream::PassiveStream(EventuallyPersistentEngine* e, dcp_consumer_t c,
                             const std::string &name, uint32_t flags,
                             uint32_t opaque, uint16_t vb, uint64_t st_seqno,
                             uint64_t en_seqno, uint64_t vb_uuid,
                             uint64_t snap_start_seqno, uint64_t snap_end_seqno,
                             uint64_t vb_high_seqno)
    : Stream(name, flags, opaque, vb, st_seqno, en_seqno, vb_uuid,
             snap_start_seqno, snap_end_seqno),
      engine(e), consumer(c), last_seqno(vb_high_seqno), cur_snapshot_start(0),
      cur_snapshot_end(0), cur_snapshot_type(none), cur_snapshot_ack(false),
      saveSnapshot(false) {
    LockHolder lh(streamMutex);
    pushToReadyQ(new StreamRequest(vb, opaque, flags, st_seqno, en_seqno,
                                  vb_uuid, snap_start_seqno, snap_end_seqno));
    itemsReady.store(true);
    type_ = STREAM_PASSIVE;

    const char* type = (flags & DCP_ADD_STREAM_FLAG_TAKEOVER) ? "takeover" : "";
    LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Attempting to add %s stream with "
        "start seqno %llu, end seqno %llu, vbucket uuid %llu, snap start seqno "
        "%llu, snap end seqno %llu, and vb_high_seqno %llu",
        consumer->logHeader(), vb, type, st_seqno, en_seqno, vb_uuid,
        snap_start_seqno, snap_end_seqno, vb_high_seqno);
}

PassiveStream::~PassiveStream() {
    LockHolder lh(streamMutex);
    clear_UNLOCKED();
    cb_assert(state_ == STREAM_DEAD);
    cb_assert(buffer.bytes == 0);
}

uint32_t PassiveStream::setDead(end_stream_status_t status) {
    LockHolder lh(streamMutex);
    transitionState(STREAM_DEAD);
    lh.unlock();
    uint32_t unackedBytes = clearBuffer();
    LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Setting stream to dead state,"
        " last_seqno is %llu, unackedBytes is %u, status is %s",
        consumer->logHeader(), vb_, last_seqno, unackedBytes,
        getEndStreamStatusStr(status));
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
    vb->getCurrentSnapshot(snap_start_seqno_, snap_end_seqno_);

    LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Attempting to reconnect stream "
        "with opaque %ld, start seq no %llu, end seq no %llu, snap start seqno "
        "%llu, and snap end seqno %llu", consumer->logHeader(), vb_, new_opaque,
        start_seqno, end_seqno_, snap_start_seqno_, snap_end_seqno_);

    LockHolder lh(streamMutex);
    last_seqno = start_seqno;
    pushToReadyQ(new StreamRequest(vb_, new_opaque, flags_, start_seqno,
                                  end_seqno_, vb_uuid_, snap_start_seqno_,
                                  snap_end_seqno_));
    lh.unlock();
    bool inverse = false;
    if (itemsReady.compare_exchange_strong(inverse, true)) {
        consumer->notifyStreamReady(vb_);
    }
}

ENGINE_ERROR_CODE PassiveStream::messageReceived(DcpResponse* resp) {
    LockHolder lh(buffer.bufMutex);
    cb_assert(resp);

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
                    "sequence) mutation received, with opaque: %ld, its "
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
                    "marker received, with opaque: %ld, its start (%llu), and"
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
                break;
            default:
                abort();
        }

        if (ret == ENGINE_TMPFAIL || ret == ENGINE_ENOMEM) {
            failed = true;
            break;
        }

        delete response;
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
        return ENGINE_ERANGE;
    }

    ENGINE_ERROR_CODE ret;
    if (saveSnapshot) {
        LockHolder lh = vb->getSnapshotLock();
        ret = commitMutation(mutation, vb->isBackfillPhase());
        vb->setCurrentSnapshot_UNLOCKED(cur_snapshot_start, cur_snapshot_end);
        saveSnapshot = false;
        lh.unlock();
    } else {
        ret = commitMutation(mutation, vb->isBackfillPhase());
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

    return ret;
}

ENGINE_ERROR_CODE PassiveStream::commitMutation(MutationResponse* mutation,
                                                bool backfillPhase) {
    if (backfillPhase) {
        return engine->getEpStore()->addTAPBackfillItem(*mutation->getItem(),
                                                        INITIAL_NRU_VALUE,
                                                        false);
    } else {
        return engine->getEpStore()->setWithMeta(*mutation->getItem(), 0,
                                                 consumer->getCookie(), true,
                                                 true, INITIAL_NRU_VALUE, false,
                                                 true);
    }
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
        return ENGINE_ERANGE;
    }

    ENGINE_ERROR_CODE ret;
    if (saveSnapshot) {
        LockHolder lh = vb->getSnapshotLock();
        ret = commitDeletion(deletion, vb->isBackfillPhase());
        vb->setCurrentSnapshot_UNLOCKED(cur_snapshot_start, cur_snapshot_end);
        saveSnapshot = false;
        lh.unlock();
    } else {
        ret = commitDeletion(deletion, vb->isBackfillPhase());
    }

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

    return ret;
}

ENGINE_ERROR_CODE PassiveStream::commitDeletion(MutationResponse* deletion,
                                                bool backfillPhase) {
    uint64_t delCas = 0;
    ItemMetaData meta = deletion->getItem()->getMetaData();
    return engine->getEpStore()->deleteWithMeta(deletion->getItem()->getKey(),
                                                &delCas, deletion->getVBucket(),
                                                consumer->getCookie(), true,
                                                &meta, backfillPhase, false,
                                                deletion->getBySeqno(), true);
}

void PassiveStream::processMarker(SnapshotMarker* marker) {
    RCPtr<VBucket> vb = engine->getVBucket(vb_);

    cur_snapshot_start = marker->getStartSeqno();
    cur_snapshot_end = marker->getEndSeqno();
    cur_snapshot_type = (marker->getFlags() & MARKER_FLAG_DISK) ? disk : memory;
    saveSnapshot = true;

    if (vb) {
        if (marker->getFlags() & MARKER_FLAG_DISK && vb->getHighSeqno() == 0) {
            vb->setBackfillPhase(true);
            /* When replica vb is in backfill phase, then open checkpoint id
               is set to 0. */
            vb->checkpointManager.setOpenCheckpointId(0);
        } else {
            if (marker->getFlags() & MARKER_FLAG_CHK ||
                vb->checkpointManager.getOpenCheckpointId() == 0) {
                uint64_t id = vb->checkpointManager.getOpenCheckpointId() + 1;
                vb->checkpointManager.checkAndAddNewCheckpoint(id, vb);
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
            lh.unlock();
            bool inverse = false;
            if (itemsReady.compare_exchange_strong(inverse, true)) {
                consumer->notifyStreamReady(vb_);
            }
            cur_snapshot_ack = false;
        }
        cur_snapshot_type = none;
        vb->setCurrentSnapshot(byseqno, byseqno);
    }
}

void PassiveStream::addStats(ADD_STAT add_stat, const void *c) {
    Stream::addStats(add_stat, c);

    const int bsize = 128;
    char buf[bsize];
    snprintf(buf, bsize, "%s:stream_%d_buffer_items", name_.c_str(), vb_);
    add_casted_stat(buf, buffer.items, add_stat, c);
    snprintf(buf, bsize, "%s:stream_%d_buffer_bytes", name_.c_str(), vb_);
    add_casted_stat(buf, buffer.bytes, add_stat, c);
    snprintf(buf, bsize, "%s:stream_%d_items_ready", name_.c_str(), vb_);
    add_casted_stat(buf, itemsReady.load() ? "true" : "false", add_stat, c);
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
        itemsReady.store(false);
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
