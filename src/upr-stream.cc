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
#include "upr-stream.h"
#include "upr-consumer.h"
#include "upr-producer.h"
#include "upr-response.h"

#define UPR_BACKFILL_SLEEP_TIME 2

const uint64_t Stream::uprMaxSeqno = std::numeric_limits<uint64_t>::max();
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

class UprBackfill : public GlobalTask {
public:
    UprBackfill(EventuallyPersistentEngine* e, stream_t s,
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

bool UprBackfill::run() {
    uint16_t vbid = stream->getVBucket();
    uint64_t lastPersistedSeqno =
        engine->getEpStore()->getVBuckets().getPersistenceSeqno(vbid);

    if (lastPersistedSeqno < endSeqno) {
        LOG(EXTENSION_LOG_WARNING, "Rescheduling backfill for vbucket %d "
            "because backfill up to seqno %llu is needed but only up to "
            "%llu is persisted", vbid, endSeqno,
            lastPersistedSeqno);
        snooze(UPR_BACKFILL_SLEEP_TIME);
        return true;
    }

    KVStore* kvstore = engine->getEpStore()->getROUnderlying(vbid);
    size_t numItems = kvstore->getNumItems(vbid, startSeqno,
                                           std::numeric_limits<uint64_t>::max());
    static_cast<ActiveStream*>(stream.get())->incrBackfillRemaining(numItems);

    if (numItems > 0) {
        shared_ptr<Callback<GetValue> >
            cb(new DiskCallback(stream));
        shared_ptr<Callback<CacheLookup> >
            cl(new CacheCallback(engine, stream));
        shared_ptr<Callback<SeqnoRange> >
            sr(new SnapshotMarkerCallback(stream));
        kvstore->dump(vbid, startSeqno, cb, cl, sr);
    }

    static_cast<ActiveStream*>(stream.get())->completeBackfill();

    return false;
}

std::string UprBackfill::getDescription() {
    std::stringstream ss;
    ss << "Upr backfill for vbucket " << stream->getVBucket();
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
      state_(STREAM_PENDING), itemsReady(false) {
}

void Stream::clear_UNLOCKED() {
    while (!readyQ.empty()) {
        UprResponse* resp = readyQ.front();
        delete resp;
        readyQ.pop();
    }
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

ActiveStream::ActiveStream(EventuallyPersistentEngine* e, UprProducer* p,
                           const std::string &n, uint32_t flags,
                           uint32_t opaque, uint16_t vb, uint64_t st_seqno,
                           uint64_t en_seqno, uint64_t vb_uuid,
                           uint64_t snap_start_seqno, uint64_t snap_end_seqno)
    :  Stream(n, flags, opaque, vb, st_seqno, en_seqno, vb_uuid,
              snap_start_seqno, snap_end_seqno),
       lastReadSeqno(st_seqno), lastSentSeqno(st_seqno), curChkSeqno(st_seqno),
       takeoverSeqno(0), takeoverState(vbucket_state_pending),
       backfillRemaining(0), itemsFromBackfill(0), itemsFromMemory(0),
       engine(e), producer(p), isBackfillTaskRunning(false),
       isFirstMemoryMarker(true), isFirstSnapshot(true) {

    const char* type = "";
    if (flags_ & UPR_ADD_STREAM_FLAG_TAKEOVER) {
        type = "takeover ";
        end_seqno_ = uprMaxSeqno;
    }

    if (start_seqno_ >= end_seqno_) {
        endStream(END_STREAM_OK);
        itemsReady = true;
    }

    type_ = STREAM_ACTIVE;

    LOG(EXTENSION_LOG_WARNING, "%s (vb %d) %sstream created with start seqno "
        "%llu and end seqno %llu", producer->logHeader(), vb, type, st_seqno,
        en_seqno);
}

UprResponse* ActiveStream::next() {
    LockHolder lh(streamMutex);

    stream_state_t initState = state_;

    UprResponse* response = NULL;
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
    isFirstSnapshot = false;
    startSeqno = std::min(snap_start_seqno_, startSeqno);

    LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Sending disk snapshot with start "
        "seqno %llu and end seqno %llu", producer->logHeader(), vb_, startSeqno,
        endSeqno);
    readyQ.push(new SnapshotMarker(opaque_, vb_, startSeqno, endSeqno,
                                   MARKER_FLAG_DISK));
    RCPtr<VBucket> vb = engine->getVBucket(vb_);
    if (!vb) {
        endStream(END_STREAM_STATE);
    } else {
        if (endSeqno > end_seqno_) {
            endSeqno = end_seqno_;
        }
        // Only re-register the cursor if we still need to get memory snapshots
        curChkSeqno = vb->checkpointManager.registerTAPCursorBySeqno(name_,
                                                                     endSeqno,
                                                                     end_seqno_);
    }

    if (!itemsReady) {
        itemsReady = true;
        lh.unlock();
        producer->notifyStreamReady(vb_, false);
    }
}

void ActiveStream::backfillReceived(Item* itm) {
    LockHolder lh(streamMutex);
    if (state_ == STREAM_BACKFILLING) {
        readyQ.push(new MutationResponse(itm, opaque_));
        lastReadSeqno = itm->getBySeqno();
        itemsFromBackfill++;

        if (!itemsReady) {
            itemsReady = true;
            lh.unlock();
            producer->notifyStreamReady(vb_, false);
        }
    } else {
        delete itm;
    }
}

void ActiveStream::completeBackfill() {
    LockHolder lh(streamMutex);

    if (state_ == STREAM_BACKFILLING) {
        isBackfillTaskRunning = false;
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Backfill complete, %d items read"
            " from disk, last seqno read: %ld", producer->logHeader(), vb_,
            itemsFromBackfill, lastReadSeqno);

        if (!itemsReady) {
            itemsReady = true;
            lh.unlock();
            producer->notifyStreamReady(vb_, false);
        }
    }
}

void ActiveStream::setVBucketStateAckRecieved() {
    LockHolder lh(streamMutex);
    if (state_ == STREAM_TAKEOVER_WAIT) {
        if (takeoverState == vbucket_state_pending) {
            RCPtr<VBucket> vbucket = engine->getVBucket(vb_);
            engine->getEpStore()->setVBucketState(vb_, vbucket_state_dead,
                                                  false, false);
            takeoverSeqno = vbucket->getHighSeqno();
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

UprResponse* ActiveStream::backfillPhase() {
    UprResponse* resp = nextQueuedItem();

    if (resp && backfillRemaining > 0 &&
        (resp->getEvent() == UPR_MUTATION ||
         resp->getEvent() == UPR_DELETION ||
         resp->getEvent() == UPR_EXPIRATION)) {
        backfillRemaining--;
    }

    if (!isBackfillTaskRunning && readyQ.empty()) {
        backfillRemaining = 0;
        if (lastReadSeqno >= end_seqno_) {
            endStream(END_STREAM_OK);
        } else if (flags_ & UPR_ADD_STREAM_FLAG_TAKEOVER) {
            transitionState(STREAM_TAKEOVER_SEND);
        } else if (flags_ & UPR_ADD_STREAM_FLAG_DISKONLY) {
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

UprResponse* ActiveStream::inMemoryPhase() {
    UprResponse* resp = nextQueuedItem();

    if (!resp) {
        resp = nextCheckpointItem();
        if (resp && lastSentSeqno >= end_seqno_) {
            endStream(END_STREAM_OK);
        }
    }
    return resp;
}

UprResponse* ActiveStream::takeoverSendPhase() {
    UprResponse* resp = nextQueuedItem();

    if (!resp) {
        resp = nextCheckpointItem();
        if (lastSentSeqno >= takeoverSeqno) {
            readyQ.push(new SetVBucketState(opaque_, vb_, takeoverState));
            transitionState(STREAM_TAKEOVER_WAIT);
        }
    }
    return resp;
}

UprResponse* ActiveStream::takeoverWaitPhase() {
    return nextQueuedItem();
}

UprResponse* ActiveStream::deadPhase() {
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
    snprintf(buffer, bsize, "%s:stream_%d_items_ready", name_.c_str(), vb_);
    add_casted_stat(buffer, itemsReady ? "true" : "false", add_stat, c);
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

    if (end_seqno_ < curChkSeqno) {
        chk_items = 0;
    } else if ((end_seqno_ - curChkSeqno) < chk_items) {
        chk_items = end_seqno_ - curChkSeqno + 1;
    }
    total += chk_items;

    add_casted_stat("estimate", total, add_stat, cookie);
    add_casted_stat("chk_items", chk_items, add_stat, cookie);
    add_casted_stat("vb_items", vb_items, add_stat, cookie);
}

UprResponse* ActiveStream::nextQueuedItem() {
    if (!readyQ.empty()) {
        UprResponse* response = readyQ.front();
        if (response->getEvent() == UPR_MUTATION ||
            response->getEvent() == UPR_DELETION ||
            response->getEvent() == UPR_EXPIRATION) {
            lastSentSeqno = dynamic_cast<MutationResponse*>(response)->getBySeqno();
        }
        readyQ.pop();
        return response;
    }
    return NULL;
}

UprResponse* ActiveStream::nextCheckpointItem() {
    RCPtr<VBucket> vbucket = engine->getVBucket(vb_);

    bool isLast;
    uint64_t snapEnd = 0;
    queued_item qi = vbucket->checkpointManager.nextItem(name_, isLast, snapEnd);

    if (qi->getOperation() == queue_op_checkpoint_end) {
        qi = vbucket->checkpointManager.nextItem(name_, isLast, snapEnd);
    }

    uint64_t snapStart = qi->getBySeqno();
    if (isFirstSnapshot) {
        snapStart = snap_start_seqno_;
    }
    snapEnd = std::min(snapEnd, end_seqno_);

    UprResponse* resp = NULL;
    if (qi->getOperation() == queue_op_set ||
        qi->getOperation() == queue_op_del) {
        lastReadSeqno = qi->getBySeqno();
        lastSentSeqno = qi->getBySeqno();
        curChkSeqno = qi->getBySeqno();
        itemsFromMemory++;

        Item* itm = new Item(qi->getKey(), qi->getFlags(), qi->getExptime(),
                             qi->getValue(), qi->getCas(), qi->getBySeqno(),
                             qi->getVBucketId(), qi->getRevSeqno());

        itm->setNRUValue(qi->getNRUValue());

        if (qi->isDeleted()) {
            itm->setDeleted();
        }
        resp = new MutationResponse(itm, opaque_);
    } else if (qi->getOperation() == queue_op_checkpoint_start) {
        isFirstMemoryMarker = false;
        isFirstSnapshot = false;
        return new SnapshotMarker(opaque_, vb_, snapStart,
                                  snapEnd, MARKER_FLAG_MEMORY);
    }

    if (isFirstMemoryMarker) {
        if (resp) {
            readyQ.push(resp);
        }
        isFirstMemoryMarker = false;
        isFirstSnapshot = false;
        resp = new SnapshotMarker(opaque_, vb_, snapStart,
                                  snapEnd, MARKER_FLAG_MEMORY);
    }

    return resp;
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
        if (reason != END_STREAM_DISCONNECTED) {
            readyQ.push(new StreamEndResponse(opaque_, reason, vb_));
        }
        transitionState(STREAM_DEAD);
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Stream closing, %llu items sent"
            " from disk, %llu items sent from memory, %llu was last seqno sent",
            producer->logHeader(), vb_, itemsFromBackfill, itemsFromMemory,
            lastSentSeqno);
    }
}

void ActiveStream::scheduleBackfill() {
    if (!isBackfillTaskRunning) {
        RCPtr<VBucket> vbucket = engine->getVBucket(vb_);
        if (!vbucket) {
            return;
        }

        curChkSeqno = vbucket->checkpointManager.registerTAPCursorBySeqno(name_,
                                                                  lastReadSeqno,
                                                                    end_seqno_);

        cb_assert(lastReadSeqno <= curChkSeqno);
        uint64_t backfillStart = lastReadSeqno + 1;

        /* We need to find the minimum seqno that needs to be backfilled in
         * order to make sure that we don't miss anything when transitioning
         * to a memory snapshot. The backfill task will always make sure that
         * the backfill end seqno is contained in the backfill.
         */
        uint64_t backfillEnd = 0;
        if (flags_ & UPR_ADD_STREAM_FLAG_DISKONLY) { // disk backfill only
            backfillEnd = vbucket->getHighSeqno();
        } else { // disk backfill + in-memory streaming
            if (backfillStart < curChkSeqno) {
                if (curChkSeqno >= end_seqno_) {
                    backfillEnd = end_seqno_;
                } else {
                    backfillEnd = curChkSeqno - 1;
                }
            }
        }

        if (backfillStart <= backfillEnd) {
            ExTask task = new UprBackfill(engine, this, backfillStart, backfillEnd,
                                          Priority::TapBgFetcherPriority, 0, false);
            ExecutorPool::get()->schedule(task, AUXIO_TASK_IDX);
            isBackfillTaskRunning = true;
        } else {
            if (flags_ & UPR_ADD_STREAM_FLAG_TAKEOVER) {
                transitionState(STREAM_TAKEOVER_SEND);
            } else {
                transitionState(STREAM_IN_MEMORY);
            }
            itemsReady = true;
        }
    }
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
    } else if (newState == STREAM_IN_MEMORY){
        isFirstMemoryMarker = true;
    } else if (newState == STREAM_TAKEOVER_SEND) {
        takeoverSeqno = engine->getVBucket(vb_)->getHighSeqno();
    } else if (newState == STREAM_DEAD) {
        RCPtr<VBucket> vb = engine->getVBucket(vb_);
        if (vb) {
            vb->checkpointManager.removeTAPCursor(name_);
        }
    }
}

size_t ActiveStream::getItemsRemaining() {
    RCPtr<VBucket> vbucket = engine->getVBucket(vb_);

    if (!vbucket) {
        return 0;
    }

    uint64_t high_seqno = vbucket->getHighSeqno();

    if (end_seqno_ < high_seqno)
        return (end_seqno_ - lastSentSeqno);
    else
        return (high_seqno - lastSentSeqno);
}

NotifierStream::NotifierStream(EventuallyPersistentEngine* e, UprProducer* p,
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
        readyQ.push(new StreamEndResponse(opaque_, END_STREAM_OK, vb_));
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
            readyQ.push(new StreamEndResponse(opaque_, status, vb_));
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
        readyQ.push(new StreamEndResponse(opaque_, END_STREAM_OK, vb_));
        transitionState(STREAM_DEAD);
        if (!itemsReady) {
            itemsReady = true;
            lh.unlock();
            producer->notifyStreamReady(vb_, true);
        }
    }
}

UprResponse* NotifierStream::next() {
    LockHolder lh(streamMutex);

    if (readyQ.empty()) {
        itemsReady = false;
        return NULL;
    }

    UprResponse* response = readyQ.front();
    readyQ.pop();

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

PassiveStream::PassiveStream(EventuallyPersistentEngine* e, UprConsumer* c,
                             const std::string &name, uint32_t flags,
                             uint32_t opaque, uint16_t vb, uint64_t st_seqno,
                             uint64_t en_seqno, uint64_t vb_uuid,
                             uint64_t snap_start_seqno, uint64_t snap_end_seqno)
    : Stream(name, flags, opaque, vb, st_seqno, en_seqno, vb_uuid,
             snap_start_seqno, snap_end_seqno),
      engine(e), consumer(c), last_seqno(st_seqno) {
    LockHolder lh(streamMutex);
    readyQ.push(new StreamRequest(vb, opaque, flags, st_seqno, en_seqno,
                                  vb_uuid, snap_start_seqno, snap_end_seqno));
    itemsReady = true;
    type_ = STREAM_PASSIVE;

    const char* type = (flags & UPR_ADD_STREAM_FLAG_TAKEOVER) ? "takeover" : "";
    LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Attempting to add %s stream with "
        "start seqno %llu, end seqno %llu, vbucket uuid %llu, snap start seqno "
        "%llu, and snap end seqno %llu", consumer->logHeader(), vb, type,
        st_seqno, en_seqno, vb_uuid, snap_start_seqno, snap_end_seqno);
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
    uint32_t unackedBytes = buffer.bytes;
    clearBuffer();
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
        readyQ.push(new AddStreamResponse(add_opaque, opaque_, status));
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
    snap_start_seqno_ = vb->failovers->getLatestEntry().by_seqno;
    LockHolder lh(streamMutex);
    readyQ.push(new StreamRequest(vb_, new_opaque, flags_, start_seqno,
                                  end_seqno_, vb_uuid_,
                                  snap_start_seqno_, snap_start_seqno_));
    if (!itemsReady) {
        itemsReady = true;
        lh.unlock();
        consumer->notifyStreamReady(vb_);
    }
}

ENGINE_ERROR_CODE PassiveStream::messageReceived(UprResponse* resp) {
    LockHolder lh(streamMutex);
    cb_assert(resp);

    if (state_ == STREAM_DEAD) {
        if (resp->getEvent() == UPR_MUTATION) {
            delete static_cast<MutationResponse*>(resp)->getItem();
        }

        delete resp;
        return ENGINE_KEY_ENOENT;
    }

    if (resp->getEvent() == UPR_DELETION || resp->getEvent() == UPR_MUTATION) {
        MutationResponse* m = static_cast<MutationResponse*>(resp);
        uint64_t bySeqno = m->getBySeqno();
        if (bySeqno <= last_seqno) {
            LOG(EXTENSION_LOG_INFO, "%s Dropping upr mutation for vbucket %d "
                "with opaque %ld because the byseqno given (%llu) must be "
                "larger than %llu", consumer->logHeader(), vb_, opaque_,
                bySeqno, last_seqno);
            delete m->getItem();
            delete m;
            return ENGINE_ERANGE;
        }
        last_seqno = bySeqno;
    }

    buffer.messages.push(resp);
    buffer.items++;
    buffer.bytes += resp->getMessageSize();

    return ENGINE_SUCCESS;
}

uint32_t PassiveStream::processBufferedMessages() {
    LockHolder lh(streamMutex);
    uint32_t count = 0;
    uint32_t total_bytes_processed = 0;
    std::queue<UprResponse*> mesg_queue;

    if (buffer.messages.empty()) {
        return 0;
    }

    while (count < PassiveStream::batchSize && !buffer.messages.empty()) {
        UprResponse* response = buffer.messages.front();

        uint32_t message_bytes = response->getMessageSize();
        buffer.messages.pop();
        buffer.items--;
        buffer.bytes -= message_bytes;
        count++;
        mesg_queue.push(response);
        total_bytes_processed += message_bytes;
    }

    lh.unlock();

    while (!mesg_queue.empty()) {
        UprResponse* response = mesg_queue.front();

        switch (response->getEvent()) {
            case UPR_MUTATION:
                processMutation(static_cast<MutationResponse*>(response));
                break;
            case UPR_DELETION:
            case UPR_EXPIRATION:
                processDeletion(static_cast<MutationResponse*>(response));
                break;
            case UPR_SNAPSHOT_MARKER:
                processMarker(static_cast<SnapshotMarker*>(response));
                break;
            case UPR_SET_VBUCKET:
                processSetVBucketState(static_cast<SetVBucketState*>(response));
                break;
            case UPR_STREAM_END:
                transitionState(STREAM_DEAD);
                break;
            default:
                abort();
        }

        mesg_queue.pop();
    }

    return total_bytes_processed;
}

void PassiveStream::processMutation(MutationResponse* mutation) {
    RCPtr<VBucket> vb = engine->getVBucket(vb_);
    if (!vb) {
        return;
    }

    ENGINE_ERROR_CODE ret;
    if (vb->isBackfillPhase()) {
        ret = engine->getEpStore()->addTAPBackfillItem(*mutation->getItem(),
                                                       INITIAL_NRU_VALUE,
                                                       false);
    } else {
        ret = engine->getEpStore()->setWithMeta(*mutation->getItem(), 0,
                                                consumer->getCookie(), true,
                                                true, INITIAL_NRU_VALUE, false);
    }

    delete mutation->getItem();
    delete mutation;

    // We should probably handle these error codes in a better way, but since
    // the producer side doesn't do anything with them anyways let's just log
    // them for now until we come up with a better solution.
    if (ret != ENGINE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING, "%s Got an error code %d while trying to "
            "process  mutation", consumer->logHeader(), ret);
    }
}

void PassiveStream::processDeletion(MutationResponse* deletion) {
    RCPtr<VBucket> vb = engine->getVBucket(vb_);
    if (!vb) {
        return;
    }

    uint64_t delCas = 0;
    ENGINE_ERROR_CODE ret;
    ItemMetaData meta = deletion->getItem()->getMetaData();
    ret = engine->getEpStore()->deleteWithMeta(deletion->getItem()->getKey(),
                                               &delCas, deletion->getVBucket(),
                                               consumer->getCookie(), true,
                                               &meta, vb->isBackfillPhase(),
                                               false, deletion->getBySeqno());
    if (ret == ENGINE_KEY_ENOENT) {
        ret = ENGINE_SUCCESS;
    }

    delete deletion->getItem();
    delete deletion;

    // We should probably handle these error codes in a better way, but since
    // the producer side doesn't do anything with them anyways let's just log
    // them for now until we come up with a better solution.
    if (ret != ENGINE_SUCCESS) {
        LOG(EXTENSION_LOG_WARNING, "%s Got an error code %d while trying to "
            "process  deletion", consumer->logHeader(), ret);
    }
}

void PassiveStream::processMarker(SnapshotMarker* marker) {
    RCPtr<VBucket> vb = engine->getVBucket(vb_);
    if (!vb) {
        return;
    }

    if (marker->getFlags() & MARKER_FLAG_DISK && vb->getHighSeqno() == 0) {
        vb->setBackfillPhase(true);
        vb->checkpointManager.checkAndAddNewCheckpoint(0, vb);
    } else {
        vb->setBackfillPhase(false);
        uint64_t id = vb->checkpointManager.getOpenCheckpointId() + 1;
        vb->checkpointManager.checkAndAddNewCheckpoint(id, vb);
    }
}

void PassiveStream::processSetVBucketState(SetVBucketState* state) {
    engine->getEpStore()->setVBucketState(vb_, state->getState(), true);
    delete state;

    LockHolder lh (streamMutex);
    readyQ.push(new SetVBucketStateResponse(opaque_, ENGINE_SUCCESS));
    if (!itemsReady) {
        itemsReady = true;
        lh.unlock();
        consumer->notifyStreamReady(vb_);
    }
}

void PassiveStream::addStats(ADD_STAT add_stat, const void *c) {
    Stream::addStats(add_stat, c);

    const int bsize = 128;
    char bbuffer[bsize];
    snprintf(bbuffer, bsize, "%s:stream_%d_buffer_items", name_.c_str(), vb_);
    add_casted_stat(bbuffer, buffer.items, add_stat, c);
    snprintf(bbuffer, bsize, "%s:stream_%d_buffer_bytes", name_.c_str(), vb_);
    add_casted_stat(bbuffer, buffer.bytes, add_stat, c);
    snprintf(bbuffer, bsize, "%s:stream_%d_items_ready", name_.c_str(), vb_);
    add_casted_stat(bbuffer, itemsReady ? "true" : "false", add_stat, c);
    snprintf(bbuffer, bsize, "%s:stream_%d_last_received_seqno", name_.c_str(),
             vb_);
    add_casted_stat(bbuffer, last_seqno, add_stat, c);
}

UprResponse* PassiveStream::next() {
    LockHolder lh(streamMutex);

    if (readyQ.empty()) {
        itemsReady = false;
        return NULL;
    }

    UprResponse* response = readyQ.front();
    readyQ.pop();
    return response;
}

void PassiveStream::clearBuffer() {
    while (!buffer.messages.empty()) {
        UprResponse* resp = buffer.messages.front();
        buffer.messages.pop();

        if (resp->getEvent() == UPR_MUTATION) {
            MutationResponse* m = static_cast<MutationResponse*> (resp);
            delete m->getItem();
        }
        delete resp;
    }

    buffer.bytes = 0;
    buffer.items = 0;
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
