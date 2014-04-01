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

class CacheCallback : public Callback<CacheLookup> {
public:
    CacheCallback(EventuallyPersistentEngine* e, stream_t &s)
        : engine_(e), stream_(s) {
        Stream *str = stream_.get();
        if (str) {
            assert(str->getType() == STREAM_ACTIVE);
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
            assert(str->getType() == STREAM_ACTIVE);
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
        Stream *str = stream.get();
        if (str) {
            assert(str->getType() == STREAM_ACTIVE);
        }

        KVStore* kvstore = engine->getEpStore()->getAuxUnderlying();
        numItems = kvstore->getNumItems(s->getVBucket(), startSeqno, endSeqno);
        static_cast<ActiveStream*>(stream.get())->incrBackfillRemaining(numItems);
    }

    bool run();

    std::string getDescription();

private:
    EventuallyPersistentEngine *engine;
    stream_t             stream;
    uint64_t                    startSeqno;
    uint64_t                    endSeqno;
    size_t                      numItems;
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

    KVStore* kvstore = engine->getEpStore()->getAuxUnderlying();

    if (numItems > 0) {
        shared_ptr<Callback<GetValue> >
            cb(new DiskCallback(stream));
        shared_ptr<Callback<CacheLookup> >
            cl(new CacheCallback(engine, stream));
        kvstore->dump(vbid, startSeqno, endSeqno, cb, cl);
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
               uint64_t vb_uuid, uint64_t high_seqno)
    : name_(name), flags_(flags), opaque_(opaque), vb_(vb),
      start_seqno_(start_seqno), end_seqno_(end_seqno), vb_uuid_(vb_uuid),
      high_seqno_(high_seqno), state_(STREAM_PENDING), itemsReady(false) {
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
    snprintf(buffer, bsize, "%s:stream_%d_high_seqno", name_.c_str(), vb_);
    add_casted_stat(buffer, high_seqno_, add_stat, c);
    snprintf(buffer, bsize, "%s:stream_%d_state", name_.c_str(), vb_);
    add_casted_stat(buffer, stateName(state_), add_stat, c);
}

ActiveStream::ActiveStream(EventuallyPersistentEngine* e, UprProducer* p,
                           const std::string &n, uint32_t flags,
                           uint32_t opaque, uint16_t vb, uint64_t st_seqno,
                           uint64_t en_seqno, uint64_t vb_uuid,
                           uint64_t hi_seqno)
    :  Stream(n, flags, opaque, vb, st_seqno, en_seqno, vb_uuid, hi_seqno),
       lastReadSeqno(st_seqno), lastSentSeqno(st_seqno), curChkSeqno(st_seqno),
       takeoverSeqno(0), takeoverState(vbucket_state_pending),
       backfillRemaining(0), itemsFromBackfill(0), itemsFromMemory(0),
       engine(e), producer(p), isBackfillTaskRunning(false) {

    if (flags_ & UPR_ADD_STREAM_FLAG_TAKEOVER) {
        end_seqno_ = uprMaxSeqno;
    }

    if (start_seqno_ >= end_seqno_) {
        endStream(END_STREAM_OK);
        itemsReady = true;
    }

    type_ = STREAM_ACTIVE;
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
            LOG(EXTENSION_LOG_WARNING, "%s Invalid state '%s' for vbucket %d",
                producer->logHeader(), stateName(state_), vb_);
            abort();
    }

    if (state_ != STREAM_DEAD && initState != state_ && !response) {
        lh.unlock();
        return next();
    }

    itemsReady = response ? true : false;
    return response;
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
        readyQ.push(new SnapshotMarker(opaque_, vb_));
        LOG(EXTENSION_LOG_WARNING, "%s Backfill complete for vb %d, last seqno "
            "read: %ld", producer->logHeader(), vb_, lastReadSeqno);

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
        } else {
            endStream(END_STREAM_OK);
        }

        if (!itemsReady) {
            itemsReady = true;
            lh.unlock();
            producer->notifyStreamReady(vb_, true);
        }
    } else {
        LOG(EXTENSION_LOG_WARNING, "%s Unexpected ack for set vbucket op on vb "
            "%d stream '%s' state '%s'", producer->logHeader(), vb_,
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
    }

    return resp;
}

UprResponse* ActiveStream::inMemoryPhase() {
    UprResponse* resp = nextQueuedItem();

    if (!resp) {
        resp = nextCheckpointItem();
        if (resp && lastSentSeqno >= end_seqno_) {
            readyQ.push(new SnapshotMarker(opaque_, vb_));
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
            readyQ.push(new SnapshotMarker(opaque_, vb_));
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
    queued_item qi = vbucket->checkpointManager.nextItem(name_, isLast);

    if (qi->getOperation() == queue_op_checkpoint_start) {
        qi = vbucket->checkpointManager.nextItem(name_, isLast);
    }

    if (qi->getOperation() == queue_op_set ||
        qi->getOperation() == queue_op_del) {
        lastReadSeqno = qi->getBySeqno();
        lastSentSeqno = qi->getBySeqno();
        curChkSeqno = qi->getBySeqno();
        itemsFromMemory++;
    }

    if (qi->getOperation() == queue_op_set ||
        qi->getOperation() == queue_op_del) {
        Item* itm = new Item(qi->getKey(), qi->getFlags(), qi->getExptime(),
                             qi->getValue(), qi->getCas(), qi->getBySeqno(),
                             qi->getVBucketId(), qi->getRevSeqno());

        itm->setNRUValue(qi->getNRUValue());

        if (qi->isDeleted()) {
            itm->setDeleted();
        }
        return new MutationResponse(itm, opaque_);
    } else if (qi->getOperation() == queue_op_checkpoint_end) {
        return new SnapshotMarker(opaque_, vb_);
    }
    return NULL;
}

void ActiveStream::setDead(end_stream_status_t status) {
    LockHolder lh(streamMutex);
    endStream(status);

    if (!itemsReady && status != END_STREAM_DISCONNECTED) {
        itemsReady = true;
        lh.unlock();
        producer->notifyStreamReady(vb_, true);
    }
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
        if (lastReadSeqno < curChkSeqno) {
            uint64_t backfillEnd = end_seqno_;
            uint64_t backfillStart = lastReadSeqno + 1;

            if (curChkSeqno < backfillEnd) {
                backfillEnd = curChkSeqno - 1;
            }
            LOG(EXTENSION_LOG_WARNING, "Scheduling backfill for vb %d (%d to %d)",
                vb_, lastReadSeqno, curChkSeqno);
            ExTask task = new UprBackfill(engine, this, backfillStart, backfillEnd,
                                          Priority::TapBgFetcherPriority, 0, false);
            ExecutorPool::get()->schedule(task, AUXIO_TASK_IDX);
            isBackfillTaskRunning = true;
        }
    }
}

void ActiveStream::transitionState(stream_state_t newState) {
    LOG(EXTENSION_LOG_DEBUG, "%s Transitioning from %s to %s",
        producer->logHeader(), stateName(state_), stateName(newState));

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
            LOG(EXTENSION_LOG_WARNING, "%s Invalid Transition from %s to %s",
                producer->logHeader(), stateName(state_), newState);
            abort();
    }

    switch (newState) {
        case STREAM_BACKFILLING:
            scheduleBackfill();
            break;
        case STREAM_TAKEOVER_SEND:
            takeoverSeqno = engine->getVBucket(vb_)->getHighSeqno();
            break;
        case STREAM_DEAD:
            engine->getVBucket(vb_)->checkpointManager.removeTAPCursor(name_);
            break;
        default:
            break;
    }

    state_ = newState;
}

NotifierStream::NotifierStream(EventuallyPersistentEngine* e, UprProducer* p,
                               const std::string &name, uint32_t flags,
                               uint32_t opaque, uint16_t vb, uint64_t st_seqno,
                               uint64_t en_seqno, uint64_t vb_uuid,
                               uint64_t hi_seqno)
    : Stream(name, flags, opaque, vb, st_seqno, en_seqno, vb_uuid, hi_seqno),
      producer(p) {
    LockHolder lh(streamMutex);
    RCPtr<VBucket> vbucket = e->getVBucket(vb_);
    if (vbucket && static_cast<uint64_t>(vbucket->getHighSeqno()) > st_seqno) {
        readyQ.push(new StreamEndResponse(opaque_, END_STREAM_OK, vb_));
        transitionState(STREAM_DEAD);
        itemsReady = true;
    }

    type_ = STREAM_NOTIFIER;
}

void NotifierStream::setDead(end_stream_status_t status) {
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
    LOG(EXTENSION_LOG_DEBUG, "Transitioning from %s to %s", stateName(state_),
        stateName(newState));

    if (state_ == newState) {
        return;
    }

    switch (state_) {
        case STREAM_PENDING:
            assert(newState == STREAM_DEAD);
            break;
        default:
            LOG(EXTENSION_LOG_WARNING, "Invalid Transition from %s to %s",
                stateName(state_), newState);
            abort();
    }

    state_ = newState;
}

PassiveStream::PassiveStream(EventuallyPersistentEngine* e, UprConsumer* c,
                             const std::string &name, uint32_t flags,
                             uint32_t opaque, uint16_t vb, uint64_t st_seqno,
                             uint64_t en_seqno, uint64_t vb_uuid,
                             uint64_t hi_seqno)
    : Stream(name, flags, opaque, vb, st_seqno, en_seqno, vb_uuid, hi_seqno),
      engine(e), consumer(c), last_seqno(st_seqno), backfill_phase(false) {
    LockHolder lh(streamMutex);
    readyQ.push(new StreamRequest(vb, opaque, flags, st_seqno, en_seqno,
                                  vb_uuid, hi_seqno));
    itemsReady = true;
    type_ = STREAM_PASSIVE;
}

void PassiveStream::setDead(end_stream_status_t status) {
    LockHolder lh(streamMutex);
    transitionState(STREAM_DEAD);
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
    high_seqno_ = vb->failovers->getLatestEntry().by_seqno;
    LockHolder lh(streamMutex);
    readyQ.push(new StreamRequest(vb_, new_opaque, flags_, start_seqno,
                                  end_seqno_, vb_uuid_, high_seqno_));
    if (!itemsReady) {
        itemsReady = true;
        lh.unlock();
        consumer->notifyStreamReady(vb_);
    }
}

ENGINE_ERROR_CODE PassiveStream::messageReceived(UprResponse* resp) {
    LockHolder lh(streamMutex);
    assert(resp);

    if (resp->getEvent() == UPR_DELETION || resp->getEvent() == UPR_MUTATION) {
        uint64_t bySeqno = static_cast<MutationResponse*>(resp)->getBySeqno();
        if (bySeqno <= last_seqno) {
            LOG(EXTENSION_LOG_INFO, "%s Dropping upr mutation for vbucket %d "
                "with opaque %ld because the byseqno given (%llu) must be "
                "larger than %llu", consumer->logHeader(), vb_, opaque_,
                bySeqno, last_seqno);
            return ENGINE_ERANGE;
        }
        last_seqno = bySeqno;
    }

    buffer.messages.push(resp);
    buffer.items++;
    buffer.bytes += resp->getMessageSize();

    return ENGINE_SUCCESS;
}

bool PassiveStream::processBufferedMessages() {
    LockHolder lh(streamMutex);
    if (buffer.messages.empty()) {
        return false;
    }

    UprResponse* response = buffer.messages.front();
    buffer.messages.pop();
    buffer.items--;
    buffer.bytes -= response->getMessageSize();
    lh.unlock();

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

    return true;
}

void PassiveStream::processMutation(MutationResponse* mutation) {
    ENGINE_ERROR_CODE ret;
    if (backfill_phase) {
        ret = engine->getEpStore()->addTAPBackfillItem(*mutation->getItem(),
                                                       INITIAL_NRU_VALUE);
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
    uint64_t delCas = 0;
    ENGINE_ERROR_CODE ret;
    ItemMetaData meta = deletion->getItem()->getMetaData();
    ret = engine->getEpStore()->deleteWithMeta(deletion->getItem()->getKey(),
                                               &delCas, deletion->getVBucket(),
                                               consumer->getCookie(), true,
                                               &meta, backfill_phase, false,
                                               deletion->getBySeqno());
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

}

void PassiveStream::processSetVBucketState(SetVBucketState* state) {
    engine->getEpStore()->setVBucketState(vb_, state->getState(), false);
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

void PassiveStream::transitionState(stream_state_t newState) {
    LOG(EXTENSION_LOG_DEBUG, "%s Transitioning from %s to %s",
        consumer->logHeader(), stateName(state_), stateName(newState));

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
            LOG(EXTENSION_LOG_WARNING, "%s Invalid Transition from %s to %s",
                consumer->logHeader(), stateName(state_), newState);
            abort();
    }

    state_ = newState;
}
