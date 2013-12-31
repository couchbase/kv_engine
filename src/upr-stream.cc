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
#include "kvstore.h"
#include "statwriter.h"
#include "upr-stream.h"
#include "upr-response.h"

class CacheCallback : public Callback<CacheLookup> {
public:
    CacheCallback(EventuallyPersistentEngine* e, ActiveStream *s) :
        engine_(e), stream_(s) {}

    void callback(CacheLookup &lookup);

private:
    EventuallyPersistentEngine* engine_;
    ActiveStream* stream_;
};

void CacheCallback::callback(CacheLookup &lookup) {
    RCPtr<VBucket> vb = engine_->getEpStore()->getVBucket(lookup.getVBucketId());
    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(lookup.getKey(), &bucket_num);
    StoredValue *v = vb->ht.unlocked_find(lookup.getKey(), bucket_num);
    if (v && v->isResident() && v->getBySeqno() == lookup.getBySeqno()) {
        Item* it = v->toItem(false, lookup.getVBucketId());
        lh.unlock();
        stream_->backfillReceived(it);
        setStatus(ENGINE_KEY_EEXISTS);
    } else {
        setStatus(ENGINE_SUCCESS);
    }
}

class DiskCallback : public Callback<GetValue> {
public:
    DiskCallback(ActiveStream* s) :
        stream_(s) {}

    void callback(GetValue &val) {
        assert(val.getValue());
        stream_->backfillReceived(val.getValue());
    }

private:
    ActiveStream* stream_;
};

class UprBackfill : public GlobalTask {
public:
    UprBackfill(EventuallyPersistentEngine* e, ActiveStream *s,
                uint64_t start_seqno, uint64_t end_seqno, const Priority &p,
                double sleeptime = 0, bool shutdown = false) :
        GlobalTask(e, p, sleeptime, shutdown), engine(e), stream(s),
        startSeqno(start_seqno), endSeqno(end_seqno) {

    }

    bool run();

    std::string getDescription();

private:
    EventuallyPersistentEngine *engine;
    ActiveStream               *stream;
    uint64_t                    startSeqno;
    uint64_t                    endSeqno;
};

bool UprBackfill::run() {
    uint16_t vbucket = stream->getVBucket();
    KVStore* kvstore = engine->getEpStore()->getAuxUnderlying();
    size_t num_items = kvstore->getNumItems(vbucket);
    size_t num_deleted = kvstore->getNumPersistedDeletes(vbucket);

    if ((num_items + num_deleted) > 0) {
        stream->incrBackfillRemaining(num_items + num_deleted);

        shared_ptr<Callback<GetValue> >
            cb(new DiskCallback(stream));
        shared_ptr<Callback<CacheLookup> >
            cl(new CacheCallback(engine, stream));
        kvstore->dump(vbucket, startSeqno, endSeqno, cb, cl);
    }

    stream->completeBackfill();

    return false;
}

std::string UprBackfill::getDescription() {
    std::stringstream ss;
    ss << "Upr backfill for vbucket " << stream->getVBucket();
    return ss.str();
}

const char * Stream::stateName(stream_state_t st) const {
    static const char * const stateNames[] = {
        "pending", "backfilling", "in-memory", "reading", "dead"
    };
    assert(st >= STREAM_PENDING && st <= STREAM_DEAD);
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

ActiveStream::ActiveStream(EventuallyPersistentEngine* e, std::string &n,
                           uint32_t flags, uint32_t opaque, uint16_t vb,
                           uint64_t st_seqno, uint64_t en_seqno,
                           uint64_t vb_uuid, uint64_t hi_seqno)
    :  Stream(n, flags, opaque, vb, st_seqno, en_seqno, vb_uuid, hi_seqno),
       lastReadSeqno(st_seqno), lastSentSeqno(st_seqno), curChkSeqno(st_seqno),
       backfillRemaining(0), engine(e), isBackfillTaskRunning(false) {
    if (start_seqno_ > end_seqno_) {
        endStream(END_STREAM_OK);
    }
}

UprResponse* ActiveStream::next() {
    LockHolder lh(streamMutex);
    switch (state_) {
        case STREAM_PENDING:
            return NULL;
        case STREAM_BACKFILLING:
            return backfillPhase();
        case STREAM_IN_MEMORY:
            return inMemoryPhase();
        case STREAM_DEAD:
            return deadPhase();
        default:
            LOG(EXTENSION_LOG_WARNING, "Invalid state '%s' for vbucket %d",
                stateName(state_), vb_);
            abort();
    }
}

void ActiveStream::clear_UNLOCKED() {
    while (!readyQ.empty()) {
        UprResponse* resp = readyQ.front();
        delete resp;
        readyQ.pop();
    }
}

void ActiveStream::backfillReceived(Item* itm) {
    LockHolder lh(streamMutex);
    if (state_ == STREAM_BACKFILLING) {
        MutationResponse* m = new MutationResponse(itm, opaque_);
        readyQ.push(m);

        lastReadSeqno = itm->getBySeqno();
        backfillRemaining--;
    } else {
        delete itm;
    }
}

void ActiveStream::completeBackfill() {
    LockHolder lh(streamMutex);
    if (state_ == STREAM_BACKFILLING) {
        backfillRemaining = 0;
        isBackfillTaskRunning = false;
        readyQ.push(new SnapshotMarker(opaque_, vb_));
        LOG(EXTENSION_LOG_WARNING, "Backfill complete for vb %d, last seqno "
            "read: %ld", lastReadSeqno);
        if (lastReadSeqno >= end_seqno_) {
            endStream(END_STREAM_OK);
        } else {
            transitionState(STREAM_IN_MEMORY);
        }
    }
}

UprResponse* ActiveStream::backfillPhase() {
    if (!isBackfillTaskRunning) {
        RCPtr<VBucket> vbucket = engine->getVBucket(vb_);
        curChkSeqno = vbucket->checkpointManager.registerTAPCursorBySeqno(name_, lastReadSeqno);

        if (lastReadSeqno < curChkSeqno) {
            uint64_t backfillEnd = end_seqno_;
            if (curChkSeqno < backfillEnd) {
                backfillEnd = curChkSeqno - 1;
            }
            LOG(EXTENSION_LOG_WARNING, "Scheduling backfill for vb %d (%d to %d)",
                vb_, lastReadSeqno, curChkSeqno);
            ExTask task = new UprBackfill(engine, this, lastReadSeqno, backfillEnd,
                                          Priority::TapBgFetcherPriority, 0, false);
            ExecutorPool::get()->schedule(task, AUXIO_TASK_IDX);
            isBackfillTaskRunning = true;
        } else {
            transitionState(STREAM_IN_MEMORY);
        }
    }

    if (!readyQ.empty()) {
        UprResponse* response = readyQ.front();
        if (response->getEvent() == UPR_MUTATION) {
            lastSentSeqno = dynamic_cast<MutationResponse*>(response)->getBySeqno();
        }
        readyQ.pop();
        return response;
    }
    return NULL;
}

UprResponse* ActiveStream::inMemoryPhase() {
    if (readyQ.empty()) {
        RCPtr<VBucket> vbucket = engine->getVBucket(vb_);
        bool isLast;
        queued_item qi = vbucket->checkpointManager.nextItem(name_, isLast);

        if (qi->getOperation() == queue_op_set ||
            qi->getOperation() == queue_op_del) {
            lastReadSeqno = qi->getBySeqno();
        }

        UprResponse* resp = NULL;
        if (qi->getOperation() == queue_op_set ||
            qi->getOperation() == queue_op_del) {
            Item* itm = new Item(qi->getKey(), qi->getFlags(), qi->getExptime(),
                            qi->getValue(), qi->getCas(), qi->getBySeqno(),
                            qi->getVBucketId(), qi->getRevSeqno());
            if (qi->isDeleted()) {
                itm->setDeleted();
            }
            resp = new MutationResponse(itm, opaque_);
        } else if (qi->getOperation() == queue_op_checkpoint_end) {
            resp = new SnapshotMarker(opaque_, vb_);
        }

        if (lastReadSeqno >= end_seqno_) {
            readyQ.push(new SnapshotMarker(opaque_, vb_));
            endStream(END_STREAM_OK);
        }

        return resp;
    } else {
        UprResponse* response = readyQ.front();
        if (response->getEvent() == UPR_MUTATION) {
            lastSentSeqno = dynamic_cast<MutationResponse*>(response)->getBySeqno();
        }
        readyQ.pop();
        return response;
    }
}

UprResponse* ActiveStream::deadPhase() {
    if (!readyQ.empty()) {
        UprResponse* response = readyQ.front();
        if (response->getEvent() == UPR_MUTATION) {
            lastSentSeqno = dynamic_cast<MutationResponse*>(response)->getBySeqno();
        }
        readyQ.pop();
        return response;
    }
    return NULL;
}

void ActiveStream::endStream(end_stream_status_t reason) {
    readyQ.push(new StreamEndResponse(opaque_, reason, vb_));
    transitionState(STREAM_DEAD);
}

void ActiveStream::transitionState(stream_state_t newState) {
    LOG(EXTENSION_LOG_DEBUG, "Transitioning from %s to %s", stateName(state_),
        stateName(newState));

    if (state_ == newState) {
        return;
    }

    switch (state_) {
        case STREAM_PENDING:
            assert(newState == STREAM_BACKFILLING || newState == STREAM_DEAD);
            break;
        case STREAM_BACKFILLING:
            assert(newState == STREAM_IN_MEMORY || newState == STREAM_DEAD);
            break;
        case STREAM_IN_MEMORY:
            assert(newState == STREAM_BACKFILLING || newState == STREAM_DEAD);
            break;
        default:
            LOG(EXTENSION_LOG_WARNING, "Invalid Transition from %s to %s",
                stateName(state_), newState);
            abort();
    }

    if (newState == STREAM_DEAD) {
        engine->getVBucket(vb_)->checkpointManager.removeTAPCursor(name_);
    }

    state_ = newState;
}
