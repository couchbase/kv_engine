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

#include "dcp-backfill.h"
#include "dcp-stream.h"
#include "ep_engine.h"

static const char* backfillStateToString(backfill_state_t state) {
    switch (state) {
        case backfill_state_init:
            return "initalizing";
        case backfill_state_scanning:
            return "scanning";
        case backfill_state_completing:
            return "completing";
        case backfill_state_done:
            return "done";
        default:
            abort();
    }
}

CacheCallback::CacheCallback(EventuallyPersistentEngine* e, stream_t &s)
    : engine_(e), stream_(s) {
    cb_assert(stream_.get() && stream_.get()->getType() == STREAM_ACTIVE);
}

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
        ActiveStream* as = static_cast<ActiveStream*>(stream_.get());
        if (!as->backfillReceived(it, BACKFILL_FROM_MEMORY)) {
            setStatus(ENGINE_ENOMEM); // Pause the backfill
        } else {
            setStatus(ENGINE_KEY_EEXISTS);
        }
    } else {
        setStatus(ENGINE_SUCCESS);
    }
}

DiskCallback::DiskCallback(stream_t &s)
    : stream_(s) {
    cb_assert(stream_.get() && stream_.get()->getType() == STREAM_ACTIVE);
}

void DiskCallback::callback(GetValue &val) {
    cb_assert(val.getValue());

    ActiveStream* as = static_cast<ActiveStream*>(stream_.get());
    if (!as->backfillReceived(val.getValue(), BACKFILL_FROM_DISK)) {
        setStatus(ENGINE_ENOMEM); // Pause the backfill
    } else {
        setStatus(ENGINE_SUCCESS);
    }
}

DCPBackfill::DCPBackfill(EventuallyPersistentEngine* e, stream_t s,
                         uint64_t start_seqno, uint64_t end_seqno)
    : engine(e), stream(s),startSeqno(start_seqno), endSeqno(end_seqno),
      scanCtx(NULL), state(backfill_state_init) {
    cb_assert(stream->getType() == STREAM_ACTIVE);
}

backfill_status_t DCPBackfill::run() {
    LockHolder lh(lock);
    switch (state) {
        case backfill_state_init:
            return create();
        case backfill_state_scanning:
            return scan();
        case backfill_state_completing:
            return complete(false);
        case backfill_state_done:
            return backfill_finished;
        default:
            LOG(EXTENSION_LOG_WARNING, "Invalid backfill state");
            abort();
    }
}

uint16_t DCPBackfill::getVBucketId() {
    return stream->getVBucket();
}

uint64_t DCPBackfill::getEndSeqno() {
    return endSeqno;
}

void DCPBackfill::cancel() {
    LockHolder lh(lock);
    complete(true);
}

backfill_status_t DCPBackfill::create() {
    uint16_t vbid = stream->getVBucket();

    uint64_t lastPersistedSeqno =
        engine->getEpStore()->getLastPersistedSeqno(vbid);

    if (lastPersistedSeqno < endSeqno) {
        LOG(EXTENSION_LOG_WARNING, "Rescheduling backfill for vbucket %d "
            "because backfill up to seqno %llu is needed but only up to "
            "%llu is persisted", vbid, endSeqno, lastPersistedSeqno);
        return backfill_snooze;
    }

    ActiveStream* as = static_cast<ActiveStream*>(stream.get());
    KVStore* kvstore = engine->getEpStore()->getROUnderlying(vbid);
    size_t numItems = kvstore->getNumItems(vbid, startSeqno,
                                           std::numeric_limits<uint64_t>::max());

    as->incrBackfillRemaining(numItems);

    shared_ptr<Callback<GetValue> > cb(new DiskCallback(stream));
    shared_ptr<Callback<CacheLookup> > cl(new CacheCallback(engine, stream));
    scanCtx = kvstore->initScanContext(cb, cl, vbid, startSeqno, false, false,
                                       false);
    if (scanCtx) {
        as->markDiskSnapshot(startSeqno, scanCtx->maxSeqno);
        transitionState(backfill_state_scanning);
    } else {
        transitionState(backfill_state_done);
    }

    return backfill_success;
}

backfill_status_t DCPBackfill::scan() {
    uint16_t vbid = stream->getVBucket();
    KVStore* kvstore = engine->getEpStore()->getROUnderlying(vbid);
    scan_error_t error = kvstore->scan(scanCtx);

    if (error == scan_again) {
        return backfill_success;
    }

    transitionState(backfill_state_completing);

    return backfill_success;
}

backfill_status_t DCPBackfill::complete(bool cancelled) {
    uint16_t vbid = stream->getVBucket();
    KVStore* kvstore = engine->getEpStore()->getROUnderlying(vbid);
    kvstore->destroyScanContext(scanCtx);

    ActiveStream* as = static_cast<ActiveStream*>(stream.get());
    as->completeBackfill();

    LOG(EXTENSION_LOG_WARNING, "Backfill task (%llu to %llu) %s for vb %d",
        startSeqno, endSeqno, cancelled ? "cancelled" : "finished",
        stream->getVBucket());

    transitionState(backfill_state_done);

    return backfill_success;
}

void DCPBackfill::transitionState(backfill_state_t newState) {
    if (state == newState) {
        return;
    }

    switch (newState) {
        case backfill_state_scanning:
            cb_assert(state == backfill_state_init);
            break;
        case backfill_state_completing:
            cb_assert(state == backfill_state_scanning);
            break;
        case backfill_state_done:
            cb_assert(state == backfill_state_init ||
                      state == backfill_state_scanning ||
                      state == backfill_state_completing);
            break;
        default:
            LOG(EXTENSION_LOG_WARNING, "Invalid backfill state transition from"
                " %s to %s", backfillStateToString(state),
                backfillStateToString(newState));
            abort();
    }
    state = newState;
}

