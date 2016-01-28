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

#include "ep_engine.h"
#include "dcp/backfill.h"
#include "dcp/stream.h"

static std::string backfillStateToString(backfill_state_t state) {
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
    : engine_(e),
      stream_(s) {
    if (stream_.get() == nullptr) {
        throw std::invalid_argument("CacheCallback(): stream is NULL");
    }
    if (stream_.get()->getType() != STREAM_ACTIVE) {
        throw std::invalid_argument("CacheCallback(): stream->getType() "
                "(which is " + std::to_string(stream_.get()->getType()) +
                ") is not ACTIVE");
    }
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
        ActiveStream* as = static_cast<ActiveStream*>(stream_.get());
        Item* it;
        try {
            it = as->isSendMutationKeyOnlyEnabled() ?
                        v->toValuelessItem(lookup.getVBucketId()) :
                        v->toItem(false, lookup.getVBucketId());
        } catch (const std::bad_alloc&) {
            setStatus(ENGINE_ENOMEM);
            LOG(EXTENSION_LOG_WARNING, "Alloc error when trying to create an "
                "item copy from hash table. Item key %s; seqno %" PRIi64 "; "
                "vb %u; isSendMutationKeyOnlyEnabled %d", v->getKey().c_str(),
                v->getBySeqno(), lookup.getVBucketId(),
                as->isSendMutationKeyOnlyEnabled());
            return;
        }
        lh.unlock();
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
    if (stream_.get() == nullptr) {
        throw std::invalid_argument("DiskCallback(): stream is NULL");
    }
    if (stream_.get()->getType() != STREAM_ACTIVE) {
        throw std::invalid_argument("DiskCallback(): stream->getType() "
                "(which is " + std::to_string(stream_.get()->getType()) +
                ") is not ACTIVE");
    }
}

void DiskCallback::callback(GetValue &val) {
    if (val.getValue() == nullptr) {
        throw std::invalid_argument("DiskCallback::callback: val is NULL");
    }

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
    if (stream->getType() != STREAM_ACTIVE) {
        throw std::invalid_argument("DCPBackfill(): stream->getType() "
                "(which is " + std::to_string(stream->getType()) +
                ") is not ACTIVE");
    }
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
    }

    throw std::logic_error("DCPBackfill::run: Invalid backfill state " +
                           std::to_string(state));
}

uint16_t DCPBackfill::getVBucketId() {
    return stream->getVBucket();
}

uint64_t DCPBackfill::getEndSeqno() {
    return endSeqno;
}

void DCPBackfill::cancel() {
    LockHolder lh(lock);
    if (state != backfill_state_done) {
        complete(true);
    }
}

backfill_status_t DCPBackfill::create() {
    uint16_t vbid = stream->getVBucket();

    uint64_t lastPersistedSeqno =
        engine->getEpStore()->getLastPersistedSeqno(vbid);

    ActiveStream* as = static_cast<ActiveStream*>(stream.get());

    if (lastPersistedSeqno < endSeqno) {
        LOG(EXTENSION_LOG_NOTICE, "%s (vb %d) Rescheduling backfill"
            "because backfill up to seqno %" PRIu64 " is needed but only up to "
            "%" PRIu64 " is persisted", as->logHeader(), vbid, endSeqno,
            lastPersistedSeqno);
        return backfill_snooze;
    }

    KVStore* kvstore = engine->getEpStore()->getROUnderlying(vbid);
    ValueFilter valFilter = ValueFilter::VALUES_DECOMPRESSED;
    if (as->isSendMutationKeyOnlyEnabled()) {
        valFilter = ValueFilter::KEYS_ONLY;
    } else {
        if (as->isCompressionEnabled()) {
            valFilter = ValueFilter::VALUES_COMPRESSED;
        }
    }

    std::shared_ptr<Callback<GetValue> > cb(new DiskCallback(stream));
    std::shared_ptr<Callback<CacheLookup> > cl(new CacheCallback(engine, stream));
    scanCtx = kvstore->initScanContext(cb, cl, vbid, startSeqno,
                                       DocumentFilter::ALL_ITEMS, valFilter);

    if (scanCtx) {
        as->incrBackfillRemaining(scanCtx->documentCount);
        as->markDiskSnapshot(startSeqno, scanCtx->maxSeqno);
        transitionState(backfill_state_scanning);
    } else {
        transitionState(backfill_state_done);
    }

    return backfill_success;
}

backfill_status_t DCPBackfill::scan() {
    uint16_t vbid = stream->getVBucket();

    if (!(stream->isActive())) {
        return complete(true);
    }

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

    LOG(EXTENSION_LOG_NOTICE,
        "%s (vb %d) Backfill task (%" PRIu64 " to %" PRIu64 ") %s",
        as->logHeader(), vbid, startSeqno, endSeqno,
        cancelled ? "cancelled" : "finished");

    transitionState(backfill_state_done);

    return backfill_success;
}

void DCPBackfill::transitionState(backfill_state_t newState) {
    if (state == newState) {
        return;
    }

    bool validTransition = false;
    switch (newState) {
        case backfill_state_init:
            // Not valid to transition back to 'init'
            break;
        case backfill_state_scanning:
            if (state == backfill_state_init) {
                validTransition = true;
            }
            break;
        case backfill_state_completing:
            if (state == backfill_state_scanning) {
                validTransition = true;
            }
            break;
        case backfill_state_done:
            if (state == backfill_state_init ||
                state == backfill_state_scanning ||
                state == backfill_state_completing) {
                validTransition = true;
            }
            break;
    }

    if (!validTransition) {
        throw std::invalid_argument("DCPBackfill::transitionState:"
            " newState (which is " + backfillStateToString(newState) +
            ") is not valid for current state (which is " +
            backfillStateToString(state) + ")");
    }

    state = newState;
}
