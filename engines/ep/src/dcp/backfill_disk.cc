/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "dcp/backfill_disk.h"
#include "dcp/stream.h"
#include "ep_engine.h"
#include "vbucket.h"

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
    }
    return "<invalid>:" + std::to_string(state);
}

CacheCallback::CacheCallback(EventuallyPersistentEngine& e, active_stream_t& s)
    : engine_(e), stream_(s) {
    if (stream_.get() == nullptr) {
        throw std::invalid_argument("CacheCallback(): stream is NULL");
    }
    if (!stream_.get()->isTypeActive()) {
        throw std::invalid_argument(
                "CacheCallback(): stream->getType() "
                "(which is " +
                to_string(stream_.get()->getType()) + ") is not Active");
    }
}

void CacheCallback::callback(CacheLookup& lookup) {
    VBucketPtr vb =
            engine_.getKVBucket()->getVBucket(lookup.getVBucketId());
    if (!vb) {
        setStatus(ENGINE_SUCCESS);
        return;
    }

    GetValue gv = vb->getInternal(lookup.getKey(),
                                  nullptr,
                                  engine_,
                                  0,
                                  /*options*/ NONE,
                                  /*diskFlushAll*/ false,
                                  stream_->isKeyOnly() ?
                                          VBucket::GetKeyOnly::Yes :
                                          VBucket::GetKeyOnly::No);
    if (gv.getStatus() == ENGINE_SUCCESS) {
        if (gv.item->getBySeqno() == lookup.getBySeqno()) {
            if (stream_->backfillReceived(std::move(gv.item),
                                          BACKFILL_FROM_MEMORY,
                                          /*force */false)) {
                setStatus(ENGINE_KEY_EEXISTS);
                return;
            }
            setStatus(ENGINE_ENOMEM); // Pause the backfill
            return;
        }
    }
    setStatus(ENGINE_SUCCESS);
}

DiskCallback::DiskCallback(active_stream_t& s) : stream_(s) {
    if (stream_.get() == nullptr) {
        throw std::invalid_argument("DiskCallback(): stream is NULL");
    }
    if (!stream_.get()->isTypeActive()) {
        throw std::invalid_argument(
                "DiskCallback(): stream->getType() "
                "(which is " +
                to_string(stream_.get()->getType()) + ") is not Active");
    }
}

void DiskCallback::callback(GetValue& val) {
    if (!val.item) {
        throw std::invalid_argument("DiskCallback::callback: val is NULL");
    }

    if (!stream_->backfillReceived(std::move(val.item),
                                   BACKFILL_FROM_DISK,
                                   /*force*/ false)) {
        setStatus(ENGINE_ENOMEM); // Pause the backfill
    } else {
        setStatus(ENGINE_SUCCESS);
    }
}

DCPBackfillDisk::DCPBackfillDisk(EventuallyPersistentEngine& e,
                                 const active_stream_t& s,
                                 uint64_t startSeqno,
                                 uint64_t endSeqno)
    : DCPBackfill(s, startSeqno, endSeqno),
      engine(e),
      scanCtx(nullptr),
      state(backfill_state_init) {
}

backfill_status_t DCPBackfillDisk::run() {
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

    throw std::logic_error("DCPBackfillDisk::run: Invalid backfill state " +
                           std::to_string(state));
}

void DCPBackfillDisk::cancel() {
    LockHolder lh(lock);
    if (state != backfill_state_done) {
        complete(true);
    }
}

backfill_status_t DCPBackfillDisk::create() {
    uint16_t vbid = stream->getVBucket();

    uint64_t lastPersistedSeqno =
            engine.getKVBucket()->getLastPersistedSeqno(vbid);

    if (lastPersistedSeqno < endSeqno) {
        stream->getLogger().log(EXTENSION_LOG_NOTICE,
                                "(vb %d) Rescheduling backfill"
                                "because backfill up to seqno %" PRIu64
                                " is needed but only up to "
                                "%" PRIu64 " is persisted",
                                vbid,
                                endSeqno,
                                lastPersistedSeqno);
        return backfill_snooze;
    }

    KVStore* kvstore = engine.getKVBucket()->getROUnderlying(vbid);
    ValueFilter valFilter = ValueFilter::VALUES_DECOMPRESSED;
    if (stream->isKeyOnly()) {
        valFilter = ValueFilter::KEYS_ONLY;
    } else {
        if (stream->isCompressionEnabled()) {
            valFilter = ValueFilter::VALUES_COMPRESSED;
        }
    }

    std::shared_ptr<Callback<GetValue> > cb(new DiskCallback(stream));
    std::shared_ptr<Callback<CacheLookup> > cl(
            new CacheCallback(engine, stream));
    scanCtx = kvstore->initScanContext(
            cb, cl, vbid, startSeqno, DocumentFilter::ALL_ITEMS, valFilter);

    if (scanCtx) {
        stream->incrBackfillRemaining(scanCtx->documentCount);
        stream->markDiskSnapshot(startSeqno, scanCtx->maxSeqno);
        transitionState(backfill_state_scanning);
    } else {
        transitionState(backfill_state_done);
    }

    return backfill_success;
}

backfill_status_t DCPBackfillDisk::scan() {
    uint16_t vbid = stream->getVBucket();

    if (!(stream->isActive())) {
        return complete(true);
    }

    KVStore* kvstore = engine.getKVBucket()->getROUnderlying(vbid);
    scan_error_t error = kvstore->scan(scanCtx);

    if (error == scan_again) {
        return backfill_success;
    }

    transitionState(backfill_state_completing);

    return backfill_success;
}

backfill_status_t DCPBackfillDisk::complete(bool cancelled) {
    uint16_t vbid = stream->getVBucket();
    KVStore* kvstore = engine.getKVBucket()->getROUnderlying(vbid);
    kvstore->destroyScanContext(scanCtx);

    stream->completeBackfill();

    EXTENSION_LOG_LEVEL severity =
            cancelled ? EXTENSION_LOG_NOTICE : EXTENSION_LOG_INFO;
    stream->getLogger().log(severity,
                            "(vb %d) Backfill task (%" PRIu64 " to %" PRIu64
                            ") %s",
                            vbid,
                            startSeqno,
                            endSeqno,
                            cancelled ? "cancelled" : "finished");

    transitionState(backfill_state_done);

    return backfill_success;
}

void DCPBackfillDisk::transitionState(backfill_state_t newState) {
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
        if (state == backfill_state_init || state == backfill_state_scanning ||
            state == backfill_state_completing) {
            validTransition = true;
        }
        break;
    }

    if (!validTransition) {
        throw std::invalid_argument(
                "DCPBackfillDisk::transitionState:"
                " newState (which is " +
                backfillStateToString(newState) +
                ") is not valid for current state (which is " +
                backfillStateToString(state) + ")");
    }

    state = newState;
}
