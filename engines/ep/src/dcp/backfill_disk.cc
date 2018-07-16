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

#include "dcp/active_stream.h"
#include "dcp/backfill_disk.h"
#include "ep_engine.h"
#include "kv_bucket.h"
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

CacheCallback::CacheCallback(EventuallyPersistentEngine& e,
                             std::shared_ptr<ActiveStream> s)
    : engine_(e), streamPtr(s) {
    if (s == nullptr) {
        throw std::invalid_argument("CacheCallback(): stream is NULL");
    }
    if (!s->isTypeActive()) {
        throw std::invalid_argument(
                "CacheCallback(): stream->getType() "
                "(which is " +
                to_string(s->getType()) + ") is not Active");
    }
}

// Do a get and restrict the collections lock scope to just these checks.
boost::optional<GetValue> CacheCallback::get(VBucket& vb,
                                             CacheLookup& lookup,
                                             ActiveStream& stream) {
    auto collectionsRHandle =
            vb.lockCollections(lookup.getKey(), true /*system allowed*/);
    // Check with collections if this key should be loaded, status EEXISTS is
    // the only way to inform the scan to not continue with this key.
    if (!collectionsRHandle.valid() ||
        collectionsRHandle.isLogicallyDeleted(lookup.getBySeqno())) {
        return {};
    }

    return vb.getInternal(nullptr,
                          engine_,
                          0,
                          /*options*/ NONE,
                          /*diskFlushAll*/ false,
                          stream.isKeyOnly() ? VBucket::GetKeyOnly::Yes
                                             : VBucket::GetKeyOnly::No,
                          collectionsRHandle);
}

void CacheCallback::callback(CacheLookup& lookup) {
    auto stream_ = streamPtr.lock();
    if (!stream_) {
        setStatus(ENGINE_SUCCESS);
        return;
    }

    VBucketPtr vb =
            engine_.getKVBucket()->getVBucket(lookup.getVBucketId());
    if (!vb) {
        setStatus(ENGINE_SUCCESS);
        return;
    }

    auto optionalGv = get(*vb, lookup, *stream_);
    if (optionalGv.is_initialized()) {
        auto& gv = optionalGv.get();

        if (gv.getStatus() == ENGINE_SUCCESS) {
            if (gv.item->getBySeqno() == lookup.getBySeqno()) {
                if (stream_->backfillReceived(std::move(gv.item),
                                              BACKFILL_FROM_MEMORY,
                                              /*force */ false)) {
                    setStatus(ENGINE_KEY_EEXISTS);
                    return;
                }
                setStatus(ENGINE_ENOMEM); // Pause the backfill
                return;
            }
        }
        setStatus(ENGINE_SUCCESS);
    } else {
        setStatus(ENGINE_KEY_EEXISTS);
    }
}

DiskCallback::DiskCallback(std::shared_ptr<ActiveStream> s) : streamPtr(s) {
    if (s == nullptr) {
        throw std::invalid_argument("DiskCallback(): stream is NULL");
    }
    if (!s->isTypeActive()) {
        throw std::invalid_argument(
                "DiskCallback(): stream->getType() "
                "(which is " +
                to_string(s->getType()) + ") is not Active");
    }
}

void DiskCallback::callback(GetValue& val) {
    auto stream_ = streamPtr.lock();
    if (!stream_) {
        setStatus(ENGINE_SUCCESS);
        return;
    }

    if (!val.item) {
        throw std::invalid_argument("DiskCallback::callback: val is NULL");
    }

    // MB-26705: Make the backfilled item cold so ideally the consumer would
    // evict this before any cached item if they get into memory pressure.
    val.item->setNRUValue(MAX_NRU_VALUE);
    val.item->setFreqCounterValue(0);

    if (!stream_->backfillReceived(std::move(val.item),
                                   BACKFILL_FROM_DISK,
                                   /*force*/ false)) {
        setStatus(ENGINE_ENOMEM); // Pause the backfill
    } else {
        setStatus(ENGINE_SUCCESS);
    }
}

DCPBackfillDisk::DCPBackfillDisk(EventuallyPersistentEngine& e,
                                 std::shared_ptr<ActiveStream> s,
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
    auto stream = streamPtr.lock();
    if (!stream) {
        LOG(EXTENSION_LOG_WARNING,
            "DCPBackfillDisk::create(): "
            "(vb:%d) backfill create ended prematurely as the associated "
            "stream is deleted by the producer conn ",
            getVBucketId());
        transitionState(backfill_state_done);
        return backfill_finished;
    }
    uint16_t vbid = stream->getVBucket();

    uint64_t lastPersistedSeqno =
            engine.getKVBucket()->getLastPersistedSeqno(vbid);

    if (lastPersistedSeqno < endSeqno) {
        stream->log(EXTENSION_LOG_NOTICE,
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

    auto cb = std::make_shared<DiskCallback>(stream);
    auto cl = std::make_shared<CacheCallback>(engine, stream);
    scanCtx = kvstore->initScanContext(
            cb, cl, vbid, startSeqno, DocumentFilter::ALL_ITEMS, valFilter);

    // Check startSeqno against the purge-seqno of the opened datafile.
    // 1) A normal stream request would of checked inside streamRequest, but
    //    compaction may have changed the purgeSeqno
    // 2) Cursor dropping can also schedule backfills and they must not re-start
    //    behind the current purge-seqno
    // If the startSeqno != 1 (a client 0 to n request becomes 1 to n) then
    // start-seqno must be above purge-seqno
    if (!scanCtx || (startSeqno != 1 && (startSeqno <= scanCtx->purgeSeqno))) {
        auto vb = engine.getVBucket(vbid);
        std::stringstream log;
        log << "DCPBackfillDisk::create(): (vb:" << getVBucketId()
            << ") cannot be scanned. Associated stream is set to dead state.";
        end_stream_status_t status = END_STREAM_BACKFILL_FAIL;
        if (scanCtx) {
            log << " startSeqno:" << startSeqno
                << " < purgeSeqno:" << scanCtx->purgeSeqno;
            kvstore->destroyScanContext(scanCtx);
            status = END_STREAM_ROLLBACK;
        } else {
            log << " failed to create scan";
        }
        log << ". The vbucket state:";
        if (vb) {
            log << VBucket::toString(vb->getState());
        } else {
            log << "vb not found!!";
        }

        stream->log(EXTENSION_LOG_WARNING, "%s", log.str().c_str());
        stream->setDead(status);
        transitionState(backfill_state_done);
    } else {
        stream->incrBackfillRemaining(scanCtx->documentCount);
        stream->markDiskSnapshot(startSeqno, scanCtx->maxSeqno);
        transitionState(backfill_state_scanning);
    }

    return backfill_success;
}

backfill_status_t DCPBackfillDisk::scan() {
    auto stream = streamPtr.lock();
    if (!stream) {
        return complete(true);
    }

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
    /* we want to destroy kv store context irrespective of a premature complete
       or not */
    KVStore* kvstore = engine.getKVBucket()->getROUnderlying(getVBucketId());
    kvstore->destroyScanContext(scanCtx);

    auto stream = streamPtr.lock();
    if (!stream) {
        LOG(EXTENSION_LOG_WARNING,
            "DCPBackfillDisk::complete(): "
            "(vb:%d) backfill create ended prematurely as the associated "
            "stream is deleted by the producer conn; %s",
            getVBucketId(),
            cancelled ? "cancelled" : "finished");
        transitionState(backfill_state_done);
        return backfill_finished;
    }

    stream->completeBackfill();

    EXTENSION_LOG_LEVEL severity =
            cancelled ? EXTENSION_LOG_NOTICE : EXTENSION_LOG_INFO;
    stream->log(severity,
                "(vb %d) Backfill task (%" PRIu64 " to %" PRIu64 ") %s",
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
