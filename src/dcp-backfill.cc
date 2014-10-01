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

#define DCP_BACKFILL_SLEEP_TIME 2

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
        static_cast<ActiveStream*>(stream_.get())->backfillReceived(it);
        setStatus(ENGINE_KEY_EEXISTS);
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
    ActiveStream* active_stream = static_cast<ActiveStream*>(stream_.get());
    active_stream->backfillReceived(val.getValue());
}

DCPBackfill::DCPBackfill(EventuallyPersistentEngine* e, stream_t s,
                         uint64_t start_seqno, uint64_t end_seqno,
                         const Priority &p, double sleeptime, bool shutdown)
    : GlobalTask(e, p, sleeptime, shutdown), engine(e), stream(s),
        startSeqno(start_seqno), endSeqno(end_seqno) {
    cb_assert(stream->getType() == STREAM_ACTIVE);
}

bool DCPBackfill::run() {
    uint16_t vbid = stream->getVBucket();

    if (engine->getEpStore()->isMemoryUsageTooHigh()) {
        LOG(EXTENSION_LOG_INFO, "VBucket %d dcp backfill task temporarily "
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
        LOG(EXTENSION_LOG_WARNING, "Rescheduling backfill for vbucket %d "
            "because backfill up to seqno %llu is needed but only up to "
            "%llu is persisted (disk %llu)", vbid, endSeqno,
            lastPersistedSeqno, diskSeqno);
        snooze(DCP_BACKFILL_SLEEP_TIME);
        return true;
    }

    ActiveStream* as = static_cast<ActiveStream*>(stream.get());
    KVStore* kvstore = engine->getEpStore()->getROUnderlying(vbid);
    size_t numItems = kvstore->getNumItems(vbid, startSeqno,
                                           std::numeric_limits<uint64_t>::max());

    as->incrBackfillRemaining(numItems);

    shared_ptr<Callback<GetValue> > cb(new DiskCallback(stream));
    shared_ptr<Callback<CacheLookup> > cl(new CacheCallback(engine, stream));
    ScanContext* ctx = kvstore->initScanContext(cb, cl, vbid, startSeqno, false,
                                                false, false);
    if (ctx) {
        as->markDiskSnapshot(startSeqno, ctx->maxSeqno);
        kvstore->scan(ctx);
        kvstore->destroyScanContext(ctx);
    }

    as->completeBackfill();

    LOG(EXTENSION_LOG_WARNING, "Backfill task (%llu to %llu) finished for vb %d"
        " disk seqno %llu memory seqno %llu", startSeqno, endSeqno,
        stream->getVBucket(), diskSeqno, lastPersistedSeqno);

    return false;
}

std::string DCPBackfill::getDescription() {
    std::stringstream ss;
    ss << "DCP backfill for vbucket " << stream->getVBucket();
    return ss.str();
}

