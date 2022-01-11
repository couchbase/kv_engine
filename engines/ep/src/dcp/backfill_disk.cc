/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp/backfill_disk.h"
#include "active_stream.h"
#include "collections/vbucket_manifest_handles.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include "kvstore/kvstore.h"
#include "vbucket.h"

CacheCallback::CacheCallback(KVBucket& bucket, std::shared_ptr<ActiveStream> s)
    : bucket(bucket), streamPtr(s) {
    if (s == nullptr) {
        throw std::invalid_argument("CacheCallback(): stream is NULL");
    }
}

// Do a get and restrict the collections lock scope to just these checks.
GetValue CacheCallback::get(VBucket& vb,
                            CacheLookup& lookup,
                            ActiveStream& stream) {
    // getInternal may generate expired items and thus may for example need to
    // update a collection high-seqno, get a handle on the collection manifest
    auto cHandle = vb.lockCollections(lookup.getKey().getDocKey());
    if (!cHandle.valid()) {
        return {};
    }
    return vb.getInternal(nullptr,
                          bucket.getEPEngine(),
                          /*options*/ NONE,
                          stream.isKeyOnly() ? VBucket::GetKeyOnly::Yes
                                             : VBucket::GetKeyOnly::No,
                          cHandle);
}

void CacheCallback::callback(CacheLookup& lookup) {
    auto stream_ = streamPtr.lock();
    if (!stream_) {
        setStatus(cb::engine_errc::success);
        return;
    }

    VBucketPtr vb = bucket.getVBucket(lookup.getVBucketId());
    if (!vb) {
        setStatus(cb::engine_errc::success);
        return;
    }

    // If diskKey is in Prepared namespace then the in-memory StoredValue isn't
    // sufficient - as it doesn't contain the durability requirements (level).
    // Must get from disk.
    if (lookup.getKey().isPrepared()) {
        setStatus(cb::engine_errc::success);
        return;
    }

    // Check if the stream will allow the key, this is here to avoid reading
    // the value when dropping keys
    if (!stream_->collectionAllowed(lookup.getKey().getDocKey())) {
        setStatus(cb::engine_errc::key_already_exists);
        return;
    }

    auto gv = get(*vb, lookup, *stream_);
    if (gv.getStatus() == cb::engine_errc::success) {
        if (gv.item->getBySeqno() == lookup.getBySeqno()) {
            if (stream_->backfillReceived(std::move(gv.item),
                                          BACKFILL_FROM_MEMORY)) {
                setStatus(cb::engine_errc::key_already_exists);
                return;
            }
            setStatus(cb::engine_errc::no_memory); // Pause the backfill
            return;
        }
    }
    setStatus(cb::engine_errc::success);
}

DiskCallback::DiskCallback(std::shared_ptr<ActiveStream> s) : streamPtr(s) {
    if (s == nullptr) {
        throw std::invalid_argument("DiskCallback(): stream is NULL");
    }
}

void DiskCallback::callback(GetValue& val) {
    auto stream_ = streamPtr.lock();
    if (!stream_) {
        setStatus(cb::engine_errc::success);
        return;
    }

    if (!val.item) {
        throw std::invalid_argument("DiskCallback::callback: val is NULL");
    }

    // MB-26705: Make the backfilled item cold so ideally the consumer would
    // evict this before any cached item if they get into memory pressure.
    val.item->setFreqCounterValue(0);

    if (!stream_->backfillReceived(std::move(val.item), BACKFILL_FROM_DISK)) {
        setStatus(cb::engine_errc::no_memory); // Pause the backfill
    } else {
        setStatus(cb::engine_errc::success);
    }
}

DCPBackfillDisk::DCPBackfillDisk(KVBucket& bucket) : bucket(bucket) {
}

DCPBackfillDisk::~DCPBackfillDisk() = default;

backfill_status_t DCPBackfillDisk::run() {
    std::lock_guard<std::mutex> lh(lock);
    auto runtimeGuard =
            folly::makeGuard([start = std::chrono::steady_clock::now(), this] {
                runtime += (std::chrono::steady_clock::now() - start);
            });
    switch (state) {
    case backfill_state_init:
        return create();
    case backfill_state_scanning:
        return scan();
    case backfill_state_completing:
        complete(false);
        return backfill_finished;
    case backfill_state_done:
        return backfill_finished;
    }

    throw std::logic_error("DCPBackfillDisk::run: Invalid backfill state " +
                           std::to_string(state));
}

void DCPBackfillDisk::cancel() {
    std::lock_guard<std::mutex> lh(lock);
    if (state != backfill_state_done) {
        complete(true);
    }
}

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
        if (state == backfill_state_init || state == backfill_state_scanning) {
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
