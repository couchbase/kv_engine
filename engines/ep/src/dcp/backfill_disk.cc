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

#include "bucket_logger.h"
#include "dcp/backfill_disk.h"
#include "active_stream.h"
#include "collections/vbucket_manifest_handles.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include "kvstore/kvstore.h"
#include "vbucket.h"
#include <fmt/ostream.h>

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
        setStatus(cb::engine_errc::stream_not_found);
        return;
    }

    VBucketPtr vb = bucket.getVBucket(lookup.getVBucketId());
    if (!vb) {
        setStatus(cb::engine_errc::not_my_vbucket);
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

    auto item = std::unique_ptr<Item>();

    // We don't need to read the value if the stream is KeyOnly AND this key
    // is not a SystemEvent (which requires the full payload for all streams)
    if (stream_->isKeyOnly() &&
        !(lookup.getKey().getDocKey().isInSystemCollection())) {
        // Create an empty value to backfill with
        item = std::make_unique<Item>(
                lookup.getKey().getDocKey(), /* docKey */
                0, /* flags */
                0, /* expiry */
                nullptr, /* value */
                0, /* how much memory to allocate */
                PROTOCOL_BINARY_RAW_BYTES, /* binary protocol */
                0, /* cas */
                lookup.getBySeqno()); /* seqNo */

    } else {
        auto gv = get(*vb, lookup, *stream_);
        // If we could not retrieve the value or the Item seqNo does not match,
        // return success so the stream continues onto the next key; no backfill
        if (gv.getStatus() != cb::engine_errc::success ||
            gv.item->getBySeqno() != lookup.getBySeqno()) {
            setStatus(cb::engine_errc::success);
            return;
        }
        item = std::move(gv.item);
    }

    // Perform the backfill and set status to key_already_exists if successful.
    // Otherwise pause stream backfill as op failed - not enough free memory
    if (stream_->backfillReceived(std::move(item), BACKFILL_FROM_MEMORY)) {
        setStatus(cb::engine_errc::key_already_exists);
    } else {
        yield();
    }
    return;
}

DiskCallback::DiskCallback(std::shared_ptr<ActiveStream> s) : streamPtr(s) {
    if (s == nullptr) {
        throw std::invalid_argument("DiskCallback(): stream is NULL");
    }
}

void DiskCallback::callback(GetValue& val) {
    auto stream_ = streamPtr.lock();
    if (!stream_) {
        setStatus(cb::engine_errc::stream_not_found);
        return;
    }

    if (!val.item) {
        throw std::invalid_argument("DiskCallback::callback: val is NULL");
    }

    auto committedState = val.item->getCommitted();
    switch (committedState) {
    case CommittedState::CommittedViaMutation:
    case CommittedState::CommittedViaPrepare:
    case CommittedState::PrepareAborted:
        break;
    case CommittedState::Pending:
    case CommittedState::PreparedMaybeVisible:
    case CommittedState::PrepareCommitted:
        if (val.item->getBySeqno() <=
            static_cast<int64_t>(persistedCompletedSeqno)) {
            setStatus(cb::engine_errc::success);
            return;
        }
    }

    // MB-26705: Make the backfilled item cold so ideally the consumer would
    // evict this before any cached item if they get into memory pressure.
    val.item->setFreqCounterValue(0);

    if (!stream_->backfillReceived(std::move(val.item), BACKFILL_FROM_DISK)) {
        yield(); // Pause the backfill
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
    case State::Create:
        return create();
    case State::Scan:
        return scan();
    case State::Done:
        return backfill_finished;
    }

    throw std::logic_error(fmt::format("{}: Invalid state:{}",
                                       __PRETTY_FUNCTION__,
                                       std::to_string(int(state))));
}

void DCPBackfillDisk::cancel() {
    std::lock_guard<std::mutex> lh(lock);
    if (state != State::Done) {
        EP_LOG_WARN(
                "DCPBackfillDisk::cancel ({}) cancelled before reaching "
                "State::Done",
                getVBucketId());
    }
}

std::ostream& operator<<(std::ostream& os, DCPBackfillDisk::State state) {
    switch (state) {
    case DCPBackfillDisk::State::Create:
        return os << "State::Create";
    case DCPBackfillDisk::State::Scan:
        return os << "State::Scan";
    case DCPBackfillDisk::State::Done:
        return os << "State::Done";
    }

    throw std::logic_error(fmt::format("{}: Invalid state:{}",
                                       __PRETTY_FUNCTION__,
                                       std::to_string(int(state))));
    return os;
}

void DCPBackfillDisk::transitionState(State newState) {
    if (state == newState) {
        return;
    }

    bool validTransition = false;
    switch (newState) {
    case State::Create:
        // No valid transition to 'create'
        break;
    case State::Scan:
        if (state == State::Create) {
            validTransition = true;
        }
        break;
    case State::Done:
        if (state == State::Create || state == State::Scan) {
            validTransition = true;
        }
        break;
    }

    if (!validTransition) {
        throw std::invalid_argument(
                fmt::format("{}: newState:{} is not valid "
                            "for current state:{}",
                            __PRETTY_FUNCTION__,
                            newState,
                            state));
    }

    state = newState;
}
