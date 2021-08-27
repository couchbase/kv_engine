/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "vbucketmap.h"
#include "bucket_logger.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include <memory>
#include <vector>

VBucketMap::VBucketMap(KVBucket& bucket)
    : size(bucket.getEPEngine().getConfiguration().getMaxVbuckets()),
      bucket(bucket) {
    auto& engine = bucket.getEPEngine();
    const auto numShards = engine.getWorkLoadPolicy().getNumShards();
    shards.resize(numShards);
    for (size_t shardId = 0; shardId < numShards; shardId++) {
        shards[shardId] = std::make_unique<KVShard>(
                bucket.getEPEngine().getConfiguration(), numShards, shardId);
    }

    auto& config = engine.getConfiguration();
    config.addValueChangedListener(
            "hlc_drift_ahead_threshold_us",
            std::make_unique<VBucketConfigChangeListener>(*this));
    config.addValueChangedListener(
            "hlc_drift_behind_threshold_us",
            std::make_unique<VBucketConfigChangeListener>(*this));
}

VBucketPtr VBucketMap::getBucket(Vbid id) const {
    if (id.get() < size) {
        return getShardByVbId(id)->getBucket(id);
    } else {
        return {};
    }
}

cb::engine_errc VBucketMap::addBucket(VBucketPtr vb) {
    if (vb->getId().get() < size) {
        getShardByVbId(vb->getId())->setBucket(vb);
        incVBStateCount(vb->getState());
        EP_LOG_DEBUG("Mapped new {} in state {}",
                     vb->getId(),
                     VBucket::toString(vb->getState()));
        return cb::engine_errc::success;
    }
    EP_LOG_WARN("Cannot create {}, max vbuckets is {}", vb->getId(), size);
    return cb::engine_errc::out_of_range;
}

void VBucketMap::dropVBucketAndSetupDeferredDeletion(
        Vbid id, const CookieIface* cookie) {
    if (id.get() < size) {
        // Note: Can't hold a shared_ptr copy when calling down to
        // KVShard::dropVBucketAndSetupDeferredDeletion. See that function for
        // details.
        {
            const auto vb = getBucket(id);
            Expects(vb);
            decVBStateCount(vb->getState());
        }

        getShardByVbId(id)->dropVBucketAndSetupDeferredDeletion(id, cookie);
    }
}

std::vector<Vbid> VBucketMap::getBuckets() const {
    std::vector<Vbid> rv;
    for (size_t i = 0; i < size; ++i) {
        VBucketPtr b(getBucket(Vbid(i)));
        if (b) {
            rv.push_back(b->getId());
        }
    }
    return rv;
}

std::vector<Vbid> VBucketMap::getBucketsInState(vbucket_state_t state) const {
    std::vector<Vbid> rv;
    for (size_t i = 0; i < size; ++i) {
        VBucketPtr b = getBucket(Vbid(i));
        if (b && b->getState() == state) {
            rv.push_back(b->getId());
        }
    }
    return rv;
}

KVShard* VBucketMap::getShardByVbId(Vbid id) const {
    return shards[id.get() % shards.size()].get();
}

KVShard* VBucketMap::getShard(KVShard::id_type shardId) const {
    return shards[shardId].get();
}

size_t VBucketMap::getNumShards() const {
    return shards.size();
}

void VBucketMap::setHLCDriftAheadThreshold(std::chrono::microseconds threshold) {
    for (size_t id = 0; id < size; id++) {
        auto vb = getBucket(Vbid(id));
        if (vb) {
            vb->setHLCDriftAheadThreshold(threshold);
        }
    }
}

void VBucketMap::setHLCDriftBehindThreshold(std::chrono::microseconds threshold) {
    for (size_t id = 0; id < size; id++) {
        auto vb = getBucket(Vbid(id));
        if (vb) {
            vb->setHLCDriftBehindThreshold(threshold);
        }
    }
}

void VBucketMap::VBucketConfigChangeListener::sizeValueChanged(const std::string &key,
                                                   size_t value) {
    if (key == "hlc_drift_ahead_threshold_us") {
        map.setHLCDriftAheadThreshold(std::chrono::microseconds(value));
    } else if (key == "hlc_drift_behind_threshold_us") {
        map.setHLCDriftBehindThreshold(std::chrono::microseconds(value));
    }
}

void VBucketMap::decVBStateCount(vbucket_state_t state) {
    --vbStateCount[state - vbucket_state_active];

    if (bucket.isCheckpointMaxSizeAutoConfig()) {
        bucket.autoConfigCheckpointMaxSize();
    }
}

void VBucketMap::incVBStateCount(vbucket_state_t state) {
    ++vbStateCount[state - vbucket_state_active];

    if (bucket.isCheckpointMaxSizeAutoConfig()) {
        bucket.autoConfigCheckpointMaxSize();
    }
}

uint16_t VBucketMap::getVBStateCount(vbucket_state_t state) const {
    return vbStateCount[state - vbucket_state_active];
}

vbucket_state_t VBucketMap::setState(VBucket& vb,
                                     vbucket_state_t newState,
                                     const nlohmann::json* meta) {
    folly::SharedMutex::WriteHolder vbStateLock(vb.getStateLock());
    return setState_UNLOCKED(vb, newState, meta, vbStateLock);
}

vbucket_state_t VBucketMap::setState_UNLOCKED(
        VBucket& vb,
        vbucket_state_t newState,
        const nlohmann::json* meta,
        folly::SharedMutex::WriteHolder& vbStateLock) {
    vbucket_state_t oldState = vb.getState();
    vb.setState_UNLOCKED(newState, meta, vbStateLock);

    decVBStateCount(oldState);
    incVBStateCount(newState);

    return oldState;
}

size_t VBucketMap::getNumAliveVBuckets() const {
    size_t res = 0;
    // Note: We don't account vbucket_state_dead as that identifies VBucket
    // objects set-up for deferred deletion on disk but that have already been
    // destroyed in memory.
    for (const auto state :
         {vbucket_state_active, vbucket_state_replica, vbucket_state_pending}) {
        res += getVBStateCount(state);
    }
    return res;
}
