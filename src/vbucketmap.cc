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

#include <vector>

#include "ep.h"
#include "ep_engine.h"
#include "vbucketmap.h"

VBucketMap::VBucketMap(Configuration &config,
                       EventuallyPersistentStore &store) :
    bucketDeletion(new AtomicValue<bool>[config.getMaxVbuckets()]),
    bucketCreation(new AtomicValue<bool>[config.getMaxVbuckets()]),
    persistenceSeqnos(new AtomicValue<uint64_t>[config.getMaxVbuckets()]),
    size(config.getMaxVbuckets())
{
    WorkLoadPolicy &workload = store.getEPEngine().getWorkLoadPolicy();
    for (size_t shardId = 0; shardId < workload.getNumShards(); shardId++) {
        KVShard *shard = new KVShard(shardId, store);
        shards.push_back(shard);
    }

    for (size_t i = 0; i < size; ++i) {
        bucketDeletion[i].store(false);
        bucketCreation[i].store(false);
        persistenceSeqnos[i].store(0);
    }

    config.addValueChangedListener("hlc_drift_ahead_threshold_us",
                                    new VBucketConfigChangeListener(*this));
    config.addValueChangedListener("hlc_drift_behind_threshold_us",
                                    new VBucketConfigChangeListener(*this));
}

VBucketMap::~VBucketMap() {
    delete[] bucketDeletion;
    delete[] bucketCreation;
    delete[] persistenceSeqnos;
    while (!shards.empty()) {
        delete shards.back();
        shards.pop_back();
    }
}

RCPtr<VBucket> VBucketMap::getBucket(id_type id) const {
    static RCPtr<VBucket> emptyVBucket;
    if (id < size) {
        return getShardByVbId(id)->getBucket(id);
    } else {
        return emptyVBucket;
    }
}

ENGINE_ERROR_CODE VBucketMap::addBucket(const RCPtr<VBucket> &b) {
    if (b->getId() < size) {
        getShardByVbId(b->getId())->setBucket(b);
        LOG(EXTENSION_LOG_INFO, "Mapped new vbucket %d in state %s",
            b->getId(), VBucket::toString(b->getState()));
        return ENGINE_SUCCESS;
    }
    LOG(EXTENSION_LOG_WARNING,
        "Cannot create vb %" PRIu16", max vbuckets is %" PRIu16, b->getId(),
        size);
    return ENGINE_ERANGE;
}

void VBucketMap::removeBucket(id_type id) {
    if (id < size) {
        // Theoretically, this could be off slightly.  In
        // practice, this happens only on dead vbuckets.
        getShardByVbId(id)->resetBucket(id);
    }
}

std::vector<VBucketMap::id_type> VBucketMap::getBuckets(void) const {
    std::vector<id_type> rv;
    for (id_type i = 0; i < size; ++i) {
        RCPtr<VBucket> b(getShardByVbId(i)->getBucket(i));
        if (b) {
            rv.push_back(b->getId());
        }
    }
    return rv;
}

std::vector<VBucketMap::id_type> VBucketMap::getBucketsSortedByState(void) const {
    std::vector<id_type> rv;
    for (int state = vbucket_state_active;
         state <= vbucket_state_dead; ++state) {
        for (size_t i = 0; i < size; ++i) {
            RCPtr<VBucket> b = getShardByVbId(i)->getBucket(i);
            if (b && b->getState() == state) {
                rv.push_back(b->getId());
            }
        }
    }
    return rv;
}

std::vector<std::pair<VBucketMap::id_type, size_t> >
VBucketMap::getActiveVBucketsSortedByChkMgrMem(void) const {
    std::vector<std::pair<id_type, size_t> > rv;
    for (id_type i = 0; i < size; ++i) {
        RCPtr<VBucket> b = getShardByVbId(i)->getBucket(i);
        if (b && b->getState() == vbucket_state_active) {
            rv.push_back(std::make_pair(b->getId(), b->getChkMgrMemUsage()));
        }
    }

    struct SortCtx {
        static bool compareSecond(std::pair<id_type, size_t> a,
                                  std::pair<id_type, size_t> b) {
            return (a.second < b.second);
        }
    };

    std::sort(rv.begin(), rv.end(), SortCtx::compareSecond);

    return rv;
}


VBucketMap::id_type VBucketMap::getSize(void) const {
    return size;
}

bool VBucketMap::isBucketDeletion(id_type id) const {
    return bucketDeletion[id].load();
}

bool VBucketMap::setBucketDeletion(id_type id, bool delBucket) {
    bool inverse = !delBucket;
    return bucketDeletion[id].compare_exchange_strong(inverse, delBucket);
}

bool VBucketMap::isBucketCreation(id_type id) const {
    return bucketCreation[id].load();
}

bool VBucketMap::setBucketCreation(id_type id, bool rv) {
    bool inverse = !rv;
    return bucketCreation[id].compare_exchange_strong(inverse, rv);
}

uint64_t VBucketMap::getPersistenceCheckpointId(id_type id) const {
    if (id < size) {
        auto vb = getBucket(id);
        if (vb) {
            return vb->getPersistenceCheckpointId();
        }
    }
    return {};
}

void VBucketMap::setPersistenceCheckpointId(id_type id,
                                            uint64_t checkpointId) {
    if (id < size) {
        auto vb = getBucket(id);
        if (vb) {
            vb->setPersistenceCheckpointId(checkpointId);
        }
    }
}

uint64_t VBucketMap::getPersistenceSeqno(id_type id) const {
    return persistenceSeqnos[id].load();
}

void VBucketMap::setPersistenceSeqno(id_type id, uint64_t seqno) {
    persistenceSeqnos[id].store(seqno);
}

void VBucketMap::addBuckets(const std::vector<VBucket*> &newBuckets) {
    std::vector<VBucket*>::const_iterator it;
    for (it = newBuckets.begin(); it != newBuckets.end(); ++it) {
        RCPtr<VBucket> v(*it);
        addBucket(v);
    }
}

KVShard* VBucketMap::getShardByVbId(id_type id) const {
    return shards[id % shards.size()];
}

KVShard* VBucketMap::getShard(KVShard::id_type shardId) const {
    return shards[shardId];
}

size_t VBucketMap::getNumShards() const {
    return shards.size();
}

void VBucketMap::setHLCDriftAheadThreshold(std::chrono::microseconds threshold) {
    for (id_type id = 0; id < size; id++) {
        auto vb = getBucket(id);
        if (vb) {
            vb->setHLCDriftAheadThreshold(threshold);
        }
    }
}

void VBucketMap::setHLCDriftBehindThreshold(std::chrono::microseconds threshold) {
    for (id_type id = 0; id < size; id++) {
        auto vb = getBucket(id);
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