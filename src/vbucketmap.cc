/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011 Couchbase, Inc
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

VBucketMap::VBucketMap(Configuration &config, EventuallyPersistentStore &store) :
    bucketDeletion(new Atomic<bool>[config.getMaxVbuckets()]),
    bucketCreation(new Atomic<bool>[config.getMaxVbuckets()]),
    persistenceCheckpointIds(new Atomic<uint64_t>[config.getMaxVbuckets()]),
    size(config.getMaxVbuckets())
{
    WorkLoadPolicy &workload = store.getEPEngine().getWorkLoadPolicy();
    numShards = workload.getNumShards();
    for (size_t shardId = 0; shardId < numShards; shardId++) {
        KVShard *shard = new KVShard(shardId, store);
        shards.push_back(shard);
    }

    for (size_t i = 0; i < size; ++i) {
        bucketDeletion[i].store(false);
        bucketCreation[i].store(true);
        persistenceCheckpointIds[i].store(0);
    }
}

VBucketMap::~VBucketMap() {
    delete[] bucketDeletion;
    delete[] bucketCreation;
    delete[] persistenceCheckpointIds;
    while (!shards.empty()) {
        delete shards.back();
        shards.pop_back();
    }
}

RCPtr<VBucket> VBucketMap::getBucket(uint16_t id) const {
    static RCPtr<VBucket> emptyVBucket;
    if (static_cast<size_t>(id) < size) {
        return getShard(id)->getBucket(id);
    } else {
        return emptyVBucket;
    }
}

ENGINE_ERROR_CODE VBucketMap::addBucket(const RCPtr<VBucket> &b) {
    if (static_cast<size_t>(b->getId()) < size) {
        getShard(b->getId())->setBucket(b);
        LOG(EXTENSION_LOG_INFO, "Mapped new vbucket %d in state %s",
            b->getId(), VBucket::toString(b->getState()));
        return ENGINE_SUCCESS;
    }
    LOG(EXTENSION_LOG_WARNING, "Cannot create vb %d, max vbuckets is %d",
        b->getId(), size);
    return ENGINE_ERANGE;
}

void VBucketMap::removeBucket(uint16_t id) {
    if (static_cast<size_t>(id) < size) {
        // Theoretically, this could be off slightly.  In
        // practice, this happens only on dead vbuckets.
        getShard(id)->resetBucket(id);
    }
}

std::vector<int> VBucketMap::getBuckets(void) const {
    std::vector<int> rv;
    for (size_t i = 0; i < size; ++i) {
        RCPtr<VBucket> b(getShard(i)->getBucket(i));
        if (b) {
            rv.push_back(b->getId());
        }
    }
    return rv;
}

std::vector<int> VBucketMap::getBucketsSortedByState(void) const {
    std::vector<int> rv;
    for (int state = vbucket_state_active;
         state <= vbucket_state_dead; ++state) {
        for (size_t i = 0; i < size; ++i) {
            RCPtr<VBucket> b = getShard(i)->getBucket(i);
            if (b && b->getState() == state) {
                rv.push_back(b->getId());
            }
        }
    }
    return rv;
}

size_t VBucketMap::getSize(void) const {
    return size;
}

bool VBucketMap::isBucketDeletion(uint16_t id) const {
    assert(id < size);
    return bucketDeletion[id].load();
}

bool VBucketMap::setBucketDeletion(uint16_t id, bool delBucket) {
    assert(id < size);
    bool inverse = !delBucket;
    return bucketDeletion[id].compare_exchange_strong(inverse, delBucket);
}

bool VBucketMap::isBucketCreation(uint16_t id) const {
    assert(id < size);
    return bucketCreation[id].load();
}

bool VBucketMap::setBucketCreation(uint16_t id, bool rv) {
    assert(id < size);
    bool inverse = !rv;
    return bucketCreation[id].compare_exchange_strong(inverse, rv);
}

uint64_t VBucketMap::getPersistenceCheckpointId(uint16_t id) const {
    assert(id < size);
    return persistenceCheckpointIds[id].load();
}

void VBucketMap::setPersistenceCheckpointId(uint16_t id, uint64_t checkpointId) {
    assert(id < size);
    persistenceCheckpointIds[id].store(checkpointId);
}

void VBucketMap::addBuckets(const std::vector<VBucket*> &newBuckets) {
    std::vector<VBucket*>::const_iterator it;
    for (it = newBuckets.begin(); it != newBuckets.end(); ++it) {
        RCPtr<VBucket> v(*it);
        addBucket(v);
    }
}

KVShard* VBucketMap::getShard(uint16_t id) const {
    return shards[id % numShards];
}

size_t VBucketMap::getNumShards() const {
    return numShards;
}
