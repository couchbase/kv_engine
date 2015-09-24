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

#include <functional>

#include "ep_engine.h"
#include "flusher.h"
#include "kvshard.h"

KVShard::KVShard(uint16_t id, EventuallyPersistentStore &store) :
    shardId(id), highPrioritySnapshot(false),
    lowPrioritySnapshot(false),
    kvConfig(store.getEPEngine().getConfiguration(), shardId),
    highPriorityCount(0)
{
    EPStats &stats = store.getEPEngine().getEpStats();
    Configuration &config = store.getEPEngine().getConfiguration();
    maxVbuckets = config.getMaxVbuckets();

    vbuckets = new RCPtr<VBucket>[maxVbuckets];

    std::string backend = kvConfig.getBackend();
    uint16_t commitInterval = 1;

    if (backend.compare("couchdb") == 0) {
        rwUnderlying = KVStoreFactory::create(kvConfig, false);
        roUnderlying = KVStoreFactory::create(kvConfig, true);
    } else if (backend.compare("forestdb") == 0) {
        rwUnderlying = KVStoreFactory::create(kvConfig);
        roUnderlying = rwUnderlying;
        commitInterval = config.getMaxVbuckets()/config.getMaxNumShards();
    }

    flusher = new Flusher(&store, this, commitInterval);
    bgFetcher = new BgFetcher(&store, this, stats);
}

KVShard::~KVShard() {
    if (flusher->state() != stopped) {
        flusher->stop(true);
        LOG(EXTENSION_LOG_WARNING, "Terminating flusher while it is in %s",
            flusher->stateName());
    }
    delete flusher;
    delete bgFetcher;

    delete rwUnderlying;

    /* Only couchstore has a read write store and a read only. ForestDB
     * only has a read write store. Hence delete the read only store only
     * in the case of couchstore.
     */
    if (kvConfig.getBackend().compare("couchdb") == 0) {
        delete roUnderlying;
    }

    delete[] vbuckets;
}

KVStore *KVShard::getRWUnderlying() {
    return rwUnderlying;
}

KVStore *KVShard::getROUnderlying() {
    return roUnderlying;
}

Flusher *KVShard::getFlusher() {
    return flusher;
}

BgFetcher *KVShard::getBgFetcher() {
    return bgFetcher;
}

void KVShard::notifyFlusher() {
    flusher->notifyFlushEvent();
}

RCPtr<VBucket> KVShard::getBucket(uint16_t id) const {
    if (id < maxVbuckets) {
        return vbuckets[id];
    } else {
        return NULL;
    }
}

void KVShard::setBucket(const RCPtr<VBucket> &vb) {
    vbuckets[vb->getId()].reset(vb);
}

void KVShard::resetBucket(uint16_t id) {
    vbuckets[id].reset();
}

std::vector<VBucket::id_type> KVShard::getVBucketsSortedByState() {
    std::vector<VBucket::id_type> rv;
    for (int state = vbucket_state_active;
         state <= vbucket_state_dead;
         ++state) {
        for (size_t i = 0; i < maxVbuckets; ++i) {
            RCPtr<VBucket> b = vbuckets[i];
            if (b && b->getState() == state) {
                rv.push_back(b->getId());
            }
        }
    }
    return rv;
}

std::vector<VBucket::id_type> KVShard::getVBuckets() {
    std::vector<VBucket::id_type> rv;
    for (size_t i = 0; i < maxVbuckets; ++i) {
        RCPtr<VBucket> b = vbuckets[i];
        if (b) {
            rv.push_back(b->getId());
        }
    }
    return rv;
}

bool KVShard::setHighPriorityVbSnapshotFlag(bool highPriority) {
    bool inverse = !highPriority;
    return highPrioritySnapshot.compare_exchange_strong(inverse, highPriority);
}

bool KVShard::setLowPriorityVbSnapshotFlag(bool lowPriority) {
    bool inverse = !lowPriority;
    return lowPrioritySnapshot.compare_exchange_strong(inverse, lowPriority);
}

void NotifyFlusherCB::callback(uint16_t &vb) {
    if (shard->getBucket(vb)) {
        shard->notifyFlusher();
    }
}
