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

#include <platform/make_unique.h>

#include "bgfetcher.h"
#include "ep_bucket.h"
#include "ep_engine.h"
#include "flusher.h"
#include "kvshard.h"

/* [EPHE TODO]: Consider not using KVShard for ephemeral bucket */
KVShard::KVShard(uint16_t id, KVBucket& kvBucket)
    : kvConfig(kvBucket.getEPEngine().getConfiguration(), id),
      vbuckets(kvConfig.getMaxVBuckets()),
      highPriorityCount(0) {
    const std::string backend = kvConfig.getBackend();
    uint16_t commitInterval = 1;

    if (backend == "couchdb") {
        rwStore.reset(KVStoreFactory::create(kvConfig, false));
        roStore.reset(KVStoreFactory::create(kvConfig, true));
    } else if (backend == "forestdb") {
        rwStore.reset(KVStoreFactory::create(kvConfig));
        commitInterval = kvConfig.getMaxVBuckets() / kvConfig.getMaxShards();
    } else {
        throw std::logic_error(
                "KVShard::KVShard: "
                "Invalid backend type '" +
                backend + "'");
    }

    if (kvBucket.getEPEngine().getConfiguration().getBucketType() ==
        "persistent") {
        flusher = std::make_unique<Flusher>(&kvBucket, this, commitInterval);
        bgFetcher = std::make_unique<BgFetcher>(kvBucket, *this);
    }
}

// Non-inline destructor so we can destruct
// unique_ptrs of forward-declared items
KVShard::~KVShard() = default;

Flusher *KVShard::getFlusher() {
    return flusher.get();
}

BgFetcher *KVShard::getBgFetcher() {
    return bgFetcher.get();
}

RCPtr<VBucket> KVShard::getBucket(uint16_t id) const {
    if (id < vbuckets.size()) {
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
        for (RCPtr<VBucket> b : vbuckets) {
            if (b && b->getState() == state) {
                rv.push_back(b->getId());
            }
        }
    }
    return rv;
}

std::vector<VBucket::id_type> KVShard::getVBuckets() {
    std::vector<VBucket::id_type> rv;
    for (RCPtr<VBucket> b : vbuckets) {
        if (b) {
            rv.push_back(b->getId());
        }
    }
    return rv;
}

void NotifyFlusherCB::callback(uint16_t &vb) {
    if (shard->getBucket(vb)) {
        shard->getFlusher()->notifyFlushEvent();
    }
}
