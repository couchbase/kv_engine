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

    if (backend == "couchdb") {
        auto stores = KVStoreFactory::create(kvConfig);
        rwStore = std::move(stores.rw);
        roStore = std::move(stores.ro);
    } else {
        throw std::logic_error(
                "KVShard::KVShard: "
                "Invalid backend type '" +
                backend + "'");
    }

    if (kvBucket.getEPEngine().getConfiguration().getBucketType() ==
        "persistent") {
        flusher = std::make_unique<Flusher>(&kvBucket, this);
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

VBucketPtr KVShard::getBucket(uint16_t id) const {
    if (id < vbuckets.size()) {
        return vbuckets[id].lock().get();
    } else {
        return NULL;
    }
}

void KVShard::setBucket(VBucketPtr vb) {
    vbuckets[vb->getId()].lock().set(vb);
}

void KVShard::dropVBucketAndSetupDeferredDeletion(VBucket::id_type id,
                                                  const void* cookie) {
    auto vb = vbuckets[id].lock();
    auto vbPtr = vb.get();
    vbPtr->setupDeferredDeletion(cookie);
    vb.reset();
}

std::vector<VBucket::id_type> KVShard::getVBucketsSortedByState() {
    std::vector<VBucket::id_type> rv;
    for (int state = vbucket_state_active;
         state <= vbucket_state_dead;
         ++state) {
        for (const auto& b : vbuckets) {
            auto vb = b.lock();
            auto vbPtr = vb.get();
            if (vbPtr && vbPtr->getState() == state) {
                rv.push_back(vbPtr->getId());
            }
        }
    }
    return rv;
}

std::vector<VBucket::id_type> KVShard::getVBuckets() {
    std::vector<VBucket::id_type> rv;
    for (const auto& b : vbuckets) {
        auto vb = b.lock();
        auto vbPtr = vb.get();
        if (vbPtr) {
            rv.push_back(vbPtr->getId());
        }
    }
    return rv;
}

void NotifyFlusherCB::callback(uint16_t &vb) {
    if (shard->getBucket(vb)) {
        shard->getFlusher()->notifyFlushEvent();
    }
}
