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

#include <functional>
#include <memory>

#include "bgfetcher.h"
#include "couch-kvstore/couch-kvstore-config.h"
#include "ep_bucket.h"
#include "ep_engine.h"
#include "flusher.h"
#include "kvshard.h"
#include "kvstore.h"
#ifdef EP_USE_MAGMA
#include "magma-kvstore/magma-kvstore_config.h"
#endif
#ifdef EP_USE_ROCKSDB
#include "rocksdb-kvstore/rocksdb-kvstore_config.h"
#endif

/* [EPHE TODO]: Consider not using KVShard for ephemeral bucket */
KVShard::KVShard(EventuallyPersistentEngine& engine, id_type id)
    : // Size vBuckets to have sufficient slots for the maximum number of
      // vBuckets each shard is responsible for. To ensure correct behaviour
      // when vbuckets isn't a multiple of num_shards, apply ceil() to the
      // division so we round up where necessary.
      vbuckets(std::ceil(float(engine.getConfiguration().getMaxVbuckets()) /
                         engine.getWorkLoadPolicy().getNumShards())),
      highPriorityCount(0) {
    const auto numShards = engine.getWorkLoadPolicy().getNumShards();
    auto& config = engine.getConfiguration();
    const std::string backend = config.getBackend();
    if (backend == "couchdb") {
        kvConfig = std::make_unique<CouchKVStoreConfig>(config, numShards, id);
        auto stores = KVStoreFactory::create(*kvConfig);
        rwStore = std::move(stores.rw);
        roStore = std::move(stores.ro);
    }
#ifdef EP_USE_MAGMA
    else if (backend == "magma") {
        // magma has its own bloom filters and should not use
        // kv_engine's bloom filters. Should save some memory.
        config.setBfilterEnabled(false);
        kvConfig = std::make_unique<MagmaKVStoreConfig>(config, numShards, id);
        auto stores = KVStoreFactory::create(*kvConfig);
        rwStore = std::move(stores.rw);
    }
#endif
#ifdef EP_USE_ROCKSDB
    else if (backend == "rocksdb") {
        kvConfig =
                std::make_unique<RocksDBKVStoreConfig>(config, numShards, id);
        auto stores = KVStoreFactory::create(*kvConfig);
        rwStore = std::move(stores.rw);
    }
#endif
    else {
        throw std::logic_error(
                "KVShard::KVShard: "
                "Invalid backend type '" +
                backend + "'");
    }
}

void KVShard::enablePersistence(EPBucket& ep) {
    flusher = std::make_unique<Flusher>(&ep, this);
    bgFetcher = std::make_unique<BgFetcher>(ep, *this);
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

VBucketPtr KVShard::getBucket(Vbid id) const {
    if (id.get() < kvConfig->getMaxVBuckets()) {
        auto element = getElement(id);
        Expects(element.get() == nullptr || element.get()->getId() == id);
        return element.get();
    } else {
        return nullptr;
    }
}

KVShard::VBMapElement::Access<KVShard::VBMapElement&> KVShard::getElement(
        Vbid id) {
    Expects(id.get() % kvConfig->getMaxShards() == kvConfig->getShardId());
    auto index = id.get() / this->kvConfig->getMaxShards();
    auto element = this->vbuckets.at(index).lock();
    return element;
}

KVShard::VBMapElement::Access<const KVShard::VBMapElement&> KVShard::getElement(
        Vbid id) const {
    Expects(id.get() % kvConfig->getMaxShards() == kvConfig->getShardId());

    auto index = id.get() / this->kvConfig->getMaxShards();
    auto element = this->vbuckets.at(index).lock();
    return element;
}

void KVShard::setBucket(VBucketPtr vb) {
    getElement(vb->getId()).set(vb);
}

void KVShard::dropVBucketAndSetupDeferredDeletion(Vbid id, const void* cookie) {
    auto vb = getElement(id);
    auto vbPtr = vb.get();
    vbPtr->setupDeferredDeletion(cookie);
    vb.reset();
}

std::vector<Vbid> KVShard::getVBucketsSortedByState() {
    std::vector<Vbid> rv;
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

std::vector<Vbid> KVShard::getVBuckets() {
    std::vector<Vbid> rv;
    for (const auto& b : vbuckets) {
        auto vb = b.lock();
        auto vbPtr = vb.get();
        if (vbPtr) {
            rv.push_back(vbPtr->getId());
        }
    }
    return rv;
}

void KVShard::setRWUnderlying(std::unique_ptr<KVStore> newStore) {
    rwStore.swap(newStore);
}

void KVShard::setROUnderlying(std::unique_ptr<KVStore> newStore) {
    roStore.swap(newStore);
}

KVStoreRWRO KVShard::takeRWRO() {
    return {std::move(rwStore), std::move(roStore)};
}
