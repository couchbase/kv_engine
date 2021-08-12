/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <functional>
#include <memory>

#include "ep_bucket.h"
#include "ep_engine.h"
#include "flusher.h"
#include "kvshard.h"
#include "kvstore/kvstore.h"

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

#ifdef EP_USE_MAGMA
    if (backend == "magma" ||
        (backend == "nexus" && config.getNexusPrimaryBackend() == "magma")) {
        // magma has its own bloom filters and should not use kv_engine's bloom
        // filters. Should save some memory.
        config.setBfilterEnabled(false);
    }
#endif

    kvConfig =
            KVStoreConfig::createKVStoreConfig(config, backend, numShards, id);
    rwStore = KVStoreFactory::create(*kvConfig);
}

// Non-inline destructor so we can destruct
// unique_ptrs of forward-declared items
KVShard::~KVShard() = default;

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

void KVShard::dropVBucketAndSetupDeferredDeletion(Vbid id,
                                                  const CookieIface* cookie) {
    VBucketPtr vbPtr;

    {
        // Rely on the underlying VBucketPtr type being a shared_ptr and grab a
        // copy of the ptr before we reset the one in the map. We need to do
        // this to prevent a lock order inversion with the dbFileRevMap lock
        auto vb = getElement(id);
        vbPtr = vb.get();
        vb.reset();
    }

    vbPtr->setupDeferredDeletion(cookie);
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

void KVShard::setRWUnderlying(std::unique_ptr<KVStoreIface> newStore) {
    rwStore.swap(newStore);
}

std::unique_ptr<KVStoreIface> KVShard::takeRW() {
    return std::move(rwStore);
}
