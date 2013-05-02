/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <functional>

#include "ep_engine.h"
#include "kvshard.hh"
#include "flusher.hh"

KVShard::KVShard(uint16_t id, EventuallyPersistentStore &store) :
    shardId(id), highPrioritySnapshot(false), lowPrioritySnapshot(false) {
    EPStats &stats = store.getEPEngine().getEpStats();
    Configuration &config = store.getEPEngine().getConfiguration();
    maxVbuckets = config.getMaxVbuckets();

    vbuckets = new RCPtr<VBucket>[maxVbuckets];

    rwUnderlying = KVStoreFactory::create(stats, config, false);
    roUnderlying = KVStoreFactory::create(stats, config, true);

    flusher = new Flusher(&store, NULL, this);
    bgFetcher = new BgFetcher(&store, this, NULL, stats);
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
    delete roUnderlying;

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

RCPtr<VBucket> KVShard::getBucket(uint16_t id) const {
    return vbuckets[id];
}

void KVShard::setBucket(const RCPtr<VBucket> &vb) {
    vbuckets[vb->getId()].reset(vb);
}

void KVShard::resetBucket(uint16_t id) {
    vbuckets[id].reset();
}

std::vector<int> KVShard::getVBucketsSortedByState() {
    std::vector<int> rv;
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

std::vector<int> KVShard::getVBuckets() {
    std::vector<int> rv;
    for (size_t i = 0; i < maxVbuckets; ++i) {
        RCPtr<VBucket> b = vbuckets[i];
        if (b) {
            rv.push_back(b->getId());
        }
    }
    return rv;
}

bool KVShard::setHighPriorityVbSnapshotFlag(bool highPriority) {
    return highPrioritySnapshot.cas(!highPriority, highPriority);
}

bool KVShard::setLowPriorityVbSnapshotFlag(bool lowPriority) {
    return lowPrioritySnapshot.cas(!lowPriority, lowPrioritySnapshot);
}
