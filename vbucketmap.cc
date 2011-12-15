/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "vbucketmap.hh"


VBucketMap::VBucketMap(Configuration &config) :
    buckets(new RCPtr<VBucket>[config.getMaxVbuckets()]),
    bucketDeletion(new Atomic<bool>[config.getMaxVbuckets()]),
    bucketVersions(new Atomic<uint16_t>[config.getMaxVbuckets()]),
    persistenceCheckpointIds(new Atomic<uint64_t>[config.getMaxVbuckets()]),
    size(config.getMaxVbuckets())
{
    highPriorityVbSnapshot.set(false);
    lowPriorityVbSnapshot.set(false);
    for (size_t i = 0; i < size; ++i) {
        bucketDeletion[i].set(false);
        bucketVersions[i].set(static_cast<uint16_t>(-1));
        persistenceCheckpointIds[i].set(0);
    }
}

VBucketMap::~VBucketMap() {
    delete[] buckets;
    delete[] bucketDeletion;
    delete[] bucketVersions;
    delete[] persistenceCheckpointIds;
}

RCPtr<VBucket> VBucketMap::getBucket(uint16_t id) const {
    static RCPtr<VBucket> emptyVBucket;
    if (static_cast<size_t>(id) < size) {
        return buckets[id];
    } else {
        return emptyVBucket;
    }
}

void VBucketMap::addBucket(const RCPtr<VBucket> &b) {
    if (static_cast<size_t>(b->getId()) < size) {
        buckets[b->getId()].reset(b);
        getLogger()->log(EXTENSION_LOG_INFO, NULL,
                         "Mapped new vbucket %d in state %s",
                         b->getId(), VBucket::toString(b->getState()));
    } else {
        throw new NeedMoreBuckets;
    }
}

void VBucketMap::removeBucket(uint16_t id) {
    if (static_cast<size_t>(id) < size) {
        // Theoretically, this could be off slightly.  In
        // practice, this happens only on dead vbuckets.
        buckets[id].reset();
    }
}

std::vector<int> VBucketMap::getBuckets(void) const {
    std::vector<int> rv;
    for (size_t i = 0; i < size; ++i) {
        RCPtr<VBucket> b(buckets[i]);
        if (b) {
            rv.push_back(b->getId());
        }
    }
    return rv;
}

size_t VBucketMap::getSize(void) const {
    return size;
}

bool VBucketMap::isBucketDeletion(uint16_t id) const {
    assert(id < size);
    return bucketDeletion[id].get();
}

bool VBucketMap::setBucketDeletion(uint16_t id, bool delBucket) {
    assert(id < size);
    return bucketDeletion[id].cas(!delBucket, delBucket);
}

uint16_t VBucketMap::getBucketVersion(uint16_t id) const {
    assert(id < size);
    return bucketVersions[id].get();
}

void VBucketMap::setBucketVersion(uint16_t id, uint16_t vb_version) {
    assert(id < size);
    bucketVersions[id].set(vb_version);
}

uint64_t VBucketMap::getPersistenceCheckpointId(uint16_t id) {
    assert(id < size);
    return persistenceCheckpointIds[id].get();
}

void VBucketMap::setPersistenceCheckpointId(uint16_t id, uint64_t checkpointId) {
    assert(id < size);
    persistenceCheckpointIds[id].set(checkpointId);
}

bool VBucketMap::isHighPriorityVbSnapshotScheduled(void) const {
    return highPriorityVbSnapshot.get();
}

bool VBucketMap::setHighPriorityVbSnapshotFlag(bool highPrioritySnapshot) {
    return highPriorityVbSnapshot.cas(!highPrioritySnapshot, highPrioritySnapshot);
}

bool VBucketMap::isLowPriorityVbSnapshotScheduled(void) const {
    return lowPriorityVbSnapshot.get();
}

bool VBucketMap::setLowPriorityVbSnapshotFlag(bool lowPrioritySnapshot) {
    return lowPriorityVbSnapshot.cas(!lowPrioritySnapshot, lowPrioritySnapshot);
}

void VBucketMap::addBuckets(const std::vector<VBucket*> &newBuckets) {
    std::vector<VBucket*>::const_iterator it;
    for (it = newBuckets.begin(); it != newBuckets.end(); ++it) {
        RCPtr<VBucket> v(*it);
        addBucket(v);
    }
}
