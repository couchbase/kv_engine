/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "vbucketmap.h"


VBucketMap::VBucketMap(Configuration &config) :
    buckets(new RCPtr<VBucket>[config.getMaxVbuckets()]),
    bucketDeletion(new Atomic<bool>[config.getMaxVbuckets()]),
    bucketCreation(new Atomic<bool>[config.getMaxVbuckets()]),
    persistenceCheckpointIds(new Atomic<uint64_t>[config.getMaxVbuckets()]),
    size(config.getMaxVbuckets())
{
    highPriorityVbSnapshot.set(false);
    lowPriorityVbSnapshot.set(false);
    for (size_t i = 0; i < size; ++i) {
        bucketDeletion[i].set(false);
        bucketCreation[i].set(false);
        persistenceCheckpointIds[i].set(0);
    }
}

VBucketMap::~VBucketMap() {
    delete[] buckets;
    delete[] bucketDeletion;
    delete[] bucketCreation;
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

ENGINE_ERROR_CODE VBucketMap::addBucket(const RCPtr<VBucket> &b) {
    if (static_cast<size_t>(b->getId()) < size) {
        buckets[b->getId()].reset(b);
        getLogger()->log(EXTENSION_LOG_INFO, NULL,
                         "Mapped new vbucket %d in state %s",
                         b->getId(), VBucket::toString(b->getState()));
        return ENGINE_SUCCESS;
    }
    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Cannot create vb %d, max vbuckets is %d",
                         b->getId(), size);
    return ENGINE_ERANGE;
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

std::vector<int> VBucketMap::getBucketsSortedByState(void) const {
    std::vector<int> rv;
    for (int state = vbucket_state_active;
         state <= vbucket_state_dead; ++state) {
        for (size_t i = 0; i < size; ++i) {
            RCPtr<VBucket> b = buckets[i];
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
    return bucketDeletion[id].get();
}

bool VBucketMap::setBucketDeletion(uint16_t id, bool delBucket) {
    assert(id < size);
    return bucketDeletion[id].cas(!delBucket, delBucket);
}

bool VBucketMap::isBucketCreation(uint16_t id) const {
    assert(id < size);
    return bucketCreation[id].get();
}

bool VBucketMap::setBucketCreation(uint16_t id, bool rv) {
    assert(id < size);
    return bucketCreation[id].cas(!rv, rv);
}

uint64_t VBucketMap::getPersistenceCheckpointId(uint16_t id) const {
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
