/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef VBUCKETMAP_HH
#define VBUCKETMAP_HH 1

#include "configuration.hh"
#include "vbucket.hh"
#include "kvshard.hh"

/**
 * A map of known vbuckets.
 */
class VBucketMap {
friend class EventuallyPersistentStore;

public:
    VBucketMap(EPStats &stats, Configuration &config,
               EventuallyPersistentStore &store);
    ~VBucketMap();

    ENGINE_ERROR_CODE addBucket(const RCPtr<VBucket> &b);
    void removeBucket(uint16_t id);
    void addBuckets(const std::vector<VBucket*> &newBuckets);
    RCPtr<VBucket> getBucket(uint16_t id) const;
    size_t getSize() const;
    std::vector<int> getBuckets(void) const;
    std::vector<int> getBucketsSortedByState(void) const;
    bool isBucketDeletion(uint16_t id) const;
    bool setBucketDeletion(uint16_t id, bool delBucket);
    bool isBucketCreation(uint16_t id) const;
    bool setBucketCreation(uint16_t id, bool rv);
    uint64_t getPersistenceCheckpointId(uint16_t id) const;
    void setPersistenceCheckpointId(uint16_t id, uint64_t checkpointId);
    KVShard* getShard(uint16_t id) const;

private:

    std::vector<KVShard*> shards;
    Atomic<bool> *bucketDeletion;
    Atomic<bool> *bucketCreation;
    Atomic<uint64_t> *persistenceCheckpointIds;
    size_t size;
    size_t numShards;

    DISALLOW_COPY_AND_ASSIGN(VBucketMap);
};

#endif /* VBUCKET_HH */
