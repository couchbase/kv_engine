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

#ifndef SRC_VBUCKETMAP_H_
#define SRC_VBUCKETMAP_H_ 1

#include "config.h"

#include <vector>

#include "configuration.h"
#include "kvshard.h"
#include "vbucket.h"

/**
 * A map of known vbuckets.
 */
class VBucketMap {
friend class EventuallyPersistentStore;
friend class Warmup;

public:
    VBucketMap(Configuration &config, EventuallyPersistentStore &store);
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
    size_t getNumShards() const;

private:

    std::vector<KVShard*> shards;
    Atomic<bool> *bucketDeletion;
    Atomic<bool> *bucketCreation;
    Atomic<uint64_t> *persistenceCheckpointIds;
    size_t size;
    size_t numShards;

    DISALLOW_COPY_AND_ASSIGN(VBucketMap);
};

#endif  // SRC_VBUCKETMAP_H_
