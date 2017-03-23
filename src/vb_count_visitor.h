/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#pragma once

#include "config.h"

#include "atomic.h"
#include "hlc.h"
#include "kv_bucket_iface.h"

class VBucket;

/**
 * Vbucket visitor that counts active vbuckets.
 */
class VBucketCountVisitor : public VBucketVisitor {
public:
    VBucketCountVisitor(vbucket_state_t state)
        : desired_state(state),
          numItems(0),
          numTempItems(0),
          nonResident(0),
          numVbucket(0),
          htMemory(0),
          htItemMemory(0),
          htCacheSize(0),
          numEjects(0),
          numExpiredItems(0),
          metaDataMemory(0),
          metaDataDisk(0),
          opsCreate(0),
          opsUpdate(0),
          opsDelete(0),
          opsReject(0),
          queueSize(0),
          queueMemory(0),
          queueFill(0),
          queueDrain(0),
          pendingWrites(0),
          chkPersistRemaining(0),
          queueAge(0),
          rollbackItemCount(0),
          totalAbsHLCDrift(),
          totalHLCDriftExceptionCounters() {
    }

    void visitBucket(RCPtr<VBucket>& vb) override;

    vbucket_state_t getVBucketState() {
        return desired_state;
    }

    size_t getNumItems() {
        return numItems;
    }

    size_t getNumTempItems() {
        return numTempItems;
    }

    size_t getNonResident() {
        return nonResident;
    }

    size_t getVBucketNumber() {
        return numVbucket;
    }

    size_t getMemResidentPer() {
        size_t numResident = numItems - nonResident;
        return (numItems != 0) ? (size_t)(numResident * 100.0) / (numItems)
                               : 100;
    }

    size_t getEjects() {
        return numEjects;
    }

    size_t getExpired() {
        return numExpiredItems;
    }

    size_t getMetaDataMemory() {
        return metaDataMemory;
    }

    size_t getMetaDataDisk() {
        return metaDataDisk;
    }

    size_t getHashtableMemory() {
        return htMemory;
    }

    size_t getItemMemory() {
        return htItemMemory;
    }
    size_t getCacheSize() {
        return htCacheSize;
    }

    size_t getOpsCreate() {
        return opsCreate;
    }
    size_t getOpsUpdate() {
        return opsUpdate;
    }
    size_t getOpsDelete() {
        return opsDelete;
    }
    size_t getOpsReject() {
        return opsReject;
    }

    size_t getQueueSize() {
        return queueSize;
    }
    size_t getQueueMemory() {
        return queueMemory;
    }
    size_t getQueueFill() {
        return queueFill;
    }
    size_t getQueueDrain() {
        return queueDrain;
    }
    uint64_t getAge() {
        return queueAge;
    }
    size_t getPendingWrites() {
        return pendingWrites;
    }
    size_t getChkPersistRemaining() {
        return chkPersistRemaining;
    }

    uint64_t getRollbackItemCount() {
        return rollbackItemCount;
    }

    HLC::DriftStats getTotalAbsHLCDrift() {
        return totalAbsHLCDrift;
    }
    HLC::DriftExceptions getTotalHLCDriftExceptionCounters() {
        return totalHLCDriftExceptionCounters;
    }

protected:
    vbucket_state_t desired_state;

private:
    size_t numItems;
    size_t numTempItems;
    size_t nonResident;
    size_t numVbucket;
    size_t htMemory;
    size_t htItemMemory;
    size_t htCacheSize;
    size_t numEjects;
    size_t numExpiredItems;
    size_t metaDataMemory;
    size_t metaDataDisk;

    size_t opsCreate;
    size_t opsUpdate;
    size_t opsDelete;
    size_t opsReject;

    size_t queueSize;
    size_t queueMemory;
    size_t queueFill;
    size_t queueDrain;
    size_t pendingWrites;
    size_t chkPersistRemaining;
    uint64_t queueAge;
    uint64_t rollbackItemCount;
    HLC::DriftStats totalAbsHLCDrift;
    HLC::DriftExceptions totalHLCDriftExceptionCounters;
};
