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

#include "atomic.h"
#include "hlc.h"
#include "vb_visitors.h"

#include <array>
#include <map>

class VBucket;

/**
 * Vbucket visitor that counts active vbuckets.
 */
class VBucketCountVisitor : public VBucketVisitor {
public:
    explicit VBucketCountVisitor(vbucket_state_t state)
        : desired_state(state),
          numItems(0),
          numTempItems(0),
          nonResident(0),
          numVbucket(0),
          htMemory(0),
          htItemMemory(0),
          htUncompressedItemMemory(0),
          htCacheSize(0),
          numEjects(0),
          numExpiredItems(0),
          metaDataMemory(0),
          metaDataDisk(0),
          checkpointMemory(0),
          checkpointMemoryUnreferenced(0),
          checkpointMemoryOverhead(0),
          opsCreate(0),
          opsDelete(0),
          opsGet(0),
          opsReject(0),
          opsUpdate(0),
          queueSize(0),
          queueMemory(0),
          queueFill(0),
          queueDrain(0),
          pendingWrites(0),
          chkPersistRemaining(0),
          datatypeCounts{{0}},
          queueAge(0),
          rollbackItemCount(0),
          numHpVBReqs(0),
          totalAbsHLCDrift(),
          totalHLCDriftExceptionCounters(),
          syncWriteAcceptedCount(0),
          syncWriteCommittedCount(0),
          syncWriteAbortedCount(0) {
    }

    void visitBucket(const VBucketPtr& vb) override;

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

    size_t getCheckpointMemory() {
        return checkpointMemory;
    }

    size_t getCheckpointMemoryUnreferenced() {
        return checkpointMemoryUnreferenced;
    }

    size_t getCheckpointMemoryOverhead() const {
        return checkpointMemoryOverhead;
    }

    size_t getHashtableMemory() {
        return htMemory;
    }

    size_t getItemMemory() {
        return htItemMemory;
    }

    size_t getUncompressedItemMemory() {
        return htUncompressedItemMemory;
    }
    size_t getCacheSize() {
        return htCacheSize;
    }

    size_t getOpsCreate() {
        return opsCreate;
    }
    size_t getOpsDelete() {
        return opsDelete;
    }
    size_t getOpsGet() {
        return opsGet;
    }
    size_t getOpsReject() {
        return opsReject;
    }
    size_t getOpsUpdate() {
        return opsUpdate;
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

    size_t getDatatypeCount(protocol_binary_datatype_t datatype) const {
        return datatypeCounts[datatype];
    }
    const size_t getNumDatatypes() const {
        return datatypeCounts.size();
    }

    uint64_t getRollbackItemCount() {
        return rollbackItemCount;
    }

    size_t getNumHpVBReqs() {
        return numHpVBReqs;
    }

    HLC::DriftStats getTotalAbsHLCDrift() {
        return totalAbsHLCDrift;
    }
    HLC::DriftExceptions getTotalHLCDriftExceptionCounters() {
        return totalHLCDriftExceptionCounters;
    }

    size_t getSyncWriteAcceptedCount() {
        return syncWriteAcceptedCount;
    }

    size_t getSyncWriteCommittedCount() {
        return syncWriteCommittedCount;
    }

    size_t getSyncWriteAbortedCount() {
        return syncWriteAbortedCount;
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
    size_t htUncompressedItemMemory;
    size_t htCacheSize;
    size_t numEjects;
    size_t numExpiredItems;
    size_t metaDataMemory;
    size_t metaDataDisk;
    size_t checkpointMemory;
    size_t checkpointMemoryUnreferenced;
    size_t checkpointMemoryOverhead;

    size_t opsCreate;
    size_t opsDelete;
    size_t opsGet;
    size_t opsReject;
    size_t opsUpdate;

    size_t queueSize;
    size_t queueMemory;
    size_t queueFill;
    size_t queueDrain;
    size_t pendingWrites;
    size_t chkPersistRemaining;
    HashTable::DatatypeCombo datatypeCounts;
    uint64_t queueAge;
    uint64_t rollbackItemCount;
    size_t numHpVBReqs;
    HLC::DriftStats totalAbsHLCDrift;
    HLC::DriftExceptions totalHLCDriftExceptionCounters;
    size_t syncWriteAcceptedCount;
    size_t syncWriteCommittedCount;
    size_t syncWriteAbortedCount;
};

/**
 * A container class holding VBucketCountVisitors to aggregate stats for
 * different vbucket states.
 */
class VBucketCountAggregator : public VBucketVisitor  {
public:
    void visitBucket(const VBucketPtr& vb) override;

    void addVisitor(VBucketCountVisitor* visitor);

private:
    std::map<vbucket_state_t, VBucketCountVisitor*> visitorMap;
};
