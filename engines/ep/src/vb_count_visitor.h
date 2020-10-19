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
    explicit VBucketCountVisitor(vbucket_state_t state) : desired_state(state) {
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
        if (numItems && numItems >= nonResident) {
            size_t numResident = numItems - nonResident;
            return size_t(numResident * 100.0) / (numItems);
        }
        // Note: access-scanner depends on 100% being returned for this case
        return 100;
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
    size_t getNumDatatypes() const {
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
    vbucket_state_t desired_state{vbucket_state_dead};

private:
    size_t numItems{0};
    size_t numTempItems{0};
    size_t nonResident{0};
    size_t numVbucket{0};
    size_t htMemory{0};
    size_t htItemMemory{0};
    size_t htUncompressedItemMemory{0};
    size_t htCacheSize{0};
    size_t numEjects{0};
    size_t numExpiredItems{0};
    size_t metaDataMemory{0};
    size_t metaDataDisk{0};
    size_t checkpointMemory{0};
    size_t checkpointMemoryUnreferenced{0};
    size_t checkpointMemoryOverhead{0};

    size_t opsCreate{0};
    size_t opsDelete{0};
    size_t opsGet{0};
    size_t opsReject{0};
    size_t opsUpdate{0};

    size_t queueSize{0};
    size_t queueMemory{0};
    size_t queueFill{0};
    size_t queueDrain{0};
    size_t pendingWrites{0};
    size_t chkPersistRemaining{0};
    HashTable::DatatypeCombo datatypeCounts{};
    uint64_t queueAge{0};
    uint64_t rollbackItemCount{0};
    size_t numHpVBReqs{0};
    HLC::DriftStats totalAbsHLCDrift;
    HLC::DriftExceptions totalHLCDriftExceptionCounters;
    size_t syncWriteAcceptedCount{0};
    size_t syncWriteCommittedCount{0};
    size_t syncWriteAbortedCount{0};
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
