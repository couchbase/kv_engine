/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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

    vbucket_state_t getVBucketState() const {
        return desired_state;
    }

    size_t getNumItems() const {
        return numItems;
    }

    size_t getNumTempItems() const {
        return numTempItems;
    }

    size_t getNonResident() const {
        return nonResident;
    }

    size_t getVBucketNumber() const {
        return numVbucket;
    }

    size_t getMemResidentPer() const {
        if (numItems && numItems >= nonResident) {
            size_t numResident = numItems - nonResident;
            return size_t(numResident * 100.0) / (numItems);
        }
        // Note: access-scanner depends on 100% being returned for this case
        return 100;
    }

    size_t getEjects() const {
        return numEjects;
    }

    size_t getExpired() const {
        return numExpiredItems;
    }

    size_t getMetaDataMemory() const {
        return metaDataMemory;
    }

    size_t getMetaDataDisk() const {
        return metaDataDisk;
    }

    size_t getCheckpointMemory() const {
        return checkpointMemory;
    }

    size_t getCheckpointMemoryUnreferenced() const {
        return checkpointMemoryUnreferenced;
    }

    size_t getCheckpointMemoryOverhead() const {
        return checkpointMemoryOverhead;
    }

    size_t getHashtableMemory() const {
        return htMemory;
    }

    size_t getItemMemory() const {
        return htItemMemory;
    }

    size_t getUncompressedItemMemory() const {
        return htUncompressedItemMemory;
    }
    size_t getCacheSize() const {
        return htCacheSize;
    }

    size_t getOpsCreate() const {
        return opsCreate;
    }
    size_t getOpsDelete() const {
        return opsDelete;
    }
    size_t getOpsGet() const {
        return opsGet;
    }
    size_t getOpsReject() const {
        return opsReject;
    }
    size_t getOpsUpdate() const {
        return opsUpdate;
    }
    size_t getQueueSize() const {
        return queueSize;
    }
    size_t getQueueMemory() const {
        return queueMemory;
    }
    size_t getQueueFill() const {
        return queueFill;
    }
    size_t getQueueDrain() const {
        return queueDrain;
    }
    uint64_t getAge() const {
        return queueAge;
    }
    size_t getPendingWrites() const {
        return pendingWrites;
    }
    size_t getChkPersistRemaining() const {
        return chkPersistRemaining;
    }

    size_t getDatatypeCount(protocol_binary_datatype_t datatype) const {
        return datatypeCounts[datatype];
    }
    size_t getNumDatatypes() const {
        return datatypeCounts.size();
    }

    uint64_t getRollbackItemCount() const {
        return rollbackItemCount;
    }

    size_t getNumHpVBReqs() const {
        return numHpVBReqs;
    }

    HLC::DriftStats getTotalAbsHLCDrift() const {
        return totalAbsHLCDrift;
    }
    HLC::DriftExceptions getTotalHLCDriftExceptionCounters() const {
        return totalHLCDriftExceptionCounters;
    }

    size_t getSyncWriteAcceptedCount() const {
        return syncWriteAcceptedCount;
    }

    size_t getSyncWriteCommittedCount() const {
        return syncWriteCommittedCount;
    }

    size_t getSyncWriteAbortedCount() const {
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
