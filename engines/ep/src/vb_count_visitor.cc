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

#include "vb_count_visitor.h"

#include "vbucket.h"

void VBucketCountVisitor::visitBucket(VBucket& vb) {
    ++numVbucket;
    numItems += vb.getNumItems();
    numTempItems += vb.getNumTempItems();
    nonResident += vb.getNumNonResidentItems();

    if (vb.getHighPriorityChkSize() > 0) {
        chkPersistRemaining++;
    }

    if (desired_state != vbucket_state_dead) {
        htMemory += vb.ht.memorySize();
        htItemMemory += vb.ht.getItemMemory();
        htUncompressedItemMemory += vb.ht.getUncompressedItemMemory();
        htCacheSize += vb.ht.getCacheSize();
        numEjects += vb.ht.getNumEjects();
        numExpiredItems += vb.numExpiredItems;
        metaDataMemory += vb.ht.getMetadataMemory();
        metaDataDisk += vb.metaDataDisk;

        checkpointMemory += vb.getChkMgrMemUsage();
        checkpointMemoryQueue += vb.getCMQueuedItemsMemUsage();
        checkpointMemoryUnreferenced +=
                vb.getChkMgrMemUsageOfUnrefCheckpoints();

        checkpointMemOverheadAllocatorBytes +=
                vb.getCMMemOverheadAllocatorBytes();
        checkpointMemOverheadAllocatorBytesQueue +=
                vb.getCMMemOverheadAllocatorBytesQueue();
        checkpointMemOverheadAllocatorBytesIndex +=
                vb.getCMMemOverheadAllocatorBytesIndex();
        checkpointMemOverheadAllocatorBytesIndexKey +=
                vb.getCMMemOverheadAllocatorBytesIndexKey();

        checkpointMemOverhead += vb.getCMMemOverhead();
        checkpointMemOverheadQueue += vb.getCMMemOverheadQueue();
        checkpointMemOverheadIndex += vb.getCMMemOverheadIndex();

        checkpointMemFreedByItemExpel += vb.getCMMemFreedByItemExpel();

        bloomFilterMemory += vb.getFilterMemoryFootprint();

        opsCreate += vb.opsCreate;
        opsDelete += vb.opsDelete;
        opsGet += vb.opsGet;
        opsReject += vb.opsReject;
        opsUpdate += vb.opsUpdate;

        queueSize += vb.dirtyQueueSize;
        queueMemory += vb.dirtyQueueMem;
        queueFill += vb.dirtyQueueFill;
        queueDrain += vb.dirtyQueueDrain;
        queueAge += vb.getQueueAge();
        pendingWrites += vb.dirtyQueuePendingWrites;
        rollbackItemCount += vb.getRollbackItemCount();
        numHpVBReqs += vb.getHighPriorityChkSize();

        /*
         * The bucket stat reports the total drift of the vbuckets.
         */
        auto absHLCDrift = vb.getHLCDriftStats();
        totalAbsHLCDrift.total += absHLCDrift.total;
        totalAbsHLCDrift.updates += absHLCDrift.updates;

        /*
         * Total up the exceptions
         */
        auto driftExceptionCounters = vb.getHLCDriftExceptionCounters();
        totalHLCDriftExceptionCounters.ahead += driftExceptionCounters.ahead;
        totalHLCDriftExceptionCounters.behind += driftExceptionCounters.behind;

        syncWriteAcceptedCount += vb.getSyncWriteAcceptedCount();
        syncWriteCommittedCount += vb.getSyncWriteCommittedCount();
        syncWriteAbortedCount += vb.getSyncWriteAbortedCount();
    }
}

void DatatypeStatVisitor::visitBucket(VBucket& vb) {
    // Iterate over each datatype combination
    auto vbDatatypeCounts = vb.ht.getDatatypeCounts();
    for (uint8_t ii = 0; ii < datatypeCounts.size(); ++ii) {
        datatypeCounts[ii] += vbDatatypeCounts[ii];
    }
}

void VBucketStatAggregator::visitBucket(VBucket& vb) {
    auto it = visitorMap.find(vb.getState());
    if (it != visitorMap.end()) {
        for (auto* visitor : it->second) {
            visitor->visitBucket(vb);
        }
    }
}

void VBucketStatAggregator::addVisitor(VBucketStatVisitor* visitor) {
    visitorMap[visitor->getVBucketState()].push_back(visitor);
}
