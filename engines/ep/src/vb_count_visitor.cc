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
 *   distributed undemor the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "vb_count_visitor.h"

void VBucketCountVisitor::visitBucket(VBucketPtr& vb) {
    ++numVbucket;
    numItems += vb->getNumItems();
    numTempItems += vb->getNumTempItems();
    nonResident += vb->getNumNonResidentItems();

    if (vb->getHighPriorityChkSize() > 0) {
        chkPersistRemaining++;
    }

    if (desired_state != vbucket_state_dead) {
        htMemory += vb->ht.memorySize();
        htItemMemory += vb->ht.getItemMemory();
        htCacheSize += vb->ht.cacheSize;
        numEjects += vb->ht.getNumEjects();
        numExpiredItems += vb->numExpiredItems;
        metaDataMemory += vb->ht.metaDataMemory;
        metaDataDisk += vb->metaDataDisk;
        opsCreate += vb->opsCreate;
        opsUpdate += vb->opsUpdate;
        opsDelete += vb->opsDelete;
        opsReject += vb->opsReject;

        queueSize += vb->dirtyQueueSize;
        queueMemory += vb->dirtyQueueMem;
        queueFill += vb->dirtyQueueFill;
        queueDrain += vb->dirtyQueueDrain;
        queueAge += vb->getQueueAge();
        backfillQueueSize += vb->getBackfillSize();
        pendingWrites += vb->dirtyQueuePendingWrites;
        rollbackItemCount += vb->getRollbackItemCount();
        numHpVBReqs += vb->getHighPriorityChkSize();

        /*
         * The bucket stat reports the total drift of the vbuckets.
         */
        auto absHLCDrift = vb->getHLCDriftStats();
        totalAbsHLCDrift.total += absHLCDrift.total;
        totalAbsHLCDrift.updates += absHLCDrift.updates;

        /*
         * Total up the exceptions
         */
        auto driftExceptionCounters = vb->getHLCDriftExceptionCounters();
        totalHLCDriftExceptionCounters.ahead += driftExceptionCounters.ahead;
        totalHLCDriftExceptionCounters.behind += driftExceptionCounters.behind;

        // Iterate over each datatype combination
        for (uint8_t ii = 0; ii < datatypeCounts.size(); ++ii) {
            datatypeCounts[ii] += vb->ht.datatypeCounts[ii];
        }
    }
}
