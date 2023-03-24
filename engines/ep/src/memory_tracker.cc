/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "memory_tracker.h"

#include "ep_engine.h"
#include "ep_engine_group.h"
#include "kv_bucket.h"

bool StrictQuotaMemoryTracker::isBelowMutationMemoryQuota(
        size_t pendingBytes) const {
    return engine.getEpStats().getEstimatedTotalMemoryUsed() + pendingBytes <
           engine.getEpStats().getMaxDataSize() *
                   engine.getKVBucket()->getMutationMemRatio();
}

bool StrictQuotaMemoryTracker::isBelowMemoryQuota(size_t pendingBytes) const {
    return engine.getEpStats().getEstimatedTotalMemoryUsed() + pendingBytes <
           engine.getEpStats().getMaxDataSize();
}

bool StrictQuotaMemoryTracker::isBelowBackfillThreshold() const {
    return engine.getEpStats().getEstimatedTotalMemoryUsed() <
           engine.getEpStats().getMaxDataSize() *
                   engine.getKVBucket()->getBackfillMemoryThreshold();
}

bool StrictQuotaMemoryTracker::needsToFreeMemory() const {
    return engine.getKVBucket()->getPageableMemCurrent() >
           engine.getKVBucket()->getPageableMemHighWatermark();
}
