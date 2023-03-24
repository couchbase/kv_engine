/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <cstddef>

class EventuallyPersistentEngine;
class EPEngineGroup;

/**
 * Interface used for OOM conditions and triggering memory reclamation.
 */
class MemoryTracker {
public:
    /**
     * Are we currently below the mutation memory quota (max_size *
     * mutation_mem_ratio)?
     *
     * We should temp oom when this is not true.
     *
     * @param pendingBytes Extra allocations that will be considered in the
     * result.
     * @return true if we are below the quota after pendingBytes are allocated
     */
    virtual bool isBelowMutationMemoryQuota(size_t pendingBytes = 0) const = 0;

    /**
     * Are we currently below the memory quota?
     *
     * We should hard oom when this is not true.
     *
     * @param pendingBytes Extra allocations that will be considered in the
     * result.
     * @return true if we are below the quota after pendingBytes are allocated
     */
    virtual bool isBelowMemoryQuota(size_t pendingBytes = 0) const = 0;

    /**
     * Are we currently below the backfill memory quota?
     * @return true if we are below the quota
     */
    virtual bool isBelowBackfillThreshold() const = 0;

    /**
     * @returns true if we need to start memory reclaimation.
     */
    virtual bool needsToFreeMemory() const = 0;

    virtual ~MemoryTracker() = default;
};

class StrictQuotaMemoryTracker : public MemoryTracker {
public:
    StrictQuotaMemoryTracker(EventuallyPersistentEngine& engine)
        : engine(engine) {
    }

    bool isBelowMutationMemoryQuota(size_t pendingBytes) const override;
    bool isBelowMemoryQuota(size_t pendingBytes) const override;
    bool isBelowBackfillThreshold() const override;
    bool needsToFreeMemory() const override;

private:
    EventuallyPersistentEngine& engine;
};
