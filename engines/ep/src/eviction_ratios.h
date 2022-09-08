/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

// forward decl
enum vbucket_state_t : int;

/**
 * Tracks the desired eviction ratios for different vbucket states.
 *
 * PagingVisitor attempts to evict the specified fraction of the items from
 * vbuckets in the given state.
 *
 * Note, deleted vbuckets are not evicted from, and pending vbuckets currently
 * share a ratio with active vbuckets, so only two ratios are tracked.
 */
class EvictionRatios {
public:
    EvictionRatios() = default;

    EvictionRatios(double activeAndPending, double replica)
        : activeAndPending(activeAndPending), replica(replica) {
    }
    /**
     * Get the fraction of items to be evicted from vbuckets in the given state.
     */
    double getForState(vbucket_state_t state) const;
    /**
     * Set the fraction of items to be evicted from vbuckets in the given state.
     *
     * Active and pending vbuckets share a ratio, setting either will overwrite
     * the existing value for both states.
     *
     * Dead vbuckets are not evicted from, so setting a ratio for dead
     * will be ignored.
     */
    void setForState(vbucket_state_t state, double value);

private:
    double activeAndPending = 0.0;
    double replica = 0.0;
};