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

#include "array_histogram.h"
#include "item_eviction_strategy.h"

using MFUHistogram =
        ArrayHistogram<uint64_t, std::numeric_limits<uint8_t>::max() + 1>;

class EvictionRatios;

/**
 * A simple eviction strategy which just selects items for eviction based on
 * their MFU, based on pre-determined thresholds.
 *
 * Separate thresholds can be provided for active+pending and replica,
 * to allow eviction to prefer to remove replica data (keeping more active data
 * resident, giving better cache hit ratio if possible).
 *
 * The thresholds are expected to be determined based on the per-vb histograms
 * of MFU values tracked for evictable items.
 */
class MFUOnlyItemEviction : public ItemEvictionStrategy {
public:
    struct Thresholds {
        // these thresholds are optional to allow expressing "do not evict
        // anything from vbs in this state" with a nullopt.
        // 0 can't be used for this, as it would still evict items with MFU==0.
        std::optional<uint8_t> activePending;
        std::optional<uint8_t> replica;
    };

    MFUOnlyItemEviction() = default;
    /**
     * Construct a ItemEvictionStrategy which selects items to evict based
     * solely on their MFU value.
     */
    MFUOnlyItemEviction(Thresholds thresholds);

    void setupVBucketVisit(uint64_t numExpectedItems) override {
        // no per-visit work needed.
    }

    void tearDownVBucketVisit(vbucket_state_t state) override {
        // no per-visit work needed.
    }

    bool shouldTryEvict(uint8_t freq,
                        uint64_t age,
                        vbucket_state_t state) override;

    void eligibleItemSeen(uint8_t freq,
                          uint64_t age,
                          vbucket_state_t state) override {
        // MFU histograms are tracked upfront and don't need to be learned
    }

private:
    // The frequency counter thresholds that are used to determine which items
    // should be considered for eviction. Computed from aggregated MFU
    // histograms.
    Thresholds thresholds;
};
