/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "initial_mfu_task.h"

#include "ep_engine.h"
#include "item.h"
#include "kv_bucket.h"
#include "mfu_only_item_eviction.h"
#include "vbucket.h"

InitialMFUTask::InitialMFUTask(EventuallyPersistentEngine& e)
    : GlobalTask(e,
                 TaskId::InitialMFUTask,
                 e.getConfiguration().getItemEvictionInitialMfuUpdateInterval(),
                 false /* completeBeforeShutdown */) {
}

std::string InitialMFUTask::getDescription() const {
    return "Initial Item MFU updater";
}

bool InitialMFUTask::run() {
    if (engine->getEpStats().isShutdown) {
        return false;
    }

    const auto& config = engine->getConfiguration();
    auto& kvBucket = *engine->getKVBucket();

    // The learning eviction strategy uses a separate age component and does not
    // require us to derive an initial MFU value from the MFU histograms.
    if (config.getItemEvictionStrategy() != "upfront_mfu_only") {
        kvBucket.setInitialMFU(Item::initialFreqCount);
        snooze(INT_MAX);
        return true;
    }

    snooze(config.getItemEvictionInitialMfuUpdateInterval());

    MFUHistogram aggregated;
    for (const auto& vb : kvBucket.getVBuckets()) {
        const auto vbState = vb.getState();
        // We want the initial MFU to allow us to keep items in active and
        // pending vBuckets for some number of paging generations.
        // As such, we only aggregate the histograms for those.
        if (vbState == vbucket_state_active ||
            vbState == vbucket_state_pending) {
            aggregated += vb.getEvictableMFUHistogram();
        }
    }

    uint8_t newInitialMFU = aggregated.getValueAtPercentile(
            config.getItemEvictionInitialMfuPercentile());

    // Increment by one, to avoid starting out at the lowest MFU value, if
    // that is very saturated.
    // Example:
    //    4 - 5     : ( 32.5210%) 7744155 █████████████▉
    //    5 - 6     : ( 65.9537%) 7961283 ██████████████▍
    //    6 - 7     : ( 72.8209%) 1635269 ██▉
    //    7 - 8     : ( 75.0479%)  530305 ▉
    // If we pick 30%ile, we'd pick initial MFU = 4, but that won't give
    // new items any protection from immediate eviction.
    newInitialMFU += 1;

    // Never start at MFU lower than 4. This leaves space for "cold" items to
    // become "very cold" as we reduce their freq counts due to unsuccessful
    // eviction/decay.
    newInitialMFU = std::max(Item::initialFreqCount, newInitialMFU);

    kvBucket.setInitialMFU(newInitialMFU);
    return true;
}

std::chrono::microseconds InitialMFUTask::maxExpectedDuration() const {
    using namespace std::chrono_literals;
    // The runtime of this task is expected to be relatively short.
    // Exact value may be tuned in the future.
    return 50ms;
}
