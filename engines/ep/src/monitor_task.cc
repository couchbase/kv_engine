/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "monitor_task.h"

#include "bucket_logger.h"
#include "dcp/dcpconnmap.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include "memory_tracker.h"

#include <executor/executorpool.h>

bool MonitorTask::run() {
    const auto fragStats = cb::ArenaMalloc::getFragmentationStats(
            engine->getArenaMallocClient());
    auto& stats = engine->getEpStats();
    stats.residentBytes = fragStats.getResidentBytes();
    stats.scoredFragmentation = stats.getScoredFragmentation(fragStats);

    // While the RSS/fragmentation back-pressure is critical the defragmenter
    // must run at its aggressive cadence (min sleep, age thresholds 0).
    // calculateSleepTimeAndRunState() checks isFragmentationCritical() before
    // the mode switch, so every mode -- static included -- collapses its sleep
    // to defragmenter_auto_min_sleep while it holds, not the configured
    // interval. So we only need to kick it out of a longer sleep once: at the
    // min it self-sustains that cadence, and re-waking it every tick would just
    // fight it, so skip the wake when it already sleeps at (or below) the min.
    // A disabled defragmenter never runs defrag(), so skip that too.
    auto& config = engine->getConfiguration();
    if (config.isDefragmenterEnabled() &&
        engine->getMemoryTracker().isFragmentationCritical()) {
        const auto minSleep =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::duration<double>{
                                config.getDefragmenterAutoMinSleep()});
        if (engine->getKVBucket()->getDefragmenterTaskSleepTime() > minSleep) {
            engine->getKVBucket()->wakeUpDefragmenter();
        }
    }

    EP_LOG_DEBUG_CTX(
            "MonitorTask:",
            {"interval", interval.load()},
            {"rss", stats.residentBytes.load()},
            {"scored_fragmentation", stats.scoredFragmentation.load()});

    // Sleep for "interval" and reschedule
    snooze(interval.load());
    return !stats.isShutdown;
}
