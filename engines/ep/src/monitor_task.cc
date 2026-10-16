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
    auto& stats = engine->getEpStats();
    // The MonitorTask is the single owner of the (expensive) jemalloc query;
    // cache the result in EPStats so the other consumers (the back-pressure
    // gate and the DefragmenterTask) read it rather than querying jemalloc.
    // Keep the snapshot in a local so this task reuses it below (the critical
    // check and the debug log) instead of re-reading the cache under rlock.
    const auto fragStats = cb::ArenaMalloc::getFragmentationStats(
            engine->getArenaMallocClient());
    *stats.fragStats.wlock() = fragStats;

    // Fragmentation is critical, so the defragmenter must run often.
    // If any defragmenter task sleeps longer than defragmenter_critical_sleep,
    // wake the tasks now.
    // A disabled defragmenter does nothing, so do not wake it.
    auto& config = engine->getConfiguration();
    if (config.isDefragmenterEnabled() &&
        engine->getMemoryTracker().isFragmentationCritical(fragStats)) {
        const auto criticalSleep =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::duration<double>{
                                config.getDefragmenterCriticalSleep()});
        if (engine->getKVBucket()->getDefragmenterTaskSleepTime() >
            criticalSleep) {
            engine->getKVBucket()->wakeUpDefragmenter();
        }
    }

    EP_LOG_DEBUG_CTX(
            "MonitorTask:",
            {"interval", interval.load()},
            {"rss", fragStats.getResidentBytes()},
            {"scored_fragmentation", stats.getScoredFragmentation(fragStats)});

    // Sleep for "interval" and reschedule
    snooze(interval.load());
    return !stats.isShutdown;
}
