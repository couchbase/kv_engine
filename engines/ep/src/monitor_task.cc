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

#include <executor/executorpool.h>

bool MonitorTask::run() {
    const auto fragStats = cb::ArenaMalloc::getFragmentationStats(
            engine->getArenaMallocClient());
    auto& stats = engine->getEpStats();
    stats.residentBytes = fragStats.getResidentBytes();

    EP_LOG_DEBUG_CTX("MonitorTask:",
                     {"interval", interval.load()},
                     {"rss", stats.residentBytes});

    // Sleep for "interval" and reschedule
    snooze(interval.load());
    return !stats.isShutdown;
}
