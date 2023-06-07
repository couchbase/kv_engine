/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "yielding_task.h"
#include <logger/logger.h>

bool YieldingTask::run() {
    try {
        auto optSleepTime = function();
        if (optSleepTime) {
            snooze(optSleepTime->count());
            // task _does_ wish to run again.
            return true;
        }
        // task does not wish to snooze and run again, done.
        return false;

    } catch (const std::exception& e) {
        LOG_CRITICAL(R"(YieldingTask::run("{}") received exception: {})",
                     name,
                     e.what());
    }
    return false;
}
