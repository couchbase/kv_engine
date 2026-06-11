/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "ep_task.h"
#include <relaxed_atomic.h>

/**
 * The MonitorTask performs relatively expensive operations
 * (eg any slow call that can't be executed in frontend threads) for system
 * monitoring purpose.
 *
 * One example (and initial motivation for introducing the task) is
 * polling the bucket RSS and storing that information into EPStats. That
 * RSS information can then be read from EPStats by hot paths (eg frontend
 * worker thread checks for memory-recovery triggers).
 */
class MonitorTask : public EpTask {
public:
    explicit MonitorTask(EventuallyPersistentEngine& e,
                         std::chrono::seconds interval)
        : EpTask(e, TaskId::MonitorTask, 0, false), interval(interval) {
    }

    bool run() override;

    std::string getDescription() const override {
        return "MonitorTask";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        return std::chrono::milliseconds(10);
    }

    void setInterval(std::chrono::seconds i) {
        interval = i;
    }

    std::chrono::seconds getInterval() const {
        return interval;
    }

private:
    cb::RelaxedAtomic<std::chrono::seconds> interval;
};
