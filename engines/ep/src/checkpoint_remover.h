/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#pragma once

#include "globaltask.h"

class EPStats;
class EventuallyPersistentEngine;

/**
 * Dispatcher job responsible for removing closed unreferenced checkpoints
 * from memory.
 */
class ClosedUnrefCheckpointRemoverTask : public GlobalTask {
public:

    /**
     * Construct ClosedUnrefCheckpointRemover.
     * @param s the store
     * @param st the stats
     */
    ClosedUnrefCheckpointRemoverTask(EventuallyPersistentEngine *e,
                                     EPStats &st, size_t interval) :
        GlobalTask(e, TaskId::ClosedUnrefCheckpointRemoverTask, interval, false),
        engine(e), stats(st), sleepTime(interval), available(true) {}

    void attemptCursorDropping(size_t amountOfMemoryToClear);

    bool run(void);

    std::string getDescription() {
        return "Removing closed unreferenced checkpoints from memory";
    }

    std::chrono::microseconds maxExpectedDuration() {
        // Empirical evidence from perf runs suggests this task runs
        // under 250ms 99.99999% of the time.
        return std::chrono::milliseconds(250);
    }

    std::pair<bool, size_t> isCursorDroppingNeeded() const;

private:
    EventuallyPersistentEngine *engine;
    EPStats                   &stats;
    size_t                     sleepTime;
    std::atomic<bool>          available;
};
