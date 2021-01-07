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
    // Enum for specifying the mechanism for recovering memory
    // (expelling or cursor dropping).
    enum class MemoryRecoveryMechanism { checkpointExpel, cursorDrop };

    /**
     * Construct ClosedUnrefCheckpointRemover.
     * @param s the store
     * @param st the stats
     */
    ClosedUnrefCheckpointRemoverTask(EventuallyPersistentEngine *e,
                                     EPStats &st, size_t interval) :
        GlobalTask(e, TaskId::ClosedUnrefCheckpointRemoverTask, interval, false),
        engine(e), stats(st), sleepTime(interval), available(true) {}

    /**
     * Attempts to free memory using either checkpoint expelling
     * or cursor dropping.
     * @param mechanism  The mechanism to use to recover memory
     * (either expelling or cursor dropping).
     * @param amountOfMemoryToClear  The amount of memory in bytes
     * that needs to be recovered.
     * @return the amount (in bytes) that was recovered.
     */
    size_t attemptMemoryRecovery(MemoryRecoveryMechanism mechanism,
                                 size_t amountOfMemoryToClear);

    bool run() noexcept override;

    std::string getDescription() override {
        return "Removing closed unreferenced checkpoints from memory";
    }

    std::chrono::microseconds maxExpectedDuration() override {
        // Empirical evidence from perf runs suggests this task runs
        // under 250ms 99.99999% of the time.
        return std::chrono::milliseconds(250);
    }

    /**
     * Determines whether to attempt to reduce checkpoint memory
     * usage.
     * @return a boolean value indicating whether to attempt
     * checkpoint memory reduction, and the size (in bytes) that it
     * should be reduced by.
     */
    std::pair<bool, size_t> isReductionInCheckpointMemoryNeeded() const;

private:
    EventuallyPersistentEngine *engine;
    EPStats                   &stats;
    size_t                     sleepTime;
    std::atomic<bool>          available;
};
