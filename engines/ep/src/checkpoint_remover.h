/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <executor/globaltask.h>

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

    bool run() override;

    std::string getDescription() const override {
        return "Removing closed unreferenced checkpoints from memory";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
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
