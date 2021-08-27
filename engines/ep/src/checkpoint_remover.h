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
     * Attempts to free memory by using item expelling from checkpoints.
     *
     * @param memToClear The amount of memory (in bytes) that needs to be
     *   recovered.
     * @return the amount of memory (in bytes) that was recovered.
     */
    size_t attemptItemExpelling(size_t memToClear);

    /**
     * Attempts to make checkpoints eligible for removal by using cursor
     * dropping from checkpoints.
     *
     * @param memToClear The amount of memory (in bytes) that needs to be
     *   recovered. Used to determine what cursors we have to drop.
     * @return the amount of memory (in bytes) that has been made eligible
     *   for recovering-
     */
    size_t attemptCursorDropping(size_t memToClear);

    bool run() override;

    std::string getDescription() const override {
        return "Removing closed unreferenced checkpoints from memory";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // Empirical evidence from perf runs suggests this task runs
        // under 250ms 99.99999% of the time.
        return std::chrono::milliseconds(250);
    }

private:
    EventuallyPersistentEngine *engine;
    EPStats                   &stats;
    size_t                     sleepTime;
    std::atomic<bool>          available;
};
