/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "utilities/testing_hook.h"
#include "utility.h"
#include "vb_ready_queue.h"
#include <executor/cb3_executorthread.h>
#include "vbucket_fwd.h"

#include <memcached/vbucket.h>

#include <queue>

class EPBucket;

/**
 * Manage persistence of data for an EPBucket.
 */
class Flusher {
public:
    Flusher(EPBucket* st, size_t flusherId);

    ~Flusher();

    bool stop(bool isForceShutdown = false);
    void wait();
    bool pause();
    bool resume();
    void start();
    void wake();
    bool step(GlobalTask *task);

    const char * stateName() const;

    void notifyFlushEvent(VBucketPtr vb);

    // Testing hook - if non-empty, called from step() just before snoozing
    // the task.
    TestingHook<> stepPreSnoozeHook;

    size_t getHPQueueSize() const;

    size_t getLPQueueSize() const;

private:
    enum class State {
        Initializing,
        Running,
        Paused,
        Stopping,
        Stopped
    };

    bool transitionState(State to);
    bool validTransition(State to) const;

    /**
     * Flush a single vBucket
     * @return true if there is more work to do
     */
    bool flushVB();
    void completeFlush();
    void initialize();
    void schedule_UNLOCKED();

    const char* stateName(State st) const;

    EPBucket* store;
    std::atomic<State> _state;

    // Used for serializaling attempts to start the flusher from
    // different threads.
    std::mutex                        taskMutex;
    std::atomic<size_t>      taskId;

    std::atomic<bool> forceShutdownReceived;
    VBReadyQueue hpVbs;
    VBReadyQueue lpVbs;
    bool doHighPriority;
    size_t numHighPriority;
    std::atomic<bool> pendingMutation;

    /**
     * UID of this flusher. Required for to name the various FlusherTasks that
     * we create.
     */
    size_t flusherId;

    DISALLOW_COPY_AND_ASSIGN(Flusher);
};
