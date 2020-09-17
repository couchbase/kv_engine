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

#include <memcached/vbucket.h>

#include <queue>

#define NO_VBUCKETS_INSTANTIATED 0xFFFF
#define RETRY_FLUSH_VBUCKET (-1)

class EPBucket;
class KVShard;

/**
 * Manage persistence of data for an EPBucket.
 */
class Flusher {
public:
    Flusher(EPBucket* st, KVShard* k);

    ~Flusher();

    bool stop(bool isForceShutdown = false);
    void wait();
    bool pause();
    bool resume();
    void start();
    void wake();
    bool step(GlobalTask *task);

    const char * stateName() const;

    void notifyFlushEvent(Vbid vbid) {
        if (!lpVbs.pushUnique(vbid)) {
            // Something is already in the queue, no need to wake the flusher
            return;
        }

        wake();
    }
    void setTaskId(size_t newId) { taskId = newId; }

    // Testing hook - if non-empty, called from step() just before snoozing
    // the task.
    TestingHook<> stepPreSnoozeHook;

    size_t getHPQueueSize() const;

    size_t getLPQueueSize() const;

    size_t getHighPriorityCount() const;

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

    KVShard *shard;

    DISALLOW_COPY_AND_ASSIGN(Flusher);
};
