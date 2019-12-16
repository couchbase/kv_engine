/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#include "executorthread.h"
#include "utility.h"
#include "vb_ready_queue.h"

#include <memcached/vbucket.h>

#include <functional>
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
    void wake(void);
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
    std::function<void()> stepPreSnoozeHook;

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
    std::queue<Vbid> hpVbs;
    VBReadyQueue lpVbs;
    bool doHighPriority;
    size_t numHighPriority;
    std::atomic<bool> pendingMutation;

    KVShard *shard;

    DISALLOW_COPY_AND_ASSIGN(Flusher);
};
