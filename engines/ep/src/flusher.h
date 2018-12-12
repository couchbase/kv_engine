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

#include "config.h"

#include "executorthread.h"
#include "utility.h"

#include <memcached/vbucket.h>

#include <list>
#include <map>
#include <queue>
#include <string>

#define NO_VBUCKETS_INSTANTIATED 0xFFFF
#define RETRY_FLUSH_VBUCKET (-1)

const double DEFAULT_MIN_SLEEP_TIME = MIN_SLEEP_TIME;
const double DEFAULT_MAX_SLEEP_TIME = 10.0;

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

    void notifyFlushEvent(void) {
        // By setting pendingMutation to true we are guaranteeing that the given
        // flusher will iterate the entire vbuckets under its shard from the
        // begining and flush for all mutations
        bool disable = false;
        if (pendingMutation.compare_exchange_strong(disable, true)) {
            wake();
        }
    }
    void setTaskId(size_t newId) { taskId = newId; }

private:
    enum class State {
        Initializing,
        Running,
        Pausing,
        Paused,
        Stopping,
        Stopped
    };

    bool transitionState(State to);
    bool validTransition(State to) const;
    void flushVB();
    void completeFlush();
    void initialize();
    void schedule_UNLOCKED();
    double computeMinSleepTime();

    const char* stateName(State st) const;

    bool canSnooze(void) {
        return lpVbs.empty() && hpVbs.empty() && !pendingMutation.load();
    }

    EPBucket* store;
    std::atomic<State> _state;

    // Used for serializaling attempts to start the flusher from
    // different threads.
    std::mutex                        taskMutex;
    std::atomic<size_t>      taskId;

    double                   minSleepTime;
    std::atomic<bool> forceShutdownReceived;
    std::queue<Vbid> hpVbs;
    std::queue<Vbid> lpVbs;
    bool doHighPriority;
    size_t numHighPriority;
    std::atomic<bool> pendingMutation;

    KVShard *shard;

    DISALLOW_COPY_AND_ASSIGN(Flusher);
};
