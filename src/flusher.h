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

#ifndef SRC_FLUSHER_H_
#define SRC_FLUSHER_H_ 1

#include "config.h"

#include <list>
#include <map>
#include <queue>
#include <string>

#include "kv_bucket.h"
#include "executorthread.h"
#include "utility.h"

#define NO_VBUCKETS_INSTANTIATED 0xFFFF
#define RETRY_FLUSH_VBUCKET (-1)

enum flusher_state {
    initializing,
    running,
    pausing,
    paused,
    stopping,
    stopped
};

class Flusher;

const double DEFAULT_MIN_SLEEP_TIME = MIN_SLEEP_TIME;
const double DEFAULT_MAX_SLEEP_TIME = 10.0;

class KVShard;

/**
 * Manage persistence of data for an EPBucket.
 */
class Flusher {
public:

    Flusher(KVBucket* st, KVShard* k, uint16_t commitInt) :
        store(st), _state(initializing), taskId(0), minSleepTime(0.1),
        initCommitInterval(commitInt), currCommitInterval(commitInt),
        forceShutdownReceived(false), doHighPriority(false), numHighPriority(0),
        pendingMutation(false), shard(k) { }

    ~Flusher() {
        if (_state != stopped) {
            LOG(EXTENSION_LOG_WARNING, "Flusher being destroyed in state %s",
                stateName(_state));
            stop(true);
        }
    }

    bool stop(bool isForceShutdown = false);
    void wait();
    bool pause();
    bool resume();
    void start();
    void wake(void);
    bool step(GlobalTask *task);

    enum flusher_state state() const;
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

    uint16_t getCommitInterval(void) {
        return currCommitInterval;
    }

    uint16_t decrCommitInterval(void);

    void resetCommitInterval(void) {
        currCommitInterval = initCommitInterval;
    }

private:
    bool transition_state(enum flusher_state to);
    void flushVB();
    void completeFlush();
    void initialize();
    void schedule_UNLOCKED();
    double computeMinSleepTime();

    const char * stateName(enum flusher_state st) const;

    bool canSnooze(void) {
        return lpVbs.empty() && hpVbs.empty() && !pendingMutation.load();
    }

    KVBucket* store;
    std::atomic<enum flusher_state> _state;

    // Used for serializaling attempts to start the flusher from
    // different threads.
    std::mutex                        taskMutex;
    std::atomic<size_t>      taskId;

    double                   minSleepTime;
    uint16_t                 initCommitInterval;
    uint16_t                 currCommitInterval;
    std::atomic<bool> forceShutdownReceived;
    std::queue<uint16_t> hpVbs;
    std::queue<uint16_t> lpVbs;
    bool doHighPriority;
    size_t numHighPriority;
    std::atomic<bool> pendingMutation;

    KVShard *shard;

    DISALLOW_COPY_AND_ASSIGN(Flusher);
};

#endif  // SRC_FLUSHER_H_
