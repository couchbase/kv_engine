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
#include <vector>

#include "common.h"
#include "dispatcher.h"
#include "ep.h"
#include "mutation_log.h"

#define NO_VBUCKETS_INSTANTIATED 0xFFFF

enum flusher_state {
    initializing,
    running,
    pausing,
    paused,
    stopping,
    stopped
};

class Flusher;

const double DEFAULT_MIN_SLEEP_TIME = 0.1;

/**
 * A DispatcherCallback adaptor over Flusher.
 */
class FlusherStepper : public DispatcherCallback {
public:
    FlusherStepper(Flusher* f) : flusher(f) { }
    bool callback(Dispatcher &d, TaskId &t);

    std::string description() {
        return std::string("Running a flusher loop.");
    }

    hrtime_t maxExpectedDuration() {
        // Flusher can take a while, but let's report if it runs for
        // more than ten minutes.
        return 10 * 60 * 1000 * 1000;
    }

private:
    Flusher *flusher;
};

/**
 * Manage persistence of data for an EventuallyPersistentStore.
 */
class Flusher {
public:

    Flusher(EventuallyPersistentStore *st, Dispatcher *d) :
        store(st), _state(initializing), dispatcher(d), minSleepTime(0.1),
        forceShutdownReceived(false), doHighPriority(false),
        numHighPriority(0) { }

    ~Flusher() {
        if (_state != stopped) {
            LOG(EXTENSION_LOG_WARNING, "Flusher being destroyed in state %s",
                stateName(_state));

        }
    }

    bool stop(bool isForceShutdown = false);
    void wait();
    bool pause();
    bool resume();

    void initialize(TaskId &);

    void start(void);
    void wake(void);
    bool step(Dispatcher&, TaskId &);

    enum flusher_state state() const;
    const char * stateName() const;

private:
    bool transition_state(enum flusher_state to);
    void doFlush();
    void completeFlush();
    void schedule_UNLOCKED();
    double computeMinSleepTime();

    const char * stateName(enum flusher_state st) const;

    uint16_t getNextVb();

    EventuallyPersistentStore   *store;
    volatile enum flusher_state  _state;
    Mutex                        taskMutex;
    TaskId                       task;
    Dispatcher                  *dispatcher;

    double                   minSleepTime;
    rel_time_t               flushStart;
    Atomic<bool> forceShutdownReceived;
    std::queue<uint16_t> hpVbs;
    std::queue<uint16_t> lpVbs;
    bool doHighPriority;
    int numHighPriority;

    DISALLOW_COPY_AND_ASSIGN(Flusher);
};

#endif  // SRC_FLUSHER_H_
