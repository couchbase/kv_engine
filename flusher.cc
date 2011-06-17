/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 NorthScale, Inc.
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
#include "config.h"
#include <stdlib.h>

#include "flusher.hh"

bool FlusherStepper::callback(Dispatcher &d, TaskId t) {
    return flusher->step(d, t);
}

bool Flusher::stop(bool isForceShutdown) {
    forceShutdownReceived = isForceShutdown;
    enum flusher_state to = forceShutdownReceived ? stopped : stopping;
    return transition_state(to);
}


static const char* hrtime2text(hrtime_t t, char *buffer, size_t size) {
    static const char * const extensions[] = {"s", "ms", "us", "ns"}; //TODO: get a greek Mu in here correctly
    int id = sizeof(extensions)/sizeof(extensions[0]) - 1;

    while (t > 9999 && id > 0) {
        id--;
        t /= 1000;
    }

    if (id == 0 && t > 599) {
        int hour = static_cast<int>(t / 3600);
        t -= hour * 3600;
        int min = static_cast<int>(t / 60);
        t -= min * 60;

        snprintf(buffer, size, "%dh:%dm:%ds", hour, min, (int)t);
    } else {
        snprintf(buffer, size, "%d %s", (int) t, extensions[id]);
    }

    return buffer;
}

void Flusher::wait(void) {
    hrtime_t startt(gethrtime());
    while (_state != stopped) {
        usleep(1000);
    }
    hrtime_t endt(gethrtime());
    char buffer[80];
    if ((endt - startt) > 1000) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Had to wait %s for shutdown\n",
                         hrtime2text(endt - startt, buffer, sizeof(buffer)));
    }
}

bool Flusher::pause(void) {
    return transition_state(pausing);
}

bool Flusher::resume(void) {
    return transition_state(running);
}

static bool validTransition(enum flusher_state from,
                            enum flusher_state to)
{
    bool rv(true);
    if (from == initializing && to == running) {
    } else if (from == initializing && to == stopping) {
    } else if (from == running && to == pausing) {
    } else if (from == running && to == stopping) {
    } else if (from == pausing && to == paused) {
    } else if (from == stopping && to == stopped) {
    } else if (from == paused && to == running) {
    } else if (from == paused && to == stopping) {
    } else if (from == pausing && to == stopping) {
    } else {
        rv = false;
    }
    return rv;
}

const char * Flusher::stateName(enum flusher_state st) const {
    static const char * const stateNames[] = {
        "initializing", "running", "pausing", "paused", "stopping", "stopped"
    };
    assert(st >= initializing && st <= stopped);
    return stateNames[st];
}

bool Flusher::transition_state(enum flusher_state to) {

    getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                     "Attempting transition from %s to %s\n",
                     stateName(_state), stateName(to));

    if (!forceShutdownReceived && !validTransition(_state, to)) {
        return false;
    }

    getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Transitioning from %s to %s\n",
                     stateName(_state), stateName(to));

    _state = to;
    //Reschedule the task
    LockHolder lh(taskMutex);
    assert(task.get());
    dispatcher->cancel(task);
    schedule_UNLOCKED();
    return true;
}

const char * Flusher::stateName() const {
    return stateName(_state);
}

enum flusher_state Flusher::state() const {
    return _state;
}

void Flusher::initialize(TaskId tid) {
    assert(task.get() == tid.get());
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                     "Initializing flusher; warming up\n");

    hrtime_t startTime = gethrtime();
    vbStateLoaded = false;
    store->warmup(vbStateLoaded);
    store->stats.warmupTime.set((gethrtime() - startTime) / 1000);
    store->stats.warmupComplete.set(true);

    getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                     "Warmup completed in %ds\n", store->stats.warmupTime.get());
    transition_state(running);
}

void Flusher::schedule_UNLOCKED() {
    dispatcher->schedule(shared_ptr<FlusherStepper>(new FlusherStepper(this)),
                         &task, Priority::FlusherPriority);
    assert(task.get());
}

void Flusher::start(void) {
    LockHolder lh(taskMutex);
    schedule_UNLOCKED();
}

void Flusher::wake(void) {
    LockHolder lh(taskMutex);
    assert(task.get());
    dispatcher->wake(task, &task);
}

bool Flusher::step(Dispatcher &d, TaskId tid) {
    try {
        switch (_state) {
        case initializing:
            initialize(tid);
            return true;
        case paused:
            return false;
        case pausing:
            transition_state(paused);
            return false;
        case running:
            {
                doFlush();
                if (_state == running) {
                    double tosleep = computeMinSleepTime();
                    if (tosleep > 0) {
                        d.snooze(tid, tosleep);
                    }
                    return true;
                } else {
                    return false;
                }
            }
        case stopping:
            {
                std::stringstream ss;
                ss << "Shutting down flusher (Write of all dirty items)"
                   << std::endl;
                getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "%s",
                                 ss.str().c_str());
            }
            store->stats.min_data_age = 0;
            completeFlush();
            getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Flusher stopped\n");
            transition_state(stopped);
            return false;
        case stopped:
            return false;
        default:
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                "Unexpected state in flusher: %s", stateName());
            assert(false);
        }
    } catch(std::runtime_error &e) {
        std::stringstream ss;
        ss << "Exception in flusher loop: " << e.what() << std::endl;
        getLogger()->log(EXTENSION_LOG_WARNING, NULL, "%s",
                         ss.str().c_str());
        assert(false);
    }

    // We should _NEVER_ get here (unless you compile with -DNDEBUG causing
    // the assertions to be removed.. It's a bug, so we should abort and
    // create a coredump
    abort();
}

void Flusher::completeFlush() {
    doFlush();
    while (flushQueue) {
        doFlush();
    }
}

double Flusher::computeMinSleepTime() {
    // If we were preempted, keep going!
    if (flushQueue && !flushQueue->empty()) {
        flushRv = 0;
        prevFlushRv = 0;
        return 0.0;
    }

    if (flushRv + prevFlushRv == 0) {
        minSleepTime = std::min(minSleepTime * 2, 1.0);
    } else {
        minSleepTime = DEFAULT_MIN_SLEEP_TIME;
    }
    prevFlushRv = flushRv;
    return std::max(static_cast<double>(flushRv), minSleepTime);
}

int Flusher::doFlush() {

    // On a fresh entry, flushQueue is null and we need to build one.
    if (!flushQueue) {
        flushRv = store->stats.min_data_age;
        flushQueue = store->beginFlush();
        if (flushQueue) {
            getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                             "Beginning a write queue flush.\n");
            rejectQueue = new std::queue<queued_item>();
            flushStart = ep_current_time();
        }
    }

    // Now do the every pass thing.
    if (flushQueue) {
        if (!flushQueue->empty()) {
            int n = store->flushSome(flushQueue, rejectQueue);
            if (_state == pausing) {
                transition_state(paused);
            }
            flushRv = std::min(n, flushRv);
        }

        if (flushQueue->empty()) {
            if (!rejectQueue->empty()) {
                // Requeue the rejects.
                store->requeueRejectedItems(rejectQueue);
            } else {
                store->completeFlush(flushStart);
                getLogger()->log(EXTENSION_LOG_INFO, NULL,
                                 "Completed a flush, age of oldest item was %ds\n",
                                 flushRv);

                delete rejectQueue;
                rejectQueue = NULL;
                flushQueue = NULL;
            }
        }
    }

    return flushRv;
}
