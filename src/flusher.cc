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

#include "config.h"

#include <stdlib.h>

#include <algorithm>
#include <list>
#include <map>
#include <vector>
#include <sstream>

#include "flusher.h"

bool Flusher::stop(bool isForceShutdown) {
    forceShutdownReceived = isForceShutdown;
    enum flusher_state to = forceShutdownReceived ? stopped : stopping;
    bool ret = transition_state(to);
    wake();
    return ret;
}

void Flusher::wait(void) {
    hrtime_t startt(gethrtime());
    while (_state != stopped) {
        if (!ExecutorPool::get()->wake(taskId)) {
            std::stringstream ss;
            ss << "Flusher task " << taskId << " vanished!";
            LOG(EXTENSION_LOG_WARNING, ss.str().c_str());
            break;
        }
        usleep(1000);
    }
    hrtime_t endt(gethrtime());
    if ((endt - startt) > 1000) {
        LOG(EXTENSION_LOG_NOTICE,  "Had to wait %s for shutdown\n",
            hrtime2text(endt - startt).c_str());
    }
}

bool Flusher::pause(void) {
    return transition_state(pausing);
}

bool Flusher::resume(void) {
    bool ret = transition_state(running);
    wake();
    return ret;
}

static bool validTransition(enum flusher_state from,
                            enum flusher_state to)
{
    // we may go to stopping from all of the stats except stopped
    if (to == stopping) {
        return from != stopped;
    }

    switch (from) {
    case initializing:
        return (to == running);
    case running:
        return (to == pausing);
    case pausing:
        return (to == paused || to == running);
    case paused:
        return (to == running);
    case stopping:
        return (to == stopped);
    case stopped:
        return false;
    }
    // THis should be impossible (unless someone added new states)
    abort();
}

const char * Flusher::stateName(enum flusher_state st) const {
    static const char * const stateNames[] = {
        "initializing", "running", "pausing", "paused", "stopping", "stopped"
    };
    cb_assert(st >= initializing && st <= stopped);
    return stateNames[st];
}

bool Flusher::transition_state(enum flusher_state to) {

    LOG(EXTENSION_LOG_DEBUG, "Attempting transition from %s to %s",
        stateName(_state), stateName(to));

    if (!forceShutdownReceived && !validTransition(_state, to)) {
        LOG(EXTENSION_LOG_WARNING, "Invalid transitioning from %s to %s",
            stateName(_state), stateName(to));
        return false;
    }

    LOG(EXTENSION_LOG_DEBUG, "Transitioning from %s to %s",
        stateName(_state), stateName(to));

    _state = to;
    return true;
}

const char * Flusher::stateName() const {
    return stateName(_state);
}

enum flusher_state Flusher::state() const {
    return _state;
}

void Flusher::initialize(size_t tid) {
    cb_assert(taskId == tid);
    LOG(EXTENSION_LOG_DEBUG, "Initializing flusher");
    transition_state(running);
}

void Flusher::schedule_UNLOCKED() {
    ExecutorPool* iom = ExecutorPool::get();
    ExTask task = new FlusherTask(ObjectRegistry::getCurrentEngine(),
                                  this, Priority::FlusherPriority,
                                  shard->getId());
    this->setTaskId(task->getId());
    iom->schedule(task, WRITER_TASK_IDX);
    cb_assert(taskId > 0);
}

void Flusher::start() {
    LockHolder lh(taskMutex);
    if (taskId) {
        LOG(EXTENSION_LOG_WARNING, "Double start in flusher task id %llu: %s",
                taskId, stateName());
        return;
    }
    schedule_UNLOCKED();
}

void Flusher::wake(void) {
    LockHolder lh(taskMutex);
    cb_assert(taskId > 0);
    ExecutorPool::get()->wake(taskId);
}

bool Flusher::step(GlobalTask *task) {
    try {
        switch (_state.load()) {
        case initializing:
            initialize(task->getId());
            return true;
        case paused:
        case pausing:
            if (_state == pausing) {
                transition_state(paused);
            }
            // Indefinitely put task to sleep..
            task->snooze(INT_MAX);
            return true;
        case running:
            {
                flushVB();
                if (_state == running) {
                    double tosleep = computeMinSleepTime();
                    if (tosleep > 0) {
                        store->commit(shard->getId());
                        resetCommitInterval();
                        task->snooze(tosleep);
                    }
                }
                return true;
            }
        case stopping:
            {
                std::stringstream ss;
                ss << "Shutting down flusher (Write of all dirty items)"
                   << std::endl;
                LOG(EXTENSION_LOG_DEBUG, "%s", ss.str().c_str());
            }
            completeFlush();
            store->commit(shard->getId());
            resetCommitInterval();
            LOG(EXTENSION_LOG_DEBUG, "Flusher stopped");
            transition_state(stopped);
        case stopped:
            {
                LockHolder lh(taskMutex);
                taskId = 0;
                return false;
            }
        default:
            LOG(EXTENSION_LOG_WARNING, "Unexpected state in flusher: %s",
                stateName());
            cb_assert(false);
        }
    } catch(std::runtime_error &e) {
        std::stringstream ss;
        ss << "Exception in flusher loop: " << e.what() << std::endl;
        LOG(EXTENSION_LOG_WARNING, "%s", ss.str().c_str());
        cb_assert(false);
    }

    // We should _NEVER_ get here (unless you compile with -DNDEBUG causing
    // the assertions to be removed.. It's a bug, so we should abort and
    // create a coredump
    abort();
}

void Flusher::completeFlush() {
    while(!canSnooze()) {
        flushVB();
    }
}

double Flusher::computeMinSleepTime() {
    if (!canSnooze() || shard->highPriorityCount.load() > 0) {
        minSleepTime = DEFAULT_MIN_SLEEP_TIME;
        return 0;
    }
    minSleepTime *= 2;
    return std::min(minSleepTime, DEFAULT_MAX_SLEEP_TIME);
}

uint16_t Flusher::decrCommitInterval(void) {
    --currCommitInterval;
    //When the current commit interval hits zero, then reset the
    //current commit interval to the initial value
    if (!currCommitInterval) {
        currCommitInterval = initCommitInterval;
        return 0;
    }

    return currCommitInterval;
}

void Flusher::flushVB(void) {
    if (store->diskFlushAll && shard->getId() != EP_PRIMARY_SHARD) {
        // another shard is doing disk flush
        bool inverse = false;
        pendingMutation.compare_exchange_strong(inverse, true);
        return;
    }

    if (lpVbs.empty()) {
        if (hpVbs.empty()) {
            doHighPriority = false;
        }
        bool inverse = true;
        if (pendingMutation.compare_exchange_strong(inverse, false)) {
            std::vector<int> vbs = shard->getVBucketsSortedByState();
            std::vector<int>::iterator itr = vbs.begin();
            for (; itr != vbs.end(); ++itr) {
                lpVbs.push(static_cast<uint16_t>(*itr));
            }
        }
    }

    if (!doHighPriority && shard->highPriorityCount.load() > 0) {
        std::vector<int> vbs = shard->getVBuckets();
        std::vector<int>::iterator itr = vbs.begin();
        for (; itr != vbs.end(); ++itr) {
            RCPtr<VBucket> vb = store->getVBucket(*itr);
            if (vb && vb->getHighPriorityChkSize() > 0) {
                hpVbs.push(static_cast<uint16_t>(*itr));
            }
        }
        numHighPriority = hpVbs.size();
        if (!hpVbs.empty()) {
            doHighPriority = true;
        }
    }

    if (hpVbs.empty() && lpVbs.empty()) {
        LOG(EXTENSION_LOG_INFO, "Trying to flush but no vbucket exist");
        return;
    } else if (!hpVbs.empty()) {
        uint16_t vbid = hpVbs.front();
        hpVbs.pop();
        if (store->flushVBucket(vbid) == RETRY_FLUSH_VBUCKET) {
            hpVbs.push(vbid);
        }
    } else {
        if (doHighPriority && --numHighPriority == 0) {
            doHighPriority = false;
        }
        uint16_t vbid = lpVbs.front();
        lpVbs.pop();
        if (store->flushVBucket(vbid) == RETRY_FLUSH_VBUCKET) {
            lpVbs.push(vbid);
        }
    }
}
