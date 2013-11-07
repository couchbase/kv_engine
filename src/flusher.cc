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

#include "flusher.h"
#include "iomanager/iomanager.h"

bool Flusher::stop(bool isForceShutdown) {
    forceShutdownReceived = isForceShutdown;
    enum flusher_state to = forceShutdownReceived ? stopped : stopping;
    return transition_state(to);
}

void Flusher::wait(void) {
    hrtime_t startt(gethrtime());
    while (_state != stopped) {
        usleep(1000);
    }
    hrtime_t endt(gethrtime());
    if ((endt - startt) > 1000) {
        LOG(EXTENSION_LOG_WARNING,  "Had to wait %s for shutdown\n",
            hrtime2text(endt - startt).c_str());
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
    assert(st >= initializing && st <= stopped);
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
    //Reschedule the task
    LockHolder lh(taskMutex);
    assert(taskId > 0);
    IOManager::get()->cancel(taskId);
    schedule_UNLOCKED();
    return true;
}

const char * Flusher::stateName() const {
    return stateName(_state);
}

enum flusher_state Flusher::state() const {
    return _state;
}

void Flusher::initialize(size_t tid) {
    assert(taskId == tid);
    LOG(EXTENSION_LOG_DEBUG, "Initializing flusher");
    transition_state(running);
}

void Flusher::schedule_UNLOCKED() {
    IOManager* iom = IOManager::get();
    ExTask task = new FlusherTask(ObjectRegistry::getCurrentEngine(),
                                  this, Priority::FlusherPriority,
                                  shard->getId());
    this->setTaskId(task->getId());
    iom->scheduleTask(task, WRITER_TASK_IDX);
    assert(taskId > 0);
}

void Flusher::start() {
    LockHolder lh(taskMutex);
    schedule_UNLOCKED();
}

void Flusher::wake(void) {
    LockHolder lh(taskMutex);
    assert(taskId > 0);
    IOManager::get()->wake(taskId);
}

bool Flusher::step(size_t tid) {
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
                        IOManager::get()->snooze(tid, tosleep);
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
                LOG(EXTENSION_LOG_DEBUG, "%s", ss.str().c_str());
            }
            completeFlush();
            LOG(EXTENSION_LOG_DEBUG, "Flusher stopped");
            transition_state(stopped);
            return false;
        case stopped:
            IOManager::get()->cancel(taskId);
            return false;
        default:
            LOG(EXTENSION_LOG_WARNING, "Unexpected state in flusher: %s",
                stateName());
            assert(false);
        }
    } catch(std::runtime_error &e) {
        std::stringstream ss;
        ss << "Exception in flusher loop: " << e.what() << std::endl;
        LOG(EXTENSION_LOG_WARNING, "%s", ss.str().c_str());
        assert(false);
    }

    // We should _NEVER_ get here (unless you compile with -DNDEBUG causing
    // the assertions to be removed.. It's a bug, so we should abort and
    // create a coredump
    abort();
}

void Flusher::completeFlush() {
    while(!canSnooze()) {
        doFlush();
    }
}

double Flusher::computeMinSleepTime() {
    if (!canSnooze() || shard->highPriorityCount.load() > 0) {
        minSleepTime = DEFAULT_MIN_SLEEP_TIME;
        return 0;
    }
    minSleepTime *= 2;
    return std::min(minSleepTime, 1.0);
}

void Flusher::doFlush() {
    uint16_t nextVb = getNextVb();
    if (nextVb == NO_VBUCKETS_INSTANTIATED) {
        return;
    }
    if (store->diskFlushAll && shard->getId() != EP_PRIMARY_SHARD) {
        // another shard is doing disk flush
        bool inverse = false;
        pendingMutation.compare_exchange_strong(inverse, true);
        return;
    }

    store->flushVBucket(nextVb);
}

uint16_t Flusher::getNextVb() {
    if (lpVbs.empty()) {
        if (hpVbs.empty()) {
            doHighPriority = false;
        }
        bool inverse = true;
        pendingMutation.compare_exchange_strong(inverse, false);
        std::vector<int> vbs = shard->getVBucketsSortedByState();
        std::vector<int>::iterator itr = vbs.begin();
        for (; itr != vbs.end(); ++itr) {
            lpVbs.push(static_cast<uint16_t>(*itr));
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
        return NO_VBUCKETS_INSTANTIATED;
    } else if (!hpVbs.empty()) {
        uint16_t vbid = hpVbs.front();
        hpVbs.pop();
        return vbid;
    } else {
        if (doHighPriority && --numHighPriority == 0) {
            doHighPriority = false;
        }
        uint16_t vbid = lpVbs.front();
        lpVbs.pop();
        return vbid;
    }
}
