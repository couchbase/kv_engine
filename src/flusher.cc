/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include "flusher.h"

#include "common.h"

#include <stdlib.h>

#include <sstream>


bool Flusher::stop(bool isForceShutdown) {
    forceShutdownReceived = isForceShutdown;
    State to = forceShutdownReceived ? State::Stopped : State::Stopping;
    bool ret = transitionState(to);
    wake();
    return ret;
}

void Flusher::wait(void) {
    hrtime_t startt(gethrtime());
    while (_state != State::Stopped) {
        if (!ExecutorPool::get()->wake(taskId)) {
            std::stringstream ss;
            ss << "Flusher::wait: taskId: " << taskId << " has vanished!";
            LOG(EXTENSION_LOG_WARNING, "%s", ss.str().c_str());
            break;
        }
        usleep(1000);
    }
    hrtime_t endt(gethrtime());
    if ((endt - startt) > 1000) {
        LOG(EXTENSION_LOG_NOTICE,
            "Flusher::wait: had to wait %s for shutdown\n",
            hrtime2text(endt - startt).c_str());
    }
}

bool Flusher::pause(void) {
    return transitionState(State::Pausing);
}

bool Flusher::resume(void) {
    bool ret = transitionState(State::Running);
    wake();
    return ret;
}

bool Flusher::validTransition(State to) const {
    // we may go to stopping from all of the stats except stopped
    if (to == State::Stopping) {
        return _state.load() != State::Stopped;
    }

    switch (_state.load()) {
    case State::Initializing:
        return (to == State::Running);
    case State::Running:
        return (to == State::Pausing);
    case State::Pausing:
        return (to == State::Paused || to == State::Running);
    case State::Paused:
        return (to == State::Running);
    case State::Stopping:
        return (to == State::Stopped);
    case State::Stopped:
        return false;
    }
    throw std::logic_error(
            "Flusher::validTransition: called with invalid "
            "_state:" +
            std::to_string(int(_state.load())));
}

const char* Flusher::stateName(State st) const {
    switch (st) {
    case State::Initializing:
        return "initializing";
    case State::Running:
        return "running";
    case State::Pausing:
        return "pausing";
    case State::Paused:
        return "paused";
    case State::Stopping:
        return "stopping";
    case State::Stopped:
        return "stopped";
    }
    throw std::logic_error(
            "Flusher::stateName: called with invalid "
            "state:" +
            std::to_string(int(st)));
}

bool Flusher::transitionState(State to) {
    if (!forceShutdownReceived && !validTransition(to)) {
        LOG(EXTENSION_LOG_WARNING,
            "Flusher::transitionState: invalid transition _state:%s, to:%s",
            stateName(_state),
            stateName(to));
        return false;
    }

    LOG(EXTENSION_LOG_DEBUG,
        "Flusher::transitionState: from %s to %s",
        stateName(_state),
        stateName(to));

    _state = to;
    return true;
}

const char* Flusher::stateName() const {
    return stateName(_state);
}

void Flusher::initialize() {
    LOG(EXTENSION_LOG_DEBUG, "Flusher::initialize: initializing");
    transitionState(State::Running);
}

void Flusher::schedule_UNLOCKED() {
    ExecutorPool* iom = ExecutorPool::get();
    ExTask task = new FlusherTask(ObjectRegistry::getCurrentEngine(),
                                  this,
                                  shard->getId());
    this->setTaskId(task->getId());
    iom->schedule(task, WRITER_TASK_IDX);
}

void Flusher::start() {
    LockHolder lh(taskMutex);
    if (taskId) {
        LOG(EXTENSION_LOG_WARNING,
            "Flusher::start: double start in flusher task id %" PRIu64 ": %s",
            uint64_t(taskId.load()),
            stateName());
        return;
    }
    schedule_UNLOCKED();
}

void Flusher::wake(void) {
    // taskId becomes zero if the flusher were stopped
    if (taskId > 0) {
        ExecutorPool::get()->wake(taskId);
    }
}

bool Flusher::step(GlobalTask *task) {
    State currentState = _state.load();

    switch (currentState) {
    case State::Initializing:
        if (task->getId() != taskId) {
            throw std::invalid_argument("Flusher::step: Argument "
                    "task->getId() (which is" + std::to_string(task->getId()) +
                    ") does not equal member variable taskId (which is" +
                    std::to_string(taskId.load()));
        }
        initialize();
        return true;

    case State::Paused:
    case State::Pausing:
        if (currentState == State::Pausing) {
            transitionState(State::Paused);
        }
        // Indefinitely put task to sleep..
        task->snooze(INT_MAX);
        return true;

    case State::Running:
        flushVB();
        if (_state == State::Running) {
            double tosleep = computeMinSleepTime();
            if (tosleep > 0) {
                task->snooze(tosleep);
            }
        }
        return true;

    case State::Stopping:
        LOG(EXTENSION_LOG_DEBUG,
            "Flusher::step: stopping flusher (write of all dirty items)");
        completeFlush();
        LOG(EXTENSION_LOG_DEBUG, "Flusher::step: stopped");
        transitionState(State::Stopped);
        return false;

    case State::Stopped:
        taskId = 0;
        return false;
    }

    // If we got here there was an unhandled switch case
    throw std::logic_error("Flusher::step: invalid _state:" +
                           std::to_string(int(currentState)));
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

void Flusher::flushVB(void) {
    if (store->isDeleteAllScheduled() && shard->getId() != EP_PRIMARY_SHARD) {
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
            for (auto vbid : shard->getVBucketsSortedByState()) {
                lpVbs.push(vbid);
            }
        }
    }

    if (!doHighPriority && shard->highPriorityCount.load() > 0) {
        for (auto vbid : shard->getVBuckets()) {
            RCPtr<VBucket> vb = store->getVBucket(vbid);
            if (vb && vb->getHighPriorityChkSize() > 0) {
                hpVbs.push(vbid);
            }
        }
        numHighPriority = hpVbs.size();
        if (!hpVbs.empty()) {
            doHighPriority = true;
        }
    }

    if (hpVbs.empty() && lpVbs.empty()) {
        LOG(EXTENSION_LOG_INFO,
            "Flusher::flushVB: Trying to flush but no vbuckets exist");
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
