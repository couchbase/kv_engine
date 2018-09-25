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

#include "bucket_logger.h"
#include "common.h"
#include "ep_bucket.h"
#include "tasks.h"

#include <platform/timeutils.h>

#include <stdlib.h>
#include <sstream>

Flusher::Flusher(EPBucket* st, KVShard* k)
    : store(st),
      _state(State::Initializing),
      taskId(0),
      minSleepTime(0.1),
      forceShutdownReceived(false),
      doHighPriority(false),
      numHighPriority(0),
      pendingMutation(false),
      shard(k) {
}

Flusher::~Flusher() {
    if (_state != State::Stopped) {
        EP_LOG_WARN("Flusher::~Flusher: being destroyed in state {}",
                    stateName(_state));
        stop(true);
    }
}

bool Flusher::stop(bool isForceShutdown) {
    forceShutdownReceived = isForceShutdown;
    State to = forceShutdownReceived ? State::Stopped : State::Stopping;
    bool ret = transitionState(to);
    wake();
    return ret;
}

void Flusher::wait(void) {
    auto startt = ProcessClock::now();
    while (_state != State::Stopped) {
        if (!ExecutorPool::get()->wake(taskId)) {
            EP_LOG_WARN("Flusher::wait: taskId: {} has vanished!", taskId);
            break;
        }
        usleep(1000);
    }
    auto endt = ProcessClock::now();
    if ((endt - startt).count() > 1000) {
        EP_LOG_INFO("Flusher::wait: had to wait {} for shutdown",
                    cb::time2text(endt - startt));
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
        EP_LOG_WARN(
                "Flusher::transitionState: invalid transition _state:{}, to:{}",
                stateName(_state),
                stateName(to));
        return false;
    }

    EP_LOG_DEBUG("Flusher::transitionState: from {} to {}",
                 stateName(_state),
                 stateName(to));

    _state = to;
    return true;
}

const char* Flusher::stateName() const {
    return stateName(_state);
}

void Flusher::initialize() {
    EP_LOG_DEBUG("Flusher::initialize: initializing");
    transitionState(State::Running);
}

void Flusher::schedule_UNLOCKED() {
    ExecutorPool* iom = ExecutorPool::get();
    ExTask task = std::make_shared<FlusherTask>(
            ObjectRegistry::getCurrentEngine(), this, shard->getId());
    this->setTaskId(task->getId());
    iom->schedule(task);
}

void Flusher::start() {
    LockHolder lh(taskMutex);
    if (taskId) {
        EP_LOG_WARN("Flusher::start: double start in flusher task id {}: {}",
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
        EP_LOG_DEBUG(
                "Flusher::step: stopping flusher (write of all dirty items)");
        completeFlush();
        EP_LOG_DEBUG("Flusher::step: stopped");
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

    // If the low-priority vBucket queue is empty, see if there's any
    // pending mutations - and if so re-populate the low pri queue.
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
            VBucketPtr vb = store->getVBucket(vbid);
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
        EP_LOG_DEBUG("Flusher::flushVB: Trying to flush but no vbuckets exist");
        return;
    } else if (!hpVbs.empty()) {
        Vbid vbid = hpVbs.front();
        hpVbs.pop();
        if (store->flushVBucket(vbid).first) {
            // More items still available, add vbid back to pending set.
            hpVbs.push(vbid);
        }
    } else {
        if (doHighPriority && --numHighPriority == 0) {
            doHighPriority = false;
        }
        Vbid vbid = lpVbs.front();
        lpVbs.pop();
        if (store->flushVBucket(vbid).first) {
            // More items still available, add vbid back to pending set.
            lpVbs.push(vbid);
        }
    }
}
