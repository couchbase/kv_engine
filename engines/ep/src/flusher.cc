/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "flusher.h"

#include "bucket_logger.h"
#include "ep_bucket.h"
#include "executorpool.h"
#include "tasks.h"

#include <platform/timeutils.h>

#include <chrono>
#include <thread>

Flusher::Flusher(EPBucket* st, KVShard* k)
    : store(st),
      _state(State::Initializing),
      taskId(0),
      forceShutdownReceived(false),
      lpVbs(st->getVBuckets().getSize()),
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

void Flusher::wait() {
    auto startt = std::chrono::steady_clock::now();
    while (_state != State::Stopped) {
        if (!ExecutorPool::get()->wakeAndWait(taskId)) {
            EP_LOG_WARN("Flusher::wait: taskId: {} has vanished!", taskId);
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        ;
    }
    auto endt = std::chrono::steady_clock::now();
    if ((endt - startt).count() > 1000) {
        EP_LOG_INFO("Flusher::wait: had to wait {} for shutdown",
                    cb::time2text(endt - startt));
    }
}

bool Flusher::pause() {
    return transitionState(State::Paused);
}

bool Flusher::resume() {
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
        return (to == State::Running || to == State::Paused);
    case State::Running:
        return (to == State::Paused);
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
    std::lock_guard<std::mutex> lh(taskMutex);
    if (taskId) {
        EP_LOG_WARN("Flusher::start: double start in flusher task id {}: {}",
                    uint64_t(taskId.load()),
                    stateName());
        return;
    }
    schedule_UNLOCKED();
}

void Flusher::wake() {
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
        // Indefinitely put task to sleep..
        task->snooze(INT_MAX);
        return true;

    case State::Running: {
        // Start by putting ourselves back to sleep once step() completes.
        // If a new VB is notified (or a VB is re-notified after it is processed
        // in the loop below) then that will cause the task to be re-awoken.
        task->snooze(INT_MAX);

        auto more = flushVB();

        if (_state == State::Running) {
            /// If there's still work to do for this shard, wake up the Flusher
            /// to run again.
            const bool shouldSnooze = hpVbs.empty() &&
                                      shard->highPriorityCount.load() == 0 &&
                                      !more;

            // Testing hook
            stepPreSnoozeHook();

            if (!shouldSnooze) {
                task->updateWaketime(std::chrono::steady_clock::now());
            }
        }
        return true;
    }
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
    // Flush all of our vBuckets
    while (flushVB()) {
    }
}

bool Flusher::flushVB() {
    if (lpVbs.empty() && hpVbs.empty()) {
        doHighPriority = false;
    }

    // Search for any high priority vBuckets to flush.
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

    // Flush a high priority vBucket if applicable
    if (!hpVbs.empty()) {
        Vbid vbid = hpVbs.front();
        hpVbs.pop();

        const auto res = store->flushVBucket(vbid);

        if (res.moreAvailable == EPBucket::MoreAvailable::Yes) {
            // More items still available, add vbid back to pending set.
            hpVbs.push(vbid);
        }

        // Flushing may move the persistence cursor to a new checkpoint.
        if (res.wakeupCkptRemover == EPBucket::WakeCkptRemover::Yes) {
            store->wakeUpCheckpointRemover();
        }

        // Return false (don't re-wake) if the lpVbs is empty (i.e. nothing to
        // do on our next iteration). If another vBucket joins this queue after
        // then it will wake the task.
        return !lpVbs.empty();
    }

    // Below here we are flushing low priority vBuckets
    if (doHighPriority && --numHighPriority == 0) {
        // Now we flush a number of low priority vBuckets equal to the number of
        // high priority vBuckets that we just flushed (or until we run out)
        doHighPriority = false;
    }

    Vbid vbid;
    if (!lpVbs.popFront(vbid)) {
        // Return no more so we don't rewake the task
        return false;
    }

    const auto res = store->flushVBucket(vbid);

    if (res.moreAvailable == EPBucket::MoreAvailable::Yes) {
        // More items still available, add vbid back to pending set.
        lpVbs.pushUnique(vbid);
    }

    // Flushing may move the persistence cursor to a new checkpoint.
    if (res.wakeupCkptRemover == EPBucket::WakeCkptRemover::Yes) {
        store->wakeUpCheckpointRemover();
    }

    // Return more (as we may have low priority vBuckets to flush)
    return true;
}

size_t Flusher::getHPQueueSize() const {
    return hpVbs.size();
}

size_t Flusher::getLPQueueSize() const {
    return lpVbs.size();
}

size_t Flusher::getHighPriorityCount() const {
    return shard->highPriorityCount.load();
}
