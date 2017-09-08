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

#include <algorithm>
#include <limits>
#include <queue>
#include <set>
#include <string>
#include <vector>

#include <phosphor/phosphor.h>

#include "connhandler.h"
#include "connmap.h"
#include "executorthread.h"
#include "dcp/backfill-manager.h"
#include "dcp/consumer.h"
#include "dcp/producer.h"

size_t ConnMap::vbConnLockNum = 32;
const double ConnNotifier::DEFAULT_MIN_STIME = 1.0;

/**
 * A Callback task for connection notifier
 */
class ConnNotifierCallback : public GlobalTask {
public:
    ConnNotifierCallback(EventuallyPersistentEngine* e, ConnNotifier* notifier)
        : GlobalTask(e, TaskId::ConnNotifierCallback),
          connNotifier(notifier),
          description("DCP connection notifier") {
    }


    bool run(void) {
        return connNotifier->notifyConnections();
    }

    cb::const_char_buffer getDescription() {
        return description;
    }

    std::chrono::microseconds maxExpectedDuration() {
        // In *theory* this should run very quickly (p50 of 64us); however
        // there's evidence it sometimes takes much longer than that - p99.999
        // of over 1s.
        // Set slow limit to 1s initially to highlight the worst runtimes;
        // consider reducing further when they are solved.
        return std::chrono::seconds(1);
    }

private:
    ConnNotifier *connNotifier;
    const cb::const_char_buffer description;
};

void ConnNotifier::start() {
    bool inverse = false;
    pendingNotification.compare_exchange_strong(inverse, true);
    ExTask connotifyTask =
            std::make_shared<ConnNotifierCallback>(&connMap.getEngine(), this);
    task = ExecutorPool::get()->schedule(connotifyTask);
}

void ConnNotifier::stop() {
    bool inverse = true;
    pendingNotification.compare_exchange_strong(inverse, false);
    ExecutorPool::get()->cancel(task);
}

void ConnNotifier::notifyMutationEvent(void) {
    bool inverse = false;
    if (pendingNotification.compare_exchange_strong(inverse, true)) {
        if (task > 0) {
            ExecutorPool::get()->wake(task);
        }
    }
}

bool ConnNotifier::notifyConnections() {
    bool inverse = true;
    pendingNotification.compare_exchange_strong(inverse, false);
    connMap.notifyAllPausedConnections();

    if (!pendingNotification.load()) {
        ExecutorPool::get()->snooze(task, DEFAULT_MIN_STIME);
        if (pendingNotification.load()) {
            // Check again if a new notification is arrived right before
            // calling snooze() above.
            ExecutorPool::get()->snooze(task, 0);
        }
    }

    return true;
}

/**
 * A task to manage connections.
 */
class ConnManager : public GlobalTask {
public:
    ConnManager(EventuallyPersistentEngine *e, ConnMap *cmap)
        : GlobalTask(e, TaskId::ConnManager,
                     e->getConfiguration().getConnectionManagerInterval(),
                     true),
          engine(e), connmap(cmap),
          snoozeTime(e->getConfiguration().getConnectionManagerInterval()) { }

    /**
     * The ConnManager task is used to run the manageConnections function
     * once a second.  This is required for two reasons:
     * (1) To clean-up dead connections
     * (2) To notify idle connections; either for connections that need to be
     * closed or to ensure dcp noop messages are sent once a second.
     */
    bool run(void) {
        TRACE_EVENT0("ep-engine/task", "ConnManager");
        connmap->manageConnections();
        snooze(snoozeTime);
        return !engine->getEpStats().isShutdown ||
               connmap->isConnections() ||
               !connmap->isDeadConnectionsEmpty();
    }

    cb::const_char_buffer getDescription() {
        return "Connection Manager";
    }

    std::chrono::microseconds maxExpectedDuration() {
        // In *theory* this should run very quickly (p50 of <1ms); however
        // there's evidence it sometimes takes much longer than that - p99.99
        // of 10s.
        // Set slow limit to 1s initially to highlight the worst runtimes;
        // consider reducing further when they are solved.
        return std::chrono::seconds(1);
    }

private:
    EventuallyPersistentEngine *engine;
    ConnMap *connmap;
    size_t snoozeTime;
};

ConnMap::ConnMap(EventuallyPersistentEngine &theEngine)
    :  vbConnLocks(vbConnLockNum),
       engine(theEngine),
       connNotifier_(nullptr) {

    Configuration &config = engine.getConfiguration();
    size_t max_vbs = config.getMaxVbuckets();
    for (size_t i = 0; i < max_vbs; ++i) {
        vbConns.push_back(std::list<connection_t>());
    }
}

void ConnMap::initialize() {
    connNotifier_ = new ConnNotifier(*this);
    connNotifier_->start();
    ExTask connMgr = std::make_shared<ConnManager>(&engine, this);
    ExecutorPool::get()->schedule(connMgr);
}

ConnMap::~ConnMap() {
    if (connNotifier_) {
        connNotifier_->stop();
        delete connNotifier_;
    }
}

void ConnMap::notifyPausedConnection(connection_t conn, bool schedule) {
    if (engine.getEpStats().isShutdown) {
        return;
    }

    if (schedule) {
        if (conn.get() && conn->isPaused() && conn->isReserved()) {
            pendingNotifications.push(conn);
            if (connNotifier_) {
                // Wake up the connection notifier so that
                // it can notify the event to a given paused connection.
                connNotifier_->notifyMutationEvent();
            }
        }
    } else {
        LockHolder rlh(releaseLock);
        if (conn.get() && conn->isPaused() && conn->isReserved()) {
            engine.notifyIOComplete(conn->getCookie(), ENGINE_SUCCESS);
        }
    }
}

void ConnMap::notifyAllPausedConnections() {
    std::queue<connection_t> queue;
    pendingNotifications.getAll(queue);

    LockHolder rlh(releaseLock);
    while (!queue.empty()) {
        connection_t &conn = queue.front();
        if (conn.get() && conn->isPaused() && conn->isReserved()) {
            engine.notifyIOComplete(conn->getCookie(), ENGINE_SUCCESS);
        }
        queue.pop();
    }
}

void ConnMap::addVBConnByVBId(connection_t &conn, int16_t vbid) {
    if (!conn.get()) {
        return;
    }

    size_t lock_num = vbid % vbConnLockNum;
    std::lock_guard<SpinLock> lh(vbConnLocks[lock_num]);
    std::list<connection_t> &vb_conns = vbConns[vbid];
    vb_conns.push_back(conn);
}

void ConnMap::removeVBConnByVBId_UNLOCKED(connection_t &conn, int16_t vbid) {
    if (!conn.get()) {
        return;
    }

    std::list<connection_t> &vb_conns = vbConns[vbid];
    std::list<connection_t>::iterator itr = vb_conns.begin();
    for (; itr != vb_conns.end(); ++itr) {
        if (conn->getCookie() == (*itr)->getCookie()) {
            vb_conns.erase(itr);
            break;
        }
    }
}

void ConnMap::removeVBConnByVBId(connection_t &conn, int16_t vbid) {
    size_t lock_num = vbid % vbConnLockNum;
    std::lock_guard<SpinLock> lh(vbConnLocks[lock_num]);
    removeVBConnByVBId_UNLOCKED(conn, vbid);
}
