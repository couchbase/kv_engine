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

#include <algorithm>
#include <limits>
#include <queue>
#include <set>
#include <string>
#include <vector>

#include <daemon/tracing.h>
#include <phosphor/phosphor.h>

#include "conn_notifier.h"
#include "conn_store.h"
#include "connhandler.h"
#include "connmap.h"
#include "dcp/backfill-manager.h"
#include "dcp/consumer.h"
#include "dcp/producer.h"
#include "executorpool.h"

/**
 * A task to manage connections.
 */
class ConnManager : public GlobalTask {
public:
    class ConfigChangeListener : public ValueChangedListener {
    public:
        ConfigChangeListener(ConnManager& connManager)
            : connManager(connManager) {
        }

        void sizeValueChanged(const std::string& key, size_t value) override {
            if (key == "connection_manager_interval") {
                connManager.setSnoozeTime(value);
            }
        }

    private:
        ConnManager& connManager;
    };

    ConnManager(EventuallyPersistentEngine* e, ConnMap* cmap)
        : GlobalTask(e,
                     TaskId::ConnManager,
                     e->getConfiguration().getConnectionManagerInterval(),
                     true),
          engine(e),
          connmap(cmap),
          snoozeTime(e->getConfiguration().getConnectionManagerInterval()) {
        engine->getConfiguration().addValueChangedListener(
                "connection_manager_interval",
                std::make_unique<ConfigChangeListener>(*this));
    }

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

    std::string getDescription() {
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

    void setSnoozeTime(size_t snooze) {
        snoozeTime = snooze;
    }

private:
    EventuallyPersistentEngine *engine;
    ConnMap *connmap;
    size_t snoozeTime;
};

ConnMap::ConnMap(EventuallyPersistentEngine& theEngine)
    : engine(theEngine),
      connNotifier_(nullptr),
      connStore(std::make_unique<ConnStore>(theEngine)) {
}

void ConnMap::initialize() {
    connNotifier_ = std::make_shared<ConnNotifier>(*this);
    connNotifier_->start();
    ExTask connMgr = std::make_shared<ConnManager>(&engine, this);
    ExecutorPool::get()->schedule(connMgr);
}

ConnMap::~ConnMap() {
    if (connNotifier_) {
        connNotifier_->stop();
    }
}

void ConnMap::notifyPausedConnection(const std::shared_ptr<ConnHandler>& conn) {
    if (engine.getEpStats().isShutdown) {
        return;
    }

    {
        LockHolder rlh(releaseLock);
        if (conn.get() && conn->isPaused() && conn->isReserved()) {
            engine.notifyIOComplete(conn->getCookie(), ENGINE_SUCCESS);
        }
    }
}

void ConnMap::addConnectionToPending(const std::shared_ptr<ConnHandler>& conn) {
    if (engine.getEpStats().isShutdown) {
        return;
    }

    if (conn.get() && conn->isPaused() && conn->isReserved()) {
        pendingNotifications.push(conn);
        if (connNotifier_) {
            // Wake up the connection notifier so that
            // it can notify the event to a given paused connection.
            connNotifier_->notifyMutationEvent();
        }
    }
}

void ConnMap::processPendingNotifications() {
    std::queue<std::weak_ptr<ConnHandler>> queue;
    pendingNotifications.getAll(queue);

    TRACE_EVENT1("ep-engine/ConnMap",
                 "processPendingNotifications",
                 "#pending",
                 queue.size());

    TRACE_LOCKGUARD_TIMED(releaseLock,
                          "mutex",
                          "ConnMap::processPendingNotifications::releaseLock",
                          SlowMutexThreshold);

    while (!queue.empty()) {
        auto conn = queue.front().lock();
        if (conn && conn->isPaused() && conn->isReserved()) {
            engine.notifyIOComplete(conn->getCookie(), ENGINE_SUCCESS);
        }
        queue.pop();
    }
}

void ConnMap::addVBConnByVBId(std::shared_ptr<ConnHandler> conn, Vbid vbid) {
    connStore->addVBConnByVbid(vbid, conn);
}

void ConnMap::removeVBConnByVBId(const void* connCookie, Vbid vbid) {
    connStore->removeVBConnByVbid(vbid, connCookie);
}

bool ConnMap::vbConnectionExists(ConnHandler* conn, Vbid vbid) {
    return connStore->doesVbConnExist(vbid, conn->getCookie());
}
