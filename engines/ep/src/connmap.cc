/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "connmap.h"
#include "conn_store.h"
#include "connhandler.h"
#include "dcp/backfill-manager.h"
#include "dcp/consumer.h"
#include "dcp/producer.h"
#include <daemon/tracing.h>
#include <executor/executorpool.h>
#include <phosphor/phosphor.h>
#include <limits>
#include <queue>
#include <string>

/**
 * A task to manage connections.
 */
class ConnManager : public GlobalTask {
public:
    class ConfigChangeListener : public ValueChangedListener {
    public:
        explicit ConfigChangeListener(ConnManager& connManager)
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
    bool run() override {
        TRACE_EVENT0("ep-engine/task", "ConnManager");
        connmap->manageConnections();
        snooze(snoozeTime);
        return !engine->getEpStats().isShutdown ||
               connmap->isConnections() ||
               !connmap->isDeadConnectionsEmpty();
    }

    std::string getDescription() const override {
        return "Connection Manager";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
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
      connStore(std::make_unique<ConnStore>(theEngine)) {
}

void ConnMap::initialize() {
    ExTask connMgr = std::make_shared<ConnManager>(&engine, this);
    ExecutorPool::get()->schedule(connMgr);
}

ConnMap::~ConnMap() = default;

void ConnMap::notifyPausedConnection(const std::shared_ptr<ConnHandler>& conn) {
    if (engine.getEpStats().isShutdown) {
        return;
    }

    if (conn.get() && conn->isPaused()) {
        engine.scheduleDcpStep(*conn->getCookie());
    }
}

void ConnMap::addVBConnByVBId(ConnHandler& conn, Vbid vbid) {
    connStore->addVBConnByVbid(vbid, conn);
}

void ConnMap::removeVBConnByVBId(const CookieIface* connCookie, Vbid vbid) {
    connStore->removeVBConnByVbid(vbid, connCookie);
}

bool ConnMap::vbConnectionExists(ConnHandler* conn, Vbid vbid) {
    return connStore->doesVbConnExist(vbid, conn->getCookie());
}
