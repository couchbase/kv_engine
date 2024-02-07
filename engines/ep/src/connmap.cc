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
#include "connmanager.h"
#include "ep_engine.h"
#include "ep_time.h"
#include <executor/executorpool.h>
#include <phosphor/phosphor.h>

ConnManager::ConnManager(EventuallyPersistentEngine& e, ConnMap* cmap)
    : EpTask(e,
             TaskId::ConnManager,
             e.getConfiguration().getConnectionManagerInterval(),
             true),
      snoozeTime(Duration(e.getConfiguration().getConnectionManagerInterval())),
      connmap(cmap) {
    engine->getConfiguration().addValueChangedListener(
            "connection_manager_interval",
            std::make_unique<ConfigChangeListener>(*this));
}

bool ConnManager::run() {
    TRACE_EVENT0("ep-engine/task", "ConnManager");
    connmap->manageConnections();
    snooze(snoozeTime.load().count());
    return !engine->getEpStats().isShutdown || connmap->isConnections() ||
           !connmap->isDeadConnectionsEmpty();
}

void ConnManager::ConfigChangeListener::floatValueChanged(std::string_view key,
                                                          float value) {
    if (key == "connection_manager_interval") {
        connManager.snoozeTime = Duration(value);
    }
}

ConnMap::ConnMap(EventuallyPersistentEngine& theEngine)
    : engine(theEngine),
      connStore(std::make_unique<ConnStore>(theEngine)) {
}

void ConnMap::initialize() {
    ExTask connMgr = std::make_shared<ConnManager>(engine, this);
    ExecutorPool::get()->schedule(connMgr);
}

ConnMap::~ConnMap() = default;

void ConnMap::addVBConnByVBId(ConnHandler& conn, Vbid vbid) {
    connStore->addVBConnByVbid(vbid, conn);
}

void ConnMap::removeVBConnByVBId(CookieIface* connCookie, Vbid vbid) {
    connStore->removeVBConnByVbid(vbid, connCookie);
}

bool ConnMap::vbConnectionExists(ConnHandler* conn, Vbid vbid) {
    return connStore->doesVbConnExist(vbid, conn->getCookie());
}
