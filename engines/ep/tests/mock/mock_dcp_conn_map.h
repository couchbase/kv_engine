/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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
#pragma once

#include "conn_notifier.h"
#include "connhandler.h"
#include "dcp/dcpconnmap.h"

/*
 * Mock of the DcpConnMap class.  Wraps the real DcpConnMap, but exposes
 * normally protected methods publically for test purposes.
 */
class MockDcpConnMap : public DcpConnMap {
public:
    explicit MockDcpConnMap(EventuallyPersistentEngine& theEngine)
        : DcpConnMap(theEngine) {
    }

    size_t getNumberOfDeadConnections() {
        return deadConnections.size();
    }

    AtomicQueue<std::weak_ptr<ConnHandler>>& getPendingNotifications() {
        return pendingNotifications;
    }

    void initialize() {
        // The ConnNotifier is created in the base-class ctor and deleted in the
        // base-class dtor.
        // We do not schedule any ConnNotifierCallback task.
        // We do not schedule any ConnManager task.
    }

    void addConn(const void* cookie, std::shared_ptr<ConnHandler> conn);

    bool removeConn(const void* cookie);

    /// return if the named handler exists for the vbid in the vbToConns
    /// structure
    bool doesVbConnExist(Vbid vbid, const std::string& name);

protected:
    /**
     * @param engine The engine
     * @param cookie The cookie that identifies the connection
     * @param connName The name that identifies the connection
     * @return a shared instance of MockDcpConsumer
     */
    std::shared_ptr<DcpConsumer> makeConsumer(
            EventuallyPersistentEngine& engine,
            const void* cookie,
            const std::string& connName,
            const std::string& consumerName) const override;
};
