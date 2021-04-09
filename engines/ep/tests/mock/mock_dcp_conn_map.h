/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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

    bool doesConnHandlerExist(const std::string& name);

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
