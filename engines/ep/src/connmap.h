/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "conn_store_fwd.h"
#include "dcp/dcp-types.h"
#include <folly/concurrency/UnboundedQueue.h>

// Forward declaration
class ConnNotifier;
class EventuallyPersistentEngine;
class Vbid;

/**
 * A collection of dcp connections.
 */
class ConnMap {
public:
    explicit ConnMap(EventuallyPersistentEngine& theEngine);
    virtual ~ConnMap();

    void initialize();

    /**
     * Purge dead connections or identify paused connections that should send
     * NOOP messages to their destinations.
     */
    virtual void manageConnections() = 0;

    /**
     * Returns true if a dead connections list is not maintained,
     * or the list is empty.
     */
    virtual bool isDeadConnectionsEmpty() {
        return true;
    }

    /**
     * Returns true if there are existing connections.
     */
    virtual bool isConnections() = 0;

    /**
     * Adds the given connection to the set of connections associated
     * with the given vbucket.
     *
     * @param conn Connection to add to the set.
     * @param vbid vBucket to add to.
     */
    void addVBConnByVBId(ConnHandler& conn, Vbid vbid);

    void removeVBConnByVBId(const CookieIface* connCookie, Vbid vbid);

    /**
     * Checks (by pointer comparison) whether a ConnHandler is already
     * present in vbConns.
     *
     */
    bool vbConnectionExists(ConnHandler* conn, Vbid vbid);

    /**
     * Notifies the front-end synchronously on this thread that this paused
     * connection should be re-considered for work.
     *
     * @param conn connection to be notified.
     */
    void notifyPausedConnection(const std::shared_ptr<ConnHandler>& conn);

    /**
     * Schedule a notify by adding it to the pendingNotifications queue.
     * It will be processed later by the ConnNotifer (in a separate thread)
     * by the processPendingNotifications method.
     *
     * @param conn connection to be scheduled for notification.
     */
    void addConnectionToPending(const std::shared_ptr<ConnHandler>& conn);

    /**
     * Notifies the front-end for all the connections in the
     * pendingNotifications queue that they should now be re-considered for
     * work.
     */
    void processPendingNotifications();

    EventuallyPersistentEngine& getEngine() {
        return engine;
    }

protected:
    // Synchronises notifying and releasing connections.
    // When we call Connection::release() memcached can free up the connection
    // at any time, so we need to ensure that we notify only valid connections
    // when we call back into memcached by notify_io_complete(cookie).
    std::mutex releaseLock;

    /* Handle to the engine who owns us */
    EventuallyPersistentEngine &engine;

    // Unbounded, multi-producer, multi-consumer(*), non-blocking queue of
    // ConnHandlers to be notified.
    // Initially holds 2^5 (32) elements.
    using PendingNotificationQueue =
            folly::UMPMCQueue<std::weak_ptr<ConnHandler>, false, 5>;
    PendingNotificationQueue pendingNotifications;

    const std::shared_ptr<ConnNotifier> connNotifier;

    // ConnStore is pretty big (the header) so use PIMPL to avoid including it
    // wherever ConnMap is included
    const std::unique_ptr<ConnStore> connStore;
};
