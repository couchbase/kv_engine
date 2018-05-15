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

#pragma once

#include "config.h"

#include "atomic.h"
#include "atomicqueue.h"
#include "dcp/dcp-types.h"

#include <climits>
#include <iterator>
#include <list>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

// Forward declaration
class ConnNotifier;
class EventuallyPersistentEngine;

/**
 * A collection of dcp connections.
 */
class ConnMap {
public:
    ConnMap(EventuallyPersistentEngine &theEngine);
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
     * @param conn Connection to add to the set. Refcount is retained.
     * @param vbid vBucket to add to.
     */
    void addVBConnByVBId(std::shared_ptr<ConnHandler> conn, int16_t vbid);

    void removeVBConnByVBId_UNLOCKED(const void* connCookie, int16_t vbid);

    void removeVBConnByVBId(const void* connCookie, int16_t vbid);

    /**
     * Notify a given paused connection.
     *
     * @param conn connection to be notified. Will retain refcount if
     *        connection needs notifying and schedule is true.
     * @param schedule true if a notification event is pushed into a queue.
     *        Otherwise, directly notify the paused connection.
     */
    void notifyPausedConnection(const std::shared_ptr<ConnHandler>& conn,
                                bool schedule = false);

    void notifyAllPausedConnections();

    EventuallyPersistentEngine& getEngine() {
        return engine;
    }

protected:

    // Synchronises notifying and releasing connections.
    // Guards modifications to std::shared_ptr<ConnHandler> objects in {map_}.
    // See also: {connLock}
    std::mutex                                    releaseLock;

    // Synchonises access to the {map_} members, i.e. adding
    // removing connections.
    // Actual modification of the underlying
    // ConnHandler objects is guarded by {releaseLock}.
    std::mutex                                    connsLock;

    using CookieToConnectionMap =
            std::unordered_map<const void*, std::shared_ptr<ConnHandler>>;
    CookieToConnectionMap map_;

    std::vector<std::mutex> vbConnLocks;
    std::vector<std::list<std::shared_ptr<ConnHandler>>> vbConns;

    /* Handle to the engine who owns us */
    EventuallyPersistentEngine &engine;

    AtomicQueue<std::shared_ptr<ConnHandler>> pendingNotifications;
    std::shared_ptr<ConnNotifier> connNotifier_;

    static size_t vbConnLockNum;
};

/**
 * Connection notifier that wakes up paused connections.
 */
class ConnNotifier : public std::enable_shared_from_this<ConnNotifier> {
public:
    ConnNotifier(ConnMap &cm)
            : connMap(cm),
              task(0),
              pendingNotification(false) {
    }

    void start();

    void stop();

    void notifyMutationEvent();

    bool notifyConnections();

private:
    static const double DEFAULT_MIN_STIME;

    ConnMap &connMap;
    std::atomic<size_t> task;
    std::atomic<bool> pendingNotification;
};
