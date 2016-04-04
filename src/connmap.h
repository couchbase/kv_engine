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

#include <climits>
#include <iterator>
#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "ep_engine.h"
#include "locks.h"
#include "syncobject.h"
#include "tapconnection.h"
#include "atomicqueue.h"
#include "dcp/consumer.h"
#include "dcp/producer.h"

// Forward declaration
class ConnNotifier;
class EventuallyPersistentEngine;

typedef SingleThreadedRCPtr<ConnHandler> connection_t;

/**
 * Connection notifier type.
 */
enum conn_notifier_type {
    TAP_CONN_NOTIFIER, //!< TAP connection notifier
    DCP_CONN_NOTIFIER  //!< DCP connection notifier
};

/**
 * A collection of tap or dcp connections.
 */
class ConnMap {
public:
    ConnMap(EventuallyPersistentEngine &theEngine);
    virtual ~ConnMap();

    void initialize(conn_notifier_type ntype);

    Consumer *newConsumer(const void* c);

    /**
     * Disconnect a connection by its cookie.
     */
    virtual void disconnect(const void *cookie) = 0;

    /**
     * Call a function on each connection.
     */
    template <typename Fun>
    void each(Fun f) {
        LockHolder lh(connsLock);
        each_UNLOCKED(f);
    }

    /**
     * Call a function on each connection *without* a lock.
     */
    template <typename Fun>
    void each_UNLOCKED(Fun f) {
        std::for_each(all.begin(), all.end(), f);
    }

    /**
     * Return the number of connections for which this predicate is true.
     */
    template <typename Fun>
    size_t count_if(Fun f) {
        LockHolder lh(connsLock);
        return count_if_UNLOCKED(f);
    }

    /**
     * Return the number of connections for which this predicate is
     * true *without* a lock.
     */
    template <typename Fun>
    size_t count_if_UNLOCKED(Fun f) {
        return static_cast<size_t>(std::count_if(all.begin(), all.end(), f));
    }

    /**
     * Purge dead connections or identify paused connections that should send
     * NOOP messages to their destinations.
     */
    virtual void manageConnections() = 0;

    connection_t findByName(const std::string &name);

    virtual void shutdownAllConnections() = 0;

    virtual bool isDeadConnectionsEmpty() {
        return true;
    }

    bool isAllEmpty() {
        LockHolder lh(connsLock);
        return all.empty();
    }

    void updateVBConnections(connection_t &conn,
                             const std::vector<uint16_t> &vbuckets);

    virtual void removeVBConnections(connection_t &conn);

    void addVBConnByVBId(connection_t &conn, int16_t vbid);

    void removeVBConnByVBId_UNLOCKED(connection_t &conn, int16_t vbid);

    void removeVBConnByVBId(connection_t &conn, int16_t vbid);

    /**
     * Notify a given paused connection.
     *
     * @param tc connection to be notified
     * @param schedule true if a notification event is pushed into a queue.
     *        Otherwise, directly notify the paused connection.
     */
    void notifyPausedConnection(connection_t conn, bool schedule = false);

    void notifyAllPausedConnections();
    bool notificationQueueEmpty();

    EventuallyPersistentEngine& getEngine() {
        return engine;
    }

protected:

    connection_t findByName_UNLOCKED(const std::string &name);

    // Synchronises notifying and releasing connections.
    // Guards modifications to connection_t objects in {map_} / {all}.
    // See also: {connLock}
    Mutex                                    releaseLock;

    // Synchonises access to the {map_} and {all} members, i.e. adding
    // removing connections.
    // Actual modification of the underlying
    // ConnHandler objects is guarded by {releaseLock}.
    Mutex                                    connsLock;

    std::map<const void*, connection_t>      map_;
    std::list<connection_t>                  all;

    SpinLock *vbConnLocks;
    std::vector<std::list<connection_t> > vbConns;

    /* Handle to the engine who owns us */
    EventuallyPersistentEngine &engine;

    AtomicQueue<connection_t> pendingNotifications;
    ConnNotifier *connNotifier_;

    static size_t vbConnLockNum;
};

/**
 * Connection notifier that wakes up paused connections.
 */
class ConnNotifier {
public:
    ConnNotifier(conn_notifier_type ntype, ConnMap &cm)
        : notifier_type(ntype), connMap(cm), task(0),
          pendingNotification(false)  { }

    void start();

    void stop();

    void wake();

    void notifyMutationEvent();

    bool notifyConnections();

    conn_notifier_type getNotifierType() const {
        return notifier_type;
    }

private:
    static const double DEFAULT_MIN_STIME;

    conn_notifier_type notifier_type;
    ConnMap &connMap;
    AtomicValue<size_t> task;
    AtomicValue<bool> pendingNotification;
};
