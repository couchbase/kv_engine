/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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
#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "ep_engine.h"
#include "locks.h"
#include "syncobject.h"
#include "connmap.h"
#include "tapconnection.h"
#include "atomicqueue.h"

// Forward declaration
class ConnNotifier;
class TapConsumer;
class TapProducer;
class Item;
class EventuallyPersistentEngine;

typedef SingleThreadedRCPtr<ConnHandler> connection_t;
/**
 * Base class for operations performed on tap connections.
 *
 * @see TapConnMap::performTapOp
 */
template <typename V>
class TapOperation {
public:
    virtual ~TapOperation() {}
    virtual void perform(TapProducer *tc, V arg) = 0;
};

/**
 * Indicate the tap operation is complete.
 */
class CompleteBackfillTapOperation : public TapOperation<void*> {
public:
    void perform(TapProducer *tc, void* arg);
};

/**
 * Indicate that we are going to schedule a tap disk backfill for a given vbucket.
 */
class ScheduleDiskBackfillTapOperation : public TapOperation<void*> {
public:
    void perform(TapProducer *tc, void* arg);
};

/**
 * Indicate the tap backfill disk stream thing is complete for a given vbucket.
 */
class CompleteDiskBackfillTapOperation : public TapOperation<void*> {
public:
    void perform(TapProducer *tc, void* arg);
};

/**
 * Complete a bg fetch job and give the item to the given tap connection.
 */
class CompletedBGFetchTapOperation : public TapOperation<Item*> {
public:
    CompletedBGFetchTapOperation(hrtime_t token, uint16_t vb, bool ie=false) :
        connToken(token), vbid(vb), implicitEnqueue(ie) {}

    void perform(TapProducer *tc, Item* arg);
private:
    hrtime_t connToken;
    uint16_t vbid;
    bool implicitEnqueue;
};

class TAPSessionStats {
public:
    TAPSessionStats() : normalShutdown(true) {}

    bool wasReplicationCompleted(const std::string &name) const;

    void clearStats(const std::string &name);

    bool normalShutdown;
    std::map<std::string, std::string> stats;
};

class TapConnMap : public ConnMap {

public:

    TapConnMap(EventuallyPersistentEngine &theEngine);

    /**
     * Find or build a tap connection for the given cookie and with
     * the given name.
     */
    TapProducer *newProducer(const void* cookie,
                             const std::string &name,
                             uint32_t flags,
                             uint64_t backfillAge,
                             int tapKeepAlive,
                             const std::vector<uint16_t> &vbuckets,
                             const std::map<uint16_t, uint64_t> &lastCheckpointIds);


    /**
     * Create a new consumer and add it in the list of TapConnections
     * @param e the engine
     * @param c the cookie representing the client
     * @return Pointer to the nw tap connection
     */
    TapConsumer *newConsumer(const void* c);

    void manageConnections();

    /**
     * Notify the paused connections that are responsible for replicating
     * a given vbucket.
     * @param vbid vbucket id
     */
    void notifyVBConnections(uint16_t vbid);

    void resetReplicaChain();

    /**
     * Get the size of the named backfill queue.
     *
     * @return the size, or -1 if we can't find the queue
     */
    ssize_t backfillQueueDepth(const std::string &name);

    void incrBackfillRemaining(const std::string &name,
                               size_t num_backfill_items);

    void shutdownAllConnections();

    void disconnect(const void *cookie);

    void scheduleBackfill(const std::set<uint16_t> &backfillVBuckets);

    bool isBackfillCompleted(std::string &name);

    /**
     * Add an event to all tap connections telling them to flush their
     * items.
     */
    void addFlushEvent();

    /**
     * Change the vbucket filter for a given TAP producer
     * @param name TAP producer name
     * @param vbuckets a new vbucket filter
     * @param checkpoints last closed checkpoint ids for a new vbucket filter
     * @return true if the TAP producer's vbucket filter is changed successfully
     */
    bool changeVBucketFilter(const std::string &name,
                             const std::vector<uint16_t> &vbuckets,
                             const std::map<uint16_t, uint64_t> &checkpoints);

    /**
     * Load TAP-related stats from the previous engine sessions
     *
     * @param session_stats all the stats from the previous engine sessions
     */
    void loadPrevSessionStats(const std::map<std::string, std::string> &session_stats);

    /**
     * Check if the given TAP producer completed the replication before
     * shutdown or crash.
     *
     * @param name TAP producer's name
     * @return true if the replication from the given TAP producer was
     * completed before shutdown or crash.
     */
    bool prevSessionReplicaCompleted(const std::string &name) {
        return prevSessionStats.wasReplicationCompleted(name);
    }

    bool checkConnectivity(const std::string &name);

    bool closeConnectionByName(const std::string &name);

    bool mapped(connection_t &tc);

    /**
     * Perform a TapOperation for a named tap connection while holding
     * appropriate locks.
     *
     * @param name the name of the tap connection to run the op
     * @param tapop the operation to perform
     * @param arg argument for the tap operation
     *
     * @return true if the tap connection was valid and the operation
     *         was performed
     */
    template <typename V>
    bool performOp(const std::string &name, TapOperation<V> &tapop, V arg) {
        bool ret(true);
        LockHolder lh(connsLock);

        connection_t tc = findByName_UNLOCKED(name);
        if (tc.get()) {
            TapProducer *tp = dynamic_cast<TapProducer*>(tc.get());
            if (tp == nullptr) {
                throw std::logic_error(
                        "TapConnMap::performOp: name (which is " + name +
                        ") refers to a connection_t which is not a TapProducer. "
                        "Connection logHeader is '" + tc.get()->logHeader() + "'");
            }
            tapop.perform(tp, arg);
            lh.unlock();
            notifyPausedConnection(tp, false);
        } else {
            ret = false;
        }

        return ret;
    }

    size_t getNoopInterval() const {
        return noopInterval_;
    }

    void setNoopInterval(size_t value) {
        noopInterval_ = value;
        nextNoop_ = 0;
    }

private:

    /**
     * Clear all the session stats for a given TAP producer
     *
     * @param name TAP producer's name
     */
    void clearPrevSessionStats(const std::string &name) {
        prevSessionStats.clearStats(name);
    }

    void getExpiredConnections_UNLOCKED(std::list<connection_t> &deadClients);

    void removeTapCursors_UNLOCKED(TapProducer *tp);

    bool closeConnectionByName_UNLOCKED(const std::string &name);

    TAPSessionStats prevSessionStats;
    size_t noopInterval_;
    size_t nextNoop_;

};
