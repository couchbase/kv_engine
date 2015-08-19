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

#include "ep_engine.h"
#include "executorthread.h"
#include "tapconnection.h"
#include "connmap.h"
#include "dcp-consumer.h"
#include "dcp-producer.h"

size_t ConnMap::vbConnLockNum = 32;
const double ConnNotifier::DEFAULT_MIN_STIME = 1.0;

/**
 * NonIO task to free the resource of a tap connection.
 */
class ConnectionReaperCallback : public GlobalTask {
public:
    ConnectionReaperCallback(EventuallyPersistentEngine &e, ConnMap& cm,
                             connection_t &conn)
        : GlobalTask(&e, Priority::TapConnectionReaperPriority),
          connMap(cm), connection(conn) {
        std::stringstream ss;
        ss << "Reaping tap or dcp connection: " << connection->getName();
        descr = ss.str();
    }

    bool run(void) {
        TapProducer *tp = dynamic_cast<TapProducer*>(connection.get());
        if (tp) {
            tp->clearQueues();
            connMap.removeVBConnections(connection);
        }
        return false;
    }

    std::string getDescription() {
        return descr;
    }

private:
    ConnMap &connMap;
    connection_t connection;
    std::string descr;
};

/**
 * A Callback task for Tap connection notifier
 */
class ConnNotifierCallback : public GlobalTask {
public:
    ConnNotifierCallback(EventuallyPersistentEngine *e, ConnNotifier *notifier)
    : GlobalTask(e, Priority::TapConnNotificationPriority),
      connNotifier(notifier) { }

    bool run(void) {
        return connNotifier->notifyConnections();
    }

    std::string getDescription() {
        if (connNotifier->getNotifierType() == TAP_CONN_NOTIFIER) {
            return std::string("TAP connection notifier");
        } else {
            return std::string("DCP connection notifier");
        }
    }

private:
    ConnNotifier *connNotifier;
};

void ConnNotifier::start() {
    bool inverse = false;
    pendingNotification.compare_exchange_strong(inverse, true);
    ExTask connotifyTask = new ConnNotifierCallback(&connMap.getEngine(), this);
    task = ExecutorPool::get()->schedule(connotifyTask, NONIO_TASK_IDX);
    cb_assert(task);
}

void ConnNotifier::stop() {
    bool inverse = true;
    pendingNotification.compare_exchange_strong(inverse, false);
    ExecutorPool::get()->cancel(task);
}

void ConnNotifier::notifyMutationEvent(void) {
    bool inverse = false;
    if (pendingNotification.compare_exchange_strong(inverse, true)) {
        cb_assert(task > 0);
        ExecutorPool::get()->wake(task);
    }
}

void ConnNotifier::wake() {
    ExecutorPool::get()->wake(task);
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
        : GlobalTask(e, Priority::TapConnMgrPriority, MIN_SLEEP_TIME, false),
          engine(e), connmap(cmap) { }

    bool run(void) {
        if (engine->getEpStats().isShutdown) {
            return false;
        }
        connmap->manageConnections();
        snooze(MIN_SLEEP_TIME);
        return true;
    }

    std::string getDescription() {
        return std::string("Connection Manager");
    }

private:
    EventuallyPersistentEngine *engine;
    ConnMap *connmap;
};

class ConnMapValueChangeListener : public ValueChangedListener {
public:
    ConnMapValueChangeListener(TapConnMap &tc)
        : connmap_(tc) {
    }

    virtual void sizeValueChanged(const std::string &key, size_t value) {
        if (key.compare("tap_noop_interval") == 0) {
            connmap_.setNoopInterval(value);
        }
    }

private:
    TapConnMap &connmap_;
};

ConnMap::ConnMap(EventuallyPersistentEngine &theEngine)
    :  engine(theEngine) {

    Configuration &config = engine.getConfiguration();
    vbConnLocks = new SpinLock[vbConnLockNum];
    size_t max_vbs = config.getMaxVbuckets();
    for (size_t i = 0; i < max_vbs; ++i) {
        vbConns.push_back(std::list<connection_t>());
    }
}

void ConnMap::initialize(conn_notifier_type ntype) {
    connNotifier_ = new ConnNotifier(ntype, *this);
    connNotifier_->start();
    ExTask connMgr = new ConnManager(&engine, this);
    ExecutorPool::get()->schedule(connMgr, NONIO_TASK_IDX);
}

ConnMap::~ConnMap() {
    delete [] vbConnLocks;
    connNotifier_->stop();
    delete connNotifier_;
}

connection_t ConnMap::findByName(const std::string &name) {
    LockHolder lh(connsLock);
    return findByName_UNLOCKED(name);
}

connection_t ConnMap::findByName_UNLOCKED(const std::string&name) {
    connection_t rv(NULL);
    std::list<connection_t>::iterator iter;
    for (iter = all.begin(); iter != all.end(); ++iter) {
        if ((*iter)->getName() == name) {
            rv = *iter;
        }
    }
    return rv;
}

void ConnMap::notifyPausedConnection(connection_t conn, bool schedule) {
    if (engine.getEpStats().isShutdown) {
        return;
    }

    Notifiable* tp = dynamic_cast<Notifiable*>(conn.get());
    if (schedule) {
        if (tp && tp->isPaused() && conn->isReserved() &&
            tp->setNotificationScheduled(true)) {
            pendingNotifications.push(conn);
            connNotifier_->notifyMutationEvent(); // Wake up the connection notifier so that
                                                  // it can notify the event to a given
                                                  // paused connection.
        }
    } else {
        LockHolder rlh(releaseLock);
        if (tp && tp->isPaused() && conn->isReserved()) {
            engine.notifyIOComplete(conn->getCookie(), ENGINE_SUCCESS);
            tp->setNotifySent(true);
        }
    }
}

void ConnMap::notifyAllPausedConnections() {
    std::queue<connection_t> queue;
    pendingNotifications.getAll(queue);

    LockHolder rlh(releaseLock);
    while (!queue.empty()) {
        connection_t &conn = queue.front();
        Notifiable *tp = dynamic_cast<Notifiable*>(conn.get());
        if (tp && tp->isPaused() && conn->isReserved()) {
            engine.notifyIOComplete(conn->getCookie(), ENGINE_SUCCESS);
            tp->setNotifySent(true);
        }
        tp->setNotificationScheduled(false);
        queue.pop();
    }
}

bool ConnMap::notificationQueueEmpty() {
    return pendingNotifications.empty();
}

void ConnMap::updateVBConnections(connection_t &conn,
                                        const std::vector<uint16_t> &vbuckets)
{
    Producer *tp = dynamic_cast<Producer*>(conn.get());
    if (!tp) {
        return;
    }

    VBucketFilter new_filter(vbuckets);
    VBucketFilter diff = tp->getVBucketFilter().filter_diff(new_filter);
    const std::set<uint16_t> &vset = diff.getVBSet();

    for (std::set<uint16_t>::const_iterator it = vset.begin(); it != vset.end(); ++it) {
        size_t lock_num = (*it) % vbConnLockNum;
        SpinLockHolder lh (&vbConnLocks[lock_num]);
        // Remove the connection that is no longer for a given vbucket
        if (!tp->vbucketFilter.empty() && tp->vbucketFilter(*it)) {
            std::list<connection_t> &vb_conns = vbConns[*it];
            std::list<connection_t>::iterator itr = vb_conns.begin();
            for (; itr != vb_conns.end(); ++itr) {
                if (conn->getCookie() == (*itr)->getCookie()) {
                    vb_conns.erase(itr);
                    break;
                }
            }
        } else { // Add the connection to the vbucket replicator list.
            std::list<connection_t> &vb_conns = vbConns[*it];
            vb_conns.push_back(conn);
        }
    }
}

void ConnMap::removeVBConnections(connection_t &conn) {
    Producer *tp = dynamic_cast<Producer*>(conn.get());
    if (!tp) {
        return;
    }

    const std::set<uint16_t> &vset = tp->vbucketFilter.getVBSet();
    for (std::set<uint16_t>::const_iterator it = vset.begin(); it != vset.end(); ++it) {
        size_t lock_num = (*it) % vbConnLockNum;
        SpinLockHolder lh (&vbConnLocks[lock_num]);
        std::list<connection_t> &vb_conns = vbConns[*it];
        std::list<connection_t>::iterator itr = vb_conns.begin();
        for (; itr != vb_conns.end(); ++itr) {
            if (conn->getCookie() == (*itr)->getCookie()) {
                vb_conns.erase(itr);
                break;
            }
        }
    }
}

void ConnMap::addVBConnByVBId(connection_t &conn, int16_t vbid) {
    if (!conn.get()) {
        return;
    }

    size_t lock_num = vbid % vbConnLockNum;
    SpinLockHolder lh (&vbConnLocks[lock_num]);
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
    SpinLockHolder lh (&vbConnLocks[lock_num]);
    removeVBConnByVBId_UNLOCKED(conn, vbid);
}


TapConnMap::TapConnMap(EventuallyPersistentEngine &e)
    : ConnMap(e), nextNoop_(0) {

    Configuration &config = engine.getConfiguration();
    noopInterval_ = config.getTapNoopInterval();
    config.addValueChangedListener("tap_noop_interval",
                                   new ConnMapValueChangeListener(*this));
}

TapConsumer *TapConnMap::newConsumer(const void* cookie)
{
    LockHolder lh(connsLock);
    TapConsumer *tc = new TapConsumer(engine, cookie, ConnHandler::getAnonName());
    connection_t tap(tc);
    LOG(EXTENSION_LOG_INFO, "%s created", tap->logHeader());
    all.push_back(tap);
    map_[cookie] = tap;
    return tc;
}

TapProducer *TapConnMap::newProducer(const void* cookie,
                                     const std::string &name,
                                     uint32_t flags,
                                     uint64_t backfillAge,
                                     int tapKeepAlive,
                                     const std::vector<uint16_t> &vbuckets,
                                     const std::map<uint16_t, uint64_t> &lastCheckpointIds)
{
    LockHolder lh(connsLock);
    TapProducer *producer(NULL);

    std::list<connection_t>::iterator iter;
    for (iter = all.begin(); iter != all.end(); ++iter) {
        producer = dynamic_cast<TapProducer*>((*iter).get());
        if (producer && producer->getName() == name) {
            producer->setExpiryTime((rel_time_t)-1);
            producer->reconnected();
            break;
        }
        else {
            producer = NULL;
        }
    }

    if (producer != NULL) {
        const void *old_cookie = producer->getCookie();
        cb_assert(old_cookie);
        map_.erase(old_cookie);

        if (tapKeepAlive == 0 || (producer->mayCompleteDumpOrTakeover() && producer->idle())) {
            LOG(EXTENSION_LOG_INFO,
                "%s keep alive timed out, should be nuked", producer->logHeader());
            producer->setName(ConnHandler::getAnonName());
            producer->setDisconnect(true);
            producer->setConnected(false);
            producer->setPaused(true);
            producer->setExpiryTime(ep_current_time() - 1);
            producer = NULL;
        }
        else {
            LOG(EXTENSION_LOG_INFO, "%s exists... grabbing the channel",
                producer->logHeader());
            // Create the dummy expired producer connection for the old connection cookie.
            // This dummy producer connection will be used for releasing the corresponding
            // memcached connection.

            // dliao: TODO no need to deal with tap or dcp separately here for the dummy?
            TapProducer *n = new TapProducer(engine,
                                             old_cookie,
                                             ConnHandler::getAnonName(),
                                             0);
            n->setDisconnect(true);
            n->setConnected(false);
            n->setPaused(true);
            n->setExpiryTime(ep_current_time() - 1);
            all.push_back(connection_t(n));
        }
    }

    bool reconnect = false;
    if (producer == NULL) {
        producer = new TapProducer(engine, cookie, name, flags);
        LOG(EXTENSION_LOG_INFO, "%s created", producer->logHeader());
        all.push_back(connection_t(producer));
    } else {
        producer->setCookie(cookie);
        producer->setReserved(true);
        producer->setConnected(true);
        producer->setDisconnect(false);
        reconnect = true;
    }
    producer->evaluateFlags();

    connection_t conn(producer);
    updateVBConnections(conn, vbuckets);

    producer->setFlagByteorderSupport((flags & TAP_CONNECT_TAP_FIX_FLAG_BYTEORDER) != 0);
    producer->setBackfillAge(backfillAge, reconnect);
    producer->setVBucketFilter(vbuckets);
    producer->registerCursor(lastCheckpointIds);

    if (reconnect) {
        producer->rollback();
    }

    map_[cookie] = conn;
    engine.storeEngineSpecific(cookie, producer);
    // Clear all previous session stats for this producer.
    clearPrevSessionStats(producer->getName());

    return producer;

}

void TapConnMap::manageConnections() {
    // To avoid connections to be stucked in a bogus state forever, we're going
    // to ping all connections that hasn't tried to walk the tap queue
    // for this amount of time..
    const int maxIdleTime = 5;

    bool addNoop = false;

    rel_time_t now = ep_current_time();
    if (now > nextNoop_ && noopInterval_ != (size_t)-1) {
        addNoop = true;
        nextNoop_ = now + noopInterval_;
    }

    std::list<connection_t> deadClients;

    LockHolder lh(connsLock);
    // We should pause unless we purged some connections or
    // all queues have items.
    getExpiredConnections_UNLOCKED(deadClients);

    // see if I have some channels that I have to signal..
    std::map<const void*, connection_t>::iterator iter;
    for (iter = map_.begin(); iter != map_.end(); ++iter) {
        TapProducer *tp = dynamic_cast<TapProducer*>(iter->second.get());
        if (tp != NULL) {
            if (tp->supportsAck() && (tp->getExpiryTime() < now) && tp->windowIsFull()) {
                LOG(EXTENSION_LOG_WARNING,
                    "%s Expired and ack windows is full. Disconnecting...",
                    tp->logHeader());
                tp->setDisconnect(true);
            } else if (addNoop) {
                tp->setTimeForNoop();
            }
        }
    }

    // Collect the list of connections that need to be signaled.
    std::list<connection_t> toNotify;
    for (iter = map_.begin(); iter != map_.end(); ++iter) {
        TapProducer *tp = dynamic_cast<TapProducer*>(iter->second.get());
        if (tp && (tp->isPaused() || tp->doDisconnect()) && !tp->isSuspended()
            && tp->isReserved()) {
            if (!tp->sentNotify() ||
                (tp->getLastWalkTime() + maxIdleTime < now)) {
                toNotify.push_back(iter->second);
            }
        }
    }

    lh.unlock();

    LockHolder rlh(releaseLock);
    std::list<connection_t>::iterator it;
    for (it = toNotify.begin(); it != toNotify.end(); ++it) {
        TapProducer *tp = dynamic_cast<TapProducer*>((*it).get());
        if (tp && tp->isReserved()) {
            engine.notifyIOComplete(tp->getCookie(), ENGINE_SUCCESS);
            tp->setNotifySent(true);
        }
    }

    // Delete all of the dead clients
    std::list<connection_t>::iterator ii;
    for (ii = deadClients.begin(); ii != deadClients.end(); ++ii) {
        LOG(EXTENSION_LOG_WARNING, "Clean up \"%s\"", (*ii)->getName().c_str());
        (*ii)->releaseReference();
        TapProducer *tp = dynamic_cast<TapProducer*>((*ii).get());
        if (tp) {
            ExTask reapTask = new ConnectionReaperCallback(engine, *this, *ii);
            ExecutorPool::get()->schedule(reapTask, NONIO_TASK_IDX);
        }
    }
}

void TapConnMap::notifyVBConnections(uint16_t vbid)
{
    size_t lock_num = vbid % vbConnLockNum;
    SpinLockHolder lh(&vbConnLocks[lock_num]);

    std::list<connection_t> &conns = vbConns[vbid];
    std::list<connection_t>::iterator it = conns.begin();
    for (; it != conns.end(); ++it) {
        Notifiable *conn = dynamic_cast<Notifiable*>((*it).get());
        if (conn && conn->isPaused() && (*it)->isReserved() &&
            conn->setNotificationScheduled(true)) {
            pendingNotifications.push(*it);
            connNotifier_->notifyMutationEvent();
        }
    }
    lh.unlock();
}

bool TapConnMap::setEvents(const std::string &name, std::list<queued_item> *q) {
    bool found(false);
    LockHolder lh(connsLock);

    connection_t tc = findByName_UNLOCKED(name);
    if (tc.get()) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc.get());
        cb_assert(tp);
        found = true;
        tp->appendQueue(q);
        lh.unlock();
        notifyPausedConnection(tp, false);
    }

    return found;
}

void TapConnMap::incrBackfillRemaining(const std::string &name,
                                       size_t num_backfill_items) {
    LockHolder lh(connsLock);

    connection_t tc = findByName_UNLOCKED(name);
    if (tc.get()) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc.get());
        cb_assert(tp);
        tp->incrBackfillRemaining(num_backfill_items);
    }
}

ssize_t TapConnMap::backfillQueueDepth(const std::string &name) {
    ssize_t rv(-1);
    LockHolder lh(connsLock);

    connection_t tc = findByName_UNLOCKED(name);
    if (tc.get()) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc.get());
        cb_assert(tp);
        rv = tp->getBackfillQueueSize();
    }

    return rv;
}

void TapConnMap::resetReplicaChain() {
    LockHolder lh(connsLock);
    rel_time_t now = ep_current_time();
    std::list<connection_t>::iterator it = all.begin();
    for (; it != all.end(); ++it) {
        connection_t &tc = *it;
        TapProducer *tp = dynamic_cast<TapProducer*>(tc.get());
        if (!(tp && (tp->isConnected() || tp->getExpiryTime() > now))) {
            continue;
        }
        LOG(EXTENSION_LOG_INFO, "%s Reset the replication chain",
            tp->logHeader());
        // Get the list of vbuckets that each TAP producer is replicating
        VBucketFilter vbfilter = tp->getVBucketFilter();
        std::vector<uint16_t> vblist (vbfilter.getVBSet().begin(), vbfilter.getVBSet().end());
        // TAP producer sends INITIAL_VBUCKET_STREAM messages to the destination to reset
        // replica vbuckets, and then backfills items to the destination.
        tp->scheduleBackfill(vblist);
        notifyPausedConnection(tp, true);
    }
}

bool TapConnMap::isBackfillCompleted(std::string &name) {
    LockHolder lh(connsLock);
    connection_t tc = findByName_UNLOCKED(name);
    if (tc.get()) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc.get());
        if (tp) {
            return tp->isBackfillCompleted();
        }
    }
    return false;
}

void TapConnMap::addFlushEvent() {
    LockHolder lh(connsLock);
    std::list<connection_t>::iterator iter;
    for (iter = all.begin(); iter != all.end(); iter++) {
        TapProducer *tp = dynamic_cast<TapProducer*>((*iter).get());
        if (tp && !tp->dumpQueue) {
            tp->flush();
        }
    }
}

void TapConnMap::scheduleBackfill(const std::set<uint16_t> &backfillVBuckets) {
    LockHolder lh(connsLock);
    rel_time_t now = ep_current_time();
    std::list<connection_t>::iterator it = all.begin();
    for (; it != all.end(); ++it) {
        connection_t &tc = *it;
        TapProducer *tp = dynamic_cast<TapProducer*>(tc.get());
        if (!(tp && (tp->isConnected() || tp->getExpiryTime() > now))) {
            continue;
        }

        std::vector<uint16_t> vblist;
        std::set<uint16_t>::const_iterator vb_it = backfillVBuckets.begin();
        for (; vb_it != backfillVBuckets.end(); ++vb_it) {
            if (tp->checkVBucketFilter(*vb_it)) {
                vblist.push_back(*vb_it);
            }
        }
        if (!vblist.empty()) {
            tp->scheduleBackfill(vblist);
            notifyPausedConnection(tp, true);
        }
    }
}

void TapConnMap::loadPrevSessionStats(const std::map<std::string, std::string> &session_stats) {
    LockHolder lh(connsLock);
    std::map<std::string, std::string>::const_iterator it =
        session_stats.find("ep_force_shutdown");

    if (it != session_stats.end()) {
        if (it->second.compare("true") == 0) {
            prevSessionStats.normalShutdown = false;
        }
    } else if (!session_stats.empty()) { // possible crash on the previous session.
        prevSessionStats.normalShutdown = false;
    }

    std::string tap_prefix("eq_tapq:");
    for (it = session_stats.begin(); it != session_stats.end(); ++it) {
        const std::string &stat_name = it->first;
        if (stat_name.substr(0, 8).compare(tap_prefix) == 0) {
            if (stat_name.find("backfill_completed") != std::string::npos ||
                stat_name.find("idle") != std::string::npos) {
                prevSessionStats.stats[stat_name] = it->second;
            }
        }
    }
}

bool TapConnMap::changeVBucketFilter(const std::string &name,
                                     const std::vector<uint16_t> &vbuckets,
                                     const std::map<uint16_t, uint64_t> &checkpoints) {
    bool rv = false;
    LockHolder lh(connsLock);
    connection_t tc = findByName_UNLOCKED(name);
    if (tc.get()) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc.get());
        if (tp && (tp->isConnected() || tp->getExpiryTime() > ep_current_time())) {
            LOG(EXTENSION_LOG_INFO, "%s Change the vbucket filter",
                tp->logHeader());
            updateVBConnections(tc, vbuckets);
            tp->setVBucketFilter(vbuckets, true);
            tp->registerCursor(checkpoints);
            rv = true;
            lh.unlock();
            notifyPausedConnection(tp, true);
        }
    }
    return rv;
}

bool TapConnMap::checkConnectivity(const std::string &name) {
    LockHolder lh(connsLock);
    rel_time_t now = ep_current_time();
    connection_t tc = findByName_UNLOCKED(name);
    if (tc.get()) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc.get());
        if (tp && (tp->isConnected() || tp->getExpiryTime() > now)) {
            return true;
        }
    }
    return false;
}

bool TapConnMap::closeConnectionByName(const std::string &name) {

    LockHolder lh(connsLock);
    return closeConnectionByName_UNLOCKED(name);
}

bool TapConnMap::mapped(connection_t &tc) {
    bool rv = false;
    std::map<const void*, connection_t>::iterator it;
    for (it = map_.begin(); it != map_.end(); ++it) {
        if (it->second.get() == tc.get()) {
            rv = true;
            break;
        }
    }
    return rv;
}

void TapConnMap::shutdownAllConnections() {
    LOG(EXTENSION_LOG_WARNING, "Shutting down tap connections!");

    connNotifier_->stop();

    LockHolder lh(connsLock);
    // We should pause unless we purged some connections or
    // all queues have items.
    if (all.empty()) {
        return;
    }

    LockHolder rlh(releaseLock);
    std::list<connection_t>::iterator ii;
    for (ii = all.begin(); ii != all.end(); ++ii) {
        LOG(EXTENSION_LOG_WARNING, "Clean up \"%s\"", (*ii)->getName().c_str());
        (*ii)->releaseReference();
        TapProducer *tp = dynamic_cast<TapProducer*>((*ii).get());
        if (tp) {
            tp->clearQueues();
        }
    }
    rlh.unlock();

    all.clear();
    map_.clear();
}

void TapConnMap::disconnect(const void *cookie) {
    LockHolder lh(connsLock);

    Configuration& config = engine.getConfiguration();
    int tapKeepAlive = static_cast<int>(config.getTapKeepalive());
    std::map<const void*, connection_t>::iterator iter(map_.find(cookie));
    if (iter != map_.end()) {
        if (iter->second.get()) {
            rel_time_t now = ep_current_time();
            TapConsumer *tc = dynamic_cast<TapConsumer*>(iter->second.get());
            if (tc || iter->second->doDisconnect()) {
                iter->second->setExpiryTime(now - 1);
                LOG(EXTENSION_LOG_WARNING, "%s disconnected",
                    iter->second->logHeader());
            }
            else { // must be producer
                iter->second->setExpiryTime(now + tapKeepAlive);
                LOG(EXTENSION_LOG_WARNING,
                    "%s disconnected, keep alive for %d seconds",
                    iter->second->logHeader(), tapKeepAlive);
            }
            iter->second->setConnected(false);
        }
        else {
            LOG(EXTENSION_LOG_WARNING,
                "Found half-linked tap connection at: %p", cookie);
        }
        map_.erase(iter);
    }
}

bool TapConnMap::isTapConsumerConnected(uint16_t vbucket) {
    LockHolder lh(connsLock);
    std::list<connection_t>::iterator it;
    for(it = all.begin(); it != all.end(); it++) {
        // Return true if a consumer is found to be active
        TapConsumer *tapConsumer = dynamic_cast<TapConsumer*>(it->get());
        if (tapConsumer && tapConsumer->isConnected()) {
            LOG(EXTENSION_LOG_DEBUG, "(vb %d) A TAP consumer "
                "already exists for vbucket: %s",
                vbucket, tapConsumer->logHeader());
            return true;
        }
    }
    return false;
}

bool TapConnMap::closeConnectionByName_UNLOCKED(const std::string &name) {
    bool rv = false;
    connection_t tc = findByName_UNLOCKED(name);
    if (tc.get()) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc.get());
        if (tp) {
            LOG(EXTENSION_LOG_WARNING, "%s Connection is closed by force",
                tp->logHeader());
            removeTapCursors_UNLOCKED(tp);

            tp->setExpiryTime(ep_current_time() - 1);
            tp->setName(ConnHandler::getAnonName());
            tp->setDisconnect(true);
            tp->setPaused(true);
            rv = true;
        }
    }
    return rv;
}

void TapConnMap::getExpiredConnections_UNLOCKED(std::list<connection_t> &deadClients) {
    rel_time_t now = ep_current_time();

    std::list<connection_t>::iterator iter;
    for (iter = all.begin(); iter != all.end();) {
        connection_t &tc = *iter;
        if (tc->isConnected()) {
            ++iter;
            continue;
        }

        TapProducer *tp = dynamic_cast<TapProducer*>(tc.get());

        bool is_dead = false;
        if (tc->getExpiryTime() <= now && !mapped(tc)) {
            if (tp) {
                if (!tp->isSuspended()) {
                    deadClients.push_back(tc);
                    removeTapCursors_UNLOCKED(tp);
                    iter = all.erase(iter);
                    is_dead = true;
                }
            } else {
                deadClients.push_back(tc);
                iter = all.erase(iter);
                is_dead = true;
            }
        }

        if (!is_dead) {
            ++iter;
        }
    }
}

void TapConnMap::removeTapCursors_UNLOCKED(TapProducer *tp) {
    // Remove all the checkpoint cursors belonging to the TAP connection.
    if (tp) {
        const VBucketMap &vbuckets = engine.getEpStore()->getVBuckets();
        size_t numOfVBuckets = vbuckets.getSize();
        // Remove all the cursors belonging to the TAP connection to be purged.
        for (size_t i = 0; i < numOfVBuckets; ++i) {
            cb_assert(i <= std::numeric_limits<uint16_t>::max());
            uint16_t vbid = static_cast<uint16_t>(i);
            RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
            if (!vb) {
                continue;
            }
            if (tp->vbucketFilter(vbid)) {
                LOG(EXTENSION_LOG_INFO,
                    "%s Remove the TAP cursor from vbucket %d",
                    tp->logHeader(), vbid);
                vb->checkpointManager.removeTAPCursor(tp->getName());
            }
        }
    }
}

void CompleteBackfillTapOperation::perform(TapProducer *tc, void *) {
    tc->completeBackfill();
}

void CompleteDiskBackfillTapOperation::perform(TapProducer *tc, void *) {
    tc->completeDiskBackfill();
}

void ScheduleDiskBackfillTapOperation::perform(TapProducer *tc, void *) {
    tc->scheduleDiskBackfill();
}

void CompletedBGFetchTapOperation::perform(TapProducer *tc, Item *arg) {
    if (connToken != tc->getConnectionToken() && !tc->isReconnected()) {
        delete arg;
        return;
    }
    tc->completeBGFetchJob(arg, vbid, implicitEnqueue);
}

bool TAPSessionStats::wasReplicationCompleted(const std::string &name) const {
    bool rv = true;

    std::string backfill_stat(name + ":backfill_completed");
    std::map<std::string, std::string>::const_iterator it = stats.find(backfill_stat);
    if (it != stats.end() && (it->second == "false" || !normalShutdown)) {
        rv = false;
    }
    std::string idle_stat(name + ":idle");
    it = stats.find(idle_stat);
    if (it != stats.end() && (it->second == "false" || !normalShutdown)) {
        rv = false;
    }

    return rv;
}

void TAPSessionStats::clearStats(const std::string &name) {
    std::string backfill_stat(name + ":backfill_completed");
    stats.erase(backfill_stat);
    std::string idle_stat(name + ":idle");
    stats.erase(idle_stat);
}

DcpConnMap::DcpConnMap(EventuallyPersistentEngine &e)
    : ConnMap(e) {

}


DcpConsumer *DcpConnMap::newConsumer(const void* cookie,
                                     const std::string &name)
{
    LockHolder lh(connsLock);

    std::string conn_name("eq_dcpq:");
    conn_name.append(name);

    std::list<connection_t>::iterator iter;
    for (iter = all.begin(); iter != all.end(); ++iter) {
        if ((*iter)->getName() == conn_name) {
            (*iter)->setDisconnect(true);
            all.erase(iter);
            break;
        }
    }

    DcpConsumer *dcp = new DcpConsumer(engine, cookie, conn_name);
    connection_t dc(dcp);
    LOG(EXTENSION_LOG_INFO, "%s Connection created", dc->logHeader());
    all.push_back(dc);
    map_[cookie] = dc;
    return dcp;

}

bool DcpConnMap::isPassiveStreamConnected(uint16_t vbucket) {
    LockHolder lh(connsLock);
    std::list<connection_t>::iterator it;
    for(it = all.begin(); it != all.end(); it++) {
        DcpConsumer* dcpConsumer = dynamic_cast<DcpConsumer*>(it->get());
        if (dcpConsumer && dcpConsumer->isStreamPresent(vbucket)) {
                LOG(EXTENSION_LOG_DEBUG, "(vb %d) A DCP passive stream "
                    "is already exists for the vbucket in connection: %s",
                    vbucket, dcpConsumer->logHeader());
                return true;
        }
    }
    return false;
}

ENGINE_ERROR_CODE DcpConnMap::addPassiveStream(ConnHandler* conn,
                                               uint32_t opaque,
                                               uint16_t vbucket,
                                               uint32_t flags)
{
    cb_assert(conn);

    /* Check if a stream (passive) for the vbucket is already present */
    if (isPassiveStreamConnected(vbucket)) {
        LOG(EXTENSION_LOG_WARNING, "%s (vb %d) Failing to add passive stream, "
            "as one already exists for the vbucket!",
            conn->logHeader(), vbucket);
        return ENGINE_KEY_EEXISTS;
    }

    LockHolder lh(connsLock);
    return conn->addStream(opaque, vbucket, flags);
}

DcpProducer *DcpConnMap::newProducer(const void* cookie,
                                     const std::string &name,
                                     bool notifyOnly)
{
    LockHolder lh(connsLock);

    std::string conn_name("eq_dcpq:");
    conn_name.append(name);

    std::list<connection_t>::iterator iter;
    for (iter = all.begin(); iter != all.end(); ++iter) {
        if ((*iter)->getName() == conn_name) {
            (*iter)->setDisconnect(true);
            all.erase(iter);
            break;
        }
    }

    DcpProducer *dcp = new DcpProducer(engine, cookie, conn_name, notifyOnly);
    LOG(EXTENSION_LOG_INFO, "%s Connection created", dcp->logHeader());
    all.push_back(connection_t(dcp));
    map_[cookie] = dcp;

    return dcp;
}

void DcpConnMap::shutdownAllConnections() {
    LOG(EXTENSION_LOG_WARNING, "Shutting down dcp connections!");

    connNotifier_->stop();

    LockHolder lh(connsLock);
    if (all.empty()) {
        return;
    }

    LockHolder rlh(releaseLock);
    std::list<connection_t>::iterator ii;
    for (ii = all.begin(); ii != all.end(); ++ii) {
        LOG(EXTENSION_LOG_WARNING, "Clean up \"%s\"", (*ii)->getName().c_str());
        (*ii)->releaseReference();
    }
    rlh.unlock();

    closeAllStreams_UNLOCKED();
    all.clear();
    map_.clear();
}

void DcpConnMap::vbucketStateChanged(uint16_t vbucket, vbucket_state_t state) {
    LockHolder lh(connsLock);
    std::map<const void*, connection_t>::iterator itr = map_.begin();
    for (; itr != map_.end(); ++itr) {
        DcpProducer* producer = dynamic_cast<DcpProducer*> (itr->second.get());
        if (producer) {
            producer->vbucketStateChanged(vbucket, state);
        }
    }
}

void DcpConnMap::closeAllStreams_UNLOCKED() {
    std::map<const void*, connection_t>::iterator itr = map_.begin();
    for (; itr != map_.end(); ++itr) {
        DcpProducer* producer = dynamic_cast<DcpProducer*> (itr->second.get());
        if (producer) {
            producer->closeAllStreams();
        } else {
            static_cast<DcpConsumer*>(itr->second.get())->closeAllStreams();
        }
    }
}

void DcpConnMap::disconnect(const void *cookie) {
    LockHolder lh(connsLock);
    disconnect_UNLOCKED(cookie);
}

void DcpConnMap::disconnect_UNLOCKED(const void *cookie) {
    std::list<connection_t>::iterator iter;
    for (iter = all.begin(); iter != all.end(); ++iter) {
        if ((*iter)->getCookie() == cookie) {
            (*iter)->setDisconnect(true);
            all.erase(iter);
            break;
        }
    }

    std::map<const void*, connection_t>::iterator itr(map_.find(cookie));
    if (itr != map_.end()) {
        connection_t conn = itr->second;
        if (conn.get()) {
            LOG(EXTENSION_LOG_INFO, "%s Removing connection",
                conn->logHeader());
            map_.erase(itr);
        }

        DcpProducer* producer = dynamic_cast<DcpProducer*> (conn.get());
        if (producer) {
            producer->closeAllStreams();
        } else {
            static_cast<DcpConsumer*>(conn.get())->closeAllStreams();
        }

        deadConnections.push_back(conn);
    }
}

void DcpConnMap::manageConnections() {
    std::list<connection_t> release;

    LockHolder lh(connsLock);
    while (!deadConnections.empty()) {
        connection_t conn = deadConnections.front();
        release.push_back(conn);
        deadConnections.pop_front();
    }

    const int maxIdleTime = 5;
    rel_time_t now = ep_current_time();

    // Collect the list of connections that need to be signaled.
    std::list<connection_t> toNotify;
    std::map<const void*, connection_t>::iterator iter;
    for (iter = map_.begin(); iter != map_.end(); ++iter) {
        connection_t conn = iter->second;
        Notifiable *tp = dynamic_cast<Notifiable*>(conn.get());
        if (tp && (tp->isPaused() || conn->doDisconnect()) &&
            conn->isReserved()) {
            if (!tp->sentNotify() ||
                (conn->getLastWalkTime() + maxIdleTime < now)) {
                toNotify.push_back(iter->second);
            }
        }
    }

    lh.unlock();

    LockHolder rlh(releaseLock);
    std::list<connection_t>::iterator it;
    for (it = toNotify.begin(); it != toNotify.end(); ++it) {
        Notifiable *tp =
            static_cast<Notifiable*>(static_cast<Producer*>((*it).get()));
        if (tp && (*it)->isReserved()) {
            engine.notifyIOComplete((*it)->getCookie(), ENGINE_SUCCESS);
            tp->setNotifySent(true);
        }
    }

    while (!release.empty()) {
        connection_t conn = release.front();
        conn->releaseReference();
        release.pop_front();
        removeVBConnections(conn);
    }
}

void DcpConnMap::removeVBConnections(connection_t &conn) {
    Producer *tp = dynamic_cast<Producer*>(conn.get());
    if (!tp) {
        return;
    }

    DcpProducer *prod = static_cast<DcpProducer*>(tp);
    std::list<uint16_t> vblist = prod->getVBList();
    std::list<uint16_t>::iterator it = vblist.begin();
    for (; it != vblist.end(); ++it) {
        uint16_t vbid = *it;
        size_t lock_num = vbid % vbConnLockNum;
        SpinLockHolder lh (&vbConnLocks[lock_num]);
        std::list<connection_t> &vb_conns = vbConns[vbid];
        std::list<connection_t>::iterator itr = vb_conns.begin();
        for (; itr != vb_conns.end(); ++itr) {
            if (conn->getCookie() == (*itr)->getCookie()) {
                vb_conns.erase(itr);
                break;
            }
        }
    }
}

void DcpConnMap::notifyVBConnections(uint16_t vbid, uint64_t bySeqno)
{
    size_t lock_num = vbid % vbConnLockNum;
    SpinLockHolder lh(&vbConnLocks[lock_num]);

    std::list<connection_t> &conns = vbConns[vbid];
    std::list<connection_t>::iterator it = conns.begin();
    for (; it != conns.end(); ++it) {
        DcpProducer *conn = static_cast<DcpProducer*>((*it).get());
        conn->notifySeqnoAvailable(vbid, bySeqno);
    }
}
