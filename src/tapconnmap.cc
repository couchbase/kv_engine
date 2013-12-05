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
#include "tapconnection.h"
#include "tapconnmap.h"

size_t ConnMap::vbConnLockNum = 32;
const double ConnNotifier::DEFAULT_MIN_STIME = 0.001;

/**
 * NonIO task to free the resource of a tap connection.
 */
class ConnectionReaperCallback : public GlobalTask {
public:
    ConnectionReaperCallback(EventuallyPersistentEngine &e, connection_t &conn)
        : GlobalTask(&e, Priority::TapConnectionReaperPriority),
          engine(e), connection(conn) {
        std::stringstream ss;
        ss << "Reaping tap or upr connection: " << connection->getName();
        descr = ss.str();
    }

    bool run(void) {
        Producer *tp = dynamic_cast<Producer*>(connection.get());
        if (tp) {
            tp->clearQueues();
            engine.getTapConnMap().removeVBTapConnections(connection);
            //            engine.getUprConnMap().removeVBTapConnections(connection);
        }
        return false;
    }

    std::string getDescription() {
        return descr;
    }

private:
    EventuallyPersistentEngine &engine;
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
      tapNotifier(notifier) { }

    bool run(void) {
        return tapNotifier->notify();
    }

    std::string getDescription() {
        return std::string("Tap connection notifier");
    }

private:
    ConnNotifier *tapNotifier;
};

void ConnNotifier::start() {
    ExTask connotifyTask = new ConnNotifierCallback(&engine, this);
    task = ExecutorPool::get()->schedule(connotifyTask, NONIO_TASK_IDX);
    assert(task);
}

void ConnNotifier::stop() {
    ExecutorPool::get()->cancel(task);
}

bool ConnNotifier::notify() {
    engine.getTapConnMap().notifyAllPausedConnections();
    //    engine.getUprConnMap().notifyAllPausedConnections();

    if (engine.getTapConnMap().notificationQueueEmpty() /*&&
                                                          engine.getUprConnMap().notificationQueueEmpty()*/) {

        ExecutorPool::get()->snooze(task, minSleepTime);
        if (minSleepTime == 1.0) {
            minSleepTime = DEFAULT_MIN_STIME;
        } else {
            minSleepTime = std::min(minSleepTime * 2, 1.0);
        }
    } else {
        // We don't sleep, but instead reset the sleep time to the default value.
        minSleepTime = DEFAULT_MIN_STIME;
    }

    return true;
}

class ConnMapValueChangeListener : public ValueChangedListener {
public:
    ConnMapValueChangeListener(ConnMap &tc)
        : connmap_(tc) {
    }

    virtual void sizeValueChanged(const std::string &key, size_t value) {
        if (key.compare("tap_noop_interval") == 0) {
            connmap_.setNoopInterval(value);
        }
    }

private:
    ConnMap &connmap_;
};

ConnMap::ConnMap(EventuallyPersistentEngine &theEngine) :
    notifyCounter(0), engine(theEngine), nextNoop_(0)
{
}

void ConnMap::initialize() {
    connNotifier_ = new ConnNotifier(engine);
    connNotifier_->start();
}

ConnMap::~ConnMap() {
    delete [] vbConnLocks;
    connNotifier_->stop();
    delete connNotifier_;
}

bool ConnMap::setEvents(const std::string &name,
                           std::list<queued_item> *q) {
    bool found(false);
    LockHolder lh(notifySync);

    connection_t tc = findByName_UNLOCKED(name);
    if (tc.get()) {
        Producer *tp = dynamic_cast<Producer*>(tc.get());
        assert(tp);
        found = true;
        tp->appendQueue(q);
        lh.unlock();
        notifyPausedConnection(tp);
    }

    return found;
}

ssize_t ConnMap::backfillQueueDepth(const std::string &name) {
    ssize_t rv(-1);
    LockHolder lh(notifySync);

    connection_t tc = findByName_UNLOCKED(name);
    if (tc.get()) {
        Producer *tp = dynamic_cast<Producer*>(tc.get());
        assert(tp);
        rv = tp->getBackfillQueueSize();
    }

    return rv;
}

connection_t ConnMap::findByName(const std::string &name) {
    LockHolder lh(notifySync);
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

void ConnMap::getExpiredConnections_UNLOCKED(std::list<connection_t> &deadClients) {
    rel_time_t now = ep_current_time();

    std::list<connection_t>::iterator iter;
    for (iter = all.begin(); iter != all.end();) {
        connection_t &tc = *iter;
        if (tc->isConnected()) {
            ++iter;
            continue;
        }

        Producer *tp = dynamic_cast<Producer*>(tc.get());

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

void ConnMap::removeTapCursors_UNLOCKED(Producer *tp) {
    // Remove all the checkpoint cursors belonging to the TAP connection.
    if (tp) {
        const VBucketMap &vbuckets = engine.getEpStore()->getVBuckets();
        size_t numOfVBuckets = vbuckets.getSize();
        // Remove all the cursors belonging to the TAP connection to be purged.
        for (size_t i = 0; i < numOfVBuckets; ++i) {
            assert(i <= std::numeric_limits<uint16_t>::max());
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

void ConnMap::addFlushEvent() {
    LockHolder lh(notifySync);
    std::list<connection_t>::iterator iter;
    for (iter = all.begin(); iter != all.end(); iter++) {
        Producer *tp = dynamic_cast<Producer*>((*iter).get());
        if (tp && !tp->dumpQueue) {
            tp->flush();
        }
    }
}

void ConnMap::notifyVBConnections(uint16_t vbid)
{
    size_t lock_num = vbid % vbConnLockNum;
    SpinLockHolder lh(&vbConnLocks[lock_num]);

    std::list<connection_t> &conns = vbConns[vbid];
    std::list<connection_t>::iterator it = conns.begin();
    for (; it != conns.end(); ++it) {
        Producer *conn = dynamic_cast<Producer*>((*it).get());
        if (conn && conn->isPaused() && conn->isReserved() &&
            conn->setNotificationScheduled(true)) {
            pendingTapNotifications.push(*it);
        }
    }
    lh.unlock();
}

bool ConnMap::mapped(connection_t &tc) {
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

void ConnMap::notifyPausedConnection(Producer *tp) {
    LockHolder rlh(releaseLock);
    if (tp && tp->isPaused() && tp->isReserved()) {
        engine.notifyIOComplete(tp->getCookie(), ENGINE_SUCCESS);
        tp->setNotifySent(true);
    }
}

void ConnMap::notifyAllPausedConnections() {
    std::queue<connection_t> queue;
    pendingTapNotifications.getAll(queue);

    LockHolder rlh(releaseLock);
    while (!queue.empty()) {
        connection_t &conn = queue.front();
        Producer *tp = dynamic_cast<Producer*>(conn.get());
        if (tp && tp->isPaused() && tp->isReserved()) {
            engine.notifyIOComplete(tp->getCookie(), ENGINE_SUCCESS);
            tp->setNotifySent(true);
        }
        tp->setNotificationScheduled(false);
        queue.pop();
    }
}

bool ConnMap::notificationQueueEmpty() {
    return pendingTapNotifications.empty();
}

void ConnMap::shutdownAllTapConnections() {
    LOG(EXTENSION_LOG_WARNING, "Shutting down tap connections!");

    connNotifier_->stop();

    LockHolder lh(notifySync);
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
        Producer *tp = dynamic_cast<Producer*>((*ii).get());
        if (tp) {
            tp->clearQueues();
        }
    }
    rlh.unlock();

    all.clear();
    map_.clear();
}

bool ConnMap::isBackfillCompleted(std::string &name) {
    LockHolder lh(notifySync);
    connection_t tc = findByName_UNLOCKED(name);
    if (tc.get()) {
        Producer *tp = dynamic_cast<Producer*>(tc.get());
        if (tp) {
            return tp->isBackfillCompleted();
        }
    }
    return false;
}

void ConnMap::updateVBTapConnections(connection_t &conn,
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

void ConnMap::removeVBTapConnections(connection_t &conn) {
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

void ConnMap::notifyIOThreadMain() {
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

    LockHolder lh(notifySync);
    // We should pause unless we purged some connections or
    // all queues have items.
    getExpiredConnections_UNLOCKED(deadClients);

    // see if I have some channels that I have to signal..
    std::map<const void*, connection_t>::iterator iter;
    for (iter = map_.begin(); iter != map_.end(); ++iter) {
        Producer *tp = dynamic_cast<Producer*>(iter->second.get());
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
        Producer *tp = dynamic_cast<Producer*>(iter->second.get());
        if (tp && (tp->isPaused() || tp->doDisconnect()) && !tp->isSuspended()
            && tp->isReserved()) {
            if (!tp->sentNotify() || (tp->lastWalkTime + maxIdleTime < now)) {
                toNotify.push_back(iter->second);
            }
        }
    }

    lh.unlock();

    LockHolder rlh(releaseLock);
    std::list<connection_t>::iterator it;
    for (it = toNotify.begin(); it != toNotify.end(); ++it) {
        Producer *tp = dynamic_cast<Producer*>((*it).get());
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
        Producer *tp = dynamic_cast<Producer*>((*ii).get());
        if (tp) {
            ExTask reapTask = new ConnectionReaperCallback(engine, *ii);
            ExecutorPool::get()->schedule(reapTask, NONIO_TASK_IDX);
        }
    }
}

void ConnMap::incrBackfillRemaining(const std::string &name, size_t num_backfill_items) {
    LockHolder lh(notifySync);

    connection_t tc = findByName_UNLOCKED(name);
    if (tc.get()) {
        Producer *tp = dynamic_cast<Producer*>(tc.get());
        assert(tp);
        tp->incrBackfillRemaining(num_backfill_items);
    }
}

bool ConnMap::closeConnectionByName_UNLOCKED(const std::string &name) {
    bool rv = false;
    connection_t tc = findByName_UNLOCKED(name);
    if (tc.get()) {
        Producer *tp = dynamic_cast<Producer*>(tc.get());
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

bool ConnMap::closeConnectionByName(const std::string &name) {

    LockHolder lh(notifySync);
    return closeConnectionByName_UNLOCKED(name);
}

void ConnMap::loadPrevSessionStats(const std::map<std::string, std::string> &session_stats) {
    LockHolder lh(notifySync);
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

TapConnMap::TapConnMap(EventuallyPersistentEngine &e)
    : ConnMap(e) {

    Configuration &config = engine.getConfiguration();
    noopInterval_ = config.getTapNoopInterval();
    config.addValueChangedListener("tap_noop_interval",
                                   new ConnMapValueChangeListener(*this));
    vbConnLocks = new SpinLock[vbConnLockNum];
    size_t max_vbs = config.getMaxVbuckets();
    for (size_t i = 0; i < max_vbs; ++i) {
        vbConns.push_back(std::list<connection_t>());
    }
}

TapConsumer *TapConnMap::newConsumer(const void* cookie)
{
    LockHolder lh(notifySync);
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
    LockHolder lh(notifySync);
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
        assert(old_cookie);
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

            // dliao: TODO no need to deal with tap or upr separately here for the dummy?
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
    updateVBTapConnections(conn, vbuckets);

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

void TapConnMap::resetReplicaChain() {
    LockHolder lh(notifySync);
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
    }
}

void TapConnMap::scheduleBackfill(const std::set<uint16_t> &backfillVBuckets) {
    LockHolder lh(notifySync);
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
        }
    }
}

bool TapConnMap::changeVBucketFilter(const std::string &name,
                                     const std::vector<uint16_t> &vbuckets,
                                     const std::map<uint16_t, uint64_t> &checkpoints) {
    bool rv = false;
    LockHolder lh(notifySync);
    connection_t tc = findByName_UNLOCKED(name);
    if (tc.get()) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc.get());
        if (tp && (tp->isConnected() || tp->getExpiryTime() > ep_current_time())) {
            LOG(EXTENSION_LOG_INFO, "%s Change the vbucket filter",
                tp->logHeader());
            updateVBTapConnections(tc, vbuckets);
            tp->setVBucketFilter(vbuckets, true);
            tp->registerCursor(checkpoints);
            rv = true;
            notify_UNLOCKED();
        }
    }
    return rv;
}

bool TapConnMap::checkConnectivity(const std::string &name) {
    LockHolder lh(notifySync);
    rel_time_t now = ep_current_time();
    connection_t tc = findByName_UNLOCKED(name);
    if (tc.get()) {
        Producer *tp = dynamic_cast<Producer*>(tc.get());
        if (tp && (tp->isConnected() || tp->getExpiryTime() > now)) {
            return true;
        }
    }
    return false;
}

void TapConnMap::disconnect(const void *cookie) {
    LockHolder lh(notifySync);

    Configuration& config = engine.getConfiguration();
    int tapKeepAlive = static_cast<int>(config.getTapKeepalive());
    std::map<const void*, connection_t>::iterator iter(map_.find(cookie));
    if (iter != map_.end()) {
        if (iter->second.get()) {
            rel_time_t now = ep_current_time();
            Consumer *tc = dynamic_cast<Consumer*>(iter->second.get());
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

void CompleteBackfillTapOperation::perform(Producer *tc, void *) {
    tc->completeBackfill();
}

void CompleteDiskBackfillTapOperation::perform(Producer *tc, void *) {
    tc->completeDiskBackfill();
}

void ScheduleDiskBackfillTapOperation::perform(Producer *tc, void *) {
    tc->scheduleDiskBackfill();
}

void CompletedBGFetchTapOperation::perform(Producer *tc, Item *arg) {
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

UprConnMap::UprConnMap(EventuallyPersistentEngine &e)
    : ConnMap(e) {

    Configuration &config = engine.getConfiguration();
    noopInterval_ = config.getTapNoopInterval(); //dliao: add for upr
    config.addValueChangedListener("upr_noop_interval",
                                   new ConnMapValueChangeListener(*this));
    vbConnLocks = new SpinLock[vbConnLockNum];
    size_t max_vbs = config.getMaxVbuckets();
    for (size_t i = 0; i < max_vbs; ++i) {
        vbConns.push_back(std::list<connection_t>());
    }
}


UprConsumer *UprConnMap::newConsumer(const void* cookie,
                                     const std::string &name)
{
    LockHolder lh(notifySync);

    connection_t conn = findByName_UNLOCKED(name);
    if (conn.get()) {
        all.remove(conn);
        map_.erase(conn->getCookie());
    }

    std::string stream_name("eq_uprq:");
    stream_name.append(name);
    UprConsumer *upr = new UprConsumer(engine, cookie, stream_name);
    connection_t uc(upr);
    LOG(EXTENSION_LOG_INFO, "%s created", uc->logHeader());
    all.push_back(uc);
    map_[cookie] = uc;
    return upr;

}


UprProducer *UprConnMap::newProducer(const void* cookie,
                                     const std::string &name)
{
    LockHolder lh(notifySync);

    connection_t conn = findByName_UNLOCKED(name);
    if (conn.get()) {
        all.remove(conn);
        map_.erase(conn->getCookie());
    }

    std::string stream_name("eq_uprq:");
    stream_name.append(name);
    UprProducer *upr = new UprProducer(engine, cookie, stream_name);
    LOG(EXTENSION_LOG_INFO, "%s created", upr->logHeader());
    all.push_back(connection_t(upr));
    map_[cookie] = upr;

    return upr;
}

void UprConnMap::disconnect(const void *cookie) {
    LockHolder lh(notifySync);
    std::map<const void*, connection_t>::iterator itr(map_.find(cookie));
    if (itr != map_.end()) {
        connection_t conn = itr->second;
        if (conn.get()) {
            all.remove(conn);
            map_.erase(itr);
        }
    }
}

bool UprConnMap::checkConnectivity(const std::string &name) {
    (void) name;
    return true;
}
