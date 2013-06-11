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
#include <set>
#include <string>
#include <vector>

#include "ep_engine.h"
#include "tapconnection.h"
#include "tapconnmap.h"


class TapConnMapValueChangeListener : public ValueChangedListener {
public:
    TapConnMapValueChangeListener(TapConnMap &tc) : tapconnmap(tc) {
    }

    virtual void sizeValueChanged(const std::string &key, size_t value) {
        if (key.compare("tap_noop_interval") == 0) {
            tapconnmap.setTapNoopInterval(value);
        }
    }

private:
    TapConnMap &tapconnmap;
};

TapConnMap::TapConnMap(EventuallyPersistentEngine &theEngine) :
    notifyCounter(0), engine(theEngine), nextTapNoop(0)
{
    Configuration &config = engine.getConfiguration();
    tapNoopInterval = config.getTapNoopInterval();
    config.addValueChangedListener("tap_noop_interval",
                                   new TapConnMapValueChangeListener(*this));
}

void TapConnMap::disconnect(const void *cookie, int tapKeepAlive) {
    LockHolder lh(notifySync);
    std::map<const void*, connection_t>::iterator iter(map.find(cookie));
    if (iter != map.end()) {
        if (iter->second.get()) {
            rel_time_t now = ep_current_time();
            TapConsumer *tc = dynamic_cast<TapConsumer*>(iter->second.get());
            if (tc || iter->second->doDisconnect()) {
                iter->second->setExpiryTime(now - 1);
                LOG(EXTENSION_LOG_WARNING, "%s disconnected",
                    iter->second->logHeader());
            } else {
                iter->second->setExpiryTime(now + tapKeepAlive);
                LOG(EXTENSION_LOG_WARNING,
                    "%s disconnected, keep alive for %d seconds",
                    iter->second->logHeader(), tapKeepAlive);
            }
            iter->second->setConnected(false);
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "Found half-linked tap connection at: %p", cookie);
        }
        map.erase(iter);
    }
}

bool TapConnMap::setEvents(const std::string &name,
                           std::list<queued_item> *q) {
    bool found(false);
    LockHolder lh(notifySync);

    connection_t tc = findByName_UNLOCKED(name);
    if (tc.get()) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc.get());
        assert(tp);
        found = true;
        tp->appendQueue(q);
        lh.unlock();
        notifyPausedConnection_UNLOCKED(tp);
    }

    return found;
}

ssize_t TapConnMap::backfillQueueDepth(const std::string &name) {
    ssize_t rv(-1);
    LockHolder lh(notifySync);

    connection_t tc = findByName_UNLOCKED(name);
    if (tc.get()) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc.get());
        assert(tp);
        rv = tp->getBackfillQueueSize();
    }

    return rv;
}

connection_t TapConnMap::findByName(const std::string &name) {
    LockHolder lh(notifySync);
    return findByName_UNLOCKED(name);
}

connection_t TapConnMap::findByName_UNLOCKED(const std::string&name) {
    connection_t rv(NULL);
    std::list<connection_t>::iterator iter;
    for (iter = all.begin(); iter != all.end(); ++iter) {
        if ((*iter)->getName() == name) {
            rv = *iter;
        }
    }
    return rv;
}

void TapConnMap::getExpiredConnections_UNLOCKED(std::list<connection_t> &deadClients) {
    rel_time_t now = ep_current_time();

    std::list<connection_t>::iterator iter;
    for (iter = all.begin(); iter != all.end(); ++iter) {
        connection_t &tc = *iter;
        if (tc->isConnected()) {
            continue;
        }

        TapProducer *tp = dynamic_cast<TapProducer*>(tc.get());

        if (tc->getExpiryTime() <= now && !mapped(tc)) {
            if (tp) {
                if (!tp->suspended) {
                    deadClients.push_back(tc);
                    removeTapCursors_UNLOCKED(tp);
                }
            } else {
                deadClients.push_back(tc);
            }
        }
    }

    // Remove them from the list of available tap connections...
    std::list<connection_t>::iterator ii;
    for (ii = deadClients.begin(); ii != deadClients.end(); ++ii) {
        std::list<connection_t>::iterator conn_it = all.begin();
        for (; conn_it != all.end(); ++conn_it) {
            if ((*ii).get() == (*conn_it).get()) {
                all.erase(conn_it);
                break;
            }
        }
    }
}

void TapConnMap::removeTapCursors_UNLOCKED(TapProducer *tp) {
    // If this TAP connection is not for the registered TAP client,
    // remove all the checkpoint cursors belonging to the TAP connection.
    if (tp && !tp->registeredTAPClient) {
        const VBucketMap &vbuckets = tp->engine.getEpStore()->getVBuckets();
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
                vb->checkpointManager.removeTAPCursor(tp->name);
            }
        }
    }
}

void TapConnMap::addFlushEvent() {
    LockHolder lh(notifySync);
    std::list<connection_t>::iterator iter;
    for (iter = all.begin(); iter != all.end(); iter++) {
        TapProducer *tp = dynamic_cast<TapProducer*>((*iter).get());
        if (tp && !tp->dumpQueue) {
            tp->flush();
        }
    }
}

TapConsumer *TapConnMap::newConsumer(const void* cookie)
{
    LockHolder lh(notifySync);
    TapConsumer *tc = new TapConsumer(engine, cookie, TapConnection::getAnonName());
    connection_t tap(tc);
    LOG(EXTENSION_LOG_INFO, "%s created", tap->logHeader());
    all.push_back(tap);
    map[cookie] = tap;
    return tc;
}

TapProducer *TapConnMap::newProducer(const void* cookie,
                                     const std::string &name,
                                     uint32_t flags,
                                     uint64_t backfillAge,
                                     int tapKeepAlive,
                                     bool isRegistered,
                                     bool closedCheckpointOnly,
                                     const std::vector<uint16_t> &vbuckets,
                                     const std::map<uint16_t, uint64_t> &lastCheckpointIds) {
    LockHolder lh(notifySync);
    TapProducer *tap(NULL);

    std::list<connection_t>::iterator iter;
    for (iter = all.begin(); iter != all.end(); ++iter) {
        tap = dynamic_cast<TapProducer*>((*iter).get());
        if (tap && tap->getName() == name) {
            tap->setExpiryTime((rel_time_t)-1);
            ++tap->reconnects;
            break;
        } else {
            tap = NULL;
        }
    }

    if (tap != NULL) {
        const void *old_cookie = tap->getCookie();
        assert(old_cookie);
        map.erase(old_cookie);

        if (tapKeepAlive == 0 || (tap->mayCompleteDumpOrTakeover() && tap->idle())) {
            LOG(EXTENSION_LOG_INFO,
                "%s keep alive timed out, should be nuked", tap->logHeader());
            tap->setName(TapConnection::getAnonName());
            tap->setDisconnect(true);
            tap->setConnected(false);
            tap->paused = true;
            tap->setExpiryTime(ep_current_time() - 1);
            tap = NULL;
        } else {
            LOG(EXTENSION_LOG_INFO, "%s exists... grabbing the channel",
                tap->logHeader());
            // Create the dummy expired tap connection for the old connection cookie.
            // This dummy tap connection will be used for releasing the corresponding
            // memcached connection.
            TapProducer *n = new TapProducer(engine,
                                             old_cookie,
                                             TapConnection::getAnonName(),
                                             0);
            n->setDisconnect(true);
            n->setConnected(false);
            n->paused = true;
            n->setExpiryTime(ep_current_time() - 1);
            all.push_back(connection_t(n));
        }
    }

    bool reconnect = false;
    if (tap == NULL) {
        tap = new TapProducer(engine, cookie, name, flags);
        LOG(EXTENSION_LOG_INFO, "%s created", tap->logHeader());
        all.push_back(connection_t(tap));
    } else {
        tap->setCookie(cookie);
        tap->setReserved(true);
        tap->evaluateFlags();
        tap->setConnected(true);
        tap->setDisconnect(false);
        reconnect = true;
    }

    tap->setTapFlagByteorderSupport((flags & TAP_CONNECT_TAP_FIX_FLAG_BYTEORDER) != 0);
    tap->setBackfillAge(backfillAge, reconnect);
    tap->setRegisteredClient(isRegistered);
    tap->setClosedCheckpointOnlyFlag(closedCheckpointOnly);
    tap->setVBucketFilter(vbuckets);
    tap->registerTAPCursor(lastCheckpointIds);

    if (reconnect) {
        tap->rollback();
    }

    map[cookie] = connection_t(tap);
    engine.storeEngineSpecific(cookie, tap);
    // Clear all previous session stats for this producer.
    clearPrevSessionStats(tap->getName());

    return tap;
}

bool TapConnMap::checkConnectivity(const std::string &name) {
    LockHolder lh(notifySync);
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

bool TapConnMap::mapped(connection_t &tc) {
    bool rv = false;
    std::map<const void*, connection_t>::iterator it;
    for (it = map.begin(); it != map.end(); ++it) {
        if (it->second.get() == tc.get()) {
            rv = true;
            break;
        }
    }
    return rv;
}

void TapConnMap::notifyPausedConnection_UNLOCKED(TapProducer *tp) {
    LockHolder rlh(releaseLock);
    if (tp && tp->paused && tp->isReserved()) {
        engine.notifyIOComplete(tp->getCookie(), ENGINE_SUCCESS);
        tp->notifySent.set(true);
    }
}

void TapConnMap::shutdownAllTapConnections() {
    LOG(EXTENSION_LOG_WARNING, "Shutting down tap connections!");
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
        TapProducer *tp = dynamic_cast<TapProducer*>((*ii).get());
        if (tp) {
            tp->clearQueues();
        }
    }
    rlh.unlock();

    all.clear();
    map.clear();
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

bool TapConnMap::isBackfillCompleted(std::string &name) {
    LockHolder lh(notifySync);
    connection_t tc = findByName_UNLOCKED(name);
    if (tc.get()) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc.get());
        if (tp) {
            return tp->isBackfillCompleted();
        }
    }
    return false;
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
            tp->setVBucketFilter(vbuckets, true);
            tp->registerTAPCursor(checkpoints);
            rv = true;
            notify_UNLOCKED();
        }
    }
    return rv;
}

void TapConnMap::notifyIOThreadMain() {
    // To avoid connections to be stucked in a bogus state forever, we're going
    // to ping all connections that hasn't tried to walk the tap queue
    // for this amount of time..
    const int maxIdleTime = 5;

    bool addNoop = false;

    rel_time_t now = ep_current_time();
    if (now > nextTapNoop && tapNoopInterval != (size_t)-1) {
        addNoop = true;
        nextTapNoop = now + tapNoopInterval;
    }

    std::list<connection_t> deadClients;

    LockHolder lh(notifySync);
    // We should pause unless we purged some connections or
    // all queues have items.
    getExpiredConnections_UNLOCKED(deadClients);

    // see if I have some channels that I have to signal..
    std::map<const void*, connection_t>::iterator iter;
    for (iter = map.begin(); iter != map.end(); ++iter) {
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
    for (iter = map.begin(); iter != map.end(); ++iter) {
        TapProducer *tp = dynamic_cast<TapProducer*>(iter->second.get());
        if (tp && (tp->paused || tp->doDisconnect()) && !tp->suspended && tp->isReserved()) {
            if (!tp->notifySent || (tp->lastWalkTime + maxIdleTime < now)) {
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
            tp->notifySent.set(true);
        }
    }

    // Delete all of the dead clients
    std::list<connection_t>::iterator ii;
    for (ii = deadClients.begin(); ii != deadClients.end(); ++ii) {
        LOG(EXTENSION_LOG_WARNING, "Clean up \"%s\"", (*ii)->getName().c_str());
        (*ii)->releaseReference();
        TapProducer *tp = dynamic_cast<TapProducer*>((*ii).get());
        if (tp) {
            tp->clearQueues();
        }
    }
}

bool TapConnMap::SetCursorToOpenCheckpoint(const std::string &name, uint16_t vbucket) {
    bool rv(false);
    LockHolder lh(notifySync);

    connection_t tc = findByName_UNLOCKED(name);
    if (tc.get()) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc.get());
        assert(tp);
        rv = true;
        tp->SetCursorToOpenCheckpoint(vbucket);
    }

    return rv;
}

void TapConnMap::incrBackfillRemaining(const std::string &name, size_t num_backfill_items) {
    LockHolder lh(notifySync);

    connection_t tc = findByName_UNLOCKED(name);
    if (tc.get()) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc.get());
        assert(tp);
        tp->incrBackfillRemaining(num_backfill_items);
    }
}

bool TapConnMap::closeTapConnectionByName(const std::string &name) {
    bool rv = false;
    LockHolder lh(notifySync);
    connection_t tc = findByName_UNLOCKED(name);
    if (tc.get()) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc.get());
        if (tp) {
            LOG(EXTENSION_LOG_WARNING, "%s Connection is closed by force",
                tp->logHeader());
            tp->setRegisteredClient(false);
            removeTapCursors_UNLOCKED(tp);

            tp->setExpiryTime(ep_current_time() - 1);
            tp->setName(TapConnection::getAnonName());
            tp->setDisconnect(true);
            tp->paused = true;
            rv = true;
        }
    }
    return rv;
}

void TapConnMap::loadPrevSessionStats(const std::map<std::string, std::string> &session_stats) {
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
