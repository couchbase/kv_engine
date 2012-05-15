/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"

#include <algorithm>

#include "ep_engine.h"
#include "tapconnmap.hh"
#include "tapconnection.hh"

/**
 * Dispatcher task to nuke a tap connection.
 */
class TapConnectionReaperCallback : public DispatcherCallback {
public:
    TapConnectionReaperCallback(EventuallyPersistentEngine &e, TapConnection *c)
        : engine(e), connection(c)
    {
        // Release resources reserved "upstream"
        const void *cookie = c->getCookie();
        if (cookie != NULL) {
            c->releaseReference();
        }
        std::stringstream ss;
        ss << "Reaping tap connection: " << connection->getName();
        descr = ss.str();
    }

    bool callback(Dispatcher &, TaskId) {
        if (connection->cleanSome()) {
            delete connection;
            return false;
        }
        return true;
    }

    std::string description() {
        return descr;
    }

private:
    EventuallyPersistentEngine &engine;
    TapConnection *connection;
    std::string descr;
};

void TapConnMap::disconnect(const void *cookie, int tapKeepAlive) {
    LockHolder lh(notifySync);
    std::map<const void*, TapConnection*>::iterator iter(map.find(cookie));
    if (iter != map.end()) {
        if (iter->second) {
            rel_time_t now = ep_current_time();
            TapConsumer *tc = dynamic_cast<TapConsumer*>(iter->second);
            if (tc || iter->second->doDisconnect()) {
                iter->second->setExpiryTime(now - 1);
                getLogger()->log(EXTENSION_LOG_INFO, NULL,
                                 "%s disconnected", iter->second->logHeader());
            } else {
                iter->second->setExpiryTime(now + tapKeepAlive);
                getLogger()->log(EXTENSION_LOG_INFO, NULL,
                                 "%s disconnected, keep alive for %d seconds",
                                 iter->second->logHeader(), tapKeepAlive);
            }
            iter->second->setConnected(false);
        } else {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Found half-linked tap connection at: %p\n",
                             cookie);
        }
        map.erase(iter);

        // Notify the daemon thread so that it may reap them..
        notifySync.notify();
    }
}

bool TapConnMap::setEvents(const std::string &name,
                           std::list<queued_item> *q) {
    bool shouldNotify(true);
    bool found(false);
    LockHolder lh(notifySync);

    TapConnection *tc = findByName_UNLOCKED(name);
    if (tc) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc);
        assert(tp);
        found = true;
        tp->appendQueue(q);
        shouldNotify = tp->paused; // notify if paused
    }

    if (shouldNotify) {
        notifySync.notify();
    }

    return found;
}

ssize_t TapConnMap::backfillQueueDepth(const std::string &name) {
    ssize_t rv(-1);
    LockHolder lh(notifySync);

    TapConnection *tc = findByName_UNLOCKED(name);
    if (tc) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc);
        assert(tp);
        rv = tp->getBackfillRemaining();
    }

    return rv;
}

TapConnection* TapConnMap::findByName(const std::string &name) {
    LockHolder lh(notifySync);
    return findByName_UNLOCKED(name);
}

TapConnection* TapConnMap::findByName_UNLOCKED(const std::string&name) {
    TapConnection *rv(NULL);
    std::list<TapConnection*>::iterator iter;
    for (iter = all.begin(); iter != all.end(); ++iter) {
        TapConnection *tc = *iter;
        if (tc->getName() == name) {
            rv = tc;
        }
    }
    return rv;
}

void TapConnMap::getExpiredConnections_UNLOCKED(std::list<TapConnection*> &deadClients,
                                                std::list<TapConnection*> &regClients) {
    rel_time_t now = ep_current_time();

    std::list<TapConnection*>::iterator iter;
    for (iter = all.begin(); iter != all.end(); ++iter) {
        TapConnection *tc = *iter;
        if (tc->isConnected()) {
            continue;
        }

        TapProducer *tp = dynamic_cast<TapProducer*>(*iter);

        if (tc->getExpiryTime() <= now && !mapped(tc)) {
            if (tp) {
                if (!tp->suspended) {
                    deadClients.push_back(tc);
                    removeTapCursors_UNLOCKED(tp);
                }
            } else {
                deadClients.push_back(tc);
            }
        } else if (tc->isReserved()) {
            if (tp == NULL || !tp->suspended) {
                // to avoid others to release it as well ;)
                tc->setReserved(false);
                regClients.push_back(tc);
            }
        }
    }

    // Remove them from the list of available tap connections...
    std::list<TapConnection*>::iterator ii;
    for (ii = deadClients.begin(); ii != deadClients.end(); ++ii) {
        all.remove(*ii);
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
                getLogger()->log(EXTENSION_LOG_INFO, NULL,
                                 "%s Remove the TAP cursor from vbucket %d\n",
                                 tp->logHeader(), vbid);
                vb->checkpointManager.removeTAPCursor(tp->name);
            }
        }
    }
}

void TapConnMap::addFlushEvent() {
    LockHolder lh(notifySync);
    bool shouldNotify(false);
    std::list<TapConnection*>::iterator iter;
    for (iter = all.begin(); iter != all.end(); iter++) {
        TapProducer *tc = dynamic_cast<TapProducer*>(*iter);
        if (tc && !tc->dumpQueue) {
            tc->flush();
            shouldNotify = true;
        }
    }
    if (shouldNotify) {
        notifySync.notify();
    }
}

TapConsumer *TapConnMap::newConsumer(const void* cookie)
{
    LockHolder lh(notifySync);
    TapConsumer *tap = new TapConsumer(engine, cookie, TapConnection::getAnonName());
    getLogger()->log(EXTENSION_LOG_INFO, NULL, "%s created\n",
                     tap->logHeader());
    all.push_back(tap);
    map[cookie] = tap;
    return tap;
}

TapProducer *TapConnMap::newProducer(const void* cookie,
                                     const std::string &name,
                                     uint32_t flags,
                                     uint64_t backfillAge,
                                     int tapKeepAlive) {
    LockHolder lh(notifySync);
    TapProducer *tap(NULL);

    std::list<TapConnection*>::iterator iter;
    for (iter = all.begin(); iter != all.end(); ++iter) {
        tap = dynamic_cast<TapProducer*>(*iter);
        if (tap && tap->getName() == name) {
            tap->setExpiryTime((rel_time_t)-1);
            ++tap->reconnects;
            break;
        } else {
            tap = NULL;
        }
    }

    // Disconnects aren't quite immediate yet, so if we see a
    // connection request for a client *and* expiryTime is 0, we
    // should kill this guy off.
    if (tap != NULL) {
        std::map<const void*, TapConnection*>::iterator miter;
        for (miter = map.begin(); miter != map.end(); ++miter) {
            if (miter->second == tap) {
                break;
            }
        }

        if (tapKeepAlive == 0 || (tap->complete() && tap->idle())) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "%s keep alive timed out, should be nuked\n",
                             tap->logHeader());
            tap->setName(TapConnection::getAnonName());
            tap->setDisconnect(true);
            tap->paused = true;
            tap = NULL;
        } else {
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "%s exists... grabbing the channel\n",
                             tap->logHeader());
            if (miter != map.end()) {
                TapProducer *n = new TapProducer(engine,
                                                 NULL,
                                                 TapConnection::getAnonName(),
                                                 0);
                n->setDisconnect(true);
                n->setConnected(false);
                n->paused = true;
                all.push_back(n);
                map[miter->first] = n;
            }
        }
    }

    bool reconnect = false;
    if (tap == NULL) {
        tap = new TapProducer(engine, cookie, name, flags);
        getLogger()->log(EXTENSION_LOG_INFO, NULL, "%s created\n",
                         tap->logHeader());
        all.push_back(tap);
    } else {
        if (tap->isReserved()) {
            assert(tap->getCookie() != NULL);
            tap->releaseReference();
        }
        tap->setCookie(cookie);
        tap->setReserved(true);
        tap->rollback();
        tap->setConnected(true);
        tap->setDisconnect(false);
        tap->evaluateFlags();
        reconnect = true;
    }

    tap->setBackfillAge(backfillAge, reconnect);

    map[cookie] = tap;
    return tap;
}

bool TapConnMap::checkConnectivity(const std::string &name) {
    LockHolder lh(notifySync);
    rel_time_t now = ep_current_time();
    TapConnection *tc = findByName_UNLOCKED(name);
    if (tc) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc);
        if (tp && (tp->isConnected() || tp->getExpiryTime() > now)) {
            return true;
        }
    }
    return false;
}

bool TapConnMap::mapped(TapConnection *tc) {
    bool rv = false;
    std::map<const void*, TapConnection*>::iterator it;
    for (it = map.begin(); it != map.end(); ++it) {
        if (it->second == tc) {
            rv = true;
        }
    }
    return rv;
}

bool TapConnMap::isPaused(TapProducer *tc) {
    return tc && tc->paused;
}

bool TapConnMap::shouldDisconnect(TapConnection *tc) {
    return tc && tc->doDisconnect();
}

void TapConnMap::shutdownAllTapConnections() {
    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                     "Shutting down tap connections!");
    LockHolder lh(notifySync);
    // We should pause unless we purged some connections or
    // all queues have items.
    if (all.empty()) {
        return;
    }
    Dispatcher *d = engine.getEpStore()->getNonIODispatcher();
    std::list<TapConnection*>::iterator ii;
    for (ii = all.begin(); ii != all.end(); ++ii) {
        getLogger()->log(EXTENSION_LOG_INFO, NULL,
                         "Schedule cleanup of \"%s\"",
                         (*ii)->getName().c_str());
        d->schedule(shared_ptr<DispatcherCallback>
                    (new TapConnectionReaperCallback(engine, *ii)),
                    NULL, Priority::TapConnectionReaperPriority,
                    0, false, true);
    }
    all.clear();
    map.clear();
}

void TapConnMap::scheduleBackfill(const std::set<uint16_t> &backfillVBuckets) {
    LockHolder lh(notifySync);
    bool shouldNotify(false);
    rel_time_t now = ep_current_time();
    std::list<TapConnection*>::iterator it = all.begin();
    for (; it != all.end(); ++it) {
        TapConnection *tc = *it;
        TapProducer *tp = dynamic_cast<TapProducer*>(tc);
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
        if (vblist.size() > 0) {
            tp->scheduleBackfill(vblist);
            shouldNotify = true;
        }
    }
    if (shouldNotify) {
        notifySync.notify();
    }
}

void TapConnMap::resetReplicaChain() {
    LockHolder lh(notifySync);
    rel_time_t now = ep_current_time();
    std::list<TapConnection*>::iterator it = all.begin();
    for (; it != all.end(); ++it) {
        TapConnection *tc = *it;
        TapProducer *tp = dynamic_cast<TapProducer*>(tc);
        if (!(tp && (tp->isConnected() || tp->getExpiryTime() > now))) {
            continue;
        }
        getLogger()->log(EXTENSION_LOG_INFO, NULL,
                         "%s Reset the replication chain.\n",
                         tp->logHeader());
        // Get the list of vbuckets that each TAP producer is replicating
        VBucketFilter vbfilter = tp->getVBucketFilter();
        std::vector<uint16_t> vblist (vbfilter.getVBSet().begin(), vbfilter.getVBSet().end());
        // TAP producer sends INITIAL_VBUCKET_STREAM messages to the destination to reset
        // replica vbuckets, and then backfills items to the destination.
        tp->scheduleBackfill(vblist);
    }
    notifySync.notify();
}

void TapConnMap::notifyIOThreadMain() {
    // To avoid connections to be stucked in a bogus state forever, we're going
    // to ping all connections that hasn't tried to walk the tap queue
    // for this amount of time..
    const int maxIdleTime = 5;

    bool addNoop = false;

    rel_time_t now = ep_current_time();
    if (now > engine.nextTapNoop && engine.tapNoopInterval != (size_t)-1) {
        addNoop = true;
        engine.nextTapNoop = now + engine.tapNoopInterval;
    }

    std::list<TapConnection*> deadClients;
    std::list<TapConnection*> registeredClients;

    LockHolder lh(notifySync);
    // We should pause unless we purged some connections or
    // all queues have items.
    getExpiredConnections_UNLOCKED(deadClients, registeredClients);
    engine.mutation_count = 0;

    // see if I have some channels that I have to signal..
    std::map<const void*, TapConnection*>::iterator iter;
    for (iter = map.begin(); iter != map.end(); ++iter) {
        TapProducer *tp = dynamic_cast<TapProducer*>(iter->second);
        if (tp != NULL) {
            if (tp->supportsAck() && (tp->getExpiryTime() < now) && tp->windowIsFull()) {
                getLogger()->log(EXTENSION_LOG_INFO, NULL,
                         "%s Expired and ack windows is full. Disconnecting...\n",
                         tp->logHeader());
                tp->setDisconnect(true);
            } else if (addNoop) {
                tp->setTimeForNoop();
            }
        }
    }

    // Collect the list of connections that need to be signaled.
    std::list<const void *> toNotify;
    for (iter = map.begin(); iter != map.end(); ++iter) {
        TapProducer *tp = dynamic_cast<TapProducer*>(iter->second);
        if (tp && (tp->paused || tp->doDisconnect()) && !tp->suspended) {
            if (!tp->notifySent || (tp->lastWalkTime + maxIdleTime < now)) {
                tp->notifySent.set(true);
                toNotify.push_back(iter->first);
                engine.getServerApi()->cookie->reserve(iter->first);
            }
        }
    }

    if (!registeredClients.empty()) {
        std::list<TapConnection*>::iterator ii;
        for (ii = registeredClients.begin(); ii != registeredClients.end(); ++ii) {
            (*ii)->releaseReference(true);
        }
    }

    // Delete all of the dead clients
    if (!deadClients.empty()) {
        Dispatcher *d = engine.getEpStore()->getNonIODispatcher();
        std::list<TapConnection*>::iterator ii;
        for (ii = deadClients.begin(); ii != deadClients.end(); ++ii) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "Schedule cleanup of \"%s\"",
                             (*ii)->getName().c_str());
            d->schedule(shared_ptr<DispatcherCallback>
                        (new TapConnectionReaperCallback(engine, *ii)),
                        NULL, Priority::TapConnectionReaperPriority,
                        0, false, true);
        }
    }
    lh.unlock();

    engine.notifyIOComplete(toNotify, ENGINE_SUCCESS);

    lh.lock();
    std::list<const void *>::iterator ii;
    for (ii = toNotify.begin(); ii != toNotify.end(); ++ii) {
        engine.getServerApi()->cookie->release(*ii);
    }
}

bool TapConnMap::SetCursorToOpenCheckpoint(const std::string &name, uint16_t vbucket) {
    bool rv(false);
    LockHolder lh(notifySync);

    TapConnection *tc = findByName_UNLOCKED(name);
    if (tc) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc);
        assert(tp);
        rv = true;
        tp->SetCursorToOpenCheckpoint(vbucket);
    }

    return rv;
}

bool TapConnMap::closeTapConnectionByName(const std::string &name) {
    bool rv = false;
    LockHolder lh(notifySync);
    TapConnection *tc = findByName_UNLOCKED(name);
    if (tc) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc);
        if (tp) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "%s Connection is closed by force.\n",
                             tp->logHeader());
            tp->setRegisteredClient(false);
            removeTapCursors_UNLOCKED(tp);

            tp->setExpiryTime(ep_current_time() - 1);
            tp->setName(TapConnection::getAnonName());
            tp->setDisconnect(true);
            tp->paused = true;
            rv = true;
            notifySync.notify();
        }
    }
    return rv;
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
    if (connCookie != tc->getCookie() && !tc->isReconnected()) {
        return;
    }
    tc->completeBGFetchJob(arg, implicitEnqueue);
}
