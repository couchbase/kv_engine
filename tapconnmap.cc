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
        : engine(e), connection(c) {

    }

    bool callback(Dispatcher &, TaskId) {
        if (connection->cleanSome()) {
            TapProducer *tp = dynamic_cast<TapProducer*>(connection);
            const void *cookie = NULL;
            if (tp != NULL) {
                cookie = tp->getCookie();
            }
            delete connection;
            if (cookie != NULL) {
                engine.getServerApi()->cookie->release(cookie);
            }
            return false;
        }
        return true;
    }

    std::string description() {
        std::stringstream ss;
        ss << "Reaping tap connection: " << connection->getName();
        return ss.str();
    }

private:
    EventuallyPersistentEngine &engine;
    TapConnection *connection;
};

void TapConnMap::disconnect(const void *cookie, int tapKeepAlive) {
    LockHolder lh(notifySync);
    std::map<const void*, TapConnection*>::iterator iter(map.find(cookie));
    if (iter != map.end()) {
        if (iter->second) {
            rel_time_t now = ep_current_time();
            TapConsumer *tc = dynamic_cast<TapConsumer*>(iter->second);
            if (!tc || iter->second->doDisconnect()) {
                iter->second->setExpiryTime(now - 1);
            } else {
                iter->second->setExpiryTime(now + tapKeepAlive);
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

ssize_t TapConnMap::queueDepth(const std::string &name) {
    ssize_t rv(-1);
    LockHolder lh(notifySync);

    TapConnection *tc = findByName_UNLOCKED(name);
    if (tc) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc);
        assert(tp);
        rv = tp->getBacklogSize();
    }

    return rv;
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

void TapConnMap::getExpiredConnections_UNLOCKED(std::list<TapConnection*> &deadClients) {
    rel_time_t now = ep_current_time();

    std::list<TapConnection*>::iterator iter;
    for (iter = all.begin(); iter != all.end(); ++iter) {
        TapConnection *tc = *iter;
        if (tc->getExpiryTime() <= now && !mapped(tc) && !tc->isConnected()) {
            TapProducer *tp = dynamic_cast<TapProducer*>(*iter);
            if (tp) {
                if (!tp->suspended) {
                    deadClients.push_back(tc);
                    removeTapCursors(tp);
                }
            } else {
                deadClients.push_back(tc);
            }
        }
    }

    // Remove them from the list of available tap connections...
    std::list<TapConnection*>::iterator ii;
    for (ii = deadClients.begin(); ii != deadClients.end(); ++ii) {
        all.remove(*ii);
    }
}

void TapConnMap::removeTapCursors(TapProducer *tp) {
    // If this TAP connection is not for the registered TAP client,
    // remove all the checkpoint cursors belonging to the TAP connection.
    if (tp && !tp->registeredTAPClient) {
        const VBucketMap &vbuckets = tp->engine.getEpStore()->getVBuckets();
        size_t numOfVBuckets = vbuckets.getSize();
        // Remove all the cursors belonging to the TAP connection to be purged.
        for (size_t i = 0; i <= numOfVBuckets; ++i) {
            assert(i <= std::numeric_limits<uint16_t>::max());
            uint16_t vbid = static_cast<uint16_t>(i);
            RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
            if (!vb) {
                continue;
            }
            if (tp->vbucketFilter(vbid)) {
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

        if (tapKeepAlive == 0) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "The TAP channel (\"%s\") exists, but should be nuked\n",
                             name.c_str());
            tap->setName(TapConnection::getAnonName());
            tap->setDisconnect(true);
            tap->paused = true;
            tap = NULL;
        } else {
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "The TAP channel (\"%s\") exists... grabbing the channel\n",
                             name.c_str());
            if (miter != map.end()) {
                TapProducer *n = new TapProducer(engine,
                                                 NULL,
                                                 TapConnection::getAnonName(),
                                                 0);
                n->setDisconnect(true);
                n->paused = true;
                all.push_back(n);
                map[miter->first] = n;
            }
        }
    }

    bool reconnect = false;
    if (tap == NULL) {
        tap = new TapProducer(engine, cookie, name, flags);
        all.push_back(tap);
    } else {
        tap->setCookie(cookie);
        tap->rollback();
        tap->setConnected(true);
        tap->evaluateFlags();
        reconnect = true;
    }

    tap->setBackfillAge(backfillAge, reconnect);
    if (tap->doRunBackfill && tap->pendingBackfill) {
        setValidity(tap->getName(), cookie);
    }

    map[cookie] = tap;
    return tap;
}

// These two methods are always called with a lock.
void TapConnMap::setValidity(const std::string &name,
                             const void* token) {
    validity[name] = token;
}
void TapConnMap::clearValidity(const std::string &name) {
    validity.erase(name);
}

// This is always called without a lock.
bool TapConnMap::checkValidity(const std::string &name,
                               const void* token) {
    LockHolder lh(notifySync);
    std::map<const std::string, const void*>::iterator viter =
        validity.find(name);
    return viter != validity.end() && viter->second == token;
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

void TapConnMap::notifyIOThreadMain() {
    bool addNoop = false;

    rel_time_t now = ep_current_time();
    if (now > engine.nextTapNoop && engine.tapNoopInterval != (size_t)-1) {
        addNoop = true;
        engine.nextTapNoop = now + engine.tapNoopInterval;
    }

    std::list<TapConnection*> deadClients;

    LockHolder lh(notifySync);
    // We should pause unless we purged some connections or
    // all queues have items.
    getExpiredConnections_UNLOCKED(deadClients);
    bool shouldPause = deadClients.empty();
    bool noEvents = engine.mutation_count == 0;
    engine.mutation_count = 0;

    if (shouldPause) {
        shouldPause = noEvents;
    }
    // see if I have some channels that I have to signal..
    std::map<const void*, TapConnection*>::iterator iter;
    for (iter = map.begin(); iter != map.end(); ++iter) {
        TapProducer *tp = dynamic_cast<TapProducer*>(iter->second);
        if (tp != NULL) {
            if (tp->supportsAck() && (tp->getExpiryTime() < now) && tp->windowIsFull()) {
                shouldPause = false;

                tp->setDisconnect(true);
            } else if (tp->doDisconnect() || !tp->idle()) {
                shouldPause = false;
            } else if (addNoop) {
                tp->setTimeForNoop();
                shouldPause = false;
            }

        }
    }

    if (shouldPause) {
        double diff = engine.nextTapNoop - now;
        if (diff > 0) {
            notifySync.wait(diff);
        }

        if (engine.shutdown) {
            return;
        }

        getExpiredConnections_UNLOCKED(deadClients);
    }

    // Collect the list of connections that need to be signaled.
    std::list<const void *> toNotify;
    for (iter = map.begin(); iter != map.end(); ++iter) {
        TapProducer *tp = dynamic_cast<TapProducer*>(iter->second);
        if (tp && (tp->paused || tp->doDisconnect())) {
            if (!tp->notifySent) {
                tp->notifySent.set(true);
                toNotify.push_back(iter->first);
            }
        }
    }

    lh.unlock();

    // Delete all of the dead clients
    if (!deadClients.empty()) {
        Dispatcher *d = engine.getEpStore()->getNonIODispatcher();
        std::list<TapConnection*>::iterator ii;
        for (ii = deadClients.begin(); ii != deadClients.end(); ++ii) {
            d->schedule(shared_ptr<DispatcherCallback>
                        (new TapConnectionReaperCallback(engine, *ii)),
                        NULL, Priority::TapConnectionReaperPriority,
                        0, false);
        }
    }
    engine.notifyIOComplete(toNotify, ENGINE_SUCCESS);
}

bool TapConnMap::recordCurrentOpenCheckpointId(const std::string &name, uint16_t vbucket) {
    bool rv(false);
    LockHolder lh(notifySync);

    TapConnection *tc = findByName_UNLOCKED(name);
    if (tc) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc);
        assert(tp);
        rv = true;
        tp->recordCurrentOpenCheckpointId(vbucket);
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

void ReceivedItemTapOperation::perform(TapProducer *tc, Item *arg) {
    tc->gotBGItem(arg, implicitEnqueue);
}

void CompletedBGFetchTapOperation::perform(TapProducer *tc,
                                           EventuallyPersistentEngine *) {
    tc->completedBGFetchJob();
}
