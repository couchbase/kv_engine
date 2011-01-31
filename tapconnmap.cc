/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"

#include <algorithm>

#include "ep_engine.h"
#include "tapconnmap.hh"
#include "tapconnection.hh"

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
    }
    purgeExpiredConnections_UNLOCKED();
}

bool TapConnMap::setEvents(const std::string &name,
                           std::list<QueuedItem> *q) {
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

int TapConnMap::purgeExpiredConnections_UNLOCKED() {
    rel_time_t now = ep_current_time();
    std::list<TapConnection*> deadClients;

    std::list<TapConnection*>::iterator iter;
    for (iter = all.begin(); iter != all.end(); ++iter) {
        TapConnection *tc = *iter;
        if (tc->getExpiryTime() <= now && !mapped(tc) && !tc->isConnected()) {
            TapProducer *tp = dynamic_cast<TapProducer*>(*iter);
            if (tp) {
                if (!tp->suspended) {
                    deadClients.push_back(tc);
                }
            } else {
                deadClients.push_back(tc);
            }
        }
    }

    std::list<TapConnection*>::iterator ii;
    for (ii = deadClients.begin(); ii != deadClients.end(); ++ii) {
        purgeSingleExpiredTapConnection(*ii);
    }

    return static_cast<int>(deadClients.size());
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

TapConsumer *TapConnMap::newConsumer(EventuallyPersistentEngine *engine,
                                     const void* cookie)
{
    LockHolder lh(notifySync);
    purgeExpiredConnections_UNLOCKED();
    TapConsumer *tap = new TapConsumer(*engine, cookie, TapConnection::getAnonName());
    all.push_back(tap);
    map[cookie] = tap;
    return tap;
}

TapProducer *TapConnMap::newProducer(EventuallyPersistentEngine *engine,
                                     const void* cookie,
                                     const std::string &name,
                                     uint32_t flags,
                                     uint64_t backfillAge,
                                     int tapKeepAlive) {
    LockHolder lh(notifySync);
    purgeExpiredConnections_UNLOCKED();

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
            if (miter->first != NULL) {
                TapProducer *n = new TapProducer(*engine,
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
        tap = new TapProducer(*engine, cookie, name, flags);
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

void TapConnMap::purgeSingleExpiredTapConnection(TapConnection *tc) {
    all.remove(tc);
    /* Assert that the connection doesn't live in the map.. */
    assert(!mapped(tc));
    delete tc;
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

void TapConnMap::notifyIOThreadMain(EventuallyPersistentEngine *engine) {
    bool addNoop = false;

    rel_time_t now = ep_current_time();
    if (now > engine->nextTapNoop && engine->tapNoopInterval != (size_t)-1) {
        addNoop = true;
        engine->nextTapNoop = now + engine->tapNoopInterval;
    }
    LockHolder lh(notifySync);
    // We should pause unless we purged some connections or
    // all queues have items.
    bool shouldPause = purgeExpiredConnections_UNLOCKED() == 0;
    bool noEvents = engine->populateEvents();

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
        double diff = engine->nextTapNoop - now;
        if (diff > 0) {
            notifySync.wait(diff);
        }

        if (engine->shutdown) {
            return;
        }
        purgeExpiredConnections_UNLOCKED();
    }

    // Collect the list of connections that need to be signaled.
    std::list<const void *> toNotify;
    for (iter = map.begin(); iter != map.end(); ++iter) {
        TapProducer *tp = dynamic_cast<TapProducer*>(iter->second);
        if (tp && (tp->paused || tp->doDisconnect())) {
            if (!tp->notifySent) {
                tp->notifySent = true;
                toNotify.push_back(iter->first);
            }
        }
    }

    lh.unlock();

    engine->notifyIOComplete(toNotify, ENGINE_SUCCESS);
}

void CompleteBackfillTapOperation::perform(TapProducer *tc, void *arg) {
    (void)arg;
    tc->completeBackfill();
}

void CompleteDiskBackfillTapOperation::perform(TapProducer *tc, void *arg) {
    (void)arg;
    tc->completeDiskBackfill();
}

void ScheduleDiskBackfillTapOperation::perform(TapProducer *tc, void *arg) {
    (void)arg;
    tc->scheduleDiskBackfill();
}

void ReceivedItemTapOperation::perform(TapProducer *tc, Item *arg) {
    tc->gotBGItem(arg, implicitEnqueue);
}

void CompletedBGFetchTapOperation::perform(TapProducer *tc,
                                           EventuallyPersistentEngine *epe) {
    (void)epe;
    tc->completedBGFetchJob();
}

void NotifyIOTapOperation::perform(TapProducer *tc,
                                   EventuallyPersistentEngine *epe) {
    epe->notifyIOComplete(tc->getCookie(), ENGINE_SUCCESS);
}
