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
            iter->second->expiry_time = ep_current_time();
            if (iter->second->doDisconnect) {
                iter->second->expiry_time--;
            } else {
                iter->second->expiry_time += tapKeepAlive;
            }
            iter->second->connected = false;
            iter->second->disconnects++;
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
        found = true;
        tc->appendQueue(q);
        shouldNotify = tc->paused; // notify if paused
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
        rv = tc->getBacklogSize();
    }

    return rv;
}

TapConnection* TapConnMap::findByName_UNLOCKED(const std::string&name) {
    TapConnection *rv(NULL);
    std::list<TapConnection*>::iterator iter;
    for (iter = all.begin(); iter != all.end(); ++iter) {
        TapConnection *tc = *iter;
        if (tc->client == name) {
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
        if (tc->expiry_time <= now && !mapped(tc) && !tc->connected && !tc->suspended) {
            deadClients.push_back(tc);
        }
    }

    for (iter = deadClients.begin(); iter != deadClients.end(); ++iter) {
        TapConnection *tc = *iter;
        purgeSingleExpiredTapConnection(tc);
    }

    return static_cast<int>(deadClients.size());
}

void TapConnMap::addFlushEvent() {
    LockHolder lh(notifySync);
    bool shouldNotify(false);
    std::list<TapConnection*>::iterator iter;
    for (iter = all.begin(); iter != all.end(); iter++) {
        TapConnection *tc = *iter;
        if (!tc->dumpQueue) {
            tc->flush();
            shouldNotify = true;
        }
    }
    if (shouldNotify) {
        notifySync.notify();
    }
}

TapConnection *TapConnMap::newConn(const void* cookie,
                                   const std::string &name,
                                   uint32_t flags,
                                   uint64_t backfillAge,
                                   int tapKeepAlive) {
    LockHolder lh(notifySync);
    purgeExpiredConnections_UNLOCKED();

    TapConnection *tap(NULL);

    std::list<TapConnection*>::iterator iter;
    for (iter = all.begin(); iter != all.end(); ++iter) {
        tap = *iter;
        if (tap->client == name) {
            tap->expiry_time = (rel_time_t)-1;
            ++tap->reconnects;
            break;
        } else {
            tap = NULL;
        }
    }

    // Disconnects aren't quite immediate yet, so if we see a
    // connection request for a client *and* expiry_time is 0, we
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
            tap->client.assign(TapConnection::getAnonTapName());
            tap->doDisconnect = true;
            tap->paused = true;
            tap = NULL;
        } else {
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "The TAP channel (\"%s\") exists... grabbing the channel\n",
                             name.c_str());
            if (miter != map.end()) {
                TapConnection *n = new TapConnection(engine,
                                                     NULL,
                                                     TapConnection::getAnonTapName(),
                                                     0);
                n->doDisconnect = true;
                n->paused = true;
                all.push_back(n);
                map[miter->first] = n;
            }
        }
    }

    bool reconnect = false;
    if (tap == NULL) {
        tap = new TapConnection(engine, cookie, name, flags);
        all.push_back(tap);
    } else {
        tap->setCookie(cookie);
        tap->rollback();
        tap->connected = true;
        tap->evaluateFlags();
        reconnect = true;
    }

    tap->setBackfillAge(backfillAge, reconnect);
    if (tap->doRunBackfill && tap->pendingBackfill) {
        setValidity(tap->client, cookie);
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
    const void *cookie = tc->cookie;
    delete tc;
    if (cookie != NULL) {
        engine.getServerApi()->cookie->release(cookie);
    }
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

bool TapConnMap::isPaused(TapConnection *tc) {
    return tc && tc->paused;
}

bool TapConnMap::shouldDisconnect(TapConnection *tc) {
    return tc && tc->doDisconnect;
}

void TapConnMap::notifyIOThreadMain() {
    bool addNoop = false;

    rel_time_t now = ep_current_time();
    if (now > engine.nextTapNoop && engine.tapNoopInterval != (size_t)-1) {
        addNoop = true;
        engine.nextTapNoop = now + engine.tapNoopInterval;
    }
    LockHolder lh(notifySync);
    // We should pause unless we purged some connections or
    // all queues have items.
    bool shouldPause = purgeExpiredConnections_UNLOCKED() == 0;
    bool noEvents = engine.populateEvents();

    if (shouldPause) {
        shouldPause = noEvents;
    }
    // see if I have some channels that I have to signal..
    std::map<const void*, TapConnection*>::iterator iter;
    for (iter = map.begin(); iter != map.end(); ++iter) {
        if (iter->second->ackSupported &&
            (iter->second->expiry_time < now) &&
            iter->second->windowIsFull())
            {
                shouldPause = false;
                iter->second->doDisconnect = true;
            } else if (iter->second->doDisconnect || !iter->second->idle()) {
            shouldPause = false;
        } else if (addNoop) {
            iter->second->setTimeForNoop();
            shouldPause = false;
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
        purgeExpiredConnections_UNLOCKED();
    }

    // Collect the list of connections that need to be signaled.
    std::list<const void *> toNotify;
    for (iter = map.begin(); iter != map.end(); ++iter) {
        if (iter->second->shouldNotify()) {
            iter->second->notifySent.set(true);
            toNotify.push_back(iter->first);
        }
    }

    lh.unlock();

    engine.notifyIOComplete(toNotify, ENGINE_SUCCESS);
}

void CompleteBackfillTapOperation::perform(TapConnection *tc, void *arg) {
    (void)arg;
    tc->completeBackfill();
}

void CompleteDiskBackfillTapOperation::perform(TapConnection *tc, void *arg) {
    (void)arg;
    tc->completeDiskBackfill();
}

void ScheduleDiskBackfillTapOperation::perform(TapConnection *tc, void *arg) {
    (void)arg;
    tc->scheduleDiskBackfill();
}

void ReceivedItemTapOperation::perform(TapConnection *tc, Item *arg) {
    tc->gotBGItem(arg, implicitEnqueue);
}

void CompletedBGFetchTapOperation::perform(TapConnection *tc,
                                           EventuallyPersistentEngine *epe) {
    (void)epe;
    tc->completedBGFetchJob();
}
