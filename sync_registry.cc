/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011 Couchbase, Inc.
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
#include <cstdio>
#include <memcached/extension.h>

#include "sync_registry.hh"
#include "ep_engine.h"
#include "locks.hh"


std::ostream& operator << (std::ostream& os, const key_spec_t &keyspec) {
    os << "key_spec_t(cas: " << keyspec.cas <<
        ", vbucket: " << keyspec.vbucketid <<
        ", key: " << keyspec.key << ")";

    return os;
}


bool SyncAbortCallback::callback(Dispatcher &, TaskId) {
    if (listener.isFinished()) {
        return false;
    }

    hrtime_t diff = (gethrtime() - listener.startTime) / 1000000;

    if (diff >= listener.engine.getSyncCmdTimeout()) {
        listener.maybeNotifyIOComplete(true);

        return false;
    }

    return true;
}

hrtime_t SyncAbortCallback::maxExpectedDuration() {
    hrtime_t syncTimeout = listener.engine.getSyncCmdTimeout() * 1000LL;
    return syncTimeout + (syncTimeout / 4);
}



void SyncRegistry::addPersistenceListener(SyncListener *syncListener) {
    LockHolder lh(persistenceMutex);
    persistenceListeners.insert(syncListener);
}


void SyncRegistry::removePersistenceListener(SyncListener *syncListener) {
    LockHolder lh(persistenceMutex);
    std::set<SyncListener*>::iterator it = persistenceListeners.find(syncListener);

    if (it != persistenceListeners.end()) {
        persistenceListeners.erase(it);
    }
}


void SyncRegistry::itemPersisted(const queued_item &item) {
    key_spec_t keyspec(item);
    LockHolder lh(persistenceMutex);
    notifyListeners(persistenceListeners, keyspec, false);
}


void SyncRegistry::itemsPersisted(std::list<queued_item> &itemlist) {
    LockHolder lh(persistenceMutex);
    std::list<queued_item>::iterator it = itemlist.begin();

    for ( ; it != itemlist.end(); it++) {
        key_spec_t keyspec(0, (*it)->getVBucketId(), (*it)->getKey());
        notifyListeners(persistenceListeners, keyspec, false);
    }
}


void SyncRegistry::addMutationListener(SyncListener *syncListener) {
    LockHolder lh(mutationMutex);
    mutationListeners.insert(syncListener);
}


void SyncRegistry::removeMutationListener(SyncListener *syncListener) {
    LockHolder lh(mutationMutex);
    std::set<SyncListener*>::iterator it = mutationListeners.find(syncListener);

    if (it != mutationListeners.end()) {
        mutationListeners.erase(it);
    }
}


void SyncRegistry::itemModified(const key_spec_t &keyspec) {
    LockHolder lh(mutationMutex);
    notifyListeners(mutationListeners, keyspec, false);
}


void SyncRegistry::itemDeleted(const key_spec_t &keyspec) {
    LockHolder lh(mutationMutex);
    notifyListeners(mutationListeners, keyspec, true);
}


void SyncRegistry::addReplicationListener(SyncListener *syncListener) {
    LockHolder lh(replicationMutex);
    replicationListeners.insert(syncListener);
}


void SyncRegistry::removeReplicationListener(SyncListener *syncListener) {
    LockHolder lh(replicationMutex);
    std::set<SyncListener*>::iterator it = replicationListeners.find(syncListener);

    if (it != replicationListeners.end()) {
        replicationListeners.erase(it);
    }
}


void SyncRegistry::itemReplicated(const key_spec_t &keyspec, uint8_t replicaCount) {
    LockHolder lh(replicationMutex);
    notifyListeners(replicationListeners, keyspec, replicaCount);
}


void SyncRegistry::notifyListeners(std::set<SyncListener*> &listeners,
                                   const key_spec_t &keyspec,
                                   bool deleted) {
    std::set<SyncListener*>::iterator it = listeners.begin();

    while (it != listeners.end()) {
        SyncListener *listener = *it;

        if (!listener->isFinished()) {
            listener->keySynced(keyspec, deleted);

            if (listener->isFinished()) {
                listener->maybeNotifyIOComplete();
                listeners.erase(it++);
            } else {
                ++it;
            }
        } else {
            ++it;
        }
    }
}


void SyncRegistry::notifyListeners(std::set<SyncListener*> &listeners,
                                   const key_spec_t &keyspec,
                                   uint8_t replicaCount) {
    std::set<SyncListener*>::iterator it = listeners.begin();

    while (it != listeners.end()) {
        SyncListener *listener = *it;

        if (!listener->isFinished()) {
            listener->keySynced(keyspec, replicaCount);

            if (listener->isFinished()) {
                listener->maybeNotifyIOComplete();
                listeners.erase(it++);
            } else {
                ++it;
            }
        } else {
            ++it;
        }
    }
}


SyncListener::SyncListener(EventuallyPersistentEngine &epEngine,
                           const void *c,
                           std::set<key_spec_t> *keys,
                           sync_type_t sync_type,
                           uint8_t replicaCount) :
    engine(epEngine), cookie(c), keySpecs(keys), persistedOrReplicated(0),
    startTime(gethrtime()), syncType(sync_type),
    replicasPerKey(replicaCount), finished(false), allowNotify(false) {
}


SyncListener::~SyncListener() {
    switch (syncType) {
    case PERSIST:
        engine.getSyncRegistry().removePersistenceListener(this);
        break;
    case MUTATION:
        engine.getSyncRegistry().removeMutationListener(this);
        break;
    case REP:
        engine.getSyncRegistry().removeReplicationListener(this);
        break;
    case REP_OR_PERSIST:
    case REP_AND_PERSIST:
        engine.getSyncRegistry().removeReplicationListener(this);
        engine.getSyncRegistry().removePersistenceListener(this);
    }

    delete keySpecs;
}


void SyncListener::keySynced(const key_spec_t &keyspec, bool deleted) {
    LockHolder lh(mutex);
    std::set<key_spec_t>::iterator it = keySpecs->find(keyspec);

    if (it != keySpecs->end()) {
        switch (syncType) {
        case PERSIST:
            {
                key_spec_t key = keyspec;
                key.cas = it->cas;
                persistedKeys.insert(key);
                finished = (persistedKeys.size() == keySpecs->size());
            }
            break;
        case MUTATION:
            if (deleted) {
                deletedKeys.insert(keyspec);
            } else {
                modifiedKeys.insert(keyspec);
            }
            finished = ((modifiedKeys.size() + deletedKeys.size()) == keySpecs->size());
            break;
        case REP_OR_PERSIST:
            {
                key_spec_t key = keyspec;
                key.cas = it->cas;
                it = persistedKeys.find(key);

                if (it == persistedKeys.end()) {
                    if (replicatedKeys.find(key) == replicatedKeys.end()) {
                        ++persistedOrReplicated;
                    }
                } else {
                    persistedKeys.erase(it);
                }

                // always insert to get latest CAS associated with the key
                persistedKeys.insert(key);
                finished = (persistedOrReplicated == keySpecs->size());
            }
            break;
        case REP_AND_PERSIST:
            {
                key_spec_t key = keyspec;
                key.cas = it->cas;
                persistedKeys.insert(key);
                finished = ((persistedKeys.size() == keySpecs->size()) &&
                            (replicatedKeys.size() == keySpecs->size()));
            }
            break;
        case REP:
            break;
        }
    }
}


void SyncListener::keySynced(const key_spec_t &keyspec, uint8_t numReplicas) {
    LockHolder lh(mutex);
    std::set<key_spec_t>::iterator it = keySpecs->find(keyspec);

    if (it != keySpecs->end()) {
        uint8_t replicasDone = numReplicas;

        if (replicaCounts.find(keyspec) != replicaCounts.end()) {
            replicasDone += replicaCounts[keyspec];
        }

        replicaCounts[keyspec] = replicasDone;

        if (replicasDone >= replicasPerKey) {
            it = replicatedKeys.find(keyspec);

            if (it == replicatedKeys.end()) {
                if ((syncType == REP_OR_PERSIST) &&
                    (persistedKeys.find(keyspec) == persistedKeys.end())) {

                    ++persistedOrReplicated;
                }
            } else {
                replicatedKeys.erase(it);
            }

            // always insert to get latest CAS associated with the key
            replicatedKeys.insert(keyspec);
        }

        switch (syncType) {
        case REP:
            finished = (replicatedKeys.size() == keySpecs->size());
            break;
        case REP_OR_PERSIST:
            finished = (persistedOrReplicated == keySpecs->size());
            break;
        case REP_AND_PERSIST:
            finished = ((persistedKeys.size() == keySpecs->size()) &&
                        (replicatedKeys.size() == keySpecs->size()));
            break;
        case PERSIST:
        case MUTATION:
            break;
        }
    }
}


bool SyncListener::maybeEnableNotifyIOComplete() {
    LockHolder lh(mutex);

    if (!finished) {
        Dispatcher *d = engine.getEpStore()->getNonIODispatcher();

        d->schedule(shared_ptr<DispatcherCallback> (new SyncAbortCallback(*this)),
                    &abortTaskId, Priority::SyncAbortPriority, 0, false, true);
    }

    return (allowNotify = !finished);
}


void SyncListener::maybeNotifyIOComplete(bool timedout) {
    LockHolder lh(mutex);

    assert(finished || timedout);

    if (allowNotify) {
        assert(engine.getServerApi()->cookie->get_engine_specific(cookie) == NULL);
        engine.getServerApi()->cookie->store_engine_specific(cookie, this);
        engine.notifyIOComplete(cookie, ENGINE_SUCCESS);
        allowNotify = false;

        if (finished) {
            Dispatcher *d = engine.getEpStore()->getNonIODispatcher();

            d->cancel(abortTaskId);
        }
    }
}


void SyncListener::destroy() {
    Dispatcher *d = engine.getEpStore()->getNonIODispatcher();

    d->schedule(shared_ptr<DispatcherCallback>(new SyncDestructionCallback(this)),
                NULL, Priority::SyncDestroyPriority, 0, false, true);
}


bool SyncDestructionCallback::callback(Dispatcher &d, TaskId) {
    if (syncListener->abortTaskId) {
        d.cancel(syncListener->abortTaskId);
    }
    syncListener->engine.getServerApi()->cookie->store_engine_specific(syncListener->cookie,
                                                                       NULL);
    delete syncListener;
    return false;
}
