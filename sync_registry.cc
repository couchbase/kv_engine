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


void SyncRegistry::addPersistenceListener(SyncListener *syncListener) {
    LockHolder lh(persistenceMutex);
    persistenceListeners.push_back(syncListener);
}


void SyncRegistry::itemPersisted(const QueuedItem &item) {
    key_spec_t keyspec(item);
    LockHolder lh(persistenceMutex);
    notifyListeners(persistenceListeners, keyspec, false);
}


void SyncRegistry::itemsPersisted(std::list<QueuedItem> &itemlist) {
    LockHolder lh(persistenceMutex);
    std::list<QueuedItem>::iterator it = itemlist.begin();

    for ( ; it != itemlist.end(); it++) {
        key_spec_t keyspec(0, it->getVBucketId(), it->getKey());
        notifyListeners(persistenceListeners, keyspec, false);
    }
}


void SyncRegistry::addMutationListener(SyncListener *syncListener) {
    LockHolder lh(mutationMutex);
    mutationListeners.push_back(syncListener);
}


void SyncRegistry::itemModified(const key_spec_t &keyspec) {
    LockHolder lh(mutationMutex);
    notifyListeners(mutationListeners, keyspec, false);
}


void SyncRegistry::itemDeleted(const key_spec_t &keyspec) {
    LockHolder lh(mutationMutex);
    notifyListeners(mutationListeners, keyspec, true);
}


void SyncRegistry::notifyListeners(std::list<SyncListener*> &listeners,
                                   const key_spec_t &keyspec,
                                   bool deleted) {
    std::list<SyncListener*>::iterator it = listeners.begin();

    while (it != listeners.end()) {
        SyncListener *listener = *it;
        if (listener->keySynced(keyspec, deleted)) {
            it = listeners.erase(it);
        } else {
            it++;
        }
    }
}


SyncListener::SyncListener(EventuallyPersistentEngine &epEngine,
                           const void *c,
                           std::set<key_spec_t> *keys,
                           sync_type_t sync_type,
                           uint8_t replicaCount) :
    engine(epEngine), cookie(c), keySpecs(keys),
    syncType(sync_type), replicas(replicaCount) {

    // TODO: support replication sync, replication and persistence sync, and
    //       replicator or persistence sync
    assert(syncType == PERSIST || syncType == MUTATION);
}


SyncListener::~SyncListener() {
    delete keySpecs;
}


bool SyncListener::keySynced(const key_spec_t &keyspec, bool deleted) {
    bool finished = false;
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
        case REP:
            // TODO
            break;
        case REP_OR_PERSIST:
            // TODO
            break;
        case REP_AND_PERSIST:
            // TODO
            break;
        }

        if (finished) {
            engine.getServerApi()->cookie->store_engine_specific(cookie, this);
            engine.notifyIOComplete(cookie, ENGINE_SUCCESS);
        }
    }

    return finished;
}
