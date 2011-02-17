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
    LockHolder lh(mutex);
    persistenceListeners.push_back(syncListener);
}


void SyncRegistry::itemPersisted(const QueuedItem &item) {
    LockHolder lh(mutex);
    notifyListeners(item);
}


void SyncRegistry::itemsPersisted(std::list<QueuedItem> &itemlist) {
    std::list<QueuedItem>::iterator it = itemlist.begin();
    LockHolder lh(mutex);

    for ( ; it != itemlist.end(); it++) {
        notifyListeners(*it);
    }
}


void SyncRegistry::notifyListeners(const QueuedItem &item) {
    std::list<SyncListener*>::iterator it = persistenceListeners.begin();
    key_spec_t keyspec = { 0, item.getVBucketId(), item.getKey() };

    while (it != persistenceListeners.end()) {
        SyncListener *listener = *it;
        if (listener->keySynced(keyspec)) {
            it = persistenceListeners.erase(it);
        } else {
            it++;
        }
    }
}


SyncListener::SyncListener(EventuallyPersistentEngine &epEngine,
                           const void *c,
                           const std::set<key_spec_t> &keys,
                           sync_type_t sync_type,
                           uint8_t replicaCount) :
    engine(epEngine), cookie(c), keySpecs(keys),
    syncType(sync_type), replicas(replicaCount) {

    // TODO: support mutation and replication sync
    assert(syncType == PERSIST);
}


bool SyncListener::keySynced(key_spec_t &key) {
    bool finished = false;
    std::set<key_spec_t>::iterator it = keySpecs.find(key);

    if (it != keySpecs.end()) {
        key.cas = it->cas;
        persistedKeys.insert(key);
        finished = (persistedKeys.size() == keySpecs.size());

        if (finished) {
            engine.getServerApi()->cookie->store_engine_specific(cookie, this);
            engine.notifyIOComplete(cookie, ENGINE_SUCCESS);
        }
    }

    return finished;
}
