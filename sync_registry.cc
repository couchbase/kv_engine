/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 NorthScale, Inc.
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


void SyncRegistry::addPersistenceListener(const SyncListener &syncListener) {
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
    std::list<SyncListener>::iterator it = persistenceListeners.begin();
    const KeySpec keyspec(item.getKey(), item.getVBucketId());

    while (it != persistenceListeners.end()) {
        if (it->keySynced(keyspec)) {
            it = persistenceListeners.erase(it);
        } else {
            it++;
        }
    }
}


bool SyncListener::keySynced(const KeySpec &key) {
    bool finished = false;

    if (keySpecs.find(key) != keySpecs.end()) {
        finished = (++syncedKeys == keySpecs.size());

        if (finished) {
            engine.getServerApi()->cookie->store_engine_specific(cookie, &syncedKeys);
            engine.notifyIOComplete(cookie, ENGINE_SUCCESS);
        }
    }

    return finished;
}
