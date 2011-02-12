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
#ifndef SYNC_REGISTRY_HH
#define SYNC_REGISTRY_HH 1

#include <set>
#include <list>

#include "common.hh"
#include "mutex.hh"
#include "queueditem.hh"


class EventuallyPersistentEngine;
class SyncListener;

/**
 * Registers listeners for the Sync commands (persistence sync and replication sync).
 */
class SyncRegistry {
public:

    SyncRegistry() {
    }

    void addPersistenceListener(const SyncListener &syncListener);
    void itemPersisted(const QueuedItem &item);
    void itemsPersisted(std::list<QueuedItem> &itemlist);

private:

    void notifyListeners(const QueuedItem &item);

    std::list<SyncListener> persistenceListeners;
    Mutex mutex;

    DISALLOW_COPY_AND_ASSIGN(SyncRegistry);
};


class SyncListener {
public:

    SyncListener(EventuallyPersistentEngine &epEngine, const void *c,
                 const std::set<KeySpec> &keys)
        : engine(epEngine), cookie(c), keySpecs(keys), syncedKeys(0) {
    }

    bool keySynced(const KeySpec &keyspec);

private:

    EventuallyPersistentEngine   &engine;
    const void                   *cookie;
    const std::set<KeySpec>      keySpecs;
    size_t                       syncedKeys;
};


#endif /* SYNC_REGISTRY_HH */
