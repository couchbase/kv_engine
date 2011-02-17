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
#ifndef SYNC_REGISTRY_HH
#define SYNC_REGISTRY_HH 1

#include <set>
#include <list>

#include "common.hh"
#include "mutex.hh"
#include "queueditem.hh"

typedef struct key_spec_t {
    uint64_t cas;
    uint16_t vbucketid;
    std::string key;
    bool operator<(const key_spec_t &other) const {
        return (key < other.key) || ((key == other.key) && (vbucketid < other.vbucketid));
    }
} key_spec_t;

typedef enum {
    PERSIST,
    MUTATION,
    REP,
    REP_OR_PERSIST,
    REP_AND_PERSIST
} sync_type_t;


class EventuallyPersistentEngine;
class SyncListener;

/**
 * Registers listeners for the Sync commands (persistence sync and replication sync).
 */
class SyncRegistry {
public:

    SyncRegistry() {
    }

    void addPersistenceListener(SyncListener *syncListener);
    void itemPersisted(const QueuedItem &item);
    void itemsPersisted(std::list<QueuedItem> &itemlist);

private:

    void notifyListeners(const QueuedItem &item);

    std::list<SyncListener*> persistenceListeners;
    Mutex mutex;

    DISALLOW_COPY_AND_ASSIGN(SyncRegistry);
};


class SyncListener {
public:

    SyncListener(EventuallyPersistentEngine &epEngine,
                 const void *c,
                 const std::set<key_spec_t> &keys,
                 sync_type_t sync_type,
                 uint8_t replicaCount = 0);

    bool keySynced(key_spec_t &keyspec);

    sync_type_t getSyncType() const {
        return syncType;
    }

    std::set<key_spec_t>& getPersistedKeys() {
        return persistedKeys;
    }

private:

    EventuallyPersistentEngine   &engine;
    const void                   *cookie;
    std::set<key_spec_t>         keySpecs;
    sync_type_t                  syncType;
    uint8_t                      replicas;
    std::set<key_spec_t>         persistedKeys;
};


#endif /* SYNC_REGISTRY_HH */
