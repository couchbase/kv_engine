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

#ifndef OBSERVE_REGISTRY_HH
#define OBSERVE_REGISTRY_HH 1

#include <list>
#include <map>

#include "common.hh"
#include "mutex.hh"
#include "locks.hh"
#include "queueditem.hh"

typedef struct observed_key_t {
    observed_key_t(std::string aKey, uint64_t(aCas))
        : key(aKey), cas(aCas), replicas(0), mutated(false), persisted(false),
        deleted(false) {
    }

    std::string key;
    uint64_t cas;
    uint8_t replicas;
    bool mutated;
    bool persisted;
    bool deleted;
} observed_key_t;

typedef std::map<std::string, std::string> state_map;

class ObserveSet;
class VBObserveSet;

class ObserveRegistry {
public:

    ObserveRegistry(EPStats *stats_ptr)
        : stats(stats_ptr) {
    }

    bool observeKey(const std::string &key,
                    const uint64_t cas,
                    const uint16_t vbucket,
                    const uint64_t expiration,
                    const std::string &obs_set_name);

    void unobserveKey(const std::string &key,
                      const uint64_t cas,
                      const uint16_t vbucket,
                      const std::string &obs_set_name);

    state_map* getObserveSetState(const std::string &obs_set_name);

    void itemsPersisted(std::list<queued_item> &itemlist);
    void itemModified(const Item &item);
    void itemReplicated(const Item &itm);
    void itemDeleted(const std::string &key, const uint64_t cas,
                     const uint16_t vbucket);

private:

    void removeObserveSet(std::map<std::string,ObserveSet*>::iterator itr);
    ObserveSet* addObserveSet(const std::string &obs_set_name,
                              const uint16_t expiration);

    std::map<std::string,ObserveSet*> registry;
    Mutex registry_mutex;
    EPStats *stats;
};

class ObserveSet {
public:

    ObserveSet(EPStats *stats_ptr, uint32_t exp)
        : expiration(exp * ObserveSet::ONE_SECOND), stats(stats_ptr),
        lastTouched(gethrtime()) {
    }

    ~ObserveSet();

    bool add(const std::string &key, const uint64_t cas,
             const uint16_t vbucket);
    void remove(const std::string &key, const uint64_t cas,
                const uint16_t vbucket);
    void keyEvent(const std::string &key, const uint64_t,
                  const uint16_t vbucket, int event);
    bool isExpired();

    state_map* getState();

private:

    static const hrtime_t ONE_SECOND;
    const hrtime_t expiration;
    std::map<int, VBObserveSet* > observe_set;
    EPStats *stats;
    hrtime_t lastTouched;
};

class VBObserveSet {
public:

    VBObserveSet(EPStats *stats_ptr)
        : stats(stats_ptr) {
    }

    ~VBObserveSet();

    bool add(const std::string &key, const uint64_t cas);
    void remove(const std::string &key, const uint64_t cas);
    void getState(state_map* sm);
    void keyEvent(const std::string &key, const uint64_t cas,
                  int event);

private:

    std::list<observed_key_t> keylist;
    EPStats *stats;
};

#endif /* OBSERVE_REGISTRY_HH */
