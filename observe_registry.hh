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

#define MAX_OBS_SET_SIZE 1000

#include <list>
#include <map>

#include "common.hh"
#include "mutex.hh"
#include "locks.hh"
#include "dispatcher.hh"
#include "queueditem.hh"

class ObserveRegistry;

#include "ep.hh"

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
class EventuallyPersistentStore;


class ObserveRegistry {
public:

    ObserveRegistry(EventuallyPersistentStore **e, EPStats *stats_ptr)
        : epstore(e), stats(stats_ptr) {
    }

    protocol_binary_response_status observeKey(const std::string &key,
                                               const uint64_t cas,
                                               const uint16_t vbucket,
                                               const uint64_t expiration,
                                               const std::string &obs_set_name);

    void unobserveKey(const std::string &key,
                      const uint64_t cas,
                      const uint16_t vbucket,
                      const std::string &obs_set_name);

    void removeExpired();

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
    EventuallyPersistentStore **epstore;
    EPStats *stats;
};

class ObserveSet {
public:

    ObserveSet(EventuallyPersistentStore **e, EPStats *stats_ptr, uint32_t exp)
        : expiration(exp * ObserveSet::ONE_SECOND), epstore(e), stats(stats_ptr),
        lastTouched(gethrtime()), size(0) {
    }

    ~ObserveSet();

    protocol_binary_response_status add(const std::string &key, const uint64_t cas,
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
    EventuallyPersistentStore **epstore;
    EPStats *stats;
    hrtime_t lastTouched;
    int size;
};

class VBObserveSet {
public:

    VBObserveSet(EventuallyPersistentStore **e, EPStats *stats_ptr)
        : epstore(e), stats(stats_ptr) {
    }

    ~VBObserveSet();

    bool add(const std::string &key, const uint64_t cas, const uint16_t vbucket);
    bool remove(const std::string &key, const uint64_t cas);
    int  size(void) { return keylist.size(); };
    void getState(state_map* sm);
    void keyEvent(const std::string &key, const uint64_t cas,
                  int event);

private:

    std::list<observed_key_t> keylist;
    EventuallyPersistentStore **epstore;
    EPStats *stats;
};

class ObserveRegistryCleaner : public DispatcherCallback {
public:

    ObserveRegistryCleaner(ObserveRegistry &o, EPStats &st, size_t sTime)
        : observeRegistry(o), stats(st), sleepTime(sTime), available(true) {
    }
    bool callback(Dispatcher &d, TaskId t);

    std::string description() {
        return std::string("Cleaning observe registry.");
    }

private:

    ObserveRegistry &observeRegistry;
    EPStats &stats;
    double sleepTime;
    bool available;
};

#endif /* OBSERVE_REGISTRY_HH */
