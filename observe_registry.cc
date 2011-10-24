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
#include "observe_registry.hh"
#include "command_ids.h"
#include "vbucket.hh"

protocol_binary_response_status ObserveRegistry::observeKey(const std::string &key,
                                                            const uint64_t cas,
                                                            const uint16_t vbucket,
                                                            const uint64_t expiration,
                                                            const std::string &obs_set_name) {
    LockHolder rl(registry_mutex);
    std::map<std::string, ObserveSet*>::iterator itr = registry.find(obs_set_name);
    ObserveSet* obs_set;
    if (itr == registry.end()) {
        obs_set = addObserveSet(obs_set_name, expiration);
        if (obs_set == NULL) {
            return PROTOCOL_BINARY_RESPONSE_ETMPFAIL;
        }
    } else if (itr->second->isExpired()) {
        removeObserveSet(itr);
        obs_set = addObserveSet(obs_set_name, expiration);
        if (obs_set == NULL) {
            return PROTOCOL_BINARY_RESPONSE_ETMPFAIL;
        }
    } else {
        obs_set = itr->second;
    }
    return obs_set->add(key, cas, vbucket);
}

void ObserveRegistry::unobserveKey(const std::string &key,
                                   const uint64_t cas,
                                   const uint16_t vbucket,
                                   const std::string &obs_set_name) {
    LockHolder rl(registry_mutex);
    std::map<std::string, ObserveSet*>::iterator itr = registry.find(obs_set_name);
    if (itr != registry.end()) {
        if (itr->second->isExpired()) {
            removeObserveSet(itr);
        } else {
            itr->second->remove(key, cas, vbucket);
        }
    }
}

void ObserveRegistry::removeExpired() {
    LockHolder lh(registry_mutex);
    std::map<std::string, ObserveSet*>::iterator itr;
    for (itr = registry.begin(); itr != registry.end(); itr++) {
        if (itr->second->isExpired()) {
            removeObserveSet(itr);
        }
    }
}

state_map* ObserveRegistry::getObserveSetState(const std::string &obs_set_name) {
    LockHolder rl(registry_mutex);
    std::map<std::string, ObserveSet*>::iterator obs_set = registry.find(obs_set_name);
    if (obs_set == registry.end()) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "Tryed to get state for non-existent observe set %s",
                         obs_set_name.c_str());
        return new state_map;
    }
    return obs_set->second->getState();
}

void ObserveRegistry::itemsPersisted(std::list<queued_item> &itemlist) {
    LockHolder lh(registry_mutex);
    std::list<queued_item>::iterator itr;
    for (itr = itemlist.begin(); itr != itemlist.end(); itr++) {
        std::map<std::string,ObserveSet*>::iterator obs_itr;
        for (obs_itr = registry.begin(); obs_itr != registry.end(); obs_itr++) {
            if (!obs_itr->second->isExpired()) {
                obs_itr->second->keyEvent((*itr)->getKey().c_str(),
                                          (*itr)->getCas(),
                                          (*itr)->getVBucketId(),
                                          OBS_PERSISTED_EVENT);
            } else {
                removeObserveSet(obs_itr);
            }
        }
    }
}

void ObserveRegistry::itemModified(const Item &itm) {
    LockHolder lh(registry_mutex);
    std::map<std::string,ObserveSet*>::iterator itr;
    for (itr = registry.begin(); itr != registry.end(); itr++) {
        if (!itr->second->isExpired()) {
            itr->second->keyEvent(itm.getKey().c_str(), itm.getCas(),
                                  itm.getVBucketId(), OBS_MODIFIED_EVENT);
        } else {
            removeObserveSet(itr);
        }
    }
}

void ObserveRegistry::itemDeleted(const std::string &key, const uint64_t cas,
                                  const uint16_t vbucket) {
    LockHolder lh(registry_mutex);
    std::map<std::string,ObserveSet*>::iterator itr;
    for (itr = registry.begin(); itr != registry.end(); itr++) {
        if (!itr->second->isExpired()) {
            itr->second->keyEvent(key, cas, vbucket, OBS_DELETED_EVENT);
        } else {
            removeObserveSet(itr);
        }
    }
}

void ObserveRegistry::itemReplicated(const Item &itm) {
    LockHolder lh(registry_mutex);
    std::map<std::string,ObserveSet*>::iterator itr;
    for (itr = registry.begin(); itr != registry.end(); itr++) {
        if (!itr->second->isExpired()) {
            itr->second->keyEvent(itm.getKey().c_str(), itm.getCas(),
                                  itm.getVBucketId(), OBS_REPLICATED_EVENT);
        } else {
            removeObserveSet(itr);
        }
    }
}

void ObserveRegistry::removeObserveSet(std::map<std::string,ObserveSet*>::iterator itr) {
    if (itr != registry.end()) {
        delete itr->second;
        registry.erase(itr);
    }
}

ObserveSet* ObserveRegistry::addObserveSet(const std::string &obs_set_name,
                                           const uint16_t expiration) {
    std::pair<std::map<std::string,ObserveSet*>::iterator,bool> res;
    res = registry.insert(std::pair<std::string,ObserveSet*>(obs_set_name,
                              new ObserveSet(epstore, stats, expiration)));
    if (!res.second) {
        stats->obsErrors++;
        return NULL;
    }
    stats->totalObserveSets++;
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Created new observe set: %s",
                     obs_set_name.c_str());
    return res.first->second;
}

const hrtime_t ObserveSet::ONE_SECOND = 1000000000;

protocol_binary_response_status ObserveSet::add(const std::string &key, uint64_t cas,
                                                const uint16_t vbucket) {
    if ((*epstore)->getVBucket(vbucket)->getState() != vbucket_state_dead) {
        std::map<int, VBObserveSet*>::iterator obs_set = observe_set.find(vbucket);
        if (obs_set == observe_set.end()) {
            std::pair<std::map<int,VBObserveSet*>::iterator,bool> res;
            res = observe_set.insert(std::pair<int,VBObserveSet*>(vbucket,
                                     new VBObserveSet(epstore, stats)));
            if (!res.second) {
                lastTouched = gethrtime();
                stats->obsErrors++;
                return PROTOCOL_BINARY_RESPONSE_ETMPFAIL;
            }
            obs_set = res.first;
        }
        lastTouched = gethrtime();
        if (size >= MAX_OBS_SET_SIZE) {
            stats->obsErrors++;
            return PROTOCOL_BINARY_RESPONSE_EBUSY;
        } else if (obs_set->second->add(key, cas, vbucket)) {
            size++;
            return PROTOCOL_BINARY_RESPONSE_SUCCESS;
        } else {
            stats->obsErrors++;
            return PROTOCOL_BINARY_RESPONSE_ETMPFAIL;
        }
    }
    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

void ObserveSet::remove(const std::string &key, const uint64_t cas,
                        const uint16_t vbucket) {
    if (observe_set.find(vbucket) != observe_set.end()) {
        VBObserveSet *vb_observe_set = observe_set.find(vbucket)->second;
        if (vb_observe_set->remove(key, cas)) {
            size--;
        }
        lastTouched = gethrtime();
    }
}

void ObserveSet::keyEvent(const std::string &key, const uint64_t cas,
                          const uint16_t vbucket, int event) {
    std::map<int,VBObserveSet*>::iterator itr = observe_set.find(vbucket);
    if (itr != observe_set.end()) {
        itr->second->keyEvent(key, cas, event);
        lastTouched = gethrtime();
    }
}

bool ObserveSet::isExpired() {
    hrtime_t now = gethrtime();
    if ((now - lastTouched) > expiration) {
        return true;
    }
    return false;
}


state_map* ObserveSet::getState() {
    state_map *obs_state = new state_map();
    std::map<int, VBObserveSet* >::iterator itr;
    for (itr = observe_set.begin(); itr != observe_set.end(); itr++) {
        if ((*epstore)->getVBucket(itr->first)->getState() == vbucket_state_active) {
            VBObserveSet *vb_observe_set = itr->second;
            vb_observe_set->getState(obs_state);
        }
    }
    return obs_state;
}

ObserveSet::~ObserveSet() {
    std::map<int, VBObserveSet* >::iterator itr;
    for (itr = observe_set.begin(); itr != observe_set.end(); itr++) {
        size -= itr->second->size();
        delete itr->second;
        stats->totalObserveSets--;
    }
}

VBObserveSet::~VBObserveSet() {
    stats->obsRegSize -= keylist.size();
}

// Returns true if an item was added to the list
bool VBObserveSet::add(const std::string &key, const uint64_t cas,
                       const uint16_t vbucket) {
    observed_key_t obs_key(key, cas);
    std::list<observed_key_t>::iterator itr;
    for (itr = keylist.begin(); itr != keylist.end(); itr++) {
        if (itr->key.compare(key) == 0 && itr->cas == cas) {
            return true;
        }
    }
    if ((*epstore)->getVBucket(vbucket)->getState() == vbucket_state_replica) {
        obs_key.replicas = -1;
    }
    stats->obsRegSize++;
    keylist.push_back(obs_key);
    return true;
}

// Returns true if an item was removed from the list, returns false if
// the item didn't exist
bool VBObserveSet::remove(const std::string &key, const uint64_t cas) {
    std::list<observed_key_t>::iterator itr;
    for (itr = keylist.begin(); itr != keylist.end(); itr++) {
        observed_key_t obs_key = *itr;
        if (obs_key.key.compare(key) == 0 && obs_key.cas == cas) {
            stats->obsRegSize--;
            keylist.erase(itr);
            return true;
        }
    }
    return false;
}

void VBObserveSet::getState(state_map *sm) {
    std::list<observed_key_t>::iterator itr;
    for (itr = keylist.begin(); itr != keylist.end(); itr++) {
        std::stringstream state_key;
        std::stringstream state_value;
        state_key << itr->key << "," << itr->cas;
        state_value << (int)itr->replicas << ",";
        if (itr->deleted) {
            state_value << "deleted";
        }
        if (itr->mutated) {
            if (itr->deleted) {
                state_value << ",";
            }
            state_value << "mutated";
        }
        if (itr->persisted) {
            if (itr->deleted || itr->mutated) {
                state_value << ",";
            }
            state_value << "persisted";
        }
        if (!itr->persisted && !itr->mutated && !itr->deleted) {
            state_value << "none";
        }
        (*sm)[state_key.str()] = state_value.str();
    }
}

void VBObserveSet::keyEvent(const std::string &key, const uint64_t cas,
                            int event) {
    std::list<observed_key_t>::iterator itr;
    for (itr = keylist.begin(); itr != keylist.end(); itr++) {
        if (itr->key.compare(key) == 0 && event == OBS_DELETED_EVENT) {
            itr->deleted = true;
        } else if (itr->key.compare(key) == 0 && itr->cas != cas &&
                   event == OBS_MODIFIED_EVENT) {
            itr->mutated = true;
        } else if (itr->key.compare(key) == 0 && itr->cas == cas) {
            if (event == OBS_PERSISTED_EVENT) {
                itr->persisted = true;
            } else if (event == OBS_REPLICATED_EVENT) {
                itr->replicas++;
            }
        }
    }
}

bool ObserveRegistryCleaner::callback(Dispatcher &d, TaskId t) {
    ++stats.obsCleanerRuns;
    observeRegistry.removeExpired();
    d.snooze(t, sleepTime);
    return true;
}
