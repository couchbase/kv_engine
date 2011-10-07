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

#include "observe_registry.hh"

bool ObserveRegistry::observeKey(const std::string &key,
                                 const uint64_t cas,
                                 const uint16_t vbucket,
                                 const uint64_t expiration,
                                 const std::string &obs_set_name) {
    LockHolder rl(registry_mutex);
    std::map<std::string, ObserveSet*>::iterator obs_set = registry.find(obs_set_name);
    if (obs_set == registry.end()) {
        std::pair<std::map<std::string,ObserveSet*>::iterator,bool> res;
        res = registry.insert(std::pair<std::string,ObserveSet*>(obs_set_name,
                              new ObserveSet(expiration)));
        if (!res.second) {
            return false;
        }
        obs_set = res.first;
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Created new observe set: %s",
                         obs_set_name.c_str());
    }
    return obs_set->second->add(key, cas, vbucket);
}

void ObserveRegistry::unobserveKey(const std::string &key,
                                   const uint64_t cas,
                                   const uint16_t vbucket,
                                   const std::string &obs_set_name) {
    LockHolder rl(registry_mutex);
    std::map<std::string, ObserveSet*>::iterator obs_set = registry.find(obs_set_name);
    if (obs_set != registry.end()) {
        obs_set->second->remove(key, cas, vbucket);
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

bool ObserveSet::add(const std::string &key, uint64_t cas,
                     const uint16_t vbucket) {
    std::map<int, VBObserveSet*>::iterator obs_set = observe_set.find(vbucket);
    if (obs_set == observe_set.end()) {
        std::pair<std::map<int,VBObserveSet*>::iterator,bool> res;
        res = observe_set.insert(std::pair<int,VBObserveSet*>(vbucket,
                                 new VBObserveSet()));
        if (!res.second) {
            return false;
        }
        obs_set = res.first;
    }
    return obs_set->second->add(key, cas);
}

void ObserveSet::remove(const std::string &key, const uint64_t cas,
                        const uint16_t vbucket) {
    if (observe_set.find(vbucket) != observe_set.end()) {
        VBObserveSet *vb_observe_set = observe_set.find(vbucket)->second;
        vb_observe_set->remove(key, cas);
    }
}

state_map* ObserveSet::getState() {
    state_map *obs_state = new state_map();
    std::map<int, VBObserveSet* >::iterator itr;
    for (itr = observe_set.begin(); itr != observe_set.end(); itr++) {
        // TODO: Check if vbucket is active here
        VBObserveSet *vb_observe_set = itr->second;
        vb_observe_set->getState(obs_state);
    }
    return obs_state;
}

bool VBObserveSet::add(const std::string &key, const uint64_t cas) {
    observed_key_t obs_key(key, cas);
    std::list<observed_key_t>::iterator itr;
    for (itr = keylist.begin(); itr != keylist.end(); itr++) {
        if (itr->key.compare(key) == 0 && itr->cas == cas) {
            return true;
        }
    }
    keylist.push_back(obs_key);
    return true;
}

void VBObserveSet::remove(const std::string &key, const uint64_t cas) {
    std::list<observed_key_t>::iterator itr;
    for (itr = keylist.begin(); itr != keylist.end(); itr++) {
        observed_key_t obs_key = *itr;
        if (obs_key.key.compare(key) == 0 && obs_key.cas == cas) {
            keylist.erase(itr);
            break;
        }
    }
}

void VBObserveSet::getState(state_map *sm) {
    std::list<observed_key_t>::iterator itr;
    for (itr = keylist.begin(); itr != keylist.end(); itr++) {
        std::stringstream state_key;
        std::stringstream state_value;
        state_key << itr->key << " " << itr->cas;
        state_value << (int)itr->replicas << " none";
        (*sm)[state_key.str()] = state_value.str();
    }
}
