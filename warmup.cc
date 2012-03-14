/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc.
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
#include "warmup.hh"
#include "ep_engine.h"

const int WarmupState::Initialize = 0;
const int WarmupState::LoadingMutationLog = 1;
const int WarmupState::KeyDump = 2;
const int WarmupState::LoadingAccessLog = 3;
const int WarmupState::LoadingKVPairs = 4;
const int WarmupState::LoadingData = 5;
const int WarmupState::Done = 6;

const char *WarmupState::toString(void) const {
    return getStateDescription(state);
}

const char *WarmupState::getStateDescription(int st) const {
    switch (st) {
    case Initialize:
        return "initialize";
    case LoadingMutationLog:
        return "loading mutation log";
    case KeyDump:
        return "loading keys";
    case LoadingAccessLog:
        return "loading access log";
    case LoadingKVPairs:
        return "loading k/v pairs";
    case LoadingData:
        return "loading data";
    case Done:
        return "done";
    default:
        return "Illegal state";
    }
}

void WarmupState::transition(int to) {
    if (legalTransition(to)) {
        std::stringstream ss;
        ss << "Warmup transition from state \""
           << getStateDescription(state) << "\" to \""
           << getStateDescription(to) << "\"";
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "%s",
                         ss.str().c_str());
        state = to;
    } else {
        // Throw an exception to make it possible to test the logic ;)
        std::stringstream ss;
        ss << "Illegal state transition from \"" << *this << "\" to " << to;
        throw std::runtime_error(ss.str());
    }
}

bool WarmupState::legalTransition(int to) const {
    switch (state) {
    case Initialize:
        return to == LoadingMutationLog;
    case LoadingMutationLog:
        return (to == LoadingAccessLog ||
                to == KeyDump);
    case KeyDump:
        return (to == LoadingKVPairs ||
                to == LoadingAccessLog);
    case LoadingAccessLog:
        return (to == Done ||
                to == LoadingData);
    case LoadingKVPairs:
        return (to == Done);
    case LoadingData:
        return (to == Done);

    default:
        return false;
    }
}

std::ostream& operator <<(std::ostream &out, const WarmupState &state)
{
    out << state.toString();
    return out;
}

Warmup::Warmup(EventuallyPersistentStore *st, Dispatcher *d) :
    state(), store(st), dispatcher(d), startTime(0), metadata(0), warmup(0)
{

}

void Warmup::start(void)
{
    dispatcher->schedule(shared_ptr<WarmupStepper>(new WarmupStepper(this)),
                         &task, Priority::WarmupPriority);
}

bool Warmup::initialize(Dispatcher&, TaskId)
{
    startTime = gethrtime();
    initialVbState = store->loadVBucketState();
    transition(WarmupState::LoadingMutationLog);
    return true;
}

bool Warmup::loadingMutationLog(Dispatcher&, TaskId)
{
    if (store->warmup(initialVbState, warmup_from_mutation_log, false)) {
        transition(WarmupState::LoadingAccessLog);
    } else {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Failed to load mutation log, falling back to key dump");
        transition(WarmupState::KeyDump);
    }

    return true;
}

bool Warmup::keyDump(Dispatcher&, TaskId)
{
    if (store->warmup(initialVbState, warmup_from_key_dump, false)) {
        transition(WarmupState::LoadingAccessLog);
    } else {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Failed to dump keys, falling back to full dump");
        transition(WarmupState::LoadingKVPairs);
    }

    return true;
}

bool Warmup::loadingAccessLog(Dispatcher&, TaskId)
{
    metadata = gethrtime() - startTime;
    store->stats.warmupKeysTime.set(metadata / 1000);
    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                     "metadata loaded in %s",
                     hrtime2text(store->stats.warmupKeysTime * 1000).c_str());

    if (store->warmup(initialVbState, warmup_from_access_log, true)) {
        transition(WarmupState::Done);
    } else {
        transition(WarmupState::LoadingData);
    }
    return true;
}

bool Warmup::loadingKVPairs(Dispatcher&, TaskId)
{
    store->warmup(initialVbState, warmup_from_full_dump, false);
    transition(WarmupState::Done);
    return true;
}

bool Warmup::loadingData(Dispatcher&, TaskId)
{
    store->warmup(initialVbState, warmup_from_full_dump, true);
    transition(WarmupState::Done);
    return true;
}

bool Warmup::done(Dispatcher&, TaskId)
{
    warmup = gethrtime() - startTime;
    store->warmupCompleted();
    store->stats.warmupComplete.set(true);
    store->stats.warmupTime.set(warmup / 1000);
    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                     "warmup completed in %s",
                     hrtime2text(store->stats.warmupTime * 1000).c_str());

    return false;
}

bool Warmup::step(Dispatcher &d, TaskId t) {
    try {
        switch (state.getState()) {
        case WarmupState::Initialize:
            return initialize(d, t);
        case WarmupState::LoadingMutationLog:
            return loadingMutationLog(d, t);
        case WarmupState::KeyDump:
            return keyDump(d, t);
        case WarmupState::LoadingAccessLog:
            return loadingAccessLog(d, t);
        case WarmupState::LoadingKVPairs:
            return loadingKVPairs(d, t);
        case WarmupState::LoadingData:
            return loadingData(d, t);
        case WarmupState::Done:
            return done(d, t);
        default:
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Internal error.. Illegal warmup state %d",
                             state.getState());
            abort();
        }
    } catch(std::runtime_error &e) {
        std::stringstream ss;
        ss << "Exception in warmup loop: " << e.what() << std::endl;
        getLogger()->log(EXTENSION_LOG_WARNING, NULL, "%s",
                         ss.str().c_str());
        abort();
    }
}

void Warmup::transition(int to) {
    int old = state.getState();
    state.transition(to);
    fireStateChange(old, to);
}

void Warmup::addWarmupStateListener(WarmupStateListener *listener) {
    LockHolder lh(stateListeners.mutex);
    stateListeners.listeners.push_back(listener);
}

void Warmup::removeWarmupStateListener(WarmupStateListener *listener) {
    LockHolder lh(stateListeners.mutex);
    stateListeners.listeners.remove(listener);
}

void Warmup::fireStateChange(const int from, const int to)
{
    LockHolder lh(stateListeners.mutex);
    std::list<WarmupStateListener*>::iterator ii;
    for (ii = stateListeners.listeners.begin();
         ii != stateListeners.listeners.end();
         ++ii) {
        (*ii)->stateChanged(from, to);
    }
}

void Warmup::addStats(ADD_STAT add_stat, const void *c) const
{
    if (store->getEPEngine().getConfiguration().isWarmup()) {
        EPStats &stats = store->getEPEngine().getEpStats();
        addStat(NULL, "enabled", add_stat, c);
        const char *stateName = state.toString();
        addStat("state", stateName, add_stat, c);
        if (strcmp(stateName, "done") == 0) {
            addStat("thread", "complete", add_stat, c);
        } else {
            addStat("thread", "running", add_stat, c);
        }
        addStat("count", stats.warmedUp, add_stat, c);
        addStat("dups", stats.warmDups, add_stat, c);
        addStat("oom", stats.warmOOM, add_stat, c);
        addStat("min_memory_threshold",
                        stats.warmupMemUsedCap * 100.0, add_stat, c);
        addStat("min_item_threshold",
                        stats.warmupNumReadCap * 100.0, add_stat, c);

        if (stats.warmupKeysTime > 0) {
            addStat("keys_time", stats.warmupKeysTime,
                            add_stat, c);
        }

        if (stats.warmupComplete.get()) {
            addStat("time", stats.warmupTime,
                            add_stat, c);
        }

        if (stats.warmup.readMutationLog) {
            if (stats.warmup.corruptMutationLog) {
                addStat("mutation_log", "corrupt",
                                add_stat, c);
            } else {
                addStat("mutation_log",
                                stats.warmup.numKeysInMutationLog,
                                add_stat, c);
            }
        }


        if (stats.warmup.readAccessLog) {
            if (stats.warmup.corruptAccessLog) {
                addStat("access_log", "corrupt",
                                add_stat, c);
            } else {
                addStat("access_log",
                                stats.warmup.numKeysInAccessLog,
                                add_stat, c);
            }
        }
    } else {
        addStat(NULL, "disabled", add_stat, c);
    }
}
