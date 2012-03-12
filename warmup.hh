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
#ifndef WARMUP_HH
#define WARMUP_HH

#include "ep.hh"
#include <ostream>

class WarmupState {
public:
    static const int Initialize;
    static const int LoadingMutationLog;
    static const int KeyDump;
    static const int LoadingAccessLog;
    static const int LoadingKVPairs;
    static const int LoadingData;
    static const int Done;

    WarmupState() : state(Initialize) {}

    void transition(int to);
    const char *toString(void) const;

    int getState(void) const { return state; }

private:
    int state;
    const char *getStateDescription(int val) const;
    bool legalTransition(int to) const;
    friend std::ostream& operator<< (std::ostream& out,
                                     const WarmupState &state);
    DISALLOW_COPY_AND_ASSIGN(WarmupState);
};

class WarmupStateListener {
public:
    virtual ~WarmupStateListener() { }
    virtual void stateChanged(const int from, const int to) = 0;
};

class Warmup {
public:
    Warmup(EventuallyPersistentStore *st, Dispatcher *d);

    bool step(Dispatcher&, TaskId);
    void start(void);

    void addWarmupStateListener(WarmupStateListener *listener);
    void removeWarmupStateListener(WarmupStateListener *listener);

    const WarmupState &getState(void) const { return state; }

private:
    void fireStateChange(const int from, const int to);

    bool initialize(Dispatcher&, TaskId);
    bool loadingMutationLog(Dispatcher&, TaskId);
    bool keyDump(Dispatcher&, TaskId);
    bool loadingAccessLog(Dispatcher&, TaskId);
    bool loadingKVPairs(Dispatcher&, TaskId);
    bool loadingData(Dispatcher&, TaskId);
    bool done(Dispatcher&, TaskId);

    void transition(int to);


    WarmupState state;
    EventuallyPersistentStore *store;
    Dispatcher *dispatcher;
    TaskId task;
    hrtime_t startTime;
    hrtime_t metadata;
    hrtime_t warmup;
    // I need the initial vbstate transferred between two states :(
    std::map<std::pair<uint16_t, uint16_t>, vbucket_state>  initialVbState;

    struct {
        Mutex mutex;
        std::list<WarmupStateListener*> listeners;
    } stateListeners;

    DISALLOW_COPY_AND_ASSIGN(Warmup);
};

class WarmupStepper : public DispatcherCallback {
public:
    WarmupStepper(Warmup* w) : warmup(w) { }

    std::string description() {
        return std::string("Running a warmup loop.");
    }

    hrtime_t maxExpectedDuration() {
        // Warmup can take a while, but let's report if it runs for
        // more than ten minutes.
        return 10 * 60 * 1000 * 1000;
    }

    bool callback(Dispatcher &d, TaskId t) {
        return warmup->step(d, t);
    }

private:
    Warmup *warmup;
};

#endif
