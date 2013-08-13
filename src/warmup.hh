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

#include "ep_engine.h"
#include <ostream>

class WarmupState {
public:
    static const int Initialize;
    static const int LoadingMutationLog;
    static const int EstimateDatabaseItemCount;
    static const int KeyDump;
    static const int LoadingAccessLog;
    static const int CheckForAccessLog;
    static const int LoadingKVPairs;
    static const int LoadingData;
    static const int Done;

    WarmupState() : state(Initialize) {}

    void transition(int to, bool allowAnystate);
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


//////////////////////////////////////////////////////////////////////////////
//                                                                          //
//    Helper class used to insert data into the epstore                     //
//                                                                          //
//////////////////////////////////////////////////////////////////////////////

/**
 * Helper class used to insert items into the storage by using
 * the KVStore::dump method to load items from the database
 */
class LoadStorageKVPairCallback : public Callback<GetValue> {
public:
    LoadStorageKVPairCallback(EventuallyPersistentStore *ep,
                              bool _maybeEnableTraffic, int _warmupState)
        : vbuckets(ep->vbMap), stats(ep->getEPEngine().getEpStats()),
          epstore(ep), startTime(ep_real_time()),
          hasPurged(false), maybeEnableTraffic(_maybeEnableTraffic),
          warmupState(_warmupState)
    {
        assert(epstore);
    }

    void initVBucket(uint16_t vbid,
                     const vbucket_state &vbstate);

    void callback(GetValue &val);
    bool isLoaded(const char* buf, size_t size, uint16_t vbid);

private:

    bool shouldEject() {
        return stats.getTotalMemoryUsed() >= stats.mem_low_wat;
    }

    void purge();

    VBucketMap &vbuckets;
    EPStats    &stats;
    EventuallyPersistentStore *epstore;
    time_t      startTime;
    bool        hasPurged;
    bool        maybeEnableTraffic;
    int         warmupState;
};


class Warmup {
public:
    Warmup(EventuallyPersistentStore *st, Dispatcher *d);

    bool step(Dispatcher&, TaskId &);
    void start(void);
    void stop(void);

    void addWarmupStateListener(WarmupStateListener *listener);
    void removeWarmupStateListener(WarmupStateListener *listener);

    const WarmupState &getState(void) const { return state; }

    void setEstimatedItemCount(size_t num);

    void setEstimatedWarmupCount(size_t num);

    void addStats(ADD_STAT add_stat, const void *c) const;

    void setReconstructLog(bool val);

    bool doReconstructLog(void) const { return reconstructLog; }

    hrtime_t getTime(void) { return warmup; }

    size_t doWarmup(MutationLog &lf, const std::map<uint16_t,
                    vbucket_state> &vbmap, Callback<GetValue> &cb);

private:
    template <typename T>
    void addStat(const char *nm, T val, ADD_STAT add_stat, const void *c) const;

    void fireStateChange(const int from, const int to);

    bool initialize(Dispatcher&, TaskId &);
    bool loadingMutationLog(Dispatcher&, TaskId &);
    bool estimateDatabaseItemCount(Dispatcher&, TaskId &);
    bool keyDump(Dispatcher&, TaskId &);
    bool loadingAccessLog(Dispatcher&, TaskId &);
    bool checkForAccessLog(Dispatcher&, TaskId &);
    bool loadingKVPairs(Dispatcher&, TaskId &);
    bool loadingData(Dispatcher&, TaskId &);
    bool done(Dispatcher&, TaskId &);

    void transition(int to, bool force=false);


    LoadStorageKVPairCallback *createLKVPCB(const std::map<uint16_t, vbucket_state> &st,
                                            bool maybeEnable, int warmupState);

    WarmupState state;
    EventuallyPersistentStore *store;
    Dispatcher *dispatcher;
    TaskId task;
    hrtime_t startTime;
    hrtime_t metadata;
    hrtime_t warmup;
    // I need the initial vbstate transferred between two states :(
    std::map<uint16_t, vbucket_state>  initialVbState;
    // True if a mutation log should be reconstructed at warmup
    bool reconstructLog;

    hrtime_t estimateTime;
    size_t estimatedItemCount;
    bool corruptMutationLog;
    bool corruptAccessLog;
    size_t estimatedWarmupCount;

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

    bool callback(Dispatcher &d, TaskId &t) {
        return warmup->step(d, t);
    }

private:
    Warmup *warmup;
};

#endif
