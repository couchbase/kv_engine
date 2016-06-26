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
#ifndef SRC_WARMUP_H_
#define SRC_WARMUP_H_ 1

#include "config.h"

#include <list>
#include <map>
#include <ostream>
#include <string>
#include <vector>

#include "ep_engine.h"

class WarmupState {
public:
    static const int Initialize;
    static const int CreateVBuckets;
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
    AtomicValue<int> state;
    const char *getStateDescription(int val) const;
    bool legalTransition(int to) const;
    friend std::ostream& operator<< (std::ostream& out,
                                     const WarmupState &state);
    DISALLOW_COPY_AND_ASSIGN(WarmupState);
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
        cb_assert(epstore);
    }

    void callback(GetValue &val);

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

class LoadValueCallback : public Callback<CacheLookup> {
public:
    LoadValueCallback(VBucketMap& vbMap, int _warmupState) :
        vbuckets(vbMap), warmupState(_warmupState) { }

    void callback(CacheLookup &lookup);

private:
    VBucketMap &vbuckets;
    int         warmupState;
};


class Warmup {
public:
    Warmup(EventuallyPersistentStore *st);

    ~Warmup();

    void step();
    void start(void);
    void stop(void);

    void setEstimatedWarmupCount(size_t num);

    size_t getEstimatedItemCount();

    void addStats(ADD_STAT add_stat, const void *c) const;

    hrtime_t getTime(void) { return warmup; }

    void setWarmupTime(void) {
        warmup.store(gethrtime() - startTime);
    }

    size_t doWarmup(MutationLog &lf, const std::map<uint16_t,
                    vbucket_state> &vbmap, Callback<GetValue> &cb);

    bool isComplete() { return warmupComplete.load(); }

    bool setComplete() {
        bool inverse = false;
        return warmupComplete.compare_exchange_strong(inverse, true);
    }

    void initialize();
    void createVBuckets(uint16_t shardId);
    void estimateDatabaseItemCount(uint16_t shardId);
    void keyDumpforShard(uint16_t shardId);
    void checkForAccessLog();
    void loadingAccessLog(uint16_t shardId);
    void loadKVPairsforShard(uint16_t shardId);
    void loadDataforShard(uint16_t shardId);
    void done();

private:
    template <typename T>
    void addStat(const char *nm, const T &val, ADD_STAT add_stat, const void *c) const;

    void fireStateChange(const int from, const int to);

    void populateShardVbStates();

    void scheduleInitialize();
    void scheduleCreateVBuckets();
    void scheduleEstimateDatabaseItemCount();
    void scheduleKeyDump();
    void scheduleCheckForAccessLog();
    void scheduleLoadingAccessLog();
    void scheduleLoadingKVPairs();
    void scheduleLoadingData();
    void scheduleCompletion();

    void transition(int to, bool force=false);

    WarmupState state;

    EventuallyPersistentStore *store;
    size_t taskId;
    hrtime_t startTime;
    AtomicValue<hrtime_t> metadata;
    AtomicValue<hrtime_t> warmup;

    std::vector<vbucket_state *> allVbStates;
    std::map<uint16_t, vbucket_state> *shardVbStates;
    AtomicValue<size_t> threadtask_count;
    bool *shardKeyDumpStatus;
    std::vector<uint16_t> *shardVbIds;

    AtomicValue<hrtime_t> estimateTime;
    AtomicValue<size_t> estimatedItemCount;
    bool cleanShutdown;
    bool corruptAccessLog;
    AtomicValue<bool> warmupComplete;
    AtomicValue<size_t> estimatedWarmupCount;

    DISALLOW_COPY_AND_ASSIGN(Warmup);
};

class WarmupInitialize : public GlobalTask {
public:
    WarmupInitialize(EventuallyPersistentStore &st,
                     Warmup *w) :
        GlobalTask(&st.getEPEngine(), TaskId::WarmupInitialize, 0, false),
        _warmup(w) {}

    std::string getDescription() {
        std::stringstream ss;
        ss<<"Warmup - initialize";
        return ss.str();
    }

    bool run() {

        _warmup->initialize();
        return false;
    }

private:
    Warmup* _warmup;
};

class WarmupCreateVBuckets : public GlobalTask {
public:
    WarmupCreateVBuckets(EventuallyPersistentStore &st,
                         uint16_t sh, Warmup *w):
        GlobalTask(&st.getEPEngine(), TaskId::WarmupCreateVBuckets, 0, false),
        _shardId(sh),
        _warmup(w) {}

    std::string getDescription() {
        std::stringstream ss;
        ss<<"Warmup - creating vbuckets: shard "<<_shardId;
        return ss.str();
    }

    bool run() {
        _warmup->createVBuckets(_shardId);
        return false;
    }

private:
    uint16_t _shardId;
    Warmup* _warmup;
};

class WarmupEstimateDatabaseItemCount : public GlobalTask {
public:
    WarmupEstimateDatabaseItemCount(EventuallyPersistentStore &st,
                                    uint16_t sh, Warmup *w):
        GlobalTask(&st.getEPEngine(), TaskId::WarmupEstimateDatabaseItemCount, 0, false),
        _shardId(sh),
        _warmup(w) {}

    std::string getDescription() {
        std::stringstream ss;
        ss<<"Warmup - estimate item count: shard "<<_shardId;
        return ss.str();
    }

    bool run() {

        _warmup->estimateDatabaseItemCount(_shardId);
        return false;
    }

private:
    uint16_t _shardId;
    Warmup* _warmup;
};

class WarmupKeyDump : public GlobalTask {
public:
    WarmupKeyDump(EventuallyPersistentStore &st, Warmup *w, uint16_t sh) :
        GlobalTask(&st.getEPEngine(), TaskId::WarmupKeyDump, 0, false),
        _shardId(sh),
        _warmup(w) {}

    std::string getDescription() {
        std::stringstream ss;
        ss<<"Warmup - key dump: shard "<<_shardId;
        return ss.str();
    }

    bool run() {
        _warmup->keyDumpforShard(_shardId);
        return false;
    }

private:
    uint16_t _shardId;
    Warmup* _warmup;
};

class WarmupCheckforAccessLog : public GlobalTask {
public:
    WarmupCheckforAccessLog(EventuallyPersistentStore &st,
                            Warmup *w) :
        GlobalTask(&st.getEPEngine(), TaskId::WarmupCheckforAccessLog, 0, false),
        _warmup(w) {}

    std::string getDescription() {
        std::stringstream ss;
        ss<<"Warmup - check for access log";
        return ss.str();
    }

    bool run() {

        _warmup->checkForAccessLog();
        return false;
    }

private:
    Warmup* _warmup;
};

class WarmupLoadAccessLog : public GlobalTask {
public:
    WarmupLoadAccessLog(EventuallyPersistentStore &st, Warmup *w, uint16_t sh) :
        GlobalTask(&st.getEPEngine(), TaskId::WarmupLoadAccessLog, 0, false),
        _warmup(w), _shardId(sh) {}

    std::string getDescription() {
        std::stringstream ss;
        ss<<"Warmup - loading access log: shard "<<_shardId;
        return ss.str();
    }

    bool run() {

        _warmup->loadingAccessLog(_shardId);
        return false;
    }

private:
    Warmup* _warmup;
    uint16_t _shardId;
};

class WarmupLoadingKVPairs : public GlobalTask {
public:
    WarmupLoadingKVPairs(EventuallyPersistentStore &st, Warmup *w, uint16_t sh) :
        GlobalTask(&st.getEPEngine(), TaskId::WarmupLoadingKVPairs, 0, false),
        _shardId(sh),
        _warmup(w) {}

    std::string getDescription() {
        std::stringstream ss;
        ss<<"Warmup - loading KV Pairs: shard "<<_shardId;
        return ss.str();
    }

    bool run() {

        _warmup->loadKVPairsforShard(_shardId);
        return false;
    }

private:
    uint16_t _shardId;
    Warmup* _warmup;
};

class WarmupLoadingData : public GlobalTask {
public:
    WarmupLoadingData(EventuallyPersistentStore &st, Warmup *w, uint16_t sh) :
        GlobalTask(&st.getEPEngine(), TaskId::WarmupLoadingData, 0, false),
        _shardId(sh),
        _warmup(w) {}

    std::string getDescription() {
        std::stringstream ss;
        ss<<"Warmup - loading data: shard "<<_shardId;
        return ss.str();
    }

    bool run() {

        _warmup->loadDataforShard(_shardId);
        return false;
    }

private:
    uint16_t _shardId;
    Warmup* _warmup;
};

class WarmupCompletion : public GlobalTask {
public:
    WarmupCompletion(EventuallyPersistentStore &st,
                     Warmup *w) :
        GlobalTask(&st.getEPEngine(), TaskId::WarmupCompletion, 0, false),
        _warmup(w) {}

    std::string getDescription() {
        std::stringstream ss;
        ss<<"Warmup - completion";
        return ss.str();
    }

    bool run() {

        _warmup->done();
        return false;
    }

private:
    Warmup* _warmup;
};

#endif  // SRC_WARMUP_H_
