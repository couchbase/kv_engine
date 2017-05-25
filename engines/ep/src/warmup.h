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

#pragma once

#include "config.h"

#include "callbacks.h"
#include "utility.h"

#include <atomic>
#include <map>
#include <ostream>
#include <string>
#include <unordered_set>
#include <vector>

#include <phosphor/phosphor.h>

class Configuration;
class EPStats;
class KVBucket;
class MutationLog;
class VBucketMap;

struct vbucket_state;

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
    std::atomic<int> state;
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
    LoadStorageKVPairCallback(KVBucket& ep,
                              bool _maybeEnableTraffic,
                              int _warmupState);

    void callback(GetValue &val);

private:
    bool shouldEject() const;

    void purge();

    VBucketMap &vbuckets;
    EPStats    &stats;
    KVBucket& epstore;
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
    Warmup(KVBucket& st, Configuration& config);

    void addToTaskSet(size_t taskId);
    void removeFromTaskSet(size_t taskId);

    ~Warmup() = default;

    void step();
    void start(void);
    void stop(void);

    void setEstimatedWarmupCount(size_t num);

    size_t getEstimatedItemCount();

    void addStats(ADD_STAT add_stat, const void *c) const;

    hrtime_t getTime(void) { return warmup; }

    void setWarmupTime(void) {
        warmup.store(gethrtime() + gethrtime_period() - startTime);
    }

    size_t doWarmup(MutationLog &lf, const std::map<uint16_t,
                    vbucket_state> &vbmap, Callback<GetValue> &cb);

    bool isComplete() { return warmupComplete.load(); }

    bool setComplete() {
        bool inverse = false;
        return warmupComplete.compare_exchange_strong(inverse, true);
    }

    bool setOOMFailure() {
        bool inverse = false;
        return warmupOOMFailure.compare_exchange_strong(inverse, true);
    }

    bool hasOOMFailure() { return warmupOOMFailure.load(); }

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

    /* Returns the number of KV stores that holds the states of all the vbuckets */
    uint16_t getNumKVStores();

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

    KVBucket& store;
    Configuration& config;

    // Unordered set to hold the current executing tasks
    std::mutex taskSetMutex;
    std::unordered_set<size_t> taskSet;

    std::atomic<hrtime_t> startTime;
    std::atomic<hrtime_t> metadata;
    std::atomic<hrtime_t> warmup;

    std::vector<std::map<uint16_t, vbucket_state>> shardVbStates;
    std::atomic<size_t> threadtask_count;
    std::vector<std::atomic<bool>> shardKeyDumpStatus;

    /// vector of vectors of VBucket IDs (one vector per shard). Each vector
    /// contains all vBucket IDs which are present for the given shard.
    std::vector<std::vector<uint16_t>> shardVbIds;

    std::atomic<hrtime_t> estimateTime;
    std::atomic<size_t> estimatedItemCount;
    bool cleanShutdown;
    bool corruptAccessLog;
    std::atomic<bool> warmupComplete;
    std::atomic<bool> warmupOOMFailure;
    std::atomic<size_t> estimatedWarmupCount;

    DISALLOW_COPY_AND_ASSIGN(Warmup);
};
