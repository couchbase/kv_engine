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

#include <memcached/engine_common.h>
#include <phosphor/phosphor.h>
#include <platform/atomic_duration.h>
#include <platform/processclock.h>

#include <atomic>
#include <deque>
#include <map>
#include <ostream>
#include <string>
#include <unordered_set>
#include <vector>

class Configuration;
class EPStats;
class KVBucket;
class MutationLog;
class VBucketMap;

struct vbucket_state;

class WarmupState {
public:
    enum class State {
        Initialize,
        CreateVBuckets,
        EstimateDatabaseItemCount,
        KeyDump,
        LoadingAccessLog,
        CheckForAccessLog,
        LoadingKVPairs,
        LoadingData,
        LoadingCollectionCounts,
        Done
    };

    WarmupState() : state(State::Initialize) {
    }

    void transition(State to, bool allowAnystate);
    const char *toString(void) const;

    State getState(void) const {
        return state;
    }

private:
    std::atomic<State> state;
    const char* getStateDescription(State val) const;
    bool legalTransition(State to) const;
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
class LoadStorageKVPairCallback : public StatusCallback<GetValue> {
public:
    LoadStorageKVPairCallback(KVBucket& ep,
                              bool maybeEnableTraffic,
                              WarmupState::State warmupState);

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
    WarmupState::State warmupState;
};

class LoadValueCallback : public StatusCallback<CacheLookup> {
public:
    LoadValueCallback(VBucketMap& vbMap, WarmupState::State warmupState)
        : vbuckets(vbMap), warmupState(warmupState) {
    }

    void callback(CacheLookup &lookup);

private:
    VBucketMap &vbuckets;
    WarmupState::State warmupState;
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

    ProcessClock::duration getTime() {
        return warmup.load();
    }

    void setWarmupTime() {
        std::lock_guard<std::mutex> lock(warmupStart.mutex);
        warmup.store(ProcessClock::now() - warmupStart.time +
                     ProcessClock::duration(1));
    }

    size_t doWarmup(MutationLog& lf,
                    const std::map<uint16_t, vbucket_state>& vbmap,
                    StatusCallback<GetValue>& cb);

    bool isComplete() const {
        return warmupComplete.load();
    }

    bool setComplete() {
        bool inverse = false;
        return warmupComplete.compare_exchange_strong(inverse, true);
    }

    /**
     * Method checks with if a setVBState should block. setVBState should be
     * blocked until warmup has processed any existing vb state and completed
     * initialisation of the vbMap from disk data.
     *
     * @param cookie the callers cookie for later notification.
     * @return true if setVBState should return EWOULDBLOCK
     */
    bool shouldSetVBStateBlock(const void* cookie);

    /**
     * Perform any notifications to any pending setVBState operations and mark
     * that vbucket creation is complete.
     */
    void processCreateVBucketsComplete();

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
    void loadCollectionCountsForShard(uint16_t shardId);
    void done();

private:
    template <typename T>
    void addStat(const char *nm, const T &val, ADD_STAT add_stat, const void *c) const;

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
    void scheduleLoadingCollectionCounts();

    void transition(WarmupState::State to, bool force = false);

    WarmupState state;

    KVBucket& store;
    Configuration& config;

    // Unordered set to hold the current executing tasks
    std::mutex taskSetMutex;
    std::unordered_set<size_t> taskSet;

    // Stores the time when the warmup process has started.
    // Lock the mutex when reading from or writing to the time member,
    // in order to synchronise access from multiple threads.
    struct {
        std::mutex mutex;
        ProcessClock::time_point time;
    } warmupStart;

    // Time it took to load metadata and complete warmup, stored atomically.
    cb::AtomicDuration metadata;
    cb::AtomicDuration warmup;

    std::vector<std::map<uint16_t, vbucket_state>> shardVbStates;
    std::atomic<size_t> threadtask_count;
    std::vector<std::atomic<bool>> shardKeyDumpStatus;

    /// vector of vectors of VBucket IDs (one vector per shard). Each vector
    /// contains all vBucket IDs which are present for the given shard.
    std::vector<std::vector<uint16_t>> shardVbIds;

    cb::AtomicDuration estimateTime;
    std::atomic<size_t> estimatedItemCount;
    bool cleanShutdown;
    bool corruptAccessLog;
    std::atomic<bool> warmupComplete;
    std::atomic<bool> warmupOOMFailure;
    std::atomic<size_t> estimatedWarmupCount;

    /// All of the cookies which need notifying when create-vbuckets is done
    std::deque<const void*> pendingSetVBStateCookies;
    /// flag to mark once warmup is passed createVbuckets
    bool createVBucketsComplete;
    /// A mutex which gives safe access to the cookies and state flag
    std::mutex pendingSetVBStateCookiesMutex;

    DISALLOW_COPY_AND_ASSIGN(Warmup);
};
