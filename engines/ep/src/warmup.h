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

#include "utility.h"

#include <memcached/engine_common.h>
#include <platform/atomic_duration.h>

#include <atomic>
#include <chrono>
#include <deque>
#include <iosfwd>
#include <map>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>

class Configuration;
class EPStats;
class EPBucket;
class GetValue;
class MutationLog;
class VBucketMap;
class Vbid;

struct vbucket_state;

template <typename...>
class StatusCallback;

/**
 * Class representing the current state (phase) of the warmup process.
 *
 * A normal warmup will proceed through various states, the exact sequence
 * dependinng on (amongst other things):
 * - Full vs. Value eviction.
 * - The presence of absence of an access.log file.
 * - The available memory in the bucket compared to the data size.
 */
class WarmupState {
public:
    /// Set of possible states warmup can be in.
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

    // Assignment disallowed; use transition() to modify current state.
    WarmupState& operator=(const WarmupState&) = delete;
    WarmupState& operator=(WarmupState&&) = delete;

    /**
     * Transition to the specified state. If the transition `current` -> `to`
     * is valid then changes the current state to `to`, otherwise throws
     * std::runtime_error
     * @param to The new state to move to.
     * @param allowAnystate If true, force the transition to the given state.
     */
    void transition(State to, bool allowAnystate);

    const char* toString() const;

    State getState() const {
        return state;
    }

private:
    std::atomic<State> state{State::Initialize};

    const char* getStateDescription(State val) const;

    /**
     * @returns true if the `to` state is legal transition based on the state
     *          warmup is currently in.
     */
    bool legalTransition(State to) const;

    friend std::ostream& operator<< (std::ostream& out,
                                     const WarmupState &state);
};

/**
 * The Warmup class is responsible for "warming-up" an ep-engine bucket on
 * startup to restore its state from disk.
 */
class Warmup {
public:
    Warmup(EPBucket& st, Configuration& config);

    ~Warmup();

    void start();
    void stop();

    size_t getEstimatedItemCount() const;

    void addStats(const AddStatFn& add_stat, const void* c) const;

    std::chrono::steady_clock::duration getTime() {
        return warmup.load();
    }

    void setWarmupTime() {
        std::lock_guard<std::mutex> lock(warmupStart.mutex);
        warmup.store(std::chrono::steady_clock::now() - warmupStart.time +
                     std::chrono::steady_clock::duration(1));
    }

    size_t doWarmup(MutationLog& lf,
                    const std::map<Vbid, vbucket_state>& vbmap,
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

    WarmupState::State getWarmupState() const {
        return state.getState();
    }

private:
    void addToTaskSet(size_t taskId);
    void removeFromTaskSet(size_t taskId);

    void step();

    void setEstimatedWarmupCount(size_t num);

    // Methods called by the different tasks to perform the given warmup step:

    void initialize();
    void createVBuckets(uint16_t shardId);
    void estimateDatabaseItemCount(uint16_t shardId);
    void keyDumpforShard(uint16_t shardId);
    void checkForAccessLog();
    void loadingAccessLog(uint16_t shardId);
    void loadKVPairsforShard(uint16_t shardId);
    void loadDataforShard(uint16_t shardId);
    void loadCollectionStatsForShard(uint16_t shardId);
    void done();


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

    EPBucket& store;
    Configuration& config;

    // Unordered set to hold the current executing tasks
    std::mutex taskSetMutex;
    std::unordered_set<size_t> taskSet;

    // Stores the time when the warmup process has started.
    // Lock the mutex when reading from or writing to the time member,
    // in order to synchronise access from multiple threads.
    struct {
        std::mutex mutex;
        std::chrono::steady_clock::time_point time;
    } warmupStart;

    // Time it took to load metadata and complete warmup, stored atomically.
    cb::AtomicDuration metadata;
    cb::AtomicDuration warmup;

    std::vector<std::map<Vbid, vbucket_state>> shardVbStates;
    std::atomic<size_t> threadtask_count{0};

    /// vector of vectors of VBucket IDs (one vector per shard). Each vector
    /// contains all vBucket IDs which are present for the given shard.
    std::vector<std::vector<Vbid>> shardVbIds;

    cb::AtomicDuration estimateTime;
    std::atomic<size_t> estimatedItemCount{std::numeric_limits<size_t>::max()};
    bool cleanShutdown{true};
    bool corruptAccessLog{false};
    std::atomic<bool> warmupComplete{false};
    std::atomic<bool> warmupOOMFailure{false};
    std::atomic<size_t> estimatedWarmupCount{
            std::numeric_limits<size_t>::max()};

    /// All of the cookies which need notifying when create-vbuckets is done
    std::deque<const void*> pendingSetVBStateCookies;
    /// flag to mark once warmup is passed createVbuckets
    bool createVBucketsComplete{false};
    /// A mutex which gives safe access to the cookies and state flag
    std::mutex pendingSetVBStateCookiesMutex;

    DISALLOW_COPY_AND_ASSIGN(Warmup);

    // To avoid making a number of methods on Warmup public; grant friendship
    // to the various Tasks which run the stages of warmup.
    friend class WarmupInitialize;
    friend class WarmupCreateVBuckets;
    friend class WarmupEstimateDatabaseItemCount;
    friend class WarmupKeyDump;
    friend class WarmupCheckforAccessLog;
    friend class WarmupLoadAccessLog;
    friend class WarmupLoadingKVPairs;
    friend class WarmupLoadingData;
    friend class WarmupLoadingCollectionCounts;
    friend class WarmupCompletion;
};
