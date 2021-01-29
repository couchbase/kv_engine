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

#include "utility.h"
#include "vbucket_fwd.h"

#include <folly/AtomicHashMap.h>
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
        LoadingCollectionCounts,
        EstimateDatabaseItemCount,
        LoadPreparedSyncWrites,
        PopulateVBucketMap,
        KeyDump,
        LoadingAccessLog,
        CheckForAccessLog,
        LoadingKVPairs,
        LoadingData,
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
 *
 * Warmup is a multi stage process, with the exact sequence of stages depending
 * on:
 * a) Eviction mode (full or value)
 * b) Presence of access.log files
 * c) Size of bucket quota vs size of data to load.
 *
 * The possible state transitions are:
 *
 *                [Initialise]
 *                     |
 *                     V
 *              [CreateVBuckets]
 *                     |
 *                     V
 *           [LoadingCollectionCounts]
 *                     |
 *                     V
 *          [EstimateDatabaseItemCount]
 *                     |
 *                     V
 *          [LoadPreparedSyncWrites]
 *                     |
 *                     V
 *            [PopulateVBucketMap]
 *                     |
 *                Eviction mode?
 *               /           \
 *            Value          Full
 *              |             |
 *              V             |
 *          [KeyDump]         |
 *              |             |
 *              V             V
 *            [CheckForAccessLog]
 *                     |
 *              Access Log Found?
 *               /              \
 *             Yes              No - Eviction mode?
 *              |                   /            \
 *              |                 Value          Full
 *              |                  |              |
 *              V                  |              V
 *       [LoadingAccessLog]        |       [LoadingKVPairs]
 *              |                  |              |
 *     maybe Enable Traffic?       |              |
 *     /                  \        |              |
 *    Yes                 No       |              |
 *     |                  |        |              |
 *     |                  V        V              |
 *     |                [LoadingData]             |
 *     |                     |                    |
 *     \---------------------+--------------------/
 *                           |
 *                           V
 *                        [Done]
 *
 * KV-engine has the following behaviour as warmup runs.
 *
 * Whilst the following phases are incomplete:
 *
 *    Initialise
 *    CreateVBuckets
 *    LoadingCollectionCounts
 *    EstimateDatabaseItemCount
 *    LoadPreparedSyncWrites
 *    PopulateVBucketMap
 *
 *  1) setVBucket requests are queued (using the EWOULDBLOCK mechanism)
 *  2) The VBucket map is empty. No operation can find a VBucket object to
 *     operate on. For CRUD operations externally clients will see
 *     'temporary_failure' instead of 'not_my_vbucket'.
 *  3) DCP Consumers cannot be created.
 *  4) DCP Producers can be created, but stream-request will fail with
 *     'not_my_vbucket'.
 *
 * On completion of PopulateVBucketMap:
 *
 *  1) All queued setVBucket requests are notified and all new setVBucket
 *     requests can be processed.
 *  2) The VBucket map is populated and operations can now find VBucket objects.
 *     * DCP producer stream-requests can now be processed.
 *     * Value Eviction buckets all CRUD operations will return
 *       'temporary_failure'.
 *     * Full Eviction buckets:
 *       a) Create and update operations return 'temporary_failure'.
 *       b) Read operations are processed. Note that read of a non-existent key
 *          results in a return code of 'temporary_failure' (MB-34909)
 *       c) Delete operations are processed,  Note that delete of a
 *          non-existent key results in a return code of 'temporary_failure'
 *          (MB-34909)
 *
 * At this point the above behaviour remains in place until warmup is considered
 * complete. Warmup completing happens in a number of the tasks.
 *
 *    KeyDump
 *    LoadingAccessLog
 *    LoadingKVPairs
 *    Done
 *
 * Note that once warmup is complete (Warmup::isComplete() returns true) the
 * warmup will conclude the phase and short-cut to Done.
 * When Warmup::isComplete() returns true:
 *  1) all CRUD operations are fully processed.
 *  2) DCP consumers can be created.
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
     * This method store the given cookie for later notification iff Warmup has
     * yet to reach and complete the PopulateVBucketMap phase.
     *
     * @param cookie the callers cookie which might be stored for later
     *        notification (see return value)
     * @return true if the cookie was stored for later notification, false if
     *         not.
     */
    bool maybeWaitForVBucketWarmup(const void* cookie);

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

    /**
     * Testing hook which if set is called every time warmup transitions to
     * a new state.
     */
    std::function<void(WarmupState::State)> stateTransitionHook;

private:
    void addToTaskSet(size_t taskId);
    void removeFromTaskSet(size_t taskId);

    void step();

    void setEstimatedWarmupCount(size_t num);

    /*
     * Methods called by the different tasks to perform the given warmup stage.
     *
     * See `Warmup` class-level comment for flowchart of these stages.
     */

    /**
     * Initialises warmup:
     * - Determines if this was a clean shutdown (based on last persisted stats)
     * - Scans the data directory to determine which vBuckets exist on-disk,
     *   from that populating Warmup::shardVbIds vector of shards to vbucket
     *   stats to warmup.
     */
    void initialize();

    /**
     * Creates VBucket objects in memory for the given shard:
     * - For each vbucket found on disk; create an in-memory VBucket object
     *   from the on-disk state.
     */
    void createVBuckets(uint16_t shardId);

    /**
     * Loads the persisted per-collection document count for each vBucket in
     * the given shard.
     */
    void loadCollectionStatsForShard(uint16_t shardId);

    /**
     * Loads the item count of each vBucket from disk for the given shardId:
     * - Reads the item count from disk and sets VBucket::numTotalItems
     * - Updates Warmup::estimatedItemCount with the estimated total items
     *   needed for warmup.
     */
    void estimateDatabaseItemCount(uint16_t shardId);

    /**
     * Loads all prepared SyncWrites for each vBucket in the given shard
     * - Performs a KVStore scan against the DurabilityPrepare namespace,
     *   loading all found documents into memory.
     */
    void loadPreparedSyncWrites(uint16_t shardId);

    /**
     * Adds all warmed up vbuckets (for the shard) to the bucket's VBMap, once
     * added to the VBMap the rest of the system will be able to locate and
     * operate on the VBucket, so this phase must only run once each vbucket is
     * completely initialised.
     * @param shardId The shard for which population should occur
     */
    void populateVBucketMap(uint16_t shardId);

    /**
     * [Value-eviction only]
     * Loads all keys into memory for each vBucket in the given shard.
     */
    void keyDumpforShard(uint16_t shardId);

    /**
     * Checks for the existance of an access log file for each shard:
     * - Checks if traffic should be enabled (i.e. enough data already
     *   loaded) - if true transitions to State::Done
     * - Checks for the existance of access logs for all shards.
     */
    void checkForAccessLog();

    /**
     * Loads the access log for the given shardId:
     * - Reads a batch of keys from the access log
     * - For each key read, attempt to fetch key+value from the underlying
     *   KVStore.
     * - If key exists (wasn't subsequently deleted), insert into the
     *   HashTable.
     */
    void loadingAccessLog(uint16_t shardId);

    /**
     * [Full-eviction only]
     * Loads both keys and values into memory for each vBucket in the given
     * shard.
     */
    void loadKVPairsforShard(uint16_t shardId);

    /**
     * Loads values into memory for each vBucket in the given shard.
     */
    void loadDataforShard(uint16_t shardId);

    /* Terminal state of warmup. Updates statistics and marks warmup as
     * completed
     */
    void done();

    /* Returns the number of KV stores that holds the states of all the vbuckets */
    uint16_t getNumKVStores();

    void populateShardVbStates();

    void scheduleInitialize();
    void scheduleCreateVBuckets();
    void scheduleLoadingCollectionCounts();
    void scheduleEstimateDatabaseItemCount();
    void scheduleLoadPreparedSyncWrites();
    void schedulePopulateVBucketMap();
    void scheduleKeyDump();
    void scheduleCheckForAccessLog();
    void scheduleLoadingAccessLog();
    void scheduleLoadingKVPairs();
    void scheduleLoadingData();
    void scheduleCompletion();

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
    cb::AtomicDuration<> metadata;
    cb::AtomicDuration<> warmup;

    std::vector<std::map<Vbid, vbucket_state>> shardVbStates;
    std::atomic<size_t> threadtask_count{0};

    /// vector of vectors of VBucket IDs (one vector per shard). Each vector
    /// contains all vBucket IDs which are present for the given shard.
    std::vector<std::vector<Vbid>> shardVbIds;

    cb::AtomicDuration<> estimateTime;
    std::atomic<size_t> estimatedItemCount{std::numeric_limits<size_t>::max()};
    bool cleanShutdown{true};
    bool corruptAccessLog{false};
    std::atomic<bool> warmupComplete{false};
    std::atomic<bool> warmupOOMFailure{false};
    std::atomic<size_t> estimatedWarmupCount{
            std::numeric_limits<size_t>::max()};

    /// All of the cookies which need notifying when create-vbuckets is done
    std::deque<const void*> pendingCookies;
    /// flag to mark once warmup is passed createVbuckets
    bool createVBucketsComplete{false};
    /// A mutex which gives safe access to the cookies and state flag
    std::mutex pendingCookiesMutex;

    /**
     * Any vbucket found in the CreateVBuckets phase are added here and then
     * removed at the PopulateVBucketMap phase
     */
    folly::AtomicHashMap<uint16_t, VBucketPtr> warmedUpVbuckets;

    DISALLOW_COPY_AND_ASSIGN(Warmup);

    // To avoid making a number of methods on Warmup public; grant friendship
    // to the various Tasks which run the stages of warmup.
    friend class WarmupInitialize;
    friend class WarmupCreateVBuckets;
    friend class WarmupLoadingCollectionCounts;
    friend class WarmupEstimateDatabaseItemCount;
    friend class WarmupLoadPreparedSyncWrites;
    friend class WarmupPopulateVBucketMap;
    friend class WarmupKeyDump;
    friend class WarmupCheckforAccessLog;
    friend class WarmupLoadAccessLog;
    friend class WarmupLoadingKVPairs;
    friend class WarmupLoadingData;
    friend class WarmupCompletion;
};
