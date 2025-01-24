/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "mutation_log.h"
#include "utilities/testing_hook.h"
#include "vbucket_fwd.h"
#include "vbucket_state.h"

#include <memcached/engine_common.h>
#include <memcached/engine_error.h>
#include <platform/atomic_duration.h>

#include <atomic>
#include <chrono>
#include <deque>
#include <iosfwd>
#include <map>
#include <mutex>
#include <unordered_set>
#include <vector>

class CookieIface;
class BySeqnoScanContext;
class Configuration;
class EPStats;
class EPBucket;
class GetValue;
class MutationLog;
class StatCollector;
class VBucketMap;
class Vbid;
enum class ValueFilter;

struct vbucket_state;

template <typename...>
class StatusCallback;

class GlobalTask;
using ExTask = std::shared_ptr<GlobalTask>;

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
     * @param allowAnyState If true, force the transition to the given state.
     */
    void transition(State to, bool allowAnyState);

    const char* toString() const;

    State getState() const {
        return state;
    }

    TestingHook<> transitionHook;

private:
    std::atomic<State> state{State::Initialize};

    const char* getStateDescription(State val) const;

    /**
     * @returns true if the `to` state is legal transition based on the state
     *          warmup is currently in.
     */
    bool legalTransition(State from, State to) const;

    friend std::ostream& operator<< (std::ostream& out,
                                     const WarmupState &state);
};

std::string to_string(WarmupState::State val);

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
 *                           ┌──────────────────┐
 *                           │    Initialise    │
 *                           └──────────────────┘
 *                                     │
 *                                     ▼
 *                           ┌──────────────────┐
 *                           │  CreateVbuckets  │
 *                           └──────────────────┘
 *                                     │
 *                                     ▼
 *                        ┌─────────────────────────┐
 *                        │ LoadingCollectionCounts │
 *                        └─────────────────────────┘
 *                                     │
 *                                     ▼
 *                       ┌───────────────────────────┐
 *                       │ EstimateDatabaseItemCount │
 *                       └───────────────────────────┘
 *                                     │
 *                                     ▼
 *                       ┌───────────────────────────┐
 *                       │   LoadPreparedSyncWrites  │
 *                       └───────────────────────────┘
 *                                     │
 *                                     ▼
 *                       ┌───────────────────────────┐
 *                       │    PoplulateVBucketMap    │
 *                       └───────────────────────────┘
 *                                     │
 *                              Eviction Mode?
 *                      Value ◀────────┴─────▶ Full
 *                         │                    │
 *                         ▼                    │
 *                   ┌──────────┐               │
 *                   │ KeyDump  │      ┌────────┘
 *                   └──────────┘      │
 *                         └───────────┤
 *                                     ▼
 *                     Warm-up reached stop thresholds?
 *         ┌────┐                      │
 *         │Done│◀───────── Yes ───────┤
 *         └────┘                      │
 *                                    No
 *                                     │
 *                                     ▼
 *                          ┌────────────────────┐
 *                          │ CheckForAccessLog  │
 *                          └────────────────────┘
 *                                     │
 *                                     ▼
 *                             Access Log Found?
 *                 ┌─────── Yes  ◀─────┴─────▶  No  ────┐
 *                 ▼                                    ▼
 *       ┌───────────────────┐                   Eviction Mode?
 *       │ LoadingAccessLog  │                          │
 *       └───────────────────┘                          │
 *                 │                     Value ◀────────┴─────▶ Full
 *                 ▼                         │                    │
 * Warm-up reached stop thresholds?          │                    │
 *                 │                ┌────────┘                    │
 *      Yes  ◀─────┴─────▶  No  ────┤                             │
 *        │                         ▼                             ▼
 *        │                  ┌─────────────┐           ┌────────────────────┐
 *        │                  │ LoadingData │           │   LoadingKVPairs   │
 *        │                  └─────────────┘           └────────────────────┘
 *        │                         │                             │
 *        │                         ▼                             │
 *        │                      ┌────┐                           │
 *        └─────────────────────▶│Done│◀──────────────────────────┘
 *                               └────┘
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
 * Note that once warmup is complete (Warmup::isFinishedLoading() returns true)
 * the warmup will conclude the phase and short-cut to Done. When
 * Warmup::isFinishedLoading() returns true all CRUD operations are fully
 * processed. When Warmup::isComplete() returns true DCP consumers can be
 * created. isComplete() will return true later than isFinishedLoading() as we
 * need to marshall all of the warmup threads before allowing DCP Consumers to
 * prevent race conditions with rollback.
 */
class Warmup {
public:
    /**
     * This constructor exists for the Primary warmup phase, which is a phase of
     * warmup that will read all metadata and populate the given EPBucket with
     * for example "VBucket" objects that are found on disk. On reaching the
     * Done phase the given warmupDoneFunction is invoked.
     *
     * @param st reference to the owning bucket that will be populated
     * @param config reference to the bucket's configuration
     * @param warmupDoneFunction This function is invoked once by Warmup on
     *        reaching the Done state.
     * @param memoryThreshold see setMemoryThreshold - value is passed to that
     *        function
     * @param itemsThreshold see setItemThreshold - value is passed to that
     *        function
     * @param name A name used in logging
     */
    Warmup(EPBucket& st,
           const Configuration& config,
           std::function<void()> warmupDoneFunction,
           size_t memoryThreshold,
           size_t itemsThreshold,
           std::string name);

    /**
     * Constructor exists for creating the Secondary warm-up object. This phase
     * will skip the metadata loading (and assumes the given Warmup object has
     * already done such loading). This object will begin warmup from the
     * CheckForAccessLog phase.
     *
     * @param primaryWarmup Parts of this object are copied/moved into the this
     *        Warmup.
     * @param memoryThreshold see setMemoryThreshold - value is passed to that
     *        function
     * @param itemsThreshold see setItemThreshold - value is passed to that
     *        function
     * @param name A name used in logging
     */
    Warmup(Warmup& primaryWarmup,
           size_t memoryThreshold,
           size_t itemsThreshold,
           std::string name);

    ~Warmup();

    Warmup(const Warmup&) = delete;
    Warmup& operator=(const Warmup&) = delete;
    Warmup(Warmup&&) = delete;
    Warmup& operator=(Warmup&&) = delete;

    void start();
    void stop();

    size_t getEstimatedKeyCount() const;
    size_t getEstimatedValueCount() const;

    void addCommonStats(const StatCollector& collector) const;

    void addStats(const StatCollector& c) const;
    void addSecondaryWarmupStats(const StatCollector& c) const;

    /**
     * Add state-labelled one-hot metrics expressing the current state of
     * warmup.
     */
    void addStatusMetrics(const StatCollector& c) const;

    std::chrono::steady_clock::duration getTime() const {
        return warmup.load();
    }

    void setWarmupTime() {
        warmup.store(std::chrono::steady_clock::now() -
                     syncData.lock()->startTime +
                     std::chrono::steady_clock::duration(1));
    }

    enum class WarmupAccessLogState { Yield, Done, Failed };
    WarmupAccessLogState tryLoadFromAccessLog(MutationLog& lf,
                                              uint16_t shardId);

    bool isComplete() const {
        return finishedLoading && state.getState() == WarmupState::State::Done;
    }

    /**
     * @return true if this warmup has loaded all metadata
     */
    bool hasLoadedMetaData() const;

    bool isFinishedLoading() const {
        return finishedLoading.load();
    }

    bool setFinishedLoading() {
        bool inverse = false;
        return finishedLoading.compare_exchange_strong(inverse, true);
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
    bool maybeWaitForVBucketWarmup(CookieIface* cookie);

    /**
     * Perform any notifications to any pending operations
     * @param status Status code to send to all waiting cookies.
     */
    void notifyWaitingCookies(cb::engine_errc status);

    bool setOOMFailure() {
        bool inverse = false;
        return warmupOOMFailure.compare_exchange_strong(inverse, true);
    }

    bool hasOOMFailure() const {
        return warmupOOMFailure.load();
    }

    bool hasSetVbucketStateFailure() const {
        return syncData.lock()->failedToSetAVbucketState;
    };

    WarmupState::State getWarmupState() const {
        return state.getState();
    }

    void setWarmupStateTransitionHook(std::function<void()> hook) {
        state.transitionHook = hook;
    }

    std::string getThreadStatState() const {
        if (isComplete()) {
            return "complete";
        }

        return "running";
    }

    /**
     * Setup a memory threshold at which Warmup will transition to Done. As
     * Warmup loads data, mem_used will be checked against this percentage of
     * the bucket quota (max_size).
     *
     * This function is related to the configuration parameter
     * primary|secondary_warmup_min_memory_threshold which requires a integer
     * value from 0 to 100, but this function can accept values greater than 100
     * for test purposes.
     *
     * @param perc A integer value that is the % of max_size at which Warmup
     *             will transition to Done.
     */
    void setMemoryThreshold(size_t perc);

    /**
     * Set a scale factor for calculating the ratio of available items that can
     * be loaded before Warmup must transition to Done.
     *
     * Setup a item loaded threshold at which Warmup will transition to Done.
     * As Warmup loads data it will transition to Done if this percentage of
     * the available items is loaded.
     *
     * This function is related to the configuration parameter
     * primary|secondary_warmup_min_items_threshold which requires a integer
     * value from 0 to 100, but this function can accept values greater than 100
     * for test purposes.
     *
     * @param perc A integer value that is the % of items at which Warmup
     *             will transition to Done.
     */
    void setItemThreshold(size_t perc);

    /**
     * Check if Warmup has reached any of the thresholds at which it should
     * transition to Done.
     *
     * A critical side-effect of this function is that when true is returned
     * the reason is logged (a significant statement for the supportability of
     * this component).
     *
     * @return true if the bucket has reached a Warmup "threshold".
     */
    bool hasReachedThreshold() const;

    const std::string& getName() const {
        return name;
    }

    /**
     * Increment stats related to how many keys have been loaded.
     * This will increment the Warmup counter and the bucket "total" owned by
     * EPStats
     */
    void incrementKeys();

    /**
     * Increment stats related to how many value have been loaded.
     * This will increment the Warmup counter and the bucket "total" owned by
     * EPStats
     */
    void incrementValues();

    /// @return the number of keys loaded by this Warmup
    size_t getKeys() const;

    /// @return the number of values loaded by this Warmup
    size_t getValues() const;

    /**
     * Warmup holds weak_ptr references to all VBuckets that are being warmed
     * up. This function will weak_ptr::lock and return a
     * shared_ptr<VBucket which the caller must check for validity.
     *
     * The current implementation means that a O(n) search is performed to find
     * a VBucket associated with vbid. n however max-vbuckets/shards
     *
     * @return VBucketPtr (shared_ptr<VBucket>) which may or may not point to a
     *         VBucket.
     */
    VBucketPtr tryAndGetVbucket(Vbid vbid) const;

    /// @return reference to internal vector of per shard VBData
    const auto& getShardVBData() const {
        return shardVBData;
    }

    /**
     * Testing hook which if set is called every time warmup transitions to
     * a new state.
     */
    TestingHook<WarmupState::State> stateTransitionHook;

private:
    void addToTaskSet(size_t taskId);
    void removeFromTaskSet(size_t taskId);

    void step();

    void setEstimatedValueCount(size_t num);

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
     *
     * @return true if task should reschedule, false if not
     */
    bool loadingAccessLog(uint16_t shardId);

    /**
     * Perform loading from the given access log for the given shard.
     * Loading of access log data will stop on reaching the end of the log or
     * if the time permitted for loading is reached (or fails).
     *
     * @return The state of loading required to determine the next warm-up phase
     */
    WarmupAccessLogState loadFromAccessLog(MutationLog& log, uint16_t shardId);

    /* Terminal state of warmup. Updates statistics and marks warmup as
     * completed
     */
    void done();

    void populateShardVbStates();

    /**
     * Helper method to schedule a WarmupBackfillTask for each shard.
     *
     * @param phase The warmup phase that is being scheduled to run. See
     *  WarmupState::State for details.
     */
    void scheduleShardedTasks(const WarmupState::State phase);

    void transition(WarmupState::State to, bool force = false);

    /// @return how many shards warmup is working with (this sets the
    ///         concurrency of certain stages).
    size_t getNumShards() const;

    /// private and common setup used by the two constructors
    void setup(size_t memoryThreshold, size_t itemsThreshold);

    /// log as INFO interesting statistics about this Warmup
    void logStats() const;

    WarmupState state;

    EPBucket& store;
    const Configuration& config;
    const EPStats& stats;

    using PendingCookiesQueue = std::deque<CookieIface*>;
    struct SyncData {
        SyncData() = default;
        SyncData(std::function<void()> doneFunction)
            : doneFunction(std::move(doneFunction)) {
        }
        bool cleanShutdown{false};
        bool corruptAccessLog{false};
        std::chrono::steady_clock::time_point startTime;
        /// True if we've been unable to persist vbucket state during warmup
        bool failedToSetAVbucketState{false};

        // If true warmup will collect cookies and notify them when
        // PopulateVBucketMap is complete.
        bool mustSaveCookies{true};
        /// All of the cookies which need notifying when create-vbuckets is done
        PendingCookiesQueue cookies;

        // Unordered set to hold the current executing tasks
        std::unordered_set<size_t> taskSet;

        // A callback to invoke when Warmup is Done.
        std::function<void()> doneFunction = []() {};

        std::unordered_map<Vbid, std::weak_ptr<VBucket>> weakVbMap;
    };
    folly::Synchronized<SyncData, std::mutex> syncData;

    // Time it took to load metadata and complete warmup, stored atomically.
    cb::AtomicDuration<> metadata;
    cb::AtomicDuration<> warmup;
    std::atomic<size_t> threadtask_count{0};

    // VBData stores information for one VBucket
    struct VBData {
        VBData(Vbid id, vbucket_state state)
            : vbid(id), state(std::move(state)) {
        }

        // The id of the vbucket
        Vbid vbid;

        // The state read from the vbucket during Warmup::populateShardVbStates
        vbucket_state state;

        // The VB object created in the CreateVBuckets phase. This object is
        // given to vbMap in the PopulateVBucketMap phase and not used in other
        // phases. It is used in phases between CreateVBuckets and
        // PopulateVBucketMap, e.g. during LoadCollectionCounts.
        VBucketPtr vbucketPtr;
    };

    // ShardList is a vector of VBData, one per vbucket found on disk for the
    // shard. The vbuckets in the container are not ordered (see
    // populateShardVbStates)
    using ShardList = std::vector<VBData>;

    // A vector of ShardList per shard. This object is indexed by shard-ID and
    // Warmup construction sizes this vector (or in the secondary Warmup path
    // moves an existing vector from primary Warmup).
    //
    // The ShardList/ShardData are all initialised in the single threaded
    // Initialise phase of warmup.
    //
    // During:
    //   * CreateVBucket
    //   * LoadingCollectionCounts
    //   * EstimateDatabaseItemCount
    //   * LoadPreparedSyncWrites
    //   * PoplulateVBucketMap
    //   * KeyDump
    //   * LoadingAccessLog
    //   * LoadingData
    //   * LoadingKVPairs
    // different threads can concurrently access this structur but each thread
    // accesses just one shard.
    std::vector<ShardList> shardVBData;

    cb::AtomicDuration<> estimateTime;

    /**
     * Has the data loading finished? This was historically called
     * warmupCompleted and many operations are blocked until this flag is set to
     * true. A warmup is not necessarily "complete" in its entirety though if we
     * stopped loading due to hitting some memory threshold (such as the LWM of
     * a full eviction bucket).
     */
    std::atomic<bool> finishedLoading{false};
    std::atomic<bool> warmupOOMFailure{false};
    std::atomic<size_t> estimatedKeyCount{std::numeric_limits<size_t>::max()};
    std::atomic<size_t> estimatedValueCount{std::numeric_limits<size_t>::max()};

    std::vector<std::vector<std::unique_ptr<MutationLog>>> accessLog;

    /**
     * A scale factor for computing how much memory can be consumed during
     * Warmup. Atomic as it can be written and read and different threads.
     */
    std::atomic<double> maxSizeScaleFactor{0.0};

    /**
     * A scale factor for calculating the maximum number of items that can be
     * loaded by Warmup. Atomic as it can be written and read and different
     * threads.
     */
    std::atomic<double> maxItemsScaleFactor{0.0};

    /// A name used in logging about Warmup
    const std::string name;

    /// counter of keys loaded by this Warmup
    cb::RelaxedAtomic<size_t> keys{0};
    /// counter of values loaded by this Warmup
    cb::RelaxedAtomic<size_t> values{0};

    // To avoid making a number of methods on Warmup public; grant friendship
    // to the various Tasks which run the stages of warmup.
    friend class WarmupInitialize;
    friend class WarmupCreateVBuckets;
    friend class WarmupLoadingCollectionCounts;
    friend class WarmupEstimateDatabaseItemCount;
    friend class WarmupLoadPreparedSyncWrites;
    friend class WarmupPopulateVBucketMap;
    friend class WarmupCheckforAccessLog;
    friend class WarmupKeyDump;
    friend class WarmupLoadAccessLog;
    friend class WarmupVbucketVisitor;
    friend class WarmupBackfillTask;
    friend class WarmupLoadingKVPairs;
    friend class WarmupLoadingData;
    friend class WarmupCompletion;
};
