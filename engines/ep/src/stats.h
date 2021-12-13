/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <folly/Synchronized.h>
#include <folly/container/F14Map.h>
#include <folly/lang/Aligned.h>
#include <memcached/durability_spec.h>
#include <memcached/types.h>
#include <platform/cb_arena_malloc_client.h>
#include <platform/corestore.h>
#include <hdrhistogram/hdrhistogram.h>
#include <platform/non_negative_counter.h>
#include <platform/platform_time.h>
#include <relaxed_atomic.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <mutex>

// If we're running with TSAN/ASAN our global new operator replacement does
// not work, so any new/delete will not call through cb_malloc so ArenaMalloc
// will not be much use.
#if defined(THREAD_SANITIZER) || defined(ADDRESS_SANITIZER)
constexpr bool GlobalNewDeleteIsOurs = false;
#else
constexpr bool GlobalNewDeleteIsOurs = true;
#endif

/**
 * Core-local statistics
 *
 * For statistics which are updated frequently by multiple cores, there can be
 * signifcant cost in maintaining a single bucket-level counter, due to cache
 * line thrashing.
 * This class contains core-local statistics which are signicantly cheaper
 * to update. They are then summed into a bucket-level when read.
 */
class CoreLocalStats {
public:
    /**
     * Map of collection id to the memory usage tracked for that collection.
     */
    folly::Synchronized<folly::F14FastMap<CollectionID, size_t>, std::mutex>
            collectionMemUsed;

    // Thread-safe type for counting occurances of discrete,
    // non-negative entities (# events, sizes).  Relaxed memory
    // ordering (no ordering or synchronization).
    // This is a signed variable as depending on how/when the core-local
    // counters merge their info, this could be negative.
    using Counter = cb::RelaxedAtomic<int64_t>;

    //! Total size of stored objects.
    Counter currentSize;

    //! Total number of blob objects
    Counter numBlob;

    //! Total size of blob memory overhead
    Counter blobOverhead;

    //! Total memory overhead to store values for resident keys.
    Counter totalValueSize;

    //! The number of storedVal object
    Counter numStoredVal;

    //! Total memory for stored values
    Counter totalStoredValSize;

    //! Amount of memory used to track items and what-not.
    Counter memOverhead;

    //! Total number of Item objects
    Counter numItem;

    //! Estimate of the total amount of memory used by checkpoints owned by CMs
    // Note: This does NOT account mem used by checkpoints owned by Destroyers
    Counter checkpointManagerEstimatedMemUsage;

    //! Total number of checkpoints across all vbuckets
    Counter numCheckpoints;
};

/**
 * Global engine stats container.
 */
class EPStats {
public:
    // Thread-safe type for counting occurances of discrete,
    // non-negative entities (# events, sizes).  Relaxed memory
    // ordering (no ordeing or synchronization).
    using Counter = cb::RelaxedAtomic<size_t>;

    EPStats();

    ~EPStats();

    // Disable copying and assignment.
    EPStats(const EPStats&) = delete;
    void operator=(const EPStats&) = delete;

    size_t getMaxDataSize() {
        return maxDataSize.load();
    }

    /**
     * Set the max data size
     */
    void setMaxDataSize(size_t size);

    /**
     * Memory tracking is enabled only if new/delete is replaced, so is
     * redirected to cb_malloc and if ArenaMalloc has the capability to track
     * allocations.
     * @return if memory tracking is enabled
     */
    static bool isMemoryTrackingEnabled();

    /**
     * The estimated memory lags behind the value getPreciseTotalMemoryUsed
     * may return (can be above or below). The returned value is only updated
     * when.
     * 1) getPreciseTotalMemoryUsed is called
     * 2) The memory tracker thread runs (which is currently every 250ms)
     *
     * Note that some non-production configurations the ArenaMalloc cannot track
     * deallocation, so in that case just return the getCurrentSize() +
     * getMemOverhead()
     *
     * @return a estimate of the total memory allocated to the engine
     */
    size_t getEstimatedTotalMemoryUsed() const;

    /**
     * @return a "precise" memory used value. This asks the underlying platform
     * ArenaMalloc how much is allocated to the engine. When this method is
     * called the current estimate is updated.
     */
    size_t getPreciseTotalMemoryUsed() const;

    /// @returns total size of stored objects.
    size_t getCurrentSize() const;

    /// @returns number of Blob objects which exist.
    size_t getNumBlob() const;

    /// @returns size of blob memory overhead in bytes.
    size_t getBlobOverhead() const;

    /// @returns total memory overhead to store values for resident keys.
    size_t getTotalValueSize() const;

    /// @returns number of StoredValue objects which exist.
    size_t getNumStoredVal() const;

    /// @returns size of all StoredValue objects.
    size_t getStoredValSize() const;

    /// @returns amount of memory used to track items and what-not.
    size_t getMemOverhead() const;

    /// @returns number of Item objects which exist.
    size_t getNumItem() const;

    /**
     * @returns the estimate of the total amount of memory used by checkpoints
     *  that reside in CMs. Does NOT account mem used by checkpoints owned by
     *  Destroyers.
     */
    size_t getCheckpointManagerEstimatedMemUsage() const;

    /// @returns the total number of checkpoints across all vbuckets
    size_t getNumCheckpoints() const;

    /// @returns total size of stored objects for a single collection.
    size_t getCollectionMemUsed(CollectionID cid) const;

    /// @returns total size of stored objects for each existing collection.
    std::unordered_map<CollectionID, size_t> getAllCollectionsMemUsed() const;

    /**
     * Used when adding a collection.
     *
     * By explicitly starting tracking collection stats the work required
     * per change can be minimised, and cleanup logic simplified.
     *
     * @param cid the collection for which to track stats
     */
    void trackCollectionStats(CollectionID cid);

    /**
     * Used when dropping a collection. Avoids stats lingering
     * forever with a zero value.
     *
     * @param cid the collection for which to stop tracking stats
     */
    void dropCollectionStats(CollectionID cid);

    /**
     * Set the low water mark to the new value.
     * Side effect is that the low water mark percentage is updated to the
     * percentage of max_size
     * @param the new low watermark (in bytes)
     */
    void setLowWaterMark(size_t value);

    /**
     * Set the high water mark to the new value.
     * Side effect is that the high water mark percentage is updated to the
     * percentage of max_size
     * @param the new high watermark (in bytes)
     */
    void setHighWaterMark(size_t value);

    //! Number of keys warmed up during key-only loading.
    Counter warmedUpKeys;
    //! Number of key-values warmed up during data loading.
    Counter warmedUpValues;
    //! Number of prepares warmed up.
    Counter warmedUpPrepares;
    //! Number of items visited whilst loading prepares
    Counter warmupItemsVisitedWhilstLoadingPrepares;
    //! Number of warmup failures due to duplicates
    Counter warmDups;
    //! Number of OOM failures at warmup time.
    Counter warmOOM;

    //! Fill % of memory used during warmup we're going to enable traffic
    std::atomic<double> warmupMemUsedCap;
    //! Fill % of number of items read during warmup we're going to
    //  enable traffic
    std::atomic<double> warmupNumReadCap;

    //! Amount of items waiting for persistence
    cb::AtomicNonNegativeCounter<size_t> diskQueueSize;
    //! Size of the in-process (output) queue.
    Counter flusher_todo;
    //! Number of transaction commits.
    Counter flusherCommits;
    //! Total time spent flushing.
    Counter cumulativeFlushTime;
    //! Total time spent committing.
    Counter cumulativeCommitTime;
    //! Objects that were rejected from persistence for being too fresh.
    Counter tooYoung;
    //! Objects that were forced into persistence for being too old.
    Counter tooOld;
    //! Number of items persisted.
    Counter totalPersisted;
    //! Number of times VBucket state persisted.
    Counter totalPersistVBState;
    //! Cumulative number of items added to the queue.
    Counter totalEnqueued;
    //! Cumulative count of items de-duplicated when queued to CheckpointManager
    Counter totalDeduplicated;
    //! Number of times an item flush failed.
    Counter flushFailed;
    //! Number of times an item is not flushed due to the item's expiry
    Counter flushExpired;

    // Expiration stats. Note: These stats are not synchronous -
    // e.g. expired_pager can be incremented /before/ curr_items is
    // decremented. This is because curr_items is sometimes only
    // updated long after the expiration action, when delete is
    // persisted to disk (and callback invoked).

    //! Number of times an object was expired on access.
    Counter expired_access;
    //! Number of times an object was expired by compactor.
    Counter expired_compactor;
    //! Number of times an object was expired by pager.
    Counter expired_pager;

    //! Number of times we failed to start a transaction
    Counter beginFailed;
    //! Number of times a commit failed.
    Counter commitFailed;
    //! How long an object is dirty before written.
    std::atomic<rel_time_t> dirtyAge;
    //! Oldest enqueued object we've seen while persisting.
    std::atomic<rel_time_t> dirtyAgeHighWat;
    //! Amount of time spent in the commit phase.
    std::atomic<rel_time_t> commit_time;
    //! Number of times we deleted a vbucket.
    Counter vbucketDeletions;
    //! Number of times we failed to delete a vbucket.
    Counter vbucketDeletionFail;

    //! Beyond this point are config items
    //! Pager low water mark.
    std::atomic<size_t> mem_low_wat;
    std::atomic<double> mem_low_wat_percent;
    //! Pager high water mark
    std::atomic<size_t> mem_high_wat;
    std::atomic<double> mem_high_wat_percent;

    //! Number of cursors dropped by checkpoint remover
    Counter cursorsDropped;

    //! Amount of memory we have freed by checkpoint removal
    std::atomic<size_t> memFreedByCheckpointRemoval;

    //! Amount of memory we have freed by checkpoint item expel
    Counter memFreedByCheckpointItemExpel;

    //! Number of times we needed to kick in the pager
    Counter pagerRuns;
    //! Number of times the expiry pager runs for purging expired items
    Counter expiryPagerRuns;
    //! Number of times the item frequency decayer runs
    Counter freqDecayerRuns;
    //! The number items expelled from checkpoints
    Counter itemsExpelledFromCheckpoints;
    //! Number of items removed from closed unreferenced checkpoints.
    Counter itemsRemovedFromCheckpoints;
    //! Number of times a value is ejected
    Counter numValueEjects;
    //! Number of times a value could not be ejected
    Counter numFailedEjects;
    //! Number of times "Not my bucket" happened
    Counter numNotMyVBuckets;

    //! Core-local statistics
    CoreStore<folly::cacheline_aligned<CoreLocalStats>> coreLocal;

    // Total memory used by hashtable items for replica vbuckets.
    cb::RelaxedAtomic<int64_t> replicaHTMemory;

    // Total memory used by checkpoints for replica vbuckets.
    cb::RelaxedAtomic<int64_t> replicaCheckpointOverhead;

    //! Whether or not to force engine shutdown.
    std::atomic<bool> forceShutdown;
    //! Number of times unrecoverable oom errors happened while processing operations.
    Counter oom_errors;
    //! Number of times temporary oom errors encountered while processing operations.
    Counter tmp_oom_errors;

    //! Number of ops blocked on all vbuckets in pending state
    Counter pendingOps;
    //! Total number of ops ever blocked on all vbuckets in pending state
    Counter pendingOpsTotal;
    //! High water value for ops blocked for any individual pending vbucket
    std::atomic<size_t> pendingOpsMax;
    //! High water value for time an op is blocked on a pending vbucket
    std::atomic<hrtime_t> pendingOpsMaxDuration;

    //! Histogram of pending operation wait times.
    Hdr1sfMicroSecHistogram pendingOpsHisto;

    //! Number of pending vbucket compaction requests
    Counter pendingCompactions;

    //! Number of times background fetches occurred.
    Counter bg_fetched;
    //! Number of times meta background fetches occurred.
    Counter bg_meta_fetched;
    //! Number of remaining bg fetch items
    Counter numRemainingBgItems;
    //! Number of remaining bg fetch jobs.
    Counter numRemainingBgJobs;
    //! The number of samples the bgWaitDelta and bgLoadDelta contains of
    Counter bgNumOperations;

    /** The sum of the deltas (in usec) from an item was put in queue until
     *  the dispatcher started the work for this item
     */
    std::atomic<hrtime_t> bgWait;
    //! The shortest wait time
    std::atomic<hrtime_t> bgMinWait;
    //! The longest wait time
    std::atomic<hrtime_t> bgMaxWait;

    //! Histogram of background wait times.
    Hdr1sfMicroSecHistogram bgWaitHisto;

    /** The sum of the deltas (in usec) from the dispatcher started to load
     *  item until was done
     */
    std::atomic<hrtime_t> bgLoad;
    //! The shortest load time
    std::atomic<hrtime_t> bgMinLoad;
    //! The longest load time
    std::atomic<hrtime_t> bgMaxLoad;

    //! Histogram of background wait loads.
    Hdr1sfMicroSecHistogram bgLoadHisto;

    //! Max wall time of deleting a vbucket
    std::atomic<hrtime_t> vbucketDelMaxWalltime;
    //! Total wall time of deleting vbuckets
    std::atomic<hrtime_t> vbucketDelTotWalltime;

    //! Histogram of setWithMeta latencies.
    Hdr1sfMicroSecHistogram setWithMetaHisto;

    //! Histogram of access scanner run times
    Hdr1sfMicroSecHistogram accessScannerHisto;
    //! Historgram of checkpoint remover run times
    Hdr1sfMicroSecHistogram checkpointRemoverHisto;
    //! Histogram of item pager run times
    Hdr1sfMicroSecHistogram itemPagerHisto;
    //! Histogram of expiry pager run times
    Hdr1sfMicroSecHistogram expiryPagerHisto;

    //! Percentage of memory in use before we throttle replication input
    std::atomic<double> replicationThrottleThreshold;

    //! The number of basic store (add, set, arithmetic, touch, etc.) operations
    Counter numOpsStore;
    //! The number of basic delete operations
    Counter numOpsDelete;
    //! The number of basic get operations
    Counter numOpsGet;

    //! The number of get with meta operations
    Counter  numOpsGetMeta;
    //! The number of set with meta operations
    Counter  numOpsSetMeta;
    //! The number of delete with meta operations
    Counter  numOpsDelMeta;
    //! The number of failed set meta ops due to conflict resoltion
    Counter numOpsSetMetaResolutionFailed;
    //! The number of failed del meta ops due to conflict resoltion
    Counter numOpsDelMetaResolutionFailed;
    //! The number of set returning meta operations
    Counter  numOpsSetRetMeta;
    //! The number of delete returning meta operations
    Counter  numOpsDelRetMeta;
    //! The number of background get meta ops due to set_with_meta operations
    Counter  numOpsGetMetaOnSetWithMeta;

    //! The number of times the access scanner runs
    Counter alogRuns;
    //! The number of times the access scanner skips generating access log
    Counter accessScannerSkips;
    //! The number of items that last access scanner task swept to log
    Counter alogNumItems;
    //! The next access scanner task schedule time (GMT)
    std::atomic<hrtime_t> alogTime;
    //! The number of seconds that the last access scanner task took
    std::atomic<rel_time_t> alogRuntime;

    //! The next expiry pager task schedule time (GMT)
    std::atomic<hrtime_t> expPagerTime;

    std::atomic<bool> isShutdown;

    Counter rollbackCount;

    /**
     * The number of items that have been visited (considered for
     * defragmentation) by the defragmenter task.
     */
    Counter defragNumVisited;

    /**
     * The number of items that have been moved (defragmented) by the
     * defragmenter task.
     */
    Counter defragNumMoved;

    /**
     * The number of StoredValues that have been moved (defragmented) by the
     * defragmenter task.
     */
    Counter defragStoredValueNumMoved;

    Counter compressorNumVisited;
    Counter compressorNumCompressed;

    //! Histogram of queue processing dirty age.
    Hdr1sfMicroSecHistogram dirtyAgeHisto;

    //! Histogram of item allocation sizes.
    HdrHistogram itemAllocSizeHisto{
            1,
            20 * 1024 * 1024, // Max alloc size is 20MB
            1,
            HdrHistogram::Iterator::IterMode::Percentiles};

    /**
     * Histogram of background fetch batch sizes
     * Multi batch size histogram is rarely used, so we don't want to used a lot
     * of memory for it. Thus, we will only use a 1 sig fig level of accuracy.
     * This will still allow us to see high batch sizes.
     */
    Hdr1sfInt32Histogram getMultiBatchSizeHisto;

    /**
     * Histogram of frequency counts for items evicted from active or pending
     * vbuckets.
     */
    HdrUint8Histogram activeOrPendingFrequencyValuesEvictedHisto;

    /**
     * Histogram of frequency counts for items evicted from replica vbuckets.
     */
    HdrUint8Histogram replicaFrequencyValuesEvictedHisto;

    /**
     * Histogram of eviction thresholds when evicting from active or pending
     * vbuckets.
     */
    HdrUint8Histogram activeOrPendingFrequencyValuesSnapshotHisto;

    /**
     * Histogram of eviction thresholds when evicting from replica vbuckets.
     */
    HdrUint8Histogram replicaFrequencyValuesSnapshotHisto;

    //
    // Command timers
    //

    //! Histogram of getvbucket timings
    Hdr1sfMicroSecHistogram getVbucketCmdHisto;

    //! Histogram of setvbucket timings
    Hdr1sfMicroSecHistogram setVbucketCmdHisto;

    //! Histogram of delvbucket timings
    Hdr1sfMicroSecHistogram delVbucketCmdHisto;

    //! Histogram of get commands.
    Hdr1sfMicroSecHistogram getCmdHisto;

    //! Histogram of store commands.
    Hdr1sfMicroSecHistogram storeCmdHisto;

    //! Histogram of arithmetic commands.
    Hdr1sfMicroSecHistogram arithCmdHisto;

    //! Time spent notifying completion of IO.
    Hdr1sfMicroSecHistogram notifyIOHisto;

    //! Histogram of get_stats commands.
    Hdr1sfMicroSecHistogram getStatsCmdHisto;

    //! Histogram of wait_for_checkpoint_persistence command
    Hdr1sfMicroSecHistogram chkPersistenceHisto;

    //
    // DB timers.
    //

    //! Histogram of insert disk writes
    Hdr1sfMicroSecHistogram diskInsertHisto;

    //! Histogram of update disk writes
    Hdr1sfMicroSecHistogram diskUpdateHisto;

    //! Histogram of delete disk writes
    Hdr1sfMicroSecHistogram diskDelHisto;

    //! Histogram of execution time of disk vbucket deletions
    Hdr1sfMicroSecHistogram diskVBDelHisto;

    //! Histogram of disk commits
    Hdr1sfMicroSecHistogram diskCommitHisto;

    // ! Histograms of various task wait times, one per Task.
    std::vector<Hdr1sfMicroSecHistogram> schedulingHisto;

    // ! Histograms of various task run times, one per Task.
    std::vector<Hdr1sfMicroSecHistogram> taskRuntimeHisto;

    //! Checkpoint Cursor histograms
    Hdr1sfMicroSecHistogram persistenceCursorGetItemsHisto;
    Hdr1sfMicroSecHistogram dcpCursorsGetItemsHisto;

    /// Histogram of the durations of SyncWrite commits; measured from when
    /// the SyncWrite is added to the durability monitor up to when it is
    /// committed.
    /// One histogram per durability level.
    std::array<Hdr1sfMicroSecHistogram,
               size_t(cb::durability::Level::PersistToMajority)>
            syncWriteCommitTimes;

    cb::ArenaMallocClient arena;

    //! Reset all stats to reasonable values.
    void reset();

    size_t getMemFootPrint() const;

    // Used by stats logging infrastructure.
    std::ostream *timingLog;

protected:

    //! Max allowable memory size.
    std::atomic<size_t> maxDataSize;
};

/**
 * Stats returned by key stats.
 */
struct key_stats {
    //! The item's CAS
    uint64_t cas;
    //! The expiration time of the itme
    rel_time_t exptime;
    //! The item's current flags.
    uint32_t flags;
    //! The vbucket state for this key
    vbucket_state_t vb_state;
    //! True if this item is dirty.
    bool dirty;
    //! True if the item has been logically deleted
    bool logically_deleted;
    //! True if the document is currently resident in memory.
    bool resident;
};
