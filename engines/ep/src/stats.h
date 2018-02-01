/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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
#include "objectregistry.h"

#include <memcached/types.h>
#include <platform/cacheline_padded.h>
#include <platform/corestore.h>
#include <platform/histogram.h>
#include <platform/non_negative_counter.h>
#include <relaxed_atomic.h>

#include <atomic>

/**
 * Global engine stats container.
 */
class EPStats {
public:
    // Thread-safe type for counting occurances of discrete,
    // non-negative entities (# events, sizes).  Relaxed memory
    // ordering (no ordeing or synchronization).
    using Counter = Couchbase::RelaxedAtomic<size_t>;

    EPStats();

    ~EPStats();

    // Disable copying and assignment.
    EPStats(const EPStats&) = delete;
    void operator=(const EPStats&) = delete;

    size_t getMaxDataSize() {
        return maxDataSize.load();
    }

    void setMaxDataSize(size_t size);

    /**
     * Set a percentage which is used to calculate a threshold to which the
     * per core memory tracker value is merged into the global counter.
     * @param percent A float percent assumed to be 0.0 to 100.0 which is used
     *  when setMaxDataSize is called. A per core threshold is calculated which
     *  tries to ensure that getEstimatedTotalMemoryUsed returns a value which
     *  lags the total of all allocations by percent.
     */
    void setMemUsedMergeThresholdPercent(float percent);

    /**
     * @return a estimated memory used. This is an estimate because memory is
     * tracked in a CoreStore container and the estimate value is only updated
     * when certain core thresholds are exceeded. Thus this function returns a
     * value which may lag behind what getPreciseTotalMemoryUsed function
     * returns.
     */
    size_t getEstimatedTotalMemoryUsed() const {
        if (memoryTrackerEnabled.load()) {
            return estimatedTotalMemory->load();
        }
        return currentSize.load() + memOverhead->load();
    }

    /**
     * @return a "precise" memory used value, this is precise because it will
     * return the total from each core, which means iterating the CoreStore
     * container to gather the current core total and summing that with
     * the current estimate.
     */
    size_t getPreciseTotalMemoryUsed() const;

    // account for allocated mem
    void memAllocated(size_t sz);

    // account for deallocated mem
    void memDeallocated(size_t sz);

    //! Number of keys warmed up during key-only loading.
    Counter warmedUpKeys;
    //! Number of key-values warmed up during data loading.
    Counter warmedUpValues;
    //! Number of warmup failures due to duplicates
    Counter warmDups;
    //! Number of OOM failures at warmup time.
    Counter warmOOM;

    //! Fill % of memory used during warmup we're going to enable traffic
    std::atomic<double> warmupMemUsedCap;
    //! Fill % of number of items read during warmup we're going to
    //  enable traffic
    std::atomic<double> warmupNumReadCap;

    //! The replication throttle write queue cap
    std::atomic<ssize_t> replicationThrottleWriteQueueCap;

    //! Amount of items waiting for persistence
    cb::NonNegativeCounter<size_t> diskQueueSize;
    //! Number of items in vbucket backfill queue
    cb::NonNegativeCounter<size_t> vbBackfillQueueSize;
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

    //! Cursor dropping thresholds used by checkpoint remover
    std::atomic<size_t> cursorDroppingLThreshold;
    std::atomic<size_t> cursorDroppingUThreshold;

    //! Number of cursors dropped by checkpoint remover
    Counter cursorsDropped;

    //! Number of times we needed to kick in the pager
    Counter pagerRuns;
    //! Number of times the expiry pager runs for purging expired items
    Counter expiryPagerRuns;
    //! Number of items removed from closed unreferenced checkpoints.
    Counter itemsRemovedFromCheckpoints;
    //! Number of times a value is ejected
    Counter numValueEjects;
    //! Number of times a value could not be ejected
    Counter numFailedEjects;
    //! Number of times "Not my bucket" happened
    Counter numNotMyVBuckets;
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
    //! Total size of StoredVal memory overhead
    Counter storedValOverhead;
    //! Amount of memory used to track items and what-not.
    cb::CachelinePadded<Counter> memOverhead;
    //! Total number of Item objects
    cb::CachelinePadded<Counter> numItem;
    //! The total amount of memory used by this bucket (From memory tracking)
    // This is a signed variable as depending on how/when the thread-local
    // counters merge their info, this could be negative
    cb::CachelinePadded<Couchbase::RelaxedAtomic<int64_t>> estimatedTotalMemory;
    //! The memory tracking by core
    CoreStore<cb::CachelinePadded<Couchbase::RelaxedAtomic<int64_t>>>
            coreTotalMemory;
    //! True if the memory usage tracker is enabled.
    std::atomic<bool> memoryTrackerEnabled;
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
    MicrosecondHistogram pendingOpsHisto;

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
    //! Max number of individual background fetch jobs that we've seen in the queue
    Counter maxRemainingBgJobs;

    /** The sum of the deltas (in usec) from an item was put in queue until
     *  the dispatcher started the work for this item
     */
    std::atomic<hrtime_t> bgWait;
    //! The shortest wait time
    std::atomic<hrtime_t> bgMinWait;
    //! The longest wait time
    std::atomic<hrtime_t> bgMaxWait;

    //! Histogram of background wait times.
    MicrosecondHistogram bgWaitHisto;

    /** The sum of the deltas (in usec) from the dispatcher started to load
     *  item until was done
     */
    std::atomic<hrtime_t> bgLoad;
    //! The shortest load time
    std::atomic<hrtime_t> bgMinLoad;
    //! The longest load time
    std::atomic<hrtime_t> bgMaxLoad;

    //! Histogram of background wait loads.
    MicrosecondHistogram bgLoadHisto;

    //! Max wall time of deleting a vbucket
    std::atomic<hrtime_t> vbucketDelMaxWalltime;
    //! Total wall time of deleting vbuckets
    std::atomic<hrtime_t> vbucketDelTotWalltime;

    //! Histogram of setWithMeta latencies.
    MicrosecondHistogram setWithMetaHisto;

    //! Histogram of access scanner run times
    MicrosecondHistogram accessScannerHisto;
    //! Historgram of checkpoint remover run times
    MicrosecondHistogram checkpointRemoverHisto;
    //! Histogram of item pager run times
    MicrosecondHistogram itemPagerHisto;
    //! Histogram of expiry pager run times
    MicrosecondHistogram expiryPagerHisto;

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

    /** The number of items that have been visited (considered for
     * defragmentation) by the defragmenter task.
     */
    Counter defragNumVisited;

    /** The number of items that have been moved (defragmented) by the
     * defragmenter task.
     */
    Counter defragNumMoved;

    //! Histogram of queue processing dirty age.
    MicrosecondHistogram dirtyAgeHisto;

    //! Histogram of item allocation sizes.
    Histogram<size_t> itemAllocSizeHisto;

    /**
     * Histogram of background fetch batch sizes
     */
    Histogram<size_t> getMultiBatchSizeHisto;

    //
    // Command timers
    //

    //! Histogram of getvbucket timings
    MicrosecondHistogram getVbucketCmdHisto;

    //! Histogram of setvbucket timings
    MicrosecondHistogram setVbucketCmdHisto;

    //! Histogram of delvbucket timings
    MicrosecondHistogram delVbucketCmdHisto;

    //! Histogram of get commands.
    MicrosecondHistogram getCmdHisto;

    //! Histogram of store commands.
    MicrosecondHistogram storeCmdHisto;

    //! Histogram of arithmetic commands.
    MicrosecondHistogram arithCmdHisto;

    //! Time spent notifying completion of IO.
    MicrosecondHistogram notifyIOHisto;

    //! Histogram of get_stats commands.
    MicrosecondHistogram getStatsCmdHisto;

    //! Histogram of wait_for_checkpoint_persistence command
    MicrosecondHistogram chkPersistenceHisto;

    //
    // DB timers.
    //

    //! Histogram of insert disk writes
    MicrosecondHistogram diskInsertHisto;

    //! Histogram of update disk writes
    MicrosecondHistogram diskUpdateHisto;

    //! Histogram of delete disk writes
    MicrosecondHistogram diskDelHisto;

    //! Histogram of execution time of disk vbucket deletions
    MicrosecondHistogram diskVBDelHisto;

    //! Histogram of disk commits
    MicrosecondHistogram diskCommitHisto;

    //! Historgram of batch reads
    MicrosecondHistogram getMultiHisto;

    // ! Histograms of various task wait times, one per Task.
    std::vector<MicrosecondHistogram> schedulingHisto;

    // ! Histograms of various task run times, one per Task.
    std::vector<MicrosecondHistogram> taskRuntimeHisto;

    //! Checkpoint Cursor histograms
    MicrosecondHistogram persistenceCursorGetItemsHisto;
    MicrosecondHistogram dcpCursorsGetItemsHisto;

    //! Reset all stats to reasonable values.
    void reset() {
        tooYoung.store(0);
        tooOld.store(0);
        totalPersistVBState.store(0);
        dirtyAge.store(0);
        dirtyAgeHighWat.store(0);
        commit_time.store(0);
        cursorsDropped.store(0);
        pagerRuns.store(0);
        itemsRemovedFromCheckpoints.store(0);
        numValueEjects.store(0);
        numFailedEjects.store(0);
        numNotMyVBuckets.store(0);
        bg_fetched.store(0);
        bgNumOperations.store(0);
        bgWait.store(0);
        bgLoad.store(0);
        bgMinWait.store(999999999);
        bgMaxWait.store(0);
        bgMinLoad.store(999999999);
        bgMaxLoad.store(0);
        oom_errors.store(0);
        tmp_oom_errors.store(0);
        pendingOps.store(0);
        pendingOpsTotal.store(0);
        pendingOpsMax.store(0);
        pendingOpsMaxDuration.store(0);
        vbucketDelMaxWalltime.store(0);
        vbucketDelTotWalltime.store(0);

        alogRuns.store(0);
        accessScannerSkips.store(0),
        defragNumVisited.store(0),
        defragNumMoved.store(0);

        pendingOpsHisto.reset();
        bgWaitHisto.reset();
        bgLoadHisto.reset();
        setWithMetaHisto.reset();
        accessScannerHisto.reset();
        checkpointRemoverHisto.reset();
        itemPagerHisto.reset();
        expiryPagerHisto.reset();
        getVbucketCmdHisto.reset();
        setVbucketCmdHisto.reset();
        delVbucketCmdHisto.reset();
        getCmdHisto.reset();
        storeCmdHisto.reset();
        arithCmdHisto.reset();
        notifyIOHisto.reset();
        getStatsCmdHisto.reset();
        chkPersistenceHisto.reset();
        diskInsertHisto.reset();
        diskUpdateHisto.reset();
        diskDelHisto.reset();
        diskVBDelHisto.reset();
        diskCommitHisto.reset();
        itemAllocSizeHisto.reset();
        getMultiBatchSizeHisto.reset();
        dirtyAgeHisto.reset();
        getMultiHisto.reset();
        persistenceCursorGetItemsHisto.reset();
        dcpCursorsGetItemsHisto.reset();
    }

    // Used by stats logging infrastructure.
    std::ostream *timingLog;

protected:
    /**
     * Check abs(value) against memUsedMergeThreshold, if it is greater then
     * The thread will attempt to reset coreMemory to zero. If the thread
     * succesfully resets coreMemory to zero than value is accumulated into
     * estimatedTotalMemory
     * @param coreMemory reference to the atomic int64 that the thread is
     *        associated with.
     * @param value The expected value of coreMemory for cmpxchg purposes and
     *        also the value which may be added into estimatedTotalMemory.
     */
    void maybeUpdateEstimatedTotalMemUsed(
            Couchbase::RelaxedAtomic<int64_t>& coreMemory, int64_t value);

    //! Max allowable memory size.
    std::atomic<size_t> maxDataSize;

    /**
     * The threshold at which memAllocated/Deallocated will write their core
     * local value into the 'global' memory counter (estimatedTotalMemory)
     */
    Couchbase::RelaxedAtomic<int64_t> memUsedMergeThreshold;

    /// percentage used in calculating the memUsedMergeThreshold
    float memUsedMergeThresholdPercent;
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
