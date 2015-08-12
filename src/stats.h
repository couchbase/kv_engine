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

#ifndef SRC_STATS_H_
#define SRC_STATS_H_ 1

#include "config.h"

#include <memcached/engine.h>

#include <map>

#include "atomic.h"
#include "common.h"
#include "histo.h"
#include "memory_tracker.h"
#include "mutex.h"

#ifndef DEFAULT_MAX_DATA_SIZE
/* Something something something ought to be enough for anybody */
#define DEFAULT_MAX_DATA_SIZE (std::numeric_limits<size_t>::max())
#endif

static const hrtime_t ONE_SECOND(1000000);

/**
 * Global engine stats container.
 */
class EPStats {
public:

    EPStats() :
        warmedUpKeys(0),
        warmedUpValues(0),
        warmDups(0),
        warmOOM(0),
        warmupMemUsedCap(0),
        warmupNumReadCap(0),
        replicationThrottleWriteQueueCap(0),
        diskQueueSize(0),
        flusher_todo(0),
        flusherCommits(0),
        cumulativeFlushTime(0),
        cumulativeCommitTime(0),
        tooYoung(0),
        tooOld(0),
        totalPersisted(0),
        totalEnqueued(0),
        flushFailed(0),
        flushExpired(0),
        expired_access(0),
        expired_compactor(0),
        expired_pager(0),
        beginFailed(0),
        commitFailed(0),
        dirtyAge(0),
        dirtyAgeHighWat(0),
        commit_time(0),
        vbucketDeletions(0),
        vbucketDeletionFail(0),
        mem_low_wat(0),
        mem_low_wat_percent(0),
        mem_high_wat(0),
        mem_high_wat_percent(0),
        cursorDroppingLThreshold(0),
        cursorDroppingUThreshold(0),
        cursorsDropped(0),
        pagerRuns(0),
        expiryPagerRuns(0),
        itemsRemovedFromCheckpoints(0),
        numValueEjects(0),
        numFailedEjects(0),
        numNotMyVBuckets(0),
        currentSize(0),
        numBlob(0),
        blobOverhead(0),
        totalValueSize(0),
        numStoredVal(0),
        totalStoredValSize(0),
        storedValOverhead(0),
        memOverhead(0),
        numItem(0),
        totalMemory(0),
        memoryTrackerEnabled(false),
        forceShutdown(false),
        oom_errors(0),
        tmp_oom_errors(0),
        pendingOps(0),
        pendingOpsTotal(0),
        pendingOpsMax(0),
        pendingOpsMaxDuration(0),
        pendingCompactions(0),
        bg_fetched(0),
        bg_meta_fetched(0),
        numRemainingBgJobs(0),
        bgNumOperations(0),
        maxRemainingBgJobs(0),
        bgWait(0),
        bgMinWait(0),
        bgMaxWait(0),
        bgLoad(0),
        bgMinLoad(0),
        bgMaxLoad(0),
        vbucketDelMaxWalltime(0),
        vbucketDelTotWalltime(0),
        numTapFetched(0),
        numTapBGFetched(0),
        numTapBGFetchRequeued(0),
        numTapFGFetched(0),
        numTapDeletes(0),
        tapBgNumOperations(0),
        replicationThrottled(0),
        replicationThrottleThreshold(0),
        tapBgWait(0),
        tapBgMinWait(0),
        tapBgMaxWait(0),
        tapBgLoad(0),
        tapBgMinLoad(0),
        tapBgMaxLoad(0),
        numOpsStore(0),
        numOpsDelete(0),
        numOpsGet(0),
        numOpsGetMeta(0),
        numOpsSetMeta(0),
        numOpsDelMeta(0),
        numOpsSetMetaResolutionFailed(0),
        numOpsDelMetaResolutionFailed(0),
        numOpsSetRetMeta(0),
        numOpsDelRetMeta(0),
        numOpsGetMetaOnSetWithMeta(0),
        mlogCompactorRuns(0),
        alogRuns(0),
        alogNumItems(0),
        alogTime(0),
        alogRuntime(0),
        isShutdown(false),
        rollbackCount(0),
        defragNumVisited(0),
        defragNumMoved(0),
        dirtyAgeHisto(GrowingWidthGenerator<hrtime_t>(0, ONE_SECOND, 1.4), 25),
        diskCommitHisto(GrowingWidthGenerator<hrtime_t>(0, ONE_SECOND, 1.4), 25),
        mlogCompactorHisto(GrowingWidthGenerator<hrtime_t>(0, ONE_SECOND, 1.4), 25),
        timingLog(NULL),
        maxDataSize(DEFAULT_MAX_DATA_SIZE) {}

    ~EPStats() {
        delete timingLog;
    }

    size_t getMaxDataSize() {
        return maxDataSize.load();
    }

    void setMaxDataSize(size_t size) {
        if (size > 0) {
            maxDataSize.store(size);
        }
    }

    size_t getTotalMemoryUsed() {
        if (memoryTrackerEnabled.load()) {
            return totalMemory.load();
        }
        return currentSize.load() + memOverhead.load();
    }

    bool decrDiskQueueSize(size_t decrementBy) {
        size_t oldVal;
        do {
            oldVal = diskQueueSize.load();
            if (oldVal < decrementBy) {
                LOG(EXTENSION_LOG_DEBUG,
                    "Cannot decrement diskQueueSize by %" PRIu64 ", "
                    "the current value is %" PRIu64 "\n",
                    uint64_t(decrementBy), uint64_t(oldVal));
                return false;
            }
        } while (!diskQueueSize.compare_exchange_strong(oldVal, oldVal - decrementBy));
        return true;
    }

    //! Number of keys warmed up during key-only loading.
    AtomicValue<size_t> warmedUpKeys;
    //! Number of key-values warmed up during data loading.
    AtomicValue<size_t> warmedUpValues;
    //! Number of warmup failures due to duplicates
    AtomicValue<size_t> warmDups;
    //! Number of OOM failures at warmup time.
    AtomicValue<size_t> warmOOM;

    //! Fill % of memory used during warmup we're going to enable traffic
    AtomicValue<double> warmupMemUsedCap;
    //! Fill % of number of items read during warmup we're going to
    //  enable traffic
    AtomicValue<double> warmupNumReadCap;

    //! The replication throttle write queue cap
    AtomicValue<ssize_t> replicationThrottleWriteQueueCap;

    //! Amount of items waiting for persistence
    AtomicValue<size_t> diskQueueSize;
    //! Size of the in-process (output) queue.
    AtomicValue<size_t> flusher_todo;
    //! Number of transaction commits.
    AtomicValue<size_t> flusherCommits;
    //! Total time spent flushing.
    AtomicValue<size_t> cumulativeFlushTime;
    //! Total time spent committing.
    AtomicValue<size_t> cumulativeCommitTime;
    //! Objects that were rejected from persistence for being too fresh.
    AtomicValue<size_t> tooYoung;
    //! Objects that were forced into persistence for being too old.
    AtomicValue<size_t> tooOld;
    //! Number of items persisted.
    AtomicValue<size_t> totalPersisted;
    //! Cumulative number of items added to the queue.
    AtomicValue<size_t> totalEnqueued;
    //! Number of times an item flush failed.
    AtomicValue<size_t> flushFailed;
    //! Number of times an item is not flushed due to the item's expiry
    AtomicValue<size_t> flushExpired;
    //! Number of times an object was expired on access.
    AtomicValue<size_t> expired_access;
    //! Number of times an object was expired by compactor.
    AtomicValue<size_t> expired_compactor;
    //! Number of times an object was expired by pager.
    AtomicValue<size_t> expired_pager;
    //! Number of times we failed to start a transaction
    AtomicValue<size_t> beginFailed;
    //! Number of times a commit failed.
    AtomicValue<size_t> commitFailed;
    //! How long an object is dirty before written.
    AtomicValue<rel_time_t> dirtyAge;
    //! Oldest enqueued object we've seen while persisting.
    AtomicValue<rel_time_t> dirtyAgeHighWat;
    //! Amount of time spent in the commit phase.
    AtomicValue<rel_time_t> commit_time;
    //! Number of times we deleted a vbucket.
    AtomicValue<size_t> vbucketDeletions;
    //! Number of times we failed to delete a vbucket.
    AtomicValue<size_t> vbucketDeletionFail;

    //! Beyond this point are config items
    //! Pager low water mark.
    AtomicValue<size_t> mem_low_wat;
    AtomicValue<double> mem_low_wat_percent;
    //! Pager high water mark
    AtomicValue<size_t> mem_high_wat;
    AtomicValue<double> mem_high_wat_percent;

    //! Cursor dropping thresholds used by checkpoint remover
    AtomicValue<size_t> cursorDroppingLThreshold;
    AtomicValue<size_t> cursorDroppingUThreshold;

    //! Number of cursors dropped by checkpoint remover
    AtomicValue<size_t> cursorsDropped;

    //! Number of times we needed to kick in the pager
    AtomicValue<size_t> pagerRuns;
    //! Number of times the expiry pager runs for purging expired items
    AtomicValue<size_t> expiryPagerRuns;
    //! Number of items removed from closed unreferenced checkpoints.
    AtomicValue<size_t> itemsRemovedFromCheckpoints;
    //! Number of times a value is ejected
    AtomicValue<size_t> numValueEjects;
    //! Number of times a value could not be ejected
    AtomicValue<size_t> numFailedEjects;
    //! Number of times "Not my bucket" happened
    AtomicValue<size_t> numNotMyVBuckets;
    //! Total size of stored objects.
    AtomicValue<size_t> currentSize;
    //! Total number of blob objects
    AtomicValue<size_t> numBlob;
    //! Total size of blob memory overhead
    AtomicValue<size_t> blobOverhead;
    //! Total memory overhead to store values for resident keys.
    AtomicValue<size_t> totalValueSize;
    //! The number of storedVal object
    AtomicValue<size_t> numStoredVal;
    //! Total memory for stored values
    AtomicValue<size_t> totalStoredValSize;
    //! Total size of StoredVal memory overhead
    AtomicValue<size_t> storedValOverhead;
    //! Amount of memory used to track items and what-not.
    AtomicValue<size_t> memOverhead;
    //! Total number of Item objects
    AtomicValue<size_t> numItem;
    //! The total amount of memory used by this bucket (From memory tracking)
    AtomicValue<size_t> totalMemory;
    //! True if the memory usage tracker is enabled.
    AtomicValue<bool> memoryTrackerEnabled;
    //! Whether or not to force engine shutdown.
    AtomicValue<bool> forceShutdown;

    //! Number of times unrecoverable oom errors happened while processing operations.
    AtomicValue<size_t> oom_errors;
    //! Number of times temporary oom errors encountered while processing operations.
    AtomicValue<size_t> tmp_oom_errors;

    //! Number of ops blocked on all vbuckets in pending state
    AtomicValue<size_t> pendingOps;
    //! Total number of ops ever blocked on all vbuckets in pending state
    AtomicValue<size_t> pendingOpsTotal;
    //! High water value for ops blocked for any individual pending vbucket
    AtomicValue<size_t> pendingOpsMax;
    //! High water value for time an op is blocked on a pending vbucket
    AtomicValue<hrtime_t> pendingOpsMaxDuration;

    //! Histogram of pending operation wait times.
    Histogram<hrtime_t> pendingOpsHisto;

    //! Number of pending vbucket compaction requests
    AtomicValue<size_t> pendingCompactions;

    //! Number of times background fetches occurred.
    AtomicValue<size_t> bg_fetched;
    //! Number of times meta background fetches occurred.
    AtomicValue<size_t> bg_meta_fetched;
    //! Number of remaining bg fetch jobs.
    AtomicValue<size_t> numRemainingBgJobs;
    //! The number of samples the bgWaitDelta and bgLoadDelta contains of
    AtomicValue<size_t> bgNumOperations;
    //! Max number of individual background fetch jobs that we've seen in the queue
    size_t maxRemainingBgJobs;

    /** The sum of the deltas (in usec) from an item was put in queue until
     *  the dispatcher started the work for this item
     */
    AtomicValue<hrtime_t> bgWait;
    //! The shortest wait time
    AtomicValue<hrtime_t> bgMinWait;
    //! The longest wait time
    AtomicValue<hrtime_t> bgMaxWait;

    //! Histogram of background wait times.
    Histogram<hrtime_t> bgWaitHisto;

    /** The sum of the deltas (in usec) from the dispatcher started to load
     *  item until was done
     */
    AtomicValue<hrtime_t> bgLoad;
    //! The shortest load time
    AtomicValue<hrtime_t> bgMinLoad;
    //! The longest load time
    AtomicValue<hrtime_t> bgMaxLoad;

    //! Histogram of background wait loads.
    Histogram<hrtime_t> bgLoadHisto;

    //! Max wall time of deleting a vbucket
    AtomicValue<hrtime_t> vbucketDelMaxWalltime;
    //! Total wall time of deleting vbuckets
    AtomicValue<hrtime_t> vbucketDelTotWalltime;

    //! Histogram of setWithMeta latencies.
    Histogram<hrtime_t> setWithMetaHisto;

    //! Histogram of access scanner run times
    Histogram<hrtime_t> accessScannerHisto;
    //! Historgram of checkpoint remover run times
    Histogram<hrtime_t> checkpointRemoverHisto;
    //! Histogram of item pager run times
    Histogram<hrtime_t> itemPagerHisto;
    //! Histogram of expiry pager run times
    Histogram<hrtime_t> expiryPagerHisto;

    /* TAP related stats */
    //! The total number of tap events sent (not including noops)
    AtomicValue<size_t> numTapFetched;
    //! Number of background fetched tap items
    AtomicValue<size_t> numTapBGFetched;
    //! Number of times a tap background fetch task is requeued
    AtomicValue<size_t> numTapBGFetchRequeued;
    //! Number of foreground fetched tap items
    AtomicValue<size_t> numTapFGFetched;
    //! Number of tap deletes.
    AtomicValue<size_t> numTapDeletes;
    //! The number of samples the tapBgWaitDelta and tapBgLoadDelta contains of
    AtomicValue<size_t> tapBgNumOperations;
    //! The number of tap notify messages throttled by replicationThrottle.
    AtomicValue<size_t> replicationThrottled;
    //! Percentage of memory in use before we throttle replication input
    AtomicValue<double> replicationThrottleThreshold;

    /** The sum of the deltas (in usec) from a tap item was put in queue until
     *  the dispatcher started the work for this item
     */
    AtomicValue<hrtime_t> tapBgWait;
    //! The shortest tap bg wait time
    AtomicValue<hrtime_t> tapBgMinWait;
    //! The longest tap bg wait time
    AtomicValue<hrtime_t> tapBgMaxWait;

    //! Histogram of tap background wait loads.
    Histogram<hrtime_t> tapBgWaitHisto;

    /** The sum of the deltas (in usec) from the dispatcher started to load
     *  a tap item until was done
     */
    AtomicValue<hrtime_t> tapBgLoad;
    //! The shortest tap load time
    AtomicValue<hrtime_t> tapBgMinLoad;
    //! The longest tap load time
    AtomicValue<hrtime_t> tapBgMaxLoad;

    //! Histogram of tap background wait loads.
    Histogram<hrtime_t> tapBgLoadHisto;

    //! The number of basic store (add, set, arithmetic, touch, etc.) operations
    AtomicValue<size_t> numOpsStore;
    //! The number of basic delete operations
    AtomicValue<size_t> numOpsDelete;
    //! The number of basic get operations
    AtomicValue<size_t> numOpsGet;

    //! The number of get with meta operations
    AtomicValue<size_t>  numOpsGetMeta;
    //! The number of set with meta operations
    AtomicValue<size_t>  numOpsSetMeta;
    //! The number of delete with meta operations
    AtomicValue<size_t>  numOpsDelMeta;
    //! The number of failed set meta ops due to conflict resoltion
    AtomicValue<size_t> numOpsSetMetaResolutionFailed;
    //! The number of failed del meta ops due to conflict resoltion
    AtomicValue<size_t> numOpsDelMetaResolutionFailed;
    //! The number of set returning meta operations
    AtomicValue<size_t>  numOpsSetRetMeta;
    //! The number of delete returning meta operations
    AtomicValue<size_t>  numOpsDelRetMeta;
    //! The number of background get meta ops due to set_with_meta operations
    AtomicValue<size_t>  numOpsGetMetaOnSetWithMeta;

    //! The number of tiems the mutation log compactor is exectued
    AtomicValue<size_t> mlogCompactorRuns;
    //! The number of tiems the access scanner runs
    AtomicValue<size_t> alogRuns;
    //! The number of items that last access scanner task swept to log
    AtomicValue<size_t> alogNumItems;
    //! The next access scanner task schedule time (GMT)
    AtomicValue<hrtime_t> alogTime;
    //! The number of seconds that the last access scanner task took
    AtomicValue<rel_time_t> alogRuntime;

    AtomicValue<bool> isShutdown;

    AtomicValue<size_t> rollbackCount;

    /** The number of items that have been visited (considered for
     * defragmentation) by the defragmenter task.
     */
    AtomicValue<size_t> defragNumVisited;

    /** The number of items that have been moved (defragmented) by the
     * defragmenter task.
     */
    AtomicValue<size_t> defragNumMoved;

    //! Histogram of queue processing dirty age.
    Histogram<hrtime_t> dirtyAgeHisto;

    //! Histogram of item allocation sizes.
    Histogram<size_t> itemAllocSizeHisto;

    //
    // Command timers
    //

    //! Histogram of getvbucket timings
    Histogram<hrtime_t> getVbucketCmdHisto;

    //! Histogram of setvbucket timings
    Histogram<hrtime_t> setVbucketCmdHisto;

    //! Histogram of delvbucket timings
    Histogram<hrtime_t> delVbucketCmdHisto;

    //! Histogram of get commands.
    Histogram<hrtime_t> getCmdHisto;

    //! Histogram of store commands.
    Histogram<hrtime_t> storeCmdHisto;

    //! Histogram of arithmetic commands.
    Histogram<hrtime_t> arithCmdHisto;

    //! Histogram of tap VBucket reset timings
    Histogram<hrtime_t> tapVbucketResetHisto;

    //! Histogram of tap mutation timings.
    Histogram<hrtime_t> tapMutationHisto;

    //! Histogram of tap vbucket set timings.
    Histogram<hrtime_t> tapVbucketSetHisto;

    //! Time spent notifying completion of IO.
    Histogram<hrtime_t> notifyIOHisto;

    //! Histogram of get_stats commands.
    Histogram<hrtime_t> getStatsCmdHisto;

    //! Histogram of wait_for_checkpoint_persistence command
    Histogram<hrtime_t> chkPersistenceHisto;

    //
    // DB timers.
    //

    //! Histogram of insert disk writes
    Histogram<hrtime_t> diskInsertHisto;

    //! Histogram of update disk writes
    Histogram<hrtime_t> diskUpdateHisto;

    //! Histogram of delete disk writes
    Histogram<hrtime_t> diskDelHisto;

    //! Histogram of execution time of disk vbucket deletions
    Histogram<hrtime_t> diskVBDelHisto;

    //! Histogram of disk commits
    Histogram<hrtime_t> diskCommitHisto;

    //! Histogram of setting vbucket state
    Histogram<hrtime_t> snapshotVbucketHisto;

    //! Histogram of mutation log compactor
    Histogram<hrtime_t> mlogCompactorHisto;

    //! Historgram of batch reads
    Histogram<hrtime_t> getMultiHisto;

    // ! Histogram of various task wait times
    Histogram<hrtime_t> *schedulingHisto;

    // ! Histogram of various task run times
    Histogram<hrtime_t> *taskRuntimeHisto;

    //! Reset all stats to reasonable values.
    void reset() {
        tooYoung.store(0);
        tooOld.store(0);
        dirtyAge.store(0);
        dirtyAgeHighWat.store(0);
        commit_time.store(0);
        pagerRuns.store(0);
        itemsRemovedFromCheckpoints.store(0);
        numValueEjects.store(0);
        numFailedEjects.store(0);
        numNotMyVBuckets.store(0);
        bgNumOperations.store(0);
        bgWait.store(0);
        bgLoad.store(0);
        bgMinWait.store(999999999);
        bgMaxWait.store(0);
        bgMinLoad.store(999999999);
        bgMaxLoad.store(0);
        tapBgNumOperations.store(0);
        tapBgWait.store(0);
        tapBgLoad.store(0);
        tapBgMinWait.store(999999999);
        tapBgMaxWait.store(0);
        tapBgMinLoad.store(999999999);
        tapBgMaxLoad.store(0);
        replicationThrottled.store(0);
        oom_errors.store(0);
        tmp_oom_errors.store(0);
        pendingOps.store(0);
        pendingOpsTotal.store(0);
        pendingOpsMax.store(0);
        pendingOpsMaxDuration.store(0);
        numTapFetched.store(0);
        vbucketDelMaxWalltime.store(0);
        vbucketDelTotWalltime.store(0);

        mlogCompactorRuns.store(0);
        alogRuns.store(0);
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
        tapBgWaitHisto.reset();
        tapBgLoadHisto.reset();
        getVbucketCmdHisto.reset();
        setVbucketCmdHisto.reset();
        delVbucketCmdHisto.reset();
        getCmdHisto.reset();
        storeCmdHisto.reset();
        arithCmdHisto.reset();
        tapVbucketResetHisto.reset();
        tapMutationHisto.reset();
        tapVbucketSetHisto.reset();
        notifyIOHisto.reset();
        getStatsCmdHisto.reset();
        chkPersistenceHisto.reset();
        diskInsertHisto.reset();
        diskUpdateHisto.reset();
        diskDelHisto.reset();
        diskVBDelHisto.reset();
        diskCommitHisto.reset();

        itemAllocSizeHisto.reset();
        dirtyAgeHisto.reset();
        mlogCompactorHisto.reset();
        getMultiHisto.reset();
    }

    // Used by stats logging infrastructure.
    std::ostream *timingLog;

private:

    //! Max allowable memory size.
    AtomicValue<size_t> maxDataSize;

    DISALLOW_COPY_AND_ASSIGN(EPStats);
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
};

#endif  // SRC_STATS_H_
