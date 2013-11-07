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

    EPStats() : maxRemainingBgJobs(0),
                dirtyAgeHisto(GrowingWidthGenerator<hrtime_t>(0, ONE_SECOND, 1.4), 25),
                diskCommitHisto(GrowingWidthGenerator<hrtime_t>(0, ONE_SECOND, 1.4), 25),
                mlogCompactorHisto(GrowingWidthGenerator<hrtime_t>(0, ONE_SECOND, 1.4), 25),
                timingLog(NULL), maxDataSize(DEFAULT_MAX_DATA_SIZE) {}

    ~EPStats() {
        delete timingLog;
    }

    size_t getMaxDataSize() {
        return maxDataSize.get();
    }

    void setMaxDataSize(size_t size) {
        if (size > 0) {
            maxDataSize.set(size);
        }
    }

    size_t getTotalMemoryUsed() {
        if (memoryTrackerEnabled.get()) {
            return totalMemory.get();
        }
        return currentSize.get() + memOverhead.get();
    }

    bool decrDiskQueueSize(size_t decrementBy) {
        size_t oldVal;
        do {
            oldVal = diskQueueSize.get();
            if (oldVal < decrementBy) {
                LOG(EXTENSION_LOG_WARNING,
                    "Warning: cannot decrement diskQueueSize by %lld, "
                    "the current value is %lld\n", decrementBy, oldVal);
                return false;
            }
        } while (!diskQueueSize.cas(oldVal, oldVal - decrementBy));
        return true;
    }

    //! Number of keys warmed up during key-only loading.
    Atomic<size_t> warmedUpKeys;
    //! Number of key-values warmed up during data loading.
    Atomic<size_t> warmedUpValues;
    //! Number of warmup failures due to duplicates
    Atomic<size_t> warmDups;
    //! Number of OOM failures at warmup time.
    Atomic<size_t> warmOOM;
    //! Number of expired keys during data loading
    Atomic<size_t> warmupExpired;

    //! Fill % of memory used during warmup we're going to enable traffic
    Atomic<double> warmupMemUsedCap;
    //! Fill % of number of items read during warmup we're going to
    //  enable traffic
    Atomic<double> warmupNumReadCap;

    //! The tap throttle write queue cap
    Atomic<ssize_t> tapThrottleWriteQueueCap;

    //! Amount of items waiting for persistence
    Atomic<size_t> diskQueueSize;
    //! Size of the in-process (output) queue.
    Atomic<size_t> flusher_todo;
    //! Number of transaction commits.
    Atomic<size_t> flusherCommits;
    //! Total time spent flushing.
    Atomic<size_t> cumulativeFlushTime;
    //! Total time spent committing.
    Atomic<size_t> cumulativeCommitTime;
    //! Objects that were rejected from persistence for being too fresh.
    Atomic<size_t> tooYoung;
    //! Objects that were forced into persistence for being too old.
    Atomic<size_t> tooOld;
    //! Number of items persisted.
    Atomic<size_t> totalPersisted;
    //! Cumulative number of items added to the queue.
    Atomic<size_t> totalEnqueued;
    //! Number of new items created in the DB.
    Atomic<size_t> newItems;
    //! Number of items removed from the DB.
    Atomic<size_t> delItems;
    //! Number of times an item flush failed.
    Atomic<size_t> flushFailed;
    //! Number of times an item is not flushed due to the item's expiry
    Atomic<size_t> flushExpired;
    //! Number of times an object was expired on access.
    Atomic<size_t> expired_access;
    //! Number of times an object was expired by pager.
    Atomic<size_t> expired_pager;
    //! Number of times we failed to start a transaction
    Atomic<size_t> beginFailed;
    //! Number of times a commit failed.
    Atomic<size_t> commitFailed;
    //! How long an object is dirty before written.
    Atomic<rel_time_t> dirtyAge;
    //! Oldest enqueued object we've seen while persisting.
    Atomic<rel_time_t> dirtyAgeHighWat;
    //! Amount of time spent in the commit phase.
    Atomic<rel_time_t> commit_time;
    //! Number of times we deleted a vbucket.
    Atomic<size_t> vbucketDeletions;
    //! Number of times we failed to delete a vbucket.
    Atomic<size_t> vbucketDeletionFail;

    //! Beyond this point are config items
    //! Pager low water mark.
    Atomic<size_t> mem_low_wat;
    //! Pager high water mark
    Atomic<size_t> mem_high_wat;

    //! Number of times we needed to kick in the pager
    Atomic<size_t> pagerRuns;
    //! Number of times the expiry pager runs for purging expired items
    Atomic<size_t> expiryPagerRuns;
    //! Number of items removed from closed unreferenced checkpoints.
    Atomic<size_t> itemsRemovedFromCheckpoints;
    //! Number of times a value is ejected
    Atomic<size_t> numValueEjects;
    //! Number of times a value could not be ejected
    Atomic<size_t> numFailedEjects;
    //! Number of times "Not my bucket" happened
    Atomic<size_t> numNotMyVBuckets;
    //! Total size of stored objects.
    Atomic<size_t> currentSize;
    //! Total memory overhead to store values for resident keys.
    Atomic<size_t> totalValueSize;
    //! Amount of memory used to track items and what-not.
    Atomic<size_t> memOverhead;
    //! The total amount of memory used by this bucket (From memory tracking)
    Atomic<size_t> totalMemory;
    //! True if the memory usage tracker is enabled.
    Atomic<bool> memoryTrackerEnabled;
    //! Whether or not to force engine shutdown.
    Atomic<bool> forceShutdown;

    //! Number of times unrecoverable oom errors happened while processing operations.
    Atomic<size_t> oom_errors;
    //! Number of times temporary oom errors encountered while processing operations.
    Atomic<size_t> tmp_oom_errors;

    //! Number of read related io operations
    Atomic<size_t> io_num_read;
    //! Number of write related io operations
    Atomic<size_t> io_num_write;
    //! Number of bytes read
    Atomic<size_t> io_read_bytes;
    //! Number of bytes written
    Atomic<size_t> io_write_bytes;

    //! Number of ops blocked on all vbuckets in pending state
    Atomic<size_t> pendingOps;
    //! Total number of ops ever blocked on all vbuckets in pending state
    Atomic<size_t> pendingOpsTotal;
    //! High water value for ops blocked for any individual pending vbucket
    Atomic<size_t> pendingOpsMax;
    //! High water value for time an op is blocked on a pending vbucket
    Atomic<hrtime_t> pendingOpsMaxDuration;

    //! Histogram of pending operation wait times.
    Histogram<hrtime_t> pendingOpsHisto;

    //! Number of pending vbucket compaction requests
    Atomic<size_t> pendingCompactions;

    //! Number of times background fetches occurred.
    Atomic<size_t> bg_fetched;
    //! Number of times meta background fetches occurred.
    Atomic<size_t> bg_meta_fetched;
    //! Number of remaining bg fetch jobs.
    Atomic<size_t> numRemainingBgJobs;
    //! The number of samples the bgWaitDelta and bgLoadDelta contains of
    Atomic<size_t> bgNumOperations;
    //! Max number of individual background fetch jobs that we've seen in the queue
    size_t maxRemainingBgJobs;

    /** The sum of the deltas (in usec) from an item was put in queue until
     *  the dispatcher started the work for this item
     */
    Atomic<hrtime_t> bgWait;
    //! The shortest wait time
    Atomic<hrtime_t> bgMinWait;
    //! The longest wait time
    Atomic<hrtime_t> bgMaxWait;

    //! Histogram of background wait times.
    Histogram<hrtime_t> bgWaitHisto;

    /** The sum of the deltas (in usec) from the dispatcher started to load
     *  item until was done
     */
    Atomic<hrtime_t> bgLoad;
    //! The shortest load time
    Atomic<hrtime_t> bgMinLoad;
    //! The longest load time
    Atomic<hrtime_t> bgMaxLoad;

    //! Histogram of background wait loads.
    Histogram<hrtime_t> bgLoadHisto;

    //! Max wall time of deleting a vbucket
    Atomic<hrtime_t> vbucketDelMaxWalltime;
    //! Total wall time of deleting vbuckets
    Atomic<hrtime_t> vbucketDelTotWalltime;

    //! Histogram of setWithMeta latencies.
    Histogram<hrtime_t> setWithMetaHisto;

    /* TAP related stats */
    //! The total number of tap events sent (not including noops)
    Atomic<size_t> numTapFetched;
    //! Number of background fetched tap items
    Atomic<size_t> numTapBGFetched;
    //! Number of times a tap background fetch task is requeued
    Atomic<size_t> numTapBGFetchRequeued;
    //! Number of foreground fetched tap items
    Atomic<size_t> numTapFGFetched;
    //! Number of tap deletes.
    Atomic<size_t> numTapDeletes;
    //! The number of samples the tapBgWaitDelta and tapBgLoadDelta contains of
    Atomic<size_t> tapBgNumOperations;
    //! The number of tap notify messages throttled by TapThrottle.
    Atomic<size_t> tapThrottled;
    //! Percentage of memory in use before we throttle tap input
    Atomic<double> tapThrottleThreshold;

    /** The sum of the deltas (in usec) from a tap item was put in queue until
     *  the dispatcher started the work for this item
     */
    Atomic<hrtime_t> tapBgWait;
    //! The shortest tap bg wait time
    Atomic<hrtime_t> tapBgMinWait;
    //! The longest tap bg wait time
    Atomic<hrtime_t> tapBgMaxWait;

    //! Histogram of tap background wait loads.
    Histogram<hrtime_t> tapBgWaitHisto;

    /** The sum of the deltas (in usec) from the dispatcher started to load
     *  a tap item until was done
     */
    Atomic<hrtime_t> tapBgLoad;
    //! The shortest tap load time
    Atomic<hrtime_t> tapBgMinLoad;
    //! The longest tap load time
    Atomic<hrtime_t> tapBgMaxLoad;

    //! Histogram of tap background wait loads.
    Histogram<hrtime_t> tapBgLoadHisto;

    //! The number of get with meta operations
    Atomic<size_t>  numOpsGetMeta;
    //! The number of set with meta operations
    Atomic<size_t>  numOpsSetMeta;
    //! The number of delete with meta operations
    Atomic<size_t>  numOpsDelMeta;
    //! The number of failed set meta ops due to conflict resoltion
    Atomic<size_t> numOpsSetMetaResolutionFailed;
    //! The number of failed del meta ops due to conflict resoltion
    Atomic<size_t> numOpsDelMetaResolutionFailed;
    //! The number of set returning meta operations
    Atomic<size_t>  numOpsSetRetMeta;
    //! The number of delete returning meta operations
    Atomic<size_t>  numOpsDelRetMeta;
    //! The number of background get meta ops due to set_with_meta operations
    Atomic<size_t>  numOpsGetMetaOnSetWithMeta;

    //! The number of tiems the mutation log compactor is exectued
    Atomic<size_t> mlogCompactorRuns;
    //! The number of tiems the access scanner runs
    Atomic<size_t> alogRuns;
    //! The number of items that last access scanner task swept to log
    Atomic<size_t> alogNumItems;
    //! The next access scanner task schedule time (GMT)
    Atomic<hrtime_t> alogTime;
    //! The number of seconds that the last access scanner task took
    Atomic<rel_time_t> alogRuntime;

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

    //! Reset all stats to reasonable values.
    void reset() {
        tooYoung.set(0);
        tooOld.set(0);
        dirtyAge.set(0);
        dirtyAgeHighWat.set(0);
        commit_time.set(0);
        pagerRuns.set(0);
        itemsRemovedFromCheckpoints.set(0);
        numValueEjects.set(0);
        numFailedEjects.set(0);
        numNotMyVBuckets.set(0);
        io_num_read.set(0);
        io_num_write.set(0);
        io_read_bytes.set(0);
        io_write_bytes.set(0);
        bgNumOperations.set(0);
        bgWait.set(0);
        bgLoad.set(0);
        bgMinWait.set(999999999);
        bgMaxWait.set(0);
        bgMinLoad.set(999999999);
        bgMaxLoad.set(0);
        tapBgNumOperations.set(0);
        tapBgWait.set(0);
        tapBgLoad.set(0);
        tapBgMinWait.set(999999999);
        tapBgMaxWait.set(0);
        tapBgMinLoad.set(999999999);
        tapBgMaxLoad.set(0);
        tapThrottled.set(0);
        pendingOps.set(0);
        pendingOpsTotal.set(0);
        pendingOpsMax.set(0);
        pendingOpsMaxDuration.set(0);
        numTapFetched.set(0);
        vbucketDelMaxWalltime.set(0);
        vbucketDelTotWalltime.set(0);

        mlogCompactorRuns.set(0);
        alogRuns.set(0);

        pendingOpsHisto.reset();
        bgWaitHisto.reset();
        bgLoadHisto.reset();
        setWithMetaHisto.reset();
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

    struct Shutdown {
        Shutdown() : isShutdown(false) {}
        bool isShutdown;
        Mutex mutex;
    } shutdown;

private:

    //! Max allowable memory size.
    Atomic<size_t> maxDataSize;

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
