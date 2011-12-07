/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef STATS_HH
#define STATS_HH 1

#include <map>
#include <memcached/engine.h>

#include "common.hh"
#include "atomic.hh"
#include "histo.hh"

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

    EPStats() : maxDataSize(DEFAULT_MAX_DATA_SIZE),
                dirtyAgeHisto(GrowingWidthGenerator<hrtime_t>(0, ONE_SECOND, 1.4), 25),
                dataAgeHisto(GrowingWidthGenerator<hrtime_t>(0, ONE_SECOND, 1.4), 25),
                diskCommitHisto(GrowingWidthGenerator<hrtime_t>(0, ONE_SECOND, 1.4), 25),
                timingLog(NULL) {}

    ~EPStats() {
        delete timingLog;
    }


    //! How long it took us to load the keys from disk.
    Atomic<hrtime_t> warmupKeysTime;
    //! How long it took us to load the data from disk.
    Atomic<hrtime_t> warmupTime;
    //! Whether we're warming up.
    Atomic<bool> warmupComplete;
    //! Number of records warmed up.
    Atomic<size_t> warmedUp;
    //! Number of warmup failures due to duplicates
    Atomic<size_t> warmDups;
    //! Number of OOM failures at warmup time.
    Atomic<size_t> warmOOM;
    //! The tap throttle write queue cap
    Atomic<size_t> tapThrottleWriteQueueCap;

    //! size of the input queue
    Atomic<size_t> queue_size;
    //! Size of the in-process (output) queue.
    Atomic<size_t> flusher_todo;
    //! Number of transaction commits.
    Atomic<size_t> flusherCommits;
    //! Number of times the flusher was preempted for a read
    Atomic<size_t> flusherPreempts;
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
    Atomic<size_t> expired;
    //! Number of times we failed to start a transaction
    Atomic<size_t> beginFailed;
    //! Number of times a commit failed.
    Atomic<size_t> commitFailed;
    //! How long an object is dirty before written.
    Atomic<rel_time_t> dirtyAge;
    //! Oldest enqueued object we've seen while persisting.
    Atomic<rel_time_t> dirtyAgeHighWat;
    //! How old persisted data was when it hit the persistence layer
    Atomic<rel_time_t> dataAge;
    //! Oldest data we've seen while persisting.
    Atomic<rel_time_t> dataAgeHighWat;
    //! How long does it take to do an entire flush cycle.
    Atomic<rel_time_t> flushDuration;
    //! Longest flush cycle we've seen.
    Atomic<rel_time_t> flushDurationHighWat;
    //! Amount of time spent in the commit phase.
    Atomic<rel_time_t> commit_time;
    //! Number of times we deleted a vbucket.
    Atomic<size_t> vbucketDeletions;
    //! Number of times we failed to delete a vbucket.
    Atomic<size_t> vbucketDeletionFail;
    //! Beyond this point are config items
    //! Minimum data age before a record can be persisted
    Atomic<int> min_data_age;
    //! Maximum data age before a record is forced to be persisted
    Atomic<int> queue_age_cap;
    //! Number of times background fetches occurred.
    Atomic<size_t> bg_fetched;
    //! Number of times we needed to kick in the pager
    Atomic<size_t> pagerRuns;
    //! Number of times the expiry pager runs for purging expired items
    Atomic<size_t> expiryPagerRuns;
    //! Number of times the checkpoint remover runs for removing closed unreferenced checkpoints.
    Atomic<size_t> checkpointRemoverRuns;
    //! Number of items removed from closed unreferenced checkpoints.
    Atomic<size_t> itemsRemovedFromCheckpoints;
    //! Number of times a value is ejected
    Atomic<size_t> numValueEjects;
    //! Number of times a replica value is ejected
    Atomic<size_t> numReplicaEjects;
    //! Number of times a value could not be ejected
    Atomic<size_t> numFailedEjects;
    //! Number of times "Not my bucket" happened
    Atomic<size_t> numNotMyVBuckets;
    //! Whether the DB cleaner completes cleaning up invalid items with old vb versions
    Atomic<bool> dbCleanerComplete;
    //! Number of deleted items reverted from hot reload
    Atomic<size_t> numRevertDeletes;
    //! Number of new items reverted from hot reload
    Atomic<size_t> numRevertAdds;
    //! Number of updated items reverted from hot reload
    Atomic<size_t> numRevertUpdates;
    //! Max allowable memory size.
    Atomic<size_t> maxDataSize;
    //! Total size of stored objects.
    Atomic<size_t> currentSize;
    //! Total memory overhead to store values for resident keys.
    Atomic<size_t> totalValueSize;
    //! Amount of memory used to track items and what-not.
    Atomic<size_t> memOverhead;

    //! Pager low water mark.
    Atomic<size_t> mem_low_wat;
    //! Pager high water mark
    Atomic<size_t> mem_high_wat;

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

    //! The number of samples the bgWaitDelta and bgLoadDelta contains of
    Atomic<size_t> bgNumOperations;
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

    //! Max wall time of deleting a vbucket
    Atomic<hrtime_t> vbucketDelMaxWalltime;
    //! Total wall time of deleting vbuckets
    Atomic<hrtime_t> vbucketDelTotWalltime;

    //! Histogram of background wait loads.
    Histogram<hrtime_t> bgLoadHisto;

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

    //
    // Observe Stats
    //

    //! The number of observe sets
    Atomic<size_t> totalObserveSets;
    //! The number of stats observe polls
    Atomic<size_t> statsObservePolls;
    //! The number of observe polls
    Atomic<size_t> observeCalls;
    //! The number of unobserve polls
    Atomic<size_t> unobserveCalls;
    //! The number of items in the observe registry
    Atomic<size_t> obsRegSize;
    //! The number of observe errors
    Atomic<size_t> obsErrors;
    //! The number of times the observe registry cleaner has run
    Atomic<size_t> obsCleanerRuns;


    //! Histogram of tap background wait loads.
    Histogram<hrtime_t> tapBgLoadHisto;

    //! Histogram of queue processing dirty age.
    Histogram<hrtime_t> dirtyAgeHisto;
    //! Histogram of queue processing data age.
    Histogram<hrtime_t> dataAgeHisto;

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

    //
    // DB timers.
    //

    //! Histogram of insert disk writes
    Histogram<hrtime_t> diskInsertHisto;

    //! Histogram of update disk writes
    Histogram<hrtime_t> diskUpdateHisto;

    //! Histogram of delete disk writes
    Histogram<hrtime_t> diskDelHisto;

    //! Histogram of execution time of disk vbucket chunk deletions
    Histogram<hrtime_t> diskVBChunkDelHisto;

    //! Histogram of execution time of disk vbucket deletions
    Histogram<hrtime_t> diskVBDelHisto;

    //! Histogram of execution time of invalid vbucket table deletions from disk
    Histogram<hrtime_t> diskInvalidVBTableDelHisto;

    //! Histogram of disk commits
    Histogram<hrtime_t> diskCommitHisto;

    //! Histogram of purging a chunk of items with the old vbucket version from disk
    Histogram<hrtime_t> diskInvaidItemDelHisto;

    Histogram<hrtime_t> checkpointRevertHisto;

    //! Histogram of setting vbucket state
    Histogram<hrtime_t> setVbucketStateHisto;
    Histogram<hrtime_t> snapshotVbucketHisto;
    Histogram<hrtime_t> couchDelqHisto;

    Histogram<hrtime_t> couchGetHisto;
    Histogram<hrtime_t> couchGetFailHisto;
    Histogram<hrtime_t> couchSetHisto;
    Histogram<hrtime_t> couchSetFailHisto;


    //! Reset all stats to reasonable values.
    void reset() {
        tooYoung.set(0);
        tooOld.set(0);
        dirtyAge.set(0);
        dirtyAgeHighWat.set(0);
        flushDuration.set(0);
        flushDurationHighWat.set(0);
        commit_time.set(0);
        pagerRuns.set(0);
        checkpointRemoverRuns.set(0);
        itemsRemovedFromCheckpoints.set(0);
        numValueEjects.set(0);
        numFailedEjects.set(0);
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

        statsObservePolls.set(0);
        observeCalls.set(0);
        unobserveCalls.set(0);
        obsErrors.set(0);
        obsCleanerRuns.set(0);

        pendingOpsHisto.reset();
        bgWaitHisto.reset();
        bgLoadHisto.reset();
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
        diskInsertHisto.reset();
        diskUpdateHisto.reset();
        diskDelHisto.reset();
        diskVBChunkDelHisto.reset();
        diskVBDelHisto.reset();
        diskInvalidVBTableDelHisto.reset();
        diskCommitHisto.reset();
        diskInvaidItemDelHisto.reset();

        dataAgeHisto.reset();
        dirtyAgeHisto.reset();
    }

    // Used by stats logging infrastructure.
    std::ostream *timingLog;

private:

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
    //! When the item was dirtied (if applicable).
    rel_time_t dirtied;
    //! How long the item has been dirty (if applicable).
    rel_time_t data_age;
    //! Last modification time
    rel_time_t last_modification_time;
    //! The item's current flags.
    uint32_t flags;
    //! True if this item is dirty.
    bool dirty;
};

/**
 * Stats returned by the underlying memory allocator.
 */
class MemoryAllocatorStats {
public:
    static void getAllocatorStats(std::map<std::string, size_t> &allocator_stats) {
#if defined(HAVE_LIBTCMALLOC) || defined(HAVE_LIBTCMALLOC_MINIMAL)
        TCMallocStats::getStats(allocator_stats);
#else
        (void) allocator_stats;
#endif
    }
};

#endif /* STATS_HH */
