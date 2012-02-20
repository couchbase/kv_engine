/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 NorthScale, Inc.
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

#include "config.h"
#include <vector>
#include <time.h>
#include <string.h>
#include <sstream>
#include <iostream>
#include <fstream>
#include <functional>

#include "ep.hh"
#include "flusher.hh"
#include "statsnap.hh"
#include "locks.hh"
#include "dispatcher.hh"
#include "kvstore.hh"
#include "ep_engine.h"
#include "htresizer.hh"
#include "checkpoint_remover.hh"
#include "invalid_vbtable_remover.hh"

extern "C" {
    static rel_time_t uninitialized_current_time(void) {
        abort();
        return 0;
    }

    static time_t default_abs_time(rel_time_t) {
        abort();
        return 0;
    }

    static rel_time_t default_reltime(time_t) {
        abort();
        return 0;
    }

    rel_time_t (*ep_current_time)() = uninitialized_current_time;
    time_t (*ep_abs_time)(rel_time_t) = default_abs_time;
    rel_time_t (*ep_reltime)(time_t) = default_reltime;

    time_t ep_real_time() {
        return ep_abs_time(ep_current_time());
    }
}

class StatsValueChangeListener : public ValueChangedListener {
public:
    StatsValueChangeListener(EPStats &st) : stats(st) {
        // EMPTY
    }

    virtual void sizeValueChanged(const std::string &key, size_t value) {
        if (key.compare("min_data_age") == 0) {
            stats.min_data_age.set(value);
        } else if (key.compare("queue_age_cap") == 0) {
            stats.queue_age_cap.set(value);
        } else if (key.compare("tap_throttle_threshold") == 0) {
            stats.tapThrottleThreshold.set(static_cast<double>(value) / 100.0);
        } else if (key.compare("tap_throttle_queue_cap") == 0) {
            stats.tapThrottleWriteQueueCap.set(value);
        }
    }

private:
    EPStats &stats;
};

/**
 * Dispatcher job to notify the underlying kv storage of a new vbucket batch count
 */
class VBucketBatchCountCallback : public DispatcherCallback {
public:
    VBucketBatchCountCallback(KVStore *s, size_t batch_count) :
        kvStore(s), batchCount(batch_count) { }

    bool callback(Dispatcher &, TaskId) {
        kvStore->setVBBatchCount(batchCount);
        return false;
    }

    std::string description() {
        std::stringstream ss;
        ss << "Notifying the kv storage of a new vbucket batch count " << batchCount;
        return ss.str();
    }

private:
    KVStore *kvStore;
    size_t batchCount;
};

/**
 * A configuration value changed listener that responds to ep-engine
 * parameter changes by invoking engine-specific methods on
 * configuration change events.
 */
class EPStoreValueChangeListener : public ValueChangedListener {
public:
    EPStoreValueChangeListener(EventuallyPersistentStore &st) : store(st) {
    }

    virtual void sizeValueChanged(const std::string &key, size_t value) {
        if (key.compare("bg_fetch_delay") == 0) {
            store.setBGFetchDelay(static_cast<uint32_t>(value));
        } else if (key.compare("expiry_window") == 0) {
            store.setItemExpiryWindow(value);
        } else if (key.compare("vb_del_chunk_size") == 0) {
            store.setVbDelChunkSize(value);
        } else if (key.compare("vb_chunk_del_time") == 0) {
            store.setVbChunkDelThresholdTime(value);
        } else if (key.compare("max_txn_size") == 0) {
            store.setTxnSize(value);
        } else if (key.compare("exp_pager_stime") == 0) {
            store.setExpiryPagerSleeptime(value);
        } else if (key.compare("couch_vbucket_batch_count") == 0) {
            shared_ptr<DispatcherCallback> cb(new VBucketBatchCountCallback(store.getRWUnderlying(),
                                                                            value));
            store.getDispatcher()->schedule(cb, NULL,
                                            Priority::VBucketBatchCountPriority,
                                            0, false);
        }
    }

private:
    EventuallyPersistentStore &store;
};

/**
 * Dispatcher job that performs disk fetches for non-resident get
 * requests.
 */
class BGFetchCallback : public DispatcherCallback {
public:
    BGFetchCallback(EventuallyPersistentStore *e,
                    const std::string &k, uint16_t vbid, uint16_t vbv,
                    uint64_t r, const void *c) :
        ep(e), key(k), vbucket(vbid), vbver(vbv), rowid(r), cookie(c),
        counter(ep->bgFetchQueue), init(gethrtime()) {
        assert(ep);
        assert(cookie);
    }

    bool callback(Dispatcher &, TaskId) {
        ep->completeBGFetch(key, vbucket, vbver, rowid, cookie, init);
        return false;
    }

    std::string description() {
        std::stringstream ss;
        ss << "Fetching item from disk:  " << key;
        return ss.str();
    }

private:
    EventuallyPersistentStore *ep;
    std::string                key;
    uint16_t                   vbucket;
    uint16_t                   vbver;
    uint64_t                   rowid;
    const void                *cookie;
    BGFetchCounter             counter;

    hrtime_t init;
};

/**
 * Dispatcher job for performing disk fetches for "stats vkey".
 */
class VKeyStatBGFetchCallback : public DispatcherCallback {
public:
    VKeyStatBGFetchCallback(EventuallyPersistentStore *e,
                            const std::string &k, uint16_t vbid, uint16_t vbv,
                            uint64_t r,
                            const void *c, shared_ptr<Callback<GetValue> > cb) :
        ep(e), key(k), vbucket(vbid), vbver(vbv), rowid(r), cookie(c),
        lookup_cb(cb), counter(e->bgFetchQueue) {
        assert(ep);
        assert(cookie);
        assert(lookup_cb);
    }

    bool callback(Dispatcher &, TaskId) {
        RememberingCallback<GetValue> gcb;

        ep->getROUnderlying()->get(key, rowid, vbucket, vbver, gcb);
        gcb.waitForValue();
        assert(gcb.fired);
        lookup_cb->callback(gcb.val);

        return false;
    }

    std::string description() {
        std::stringstream ss;
        ss << "Fetching item from disk for vkey stat:  " << key;
        return ss.str();
    }

private:
    EventuallyPersistentStore       *ep;
    std::string                      key;
    uint16_t                         vbucket;
    uint16_t                         vbver;
    uint64_t                         rowid;
    const void                      *cookie;
    shared_ptr<Callback<GetValue> >  lookup_cb;
    BGFetchCounter                   counter;
};

/**
 * Dispatcher job responsible for keeping the current state of
 * vbuckets recorded in the main db.
 */
class SnapshotVBucketsCallback : public DispatcherCallback {
public:
    SnapshotVBucketsCallback(EventuallyPersistentStore *e, const Priority &p)
        : ep(e), priority(p) { }

    bool callback(Dispatcher &, TaskId) {
        ep->snapshotVBuckets(priority);
        return false;
    }

    std::string description() {
        return "Snapshotting vbuckets";
    }
private:
    EventuallyPersistentStore *ep;
    const Priority &priority;
};

/**
 * Dispatcher job to perform fast vbucket deletion.
 */
class FastVBucketDeletionCallback : public DispatcherCallback {
public:
    FastVBucketDeletionCallback(EventuallyPersistentStore *e, RCPtr<VBucket> vb,
                                uint16_t vbv, EPStats &st) : ep(e),
                                                             vbucket(vb->getId()),
                                                             vbver(vbv),
                                               stats(st) {}

    bool callback(Dispatcher &, TaskId) {
        bool rv(true); // try again by default
        hrtime_t start_time(gethrtime());
        vbucket_del_result result = ep->completeVBucketDeletion(vbucket, vbver);
        if (result == vbucket_del_success || result == vbucket_del_invalid) {
            hrtime_t spent(gethrtime() - start_time);
            hrtime_t wall_time = spent / 1000;
            BlockTimer::log(spent, "disk_vb_del", stats.timingLog);
            stats.diskVBDelHisto.add(wall_time);
            stats.vbucketDelMaxWalltime.setIfBigger(wall_time);
            stats.vbucketDelTotWalltime.incr(wall_time);
            rv = false;
        }
        return rv;
    }

    std::string description() {
        std::stringstream ss;
        ss << "Removing vbucket " << vbucket << " from disk";
        return ss.str();
    }

private:
    EventuallyPersistentStore *ep;
    uint16_t vbucket;
    uint16_t vbver;
    EPStats &stats;
};

/**
 * Dispatcher job to perform ranged vbucket deletion.
 */
class VBucketDeletionCallback : public DispatcherCallback {
public:
    VBucketDeletionCallback(EventuallyPersistentStore *e, RCPtr<VBucket> vb,
                            uint16_t vbucket_version, EPStats &st,
                            size_t csize = 100, uint32_t chunk_del_time = 500)
        : ep(e), stats(st), vb_version(vbucket_version),
          chunk_size(csize), chunk_del_threshold_time(chunk_del_time),
          vbdv(csize) {
        assert(ep);
        assert(vb);
        chunk_num = 1;
        execution_time = 0;
        start_wall_time = gethrtime();
        vbucket = vb->getId();
        vb->ht.visit(vbdv);
        vbdv.createRangeList(range_list);
        current_range = range_list.begin();
        if (current_range != range_list.end()) {
            chunk_del_range_size = current_range->second - current_range->first;
        } else {
            chunk_del_range_size = 100;
        }
    }

    bool callback(Dispatcher &d, TaskId t) {
        bool rv = false, isLastChunk = false;

        chunk_range_t range;
        if (current_range == range_list.end()) {
            range.first = -1;
            range.second = -1;
            isLastChunk = true;
        } else {
            if (current_range->second == range_list.back().second) {
                isLastChunk = true;
            }
            range.first = current_range->first;
            range.second = current_range->second;
        }

        hrtime_t start_time = gethrtime();
        vbucket_del_result result = ep->completeVBucketDeletion(vbucket,
                                                                vb_version,
                                                                range,
                                                                isLastChunk);
        hrtime_t chunk_time = (gethrtime() - start_time) / 1000;
        stats.diskVBChunkDelHisto.add(chunk_time);
        execution_time += chunk_time;

        switch(result) {
        case vbucket_del_success:
            if (!isLastChunk) {
                hrtime_t chunk_del_time = chunk_time / 1000; // chunk deletion exec time in msec
                if (range.first != -1 && range.second != -1 && chunk_del_time != 0) {
                    // Adjust the chunk's range size based on the chunk deletion execution time
                    chunk_del_range_size = (chunk_del_range_size * chunk_del_threshold_time)
                                           / chunk_del_time;
                    chunk_del_range_size = std::max(static_cast<int64_t>(100),
                                                    chunk_del_range_size);
                }

                ++current_range;
                // Split the current chunk into two chunks if its range size > the new range size
                if ((current_range->second - current_range->first) > chunk_del_range_size) {
                    range_list.splitChunkRange(current_range, chunk_del_range_size);
                } else {
                    // Merge the current chunk with its subsequent chunks before we reach the chunk
                    // that includes the end point of the new range size
                    range_list.mergeChunkRanges(current_range, chunk_del_range_size);
                }
                ++chunk_num;
                rv = true;
            } else { // Completion of a vbucket deletion
                stats.diskVBDelHisto.add(execution_time);
                hrtime_t wall_time = (gethrtime() - start_wall_time) / 1000;
                stats.vbucketDelMaxWalltime.setIfBigger(wall_time);
                stats.vbucketDelTotWalltime.incr(wall_time);
            }
            break;
        case vbucket_del_fail:
            d.snooze(t, 10);
            rv = true;
            getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                             "Reschedule to delete the chunk %d of vbucket %d from disk\n",
                             chunk_num, vbucket);
            break;
        case vbucket_del_invalid:
            break;
        }

        return rv;
    }

    std::string description() {
        std::stringstream ss;
        int64_t range_size = 0;
        if (current_range != range_list.end()) {
            range_size = current_range->second - current_range->first;
        }
        ss << "Removing the chunk " << chunk_num << "/" << range_list.size()
           << " of vbucket " << vbucket << " with the range size " << range_size
           << " from disk.";
        return ss.str();
    }
private:
    EventuallyPersistentStore    *ep;
    EPStats                      &stats;
    uint16_t                      vbucket;
    uint16_t                      vb_version;
    size_t                        chunk_size;
    size_t                        chunk_num;
    int64_t                       chunk_del_range_size;
    uint32_t                      chunk_del_threshold_time;
    VBucketDeletionVisitor        vbdv;
    hrtime_t                      execution_time;
    hrtime_t                      start_wall_time;
    VBDeletionChunkRangeList      range_list;
    chunk_range_iterator_t        current_range;
};


/**
 * Helper class used to insert items into the storage by using
 * the KVStore::dump method to load items from the database
 */
class LoadStorageKVPairCallback : public Callback<GetValue> {
public:
    LoadStorageKVPairCallback(VBucketMap &vb, EPStats &st,
                              EventuallyPersistentStore *ep)
        : vbuckets(vb), stats(st), epstore(ep), startTime(ep_real_time()),
          hasPurged(false) {
        assert(epstore);
    }

    void initVBucket(uint16_t vbid, uint16_t vb_version,
                     uint64_t checkpointId, vbucket_state_t prevState);

    void callback(GetValue &val);

private:

    bool shouldEject() {
        return StoredValue::getCurrentSize(stats) >= stats.mem_low_wat;
    }

    void purge();

    VBucketMap &vbuckets;
    EPStats    &stats;
    EventuallyPersistentStore *epstore;
    time_t      startTime;
    bool        hasPurged;
};


EventuallyPersistentStore::EventuallyPersistentStore(EventuallyPersistentEngine &theEngine,
                                                     KVStore *t,
                                                     bool startVb0,
                                                     bool concurrentDB) :
    engine(theEngine), stats(engine.getEpStats()), rwUnderlying(t),
    storageProperties(t->getStorageProperties()),
    vbuckets(theEngine.getConfiguration()),
    mutationLog(theEngine.getConfiguration().getKlogPath(),
                theEngine.getConfiguration().getKlogBlockSize()),
    diskFlushAll(false),
    tctx(stats, t, mutationLog, theEngine.observeRegistry),
    bgFetchDelay(0)
{
    getLogger()->log(EXTENSION_LOG_INFO, NULL,
                     "Storage props:  c=%d/r=%d/rw=%d\n",
                     storageProperties.maxConcurrency(),
                     storageProperties.maxReaders(),
                     storageProperties.maxWriters());

    doPersistence = getenv("EP_NO_PERSISTENCE") == NULL;
    dispatcher = new Dispatcher(theEngine, "RW_Dispatcher");
    if (storageProperties.maxConcurrency() > 1
        && storageProperties.maxReaders() > 1
        && concurrentDB) {
        roUnderlying = engine.newKVStore();
        roDispatcher = new Dispatcher(theEngine, "RO_Dispatcher");
        roDispatcher->start();
    } else {
        roUnderlying = rwUnderlying;
        roDispatcher = dispatcher;
    }
    nonIODispatcher = new Dispatcher(theEngine, "NONIO_Dispatcher");
    flusher = new Flusher(this, dispatcher);

    stats.memOverhead = sizeof(EventuallyPersistentStore);

    Configuration &config = engine.getConfiguration();

    setItemExpiryWindow(config.getExpiryWindow());
    config.addValueChangedListener("expiry_window",
                                   new EPStoreValueChangeListener(*this));

    setTxnSize(config.getMaxTxnSize());
    config.addValueChangedListener("max_txn_size",
                                   new EPStoreValueChangeListener(*this));

    stats.min_data_age.set(config.getMinDataAge());
    config.addValueChangedListener("min_data_age",
                                   new StatsValueChangeListener(stats));

    stats.queue_age_cap.set(config.getQueueAgeCap());
    config.addValueChangedListener("queue_age_cap",
                                   new StatsValueChangeListener(stats));

    stats.tapThrottleThreshold.set(static_cast<double>(config.getTapThrottleThreshold())
                                   / 100.0);
    config.addValueChangedListener("tap_throttle_threshold",
                                   new StatsValueChangeListener(stats));

    stats.tapThrottleWriteQueueCap.set(config.getTapThrottleQueueCap());
    config.addValueChangedListener("tap_throttle_queue_cap",
                                   new StatsValueChangeListener(stats));

    setBGFetchDelay(config.getBgFetchDelay());
    config.addValueChangedListener("bg_fetch_delay",
                                   new EPStoreValueChangeListener(*this));

    setVbDelChunkSize(config.getVbDelChunkSize());
    config.addValueChangedListener("vb_del_chunk_size",
                                   new EPStoreValueChangeListener(*this));
    setVbChunkDelThresholdTime(config.getVbChunkDelTime());
    config.addValueChangedListener("vb_chunk_del_time",
                                   new EPStoreValueChangeListener(*this));

    invalidItemDbPager = new InvalidItemDbPager(this, stats, vbDelChunkSize);

    config.addValueChangedListener("couch_vbucket_batch_count",
                                   new EPStoreValueChangeListener(*this));

    if (startVb0) {
        RCPtr<VBucket> vb(new VBucket(0, vbucket_state_active, stats,
                                      engine.getCheckpointConfig()));
        vbuckets.addBucket(vb);
        vbuckets.setBucketVersion(0, 0);
    }

    persistenceCheckpointIds = new uint64_t[BASE_VBUCKET_SIZE];
    for (size_t i = 0; i < BASE_VBUCKET_SIZE; ++i) {
        persistenceCheckpointIds[i] = 0;
    }

    size_t num_shards = rwUnderlying->getNumShards();
    dbShardQueues = new std::vector<queued_item>[num_shards];

    try {
        mutationLog.open();
        assert(theEngine.getConfiguration().getKlogPath() == ""
               || mutationLog.isEnabled());
    } catch(MutationLog::ReadException e) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Error opening mutation log:  %s (disabling)", e.what());
        mutationLog.disable();
    }

    bool syncset(mutationLog.setSyncConfig(theEngine.getConfiguration().getKlogSync()));
    assert(syncset);

    startDispatcher();
    startFlusher();
    startNonIODispatcher();
    assert(rwUnderlying);
    assert(roUnderlying);
}

class FlusherLeavingStateListener : public FlusherStateListener {
public:
    FlusherLeavingStateListener(Flusher &f, enum flusher_state st) :
        flusher(f), state(st) {}

    virtual void  stateChanged(const enum flusher_state &from,
                               const enum flusher_state &to) {
        (void)from;
        if (to != state) {
            LockHolder lh(syncobject);
            syncobject.notify();
        }
    }

    void wait() {
        LockHolder lh(syncobject);
        if (flusher.state() != state) {
            // We're not in that state anymore
            return;
        }
        syncobject.wait();
    }

private:
    Flusher &flusher;
    enum flusher_state state;
    SyncObject syncobject;
};

class FlusherEnterStateListener : public FlusherStateListener {
public:
    FlusherEnterStateListener(Flusher &f, enum flusher_state st) :
        flusher(f), state(st) {}

    virtual void  stateChanged(const enum flusher_state &from,
                               const enum flusher_state &to) {
        (void)from;
        if (to == state) {
            LockHolder lh(syncobject);
            syncobject.notify();
        }
    }

    void wait() {
        LockHolder lh(syncobject);
        if (flusher.state() == state) {
            // We're already that state
            return;
        }
        syncobject.wait();
    }

private:
    Flusher &flusher;
    enum flusher_state state;
    SyncObject syncobject;
};

void EventuallyPersistentStore::initialize() {
    // We should nuke everything unless we want warmup
    Configuration &config = engine.getConfiguration();
    if (!config.isWarmup()) {
        reset();
    }

    // We should at least wait until we've got the vbucket lists..
    FlusherLeavingStateListener flusherListener(*flusher, initializing);
    flusher->addFlusherStateListener(&flusherListener);
    flusherListener.wait();
    flusher->removeFlusherStateListener(&flusherListener);

    // We might want to wait until we've loaded all data...
    if (config.isWaitforwarmup()) {
        flusher_state state = running;

        if (getROUnderlying()->isKeyDumpSupported()) {
            state = loading_data;
        }

        FlusherEnterStateListener fl(*flusher, state);
        flusher->addFlusherStateListener(&fl);
        fl.wait();
        flusher->removeFlusherStateListener(&fl);
    }

    if (config.isFailpartialwarmup() && stats.warmOOM > 0) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Warmup failed to load %d records due to OOM, exiting.\n",
                         static_cast<unsigned int>(stats.warmOOM));
        exit(1);
    }

    // Run the vbucket state snapshot job once after the warmup
    scheduleVBSnapshot(Priority::VBucketPersistHighPriority);

    size_t expiryPagerSleeptime = config.getExpPagerStime();
    if (HashTable::getDefaultStorageValueType() != small) {
        shared_ptr<DispatcherCallback> cb(new ItemPager(this, stats));
        nonIODispatcher->schedule(cb, NULL, Priority::ItemPagerPriority, 10);

        setExpiryPagerSleeptime(expiryPagerSleeptime);
        config.addValueChangedListener("exp_pager_stime",
                                       new EPStoreValueChangeListener(*this));
    }

    shared_ptr<DispatcherCallback> htr(new HashtableResizer(this));
    nonIODispatcher->schedule(htr, NULL, Priority::HTResizePriority, 10);

    shared_ptr<DispatcherCallback> item_db_cb(getInvalidItemDbPager());
    dispatcher->schedule(item_db_cb, NULL,
                         Priority::InvalidItemDbPagerPriority, 0);


    size_t checkpointRemoverInterval = config.getChkRemoverStime();
    shared_ptr<DispatcherCallback> chk_cb(new ClosedUnrefCheckpointRemover(this,
                                                                           stats,
                                                                           checkpointRemoverInterval));
    nonIODispatcher->schedule(chk_cb, NULL,
                              Priority::CheckpointRemoverPriority,
                              checkpointRemoverInterval);

    shared_ptr<DispatcherCallback> obsRegCb(new ObserveRegistryCleaner(
                                            engine.getObserveRegistry(),
                                            stats, 60));

    nonIODispatcher->schedule(obsRegCb, NULL,
                              Priority::ObserveRegistryCleanerPriority,
                              10);

    shared_ptr<StatSnap> sscb(new StatSnap(&engine));
    dispatcher->schedule(sscb, NULL, Priority::StatSnapPriority,
                         STATSNAP_FREQ);

    if (config.getBackend().compare("sqlite") == 0 &&
        rwUnderlying->getStorageProperties().hasEfficientVBDeletion()) {
        shared_ptr<DispatcherCallback> invalidVBTableRemover(new InvalidVBTableRemover(&engine));
        dispatcher->schedule(invalidVBTableRemover, NULL,
                             Priority::VBucketDeletionPriority,
                             INVALID_VBTABLE_DEL_FREQ);
    }
}


/**
 * Hash table visitor used to collect dirty objects to verify storage.
 */
class VerifyStoredVisitor : public HashTableVisitor {
public:
    std::vector<std::string> dirty;
    void visit(StoredValue *v) {
        if (v->isDirty()) {
            dirty.push_back(v->getKey());
        }
    }
};

EventuallyPersistentStore::~EventuallyPersistentStore() {
    bool forceShutdown = engine.isForceShutdown();
    stopFlusher();
    dispatcher->stop(forceShutdown);
    if (hasSeparateRODispatcher()) {
        roDispatcher->stop(forceShutdown);
        delete roUnderlying;
    }
    nonIODispatcher->stop(forceShutdown);

    delete flusher;
    delete dispatcher;
    delete nonIODispatcher;
    delete []persistenceCheckpointIds;
    delete []dbShardQueues;
}

void EventuallyPersistentStore::startDispatcher() {
    dispatcher->start();
}

void EventuallyPersistentStore::startNonIODispatcher() {
    nonIODispatcher->start();
}

const Flusher* EventuallyPersistentStore::getFlusher() {
    return flusher;
}

void EventuallyPersistentStore::startFlusher() {
    flusher->start();
}

void EventuallyPersistentStore::stopFlusher() {
    bool rv = flusher->stop(engine.isForceShutdown());
    if (rv && !engine.isForceShutdown()) {
        flusher->wait();
    }
}

bool EventuallyPersistentStore::pauseFlusher() {
    tctx.commitSoon();
    flusher->pause();
    return true;
}

bool EventuallyPersistentStore::resumeFlusher() {
    flusher->resume();
    return true;
}

RCPtr<VBucket> EventuallyPersistentStore::getVBucket(uint16_t vbucket) {
    return vbuckets.getBucket(vbucket);
}

RCPtr<VBucket> EventuallyPersistentStore::getVBucket(uint16_t vbid,
                                                     vbucket_state_t wanted_state) {
    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    vbucket_state_t found_state(vb ? vb->getState() : vbucket_state_dead);
    if (found_state == wanted_state) {
        return vb;
    } else {
        RCPtr<VBucket> rv;
        return rv;
    }
}

void EventuallyPersistentStore::firePendingVBucketOps() {
    uint16_t i;
    for (i = 0; i < vbuckets.getSize(); i++) {
        RCPtr<VBucket> vb = getVBucket(i, vbucket_state_active);
        if (vb) {
            vb->fireAllOps(engine);
        }
    }
}

/// @cond DETAILS
/**
 * Inner loop of deleteExpiredItems.
 */
class Deleter {
public:
    Deleter(EventuallyPersistentStore *ep) : e(ep), startTime(ep_real_time()) {}
    void operator() (std::pair<uint16_t, std::string> vk) {
        RCPtr<VBucket> vb = e->getVBucket(vk.first);
        if (vb) {
            int bucket_num(0);
            LockHolder lh = vb->ht.getLockedBucket(vk.second, &bucket_num);
            StoredValue *v = vb->ht.unlocked_find(vk.second, bucket_num);
            if (v && v->isExpired(startTime)) {
                vb->ht.unlocked_softDelete(v, 0);
                e->queueDirty(vk.second, vb->getId(), queue_op_del,
                              v->getSeqno(), v->getId(), false);
            }
        }
    }
private:
    EventuallyPersistentStore *e;
    time_t                     startTime;
};
/// @endcond

void
EventuallyPersistentStore::deleteExpiredItems(std::list<std::pair<uint16_t, std::string> > &keys) {
    // This can be made a lot more efficient, but I'd rather see it
    // show up in a profiling report first.
    std::for_each(keys.begin(), keys.end(), Deleter(this));
}

StoredValue *EventuallyPersistentStore::fetchValidValue(RCPtr<VBucket> vb,
                                                        const std::string &key,
                                                        int bucket_num,
                                                        bool wantDeleted) {
    StoredValue *v = vb->ht.unlocked_find(key, bucket_num, wantDeleted);
    if (v && !v->isDeleted()) { // In the deleted case, we ignore expiration time.
        if (v->isExpired(ep_real_time())) {
            ++stats.expired;
            vb->ht.unlocked_softDelete(v, 0);
            queueDirty(key, vb->getId(), queue_op_del, v->getSeqno(), v->getId());
            return NULL;
        }
        v->touch();
    }
    return v;
}

protocol_binary_response_status EventuallyPersistentStore::evictKey(const std::string &key,
                                                                    uint16_t vbucket,
                                                                    const char **msg,
                                                                    size_t *msg_size,
                                                                    bool force) {
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb || (vb->getState() != vbucket_state_active && !force)) {
        return PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num, force);

    protocol_binary_response_status rv(PROTOCOL_BINARY_RESPONSE_SUCCESS);

    *msg_size = 0;
    if (v) {
        if (force)  {
            v->markClean(NULL);
        }
        if (v->isResident()) {
            if (v->ejectValue(stats, vb->ht)) {
                *msg = "Ejected.";
            } else {
                *msg = "Can't eject: Dirty or a small object.";
            }
        } else {
            *msg = "Already ejected.";
        }
    } else {
        *msg = "Not found.";
        rv = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
    }

    return rv;
}

ENGINE_ERROR_CODE EventuallyPersistentStore::set(const Item &itm,
                                                 const void *cookie,
                                                 bool force) {

    RCPtr<VBucket> vb = getVBucket(itm.getVBucketId());
    if (!vb || vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_active) {
        if (vb->checkpointManager.isHotReload()) {
            if (vb->addPendingOp(cookie)) {
                return ENGINE_EWOULDBLOCK;
            }
        }
    } else if (vb->getState() == vbucket_state_replica && !force) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_pending && !force) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    }

    bool cas_op = (itm.getCas() != 0);

    int64_t row_id = -1;
    mutation_type_t mtype = vb->ht.set(itm, row_id);
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    switch (mtype) {
    case NOMEM:
        ret = ENGINE_ENOMEM;
        break;
    case INVALID_CAS:
    case IS_LOCKED:
        ret = ENGINE_KEY_EEXISTS;
        break;
    case NOT_FOUND:
        if (cas_op) {
            ret = ENGINE_KEY_ENOENT;
            break;
        }
        // FALLTHROUGH
    case WAS_DIRTY:
        // Even if the item was dirty, push it into the vbucket's open checkpoint.
    case WAS_CLEAN:
        queueDirty(itm.getKey(), itm.getVBucketId(), queue_op_set,
                   itm.getSeqno(), row_id);
        break;
    case INVALID_VBUCKET:
        ret = ENGINE_NOT_MY_VBUCKET;
        break;
    }

    return ret;
}

ENGINE_ERROR_CODE EventuallyPersistentStore::add(const Item &itm,
                                                 const void *cookie)
{
    RCPtr<VBucket> vb = getVBucket(itm.getVBucketId());
    if (!vb || vb->getState() == vbucket_state_dead || vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_active) {
        if (vb->checkpointManager.isHotReload()) {
            if (vb->addPendingOp(cookie)) {
                return ENGINE_EWOULDBLOCK;
            }
        }
    } else if(vb->getState() == vbucket_state_pending) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    }

    if (itm.getCas() != 0) {
        // Adding with a cas value doesn't make sense..
        return ENGINE_NOT_STORED;
    }

    switch (vb->ht.add(itm)) {
    case ADD_NOMEM:
        return ENGINE_ENOMEM;
    case ADD_EXISTS:
        return ENGINE_NOT_STORED;
    case ADD_SUCCESS:
    case ADD_UNDEL:
        queueDirty(itm.getKey(), itm.getVBucketId(), queue_op_set,
                   itm.getSeqno(), -1);
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentStore::addTAPBackfillItem(const Item &itm, bool meta) {

    RCPtr<VBucket> vb = getVBucket(itm.getVBucketId());
    if (!vb ||
        vb->getState() == vbucket_state_dead ||
        (vb->getState() == vbucket_state_active &&
         !engine.getCheckpointConfig().isInconsistentSlaveCheckpoint())) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    }

    int64_t row_id = -1;
    mutation_type_t mtype;

    if (meta) {
        mtype = vb->ht.set(itm, 0, row_id, true);
    } else {
        mtype = vb->ht.set(itm, row_id);
    }
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    switch (mtype) {
    case NOMEM:
        ret = ENGINE_ENOMEM;
        break;
    case INVALID_CAS:
    case IS_LOCKED:
        ret = ENGINE_KEY_EEXISTS;
        break;
    case WAS_DIRTY:
        // If a given backfill item is already dirty, don't queue the same item again.
        break;
    case NOT_FOUND:
        // FALLTHROUGH
    case WAS_CLEAN:
        queueDirty(itm.getKey(), itm.getVBucketId(), queue_op_set,
                   itm.getSeqno(), row_id, true);
        break;
    case INVALID_VBUCKET:
        ret = ENGINE_NOT_MY_VBUCKET;
        break;
    }

    return ret;
}


void EventuallyPersistentStore::snapshotVBuckets(const Priority &priority) {

    class VBucketStateVisitor : public VBucketVisitor {
    public:
        VBucketStateVisitor(VBucketMap &vb_map) : vbuckets(vb_map) { }
        bool visitBucket(RCPtr<VBucket> &vb) {
            std::pair<uint16_t, uint16_t> p(vb->getId(),
                                            vbuckets.getBucketVersion(vb->getId()));
            vbucket_state vb_state;
            vb_state.state = vb->getState();
            vb_state.checkpointId = vbuckets.getPersistenceCheckpointId(vb->getId());
            states[p] = vb_state;
            return false;
        }

        void visit(StoredValue*) {
            assert(false); // this does not happen
        }

        std::map<std::pair<uint16_t, uint16_t>, vbucket_state> states;

    private:
        VBucketMap &vbuckets;
    };

    if (priority == Priority::VBucketPersistHighPriority) {
        vbuckets.setHighPriorityVbSnapshotFlag(false);
    } else {
        vbuckets.setLowPriorityVbSnapshotFlag(false);
    }

    VBucketStateVisitor v(vbuckets);
    visit(v);
    if (!rwUnderlying->snapshotVBuckets(v.states)) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "Rescheduling a task to snapshot vbuckets\n");
        scheduleVBSnapshot(priority);
    }
}

void EventuallyPersistentStore::setVBucketState(uint16_t vbid,
                                                vbucket_state_t to) {
    // Lock to prevent a race condition between a failed update and add.
    LockHolder lh(vbsetMutex);
    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    if (vb) {
        vb->setState(to, engine.getServerApi());
        lh.unlock();
        if (vb->getState() == vbucket_state_pending && to == vbucket_state_active) {
            engine.notifyNotificationThread();
        }
        scheduleVBSnapshot(Priority::VBucketPersistLowPriority);
    } else {
        RCPtr<VBucket> newvb(new VBucket(vbid, to, stats, engine.getCheckpointConfig()));
        if (to != vbucket_state_active) {
            newvb->checkpointManager.setOpenCheckpointId(0);
        }
        uint16_t vb_version = vbuckets.getBucketVersion(vbid);
        uint16_t vb_new_version = vb_version == (std::numeric_limits<uint16_t>::max() - 1) ?
                                  0 : vb_version + 1;
        vbuckets.addBucket(newvb);
        vbuckets.setBucketVersion(vbid, vb_new_version);
        lh.unlock();
        scheduleVBSnapshot(Priority::VBucketPersistHighPriority);
    }
}

void EventuallyPersistentStore::scheduleVBSnapshot(const Priority &p) {
    if (p == Priority::VBucketPersistHighPriority) {
        if (!vbuckets.setHighPriorityVbSnapshotFlag(true)) {
            return;
        }
    } else {
        if (!vbuckets.setLowPriorityVbSnapshotFlag(true)) {
            return;
        }
    }
    dispatcher->schedule(shared_ptr<DispatcherCallback>(new SnapshotVBucketsCallback(this, p)),
                         NULL, p, 0, false);
}

vbucket_del_result
EventuallyPersistentStore::completeVBucketDeletion(uint16_t vbid, uint16_t vb_version,
                                                   std::pair<int64_t, int64_t> row_range,
                                                   bool isLastChunk) {
    LockHolder lh(vbsetMutex);

    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    if (!vb || vb->getState() == vbucket_state_dead || vbuckets.isBucketDeletion(vbid)) {
        lh.unlock();
        if (row_range.first < 0 || row_range.second < 0 ||
            rwUnderlying->delVBucket(vbid, vb_version, row_range)) {
            if (isLastChunk) {
                vbuckets.setBucketDeletion(vbid, false);
                ++stats.vbucketDeletions;
            }
            return vbucket_del_success;
        } else {
            ++stats.vbucketDeletionFail;
            return vbucket_del_fail;
        }
    }
    return vbucket_del_invalid;
}

vbucket_del_result
EventuallyPersistentStore::completeVBucketDeletion(uint16_t vbid, uint16_t vbver) {
    LockHolder lh(vbsetMutex);

    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    if (!vb || vb->getState() == vbucket_state_dead || vbuckets.isBucketDeletion(vbid)) {
        lh.unlock();
        if (rwUnderlying->delVBucket(vbid, vbver)) {
            vbuckets.setBucketDeletion(vbid, false);
            mutationLog.deleteAll(vbid);
            // This is happening in an independent transaction, so
            // we're going go ahead and commit it out.
            mutationLog.commit1();
            mutationLog.commit2();
            ++stats.vbucketDeletions;
            return vbucket_del_success;
        } else {
            ++stats.vbucketDeletionFail;
            return vbucket_del_fail;
        }
    }
    return vbucket_del_invalid;
}

void EventuallyPersistentStore::scheduleVBDeletion(RCPtr<VBucket> vb, uint16_t vb_version,
                                                   double delay=0) {
    if (vbuckets.setBucketDeletion(vb->getId(), true)) {
        if (storageProperties.hasEfficientVBDeletion()) {
            shared_ptr<DispatcherCallback> cb(new FastVBucketDeletionCallback(this, vb,
                                                                              vb_version,
                                                                              stats));
            dispatcher->schedule(cb,
                                 NULL, Priority::FastVBucketDeletionPriority,
                                 delay, false);
        } else {
            size_t chunk_size = vbDelChunkSize;
            uint32_t vb_chunk_del_time = vbChunkDelThresholdTime;
            shared_ptr<DispatcherCallback> cb(new VBucketDeletionCallback(this, vb, vb_version,
                                                                          stats, chunk_size,
                                                                          vb_chunk_del_time));
            dispatcher->schedule(cb,
                                 NULL, Priority::VBucketDeletionPriority,
                                 delay, false);
        }
    }
}

bool EventuallyPersistentStore::deleteVBucket(uint16_t vbid) {
    // Lock to prevent a race condition between a failed update and add (and delete).
    LockHolder lh(vbsetMutex);
    bool rv(false);

    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    if (vb && vb->getState() == vbucket_state_dead) {
        uint16_t vb_version = vbuckets.getBucketVersion(vbid);
        lh.unlock();
        rv = true;
        HashTableStatVisitor statvis = vb->ht.clear();
        stats.currentSize.decr(statvis.memSize - statvis.valSize);
        assert(stats.currentSize.get() < GIGANTOR);
        vbuckets.removeBucket(vbid);
        scheduleVBSnapshot(Priority::VBucketPersistHighPriority);
        scheduleVBDeletion(vb, vb_version);
    }
    return rv;
}

bool EventuallyPersistentStore::resetVBucket(uint16_t vbid) {
    LockHolder lh(vbsetMutex);
    bool rv(false);

    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    if (vb) {
        if (vb->ht.getNumItems() == 0) { // Already reset?
            return true;
        }
        uint16_t vb_version = vbuckets.getBucketVersion(vbid);
        uint16_t vb_new_version = vb_version == (std::numeric_limits<uint16_t>::max() - 1) ?
                                  0 : vb_version + 1;
        if (engine.getConfiguration().getBackend().compare("couchdb") == 0) {
            // TROND: We don't use vbucket versions for couch..
            vb_new_version = vb_version;
        }

        vbuckets.setBucketVersion(vbid, vb_new_version);
        vbuckets.setPersistenceCheckpointId(vbid, 0);
        lh.unlock();

        // Clear the hashtable, checkpoints, and stats for the target vbucket.
        HashTableStatVisitor statvis = vb->ht.clear();
        stats.currentSize.decr(statvis.memSize - statvis.valSize);
        assert(stats.currentSize.get() < GIGANTOR);
        vb->checkpointManager.clear(vb->getState());
        vb->resetStats();

        scheduleVBSnapshot(Priority::VBucketPersistHighPriority);
        // Clear all the items from the vbucket kv table on disk.
        scheduleVBDeletion(vb, vb_version);
        rv = true;
    }
    return rv;
}

void EventuallyPersistentStore::completeBGFetch(const std::string &key,
                                                uint16_t vbucket,
                                                uint16_t vbver,
                                                uint64_t rowid,
                                                const void *cookie,
                                                hrtime_t init) {
    hrtime_t start(gethrtime());
    ++stats.bg_fetched;
    std::stringstream ss;
    ss << "Completed a background fetch, now at " << bgFetchQueue.get()
       << std::endl;
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL, ss.str().c_str());

    // Go find the data
    RememberingCallback<GetValue> gcb;

    roUnderlying->get(key, rowid, vbucket, vbver, gcb);
    gcb.waitForValue();
    assert(gcb.fired);

    // Lock to prevent a race condition between a fetch for restore and delete
    LockHolder lh(vbsetMutex);

    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (vb && vb->getState() == vbucket_state_active && gcb.val.getStatus() == ENGINE_SUCCESS) {
        int bucket_num(0);
        LockHolder hlh = vb->ht.getLockedBucket(key, &bucket_num);
        StoredValue *v = fetchValidValue(vb, key, bucket_num);

        if (v && !v->isResident()) {
            assert(gcb.val.getStatus() == ENGINE_SUCCESS);
            v->restoreValue(gcb.val.getValue()->getValue(), stats, vb->ht);
            assert(v->isResident());
        }
    }

    lh.unlock();

    hrtime_t stop = gethrtime();

    if (stop > start && start > init) {
        // skip the measurement if the counter wrapped...
        ++stats.bgNumOperations;
        hrtime_t w = (start - init) / 1000;
        BlockTimer::log(start - init, "bgwait", stats.timingLog);
        stats.bgWaitHisto.add(w);
        stats.bgWait += w;
        stats.bgMinWait.setIfLess(w);
        stats.bgMaxWait.setIfBigger(w);

        hrtime_t l = (stop - start) / 1000;
        BlockTimer::log(stop - start, "bgload", stats.timingLog);
        stats.bgLoadHisto.add(l);
        stats.bgLoad += l;
        stats.bgMinLoad.setIfLess(l);
        stats.bgMaxLoad.setIfBigger(l);
    }

    engine.notifyIOComplete(cookie, gcb.val.getStatus());
    delete gcb.val.getValue();
}

void EventuallyPersistentStore::bgFetch(const std::string &key,
                                        uint16_t vbucket,
                                        uint16_t vbver,
                                        uint64_t rowid,
                                        const void *cookie) {
    shared_ptr<BGFetchCallback> dcb(new BGFetchCallback(this, key,
                                                        vbucket, vbver,
                                                        rowid, cookie));
    assert(bgFetchQueue > 0);
    std::stringstream ss;
    ss << "Queued a background fetch, now at " << bgFetchQueue.get()
       << std::endl;
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL, ss.str().c_str());
    roDispatcher->schedule(dcb, NULL, Priority::BgFetcherPriority, bgFetchDelay);
}

GetValue EventuallyPersistentStore::getInternal(const std::string &key,
                                        uint16_t vbucket,
                                        const void *cookie,
                                        bool queueBG,
                                        bool honorStates,
                                        vbucket_state_t allowedState) {
    vbucket_state_t disallowedState = (allowedState == vbucket_state_active) ?
        vbucket_state_replica : vbucket_state_active;
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if (honorStates && vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if (vb->getState() == allowedState) {
        if (vb->checkpointManager.isHotReload()) {
            if (vb->addPendingOp(cookie)) {
                return GetValue(NULL, ENGINE_EWOULDBLOCK);
            }
        }
    } else if (honorStates && vb->getState() == disallowedState) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if (honorStates && vb->getState() == vbucket_state_pending) {
        if (vb->addPendingOp(cookie)) {
            return GetValue(NULL, ENGINE_EWOULDBLOCK);
        }
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num);

    if (v) {
        // If the value is not resident, wait for it...
        if (!v->isResident()) {
            if (queueBG) {
                bgFetch(key, vbucket, vbuckets.getBucketVersion(vbucket),
                        v->getId(), cookie);
            }
            return GetValue(NULL, ENGINE_EWOULDBLOCK, v->getId(), -1, v);
        }

        GetValue rv(v->toItem(v->isLocked(ep_current_time()), vbucket),
                    ENGINE_SUCCESS, v->getId(), -1, v);
        return rv;
    } else {
        GetValue rv;
        if (engine.isDegradedMode()) {
            rv.setStatus(ENGINE_TMPFAIL);
        }
        return rv;
    }
}

ENGINE_ERROR_CODE EventuallyPersistentStore::getMetaData(const std::string &key,
                                                         uint16_t vbucket,
                                                         const void *cookie,
                                                         std::string &meta,
                                                         uint64_t &cas,
                                                         uint32_t &flags)
{
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb || vb->getState() == vbucket_state_dead ||
        vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_active ||
               vb->getState() == vbucket_state_pending) {
        if (vb->checkpointManager.isHotReload()) {
            if (vb->addPendingOp(cookie)) {
                return ENGINE_EWOULDBLOCK;
            }
        }
    }

    int bucket_num(0);
    flags = 0;
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num, true);

    if (v) {
        if (v->isDeleted()) {
            flags |= ntohl(GET_META_ITEM_DELETED_FLAG);
        }
        cas = v->getCas();
        Item::encodeMeta(v->getSeqno(), cas, v->valLength(),
                         v->getFlags(), meta);
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_KEY_ENOENT;
    }
}

ENGINE_ERROR_CODE EventuallyPersistentStore::setWithMeta(const Item &itm,
                                                         uint64_t cas,
                                                         const void *cookie,
                                                         bool force,
                                                         bool allowExisting)
{
    RCPtr<VBucket> vb = getVBucket(itm.getVBucketId());
    if (!vb || vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_active) {
        if (vb->checkpointManager.isHotReload()) {
            if (vb->addPendingOp(cookie)) {
                return ENGINE_EWOULDBLOCK;
            }
        }
    } else if (vb->getState() == vbucket_state_replica && !force) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_pending && !force) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    }

    int64_t row_id = -1;
    mutation_type_t mtype = vb->ht.set(itm, cas, row_id, allowExisting);
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    switch (mtype) {
    case NOMEM:
        ret = ENGINE_ENOMEM;
        break;
    case INVALID_CAS:
    case IS_LOCKED:
        ret = ENGINE_KEY_EEXISTS;
        break;
    case INVALID_VBUCKET:
        ret = ENGINE_NOT_MY_VBUCKET;
        break;
    case WAS_DIRTY:
    case WAS_CLEAN:
    case NOT_FOUND:
        queueDirty(itm.getKey(), itm.getVBucketId(), queue_op_set,
                   itm.getSeqno(), row_id);
        break;
    default:
        abort();
    }

    return ret;
}

GetValue EventuallyPersistentStore::getAndUpdateTtl(const std::string &key,
                                                    uint16_t vbucket,
                                                    const void *cookie,
                                                    bool queueBG,
                                                    uint32_t exptime)
{
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if (vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if (vb->getState() == vbucket_state_active) {
        if (vb->checkpointManager.isHotReload()) {
            if (vb->addPendingOp(cookie)) {
                return GetValue(NULL, ENGINE_EWOULDBLOCK);
            }
        }
    } else if (vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if (vb->getState() == vbucket_state_pending) {
        if (vb->addPendingOp(cookie)) {
            return GetValue(NULL, ENGINE_EWOULDBLOCK);
        }
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num);

    if (v) {
        v->setExptime(exptime);
        // If the value is not resident, wait for it...
        if (!v->isResident()) {
            if (queueBG) {
                bgFetch(key, vbucket, vbuckets.getBucketVersion(vbucket),
                        v->getId(), cookie);
                return GetValue(NULL, ENGINE_EWOULDBLOCK, v->getId());
            } else {
                // You didn't want the item anyway...
                return GetValue(NULL, ENGINE_SUCCESS, v->getId());
            }
        }

        GetValue rv(v->toItem(v->isLocked(ep_current_time()), vbucket),
                    ENGINE_SUCCESS, v->getId());
        return rv;
    } else {
        GetValue rv;
        if (engine.isDegradedMode()) {
            rv.setStatus(ENGINE_TMPFAIL);
        }
        return rv;
    }
}

ENGINE_ERROR_CODE
EventuallyPersistentStore::getFromUnderlying(const std::string &key,
                                             uint16_t vbucket,
                                             const void *cookie,
                                             shared_ptr<Callback<GetValue> > cb) {
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb || vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_active) {
        if (vb->checkpointManager.isHotReload()) {
            if (vb->addPendingOp(cookie)) {
                return ENGINE_EWOULDBLOCK;
            }
        }
    } else if (vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_pending) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num);

    if (v) {
        uint16_t vbver = vbuckets.getBucketVersion(vbucket);
        shared_ptr<VKeyStatBGFetchCallback> dcb(new VKeyStatBGFetchCallback(this, key,
                                                                            vbucket,
                                                                            vbver,
                                                                            v->getId(),
                                                                            cookie, cb));
        assert(bgFetchQueue > 0);
        roDispatcher->schedule(dcb, NULL, Priority::VKeyStatBgFetcherPriority, bgFetchDelay);
        return ENGINE_EWOULDBLOCK;
    } else if (engine.isDegradedMode()) {
        return ENGINE_TMPFAIL;
    } else {
        return ENGINE_KEY_ENOENT;
    }
}

bool EventuallyPersistentStore::getLocked(const std::string &key,
                                          uint16_t vbucket,
                                          Callback<GetValue> &cb,
                                          rel_time_t currentTime,
                                          uint32_t lockTimeout,
                                          const void *cookie) {
    RCPtr<VBucket> vb = getVBucket(vbucket, vbucket_state_active);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        GetValue rv(NULL, ENGINE_NOT_MY_VBUCKET);
        cb.callback(rv);
        return false;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num);

    if (v) {

        // if v is locked return error
        if (v->isLocked(currentTime)) {
            GetValue rv;
            cb.callback(rv);
            return false;
        }

        // If the value is not resident, wait for it...
        if (!v->isResident()) {

            if (cookie) {
                bgFetch(key, vbucket, vbuckets.getBucketVersion(vbucket),
                        v->getId(), cookie);
            }
            GetValue rv(NULL, ENGINE_EWOULDBLOCK, v->getId());
            cb.callback(rv);
            return false;
        }

        // acquire lock and increment cas value
        v->lock(currentTime + lockTimeout);

        Item *it = v->toItem(false, vbucket);
        it->setCas();
        v->setCas(it->getCas());

        GetValue rv(it);
        cb.callback(rv);

    } else {
        GetValue rv;
        if (engine.isDegradedMode()) {
            rv.setStatus(ENGINE_TMPFAIL);
        }
        cb.callback(rv);
    }
    return true;
}

StoredValue* EventuallyPersistentStore::getStoredValue(const std::string &key,
                                                       uint16_t vbucket,
                                                       bool honorStates) {
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return NULL;
    } else if (honorStates && vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return NULL;
    } else if (vb->getState() == vbucket_state_active) {
        // OK
    } else if(honorStates && vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return NULL;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    return fetchValidValue(vb, key, bucket_num);
}

ENGINE_ERROR_CODE
EventuallyPersistentStore::unlockKey(const std::string &key,
                                     uint16_t vbucket,
                                     uint64_t cas,
                                     rel_time_t currentTime)
{

    RCPtr<VBucket> vb = getVBucket(vbucket, vbucket_state_active);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num);

    if (v) {
        if (v->isLocked(currentTime)) {
            if (v->getCas() == cas) {
                v->unlock();
                return ENGINE_SUCCESS;
            }
        }
        return ENGINE_TMPFAIL;
    }

    if (engine.isDegradedMode()) {
        return ENGINE_TMPFAIL;
    }

    return ENGINE_KEY_ENOENT;
}


bool EventuallyPersistentStore::getKeyStats(const std::string &key,
                                            uint16_t vbucket,
                                            struct key_stats &kstats)
{
    RCPtr<VBucket> vb = getVBucket(vbucket, vbucket_state_active);
    if (!vb) {
        return false;
    }

    bool found = false;
    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num);

    found = (v != NULL);
    if (found) {
        kstats.dirty = v->isDirty();
        kstats.exptime = v->getExptime();
        kstats.flags = v->getFlags();
        kstats.cas = v->getCas();
        // TODO:  Know this somehow.
        kstats.dirtied = 0; // v->getDirtied();
        kstats.data_age = v->getDataAge();
        kstats.last_modification_time = ep_abs_time(v->getDataAge());
    }
    return found;
}

ENGINE_ERROR_CODE EventuallyPersistentStore::deleteItem(const std::string &key,
                                                        uint32_t seqno,
                                                        uint64_t cas,
                                                        uint16_t vbucket,
                                                        const void *cookie,
                                                        bool force,
                                                        bool use_meta) {
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb || vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_active) {
        if (vb->checkpointManager.isHotReload()) {
            if (vb->addPendingOp(cookie)) {
                return ENGINE_EWOULDBLOCK;
            }
        }
    } else if(vb->getState() == vbucket_state_replica && !force) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if(vb->getState() == vbucket_state_pending && !force) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = vb->ht.unlocked_find(key, bucket_num);
    if (!v) {
        if (engine.isDegradedMode()) {
            LockHolder rlh(restore.mutex);
            restore.itemsDeleted.insert(key);
        } else {
            return ENGINE_KEY_ENOENT;
        }
    }

    mutation_type_t delrv;
    if (use_meta) {
        delrv = vb->ht.unlocked_softDelete(v, cas, seqno);
    } else {
        delrv = vb->ht.unlocked_softDelete(v, cas);
    }

    ENGINE_ERROR_CODE rv;
    bool expired = false;
    if (delrv == NOT_FOUND || delrv == INVALID_CAS) {
        if (v && v->isExpired(ep_real_time())) {
            expired = true;
        }
        rv = (delrv == INVALID_CAS) ? ENGINE_KEY_EEXISTS : ENGINE_KEY_ENOENT;
    } else if (delrv == IS_LOCKED) {
        rv = ENGINE_TMPFAIL;
    } else { // WAS_CLEAN or WAS_DIRTY
        rv = ENGINE_SUCCESS;
    }

    if (delrv == WAS_CLEAN ||
        delrv == WAS_DIRTY ||
        (delrv == NOT_FOUND && (expired || engine.isDegradedMode()))) {
        // As replication is interleaved with online restore, deletion of items that might
        // exist in the restore backup files should be queued and replicated.
        uint32_t seqnum = v ? v->getSeqno() : 0;
        int64_t rowid = v ? v->getId() : -1;
        lh.unlock();
        queueDirty(key, vbucket, queue_op_del, seqnum, rowid);
    }
    return rv;
}

void EventuallyPersistentStore::reset() {
    std::vector<int> buckets = vbuckets.getBuckets();
    std::vector<int>::iterator it;
    for (it = buckets.begin(); it != buckets.end(); ++it) {
        RCPtr<VBucket> vb = getVBucket(*it);
        if (vb) {
            HashTableStatVisitor statvis = vb->ht.clear();
            stats.currentSize.decr(statvis.memSize - statvis.valSize);
            assert(stats.currentSize.get() < GIGANTOR);
            vb->checkpointManager.clear(vb->getState());
            vb->resetStats();
        }
    }
    if (diskFlushAll.cas(false, true)) {
        // Increase the write queue size by 1 as flusher will execute flush_all as a single task.
        stats.queue_size.set(getWriteQueueSize() + 1);
    }
}

void EventuallyPersistentStore::enqueueCommit() {
    queued_item qi(new QueuedItem("", 0, queue_op_commit));
    writing.push(qi);
    stats.memOverhead.incr(sizeof(queued_item));
    assert(stats.memOverhead.get() < GIGANTOR);
    ++stats.totalEnqueued;
}

bool EventuallyPersistentStore::isVbCachedStateStale(uint16_t vb, vbucket_state_t st) {
    std::map<uint16_t, vbucket_state_t>::iterator iter;
    iter = flusherCachedVbStates.find(vb);

    if (iter == flusherCachedVbStates.end() || iter->second != st) {
        flusherCachedVbStates[vb] = st;
        return true;
    }

    return false;
}

std::queue<queued_item>* EventuallyPersistentStore::beginFlush() {
    std::queue<queued_item> *rv(NULL);

    if (!hasItemsForPersistence() && writing.empty() && !diskFlushAll) {
        stats.dirtyAge = 0;
        // If the persistence queue is empty, reset queue-related stats for each vbucket.
        size_t numOfVBuckets = vbuckets.getSize();
        for (size_t i = 0; i < numOfVBuckets; ++i) {
            assert(i <= std::numeric_limits<uint16_t>::max());
            uint16_t vbid = static_cast<uint16_t>(i);
            RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
            if (vb) {
                vb->dirtyQueueSize.set(0);
                vb->dirtyQueueMem.set(0);
                vb->dirtyQueueAge.set(0);
                vb->dirtyQueuePendingWrites.set(0);
            }
        }
    } else {
        assert(rwUnderlying);
        if (diskFlushAll) {
            queued_item qi(new QueuedItem("", 0xffff, queue_op_flush));
            writing.push(qi);
            stats.memOverhead.incr(sizeof(queued_item));
            assert(stats.memOverhead.get() < GIGANTOR);
        }

        std::vector<queued_item> item_list;
        std::set<queued_item, CompareQueuedItemsByKey> item_set;
        size_t dedup = 0;
        size_t num_items = 0;
        size_t numOfVBuckets = vbuckets.getSize();

        item_list.reserve(getTxnSize());
        assert(numOfVBuckets <= std::numeric_limits<uint16_t>::max());

        for (size_t i = 0; i < numOfVBuckets; ++i) {
            uint16_t vbid = static_cast<uint16_t>(i);
            RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
            if (!vb) {
                // Undefined vbucket..
                continue;
            }

            vbucket_state_t st = vb->getState();
            if (isVbCachedStateStale(vbid, st)) {
                rwUnderlying->vbStateChanged(vbid, st);
            }

            // Grab all the items from online restore.
            LockHolder rlh(restore.mutex);
            std::map<uint16_t, std::vector<queued_item> >::iterator rit = restore.items.find(vbid);
            if (rit != restore.items.end()) {
                item_list.insert(item_list.end(), rit->second.begin(), rit->second.end());
                rit->second.clear();
            }
            rlh.unlock();

            // Grab all the backfill items if exist.
            vb->getBackfillItems(item_list);

            // Get all dirty items from the checkpoint.
            uint64_t checkpointId = vb->checkpointManager.getAllItemsForPersistence(item_list);
            persistenceCheckpointIds[vbid] = checkpointId;

            std::vector<queued_item>::reverse_iterator reverse_it = item_list.rbegin();
            // Perform further deduplication here by removing duplicate mutations for each key.
            for (; reverse_it != item_list.rend(); ++reverse_it) {
                queued_item qi = *reverse_it;
                switch (qi->getOperation()) {
                case queue_op_set:
                case queue_op_del:
                    if (!(item_set.insert(qi).second)) {
                        ++dedup;
                        vb->doStatsForFlushing(*qi, qi->size());
                    }
                default:
                    // Ignore
                    ;
                }
            }

            uint16_t shard_id = 0;
            std::set<queued_item, CompareQueuedItemsByKey>::iterator sit = item_set.begin();
            for (; sit != item_set.end(); ++sit) {
                const queued_item &qitem = *sit;
                shard_id = rwUnderlying->getShardId(*qitem);
                dbShardQueues[shard_id].push_back(*sit);
            }
            num_items += item_set.size();
            item_list.clear();
            item_set.clear();
        }

        if (num_items > 0) {
            pushToOutgoingQueue();
        }
        size_t queue_size = getWriteQueueSize();
        stats.flusherDedup += dedup;
        stats.flusher_todo.set(writing.size());
        stats.queue_size.set(queue_size);
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "Flushing %d items with %d still in queue\n",
                         writing.size(), queue_size);
        rv = &writing;
    }
    return rv;
}

void EventuallyPersistentStore::pushToOutgoingQueue() {
    size_t num_items = 0;
    size_t num_shards = rwUnderlying->getNumShards();
    for (size_t i = 0; i < num_shards; ++i) {
        if (dbShardQueues[i].empty()) {
            continue;
        }
        rwUnderlying->optimizeWrites(dbShardQueues[i]);
        std::vector<queued_item>::iterator it = dbShardQueues[i].begin();
        for(; it != dbShardQueues[i].end(); ++it) {
            writing.push(*it);
        }
        num_items += dbShardQueues[i].size();
        dbShardQueues[i].clear();
    }
    stats.memOverhead.incr(num_items * sizeof(queued_item));
    assert(stats.memOverhead.get() < GIGANTOR);
}

void EventuallyPersistentStore::requeueRejectedItems(std::queue<queued_item> *rej) {
    size_t queue_size = rej->size();
    // Requeue the rejects.
    while (!rej->empty()) {
        writing.push(rej->front());
        rej->pop();
    }
    stats.memOverhead.incr(queue_size * sizeof(queued_item));
    assert(stats.memOverhead.get() < GIGANTOR);
    stats.queue_size.set(getWriteQueueSize());
    stats.flusher_todo.set(writing.size());
}

void EventuallyPersistentStore::completeFlush(rel_time_t flush_start) {
    LockHolder lh(vbsetMutex);
    size_t numOfVBuckets = vbuckets.getSize();
    bool schedule_vb_snapshot = false;
    for (size_t i = 0; i < numOfVBuckets; ++i) {
        assert(i <= std::numeric_limits<uint16_t>::max());
        uint16_t vbid = static_cast<uint16_t>(i);
        RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
        if (!vb || vb->getState() == vbucket_state_dead) {
            continue;
        }
        if (persistenceCheckpointIds[vbid] > 0 &&
            persistenceCheckpointIds[vbid] != vbuckets.getPersistenceCheckpointId(vbid)) {
            vbuckets.setPersistenceCheckpointId(vbid, persistenceCheckpointIds[vbid]);
            schedule_vb_snapshot = true;
        }
    }
    lh.unlock();

    // Schedule the vbucket state snapshot task to record the latest checkpoint Id
    // that was successfully persisted for each vbucket.
    if (schedule_vb_snapshot) {
        scheduleVBSnapshot(Priority::VBucketPersistHighPriority);
    }

    stats.flusher_todo.set(writing.size());
    stats.queue_size.set(getWriteQueueSize());
    rel_time_t complete_time = ep_current_time();
    stats.flushDuration.set(complete_time - flush_start);
    stats.flushDurationHighWat.set(std::max(stats.flushDuration.get(),
                                            stats.flushDurationHighWat.get()));
    stats.cumulativeFlushTime.incr(complete_time - flush_start);
}

int EventuallyPersistentStore::flushSome(std::queue<queued_item> *q,
                                         std::queue<queued_item> *rejectQueue) {
    if (!tctx.enter()) {
        ++stats.beginFailed;
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Failed to start a transaction.\n");
        // Copy the input queue into the reject queue.
        while (!q->empty()) {
            rejectQueue->push(q->front());
            q->pop();
        }
        return 1; // This will cause us to jump out and delay a second
    }
    int tsz = tctx.remaining();
    int oldest = stats.min_data_age;
    int completed(0);
    for (completed = 0;
         completed < tsz && !q->empty() && !shouldPreemptFlush(completed);
         ++completed) {

        int n = flushOne(q, rejectQueue);
        if (n != 0 && n < oldest) {
            oldest = n;
        }
    }
    if (shouldPreemptFlush(completed)) {
        ++stats.flusherPreempts;
    } else {
        tctx.commit();
    }
    tctx.leave(completed);
    return oldest;
}

size_t EventuallyPersistentStore::getWriteQueueSize(void) {
    size_t size = 0;
    size_t numOfVBuckets = vbuckets.getSize();
    for (size_t i = 0; i < numOfVBuckets; ++i) {
        assert(i <= std::numeric_limits<uint16_t>::max());
        uint16_t vbid = static_cast<uint16_t>(i);
        RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
        if (vb && (vb->getState() != vbucket_state_dead)) {
            size += vb->checkpointManager.getNumItemsForPersistence() + vb->getBackfillSize();
        }
    }
    return size;
}

bool EventuallyPersistentStore::hasItemsForPersistence(void) {
    bool hasItems = false;
    size_t numOfVBuckets = vbuckets.getSize();
    for (size_t i = 0; i < numOfVBuckets; ++i) {
        assert(i <= std::numeric_limits<uint16_t>::max());
        uint16_t vbid = static_cast<uint16_t>(i);
        RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
        if (vb && (vb->getState() != vbucket_state_dead)) {
            LockHolder rlh(restore.mutex);
            std::map<uint16_t, std::vector<queued_item> >::iterator it = restore.items.find(vbid);
            if (vb->checkpointManager.hasNextForPersistence() ||
                vb->getBackfillSize() > 0 ||
                (it != restore.items.end() && !it->second.empty())) {
                hasItems = true;
                break;
            }
        }
    }
    return hasItems;
}

void EventuallyPersistentStore::setPersistenceCheckpointId(uint16_t vbid, uint64_t checkpointId) {
    LockHolder lh(vbsetMutex);
    vbuckets.setPersistenceCheckpointId(vbid, checkpointId);
}


protocol_binary_response_status EventuallyPersistentStore::revertOnlineUpdate(RCPtr<VBucket> vb) {
    protocol_binary_response_status rv(PROTOCOL_BINARY_RESPONSE_SUCCESS);

    const char *msg = NULL;
    size_t msg_size = 0;
    std::vector<queued_item> item_list;

    if (!vb || vb->getState() == vbucket_state_dead) {
        return rv;
    }

    uint16_t vbid = vb->getId();
    BlockTimer timer(&stats.checkpointRevertHisto);

    //Acquire a lock before starting the hot reload process
    LockHolder lh(vbsetMutex);
    rv = vb->checkpointManager.beginHotReload();
    if ( rv != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        return rv;
    }
    lh.unlock();

    // Get all the mutations from the current position of the online update cursor to
    // the tail of the current open checkpoint.
    vb->checkpointManager.getAllItemsForOnlineUpdate(item_list);
    if (item_list.size() == 0) {
        // Need to count items for checkpoint_start, checkpoint_end, onlineupdate_start,
        // onlineupdate_revert
        vb->checkpointManager.endHotReload(4);
        return rv;
    }

    std::set<queued_item, CompareQueuedItemsByKey> item_set;
    std::pair<std::set<queued_item, CompareQueuedItemsByKey>::iterator, bool> ret;
    std::vector<queued_item>::reverse_iterator reverse_it = item_list.rbegin();
    // Perform further deduplication here by removing duplicate mutations for each key.
    // For this, traverse the array from the last element.
    uint64_t total = 0;
    for(; reverse_it != item_list.rend(); ++reverse_it, ++total) {
        queued_item qi = *reverse_it;

        ret = item_set.insert(qi);

        vb->doStatsForFlushing(*qi, qi->size());
    }
    item_list.assign(item_set.begin(), item_set.end());

    std::vector<queued_item>::iterator it = item_list.begin();
    for(; it != item_list.end(); ++it) {
        if ((*it)->getOperation() == queue_op_del)  {
            ++stats.numRevertDeletes;
            //Reset the deleted value first before evict it.
            Item itm((*it)->getKey().c_str(), (*it)->getKey().length(), 0,
                     0, 0, 0, -1, (*it)->getVBucketId());
            vb->ht.add(itm, false, false);
            this->evictKey((*it)->getKey(), vbid, &msg, &msg_size, true);
        } else if ((*it)->getOperation() == queue_op_set) {
            //check if it is add or set
            if ((*it)->getRowId() < 0)  {
                ++stats.numRevertAdds;
                //since no value exists on disk, simply delete it from hashtable
                vb->ht.del((*it)->getKey());
            } else {
                ++stats.numRevertUpdates;
                this->evictKey((*it)->getKey(), vbid, &msg, &msg_size, true);
            }
        }

    }
    item_list.clear();

    //Stop the hot reload process
    vb->checkpointManager.endHotReload(total);
    engine.getTapConnMap().notify();

    return rv;
}

/**
 * Callback invoked after persisting an item from memory to disk.
 *
 * This class exists to create a closure around a few variables within
 * EventuallyPersistentStore::flushOne so that an object can be
 * requeued in case of failure to store in the underlying layer.
 */
class PersistenceCallback : public Callback<mutation_result>,
                            public Callback<int> {
public:

    PersistenceCallback(const queued_item &qi, std::queue<queued_item> *q,
                        EventuallyPersistentStore *st, MutationLog *ml,
                        rel_time_t qd, rel_time_t d, EPStats *s) :
        queuedItem(qi), rq(q), store(st), mutationLog(ml),
        queued(qd), dirtied(d), stats(s) {

        assert(rq);
        assert(s);
    }

    // This callback is invoked for set only.
    void callback(mutation_result &value) {
        if (value.first == 1) {
            stats->totalPersisted++;
            if (value.second > 0) {
                mutationLog->newItem(queuedItem->getVBucketId(), queuedItem->getKey(),
                                     value.second);
                ++stats->newItems;
                setId(value.second);
            }
            RCPtr<VBucket> vb = store->getVBucket(queuedItem->getVBucketId());
            if (vb && vb->getState() != vbucket_state_active &&
                vb->getState() != vbucket_state_pending) {
                int bucket_num(0);
                LockHolder lh = vb->ht.getLockedBucket(queuedItem->getKey(), &bucket_num);
                StoredValue *v = store->fetchValidValue(vb, queuedItem->getKey(),
                                                        bucket_num, true);
                double current = static_cast<double>(StoredValue::getCurrentSize(*stats));
                double lower = static_cast<double>(stats->mem_low_wat);
                if (v && current > lower) {
                    // Check if the key exists in the open or closed checkpoints.
                    bool foundInCheckpoints =
                        vb->checkpointManager.isKeyResidentInCheckpoints(v->getKey());
                    if (!foundInCheckpoints &&
                        v->ejectValue(*stats, vb->ht) &&
                        vb->getState() == vbucket_state_replica) {
                        ++stats->numReplicaEjects;
                    }
                }
            }
        } else {
            // If the return was 0 here, we're in a bad state because
            // we do not know the rowid of this object.
            if (value.first == 0) {
                RCPtr<VBucket> vb = store->getVBucket(queuedItem->getVBucketId());
                int bucket_num(0);
                LockHolder lh = vb->ht.getLockedBucket(queuedItem->getKey(), &bucket_num);
                StoredValue *v = store->fetchValidValue(vb, queuedItem->getKey(),
                                                        bucket_num, true);
                if (v) {
                    std::stringstream ss;
                    ss << "Persisting ``" << queuedItem->getKey() << "'' on vb"
                       << queuedItem->getVBucketId() << " (rowid=" << v->getId()
                       << ") returned 0 updates\n";
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL, "%s", ss.str().c_str());
                } else {
                    getLogger()->log(EXTENSION_LOG_INFO, NULL,
                                     "Error persisting now missing ``%s'' from vb%d\n",
                                     queuedItem->getKey().c_str(), queuedItem->getVBucketId());
                }
            } else {
                redirty();
            }
        }
    }

    // This callback is invoked for deletions only.
    //
    // The boolean indicates whether the underlying storage
    // successfully deleted the item.
    void callback(int &value) {
        // > 1 would be bad.  We were only trying to delete one row.
        assert(value < 2);
        // -1 means fail
        // 1 means we deleted one row
        // 0 means we did not delete a row, but did not fail (did not exist)
        if (value >= 0) {
            RCPtr<VBucket> vb = store->getVBucket(queuedItem->getVBucketId());
            if (value > 0) {
                stats->totalPersisted++;
                ++stats->delItems;
                ++vb->opsDelete;
            }

            mutationLog->delItem(queuedItem->getVBucketId(), queuedItem->getKey());

            // We have succesfully removed an item from the disk, we
            // may now remove it from the hash table.
            if (vb) {
                int bucket_num(0);
                LockHolder lh = vb->ht.getLockedBucket(queuedItem->getKey(), &bucket_num);
                StoredValue *v = store->fetchValidValue(vb, queuedItem->getKey(),
                                                        bucket_num, true);
                if (v && v->isDeleted()) {
                    if (store->getEPEngine().isDegradedMode()) {
                        LockHolder rlh(store->restore.mutex);
                        store->restore.itemsDeleted.insert(queuedItem->getKey());
                    }
                    bool deleted = vb->ht.unlocked_del(queuedItem->getKey(),
                                                       bucket_num);
                    assert(deleted);
                } else if (v) {
                    v->clearId();
                }
            }
        } else {
            redirty();
        }
    }

private:

    void setId(int64_t id) {
        bool did = store->invokeOnLockedStoredValue(queuedItem->getKey(),
                                                    queuedItem->getVBucketId(),
                                                    &StoredValue::setId,
                                                    id);
        if (!did) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Failed to set id on vb%d ``%s''\n",
                             queuedItem->getVBucketId(), queuedItem->getKey().c_str());
        }
    }

    void redirty() {
        ++stats->flushFailed;
        store->invokeOnLockedStoredValue(queuedItem->getKey(),
                                         queuedItem->getVBucketId(),
                                         &StoredValue::reDirty,
                                         dirtied);
        rq->push(queuedItem);
    }

    const queued_item queuedItem;
    std::queue<queued_item> *rq;
    EventuallyPersistentStore *store;
    MutationLog *mutationLog;
    rel_time_t queued;
    rel_time_t dirtied;
    EPStats *stats;
    DISALLOW_COPY_AND_ASSIGN(PersistenceCallback);
};

int EventuallyPersistentStore::flushOneDeleteAll() {
    rwUnderlying->reset();
    // Log a flush of every known vbucket.
    std::vector<int> vbs(vbuckets.getBuckets());
    for (std::vector<int>::iterator it(vbs.begin()); it != vbs.end(); ++it) {
        mutationLog.deleteAll(static_cast<uint16_t>(*it));
    }
    // This is happening in an independent transaction, so we're going
    // go ahead and commit it out.
    mutationLog.commit1();
    mutationLog.commit2();
    diskFlushAll.cas(true, false);
    return 1;
}

// While I actually know whether a delete or set was intended, I'm
// still a bit better off running the older code that figures it out
// based on what's in memory.
int EventuallyPersistentStore::flushOneDelOrSet(const queued_item &qi,
                                           std::queue<queued_item> *rejectQueue) {

    RCPtr<VBucket> vb = getVBucket(qi->getVBucketId());
    if (!vb) {
        return 0;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(qi->getKey(), &bucket_num);
    StoredValue *v = fetchValidValue(vb, qi->getKey(), bucket_num, true);

    size_t itemBytes = qi->size();
    vb->doStatsForFlushing(*qi, itemBytes);

    bool found = v != NULL;
    int64_t rowid = found ? v->getId() : -1;
    bool deleted = found && v->isDeleted();
    bool isDirty = found && v->isDirty();
    rel_time_t queued(qi->getQueuedTime()), dirtied(0);

    Item itm(qi->getKey(),
             found ? v->getFlags() : 0,
             found ? v->getExptime() : 0,
             found ? v->getValue() : value_t(NULL),
             found ? v->getCas() : 0,
             rowid,
             qi->getVBucketId(),
             found ? v->getSeqno() : 0);

    int ret = 0;

    if (!deleted && isDirty && v->isExpired(ep_real_time() + itemExpiryWindow)) {
        ++stats.flushExpired;
        v->markClean(&dirtied);
        isDirty = false;
        // If the new item is expired within current_time + expiry_window, clear the row id
        // from hashtable and remove the old item from database.
        v->clearId();
        deleted = true;
    }

    if (isDirty) {
        dirtied = v->getDataAge();
        // Calculate stats if this had a positive time.
        rel_time_t now = ep_current_time();
        int dataAge = now - dirtied;
        int dirtyAge = now - queued;
        bool eligible = true;

        if (v->isPendingId()) {
            eligible = false;
        } else if (dirtyAge > stats.queue_age_cap.get()) {
            ++stats.tooOld;
        } else if (dataAge < stats.min_data_age.get()) {
            eligible = false;
            // Skip this one.  It's too young.
            ret = stats.min_data_age.get() - dataAge;
            ++stats.tooYoung;
        }

        if (eligible) {
            assert(dirtyAge < (86400 * 30));
            stats.dirtyAgeHisto.add(dirtyAge / 1000);
            stats.dataAgeHisto.add(dataAge / 1000);
            stats.dirtyAge.set(dirtyAge);
            stats.dataAge.set(dataAge);
            stats.dirtyAgeHighWat.set(std::max(stats.dirtyAge.get(),
                                               stats.dirtyAgeHighWat.get()));
            stats.dataAgeHighWat.set(std::max(stats.dataAge.get(),
                                              stats.dataAgeHighWat.get()));
        } else {
            isDirty = false;
            v->reDirty(dirtied);
            rejectQueue->push(qi);
            ++vb->opsReject;
        }
    }

    if (isDirty && !deleted) {
        if (qi->getVBucketVersion() != vbuckets.getBucketVersion(qi->getVBucketId())) {
            lh.unlock();
        } else {
            // If a vbucket snapshot task with the high priority is currently scheduled,
            // requeue the persistence task and wait until the snapshot task is completed.
            if (vbuckets.isHighPriorityVbSnapshotScheduled()) {
                v->clearPendingId();
                lh.unlock();
                rejectQueue->push(qi);
                ++vb->opsReject;
            } else {
                assert(rowid == v->getId());
                if (rowid == -1) {
                    v->setPendingId();
                }

                // TODO: An item should be marked as clean in TransactionContext::commit()
                // to support a consistent read from disk after the item is ejected.
                v->markClean(NULL);
                lh.unlock();
                BlockTimer timer(rowid == -1 ?
                                 &stats.diskInsertHisto : &stats.diskUpdateHisto,
                                 rowid == -1 ? "disk_insert" : "disk_update",
                                 stats.timingLog);
                PersistenceCallback *cb;
                cb = new PersistenceCallback(qi, rejectQueue, this, &mutationLog,
                                             queued, dirtied, &stats);
                tctx.addCallback(cb);
                rwUnderlying->set(itm, qi->getVBucketVersion(), *cb);
                if (rowid == -1)  {
                    ++vb->opsCreate;
                } else {
                    ++vb->opsUpdate;
                }
            }
        }
    } else if (deleted) {
        lh.unlock();
        BlockTimer timer(&stats.diskDelHisto, "disk_delete", stats.timingLog);

        PersistenceCallback *cb;
        cb = new PersistenceCallback(qi, rejectQueue, this, &mutationLog,
                                     queued, dirtied, &stats);
        if (rowid > 0) {
            uint16_t vbid(qi->getVBucketId());
            uint16_t vbver(vbuckets.getBucketVersion(vbid));
            tctx.addCallback(cb);
            rwUnderlying->del(itm, rowid, vbver, *cb);
        } else {
            // bypass deletion if missing items, but still call the
            // deletion callback for clean cleanup.
            int affected(0);
            cb->callback(affected);
            delete cb;
        }
    }

    return ret;
}

int EventuallyPersistentStore::flushOne(std::queue<queued_item> *q,
                                        std::queue<queued_item> *rejectQueue) {

    queued_item qi = q->front();
    q->pop();
    stats.memOverhead.decr(sizeof(queued_item));
    assert(stats.memOverhead.get() < GIGANTOR);

    int rv = 0;
    switch (qi->getOperation()) {
    case queue_op_flush:
        rv = flushOneDeleteAll();
        break;
    case queue_op_set:
        if (qi->getVBucketVersion() == vbuckets.getBucketVersion(qi->getVBucketId())) {
            size_t prevRejectCount = rejectQueue->size();

            rv = flushOneDelOrSet(qi, rejectQueue);
            if (rejectQueue->size() == prevRejectCount) {
                // flush operation was not rejected
                tctx.addUncommittedItem(qi);
            }
        }
        break;
    case queue_op_del:
        rv = flushOneDelOrSet(qi, rejectQueue);
        break;
    case queue_op_commit:
        tctx.commit();
        tctx.enter();
        break;
    case queue_op_empty:
        assert(false);
        break;
    default:
        break;
    }
    stats.flusher_todo--;

    return rv;

}

void EventuallyPersistentStore::queueDirty(const std::string &key,
                                           uint16_t vbid,
                                           enum queue_operation op,
                                           uint32_t seqno,
                                           int64_t rowid,
                                           bool tapBackfill) {
    if (doPersistence) {
        RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
        if (vb) {
            QueuedItem *qi = new QueuedItem(key, vbid, op,
                                            vbuckets.getBucketVersion(vbid),
                                            rowid, seqno);

            queued_item itm(qi);
            bool rv = tapBackfill ?
                      vb->queueBackfillItem(itm) : vb->checkpointManager.queueDirty(itm, vb);
            if (rv) {
                ++stats.queue_size;
                ++stats.totalEnqueued;
                vb->doStatsForQueueing(*itm, itm->size());
            }
        }
    }
}

int EventuallyPersistentStore::restoreItem(const Item &itm, enum queue_operation op)
{
    const std::string &key = itm.getKey();
    uint16_t vbid = itm.getVBucketId();
    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    if (!vb) {
        return -1;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    LockHolder rlh(restore.mutex);
    if (restore.itemsDeleted.find(key) == restore.itemsDeleted.end() &&
        vb->ht.unlocked_restoreItem(itm, op, bucket_num)) {

        lh.unlock();
        queued_item qi(new QueuedItem(key, vbid, op, vbuckets.getBucketVersion(vbid)));
        std::map<uint16_t, std::vector<queued_item> >::iterator it = restore.items.find(vbid);
        if (it != restore.items.end()) {
            it->second.push_back(qi);
        } else {
            std::vector<queued_item> vb_items;
            vb_items.push_back(qi);
            restore.items[vbid] = vb_items;
        }
        return 0;
    }

    return 1;
}

std::map<std::pair<uint16_t, uint16_t>, vbucket_state> EventuallyPersistentStore::loadVBucketState() {
    return roUnderlying->listPersistedVbuckets();
}

void EventuallyPersistentStore::completeDegradedMode() {
    LockHolder lh(restore.mutex);
    restore.itemsDeleted.clear();
}

void EventuallyPersistentStore::warmupCompleted() {
    engine.warmupCompleted();
    if (!engine.isDegradedMode()) {
        completeDegradedMode();
    }
}

static void warmupLogCallback(void *arg, uint16_t vb, uint16_t vbver,
                              const std::string &key, uint64_t rowid) {
    shared_ptr<Callback<GetValue> > *cb = reinterpret_cast<shared_ptr<Callback<GetValue> >*>(arg);
    Item *itm = new Item(key.data(), key.size(),
                         0, // flags
                         0, // exp
                         NULL, 0, // data
                         0, // CAS
                         rowid,
                         vb);

    GetValue gv(itm, ENGINE_SUCCESS, rowid, vbver, NULL, true /* partial */);

    (*cb)->callback(gv);
}

bool EventuallyPersistentStore::warmupFromLog(const std::map<std::pair<uint16_t, uint16_t>,
                                                             vbucket_state> &state,
                                              shared_ptr<Callback<GetValue> > cb) {

    if (!mutationLog.exists()) {
        return false;
    }

    bool rv(true);

    MutationLogHarvester harvester(mutationLog);
    for (std::map<std::pair<uint16_t, uint16_t>, vbucket_state>::const_iterator it = state.begin();
         it != state.end(); ++it) {

        harvester.setVbVer(it->first.first, it->first.second);
    }

    hrtime_t start(gethrtime());
    rv = harvester.load();
    hrtime_t end1(gethrtime());

    if (!rv) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Failed to read mutation log: %s",
                         mutationLog.getLogFile().c_str());
        return false;
    }

    if (harvester.total() == 0) {
        // We didn't read a single item from the log..
        // @todo. the harvester should be extened to either
        // "throw" a FileNotFound exception, or a method we may
        // look at in order to check if it existed.
        return false;
    }

    getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                     "Completed log read in %s with %d entries\n",
                     hrtime2text(end1 - start).c_str(), harvester.total());

    harvester.apply(&cb, &warmupLogCallback);
    mutationLog.resetCounts(harvester.getItemsSeen());

    hrtime_t end2(gethrtime());
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                     "Completed repopulation from log in %dms\n",
                     ((end2 - end1) / 1000000));

    // Anything left in the "loading" map at this point is uncommitted.
    // TODO:  Forward reconciliation of uncommitted data.

    return rv;
}

void
EventuallyPersistentStore::warmup(const std::map<std::pair<uint16_t, uint16_t>, vbucket_state> &st,
                                  bool keysOnly) {
    LoadStorageKVPairCallback *load_cb = new LoadStorageKVPairCallback(vbuckets, stats, this);
    shared_ptr<Callback<GetValue> > cb(load_cb);
    std::map<std::pair<uint16_t, uint16_t>, vbucket_state>::const_iterator it;
    std::vector<uint16_t> vbids;
    for (it = st.begin(); it != st.end(); ++it) {
        std::pair<uint16_t, uint16_t> vbp = it->first;
        vbucket_state vbs = it->second;
        if (vbs.state == vbucket_state_active || vbs.state == vbucket_state_replica) {
            vbids.push_back(vbp.first);
        }

        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "Loading %s for vbucket %d - was in %s state\n",
                         keysOnly ? "keys" : "data", vbp.first,
                         VBucket::toString(vbs.state));
        load_cb->initVBucket(vbp.first, vbp.second, vbs.checkpointId + 1,
                       vbs.state);
    }

    if (keysOnly) {
        bool readLog(false);

        try {
            readLog = warmupFromLog(st, cb);
        } catch(MutationLog::ReadException e) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Error reading warmup log:  %s", e.what());
        }
        if (!readLog && roUnderlying->isKeyDumpSupported()) {
            roUnderlying->dumpKeys(vbids, cb);
        }
    } else {
        roUnderlying->dump(cb);
        invalidItemDbPager->createRangeList();
    }
}

void EventuallyPersistentStore::setExpiryPagerSleeptime(size_t val) {
    LockHolder lh(expiryPager.mutex);

    if (expiryPager.sleeptime != 0) {
        getNonIODispatcher()->cancel(expiryPager.task);
    }

    expiryPager.sleeptime = val;
    if (val != 0) {
        shared_ptr<DispatcherCallback> exp_cb(new ExpiredItemPager(this, stats,
                                                                   expiryPager.sleeptime));

        getNonIODispatcher()->schedule(exp_cb, &expiryPager.task,
                                       Priority::ItemPagerPriority,
                                       expiryPager.sleeptime);
    }
}

void EventuallyPersistentStore::visit(VBucketVisitor &visitor)
{
    size_t maxSize = vbuckets.getSize();
    for (size_t i = 0; i <= maxSize; ++i) {
        assert(i <= std::numeric_limits<uint16_t>::max());
        uint16_t vbid = static_cast<uint16_t>(i);
        RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
        if (vb) {
            bool wantData = visitor.visitBucket(vb);
            // We could've lost this along the way.
            if (wantData) {
                vb->ht.visit(visitor);
            }
        }
    }
    visitor.complete();
}

void LoadStorageKVPairCallback::initVBucket(uint16_t vbid, uint16_t vb_version,
                                            uint64_t checkpointId, vbucket_state_t prevState) {
    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    if (!vb) {
        vb.reset(new VBucket(vbid, vbucket_state_dead, stats,
                             epstore->getEPEngine().getCheckpointConfig()));
        vbuckets.addBucket(vb);
    }
    // Set the past initial state of each vbucket.
    vb->setInitialState(prevState);
    // Pass the open checkpoint Id for each vbucket.
    vb->checkpointManager.setOpenCheckpointId(checkpointId);
    // For each vbucket, set its vbucket version.
    vbuckets.setBucketVersion(vbid, vb_version);
    // For each vbucket, set its latest checkpoint Id that was
    // successfully persisted.
    vbuckets.setPersistenceCheckpointId(vbid, checkpointId - 1);
}

void LoadStorageKVPairCallback::callback(GetValue &val) {
    Item *i = val.getValue();
    if (i != NULL) {
        uint16_t vb_version = vbuckets.getBucketVersion(i->getVBucketId());
        if (vb_version != static_cast<uint16_t>(-1) && val.getVBucketVersion() != vb_version) {
            epstore->getInvalidItemDbPager()->addInvalidItem(i, val.getVBucketVersion());

            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Received invalid item (v %d != v %d).. ignored",
                             val.getVBucketVersion(), vb_version);

            delete i;
            return;
        }

        RCPtr<VBucket> vb = vbuckets.getBucket(i->getVBucketId());
        if (!vb) {
            vb.reset(new VBucket(i->getVBucketId(), vbucket_state_dead, stats,
                                 epstore->getEPEngine().getCheckpointConfig()));
            vbuckets.addBucket(vb);
            vbuckets.setBucketVersion(i->getVBucketId(), val.getVBucketVersion());
        }
        bool succeeded(false);
        int retry = 2;
        do {
            switch (vb->ht.insert(*i, shouldEject(), val.isPartial())) {
            case NOMEM:
                if (retry == 2) {
                    if (hasPurged) {
                        if (++stats.warmOOM == 1) {
                            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                             "Warmup dataload failure: max_size too low.");
                        }
                    } else {
                        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                         "Emergency startup purge to free space for load.");
                        purge();
                    }
                } else {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Cannot store an item after emergency purge.");
                    ++stats.warmOOM;
                }
                break;
            case INVALID_CAS:
                if (epstore->getROUnderlying()->isKeyDumpSupported()) {
                    getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                        "Value changed in memory before restore from disk. Ignored disk value for: %s.",
                         i->getKey().c_str());
                } else {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                        "Warmup dataload error: Duplicate key: %s.",
                        i->getKey().c_str());
                }
                ++stats.warmDups;
                succeeded = true;
                break;
            case NOT_FOUND:
                succeeded = true;
                break;
            default:
                abort();
            }
        } while (!succeeded && retry > 0);

        if (succeeded && i->isExpired(startTime)) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Item was expired at load:  %s",
                             i->getKey().c_str());
            epstore->deleteItem(i->getKey(),
                                0, 0, // seqno, cas
                                i->getVBucketId(), NULL,
                                true, false); // force, use_meta
        }
        delete i;
    }
    if (!val.isPartial()) {
        ++stats.warmedUp;
    }
}

void LoadStorageKVPairCallback::purge() {
    class EmergencyPurgeVisitor : public VBucketVisitor {
    public:
        EmergencyPurgeVisitor(EPStats &s) : stats(s) {}

        void visit(StoredValue *v) {
            v->ejectValue(stats, currentBucket->ht);
        }
    private:
        EPStats &stats;
    };

    std::vector<int> vbucketIds(vbuckets.getBuckets());
    std::vector<int>::iterator it;
    EmergencyPurgeVisitor epv(stats);
    for (it = vbucketIds.begin(); it != vbucketIds.end(); ++it) {
        int vbid = *it;
        RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
        if (vb && epv.visitBucket(vb)) {
            vb->ht.visit(epv);
        }
    }
    hasPurged = true;
}

bool TransactionContext::enter() {
    if (!intxn) {
        _remaining = txnSize.get();
        intxn = underlying->begin();
    }
    return intxn;
}

void TransactionContext::leave(int completed) {
    _remaining -= completed;
    if (remaining() <= 0 && intxn) {
        commit();
    }
}

void TransactionContext::commit() {
    BlockTimer timer(&stats.diskCommitHisto, "disk_commit", stats.timingLog);
    rel_time_t cstart = ep_current_time();
    mutationLog.commit1();
    while (!underlying->commit()) {
        sleep(1);
        ++stats.commitFailed;
    }
    mutationLog.commit2();
    ++stats.flusherCommits;

    std::list<PersistenceCallback*>::iterator iter;
    for (iter = transactionCallbacks.begin();
         iter != transactionCallbacks.end();
         ++iter) {
        delete *iter;
    }
    transactionCallbacks.clear();
    rel_time_t complete_time = ep_current_time();

    stats.commit_time.set(complete_time - cstart);
    stats.cumulativeCommitTime.incr(complete_time - cstart);
    intxn = false;
    uncommittedItems.clear();
    numUncommittedItems = 0;
}

void TransactionContext::addUncommittedItem(const queued_item &qi) {
    uncommittedItems.push_back(qi);
    ++numUncommittedItems;
}

VBCBAdaptor::VBCBAdaptor(EventuallyPersistentStore *s,
                         shared_ptr<VBucketVisitor> v,
                         const char *l, double sleep) :
    store(s), visitor(v), label(l), sleepTime(sleep), currentvb(0)
{
    const VBucketFilter &vbFilter = visitor->getVBucketFilter();
    size_t maxSize = store->vbuckets.getSize();
    for (size_t i = 0; i <= maxSize; ++i) {
        assert(i <= std::numeric_limits<uint16_t>::max());
        uint16_t vbid = static_cast<uint16_t>(i);
        RCPtr<VBucket> vb = store->vbuckets.getBucket(vbid);
        if (vb && vbFilter(vbid)) {
            vbList.push(vbid);
        }
    }
}

bool VBCBAdaptor::callback(Dispatcher & d, TaskId t) {
    if (!vbList.empty()) {
        currentvb = vbList.front();
        RCPtr<VBucket> vb = store->vbuckets.getBucket(currentvb);
        if (vb) {
            if (visitor->pauseVisitor()) {
                d.snooze(t, sleepTime);
                return true;
            }
            if (visitor->visitBucket(vb)) {
                vb->ht.visit(*visitor);
            }
        }
        vbList.pop();
    }

    bool isdone = vbList.empty();
    if (isdone) {
        visitor->complete();
    }
    return !isdone;
}
