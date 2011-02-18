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
#include <functional>

#include "ep.hh"
#include "flusher.hh"
#include "locks.hh"
#include "dispatcher.hh"
#include "kvstore.hh"
#include "ep_engine.h"
#include "htresizer.hh"

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
 * Wake up connections blocked on pending vbuckets when their state
 * changes.
 */
class NotifyVBStateChangeCallback : public DispatcherCallback {
public:
    NotifyVBStateChangeCallback(RCPtr<VBucket> vb, EventuallyPersistentEngine &e)
        : vbucket(vb), engine(e) { }

    bool callback(Dispatcher &, TaskId) {
        vbucket->fireAllOps(engine);
        return false;
    }

    std::string description() {
        std::stringstream ss;
        ss << "Notifying state change of vbucket " << vbucket->getId();
        return ss.str();
    }

private:
    RCPtr<VBucket>              vbucket;
    EventuallyPersistentEngine &engine;
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
        if (ep->completeVBucketDeletion(vbucket, vbver) == vbucket_del_success) {
            hrtime_t wall_time = (gethrtime() - start_time) / 1000;
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
        chunk_del_range_size = current_range->second - current_range->first;
    }

    bool callback(Dispatcher &d, TaskId t) {
        bool rv = false, isLastChunk = false;

        chunk_range range;
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
        int64_t range_size = current_range->second - current_range->first;
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
    chunk_range_iterator          current_range;
};

EventuallyPersistentStore::EventuallyPersistentStore(EventuallyPersistentEngine &theEngine,
                                                     KVStore *t,
                                                     bool startVb0,
                                                     bool concurrentDB) :
    engine(theEngine), stats(engine.getEpStats()), rwUnderlying(t),
    storageProperties(t->getStorageProperties()), tctx(stats, t, theEngine.syncRegistry),
    bgFetchDelay(0)
{
    getLogger()->log(EXTENSION_LOG_INFO, NULL,
                     "Storage props:  c=%d/r=%d/rw=%d\n",
                     storageProperties.maxConcurrency(),
                     storageProperties.maxReaders(),
                     storageProperties.maxWriters());

    doPersistence = getenv("EP_NO_PERSISTENCE") == NULL;
    dispatcher = new Dispatcher();
    if (storageProperties.maxConcurrency() > 1
        && storageProperties.maxReaders() > 1
        && concurrentDB) {
        roUnderlying = engine.newKVStore();
        roDispatcher = new Dispatcher();
        roDispatcher->start();
    } else {
        roUnderlying = rwUnderlying;
        roDispatcher = dispatcher;
    }
    nonIODispatcher = new Dispatcher();
    flusher = new Flusher(this, dispatcher);
    invalidItemDbPager = new InvalidItemDbPager(this, stats, engine.getVbDelChunkSize());

    stats.memOverhead = sizeof(EventuallyPersistentStore);

    setTxnSize(DEFAULT_TXN_SIZE);

    if (startVb0) {
        RCPtr<VBucket> vb(new VBucket(0, vbucket_state_active, stats));
        vbuckets.addBucket(vb);
        vbuckets.setBucketVersion(0, 0);
    }

    numbOfWriteQueues = rwUnderlying->getNumShards();
    towrite = new AtomicQueue<QueuedItem>[numbOfWriteQueues];

    startDispatcher();
    startFlusher();
    startNonIODispatcher();
    assert(rwUnderlying);
    assert(roUnderlying);
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
    delete[] towrite;
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
    bool rv = flusher->stop();
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

/// @cond DETAILS
/**
 * Inner loop of deleteMany.
 */
class Deleter {
public:
    Deleter(EventuallyPersistentStore *ep) : e(ep) {}
    void operator() (std::pair<uint16_t, std::string> vk) {
        RCPtr<VBucket> vb = e->getVBucket(vk.first);
        if (vb) {
            int bucket_num(0);
            LockHolder lh = vb->ht.getLockedBucket(vk.second, &bucket_num);
            StoredValue *v = vb->ht.unlocked_find(vk.second, bucket_num);
            if (v) {
                vb->ht.unlocked_softDelete(vk.second, 0, bucket_num);
                e->queueDirty(vk.second, vb->getId(), queue_op_del, v->getId(), v->getKeyValLength());
            }
        }
    }
private:
    EventuallyPersistentStore *e;
};
/// @endcond

void EventuallyPersistentStore::deleteMany(std::list<std::pair<uint16_t, std::string> > &keys) {
    // This can be made a lot more efficient, but I'd rather see it
    // show up in a profiling report first.
    std::for_each(keys.begin(), keys.end(), Deleter(this));
}

StoredValue *EventuallyPersistentStore::fetchValidValue(RCPtr<VBucket> vb,
                                                        const std::string &key,
                                                        int bucket_num,
                                                        bool wantDeleted) {
    StoredValue *v = vb->ht.unlocked_find(key, bucket_num, wantDeleted);
    if (v && v->isDeleted()) {
        // In the deleted case, we ignore expiration time.
        return v;
    } else if (v && v->isExpired(ep_real_time())) {
        ++stats.expired;
        vb->ht.unlocked_softDelete(key, 0, bucket_num);
        queueDirty(key, vb->getId(), queue_op_del, v->getId(), v->getKeyValLength());
        return NULL;
    }
    return v;
}

protocol_binary_response_status EventuallyPersistentStore::evictKey(const std::string &key,
                                                                    uint16_t vbucket,
                                                                    const char **msg,
                                                                    size_t *) {
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!(vb && vb->getState() == vbucket_state_active)) {
        return PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num);

    protocol_binary_response_status rv(PROTOCOL_BINARY_RESPONSE_SUCCESS);

    if (v) {
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

ENGINE_ERROR_CODE EventuallyPersistentStore::set(const Item &item,
                                                 const void *cookie,
                                                 bool force) {

    RCPtr<VBucket> vb = getVBucket(item.getVBucketId());
    if (!vb || vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_active) {
        // OK
    } else if (vb->getState() == vbucket_state_replica && !force) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_pending && !force) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    }

    bool cas_op = (item.getCas() != 0);

    int64_t row_id = -1;
    mutation_type_t mtype = vb->ht.set(item, row_id);
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
        // Do normal stuff, but don't enqueue dirty flags.
        break;
    case NOT_FOUND:
        if (cas_op) {
            ret = ENGINE_KEY_ENOENT;
            break;
        }
        // FALLTHROUGH
    case WAS_CLEAN:
        queueDirty(item.getKey(), item.getVBucketId(), queue_op_set, row_id, item.getKey().length() + item.getValue()->length());
        break;
    case INVALID_VBUCKET:
        ret = ENGINE_NOT_MY_VBUCKET;
        break;
    }

    return ret;
}

ENGINE_ERROR_CODE EventuallyPersistentStore::add(const Item &item,
                                                 const void *cookie)
{
    RCPtr<VBucket> vb = getVBucket(item.getVBucketId());
    if (!vb || vb->getState() == vbucket_state_dead || vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_active) {
        // OK
    } else if(vb->getState() == vbucket_state_pending) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    }

    if (item.getCas() != 0) {
        // Adding with a cas value doesn't make sense..
        return ENGINE_NOT_STORED;
    }

    switch (vb->ht.add(item)) {
    case ADD_NOMEM:
        return ENGINE_ENOMEM;
    case ADD_EXISTS:
        return ENGINE_NOT_STORED;
    case ADD_SUCCESS:
    case ADD_UNDEL:
        queueDirty(item.getKey(), item.getVBucketId(), queue_op_set, -1, item.getKey().length() + item.getValue()->length());
    }
    return ENGINE_SUCCESS;
}


void EventuallyPersistentStore::snapshotVBuckets(const Priority &priority) {

    class VBucketStateVisitor : public VBucketVisitor {
    public:
        VBucketStateVisitor(VBucketMap &vb_map) : vbuckets(vb_map) { }
        bool visitBucket(RCPtr<VBucket> vb) {
            std::pair<uint16_t, uint16_t> p(vb->getId(),
                                            vbuckets.getBucketVersion(vb->getId()));
            states[p] = VBucket::toString(vb->getState());
            return false;
        }

        void visit(StoredValue*) {
            assert(false); // this does not happen
        }

        std::map<std::pair<uint16_t, uint16_t>, std::string> states;

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
        nonIODispatcher->schedule(shared_ptr<DispatcherCallback>
                                             (new NotifyVBStateChangeCallback(vb,
                                                                        engine)),
                                  NULL, Priority::NotifyVBStateChangePriority, 0, false);
        scheduleVBSnapshot(Priority::VBucketPersistLowPriority);
    } else {
        RCPtr<VBucket> newvb(new VBucket(vbid, to, stats));
        uint16_t vb_version = vbuckets.getBucketVersion(vbid);
        uint16_t vb_new_version = vb_version == (std::numeric_limits<uint16_t>::max() - 1) ?
                                  0 : vb_version + 1;
        vbuckets.addBucket(newvb);
        vbuckets.setBucketVersion(vbid, vb_new_version);
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
                                 NULL, Priority::VBucketDeletionPriority,
                                 delay, false);
        } else {
            size_t chunk_size = engine.getVbDelChunkSize();
            uint32_t vb_chunk_del_time = engine.getVbChunkDelThresholdTime();
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
        stats.currentSize.decr(vb->ht.memSize);
        assert(stats.currentSize.get() < GIGANTOR);
        stats.totalCacheSize.decr(vb->ht.memSize);
        assert(stats.totalCacheSize.get() < GIGANTOR);
        vbuckets.removeBucket(vbid);
        scheduleVBSnapshot(Priority::VBucketPersistHighPriority);
        scheduleVBDeletion(vb, vb_version);
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
        stats.bgWaitHisto.add(w);
        stats.bgWait += w;
        stats.bgMinWait.setIfLess(w);
        stats.bgMaxWait.setIfBigger(w);

        hrtime_t l = (stop - start) / 1000;
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

GetValue EventuallyPersistentStore::get(const std::string &key,
                                        uint16_t vbucket,
                                        const void *cookie,
                                        bool queueBG, bool honorStates) {
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if (honorStates && vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if (vb->getState() == vbucket_state_active) {
        // OK
    } else if(honorStates && vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if(honorStates && vb->getState() == vbucket_state_pending) {
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
            return GetValue(NULL, ENGINE_EWOULDBLOCK, v->getId());
        }

        // return an invalid cas value if the item is locked
        uint64_t icas = v->isLocked(ep_current_time())
            ? static_cast<uint64_t>(-1)
            : v->getCas();
        GetValue rv(new Item(v->getKey(), v->getFlags(), v->getExptime(),
                             v->getValue(), icas, v->getId(), vbucket),
                    ENGINE_SUCCESS, v->getId());
        return rv;
    } else {
        GetValue rv;
        return rv;
    }
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
        // OK
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
        v->setExptime(engine.getServerApi()->core->realtime(exptime));
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

        // return an invalid cas value if the item is locked
        uint64_t icas = v->isLocked(ep_current_time())
            ? static_cast<uint64_t>(-1)
            : v->getCas();
        GetValue rv(new Item(v->getKey(), v->getFlags(), v->getExptime(),
                             v->getValue(), icas, v->getId(), vbucket),
                    ENGINE_SUCCESS, v->getId());
        return rv;
    } else {
        GetValue rv;
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
        // OK
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

        Item *it = new Item(v->getKey(), v->getFlags(), v->getExptime(),
                            v->getValue(), v->getCas());

        it->setCas();
        v->setCas(it->getCas());

        GetValue rv(it);
        cb.callback(rv);

    } else {
        GetValue rv;
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

void EventuallyPersistentStore::setMinDataAge(int to) {
    stats.min_data_age.set(to);
}

void EventuallyPersistentStore::setQueueAgeCap(int to) {
    stats.queue_age_cap.set(to);
}

ENGINE_ERROR_CODE EventuallyPersistentStore::del(const std::string &key,
                                                 uint64_t cas,
                                                 uint16_t vbucket,
                                                 const void *cookie,
                                                 bool force) {
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb || vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_active) {
        // OK
    } else if(vb->getState() == vbucket_state_replica && !force) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if(vb->getState() == vbucket_state_pending && !force) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    }

    int64_t row_id = -1;
    mutation_type_t delrv = vb->ht.softDelete(key, cas, row_id);
    ENGINE_ERROR_CODE rv;

    if (delrv == NOT_FOUND) {
        rv = ENGINE_KEY_ENOENT;
    } else if (delrv == IS_LOCKED) {
        rv = ENGINE_TMPFAIL;
    } else {
        rv = ENGINE_SUCCESS;
    }

    if (delrv == WAS_CLEAN || (delrv == NOT_FOUND && row_id != -1)) {
        queueDirty(key, vbucket, queue_op_del, row_id, key.length());
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
            stats.currentSize.decr(statvis.memSize);
            assert(stats.currentSize.get() < GIGANTOR);
            stats.totalCacheSize.decr(statvis.memSize);
        }
    }

    std::queue<QueuedItem> items;
    // Clear all the write queues.
    for (size_t i = 0; i < numbOfWriteQueues; ++i) {
        towrite[i].getAll(items);
        while (!items.empty()) {
            QueuedItem qi = items.front();
            stats.memOverhead.decr(qi.size());
            assert(stats.memOverhead.get() < GIGANTOR);
            items.pop();
        }
    }
    // row_id -2 will push the reset operation into the front in the persistence queue.
    queueDirty("", 0, queue_op_flush, -2, 0);
}

void EventuallyPersistentStore::enqueueCommit() {
    QueuedItem causeCommit("", 0, queue_op_commit);
    writing.push(causeCommit);
    stats.memOverhead.incr(causeCommit.size());
    assert(stats.memOverhead.get() < GIGANTOR);
    ++stats.totalEnqueued;
}

std::queue<QueuedItem>* EventuallyPersistentStore::beginFlush() {
    std::queue<QueuedItem> *rv(NULL);
    if (getWriteQueueSize() == 0 && writing.empty()) {
        stats.dirtyAge = 0;
    } else {
        assert(rwUnderlying);
        std::vector<QueuedItem> item_list;
        item_list.reserve(DEFAULT_TXN_SIZE);
        bool shouldCommit(false);
        for (size_t i = 0; i < numbOfWriteQueues; ++i) {
            towrite[i].toArray(item_list);
            rwUnderlying->optimizeWrites(item_list);
            std::vector<QueuedItem>::iterator it = item_list.begin();
            size_t moved(0);
            for (; it != item_list.end(); ++it) {
                writing.push(*it);
                if (shouldCommit) {
                    enqueueCommit();
                    shouldCommit = false;
                }
                ++moved;
            }
            item_list.clear();

            if (moved > 0) {
                shouldCommit = true;
            }
        }

        stats.flusher_todo.set(writing.size());
        stats.queue_size.set(getWriteQueueSize());
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "Flushing %d items with %d still in queue\n",
                         writing.size(), getWriteQueueSize());
        rv = &writing;
    }
    return rv;
}

void EventuallyPersistentStore::completeFlush(std::queue<QueuedItem> *rej,
                                              rel_time_t flush_start) {
    // Requeue the rejects.
    stats.queue_size.incr(rej->size());
    while (!rej->empty()) {
        writing.push(rej->front());
        rej->pop();
    }

    stats.queue_size.set(getWriteQueueSize() + writing.size());
    rel_time_t complete_time = ep_current_time();
    stats.flushDuration.set(complete_time - flush_start);
    stats.flushDurationHighWat.set(std::max(stats.flushDuration.get(),
                                            stats.flushDurationHighWat.get()));
    stats.cumulativeFlushTime.incr(complete_time - flush_start);
}

int EventuallyPersistentStore::flushSome(std::queue<QueuedItem> *q,
                                         std::queue<QueuedItem> *rejectQueue) {
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

    PersistenceCallback(const QueuedItem &qi, std::queue<QueuedItem> *q,
                        EventuallyPersistentStore *st,
                        rel_time_t qd, rel_time_t d, EPStats *s) :
        queuedItem(qi), rq(q), store(st), queued(qd), dirtied(d), stats(s) {
        assert(rq);
        assert(s);
    }

    // This callback is invoked for set only.
    void callback(mutation_result &value) {
        if (value.first == 1) {
            if (value.second > 0) {
                ++stats->newItems;
                setId(value.second);
            }
            RCPtr<VBucket> vb = store->getVBucket(queuedItem.getVBucketId());
            if (vb && vb->getState() != vbucket_state_active) {
                int bucket_num(0);
                LockHolder lh = vb->ht.getLockedBucket(queuedItem.getKey(), &bucket_num);
                StoredValue *v = store->fetchValidValue(vb, queuedItem.getKey(),
                                                        bucket_num, true);
                double current = static_cast<double>(StoredValue::getCurrentSize(*stats));
                double lower = static_cast<double>(stats->mem_low_wat);
                if (v && current > lower) {
                    if (v->ejectValue(*stats, vb->ht) && vb->getState() == vbucket_state_replica) {
                        ++stats->numReplicaEjects;
                    }
                }
            }
        } else {
            // If the return was 0 here, we're in a bad state because
            // we do not know the rowid of this object.
            if (value.first == 0) {
                RCPtr<VBucket> vb = store->getVBucket(queuedItem.getVBucketId());
                int bucket_num(0);
                LockHolder lh = vb->ht.getLockedBucket(queuedItem.getKey(), &bucket_num);
                StoredValue *v = store->fetchValidValue(vb, queuedItem.getKey(),
                                                        bucket_num, true);
                if (v) {
                    std::stringstream ss;
                    ss << "Persisting ``" << queuedItem.getKey() << "'' on vb"
                       << queuedItem.getVBucketId() << " (rowid=" << v->getId()
                       << ") returned 0 updates\n";
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL, "%s", ss.str().c_str());
                } else {
                    getLogger()->log(EXTENSION_LOG_INFO, NULL,
                                     "Error persisting now missing ``%s'' from vb%d\n",
                                     queuedItem.getKey().c_str(), queuedItem.getVBucketId());
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
            if (value > 0) {
                ++stats->delItems;
            }
            // We have succesfully removed an item from the disk, we
            // may now remove it from the hash table.
            RCPtr<VBucket> vb = store->getVBucket(queuedItem.getVBucketId());
            if (vb) {
                int bucket_num(0);
                LockHolder lh = vb->ht.getLockedBucket(queuedItem.getKey(), &bucket_num);
                StoredValue *v = store->fetchValidValue(vb, queuedItem.getKey(),
                                                        bucket_num, true);

                if (v && v->isDeleted()) {
                    bool deleted = vb->ht.unlocked_del(queuedItem.getKey(),
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
        bool did = store->invokeOnLockedStoredValue(queuedItem.getKey(),
                                                    queuedItem.getVBucketId(),
                                                    &StoredValue::setId,
                                                    id);
        if (!did) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Failed to set id on vb%d ``%s''\n",
                             queuedItem.getVBucketId(), queuedItem.getKey().c_str());
        }
    }

    void redirty() {
        stats->memOverhead.incr(queuedItem.size());
        assert(stats->memOverhead.get() < GIGANTOR);
        ++stats->flushFailed;
        store->invokeOnLockedStoredValue(queuedItem.getKey(),
                                         queuedItem.getVBucketId(),
                                         &StoredValue::reDirty,
                                         dirtied);
        rq->push(queuedItem);
    }

    const QueuedItem queuedItem;
    std::queue<QueuedItem> *rq;
    EventuallyPersistentStore *store;
    rel_time_t queued;
    rel_time_t dirtied;
    EPStats *stats;
    DISALLOW_COPY_AND_ASSIGN(PersistenceCallback);
};

int EventuallyPersistentStore::flushOneDeleteAll() {
    rwUnderlying->reset();
    return 1;
}

// While I actually know whether a delete or set was intended, I'm
// still a bit better off running the older code that figures it out
// based on what's in memory.
int EventuallyPersistentStore::flushOneDelOrSet(QueuedItem &qi,
                                           std::queue<QueuedItem> *rejectQueue) {

    RCPtr<VBucket> vb = getVBucket(qi.getVBucketId());
    if (!vb) {
        return 0;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(qi.getKey(), &bucket_num);
    StoredValue *v = fetchValidValue(vb, qi.getKey(), bucket_num, true);

    size_t itemBytes = v != NULL ? v->getKeyValLength() : 0;
    vb->doStatsForFlushing(qi, itemBytes);

    int64_t rowid = v != NULL ? v->getId() : -1;
    bool found = v != NULL;
    bool deleted = found && v->isDeleted();
    bool isDirty = found && v->isDirty();
    Item *val = NULL;
    rel_time_t queued(qi.getDirtied()), dirtied(0);

    int ret = 0;

    if (isDirty && v->isExpired(ep_real_time() + engine.getItemExpiryWindow())) {
        ++stats.flushExpired;
        v->markClean(&dirtied);
        isDirty = false;
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
            stats.dirtyAgeHisto.add(dirtyAge * 1000000);
            stats.dataAgeHisto.add(dataAge * 1000000);
            stats.dirtyAge.set(dirtyAge);
            stats.dataAge.set(dataAge);
            stats.dirtyAgeHighWat.set(std::max(stats.dirtyAge.get(),
                                               stats.dirtyAgeHighWat.get()));
            stats.dataAgeHighWat.set(std::max(stats.dataAge.get(),
                                              stats.dataAgeHighWat.get()));
            // Copy it for the duration.
            if (!deleted) {
                assert(rowid == v->getId());
                val = new Item(qi.getKey(), v->getFlags(), v->getExptime(),
                               v->getValue(), v->getCas(), rowid,
                               qi.getVBucketId());
            }

            if (rowid == -1) {
                v->setPendingId();
            }
        } else {
            isDirty = false;
            v->reDirty(dirtied);
            rejectQueue->push(qi);
            stats.memOverhead.incr(qi.size());
            assert(stats.memOverhead.get() < GIGANTOR);
            ++vb->opsReject;
        }
    }

    if (isDirty && !deleted) {
        if (qi.getVBucketVersion() != vbuckets.getBucketVersion(qi.getVBucketId())) {
            lh.unlock();
        } else {
            // If a vbucket snapshot task with the high priority is currently scheduled,
            // requeue the persistence task and wait until the snapshot task is completed.
            if (vbuckets.isHighPriorityVbSnapshotScheduled()) {
                lh.unlock();
                uint16_t shard_id = rwUnderlying->getShardId(qi);
                towrite[shard_id].push(qi);
                stats.memOverhead.incr(qi.size());
                assert(stats.memOverhead.get() < GIGANTOR);
                ++stats.totalEnqueued;
                stats.queue_size = getWriteQueueSize();

                vb->doStatsForQueueing(qi, itemBytes);
            } else {
                v->markClean(NULL);
                lh.unlock();
                BlockTimer timer(rowid == -1 ?
                                 &stats.diskInsertHisto : &stats.diskUpdateHisto);
                PersistenceCallback cb(qi, rejectQueue, this, queued, dirtied, &stats);
                rwUnderlying->set(*val, qi.getVBucketVersion(), cb);
                if (rowid == -1)  {
                    ++vb->opsCreate;
                } else {
                    ++vb->opsUpdate;
                }
            }
        }
    } else if (deleted) {
        lh.unlock();
        BlockTimer timer(&stats.diskDelHisto);
        PersistenceCallback cb(qi, rejectQueue, this, queued, dirtied, &stats);
        if (rowid > 0) {
            uint16_t vbid(qi.getVBucketId());
            uint16_t vbver(vbuckets.getBucketVersion(vbid));
            rwUnderlying->del(qi.getKey(), rowid, vbid, vbver, cb);
            ++vb->opsDelete;
        } else {
            // bypass deletion if missing items, but still call the
            // deletion callback for clean cleanup.
            int affected(0);
            cb.callback(affected);
        }
    }

    if (val != NULL) {
        delete val;
    }

    return ret;
}

int EventuallyPersistentStore::flushOne(std::queue<QueuedItem> *q,
                                        std::queue<QueuedItem> *rejectQueue) {

    QueuedItem qi = q->front();
    q->pop();
    stats.memOverhead.decr(qi.size());
    assert(stats.memOverhead.get() < GIGANTOR);

    int rv = 0;
    switch (qi.getOperation()) {
    case queue_op_flush:
        rv = flushOneDeleteAll();
        break;
    case queue_op_set:
        if (qi.getVBucketVersion() == vbuckets.getBucketVersion(qi.getVBucketId())) {
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
    }
    stats.flusher_todo--;

    return rv;

}

void EventuallyPersistentStore::queueDirty(const std::string &key,
                                           uint16_t vbid,
                                           enum queue_operation op,
                                           int64_t obid,
                                           size_t itemBytes) {
    if (doPersistence) {
        QueuedItem item(key, vbid, op, vbuckets.getBucketVersion(vbid), obid);

        uint16_t shard_id = (op == queue_op_flush) ?
                            0 : rwUnderlying->getShardId(item);
        towrite[shard_id].push(item);
        stats.memOverhead.incr(item.size());
        assert(stats.memOverhead.get() < GIGANTOR);
        ++stats.totalEnqueued;
        stats.queue_size = getWriteQueueSize();

        RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
		if (vb) {
            vb->doStatsForQueueing(item, itemBytes);
        }
    }
}

void LoadStorageKVPairCallback::initVBucket(uint16_t vbid, uint16_t vb_version,
                                            vbucket_state_t state) {
    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    if (!vb) {
        vb.reset(new VBucket(vbid, state, stats));
        vbuckets.addBucket(vb);
        vbuckets.setBucketVersion(vbid, vb_version);
    }
    if (vbid == 0 && vbuckets.getBucketVersion(0) != vb_version) {
        vbuckets.setBucketVersion(0, vb_version);
    }
}

void LoadStorageKVPairCallback::callback(GetValue &val) {
    Item *i = val.getValue();
    if (i != NULL) {
        uint16_t vb_version = vbuckets.getBucketVersion(i->getVBucketId());
        if (vb_version != static_cast<uint16_t>(-1) && val.getVBucketVersion() != vb_version) {
            epstore->getInvalidItemDbPager()->addInvalidItem(i, val.getVBucketVersion());
            delete i;
            return;
        }

        RCPtr<VBucket> vb = vbuckets.getBucket(i->getVBucketId());
        if (!vb) {
            vb.reset(new VBucket(i->getVBucketId(), vbucket_state_dead, stats));
            vbuckets.addBucket(vb);
            vbuckets.setBucketVersion(i->getVBucketId(), val.getVBucketVersion());
        }
        bool retain(shouldBeResident());
        bool succeeded(false);

        switch (vb->ht.add(*i, false, retain)) {
        case ADD_SUCCESS:
        case ADD_UNDEL:
            // Yay
            succeeded = true;
            break;
        case ADD_EXISTS:
            // Boo
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warmup dataload error: Duplicate key: %s.\n",
                             i->getKey().c_str());
            ++stats.warmDups;
            succeeded = true;
            break;
        case ADD_NOMEM:
            if (hasPurged) {
                if (++stats.warmOOM == 1) {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Warmup dataload failure: max_size too low.\n");
                }
            } else {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Emergency startup purge to free space for load.\n");
                purge();
                // Try that item again.
                switch(vb->ht.add(*i, false, retain)) {
                case ADD_SUCCESS:
                case ADD_UNDEL:
                    succeeded = true;
                    break;
                case ADD_EXISTS:
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Warmup dataload error: Duplicate key: %s.\n",
                                     i->getKey().c_str());
                    ++stats.warmDups;
                    succeeded = true;
                    break;
                case ADD_NOMEM:
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Cannot store an item after emergency purge.\n");
                    ++stats.warmOOM;
                    break;
                default:
                    abort();
                }
            }
            break;
        default:
            abort();
        }

        if (succeeded && i->isExpired(startTime)) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Item was expired at load:  %s\n",
                             i->getKey().c_str());
            epstore->del(i->getKey(), 0, i->getVBucketId(), NULL, true);
        }

        delete i;
    }
    ++stats.warmedUp;
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
    BlockTimer timer(&stats.diskCommitHisto);
    rel_time_t cstart = ep_current_time();
    while (!underlying->commit()) {
        sleep(1);
        ++stats.commitFailed;
    }
    ++stats.flusherCommits;
    rel_time_t complete_time = ep_current_time();

    stats.commit_time.set(complete_time - cstart);
    stats.cumulativeCommitTime.incr(complete_time - cstart);
    intxn = false;
    syncRegistry.itemsPersisted(uncommittedItems);
    uncommittedItems.clear();
}

void TransactionContext::addUncommittedItem(const QueuedItem &item) {
    uncommittedItems.push_back(item);
}

bool VBCBAdaptor::callback(Dispatcher &, TaskId) {
    RCPtr<VBucket> vb = store->vbuckets.getBucket(currentvb);
    if (vb) {
        if (visitor->visitBucket(vb)) {
            vb->ht.visit(*visitor);
        }
    }
    size_t maxSize = store->vbuckets.getSize();
    assert(currentvb <= std::numeric_limits<uint16_t>::max());
    bool isdone = currentvb >= static_cast<uint16_t>(maxSize);
    ++currentvb;
    if (isdone) {
        visitor->complete();
    }
    return !isdone;
}
