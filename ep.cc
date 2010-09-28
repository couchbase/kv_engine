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
#include <iostream>
#include <functional>

#include "ep.hh"
#include "flusher.hh"
#include "locks.hh"
#include "dispatcher.hh"
#include "sqlite-kvstore.hh"
#include "ep_engine.h"
#include "item_pager.hh"

extern "C" {
    static rel_time_t uninitialized_current_time(void) {
        abort();
        return 0;
    }

    static time_t default_abs_time(rel_time_t offset) {
        /* This is overridden at init time */
        return time(NULL) - ep_current_time() + offset;
    }

    rel_time_t (*ep_current_time)() = uninitialized_current_time;
    time_t (*ep_abs_time)(rel_time_t) = default_abs_time;

    time_t ep_real_time() {
        return ep_abs_time(ep_current_time());
    }
}

class BGFetchCallback : public DispatcherCallback {
public:
    BGFetchCallback(EventuallyPersistentStore *e,
                    const std::string &k, uint16_t vbid, uint64_t r,
                    const void *c) :
        ep(e), key(k), vbucket(vbid), rowid(r), cookie(c),
        init(gethrtime()), start(0) {
        assert(ep);
        assert(cookie);
    }

    bool callback(Dispatcher &d, TaskId t) {
        (void)d; (void)t;
        start = gethrtime();
        ep->completeBGFetch(key, vbucket, rowid, cookie, init, start);
        return false;
    }

private:
    EventuallyPersistentStore *ep;
    std::string                key;
    uint16_t                   vbucket;
    uint64_t                   rowid;
    const void                *cookie;

    hrtime_t init;
    hrtime_t start;
};

class VKeyStatBGFetchCallback : public DispatcherCallback {
public:
    VKeyStatBGFetchCallback(EventuallyPersistentStore *e,
                            const std::string &k, uint16_t vbid, uint64_t r,
                            const void *c, shared_ptr<Callback<GetValue> > cb) :
        ep(e), key(k), vbucket(vbid), rowid(r), cookie(c),
        lookup_cb(cb) {
        assert(ep);
        assert(cookie);
        assert(lookup_cb);
    }

    bool callback(Dispatcher &d, TaskId t) {
        (void)d; (void)t;
        RememberingCallback<GetValue> gcb;

        --ep->bgFetchQueue;
        ep->getUnderlying()->get(key, rowid, gcb);
        gcb.waitForValue();
        assert(gcb.fired);
        lookup_cb->callback(gcb.val);

        return false;
    }

private:
    EventuallyPersistentStore       *ep;
    std::string                      key;
    uint16_t                         vbucket;
    uint64_t                         rowid;
    const void                      *cookie;
    shared_ptr<Callback<GetValue> >  lookup_cb;
};

class SetVBStateCallback : public DispatcherCallback {
public:
    SetVBStateCallback(EventuallyPersistentStore *e, uint16_t vb,
                       const std::string &k)
        : ep(e), vbid(vb), key(k) { }

    bool callback(Dispatcher &d, TaskId t) {
        (void)d; (void)t;
        ep->completeSetVBState(vbid, key);
        return false;
    }

private:
    EventuallyPersistentStore *ep;
    uint16_t vbid;
    std::string key;
};

class NotifyVBStateChangeCallback : public DispatcherCallback {
public:
    NotifyVBStateChangeCallback(RCPtr<VBucket> vb, SERVER_HANDLE_V1 *a)
        : vbucket(vb), api(a) {
        assert(api);
    }

    bool callback(Dispatcher &d, TaskId t) {
        (void)d; (void)t;
        vbucket->fireAllOps(api);
        return false;
    }

private:
    RCPtr<VBucket>   vbucket;
    SERVER_HANDLE_V1 *api;
};

class VBucketDeletionCallback : public DispatcherCallback {
public:
    VBucketDeletionCallback(EventuallyPersistentStore *e, uint16_t vbid)
        : ep(e), vbucket(vbid) {
        assert(ep);
    }

    bool callback(Dispatcher &d, TaskId t) {
        (void)d; (void)t;
        ep->completeVBucketDeletion(vbucket);
        return false;
    }

private:
    EventuallyPersistentStore *ep;
    uint16_t                   vbucket;

};

EventuallyPersistentStore::EventuallyPersistentStore(EventuallyPersistentEngine &theEngine,
                                                     StrategicSqlite3 *t,
                                                     bool startVb0) :
    engine(theEngine), stats(engine.getEpStats()), bgFetchDelay(0)
{
    doPersistence = getenv("EP_NO_PERSISTENCE") == NULL;
    dispatcher = new Dispatcher();
    nonIODispatcher = new Dispatcher();
    flusher = new Flusher(this, dispatcher);

    stats.memOverhead = sizeof(EventuallyPersistentStore);

    setTxnSize(DEFAULT_TXN_SIZE);

    underlying = t;

    if (startVb0) {
        RCPtr<VBucket> vb(new VBucket(0, active, stats));
        vbuckets.addBucket(vb);
    }

    startDispatcher();
    startFlusher();
    startNonIODispatcher();
    assert(underlying);
}

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
    stopFlusher();
    dispatcher->stop();
    nonIODispatcher->stop();

    delete flusher;
    delete dispatcher;
    delete nonIODispatcher;
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
    if (rv) {
        flusher->wait();
    }
}

bool EventuallyPersistentStore::pauseFlusher() {
    flusher->pause();
    return true;
}

bool EventuallyPersistentStore::resumeFlusher() {
    flusher->resume();
    return true;
}

RCPtr<VBucket> EventuallyPersistentStore::getVBucket(uint16_t vbid,
                                                     vbucket_state_t wanted_state) {
    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    vbucket_state_t found_state(vb ? vb->getState() : dead);
    if (found_state == wanted_state) {
        return vb;
    } else {
        RCPtr<VBucket> rv;
        return rv;
    }
}

class Deleter {
public:
    Deleter(EventuallyPersistentStore *ep) : e(ep) {}
    void operator() (std::pair<uint16_t, std::string> vk) {
        RCPtr<VBucket> vb = e->getVBucket(vk.first);
        if (vb) {
            int bucket_num = vb->ht.bucket(vk.second);
            LockHolder lh(vb->ht.getMutex(bucket_num));

            StoredValue *v = vb->ht.unlocked_find(vk.second, bucket_num);
            if (v) {
                if (vb->ht.unlocked_softDelete(vk.second, bucket_num)) {
                    e->queueDirty(vk.second, vb->getId(), queue_op_del);
                }
            }
        }
    }
private:
    EventuallyPersistentStore *e;
};

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
        if (vb->ht.unlocked_softDelete(key, bucket_num)) {
            queueDirty(key, vb->getId(), queue_op_del);
        }
        return NULL;
    }
    return v;
}

protocol_binary_response_status EventuallyPersistentStore::evictKey(const std::string &key,
                                                                    uint16_t vbucket,
                                                                    const char **msg) {
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!(vb && vb->getState() == active)) {
        return PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET;
    }

    int bucket_num = vb->ht.bucket(key);
    LockHolder lh(vb->ht.getMutex(bucket_num));
    StoredValue *v = fetchValidValue(vb, key, bucket_num);

    protocol_binary_response_status rv(PROTOCOL_BINARY_RESPONSE_SUCCESS);

    if (v) {
        if (v->isResident()) {
            if (v->ejectValue(stats)) {
                ++stats.numValueEjects;
                ++stats.numNonResident;
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
    if (!vb || vb->getState() == dead) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == active) {
        // OK
    } else if(vb->getState() == replica && !force) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if(vb->getState() == pending && !force) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    }

    bool cas_op = (item.getCas() != 0);

    mutation_type_t mtype = vb->ht.set(item, !force);

    if (cas_op && mtype == NOT_FOUND) {
        return ENGINE_KEY_ENOENT;
    } else if (mtype == NOMEM) {
        assert(!force);
        return ENGINE_ENOMEM;
    } else if (mtype == INVALID_CAS) {
        return ENGINE_KEY_EEXISTS;
    } else if (mtype == IS_LOCKED) {
        return ENGINE_KEY_EEXISTS;
    } else if (mtype == WAS_CLEAN || mtype == NOT_FOUND) {
        if (item.isExpired(ep_real_time() + engine.getItemExpiryWindow())) {
            ++stats.flushExpired;
            return ENGINE_SUCCESS;
        }
        queueDirty(item.getKey(), item.getVBucketId(), queue_op_set);
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentStore::add(const Item &item,
                                                 const void *cookie)
{
    RCPtr<VBucket> vb = getVBucket(item.getVBucketId());
    if (!vb || vb->getState() == dead || vb->getState() == replica) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == active) {
        // OK
    } else if(vb->getState() == pending) {
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
        queueDirty(item.getKey(), item.getVBucketId(), queue_op_set);
        /* FALLTHROUGH */
    default:
        return ENGINE_SUCCESS;
    }
}

RCPtr<VBucket> EventuallyPersistentStore::getVBucket(uint16_t vbucket) {
    return vbuckets.getBucket(vbucket);
}

void EventuallyPersistentStore::completeSetVBState(uint16_t vbid, const std::string &key) {
    if (!underlying->setVBState(vbid, key)) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                             "Rescheduling a task to set the state of vbucket %d in disc\n", vbid);
        dispatcher->schedule(shared_ptr<DispatcherCallback>(new SetVBStateCallback(this, vbid, key)),
                             NULL, Priority::SetVBucketPriority, 5, false);
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
                                                                        engine.getServerApi())),
                                  NULL, Priority::NotifyVBStateChangePriority, 0, false);
        dispatcher->schedule(shared_ptr<DispatcherCallback>(new SetVBStateCallback(this, vb,
                                                                                   VBucket::toString(to))),
                             NULL, Priority::SetVBucketPriority, 0, false);
    } else {
        RCPtr<VBucket> newvb(new VBucket(vbid, to, stats));
        vbuckets.addBucket(newvb);
    }
}

void EventuallyPersistentStore::completeVBucketDeletion(uint16_t vbid) {
    LockHolder lh(vbsetMutex);

    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    if (!vb || vb->getState() == dead || vbuckets.isBucketDeletion(vbid)) {
        lh.unlock();
        if (underlying->delVBucket(vbid)) {
            vbuckets.setBucketDeletion(vbid, false);
            ++stats.vbucketDeletions;
        } else {
            ++stats.vbucketDeletionFail;
            getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                             "Rescheduling a task to delete vbucket %d from disk\n", vbid);
            dispatcher->schedule(shared_ptr<DispatcherCallback>(new VBucketDeletionCallback(this, vbid)),
                                 NULL, Priority::VBucketDeletionPriority, 10, false);
        }
    }
}

bool EventuallyPersistentStore::deleteVBucket(uint16_t vbid) {
    // Lock to prevent a race condition between a failed update and add (and delete).
    LockHolder lh(vbsetMutex);
    bool rv(false);

    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    if (vb && vb->getState() == dead) {
        vbuckets.setBucketDeletion(vbid, true);
        HashTableStatVisitor statvis(vbuckets.removeBucket(vbid));
        stats.numNonResident.decr(statvis.numNonResident);
        stats.currentSize.decr(statvis.memSize);
        assert(stats.currentSize.get() < GIGANTOR);
        stats.totalCacheSize.decr(statvis.memSize);
        dispatcher->schedule(shared_ptr<DispatcherCallback>(new VBucketDeletionCallback(this, vbid)),
                             NULL, Priority::VBucketDeletionPriority, 0, false);
        rv = true;
    }
    return rv;
}

void EventuallyPersistentStore::completeBGFetch(const std::string &key,
                                                uint16_t vbucket,
                                                uint64_t rowid,
                                                const void *cookie,
                                                hrtime_t init, hrtime_t start) {
    --bgFetchQueue;
    ++stats.bg_fetched;
    std::stringstream ss;
    ss << "Completed a background fetch, now at " << bgFetchQueue.get()
       << std::endl;
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL, ss.str().c_str());

    // Go find the data
    RememberingCallback<GetValue> gcb;

    underlying->get(key, rowid, gcb);
    gcb.waitForValue();
    assert(gcb.fired);

    // Lock to prevent a race condition between a fetch for restore and delete
    LockHolder lh(vbsetMutex);

    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (vb && vb->getState() == active && gcb.val.getStatus() == ENGINE_SUCCESS) {
        int bucket_num = vb->ht.bucket(key);
        LockHolder vblh(vb->ht.getMutex(bucket_num));
        StoredValue *v = fetchValidValue(vb, key, bucket_num);

        if (v) {
            if (v->restoreValue(gcb.val.getValue()->getValue(), stats)) {
                --stats.numNonResident;
            }
        }
    }

    lh.unlock();

    hrtime_t stop = gethrtime();

    if (stop > start && start > init) {
        // skip the measurement if the counter wrapped...
        ++stats.bgNumOperations;
        hrtime_t w = (start - init) / 1000;
        stats.bgWait += w;
        stats.bgMinWait.setIfLess(w);
        stats.bgMaxWait.setIfBigger(w);

        hrtime_t l = (stop - start) / 1000;
        stats.bgLoad += l;
        stats.bgMinLoad.setIfLess(l);
        stats.bgMaxLoad.setIfBigger(l);
    }

    engine.getServerApi()->cookie->notify_io_complete(cookie, gcb.val.getStatus());
    delete gcb.val.getValue();
}

void EventuallyPersistentStore::bgFetch(const std::string &key,
                                        uint16_t vbucket,
                                        uint64_t rowid,
                                        const void *cookie) {
    shared_ptr<BGFetchCallback> dcb(new BGFetchCallback(this, key,
                                                        vbucket, rowid, cookie));
    ++bgFetchQueue;
    assert(bgFetchQueue > 0);
    std::stringstream ss;
    ss << "Queued a background fetch, now at " << bgFetchQueue.get()
       << std::endl;
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL, ss.str().c_str());
    dispatcher->schedule(dcb, NULL, Priority::BgFetcherPriority, bgFetchDelay);
}

GetValue EventuallyPersistentStore::get(const std::string &key,
                                        uint16_t vbucket,
                                        const void *cookie,
                                        bool queueBG, bool honorStates) {
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if (honorStates && vb->getState() == dead) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if (vb->getState() == active) {
        // OK
    } else if(honorStates && vb->getState() == replica) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if(honorStates && vb->getState() == pending) {
        if (vb->addPendingOp(cookie)) {
            return GetValue(NULL, ENGINE_EWOULDBLOCK);
        }
    }

    int bucket_num = vb->ht.bucket(key);
    LockHolder lh(vb->ht.getMutex(bucket_num));
    StoredValue *v = fetchValidValue(vb, key, bucket_num);

    if (v) {
        // If the value is not resident, wait for it...
        if (!v->isResident()) {
            if (queueBG) {
                bgFetch(key, vbucket, v->getId(), cookie);
            }
            return GetValue(NULL, ENGINE_EWOULDBLOCK, v->getId());
        }

        // return an invalid cas value if the item is locked
        GetValue rv(new Item(v->getKey(), v->getFlags(), v->getExptime(),
                             v->getValue(),
                             v->isLocked(ep_current_time()) ? -1 : v->getCas(),
                             v->getId(), vbucket),
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
    if (!vb || vb->getState() == dead) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == active) {
        // OK
    } else if (vb->getState() == replica) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == pending) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    }

    int bucket_num = vb->ht.bucket(key);
    LockHolder lh(vb->ht.getMutex(bucket_num));
    StoredValue *v = fetchValidValue(vb, key, bucket_num);

    if (v) {
        shared_ptr<VKeyStatBGFetchCallback> dcb(new VKeyStatBGFetchCallback(this, key,
                                                                            vbucket,
                                                                            v->getId(),
                                                                            cookie, cb));
        ++bgFetchQueue;
        assert(bgFetchQueue > 0);
        dispatcher->schedule(dcb, NULL, Priority::VKeyStatBgFetcherPriority, bgFetchDelay);
        return ENGINE_EWOULDBLOCK;
    } else {
        return ENGINE_KEY_ENOENT;
    }
}

bool EventuallyPersistentStore::getLocked(const std::string &key,
                                          uint16_t vbucket,
                                          Callback<GetValue> &cb,
                                          rel_time_t currentTime,
                                          uint32_t lockTimeout) {
    RCPtr<VBucket> vb = getVBucket(vbucket, active);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        GetValue rv(NULL, ENGINE_NOT_MY_VBUCKET);
        cb.callback(rv);
        return false;
    }

    int bucket_num = vb->ht.bucket(key);
    LockHolder lh(vb->ht.getMutex(bucket_num));
    StoredValue *v = fetchValidValue(vb, key, bucket_num);

    if (v) {
        if (v->isLocked(currentTime)) {
            GetValue rv;
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

bool EventuallyPersistentStore::getKeyStats(const std::string &key,
                                            uint16_t vbucket,
                                            struct key_stats &kstats)
{
    RCPtr<VBucket> vb = getVBucket(vbucket, active);
    if (!vb) {
        return false;
    }

    bool found = false;
    int bucket_num = vb->ht.bucket(key);
    LockHolder lh(vb->ht.getMutex(bucket_num));
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
                                                 uint16_t vbucket,
                                                 const void *cookie) {
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb || vb->getState() == dead) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == active) {
        // OK
    } else if(vb->getState() == replica) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if(vb->getState() == pending) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    }

    mutation_type_t delrv = vb->ht.softDelete(key);
    ENGINE_ERROR_CODE rv = delrv == NOT_FOUND ? ENGINE_KEY_ENOENT : ENGINE_SUCCESS;

    if (delrv == WAS_CLEAN) {
        queueDirty(key, vbucket, queue_op_del);
    }
    return rv;
}

void EventuallyPersistentStore::reset() {
    std::vector<int> buckets = vbuckets.getBuckets();
    std::vector<int>::iterator it;
    for (it = buckets.begin(); it != buckets.end(); ++it) {
        RCPtr<VBucket> vb = getVBucket(*it, active);
        if (vb) {
            HashTableStatVisitor statvis;
            vb->ht.visit(statvis);
            stats.numNonResident.decr(statvis.numNonResident);
            stats.currentSize.decr(statvis.memSize);
            assert(stats.currentSize.get() < GIGANTOR);
            stats.totalCacheSize.decr(statvis.memSize);
            vb->ht.clear();
        }
    }

    queueDirty("", 0, queue_op_flush);
}

std::queue<QueuedItem>* EventuallyPersistentStore::beginFlush() {
    std::queue<QueuedItem> *rv(NULL);
    if (towrite.empty() && writing.empty()) {
        stats.dirtyAge = 0;
    } else {
        assert(underlying);
        towrite.getAll(writing);
        stats.flusher_todo.set(writing.size());
        stats.queue_size.set(towrite.size());
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "Flushing %d items with %d still in queue\n",
                         writing.size(), towrite.size());
        rv = &writing;
    }
    return rv;
}

void EventuallyPersistentStore::completeFlush(std::queue<QueuedItem> *rej,
                                              rel_time_t flush_start) {
    // Requeue the rejects.
    stats.queue_size += rej->size();
    while (!rej->empty()) {
        writing.push(rej->front());
        rej->pop();
    }

    stats.queue_size.set(towrite.size() + writing.size());
    rel_time_t complete_time = ep_current_time();
    stats.flushDuration.set(complete_time - flush_start);
    stats.flushDurationHighWat.set(std::max(stats.flushDuration.get(),
                                            stats.flushDurationHighWat.get()));
    stats.cumulativeFlushTime.incr(complete_time - flush_start);
}

int EventuallyPersistentStore::flushSome(std::queue<QueuedItem> *q,
                                         std::queue<QueuedItem> *rejectQueue) {
    int tsz = getTxnSize();
    underlying->begin();
    int oldest = stats.min_data_age;
    for (int i = 0; i < tsz && !q->empty() && bgFetchQueue == 0; i++) {
        int n = flushOne(q, rejectQueue);
        if (n != 0 && n < oldest) {
            oldest = n;
        }
    }
    if (bgFetchQueue > 0) {
        ++stats.flusherPreempts;
    }
    rel_time_t cstart = ep_current_time();
    while (!underlying->commit()) {
        sleep(1);
        stats.commitFailed++;
    }
    ++stats.flusherCommits;
    rel_time_t complete_time = ep_current_time();

    stats.commit_time.set(complete_time - cstart);
    stats.cumulativeCommitTime.incr(complete_time - cstart);
    return oldest;
}

// This class exists to create a closure around a few variables within
// EventuallyPersistentStore::flushOne so that an object can be
// requeued in case of failure to store in the underlying layer.

class Requeuer : public Callback<std::pair<bool, int64_t> >,
                 public Callback<int> {
public:

    Requeuer(const QueuedItem &qi, std::queue<QueuedItem> *q,
             EventuallyPersistentStore *st,
             rel_time_t qd, rel_time_t d, struct EPStats *s) :
        queuedItem(qi), rq(q), store(st), queued(qd), dirtied(d), stats(s) {
        assert(rq);
        assert(s);
    }

    // This callback is invoked for set only.
    //
    // The pair is <success?, objId>.  If successful, we may receive a
    // new object ID for this object and need to tell the object what
    // its ID is (this is how we'll find it on disk later).  We also
    // can succeed *without* receiving a new ID (which is how we'd
    // distinguish an insert from an update if we cared).
    void callback(std::pair<bool, int64_t> &value) {
        if (value.first && value.second > 0) {
            ++stats->newItems;
            setId(value.second);
        } else if (!value.first) {
            redirty();
        }
    }

    // This callback is invoked for deletions only.
    //
    // The boolean indicates whether the underlying storage
    // successfully deleted the item.
    void callback(int &value) {
        if (value > 0) {
            ++stats->delItems;
            // We have succesfully removed an item from the disk, we
            // may now remove it from the hash table.
            RCPtr<VBucket> vb = store->getVBucket(queuedItem.getVBucketId());
            if (vb) {
                int bucket_num = vb->ht.bucket(queuedItem.getKey());
                LockHolder lh(vb->ht.getMutex(bucket_num));
                StoredValue *v = store->fetchValidValue(vb, queuedItem.getKey(),
                                                        bucket_num, true);

                if (v && v->isDeleted()) {
                    bool deleted = vb->ht.unlocked_del(queuedItem.getKey(),
                                                       bucket_num);
                    assert(deleted);
                }
            }
        } else if (value == 0) {
            // This occurs when a deletion was sent for a record that
            // has already been deleted.
        } else {
            redirty();
        }
    }

private:

    void setId(int64_t id) {
        store->invokeOnLockedStoredValue(queuedItem.getKey(),
                                         queuedItem.getVBucketId(),
                                         std::mem_fun(&StoredValue::setId),
                                         id);
    }

    void redirty() {
        stats->memOverhead.incr(queuedItem.size());
        assert(stats->memOverhead.get() < GIGANTOR);
        stats->flushFailed++;
        store->invokeOnLockedStoredValue(queuedItem.getKey(),
                                         queuedItem.getVBucketId(),
                                         std::mem_fun(&StoredValue::reDirty),
                                         dirtied);
        rq->push(queuedItem);
    };

    const QueuedItem queuedItem;
    std::queue<QueuedItem> *rq;
    EventuallyPersistentStore *store;
    rel_time_t queued;
    rel_time_t dirtied;
    struct EPStats *stats;
    DISALLOW_COPY_AND_ASSIGN(Requeuer);
};

int EventuallyPersistentStore::flushOneDeleteAll() {
    underlying->reset();
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

    int bucket_num = vb->ht.bucket(qi.getKey());
    LockHolder lh(vb->ht.getMutex(bucket_num));
    StoredValue *v = fetchValidValue(vb, qi.getKey(), bucket_num, true);

    int64_t rowid = v != NULL ? v->getId() : -1;
    bool found = v != NULL;
    bool deleted = found && v->isDeleted();
    bool isDirty = found && v->isDirty();
    Item *val = NULL;
    rel_time_t queued(qi.getDirtied()), dirtied(0);

    int ret = 0;

    if (isDirty) {
        v->markClean(&dirtied);
        // Calculate stats if this had a positive time.
        rel_time_t now = ep_current_time();
        int dataAge = now - dirtied;
        int dirtyAge = now - queued;
        bool eligible = true;

        if (dirtyAge > stats.queue_age_cap.get()) {
            stats.tooOld++;
        } else if (dataAge < stats.min_data_age.get()) {
            eligible = false;
            // Skip this one.  It's too young.
            ret = stats.min_data_age.get() - dataAge;
            isDirty = false;
            stats.tooYoung++;
            v->reDirty(dirtied);
            rejectQueue->push(qi);
            stats.memOverhead.incr(qi.size());
            assert(stats.memOverhead.get() < GIGANTOR);
        }

        if (eligible) {
            assert(dirtyAge < (86400 * 30));
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

        }
    }

    lh.unlock();

    if (isDirty && !deleted) {
        // If vbucket deletion is currently being flushed, don't flush a set operation,
        // but requeue a set operation to a flusher to avoid duplicate items on disk
        if (vbuckets.isBucketDeletion(qi.getVBucketId())) {
            towrite.push(qi);
            stats.memOverhead.incr(qi.size());
            assert(stats.memOverhead.get() < GIGANTOR);
            stats.totalEnqueued++;
            stats.queue_size = towrite.size();
        } else {
            Requeuer cb(qi, rejectQueue, this, queued, dirtied, &stats);
            underlying->set(*val, cb);
        }
    } else if (deleted) {
        Requeuer cb(qi, rejectQueue, this, queued, dirtied, &stats);
        underlying->del(qi.getKey(), rowid, cb);
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
    stats.flusher_todo--;

    int rv = 0;
    switch (qi.getOperation()) {
    case queue_op_flush:
        rv = flushOneDeleteAll();
        break;
    case queue_op_set:
        // FALLTHROUGH
    case queue_op_del:
        rv = flushOneDelOrSet(qi, rejectQueue);
        break;
    }

    return rv;

}

void EventuallyPersistentStore::queueDirty(const std::string &key, uint16_t vbid,
                                           enum queue_operation op) {
    if (doPersistence) {
        // Assume locked.
        QueuedItem qi(key, vbid, op);
        towrite.push(qi);
        stats.memOverhead.incr(qi.size());
        assert(stats.memOverhead.get() < GIGANTOR);
        stats.totalEnqueued++;
        stats.queue_size = towrite.size();
    }
}

void LoadStorageKVPairCallback::initVBucket(uint16_t vbid,
                                            vbucket_state_t state) {
    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    if (!vb) {
        vb.reset(new VBucket(vbid, state, stats));
        vbuckets.addBucket(vb);
    }
}

void LoadStorageKVPairCallback::callback(GetValue &val) {
    Item *i = val.getValue();
    if (i != NULL) {
        RCPtr<VBucket> vb = vbuckets.getBucket(i->getVBucketId());
        if (!vb) {
            vb.reset(new VBucket(i->getVBucketId(), pending, stats));
            vbuckets.addBucket(vb);
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
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Warmup dataload failure: max_size too low.\n");
                ++stats.warmOOM;
            } else {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Emergency startup purge to free space for load.\n");
                purge();
                // Try that item again.
                if (!vb->ht.add(*i, false, retain)) {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Cannot store an item after emergency purge.\n");
                    ++stats.warmOOM;
                }
            }
            break;
        default:
            abort();
        }

        if (succeeded && !retain) {
            ++stats.numValueEjects;
            ++stats.numNonResident;
        }

        delete i;
    }
    stats.warmedUp++;
}

void LoadStorageKVPairCallback::purge() {

    class EmergencyPurgeVisitor : public HashTableVisitor {
    public:
        EmergencyPurgeVisitor(EPStats &s) : stats(s) {}

        void visit(StoredValue *v) {
            if (v->ejectValue(stats)) {
                ++stats.numValueEjects;
                ++stats.numNonResident;
            }
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
        if (vb) {
            vb->ht.visit(epv);
        }
    }
    hasPurged = true;
}
