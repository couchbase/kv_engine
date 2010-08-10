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

    rel_time_t (*ep_current_time)() = uninitialized_current_time;
}

class BGFetchCallback : public DispatcherCallback {
public:
    BGFetchCallback(EventuallyPersistentStore *e, SERVER_CORE_API *capi,
                    const std::string &k, uint16_t vbid, uint64_t r,
                    const void *c) :
        ep(e), core(capi), key(k), vbucket(vbid), rowid(r), cookie(c),
        init(gethrtime()), start(0) {
        assert(ep);
        assert(core);
        assert(cookie);
    }

    bool callback(Dispatcher &d, TaskId t) {
        (void)d; (void)t;
        start = gethrtime();
        ep->completeBGFetch(key, vbucket, rowid, cookie, core);
        hrtime_t stop = gethrtime();

        if (stop > start && start > init) {
            // skip the measurement if the counter wrapped...
            ++ep->stats.bgNumOperations;
            hrtime_t w = (start - init) / 1000;
            ep->stats.bgWait += w;
            ep->stats.bgMinWait.setIfLess(w);
            ep->stats.bgMaxWait.setIfBigger(w);

            hrtime_t l = (stop - start) / 1000;
            ep->stats.bgLoad += l;
            ep->stats.bgMinLoad.setIfLess(l);
            ep->stats.bgMaxLoad.setIfBigger(l);
        }

        return false;
    }

private:
    EventuallyPersistentStore *ep;
    SERVER_CORE_API           *core;
    std::string                key;
    uint16_t                   vbucket;
    uint64_t                   rowid;
    const void                *cookie;

    hrtime_t init;
    hrtime_t start;
};

class SetVBStateCallback : public DispatcherCallback {
public:
    SetVBStateCallback(RCPtr<VBucket> vb, vbucket_state_t st, SERVER_CORE_API *c)
        : vbucket(vb), state(st), core(c) {
        assert(core);
    }

    bool callback(Dispatcher &d, TaskId t) {
        (void)d; (void)t;
        vbucket->setState(state, core);
        return false;
    }

private:
    RCPtr<VBucket>   vbucket;
    vbucket_state_t  state;
    SERVER_CORE_API *core;
};

EventuallyPersistentStore::EventuallyPersistentStore(EventuallyPersistentEngine &theEngine,
                                                     StrategicSqlite3 *t,
                                                     bool startVb0) :
    engine(theEngine), stats(engine.getEpStats()),
    loadStorageKVPairCallback(vbuckets, stats), bgFetchDelay(0)
{
    doPersistence = getenv("EP_NO_PERSISTENCE") == NULL;
    dispatcher = new Dispatcher();
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

    // Verify that we don't have any dirty objects!
    if (getenv("EP_VERIFY_SHUTDOWN_FLUSH") != NULL) {
        VerifyStoredVisitor walker;
        // TODO: Something smarter for multiple vbuckets.
        RCPtr<VBucket> vb = getVBucket(0);
        assert(vb);
        vb->ht.visit(walker);
        if (!walker.dirty.empty()) {
            std::vector<std::string>::const_iterator iter;
            for (iter = walker.dirty.begin();
                 iter != walker.dirty.end();
                 ++iter) {
                std::cerr << "ERROR: Object dirty after flushing: "
                          << iter->c_str() << std::endl;
            }

            throw std::runtime_error("Internal error, objects dirty objects exists");
        }
    }

    delete flusher;
    delete dispatcher;
}

void EventuallyPersistentStore::startDispatcher() {
    dispatcher->start();
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

protocol_binary_response_status EventuallyPersistentStore::evictKey(const std::string &key,
                                                                    uint16_t vbucket,
                                                                    const char **msg) {
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!(vb && vb->getState() == active)) {
        return PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET;
    }

    int bucket_num = vb->ht.bucket(key);
    LockHolder lh(vb->ht.getMutex(bucket_num));
    StoredValue *v = vb->ht.unlocked_find(key, bucket_num);

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
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == active) {
        // OK
    } else if(vb->getState() == replica && !force) {
        return ENGINE_NOT_MY_VBUCKET;
    } else if(vb->getState() == pending && !force) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    }

    bool cas_op = (item.getCas() != 0);

    mutation_type_t mtype = vb->ht.set(item);

    if (cas_op && mtype == NOT_FOUND) {
        return ENGINE_KEY_ENOENT;
    } else if (mtype == NOMEM) {
        return ENGINE_ENOMEM;
    } else if (mtype == INVALID_CAS) {
        return ENGINE_KEY_EEXISTS;
    } else if (mtype == IS_LOCKED) {
        return ENGINE_KEY_EEXISTS;
    } else if (mtype == WAS_CLEAN || mtype == NOT_FOUND) {
        queueDirty(item.getKey(), item.getVBucketId(), queue_op_set);
        if (mtype == NOT_FOUND) {
            stats.curr_items++;
        }
    }

    return ENGINE_SUCCESS;
}

RCPtr<VBucket> EventuallyPersistentStore::getVBucket(uint16_t vbucket) {
    return vbuckets.getBucket(vbucket);
}

void EventuallyPersistentStore::setVBucketState(uint16_t vbid,
                                                vbucket_state_t to,
                                                SERVER_CORE_API *core,
                                                bool async) {
    // Lock to prevent a race condition between a failed update and add.
    LockHolder lh(vbsetMutex);
    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    if (vb) {
        if (async) {
            dispatcher->schedule(shared_ptr<DispatcherCallback>(new SetVBStateCallback(vb, to, core)),
                                 NULL, -1);
        } else {
            vb->setState(to, core);
        }
    } else {
        RCPtr<VBucket> newvb(new VBucket(vbid, to, stats));
        vbuckets.addBucket(newvb);
    }
    queueDirty(VBucket::toString(to), vbid, queue_op_vb_set);
}

bool EventuallyPersistentStore::deleteVBucket(uint16_t vbid) {
    // Lock to prevent a race condition between a failed update and add (and delete).
    LockHolder lh(vbsetMutex);
    bool rv(false);

    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    if (vb && vb->getState() == dead) {
        stats.curr_items.decr(vbuckets.removeBucket(vbid));
        queueDirty("", vbid, queue_op_vb_flush);
        rv = true;
    }
    return rv;
}

void EventuallyPersistentStore::completeBGFetch(const std::string &key,
                                                uint16_t vbucket,
                                                uint64_t rowid,
                                                const void *cookie,
                                                SERVER_CORE_API *core) {
    --bgFetchQueue;
    ++stats.bg_fetched;

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
        StoredValue *v = vb->ht.unlocked_find(key, bucket_num);

        if (v) {
            if (v->restoreValue(gcb.val.getValue()->getValue(), stats)) {
                --stats.numNonResident;
            }
        }
    }

    lh.unlock();

    core->notify_io_complete(cookie, gcb.val.getStatus());
    delete gcb.val.getValue();
}

void EventuallyPersistentStore::bgFetch(const std::string &key,
                                        uint16_t vbucket,
                                        uint64_t rowid,
                                        const void *cookie,
                                        SERVER_CORE_API *core) {
    shared_ptr<BGFetchCallback> dcb(new BGFetchCallback(this, core, key,
                                                        vbucket, rowid, cookie));
    ++bgFetchQueue;
    assert(bgFetchQueue > 0);
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                     "Queued a background fetch, now at %zd\n",
                     bgFetchQueue.get());
    dispatcher->schedule(dcb, NULL, -1, bgFetchDelay);
}

GetValue EventuallyPersistentStore::get(const std::string &key,
                                        uint16_t vbucket,
                                        const void *cookie,
                                        SERVER_CORE_API *core) {
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb || vb->getState() == dead) {
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if (vb->getState() == active) {
        // OK
    } else if(vb->getState() == replica) {
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if(vb->getState() == pending) {
        if (vb->addPendingOp(cookie)) {
            return GetValue(NULL, ENGINE_EWOULDBLOCK);
        }
    }

    int bucket_num = vb->ht.bucket(key);
    LockHolder lh(vb->ht.getMutex(bucket_num));
    StoredValue *v = vb->ht.unlocked_find(key, bucket_num);

    if (v) {
        // If the value is not resident, wait for it...
        if (!v->isResident()) {
            bgFetch(key, vbucket, v->getId(), cookie, core);
            return GetValue(NULL, ENGINE_EWOULDBLOCK);
        }

        // return an invalid cas value if the item is locked
        GetValue rv(new Item(v->getKey(), v->getFlags(), v->getExptime(),
                             v->getValue(),
                             v->isLocked(ep_current_time()) ? -1 : v->getCas()));
        lh.unlock();
        return rv;
    } else {
        GetValue rv;
        lh.unlock();
        return rv;
    }
}

bool EventuallyPersistentStore::getLocked(const std::string &key,
                                          uint16_t vbucket,
                                          Callback<GetValue> &cb,
                                          rel_time_t currentTime,
                                          uint32_t lockTimeout) {
    RCPtr<VBucket> vb = getVBucket(vbucket, active);
    if (!vb) {
        GetValue rv(NULL, ENGINE_NOT_MY_VBUCKET);
        cb.callback(rv);
        return false;
    }

    int bucket_num = vb->ht.bucket(key);
    LockHolder lh(vb->ht.getMutex(bucket_num));
    StoredValue *v = vb->ht.unlocked_find(key, bucket_num);

    if (v) {
        if (v->isLocked(currentTime)) {
            GetValue rv;
            cb.callback(rv);
            lh.unlock();
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
    lh.unlock();
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
    StoredValue *v = vb->ht.unlocked_find(key, bucket_num);

    found = (v != NULL);
    if (found) {
        kstats.dirty = v->isDirty();
        kstats.exptime = v->getExptime();
        kstats.flags = v->getFlags();
        kstats.cas = v->getCas();
        // TODO:  Know this somehow.
        kstats.dirtied = 0; // v->getDirtied();
        kstats.data_age = v->getDataAge();
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
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == active) {
        // OK
    } else if(vb->getState() == replica) {
        return ENGINE_NOT_MY_VBUCKET;
    } else if(vb->getState() == pending) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    }

    bool existed = vb->ht.del(key);
    ENGINE_ERROR_CODE rv = existed ? ENGINE_SUCCESS : ENGINE_KEY_ENOENT;

    if (existed) {
        queueDirty(key, vbucket, queue_op_del);
        stats.curr_items--;
    }
    return rv;
}

void EventuallyPersistentStore::reset() {
    std::vector<int> buckets = vbuckets.getBuckets();
    std::vector<int>::iterator it;
    for (it = buckets.begin(); it != buckets.end(); ++it) {
        RCPtr<VBucket> vb = getVBucket(*it, active);
        if (vb) {
            stats.curr_items -= vb->ht.clear();
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
    rel_time_t cstart = ep_current_time();
    while (!underlying->commit()) {
        sleep(1);
        stats.commitFailed++;
    }
    rel_time_t complete_time = ep_current_time();

    stats.commit_time.set(complete_time - cstart);
    return oldest;
}


// This class exists to create a closure around a few variables within
// EventuallyPersistentStore::flushOne so that an object can be
// requeued in case of failure to store in the underlying layer.

class Requeuer : public Callback<std::pair<bool, int64_t> >,
                 public Callback<bool> {
public:

    Requeuer(const QueuedItem &qi, std::queue<QueuedItem> *q,
             StoredValue *v, rel_time_t qd, rel_time_t d, struct EPStats *s) :
        queuedItem(qi), rq(q), sval(v), queued(qd), dirtied(d), stats(s) {
        assert(rq);
        assert(s);
    }

    void callback(std::pair<bool, int64_t> &value) {
        if (value.first && sval != NULL && value.second > 0) {
            sval->setId(value.second);
        } else if (!value.first) {
            stats->flushFailed++;
            if (sval != NULL) {
                sval->reDirty(dirtied);
            }
            rq->push(queuedItem);
        }
    }

    void callback(bool &value) {
        if (!value) {
            stats->flushFailed++;
            if (sval != NULL) {
                sval->reDirty(dirtied);
            }
            rq->push(queuedItem);
        }
    }

private:
    const QueuedItem queuedItem;
    std::queue<QueuedItem> *rq;
    StoredValue *sval;
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
    StoredValue *v = vb->ht.unlocked_find(qi.getKey(), bucket_num);

    bool found = v != NULL;
    bool isDirty = (found && v->isDirty());
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
            val = new Item(qi.getKey(), v->getFlags(), v->getExptime(),
                           v->getValue(), v->getCas(), v->getId(),
                           qi.getVBucketId());

        }
    }
    lh.unlock();

    if (found && isDirty) {
        Requeuer cb(qi, rejectQueue, v, queued, dirtied, &stats);
        underlying->set(*val, cb);
    } else if (!found) {
        Requeuer cb(qi, rejectQueue, v, queued, dirtied, &stats);
        underlying->del(qi.getKey(), qi.getVBucketId(), cb);
    }

    if (val != NULL) {
        delete val;
    }

    return ret;
}

int EventuallyPersistentStore::flushOneDeleteVBucket(QueuedItem &qi,
                                                     std::queue<QueuedItem> *rejectQueue) {
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                     "Deleting vbucket %d from disk\n", qi.getVBucketId());
    if (!underlying->delVBucket(qi.getVBucketId())) {
        rejectQueue->push(qi);
    }
    return 1;
}

int EventuallyPersistentStore::flushVBSet(QueuedItem &qi,
                                          std::queue<QueuedItem> *rejectQueue) {

    if (!underlying->setVBState(qi.getVBucketId(), qi.getKey())) {
        rejectQueue->push(qi);
    }
    return 1;
}

int EventuallyPersistentStore::flushOne(std::queue<QueuedItem> *q,
                                        std::queue<QueuedItem> *rejectQueue) {

    QueuedItem qi = q->front();
    q->pop();
    stats.memOverhead.decr(qi.size());
    stats.flusher_todo--;

    int rv = 0;
    switch (qi.getOperation()) {
    case queue_op_flush:
        rv = flushOneDeleteAll();
        break;
    case queue_op_vb_flush:
        rv = flushOneDeleteVBucket(qi, rejectQueue);
        break;
    case queue_op_set:
        // FALLTHROUGH
    case queue_op_del:
        rv = flushOneDelOrSet(qi, rejectQueue);
        break;
    case queue_op_vb_set:
        rv = flushVBSet(qi, rejectQueue);
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
        stats.totalEnqueued++;
        stats.queue_size = towrite.size();
    }
}
