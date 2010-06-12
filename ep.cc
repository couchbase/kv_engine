/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <vector>
#include <time.h>
#include <string.h>
#include <iostream>

#include "ep.hh"
#include "flusher.hh"
#include "locks.hh"
#include "dispatcher.hh"
#include "sqlite-kvstore.hh"

extern "C" {
    static rel_time_t uninitialized_current_time(void) {
        abort();
        return 0;
    }

    rel_time_t (*ep_current_time)() = uninitialized_current_time;
}

class GetCallback : public DispatcherCallback {
public:
    GetCallback(StrategicSqlite3 *kvs, const std::string &k,
                uint16_t vbid, shared_ptr<Callback<GetValue> > cb) :
        store(kvs), key(k), vbucket(vbid), _callback(cb) {}

    bool callback(Dispatcher &d, TaskId t) {
        (void)d; (void)t;
        store->get(key, vbucket, *_callback);
        return false;
    }

private:
    StrategicSqlite3 *store;
    std::string key;
    uint16_t vbucket;
    shared_ptr<Callback<GetValue> > _callback;
};

class SetVBStateCallback : public DispatcherCallback {
public:
    SetVBStateCallback(RCPtr<VBucket> vb, vbucket_state_t st, SERVER_CORE_API *c)
         : vbucket(vb), state(st), core(c) {}

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

EventuallyPersistentStore::EventuallyPersistentStore(StrategicSqlite3 *t,
                                                     bool startVb0) :
    loadStorageKVPairCallback(vbuckets, stats)
{
    stats.min_data_age.set(DEFAULT_MIN_DATA_AGE);
    stats.queue_age_cap.set(DEFAULT_MIN_DATA_AGE_CAP);

    doPersistence = getenv("EP_NO_PERSISTENCE") == NULL;
    dispatcher = new Dispatcher();
    flusher = new Flusher(this, dispatcher);

    setTxnSize(DEFAULT_TXN_SIZE);

    underlying = t;

    if (startVb0) {
        RCPtr<VBucket> vb(new VBucket(0, active));
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
    } else if (mtype == INVALID_CAS) {
        return ENGINE_KEY_EEXISTS;
    } else if (mtype == IS_LOCKED) {
        return ENGINE_KEY_EEXISTS;
    } else if (mtype == WAS_CLEAN || mtype == NOT_FOUND) {
        queueDirty(item.getKey(), item.getVBucketId());
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
                                                SERVER_CORE_API *core) {
    // Lock to prevent a race condition between a failed update and add.
    LockHolder lh(vbsetMutex);
    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    if (vb) {
        dispatcher->schedule(shared_ptr<DispatcherCallback>(new SetVBStateCallback(vb,
                                                                                   to,
                                                                                   core)),
                             NULL, -1);
    } else {
        RCPtr<VBucket> newvb(new VBucket(vbid, to));
        vbuckets.addBucket(newvb);
    }
}

bool EventuallyPersistentStore::deleteVBucket(uint16_t vbid) {
    // Lock to prevent a race condition between a failed update and add (and delete).
    LockHolder lh(vbsetMutex);
    bool rv(false);

    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    if (vb && vb->getState() == dead) {
        vbuckets.removeBucket(vbid);
        rv = true;
    }
    return rv;
}

GetValue EventuallyPersistentStore::get(const std::string &key,
                                        uint16_t vbucket,
                                        const void *cookie) {
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb || vb->getState() == dead) {
        return GetValue(ENGINE_NOT_MY_VBUCKET);
    } else if (vb->getState() == active) {
        // OK
    } else if(vb->getState() == replica) {
        return GetValue(ENGINE_NOT_MY_VBUCKET);
    } else if(vb->getState() == pending) {
        if (vb->addPendingOp(cookie)) {
            return GetValue(ENGINE_EWOULDBLOCK);
        }
    }

    int bucket_num = vb->ht.bucket(key);
    LockHolder lh(vb->ht.getMutex(bucket_num));
    StoredValue *v = vb->ht.unlocked_find(key, bucket_num);

    if (v) {
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
        GetValue rv(ENGINE_NOT_MY_VBUCKET);
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

void EventuallyPersistentStore::getFromUnderlying(const std::string &key,
                                                  uint16_t vbucket,
                                                  shared_ptr<Callback<GetValue> > cb) {

    shared_ptr<GetCallback> dcb(new GetCallback(underlying, key, vbucket, cb));
    dispatcher->schedule(dcb, NULL, -1);
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
        kstats.dirtied = v->getDirtied();
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

void EventuallyPersistentStore::resetStats(void) {
    stats.tooYoung.set(0);
    stats.tooOld.set(0);
    stats.dirtyAge.set(0);
    stats.dirtyAgeHighWat.set(0);
    stats.flushDuration.set(0);
    stats.flushDurationHighWat.set(0);
    stats.commit_time.set(0);
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
        queueDirty(key, vbucket);
        stats.curr_items--;
    }
    return rv;
}

void EventuallyPersistentStore::reset() {
    // TODO: Something smarter for multiple vbuckets.
    RCPtr<VBucket> vb = getVBucket(0, active);
    if (vb) {
        vb->ht.clear();
        queueDirty("", 0);
    }
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
    for (int i = 0; i < tsz && !q->empty(); i++) {
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
                sval->reDirty(queued, dirtied);
            }
            rq->push(queuedItem);
        }
    }

    void callback(bool &value) {
        if (!value) {
            stats->flushFailed++;
            if (sval != NULL) {
                sval->reDirty(queued, dirtied);
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

int EventuallyPersistentStore::flushOne(std::queue<QueuedItem> *q,
                                        std::queue<QueuedItem> *rejectQueue) {

    QueuedItem qi = q->front();
    q->pop();

    // Special case hack:  Flush
    if (qi.getKey().size() == 0) {
        underlying->reset();
        stats.flusher_todo--;
        return 1;
    }

    RCPtr<VBucket> vb = getVBucket(qi.getVBucketId());
    if (!vb) {
        stats.flusher_todo--;
        return 0;
    }

    int bucket_num = vb->ht.bucket(qi.getKey());
    LockHolder lh(vb->ht.getMutex(bucket_num));
    StoredValue *v = vb->ht.unlocked_find(qi.getKey(), bucket_num);

    bool found = v != NULL;
    bool isDirty = (found && v->isDirty());
    Item *val = NULL;
    rel_time_t queued(0), dirtied(0);

    int ret = 0;

    if (isDirty) {
        v->markClean(&queued, &dirtied);
        assert(dirtied > 0);
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
            v->reDirty(queued, dirtied);
            rejectQueue->push(qi);
        }

        if (eligible) {
            assert(dirtyAge < (86400 * 30));
            assert(dataAge <= dirtyAge);
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

            // Consider this persisted as it is our intention, though
            // it may fail and be requeued later.
            stats.totalPersisted++;
        }
    }
    stats.flusher_todo--;
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

void EventuallyPersistentStore::queueDirty(const std::string &key, uint16_t vbid) {
    if (doPersistence) {
        // Assume locked.
        towrite.push(QueuedItem(key, vbid));
        stats.totalEnqueued++;
        stats.queue_size = towrite.size();
    }
}
