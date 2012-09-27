/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef BGFETCHER_HH
#define BGFETCHER_HH 1

#include <map>
#include <vector>
#include <list>

#include "common.hh"
#include "dispatcher.hh"

const uint16_t MAX_BGFETCH_RETRY=5;

class VBucketBGFetchItem {
public:
    VBucketBGFetchItem(const std::string &k, uint64_t s, const void *c) :
                       key(k), cookie(c), retryCount(0), initTime(gethrtime()) {
        value.setId(s);
    }
    ~VBucketBGFetchItem() {}

    void delValue() {
        delete value.getValue();
        value.setValue(NULL);
    }
    bool canRetry() {
        return retryCount < MAX_BGFETCH_RETRY;
    }
    void incrRetryCount() {
        ++retryCount;
    }
    uint16_t getRetryCount() {
        return retryCount;
    }

    const std::string key;
    const void * cookie;
    GetValue value;
    uint16_t retryCount;
    hrtime_t initTime;
};

typedef unordered_map<uint64_t, std::list<VBucketBGFetchItem *> > vb_bgfetch_queue_t;

// Forward declaration.
class EventuallyPersistentStore;
class BgFetcher;

/**
 * A DispatcherCallback for BgFetcher
 */
class BgFetcherCallback : public DispatcherCallback {
public:
    BgFetcherCallback(BgFetcher *b) : bgfetcher(b) { }

    bool callback(Dispatcher &d, TaskId &t);
    std::string description() {
        return std::string("Batching background fetch.");
    }

private:
    BgFetcher *bgfetcher;
};

/**
 * Dispatcher job responsible for batching data reads and push to
 * underlying storage
 */
class BgFetcher {
public:
    static const double sleepInterval;
    /**
     * Construct a BgFetcher task.
     *
     * @param s the store
     * @param d the dispatcher
     */
    BgFetcher(EventuallyPersistentStore *s, Dispatcher *d, EPStats &st) :
        store(s), dispatcher(d), stats(st) {}

    void start(void);
    void stop(void);
    bool run(TaskId &tid);
    bool pendingJob(void);

    void notifyBGEvent(void) {
        if (++stats.numRemainingBgJobs == 1) {
            LockHolder lh(taskMutex);
            assert(task.get());
            dispatcher->wake(task);
        }
    }

private:
    void doFetch(uint16_t vbId);
    void clearItems(uint16_t vbId);

    EventuallyPersistentStore *store;
    Dispatcher *dispatcher;
    vb_bgfetch_queue_t items2fetch;
    TaskId task;
    Mutex taskMutex;
    EPStats &stats;
};

#endif /* BGFETCHER_HH */
