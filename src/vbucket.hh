/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#ifndef VBUCKET_HH
#define VBUCKET_HH 1

#include <cassert>

#include <map>
#include <vector>
#include <set>
#include <sstream>
#include <algorithm>

#include <memcached/engine.h>
#include "queueditem.hh"
#include "common.hh"
#include "atomic.hh"
#include "stored-value.hh"
#include "checkpoint.hh"

const size_t BASE_VBUCKET_SIZE=1024;

/**
 * Function object that returns true if the given vbucket is acceptable.
 */
class VBucketFilter {
public:

    /**
     * Instiatiate a VBucketFilter that always returns true.
     */
    explicit VBucketFilter() : acceptable() {}

    /**
     * Instantiate a VBucketFilter that returns true for any of the
     * given vbucket IDs.
     */
    explicit VBucketFilter(const std::vector<uint16_t> &a) :
        acceptable(a.begin(), a.end()) {}

    explicit VBucketFilter(const std::set<uint16_t> &s) : acceptable(s) {}

    void assign(const std::set<uint16_t> &a) {
        acceptable = a;
    }

    bool operator ()(uint16_t v) const {
        return acceptable.empty() || acceptable.find(v) != acceptable.end();
    }

    size_t size() const { return acceptable.size(); }

    bool empty() const { return acceptable.empty(); }

    void reset() {
        acceptable.clear();
    }

    /**
     * Calculate the difference between this and another filter.
     * If "this" contains elements, [1,2,3,4] and other contains [3,4,5,6]
     * the returned filter contains: [1,2,5,6]
     * @param other the other filter to compare with
     * @return a new filter with the elements present in only one of the two
     *         filters.
     */
    VBucketFilter filter_diff(const VBucketFilter &other) const;

    /**
     * Calculate the intersection between this and another filter.
     * If "this" contains elements, [1,2,3,4] and other contains [3,4,5,6]
     * the returned filter contains: [3,4]
     * @param other the other filter to compare with
     * @return a new filter with the elements present in both of the two
     *         filters.
     */
    VBucketFilter filter_intersection(const VBucketFilter &other) const;

    const std::set<uint16_t> &getVBSet() const { return acceptable; }

    bool addVBucket(uint16_t vbucket) {
        std::pair<std::set<uint16_t>::iterator, bool> rv = acceptable.insert(vbucket);
        return rv.second;
    }

    void removeVBucket(uint16_t vbucket) {
        acceptable.erase(vbucket);
    }

    /**
     * Dump the filter in a human readable form ( "{ bucket, bucket, bucket }"
     * to the specified output stream.
     */
    friend std::ostream& operator<< (std::ostream& out,
                                     const VBucketFilter &filter);

private:

    std::set<uint16_t> acceptable;
};

class EventuallyPersistentEngine;
class BgFetcher;

class VBucketBGFetchItem {
public:
    VBucketBGFetchItem(const std::string k, uint64_t s, const void *c) :
                       key(k), cookie(c), initTime(gethrtime()) {
        value.setId(s);
    }
    ~VBucketBGFetchItem() {
        delete value.getValue();
    }

    const std::string key;
    const void * cookie;
    GetValue value;
    hrtime_t initTime;
};

typedef unordered_map<uint64_t, std::list<VBucketBGFetchItem *> > vb_bgfetch_queue_t;

/**
 * An individual vbucket.
 */
class VBucket : public RCValue {
public:

    VBucket(int i, vbucket_state_t newState, EPStats &st, CheckpointConfig &checkpointConfig,
            vbucket_state_t initState = vbucket_state_dead, uint64_t checkpointId = 1) :
        ht(st), checkpointManager(st, i, checkpointConfig, checkpointId), id(i), state(newState),
        initialState(initState), stats(st) {

        backfill.isBackfillPhase = false;
        pendingOpsStart = 0;
        stats.memOverhead.incr(sizeof(VBucket)
                               + ht.memorySize() + sizeof(CheckpointManager));
        assert(stats.memOverhead.get() < GIGANTOR);
    }

    ~VBucket() {
        if (!pendingOps.empty() || !pendingBGFetches.empty()) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Have %ld pending ops and %ld pending reads "
                             "while destroying vbucket\n",
                             pendingOps.size(), pendingBGFetches.size());
        }

        stats.numRemainingBgJobs.decr(pendingBGFetches.size());
        while(!pendingBGFetches.empty()) {
            delete pendingBGFetches.front();
            pendingBGFetches.pop();
        }
        stats.memOverhead.decr(sizeof(VBucket) + ht.memorySize() + sizeof(CheckpointManager));
        assert(stats.memOverhead.get() < GIGANTOR);
        getLogger()->log(EXTENSION_LOG_INFO, NULL,
                         "Destroying vbucket %d\n", id);
    }

    int getId(void) const { return id; }
    vbucket_state_t getState(void) const { return state; }
    void setState(vbucket_state_t to, SERVER_HANDLE_V1 *sapi);

    vbucket_state_t getInitialState(void) { return initialState; }
    void setInitialState(vbucket_state_t initState) {
        initialState = initState;
    }

    bool addPendingOp(const void *cookie) {
        LockHolder lh(pendingOpLock);
        if (state != vbucket_state_pending) {
            // State transitioned while we were waiting.
            return false;
        }
        // Start a timer when enqueuing the first client.
        if (pendingOps.empty()) {
            pendingOpsStart = gethrtime();
        }
        pendingOps.push_back(cookie);
        ++stats.pendingOps;
        ++stats.pendingOpsTotal;
        return true;
    }

    void doStatsForQueueing(QueuedItem& item, size_t itemBytes);
    void doStatsForFlushing(QueuedItem& item, size_t itemBytes);
    void resetStats();

    // Get age sum in millisecond
    uint64_t getQueueAge() {
        return (ep_current_time() * dirtyQueueSize - dirtyQueueAge) * 1000;
    }

    void fireAllOps(EventuallyPersistentEngine &engine);

    size_t size(void) {
        HashTableDepthStatVisitor v;
        ht.visitDepth(v);
        return v.size;
    }

    size_t getBackfillSize() {
        LockHolder lh(backfill.mutex);
        return backfill.items.size();
    }
    bool queueBackfillItem(const queued_item &qi) {
        LockHolder lh(backfill.mutex);
        backfill.items.push(qi);
        stats.memOverhead.incr(sizeof(queued_item));
        return true;
    }
    void getBackfillItems(std::vector<queued_item> &items) {
        LockHolder lh(backfill.mutex);
        size_t num_items = backfill.items.size();
        while (!backfill.items.empty()) {
            items.push_back(backfill.items.front());
            backfill.items.pop();
        }
        stats.memOverhead.decr(num_items * sizeof(queued_item));
    }
    bool isBackfillPhase() {
        LockHolder lh(backfill.mutex);
        return backfill.isBackfillPhase;
    }
    void setBackfillPhase(bool backfillPhase) {
        LockHolder lh(backfill.mutex);
        backfill.isBackfillPhase = backfillPhase;
    }

    bool getBGFetchItems(vb_bgfetch_queue_t &fetches);
    void queueBGFetchItem(VBucketBGFetchItem *fetch, BgFetcher *bgFetcher);
    size_t numPendingBGFetchItems(void) {
        // do a dirty read of number of fetch items
        return pendingBGFetches.size();
    }
    bool hasPendingBGFetchItems(void) {
        LockHolder lh(pendingBGFetchesLock);
        return !pendingBGFetches.empty();
    }

    static const char* toString(vbucket_state_t s) {
        switch(s) {
        case vbucket_state_active: return "active"; break;
        case vbucket_state_replica: return "replica"; break;
        case vbucket_state_pending: return "pending"; break;
        case vbucket_state_dead: return "dead"; break;
        }
        return "unknown";
    }

    static vbucket_state_t fromString(const char* state) {
        if (strcmp(state, "active") == 0) {
            return vbucket_state_active;
        } else if (strcmp(state, "replica") == 0) {
            return vbucket_state_replica;
        } else if (strcmp(state, "pending") == 0) {
            return vbucket_state_pending;
        } else {
            return vbucket_state_dead;
        }
    }

    void addStats(bool details, ADD_STAT add_stat, const void *c);

    static const vbucket_state_t ACTIVE;
    static const vbucket_state_t REPLICA;
    static const vbucket_state_t PENDING;
    static const vbucket_state_t DEAD;

    HashTable         ht;
    CheckpointManager checkpointManager;
    struct {
        Mutex mutex;
        std::queue<queued_item> items;
        bool isBackfillPhase;
    } backfill;

    Atomic<size_t>  opsCreate;
    Atomic<size_t>  opsUpdate;
    Atomic<size_t>  opsDelete;
    Atomic<size_t>  opsReject;

    Atomic<size_t>  dirtyQueueSize;
    Atomic<size_t>  dirtyQueueMem;
    Atomic<size_t>  dirtyQueueFill;
    Atomic<size_t>  dirtyQueueDrain;
    Atomic<uint64_t> dirtyQueueAge;
    Atomic<size_t>  dirtyQueuePendingWrites;

    Atomic<size_t>  numExpiredItems;

private:
    template <typename T>
    void addStat(const char *nm, T val, ADD_STAT add_stat, const void *c);

    void fireAllOps(EventuallyPersistentEngine &engine, ENGINE_ERROR_CODE code);

    int                      id;
    Atomic<vbucket_state_t>  state;
    vbucket_state_t          initialState;
    Mutex                    pendingOpLock;
    std::vector<const void*> pendingOps;
    hrtime_t                 pendingOpsStart;
    EPStats                 &stats;

    Mutex pendingBGFetchesLock;
    std::queue<VBucketBGFetchItem *> pendingBGFetches;

    DISALLOW_COPY_AND_ASSIGN(VBucket);
};

#endif /* VBUCKET_HH */
