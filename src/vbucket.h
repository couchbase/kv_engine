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

#ifndef SRC_VBUCKET_H_
#define SRC_VBUCKET_H_ 1

#include "config.h"

#include <list>
#include <queue>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "atomic.h"
#include "bgfetcher.h"
#include "checkpoint.h"
#include "common.h"
#include "stored-value.h"
#include "failover-table.h"

const size_t MIN_CHK_FLUSH_TIMEOUT = 10; // 10 sec.
const size_t MAX_CHK_FLUSH_TIMEOUT = 30; // 30 sec.

struct HighPriorityVBEntry {
    HighPriorityVBEntry() :
        cookie(NULL), id(0), start(gethrtime()), isBySeqno_(false) { }
    HighPriorityVBEntry(const void *c, uint64_t idNum, bool isBySeqno) :
        cookie(c), id(idNum), start(gethrtime()), isBySeqno_(isBySeqno) { }

    const void *cookie;
    uint64_t id;
    hrtime_t start;
    bool isBySeqno_;
};

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
class KVShard;

struct KVStatsCtx{
    KVStatsCtx() : vbucket(std::numeric_limits<uint16_t>::max()),
                   fileSpaceUsed(0), fileSize(0),
                   numCreates(0), numUpdates(0) {}

    uint16_t vbucket;
    size_t fileSpaceUsed;
    size_t fileSize;
    size_t numCreates;
    size_t numUpdates;
};

typedef struct KVStatsCtx kvstats_ctx;

/**
 * An individual vbucket.
 */
class VBucket : public RCValue {
public:

    VBucket(int i, vbucket_state_t newState, EPStats &st,
            CheckpointConfig &chkConfig, KVShard *kvshard,
            int64_t lastSeqno, vbucket_state_t initState = vbucket_state_dead,
            uint64_t chkId = 1) :
        ht(st),
        checkpointManager(st, i, chkConfig, lastSeqno, chkId),
        failovers(25),
        opsCreate(0),
        opsUpdate(0),
        opsDelete(0),
        opsReject(0),
        dirtyQueueSize(0),
        dirtyQueueMem(0),
        dirtyQueueFill(0),
        dirtyQueueDrain(0),
        dirtyQueueAge(0),
        dirtyQueuePendingWrites(0),
        numExpiredItems(0),
        fileSpaceUsed(0),
        fileSize(0),
        id(i),
        state(newState),
        initialState(initState),
        stats(st), numHpChks(0),
        shard(kvshard)
    {
        backfill.isBackfillPhase = false;
        pendingOpsStart = 0;
        stats.memOverhead.fetch_add(sizeof(VBucket)
                               + ht.memorySize() + sizeof(CheckpointManager));
        assert(stats.memOverhead.load() < GIGANTOR);
        // Pre-fill the failover log with an intial entry.
        // This will be overwritten with the disk state except for on initial creation.
        failovers.createEntry(failovers.generateId(), lastSeqno);
    }

    ~VBucket() {
        if (!pendingOps.empty() || !pendingBGFetches.empty()) {
            LOG(EXTENSION_LOG_WARNING,
                "Have %ld pending ops and %ld pending reads "
                "while destroying vbucket\n",
                pendingOps.size(), pendingBGFetches.size());
        }

        stats.decrDiskQueueSize(dirtyQueueSize.load());

        size_t num_pending_fetches = 0;
        vb_bgfetch_queue_t::iterator itr = pendingBGFetches.begin();
        for (; itr != pendingBGFetches.end(); ++itr) {
            std::list<VBucketBGFetchItem *> &bgitems = itr->second;
            std::list<VBucketBGFetchItem *>::iterator vit = bgitems.begin();
            for (; vit != bgitems.end(); ++vit) {
                delete (*vit);
                ++num_pending_fetches;
            }
        }
        stats.numRemainingBgJobs.fetch_sub(num_pending_fetches);
        pendingBGFetches.clear();

        stats.memOverhead.fetch_sub(sizeof(VBucket) + ht.memorySize() + sizeof(CheckpointManager));
        assert(stats.memOverhead.load() < GIGANTOR);

        LOG(EXTENSION_LOG_INFO, "Destroying vbucket %d\n", id);
    }

    int64_t getHighSeqno() {
        return checkpointManager.getHighSeqno();
    }

    uint64_t getUUID() {
        return failovers.table.front().vb_uuid;
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

    void doStatsForQueueing(Item& item, size_t itemBytes);
    void doStatsForFlushing(Item& item, size_t itemBytes);
    void resetStats();

    // Get age sum in millisecond
    uint64_t getQueueAge() {
        rel_time_t currentAge = ep_current_time() * dirtyQueueSize;
        if (currentAge < dirtyQueueAge) {
            return 0;
        }
        return (currentAge - dirtyQueueAge) * 1000;
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
    bool queueBackfillItem(queued_item& qi) {
        LockHolder lh(backfill.mutex);
        qi->setBySeqno(checkpointManager.nextBySeqno());
        backfill.items.push(qi);
        ++stats.diskQueueSize;
        doStatsForQueueing(*qi, qi->size());
        stats.memOverhead.fetch_add(sizeof(queued_item));
        return true;
    }
    void getBackfillItems(std::vector<queued_item> &items) {
        LockHolder lh(backfill.mutex);
        size_t num_items = backfill.items.size();
        while (!backfill.items.empty()) {
            items.push_back(backfill.items.front());
            backfill.items.pop();
        }
        stats.memOverhead.fetch_sub(num_items * sizeof(queued_item));
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
    void queueBGFetchItem(const std::string &key, VBucketBGFetchItem *fetch,
                          BgFetcher *bgFetcher);
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

    void addHighPriorityVBEntry(uint64_t id, const void *cookie, bool isBySeqno);
    void notifyCheckpointPersisted(EventuallyPersistentEngine &e, uint64_t id, bool isBySeqno);
    void notifyAllPendingConnsFailed(EventuallyPersistentEngine &e);
    size_t getHighPriorityChkSize();
    static size_t getCheckpointFlushTimeout();

    void addStats(bool details, ADD_STAT add_stat, const void *c,
                  item_eviction_policy_t policy);

    size_t getNumItems(item_eviction_policy_t policy);

    size_t getNumNonResidentItems(item_eviction_policy_t policy);

    size_t getNumTempItems(void) {
        return ht.getNumTempItems();
    }

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

    std::queue<queued_item> rejectQueue;
    FailoverTable failovers;

    AtomicValue<size_t>  opsCreate;
    AtomicValue<size_t>  opsUpdate;
    AtomicValue<size_t>  opsDelete;
    AtomicValue<size_t>  opsReject;

    AtomicValue<size_t>  dirtyQueueSize;
    AtomicValue<size_t>  dirtyQueueMem;
    AtomicValue<size_t>  dirtyQueueFill;
    AtomicValue<size_t>  dirtyQueueDrain;
    AtomicValue<uint64_t> dirtyQueueAge;
    AtomicValue<size_t>  dirtyQueuePendingWrites;

    AtomicValue<size_t>  numExpiredItems;
    volatile size_t  fileSpaceUsed;
    volatile size_t  fileSize;

private:
    template <typename T>
    void addStat(const char *nm, const T &val, ADD_STAT add_stat, const void *c);

    void fireAllOps(EventuallyPersistentEngine &engine, ENGINE_ERROR_CODE code);

    void adjustCheckpointFlushTimeout(size_t wall_time);

    int                      id;
    AtomicValue<vbucket_state_t>  state;
    vbucket_state_t          initialState;
    Mutex                    pendingOpLock;
    std::vector<const void*> pendingOps;
    hrtime_t                 pendingOpsStart;
    EPStats                 &stats;

    Mutex pendingBGFetchesLock;
    vb_bgfetch_queue_t pendingBGFetches;

    Mutex hpChksMutex;
    std::list<HighPriorityVBEntry> hpChks;
    volatile size_t numHpChks; // size of list hpChks (to avoid MB-9434)
    KVShard *shard;

    static size_t chkFlushTimeout;

    DISALLOW_COPY_AND_ASSIGN(VBucket);
};

#endif  // SRC_VBUCKET_H_
