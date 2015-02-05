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
#include "bloomfilter.h"
#include "checkpoint.h"
#include "common.h"
#include "stored-value.h"

const size_t MIN_CHK_FLUSH_TIMEOUT = 10; // 10 sec.
const size_t MAX_CHK_FLUSH_TIMEOUT = 30; // 30 sec.
static const int64_t INITIAL_DRIFT = -140737488355328; //lowest possible 48-bit integer

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
class FailoverTable;
class KVShard;

// First bool is true if an item exists in VB DB file.
// second bool is true if the operation is SET (i.e., insert or update).
typedef std::pair<bool, bool> kstat_entry_t;

struct KVStatsCtx{
    KVStatsCtx() : vbucket(std::numeric_limits<uint16_t>::max()),
                   fileSpaceUsed(0), fileSize(0) {}

    uint16_t vbucket;
    size_t fileSpaceUsed;
    size_t fileSize;
    unordered_map<std::string, kstat_entry_t> keyStats;
};

typedef struct KVStatsCtx kvstats_ctx;

/**
 * An individual vbucket.
 */
class VBucket : public RCValue {
public:

    VBucket(int i, vbucket_state_t newState, EPStats &st,
            CheckpointConfig &chkConfig, KVShard *kvshard,
            int64_t lastSeqno, uint64_t lastSnapStart,
            uint64_t lastSnapEnd, FailoverTable *table,
            shared_ptr<Callback<uint16_t> > cb,
            vbucket_state_t initState = vbucket_state_dead,
            uint64_t chkId = 1, uint64_t purgeSeqno = 0,
            uint64_t maxCas = 0, int64_t driftCounter = INITIAL_DRIFT):
        ht(st),
        checkpointManager(st, i, chkConfig, lastSeqno, lastSnapStart,
                          lastSnapEnd, cb, chkId),
        failovers(table),
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
        metaDataDisk(0),
        numExpiredItems(0),
        fileSpaceUsed(0),
        fileSize(0),
        id(i),
        state(newState),
        initialState(initState),
        stats(st),
        purge_seqno(purgeSeqno),
        max_cas(maxCas),
        drift_counter(driftCounter),
        time_sync_enabled(false),
        persisted_snapshot_start(lastSnapStart),
        persisted_snapshot_end(lastSnapEnd),
        numHpChks(0),
        shard(kvshard),
        bFilter(NULL),
        tempFilter(NULL)
    {
        backfill.isBackfillPhase = false;
        pendingOpsStart = 0;
        stats.memOverhead.fetch_add(sizeof(VBucket)
                               + ht.memorySize() + sizeof(CheckpointManager));
        cb_assert(stats.memOverhead.load() < GIGANTOR);
    }

    ~VBucket();

    int64_t getHighSeqno() {
        return checkpointManager.getHighSeqno();
    }

    uint64_t getPurgeSeqno() {
        return purge_seqno;
    }

    void setPurgeSeqno(uint64_t to) {
        purge_seqno = to;
    }

    void setPersistedSnapshot(uint64_t start, uint64_t end) {
        LockHolder lh(snapshotMutex);
        persisted_snapshot_start = start;
        persisted_snapshot_end = end;
    }

    void getPersistedSnapshot(snapshot_range_t& range) {
        LockHolder lh(snapshotMutex);
        range.start = persisted_snapshot_start;
        range.end = persisted_snapshot_end;
    }

    uint64_t getMaxCas() {
        return max_cas;
    }

    bool isTimeSyncEnabled() {
        return time_sync_enabled.load();
    }

    void setMaxCas(uint64_t cas) {
        atomic_setIfBigger(max_cas, cas);
    }

    /**
     * To set drift counter's initial value
     * and to toggle the timeSync between ON/OFF.
     */
    void setDriftCounterState(int64_t initial_drift, uint8_t time_sync) {
        drift_counter = initial_drift;
        time_sync_enabled = time_sync;
    }

    int64_t getDriftCounter() {
        return drift_counter;
    }

    void setDriftCounter(int64_t adjustedTime) {
        // Update drift counter only if timeSync is enabled for
        // the vbucket.
        if (time_sync_enabled) {
            int64_t wallTime = gethrtime();
            if ((wallTime + getDriftCounter()) < adjustedTime) {
                drift_counter = (adjustedTime - wallTime);
            }
        }
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
    void incrMetaDataDisk(Item& qi);
    void decrMetaDataDisk(Item& qi);

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
    bool queueBackfillItem(queued_item& qi, bool genSeqno) {
        LockHolder lh(backfill.mutex);
        if (genSeqno) {
            qi->setBySeqno(checkpointManager.nextBySeqno());
        } else {
            checkpointManager.setBySeqno(qi->getBySeqno());
        }
        backfill.items.push(qi);
        ++stats.diskQueueSize;
        ++stats.totalEnqueued;
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

    void addHighPriorityVBEntry(uint64_t id, const void *cookie,
                                bool isBySeqno);
    void notifyCheckpointPersisted(EventuallyPersistentEngine &e,
                                   uint64_t id, bool isBySeqno);
    void notifyAllPendingConnsFailed(EventuallyPersistentEngine &e);
    size_t getHighPriorityChkSize();
    static size_t getCheckpointFlushTimeout();

    /**
     * BloomFilter operations for vbucket
     */
    void initTempFilter(size_t key_count, double probability);
    void addToFilter(const std::string &key);
    bool maybeKeyExistsInFilter(const std::string &key);
    bool isTempFilterAvailable();
    void addToTempFilter(const std::string &key);
    void swapFilter();
    void clearFilter();
    void setFilterStatus(bfilter_status_t to);
    std::string getFilterStatusString();

    uint64_t nextHLCCas();

    // Applicable only for FULL EVICTION POLICY
    bool isResidentRatioUnderThreshold(float threshold,
                                       item_eviction_policy_t policy);

    void addStats(bool details, ADD_STAT add_stat, const void *c,
                  item_eviction_policy_t policy);

    size_t getNumItems(item_eviction_policy_t policy);

    size_t getNumNonResidentItems(item_eviction_policy_t policy);

    size_t getNumTempItems(void) {
        return ht.getNumTempItems();
    }

    bool decrDirtyQueueSize(size_t decrementBy) {
        size_t oldVal;
        do {
            oldVal = dirtyQueueSize.load();
            if (oldVal < decrementBy) {
                LOG(EXTENSION_LOG_DEBUG,
                    "Cannot decrement dirty queue size of vbucket %d by %lld, "
                    "the current value is %lld\n", id, decrementBy, oldVal);
                return false;
            }
        } while (!dirtyQueueSize.compare_exchange_strong(oldVal, oldVal - decrementBy));
        return true;
    }

    void addPersistenceNotification(shared_ptr<Callback<uint64_t> > cb);
    void notifySeqnoPersisted(uint64_t highseqno);

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

    KVShard *getShard(void) {
        return shard;
    }

    std::queue<queued_item> rejectQueue;
    FailoverTable *failovers;

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
    AtomicValue<size_t>  metaDataDisk;

    AtomicValue<size_t>  numExpiredItems;
    AtomicValue<size_t>  fileSpaceUsed;
    AtomicValue<size_t>  fileSize;

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
    uint64_t                 purge_seqno;
    AtomicValue<uint64_t>    max_cas;
    AtomicValue<int64_t>     drift_counter;
    AtomicValue<bool>        time_sync_enabled;

    Mutex pendingBGFetchesLock;
    vb_bgfetch_queue_t pendingBGFetches;

    Mutex snapshotMutex;
    uint64_t persisted_snapshot_start;
    uint64_t persisted_snapshot_end;

    Mutex hpChksMutex;
    std::list<HighPriorityVBEntry> hpChks;
    volatile size_t numHpChks; // size of list hpChks (to avoid MB-9434)
    KVShard *shard;

    Mutex bfMutex;
    BloomFilter *bFilter;
    BloomFilter *tempFilter;    // Used during compaction.

    /**
     * The following list is to contain pending notifications
     * that need to be alerted whenever the desired sequence
     * numbers have been persisted.
     */
    Mutex persistedNotificationsMutex;
    std::list<shared_ptr<Callback<uint64_t>> > persistedNotifications;

    static size_t chkFlushTimeout;

    DISALLOW_COPY_AND_ASSIGN(VBucket);
};

#endif  // SRC_VBUCKET_H_
