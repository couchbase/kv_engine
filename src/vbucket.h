/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include "bloomfilter.h"
#include "checkpoint.h"
#include "ep_types.h"
#include "failover-table.h"
#include "hash_table.h"
#include "hlc.h"
#include "kvstore.h"
#include "monotonic.h"
#include "stored-value.h"
#include "utility.h"

#include <atomic>
#include <queue>

class BgFetcher;

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
 * The following will be used to identify
 * the source of an item's expiration.
 */
enum class ExpireBy { Pager, Compactor, Access };

/* Structure that holds info needed for notification for an item being updated
   in the vbucket */
struct VBNotifyCtx {
    Monotonic<int64_t> bySeqno;
    bool notifyReplication;
    bool notifyFlusher;
};

typedef std::unique_ptr<Callback<uint16_t&, VBNotifyCtx&>> NewSeqnoCallback;

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

/**
 * An individual vbucket.
 */
class VBucket : public RCValue {
public:

    // Identifier for a vBucket
    typedef uint16_t id_type;

    VBucket(id_type i,
            vbucket_state_t newState,
            EPStats& st,
            CheckpointConfig& chkConfig,
            KVShard* kvshard,
            int64_t lastSeqno,
            uint64_t lastSnapStart,
            uint64_t lastSnapEnd,
            std::unique_ptr<FailoverTable> table,
            std::shared_ptr<Callback<id_type>> flusherCb,
            NewSeqnoCallback newSeqnoCb,
            Configuration& config,
            item_eviction_policy_t evictionPolicy,
            vbucket_state_t initState = vbucket_state_dead,
            uint64_t purgeSeqno = 0,
            uint64_t maxCas = 0);

    ~VBucket();

    int64_t getHighSeqno() const {
        return checkpointManager.getHighSeqno();
    }

    size_t getChkMgrMemUsage() {
        return checkpointManager.getMemoryUsage();
    }

    size_t getChkMgrMemUsageOfUnrefCheckpoints() {
        return checkpointManager.getMemoryUsageOfUnrefCheckpoints();
    }

    uint64_t getPurgeSeqno() const {
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

    snapshot_range_t getPersistedSnapshot() const {
        LockHolder lh(snapshotMutex);
        return {persisted_snapshot_start, persisted_snapshot_end};
    }

    uint64_t getMaxCas() const {
        return hlc.getMaxHLC();
    }

    void setMaxCas(uint64_t cas) {
        hlc.setMaxHLC(cas);
    }

    void setMaxCasAndTrackDrift(uint64_t cas) {
        hlc.setMaxHLCAndTrackDrift(cas);
    }

    void forceMaxCas(uint64_t cas) {
        hlc.forceMaxHLC(cas);
    }

    HLC::DriftStats getHLCDriftStats() const {
        return hlc.getDriftStats();
    }

    HLC::DriftExceptions getHLCDriftExceptionCounters() const {
        return hlc.getDriftExceptionCounters();
    }

    void setHLCDriftAheadThreshold(std::chrono::microseconds threshold) {
        hlc.setDriftAheadThreshold(threshold);
    }

    void setHLCDriftBehindThreshold(std::chrono::microseconds threshold) {
        hlc.setDriftBehindThreshold(threshold);
    }

    bool isTakeoverBackedUp() {
        return takeover_backed_up.load();
    }

    void setTakeoverBackedUpState(bool to) {
        bool inverse = !to;
        takeover_backed_up.compare_exchange_strong(inverse, to);
    }

    // States whether the VBucket is in the process of being created
    bool isBucketCreation() const {
        return bucketCreation.load();
    }

    bool setBucketCreation(bool rv) {
        bool inverse = !rv;
        return bucketCreation.compare_exchange_strong(inverse, rv);
    }

    // States whether the VBucket is in the process of being deleted
    bool isBucketDeletion() const {
        return bucketDeletion.load();
    }

    bool setBucketDeletion(bool delBucket) {
        bool inverse = !delBucket;
        return bucketDeletion.compare_exchange_strong(inverse, delBucket);
    }

    // Returns the last persisted sequence number for the VBucket
    uint64_t getPersistenceSeqno() const {
        return persistenceSeqno.load();
    }

    void setPersistenceSeqno(uint64_t seqno) {
        persistenceSeqno.store(seqno);
    }

    id_type getId() const { return id; }
    vbucket_state_t getState(void) const { return state.load(); }
    void setState(vbucket_state_t to);
    RWLock& getStateLock() {return stateLock;}

    vbucket_state_t getInitialState(void) { return initialState; }
    void setInitialState(vbucket_state_t initState) {
        initialState = initState;
    }

    vbucket_state getVBucketState() const;

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

    void doStatsForQueueing(const Item& item, size_t itemBytes);
    void doStatsForFlushing(Item& item, size_t itemBytes);
    void incrMetaDataDisk(Item& qi);
    void decrMetaDataDisk(Item& qi);

    void resetStats();

    // Get age sum in millisecond
    uint64_t getQueueAge() {
        uint64_t currDirtyQueueAge = dirtyQueueAge.load(
                                                    std::memory_order_relaxed);
        rel_time_t currentAge = ep_current_time() * dirtyQueueSize;
        if (currentAge < currDirtyQueueAge) {
            return 0;
        }
        return (currentAge - currDirtyQueueAge) * 1000;
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
    bool queueBackfillItem(queued_item& qi,
                           const GenerateBySeqno generateBySeqno) {
        LockHolder lh(backfill.mutex);
        if (GenerateBySeqno::Yes == generateBySeqno) {
            qi->setBySeqno(checkpointManager.nextBySeqno());
        } else {
            checkpointManager.setBySeqno(qi->getBySeqno());
        }
        backfill.items.push(qi);
        ++stats.diskQueueSize;
        ++stats.totalEnqueued;
        doStatsForQueueing(*qi, qi->size());
        stats.memOverhead->fetch_add(sizeof(queued_item));
        return true;
    }
    void getBackfillItems(std::vector<queued_item> &items) {
        LockHolder lh(backfill.mutex);
        size_t num_items = backfill.items.size();
        while (!backfill.items.empty()) {
            items.push_back(backfill.items.front());
            backfill.items.pop();
        }
        stats.memOverhead->fetch_sub(num_items * sizeof(queued_item));
    }
    bool isBackfillPhase() {
        LockHolder lh(backfill.mutex);
        return backfill.isBackfillPhase;
    }
    void setBackfillPhase(bool backfillPhase) {
        LockHolder lh(backfill.mutex);
        backfill.isBackfillPhase = backfillPhase;
    }

    /*
     * Returns the map of bgfetch items for this vbucket, clearing the
     * pendingBGFetches.
     */
    vb_bgfetch_queue_t getBGFetchItems();

    /* queue a background fetch of the specified item.
     * Returns the number of pending background fetches after
     * adding the specified item.
     **/
    size_t queueBGFetchItem(const DocKey& key,
                            VBucketBGFetchItem *fetch,
                            BgFetcher *bgFetcher);

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
    void notifyOnPersistence(EventuallyPersistentEngine &e,
                             uint64_t id, bool isBySeqno);
    void notifyAllPendingConnsFailed(EventuallyPersistentEngine &e);
    size_t getHighPriorityChkSize();
    static size_t getCheckpointFlushTimeout();

    /**
     * BloomFilter operations for vbucket
     */
    void createFilter(size_t key_count, double probability);
    void initTempFilter(size_t key_count, double probability);
    void addToFilter(const DocKey& key);
    bool maybeKeyExistsInFilter(const DocKey& key);
    bool isTempFilterAvailable();
    void addToTempFilter(const DocKey& key);
    void swapFilter();
    void clearFilter();
    void setFilterStatus(bfilter_status_t to);
    std::string getFilterStatusString();
    size_t getFilterSize();
    size_t getNumOfKeysInFilter();

    uint64_t nextHLCCas() {
        return hlc.nextHLC();
    }

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
                    "Cannot decrement dirty queue size of vbucket %" PRIu16
                    "by %" PRIu64 ", the current value is %" PRIu64 "\n", id,
                    uint64_t(decrementBy), uint64_t(oldVal));
                return false;
            }
        } while (!dirtyQueueSize.compare_exchange_strong(oldVal, oldVal - decrementBy));
        return true;
    }

    void incrRollbackItemCount(uint64_t val) {
        rollbackItemCount.fetch_add(val, std::memory_order_relaxed);
    }

    uint64_t getRollbackItemCount(void) {
        return rollbackItemCount.load(std::memory_order_relaxed);
    }

    // Return the persistence checkpoint ID
    uint64_t getPersistenceCheckpointId() const;

    // Set the persistence checkpoint ID to the given value.
    void setPersistenceCheckpointId(uint64_t checkpointId);

    static const vbucket_state_t ACTIVE;
    static const vbucket_state_t REPLICA;
    static const vbucket_state_t PENDING;
    static const vbucket_state_t DEAD;

    HashTable         ht;
    CheckpointManager checkpointManager;

    // Struct for managing 'backfill' items - Items which have been added by
    // an incoming TAP stream and need to be persisted to disk.
    struct {
        std::mutex mutex;
        std::queue<queued_item> items;
        bool isBackfillPhase;
    } backfill;

    KVShard *getShard(void) {
        return shard;
    }

    /**
     * Queue an item for persistence and replication
     *
     * The caller of this function must hold the lock of the hash table
     * partition that contains the StoredValue being Queued.
     *
     * @param v the dirty item
     * @param pHtLh LockHolder of ht lock that must be released before
     *        calling notify callback
     * @param generateBySeqno request that the seqno is generated by this call
     * @param generateCas request that the CAS is generated by this call
     *
     * @return sequence number generated for the mutation
     */
    uint64_t queueDirty(
            StoredValue& v,
            std::unique_lock<std::mutex>* pHtLh = nullptr,
            const GenerateBySeqno generateBySeqno = GenerateBySeqno::Yes,
            const GenerateCas generateCas = GenerateCas::Yes);

    /**
     * Gets the valid StoredValue for the key and deletes an expired item if
     * desired by the caller. Requires the hash bucket to be locked
     *
     * @param lh Reference to the hash bucket lock
     * @param key
     * @param bucket_num Hash bucket number
     * @param wantsDeleted
     * @param trackReference
     * @param queueExpired Delete an expired item
     */
    StoredValue* fetchValidValue(std::unique_lock<std::mutex>& lh,
                                 const DocKey& key,
                                 int bucket_num,
                                 bool wantsDeleted = false,
                                 bool trackReference = true,
                                 bool queueExpired = true);

    /**
     * Increase the expiration count global stats and in the vbucket stats
     */
    void incExpirationStat(ExpireBy source);

    /**
     * Enqueue a background fetch for a key.
     *
     * @param key the key to be bg fetched
     * @param cookie the cookie of the requestor
     * @param engine Reference to ep engine
     * @param bgFetchDelay
     * @param isMeta whether the fetch is for a non-resident value or metadata
     *               of a (possibly) deleted item
     */
    void bgFetch(const DocKey& key,
                 const void* cookie,
                 EventuallyPersistentEngine& engine,
                 int bgFetchDelay,
                 bool isMeta = false);

    /**
     * Complete the background fetch for the specified item. Depending on the
     * state of the item, restore it to the hashtable as appropriate,
     * potentially queuing it as dirty.
     *
     * @param key The key of the item
     * @param fetched_item The item which has been fetched.
     * @param startTime The time processing of the batch of items started.
     *
     * @return ENGINE_ERROR_CODE status notified to be to the front end
     */
    ENGINE_ERROR_CODE completeBGFetchForSingleItem(
            const DocKey& key,
            const VBucketBGFetchItem& fetched_item,
            const hrtime_t startTime);

    /**
     * Retrieve an item from the disk for vkey stats
     *
     * @param key the key to fetch
     * @param cookie the connection cookie
     * @param eviction_policy The eviction policy
     * @param engine Reference to ep engine
     * @param bgFetchDelay
     *
     * @return VBReturnCtx indicates notifyCtx and operation result
     */
    ENGINE_ERROR_CODE statsVKey(const DocKey& key,
                                const void* cookie,
                                EventuallyPersistentEngine& engine,
                                int bgFetchDelay);

    /**
     * Complete the vkey stats for an item background fetched from disk.
     *
     * @param key The key of the item
     * @param gcb Bgfetch cbk obj containing the item from disk
     *
     */
    void completeStatsVKey(const DocKey& key,
                           RememberingCallback<GetValue>& gcb);

    std::queue<queued_item> rejectQueue;
    std::unique_ptr<FailoverTable> failovers;

    std::atomic<size_t>  opsCreate;
    std::atomic<size_t>  opsUpdate;
    std::atomic<size_t>  opsDelete;
    std::atomic<size_t>  opsReject;

    std::atomic<size_t>  dirtyQueueSize;
    std::atomic<size_t>  dirtyQueueMem;
    std::atomic<size_t>  dirtyQueueFill;
    std::atomic<size_t>  dirtyQueueDrain;
    std::atomic<uint64_t> dirtyQueueAge;
    std::atomic<size_t>  dirtyQueuePendingWrites;
    std::atomic<size_t>  metaDataDisk;

    std::atomic<size_t>  numExpiredItems;

private:
    template <typename T>
    void addStat(const char *nm, const T &val, ADD_STAT add_stat, const void *c);

    void fireAllOps(EventuallyPersistentEngine &engine, ENGINE_ERROR_CODE code);

    void adjustCheckpointFlushTimeout(size_t wall_time);

    void decrDirtyQueueMem(size_t decrementBy);

    void decrDirtyQueueAge(uint32_t decrementBy);

    void decrDirtyQueuePendingWrites(size_t decrementBy);

    /**
     * Helper function to update stats after completion of a background fetch
     * for either the value of metadata of a key.
     *
     * @param init the time of epstore's initialization
     * @param start the time when the background fetch was started
     * @param stop the time when the background fetch completed
     */
    void updateBGStats(const hrtime_t init,
                       const hrtime_t start,
                       const hrtime_t stop);

    id_type                         id;
    std::atomic<vbucket_state_t>    state;
    RWLock                          stateLock;
    vbucket_state_t                 initialState;
    std::mutex                           pendingOpLock;
    std::vector<const void*>        pendingOps;
    hrtime_t                        pendingOpsStart;
    EPStats                        &stats;
    uint64_t                        purge_seqno;
    std::atomic<bool>               takeover_backed_up;

    std::mutex pendingBGFetchesLock;
    vb_bgfetch_queue_t pendingBGFetches;

    /* snapshotMutex is used to update/read the pair {start, end} atomically,
       but not if reading a single field. */
    mutable std::mutex snapshotMutex;
    uint64_t persisted_snapshot_start;
    uint64_t persisted_snapshot_end;

    std::mutex hpChksMutex;
    std::list<HighPriorityVBEntry> hpChks;
    std::atomic<size_t> numHpChks; // size of list hpChks (to avoid MB-9434)
    KVShard *shard;

    std::mutex bfMutex;
    BloomFilter *bFilter;
    BloomFilter *tempFilter;    // Used during compaction.

    std::atomic<uint64_t> rollbackItemCount;

    HLC hlc;
    std::string statPrefix;
    // The persistence checkpoint ID for this vbucket.
    std::atomic<uint64_t> persistenceCheckpointId;
    // Flag to indicate the bucket is being created
    std::atomic<bool> bucketCreation;
    // Flag to indicate the bucket is being deleted
    std::atomic<bool> bucketDeletion;
    std::atomic<uint64_t> persistenceSeqno;

    // A callback to be called when a new seqno is generated in the vbucket as
    // a result of a front end call
    NewSeqnoCallback newSeqnoCb;

    // This member holds the eviction policy used
    const item_eviction_policy_t eviction;

    const bool multiBGFetchEnabled;

    static std::atomic<size_t> chkFlushTimeout;

    DISALLOW_COPY_AND_ASSIGN(VBucket);
};

#endif  // SRC_VBUCKET_H_
