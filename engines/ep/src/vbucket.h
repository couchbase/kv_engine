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

#pragma once

#include "config.h"

#include "bloomfilter.h"
#include "checkpoint_config.h"
#include "collections/vbucket_manifest.h"
#include "dcp/dcp-types.h"
#include "hash_table.h"
#include "hlc.h"
#include "item_pager.h"
#include "kvstore.h"
#include "monotonic.h"

#include <memcached/engine.h>
#include <platform/atomic_duration.h>
#include <platform/non_negative_counter.h>
#include <relaxed_atomic.h>
#include <atomic>
#include <queue>

class EPStats;
class CheckpointManager;
class ConflictResolution;
class Configuration;
class ItemMetaData;
class PreLinkDocumentContext;
class EventuallyPersistentEngine;
class DCPBackfill;
class RollbackResult;
class VBucketBGFetchItem;

/**
 * The following will be used to identify
 * the source of an item's expiration.
 */
enum class ExpireBy { Pager, Compactor, Access };

/* Structure that holds info needed for notification for an item being updated
   in the vbucket */
struct VBNotifyCtx {
    VBNotifyCtx() : bySeqno(0), notifyReplication(false), notifyFlusher(false) {
    }
    Monotonic<int64_t> bySeqno;
    bool notifyReplication;
    bool notifyFlusher;
};

/**
 * Structure that holds info needed to queue an item in chkpt or vb backfill
 * queue
 */
struct VBQueueItemCtx {
    VBQueueItemCtx(GenerateBySeqno genBySeqno,
                   GenerateCas genCas,
                   TrackCasDrift trackCasDrift,
                   bool isBackfillItem,
                   PreLinkDocumentContext* preLinkDocumentContext_)
        : genBySeqno(genBySeqno),
          genCas(genCas),
          trackCasDrift(trackCasDrift),
          isBackfillItem(isBackfillItem),
          preLinkDocumentContext(preLinkDocumentContext_) {
    }
    /* Indicates if we should queue an item or not. If this is false other
       members should not be used */
    GenerateBySeqno genBySeqno;
    GenerateCas genCas;
    TrackCasDrift trackCasDrift;
    bool isBackfillItem;
    PreLinkDocumentContext* preLinkDocumentContext;
};

/**
 * Structure that holds seqno based or checkpoint persistence based high
 * priority requests to a vbucket
 */
struct HighPriorityVBEntry {
    HighPriorityVBEntry(const void* c,
                        uint64_t idNum,
                        HighPriorityVBNotify reqType)
        : cookie(c), id(idNum), reqType(reqType), start(ProcessClock::now()) {
    }

    const void* cookie;
    uint64_t id;
    HighPriorityVBNotify reqType;

    /* for stats (histogram) */
    ProcessClock::time_point start;
};

typedef std::unique_ptr<Callback<const uint16_t, const VBNotifyCtx&>>
        NewSeqnoCallback;

class EventuallyPersistentEngine;
class FailoverTable;
class KVShard;
class VBucketMemoryDeletionTask;

/**
 * An individual vbucket.
 */
class VBucket : public std::enable_shared_from_this<VBucket> {
public:

    // Identifier for a vBucket
    typedef uint16_t id_type;

    enum class GetKeyOnly {
         Yes,
         No
     };

    VBucket(id_type i,
            vbucket_state_t newState,
            EPStats& st,
            CheckpointConfig& chkConfig,
            int64_t lastSeqno,
            uint64_t lastSnapStart,
            uint64_t lastSnapEnd,
            std::unique_ptr<FailoverTable> table,
            std::shared_ptr<Callback<id_type>> flusherCb,
            std::unique_ptr<AbstractStoredValueFactory> valFact,
            NewSeqnoCallback newSeqnoCb,
            Configuration& config,
            item_eviction_policy_t evictionPolicy,
            vbucket_state_t initState = vbucket_state_dead,
            uint64_t purgeSeqno = 0,
            uint64_t maxCas = 0,
            int64_t hlcEpochSeqno = HlcCasSeqnoUninitialised,
            bool mightContainXattrs = false,
            const std::string& collectionsManifest = "");

    virtual ~VBucket();

    int64_t getHighSeqno() const;

    size_t getChkMgrMemUsage() const;

    size_t getChkMgrMemUsageOfUnrefCheckpoints() const;

    size_t getChkMgrMemUsageOverhead() const;

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

    /**
     * @returns a seqno, documents with a seqno >= the returned value have a HLC
     * generated CAS. Can return HlcCasSeqnoUninitialised if warmup has not
     * established or nothing is persisted
     */
    int64_t getHLCEpochSeqno() const {
        return hlc.getEpochSeqno();
    }

    /**
     * Set the seqno to be used to establish if an item has a HLC generated CAS.
     * @param seqno the value to store in the vbucket
     * @throws if an attempt to set to < 0
     */
    void setHLCEpochSeqno(int64_t seqno) {
        if (seqno < 0) {
            throw std::invalid_argument("VBucket::setHLCEpochSeqno(" +
                                        std::to_string(seqno) + ") seqno < 0 ");
        }
        hlc.setEpochSeqno(seqno);
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

    /**
     * @return true if the vbucket deletion is to be deferred to a background
     *         task.
     */
    bool isDeletionDeferred() const {
        return deferredDeletion.load();
    }

    /**
     * @param value true if the vbucket's deletion should be deferred to a
     *        background task. This is for VBucket objects created by
     *        makeVBucket and owned by a VBucketPtr. If the VBucket was manually
     *        created this will have no effect on deletion.
     */
    void setDeferredDeletion(bool value) {
        deferredDeletion.store(true);
    }

    /**
     * @param A cookie to notify when the deferred deletion completes.
     */
    void setDeferredDeletionCookie(const void* cookie) {
        deferredDeletionCookie = cookie;
    }

    /**
     * @return the cookie which could of been set when setupDeferredDeletion was
     *         called.
     */
    const void* getDeferredDeletionCookie() const {
        return deferredDeletionCookie;
    }

    /**
     * Setup deferred deletion, this is where deletion of the vbucket is
     * deferred and completed by an AUXIO/NONIO task. AUXIO for EPVBucket
     * as it will hit disk for the data file unlink, NONIO is used for
     * EphemeralVBucket as only memory resources need freeing.
     * @param cookie A cookie to notify when the deletion task completes.
     */
    virtual void setupDeferredDeletion(const void* cookie) = 0;

    // Returns the last persisted sequence number for the VBucket
    virtual uint64_t getPersistenceSeqno() const = 0;

    /**
     * Returns the sequence number to expose publically as the highest
     * persisted seqno. Note this is may differ from getPersistenceSeqno,
     * depending on the Bucket type.
     *
     * Historical note: This is the same as PersistenceSeqno for EP buckets,
     * and hence before Spock wasn't a separate function; however for Ephemeral
     * buckets we need to distinguish between what sequence number we report
     * to external clients for Observe/persistTo, and what sequence number we
     * report to internal DCP / ns_server for takeover:
     *  a) Clients need 0 for the Ephemeral "persisted to" seqno (as
     *     there isn't any Persistence and we can't claim something is on-disk
     *     when it is not).
     *  b) ns_server / replication needs a non-zero, "logically-persisted" seqno
     *     from the replica to know that a vBucket is ready for takeover.
     * As such, getPublicPersistenceSeqno() is used for (a), and
     * getPersistenceSeqno() is used for (b).
     */
    virtual uint64_t getPublicPersistenceSeqno() const = 0;

    void setPersistenceSeqno(uint64_t seqno) {
        persistenceSeqno.store(seqno);
    }

    id_type getId() const { return id; }
    vbucket_state_t getState(void) const { return state.load(); }

    /**
     * Sets the vbucket state to a desired state
     *
     * @param to desired vbucket state
     */
    void setState(vbucket_state_t to);

    /**
     * Sets the vbucket state to a desired state with the 'stateLock' already
     * acquired
     *
     * @param to desired vbucket state
     * @param vbStateLock write lock holder on 'stateLock'
     */
    void setState_UNLOCKED(vbucket_state_t to, WriterLockHolder& vbStateLock);

    cb::RWLock& getStateLock() {return stateLock;}

    vbucket_state_t getInitialState(void) { return initialState; }

    vbucket_state getVBucketState() const;

    /**
     * This method performs operations on the stored value prior
     * to expiring the item.
     *
     * @param v the stored value
     */
    void handlePreExpiry(const std::unique_lock<std::mutex>& hbl,
                         StoredValue& v);

    bool addPendingOp(const void *cookie);

    void doStatsForQueueing(const Item& item, size_t itemBytes);
    void doStatsForFlushing(const Item& item, size_t itemBytes);
    void incrMetaDataDisk(const Item& qi);
    void decrMetaDataDisk(const Item& qi);

    /// Increase the total count of items in this VBucket by 1.
    virtual void incrNumTotalItems() = 0;

    /// Decrease the total count of items in this VBucket by 1.
    virtual void decrNumTotalItems() = 0;

    /**
     * Set the total count of items in this VBucket to the specified value.
     */
    virtual void setNumTotalItems(size_t items) = 0;

    /// Reset all statistics assocated with this vBucket.
    virtual void resetStats();

    // Get age sum in millisecond
    uint64_t getQueueAge();

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

    /**
     * Process an item from a DCP backfill.
     * It puts it onto a queue for persistence and/or generates a seqno and
     * updates stats
     *
     * @param qi item to be processed
     * @param generateBySeqno indicates if a new seqno must generated or the
     *                        seqno in the item must be used
     *
     *
     */
    virtual void queueBackfillItem(queued_item& qi,
                                   const GenerateBySeqno generateBySeqno) = 0;

    /**
     * Transfer any backfill items to the specified vector, up to the optional
     * limit.
     * @param items Destination for items transferred from backfill.
     * @param limit If non-zero, limit the number of items transferred to the
     * specified number.
     * @return true if any more items remain in the backfill set - i.e. if
     *         limit constrained the number of items.
     */
    bool getBackfillItems(std::vector<queued_item>& items, size_t limit = 0);

    struct ItemsToFlush {
        std::vector<queued_item> items;
        snapshot_range_t range;
        bool moreAvailable = false;
    };

    /**
     * Obtain the series of items to be flushed for this vBucket.
     *
     * @param vb VBucket to fetch items for.
     * @param approxLimit Upper bound on how many items to fetch.
     * @return The items to flush; along with their seqno range and
     *         if more items are available for this vBucket (i.e. the
     *         limit was reached).
     */
    ItemsToFlush getItemsToPersist(size_t approxLimit);

    bool isBackfillPhase() {
        return backfill.isBackfillPhase.load();
    }

    void setBackfillPhase(bool backfillPhase) {
        backfill.isBackfillPhase.store(backfillPhase);
    }

    /**
     * Returns the map of bgfetch items for this vbucket, clearing the
     * pendingBGFetches.
     */
    virtual vb_bgfetch_queue_t getBGFetchItems() = 0;

    virtual bool hasPendingBGFetchItems() = 0;

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

    /**
     * Checks and decides whether to add high priority request on the vbucket.
     * This is an async request made by modules like ns-server during
     * rebalance. The request is for a response from the vbucket when it
     * 'sees' beyond a certain sequence number or when a certain checkpoint
     * is persisted.
     * Depending on the vbucket type, the meaning 'seeing' a sequence number
     * changes. That is, it could mean persisted in case of EPVBucket and
     * added to the sequenced data structure in case of EphemeralVBucket.
     *
     * @param seqnoOrChkId seqno to be seen or checkpoint id to be persisted
     * @param cookie cookie of conn to be notified
     * @param reqType indicating request for seqno or chk persistence
     *
     * @return RequestScheduled if a high priority request is added and
     *                          notification will be done asynchronously
     *         NotSupported if the request is not supported for the reqType
     *         RequestNotScheduled if a high priority request is NOT added (as
     *                             it is not required). This implies there won't
     *                             be a subsequent notification
     */
    virtual HighPriorityVBReqStatus checkAddHighPriorityVBEntry(
            uint64_t seqnoOrChkId,
            const void* cookie,
            HighPriorityVBNotify reqType) = 0;

    /**
     * Notify the high priority requests on the vbucket.
     * This is the response to async requests made by modules like ns-server
     * during rebalance.
     *
     * @param engine Ref to ep-engine
     * @param id seqno or checkpoint id causing the notification(s).
     * @param notifyType indicating notify for seqno or chk persistence
     */
    virtual void notifyHighPriorityRequests(
            EventuallyPersistentEngine& engine,
            uint64_t id,
            HighPriorityVBNotify notifyType) = 0;

    virtual void notifyAllPendingConnsFailed(EventuallyPersistentEngine& e) = 0;

    /**
     * Get high priority notifications for a seqno or checkpoint persisted
     *
     * @param engine Ref to ep-engine
     * @param id seqno or checkpoint id for which notifies are to be found
     * @param notifyType indicating notify for seqno or chk persistence
     *
     * @return map of notifications with conn cookie as the key and notify
     *         status as the value
     */
    std::map<const void*, ENGINE_ERROR_CODE> getHighPriorityNotifications(
            EventuallyPersistentEngine& engine,
            uint64_t idNum,
            HighPriorityVBNotify notifyType);

    size_t getHighPriorityChkSize() {
        return numHpVBReqs.load();
    }

    /**
     * BloomFilter operations for vbucket
     */
    void createFilter(size_t key_count, double probability);
    void initTempFilter(size_t key_count, double probability);
    void addToFilter(const DocKey& key);
    virtual bool maybeKeyExistsInFilter(const DocKey& key);
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
    bool isResidentRatioUnderThreshold(float threshold);

    /**
     * Returns true if deleted items (aka tombstones) are always resident in
     * memory (and hence we do not need to attempt a bgFetch if we try to
     * access a deleted key which isn't found in memory).
     */
    virtual bool areDeletedItemsAlwaysResident() const = 0;

    virtual void addStats(bool details, ADD_STAT add_stat, const void* c) = 0;

    virtual KVShard* getShard() = 0;

    /**
     * Returns the number of alive (non-deleted) Items the VBucket.
     *
     * Includes items which are not currently resident in memory (i.e. under
     * Full eviction and have been fully evicted from memory).
     * Does *not* include deleted items.
     */
    virtual size_t getNumItems() const = 0;

    virtual size_t getNumNonResidentItems() const = 0;

    size_t getNumTempItems(void) {
        return ht.getNumTempItems();
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

    // Mark the value associated with the given key as dirty
    void markDirty(const DocKey& key);

    /**
     * Obtain the read handle for the collections manifest.
     * The caller will have read-only access to manifest using the methods
     * exposed by the ReadHandle
     */
    Collections::VB::Manifest::ReadHandle lockCollections() const {
        return manifest.lock();
    }

    /**
     * Obtain a caching read handle for the collections manifest.
     * The returned handle will lookup the collection associated with key
     * and cache the internal iterator so that future usage of
     * isLogicallyDeleted doesn't need to re-scan and lookup. This is different
     * to a plain ReadHandle which provides more functionality (more methods
     * for the caller), but may result in extra lookups and key-scans.
     * @param key A key to use for constructing the read handle.
     * @param allowSystem true if system keys are allowed (the KV
     *        internal keys like create collection). A frontend operation
     *        should not be allowed, whereas a disk backfill is allowed
     * @return a CachingReadHandle which the caller should test is valid with
     *         CachingReadHandle::valid
     */
    Collections::VB::Manifest::CachingReadHandle lockCollections(
            const DocKey& key, bool allowSystem = false) const {
        return manifest.lock(key, allowSystem);
    }

    /**
     * Update the Collections::VB::Manifest and the VBucket.
     * Adds SystemEvents for the create and delete of collections into the
     * checkpoint.
     *
     * @param m A Collections::Manifest to apply to the VB::Manifest
     */
    void updateFromManifest(const Collections::Manifest& m) {
        manifest.wlock().update(*this, m);
    }

    /**
     * Finalise the deletion of a collection (no items remain in the collection)
     *
     * @param identifier ID of the collection that has completed deleting
     */
    void completeDeletion(CollectionID identifier) {
        manifest.wlock().completeDeletion(*this, identifier);
    }

    /**
     * Add a collection to this vbucket with a pre-assigned seqno. I.e.
     * this VB is a replica.
     *
     * @param manifestUid the uid of the manifest which made the change
     * @param identifier CID for the collection being added.
     * @param bySeqno The seqno assigned to the collection create event.
     */
    void replicaAddCollection(Collections::uid_t manifestUid,
                              CollectionID identifier,
                              int64_t bySeqno) {
        manifest.wlock().replicaAdd(*this, manifestUid, identifier, bySeqno);
    }

    /**
     * Delete a collection from this vbucket with a pre-assigned seqno. I.e.
     * this VB is a replica.
     *
     * @param manifestUid the uid of the manifest which made the change
     * @param identifier CID for the collection to begin deleting.
     * @param bySeqno The seqno assigned to the collection delete event.
     */
    void replicaBeginDeleteCollection(Collections::uid_t manifestUid,
                                      CollectionID identifier,
                                      int64_t bySeqno) {
        manifest.wlock().replicaBeginDelete(
                *this, manifestUid, identifier, bySeqno);
    }

    /**
     * Get the collection manifest
     *
     * @return const reference to the manifest
     */
    const Collections::VB::Manifest& getManifest() const {
        return manifest;
    }

    static const vbucket_state_t ACTIVE;
    static const vbucket_state_t REPLICA;
    static const vbucket_state_t PENDING;
    static const vbucket_state_t DEAD;

    HashTable         ht;

    /// Manager of this vBucket's checkpoints. unique_ptr for pimpl.
    std::unique_ptr<CheckpointManager> checkpointManager;

    // Struct for managing 'backfill' items - Items which have been added by
    // an incoming DCP stream and need to be persisted to disk.
    struct {
        std::mutex mutex;
        std::queue<queued_item> items;
        std::atomic<bool> isBackfillPhase;
    } backfill;

    /**
     * Gets the valid StoredValue for the key and deletes an expired item if
     * desired by the caller. Requires the hash bucket to be locked
     *
     * @param hbl Reference to the hash bucket lock
     * @param key
     * @param wantsDeleted
     * @param trackReference
     * @param queueExpired Delete an expired item
     */
    StoredValue* fetchValidValue(HashTable::HashBucketLock& hbl,
                                 const DocKey& key,
                                 WantsDeleted wantsDeleted,
                                 TrackReference trackReference,
                                 QueueExpired queueExpired);

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
    virtual ENGINE_ERROR_CODE completeBGFetchForSingleItem(
            const DocKey& key,
            const VBucketBGFetchItem& fetched_item,
            const ProcessClock::time_point startTime) = 0;

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
    virtual ENGINE_ERROR_CODE statsVKey(const DocKey& key,
                                        const void* cookie,
                                        EventuallyPersistentEngine& engine,
                                        int bgFetchDelay) = 0;

    /**
     * Complete the vkey stats for an item background fetched from disk.
     *
     * @param key The key of the item
     * @param gcb Bgfetch cbk obj containing the item from disk
     *
     */
    virtual void completeStatsVKey(const DocKey& key, const GetValue& gcb) = 0;

    /**
     * Set (add new or update) an item into in-memory structure like
     * hash table and do not generate a seqno. This is called internally from
     * ep-engine when we want to update our in-memory data (like in HT) with
     * another source of truth like disk.
     * Currently called during rollback.
     *
     * @param itm Item to be added or updated. Upon success, the itm
     *            revSeqno are updated
     *
     * @return Result indicating the status of the operation
     */
    MutationStatus setFromInternal(Item& itm);

    /**
     * Set (add new or update) an item in the vbucket.
     *
     * @param itm Item to be added or updated. Upon success, the itm
     *            bySeqno, cas and revSeqno are updated
     * @param cookie the connection cookie
     * @param engine Reference to ep engine
     * @param bgFetchDelay
     * @param predicate a function to call which if returns true, the set will
     *        succeed. The function is called against any existing item.
     *
     * @return ENGINE_ERROR_CODE status notified to be to the front end
     */
    ENGINE_ERROR_CODE set(Item& itm,
                          const void* cookie,
                          EventuallyPersistentEngine& engine,
                          int bgFetchDelay,
                          cb::StoreIfPredicate predicate);

    /**
     * Replace (overwrite existing) an item in the vbucket.
     *
     * @param itm Item to be added or updated. Upon success, the itm
     *            bySeqno, cas and revSeqno are updated
     * @param cookie the connection cookie
     * @param engine Reference to ep engine
     * @param bgFetchDelay
     * @param predicate a function to call which if returns true, the replace
     *        will succeed. The function is called against any existing item.
     * @param readHandle Reader access to the Item's collection data.
     *
     * @return ENGINE_ERROR_CODE status notified to be to the front end
     */
    ENGINE_ERROR_CODE replace(
            Item& itm,
            const void* cookie,
            EventuallyPersistentEngine& engine,
            int bgFetchDelay,
            cb::StoreIfPredicate predicate,
            const Collections::VB::Manifest::CachingReadHandle& readHandle);

    /**
     * Add an item directly into its vbucket rather than putting it on a
     * checkpoint (backfill the item). The can happen during DCP or when a
     * replica vbucket is receiving backfill items from active vbucket.
     *
     * @param itm Item to be added/updated from DCP backfill. Upon
     *            success, the itm revSeqno is updated
     * @param genBySeqno whether or not to generate sequence number
     *
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE addBackfillItem(Item& itm, GenerateBySeqno genBySeqno);

    /**
     * Set an item in the store from a non-front end operation (DCP, XDCR)
     *
     * @param item the item to set. Upon success, the itm revSeqno is updated
     * @param cas value to match
     * @param seqno sequence number of mutation
     * @param cookie the cookie representing the client to store the item
     * @param engine Reference to ep engine
     * @param bgFetchDelay
     * @param checkConflicts should conflict resolution be done?
     * @param allowExisting set to false if you want set to fail if the
     *                      item exists already
     * @param genBySeqno whether or not to generate sequence number
     * @param genCas
     * @param isReplication set to true if we are to use replication
     *                      throttle threshold
     * @param readHandle Reader access to the Item's collection data.
     *
     * @return the result of the store operation
     */
    ENGINE_ERROR_CODE setWithMeta(
            Item& itm,
            uint64_t cas,
            uint64_t* seqno,
            const void* cookie,
            EventuallyPersistentEngine& engine,
            int bgFetchDelay,
            CheckConflicts checkConflicts,
            bool allowExisting,
            GenerateBySeqno genBySeqno,
            GenerateCas genCas,
            bool isReplication,
            const Collections::VB::Manifest::CachingReadHandle& readHandle);

    /**
     * Delete an item in the vbucket
     *
     * @param[in,out] cas value to match; new cas after logical delete
     * @param cookie the cookie representing the client to store the item
     * @param engine Reference to ep engine
     * @param bgFetchDelay
     * @param[out] itemMeta pointer to item meta data that needs to be returned
     *                      as a result the delete. A NULL pointer indicates
     *                      that no meta data needs to be returned.
     * @param[out] mutInfo Info to uniquely identify (and order) the delete
     *                     seq. A NULL pointer indicates no info needs to be
     *                     returned.
     * @param readHandle Reader access to the affected key's collection data.
     *
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE deleteItem(
            uint64_t& cas,
            const void* cookie,
            EventuallyPersistentEngine& engine,
            int bgFetchDelay,
            ItemMetaData* itemMeta,
            mutation_descr_t& mutInfo,
            const Collections::VB::Manifest::CachingReadHandle& readHandle);

    /**
     * Delete an item in the vbucket from a non-front end operation (DCP, XDCR)
     *
     * @param[in, out] cas value to match; new cas after logical delete
     * @param[out] seqno Pointer to get the seqno generated for the item. A
     *                   NULL value is passed if not needed
     * @param cookie the cookie representing the client to store the item
     * @param engine Reference to ep engine
     * @param bgFetchDelay
     * @param checkConflicts should conflict resolution be done?
     * @param itemMeta ref to item meta data
     * @param backfill indicates if the item must be put onto vb queue or
     *                 onto checkpoint
     * @param genBySeqno whether or not to generate sequence number
     * @param generateCas whether or not to generate cas
     * @param bySeqno seqno of the key being deleted
     * @param isReplication set to true if we are to use replication
     *                      throttle threshold
     * @param readHandle Reader access to the key's collection data.
     *
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE deleteWithMeta(
            uint64_t& cas,
            uint64_t* seqno,
            const void* cookie,
            EventuallyPersistentEngine& engine,
            int bgFetchDelay,
            CheckConflicts checkConflicts,
            const ItemMetaData& itemMeta,
            bool backfill,
            GenerateBySeqno genBySeqno,
            GenerateCas generateCas,
            uint64_t bySeqno,
            bool isReplication,
            const Collections::VB::Manifest::CachingReadHandle& readHandle);

    /**
     * Delete an expired item
     *
     * @param it item to be deleted
     * @param startTime the time to be compared with this item's expiry time
     * @param source Expiry source
     */
    void deleteExpiredItem(const Item& it,
                           time_t startTime,
                           ExpireBy source);

    /**
     * Evict a key from memory.
     *
     * @param key Key to evict
     * @param[out] msg Updated to point to a string (with static duration)
     *                 describing the result of the operation.
     *
     * @return SUCCESS if key was successfully evicted (or was already
     *                 evicted), or the reason why the request failed.
     *
     */
    virtual protocol_binary_response_status evictKey(const DocKey& key,
                                                     const char** msg) = 0;

    /**
     * Page out a StoredValue from memory.
     *
     * The definition of "page out" is up to the underlying VBucket
     * implementation - this may mean simply ejecting the value from memory
     * (Value Eviction), removing the entire document from memory (Full Eviction),
     * or actually deleting the document (Ephemeral Buckets).
     *
     * @param lh Bucket lock associated with the StoredValue.
     * @param v[in, out] Ref to the StoredValue to be ejected. Based on the
     *                   VBucket type, policy in the vbucket contents of v and
     *                   v itself may be changed
     *
     * @return true if an item is ejected.
     */
    virtual bool pageOut(const HashTable::HashBucketLock& lh,
                         StoredValue*& v) = 0;

    /*
     * Check to see if a StoredValue is eligible to be paged out of memory.
     *
     * @param lh Bucket lock associated with the StoredValue.
     * @param v Reference to the StoredValue to be ejected.
     *
     * @return true if the StoredValue is eligible to be paged out.
     *
     */
    virtual bool eligibleToPageOut(const HashTable::HashBucketLock& lh,
                                   const StoredValue& v) const = 0;

    /**
     * Add an item in the store
     *
     * @param itm the item to add. On success, this will have its seqno and
     *            CAS updated.
     * @param cookie the cookie representing the client to store the item
     * @param engine Reference to ep engine
     * @param bgFetchDelay
     * @param readHandle Reader access to the Item's collection data.
     *
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE add(
            Item& itm,
            const void* cookie,
            EventuallyPersistentEngine& engine,
            int bgFetchDelay,
            const Collections::VB::Manifest::CachingReadHandle& readHandle);

    /**
     * Retrieve a value, but update its TTL first
     *
     * @param cookie the connection cookie
     * @param engine Reference to ep engine
     * @param bgFetchDelay
     * @param exptime the new expiry time for the object
     * @param readHandle Reader access to the key's collection data.
     *
     * @return a GetValue representing the result of the request
     */
    GetValue getAndUpdateTtl(
            const void* cookie,
            EventuallyPersistentEngine& engine,
            int bgFetchDelay,
            time_t exptime,
            const Collections::VB::Manifest::CachingReadHandle& readHandle);
    /**
     * Queue an Item to the checkpoint and return its seqno
     *
     * @param item an Item object to queue, can be any kind of item and will be
     *        given a CAS and seqno by this function.
     * @param seqno An optional sequence number, if not specified checkpoint
     *        queueing will assign a seqno to the Item.
     */
    int64_t queueItem(Item* item, OptionalSeqno seqno);

    /**
     * Get metadata and value for a given key
     *
     * @param cookie the cookie representing the client
     * @param engine Reference to ep engine
     * @param bgFetchDelay
     * @param options flags indicating some retrieval related info
     * @param diskFlushAll
     * @param getKeyOnly if GetKeyOnly::Yes we want only the key
     * @param readHandle Reader access to the requested key's collection data.
     *
     * @return the result of the operation
     */
    GetValue getInternal(
            const void* cookie,
            EventuallyPersistentEngine& engine,
            int bgFetchDelay,
            get_options_t options,
            bool diskFlushAll,
            GetKeyOnly getKeyOnly,
            const Collections::VB::Manifest::CachingReadHandle& readHandle);

    /**
     * Retrieve the meta data for given key
     *
     * @param cookie the connection cookie
     * @param engine Reference to ep engine
     * @param bgFetchDelay Delay in secs before we run the bgFetch task
     * @param readHandle Reader access to the key's collection data.
     * @param[out] metadata meta information returned to the caller
     * @param[out] deleted specifies the caller whether or not the key is
     *                     deleted
     * @param[out] datatype specifies the datatype of the item
     *
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE getMetaData(
            const void* cookie,
            EventuallyPersistentEngine& engine,
            int bgFetchDelay,
            const Collections::VB::Manifest::CachingReadHandle& readHandle,
            ItemMetaData& metadata,
            uint32_t& deleted,
            uint8_t& datatype);

    /**
     * Looks up the key stats for the given {vbucket, key}.
     *
     * @param cookie The client's cookie
     * @param engine Reference to ep engine
     * @param bgFetchDelay
     * @param[out] kstats On success the keystats for this item.
     * @param wantsDeleted If yes then return keystats even if the item is
     *                     marked as deleted. If no then will return
     *                     ENGINE_KEY_ENOENT for deleted items.
     * @param readHandle Reader access to the key's collection data.
     *
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE getKeyStats(
            const void* cookie,
            EventuallyPersistentEngine& engine,
            int bgFetchDelay,
            struct key_stats& kstats,
            WantsDeleted wantsDeleted,
            const Collections::VB::Manifest::CachingReadHandle& readHandle);

    /**
     * Gets a locked item for a given key.
     *
     * @param currentTime Current time to use for locking the item for a
     *                    duration of lockTimeout
     * @param lockTimeout Timeout for the lock on the item
     * @param cookie The client's cookie
     * @param engine Reference to ep engine
     * @param bgFetchDelay Delay in secs before we run the bgFetch task
     * @param readHandle Reader access to the key's collection data.
     *
     * @return the result of the operation (contains locked item on success)
     */
    GetValue getLocked(
            rel_time_t currentTime,
            uint32_t lockTimeout,
            const void* cookie,
            EventuallyPersistentEngine& engine,
            int bgFetchDelay,
            const Collections::VB::Manifest::CachingReadHandle& readHandle);
    /**
     * Update in memory data structures after an item is deleted on disk
     *
     * @param queuedItem reference to the deleted item
     * @param deleted indicates if item actaully deleted or not (in case item
     *                did not exist on disk)
     */
    void deletedOnDiskCbk(const Item& queuedItem, bool deleted);

    /**
     * Update in memory data structures after a rollback on disk
     *
     * @param queuedItem item key
     *
     * @return indicates if the operation is succcessful
     */
    bool deleteKey(const DocKey& key);

    /**
     * Creates a DCP backfill object
     *
     * @param e ref to EventuallyPersistentEngine
     * @param stream Shared ptr to the stream for which this backfill obj is
     *               created
     * @param startSeqno requested start sequence number of the backfill
     * @param endSeqno requested end sequence number of the backfill
     *
     * @return pointer to the backfill object created. Caller to own this
     *         object and hence must handle deletion.
     */
    virtual std::unique_ptr<DCPBackfill> createDCPBackfill(
            EventuallyPersistentEngine& e,
            std::shared_ptr<ActiveStream> stream,
            uint64_t startSeqno,
            uint64_t endSeqno) = 0;

    /**
     * Update failovers, checkpoint mgr and other vBucket members after
     * rollback.
     *
     * @param rollbackResult contains high seqno of the vBucket after rollback,
     *                       snapshot start seqno of the last snapshot in the
     *                       vBucket after the rollback,
     *                       snapshot end seqno of the last snapshot in the
     *                       vBucket after the rollback
     * @param prevHighSeqno high seqno before the rollback
     */
    void postProcessRollback(const RollbackResult& rollbackResult,
                             uint64_t prevHighSeqno);

    /**
     * Debug - print a textual description of the VBucket to stderr.
     */
    virtual void dump() const;

    /**
     * Sets the callback function to invoke when a frequency counter becomes
     * saturated.  The callback function is to invoke the ItemFreqDecayer
     * task.
     *
     * @param callbackFunction - the function to callback.
     */
    void setFreqSaturatedCallback(std::function<void()> callbackFunction);

    /**
     * Returns the number of deletes in the memory
     *
     * @return number of deletes
     */
    size_t getNumInMemoryDeletes() const {
        /* couchbase vbuckets: this is generally (after deletes are persisted)
                               zero as hash table doesn't keep deletes after
                               they are persisted.
           ephemeral vbuckets: we keep deletes in both hash table and ordered
                               data structure. */
        return ht.getNumDeletedItems();
    }

    /**
     * Set that this VBucket might store documents with xattrs.
     * Persistent vbucket will flush this to disk.
     */
    void setMightContainXattrs() {
        mayContainXattrs.store(true);
    }

    bool mightContainXattrs() const {
        return mayContainXattrs.load();
    }

    cb::vbucket_info getInfo() const {
        return {mightContainXattrs()};
    }

    /**
     * If the key@bySeqno is found, remove it from the hash table
     *
     * @param key The key to look for
     * @param bySeqno The seqno of the key to remove
     */
    void removeKey(const DocKey& key, int64_t bySeqno);

    static std::chrono::seconds getCheckpointFlushTimeout();

    /**
     * Set the memory threshold on the current bucket quota for accepting a
     * new mutation. This is same across all the vbuckets
     *
     * @param memThreshold Threshold between 0 and 1
     */
    static void setMutationMemoryThreshold(double memThreshold);

    /**
     * Check if this StoredValue has become logically non-existent.
     * By logically non-existent, the item has been deleted
     * or doesn't exist
     *
     * @param v StoredValue to check
     * @param readHandle a ReadHandle for safe reading of collection data
     * @return true if the item is logically non-existent,
     *         false otherwise
     */
    static bool isLogicallyNonExistent(
            const StoredValue& v,
            const Collections::VB::Manifest::CachingReadHandle& readHandle);

    std::queue<queued_item> rejectQueue;
    std::unique_ptr<FailoverTable> failovers;

    std::atomic<size_t>  opsCreate;
    std::atomic<size_t>  opsDelete;
    std::atomic<size_t>  opsGet;
    std::atomic<size_t>  opsReject;
    std::atomic<size_t>  opsUpdate;

    cb::NonNegativeCounter<size_t> dirtyQueueSize;
    std::atomic<size_t>  dirtyQueueMem;
    std::atomic<size_t>  dirtyQueueFill;
    std::atomic<size_t>  dirtyQueueDrain;
    std::atomic<uint64_t> dirtyQueueAge;
    std::atomic<size_t>  dirtyQueuePendingWrites;
    std::atomic<size_t>  metaDataDisk;

    std::atomic<size_t>  numExpiredItems;

    /**
     * A custom delete function for deleting VBucket objects. Any thread could
     * be the last thread to release a VBucketPtr and deleting a VB will
     * eventually hit the I/O sub-system when we unlink the file, to be sure no
     * front-end thread does this work, we schedule the deletion to a background
     * task. This task scheduling is triggered by the shared_ptr/VBucketPtr
     * using this object as the deleter.
     */
    struct DeferredDeleter {
        DeferredDeleter(EventuallyPersistentEngine& engine) : engine(engine) {
        }

        /**
         * Called when the VBucketPtr has no more owners and runs delete on
         * the object.
         */
        void operator()(VBucket* vb) const;

        EventuallyPersistentEngine& engine;
    };

protected:
    /**
     * This function checks for the various states of the value & depending on
     * which the calling function can issue a bgfetch as needed.
     */
    std::pair<MutationStatus, GetValue> processGetAndUpdateTtl(
            HashTable::HashBucketLock& hbl,
            StoredValue* v,
            time_t exptime,
            const Collections::VB::Manifest::CachingReadHandle& readHandle);
    /**
     * This function checks cas, expiry and other partition (vbucket) related
     * rules before setting an item into other in-memory structure like HT,
     * and checkpoint mgr. This function assumes that HT bucket lock is grabbed.
     *
     * @param hbl Hash table bucket lock that must be held
     * @param v Reference to the ptr of StoredValue. This can be changed if a
     *          new StoredValue is added or just its contents is changed if the
     *          exisiting StoredValue is updated
     * @param itm Item to be added/updated. On success, its revSeqno is updated
     * @param cas value to match
     * @param allowExisting set to false if you want set to fail if the
     *                      item exists already
     * @param hasMetaData
     * @param queueItmCtx holds info needed to queue an item in chkpt or vb
     *                    backfill queue
     * @param storeIfStatus the status of any conditional store predicate
     * @param maybeKeyExists true if the key /may/ exist on disk (as an active,
     *                       alive document). Only valid if `v` is null.
     * @param isReplication true if issued by consumer (for replication)
     *
     * @return Result indicating the status of the operation and notification
     *                info (if operation was successful).
     */
    std::pair<MutationStatus, boost::optional<VBNotifyCtx>> processSet(
            const HashTable::HashBucketLock& hbl,
            StoredValue*& v,
            Item& itm,
            uint64_t cas,
            bool allowExisting,
            bool hasMetaData,
            const VBQueueItemCtx& queueItmCtx,
            cb::StoreIfStatus storeIfStatus,
            bool maybeKeyExists = true,
            bool isReplication = false);

    /**
     * This function checks cas, expiry and other partition (vbucket) related
     * rules before adding an item into other in-memory structure like HT,
     * and checkpoint mgr. This function assumes that HT bucket lock is grabbed.
     *
     * @param hbl Hash table bucket lock that must be held
     * @param v[in, out] the stored value to do this operation on
     * @param itm Item to be added/updated. On success, its revSeqno is updated
     * @param isReplication true if issued by consumer (for replication)
     * @param queueItmCtx holds info needed to queue an item in chkpt or vb
     *                    backfill queue
     * @param readHandle Reader access to the Item's collection data.
     *
     * @return Result indicating the status of the operation and notification
     *                info (if the operation was successful).
     */
    std::pair<AddStatus, boost::optional<VBNotifyCtx>> processAdd(
            const HashTable::HashBucketLock& hbl,
            StoredValue*& v,
            Item& itm,
            bool maybeKeyExists,
            bool isReplication,
            const VBQueueItemCtx& queueItmCtx,
            const Collections::VB::Manifest::CachingReadHandle& readHandle);

    /**
     * This function checks cas, eviction policy and other partition
     * (vbucket) related rules before logically (soft) deleting an item in
     * in-memory structure like HT, and checkpoint mgr.
     * Assumes that HT bucket lock is grabbed.
     *
     * @param hbl Hash table bucket lock that must be held
     * @param v Reference to the StoredValue to be soft deleted
     * @param cas the expected CAS of the item (or 0 to override)
     * @param metadata ref to item meta data
     * @param queueItmCtx holds info needed to queue an item in chkpt or vb
     *                    backfill queue
     * @param use_meta Indicates if v must be updated with the metadata
     * @param bySeqno seqno of the key being deleted
     *
     * @return pointer to the updated StoredValue. It can be same as that of
     *         v or different value if a new StoredValue is created for the
     *         update.
     *         status of the operation.
     *         notification info, if status was successful.
     */
    std::tuple<MutationStatus, StoredValue*, boost::optional<VBNotifyCtx>>
    processSoftDelete(const HashTable::HashBucketLock& hbl,
                      StoredValue& v,
                      uint64_t cas,
                      const ItemMetaData& metadata,
                      const VBQueueItemCtx& queueItmCtx,
                      bool use_meta,
                      uint64_t bySeqno);

    /**
     * Delete a key (associated StoredValue) from ALL in-memory data structures
     * like HT.
     * Assumes that HT bucket lock is grabbed.
     *
     * Currently StoredValues form HashTable intrusively. That is, HashTable
     * does not store a reference or a copy of the StoredValue. If any other
     * in-memory data strucutures are formed intrusively using StoredValues,
     * then it must be decided in this function which data structure deletes
     * the StoredValue. Currently it is HashTable that deleted the StoredValue
     *
     * @param hbl Hash table bucket lock that must be held
     * @param v Reference to the StoredValue to be deleted
     *
     * @return true if an object was deleted, false otherwise
     */
    bool deleteStoredValue(const HashTable::HashBucketLock& hbl,
                           StoredValue& v);

    /**
     * Queue an item for persistence and replication. Maybe track CAS drift
     *
     * The caller of this function must hold the lock of the hash table
     * partition that contains the StoredValue being Queued.
     *
     * @param v the dirty item. The cas and seqno maybe updated based on the
     *          flags passed
     * @param queueItmCtx holds info needed to queue an item in chkpt or vb
     *                    backfill queue, whether to track cas, generate seqno,
     *                    generate new cas
     *
     * @return Notification context containing info needed to notify the
     *         clients (like connections, flusher)
     */
    VBNotifyCtx queueDirty(StoredValue& v, const VBQueueItemCtx& queueItmCtx);

    /**
     * Queue an item for persistence and replication
     *
     * The caller of this function must hold the lock of the hash table
     * partition that contains the StoredValue being Queued.
     *
     * @param v the dirty item. The cas and seqno maybe updated based on the
     *          flags passed
     * @param generateBySeqno request that the seqno is generated by this call
     * @param generateCas request that the CAS is generated by this call
     * @param isBackfillItem indicates if the item must be put onto vb queue or
     *        onto checkpoint
     * @param preLinkDocumentContext context object which allows running the
     *        document pre link callback after the cas is assinged (but
     *        but document not available for anyone)
     *
     * @return Notification context containing info needed to notify the
     *         clients (like connections, flusher)
     */
    VBNotifyCtx queueDirty(
            StoredValue& v,
            GenerateBySeqno generateBySeqno = GenerateBySeqno::Yes,
            GenerateCas generateCas = GenerateCas::Yes,
            bool isBackfillItem = false,
            PreLinkDocumentContext* preLinkDocumentContext = nullptr);

    /**
     * Adds a temporary StoredValue in in-memory data structures like HT.
     * Assumes that HT bucket lock is grabbed.
     *
     * @param hbl Hash table bucket lock that must be held
     * @param key the key for which a temporary item needs to be added
     * @param isReplication true if issued by consumer (for replication)
     *
     * @return Result indicating the status of the operation
     */
    TempAddStatus addTempStoredValue(const HashTable::HashBucketLock& hbl,
                                     const DocKey& key,
                                     bool isReplication = false);

    /**
     * Internal wrapper function around the callback to be called when a new
     * seqno is generated in the vbucket
     *
     * @param notifyCtx holds info needed for notification
     */
    void notifyNewSeqno(const VBNotifyCtx& notifyCtx);

    /**
     * VBucket internal function to store high priority requests on the vbucket.
     *
     * @param seqnoOrChkId seqno to be seen or checkpoint id to be persisted
     * @param cookie cookie of conn to be notified
     * @param reqType request type indicating seqno or chk persistence
     */
    void addHighPriorityVBEntry(uint64_t seqnoOrChkId,
                                const void* cookie,
                                HighPriorityVBNotify reqType);

    /**
     * Get all high priority notifications as temporary failures because they
     * could not be completed.
     *
     * @param engine Ref to ep-engine
     *
     * @return map of notifies with conn cookie as the key and notify status as
     *         the value
     */
    std::map<const void*, ENGINE_ERROR_CODE> tmpFailAndGetAllHpNotifies(
            EventuallyPersistentEngine& engine);

    /**
     * Check if there is memory available to allocate the in-memory
     * instance (StoredValue or OrderedStoredValue) for an item.
     *
     * @param st Reference to epstats
     * @param item Item that is being added
     * @param isReplication Flag indicating if the item is from replication
     *
     * @return True if there is memory for the item; else False
     */
    bool hasMemoryForStoredValue(EPStats& st,
                                 const Item& item,
                                 bool isReplication);

    void _addStats(bool details, ADD_STAT add_stat, const void* c);

    template <typename T>
    void addStat(const char* nm,
                 const T& val,
                 ADD_STAT add_stat,
                 const void* c);

    /* This member holds the eviction policy used */
    const item_eviction_policy_t eviction;

    /* Reference to global (EP engine wide) stats */
    EPStats& stats;

    /* last seqno that is persisted on the disk */
    std::atomic<uint64_t> persistenceSeqno;

    /* holds all high priority async requests to the vbucket */
    std::list<HighPriorityVBEntry> hpVBReqs;

    /* synchronizes access to hpVBReqs */
    std::mutex hpVBReqsMutex;

    /* size of list hpVBReqs (to avoid MB-9434) */
    Couchbase::RelaxedAtomic<size_t> numHpVBReqs;

    /**
     * VBucket sub-classes must implement a function that will schedule
     * an appropriate task that will delete the VBucket and its resources.
     *
     * @param engine owning engine (required for task construction)
     */
    virtual void scheduleDeferredDeletion(
            EventuallyPersistentEngine& engine) = 0;

    /**
     * Update the revision seqno of a newly StoredValue item.
     * We must ensure that it is greater the maxDeletedRevSeqno
     *
     * @param v StoredValue added newly. Its revSeqno is updated
     */
    void updateRevSeqNoOfNewStoredValue(StoredValue& v);

private:
    void fireAllOps(EventuallyPersistentEngine& engine, ENGINE_ERROR_CODE code);

    void decrDirtyQueueMem(size_t decrementBy);

    void decrDirtyQueueAge(uint32_t decrementBy);

    void decrDirtyQueuePendingWrites(size_t decrementBy);

    /**
     * Updates an existing StoredValue in in-memory data structures like HT.
     * Assumes that HT bucket lock is grabbed.
     *
     * @param htLock Hash table lock that must be held
     * @param v Reference to the StoredValue to be updated.
     * @param itm Item to be updated.
     * @param queueItmCtx holds info needed to queue an item in chkpt or vb
     *                    backfill queue
     * @param justTouch   To note that this object is an existing item with
     *                    the same value but with few flags changed.
     * @return pointer to the updated StoredValue. It can be same as that of
     *         v or different value if a new StoredValue is created for the
     *         update.
     *         status of the operation.
     *         notification info.
     */
    virtual std::tuple<StoredValue*, MutationStatus, VBNotifyCtx>
    updateStoredValue(const HashTable::HashBucketLock& hbl,
                      StoredValue& v,
                      const Item& itm,
                      const VBQueueItemCtx& queueItmCtx,
                      bool justTouch = false) = 0;

    /**
     * Adds a new StoredValue in in-memory data structures like HT.
     * Assumes that HT bucket lock is grabbed.
     *
     * @param hbl Hash table bucket lock that must be held
     * @param itm Item to be added.
     * @param queueItmCtx holds info needed to queue an item in chkpt or vb
     *                    backfill queue
     * @param genRevSeqno whether to generate new revision sequence number
     *                    or not
     *
     * @return Ptr of the StoredValue added and notification info
     */
    virtual std::pair<StoredValue*, VBNotifyCtx> addNewStoredValue(
            const HashTable::HashBucketLock& hbl,
            const Item& itm,
            const VBQueueItemCtx& queueItmCtx,
            GenerateRevSeqno genRevSeqno) = 0;

    /**
     * Logically (soft) delete item in all in-memory data structures. Also
     * updates revSeqno. Depending on the in-memory data structure the item may
     * be marked delete and/or reset and/or a new value (marked as deleted)
     * added.
     * Assumes that HT bucket lock is grabbed.
     * Also assumes that v is in the hash table.
     *
     * @param hbl Hash table bucket lock that must be held
     * @param v Reference to the StoredValue to be soft deleted
     * @param onlyMarkDeleted indicates if we must reset the StoredValue or
     *                        just mark deleted
     * @param queueItmCtx holds info needed to queue an item in chkpt or vb
     *                    backfill queue
     * @param bySeqno seqno of the key being deleted
     *
     * @return pointer to the updated StoredValue. It can be same as that of
     *         v or different value if a new StoredValue is created for the
     *         update.
     *         notification info.
     */
    virtual std::tuple<StoredValue*, VBNotifyCtx> softDeleteStoredValue(
            const HashTable::HashBucketLock& hbl,
            StoredValue& v,
            bool onlyMarkDeleted,
            const VBQueueItemCtx& queueItmCtx,
            uint64_t bySeqno) = 0;

    /**
     * This function handles expiry relatead stuff before logically (soft)
     * deleting an item in in-memory structures like HT, and checkpoint mgr.
     * Assumes that HT bucket lock is grabbed.
     *
     * @param hbl Hash table bucket lock that must be held
     * @param v Reference to the StoredValue to be soft deleted
     *
     * @return status of the operation.
     *         pointer to the updated StoredValue. It can be same as that of
     *         v or different value if a new StoredValue is created for the
     *         update.
     *         notification info.
     */
    std::tuple<MutationStatus, StoredValue*, VBNotifyCtx> processExpiredItem(
            const HashTable::HashBucketLock& hbl, StoredValue& v);

    /**
     * Add a temporary item in hash table and enqueue a background fetch for a
     * key.
     *
     * @param hbl Reference to the hash table bucket lock
     * @param key the key to be bg fetched
     * @param cookie the cookie of the requestor
     * @param engine Reference to ep engine
     * @param bgFetchDelay Delay in secs before we run the bgFetch task
     * @param metadataOnly whether the fetch is for a non-resident value or
     *                     metadata of a (possibly) deleted item
     * @param isReplication indicates if the call is for a replica vbucket
     *
     * @return ENGINE_ERROR_CODE status notified to be to the front end
     */
    virtual ENGINE_ERROR_CODE addTempItemAndBGFetch(
            HashTable::HashBucketLock& hbl,
            const DocKey& key,
            const void* cookie,
            EventuallyPersistentEngine& engine,
            int bgFetchDelay,
            bool metadataOnly,
            bool isReplication = false) = 0;

    /**
     * Enqueue a background fetch for a key.
     *
     * @param key the key to be bg fetched
     * @param cookie the cookie of the requestor
     * @param engine Reference to ep engine
     * @param bgFetchDelay Delay in secs before we run the bgFetch task
     * @param isMeta whether the fetch is for a non-resident value or metadata
     *               of a (possibly) deleted item
     */
    virtual void bgFetch(const DocKey& key,
                         const void* cookie,
                         EventuallyPersistentEngine& engine,
                         int bgFetchDelay,
                         bool isMeta = false) = 0;

    /**
     * Get metadata and value for a non-resident key
     *
     * @param key key for which metadata and value should be retrieved
     * @param cookie the cookie representing the client
     * @param engine Reference to ep engine
     * @param bgFetchDelay Delay in secs before we run the bgFetch task
     * @param queueBgFetch Indicates whether a background fetch needs to be
     *        queued
     * @param v reference to the stored value of the non-resident key
     *
     * @return the result of the operation
     */
    virtual GetValue getInternalNonResident(const DocKey& key,
                                            const void* cookie,
                                            EventuallyPersistentEngine& engine,
                                            int bgFetchDelay,
                                            QueueBgFetch queueBgFetch,
                                            const StoredValue& v) = 0;

    /**
     * Increase the expiration count global stats and in the vbucket stats
     */
    void incExpirationStat(ExpireBy source);

    void adjustCheckpointFlushTimeout(std::chrono::seconds wall_time);

    /**
     * Given a StoredValue with XATTRs - prune the user keys so only system keys
     * remain.
     *
     * @param v StoredValue with XATTR value
     * @param itemMeta New ItemMetaData to use in item creation
     * @return unique_ptr<Item> which matches the StoredValue's meta-data and
     *         has the XATTR value with only the system-keys. If the pruning
     *         removed all keys (because no system-keys exist) an empty
     *         unique_ptr is returned.
     */
    std::unique_ptr<Item> pruneXattrDocument(StoredValue& v,
                                             const ItemMetaData& itemMeta);

    /**
     * Estimate the new total memory usage with the allocation of an in-memory
     * instance for item
     *
     * @param st Reference to epstats
     * @param item Item that is being added
     *
     * @return new total size for this Bucket once Item is allocated
     */
    virtual size_t estimateNewMemoryUsage(EPStats& st, const Item& item) = 0;

    /*
     * Call the predicate with item_info from v (none if v is nullptr)
     * @param predicate a function to call, must be initialised
     * @param v the StoredValue (or nullptr if none in cache)
     * @return how the caller should proceed (store_if semantics)
     */
    cb::StoreIfStatus callPredicate(cb::StoreIfPredicate predicate,
                                    StoredValue* v);

    id_type                         id;
    std::atomic<vbucket_state_t>    state;
    cb::RWLock                      stateLock;
    vbucket_state_t                 initialState;
    std::mutex                           pendingOpLock;
    std::vector<const void*>        pendingOps;
    ProcessClock::time_point        pendingOpsStart;
    WeaklyMonotonic<uint64_t>       purge_seqno;
    std::atomic<bool>               takeover_backed_up;

    /* snapshotMutex is used to update/read the pair {start, end} atomically,
       but not if reading a single field. */
    mutable std::mutex snapshotMutex;
    uint64_t persisted_snapshot_start;
    uint64_t persisted_snapshot_end;

    std::mutex bfMutex;
    std::unique_ptr<BloomFilter> bFilter;
    std::unique_ptr<BloomFilter> tempFilter;    // Used during compaction.

    std::atomic<uint64_t> rollbackItemCount;

    HLC hlc;
    std::string statPrefix;
    // The persistence checkpoint ID for this vbucket.
    std::atomic<uint64_t> persistenceCheckpointId;
    // Flag to indicate the vbucket is being created
    std::atomic<bool> bucketCreation;
    // Flag to indicate the vbucket deletion is deferred
    std::atomic<bool> deferredDeletion;
    /// A cookie that can be set when the vbucket is deletion is deferred, the
    /// cookie will be notified when the deferred deletion completes
    const void* deferredDeletionCookie;

    // Ptr to the item conflict resolution module
    std::unique_ptr<ConflictResolution> conflictResolver;

    // A callback to be called when a new seqno is generated in the vbucket as
    // a result of a front end call
    NewSeqnoCallback newSeqnoCb;

    /// The VBucket collection state
    Collections::VB::Manifest manifest;

    /**
     * records if the vbucket has had xattrs documents written to it, note that
     * rollback of data or delete of all the xattr documents does not undo the
     * flag.
     */
    std::atomic<bool> mayContainXattrs;

    static cb::AtomicDuration chkFlushTimeout;

    static double mutationMemThreshold;

    friend class VBucketTest;

    DISALLOW_COPY_AND_ASSIGN(VBucket);
};

using VBucketPtr = std::shared_ptr<VBucket>;

/**
 * Represents a locked VBucket that provides RAII semantics for the lock.
 *
 * Behaves like the underlying shared_ptr - i.e. `operator->` is overloaded
 * to return the underlying VBucket.
 */
class LockedVBucketPtr {
public:
    LockedVBucketPtr(VBucketPtr vb, std::unique_lock<std::mutex>&& lock)
        : vb(std::move(vb)), lock(std::move(lock)) {
    }

    VBucket& operator*() const {
        return *vb;
    }

    VBucket* operator->() const {
        return vb.get();
    }

    explicit operator bool() const {
        return vb.operator bool();
    }

    VBucketPtr& getVB() {
        return vb;
    }

    /// Return true if this object owns the mutex.
    bool owns_lock() const {
        return lock.owns_lock();
    }

private:
    VBucketPtr vb;
    std::unique_lock<std::mutex> lock;
};
