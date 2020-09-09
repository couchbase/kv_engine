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

#include "bloomfilter.h"
#include "checkpoint_types.h"
#include "collections/collections_types.h"
#include "dcp/dcp-types.h"
#include "hash_table.h"
#include "hlc.h"
#include "monotonic.h"
#include "vbucket_fwd.h"
#include "vbucket_notify_context.h"

#include <folly/SynchronizedPtr.h>
#include <memcached/engine.h>
#include <nlohmann/json_fwd.hpp>
#include <platform/atomic_duration.h>
#include <platform/non_negative_counter.h>
#include <relaxed_atomic.h>
#include <atomic>
#include <list>
#include <queue>

class ActiveDurabilityMonitor;
struct CheckpointSnapshotRange;
class CheckpointManager;
class CheckpointConfig;
class ConflictResolution;
class Configuration;
class CompactionBGFetchItem;
struct DCPBackfillIface;
class DiskDocKey;
class DurabilityMonitor;
class EPStats;
class EventuallyPersistentEngine;
class GetValue;
class ItemMetaData;
class KVStore;
class PassiveDurabilityMonitor;
class PreLinkDocumentContext;
class RollbackResult;
class FrontEndBGFetchItem;
struct VBQueueItemCtx;
struct vbucket_transition_state;
struct vb_bgfetch_item_ctx_t;
using vb_bgfetch_queue_t =
        std::unordered_map<DiskDocKey, vb_bgfetch_item_ctx_t>;

template <typename... RV>
class Callback;

namespace Collections {
class Manifest;
}

namespace Collections::VB {
class CachingReadHandle;
class Manifest;
class ManifestEntry;
class ReadHandle;
class WriteHandle;
} // namespace Collections::VB

/**
 * Structure that holds seqno based or checkpoint persistence based high
 * priority requests to a vbucket
 */
struct HighPriorityVBEntry {
    HighPriorityVBEntry(const void* c,
                        uint64_t idNum,
                        HighPriorityVBNotify reqType)
        : cookie(c),
          id(idNum),
          reqType(reqType),
          start(std::chrono::steady_clock::now()) {
    }

    const void* cookie;
    uint64_t id;
    HighPriorityVBNotify reqType;

    /* for stats (histogram) */
    std::chrono::steady_clock::time_point start;
};

/**
 * Callback function to be invoked by ActiveDurabilityMonitor when SyncWrite(s)
 * are ready to be resolved (either met requirements and should be Committed, or
 * cannot meet requirements and should be Aborted).
 *
 * Will normally call the DurabilityCompletionTask to wake up and process
 * those resolved SyncWrites.
 */
using SyncWriteResolvedCallback = std::function<void(Vbid vbid)>;

/**
 * Callback function invoked when an accepted SyncWrite operation has been
 * completed (has been committed / aborted / times out).
 */
using SyncWriteCompleteCallback =
        std::function<void(const void* cookie, ENGINE_ERROR_CODE status)>;

/// Instance of SyncWriteCompleteCallback which does nothing.
const SyncWriteCompleteCallback NoopSyncWriteCompleteCb =
        [](const void* cookie, ENGINE_ERROR_CODE status) {};

/**
 * Callback function invoked at Replica for sending a SeqnoAck message to the
 * Active. That is triggered at Replica by High Prepared Seqno updates within
 * the PassiveDurabilityMonitor.
 */
using SeqnoAckCallback = std::function<void(Vbid vbid, int64_t seqno)>;

/// Instance of SeqnoAckCallback which does nothing.
const SeqnoAckCallback NoopSeqnoAckCb = [](Vbid vbid, int64_t seqno) {};

class EventuallyPersistentEngine;
class FailoverTable;
class KVShard;
class VBucketMemoryDeletionTask;

/**
 * An individual vbucket.
 */
class VBucket : public std::enable_shared_from_this<VBucket> {
public:

    enum class GetKeyOnly {
         Yes,
         No
     };

    enum class UseActiveVBMemThreshold { Yes, No };

    VBucket(Vbid i,
            vbucket_state_t newState,
            EPStats& st,
            CheckpointConfig& chkConfig,
            int64_t lastSeqno,
            uint64_t lastSnapStart,
            uint64_t lastSnapEnd,
            std::unique_ptr<FailoverTable> table,
            std::shared_ptr<Callback<Vbid>> flusherCb,
            std::unique_ptr<AbstractStoredValueFactory> valFact,
            NewSeqnoCallback newSeqnoCb,
            SyncWriteResolvedCallback syncWriteResolvedCb,
            SyncWriteCompleteCallback syncWriteCb,
            SeqnoAckCallback seqnoAckCb,
            Configuration& config,
            EvictionPolicy evictionPolicy,
            std::unique_ptr<Collections::VB::Manifest> manifest,
            vbucket_state_t initState = vbucket_state_dead,
            uint64_t purgeSeqno = 0,
            uint64_t maxCas = 0,
            int64_t hlcEpochSeqno = HlcCasSeqnoUninitialised,
            bool mightContainXattrs = false,
            const nlohmann::json* replTopology = {},
            uint64_t maxVisibleSeqno = 0);

    virtual ~VBucket();

    /**
     * Get the vBucket's high seqno. This is the sequence number of the highest
     * in-memory mutation the vBucket has performed.
     *
     * See also: getPersistenceSeqno(), getHighPreparedSeqno().
     */
    int64_t getHighSeqno() const;

    /**
     * Get the vBucket's high_prepared_seqno. This is the sequence number of
     * the highest prepared SyncWrite which has locally met its durability
     * requirements.
     */
    int64_t getHighPreparedSeqno() const;

    /**
     * Get the vBucket's High Completed Seqno. This is the sequence number of
     * the highest prepared SyncWrite which has been completed, i.e.:
     *
     * 1) whether the Prepare has globally met its durability requirements and
     *     has been committed by the active node
     * 2) or, timeout has triggered on Active for the Prepare and it has been
     *     aborted
     * 3) And (in either cases) all earlier SyncWrites have been completed.
     */
    int64_t getHighCompletedSeqno() const;

    size_t getChkMgrMemUsage() const;

    size_t getChkMgrMemUsageOfUnrefCheckpoints() const;

    size_t getChkMgrMemUsageOverhead() const;

    uint64_t getPurgeSeqno() const {
        return purge_seqno;
    }

    void setPurgeSeqno(uint64_t to) {
        purge_seqno = to;
    }

    void setPersistedSnapshot(const snapshot_range_t& range) {
        LockHolder lh(snapshotMutex);
        persistedRange = range;
    }

    snapshot_range_t getPersistedSnapshot() const {
        LockHolder lh(snapshotMutex);
        return persistedRange;
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

    /// Returns the current HLC time and which mode it is in.
    cb::HlcTime getHLCNow() const {
        return hlc.peekHLC();
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

    /**
     * @returns the cumulative number of SyncWrite operations accepted
     * (registered with DurabilityMonitor) for this vbucket.
     */
    size_t getSyncWriteAcceptedCount() const;

    /**
     * @returns the cumulative number of SyncWrite operations Committed
     * (successfully completed) for this vbucket.
     */
    size_t getSyncWriteCommittedCount() const;

    /**
     * @returns the cumulative number of SyncWrite operations Aborted
     * (did not successfully complete) for this vbucket.
     */
    size_t getSyncWriteAbortedCount() const;

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
        deferredDeletion.store(value);
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

    Vbid getId() const {
        return id;
    }
    vbucket_state_t getState() const { return state.load(); }

    /**
     * Sets the vbucket state to a desired state
     *
     * @param to desired vbucket state
     * @param meta optional meta information to apply alongside the state.
     */
    void setState(vbucket_state_t to, const nlohmann::json* meta = nullptr);

    /**
     * Sets the vbucket state to a desired state with the 'stateLock' already
     * acquired
     *
     * @param to desired vbucket state
     * @param meta optional meta information to apply alongside the state.
     * @param vbStateLock write lock holder on 'stateLock'
     */
    void setState_UNLOCKED(vbucket_state_t to,
                           const nlohmann::json* meta,
                           const folly::SharedMutex::WriteHolder& vbStateLock);

    auto& getStateLock() {
        return stateLock;
    }

    vbucket_state_t getInitialState() { return initialState; }

    vbucket_transition_state getTransitionState() const;

    /**
     * @return the replication topology set for this VBucket
     */
    nlohmann::json getReplicationTopology() const;

    /**
     * Enforce timeout for the expired SyncWrites in this VBucket.
     *
     * @param asOf The time to be compared with tracked-SWs' expiry-time
     */
    void processDurabilityTimeout(
            const std::chrono::steady_clock::time_point asOf);

    void notifySyncWritesPendingCompletion();

    /**
     * For all SyncWrites which the DurabilityMonitor has resolved (to be
     * committed or aborted), perform the appropriate operation - i.e.
     * actually perform the Commit / Abort operation.
     *
     * Typically called by the DurabilityCompletionTask.
     */
    void processResolvedSyncWrites();

    /**
     * This method performs operations on the stored value prior
     * to expiring the item.
     *
     * @param v the stored value
     */
    void handlePreExpiry(const HashTable::HashBucketLock& hbl, StoredValue& v);

    bool addPendingOp(const void *cookie);

    void doStatsForQueueing(const Item& item, size_t itemBytes);

    /**
     * Stores flush stats for deferred update after flush-success.
     */
    class AggregatedFlushStats {
    public:
        void accountItem(const Item& item);

        size_t getNumItems() const {
            return numItems;
        }

        size_t getTotalBytes() const {
            return totalBytes;
        }

        size_t getTotalAgeInMicro() const {
            return totalAgeInMicro;
        }

    private:
        size_t numItems = 0;
        size_t totalBytes = 0;
        size_t totalAgeInMicro = 0;
    };

    /**
     * Update flush stats after a flush batch has been persisted.
     * Args in input provide the necessary info about the flush batch.
     *
     * @param aggStats
     */
    void doAggregatedFlushStats(const AggregatedFlushStats& aggStats);

    void incrMetaDataDisk(const Item& qi);
    void decrMetaDataDisk(const Item& qi);

    /**
     * Increase the total count of items in this VBucket
     * @param numItemsAdded will be incremented the total item count with
     * default value of 1.
     */
    virtual void incrNumTotalItems(size_t numItemsAdded = 1) = 0;

    /**
     * Decrease the total count of items in this VBucket
     * @param numItemsRemoved will be decremented from the total item count with
     * default value of 1.
     */
    virtual void decrNumTotalItems(size_t numItemsRemoved = 1) = 0;

    /**
     * Set the total count of items in this VBucket to the specified value.
     */
    virtual void setNumTotalItems(size_t items) = 0;

    virtual size_t getNumTotalItems() const = 0;

    /// Reset all statistics assocated with this vBucket.
    virtual void resetStats();

    // Get age sum in millisecond
    uint64_t getQueueAge();

    void fireAllOps(EventuallyPersistentEngine &engine);

    /**
     * Get (and clear) the cookies for all in-flight SyncWrites from the ADM
     */
    std::vector<const void*> getCookiesForInFlightSyncWrites();

    /**
     * Prepare the transition away from active by doing necessary work in the
     * ADM.
     *
     * @return cookies for all in-flight SyncWrites so that clients can be
     *         notified
     */
    std::vector<const void*> prepareTransitionAwayFromActive();

    size_t size();

    // @todo: Remove this structure and use CM::ItemsForCursor, they are almost
    //  identical. That can be easily done after we have removed the reject
    //  queue, so we may want to do that within the reject queue removal.
    //  Doing as part of MB-37280 otherwise.
    struct ItemsToFlush {
        std::vector<queued_item> items;
        std::vector<CheckpointSnapshotRange> ranges;
        bool moreAvailable = false;
        std::optional<uint64_t> maxDeletedRevSeqno = {};
        CheckpointType checkpointType = CheckpointType::Memory;

        // See CM::ItemsForCursor for details.
        UniqueFlushHandle flushHandle;
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

    bool isReceivingInitialDiskSnapshot() {
        return receivingInitialDiskSnapshot.load();
    }

    void setReceivingInitialDiskSnapshot(bool receivingDiskSnapshot) {
        receivingInitialDiskSnapshot.store(receivingDiskSnapshot);
    }

    /// @return true if we are a replica receiving a disk based snapshot
    bool isReceivingDiskSnapshot() const;

    /**
     * Returns the map of bgfetch items for this vbucket, clearing the
     * pendingBGFetches.
     */
    virtual vb_bgfetch_queue_t getBGFetchItems() = 0;

    virtual bool hasPendingBGFetchItems() = 0;

    static const char* toString(vbucket_state_t s);

    static vbucket_state_t fromString(const char* state);

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

    virtual void addStats(VBucketStatsDetailLevel detail,
                          const AddStatFn& add_stat,
                          const void* c) = 0;

    /**
     * Output DurabiltyMonitor stats.
     *
     * @param addStat the callback to memcached
     * @param cookie
     */
    void addDurabilityMonitorStats(const AddStatFn& addStat,
                                   const void* cookie) const;

    /// Dump the internal state of the durabilityMonitor to the given stream.
    void dumpDurabilityMonitor(std::ostream& os) const;

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

    size_t getNumTempItems() {
        return ht.getNumTempItems();
    }

    /**
     * @returns the number of system items stored in this vbucket
     */
    virtual size_t getNumSystemItems() const = 0;

    void incrRollbackItemCount(uint64_t val) {
        rollbackItemCount.fetch_add(val, std::memory_order_relaxed);
    }

    uint64_t getRollbackItemCount() {
        return rollbackItemCount.load(std::memory_order_relaxed);
    }

    /**
     * Obtain the read handle for the collections manifest.
     * The caller will have read-only access to manifest using the methods
     * exposed by the ReadHandle
     */
    Collections::VB::ReadHandle lockCollections() const;

    /**
     * Obtain a caching read handle for the collections manifest.
     * The returned handle will lookup the collection associated with key
     * and cache the internal iterator so that future usage of
     * isLogicallyDeleted doesn't need to re-scan and lookup. This is different
     * to a plain ReadHandle which provides more functionality (more methods
     * for the caller), but may result in extra lookups and key-scans.
     * @param key A key to use for constructing the read handle.
     * @return a CachingReadHandle which the caller should test is valid with
     *         CachingReadHandle::valid
     */
    Collections::VB::CachingReadHandle lockCollections(const DocKey& key) const;

    /**
     * Update the Collections::VB::Manifest and the VBucket.
     * Adds SystemEvents for the create and delete of collections into the
     * checkpoint.
     *
     * @param m A Collections::Manifest to apply to the VB::Manifest
     * @param true if the update was successful
     */
    Collections::VB::ManifestUpdateStatus updateFromManifest(
            const Collections::Manifest& m);

    /**
     * Add a collection to this vbucket with a pre-assigned seqno. I.e.
     * this VB is a replica.
     *
     * @param uid the uid of the manifest which made the change
     * @param identifiers ScopeID and CollectionID pair
     * @param collectionName name of the added collection
     * @param maxTtl An optional maxTTL for the collection
     * @param bySeqno The seqno assigned to the collection create event.
     */
    void replicaAddCollection(Collections::ManifestUid uid,
                              ScopeCollectionPair identifiers,
                              std::string_view collectionName,
                              cb::ExpiryLimit maxTtl,
                              int64_t bySeqno);

    /**
     * Drop a collection from this vbucket with a pre-assigned seqno. I.e.
     * this VB is a replica.
     *
     * @param uid the uid of the manifest which made the change
     * @param cid CollectionID to drop
     * @param bySeqno The seqno assigned to the collection drop event.
     */
    void replicaDropCollection(Collections::ManifestUid uid,
                               CollectionID cid,
                               int64_t bySeqno);

    /**
     * Add a scope to this vbucket with a pre-assigned seqno. I.e. this VB is a
     * replica.
     *
     * @param uid the uid of the manifest which made the change
     * @param sid ScopeID of the scope
     * @param scopeName name of the added scope
     * @param bySeqno The seqno assigned to the scope create event.
     */
    void replicaAddScope(Collections::ManifestUid uid,
                         ScopeID sid,
                         std::string_view scopeName,
                         int64_t bySeqno);

    /**
     * Drop a scope from this vbucket with a pre-assigned seqno. I.e. this VB
     * is a replica.
     *
     * @param uid the uid of the manifest which made the change
     * @param sid ScopeID to drop
     * @param bySeqno The seqno assigned to the scope drop event.
     */
    void replicaDropScope(Collections::ManifestUid uid,
                          ScopeID sid,
                          int64_t bySeqno);

    /**
     * Get the collection manifest
     *
     * @return reference to the manifest
     */
    Collections::VB::Manifest& getManifest() {
        return *manifest;
    }

    /**
     * Get the collection manifest
     *
     * @return const reference to the manifest
     */
    const Collections::VB::Manifest& getManifest() const {
        return *manifest;
    }

    static const vbucket_state_t ACTIVE;
    static const vbucket_state_t REPLICA;
    static const vbucket_state_t PENDING;
    static const vbucket_state_t DEAD;

    HashTable ht;

    /// Manager of this vBucket's checkpoints. unique_ptr for pimpl.
    std::unique_ptr<CheckpointManager> checkpointManager;

    /**
     * Searches for a 'valid' StoredValue in the VBucket.
     *
     * Only looks in the in-memory HashTable; if fully-evicted returns false.
     *
     * The definition of 'valid' depends on the value of WantsDeleted: if a
     * deleted or expired item is found then returns nullptr, unless
     * WantsDeleted is Yes.
     * If an expired item is found then will enqueue a delete to clean up the
     * item unless QueueExpired is No.
     *
     * @param wantsDeleted
     * @param trackReference
     * @param queueExpired Delete an expired item
     * @param cHandle Collections readhandle (caching mode) for this key
     * @param fetchRequestedForReplicaItem bi-state enum to inform the method
     * if the fetch is for a GET_REPLICA, if so we should only fetch committed
     * values
     * @return a FindResult consisting of a pointer to the StoredValue (if
     * found) and the associated HashBucketLock which guards it.
     */
    HashTable::FindResult fetchValidValue(
            WantsDeleted wantsDeleted,
            TrackReference trackReference,
            QueueExpired queueExpired,
            const Collections::VB::CachingReadHandle& cHandle,
            ForGetReplicaOp fetchRequestedForReplicaItem = ForGetReplicaOp::No);

    /**
     * Result of the fetchValueForWrite() method.
     */
    struct FetchForWriteResult {
        enum class Status {
            /// Found an existing item with the given key.
            OkFound,
            /// No item found with this key, but key is available to write.
            /// `storedValue` set to nullptr, lock acquired ready for inserting.
            OkVacant,
            /// An item exists with the given key, however it cannot be accessed
            /// as a SyncWrite is in progress.
            /// storedValue set to nullptr, lock doesn't own anything.
            ESyncWriteInProgress,
        };
        Status status;

        /// status==OkFound then pointer to found StoredValue; else nullptr.
        StoredValue* storedValue = nullptr;
        /**
         * The (locked) HashBucketLock for the given key.
         * This returns a locked object for the 'Ok...' status codes (even if
         * the requested key doesn't exist) to facilitate use-cases where the
         * caller subsequently needs to insert a StoredValue for this key, to
         * avoid unlocking and re-locking the mutex.
         * For 'E...' status codes it is unlocked, as those are error states
         * and no valid lock to hold.
         */
        HashTable::HashBucketLock lock;
    };

    /**
     * Searches for a StoredValue in the VBucket to modify.
     *
     * Only looks in the in-memory HashTable; if fully-evicted returns false.
     *
     * If the item exists then returns OkFound and the StoredValue+lock.
     * If the item doesn't exist (but the key can be written to) returns
     * OkVacant and the lock for where that key would be (and nullptr).
     * Otherwise returns an error status code - ee FetchForWriteResult::Status
     * for details.
     *
     * If an expired item is found then will enqueue a delete to clean up the
     * item if QueueExpired is yes.
     *
     * @param cHandle Collections readhandle (caching mode) for this key.
     * @param wantsDeleted If Yes then deleted items will be returned,
     *        otherwise a deleted item is treated as non-existant (and will
     *        return nullptr).
     * @param queueExpired Delete an expired item
     * @return a FindResult consisting of a pointer to the StoredValue (if
     * found) and the associated HashBucketLock which guards it.
     */
    FetchForWriteResult fetchValueForWrite(
            const Collections::VB::CachingReadHandle& cHandle,
            QueueExpired queueExpired);

    /**
     * Searches for a Prepared SyncWrite in the VBucket.
     *
     * Only looks in the in-memory HashTable (Prepared items are never
     * evicted).
     *
     * @param cHandle Collections readhandle (caching mode) for this key
     * @return a FindResult consisting of a pointer to the StoredValue (if
     * found) and the associated HashBucketLock which guards it.
     */
    HashTable::FindResult fetchPreparedValue(
            const Collections::VB::CachingReadHandle& cHandle);

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
            const DiskDocKey& key,
            const FrontEndBGFetchItem& fetched_item,
            const std::chrono::steady_clock::time_point startTime) = 0;

    /**
     * Retrieve an item from the disk for vkey stats
     *
     * @param key the key to fetch
     * @param cookie the connection cookie
     * @param eviction_policy The eviction policy
     * @param engine Reference to ep engine
     *
     * @return VBReturnCtx indicates notifyCtx and operation result
     */
    virtual ENGINE_ERROR_CODE statsVKey(const DocKey& key,
                                        const void* cookie,
                                        EventuallyPersistentEngine& engine) = 0;

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
    MutationStatus setFromInternal(const Item& itm);

    /**
     * Set (add new or update) an item in the vbucket.
     *
     * @param itm Item to be added or updated. Upon success, the itm
     *            bySeqno, cas and revSeqno are updated
     * @param cookie the connection cookie
     * @param engine Reference to ep engine
     * @param predicate a function to call which if returns true, the set will
     *        succeed. The function is called against any existing item.
     * @param readHandle Collections readhandle (caching mode) for this key
     *
     * @return ENGINE_ERROR_CODE status notified to be to the front end
     */
    ENGINE_ERROR_CODE set(Item& itm,
                          const void* cookie,
                          EventuallyPersistentEngine& engine,
                          cb::StoreIfPredicate predicate,
                          const Collections::VB::CachingReadHandle& cHandle);

    /**
     * Replace (overwrite existing) an item in the vbucket.
     *
     * @param itm Item to be added or updated. Upon success, the itm
     *            bySeqno, cas and revSeqno are updated
     * @param cookie the connection cookie
     * @param engine Reference to ep engine
     * @param predicate a function to call which if returns true, the replace
     *        will succeed. The function is called against any existing item.
     * @param cHandle Collections readhandle (caching mode) for this key
     *
     * @return ENGINE_ERROR_CODE status notified to be to the front end
     */
    ENGINE_ERROR_CODE replace(
            Item& itm,
            const void* cookie,
            EventuallyPersistentEngine& engine,
            cb::StoreIfPredicate predicate,
            const Collections::VB::CachingReadHandle& cHandle);

    /**
     * Set an item in the store from a non-front end operation (DCP, XDCR)
     *
     * @param item the item to set. Upon success, the itm revSeqno is updated
     * @param cas value to match
     * @param seqno sequence number of mutation
     * @param cookie the cookie representing the client to store the item
     * @param engine Reference to ep engine
     * @param checkConflicts should conflict resolution be done?
     * @param allowExisting set to false if you want set to fail if the
     *                      item exists already
     * @param genBySeqno whether or not to generate sequence number
     * @param genCas
     * @param cHandle Collections readhandle (caching mode) for this key
     *
     * @return the result of the store operation
     */
    ENGINE_ERROR_CODE setWithMeta(
            Item& itm,
            uint64_t cas,
            uint64_t* seqno,
            const void* cookie,
            EventuallyPersistentEngine& engine,
            CheckConflicts checkConflicts,
            bool allowExisting,
            GenerateBySeqno genBySeqno,
            GenerateCas genCas,
            const Collections::VB::CachingReadHandle& cHandle);

    ENGINE_ERROR_CODE prepare(
            Item& itm,
            uint64_t cas,
            uint64_t* seqno,
            const void* cookie,
            EventuallyPersistentEngine& engine,
            CheckConflicts checkConflicts,
            bool allowExisting,
            GenerateBySeqno genBySeqno,
            GenerateCas genCas,
            const Collections::VB::CachingReadHandle& cHandle);

    /**
     * Delete an item in the vbucket
     *
     * @param[in,out] cas value to match; new cas after logical delete
     * @param cookie the cookie representing the client to store the item
     * @param engine Reference to ep engine
     * @param durability Optional durability requirements for this deletion.
     * @param[out] itemMeta pointer to item meta data that needs to be returned
     *                      as a result the delete. A NULL pointer indicates
     *                      that no meta data needs to be returned.
     * @param[out] mutInfo Info to uniquely identify (and order) the delete
     *                     seq. A NULL pointer indicates no info needs to be
     *                     returned.
     * @param cHandle Collections readhandle (caching mode) for this key
     *
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE deleteItem(
            uint64_t& cas,
            const void* cookie,
            EventuallyPersistentEngine& engine,
            std::optional<cb::durability::Requirements> durability,
            ItemMetaData* itemMeta,
            mutation_descr_t& mutInfo,
            const Collections::VB::CachingReadHandle& cHandle);

    /**
     * Delete an item in the vbucket from a non-front end operation (DCP, XDCR)
     *
     * @param[in, out] cas value to match; new cas after logical delete
     * @param[out] seqno Pointer to get the seqno generated for the item. A
     *                   NULL value is passed if not needed
     * @param cookie the cookie representing the client to store the item
     * @param engine Reference to ep engine
     * @param checkConflicts should conflict resolution be done?
     * @param itemMeta ref to item meta data
     * @param genBySeqno whether or not to generate sequence number
     * @param generateCas whether or not to generate cas
     * @param bySeqno seqno of the key being deleted
     * @param cHandle Collections readhandle (caching mode) for this key
     * @param deleteSource The source of the deletion, which if TTL triggers the
     *                     expiration path.
     *
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE deleteWithMeta(
            uint64_t& cas,
            uint64_t* seqno,
            const void* cookie,
            EventuallyPersistentEngine& engine,
            CheckConflicts checkConflicts,
            const ItemMetaData& itemMeta,
            GenerateBySeqno genBySeqno,
            GenerateCas generateCas,
            uint64_t bySeqno,
            const Collections::VB::CachingReadHandle& cHandle,
            DeleteSource deleteSource);

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
     * @param[out] msg Updated to point to a string (with static duration)
     *                 describing the result of the operation.
     * @param cHandle Collections readhandle (caching mode) for this key
     *
     * @return SUCCESS if key was successfully evicted (or was already
     *                 evicted), or the reason why the request failed.
     *
     */
    virtual cb::mcbp::Status evictKey(
            const char** msg,
            const Collections::VB::CachingReadHandle& cHandle) = 0;

    /**
     * Page out a StoredValue from memory.
     *
     * The definition of "page out" is up to the underlying VBucket
     * implementation - this may mean simply ejecting the value from memory
     * (Value Eviction), removing the entire document from memory (Full
     * Eviction), or actually deleting the document (Ephemeral Buckets).
     *
     * @param readHandle Collections ReadHandle required by ephemeral as
     *                   paging out may result in deletions that increment the
     *                   high seqno for the collection.
     * @param lh Bucket lock associated with the StoredValue.
     * @param v[in, out] Ref to the StoredValue to be ejected. Based on the
     *                   VBucket type, policy in the vbucket contents of v and
     *                   v itself may be changed
     *
     * @return true if an item is ejected.
     */
    virtual bool pageOut(const Collections::VB::ReadHandle& readHandle,
                         const HashTable::HashBucketLock& lh,
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
     * Check how much memory could be reclaimed if every resident item
     * were evicted.
     * Note, the real amount of reclaimable memory will be lower than this,
     * as certain items (prepares, dirty items, system events,
     * deletes for ephemeral) cannot be evicted.
     */
    virtual size_t getPageableMemUsage() = 0;

    /**
     * Add an item in the store
     *
     * @param itm the item to add. On success, this will have its seqno and
     *            CAS updated.
     * @param cookie the cookie representing the client to store the item
     * @param engine Reference to ep engine
     * @param cHandle Collections readhandle (caching mode) for this key
     *
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE add(Item& itm,
                          const void* cookie,
                          EventuallyPersistentEngine& engine,
                          const Collections::VB::CachingReadHandle& cHandle);

    /**
     * Retrieve a value, but update its TTL first
     *
     * @param cookie the connection cookie
     * @param engine Reference to ep engine
     * @param exptime the new expiry time for the object
     * @param cHandle Collections readhandle (caching mode) for this key
     *
     * @return a GetValue representing the result of the request
     */
    GetValue getAndUpdateTtl(const void* cookie,
                             EventuallyPersistentEngine& engine,
                             time_t exptime,
                             const Collections::VB::CachingReadHandle& cHandle);
    /**
     * Add a system event Item to the vbucket and return its seqno. Does
     * not set the collection high seqno of the item as that requires a read
     * lock but this is called from within a write lock scope. Also, it does not
     * make sense to attempt to update a collection high seqno for certain
     * events, such as scope creations and deletions.
     *
     * Ephemeral vs persistent buckets implement this function differently
     *
     * @param wHandle A collections manifest write handle required to ensure we
     *        lock correctly around VBucket::notifyNewSeqno.
     * @param item an Item object to queue, can be any kind of item and will be
     *        given a CAS and seqno by this function.
     * @param seqno An optional sequence number, if not specified checkpoint
     *        queueing will assign a seqno to the Item.
     * @param cid The collection ID that this system event is concerned with.
     *        Optional as this may be a scope system event.
     * @param wHandle Collections write handle under which this operation is
     *        locked.
     */
    virtual uint64_t addSystemEventItem(
            Item* item,
            OptionalSeqno seqno,
            std::optional<CollectionID> cid,
            const Collections::VB::WriteHandle& wHandle) = 0;

    /**
     * Get metadata and value for a given key
     *
     * @param cookie the cookie representing the client
     * @param engine Reference to ep engine
     * @param options flags indicating some retrieval related info
     * @param getKeyOnly if GetKeyOnly::Yes we want only the key
     * @param cHandle Collections readhandle (caching mode) for this key
     * @param getReplicaItem bi-state enum to state of this get internal
     * is being executed to find a replica item.
     *
     * @return the result of the operation
     */
    GetValue getInternal(const void* cookie,
                         EventuallyPersistentEngine& engine,
                         get_options_t options,
                         GetKeyOnly getKeyOnly,
                         const Collections::VB::CachingReadHandle& cHandle,
                         ForGetReplicaOp getReplicaItem = ForGetReplicaOp::No);

    /**
     * Retrieve the meta data for given key
     *
     * @param cookie the connection cookie
     * @param engine Reference to ep engine
     * @param cHandle Collections readhandle (caching mode) for this key
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
            const Collections::VB::CachingReadHandle& cHandle,
            ItemMetaData& metadata,
            uint32_t& deleted,
            uint8_t& datatype);

    /**
     * Looks up the key stats for the given {vbucket, key}.
     *
     * @param cookie The client's cookie
     * @param engine Reference to ep engine
     * @param[out] kstats On success the keystats for this item.
     * @param wantsDeleted If yes then return keystats even if the item is
     *                     marked as deleted. If no then will return
     *                     ENGINE_KEY_ENOENT for deleted items.
     * @param cHandle Collections readhandle (caching mode) for this key
     *
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE getKeyStats(
            const void* cookie,
            EventuallyPersistentEngine& engine,
            struct key_stats& kstats,
            WantsDeleted wantsDeleted,
            const Collections::VB::CachingReadHandle& cHandle);

    /**
     * Gets a locked item for a given key.
     *
     * @param currentTime Current time to use for locking the item for a
     *                    duration of lockTimeout
     * @param lockTimeout Timeout for the lock on the item
     * @param cookie The client's cookie
     * @param engine Reference to ep engine
     * @param cHandle Collections readhandle (caching mode) for this key
     *
     * @return the result of the operation (contains locked item on success)
     */
    GetValue getLocked(rel_time_t currentTime,
                       uint32_t lockTimeout,
                       const void* cookie,
                       EventuallyPersistentEngine& engine,
                       const Collections::VB::CachingReadHandle& cHandle);

    /**
     * Perform a commit against the given pending Sync Write.
     *
     * @param key Key to commit
     * @param prepareSeqno The sequence number of the existing pending
     *                      SyncWrite
     * @param commitSeqno Optional commit sequence number to use for the commit.
     *                    If omitted then a sequence number will be generated
     *                    by the CheckpointManager.
     * @param cookie (Optional) The cookie representing the client connection,
     *     must be provided if the operation needs to be notified to a client
     */
    ENGINE_ERROR_CODE commit(const DocKey& key,
                             uint64_t prepareSeqno,
                             std::optional<int64_t> commitSeqno,
                             const Collections::VB::CachingReadHandle& cHandle,
                             const void* cookie = nullptr);

    /**
     * Perform an abort against the given pending Sync Write.
     *
     * @param key Key to abort
     * @param prepareSeqno The sequence number of the existing pending
     *     SyncWrite
     * @param abortSeqno Optional abort sequence number to use for the abort.
     *     If omitted then a sequence number will be generated by the
     *     CheckpointManager.
     * @param cHandle The collections handle
     * @param cookie (Optional) The cookie representing the client connection,
     *     must be provided if the operation needs to be notified to a client
     */
    ENGINE_ERROR_CODE abort(const DocKey& key,
                            uint64_t prepareSeqno,
                            std::optional<int64_t> abortSeqno,
                            const Collections::VB::CachingReadHandle& cHandle,
                            const void* cookie = nullptr);

    /**
     * Notify the ActiveDurabilityMonitor that a SyncWrite has been locally
     * accepted into memory, and if that SyncWrite has met durability
     * requirements then Commit it.
     *
     * Expected to be called after VBucket SyncWrite mutation methods
     * (VBucket::set, add, replace...) once the Prepare has been successfully
     * added to the VBucket.
     *
     * The following locks should *not* be held during this call or it may
     * result in deadlock (lock order inversion):
     * - the Collections Manifest lock (exclusive or shared)
     * - Any HashBucketLock.
     *
     * This allows us to do "durable" sets in the case where we have no
     * replicas (i.e. every set should be Committed immediately).
     * We can't do this when we add the SyncWrite because the general use case
     * is to commit on replica ack, which requires doing a find against the
     * HashTable (requires locking the HashBucket) which would result in a
     * lock-order inversion if we did it inside the addSyncWrite call.
     */
    void notifyActiveDMOfLocalSyncWrite();

    /**
     * Notify a client connection that the processing of a SyncWrite has been
     * completed.
     *
     * @param cookie The client's cookie
     * @param result The result of the SyncWrite processing
     */
    void notifyClientOfSyncWriteComplete(const void* cookie,
                                         ENGINE_ERROR_CODE result);

    /**
     * Notify the PassiveDM that the snapshot-end mutation for the currently
     * processed snapshot has been received.
     * The PassiveDM uses the last snapshot-end seqno for enforcing some
     * snapshot-boundary rules at HPS updates.
     *
     * @param snapEnd The seqno of the last snapshot-end mutation received over
     *     the PassiveStream
     */
    void notifyPassiveDMOfSnapEndReceived(uint64_t snapEnd);

    /**
     * Send a SeqnoAck message on the PassiveStream (if any) for this VBucket.
     *
     * @param seqno The payload
     */
    void sendSeqnoAck(int64_t seqno);

    /**
     * Update in memory data structures after an item is deleted on disk
     *
     * @param queuedItem reference to the deleted item
     * @param deleted indicates if item actaully deleted or not (in case item
     *                did not exist on disk)
     */
    void deletedOnDiskCbk(const Item& queuedItem, bool deleted);

    /**
     * Remove the given Item from the in memory data structures
     * (after a rollback on disk).
     *
     * @param item To remove from memory.
     * @return indicates if the operation is succcessful
     */
    bool removeItemFromMemory(const Item& item);

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
    virtual std::unique_ptr<DCPBackfillIface> createDCPBackfill(
            EventuallyPersistentEngine& e,
            std::shared_ptr<ActiveStream> stream,
            uint64_t startSeqno,
            uint64_t endSeqno) = 0;

    /**
     * Creates a DCP backfill object for retrieving the given collection
     *
     * @param e ref to EventuallyPersistentEngine
     * @param stream Shared ptr to the stream for which this backfill obj is
     *               created
     * @param cid CollectionID to scan for
     *
     * @return pointer to the backfill object created. Caller to own this
     *         object and hence must handle deletion.
     */
    virtual std::unique_ptr<DCPBackfillIface> createDCPBackfill(
            EventuallyPersistentEngine& e,
            std::shared_ptr<ActiveStream> stream,
            CollectionID cid) = 0;

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
     * Update collections following a rollback
     *
     * @param kvstore A KVStore that is used for retrieving stored metadata
     */
    void collectionsRolledBack(KVStore& kvstore);

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
     * Implementation dependent method called by the collections erasing code
     *
     * @param bySeqno The seqno of the key to drop
     * @param cHandle Collections readhandle (caching mode) for this key
     */
    virtual void dropKey(int64_t bySeqno,
                         Collections::VB::CachingReadHandle& cHandle) = 0;

    /**
     * Drops the key from the DM so we can purge a collection
     *
     * @param key pending key
     * @param seqno The seqno of the pending key to drop
     */
    void dropPendingKey(const DocKey& key, int64_t seqno);

    /**
     * Get the number of deleted items that are "persisted".
     * Note1: This stat is used by ns_server during takeover.
     *
     * Note2: the virtual method allows ephemeral vb to return something
     * logically equivalent
     *
     * @returns the number of deletes which are persisted
     */
    virtual size_t getNumPersistedDeletes() const = 0;

    static std::chrono::seconds getCheckpointFlushTimeout();

    /**
     * Set the memory threshold on the current bucket quota for accepting a
     * new mutation. This is same across all the vbuckets
     *
     * @param memThreshold Threshold between 0 and 100, value is converted to
     *        double and divided by 100, e.g. 90 sets a value of 0.9
     */
    static void setMutationMemoryThreshold(size_t memThreshold);

    /**
     * Check if this StoredValue has become logically non-existent.
     * By logically non-existent, the item has been deleted
     * or doesn't exist
     *
     * @param v StoredValue to check
     * @param cHandle Collections readhandle (caching mode) for this key
     * @return true if the item is logically non-existent,
     *         false otherwise
     */
    static bool isLogicallyNonExistent(
            const StoredValue& v,
            const Collections::VB::CachingReadHandle& cHandle);

    /**
     * Helper function to validate the specified setVbucketState meta
     * information.
     * @returns An empty string if the information is valid,
     * otherwise string describing the (first) validation failure.
     */
    static std::string validateSetStateMeta(const nlohmann::json& meta);

    /**
     * Helper function to validate the specified vBucket replication topology.
     * @param topology A JSON array of replicaton chains
     * @returns An empty string if the information is valid,
     * otherwise string describing the (first) validation failure.
     */
    static std::string validateReplicationTopology(
            const nlohmann::json& topology);

    /**
     * Inform the vBucket that sequence number(s) have been acknowledged by
     * a replica node.
     *
     * @param vbStateLock read lock on the vBucket state.
     * @param replicaId The replica node which has acknowledged.
     * @param preparedSeqno The sequence number the replica has prepared up to.
     */
    ENGINE_ERROR_CODE seqnoAcknowledged(
            const folly::SharedMutex::ReadHolder& vbStateLock,
            const std::string& replicaId,
            uint64_t preparedSeqno);

    /**
     * Notify the DurabilityMonitor that the Flusher has persisted all the
     * items remaining for this VBucket.
     */
    void notifyPersistenceToDurabilityMonitor();

    /**
     * @return a const reference to the current Durability Monitor.
     */
    const DurabilityMonitor& getDurabilityMonitor() const;

    /**
     * Rollback callback function to add a new pending SyncWrite
     *
     * @param item the prepare to add
     */
    void addSyncWriteForRollback(const Item& item);

    /**
     * Remove any queued acks for the given node from the ActiveDM.
     * Note that we can remove acks only from the ActiveDM, so we need lock the
     * vbstate to prevent a concurrent state change active->non-active
     *
     * @param node Name of the node for which we wish to remove the ack
     * @param vbstateLock Exclusive lock to vbstate
     */
    void removeAcksFromADM(const std::string& node,
                           const folly::SharedMutex::WriteHolder& vbstateLock);

    /**
     * Remove any queued acks for the given node from the ActiveDM.
     * Note that we can remove acks only from the ActiveDM, so we need lock the
     * vbstate to prevent a concurrent state change active->non-active
     *
     * @param node Name of the node for which we wish to remove the ack
     * @param vbstateLock Shared lock to vbstate
     */
    void removeAcksFromADM(const std::string& node,
                           const folly::SharedMutex::ReadHolder& vbstateLock);

    /**
     * Set the window for which a duplicate prepare may be valid. This is any
     * currently outstanding prepare.
     */
    void setDuplicatePrepareWindow();

    /**
     * @return the maximum visible seqno for the vbucket
     */
    uint64_t getMaxVisibleSeqno() const;

    virtual void saveDroppedCollection(
            CollectionID cid,
            Collections::VB::WriteHandle& writeHandle,
            const Collections::VB::ManifestEntry& droppedEntry,
            uint64_t droppedSeqno) = 0;

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
     * Should SyncWrites be blocked (isDurabilityPossible() return false) if
     * there are more than N replicas configured?
     * Workaround for known issue with failover / rollback - see MB-34453 /
     * MB-34150.
     */
    const size_t maxAllowedReplicasForSyncWrites;

    /**
     * A custom delete function for deleting VBucket objects. Any thread could
     * be the last thread to release a VBucketPtr and deleting a VB will
     * eventually hit the I/O sub-system when we unlink the file, to be sure no
     * front-end thread does this work, we schedule the deletion to a background
     * task. This task scheduling is triggered by the shared_ptr/VBucketPtr
     * using this object as the deleter.
     */
    struct DeferredDeleter {
        explicit DeferredDeleter(EventuallyPersistentEngine& engine)
            : engine(engine) {
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
            const Collections::VB::CachingReadHandle& cHandle);
    /**
     * This function checks cas, expiry and other partition (vbucket) related
     * rules before setting an item into other in-memory structure like HT,
     * and checkpoint mgr. This function assumes that HT bucket lock is grabbed.
     *
     * Prevents operations on in-flight SyncWrites.
     * Redirects the addition of new prepares to addNewStoredValue.
     *
     * @param htRes Committed and Pending StoredValues, include HBL.
     * @param v Reference to the ptr of StoredValue to modify. This can be
     *          changed if a new StoredValue is added or just its contents is
     *          changes if the existing StoredValue is updated.
     * @param itm Item to be added/updated. On success, its revSeqno is updated
     * @param cas value to match
     * @param allowExisting set to false if you want set to fail if the
     *                      item exists already
     * @param hasMetaData
     * @param queueItmCtx holds info needed to queue an item in chkpt
     * @param storeIfStatus the status of any conditional store predicate
     * @param maybeKeyExists true if the key /may/ exist on disk (as an active,
     *                       alive document). Only valid if `v` is null.
     *
     * @return Result indicating the status of the operation and notification
     *                info (if operation was successful).
     */
    std::pair<MutationStatus, std::optional<VBNotifyCtx>> processSet(
            HashTable::FindUpdateResult& htRes,
            StoredValue*& v,
            Item& itm,
            uint64_t cas,
            bool allowExisting,
            bool hasMetaData,
            const VBQueueItemCtx& queueItmCtx,
            cb::StoreIfStatus storeIfStatus,
            bool maybeKeyExists = true);

    /**
     * Inner function for processSet. Allows overwriting of in-flight prepares.
     */
    std::pair<MutationStatus, std::optional<VBNotifyCtx>> processSetInner(
            HashTable::FindUpdateResult& htRes,
            StoredValue*& v,
            Item& itm,
            uint64_t cas,
            bool allowExisting,
            bool hasMetaData,
            const VBQueueItemCtx& queueItmCtx,
            cb::StoreIfStatus storeIfStatus,
            bool maybeKeyExists = true);

    /**
     * This function checks cas, expiry and other partition (vbucket) related
     * rules before adding an item into other in-memory structure like HT,
     * and checkpoint mgr. This function assumes that HT bucket lock is grabbed.
     *
     * @param htRes Committed and Pending StoredValues
     * @param v[in, out] the stored value to do this operation on
     * @param itm Item to be added/updated. On success, its revSeqno is updated
     * @param queueItmCtx holds info needed to queue an item in chkpt
     * @param cHandle Collections readhandle (caching mode) for this key
     *
     * @return Result indicating the status of the operation and notification
     *                info (if the operation was successful).
     */
    std::pair<AddStatus, std::optional<VBNotifyCtx>> processAdd(
            HashTable::FindUpdateResult& htRes,
            StoredValue*& v,
            Item& itm,
            bool maybeKeyExists,
            const VBQueueItemCtx& queueItmCtx,
            const Collections::VB::CachingReadHandle& cHandle);

    /**
     * This function checks cas, eviction policy and other partition
     * (vbucket) related rules before logically (soft) deleting an item in
     * in-memory structure like HT, and checkpoint mgr.
     * Assumes that HT bucket lock is grabbed.
     *
     * @param htRes Committed and Pending StoredValues
     * @param v Reference to the StoredValue to delete (in the general case)
     * @param cas the expected CAS of the item (or 0 to override)
     * @param metadata ref to item meta data
     * @param queueItmCtx holds info needed to queue an item in chkpt
     * @param use_meta Indicates if v must be updated with the metadata
     * @param bySeqno seqno of the key being deleted
     * @param deleteSource The source of the deletion
     *
     * @return pointer to the updated StoredValue. It can be same as that of
     *         v or different value if a new StoredValue is created for the
     *         update.
     *         status of the operation.
     *         notification info, if status was successful.
     */
    std::tuple<MutationStatus, StoredValue*, std::optional<VBNotifyCtx>>
    processSoftDelete(HashTable::FindUpdateResult& htRes,
                      StoredValue& v,
                      uint64_t cas,
                      const ItemMetaData& metadata,
                      const VBQueueItemCtx& queueItmCtx,
                      bool use_meta,
                      uint64_t bySeqno,
                      DeleteSource deleteSource);
    /**
     * Inner function for processSoftDelete. Allows overwriting of in-flight
     * prepares.
     */
    std::tuple<MutationStatus, StoredValue*, std::optional<VBNotifyCtx>>
    processSoftDeleteInner(const HashTable::HashBucketLock& hbl,
                           StoredValue& v,
                           uint64_t cas,
                           const ItemMetaData& metadata,
                           const VBQueueItemCtx& queueItmCtx,
                           bool use_meta,
                           uint64_t bySeqno,
                           DeleteSource deleteSource);

    /**
     * Delete a key (associated StoredValue) from ALL in-memory data structures
     * like HT.
     * Does NOT queue a mutation to the checkpoint manager, so this deletion
     * will not be persited to disk / written to replicas.
     * Expected usage is to reconcile HashTable with current VBucket state.
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
     * Enqueue an item for persistence and replication.
     * Other actions can be performed depending on the context passed in input.
     *
     * @param hbl The lock to the HashTable-bucket containing the StoredValue
     * @param v The dirty StoredValue.
     * @param ctx The VBQueueItemCtx. Holds info needed to enqueue the item,
     *     look at the structure for details.
     * @return the notification context used for notifying the Flusher and
     *     Replica Connections.
     */
    VBNotifyCtx queueDirty(const HashTable::HashBucketLock& hbl,
                           StoredValue& v,
                           const VBQueueItemCtx& ctx);

    /**
     * Enqueue an Abort item for persistence and replication.
     * An Abort item is a logical delete of a Pending SyncWrite that could not
     * be completed within the required Timeout requirement.
     *
     * @param hbl The lock to the HashTable-bucket containing the StoredValue
     * @param v The StoredValue of the Pending being aborted.
     * @param prepareSeqno The seqno of the Prepare being aborted
     * @param ctx The VBQueueItemCtx. Holds info needed to enqueue the item,
     *     look at the structure for details.
     * @return the notification context used for notifying the Flusher and
     *     Replica Connections.
     */
    VBNotifyCtx queueAbort(const HashTable::HashBucketLock& hbl,
                           const StoredValue& v,
                           int64_t prepareSeqno,
                           const VBQueueItemCtx& ctx);

    /**
     * Enqueue an new Abort item for persistence and replication. Needed when
     * the prepare it aborts has _not_ already been received, so there is no
     * prepared stored value.
     * An Abort item is a logical delete of a Pending SyncWrite that could not
     * be completed within the required Timeout requirement.
     *
     * @param item the aborted item to queue
     * @param ctx The VBQueueItemCtx. Holds info needed to enqueue the item,
     *     look at the structure for details.
     * @return the notification context used for notifying the Flusher and
     *     Replica Connections.
     */
    VBNotifyCtx queueAbortForUnseenPrepare(queued_item item,
                                           const VBQueueItemCtx& ctx);

    /**
     * Construct a new aborted item. Needed if the prepare which is
     * being aborted was not received due to deduplication (replica).
     *
     * @param key the key for which the abort should be created
     * @param prepareSeqno The seqno of the Prepare being aborted
     * @param abortSeqno The desired seqno of the abort
     * @return the abort item
     */
    queued_item createNewAbortedItem(const DocKey& key,
                                     int64_t prepareSeqno,
                                     int64_t abortSeqno);

    struct AddTempSVResult {
        TempAddStatus status;
        StoredValue* storedValue;
    };

    /**
     * Adds a temporary StoredValue in in-memory data structures like HT.
     * Assumes that HT bucket lock is grabbed.
     *
     * @param hbl Hash table bucket lock that must be held
     * @param key the key for which a temporary item needs to be added
     *
     * @return Result indicating the status of the operation. If successful
     *         (BgFetch) then includes the pointer to the created temp item.
     */
    AddTempSVResult addTempStoredValue(const HashTable::HashBucketLock& hbl,
                                       const DocKey& key);

    /**
     * Internal wrapper function around the callback to be called when a new
     * seqno is generated in the vbucket.
     *
     * @param notifyCtx holds info needed for notification
     */
    void notifyNewSeqno(const VBNotifyCtx& notifyCtx);

    /**
     * Perform the post-queue collections stat counting using the caching read
     * handle.
     *
     * @param cHandle read handle for the collection that the item causing
     *        the generation of a newSeqno belongs to
     * @param notifyCtx holds info needed for stat counting
     */
    void doCollectionsStats(const Collections::VB::CachingReadHandle& cHandle,
                            const VBNotifyCtx& notifyCtx);

    /**
     * Perform the post-queue collections stat counting using a read handle and
     * a given CollectionID.
     *
     * @param readHandle read handle for the entire collection manifest that
     *        allows us to lookup a collection then set the high seqno for it
     * @param collection the collection we need to update
     * @param notifyCtx holds info needed for stat counting
     */
    void doCollectionsStats(const Collections::VB::ReadHandle& readHandle,
                            CollectionID collection,
                            const VBNotifyCtx& notifyCtx);

    /**
     * Perform the post-queue collections stat counting using a write handle and
     * a given CollectionID.
     *
     * @param writeHandle write handle for the entire collection manifest that
     *        allows us to lookup a collection then set the high seqno for it
     * @param collection the collection we need to update
     * @param notifyCtx holds info needed for stat counting
     */
    void doCollectionsStats(const Collections::VB::WriteHandle& writeHandle,
                            CollectionID collection,
                            const VBNotifyCtx& notifyCtx);

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
     * @param useActiveVBMemThreshold whether active VB memory threshold
     *                                should be used regardless of VB state.
     *
     * @return True if there is memory for the item; else False
     */
    bool hasMemoryForStoredValue(
            EPStats& st,
            const Item& item,
            UseActiveVBMemThreshold useActiveVBMemThrehsold =
                    UseActiveVBMemThreshold::No);

    void _addStats(VBucketStatsDetailLevel detail,
                   const AddStatFn& add_stat,
                   const void* c);

    template <typename T>
    void addStat(const char* nm,
                 const T& val,
                 const AddStatFn& add_stat,
                 const void* c);

    /* This member holds the eviction policy used */
    const EvictionPolicy eviction;

    /* Reference to global (EP engine wide) stats */
    EPStats& stats;

    /* last seqno that is persisted on the disk */
    std::atomic<uint64_t> persistenceSeqno;

    /* holds all high priority async requests to the vbucket */
    std::list<HighPriorityVBEntry> hpVBReqs;

    /* synchronizes access to hpVBReqs */
    std::mutex hpVBReqsMutex;

    /* size of list hpVBReqs (to avoid MB-9434) */
    cb::RelaxedAtomic<size_t> numHpVBReqs;

    /// Tracks SyncWrites and determines when they should be committed /
    /// aborted.
    /// Guarded by the stateLock - read for access (dereferencing pointer),
    /// write for modifying what the pointer points to.
    std::unique_ptr<DurabilityMonitor> durabilityMonitor;

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

    /**
     * Updates the replication topology and propagates the new topology to
     * the DurabilityMonitor.
     * A new DurabilityMonitor instance may be instantiated in this function,
     * depending on the current VBucket::state and any previous
     * durabilityMonitor.
     *
     * @param topology The new topology, null if no toplogy was specified.
     */
    void setupSyncReplication(const nlohmann::json* topology);

    /**
     * @return a reference (if valid, i.e. vbstate=active) to the Active DM
     */
    ActiveDurabilityMonitor& getActiveDM();

    /**
     * @return a reference (if valid, i.e. vbstate=replica) to the Passive DM
     */
    PassiveDurabilityMonitor& getPassiveDM();

    /**
     * Increase the expiration count global stats and in the vbucket stats
     */
    void incExpirationStat(ExpireBy source);

    /**
     * This function handles expiry related stuff before logically (soft)
     * deleting an item in in-memory structures like HT, and checkpoint mgr.
     * Assumes that HT bucket lock is grabbed.
     *
     * @param hbl Hash table bucket lock that must be held
     * @param v Reference to the StoredValue to be soft deleted
     * @param cHandle Collections readhandle (caching mode) for this key
     *
     * @return status of the operation.
     *         pointer to the updated StoredValue. It can be same as that of
     *         v or different value if a new StoredValue is created for the
     *         update.
     *         notification info.
     */
    std::tuple<MutationStatus, StoredValue*, VBNotifyCtx> processExpiredItem(
            const HashTable::HashBucketLock& hbl,
            StoredValue& v,
            const Collections::VB::CachingReadHandle& cHandle);

private:
    void fireAllOps(EventuallyPersistentEngine& engine, ENGINE_ERROR_CODE code);

    void decrDirtyQueueMem(size_t decrementBy);

    void decrDirtyQueueAge(size_t decrementBy);

    void decrDirtyQueuePendingWrites(size_t decrementBy);

    /**
     * Updates an existing StoredValue in in-memory data structures like HT.
     * Assumes that HT bucket lock is grabbed.
     *
     * @param hbl Hash table lock that must be held
     * @param v Reference to the StoredValue to be updated.
     * @param itm Item to be updated.
     * @param queueItmCtx holds info needed to queue an item in chkpt
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
     * @param queueItmCtx holds info needed to queue an item in chkpt
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
     * @param queueItmCtx holds info needed to queue an item in chkpt
     * @param bySeqno seqno of the key being deleted
     * @param deleteSource The source of the delete (explicit or TTL [expiry])
     *
     * @return - pointer to the updated StoredValue. If deletionStatus is
     *           Success then a valid pointer, it can be same as that of
     *           v or different value if a new StoredValue is created for the
     *           update.
     *           If DeletionStatus is IsPendingSyncWrite then nullptr.
     *         - status of the delete.
     *         - notification info.
     */
    virtual std::tuple<StoredValue*, DeletionStatus, VBNotifyCtx>
    softDeleteStoredValue(const HashTable::HashBucketLock& hbl,
                          StoredValue& v,
                          bool onlyMarkDeleted,
                          const VBQueueItemCtx& queueItmCtx,
                          uint64_t bySeqno,
                          DeleteSource deleteSource) = 0;

    /**
     * Commit the given pending item; removing any previous committed item with
     * the same key from in-memory structures.
     * @param values Reference to the struct containing StoredValueProxies to
     *               the pending and committed items.
     * @param prepareSeqno The seqno of the prepare that we are committing
     * @param queueItmCtx Options on how the item should be queued.
     * @param commitSeqno Optional seqno to use for the committed item. If
     *                    omitted then CheckpointManager will generate one.
     * @return Information on who should be notified of the commit.
     */
    virtual VBNotifyCtx commitStoredValue(
            HashTable::FindUpdateResult& values,
            uint64_t prepareSeqno,
            const VBQueueItemCtx& queueItmCtx,
            std::optional<int64_t> commitSeqno) = 0;

    /**
     * Abort the given pending item by removing it from in-memory structures
     * and disk.
     *
     * @param hbl Reference to the hash table bucket lock
     * @param v StoredValue to be aborted. Must refer to a pending StoredValue
     * @param prepareSeqno The seqno of the Prepare sync-write being aborted
     * @param abortSeqno Optional seqno to use for the aborted item. If omitted
     *     then CheckpointManager will generate one.
     * @return Information on who should be notified of the commit.
     */
    virtual VBNotifyCtx abortStoredValue(const HashTable::HashBucketLock& hbl,
                                         StoredValue& v,
                                         int64_t prepareSeqno,
                                         std::optional<int64_t> abortSeqno) = 0;

    /**
     * Add a new abort item. To be used when an abort has been received, but the
     * matching prepare was not.
     *
     * @param hbl Reference to the hash table bucket lock
     * @param k key for the new aborted item
     * @param prepareSeqno The seqno of the Prepare the abort *would* have
     * aborted if the Prepare had been received.
     * @param abortSeqno seqno to use for the aborted item. This is mandatory,
     *                   as an abort for an absent prepare should only occur
     *                   in a replica, where the abortSeqno is always available.
     * @return Information on who should be notified of the commit.
     */
    virtual VBNotifyCtx addNewAbort(const HashTable::HashBucketLock& hbl,
                                    const DocKey& key,
                                    int64_t prepareSeqno,
                                    int64_t abortSeqno) = 0;

    /**
     * Add a temporary item in hash table and enqueue a background fetch for a
     * key.
     *
     * @param hbl Reference to the hash table bucket lock
     * @param key the key to be bg fetched
     * @param cookie the cookie of the requestor
     * @param engine Reference to ep engine
     * @param metadataOnly whether the fetch is for a non-resident value or
     *                     metadata of a (possibly) deleted item
     *
     * @return ENGINE_ERROR_CODE status notified to be to the front end
     */
    virtual ENGINE_ERROR_CODE addTempItemAndBGFetch(
            HashTable::HashBucketLock& hbl,
            const DocKey& key,
            const void* cookie,
            EventuallyPersistentEngine& engine,
            bool metadataOnly) = 0;

    /**
     * Enqueue a background fetch for a key.
     *
     * @param key the key to be bg fetched
     * @param cookie the cookie of the requestor
     * @param engine Reference to ep engine
     * @param isMeta whether the fetch is for a non-resident value or metadata
     *               of a (possibly) deleted item
     */
    virtual void bgFetch(const DocKey& key,
                         const void* cookie,
                         EventuallyPersistentEngine& engine,
                         bool isMeta = false) = 0;

    /**
     * Enqueue a background fetch (due to compaction) to expire a key.
     *
     * @param key the key to be bg fetched
     * @param item Reference to the item that is currnetly being compacted
     */
    virtual void bgFetchForCompactionExpiry(const DocKey& key,
                                            const Item& item) = 0;

    /**
     * Get metadata and value for a non-resident key
     *
     * @param key key for which metadata and value should be retrieved
     * @param cookie the cookie representing the client
     * @param engine Reference to ep engine
     * @param queueBgFetch Indicates whether a background fetch needs to be
     *        queued
     * @param v reference to the stored value of the non-resident key
     *
     * @return the result of the operation
     */
    virtual GetValue getInternalNonResident(const DocKey& key,
                                            const void* cookie,
                                            EventuallyPersistentEngine& engine,
                                            QueueBgFetch queueBgFetch,
                                            const StoredValue& v) = 0;

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

    /**
     * Is the durability level valid for this type of vBucket?
     *
     * @param level Level to check
     * @return True if valid, false if not
     */
    virtual bool isValidDurabilityLevel(cb::durability::Level level) = 0;

    /**
     * Check if the durability requirements of the given item can be satisfied
     * by this vBucket.
     *
     * @param item The durable write
     * @return ENGINE_SUCCESS if durability is possible, appropriate error code
     *         to return if not
     */
    ENGINE_ERROR_CODE checkDurabilityRequirements(const Item& item);

    /**
     * Check if the given durability requirements can be satisfied by this
     * vBucket.
     *
     * @param reqs The durability requirements
     * @return ENGINE_SUCCESS if durability is possible, appropriate error code
     *         to return if not
     */
    ENGINE_ERROR_CODE checkDurabilityRequirements(
            const cb::durability::Requirements& reqs);

    /**
     * Base function for queueing an item for persistence and replication.
     *
     * @param item The item to queue.
     * @param ctx The VBQueueItemCtx. Holds info needed to queue the item,
     *     look at the structure for details.
     * @return the notification context used for notifying the Flusher and
     *     Replica Connections.
     */
    VBNotifyCtx queueItem(queued_item& item, const VBQueueItemCtx& ctx);

    /**
     * Deal with the prepare in the HashTable in the derived class specific way
     * as it is to be "replaced" by a mutation. Consumes the StoredValue* in the
     * StoredValueProxy making it no longer usable.
     *
     * @param v StoredValueProxy of the prepare to complete
     */
    virtual void processImplicitlyCompletedPrepare(
            HashTable::StoredValueProxy& v) = 0;

    /**
     * Remove any queued acks for the given node from the ActiveDM.
     *
     * @param node Name of the node for which we wish to remove the ack
     */
    void removeAcksFromADM(const std::string& node);

    Vbid id;
    std::atomic<vbucket_state_t>    state;
    folly::SharedMutex stateLock;

    vbucket_state_t                 initialState;

    /**
     * The replication topology, set as part of SET_VBUCKET_STATE.
     * It is encoded as nlohmann::json array of (max 2) replication chains.
     * Each replication chain is itself a nlohmann::json array of nodes
     * representing the chain.
     * Using unique_ptr for pimpl (to avoid requiring definition of
     * nlohmann::json in this header).
     */
    folly::SynchronizedPtr<std::unique_ptr<nlohmann::json>> replicationTopology;

    std::mutex                           pendingOpLock;
    std::vector<const void*>        pendingOps;
    std::chrono::steady_clock::time_point pendingOpsStart;

    /**
     * Sequence number of the highest purged tombstone.
     * - Weakly monotonic as this should not go backwards.
     * - Atomic so it can be read without locks for stats printing.
     */
    AtomicWeaklyMonotonic<uint64_t> purge_seqno;
    std::atomic<bool>               takeover_backed_up;

    /* snapshotMutex is used to update/read the pair {start, end} atomically,
       but not if reading a single field. */
    mutable std::mutex snapshotMutex;
    snapshot_range_t persistedRange;

    /*
     * When a vbucket is in the middle of receiving the initial disk snapshot
     * we do not want to accept stream requests (instead we return tmp fail).
     * The reason for this is that if ns_server fails to see kv_engine
     * receive the full disk snapshot, it deletes the vbucket files.
     */
    std::atomic<bool> receivingInitialDiskSnapshot;

    std::mutex bfMutex;
    std::unique_ptr<BloomFilter> bFilter;
    std::unique_ptr<BloomFilter> tempFilter;    // Used during compaction.

    std::atomic<uint64_t> rollbackItemCount;

    HLC hlc;
    std::string statPrefix;
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

    /**
     * Callback invoked when one or more SyncWrites are ready to be resolved for
     * this VBucket (either met requirements and should be Committed, or cannot
     * meet requirements and should be Aborted).
     */
    SyncWriteResolvedCallback syncWriteResolvedCb;

    /**
     * Callback invoked after a SyncWrite has been completed (Committed /
     * Aborted / Times Out), so the requesting client can be informed of the
     * SyncWrite's fate.
     */
    SyncWriteCompleteCallback syncWriteCompleteCb;

    /**
     * Callback invoked by a Replica VBucket after a High Prepared Seqno update
     * within the PassiveDurabilityMonitor.
     */
    SeqnoAckCallback seqnoAckCb;

    /// The VBucket collection state
    std::unique_ptr<Collections::VB::Manifest> manifest;

    /**
     * records if the vbucket has had xattrs documents written to it, note that
     * rollback of data or delete of all the xattr documents does not undo the
     * flag.
     */
    std::atomic<bool> mayContainXattrs;

    // Durable writes are enqueued also into the DurabilityMonitor.
    // The seqno-order of items tracked by the DM must be the same as in the
    // Backfill/CheckpointManager Queues (seqno is strictly monotonic).
    // I.e., adding to Queue and adding into the DM must be an atomic operation,
    // which is what this mutex is used for.
    std::mutex dmQueueMutex;

    static cb::AtomicDuration<> chkFlushTimeout;

    static double mutationMemThreshold;

    // The seqno threshold below which we may replace a prepare with another
    // prepare (if the associated Commit/Abort may have been deduped)
    int64_t allowedDuplicatePrepareThreshold = 0;

    friend class DurabilityMonitorTest;
    friend class SingleThreadedActiveStreamTest;
    friend class VBucketTestBase;
    friend class VBucketTestIntrospector;
    friend class VBucketDurabilityTest;
    friend class DurabilityEPBucketTest;

    DISALLOW_COPY_AND_ASSIGN(VBucket);
};

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

    /// Get the underlying lock (to allow caller to release the lock and
    /// reacquire it at a later time)
    std::unique_lock<std::mutex>& getLock() {
        return lock;
    }

private:
    VBucketPtr vb;
    std::unique_lock<std::mutex> lock;
};
