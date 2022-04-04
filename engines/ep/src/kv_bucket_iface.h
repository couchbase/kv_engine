/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "kvstore/kvstore_fwd.h"
#include "permitted_vb_states.h"
#include "rollback_result.h"
#include "vbucket_fwd.h"
#include "vbucket_notify_context.h"
#include <executor/globaltask.h>
#include <folly/SharedMutex.h>
#include <memcached/engine.h>
#include <memcached/range_scan.h>
#include <memcached/range_scan_id.h>
#include <nlohmann/json_fwd.hpp>
#include <statistics/cardinality.h>
#include <list>
#include <string_view>

/* Forward declarations */
struct CompactionConfig;
class ExtendedMetaData;
struct CompactionConfig;
class ConflictResolution;
class DefragmenterTask;
class DiskDocKey;
class EventuallyPersistentEngine;
class FailoverTable;
class Flusher;
class HashTable;
class ItemMetaData;
class KVBucket;
class KVShard;
class KVStoreIface;
class MutationLog;
class PauseResumeVBVisitor;
class PersistenceCallback;
class VBucketMap;
class VBucketVisitor;
class InterruptableVBucketVisitor;
class BucketStatCollector;
class RangeScanDataHandlerIFace;
class StatCollector;
class StorageProperties;
class Warmup;
class VBucketFilter;
namespace Collections {
class Manager;
class Manifest;
}
namespace Collections::VB {
class Manifest;
}
struct key_stats;

namespace cb::rangescan {
struct SamplingConfiguration;
struct SnapshotRequirements;
} // namespace cb::rangescan

class BGFetchItem;
using bgfetched_item_t = std::pair<DiskDocKey, const BGFetchItem*>;

class GlobalTask;
using ExTask = std::shared_ptr<GlobalTask>;

/**
 * This is the abstract base class that manages the bucket behavior in
 * ep-engine.
 * Different bucket types can be derived from this class.
 */

class KVBucketIface {
public:

    /**
     * Represents a position within the epStore, used when visiting items.
     *
     * Currently opaque (and constant), clients can pass them around but
     * cannot reposition the iterator.
     */
    class Position {
    public:
        bool operator==(const Position& other) const {
            return (vbucket_id == other.vbucket_id);
        }

    private:
        explicit Position(Vbid vbucket_id_) : vbucket_id(vbucket_id_) {
        }

        Vbid vbucket_id;

        friend class KVBucket;
        friend std::ostream& operator<<(std::ostream& os, const Position& pos);
    };

    /**
     * Start necessary tasks.
     * Client calling initialize must also call deinitialize before deleting
     * a concrete KVBucketIface instance
     */
    virtual bool initialize() = 0;

    /**
     * Stop tasks started in initialize()
     */
    virtual void deinitialize() = 0;

    /**
     * Set an item in the store.
     * @param item the item to set
     * @param cookie the cookie representing the client to store the item
     * @param predicate an optional function to call which if returns true,
     *        the replace will succeed. The function is called against any
     *        existing item.
     * @return the result of the store operation
     */
    virtual cb::engine_errc set(Item& item,
                                const CookieIface* cookie,
                                cb::StoreIfPredicate predicate = {}) = 0;

    /**
     * Add an item in the store.
     * @param item the item to add
     * @param cookie the cookie representing the client to store the item
     * @return the result of the operation
     */
    virtual cb::engine_errc add(Item& item, const CookieIface* cookie) = 0;

    /**
     * Replace an item in the store.
     * @param item the item to replace
     * @param cookie the cookie representing the client to store the item
     * @param predicate an optional function to call which if returns true,
     *        the replace will succeed. The function is called against any
     *        existing item.
     * @return the result of the operation
     */
    virtual cb::engine_errc replace(Item& item,
                                    const CookieIface* cookie,
                                    cb::StoreIfPredicate predicate = {}) = 0;

    /**
     * Retrieve a value.
     *
     * @param key     the key to fetch
     * @param vbucket the vbucket from which to retrieve the key
     * @param cookie  the connection cookie
     * @param options options specified for retrieval
     *
     * @return a GetValue representing the result of the request
     */
    virtual GetValue get(const DocKey& key,
                         Vbid vbucket,
                         const CookieIface* cookie,
                         get_options_t options) = 0;

    /**
     * Retrieve a value randomly from the store.
     *
     * @param cid collection to retrieve from
     * @param cookie the connection cookie
     * @return a GetValue representing the value retrieved
     */
    virtual GetValue getRandomKey(CollectionID cid,
                                  const CookieIface* cookie) = 0;

    /**
     * Retrieve a value from a vbucket in replica state.
     *
     * @param key     the key to fetch
     * @param vbucket the vbucket from which to retrieve the key
     * @param cookie  the connection cookie
     * @param options options specified for retrieval
     *
     * @return a GetValue representing the result of the request
     */
    virtual GetValue getReplica(
            const DocKey& key,
            Vbid vbucket,
            const CookieIface* cookie,
            get_options_t options = static_cast<get_options_t>(
                    QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE |
                    DELETE_TEMP | HIDE_LOCKED_CAS)) = 0;

    /**
     * Retrieve the meta data for an item
     *
     * @param key the key to get the meta data for
     * @param vbucket the vbucket from which to retrieve the key
     * @param cookie the connection cookie
     * @param[out] metadata where to store the meta informaion
     * @param[out] deleted specifies whether or not the key is deleted
     * @param[out] datatype specifies the datatype of the item
     */
    virtual cb::engine_errc getMetaData(const DocKey& key,
                                        Vbid vbucket,
                                        const CookieIface* cookie,
                                        ItemMetaData& metadata,
                                        uint32_t& deleted,
                                        uint8_t& datatype) = 0;

    /**
     * Set an item in the store.
     * @param item the item to set
     * @param cas value to match
     * @param seqno sequence number of mutation
     * @param cookie the cookie representing the client to store the item
     * @param permittedVBStates set of VB states that the target VB can be in
     * @param checkConflicts set to Yes if conflict resolution must be done
     * @param allowExisting set to false if you want set to fail if the
     *                      item exists already
     * @param genBySeqno whether or not to generate sequence number
     * @param emd ExtendedMetaData class object that contains any ext meta
     *
     * @return the result of the store operation
     */
    virtual cb::engine_errc setWithMeta(
            Item& item,
            uint64_t cas,
            uint64_t* seqno,
            const CookieIface* cookie,
            PermittedVBStates permittedVBStates,
            CheckConflicts checkConflicts,
            bool allowExisting,
            GenerateBySeqno genBySeqno = GenerateBySeqno::Yes,
            GenerateCas genCas = GenerateCas::No,
            ExtendedMetaData* emd = nullptr) = 0;

    /**
     * Add a prepare to the store
     * @param item the prepare to set
     * @param cookie the cookie representing the client to store the item
     *
     * @return the result of the store operation
     */
    virtual cb::engine_errc prepare(Item& item, const CookieIface* cookie) = 0;

    /**
     * Retrieve a value, but update its TTL first
     *
     * @param key the key to fetch
     * @param vbucket the vbucket from which to retrieve the key
     * @param cookie the connection cookie
     * @param exptime the new expiry time for the object
     *
     * @return a GetValue representing the result of the request
     */
    virtual GetValue getAndUpdateTtl(const DocKey& key,
                                     Vbid vbucket,
                                     const CookieIface* cookie,
                                     time_t exptime) = 0;

    /**
     * Retrieve an item from the disk for vkey stats
     *
     * @param key the key to fetch
     * @param vbucket the vbucket from which to retrieve the key
     * @param cookie the connection cookie
     * @param cb callback to return an item fetched from the disk
     *
     * @return a status resulting form executing the method
     */
    virtual cb::engine_errc statsVKey(const DocKey& key,
                                      Vbid vbucket,
                                      const CookieIface* cookie) = 0;

    virtual void completeStatsVKey(const CookieIface* cookie,
                                   const DocKey& key,
                                   Vbid vbid,
                                   uint64_t bySeqNum) = 0;

    virtual cb::mcbp::Status evictKey(const DocKey& key,
                                      Vbid vbucket,
                                      const char** msg) = 0;

    /**
     * delete an item in the store
     *
     * @param key the key of the item
     * @param[in, out] cas the CAS ID for a CASed delete (0 to override)
     * @param vbucket the vbucket for the key
     * @param cookie the cookie representing the client
     * @param durability Optional durability requirements for this delete.
     * @param[out] itemMeta the pointer to the metadata memory.
     * @param[out] mutInfo mutation information
     *
     * @return the result of the operation
     */
    virtual cb::engine_errc deleteItem(
            const DocKey& key,
            uint64_t& cas,
            Vbid vbucket,
            const CookieIface* cookie,
            std::optional<cb::durability::Requirements> durability,
            ItemMetaData* itemMeta,
            mutation_descr_t& mutInfo) = 0;

    /**
     * Delete an item in the store from a non-front end operation (DCP, XDCR)
     *
     * @param key the key of the item
     * @param[in, out] cas the CAS ID for a CASed delete (0 to override)
     * @param[out] seqno Pointer to get the seqno generated for the item. A
     *                   NULL value is passed if not needed
     * @param vbucket the vbucket for the key
     * @param cookie the cookie representing the client
     * @param permittedVBStates set of VB states that the target VB can be in
     * @param checkConflicts set to Yes if conflict resolution must be done
     * @param itm item holding a deleted value. A NULL value is passed
     *            if an empty body is to be used for deletion.
     * @param itemMeta the metadata to use for this deletion.
     * @param genBySeqno whether or not to generate sequence number
     * @param generateCas whether or not to generate cas
     * @param bySeqno seqno of the key being deleted
     * @param emd ExtendedMetaData class object that contains any ext meta
     * @param deleteSource Determines the source of deletion and if TTL, it
     *                     triggers the expiry path.
     *
     * @return the result of the delete operation
     */
    virtual cb::engine_errc deleteWithMeta(const DocKey& key,
                                           uint64_t& cas,
                                           uint64_t* seqno,
                                           Vbid vbucket,
                                           const CookieIface* cookie,
                                           PermittedVBStates permittedVBStates,
                                           CheckConflicts checkConflicts,
                                           const ItemMetaData& itemMeta,
                                           GenerateBySeqno genBySeqno,
                                           GenerateCas generateCas,
                                           uint64_t bySeqno,
                                           ExtendedMetaData* emd,
                                           DeleteSource deleteSource) = 0;

    /**
     * Resets the Bucket. Removes all elements from each VBucket's &
     * CheckpointManager.
     * Specific subclasses (e.g. EPBucket) may have additional work to do
     * (update disk etc).
     */
    virtual void reset() = 0;

    /**
     * Pause the bucket's Flusher.
     * @return true if successful.
     */
    virtual bool pauseFlusher() = 0;

    /**
     * Resume the Flusher for all shards.
     * @return true if successful.
     */
    virtual bool resumeFlusher() = 0;

    /// Wake up the flusher for all shards, if the disk queue is non-empty.
    virtual void wakeUpFlusher() = 0;

    /**
     * Takes a snapshot of the current stats and persists them to disk.
     *
     * @param shuttingDown is the bucket shutting down?
     */
    virtual void snapshotStats(bool shuttingDown) = 0;

    /**
     * Get summarized vBucket stats for this bucket - total for all
     * active,replica buckets.
     */
    virtual void getAggregatedVBucketStats(
            const BucketStatCollector& collector,
            cb::prometheus::Cardinality cardinality) = 0;

    /**
     * Get file statistics
     *
     * @param cookie Cookie associated with ADD_STAT
     * @param add_stat Callback to use to add stats to the caller.
     * @return cb::engine_errc::success if stats were successfully retrieved, or
     *         cb::engine_errc::no_such_key if file stats are not available
     *         from the store.
     */
    virtual cb::engine_errc getFileStats(
            const BucketStatCollector& collector) = 0;

    /**
     * Get detailed (per-vbucket) disk stats.
     *
     * @param cookie Cookie associated with ADD_STAT
     * @param add_stat Callback to use to add stats to the caller.
     * @return cb::engine_errc::success if stats were successfully retrieved, or
     *         cb::engine_errc::no_such_key if per-vbucket disk stats are not
     * available from the store.
     */
    virtual cb::engine_errc getPerVBucketDiskStats(
            const CookieIface* cookie, const AddStatFn& add_stat) = 0;

    /**
     * Complete a batch of background fetch of a non resident value or metadata.
     *
     * @param vbId the vbucket in which the requested key lived
     * @param fetchedItems vector of completed background feches containing key,
     *                     value, client cookies
     * @param start the time when the background fetch was started
     *
     */
    virtual void completeBGFetchMulti(
            Vbid vbId,
            std::vector<bgfetched_item_t>& fetchedItems,
            std::chrono::steady_clock::time_point start) = 0;

    virtual VBucketPtr getVBucket(Vbid vbid) = 0;

    /**
     * Returns the last persisted checkpoint Id for the specified vBucket.
     * @param vb VBucket ID to get checkpoint Id for.
     * @return A pair of {checkpointId, true} if the persisted checkpointID is
     *         available (Persistent bucket), or false if bucket is not
     *         persistent.
     */
    virtual std::pair<uint64_t, bool> getLastPersistedCheckpointId(Vbid vb) = 0;

    virtual uint64_t getLastPersistedSeqno(Vbid vb) = 0;

    /**
     * Deletes a vbucket
     *
     * @param vbid The vbucket to delete.
     * @param c The cookie for this connection.
     *          Used in synchronous bucket deletes
     *          to notify the connection of operation completion.
     */
    virtual cb::engine_errc deleteVBucket(Vbid vbid,
                                          const CookieIface* c = nullptr) = 0;

    /**
     * Check for the existence of a vbucket in the case of couchstore.
     *
     * @param db_file_id vbucketid for couchstore
     */
    virtual cb::engine_errc checkForDBExistence(Vbid db_file_id) = 0;

    /**
     * Triggers compaction of a database file
     *
     * @param vbid The vbucket to compact
     * @param c The context for compaction of a DB file
     * @param ck cookie used to notify connection of operation completion
     * @param delay millisecond delay for the task to execute, 0 is run 'now'
     */
    virtual cb::engine_errc scheduleCompaction(
            Vbid vbid,
            const CompactionConfig& c,
            const CookieIface* ck,
            std::chrono::milliseconds delay) = 0;

    /**
     * Cancels compaction of a database file
     *
     * @param vbid The vbucket being compacted
     */
    virtual cb::engine_errc cancelCompaction(Vbid vbid) = 0;

    /**
     * Reset a given vbucket from memory and disk. This differs from vbucket
     * deletion in that it does not delete the vbucket instance from memory hash
     * table.
     */
    virtual bool resetVBucket(Vbid vbid) = 0;

    /**
     * Visit each VBucket in the Bucket, calling VBucketVisitor::visitBucket()
     * on each vbucket.
     *
     * Note this is synchronous, and hence is only suitable if visitor performs
     * a small & constant amount of work on each vBucket. See visitAsync() below
     * for handling large / variable amounts of work.
     */
    virtual void visit(VBucketVisitor& visitor) = 0;

    /**
     * Visit each VBucket in the Bucket, calling
     * InterruptableVBucketVisitor::visitBucket() on each vBucket.
     * Visiting is executed in a background task (asynchronously).
     *
     * This method is suitable where the visitor needs to perform a large and/or
     * variable amount of work for each vBucket, and hence it should be
     * performed asynchronously in a background task.
     *
     * After visiting each vb, InterruptableVBucketVisitor::shouldInterrupt()
     * will be called to check if execution of the background task should be
     * paused or stopped.
     * If paused, then will yield back to the Executor to allow any waiting
     * higher-priority tasks to run.
     * While if stopped, the visitor will inform the Executor that the task has
     * completed.
     *
     * @param visitor Object to visit each bucket with.
     * @param label Name to associate with the created task.
     * @param id TaskId to use for the background task. This also dictates which
     *           TaskQueue (Reader, Writer, AuxIO, NonIO) to use.
     * @param maxExpectedDuration Maximum duration this task is expected to run
     *                            for. If the duration is exceeded will log a
     *                            warning.
     */
    virtual size_t visitAsync(
            std::unique_ptr<InterruptableVBucketVisitor> visitor,
            const char* lbl,
            TaskId id,
            std::chrono::microseconds maxExpectedDuration) = 0;

    /**
     * Visit the items in this epStore, starting the iteration from the
     * given startPosition and allowing the visit to be paused at any point.
     *
     * During visitation, the visitor object can request that the visit
     * is stopped after the current item. The position passed to the
     * visitor can then be used to restart visiting at the *APPROXIMATE*
     * same position as it paused.
     * This is approximate as various locks are released when the
     * function returns, so any changes to the underlying epStore may cause
     * the visiting to restart at the slightly different place.
     *
     * As a consequence, *DO NOT USE THIS METHOD* if you need to guarantee
     * that all items are visited!
     *
     * @param visitor   The visitor object.
     * @param start_pos Position of which vbucket that should visited first
     * @param filter    An optional VbucketFilter that specifies which vbuckets
     *                  may be visited
     * @return The final epStore position visited; equal to
     *         EPBucket::end() if all items were visited
     *         otherwise the position to resume from.
     */
    virtual Position pauseResumeVisit(PauseResumeVBVisitor& visitor,
                                      Position& start_pos,
                                      VBucketFilter* filter) = 0;

    /**
     * Return a position at the start of the epStore.
     */
    virtual Position startPosition() const = 0;

    /**
     * Return a position at the end of the epStore. Has similar semantics
     * as STL end() (i.e. one past the last element).
     */
    virtual Position endPosition() const = 0;

    virtual Warmup* getWarmup() const = 0;

    /**
     * Looks up the key stats for the given {vbucket, key}.
     * @param key The key to lookup
     * @param vbucket The vbucket the key belongs to.
     * @param cookie The client's cookie
     * @param[out] kstats On success the keystats for this item.
     * @param wantsDeleted If yes then return keystats even if the item is
     *                     marked as deleted. If no then will return
     *                     cb::engine_errc::no_such_key for deleted items.
     */
    virtual cb::engine_errc getKeyStats(const DocKey& key,
                                        Vbid vbucket,
                                        const CookieIface* cookie,
                                        key_stats& kstats,
                                        WantsDeleted wantsDeleted) = 0;

    virtual std::string validateKey(const DocKey& key,
                                    Vbid vbucket,
                                    Item& diskItem) = 0;

    virtual GetValue getLocked(const DocKey& key,
                               Vbid vbucket,
                               rel_time_t currentTime,
                               uint32_t lockTimeout,
                               const CookieIface* cookie) = 0;

    virtual cb::engine_errc unlockKey(const DocKey& key,
                                      Vbid vbucket,
                                      uint64_t cas,
                                      rel_time_t currentTime,
                                      const CookieIface* cookie) = 0;

    virtual KVStoreIface* getRWUnderlying(Vbid vbId) = 0;

    virtual KVStoreIface* getRWUnderlyingByShard(size_t shardId) = 0;

    virtual const KVStoreIface* getROUnderlyingByShard(
            size_t shardId) const = 0;

    virtual const KVStoreIface* getROUnderlying(Vbid vbId) const = 0;

    /**
     * takeRW and setRW are used for changing the kvstore(s) in unit tests
     * takeRW will move the value of rw out of this object, leaving the
     * KVBucket with no store, setRW should be run after take to put back valid
     * KVStores
     * @param shardId the shard to take from
     */
    virtual std::unique_ptr<KVStoreIface> takeRW(size_t shardId) = 0;

    /**
     * takeRW and  setRW are used for changing the kvstore(s) in unit tests
     * setRW will move the value of rw over the current rw
     * @param shardId the shared to set onto
     * @param rw the read write KVStore
     */
    virtual void setRW(size_t shardId, std::unique_ptr<KVStoreIface> rw) = 0;

    virtual void processExpiredItem(Item& it,
                                    time_t startTime,
                                    ExpireBy source) = 0;

    /**
     * Get the memoized storage properties from the DB.kv
     */
    virtual const StorageProperties getStorageProperties() const = 0;

    /**
     * schedule a vb_state snapshot task for a given shard.
     */
    virtual void scheduleVBStatePersist() = 0;

    /**
     * Schedule a vbstate persistence task for a given vbucket.
     */
    virtual void scheduleVBStatePersist(Vbid vbid) = 0;

    virtual const VBucketMap& getVBuckets() const = 0;

    virtual EventuallyPersistentEngine& getEPEngine() = 0;
    virtual const EventuallyPersistentEngine& getEPEngine() const = 0;

    virtual size_t getExpiryPagerSleeptime() = 0;

    virtual size_t getTransactionTimePerItem() = 0;

    virtual void setBackfillMemoryThreshold(double threshold) = 0;

    virtual void setExpiryPagerSleeptime(size_t val) = 0;

    virtual void setExpiryPagerTasktime(ssize_t val) = 0;

    virtual void enableExpiryPager() = 0;
    virtual void disableExpiryPager() = 0;

    virtual void enableAccessScannerTask() = 0;
    virtual void disableAccessScannerTask() = 0;
    virtual void setAccessScannerSleeptime(size_t val, bool useStartTime) = 0;
    virtual void resetAccessScannerStartTime() = 0;

    virtual void resetAccessScannerTasktime() = 0;

    virtual void setAllBloomFilters(bool to) = 0;

    virtual float getBfiltersResidencyThreshold() = 0;

    virtual void setBfiltersResidencyThreshold(float to) = 0;

    virtual bool isMetaDataResident(VBucketPtr &vb,
                                    const DocKey& key) = 0;

    virtual void logQTime(const GlobalTask& taskType,
                          std::string_view threadName,
                          std::chrono::steady_clock::duration enqTime) = 0;

    virtual void logRunTime(const GlobalTask& taskType,
                            std::string_view threadName,
                            std::chrono::steady_clock::duration runTime) = 0;

    virtual void updateCachedResidentRatio(size_t activePerc,
                                           size_t replicaPerc) = 0;

    /**
     * Has warmup loaded enough data to serve ops?
     */
    virtual bool isWarmupLoadingData() = 0;

    /**
     * All aspects of warmup have finished - loading data + any background tasks
     */
    virtual bool isWarmupComplete() = 0;

    virtual bool isMemUsageAboveBackfillThreshold() = 0;

    virtual void addKVStoreStats(const AddStatFn& add_stat,
                                 const CookieIface* cookie) = 0;

    virtual void addKVStoreTimingStats(const AddStatFn& add_stat,
                                       const CookieIface* cookie) = 0;

    /* Given a named KVStore statistic, return the value of that statistic,
     * accumulated across any shards.
     *
     * @param name The name of the statistic
     * @param[out] value The value of the statistic.
     * @return True if the statistic was successfully returned via {value},
     *              else false.
     */
    virtual bool getKVStoreStat(std::string_view name, size_t& value) = 0;

    /// Get statistic values for specified ones, accumulated across any shards.
    ///
    /// @param [in] keys specifies the statistics to be fetched.
    /// @return statistic values. Note that the string_view keys in the returned
    /// map refer to the same string keys that the input string_view refers to.
    /// Hence the map is ok to use only as long as the string keys live.
    ///
    virtual GetStatsMap getKVStoreStats(
            gsl::span<const std::string_view> keys) = 0;

    virtual void resetUnderlyingStats() = 0;
    virtual const KVStoreIface* getOneROUnderlying() const = 0;
    virtual KVStoreIface* getOneRWUnderlying() = 0;

    /**
     * @return A single Flusher pointer that belongs to this Bucket. May be
     *         nullptr if none exist. No assumption should be made as to which
     *         Flusher object is returned.
     */
    virtual Flusher* getOneFlusher() = 0;

    virtual EvictionPolicy getItemEvictionPolicy() const = 0;

    /*
     * Request a rollback of the vbucket to the specified seqno.
     * If the rollbackSeqno is not a checkpoint boundary, then the rollback
     * will be to the nearest checkpoint.
     * There are also cases where the rollback will be forced to 0.
     * various failures or if the rollback is > 50% of the data.
     *
     * A check of the vbucket's high-seqno indicates if a rollback request
     * was not honoured exactly.
     *
     * @param vbid The vbucket to rollback
     * @rollbackSeqno The seqno to rollback to.
     *
     * @return TaskStatus::Complete upon successful rollback
     *         TaskStatus::Abort if vbucket is not replica or
     *                           if vbucket is not valid
     *                           if vbucket reset and rollback fails
     *         TaskStatus::Reschedule if you cannot get a lock on the vbucket
     */
    virtual TaskStatus rollback(Vbid vbid, uint64_t rollbackSeqno) = 0;

    /**
     * Attempt to free up currently in-use memory this bucket.
     * Possible ways to free memory depend on the underlying bucket type and
     * configuration, but examples include evicting resident values,
     * checking for any expired items, etc.
     */
    virtual void attemptToFreeMemory() = 0;

    virtual void wakeUpCheckpointMemRecoveryTask() = 0;

    virtual void runDefragmenterTask() = 0;

    /**
     * Invoke the run method of the ItemFreqDecayerTask.  Currently only used
     * for testing purposes.
     */
    virtual void runItemFreqDecayerTask() = 0;

    virtual bool runAccessScannerTask() = 0;

    virtual void runVbStatePersistTask(Vbid vbid) = 0;

    virtual void setCompactionWriteQueueCap(size_t to) = 0;

    virtual void setCompactionMaxConcurrency(float to) = 0;

    virtual bool isAccessScannerEnabled() = 0;

    virtual bool isExpPagerEnabled() = 0;

    /// Check if there were any out-of-memory errors during warmup
    virtual bool isWarmupOOMFailure() = 0;

    /// Check if any of the vbucket set state failed during warmup
    virtual bool hasWarmupSetVbucketStateFailed() const = 0;

    virtual bool maybeWaitForVBucketWarmup(const CookieIface* cookie) = 0;

    virtual size_t getActiveResidentRatio() const = 0;

    virtual size_t getReplicaResidentRatio() const = 0;

    /*
     * Change the max_cas of the specified vbucket to cas without any
     * care for the data or ongoing operations...
     */
    virtual cb::engine_errc forceMaxCas(Vbid vbucket, uint64_t cas) = 0;

    /**
     * Create a VBucket object appropriate for this Bucket class.
     */
    virtual VBucketPtr makeVBucket(
            Vbid id,
            vbucket_state_t state,
            KVShard* shard,
            std::unique_ptr<FailoverTable> table,
            NewSeqnoCallback newSeqnoCb,
            std::unique_ptr<Collections::VB::Manifest> manifest,
            vbucket_state_t initState = vbucket_state_dead,
            int64_t lastSeqno = 0,
            uint64_t lastSnapStart = 0,
            uint64_t lastSnapEnd = 0,
            uint64_t purgeSeqno = 0,
            uint64_t maxCas = 0,
            int64_t hlcEpochSeqno = HlcCasSeqnoUninitialised,
            bool mightContainXattrs = false,
            const nlohmann::json* replicationTopology = {},
            uint64_t maxVisibleSeqno = 0) = 0;

    /**
     * Notify all the clients of a new seqno being added in the vbucket
     *
     * @param vbid vBucket number
     * @param notifyCtx notify information
     */
    virtual void notifyNewSeqno(const Vbid vbid,
                                const VBNotifyCtx& notifyCtx) = 0;

    /**
     * @return true if the bucket supports 'get_all_keys'; else false
     */
    virtual bool isGetAllKeysSupported() const = 0;

    /**
     * @return true if the bucket supports by-id scans
     */
    virtual bool isByIdScanSupported() const = 0;

    /**
     * Dependent on bucket type schedule a task for collection manifest
     * persistence.
     * @param cookie connection cookie
     * @param unique_ptr to the new manifest - the task will take ownership of
     * this.
     * @return true if a task was scheduled
     */
    virtual bool maybeScheduleManifestPersistence(
            const CookieIface* cookie,
            std::unique_ptr<Collections::Manifest>& newManifest) = 0;

    /**
     * Create a new range scan on a vbucket
     *
     * @param vbid vbucket to create on
     * @param start key for the start of the range
     * @param end key for the end of the range
     * @param handler object that will receive callbacks when the scan continues
     * @param cookie connection cookie to notify when done
     * @param keyOnly key/value configuration of the scan
     * @param snapshotReqs optional requirements that the snapshot must satisfy
     * @param samplingConfig the parameters for the optional random sampling
     *
     * @return pair of status/cb::rangescan::Id - ID is valid on success
     */
    virtual std::pair<cb::engine_errc, cb::rangescan::Id> createRangeScan(
            Vbid vbid,
            CollectionID cid,
            cb::rangescan::KeyView start,
            cb::rangescan::KeyView end,
            std::unique_ptr<RangeScanDataHandlerIFace> handler,
            const CookieIface& cookie,
            cb::rangescan::KeyOnly keyOnly,
            std::optional<cb::rangescan::SnapshotRequirements> snapshotReqs,
            std::optional<cb::rangescan::SamplingConfiguration>
                    samplingConfig) = 0;
    /**
     * Continue the range scan with the given identifier.
     *
     * @param vbid vbucket to find the scan on
     * @param uuid The identifier of the scan to continue
     * @param cookie The client cookie requesting the continue
     * @param itemLimit The maximum number of items the continue can return
     *                  0 means no limit enforced
     * @param timeLimit The maximum duration the continue can return
     *                  0 means no limit enforced
     * @return would_block if the scan was found and successfully scheduled
     */
    virtual cb::engine_errc continueRangeScan(
            Vbid vbid,
            cb::rangescan::Id uuid,
            const CookieIface& cookie,
            size_t itemLimit,
            std::chrono::milliseconds timeLimit) = 0;
    /**
     * Cancel the range scan with the given identifier.
     *
     * @param vbid vbucket to find the scan on
     * @param uuid The identifier of the scan to continue
     * @param schedule true if a task should be scheduled for the cancellation
     * @return would_block if the scan was found and successfully scheduled for
     *         cancellation
     */
    virtual cb::engine_errc cancelRangeScan(Vbid vbid,
                                            cb::rangescan::Id uuid) = 0;

    /**
     * Result of the loadPreparedSyncWrites function
     */
    struct LoadPreparedSyncWritesResult {
        uint64_t itemsVisited = 0;
        uint64_t preparesLoaded = 0;
        uint64_t defaultCollectionMaxVisibleSeqno = 0;
        bool success = false;
    };

protected:
    /**
     * Get metadata and value for a given key
     *
     * @param key Key for which metadata and value should be retrieved
     * @param vbucket the vbucket from which to retrieve the key
     * @param cookie The connection cookie
     * @param getReplicaItem bi-state enum to inform the method if it is dealing
     * with a get replica op
     * @param options Flags indicating some retrieval related info
     *
     * @return the result of the operation
     */
    virtual GetValue getInternal(const DocKey& key,
                                 Vbid vbucket,
                                 const CookieIface* cookie,
                                 ForGetReplicaOp getReplicaItem,
                                 get_options_t options = TRACK_REFERENCE) = 0;

    /**
     * Prepare the given vBucket for rollback
     * @param vbid to prepare for rollback
     *
     * @return context object for this rollback
     */
    virtual std::unique_ptr<RollbackCtx> prepareToRollback(Vbid vbid) = 0;

    /**
     * Do rollback of data on the underlying disk / data structure
     *
     * @param vbid vBucket id
     * @param rollbackSeqno intended point (in seqno) of rollback
     *
     * @result object that indicates if rollback was successful,
     *         highSeqno of the vBucket after rollback,
     *         and the last snaspshot range in the vb after rollback.
     */
    virtual RollbackResult doRollback(Vbid vbid, uint64_t rollbackSeqno) = 0;

    /*
     * Helper method for the rollback function.
     * Purge all unpersisted items from the current checkpoint(s) and fixup
     * the hashtable for any that are > the rollbackSeqno.
     *
     * @param vb ref to vBucket on which rollback is done
     * @param rollbackSeqno intended point (in seqno) of rollback
     */
    virtual void rollbackUnpersistedItems(VBucket& vb,
                                          int64_t rollbackSeqno) = 0;

    /**
     * Load the prepared SyncWrites from disk for the given vBucket.
     *
     * @param vb vBucket for which we will load SyncWrites
     *
     * @returns number of prepares loaded
     */
    virtual LoadPreparedSyncWritesResult loadPreparedSyncWrites(
            VBucket& vb) = 0;

    // During the warmup phase we might want to enable external traffic
    // at a given point in time.. The LoadStorageKvPairCallback will be
    // triggered whenever we want to check if we could enable traffic..
    friend class LoadStorageKVPairCallback;

    friend class Warmup;
    friend class PersistenceCallback;
    friend class VBCBAdaptor;
};
