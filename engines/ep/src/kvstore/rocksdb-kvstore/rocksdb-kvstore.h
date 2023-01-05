/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/**
 * Experimental RocksDB KVStore implementation
 *
 * Uses RocksDB (https://github.com/facebook/rocksdb) as a backend.
 */

#pragma once

#include "kvstore/kvstore.h"
#include "kvstore/kvstore_transaction_context.h"
#include "objectregistry.h"
#include "rollback_result.h"
#include "vbucket_bgfetch_item.h"
#include "vbucket_state.h"

#include <platform/dirutils.h>
#include <platform/non_negative_counter.h>
#include <map>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/listener.h>
#include <rocksdb/utilities/memory_util.h>
#include <string>


// Local tests have showed that skipping the call to
// `ObjectRegistry::onSwitchThread` from the RocksDB background Flush
// threads makes the mem_used stat to grow quickly, leading to resident-ratio
// close to 0 and constant tempOOM. At the same time, the memcached resident
// set does not grow. This behaviour suggests that some RocksDB memory
// allocations happen in a tracked thread (e.g., mc:writer executing
// rocksdb::DB::Write()), but the deallocation is performed in a RocksDB
// background Flush thread.
// For MB-27330, the same happens with some memory allocated in tracked
// threads and deallocated in RocksDB background Compaction threads.
class EventListener : public rocksdb::EventListener {
public:
    explicit EventListener(EventuallyPersistentEngine* epe) : engine(epe) {
    }

    void OnFlushBegin(rocksdb::DB*, const rocksdb::FlushJobInfo&) override {
        ObjectRegistry::onSwitchThread(engine, false);
    }

    void OnCompactionBegin(rocksdb::DB*,
                           const rocksdb::CompactionJobInfo&) override {
        ObjectRegistry::onSwitchThread(engine, false);
    }

    void OnCompactionCompleted(rocksdb::DB*,
                               const rocksdb::CompactionJobInfo&) override {
        ObjectRegistry::onSwitchThread(nullptr, false);
    }

private:
    EventuallyPersistentEngine* engine;
};

// Used to order the seqno Column Family to support iterating items by seqno
class SeqnoComparator : public rocksdb::Comparator {
public:
    int Compare(const rocksdb::Slice& a,
                const rocksdb::Slice& b) const override {
        const auto seqnoA = *reinterpret_cast<const int64_t*>(a.data());
        const auto seqnoB = *reinterpret_cast<const int64_t*>(b.data());

        if (seqnoA < seqnoB) {
            return -1;
        }
        if (seqnoA > seqnoB) {
            return +1;
        }

        return 0;
    }

    const char* Name() const override {
        return "SeqnoComparator";
        /* Change this if the comparator implementation is altered
         This is used to ensure the operator with which the DB was
         created is the same as the one provided when opening the DB.
         */
    }
    /* Additional functions which must be implemented but aren't required
     *  to do anything, but could be properly implemented in the future
     *  if beneficial.
     */
    void FindShortestSeparator(std::string*,
                               const rocksdb::Slice&) const override {
    }
    void FindShortSuccessor(std::string*) const override {
    }
};

class RocksRequest;
class RocksDBKVStoreConfig;
class VBHandle;
struct KVStatsCtx;

/**
 * A persistence store based on rocksdb.
 */
class RocksDBKVStore : public KVStore {
public:
    /**
     * Container for pending RocksDB requests.
     *
     * Using deque as as the expansion behaviour is less aggressive compared to
     * std::vector (RocksRequest objects are ~160 bytes in size).
     */
    using PendingRequestQueue = std::deque<RocksRequest>;

    /**
     * Constructor
     *
     * @param config    Configuration information
     */
    explicit RocksDBKVStore(RocksDBKVStoreConfig& config);

    ~RocksDBKVStore() override;

    void operator=(RocksDBKVStore& from) = delete;

    bool commit(std::unique_ptr<TransactionContext> txnCtx,
                VB::Commit& commitData) override;

    void addStats(const AddStatFn& add_stat, const void* c) const override;

    /*
     * Get a RocksDBKVStore specific stat
     *
     * @param name The name of the statistic to fetch.
     * @param[out] value Value of the given stat (if exists).
     * @return True if the stat exists, is of type size_t and was successfully
     *         returned, else false.
     */
    bool getStat(std::string_view name, size_t& value) const override;

    /**
     * Query the properties of the underlying storage.
     */
    StorageProperties getStorageProperties() const override;

    /**
     * Overrides set().
     */
    void set(TransactionContext& txnCtx, queued_item item) override;

    GetValue get(const DiskDocKey& key,
                 Vbid vb,
                 ValueFilter filter) const override;

    GetValue getWithHeader(const KVFileHandle& kvFileHandle,
                           const DiskDocKey& key,
                           Vbid vb,
                           ValueFilter filter) const override;

    void getMulti(Vbid vb, vb_bgfetch_queue_t& itms) const override;

    void getRange(Vbid vb,
                  const DiskDocKey& startKey,
                  const DiskDocKey& endKey,
                  ValueFilter filter,
                  const GetRangeCb& cb) const override;

    /**
     * Overrides del().
     */
    void del(TransactionContext& txnCtx, queued_item item) override;

    // This is a blocking call. The function waits until other threads have
    // finished processing on a VBucket DB (e.g., 'commit') before deleting
    // the VBucket and returning to the caller.
    void delVBucket(Vbid vbucket,
                    std::unique_ptr<KVStoreRevision> vb_version) override;

    std::vector<vbucket_state*> listPersistedVbuckets() override;

    /**
     * Take a snapshot of the vbucket states in the main DB.
     */
    bool snapshotVBucket(Vbid vbucketId, const vbucket_state& vbstate) override;

    void destroyInvalidVBuckets(bool);

    size_t getNumShards();

    CompactDBStatus compactDB(std::unique_lock<std::mutex>&,
                              std::shared_ptr<CompactionContext>) override {
        // Explicit compaction is not needed.
        // Compaction is continuously occurring in separate threads
        // under RocksDB's control
        return CompactDBStatus::Success;
    }

    vbucket_state* getCachedVBucketState(Vbid vbucketId) override {
        return cachedVBStates[getCacheSlot(vbucketId)].get();
    }

    ReadVBStateResult getPersistedVBucketState(Vbid vbid) const override;

    size_t getNumPersistedDeletes(Vbid vbid) override {
        // TODO vmx 2016-10-29: implement
        return 0;
    }

    DBFileInfo getDbFileInfo(Vbid vbid) override {
        // TODO vmx 2016-10-29: implement
        DBFileInfo vbinfo;
        return vbinfo;
    }

    DBFileInfo getAggrDbFileInfo() override {
        // TODO vmx 2016-10-29: implement
        DBFileInfo vbinfo;
        return vbinfo;
    }

    size_t getItemCount(Vbid vbid) override;

    RollbackResult rollback(Vbid vbid,
                            uint64_t rollbackSeqno,
                            std::unique_ptr<RollbackCB>) override {
        // TODO vmx 2016-10-29: implement
        // NOTE vmx 2016-10-29: For LevelDB/RocksDB it will probably
        // always be a full rollback as it doesn't support Couchstore
        // like rollback semantics
        return RollbackResult(/* not a success */ false);
    }

    void pendingTasks() override {
        // NOTE vmx 2016-10-29: Intentionally left empty;
    }

    cb::engine_errc getAllKeys(
            Vbid vbid,
            const DiskDocKey& start_key,
            uint32_t count,
            std::shared_ptr<StatusCallback<const DiskDocKey&>> cb)
            const override {
        // TODO vmx 2016-10-29: implement
        return cb::engine_errc::success;
    }

    std::unique_ptr<BySeqnoScanContext> initBySeqnoScanContext(
            std::unique_ptr<StatusCallback<GetValue>> cb,
            std::unique_ptr<StatusCallback<CacheLookup>> cl,
            Vbid vbid,
            uint64_t startSeqno,
            DocumentFilter options,
            ValueFilter valOptions,
            SnapshotSource source,
            std::unique_ptr<KVFileHandle> fileHandle = nullptr) const override;

    std::unique_ptr<ByIdScanContext> initByIdScanContext(
            std::unique_ptr<StatusCallback<GetValue>> cb,
            std::unique_ptr<StatusCallback<CacheLookup>> cl,
            Vbid vbid,
            const std::vector<ByIdRange>& ranges,
            DocumentFilter options,
            ValueFilter valOptions) const override {
        throw std::runtime_error(
                "RocksDB no support for byID initByIdScanContext");
    }

    scan_error_t scan(BySeqnoScanContext& sctx) const override;
    scan_error_t scan(ByIdScanContext& sctx) const override {
        throw std::runtime_error("RocksDB no support for byID scan");
    }

    std::unique_ptr<KVFileHandle> makeFileHandle(Vbid vbid) const override;

    std::pair<GetCollectionStatsStatus, Collections::VB::PersistedStats>
    getCollectionStats(const KVFileHandle& kvFileHandle,
                       CollectionID collection) const override;

    std::pair<GetCollectionStatsStatus, Collections::VB::PersistedStats>
    getCollectionStats(Vbid vbid, CollectionID collection) const override;

    void prepareToCreateImpl(Vbid vbid) override {
        // TODO DJR 2017-05-19 implement this.
    }

    std::unique_ptr<KVStoreRevision> prepareToDeleteImpl(Vbid vbid) override {
        // TODO DJR 2017-05-19 implement this.
        return 0;
    }

    std::optional<Collections::ManifestUid> getCollectionsManifestUid(
            KVFileHandle& kvFileHandle) const override {
        // TODO: rocksDb has no collections support, return default manifest-uid
        return Collections::ManifestUid{0};
    }

    std::pair<bool, Collections::KVStore::Manifest> getCollectionsManifest(
            Vbid vbid) const override {
        // TODO: rocksDb has no collections support, return default manifest
        return {true,
                Collections::KVStore::Manifest{
                        Collections::KVStore::Manifest::Default{}}};
    }

    std::pair<bool, std::vector<Collections::KVStore::DroppedCollection>>
    getDroppedCollections(Vbid vbid) const override {
        // TODO: rocksDb has no collections support, return empty
        return {};
    }

    const KVStoreConfig& getConfig() const override;

    GetValue getBySeqno(KVFileHandle& handle,
                        Vbid vbid,
                        uint64_t seq,
                        ValueFilter filter) const override;

    std::unique_ptr<TransactionContext> begin(
            Vbid vbid, std::unique_ptr<PersistenceCallback> pcb) override;

protected:
    // Write a batch of updates to the given database; measuring the time
    // taken and adding the timer to the commit histogram.
    rocksdb::Status writeAndTimeBatch(rocksdb::WriteBatch batch);

private:
    RocksDBKVStoreConfig& configuration;

    // Unique RocksDB instance, per-Shard.
    std::unique_ptr<rocksdb::DB> rdb;

    // Guards access to the 'vbHandles' vector. Users should lock this mutex
    // before accessing the vector to get a copy of any shared_ptr owned by
    // the vector. The mutex can be unlocked once a thread has its own copy
    // of the shared_ptr.
    mutable std::mutex vbhMutex;

    // This vector stores a VBHandle (i.e., handles for all the ColumnFamilies)
    // for each VBucket. The entry for a VBucket can be inserted in two
    // different cases:
    //     1) When the store processes an operation on the VBucket for the
    //          first time (in a call to 'getVBHandle()')
    //     2) In 'openDB()', all the ColumnFamilyHandles for all the existing
    //         Vbuckets are loaded.
    // An entry is removed only in 'delVBucket(vbid)'.
    std::vector<std::shared_ptr<VBHandle>> vbHandles;

    SeqnoComparator seqnoComparator;

    rocksdb::DBOptions dbOptions;
    rocksdb::ColumnFamilyOptions defaultCFOptions;
    rocksdb::ColumnFamilyOptions seqnoCFOptions;

    // Per-shard Block Cache
    std::shared_ptr<rocksdb::Cache> blockCache;

    enum class ColumnFamily { Default, Seqno };

    rocksdb::ColumnFamilyOptions getBaselineDefaultCFOptions();

    rocksdb::ColumnFamilyOptions getBaselineSeqnoCFOptions();

    // Helper function to apply the string-format 'newCfOptions' and
    // 'newBbtOptions' on top of 'cfOptions'.
    void applyUserCFOptions(rocksdb::ColumnFamilyOptions& cfOptions,
                            const std::string& newCfOptions,
                            const std::string& newBbtOptions);

    // Opens the DB on disk and instantiates 'rdb'. Also, it
    // populates 'vbHandles' with the ColumnFamilyHandles for all the
    // existing VBuckets.
    void openDB();

    /*
     * This function returns an instance of VBHandle for the given vbid.
     * See also getOrCreateVBHandle.
     *
     * @param vbid vbucket id for the vbucket DB to open
     */
    std::shared_ptr<VBHandle> getVBHandle(Vbid vbid) const;

    /*
     * This function returns an instance of VBHandle for the given vbid.
     * The VBHandle for 'vbid' is created if it does not exist.
     *
     * @param vbid vbucket id for the vbucket DB to open
     */
    std::shared_ptr<VBHandle> getOrCreateVBHandle(Vbid vbid);

    /*
     * The DB for each Shard is created in a separated subfolder of
     * 'configuration.getDBName()'. This function returns the path of the DB
     * subfolder for the current Shard.
     *
     * @return DB relative path for the current Shard
     */
    std::string getDBSubdir();

    /*
     * This function returns a set of pointers to all Caches allocated for
     * the rocksdb::DB instances managed by the current Shard.
     */
    std::unordered_set<const rocksdb::Cache*> getCachePointers() const;

    // This helper function adds all the block cache pointers of 'cfOptions'
    // to 'cache_set'
    static void addCFBlockCachePointers(
            const rocksdb::ColumnFamilyOptions& cfOptions,
            std::unordered_set<const rocksdb::Cache*>& cache_set);

    /*
     * This function returns the 'rocksdb::StatsLevel' value for the
     * 'stats_level' string representation given in input.
     */
    static rocksdb::StatsLevel getStatsLevel(const std::string& stats_level);

    rocksdb::Slice getKeySlice(const DiskDocKey& key) const;
    rocksdb::Slice getSeqnoSlice(const int64_t* seqno) const;
    rocksdb::Slice getSeqnoSlice(const uint64_t* seqno) const;
    int64_t getNumericSeqno(const rocksdb::Slice& seqnoSlice) const;

    std::unique_ptr<Item> makeItem(Vbid vb,
                                   const DiskDocKey& key,
                                   const rocksdb::Slice& s,
                                   bool includeValue) const;

    GetValue makeGetValue(Vbid vb,
                          const DiskDocKey& key,
                          const rocksdb::Slice& value,
                          bool includeValue) const;

    /**
     * Read the state of the given vBucket from disk and load into the cache
     */
    void loadVBStateCache(const VBHandle& vbh);

    ReadVBStateResult readVBStateFromDisk(const VBHandle& vbh) const;

    // Serialize the vbucket state and add it to the local CF in the specified
    // batch of writes.
    rocksdb::Status saveVBStateToBatch(const VBHandle& db,
                                       const vbucket_state& vbState,
                                       rocksdb::WriteBatch& batch);

    rocksdb::Status saveDocs(Vbid vbid,
                             VB::Commit& commitData,
                             const PendingRequestQueue& commitBatch);

    rocksdb::Status addRequestToWriteBatch(const VBHandle& db,
                                           rocksdb::WriteBatch& batch,
                                           const RocksRequest& request);

    void commitCallback(TransactionContext& txnCtx,
                        rocksdb::Status status,
                        const PendingRequestQueue& commitBatch);

    int64_t readHighSeqnoFromDisk(const VBHandle& db) const;

    int64_t getVbstateKey() const;

    // Helper function to retrieve stats from the RocksDB MemoryUtil API.
    bool getStatFromMemUsage(const rocksdb::MemoryUtil::UsageType type,
                             size_t& value) const;

    // Helper function to retrieve stats from the RocksDB Statistics API.
    bool getStatFromStatistics(const rocksdb::Tickers ticker,
                               size_t& value) const;

    // Helper function to retrieve stats from the RocksDB Property API for a
    // given Column Family.
    bool getStatFromProperties(ColumnFamily cf,
                               const std::string& property,
                               size_t& value) const;

    // The Memtable Quota is given by the 'rocksdb_memtables_ratio'
    // configuration parameter as ratio of the Bucket Quota. This function
    // calculates and applies the Memtable size for every single ColumnFamily
    // depending on the *current state* of the store. Thus, it must be called
    // under lock on 'vbhMutex', so that the number of VBuckets seen is
    // consistent.
    void applyMemtablesQuota(const std::lock_guard<std::mutex>&);

    // Returns the current number of VBuckets managed by the underlying
    // RocksDB instance. It must be called under lock on 'vbhMutex', so that
    // the number of VBuckets seen is consistent.
    size_t getVBucketsCount(const std::lock_guard<std::mutex>&) const;

    /// private getWithHeader shared with public get and getWithHeader
    GetValue getWithHeader(const DiskDocKey& key,
                           Vbid vb,
                           ValueFilter filter) const;

    rocksdb::WriteOptions writeOptions;

    // RocksDB does *not* need additional synchronisation around
    // db->Write, but we need to prevent delVBucket racing with
    // commit, potentially losing data.
    std::mutex writeMutex;

    // The number of total hits in the SeqnoCF when executing 'scan()'.
    // Note that it is equal to number of times we perform a point lookup from
    // the DefaultCF. Mutable as scan is logically const.
    mutable cb::NonNegativeCounter<size_t> scanTotalSeqnoHits;
    // The number of hits of old seqnos in the SeqnoCF when executing 'scan()'.
    // This is the number of times we perform a "useless" point lookup from the
    // DefaultCF (caused by old seqnos never deleted from the SeqnoCF).
    // Mutable as scan is logically const.
    mutable cb::NonNegativeCounter<size_t> scanOldSeqnoHits;

    struct SnapshotDeleter {
        explicit SnapshotDeleter(rocksdb::DB& db) : db(db) {
        }
        void operator()(const rocksdb::Snapshot* s) {
            db.ReleaseSnapshot(s);
        }
        rocksdb::DB& db;
    };
    using SnapshotPtr =
            std::unique_ptr<const rocksdb::Snapshot, SnapshotDeleter>;

    class RocksDBHandle : public ::KVFileHandle {
    public:
        RocksDBHandle(const RocksDBKVStore& kvstore, rocksdb::DB& db);
        SnapshotPtr snapshot;
    };

    BucketLogger& logger;
};

struct RocksDBKVStoreTransactionContext : public TransactionContext {
    // Defined in the .cc so that we don't need the full inclusion of
    // RocksRequest
    RocksDBKVStoreTransactionContext(KVStore& kvstore,
                                     Vbid vbid,
                                     std::unique_ptr<PersistenceCallback> cb);

    // Used for queueing mutation requests (in `set` and `del`) and flushing
    // them to disk (in `commit`).
    // unique_ptr for pimpl.
    std::unique_ptr<RocksDBKVStore::PendingRequestQueue> pendingReqs;
};
