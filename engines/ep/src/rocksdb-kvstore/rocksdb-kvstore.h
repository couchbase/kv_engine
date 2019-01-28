/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

/**
 * Experimental RocksDB KVStore implementation
 *
 * Uses RocksDB (https://github.com/facebook/rocksdb) as a backend.
 */

#pragma once

#include "../objectregistry.h"
#include "collections/collection_persisted_stats.h"
#include "vbucket_bgfetch_item.h"

#include <platform/dirutils.h>
#include <platform/non_negative_counter.h>
#include <map>
#include <vector>

#include <kvstore.h>

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
    EventListener(EventuallyPersistentEngine* epe) : engine(epe) {
    }
    void OnFlushBegin(rocksdb::DB*, const rocksdb::FlushJobInfo&) override {
        ObjectRegistry::onSwitchThread(engine, false);
    }
    // Called at the beginning of a Compaction job
    rocksdb::CompactionEventListener* GetCompactionEventListener() override {
        ObjectRegistry::onSwitchThread(engine, false);
        return nullptr;
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
     * Constructor
     *
     * @param config    Configuration information
     */
    RocksDBKVStore(RocksDBKVStoreConfig& config);

    ~RocksDBKVStore();

    void operator=(RocksDBKVStore& from) = delete;

    /**
     * Reset database to a clean state.
     */
    void reset(Vbid vbucketId) override;

    /**
     * Begin a transaction (if not already in one).
     */
    bool begin(std::unique_ptr<TransactionContext> txCtx) override;

    /**
     * Commit a transaction (unless not currently in one).
     *
     * Returns false if the commit fails.
     */
    bool commit(Collections::VB::Flush& collectionsFlush) override;

    /**
     * Rollback a transaction (unless not currently in one).
     */
    void rollback() override;

    /*
     * Get a RocksDBKVStore specific stat
     *
     * @param name The name of the statistic to fetch.
     * @param[out] value Value of the given stat (if exists).
     * @return True if the stat exists, is of type size_t and was successfully
     *         returned, else false.
     */
    bool getStat(const char* name, size_t& value) override;

    /**
     * Query the properties of the underlying storage.
     */
    StorageProperties getStorageProperties() override;

    /**
     * Overrides set().
     */
    void set(const Item& item,
             Callback<TransactionContext, mutation_result>& cb) override;

    /**
     * Overrides get().
     */
    GetValue get(const StoredDocKey& key,
                 Vbid vb,
                 bool fetchDelete = false) override;

    GetValue getWithHeader(void* dbHandle,
                           const StoredDocKey& key,
                           Vbid vb,
                           GetMetaOnly getMetaOnly,
                           bool fetchDelete = false) override;

    void getMulti(Vbid vb, vb_bgfetch_queue_t& itms) override;

    /**
     * Overrides del().
     */
    void del(const Item& itm, Callback<TransactionContext, int>& cb) override;

    // This is a blocking call. The function waits until other threads have
    // finished processing on a VBucket DB (e.g., 'commit') before deleting
    // the VBucket and returning to the caller.
    void delVBucket(Vbid vbucket, uint64_t vb_version) override;

    std::vector<vbucket_state*> listPersistedVbuckets(void) override;

    /**
     * Take a snapshot of the stats in the main DB.
     */
    bool snapshotStats(const std::map<std::string, std::string>& m);
    /**
     * Take a snapshot of the vbucket states in the main DB.
     */
    bool snapshotVBucket(Vbid vbucketId,
                         const vbucket_state& vbstate,
                         VBStatePersist options) override;

    void destroyInvalidVBuckets(bool);

    size_t getNumShards();

    void optimizeWrites(std::vector<queued_item>&) {
    }

    uint16_t getNumVbsPerFile(void) override {
        // TODO vmx 2016-10-29: return the actual value
        return 1024;
    }

    bool compactDB(compaction_ctx*) override {
        // Explicit compaction is not needed.
        // Compaction is continuously occurring in separate threads
        // under RocksDB's control
        return true;
    }

    Vbid getDBFileId(const cb::mcbp::Request&) override {
        // Not needed if there is no explicit compaction
        return Vbid(0);
    }

    vbucket_state* getVBucketState(Vbid vbucketId) override {
        return cachedVBStates[vbucketId.get()].get();
    }

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

    size_t getItemCount(Vbid vbid) override {
        // TODO vmx 2016-10-29: implement
        return 0;
    }

    RollbackResult rollback(Vbid vbid,
                            uint64_t rollbackSeqno,
                            std::shared_ptr<RollbackCB> cb) override {
        // TODO vmx 2016-10-29: implement
        // NOTE vmx 2016-10-29: For LevelDB/RocksDB it will probably
        // always be a full rollback as it doesn't support Couchstore
        // like rollback semantics
        return RollbackResult(false, 0, 0, 0);
    }

    void pendingTasks() override {
        // NOTE vmx 2016-10-29: Intentionally left empty;
    }

    ENGINE_ERROR_CODE getAllKeys(
            Vbid vbid,
            const DocKey start_key,
            uint32_t count,
            std::shared_ptr<Callback<const DocKey&>> cb) override {
        // TODO vmx 2016-10-29: implement
        return ENGINE_SUCCESS;
    }

    ScanContext* initScanContext(
            std::shared_ptr<StatusCallback<GetValue>> cb,
            std::shared_ptr<StatusCallback<CacheLookup>> cl,
            Vbid vbid,
            uint64_t startSeqno,
            DocumentFilter options,
            ValueFilter valOptions) override;

    scan_error_t scan(ScanContext* sctx) override;

    void destroyScanContext(ScanContext* ctx) override;

    Collections::VB::PersistedManifest getCollectionsManifest(
            Vbid vbid) override {
        // TODO DJR 2017-05-19 implement this.
        return {};
    }

    std::unique_ptr<KVFileHandle, KVFileHandleDeleter> makeFileHandle(
            Vbid vbid) override {
        // TODO JWW 2018-07-30 implement this fully - for now return something
        // as this function is called from warmup

        return std::unique_ptr<KVFileHandle, KVFileHandleDeleter>{
                new KVFileHandle(*this)};
    }

    void freeFileHandle(KVFileHandle* kvFileHandle) const override {
        // TODO JWW 2018-08-14 implement this dependent on makeFileHandle
        delete kvFileHandle;
    }

    Collections::VB::PersistedStats getCollectionStats(
            const KVFileHandle& kvFileHandle,
            CollectionID collection) override {
        // TODO JWW 2018-07-30 implement this
        return {};
    }

    void incrementRevision(Vbid vbid) override {
        // TODO DJR 2017-05-19 implement this.
    }

    uint64_t prepareToDelete(Vbid vbid) override {
        // TODO DJR 2017-05-19 implement this.
        return 0;
    }

    Collections::KVStore::Manifest getCollectionsManifest_new(
            Vbid vbid) override {
        throw std::logic_error(
                "Unimplemented RocksDBKVStore getCollectionsManifest");
        return {};
    }

    std::vector<Collections::KVStore::DroppedCollection> getDroppedCollections(
            Vbid vbid) override {
        throw std::logic_error(
                "Unimplemented RocksDBKVStore getDroppedCollections");
        return {};
    }

protected:
    // Write a batch of updates to the given database; measuring the time
    // taken and adding the timer to the commit histogram.
    rocksdb::Status writeAndTimeBatch(rocksdb::WriteBatch batch);

private:
    // Unique RocksDB instance, per-Shard.
    std::unique_ptr<rocksdb::DB> rdb;

    // Guards access to the 'vbHandles' vector. Users should lock this mutex
    // before accessing the vector to get a copy of any shared_ptr owned by
    // the vector. The mutex can be unlocked once a thread has its own copy
    // of the shared_ptr.
    std::mutex vbhMutex;

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
     * The VBHandle for 'vbid' is created if it does not exist.
     *
     * @param vbid vbucket id for the vbucket DB to open
     */
    std::shared_ptr<VBHandle> getVBHandle(Vbid vbid);

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
    std::unordered_set<const rocksdb::Cache*> getCachePointers();

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

    rocksdb::Slice getKeySlice(const DocKey& key);
    rocksdb::Slice getSeqnoSlice(const int64_t* seqno);
    int64_t getNumericSeqno(const rocksdb::Slice& seqnoSlice);

    std::unique_ptr<Item> makeItem(Vbid vb,
                                   const DocKey& key,
                                   const rocksdb::Slice& s,
                                   GetMetaOnly getMetaOnly);

    GetValue makeGetValue(Vbid vb,
                          const DocKey& key,
                          const std::string& value,
                          GetMetaOnly getMetaOnly = GetMetaOnly::No);

    void readVBState(const VBHandle& db);

    // Serialize the vbucket state and add it to the local CF in the specified
    // batch of writes.
    rocksdb::Status saveVBStateToBatch(const VBHandle& db,
                                       const vbucket_state& vbState,
                                       rocksdb::WriteBatch& batch);

    rocksdb::Status saveDocs(
            Vbid vbid,
            Collections::VB::Flush& collectionsFlush,
            const std::vector<std::unique_ptr<RocksRequest>>& commitBatch);

    rocksdb::Status addRequestToWriteBatch(const VBHandle& db,
                                           rocksdb::WriteBatch& batch,
                                           RocksRequest* request);

    void commitCallback(
            rocksdb::Status status,
            const std::vector<std::unique_ptr<RocksRequest>>& commitBatch);

    int64_t readHighSeqnoFromDisk(const VBHandle& db);

    int64_t getVbstateKey();

    // Helper function to retrieve stats from the RocksDB MemoryUtil API.
    bool getStatFromMemUsage(const rocksdb::MemoryUtil::UsageType type,
                             size_t& value);

    // Helper function to retrieve stats from the RocksDB Statistics API.
    bool getStatFromStatistics(const rocksdb::Tickers ticker, size_t& value);

    // Helper function to retrieve stats from the RocksDB Property API for a
    // given Column Family.
    bool getStatFromProperties(ColumnFamily cf,
                               const std::string& property,
                               size_t& value);

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

    // Used for queueing mutation requests (in `set` and `del`) and flushing
    // them to disk (in `commit`).
    std::vector<std::unique_ptr<RocksRequest>> pendingReqs;

    rocksdb::WriteOptions writeOptions;

    // RocksDB does *not* need additional synchronisation around
    // db->Write, but we need to prevent delVBucket racing with
    // commit, potentially losing data.
    std::mutex writeMutex;

    // This variable is used to verify that the KVStore API is used correctly
    // when RocksDB is used as store. "Correctly" means that the caller must
    // use the API in the following way:
    //      - begin() x1
    //      - set() / del() xN
    //      - commit()
    bool in_transaction;

    std::unique_ptr<TransactionContext> transactionCtx;

    std::atomic<size_t> scanCounter; // atomic counter for generating scan id

    // The number of total hits in the SeqnoCF when executing 'scan()'.
    // Note that it is equal to number of times we perform a point lookup from
    // the DefaultCF.
    cb::NonNegativeCounter<size_t> scanTotalSeqnoHits;
    // The number of hits of old seqnos in the SeqnoCF when executing 'scan()'.
    // This is the number of times we perform a "useless" point lookup from the
    // DefaultCF (caused by old seqnos never deleted from the SeqnoCF).
    cb::NonNegativeCounter<size_t> scanOldSeqnoHits;

    struct SnapshotDeleter {
        SnapshotDeleter(rocksdb::DB& db) : db(db) {
        }
        void operator()(const rocksdb::Snapshot* s) {
            db.ReleaseSnapshot(s);
        }
        rocksdb::DB& db;
    };
    using SnapshotPtr =
            std::unique_ptr<const rocksdb::Snapshot, SnapshotDeleter>;
    std::map<size_t, SnapshotPtr> scanSnapshots;
    std::mutex scanSnapshotsMutex;

    BucketLogger& logger;
};
