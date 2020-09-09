/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc.
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

#include "collections/collection_persisted_stats.h"
#include "kvstore.h"
#include "kvstore_priv.h"
#include "libmagma/magma.h"
#include "rollback_result.h"
#include "vbucket_bgfetch_item.h"
#include "vbucket_state.h"

#include <folly/Synchronized.h>
#include <platform/dirutils.h>
#include <platform/non_negative_counter.h>

#include <map>
#include <queue>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

namespace magma {
class Slice;
class Status;
} // namespace magma

class MagmaRequest;
class MagmaKVStoreConfig;
class MagmaCompactionCB;
struct kvstats_ctx;
struct vbucket_state;

/**
 * MagmaDbStats are a set of stats maintained within the Magma KVStore
 * rather than on the vbucket_state. This is because compaction is unable
 * to update the vbstate since it runs in a different thread than the
 * BG Flusher thread. Whenver we retrieve vbstate, we read the MagmaDbStats
 * which is stored in  magma (in the latest state file) and replace them
 * overtop the vbstate values.
 */
class MagmaDbStats : public magma::UserStats {
public:
    explicit MagmaDbStats() = default;

    MagmaDbStats(int64_t docCount,
                 uint64_t purgeSeqno) {
        auto locked = stats.wlock();
        locked->reset(docCount, purgeSeqno);
    }

    MagmaDbStats(const MagmaDbStats& other) {
        *this = other;
    }

    MagmaDbStats& operator=(const MagmaDbStats& other) {
        auto locked = stats.wlock();
        auto otherLocked = other.stats.rlock();
        *locked = *otherLocked;
        return *this;
    }

    void reset(const MagmaDbStats& other) {
        auto locked = stats.wlock();
        auto otherLocked = other.stats.rlock();
        locked->reset(otherLocked->docCount,
                      otherLocked->purgeSeqno);
    }

    /**
     * Merge the stats with existing stats.
     * For docCount and onDiskPrepares, we add the delta.
     * For highSeqno and purgeSeqno, we set them if the new
     * value is higher than the old value.
     *
     * @param other should be a MagmaDbStats instance
     */
    void Merge(const magma::UserStats& other) override;

    /**
     * clone the stats
     *
     * @return MagmaDbStats
     */
    std::unique_ptr<magma::UserStats> Clone() override;

    /**
     * Marshal the stats into a json string
     *
     * @return string MagmaDbStats in a json string format
     * @throws logic_error if unable to parse json
     */
    std::string Marshal() override;

    /**
     * Unmarshal the json string into DbStats
     *
     * @return Status potential error code; for magma wrapper we throw
     *                errors rather than return status.
     */
    magma::Status Unmarshal(const std::string& encoded) override;

    struct Stats {
        Stats() = default;

        Stats(const Stats& other) {
            docCount = other.docCount;
            purgeSeqno = other.purgeSeqno;
        }

        void reset(int64_t docCount,
                   uint64_t purgeSeqno) {
            this->docCount = docCount;
            this->purgeSeqno.reset(purgeSeqno);
        }

        int64_t docCount{0};
        Monotonic<uint64_t> purgeSeqno{0};
    };

    folly::Synchronized<Stats> stats;
};

/**
 * MagmaScanContext is BySeqnoScanContext with the magma
 * iterator added.
 */
class MagmaScanContext : public BySeqnoScanContext {
public:
    MagmaScanContext(std::unique_ptr<StatusCallback<GetValue>> cb,
                     std::unique_ptr<StatusCallback<CacheLookup>> cl,
                     Vbid vb,
                     std::unique_ptr<KVFileHandle> handle,
                     int64_t start,
                     int64_t end,
                     uint64_t purgeSeqno,
                     DocumentFilter _docFilter,
                     ValueFilter _valFilter,
                     uint64_t _documentCount,
                     const vbucket_state& vbucketState,
                     const std::vector<Collections::KVStore::DroppedCollection>&
                             droppedCollections,
                     std::unique_ptr<magma::Magma::SeqIterator> itr);

    std::unique_ptr<magma::Magma::SeqIterator> itr{nullptr};
};

/**
 * A persistence store based on magma.
 */
class MagmaKVStore : public KVStore {
public:
    using WriteOps = std::vector<magma::Magma::WriteOperation>;
    using ReadOps = std::vector<magma::Slice>;

    /**
     * A localDb request is used to scope the memory required for
     * inserting a localDb update into the kvstore localDb.
     */
    class MagmaLocalReq {
    public:
        MagmaLocalReq(std::string_view key,
                      std::string&& value,
                      bool deleted = false)
            : key(key), value(std::move(value)), deleted(deleted) {
        }

        // Some localDb Reqs come from flatbuffers. We need to convert
        // the flatbuffer into a string.
        MagmaLocalReq(std::string_view key,
                      const flatbuffers::DetachedBuffer& buf);

        static MagmaLocalReq makeDeleted(std::string_view key,
                                         std::string&& value = {}) {
            return MagmaLocalReq(
                    std::move(key), std::move(value), true /*deleted*/);
        }

        std::string key;
        std::string value;
        bool deleted{false};
    };

    using LocalDbReqs = std::vector<MagmaLocalReq>;

    /**
     * Add localDbReqs to the WriteOps vector to be inserted into
     * the kvstore.
     *
     * @param localDbReqs vector of localDb updates
     * @param writeOps vector of Magma::WriteOperations's
     */
    void addLocalDbReqs(const LocalDbReqs& localDbReqs, WriteOps& writeOps);

    /**
     * Add MagmaDbStats to the WriteOps vector to be inserted into the kvstore.
     * @param stats The MagmaDbStats object
     * @param writeOps vector of Magma::WriteOperations
     */
    void addStatUpdateToWriteOps(MagmaDbStats& stats, WriteOps& writeOps) const;

    MagmaKVStore(MagmaKVStoreConfig& config);

    ~MagmaKVStore() override;

    void operator=(MagmaKVStore& from) = delete;

    /**
     * Reset database to a clean state.
     */
    void reset(Vbid vbid) override;

    /**
     * Commit a transaction (unless not currently in one).
     *
     * Returns false if the commit fails.
     */
    bool commit(VB::Commit& commitData) override;

    /**
     * Rollback a transaction (unless not currently in one).
     */
    void rollback() override;

    StorageProperties getStorageProperties() override;

    void setMaxDataSize(size_t size) override;

    /**
     * Get magma stats
     *
     * @param name stat name
     * @param value returned value when function return is true
     * @return true if stat found, value is set
     */
    bool getStat(const char* name, size_t& value) override;

    /**
     * Adds a request to a queue for batch processing at commit()
     */
    void set(queued_item itm) override;

    GetValue get(const DiskDocKey& key, Vbid vb) override;

    GetValue getWithHeader(const KVFileHandle& kvFileHandle,
                           const DiskDocKey& key,
                           Vbid vb,
                           GetMetaOnly getMetaOnly) override;

    void getMulti(Vbid vb, vb_bgfetch_queue_t& itms) override;

    void getRange(Vbid vb,
                  const DiskDocKey& startKey,
                  const DiskDocKey& endKey,
                  const GetRangeCb& cb) override;

    void del(queued_item itm) override;

    void delVBucket(Vbid vbucket, uint64_t fileRev) override;

    std::vector<vbucket_state*> listPersistedVbuckets() override;

    /**
     * Take a snapshot of the stats in the main DB.
     */
    bool snapshotStats(const std::map<std::string, std::string>& m) {
        // TODO
        return false;
    }

    /**
     * Take a snapshot of the vbucket states in the main DB.
     */
    bool snapshotVBucket(Vbid vbucketId, const vbucket_state& vbstate) override;

    // Compaction in magma is asynchronous. Its triggered by 3 conditions:
    //  - Level compaction
    //  - Expiry compaction
    //    In magma, a histogram of when items will expire is maintained.
    //    Periodically, magma will trigger a compaction to visit those
    //    sstables which have expired items to have them removed.
    //  - Dropped collections removal
    //    At the end of the flusher loop in kv_engine, a call to magma will be
    //    made to trigger asynchronous PurgeRange scans to purge the store
    //    of any dropped collections. For each collectionID, 2 scans will be
    //    triggered, [Default+CollectionID] and
    //    [DurabilityPrepare+CollectionID].
    //    While a purge scan will identify the sstables containing the scan
    //    key ex.[Default+CollectionID], all items for that CollectionID will
    //    be removed ie. both Default & DurabilityPrepare. This guarantees that
    //    the data from each sstable containing the collectionID is visited
    //    once.
    //    Also during this call from the bg flusher, any completed compaction
    //    data will be picked up. The max_purged_seq will update the vbstate
    //    and any dropped collections will trigger 2 scans (same as above) to
    //    determine if a collection has been removed and if so, the collection
    //    manifest is updated.
    //
    //    Regardless of which type of compaction is running, all compactions
    //    required a kv_engine callback to pick up CompactionContext and all
    //    compactions can remove expired items or dropped collection items.
    //
    //    Synchronous compaction is supported for testing. Normally, kv_engine
    //    with magma store should never call compactDB. But for testing, we
    //    need to support a synchronous call. When compactDB is called, it will
    //    save the CompactionContext passed in to compactDB and will use it
    //    to perform compaction.
    bool compactDB(std::unique_lock<std::mutex>& vbLock,
                   std::shared_ptr<CompactionContext> ctx) override;

    size_t getNumPersistedDeletes(Vbid vbid) override {
        // TODO
        return 0;
    }

    DBFileInfo getDbFileInfo(Vbid vbid) override;

    DBFileInfo getAggrDbFileInfo() override;

    size_t getItemCount(Vbid vbid) override;

    RollbackResult rollback(Vbid vbid,
                            uint64_t rollbackSeqno,
                            std::unique_ptr<RollbackCB>) override;

    void pendingTasks() override;

    ENGINE_ERROR_CODE getAllKeys(
            Vbid vbid,
            const DiskDocKey& start_key,
            uint32_t count,
            std::shared_ptr<StatusCallback<const DiskDocKey&>> cb) override;

    std::unique_ptr<BySeqnoScanContext> initBySeqnoScanContext(
            std::unique_ptr<StatusCallback<GetValue>> cb,
            std::unique_ptr<StatusCallback<CacheLookup>> cl,
            Vbid vbid,
            uint64_t startSeqno,
            DocumentFilter options,
            ValueFilter valOptions,
            SnapshotSource source) override;

    std::unique_ptr<ByIdScanContext> initByIdScanContext(
            std::unique_ptr<StatusCallback<GetValue>> cb,
            std::unique_ptr<StatusCallback<CacheLookup>> cl,
            Vbid vbid,
            const std::vector<ByIdRange>& ranges,
            DocumentFilter options,
            ValueFilter valOptions) override;

    scan_error_t scan(BySeqnoScanContext& sctx) override;
    scan_error_t scan(ByIdScanContext& ctx) override;

    class MagmaKVFileHandle : public ::KVFileHandle {
    public:
        MagmaKVFileHandle(Vbid vbid) : vbid(vbid) {
        }
        Vbid vbid;
    };

    std::unique_ptr<KVFileHandle> makeFileHandle(Vbid vbid) override;

    Collections::VB::PersistedStats getCollectionStats(
            const KVFileHandle& kvFileHandle, CollectionID collection) override;

    /**
     * Increment the kvstore revision.
     */
    void prepareToCreateImpl(Vbid vbid) override;

    /**
     * Soft delete the kvstore.
     */
    uint64_t prepareToDeleteImpl(Vbid vbid) override;

    /**
     * Retrieve the manifest from the local db.
     * MagmaKVStore implements this method as a read of 3 _local documents
     * manifest, open collections, open scopes
     *
     * @param vbid vbucket id
     * @return collections manifest
     */
    Collections::KVStore::Manifest getCollectionsManifest(Vbid vbid) override;

    /**
     * read local document to get the vector of dropped collections
     * @param vbid vbucket id
     * @return a vector of dropped collections (can be empty)
     */
    std::vector<Collections::KVStore::DroppedCollection> getDroppedCollections(
            Vbid vbid) override;

    /**
     * This function maintains the set of open collections, adding newly opened
     * collections and removing those which are dropped. To validate the
     * creation of new collections, this method must read the dropped
     * collections.
     *
     * @param vbid vbucket id
     * @param localDbReqs vector of localDb updates
     * @param collectionsFlush flush object for a single 'flush/commit'
     */
    void updateCollectionsMeta(Vbid vbid,
                               LocalDbReqs& localDbReqs,
                               Collections::VB::Flush& collectionsFlush);

    /**
     * Maintain the current uid committed
     *
     * @param localDbReqs vector of localDb updates
     * @param collectionsFlush flush object for a single 'flush/commit'
     */
    void updateManifestUid(LocalDbReqs& localDbReqs,
                           Collections::VB::Flush& collectionsFlush);

    /**
     * Maintain the list of open collections. The maintenance requires
     * reading the dropped collections which is passed back to avoid
     * a reread.
     *
     * @param vbid vbucket id
     * @param localDbReqs vector of localDb updates
     * @param collectionsFlush flush object for a single 'flush/commit'
     */
    void updateOpenCollections(Vbid vbid,
                               LocalDbReqs& localDbReqs,
                               Collections::VB::Flush& collectionsFlush);

    /**
     * Maintain the list of dropped collections
     *
     * @param vbid vbucket id
     * @param localDbReqs vector of localDb updates
     * @param collectionsFlush flush object for a single 'flush/commit'
     */
    void updateDroppedCollections(Vbid vbid,
                                  LocalDbReqs& localDbReqs,
                                  Collections::VB::Flush& collectionsFlush);

    /**
     * Maintain the list of open scopes
     *
     * @param vbid vbucket id
     * @param localDbReqs vector of localDb updates
     * @param collectionsFlush flush object for a single 'flush/commit'
     */
    void updateScopes(Vbid vbid,
                      LocalDbReqs& localDbReqs,
                      Collections::VB::Flush& collectionsFlush);

    /**
     * Given a collection id, return the key used to maintain the
     * collection stats in the local db.
     *
     * @param cid Collection ID
     */
    std::string getCollectionsStatsKey(CollectionID cid);

    /**
     * Save stats for collection cid
     *
     * @param localDbReqs vector of localDb updates
     * @param cid Collection ID
     * @param stats The stats that should be applied and persisted
     */
    void saveCollectionStats(LocalDbReqs& localDbReqs,
                             CollectionID cid,
                             const Collections::VB::PersistedStats& stats);

    /**
     * Delete the collection stats for the given collection id
     *
     * @param localDbReqs vector of localDb updates
     * @param cid Collection ID
     */
    void deleteCollectionStats(LocalDbReqs& localDbReqs, CollectionID cid);

    /**
     * Read from local DB
     */
    std::pair<magma::Status, std::string> readLocalDoc(
            Vbid vbid, const magma::Slice& keySlice);

    /**
     * Encode the cached vbucket_state into a nlohmann json struct
     */
    nlohmann::json encodeVBState(const vbucket_state& vbstate,
                                 uint64_t kvstoreRev) const;

    /**
     * Read the vbstate from disk and load into cache
     */
    magma::Status loadVBStateCache(Vbid vbid, bool resetKVStoreRev = false);

    /**
     * Write the vbucket_state to disk.
     * This is done outside an atomic batch so we need to
     * create a WriteDocs batch to write it.
     *
     * @param vbid vbucket id
     * @param vbstate vbucket state
     * @return status
     */
    magma::Status writeVBStateToDisk(Vbid vbid, const vbucket_state& vbstate);

    /**
     * Return value of readVBStateFromDisk.
     */
    struct DiskState {
        magma::Status status;
        vbucket_state vbstate;

        // Revision number of the vBucket/kvstore that we store in the
        // vbucket_state local doc for magma vBuckets
        uint64_t kvstoreRev;
    };

    /**
     * Read the encoded vstate + docCount from the local db.
     */
    DiskState readVBStateFromDisk(Vbid vbid);

    /**s
     * Read the encoded vbstate from the given snapshot.
     */
    virtual DiskState readVBStateFromDisk(Vbid vbid,
                                          magma::Magma::Snapshot& snapshot);

    /**
     * Write the encoded vbstate to localDb.
     */
    void addVBStateUpdateToLocalDbReqs(LocalDbReqs& localDbReqs,
                                       const vbucket_state& vbs,
                                       uint64_t kvstoreRev);

    /**
     * Whenever we go to cachedVBStates, we need to merge in
     * the MagmaDbStats stats to make the vbstate current. We read
     * the MagmaDbStats stats from magma which are kept on the state file.
     */
    void mergeMagmaDbStatsIntoVBState(vbucket_state& vbstate, Vbid vbid);

    /**
     * Get vbstate from cache. If cache not populated, read it from disk
     * and populate cache. If not on disk, return nullptr;
     *
     * Note: When getting the vbstate, we merge in the MagmaDbStats stats.
     * See mergeMagmaDbStatsIntoVBState() above.
     */
    vbucket_state* getVBucketState(Vbid vbucketId) override;

    /**
     * Populate kvstore stats with magma specific stats
     */
    void addStats(const AddStatFn& add_stat,
                  const void* c,
                  const std::string& args) override;

    /**
     * Construct a compaction context for use with implicit compactions. Calls
     * back up to the bucket to do so as we need certain callbacks and config.
     */
    std::shared_ptr<CompactionContext> makeCompactionContext(Vbid vbid);

    const KVStoreConfig& getConfig() const override;

    void setMagmaFragmentationPercentage(size_t value);

    /**
     * Set the number of magma flushers and compactors based on configuration
     * settings of number of backend threads, number of writer threads, and
     * percentage of flusher threads.
     */
    void calculateAndSetMagmaThreads();

    // Magma uses a unique logger with a prefix of magma so that all logging
    // calls from the wrapper thru magma will be prefixed with magma.
    std::shared_ptr<BucketLogger> logger;

protected:
    class ConfigChangeListener;

    /**
     * CompactDB implementation. See comments on public compactDB.
     */
    bool compactDBInternal(std::shared_ptr<CompactionContext> ctx);

    /*
     * The DB for each VBucket is created in a separated subfolder of
     * `configuration.getDBName()`. This function returns the path of the DB
     * subfolder for the given `vbid`.
     *
     * @param vbid vbucket id for the vbucket DB subfolder to return
     */
    std::string getVBDBSubdir(Vbid vbid);

    std::unique_ptr<Item> makeItem(Vbid vb,
                                   const magma::Slice& keySlice,
                                   const magma::Slice& metaSlice,
                                   const magma::Slice& valueSlice,
                                   GetMetaOnly getMetaOnly);

    GetValue makeGetValue(Vbid vb,
                          const magma::Slice& keySlice,
                          const magma::Slice& metaSlice,
                          const magma::Slice& valueSlice,
                          GetMetaOnly getMetaOnly = GetMetaOnly::No);

    virtual int saveDocs(VB::Commit& commitData, kvstats_ctx& kvctx);

    void commitCallback(int status, kvstats_ctx& kvctx);

    static ENGINE_ERROR_CODE magmaErr2EngineErr(magma::Status::Code err,
                                                bool found = true);

    /// private getWithHeader shared with public get and getWithHeader
    GetValue getWithHeader(const DiskDocKey& key,
                           Vbid vbid,
                           GetMetaOnly getMetaOnly);

    /**
     * MagmaCompactionCB is the class invoked by magma compactions,
     * both implicit and explicit. For explict compactions, which come
     * through compactDB, we pass the CompactionContext thru to the callback
     * routine. For implicit compaction, we call makeCompactionCtx to
     * create the CompactionContext on the fly.
     *
     * Since implicit compactions can run in a thread other than the
     * BG Writer thread, we keep track of MagmaDbStats stats during
     * compaction and magma will call the GetUserStats() routine and
     * merge them with the existing MagmaDbStats stats.
     */
    class MagmaCompactionCB : public magma::Magma::CompactionCallback {
    public:
        MagmaCompactionCB(MagmaKVStore& magmaKVStore,
                          std::shared_ptr<CompactionContext> ctx = nullptr);

        ~MagmaCompactionCB() override;
        bool operator()(const magma::Slice& keySlice,
                        const magma::Slice& metaSlice,
                        const magma::Slice& valueSlice) override {
            return magmaKVStore.compactionCallBack(
                    *this, keySlice, metaSlice, valueSlice);
        }
        const magma::UserStats* GetUserStats() override {
            return &magmaDbStats;
        }
        MagmaKVStore& magmaKVStore;

        // The only usage of this is is in compactionCallback and it could be a
        // local variable instead but it's less expensive to reset the contents
        // of the stringstream than it is to destroy/recreate the stringstream
        // for each key we visit.
        std::stringstream itemKeyBuf;

        std::shared_ptr<CompactionContext> ctx;

        MagmaDbStats magmaDbStats;
    };

    /**
     * Called for each item during compaction to determine whether we should
     * keep or drop the item (and drive expiry).
     *
     * @return true if the item should be dropped
     */
    bool compactionCallBack(MagmaKVStore::MagmaCompactionCB& cbCtx,
                            const magma::Slice& keySlice,
                            const magma::Slice& metaSlice,
                            const magma::Slice& valueSlice);

    /**
     * Get the MagmaDbStats from the Magma::KVStore
     * @param vbid
     * @return The MagmaDbStats (default constructed if they don't exist in
     * magma yet)
     */
    MagmaDbStats getMagmaDbStats(Vbid vbid);

    MagmaKVStoreConfig& configuration;

    /**
     * Mamga instance for a shard
     */
    std::unique_ptr<magma::Magma> magma;

    /**
     * Container for pending Magma requests.
     *
     * Using deque as as the expansion behaviour is less aggressive compared to
     * std::vector (MagmaRequest objects are ~176 bytes in size).
     */
    using PendingRequestQueue = std::deque<MagmaRequest>;

    // Used for queueing mutation requests (in `set` and `del`) and flushing
    // them to disk (in `commit`).
    // unique_ptr for pimpl.
    std::unique_ptr<PendingRequestQueue> pendingReqs;

    // Path to magma files. Include shardID.
    const std::string magmaPath;

    std::atomic<size_t> scanCounter; // atomic counter for generating scan id

    // Keep track of the vbucket revision
    std::vector<Monotonic<uint64_t>> kvstoreRevList;

    // For testing, we need to simulate couchstore where every batch
    // is a potential rollback point.
    bool doCommitEveryBatch{false};

    // Using upsert for Set means we can't keep accurate document totals.
    // This is used for testing only!
    bool useUpsertForSet{false};

    folly::Synchronized<std::queue<std::tuple<Vbid, uint64_t>>>
            pendingVbucketDeletions;
};
