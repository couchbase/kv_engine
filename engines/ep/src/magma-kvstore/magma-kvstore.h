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
 * Magma info is used to mimic info that is stored internally in couchstore.
 * This info is stored with vbucket_state every time vbucket_state is stored.
 */
class MagmaInfo {
public:
    MagmaInfo() = default;

    // Note: we don't want to reset the kvstoreRev because it tracks
    // the kvstore revision which is a monotonically increasing
    // value each time the kvstore is created.
    void reset() {
        docCount = 0;
        persistedDeletes = 0;
    }

    cb::NonNegativeCounter<uint64_t> docCount{0};
    cb::NonNegativeCounter<uint64_t> persistedDeletes{0};

    // Magma API takes a 32bit KVStoreRevision
    Monotonic<uint32_t> kvstoreRev{1};
};

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

    MagmaDbStats(const MagmaDbStats& other)
        : docCount(other.docCount),
          onDiskPrepares(other.onDiskPrepares),
          highSeqno(other.highSeqno),
          purgeSeqno(other.purgeSeqno) {
    }

    MagmaDbStats& operator=(const MagmaDbStats& other) {
        docCount = other.docCount;
        onDiskPrepares = other.onDiskPrepares;
        highSeqno = other.highSeqno;
        purgeSeqno = other.purgeSeqno;
        return *this;
    }

    void reset(const MagmaDbStats& other) {
        docCount = other.docCount;
        onDiskPrepares = other.onDiskPrepares;
        highSeqno.reset(other.highSeqno);
        purgeSeqno.reset(other.purgeSeqno);
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

    int64_t docCount{0};
    int64_t onDiskPrepares{0};
    Monotonic<uint64_t> highSeqno{0};
    Monotonic<uint64_t> purgeSeqno{0};
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

    MagmaKVStore(MagmaKVStoreConfig& config);

    ~MagmaKVStore() override;

    void operator=(MagmaKVStore& from) = delete;

    /**
     * Reset database to a clean state.
     */
    void reset(Vbid vbid) override;

    /**
     * Begin a transaction (if not already in one).
     */
    bool begin(std::unique_ptr<TransactionContext> txCtx) override;

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
    //    required a kv_engine callback to pick up compaction_ctx and all
    //    compactions can remove expired items or dropped collection items.
    //
    //    Synchronous compaction is supported for testing. Normally, kv_engine
    //    with magma store should never call compactDB. But for testing, we
    //    need to support a synchronous call. When compactDB is called, it will
    //    save the compaction_ctx passed in to compactDB and will use it
    //    to perform compaction.
    bool compactDB(std::shared_ptr<compaction_ctx> ctx) override;

    Vbid getDBFileId(const cb::mcbp::Request&) override;

    size_t getNumPersistedDeletes(Vbid vbid) override {
        // TODO
        return 0;
    }

    DBFileInfo getDbFileInfo(Vbid vbid) override {
        // Magma does not support DBFileInfo
        DBFileInfo vbinfo;
        return vbinfo;
    }

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
            std::shared_ptr<Callback<const DiskDocKey&>> cb) override;

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

    std::optional<Collections::VB::PersistedStats> getCollectionStats(
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
     * @param collectionsFlush
     */
    void updateCollectionsMeta(Vbid vbid,
                               LocalDbReqs& localDbReqs,
                               Collections::VB::Flush& collectionsFlush);

    /**
     * Maintain the current uid committed
     *
     * @param localDbReqs vector of localDb updates
     */
    void updateManifestUid(LocalDbReqs& localDbReqs);

    /**
     * Maintain the list of open collections. The maintenance requires
     * reading the dropped collections which is passed back to avoid
     * a reread.
     *
     * @param vbid vbucket id
     * @param localDbReqs vector of localDb updates
     * @return dropped collection list
     */

    std::vector<Collections::KVStore::DroppedCollection> updateOpenCollections(
            Vbid vbid, LocalDbReqs& localDbReqs);

    /**
     * Maintain the list of dropped collections
     *
     * @param vbid vbucket id
     * @param localDbReqs vector of localDb updates
     * @param dropped This method will only read the dropped collections
     *   from storage if this optional is not initialised
     */
    void updateDroppedCollections(
            Vbid vbid,
            LocalDbReqs& localDbReqs,
            std::optional<std::vector<Collections::KVStore::DroppedCollection>>
                    dropped);

    /**
     * Maintain the list of open scopes
     *
     * @param vbid vbucket id
     * @param localDbReqs vector of localDb updates
     */
    void updateScopes(Vbid vbid, LocalDbReqs& localDbReqs);

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
     * @param stats The stats that should be persisted
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
     * Encode the cached vbucket_state and magmaInfo into a nlohmann json struct
     */
    nlohmann::json encodeVBState(const vbucket_state& vbstate,
                                 const MagmaInfo& magmaInfo) const;

    /**
     * Read the vbstate from disk and load into cache
     */
    magma::Status loadVBStateCache(Vbid vbid);

    /**
     * Write the vbucket_state and MagmaInfo to disk.
     * This is done outside an atomic batch so we need to
     * create a batch to write it.
     *
     * @param vbid vbucket id
     * @param vbstate vbucket state
     * @param minfo MagmaInfo
     * @return status
     */
    magma::Status writeVBStateToDisk(Vbid vbid,
                                     const vbucket_state& vbstate,
                                     const MagmaInfo& minfo);

    /**
     * Return value of readVBStateFromDisk. Would have problems assigning
     * MagmaInfo otherwise as it include a Monotonic.
     */
    struct DiskState {
        magma::Status status;
        vbucket_state vbstate;
        MagmaInfo magmaInfo;
    };

    /**
     * Read the encoded vstate + docCount from the local db.
     */
    DiskState readVBStateFromDisk(Vbid vbid);

    /**
     * Write the encoded vbstate + docCount to the local db.
     */
    void addVBStateUpdateToLocalDbReqs(LocalDbReqs& localDbReqs,
                                       const vbucket_state& vbs,
                                       const MagmaInfo& minfo);

    /**
     * Get vbstate from cache. If cache not populated, read it from disk
     * and populate cache. If not on disk, return nullptr;
     *
     * vbstate and magmaInfo always go together, we should never have a
     * case where only 1 of them is initialized. So, if vbstate is
     * uninitilized, assume magmaInfo is as well.
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
    std::shared_ptr<compaction_ctx> makeCompactionContext(Vbid vbid);

    const KVStoreConfig& getConfig() const override;

    // Magma uses a unique logger with a prefix of magma so that all logging
    // calls from the wrapper thru magma will be prefixed with magma.
    std::shared_ptr<BucketLogger> logger;

protected:
    /**
     * CompactDB implementation. See comments on public compactDB.
     */
    bool compactDBInternal(std::shared_ptr<compaction_ctx> ctx);

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

    /**
     * @return a reference to the MagmaInfo for the vbid, created on demand
     */
    MagmaInfo& getMagmaInfo(Vbid vbid);

    /// private getWithHeader shared with public get and getWithHeader
    GetValue getWithHeader(const DiskDocKey& key,
                           Vbid vbid,
                           GetMetaOnly getMetaOnly);

    class MagmaCompactionCB : public magma::Magma::CompactionCallback {
    public:
        MagmaCompactionCB(MagmaKVStore& magmaKVStore);
        ~MagmaCompactionCB() override;
        bool operator()(const magma::Slice& keySlice,
                        const magma::Slice& metaSlice,
                        const magma::Slice& valueSlice) override {
            return magmaKVStore.compactionCallBack(
                    *this, keySlice, metaSlice, valueSlice);
        }
        MagmaKVStore& magmaKVStore;
        bool initialized{false};
        std::stringstream itemKeyBuf;
        std::shared_ptr<compaction_ctx> magmaCompactionCtx;
        compaction_ctx* ctx{nullptr};
        Vbid vbid{};
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

    // Magma does *not* need additional synchronisation around
    // db->Write, but we need to prevent delVBucket racing with
    // commit, potentially losing data.
    std::mutex writeLock;

    // This variable is used to verify that the KVStore API is used correctly
    // when Magma is used as store. "Correctly" means that the caller must
    // use the API in the following way:
    //      - begin() x1
    //      - set() / del() xN
    //      - commit()
    bool in_transaction;
    std::unique_ptr<TransactionContext> transactionCtx;

    // Path to magma files. Include shardID.
    const std::string magmaPath;

    std::atomic<size_t> scanCounter; // atomic counter for generating scan id

    // Magma does not keep track of docCount, # of persistedDeletes or
    // revFile internal so we need a mechanism to do that. We use magmaInfo
    // as the structure to store that and we save magmaInfo with the vbstate.
    std::vector<std::unique_ptr<MagmaInfo>> cachedMagmaInfo;

    // For testing, we need to simulate couchstore where every batch
    // is a potential rollback point.
    bool doCommitEveryBatch{false};

    // Using upsert for Set means we can't keep accurate document totals.
    // This is used for testing only!
    bool useUpsertForSet{false};

    // This needs to be a shared_ptr because its possible an implicit
    // compaction kicks off while an explicit compaction is happening
    // and we don't want to free it while the implicit compaction is working.
    std::vector<std::shared_ptr<compaction_ctx>> compaction_ctxList;
    std::mutex compactionCtxMutex;

    folly::Synchronized<std::queue<std::tuple<Vbid, uint64_t>>>
            pendingVbucketDeletions;

    friend class MagmaCompactionCB;
};
