/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include "../objectregistry.h"
#include "collections/collection_persisted_stats.h"
#include "kvstore.h"
#include "kvstore_priv.h"
#include "libmagma/magma.h"
#include "rollback_result.h"
#include "vbucket_bgfetch_item.h"

#include <folly/Synchronized.h>
#include <platform/dirutils.h>
#include <platform/non_negative_counter.h>

#include <map>
#include <queue>
#include <shared_mutex>
#include <string>
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
    Monotonic<uint64_t> kvstoreRev{1};
};

/**
 * A persistence store based on magma.
 */
class MagmaKVStore : public KVStore {
public:
    MagmaKVStore(MagmaKVStoreConfig& config);

    ~MagmaKVStore();

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

    std::vector<vbucket_state*> listPersistedVbuckets(void) override;

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
    //
    // For DP, we will support only Level and Sychronous compaction. This
    // alleviates the need for a callback to pick up compaction_ctx.
    bool compactDB(compaction_ctx*) override;

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

    DBFileInfo getAggrDbFileInfo() override {
        // Magma does not support DBFileInfo
        DBFileInfo vbinfo;
        return vbinfo;
    }

    size_t getItemCount(Vbid vbid) override;

    RollbackResult rollback(Vbid vbid,
                            uint64_t rollbackSeqno,
                            std::shared_ptr<RollbackCB> cb) override;

    void pendingTasks() override;

    ENGINE_ERROR_CODE getAllKeys(
            Vbid vbid,
            const DiskDocKey& start_key,
            uint32_t count,
            std::shared_ptr<Callback<const DiskDocKey&>> cb) override;

    ScanContext* initScanContext(
            std::shared_ptr<StatusCallback<GetValue>> cb,
            std::shared_ptr<StatusCallback<CacheLookup>> cl,
            Vbid vbid,
            uint64_t startSeqno,
            DocumentFilter options,
            ValueFilter valOptions) override;

    scan_error_t scan(ScanContext* sctx) override;

    void destroyScanContext(ScanContext* ctx) override;

    /**
     * The magmaKVHandle protects magma from a kvstore being dropped
     * while an API operation is active. This is required because
     * unlike couchstore which just unlinks the data file, magma
     * must wait for all threads to exit and then block subsequent
     * threads from proceeding while the kvstore is being dropped.
     * Inside the handle is the vbstateMutex. This mutex is used
     * to protect the vbstate from race conditions when updated.
     */
    struct MagmaKVHandleStruct {
        std::shared_timed_mutex vbstateMutex;
    };
    using MagmaKVHandle = std::shared_ptr<MagmaKVHandleStruct>;

    std::vector<std::pair<MagmaKVHandle, std::shared_timed_mutex>>
            magmaKVHandles;

    const MagmaKVHandle getMagmaKVHandle(Vbid vbid) {
        std::lock_guard<std::shared_timed_mutex> lock(
                magmaKVHandles[vbid.get()].second);
        return magmaKVHandles[vbid.get()].first;
    }

    class MagmaKVFileHandle : public ::KVFileHandle {
    public:
        MagmaKVFileHandle(MagmaKVStore& kvstore, Vbid vbid)
            : vbid(vbid), kvHandle(kvstore.getMagmaKVHandle(vbid)) {
        }
        Vbid vbid;
        MagmaKVHandle kvHandle;
    };

    std::unique_ptr<KVFileHandle> makeFileHandle(Vbid vbid) override;

    boost::optional<Collections::VB::PersistedStats> getCollectionStats(
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
     * creation
     * of new collections, this method must read the dropped collections.
     *
     * @param vbid vbucket id
     * @param commitBatch current magma commit batch
     * @param collectionsFlush
     * @return status magma status
     */
    magma::Status updateCollectionsMeta(
            Vbid vbid,
            magma::Magma::CommitBatch& commitBatch,
            Collections::VB::Flush& collectionsFlush);

    /**
     * Maintain the current uid committed
     *
     * @param vbid vbucket id
     * @param commitBatch current magma commit batch
     * @return status magma status
     */
    magma::Status updateManifestUid(magma::Magma::CommitBatch& commitBatch);

    /**
     * Maintain the list of open collections. The maintenance requires
     * reading the dropped collections which is passed back to avoid
     * a reread.
     *
     * @param vbid vbucket id
     * @param commitBatch current magma commit batch
     * @return pair magma status and dropped collection list
     */
    std::pair<magma::Status,
              std::vector<Collections::KVStore::DroppedCollection>>
    updateOpenCollections(Vbid vbid, magma::Magma::CommitBatch& commitBatch);

    /**
     * Maintain the list of dropped collections
     *
     * @param vbid vbucket id
     * @param commitBatch current magma commit batch
     * @param dropped This method will only read the dropped collections
     *   from storage if this optional is not initialised
     * @return status magma status
     */
    magma::Status updateDroppedCollections(
            Vbid vbid,
            magma::Magma::CommitBatch& commitBatch,
            boost::optional<
                    std::vector<Collections::KVStore::DroppedCollection>>
                    dropped);

    /**
     * Maintain the list of open scopes
     *
     * @param vbid vbucket id
     * @param commitBatch current magma commit batch
     * @return status magma status
     */
    magma::Status updateScopes(Vbid vbid,
                               magma::Magma::CommitBatch& commitBatch);

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
     * @param commitBatch current magma commit batch
     * @param cid Collection ID
     * @param stats The stats that should be persisted
     */
    void saveCollectionStats(magma::Magma::CommitBatch& commitBatch,
                             CollectionID cid,
                             const Collections::VB::PersistedStats& stats);

    /**
     * Delete the collection stats for the given collection id
     *
     * @param commitBatch current magma commit batch
     * @param cid Collection ID
     */
    magma::Status deleteCollectionStats(magma::Magma::CommitBatch& commitBatch,
                                        CollectionID cid);

    /**
     * Encode a document being stored in the local db by prefixing the
     * MetaData to the value.
     *
     * Magma requires all documents stored in local DB to also include
     * metadata because it uses the callback functions like getExpiryTime,
     * isDeleted() and getSeqNum() as part of compaction.
     */
    std::string encodeLocalDoc(Vbid vbid,
                               const std::string& value,
                               bool isDelete);

    /**
     * Decode a document being stored in the local db by extracting the
     * MetaData from the value
     */
    std::pair<std::string, bool> decodeLocalDoc(const magma::Slice& valSlice);

    /**
     * Read from local DB
     */
    std::pair<magma::Status, std::string> readLocalDoc(
            Vbid vbid, const magma::Slice& keySlice);

    /**
     * Add a local document to the commitBatch
     */
    magma::Status setLocalDoc(magma::Magma::CommitBatch& commitBatch,
                              const magma::Slice& keySlice,
                              std::string& valBuf,
                              bool deleted = false);

    /**
     * Encode the cached vbucket_state and magmaInfo into a nlohmann json struct
     */
    nlohmann::json encodeVBState(const vbucket_state& vbstate,
                                 MagmaInfo& magmaInfo) const;

    /**
     * Read the encoded vstate + magmaInfo from the local db into the cache.
     */
    magma::Status readVBStateFromDisk(Vbid vbid);

    /**
     * Write the encoded vbstate + magmaInfo to the local db.
     * with the new vbstate as well as write to disk.
     */
    magma::Status writeVBStateToDisk(Vbid vbid,
                                     magma::Magma::CommitBatch& commitBatch,
                                     vbucket_state& vbs,
                                     MagmaInfo& minfo);

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

    // Magma uses a unique logger with a prefix of magma so that all logging
    // calls from the wrapper thru magma will be prefixed with magma.
    std::shared_ptr<BucketLogger> logger;

private:
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

    int saveDocs(VB::Commit& commitData,
                 kvstats_ctx& kvctx,
                 const MagmaKVHandle& kvHandle);

    void commitCallback(int status, kvstats_ctx& kvctx);

    static ENGINE_ERROR_CODE magmaErr2EngineErr(magma::Status::Code err,
                                                bool found = true);

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

    // We need to mimic couchstores ability to turn every batch of items
    // into a rollback point. This is used for testing only!
    bool commitPointEveryBatch{false};

    // Using upsert for Set means we can't keep accurate document totals.
    // This is used for testing only!
    bool useUpsertForSet{false};

    // Get lock on KVHandle and wait for all threads to exit before
    // returning the lock to the caller.
    std::unique_lock<std::shared_timed_mutex> getExclusiveKVHandle(Vbid vbid);

    struct MagmaCompactionCtx {
        MagmaCompactionCtx(compaction_ctx* ctx, MagmaKVHandle kvHandle)
            : ctx(ctx), kvHandle(kvHandle){};
        compaction_ctx* ctx;
        MagmaKVHandle kvHandle;
    };
    // This needs to be a shared_ptr because its possible an implicit
    // compaction kicks off while an explicit compaction is happening
    // and we don't want to free it while the implicit compaction is working.
    std::vector<std::shared_ptr<MagmaCompactionCtx>> compaction_ctxList;
    std::mutex compactionCtxMutex;

    class MagmaCompactionCB : public magma::Magma::CompactionCallback {
    public:
        MagmaCompactionCB(MagmaKVStore& magmaKVStore);
        ~MagmaCompactionCB();
        bool operator()(const magma::Slice& keySlice,
                        const magma::Slice& metaSlice,
                        const magma::Slice& valueSlice) {
            return magmaKVStore.compactionCallBack(
                    *this, keySlice, metaSlice, valueSlice);
        }
        MagmaKVStore& magmaKVStore;
        bool initialized{false};
        std::stringstream itemKeyBuf;
        std::shared_ptr<MagmaCompactionCtx> magmaCompactionCtx;
        compaction_ctx* ctx{nullptr};
        MagmaKVHandle kvHandle;
        Vbid vbid{};
    };

    bool compactionCallBack(MagmaKVStore::MagmaCompactionCB& cbCtx,
                            const magma::Slice& keySlice,
                            const magma::Slice& metaSlice,
                            const magma::Slice& valueSlice);

    /**
     * @return a reference to the MagmaInfo for the vbid, created on demand
     */
    MagmaInfo& getMagmaInfo(Vbid vbid);

    /// private getWithHeader shared with public get and getWithHeader
    GetValue getWithHeader(const DiskDocKey& key,
                           Vbid vbid,
                           GetMetaOnly getMetaOnly);

    folly::Synchronized<std::queue<std::tuple<Vbid, uint64_t>>>
            pendingVbucketDeletions;

    friend class MagmaCompactionCB;
};
