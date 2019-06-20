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

#include <platform/dirutils.h>
#include <platform/non_negative_counter.h>

#include <map>
#include <shared_mutex>
#include <string>
#include <vector>

namespace magma {
class Slice;
class Status;
} // namespace magma

class MagmaRequest;
class MagmaKVStoreConfig;
struct kvstats_ctx;
struct vbucket_state;

/**
 * Magma info is used to mimic info that is stored internally in couchstore.
 * This info is stored with vbucket_state every time vbucket_state is stored.
 */
class MagmaInfo {
public:
    MagmaInfo() = default;

    void reset() {
        docCount = 0;
        persistedDeletes = 0;
    }
    cb::NonNegativeCounter<uint64_t> docCount{0};
    cb::NonNegativeCounter<uint64_t> persistedDeletes{0};
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
    bool commit(Collections::VB::Flush& collectionsFlush) override;

    /**
     * Rollback a transaction (unless not currently in one).
     */
    void rollback() override;

    StorageProperties getStorageProperties() override;

    /**
     * Adds a request to a queue for batch processing at commit()
     */
    void set(const Item& item, SetCallback cb) override;

    GetValue get(const DiskDocKey& key, Vbid vb) override;

    GetValue getWithHeader(void* dbHandle,
                           const DiskDocKey& key,
                           Vbid vb,
                           GetMetaOnly getMetaOnly) override;

    void getMulti(Vbid vb, vb_bgfetch_queue_t& itms) override;

    void getRange(Vbid vb,
                  const DiskDocKey& startKey,
                  const DiskDocKey& endKey,
                  const GetRangeCb& cb) override;

    void del(const Item& itm, KVStore::DeleteCallback cb) override;

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
    bool snapshotVBucket(Vbid vbucketId,
                         const vbucket_state& vbstate,
                         VBStatePersist options) override;

    bool compactDB(compaction_ctx*) override {
        // Explicit compaction is not needed.
        // Compaction is continuously occurring in separate threads
        // under Magma's control
        return true;
    }

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
                            std::shared_ptr<RollbackCB> cb) override {
        // TODO
        return RollbackResult(false);
    }

    void pendingTasks() override {
        // Magma does not use pendingTasks
    }

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

    std::unique_ptr<KVFileHandle, KVFileHandleDeleter> makeFileHandle(
            Vbid vbid) override {
        return std::unique_ptr<KVFileHandle, KVFileHandleDeleter>{
                new KVFileHandle(*this)};
    }

    void freeFileHandle(KVFileHandle* kvFileHandle) const override {
        // TODO
        delete kvFileHandle;
    }

    Collections::VB::PersistedStats getCollectionStats(
            const KVFileHandle& kvFileHandle,
            CollectionID collection) override {
        // TODO
        return {};
    }

    void incrementRevision(Vbid vbid) override {
        // magma does not use file revisions
    }

    uint64_t prepareToDeleteImpl(Vbid vbid) override {
        // magma does not use prepareToDelete
        return 0;
    }

    Collections::KVStore::Manifest getCollectionsManifest(Vbid vbid) override {
        // TODO
        return Collections::KVStore::Manifest{
                Collections::KVStore::Manifest::Default{}};
    }

    std::vector<Collections::KVStore::DroppedCollection> getDroppedCollections(
            Vbid vbid) override {
        // TODO
        return {};
    }

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
                              bool deleted);

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
     * The magmaKVHandle protects magma from a kvstore being dropped
     * while an API operation is active. This is required because
     * unlike couchstore which just unlinks the data file, magma
     * must wait for all threads to exit and then block subsequent
     * threads from proceeding while the kvstore is being dropped.
     * Inside the handle is the vbstateMutex. This mutex is used
     * to protect the vbstate from race conditions when its updated.
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

    int saveDocs(Collections::VB::Flush& collectionsFlush,
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

    // Magma uses a unique logger with a prefix of magma so that all logging
    // calls from the wrapper thru magma will be prefixed with magma.
    std::shared_ptr<BucketLogger> logger;

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
};
