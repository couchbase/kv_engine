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
#include "vbucket_bgfetch_item.h"

#include <platform/dirutils.h>

#include <map>
#include <string>
#include <vector>

class MagmaRequest;
class MagmaKVStoreConfig;
class KVMagma;
struct KVStatsCtx;

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
    void set(const Item& item,
             Callback<TransactionContext, mutation_result>& cb) override;

    GetValue get(const StoredDocKey& key,
                 Vbid vb,
                 bool fetchDelete = false) override;

    GetValue getWithHeader(void* dbHandle,
                           const StoredDocKey& key,
                           Vbid vb,
                           GetMetaOnly getMetaOnly,
                           bool fetchDelete = false) override;

    void getMulti(Vbid vb, vb_bgfetch_queue_t& itms) override;

    void del(const Item& itm, Callback<TransactionContext, int>& cb) override;

    void delVBucket(Vbid vbucket, uint64_t fileRev) override;

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

    size_t getNumShards() const;

    void optimizeWrites(std::vector<queued_item>&) {
    }

    uint16_t getNumVbsPerFile() override {
        // TODO Adapt this if this changes.
        return 1024;
    }

    bool compactDB(compaction_ctx*) override {
        // Explicit compaction is not needed.
        // Compaction is continuously occurring in separate threads
        // under Magma's control
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
        // TODO storage-team 2018-10-9 - need to implement
        return 0;
    }

    DBFileInfo getDbFileInfo(Vbid vbid) override {
        // TODO how will magma implement this
        DBFileInfo vbinfo;
        return vbinfo;
    }

    DBFileInfo getAggrDbFileInfo() override {
        // TODO storage-team 2018-10-9 - need to implement
        DBFileInfo vbinfo;
        return vbinfo;
    }

    size_t getItemCount(Vbid vbid) override {
        // TODO storage-team 2018-10-9 need to implement
        return 0;
    }

    RollbackResult rollback(Vbid vbid,
                            uint64_t rollbackSeqno,
                            std::shared_ptr<RollbackCB> cb) override {
        // TODO storage-team 2018-10-9 need to implement
        return RollbackResult(false, 0, 0, 0);
    }

    void pendingTasks() override {
        // TODO storage-team 2018-10-9 need to implement
    }

    ENGINE_ERROR_CODE getAllKeys(
            Vbid vbid,
            const DocKey start_key,
            uint32_t count,
            std::shared_ptr<Callback<const DocKey&>> cb) override {
        // TODO 2018-10-9 need to implement
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
        // TODO 2018-10-9 need to implement
        return {};
    }

    std::unique_ptr<KVFileHandle, KVFileHandleDeleter> makeFileHandle(
            Vbid vbid) override {
        return std::unique_ptr<KVFileHandle, KVFileHandleDeleter>{
                new KVFileHandle(*this)};
    }

    void freeFileHandle(KVFileHandle* kvFileHandle) const override {
        // TODO: magma file transfer handle
        delete kvFileHandle;
    }

    Collections::VB::PersistedStats getCollectionStats(
            const KVFileHandle& kvFileHandle,
            CollectionID collection) override {
        // TODO magma collection item count implementation
        return {};
    }

    void incrementRevision(Vbid vbid) override {
        // TODO storage-team 2018-10-9 need to implement
    }

    uint64_t prepareToDelete(Vbid vbid) override {
        // TODO storage-team 2018-10-9 need to implement
        return 0;
    }

    Collections::KVStore::Manifest getCollectionsManifest_new(
            Vbid vbid) override {
        throw std::logic_error(
                "Unimplemented MagmaKVStore getCollectionsManifest");
        return {};
    }

    std::vector<Collections::KVStore::DroppedCollection> getDroppedCollections(
            Vbid vbid) override {
        throw std::logic_error(
                "Unimplemented MagmaKVStore getDroppedCollections");
        return {};
    }

private:
    // This is used for synchonization in `openDB` to avoid that we open two
    // instances on the same DB (e.g., this would be possible
    // when we `Flush` and `Warmup` run in parallel).
    std::mutex openDBMutex;
    // Thus, we put an entry in this vector at position `vbid` when we `openDB`
    // for a VBucket for the first time. Then, further calls to `openDB(vbid)`
    // return the pointer stored in this vector. An entry is removed only when
    // `delVBucket(vbid)`.
    std::vector<std::unique_ptr<KVMagma>> vbDB;

    /*
     * This function returns an instance of `KVMagma` for the given `vbid`.
     * The DB for `vbid` is created if it does not exist.
     *
     * @param vbid vbucket id for the vbucket DB to open
     */
    const KVMagma& openDB(Vbid vbid);

    /*
     * The DB for each VBucket is created in a separated subfolder of
     * `configuration.getDBName()`. This function returns the path of the DB
     * subfolder for the given `vbid`.
     *
     * @param vbid vbucket id for the vbucket DB subfolder to return
     */
    std::string getVBDBSubdir(Vbid vbid);

    /*
     * This function returns a vector of Vbucket IDs that already exist on
     * disk. The function considers only the Vbuckets managed by the current
     * Shard.
     */
    std::vector<Vbid> discoverVBuckets();

    std::unique_ptr<Item> makeItem(Vbid vb,
                                   const DocKey& key,
                                   const std::string& value,
                                   GetMetaOnly getMetaOnly);

    GetValue makeGetValue(Vbid vb,
                          const DocKey& key,
                          const std::string& value,
                          GetMetaOnly getMetaOnly = GetMetaOnly::No);

    void readVBState(const KVMagma& db);

    int saveDocs(Vbid vbid,
                 Collections::VB::Flush& collectionsFlush,
                 const std::vector<std::unique_ptr<MagmaRequest>>& commitBatch);

    void commitCallback(
            int status,
            const std::vector<std::unique_ptr<MagmaRequest>>& commitBatch);

    int64_t readHighSeqnoFromDisk(const KVMagma& db);

    std::string getVbstateKey();

    // Used for queueing mutation requests (in `set` and `del`) and flushing
    // them to disk (in `commit`).
    std::vector<std::unique_ptr<MagmaRequest>> pendingReqs;

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
    const std::string magmaPath;

    std::atomic<size_t> scanCounter; // atomic counter for generating scan id

    BucketLogger& logger;
};
