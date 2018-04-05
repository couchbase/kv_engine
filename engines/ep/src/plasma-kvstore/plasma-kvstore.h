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
 * Experimental Plasma KVStore implementation
 *
 */

#pragma once

#include <platform/dirutils.h>
#include <map>
#include <vector>

#include <kvstore.h>

#include <string>

#include "../objectregistry.h"
#include "vbucket_bgfetch_item.h"

class PlasmaRequest;
class PlasmaKVStoreConfig;
class KVPlasma;
struct KVStatsCtx;

/**
 * A persistence store based on plasma.
 */
class PlasmaKVStore : public KVStore {
public:
    /**
     * Constructor
     *
     * @param config    Configuration information
     */
    PlasmaKVStore(PlasmaKVStoreConfig& config);

    ~PlasmaKVStore();

    void operator=(PlasmaKVStore& from) = delete;

    /**
     * Reset database to a clean state.
     */
    void reset(uint16_t vbucketId) override;

    /**
     * Begin a transaction (if not already in one).
     */
    bool begin(std::unique_ptr<TransactionContext> txCtx) override;

    /**
     * Commit a transaction (unless not currently in one).
     *
     * Returns false if the commit fails.
     */
    bool commit(const Item* collectionsManifest) override;

    /**
     * Rollback a transaction (unless not currently in one).
     */
    void rollback() override;

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
                 uint16_t vb,
                 bool fetchDelete = false) override;

    GetValue getWithHeader(void* dbHandle,
                           const StoredDocKey& key,
                           uint16_t vb,
                           GetMetaOnly getMetaOnly,
                           bool fetchDelete = false) override;

    void getMulti(uint16_t vb, vb_bgfetch_queue_t& itms) override;

    /**
     * Overrides del().
     */
    void del(const Item& itm, Callback<TransactionContext, int>& cb) override;

    void delVBucket(uint16_t vbucket, uint64_t vb_version) override;

    std::vector<vbucket_state*> listPersistedVbuckets(void) override;

    /**
     * Take a snapshot of the stats in the main DB.
     */
    bool snapshotStats(const std::map<std::string, std::string>& m);
    /**
     * Take a snapshot of the vbucket states in the main DB.
     */
    bool snapshotVBucket(uint16_t vbucketId,
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
        // under Plasma's control
        return true;
    }

    uint16_t getDBFileId(const protocol_binary_request_compact_db&) override {
        // Not needed if there is no explicit compaction
        return 0;
    }

    vbucket_state* getVBucketState(uint16_t vbucketId) override {
        return cachedVBStates[vbucketId].get();
    }

    size_t getNumPersistedDeletes(uint16_t vbid) override {
        // TODO vmx 2016-10-29: implement
        return 0;
    }

    DBFileInfo getDbFileInfo(uint16_t vbid) override {
        // TODO vmx 2016-10-29: implement
        DBFileInfo vbinfo;
        return vbinfo;
    }

    DBFileInfo getAggrDbFileInfo() override {
        // TODO vmx 2016-10-29: implement
        DBFileInfo vbinfo;
        return vbinfo;
    }

    size_t getItemCount(uint16_t vbid) override {
        // TODO vmx 2016-10-29: implement
        return 0;
    }

    RollbackResult rollback(uint16_t vbid,
                            uint64_t rollbackSeqno,
                            std::shared_ptr<RollbackCB> cb) override {
        // TODO vmx 2016-10-29: implement
        // NOTE vmx 2016-10-29: For LevelDB/Plasma it will probably
        // always be a full rollback as it doesn't support Couchstore
        // like rollback semantics
        return RollbackResult(false, 0, 0, 0);
    }

    void pendingTasks() override {
        // NOTE vmx 2016-10-29: Intentionally left empty;
    }

    ENGINE_ERROR_CODE getAllKeys(
            uint16_t vbid,
            const DocKey start_key,
            uint32_t count,
            std::shared_ptr<Callback<const DocKey&>> cb) override {
        // TODO vmx 2016-10-29: implement
        return ENGINE_SUCCESS;
    }

    ScanContext* initScanContext(std::shared_ptr<StatusCallback<GetValue>> cb,
                                 std::shared_ptr<StatusCallback<CacheLookup>> cl,
                                 uint16_t vbid,
                                 uint64_t startSeqno,
                                 DocumentFilter options,
                                 ValueFilter valOptions) override;

    scan_error_t scan(ScanContext* sctx) override;

    void destroyScanContext(ScanContext* ctx) override;

    std::string getCollectionsManifest(uint16_t vbid) override {
        // TODO DJR 2017-05-19 implement this.
        return "";
    }

    void incrementRevision(uint16_t vbid) override {
        // TODO DJR 2017-05-19 implement this.
    }

    uint64_t prepareToDelete(uint16_t vbid) override {
        // TODO DJR 2017-05-19 implement this.
        return 0;
    }

	/*
    std::unique_ptr<PlasmaKVStore> makeReadOnlyStore() {
        // Not using make_unique due to the private constructor we're calling
        return std::unique_ptr<PlasmaKVStore>(
                new PlasmaKVStore(configuration));
    }
	*/

private:
    // This is used for synchonization in `openDB` to avoid that we open two
    // instances on the same DB (e.g., this would be possible
    // when we `Flush` and `Warmup` run in parallel).
    std::mutex openDBMutex;
    // Thus, we put an entry in this vector at position `vbid` when we `openDB`
    // for a VBucket for the first time. Then, further calls to `openDB(vbid)`
    // return the pointer stored in this vector. An entry is removed only when
    // `delVBucket(vbid)`.
    std::vector<std::unique_ptr<KVPlasma>> vbDB;

    /*
     * This function returns an instance of `KVPlasma` for the given `vbid`.
     * The DB for `vbid` is created if it does not exist.
     *
     * @param vbid vbucket id for the vbucket DB to open
     */
    const KVPlasma& openDB(uint16_t vbid);

    /*
     * The DB for each VBucket is created in a separated subfolder of
     * `configuration.getDBName()`. This function returns the path of the DB
     * subfolder for the given `vbid`.
     *
     * @param vbid vbucket id for the vbucket DB subfolder to return
     */
    std::string getVBDBSubdir(uint16_t vbid);

    /*
     * This function returns a vector of Vbucket IDs that already exist on
     * disk. The function considers only the Vbuckets managed by the current
     * Shard.
     */
    std::vector<uint16_t> discoverVBuckets();

    std::unique_ptr<Item> makeItem(uint16_t vb,
                                   const DocKey& key,
                                   const std::string& value,
                                   GetMetaOnly getMetaOnly);

    GetValue makeGetValue(uint16_t vb,
                          const DocKey& key,
                          const std::string& value,
                          GetMetaOnly getMetaOnly = GetMetaOnly::No);

    void readVBState(const KVPlasma& db);

    int saveDocs(
            uint16_t vbid,
            const Item* collectionsManifest,
            const std::vector<std::unique_ptr<PlasmaRequest>>& commitBatch);

    void commitCallback(
            int status,
            const std::vector<std::unique_ptr<PlasmaRequest>>& commitBatch);

    int64_t readHighSeqnoFromDisk(const KVPlasma& db);

    std::string getVbstateKey();

    // Used for queueing mutation requests (in `set` and `del`) and flushing
    // them to disk (in `commit`).
    std::vector<std::unique_ptr<PlasmaRequest>> pendingReqs;

    // Plasma does *not* need additional synchronisation around
    // db->Write, but we need to prevent delVBucket racing with
    // commit, potentially losing data.
    std::mutex writeLock;

    // This variable is used to verify that the KVStore API is used correctly
    // when Plasma is used as store. "Correctly" means that the caller must
    // use the API in the following way:
    //      - begin() x1
    //      - set() / del() xN
    //      - commit()
    bool in_transaction;
    std::unique_ptr<TransactionContext> transactionCtx;
    const std::string plasmaPath;

    std::atomic<size_t> scanCounter; // atomic counter for generating scan id

    Logger& logger;
};
