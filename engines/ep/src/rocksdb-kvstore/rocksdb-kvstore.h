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

#include <platform/dirutils.h>
#include <map>
#include <vector>

#include <kvstore.h>

#include <rocksdb/db.h>
#include <rocksdb/listener.h>
#include <string>

#include "../objectregistry.h"
#include "vbucket_bgfetch_item.h"

// Used to set the correct engine in the ObjectRegistry thread local
// in RocksDB's flusher threads.
class FlushStartListener : public rocksdb::EventListener {
public:
    FlushStartListener(EventuallyPersistentEngine* epe) : engine(epe) {
    }
    void OnFlushBegin(rocksdb::DB*, const rocksdb::FlushJobInfo&) override {
        ObjectRegistry::onSwitchThread(engine, false);
    }

private:
    EventuallyPersistentEngine* engine;
};

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
    RocksDBKVStore(KVStoreConfig& config);

    ~RocksDBKVStore();

    void operator=(RocksDBKVStore& from) = delete;

    /**
     * Reset database to a clean state.
     */
    void reset(uint16_t vbucketId) override;

    /**
     * Begin a transaction (if not already in one).
     */
    bool begin() override;

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
    void set(const Item& item, Callback<mutation_result>& cb) override;

    /**
     * Overrides get().
     */
    GetValue get(const DocKey& key,
                 uint16_t vb,
                 bool fetchDelete = false) override;

    GetValue getWithHeader(void* dbHandle,
                           const DocKey& key,
                           uint16_t vb,
                           GetMetaOnly getMetaOnly,
                           bool fetchDelete = false) override;

    void getMulti(uint16_t vb, vb_bgfetch_queue_t& itms) override;

    /**
     * Overrides del().
     */
    void del(const Item& itm, Callback<int>& cb) override;

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

    size_t getNumShards() {
        return configuration.getMaxShards();
    }

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

    uint16_t getDBFileId(const protocol_binary_request_compact_db&) override {
        // Not needed if there is no explicit compaction
        return 0;
    }

    vbucket_state* getVBucketState(uint16_t vbucketId) override {
        return cachedVBStates[vbucketId];
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
        // NOTE vmx 2016-10-29: For LevelDB/RocksDB it will probably
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

    ScanContext* initScanContext(std::shared_ptr<Callback<GetValue>> cb,
                                 std::shared_ptr<Callback<CacheLookup>> cl,
                                 uint16_t vbid,
                                 uint64_t startSeqno,
                                 DocumentFilter options,
                                 ValueFilter valOptions) override;

    scan_error_t scan(ScanContext* sctx) override {
        // TODO vmx 2016-10-29: implement
        return scan_success;
    }

    void destroyScanContext(ScanContext* ctx) override {
        // TODO vmx 2016-10-29: implement
        delete ctx;
    }

    bool persistCollectionsManifestItem(uint16_t vbid,
                                        const Item& manifestItem) override {
        // TODO DJR 2017-05-19 implement this.
        return false;
    }

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

    std::unique_ptr<RocksDBKVStore> makeReadOnlyStore() {
        // Not using make_unique due to the private constructor we're calling
        return std::unique_ptr<RocksDBKVStore>(
                new RocksDBKVStore(configuration));
    }

private:
    /**
     * Direct access to the DB.
     */
    std::unique_ptr<rocksdb::DB> db;
    char* keyBuffer;
    char* valBuffer;
    size_t valSize;

    rocksdb::Options rdbOptions;

    void open();

    void close();

    std::string mkKeyStr(uint16_t, const DocKey& k);
    rocksdb::SliceParts mkKeySliceParts(uint16_t, const DocKey& k);
    void grokKeySlice(const rocksdb::Slice&, uint16_t*, std::string*);

    void adjustValBuffer(const size_t);

    rocksdb::Slice mkValSlice(const Item& item);
    rocksdb::SliceParts mkValSliceParts(const Item& item);
    std::unique_ptr<Item> grokValSlice(uint16_t vb,
                                       const DocKey& key,
                                       const rocksdb::Slice& s);

    GetValue makeGetValue(uint16_t vb,
                          const DocKey& key,
                          const std::string& value);

    std::unique_ptr<rocksdb::WriteBatch> batch;
    rocksdb::WriteOptions writeOptions;

    // RocksDB does *not* need additional synchronisation around
    // db->Write, but we need to prevent delVBucket racing with
    // commit, potentially losing data.
    std::mutex writeLock;

    std::atomic<size_t> scanCounter; // atomic counter for generating scan id
};
