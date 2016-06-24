/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#ifndef SRC_FOREST_KVSTORE_FOREST_KVSTORE_H_
#define SRC_FOREST_KVSTORE_FOREST_KVSTORE_H_ 1

#include "libforestdb/forestdb.h"
#include "kvstore.h"

//Maximum length of a key
const size_t MAX_KEY_LENGTH = 250;

// Additional 3 Bytes for flex meta, datatype and conflict resolution mode
const size_t FORESTDB_METADATA_SIZE  ((3 * sizeof(uint32_t) + 2 * sizeof(uint64_t)) +
                                      FLEX_DATA_OFFSET + EXT_META_LEN +
                                      CONFLICT_RES_META_LEN);

typedef struct ForestMetaData {
    uint64_t cas;
    uint64_t rev_seqno;
    uint32_t exptime;
    uint32_t texptime;
    uint32_t flags;
    uint8_t  flex_meta;
    uint8_t  ext_meta[EXT_META_LEN];
    uint8_t  confresmode;
} ForestMetaData;

#define forestMetaOffset(field) offsetof(ForestMetaData, field)

enum class handleType {
    READER,
    WRITER
};

/**
 * Class representing a document to be persisted in ForestDB.
 */
class ForestRequest : public IORequest
{
public:
    /**
     * Constructor
     *
     * @param it  Item instance to be persisted
     * @param cb  persistence callback
     * @param del flag indicating if it is an item deletion or not
     */
    ForestRequest(const Item& it, MutationRequestCallback& cb, bool del);

    /**
     * Destructor
     */
    ~ForestRequest();

    void setStatus(int8_t errCode) {
        status = errCode;
    }

    int8_t getStatus(void) const {
        return status;
    }

private :
    int8_t status;
};

/**
 * KVStore with ForestDB as the underlying storage system
 */
class ForestKVStore : public KVStore
{
    public:
    /**
     * Constructor
     *
     * @param config    Configuration information
     */
    ForestKVStore(KVStoreConfig& config);

    /**
     * Copy constructor
     *
     * @param from the source kvstore instance
     */
    ForestKVStore(const ForestKVStore& from);

    /**
     * Destructor
     */
    ~ForestKVStore();

    /**
     * Reset database to a clean state.
     */
    void reset(uint16_t vbucketId) override;

    /**
     * Begin a transaction (if not already in one).
     *
     * @return true if the transaction is started successfully
     */
    bool begin(void) override {
        if (isReadOnly()) {
            throw std::logic_error("ForestKVStore::begin: Not valid on a "
                    "read-only object.");
        }
        intransaction = true;
        return intransaction;
    }

    /**
     * Commit a transaction (unless not currently in one).
     *
     * @return true if the commit is completed successfully.
     */
    bool commit() override;

    /**
     * Rollback a transaction (unless not currently in one).
     */
    void rollback(void) override {
        if (isReadOnly()) {
            throw std::logic_error("ForestKVStore::rollback: Not valid on a "
                    "read-only object.");
        }
        if (intransaction) {
            intransaction = false;
        }
    }

    /**
     * Query the properties of the underlying storage.
     *
     * @return properties of the underlying storage system
     */
    StorageProperties getStorageProperties(void) override;

    /**
     * Insert or update a given document.
     *
     * @param itm instance representing the document to be inserted or updated
     * @param cb callback instance for SET
     */
    void set(const Item& itm, Callback<mutation_result>& cb) override;

    /**
     * Retrieve the document with a given key from the underlying storage system.
     *
     * @param key the key of a document to be retrieved
     * @param vb vbucket id of a document
     * @param cb callback instance for GET
     * @param fetchDelete True if we want to retrieve a deleted item if it not
     *        purged yet.
     */
    void get(const std::string& key, uint16_t vb, Callback<GetValue>& cb,
             bool fetchDelete = false) override;

    void getWithHeader(void* handle, const std::string& key,
                       uint16_t vb, Callback<GetValue>& cb,
                       bool fetchDelete = false) override;

    /**
     * Retrieve multiple documents from the underlying storage system at once.
     *
     * @param vb vbucket id of a document
     * @param itms list of items whose documents are going to be retrieved
     */
    void getMulti(uint16_t vb, vb_bgfetch_queue_t& itms) override;

    /**
     * Get the number of the vbuckets in the underlying database file
     *
     * returns - the number of vbuckets in the file
     */
    uint16_t getNumVbsPerFile(void) override {
        return cachedValidVBCount.load();
    }

    /**
     * Delete a given document from the underlying storage system.
     *
     * @param itm instance representing the document to be deleted
     * @param cb callback instance for DELETE
     */
    void del(const Item& itm, Callback<int>& cb) override;

    bool delVBucket(uint16_t vbucket) override;

    /**
     * Retrieve the list of persisted vbucket states
     *
     * @return vbucket state vector instance where key is vbucket id and
     * value is vbucket state
     */
    std::vector<vbucket_state *> listPersistedVbuckets(void) override;

    /**
     * Persist a snapshot of the vbucket states in the underlying storage system.
     *
     * @param vbucketId vbucket id
     * @param vbstate vbucket state
     * @param options - options used for persisting state to disk
     * @return true if the snapshot is done successfully
     */
    bool snapshotVBucket(uint16_t vbucketId, vbucket_state& vbstate,
                         VBStatePersist options) override;

    /**
     * Compact a forestdb database file
     *
     * @param ctx  - compaction context containing callback hooks
     *
     * @return false if the compaction fails; true if successful
     */
    bool compactDB(compaction_ctx* ctx) override;

    /**
     * Return the database file id from the compaction request
     * @param compact_req request structure for compaction
     *
     * return database file id
     */
    uint16_t getDBFileId(const protocol_binary_request_compact_db& req) override {
        return ntohs(req.message.body.db_file_id);
    }

    /**
     * Callback that is invoked by FDB api: fdb_changes_since
     *
     * @param handle pointer to handle for the KV store
     * @param doc pointer to the document
     * @param ctx context set by the caller
     *
     * return:
     *      FDB_CHANGES_CLEAN   : Success, fdb_doc freed by API
     *      FDB_CHANGES_PRESERVE: Success, fdb_doc will need to be freed by caller
     *      FDB_CHANGES_CANCEL  : Failure, fdb_doc freed by API, API stops iteration
     */
    static fdb_changes_decision recordChanges(fdb_kvs_handle* handle,
                                              fdb_doc* doc, void* ctx);

    /**
     * Callback invoked at compaction time on the database file to purge
     * tombstone entries and invoke expiry/bloom filter callbacks, if set
     *
     * @param fhandle handle for the ForestDB database file
     * @param status  phase of compaction being performed.
     *                For example., if the phase is set to FDB_CS_MOVE_DOC, this
     *                callback is invoked every time a doc is being moved from
     *                one file to another
     * @param kv_name     if the file is split into multiple KV stores, the name of
     *                    the KV store on which the compaction is being performed
     * @param doc         document being compacted
     * @param old_offset  offset in the old file
     * @param new_offset  offset in the new file
     * @param ctx         context set by the caller
     *
     * returns a decision whether to keep or move the document
     */
    static fdb_compact_decision compaction_cb(fdb_file_handle* fhandle,
                                              fdb_compaction_status status,
                                              const char* kv_name,
                                              const fdb_doc* doc, uint64_t old_offset,
                                              uint64_t new_offset, void* ctx);

    vbucket_state *getVBucketState(uint16_t vbid) override;

    /**
     * Get the number of items from a ForestDB KVStore instance
     * inclusive of the max sequence number
     *
     * @param vbid The vbucket id for which the count is needed
     * @param min_seq The sequence number to start the count from
     * @param max_seq The sequence number to stop the count at
     * @return total number of items
     */
    size_t getNumItems(uint16_t vbid, uint64_t min_seq, uint64_t max_seq) override;

    /**
     * This method will return the total number of items in the vbucket
     *
     * vbid - vbucket id
     */
    size_t getItemCount(uint16_t vbid) override;

    /**
     * Get the number of deleted items that are persisted to a vbucket KVStore
     * instance
     *
     * @param vbid The vbucket id of the file to get the number of deletes
     * @return number of persisted deletes for the given vbucket
     */
    size_t getNumPersistedDeletes(uint16_t vbid) override;

    /**
     * Do a rollback to the specified sequence number on the particular vbucket
     *
     * @param vbid          The vbucket id to be rolled back
     * @param rollbackSeqno The sequence number to which the engine needs
     *                      to be rolled back
     * @param cb            callback function to be invoked
     */
    RollbackResult rollback(uint16_t vbid, uint64_t rollbackSeqno,
                            std::shared_ptr<RollbackCB> cb) override;

    void pendingTasks() override {
        return;
    }

    uint64_t getLastPersistedSeqno(uint16_t vbid) {
        return 0;
    }

    /**
     * Get the stats that belong to a database file. The current
     * implementation of this API retrieves the information on a
     * vbucket level in order to be compatible with the behavior
     * of CouchKVStore. Once CouchKVStore is deprecated, this API
     * should retrieve information for a shard file.
     *
     * @param vbId The vbucket id for which stats are needed
     */
    DBFileInfo getDbFileInfo(uint16_t vbId) override;

    /**
     * Get the file statistics for the underlying KV store
     *
     * return cumulative file size and space usage for the KV store
     */
    DBFileInfo getAggrDbFileInfo() override;

    ENGINE_ERROR_CODE getAllKeys(uint16_t vbid, std::string& start_key,
                                 uint32_t count,
                                 std::shared_ptr<Callback<uint16_t&, char*&> > cb) override;

    ScanContext *initScanContext(std::shared_ptr<Callback<GetValue> > cb,
                                 std::shared_ptr<Callback<CacheLookup> > cl,
                                 uint16_t vbid, uint64_t startSeqno,
                                 DocumentFilter options,
                                 ValueFilter valOptions) override;

    scan_error_t scan(ScanContext* sctx) override;

    void destroyScanContext(ScanContext* ctx) override;

private:
    bool intransaction;
    const std::string dbname;
    std::atomic<uint64_t> dbFileRevNum;
    fdb_file_handle* dbFileHandle;
    std::unordered_map<uint16_t, fdb_kvs_handle *> writeHandleMap;
    std::unordered_map<uint16_t, fdb_kvs_handle *> readHandleMap;
    std::vector<Couchbase::RelaxedAtomic<size_t>> cachedDeleteCount;
    Couchbase::RelaxedAtomic<uint64_t> cachedFileSize;
    Couchbase::RelaxedAtomic<uint64_t> cachedSpaceUsed;
    fdb_kvs_handle* vbStateHandle;
    fdb_config fileConfig;
    fdb_kvs_config kvsConfig;
    std::vector<ForestRequest *> pendingReqsQ;
    static std::mutex initLock;
    static int numGlobalFiles;
    std::atomic<size_t> backfillCounter;
    std::map<size_t, fdb_kvs_handle *> backfills;
    std::mutex backfillLock; /* guard for the backfills map */

private:
    void close();
    fdb_config getFileConfig();
    fdb_kvs_config getKVConfig();
    void initForestDb();
    void shutdownForestDb();
    ENGINE_ERROR_CODE readVBState(uint16_t vbId);
    fdb_kvs_handle* getKvsHandle(uint16_t vbId, handleType htype);
    bool save2forestdb();
    void updateFileInfo();
    GetValue docToItem(fdb_kvs_handle* kvsHandle, fdb_doc* rdoc, uint16_t vbId,
                       bool metaOnly = false, bool fetchDelete = false);
    ENGINE_ERROR_CODE forestErr2EngineErr(fdb_status errCode);
    size_t getNumItems(fdb_kvs_handle* kvsHandle,
                       uint64_t min_seq,
                       uint64_t max_seq);
};

#endif  // SRC_FOREST_KVSTORE_FOREST_KVSTORE_H_
