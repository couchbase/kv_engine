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

#ifndef SRC_COUCH_KVSTORE_COUCH_KVSTORE_H_
#define SRC_COUCH_KVSTORE_COUCH_KVSTORE_H_ 1

#include "config.h"

#include "atomicqueue.h"
#include "configuration.h"
#include "couch-kvstore/couch-fs-stats.h"
#include "couch-kvstore/couch-kvstore-metadata.h"
#include "item.h"
#include "kvstore.h"
#include "kvstore_priv.h"
#include "libcouchstore/couch_db.h"
#include "monotonic.h"

#include <platform/histogram.h>
#include <platform/strerror.h>
#include <relaxed_atomic.h>

#include <map>
#include <memory>
#include <string>
#include <vector>


#define COUCHSTORE_NO_OPTIONS 0

class EventuallyPersistentEngine;

/**
 * Class representing a document to be persisted in couchstore.
 */
class CouchRequest : public IORequest
{
public:
    /**
     * Constructor
     *
     * @param it Item instance to be persisted
     * @param rev vbucket database revision number
     * @param cb persistence callback
     * @param del flag indicating if it is an item deletion or not
     * @param persistDocNamespace true if we should store the key's namespace
     */
    CouchRequest(const Item& it,
                 uint64_t rev,
                 MutationRequestCallback& cb,
                 bool del,
                 bool persistDocNamespace);

    virtual ~CouchRequest() {}

    /**
     * Get the vbucket id of a document to be persisted
     *
     * @return vbucket id of a document
     */
    Vbid getVBucketId() {
        return vbucketId;
    }

    /**
     * Get the revision number of the vbucket database file
     * where the document is persisted
     *
     * @return revision number of the corresponding vbucket database file
     */
    uint64_t getRevNum(void) {
        return fileRevNum;
    }

    /**
     * Get the couchstore Doc instance of a document to be persisted
     *
     * @return pointer to the couchstore Doc instance of a document
     *         or nullptr if the its a deleted item and doesn't have
     *         a value.
     */
    void *getDbDoc(void) {
        if (deleteItem && value.get() == nullptr) {
            return nullptr;
        }
        return &dbDoc;
    }

    /**
     * Get the couchstore DocInfo instance of a document to be persisted
     *
     * @return pointer to the couchstore DocInfo instance of a document
     */
    DocInfo *getDbDocInfo(void) {
        return &dbDocInfo;
    }

    /**
     * Get the length of a document body to be persisted
     *
     * @return length of a document body
     */
    size_t getNBytes() {
        return dbDocInfo.rev_meta.size + dbDocInfo.size;
    }

    /**
     * Return true if the document to be persisted is for DELETE
     *
     * @return true if the document to be persisted is for DELETE
     */
    bool isDelete() {
        return deleteItem;
    };

    size_t getKeySize() const {
        return dbDocInfo.id.size;
    }

    /**
     * Get the key of a document to be persisted
     *
     * @return key of a document to be persisted
     */
    const StoredDocKey& getKey(void) const {
        return key;
    }

protected:
    static couchstore_content_meta_flags getContentMeta(const Item& it);

    value_t value;

    MetaData meta;
    uint64_t fileRevNum;
    Doc dbDoc;
    DocInfo dbDocInfo;
};

struct kvstats_ctx;

/**
 * KVStore with couchstore as the underlying storage system
 */
class CouchKVStore : public KVStore
{
public:
    /**
     * Constructor - creates a read/write CouchKVStore
     *
     * @param config    Configuration information
     */
    CouchKVStore(KVStoreConfig& config);

    /**
     * Alternate constructor for injecting base FileOps
     *
     * @param config    Configuration information
     * @param ops       Couchstore FileOps implementation to be used
     */
    CouchKVStore(KVStoreConfig& config, FileOpsInterface& ops);

    /**
     * Deconstructor
     */
    ~CouchKVStore();

    /**
     * A read only CouchKVStore can only be created by a RW store. They should
     * be created in pairs as they share some data.
     *
     * @return a unique_ptr holding a RO 'sibling' to this object.
     */
    std::unique_ptr<CouchKVStore> makeReadOnlyStore();

    void initialize();

    /**
     * Reset vbucket to a clean state.
     */
    void reset(Vbid vbucketId) override;

    /**
     * Begin a transaction (if not already in one).
     *
     * @return true if the transaction is started successfully
     */
    bool begin(std::unique_ptr<TransactionContext> txCtx) override {
        if (!txCtx) {
            throw std::invalid_argument("CouchKVStore::begin: txCtx is null");
        }
        if (isReadOnly()) {
            throw std::logic_error("CouchKVStore::begin: Not valid on a "
                    "read-only object.");
        }
        intransaction = true;
        transactionCtx = std::move(txCtx);
        return intransaction;
    }

    /**
     * Commit a transaction (unless not currently in one).
     *
     * @param collectionsManifest a pointer to an Item which is a SystemEvent
     *        that contains a collections manifest to be written in the commit.
     *        Can be nullptr if the commit has no manifest to write.
     * @return true if the commit is completed successfully.
     */
    bool commit(Collections::VB::Flush& collectionsFlush) override;

    /**
     * Rollback a transaction (unless not currently in one).
     */
    void rollback(void) override {
        if (isReadOnly()) {
            throw std::logic_error("CouchKVStore::rollback: Not valid on a "
                    "read-only object.");
        }
        if (intransaction) {
            intransaction = false;
            transactionCtx.reset();
        }
    }

    /**
     * Query the properties of the underlying storage.
     *
     * @return properties of the underlying storage system
     */
    StorageProperties getStorageProperties(void) override;

    void set(const Item& itm,
             Callback<TransactionContext, mutation_result>& cb) override;

    /**
     * Retrieve the document with a given key from the underlying storage
     * system.
     *
     * @param key the key of a document to be retrieved
     * @param vb vbucket id of a document
     * @param fetchDelete True if we want to retrieve a deleted item if it not
     *        purged yet.
     * @return the result of the get
     */
    GetValue get(const StoredDocKey& key,
                 Vbid vb,
                 bool fetchDelete = false) override;

    /**
     * Retrieve the document with a given key from the underlying storage.
     * @param dbHandle the dbhandle
     * @param key the key of a document to be retrieved
     * @param vb vbucket id of a document
     * @param getMetaOnly Yes if we only want to retrieve the meta data for a
     * document
     * @param fetchDelete True if we want to retrieve a deleted item if it not
     *        purged yet.
     * @return the result of the get
     */
    GetValue getWithHeader(void* dbHandle,
                           const StoredDocKey& key,
                           Vbid vb,
                           GetMetaOnly getMetaOnly,
                           bool fetchDelete = false) override;

    /**
     * Retrieve the multiple documents from the underlying storage system at once.
     *
     * @param vb vbucket id of a document
     * @param itms list of items whose documents are going to be retrieved
     */
    void getMulti(Vbid vb, vb_bgfetch_queue_t& itms) override;

    /**
     * Get the number of vbuckets in a single database file
     *
     * returns - the number of vbuckets in a database file
     */
    uint16_t getNumVbsPerFile(void) override {
        return 1;
    }

    void del(const Item& itm, Callback<TransactionContext, int>& cb) override;

    /**
     * Delete a given vbucket database instance from underlying storage
     *
     * @param vbucket vbucket id
     * @param fileRev the revision of the file to delete
     */
    void delVBucket(Vbid vbucket, uint64_t fileRev) override;

    /**
     * Retrieve the list of persisted vbucket states
     *
     * @return vbucket state vector instance where key is vbucket id and
     * value is vbucket state
     */
   std::vector<vbucket_state *>  listPersistedVbuckets(void) override;

    /**
     * Retrieve ths list of persisted engine stats
     *
     * @param stats map instance where the persisted engine stats will be added
     */
    void getPersistedStats(std::map<std::string, std::string> &stats) override;

    /**
     * Persist a snapshot of the vbucket states in the underlying storage system.
     *
     * @param vbucketId - vbucket id
     * @param vbstate   - vbucket state
     * @param options   - options used for persisting the state to disk
     * @return true if the snapshot is done successfully
     */
    bool snapshotVBucket(Vbid vbucketId,
                         const vbucket_state& vbstate,
                         VBStatePersist options) override;

    /**
    * Compact a database file in the underlying storage system.
    *
    * @param ctx - compaction context that holds the identifier of the
                   underlying database file, options and callbacks
                   that need to invoked.
    * @return true if successful
    */
    bool compactDB(compaction_ctx *ctx) override;

    /**
     * Return the database file id from the compaction request
     * @param compact_req request structure for compaction
     *
     * return database file id
     */
    Vbid getDBFileId(const protocol_binary_request_compact_db& req) override {
        return req.message.header.request.vbucket.ntoh();
    }

    vbucket_state* getVBucketState(Vbid vbid) override;

    /**
     * Get the number of deleted items that are persisted to a vbucket file
     *
     * @param vbid The vbucket if of the file to get the number of deletes for
     */
    size_t getNumPersistedDeletes(Vbid vbid) override;

    /**
     * Get the vbucket pertaining stats from a vbucket database file
     *
     * @param vbid The vbucket of the file to get the number of docs for
     */
    DBFileInfo getDbFileInfo(Vbid vbid) override;

    /**
     * Get the file statistics for the underlying KV store
     *
     * return cumulative file size and space usage for the KV store
     */
    DBFileInfo getAggrDbFileInfo() override;

    /**
     * This method will return the total number of items in the vbucket. Unlike
     * the getNumItems function that returns items within a specified range of
     * sequence numbers, this will return all the items in the vbucket.
     *
     * vbid - vbucket id
     */
    size_t getItemCount(Vbid vbid) override;

    /**
     * Do a rollback to the specified seqNo on the particular vbucket
     *
     * @param vbid The vbucket of the file that's to be rolled back
     * @param rollbackSeqno The sequence number upto which the engine needs
     * to be rolled back
     * @param cb getvalue callback
     */
    RollbackResult rollback(Vbid vbid,
                            uint64_t rollbackSeqno,
                            std::shared_ptr<RollbackCB> cb) override;

    /**
     * Perform pending tasks after persisting dirty items
     */
    void pendingTasks() override;

    bool getStat(const char* name, size_t& value) override;

    static int recordDbDump(Db *db, DocInfo *docinfo, void *ctx);
    static int recordDbStat(Db *db, DocInfo *docinfo, void *ctx);
    static int getMultiCb(Db *db, DocInfo *docinfo, void *ctx);
    ENGINE_ERROR_CODE readVBState(Db* db, Vbid vbId);

    couchstore_error_t fetchDoc(Db* db,
                                DocInfo* docinfo,
                                GetValue& docValue,
                                Vbid vbId,
                                GetMetaOnly metaOnly);
    ENGINE_ERROR_CODE couchErr2EngineErr(couchstore_error_t errCode);

    uint64_t getLastPersistedSeqno(Vbid vbid);

    /**
     * Get all_docs API, to return the list of all keys in the store
     */
    ENGINE_ERROR_CODE getAllKeys(
            Vbid vbid,
            const DocKey start_key,
            uint32_t count,
            std::shared_ptr<Callback<const DocKey&>> cb) override;

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
            Vbid vbid) override;

    uint64_t getCollectionItemCount(const KVFileHandle& kvFileHandle,
                                    CollectionID collection) override;

    std::unique_ptr<KVFileHandle, KVFileHandleDeleter> makeFileHandle(
            Vbid vbid) override;

    void freeFileHandle(KVFileHandle* kvFileHandle) const override;

    /**
     * Increment the revision number of the vbucket.
     * @param vbid ID of the vbucket to change.
     */
    void incrementRevision(Vbid vbid) override;

    /**
     * Prepare for delete of the vbucket file, this just removes the in-memory
     * stats for the vbucket and returns the current file revision (which is
     * the revision that must later be unlinked).
     *
     * @param vbid ID of the vbucket being deleted
     * @return the revision ID to delete (via ::delVBucket)
     */
    uint64_t prepareToDelete(Vbid vbid) override;

protected:
    /**
     * Internal RAII class for managing a Db* and having it closed when
     * the DbHolder goes out of scope.
     */
    class DbHolder {
    public:
        DbHolder(CouchKVStore& kvs) : kvstore(kvs), db(nullptr), fileRev(0) {
        }

        DbHolder(const DbHolder&) = delete;
        DbHolder(const DbHolder&&) = delete;
        DbHolder operator=(const DbHolder&) = delete;

        Db** getDbAddress() {
            return &db;
        }

        Db* getDb() {
            return db;
        }

        Db* getDb() const {
            return db;
        }

        operator Db*() {
            return db;
        }

        Db* releaseDb() {
            auto* result = db;
            db = nullptr;
            return result;
        }

        void setFileRev(uint64_t rev) {
            fileRev = rev;
        }

        uint64_t getFileRev() const {
            return fileRev;
        }

        // Allow a non-RAII close, needed for some use-cases
        void close() {
            if (db) {
                kvstore.closeDatabaseHandle(releaseDb());
            }
        }

        ~DbHolder() {
            close();
        }
        CouchKVStore& kvstore;
        Db* db;
        uint64_t fileRev;
    };

    /*
     * Returns the DbInfo for the given vbucket database.
     */
    DbInfo getDbInfo(Vbid vbid);

    bool setVBucketState(Vbid vbucketId,
                         const vbucket_state& vbstate,
                         VBStatePersist options);

    template <typename T>
    void addStat(const std::string &prefix, const char *nm, T &val,
                 ADD_STAT add_stat, const void *c);

    void operator=(const CouchKVStore &from);

    void close();
    bool commit2couchstore(Collections::VB::Flush& collectionsFlush);

    uint64_t checkNewRevNum(std::string &dbname, bool newFile = false);
    void populateFileNameMap(std::vector<std::string>& filenames,
                             std::vector<Vbid>* vbids);
    void updateDbFileMap(Vbid vbucketId, uint64_t newFileRev);
    couchstore_error_t openDB(Vbid vbucketId,
                              DbHolder& db,
                              couchstore_open_flags options,
                              FileOpsInterface* ops = nullptr);

    couchstore_error_t openSpecificDB(Vbid vbucketId,
                                      uint64_t rev,
                                      DbHolder& db,
                                      couchstore_open_flags options,
                                      FileOpsInterface* ops = nullptr);

    /**
     * save the Documents held in docs to the file associated with vbid/rev
     *
     * @param vbid the vbucket file to open/write/commit
     * @param docs vector of Doc* to be written (can be empty)
     * @param docsinfo vector of DocInfo* to be written (non const due to
     *        couchstore API). Entry n corresponds to entry n of docs.
     * @param kvctx a stats context object to update
     * @param collectionsManifest a pointer to an item which contains the
     *        manifest update data (can be nullptr)
     *
     * @returns COUCHSTORE_SUCCESS or a failure code (failure paths log)
     */
    couchstore_error_t saveDocs(Vbid vbid,
                                const std::vector<Doc*>& docs,
                                std::vector<DocInfo*>& docinfos,
                                kvstats_ctx& kvctx,
                                Collections::VB::Flush& collectionsFlush);

    void commitCallback(std::vector<CouchRequest *> &committedReqs,
                        kvstats_ctx &kvctx,
                        couchstore_error_t errCode);
    couchstore_error_t saveVBState(Db *db, const vbucket_state &vbState);

    /**
     * Save the collections manifest to the _local/collections_manifest document
     *
     * The document written is a JSON representation of the VBucket's
     * collection manifest and is generated by
     * an Item crafted by the Collections::VB::Manifest class.
     *
     * @param db Handle to the open data store.
     * @param serialisedManifest buffer of VB::Manifest data
     */
    couchstore_error_t saveCollectionsManifest(
            Db& db, cb::const_byte_buffer serialisedManifest);

    /**
     * Read the collections manifest from the _local/collections_manifest
     * document.
     *
     * @return The manifest data or an empty object if no manifest is present.
     */
    Collections::VB::PersistedManifest readCollectionsManifest(Db& db);

    /**
     * Save count for collection cid into the file referenced by db
     * @param db The Db to write to
     * @param cid The collection to update
     * @param count The value to write
     */
    void saveItemCount(Db& db, CollectionID cid, uint64_t count);

    /**
     * Delete the count for collection cid
     * @param db The Db to write to
     * @param cid The collection to delete
     */
    void deleteItemCount(Db& db, CollectionID cid);

    void setDocsCommitted(uint16_t docs);
    void closeDatabaseHandle(Db *db);

    /**
     * Unlink selected couch file, which will be removed by the OS,
     * once all its references close.
     */
    void unlinkCouchFile(Vbid vbucket, uint64_t fRev);

    /**
     * Remove compact file
     *
     * @param dbname
     * @param vbucket id
     */
    void removeCompactFile(const std::string& dbname, Vbid vbid);

    void removeCompactFile(const std::string &filename);

    /**
     * Perform compaction using the context and dhook call back.
     * @param hook_ctx a context with information for the compaction process
     * @param dhook a docinfo hook which will be called with each compacted key
     * @return true indicating the compaction was successful.
     */
    bool compactDBInternal(compaction_ctx* hook_ctx,
                           couchstore_docinfo_hook dhook);

    /// Copy relevant DbInfo stats to the common FileStats struct
    static FileInfo toFileInfo(const DbInfo& info);

    const std::string dbname;

    using MonotonicRevision = AtomicMonotonic<uint64_t, ThrowExceptionPolicy>;

    using RevisionMap = std::vector<MonotonicRevision>;

    /**
     * Per-vbucket file revision atomic to ensure writer threads see increments.
     *
     * Owned via a shared_ptr, as there should be a single RevisionMap per
     * RW/RO pair.
     */
    std::shared_ptr<RevisionMap> dbFileRevMap;

    /**
     * An internal rwlock used to keep openDB and compaction in sync
     * Primarily that compaction and scans can be ran concurrently, we must
     * ensure that a scan doesn't read the fileRev then compaction moves the
     * fileRev (and the real file) before the scan performs an open.
     * Many opens are allowed in parallel, just compact must block.
     */
    cb::RWLock openDbMutex;

    uint16_t numDbFiles;
    std::vector<CouchRequest *> pendingReqsQ;
    bool intransaction;
    std::unique_ptr<TransactionContext> transactionCtx;

    /**
     * FileOpsInterface implementation for couchstore which tracks
     * all bytes read/written by couchstore *except* compaction.
     *
     * Backed by this->st.fsStats
     */
    std::unique_ptr<FileOpsInterface> statCollectingFileOps;

    /**
     * FileOpsInterface implementation for couchstore which tracks
     * all bytes read/written by couchstore just for compaction
     *
     * Backed by this->st.fsStatsCompaction
     */
    std::unique_ptr<FileOpsInterface> statCollectingFileOpsCompaction;

    /* deleted docs in each file, indexed by vBucket. RelaxedAtomic
       to allow stats access witout lock */
    std::vector<Couchbase::RelaxedAtomic<size_t>> cachedDeleteCount;
    std::vector<Couchbase::RelaxedAtomic<uint64_t>> cachedFileSize;
    std::vector<Couchbase::RelaxedAtomic<uint64_t>> cachedSpaceUsed;
    /* pending file deletions */
    AtomicQueue<std::string> pendingFileDeletions;

    std::atomic<size_t> scanCounter; //atomic counter for generating scan id
    std::map<size_t, Db*> scans; //map holding active scans
    std::mutex scanLock; //lock guarding the scan map

    BucketLogger& logger;

    /**
     * Base fileops implementation to be wrapped by stat collecting fileops
     */
    FileOpsInterface& base_ops;

private:
    /**
     * Construct the store, this constructor does the object initialisation and
     * is used by the public read/write constructors and the private read-only
     * constructor.
     *
     * @param config configuration data for the store
     * @param ops the file ops to use
     * @param readOnly true if the store can only do read functionality
     * @param dbFileRevMap a revisionMap to use (which should be data owned by
     *        the RW store).
     */
    CouchKVStore(KVStoreConfig& config,
                 FileOpsInterface& ops,
                 bool readOnly,
                 std::shared_ptr<RevisionMap> dbFileRevMap);

    /**
     * Construct a read-only store - private as should be called via
     * CouchKVStore::makeReadOnlyStore
     *
     * @param config configuration data for the store
     * @param dbFileRevMap The revisionMap to use (which should be intially
     * created owned by the RW store).
     */
    CouchKVStore(KVStoreConfig& config,
                 std::shared_ptr<RevisionMap> dbFileRevMap);

    /**
     * RAII holder for a couchstore LocalDoc object
     */
    class LocalDocHolder {
    public:
        LocalDocHolder() : localDoc(nullptr) {
        }

        ~LocalDocHolder() {
            if (localDoc) {
                couchstore_free_local_document(localDoc);
            }
        }

        LocalDocHolder(const DbHolder&) = delete;
        LocalDocHolder(const DbHolder&&) = delete;
        LocalDocHolder operator=(const DbHolder&) = delete;

        LocalDoc** getLocalDocAddress() {
            return &localDoc;
        }

        LocalDoc* getLocalDoc() {
            return localDoc;
        }

    private:
        LocalDoc* localDoc;
    };

    class CouchKVFileHandle : public ::KVFileHandle {
    public:
        CouchKVFileHandle(CouchKVStore& kvstore)
            : ::KVFileHandle(kvstore), db(kvstore) {
        }

        ~CouchKVFileHandle() override {
        }

        DbHolder& getDbHolder() {
            return db;
        }

        Db* getDb() const {
            return db.getDb();
        }

    private:
        DbHolder db;
    };
};

#endif  // SRC_COUCH_KVSTORE_COUCH_KVSTORE_H_
