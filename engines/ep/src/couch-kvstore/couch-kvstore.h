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
#pragma once

#include "atomicqueue.h"
#include "configuration.h"
#include "couch-kvstore/couch-fs-stats.h"
#include "couch-kvstore/couch-kvstore-metadata.h"
#include "kvstore.h"
#include "kvstore_priv.h"
#include "libcouchstore/couch_db.h"
#include "monotonic.h"

#include <folly/SharedMutex.h>
#include <folly/Synchronized.h>
#include <platform/strerror.h>
#include <relaxed_atomic.h>

#include <engines/ep/src/vbucket_state.h>
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
     * @param item Item to be persisted
     */
    CouchRequest(queued_item item);

    ~CouchRequest();

    /**
     * Get the couchstore Doc instance of a document to be persisted
     *
     * @return pointer to the couchstore Doc instance of a document
     *         or nullptr if the its a deleted item and doesn't have
     *         a value.
     */
    Doc* getDbDoc() {
        if (isDelete() && value.get() == nullptr) {
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

protected:
    static couchstore_content_meta_flags getContentMeta(const Item& it);

    value_t value;

    MetaData meta;
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
            throw std::logic_error(
                    "CouchKVStore::begin: Not valid on a read-only object.");
        }
        if (collectionsMeta.needsCommit) {
            throw std::logic_error("CouchKVStore::begin needsCommit:true");
        }
        intransaction = true;
        transactionCtx = std::move(txCtx);
        return intransaction;
    }

    /**
     * Commit a transaction (unless not currently in one).
     *
     * @param flushData - see KVStore::commit
     * @return true if the commit is completed successfully.
     */
    bool commit(VB::Commit& commitData) override;

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

    void set(queued_item item) override;

    /**
     * Retrieve the document with a given key from the underlying storage
     * system.
     *
     * @param key the key of a document to be retrieved
     * @param vb vbucket id of a document
     * @return the result of the get
     */
    GetValue get(const DiskDocKey& key, Vbid vb) override;

    /**
     * Retrieve the document with a given key from the underlying storage.
     * @param kvFileHandle the open file to get from
     * @param key the key of a document to be retrieved
     * @param vb vbucket id of a document
     * @param getMetaOnly Yes if we only want to retrieve the meta data for a
     * document
     * @return the result of the get
     */
    GetValue getWithHeader(const KVFileHandle& kvFileHandle,
                           const DiskDocKey& key,
                           Vbid vb,
                           GetMetaOnly getMetaOnly) override;

    void getMulti(Vbid vb, vb_bgfetch_queue_t& itms) override;

    void getRange(Vbid vb,
                  const DiskDocKey& startKey,
                  const DiskDocKey& endKey,
                  const KVStore::GetRangeCb& cb) override;

    void del(queued_item item) override;

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
     * @return true if the snapshot is done successfully
     */
    bool snapshotVBucket(Vbid vbucketId, const vbucket_state& vbstate) override;

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
    Vbid getDBFileId(const cb::mcbp::Request& req) override {
        return req.getVBucket();
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

    std::unique_ptr<KVFileHandle> makeFileHandle(Vbid vbid) override;

    /**
     * prepareToCreate will increment the revision number of the vbucket, but is
     * a no-op if readOnly()
     * @param vbid ID of the vbucket to change.
     */
    void prepareToCreateImpl(Vbid vbid) override;

    /**
     * Prepare for delete of the vbucket file, this just removes the in-memory
     * stats for the vbucket and returns the current file revision (which is
     * the revision that must later be unlinked).
     *
     * @param vbid ID of the vbucket being deleted
     * @return the revision ID to delete (via ::delVBucket)
     */
    uint64_t prepareToDeleteImpl(Vbid vbid) override;

    /**
     * CouchKVStore implements this method as a read of 3 _local documents
     */
    Collections::KVStore::Manifest getCollectionsManifest(Vbid vbid) override;

    /**
     * CouchKVStore implements this method as a read of 1 _local document
     */
    std::vector<Collections::KVStore::DroppedCollection> getDroppedCollections(
            Vbid vbid) override;

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
        DbHolder(DbHolder&&) = delete;
        DbHolder& operator=(const DbHolder&) = delete;
        DbHolder& operator=(DbHolder&&) = delete;

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

        LocalDocHolder(const LocalDocHolder&) = delete;
        LocalDocHolder& operator=(const LocalDocHolder&) = delete;

        LocalDocHolder(LocalDocHolder&& other) : localDoc(other.localDoc) {
            other.localDoc = nullptr;
        }

        LocalDocHolder& operator=(LocalDocHolder&& other) {
            localDoc = other.localDoc;
            other.localDoc = nullptr;
            return *this;
        }

        LocalDoc** getLocalDocAddress() {
            return &localDoc;
        }

        LocalDoc* getLocalDoc() {
            return localDoc;
        }

        cb::const_byte_buffer getBuffer() const {
            return {reinterpret_cast<const uint8_t*>(localDoc->json.buf),
                    localDoc->json.size};
        }

    private:
        LocalDoc* localDoc;
    };

    /**
     * Container for pending couchstore requests.
     *
     * Using deque as (a) it doesn't move (and hence invalidate) any existing
     * elements, which is relied on as CouchRequest has pointers to it's own
     * data, and (b) as the expansion behviour is less aggressive compared to
     * std::vector (CouchRequest objects are ~256 bytes in size).
     */
    using PendingRequestQueue = std::deque<CouchRequest>;

    /*
     * Returns the DbInfo for the given vbucket database.
     */
    DbInfo getDbInfo(Vbid vbid);

    bool writeVBucketState(Vbid vbucketId, const vbucket_state& vbstate);

    template <typename T>
    void addStat(const std::string& prefix,
                 const char* nm,
                 T& val,
                 const AddStatFn& add_stat,
                 const void* c);

    void operator=(const CouchKVStore &from);

    void close();
    bool commit2couchstore(VB::Commit& commitData);

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
     *
     * @returns COUCHSTORE_SUCCESS or a failure code (failure paths log)
     */
    couchstore_error_t saveDocs(Vbid vbid,
                                const std::vector<Doc*>& docs,
                                std::vector<DocInfo*>& docinfos,
                                kvstats_ctx& kvctx);

    void commitCallback(PendingRequestQueue& committedReqs,
                        kvstats_ctx& kvctx,
                        couchstore_error_t errCode);
    couchstore_error_t saveVBState(Db *db, const vbucket_state &vbState);

    /**
     * Save stats for collection cid into the file referenced by db
     * @param db The Db to write to
     * @param cid The collection to update
     * @param stats The stats that should be persisted
     */
    void saveCollectionStats(Db& db,
                             CollectionID cid,
                             Collections::VB::PersistedStats stats);

    /**
     * Delete the count for collection cid
     * @param db The Db to write to
     * @param cid The collection to delete
     */
    void deleteCollectionStats(Db& db, CollectionID cid);

    boost::optional<Collections::VB::PersistedStats> getCollectionStats(
            const KVFileHandle& kvFileHandle, CollectionID collection) override;

    /**
     * Read a document from the local docs index
     *
     * Internally logs errors from couchstore
     *
     * @param db The database handle to read from
     * @param name The name of the document to read
     * @return LocalDocHolder storing null if name does not exist
     */
    LocalDocHolder readLocalDoc(Db& db, const std::string& name);

    /**
     * Write a document to the local docs index
     *
     * Internally logs errors from couchstore
     *
     * @param db The database handle to write to
     * @param name The name of the document to write
     * @param data The data to write
     * @return error code success or other (non-success is logged)
     */
    couchstore_error_t writeLocalDoc(Db& db,
                                     const std::string& name,
                                     cb::const_char_buffer data);

    /**
     * Delete a document from the local docs index
     *
     * Internally logs errors from couchstore
     *
     * @param db The database handle to write to
     * @param name The name of the document to delete
     * @return error code success or other (non-success is logged)
     */
    couchstore_error_t deleteLocalDoc(Db& db, const std::string& name);

    /**
     * Sync the KVStore::collectionsMeta structures to the database.
     *
     * @param db The database handle to update
     * @return error code success or other (non-success is logged)
     */
    couchstore_error_t updateCollectionsMeta(
            Db& db, Collections::VB::Flush& collectionsFlush);

    /**
     * Called from updateCollectionsMeta this function maintains the current
     * uid committed
     *
     * @param db The database handle to update
     * @return error code success or other (non-success is logged)
     */
    couchstore_error_t updateManifestUid(Db& db);

    /**
     * Called from updateCollectionsMeta this function maintains the set of open
     * collections, adding newly opened collections and removing those which are
     * dropped. To validate the creation of new collections, this method must
     * read the dropped collections, which it returns via the std::pair this
     * can then be passed into updateDroppedCollections so it can avoid a
     * duplicated read of the dropped collections.
     *
     * @param db The database handle to update
     * @return a pair of error code and the dropped collections
     */
    std::pair<couchstore_error_t,
              std::vector<Collections::KVStore::DroppedCollection>>
    updateOpenCollections(Db& db);

    /**
     * Called from updateCollectionsMeta this function maintains the set of
     * dropped collections.
     *
     * @param db The database handle to update
     * @param dropped This method will only read the dropped collections from
     *        storage if this optional is not initialised
     * @return error code success or other (non-success is logged)
     */
    couchstore_error_t updateDroppedCollections(
            Db& db,
            boost::optional<
                    std::vector<Collections::KVStore::DroppedCollection>>
                    dropped);

    /**
     * Called from updateCollectionsMeta this function maintains the set of
     * open scopes.
     *
     * @param db The database handle to update
     * @return error code success or other (non-success is logged)
     */
    couchstore_error_t updateScopes(Db& db);

    /**
     * read local document to get the vector of dropped collections from an
     * already open db handle
     * @param db The database handle to read from
     * @return a vector of dropped collections (can be empty)
     */
    std::vector<Collections::KVStore::DroppedCollection> getDroppedCollections(
            Db& db);

    /**
     * read local document to get the count of dropped collections from an
     * already open db handle
     * @param db The database handle to read from
     * @return number of collections still to be fully purged.
     */
    size_t getDroppedCollectionCount(Db& db);

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

    enum class ReadVBStateStatus {
        Success,
        JsonInvalid,
        CorruptSnapshot,
        CouchstoreError
    };

    /**
     * Result of the readVBState function
     */
    struct ReadVBStateResult {
        ReadVBStateStatus status;

        // Only valid if status == ReadVBStateStatus::Success
        vbucket_state state;
    };

    /**
     * Process the vbstate snapshot strings which are stored in the vbstate
     * document. Check for validity and return a status + decoded snapshot.
     */
    std::tuple<ReadVBStateStatus, uint64_t, uint64_t> processVbstateSnapshot(
            Vbid vb,
            vbucket_state_t state,
            int64_t version,
            uint64_t snapStart,
            uint64_t snapEnd,
            uint64_t highSeqno);

    /**
     * Read the vbucket_state from disk.
     */
    ReadVBStateResult readVBState(Db* db, Vbid vbid);

    /**
     * Read the vbucket_state from disk and update the cache if successful
     */
    ReadVBStateResult readVBStateAndUpdateCache(Db* db, Vbid vbid);

    /**
     * Internal getWithHeader that uses Db type for the get
     */
    GetValue getWithHeader(DbHolder& db,
                           const DiskDocKey& key,
                           Vbid vb,
                           GetMetaOnly getMetaOnly);

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
    folly::SharedMutex openDbMutex;

    uint16_t numDbFiles;
    PendingRequestQueue pendingReqsQ;
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
    std::vector<cb::RelaxedAtomic<size_t>> cachedDeleteCount;
    std::vector<cb::RelaxedAtomic<uint64_t>> cachedFileSize;
    std::vector<cb::RelaxedAtomic<uint64_t>> cachedSpaceUsed;

    /* pending file deletions */
    folly::Synchronized<std::queue<std::string>> pendingFileDeletions;

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


    class CouchKVFileHandle : public ::KVFileHandle {
    public:
        CouchKVFileHandle(CouchKVStore& kvstore) : db(kvstore) {
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
