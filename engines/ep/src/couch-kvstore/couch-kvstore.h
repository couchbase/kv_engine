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

class CouchKVStoreConfig;
class DbHolder;
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
    explicit CouchRequest(queued_item item);

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
    DocInfo* getDbDocInfo() {
        return &dbDocInfo;
    }

    void setUpdate() {
        update = true;
    }

    bool isUpdate() const {
        return update;
    }

protected:
    static couchstore_content_meta_flags getContentMeta(const Item& it);

    value_t value;

    MetaData meta;
    Doc dbDoc;
    DocInfo dbDocInfo;

    // This flag is at save-doc-callback if the write if for an existing key.
    // Then used in the commit-callback for updating stats.
    bool update{false};
};

/**
 * Class for storing an update to the local documents index, will store a key
 * and value alongside a couchstore LocalDoc struct.
 */
class CouchLocalDocRequest {
public:
    struct IsDeleted {};
    /// store a key and value
    CouchLocalDocRequest(std::string&& key, std::string&& value);
    /// store a key and flatbuffer value (which will be copied into a string)
    CouchLocalDocRequest(std::string&& key,
                         const flatbuffers::DetachedBuffer& value);
    /// store a key for deletion
    CouchLocalDocRequest(std::string&& key, IsDeleted);
    /// @return reference to the LocalDoc structure
    LocalDoc& getLocalDoc();

protected:
    void setupKey();
    void setupValue();

    std::string key;
    std::string value;
    LocalDoc doc{};
};

struct CompactionReplayPrepareStats {
    uint64_t onDiskPrepares = 0;
    uint64_t onDiskPrepareBytes = 0;
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
    explicit CouchKVStore(CouchKVStoreConfig& config);

    /**
     * Alternate constructor for injecting base FileOps
     *
     * @param config    Configuration information
     * @param ops       Couchstore FileOps implementation to be used
     */
    CouchKVStore(CouchKVStoreConfig& config, FileOpsInterface& ops);

    /**
     * Deconstructor
     */
    ~CouchKVStore() override;

    /**
     * A read only CouchKVStore can only be created by a RW store. They should
     * be created in pairs as they share some data.
     *
     * @return a unique_ptr holding a RO 'sibling' to this object.
     */
    std::unique_ptr<CouchKVStore> makeReadOnlyStore() const;

    /**
     * Reset vbucket to a clean state.
     */
    void reset(Vbid vbucketId) override;

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
    void rollback() override {
        if (isReadOnly()) {
            throw std::logic_error("CouchKVStore::rollback: Not valid on a "
                    "read-only object.");
        }
        if (inTransaction) {
            inTransaction = false;
            transactionCtx.reset();
        }
    }

    /**
     * Query the properties of the underlying storage.
     *
     * @return properties of the underlying storage system
     */
    StorageProperties getStorageProperties() override;

    void set(queued_item item) override;

    GetValue get(const DiskDocKey& key, Vbid vb, ValueFilter filter) override;

    using KVStore::get;

    GetValue getWithHeader(const KVFileHandle& kvFileHandle,
                           const DiskDocKey& key,
                           Vbid vb,
                           ValueFilter filter) override;

    void getMulti(Vbid vb, vb_bgfetch_queue_t& itms) override;

    void getRange(Vbid vb,
                  const DiskDocKey& startKey,
                  const DiskDocKey& endKey,
                  ValueFilter filter,
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
    std::vector<vbucket_state*> listPersistedVbuckets() override;

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

    bool compactDB(std::unique_lock<std::mutex>& vbLock,
                   std::shared_ptr<CompactionContext> ctx) override;

    void abortCompactionIfRunning(std::unique_lock<std::mutex>& vbLock,
                                  Vbid vbid) override;

    vbucket_state* getCachedVBucketState(Vbid vbid) override;

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
                            std::unique_ptr<RollbackCB>) override;

    /**
     * Perform pending tasks after persisting dirty items
     */
    void pendingTasks() override;

    bool getStat(std::string_view name, size_t& value) const override;

    couchstore_error_t fetchDoc(Db* db,
                                DocInfo* docinfo,
                                GetValue& docValue,
                                Vbid vbId,
                                ValueFilter filter);
    ENGINE_ERROR_CODE couchErr2EngineErr(couchstore_error_t errCode);

    ENGINE_ERROR_CODE getAllKeys(
            Vbid vbid,
            const DiskDocKey& start_key,
            uint32_t count,
            std::shared_ptr<StatusCallback<const DiskDocKey&>> cb) override;

    bool supportsHistoricalSnapshots() const override {
        return true;
    }

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
    scan_error_t scan(ByIdScanContext& sctx) override;

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
    Collections::KVStore::Manifest getCollectionsManifest(Db& db);

    /**
     * CouchKVStore implements this method as a read of 1 _local document
     */
    std::vector<Collections::KVStore::DroppedCollection> getDroppedCollections(
            Vbid vbid) override;

    /**
     * Read vbstate from disk. Does not update the cached vbstate.
     *
     * @param vbid
     * @return the persisted vbstate
     */
    vbucket_state getPersistedVBucketState(Vbid vbid) override;

    /// Get the logger used by this bucket
    BucketLogger& getLogger() {
        return logger;
    }

    /**
     * Close the given database (and free the memory)
     *
     * @param db Database
     */
    void closeDatabaseHandle(Db* db);

protected:
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

        LocalDocHolder(LocalDocHolder&& other) noexcept {
            localDoc = other.localDoc;
            other.localDoc = nullptr;
        }

        LocalDocHolder& operator=(LocalDocHolder&& other) noexcept {
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

    const KVStoreConfig& getConfig() const override;

    /**
     * Container for pending couchstore requests.
     *
     * Using deque as (a) it doesn't move (and hence invalidate) any existing
     * elements, which is relied on as CouchRequest has pointers to it's own
     * data, and (b) as the expansion behviour is less aggressive compared to
     * std::vector (CouchRequest objects are ~256 bytes in size).
     */
    using PendingRequestQueue = std::deque<CouchRequest>;
    using PendingLocalDocRequestQueue = std::deque<CouchLocalDocRequest>;

    /*
     * Returns the DbInfo for the given vbucket database.
     */
    cb::couchstore::Header getDbInfo(Vbid vbid);

    bool writeVBucketState(Vbid vbucketId, const vbucket_state& vbstate);

    void close();
    bool commit2couchstore(VB::Commit& commitData);

    /**
     * Populate CouchKVStore::dbFileRevMap and remove any couch files that are
     * not the most recent revision.
     *
     * This is done by getting a directory listing of dbname and then for each
     * vbucket file storing the greatest revision in the dbFileRevMap. Any files
     * found that are not the most recent are deleted.
     *
     * The data used in population, a map of vbid to revisions is returned
     * so that a second directory scan isn't needed for further initialisation.
     *
     * @return map of vbid to revisions used in populating dbFileRevMap
     */
    std::unordered_map<Vbid, std::unordered_set<uint64_t>>
    populateRevMapAndRemoveStaleFiles();

    /**
     * Get a map of vbucket => revision(s), created from the given filenames.
     * The expected usage is that a list of *.couch files found in the dbname
     * directory is created and handed to this function.
     *
     * Multiple revisions can exist on disk primarily from an unclean shutdown.
     *
     * Testing/usage note: the input file list is expected to be effectively
     * "ls *.couch*", CouchKVStore uses the following for input:
     *   cb::io::findFilesContaining(dbname, ".couch")
     *
     * master.couch is ignored and any other files are logged as a warning
     *
     * @param a vector of file names (can be absolute paths to files)
     * @return map of vbid to revisions
     */
    std::unordered_map<Vbid, std::unordered_set<uint64_t>> getVbucketRevisions(
            const std::vector<std::string>& filenames) const;

    /**
     * Set the revision of the vbucket
     * @param vbucketId vbucket to update
     * @param newFileRev new value
     */
    void updateDbFileMap(Vbid vbucketId, uint64_t newFileRev);

    /// @return the current file revision for the vbucket
    uint64_t getDbRevision(Vbid vbucketId) const;

    couchstore_error_t openDB(Vbid vbucketId,
                              DbHolder& db,
                              couchstore_open_flags options,
                              FileOpsInterface* ops = nullptr);

    couchstore_error_t openSpecificDB(Vbid vbucketId,
                                      uint64_t rev,
                                      DbHolder& db,
                                      couchstore_open_flags options,
                                      FileOpsInterface* ops = nullptr);

    /// Open a specific database file identified with dbFileName and store
    /// it in the provided DbHolder. (openSpecificDB will try to determine
    /// the database filename so it cannot be used to open a temporary
    /// database file (currently used in compaction)
    couchstore_error_t openSpecificDBFile(Vbid vbucketId,
                                          uint64_t rev,
                                          DbHolder& db,
                                          couchstore_open_flags options,
                                          const std::string& dbFileName,
                                          FileOpsInterface* ops = nullptr);

    /**
     * save the Documents held in docs to the file associated with vbid/rev
     *
     * @param vbid the vbucket file to open/write/commit
     * @param docs vector of Doc* to be written (can be empty)
     * @param docsinfo vector of DocInfo* to be written (non const due to
     *        couchstore API). Entry n corresponds to entry n of docs.
     * @param kvReqs Vector of pointers to KV requests being passed to the
     *        storage. Same order as docs and docsinfo.
     * @param kvctx a stats context object to update
     *
     * @returns COUCHSTORE_SUCCESS or a failure code (failure paths log)
     */
    couchstore_error_t saveDocs(Vbid vbid,
                                const std::vector<Doc*>& docs,
                                const std::vector<DocInfo*>& docinfos,
                                const std::vector<void*>& kvReqs,
                                kvstats_ctx& kvctx);

    void commitCallback(PendingRequestQueue& committedReqs,
                        kvstats_ctx& kvctx,
                        couchstore_error_t errCode);

    /**
     * Turn the given vbstate into a string (JSON) representation suitable
     * for saving the local/vbstate document
     */
    std::string makeJsonVBState(const vbucket_state& vbState);

    /**
     * Save stats for collection cid into the file referenced by db
     * @param db The Db to write to
     * @param cid The collection to update
     * @param stats Stats an object of stats collected by the flush
     */
    void saveCollectionStats(Db& db,
                             CollectionID cid,
                             const Collections::VB::PersistedStats& stats);

    /**
     * Delete the count for collection cid
     * @param cid The collection to delete
     */
    void deleteCollectionStats(CollectionID cid);

    /// Get the collection stats for the given collection
    Collections::VB::PersistedStats getCollectionStats(
            const KVFileHandle& kvFileHandle, CollectionID collection) override;
    Collections::VB::PersistedStats getCollectionStats(Db& db,
                                                       CollectionID collection);

    /// Get the collection stats from the local doc named statDocName
    Collections::VB::PersistedStats getCollectionStats(
            Db& db, const std::string& statDocName);

    /**
     * Read a document from the local docs index
     *
     * Internally logs errors from couchstore
     *
     * @param db The database handle to read from
     * @param name The name of the document to read
     * @return LocalDocHolder storing null if name does not exist
     */
    LocalDocHolder readLocalDoc(Db& db, std::string_view name);

    /**
     * Sync the KVStore::collectionsMeta structures to the database.
     *
     * @param db The database handle used to read state
     */
    void updateCollectionsMeta(Db& db,
                               Collections::VB::Flush& collectionsFlush);

    /**
     * Called from updateCollectionsMeta this function maintains the current
     * uid committed by creating a pending write to the local document index.
     * @param collectionsFlush flush object for a single 'flush/commit'
     */
    void updateManifestUid(Collections::VB::Flush& collectionsFlush);

    /**
     * Called from updateCollectionsMeta this function maintains the set of open
     * collections, adding newly opened collections and removing those which are
     * dropped. To validate the creation of new collections, this method must
     * read the dropped collections, which it returns via the std::pair this
     * can then be passed into updateDroppedCollections so it can avoid a
     * duplicated read of the dropped collections.
     *
     * @param db The database handle to update
     * @param collectionsFlush flush object for a single 'flush/commit'
     */
    void updateOpenCollections(Db& db,
                               Collections::VB::Flush& collectionsFlush);

    /**
     * Called from updateCollectionsMeta this function maintains the set of
     * dropped collections.
     *
     * @param db The database handle to update
     * @param collectionsFlush flush object for a single 'flush/commit'
     */
    void updateDroppedCollections(Db& db,
                                  Collections::VB::Flush& collectionsFlush);

    /**
     * Called from updateCollectionsMeta this function maintains the set of
     * open scopes.
     *
     * @param db The database handle to update
     * @param collectionsFlush flush object for a single 'flush/commit'
     * @return error code success or other (non-success is logged)
     */
    void updateScopes(Db& db, Collections::VB::Flush& collectionsFlush);

    /**
     * read local document to get the vector of dropped collections from an
     * already open db handle
     * @param db The database handle to read from
     * @return a vector of dropped collections (can be empty)
     */
    std::vector<Collections::KVStore::DroppedCollection> getDroppedCollections(
            Db& db);

    /**
     * Unlink selected couch file, which will be removed by the OS,
     * once all its references close.
     */
    void unlinkCouchFile(Vbid vbucket, uint64_t fRev);

    /**
     * Remove compact file if this isn't the RO store
     */
    void maybeRemoveCompactFile(const std::string& dbname, Vbid vbid);

    /**
     * Remove compact file if this isn't the RO store
     * @throws logic_error if this is the RO store
     */
    void removeCompactFile(const std::string& filename, Vbid vbid);

    /** Try to move all modifications from the source Db over to the
     * destination database.
     *
     * @param source The database to start from
     * @param destination The destination database
     * @param lock The write lock for the destination database (must be held
     *             when calling the method (and will be locked when the method
     *             returns)
     * @param copyWithoutLock Should data be copied while holding the lock (
     *                        prevents other threads to write to the
     *                        source database)
     * @param purge_seqno The purge seqno to set in the headers
     * @param prepareStats The prepare stats that compaction updates
     * @param vbid the vbucket (used for logging)
     * @return true if the destination database is caught up with the source
     *              database
     */
    bool tryToCatchUpDbFile(Db& source,
                            Db& destination,
                            std::unique_lock<std::mutex>& lock,
                            bool copyWithoutLock,
                            uint64_t purge_seqno,
                            CompactionReplayPrepareStats& prepareStats,
                            Vbid vbid);

    /**
     * Perform compaction using the context and dhook call back.
     *
     * @param sourceDb the source database to compact
     * @param compact_file the name of the temporary file to use for the
     *                     compacted version
     * @param vbLock the lock to acquire exclusive write access to the bucket
     * @param hook_ctx a context with information for the compaction process
     * @param dhook a docinfo hook which will be called with each compacted key
     * @return true indicating the compaction was successful.
     */
    bool compactDBInternal(DbHolder& sourceDb,
                           const std::string& compact_file,
                           std::unique_lock<std::mutex>& vbLock,
                           CompactionContext* hook_ctx);

    /// try to load _local/vbstate and patch the num_on_disk_prepares
    /// and subtract the number of prepares pruned
    couchstore_error_t maybePatchOnDiskPrepares(
            Db& db,
            const CompactionStats& stats,
            PendingLocalDocRequestQueue& localDocQueue,
            Vbid vbid);

    void setMb40415RegressionHook(bool value) {
        mb40415_regression_hook = value;
    }

    void setConcurrentCompactionPostLockHook(
            std::function<void(const std::string&)> hook) {
        concurrentCompactionPostLockHook = std::move(hook);
    }

    enum class ReadVBStateStatus : uint8_t {
        Success = 0,
        JsonInvalid,
        CorruptSnapshot,
        CouchstoreError
    };

    std::string to_string(ReadVBStateStatus status);

    /**
     * Result of the readVBState function
     */
    struct ReadVBStateResult {
        ReadVBStateStatus status{ReadVBStateStatus::Success};

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
                           ValueFilter filter);

    /**
     * Process the given queue of local index updates (pendingLocalReqsQ) and
     * make one call to cb::couchstore::saveLocalDocuments. no-op if the queue
     * is empty
     */
    couchstore_error_t updateLocalDocuments(Db& db,
                                            PendingLocalDocRequestQueue& queue);

    /**
     * Write a single name/value to the local index of db using
     * one call to couchstore_save_local_document
     */
    couchstore_error_t updateLocalDocument(Db& db,
                                           std::string_view name,
                                           std::string_view value);

    CouchKVStoreConfig& configuration;

    // The directory for the database
    const std::string dbname;

    using RevisionMap = folly::Synchronized<
            std::vector<Monotonic<uint64_t, ThrowExceptionPolicy>>>;

    /**
     * Per-vbucket file revision atomic to ensure writer threads see increments.
     *
     * Owned via a shared_ptr, as there should be a single RevisionMap per
     * RW/RO pair.
     */
    std::shared_ptr<RevisionMap> dbFileRevMap;

    PendingRequestQueue pendingReqsQ;

    /**
     * A queue of pending local document updates (set or delete) that is used
     * by the 'flush' path of KVStore (begin/[set|del]/commit). The commit
     * path will write this queue of requests before finally calling
     * couchstore_commit
     */
    PendingLocalDocRequestQueue pendingLocalReqsQ;

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
    /// Size of on-disk prepares, indexed by vBucket RelaxedAtomic to allow
    /// stats access without lock.
    std::vector<cb::RelaxedAtomic<size_t>> cachedOnDiskPrepareSize;

    /* pending file deletions */
    folly::Synchronized<std::queue<std::string>> pendingFileDeletions;

    /// We need to make sure that multiple threads won't start compaction
    /// on the same vbucket while compaction for that vbucket is running.
    std::vector<std::atomic_bool> vbCompactionRunning;

    /// A vector where each entry represents a vbucket and the rollback
    /// may set the element to true if it runs at the same time as
    /// compaction is running (and we just need to abort the compaction)
    std::vector<std::atomic_bool> vbAbortCompaction;

    BucketLogger& logger;

    /**
     * Base fileops implementation to be wrapped by stat collecting fileops
     */
    FileOpsInterface& base_ops;

    /**
     * Construct the store, this constructor does the object initialisation and
     * is used by the public read/write constructors and the private read-only
     * constructor.
     *
     * @param config configuration data for the store
     * @param ops the file ops to use
     * @param readOnly true if the store can only do read functionality
     * @param revMap a revisionMap to use (which should be data created by the
     *        RW store).
     */
    CouchKVStore(CouchKVStoreConfig& config,
                 FileOpsInterface& ops,
                 bool readOnly,
                 std::shared_ptr<RevisionMap> revMap);

    struct CreateReadWrite {};
    /**
     * Construction for a read-write CouchKVStore.
     * This constructor:
     *  - Attempts to create the data directory
     *    Removes stale data files (e.g. when multiple copies of a vbucket file
     *    exist).
     *  - Removes any .compact files, but only for the most recent revision of
     *    a vbucket. E.g. if x.couch.y then delete x.couch.y.compact
     *  - Creates and initialises a vbucket revision 'map'
     *  - Initialises from the vbucket files that remain in the data directory
     *
     * @param CreateReadWrite tag to clearly differentiate from ReadOnly method
     * @param config config to use
     * @param ops The ops interface to use for File I/O
     */
    CouchKVStore(CreateReadWrite,
                 CouchKVStoreConfig& config,
                 FileOpsInterface& ops);

    struct CreateReadOnly {};
    /**
     * Construction for a read-only CouchKVStore.
     * This constructor will initialise from files it finds in the data
     * directory and use the given RevisionMap for operations.
     *
     * @param CreateReadOnly tag to clearly differentiate from ReadWrite method
     * @param config config to use
     * @param ops The ops interface to use for File I/O
     * @param dbFileRevMap to use
     */
    CouchKVStore(CreateReadOnly,
                 CouchKVStoreConfig& config,
                 FileOpsInterface& ops,
                 std::shared_ptr<RevisionMap> dbFileRevMap);

    /**
     * Common RO/RW initialisation
     *
     * @param map reference to data created by RW construction this method needs
     *        the keys of the map
     */
    void initialize(
            const std::unordered_map<Vbid, std::unordered_set<uint64_t>>& map);

    /**
     * Construction helper method, creates the wrapped vector and resizes it
     *
     * @param vbucketCount size of RevisionMap
     * @returns An initialised RevisionMap accessed via the shared_ptr
     */
    static std::shared_ptr<RevisionMap> makeRevisionMap(size_t vbucketCount);

    /// Allow the unit tests to add a hook into compaction
    bool mb40415_regression_hook{false};

    /// Hook to allow for unit testing of compaction and flushing happening
    /// in parallel.
    /// The hook gets called after the initial compaction runs, and then
    /// after each step in the catch-up-phase. Notably it gets called after
    /// the vbucket lock is re-acquired
    std::function<void(const std::string&)> concurrentCompactionPostLockHook =
            [](const std::string&) {};
};
