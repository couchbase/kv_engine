/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
    explicit CouchKVStore(const CouchKVStoreConfig& config);

    /**
     * Alternate constructor for injecting base FileOps
     *
     * @param config    Configuration information
     * @param ops       Couchstore FileOps implementation to be used
     */
    CouchKVStore(const CouchKVStoreConfig& config, FileOpsInterface& ops);

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
    cb::engine_errc couchErr2EngineErr(couchstore_error_t errCode);

    cb::engine_errc getAllKeys(
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
            SnapshotSource source) const override;

    std::unique_ptr<ByIdScanContext> initByIdScanContext(
            std::unique_ptr<StatusCallback<GetValue>> cb,
            std::unique_ptr<StatusCallback<CacheLookup>> cl,
            Vbid vbid,
            const std::vector<ByIdRange>& ranges,
            DocumentFilter options,
            ValueFilter valOptions) const override;

    scan_error_t scan(BySeqnoScanContext& sctx) const override;
    scan_error_t scan(ByIdScanContext& sctx) const override;

    std::unique_ptr<KVFileHandle> makeFileHandle(Vbid vbid) const override;

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
    std::pair<bool, Collections::KVStore::Manifest> getCollectionsManifest(
            Vbid vbid) override;

    /**
     * CouchKVStore implements this method as a read of 1 _local document
     */
    std::pair<bool, std::vector<Collections::KVStore::DroppedCollection>>
    getDroppedCollections(Vbid vbid) override;

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
    void closeDatabaseHandle(Db* db) const;

    /**
     * @return the local document name for the collections stats
     */
    static std::string getCollectionStatsLocalDocId(CollectionID cid);

    /**
     * @return the collection id from the given document name (which is expected
     * to be a stats document name).
     */
    static CollectionID getCollectionIdFromStatsDocId(std::string_view id);

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
                              FileOpsInterface* ops = nullptr) const;

    couchstore_error_t openSpecificDB(Vbid vbucketId,
                                      uint64_t rev,
                                      DbHolder& db,
                                      couchstore_open_flags options,
                                      FileOpsInterface* ops = nullptr) const;

    /// Open a specific database file identified with dbFileName and store
    /// it in the provided DbHolder. (openSpecificDB will try to determine
    /// the database filename so it cannot be used to open a temporary
    /// database file (currently used in compaction)
    couchstore_error_t openSpecificDBFile(
            Vbid vbucketId,
            uint64_t rev,
            DbHolder& db,
            couchstore_open_flags options,
            const std::string& dbFileName,
            FileOpsInterface* ops = nullptr) const;

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
    std::pair<bool, Collections::VB::PersistedStats> getCollectionStats(
            const KVFileHandle& kvFileHandle, CollectionID collection) override;
    std::pair<bool, Collections::VB::PersistedStats> getCollectionStats(
            Db& db, CollectionID collection);

    /// Get the collection stats from the local doc named statDocName
    std::pair<bool, Collections::VB::PersistedStats> getCollectionStats(
            Db& db, const std::string& statDocName);

    /**
     * Return value of readLocalDoc. Status indicates if doc is valid or not
     */
    struct ReadLocalDocResult {
        couchstore_error_t status;
        LocalDocHolder doc;
    };

    /**
     * Read a document from the local docs index
     *
     * Internally logs errors from couchstore
     *
     * @param db The database handle to read from
     * @param name The name of the document to read
     * @return ReadLocalDocResult
     */
    ReadLocalDocResult readLocalDoc(Db& db, std::string_view name) const;

    /**
     * Sync the KVStore::collectionsMeta structures to the database.
     *
     * @param db The database handle used to read state
     * @return error code if the update fails
     */
    couchstore_error_t updateCollectionsMeta(
            Db& db, Collections::VB::Flush& collectionsFlush);

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
     * @return error code if the update fails
     */
    couchstore_error_t updateOpenCollections(
            Db& db, Collections::VB::Flush& collectionsFlush);

    /**
     * Called from updateCollectionsMeta this function maintains the set of
     * dropped collections.
     *
     * @param db The database handle to update
     * @param collectionsFlush flush object for a single 'flush/commit'
     * @return error code if the update fails
     */
    couchstore_error_t updateDroppedCollections(
            Db& db, Collections::VB::Flush& collectionsFlush);

    /**
     * Called from updateCollectionsMeta this function maintains the set of
     * open scopes.
     *
     * @param db The database handle to update
     * @param collectionsFlush flush object for a single 'flush/commit'
     * @return error code if the update fails
     */
    couchstore_error_t updateScopes(Db& db,
                                    Collections::VB::Flush& collectionsFlush);

    /**
     * Get the manifest from the Db object.
     * CouchKVStore implements this method as a read of 3 _local documents.
     */
    std::pair<couchstore_error_t, Collections::KVStore::Manifest>
    getCollectionsManifest(Db& db);

    /**
     * Read local document to get the vector of dropped collections from an
     * already open db handle
     *
     * @param db The database handle to read from
     * @return a pair of status and vector of dropped collections (can be empty)
     */
    std::pair<couchstore_error_t,
              std::vector<Collections::KVStore::DroppedCollection>>
    getDroppedCollections(Db& db) const;

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
     * @param hook_ctx CompactionContext for the current compaction
     * @return true if the destination database is caught up with the source
     *              database
     */
    bool tryToCatchUpDbFile(Db& source,
                            Db& destination,
                            std::unique_lock<std::mutex>& lock,
                            bool copyWithoutLock,
                            uint64_t purge_seqno,
                            CompactionReplayPrepareStats& prepareStats,
                            Vbid vbid,
                            CompactionContext& hook_ctx);

    /**
     * The following status codes can be returned by compactDBInternal
     * Success - Compaction was successful, vbucket switched over to the new
     *           file.
     * Aborted - Compaction did not switch over to new file. No error occurred,
     *           but a 'state' change means compaction could not proceed, e.g.
     *           the vbucket was concurrently deleted or rolled back.
     * Failed -  Compaction did not switch over to new file because an error
     *           occurred.
     */
    enum class CompactDBInternalStatus { Success, Aborted, Failed };

    /**
     * Perform compaction using the context and dhook call back.
     *
     * @param sourceDb the source database to compact
     * @param compact_file the name of the temporary file to use for the
     *                     compacted version
     * @param vbLock the lock to acquire exclusive write access to the bucket
     * @param hook_ctx a context with information for the compaction process
     * @return CompactDBInternalStatus indicating the compaction outcome
     */
    CompactDBInternalStatus compactDBInternal(
            DbHolder& sourceDb,
            const std::string& compact_file,
            std::unique_lock<std::mutex>& vbLock,
            CompactionContext* hook_ctx);

    /**
     * This is the final 'phase' of compaction, it assumes that we are going to
     * make newRevision the new data-file and does some work to update
     * memory state to reflect the newly compacted file.
     *
     * @param vbid the vbucket being compacted
     * @param newRevision the revision of the new file
     * @param hookCtx the context for this compaction run
     * @param prepareStats data used for memory state update
     * @returns true if success, false if some failure occurred
     */
    bool compactDBTryAndSwitchToNewFile(
            Vbid vbid,
            uint64_t newRevision,
            CompactionContext* hookCtx,
            const CompactionReplayPrepareStats& prepareStats);

    /**
     * Function considers if the metadata needs patching (following compaction).
     *
     * _local/vbstate may need num_on_disk_prepares and on_disk_prepare_bytes
     * modifying if prepares were purged by compaction.
     *
     * The collection stats may need updating if documents in collections were
     * purged.
     *
     * @param source The source of compaction
     * @param target The compacted database
     * @param stats Data tracked by compaction used for stat updates
     * @param localDocQueue the queue which will be updated with new documents
     * @param vbid vbucket being compacted
     */
    couchstore_error_t maybePatchMetaData(
            Db& source,
            Db& target,
            CompactionStats& stats,
            PendingLocalDocRequestQueue& localDocQueue,
            Vbid vbid);

    void setMb40415RegressionHook(bool value) {
        mb40415_regression_hook = value;
    }

    void setConcurrentCompactionPostLockHook(
            std::function<void(const std::string&)> hook) {
        concurrentCompactionPostLockHook = std::move(hook);
    }

    void setConcurrentCompactionPreLockHook(std::function<void(const std::string&)> hook) {
        concurrentCompactionPreLockHook = std::move(hook);
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
            uint64_t highSeqno) const;

    /**
     * Read the vbucket_state from disk.
     */
    ReadVBStateResult readVBState(Db* db, Vbid vbid) const;

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

    /**
     * Replay needs to update preapre counts, bytes, and collection sizes in the
     * appropriate local documents as we might have changed their values during
     * the compaction. In addition we need to set the purge sequence number in
     * the couchstore header.
     *
     * @param db The database instance to update
     * @param prepareStats All the prepare stats changed during compaction
     * @param collectionStats object tracking collection stats during replay
     * @param purgeSeqno The new value to store in the couchstore header
     * @param hook_ctx The CompactionContext
     * @returns COUCHSTORE_SUCCESS on success, couchstore error otherwise (which
     *          will cause replay to fail).
     */
    couchstore_error_t replayPrecommitHook(
            Db& db,
            CompactionReplayPrepareStats& prepareStats,
            const Collections::VB::FlushAccounting& collectionStats,
            uint64_t purgeSeqno,
            CompactionContext& hook_ctx);

    /**
     * Replay will begin by copying local documents, we detect all modified
     * collections here as we can see 'stat' documents copying from source to
     * target. When a stats doc is seen the stats from the target database are
     * loaded and used as baseline values to which we will add/subtract as
     * documents copy from source to target.
     *
     * @param target handle on the compacted database
     * @param localDocInfo DocInfo for a local document
     * @param collectionStats object to load stats into
     */
    couchstore_error_t replayPreCopyLocalDoc(
            Db& target,
            const DocInfo* localDocInfo,
            Collections::VB::FlushAccounting& collectionStats);

    /**
     * Helper method for replayPrecommitHook, processes the dropped collections
     * (if any) and returns a local doc queue that has the correct dropped
     * collection data in it (or is an empty queue)
     * @param db The database instance to read and update
     * @param hook_ctx The CompactionContext
     * @return queue that replayPrecommitHook uses for further local doc updates
     */
    PendingLocalDocRequestQueue replayPrecommitProcessDroppedCollections(
            Db& db, const CompactionContext& hook_ctx);

    /**
     * Open the current vbid.couch.rev file if it exists, or atomic-create it.
     * The result of the function is either a well-formed file that contains the
     * first Header at filepos 0 (success path), or no file (failure path).
     *
     * @param vbid
     * @returns The DbHolder for file or {} if the operation fails
     */
    std::optional<DbHolder> openOrCreate(Vbid vbid) noexcept;

    const CouchKVStoreConfig& configuration;

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
    CouchKVStore(const CouchKVStoreConfig& config,
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
                 const CouchKVStoreConfig& config,
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
                 const CouchKVStoreConfig& config,
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
    TestingHook<const std::string&> concurrentCompactionPostLockHook;

    /// Same as above but the hook is called before the vBucket lock is
    /// re-acquired. This allows the actual flusher to run in the hook rather
    /// poking the kvstore manually
    TestingHook<const std::string&> concurrentCompactionPreLockHook;
};
