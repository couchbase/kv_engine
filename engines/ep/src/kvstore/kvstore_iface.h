/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "blob.h"
#include "callbacks_fwd.h"
#include "collections/kvstore.h"
#include "ep_types.h"
#include "persistence_callback.h"
#include "vbucket_state.h"

#include <memcached/engine.h>
#include <memcached/engine_common.h>
#include <memcached/engine_error.h>
#include <memcached/thread_pool_config.h>

class ByIdScanContext;
class BySeqnoScanContext;
class CookieIface;
class GetValue;
class KVFileHandle;
class KVStoreConfig;
class KVStoreStats;
class RollbackCB;
class RollbackCtx;
class RollbackResult;
class StorageProperties;
class PersistenceCallback;
class vb_bgfetch_item_ctx_t;

struct ByIdRange;
struct CompactionConfig;
struct CompactionContext;
struct DBFileInfo;
struct TransactionContext;

namespace Collections::VB {
struct PersistedStats;
} // namespace Collections::VB

namespace VB {
class Commit;
}

namespace cb::snapshot {
struct Manifest;
} // namespace cb::snapshot

/**
 * When fetching documents from disk, what form should the value be returned?
 */
enum class ValueFilter {
    /// Only return the key & metadata (no value).
    KEYS_ONLY,
    /// Return key & metadata, and value. If value is compressed then return
    /// in compressed form.
    VALUES_COMPRESSED,
    /// Return key & metadata, and value. Value will be returned uncompressed.
    VALUES_DECOMPRESSED
};

std::string format_as(ValueFilter vf);

enum class ScanStatus {
    Success, // reached the end
    Yield, // scan should yield and resume later
    Cancelled, // scan cannot continue and is cancelled
    Failed, // some critical failure occurred, e.g. a fatal system call error
};

std::ostream& operator<<(std::ostream& os, ScanStatus);
std::string format_as(ScanStatus);

enum class DocumentFilter {
    ALL_ITEMS,
    NO_DELETES,
    ALL_ITEMS_AND_DROPPED_COLLECTIONS
};
std::string format_as(DocumentFilter df);

enum class SnapshotSource {
    Head, // Latest version of all keys
    HeadAllVersions // All versions from the head (used in CDC stream)
};

using MakeCompactionContextCallback =
        std::function<std::shared_ptr<CompactionContext>(
                Vbid, CompactionConfig&, uint64_t)>;

using vb_bgfetch_queue_t =
        std::unordered_map<DiskDocKey, vb_bgfetch_item_ctx_t>;

// Revision of a KVStore used by prepareToDelete et. al. to allow Nexus to deal
// with different implementations dealing with revisioning differently.
class KVStoreRevision {
public:
    virtual ~KVStoreRevision() = default;

    KVStoreRevision(uint64_t rev) : rev(rev) {
    }

    uint64_t getRevision() const {
        return rev;
    }

protected:
    uint64_t rev;
};

/**
 * The following status codes can be returned by compactDB
 * Success - Compaction was successful, vbucket switched over to the new
 *           file.
 * Aborted - Compaction did not switch over to new file. No error occurred,
 *           but a 'state' change means compaction could not proceed, e.g.
 *           the vbucket was concurrently deleted or rolled back.
 * Failed -  Compaction did not switch over to new file because an error
 *           occurred.
 */
enum class CompactDBStatus { Success, Aborted, Failed };

std::ostream& operator<<(std::ostream&, const CompactDBStatus&);
std::string format_as(CompactDBStatus);

/**
 * Functional interface of a KVStore. Each KVStore implementation must implement
 * each of these functions.
 */
class KVStoreIface {
public:
    virtual ~KVStoreIface() = default;

    /**
     * Called when the engine is going away so we can shutdown any backend tasks
     * the underlying store create to prevent them from racing with destruction.
     */
    virtual void deinitialize() = 0;

    /**
     * Called when the engine is about to be paused, in preparation for saving
     * a copy of the on-disk state.
     * KVStore should perform any necessary work to ensure that on-disk state
     * is quiesced and that after this method returns on-disk state is not
     * modified until unpasue() is called.
     * @returns true if KVStore successfully paused, otherwise false.
     */
    virtual bool pause() = 0;

    /**
     * Called when the engine is about to be resumed, cancelling a previous
     * pause.
     * KVStore should perform any necessary work to resume background
     * flushing / persistence operations.
     */
    virtual void resume() = 0;

    /**
     * Allow the kvstore to add extra statistics information
     * back to the client
     * @param prefix prefix to use for the stats
     * @param add_stat the callback function to add statistics
     * @param cookie the cookie to pass to the callback function
     */
    virtual void addStats(const AddStatFn& add_stat,
                          CookieIface& cookie) const = 0;

    /// Get the Encryption Key Identifiers used by the provided VBucket
    virtual std::pair<cb::engine_errc, nlohmann::json>
    getVbucketEncryptionKeyIds(Vbid vb) const {
        return {cb::engine_errc::not_supported, {}};
    }

    /**
     * Prepare a snapshot.
     *
     * All files in the snapshot should be relative to the snapshot directory.
     * If the method fails (or an exception is thrown) the information in
     * the manifest will be discarded and the snapshot directory will be
     * removed by the caller.
     *
     * @param snapshotDirectory the destination directory for the snapshot
     * @param vb The vbucket to create the snapshot for
     * @param manifest The snapshot manifest to populate with information
     * @return status of the operation
     */
    virtual cb::engine_errc prepareSnapshot(
            const std::filesystem::path& snapshotDirectory,
            Vbid vb,
            cb::snapshot::Manifest& manifest);

    /**
     * Request the specified statistic name from the kvstore.
     *
     * @param name The name of the statistic to fetch.
     * @param[out] value Value of the given stat (if exists).
     * @return True if the stat exists, is of type size_t and was successfully
     *         returned, else false.
     */
    virtual bool getStat(std::string_view name, size_t& value) const = 0;

    /// Request the specified statistics from kvstore.
    ///
    /// @param [in] keys specifies a set of statistics to be fetched.
    /// @return statistic values. Note that the string_view keys in the returned
    /// map refer to the same string keys that the input string_view refers to.
    /// Hence the map is ok to use only as long as the string keys live.
    ///
    virtual GetStatsMap getStats(
            gsl::span<const std::string_view> keys) const = 0;

    /**
     * Show kvstore specific timing stats.
     *
     * @param add_stat the callback function to add statistics
     * @param c the cookie to pass to the callback function
     */
    virtual void addTimingStats(const AddStatFn& add_stat,
                                CookieIface& c) const = 0;

    /**
     * Resets kvstore specific stats
     */
    virtual void resetStats() = 0;

    virtual size_t getMemFootPrint() const = 0;

    /**
     * Commit a transaction (unless not currently in one).
     *
     * @param txnCtx context for the current transaction (consumes the
     *        TransactionContext)
     * @param commitData a reference to a VB::Commit object which is required
     *        for persisted metadata updates and collection item counting
     * @return false if the commit fails
     */
    virtual bool commit(std::unique_ptr<TransactionContext> txnCtx,
                        VB::Commit& commitData) = 0;

    /**
     * Get the properties of the underlying storage.
     */
    virtual StorageProperties getStorageProperties() const = 0;

    /**
     * Set an item into the kv store. cc
     *
     * @param txnCtx context for the transaction
     * @param item The item to store
     */
    virtual void set(TransactionContext& txnCtx, queued_item item) = 0;

    /**
     * Get an item from the kv store.
     * @param key The document key to fetch.
     * @param vb The vbucket to fetch from.
     * @param filter In what form should the item be fetched?
     *        Item::getDatatype() will reflect the format they are returned in.
     */
    virtual GetValue get(
            const DiskDocKey& key,
            Vbid vb,
            ValueFilter filter = ValueFilter::VALUES_DECOMPRESSED) const = 0;

    /**
     * Retrieve the document with a given key from the underlying storage
     * @param kvFileHandle the open file to get from
     * @param key the key of a document to be retrieved
     * @param vb vbucket id of a document
     * @param filter In what form should the item be fetched?
     *        Item::getDatatype() will reflect the format they are returned in.
     * @return the result of the get
     */
    virtual GetValue getWithHeader(const KVFileHandle& kvFileHandle,
                                   const DiskDocKey& key,
                                   Vbid vb,
                                   ValueFilter filter) const = 0;

    /**
     * Updates the ratio of alive vbuckets in this shard to alive vbuckets in
     * all shards.
     *
     * This is used to determine what fraction of the memory quota each shards
     * can use.
     *
     * @param ratio the new ratio
     */
    virtual void setAliveVBucketRatio(double ratio) = 0;

    /**
     * Set the max bucket quota to the given size.
     *
     * @param size  The new max bucket quota size.
     */
    virtual void setMaxDataSize(size_t size) = 0;

    using CreateItemCB =
            std::function<std::pair<cb::engine_errc, std::unique_ptr<Item>>(
                    const DocKeyView& key,
                    size_t nbytes,
                    uint32_t flags,
                    time_t exptime,
                    const value_t& body,
                    protocol_binary_datatype_t datatype,
                    uint64_t theCas,
                    int64_t bySeq,
                    Vbid vbid,
                    uint64_t revSeq)>;

    static CreateItemCB getDefaultCreateItemCallback();

    /**
     * Retrieve multiple documents from the underlying storage system at once.
     *
     * @param vb vbucket id of a document
     * @param itms list of items whose documents are going to be retrieved.
     * @param createItemCb the callback that will determine if there is
     * sufficient memory before creating an item.
     */
    virtual void getMulti(Vbid vb,
                          vb_bgfetch_queue_t& itms,
                          CreateItemCB createItemCb) const = 0;

    /**
     * Callback for getRange().
     * @param value The fetched value. Note r-value receiver can modify (e.g.
     * move-from) it if desired.
     */
    using GetRangeCb = std::function<void(GetValue&& value)>;

    /**
     * Get a range of items from a single vBucket
     * (if supported by the kv store).
     *
     * Searches the given vBucket for all items with keys in the half-open
     * range [startKey,endKey). For each item found invokes the given callback.
     *
     * @param vb vBucket id to fetch from.
     * @param startKey The key to start searching at. Search includes this key.
     * @param endKey The key to end searching at. Search excludes this key.
     * @param filter In what form should the item be fetched?
     * @param callback Callback invoked for each key found.
     * @throws std::runtime_error if the range scan could not be successfully
     *         completed. (Note: finding zero docments in the given range is
     *         considered successful).
     */
    virtual void getRange(Vbid vb,
                          const DiskDocKey& startKey,
                          const DiskDocKey& endKey,
                          ValueFilter filter,
                          const GetRangeCb& cb) const = 0;

    /**
     * Delete an item from the kv store.
     *
     * @param txnCtx context for the transaction
     * @param item The item to delete
     */
    virtual void del(TransactionContext& txnCtx, queued_item item) = 0;

    /**
     * Delete a given vbucket database instance from underlying storage
     *
     * @param vbucket vbucket id
     * @param fileRev the revision of the file to delete
     */
    virtual void delVBucket(Vbid vbucket,
                            std::unique_ptr<KVStoreRevision> fileRev) = 0;

    /**
     * Get a list of all persisted vbuckets (with their states).
     */
    virtual std::vector<vbucket_state*> listPersistedVbuckets() = 0;

    /**
     * This API is called after warmup has populated the vBucket map and after
     * flushing the vbucket states for all vbuckets managed by this KVStore.
     */
    virtual void completeLoadingVBuckets() = 0;

    /**
     * Persist a snapshot of the vbucket states in the underlying storage
     * system.
     *
     * @param vbucketId id of the vbucket that needs to be snapshotted
     * @param meta Information to be passed to the storage
     */
    virtual bool snapshotVBucket(Vbid vbucketId, const VB::Commit& meta) = 0;

    /**
     * Compact a database file.
     *
     * @param vbLock a lock to serialize compaction and flusher to the
     *               specific vbucket. When called the lock is _HELD_
     *               so engines who don't need exclusive access should
     *               release the lock. The lock may be held or released
     *               upon return, the caller will take the appropriate action.
     * @param c shared_ptr to the CompactionContext that includes various
     * callbacks and compaction parameters
     * @return status of compaction
     */
    virtual CompactDBStatus compactDB(std::unique_lock<std::mutex>& vbLock,
                                      std::shared_ptr<CompactionContext> c) = 0;

    /**
     * Abort compaction for the provided vbucket if it is running
     *
     * @param vbLock The lock used to serialize access for compaction and
     *               flusher (should be held when calling the method; added to
     *               the API so that the inner parts can ensure that it is
     *               held).
     * @param vbucket The vbucket of interest
     */
    virtual void abortCompactionIfRunning(std::unique_lock<std::mutex>& vbLock,
                                          Vbid vbid) = 0;

    /**
     * Returns a ptr to the vbucket_state in the KVStore cache. Not all
     * implementations can simply return the cached value (magma) so this is
     * virtual.
     */
    virtual vbucket_state* getCachedVBucketState(Vbid vbid) = 0;

    enum class ReadVBStateStatus : uint8_t {
        Success = 0,
        NotFound,
        JsonInvalid,
        CorruptSnapshot,
        Error,
    };

    /**
     * Result of the readVBState function
     */
    struct ReadVBStateResult {
        ReadVBStateStatus status{ReadVBStateStatus::Success};

        // Only valid if status == ReadVBStateStatus::Success
        vbucket_state state;
    };

    /**
     * Return the vbucket_state stored on disk for the given vBucket. Does NOT
     * update the cachedVBState.
     * @throws exceptions if there was a problem returning the state.
     */
    virtual ReadVBStateResult getPersistedVBucketState(Vbid vbid) const = 0;

    /**
     * Return the vbucket_state stored on disk for the given handle
     *
     * @param handle A KVFileHandle for a snapshot
     * @param vbid the vbucket ID for the snapshot (used in logging only)
     * @throws exceptions if there was a problem returning the state.
     */
    virtual ReadVBStateResult getPersistedVBucketState(KVFileHandle& handle,
                                                       Vbid vbid) const = 0;

    /**
     * Get the number of deleted items that are persisted to a vbucket file
     *
     * @param vbid The vbucket if of the file to get the number of deletes for.
     * @returns the number of deletes which are persisted
     * @throws std::runtime_error (and subclasses) if it was not possible to
     *         obtain a count of persisted deletes.
     */
    virtual size_t getNumPersistedDeletes(Vbid vbid) = 0;

    /**
     * This method will return information about the file whose id
     * is passed in as an argument. The information returned contains
     * the item count, file size and space used.
     *
     * Note: For some backends (Couchstore), this may require going to disk.
     *
     * @throws std::runtime_error (and subclasses) if it was not possible to
     *         obtain the DB file info.
     */
    virtual DBFileInfo getDbFileInfo(Vbid dbFileId) = 0;

    /**
     * This method will return file size and space used for the
     * entire KV store
     */
    virtual DBFileInfo getAggrDbFileInfo() = 0;

    /**
     * This method will return the total number of items in the vbucket
     *
     * vbid - vbucket id
     */
    virtual size_t getItemCount(Vbid vbid) = 0;

    /**
     * This method will return the purge seqno from the VB KVstore
     *
     * @param vbid - vbucket id to get the purgeSeqno for.
     */
    virtual uint64_t getPurgeSeqno(Vbid vbid) = 0;

    /**
     * Rollback the specified vBucket to the state it had at rollbackseqno.
     *
     * On success, the vBucket should have discarded *at least* back to the
     * specified rollbackseqno; if necessary it is valid to rollback further.
     * A minimal implementation is permitted to rollback to zero.
     *
     * @param vbid VBucket to rollback
     * @param rollbackseqno Sequence number to rollback to (minimum).
     * @param cb For each mutation which has been rolled back (i.e. from the
     * selected rollback point to the latest); invoke this callback with the Key
     * of the now-discarded update. Callers can use this to undo the effect of
     * the discarded updates on their in-memory view.
     * @return success==true and details of the sequence numbers after rollback
     * if rollback succeeded; else false.
     */
    virtual RollbackResult rollback(Vbid vbid,
                                    uint64_t rollbackseqno,
                                    std::unique_ptr<RollbackCB>) = 0;

    /**
     * This method is called after persisting a batch of data to perform any
     * pending tasks on the underlying KVStore instance.
     */
    virtual void pendingTasks() = 0;

    /**
     * Get all_docs API, to return the list of all keys in the store
     * @param vbid vbucket id of which to collect keys from
     * @param start_key key of where to start the scan from once found all keys
     * after this should be returned.
     * @param count the max number of keys that should be collected by kvstore
     * implementation for this vbucket
     * @param cb shared pointer to a callback function
     * @return engine status code
     */
    virtual cb::engine_errc getAllKeys(
            Vbid vbid,
            const DiskDocKey& start_key,
            uint32_t count,
            std::shared_ptr<StatusCallback<const DiskDocKey&>> cb) const = 0;

    /// Does the backend support historical snapshots
    virtual bool supportsHistoricalSnapshots() const = 0;

    /**
     * Create a KVStore seqno range Scan Context with the given options.
     * On success, returns a unique_pointer to the ScanContext. The caller can
     * then call scan() to execute the scan. The scan context is locked
     * to a single version (snapshot) of the database (it does not change
     * while the scan is running). The snapshot may either be "historical"
     * (returns all of the data (from start seqno) up to the oldest snapshot
     * available containing start seqno), or it may be "current" containing
     * all of the data "right now").
     *
     * The caller specifies two callback objects - GetValue and CacheLookup:
     *
     * 1. GetValue callback is invoked for each object loaded from disk, for
     *    the caller to process that item.
     *    If the callback has status cb::engine_errc::success then scanning
     * continues. If the callback has status cb::engine_errc::no_memory then the
     * scan is paused - scan() returns early allowing caller to reduce memory
     * pressure. If scan() is called again it will resume at the _same_ item
     * which returned cb::engine_errc::no_memory last time.
     *
     * 2. CacheLookup callback an an optimization to avoid loading data from
     *    disk for already-resident items - it is invoked _before_ loading the
     *    item's value from disk, to give ep-engine's in-memory cache the
     *    opportunity to fulfill the item (assuming the item is in memory).
     *    If this callback has status cb::engine_errc::key_already_exists then
     * the document is considered to have been handled purely from memory and
     * the GetValue callback is skipped. If this callback has status
     * cb::engine_errc::success then it wasn't fulfilled from memory, and will
     * instead be loaded from disk and GetValue callback invoked.
     *
     * @param cb GetValue callback - ownership passes to the returned object
     * @param cl Cache lookup callback - ownership passes to the returned object
     * @param vbid The vbucket to scan
     * @param startSeqno The seqno to begin scanning from
     * @param options DocumentFilter for the scan - e.g. return deleted items
     * @param valOptions ValueFilter - e.g. return the document body
     * @param source - Should a historical or the current head be used
     * @param fileHandle optional pointer to a KVFileHandle to data store that
     *                   scan context is for. If fileHandle is a nullptr then
     *                   the method will create a new KVFileHandle for the scan
     *                   context.
     * @return a BySeqnoScanContext, null if there's an error
     */
    virtual std::unique_ptr<BySeqnoScanContext> initBySeqnoScanContext(
            std::unique_ptr<StatusCallback<GetValue>> cb,
            std::unique_ptr<StatusCallback<CacheLookup>> cl,
            Vbid vbid,
            uint64_t startSeqno,
            DocumentFilter options,
            ValueFilter valOptions,
            SnapshotSource source,
            std::unique_ptr<KVFileHandle> fileHandle = nullptr) const = 0;

    /**
     * Create a KVStore id range Scan Context with the given options.
     * On success, returns a unique_pointer to the ScanContext. The caller can
     * then call scan() to execute the scan.
     *
     * The caller specifies two callback objects - GetValue and CacheLookup:
     *
     * 1. GetValue callback is invoked for each object loaded from disk, for
     *    the caller to process that item.
     * 2. CacheLookup callback an an optimization to avoid loading data from
     *    disk for already-resident items - it is invoked _before_ loading the
     *    item's value from disk, to give ep-engine's in-memory cache the
     *    opportunity to fulfil the item (assuming the item is in memory).
     *    If this callback has status cb::engine_errc::key_already_exists then
     * the document is considered to have been handled purely from memory and
     * the GetValue callback is skipped. If this callback has status
     * cb::engine_errc::success then it wasn't fulfilled from memory, and will
     * instead be loaded from disk and GetValue callback invoked.
     *
     * @param cb GetValue callback - ownership passes to the returned object
     * @param cl Cache lookup callback - ownership passes to the returned object
     * @param vbid vbucket to scan
     * @param ranges Multiple ranges can be scanned, this param specifies them
     * @param options DocumentFilter for the scan - e.g. return deleted items
     * @param valOptions ValueFilter - e.g. return the document body
     * @param handle optional KVFileHandle to use in the scan. If handle is a
     *        nullptr then the method will create a new KVFileHandle for the
     *        scan context.
     * @returns nullptr for failure or a ByIdScanContext for use with ::scan
     */
    virtual std::unique_ptr<ByIdScanContext> initByIdScanContext(
            std::unique_ptr<StatusCallback<GetValue>> cb,
            std::unique_ptr<StatusCallback<CacheLookup>> cl,
            Vbid vbid,
            const std::vector<ByIdRange>& ranges,
            DocumentFilter options,
            ValueFilter valOptions,
            std::unique_ptr<KVFileHandle> handle = nullptr) const = 0;

    /**
     * Run a BySeqno scan
     * @param sctx non-const reference to the context, internal callbacks may
     *        write to the object as progress is made through the scan
     */
    virtual ScanStatus scan(BySeqnoScanContext& sctx) const = 0;

    /**
     * Run a BySeqno scan that retrieves all versions of a document from the
     * configured "window of history".
     *
     * This function is only valid to call when the KVStore reports
     * StorageProperties::HistoryRetentionAvailable::Yes and that the scan
     * is with the range of retained history as reported by
     * ScanContext::historyStartSeqno
     *
     * @param sctx non-const reference to the context, internal callbacks may
     *        write to the object as progress is made through the scan
     */
    virtual ScanStatus scanAllVersions(BySeqnoScanContext& sctx) const = 0;

    /**
     * Run a ById scan
     * @param sctx non-const reference to the context, internal callbacks may
     *        write to the object as progress is made through the scan
     */
    virtual ScanStatus scan(ByIdScanContext& sctx) const = 0;

    /**
     * Obtain a KVFileHandle which holds the KVStore implementation's handle
     * and provides RAII management of the resource.
     *
     * @param vbid the vbucket to open
     * @return a unique_ptr to a new KVFileHandle object
     */
    virtual std::unique_ptr<KVFileHandle> makeFileHandle(Vbid vbid) const = 0;

    /**
     * Retrieve the stored stats for the given collection, does not error
     * for collection not found as that's a legitimate state (and returns 0 for
     * all stats).
     * @param kvFileHandle a handle into a KV data file
     * @param collection the id of the collection to lookup
     * @return pair of GetCollectionStatsStatus and the stats. The status can
     *         be Success for when stats for the collection were found.
     *         NotFound for when no stats were found and the returned stats are
     *         default initialised. Finally Failed can occur for unexpected
     *         errors.
     */
    enum class GetCollectionStatsStatus {
        Success = 0,
        NotFound = 1,
        Failed = 2
    };
    virtual std::pair<GetCollectionStatsStatus, Collections::VB::PersistedStats>
    getCollectionStats(const KVFileHandle& kvFileHandle,
                       CollectionID collection) const = 0;

    /**
     * Retrieve the stored stats for the given collection, does not error
     * for collection not found as that's a legitimate state (and returns 0 for
     * all stats).
     * @param vbid of the vbucket to get collections stats from
     * @param collection the id of the collection to lookup
     * @return pair of GetCollectionStatsStatus and the stats. The status can
     *         be Success for when stats for the collection were found.
     *         NotFound for when no stats were found and the returned stats are
     *         default initialised. Finally Failed can occur for unexpected
     *         errors.
     */
    virtual std::pair<GetCollectionStatsStatus, Collections::VB::PersistedStats>
    getCollectionStats(Vbid vbid, CollectionID collection) const = 0;

    /**
     * Note that for disk snapshots that have no manifest yet written, a uid
     * of zero is returned (the default state).
     * @return The ManifestUid or nothing if failure occurred
     */
    virtual std::optional<Collections::ManifestUid> getCollectionsManifestUid(
            KVFileHandle& kvFileHandle) const = 0;

    /**
     * Return data that EPBucket requires for the creation of a
     * Collections::VB::Manifest
     *
     * @param vbid vbucket to get data from
     * @return pair of bool status and the persisted manifest data for the given
     *         vbid
     */
    virtual std::pair<bool, Collections::KVStore::Manifest>
    getCollectionsManifest(Vbid vbid) const = 0;

    /**
     * Return all collections that are dropped, i.e. not open but still exist
     * The implementation of this method can return empty vector if the
     * underlying KV store atomically drops collections
     *
     * @param vbid vbucket to get data from
     * @return pair of bool status and vector of collections that are dropped
     *         but still may have data
     */
    virtual std::pair<bool,
                      std::vector<Collections::KVStore::DroppedCollection>>
    getDroppedCollections(Vbid vbid) const = 0;

    /**
     * Get the configuration class from the derived class. We have derived class
     * specific config so we want to store the derived class specific config in
     * the derived classes to save from having to dynamic cast all over the
     * place, but, we have common code here that needs the common config too.
     *
     * @return Non derived class specific config
     */
    virtual const KVStoreConfig& getConfig() const = 0;

    /**
     * Get an item from the KVStore using a seqno for lookup
     *
     * @param handle the KVFileHandle for an open file
     * @param vbucket of the get
     * @param seq the seqno to look for
     * @param filter ValueFilter for key+meta or key+meta+value lookup
     */
    virtual GetValue getBySeqno(KVFileHandle& handle,
                                Vbid vbid,
                                uint64_t seq,
                                ValueFilter filter) const = 0;

    /**
     * Set the number of storage threads based on configuration settings
     */
    virtual void setStorageThreads(
            ThreadPoolConfig::StorageThreadCount num) = 0;

    /**
     * End a transaction by resetting the appropriate state. Public so that we
     * can call this from ~TransactionContext().
     *
     * @param vbid to end a transaction for
     */
    virtual void endTransaction(Vbid vbid) = 0;

    /**
     * Construct a transaction context applicable to the given KVStore and
     * return it to the caller
     *
     * @param vbid vBucket to operate on
     * @param pcb PersistenceCallback object (default constructed if not given)
     * @return TransactionContext
     */
    virtual std::unique_ptr<TransactionContext> begin(
            Vbid vbid,
            std::unique_ptr<PersistenceCallback> pcb =
                    std::make_unique<PersistenceCallback>()) = 0;

    virtual const KVStoreStats& getKVStoreStat() const = 0;

    virtual void setMakeCompactionContextCallback(
            MakeCompactionContextCallback cb) = 0;

    /**
     * Test-only. See definition of postFlushHook for details.
     */
    virtual void setPreFlushHook(std::function<void()> hook) = 0;

    /**
     * Test-only. See definition of postFlushHook for details.
     */
    virtual void setPostFlushHook(std::function<void()> hook) = 0;

    virtual void setSaveDocsPostWriteDocsHook(std::function<void()> hook) = 0;

    /**
     * Get json of persisted engine and DCP stats. This API is invoked during
     * warmup to get the engine stats from the previous session.
     *
     * @return stats nlohmann::json object of the engine stats from the previous
     * session is stored. If the function fails and empty nlohmann::json will be
     * returned
     */
    virtual nlohmann::json getPersistedStats() const = 0;

    /**
     * Persist a snapshot of a collection of stats.
     */
    virtual bool snapshotStats(const nlohmann::json& stats) = 0;

    /**
     * Prepare for rollback of the vbucket
     *
     * @param vbid ID of the vbucket about to be rolled back
     *
     * @return context object for this rollback
     */
    virtual std::unique_ptr<RollbackCtx> prepareToRollback(Vbid vbid) = 0;

    /**
     * Prepare for create of the vbucket
     * @param vbid ID of the vbucket about to be created
     */
    virtual void prepareToCreate(Vbid vbid) = 0;

    virtual bool keyMayExist(Vbid vb, const DocKeyView& key) const = 0;

    /**
     * Prepare for delete of the vbucket file
     *
     * @param vbid ID of the vbucket being deleted
     * @return the revision ID to delete (via ::delVBucket)
     */
    virtual std::unique_ptr<KVStoreRevision> prepareToDelete(Vbid vbid) = 0;

    virtual uint64_t getLastPersistedSeqno(Vbid vbid) = 0;

    /**
     * This method is called before persisting a batch of data to sort the data
     * by key and seqno so that we can de-duplicate the writes passed to the
     * KVStore if it cannot de-duplicate them itself.
     */
    virtual void prepareForDeduplication(std::vector<queued_item>& items) = 0;

    /**
     * Set a system event into the KVStore.
     * @param item The Item representing the event
     */
    virtual void setSystemEvent(TransactionContext& txnCtx,
                                const queued_item) = 0;

    /**
     * delete a system event in the KVStore.
     * @param item The Item representing the event
     */
    virtual void delSystemEvent(TransactionContext& txnCtx,
                                const queued_item) = 0;

    /**
     * Prepare for delete of the vbucket file - Implementation specific method
     * that is called by prepareToDelete
     *
     * @param vbid ID of the vbucket being deleted
     * @return the revision ID to delete (via ::delVBucket)
     */
    virtual std::unique_ptr<KVStoreRevision> prepareToDeleteImpl(Vbid vbid) = 0;

    /*
     * Prepare for a creation of the vbucket file - Implementation specific
     * method that is called by prepareToCreate
     *
     * @param vbid ID of the vbucket being created
     */
    virtual void prepareToCreateImpl(Vbid vbid) = 0;

    /**
     * Method to configure the amount of history a vbucket should retain.
     * The given size comes from the cluster and is the total bucket size.
     * The KVStore may need to calculate a per vbucket size using nVbuckets.
     */
    virtual void setHistoryRetentionBytes(size_t size, size_t nVbuckets) = 0;

    /**
     * Method to configure the history a vbucket should retain, by age.
     */
    virtual void setHistoryRetentionSeconds(std::chrono::seconds secs) = 0;

    virtual std::optional<uint64_t> getHistoryStartSeqno(Vbid vbid) = 0;

    /// Fusion API, supported only by MagmaKVStore

    virtual nlohmann::json getFusionStats(FusionStat stat, Vbid vbid) = 0;
    virtual std::pair<cb::engine_errc, nlohmann::json> getFusionStorageSnapshot(
            std::string_view fusionNamespace,
            Vbid vbid,
            std::string_view snapshotUuid,
            std::time_t validity) = 0;
    virtual cb::engine_errc releaseFusionStorageSnapshot(
            std::string_view fusionNamespace,
            Vbid vbid,
            std::string_view snapshotUuid) = 0;
    virtual cb::engine_errc setFusionMetadataAuthToken(
            std::string_view token) = 0;
    virtual std::string getFusionMetadataAuthToken() const = 0;
};

std::string to_string(KVStoreIface::ReadVBStateStatus status);

std::ostream& operator<<(std::ostream&,
                         const KVStoreIface::GetCollectionStatsStatus&);
std::string format_as(const KVStoreIface::GetCollectionStatsStatus&);
