/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#include "config.h"

#include "callbacks.h"
#include "collections/eraser_context.h"
#include "collections/scan_context.h"
#include "storeddockey.h"

#include <memcached/engine_common.h>
#include <platform/histogram.h>

#include <relaxed_atomic.h>
#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
#include <list>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

/* Forward declarations */
class BucketLogger;
class Item;
class KVStore;
class KVStoreConfig;
class PersistenceCallback;
class RollbackCB;
class RollbackResult;

namespace cb {
namespace mcbp {
class Request;
}
} // namespace cb

namespace Collections {
namespace VB {
struct PersistedStats;
class Flush;
} // namespace VB
} // namespace Collections

struct vb_bgfetch_item_ctx_t;
union protocol_binary_request_compact_db;

using vb_bgfetch_queue_t =
        std::unordered_map<StoredDocKey, vb_bgfetch_item_ctx_t>;

enum class GetMetaOnly { Yes, No };

typedef std::shared_ptr<Callback<Vbid&, const DocKey&, bool&>> BloomFilterCBPtr;
typedef std::shared_ptr<Callback<Item&, time_t&> > ExpiredItemsCBPtr;

/**
 * Generic information about a KVStore file
 */
struct FileInfo {
    FileInfo() = default;

    FileInfo(uint64_t items,
             uint64_t deletedItems,
             uint64_t size,
             uint64_t purgeSeqno)
        : items(items),
          deletedItems(deletedItems),
          size(size),
          purgeSeqno(purgeSeqno) {
    }
    /// The number of items stored
    uint64_t items = 0;

    /// The number of deleted item stored
    uint64_t deletedItems = 0;

    /// The size on disk of the KVStore file
    uint64_t size = 0;

    /// Last purge sequence number
    uint64_t purgeSeqno = 0;
};

struct CompactionStats {
    size_t collectionsItemsPurged = 0;
    size_t collectionsDeletedItemsPurged = 0;
    uint64_t tombstonesPurged = 0;
    FileInfo pre;
    FileInfo post;
};

struct CompactionConfig {
    uint64_t purge_before_ts = 0;
    uint64_t purge_before_seq = 0;
    uint8_t drop_deletes = 0;
    Vbid db_file_id = Vbid(0);
    uint64_t purgeSeq = 0;
    bool retain_erroneous_tombstones = false;
};

struct compaction_ctx {
    compaction_ctx(const CompactionConfig& config, uint64_t purgeSeq)
        : compactConfig(config), max_purged_seq(purgeSeq) {
    }

    CompactionConfig compactConfig;
    uint64_t max_purged_seq;
    const KVStoreConfig* config;
    uint32_t curr_time;
    BloomFilterCBPtr bloomFilterCallback;
    ExpiredItemsCBPtr expiryCallback;
    struct CompactionStats stats;
    /// pointer as context cannot be constructed until deeper inside storage
    std::unique_ptr<Collections::VB::EraserContext> eraserContext;
    Collections::IsDroppedCb collectionsEraser;
};

/**
 * State associated with a KVStore transaction (begin() / commit() pair).
 * Users would typically subclass this, and provide an instance to begin().
 * The KVStore will then provide a pointer to it during every persistenca
 * callback.
 */
struct TransactionContext {
    virtual ~TransactionContext(){};
};

/**
 * Result of database mutation operations.
 *
 * This is a pair where .first is the number of rows affected, and
 * .second is true if it is an insertion.
 *
 * .first will be -1 if there was an error performing the update.
 *
 * .first will be 0 if the update did not error, but did not occur.
 * This would generally be considered a fatal condition (in practice,
 * it requires you to be firing an update at a missing rowid).
 */
typedef std::pair<int, bool> mutation_result;

class NoLookupCallback : public StatusCallback<CacheLookup> {
public:
    NoLookupCallback() {}
    ~NoLookupCallback() {}
    void callback(CacheLookup&) {}
};

struct DBFileInfo {
    DBFileInfo() :
        fileSize(0), spaceUsed(0) { }

    DBFileInfo(uint64_t fileSize_, uint64_t spaceUsed_)
        : fileSize(fileSize_), spaceUsed(spaceUsed_) {}

    uint64_t fileSize;
    uint64_t spaceUsed;
};

enum scan_error_t {
    scan_success,
    scan_again,
    scan_failed
};

enum class DocumentFilter {
    ALL_ITEMS,
    NO_DELETES,
    ALL_ITEMS_AND_DROPPED_COLLECTIONS
};

enum class ValueFilter {
    KEYS_ONLY,
    VALUES_COMPRESSED,
    VALUES_DECOMPRESSED
};

enum class VBStatePersist {
    VBSTATE_CACHE_UPDATE_ONLY,       //Update only cached state in-memory
    VBSTATE_PERSIST_WITHOUT_COMMIT,  //Persist without committing to disk
    VBSTATE_PERSIST_WITH_COMMIT      //Persist with commit to disk
};

class ScanContext {
public:
    ScanContext(std::shared_ptr<StatusCallback<GetValue>> cb,
                std::shared_ptr<StatusCallback<CacheLookup>> cl,
                Vbid vb,
                size_t id,
                int64_t start,
                int64_t end,
                uint64_t purgeSeqno,
                DocumentFilter _docFilter,
                ValueFilter _valFilter,
                uint64_t _documentCount,
                const KVStoreConfig& _config,
                const Collections::VB::PersistedManifest& manifestData);

    const std::shared_ptr<StatusCallback<GetValue>> callback;
    const std::shared_ptr<StatusCallback<CacheLookup>> lookup;

    int64_t lastReadSeqno;
    const int64_t startSeqno;
    const int64_t maxSeqno;
    const uint64_t purgeSeqno;
    const size_t scanId;
    const Vbid vbid;
    const DocumentFilter docFilter;
    const ValueFilter valFilter;
    const uint64_t documentCount;

    BucketLogger* logger;
    const KVStoreConfig& config;
    Collections::VB::ScanContext collectionsContext;
};

struct FileStats {
    // Read time length
    MicrosecondHistogram readTimeHisto;
    // Distance from last read
    Histogram<size_t> readSeekHisto = {ExponentialGenerator<size_t>(1, 2), 50};
    // Size of read
    Histogram<size_t> readSizeHisto = {ExponentialGenerator<size_t>(1, 2), 25};
    // Write time length
    MicrosecondHistogram writeTimeHisto;
    // Write size
    Histogram<size_t> writeSizeHisto = {ExponentialGenerator<size_t>(1, 2), 25};
    // Time spent in sync
    MicrosecondHistogram syncTimeHisto;
    // Read count per open() / close() pair
    Histogram<uint32_t> readCountHisto = {
            ExponentialGenerator<uint32_t>(2, 1.333), 50};
    // Write count per open() / close() pair
    Histogram<uint32_t> writeCountHisto = {
            ExponentialGenerator<uint32_t>(2, 1.333), 50};

    // total bytes read from disk.
    std::atomic<size_t> totalBytesRead{0};
    // Total bytes written to disk.
    std::atomic<size_t> totalBytesWritten{0};

    void reset();
};

/**
 * Stats and timings for KVStore
 */
class KVStoreStats {

public:
    /**
     * Default constructor
     */
    KVStoreStats() :
      docsCommitted(0),
      numOpen(0),
      numClose(0),
      numLoadedVb(0),
      numCompactionFailure(0),
      numGetFailure(0),
      numSetFailure(0),
      numDelFailure(0),
      numOpenFailure(0),
      numVbSetFailure(0),
      io_bg_fetch_docs_read(0),
      io_num_write(0),
      io_bgfetch_doc_bytes(0),
      io_write_bytes(0),
      readSizeHisto(ExponentialGenerator<size_t>(1, 2), 25),
      writeSizeHisto(ExponentialGenerator<size_t>(1, 2), 25),
      getMultiFsReadCount(0),
      getMultiFsReadHisto(ExponentialGenerator<uint32_t>(6, 1.2), 50),
      getMultiFsReadPerDocHisto(ExponentialGenerator<uint32_t>(6, 1.2),50) {
    }

    KVStoreStats(const KVStoreStats &copyFrom) {}

    void reset() {
        docsCommitted = 0;
        numOpen = 0;
        numClose = 0;
        numLoadedVb = 0;
        numCompactionFailure = 0;
        numGetFailure = 0;
        numSetFailure = 0;
        numDelFailure = 0;
        numOpenFailure = 0;
        numVbSetFailure = 0;

        readTimeHisto.reset();
        readSizeHisto.reset();
        writeTimeHisto.reset();
        writeSizeHisto.reset();
        delTimeHisto.reset();
        compactHisto.reset();
        snapshotHisto.reset();
        commitHisto.reset();
        saveDocsHisto.reset();
        batchSize.reset();
        getMultiFsReadCount = 0;
        getMultiFsReadHisto.reset();
        getMultiFsReadPerDocHisto.reset();
        fsStats.reset();
    }

    // the number of docs committed
    Couchbase::RelaxedAtomic<size_t> docsCommitted;
    // the number of open() calls
    Couchbase::RelaxedAtomic<size_t> numOpen;
    // the number of close() calls
    Couchbase::RelaxedAtomic<size_t> numClose;
    // the number of vbuckets loaded
    Couchbase::RelaxedAtomic<size_t> numLoadedVb;

    //stats tracking failures
    Couchbase::RelaxedAtomic<size_t> numCompactionFailure;
    Couchbase::RelaxedAtomic<size_t> numGetFailure;
    Couchbase::RelaxedAtomic<size_t> numSetFailure;
    Couchbase::RelaxedAtomic<size_t> numDelFailure;
    Couchbase::RelaxedAtomic<size_t> numOpenFailure;
    Couchbase::RelaxedAtomic<size_t> numVbSetFailure;

    /**
     * Number of documents read (full and meta-only) from disk for background
     * fetch operations.
     */
    Couchbase::RelaxedAtomic<size_t> io_bg_fetch_docs_read;
    //! Number of write related io operations
    Couchbase::RelaxedAtomic<size_t> io_num_write;
    //! Document bytes (key+meta+value) read for background fetch operations.
    Couchbase::RelaxedAtomic<size_t> io_bgfetch_doc_bytes;
    //! Number of bytes written (key + value + application rev metadata)
    Couchbase::RelaxedAtomic<size_t> io_write_bytes;

    /* for flush and vb delete, no error handling in KVStore, such
     * failure should be tracked in MC-engine  */

    // How long it takes us to complete a read
    MicrosecondHistogram readTimeHisto;
    // How big are our reads?
    Histogram<size_t> readSizeHisto;
    // How long it takes us to complete a write
    MicrosecondHistogram writeTimeHisto;
    // How big are our writes?
    Histogram<size_t> writeSizeHisto;
    // Time spent in delete() calls.
    MicrosecondHistogram delTimeHisto;
    // Time spent in commit
    MicrosecondHistogram commitHisto;
    // Time spent in compaction
    MicrosecondHistogram compactHisto;
    // Time spent in saving documents to disk
    MicrosecondHistogram saveDocsHisto;
    // Batch size while saving documents
    Histogram<size_t> batchSize;
    //Time spent in vbucket snapshot
    MicrosecondHistogram snapshotHisto;

    // Count and histogram filesystem read()s per getMulti() request
    Couchbase::RelaxedAtomic<size_t> getMultiFsReadCount;
    Histogram<uint32_t> getMultiFsReadHisto;

    // Histogram of filesystem read()s per getMulti() request, divided by
    // the number of documents fetched; gives an average read() count
    // per fetched document.
    Histogram<uint32_t> getMultiFsReadPerDocHisto;

    // Stats from the underlying OS file operations
    FileStats fsStats;

    // Underlying stats for OS file operations during compaction
    FileStats fsStatsCompaction;
};

/**
 * Type of vbucket map.
 *
 * key is the vbucket identifier.
 * value is a pair of string representation of the vbucket state and
 * its latest checkpoint Id persisted.
 */
struct vbucket_state;
typedef std::map<Vbid, vbucket_state> vbucket_map_t;

/**
 * Properties of the storage layer.
 *
 * If concurrent filesystem access is possible, maxConcurrency() will
 * be greater than one.  One will need to determine whether more than
 * one writer is possible as well as whether more than one reader is
 * possible.
 */
class StorageProperties {
public:

    enum class EfficientVBDump {
        Yes,
        No
    };

    enum class EfficientVBDeletion {
        Yes,
        No
    };

    enum class PersistedDeletion {
        Yes,
        No
    };

    enum class EfficientGet {
        Yes,
        No
    };

    enum class ConcurrentWriteCompact {
        Yes,
        No
    };

    StorageProperties(EfficientVBDump evb, EfficientVBDeletion evd, PersistedDeletion pd,
                      EfficientGet eget, ConcurrentWriteCompact cwc)
        : efficientVBDump(evb), efficientVBDeletion(evd),
          persistedDeletions(pd), efficientGet(eget),
          concWriteCompact(cwc) {}

    /* True if we can efficiently dump a single vbucket */
    bool hasEfficientVBDump() const {
        return (efficientVBDump == EfficientVBDump::Yes);
    }

    /* True if we can efficiently delete a vbucket all at once */
    bool hasEfficientVBDeletion() const {
        return (efficientVBDeletion == EfficientVBDeletion::Yes);
    }

    /* True if we can persist deletions to disk */
    bool hasPersistedDeletions() const {
        return (persistedDeletions == PersistedDeletion::Yes);
    }

    /* True if we can batch-process multiple get operations at once */
    bool hasEfficientGet() const {
        return (efficientGet == EfficientGet::Yes);
    }

    /* True if the underlying storage supports concurrent writing
     * and compacting */
    bool hasConcWriteCompact() const {
        return (concWriteCompact == ConcurrentWriteCompact::Yes);
    }

private:
    EfficientVBDump efficientVBDump;
    EfficientVBDeletion efficientVBDeletion;
    PersistedDeletion persistedDeletions;
    EfficientGet efficientGet;
    ConcurrentWriteCompact concWriteCompact;
};

/**
 * Abstract file handle class to allow a DB file to be opened and held open
 * for multiple KVStore methods.
 */
class KVFileHandle {
public:
    KVFileHandle(const KVStore& kvs) : kvs(kvs) {
    }
    virtual ~KVFileHandle() {
    }
    const KVStore& kvs;
};

struct KVFileHandleDeleter {
    void operator()(KVFileHandle* kvFileHandle);
};

/**
 * Base class representing kvstore operations.
 */
class KVStore {
public:
    /// Ordered container of persistence callbacks currently registered.
    using PersistenceCallbacks =
            std::deque<std::unique_ptr<PersistenceCallback>>;

    KVStore(KVStoreConfig& config, bool read_only = false);

    virtual ~KVStore();

    /**
     * Allow the kvstore to add extra statistics information
     * back to the client
     * @param prefix prefix to use for the stats
     * @param add_stat the callback function to add statistics
     * @param c the cookie to pass to the callback function
     */
    void addStats(const AddStatFn& add_stat, const void* c);

    /**
     * Request the specified statistic name from the kvstore.
     *
     * @param name The name of the statistic to fetch.
     * @param[out] value Value of the given stat (if exists).
     * @return True if the stat exists, is of type size_t and was successfully
     *         returned, else false.
     */
    virtual bool getStat(const char* name, size_t& value) {
        return false;
    }

    /**
     * Show kvstore specific timing stats.
     *
     * @param add_stat the callback function to add statistics
     * @param c the cookie to pass to the callback function
     */
    virtual void addTimingStats(const AddStatFn& add_stat, const void* c);

    /**
     * Resets kvstore specific stats
     */
    void resetStats() {
        st.reset();
    }

    /**
     * Reset the vbucket to a clean state.
     */
    virtual void reset(Vbid vbid) = 0;

    /**
     * Begin a transaction (if not already in one).
     *
     * @param txCtx A transaction context to associate with this transaction.
     *        The context will be passed to each operations' completion
     *        callback, so this can be used to hold state common to the entire
     *        transaction without having to duplicate it in every Callback.
     *
     * @return false if we cannot begin a transaction
     */
    virtual bool begin(std::unique_ptr<TransactionContext> txCtx) = 0;

    /**
     * Commit a transaction (unless not currently in one).
     *
     * @param collectionsFlush a reference to a Collections::VB::Flush object
     *        which is required for persisted metadata updates and item counting
     * @return false if the commit fails
     */
    virtual bool commit(Collections::VB::Flush& collectionsFlush) = 0;

    /**
     * Rollback the current transaction.
     */
    virtual void rollback() = 0;

    /**
     * Get the properties of the underlying storage.
     */
    virtual StorageProperties getStorageProperties() = 0;

    /**
     * Set an item into the kv store.
     *
     * @param item The item to store
     * @param cb Pointer to a callback object which will be invoked when the
     *           set() has been persisted to disk.
     */
    virtual void set(const Item& item,
                     Callback<TransactionContext, mutation_result>& cb) = 0;

    /**
     * Get an item from the kv store.
     */
    virtual GetValue get(const StoredDocKey& key,
                         Vbid vb,
                         bool fetchDelete = false) = 0;

    virtual GetValue getWithHeader(void* dbHandle,
                                   const StoredDocKey& key,
                                   Vbid vb,
                                   GetMetaOnly getMetaOnly,
                                   bool fetchDelete = false) = 0;
    /**
     * Get multiple items if supported by the kv store
     */
    virtual void getMulti(Vbid vb, vb_bgfetch_queue_t& itms) {
        throw std::runtime_error("Backend does not support getMulti()");
    }

    /**
     * Get the number of vbuckets in a single database file
     *
     * returns - the number of vbuckets in the file
     */
    virtual uint16_t getNumVbsPerFile(void) = 0;

    /**
     * Delete an item from the kv store.
     *
     * @param item The item to delete
     * @param cb Pointer to a callback object which will be invoked when the
     *           del() has been persisted to disk.
     */
    virtual void del(const Item& itm,
                     Callback<TransactionContext, int>& cb) = 0;

    /**
     * Delete a given vbucket database instance from underlying storage
     *
     * @param vbucket vbucket id
     * @param fileRev the revision of the file to delete
     */
    virtual void delVBucket(Vbid vbucket, uint64_t fileRev) = 0;

    /**
     * Get a list of all persisted vbuckets (with their states).
     */
    virtual std::vector<vbucket_state *> listPersistedVbuckets(void) = 0;


    /**
     * Get a list of all persisted engine and DCP stats. This API is mainly
     * invoked during warmup to get the engine stats from the previous session.
     *
     * @param stats map instance where the engine stats from the previous
     * session is stored.
     */
    virtual void getPersistedStats(std::map<std::string, std::string> &stats) {
        (void) stats;
    }

    /**
     * Persist a snapshot of a collection of stats.
     */
    bool snapshotStats(const std::map<std::string, std::string> &m);

    /**
     * Snapshot vbucket state
     * @param vbucketId id of the vbucket that needs to be snapshotted
     * @param vbstate   state of the vbucket
     * @param cb        stats callback
     * @param options   options for persisting the state
     */
    virtual bool snapshotVBucket(Vbid vbucketId,
                                 const vbucket_state& vbstate,
                                 VBStatePersist options) = 0;

    /**
     * Compact a database file.
     */
    virtual bool compactDB(compaction_ctx *c) = 0;

    /**
     * Return the database file id from the compaction request
     * @param compact_req request structure for compaction
     *
     * return database file id
     */
    virtual Vbid getDBFileId(const cb::mcbp::Request& req) = 0;

    virtual vbucket_state* getVBucketState(Vbid vbid) = 0;

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
                                    std::shared_ptr<RollbackCB> cb) = 0;

    /**
     * This method is called before persisting a batch of data if you'd like to
     * do stuff to them that might improve performance at the IO layer.
     */
    void optimizeWrites(std::vector<queued_item>& items);

    PersistenceCallbacks& getPersistenceCbList() {
        return pcbs;
    }

    /**
     * This method is called after persisting a batch of data to perform any
     * pending tasks on the underlying KVStore instance.
     */
    virtual void pendingTasks() = 0;

    uint64_t getLastPersistedSeqno(Vbid vbid);

    bool isReadOnly(void) {
        return readOnly;
    }

    KVStoreConfig& getConfig(void) {
        return configuration;
    }

    KVStoreStats& getKVStoreStat(void) {
        return st;
    }

    virtual ENGINE_ERROR_CODE getAllKeys(
            Vbid vbid,
            const DocKey start_key,
            uint32_t count,
            std::shared_ptr<Callback<const DocKey&>> cb) = 0;

    /**
     * Create a KVStore Scan Context with the given options. On success,
     * returns a pointer to the ScanContext. The caller can then call scan()
     * to execute the scan. The context should be deleted by the caller using
     * destroyScanContext() when finished with.
     *
     * If the ScanContext cannot be created, returns null.
     */
    virtual ScanContext* initScanContext(
            std::shared_ptr<StatusCallback<GetValue>> cb,
            std::shared_ptr<StatusCallback<CacheLookup>> cl,
            Vbid vbid,
            uint64_t startSeqno,
            DocumentFilter options,
            ValueFilter valOptions) = 0;

    virtual scan_error_t scan(ScanContext* sctx) = 0;

    virtual void destroyScanContext(ScanContext* ctx) = 0;

    /**
     * KVStore must implement this method which should read and return the
     * collection manifest data as a std::string (data written by
     * persistCollectionsManifestItem)
     */
    virtual Collections::VB::PersistedManifest getCollectionsManifest(
            Vbid vbid) = 0;

    /**
     * Obtain a KVFileHandle which holds the KVStore implementation's handle
     * and provides RAII management of the resource.
     *
     * @param vbid the vbucket to open
     * @return a unique_ptr to a new KVFileHandle object
     */
    virtual std::unique_ptr<KVFileHandle, KVFileHandleDeleter> makeFileHandle(
            Vbid vbid) = 0;

    /**
     * Free KVFileHandle - KVStore to override and release resources allocated
     * by makeFileHandle.
     */
    virtual void freeFileHandle(KVFileHandle* kvFileHandle) const = 0;

    /**
     * Retrieve the stored item count for the given collection, does not error
     * for collection not found as that's a legitimate state (and returns 0)
     * @param kvFileHandle a handle into a KV data file
     * @param collection the id of the collection to lookup
     * @return the persisted stats for the collection
     */
    virtual Collections::VB::PersistedStats getCollectionStats(
            const KVFileHandle& kvFileHandle, CollectionID collection) = 0;

    /**
     * Increment the revision number of the vbucket.
     * @param vbid ID of the vbucket to change.
     */
    virtual void incrementRevision(Vbid vbid) = 0;

    /**
     * Prepare for delete of the vbucket file
     *
     * @param vbid ID of the vbucket being deleted
     * @return the revision ID to delete (via ::delVBucket)
     */
    virtual uint64_t prepareToDelete(Vbid vbid) = 0;

    /**
     * Set a system event into the KVStore.
     * Collection system events will be used to maintain extra meta-data before
     * writing to disk.
     * @param item The Item representing the event
     * @param cb a callback object which is called once persisted
     */
    void setSystemEvent(const Item& item,
                        Callback<TransactionContext, mutation_result>& cb);
    /**
     * delete a system event in the KVStore.
     * Collection system events will be used to maintain extra meta-data before
     * writing to disk.
     * @param item The Item representing the event
     * @param cb a callback object which is called once persisted
     */
    void delSystemEvent(const Item& item,
                        Callback<TransactionContext, int>& cb);

protected:

    /* all stats */
    KVStoreStats st;
    KVStoreConfig& configuration;
    bool readOnly;
    std::vector<std::unique_ptr<vbucket_state>> cachedVBStates;
    /* non-deleted docs in each file, indexed by vBucket.
       RelaxedAtomic to allow stats access without lock. */
    std::vector<Couchbase::RelaxedAtomic<size_t>> cachedDocCount;
    Couchbase::RelaxedAtomic<uint16_t> cachedValidVBCount;

    PersistenceCallbacks pcbs;

    void createDataDir(const std::string& dbname);
    template <typename T>
    void addStat(const std::string& prefix,
                 const char* nm,
                 T& val,
                 const AddStatFn& add_stat,
                 const void* c);

    /**
     * Updates the cached state for a vbucket
     *
     * @param vbid the vbucket id
     * @param vbState the new state information for the vbucket
     *
     * @return true if the cached vbucket state is updated
     */
    bool updateCachedVBState(Vbid vbid, const vbucket_state& vbState);
};

/**
 * Structure holding the read/write and read only instances of the KVStore.
 * They could be the same underlying object, or different.
 */
struct KVStoreRWRO {
    KVStoreRWRO() /*rw/ro default init is ok*/ {
    }
    KVStoreRWRO(KVStore* rw, KVStore* ro) : rw(rw), ro(ro) {
    }

    std::unique_ptr<KVStore> rw;
    std::unique_ptr<KVStore> ro;
};

/**
 * The KVStoreFactory creates the correct KVStore instance(s) when
 * needed by EPStore.
 */
class KVStoreFactory {
public:
    /**
     * Create a KVStore using the type found in the config
     *
     * @param config engine configuration
     */
    static KVStoreRWRO create(KVStoreConfig& config);
};

/**
 * Callback class used by DcpConsumer, for rollback operation
 */
class RollbackCB : public StatusCallback<GetValue> {
public:
    RollbackCB() : dbHandle(NULL) { }

    virtual void callback(GetValue &val) = 0;

    void setDbHeader(void *db) {
        dbHandle = db;
    }

protected:
    /// The database handle to use when lookup up items in the new, rolled back
    /// database.
    void *dbHandle;
};
