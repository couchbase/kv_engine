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

#include "callbacks.h"
#include "collections/eraser_context.h"
#include "collections/kvstore.h"

#include <memcached/engine_common.h>
#include <utilities/hdrhistogram.h>

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
class DiskDocKey;
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

namespace VB {
class Commit;
} // namespace VB

namespace Collections {
namespace VB {
struct PersistedStats;
} // namespace VB
} // namespace Collections

struct vb_bgfetch_item_ctx_t;
struct TransactionContext;
union protocol_binary_request_compact_db;

using vb_bgfetch_queue_t =
        std::unordered_map<DiskDocKey, vb_bgfetch_item_ctx_t>;

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
    uint64_t preparesPurged = 0;
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
    BloomFilterCBPtr bloomFilterCallback;
    ExpiredItemsCBPtr expiryCallback;
    struct CompactionStats stats;
    /// pointer as context cannot be constructed until deeper inside storage
    std::unique_ptr<Collections::VB::EraserContext> eraserContext;
    Collections::KVStore::DroppedCb droppedKeyCb;

    /// The SyncRepl HCS, can purge any prepares before the HCS.
    uint64_t highCompletedSeqno = 0;
};

struct kvstats_ctx {
    kvstats_ctx(VB::Commit& commitData) : commitData(commitData) {
    }
    // @TODO consider folly::F14Set for reduced memory when set is large
    /// If key exists in set, they key exists in the VB datafile
    std::unordered_set<DiskDocKey> keyStats;
    /// flusher data for managing manifest changes, item counts, vbstate
    VB::Commit& commitData;

    /**
     * Delta of onDiskPrepares that we should add to the value tracked in
     * the persisted VB state before commit
     */
    size_t onDiskPrepareDelta = 0;
};

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

struct vbucket_state;

/**
 * Abstract file handle class to allow a DB file to be opened and held open
 * for multiple KVStore methods.
 */
class KVFileHandle {
public:
    KVFileHandle() {
    }
    virtual ~KVFileHandle() {
    }
};

class ScanContext {
public:
    ScanContext(std::shared_ptr<StatusCallback<GetValue>> cb,
                std::shared_ptr<StatusCallback<CacheLookup>> cl,
                Vbid vb,
                std::unique_ptr<KVFileHandle> handle,
                int64_t start,
                int64_t end,
                uint64_t purgeSeqno,
                DocumentFilter _docFilter,
                ValueFilter _valFilter,
                uint64_t _documentCount,
                const vbucket_state& vbucketState,
                const KVStoreConfig& _config,
                const std::vector<Collections::KVStore::DroppedCollection>&
                        droppedCollections);

    const std::shared_ptr<StatusCallback<GetValue>> callback;
    const std::shared_ptr<StatusCallback<CacheLookup>> lookup;

    int64_t lastReadSeqno;
    const int64_t startSeqno;
    const int64_t maxSeqno;
    const uint64_t purgeSeqno;
    const std::unique_ptr<KVFileHandle> handle;
    const Vbid vbid;
    const DocumentFilter docFilter;
    const ValueFilter valFilter;
    const uint64_t documentCount;

    /**
     * The highest seqno of a mutation or commit on disk. Used for backfill
     * for non sync-write aware connections as the snapshot end to ensure the
     * snapshot end matches the last item sent (aborts and prepares are skipped
     * for such connections).
     */
    const uint64_t maxVisibleSeqno;
    /**
     * The on disk "High Completed Seqno". This number changes in different ways
     * when compared to the one in memory so has been named differently. The
     * seqno will be read from disk and sent to a replica in a snapshot marker
     * so that we can optimise warmup after having received a disk snapshot.
     * This is necessary due to de-duplication as a replica will see logical
     * commits out of order. It cannot update the HCS value reliably with the
     * information received and perform the warmup optimisation so the active
     * node will send a persistedCompletedSeqno value which it will write at the
     * end of the snapshot. This seqno is also used to optimise local warmup.
     */
    const uint64_t persistedCompletedSeqno;

    BucketLogger* logger;
    const KVStoreConfig& config;
    Collections::VB::ScanContext collectionsContext;
};

struct FileStats {
    FileStats() = default;

    // Read time length
    Hdr1sfMicroSecHistogram readTimeHisto;
    // Distance from last read
    Hdr1sfInt32Histogram readSeekHisto;
    // Size of read
    Hdr1sfInt32Histogram readSizeHisto;
    // Write time length
    Hdr1sfMicroSecHistogram writeTimeHisto;
    // Write size
    Hdr1sfInt32Histogram writeSizeHisto;
    // Time spent in sync
    Hdr1sfMicroSecHistogram syncTimeHisto;
    // Read count per open() / close() pair
    Hdr1sfInt32Histogram readCountHisto;
    // Write count per open() / close() pair
    Hdr1sfInt32Histogram writeCountHisto;

    // total bytes read from disk.
    cb::RelaxedAtomic<size_t> totalBytesRead{0};
    // Total bytes written to disk.
    cb::RelaxedAtomic<size_t> totalBytesWritten{0};

    size_t getMemFootPrint() const;

    void reset();
};

/**
 * Stats and timings for KVStore
 */
class KVStoreStats {

public:
    KVStoreStats();

    /// Resets all statistics to their initial vaule.
    void reset();

    // the number of docs committed
    cb::RelaxedAtomic<size_t> docsCommitted;
    // the number of open() calls
    cb::RelaxedAtomic<size_t> numOpen;
    // the number of close() calls
    cb::RelaxedAtomic<size_t> numClose;
    // the number of vbuckets loaded
    cb::RelaxedAtomic<size_t> numLoadedVb;

    //stats tracking failures
    cb::RelaxedAtomic<size_t> numCompactionFailure;
    cb::RelaxedAtomic<size_t> numGetFailure;
    cb::RelaxedAtomic<size_t> numSetFailure;
    cb::RelaxedAtomic<size_t> numDelFailure;
    cb::RelaxedAtomic<size_t> numOpenFailure;
    cb::RelaxedAtomic<size_t> numVbSetFailure;

    /**
     * Number of documents read (full and meta-only) from disk for background
     * fetch operations.
     */
    cb::RelaxedAtomic<size_t> io_bg_fetch_docs_read;
    //! Number of logical write operations (i.e. one per saved doc; not
    //  considering how many actual pwrite() calls were made).
    cb::RelaxedAtomic<size_t> io_num_write;
    //! Document bytes (key+meta+value) read for background fetch operations.
    cb::RelaxedAtomic<size_t> io_bgfetch_doc_bytes;
    //! Number of bytes written (key + value + application rev metadata)
    cb::RelaxedAtomic<size_t> io_document_write_bytes;

    /* for flush and vb delete, no error handling in KVStore, such
     * failure should be tracked in MC-engine  */

    // How long it takes us to complete a read
    Hdr1sfMicroSecHistogram readTimeHisto;
    // How big are our reads?
    Hdr1sfInt32Histogram readSizeHisto;
    // How long it takes us to complete a write
    Hdr1sfMicroSecHistogram writeTimeHisto;
    // Number of logical bytes written to disk for each document saved
    // (document key + meta + value).
    Hdr1sfInt32Histogram writeSizeHisto;
    // Time spent in delete() calls.
    Hdr1sfMicroSecHistogram delTimeHisto;
    // Time spent in commit
    Hdr1sfMicroSecHistogram commitHisto;
    // Time spent in compaction
    Hdr1sfMicroSecHistogram compactHisto;
    // Time spent in saving documents to disk
    Hdr1sfMicroSecHistogram saveDocsHisto;
    // Batch size while saving documents
    Hdr1sfInt32Histogram batchSize;
    //Time spent in vbucket snapshot
    Hdr1sfMicroSecHistogram snapshotHisto;

    // Count and histogram filesystem read()s per getMulti() request
    cb::RelaxedAtomic<size_t> getMultiFsReadCount;
    Hdr1sfInt32Histogram getMultiFsReadHisto;

    // Histogram of filesystem read()s per getMulti() request, divided by
    // the number of documents fetched; gives an average read() count
    // per fetched document.
    Hdr1sfInt32Histogram getMultiFsReadPerDocHisto;

    /// Histogram of disk Write Amplification ratios for each batch of items
    /// flushed to disk (each saveDocs() call).
    /// Encoded as integer, by multipling the floating-point ratio by 10 -
    // e.g. ratio of 3.3 -> 33
    HdrHistogram flusherWriteAmplificationHisto{
            0, 1000, 2, HdrHistogram::Iterator::IterMode::Percentiles};

    // Stats from the underlying OS file operations
    FileStats fsStats;

    // Underlying stats for OS file operations during compaction
    FileStats fsStatsCompaction;

    size_t getMemFootPrint() const {
        return readTimeHisto.getMemFootPrint() +
               readSizeHisto.getMemFootPrint() +
               writeTimeHisto.getMemFootPrint() +
               writeSizeHisto.getMemFootPrint() +
               delTimeHisto.getMemFootPrint() + compactHisto.getMemFootPrint() +
               snapshotHisto.getMemFootPrint() + commitHisto.getMemFootPrint() +
               saveDocsHisto.getMemFootPrint() + batchSize.getMemFootPrint() +
               getMultiFsReadHisto.getMemFootPrint() +
               getMultiFsReadPerDocHisto.getMemFootPrint() +
               fsStats.getMemFootPrint() + fsStatsCompaction.getMemFootPrint() +
               flusherWriteAmplificationHisto.getMemFootPrint();
    }
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
 * Base class representing kvstore operations.
 */
class KVStore {
public:
    /**
     * Enum to provide a implementation independent mutation status code of
     * a mutation result at the storage layer. Storage engines should re-map
     * their status codes to a KVStore::MutationStatus. When calling the
     * persistence callbacks. To inform it of the result of the mutation.
     */
    enum class MutationStatus { Success, DocNotFound, Failed };

    /**
     * Enum to represent the state of a resulting set mutation performed by
     * the storage engine. This is used to inform the set persistence callback
     * of the given mutation and is re-mapped from the KVStore::MutationStatus
     * returned from the given storage engine.
     */
    enum class MutationSetResultState { DocNotFound, Failed, Insert, Update };

    KVStore(KVStoreConfig& config, bool read_only = false);

    virtual ~KVStore();

    /**
     * Allow the kvstore to add extra statistics information
     * back to the client
     * @param prefix prefix to use for the stats
     * @param add_stat the callback function to add statistics
     * @param c the cookie to pass to the callback function
     * @param args are additional arguments to be parsed, can be empty
     */
    virtual void addStats(const AddStatFn& add_stat,
                          const void* c,
                          const std::string& args);

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

    size_t getMemFootPrint() {
        return st.getMemFootPrint();
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
     * @param commitData a reference to a VB::Commit object which is required
     *        for persisted metadata updates and collection item counting
     * @return false if the commit fails
     */
    virtual bool commit(VB::Commit& commitData) = 0;

    /**
     * Rollback the current transaction.
     */
    virtual void rollback() = 0;

    /**
     * Get the properties of the underlying storage.
     */
    virtual StorageProperties getStorageProperties() = 0;

    /**
     * Set an item into the kv store. cc
     *
     * @param item The item to store
     * @param cb Callback object which will be invoked when the set() has been
     *        persisted to disk.
     */
    virtual void set(queued_item item) = 0;

    /**
     * Get an item from the kv store.
     */
    virtual GetValue get(const DiskDocKey& key, Vbid vb) = 0;

    virtual GetValue getWithHeader(const KVFileHandle& kvFileHandle,
                                   const DiskDocKey& key,
                                   Vbid vb,
                                   GetMetaOnly getMetaOnly) = 0;

    /**
     * Set the max bucket quota to the given size.
     *
     * @param size  The new max bucket quota size.
     */
    virtual void setMaxDataSize(size_t size) {
        // Might be overloaded to do some work
    }

    /**
     * Retrieve multiple documents from the underlying storage system at once.
     *
     * @param vb vbucket id of a document
     * @param itms list of items whose documents are going to be retrieved
     */
    virtual void getMulti(Vbid vb, vb_bgfetch_queue_t& itms) {
        throw std::runtime_error("Backend does not support getMulti()");
    }

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
     * @param callback Callback invoked for each key found.
     * @throws std::runtime_error if the range scan could not be successfully
     *         completed. (Note: finding zero docments in the given range is
     *         considered successful).
     */
    virtual void getRange(Vbid vb,
                          const DiskDocKey& startKey,
                          const DiskDocKey& endKey,
                          const GetRangeCb& cb) {
        throw std::runtime_error("Backend does not support getRange()");
    }

    /**
     * Delete an item from the kv store.
     *
     * @param item The item to delete
     */
    virtual void del(queued_item item) = 0;

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
     */
    virtual bool snapshotVBucket(Vbid vbucketId,
                                 const vbucket_state& vbstate) = 0;

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

    void setVBucketState(Vbid vbid, const vbucket_state& vbs);

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

    /**
     * This method is called after persisting a batch of data to perform any
     * pending tasks on the underlying KVStore instance.
     */
    virtual void pendingTasks() = 0;

    uint64_t getLastPersistedSeqno(Vbid vbid);

    bool isReadOnly() const {
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
            const DiskDocKey& start_key,
            uint32_t count,
            std::shared_ptr<Callback<const DiskDocKey&>> cb) = 0;

    /**
     * Create a KVStore Scan Context with the given options. On success,
     * returns a pointer to the ScanContext. The caller can then call scan()
     * to execute the scan. The context should be deleted by the caller using
     * destroyScanContext() when finished with.
     *
     * The caller specifies two callback objects - GetValue and CacheLookup:
     *
     * 1. GetValue callback is invoked for each object loaded from disk, for
     *    the caller to process that item.
     * 2. CacheLookup callback an an optimization to avoid loading data from
     *    disk for already-resident items - it is invoked _before_ loading the
     *    item's value from disk, to give ep-engine's in-memory cache the
     *    opportunity to fulfill the item (assuming the item is in memory).
     *    If this callback has status ENGINE_KEY_EEXISTS then the document is
     *    considered to have been handled purely from memory and the GetValue
     *    callback is skipped.
     *    If this callback has status ENGINE_SUCCESS then it wasn't fulfilled
     *    from memory, and will instead be loaded from disk and GetValue
     *    callback invoked.
     *
     * @param cb GetValue callback
     * @param cl Cache lookup callback
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
     * Obtain a KVFileHandle which holds the KVStore implementation's handle
     * and provides RAII management of the resource.
     *
     * @param vbid the vbucket to open
     * @return a unique_ptr to a new KVFileHandle object
     */
    virtual std::unique_ptr<KVFileHandle> makeFileHandle(Vbid vbid) = 0;

    /**
     * Retrieve the stored item count for the given collection, does not error
     * for collection not found as that's a legitimate state (and returns 0)
     * @param kvFileHandle a handle into a KV data file
     * @param collection the id of the collection to lookup
     * @return optional persisted stats, initialised if the collection was found
     */
    virtual boost::optional<Collections::VB::PersistedStats> getCollectionStats(
            const KVFileHandle& kvFileHandle, CollectionID collection) = 0;

    /**
     * Prepare for delete of the vbucket file
     *
     * @param vbid ID of the vbucket being deleted
     * @return the revision ID to delete (via ::delVBucket)
     */
    uint64_t prepareToDelete(Vbid vbid);

    /**
     * Prepare for create of the vbucket
     * @param vbid ID of the vbucket about to be created
     */
    void prepareToCreate(Vbid vbid);

    /**
     * Set a system event into the KVStore.
     * Collection system events will be used to maintain extra meta-data before
     * writing to disk.
     * @param item The Item representing the event
     */
    void setSystemEvent(const queued_item);

    /**
     * delete a system event in the KVStore.
     * Collection system events will be used to maintain extra meta-data before
     * writing to disk.
     * @param item The Item representing the event
     */
    void delSystemEvent(const queued_item);

    /**
     * Return data that EPBucket requires for the creation of a
     * Collections::VB::Manifest
     *
     * @param vbid vbucket to get data from
     * @return the persisted manifest data for the given vbid
     */
    virtual Collections::KVStore::Manifest getCollectionsManifest(
            Vbid vbid) = 0;

    /**
     * Return all collections that are dropped, i.e. not open but still exist
     * The implementation of this method can return empty vector if the
     * underlying KV store atomically drops collections
     *
     * @param vbid vbucket to get data from
     * @return vector of collections that are dropped but still may have data
     */
    virtual std::vector<Collections::KVStore::DroppedCollection>
    getDroppedCollections(Vbid vbid) = 0;

protected:
    /// Get a string to use as the prefix for the stats. This is typically
    /// "ro_<shard id>" for the read only store, and "rw_<shard id>" for the
    /// read write store.
    std::string getStatsPrefix() const;

    /**
     * Prepare for delete of the vbucket file - Implementation specific method
     * that is called by prepareToDelete
     *
     * @param vbid ID of the vbucket being deleted
     * @return the revision ID to delete (via ::delVBucket)
     */
    virtual uint64_t prepareToDeleteImpl(Vbid vbid) = 0;

    /*
     * Prepare for a creation of the vbucket file - Implementation specific
     * method that is called by prepareToCreate
     *
     * @param vbid ID of the vbucket being created
     */
    virtual void prepareToCreateImpl(Vbid vbid) = 0;

    void createDataDir(const std::string& dbname);

    /**
     * Updates the cached state for a vbucket
     *
     * @param vbid the vbucket id
     * @param vbState the new state information for the vbucket
     *
     * @return true if the cached vbucket state is updated
     */
    bool updateCachedVBState(Vbid vbid, const vbucket_state& vbState);

    /**
     * Reset the cached state for a vbucket (see vbucket_state::reset)
     *
     * @param vbid the vbucket id to call reset on
     */
    void resetCachedVBState(Vbid vbid);

    /* all stats */
    KVStoreStats st;
    KVStoreConfig& configuration;
    bool readOnly;
    std::vector<std::unique_ptr<vbucket_state>> cachedVBStates;
    /* non-deleted docs in each file, indexed by vBucket.
       RelaxedAtomic to allow stats access without lock. */
    std::vector<cb::RelaxedAtomic<size_t>> cachedDocCount;
    cb::RelaxedAtomic<uint16_t> cachedValidVBCount;

    /// Metadata that the underlying implementation must persist
    Collections::KVStore::CommitMetaData collectionsMeta;
};

std::string to_string(KVStore::MutationStatus status);
std::string to_string(KVStore::MutationSetResultState status);

/**
 * Structure holding the read/write and read only instances of the KVStore.
 * They could be the same underlying object, or different.
 */
struct KVStoreRWRO {
    KVStoreRWRO() /*rw/ro default init is ok*/ {
    }
    KVStoreRWRO(KVStore* rw, KVStore* ro) : rw(rw), ro(ro) {
    }

    KVStoreRWRO(std::unique_ptr<KVStore> rw, std::unique_ptr<KVStore> ro)
        : rw(std::move(rw)), ro(std::move(ro)) {
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
    RollbackCB() {
    }

    virtual void callback(GetValue &val) = 0;

    void setKVFileHandle(std::unique_ptr<KVFileHandle> handle) {
        kvFileHandle = std::move(handle);
    }

    const KVFileHandle* getKVFileHandle() const {
        return kvFileHandle.get();
    }

protected:
    /// The database handle to use when lookup up items in the new, rolled back
    /// database.
    std::unique_ptr<KVFileHandle> kvFileHandle;
};

/**
 * State associated with a KVStore transaction (begin() / commit() pair).
 * Users would typically subclass this, and provide an instance to begin().
 * The KVStore will then provide a pointer to it during every persistence
 * callback.
 */
struct TransactionContext {
    TransactionContext(Vbid vbid) : vbid(vbid) {
    }
    virtual ~TransactionContext(){};

    /**
     * Callback for sets. Invoked after persisting an item. Does nothing by
     * default as a subclass should provide functionality but we want to allow
     * simple tests to run without doing so.
     */
    virtual void setCallback(const queued_item& item,
                             KVStore::MutationSetResultState mutationStatus) {
    }

    /**
     * Callback for deletes. Invoked after persisting an item. Does nothing by
     * default as a subclass should provide functionality but we want to allow
     * simple tests to run without doing so.
     */
    virtual void deleteCallback(const queued_item& item,
                                KVStore::MutationStatus mutationStatus) {
    }

    const Vbid vbid;
};
