/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "callbacks.h"
#include "collections/eraser_context.h"
#include "collections/kvstore.h"
#include "kvstore_fwd.h"
#include "utilities/testing_hook.h"

#include <memcached/engine_common.h>
#include <memcached/thread_pool_config.h>
#include <utilities/hdrhistogram.h>

#include <relaxed_atomic.h>
#include <atomic>
#include <chrono>
#include <cstring>
#include <list>
#include <map>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

/* Forward declarations */
class BucketLogger;
class CookieIface;
class DiskDocKey;
class Item;
class KVStore;
class KVStoreConfig;
class PersistenceCallback;
class RollbackCB;
class RollbackResult;

namespace cb::mcbp {
class Request;
} // namespace cb::mcbp

namespace VB {
class Commit;
} // namespace VB

namespace Collections::VB {
struct PersistedStats;
} // namespace Collections::VB

class vb_bgfetch_item_ctx_t;
struct TransactionContext;

using vb_bgfetch_queue_t =
        std::unordered_map<DiskDocKey, vb_bgfetch_item_ctx_t>;

enum class GetMetaOnly { Yes, No };

using BloomFilterCBPtr = std::shared_ptr<Callback<Vbid&, const DocKey&, bool&>>;
using ExpiredItemsCBPtr = std::shared_ptr<Callback<Item&, time_t&>>;

enum class SnapshotSource { Historical, Head };

/**
 * Generic information about a KVStore file
 */
struct FileInfo {
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
    uint64_t prepareBytesPurged = 0;
    FileInfo pre;
    FileInfo post;

    /**
     * Per-collection size updates to be applied post-compaction.
     */
    using CollectionSizeUpdates = std::unordered_map<CollectionID, ssize_t>;
    CollectionSizeUpdates collectionSizeUpdates;
};

struct CompactionConfig {
    CompactionConfig() = default;
    CompactionConfig(uint64_t purge_before_ts,
                     uint64_t purge_before_seq,
                     bool drop_deletes,
                     bool retain_erroneous_tombstones)
        : purge_before_ts(purge_before_ts),
          purge_before_seq(purge_before_seq),
          drop_deletes(drop_deletes),
          retain_erroneous_tombstones(retain_erroneous_tombstones) {
    }

    CompactionConfig(const CompactionConfig&) = default;
    CompactionConfig& operator=(const CompactionConfig& other) = default;

    /// Move will value copy from 'other' and leave 'other' default constructed
    CompactionConfig(CompactionConfig&& other);
    /// Move will value copy from 'other' and leave 'other' default constructed
    CompactionConfig& operator=(CompactionConfig&& other);

    bool operator==(const CompactionConfig& c) const;
    bool operator!=(const CompactionConfig& c) const {
        return !(*this == c);
    }

    /**
     * Merge 'other' into this instance. Merge results in this object being
     * representative of the current config and the other config.
     *
     * - drop_deletes/retain_erroneous_tombstones are 'sticky', once true
     *   they will remain true.
     * - purge_before_ts and purge_before_seq become the max of this vs other
     */
    void merge(const CompactionConfig& other);

    uint64_t purge_before_ts = 0;
    uint64_t purge_before_seq = 0;
    bool drop_deletes = false;
    bool retain_erroneous_tombstones = false;
};

struct CompactionContext {
    CompactionContext(Vbid vbid,
                      const CompactionConfig& config,
                      uint64_t purgeSeq)
        : vbid(vbid), compactConfig(config), max_purged_seq(purgeSeq) {
    }
    Vbid vbid;

    /// The configuration for this compaction.
    const CompactionConfig compactConfig;

    uint64_t max_purged_seq;
    BloomFilterCBPtr bloomFilterCallback;
    ExpiredItemsCBPtr expiryCallback;
    struct CompactionStats stats;
    /// pointer as context cannot be constructed until deeper inside storage
    std::unique_ptr<Collections::VB::EraserContext> eraserContext;
    Collections::KVStore::DroppedCb droppedKeyCb =
            [](const DiskDocKey&, int64_t, bool, int64_t) {};

    /**
     * A function to call on completion of compaction (before we swap our files)
     * to correctly set in memory state such as the purge seqno.
     */
    std::function<void(CompactionContext&)> completionCallback;

    /// The SyncRepl HCS, can purge any prepares before the HCS.
    uint64_t highCompletedSeqno = 0;
};

using MakeCompactionContextCallback =
        std::function<std::shared_ptr<CompactionContext>(
                Vbid, CompactionConfig&, uint64_t)>;

struct kvstats_ctx {
    explicit kvstats_ctx(VB::Commit& commitData) : commitData(commitData) {
    }

    /// flusher data for managing manifest changes, item counts, vbstate
    VB::Commit& commitData;

    /**
     * Delta of onDiskPrepares that we should add to the value tracked in
     * the persisted VB state before commit
     */
    size_t onDiskPrepareDelta = 0;

    /**
     * Delta of onDiskPrepareBytes that we should add to the value tracked in
     * the persisted VB state before commit.
     */
    ssize_t onDiskPrepareBytesDelta = 0;
};

class NoLookupCallback : public StatusCallback<CacheLookup> {
public:
    void callback(CacheLookup&) override {
    }
};

struct DBFileInfo {
    /// Total size of the file (what 'stat()' would return). Includes both
    /// current data (spaceUsed) plus any previous data which is no longer
    /// referenced in current file header.
    uint64_t fileSize = 0;

    /// Total size of "current" data in the file - sum of all
    /// keys+metdata+values (included deleted docs) plus overheads to manage it
    /// (indexes such as B-Trees, headers etc).
    uint64_t spaceUsed = 0;

    /// Total size of all SyncWrite prepares, both completed and pending.
    /// This can be used to adjust spaceUsed to give an estimate of how much
    /// data in the file is actually needed - completed prepares are no
    /// longer needed and can be purged during compaction - as such they can
    /// be considered part of the "Fragmented" count.
    uint64_t prepareBytes = 0;

    /**
     * @returns An estimate of the number of bytes which are "live" data and
     * hence are not subject to being discarded during compactionn. This
     * is calculated as the size of the current data (spaceUsed), minus an
     * estimate of the size of completed prepares (which will be purged on
     * compaction).
     * Note: All prepared SyncWrites (completed and in-progress) are used as
     *       an estimate for completed sync writes, given (a) it's difficult
     *       to track exactly how any prepares have been completed and (b)
     *       in general we expect the overwhelming majority of on-disk prepares
     *       to be completed.
     */
    uint64_t getEstimatedLiveData() const {
        if (spaceUsed > prepareBytes) {
            // Sanity check - if totalOnDiskPrepareSize is somehow larger than
            // spaceUsed then skip the adjustment.
            return spaceUsed - prepareBytes;
        }
        return spaceUsed;
    }
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

struct vbucket_state;

/**
 * Abstract file handle class to allow a DB file to be opened and held open
 * for multiple KVStore methods.
 */
class KVFileHandle {
public:
    virtual ~KVFileHandle() = default;
};

class ScanContext {
public:
    ScanContext(Vbid vbid,
                std::unique_ptr<KVFileHandle> handle,
                DocumentFilter docFilter,
                ValueFilter valFilter,
                std::unique_ptr<StatusCallback<GetValue>> cb,
                std::unique_ptr<StatusCallback<CacheLookup>> cl,
                const std::vector<Collections::KVStore::DroppedCollection>&
                        droppedCollections,
                int64_t maxSeqno);

    virtual ~ScanContext() = default;

    const StatusCallback<GetValue>& getValueCallback() const {
        return *callback;
    }

    StatusCallback<GetValue>& getValueCallback() {
        return *callback;
    }

    const StatusCallback<CacheLookup>& getCacheCallback() const {
        return *lookup;
    }

    StatusCallback<CacheLookup>& getCacheCallback() {
        return *lookup;
    }

    const Vbid vbid;
    int64_t lastReadSeqno{0};
    const std::unique_ptr<KVFileHandle> handle;
    const DocumentFilter docFilter;
    const ValueFilter valFilter;
    std::unique_ptr<StatusCallback<GetValue>> callback;
    std::unique_ptr<StatusCallback<CacheLookup>> lookup;
    BucketLogger* logger;
    const Collections::VB::ScanContext collectionsContext;
    int64_t maxSeqno;
};

class BySeqnoScanContext : public ScanContext {
public:
    BySeqnoScanContext(
            std::unique_ptr<StatusCallback<GetValue>> cb,
            std::unique_ptr<StatusCallback<CacheLookup>> cl,
            Vbid vb,
            std::unique_ptr<KVFileHandle> handle,
            int64_t start,
            int64_t end,
            uint64_t purgeSeqno,
            DocumentFilter _docFilter,
            ValueFilter _valFilter,
            uint64_t _documentCount,
            const vbucket_state& vbucketState,
            const std::vector<Collections::KVStore::DroppedCollection>&
                    droppedCollections,
            std::optional<uint64_t> timestamp = {});

    const int64_t startSeqno;
    const uint64_t purgeSeqno;
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

    /// Timestamp for the data (if available)
    const std::optional<uint64_t> timestamp;
};

/**
 * ByIdRange describes a sub-set of 'keys' from the lexicographically ordered
 * ById index.
 *    keys = {k | k >= startKey and k < endKey}
 * E.g. startKey="b" and endKey="c" when the ById index is:
 *    {"a", "b", "ba", "bb", "c" }
 * yields:
 *    {"b", "ba", "bb"}
 */
struct ByIdRange {
    ByIdRange(DiskDocKey start, DiskDocKey end)
        : startKey(std::move(start)), endKey(std::move(end)) {
    }
    DiskDocKey startKey;
    DiskDocKey endKey;
    bool rangeScanSuccess{false};
};

class ByIdScanContext : public ScanContext {
public:
    ByIdScanContext(std::unique_ptr<StatusCallback<GetValue>> cb,
                    std::unique_ptr<StatusCallback<CacheLookup>> cl,
                    Vbid vb,
                    std::unique_ptr<KVFileHandle> handle,
                    std::vector<ByIdRange> ranges,
                    DocumentFilter _docFilter,
                    ValueFilter _valFilter,
                    const std::vector<Collections::KVStore::DroppedCollection>&
                            droppedCollections,
                    int64_t maxSeqno);
    std::vector<ByIdRange> ranges;
    // Key should be set by KVStore when a scan must be paused, this is where
    // a scan can resume from
    DiskDocKey lastReadKey;
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
    mutable cb::RelaxedAtomic<size_t> numOpen;
    // the number of close() calls
    mutable cb::RelaxedAtomic<size_t> numClose;
    // the number of vbuckets loaded
    cb::RelaxedAtomic<size_t> numLoadedVb;

    //stats tracking failures
    cb::RelaxedAtomic<size_t> numCompactionFailure;
    cb::RelaxedAtomic<size_t> numGetFailure;
    cb::RelaxedAtomic<size_t> numSetFailure;
    cb::RelaxedAtomic<size_t> numDelFailure;
    mutable cb::RelaxedAtomic<size_t> numOpenFailure;
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
            1, 1000, 2, HdrHistogram::Iterator::IterMode::Percentiles};

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
 * Properties of the storage layer.
 *
 * If concurrent filesystem access is possible, maxConcurrency() will
 * be greater than one.  One will need to determine whether more than
 * one writer is possible as well as whether more than one reader is
 * possible.
 */
class StorageProperties {
public:
    enum class EfficientVBDump : bool { Yes, No };

    enum class EfficientVBDeletion : bool { Yes, No };

    enum class PersistedDeletion : bool { Yes, No };

    enum class EfficientGet : bool { Yes, No };

    /**
     * Does the KVStore allow externally driven compactions (driven via
     * ns_server/EPBucket) whilst we do writes?
     */
    enum class ConcurrentWriteCompact : bool { Yes, No };

    enum class ByIdScan : bool { Yes, No };

    StorageProperties(EfficientVBDump evb,
                      EfficientVBDeletion evd,
                      PersistedDeletion pd,
                      EfficientGet eget,
                      ConcurrentWriteCompact cwc,
                      ByIdScan byIdScan)
        : efficientVBDump(evb),
          efficientVBDeletion(evd),
          persistedDeletions(pd),
          efficientGet(eget),
          concWriteCompact(cwc),
          byIdScan(byIdScan) {
    }

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

    bool hasByIdScan() const {
        return byIdScan == ByIdScan::Yes;
    }

private:
    EfficientVBDump efficientVBDump;
    EfficientVBDeletion efficientVBDeletion;
    PersistedDeletion persistedDeletions;
    EfficientGet efficientGet;
    ConcurrentWriteCompact concWriteCompact;
    ByIdScan byIdScan;
};


/**
 * Base class representing kvstore operations.
 */
class KVStore {
public:
    /// Result of flushing a Deletion, passed to the PersistenceCallback.
    enum class FlushStateDeletion { Delete, DocNotFound, Failed };

    /// Result of flushing a Mutation, passed to the PersistenceCallback.
    enum class FlushStateMutation { Insert, Update, Failed };

    explicit KVStore(bool read_only = false);

    virtual ~KVStore();

    /**
     * Called when the engine is going away so we can shutdown any backend tasks
     * the underlying store create to prevent them from racing with destruction.
     */
    virtual void deinitialize() {
    }

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
    virtual bool getStat(std::string_view name, size_t& value) const {
        return false;
    }

    /// Request the specified statistics from kvstore.
    ///
    /// @param [in] keys specifies a set of statistics to be fetched.
    /// @return statistic values. Note that the string_view keys in the returned
    /// map refer to the same string keys that the input string_view refers to.
    /// Hence the map is ok to use only as long as the string keys live.
    ///
    virtual GetStatsMap getStats(gsl::span<const std::string_view> keys) const;

    /**
     * Show kvstore specific timing stats.
     *
     * @param add_stat the callback function to add statistics
     * @param c the cookie to pass to the callback function
     */
    virtual void addTimingStats(const AddStatFn& add_stat,
                                const CookieIface* c);

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
     * Begin a transaction (if not already in one).
     *
     * @param txCtx A transaction context to associate with this transaction.
     *        The context will be passed to each operations' completion
     *        callback, so this can be used to hold state common to the entire
     *        transaction without having to duplicate it in every Callback.
     *
     * @return false if we cannot begin a transaction
     */
    bool begin(std::unique_ptr<TransactionContext> txCtx);

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
     * @param key The document key to fetch.
     * @param vb The vbucket to fetch from.
     * @param filter In what form should the item be fetched?
     *        Item::getDatatype() will reflect the format they are returned in.
     */
    virtual GetValue get(const DiskDocKey& key,
                         Vbid vb,
                         ValueFilter filter) = 0;

    /**
     * Convenience version of get() which fetches the value uncompressed.
     */
    GetValue get(const DiskDocKey& key, Vbid vb) {
        return get(key, vb, ValueFilter::VALUES_DECOMPRESSED);
    }

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
                                   ValueFilter filter) = 0;

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
     * @param itms list of items whose documents are going to be retrieved.
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
    virtual std::vector<vbucket_state *> listPersistedVbuckets() = 0;

    /**
     * Get json of persisted engine and DCP stats. This API is invoked during
     * warmup to get the engine stats from the previous session.
     *
     * @return stats nlohmann::json object of the engine stats from the previous
     * session is stored. If the function fails and empty nlohmann::json will be
     * returned
     */
    nlohmann::json getPersistedStats();

    /**
     * Persist a snapshot of a collection of stats.
     */
    bool snapshotStats(const nlohmann::json& stats);

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
     *
     * @param vbLock a lock to serialize compaction and flusher to the
     *               specific vbucket. When called the lock is _HELD_
     *               so engines who don't need exclusive access should
     *               release the lock. The lock may be held or released
     *               upon return, the caller will take the appropriate action.
     * @param c shared_ptr to the CompactionContext that includes various
     * callbacks and compaction parameters
     * @return true if the compaction was successful
     */
    virtual bool compactDB(std::unique_lock<std::mutex>& vbLock,
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
                                          Vbid vbid){};

    /**
     * Returns a ptr to the vbucket_state in the KVStore cache. Not all
     * implementations can simply return the cached value (magma) so this is
     * virtual.
     */
    virtual vbucket_state* getCachedVBucketState(Vbid vbid) = 0;

    /**
     * Return the vbucket_state stored on disk for the given vBucket. Does NOT
     * update the cachedVBState.
     * @throws exceptions if there was a problem returning the state.
     */
    virtual vbucket_state getPersistedVBucketState(Vbid vbid) = 0;

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
                                    std::unique_ptr<RollbackCB>) = 0;

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

    bool isReadWrite() const {
        return !isReadOnly();
    }

    KVStoreStats& getKVStoreStat() {
        return st;
    }

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
            std::shared_ptr<StatusCallback<const DiskDocKey&>> cb) = 0;

    /// Does the backend support historical snapshots
    virtual bool supportsHistoricalSnapshots() const {
        return false;
    }

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
     * @return a BySeqnoScanContext, null if there's an error
     */
    virtual std::unique_ptr<BySeqnoScanContext> initBySeqnoScanContext(
            std::unique_ptr<StatusCallback<GetValue>> cb,
            std::unique_ptr<StatusCallback<CacheLookup>> cl,
            Vbid vbid,
            uint64_t startSeqno,
            DocumentFilter options,
            ValueFilter valOptions,
            SnapshotSource source) const = 0;

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
     * If the ScanContext cannot be created, returns null.
     */
    virtual std::unique_ptr<ByIdScanContext> initByIdScanContext(
            std::unique_ptr<StatusCallback<GetValue>> cb,
            std::unique_ptr<StatusCallback<CacheLookup>> cl,
            Vbid vbid,
            const std::vector<ByIdRange>& ranges,
            DocumentFilter options,
            ValueFilter valOptions) const = 0;

    /**
     * Run a BySeqno scan
     * @param sctx non-const reference to the context, internal callbacks may
     *        write to the object as progress is made through the scan
     */
    virtual scan_error_t scan(BySeqnoScanContext& sctx) const = 0;

    /**
     * Run a ById scan
     * @param sctx non-const reference to the context, internal callbacks may
     *        write to the object as progress is made through the scan
     */
    virtual scan_error_t scan(ByIdScanContext& sctx) const = 0;

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
     * @return pair of bool for success and persisted stats initialised if the
     * collection stats have been written or default initialised stats
     * otherwise.
     */
    virtual std::pair<bool, Collections::VB::PersistedStats> getCollectionStats(
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
     * @param item The Item representing the event
     */
    void setSystemEvent(const queued_item);

    /**
     * delete a system event in the KVStore.
     * @param item The Item representing the event
     */
    void delSystemEvent(const queued_item);

    /**
     * Return data that EPBucket requires for the creation of a
     * Collections::VB::Manifest
     *
     * @param vbid vbucket to get data from
     * @return pair of bool status and the persisted manifest data for the given
     *         vbid
     */
    virtual std::pair<bool, Collections::KVStore::Manifest>
    getCollectionsManifest(Vbid vbid) = 0;

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
    getDroppedCollections(Vbid vbid) = 0;

    void setMakeCompactionContextCallback(MakeCompactionContextCallback cb) {
        makeCompactionContextCallback = cb;
    }

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
     * Set the number of storage threads based on configuration settings
     */
    virtual void setStorageThreads(ThreadPoolConfig::StorageThreadCount num) {
        // ignored by default
    }

    /**
     * Test-only. See definition of postFlushHook for details.
     */
    void setPostFlushHook(std::function<void()> hook) {
        postFlushHook = hook;
    }

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
     * @param vbid
     * @param newVbstate
     * @return Whether or not the new vbstate needs to be persisted
     */
    bool needsToBePersisted(Vbid vbid, const vbucket_state& newVbstate);

    /**
     * Updates the cached state for a vbucket
     *
     * @param vbid the vbucket id
     * @param vbState the new state information for the vbucket
     */
    void updateCachedVBState(Vbid vbid, const vbucket_state& vbState);

    /**
     * Reset the cached state for a vbucket (see vbucket_state::reset)
     *
     * @param vbid the vbucket id to call reset on
     */
    void resetCachedVBState(Vbid vbid);

    /* all stats */
    KVStoreStats st;
    bool readOnly;
    std::vector<std::unique_ptr<vbucket_state>> cachedVBStates;
    /* non-deleted docs in each file, indexed by vBucket.
       RelaxedAtomic to allow stats access without lock. */
    std::vector<cb::RelaxedAtomic<size_t>> cachedDocCount;
    cb::RelaxedAtomic<uint16_t> cachedValidVBCount;

    /**
     * Callback function to be invoked when the underlying KVStore needs to
     * create a compaction context.
     */
    MakeCompactionContextCallback makeCompactionContextCallback;

    // This variable is used to verify that the KVStore API is used correctly
    // "Correctly" means that the caller must use the API in the following way:
    //      - begin() x1
    //      - set() / del() xN
    //      - commit()
    bool inTransaction{false};

    std::unique_ptr<TransactionContext> transactionCtx;

    // Test-only. If set, this is executed after the a flush-batch is committed
    // to disk but before we call back into the PersistenceCallback.
    TestingHook<> postFlushHook;
};

std::string to_string(KVStore::FlushStateDeletion status);
std::string to_string(KVStore::FlushStateMutation state);

/**
 * Structure holding the read/write and read only instances of the KVStore.
 * They could be the same underlying object, or different.
 */
struct KVStoreRWRO {
    KVStoreRWRO() = default;
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
    void callback(GetValue& val) override = 0;

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
    explicit TransactionContext(Vbid vbid) : vbid(vbid) {
    }
    virtual ~TransactionContext() = default;

    /**
     * Callback for sets. Invoked after persisting an item. Does nothing by
     * default as a subclass should provide functionality but we want to allow
     * simple tests to run without doing so.
     */
    virtual void setCallback(const queued_item&, KVStore::FlushStateMutation) {
    }

    /**
     * Callback for deletes. Invoked after persisting an item. Does nothing by
     * default as a subclass should provide functionality but we want to allow
     * simple tests to run without doing so.
     */
    virtual void deleteCallback(const queued_item&,
                                KVStore::FlushStateDeletion) {
    }

    const Vbid vbid;
};
