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
#include "kvstore_iface.h"
#include "persistence_callback.h"
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
class EPStats;
class Item;
class KVStore;
class KVStoreConfig;
class RollbackCB;
class RollbackResult;
class VBucket;

namespace cb::mcbp {
class Request;
} // namespace cb::mcbp


enum class GetMetaOnly { Yes, No };

using BloomFilterCBPtr = std::shared_ptr<Callback<Vbid&, const DocKey&, bool&>>;
using ExpiredItemsCBPtr = std::shared_ptr<Callback<Item&, time_t&>>;


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
    mutable cb::RelaxedAtomic<size_t> numGetFailure;
    cb::RelaxedAtomic<size_t> numSetFailure;
    cb::RelaxedAtomic<size_t> numDelFailure;
    mutable cb::RelaxedAtomic<size_t> numOpenFailure;
    cb::RelaxedAtomic<size_t> numVbSetFailure;

    /**
     * Number of documents read (full and meta-only) from disk for background
     * fetch operations.
     */
    mutable cb::RelaxedAtomic<size_t> io_bg_fetch_docs_read;
    //! Number of logical write operations (i.e. one per saved doc; not
    //  considering how many actual pwrite() calls were made).
    cb::RelaxedAtomic<size_t> io_num_write;
    //! Document bytes (key+meta+value) read for background fetch operations.
    mutable cb::RelaxedAtomic<size_t> io_bgfetch_doc_bytes;
    //! Number of bytes written (key + value + application rev metadata)
    cb::RelaxedAtomic<size_t> io_document_write_bytes;

    /* for flush and vb delete, no error handling in KVStore, such
     * failure should be tracked in MC-engine  */

    // How long it takes us to complete a read
    mutable Hdr1sfMicroSecHistogram readTimeHisto;
    // How big are our reads?
    mutable Hdr1sfInt32Histogram readSizeHisto;
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
    mutable cb::RelaxedAtomic<size_t> getMultiFsReadCount;
    mutable Hdr1sfInt32Histogram getMultiFsReadHisto;

    // Histogram of filesystem read()s per getMulti() request, divided by
    // the number of documents fetched; gives an average read() count
    // per fetched document.
    mutable Hdr1sfInt32Histogram getMultiFsReadPerDocHisto;

    /// Histogram of disk Write Amplification ratios for each batch of items
    /// flushed to disk (each saveDocs() call).
    /// Encoded as integer, by multipling the floating-point ratio by 10 -
    // e.g. ratio of 3.3 -> 33
    HdrHistogram flusherWriteAmplificationHisto{
            1, 1000, 2, HdrHistogram::Iterator::IterMode::Percentiles};

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
    enum class ByIdScan : bool { Yes, No };

    /**
     * Will the KVStore de-dupe items such that only the highest seqno for any
     * given key in a single flush batch is persisted?
     */
    enum class AutomaticDeduplication : bool { Yes, No };

    /**
     * Will the KVStore count items in the prepare namespace (and update the
     * values appropriately in the vbstate)
     */
    enum class PrepareCounting : bool { Yes, No };

    StorageProperties(ByIdScan byIdScan,
                      AutomaticDeduplication automaticDeduplication,
                      PrepareCounting prepareCounting)
        : byIdScan(byIdScan),
          automaticDeduplication(automaticDeduplication),
          prepareCounting(prepareCounting) {
    }

    bool hasByIdScan() const {
        return byIdScan == ByIdScan::Yes;
    }

    bool hasAutomaticDeduplication() const {
        return automaticDeduplication == AutomaticDeduplication::Yes;
    }

    bool hasPrepareCounting() const {
        return prepareCounting == PrepareCounting::Yes;
    }

private:
    ByIdScan byIdScan;
    AutomaticDeduplication automaticDeduplication;
    PrepareCounting prepareCounting;
};


/**
 * Base class for some KVStores that implements common functionality.
 */
class KVStore : public KVStoreIface {
public:
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
                          const std::string& args) const;

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
                                const CookieIface* c) const;

    /**
     * Resets kvstore specific stats
     */
    virtual void resetStats() {
        st.reset();
    }

    virtual size_t getMemFootPrint() const {
        return st.getMemFootPrint();
    }

    /**
     * Needed to prevent the convenience version of get below from hiding the
     * interface version.
     */
    virtual GetValue get(const DiskDocKey& key,
                         Vbid vb,
                         ValueFilter filter) const = 0;
    /**
     * Convenience version of get() which fetches the value uncompressed.
     */
    GetValue get(const DiskDocKey& key, Vbid vb) const {
        return get(key, vb, ValueFilter::VALUES_DECOMPRESSED);
    }

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
    virtual void getMulti(Vbid vb, vb_bgfetch_queue_t& itms) const {
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
                          const GetRangeCb& cb) const {
        throw std::runtime_error("Backend does not support getRange()");
    }

    nlohmann::json getPersistedStats() const;

    bool snapshotStats(const nlohmann::json& stats);

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

    void prepareForDeduplication(std::vector<queued_item>& items);

    uint64_t getLastPersistedSeqno(Vbid vbid);

    const KVStoreStats& getKVStoreStat() const {
        return st;
    }

    /// Does the backend support historical snapshots
    virtual bool supportsHistoricalSnapshots() const {
        return false;
    }

    uint64_t prepareToDelete(Vbid vbid);

    void prepareToCreate(Vbid vbid);

    void setSystemEvent(TransactionContext& txnCtx, const queued_item);

    void delSystemEvent(TransactionContext& txnCtx, const queued_item);

    void setMakeCompactionContextCallback(MakeCompactionContextCallback cb) {
        makeCompactionContextCallback = cb;
    }

    /**
     * Set the number of storage threads based on configuration settings
     */
    virtual void setStorageThreads(ThreadPoolConfig::StorageThreadCount num) {
        // ignored by default
    }

    void setPostFlushHook(std::function<void()> hook) {
        postFlushHook = hook;
    }

    void endTransaction(Vbid vbid);

    /**
     * Validate if the given vBucket is in a transaction
     *
     * @param vbid to check
     * @param caller to print in throw if not in transaction
     * @throws invalid_argument if not in transaction
     */
    void checkIfInTransaction(Vbid vbid, std::string_view caller);

protected:
    /// Get a string to use as the prefix for the stats. This is typically
    /// "ro_<shard id>" for the read only store, and "rw_<shard id>" for the
    /// read write store.
    std::string getStatsPrefix() const;

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

    /**
     * We cache many values per-vBucket and to save memory usage we only
     * allocate num vBuckets / num shards slots in the array. Return correct
     * slot.
     *
     * @param vbid Vbid to map
     * @return vbid / num shards
     */
    Vbid::id_type getCacheSlot(Vbid vbid) const;

    /// @returns the size to use for the cached values
    size_t getCacheSize() const;

    /**
     * Start a transaction by setting the necessary inTransaction bool
     *
     * @param vbid to start a transaction for
     * @return true if a transaction can be started, false if not
     */
    bool startTransaction(Vbid vbid);

    /* all stats */
    KVStoreStats st;
    std::vector<std::unique_ptr<vbucket_state>> cachedVBStates;
    cb::RelaxedAtomic<uint16_t> cachedValidVBCount;

    /**
     * Callback function to be invoked when the underlying KVStore needs to
     * create a compaction context.
     */
    MakeCompactionContextCallback makeCompactionContextCallback;

    /**
     * Guards against users attempting to flush against the same vBucket
     * concurrently.
     */
    std::vector<std::atomic_bool> inTransaction;

    // Test-only. If set, this is executed after the a flush-batch is committed
    // to disk but before we call back into the PersistenceCallback.
    TestingHook<> postFlushHook;
};

std::string to_string(FlushStateDeletion status);
std::string to_string(FlushStateMutation state);

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
    static std::unique_ptr<KVStoreIface> create(KVStoreConfig& config);
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
