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
#include "ep_time.h"
#include "kvstore_fwd.h"
#include "kvstore_iface.h"
#include "rollback_result.h"
#include "utilities/testing_hook.h"
#include "vbucket_fwd.h"

#include <memcached/engine_common.h>
#include <memcached/thread_pool_config.h>
#include <hdrhistogram/hdrhistogram.h>

#include <relaxed_atomic.h>
#include <atomic>
#include <chrono>
#include <cstring>
#include <map>
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
    size_t collectionsPurged = 0;
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
     * - drop_deletes, retain_erroneous_tombstones and internally_requested are
     *   'sticky', once true they will remain true.
     * - purge_before_ts and purge_before_seq become the max of this vs other
     */
    void merge(const CompactionConfig& other);

    uint64_t purge_before_ts = 0;
    uint64_t purge_before_seq = 0;
    bool drop_deletes = false;
    bool retain_erroneous_tombstones = false;

    // Did KV-engine request compaction? E.g. a collection was dropped
    bool internally_requested = false;
};

/**
 * Type of item purged
 */
enum class PurgedItemType {
    // Tombstone - deleted document
    Tombstone = 0,
    // Logical deletion - item belonging to dropped collection
    LogicalDelete,
    // Prepare - complete prepare (seqno lower than PCS)
    Prepare,
};

/**
 * RollbackPurgeSeqnoCtx implements common behaviours to all KVStores that are
 * executed when we update the rollbackPurgeSeqno. This allows us to subclass
 * PurgedItemContext for KVStores with specific additional behaviours.
 */
class RollbackPurgeSeqnoCtx {
public:
    RollbackPurgeSeqnoCtx(uint64_t rollbackPurgeSeqno)
        : rollbackPurgeSeqno(rollbackPurgeSeqno) {
    }

    virtual ~RollbackPurgeSeqnoCtx() = default;

    /**
     * Update the rollback purge seqno
     *
     * @param seqno The seqno of the item purged
     */
    virtual void updateRollbackPurgeSeqno(uint64_t seqno) {
        rollbackPurgeSeqno = std::max(rollbackPurgeSeqno, seqno);
    }

    uint64_t getRollbackPurgeSeqno() const {
        return rollbackPurgeSeqno;
    }

protected:
    /**
     * The purgeSeqno from the VBucket/DCP perspective. This purge seqno relates
     * to the point at which DCP consumers connecting with lower start seqnos
     * would have to roll back to zero. The KVStore may purge seqnos higher
     * than this but only for items that are not required to rebuild a replica.
     * Such items currently include completed prepares and logical deletetions
     * (items belonging to dropped collections).
     */
    uint64_t rollbackPurgeSeqno;
};

/**
 * PurgedItemContext implements common behaviours to all KVStores that are
 * executed when we purge an item. A KVStore calls purgedItem for each item
 * purged along with the type of the item and the seqno of it. This allows us to
 * subclass PurgedItemContext for KVStores with specific additional behaviours.
 */
class PurgedItemCtx {
public:
    PurgedItemCtx(uint64_t purgeSeq)
        : rollbackPurgeSeqnoCtx(
                  std::make_unique<RollbackPurgeSeqnoCtx>(purgeSeq)) {
    }

    virtual ~PurgedItemCtx() = default;

    /**
     * Process a purged item
     *
     * @param type The type of the item purged
     * @param seqno The seqno of the item purged
     */
    virtual void purgedItem(PurgedItemType type, uint64_t seqno) {
        switch (type) {
        case PurgedItemType::Tombstone:
            // Only tombstones need to move the rollback purge seqno
            rollbackPurgeSeqnoCtx->updateRollbackPurgeSeqno(seqno);
            break;
        case PurgedItemType::LogicalDelete:
        case PurgedItemType::Prepare:
            break;
        }
    }

    /**
     * Overridable ctx object that tracks the rollbackPurgeSeqno. KVStores may
     * override it to add additional behaves that they may wish to execute when
     * updating the purge seqno.
     */
    std::unique_ptr<RollbackPurgeSeqnoCtx> rollbackPurgeSeqnoCtx;
};

struct CompactionContext {
    CompactionContext(VBucketPtr vb,
                      CompactionConfig config,
                      uint64_t purgeSeq,
                      std::optional<time_t> timeToExpireFrom = {})
        : compactConfig(std::move(config)),
          timeToExpireFrom(timeToExpireFrom),
          purgedItemCtx(std::make_unique<PurgedItemCtx>(purgeSeq)),
          vb(vb) {
        isShuttingDown = []() { return false; };
    }

    uint64_t getRollbackPurgeSeqno() const {
        return purgedItemCtx->rollbackPurgeSeqnoCtx->getRollbackPurgeSeqno();
    }

    VBucketPtr getVBucket() const {
        auto vbPtr = vb.lock();
        if (!vbPtr) {
            throw std::runtime_error(
                    "CompactionContext::getVBucket() vbucket no longer exists");
        }
        return vbPtr;
    }

    /// The configuration for this compaction.
    const CompactionConfig compactConfig;
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

    /// Time from which we expire items (if set). Otherwise current time is used
    std::optional<time_t> timeToExpireFrom = {};

    /**
     * Function to call if the (in-memory) VBucket purge seqno might need to be
     * updated. This will only be performed if the param seqno is greater than
     * the current seqno.
     */
    std::function<void(uint64_t)> maybeUpdateVBucketPurgeSeqno;

    /**
     * Context object used to udpate status we track when we purge an item.
     * By default this will use a PurgedItemContext which updates a
     * rollbackPurgeSeqno for specific items. This behaviour is common to all
     * KVStores. KVStore can overwrite purgedItemContext with some subclass to
     * implement additional functionality that may/may not need to be done when
     * purging items.
     */
    std::unique_ptr<PurgedItemCtx> purgedItemCtx;

    /**
     * Function which tells us that the bucket is shutting down and to stop
     * compacting. Compaction is often a very long running task which would
     * impede shutdown which can cause rebalance failures in ns_server when we
     * hit bucket shutdown timeouts.
     */
    std::function<bool()> isShuttingDown;

private:
    /// Pointer to the in memory vbucket that we're compacting for
    std::weak_ptr<VBucket> vb;
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

    virtual const StatusCallback<GetValue>& getValueCallback() const {
        return *callback;
    }

    virtual StatusCallback<GetValue>& getValueCallback() {
        return *callback;
    }

    virtual const StatusCallback<CacheLookup>& getCacheCallback() const {
        return *lookup;
    }

    virtual StatusCallback<CacheLookup>& getCacheCallback() {
        return *lookup;
    }

    const Vbid vbid;
    int64_t lastReadSeqno{0};
    std::unique_ptr<KVFileHandle> handle;
    const DocumentFilter docFilter;
    const ValueFilter valFilter;
    BucketLogger* logger;
    const Collections::VB::ScanContext collectionsContext;
    int64_t maxSeqno;

    /**
     * Cumulative count of bytes read from disk during this scan. Counts
     * key + meta for each document visited during the scan, plus the value
     * size where the value needed to be read from disk (required and not
     * already present in the cache).
     * For documents whose values are compressed on-disk, we account the
     * compressed size here (given that is the size of data read from disk).
     * Note this stat does _not_ include data which was read as part of
     * finding and reading the data from disk (B-Tree nodes, LSM headers etc).
     */
    size_t diskBytesRead{0};

protected:
    std::unique_ptr<StatusCallback<GetValue>> callback;
    std::unique_ptr<StatusCallback<CacheLookup>> lookup;
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

    bool operator==(const ByIdRange& other) const;
    bool operator!=(const ByIdRange& other) const;
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
    mutable cb::RelaxedAtomic<size_t> numGetFailure;
    cb::RelaxedAtomic<size_t> numSetFailure;
    cb::RelaxedAtomic<size_t> numDelFailure;
    mutable cb::RelaxedAtomic<size_t> numOpenFailure;
    cb::RelaxedAtomic<size_t> numVbSetFailure;

    cb::RelaxedAtomic<size_t> numCompactionAborted;

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

    // Time spent serving a GetKeys operation. Mutable as getAllKeys is
    // logically const
    mutable Hdr1sfMicroSecHistogram getAllKeysHisto;

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

    /**
     * Will the KVStore make callbacks with stale (superseded) items during
     * compaction?
     */
    enum class CompactionStaleItemCallbacks : bool { Yes, No };

    StorageProperties(ByIdScan byIdScan,
                      AutomaticDeduplication automaticDeduplication,
                      PrepareCounting prepareCounting,
                      CompactionStaleItemCallbacks compactionStaleItemCallbacks)
        : byIdScan(byIdScan),
          automaticDeduplication(automaticDeduplication),
          prepareCounting(prepareCounting),
          compactionStaleItemCallbacks(compactionStaleItemCallbacks) {
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

    bool hasCompactionStaleItemCallbacks() const {
        return compactionStaleItemCallbacks ==
               CompactionStaleItemCallbacks::Yes;
    }

private:
    ByIdScan byIdScan;
    AutomaticDeduplication automaticDeduplication;
    PrepareCounting prepareCounting;
    CompactionStaleItemCallbacks compactionStaleItemCallbacks;
};


/**
 * Base class for some KVStores that implements common functionality.
 */
class KVStore : public KVStoreIface {
public:
    ~KVStore() override;

    /**
     * Called when the engine is going away so we can shutdown any backend tasks
     * the underlying store create to prevent them from racing with destruction.
     */
    void deinitialize() override {
    }

    /**
     * Allow the kvstore to add extra statistics information
     * back to the client
     * @param prefix prefix to use for the stats
     * @param add_stat the callback function to add statistics
     * @param c the cookie to pass to the callback function
     */
    void addStats(const AddStatFn& add_stat, const void* c) const override;

    /**
     * Request the specified statistic name from the kvstore.
     *
     * @param name The name of the statistic to fetch.
     * @param[out] value Value of the given stat (if exists).
     * @return True if the stat exists, is of type size_t and was successfully
     *         returned, else false.
     */
    bool getStat(std::string_view name, size_t& value) const override {
        return false;
    }

    /// Request the specified statistics from kvstore.
    ///
    /// @param [in] keys specifies a set of statistics to be fetched.
    /// @return statistic values. Note that the string_view keys in the returned
    /// map refer to the same string keys that the input string_view refers to.
    /// Hence the map is ok to use only as long as the string keys live.
    ///
    GetStatsMap getStats(gsl::span<const std::string_view> keys) const override;

    /**
     * Show kvstore specific timing stats.
     *
     * @param add_stat the callback function to add statistics
     * @param c the cookie to pass to the callback function
     */
    void addTimingStats(const AddStatFn& add_stat,
                        const CookieIface* c) const override;

    /**
     * Resets kvstore specific stats
     */
    void resetStats() override {
        st.reset();
    }

    size_t getMemFootPrint() const override {
        return st.getMemFootPrint();
    }

    /**
     * Needed to prevent the convenience version of get below from hiding the
     * interface version.
     */
    GetValue get(const DiskDocKey& key,
                 Vbid vb,
                 ValueFilter filter) const override = 0;
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
    void setMaxDataSize(size_t size) override {
        // Might be overloaded to do some work
    }

    /**
     * Retrieve multiple documents from the underlying storage system at once.
     *
     * @param vb vbucket id of a document
     * @param itms list of items whose documents are going to be retrieved.
     */
    void getMulti(Vbid vb, vb_bgfetch_queue_t& itms) const override {
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
    void getRange(Vbid vb,
                  const DiskDocKey& startKey,
                  const DiskDocKey& endKey,
                  ValueFilter filter,
                  const GetRangeCb& cb) const override {
        throw std::runtime_error("Backend does not support getRange()");
    }

    nlohmann::json getPersistedStats() const override;

    bool snapshotStats(const nlohmann::json& stats) override;

    /**
     * Abort compaction for the provided vbucket if it is running
     *
     * @param vbLock The lock used to serialize access for compaction and
     *               flusher (should be held when calling the method; added to
     *               the API so that the inner parts can ensure that it is
     *               held).
     * @param vbucket The vbucket of interest
     */
    void abortCompactionIfRunning(std::unique_lock<std::mutex>& vbLock,
                                  Vbid vbid) override{};

    void prepareForDeduplication(std::vector<queued_item>& items) override;

    uint64_t getLastPersistedSeqno(Vbid vbid) override;

    const KVStoreStats& getKVStoreStat() const override {
        return st;
    }

    /// Does the backend support historical snapshots
    bool supportsHistoricalSnapshots() const override {
        return false;
    }

    std::unique_ptr<KVStoreRevision> prepareToDelete(Vbid vbid) override;

    void prepareToCreate(Vbid vbid) override;

    std::unique_ptr<RollbackCtx> prepareToRollback(Vbid vbid) override {
        // Do nothing by default, default ctx is fine
        return std::make_unique<RollbackCtx>();
    }

    void setSystemEvent(TransactionContext& txnCtx, const queued_item) override;

    void delSystemEvent(TransactionContext& txnCtx, const queued_item) override;

    void setMakeCompactionContextCallback(
            MakeCompactionContextCallback cb) override {
        makeCompactionContextCallback = cb;
    }

    /**
     * Set the number of storage threads based on configuration settings
     */
    void setStorageThreads(ThreadPoolConfig::StorageThreadCount num) override {
        // ignored by default
    }

    void setPreFlushHook(std::function<void()> hook) override {
        preFlushHook = hook;
    }

    void setPostFlushHook(std::function<void()> hook) override {
        postFlushHook = hook;
    }

    void setSaveDocsPostWriteDocsHook(std::function<void()> hook) override {
        saveDocsPostWriteDocsHook = hook;
    }

    void endTransaction(Vbid vbid) override;

    /**
     * Validate if the given vBucket is in a transaction
     *
     * @param vbid to check
     * @param caller to print in throw if not in transaction
     * @throws invalid_argument if not in transaction
     */
    void checkIfInTransaction(Vbid vbid, std::string_view caller);

    /**
     * Function inspects the Item for some known issues that may exist in
     * persisted data (possibly from older releases and now present due to
     * upgrade). If an inconsistency is found it will log a fix the Item.
     *
     * @param item [in/out] the Item to check and if needed, fix.
     * @return true if the Item was changed by the function because of an issue
     */
    static bool checkAndFixKVStoreCreatedItem(Item& item);

protected:
    /**
     * Process the vbstate snapshot strings which are store in the vbstate
     * document. Check for validity and return a status + decoded snapshot.
     *
     * @param vb vbid
     * @param state state
     * @return tuple of:
     *     bool - invalid (false) or valid (true)
     *     uint64_t - snapshot start
     *     uint64_t - snapshot end
     */
    std::tuple<bool, uint64_t, uint64_t> processVbstateSnapshot(
            Vbid vb, vbucket_state state) const;

    /// Get a string to use as the prefix for the stats. This is typically
    /// "ro_<shard id>" for the read only store, and "rw_<shard id>" for the
    /// read write store.
    std::string getStatsPrefix() const;

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

    // Test-only. If set, this is executed before the a flush-batch is committed
    // to disk.
    TestingHook<> preFlushHook;

    // Test-only. If set, this is executed after the a flush-batch is committed
    // to disk but before we call back into the PersistenceCallback.
    TestingHook<> postFlushHook;

    // Test-only. Hook which is called in saveDocs after documents have
    // been written to the underlying KVStore, but before any stats updates.
    TestingHook<> saveDocsPostWriteDocsHook;
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
    static std::unique_ptr<KVStoreIface> create(KVStoreConfig& config);
};

/**
 * Callback class used by DcpConsumer, for rollback operation
 */
class RollbackCB : public StatusCallback<GetValue> {
public:
    void callback(GetValue& val) override = 0;

    virtual void setKVFileHandle(std::unique_ptr<KVFileHandle> handle) {
        kvFileHandle = std::move(handle);
    }

    virtual const KVFileHandle* getKVFileHandle() const {
        return kvFileHandle.get();
    }

protected:
    /// The database handle to use when lookup up items in the new, rolled back
    /// database.
    std::unique_ptr<KVFileHandle> kvFileHandle;
};

std::ostream& operator<<(std::ostream& os, const ValueFilter& vf);
std::ostream& operator<<(std::ostream& os, const DocumentFilter& df);
