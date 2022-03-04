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

#include <fcntl.h>
#include <folly/lang/Assume.h>
#include <map>
#include <string>
#include <utility>

#include "collections/vbucket_manifest.h"
#include "common.h"
#include "couch-kvstore/couch-kvstore-config.h"
#include "couch-kvstore/couch-kvstore.h"
#include "item.h"
#include "kvstore/couch-kvstore/couch-kvstore-config.h"
#include "kvstore/couch-kvstore/couch-kvstore.h"
#include "kvstore/nexus-kvstore/nexus-kvstore-config.h"
#include "kvstore/nexus-kvstore/nexus-kvstore.h"
#include "vbucket_state.h"
#ifdef EP_USE_MAGMA
#include "magma-kvstore/magma-kvstore.h"
#include "magma-kvstore/magma-kvstore_config.h"
#endif /* EP_USE_MAGMA */
#ifdef EP_USE_ROCKSDB
#include "rocksdb-kvstore/rocksdb-kvstore.h"
#include "rocksdb-kvstore/rocksdb-kvstore_config.h"
#endif
#include "bucket_logger.h"
#include "kvstore.h"
#include "kvstore_config.h"
#include "persistence_callback.h"
#include "vbucket.h"
#include "vbucket_state.h"

#include <platform/dirutils.h>
#include <statistics/cbstat_collector.h>
#include <sys/stat.h>
#include <sys/types.h>

ScanContext::ScanContext(
        Vbid vbid,
        std::unique_ptr<KVFileHandle> handle,
        DocumentFilter docFilter,
        ValueFilter valFilter,
        std::unique_ptr<StatusCallback<GetValue>> cb,
        std::unique_ptr<StatusCallback<CacheLookup>> cl,
        const std::vector<Collections::KVStore::DroppedCollection>&
                droppedCollections,
        int64_t maxSeqno)
    : vbid(vbid),
      handle(std::move(handle)),
      docFilter(docFilter),
      valFilter(valFilter),
      logger(getGlobalBucketLogger().get()),
      collectionsContext(droppedCollections),
      maxSeqno(maxSeqno),
      callback(std::move(cb)),
      lookup(std::move(cl)) {
    Expects(callback != nullptr);
    Expects(lookup != nullptr);
}

BySeqnoScanContext::BySeqnoScanContext(
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
        std::optional<uint64_t> timestamp)

    : ScanContext(vb,
                  std::move(handle),
                  _docFilter,
                  _valFilter,
                  std::move(cb),
                  std::move(cl),
                  droppedCollections,
                  end),
      startSeqno(start),
      purgeSeqno(purgeSeqno),
      documentCount(_documentCount),
      maxVisibleSeqno(vbucketState.maxVisibleSeqno),
      persistedCompletedSeqno(vbucketState.persistedCompletedSeqno),
      timestamp(std::move(timestamp)) {
}

ByIdScanContext::ByIdScanContext(
        std::unique_ptr<StatusCallback<GetValue>> cb,
        std::unique_ptr<StatusCallback<CacheLookup>> cl,
        Vbid vb,
        std::unique_ptr<KVFileHandle> handle,
        std::vector<ByIdRange> ranges,
        DocumentFilter _docFilter,
        ValueFilter _valFilter,
        const std::vector<Collections::KVStore::DroppedCollection>&
                droppedCollections,
        int64_t maxSeqno,
        uint64_t persistedCompletedSeqno)
    : ScanContext(vb,
                  std::move(handle),
                  _docFilter,
                  _valFilter,
                  std::move(cb),
                  std::move(cl),
                  droppedCollections,
                  maxSeqno),
      ranges(std::move(ranges)),
      lastReadKey(nullptr, 0),
      persistedCompletedSeqno(persistedCompletedSeqno) {
}

void FileStats::reset() {
    readTimeHisto.reset();
    readSeekHisto.reset();
    readSizeHisto.reset();
    writeTimeHisto.reset();
    writeSizeHisto.reset();
    syncTimeHisto.reset();
    readCountHisto.reset();
    writeCountHisto.reset();
    totalBytesRead = 0;
    totalBytesWritten = 0;
}

size_t FileStats::getMemFootPrint() const {
    return readTimeHisto.getMemFootPrint() + readSeekHisto.getMemFootPrint() +
           readSizeHisto.getMemFootPrint() + writeTimeHisto.getMemFootPrint() +
           writeSizeHisto.getMemFootPrint() + syncTimeHisto.getMemFootPrint() +
           readCountHisto.getMemFootPrint() + writeCountHisto.getMemFootPrint();
}

KVStoreStats::KVStoreStats() = default;

void KVStoreStats::reset() {
    docsCommitted = 0;
    numOpen = 0;
    numClose = 0;
    numLoadedVb = 0;

    numGetFailure = 0;
    numSetFailure = 0;
    numDelFailure = 0;
    numOpenFailure = 0;
    numVbSetFailure = 0;

    numCompactionAborted = 0;

    io_bg_fetch_docs_read = 0;
    io_num_write = 0;
    io_bgfetch_doc_bytes = 0;
    io_document_write_bytes = 0;

    readTimeHisto.reset();
    readSizeHisto.reset();
    writeTimeHisto.reset();
    writeSizeHisto.reset();
    delTimeHisto.reset();
    commitHisto.reset();
    compactHisto.reset();
    saveDocsHisto.reset();
    batchSize.reset();
    snapshotHisto.reset();

    getMultiFsReadCount.reset();
    getMultiFsReadHisto.reset();
    getMultiFsReadPerDocHisto.reset();
    flusherWriteAmplificationHisto.reset();
}

std::unique_ptr<KVStoreIface> KVStoreFactory::create(KVStoreConfig& config) {
    // Directory for kvstore files should exist already (see
    // EventuallyPersistentEngine::initialize).
    if (!cb::io::isDirectory(config.getDBName())) {
        throw std::runtime_error(fmt::format(
                "KVStoreFactory ctor: Specified dbname '{}' is not a directory",
                config.getDBName()));
    }

    std::string backend = config.getBackend();
    if (backend == "couchdb") {
        auto rw = std::make_unique<CouchKVStore>(
                dynamic_cast<CouchKVStoreConfig&>(config));
        return std::move(rw);
    } else if (backend == "nexus") {
        auto rw = std::make_unique<NexusKVStore>(
                dynamic_cast<NexusKVStoreConfig&>(config));
        return std::move(rw);
    }
#ifdef EP_USE_MAGMA
    else if (backend == "magma") {
        auto rw = std::make_unique<MagmaKVStore>(
                dynamic_cast<MagmaKVStoreConfig&>(config));
        return std::move(rw);
    }
#endif
#ifdef EP_USE_ROCKSDB
    else if (backend == "rocksdb") {
        auto rw = std::make_unique<RocksDBKVStore>(
                dynamic_cast<RocksDBKVStoreConfig&>(config));
        return std::move(rw);
    }
#endif
    else {
        throw std::invalid_argument("KVStoreFactory::create unknown backend:" +
                                    config.getBackend());
    }

    return {};
}

bool KVStore::needsToBePersisted(Vbid vbid, const vbucket_state& newVbstate) {
    /*
     * The vbucket state information is to be persisted only if there is no
     * cached vbstate (ie, we are writing a vbstate on disk for the first time)
     * or if a change is detected in:
     * - the state
     * - the failover table, or
     * - the replication topology or
     *   (above owned by struct vbucket_transition_state)
     * - the persisted completed seqno or
     * - the persisted prepared seqno or
     * - the high prepared seqno
     */
    const auto* cached = getCachedVBucketState(vbid);

    if (!cached) {
        return true;
    }

    return (cached->transition.needsToBePersisted(newVbstate.transition) ||
            cached->persistedCompletedSeqno !=
                    newVbstate.persistedCompletedSeqno ||
            cached->persistedPreparedSeqno !=
                    newVbstate.persistedPreparedSeqno ||
            cached->highPreparedSeqno != newVbstate.highPreparedSeqno ||
            cached->maxVisibleSeqno != newVbstate.maxVisibleSeqno);
}

void KVStore::updateCachedVBState(Vbid vbid, const vbucket_state& newState) {
    vbucket_state* vbState = getCachedVBucketState(vbid);

    if (!vbState) {
        cachedVBStates[getCacheSlot(vbid)] =
                std::make_unique<vbucket_state>(newState);
        if (cachedVBStates[getCacheSlot(vbid)]->transition.state !=
            vbucket_state_dead) {
            cachedValidVBCount++;
        }
    } else {
        *vbState = newState;
    }
}

bool KVStore::snapshotStats(const nlohmann::json& stats) {
    std::string dbname = getConfig().getDBName();
    std::string next_fname = dbname + "/stats.json.new";

    FILE *new_stats = fopen(next_fname.c_str(), "w");
    if (new_stats == nullptr) {
        EP_LOG_INFO(
                "Failed to open the engine stats "
                "file \"{}\" due to an error \"{}\"; Not critical because new "
                "stats will be dumped later, please ignore.",
                next_fname.c_str(),
                strerror(errno));
        return false;
    }

    bool rv = true;
    if (fprintf(new_stats, "%s\n", stats.dump().c_str()) < 0) {
        EP_LOG_INFO(
                "Failed to write the engine stats to "
                "file \"{}\" due to an error \"{}\"; Not critical because new "
                "stats will be dumped later, please ignore.",
                next_fname.c_str(),
                strerror(errno));
        rv = false;
    }
    fclose(new_stats);

    if (rv) {
        std::string old_fname = dbname + "/stats.json.old";
        std::string stats_fname = dbname + "/stats.json";
        if (cb::io::isFile(old_fname) && remove(old_fname.c_str()) != 0) {
            EP_LOG_WARN(
                    "Failed to remove '{}': {}", old_fname, strerror(errno));
            remove(next_fname.c_str());
            rv = false;
        } else if (cb::io::isFile(stats_fname) &&
                   rename(stats_fname.c_str(), old_fname.c_str()) != 0) {
            EP_LOG_WARN("Failed to rename '{}' to '{}': {}",
                        stats_fname,
                        old_fname,
                        strerror(errno));
            remove(next_fname.c_str());
            rv = false;
        } else if (rename(next_fname.c_str(), stats_fname.c_str()) != 0) {
            EP_LOG_WARN("Failed to rename '{}' to '{}': {}",
                        next_fname,
                        stats_fname,
                        strerror(errno));
            remove(next_fname.c_str());
            rv = false;
        }
    }

    return rv;
}

nlohmann::json KVStore::getPersistedStats() const {
    std::string dbname = getConfig().getDBName();
    const auto fname = cb::io::sanitizePath(dbname + "/stats.json");
    if (!cb::io::isFile(fname)) {
        return {};
    }

    std::string buffer;
    try {
        buffer = cb::io::loadFile(fname);
    } catch (const std::exception& exception) {
        EP_LOG_WARN(
                "KVStore::getPersistedStats: Failed to load the engine "
                "session stats due to IO exception \"{}\"",
                exception.what());
        return {};
    }

    nlohmann::json json;
    try {
        json = nlohmann::json::parse(buffer);
    } catch (const nlohmann::json::exception& exception) {
        EP_LOG_WARN(
                "KVStore::getPersistedStats:"
                " Failed to parse the session stats json doc!!!: \"{}\"",
                exception.what());
        return {};
    }

    return json;
}

KVStore::~KVStore() = default;

std::string KVStore::getStatsPrefix() const {
    const auto shardId = getConfig().getShardId();
    return "rw_" + std::to_string(shardId);
}

GetStatsMap KVStore::getStats(gsl::span<const std::string_view> keys) const {
    GetStatsMap stats;
    for (const auto& key : keys) {
        size_t value;
        if (getStat(key, value)) {
            stats.try_emplace(key, value);
        }
    }
    return stats;
}

void KVStore::addStats(const AddStatFn& add_stat, const void* c) const {
    const char* backend = getConfig().getBackend().c_str();
    const auto prefix = getStatsPrefix();

    /* stats for both read-only and read-write threads */
    add_prefixed_stat(prefix, "backend_type", backend, add_stat, c);
    add_prefixed_stat(prefix, "open", st.numOpen, add_stat, c);
    add_prefixed_stat(prefix, "close", st.numClose, add_stat, c);
    add_prefixed_stat(prefix, "numLoadedVb", st.numLoadedVb, add_stat, c);

    // failure stats
    add_prefixed_stat(prefix, "failure_open", st.numOpenFailure, add_stat, c);
    add_prefixed_stat(prefix, "failure_get", st.numGetFailure, add_stat, c);

    add_prefixed_stat(prefix, "failure_set", st.numSetFailure, add_stat, c);
    add_prefixed_stat(prefix, "failure_del", st.numDelFailure, add_stat, c);
    add_prefixed_stat(prefix, "failure_vbset", st.numVbSetFailure, add_stat, c);
    add_prefixed_stat(prefix, "lastCommDocs", st.docsCommitted, add_stat, c);

    add_prefixed_stat(prefix,
                      "io_bg_fetch_docs_read",
                      st.io_bg_fetch_docs_read,
                      add_stat,
                      c);
    add_prefixed_stat(prefix, "io_num_write", st.io_num_write, add_stat, c);
    add_prefixed_stat(prefix,
                      "io_bg_fetch_doc_bytes",
                      st.io_bgfetch_doc_bytes,
                      add_stat,
                      c);
    add_prefixed_stat(prefix,
                      "io_document_write_bytes",
                      st.io_document_write_bytes,
                      add_stat,
                      c);
}

void KVStore::addTimingStats(const AddStatFn& add_stat,
                             const CookieIface* c) const {
    const auto prefix = getStatsPrefix();

    add_prefixed_stat(prefix, "commit", st.commitHisto, add_stat, c);
    add_prefixed_stat(prefix, "compact", st.compactHisto, add_stat, c);
    add_prefixed_stat(prefix, "snapshot", st.snapshotHisto, add_stat, c);
    add_prefixed_stat(prefix, "delete", st.delTimeHisto, add_stat, c);
    add_prefixed_stat(prefix, "save_documents", st.saveDocsHisto, add_stat, c);
    add_prefixed_stat(prefix, "readTime", st.readTimeHisto, add_stat, c);
    add_prefixed_stat(prefix, "readSize", st.readSizeHisto, add_stat, c);
    add_prefixed_stat(prefix, "writeTime", st.writeTimeHisto, add_stat, c);
    add_prefixed_stat(prefix, "writeSize", st.writeSizeHisto, add_stat, c);
    add_prefixed_stat(prefix, "saveDocCount", st.batchSize, add_stat, c);

    add_prefixed_stat(
            prefix, "getMultiFsReadCount", st.getMultiFsReadHisto, add_stat, c);
    add_prefixed_stat(prefix,
                      "getMultiFsReadPerDocCount",
                      st.getMultiFsReadPerDocHisto,
                      add_stat,
                      c);
    add_prefixed_stat(prefix,
                      "flusherWriteAmplificationRatio",
                      st.flusherWriteAmplificationHisto,
                      add_stat,
                      c);

    add_prefixed_stat(prefix, "getAllKeys", st.getAllKeysHisto, add_stat, c);
}

void KVStore::prepareForDeduplication(std::vector<queued_item>& items) {
    if (items.empty()) {
        return;
    }

    OrderItemsForDeDuplication cq;
    std::sort(items.begin(), items.end(), cq);
}

uint64_t KVStore::getLastPersistedSeqno(Vbid vbid) {
    vbucket_state* state = getCachedVBucketState(vbid);
    if (state) {
        return state->highSeqno;
    }
    return 0;
}

std::unique_ptr<KVStoreRevision> KVStore::prepareToDelete(Vbid vbid) {
    // MB-34380: We must clear the cached state
    resetCachedVBState(vbid);
    return prepareToDeleteImpl(vbid);
}

void KVStore::prepareToCreate(Vbid vbid) {
    resetCachedVBState(vbid);
    prepareToCreateImpl(vbid);
}

void KVStore::resetCachedVBState(Vbid vbid) {
    vbucket_state* state = getCachedVBucketState(vbid);
    if (state) {
        state->reset();
    }
}

void KVStore::setSystemEvent(TransactionContext& txnCtx,
                             const queued_item item) {
    set(txnCtx, item);
}

void KVStore::delSystemEvent(TransactionContext& txnCtx,
                             const queued_item item) {
    del(txnCtx, item);
}

std::string to_string(FlushStateDeletion state) {
    switch (state) {
    case FlushStateDeletion::Delete:
        return "FlushStateDeletion::Delete";
    case FlushStateDeletion::LogicallyDocNotFound:
        return "FlushStateDeletion::LogicallyDocNotFound";
    case FlushStateDeletion::DocNotFound:
        return "FlushStateDeletion::DocNotFound";
    case FlushStateDeletion::Failed:
        return "FlushStateDeletion::Failed";
    }
    folly::assume_unreachable();
}

std::string to_string(FlushStateMutation state) {
    switch (state) {
    case FlushStateMutation::Failed:
        return "FlushStateMutation::Failed";
    case FlushStateMutation::Insert:
        return "FlushStateMutation::Insert";
    case FlushStateMutation::LogicalInsert:
        return "FlushStateMutation::LogicalInsert";
    case FlushStateMutation::Update:
        return "FlushStateMutation::Update";
    }
    folly::assume_unreachable();
}

IORequest::IORequest(queued_item itm)
    : item(std::move(itm)),
      key(DiskDocKey(*item)),
      start(std::chrono::steady_clock::now()) {
}

IORequest::~IORequest() = default;

bool IORequest::isDelete() const {
    return item->isDeleted() && !item->isPending();
}

CompactionConfig::CompactionConfig(CompactionConfig&& other) {
    *this = std::move(other);
}

CompactionConfig& CompactionConfig::operator=(CompactionConfig&& other) {
    if (this != &other) {
        purge_before_ts = other.purge_before_ts;
        purge_before_seq = other.purge_before_seq;
        drop_deletes = other.drop_deletes;
        retain_erroneous_tombstones = other.retain_erroneous_tombstones;
        internally_requested = other.internally_requested;
        other.purge_before_ts = 0;
        other.purge_before_seq = 0;
        other.drop_deletes = false;
        other.retain_erroneous_tombstones = false;
        other.internally_requested = false;
    }
    return *this;
}

bool CompactionConfig::operator==(const CompactionConfig& other) const {
    return purge_before_ts == other.purge_before_ts &&
           purge_before_seq == other.purge_before_seq &&
           drop_deletes == other.drop_deletes &&
           retain_erroneous_tombstones == other.retain_erroneous_tombstones &&
           internally_requested == other.internally_requested;
}

// Merge other into this.
// drop_deletes, retain_erroneous_tombstones and internally_requested are
// 'sticky', once true they stay true for all subsequent merges.
// purge_before_ts and purge_before_seq should be the largest value out of
// the current and the input
void CompactionConfig::merge(const CompactionConfig& other) {
    retain_erroneous_tombstones =
            retain_erroneous_tombstones || other.retain_erroneous_tombstones;
    drop_deletes = drop_deletes || other.drop_deletes;
    internally_requested = internally_requested || other.internally_requested;
    purge_before_ts =
            std::max<uint64_t>(purge_before_ts, other.purge_before_ts);
    purge_before_seq =
            std::max<uint64_t>(purge_before_seq, other.purge_before_seq);
}

Vbid::id_type KVStore::getCacheSlot(Vbid vbid) const {
    return vbid.get() / getConfig().getMaxShards();
}

size_t KVStore::getCacheSize() const {
    return getConfig().getCacheSize();
}

bool KVStore::startTransaction(Vbid vbid) {
    auto& vbInTransaction = inTransaction[getCacheSlot(vbid)];
    bool expected = false;
    if (!vbInTransaction.compare_exchange_strong(expected, true)) {
        // Some other thread is already in a transaction against this vBucket.
        return false;
    }

    return true;
}

void KVStore::endTransaction(Vbid vbid) {
    auto& vbInTransaction = inTransaction[getCacheSlot(vbid)];
    vbInTransaction = false;
}

void KVStore::checkIfInTransaction(Vbid vbid, std::string_view caller) {
    if (!inTransaction[getCacheSlot(vbid)]) {
        throw std::invalid_argument(fmt::format(
                "{} inTransaction {} must be true to perform this operation.",
                caller,
                vbid));
    }
}

std::tuple<bool, uint64_t, uint64_t> KVStore::processVbstateSnapshot(
        Vbid vb, vbucket_state vbState) const {
    bool status = true;
    uint64_t snapStart = vbState.lastSnapStart;
    uint64_t snapEnd = vbState.lastSnapEnd;
    uint64_t highSeqno = vbState.highSeqno;

    // All upgrade paths we now expect start and end
    if (!(highSeqno >= snapStart && highSeqno <= snapEnd)) {
        // very likely MB-34173, log this occurrence.
        // log the state, range and version
        EP_LOG_WARN(
                "KVStore::processVbstateSnapshot {} {} with invalid snapshot "
                "range. Found version:{}, highSeqno:{}, start:{}, end:{}",
                vb,
                VBucket::toString(vbState.transition.state),
                vbState.version,
                highSeqno,
                snapStart,
                snapEnd);

        if (vbState.transition.state == vbucket_state_active) {
            // Reset the snapshot range to match what the flusher would
            // normally set, that is start and end equal the high-seqno
            snapStart = snapEnd = highSeqno;
        } else {
            // Flag that the VB is corrupt, it needs rebuilding
            status = false;
            snapStart = 0, snapEnd = 0;
        }
    }

    return {status, snapStart, snapEnd};
}

std::ostream& operator<<(std::ostream& os, const ValueFilter& vf) {
    switch (vf) {
    case ValueFilter::KEYS_ONLY:
        os << "KEYS_ONLY";
        break;
    case ValueFilter::VALUES_COMPRESSED:
        os << "VALUES_COMPRESSED";
        break;
    case ValueFilter::VALUES_DECOMPRESSED:
        os << "VALUES_DECOMPRESSED";
        break;
    default:
        os << "INVALID ValueFilter value:" +
                        std::to_string(static_cast<uint64_t>(vf));
        break;
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, const DocumentFilter& df) {
    switch (df) {
    case DocumentFilter::ALL_ITEMS:
        os << "ALL_ITEMS";
        break;
    case DocumentFilter::NO_DELETES:
        os << "NO_DELETES";
        break;
    case DocumentFilter::ALL_ITEMS_AND_DROPPED_COLLECTIONS:
        os << "ALL_ITEMS_AND_DROPPED_COLLECTIONS";
        break;
    default:
        os << "INVALID ValueFilter value:" +
                        std::to_string(static_cast<uint64_t>(df));
        break;
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, ScanStatus status) {
    switch (status) {
    case ScanStatus::Success:
        return os << "ScanStatus::Success";
    case ScanStatus::Yield:
        return os << "ScanStatus::Yield";
    case ScanStatus::Cancelled:
        return os << "ScanStatus::Cancelled";
    case ScanStatus::Failed:
        return os << "ScanStatus::Failed";
    }
    folly::assume_unreachable();
}