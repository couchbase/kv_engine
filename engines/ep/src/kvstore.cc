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
      callback(std::move(cb)),
      lookup(std::move(cl)),
      logger(getGlobalBucketLogger().get()),
      collectionsContext(droppedCollections),
      maxSeqno(maxSeqno) {
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
        int64_t maxSeqno)
    : ScanContext(vb,
                  std::move(handle),
                  _docFilter,
                  _valFilter,
                  std::move(cb),
                  std::move(cl),
                  droppedCollections,
                  maxSeqno),
      ranges(std::move(ranges)),
      lastReadKey(nullptr, 0) {
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

    numCompactionFailure = 0;
    numGetFailure = 0;
    numSetFailure = 0;
    numDelFailure = 0;
    numOpenFailure = 0;
    numVbSetFailure = 0;

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

    fsStats.reset();
    fsStatsCompaction.reset();
}

KVStoreRWRO KVStoreFactory::create(KVStoreConfig& config) {
    std::string backend = config.getBackend();
    if (backend == "couchdb") {
        auto rw = std::make_unique<CouchKVStore>(
                dynamic_cast<CouchKVStoreConfig&>(config));
        auto ro = rw->makeReadOnlyStore();
        return {rw.release(), ro.release()};
    }
#ifdef EP_USE_MAGMA
    else if (backend == "magma") {
        auto rw = std::make_unique<MagmaKVStore>(
                dynamic_cast<MagmaKVStoreConfig&>(config));
        return {rw.release(), nullptr};
    }
#endif
#ifdef EP_USE_ROCKSDB
    else if (backend == "rocksdb") {
        auto rw = std::make_unique<RocksDBKVStore>(
                dynamic_cast<RocksDBKVStoreConfig&>(config));
        return {rw.release(), nullptr};
    }
#endif
    else {
        throw std::invalid_argument("KVStoreFactory::create unknown backend:" +
                                    config.getBackend());
    }

    return {};
}

void KVStore::createDataDir(const std::string& dbname) {
    try {
        cb::io::mkdirp(dbname);
    } catch (const std::system_error& error) {
        std::stringstream ss;
        ss << "Failed to create data directory ["
           << dbname << "]: " << error.code().message();
        throw std::runtime_error(ss.str());
    }
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
        cachedVBStates[vbid.get()] = std::make_unique<vbucket_state>(newState);
        if (cachedVBStates[vbid.get()]->transition.state !=
            vbucket_state_dead) {
            cachedValidVBCount++;
        }
    } else {
        *vbState = newState;
    }
}

bool KVStore::snapshotStats(const nlohmann::json& stats) {
    if (isReadOnly()) {
        throw std::logic_error("KVStore::snapshotStats: Cannot perform "
                        "on a read-only instance.");
    }

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
        return nlohmann::json();
    }

    std::string buffer;
    try {
        buffer = cb::io::loadFile(fname);
    } catch (const std::exception& exception) {
        EP_LOG_WARN(
                "KVStore::getPersistedStats: Failed to load the engine "
                "session stats due to IO exception \"{}\"",
                exception.what());
        return nlohmann::json();
    }

    nlohmann::json json;
    try {
        json = nlohmann::json::parse(buffer);
    } catch (const nlohmann::json::exception& exception) {
        EP_LOG_WARN(
                "KVStore::getPersistedStats:"
                " Failed to parse the session stats json doc!!!: \"{}\"",
                exception.what());
        return nlohmann::json();
    }

    return json;
}

KVStore::KVStore(bool read_only) : readOnly(read_only) {
}

KVStore::~KVStore() = default;

std::string KVStore::getStatsPrefix() const {
    const auto shardId = getConfig().getShardId();
    if (isReadOnly()) {
        return "ro_" + std::to_string(shardId);
    }
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

void KVStore::addStats(const AddStatFn& add_stat,
                       const void* c,
                       const std::string& args) const {
    const char* backend = getConfig().getBackend().c_str();
    const auto prefix = getStatsPrefix();

    /* stats for both read-only and read-write threads */
    add_prefixed_stat(prefix, "backend_type", backend, add_stat, c);
    add_prefixed_stat(prefix, "open", st.numOpen, add_stat, c);
    add_prefixed_stat(prefix, "close", st.numClose, add_stat, c);
    add_prefixed_stat(prefix, "numLoadedVb", st.numLoadedVb, add_stat, c);

    // failure stats
    add_prefixed_stat(
            prefix, "failure_compaction", st.numCompactionFailure, add_stat, c);
    add_prefixed_stat(prefix, "failure_open", st.numOpenFailure, add_stat, c);
    add_prefixed_stat(prefix, "failure_get", st.numGetFailure, add_stat, c);

    if (!isReadOnly()) {
        add_prefixed_stat(prefix, "failure_set", st.numSetFailure, add_stat, c);
        add_prefixed_stat(prefix, "failure_del", st.numDelFailure, add_stat, c);
        add_prefixed_stat(
                prefix, "failure_vbset", st.numVbSetFailure, add_stat, c);
        add_prefixed_stat(
                prefix, "lastCommDocs", st.docsCommitted, add_stat, c);
    }

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

    const size_t read = st.fsStats.totalBytesRead.load() +
                        st.fsStatsCompaction.totalBytesRead.load();
    add_prefixed_stat(prefix, "io_total_read_bytes", read, add_stat, c);

    const size_t written = st.fsStats.totalBytesWritten.load() +
                           st.fsStatsCompaction.totalBytesWritten.load();
    add_prefixed_stat(prefix, "io_total_write_bytes", written, add_stat, c);

    if (!isReadOnly()) {
        // Flusher Write Amplification - ratio of bytes written to disk by
        // flusher to "useful" user data written - i.e. doesn't include bytes
        // written later by compaction (after initial flush). Used to measure
        // the impact of KVstore on persistTo times.
        const double flusherWriteAmp =
                double(st.fsStats.totalBytesWritten.load()) /
                st.io_document_write_bytes;
        add_prefixed_stat(prefix,
                          "io_flusher_write_amplification",
                          flusherWriteAmp,
                          add_stat,
                          c);

        // Total Write Amplification - ratio of total bytes written to disk
        // to "useful" user data written over entire disk lifecycle. Includes
        // bytes during initial item flush to disk  and compaction.
        // Used to measure the overall write amplification.
        const double totalWriteAmp =
                double(written) / st.io_document_write_bytes;
        add_prefixed_stat(prefix,
                          "io_total_write_amplification",
                          totalWriteAmp,
                          add_stat,
                          c);
    }

    add_prefixed_stat(prefix,
                      "io_compaction_read_bytes",
                      st.fsStatsCompaction.totalBytesRead,
                      add_stat,
                      c);
    add_prefixed_stat(prefix,
                      "io_compaction_write_bytes",
                      st.fsStatsCompaction.totalBytesWritten,
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

    //file ops stats
    add_prefixed_stat(
            prefix, "fsReadTime", st.fsStats.readTimeHisto, add_stat, c);
    add_prefixed_stat(
            prefix, "fsWriteTime", st.fsStats.writeTimeHisto, add_stat, c);
    add_prefixed_stat(
            prefix, "fsSyncTime", st.fsStats.syncTimeHisto, add_stat, c);
    add_prefixed_stat(
            prefix, "fsReadSize", st.fsStats.readSizeHisto, add_stat, c);
    add_prefixed_stat(
            prefix, "fsWriteSize", st.fsStats.writeSizeHisto, add_stat, c);
    add_prefixed_stat(
            prefix, "fsReadSeek", st.fsStats.readSeekHisto, add_stat, c);
    add_prefixed_stat(
            prefix, "fsReadCount", st.fsStats.readCountHisto, add_stat, c);
    add_prefixed_stat(
            prefix, "fsWriteCount", st.fsStats.writeCountHisto, add_stat, c);
}

void KVStore::optimizeWrites(std::vector<queued_item>& items) {
    if (isReadOnly()) {
        throw std::logic_error(
                "KVStore::optimizeWrites: Not valid on a "
                "read-only object");
    }
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

uint64_t KVStore::prepareToDelete(Vbid vbid) {
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

void KVStore::setSystemEvent(const queued_item item) {
    set(item);
}

void KVStore::delSystemEvent(const queued_item item) {
    del(item);
}

bool KVStore::begin(std::unique_ptr<TransactionContext> txCtx) {
    if (!txCtx) {
        throw std::invalid_argument("KVStore::begin: txCtx is null");
    }
    if (isReadOnly()) {
        throw std::logic_error(
                "KVStore::begin: Not valid on a read-only object.");
    }
    inTransaction = true;
    transactionCtx = std::move(txCtx);
    return inTransaction;
}

std::string to_string(KVStore::FlushStateDeletion state) {
    switch (state) {
    case KVStore::FlushStateDeletion::Delete:
        return "FlushStateDeletion::Delete";
    case KVStore::FlushStateDeletion::DocNotFound:
        return "FlushStateDeletion::DocNotFound";
    case KVStore::FlushStateDeletion::Failed:
        return "FlushStateDeletion::Failed";
    }
    folly::assume_unreachable();
}

std::string to_string(KVStore::FlushStateMutation state) {
    switch (state) {
    case KVStore::FlushStateMutation::Failed:
        return "FlushStateMutation::Failed";
    case KVStore::FlushStateMutation::Insert:
        return "FlushStateMutation::Insert";
    case KVStore::FlushStateMutation::Update:
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
        other.purge_before_ts = 0;
        other.purge_before_seq = 0;
        other.drop_deletes = false;
        other.retain_erroneous_tombstones = false;
    }
    return *this;
}

bool CompactionConfig::operator==(const CompactionConfig& other) const {
    return purge_before_ts == other.purge_before_ts &&
           purge_before_seq == other.purge_before_seq &&
           drop_deletes == other.drop_deletes &&
           retain_erroneous_tombstones == other.retain_erroneous_tombstones;
}

// Merge other into this.
// drop_deletes and retain_erroneous_tombstones are 'sticky' once true
// they should stay true for all subsequent merges.
// purge_before_ts and purge_before_seq should be the largest value out of
// the current and the input
void CompactionConfig::merge(const CompactionConfig& other) {
    retain_erroneous_tombstones =
            retain_erroneous_tombstones || other.retain_erroneous_tombstones;
    drop_deletes = drop_deletes || other.drop_deletes;
    purge_before_ts =
            std::max<uint64_t>(purge_before_ts, other.purge_before_ts);
    purge_before_seq =
            std::max<uint64_t>(purge_before_seq, other.purge_before_seq);
}
