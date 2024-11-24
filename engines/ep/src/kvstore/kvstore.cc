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

#include "bucket_logger.h"
#include "collections/vbucket_manifest.h"
#include "couch-kvstore/couch-kvstore-config.h"
#include "couch-kvstore/couch-kvstore.h"
#include "file_ops_tracker.h"
#include "item.h"
#include "kvstore.h"
#include "kvstore/couch-kvstore/couch-kvstore-config.h"
#include "kvstore/couch-kvstore/couch-kvstore.h"
#include "kvstore/nexus-kvstore/nexus-kvstore-config.h"
#include "kvstore/nexus-kvstore/nexus-kvstore.h"
#include "kvstore_config.h"
#include "mcbp/protocol/datatype.h"
#include "persistence_callback.h"
#include "vbucket.h"
#include "vbucket_state.h"

#ifdef EP_USE_MAGMA
#include "magma-kvstore/magma-kvstore.h"
#include "magma-kvstore/magma-kvstore_config.h"
#endif /* EP_USE_MAGMA */

#include <cbcrypto/digest.h>
#include <platform/dirutils.h>
#include <platform/uuid.h>
#include <statistics/cbstat_collector.h>
#include <sys/stat.h>
#include <utilities/logtags.h>

ScanContext::ScanContext(
        Vbid vbid,
        std::unique_ptr<KVFileHandle> handle,
        DocumentFilter docFilter,
        ValueFilter valFilter,
        std::unique_ptr<StatusCallback<GetValue>> cb,
        std::unique_ptr<StatusCallback<CacheLookup>> cl,
        const std::vector<Collections::KVStore::OpenCollection>*
                openCollections,
        const std::vector<Collections::KVStore::DroppedCollection>&
                droppedCollections,
        uint64_t maxSeqno,
        uint64_t historyStartSeqno)
    : vbid(vbid),
      handle(std::move(handle)),
      docFilter(docFilter),
      valFilter(valFilter),
      logger(getGlobalBucketLogger().get()),
      collectionsContext(openCollections, droppedCollections),
      maxSeqno(maxSeqno),
      historyStartSeqno(historyStartSeqno),
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
        uint64_t start,
        uint64_t end,
        uint64_t purgeSeqno,
        DocumentFilter _docFilter,
        ValueFilter _valFilter,
        uint64_t _documentCount,
        const vbucket_state& vbucketState,
        const std::vector<Collections::KVStore::OpenCollection>*
                openCollections,
        const std::vector<Collections::KVStore::DroppedCollection>&
                droppedCollections,
        uint64_t historyStartSeqno)

    : ScanContext(vb,
                  std::move(handle),
                  _docFilter,
                  _valFilter,
                  std::move(cb),
                  std::move(cl),
                  openCollections,
                  droppedCollections,
                  end,
                  historyStartSeqno),
      startSeqno(start),
      purgeSeqno(purgeSeqno),
      documentCount(_documentCount),
      maxVisibleSeqno(vbucketState.maxVisibleSeqno),
      persistedCompletedSeqno(vbucketState.persistedCompletedSeqno) {
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
        uint64_t maxSeqno,
        uint64_t historyStartSeqno)
    : ScanContext(vb,
                  std::move(handle),
                  _docFilter,
                  _valFilter,
                  std::move(cb),
                  std::move(cl),
                  nullptr, // no open collections required on this path
                  droppedCollections,
                  maxSeqno,
                  historyStartSeqno),
      ranges(std::move(ranges)),
      resumeFromKey(nullptr, 0) {
}

bool ByIdRange::operator==(const ByIdRange& other) const {
    return startKey == other.startKey && endKey == other.endKey &&
           rangeScanSuccess == other.rangeScanSuccess;
}

bool ByIdRange::operator!=(const ByIdRange& other) const {
    return !(*this == other);
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

std::unique_ptr<KVStoreIface> KVStoreFactory::create(
        KVStoreConfig& config, EncryptionKeyProvider* encryptionKeyProvider) {
    // Directory for kvstore files should exist already (see
    // EventuallyPersistentEngine::initialize).
    if (!cb::io::isDirectory(config.getDBName())) {
        throw std::runtime_error(fmt::format(
                "KVStoreFactory ctor: Specified dbname '{}' is not a directory",
                config.getDBName()));
    }

    const auto backend = config.getBackendString();
    if (backend == "couchdb") {
        return std::make_unique<CouchKVStore>(
                dynamic_cast<CouchKVStoreConfig&>(config),
                encryptionKeyProvider);
    }

    if (backend == "nexus") {
        return std::make_unique<NexusKVStore>(
                dynamic_cast<NexusKVStoreConfig&>(config),
                encryptionKeyProvider);
    }

#ifdef EP_USE_MAGMA
    if (backend == "magma") {
        return std::make_unique<MagmaKVStore>(
                dynamic_cast<MagmaKVStoreConfig&>(config),
                encryptionKeyProvider);
    }
#endif

    throw std::invalid_argument(
            fmt::format("KVStoreFactory::create: unknown backend {}",
                        config.getBackendString()));
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
     * - the max visible seqno
     * - the max cas
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
            cached->maxVisibleSeqno != newVbstate.maxVisibleSeqno ||
            cached->maxCas != newVbstate.maxCas);
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

KVStore::KVStore() : fileOpsTracker(FileOpsTracker::instance()) {
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

void KVStore::addStats(const AddStatFn& add_stat, CookieIface& c) const {
    const char* backend = getConfig().getBackendString().c_str();
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

void KVStore::addTimingStats(const AddStatFn& add_stat, CookieIface& c) const {
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

GetValue KVStore::get(const DiskDocKey& key, Vbid vb) const {
    return get(key, vb, ValueFilter::VALUES_DECOMPRESSED);
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
std::string format_as(const FlushStateDeletion& fsd) {
    switch (fsd) {
    case FlushStateDeletion::Delete:
        return "FlushStateDeletion::Delete";
    case FlushStateDeletion::LogicallyDocNotFound:
        return "FlushStateDeletion::LogicallyDocNotFound";
    case FlushStateDeletion::DocNotFound:
        return "FlushStateDeletion::DocNotFound";
    case FlushStateDeletion::Failed:
        return "FlushStateDeletion::Failed";
    }
    return fmt::format("INVALID FlushStateDeletion value: {}",
                       static_cast<int>(fsd));
}

std::ostream& operator<<(std::ostream& os, const FlushStateDeletion& state) {
    return os << format_as(state);
}

std::string format_as(const FlushStateMutation& fsm) {
    switch (fsm) {
    case FlushStateMutation::Failed:
        return "FlushStateMutation::Failed";
    case FlushStateMutation::Insert:
        return "FlushStateMutation::Insert";
    case FlushStateMutation::LogicalInsert:
        return "FlushStateMutation::LogicalInsert";
    case FlushStateMutation::Update:
        return "FlushStateMutation::Update";
    }
    return fmt::format("INVALID FlushStateMutation value: {}",
                       static_cast<int>(fsm));
}

std::ostream& operator<<(std::ostream& os, const FlushStateMutation& state) {
    return os << format_as(state);
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
        obsolete_keys = other.obsolete_keys;
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
           internally_requested == other.internally_requested &&
           obsolete_keys == other.obsolete_keys;
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

    for (const auto& k : other.obsolete_keys) {
        if (std::find(obsolete_keys.begin(), obsolete_keys.end(), k) ==
            obsolete_keys.end()) {
            obsolete_keys.push_back(k);
        }
    }
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

bool KVStore::isDocumentPotentiallyCorruptedByMB52793(
        bool deleted, protocol_binary_datatype_t datatype) {
    // As per MB-52793, Deleted documents with a zero length value can
    // incorrectly end up with the datatype set to XATTR (when it should be
    // RAW_BYTES), if it was previously a deleted document /with/ a value of
    // system XATTRs.
    // To be able to fixup such documents we need to know more than just the
    // metadata, as the value size is stored as part of the value. As such;
    // return true if it meets all the criteria which metadata informs us of.
    // Handle case where datatype is xattr and snappy since Magma per-doc
    // compression adds snappy datatype.
    return deleted && cb::mcbp::datatype::is_xattr(datatype);
}

// MB-51373: Fix the datatype of invalid documents. Currently checks for
// datatype ! raw but no value, this invalid document has been seen in
// production deployments and reading them can lead to a restart
bool KVStore::checkAndFixKVStoreCreatedItem(Item& item) {
#if CB_DEVELOPMENT_ASSERTS
    if (item.isDeleted() &&
        item.getDataType() == PROTOCOL_BINARY_DATATYPE_XATTR) {
        // If we encounter a potential invalid doc (MB-51373) - Delete with
        // datatype XATTR, we should have its value to be able to verify it
        // is correct, or otherwise sanitise it.
        Expects(item.getValue());
    }
#endif
    if (item.isDeleted() && item.getValue() && item.getNBytes() == 0 &&
        item.getDataType() != PROTOCOL_BINARY_RAW_BYTES) {
        std::stringstream ss;
        ss << item;
        EP_LOG_WARN(
                "KVStore::checkAndFixKVStoreCreatedItem: {} correcting invalid "
                "datatype {}",
                item.getVBucketId(),
                cb::UserDataView(ss.str()).getSanitizedValue());
        item.setDataType(PROTOCOL_BINARY_RAW_BYTES);
        return true;
    }
    return false;
}

static std::string calculateSha512sum(const std::filesystem::path& path,
                                      std::size_t size) {
    try {
        return cb::crypto::sha512sum(path, size);
    } catch (const std::exception& e) {
        EP_LOG_WARN_CTX("Failed calculating sha512",
                        {"path", path.string()},
                        {"size", size},
                        {"error", e.what()});
    }
    return {};
}
static void maybeUpdateSha512Sum(const std::filesystem::path& root,
                                 cb::snapshot::FileInfo& info) {
    if (info.sha512.empty()) {
        auto sum = calculateSha512sum(root / info.path, info.size);
        if (!sum.empty()) {
            info.sha512 = sum;
        }
    }
}

std::variant<cb::engine_errc, cb::snapshot::Manifest> KVStore::prepareSnapshot(
        const std::filesystem::path& path, Vbid vbid) {
    // Generate a path/uuid for the snapshot
    auto uuid = ::to_string(cb::uuid::random());
    const auto snapshotPath = path / uuid;
    create_directories(snapshotPath);

    auto removePath = folly::makeGuard([&snapshotPath] {
        std::error_code ec;
        remove_all(snapshotPath, ec);
    });

    // Call implementation to get backend specifc snapshot prepared
    auto prepared = prepareSnapshotImpl(snapshotPath, vbid, uuid);
    if (std::holds_alternative<cb::engine_errc>(prepared)) {
        return std::get<cb::engine_errc>(prepared);
    }
    auto& manifest = std::get<cb::snapshot::Manifest>(prepared);

    // Add checksums for all files in the snapshot
    for (auto& file : manifest.files) {
        maybeUpdateSha512Sum(path / manifest.uuid, file);
    }
    for (auto& file : manifest.deks) {
        maybeUpdateSha512Sum(path / manifest.uuid, file);
    }

    auto* fp = fopen((snapshotPath / "manifest.json").string().c_str(), "w");
    if (fp) {
        const auto s1 =
                fprintf(fp, "%s\n", nlohmann::json(manifest).dump().c_str());
        const auto s2 = fclose(fp);
        if (s1 > 0 && s2 == 0) {
            // Success - remove the clean-up guard and return the manifest
            removePath.dismiss();
            return manifest;
        }
        EP_LOG_WARN_CTX("prepareSnapshot fprintf/fclose fail for manifest.json",
                        {"errno", errno},
                        {"fprintf", s1},
                        {"fclose", s2},
                        {"vb", vbid},
                        {"path", snapshotPath});
    } else {
        EP_LOG_WARN_CTX("prepareSnapshot fopen fail for manifest.json",
                        {"errno", errno},
                        {"vb", vbid},
                        {"path", snapshotPath});
    }
    return cb::engine_errc::failed;
}

std::string format_as(const ValueFilter vf) {
    switch (vf) {
    case ValueFilter::KEYS_ONLY:
        return "KEYS_ONLY";
    case ValueFilter::VALUES_COMPRESSED:
        return "VALUES_COMPRESSED";
    case ValueFilter::VALUES_DECOMPRESSED:
        return "VALUES_DECOMPRESSED";
    }
    return fmt::format("INVALID ValueFilter value:{}",
                       static_cast<uint64_t>(vf));
}

std::ostream& operator<<(std::ostream& os, const ValueFilter& vf) {
    return os << format_as(vf);
}

std::string format_as(const DocumentFilter df) {
    switch (df) {
    case DocumentFilter::ALL_ITEMS:
        return "ALL_ITEMS";
    case DocumentFilter::NO_DELETES:
        return "NO_DELETES";
    case DocumentFilter::ALL_ITEMS_AND_DROPPED_COLLECTIONS:
        return "ALL_ITEMS_AND_DROPPED_COLLECTIONS";
    }
    return fmt::format("INVALID ValueFilter value:{}",
                       static_cast<uint64_t>(df));
}

std::ostream& operator<<(std::ostream& os, const DocumentFilter& df) {
    return os << format_as(df);
}

std::string format_as(ScanStatus status) {
    switch (status) {
    case ScanStatus::Success:
        return "ScanStatus::Success";
    case ScanStatus::Yield:
        return "ScanStatus::Yield";
    case ScanStatus::Cancelled:
        return "ScanStatus::Cancelled";
    case ScanStatus::Failed:
        return "ScanStatus::Failed";
    }
    folly::assume_unreachable();
}

std::ostream& operator<<(std::ostream& os, ScanStatus status) {
    return os << format_as(status);
}

std::string to_string(KVStoreIface::ReadVBStateStatus status) {
    switch (status) {
    case KVStoreIface::ReadVBStateStatus::Success:
        return "Success";
    case KVStoreIface::ReadVBStateStatus::NotFound:
        return "NotFound";
    case KVStoreIface::ReadVBStateStatus::JsonInvalid:
        return "JsonInvalid";
    case KVStoreIface::ReadVBStateStatus::CorruptSnapshot:
        return "CorruptSnapshot";
    case KVStoreIface::ReadVBStateStatus::Error:
        return "Error";
    }
    folly::assume_unreachable();
}

std::string format_as(CompactDBStatus status) {
    switch (status) {
    case CompactDBStatus::Success:
        return "CompactDBStatus::Success";
    case CompactDBStatus::Aborted:
        return "CompactDBStatus::Aborted";
    case CompactDBStatus::Failed:
        return "CompactDBStatus::Failed";
    }
    return fmt::format("INVALID CompactDBStatus value:{}",
                       static_cast<int>(status));
}

std::ostream& operator<<(std::ostream& os, const CompactDBStatus& status) {
    return os << format_as(status);
}

std::string format_as(const KVStoreIface::GetCollectionStatsStatus& status) {
    switch (status) {
    case KVStoreIface::GetCollectionStatsStatus::Success:
        return "GetCollectionStatsStatus::Success";
    case KVStoreIface::GetCollectionStatsStatus::NotFound:
        return "GetCollectionStatsStatus::NotFound";
    case KVStoreIface::GetCollectionStatsStatus::Failed:
        return "GetCollectionStatsStatus::Failed";
    }
    return fmt::format("INVALID GetCollectionStatsStatus value:{}",
                       static_cast<int>(status));
}

std::ostream& operator<<(std::ostream& os,
                         const KVStoreIface::GetCollectionStatsStatus& status) {
    return os << format_as(status);
}

DBFileInfo& DBFileInfo::operator+=(const DBFileInfo& other) {
    spaceUsed += other.spaceUsed;
    fileSize += other.fileSize;
    prepareBytes += other.prepareBytes;
    return *this;
}
