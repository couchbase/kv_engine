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

#include "config.h"

#include <map>
#include <string>
#include <fcntl.h>

#include "common.h"
#include "couch-kvstore/couch-kvstore.h"
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
#include "statwriter.h"
#include "vbucket.h"

#include <platform/dirutils.h>
#include <sys/types.h>
#include <sys/stat.h>

ScanContext::ScanContext(std::shared_ptr<StatusCallback<GetValue>> cb,
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
                         const Collections::VB::PersistedManifest& manifestData)
    : callback(cb),
      lookup(cl),
      lastReadSeqno(0),
      startSeqno(start),
      maxSeqno(end),
      purgeSeqno(purgeSeqno),
      scanId(id),
      vbid(vb),
      docFilter(_docFilter),
      valFilter(_valFilter),
      documentCount(_documentCount),
      logger(globalBucketLogger.get()),
      config(_config),
      collectionsContext(manifestData) {
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

KVStoreRWRO KVStoreFactory::create(KVStoreConfig& config) {
    std::string backend = config.getBackend();
    if (backend == "couchdb") {
        auto rw = std::make_unique<CouchKVStore>(config);
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

void KVFileHandleDeleter::operator()(KVFileHandle* kvFileHandle) {
    kvFileHandle->kvs.freeFileHandle(kvFileHandle);
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

bool KVStore::updateCachedVBState(Vbid vbid, const vbucket_state& newState) {
    vbucket_state* vbState = getVBucketState(vbid);

    bool state_change_detected = true;
    if (vbState != nullptr) {
        //Check if there's a need for persistence
        if (vbState->needsToBePersisted(newState)) {
            vbState->state = newState.state;
            vbState->failovers = newState.failovers;
        } else {
            state_change_detected = false;
        }

        vbState->checkpointId = newState.checkpointId;

        if (newState.maxDeletedSeqno > 0 &&
                vbState->maxDeletedSeqno < newState.maxDeletedSeqno) {
            vbState->maxDeletedSeqno = newState.maxDeletedSeqno;
        }

        vbState->highSeqno = newState.highSeqno;
        vbState->lastSnapStart = newState.lastSnapStart;
        vbState->lastSnapEnd = newState.lastSnapEnd;
        vbState->maxCas = std::max(vbState->maxCas, newState.maxCas);
        vbState->hlcCasEpochSeqno = newState.hlcCasEpochSeqno;
        vbState->mightContainXattrs = newState.mightContainXattrs;
    } else {
        cachedVBStates[vbid.get()] = std::make_unique<vbucket_state>(newState);
        if (cachedVBStates[vbid.get()]->state != vbucket_state_dead) {
            cachedValidVBCount++;
        }
    }

    return state_change_detected;
}

bool KVStore::snapshotStats(const std::map<std::string,
                            std::string> &stats) {
    if (isReadOnly()) {
        throw std::logic_error("KVStore::snapshotStats: Cannot perform "
                        "on a read-only instance.");
    }

    size_t count = 0;
    size_t size = stats.size();
    std::stringstream stats_buf;
    stats_buf << "{";
    std::map<std::string, std::string>::const_iterator it = stats.begin();
    for (; it != stats.end(); ++it) {
        stats_buf << "\"" << it->first << "\": \"" << it->second << "\"";
        ++count;
        if (count < size) {
            stats_buf << ", ";
        }
    }
    stats_buf << "}";
    std::string dbname = configuration.getDBName();
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
    if (fprintf(new_stats, "%s\n", stats_buf.str().c_str()) < 0) {
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
        if (access(old_fname.c_str(), F_OK) == 0 && remove(old_fname.c_str()) != 0) {
            EP_LOG_WARN(
                    "Failed to remove '{}': {}", old_fname, strerror(errno));
            remove(next_fname.c_str());
            rv = false;
        } else if (access(stats_fname.c_str(), F_OK) == 0 &&
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

template <typename T>
void KVStore::addStat(const std::string& prefix,
                      const char* stat,
                      T& val,
                      const AddStatFn& add_stat,
                      const void* c) {
    std::stringstream fullstat;
    fullstat << prefix << ":" << stat;
    add_casted_stat(fullstat.str().c_str(), val, add_stat, c);
}

KVStore::KVStore(KVStoreConfig& config, bool read_only)
    : configuration(config), readOnly(read_only) {
}

KVStore::~KVStore() = default;

void KVStore::addStats(const AddStatFn& add_stat, const void* c) {
    const char* backend = configuration.getBackend().c_str();

    uint16_t shardId = configuration.getShardId();
    std::stringstream prefixStream;

    if (isReadOnly()) {
        prefixStream << "ro_" << shardId;
    } else {
        prefixStream << "rw_" << shardId;
    }

    const std::string& prefix = prefixStream.str();

    /* stats for both read-only and read-write threads */
    addStat(prefix, "backend_type",   backend,            add_stat, c);
    addStat(prefix, "open",           st.numOpen,         add_stat, c);
    addStat(prefix, "close",          st.numClose,        add_stat, c);
    addStat(prefix, "numLoadedVb",    st.numLoadedVb,     add_stat, c);

    // failure stats
    addStat(prefix, "failure_compaction", st.numCompactionFailure, add_stat, c);
    addStat(prefix, "failure_open",   st.numOpenFailure, add_stat, c);
    addStat(prefix, "failure_get",    st.numGetFailure,  add_stat, c);

    if (!isReadOnly()) {
        addStat(prefix, "failure_set",   st.numSetFailure,   add_stat, c);
        addStat(prefix, "failure_del",   st.numDelFailure,   add_stat, c);
        addStat(prefix, "failure_vbset", st.numVbSetFailure, add_stat, c);
        addStat(prefix, "lastCommDocs",  st.docsCommitted,   add_stat, c);
    }

    addStat(prefix,
            "io_bg_fetch_docs_read",
            st.io_bg_fetch_docs_read,
            add_stat,
            c);
    addStat(prefix, "io_num_write", st.io_num_write, add_stat, c);
    addStat(prefix,
            "io_bg_fetch_doc_bytes",
            st.io_bgfetch_doc_bytes,
            add_stat,
            c);
    addStat(prefix, "io_write_bytes", st.io_write_bytes, add_stat, c);

    const size_t read = st.fsStats.totalBytesRead.load() +
                        st.fsStatsCompaction.totalBytesRead.load();
    addStat(prefix, "io_total_read_bytes", read, add_stat, c);

    const size_t written = st.fsStats.totalBytesWritten.load() +
                           st.fsStatsCompaction.totalBytesWritten.load();
    addStat(prefix, "io_total_write_bytes", written, add_stat, c);

    addStat(prefix, "io_compaction_read_bytes",
            st.fsStatsCompaction.totalBytesRead, add_stat, c);
    addStat(prefix, "io_compaction_write_bytes",
            st.fsStatsCompaction.totalBytesWritten, add_stat, c);

    // Specific to RocksDB. Per-shard stats.
    size_t value = 0;
    // Memory Usage
    if (getStat("kMemTableTotal", value)) {
        addStat(prefix, "rocksdb_kMemTableTotal", value, add_stat, c);
    }
    if (getStat("kMemTableUnFlushed", value)) {
        addStat(prefix, "rocksdb_kMemTableUnFlushed", value, add_stat, c);
    }
    if (getStat("kTableReadersTotal", value)) {
        addStat(prefix, "rocksdb_kTableReadersTotal", value, add_stat, c);
    }
    if (getStat("kCacheTotal", value)) {
        addStat(prefix, "rocksdb_kCacheTotal", value, add_stat, c);
    }
    // MemTable Size per-CF
    if (getStat("default_kSizeAllMemTables", value)) {
        addStat(prefix,
                "rocksdb_default_kSizeAllMemTables",
                value,
                add_stat,
                c);
    }
    if (getStat("seqno_kSizeAllMemTables", value)) {
        addStat(prefix, "rocksdb_seqno_kSizeAllMemTables", value, add_stat, c);
    }
    // Block Cache hit/miss
    if (getStat("rocksdb.block.cache.hit", value)) {
        addStat(prefix, "rocksdb_block_cache_hit", value, add_stat, c);
    }
    if (getStat("rocksdb.block.cache.miss", value)) {
        addStat(prefix, "rocksdb_block_cache_miss", value, add_stat, c);
    }
    if (getStat("rocksdb.block.cache.data.hit", value)) {
        addStat(prefix, "rocksdb_block_cache_data_hit", value, add_stat, c);
    }
    if (getStat("rocksdb.block.cache.data.miss", value)) {
        addStat(prefix, "rocksdb_block_cache_data_miss", value, add_stat, c);
    }
    if (getStat("rocksdb.block.cache.index.hit", value)) {
        addStat(prefix, "rocksdb_block_cache_index_hit", value, add_stat, c);
    }
    if (getStat("rocksdb.block.cache.index.miss", value)) {
        addStat(prefix, "rocksdb_block_cache_index_miss", value, add_stat, c);
    }
    if (getStat("rocksdb.block.cache.filter.hit", value)) {
        addStat(prefix, "rocksdb_block_cache_filter_hit", value, add_stat, c);
    }
    if (getStat("rocksdb.block.cache.filter.miss", value)) {
        addStat(prefix, "rocksdb_block_cache_filter_miss", value, add_stat, c);
    }
    // BlockCache Hit Ratio
    size_t hit = 0;
    size_t miss = 0;
    if (getStat("rocksdb.block.cache.data.hit", hit) &&
        getStat("rocksdb.block.cache.data.miss", miss) && (hit + miss) != 0) {
        const auto ratio =
                gsl::narrow_cast<int>(float(hit) / (hit + miss) * 10000);
        addStat(prefix,
                "rocksdb_block_cache_data_hit_ratio",
                ratio,
                add_stat,
                c);
    }
    if (getStat("rocksdb.block.cache.index.hit", hit) &&
        getStat("rocksdb.block.cache.index.miss", miss) && (hit + miss) != 0) {
        const auto ratio =
                gsl::narrow_cast<int>(float(hit) / (hit + miss) * 10000);
        addStat(prefix,
                "rocksdb_block_cache_index_hit_ratio",
                ratio,
                add_stat,
                c);
    }
    if (getStat("rocksdb.block.cache.filter.hit", hit) &&
        getStat("rocksdb.block.cache.filter.miss", miss) && (hit + miss) != 0) {
        const auto ratio =
                gsl::narrow_cast<int>(float(hit) / (hit + miss) * 10000);
        addStat(prefix,
                "rocksdb_block_cache_filter_hit_ratio",
                ratio,
                add_stat,
                c);
    }
    // Disk Usage per-CF
    if (getStat("default_kTotalSstFilesSize", value)) {
        addStat(prefix,
                "rocksdb_default_kTotalSstFilesSize",
                value,
                add_stat,
                c);
    }
    if (getStat("seqno_kTotalSstFilesSize", value)) {
        addStat(prefix, "rocksdb_seqno_kTotalSstFilesSize", value, add_stat, c);
    }
    // Scan stats
    if (getStat("scan_totalSeqnoHits", value)) {
        addStat(prefix, "rocksdb_scan_totalSeqnoHits", value, add_stat, c);
    }
    if (getStat("scan_oldSeqnoHits", value)) {
        addStat(prefix, "rocksdb_scan_oldSeqnoHits", value, add_stat, c);
    }
}

void KVStore::addTimingStats(const AddStatFn& add_stat, const void* c) {
    uint16_t shardId = configuration.getShardId();
    std::stringstream prefixStream;

    if (isReadOnly()) {
        prefixStream << "ro_" << shardId;
    } else {
        prefixStream << "rw_" << shardId;
    }

    const std::string& prefix = prefixStream.str();

    addStat(prefix, "commit",      st.commitHisto,      add_stat, c);
    addStat(prefix, "compact",     st.compactHisto,     add_stat, c);
    addStat(prefix, "snapshot",    st.snapshotHisto,    add_stat, c);
    addStat(prefix, "delete",      st.delTimeHisto,     add_stat, c);
    addStat(prefix, "save_documents", st.saveDocsHisto, add_stat, c);
    addStat(prefix, "readTime", st.readTimeHisto, add_stat, c);
    addStat(prefix, "readSize", st.readSizeHisto, add_stat, c);
    addStat(prefix, "writeTime",   st.writeTimeHisto,   add_stat, c);
    addStat(prefix, "writeSize",   st.writeSizeHisto,   add_stat, c);
    addStat(prefix, "saveDocCount",   st.batchSize,     add_stat, c);

    addStat(prefix, "getMultiFsReadCount", st.getMultiFsReadHisto, add_stat, c);
    addStat(prefix,
            "getMultiFsReadPerDocCount",
            st.getMultiFsReadPerDocHisto,
            add_stat,
            c);

    //file ops stats
    addStat(prefix, "fsReadTime",  st.fsStats.readTimeHisto,  add_stat, c);
    addStat(prefix, "fsWriteTime", st.fsStats.writeTimeHisto, add_stat, c);
    addStat(prefix, "fsSyncTime",  st.fsStats.syncTimeHisto,  add_stat, c);
    addStat(prefix, "fsReadSize",  st.fsStats.readSizeHisto,  add_stat, c);
    addStat(prefix, "fsWriteSize", st.fsStats.writeSizeHisto, add_stat, c);
    addStat(prefix, "fsReadSeek",  st.fsStats.readSeekHisto,  add_stat, c);
    addStat(prefix, "fsReadCount", st.fsStats.readCountHisto, add_stat, c);
    addStat(prefix, "fsWriteCount", st.fsStats.writeCountHisto, add_stat, c);
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

    CompareQueuedItemsBySeqnoAndKey cq;
    std::sort(items.begin(), items.end(), cq);
}

uint64_t KVStore::getLastPersistedSeqno(Vbid vbid) {
    vbucket_state* state = getVBucketState(vbid);
    if (state) {
        return state->highSeqno;
    }
    return 0;
}

void KVStore::setSystemEvent(
        const Item& item, Callback<TransactionContext, mutation_result>& cb) {
    // Passthrough
    // @todo: use item to update the metadata we need to store
    set(item, cb);
}

void KVStore::delSystemEvent(const Item& item,
                             Callback<TransactionContext, int>& cb) {
    // Passthrough
    // @todo: use item to update the metadata we need to store
    del(item, cb);
}

IORequest::IORequest(Vbid vbId,
                     MutationRequestCallback& cb,
                     bool del,
                     const DocKey itmKey,
                     bool pending)
    : vbucketId(vbId), deleteItem(del), key(itmKey, pending) {
    if (del) {
        callback.delCb = cb.delCb;
    } else {
        callback.setCb = cb.setCb;
    }

    start = std::chrono::steady_clock::now();
}
