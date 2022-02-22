/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "rocksdb-kvstore.h"
#include "rocksdb-kvstore_config.h"

#include "bucket_logger.h"
#include "collections/collection_persisted_stats.h"
#include "ep_time.h"
#include "item.h"
#include "kvstore/kvstore_priv.h"
#include "kvstore/kvstore_transaction_context.h"
#include "vb_commit.h"
#include "vbucket.h"
#include "vbucket_state.h"

#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <platform/cbassert.h>
#include <platform/sysinfo.h>
#include <rocksdb/convenience.h>
#include <rocksdb/filter_policy.h>
#include <statistics/cbstat_collector.h>

#include <gsl/gsl-lite.hpp>
#include <stdio.h>
#include <string.h>
#include <algorithm>
#include <limits>
#include <thread>

namespace rockskv {

// The `#pragma pack(1)` directive are to keep the size of MetaData as small as
// possible and uniform across different platforms, as MetaData is written
// directly to disk.
#pragma pack(1)

/**
 * MetaData is used to serialize and de-serialize metadata respectively when
 * writing a Document mutation request to RocksDB and when reading a Document
 * from RocksDB.
 */
class MetaData {
public:
    // The Operation this represents - maps to queue_op types:
    enum class Operation {
        // A standard mutation (or deletion). Present in the 'normal'
        // (committed) namespace.
        Mutation,

        // A prepared SyncWrite. `durability_level` field indicates the level
        // Present in the DurabilityPrepare namespace.
        PreparedSyncWrite,

        // A committed SyncWrite.
        // This exists so we can correctly backfill from disk a Committed
        // mutation and sent out as a DCP_COMMIT to sync_replication
        // enabled DCP clients.
        // Present in the 'normal' (committed) namespace.
        CommittedSyncWrite,

        // An aborted SyncWrite.
        // This exists so we can correctly backfill from disk an Aborted
        // mutation and sent out as a DCP_ABORT to sync_replication
        // enabled DCP clients.
        // Present in the DurabilityPrepare namespace.
        Abort,
    };

    MetaData()
        : deleted(0),
          deleteSource(static_cast<uint8_t>(DeleteSource::Explicit)),
          version(0),
          operation(static_cast<uint8_t>(Operation::Mutation)),
          durabilityLevel(0),
          datatype(0),
          flags(0),
          valueSize(0),
          exptime(0),
          cas(0),
          revSeqno(0),
          bySeqno(0){};
    MetaData(bool deleted,
             uint8_t deleteSource,
             uint8_t version,
             uint8_t datatype,
             uint32_t flags,
             uint32_t valueSize,
             time_t exptime,
             uint64_t cas,
             uint64_t revSeqno,
             int64_t bySeqno,
             queue_op operation,
             cb::durability::Level durabilityLevel,
             int64_t prepareSeqno)
        : deleted(deleted),
          deleteSource(deleteSource),
          version(version),
          operation(static_cast<uint8_t>(toOperation(operation))),
          durabilityLevel(static_cast<uint8_t>(durabilityLevel)),
          datatype(datatype),
          flags(flags),
          valueSize(valueSize),
          exptime(exptime),
          cas(cas),
          revSeqno(revSeqno),
          bySeqno(bySeqno),
          prepareSeqno(prepareSeqno){};

    Operation getOperation() const {
        return static_cast<Operation>(operation);
    }

    cb::durability::Level getDurabilityLevel() const {
        return static_cast<cb::durability::Level>(durabilityLevel);
    }

    uint8_t deleted : 1;
    // Note: to utilise deleteSource properly, casting to the type DeleteSource
    // is strongly recommended. It is stored as a uint8_t for better packing.
    uint8_t deleteSource : 1;

    // Metadata Version. Pre-any GA release == version 0.
    uint8_t version : 6;

    // Byte with packed fields - operation (Pending=0, Commit=1, Abort=2)
    //                         - durability_level (cb::durability::Level)
    uint8_t operation : 2;
    uint8_t durabilityLevel : 2;
    // 4 bits unused

    uint8_t datatype;
    uint32_t flags;
    uint32_t valueSize;
    time_t exptime;
    uint64_t cas;
    cb::uint48_t revSeqno;
    cb::uint48_t bySeqno;

    // @TODO only required for committed and aborted SyncWrites, move out.
    cb::uint48_t prepareSeqno;

private:
    static Operation toOperation(queue_op op) {
        switch (op) {
        case queue_op::mutation:
            return Operation::Mutation;
        case queue_op::pending_sync_write:
            return Operation::PreparedSyncWrite;
        case queue_op::commit_sync_write:
            return Operation::CommittedSyncWrite;
        case queue_op::abort_sync_write:
            return Operation::Abort;
        case queue_op::system_event:
            return Operation::Mutation;
        default:
            throw std::invalid_argument(
                    "rockskv::MetaData::toOperation: Unsupported op " +
                    to_string(op));
        }
    }
};
#pragma pack()

static_assert(sizeof(MetaData) == 45,
              "rocksdb::MetaData is not the expected size.");

} // namespace rockskv

/**
 * Class representing a document to be persisted in RocksDB.
 */
class RocksRequest : public IORequest {
public:
    /**
     * Constructor
     *
     * @param item Item instance to be persisted
     * @param callback Persistence Callback
     * @param del Flag indicating if it is an item deletion or not
     */
    explicit RocksRequest(queued_item it)
        : IORequest(std::move(it)), docBody(item->getValue()) {
        docMeta = rockskv::MetaData(
                item->isDeleted(),
                (item->isDeleted()
                         ? static_cast<uint8_t>(item->deletionSource())
                         : 0),
                0,
                item->getDataType(),
                item->getFlags(),
                item->getNBytes(),
                item->isDeleted() ? ep_real_time() : item->getExptime(),
                item->getCas(),
                item->getRevSeqno(),
                item->getBySeqno(),
                item->getOperation(),
                item->getDurabilityReqs().getLevel(),
                item->getPrepareSeqno());
    }

    const rockskv::MetaData& getDocMeta() const {
        return docMeta;
    }

    // Get a rocksdb::Slice wrapping the Document MetaData
    rocksdb::Slice getDocMetaSlice() const {
        return rocksdb::Slice(reinterpret_cast<const char*>(&docMeta),
                              sizeof(docMeta));
    }

    // Get a rocksdb::Slice wrapping the Document Body
    rocksdb::Slice getDocBodySlice() const {
        const char* data = docBody ? docBody->getData() : nullptr;
        size_t size = docBody ? docBody->valueSize() : 0;
        return rocksdb::Slice(data, size);
    }

private:
    rockskv::MetaData docMeta;
    value_t docBody;
};

// RocksDB docs suggest to "Use `rocksdb::DB::DestroyColumnFamilyHandle()` to
// close a column family instead of deleting the column family handle directly"
struct ColumnFamilyDeleter {
    explicit ColumnFamilyDeleter(rocksdb::DB& db) : db(db) {
    }
    void operator()(rocksdb::ColumnFamilyHandle* cfh) {
        db.DestroyColumnFamilyHandle(cfh);
    }

private:
    rocksdb::DB& db;
};
using ColumnFamilyPtr =
        std::unique_ptr<rocksdb::ColumnFamilyHandle, ColumnFamilyDeleter>;

// The 'VBHandle' class is a wrapper around the ColumnFamilyHandles
// for a VBucket.
class VBHandle : public KVFileHandle {
public:
    VBHandle(rocksdb::DB& rdb,
             rocksdb::ColumnFamilyHandle* defaultCFH,
             rocksdb::ColumnFamilyHandle* seqnoCFH,
             Vbid vbid)
        : rdb(rdb),
          defaultCFH(ColumnFamilyPtr(defaultCFH, ColumnFamilyDeleter(rdb))),
          seqnoCFH(ColumnFamilyPtr(seqnoCFH, ColumnFamilyDeleter(rdb))),
          vbid(vbid) {
    }

    void dropColumnFamilies() {
        // The call to DropColumnFamily() records the drop in the Manifest, but
        // the actual remove will happen when the ColumFamilyHandle is deleted.
        auto status = rdb.DropColumnFamily(defaultCFH.get());
        if (!status.ok()) {
            throw std::runtime_error(
                    "VBHandle::dropColumnFamilies: DropColumnFamily failed for "
                    "[" +
                    vbid.to_string() + ", CF: default]: " + status.getState());
        }
        status = rdb.DropColumnFamily(seqnoCFH.get());
        if (!status.ok()) {
            throw std::runtime_error(
                    "VBHandle::dropColumnFamilies: DropColumnFamily failed for "
                    "[" +
                    vbid.to_string() + ", CF: seqno]: " + status.getState());
        }
    }

    rocksdb::DB& rdb;
    const ColumnFamilyPtr defaultCFH;
    const ColumnFamilyPtr seqnoCFH;
    const Vbid vbid;
};

RocksDBKVStore::RocksDBKVStore(RocksDBKVStoreConfig& configuration)
    : KVStore(),
      configuration(configuration),
      vbHandles(configuration.getMaxVBuckets()),
      logger(configuration.getLogger()) {
    auto cacheSize = getCacheSize();
    cachedVBStates.resize(cacheSize);
    inTransaction = std::vector<std::atomic_bool>(cacheSize);

    writeOptions.sync = true;

    // The RocksDB Options is a set of DBOptions and ColumnFamilyOptions.
    // Together they cover all RocksDB available parameters.
    auto status = rocksdb::GetDBOptionsFromString(
            dbOptions, configuration.getDBOptions(), &dbOptions);
    if (!status.ok()) {
        throw std::invalid_argument(
                std::string("RocksDBKVStore::open: GetDBOptionsFromString "
                            "error: ") +
                status.getState());
    }

    {
        // This environment is all shared so we shouldn't track any allocations
        // of it against any particular bucket.
        NonBucketAllocationGuard guard;

        // Set number of background threads - note these are per-environment, so
        // are shared across all DB instances (vBuckets) and all Buckets.
        auto lowPri = configuration.getLowPriBackgroundThreads();
        if (lowPri == 0) {
            lowPri = cb::get_available_cpu_count();
        }
        rocksdb::Env::Default()->SetBackgroundThreads(lowPri,
                                                      rocksdb::Env::LOW);

        auto highPri = configuration.getHighPriBackgroundThreads();
        if (highPri == 0) {
            highPri = cb::get_available_cpu_count();
        }
        rocksdb::Env::Default()->SetBackgroundThreads(highPri,
                                                      rocksdb::Env::HIGH);
    }

    dbOptions.create_if_missing = true;

    // We use EventListener to set the correct ThreadLocal engine in the
    // ObjectRegistry for the RocksDB Flusher and Compactor threads. This
    // allows the memory tracker to track allocations and deallocations against
    // the appropriate bucket.
    auto eventListener =
            std::make_shared<EventListener>(ObjectRegistry::getCurrentEngine());
    dbOptions.listeners.emplace_back(eventListener);

    // Enable Statistics if 'Statistics::stat_level_' is provided by the
    // configuration. We create a statistics object and pass to the multiple
    // DBs managed by the same KVStore. Then the statistics object will contain
    // aggregated values for all those DBs. Note that some stats are undefined
    // and have no meaningful information across multiple DBs (e.g.,
    // "rocksdb.sequence.number").
    if (!configuration.getStatsLevel().empty()) {
        dbOptions.statistics = rocksdb::CreateDBStatistics();
        dbOptions.statistics->stats_level_ =
                getStatsLevel(configuration.getStatsLevel());
    }

    // Apply the environment rate limit for Flush and Compaction
    dbOptions.rate_limiter = configuration.getEnvRateLimiter();

    // Use a Database (per-shard) write buffer limit.
    // MB-34129: Temporary, ideally should have the per column-family limits
    // setup by applyMemtablesQuota but that currently results in the following
    // asserting firing in Debug builds of RocksDB:
    //    Assertion failed: (!ShouldScheduleFlush()), function MemTable,
    //        file ../db/memtable.cc, line 110.
    const auto perShardMemTablesQuota = configuration.getBucketQuota() /
                                        configuration.getMaxShards() *
                                        configuration.getMemtablesRatio();
    dbOptions.db_write_buffer_size = perShardMemTablesQuota;

    // Allocate the per-shard Block Cache
    if (configuration.getBlockCacheRatio() > 0.0) {
        auto blockCacheQuota = configuration.getBucketQuota() *
                               configuration.getBlockCacheRatio();
        // Keeping default settings for:
        // num_shard_bits = -1 (automatically determined)
        // strict_capacity_limit = false (do not fail insert when cache is full)
        blockCache = rocksdb::NewLRUCache(
                blockCacheQuota / configuration.getMaxShards(),
                -1 /*num_shard_bits*/,
                false /*strict_capacity_limit*/,
                configuration.getBlockCacheHighPriPoolRatio());
    }
    // Configure all the Column Families
    const auto& cfOptions = configuration.getCFOptions();
    const auto& bbtOptions = configuration.getBBTOptions();
    defaultCFOptions = getBaselineDefaultCFOptions();
    seqnoCFOptions = getBaselineSeqnoCFOptions();
    applyUserCFOptions(defaultCFOptions, cfOptions, bbtOptions);
    applyUserCFOptions(seqnoCFOptions, cfOptions, bbtOptions);

    // Open the DB and load the ColumnFamilyHandle for all the
    // existing Column Families (populates the 'vbHandles' vector)
    openDB();

    // Read persisted VBs state
    for (const auto& vbh : vbHandles) {
        if (vbh) {
            loadVBStateCache(*vbh);
            // Update stats
            ++st.numLoadedVb;
        }
    }
}

RocksDBKVStore::~RocksDBKVStore() {
    // Guarantees that all the ColumnFamilyHandles for the existing VBuckets
    // are released before 'rdb' is deleted. From RocksDB docs:
    //     "Before delete DB, you have to close All column families by calling
    //      DestroyColumnFamilyHandle() with all the handles."
    vbHandles.clear();
    // MB-28493: We need to destroy RocksDB instance before BlockCache and
    // CFOptions are destroyed as a temporary workaround for some RocksDB
    // open issues.
    rdb.reset();
}

void RocksDBKVStore::openDB() {
    auto dbname = getDBSubdir();

    std::vector<std::string> cfs;
    auto status = rocksdb::DB::ListColumnFamilies(dbOptions, dbname, &cfs);
    if (!status.ok()) {
        // If ListColumnFamilies failed because the DB does not exist,
        // then it will be created the first time we call 'rocksdb::DB::Open'.
        // Else, we throw an error if ListColumnFamilies failed for any other
        // unexpected reason.
        if (!(status.code() == rocksdb::Status::kIOError &&
              std::string(status.getState())
                              .find("No such file or directory") !=
                      std::string::npos)) {
            throw std::runtime_error(
                    "RocksDBKVStore::openDB: ListColumnFamilies failed for DB "
                    "'" +
                    dbname + "': " + status.getState());
        }
    }

    // We need to pass a ColumnFamilyDescriptor for every existing CF.
    // We populate 'cfDescriptors' so that it results in a vector
    // containing packed CFs for every VBuckets, e.g. with
    // MaxShards=4:
    //     cfDescriptors[0] = default_0
    //     cfDescriptors[1] = seqno_0
    //     cfDescriptors[2] = default_4
    //     cfDescriptors[3] = seqno_4
    //     ..
    // That helps us in populating 'vbHandles' later, because after
    // 'rocksdb::DB::Open' handles[i] will be the handle that we will use
    // to operate on the ColumnFamily at cfDescriptors[i].
    std::vector<rocksdb::ColumnFamilyDescriptor> cfDescriptors;
    for (uint16_t vbid = 0; vbid < configuration.getMaxVBuckets(); vbid++) {
        if ((vbid % configuration.getMaxShards()) ==
            configuration.getShardId()) {
            std::string defaultCF = "default_" + std::to_string(vbid);
            std::string seqnoCF = "local+seqno_" + std::to_string(vbid);
            if (std::find(cfs.begin(), cfs.end(), defaultCF) != cfs.end()) {
                if (std::find(cfs.begin(), cfs.end(), seqnoCF) == cfs.end()) {
                    throw std::logic_error("RocksDBKVStore::openDB: DB '" +
                                           dbname +
                                           "' is in inconsistent state: CF " +
                                           seqnoCF + " not found.");
                }
                cfDescriptors.emplace_back(defaultCF, defaultCFOptions);
                cfDescriptors.emplace_back(seqnoCF, seqnoCFOptions);
            }
        }
    }

    // TODO: The RocksDB built-in 'default' CF always exists, need to check if
    // we can drop it.
    cfDescriptors.emplace_back(rocksdb::kDefaultColumnFamilyName,
                               rocksdb::ColumnFamilyOptions());
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    rocksdb::DB* db;
    status = rocksdb::DB::Open(dbOptions, dbname, cfDescriptors, &handles, &db);
    if (!status.ok()) {
        throw std::runtime_error(
                "RocksDBKVStore::openDB: Open failed for database '" + dbname +
                "': " + status.getState());
    }
    rdb.reset(db);

    // The way we populated 'cfDescriptors' guarantees that: if 'cfDescriptors'
    // contains more than only the RocksDB 'default' CF (i.e.,
    // '(cfDescriptors.size() - 1) > 0') then 'cfDescriptors[i]' and
    // 'cfDescriptors[i+1]' are respectively the 'default_' and 'seqno'_ CFs
    // for a certain VBucket.
    for (uint16_t i = 0; i < (cfDescriptors.size() - 1); i += 2) {
        // Note: any further sanity-check is redundant as we will have always
        // 'cf = "default_<vbid>"'.
        const auto& cf = cfDescriptors[i].name;
        Vbid vbid(std::stoi(cf.substr(8)));
        vbHandles[vbid.get()] = std::make_shared<VBHandle>(
                *rdb, handles[i], handles[i + 1], vbid);
    }

    // We need to release the ColumnFamilyHandle for the built-in 'default' CF
    // here, as it is not managed by any VBHandle.
    rdb->DestroyColumnFamilyHandle(handles.back());
}

std::shared_ptr<VBHandle> RocksDBKVStore::getVBHandle(Vbid vbid) const {
    if (vbid.get() >= configuration.getMaxVBuckets()) {
        throw std::invalid_argument(
                fmt::format("RocksDBKVStore::getVBHandle: {} exceeds valid "
                            "vBucket range (0..{})",
                            vbid,
                            configuration.getMaxVBuckets() - 1));
    }
    std::lock_guard<std::mutex> lg(vbhMutex);
    if (vbHandles[vbid.get()]) {
        return vbHandles[vbid.get()];
    }

    return {};
}

std::shared_ptr<VBHandle> RocksDBKVStore::getOrCreateVBHandle(Vbid vbid) {
    std::lock_guard<std::mutex> lg(vbhMutex);
    if (vbHandles[vbid.get()]) {
        return vbHandles[vbid.get()];
    }

    // If the VBHandle for vbid does not exist it means that we need to create
    // the VBucket, i.e. we need to create the set of CFs on DB for vbid
    std::vector<rocksdb::ColumnFamilyDescriptor> cfDescriptors;
    auto vbid_ = std::to_string(vbid.get());
    cfDescriptors.emplace_back("default_" + vbid_, defaultCFOptions);
    cfDescriptors.emplace_back("local+seqno_" + vbid_, seqnoCFOptions);

    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    auto status = rdb->CreateColumnFamilies(cfDescriptors, &handles);
    if (!status.ok()) {
        for (auto* cfh : handles) {
            status = rdb->DropColumnFamily(cfh);
            if (!status.ok()) {
                throw std::runtime_error(
                        "RocksDBKVStore::getVBHandle: DropColumnFamily failed "
                        "for CF " +
                        cfh->GetName() + ": " + status.getState());
            }
        }
        throw std::runtime_error(
                "RocksDBKVStore::getVBHandle: CreateColumnFamilies failed "
                "for " +
                vbid.to_string() + ": " + status.getState());
    }

    vbHandles[vbid.get()] =
            std::make_shared<VBHandle>(*rdb, handles[0], handles[1], vbid);

    return vbHandles[vbid.get()];
}

std::string RocksDBKVStore::getDBSubdir() {
    return configuration.getDBName() + "/rocksdb." +
           std::to_string(configuration.getShardId());
}

bool RocksDBKVStore::commit(std::unique_ptr<TransactionContext> txnCtx,
                            VB::Commit& commitData) {
    checkIfInTransaction(txnCtx->vbid, "RocksDBKVStore::commit");

    auto& ctx = dynamic_cast<RocksDBKVStoreTransactionContext&>(*txnCtx);
    if (ctx.pendingReqs->empty()) {
        return true;
    }

    // Swap `pendingReqs` with the temporary `commitBatch` so that we can
    // shorten the scope of the lock.
    PendingRequestQueue commitBatch;
    {
        std::lock_guard<std::mutex> lock(writeMutex);
        std::swap(*ctx.pendingReqs, commitBatch);
    }

    bool success = true;
    auto vbid = txnCtx->vbid;

    // Flush all documents to disk
    auto status = saveDocs(vbid, commitData, commitBatch);
    if (!status.ok()) {
        logger.warn(
                "RocksDBKVStore::commit: saveDocs error:{}, "
                "{}",
                status.code(),
                vbid);
        success = false;
    }

    postFlushHook();

    commitCallback(ctx, status, commitBatch);

    // This behaviour is to replicate the one in Couchstore.
    // Set `in_transanction = false` only if `commit` is successful.
    if (success) {
        updateCachedVBState(vbid, commitData.proposedVBState);
    }

    return success;
}

void RocksDBKVStore::commitCallback(TransactionContext& txnCtx,
                                    rocksdb::Status status,
                                    const PendingRequestQueue& commitBatch) {
    const auto flushSuccess = (status.code() == rocksdb::Status::Code::kOk);
    for (const auto& request : commitBatch) {
        auto dataSize = request.getDocMetaSlice().size() +
                        request.getDocBodySlice().size();
        const auto& key = request.getKey();
        /* update ep stats */
        ++st.io_num_write;
        st.io_document_write_bytes += (key.size() + dataSize);

        if (request.isDelete()) {
            FlushStateDeletion state;
            if (flushSuccess) {
                // TODO: Should set `state` to Success or DocNotFound depending
                //  on if this is a delete to an existing (Success) or
                //  non-existing (DocNotFound) item. However, to achieve this we
                //  would need to perform a Get to RocksDB which is costly. For
                //  now just assume that the item did not exist.
                state = FlushStateDeletion::DocNotFound;
                st.delTimeHisto.add(request.getDelta() / 1000);
            } else {
                state = FlushStateDeletion::Failed;
                ++st.numDelFailure;
            }
            txnCtx.deleteCallback(request.getItem(), state);
        } else {
            FlushStateMutation state;
            if (flushSuccess) {
                // TODO: Should set `state` to Insert or Update depending on
                // whether we had already an alive item on disk or not.
                // However, to achieve this we would need to perform a Get to
                // RocksDB which is costly. For now just assume that the item
                // did not exist.
                state = FlushStateMutation::Insert;
                st.writeTimeHisto.add(request.getDelta() / 1000);
                st.writeSizeHisto.add(dataSize + key.size());
            } else {
                state = FlushStateMutation::Failed;
                ++st.numSetFailure;
            }
            txnCtx.setCallback(request.getItem(), state);
        }
    }
}

size_t RocksDBKVStore::getItemCount(Vbid vbid) {
    const auto vbh = getVBHandle(vbid);
    if (!vbh) {
        throw std::system_error(
                std::make_error_code(std::errc::no_such_file_or_directory),
                fmt::format("RocksDBKVStore::getMagmaDbStats: failed to open "
                            "database file for {}",
                            vbid));
    }
    std::string val;
    // TODO: Maintain an accurate item count, not just an estimate.
    rdb->GetProperty(vbh->defaultCFH.get(), "rocksdb.estimate-num-keys", &val);
    return std::stoi(val);
}

std::vector<vbucket_state*> RocksDBKVStore::listPersistedVbuckets() {
    std::vector<vbucket_state*> result;
    for (const auto& vb : cachedVBStates) {
        result.emplace_back(vb.get());
    }
    return result;
}

void RocksDBKVStore::set(TransactionContext& txnCtx, queued_item item) {
    checkIfInTransaction(txnCtx.vbid, "RocksDBKVStore::set");

    auto& ctx = dynamic_cast<RocksDBKVStoreTransactionContext&>(txnCtx);
    ctx.pendingReqs->emplace_back(std::move(item));
}

GetValue RocksDBKVStore::get(const DiskDocKey& key,
                             Vbid vb,
                             ValueFilter filter) const {
    return getWithHeader(key, vb, filter);
}

GetValue RocksDBKVStore::getWithHeader(const KVFileHandle& kvFileHandle,
                                       const DiskDocKey& key,
                                       Vbid vb,
                                       ValueFilter filter) const {
    return getWithHeader(key, vb, filter);
}

GetValue RocksDBKVStore::getWithHeader(const DiskDocKey& key,
                                       Vbid vb,
                                       ValueFilter filter) const {
    const auto vbh = getVBHandle(vb);
    rocksdb::Slice keySlice = getKeySlice(key);
    rocksdb::PinnableSlice value;
    rocksdb::Status s = rdb->Get(
            rocksdb::ReadOptions(), vbh->defaultCFH.get(), keySlice, &value);
    if (!s.ok()) {
        st.numGetFailure++;
        return GetValue{nullptr, cb::engine_errc::no_such_key};
    }

    ++st.io_bg_fetch_docs_read;
    st.io_bgfetch_doc_bytes += keySlice.size() + value.size();
    return makeGetValue(vb, key, value, filter != ValueFilter::KEYS_ONLY);
}

void RocksDBKVStore::getMulti(Vbid vb, vb_bgfetch_queue_t& itms) const {
    // TODO RDB: RocksDB supports a multi get which we should use here.
    for (auto& it : itms) {
        auto& key = it.first;
        rocksdb::Slice keySlice = getKeySlice(key);
        rocksdb::PinnableSlice value;
        const auto vbh = getVBHandle(vb);
        rocksdb::Status s = rdb->Get(rocksdb::ReadOptions(),
                                     vbh->defaultCFH.get(),
                                     keySlice,
                                     &value);
        if (s.ok()) {
            it.second.value = makeGetValue(
                    vb,
                    key,
                    value,
                    it.second.getValueFilter() != ValueFilter::KEYS_ONLY);
            ++st.io_bg_fetch_docs_read;
            st.io_bgfetch_doc_bytes += keySlice.size() + value.size();
        } else {
            it.second.value.setStatus(cb::engine_errc::no_such_key);
        }
    }
}

void RocksDBKVStore::getRange(Vbid vb,
                              const DiskDocKey& startKey,
                              const DiskDocKey& endKey,
                              ValueFilter filter,
                              const KVStore::GetRangeCb& cb) const {
    auto startSlice = getKeySlice(startKey);
    auto endSlice = getKeySlice(endKey);
    rocksdb::ReadOptions rangeOptions;
    rangeOptions.iterate_upper_bound = &endSlice;

    const auto vbh = getVBHandle(vb);
    std::unique_ptr<rocksdb::Iterator> it(
            rdb->NewIterator(rangeOptions, vbh->defaultCFH.get()));
    if (!it) {
        throw std::logic_error(
                "RocksDBKVStore::getRange: rocksdb::Iterator to Default Column "
                "Family is nullptr");
    }

    for (it->Seek(startSlice); it->Valid(); it->Next()) {
        auto key = DiskDocKey{it->key().data(), it->key().size()};
        auto gv = makeGetValue(
                vb, key, it->value(), filter != ValueFilter::KEYS_ONLY);
        if (gv.item->isDeleted()) {
            // Ignore deleted items.
            continue;
        }
        cb(std::move(gv));
    }
    Expects(it->status().ok());
}

void RocksDBKVStore::del(TransactionContext& txnCtx, queued_item item) {
    checkIfInTransaction(txnCtx.vbid, "RocksDBKVStore::del");

    if (!item->isDeleted()) {
        throw std::invalid_argument(
                "RocksDBKVStore::del item to delete is not marked as deleted.");
    }
    // TODO: Deleted items remain as tombstones, but are not yet expired,
    // they will accumuate forever.
    auto& ctx = dynamic_cast<RocksDBKVStoreTransactionContext&>(txnCtx);
    ctx.pendingReqs->emplace_back(std::move(item));
}

void RocksDBKVStore::delVBucket(Vbid vbid,
                                std::unique_ptr<KVStoreRevision> vb_version) {
    std::lock_guard<std::mutex> lg1(writeMutex);
    std::lock_guard<std::mutex> lg2(vbhMutex);

    if (!vbHandles[vbid.get()]) {
        logger.warn("RocksDBKVStore::delVBucket: VBucket not found, {}", vbid);
        return;
    }

    // 'vbHandles' stores a shared_ptr to VBHandle for each VBucket . The
    // ownership of each pointer is shared among multiple threads performing
    // different operations (e.g., 'get' and 'commit').
    // We want to call 'DropColumnFamily' here rather than in other threads
    // because it is an expensive, IO-intensive operation and we do not want
    // it to cause another thread (possibly a front-end one) from being blocked
    // performing the drop.
    // So, the thread executing 'delVBucket' spins until it is the exclusive
    // owner of the shared_ptr (i.e., other concurrent threads like 'commit'
    // have completed and do not own any copy of the shared_ptr).
    {
        std::shared_ptr<VBHandle> sharedPtr;
        std::swap(vbHandles[vbid.get()], sharedPtr);
        while (!sharedPtr.unique()) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
        // Drop all the CF for vbid.
        sharedPtr->dropColumnFamilies();
    }
}

bool RocksDBKVStore::snapshotVBucket(Vbid vbucketId,
                                     const vbucket_state& vbstate) {
    if (!needsToBePersisted(vbucketId, vbstate)) {
        return true;
    }

    auto start = std::chrono::steady_clock::now();

    const auto vbh = getOrCreateVBHandle(vbucketId);
    rocksdb::WriteBatch batch;
    auto status = saveVBStateToBatch(*vbh, vbstate, batch);
    if (!status.ok()) {
        ++st.numVbSetFailure;
        logger.warn(
                "RocksDBKVStore::snapshotVBucket: saveVBStateToBatch() "
                "failed state:{} {} :{}",
                VBucket::toString(vbstate.transition.state),
                vbucketId,
                status.getState());
        return false;
    }
    status = rdb->Write(writeOptions, &batch);
    if (!status.ok()) {
        ++st.numVbSetFailure;
        logger.warn(
                "RocksDBKVStore::snapshotVBucket: Write() "
                "failed state:{} {} :{}",
                VBucket::toString(vbstate.transition.state),
                vbucketId,
                status.getState());
        return false;
    }

    updateCachedVBState(vbucketId, vbstate);

    EP_LOG_DEBUG("RocksDBKVStore::snapshotVBucket: Snapshotted {} state:{}",
                 vbucketId,
                 nlohmann::json(vbstate).dump());

    st.snapshotHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start));

    return true;
}

void RocksDBKVStore::destroyInvalidVBuckets(bool) {
    // TODO RDB:  implement
}

size_t RocksDBKVStore::getNumShards() {
    return configuration.getMaxShards();
}

bool RocksDBKVStore::getStat(std::string_view name, size_t& value) const {
    // Memory Usage
    if (name == "kMemTableTotal") {
        return getStatFromMemUsage(rocksdb::MemoryUtil::kMemTableTotal, value);
    } else if (name == "kMemTableUnFlushed") {
        return getStatFromMemUsage(rocksdb::MemoryUtil::kMemTableUnFlushed,
                                   value);
    } else if (name == "kTableReadersTotal") {
        return getStatFromMemUsage(rocksdb::MemoryUtil::kTableReadersTotal,
                                   value);
    } else if (name == "kCacheTotal") {
        return getStatFromMemUsage(rocksdb::MemoryUtil::kCacheTotal, value);
    }

    // MemTable Size per Column Famiy
    else if (name == "default_kSizeAllMemTables") {
        return getStatFromProperties(ColumnFamily::Default,
                                     rocksdb::DB::Properties::kSizeAllMemTables,
                                     value);
    } else if (name == "seqno_kSizeAllMemTables") {
        return getStatFromProperties(ColumnFamily::Seqno,
                                     rocksdb::DB::Properties::kSizeAllMemTables,
                                     value);
    }

    // Block Cache hit/miss
    else if (name == "rocksdb.block.cache.hit") {
        return getStatFromStatistics(rocksdb::Tickers::BLOCK_CACHE_HIT, value);
    } else if (name == "rocksdb.block.cache.miss") {
        return getStatFromStatistics(rocksdb::Tickers::BLOCK_CACHE_MISS, value);
    } else if (name == "rocksdb.block.cache.data.hit") {
        return getStatFromStatistics(rocksdb::Tickers::BLOCK_CACHE_DATA_HIT,
                                     value);
    } else if (name == "rocksdb.block.cache.data.miss") {
        return getStatFromStatistics(rocksdb::Tickers::BLOCK_CACHE_DATA_MISS,
                                     value);
    } else if (name == "rocksdb.block.cache.index.hit") {
        return getStatFromStatistics(rocksdb::Tickers::BLOCK_CACHE_INDEX_HIT,
                                     value);
    } else if (name == "rocksdb.block.cache.index.miss") {
        return getStatFromStatistics(rocksdb::Tickers::BLOCK_CACHE_INDEX_MISS,
                                     value);
    } else if (name == "rocksdb.block.cache.filter.hit") {
        return getStatFromStatistics(rocksdb::Tickers::BLOCK_CACHE_FILTER_HIT,
                                     value);
    } else if (name == "rocksdb.block.cache.filter.miss") {
        return getStatFromStatistics(rocksdb::Tickers::BLOCK_CACHE_FILTER_MISS,
                                     value);
    }

    // Disk Usage per Column Family
    else if (name == "default_kTotalSstFilesSize") {
        return getStatFromProperties(
                ColumnFamily::Default,
                rocksdb::DB::Properties::kTotalSstFilesSize,
                value);
    } else if (name == "seqno_kTotalSstFilesSize") {
        return getStatFromProperties(
                ColumnFamily::Seqno,
                rocksdb::DB::Properties::kTotalSstFilesSize,
                value);
    }

    // Scan stats
    else if (name == "scan_totalSeqnoHits") {
        value = scanTotalSeqnoHits.load();
        return true;
    } else if (name == "scan_oldSeqnoHits") {
        value = scanOldSeqnoHits.load();
        return true;
    }

    return false;
}

StorageProperties RocksDBKVStore::getStorageProperties() const {
    StorageProperties rv(StorageProperties::ByIdScan::No,
                         StorageProperties::AutomaticDeduplication::Yes,
                         StorageProperties::PrepareCounting::No,
                         StorageProperties::CompactionStaleItemCallbacks::No);
    return rv;
}

std::unordered_set<const rocksdb::Cache*> RocksDBKVStore::getCachePointers()
        const {
    std::unordered_set<const rocksdb::Cache*> cache_set;

    // TODO: Cache from DBImpl. The 'std::shared_ptr<Cache>
    // table_cache_' pointer is not exposed through the 'DB' interface

    // Cache from DBOptions
    // Note: we do not use the 'row_cache' currently.
    cache_set.insert(rdb->GetDBOptions().row_cache.get());

    // Cache from table factories.
    addCFBlockCachePointers(defaultCFOptions, cache_set);
    addCFBlockCachePointers(seqnoCFOptions, cache_set);

    return cache_set;
}

void RocksDBKVStore::addCFBlockCachePointers(
        const rocksdb::ColumnFamilyOptions& cfOptions,
        std::unordered_set<const rocksdb::Cache*>& cache_set) {
    if (cfOptions.table_factory) {
        auto* table_options = cfOptions.table_factory->GetOptions();
        auto* bbt_options =
                static_cast<rocksdb::BlockBasedTableOptions*>(table_options);
        cache_set.insert(bbt_options->block_cache.get());
        cache_set.insert(bbt_options->block_cache_compressed.get());
    }
}

rocksdb::StatsLevel RocksDBKVStore::getStatsLevel(
        const std::string& stats_level) {
    if (stats_level == "kExceptDetailedTimers") {
        return rocksdb::StatsLevel::kExceptDetailedTimers;
    } else if (stats_level == "kExceptTimeForMutex") {
        return rocksdb::StatsLevel::kExceptTimeForMutex;
    } else if (stats_level == "kAll") {
        return rocksdb::StatsLevel::kAll;
    } else {
        throw std::invalid_argument(
                std::string("RocksDBKVStore::getStatsLevel: stats_level: '") +
                stats_level + std::string("'"));
    }
}

rocksdb::Slice RocksDBKVStore::getKeySlice(const DiskDocKey& key) const {
    return rocksdb::Slice(reinterpret_cast<const char*>(key.data()),
                          key.size());
}

rocksdb::Slice RocksDBKVStore::getSeqnoSlice(const int64_t* seqno) const {
    return rocksdb::Slice(reinterpret_cast<const char*>(seqno), sizeof(*seqno));
}

int64_t RocksDBKVStore::getNumericSeqno(
        const rocksdb::Slice& seqnoSlice) const {
    assert(seqnoSlice.size() == sizeof(int64_t));
    int64_t seqno;
    std::memcpy(&seqno, seqnoSlice.data(), seqnoSlice.size());
    return seqno;
}

std::unique_ptr<Item> RocksDBKVStore::makeItem(Vbid vb,
                                               const DiskDocKey& key,
                                               const rocksdb::Slice& s,
                                               bool includeValue) const {
    assert(s.size() >= sizeof(rockskv::MetaData));

    const char* data = s.data();

    rockskv::MetaData meta;
    std::memcpy(&meta, data, sizeof(meta));
    data += sizeof(meta);

    includeValue = includeValue && (meta.valueSize > 0);

    auto item = std::make_unique<Item>(key.getDocKey(),
                                       meta.flags,
                                       meta.exptime,
                                       includeValue ? data : nullptr,
                                       includeValue ? meta.valueSize : 0,
                                       meta.datatype,
                                       meta.cas,
                                       meta.bySeqno,
                                       vb,
                                       meta.revSeqno);

    if (meta.deleted) {
        item->setDeleted(static_cast<DeleteSource>(meta.deleteSource));
    }

    switch (meta.getOperation()) {
    case rockskv::MetaData::Operation::Mutation:
        // Item already defaults to Mutation - nothing else to do.
        return item;
    case rockskv::MetaData::Operation::PreparedSyncWrite:
        // From disk we return an infinite timeout; as this could
        // refer to an already-committed SyncWrite and hence timeout
        // must be ignored.
        item->setPendingSyncWrite({meta.getDurabilityLevel(),
                                   cb::durability::Timeout::Infinity()});
        return item;
    case rockskv::MetaData::Operation::CommittedSyncWrite:
        item->setCommittedviaPrepareSyncWrite();
        item->setPrepareSeqno(meta.prepareSeqno);
        return item;
    case rockskv::MetaData::Operation::Abort:
        item->setAbortSyncWrite();
        item->setPrepareSeqno(meta.prepareSeqno);
        return item;
    }

    folly::assume_unreachable();
}

GetValue RocksDBKVStore::makeGetValue(Vbid vb,
                                      const DiskDocKey& key,
                                      const rocksdb::Slice& value,
                                      bool includeValue) const {
    return GetValue(makeItem(vb, key, value, includeValue),
                    cb::engine_errc::success,
                    -1,
                    false);
}

RocksDBKVStore::DiskState RocksDBKVStore::readVBStateFromDisk(
        const VBHandle& vbh) const {
    auto key = getVbstateKey();
    std::string jsonStr;
    vbucket_state vbState;
    auto vbid = vbh.vbid;
    auto status = rdb->Get(rocksdb::ReadOptions(),
                           vbh.seqnoCFH.get(),
                           getSeqnoSlice(&key),
                           &jsonStr);
    if (!status.ok()) {
        if (status.IsNotFound()) {
            logger.info(
                    "RocksDBKVStore::readVBStateFromDisk: '_local/vbstate.{}' "
                    "not "
                    "found",
                    vbid.get());
        } else {
            logger.warn(
                    "RocksDBKVStore::readVBStateFromDisk: error getting "
                    "vbstate "
                    "error:{}, {}",
                    status.getState(),
                    vbid);
        }
    } else {
        nlohmann::json json;
        try {
            json = nlohmann::json::parse(jsonStr);
        } catch (const nlohmann::json::exception& e) {
            logger.warn(
                    "RocksKVStore::readVBStateFromDisk: Failed to parse the "
                    "vbstat "
                    "json doc for {}, json:{} with reason:{}",
                    vbid,
                    jsonStr,
                    e.what());
            return {};
        }

        // Merge in the high_seqno (which is implicitly stored as the highest
        // seqno item).
        json["high_seqno"] = std::to_string(readHighSeqnoFromDisk(vbh));

        try {
            vbState = json;
        } catch (const nlohmann::json::exception& e) {
            logger.warn(
                    "RocksKVStore::readVBStateFromDisk: Failed to "
                    "convert the vbstat json doc for {} to vbState, json:{}, "
                    "with "
                    "reason:{}",
                    vbid,
                    jsonStr,
                    e.what());
            return {};
        }

        bool snapshotValid;
        std::tie(snapshotValid, vbState.lastSnapStart, vbState.lastSnapEnd) =
                processVbstateSnapshot(vbid, vbState);

        if (!snapshotValid) {
            status = rocksdb::Status::Corruption(
                    "RocksDBKVStore::readVBStateFromDisk: " + vbid.to_string() +
                    " detected corrupt snapshot.");
        }
    }

    return {status, vbState};
}

void RocksDBKVStore::loadVBStateCache(const VBHandle& vbh) {
    auto diskState = readVBStateFromDisk(vbh);

    // Cannot use make_unique here as it doesn't support brace-initialization
    // until C++20.
    auto vbid = vbh.vbid;
    cachedVBStates[getCacheSlot(vbid)] =
            std::make_unique<vbucket_state>(diskState.vbstate);
}

rocksdb::Status RocksDBKVStore::saveVBStateToBatch(const VBHandle& vbh,
                                                   const vbucket_state& vbState,
                                                   rocksdb::WriteBatch& batch) {
    auto key = getVbstateKey();
    rocksdb::Slice keySlice = getSeqnoSlice(&key);
    nlohmann::json json = vbState;

    // Strip out the high_seqno it is automatically tracked by RocksDB (as the
    // highest seqno item written) so unnecessary (and potentially confusing)
    // to store in vbstate document.
    json.erase("high_seqno");

    return batch.Put(vbh.seqnoCFH.get(), keySlice, json.dump());
}

rocksdb::ColumnFamilyOptions RocksDBKVStore::getBaselineDefaultCFOptions() {
    rocksdb::ColumnFamilyOptions cfOptions;
    // Note: While we *mostly* use only point-lookup, still need to support
    // range lookups for warmup - to be able to identify all Prepared Sync
    // Writes. As such, use similar options to OptimizeForPointLookup(),
    // but with the default index_type which supports binary search.
    rocksdb::BlockBasedTableOptions blockBasedOptions;
    blockBasedOptions.filter_policy.reset(
            rocksdb::NewBloomFilterPolicy(10, false));
    cfOptions.table_factory.reset(
            rocksdb::NewBlockBasedTableFactory(blockBasedOptions));

    // Note: Block Cache is *not* configured here - it is later reset with the
    // shared 'blockCache' of size 'rocksdb_block_cache_size'

    return cfOptions;
}

rocksdb::ColumnFamilyOptions RocksDBKVStore::getBaselineSeqnoCFOptions() {
    rocksdb::ColumnFamilyOptions cfOptions;
    cfOptions.comparator = &seqnoComparator;
    return cfOptions;
}

void RocksDBKVStore::applyUserCFOptions(rocksdb::ColumnFamilyOptions& cfOptions,
                                        const std::string& newCfOptions,
                                        const std::string& newBbtOptions) {
    // Apply 'newCfOptions' on top of 'cfOptions'
    auto status = rocksdb::GetColumnFamilyOptionsFromString(
            cfOptions, newCfOptions, &cfOptions);
    if (!status.ok()) {
        throw std::invalid_argument(
                std::string("RocksDBKVStore::applyUserCFOptions:  "
                            "GetColumnFamilyOptionsFromString error: ") +
                status.getState());
    }

    // RocksDB ColumnFamilyOptions provide advanced options for the
    // Block Based Table file format, which is the default format for SST files.
    // Apply 'newBbtOptions' on top of the current BlockBasedTableOptions of
    // 'cfOptions'
    rocksdb::BlockBasedTableOptions baseOptions;
    if (cfOptions.table_factory) {
        auto* bbtOptions = cfOptions.table_factory->GetOptions();
        if (bbtOptions) {
            baseOptions = *(
                    static_cast<rocksdb::BlockBasedTableOptions*>(bbtOptions));
        }
    }

    rocksdb::BlockBasedTableOptions tableOptions;
    status = rocksdb::GetBlockBasedTableOptionsFromString(
            baseOptions, newBbtOptions, &tableOptions);
    if (!status.ok()) {
        throw std::invalid_argument(
                std::string("RocksDBKVStore::applyUserCFOptions: "
                            "GetBlockBasedTableOptionsFromString error: ") +
                status.getState());
    }

    // If using Partitioned Filters, then use the RocksDB recommended params
    // (https://github.com/facebook/rocksdb/blob/master/include/rocksdb/filter_policy.h#L133):
    //     "bits_per_key: bits per key in bloom filter. A good value for
    //           bits_per_key is 10, which yields a filter with ~1% false
    //           positive rate.
    //       use_block_based_builder: use block based filter rather than full
    //           filter. If you want to build a full filter, it needs to be
    //           set to false."
    if (tableOptions.partition_filters == true) {
        tableOptions.filter_policy.reset(
                rocksdb::NewBloomFilterPolicy(10, false));
    }

    // Always use the per-shard shared Block Cache. If it is nullptr, RocksDB
    // will allocate a default size Block Cache.
    tableOptions.block_cache = blockCache;

    // Set the new BlockBasedTableOptions
    cfOptions.table_factory.reset(
            rocksdb::NewBlockBasedTableFactory(tableOptions));

    // Set the user-provided size amplification factor if under Universal
    // Compaction
    if (cfOptions.compaction_style ==
        rocksdb::CompactionStyle::kCompactionStyleUniversal) {
        cfOptions.compaction_options_universal.max_size_amplification_percent =
                configuration.getUCMaxSizeAmplificationPercent();
    }
}

rocksdb::Status RocksDBKVStore::writeAndTimeBatch(rocksdb::WriteBatch batch) {
    auto begin = std::chrono::steady_clock::now();
    auto status = rdb->Write(writeOptions, &batch);
    st.commitHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - begin));
    return status;
}

rocksdb::Status RocksDBKVStore::saveDocs(
        Vbid vbid,
        VB::Commit& commitData,
        const PendingRequestQueue& commitBatch) {
    auto reqsSize = commitBatch.size();
    if (reqsSize == 0) {
        st.docsCommitted = 0;
        return rocksdb::Status::OK();
    }

    rocksdb::Status status;
    int64_t maxDBSeqno = 0;
    rocksdb::WriteBatch batch;

    const auto vbh = getOrCreateVBHandle(vbid);

    for (const auto& request : commitBatch) {
        int64_t bySeqno = request.getDocMeta().bySeqno;
        maxDBSeqno = std::max(maxDBSeqno, bySeqno);

        status = addRequestToWriteBatch(*vbh, batch, request);
        if (!status.ok()) {
            logger.warn(
                    "RocksDBKVStore::saveDocs: addRequestToWriteBatch "
                    "error:{}, {}",
                    status.code(),
                    vbid);
            return status;
        }

        // Check if we should split into a new writeBatch if the batch size
        // exceeds the write_buffer_size - this is necessary because we
        // don't want our WriteBatch to exceed the configured memtable size, as
        // that can cause significant memory bloating (see MB-26521).
        // Note the limit check is only approximate, as the batch contains
        // updates for at least 2 CFs (key & seqno) which will be written into
        // separate memtables, so we don't exactly know the size contribution
        // to each memtable in the batch.
        const auto batchLimit = defaultCFOptions.write_buffer_size +
                                seqnoCFOptions.write_buffer_size;
        if (batch.GetDataSize() > batchLimit) {
            status = writeAndTimeBatch(batch);
            if (!status.ok()) {
                logger.warn(
                        "RocksDBKVStore::saveDocs: rocksdb::DB::Write "
                        "error:{}, "
                        "{}",
                        status.code(),
                        vbid);
                return status;
            }
            batch.Clear();
        }
    }

    status = saveVBStateToBatch(*vbh, commitData.proposedVBState, batch);
    if (!status.ok()) {
        ++st.numVbSetFailure;
        logger.warn("RocksDBKVStore::saveDocs: saveVBStateToBatch error:{}",
                    status.code());
        return status;
    }

    status = writeAndTimeBatch(batch);
    if (!status.ok()) {
        ++st.numVbSetFailure;
        logger.warn(
                "RocksDBKVStore::saveDocs: rocksdb::DB::Write error:{}, "
                "{}",
                status.code(),
                vbid);
        return status;
    }

    st.batchSize.add(reqsSize);
    st.docsCommitted = reqsSize;

    // Update high seqno
    commitData.proposedVBState.highSeqno = maxDBSeqno;

    return rocksdb::Status::OK();
}

rocksdb::Status RocksDBKVStore::addRequestToWriteBatch(
        const VBHandle& vbh,
        rocksdb::WriteBatch& batch,
        const RocksRequest& request) {
    Vbid vbid = vbh.vbid;

    rocksdb::Slice keySlice = getKeySlice(request.getKey());
    rocksdb::SliceParts keySliceParts(&keySlice, 1);

    rocksdb::Slice docSlices[] = {request.getDocMetaSlice(),
                                  request.getDocBodySlice()};
    rocksdb::SliceParts valueSliceParts(docSlices, 2);

    // bySeqno index uses a int64_t (partly because we use negative values
    // for magic keys (e.g. vbState). Request.bySeqno is uint48_t, so need
    // to copy into a temporary of correct size.
    const int64_t bySeqno = request.getDocMeta().bySeqno;
    rocksdb::Slice bySeqnoSlice = getSeqnoSlice(&bySeqno);
    // We use the `saveDocsHisto` to track the time spent on
    // `rocksdb::WriteBatch::Put()`.
    auto begin = std::chrono::steady_clock::now();
    auto status =
            batch.Put(vbh.defaultCFH.get(), keySliceParts, valueSliceParts);
    if (!status.ok()) {
        logger.warn(
                "RocksDBKVStore::saveDocs: rocksdb::WriteBatch::Put "
                "[ColumnFamily: \'default\']  error:{}, "
                "{}",
                status.code(),
                vbid);
        return status;
    }
    status = batch.Put(vbh.seqnoCFH.get(), bySeqnoSlice, keySlice);
    if (!status.ok()) {
        logger.warn(
                "RocksDBKVStore::saveDocs: rocksdb::WriteBatch::Put "
                "[ColumnFamily: \'seqno\']  error:{}, "
                "{}",
                status.code(),
                vbid);
        return status;
    }
    st.saveDocsHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - begin));

    return rocksdb::Status::OK();
}

int64_t RocksDBKVStore::readHighSeqnoFromDisk(const VBHandle& vbh) const {
    std::unique_ptr<rocksdb::Iterator> it(
            rdb->NewIterator(rocksdb::ReadOptions(), vbh.seqnoCFH.get()));

    // Seek to the highest seqno=>key mapping stored for the vbid
    auto maxSeqno = std::numeric_limits<int64_t>::max();
    rocksdb::Slice maxSeqnoSlice = getSeqnoSlice(&maxSeqno);
    it->SeekForPrev(maxSeqnoSlice);

    if (!it->Valid()) {
        return 0;
    }
    auto highSeqno = getNumericSeqno(it->key());
    // We use a negative seqno as key for VBState. Do not consider it.
    return highSeqno >= 0 ? highSeqno : 0;
}

int64_t RocksDBKVStore::getVbstateKey() const {
    // We put the VBState into the SeqnoCF. As items in the SeqnoCF are ordered
    // by increasing-seqno, we reserve a negative special key to VBState so
    // that we can access it in O(1).
    return -9999;
}

RocksDBKVStore::RocksDBHandle::RocksDBHandle(const RocksDBKVStore& kvstore,
                                             rocksdb::DB& rdb)
    : snapshot(rdb.GetSnapshot(), SnapshotDeleter(rdb)) {
}

std::unique_ptr<KVFileHandle> RocksDBKVStore::makeFileHandle(Vbid vbid) const {
    return std::make_unique<RocksDBHandle>(*this, *rdb);
}

std::pair<KVStore::GetCollectionStatsStatus, Collections::VB::PersistedStats>
RocksDBKVStore::getCollectionStats(const KVFileHandle& kvFileHandle,
                                   CollectionID collection) const {
    // TODO JWW 2018-07-30 implement this, for testing purposes return dummy
    // values to imply the function didn't fail
    return {GetCollectionStatsStatus::Success,
            Collections::VB::PersistedStats()};
}

std::pair<KVStore::GetCollectionStatsStatus, Collections::VB::PersistedStats>
RocksDBKVStore::getCollectionStats(Vbid vbid, CollectionID collection) const {
    // TODO JWW 2018-07-30 implement this, for testing purposes return dummy
    // values to imply the function didn't fail
    return {GetCollectionStatsStatus::Success,
            Collections::VB::PersistedStats()};
}

std::unique_ptr<BySeqnoScanContext> RocksDBKVStore::initBySeqnoScanContext(
        std::unique_ptr<StatusCallback<GetValue>> cb,
        std::unique_ptr<StatusCallback<CacheLookup>> cl,
        Vbid vbid,
        uint64_t startSeqno,
        DocumentFilter options,
        ValueFilter valOptions,
        SnapshotSource source,
        std::unique_ptr<KVFileHandle> fileHandle) const {
    if (source == SnapshotSource::Historical) {
        throw std::runtime_error(
                "RocksDBKVStore::initBySeqnoScanContext: historicalSnapshot "
                "not implemented");
    }

    // As we cannot efficiently determine how many documents this scan will
    // find, we approximate this value with the seqno difference + 1
    // as scan is supposed to be inclusive at both ends,
    // seqnos 2 to 4 covers 3 docs not 4 - 2 = 2
    auto handle = std::move(fileHandle);
    if (!handle) {
        handle = makeFileHandle(vbid);
    }

    auto vbHandle = getVBHandle(vbid);
    auto diskState = readVBStateFromDisk(*vbHandle);
    return std::make_unique<BySeqnoScanContext>(
            std::move(cb),
            std::move(cl),
            vbid,
            std::move(handle),
            startSeqno,
            diskState.vbstate.highSeqno,
            0, /*TODO RDB: pass the real purge-seqno*/
            options,
            valOptions,
            /* documentCount */ diskState.vbstate.highSeqno - startSeqno + 1,
            diskState.vbstate,
            std::vector<Collections::KVStore::DroppedCollection>{
                    /*no collections in rocksdb*/});
}

ScanStatus RocksDBKVStore::scan(BySeqnoScanContext& ctx) const {
    if (ctx.lastReadSeqno == ctx.maxSeqno) {
        return ScanStatus::Success;
    }

    auto startSeqno = ctx.startSeqno;
    if (ctx.lastReadSeqno != 0) {
        startSeqno = ctx.lastReadSeqno + 1;
    }

    TRACE_EVENT2("RocksDBKVStore",
                 "scan",
                 "vbid",
                 ctx.vbid.get(),
                 "startSeqno",
                 startSeqno);

    rocksdb::ReadOptions snapshotOpts{rocksdb::ReadOptions()};

    auto& handle = static_cast<RocksDBHandle&>(*ctx.handle);
    snapshotOpts.snapshot = handle.snapshot.get();

    rocksdb::Slice startSeqnoSlice = getSeqnoSlice(&startSeqno);
    const auto vbh = getVBHandle(ctx.vbid);
    if (!vbh) {
        return ScanStatus::Failed;
    }
    std::unique_ptr<rocksdb::Iterator> it(
            rdb->NewIterator(snapshotOpts, vbh->seqnoCFH.get()));
    if (!it) {
        throw std::logic_error(
                "RocksDBKVStore::scan: rocksdb::Iterator to Seqno Column "
                "Family is nullptr");
    }
    it->Seek(startSeqnoSlice);

    rocksdb::Slice endSeqnoSlice = getSeqnoSlice(&ctx.maxSeqno);
    auto isPastEnd = [&endSeqnoSlice, this](rocksdb::Slice seqSlice) {
        return seqnoComparator.Compare(seqSlice, endSeqnoSlice) == 1;
    };

    for (; it->Valid() && !isPastEnd(it->key()); it->Next()) {
        scanTotalSeqnoHits++;
        auto seqno = getNumericSeqno(it->key());
        rocksdb::Slice keySlice = it->value();
        std::string valueStr;
        auto s = rdb->Get(
                snapshotOpts, vbh->defaultCFH.get(), keySlice, &valueStr);

        if (!s.ok()) {
            // TODO RDB: Old seqnos are never removed from the db!
            // If the item does not exist (s.isNotFound())
            // the seqno => key mapping could be removed; not even
            // a tombstone remains of that item.

            // Note: I account also the hits for deleted documents because it
            // is logically correct. But, we switch on the RocksDB built-in
            // Bloom Filter by default and we try to keep all the Filter blocks
            // in the BlockCache. So, I expect that the impact of old-seqno hits
            // is minimum in this case.
            scanOldSeqnoHits++;

            continue;
        }

        rocksdb::Slice valSlice(valueStr);

        DiskDocKey key{keySlice.data(), keySlice.size()};

        std::unique_ptr<Item> itm =
                makeItem(ctx.vbid,
                         key,
                         valSlice,
                         ctx.valFilter != ValueFilter::KEYS_ONLY ||
                                 key.getDocKey().isInSystemCollection());

        if (itm->getBySeqno() > seqno) {
            // TODO RDB: Old seqnos are never removed from the db!
            // If the item has a newer seqno now, the stale
            // seqno => key mapping could be removed
            scanOldSeqnoHits++;
            continue;
        } else if (itm->getBySeqno() < seqno) {
            throw std::logic_error(
                    "RocksDBKVStore::scan: index has a higher seqno"
                    "than the document in a snapshot!");
        }

        bool includeDeletes =
                (ctx.docFilter == DocumentFilter::NO_DELETES) ? false : true;
        bool onlyKeys =
                (ctx.valFilter == ValueFilter::KEYS_ONLY) ? true : false;

        // Skip deleted items if they were not requested - apart from
        // Prepared SyncWrites as the "deleted" there refers to the future
        // value (a Prepare is actually deleted using an Abort).
        // TODO RDB: refactor this to the MagmaKVStore style of skip for deleted
        // items
        int64_t byseqno = itm->getBySeqno();
        if (!includeDeletes && itm->isDeleted() && !itm->isPending()) {
            ctx.lastReadSeqno = byseqno;
            continue;
        }

        if (!key.getDocKey().isInSystemCollection()) {
            if (ctx.docFilter !=
                DocumentFilter::ALL_ITEMS_AND_DROPPED_COLLECTIONS) {
                if (ctx.collectionsContext.isLogicallyDeleted(key.getDocKey(),
                                                              byseqno)) {
                    ctx.lastReadSeqno = byseqno;
                    continue;
                }
            }

            CacheLookup lookup(key, byseqno, ctx.vbid);

            ctx.getCacheCallback().callback(lookup);

            auto status = ctx.getCacheCallback().getStatus();
            if (ctx.getCacheCallback().shouldYield()) {
                return ScanStatus::Yield;
            } else if (status == cb::engine_errc::key_already_exists) {
                ctx.lastReadSeqno = byseqno;
                continue;
            } else if (status != cb::engine_errc::success) {
                return ScanStatus::Cancelled;
            }
        }

        GetValue rv(std::move(itm), cb::engine_errc::success, -1, onlyKeys);
        ctx.getValueCallback().callback(rv);
        auto status = ctx.getValueCallback().getStatus();

        if (ctx.getValueCallback().shouldYield()) {
            return ScanStatus::Yield;
        } else if (status != cb::engine_errc::success) {
            return ScanStatus::Cancelled;
        }

        ctx.lastReadSeqno = byseqno;
    }

    cb_assert(it->status().ok()); // Check for any errors found during the scan

    return ScanStatus::Success;
}

bool RocksDBKVStore::getStatFromMemUsage(
        const rocksdb::MemoryUtil::UsageType type, size_t& value) const {
    std::vector<rocksdb::DB*> dbs = {rdb.get()};
    auto cache_set = getCachePointers();
    std::map<rocksdb::MemoryUtil::UsageType, uint64_t> usageByType;

    auto status = rocksdb::MemoryUtil::GetApproximateMemoryUsageByType(
            dbs, cache_set, &usageByType);
    if (!status.ok()) {
        logger.info(
                "RocksDBKVStore::getStatFromMemUsage: "
                "GetApproximateMemoryUsageByType error: {}",
                status.getState());
        return false;
    }

    value = usageByType.at(type);

    return true;
}

bool RocksDBKVStore::getStatFromStatistics(const rocksdb::Tickers ticker,
                                           size_t& value) const {
    const auto statistics = rdb->GetDBOptions().statistics;
    if (!statistics) {
        return false;
    }
    value = statistics->getTickerCount(ticker);
    return true;
}

bool RocksDBKVStore::getStatFromProperties(ColumnFamily cf,
                                           const std::string& property,
                                           size_t& value) const {
    value = 0;
    std::lock_guard<std::mutex> lg(vbhMutex);
    for (const auto& vbh : vbHandles) {
        if (vbh) {
            rocksdb::ColumnFamilyHandle* cfh = nullptr;
            switch (cf) {
            case ColumnFamily::Default:
                cfh = vbh->defaultCFH.get();
                break;
            case ColumnFamily::Seqno:
                cfh = vbh->seqnoCFH.get();
                break;
            }
            if (!cfh) {
                return false;
            }
            std::string out;
            if (!rdb->GetProperty(cfh, property, &out)) {
                return false;
            }
            value += std::stoull(out);
        }
    }

    return true;
}

// NOTE: THIS FUNCTION IS CURRENTLY NOT USED - see MB-34129
//
// As we implement a VBucket as a pair of two Column Families (a 'default' CF
// and a 'local+seqno' CF), we need to re-set the 'write_buffer_size' for each
// CF when the number of VBuckets managed by the current store changes. The
// goal is to keep the total allocation for all the Memtables under the
// 'rocksdb_memtables_ratio' given in configuration.
// Thus, this function performs the following basic steps:
//     1) Re-calculate the new sizes of all Memtables;
//     2) Apply the new sizes.
// We apply the new sizes using the rocksdb::DB::SetOptions() API. The
// 'write_buffer_size' is a dynamically changeable option. This call changes
// the size of mutable Memtables instantly. If the new size is below the
// current allocation for the Memtable, the next key-value pair added will mark
// the Memtable as immutable and will trigger a flush.
void RocksDBKVStore::applyMemtablesQuota(
        const std::lock_guard<std::mutex>& lock) {
    const auto vbuckets = getVBucketsCount(lock);

    // 1) If configuration.getMemtablesRatio() == 0.0, then
    //      we just want to use the baseline write_buffer_size.
    // 2) If vbuckets == 0, then there is no Memtable (this happens only
    //      when the underlying RocksDB instance has just been created).
    // On both cases the following logic does not apply, so the
    // write_buffer_size for both the 'default' and the 'seqno' CFs is left
    // to the baseline value.
    if (configuration.getMemtablesRatio() > 0.0 && vbuckets > 0) {
        const auto memtablesQuota = configuration.getBucketQuota() /
                                    configuration.getMaxShards() *
                                    configuration.getMemtablesRatio();
        // TODO: for now I am hard-coding the percentage of Memtables Quota
        // that we allocate for the 'deafult' (90%) and 'seqno' (10%) CFs. The
        // plan is to expose this percentage as a configuration parameter in a
        // follow-up patch.
        const auto defaultCFMemtablesQuota = memtablesQuota * 0.9;
        const auto seqnoCFMemtablesQuota =
                memtablesQuota - defaultCFMemtablesQuota;

        // Set the the write_buffer_size for the 'default' CF
        defaultCFOptions.write_buffer_size =
                defaultCFMemtablesQuota / vbuckets /
                defaultCFOptions.max_write_buffer_number;
        // Set the write_buffer_size for the 'seqno' CF
        seqnoCFOptions.write_buffer_size =
                seqnoCFMemtablesQuota / vbuckets /
                seqnoCFOptions.max_write_buffer_number;

        // Apply the new write_buffer_size
        const std::unordered_map<std::string, std::string>
                newDefaultCFWriteBufferSize{std::make_pair(
                        "write_buffer_size",
                        std::to_string(defaultCFOptions.write_buffer_size))};
        const std::unordered_map<std::string, std::string>
                newSeqnoCFWriteBufferSize{std::make_pair(
                        "write_buffer_size",
                        std::to_string(seqnoCFOptions.write_buffer_size))};
        for (const auto& vbh : vbHandles) {
            if (vbh) {
                auto status = rdb->SetOptions(vbh->defaultCFH.get(),
                                              newDefaultCFWriteBufferSize);
                if (!status.ok()) {
                    throw std::runtime_error(
                            "RocksDBKVStore::applyMemtablesQuota: SetOptions "
                            "failed for [" +
                            (vbh->vbid).to_string() + ", CF: default]: " +
                            status.getState());
                }
                status = rdb->SetOptions(vbh->seqnoCFH.get(),
                                         newSeqnoCFWriteBufferSize);
                if (!status.ok()) {
                    throw std::runtime_error(
                            "RocksDBKVStore::applyMemtablesQuota: SetOptions "
                            "failed for [ " +
                            (vbh->vbid).to_string() + ", CF: seqno]: " +
                            status.getState());
                }
            }
        }
    }

    // Overwrite Compaction options if Compaction Optimization is enabled
    // for the 'default' CF
    if (configuration.getDefaultCfOptimizeCompaction() == "level") {
        defaultCFOptions.OptimizeLevelStyleCompaction(
                defaultCFOptions.write_buffer_size);
    } else if (configuration.getDefaultCfOptimizeCompaction() == "universal") {
        defaultCFOptions.OptimizeUniversalStyleCompaction(
                defaultCFOptions.write_buffer_size);
    }
    // Overwrite Compaction options if Compaction Optimization is enabled
    // for the 'seqno' CF
    if (configuration.getSeqnoCfOptimizeCompaction() == "level") {
        seqnoCFOptions.OptimizeLevelStyleCompaction(
                seqnoCFOptions.write_buffer_size);
    } else if (configuration.getSeqnoCfOptimizeCompaction() == "universal") {
        seqnoCFOptions.OptimizeUniversalStyleCompaction(
                seqnoCFOptions.write_buffer_size);
    }
}

size_t RocksDBKVStore::getVBucketsCount(
        const std::lock_guard<std::mutex>&) const {
    uint16_t count = 0;
    for (const auto& vbh : vbHandles) {
        if (vbh) {
            count++;
        }
    }
    return count;
}

void RocksDBKVStore::addStats(const AddStatFn& add_stat, const void* c) const {
    KVStore::addStats(add_stat, c);
    const auto prefix = getStatsPrefix();

    // Per-shard stats.
    size_t val = 0;
    // Memory Usage
    if (getStat("kMemTableTotal", val)) {
        add_prefixed_stat(prefix, "rocksdb_kMemTableTotal", val, add_stat, c);
    }
    if (getStat("kMemTableUnFlushed", val)) {
        add_prefixed_stat(
                prefix, "rocksdb_kMemTableUnFlushed", val, add_stat, c);
    }
    if (getStat("kTableReadersTotal", val)) {
        add_prefixed_stat(
                prefix, "rocksdb_kTableReadersTotal", val, add_stat, c);
    }
    if (getStat("kCacheTotal", val)) {
        add_prefixed_stat(prefix, "rocksdb_kCacheTotal", val, add_stat, c);
    }
    // MemTable Size per-CF
    if (getStat("default_kSizeAllMemTables", val)) {
        add_prefixed_stat(
                prefix, "rocksdb_default_kSizeAllMemTables", val, add_stat, c);
    }
    if (getStat("seqno_kSizeAllMemTables", val)) {
        add_prefixed_stat(
                prefix, "rocksdb_seqno_kSizeAllMemTables", val, add_stat, c);
    }
    // Block Cache hit/miss
    if (getStat("rocksdb.block.cache.hit", val)) {
        add_prefixed_stat(prefix, "rocksdb_block_cache_hit", val, add_stat, c);
    }
    if (getStat("rocksdb.block.cache.miss", val)) {
        add_prefixed_stat(prefix, "rocksdb_block_cache_miss", val, add_stat, c);
    }
    if (getStat("rocksdb.block.cache.data.hit", val)) {
        add_prefixed_stat(
                prefix, "rocksdb_block_cache_data_hit", val, add_stat, c);
    }
    if (getStat("rocksdb.block.cache.data.miss", val)) {
        add_prefixed_stat(
                prefix, "rocksdb_block_cache_data_miss", val, add_stat, c);
    }
    if (getStat("rocksdb.block.cache.index.hit", val)) {
        add_prefixed_stat(
                prefix, "rocksdb_block_cache_index_hit", val, add_stat, c);
    }
    if (getStat("rocksdb.block.cache.index.miss", val)) {
        add_prefixed_stat(
                prefix, "rocksdb_block_cache_index_miss", val, add_stat, c);
    }
    if (getStat("rocksdb.block.cache.filter.hit", val)) {
        add_prefixed_stat(
                prefix, "rocksdb_block_cache_filter_hit", val, add_stat, c);
    }
    if (getStat("rocksdb.block.cache.filter.miss", val)) {
        add_prefixed_stat(
                prefix, "rocksdb_block_cache_filter_miss", val, add_stat, c);
    }
    // BlockCache Hit Ratio
    size_t hit = 0;
    size_t miss = 0;
    if (getStat("rocksdb.block.cache.data.hit", hit) &&
        getStat("rocksdb.block.cache.data.miss", miss) && (hit + miss) != 0) {
        const auto ratio =
                gsl::narrow_cast<int>(float(hit) / (hit + miss) * 10000);
        add_prefixed_stat(prefix,
                          "rocksdb_block_cache_data_hit_ratio",
                          ratio,
                          add_stat,
                          c);
    }
    if (getStat("rocksdb.block.cache.index.hit", hit) &&
        getStat("rocksdb.block.cache.index.miss", miss) && (hit + miss) != 0) {
        const auto ratio =
                gsl::narrow_cast<int>(float(hit) / (hit + miss) * 10000);
        add_prefixed_stat(prefix,
                          "rocksdb_block_cache_index_hit_ratio",
                          ratio,
                          add_stat,
                          c);
    }
    if (getStat("rocksdb.block.cache.filter.hit", hit) &&
        getStat("rocksdb.block.cache.filter.miss", miss) && (hit + miss) != 0) {
        const auto ratio =
                gsl::narrow_cast<int>(float(hit) / (hit + miss) * 10000);
        add_prefixed_stat(prefix,
                          "rocksdb_block_cache_filter_hit_ratio",
                          ratio,
                          add_stat,
                          c);
    }
    // Disk Usage per-CF
    if (getStat("default_kTotalSstFilesSize", val)) {
        add_prefixed_stat(
                prefix, "rocksdb_default_kTotalSstFilesSize", val, add_stat, c);
    }
    if (getStat("seqno_kTotalSstFilesSize", val)) {
        add_prefixed_stat(
                prefix, "rocksdb_seqno_kTotalSstFilesSize", val, add_stat, c);
    }
    // Scan stats
    if (getStat("scan_totalSeqnoHits", val)) {
        add_prefixed_stat(
                prefix, "rocksdb_scan_totalSeqnoHits", val, add_stat, c);
    }
    if (getStat("scan_oldSeqnoHits", val)) {
        add_prefixed_stat(
                prefix, "rocksdb_scan_oldSeqnoHits", val, add_stat, c);
    }
}

const KVStoreConfig& RocksDBKVStore::getConfig() const {
    return configuration;
}

vbucket_state RocksDBKVStore::getPersistedVBucketState(Vbid vbid) const {
    auto handle = getVBHandle(vbid);
    auto state = readVBStateFromDisk(*handle);
    if (!state.status.ok()) {
        throw std::runtime_error(
                "RocksDBKVStore::getPersistedVBucketState "
                "failed with status " +
                state.status.ToString());
    }

    return state.vbstate;
}

GetValue RocksDBKVStore::getBySeqno(KVFileHandle& handle,
                                    Vbid vbid,
                                    uint64_t seq,
                                    ValueFilter filter) const {
    // @todo: Add support for RocksDB
    return GetValue{nullptr, cb::engine_errc::no_such_key};
}

std::unique_ptr<TransactionContext> RocksDBKVStore::begin(
        Vbid vbid, std::unique_ptr<PersistenceCallback> pcb) {
    if (!startTransaction(vbid)) {
        return {};
    }

    return std::make_unique<RocksDBKVStoreTransactionContext>(
            *this, vbid, std::move(pcb));
}

RocksDBKVStoreTransactionContext::RocksDBKVStoreTransactionContext(
        KVStore& kvstore, Vbid vbid, std::unique_ptr<PersistenceCallback> cb)
    : TransactionContext(kvstore, vbid, std::move(cb)),
      pendingReqs(std::make_unique<RocksDBKVStore::PendingRequestQueue>()) {
}
