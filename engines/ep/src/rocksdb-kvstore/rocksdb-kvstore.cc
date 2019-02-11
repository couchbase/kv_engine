/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include "rocksdb-kvstore.h"
#include "rocksdb-kvstore_config.h"

#include "bucket_logger.h"
#include "ep_time.h"

#include "kvstore_priv.h"

#include <phosphor/phosphor.h>
#include <platform/sysinfo.h>
#include <rocksdb/convenience.h>
#include <rocksdb/filter_policy.h>

#include <nlohmann/json.hpp>
#include <stdio.h>
#include <string.h>
#include <algorithm>
#include <gsl/gsl>
#include <limits>
#include <thread>

#include "vbucket.h"

namespace rockskv {
// MetaData is used to serialize and de-serialize metadata respectively when
// writing a Document mutation request to RocksDB and when reading a Document
// from RocksDB.
class MetaData {
public:
    MetaData()
        : deleted(0),
          deleteSource(static_cast<uint8_t>(DeleteSource::Explicit)),
          version(0),
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
             int64_t bySeqno)
        : deleted(deleted),
          deleteSource(deleteSource),
          version(version),
          datatype(datatype),
          flags(flags),
          valueSize(valueSize),
          exptime(exptime),
          cas(cas),
          revSeqno(revSeqno),
          bySeqno(bySeqno){};

// The `#pragma pack(1)` directive and the order of members are to keep
// the size of MetaData as small as possible and uniform across different
// platforms.
#pragma pack(1)
    uint8_t deleted : 1;
    uint8_t deleteSource : 1;
    // Note: to utilise deleteSource properly, casting to the type DeleteSource
    // is strongly recommended. It is stored as a uint8_t for better packing.
    uint8_t version : 6;
    uint8_t datatype;
    uint32_t flags;
    uint32_t valueSize;
    time_t exptime;
    uint64_t cas;
    uint64_t revSeqno;
    int64_t bySeqno;
#pragma pack()
};
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
    RocksRequest(const Item& item, MutationRequestCallback& callback)
        : IORequest(item.getVBucketId(),
                    callback,
                    item.isDeleted(),
                    item.getKey(),
                    item.isPending()),
          docBody(item.getValue()) {
        docMeta = rockskv::MetaData(
                item.isDeleted(),
                (item.isDeleted() ? static_cast<uint8_t>(item.deletionSource())
                                  : 0),
                0,
                item.getDataType(),
                item.getFlags(),
                item.getNBytes(),
                item.isDeleted() ? ep_real_time() : item.getExptime(),
                item.getCas(),
                item.getRevSeqno(),
                item.getBySeqno());
    }

    const rockskv::MetaData& getDocMeta() {
        return docMeta;
    }

    // Get a rocksdb::Slice wrapping the Document MetaData
    rocksdb::Slice getDocMetaSlice() {
        return rocksdb::Slice(reinterpret_cast<char*>(&docMeta),
                              sizeof(docMeta));
    }

    // Get a rocksdb::Slice wrapping the Document Body
    rocksdb::Slice getDocBodySlice() {
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
    ColumnFamilyDeleter(rocksdb::DB& db) : db(db) {
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
class VBHandle {
public:
    VBHandle(rocksdb::DB& rdb,
             rocksdb::ColumnFamilyHandle* defaultCFH,
             rocksdb::ColumnFamilyHandle* seqnoCFH,
             Vbid vbid)
        : rdb(rdb),
          defaultCFH(ColumnFamilyPtr(defaultCFH, rdb)),
          seqnoCFH(ColumnFamilyPtr(seqnoCFH, rdb)),
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
    : KVStore(configuration),
      vbHandles(configuration.getMaxVBuckets()),
      in_transaction(false),
      scanCounter(0),
      logger(configuration.getLogger()) {
    cachedVBStates.resize(configuration.getMaxVBuckets());
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

    // Set number of background threads - note these are per-environment, so
    // are shared across all DB instances (vBuckets) and all Buckets.
    auto lowPri = configuration.getLowPriBackgroundThreads();
    if (lowPri == 0) {
        lowPri = cb::get_available_cpu_count();
    }
    rocksdb::Env::Default()->SetBackgroundThreads(lowPri, rocksdb::Env::LOW);

    auto highPri = configuration.getHighPriBackgroundThreads();
    if (highPri == 0) {
        highPri = cb::get_available_cpu_count();
    }
    rocksdb::Env::Default()->SetBackgroundThreads(highPri, rocksdb::Env::HIGH);

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

    // Calculate and apply the correct write_buffer_size for all Column
    // Families. The Memtable size of each CF depends on the count of existing
    // CFs in DB (besides other things). Thus, this must be called after
    // 'openDB' (so that all the existing CFs have been loaded).
    applyMemtablesQuota(std::lock_guard<std::mutex>(vbhMutex));

    // Read persisted VBs state
    for (const auto vbh : vbHandles) {
        if (vbh) {
            readVBState(*vbh);
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

    in_transaction = false;
}

void RocksDBKVStore::openDB() {
    auto dbname = getDBSubdir();
    createDataDir(dbname);

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

std::shared_ptr<VBHandle> RocksDBKVStore::getVBHandle(Vbid vbid) {
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

    // The number of VBuckets has increased, we need to re-balance the
    // Memtables Quota among the CFs of existing VBuckets.
    applyMemtablesQuota(lg);

    return vbHandles[vbid.get()];
}

std::string RocksDBKVStore::getDBSubdir() {
    return configuration.getDBName() + "/rocksdb." +
           std::to_string(configuration.getShardId());
}

bool RocksDBKVStore::begin(std::unique_ptr<TransactionContext> txCtx) {
    if (!txCtx) {
        throw std::invalid_argument("RocksDBKVStore::begin: txCtx is null");
    }
    in_transaction = true;
    transactionCtx = std::move(txCtx);
    return in_transaction;
}

bool RocksDBKVStore::commit(Collections::VB::Flush& collectionsFlush) {
    // This behaviour is to replicate the one in Couchstore.
    // If `commit` is called when not in transaction, just return true.
    if (!in_transaction) {
        return true;
    }

    if (pendingReqs.size() == 0) {
        in_transaction = false;
        return true;
    }

    // Swap `pendingReqs` with the temporary `commitBatch` so that we can
    // shorten the scope of the lock.
    std::vector<std::unique_ptr<RocksRequest>> commitBatch;
    {
        std::lock_guard<std::mutex> lock(writeMutex);
        std::swap(pendingReqs, commitBatch);
    }

    bool success = true;
    auto vbid = commitBatch[0]->getVBucketId();

    // Flush all documents to disk
    auto status = saveDocs(vbid, collectionsFlush, commitBatch);
    if (!status.ok()) {
        logger.warn(
                "RocksDBKVStore::commit: saveDocs error:{}, "
                "{}",
                status.code(),
                vbid);
        success = false;
    }

    commitCallback(status, commitBatch);

    // This behaviour is to replicate the one in Couchstore.
    // Set `in_transanction = false` only if `commit` is successful.
    if (success) {
        in_transaction = false;
        transactionCtx.reset();
    }

    return success;
}

static int getMutationStatus(rocksdb::Status status) {
    switch (status.code()) {
    case rocksdb::Status::Code::kOk:
        return MUTATION_SUCCESS;
    case rocksdb::Status::Code::kNotFound:
        // This return value causes ep-engine to drop the failed flush
        return DOC_NOT_FOUND;
    case rocksdb::Status::Code::kBusy:
        // This return value causes ep-engine to keep re-queueing the failed
        // flush
        return MUTATION_FAILED;
    default:
        throw std::runtime_error(
                std::string("getMutationStatus: RocksDB error:") +
                std::string(status.getState()));
    }
}

void RocksDBKVStore::commitCallback(
        rocksdb::Status status,
        const std::vector<std::unique_ptr<RocksRequest>>& commitBatch) {
    for (const auto& request : commitBatch) {
        auto dataSize = request->getDocMetaSlice().size() +
                        request->getDocBodySlice().size();
        const auto& key = request->getKey();
        /* update ep stats */
        ++st.io_num_write;
        st.io_write_bytes += (key.size() + dataSize);

        auto rv = getMutationStatus(status);
        if (request->isDelete()) {
            if (status.code()) {
                ++st.numDelFailure;
            } else {
                st.delTimeHisto.add(request->getDelta() / 1000);
            }
            if (rv != -1) {
                // TODO: Should set `rv` to 1 or 0 depending on if this is a
                // delete to an existing (1) or non-existing (0) item. However,
                // to achieve this we would need to perform a Get to RocksDB
                // which is costly. For now just assume that the item did
                // not exist.
                rv = 0;
            }
            request->getDelCallback()->callback(*transactionCtx, rv);
        } else {
            if (status.code()) {
                ++st.numSetFailure;
            } else {
                st.writeTimeHisto.add(request->getDelta() / 1000);
                st.writeSizeHisto.add(dataSize + key.size());
            }
            // TODO: Should set `mr.second` to true or false depending on if
            // this is an insertion (true) or an update of an existing item
            // (false). However, to achieve this we would need to perform a Get
            // to RocksDB which is costly. For now just assume that the item
            // did not exist.
            mutation_result mr = std::make_pair(1, true);
            request->getSetCallback()->callback(*transactionCtx, mr);
        }
    }
}

void RocksDBKVStore::rollback() {
    if (in_transaction) {
        in_transaction = false;
        transactionCtx.reset();
    }
}

std::vector<vbucket_state*> RocksDBKVStore::listPersistedVbuckets() {
    std::vector<vbucket_state*> result;
    for (const auto& vb : cachedVBStates) {
        result.emplace_back(vb.get());
    }
    return result;
}

void RocksDBKVStore::set(const Item& item,
                         Callback<TransactionContext, mutation_result>& cb) {
    if (!in_transaction) {
        throw std::logic_error(
                "RocksDBKVStore::set: in_transaction must be true to perform a "
                "set operation.");
    }
    MutationRequestCallback callback;
    callback.setCb = &cb;
    pendingReqs.push_back(std::make_unique<RocksRequest>(item, callback));
}

GetValue RocksDBKVStore::get(const StoredDocKey& key,
                             Vbid vb,
                             bool fetchDelete) {
    return getWithHeader(nullptr, key, vb, GetMetaOnly::No, fetchDelete);
}

GetValue RocksDBKVStore::getWithHeader(void* dbHandle,
                                       const StoredDocKey& key,
                                       Vbid vb,
                                       GetMetaOnly getMetaOnly,
                                       bool fetchDelete) {
    std::string value;
    const auto vbh = getVBHandle(vb);
    // TODO RDB: use a PinnableSlice to avoid some memcpy
    rocksdb::Slice keySlice = getKeySlice(key);
    rocksdb::Status s = rdb->Get(
            rocksdb::ReadOptions(), vbh->defaultCFH.get(), keySlice, &value);
    if (!s.ok()) {
        return GetValue{NULL, ENGINE_KEY_ENOENT};
    }
    return makeGetValue(vb, key, value, getMetaOnly);
}

void RocksDBKVStore::getMulti(Vbid vb, vb_bgfetch_queue_t& itms) {
    // TODO RDB: RocksDB supports a multi get which we should use here.
    for (auto& it : itms) {
        auto& key = it.first;
        rocksdb::Slice keySlice = getKeySlice(key);
        std::string value;
        const auto vbh = getVBHandle(vb);
        rocksdb::Status s = rdb->Get(rocksdb::ReadOptions(),
                                     vbh->defaultCFH.get(),
                                     keySlice,
                                     &value);
        if (s.ok()) {
            it.second.value =
                    makeGetValue(vb, key, value, it.second.isMetaOnly);
            GetValue* rv = &it.second.value;
            for (auto& fetch : it.second.bgfetched_list) {
                fetch->value = rv;
            }
        } else {
            for (auto& fetch : it.second.bgfetched_list) {
                fetch->value->setStatus(ENGINE_KEY_ENOENT);
            }
        }
    }
}

void RocksDBKVStore::reset(Vbid vbucketId) {
    // TODO RDB:  Implement.
}

void RocksDBKVStore::del(const Item& item,
                         Callback<TransactionContext, int>& cb) {
    if (!item.isDeleted()) {
        throw std::invalid_argument(
                "RocksDBKVStore::del item to delete is not marked as deleted.");
    }
    if (!in_transaction) {
        throw std::logic_error(
                "RocksDBKVStore::del: in_transaction must be true to perform a "
                "delete operation.");
    }
    // TODO: Deleted items remain as tombstones, but are not yet expired,
    // they will accumuate forever.
    MutationRequestCallback callback;
    callback.delCb = &cb;
    pendingReqs.push_back(std::make_unique<RocksRequest>(item, callback));
}

void RocksDBKVStore::delVBucket(Vbid vbid, uint64_t vb_version) {
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

    // The number of VBuckets has decreased, we need to re-balance the
    // Memtables Quota among the CFs of existing VBuckets.
    applyMemtablesQuota(lg2);
}

bool RocksDBKVStore::snapshotVBucket(Vbid vbucketId,
                                     const vbucket_state& vbstate,
                                     VBStatePersist options) {
    // TODO RDB: Refactor out behaviour common to this and CouchKVStore
    auto start = std::chrono::steady_clock::now();

    if (updateCachedVBState(vbucketId, vbstate) &&
        (options == VBStatePersist::VBSTATE_PERSIST_WITHOUT_COMMIT ||
         options == VBStatePersist::VBSTATE_PERSIST_WITH_COMMIT)) {
        const auto vbh = getVBHandle(vbucketId);
        rocksdb::WriteBatch batch;
        auto status = saveVBStateToBatch(*vbh, vbstate, batch);
        if (!status.ok()) {
            logger.warn(
                    "RocksDBKVStore::snapshotVBucket: saveVBStateToBatch() "
                    "failed state:{} {} :{}",
                    VBucket::toString(vbstate.state),
                    vbucketId,
                    status.getState());
            return false;
        }
        status = rdb->Write(writeOptions, &batch);
        if (!status.ok()) {
            logger.warn(
                    "RocksDBKVStore::snapshotVBucket: Write() "
                    "failed state:{} {} :{}",
                    VBucket::toString(vbstate.state),
                    vbucketId,
                    status.getState());
            return false;
        }
    }

    EP_LOG_DEBUG("RocksDBKVStore::snapshotVBucket: Snapshotted {} state:{}",
                 vbucketId,
                 vbstate.toJSON());

    st.snapshotHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start));

    return true;
}

bool RocksDBKVStore::snapshotStats(const std::map<std::string, std::string>&) {
    // TODO RDB:  Implement
    return true;
}

void RocksDBKVStore::destroyInvalidVBuckets(bool) {
    // TODO RDB:  implement
}

size_t RocksDBKVStore::getNumShards() {
    return configuration.getMaxShards();
}

bool RocksDBKVStore::getStat(const char* name_, size_t& value) {
    std::string name(name_);

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

StorageProperties RocksDBKVStore::getStorageProperties(void) {
    StorageProperties rv(StorageProperties::EfficientVBDump::Yes,
                         StorageProperties::EfficientVBDeletion::Yes,
                         StorageProperties::PersistedDeletion::No,
                         // TODO RDB: Not strictly true, multiGet
                         // does not yet use the underlying multi get
                         // of RocksDB
                         StorageProperties::EfficientGet::Yes,
                         StorageProperties::ConcurrentWriteCompact::Yes);
    return rv;
}

std::unordered_set<const rocksdb::Cache*> RocksDBKVStore::getCachePointers() {
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

rocksdb::Slice RocksDBKVStore::getKeySlice(const DocKey& key) {
    return rocksdb::Slice(reinterpret_cast<const char*>(key.data()),
                          key.size());
}

rocksdb::Slice RocksDBKVStore::getSeqnoSlice(const int64_t* seqno) {
    return rocksdb::Slice(reinterpret_cast<const char*>(seqno), sizeof(*seqno));
}

int64_t RocksDBKVStore::getNumericSeqno(const rocksdb::Slice& seqnoSlice) {
    assert(seqnoSlice.size() == sizeof(int64_t));
    int64_t seqno;
    std::memcpy(&seqno, seqnoSlice.data(), seqnoSlice.size());
    return seqno;
}

std::unique_ptr<Item> RocksDBKVStore::makeItem(Vbid vb,
                                               const DocKey& key,
                                               const rocksdb::Slice& s,
                                               GetMetaOnly getMetaOnly) {
    assert(s.size() >= sizeof(rockskv::MetaData));

    const char* data = s.data();

    rockskv::MetaData meta;
    std::memcpy(&meta, data, sizeof(meta));
    data += sizeof(meta);

    bool includeValue = getMetaOnly == GetMetaOnly::No && meta.valueSize;

    auto item = std::make_unique<Item>(key,
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

    return item;
}

GetValue RocksDBKVStore::makeGetValue(Vbid vb,
                                      const DocKey& key,
                                      const std::string& value,
                                      GetMetaOnly getMetaOnly) {
    rocksdb::Slice sval(value);
    return GetValue(
            makeItem(vb, key, sval, getMetaOnly), ENGINE_SUCCESS, -1, 0);
}

void RocksDBKVStore::readVBState(const VBHandle& vbh) {
    // Largely copied from CouchKVStore
    // TODO RDB: refactor out sections common to CouchKVStore
    vbucket_state_t state = vbucket_state_dead;
    uint64_t checkpointId = 0;
    uint64_t maxDeletedSeqno = 0;
    int64_t highSeqno = readHighSeqnoFromDisk(vbh);
    std::string failovers;
    uint64_t purgeSeqno = 0;
    uint64_t lastSnapStart = 0;
    uint64_t lastSnapEnd = 0;
    uint64_t maxCas = 0;
    int64_t hlcCasEpochSeqno = HlcCasSeqnoUninitialised;
    bool mightContainXattrs = false;
    bool supportsNamespaces = false;

    auto key = getVbstateKey();
    std::string vbstate;
    auto vbid = vbh.vbid;
    auto status = rdb->Get(rocksdb::ReadOptions(),
                           vbh.seqnoCFH.get(),
                           getSeqnoSlice(&key),
                           &vbstate);
    if (!status.ok()) {
        if (status.IsNotFound()) {
            logger.info(
                    "RocksDBKVStore::readVBState: '_local/vbstate.{}' not "
                    "found",
                    vbid.get());
        } else {
            logger.warn(
                    "RocksDBKVStore::readVBState: error getting vbstate "
                    "error:{}, {}",
                    status.getState(),
                    vbid);
        }
    } else {
        nlohmann::json json;
        try {
            json = nlohmann::json::parse(vbstate);
        } catch (const nlohmann::json::exception& e) {
            logger.warn(
                    "RocksKVStore::readVBState: Failed to parse the vbstat "
                    "json doc for {}, json:{} with reason:{}",
                    vbid,
                    vbstate,
                    e.what());
            return;
        }

        auto vb_state = json.value("state", "");
        auto checkpoint_id = json.value("checkpoint_id", "");
        auto max_deleted_seqno = json.value("max_deleted_seqno", "");
        auto snapStart = json.find("snap_start");
        auto snapEnd = json.find("snap_end");
        auto maxCasValue = json.find("max_cas");
        auto hlcCasEpoch = json.find("hlc_epoch");
        mightContainXattrs = json.value("might_contain_xattrs", false);
        supportsNamespaces = json.value("namespaces_supported", false);

        auto failover_json = json.find("failover_table");
        if (vb_state.empty() || checkpoint_id.empty() ||
            max_deleted_seqno.empty()) {
            std::stringstream error;
            logger.warn(
                    "RocksDBKVStore::readVBState: State"
                    " JSON doc for {} is in the wrong format:{}, "
                    "vb state:{}, checkpoint id:{} and max deleted seqno:{}",
                    vbid,
                    vbstate,
                    vb_state,
                    checkpoint_id,
                    max_deleted_seqno);
        } else {
            state = VBucket::fromString(vb_state.c_str());
            maxDeletedSeqno = std::stoull(max_deleted_seqno);
            checkpointId = std::stoull(checkpoint_id);

            if (snapStart == json.end()) {
                lastSnapStart = gsl::narrow<uint64_t>(highSeqno);
            } else {
                lastSnapStart = std::stoull(snapStart->get<std::string>());
            }

            if (snapEnd == json.end()) {
                lastSnapEnd = gsl::narrow<uint64_t>(highSeqno);
            } else {
                lastSnapEnd = std::stoull(snapEnd->get<std::string>());
            }

            if (maxCasValue != json.end()) {
                maxCas = std::stoull(maxCasValue->get<std::string>());
            }

            if (hlcCasEpoch != json.end()) {
                hlcCasEpochSeqno = std::stoull(hlcCasEpoch->get<std::string>());
            }

            if (failover_json != json.end()) {
                failovers = failover_json->dump();
            }
        }
    }

    cachedVBStates[vbh.vbid.get()] =
            std::make_unique<vbucket_state>(state,
                                            checkpointId,
                                            maxDeletedSeqno,
                                            highSeqno,
                                            purgeSeqno,
                                            lastSnapStart,
                                            lastSnapEnd,
                                            maxCas,
                                            hlcCasEpochSeqno,
                                            mightContainXattrs,
                                            failovers,
                                            supportsNamespaces);
}

rocksdb::Status RocksDBKVStore::saveVBStateToBatch(const VBHandle& vbh,
                                                   const vbucket_state& vbState,
                                                   rocksdb::WriteBatch& batch) {
    std::stringstream jsonState;

    jsonState << "{\"state\": \"" << VBucket::toString(vbState.state) << "\""
              << ",\"checkpoint_id\": \"" << vbState.checkpointId << "\""
              << ",\"max_deleted_seqno\": \"" << vbState.maxDeletedSeqno
              << "\"";
    if (!vbState.failovers.empty()) {
        jsonState << ",\"failover_table\": " << vbState.failovers;
    }
    jsonState << ",\"snap_start\": \"" << vbState.lastSnapStart << "\""
              << ",\"snap_end\": \"" << vbState.lastSnapEnd << "\""
              << ",\"max_cas\": \"" << vbState.maxCas << "\""
              << ",\"hlc_epoch\": \"" << vbState.hlcCasEpochSeqno << "\"";

    if (vbState.mightContainXattrs) {
        jsonState << ",\"might_contain_xattrs\": true";
    } else {
        jsonState << ",\"might_contain_xattrs\": false";
    }

    // This KV writes namespaces
    jsonState << ",\"namespaces_supported\": true";
    jsonState << "}";

    auto key = getVbstateKey();
    rocksdb::Slice keySlice = getSeqnoSlice(&key);
    return batch.Put(vbh.seqnoCFH.get(), keySlice, jsonState.str());
}

rocksdb::ColumnFamilyOptions RocksDBKVStore::getBaselineDefaultCFOptions() {
    rocksdb::ColumnFamilyOptions cfOptions;
    // Enable Point Lookup Optimization for the 'default' Column Family
    // Note: whatever we give in input as 'block_cache_size_mb', the Block
    // Cache will be reset with the shared 'blockCache' of size
    // 'rocksdb_block_cache_size'
    cfOptions.OptimizeForPointLookup(1);
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
        auto& configuration =
                dynamic_cast<RocksDBKVStoreConfig&>(this->configuration);
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
        Collections::VB::Flush& collectionsFlush,
        const std::vector<std::unique_ptr<RocksRequest>>& commitBatch) {
    auto reqsSize = commitBatch.size();
    if (reqsSize == 0) {
        st.docsCommitted = 0;
        return rocksdb::Status::OK();
    }

    auto& vbstate = cachedVBStates[vbid.get()];
    if (vbstate == nullptr) {
        throw std::logic_error("RocksDBKVStore::saveDocs: cachedVBStates[" +
                               vbid.to_string() + "] is NULL");
    }

    rocksdb::Status status;
    int64_t maxDBSeqno = 0;
    rocksdb::WriteBatch batch;

    const auto vbh = getVBHandle(vbid);

    for (const auto& request : commitBatch) {
        int64_t bySeqno = request->getDocMeta().bySeqno;
        maxDBSeqno = std::max(maxDBSeqno, bySeqno);

        status = addRequestToWriteBatch(*vbh, batch, request.get());
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

    status = saveVBStateToBatch(*vbh, *vbstate, batch);
    if (!status.ok()) {
        logger.warn("RocksDBKVStore::saveDocs: saveVBStateToBatch error:{}",
                    status.code());
        return status;
    }

    status = writeAndTimeBatch(batch);
    if (!status.ok()) {
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
    vbstate->highSeqno = maxDBSeqno;

    return rocksdb::Status::OK();
}

rocksdb::Status RocksDBKVStore::addRequestToWriteBatch(
        const VBHandle& vbh,
        rocksdb::WriteBatch& batch,
        RocksRequest* request) {
    Vbid vbid = request->getVBucketId();

    rocksdb::Slice keySlice = getKeySlice(request->getKey());
    rocksdb::SliceParts keySliceParts(&keySlice, 1);

    rocksdb::Slice docSlices[] = {request->getDocMetaSlice(),
                                  request->getDocBodySlice()};
    rocksdb::SliceParts valueSliceParts(docSlices, 2);

    rocksdb::Slice bySeqnoSlice = getSeqnoSlice(&request->getDocMeta().bySeqno);
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

int64_t RocksDBKVStore::readHighSeqnoFromDisk(const VBHandle& vbh) {
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

int64_t RocksDBKVStore::getVbstateKey() {
    // We put the VBState into the SeqnoCF. As items in the SeqnoCF are ordered
    // by increasing-seqno, we reserve a negative special key to VBState so
    // that we can access it in O(1).
    return -9999;
}

ScanContext* RocksDBKVStore::initScanContext(
        std::shared_ptr<StatusCallback<GetValue>> cb,
        std::shared_ptr<StatusCallback<CacheLookup>> cl,
        Vbid vbid,
        uint64_t startSeqno,
        DocumentFilter options,
        ValueFilter valOptions) {
    size_t scanId = scanCounter++;

    {
        std::lock_guard<std::mutex> lg(scanSnapshotsMutex);
        scanSnapshots.emplace(scanId, SnapshotPtr(rdb->GetSnapshot(), *rdb));
    }

    // As we cannot efficiently determine how many documents this scan will
    // find, we approximate this value with the seqno difference + 1
    // as scan is supposed to be inclusive at both ends,
    // seqnos 2 to 4 covers 3 docs not 4 - 2 = 2

    uint64_t endSeqno = cachedVBStates[vbid.get()]->highSeqno;
    return new ScanContext(cb,
                           cl,
                           vbid,
                           scanId,
                           startSeqno,
                           endSeqno,
                           0, /*TODO RDB: pass the real purge-seqno*/
                           options,
                           valOptions,
                           /* documentCount */ endSeqno - startSeqno + 1,
                           configuration,
                           {/*no collections in rocksdb*/});
}

scan_error_t RocksDBKVStore::scan(ScanContext* ctx) {
    if (!ctx) {
        return scan_failed;
    }

    if (ctx->lastReadSeqno == ctx->maxSeqno) {
        return scan_success;
    }

    auto startSeqno = ctx->startSeqno;
    if (ctx->lastReadSeqno != 0) {
        startSeqno = ctx->lastReadSeqno + 1;
    }

    TRACE_EVENT2("RocksDBKVStore",
                 "scan",
                 "vbid",
                 ctx->vbid.get(),
                 "startSeqno",
                 startSeqno);

    GetMetaOnly isMetaOnly = ctx->valFilter == ValueFilter::KEYS_ONLY
                                     ? GetMetaOnly::Yes
                                     : GetMetaOnly::No;

    rocksdb::ReadOptions snapshotOpts{rocksdb::ReadOptions()};

    // Lock for safe access to the scanSnapshots map and to ensure the snapshot
    // doesn't get destroyed whilst we have the pointer.
    // @todo use a shared_ptr and reduce the lock scope to just the map::at call
    std::lock_guard<std::mutex> lg(scanSnapshotsMutex);
    snapshotOpts.snapshot = scanSnapshots.at(ctx->scanId).get();

    rocksdb::Slice startSeqnoSlice = getSeqnoSlice(&startSeqno);
    const auto vbh = getVBHandle(ctx->vbid);
    std::unique_ptr<rocksdb::Iterator> it(
            rdb->NewIterator(snapshotOpts, vbh->seqnoCFH.get()));
    if (!it) {
        throw std::logic_error(
                "RocksDBKVStore::scan: rocksdb::Iterator to Seqno Column "
                "Family is nullptr");
    }
    it->Seek(startSeqnoSlice);

    rocksdb::Slice endSeqnoSlice = getSeqnoSlice(&ctx->maxSeqno);
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

        DocKey key(reinterpret_cast<const uint8_t*>(keySlice.data()),
                   keySlice.size(),
                   DocKeyEncodesCollectionId::Yes);

        std::unique_ptr<Item> itm =
                makeItem(ctx->vbid, key, valSlice, isMetaOnly);

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
                (ctx->docFilter == DocumentFilter::NO_DELETES) ? false : true;
        bool onlyKeys =
                (ctx->valFilter == ValueFilter::KEYS_ONLY) ? true : false;

        if (!includeDeletes && itm->isDeleted()) {
            continue;
        }
        int64_t byseqno = itm->getBySeqno();

        if (!key.getCollectionID().isSystem()) {
            if (ctx->docFilter !=
                DocumentFilter::ALL_ITEMS_AND_DROPPED_COLLECTIONS) {
                if (ctx->collectionsContext.isLogicallyDeleted(key, byseqno)) {
                    ctx->lastReadSeqno = byseqno;
                    continue;
                }
            }

            CacheLookup lookup(key, byseqno, ctx->vbid);

            ctx->lookup->callback(lookup);

            auto status = ctx->lookup->getStatus();
            if (status == ENGINE_KEY_EEXISTS) {
                ctx->lastReadSeqno = byseqno;
                continue;
            } else if (status == ENGINE_ENOMEM) {
                return scan_again;
            }
        }

        GetValue rv(std::move(itm), ENGINE_SUCCESS, -1, onlyKeys);
        ctx->callback->callback(rv);
        auto status = ctx->callback->getStatus();

        if (status == ENGINE_ENOMEM) {
            return scan_again;
        }

        ctx->lastReadSeqno = byseqno;
    }

    cb_assert(it->status().ok()); // Check for any errors found during the scan

    return scan_success;
}

void RocksDBKVStore::destroyScanContext(ScanContext* ctx) {
    if (ctx == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lg(scanSnapshotsMutex);
    // TODO RDB: Might be nice to have the snapshot in the ctx and
    // release it on destruction
    auto it = scanSnapshots.find(ctx->scanId);
    if (it != scanSnapshots.end()) {
        scanSnapshots.erase(it);
    }
    delete ctx;
}

bool RocksDBKVStore::getStatFromMemUsage(
        const rocksdb::MemoryUtil::UsageType type, size_t& value) {
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
                                           size_t& value) {
    const auto statistics = rdb->GetDBOptions().statistics;
    if (!statistics) {
        return false;
    }
    value = statistics->getTickerCount(ticker);
    return true;
}

bool RocksDBKVStore::getStatFromProperties(ColumnFamily cf,
                                           const std::string& property,
                                           size_t& value) {
    value = 0;
    std::lock_guard<std::mutex> lg(vbhMutex);
    for (const auto vbh : vbHandles) {
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

    auto& configuration =
            dynamic_cast<RocksDBKVStoreConfig&>(this->configuration);

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
