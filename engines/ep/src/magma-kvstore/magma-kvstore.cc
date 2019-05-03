/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include "magma-kvstore.h"
#include "bucket_logger.h"
#include "ep_time.h"
#include "item.h"
#include "magma-kvstore_config.h"
#include "vbucket.h"
#include "vbucket_state.h"

#include <string.h>
#include <algorithm>
#include <limits>

using namespace magma;
using namespace std::chrono_literals;

namespace magmakv {
// MetaData is used to serialize and de-serialize metadata respectively when
// writing a Document mutation request to Magma and when reading a Document
// from Magma.

// The `#pragma pack(1)` directive and the order of members are to keep
// the size of MetaData as small as possible and uniform across different
// platforms.
#pragma pack(1)
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

    MetaData() = default;

    MetaData(const Item& it)
        : metaDataVersion(0),
          bySeqno(it.getBySeqno()),
          cas(it.getCas()),
          revSeqno(it.getRevSeqno()),
          exptime(it.getExptime()),
          flags(it.getFlags()),
          valueSize(it.getNBytes()),
          vbid(it.getVBucketId().get()),
          datatype(it.getDataType()) {
        if (it.isDeleted()) {
            deleted = 1;
            deleteSource = static_cast<uint8_t>(it.deletionSource());
        } else {
            deleted = 0;
            deleteSource = 0;
        }
        operation = static_cast<uint8_t>(toOperation(it.getOperation()));
        durabilityLevel =
                static_cast<uint8_t>(it.getDurabilityReqs().getLevel());
    };

    // Magma requires meta data for local documents. Rather than support 2
    // different meta data versions, we simplify by using just 1.
    MetaData(bool isDeleted, uint32_t valueSize, int64_t seqno, Vbid vbid)
        : metaDataVersion(0),
          bySeqno(seqno),
          cas(0),
          revSeqno(0),
          exptime(0),
          flags(0),
          valueSize(valueSize),
          vbid(vbid.get()),
          datatype(0),
          operation(0),
          durabilityLevel(0) {
        if (isDeleted) {
            deleted = 1;
            deleteSource = static_cast<uint8_t>(DeleteSource::Explicit);
        } else {
            deleted = 0;
            deleteSource = 0;
        }
    };

    Operation getOperation() const {
        return static_cast<Operation>(operation);
    }

    cb::durability::Level getDurabilityLevel() const {
        return static_cast<cb::durability::Level>(durabilityLevel);
    }

    std::string to_string() const {
        std::stringstream ss;
        int vers = metaDataVersion;
        int dt = datatype;
        auto op = to_string(static_cast<Operation>(operation));
        cb::durability::Requirements req(getDurabilityLevel(), 0);
        ss << "bySeqno:" << bySeqno << " cas:" << cas << " exptime:" << exptime
           << " revSeqno:" << revSeqno << " flags:" << flags
           << " valueSize:" << valueSize << " vbid:" << vbid
           << " deleted:" << (deleted == 0 ? "false" : "true")
           << " deleteSource:"
           << (deleted == 0 ? " " : deleteSource == 0 ? "Explicit" : "TTL")
           << " version:" << vers << " datatype:" << dt << " operation:" << op
           << " durabilityLevel:" << cb::durability::to_string(req);
        return ss.str();
    }

    uint8_t metaDataVersion;
    int64_t bySeqno;
    uint64_t cas;
    uint64_t revSeqno;
    uint32_t exptime;
    uint32_t flags;
    uint32_t valueSize;
    uint16_t vbid;
    uint8_t datatype;
    uint8_t deleted : 1;
    uint8_t deleteSource : 1;
    uint8_t operation : 2;
    uint8_t durabilityLevel : 2;

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
                    "magma::MetaData::toOperation: Unsupported op " +
                    std::to_string(static_cast<uint8_t>(op)));
        }
    }

    static std::string to_string(Operation op) {
        switch (op) {
        case Operation::Mutation:
            return "Mutation";
        case Operation::PreparedSyncWrite:
            return "PreparedSyncWrite";
        case Operation::CommittedSyncWrite:
            return "CommittedSyncWrite";
        case Operation::Abort:
            return "Abort";
        }
        return "Unknonw";
    }
};
#pragma pack()

static_assert(sizeof(MetaData) == 41,
              "magmakv::MetaData is not the expected size.");
} // namespace magmakv

// Keys to localdb docs
static const Slice vbstateKey = {"_vbstate", strlen("_vbstate")};

/**
 * Simple routine used as part of logger to convert a key into hex and ascii
 */
static void hexdump(const char* buf,
                    const int len,
                    std::string& hex,
                    std::string& ascii) {
    char tmp[10];

    hex.resize(0);
    ascii.resize(0);

    for (int i = 0; i < len; i++) {
        if (isprint(buf[i])) {
            ascii.append(&buf[i], 1);
        } else {
            ascii.append(" ", 1);
        }

        snprintf(tmp, sizeof(tmp), "%02x", static_cast<uint8_t>(buf[i]));
        hex.append(tmp, 2);
    }
}

/**
 * Class representing a document to be persisted in Magma.
 */
class MagmaRequest : public IORequest {
public:
    /**
     * Constructor
     *
     * @param item Item instance to be persisted
     * @param callback Persistence Callback
     * @param logger Used for trace logging
     */
    MagmaRequest(const Item& item,
                 MutationRequestCallback callback,
                 std::shared_ptr<BucketLogger> logger)
        : IORequest(item.getVBucketId(), std::move(callback), DiskDocKey{item}),
          docMeta(magmakv::MetaData(item)),
          docBody(item.getValue()) {
        if (logger->should_log(spdlog::level::trace)) {
            logger->trace("Request:{}", to_string());
        }
    }

    magmakv::MetaData& getDocMeta() {
        return docMeta;
    }

    Vbid getVbID() const {
        return Vbid(docMeta.vbid);
    }

    size_t getKeyLen() const {
        return key.size();
    }

    const char* getKey() const {
        return reinterpret_cast<const char*>(key.data());
    }

    size_t getBodySize() const {
        return docBody ? docBody->valueSize() : 0;
    }

    char* getBodyData() const {
        return docBody ? const_cast<char*>(docBody->getData()) : nullptr;
    }

    size_t getMetaSize() const {
        return sizeof(magmakv::MetaData);
    }

    void markOldItemExists() {
        itemOldExists = true;
    }

    bool oldItemExists() const {
        return itemOldExists;
    }

    void markOldItemIsDelete() {
        itemOldIsDelete = true;
    }

    bool oldItemIsDelete() const {
        return itemOldIsDelete;
    }

    void markRequestFailed() {
        reqFailed = true;
    }

    bool requestFailed() const {
        return reqFailed;
    }

    std::string to_string() {
        std::string hex, ascii;
        hexdump(getKey(), getKeyLen(), hex, ascii);
        std::stringstream ss;
        ss << "Key:" << ascii << " " << hex
           << " docMeta:" << docMeta.to_string()
           << " itemOldExists:" << (itemOldExists ? "true" : "false")
           << " itemOldIsDelete:" << (itemOldIsDelete ? "true" : "false")
           << " reqFailed:" << (reqFailed ? "true" : "false");
        return ss.str();
    }

private:
    magmakv::MetaData docMeta;
    value_t docBody;
    bool itemOldExists{false};
    bool itemOldIsDelete{false};
    bool reqFailed{false};
};

/**
 * Magma stores an item in 3 slices... key, metadata, value
 * See libmagma/slice.h for more info on slices.
 *
 * Helper functions to pull metadata stuff out of the metadata slice.
 * The are used down in magma and are passed in as part of the configuration.
 */
static const uint64_t getSeqNum(const Slice& metaSlice) {
    magmakv::MetaData* docMeta = reinterpret_cast<magmakv::MetaData*>(
            const_cast<char*>(metaSlice.Data()));
    return docMeta->bySeqno;
}

static const uint32_t getExpiryTime(const Slice& metaSlice) {
    magmakv::MetaData* docMeta = reinterpret_cast<magmakv::MetaData*>(
            const_cast<char*>(metaSlice.Data()));
    return docMeta->exptime;
}

static const bool isDeleted(const Slice& metaSlice) {
    magmakv::MetaData* docMeta = reinterpret_cast<magmakv::MetaData*>(
            const_cast<char*>(metaSlice.Data()));
    return docMeta->deleted > 0 ? true : false;
}

static DiskDocKey makeDiskDocKey(const Slice& key) {
    return DiskDocKey{key.Data(), key.Len()};
}

class KVMagma {
public:
    KVMagma(const Vbid vb, const std::string path) : vbid(vb.get()) {
        // open magma
    }

    int SetOrDel(const MagmaRequest* req) {
        if (req->isDelete()) {
            ; // TODO should we have a merge delta ops for old value?
        }
        return 0;
    }

    int Get(const DiskDocKey& key, void** value, int* valueLen) {
        return 0;
    }

    Vbid vbid;
    int magmaHandleId;
};

MagmaKVStore::MagmaKVStore(MagmaKVStoreConfig& configuration)
    : KVStore(configuration),
      vbDB(configuration.getMaxVBuckets()),
      pendingReqs(std::make_unique<PendingRequestQueue>()),
      in_transaction(false),
      magmaPath(configuration.getDBName() + "/magma." +
                std::to_string(configuration.getShardId())),
      scanCounter(0) {
    const size_t memtablesQuota = configuration.getBucketQuota() /
                                  configuration.getMaxShards() *
                                  configuration.getMagmaMemQuotaRatio();

    // The writeCacheSize is based on the bucketQuota but also must be
    // between minWriteCacheSize(8MB) and maxWriteCacheSize(128MB).
    // Too small of a writeCache, high write amplification.
    // Too large of a writeCache, high space amplification.
    size_t writeCacheSize =
            std::min(memtablesQuota, configuration.getMagmaMaxWriteCache());
    writeCacheSize =
            std::max(writeCacheSize, configuration.getMagmaMinWriteCache());

    configuration.magmaCfg.Path = magmaPath;
    configuration.magmaCfg.MaxKVStores = configuration.getMaxVBuckets();
    configuration.magmaCfg.MaxKVStoreLSDBufferSize =
            configuration.getMagmaDeleteMemtableWritecache();
    configuration.magmaCfg.LSDFragmentationRatio =
            configuration.getMagmaDeleteFragRatio();
    configuration.magmaCfg.MaxCommitPoints =
            configuration.getMagmaMaxCommitPoints();
    auto commitPointInterval = configuration.getMagmaCommitPointInterval();
    configuration.magmaCfg.CommitPointInterval =
            commitPointInterval * std::chrono::milliseconds{1min};
    configuration.magmaCfg.MinValueSize =
            configuration.getMagmaValueSeparationSize();
    configuration.magmaCfg.MaxWriteCacheSize = writeCacheSize;
    configuration.magmaCfg.WALBufferSize =
            configuration.getMagmaWalBufferSize();
    configuration.magmaCfg.NumFlushers = configuration.getMagmaNumFlushers();
    configuration.magmaCfg.NumCompactors =
            configuration.getMagmaNumCompactors();
    configuration.magmaCfg.WALSyncTime = 0ms;
    configuration.magmaCfg.ExpiryFragThreshold =
            configuration.getMagmaExpiryFragThreshold();
    configuration.magmaCfg.TombstoneFragThreshold =
            configuration.getMagmaTombstoneFragThreshold();
    configuration.magmaCfg.GetSeqNum = getSeqNum;
    configuration.magmaCfg.GetExpiryTime = getExpiryTime;
    configuration.magmaCfg.IsTombstone = isDeleted;

    cachedVBStates.resize(configuration.getMaxVBuckets());
    cachedMagmaInfo.resize(configuration.getMaxVBuckets());

    commitPointEveryBatch = configuration.getMagmaCommitPointEveryBatch();
    useUpsertForSet = configuration.getMagmaEnableUpsert();
    if (useUpsertForSet) {
        configuration.magmaCfg.EnableUpdateStatusForSet = false;
    } else {
        configuration.magmaCfg.EnableUpdateStatusForSet = true;
    }

    // Set up thread and memory tracking.
    configuration.magmaCfg.SetupThreadContext = []() {
        ObjectRegistry::onSwitchThread(ObjectRegistry::getCurrentEngine(),
                                       false);
    };

    // This allows magma writeCache to include its memory in the memory
    // tracking.
    configuration.magmaCfg.WriteCacheAllocationCallback = [](size_t size,
                                                             bool alloc) {
        if (alloc) {
            ObjectRegistry::memoryAllocated(size);
        } else {
            ObjectRegistry::memoryDeallocated(size);
        }
    };

    // Create the data directory.
    createDataDir(configuration.magmaCfg.Path);

    // Create a logger that is prefixed with magma so that all code from
    // wrapper down through magma use this logger.
    std::string loggerName =
            "magma_" + std::to_string(configuration.getShardId());
    logger = BucketLogger::createBucketLogger(loggerName, loggerName);
    configuration.magmaCfg.LogContext = logger;

    logger->info(
            "MagmaKVStore Constructor"
            " Path:{}"
            " BucketQuota:{}MB"
            " #Shards:{}"
            " MemQuotaRatio:{}"
            " MaxWriteCacheSize:{}MB"
            " MaxKVStores:{}"
            " MaxKVStoreLSDBufferSize:{}"
            " LSDFragmentationRatio:{} "
            " MaxCommitPoints:{}"
            " CommitPointInterval:{}min"
            " BatchCommitPoint:{}"
            " MinValueSize:{}"
            " UseUpsert:{}"
            " WALBufferSize:{}KB"
            " WalSyncTime:{}ms"
            " NumFlushers:{}"
            " NumCompactors:{}"
            " ExpiryFragThreshold:{}"
            " TombstoneFragThreshold:{}",
            configuration.magmaCfg.Path,
            configuration.getBucketQuota() / 1024 / 1024,
            configuration.getMaxShards(),
            configuration.getMagmaMemQuotaRatio(),
            configuration.magmaCfg.MaxWriteCacheSize / 1024 / 1024,
            configuration.magmaCfg.MaxKVStores,
            configuration.magmaCfg.MaxKVStoreLSDBufferSize,
            configuration.magmaCfg.LSDFragmentationRatio,
            configuration.magmaCfg.MaxCommitPoints,
            configuration.getMagmaCommitPointInterval(),
            commitPointEveryBatch,
            configuration.magmaCfg.MinValueSize,
            useUpsertForSet,
            configuration.magmaCfg.WALBufferSize / 1024,
            int(configuration.magmaCfg.WALSyncTime /
                std::chrono::milliseconds{1ms}),
            configuration.magmaCfg.NumFlushers,
            configuration.magmaCfg.NumCompactors,
            configuration.magmaCfg.ExpiryFragThreshold,
            configuration.magmaCfg.TombstoneFragThreshold);

    // Open the magma instance for this shard and populate the
    // vbstate and magmaInfo.
    magma = std::make_unique<Magma>(configuration.magmaCfg);
    auto status = magma->Open();
    if (!status) {
        std::string err = "Magma open failed. Error:" + status.String();
        logger->critical(err);
        throw std::logic_error(err);
    }
    auto kvstoreList = magma->GetKVStoreList();
    for (auto& kvid : kvstoreList) {
        auto vbstate = getVBucketState(Vbid(kvid));
        if (!vbstate) {
            throw std::logic_error("MagmaKVStore vbstate vbid:" +
                                   std::to_string(kvid) + " not found.");
        }
    }
}

MagmaKVStore::~MagmaKVStore() {
    if (!in_transaction) {
        magma->Sync();
    }
    logger->debug("MagmaKVStore Destructor");
}

std::string MagmaKVStore::getVBDBSubdir(Vbid vbid) {
    return magmaPath + std::to_string(vbid.get());
}

std::vector<Vbid> MagmaKVStore::discoverVBuckets() {
    std::vector<Vbid> vbids;
    auto vbDirs = cb::io::findFilesContaining(magmaPath, "");
    for (const auto& dir : vbDirs) {
        size_t lastDotIndex = dir.rfind(".");
        size_t vbidLength = dir.size() - lastDotIndex - 1;
        std::string vbidStr = dir.substr(lastDotIndex + 1, vbidLength);
        Vbid vbid(std::stoi(vbidStr.c_str()));
        // Take in account only VBuckets managed by this Shard
        if ((vbid.get() % configuration.getMaxShards()) ==
            configuration.getShardId()) {
            vbids.push_back(vbid);
        }
    }
    return vbids;
}

bool MagmaKVStore::begin(std::unique_ptr<TransactionContext> txCtx) {
    in_transaction = true;
    transactionCtx = std::move(txCtx);
    return in_transaction;
}

bool MagmaKVStore::commit(Collections::VB::Flush& collectionsFlush) {
    // This behaviour is to replicate the one in Couchstore.
    // If `commit` is called when not in transaction, just return true.
    if (!in_transaction) {
        logger->warn("MagmaKVStore::commit called not in transaction");
        return true;
    }

    if (pendingReqs->size() == 0) {
        in_transaction = false;
        return true;
    }

    kvstats_ctx kvctx(collectionsFlush);
    bool success = true;

    // Flush all documents to disk
    auto status = saveDocs(collectionsFlush, kvctx);
    if (status) {
        logger->warn("MagmaKVStore::commit: saveDocs {} error:{}",
                     pendingReqs->front().getVbID(),
                     status);
        success = false;
    }

    commitCallback(status, kvctx);

    // This behaviour is to replicate the one in Couchstore.
    // Set `in_transanction = false` only if `commit` is successful.
    if (success) {
        in_transaction = false;
        transactionCtx.reset();
    }

    pendingReqs->clear();
    logger->trace("MagmaKVStore::commit success:{}", success);

    return success;
}

void MagmaKVStore::commitCallback(int errCode, kvstats_ctx&) {
    for (const auto& req : *pendingReqs) {
        if (errCode) {
            ++st.numSetFailure;
        }

        size_t mutationSize =
                req.getKeyLen() + req.getBodySize() + req.getMetaSize();
        st.io_num_write++;
        st.io_write_bytes += mutationSize;

        // Note:
        // There are 3 cases for setting rv based on errCode
        // MUTATUION_SUCCESS
        // DOC_NOT_FOUND
        // MUTATION_FAILED - magma does not support this case because
        //     we don't want to requeue the item.  If errCode is bad,
        //     its most likely something fatal with the kvstore
        //     or it might be the kvstore is pending being dropped.
        int rv = DOC_NOT_FOUND;
        if (req.oldItemExists() && !req.oldItemIsDelete()) {
            rv = MUTATION_SUCCESS;
        }

        if (req.isDelete()) {
            if (req.requestFailed()) {
                st.numDelFailure++;
            } else {
                st.delTimeHisto.add(req.getDelta());
            }

            logger->trace("commitCallback {} errCode:{} getDelCallback rv:{}",
                          pendingReqs->front().getVbID(),
                          errCode,
                          rv);

            req.getDelCallback()(*transactionCtx, rv);
        } else {
            int mutationStatus = MUTATION_SUCCESS;
            if (req.oldItemIsDelete()) {
                rv = DOC_NOT_FOUND;
            }

            if (req.requestFailed()) {
                st.numSetFailure++;
                mutationStatus = MUTATION_FAILED;
            } else {
                st.writeTimeHisto.add(req.getDelta());
                st.writeSizeHisto.add(mutationSize);
            }
            mutation_result p(mutationStatus,
                              rv == DOC_NOT_FOUND ? true : false);
            req.getSetCallback()(*transactionCtx, p);

            logger->trace(
                    "commitCallback {} errCode:{} getSetCallback rv:{} "
                    "insertion:{}",
                    pendingReqs->front().getVbID(),
                    errCode,
                    rv,
                    mutationStatus);
        }
    }
}

void MagmaKVStore::rollback() {
    if (in_transaction) {
        in_transaction = false;
        transactionCtx.reset();
    }
}

StorageProperties MagmaKVStore::getStorageProperties() {
    StorageProperties rv(StorageProperties::EfficientVBDump::Yes,
                         StorageProperties::EfficientVBDeletion::Yes,
                         StorageProperties::PersistedDeletion::No,
                         StorageProperties::EfficientGet::Yes,
                         StorageProperties::ConcurrentWriteCompact::Yes);
    return rv;
}

std::vector<vbucket_state*> MagmaKVStore::listPersistedVbuckets() {
    std::vector<vbucket_state*> result;
    for (const auto& vb : cachedVBStates) {
        result.emplace_back(vb.get());
    }
    return result;
}

void MagmaKVStore::set(const Item& item, SetCallback cb) {
    if (!in_transaction) {
        throw std::logic_error(
                "MagmaKVStore::set: in_transaction must be true to perform a "
                "set operation.");
    }
    pendingReqs->emplace_back(item, std::move(cb), logger);
}

GetValue MagmaKVStore::get(const DiskDocKey& key, Vbid vb, bool fetchDelete) {
    return getWithHeader(nullptr, key, vb, GetMetaOnly::No, fetchDelete);
}

GetValue MagmaKVStore::getWithHeader(void* dbHandle,
                                     const DiskDocKey& key,
                                     Vbid vb,
                                     GetMetaOnly getMetaOnly,
                                     bool fetchDelete) {
    void* value = nullptr;
    int valueLen = 0;
    KVMagma db(vb, magmaPath);
    int status = db.Get(key, &value, &valueLen);
    if (status < 0) {
        logger->warn(
                "MagmaKVStore::getWithHeader: magma::DB::Lookup error:{}, "
                "vb:{}",
                status,
                vb);
    }
    std::string valStr(reinterpret_cast<char*>(value), valueLen);
    return makeGetValue(vb, key, valStr, getMetaOnly);
}

void MagmaKVStore::getMulti(Vbid vb, vb_bgfetch_queue_t& itms) {
    KVMagma db(vb, magmaPath);
    for (auto& it : itms) {
        auto& key = it.first;
        void* value = nullptr;
        int valueLen = 0;
        int status = db.Get(key, &value, &valueLen);
        if (status < 0) {
            logger->warn(
                    "MagmaKVStore::getMulti: magma::DB::Lookup error:{}, "
                    "vb:{}",
                    status,
                    vb);
            for (auto& fetch : it.second.bgfetched_list) {
                fetch->value->setStatus(ENGINE_KEY_ENOENT);
            }
            continue;
        }
        std::string valStr(reinterpret_cast<char*>(value), valueLen);
        it.second.value = makeGetValue(vb, key, valStr, it.second.isMetaOnly);
        GetValue* rv = &it.second.value;
        for (auto& fetch : it.second.bgfetched_list) {
            fetch->value = rv;
        }
    }
}

void MagmaKVStore::reset(Vbid vbucketId) {
    // TODO storage-team 2018-10-9 need to implement
}

void MagmaKVStore::del(const Item& item, KVStore::DeleteCallback cb) {
    if (!in_transaction) {
        throw std::logic_error(
                "MagmaKVStore::del: in_transaction must be true to perform a "
                "delete operation.");
    }
    pendingReqs->emplace_back(item, std::move(cb), logger);
}

void MagmaKVStore::delVBucket(Vbid vbid, uint64_t vb_version) {
    std::lock_guard<std::mutex> lg(writeLock);
    // TODO: check if needs lock on `openDBMutex`.
    vbDB[vbid.get()].reset();
    // Just destroy the DB in the sub-folder for vbid
    auto dbname = getVBDBSubdir(vbid);
    // DESTROY DB...
}

// Note: It is assumed this can only be called from bg flusher thread or
// there will be issues with writes coming from multiple threads.
bool MagmaKVStore::snapshotVBucket(Vbid vbucketId,
                                   const vbucket_state& vbstate,
                                   VBStatePersist options) {
    auto start = std::chrono::steady_clock::now();

    // If the cached entry is empty, first attempt to fill it
    // from the disk.
    if (!cachedVBStates[vbucketId.get()]) {
        auto status = readVBStateFromDisk(vbucketId);
        if (!status) {
            switch (status.ErrorCode()) {
            case magma::Status::Code::NotExists: // kvstore doesn't exist
            case magma::Status::Code::NotFound: // pending delete
                break;
            default:
                throw std::logic_error("MagmaKVStore::snapshotVBucket " +
                                       status.String());
            }
            // Since we have an empty cache and there is nothing on
            // disk, we need to initialize magmaInfo cache because
            // cachedVBStates will get initialized in
            // updateCachedVBState.
            auto minfo = std::make_unique<MagmaInfo>();
            cachedMagmaInfo[vbucketId.get()] = std::move(minfo);
        }
    }

    if (updateCachedVBState(vbucketId, vbstate) &&
        // snapshotVBucket for magma doesn't really support a
        // VBSTATE_PERSIST_WITHOUT_COMMIT option. Since this is going to write
        // the vbstate to the local DB and its not part of an ongoing
        // commitBatch, all we can do is create a commitBatch for this
        // snapshotVBucket call and commit it to disk. Fortunately, it appears
        // VBSTATE_PERSIST_WITHOUT_COMMIT is not used except in testing.
        (options == VBStatePersist::VBSTATE_PERSIST_WITHOUT_COMMIT ||
         options == VBStatePersist::VBSTATE_PERSIST_WITH_COMMIT)) {
        if (in_transaction) {
            throw std::logic_error(
                    "MagmaKVStore::snapshotVBucket called while transaction "
                    "active.");
        }
        std::unique_ptr<Magma::CommitBatch> batch;
        auto status = magma->NewCommitBatch(vbucketId.get(), batch);
        if (!status) {
            logger->critical(
                    "MagmaKVStore::snapshotVBucket failed creating "
                    "commitBatch for "
                    "{} err:{}",
                    vbucketId,
                    status.String());
            return false;
        }
        writeVBStateToDisk(vbucketId, *(batch.get()));
        status = magma->ExecuteCommitBatch(std::move(batch));
        if (!status) {
            logger->critical(
                    "MagmaKVStore::snapshotVBucket: "
                    "magma::ExecuteCommitBatch "
                    "{} error:{}",
                    vbucketId,
                    status.String());
            return false;
        }
        status = magma->SyncCommitBatches(true);
        if (!status) {
            logger->critical(
                    "MagmaKVStore::snapshotVBucket: "
                    "magma::SyncCommitBatches {} "
                    "error:{}",
                    vbucketId,
                    status.String());
            return false;
        }
    }

    st.snapshotHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start));

    return true;
}

bool MagmaKVStore::snapshotStats(const std::map<std::string, std::string>&) {
    // TODO storage-team 2018-10-9 need to implement
    return true;
}

void MagmaKVStore::destroyInvalidVBuckets(bool) {
    // TODO storage-team 2018-10-9 need to implement
}

size_t MagmaKVStore::getNumShards() const {
    return configuration.getMaxShards();
}

std::unique_ptr<Item> MagmaKVStore::makeItem(Vbid vb,
                                             const DiskDocKey& key,
                                             const std::string& value,
                                             GetMetaOnly getMetaOnly) {
    Expects(value.size() >= sizeof(magmakv::MetaData));

    const char* data = value.c_str();

    magmakv::MetaData meta;
    std::memcpy(&meta, data, sizeof(meta));
    data += sizeof(meta);

    bool includeValue = getMetaOnly == GetMetaOnly::No && meta.valueSize;

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
        item->setDeleted();
    }

    return item;
}

GetValue MagmaKVStore::makeGetValue(Vbid vb,
                                    const DiskDocKey& key,
                                    const std::string& value,
                                    GetMetaOnly getMetaOnly) {
    return GetValue(
            makeItem(vb, key, value, getMetaOnly), ENGINE_SUCCESS, -1, 0);
}

int MagmaKVStore::saveDocs(Collections::VB::Flush& collectionsFlush,
                           kvstats_ctx& kvctx) {
    uint64_t ninserts = 0;
    uint64_t ndeletes = 0;
    int64_t lastSeqno = 0;

    auto vbid = pendingReqs->front().getVbID();

    // Start a magma CommitBatch.
    // This is an atomic batch of items... all or nothing.
    std::unique_ptr<Magma::CommitBatch> batch;
    Status status = magma->NewCommitBatch(vbid.get(), batch);
    if (!status) {
        logger->warn("MagmaKVStore::saveDocs NewCommitBatch failed {} err:{}",
                     vbid,
                     status.String());
        return status.ErrorCode();
    }

    auto begin = std::chrono::steady_clock::now();

    for (auto& req : *pendingReqs) {
        Slice key = Slice{req.getKey(), req.getKeyLen()};
        auto docMeta = req.getDocMeta();
        Slice meta = Slice{reinterpret_cast<char*>(&docMeta),
                           sizeof(magmakv::MetaData)};
        Slice value = Slice{req.getBodyData(), req.getBodySize()};

        if (req.getDocMeta().bySeqno > lastSeqno) {
            lastSeqno = req.getDocMeta().bySeqno;
        }

        bool found{false};
        bool tombstone{false};
        status = batch->Set(key, meta, value, &found, &tombstone);
        if (!status) {
            logger->warn("MagmaKVStore::saveDocs: Set {} error {}",
                         vbid,
                         status.String());
            req.markRequestFailed();
            continue;
        }

        if (logger->should_log(spdlog::level::debug)) {
            std::string hex, ascii;
            hexdump(key.Data(), key.Len(), hex, ascii);
            logger->debug(
                    "MagmaKVStore::saveDocs {} key:{} {} seqno:{} delete:{} "
                    "found:{} "
                    "tombstone:{}",
                    vbid,
                    ascii,
                    hex,
                    req.getDocMeta().bySeqno,
                    req.isDelete(),
                    found,
                    tombstone);
        }

        if (found) {
            req.markOldItemExists();
            if (tombstone) {
                req.markOldItemIsDelete();
                // Old item is a delete and new is an insert.
                if (!req.isDelete()) {
                    ninserts++;
                }
            } else if (req.isDelete()) {
                // Old item is insert and new is delete.
                ndeletes++;
            }
        } else {
            // Old item doesn't exist and new is an insert.
            if (!req.isDelete()) {
                ninserts++;
            }
        }

        auto collectionsKey = makeDiskDocKey(key);
        kvctx.keyStats[collectionsKey] = false;

        if (req.oldItemExists()) {
            if (!req.oldItemIsDelete()) {
                // If we are replacing the item...
                auto itr = kvctx.keyStats.find(collectionsKey);
                if (itr != kvctx.keyStats.end()) {
                    itr->second = true;
                }
            }
        }
    }

    auto* vbstate = getVBucketState(vbid);
    if (vbstate) {
        vbstate->highSeqno = lastSeqno;
    }

    auto magmaInfo = getMagmaInfo(vbid);
    magmaInfo.docCount += ninserts - ndeletes;
    magmaInfo.persistedDeletes += ndeletes;

    // Write out current vbstate to the CommitBatch.
    status = writeVBStateToDisk(vbid, *(batch.get()));
    if (!status) {
        return status.ErrorCode();
    }

    // Done with the CommitBatch so add in the endTxn and create
    // a snapshot so that documents can not be read.
    status = magma->ExecuteCommitBatch(std::move(batch));
    if (!status) {
        logger->warn("MagmaKVStore::saveDocs: ExecuteCommitBatch {} error {}",
                     vbid,
                     status.ErrorCode());
    } else {
        // Flush the WAL to disk.
        // If the shard writeCache is full, this will trigger all the
        // KVStores to persist their memtables.
        status = magma->SyncCommitBatches(commitPointEveryBatch);
        if (!status) {
            logger->warn(
                    "MagmaKVStore::saveDocs: SyncCommitBatches {} error {}",
                    vbid,
                    status.ErrorCode());
        }
    }

    st.commitHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - begin));

    return status.ErrorCode();
}

ScanContext* MagmaKVStore::initScanContext(
        std::shared_ptr<StatusCallback<GetValue>> cb,
        std::shared_ptr<StatusCallback<CacheLookup>> cl,
        Vbid vbid,
        uint64_t startSeqno,
        DocumentFilter options,
        ValueFilter valOptions) {
    size_t scanId = scanCounter++;

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
                           0, /*TODO MAGMA: pass the read purge-seqno */
                           options,
                           valOptions,
                           /* documentCount */ endSeqno - startSeqno + 1,
                           configuration,
                           {/* TODO: add collections in magma */});
}

scan_error_t MagmaKVStore::scan(ScanContext* ctx) {
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
    (void)startSeqno;

    return scan_success;
}

void MagmaKVStore::destroyScanContext(ScanContext* ctx) {
    // TODO Might be nice to have the snapshot in the ctx and
    // release it on destruction
}

vbucket_state* MagmaKVStore::getVBucketState(Vbid vbid) {
    auto vbstate = cachedVBStates[vbid.get()].get();

    if (!vbstate) {
        auto status = readVBStateFromDisk(vbid);
        if (!status) {
            logger->warn("MagmaKVStore::getVBucketState failed - error:{}",
                         status.String());
            return nullptr;
        }

        vbstate = cachedVBStates[vbid.get()].get();
    }

    if (logger->should_log(spdlog::level::trace)) {
        auto j = encodeVBState(*vbstate, getMagmaInfo(vbid));
        logger->trace("getVBucketState {} vbstate:{}", vbid, j.dump());
    }
    return vbstate;
}

MagmaInfo& MagmaKVStore::getMagmaInfo(Vbid vbid) {
    auto magmaInfo = cachedMagmaInfo[vbid.get()].get();
    if (!magmaInfo) {
        throw std::logic_error("MagmaKVStore::getMagmaInfo empty vbid:" +
                               std::to_string(vbid.get()));
    }
    return *magmaInfo;
}

magma::Status MagmaKVStore::readVBStateFromDisk(Vbid vbid) {
    Status status = Status::OK();
    std::string valString;
    std::tie(status, valString) = readLocalDoc(vbid, vbstateKey);
    if (!status) {
        return status;
    }
    nlohmann::json j;

    try {
        j = nlohmann::json::parse(valString);
    } catch (const nlohmann::json::exception& e) {
        return Status("MagmaKVStore::readVBStateFromDisk failed - vbucket( " +
                      std::to_string(vbid.get()) + ") " +
                      " Failed to parse the vbstate json doc: " + valString +
                      ". Reason: " + e.what());
    }

    vbucket_state vbstate = j;
    logger->trace("readVBStateFromDisk {} vbstate:{}", vbid, j.dump());

    auto vbs = std::make_unique<vbucket_state>(vbstate);
    cachedVBStates[vbid.get()] = std::move(vbs);

    MagmaInfo magmaInfo;
    magmaInfo.docCount = std::stoull(j.at("doc_count").get<std::string>());
    magmaInfo.persistedDeletes =
            std::stoull(j.at("persisted_deletes").get<std::string>());

    auto minfo = std::make_unique<MagmaInfo>(magmaInfo);
    cachedMagmaInfo[vbid.get()] = std::move(minfo);
    return status;
}

magma::Status MagmaKVStore::writeVBStateToDisk(
        Vbid vbid, Magma::CommitBatch& commitBatch) {
    auto vbs = cachedVBStates[vbid.get()].get();
    if (!vbs) {
        throw std::logic_error(
                "MagmaKVStore::writeVBStateToDisk cachedVBStates empty");
    }
    auto minfo = cachedMagmaInfo[vbid.get()].get();
    if (!minfo) {
        throw std::logic_error(
                "MagmaKVStore::writeVBStateToDisk cachedMagmaInfo empty");
    }

    auto jstr = encodeVBState(*vbs, *minfo).dump();
    logger->trace("writeVBStateToDisk {} vbstate:{}", vbid, jstr);
    return setLocalDoc(commitBatch, vbstateKey, jstr, false);
}

std::string MagmaKVStore::encodeLocalDoc(Vbid vbid,
                                         const std::string& value,
                                         const bool isDelete) {
    uint64_t seqno;
    magma->GetMaxSeqno(vbid.get(), seqno);
    auto docMeta = magmakv::MetaData(
            isDelete, sizeof(magmakv::MetaData) + value.size(), seqno, vbid);
    std::string retString;
    retString.reserve(sizeof(magmakv::MetaData) + value.size());
    retString.append(reinterpret_cast<char*>(&docMeta),
                     sizeof(magmakv::MetaData));
    retString.append(value.data(), value.size());
    return retString;
}

std::pair<std::string, bool> MagmaKVStore::decodeLocalDoc(
        const Slice& valSlice) {
    std::string value = {valSlice.Data() + sizeof(magmakv::MetaData),
                         valSlice.Len() - sizeof(magmakv::MetaData)};
    return std::make_pair(value, isDeleted(valSlice));
}

std::pair<Status, std::string> MagmaKVStore::readLocalDoc(
        Vbid vbid, const Slice& keySlice) {
    Slice valSlice;
    Magma::FetchBuffer valBuf;
    bool found{false};
    magma::Status retStatus = Status::OK();
    std::string valString;

    auto status =
            magma->GetLocal(vbid.get(), keySlice, valBuf, valSlice, found);
    // If the kvstore hasn't been created yet, we get a NotExists error.
    // We don't know if its been created.
    if (!status && status.ErrorCode() != Status::Code::NotExists) {
        retStatus = magma::Status(
                status.ErrorCode(),
                "MagmaKVStore::readLocalDoc vbid:" + vbid.to_string() +
                        " key:" + keySlice.ToString() + " " + status.String());
    } else {
        if (!found) {
            retStatus = magma::Status(
                    magma::Status::Code::NotFound,
                    "MagmaKVStore::readLocalDoc(vbid:" + vbid.to_string() +
                            " key:" + keySlice.ToString() + " not found.");
        } else {
            bool deleted;
            std::tie(valString, deleted) = decodeLocalDoc(valSlice);
            // If deleted, return empty value
            if (deleted) {
                retStatus = magma::Status(
                        magma::Status::Code::NotFound,
                        "MagmaKVStore::readLocalDoc(vbid:" + vbid.to_string() +
                                " key:" + keySlice.ToString() + " deleted.");
                logger->warn("MagmaKVStore::readLocalDoc({} key:{}) deleted",
                             vbid,
                             keySlice.ToString());
            } else {
                logger->trace("MagmaKVStore::readLocalDoc({} key:{})",
                              vbid,
                              keySlice.ToString());
            }
        }
    }

    return std::make_pair(retStatus, valString);
}

Status MagmaKVStore::setLocalDoc(Magma::CommitBatch& commitBatch,
                                 const Slice& keySlice,
                                 std::string& valBuf,
                                 bool deleted) {
    auto valString =
            encodeLocalDoc(Vbid(commitBatch.GetkvID()), valBuf, deleted);

    // Store in localDB
    Slice valSlice = {valString.c_str(), valString.size()};

    logger->trace("MagmaKVStore::setLocalDoc({} key:{})",
                  commitBatch.GetkvID(),
                  const_cast<Slice&>(keySlice).ToString());

    return commitBatch.SetLocal(keySlice, valSlice);
}

nlohmann::json MagmaKVStore::encodeVBState(vbucket_state& vbstate,
                                           MagmaInfo& magmaInfo) const {
    nlohmann::json j = vbstate;
    j["doc_count"] = std::to_string(static_cast<uint64_t>(magmaInfo.docCount));
    j["persisted_deletes"] =
            std::to_string(static_cast<uint64_t>(magmaInfo.persistedDeletes));
    return j;
}

ENGINE_ERROR_CODE MagmaKVStore::magmaErr2EngineErr(Status::Code err,
                                                   bool found) {
    if (!found) {
        return ENGINE_KEY_ENOENT;
    }
    // This routine is intended to mimic couchErr2EngineErr.
    // Since magma doesn't have a memory allocation error, all magma errors
    // get translated into ENGINE_TMPFAIL.
    switch (err) {
    case Status::Code::Ok:
        return ENGINE_SUCCESS;
    default:
        return ENGINE_TMPFAIL;
    }
}
