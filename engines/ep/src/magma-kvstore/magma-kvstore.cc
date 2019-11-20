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

#include <mcbp/protocol/request.h>
#include <utilities/logtags.h>

#include <string.h>
#include <algorithm>
#include <limits>

class Snapshot;

// Unfortunately, turning on logging for the tests is limited to debug
// mode. While we are in the midst of dropping in magma, this provides
// a simple way to change all the logging->trace calls to logging->debug
// by changing trace to debug..
// Once magma has been completed, we can remove this #define and change
// all the logging calls back to trace.
#define TRACE trace

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
          datatype(it.getDataType()),
          prepareSeqno(it.getPrepareSeqno()) {
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
          durabilityLevel(0),
          prepareSeqno(0) {
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

    std::string to_string(Operation op) const {
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
        return std::string{"Unknown:" +
                           std::to_string(static_cast<uint8_t>(op))};
    }

    std::string to_string() const {
        std::stringstream ss;
        int vers = metaDataVersion;
        int dt = datatype;
        cb::durability::Requirements req(getDurabilityLevel(), {});
        ss << "bySeqno:" << bySeqno << " cas:" << cas << " exptime:" << exptime
           << " revSeqno:" << revSeqno << " flags:" << flags
           << " valueSize:" << valueSize << " vbid:" << vbid
           << " deleted:" << (deleted == 0 ? "false" : "true")
           << " deleteSource:"
           << (deleted == 0 ? " " : deleteSource == 0 ? "Explicit" : "TTL")
           << " version:" << vers << " datatype:" << dt
           << " operation:" << to_string(getOperation())
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
    cb::uint48_t prepareSeqno;

private:
    static Operation toOperation(queue_op op) {
        switch (op) {
        case queue_op::mutation:
        case queue_op::system_event:
            return Operation::Mutation;
        case queue_op::pending_sync_write:
            return Operation::PreparedSyncWrite;
        case queue_op::commit_sync_write:
            return Operation::CommittedSyncWrite;
        case queue_op::abort_sync_write:
            return Operation::Abort;
        case queue_op::flush:
        case queue_op::empty:
        case queue_op::checkpoint_start:
        case queue_op::checkpoint_end:
        case queue_op::set_vbucket_state:
            break;
        }
        throw std::invalid_argument(
                "magma::MetaData::toOperation: Unsupported op " +
                std::to_string(static_cast<uint8_t>(op)));
    }
};
#pragma pack()

static_assert(sizeof(MetaData) == 47,
              "magmakv::MetaData is not the expected size.");
} // namespace magmakv

// Keys to localdb docs
static const Slice vbstateKey = {"_vbstate"};
static const Slice manifestKey = {"_collections/manifest"};
static const Slice openCollectionsKey = {"_collections/open"};
static const Slice openScopesKey = {"_scopes/open"};
static const Slice droppedCollectionsKey = {"_collections/dropped"};

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
     * @param logger Used for logging
     */
    MagmaRequest(const Item& item,
                 MutationRequestCallback callback,
                 std::shared_ptr<BucketLogger> logger)
        : IORequest(item.getVBucketId(), std::move(callback), DiskDocKey{item}),
          docMeta(magmakv::MetaData(item)),
          docBody(item.getValue()) {
        if (logger->should_log(spdlog::level::TRACE)) {
            logger->TRACE("MagmaRequest:{}", to_string());
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
        std::stringstream ss;
        ss << "Key:" << key.to_string() << " docMeta:" << docMeta.to_string()
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

static const bool isCompressed(const Slice& metaSlice) {
    magmakv::MetaData* docMeta = reinterpret_cast<magmakv::MetaData*>(
            const_cast<char*>(metaSlice.Data()));
    return mcbp::datatype::is_snappy(docMeta->datatype);
}

static const Vbid getVbid(const Slice& metaSlice) {
    magmakv::MetaData* docMeta = reinterpret_cast<magmakv::MetaData*>(
            const_cast<char*>(metaSlice.Data()));
    return Vbid(docMeta->vbid);
}

static const magmakv::MetaData& getDocMeta(const Slice& metaSlice) {
    magmakv::MetaData* docMeta = reinterpret_cast<magmakv::MetaData*>(
            const_cast<char*>(metaSlice.Data()));
    return *docMeta;
}

static const bool isPrepared(const Slice& metaSlice) {
    magmakv::MetaData* docMeta = reinterpret_cast<magmakv::MetaData*>(
            const_cast<char*>(metaSlice.Data()));
    return static_cast<magmakv::MetaData::Operation>(docMeta->operation) ==
           magmakv::MetaData::Operation::PreparedSyncWrite;
}

static DiskDocKey makeDiskDocKey(const Slice& key) {
    return DiskDocKey{key.Data(), key.Len()};
}

MagmaKVStore::MagmaCompactionCB::MagmaCompactionCB(MagmaKVStore& magmaKVStore)
    : magmaKVStore(magmaKVStore) {
    magmaKVStore.logger->TRACE("MagmaCompactionCB constructor");
    initialized = false;
}

MagmaKVStore::MagmaCompactionCB::~MagmaCompactionCB() {
    magmaKVStore.logger->debug("MagmaCompactionCB destructor");
}

bool MagmaKVStore::compactionCallBack(MagmaKVStore::MagmaCompactionCB& cbCtx,
                                      const magma::Slice& keySlice,
                                      const magma::Slice& metaSlice,
                                      const magma::Slice& valueSlice) {
    std::stringstream itemString;
    if (logger->should_log(spdlog::level::TRACE)) {
        itemString << "key:"
                   << cb::UserData{makeDiskDocKey(keySlice).to_string()};
        itemString << " ";
        itemString << getDocMeta(metaSlice).to_string();
        logger->TRACE("MagmaCompactionCB {}", itemString.str());
    }

    if (!cbCtx.initialized) {
        cbCtx.vbid = getVbid(metaSlice);

        // If we have a compaction_ctx, that means we are doing an
        // explicit compaction triggered by the kv_engine.
        {
            std::lock_guard<std::mutex> lock(compactionCtxMutex);
            if (compaction_ctxList[cbCtx.vbid.get()]) {
                cbCtx.ctx = compaction_ctxList[cbCtx.vbid.get()]->ctx;
                cbCtx.kvHandle = compaction_ctxList[cbCtx.vbid.get()]->kvHandle;
            }
        }
        if (!cbCtx.ctx) {
            cbCtx.kvHandle = getMagmaKVHandle(cbCtx.vbid);
            // TODO: we don't have a callback function to kv_engine
            // to grab the compaction_ctx when the compaction is
            // triggered by magma (Level, Expiry).
        }
        cbCtx.initialized = true;
    }

    if (cbCtx.ctx) {
        auto seqno = getSeqNum(metaSlice);
        auto exptime = getExpiryTime(metaSlice);
        Status status;

        if (cbCtx.ctx->droppedKeyCb) {
            // We need to check both committed and prepared documents - if the
            // collection has been logically deleted then we need to discard
            // both types of keys.
            // As such use the docKey (i.e. without any DurabilityPrepare
            // namespace) when checking if logically deleted;
            auto diskKey = makeDiskDocKey(keySlice);
            if (cbCtx.ctx->eraserContext->isLogicallyDeleted(
                        diskKey.getDocKey(), seqno)) {
                // Inform vb that the key@seqno is dropped
                cbCtx.ctx->droppedKeyCb(diskKey, seqno);
                if (!isDeleted(metaSlice)) {
                    cbCtx.ctx->stats.collectionsItemsPurged++;
                } else {
                    if (isPrepared(metaSlice)) {
                        cbCtx.ctx->stats.preparesPurged++;
                    }
                    cbCtx.ctx->stats.collectionsDeletedItemsPurged++;
                }
                return true;
            }
        }

        if (isDeleted(metaSlice)) {
            uint64_t maxSeqno;
            status = magma->GetMaxSeqno(cbCtx.vbid.get(), maxSeqno);
            if (!status) {
                throw std::runtime_error("MagmaCompactionCallback Failed : " +
                                         status.String());
            }

            if (seqno != maxSeqno) {
                if (cbCtx.ctx->compactConfig.drop_deletes) {
                    logger->TRACE("MagmaCompactionCB DROP drop_deletes {}",
                                  itemString.str());
                    cbCtx.ctx->stats.tombstonesPurged++;
                    if (cbCtx.ctx->max_purged_seq < seqno) {
                        cbCtx.ctx->max_purged_seq = seqno;
                    }
                    return true;
                }

                if (exptime < cbCtx.ctx->compactConfig.purge_before_ts &&
                    (exptime ||
                     !cbCtx.ctx->compactConfig.retain_erroneous_tombstones) &&
                    (!cbCtx.ctx->compactConfig.purge_before_seq ||
                     seqno <= cbCtx.ctx->compactConfig.purge_before_seq)) {
                    logger->TRACE("MagmaCompactionCB DROP expired tombstone {}",
                                  itemString.str());
                    cbCtx.ctx->stats.tombstonesPurged++;
                    if (cbCtx.ctx->max_purged_seq < seqno) {
                        cbCtx.ctx->max_purged_seq = seqno;
                    }
                    return true;
                }
            }
        } else {
            // We can remove any prepares that have been completed. This works
            // because we send Mutations instead of Commits when streaming from
            // Disk so we do not need to send a Prepare message to keep things
            // consistent on a replica.
            if (isPrepared(metaSlice)) {
                if (cbCtx.ctx->max_purged_seq < seqno) {
                    cbCtx.ctx->stats.preparesPurged++;
                    return true;
                }
            }
            time_t currTime = ep_real_time();
            if (exptime && exptime < currTime) {
                auto docMeta = getDocMeta(metaSlice);
                auto itm = std::make_unique<Item>(
                        makeDiskDocKey(keySlice).getDocKey(),
                        docMeta.flags,
                        docMeta.exptime,
                        valueSlice.Data(),
                        valueSlice.Len(),
                        docMeta.datatype,
                        docMeta.cas,
                        docMeta.bySeqno,
                        cbCtx.vbid,
                        docMeta.revSeqno);
                if (isCompressed(metaSlice)) {
                    itm->decompressValue();
                }
                itm->setDeleted(DeleteSource::TTL);
                cbCtx.ctx->expiryCallback->callback(*(itm.get()), currTime);
                logger->TRACE("MagmaCompactionCB expiry callback {}",
                              itemString.str());
            }
        }
    }

    logger->TRACE("MagmaCompactionCB KEEP {}", itemString.str());
    return false;
}

MagmaKVStore::MagmaKVStore(MagmaKVStoreConfig& configuration)
    : KVStore(configuration),
      pendingReqs(std::make_unique<PendingRequestQueue>()),
      in_transaction(false),
      magmaPath(configuration.getDBName() + "/magma." +
                std::to_string(configuration.getShardId())),
      scanCounter(0),
      cachedMagmaInfo(configuration.getMaxVBuckets()),
      compaction_ctxList(configuration.getMaxVBuckets()) {
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
    configuration.magmaCfg.NumWALBuffers =
            configuration.getMagmaWalNumBuffers();
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

    commitPointEveryBatch = configuration.getMagmaCommitPointEveryBatch();
    useUpsertForSet = configuration.getMagmaEnableUpsert();
    if (useUpsertForSet) {
        configuration.magmaCfg.EnableUpdateStatusForSet = false;
    } else {
        configuration.magmaCfg.EnableUpdateStatusForSet = true;
    }

    // Set up thread and memory tracking.
    auto currEngine = ObjectRegistry::getCurrentEngine();
    configuration.magmaCfg.SetupThreadContext = [currEngine]() {
        ObjectRegistry::onSwitchThread(currEngine, false);
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

    // Populate the magmaKVHandles
    magmaKVHandles =
            std::vector<std::pair<MagmaKVHandle, std::shared_timed_mutex>>(
                    configuration.getMaxVBuckets());
    for (int i = 0; i < configuration.getMaxVBuckets(); i++) {
        magmaKVHandles[i].first = std::make_shared<MagmaKVHandleStruct>();
    }

    configuration.magmaCfg.MakeCompactionCallback = [&]() {
        return std::make_unique<MagmaKVStore::MagmaCompactionCB>(*this);
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
        std::string err =
                "MagmaKVStore Magma open failed. Status:" + status.String();
        logger->critical(err);
        throw std::logic_error(err);
    }
    auto kvstoreList = magma->GetKVStoreList();
    for (auto& kvid : kvstoreList) {
        // No need to get vbHandle or vbstateMutex as we are just
        // booting magma.
        status = readVBStateFromDisk(Vbid(kvid));
        if (!status) {
            throw std::logic_error("MagmaKVStore vbstate vbid:" +
                                   std::to_string(kvid) +
                                   " not found."
                                   " Status:" +
                                   status.String());
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

    auto kvHandle = getMagmaKVHandle(pendingReqs->front().getVbID());

    // Flush all documents to disk
    auto errCode = saveDocs(collectionsFlush, kvctx, kvHandle);
    if (errCode != ENGINE_SUCCESS) {
        logger->warn("MagmaKVStore::commit: saveDocs {} errCode:{}",
                     pendingReqs->front().getVbID(),
                     errCode);
        success = false;
    }

    commitCallback(errCode, kvctx);

    // This behaviour is to replicate the one in Couchstore.
    // Set `in_transanction = false` only if `commit` is successful.
    if (success) {
        in_transaction = false;
        transactionCtx.reset();
    }

    pendingReqs->clear();
    logger->TRACE("MagmaKVStore::commit success:{}", success);

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
        st.io_document_write_bytes += mutationSize;

        // Note:
        // There are 3 cases for setting rv based on errCode
        // MutationStatus::MutationSuccess
        // MutationStatus::DocNotFound
        // MutationStatus::MutationFailed - magma does not support this case
        // because
        //     we don't want to requeue the item.  If errCode is bad,
        //     its most likely something fatal with the kvstore
        //     or it might be the kvstore is pending being dropped.
        auto rv = MutationStatus::DocNotFound;
        if (req.oldItemExists() && !req.oldItemIsDelete()) {
            rv = MutationStatus::Success;
        }

        if (req.isDelete()) {
            if (req.requestFailed()) {
                st.numDelFailure++;
            } else {
                st.delTimeHisto.add(req.getDelta());
            }

            logger->TRACE(
                    "MagmaKVStore::commitCallback(Del) {} errCode:{} rv:{}",
                    pendingReqs->front().getVbID(),
                    errCode,
                    to_string(rv));

            req.getDelCallback()(*transactionCtx, rv);
        } else {
            auto mutationSetStatus = MutationSetResultState::Update;
            if (req.oldItemIsDelete()) {
                rv = MutationStatus::DocNotFound;
            }

            if (req.requestFailed()) {
                st.numSetFailure++;
                mutationSetStatus = MutationSetResultState::Failed;
            } else {
                st.writeTimeHisto.add(req.getDelta());
                st.writeSizeHisto.add(mutationSize);
            }

            if (rv == MutationStatus::DocNotFound) {
                mutationSetStatus = MutationSetResultState::Insert;
            }
            req.getSetCallback()(*transactionCtx, mutationSetStatus);

            logger->TRACE(
                    "MagmaKVStore::commitCallback(Set) {} errCode:{} rv:{} "
                    "insertion:{}",
                    pendingReqs->front().getVbID(),
                    errCode,
                    to_string(rv),
                    to_string(mutationSetStatus));
        }
    }
}

void MagmaKVStore::rollback() {
    if (in_transaction) {
        in_transaction = false;
        transactionCtx.reset();
    }
}

// Magma does allow compactions to run concurrent with writes.
// What it does not do is allow the update of vbstate info outside
// of the bg_writer thread.
StorageProperties MagmaKVStore::getStorageProperties() {
    StorageProperties rv(StorageProperties::EfficientVBDump::Yes,
                         StorageProperties::EfficientVBDeletion::Yes,
                         StorageProperties::PersistedDeletion::No,
                         StorageProperties::EfficientGet::Yes,
                         StorageProperties::ConcurrentWriteCompact::No);
    return rv;
}

// Note: This routine is only called during warmup. The caller
// can not make changes to the vbstate or it would cause race conditions.
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

GetValue MagmaKVStore::get(const DiskDocKey& key, Vbid vb) {
    return getWithHeader(nullptr, key, vb, GetMetaOnly::No);
}

GetValue MagmaKVStore::getWithHeader(void* dbHandle,
                                     const DiskDocKey& key,
                                     Vbid vbid,
                                     GetMetaOnly getMetaOnly) {
    Slice keySlice = {reinterpret_cast<const char*>(key.data()), key.size()};
    Slice metaSlice;
    Slice valueSlice;
    Magma::FetchBuffer idxBuf;
    Magma::FetchBuffer seqBuf;
    bool found;

    auto kvHandle = getMagmaKVHandle(vbid);

    auto start = std::chrono::steady_clock::now();

    Status status = magma->Get(
            vbid.get(), keySlice, idxBuf, seqBuf, metaSlice, valueSlice, found);

    if (logger->should_log(spdlog::level::TRACE)) {
        logger->TRACE(
                "MagmaKVStore::getWithHeader {} key:{} status:{} found:{} "
                "deleted:{}",
                vbid,
                cb::UserData{key.to_string()},
                status.String(),
                found,
                found ? isDeleted(metaSlice) : false);
    }

    if (!status) {
        logger->warn("MagmaKVStore::getWithHeader {} key:{} status:{}",
                     vbid,
                     cb::UserData{makeDiskDocKey(keySlice).to_string()},
                     status.String());
        return GetValue{NULL, magmaErr2EngineErr(status.ErrorCode())};
    }

    if (!found) {
        return GetValue{NULL, ENGINE_KEY_ENOENT};
    }

    // record stats
    st.readTimeHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start));
    st.readSizeHisto.add(keySlice.Len() + metaSlice.Len() + valueSlice.Len());

    return makeGetValue(vbid, keySlice, metaSlice, valueSlice, getMetaOnly);
}

void MagmaKVStore::getMulti(Vbid vbid, vb_bgfetch_queue_t& itms) {
    auto kvHandle = getMagmaKVHandle(vbid);

    for (auto& it : itms) {
        auto& key = it.first;
        Slice keySlice = {reinterpret_cast<const char*>(key.data()),
                          key.size()};
        Slice metaSlice;
        Slice valueSlice;
        Magma::FetchBuffer idxBuf;
        Magma::FetchBuffer seqBuf;
        bool found;

        Status status = magma->Get(vbid.get(),
                                   keySlice,
                                   idxBuf,
                                   seqBuf,
                                   metaSlice,
                                   valueSlice,
                                   found);

        if (logger->should_log(spdlog::level::TRACE)) {
            logger->TRACE(
                    "MagmaKVStore::getMulti {} key:{} status:{} found:{} "
                    "deleted:{}",
                    vbid,
                    cb::UserData{makeDiskDocKey(keySlice).to_string()},
                    status.String(),
                    found,
                    found ? isDeleted(metaSlice) : false);
        }

        auto errCode = magmaErr2EngineErr(status.ErrorCode(), found);
        vb_bgfetch_item_ctx_t& bg_itm_ctx = it.second;
        bg_itm_ctx.value.setStatus(errCode);

        if (found) {
            it.second.value = makeGetValue(vbid,
                                           keySlice,
                                           metaSlice,
                                           valueSlice,
                                           it.second.isMetaOnly);
            GetValue* rv = &it.second.value;

            for (auto& fetch : bg_itm_ctx.bgfetched_list) {
                fetch->value = rv;
                st.readTimeHisto.add(
                        std::chrono::duration_cast<std::chrono::microseconds>(
                                std::chrono::steady_clock::now() -
                                fetch->initTime));
                st.readSizeHisto.add(bg_itm_ctx.value.item->getKey().size() +
                                     bg_itm_ctx.value.item->getNBytes());
            }
        } else {
            if (!status) {
                logger->critical(
                        "MagmaKVStore::getMulti: {} key:{} status:{}, ",
                        vbid,
                        cb::UserData{makeDiskDocKey(keySlice).to_string()},
                        status.String());
                st.numGetFailure++;
            }
            for (auto& fetch : bg_itm_ctx.bgfetched_list) {
                fetch->value->setStatus(errCode);
            }
        }
    }
}

void MagmaKVStore::getRange(Vbid vbid,
                            const DiskDocKey& startKey,
                            const DiskDocKey& endKey,
                            const GetRangeCb& cb) {
    Slice startKeySlice = {reinterpret_cast<const char*>(startKey.data()),
                           startKey.size()};
    Slice endKeySlice = {reinterpret_cast<const char*>(endKey.data()),
                         endKey.size()};

    auto callback = [&](Slice& keySlice, Slice& metaSlice, Slice& valueSlice) {
        if (logger->should_log(spdlog::level::TRACE)) {
            logger->TRACE(
                    "MagmaKVStore::getRange callback {} key:{} seqno:{} "
                    "deleted:{}",
                    vbid,
                    cb::UserData{makeDiskDocKey(keySlice).to_string()},
                    getSeqNum(metaSlice),
                    isDeleted(metaSlice));
        }

        if (isDeleted(metaSlice)) {
            return;
        }
        auto rv = makeGetValue(vbid, keySlice, metaSlice, valueSlice);
        cb(std::move(rv));
    };

    if (logger->should_log(spdlog::level::TRACE)) {
        logger->TRACE("MagmaKVStore::getRange {} start:{} end:{}",
                      vbid,
                      cb::UserData{makeDiskDocKey(startKeySlice).to_string()},
                      cb::UserData{makeDiskDocKey(endKeySlice).to_string()});
    }

    auto kvHandle = getMagmaKVHandle(vbid);

    auto status = magma->GetRange(
            vbid.get(), startKeySlice, endKeySlice, callback, true);
    if (!status) {
        logger->critical(
                "MagmaKVStore::getRange {} start:{} end:{} status:{}",
                vbid,
                cb::UserData{makeDiskDocKey(startKeySlice).to_string()},
                cb::UserData{makeDiskDocKey(endKeySlice).to_string()},
                status.String());
    }
}

void MagmaKVStore::reset(Vbid vbid) {
    auto vbstate = getVBucketState(vbid);
    if (!vbstate) {
        throw std::invalid_argument(
                "MagmaKVStore::reset: No entry in cached "
                "states for " +
                vbid.to_string());
    }

    {
        // Get exclusive access to the handle
        auto lock = getExclusiveKVHandle(vbid);
        Status status;
        Magma::KVStoreRevision kvsRev;
        std::tie(status, kvsRev) = magma->SoftDeleteKVStore(vbid.get());
        if (!status) {
            logger->critical(
                    "MagmaKVStore::reset SoftDeleteKVStore failed. {} "
                    "status:{}",
                    vbid.to_string(),
                    status.String());
        }
        // Even though SoftDeleteKVStore might have failed, go ahead and
        // call DeleteKVStore because it will remove the KVStore path
        // regardless if we think the kvstore is valid or not.
        status = magma->DeleteKVStore(vbid.get(), kvsRev);
        if (!status) {
            logger->critical(
                    "MagmaKVStore::reset DeleteKVStore {} failed. status:{}",
                    vbid,
                    status.String());
        }

        // Reset the vbstate and magmaInfo
        auto kvHandle = magmaKVHandles[vbid.get()].first;
        {
            std::lock_guard<std::shared_timed_mutex> lock(
                    kvHandle->vbstateMutex);
            vbstate = cachedVBStates[vbid.get()].get();
            vbstate->reset();
            cachedMagmaInfo[vbid.get()]->reset();
        }
    }

    // We've released all our locks so snapshot the reset vbstate
    snapshotVBucket(
            vbid, *vbstate, VBStatePersist::VBSTATE_PERSIST_WITH_COMMIT);
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
    // Get exclusive access to the handle
    auto lock = getExclusiveKVHandle(vbid);
    auto status = magma->DeleteKVStore(
            vbid.get(), static_cast<Magma::KVStoreRevision>(vb_version));
    if (!status) {
        logger->warn(
                "MagmaKVStore::delVBucket DeleteKVStore {} revision:{} failed. "
                "status:{}",
                vbid,
                vb_version,
                status.String());
    }
}

void MagmaKVStore::prepareToCreateImpl(Vbid vbid) {
    auto kvHandle = getMagmaKVHandle(vbid);
    if (magma->KVStoreExists(vbid.get())) {
        throw std::logic_error("MagmaKVStore::prepareToCreateImpl " +
                               vbid.to_string() +
                               " Can't call prepareToCreate before calling" +
                               " prepareToDelete on an existing kvstore.");
    }
    if (cachedVBStates[vbid.get()]) {
        std::lock_guard<std::shared_timed_mutex> lock(kvHandle->vbstateMutex);
        cachedVBStates[vbid.get()]->reset();
        cachedMagmaInfo[vbid.get()]->reset();
        cachedMagmaInfo[vbid.get()]->kvstoreRev++;
        logger->info("MagmaKVStore::prepareToCreateImpl {} kvstoreRev:{}",
                     vbid,
                     cachedMagmaInfo[vbid.get()]->kvstoreRev);
    }
}

uint64_t MagmaKVStore::prepareToDeleteImpl(Vbid vbid) {
    auto kvHandle = getMagmaKVHandle(vbid);
    if (magma->KVStoreExists(vbid.get())) {
        auto kvHandle = getMagmaKVHandle(vbid);
        Status status;
        Magma::KVStoreRevision kvsRev;
        std::tie(status, kvsRev) = magma->SoftDeleteKVStore(vbid.get());
        if (!status) {
            logger->critical(
                    "MagmaKVStore::prepareToCreateImpl SoftDeleteKVStore "
                    "failed. {} status:{}",
                    vbid,
                    status.String());
        }
        logger->info("MagmaKVStore::prepareToDeleteImpl {} kvstoreRev:{}",
                     vbid,
                     cachedMagmaInfo[vbid.get()]->kvstoreRev);
        return static_cast<uint64_t>(cachedMagmaInfo[vbid.get()]->kvstoreRev);
    }
    return 0;
}

// Note: It is assumed this can only be called from bg flusher thread or
// there will be issues with writes coming from multiple threads.
bool MagmaKVStore::snapshotVBucket(Vbid vbid,
                                   const vbucket_state& vbstate,
                                   VBStatePersist options) {
    auto kvHandle = getMagmaKVHandle(vbid);
    std::unique_lock<std::shared_timed_mutex> vbstateLock(
            kvHandle->vbstateMutex);

    // It possible that snapshot is called either on a vbucket that
    // doesn't exist yet or on a vbucket that was newly created and
    // magmaInfo isn't created yet. Create it here if it doesn't exist.
    if (!cachedMagmaInfo[vbid.get()]) {
        auto minfo = std::make_unique<MagmaInfo>();
        cachedMagmaInfo[vbid.get()] = std::move(minfo);
    }

    if (logger->should_log(spdlog::level::TRACE)) {
        std::string opt;
        switch (options) {
        case VBStatePersist::VBSTATE_CACHE_UPDATE_ONLY:
            opt = "UPDATE_ONLY";
            break;
        case VBStatePersist::VBSTATE_PERSIST_WITHOUT_COMMIT:
            opt = "WITHOUT_COMMIT";
            break;
        case VBStatePersist::VBSTATE_PERSIST_WITH_COMMIT:
            opt = "WITH_COMMIT";
            break;
        }
        if (opt.empty()) {
            throw std::logic_error(
                    "MagmaKVStore::snapshotVBucket Unknown VBStatePersist "
                    "option:" +
                    std::to_string(static_cast<int>(options)));
        }
        auto minfo = cachedMagmaInfo[vbid.get()].get();
        auto j = encodeVBState(vbstate, *minfo);
        logger->TRACE("MagmaKVStore::snapshotVBucket {} persist:{} vbstate:{}",
                      vbid,
                      opt,
                      j.dump());
    }

    auto start = std::chrono::steady_clock::now();

    if (updateCachedVBState(vbid, vbstate) &&
        // snapshotVBucket for magma doesn't really support a
        // VBSTATE_PERSIST_WITHOUT_COMMIT option. Since this is going to write
        // the vbstate to the local DB and its not part of an ongoing
        // commitBatch, all we can do is create a commitBatch for this
        // snapshotVBucket call and commit it to disk. Fortunately, it appears
        // VBSTATE_PERSIST_WITHOUT_COMMIT is not used except in testing.
        (options == VBStatePersist::VBSTATE_PERSIST_WITHOUT_COMMIT ||
         options == VBStatePersist::VBSTATE_PERSIST_WITH_COMMIT)) {
        // At this point, we've updated cachedVBStates but we need to
        // release the vbstateMutex lock or it causes a lock inversion
        // error between transaction locks held in magma and the vbstateMutex.
        // The normal mode of operation is to call commit which starts a
        // magma transaction that acquires magma transaction locks. As part
        // of the transaction, we need to update cachedVBState so we need to
        // get the vbstateMutex.
        // snapshotVBState can be called directly from a test like
        // kvstore_test.cc in initialize_kv_store(). Here we are getting
        // the locks in reverse order where we update the cachedVBStates,
        // which requires the lock and then we want to start a magma transaction
        // to write the new vbstate out.
        // Make a copy of the vbstate and MagmaInfo and release the locks.
        auto vbs = *cachedVBStates[vbid.get()].get();
        auto minfo = *cachedMagmaInfo[vbid.get()].get();

        vbstateLock.unlock();

        std::unique_ptr<Magma::CommitBatch> batch;
        auto status = magma->NewCommitBatch(
                vbid.get(),
                batch,
                static_cast<Magma::KVStoreRevision>(
                        cachedMagmaInfo[vbid.get()]->kvstoreRev));
        if (!status) {
            logger->critical(
                    "MagmaKVStore::snapshotVBucket failed creating "
                    "commitBatch for "
                    "{} status:{}",
                    vbid,
                    status.String());
            return false;
        }

        writeVBStateToDisk(vbid, *(batch.get()), vbs, minfo);

        status = magma->ExecuteCommitBatch(std::move(batch));
        if (!status) {
            logger->critical(
                    "MagmaKVStore::snapshotVBucket: "
                    "magma::ExecuteCommitBatch "
                    "{} status:{}",
                    vbid,
                    status.String());
            return false;
        }
        status = magma->SyncCommitBatches(commitPointEveryBatch);
        if (!status) {
            logger->critical(
                    "MagmaKVStore::snapshotVBucket: "
                    "magma::SyncCommitBatches {} "
                    "status:{}",
                    vbid,
                    status.String());
            return false;
        }
    }

    st.snapshotHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start));

    return true;
}

std::unique_ptr<Item> MagmaKVStore::makeItem(Vbid vb,
                                             const Slice& keySlice,
                                             const Slice& metaSlice,
                                             const Slice& valueSlice,
                                             GetMetaOnly getMetaOnly) {
    auto key = makeDiskDocKey(keySlice);
    auto& meta = *reinterpret_cast<const magmakv::MetaData*>(metaSlice.Data());

    bool includeValue = getMetaOnly == GetMetaOnly::No && meta.valueSize;

    auto item =
            std::make_unique<Item>(key.getDocKey(),
                                   meta.flags,
                                   meta.exptime,
                                   includeValue ? valueSlice.Data() : nullptr,
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
    case magmakv::MetaData::Operation::Mutation:
        // Item already defaults to Mutation - nothing else to do.
        return item;
    case magmakv::MetaData::Operation::PreparedSyncWrite:
        // From disk we return a zero (infinite) timeout; as this could
        // refer to an already-committed SyncWrite and hence timeout
        // must be ignored.
        item->setPendingSyncWrite({meta.getDurabilityLevel(),
                                   cb::durability::Timeout::Infinity()});
        return item;
    case magmakv::MetaData::Operation::CommittedSyncWrite:
        item->setCommittedviaPrepareSyncWrite();
        item->setPrepareSeqno(meta.prepareSeqno);
        return item;
    case magmakv::MetaData::Operation::Abort:
        item->setAbortSyncWrite();
        item->setPrepareSeqno(meta.prepareSeqno);
        return item;
    }

    throw std::logic_error("MagmaKVStore::makeItem unexpected operation:" +
                           meta.to_string(meta.getOperation()));
}

GetValue MagmaKVStore::makeGetValue(Vbid vb,
                                    const Slice& keySlice,
                                    const Slice& metaSlice,
                                    const Slice& valueSlice,
                                    GetMetaOnly getMetaOnly) {
    return GetValue(makeItem(vb, keySlice, metaSlice, valueSlice, getMetaOnly),
                    ENGINE_SUCCESS,
                    -1,
                    0);
}

int MagmaKVStore::saveDocs(Collections::VB::Flush& collectionsFlush,
                           kvstats_ctx& kvctx,
                           const MagmaKVHandle& kvHandle) {
    uint64_t ninserts = 0;
    uint64_t ndeletes = 0;
    int64_t lastSeqno = 0;

    auto vbid = pendingReqs->front().getVbID();

    // Start a magma CommitBatch.
    // This is an atomic batch of items... all or nothing.
    std::unique_ptr<Magma::CommitBatch> batch;
    Status status = magma->NewCommitBatch(
            vbid.get(),
            batch,
            static_cast<Magma::KVStoreRevision>(
                    cachedMagmaInfo[vbid.get()]->kvstoreRev));
    if (!status) {
        logger->warn(
                "MagmaKVStore::saveDocs NewCommitBatch failed {} status:{}",
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
            logger->warn("MagmaKVStore::saveDocs: Set {} status:{}",
                         vbid,
                         status.String());
            req.markRequestFailed();
            continue;
        }

        if (logger->should_log(spdlog::level::TRACE)) {
            logger->TRACE(
                    "MagmaKVStore::saveDocs {} key:{} seqno:{} delete:{} "
                    "found:{} "
                    "tombstone:{}",
                    vbid,
                    cb::UserData{makeDiskDocKey(key).to_string()},
                    req.getDocMeta().bySeqno,
                    req.isDelete(),
                    found,
                    tombstone);
        }

        auto diskDocKey = makeDiskDocKey(key);
        auto docKey = diskDocKey.getDocKey();

        if (found) {
            req.markOldItemExists();
            if (tombstone) {
                req.markOldItemIsDelete();
                // Old item is a delete and new is an insert.
                if (!req.isDelete()) {
                    ninserts++;
                    if (diskDocKey.isCommitted()) {
                        collectionsFlush.incrementDiskCount(docKey);
                    } else {
                        kvctx.onDiskPrepareDelta++;
                    }
                }
            } else if (req.isDelete()) {
                // Old item is insert and new is delete.
                ndeletes++;
                if (diskDocKey.isCommitted()) {
                    collectionsFlush.decrementDiskCount(docKey);
                } else {
                    kvctx.onDiskPrepareDelta--;
                }
            }
        } else {
            // Old item doesn't exist and new is an insert.
            if (!req.isDelete()) {
                ninserts++;
                if (diskDocKey.isCommitted()) {
                    collectionsFlush.incrementDiskCount(docKey);
                } else {
                    kvctx.onDiskPrepareDelta++;
                }
            }
        }

        kvctx.keyStats[diskDocKey] = false;

        if (req.oldItemExists()) {
            if (!req.oldItemIsDelete()) {
                // If we are replacing the item...
                auto itr = kvctx.keyStats.find(diskDocKey);
                if (itr != kvctx.keyStats.end()) {
                    itr->second = true;
                }
            }
        }

        collectionsFlush.setPersistedHighSeqno(
                docKey, req.getDocMeta().bySeqno, int(req.isDelete()));
    }

    collectionsFlush.saveCollectionStats(
            std::bind(&MagmaKVStore::saveCollectionStats,
                      this,
                      std::ref(*(batch.get())),
                      std::placeholders::_1,
                      std::placeholders::_2));

    {
        std::lock_guard<std::shared_timed_mutex> lock(kvHandle->vbstateMutex);
        auto vbstate = cachedVBStates[vbid.get()].get();
        if (vbstate) {
            vbstate->highSeqno = lastSeqno;
            vbstate->onDiskPrepares += kvctx.onDiskPrepareDelta;

            auto magmaInfo = cachedMagmaInfo[vbid.get()].get();
            magmaInfo->docCount += ninserts - ndeletes;
            magmaInfo->persistedDeletes += ndeletes;

            // Write out current vbstate to the CommitBatch.
            status = writeVBStateToDisk(
                    vbid, *(batch.get()), *vbstate, *magmaInfo);
            if (!status) {
                return status.ErrorCode();
            }
        }
    }

    if (collectionsMeta.needsCommit) {
        status = updateCollectionsMeta(vbid, *(batch.get()), collectionsFlush);
        if (!status) {
            logger->warn(
                    "MagmaKVStore::saveDocs: updateCollectionsMeta status:{}",
                    status.String());
        }
    }

    // Done with the CommitBatch so add in the endTxn and create
    // a snapshot so that documents can not be read.
    status = magma->ExecuteCommitBatch(std::move(batch));
    if (!status) {
        logger->warn("MagmaKVStore::saveDocs: ExecuteCommitBatch {} status:{}",
                     vbid,
                     status.String());
    } else {
        // Flush the WAL to disk.
        // If the shard writeCache is full, this will trigger all the
        // KVStores to persist their memtables.
        status = magma->SyncCommitBatches(commitPointEveryBatch);
        if (!status) {
            logger->warn(
                    "MagmaKVStore::saveDocs: SyncCommitBatches {} status:{}",
                    vbid,
                    status.String());
        }
    }

    st.commitHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - begin));

    return status.ErrorCode();
}

class MagmaScanContext : public ScanContext {
public:
    MagmaScanContext(std::shared_ptr<StatusCallback<GetValue>> cb,
                     std::shared_ptr<StatusCallback<CacheLookup>> cl,
                     Vbid vbid,
                     size_t id,
                     int64_t start,
                     int64_t end,
                     uint64_t purgeSeqno,
                     DocumentFilter _docFilter,
                     ValueFilter _valFilter,
                     uint64_t _documentCount,
                     const vbucket_state& vbucketState,
                     const KVStoreConfig& _config,
                     const std::vector<Collections::KVStore::DroppedCollection>&
                             droppedCollections,
                     MagmaKVStore::MagmaKVHandle kvHandle)
        : ScanContext(cb,
                      cl,
                      vbid,
                      id,
                      start,
                      end,
                      purgeSeqno,
                      _docFilter,
                      _valFilter,
                      _documentCount,
                      vbucketState,
                      _config,
                      droppedCollections),
          kvHandle(kvHandle) {
    }

private:
    MagmaKVStore::MagmaKVHandle kvHandle;
};

ScanContext* MagmaKVStore::initScanContext(
        std::shared_ptr<StatusCallback<GetValue>> cb,
        std::shared_ptr<StatusCallback<CacheLookup>> cl,
        Vbid vbid,
        uint64_t startSeqno,
        DocumentFilter options,
        ValueFilter valOptions) {
    size_t scanId = scanCounter++;

    auto kvHandle = getMagmaKVHandle(vbid);

    vbucket_state vbstate;

    {
        std::shared_lock<std::shared_timed_mutex> lock(kvHandle->vbstateMutex);
        const auto* vbstatePtr = getVBucketState(vbid);

        if (!vbstatePtr) {
            logger->warn("MagmaKVStore::initScanContext {} vbstate is null",
                         vbid);
            return nullptr;
        }
        // copy the vbstate for use outside the lock to init the scan context
        vbstate = *vbstatePtr;
    }

    uint64_t highSeqno = vbstate.highSeqno;
    uint64_t purgeSeqno = vbstate.purgeSeqno;
    uint64_t docCount = highSeqno - startSeqno + 1;

    auto collectionsManifest = getDroppedCollections(vbid);

    if (logger->should_log(spdlog::level::info)) {
        std::string docFilter;
        switch (options) {
        case DocumentFilter::ALL_ITEMS:
            docFilter = "ALL_ITEMS";
            break;
        case DocumentFilter::NO_DELETES:
            docFilter = "NO_DELETES";
            break;
        case DocumentFilter::ALL_ITEMS_AND_DROPPED_COLLECTIONS:
            docFilter = "ALL_ITEMS_AND_DROPPED_COLLECTIONS";
            break;
        }
        if (docFilter.size() == 0) {
            throw std::logic_error(
                    "MagmaKVStore::initScanContext Unknown DocumentFilter:" +
                    std::to_string(static_cast<int>(options)));
        }

        std::string valFilter;
        switch (valOptions) {
        case ValueFilter::KEYS_ONLY:
            valFilter = "KEYS_ONLY";
            break;
        case ValueFilter::VALUES_COMPRESSED:
            valFilter = "VALUES_COMPRESSED";
            break;
        case ValueFilter::VALUES_DECOMPRESSED:
            valFilter = "VALUES_DECOMPRESSED";
            break;
        }
        if (valFilter.size() == 0) {
            throw std::logic_error(
                    "MagmaKVStore::initScanContext Unknown ValueFilter:" +
                    std::to_string(static_cast<int>(valOptions)));
        }

        logger->info(
                "initScanContext {} seqno:{} endSeqno:{}"
                " purgeSeqno:{} docCount:{} docFilter:{} valFilter:{}",
                vbid,
                startSeqno,
                highSeqno,
                purgeSeqno,
                docCount,
                docFilter,
                valFilter);
    }

    auto mctx = new MagmaScanContext(cb,
                                     cl,
                                     vbid,
                                     scanId,
                                     startSeqno,
                                     highSeqno,
                                     purgeSeqno,
                                     options,
                                     valOptions,
                                     docCount,
                                     vbstate,
                                     configuration,
                                     collectionsManifest,
                                     kvHandle);

    mctx->logger = logger.get();
    return mctx;
}

scan_error_t MagmaKVStore::scan(ScanContext* inCtx) {
    if (!inCtx) {
        return scan_failed;
    }

    auto ctx = static_cast<MagmaScanContext*>(inCtx);

    if (ctx->lastReadSeqno == ctx->maxSeqno) {
        logger->TRACE("MagmaKVStore::scan {} lastReadSeqno:{} == maxSeqno:{}",
                      ctx->vbid,
                      ctx->lastReadSeqno,
                      ctx->maxSeqno);
        return scan_success;
    }

    auto startSeqno = ctx->startSeqno;
    if (ctx->lastReadSeqno != 0) {
        startSeqno = ctx->lastReadSeqno + 1;
    }

    GetMetaOnly isMetaOnly = ctx->valFilter == ValueFilter::KEYS_ONLY
                                     ? GetMetaOnly::Yes
                                     : GetMetaOnly::No;
    bool onlyKeys = ctx->valFilter == ValueFilter::KEYS_ONLY;

    auto itr = magma->NewSeqIterator(ctx->vbid.get());
    if (!itr) {
        logger->warn("MagmaKVStore::scan failed {}", ctx->vbid.get());
        return scan_failed;
    }

    for (itr->Seek(startSeqno, ctx->maxSeqno); itr->Valid(); itr->Next()) {
        Slice keySlice, metaSlice, valSlice;
        uint64_t seqno;
        itr->GetRecord(keySlice, metaSlice, valSlice, seqno);

        if (keySlice.Len() > std::numeric_limits<uint16_t>::max()) {
            throw std::invalid_argument(
                    "MagmaKVStore::scan: "
                    "key length " +
                    std::to_string(keySlice.Len()) + " > " +
                    std::to_string(std::numeric_limits<uint16_t>::max()));
        }

        auto diskKey = makeDiskDocKey(keySlice);

        if (isDeleted(metaSlice) &&
            ctx->docFilter == DocumentFilter::NO_DELETES) {
            logger->TRACE(
                    "MagmaKVStore::scan SKIPPED(Deleted) {} key:{} seqno:{}",
                    ctx->vbid,
                    cb::UserData{diskKey.to_string()},
                    seqno);
            continue;
        }

        auto docKey = diskKey.getDocKey();

        // Determine if the key is logically deleted, if it is we skip the key
        // Note that system event keys (like create scope) are never skipped
        // here
        if (!docKey.getCollectionID().isSystem()) {
            if (ctx->docFilter !=
                DocumentFilter::ALL_ITEMS_AND_DROPPED_COLLECTIONS) {
                if (ctx->collectionsContext.isLogicallyDeleted(docKey, seqno)) {
                    ctx->lastReadSeqno = seqno;
                    logger->TRACE(
                            "MagmaKVStore::scan SKIPPED(Collection Deleted) {} "
                            "key:{} "
                            "seqno:{}",
                            ctx->vbid,
                            cb::UserData{diskKey.to_string()},
                            seqno);
                    continue;
                }
            }

            CacheLookup lookup(diskKey, seqno, ctx->vbid);

            ctx->lookup->callback(lookup);
            if (ctx->lookup->getStatus() == ENGINE_KEY_EEXISTS) {
                ctx->lastReadSeqno = seqno;
                logger->TRACE(
                        "MagmaKVStore::scan SKIPPED(ENGINE_KEY_EEXISTS) {} "
                        "key:{} seqno:{}",
                        ctx->vbid,
                        cb::UserData{diskKey.to_string()},
                        seqno);
                continue;
            } else if (ctx->lookup->getStatus() == ENGINE_ENOMEM) {
                logger->warn(
                        "MagmaKVStore::scan lookup->callback {} "
                        "key:{} returned ENGINE_ENOMEM",
                        ctx->vbid,
                        cb::UserData{diskKey.to_string()});
                return scan_again;
            }
        }

        logger->TRACE(
                "MagmaKVStore::scan {} key:{} seqno:{} deleted:{} expiry:{} "
                "compressed:{}",
                ctx->vbid,
                diskKey.to_string(),
                seqno,
                isDeleted(metaSlice),
                getExpiryTime(metaSlice),
                isCompressed(metaSlice));

        auto itm =
                makeItem(ctx->vbid, keySlice, metaSlice, valSlice, isMetaOnly);

        // When we are suppose to return the values as compressed AND
        // the value isn't compressed, we need to compress the value.
        if (ctx->valFilter == ValueFilter::VALUES_COMPRESSED &&
            !isCompressed(metaSlice)) {
            if (!itm->compressValue(true)) {
                logger->warn(
                        "MagmaKVStore::scan failed to compress value - {} "
                        "key:{} "
                        "seqno:{}",
                        ctx->vbid,
                        cb::UserData{diskKey.to_string()},
                        seqno);
                continue;
            }
        }

        GetValue rv(std::move(itm), ENGINE_SUCCESS, -1, onlyKeys);
        ctx->callback->callback(rv);
        auto callbackStatus = ctx->callback->getStatus();
        if (callbackStatus == ENGINE_ENOMEM) {
            logger->warn(
                    "MagmaKVStore::scan callback {} "
                    "key:{} returned ENGINE_ENOMEM",
                    ctx->vbid,
                    cb::UserData{diskKey.to_string()});
            return scan_again;
        }
        ctx->lastReadSeqno = seqno;
    }

    return scan_success;
}

void MagmaKVStore::destroyScanContext(ScanContext* inCtx) {
    if (inCtx) {
        auto ctx = static_cast<MagmaScanContext*>(inCtx);
        delete ctx;
    }
}

vbucket_state* MagmaKVStore::getVBucketState(Vbid vbid) {
    auto kvHandle = getMagmaKVHandle(vbid);

    // Have to assume the caller is protecting the vbstate
    auto vbstate = cachedVBStates[vbid.get()].get();
    if (vbstate && logger->should_log(spdlog::level::TRACE)) {
        auto j = encodeVBState(*vbstate, *cachedMagmaInfo[vbid.get()].get());
        logger->TRACE(
                "MagmaKVStore::getVBucketState {} vbstate:{}", vbid, j.dump());
    }
    return vbstate;
}

// Note: Assume the kvHandle and vbstateMutex are held
magma::Status MagmaKVStore::readVBStateFromDisk(Vbid vbid) {
    Status status = Status::OK();
    std::string valString;
    std::tie(status, valString) = readLocalDoc(vbid, vbstateKey);

    if (!status) {
        cachedMagmaInfo[vbid.get()].reset(new MagmaInfo());
        cachedVBStates[vbid.get()].reset(new vbucket_state());
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
    logger->TRACE(
            "MagmaKVStore::readVBStateFromDisk {} vbstate:{}", vbid, j.dump());

    auto vbs = std::make_unique<vbucket_state>(vbstate);
    cachedVBStates[vbid.get()] = std::move(vbs);

    auto minfo = std::make_unique<MagmaInfo>();
    minfo->docCount = std::stoull(j.at("doc_count").get<std::string>());
    minfo->persistedDeletes =
            std::stoull(j.at("persisted_deletes").get<std::string>());
    minfo->kvstoreRev.reset(
            std::stoull(j.at("kvstore_revision").get<std::string>()));

    cachedMagmaInfo[vbid.get()] = std::move(minfo);
    return status;
}

magma::Status MagmaKVStore::writeVBStateToDisk(Vbid vbid,
                                               Magma::CommitBatch& commitBatch,
                                               vbucket_state& vbs,
                                               MagmaInfo& minfo) {
    auto jstr = encodeVBState(vbs, minfo).dump();
    logger->TRACE("MagmaKVStore::writeVBStateToDisk {} vbstate:{}", vbid, jstr);
    return setLocalDoc(commitBatch, vbstateKey, jstr);
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
                if (logger->should_log(spdlog::level::TRACE)) {
                    logger->warn("MagmaKVStore::readLocalDoc {} key:{} deleted",
                                 vbid,
                                 keySlice.ToString());
                }
            } else {
                if (logger->should_log(spdlog::level::TRACE)) {
                    logger->TRACE(
                            "MagmaKVStore::readLocalDoc {} key:{} valueLen:{}",
                            vbid,
                            keySlice.ToString(),
                            valString.length());
                }
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

    if (logger->should_log(spdlog::level::TRACE)) {
        logger->TRACE("MagmaKVStore::setLocalDoc {} key:{} valueLen:{}",
                      commitBatch.GetkvID(),
                      keySlice.ToString(),
                      valBuf.length());
    }

    return commitBatch.SetLocal(keySlice, valSlice);
}

nlohmann::json MagmaKVStore::encodeVBState(const vbucket_state& vbstate,
                                           MagmaInfo& magmaInfo) const {
    nlohmann::json j = vbstate;
    j["doc_count"] = std::to_string(static_cast<uint64_t>(magmaInfo.docCount));
    j["persisted_deletes"] =
            std::to_string(static_cast<uint64_t>(magmaInfo.persistedDeletes));
    j["kvstore_revision"] =
            std::to_string(static_cast<uint64_t>(magmaInfo.kvstoreRev));
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
    if (err == Status::Code::Ok) {
        return ENGINE_SUCCESS;
    }
    return ENGINE_TMPFAIL;
}

Vbid MagmaKVStore::getDBFileId(const cb::mcbp::Request& req) {
    return req.getVBucket();
}

size_t MagmaKVStore::getItemCount(Vbid vbid) {
    auto kvHandle = getMagmaKVHandle(vbid);
    std::shared_lock<std::shared_timed_mutex> lock(kvHandle->vbstateMutex);
    if (cachedMagmaInfo[vbid.get()]) {
        return cachedMagmaInfo[vbid.get()]->docCount;
    }
    return 0;
}

std::unique_lock<std::shared_timed_mutex> MagmaKVStore::getExclusiveKVHandle(
        Vbid vbid) {
    std::unique_lock<std::shared_timed_mutex> lock(
            magmaKVHandles[vbid.get()].second);
    while (!magmaKVHandles[vbid.get()].first.unique()) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    return lock;
}

ENGINE_ERROR_CODE MagmaKVStore::getAllKeys(
        Vbid vbid,
        const DiskDocKey& startKey,
        uint32_t count,
        std::shared_ptr<Callback<const DiskDocKey&>> cb) {
    auto kvHandle = getMagmaKVHandle(vbid);

    Slice startKeySlice = {reinterpret_cast<const char*>(startKey.data()),
                           startKey.size()};

    // The magma GetRange API takes a start and end key. getAllKeys only
    // supplies a start key and count of the number of keys it wants.
    // When endKeySlice is uninitialized (as it is here), the
    // endKeySlice.Len() will be 0 which tells GetRange to ignore it.
    Slice endKeySlice;

    uint32_t processed = 0;

    auto callback = [&](Slice& keySlice, Slice& metaSlice, Slice& valueSlice) {
        if (logger->should_log(spdlog::level::TRACE)) {
            logger->TRACE(
                    "MagmaKVStore::getAllKeys callback {} key:{} seqno:{} "
                    "deleted:{}",
                    vbid,
                    cb::UserData{makeDiskDocKey(keySlice).to_string()},
                    getSeqNum(metaSlice),
                    isDeleted(metaSlice));
        }

        if (isDeleted(metaSlice)) {
            return;
        }
        auto retKey = makeDiskDocKey(keySlice);
        cb->callback(retKey);
        processed++;
    };

    if (logger->should_log(spdlog::level::TRACE)) {
        logger->TRACE("MagmaKVStore::getAllKeys {} start:{} count:{})",
                      vbid,
                      cb::UserData{startKey.to_string()},
                      count);
    }

    auto status = magma->GetRange(
            vbid.get(), startKeySlice, endKeySlice, callback, false, count);
    if (!status) {
        logger->critical("MagmaKVStore::getAllKeys {} startKey:{} status:{}",
                         vbid,
                         cb::UserData{startKey.to_string()},
                         status.String());
    }

    if (!status) {
        return magmaErr2EngineErr(status.ErrorCode(), true);
    }

    return ENGINE_SUCCESS;
}

bool MagmaKVStore::compactDB(compaction_ctx* ctx) {
    std::chrono::steady_clock::time_point start =
            std::chrono::steady_clock::now();

    logger->info(
            "compactDB {} purge_before_ts:{} purge_before_seq:{}"
            " drop_deletes:{} purgeSeq:{} retain_erroneous_tombstones:{}",
            ctx->compactConfig.db_file_id,
            ctx->compactConfig.purge_before_ts,
            ctx->compactConfig.purge_before_seq,
            ctx->compactConfig.drop_deletes,
            ctx->compactConfig.purgeSeq,
            ctx->compactConfig.retain_erroneous_tombstones);

    Vbid vbid = ctx->compactConfig.db_file_id;

    auto kvHandle = getMagmaKVHandle(vbid);

    auto status = magma->Sync();
    if (!status) {
        logger->warn("MagmaKVStore::compactDB Sync failed. {}",
                     ctx->compactConfig.db_file_id);
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(compactionCtxMutex);
        compaction_ctxList[vbid.get()] =
                std::make_unique<MagmaCompactionCtx>(ctx, kvHandle);
    }

    auto dropped = getDroppedCollections(vbid);
    ctx->eraserContext =
            std::make_unique<Collections::VB::EraserContext>(dropped);

    {
        std::lock_guard<std::shared_timed_mutex> lock(kvHandle->vbstateMutex);
        ctx->highCompletedSeqno =
                cachedVBStates[vbid.get()]->persistedCompletedSeqno;
    }

    // If there aren't any collections to drop, this compaction is likely
    // being called from a test because kv_engine shouldn't call compactDB
    // to compact levels, magma takes care of that already.
    if (dropped.empty()) {
        // Perform synchronous compaction.
        status = magma->CompactKVStore(vbid.get(), Magma::StoreType::Key);
        if (!status) {
            logger->warn("MagmaKVStore::compactDB Sync failed. vbucket {} ",
                         vbid);
            return false;
        }
    } else {
        for (auto& dc : dropped) {
            std::string keyString =
                    Collections::makeCollectionIdIntoString(dc.collectionId);
            Slice keySlice{keyString};

            if (logger->should_log(spdlog::level::TRACE)) {
                auto docKey = makeDiskDocKey(keySlice);
                logger->TRACE("PurgeKVStore {} key:{}",
                              vbid,
                              cb::UserData{docKey.to_string()});
            }

            // This will find all the keys with a prefix of the collection ID
            // and drop them.
            status = magma->PurgeKVStore(
                    vbid.get(), keySlice, keySlice, nullptr, true);
            if (!status) {
                logger->warn(
                        "MagmaKVStore::compactDB PurgeKVStore {} CID:{} failed "
                        "status:{}",
                        vbid,
                        cb::UserData{makeDiskDocKey(keySlice).to_string()},
                        status.String());
            }

            // We've finish processing this collection.
            // Create a SystemEvent key for the collection and process it.
            auto collectionKey =
                    StoredDocKey(SystemEventFactory::makeKey(
                                         SystemEvent::Collection, keyString),
                                 CollectionID::System);

            keySlice = {reinterpret_cast<const char*>(collectionKey.data()),
                        collectionKey.size()};

            auto key = makeDiskDocKey(keySlice);
            if (logger->should_log(spdlog::level::TRACE)) {
                logger->TRACE(
                        "MagmaKVStore::compactDB processEndOfCollection {} "
                        "key:{}",
                        vbid,
                        cb::UserData{key.to_string()});
            }

            ctx->eraserContext->processEndOfCollection(key.getDocKey(),
                                                       SystemEvent::Collection);
        }
    }

    // Need to save off new vbstate and possibly collections manifest.
    // Start a new CommitBatch
    std::unique_ptr<Magma::CommitBatch> batch;

    status = magma->NewCommitBatch(
            vbid.get(),
            batch,
            static_cast<Magma::KVStoreRevision>(
                    cachedMagmaInfo[vbid.get()]->kvstoreRev));
    if (!status) {
        logger->warn(
                "MagmaKVStore::compactDB failed creating batch for {} "
                "status:{}",
                vbid,
                status.String());
        return false;
    }

    vbucket_state vbs;
    MagmaInfo minfo;
    {
        std::lock_guard<std::shared_timed_mutex> lock(kvHandle->vbstateMutex);
        cachedVBStates[vbid.get()]->onDiskPrepares -= ctx->stats.preparesPurged;
        cachedVBStates[vbid.get()]->purgeSeqno = ctx->max_purged_seq;
        cachedMagmaInfo[vbid.get()]->docCount -=
                ctx->stats.collectionsItemsPurged + ctx->stats.preparesPurged;
        vbs = *cachedVBStates[vbid.get()].get();
        minfo = *cachedMagmaInfo[vbid.get()].get();
    }
    writeVBStateToDisk(vbid, *(batch.get()), vbs, minfo);

    if (ctx->eraserContext->needToUpdateCollectionsMetadata()) {
        std::string s;
        status = setLocalDoc(*(batch.get()), droppedCollectionsKey, s, true);
        if (!status) {
            logger->warn(
                    "MagmaKVStore::saveDocs: magma::setLocalDoc "
                    "{} update dropped collections failed status:{}",
                    vbid,
                    status.String());
        }
    }

    // Write & Sync the CommitBatch
    status = magma->ExecuteCommitBatch(std::move(batch));
    if (!status) {
        logger->warn(
                "MagmaKVStore::saveDocs: magma::ExecuteCommitBatch "
                "{} status:{}",
                vbid,
                status.String());
    } else {
        status = magma->SyncCommitBatches(commitPointEveryBatch);
        if (!status) {
            logger->warn(
                    "MagmaKVStore::saveDocs: magma::SyncCommitBatches {} "
                    "status:{}",
                    vbid,
                    status.String());
        }
    }

    st.compactHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start));

    logger->TRACE(
            "MagmaKVStore::compactDB max_purged_seq:{}"
            " collectionsItemsPurged:{}"
            " collectionsDeletedItemsPurged:{}"
            " tombstonesPurged:{}"
            " preparesPurged:{}",
            ctx->max_purged_seq,
            ctx->stats.collectionsItemsPurged,
            ctx->stats.collectionsDeletedItemsPurged,
            ctx->stats.tombstonesPurged,
            ctx->stats.preparesPurged);

    {
        std::lock_guard<std::mutex> lock(compactionCtxMutex);
        compaction_ctxList[vbid.get()].reset();
    }

    return true;
}

std::unique_ptr<KVFileHandle, KVFileHandleDeleter> MagmaKVStore::makeFileHandle(
        Vbid vbid) {
    std::unique_ptr<MagmaKVFileHandle, KVFileHandleDeleter> kvfh(
            new MagmaKVFileHandle(*this, vbid));
    return std::move(kvfh);
}

RollbackResult MagmaKVStore::rollback(Vbid vbid,
                                      uint64_t rollbackSeqno,
                                      std::shared_ptr<RollbackCB> cb) {
    logger->info("MagmaKVStore::rollback {} seqno:{}", vbid, rollbackSeqno);

    auto kvHandle = getMagmaKVHandle(vbid);

    Magma::FetchBuffer idxBuf;
    Magma::FetchBuffer seqBuf;
    auto cacheLookup = std::make_shared<NoLookupCallback>();
    auto fh = makeFileHandle(vbid);
    cb->setDbHeader(reinterpret_cast<void*>(fh.get()));

    auto keyCallback = [&](const Slice& keySlice,
                           const uint64_t seqno,
                           std::shared_ptr<magma::Snapshot>& keySS,
                           std::shared_ptr<magma::Snapshot>& seqSS) {
        auto docKey = makeDiskDocKey(keySlice);
        CacheLookup lookup(docKey, seqno, vbid);
        cacheLookup->callback(lookup);
        if (cacheLookup->getStatus() == ENGINE_KEY_EEXISTS) {
            return;
        }
        Slice metaSlice;
        Slice valueSlice;
        bool found;
        Status status = magma->Get(vbid.get(),
                                   keySlice,
                                   keySS,
                                   seqSS,
                                   idxBuf,
                                   seqBuf,
                                   metaSlice,
                                   valueSlice,
                                   found);

        if (!status) {
            logger->warn("MagmaKVStore::Rollback Get {} key:{} status:{}",
                         vbid,
                         cb::UserData{docKey.to_string()},
                         status.String());
            return;
        }

        if (logger->should_log(spdlog::level::TRACE)) {
            logger->TRACE(
                    "MagmaKVStore::Rollback Get {} key:{} seqno:{} "
                    "found:{}",
                    vbid,
                    cb::UserData{docKey.to_string()},
                    seqno,
                    found);
        }

        // If we don't find the item or its not the latest,
        // we are not interested.
        if (!found || getSeqNum(metaSlice) != seqno) {
            return;
        }

        auto rv = makeGetValue(
                vbid, keySlice, metaSlice, valueSlice, GetMetaOnly::Yes);
        cb->callback(rv);
    };

    auto status = magma->Rollback(vbid.get(), rollbackSeqno, keyCallback);
    if (!status) {
        logger->critical("MagmaKVStore::rollback Rollback {} status:{}",
                         vbid,
                         status.String());
        return RollbackResult(false);
    }

    // No need to worry about locks cause no one can be
    // using this vbucket since its in the middle of rollback.

    cachedVBStates[vbid.get()].reset();
    cachedMagmaInfo[vbid.get()].reset();
    status = readVBStateFromDisk(vbid);
    if (!status) {
        logger->critical(
                "MagmaKVStore::rollback {} readVBStateFromDisk status:{}",
                vbid,
                status.String());
        return RollbackResult(false);
    }
    auto vbstate = cachedVBStates[vbid.get()].get();
    if (!vbstate) {
        logger->critical(
                "MagmaKVStore::rollback getVBState {} vbstate not found", vbid);
        return RollbackResult(false);
    }

    return RollbackResult(true,
                          vbstate->highSeqno,
                          vbstate->lastSnapStart,
                          vbstate->lastSnapEnd);
}

Collections::KVStore::Manifest MagmaKVStore::getCollectionsManifest(Vbid vbid) {
    auto kvHandle = getMagmaKVHandle(vbid);

    Status status;

    std::string manifest;
    std::tie(status, manifest) = readLocalDoc(vbid, manifestKey);

    std::string openCollections;
    std::tie(status, openCollections) = readLocalDoc(vbid, openCollectionsKey);

    std::string openScopes;
    std::tie(status, openScopes) = readLocalDoc(vbid, openScopesKey);

    std::string droppedCollections;
    std::tie(status, droppedCollections) = readLocalDoc(vbid, droppedCollectionsKey);
    return Collections::KVStore::decodeManifest(
            {reinterpret_cast<const uint8_t*>(manifest.data()),
             manifest.length()},
            {reinterpret_cast<const uint8_t*>(openCollections.data()),
             openCollections.length()},
            {reinterpret_cast<const uint8_t*>(openScopes.data()),
             openScopes.length()},
            {reinterpret_cast<const uint8_t*>(droppedCollections.data()),
             droppedCollections.length()});
}

std::vector<Collections::KVStore::DroppedCollection>
MagmaKVStore::getDroppedCollections(Vbid vbid) {
    Status status;
    std::string dropped;
    std::tie(status, dropped) = readLocalDoc(vbid, droppedCollectionsKey);
    return Collections::KVStore::decodeDroppedCollections(
            {reinterpret_cast<const uint8_t*>(dropped.data()),
             dropped.length()});
}

Status MagmaKVStore::updateCollectionsMeta(
        Vbid vbid,
        Magma::CommitBatch& commitBatch,
        Collections::VB::Flush& collectionsFlush) {
    auto status = updateManifestUid(commitBatch);
    if (!status) {
        return status;
    }

    // If the updateOpenCollections reads the dropped collections, it can pass
    // them via this optional to updateDroppedCollections, thus we only read
    // the dropped list once per update.
    boost::optional<std::vector<Collections::KVStore::DroppedCollection>>
            dropped;

    if (!collectionsMeta.collections.empty() ||
        !collectionsMeta.droppedCollections.empty()) {
        std::tie(status, dropped) = updateOpenCollections(vbid, commitBatch);
        if (!status) {
            return status;
        }
    }

    if (!collectionsMeta.droppedCollections.empty()) {
        if (!dropped.is_initialized()) {
            dropped = getDroppedCollections(vbid);
        }
        status = updateDroppedCollections(vbid, commitBatch, dropped);
        if (!status) {
            return status;
        }
        collectionsFlush.setNeedsPurge();
    }

    if (!collectionsMeta.scopes.empty() ||
        !collectionsMeta.droppedScopes.empty()) {
        status = updateScopes(vbid, commitBatch);
        if (!status) {
            return status;
        }
    }

    collectionsMeta.clear();
    return Status::OK();
}

Status MagmaKVStore::updateManifestUid(Magma::CommitBatch& commitBatch) {
    // write back, no read required
    auto buf = Collections::KVStore::encodeManifestUid(collectionsMeta);
    std::string s{reinterpret_cast<const char*>(buf.data()), buf.size()};
    return setLocalDoc(commitBatch, manifestKey, s);
}

std::pair<magma::Status, std::vector<Collections::KVStore::DroppedCollection>>
MagmaKVStore::updateOpenCollections(Vbid vbid,
                                    Magma::CommitBatch& commitBatch) {
    auto dropped = getDroppedCollections(vbid);
    Status status;
    std::string collections;
    std::tie(status, collections) = readLocalDoc(vbid, openCollectionsKey);
    auto buf = Collections::KVStore::encodeOpenCollections(
            dropped,
            collectionsMeta,
            {reinterpret_cast<const uint8_t*>(collections.data()),
             collections.length()});

    std::string s{reinterpret_cast<const char*>(buf.data()), buf.size()};
    status = setLocalDoc(commitBatch, openCollectionsKey, s);
    return std::make_pair(status, dropped);
}

magma::Status MagmaKVStore::updateDroppedCollections(
        Vbid vbid,
        Magma::CommitBatch& commitBatch,
        boost::optional<std::vector<Collections::KVStore::DroppedCollection>>
                dropped) {
    for (const auto& drop : collectionsMeta.droppedCollections) {
        // Delete the 'stats' document for the collection
        deleteCollectionStats(commitBatch, drop.collectionId);
    }

    // If the input 'dropped' is not initialised we must read the dropped
    // collection data
    if (!dropped.is_initialized()) {
        dropped = getDroppedCollections(vbid);
    }

    auto buf = Collections::KVStore::encodeDroppedCollections(collectionsMeta,
                                                              dropped);
    std::string s{reinterpret_cast<const char*>(buf.data()), buf.size()};
    return setLocalDoc(commitBatch, droppedCollectionsKey, s);
}

std::string MagmaKVStore::getCollectionsStatsKey(CollectionID cid) {
    return std::string{"|" + cid.to_string() + "|"};
}

void MagmaKVStore::saveCollectionStats(
        magma::Magma::CommitBatch& commitBatch,
        CollectionID cid,
        const Collections::VB::PersistedStats& stats) {
    auto key = getCollectionsStatsKey(cid);
    Slice keySlice(key);
    auto encodedStats = stats.getLebEncodedStats();
    auto status = setLocalDoc(commitBatch, keySlice, encodedStats);
    if (!status) {
        logger->warn(
                "MagmaKVStore::saveCollectionStats setLocalDoc {} failed. "
                "status:{}",
                keySlice.ToString(),
                status.String());
    }
    return;
}

Collections::VB::PersistedStats MagmaKVStore::getCollectionStats(
        const KVFileHandle& kvFileHandle, CollectionID cid) {
    const auto& kvfh = static_cast<const MagmaKVFileHandle&>(kvFileHandle);
    auto vbid = kvfh.vbid;
    auto key = getCollectionsStatsKey(cid);
    Slice keySlice(key);
    Status status;
    std::string stats;
    std::tie(status, stats) = readLocalDoc(vbid, keySlice);
    if (!status) {
        return {};
    }
    return Collections::VB::PersistedStats(stats.c_str(), stats.size());
}

magma::Status MagmaKVStore::deleteCollectionStats(
        Magma::CommitBatch& commitBatch, CollectionID cid) {
    auto key = getCollectionsStatsKey(cid);
    Slice keySlice(key);
    std::string value;
    return setLocalDoc(commitBatch, keySlice, value, true);
}

magma::Status MagmaKVStore::updateScopes(Vbid vbid,
                                         Magma::CommitBatch& commitBatch) {
    Status status;
    std::string scopes;
    std::tie(status, scopes) = readLocalDoc(vbid, openScopesKey);
    auto buf = encodeScopes(
            collectionsMeta,
            {reinterpret_cast<const uint8_t*>(scopes.data()), scopes.length()});
    std::string s{reinterpret_cast<const char*>(buf.data()), buf.size()};
    return setLocalDoc(commitBatch, openScopesKey, s);
}
