/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "magma-kvstore.h"

#include "bucket_logger.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "item.h"
#include "kvstore_transaction_context.h"
#include "magma-kvstore_config.h"
#include "magma-kvstore_iorequest.h"
#include "magma-kvstore_metadata.h"
#include "objectregistry.h"
#include "vb_commit.h"
#include "vbucket.h"
#include "vbucket_state.h"
#include <executor/executorpool.h>
#include <mcbp/protocol/unsigned_leb128.h>
#include <nlohmann/json.hpp>
#include <statistics/cbstat_collector.h>
#include <utilities/logtags.h>
#include <algorithm>
#include <cstring>
#include <limits>
#include <utility>

class Snapshot;

using namespace magma;
using namespace std::chrono_literals;

// Keys to localdb docs
static const std::string vbstateKey = "_vbstate";
static const std::string manifestKey = "_collections/manifest";
static const std::string openCollectionsKey = "_collections/open";
static const std::string openScopesKey = "_scopes/open";
static const std::string droppedCollectionsKey = "_collections/dropped";

// Unfortunately, turning on logging for the tests is limited to debug
// mode. While we are in the midst of dropping in magma, this provides
// a simple way to change all the logging->trace calls to logging->debug
// by changing trace to debug..
// Once magma has been completed, we can remove this #define and change
// all the logging calls back to trace.
#define TRACE trace

namespace magmakv {
MetaData::MetaData(const Item& it)
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
    durabilityLevel = static_cast<uint8_t>(it.getDurabilityReqs().getLevel());
}

MetaData::MetaData(bool isDeleted, uint32_t valueSize, int64_t seqno, Vbid vbid)
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
}

cb::durability::Level MetaData::getDurabilityLevel() const {
    return static_cast<cb::durability::Level>(durabilityLevel);
}

std::string MetaData::to_string(Operation op) const {
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
    return std::string{"Unknown:" + std::to_string(static_cast<uint8_t>(op))};
}

std::string MetaData::to_string() const {
    std::stringstream ss;
    int vers = metaDataVersion;
    int dt = datatype;
    cb::durability::Requirements req(getDurabilityLevel(), {});
    ss << "bySeqno:" << bySeqno << " cas:" << cas << " exptime:" << exptime
       << " revSeqno:" << revSeqno << " flags:" << flags
       << " valueSize:" << valueSize << " vbid:" << vbid
       << " deleted:" << (deleted == 0 ? "false" : "true") << " deleteSource:"
       << (deleted == 0 ? " " : deleteSource == 0 ? "Explicit" : "TTL")
       << " version:" << vers << " datatype:" << dt
       << " operation:" << to_string(getOperation())
       << " durabilityLevel:" << cb::durability::to_string(req);
    return ss.str();
}

MetaData::Operation MetaData::toOperation(queue_op op) {
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

/**
 * Magma stores an item in 3 slices... key, metadata, value
 * See libmagma/slice.h for more info on slices.
 *
 * Helper functions to pull metadata stuff out of the metadata slice.
 * The are used down in magma and are passed in as part of the configuration.
 */
static const MetaData& getDocMeta(const Slice& metaSlice) {
    const auto* docMeta =
            reinterpret_cast<MetaData*>(const_cast<char*>(metaSlice.Data()));
    return *docMeta;
}

static uint64_t getSeqNum(const Slice& metaSlice) {
    return getDocMeta(metaSlice).bySeqno;
}

static size_t getValueSize(const Slice& metaSlice) {
    return getDocMeta(metaSlice).valueSize;
}

static uint32_t getExpiryTime(const Slice& metaSlice) {
    return getDocMeta(metaSlice).exptime;
}

static bool isDeleted(const Slice& metaSlice) {
    return getDocMeta(metaSlice).deleted > 0;
}

static bool isCompressed(const Slice& metaSlice) {
    return mcbp::datatype::is_snappy(getDocMeta(metaSlice).datatype);
}

static Vbid getVbid(const Slice& metaSlice) {
    return Vbid(getDocMeta(metaSlice).vbid);
}

static bool isPrepared(const Slice& metaSlice) {
    return static_cast<MetaData::Operation>(getDocMeta(metaSlice).operation) ==
           MetaData::Operation::PreparedSyncWrite;
}

static bool isAbort(const Slice& metaSlice) {
    return static_cast<MetaData::Operation>(getDocMeta(metaSlice).operation) ==
           MetaData::Operation::Abort;
}

} // namespace magmakv

MagmaRequest::MagmaRequest(queued_item it, std::shared_ptr<BucketLogger> logger)
    : IORequest(std::move(it)),
      docMeta(magmakv::MetaData(*item)),
      docBody(item->getValue()) {
    if (logger->should_log(spdlog::level::TRACE)) {
        logger->TRACE("MagmaRequest:{}", to_string());
    }
}

std::string MagmaRequest::to_string() {
    std::stringstream ss;
    ss << "Key:" << key.to_string() << " docMeta:" << docMeta.to_string()
       << " itemOldExists:" << (itemOldExists ? "true" : "false")
       << " itemOldIsDelete:" << (itemOldIsDelete ? "true" : "false");
    return ss.str();
}

static DiskDocKey makeDiskDocKey(const Slice& key) {
    return DiskDocKey{key.Data(), key.Len()};
}

MagmaKVStore::MagmaCompactionCB::MagmaCompactionCB(
        MagmaKVStore& magmaKVStore, std::shared_ptr<CompactionContext> ctx)
    : magmaKVStore(magmaKVStore), ctx(std::move(ctx)) {
    magmaKVStore.logger->TRACE("MagmaCompactionCB constructor");
}

MagmaKVStore::MagmaCompactionCB::~MagmaCompactionCB() {
    magmaKVStore.logger->debug("MagmaCompactionCB destructor");
}

bool MagmaKVStore::compactionCallBack(MagmaKVStore::MagmaCompactionCB& cbCtx,
                                      const magma::Slice& keySlice,
                                      const magma::Slice& metaSlice,
                                      const magma::Slice& valueSlice) {
    Expects(currEngine == ObjectRegistry::getCurrentEngine());
    // If we are compacting the localDb, items don't have metadata so
    // we always keep everything.
    if (metaSlice.Len() == 0) {
        return false;
    }

    auto vbid = magmakv::getVbid(metaSlice);
    auto& itemString = cbCtx.itemKeyBuf;
    if (logger->should_log(spdlog::level::TRACE)) {
        itemString.str(std::string());
        itemString << "key:"
                   << cb::UserData{makeDiskDocKey(keySlice).to_string()};
        itemString << " ";
        itemString << magmakv::getDocMeta(metaSlice).to_string();
        logger->TRACE("MagmaCompactionCB: {} {}",
                      vbid,
                      cb::UserData(itemString.str()));
    }

    if (!cbCtx.ctx) {
        // Don't already have a compaction context (i.e. this is the first
        // key for an implicit compaction) - atempt to create one.

        // Note that Magma implicit (internal) compactions can start _as soon
        // as_ the Magma instance is Open()'d, which means this method can be
        // called beforew Warmup has completed - and hence before
        // makeCompactionContextCallback is assigned to a non-empty value.
        // Until warmup *does* complete it isn't possible for us to know how
        // to correctly deal with those keys - for example need to have
        // initialised document counters during warmup to be able to update
        // counter on TTL expiry. As such, until we have a non-empty
        // makeCompactionContextCallback, simply return false.
        if (!makeCompactionContextCallback) {
            return false;
        }
        cbCtx.ctx = makeCompactionContext(vbid);
    }

    auto seqno = magmakv::getSeqNum(metaSlice);
    auto exptime = magmakv::getExpiryTime(metaSlice);

    if (cbCtx.ctx->droppedKeyCb) {
        // We need to check both committed and prepared documents - if the
        // collection has been logically deleted then we need to discard
        // both types of keys.
        // As such use the docKey (i.e. without any DurabilityPrepare
        // namespace) when checking if logically deleted;
        auto diskKey = makeDiskDocKey(keySlice);
        if (cbCtx.ctx->eraserContext->isLogicallyDeleted(diskKey.getDocKey(),
                                                         seqno)) {
            // Inform vb that the key@seqno is dropped
            cbCtx.ctx->droppedKeyCb(
                    diskKey, seqno, magmakv::isAbort(metaSlice), cbCtx.ctx->highCompletedSeqno);

            { // Locking scope for magmaDbStats
                auto dbStats = cbCtx.magmaDbStats.stats.wlock();

                if (magmakv::isPrepared(metaSlice)) {
                    cbCtx.ctx->stats.preparesPurged++;
                } else {
                    if (magmakv::isDeleted(metaSlice)) {
                        cbCtx.ctx->stats.collectionsDeletedItemsPurged++;
                    }
                }
            }

            if (logger->should_log(spdlog::level::TRACE)) {
                logger->TRACE(
                        "MagmaKVStore::MagmaCompactionCallback: DROP "
                        "collections "
                        "{}",
                        itemString.str());
            }
            return true;
        }
    }

    if (magmakv::isDeleted(metaSlice)) {
        uint64_t maxSeqno;
        auto status = magma->GetMaxSeqno(vbid.get(), maxSeqno);
        if (!status) {
            throw std::runtime_error("MagmaCompactionCallback: Failed : " +
                                     vbid.to_string() + " " + status.String());
        }

        // A bunch of DCP code relies on us keeping the last item (it may be a
        // tombstone) so we can't purge the item at maxSeqno.
        if (seqno != maxSeqno) {
            bool drop = false;
            if (cbCtx.ctx->compactConfig.drop_deletes) {
                if (logger->should_log(spdlog::level::TRACE)) {
                    logger->TRACE("MagmaCompactionCB: {} DROP drop_deletes {}",
                                  vbid,
                                  cb::UserData(itemString.str()));
                }
                drop = true;
            }

            if (exptime && exptime < cbCtx.ctx->compactConfig.purge_before_ts) {
                if (logger->should_log(spdlog::level::TRACE)) {
                    logger->TRACE(
                            "MagmaCompactionCB: {} DROP expired tombstone {}",
                            vbid,
                            cb::UserData(itemString.str()));
                }
                drop = true;
            }

            if (drop) {
                cbCtx.ctx->stats.tombstonesPurged++;
                if (cbCtx.ctx->max_purged_seq < seqno) {
                    cbCtx.ctx->max_purged_seq = seqno;
                }
                auto dbStats = cbCtx.magmaDbStats.stats.wlock();
                if (dbStats->purgeSeqno < seqno) {
                    dbStats->purgeSeqno = seqno;
                }
                return true;
            }
        }
    } else {
        // We can remove any prepares that have been completed. This works
        // because we send Mutations instead of Commits when streaming from
        // Disk so we do not need to send a Prepare message to keep things
        // consistent on a replica.
        if (magmakv::isPrepared(metaSlice)) {
            if (seqno <= cbCtx.ctx->highCompletedSeqno) {
                cbCtx.ctx->stats.preparesPurged++;

                if (logger->should_log(spdlog::level::TRACE)) {
                    logger->TRACE(
                            "MagmaKVStore::MagmaCompactionCallback: "
                            "DROP prepare {}",
                            itemString.str());
                }
                return true;
            }
        }
        time_t currTime = ep_real_time();
        if (exptime && exptime < currTime) {
            auto docMeta = magmakv::getDocMeta(metaSlice);
            auto itm =
                    std::make_unique<Item>(makeDiskDocKey(keySlice).getDocKey(),
                                           docMeta.flags,
                                           docMeta.exptime,
                                           valueSlice.Data(),
                                           valueSlice.Len(),
                                           docMeta.datatype,
                                           docMeta.cas,
                                           docMeta.bySeqno,
                                           vbid,
                                           docMeta.revSeqno);
            if (magmakv::isCompressed(metaSlice)) {
                itm->decompressValue();
            }
            itm->setDeleted(DeleteSource::TTL);
            cbCtx.ctx->expiryCallback->callback(*(itm.get()), currTime);

            if (logger->should_log(spdlog::level::TRACE)) {
                logger->TRACE("MagmaCompactionCB: {} expiry callback {}",
                              vbid,
                              cb::UserData(itemString.str()));
            }
        }
    }

    if (logger->should_log(spdlog::level::TRACE)) {
        logger->TRACE("MagmaCompactionCB: {} KEEP {}",
                      vbid,
                      cb::UserData(itemString.str()));
    }
    return false;
}

MagmaKVStore::MagmaKVStore(MagmaKVStoreConfig& configuration)
    : KVStore(),
      configuration(configuration),
      pendingReqs(std::make_unique<PendingRequestQueue>()),
      magmaPath(configuration.getDBName() + "/magma." +
                std::to_string(configuration.getShardId())),
      scanCounter(0),
      currEngine(ObjectRegistry::getCurrentEngine()) {
    configuration.magmaCfg.Path = magmaPath;
    configuration.magmaCfg.MaxKVStores = configuration.getMaxVBuckets();
    configuration.magmaCfg.MaxKVStoreLSDBufferSize =
            configuration.getMagmaDeleteMemtableWritecache();
    configuration.magmaCfg.LSDFragmentationRatio =
            configuration.getMagmaDeleteFragRatio();
    configuration.magmaCfg.MaxCheckpoints =
            configuration.getMagmaMaxCheckpoints();
    configuration.magmaCfg.CheckpointCreationInterval =
            configuration.getMagmaCheckpointInterval();
    configuration.magmaCfg.CheckpointCreationThreshold =
            configuration.getMagmaCheckpointThreshold();
    configuration.magmaCfg.HeartbeatInterval =
            configuration.getMagmaHeartbeatInterval();
    configuration.magmaCfg.MinValueSize =
            configuration.getMagmaValueSeparationSize();
    configuration.magmaCfg.MaxWriteCacheSize =
            configuration.getMagmaMaxWriteCache();
    configuration.magmaCfg.WALBufferSize =
            configuration.getMagmaInitialWalBufferSize();
    configuration.magmaCfg.WALSyncTime = 0ms;
    configuration.magmaCfg.EnableWAL = configuration.getMagmaEnableWAL();
    configuration.magmaCfg.ExpiryFragThreshold =
            configuration.getMagmaExpiryFragThreshold();
    configuration.magmaCfg.KVStorePurgerInterval =
            configuration.getMagmaExpiryPurgerInterval();
    configuration.magmaCfg.GetSeqNum = magmakv::getSeqNum;
    configuration.magmaCfg.GetExpiryTime = [this](const magma::Slice& slice) {
        return getExpiryOrPurgeTime(slice);
    };
    configuration.magmaCfg.GetValueSize = magmakv::getValueSize;
    configuration.magmaCfg.IsTombstone = magmakv::isDeleted;
    configuration.magmaCfg.EnableDirectIO =
            configuration.getMagmaEnableDirectIo();
    configuration.magmaCfg.EnableBlockCache =
            configuration.getMagmaEnableBlockCache();
    configuration.magmaCfg.WriteCacheRatio =
            configuration.getMagmaWriteCacheRatio();
    configuration.magmaCfg.MaxRecoveryBytes =
            configuration.getMagmaMaxRecoveryBytes();
    configuration.magmaCfg.LSMMaxLevel0CompactionTTL =
            configuration.getMagmaMaxLevel0TTL();
    configuration.magmaCfg.BloomFilterAccuracy =
            configuration.getMagmaBloomFilterAccuracy();
    configuration.magmaCfg.BloomFilterAccuracyForBottomLevel =
            configuration.getMagmaBloomFilterAccuracyForBottomLevel();

    configuration.setStore(this);

    magma::SetMaxOpenFiles(configuration.getMaxFileDescriptors());

    // To save memory only allocate counters for the number of vBuckets that
    // this shard will have to deal with
    auto cacheSize = getCacheSize();
    cachedVBStates.resize(cacheSize);
    kvstoreRevList.resize(cacheSize, Monotonic<uint64_t>(0));

    useUpsertForSet = configuration.getMagmaEnableUpsert();
    if (useUpsertForSet) {
        configuration.magmaCfg.EnableUpdateStatusForSet = false;
    } else {
        configuration.magmaCfg.EnableUpdateStatusForSet = true;
    }

    doCheckpointEveryBatch = configuration.getMagmaCheckpointEveryBatch();

    // Set up thread and memory tracking.
    // Please note... SetupThreadContext and ResetThreadContext
    // will being removed once magma tracking is complete.
    configuration.magmaCfg.SetupThreadContext = [this]() {
        ObjectRegistry::onSwitchThread(currEngine, false);
    };
    configuration.magmaCfg.ResetThreadContext = []() {
        ObjectRegistry::onSwitchThread(nullptr);
    };

    // If currEngine is null, which can happen with some tests,
    // that is ok because a null currEngine means we are operating
    // in a global environment.
    configuration.magmaCfg.ExecutionEnv = currEngine;
    configuration.magmaCfg.SwitchExecutionEnvFunc =
            [](void* env) -> EventuallyPersistentEngine* {
        auto eng = static_cast<EventuallyPersistentEngine*>(env);
        return ObjectRegistry::onSwitchThread(eng, true);
    };

    configuration.magmaCfg.MakeCompactionCallback = [&]() {
        return std::make_unique<MagmaKVStore::MagmaCompactionCB>(*this);
    };

    configuration.magmaCfg.MakeUserStats = []() {
        return std::make_unique<MagmaDbStats>();
    };

    // Create the data directory.
    createDataDir(configuration.magmaCfg.Path);

    // Create a logger that is prefixed with magma so that all code from
    // wrapper down through magma uses this logger. As the spdlog API does not
    // set their log functions to virtual and magma needs to maintain some
    // separation from KV it does not use the BucketLogger log functions which
    // prefix the engine name. As such we have to manually add the prefix to the
    // logger name/prefix here.

    EventuallyPersistentEngine* engine = ObjectRegistry::getCurrentEngine();
    // Some tests may not have an engine.
    std::string loggerName;
    if (engine) {
        loggerName += '(' + engine->getName() + ") ";
    }

    loggerName += "magma_" + std::to_string(configuration.getShardId());
    logger = BucketLogger::createBucketLogger(loggerName);
    configuration.magmaCfg.LogContext = logger;
    configuration.magmaCfg.UID = loggerName;

    // Open the magma instance for this shard and populate the vbstate.
    magma = std::make_unique<Magma>(configuration.magmaCfg);
    auto status = magma->Open();
    if (!status) {
        std::string err =
                "MagmaKVStore Magma open failed. Status:" + status.String();
        logger->critical(err);
        throw std::logic_error(err);
    }

    setMaxDataSize(configuration.getBucketQuota());
    setMagmaFragmentationPercentage(
            configuration.getMagmaFragmentationPercentage());
    calculateAndSetMagmaThreads();

    auto kvstoreList = magma->GetKVStoreList();
    for (auto& kvid : kvstoreList) {
        // While loading the vbstate cache, set the kvstoreRev.
        status = loadVBStateCache(Vbid(kvid), true);
        ++st.numLoadedVb;
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
    logger->unregister();
}

void MagmaKVStore::deinitialize() {
    logger->info("MagmaKVStore: {} deinitializing", configuration.getShardId());

    if (!inTransaction) {
        magma->Sync(true);
    }

    // Close shuts down all of the magma background threads (compaction is the
    // one that we care about here). The compaction callbacks require the magma
    // instance to exist so we must do this before we reset it.
    magma->Close();

    // Flusher should have already been stopped so it should be safe to destroy
    // the magma instance now
    magma.reset();

    logger->info("MagmaKVStore: {} deinitialized", configuration.getShardId());
}

std::string MagmaKVStore::getVBDBSubdir(Vbid vbid) {
    return magmaPath + std::to_string(vbid.get());
}

bool MagmaKVStore::commit(TransactionContext& txnCtx, VB::Commit& commitData) {
    // This behaviour is to replicate the one in Couchstore.
    // If `commit` is called when not in transaction, just return true.
    if (!inTransaction) {
        logger->warn("MagmaKVStore::commit called not in transaction");
        return true;
    }

    if (pendingReqs->size() == 0) {
        inTransaction = false;
        return true;
    }

    kvstats_ctx kvctx(commitData);
    bool success = true;

    // Flush all documents to disk
    auto errCode = saveDocs(txnCtx, commitData, kvctx);
    if (errCode != static_cast<int>(cb::engine_errc::success)) {
        logger->warn("MagmaKVStore::commit: saveDocs {} errCode:{}",
                     pendingReqs->front().getVbID(),
                     errCode);
        success = false;
    }

    postFlushHook();

    commitCallback(txnCtx, errCode, kvctx);

    // This behaviour is to replicate the one in Couchstore.
    // Set `in_transanction = false` only if `commit` is successful.
    if (success) {
        updateCachedVBState(txnCtx.vbid, commitData.proposedVBState);
        inTransaction = false;
    }

    pendingReqs->clear();
    logger->TRACE("MagmaKVStore::commit success:{}", success);

    return success;
}

void MagmaKVStore::commitCallback(TransactionContext& txnCtx,
                                  int errCode,
                                  kvstats_ctx&) {
    const auto flushSuccess = (errCode == Status::Code::Ok);
    for (const auto& req : *pendingReqs) {
        size_t mutationSize =
                req.getRawKeyLen() + req.getBodySize() + req.getMetaSize();
        st.io_num_write++;
        st.io_document_write_bytes += mutationSize;

        if (req.isDelete()) {
            FlushStateDeletion state;
            if (flushSuccess) {
                if (req.oldItemExists() && !req.oldItemIsDelete()) {
                    // Deletion is for an existing item on disk
                    state = FlushStateDeletion::Delete;
                } else {
                    // Deletion is for a non-existing item on disk
                    state = FlushStateDeletion::DocNotFound;
                }
                st.delTimeHisto.add(req.getDelta());
            } else {
                state = FlushStateDeletion::Failed;
                ++st.numDelFailure;
            }

            if (logger->should_log(spdlog::level::TRACE)) {
                logger->TRACE(
                        "MagmaKVStore::commitCallback(Delete) {} key:{} "
                        "errCode:{} "
                        "deleteState:{}",
                        pendingReqs->front().getVbID(),
                        cb::UserData(req.getKey().to_string()),
                        errCode,
                        to_string(state));
            }

            txnCtx.deleteCallback(req.getItem(), state);
        } else {
            FlushStateMutation state;
            if (flushSuccess) {
                if (req.oldItemExists() && !req.oldItemIsDelete()) {
                    // Mutation is for an existing item on disk
                    state = FlushStateMutation::Update;
                } else {
                    // Mutation is for a non-existing item on disk
                    state = FlushStateMutation::Insert;
                }
                st.writeTimeHisto.add(req.getDelta());
                st.writeSizeHisto.add(mutationSize);
            } else {
                state = FlushStateMutation::Failed;
                ++st.numSetFailure;
            }

            if (logger->should_log(spdlog::level::TRACE)) {
                logger->TRACE(
                        "MagmaKVStore::commitCallback(Set) {} key:{} "
                        "errCode:{} "
                        "setState:{}",
                        pendingReqs->front().getVbID(),
                        cb::UserData(req.getKey().to_string()),
                        errCode,
                        to_string(state));
            }

            txnCtx.setCallback(req.getItem(), state);
        }
    }
}

void MagmaKVStore::rollback() {
    if (inTransaction) {
        inTransaction = false;
    }
}

StorageProperties MagmaKVStore::getStorageProperties() const {
    StorageProperties rv(StorageProperties::ConcurrentWriteCompact::Yes,
                         StorageProperties::ByIdScan::No,
                         // @TODO MB-33784: Enable auto de-dupe if/when magma
                         // supports it
                         StorageProperties::AutomaticDeduplication::No);
    return rv;
}

void MagmaKVStore::setMaxDataSize(size_t size) {
    const size_t memoryQuota = (size / configuration.getMaxShards()) *
                               configuration.getMagmaMemQuotaRatio();
    magma->SetMemoryQuota(memoryQuota);
}

// Note: This routine is only called during warmup. The caller
// can not make changes to the vbstate or it would cause race conditions.
std::vector<vbucket_state*> MagmaKVStore::listPersistedVbuckets() {
    std::vector<vbucket_state*> result;
    Vbid vbid(0);
    for (const auto& vb : cachedVBStates) {
        if (vb) {
            mergeMagmaDbStatsIntoVBState(*vb, vbid);
        }
        result.emplace_back(vb.get());
        vbid++;
    }
    return result;
}

void MagmaKVStore::set(TransactionContext& txnCtx, queued_item item) {
    if (!inTransaction) {
        throw std::logic_error(
                "MagmaKVStore::set: inTransaction must be true to perform a "
                "set operation.");
    }
    pendingReqs->emplace_back(std::move(item), logger);
}

GetValue MagmaKVStore::get(const DiskDocKey& key,
                           Vbid vb,
                           ValueFilter filter) const {
    return getWithHeader(key, vb, filter);
}

GetValue MagmaKVStore::getWithHeader(const KVFileHandle& kvFileHandle,
                                     const DiskDocKey& key,
                                     Vbid vbid,
                                     ValueFilter filter) const {
    return getWithHeader(key, vbid, filter);
}

GetValue MagmaKVStore::getWithHeader(const DiskDocKey& key,
                                     Vbid vbid,
                                     ValueFilter filter) const {
    Slice keySlice = {reinterpret_cast<const char*>(key.data()), key.size()};
    Slice metaSlice;
    Slice valueSlice;
    Magma::FetchBuffer idxBuf;
    Magma::FetchBuffer seqBuf;
    bool found;

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
                found ? magmakv::isDeleted(metaSlice) : false);
    }

    if (!status) {
        logger->warn("MagmaKVStore::getWithHeader {} key:{} status:{}",
                     vbid,
                     cb::UserData{makeDiskDocKey(keySlice).to_string()},
                     status.String());
        st.numGetFailure++;
        return GetValue{nullptr, magmaErr2EngineErr(status.ErrorCode())};
    }

    if (!found) {
        // Whilst this isn't strictly a failure if we're running full eviction
        // it could be considered one for value eviction.
        st.numGetFailure++;
        return GetValue{nullptr, cb::engine_errc::no_such_key};
    }

    // record stats
    st.readTimeHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start));
    st.readSizeHisto.add(keySlice.Len() + metaSlice.Len() + valueSlice.Len());

    ++st.io_bg_fetch_docs_read;
    st.io_bgfetch_doc_bytes +=
            keySlice.Len() + metaSlice.Len() + valueSlice.Len();

    return makeGetValue(vbid, keySlice, metaSlice, valueSlice, filter);
}

using GetOperations = magma::OperationsList<Magma::GetOperation>;

void MagmaKVStore::getMulti(Vbid vbid, vb_bgfetch_queue_t& itms) const {
    // Convert the vb_bgfetch_queue_t (which is a std::unordered_map
    // under the covers) to a vector of GetOperations.
    // Note: We can't pass vb_bgfetch_queue_t to GetDocs because GetDocs
    // requires a list type which can be broken up for use by coroutines
    // and a std::map doesn't support a numeric 'at' index.
    GetOperations getOps;
    for (auto& it : itms) {
        auto& key = it.first;
        getOps.Add(Magma::GetOperation(
                {reinterpret_cast<const char*>(key.data()), key.size()},
                &it.second));
    }

    auto cb = [this, &vbid](bool found,
                            Status status,
                            const Magma::GetOperation& op,
                            const Slice& metaSlice,
                            const Slice& valueSlice) {
        if (logger->should_log(spdlog::level::TRACE)) {
            logger->TRACE(
                    "MagmaKVStore::getMulti {} key:{} status:{} found:{} "
                    "deleted:{}",
                    vbid,
                    cb::UserData{makeDiskDocKey(op.Key).to_string()},
                    status.String(),
                    found,
                    found ? magmakv::isDeleted(metaSlice) : false);
        }
        auto errCode = magmaErr2EngineErr(status.ErrorCode(), found);
        auto* bg_itm_ctx =
                reinterpret_cast<vb_bgfetch_item_ctx_t*>(op.UserContext);
        bg_itm_ctx->value.setStatus(errCode);
        if (found) {
            bg_itm_ctx->value = makeGetValue(vbid,
                                             op.Key,
                                             metaSlice,
                                             valueSlice,
                                             bg_itm_ctx->getValueFilter());
            GetValue* rv = &bg_itm_ctx->value;

            for (auto& fetch : bg_itm_ctx->getRequests()) {
                fetch->value = rv;
                st.readTimeHisto.add(
                        std::chrono::duration_cast<std::chrono::microseconds>(
                                std::chrono::steady_clock::now() -
                                fetch->initTime));
                st.readSizeHisto.add(bg_itm_ctx->value.item->getKey().size() +
                                     bg_itm_ctx->value.item->getNBytes());
            }
            ++st.io_bg_fetch_docs_read;
            st.io_bgfetch_doc_bytes +=
                    op.Key.Len() + metaSlice.Len() + valueSlice.Len();
        } else {
            if (!status) {
                logger->critical(
                        "MagmaKVStore::getMulti::GetDocs {} key:{} status:{}, ",
                        vbid,
                        cb::UserData{makeDiskDocKey(op.Key).to_string()},
                        status.String());
                st.numGetFailure++;
            }
        }
    };

    auto status = magma->GetDocs(vbid.get(), getOps, cb);
    if (!status) {
        logger->warn("MagmaKVStore::getMulti::GetDocs {} failed. Status:{}",
                     vbid,
                     status.String());
    }
}

void MagmaKVStore::getRange(Vbid vbid,
                            const DiskDocKey& startKey,
                            const DiskDocKey& endKey,
                            ValueFilter filter,
                            const GetRangeCb& cb) const {
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
                    magmakv::getSeqNum(metaSlice),
                    magmakv::isDeleted(metaSlice));
        }

        if (magmakv::isDeleted(metaSlice)) {
            return;
        }
        auto rv = makeGetValue(vbid, keySlice, metaSlice, valueSlice, filter);
        cb(std::move(rv));
    };

    if (logger->should_log(spdlog::level::TRACE)) {
        logger->TRACE("MagmaKVStore::getRange {} start:{} end:{}",
                      vbid,
                      cb::UserData{makeDiskDocKey(startKeySlice).to_string()},
                      cb::UserData{makeDiskDocKey(endKeySlice).to_string()});
    }

    auto status = magma->GetRange(vbid.get(),
                                  startKeySlice,
                                  endKeySlice,
                                  callback,
                                  filter != ValueFilter::KEYS_ONLY);
    if (!status) {
        logger->critical(
                "MagmaKVStore::getRange {} start:{} end:{} status:{}",
                vbid,
                cb::UserData{makeDiskDocKey(startKeySlice).to_string()},
                cb::UserData{makeDiskDocKey(endKeySlice).to_string()},
                status.String());
        st.numGetFailure++;
    }
}

void MagmaKVStore::del(TransactionContext& txnCtx, queued_item item) {
    if (!inTransaction) {
        throw std::logic_error(
                "MagmaKVStore::del: inTransaction must be true to perform a "
                "delete operation.");
    }
    pendingReqs->emplace_back(std::move(item), logger);
}

void MagmaKVStore::delVBucket(Vbid vbid, uint64_t kvstoreRev) {
    auto status = magma->DeleteKVStore(
            vbid.get(), static_cast<Magma::KVStoreRevision>(kvstoreRev));
    logger->info(
            "MagmaKVStore::delVBucket DeleteKVStore {} kvstoreRev:{}. "
            "status:{}",
            vbid,
            kvstoreRev,
            status.String());
}

void MagmaKVStore::prepareToCreateImpl(Vbid vbid) {
    auto vbstate = getCachedVBucketState(vbid);
    if (vbstate) {
        vbstate->reset();
    }
    kvstoreRevList[getCacheSlot(vbid)]++;

    logger->info("MagmaKVStore::prepareToCreateImpl {} kvstoreRev:{}",
                 vbid,
                 kvstoreRevList[getCacheSlot(vbid)]);
}

uint64_t MagmaKVStore::prepareToDeleteImpl(Vbid vbid) {
    Status status;
    Magma::KVStoreRevision kvsRev;
    std::tie(status, kvsRev) = magma->GetKVStoreRevision(vbid.get());
    if (!status) {
        logger->critical(
                "MagmaKVStore::prepareToDeleteImpl {} "
                "GetKVStoreRevision failed. Status:{}",
                vbid,
                status.String());
        // Even though we couldn't get the kvstore revision from magma,
        // we'll use what is in kv engine and assume its the latest.
        kvsRev = kvstoreRevList[getCacheSlot(vbid)];
    }
    return kvsRev;
}

// Note: It is assumed this can only be called from bg flusher thread or
// there will be issues with writes coming from multiple threads.
bool MagmaKVStore::snapshotVBucket(Vbid vbid, const vbucket_state& newVBState) {
    if (logger->should_log(spdlog::level::TRACE)) {
        logger->TRACE("MagmaKVStore::snapshotVBucket {} newVBState:{}",
                      vbid,
                      encodeVBState(newVBState));
    }

    if (!needsToBePersisted(vbid, newVBState)) {
        return true;
    }

    auto start = std::chrono::steady_clock::now();

    if (!writeVBStateToDisk(vbid, newVBState)) {
        return false;
    }

    updateCachedVBState(vbid, newVBState);

    st.snapshotHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start));

    return true;
}

std::unique_ptr<Item> MagmaKVStore::makeItem(Vbid vb,
                                             const Slice& keySlice,
                                             const Slice& metaSlice,
                                             const Slice& valueSlice,
                                             ValueFilter filter) const {
    auto key = makeDiskDocKey(keySlice);
    auto& meta = *reinterpret_cast<const magmakv::MetaData*>(metaSlice.Data());

    const bool includeValue = (filter != ValueFilter::KEYS_ONLY ||
                               key.getDocKey().isInSystemCollection()) &&
                              meta.valueSize;

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
                                    ValueFilter filter) const {
    return GetValue(makeItem(vb, keySlice, metaSlice, valueSlice, filter),
                    cb::engine_errc::success,
                    -1,
                    false);
}

int MagmaKVStore::saveDocs(TransactionContext& txnCtx, VB::Commit& commitData,
                           kvstats_ctx& kvctx) {
    uint64_t ninserts = 0;
    uint64_t ndeletes = 0;

    auto vbid = txnCtx.vbid;

    auto writeDocsCB = [this, &commitData, &kvctx, &ninserts, &ndeletes, vbid](
                               const Magma::WriteOperation& op,
                               const bool docExists,
                               const magma::Slice oldMeta) {
        auto req = reinterpret_cast<MagmaRequest*>(op.UserData);
        auto diskDocKey = makeDiskDocKey(op.Key);
        auto docKey = diskDocKey.getDocKey();

        const bool isTombstone =
                docExists && configuration.magmaCfg.IsTombstone(oldMeta);

        if (logger->should_log(spdlog::level::TRACE)) {
            logger->TRACE(
                    "MagmaKVStore::writeDocsCB {} key:{} seqno:{} delete:{} "
                    "docExists:{} tombstone:{}",
                    vbid,
                    cb::UserData{diskDocKey.to_string()},
                    req->getDocMeta().bySeqno,
                    req->isDelete(),
                    docExists,
                    isTombstone);
        }

        // We don't track collection stats for prepares for magma. The only
        // non-committed stat we count for collections is the on disk size (as
        // this includes prepares for couchstore Buckets). As we remove prepares
        // at compaction we can't currently track them or their on disk size
        // correctly as magma as we may visit stale values. The same follows for
        // the on disk size of collections. As we cannot track prepare size
        // correctly for magma we must avoid counting it at all.
        if (diskDocKey.isCommitted()) {
            auto isDeleted = req->isDelete() ? IsDeleted::Yes : IsDeleted::No;
            auto isCommitted = diskDocKey.isCommitted() ? IsCommitted::Yes
                                                        : IsCommitted::No;
            if (docExists) {
                auto oldIsDeleted =
                        isTombstone ? IsDeleted::Yes : IsDeleted::No;
                commitData.collections.updateStats(
                        docKey,
                        req->getDocMeta().bySeqno,
                        isCommitted,
                        isDeleted,
                        req->getBodySize(),
                        configuration.magmaCfg.GetSeqNum(oldMeta),
                        oldIsDeleted,
                        configuration.magmaCfg.GetValueSize(oldMeta),
                        WantsDropped::Yes);
            } else {
                commitData.collections.updateStats(docKey,
                                                   req->getDocMeta().bySeqno,
                                                   isCommitted,
                                                   isDeleted,
                                                   req->getBodySize(),
                                                   WantsDropped::Yes);
            }
        } else {
            // Tell Collections::Flush that it may need to record this seqno
            commitData.collections.maybeUpdatePersistedHighSeqno(
                    docKey, req->getDocMeta().bySeqno, req->isDelete());
        }

        if (docExists) {
            req->markOldItemExists();
            if (isTombstone) {
                req->markOldItemIsDelete();
                // Old item is a delete and new is an insert.
                if (!req->isDelete()) {
                    if (diskDocKey.isCommitted()) {
                        ninserts++;
                    } else {
                        kvctx.onDiskPrepareDelta++;
                    }
                }
            } else if (req->isDelete()) {
                // Old item is insert and new is delete.
                if (diskDocKey.isCommitted()) {
                    ndeletes++;
                } else {
                    kvctx.onDiskPrepareDelta--;
                }
            }
        } else {
            // Old item doesn't exist and new is an insert.
            if (!req->isDelete()) {
                if (diskDocKey.isCommitted()) {
                    ninserts++;
                } else {
                    kvctx.onDiskPrepareDelta++;
                }
            }
        }

        // @todo MB-42900: Add support for onDiskPrepareBytes
    };

    // LocalDbReqs and MagmaDbStats are used to store the memory for the localDb
    // and stat updates as WriteOps is non-owning.
    LocalDbReqs localDbReqs;
    MagmaDbStats magmaDbStats;
    int64_t lastSeqno = 0;

    auto beginTime = std::chrono::steady_clock::now();
    std::chrono::microseconds saveDocsDuration;

    auto postWriteDocsCB = [this,
                            &commitData,
                            &localDbReqs,
                            &lastSeqno,
                            &vbid,
                            &ninserts,
                            &ndeletes,
                            &magmaDbStats,
                            &beginTime,
                            &saveDocsDuration](WriteOps& postWriteOps) {
        // Merge in the delta changes
        {
            auto lockedStats = magmaDbStats.stats.wlock();
            lockedStats->docCount = ninserts - ndeletes;
        }
        addStatUpdateToWriteOps(magmaDbStats, postWriteOps);

        auto& vbstate = commitData.proposedVBState;
        vbstate.highSeqno = lastSeqno;
        // @todo: Magma doesn't track onDiskPrepares
        // @todo MB-42900: Magma doesn't track onDiskPrepareBytes

        // Write out current vbstate to the CommitBatch.
        addVBStateUpdateToLocalDbReqs(
                localDbReqs, vbstate, kvstoreRevList[getCacheSlot(vbid)]);

        commitData.collections.saveCollectionStats(
                [this, &localDbReqs](
                        CollectionID cid,
                        const Collections::VB::PersistedStats& stats) {
                    saveCollectionStats(localDbReqs, cid, stats);
                });

        if (commitData.collections.isReadyForCommit()) {
            auto status = updateCollectionsMeta(
                    vbid, localDbReqs, commitData.collections);
            if (!status.IsOK()) {
                logger->warn(
                        "MagmaKVStore::saveDocs {} Failed to set "
                        "collections meta, got status {}",
                        vbid,
                        status.String());
                return status;
            }
        }
        addLocalDbReqs(localDbReqs, postWriteOps);
        auto now = std::chrono::steady_clock::now();
        saveDocsDuration =
                std::chrono::duration_cast<std::chrono::microseconds>(
                        now - beginTime);
        beginTime = now;

        return Status::OK();
    };

    // Vector of updates to be written to the data store.
    WriteOps writeOps;
    writeOps.reserve(pendingReqs->size());

    // TODO: Replace writeOps with Magma::WriteOperations when it
    // becomes available. This will allow us to pass pendingReqs
    // in and create the WriteOperation from the pendingReqs queue.
    for (auto& req : *pendingReqs) {
        auto& docMeta = req.getDocMeta();
        Slice valSlice{req.getBodyData(), req.getBodySize()};
        if (req.getDocMeta().bySeqno > lastSeqno) {
            lastSeqno = req.getDocMeta().bySeqno;
        }

        switch (commitData.writeOp) {
        case WriteOperation::Insert:
            writeOps.emplace_back(Magma::WriteOperation::NewDocInsert(
                    {req.getRawKey(), req.getRawKeyLen()},
                    {reinterpret_cast<char*>(&docMeta),
                     sizeof(magmakv::MetaData)},
                    valSlice,
                    &req));
            break;
        case WriteOperation::Upsert:
            writeOps.emplace_back(Magma::WriteOperation::NewDocUpsert(
                    {req.getRawKey(), req.getRawKeyLen()},
                    {reinterpret_cast<char*>(&docMeta),
                     sizeof(magmakv::MetaData)},
                    valSlice,
                    &req));
            break;
        }
    }

    auto [getDroppedStatus, dropped] = getDroppedCollections(vbid);
    if (!getDroppedStatus) {
        logger->warn(
                "MagmaKVStore::saveDocs {} Failed to get dropped "
                "collections",
                vbid);
        // We have to return some non-success status, Invalid will do
        return Status::Invalid;
    }

    commitData.collections.setDroppedCollectionsForStore(dropped);

    auto status = magma->WriteDocs(vbid.get(),
                                   writeOps,
                                   kvstoreRevList[getCacheSlot(vbid)],
                                   writeDocsCB,
                                   postWriteDocsCB);
    if (status) {
        st.saveDocsHisto.add(saveDocsDuration);
        kvctx.commitData.collections.postCommitMakeStatsVisible();

        // Commit duration can be approximately calculated as the time taken
        // between post writedocs callback and now
        st.commitHisto.add(
                std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - beginTime));

        st.batchSize.add(pendingReqs->size());
        st.docsCommitted = pendingReqs->size();
    } else {
        logger->critical(
                "MagmaKVStore::saveDocs {} WriteDocs failed. Status:{}",
                vbid,
                status.String());
    }

    // Only used for unit testing
    if (doCheckpointEveryBatch) {
        auto chkStatus = magma->Sync(true);
        if (!chkStatus) {
            logger->critical(
                    "MagmaKVStore::saveDocs {} Unable to create checkpoint. "
                    "Status:{}",
                    vbid,
                    chkStatus.String());
        }
    }

    return status.ErrorCode();
}

MagmaScanContext::MagmaScanContext(
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
        std::unique_ptr<magma::Magma::SeqIterator> itr)
    : BySeqnoScanContext(std::move(cb),
                         std::move(cl),
                         vb,
                         std::move(handle),
                         start,
                         end,
                         purgeSeqno,
                         _docFilter,
                         _valFilter,
                         _documentCount,
                         vbucketState,
                         droppedCollections),
      itr(std::move(itr)) {
}

std::unique_ptr<BySeqnoScanContext> MagmaKVStore::initBySeqnoScanContext(
        std::unique_ptr<StatusCallback<GetValue>> cb,
        std::unique_ptr<StatusCallback<CacheLookup>> cl,
        Vbid vbid,
        uint64_t startSeqno,
        DocumentFilter options,
        ValueFilter valOptions,
        SnapshotSource source) const {
    if (source == SnapshotSource::Historical) {
        throw std::runtime_error(
                "MagmaKVStore::initBySeqnoScanContext: historicalSnapshot not "
                "implemented");
    }

    auto handle = makeFileHandle(vbid);

    std::unique_ptr<Magma::Snapshot> snapshot;

    // Flush writecache for creating an on-disk snapshot
    auto status = magma->SyncKVStore(vbid.get());
    if (!status.IsOK()) {
        logger->warn(
                "MagmaKVStore::initBySeqnoScanContext {} Failed to sync "
                "kvstore with status {}",
                vbid,
                status.String());
        return nullptr;
    }

    status = magma->GetDiskSnapshot(vbid.get(), snapshot);

    if (!status.IsOK()) {
        logger->warn(
                "MagmaKVStore::initBySeqnoScanContext {} Failed to get magma snapshot "
                "with status {}",
                vbid,
                status.String());
        return nullptr;
    }

    if (!snapshot.get()) {
        logger->error(
                "MagmaKVStore::initBySeqnoScanContext {} Failed to get magma snapshot,"
                " no pointer returned",
                vbid);
        return nullptr;
    }

    auto itr = magma->NewSeqIterator(*snapshot);
    if (!itr) {
        logger->warn(
                "MagmaKVStore::initBySeqnoScanContext {} Failed to get magma seq "
                "iterator",
                vbid);
        return nullptr;
    }

    auto readState = readVBStateFromDisk(vbid, *snapshot);
    if (!readState.status.IsOK()) {
        logger->warn(
                "MagmaKVStore::initBySeqnoScanContext {} failed to read "
                "vbstate from disk. Status:{}",
                vbid,
                readState.status.String());
        return nullptr;
    }

    uint64_t highSeqno = readState.vbstate.highSeqno;
    uint64_t purgeSeqno = readState.vbstate.purgeSeqno;
    uint64_t nDocsToRead = highSeqno - startSeqno + 1;

    auto [getDroppedStatus, dropped] = getDroppedCollections(vbid, *snapshot);
    if (!getDroppedStatus.OK()) {
        logger->warn(
                "MagmaKVStore::initBySeqnoScanContext {} failed to get "
                "dropped collections from disk. Status:{}",
                vbid,
                getDroppedStatus.String());
    }

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
                    "MagmaKVStore::initBySeqnoScanContext Unknown "
                    "DocumentFilter:" +
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
                    "MagmaKVStore::initBySeqnoScanContext Unknown "
                    "ValueFilter:" +
                    std::to_string(static_cast<int>(valOptions)));
        }

        logger->info(
                "MagmaKVStore::initBySeqnoScanContext {} seqno:{} endSeqno:{}"
                " purgeSeqno:{} nDocsToRead:{} docFilter:{} valFilter:{}",
                vbid,
                startSeqno,
                highSeqno,
                purgeSeqno,
                nDocsToRead,
                docFilter,
                valFilter);
    }

    auto mctx = std::make_unique<MagmaScanContext>(std::move(cb),
                                                   std::move(cl),
                                                   vbid,
                                                   std::move(handle),
                                                   startSeqno,
                                                   highSeqno,
                                                   purgeSeqno,
                                                   options,
                                                   valOptions,
                                                   nDocsToRead,
                                                   readState.vbstate,
                                                   dropped,
                                                   std::move(itr));
    return mctx;
}

std::unique_ptr<ByIdScanContext> MagmaKVStore::initByIdScanContext(
        std::unique_ptr<StatusCallback<GetValue>> cb,
        std::unique_ptr<StatusCallback<CacheLookup>> cl,
        Vbid vbid,
        const std::vector<ByIdRange>& ranges,
        DocumentFilter options,
        ValueFilter valOptions) const {
    throw std::runtime_error(
            "MagmaKVStore::initByIdScanContext (id scan) unimplemented");
    return {};
}

scan_error_t MagmaKVStore::scan(BySeqnoScanContext& ctx) const {
    if (ctx.lastReadSeqno == ctx.maxSeqno) {
        logger->TRACE("MagmaKVStore::scan {} lastReadSeqno:{} == maxSeqno:{}",
                      ctx.vbid,
                      ctx.lastReadSeqno,
                      ctx.maxSeqno);
        return scan_success;
    }

    auto startSeqno = ctx.startSeqno;
    if (ctx.lastReadSeqno != 0) {
        startSeqno = ctx.lastReadSeqno + 1;
    }

    bool onlyKeys = ctx.valFilter == ValueFilter::KEYS_ONLY;

    auto& mctx = dynamic_cast<MagmaScanContext&>(ctx);
    for (mctx.itr->Seek(startSeqno, ctx.maxSeqno); mctx.itr->Valid();
         mctx.itr->Next()) {
        Slice keySlice, metaSlice, valSlice;
        uint64_t seqno;
        mctx.itr->GetRecord(keySlice, metaSlice, valSlice, seqno);

        if (keySlice.Len() > std::numeric_limits<uint16_t>::max()) {
            throw std::invalid_argument(
                    "MagmaKVStore::scan: "
                    "key length " +
                    std::to_string(keySlice.Len()) + " > " +
                    std::to_string(std::numeric_limits<uint16_t>::max()));
        }

        auto diskKey = makeDiskDocKey(keySlice);

        if (magmakv::isDeleted(metaSlice) &&
            ctx.docFilter == DocumentFilter::NO_DELETES) {
            if (logger->should_log(spdlog::level::TRACE)) {
                logger->TRACE(
                        "MagmaKVStore::scan SKIPPED(Deleted) {} key:{} "
                        "seqno:{}",
                        ctx.vbid,
                        cb::UserData{diskKey.to_string()},
                        seqno);
            }
            continue;
        }

        auto docKey = diskKey.getDocKey();

        // Determine if the key is logically deleted, if it is we skip the key
        // Note that system event keys (like create scope) are never skipped
        // here
        if (!docKey.isInSystemCollection()) {
            if (ctx.docFilter !=
                DocumentFilter::ALL_ITEMS_AND_DROPPED_COLLECTIONS) {
                if (ctx.collectionsContext.isLogicallyDeleted(docKey, seqno)) {
                    ctx.lastReadSeqno = seqno;
                    if (logger->should_log(spdlog::level::TRACE)) {
                        logger->TRACE(
                                "MagmaKVStore::scan SKIPPED(Collection "
                                "Deleted) {} "
                                "key:{} "
                                "seqno:{}",
                                ctx.vbid,
                                cb::UserData{diskKey.to_string()},
                                seqno);
                    }
                    continue;
                }
            }

            CacheLookup lookup(diskKey, seqno, ctx.vbid);

            ctx.lookup->callback(lookup);
            if (ctx.lookup->getStatus() ==
                static_cast<int>(cb::engine_errc::key_already_exists)) {
                ctx.lastReadSeqno = seqno;
                if (logger->should_log(spdlog::level::TRACE)) {
                    logger->TRACE(
                            "MagmaKVStore::scan "
                            "SKIPPED(cb::engine_errc::key_already_exists) {} "
                            "key:{} seqno:{}",
                            ctx.vbid,
                            cb::UserData{diskKey.to_string()},
                            seqno);
                }
                continue;
            } else if (ctx.lookup->getStatus() ==
                       static_cast<int>(cb::engine_errc::no_memory)) {
                logger->warn(
                        "MagmaKVStore::scan lookup->callback {} "
                        "key:{} returned cb::engine_errc::no_memory",
                        ctx.vbid,
                        cb::UserData{diskKey.to_string()});
                return scan_again;
            }
        }

        if (logger->should_log(spdlog::level::TRACE)) {
            logger->TRACE(
                    "MagmaKVStore::scan {} key:{} seqno:{} deleted:{} "
                    "expiry:{} "
                    "compressed:{}",
                    ctx.vbid,
                    cb::UserData{diskKey.to_string()},
                    seqno,
                    magmakv::isDeleted(metaSlice),
                    magmakv::getExpiryTime(metaSlice),
                    magmakv::isCompressed(metaSlice));
        }

        auto itm = makeItem(
                ctx.vbid, keySlice, metaSlice, valSlice, ctx.valFilter);

        // When we are requested to return the values as compressed AND
        // the value isn't compressed, attempt to compress the value.
        if (ctx.valFilter == ValueFilter::VALUES_COMPRESSED &&
            !magmakv::isCompressed(metaSlice)) {
            if (!itm->compressValue()) {
                logger->warn(
                        "MagmaKVStore::scan failed to compress value - {} "
                        "key:{} "
                        "seqno:{}",
                        ctx.vbid,
                        cb::UserData{diskKey.to_string()},
                        seqno);
                continue;
            }
        }

        GetValue rv(std::move(itm), cb::engine_errc::success, -1, onlyKeys);
        ctx.callback->callback(rv);
        auto callbackStatus = ctx.callback->getStatus();
        if (callbackStatus == static_cast<int>(cb::engine_errc::no_memory)) {
            if (logger->should_log(spdlog::level::TRACE)) {
                logger->TRACE(
                        "MagmaKVStore::scan callback {} "
                        "key:{} returned cb::engine_errc::no_memory",
                        ctx.vbid,
                        cb::UserData{diskKey.to_string()});
            }
            return scan_again;
        }
        ctx.lastReadSeqno = seqno;
    }

    return scan_success;
}

scan_error_t MagmaKVStore::scan(ByIdScanContext& ctx) const {
    throw std::runtime_error("MagmaKVStore::scan (by id scan) unimplemented");
    return scan_failed;
}

void MagmaKVStore::mergeMagmaDbStatsIntoVBState(vbucket_state& vbstate,
                                                Vbid vbid) const {
    auto dbStats = getMagmaDbStats(vbid);
    if (!dbStats) {
        // No stats available from Magma for this vbid, nothing to do.
        return;
    }
    auto lockedStats = dbStats->stats.rlock();
    vbstate.purgeSeqno = lockedStats->purgeSeqno;

    // We don't update the number of onDiskPrepares because we can't easily
    // track the number for magma
}

vbucket_state* MagmaKVStore::getCachedVBucketState(Vbid vbid) {
    auto& vbstate = cachedVBStates[getCacheSlot(vbid)];
    if (vbstate) {
        mergeMagmaDbStatsIntoVBState(*vbstate, vbid);
        if (logger->should_log(spdlog::level::TRACE)) {
            logger->TRACE("MagmaKVStore::getCachedVBucketState {} vbstate:{}",
                          vbid,
                          encodeVBState(*vbstate));
        }
    }
    return vbstate.get();
}

vbucket_state MagmaKVStore::getPersistedVBucketState(Vbid vbid) {
    auto state = readVBStateFromDisk(vbid);
    if (!state.status.IsOK()) {
        throw std::runtime_error(
                "MagmaKVStore::getPersistedVBucketState "
                "failed with status " +
                state.status.String());
    }

    return state.vbstate;
}

uint64_t MagmaKVStore::getKVStoreRevision(Vbid vbid) const {
    uint64_t kvstoreRev{0};
    auto [status, kvsRev] = magma->GetKVStoreRevision(vbid.get());
    if (status) {
        kvstoreRev = static_cast<uint64_t>(kvsRev);
    }

    return kvstoreRev;
}

MagmaKVStore::DiskState MagmaKVStore::readVBStateFromDisk(Vbid vbid) const {
    Slice keySlice(vbstateKey);
    auto kvstoreRev = getKVStoreRevision(vbid);
    auto [status, valString] = readLocalDoc(vbid, keySlice);

    if (!status.IsOK()) {
        return {status, {}, kvstoreRev};
    }

    nlohmann::json j;

    try {
        j = nlohmann::json::parse(valString);
    } catch (const nlohmann::json::exception& e) {
        return {Status("MagmaKVStore::readVBStateFromDisk failed - vbucket( " +
                       std::to_string(vbid.get()) + ") " +
                       " Failed to parse the vbstate json doc: " + valString +
                       ". Reason: " + e.what()),
                {},
                kvstoreRev};
    }

    logger->TRACE("MagmaKVStore::readVBStateFromDisk {} vbstate:{}", vbid, j);

    vbucket_state vbstate = j;
    mergeMagmaDbStatsIntoVBState(vbstate, vbid);

    return {status, vbstate, kvstoreRev};
}

MagmaKVStore::DiskState MagmaKVStore::readVBStateFromDisk(
        Vbid vbid, magma::Magma::Snapshot& snapshot) const {
    Slice keySlice(vbstateKey);
    std::string val;
    auto status = Status::OK();
    auto kvstoreRev = getKVStoreRevision(vbid);

    std::tie(status, val) = readLocalDoc(vbid, snapshot, keySlice);

    if (!status.IsOK()) {
        return {status, {}, kvstoreRev};
    }

    nlohmann::json j;

    try {
        j = nlohmann::json::parse(val);
    } catch (const nlohmann::json::exception& e) {
        return {Status("MagmaKVStore::readVBStateFromDisk failed - " +
                       vbid.to_string() + " failed to parse the vbstate json " +
                       "doc: " + val + ". Reason: " + e.what()),
                {},
                kvstoreRev};
    }

    vbucket_state vbstate = j;

    auto userStats = magma->GetKVStoreUserStats(snapshot);
    if (!userStats) {
        return {Status("MagmaKVStore::readVBStateFromDisk failed - " +
                       vbid.to_string() + " magma didn't return UserStats"),
                {},
                kvstoreRev};
    }

    auto* magmaUserStats = dynamic_cast<MagmaDbStats*>(userStats.get());
    if (!magmaUserStats) {
        throw std::runtime_error("MagmaKVStore::readVBStateFromDisk error - " +
                                 vbid.to_string() + " magma returned invalid " +
                                 "type of UserStats");
    }
    auto lockedStats = magmaUserStats->stats.rlock();
    vbstate.purgeSeqno = lockedStats->purgeSeqno;

    return {status, vbstate, kvstoreRev};
}

magma::Status MagmaKVStore::loadVBStateCache(Vbid vbid, bool resetKVStoreRev) {
    const auto readState = readVBStateFromDisk(vbid);

    // If the vBucket exists but does not have a vbucket_state (i.e. NotFound
    // is returned from readVBStateFromDisk) then just use a defaulted
    // vbucket_state (which defaults to the dead state).
    if (!readState.status.IsOK() &&
        readState.status.ErrorCode() != magma::Status::NotFound) {
        logger->error("MagmaKVStore::loadVBStateCache {} failed. {}",
                      vbid,
                      readState.status.String());
        return readState.status;
    }

    cachedVBStates[getCacheSlot(vbid)] =
            std::make_unique<vbucket_state>(readState.vbstate);

    // We only want to reset the kvstoreRev when loading up the
    // vbstate cache during magma instantiation.
    if (resetKVStoreRev) {
        kvstoreRevList[getCacheSlot(vbid)].reset(readState.kvstoreRev);
    }

    return Status::OK();
}

void MagmaKVStore::addVBStateUpdateToLocalDbReqs(LocalDbReqs& localDbReqs,
                                                 const vbucket_state& vbs,
                                                 uint64_t kvstoreRev) {
    std::string vbstateString = encodeVBState(vbs);
    logger->TRACE("MagmaKVStore::addVBStateUpdateToLocalDbReqs vbstate:{}",
                  vbstateString);
    localDbReqs.emplace_back(
            MagmaLocalReq(vbstateKey, std::move(vbstateString)));
}

std::pair<Status, std::string> MagmaKVStore::processReadLocalDocResult(
        Status status,
        Vbid vbid,
        const magma::Slice& keySlice,
        std::string_view value,
        bool found) const {
    magma::Status retStatus = Status::OK();
    if (!status) {
        retStatus = magma::Status(
                status.ErrorCode(),
                "MagmaKVStore::readLocalDoc " + vbid.to_string() +
                        " key:" + keySlice.ToString() + " " + status.String());
        logger->warn("MagmaKVStore::readLocalDoc {} key:{} returned status: {}",
                     vbid,
                     keySlice.ToString(),
                     status.String());
    } else {
        if (!found) {
            retStatus = magma::Status(
                    magma::Status::Code::NotFound,
                    "MagmaKVStore::readLocalDoc " + vbid.to_string() +
                            " key:" + keySlice.ToString() + " not found.");
        } else if (logger->should_log(spdlog::level::TRACE)) {
            logger->TRACE("MagmaKVStore::readLocalDoc {} key:{} valueLen:{}",
                          vbid,
                          keySlice.ToString(),
                          value.length());
        }
    }
    return std::make_pair(retStatus, std::string(value));
}

std::pair<Status, std::string> MagmaKVStore::readLocalDoc(
        Vbid vbid, const Slice& keySlice) const {
    bool found{false};
    magma::Status retStatus = Status::OK();
    std::string valString;

    auto status = magma->GetLocal(vbid.get(), keySlice, valString, found);
    return processReadLocalDocResult(status, vbid, keySlice, valString, found);
}

std::pair<Status, std::string> MagmaKVStore::readLocalDoc(
        Vbid vbid,
        magma::Magma::Snapshot& snapshot,
        const Slice& keySlice) const {
    bool found{false};
    magma::Status retStatus = Status::OK();
    std::string valString;

    auto status = magma->GetLocal(snapshot, keySlice, valString, found);
    return processReadLocalDocResult(status, vbid, keySlice, valString, found);
}

std::string MagmaKVStore::encodeVBState(const vbucket_state& vbstate) const {
    nlohmann::json j = vbstate;
    return j.dump();
}

cb::engine_errc MagmaKVStore::magmaErr2EngineErr(Status::Code err, bool found) {
    if (!found) {
        return cb::engine_errc::no_such_key;
    }
    // This routine is intended to mimic couchErr2EngineErr.
    // Since magma doesn't have a memory allocation error, all magma errors
    // get translated into cb::engine_errc::temporary_failure.
    if (err == Status::Code::Ok) {
        return cb::engine_errc::success;
    }
    return cb::engine_errc::temporary_failure;
}

std::optional<MagmaDbStats> MagmaKVStore::getMagmaDbStats(Vbid vbid) const {
    auto userStats = magma->GetKVStoreUserStats(vbid.get());
    if (!userStats) {
        return {};
    }
    auto* otherStats = dynamic_cast<MagmaDbStats*>(userStats.get());
    if (!otherStats) {
        throw std::runtime_error(
                "MagmaKVStore::getMagmaDbStats: stats retrieved from magma "
                "are incorrect type");
    }
    if (logger->should_log(spdlog::level::TRACE)) {
        logger->TRACE("MagmaKVStore::getMagmaDbStats {} dbStats:{}",
                      vbid,
                      otherStats->Marshal());
    }
    return *otherStats;
}

size_t MagmaKVStore::getItemCount(Vbid vbid) {
    auto dbStats = getMagmaDbStats(vbid);
    if (!dbStats) {
        throw std::system_error(
                std::make_error_code(std::errc::no_such_file_or_directory),
                fmt::format("MagmaKVStore::getMagmaDbStats: failed to open "
                            "database file for {}",
                            vbid));
    }
    auto lockedStats = dbStats->stats.rlock();
    return lockedStats->docCount;
}

cb::engine_errc MagmaKVStore::getAllKeys(
        Vbid vbid,
        const DiskDocKey& startKey,
        uint32_t count,
        std::shared_ptr<StatusCallback<const DiskDocKey&>> cb) const {
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
                    magmakv::getSeqNum(metaSlice),
                    magmakv::isDeleted(metaSlice));
        }

        if (magmakv::isDeleted(metaSlice)) {
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

    return cb::engine_errc::success;
}

bool MagmaKVStore::compactDB(std::unique_lock<std::mutex>& vbLock,
                             std::shared_ptr<CompactionContext> ctx) {
    vbLock.unlock();
    auto res = compactDBInternal(vbLock, std::move(ctx));

    if (!res) {
        st.numCompactionFailure++;
    }

    return res;
}

bool MagmaKVStore::compactDBInternal(std::unique_lock<std::mutex>& vbLock,
                                     std::shared_ptr<CompactionContext> ctx) {
    std::chrono::steady_clock::time_point start =
            std::chrono::steady_clock::now();

    logger->info(
            "MagmaKVStore::compactDBInternal: {} purge_before_ts:{} "
            "purge_before_seq:{} drop_deletes:{} "
            "retain_erroneous_tombstones:{}",
            ctx->vbid,
            ctx->compactConfig.purge_before_ts,
            ctx->compactConfig.purge_before_seq,
            ctx->compactConfig.drop_deletes,
            ctx->compactConfig.retain_erroneous_tombstones);

    Vbid vbid = ctx->vbid;

    auto [getDroppedStatus, dropped] = getDroppedCollections(vbid);
    if (!getDroppedStatus) {
        logger->warn(
                "MagmaKVStore::compactDBInternal: {} Failed to get "
                "dropped collections",
                vbid);
        return false;
    }

    ctx->eraserContext =
            std::make_unique<Collections::VB::EraserContext>(dropped);

    auto diskState = readVBStateFromDisk(vbid);
    if (!diskState.status) {
        logger->warn(
                "MagmaKVStore::compactDBInternal: trying to run "
                "compaction on {} but can't read vbstate. Status:{}",
                vbid,
                diskState.status.String());
        return false;
    }
    ctx->highCompletedSeqno = diskState.vbstate.persistedCompletedSeqno;

    auto compactionCB = [this, ctx]() {
        return std::make_unique<MagmaKVStore::MagmaCompactionCB>(*this, ctx);
    };

    uint64_t collectionItemsDropped = 0;
    LocalDbReqs localDbReqs;

    Status status;
    if (dropped.empty()) {
        // Compact the entire key range
        Slice nullKey;
        status = magma->CompactKVStore(
                vbid.get(), nullKey, nullKey, compactionCB);
        if (!status) {
            logger->warn(
                    "MagmaKVStore::compactDBInternal CompactKVStore failed. "
                    "{} ",
                    vbid);
            return false;
        }
    } else {
        std::vector<std::pair<Collections::KVStore::DroppedCollection,
                              Collections::VB::PersistedStats>>
                dcInfo;
        for (auto& dc : dropped) {
            auto handle = makeFileHandle(vbid);
            auto [success, stats] =
                    getDroppedCollectionStats(vbid, dc.collectionId);
            if (!success) {
                logger->warn(
                        "MagmaKVStore::compactDBInternal getCollectionStats() "
                        "failed for dropped collection "
                        "cid:{} {}",
                        dc.collectionId,
                        vbid);
                return false;
            }
            dcInfo.emplace_back(dc, stats);
        }

        // For magma we also need to compact the prepare namespace as this is
        // disjoint from the collection namespaces.
        cb::mcbp::unsigned_leb128<CollectionIDType> leb128(CollectionID::DurabilityPrepare);
        Slice prepareSlice(reinterpret_cast<const char*>(leb128.data()),
                           leb128.size());
        status = magma->CompactKVStore(
                vbid.get(), prepareSlice, prepareSlice, compactionCB);
        if (!status) {
            logger->warn(
                    "MagmaKVStore::compactDBInternal CompactKVStore {} "
                    "over the prepare namespace failed with status:{}",
                    vbid,
                    status.String());
        }

        for (auto& [dc, stats] : dcInfo) {
            std::string keyString =
                    Collections::makeCollectionIdIntoString(dc.collectionId);
            Slice keySlice{keyString};

            if (logger->should_log(spdlog::level::TRACE)) {
                auto docKey = makeDiskDocKey(keySlice);
                logger->TRACE(
                        "MagmaKVStore::compactDBInternal CompactKVStore {} "
                        "key:{}",
                        vbid,
                        cb::UserData{docKey.to_string()});
            }

            status = magma->CompactKVStore(
                    vbid.get(), keySlice, keySlice, compactionCB);
            if (!status) {
                logger->warn(
                        "MagmaKVStore::compactDBInternal CompactKVStore {} "
                        "CID:{} failed "
                        "status:{}",
                        vbid,
                        cb::UserData{makeDiskDocKey(keySlice).to_string()},
                        status.String());
                continue;
            }
            // Can't track number of collection items purged properly in the
            // compaction callback for magma as it may be called multiple
            // times per key. We CAN just subtract the number of items we know
            // belong to the collection though before we update the vBucket doc
            // count.
            collectionItemsDropped += stats.itemCount;

            // We've finish processing this collection.
            // Create a SystemEvent key for the collection and process it.
            auto collectionKey =
                    SystemEventFactory::makeCollectionEventKey(dc.collectionId);

            keySlice = {reinterpret_cast<const char*>(collectionKey.data()),
                        collectionKey.size()};

            auto key = makeDiskDocKey(keySlice);
            if (logger->should_log(spdlog::level::TRACE)) {
                logger->TRACE(
                        "MagmaKVStore::compactDBInternal: "
                        "processEndOfCollection {} key:{}",
                        vbid,
                        cb::UserData{key.to_string()});
            }

            // Drop the collection stats local doc which we kept around until
            // now to maintain the document count when we erase the collections.
            // But only after checking the ordering (seqno). Stats are only
            // removed if there is no recreation of the collection.
            if (dc.endSeqno > stats.highSeqno) {
                deleteDroppedCollectionStats(localDbReqs, dc.collectionId);
            }
            ctx->eraserContext->processSystemEvent(key.getDocKey(),
                                                   SystemEvent::Collection);
        }
    }

    // Make our completion callback before writing the new file. We should
    // update our in memory state before we finalize on disk state so that we
    // don't have to worry about race conditions with things like the purge
    // seqno.
    if (ctx->completionCallback) {
        ctx->stats.collectionsItemsPurged = collectionItemsDropped;
        ctx->completionCallback(*ctx);
    }

    if (ctx->eraserContext->needToUpdateCollectionsMetadata()) {
        // Need to write back some collections metadata, in particular we need
        // to remove from the dropped collections doc all of the collections
        // that we have just purged. A collection drop might have happened after
        // we started this compaction though and we may not have dropped it yet
        // so we can't just delete the doc. To avoid a flusher coming along and
        // dropping another collection while we do this we need to hold the
        // vBucket write lock.
        vbLock.lock();

        // 1) Get the current state from disk
        auto [getDroppedStatus, droppedCollections] =
                getDroppedCollections(vbid);
        if (!getDroppedStatus) {
            throw std::runtime_error(
                    fmt::format("MagmaKVStore::compactDbInternal {} failed "
                                "getDroppedCollections",
                                vbid));
        }

        // 2) Generate a new flatbuffer document to write back
        auto fbData = Collections::VB::Flush::
                encodeRelativeComplementOfDroppedCollections(
                        droppedCollections,
                        ctx->eraserContext->getDroppedCollections());

        // 3) If the function returned data, write it, else the document is
        // delete.
        WriteOps writeOps;
        if (fbData.data()) {
            localDbReqs.emplace_back(
                    MagmaLocalReq(droppedCollectionsKey, fbData));
        } else {
            // Need to ensure the 'dropped' list on disk is now gone
            localDbReqs.emplace_back(
                    MagmaLocalReq::makeDeleted(droppedCollectionsKey));
        }

        addLocalDbReqs(localDbReqs, writeOps);
        MagmaDbStats magmaDbStats;

        { // locking scope for magmaDbStats
            auto stats = magmaDbStats.stats.wlock();
            stats->docCount -= collectionItemsDropped;
        }

        addStatUpdateToWriteOps(magmaDbStats, writeOps);

        status = magma->WriteDocs(
                vbid.get(), writeOps, kvstoreRevList[getCacheSlot(vbid)]);
        if (!status) {
            logger->critical(
                    "MagmaKVStore::compactDBInternal {} WriteDocs failed. "
                    "Status:{}",
                    vbid,
                    status.String());
            return false;
        }

        if (doCheckpointEveryBatch) {
            status = magma->Sync(true);
            if (!status) {
                logger->critical(
                        "MagmaKVStore::compactDBInternal {} Sync "
                        "failed. "
                        "Status:{}",
                        vbid,
                        status.String());
                return false;
            }
        }

        st.compactHisto.add(
                std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - start));
        logger->TRACE(
                "MagmaKVStore::compactDBInternal: max_purged_seq:{}"
                " collectionsItemsPurged:{}"
                " collectionsDeletedItemsPurged:{}"
                " tombstonesPurged:{}"
                " preparesPurged:{}",
                ctx->max_purged_seq,
                ctx->stats.collectionsItemsPurged,
                ctx->stats.collectionsDeletedItemsPurged,
                ctx->stats.tombstonesPurged,
                ctx->stats.preparesPurged);
    }
    return true;
}

std::unique_ptr<KVFileHandle> MagmaKVStore::makeFileHandle(Vbid vbid) const {
    return std::make_unique<MagmaKVFileHandle>(vbid);
}

RollbackResult MagmaKVStore::rollback(Vbid vbid,
                                      uint64_t rollbackSeqno,
                                      std::unique_ptr<RollbackCB> callback) {
    logger->TRACE("MagmaKVStore::rollback {} seqno:{}", vbid, rollbackSeqno);

    auto cacheLookup = std::make_shared<NoLookupCallback>();
    callback->setKVFileHandle(makeFileHandle(vbid));

    auto keyCallback =
            [this, cb = callback.get(), vbid](const Slice& keySlice,
                                              const uint64_t seqno,
                                              const Slice& metaSlice) {
                auto diskKey = makeDiskDocKey(keySlice);
                auto docKey = diskKey.getDocKey();
                if (docKey.isInSystemCollection()) {
                    return;
                }

                if (logger->should_log(spdlog::level::TRACE)) {
                    logger->TRACE(
                            "MagmaKVStore::rollback callback key:{} seqno:{}",
                            cb::UserData{diskKey.to_string()},
                            seqno);
                }

                auto rv = this->makeGetValue(vbid,
                                             keySlice,
                                             metaSlice,
                                             Slice(),
                                             ValueFilter::KEYS_ONLY);
                cb->callback(rv);
            };

    auto status = magma->Rollback(vbid.get(), rollbackSeqno, keyCallback);
    switch (status.ErrorCode()) {
    case Status::Ok:
        break;
    case Status::Internal:
    case Status::Invalid:
    case Status::Corruption:
    case Status::IOError:
    case Status::Compress:
    case Status::Exists:
    case Status::NotExists:
    case Status::ReadOnly:
    case Status::TransientIO:
    case Status::Busy:
        logger->critical("MagmaKVStore::rollback Rollback {} status:{}",
                         vbid,
                         status.String());
        // Fall through
    case Status::NotFound:
        // magma->Rollback returns non-success in the case where we have no
        // rollback points (and should reset the vBucket to 0). This isn't a
        // critical error so don't log it
        return RollbackResult(false);
    }

    // Did a rollback, so need to reload the vbstate cache.
    status = loadVBStateCache(vbid);
    if (!status.IsOK()) {
        logger->error("MagmaKVStore::rollback {} readVBStateFromDisk status:{}",
                      vbid,
                      status.String());
        return RollbackResult(false);
    }

    auto vbstate = getCachedVBucketState(vbid);
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

std::pair<bool, Collections::KVStore::Manifest>
MagmaKVStore::getCollectionsManifest(Vbid vbid) const {
    Status status;

    std::string manifest;
    std::tie(status, manifest) = readLocalDoc(vbid, manifestKey);

    std::string openCollections;
    std::tie(status, openCollections) = readLocalDoc(vbid, openCollectionsKey);

    std::string openScopes;
    std::tie(status, openScopes) = readLocalDoc(vbid, openScopesKey);

    std::string droppedCollections;
    std::tie(status, droppedCollections) =
            readLocalDoc(vbid, droppedCollectionsKey);
    return {true,
            Collections::KVStore::decodeManifest(
                    {reinterpret_cast<const uint8_t*>(manifest.data()),
                     manifest.length()},
                    {reinterpret_cast<const uint8_t*>(openCollections.data()),
                     openCollections.length()},
                    {reinterpret_cast<const uint8_t*>(openScopes.data()),
                     openScopes.length()},
                    {reinterpret_cast<const uint8_t*>(
                             droppedCollections.data()),
                     droppedCollections.length()})};
}

std::pair<bool, std::vector<Collections::KVStore::DroppedCollection>>
MagmaKVStore::getDroppedCollections(Vbid vbid) {
    Slice keySlice(droppedCollectionsKey);
    auto [status, dropped] = readLocalDoc(vbid, keySlice);
    // Currently we need the NotExists check for the case in which we create the
    // (magma)KVStore (done at first flush).
    // @TODO investigate removal/use of snapshot variant
    if (!status.IsOK() && status.ErrorCode() != Status::Code::NotExists &&
        status.ErrorCode() != Status::Code::NotFound) {
        return {false, {}};
    }

    return {true,
            Collections::KVStore::decodeDroppedCollections(
                    {reinterpret_cast<const uint8_t*>(dropped.data()),
                     dropped.length()})};
}

std::pair<magma::Status, std::vector<Collections::KVStore::DroppedCollection>>
MagmaKVStore::getDroppedCollections(Vbid vbid,
                                    magma::Magma::Snapshot& snapshot) const {
    Slice keySlice(droppedCollectionsKey);
    auto [status, dropped] = readLocalDoc(vbid, snapshot, keySlice);
    return {status,
            Collections::KVStore::decodeDroppedCollections(
                    {reinterpret_cast<const uint8_t*>(dropped.data()),
                     dropped.length()})};
}

magma::Status MagmaKVStore::updateCollectionsMeta(
        Vbid vbid,
        LocalDbReqs& localDbReqs,
        Collections::VB::Flush& collectionsFlush) {
    updateManifestUid(localDbReqs, collectionsFlush);

    if (collectionsFlush.isOpenCollectionsChanged()) {
        auto status =
                updateOpenCollections(vbid, localDbReqs, collectionsFlush);
        if (!status.IsOK()) {
            return status;
        }
    }

    if (collectionsFlush.isDroppedCollectionsChanged() ||
        collectionsFlush.isDroppedStatsChanged()) {
        auto status =
                updateDroppedCollections(vbid, localDbReqs, collectionsFlush);
        if (!status.IsOK()) {
            return status;
        }
    }

    if (collectionsFlush.isScopesChanged()) {
        auto status = updateScopes(vbid, localDbReqs, collectionsFlush);
        if (!status.IsOK()) {
            return status;
        }
    }

    return Status::OK();
}

void MagmaKVStore::updateManifestUid(LocalDbReqs& localDbReqs,
                                     Collections::VB::Flush& collectionsFlush) {
    auto buf = collectionsFlush.encodeManifestUid();
    localDbReqs.emplace_back(MagmaLocalReq(manifestKey, buf));
}

magma::Status MagmaKVStore::updateOpenCollections(
        Vbid vbid,
        LocalDbReqs& localDbReqs,
        Collections::VB::Flush& collectionsFlush) {
    Slice keySlice(openCollectionsKey);
    auto [status, collections] = readLocalDoc(vbid, keySlice);

    if (status.IsOK() || status.ErrorCode() == Status::Code::NotFound) {
        auto buf = collectionsFlush.encodeOpenCollections(
                {reinterpret_cast<const uint8_t*>(collections.data()),
                 collections.length()});

        localDbReqs.emplace_back(MagmaLocalReq(openCollectionsKey, buf));
        return Status::OK();
    }

    return status;
}

magma::Status MagmaKVStore::updateDroppedCollections(
        Vbid vbid,
        LocalDbReqs& localDbReqs,
        Collections::VB::Flush& collectionsFlush) {
    // For couchstore we would drop the collections stats local doc here but
    // magma can visit old keys (during the collection erasure compaction) and
    // we can't work out if they are the latest or not. To track the document
    // count correctly we need to keep the stats doc around until the collection
    // erasure compaction runs which will then delete the doc when it has
    // processed the collection.
    auto [status, dropped] = getDroppedCollections(vbid);
    if (!status) {
        return Status(Status::Code::Invalid,
                      "Failed to get dropped collections");
    }

    // We may not have dropped a collection but we may have a dropped collection
    // stat that needs updating if we have changed the state of a document in
    // a new generation of the collection, as such, we need to update stats
    // for both dropped collections and changes in state that result in stat
    // changes
    std::unordered_set<CollectionID> droppedStats;
    auto flushDroppedStats = collectionsFlush.getDroppedStats();
    for (auto [cid, stats] : collectionsFlush.getDroppedStats()) {
        droppedStats.insert(cid);
    }

    auto flushDroppedCollections = collectionsFlush.getDroppedCollections();
    for (auto [cid, droppedCollection] : flushDroppedCollections) {
        droppedStats.insert(cid);
    }

    for (auto cid : droppedStats) {
        auto droppedInBatch = flushDroppedCollections.find(cid) !=
                              flushDroppedCollections.end();

        // Step 1) Read the dropped stats - they may already exist and we should
        // add to their values if they do.
        auto [droppedStatus, droppedStats] =
                getDroppedCollectionStats(vbid, cid);

        // Step 2) Read the current alive stats on disk, we'll add them to
        // the dropped stats as the collection is now gone, provided we dropped
        // the collection in this flush batch. If we had just adjusted stats due
        // to a change in doc state then we'd have already tracked this in the
        // original drop
        if (droppedInBatch) {
            auto [status, currentAliveStats] = getCollectionStats(vbid, cid);
            if (status) {
                droppedStats.itemCount += currentAliveStats.itemCount;
                droppedStats.diskSize += currentAliveStats.diskSize;
            }
        }

        // Step 3) Add the dropped stats from FlushAccounting
        auto droppedInFlush = collectionsFlush.getDroppedStats(cid);
        droppedStats.itemCount += droppedInFlush.getItemCount();
        droppedStats.diskSize += droppedInFlush.getDiskSize();

        // Step 4) Write the new dropped stats back for compaction
        auto key = getDroppedCollectionsStatsKey(cid);
        auto encodedStats = droppedStats.getLebEncodedStats();
        localDbReqs.emplace_back(MagmaLocalReq(key, std::move(encodedStats)));

        // Step 5) Delete the latest alive stats if the collection wasn't
        // resurrected, we don't need them anymore
        if (!collectionsFlush.isOpen(cid)) {
            deleteCollectionStats(localDbReqs, cid);
        }
    }

    auto buf = collectionsFlush.encodeDroppedCollections(dropped);
    localDbReqs.emplace_back(MagmaLocalReq(droppedCollectionsKey, buf));

    return Status::OK();
}

std::string MagmaKVStore::getCollectionsStatsKey(CollectionID cid) const {
    return std::string{"|" + cid.to_string() + "|"};
}

std::string MagmaKVStore::getDroppedCollectionsStatsKey(CollectionID cid) {
    return std::string{"|" + cid.to_string() + "|.deleted"};
}

void MagmaKVStore::saveCollectionStats(
        LocalDbReqs& localDbReqs,
        CollectionID cid,
        const Collections::VB::PersistedStats& stats) {
    auto key = getCollectionsStatsKey(cid);
    auto encodedStats = stats.getLebEncodedStats();
    localDbReqs.emplace_back(MagmaLocalReq(key, std::move(encodedStats)));
}

std::pair<bool, Collections::VB::PersistedStats>
MagmaKVStore::getDroppedCollectionStats(Vbid vbid, CollectionID cid) {
    auto key = getDroppedCollectionsStatsKey(cid);
    return getCollectionStats(vbid, key);
}

std::pair<bool, Collections::VB::PersistedStats>
MagmaKVStore::getCollectionStats(const KVFileHandle& kvFileHandle,
                                 CollectionID cid) const {
    const auto& kvfh = static_cast<const MagmaKVFileHandle&>(kvFileHandle);
    auto vbid = kvfh.vbid;
    return getCollectionStats(vbid, cid);
}

std::pair<bool, Collections::VB::PersistedStats>
MagmaKVStore::getCollectionStats(Vbid vbid, CollectionID cid) const {
    auto key = getCollectionsStatsKey(cid);
    return getCollectionStats(vbid, key);
}

std::pair<bool, Collections::VB::PersistedStats>
MagmaKVStore::getCollectionStats(Vbid vbid, magma::Slice keySlice) const {
    Status status;
    std::string stats;
    std::tie(status, stats) = readLocalDoc(vbid, keySlice);
    if (!status.IsOK()) {
        if (status.ErrorCode() != Status::Code::NotFound) {
            logger->warn("MagmaKVStore::getCollectionStats(): {}",
                         status.Message());
            return {false, Collections::VB::PersistedStats()};
        }
        // Return Collections::VB::PersistedStats(true) with everything set to
        // 0 as the collection might have not been persisted to disk yet.
        return {true, Collections::VB::PersistedStats()};
    }
    return {true, Collections::VB::PersistedStats(stats.c_str(), stats.size())};
}

void MagmaKVStore::deleteDroppedCollectionStats(LocalDbReqs& localDbReqs,
                                                CollectionID cid) {
    auto key = getDroppedCollectionsStatsKey(cid);
    localDbReqs.emplace_back(MagmaLocalReq::makeDeleted(key));
}

void MagmaKVStore::deleteCollectionStats(LocalDbReqs& localDbReqs,
                                         CollectionID cid) {
    auto key = getCollectionsStatsKey(cid);
    localDbReqs.emplace_back(MagmaLocalReq::makeDeleted(key));
}

magma::Status MagmaKVStore::updateScopes(
        Vbid vbid,
        LocalDbReqs& localDbReqs,
        Collections::VB::Flush& collectionsFlush) {
    Slice keySlice(openScopesKey);
    auto [status, scopes] = readLocalDoc(vbid, keySlice);

    if (status.IsOK() || status.ErrorCode() == Status::Code::NotFound) {
        auto buf = collectionsFlush.encodeOpenScopes(
                {reinterpret_cast<const uint8_t*>(scopes.data()),
                 scopes.length()});
        localDbReqs.emplace_back(MagmaLocalReq(openScopesKey, buf));
        return Status::OK();
    }

    return status;
}

bool MagmaKVStore::getStat(std::string_view name, size_t& value) const {
    std::array<std::string_view, 1> keys = {{name}};
    auto stats = getStats(keys);
    auto stat = stats.find(name);
    if (stat != stats.end()) {
        value = stat->second;
        return true;
    }
    return false;
}

GetStatsMap MagmaKVStore::getStats(
        gsl::span<const std::string_view> keys) const {
    Magma::MagmaStats magmaStats;
    magma->GetStats(magmaStats);

    GetStatsMap stats;
    auto fill = [&](std::string_view statName, size_t value) {
        auto it = std::find(keys.begin(), keys.end(), statName);
        if (it != keys.end()) {
            stats.try_emplace(*it, value);
        }
    };
    fill("memory_quota", magmaStats.MemoryQuota);
    fill("write_cache_quota", magmaStats.WriteCacheQuota);
    fill("failure_get", st.numGetFailure.load());
    fill("failure_compaction", st.numCompactionFailure.load());
    fill("storage_mem_used", magmaStats.TotalMemUsed);
    fill("magma_NCompacts", magmaStats.NCompacts);
    fill("magma_NFlushes", magmaStats.NFlushes);
    fill("magma_NTTLCompacts", magmaStats.NTTLCompacts);
    fill("magma_NFileCountCompacts", magmaStats.NFileCountCompacts);
    fill("magma_NWriterCompacts", magmaStats.NWriterCompacts);
    fill("magma_BytesOutgoing", magmaStats.BytesOutgoing);
    fill("magma_NReadBytes", magmaStats.NReadBytes);
    fill("magma_NReadBytesGet", magmaStats.NReadBytesGet);
    fill("magma_NGets", magmaStats.NGets);
    fill("magma_NSets", magmaStats.NSets);
    fill("magma_NInserts", magmaStats.NInserts);
    fill("magma_NReadIO", magmaStats.NReadIOs);
    fill("magma_NReadBytesCompact", magmaStats.NReadBytesCompact);
    fill("magma_BytesIncoming", magmaStats.BytesIncoming);
    fill("magma_NWriteBytes", magmaStats.NWriteBytes);
    fill("magma_NWriteBytesCompact", magmaStats.NWriteBytesCompact);
    fill("magma_LogicalDataSize", magmaStats.LogicalDataSize);
    fill("magma_LogicalDiskSize", magmaStats.LogicalDiskSize);
    fill("magma_TotalDiskUsage", magmaStats.TotalDiskUsage);
    fill("magma_WALDiskUsage", magmaStats.WalStats.DiskUsed);
    fill("magma_BlockCacheMemUsed", magmaStats.BlockCacheMemUsed);
    fill("magma_KeyIndexSize", magmaStats.KeyStats.LogicalDataSize);
    fill("magma_SeqIndex_IndexBlockSize",
         magmaStats.SeqStats.TotalIndexBlocksSize);
    fill("magma_WriteCacheMemUsed", magmaStats.WriteCacheMemUsed);
    fill("magma_WALMemUsed", magmaStats.WALMemUsed);
    fill("magma_TableMetaMemUsed", magmaStats.TableMetaMemUsed);
    fill("magma_BufferMemUsed", magmaStats.BufferMemUsed);
    fill("magma_TotalMemUsed", magmaStats.TotalMemUsed);
    fill("magma_TotalBloomFilterMemUsed", magmaStats.TotalBloomFilterMemUsed);
    fill("magma_BlockCacheHits", magmaStats.BlockCacheHits);
    fill("magma_BlockCacheMisses", magmaStats.BlockCacheMisses);
    fill("magma_NTablesDeleted", magmaStats.NTablesDeleted);
    fill("magma_NTablesCreated", magmaStats.NTablesCreated);
    fill("magma_NTableFiles", magmaStats.NTableFiles);
    fill("magma_NSyncs", magmaStats.NSyncs);
    return stats;
}

void MagmaKVStore::addStats(const AddStatFn& add_stat,
                            const void* c,
                            const std::string& args) const {
    KVStore::addStats(add_stat, c, args);
    const auto prefix = getStatsPrefix();

    Magma::MagmaStats stats;
    magma->GetStats(stats);
    auto statName = prefix + ":magma";
    add_casted_stat(statName.c_str(), stats.JSON().dump(), add_stat, c);
}

void MagmaKVStore::pendingTasks() {
    std::queue<std::tuple<Vbid, uint64_t>> vbucketsToDelete;
    vbucketsToDelete.swap(*pendingVbucketDeletions.wlock());
    while (!vbucketsToDelete.empty()) {
        Vbid vbid;
        uint64_t vb_version;
        std::tie(vbid, vb_version) = vbucketsToDelete.front();
        vbucketsToDelete.pop();
        delVBucket(vbid, vb_version);
    }
}

std::shared_ptr<CompactionContext> MagmaKVStore::makeCompactionContext(
        Vbid vbid) {
    if (!makeCompactionContextCallback) {
        throw std::runtime_error(
                "MagmaKVStore::makeCompactionContext: Have not set "
                "makeCompactionContextCallback to create a CompactionContext");
    }

    CompactionConfig config{};
    auto ctx = makeCompactionContextCallback(vbid, config, 0 /*purgeSeqno*/);

    auto [status, dropped] = getDroppedCollections(vbid);
    if (!status) {
        throw std::runtime_error(
                fmt::format("MagmaKVStore::makeCompactionContext: {} failed to "
                            "get the dropped collections",
                            vbid));
    }
    ctx->eraserContext =
            std::make_unique<Collections::VB::EraserContext>(dropped);

    auto readState = readVBStateFromDisk(vbid);
    if (!readState.status.IsOK()) {
        std::stringstream ss;
        ss << "MagmaKVStore::makeCompactionContext " << vbid
           << " failed to read vbstate from disk. Status:"
           << readState.status.String();
        throw std::runtime_error(ss.str());
    } else {
        ctx->highCompletedSeqno = readState.vbstate.persistedCompletedSeqno;
    }

    logger->debug(
            "MagmaKVStore::makeCompactionContext {} purge_before_ts:{} "
            "purge_before_seq:{}"
            " drop_deletes:{} retain_erroneous_tombstones:{}",
            ctx->vbid,
            ctx->compactConfig.purge_before_ts,
            ctx->compactConfig.purge_before_seq,
            ctx->compactConfig.drop_deletes,
            ctx->compactConfig.retain_erroneous_tombstones);

    return ctx;
}

const KVStoreConfig& MagmaKVStore::getConfig() const {
    return configuration;
}

DBFileInfo MagmaKVStore::getDbFileInfo(Vbid vbid) {
    DBFileInfo vbinfo;
    auto [status, kvstats] = magma->GetKVStoreStats(vbid.get());
    if (status) {
        vbinfo.spaceUsed = kvstats.ActiveDiskUsage;
        vbinfo.fileSize = kvstats.TotalDiskUsage;
    }
    logger->debug(
            "MagmaKVStore::getDbFileInfo {} spaceUsed:{} fileSize:{} status:{}",
            vbid,
            vbinfo.spaceUsed,
            vbinfo.fileSize,
            status.String());
    return vbinfo;
}

DBFileInfo MagmaKVStore::getAggrDbFileInfo() {
    Magma::MagmaStats stats;
    magma->GetStats(stats);
    // @todo MB-42900: Track on-disk-prepare-bytes
    DBFileInfo vbinfo{
            stats.ActiveDiskUsage, stats.ActiveDataSize, 0 /*prepareBytes*/};
    return vbinfo;
}

Status MagmaKVStore::writeVBStateToDisk(Vbid vbid,
                                        const vbucket_state& vbstate) {
    LocalDbReqs localDbReqs;
    addVBStateUpdateToLocalDbReqs(
            localDbReqs, vbstate, kvstoreRevList[getCacheSlot(vbid)]);

    WriteOps writeOps;
    addLocalDbReqs(localDbReqs, writeOps);

    auto status = magma->WriteDocs(
            vbid.get(), writeOps, kvstoreRevList[getCacheSlot(vbid)]);
    if (!status) {
        ++st.numVbSetFailure;
        logger->critical(
                "MagmaKVStore::writeVBStateToDisk failed creating "
                "commitBatch for "
                "{} status:{}",
                vbid,
                status.String());
        return status;
    }

    if (doCheckpointEveryBatch) {
        status = magma->Sync(true);
        if (!status) {
            ++st.numVbSetFailure;
            logger->critical(
                    "MagmaKVStore::writeVBStateToDisk: "
                    "magma::Sync {} "
                    "status:{}",
                    vbid,
                    status.String());
        }
    }
    return Status::OK();
}

MagmaKVStore::MagmaLocalReq::MagmaLocalReq(
        std::string_view key, const flatbuffers::DetachedBuffer& buf)
    : key(key), value(reinterpret_cast<const char*>(buf.data()), buf.size()) {
}

void MagmaKVStore::addLocalDbReqs(const LocalDbReqs& localDbReqs,
                                  WriteOps& writeOps) {
    for (auto& req : localDbReqs) {
        Slice keySlice(req.key);
        Slice valSlice(req.value);
        if (req.deleted) {
            writeOps.emplace_back(
                    Magma::WriteOperation::NewLocalDocDelete(keySlice));
        } else {
            writeOps.emplace_back(Magma::WriteOperation::NewLocalDocUpdate(
                    keySlice, valSlice));
        }
    }
}

void MagmaKVStore::addStatUpdateToWriteOps(
        MagmaDbStats& stats, MagmaKVStore::WriteOps& writeOps) const {
    writeOps.emplace_back(Magma::WriteOperation::NewUserStatsUpdate(&stats));
}

void MagmaKVStore::setMagmaFragmentationPercentage(size_t value) {
    magma->SetFragmentationRatio(value / 100.0);
}

void MagmaKVStore::setStorageThreads(ThreadPoolConfig::StorageThreadCount num) {
    configuration.setStorageThreads(num);
    calculateAndSetMagmaThreads();
}

void MagmaKVStore::calculateAndSetMagmaThreads() {
    auto backendThreads = configuration.getStorageThreads();
    auto rawBackendThreads =
            static_cast<int>(configuration.getStorageThreads());
    if (backendThreads == ThreadPoolConfig::StorageThreadCount::Default) {
        rawBackendThreads = configuration.getMagmaMaxDefaultStorageThreads();
    }

    auto flusherRatio =
            static_cast<float>(configuration.getMagmaFlusherPercentage()) / 100;
    auto flushers = std::ceil(rawBackendThreads * flusherRatio);
    auto compactors = rawBackendThreads - flushers;

    if (flushers <= 0) {
        logger->warn(
                "MagmaKVStore::calculateAndSetMagmaThreads "
                "flushers <= 0. "
                "StorageThreads:{} "
                "WriterThreads:{} "
                "Setting flushers=1",
                rawBackendThreads,
                ExecutorPool::get()->getNumWriters());
        flushers = 1;
    }

    if (compactors <= 0) {
        logger->warn(
                "MagmaKVStore::calculateAndSetMagmaThreads "
                "compactors <= 0. "
                "StorageThreads:{} "
                "WriterThreads:{} "
                "Setting compactors=1",
                rawBackendThreads,
                ExecutorPool::get()->getNumWriters());
        compactors = 1;
    }

    logger->info(
            "MagmaKVStore::calculateAndSetMagmaThreads: Flushers:{} "
            "Compactors:{}",
            flushers,
            compactors);

    magma->SetNumThreads(Magma::ThreadType::Flusher, flushers);
    magma->SetNumThreads(Magma::ThreadType::Compactor, compactors);
}

uint32_t MagmaKVStore::getExpiryOrPurgeTime(const magma::Slice& slice) {
    auto exptime = magmakv::getExpiryTime(slice);

    if (magmakv::isDeleted(slice)) {
        exptime += configuration.getMetadataPurgeAge();
    }

    return exptime;
}

void to_json(nlohmann::json& json, const MagmaDbStats& dbStats) {
    auto locked = dbStats.stats.rlock();
    json = nlohmann::json{{"docCount", std::to_string(locked->docCount)},
                          {"purgeSeqno", std::to_string(locked->purgeSeqno)}};
}

void from_json(const nlohmann::json& j, MagmaDbStats& dbStats) {
    auto locked = dbStats.stats.wlock();
    locked->docCount = std::stoull(j.at("docCount").get<std::string>());
    locked->purgeSeqno.reset(
            std::stoull(j.at("purgeSeqno").get<std::string>()));
}

void MagmaDbStats::Merge(const UserStats& other) {
    auto otherStats = dynamic_cast<const MagmaDbStats*>(&other);
    if (!otherStats) {
        throw std::invalid_argument("MagmaDbStats::Merge: Bad cast of other");
    }
    auto locked = stats.wlock();
    auto otherLocked = otherStats->stats.rlock();

    locked->docCount += otherLocked->docCount;
    if (otherLocked->purgeSeqno > locked->purgeSeqno) {
        locked->purgeSeqno = otherLocked->purgeSeqno;
    }
}

std::unique_ptr<magma::UserStats> MagmaDbStats::Clone() {
    auto cloned = std::make_unique<MagmaDbStats>();
    cloned->reset(*this);
    return cloned;
}

std::string MagmaDbStats::Marshal() {
    nlohmann::json j = *this;
    return j.dump();
}

Status MagmaDbStats::Unmarshal(const std::string& encoded) {
    nlohmann::json j;
    try {
        j = nlohmann::json::parse(encoded);
    } catch (const nlohmann::json::exception& e) {
        throw std::logic_error("MagmaDbStats::Unmarshal cannot decode json:" +
                               encoded + " " + e.what());
    }

    try {
        reset(j);
    } catch (const nlohmann::json::exception& e) {
        throw std::logic_error(
                "MagmaDbStats::Unmarshal cannot construct MagmaDbStats from "
                "json:" +
                encoded + " " + e.what());
    }

    return Status::OK();
}
