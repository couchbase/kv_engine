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

#include "backup/backup.h"
#include "bucket_logger.h"
#include "collections/collection_persisted_stats.h"
#include "collections/kvstore.h"
#include "dockey_validator.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "hlc.h"
#include "item.h"
#include "kv_magma_common/magma-kvstore_metadata.h"
#include "kvstore/kvstore_transaction_context.h"
#include "kvstore/rollback_callback.h"
#include "kvstore/storage_common/storage_common/local_doc_constants.h"
#include "magma-kvstore_config.h"
#include "magma-kvstore_iorequest.h"
#include "magma-kvstore_rollback_purge_seqno_ctx.h"
#include "magma-memory-tracking-proxy.h"
#include "objectregistry.h"
#include "vb_commit.h"
#include "vbucket.h"
#include "vbucket_state.h"
#include <executor/executorpool.h>
#include <folly/ScopeGuard.h>
#include <mcbp/protocol/datatype.h>
#include <mcbp/protocol/unsigned_leb128.h>
#include <nlohmann/json.hpp>
#include <platform/cb_arena_malloc.h>
#include <platform/compress.h>
#include <platform/sized_buffer.h>
#include <statistics/cbstat_collector.h>
#include <utilities/dek_file_utilities.h>
#include <utilities/logtags.h>
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <limits>
#include <utility>

class Snapshot;

using magma::Magma;
using magma::MagmaFileStats;
using magma::MagmaHistogramStats;
using magma::Slice;
using magma::Status;
using namespace std::chrono_literals;

// Unfortunately, turning on logging for the tests is limited to debug
// mode. While we are in the midst of dropping in magma, this provides
// a simple way to change all the logging->trace calls to logging->debug
// by changing trace to debug..
// Once magma has been completed, we can remove this #define and change
// all the logging calls back to trace.
#define TRACE trace

namespace magmakv {
MetaData makeMetaData(const Item& it) {
    auto metadata = MetaData();

    // From Trinity always store V2. This allows the history bit to be defined
    // and allows optional readback of V1 durability
    metadata.setVersion(MetaData::Version::V2);
    metadata.setBySeqno(it.getBySeqno());
    metadata.setCas(it.getCas());
    metadata.setRevSeqno(it.getRevSeqno());
    metadata.setExptime(it.getExptime());
    metadata.setFlags(it.getFlags());
    metadata.setValueSize(it.getNBytes());
    metadata.setDataType(it.getDataType());

    if (it.isDeleted() && !it.isPending()) {
        metadata.setDeleted(true, it.deletionSource());
    }

    if (it.isPending()) {
        metadata.setDurabilityDetailsForPrepare(
                it.isDeleted(),
                static_cast<uint8_t>(it.getDurabilityReqs().getLevel()));
    }

    if (it.isAbort()) {
        metadata.setDurabilityDetailsForAbort(it.getPrepareSeqno());
    }

    // If Item can be deduplicated, then history=false.
    metadata.setHistory(!it.canDeduplicate());

    return metadata;
}

std::string MetaData::to_string() const {
    fmt::memory_buffer memoryBuffer;
    fmt::format_to(std::back_inserter(memoryBuffer),
                   "bySeqno:{} cas:{} exptime:{} revSeqno:{} flags:{} "
                   "valueSize:{} bits v0:{:x} v2:{:x}, "
                   "deleted:{} deleteSource:{} version:{} datatype:{}",
                   allMeta.v0.bySeqno,
                   allMeta.v0.cas,
                   allMeta.v0.exptime,
                   allMeta.v0.revSeqno,
                   allMeta.v0.flags,
                   allMeta.v0.valueSize,
                   allMeta.v0.bits.all,
                   allMeta.v2.bits.all,
                   isDeleted(),
                   !isDeleted() ? " " : ::to_string(getDeleteSource()),
                   static_cast<uint8_t>(getVersion()),
                   allMeta.v0.datatype);

    if (isHistoryDefined()) {
        fmt::format_to(std::back_inserter(memoryBuffer),
                       " history:{}",
                       isHistoryEnabled());
    }

    if (isDurabilityDefined()) {
        if (isDeleted()) {
            // abort
            fmt::format_to(std::back_inserter(memoryBuffer),
                           " prepareSeqno:{}",
                           allMeta.v1.durabilityDetails.completed.prepareSeqno);
        } else {
            // prepare
            fmt::format_to(std::back_inserter(memoryBuffer),
                           " durabilityLevel:{} syncDelete:{}",
                           allMeta.v1.durabilityDetails.pending.level,
                           allMeta.v1.durabilityDetails.pending.isDelete);
        }
    }

    return fmt::to_string(memoryBuffer);
}

/**
 * Magma stores an item in 3 slices... key, metadata, value
 * See libmagma/slice.h for more info on slices.
 *
 * Helper functions to pull metadata stuff out of the metadata slice.
 * The are used down in magma and are passed in as part of the configuration.
 *
 * NOTE: Constructing a MetaData object from the metaSlice is low but non-zero
 *       cost (needs decoding of multiple LEB128 values). As such, avoid
 *       calling multiple of these functions on the same same slice - prefer
 *       to call getDocMeta(slice) once to construct a MetaData object once,
 *       then manipulate that.
 */
static const MetaData getDocMeta(std::string_view meta) {
    return MetaData(meta);
}

static const MetaData getDocMeta(const Slice& metaSlice) {
    return getDocMeta(std::string_view{metaSlice.Data(), metaSlice.Len()});
}

static uint64_t getSeqNum(const Slice& metaSlice) {
    return getDocMeta(metaSlice).getBySeqno();
}

static size_t getValueSize(const Slice& metaSlice) {
    return getDocMeta(metaSlice).getValueSize();
}

static bool isDeleted(const Slice& metaSlice) {
    return getDocMeta(metaSlice).isDeleted();
}

static bool isCompressed(const MetaData& docMeta) {
    return cb::mcbp::datatype::is_snappy(docMeta.getDatatype());
}

static bool isPrepared(const Slice& keySlice, const MetaData& docMeta) {
    return DiskDocKey(keySlice.Data(), keySlice.Len()).isPrepared() &&
           !docMeta.isDeleted();
}

static bool isAbort(const Slice& keySlice, const MetaData& docMeta) {
    return DiskDocKey(keySlice.Data(), keySlice.Len()).isPrepared() &&
           docMeta.isDeleted();
}

static std::chrono::seconds getHistoryTimeStamp(const Slice& metaSlice) {
    return getDocMeta(metaSlice).getHistoryTimeStamp();
}

static std::optional<std::chrono::seconds> getHistoryTimeNow(
        EventuallyPersistentEngine* engine, Magma::KVStoreID kvid) {
    if (std::optional<cb::HlcTime> hlc;
        engine && (hlc = engine->getVBucketHlcNow(Vbid(kvid)))) {
        return hlc->now;
    }
    if (engine) {
        // "NMVB" - indicate to caller that no time could be determined.
        // This occurs whilst compacting runs against a vbucket that KV has
        // removed from the vbucket map. In this case returning "no-time"
        // ensures the caller gets a clear message that no history should be
        // purged by time checks during this state.
        return std::nullopt;
    }

    // else no engine, some unit tests run without an engine and may require
    // a "relevant" time
    using namespace std::chrono;
    return duration_cast<seconds>(nanoseconds(HLC::getMaskedTime()));
}

static magma::Magma::HistoryMode getHistoryModeFromMeta(
        const Slice& metaSlice) {
    return getDocMeta(metaSlice).isHistoryEnabled()
                   ? magma::Magma::HistoryMode::Enabled
                   : magma::Magma::HistoryMode::Disabled;
}

} // namespace magmakv

MagmaRequest::MagmaRequest(queued_item it)
    : IORequest(std::move(it)), docMeta(magmakv::makeMetaData(*item).encode()) {
}

std::string MagmaRequest::to_string() {
    return fmt::format("Key:{} docMeta:{} oldItemAlive:{}",
                       key.to_string(),
                       magmakv::getDocMeta(getDocMeta()).to_string(),
                       oldItemAlive);
}

static DiskDocKey makeDiskDocKey(const Slice& key) {
    return DiskDocKey{key.Data(), key.Len()};
}

/**
 * Given a magma::Status::Code on error, map to the equivalent cb::engine_errc
 */
cb::engine_errc magmaErr2EngineErr(Status::Code result) {
    // General approach is return 'temporary_failure' for recoverable errors,
    // if non-recoverage then return failed, unless there's something more
    // relevent.
    switch (result) {
    case Status::Ok:
        return cb::engine_errc::success;
    case Status::OkDocNotFound:
        return cb::engine_errc::no_such_key;
    case Status::Internal:
        return cb::engine_errc::failed;
    case Status::Invalid:
        return cb::engine_errc::invalid_arguments;
    case Status::InvalidKVStore:
        return cb::engine_errc::temporary_failure;
    case Status::Corruption:
        return cb::engine_errc::failed;
    case Status::NotFound:
        return cb::engine_errc::failed;
    case Status::IOError:
        return cb::engine_errc::temporary_failure;
    case Status::ReadOnly:
        return cb::engine_errc::temporary_failure;
    case Status::TransientIO:
        return cb::engine_errc::temporary_failure;
    case Status::DiskFull:
        return cb::engine_errc::temporary_failure;
    case Status::Cancelled:
        return cb::engine_errc::temporary_failure;
    case Status::RetryLater:
        return cb::engine_errc::temporary_failure;
    case Status::CheckpointNotFound:
        return cb::engine_errc::temporary_failure;
    case Status::EncryptionKeyNotFound:
        return cb::engine_errc::encryption_key_not_available;
    case Status::NoAccess:
        return cb::engine_errc::temporary_failure;
    }
    folly::assume_unreachable();
}

class MagmaKVFileHandle : public KVFileHandle {
public:
    MagmaKVFileHandle(Vbid vbid,
                      DomainAwareUniquePtr<magma::Magma::Snapshot> snapshot,
                      MagmaMemoryTrackingProxy& magma)
        : vbid(vbid), snapshot(std::move(snapshot)), magma(magma) {
        Expects(this->snapshot);
    }

    size_t getHowManyBytesCouldBeFreed() const override {
        return magma.GetDiskSizeOverhead(*snapshot);
    }

    Vbid vbid;
    DomainAwareUniquePtr<magma::Magma::Snapshot> snapshot;
    MagmaMemoryTrackingProxy& magma;
};

MagmaKVStore::MagmaCompactionCB::MagmaCompactionCB(
        MagmaKVStore& magmaKVStore,
        Vbid vbid,
        std::shared_ptr<CompactionContext> compactionContext,
        std::optional<CollectionID> cid)
    : vbid(vbid),
      ctx(std::move(compactionContext)),
      magmaKVStore(magmaKVStore),
      onlyThisCollection(cid) {
    magmaKVStore.logger->TRACE("MagmaCompactionCB constructor");

    // If we've not got a CompactionContext then this must be an implicit
    // compaction so we need to create a CompactionContext
    if (!ctx) {
        if (!magmaKVStore.makeCompactionContextCallback) {
            // We can't perform an implicit compaction if
            // makeCompactionContextCallback isn't set.
            throw std::invalid_argument(
                    "MagmaKVStore::MagmaCompactionCB::MagmaCompactionCB() no "
                    "makeCompactionContextCallback set");
        }
        ctx = magmaKVStore.makeImplicitCompactionContext(vbid);
        // If we don't have a valid compaction context then throw to prevent us
        // creating a MagmaCompactionCB that won't be able to do any compaction
        if (!ctx) {
            throw std::runtime_error(
                    "MagmaKVStore::MagmaCompactionCB::MagmaCompactionCB() "
                    "couldn't create compaction context");
        }
        ctx->purgedItemCtx->rollbackPurgeSeqnoCtx =
                std::make_unique<MagmaImplicitCompactionPurgedItemContext>(
                        ctx->getRollbackPurgeSeqno(),
                        magmaDbStats,
                        ctx->maybeUpdateVBucketPurgeSeqno);

        Status status;
        std::tie(status, oldestRollbackableHighSeqno) =
                magmaKVStore.getOldestRollbackableHighSeqno(vbid);
        if (!status) {
            throw std::runtime_error(
                    "MagmaCompactionCallback: Failed to get "
                    "getOldestRollbackableHighSeqno: " +
                    vbid.to_string() + " : " + status.String());
        }
    } else {
        ctx->purgedItemCtx->rollbackPurgeSeqnoCtx =
                std::make_unique<MagmaRollbackPurgeSeqnoCtx>(
                        ctx->getRollbackPurgeSeqno(), magmaDbStats);

        // set to unlimited since all checkpoints are cleared by magma explicit
        // compaction and so magma rollback would not be possible after the
        // explicit compaction
        oldestRollbackableHighSeqno = std::numeric_limits<uint64_t>::max();
    }
}

MagmaKVStore::MagmaCompactionCB::~MagmaCompactionCB() {
    magmaKVStore.logger->debug("MagmaCompactionCB destructor");
}

void MagmaKVStore::processCollectionPurgeDelta(MagmaDbStats& magmaDbStats,
                                               CollectionID cid,
                                               int64_t delta) {
    magmaDbStats.docCount += delta;
    magmaDbStats.droppedCollectionCounts[static_cast<uint32_t>(cid)] += delta;
}

bool MagmaKVStore::MagmaCompactionCB::operator()(
        const magma::Slice& keySlice,
        const magma::Slice& metaSlice,
        const magma::Slice& valueSlice) {
    // catch any exceptions from the compaction callback and log them, as we
    // don't want to crash magma given it runs on a backend thread.
    try {
        auto [status, drop] = magmaKVStore.compactionCallBack(
                *this, keySlice, metaSlice, valueSlice);
        SetStatus(status);
        return drop;
    } catch (std::exception& e) {
        auto msg = fmt::format("MagmaKVStore::compactionCallBack() threw:'{}'",
                               e.what());
        magmaKVStore.logger->warn(msg);
        SetStatus({Status::Internal, msg});
    }
    return false;
}

bool MagmaKVStore::MagmaCompactionCB::canPurge(CollectionID collection) {
    if (!onlyThisCollection) {
        return true;
    }
    return onlyThisCollection.value() == collection;
}

MagmaKVStoreTransactionContext::MagmaKVStoreTransactionContext(
        KVStore& kvstore, Vbid vbid, std::unique_ptr<PersistenceCallback> cb)
    : TransactionContext(kvstore, vbid, std::move(cb)) {
}

void MagmaKVStoreTransactionContext::preparePendingRequests(
        magma::Magma::HistoryMode batchHistoryMode) {
    if (batchHistoryMode == magma::Magma::HistoryMode::Disabled) {
        return;
    }

    // MB-55199: Magma requires key/seqno order, but ascending seqno.
    // KV-engine flusher writes out the batch in key/seqno, but descending seqno
    // order.
    std::sort(pendingReqs.begin(),
              pendingReqs.end(),
              [](const auto& lhs, const auto& rhs) {
                  const auto comp = lhs->getKey().compare(rhs->getKey());
                  // When keys are equal, sort by seqno.
                  if (comp == 0) {
                      return lhs->getItem().getBySeqno() <
                             rhs->getItem().getBySeqno();
                  }
                  return comp < 0;
              });
}

std::pair<Status, bool> MagmaKVStore::compactionCallBack(
        MagmaKVStore::MagmaCompactionCB& cbCtx,
        const magma::Slice& keySlice,
        const magma::Slice& metaSlice,
        const magma::Slice& valueSlice) const {
    // This callback is operating on the secondary memory domain
    Expects(currEngine == ObjectRegistry::getCurrentEngine());
    // If we are compacting the localDb, items don't have metadata so
    // we always keep everything.
    if (metaSlice.Len() == 0) {
        return {Status::OK(), false};
    }

    if (cbCtx.ctx->isShuttingDown()) {
        return {{Status::Cancelled, "Shutting down"}, false};
    }

    auto vbid = cbCtx.vbid;
    std::string userSanitizedItemStr;
    if (logger->should_log(spdlog::level::TRACE)) {
        userSanitizedItemStr =
                "key:" +
                cb::UserData{makeDiskDocKey(keySlice).to_string()}
                        .getSanitizedValue() +
                " " + magmakv::getDocMeta(metaSlice).to_string();
        logger->TRACE("MagmaCompactionCB: {} {}", vbid, userSanitizedItemStr);
    }

    if (configuration.isSanityCheckingVBucketMapping()) {
        validateKeyMapping("MagmaKVStore::compactionCallback",
                           configuration.getVBucketMappingErrorHandlingMethod(),
                           makeDiskDocKey(keySlice).getDocKey(),
                           vbid,
                           configuration.getMaxVBuckets());
    }

    return compactionCore(
            cbCtx, keySlice, metaSlice, valueSlice, userSanitizedItemStr);
}

// Compaction core code which runs on the primary memory domain as it now will
// create items for KV
std::pair<Status, bool> MagmaKVStore::compactionCore(
        MagmaKVStore::MagmaCompactionCB& cbCtx,
        const magma::Slice& keySlice,
        const magma::Slice& metaSlice,
        const magma::Slice& valueSlice,
        std::string_view userSanitizedItemStr) const {
    // Run on primary domain so that we can create objects to pass to KV
    cb::UseArenaMallocPrimaryDomain domainGuard;

    const auto docMeta = magmakv::getDocMeta(metaSlice);
    const uint64_t seqno = docMeta.getBySeqno();
    const auto exptime = docMeta.getExptime();

    auto vbid = cbCtx.vbid;

    // eraserContext is not set for implicit compactions. Explicit
    // compactions in magma are not rollback able while implicit are. If we
    // rollback a collection drop via implicit compaction, it can result in
    // the dropped keys not showing up in the rollback callback.
    if (cbCtx.ctx->eraserContext && cbCtx.ctx->droppedKeyCb) {
        // We need to check both committed and prepared documents - if the
        // collection has been logically deleted then we need to discard
        // both types of keys.
        // As such use the docKey (i.e. without any DurabilityPrepare
        // namespace) when checking if logically deleted;
        auto diskKey = makeDiskDocKey(keySlice);

        if (cbCtx.canPurge(diskKey.getDocKey().getCollectionID()) &&
            cbCtx.ctx->eraserContext->isLogicallyDeleted(
                    diskKey.getDocKey(), docMeta.isDeleted(), seqno)) {
            try {
                // Inform vb that the key@seqno is dropped
                cbCtx.ctx->droppedKeyCb(diskKey,
                                        seqno,
                                        magmakv::isAbort(keySlice, docMeta),
                                        cbCtx.ctx->highCompletedSeqno);
            } catch (const std::exception& e) {
                logger->warn(
                        "MagmaKVStore::compactionCallBack(): droppedKeyCb "
                        "exception: {}",
                        e.what());
                return {{Status::Internal, e.what()}, false};
            }

            if (magmakv::isPrepared(keySlice, docMeta)) {
                cbCtx.ctx->stats.preparesPurged++;
            } else {
                if (docMeta.isDeleted()) {
                    cbCtx.ctx->stats.collectionsDeletedItemsPurged++;
                }
            }

            if (logger->should_log(spdlog::level::TRACE)) {
                logger->TRACE(
                        "MagmaKVStore::compactionCore: {} DROP "
                        "collections "
                        "{}",
                        vbid,
                        userSanitizedItemStr);
            }
            cbCtx.ctx->purgedItemCtx->purgedItem(PurgedItemType::LogicalDelete,
                                                 seqno);
            return {Status::OK(), true};
        }
    }

    if (docMeta.isDeleted()) {
        uint64_t maxSeqno;
        auto status = magma->GetMaxSeqno(vbid.get(), maxSeqno);
        if (!status) {
            throw std::runtime_error("MagmaKVStore::compactionCore: Failed : " +
                                     vbid.to_string() + " " + status.String());
        }

        // A bunch of DCP code relies on us keeping the last item (it may be a
        // tombstone) so we can't purge the item at maxSeqno.
        // We can't drop tombstones with seqno >
        // cbCtx.oldestRollbackableHighSeqno. If dropped, they will not be found
        // by rollback callback causing items to not be restored to the
        // hashTable on rollback.
        if (seqno != maxSeqno && seqno <= cbCtx.oldestRollbackableHighSeqno) {
            bool drop = false;
            if (cbCtx.ctx->compactConfig.drop_deletes) {
                if (logger->should_log(spdlog::level::TRACE)) {
                    logger->TRACE(
                            "MagmaKVStore::compactionCore: {} DROP "
                            "drop_deletes {}",
                            vbid,
                            userSanitizedItemStr);
                }
                drop = true;
            }

            if (exptime && exptime < cbCtx.ctx->compactConfig.purge_before_ts) {
                if (logger->should_log(spdlog::level::TRACE)) {
                    logger->TRACE(
                            "MagmaKVStore::compactionCore: {} DROP expired "
                            "tombstone {}",
                            vbid,
                            userSanitizedItemStr);
                }
                drop = true;
            }

            if (drop) {
                cbCtx.ctx->stats.tombstonesPurged++;
                cbCtx.ctx->purgedItemCtx->purgedItem(PurgedItemType::Tombstone,
                                                     seqno);
                return {Status::OK(), true};
            }
        }
    } else {
        // We can remove any prepares that have been completed. This works
        // because we send Mutations instead of Commits when streaming from
        // Disk so we do not need to send a Prepare message to keep things
        // consistent on a replica.
        // We also don't drop prepares that are rollbackable. If they are
        // deleted, rollback callback will not be called on the prepares during
        // rollback.
        if (magmakv::isPrepared(keySlice, docMeta)) {
            if (seqno <= cbCtx.ctx->highCompletedSeqno &&
                seqno <= cbCtx.oldestRollbackableHighSeqno) {
                cbCtx.ctx->stats.preparesPurged++;

                if (logger->should_log(spdlog::level::TRACE)) {
                    logger->TRACE(
                            "MagmaKVStore::compactionCore: {}"
                            "DROP prepare {}",
                            vbid,
                            userSanitizedItemStr);
                }
                cbCtx.ctx->purgedItemCtx->purgedItem(PurgedItemType::Prepare,
                                                     seqno);
                return {Status::OK(), true};
            }
        }

        time_t timeToExpireFrom;
        if (cbCtx.ctx->timeToExpireFrom) {
            timeToExpireFrom = cbCtx.ctx->timeToExpireFrom.value();
        } else {
            timeToExpireFrom = ep_real_time();
        }

        if (exptime && exptime < timeToExpireFrom &&
            !magmakv::isPrepared(keySlice, docMeta)) {
            auto itm =
                    std::make_unique<Item>(makeDiskDocKey(keySlice).getDocKey(),
                                           docMeta.getFlags(),
                                           docMeta.getExptime(),
                                           valueSlice.Data(),
                                           valueSlice.Len(),
                                           docMeta.getDatatype(),
                                           docMeta.getCas(),
                                           docMeta.getBySeqno(),
                                           vbid,
                                           docMeta.getRevSeqno());
            if (magmakv::isCompressed(docMeta)) {
                itm->decompressValue();
            }
            itm->setDeleted(DeleteSource::TTL);
            cbCtx.ctx->expiryCallback->callback(*(itm.get()), timeToExpireFrom);

            if (logger->should_log(spdlog::level::TRACE)) {
                logger->TRACE(
                        "MagmaKVStore::compactionCore: {} expiry callback {}",
                        vbid,
                        userSanitizedItemStr);
            }
        }
    }

    if (logger->should_log(spdlog::level::TRACE)) {
        logger->TRACE("MagmaKVStore::compactionCore: {} KEEP {}",
                      vbid,
                      userSanitizedItemStr);
    }
    return {Status::OK(), false};
}

MagmaKVStore::MagmaKVStore(MagmaKVStoreConfig& configuration,
                           EncryptionKeyProvider* encryptionKeyProvider,
                           std::string_view chronicleAuthToken)
    : KVStore(),
      configuration(configuration),
      magmaPath(configuration.getDBName() + "/magma." +
                std::to_string(configuration.getShardId())),
      scanCounter(0),
      continuousBackupStatus(getCacheSize(), BackupStatus::Stopped),
      currEngine(ObjectRegistry::getCurrentEngine()),
      fusionUploaderManager(*this) {
    configuration.magmaCfg.Path = magmaPath;
    configuration.magmaCfg.MaxKVStores = configuration.getMaxVBuckets();
    configuration.magmaCfg.MaxKVStoreLSDBufferSize = gsl::narrow_cast<int>(
            configuration.getMagmaDeleteMemtableWritecache());
    configuration.magmaCfg.LSDFragmentationRatio =
            configuration.getMagmaDeleteFragRatio();
    configuration.magmaCfg.MaxCheckpoints =
            configuration.getMagmaMaxCheckpoints();
    configuration.magmaCfg.CheckpointCreationInterval =
            configuration.getMagmaCheckpointInterval();
    configuration.magmaCfg.CheckpointCreationThreshold =
            configuration.getMagmaCheckpointThreshold();
    configuration.magmaCfg.MinCheckpointCreationInterval =
            configuration.getMagmaMinCheckpointInterval();
    configuration.magmaCfg.HeartbeatInterval =
            configuration.getMagmaHeartbeatInterval();
    configuration.magmaCfg.MaxWriteCacheSize =
            configuration.getMagmaMaxWriteCache();
    configuration.magmaCfg.MinValueBlockSizeThreshold =
            configuration.getMagmaMinValueBlockSizeThreshold();
    configuration.magmaCfg.WALBufferSize = gsl::narrow_cast<uint32_t>(
            configuration.getMagmaInitialWalBufferSize());
    configuration.magmaCfg.EnableWAL = configuration.getMagmaEnableWAL();
    configuration.magmaCfg.EnableMemoryOptimizedWrites =
            configuration.getMagmaEnableMemoryOptimizedWrites();
    configuration.magmaCfg.EnableGroupCommit =
            configuration.getMagmaEnableGroupCommit();
    configuration.magmaCfg.GroupCommitMaxSyncWaitDuration =
            configuration.getMagmaGroupCommitMaxSyncWaitDuration();
    configuration.magmaCfg.GroupCommitMaxTransactionCount =
            configuration.getMagmaGroupCommitMaxTransactionCount();
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
    configuration.magmaCfg.GetHistoryTimestamp = magmakv::getHistoryTimeStamp;
    configuration.magmaCfg.GetHistoryTimeNow = [this](Magma::KVStoreID kvid) {
        return magmakv::getHistoryTimeNow(currEngine, kvid);
    };
    configuration.magmaCfg.GetHistoryModeFromMeta =
            magmakv::getHistoryModeFromMeta;
    configuration.magmaCfg.GetEncryptionKey =
            [encryptionKeyProvider](
                    auto id) -> std::optional<cb::crypto::DataEncryptionKey> {
        if (!encryptionKeyProvider) {
            cb::UseArenaMallocSecondaryDomain domainGuard;
            if (id.empty()) {
                return cb::crypto::DataEncryptionKey{};
            }
            return {};
        }

        cb::UseArenaMallocPrimaryDomain primaryDomainGuard;
        auto key = encryptionKeyProvider->lookup(id);
        {
            cb::UseArenaMallocSecondaryDomain secondaryDomainGuard;
            if (key) {
                return *key;
            }
            if (id.empty()) {
                return cb::crypto::DataEncryptionKey{};
            }
            return {};
        }
    };
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
    configuration.magmaCfg.MemoryQuotaLowWaterMarkRatio =
            configuration.getMagmaMemoryQuotaLowWaterMarkRatio();
    configuration.magmaCfg.CompressionAlgo =
            configuration.getMagmaIndexCompressionAlgoString();
    configuration.magmaCfg.DataCompressionAlgo =
            configuration.getMagmaDataCompressionAlgoString();
    configuration.magmaCfg.SeqTreeBlockSize =
            configuration.getMagmaSeqTreeDataBlockSize();
    configuration.magmaCfg.SeqTreeIndexBlockSize =
            configuration.getMagmaSeqTreeIndexBlockSize();
    configuration.magmaCfg.KeyTreeBlockSize =
            configuration.getMagmaKeyTreeDataBlockSize();
    configuration.magmaCfg.KeyTreeIndexBlockSize =
            configuration.getMagmaKeyTreeIndexBlockSize();
    configuration.magmaCfg.FusionLogStoreURI =
            configuration.getFusionLogstoreURI();
    configuration.magmaCfg.FusionMetadataStoreURI =
            configuration.getFusionMetadatastoreURI();
    configuration.magmaCfg.FusionNamespace = configuration.getFusionNamespace();

    configuration.setStore(this);

    // To save memory only allocate counters for the number of vBuckets that
    // this shard will have to deal with
    auto cacheSize = getCacheSize();
    cachedVBStates.resize(cacheSize);
    inTransaction = std::vector<std::atomic_bool>(cacheSize);
    kvstoreRevList.resize(cacheSize, Monotonic<uint32_t>(0));

    useUpsertForSet = configuration.getMagmaEnableUpsert();
    if (useUpsertForSet) {
        configuration.magmaCfg.EnableUpdateStatusForSet = false;
    } else {
        configuration.magmaCfg.EnableUpdateStatusForSet = true;
    }

    doSyncEveryBatch = configuration.getMagmaSyncEveryBatch();

    // The execution environment is the current engine creating the KVStore and
    // magma must track in the Secondary MemoryDomain.
    // If currEngine is null, which can happen with some tests, that is okay
    // because a null currEngine means we are operating in a global environment.
    configuration.magmaCfg.ExecutionEnv = {currEngine,
                                           int(cb::MemoryDomain::Secondary)};

    configuration.magmaCfg.SwitchExecutionEnvFunc =
            [](std::pair<void*, int> env) -> std::pair<void*, int> {
        Expects(env.second <= int(cb::MemoryDomain::None));
        auto eng = static_cast<EventuallyPersistentEngine*>(env.first);
        auto oldState = ObjectRegistry::switchToEngine(
                eng, true, cb::MemoryDomain(env.second));
        return {oldState.first, int(oldState.second)};
    };

    configuration.magmaCfg.MakeCompactionCallback =
            [&](const Magma::KVStoreID kvID) {
                return std::make_unique<MagmaKVStore::MagmaCompactionCB>(
                        *this, Vbid(kvID));
            };

    configuration.magmaCfg.MakeUserStats = []() {
        return std::make_unique<MagmaDbStats>();
    };

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
    configuration.magmaCfg.LogContext = logger->getSpdLogger();
    configuration.magmaCfg.UID = loggerName;

    try {
        initialize(encryptionKeyProvider, chronicleAuthToken);
    } catch (...) {
        // If the constructor fails, the destructor does not run.
        // Ensure that the logger is destroyed.
        logger->unregister();
        throw;
    }
}

void MagmaKVStore::initialize(EncryptionKeyProvider* encryptionKeyProvider,
                              std::string_view chronicleAuthToken) {
    magma = std::make_unique<MagmaMemoryTrackingProxy>(configuration.magmaCfg);

    magma->SetMaxOpenFiles(configuration.getMaxFileDescriptors());
    setMaxDataSize(configuration.getBucketQuota());
    setMagmaFragmentationPercentage(
            configuration.getMagmaFragmentationPercentage());
    calculateAndSetMagmaThreads();

    // MB-55533: These must be set before calling Open
    setHistoryRetentionSeconds(configuration.getHistoryRetentionTime());
    setHistoryRetentionBytes(configuration.getHistoryRetentionSize(),
                             configuration.getMaxVBuckets());

    setContinuousBackupInterval(configuration.getContinousBackupInterval());

    if (configuration.isContinousBackupEnabled()) {
        magma->DisableHistoryEviction();
        historyEvictionPaused = true;
    }

    magma->SetFusionMetadataStoreAuthToken(std::string(chronicleAuthToken));

    // Open the magma instance for this shard and populate the vbstate.
    auto status = magma->Open();

    if (status.ErrorCode() == Status::Code::DiskFull) {
        // Magma construction needs to recover and replay all WAL ops to provide
        // us a consistent state. Magma may require disk space to do so. We
        // are required to provide a read only view of the data for recovery
        // should a disk become unwriteable. To accomplish this magma has a
        // read only mode that be constructed to give us a consistent view of
        // data without performing recovery. This read only instance cannot be
        // written to so it can only be used in an emergency should the disk be
        // full.
        //
        // It is worth noting that nothing in kv_engine will modify this magma
        // instance to make it writeable should the disk stop being full. A
        // restart is required. We /could/ write code to swap the instance but
        // it's probably not going to be trivial and we don't expect to run out
        // of disk space very often so it's of questionable worth.
        logger->warn(
                "MagmaKVStore::MagmaKVStore: opening in read only mode as "
                "magma reported a disk full error");
        configuration.setReadOnly(true);
        magma = std::make_unique<MagmaMemoryTrackingProxy>(
                configuration.magmaCfg);
        status = magma->Open();
    }

    if (!status) {
        if (status.ErrorCode() == Status::Code::EncryptionKeyNotFound) {
            throw cb::engine_error(
                    cb::engine_errc::encryption_key_not_available,
                    "MagmaKVStore::initialize()");
        }
        std::string err =
                "MagmaKVStore Magma open failed. Status:" + status.String();
        logger->critical(err);

        throw std::logic_error(err);
    }

    magma->executeOnKVStoreList(
            [this](const std::vector<magma::Magma::KVStoreID>& kvstores) {
                cb::UseArenaMallocPrimaryDomain domainGuard;
                for (auto kvid : kvstores) {
                    auto status = loadVBStateCache(Vbid(kvid), true);
                    ++st.numLoadedVb;
                    if (status != ReadVBStateStatus::Success &&
                        status != ReadVBStateStatus::NotFound) {
                        throw std::logic_error("MagmaKVStore vbstate vbid:" +
                                               std::to_string(kvid) +
                                               " not found."
                                               " Status:" +
                                               to_string(status));
                    }
                }
            });

    if (encryptionKeyProvider) {
        encryptionKeyProvider->addListener(
                [this](const auto& ks) { magma->setActiveEncryptionKeys(ks); });
    }

    magma->SetFusionUploadInterval(configuration.getFusionUploadInterval());
    magma->SetFusionLogCheckpointInterval(
            configuration.getFusionLogCheckpointInterval());
    magma->SetFusionLogstoreFragmentationThreshold(
            configuration.getFusionLogstoreFragmentationThreshold());
}

MagmaKVStore::~MagmaKVStore() {
    logger->unregister();
}

void MagmaKVStore::deinitialize() {
    logger->debug("MagmaKVStore: {} deinitializing",
                  configuration.getShardId());

    // Close shuts down all of the magma background threads (compaction is the
    // one that we care about here). The compaction callbacks require the magma
    // instance to exist so we must do this before we reset it.
    magma->Close();

    // Flusher should have already been stopped so it should be safe to destroy
    // the magma instance now
    magma.reset();

    logger->debug("MagmaKVStore: {} deinitialized", configuration.getShardId());
}

bool MagmaKVStore::pause() {
    logger->infoWithContext("MagmaKVStore:pause()",
                            {{"shard", configuration.getShardId()}});
    auto status = magma->Pause();
    if (!status.IsOK()) {
        logger->warnWithContext("MagmaKVStore::pause() ailed",
                                {{"shard", configuration.getShardId()},
                                 {"error", status.String()}});
        return false;
    }
    return true;
}

void MagmaKVStore::resume() {
    logger->infoWithContext("MagmaKVStore:resume()",
                            {{"shard", configuration.getShardId()}});
    magma->Resume();
}

void MagmaKVStore::postVBStateFlush(Vbid vbid,
                                    const vbucket_state& committedState) {
    const auto newState = committedState.transition.state;
    const auto isBackupEnabled = configuration.isContinousBackupEnabled() &&
                                 configuration.isHistoryRetentionEnabled();

    auto& backupStatus = continuousBackupStatus[getCacheSlot(vbid)];

    const auto startBackup = backupStatus == BackupStatus::Stopped &&
                             newState == vbucket_state_active &&
                             isBackupEnabled;

    const auto stopBackup =
            backupStatus == BackupStatus::Started &&
            (newState != vbucket_state_active || !isBackupEnabled);

    if (startBackup) {
        const auto backupPath = getContinuousBackupPath(vbid, committedState);
        // The directory path will depend on the vbucket state. Magma wants the
        // path to be present when we call StartBackup().
        std::error_code ec;
        if (std::filesystem::create_directories(backupPath, ec); ec) {
            logger->logWithContext(
                    spdlog::level::warn,
                    "Failed to create continuous backup directory",
                    {{"backup_path", backupPath.generic_string()},
                     {"error", ec.message()}});
        }

        logger->logWithContext(spdlog::level::info,
                               "Starting continuous backup",
                               {{"vb", vbid},
                                {"vb_state", VBucket::toString(newState)},
                                {"backup_path", backupPath.generic_string()}});
        auto status =
                magma->StartBackup(vbid.get(), backupPath.generic_string());
        if (!status) {
            logger->logWithContext(spdlog::level::warn,
                                   "Failed to start continuous backup",
                                   {{"vb", vbid}, {"error", status.String()}});
            return;
        }
        backupStatus = BackupStatus::Started;
    } else if (stopBackup) {
        logger->logWithContext(
                spdlog::level::info,
                "Stopping continuous backup",
                {{"vb", vbid}, {"vb_state", VBucket::toString(newState)}});
        auto status = magma->StopBackup(vbid.get());
        if (!status) {
            logger->logWithContext(spdlog::level::warn,
                                   "Failed to stop continuous backup",
                                   {{"vb", vbid}, {"error", status.String()}});
            return;
        }
        backupStatus = BackupStatus::Stopped;
    }
}

bool MagmaKVStore::commit(std::unique_ptr<TransactionContext> txnCtx,
                          VB::Commit& commitData) {
    checkIfInTransaction(txnCtx->vbid, "MagmaKVStore::commit");

    auto& ctx = dynamic_cast<MagmaKVStoreTransactionContext&>(*txnCtx);
    if (ctx.pendingReqs.empty()) {
        return true;
    }

    kvstats_ctx kvctx(commitData);
    bool success = true;

    preFlushHook();

    // Flush all documents to disk
    auto errCode = saveDocs(
            ctx, commitData, kvctx, toHistoryMode(commitData.historical));
    if (errCode != static_cast<int>(cb::engine_errc::success)) {
        logger->warn("MagmaKVStore::commit: saveDocs {} errCode:{}",
                     txnCtx->vbid,
                     errCode);
        success = false;
    }

    postFlushHook();

    commitCallback(ctx, errCode, kvctx);

    // This behaviour is to replicate the one in Couchstore.
    // Set `in_transanction = false` only if `commit` is successful.
    if (success) {
        updateCachedVBState(txnCtx->vbid, commitData.proposedVBState);
        postVBStateFlush(txnCtx->vbid, commitData.proposedVBState);
    }

    logger->TRACE("MagmaKVStore::commit success:{}", success);

    return success;
}

void MagmaKVStore::commitCallback(MagmaKVStoreTransactionContext& txnCtx,
                                  int errCode,
                                  kvstats_ctx&) {
    const auto flushSuccess = (errCode == Status::Code::Ok);
    for (const auto& reqPtr : txnCtx.pendingReqs) {
        const auto& req = *reqPtr;
        size_t mutationSize = req.getRawKeyLen() + req.getBodySize() +
                              req.getDocMeta().size();
        st.io_num_write++;
        st.io_document_write_bytes += mutationSize;

        if (req.isDelete()) {
            FlushStateDeletion state;
            if (flushSuccess) {
                if (req.isLogicalInsert()) {
                    // Deletion is for an item that exists on disk but logically
                    // does not (i.e. the old reviison belongs to a collection
                    // that has been dropped).
                    state = FlushStateDeletion::LogicallyDocNotFound;
                } else if (req.isOldItemAlive()) {
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
                        txnCtx.vbid,
                        cb::UserData(req.getKey().to_string()),
                        errCode,
                        state);
            }

            txnCtx.deleteCallback(req.getItem(), state);
        } else {
            FlushStateMutation state;
            if (flushSuccess) {
                if (req.isLogicalInsert()) {
                    // Mutation is for an item that exists on disk but logically
                    // does not (i.e. the old reviison belongs to a collection
                    // that has been dropped).
                    state = FlushStateMutation::LogicalInsert;
                } else if (req.isOldItemAlive()) {
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
                        txnCtx.vbid,
                        cb::UserData(req.getKey().to_string()),
                        errCode,
                        state);
            }

            txnCtx.setCallback(req.getItem(), state);
        }
    }
}

StorageProperties MagmaKVStore::getStorageProperties() const {
    StorageProperties rv(StorageProperties::ByIdScan::Yes,
                         // @TODO MB-33784 Enable auto de-dupe if/when magma
                         // supports it
                         StorageProperties::AutomaticDeduplication::No,
                         StorageProperties::PrepareCounting::No,
                         StorageProperties::CompactionStaleItemCallbacks::Yes,
                         StorageProperties::HistoryRetentionAvailable::Yes,
                         StorageProperties::ContinuousBackupAvailable::Yes,
                         StorageProperties::BloomFilterAvailable::Yes,
                         StorageProperties::Fusion::Yes);
    return rv;
}

void MagmaKVStore::setMaxDataSize(size_t size) {
    configuration.setBucketQuota(size);
    const size_t memoryQuota = (size / configuration.getMaxShards()) *
                               configuration.getMagmaMemQuotaRatio();
    magma->SetMemoryQuota(memoryQuota);
}

std::variant<cb::engine_errc, std::unordered_set<std::string>>
MagmaKVStore::getEncryptionKeyIds() const {
    return magma->getEncryptionKeyIds();
}

// Note: This routine is only called during warmup. The caller
// can not make changes to the vbstate or it would cause race conditions.
std::vector<vbucket_state*> MagmaKVStore::listPersistedVbuckets() {
    std::vector<vbucket_state*> result;
    for (size_t slot = 0; slot < cachedVBStates.size(); slot++) {
        const auto& vb = cachedVBStates[slot];
        if (vb) {
            auto id = gsl::narrow_cast<uint16_t>(
                    (configuration.getMaxShards() * slot) +
                    configuration.getShardId());
            mergeMagmaDbStatsIntoVBState(*vb, Vbid{id});
        }
        result.emplace_back(vb.get());
    }
    return result;
}

void MagmaKVStore::completeLoadingVBuckets() {
    KVStore::completeLoadingVBuckets();
    if (historyEvictionPaused.exchange(false)) {
        magma->EnableHistoryEviction();
    }
}

void MagmaKVStore::set(TransactionContext& txnCtx, queued_item item) {
    checkIfInTransaction(txnCtx.vbid, "MagmaKVStore::set");
    if (configuration.isSanityCheckingVBucketMapping()) {
        validateKeyMapping("MagmaKVStore::set",
                           configuration.getVBucketMappingErrorHandlingMethod(),
                           item->getKey(),
                           item->getVBucketId(),
                           configuration.getMaxVBuckets());
    }

    auto& ctx = static_cast<MagmaKVStoreTransactionContext&>(txnCtx);
    ctx.pendingReqs.emplace_back(
            std::make_unique<MagmaRequest>(std::move(item)));
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
    DomainAwareFetchBuffer idxBuf;
    DomainAwareFetchBuffer seqBuf;

    auto start = cb::time::steady_clock::now();

    Status status = magma->Get(
            vbid.get(), keySlice, idxBuf, seqBuf, metaSlice, valueSlice);

    if (!status) {
        logger->warn("MagmaKVStore::getWithHeader {} key:{} status:{}",
                     vbid,
                     cb::UserData{makeDiskDocKey(keySlice).to_string()},
                     status.String());
        st.numGetFailure++;
        return GetValue{nullptr, magmaErr2EngineErr(status.ErrorCode())};
    }

    bool found = status.IsOkDocFound();
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

    if (!found) {
        return GetValue{nullptr, cb::engine_errc::no_such_key};
    }

    // record stats
    st.readTimeHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            cb::time::steady_clock::now() - start));
    st.readSizeHisto.add(keySlice.Len() + metaSlice.Len() + valueSlice.Len());

    ++st.io_bg_fetch_docs_read;
    st.io_bgfetch_doc_bytes +=
            keySlice.Len() + metaSlice.Len() + valueSlice.Len();

    return makeGetValue(vbid,
                        keySlice,
                        metaSlice,
                        valueSlice,
                        filter,
                        getDefaultCreateItemCallback());
}

bool MagmaKVStore::keyMayExist(Vbid vbid, const DocKeyView& key) const {
    if (key.getEncoding() == DocKeyEncodesCollectionId::Yes) {
        Slice keySlice = {reinterpret_cast<const char*>(key.data()),
                          key.size()};
        return magma->KeyMayExist(vbid.get(), keySlice);
    }
    StoredDocKey key1{key};
    Slice keySlice = {reinterpret_cast<const char*>(key1.data()), key1.size()};
    return magma->KeyMayExist(vbid.get(), keySlice);
}

using GetOperations = magma::OperationsList<Magma::GetOperation>;

void MagmaKVStore::getMulti(Vbid vbid,
                            vb_bgfetch_queue_t& itms,
                            CreateItemCB createItemCb) const {
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

    auto cb = [this, &vbid, &createItemCb](Status status,
                                           const Magma::GetOperation& op,
                                           const Slice& metaSlice,
                                           const Slice& valueSlice) {
        bool found = status.IsOkDocFound();
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
        auto errCode = magmaErr2EngineErr(status.ErrorCode());
        auto* bg_itm_ctx =
                reinterpret_cast<vb_bgfetch_item_ctx_t*>(op.UserContext);
        bg_itm_ctx->value.setStatus(errCode);
        if (found) {
            bg_itm_ctx->value = makeGetValue(vbid,
                                             op.Key,
                                             metaSlice,
                                             valueSlice,
                                             bg_itm_ctx->getValueFilter(),
                                             createItemCb);
            GetValue* rv = &bg_itm_ctx->value;

            for (auto& fetch : bg_itm_ctx->getRequests()) {
                fetch->value = rv;
                st.readTimeHisto.add(
                        std::chrono::duration_cast<std::chrono::microseconds>(
                                cb::time::steady_clock::now() -
                                fetch->initTime));
                st.readSizeHisto.add(op.Key.Len() + valueSlice.Len());
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
            // continue scanning
            return false;
        }
        auto rv = makeGetValue(vbid,
                               keySlice,
                               metaSlice,
                               valueSlice,
                               filter,
                               getDefaultCreateItemCallback());
        cb(std::move(rv));

        // continue scanning
        return false;
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
    checkIfInTransaction(txnCtx.vbid, "MagmaKVStore::del");
    if (configuration.isSanityCheckingVBucketMapping()) {
        validateKeyMapping("MagmaKVStore::del",
                           configuration.getVBucketMappingErrorHandlingMethod(),
                           item->getKey(),
                           item->getVBucketId(),
                           configuration.getMaxVBuckets());
    }

    auto& ctx = dynamic_cast<MagmaKVStoreTransactionContext&>(txnCtx);
    ctx.pendingReqs.emplace_back(
            std::make_unique<MagmaRequest>(std::move(item)));
}

void MagmaKVStore::delVBucket(Vbid vbid,
                              std::unique_ptr<KVStoreRevision> kvstoreRev) {
    auto status = magma->DeleteKVStore(
            vbid.get(),
            static_cast<Magma::KVStoreRevision>(kvstoreRev->getRevision()));
    if (!status.IsOK()) {
        logger->warnWithContext("MagmaKVStore::delVBucket DeleteKVStore",
                                {{"vb", vbid},
                                 {"kvstoreRev", kvstoreRev->getRevision()},
                                 {"error", status.String()}});
    }
}

void MagmaKVStore::prepareToCreateImpl(Vbid vbid) {
    auto vbstate = getCachedVBucketState(vbid);
    if (vbstate) {
        vbstate->reset();
    }
    kvstoreRevList[getCacheSlot(vbid)]++;
    logger->debug("MagmaKVStore::prepareToCreateImpl {} kvstoreRev:{}",
                  vbid,
                  kvstoreRevList[getCacheSlot(vbid)]);
}

std::unique_ptr<KVStoreRevision> MagmaKVStore::prepareToDeleteImpl(Vbid vbid) {
    auto& backupStatus = continuousBackupStatus[getCacheSlot(vbid)];
    if (backupStatus == BackupStatus::Started) {
        // Stopping the backup will force a backup to be created before the
        // vBucket is deleted. This is necessary to support the case of
        // restoring to just before bucket flush.
        auto status = magma->StopBackup(vbid.get());
        if (!status) {
            // We should not treat the failure here as fatal, log and continue
            // with deleting the vbucket.
            logger->logWithContext(spdlog::level::warn,
                                   "Failed to stop continuous backup before "
                                   "deleting vbucket",
                                   {{"vb", vbid}, {"error", status.String()}});
        }
        // Reset the continuous backup status for the vbid as we are deleting
        // this vbucket and new vbucket with this vbid should start with stopped
        // status.
        backupStatus = BackupStatus::Stopped;
    }

    auto [status, kvsRev] = magma->GetKVStoreRevision(vbid.get());
    if (status) {
        return std::make_unique<KVStoreRevision>(kvsRev);
    }

    // Even though we couldn't get the kvstore revision from magma, we'll use
    // what is in kv engine and assume its the latest. We might not be able to
    // get the revision of the KVStore if we've not persited any documents yet
    // for this vbid.
    auto rev = kvstoreRevList[getCacheSlot(vbid)];
    logger->infoWithContext(
            "MagmaKVStore::prepareToDeleteImpl: magma didn't find kvstore for "
            "getRevision. Using cached revision",
            {{"vb", vbid},
             {"error", status.String()},
             {"cached_revision", rev.load()}});

    return std::make_unique<KVStoreRevision>(rev);
}

/**
 * Magma specific RollbackCtx which resumes background (implicit) compactions
 * for the given vBucket on destruction
 */
class MagmaRollbackCtx : public RollbackCtx {
public:
    MagmaRollbackCtx(MagmaKVStore& kvstore, Vbid vbid)
        : kvstore(kvstore), vbid(vbid) {
    }

    ~MagmaRollbackCtx() override {
        kvstore.resumeImplicitCompaction(vbid);
    }

protected:
    MagmaKVStore& kvstore;
    Vbid vbid;
};

std::unique_ptr<RollbackCtx> MagmaKVStore::prepareToRollback(Vbid vbid) {
    // Before we lock the vBucket state lock we need to inhibit magma's
    // background compactions to avoid a potential deadlock. Magma rollback
    // needs to wait for compactions to finish and compactions may, if they are
    // expiring an item, need to acquire the vBucket state lock for the duration
    // of that operation. That could cause a deadlock so we'll halt background
    // compactions before we take the vBucket state lock. The compaction should
    // not take the vbsetMutex or the vBucket lock so we can safely lock those
    // first. When the ctx object goes out of scope it will re-enable background
    // compactions for the vBucket.
    magma->StopBGCompaction(vbid.get());
    return std::make_unique<MagmaRollbackCtx>(*this, vbid);
}

void MagmaKVStore::resumeImplicitCompaction(Vbid vbid) {
    magma->ResumeBGCompaction(vbid.get());
}

// Note: It is assumed this can only be called from bg flusher thread or
// there will be issues with writes coming from multiple threads.
bool MagmaKVStore::snapshotVBucket(Vbid vbid, const VB::Commit& meta) {
    if (logger->should_log(spdlog::level::TRACE)) {
        logger->TRACE("MagmaKVStore::snapshotVBucket {} newVBState:{}",
                      vbid,
                      encodeVBState(meta.proposedVBState));
    }

    if (!needsToBePersisted(vbid, meta.proposedVBState)) {
        postVBStateFlush(vbid, meta.proposedVBState);
        return true;
    }

    auto start = cb::time::steady_clock::now();

    if (!writeVBStateToDisk(vbid, meta)) {
        return false;
    }

    updateCachedVBState(vbid, meta.proposedVBState);
    postVBStateFlush(vbid, meta.proposedVBState);

    st.snapshotHisto.add(std::chrono::duration_cast<std::chrono::microseconds>(
            cb::time::steady_clock::now() - start));

    return true;
}

GetValue MagmaKVStore::makeItem(Vbid vb,
                                const Slice& keySlice,
                                const Slice& metaSlice,
                                const Slice& valueSlice,
                                ValueFilter filter,
                                CreateItemCB createItemCb) const {
    auto key = makeDiskDocKey(keySlice);
    const auto meta = magmakv::getDocMeta(metaSlice);

    const bool forceValueFetch = isDocumentPotentiallyCorruptedByMB52793(
            meta.isDeleted(), meta.getDatatype());

    const bool includeValue = filter != ValueFilter::KEYS_ONLY ||
                              key.getDocKey().isInSystemEventCollection();

    size_t nbytes = 0;
    // Only create the body for the value filter and when a valueSlice is given
    if ((includeValue || forceValueFetch) && valueSlice.Data()) {
        nbytes = valueSlice.Len();
    }

    auto [status, item] = createItemCb(key.getDocKey(),
                                       nbytes,
                                       meta.getFlags(),
                                       meta.getExptime(),
                                       value_t(),
                                       meta.getDatatype(),
                                       meta.getCas(),
                                       meta.getBySeqno(),
                                       vb,
                                       meta.getRevSeqno());
    if (status != cb::engine_errc::success) {
        return GetValue(nullptr, status);
    }

    // Blob creation is deferred to avoid exceeding quota.
    if ((includeValue || forceValueFetch) && valueSlice.Data()) {
        item->replaceValue(TaggedPtr<Blob>(
                Blob::New(valueSlice.Data(), meta.getValueSize()),
                TaggedPtrBase::NoTagValue));
    }

    if (filter != ValueFilter::KEYS_ONLY) {
        checkAndFixKVStoreCreatedItem(*item);
    }
    if (filter == ValueFilter::VALUES_DECOMPRESSED) {
        // Decompressed values requested, but the value is compressed.
        // Attempt to decompress the value.
        if (!item->decompressValue()) {
            logger->warn(
                    "MagmaKVStore::makeItem failed to decompress value "
                    "vbid{} "
                    "key:{} "
                    "seqno:{} ",
                    "datatype:{} ",
                    vb,
                    cb::UserData{key.getDocKey().to_string()},
                    meta.getBySeqno(),
                    meta.getDatatype());
        }
    }

    if (meta.isDeleted()) {
        item->setDeleted(meta.getDeleteSource());
    }

    if (magmakv::isPrepared(keySlice, meta)) {
        auto level =
                static_cast<cb::durability::Level>(meta.getDurabilityLevel());
        item->setPendingSyncWrite({level, cb::durability::Timeout::Infinity()});
        if (meta.isSyncDelete()) {
            item->setDeleted(DeleteSource::Explicit);
        }
    } else if (magmakv::isAbort(keySlice, meta)) {
        item->setAbortSyncWrite();
        item->setPrepareSeqno(meta.getPrepareSeqno());
    }

    if (item->getValue()) {
        // function shared with KEY_ONLY creation paths, so only sanitize when
        // a body was created
        checkAndFixKVStoreCreatedItem(*item);
    }

    return GetValue(std::move(item), status);
}

GetValue MagmaKVStore::makeGetValue(Vbid vb,
                                    const Slice& keySlice,
                                    const Slice& metaSlice,
                                    const Slice& valueSlice,
                                    ValueFilter filter,
                                    CreateItemCB createItemCb) const {
    return makeItem(vb,
                    keySlice,
                    metaSlice,
                    valueSlice,
                    filter,
                    std::move(createItemCb));
}

int MagmaKVStore::saveDocs(MagmaKVStoreTransactionContext& txnCtx,
                           VB::Commit& commitData,
                           kvstats_ctx& kvctx,
                           magma::Magma::HistoryMode historyMode) {
    uint64_t ninserts = 0;
    uint64_t ndeletes = 0;

    auto vbid = txnCtx.vbid;

    auto writeDocsCB = [this, &commitData, &kvctx, &ninserts, &ndeletes, vbid](
                               const Magma::WriteOperation& op,
                               const bool docExists,
                               const magma::Slice oldMeta) {
        auto req = reinterpret_cast<MagmaRequest *>(op.UserData);
        auto diskDocKey = makeDiskDocKey(op.Key);
        auto docKey = diskDocKey.getDocKey();
        size_t docSize = req->getRawKeyLen() + op.Meta.Len() +
                         configuration.magmaCfg.GetValueSize(op.Meta);

        const bool isTombstone =
                docExists && configuration.magmaCfg.IsTombstone(oldMeta);

        if (logger->should_log(spdlog::level::TRACE)) {
            logger->TRACE(
                    "MagmaKVStore::writeDocsCB {} key:{} seqno:{} delete:{} "
                    "docExists:{} tombstone:{}",
                    vbid,
                    cb::UserData{diskDocKey.to_string()},
                    magmakv::getDocMeta(req->getDocMeta()).getBySeqno(),
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
            size_t oldDocSize = 0;
            using namespace Collections::VB;
            FlushAccounting::UpdateStatsResult res;
            if (docExists) {
                auto oldIsDeleted =
                        isTombstone ? IsDeleted::Yes : IsDeleted::No;
                oldDocSize = req->getRawKeyLen() + oldMeta.Len() +
                             configuration.magmaCfg.GetValueSize(oldMeta);
                res = commitData.collections.updateStats(
                        docKey,
                        magmakv::getDocMeta(req->getDocMeta()).getBySeqno(),
                        isCommitted,
                        isDeleted,
                        docSize,
                        configuration.magmaCfg.GetSeqNum(oldMeta),
                        oldIsDeleted,
                        oldDocSize,
                        CompactionCallbacks::AnyRevision);
                if (res.logicalInsert) {
                    req->markLogicalInsert();
                }
            } else {
                res.newDocReflectedInDiskSize =
                        commitData.collections.updateStats(
                                docKey,
                                magmakv::getDocMeta(req->getDocMeta())
                                        .getBySeqno(),
                                isCommitted,
                                isDeleted,
                                docSize,
                                CompactionCallbacks::AnyRevision);
            }
            if (res.newDocReflectedInDiskSize) {
                // note that this operation has updated the per collection
                // disk size.
                // If compression later changes the size of the stored
                // value, then the disk size needs fixing up.
                // Note that compression isn't applied _before_ this point
                // to avoid holding compressed and uncompressed versions of
                // documents for the duration of the flush batch.
                // instead, each is compressed individually before being
                // written.
                req->markNewDocReflectedInDiskSize();
            }
            updateStatsHook(*req, oldDocSize);
        } else {
            // Tell Collections::Flush that it may need to record this seqno
            commitData.collections.maybeUpdatePersistedHighSeqno(
                    docKey,
                    magmakv::getDocMeta(req->getDocMeta()).getBySeqno(),
                    req->isDelete());
        }

        if (docExists) {
            // Storing a delete of a previous live document has no affect on
            // item count
            if (req->isLogicalInsert() && req->isDelete()) {
                return;
            }

            if (!isTombstone) {
                req->markOldItemAlive();
            }

            if (isTombstone) {
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
            } else if (req->isLogicalInsert()) {
                ninserts++;
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
    WriteOps postWriteOps;
    int64_t lastSeqno = 0;

    auto beginTime = cb::time::steady_clock::now();
    std::chrono::microseconds saveDocsDuration;

    auto postWriteDocsCB =
            [this,
             &commitData,
             &localDbReqs,
             &lastSeqno,
             &vbid,
             &ninserts,
             &ndeletes,
             &magmaDbStats,
             &beginTime,
             &saveDocsDuration,
             &postWriteOps]() -> std::pair<Status, Magma::WriteOpsCPtr> {

        auto& vbstate = commitData.proposedVBState;
        vbstate.highSeqno = lastSeqno;

        magmaDbStats.docCount = ninserts - ndeletes;
        // Don't update UserStats highSeqno if it has not changed
        if (vbstate.highSeqno > magmaDbStats.highSeqno) {
            magmaDbStats.highSeqno = vbstate.highSeqno;
        }

        // If present (>0) set the purgeSeqno
        if (commitData.purgeSeqno) {
            magmaDbStats.purgeSeqno = commitData.purgeSeqno;
        }

        // @todo: Magma doesn't track onDiskPrepares
        // @todo MB-42900: Magma doesn't track onDiskPrepareBytes

        // Write out current vbstate to the CommitBatch.
        addVBStateUpdateToLocalDbReqs(
                localDbReqs, vbstate, kvstoreRevList[getCacheSlot(vbid)]);

        // We have to update the collections meta before we save the collections
        // stats because the drop of a collection will delete "alive" stats doc
        // for the collection but a drop + create in the same batch needs to
        // overwrite the original doc with the new variant. So, we delete it
        // here then recreate it below in that case.
        if (commitData.collections.isReadyForCommit()) {
            auto status = updateCollectionsMeta(
                    vbid, localDbReqs, commitData.collections, magmaDbStats);
            if (!status.IsOK()) {
                logger->warn(
                        "MagmaKVStore::saveDocs {} Failed to set "
                        "collections meta, got status {}",
                        vbid,
                        status.String());
                return {status, nullptr};
            }
        }

        addStatUpdateToWriteOps(magmaDbStats, postWriteOps);

        commitData.collections.saveCollectionStats(
                vbid,
                [this, &localDbReqs](
                        CollectionID cid,
                        const Collections::VB::PersistedStats& stats) {
                    saveCollectionStats(localDbReqs, cid, stats);
                });

        addLocalDbReqs(localDbReqs, postWriteOps);
        auto now = cb::time::steady_clock::now();
        saveDocsDuration =
                std::chrono::duration_cast<std::chrono::microseconds>(
                        now - beginTime);
        beginTime = now;
        return {Status::OK(), &postWriteOps};
    };

    auto& ctx = dynamic_cast<MagmaKVStoreTransactionContext&>(txnCtx);

    ctx.preparePendingRequests(historyMode);

    // Vector of updates to be written to the data store.
    WriteOps writeOps;
    writeOps.reserve(ctx.pendingReqs.size());

    // TODO: Replace writeOps with Magma::WriteOperations when it
    // becomes available. This will allow us to pass pendingReqs
    // in and create the WriteOperation from the pendingReqs queue.
    for (auto& reqPtr : ctx.pendingReqs) {
        auto& req = *reqPtr;
        Slice valSlice{req.getBodyData(), req.getBodySize()};

        auto docMeta = req.getDocMeta();
        auto decodedMeta = magmakv::getDocMeta(docMeta);
        if (decodedMeta.getBySeqno() > lastSeqno) {
            lastSeqno = decodedMeta.getBySeqno();
        }

        switch (commitData.writeOp) {
        case WriteOperation::Insert:
            writeOps.emplace_back(Magma::WriteOperation::NewDocInsert(
                    {req.getRawKey(), req.getRawKeyLen()},
                    {docMeta.data(), docMeta.size()},
                    valSlice,
                    &req));
            break;
        case WriteOperation::Upsert:
            writeOps.emplace_back(Magma::WriteOperation::NewDocUpsert(
                    {req.getRawKey(), req.getRawKeyLen()},
                    {docMeta.data(), docMeta.size()},
                    valSlice,
                    &req));
            break;
        }
    }

    // If dropped collections exists, read the dropped collections metadata
    // so the flusher can do the correct stat calculations. This is
    // conditional as (trying) to read this data slows down saveDocs.
    if (commitData.collections.droppedCollectionsExists()) {
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
    }

    // WriteOperations are non-owning, so temporary storage is required for
    // compressed values and updated meta, for use in per-document compression.
    // Must live for the duration of the WriteDocs call.
    // This storage will be reused for each document that requires compression.
    std::string newMeta;
    cb::compression::Buffer newValueBuffer;
    Magma::WriteOperation prevResult;

    // callback which _may_ apply compression to each written document
    std::function<bool(const Magma::WriteOperation& op,
                       Magma::WriteOperation& result)>
            compressCB;

    if (configuration.isPerDocumentCompressionEnabled()) {
        // compression buffers aren't copyable (and std::function requires
        // the lambda be copy constructable), otherwise a capture
        // initialiser could be used.
        compressCB = [&prevResult, &commitData, &newMeta, &newValueBuffer](
                const Magma::WriteOperation &op,
                Magma::WriteOperation &result) {
            // If the same operation as last time is being transformed, return
            // the previous result as-is.
            auto oldSeqno = prevResult.Meta.Len()
                            ? magmakv::getSeqNum(prevResult.Meta)
                            : 0;
            if (oldSeqno == magmakv::getSeqNum(op.Meta)) {
                result = prevResult;
                return !magmakv::isCompressed(magmakv::getDocMeta(op.Meta)) &&
                       magmakv::isCompressed(magmakv::getDocMeta(result.Meta));
            }

            bool isCompressed = MagmaKVStore::maybeCompressValue(
                    commitData, newMeta, newValueBuffer, op, result);

            prevResult = result;
            return isCompressed;
        };
    }

    auto status = magma->WriteDocs(vbid.get(),
                                   writeOps,
                                   kvstoreRevList[getCacheSlot(vbid)],
                                   historyMode,
                                   writeDocsCB,
                                   postWriteDocsCB,
                                   std::move(compressCB));

    saveDocsPostWriteDocsHook();

    if (status) {
        st.saveDocsHisto.add(saveDocsDuration);
        kvctx.commitData.collections.postCommitMakeStatsVisible();

        // Commit duration can be approximately calculated as the time taken
        // between post writedocs callback and now
        st.commitHisto.add(
                std::chrono::duration_cast<std::chrono::microseconds>(
                        cb::time::steady_clock::now() - beginTime));

        st.batchSize.add(ctx.pendingReqs.size());
        st.docsCommitted = ctx.pendingReqs.size();
    } else {
        logger->critical(
                "MagmaKVStore::saveDocs {} WriteDocs failed. Status:{}",
                vbid,
                status.String());
    }

    // Only used for unit testing
    if (doSyncEveryBatch) {
        auto chkStatus = magma->Sync(true, true /*fusion*/);
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

bool MagmaKVStore::maybeCompressValue(VB::Commit& commitData,
                                      std::string& newMetaStorage,
                                      cb::compression::Buffer& newValueStorage,
                                      const Magma::WriteOperation& op,
                                      Magma::WriteOperation& result) {
    auto meta = magmakv::getDocMeta(op.Meta);
    if (op.Value.Empty()) {
        // empty value, nothing to compress
        return false;
    }

    if (cb::mcbp::datatype::is_snappy(meta.getDatatype())) {
        // already compressed
        return false;
    }
    if (!cb::compression::deflateSnappy({op.Value.Data(), op.Value.Len()},
                                        newValueStorage)) {
        // compression failed
        return false;
    }

    // we've compressed the value, now we need to update the
    // output operation with the compressed value and a copy
    // of the meta with the datatype and value size altered
    meta.setDataType(meta.getDatatype() | uint8_t(cb::mcbp::Datatype::Snappy));
    meta.setValueSize(gsl::narrow<uint32_t>(newValueStorage.size()));
    newMetaStorage = meta.encode();
    // copy over the existing operation (doesn't deep copy
    // any slices)
    result = op;
    // now point the meta and value to the temporary buffers
    result.Meta = newMetaStorage;
    result.Value = std::string_view(newValueStorage);

    return true;
}

/**
 * MagmaScanContext is BySeqnoScanContext with the magma
 * iterator added.
 */
class MagmaScanContext : public BySeqnoScanContext {
public:
    MagmaScanContext(std::unique_ptr<StatusCallback<GetValue>> cb,
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
                     const std::vector<Collections::KVStore::OpenCollection>*
                             openCollections,
                     const std::vector<Collections::KVStore::DroppedCollection>&
                             droppedCollections,
                     uint64_t historyStartSeqno)
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
                             openCollections,
                             droppedCollections,
                             historyStartSeqno) {
    }
};

std::unique_ptr<BySeqnoScanContext> MagmaKVStore::initBySeqnoScanContext(
        std::unique_ptr<StatusCallback<GetValue>> cb,
        std::unique_ptr<StatusCallback<CacheLookup>> cl,
        Vbid vbid,
        uint64_t startSeqno,
        DocumentFilter options,
        ValueFilter valOptions,
        SnapshotSource source,
        std::unique_ptr<KVFileHandle> fileHandle) const {
    auto handle = std::move(fileHandle);
    if (!handle) {
        handle = makeFileHandle(vbid);
    }

    if (!handle) {
        logger->warn(
                "MagmaKVStore::initBySeqnoScanContext {} Failed to get file "
                "handle",
                vbid);
        return nullptr;
    }

    auto& snapshot = *dynamic_cast<MagmaKVFileHandle&>(*handle).snapshot;

    auto readState = readVBStateFromDisk(vbid, snapshot);
    if (readState.status != ReadVBStateStatus::Success) {
        logger->warn(
                "MagmaKVStore::initBySeqnoScanContext {} failed to read "
                "vbstate from disk. Status:{}",
                vbid,
                to_string(readState.status));
        return nullptr;
    }

    uint64_t highSeqno = readState.state.highSeqno;
    uint64_t purgeSeqno = readState.state.purgeSeqno;
    uint64_t nDocsToRead = highSeqno - startSeqno + 1;

    auto [getDroppedStatus, dropped] = getDroppedCollections(vbid, snapshot);
    if (!getDroppedStatus) {
        logger->warn(
                "MagmaKVStore::initBySeqnoScanContext {} failed to get "
                "dropped collections from disk. Status:{}",
                vbid,
                getDroppedStatus.String());
    }

    std::vector<Collections::KVStore::OpenCollection> openCollections;
    if (source == SnapshotSource::HeadAllVersions) {
        // To correctly scan all versions, the open collections are also
        // required for correct handling of any versions of data in dropped
        // collections.
        magma::Status status;
        std::tie(status, openCollections) = getOpenCollections(vbid, snapshot);
        if (!status) {
            logger->warn(
                    "MagmaKVStore::initBySeqnoScanContext {} failed to get "
                    "open collections from disk. Status:{}",
                    vbid,
                    status.String());
        }
    }

    auto historyStartSeqno = magma->GetOldestHistorySeqno(snapshot);
    if (logger->should_log(spdlog::level::debug)) {
        logger->debug(
                "MagmaKVStore::initBySeqnoScanContext {} seqno:{} endSeqno:{}"
                " purgeSeqno:{}, historyStartSeqno:{} nDocsToRead:{}"
                " docFilter:{} valFilter:{}",
                vbid,
                startSeqno,
                highSeqno,
                purgeSeqno,
                historyStartSeqno,
                nDocsToRead,
                options,
                valOptions);
    }

    return std::make_unique<MagmaScanContext>(
            std::move(cb),
            std::move(cl),
            vbid,
            std::move(handle),
            startSeqno,
            highSeqno,
            purgeSeqno,
            options,
            valOptions,
            nDocsToRead,
            readState.state,
            source == SnapshotSource::HeadAllVersions ? &openCollections
                                                      : nullptr,
            dropped,
            historyStartSeqno);
}

/**
 * MagmaScanContext is BySeqnoScanContext with the magma
 * iterator added.
 */
class MagmaByIdScanContext : public ByIdScanContext {
public:
    MagmaByIdScanContext(
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
        : ByIdScanContext(std::move(cb),
                          std::move(cl),
                          vb,
                          std::move(handle),
                          ranges,
                          _docFilter,
                          _valFilter,
                          droppedCollections,
                          maxSeqno,
                          historyStartSeqno) {
    }
};

std::unique_ptr<ByIdScanContext> MagmaKVStore::initByIdScanContext(
        std::unique_ptr<StatusCallback<GetValue>> cb,
        std::unique_ptr<StatusCallback<CacheLookup>> cl,
        Vbid vbid,
        const std::vector<ByIdRange>& ranges,
        DocumentFilter options,
        ValueFilter valOptions,
        std::unique_ptr<KVFileHandle> handle) const {
    if (!handle) {
        handle = makeFileHandle(vbid);
    }

    if (!handle) {
        logger->warn(
                "MagmaKVStore::initByIdScanContext {} Failed makeFileHandle",
                vbid);
        return nullptr;
    }

    auto& snapshot = *dynamic_cast<MagmaKVFileHandle&>(*handle).snapshot;

    auto readState = readVBStateFromDisk(vbid, snapshot);
    if (readState.status != ReadVBStateStatus::Success) {
        logger->warn(
                "MagmaKVStore::initByIdcanContext {} Failed readVBStateFromDisk"
                " Status:{}",
                vbid,
                to_string(readState.status));
        return nullptr;
    }

    auto [getDroppedStatus, dropped] = getDroppedCollections(vbid, snapshot);
    if (!getDroppedStatus) {
        logger->warn(
                "MagmaKVStore::initByIdcanContext {} Failed "
                "getDroppedCollections Status:{}",
                vbid,
                getDroppedStatus.String());
        return nullptr;
    }

    auto historyStartSeqno = magma->GetOldestHistorySeqno(snapshot);
    logger->debugWithContext(
            "MagmaKVStore::initByIdScanContext",
            {{"vb", vbid}, {"historyStartSeqno", historyStartSeqno}});
    return std::make_unique<MagmaByIdScanContext>(std::move(cb),
                                                  std::move(cl),
                                                  vbid,
                                                  std::move(handle),
                                                  ranges,
                                                  options,
                                                  valOptions,
                                                  dropped,
                                                  readState.state.highSeqno,
                                                  historyStartSeqno);
}

ScanStatus MagmaKVStore::scan(BySeqnoScanContext& ctx) const {
    return scan(ctx, magma::Magma::SeqIterator::Mode::Snapshot);
}

ScanStatus MagmaKVStore::scanAllVersions(BySeqnoScanContext& ctx) const {
    return scan(ctx, magma::Magma::SeqIterator::Mode::History);
}

ScanStatus MagmaKVStore::scan(BySeqnoScanContext& ctx,
                              magma::Magma::SeqIterator::Mode mode) const {
    if (ctx.lastReadSeqno == ctx.maxSeqno) {
        logger->TRACE("MagmaKVStore::scan {} lastReadSeqno:{} == maxSeqno:{}",
                      ctx.vbid,
                      ctx.lastReadSeqno,
                      ctx.maxSeqno);
        return ScanStatus::Success;
    }

    auto startSeqno = ctx.startSeqno;
    if (ctx.lastReadSeqno != 0) {
        startSeqno = ctx.lastReadSeqno + 1;
    }

    auto& mctx = dynamic_cast<MagmaScanContext&>(ctx);
    auto& snapshot = *dynamic_cast<MagmaKVFileHandle&>(*mctx.handle).snapshot;
    auto itr = magma->NewSeqIterator(snapshot);
    if (!itr) {
        logger->error("MagmaKVStore::scan {} Failed to get magma seq iterator",
                      mctx.vbid);
        return ScanStatus::Failed;
    }

    for (itr->Initialize(startSeqno, ctx.maxSeqno, mode); itr->Valid();
         itr->Next()) {
        ++ctx.keysScanned;

        Slice keySlice, metaSlice, valSlice;
        uint64_t seqno;
        itr->GetRecord(keySlice, metaSlice, valSlice, seqno);
        const auto result =
                scanOne(ctx,
                        keySlice,
                        seqno,
                        metaSlice,
                        valSlice,
                        [](Slice&) -> Status {
                            throw std::runtime_error(
                                    "scan(BySeqno) tried to read the value");
                        });

        // Always update the lastReadSeqno for correct resume behaviour and
        // sane debug/logging for failure cases.
        // Note: MB-53806 made changes which mean that in a yield case this
        // seqno has been processed and any resume then will be lastReadSeqno+1
        // (as per the start of this function)
        ctx.lastReadSeqno = seqno;

        switch (result) {
        case ScanStatus::Cancelled:
        case ScanStatus::Failed:
        case ScanStatus::Yield:
            return result;
        case ScanStatus::Success:
            // scan was successful after processing the seqno and there's no
            // reason to not continue to the next seqno.
            continue;
        }
    }

    if (!itr->GetStatus().IsOK()) {
        logger->warn("MagmaKVStore::scan(BySeq) scan failed {} error:{}",
                     ctx.vbid,
                     itr->GetStatus().String());
        // Loop terminated as iterator indicates failure
        return ScanStatus::Failed;
    }

    // Loop terminated at the end of the range - success
    return ScanStatus::Success;
}

ScanStatus MagmaKVStore::scan(ByIdScanContext& ctx) const {
    // Process each range until it's completed
    for (auto& range : ctx.ranges) {
        if (range.rangeScanSuccess) {
            continue;
        }
        const auto status = scan(ctx, range);
        switch (status) {
        case ScanStatus::Success:
            // This range has been completely visited and the next range can be
            // scanned.
            range.rangeScanSuccess = true;
            continue;
        case ScanStatus::Yield:
            // Update the startKey so backfill can resume from the right
            // resume point. See how resumeFromKey is set for details.
            range.startKey = ctx.resumeFromKey;
            return status;
        case ScanStatus::Failed:
            // deeper calls log details
            logger->warn("MagmaKVStore::scan(ById) scan failed {}", ctx.vbid);
        case ScanStatus::Cancelled:
            return status;
        }
    }
    return ScanStatus::Success;
}

ScanStatus MagmaKVStore::scan(ByIdScanContext& ctx,
                              const ByIdRange& range) const {
    auto& snapshot = *dynamic_cast<MagmaKVFileHandle&>(*ctx.handle).snapshot;
    auto itr = magma->NewKeyIterator(snapshot);
    if (!itr) {
        logger->error("MagmaKVStore::scan {} Failed to get magma key iterator",
                      ctx.vbid);

        return ScanStatus::Failed;
    }

    Slice keySlice = {reinterpret_cast<const char*>(range.startKey.data()),
                      range.startKey.size()};
    for (itr->Seek(keySlice); itr->Valid(); itr->Next()) {
        ++ctx.keysScanned;

        // Read the key and check we're not outside the range
        keySlice = itr->GetKey();
        if (std::string_view(keySlice) > std::string_view(range.endKey)) {
            return ScanStatus::Success;
        }

        // ById will read the value only if the CacheLookup fails, so pass a
        // callback to get the value and an empty Slice
        uint64_t seqno = itr->GetSeqno();
        auto metaSlice = itr->GetMeta();
        const auto result = scanOne(
                ctx, keySlice, seqno, metaSlice, {}, [&itr](Slice& value) {
                    return itr->GetValue(value);
                });

        switch (result) {
        case ScanStatus::Yield:
            // Only need to set the resume point for a Yield. The resumeFromKey
            // could  be set for all cases, but this reduces copying for every
            // scanned key.
            ctx.resumeFromKey = makeDiskDocKey(keySlice);
            ctx.resumeFromKey.append(0);
            [[fallthrough]];
        case ScanStatus::Cancelled:
        case ScanStatus::Failed:
            return result;
        case ScanStatus::Success:
            // Success - scan the next key
            continue;
        }
    }

    if (!itr->GetStatus().IsOK()) {
        logger->warn("MagmaKVStore::scan(ById) scan failed {} error:{}",
                     ctx.vbid,
                     itr->GetStatus().String());
        // Loop terminated as iterator indicates failure
        return ScanStatus::Failed;
    }

    // Loop terminated at the end of the range - success
    return ScanStatus::Success;
}

ScanStatus MagmaKVStore::scanOne(
        ScanContext& ctx,
        const Slice& keySlice,
        uint64_t seqno,
        const Slice& metaSlice,
        const Slice& valSlice,
        std::function<Status(Slice&)> valueRead) const {
    if (keySlice.Len() > std::numeric_limits<uint16_t>::max()) {
        throw std::invalid_argument(
                "MagmaKVStore::scanOne: "
                "key length " +
                std::to_string(keySlice.Len()) + " > " +
                std::to_string(std::numeric_limits<uint16_t>::max()));
    }

    ctx.diskBytesRead += keySlice.Len() + metaSlice.Len() + valSlice.Len();

    CacheLookup lookup(makeDiskDocKey(keySlice), seqno, ctx.vbid);

    if (configuration.isSanityCheckingVBucketMapping()) {
        validateKeyMapping("MagmaKVStore::scanOne",
                           configuration.getVBucketMappingErrorHandlingMethod(),
                           lookup.getKey().getDocKey(),
                           ctx.vbid,
                           configuration.getMaxVBuckets());
    }

    const auto docMeta = magmakv::getDocMeta(metaSlice);
    if (docMeta.isDeleted() && ctx.docFilter == DocumentFilter::NO_DELETES) {
        if (logger->should_log(spdlog::level::TRACE)) {
            logger->TRACE(
                    "MagmaKVStore::scanOne SKIPPED(Deleted) {} key:{} "
                    "seqno:{}",
                    ctx.vbid,
                    cb::UserData{lookup.getKey().to_string()},
                    seqno);
        }
        return ScanStatus::Success;
    }

    // Determine if the key is logically deleted before trying cache/disk read
    if (ctx.docFilter != DocumentFilter::ALL_ITEMS_AND_DROPPED_COLLECTIONS) {
        if (ctx.collectionsContext.isLogicallyDeleted(
                    lookup.getKey().getDocKey(), docMeta.isDeleted(), seqno)) {
            if (logger->should_log(spdlog::level::TRACE)) {
                logger->TRACE(
                        "MagmaKVStore::scanOne SKIPPED(Collection "
                        "Deleted) {} key:{} seqno:{}",
                        ctx.vbid,
                        cb::UserData{lookup.getKey().to_string()},
                        seqno);
            }
            return ScanStatus::Success;
        }
    }

    // system collections aren't in cache so we can skip that lookup
    if (!lookup.getKey().getDocKey().isInSystemEventCollection()) {
        ctx.getCacheCallback().callback(lookup);
        if (ctx.getCacheCallback().getStatus() ==
            cb::engine_errc::key_already_exists) {
            if (logger->should_log(spdlog::level::TRACE)) {
                logger->TRACE(
                        "MagmaKVStore::scanOne "
                        "SKIPPED(cb::engine_errc::key_already_exists) {} "
                        "key:{} seqno:{}",
                        ctx.vbid,
                        cb::UserData{lookup.getKey().to_string()},
                        seqno);
            }
            return ScanStatus::Success;
        }
        if (ctx.getCacheCallback().shouldYield()) {
            if (logger->should_log(spdlog::level::TRACE)) {
                logger->TRACE(
                        "MagmaKVStore::scanOne lookup->callback {} "
                        "key:{} requested yield",
                        ctx.vbid,
                        cb::UserData{lookup.getKey().to_string()});
            }
            return ScanStatus::Yield;
        }
        if (ctx.getCacheCallback().getStatus() != cb::engine_errc::success) {
            if (logger->should_log(spdlog::level::TRACE)) {
                logger->TRACE(
                        "MagmaKVStore::scanOne lookup->callback {} "
                        "key:{} returned {} -> ScanStatus::Cancelled",
                        ctx.vbid,
                        cb::UserData{lookup.getKey().to_string()},
                        to_string(ctx.getCacheCallback().getStatus()));
            }
            return ScanStatus::Cancelled;
        }
    }

    Slice value = valSlice;
    // May need to now read the value
    if (!value.Data()) {
        if (auto status = valueRead(value); !status.IsOK()) {
            logger->warn(
                    "MagmaKVStore::scan failed to read value - {} "
                    "key:{}, seqno:{}, status:{}",
                    ctx.vbid,
                    cb::UserData{lookup.getKey().to_string()},
                    seqno,
                    status.String());
            return ScanStatus::Failed;
        }
        ctx.diskBytesRead += value.Len();
    }

    if (logger->should_log(spdlog::level::TRACE)) {
        logger->TRACE(
                "MagmaKVStore::scanOne {} key:{} seqno:{} deleted:{} "
                "expiry:{} "
                "compressed:{}",
                ctx.vbid,
                cb::UserData{lookup.getKey().to_string()},
                seqno,
                docMeta.isDeleted(),
                docMeta.getExptime(),
                magmakv::isCompressed(docMeta));
    }

    auto gv = makeItem(ctx.vbid,
                       keySlice,
                       metaSlice,
                       value,
                       ctx.valFilter,
                       getDefaultCreateItemCallback());
    Expects(gv.getStatus() == cb::engine_errc::success);

    // When we are requested to return the values as compressed AND
    // the value isn't compressed, attempt to compress the value.
    if (ctx.valFilter == ValueFilter::VALUES_COMPRESSED &&
        !magmakv::isCompressed(docMeta)) {
        if (!gv.item->compressValue()) {
            logger->warn(
                    "MagmaKVStore::scanOne failed to compress value - {} "
                    "key:{} "
                    "seqno:{}",
                    ctx.vbid,
                    cb::UserData{lookup.getKey().to_string()},
                    seqno);
            // return Failed to stop the scan and for DCP, end the stream
            return ScanStatus::Failed;
        }
    }

    if (ctx.valFilter == ValueFilter::KEYS_ONLY) {
        gv.setPartial();
    }
    ctx.getValueCallback().callback(gv);
    auto callbackStatus = ctx.getValueCallback().getStatus();
    if (callbackStatus == cb::engine_errc::success) {
        return ScanStatus::Success;
    }
    if (ctx.getValueCallback().shouldYield()) {
        if (logger->should_log(spdlog::level::TRACE)) {
            logger->TRACE(
                    "MagmaKVStore::scanOne callback {} "
                    "key:{} requested yield",
                    ctx.vbid,
                    cb::UserData{lookup.getKey().to_string()});
        }
        return ScanStatus::Yield;
    }

    if (logger->should_log(spdlog::level::TRACE)) {
        logger->TRACE(
                "MagmaKVStore::scanOne` callback {} "
                "key:{} returned {} -> Aborted ",
                ctx.vbid,
                cb::UserData{lookup.getKey().to_string()},
                to_string(callbackStatus));
    }
    return ScanStatus::Cancelled;
}

void MagmaKVStore::mergeMagmaDbStatsIntoVBState(vbucket_state& vbstate,
                                                Vbid vbid) const {
    auto dbStats = getMagmaDbStats(vbid);
    if (!dbStats) {
        // No stats available from Magma for this vbid, nothing to do.
        return;
    }
    vbstate.purgeSeqno = dbStats->purgeSeqno;

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

KVStoreIface::ReadVBStateResult MagmaKVStore::getPersistedVBucketState(
        Vbid vbid) const {
    auto state = readVBStateFromDisk(vbid);
    if (state.status != ReadVBStateStatus::Success) {
        throw std::runtime_error(
                fmt::format("MagmaKVStore::getPersistedVBucketState() {} "
                            "failed with status {}",
                            vbid,
                            to_string(state.status)));
    }

    return state;
}

KVStoreIface::ReadVBStateResult MagmaKVStore::getPersistedVBucketState(
        KVFileHandle& handle, Vbid vbid) const {
    auto& magmaHandle = static_cast<MagmaKVFileHandle&>(handle);

    auto state = readVBStateFromDisk(magmaHandle.vbid, *magmaHandle.snapshot);
    if (state.status != ReadVBStateStatus::Success) {
        throw std::runtime_error(
                fmt::format("MagmaKVStore::getPersistedVBucketState(handle) {} "
                            "failed with status {}",
                            vbid,
                            to_string(state.status)));
    }
    return state;
}

uint32_t MagmaKVStore::getKVStoreRevision(Vbid vbid) const {
    uint32_t kvstoreRev{0};
    auto [status, kvsRev] = magma->GetKVStoreRevision(vbid.get());
    if (status) {
        kvstoreRev = kvsRev;
    }

    return kvstoreRev;
}

KVStoreIface::ReadVBStateResult MagmaKVStore::readVBStateFromDisk(
        Vbid vbid) const {
    Slice keySlice(LocalDocKey::vbstate);
    auto [magmaStatus, valString] = readLocalDoc(vbid, keySlice);

    if (!magmaStatus.IsOkDocFound()) {
        logger->warn(
                "MagmaKVStore::readVBStateFromDisk: {} readLocalDoc "
                "returned status {}",
                vbid,
                magmaStatus);
        auto status = magmaStatus.ErrorCode() == Status::OkDocNotFound
                              ? ReadVBStateStatus::NotFound
                              : ReadVBStateStatus::Error;
        return {status, {}};
    }

    nlohmann::json j;

    try {
        j = nlohmann::json::parse(valString);
    } catch (const nlohmann::json::exception& e) {
        return {ReadVBStateStatus::JsonInvalid, {}};
    }

    logger->TRACE("MagmaKVStore::readVBStateFromDisk {} vbstate:{}", vbid, valString);

    vbucket_state vbstate = j;
    mergeMagmaDbStatsIntoVBState(vbstate, vbid);

    bool snapshotValid;
    std::tie(snapshotValid, vbstate.lastSnapStart, vbstate.lastSnapEnd) =
            processVbstateSnapshot(vbid, vbstate);

    auto status = ReadVBStateStatus::Success;
    if (!snapshotValid) {
        status = ReadVBStateStatus::CorruptSnapshot;
    }

    return {status, vbstate};
}

KVStoreIface::ReadVBStateResult MagmaKVStore::readVBStateFromDisk(
        Vbid vbid, magma::Magma::Snapshot& snapshot) const {
    Slice keySlice(LocalDocKey::vbstate);
    auto [magmaStatus, val] = readLocalDoc(vbid, snapshot, keySlice);

    if (!magmaStatus.IsOkDocFound()) {
        auto status = magmaStatus.IsOkDocNotFound()
                              ? ReadVBStateStatus::NotFound
                              : ReadVBStateStatus::Error;
        return {status, {}};
    }

    nlohmann::json j;

    try {
        j = nlohmann::json::parse(val);
    } catch (const nlohmann::json::exception& e) {
        return {ReadVBStateStatus::JsonInvalid, {}};
    }

    vbucket_state vbstate = j;

    auto userStats = magma->GetKVStoreUserStats(snapshot);
    if (!userStats) {
        return {ReadVBStateStatus::Error, {}};
    }

    auto* magmaUserStats = dynamic_cast<MagmaDbStats*>(userStats.get());
    if (!magmaUserStats) {
        throw std::runtime_error("MagmaKVStore::readVBStateFromDisk error - " +
                                 vbid.to_string() + " magma returned invalid " +
                                 "type of UserStats");
    }
    vbstate.purgeSeqno = magmaUserStats->purgeSeqno;

    bool snapshotValid;
    std::tie(snapshotValid, vbstate.lastSnapStart, vbstate.lastSnapEnd) =
            processVbstateSnapshot(vbid, vbstate);

    ReadVBStateStatus status = ReadVBStateStatus::Success;
    if (!snapshotValid) {
        status = ReadVBStateStatus::CorruptSnapshot;
    }

    return {status, vbstate};
}

KVStoreIface::ReadVBStateStatus MagmaKVStore::loadVBStateCache(
        Vbid vbid, bool resetKVStoreRev) {
    const auto readState = readVBStateFromDisk(vbid);

    // We only want to reset the kvstoreRev when loading up the vbstate cache
    // during magma instantiation. We do this regardless of the state that we
    // find on disk as magma asserts that the kvstore rev must be monotonic
    // and we may have a case in which the revision exists without a vBucket
    // state.
    if (resetKVStoreRev) {
        auto kvstoreRev = getKVStoreRevision(vbid);
        kvstoreRevList[getCacheSlot(vbid)].reset(kvstoreRev);
    }

    // If the vBucket exists but does not have a vbucket_state (i.e.
    // OkDocNotFound is returned from readVBStateFromDisk) then just use a
    // defaulted vbucket_state (which defaults to the dead state).
    if (readState.status != ReadVBStateStatus::Success) {
        logger->warn(
                "MagmaKVStore::loadVBStateCache {} failed. Rev:{} "
                "Status:{}",
                vbid,
                getKVStoreRevision(vbid),
                to_string(readState.status));
        return readState.status;
    }

    cachedVBStates[getCacheSlot(vbid)] =
            std::make_unique<vbucket_state>(readState.state);

    return ReadVBStateStatus::Success;
}

void MagmaKVStore::addVBStateUpdateToLocalDbReqs(LocalDbReqs& localDbReqs,
                                                 const vbucket_state& vbs,
                                                 uint64_t kvstoreRev) {
    std::string vbstateString = encodeVBState(vbs);
    logger->TRACE("MagmaKVStore::addVBStateUpdateToLocalDbReqs vbstate:{}",
                  vbstateString);
    localDbReqs.emplace_back(
            MagmaLocalReq(LocalDocKey::vbstate, std::move(vbstateString)));
}

KVStoreIface::ReadVBStateResult MagmaKVStore::loadVBucketSnapshot(
        Vbid vbid, vbucket_state_t state, const nlohmann::json& topology) {
    ReadVBStateResult ret{ReadVBStateStatus::Error, {}};
    magma::Magma::CreateUsingMountConfig createUsingMountConfig;
    createUsingMountConfig.CreateCallback =
            [this, vbid, state, &topology, out = &ret](
                    const magma::Magma::GetLocalDoc& getLocalDoc,
                    const magma::Magma::UpdateLocalDoc& updateLocalDoc)
            -> magma::Status {
        // We are in secondary domain
        const Slice keySlice(LocalDocKey::vbstate);
        std::string valString;
        auto magmaStatus = getLocalDoc(keySlice, valString);
        if (!magmaStatus.IsOkDocFound()) {
            {
                cb::UseArenaMallocPrimaryDomain primaryDomain;
                logger->warn(
                        "MagmaKVStore::loadVBucketSnapshot: {} "
                        "readLocalDoc returned status {}",
                        vbid,
                        magmaStatus);
            }
            if (magmaStatus.IsOkDocNotFound()) {
                out->status = ReadVBStateStatus::NotFound;
                // Return OK as we will create an empty VBucket
                return magma::Status::OK();
            }
            out->status = ReadVBStateStatus::Error;
            return magmaStatus;
        }
        std::string encodedVBState;
        {
            // Back to primary domain to call KVStore methods
            cb::UseArenaMallocPrimaryDomain primaryDomain;
            try {
                auto& vbstate = out->state;
                vbstate = nlohmann::json::parse(valString);
                mergeMagmaDbStatsIntoVBState(vbstate, vbid);
                bool snapshotValid;
                std::tie(snapshotValid,
                         vbstate.lastSnapStart,
                         vbstate.lastSnapEnd) =
                        processVbstateSnapshot(vbid, vbstate);
                if (!snapshotValid) {
                    out->status = ReadVBStateStatus::CorruptSnapshot;
                    return {magma::Status::Corruption,
                            "Corrupt vbstate snapshot"};
                }
                vbstate.transition.replicationTopology = topology;
                vbstate.transition.state = state;
                auto primaryEncodedVBState = encodeVBState(out->state);
                {
                    cb::UseArenaMallocSecondaryDomain secondaryDomain;
                    // Copy into secondary domain
                    encodedVBState = primaryEncodedVBState;
                }
                out->status = ReadVBStateStatus::Success;
            } catch (const nlohmann::json::exception& ex) {
                out->status = ReadVBStateStatus::JsonInvalid;
                return {magma::Status::Corruption, ex.what()};
            } catch (const std::exception& ex) {
                out->status = ReadVBStateStatus::Error;
                return {magma::Status::Internal, ex.what()};
            }
            if (out->status != ReadVBStateStatus::Success) {
                return {magma::Status::Internal, "CreateCallback failure"};
            }
        }
        // Back to secondary domain to call magma callback
        magmaStatus =
                updateLocalDoc(magma::Magma::WriteOperation::NewLocalDocUpdate(
                        keySlice, encodedVBState));
        return magmaStatus;
    };
    auto status = magma->CreateKVStore(vbid.get(),
                                       kvstoreRevList[getCacheSlot(vbid)],
                                       {std::move(createUsingMountConfig)});
    if (ret.status == ReadVBStateStatus::Success && !status) {
        ret.status = ReadVBStateStatus::Error;
    }
    if (ret.status != ReadVBStateStatus::Success) {
        logger->warn(
                "MagmaKVStore::loadVBucketSnapshot: {} "
                "CreateKVStore returned status {}",
                vbid,
                status);
        return ret;
    }
    updateCachedVBState(vbid, ret.state);
    return ret;
}

std::pair<Status, std::string> MagmaKVStore::processReadLocalDocResult(
        Status status,
        Vbid vbid,
        const magma::Slice& keySlice,
        std::string_view value) const {
    magma::Status retStatus = Status::OK();
    if (!status) {
        retStatus = magma::Status(
                status.ErrorCode(),
                "MagmaKVStore::readLocalDoc " + vbid.to_string() +
                        " key:" + keySlice.ToString() + " " + status.String());
        logger->warn("{}", retStatus.Message());
    } else {
        if (status.IsOkDocNotFound()) {
            retStatus = magma::Status(
                    magma::Status::Code::OkDocNotFound,
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
    auto [status, value] = magma->GetLocal(vbid.get(), keySlice);
    return processReadLocalDocResult(status, vbid, keySlice, *value);
}

std::pair<Status, std::string> MagmaKVStore::readLocalDoc(
        Vbid vbid,
        magma::Magma::Snapshot& snapshot,
        const Slice& keySlice) const {
    auto [status, value] = magma->GetLocal(snapshot, keySlice);
    return processReadLocalDocResult(status, vbid, keySlice, *value);
}

std::string MagmaKVStore::encodeVBState(const vbucket_state& vbstate) const {
    nlohmann::json j = vbstate;
    return j.dump();
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
    return dbStats->docCount;
}

uint64_t MagmaKVStore::getPurgeSeqno(Vbid vbid) {
    auto stats = magma->GetKVStoreUserStats(vbid.get());

    if (!stats) {
        logger->warn(
                "MagmaKVStore::getPurgeSeqno {} failed GetKVStoreUserStats",
                vbid);
        return 0;
    }
    auto* otherStats = dynamic_cast<MagmaDbStats*>(stats.get());
    return otherStats->purgeSeqno;
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

    auto callback =
            [&](Slice& keySlice, Slice& metaSlice, Slice& valueSlice) -> bool {
        const auto docMeta = magmakv::getDocMeta(metaSlice);
        if (logger->should_log(spdlog::level::TRACE)) {
            logger->TRACE(
                    "MagmaKVStore::getAllKeys callback {} key:{} seqno:{} "
                    "deleted:{}",
                    vbid,
                    cb::UserData{makeDiskDocKey(keySlice).to_string()},
                    magmakv::getSeqNum(metaSlice),
                    docMeta.isDeleted());
        }

        if (docMeta.isDeleted()) {
            // continue scanning
            return false;
        }
        auto retKey = makeDiskDocKey(keySlice);
        cb->callback(retKey);

        return cb->getStatus() != cb::engine_errc::success;
    };

    if (logger->should_log(spdlog::level::TRACE)) {
        logger->TRACE("MagmaKVStore::getAllKeys {} start:{} count:{})",
                      vbid,
                      cb::UserData{startKey.to_string()},
                      count);
    }

    auto start = cb::time::steady_clock::now();
    auto status = magma->GetRange(
            vbid.get(), startKeySlice, endKeySlice, callback, false);
    if (!status) {
        logger->critical("MagmaKVStore::getAllKeys {} startKey:{} status:{}",
                         vbid,
                         cb::UserData{startKey.to_string()},
                         status.String());
    }

    if (!status) {
        return magmaErr2EngineErr(status.ErrorCode());
    }

    st.getAllKeysHisto.add(
            std::chrono::duration_cast<std::chrono::microseconds>(
                    cb::time::steady_clock::now() - start));

    return cb::engine_errc::success;
}

CompactDBStatus MagmaKVStore::compactDB(
        std::unique_lock<std::mutex>& vbLock,
        std::shared_ptr<CompactionContext> ctx) {
    vbLock.unlock();
    return compactDBInternal(vbLock, std::move(ctx));
}

CompactDBStatus MagmaKVStore::compactDBInternal(
        std::unique_lock<std::mutex>& vbLock,
        std::shared_ptr<CompactionContext> ctx) {
    cb::time::steady_clock::time_point start = cb::time::steady_clock::now();
    auto vbid = ctx->getVBucket()->getId();
    logger->debug(
            "MagmaKVStore::compactDBInternal: {} purge_before_ts:{} "
            "purge_before_seq:{} drop_deletes:{} "
            "retain_erroneous_tombstones:{}",
            vbid,
            ctx->compactConfig.purge_before_ts,
            ctx->compactConfig.purge_before_seq,
            ctx->compactConfig.drop_deletes,
            ctx->compactConfig.retain_erroneous_tombstones);

    // Gather stats so we log pre/post difference. For magma, tombstone count
    // isn't known so is not updated.
    // The pre/post stats are approximate as magma is not locked between here
    // and the call to CompactKVStore
    auto updateStats = [this, vbid](FileInfo& info) {
        auto stats = getMagmaDbStats(vbid);
        if (stats) {
            info.items = stats->docCount;
            info.purgeSeqno = stats->purgeSeqno;
        }
        auto dbInfo = getDbFileInfo(vbid);
        info.size = dbInfo.fileSize;
    };
    updateStats(ctx->stats.pre);

    auto [getDroppedStatus, dropped] = getDroppedCollections(vbid);
    if (!getDroppedStatus) {
        logger->warn(
                "MagmaKVStore::compactDBInternal: {} Failed to get "
                "dropped collections",
                vbid);
        return CompactDBStatus::Failed;
    }

    ctx->eraserContext =
            std::make_unique<Collections::VB::EraserContext>(dropped);

    auto diskState = readVBStateFromDisk(vbid);
    if (diskState.status != ReadVBStateStatus::Success) {
        logger->warn(
                "MagmaKVStore::compactDBInternal: trying to run "
                "compaction on {} but can't read vbstate. Status:{}",
                vbid,
                to_string(diskState.status));
        return CompactDBStatus::Failed;
    }
    ctx->highCompletedSeqno = diskState.state.persistedCompletedSeqno;

    auto compactionCB = [this, vbid, ctx](const Magma::KVStoreID kvID) {
        return std::make_unique<MagmaKVStore::MagmaCompactionCB>(
                *this, vbid, ctx);
    };

    LocalDbReqs localDbReqs;

    Status status;
    std::unordered_set<CollectionID> purgedCollections;
    uint64_t purgedCollectionsEndSeqno = 0;
    auto compactionStatus = CompactDBStatus::Success;
    // Item counts of the dropped collections.
    std::vector<std::pair<Collections::KVStore::DroppedCollection, uint64_t>>
            dcInfo;

    if (dropped.empty()) {
        // Compact the entire key range
        Slice nullKey;
        status = magma->CompactKVStore(
                vbid.get(), nullKey, nullKey, compactionCB, ctx->obsolete_keys);
        if (!status) {
            if (status.ErrorCode() == Status::Cancelled) {
                return CompactDBStatus::Aborted;
            }
            logger->warn(
                    "MagmaKVStore::compactDBInternal CompactKVStore failed. "
                    "{} status:{}",
                    vbid,
                    status.String());
            return CompactDBStatus::Failed;
        }
    } else {
        for (auto& dc : dropped) {
            auto [status, itemCount] =
                    getDroppedCollectionItemCount(vbid, dc.collectionId);
            if (status == KVStore::GetCollectionStatsStatus::Failed) {
                logger->warn(
                        "MagmaKVStore::compactDBInternal getCollectionStats() "
                        "failed for dropped collection "
                        "cid:{} {}",
                        dc.collectionId,
                        vbid);
                return CompactDBStatus::Failed;
            }
            dcInfo.emplace_back(dc, itemCount);
        }

        for (auto& [dc, itemCount] : dcInfo) {
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

            auto compactionCBAndHook = [this,
                                        vbid,
                                        &ctx,
                                        cid = dc.collectionId](
                                               const Magma::KVStoreID kvID) {
                auto ret = std::make_unique<MagmaKVStore::MagmaCompactionCB>(
                        *this, vbid, ctx, cid);
                preCompactKVStoreHook();
                return ret;
            };

            status = magma->CompactKVStore(vbid.get(),
                                           keySlice,
                                           keySlice,
                                           compactionCBAndHook,
                                           ctx->obsolete_keys);

            compactionStatusHook(status);

            if (!status) {
                if (status.ErrorCode() == Status::Cancelled) {
                    compactionStatus = CompactDBStatus::Aborted;
                } else {
                    logger->warn(
                            "MagmaKVStore::compactDBInternal CompactKVStore {} "
                            "CID:{} failed status:{}",
                            vbid,
                            cb::UserData{makeDiskDocKey(keySlice).to_string()},
                            status.String());
                    compactionStatus = CompactDBStatus::Failed;
                }
                continue;
            }
            // Can't track number of collection items purged properly in the
            // compaction callback for magma as it may be called multiple
            // times per key. We CAN just subtract the number of items we know
            // belong to the collection though before we update the vBucket doc
            // count.
            ctx->stats.collectionsItemsPurged += itemCount;
            ctx->stats.collectionsPurged++;

            // We've finish processing this collection.
            // Create a SystemEvent key for the collection and process it.
            auto collectionKey = SystemEventFactory::makeCollectionEventKey(
                    dc.collectionId, SystemEvent::Collection);

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

            ctx->eraserContext->processSystemEvent(key.getDocKey(),
                                                   SystemEvent::Collection);

            // This collection purged successfully, save this for later so that
            // we can update the droppedCollections local doc
            purgedCollections.insert(dc.collectionId);
            purgedCollectionsEndSeqno =
                    std::max(dc.endSeqno, purgedCollectionsEndSeqno);
        }

        // Also need to compact the prepare namespace as this is disjoint from
        // the collection namespaces. This is done after the main collection
        // purge and uses a simpler callback
        cb::mcbp::unsigned_leb128<CollectionIDType> leb128(
                CollectionID::DurabilityPrepare);
        Slice prepareSlice(reinterpret_cast<const char*>(leb128.data()),
                           leb128.size());
        status = magma->CompactKVStore(vbid.get(),
                                       prepareSlice,
                                       prepareSlice,
                                       compactionCB,
                                       ctx->obsolete_keys);
        compactionStatusHook(status);
        if (!status) {
            if (status.ErrorCode() == Status::Cancelled) {
                return CompactDBStatus::Aborted;
            }
            logger->warn(
                    "MagmaKVStore::compactDBInternal CompactKVStore {} "
                    "over the prepare namespace failed with status:{}",
                    vbid,
                    status.String());

            // It's important that we return here because a failure to do so
            // would result in us not cleaning up prepares for a dropped
            // collection if the compaction of a dropped collection succeeds.
            return CompactDBStatus::Failed;
        }

        // Finally, visit the system namespace to ensure any events related to
        // the dropped collections are also purged
        cb::mcbp::unsigned_leb128<CollectionIDType> sys(
                CollectionID::SystemEvent);
        Slice systemSlice(reinterpret_cast<const char*>(sys.data()),
                          sys.size());
        status = magma->CompactKVStore(vbid.get(),
                                       systemSlice,
                                       systemSlice,
                                       compactionCB,
                                       ctx->obsolete_keys);
        if (!status) {
            if (status.ErrorCode() == Status::Cancelled) {
                return CompactDBStatus::Aborted;
            }
            logger->warn(
                    "MagmaKVStore::compactDBInternal CompactKVStore {} "
                    "of system namespace failed status:{}",
                    vbid,
                    status.String());
            return CompactDBStatus::Failed;
        }
    }

    // Make our completion callback before writing the new file. We should
    // update our in memory state before we finalize on disk state so that we
    // don't have to worry about race conditions with things like the purge
    // seqno.
    if (ctx->completionCallback) {
        try {
            ctx->completionCallback(*ctx);
        } catch (const std::exception& e) {
            logger->error("CompactionContext::completionCallback {}", e.what());
            return CompactDBStatus::Failed;
        }
    }

    if (ctx->eraserContext->needToUpdateCollectionsMetadata()) {
        // Need to write back some collections metadata, in particular we need
        // to remove from the dropped collections doc all of the collections
        // that we have just purged. This is also where we update the item
        // counts. A collection drop might have happened after we started this
        // compaction though and we may not have dropped it yet so we can't just
        // delete the doc. To avoid a flusher coming along and dropping another
        // collection while we do this we need to hold the vBucket write lock.
        vbLock.lock();

        // 1) Get the current state from disk
        auto [collDroppedStatus, droppedCollections] =
                getDroppedCollections(vbid);
        if (!collDroppedStatus) {
            logger->critical(
                    "MagmaKVStore::compactDbInternal {} failed "
                    "getDroppedCollections",
                    vbid);
            return CompactDBStatus::Failed;
        }

        // 2) Generate a new flatbuffer document to write back
        auto fbData = Collections::VB::Flush::
                encodeRelativeComplementOfDroppedCollections(
                        droppedCollections,
                        purgedCollections,
                        purgedCollectionsEndSeqno);

        // 3) If the function returned data, write it, else the document is
        // delete.
        WriteOps writeOps;
        if (fbData.data()) {
            localDbReqs.emplace_back(
                    MagmaLocalReq(LocalDocKey::droppedCollections, fbData));
        } else {
            // Need to ensure the 'dropped' list on disk is now gone
            localDbReqs.emplace_back(MagmaLocalReq::makeDeleted(
                    LocalDocKey::droppedCollections));
        }

        // Update the dbStats together with clearing droppedCollections, to
        // ensure that we will not persist an inconsistent state.
        MagmaDbStats dbStatsDelta;
        for (auto& [dc, itemCount] : dcInfo) {
            processCollectionPurgeDelta(
                    dbStatsDelta, dc.collectionId, -itemCount);
        }
        addStatUpdateToWriteOps(dbStatsDelta, writeOps);
        addLocalDbReqs(localDbReqs, writeOps);

        const auto historyMode = ctx->getVBucket()->isHistoryRetentionEnabled()
                                         ? Magma::HistoryMode::Enabled
                                         : Magma::HistoryMode::Disabled;
        status = magma->WriteDocs(vbid.get(),
                                  writeOps,
                                  kvstoreRevList[getCacheSlot(vbid)],
                                  historyMode);
        if (!status) {
            logger->critical(
                    "MagmaKVStore::compactDBInternal {} WriteDocs failed. "
                    "Status:{}",
                    vbid,
                    status.String());
            return CompactDBStatus::Failed;
        }

        if (doSyncEveryBatch) {
            status = magma->Sync(true);
            if (!status) {
                logger->critical(
                        "MagmaKVStore::compactDBInternal {} Sync "
                        "failed. "
                        "Status:{}",
                        vbid,
                        status.String());
                return CompactDBStatus::Failed;
            }
        }

        st.compactHisto.add(
                std::chrono::duration_cast<std::chrono::microseconds>(
                        cb::time::steady_clock::now() - start));
        logger->TRACE(
                "MagmaKVStore::compactDBInternal: max_purged_seq:{}"
                " collectionsItemsPurged:{}"
                " collectionsDeletedItemsPurged:{}"
                " tombstonesPurged:{}"
                " preparesPurged:{}",
                ctx->getRollbackPurgeSeqno(),
                ctx->stats.collectionsItemsPurged,
                ctx->stats.collectionsDeletedItemsPurged,
                ctx->stats.tombstonesPurged,
                ctx->stats.preparesPurged);
    }

    updateStats(ctx->stats.post);
    return compactionStatus;
}

std::unique_ptr<KVFileHandle> MagmaKVStore::makeFileHandle(Vbid vbid) const {
    DomainAwareUniquePtr<magma::Magma::Snapshot> snapshot;
    // Flush write cache for creating an on-disk snapshot
    auto status = magma->SyncKVStore(vbid.get(), false);
    fileHandleSyncStatusHook(status);
    if (status.IsOK()) {
        status = magma->GetDiskSnapshot(vbid.get(), snapshot);
    } else {
        if (status.ErrorCode() == magma::Status::ReadOnly) {
            logger->warn(
                    "MagmaKVStore::makeFileHandle {} creating in memory "
                    "snapshot as magma is in read-only mode",
                    vbid);
        } else if (status.ErrorCode() == magma::Status::DiskFull) {
            ++st.numOpenFailure;
            logger->warn(
                    "MagmaKVStore::makeFileHandle {} creating in memory "
                    "snapshot as magma returned DiskFull when trying to Sync "
                    "KVStore",
                    vbid);
        } else {
            ++st.numOpenFailure;
            logger->warn(
                    "MagmaKVStore::makeFileHandle {} Failed to sync kvstore "
                    "with status {}, so we'll be unable to create a disk "
                    "snapshot for the MagmaKVFileHandle",
                    vbid,
                    status);
            return nullptr;
        }
        // If magma returns ReadOnly mode here then we've opened it in ReadOnly
        // mode during warmup as the original open attempt errored with
        // DiskFull. In the ReadOnly open magma has to replay the WAL ops in
        // memory and cannot flush them to disk. Or if we've received a DiskFull
        // status then we're unable to Sync the KVStore to disk. As such, we
        // have to open the memory snapshot rather than the disk snapshot here.
        status = magma->GetSnapshot(vbid.get(), snapshot);
    }

    if (!status) {
        logger->warn(
                "MagmaKVStore::makeFileHandle {} Failed to get magma snapshot "
                "with status {}",
                vbid,
                status);
        return nullptr;
    }

    if (!snapshot) {
        logger->error(
                "MagmaKVStore::makeFileHandle {} Failed to get magma snapshot,"
                " no pointer returned",
                vbid);
        return nullptr;
    }

    return std::make_unique<MagmaKVFileHandle>(
            vbid, std::move(snapshot), *magma);
}

RollbackResult MagmaKVStore::rollback(Vbid vbid,
                                      uint64_t rollbackSeqno,
                                      std::unique_ptr<RollbackCB> callback) {
    logger->TRACE("MagmaKVStore::rollback {} seqno:{}", vbid, rollbackSeqno);

    callback->setKVFileHandle(makeFileHandle(vbid));

    auto keyCallback =
            [this, cb = callback.get(), vbid](const Slice& keySlice,
                                              const uint64_t seqno,
                                              const Slice& metaSlice) {
                auto diskKey = makeDiskDocKey(keySlice);

                if (configuration.isSanityCheckingVBucketMapping()) {
                    validateKeyMapping(
                            "MagmaKVStore::rollback",
                            configuration
                                    .getVBucketMappingErrorHandlingMethod(),
                            diskKey.getDocKey(),
                            vbid,
                            configuration.getMaxVBuckets());
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
                                             ValueFilter::KEYS_ONLY,
                                             getDefaultCreateItemCallback());
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
    case Status::ReadOnly:
    case Status::TransientIO:
    case Status::DiskFull:
    case Status::NotFound:
    case Status::Cancelled:
    case Status::RetryLater:
    case Status::NoAccess:
    case Status::OkDocNotFound:
    case Status::EncryptionKeyNotFound:
        logger->critical("MagmaKVStore::rollback Rollback {} status:{}",
                         vbid,
                         status.String());
        // Fall through
    case Status::InvalidKVStore:
        logger->warn(
                "MagmaKVStore::rollback {} KVStore not found, nothing has "
                "been persisted yet.",
                vbid);
        // Fall through
    case Status::CheckpointNotFound:
        // magma->Rollback returns non-success in the case where we have no
        // rollback points (and should reset the vBucket to 0). This isn't a
        // critical error so don't log it
        return RollbackResult(false);
    }

    // Did a rollback, so need to reload the vbstate cache.
    auto loadVbStateStatus = loadVBStateCache(vbid);
    if (loadVbStateStatus != ReadVBStateStatus::Success) {
        logger->error("MagmaKVStore::rollback {} readVBStateFromDisk status:{}",
                      vbid,
                      to_string(loadVbStateStatus));
        return RollbackResult(false);
    }

    auto vbstate = getCachedVBucketState(vbid);
    if (!vbstate) {
        logger->critical(
                "MagmaKVStore::rollback getVBState {} vbstate not found", vbid);
        return RollbackResult(false);
    }

    return {true,
            static_cast<uint64_t>(vbstate->highSeqno),
            vbstate->lastSnapStart,
            vbstate->lastSnapEnd};
}

std::optional<Collections::ManifestUid> MagmaKVStore::getCollectionsManifestUid(
        KVFileHandle& kvFileHandle) const {
    auto& kvfh = static_cast<MagmaKVFileHandle&>(kvFileHandle);

    auto [status, manifest] =
            readLocalDoc(kvfh.vbid, *kvfh.snapshot, LocalDocKey::manifest);
    if (!status.IsOkDocFound()) {
        if (status.ErrorCode() != Status::Code::OkDocNotFound) {
            logger->warn("MagmaKVStore::getCollectionsManifestUid(): {}",
                         status.Message());
            return std::nullopt;
        }

        return Collections::ManifestUid{0};
    }

    return Collections::KVStore::decodeManifestUid(
            {reinterpret_cast<const uint8_t*>(manifest.data()),
             manifest.length()});
}

std::pair<bool, Collections::ManifestUid>
MagmaKVStore::getCollectionsManifestUid(Vbid vbid) const {
    auto [status, uidDoc] = readLocalDoc(vbid, LocalDocKey::manifest);
    if (status) {
        return {true,
                Collections::KVStore::decodeManifestUid(
                        {reinterpret_cast<const uint8_t*>(uidDoc.data()),
                         uidDoc.length()})};
    }
    return {false, Collections::ManifestUid{}};
}

std::pair<magma::Status, Collections::KVStore::Manifest>
MagmaKVStore::getCollectionsManifest(Vbid vbid,
                                     magma::Magma::Snapshot& snapshot) const {
    auto [status, manifest] =
            readLocalDoc(vbid, snapshot, LocalDocKey::manifest);
    if (!status) {
        return {status,
                Collections::KVStore::Manifest{
                        Collections::KVStore::Manifest::Default{}}};
    }

    std::string openCollections;
    std::tie(status, openCollections) =
            readLocalDoc(vbid, snapshot, LocalDocKey::openCollections);
    if (!status) {
        return {status,
                Collections::KVStore::Manifest{
                        Collections::KVStore::Manifest::Default{}}};
    }

    std::string openScopes;
    std::tie(status, openScopes) =
            readLocalDoc(vbid, snapshot, LocalDocKey::openScopes);
    if (!status) {
        return {status,
                Collections::KVStore::Manifest{
                        Collections::KVStore::Manifest::Default{}}};
    }

    std::string droppedCollections;
    std::tie(status, droppedCollections) =
            readLocalDoc(vbid, snapshot, LocalDocKey::droppedCollections);
    if (!status) {
        return {status,
                Collections::KVStore::Manifest{
                        Collections::KVStore::Manifest::Default{}}};
    }

    return {status,
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

std::pair<bool, Collections::KVStore::Manifest>
MagmaKVStore::getCollectionsManifest(Vbid vbid) const {
    DomainAwareUniquePtr<magma::Magma::Snapshot> snapshot;
    auto status = magma->GetSnapshot(vbid.get(), snapshot);

    if (!status) {
        return {false,
                Collections::KVStore::Manifest{
                        Collections::KVStore::Manifest::Default{}}};
    }

    Collections::KVStore::Manifest manifest{
            Collections::KVStore::Manifest::Empty{}};
    std::tie(status, manifest) = getCollectionsManifest(vbid, *snapshot);
    return {status.IsOK(), std::move(manifest)};
}

std::pair<bool, std::vector<Collections::KVStore::DroppedCollection>>
MagmaKVStore::getDroppedCollections(Vbid vbid) const {
    Slice keySlice(LocalDocKey::droppedCollections);
    auto [status, dropped] = readLocalDoc(vbid, keySlice);
    // Currently we need the InvalidKVStore check for the case in which we
    // create the (magma)KVStore (done at first flush).
    // @TODO investigate removal/use of snapshot variant
    if (!status.IsOK() && status.ErrorCode() != Status::Code::InvalidKVStore) {
        return {false, {}};
    }

    return {true,
            Collections::KVStore::decodeDroppedCollections(
                    {reinterpret_cast<const uint8_t*>(dropped.data()),
                     dropped.length()})};
}

std::pair<magma::Status, std::vector<Collections::KVStore::OpenCollection>>
MagmaKVStore::getOpenCollections(Vbid vbid,
                                 magma::Magma::Snapshot& snapshot) const {
    Slice keySlice(LocalDocKey::openCollections);
    auto [status, openCollections] = readLocalDoc(vbid, snapshot, keySlice);
    return {status,
            Collections::KVStore::decodeOpenCollections(
                    {reinterpret_cast<const uint8_t*>(openCollections.data()),
                     openCollections.length()})};
}

std::pair<magma::Status, std::vector<Collections::KVStore::DroppedCollection>>
MagmaKVStore::getDroppedCollections(Vbid vbid,
                                    magma::Magma::Snapshot& snapshot) const {
    Slice keySlice(LocalDocKey::droppedCollections);
    auto [status, dropped] = readLocalDoc(vbid, snapshot, keySlice);
    return {status,
            Collections::KVStore::decodeDroppedCollections(
                    {reinterpret_cast<const uint8_t*>(dropped.data()),
                     dropped.length()})};
}

magma::Status MagmaKVStore::updateCollectionsMeta(
        Vbid vbid,
        LocalDbReqs& localDbReqs,
        Collections::VB::Flush& collectionsFlush,
        MagmaDbStats& dbStats) {
    // If collectionsFlush manifestUid is 0 then we've updated an item that
    // belongs to a dropped but not yet purged collection (collection
    // resurrection). That requires an update to the dropped collections stats
    // which is dealt with below, but without a system event in this flush batch
    // we won't have set the manifestUid (it will be 0). Whatever manifest uid
    // is currently on disk is correct so skip the change here.
    if (collectionsFlush.getManifestUid() != 0) {
        updateManifestUid(localDbReqs, collectionsFlush);
    }

    if (collectionsFlush.isOpenCollectionsChanged()) {
        auto status =
                updateOpenCollections(vbid, localDbReqs, collectionsFlush);
        if (!status.IsOK()) {
            return status;
        }
    }

    if (collectionsFlush.isDroppedCollectionsChanged() ||
        collectionsFlush.isDroppedStatsChanged()) {
        auto status = updateDroppedCollections(
                vbid, localDbReqs, collectionsFlush, dbStats);
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
    localDbReqs.emplace_back(MagmaLocalReq(LocalDocKey::manifest, buf));
}

magma::Status MagmaKVStore::updateOpenCollections(
        Vbid vbid,
        LocalDbReqs& localDbReqs,
        Collections::VB::Flush& collectionsFlush) {
    Slice keySlice(LocalDocKey::openCollections);
    auto [status, collections] = readLocalDoc(vbid, keySlice);

    if (status) {
        auto buf = collectionsFlush.encodeOpenCollections(
                {reinterpret_cast<const uint8_t*>(collections.data()),
                 collections.length()});

        localDbReqs.emplace_back(
                MagmaLocalReq(LocalDocKey::openCollections, buf));
        return Status::OK();
    }

    return status;
}

magma::Status MagmaKVStore::updateDroppedCollections(
        Vbid vbid,
        LocalDbReqs& localDbReqs,
        Collections::VB::Flush& collectionsFlush,
        MagmaDbStats& dbStats) {
    // For couchstore we would drop the collections stats local doc here but
    // magma can visit old keys (during the collection erasure compaction) and
    // we can't work out if they are the latest or not. To track the document
    // count correctly we need to keep the stats doc around until the collection
    // erasure compaction runs which will then delete the doc when it has
    // processed the collection.
    auto [status, dropped] = getDroppedCollections(vbid);
    if (!status) {
        return {Status::Code::Invalid, "Failed to get dropped collections"};
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
        auto droppedDelta = 0;

        // Step 1) Read the current alive stats on disk, we'll add them to
        // the dropped stats delta as the collection is now gone, provided we
        // dropped the collection in this flush batch. If we had just adjusted
        // stats due to a change in doc state then we'd have already tracked
        // this in the original drop
        if (droppedInBatch) {
            auto [getStatus, currentAliveStats] = getCollectionStats(vbid, cid);
            if (status) {
                droppedDelta += currentAliveStats.itemCount;
            }
        }

        // Step 2) Add the dropped stats from FlushAccounting
        auto droppedInFlush = collectionsFlush.getDroppedStats(cid);
        droppedDelta += droppedInFlush.getItemCount();

        // Step 4) Update the statsdelta
        dbStats.droppedCollectionCounts[static_cast<uint32_t>(cid)] =
                droppedDelta;

        // Step 5) Delete the latest alive stats if the collection wasn't
        // resurrected, we don't need them anymore
        if (!collectionsFlush.isOpen(cid)) {
            deleteCollectionStats(localDbReqs, cid);
        }
    }

    auto buf = collectionsFlush.encodeDroppedCollections(dropped);
    localDbReqs.emplace_back(
            MagmaLocalReq(LocalDocKey::droppedCollections, buf));

    return Status::OK();
}

std::string MagmaKVStore::getCollectionsStatsKey(CollectionID cid) const {
    return std::string{"|" + cid.to_string() + "|"};
}

void MagmaKVStore::saveCollectionStats(
        LocalDbReqs& localDbReqs,
        CollectionID cid,
        const Collections::VB::PersistedStats& stats) {
    auto key = getCollectionsStatsKey(cid);
    auto encodedStats = stats.getLebEncodedStats();
    localDbReqs.emplace_back(MagmaLocalReq(key, std::move(encodedStats)));
}

std::pair<KVStore::GetCollectionStatsStatus, Collections::VB::PersistedStats>
MagmaKVStore::getCollectionStats(const KVFileHandle& kvFileHandle,
                                 CollectionID cid) const {
    const auto& kvfh = static_cast<const MagmaKVFileHandle&>(kvFileHandle);
    return getCollectionStats(
            kvfh.vbid, getCollectionsStatsKey(cid), kvfh.snapshot.get());
}

std::pair<KVStore::GetCollectionStatsStatus, uint64_t>
MagmaKVStore::getDroppedCollectionItemCount(Vbid vbid, CollectionID cid) const {
    auto dbStats = getMagmaDbStats(vbid);
    if (!dbStats) {
        // No stats available from Magma for this vbid, nothing to do.
        return {GetCollectionStatsStatus::NotFound, 0};
    }

    return {GetCollectionStatsStatus::Success,
            dbStats->droppedCollectionCounts[(uint32_t)cid]};
}

std::pair<KVStore::GetCollectionStatsStatus, Collections::VB::PersistedStats>
MagmaKVStore::getCollectionStats(Vbid vbid, CollectionID cid) const {
    auto key = getCollectionsStatsKey(cid);
    return getCollectionStats(vbid, key);
}

std::pair<KVStore::GetCollectionStatsStatus, Collections::VB::PersistedStats>
MagmaKVStore::getCollectionStats(Vbid vbid,
                                 magma::Slice keySlice,
                                 magma::Magma::Snapshot* snapshot) const {
    Status status;
    std::string stats;
    if (snapshot) {
        std::tie(status, stats) = readLocalDoc(vbid, *snapshot, keySlice);
    } else {
        std::tie(status, stats) = readLocalDoc(vbid, keySlice);
    }

    if (!status.IsOkDocFound()) {
        if (status.ErrorCode() != Status::Code::OkDocNotFound) {
            logger->warn("MagmaKVStore::getCollectionStats(): {}",
                         status.Message());
            return {KVStore::GetCollectionStatsStatus::Failed,
                    Collections::VB::PersistedStats()};
        }
        // Return Collections::VB::PersistedStats(true) with everything set to
        // 0 as the collection might have not been persisted to disk yet.
        return {KVStore::GetCollectionStatsStatus::NotFound,
                Collections::VB::PersistedStats()};
    }
    return {KVStore::GetCollectionStatsStatus::Success,
            Collections::VB::PersistedStats(stats.c_str(), stats.size())};
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
    Slice keySlice(LocalDocKey::openScopes);
    auto [status, scopes] = readLocalDoc(vbid, keySlice);

    if (status) {
        auto buf = collectionsFlush.encodeOpenScopes(
                {reinterpret_cast<const uint8_t*>(scopes.data()),
                 scopes.length()});
        localDbReqs.emplace_back(MagmaLocalReq(LocalDocKey::openScopes, buf));
        return Status::OK();
    }

    return status;
}

void MagmaKVStore::addTimingStats(const AddStatFn& add_stat,
                                  CookieIface& c) const {
    KVStore::addTimingStats(add_stat, c);
    const auto prefix = getStatsPrefix();

    MagmaFileStats fileStats;
    magma->GetFileStats(fileStats);

    add_prefixed_stat(
            prefix, "fsReadTime", fileStats.ReadTimeHisto, add_stat, c);
    add_prefixed_stat(
            prefix, "fsWriteTime", fileStats.WriteTimeHisto, add_stat, c);
    add_prefixed_stat(
            prefix, "fsSyncTime", fileStats.SyncTimeHisto, add_stat, c);
    add_prefixed_stat(
            prefix, "fsDelete", fileStats.DeleteTimeHisto, add_stat, c);
    add_prefixed_stat(
            prefix, "fsReadSize", fileStats.ReadSizeHisto, add_stat, c);
    add_prefixed_stat(
            prefix, "fsWriteSize", fileStats.WriteSizeHisto, add_stat, c);

    MagmaHistogramStats histoStats;
    magma->GetHistogramStats(histoStats);
    add_prefixed_stat(prefix,
                      "flushQueueTime",
                      histoStats.FlushQueueTimeHisto,
                      add_stat,
                      c);
    add_prefixed_stat(prefix,
                      "flushWaitTime",
                      histoStats.FlushWaitTimeHisto,
                      add_stat,
                      c);
    add_prefixed_stat(
            prefix, "getStatsTime", histoStats.GetStatsTimeHisto, add_stat, c);
    add_prefixed_stat(prefix,
                      "keyWriteBlockingCompactTime",
                      histoStats.KeyStats.WriteBlockingCompactTimeHisto,
                      add_stat,
                      c);
    add_prefixed_stat(prefix,
                      "keyCompactTime",
                      histoStats.KeyStats.CompactTimeHisto,
                      add_stat,
                      c);
    add_prefixed_stat(prefix,
                      "seqWriteBlockingCompactTime",
                      histoStats.SeqStats.WriteBlockingCompactTimeHisto,
                      add_stat,
                      c);
    add_prefixed_stat(prefix,
                      "seqCompactTime",
                      histoStats.SeqStats.CompactTimeHisto,
                      add_stat,
                      c);
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
    GetStatsMap stats;
    auto fill = [&](std::string_view statName, size_t value) {
        auto it = std::find(keys.begin(), keys.end(), statName);
        if (it != keys.end()) {
            stats.try_emplace(*it, value);
        }
    };

    fill("failure_get", st.numGetFailure.load());
    fill("continuous_backup_callback_count",
         st.continuousBackupCallbackCount.load());
    fill("continuous_backup_callback_micros",
         st.continuousBackupCallbackTime.load().count());

    // if no other stat is required return early without calling into magma
    if (keys.size() == stats.size()) {
        return stats;
    }

    auto magmaStats = magma->GetStats();
    fill("memory_quota", magmaStats->MemoryQuota);
    fill("storage_mem_used", magmaStats->TotalMemUsed);
    fill("magma_HistorySizeBytesEvicted", magmaStats->HistorySizeBytesEvicted);
    fill("magma_HistoryTimeBytesEvicted", magmaStats->HistoryTimeBytesEvicted);
    fill("magma_MemoryQuotaLowWaterMark", magmaStats->MemoryQuotaLowWaterMark);
    fill("magma_BloomFilterMemoryQuota", magmaStats->BloomFilterMemoryQuota);
    fill("magma_WriteCacheQuota", magmaStats->WriteCacheQuota);
    fill("magma_NCompacts", magmaStats->NCompacts);
    fill("magma_NDropEncryptionKeysCompacts",
         magmaStats->NDropEncryptionKeysCompacts);
    fill("magma_NDataLevelCompacts", magmaStats->NDataLevelCompacts);
    fill("magma_KeyIndex_NCompacts", magmaStats->KeyStats.NCompacts);
    fill("magma_SeqIndex_NCompacts", magmaStats->SeqStats.NCompacts);
    fill("magma_NFlushes", magmaStats->NFlushes);
    fill("magma_NTTLCompacts", magmaStats->NTTLCompacts);
    fill("magma_NFileCountCompacts", magmaStats->NFileCountCompacts);
    fill("magma_KeyIndex_NFileCountCompacts",
         magmaStats->KeyStats.NFileCountCompacts);
    fill("magma_SeqIndex_NFileCountCompacts",
         magmaStats->SeqStats.NFileCountCompacts);
    fill("magma_NWriterCompacts", magmaStats->NWriterCompacts);
    fill("magma_KeyIndex_NWriterCompacts",
         magmaStats->KeyStats.NWriterCompacts);
    fill("magma_SeqIndex_NWriterCompacts",
         magmaStats->SeqStats.NWriterCompacts);
    fill("magma_BytesOutgoing", magmaStats->BytesOutgoing);
    fill("magma_NReadBytes", magmaStats->NReadBytes);
    fill("magma_FSReadBytes", magmaStats->FSReadBytes);
    fill("magma_NReadBytesGet", magmaStats->NReadBytesGet);
    fill("magma_KeyIterator_ItemsRead",
         magmaStats->KeyStats.NumIteratorItemsRead);
    fill("magma_KeyIterator_ItemsSkipped",
         magmaStats->KeyStats.NumIteratorItemsSkipped);
    fill("magma_SeqIterator_ItemsRead",
         magmaStats->SeqStats.NumIteratorItemsRead);
    fill("magma_SeqIterator_ItemsSkipped",
         magmaStats->SeqStats.NumIteratorItemsSkipped);
    fill("magma_NGets", magmaStats->NGets);
    fill("magma_NSets", magmaStats->NSets);
    fill("magma_NInserts", magmaStats->NInserts);
    fill("magma_NReadIO", magmaStats->NReadIOs);
    fill("magma_NReadBytesCompact", magmaStats->NReadBytesCompact);
    fill("magma_BytesIncoming", magmaStats->BytesIncoming);
    fill("magma_KeyIndex_BytesIncoming", magmaStats->KeyStats.BytesIncoming);
    fill("magma_SeqIndex_BytesIncoming", magmaStats->SeqStats.BytesIncoming);
    fill("magma_SeqIndex_Delta_BytesIncoming",
         magmaStats->SeqStats.DeltaBytesIncoming);
    fill("magma_NWriteBytes", magmaStats->NWriteBytes);
    fill("magma_FSWriteBytes", magmaStats->FSWriteBytes);
    fill("magma_KeyIndex_NWriteBytes", magmaStats->KeyStats.NWriteBytes);
    fill("magma_SeqIndex_NWriteBytes", magmaStats->SeqStats.NWriteBytes);
    fill("magma_KeyIndex_NWriteBytesFileCountCompact",
         magmaStats->KeyStats.FileCountCompactWriter.NWriteBytes);
    fill("magma_SeqIndex_NWriteBytesFileCountCompact",
         magmaStats->SeqStats.FileCountCompactWriter.NWriteBytes);

    size_t deltaNWriteBytes = 0;
    auto& levelStats = magmaStats->SeqStats.LevelStats;
    for (size_t i = 0; !levelStats.empty() && i < levelStats.size() - 1; i++) {
        deltaNWriteBytes += levelStats[i].NWriteBytes;
    }
    fill("magma_SeqIndex_Delta_NWriteBytes", deltaNWriteBytes);

    fill("magma_NWriteBytesCompact", magmaStats->NWriteBytesCompact);
    fill("magma_LogicalDataSize", magmaStats->LogicalDataSize);
    fill("magma_LogicalDiskSize", magmaStats->LogicalDiskSize);
    fill("magma_HistoryLogicalDiskSize", magmaStats->HistoryLogicalDiskSize);
    fill("magma_HistoryLogicalDataSize", magmaStats->HistoryLogicalDataSize);
    fill("magma_TotalDiskUsage", magmaStats->TotalDiskUsage);
    fill("magma_WastedSpace", magmaStats->WastedSpace);
    fill("magma_CheckpointOverhead", magmaStats->CheckpointOverhead);
    fill("magma_ActiveDiskUsage", magmaStats->ActiveDiskUsage);
    fill("magma_WALDiskUsage", magmaStats->WalStats.DiskUsed);
    fill("magma_BlockCacheMemUsed", magmaStats->BlockCacheMemUsed);
    fill("magma_KeyIndexSize", magmaStats->KeyStats.LogicalDataSize);
    fill("magma_KeyIndexNTableFiles", magmaStats->KeyStats.NTableFiles);

    fill("magma_SeqIndex_IndexBlockSize",
         magmaStats->SeqStats.TotalIndexBlocksSize);
    fill("magma_WriteCacheMemUsed", magmaStats->WriteCacheMemUsed);
    fill("magma_WALMemUsed", magmaStats->WALMemUsed);
    fill("magma_ReadAheadBufferMemUsed", magmaStats->ReadAheadBufferMemUsed);
    fill("magma_TableObjectMemUsed", magmaStats->TableObjectMemUsed);
    fill("magma_LSMTreeObjectMemUsed", magmaStats->LSMTreeObjectMemUsed);
    fill("magma_HistogramMemUsed", magmaStats->HistogramMemUsed);
    fill("magma_TreeSnapshotMemUsed", magmaStats->TreeSnapshotMemoryUsed);
    fill("magma_TableMetaMemUsed", magmaStats->TableMetaMemUsed);
    fill("magma_BufferMemUsed", magmaStats->BufferMemUsed);
    fill("magma_TotalMemUsed", magmaStats->TotalMemUsed);
    fill("magma_TotalBloomFilterMemUsed", magmaStats->TotalBloomFilterMemUsed);
    fill("magma_BlockCacheHits", magmaStats->BlockCacheHits);
    fill("magma_BlockCacheMisses", magmaStats->BlockCacheMisses);
    fill("magma_NTablesDeleted", magmaStats->NTablesDeleted);
    fill("magma_NTablesCreated", magmaStats->NTablesCreated);
    fill("magma_NTableFiles", magmaStats->NTableFiles);
    fill("magma_NSyncs", magmaStats->NSyncs);
    fill("magma_DataBlocksSize", magmaStats->DataBlocksSize);
    fill("magma_DataBlocksCompressSize", magmaStats->DataBlocksCompressSize);

    /* Fusion Stats*/
    fill("fusion_NumSyncs", magmaStats->FusionFSStats.NumSyncs);
    fill("fusion_NumBytesSynced", magmaStats->FusionFSStats.NumBytesSynced);
    fill("fusion_NumLogsMigrated", magmaStats->FusionFSStats.NumLogsMigrated);
    fill("fusion_NumBytesMigrated", magmaStats->FusionFSStats.NumBytesMigrated);
    fill("fusion_LogStoreSize", magmaStats->FusionFSStats.LogStoreSize);
    fill("fusion_LogStoreGarbageSize",
         magmaStats->FusionFSStats.LogStoreGarbageSize);
    fill("fusion_NumLogsCleaned", magmaStats->FusionFSStats.NumLogsCleaned);
    fill("fusion_NumLogCleanBytesRead",
         magmaStats->FusionFSStats.NumLogCleanBytesRead);
    fill("fusion_ExtentMergerReads",
         magmaStats->FusionFSStats.ExtentMergerReads);
    fill("fusion_ExtentMergerBytesRead",
         magmaStats->FusionFSStats.ExtentMergerBytesRead);
    fill("fusion_LogStorePendingDeleteSize",
         magmaStats->FusionFSStats.LogStorePendingDeleteSize);
    fill("fusion_NumLogCleanReads", magmaStats->FusionFSStats.NumLogCleanReads);
    fill("fusion_NumLogStoreRemotePuts",
         magmaStats->FusionFSStats.NumLogStoreRemotePuts);
    fill("fusion_NumLogStoreReads", magmaStats->FusionFSStats.NumLogStoreReads);
    fill("fusion_NumLogStoreRemoteGets",
         magmaStats->FusionFSStats.NumLogStoreRemoteGets);
    fill("fusion_NumLogStoreRemoteLists",
         magmaStats->FusionFSStats.NumLogStoreRemoteLists);
    fill("fusion_NumLogStoreRemoteDeletes",
         magmaStats->FusionFSStats.NumLogStoreRemoteDeletes);
    fill("fusion_FileMapMemUsed", magmaStats->FusionFSStats.FileMapMemUsed);
    fill("fusion_NumSyncFailures", magmaStats->FusionFSStats.NumSyncFailures);
    fill("fusion_NumMigrationFailures",
         magmaStats->FusionFSStats.NumMigrationFailures);
    fill("fusion_NumLogsMounted", magmaStats->FusionFSStats.NumLogsMounted);
    fill("fusion_SyncSessionTotalBytes",
         magmaStats->FusionFSStats.SyncSessionTotalBytes);
    fill("fusion_SyncSessionCompletedBytes",
         magmaStats->FusionFSStats.SyncSessionCompletedBytes);
    fill("fusion_MigrationTotalBytes",
         magmaStats->FusionFSStats.MigrationTotalBytes);
    fill("fusion_MigrationCompletedBytes",
         magmaStats->FusionFSStats.MigrationCompletedBytes);
    fill("fusion_NumLogSegments", magmaStats->FusionFSStats.NumLogSegments);
    fill("fusion_NumFileExtents", magmaStats->FusionFSStats.NumFileExtents);
    fill("fusion_NumFiles", magmaStats->FusionFSStats.NumFiles);
    fill("fusion_TotalFileSize", magmaStats->FusionFSStats.TotalFileSize);
    return stats;
}

void MagmaKVStore::addStats(const AddStatFn& add_stat, CookieIface& c) const {
    KVStore::addStats(add_stat, c);
    const auto prefix = getStatsPrefix();

    auto stats = magma->GetStats();
    auto statName = prefix + ":magma";
    add_casted_stat(statName, stats->JSON().dump(), add_stat, c);
}

void MagmaKVStore::pendingTasks() {
    std::queue<std::tuple<Vbid, uint64_t>> vbucketsToDelete;
    vbucketsToDelete.swap(*pendingVbucketDeletions.wlock());
    while (!vbucketsToDelete.empty()) {
        Vbid vbid;
        uint64_t vb_version;
        std::tie(vbid, vb_version) = vbucketsToDelete.front();
        vbucketsToDelete.pop();
        delVBucket(vbid, std::make_unique<KVStoreRevision>(vb_version));
    }
}

std::shared_ptr<CompactionContext> MagmaKVStore::makeImplicitCompactionContext(
        Vbid vbid) {
    if (!makeCompactionContextCallback) {
        throw std::runtime_error(
                "MagmaKVStore::makeImplicitCompactionContext: Have not set "
                "makeCompactionContextCallback to create a CompactionContext");
    }

    CompactionConfig config{};
    auto ctx = makeCompactionContextCallback(vbid, config, 0 /*purgeSeqno*/);
    if (!ctx) {
        // if we don't a CompactionContext then return a nullptr so we don't
        // dereference the ctx ptr. This is probably due to the fact that
        // there's no vbucket in memory for this vbid
        return nullptr;
    }

    auto [status, dropped] = getDroppedCollections(vbid);
    if (!status) {
        throw std::runtime_error(fmt::format(
                "MagmaKVStore::makeImplicitCompactionContext: {} failed to "
                "get the dropped collections",
                vbid));
    }

    auto readState = readVBStateFromDisk(vbid);
    if (readState.status != ReadVBStateStatus::Success) {
        std::stringstream ss;
        ss << "MagmaKVStore::makeImplicitCompactionContext " << vbid
           << " failed to read vbstate from disk. Status:"
           << to_string(readState.status);
        throw std::runtime_error(ss.str());
    }
    ctx->highCompletedSeqno = readState.state.persistedCompletedSeqno;

    logger->debug(
            "MagmaKVStore::makeImplicitCompactionContext {} purge_before_ts:{} "
            "purge_before_seq:{}"
            " drop_deletes:{} retain_erroneous_tombstones:{}",
            vbid,
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
    auto [status, vbinfo] = magma->GetStatsForDbInfo(vbid.get());
    if (!status) {
        logger->warn("MagmaKVStore::getDbFileInfo failed status:{}",
                     status.String());
    }
    logger->debug(
            "MagmaKVStore::getDbFileInfo {} spaceUsed:{} fileSize:{} "
            "historyDiskSize:{} historyStartTimestamp:{} status:{}",
            vbid,
            vbinfo.spaceUsed,
            vbinfo.fileSize,
            vbinfo.historyDiskSize,
            vbinfo.historyStartTimestamp.count(),
            status.String());
    return vbinfo;
}

DBFileInfo MagmaKVStore::getAggrDbFileInfo() {
    const auto stats = magma->GetStats();
    // Compressed size.
    const size_t nonHistoryDiskSize =
            stats->TotalDiskUsage - stats->HistoryDiskUsage;
    // Magma internally calculates fragmentation ratio based on the
    // uncompressed disk and data sizes. Due to block compression, it cannot
    // give an accurate estimate of the compressed data size (unfragmented
    // disk). This is because only after a compaction can we know which
    // unfragmented docs will get together to form data blocks.
    //
    // To keep couch_docs_fragmentation inline with Magma's internal
    // fragmentation, we hence have to derive the compressed data size using
    // the internal ratio and compressed disk size. This is why
    // DBFileInfo.spaceUsed reported by Magma does not make much sense and is
    // only reported so that fragmentation can be computed.
    const auto nonHistoryDataSize = static_cast<size_t>(
            nonHistoryDiskSize * (1 - stats->Fragmentation));
    // @todo MB-42900: Track on-disk-prepare-bytes
    DBFileInfo vbinfo{
            stats->TotalDiskUsage,
            nonHistoryDataSize,
            0 /*prepareBytes*/,
            stats->HistoryDiskUsage,
            std::chrono::seconds(stats->SeqStats.HistoryStartTimestamp)};
    return vbinfo;
}

Status MagmaKVStore::writeVBStateToDisk(Vbid vbid, const VB::Commit& meta) {
    LocalDbReqs localDbReqs;
    addVBStateUpdateToLocalDbReqs(localDbReqs,
                                  meta.proposedVBState,
                                  kvstoreRevList[getCacheSlot(vbid)]);

    WriteOps writeOps;
    addLocalDbReqs(localDbReqs, writeOps);

    auto status = magma->WriteDocs(vbid.get(),
                                   writeOps,
                                   kvstoreRevList[getCacheSlot(vbid)],
                                   toHistoryMode(meta.historical));
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

    if (doSyncEveryBatch) {
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

void MagmaKVStore::setMagmaEnableBlockCache(bool enable) {
    magma->EnableBlockCache(enable);
}

void MagmaKVStore::setMagmaSeqTreeDataBlockSize(size_t value) {
    magma->SetSeqTreeDataBlockSize(value);
}

void MagmaKVStore::setMagmaMinValueBlockSizeThreshold(size_t value) {
    magma->SetMinValueBlockSizeThreshold(value);
}

void MagmaKVStore::setMagmaSeqTreeIndexBlockSize(size_t value) {
    magma->SetSeqTreeIndexBlockSize(value);
}

void MagmaKVStore::setMagmaKeyTreeDataBlockSize(size_t value) {
    magma->SetKeyTreeDataBlockSize(value);
}

void MagmaKVStore::setMagmaKeyTreeIndexBlockSize(size_t value) {
    magma->SetKeyTreeIndexBlockSize(value);
}

void MagmaKVStore::setStorageThreads(ThreadPoolConfig::StorageThreadCount num) {
    configuration.setStorageThreads(num);
    calculateAndSetMagmaThreads();
}

void MagmaKVStore::calculateAndSetMagmaThreads() {
    auto backendThreads = configuration.getStorageThreads();
    auto rawBackendThreads =
            static_cast<size_t>(configuration.getStorageThreads());
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

    // magma will log the number of threads it uses
    magma->SetNumThreads(Magma::ThreadType::Flusher, flushers);
    magma->SetNumThreads(Magma::ThreadType::Compactor, compactors);
}

uint32_t MagmaKVStore::getExpiryOrPurgeTime(const magma::Slice& slice) {
    const auto docMeta = magmakv::getDocMeta(slice);

    auto exptime = docMeta.getExptime();

    if (docMeta.isDeleted()) {
        exptime += configuration.getMetadataPurgeAge();
    }

    return exptime;
}

GetValue MagmaKVStore::getBySeqno(KVFileHandle& handle,
                                  Vbid vbid,
                                  uint64_t seq,
                                  ValueFilter filter) const {
    auto& snapshot = *dynamic_cast<const MagmaKVFileHandle&>(handle).snapshot;
    GetValue rv(nullptr, cb::engine_errc::no_such_key);
    auto [status, key, meta, value] = magma->GetBySeqno(snapshot, seq);
    if (!status.IsOK()) {
        ++st.numGetFailure;
        logger->warn(
                "MagmaKVStore::getBySeqno {} Failed to get seqno:{} kvstore "
                "with status {}",
                vbid,
                seq,
                status);
        return rv;
    }

    if (status.IsOkDocFound()) {
        rv = makeItem(vbid,
                      *key,
                      *meta,
                      *value,
                      filter,
                      getDefaultCreateItemCallback());
        Expects(rv.getStatus() == cb::engine_errc::success);
        return rv;
    }
    return rv;
}

std::unique_ptr<TransactionContext> MagmaKVStore::begin(
        Vbid vbid, std::unique_ptr<PersistenceCallback> pcb) {
    if (!startTransaction(vbid)) {
        return {};
    }

    return std::make_unique<MagmaKVStoreTransactionContext>(
            *this, vbid, std::move(pcb));
}

std::pair<Status, uint64_t> MagmaKVStore::getOldestRollbackableHighSeqno(
        Vbid vbid) {
    Status status;
    DomainAwareUniquePtr<Magma::Snapshot> oldestSnapshot;
    status = magma->GetOldestDiskSnapshot(vbid.get(), oldestSnapshot);
    if (!status || !oldestSnapshot) {
        // Magma will return Status::Code::CheckpointNotFound if no rollbackable
        // checkpoints exist. This is not an error condition since a rollback to
        // any seqno will result in a rollback to zero.
        if (status.ErrorCode() == Status::Code::CheckpointNotFound) {
            // Return int_max as seqno since there is no rollbackable seqno
            return {Status::OK(), std::numeric_limits<uint64_t>::max()};
        }

        logger->warn(
                "MagmaKVStore::getOldestRollbackableHighSeqno {} Failed to "
                "get oldest disk snapshot with status {}",
                vbid,
                status);
        return {status, 0};
    }

    uint64_t seqno{0};
    auto userStats = magma->GetKVStoreUserStats(*oldestSnapshot);
    if (userStats) {
        auto* magmaUserStats = dynamic_cast<MagmaDbStats*>(userStats.get());
        if (magmaUserStats) {
            seqno = magmaUserStats->highSeqno;
        } else {
            logger->warn(
                    "MagmaKVStore::getOldestRollbackableHighSeqno {} Failed to "
                    "cast UserStats to MagmaDbStats",
                    vbid);
        }
    }

    return {status, seqno};
}

void MagmaKVStore::setHistoryRetentionBytes(size_t bytes, size_t nVbuckets) {
    Expects(nVbuckets);
    configuration.setHistoryRetentionSize(bytes);
    magma->SetHistoryRetentionSize(bytes / nVbuckets);
}

void MagmaKVStore::setHistoryRetentionSeconds(std::chrono::seconds seconds) {
    configuration.setHistoryRetentionTime(seconds);
    magma->SetHistoryRetentionTime(seconds);
}

std::optional<uint64_t> MagmaKVStore::getHistoryStartSeqno(Vbid vbid) {
    return magma->GetOldestHistorySeqno(vbid.get());
}

bool MagmaKVStore::isContinuousBackupStarted(Vbid vbid) {
    return continuousBackupStatus[getCacheSlot(vbid)] == BackupStatus::Started;
}

void MagmaKVStore::setContinuousBackupInterval(std::chrono::seconds interval) {
    magma->SetBackupInterval(
            std::chrono::duration_cast<std::chrono::minutes>(interval));
}

std::filesystem::path MagmaKVStore::getContinuousBackupPath(
        Vbid vbid, const vbucket_state& committedState) {
    const auto& failovers = committedState.transition.failovers;
    if (failovers.empty()) {
        throw std::runtime_error(
                fmt::format("MagmaKVStore::getContinuousBackupPath: failover "
                            "table is empty"));
    }

    auto vbUuid = failovers.front()["id"].template get<size_t>();
    return configuration.getContinuousBackupPath() /
           fmt::to_string(vbid.get()) / fmt::to_string(vbUuid);
}

std::pair<Status, std::string> MagmaKVStore::onContinuousBackupCallback(
        Vbid vbid, magma::Magma::Snapshot& snapshot) {
    // Work is done in the KV domain, but the flatbuffer result is constructed
    // and returned under Magma.
    cb::UseArenaMallocPrimaryDomain domainGuard;
    auto start = cb::time::steady_clock::now();
    auto statUpdate = folly::makeGuard([this, start]() {
        st.continuousBackupCallbackCount.fetch_add(1);
        st.continuousBackupCallbackTime +=
                std::chrono::duration_cast<std::chrono::microseconds>(
                        cb::time::steady_clock::now() - start);
    });

    auto [magmaStatus, vbstateString] =
            readLocalDoc(vbid, snapshot, LocalDocKey::vbstate);

    if (!magmaStatus.IsOkDocFound()) {
        logger->warnWithContext(
                "MagmaKVStore::continuousBackupCallback failed to read vbstate "
                "from disk",
                {{"vb", vbid}, {"error", magmaStatus.String()}});
        // Return value allocated against Magma (magma::Status ctor allocates).
        cb::UseArenaMallocSecondaryDomain returnGuard;
        return {magmaStatus, {}};
    }

    vbucket_state vbstate;
    try {
        vbstate = nlohmann::json::parse(vbstateString);
    } catch (const nlohmann::json::exception& e) {
        logger->warnWithContext(
                "MagmaKVStore::continuousBackupCallback failed to parse the "
                "vbstate",
                {{"vb", vbid}, {"error", e.what()}});
        // Return value allocated against Magma (magma::Status ctor allocates).
        cb::UseArenaMallocSecondaryDomain returnGuard;
        return {Status(Status::Internal, e.what()), {}};
    }

    std::vector<vbucket_failover_t> failovers;
    for (const auto& e : vbstate.transition.failovers) {
        failovers.push_back({e["id"], e["seq"]});
    }

    Collections::KVStore::Manifest manifest{
            Collections::KVStore::Manifest::Empty{}};
    std::tie(magmaStatus, manifest) = getCollectionsManifest(vbid, snapshot);
    if (!magmaStatus) {
        // Return value allocated against Magma (magma::Status ctor allocates).
        cb::UseArenaMallocSecondaryDomain returnGuard;
        return {magmaStatus, {}};
    }

    // The return value, including the metadata, is accounted against Magma.
    cb::UseArenaMallocSecondaryDomain returnGuard;
    auto metadata =
            Backup::encodeBackupMetadata(vbstate.maxCas, failovers, manifest);
    return {Status::OK(), std::move(metadata)};
}

std::pair<Status, std::string> MagmaKVStore::onContinuousBackupCallback(
        const KVFileHandle& kvFileHandle) {
    auto& magmaFileHandle =
            dynamic_cast<const MagmaKVFileHandle&>(kvFileHandle);
    return onContinuousBackupCallback(magmaFileHandle.vbid,
                                      *magmaFileHandle.snapshot.get());
}

cb::engine_errc MagmaKVStore::checkFusionStatCallStatus(
        FusionStat stat, Vbid vbid, magma::Status status) const {
    if (status.ErrorCode() != Status::Code::Ok) {
        if (status.ErrorCode() == Status::Code::InvalidKVStore) {
            return cb::engine_errc::not_my_vbucket;
        }
        EP_LOG_WARN_CTX("MagmaKVStore::checkFusionStatCallStatus: ",
                        {"stat", stat},
                        {"vb", vbid.get()},
                        {"status", status.String()});
        return cb::engine_errc::failed;
    }
    return cb::engine_errc::success;
}

std::pair<cb::engine_errc, nlohmann::json> MagmaKVStore::getFusionStats(
        FusionStat stat, Vbid vbid) {
    switch (stat) {
    case FusionStat::Invalid:
        throw std::logic_error("MagmaKVStore::getFusionStats: Invalid");
    case FusionStat::SyncInfo: {
        const auto [status, data] =
                magma->GetFusionSyncInfo(Magma::KVStoreID(vbid.get()));
        return {checkFusionStatCallStatus(stat, vbid, status), data};
    }
    case FusionStat::ActiveGuestVolumes: {
        const auto [status, data] = magma->GetActiveFusionGuestVolumes(
                Magma::KVStoreID(vbid.get()));
        return {checkFusionStatCallStatus(stat, vbid, status), data};
    }
    case FusionStat::Uploader: {
        const auto id = Magma::KVStoreID(vbid.get());
        nlohmann::json json;
        { json["state"] = fusionUploaderManager.getUploaderState(vbid); }
        {
            const auto [status, data] = magma->GetFusionUploaderTerm(id);
            if (const auto errc = checkFusionStatCallStatus(stat, vbid, status);
                errc != cb::engine_errc::success) {
                return {errc, {}};
            }
            json["term"] = data;
        }
        {
            auto [status, data] = magma->GetFusionPendingSyncBytes(id);
            const auto errc = checkFusionStatCallStatus(stat, vbid, status);
            if (errc != cb::engine_errc::success) {
                return {errc, {}};
            }
            json["snapshot_pending_bytes"] = data;
        }
        {
            const auto [status, stats] = magma->GetFusionUploaderStats(id);
            if (const auto errc = checkFusionStatCallStatus(stat, vbid, status);
                errc != cb::engine_errc::success) {
                return {errc, {}};
            }
            json.update(stats);
        }
        // @TODO MB-65656: Magma is currently missing the implementation for
        // last_sync_log_seqno and last_sync_log_term. Once the implementation
        // is done, we can add them here.
        return {cb::engine_errc::success, json};
    }
    case FusionStat::Migration: {
        const auto id = Magma::KVStoreID(vbid.get());
        nlohmann::json json;
        {
            const auto [status, stats] = magma->GetFusionMigrationStats(id);
            if (const auto errc = checkFusionStatCallStatus(stat, vbid, status);
                errc != cb::engine_errc::success) {
                return {errc, {}};
            }
            json.update(stats);
        }
        return {cb::engine_errc::success, json};
    }
    }

    folly::assume_unreachable();
}

std::pair<cb::engine_errc, nlohmann::json>
MagmaKVStore::getFusionStorageSnapshot(std::string_view fusionNamespace,
                                       Vbid vbid,
                                       std::string_view snapshotUuid,
                                       std::time_t validity) {
    const auto res =
            magma->GetFusionStorageSnapshot(std::string(fusionNamespace),
                                            Magma::KVStoreID(vbid.get()),
                                            std::string(snapshotUuid),
                                            validity);
    if (std::get<Status>(res).ErrorCode() != Status::Code::Ok) {
        EP_LOG_WARN_CTX("MagmaKVStore::getFusionStorageSnapshot: ",
                        {"vb", vbid.get()},
                        {"status", std::get<Status>(res).String()},
                        {"fusion_namespace", fusionNamespace},
                        {"snapshot_uuid", snapshotUuid},
                        {"validity", validity});
        return {cb::engine_errc::failed, {}};
    }
    return {cb::engine_errc::success, std::get<nlohmann::json>(res)};
}

cb::engine_errc MagmaKVStore::releaseFusionStorageSnapshot(
        std::string_view fusionNamespace,
        Vbid vbid,
        std::string_view snapshotUuid) {
    const auto res =
            magma->ReleaseFusionStorageSnapshot(std::string(fusionNamespace),
                                                Magma::KVStoreID(vbid.get()),
                                                std::string(snapshotUuid));
    if (res.ErrorCode() != Status::Code::Ok) {
        EP_LOG_WARN_CTX("MagmaKVStore::releaseFusionStorageSnapshot: ",
                        {"vb", vbid.get()},
                        {"status", res.String()},
                        {"fusion_namespace", fusionNamespace},
                        {"snapshot_uuid", snapshotUuid});
        return cb::engine_errc::failed;
    }
    return cb::engine_errc::success;
}

cb::engine_errc MagmaKVStore::setChronicleAuthToken(std::string_view token) {
    magma->SetFusionMetadataStoreAuthToken(std::string(token));
    return cb::engine_errc::success;
}

std::string MagmaKVStore::getChronicleAuthToken() const {
    return magma->GetFusionMetadataStoreAuthToken();
}

std::pair<cb::engine_errc, std::vector<std::string>> MagmaKVStore::mountVBucket(
        Vbid vbid,
        VBucketSnapshotSource source,
        const std::vector<std::string>& paths) {
    // Note: Creating a fusion vbucket is a 2-step operation, ie Mount + Create
    // (in order). So we have to bump the revision here only.
    const auto rev = ++kvstoreRevList[getCacheSlot(vbid)];

    magma::Magma::KVStoreMountConfig config;
    switch (source) {
        using Type = magma::Magma::KVStoreMountConfig::Type;
    case VBucketSnapshotSource::Local:
        config.Source = Type::Local;
        break;
    case VBucketSnapshotSource::Fusion:
        config.Source = Type::Fusion;
        break;
    }
    config.MountPaths = paths;

    const auto res = magma->MountKVStore(Magma::KVStoreID(vbid.get()),
                                         magma::Magma::KVStoreRevision(rev),
                                         config);
    const auto status = std::get<Status>(res);
    if (status.ErrorCode() != Status::Code::Ok) {
        EP_LOG_WARN_CTX("MagmaKVStore::mountVBucket: ",
                        {"vb", vbid},
                        {"status", status.String()});
        return {cb::engine_errc::failed, {}};
    }
    return {cb::engine_errc::success, std::get<std::vector<std::string>>(res)};
}

cb::engine_errc MagmaKVStore::syncFusionLogstore(Vbid vbid) {
    const auto status = magma->SyncKVStore(Magma::KVStoreID(vbid.get()), true);
    if (status.ErrorCode() != Status::Code::Ok) {
        if (status.ErrorCode() == Status::Code::InvalidKVStore) {
            return cb::engine_errc::not_my_vbucket;
        }
        EP_LOG_WARN_CTX("MagmaKVStore::syncFusionLogstore: ",
                        {"vb", vbid},
                        {"status", status.String()});
        return cb::engine_errc::failed;
    }
    return cb::engine_errc::success;
}

std::chrono::seconds MagmaKVStore::getFusionUploadInterval() const {
    return magma->GetFusionUploadInterval();
}

std::chrono::seconds MagmaKVStore::getFusionLogCheckpointInterval() const {
    return magma->GetFusionLogCheckpointInterval();
}

cb::engine_errc MagmaKVStore::setFusionLogStoreURI(std::string_view uri) {
    const auto status = magma->SetFusionLogStoreURI(std::string(uri));
    if (status.ErrorCode() != Status::Code::Ok) {
        EP_LOG_WARN_CTX("MagmaKVStore::SetFusionLogStoreURI: ", {"uri", uri});
        return cb::engine_errc::failed;
    }
    return cb::engine_errc::success;
}

std::string MagmaKVStore::getFusionLogStoreURI() const {
    return magma->GetFusionLogStoreURI();
}

cb::engine_errc MagmaKVStore::setFusionMetadataStoreURI(std::string_view uri) {
    const auto status = magma->SetFusionMetadataStoreURI(std::string(uri));
    if (status.ErrorCode() != Status::Code::Ok) {
        EP_LOG_WARN_CTX("MagmaKVStore::SetFusionMetadataStoreURI: ",
                        {"uri", uri});
        return cb::engine_errc::failed;
    }
    return cb::engine_errc::success;
}

std::string MagmaKVStore::getFusionMetadataStoreURI() const {
    return magma->GetFusionMetadataStoreURI();
}

void MagmaKVStore::setMagmaFusionUploadInterval(std::chrono::seconds value) {
    magma->SetFusionUploadInterval(value);
}

std::chrono::seconds MagmaKVStore::getMagmaFusionUploadInterval() const {
    return magma->GetFusionUploadInterval();
}

void MagmaKVStore::setMagmaFusionLogstoreFragmentationThreshold(float value) {
    magma->SetFusionLogstoreFragmentationThreshold(value);
}

float MagmaKVStore::getMagmaFusionLogstoreFragmentationThreshold() const {
    return magma->GetFusionLogstoreFragmentationThreshold();
}

cb::engine_errc MagmaKVStore::startFusionUploader(Vbid vbid, uint64_t term) {
    return fusionUploaderManager.startUploader(*currEngine, vbid, term);
}

cb::engine_errc MagmaKVStore::doStartFusionUploader(Vbid vbid, uint64_t term) {
    const auto status =
            magma->StartFusionUploader(Magma::KVStoreID(vbid.get()), term);
    fusionUploaderManager.onToggleComplete(vbid);
    if (status.ErrorCode() != Status::Code::Ok) {
        if (status.ErrorCode() == Status::Code::InvalidKVStore) {
            return cb::engine_errc::not_my_vbucket;
        }
        EP_LOG_WARN_CTX("MagmaKVStore::doStartFusionUploader: ",
                        {"vb", vbid},
                        {"term", term},
                        {"status", status.String()});
        return cb::engine_errc::failed;
    }
    return cb::engine_errc::success;
}

cb::engine_errc MagmaKVStore::stopFusionUploader(Vbid vbid) {
    return fusionUploaderManager.stopUploader(*currEngine, vbid);
}

cb::engine_errc MagmaKVStore::doStopFusionUploader(Vbid vbid) {
    const auto status = magma->StopFusionUploader(Magma::KVStoreID(vbid.get()));
    fusionUploaderManager.onToggleComplete(vbid);
    if (status.ErrorCode() != Status::Code::Ok) {
        if (status.ErrorCode() == Status::Code::InvalidKVStore) {
            return cb::engine_errc::not_my_vbucket;
        }
        EP_LOG_WARN_CTX("MagmaKVStore::doStopFusionUploader: ",
                        {"vb", vbid},
                        {"status", status.String()});
        return cb::engine_errc::failed;
    }
    return cb::engine_errc::success;
}

bool MagmaKVStore::isFusionUploader(Vbid vbid) const {
    const auto [status, info] =
            magma->IsFusionUploader(Magma::KVStoreID(vbid.get()));
    checkFusionStatCallStatus(FusionStat::Uploader, vbid, status);
    return info;
}

uint64_t MagmaKVStore::getFusionUploaderTerm(Vbid vbid) const {
    const auto [status, term] =
            magma->GetFusionUploaderTerm(Magma::KVStoreID(vbid.get()));
    checkFusionStatCallStatus(FusionStat::Uploader, vbid, status);
    return term;
}

FusionUploaderState MagmaKVStore::getFusionUploaderState(Vbid vbid) const {
    return fusionUploaderManager.getUploaderState(vbid);
}

std::variant<cb::engine_errc, cb::snapshot::Manifest>
MagmaKVStore::prepareSnapshotImpl(
        const std::filesystem::path& snapshotDirectory,
        Vbid vb,
        std::string_view uuid) {
    magma->SyncKVStore(Magma::KVStoreID(vb.get()), false);
    const auto res = magma->Clone(snapshotDirectory.string(),
                                  Magma::KVStoreID(vb.get()));
    if (std::get<Status>(res).ErrorCode() != Status::Code::Ok) {
        EP_LOG_WARN_CTX("MagmaKVStore::prepareSnapshotImpl Clone failed",
                        {"vb", vb},
                        {"uuid", uuid},
                        {"status", std::get<Status>(res).String()});
        return cb::engine_errc::failed;
    }

    const auto& cloneManifest = std::get<magma::CloneManifest>(res);
    cb::snapshot::Manifest manifest(vb, uuid);
    std::size_t fileid = 1;
    for (const auto& file : cloneManifest.Files) {
        manifest.files.emplace_back(file.FilePath, file.Size, fileid++);
    }

    create_directories(snapshotDirectory / "deks");
    const auto root = std::filesystem::path(configuration.getDBName());
    for (const auto& id : cloneManifest.EncryptionKeyIDs) {
        auto keyfile = cb::dek::util::copyKeyFile(
                id, root / "deks", snapshotDirectory / "deks");
        manifest.deks.emplace_back(
                fmt::format("deks/{}", keyfile.filename().string()),
                file_size(snapshotDirectory / "deks" / keyfile.filename()),
                fileid++);
    }

    if (logger->should_log(spdlog::level::debug)) {
        logger->debugWithContext(
                "MagmaKVStore::prepareSnapshotImpl generated a snapshot",
                {{"manifest", nlohmann::json(manifest)}});
    }

    return manifest;
}