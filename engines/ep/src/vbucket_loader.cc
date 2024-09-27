/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "vbucket_loader.h"

#include "bucket_logger.h"
#include "checkpoint_manager.h"
#include "collections/collection_persisted_stats.h"
#include "collections/manager.h"
#include "collections/vbucket_manifest_handles.h"
#include "ep_engine.h"
#include "ep_vb.h"
#include "failover-table.h"
#include "vbucket_state.h"

VBucketLoader::~VBucketLoader() = default;

VBucketLoader::VBucketLoader(EPBucket& st,
                             const Configuration& config,
                             VBucketPtr vb,
                             uint16_t shardId)
    : store(st), config(config), vb(std::move(vb)), shardId(shardId) {
}

VBucketLoader::CreateVBucketStatus VBucketLoader::createVBucket(
        Vbid vbid,
        const vbucket_state& vbs,
        size_t maxFailoverEntries,
        bool cleanShutdown) {
    Expects(!vb);
    auto status = CreateVBucketStatus::Success;
    auto curVb = store.getVBucket(vbid);
    if (curVb) {
        vb = curVb;
        status = CreateVBucketStatus::AlreadyExists;
    } else {
        std::unique_ptr<FailoverTable> table;
        if (vbs.transition.failovers.empty()) {
            table = std::make_unique<FailoverTable>(maxFailoverEntries);
        } else {
            table = std::make_unique<FailoverTable>(vbs.transition.failovers,
                                                    maxFailoverEntries,
                                                    vbs.highSeqno);
        }
        KVShard* shard = store.getVBuckets().getShardByVbId(vbid);

        std::unique_ptr<Collections::VB::Manifest> manifest;
        if (config.isCollectionsEnabled()) {
            auto [getManifestStatus, persistedManifest] =
                    store.getROUnderlyingByShard(shardId)
                            ->getCollectionsManifest(vbid);
            if (!getManifestStatus) {
                return CreateVBucketStatus::FailedReadingCollectionsManifest;
            }

            manifest = std::make_unique<Collections::VB::Manifest>(
                    store.getSharedCollectionsManager(), persistedManifest);
        } else {
            manifest = std::make_unique<Collections::VB::Manifest>(
                    store.getSharedCollectionsManager());
        }

        const auto* topology = vbs.transition.replicationTopology.empty()
                                       ? nullptr
                                       : &vbs.transition.replicationTopology;
        vb = store.makeVBucket(vbid,
                               vbs.transition.state,
                               shard,
                               std::move(table),
                               std::move(manifest),
                               vbs.transition.state,
                               vbs.highSeqno,
                               vbs.lastSnapStart,
                               vbs.lastSnapEnd,
                               vbs.purgeSeqno,
                               vbs.maxCas,
                               vbs.hlcCasEpochSeqno,
                               vbs.mightContainXattrs,
                               topology,
                               vbs.maxVisibleSeqno,
                               vbs.persistedPreparedSeqno);

        if (vbs.transition.state == vbucket_state_active &&
            (!cleanShutdown ||
             store.getCollectionsManager().needsUpdating(*vb))) {
            if (static_cast<uint64_t>(vbs.highSeqno) == vbs.lastSnapEnd) {
                vb->createFailoverEntry(vbs.lastSnapEnd);
            } else {
                vb->createFailoverEntry(vbs.lastSnapStart);
            }
            status = CreateVBucketStatus::SuccessFailover;
        }
        vb->setFreqSaturatedCallback(
                [store = &store]() { store->itemFrequencyCounterSaturated(); });
    }

    // Pass the max deleted seqno for each vbucket.
    vb->ht.setMaxDeletedRevSeqno(vbs.maxDeletedSeqno);

    // For each vbucket, set the last persisted seqno checkpoint
    vb->setPersistenceSeqno(vbs.highSeqno);

    return status;
}

VBucketLoader::LoadCollectionStatsStatus VBucketLoader::loadCollectionStats(
        const KVStoreIface* kvstore) {
    Expects(vb);
    if (!kvstore) {
        kvstore = store.getROUnderlyingByShard(shardId);
    }
    Expects(kvstore);
    // Take the KVFileHandle before we lock the manifest to prevent lock
    // order inversions.
    auto kvstoreContext = kvstore->makeFileHandle(vb->getId());
    if (!kvstoreContext) {
        return LoadCollectionStatsStatus::NoFileHandle;
    }

    std::unique_lock wlh(vb->getStateLock());
    auto wh = vb->getManifest().wlock(wlh);
    // For each collection in the VB, get its stats
    for (auto& collection : wh) {
        // start tracking in-memory stats before items are warmed up.
        // This may be called repeatedly; it is idempotent.
        store.stats.trackCollectionStats(collection.first);

        // getCollectionStats() can still can fail if the data store on disk
        // has been corrupted between the call to makeFileHandle() and
        // getCollectionStats()
        auto [status, stats] =
                kvstore->getCollectionStats(*kvstoreContext, collection.first);
        if (status == KVStore::GetCollectionStatsStatus::Failed) {
            return LoadCollectionStatsStatus::Failed;
        }
        // For NotFound we're ok to use the default initialised stats

        collection.second.setItemCount(stats.itemCount);
        collection.second.setPersistedHighSeqno(stats.highSeqno);
        collection.second.setDiskSize(stats.diskSize);
        // Set the in memory high seqno - might be 0 in the case of the
        // default collection so we have to reset the monotonic value
        collection.second.resetHighSeqno(stats.highSeqno);
    }

    return LoadCollectionStatsStatus::Success;
}

KVBucketIface::LoadPreparedSyncWritesResult
VBucketLoader::loadPreparedSyncWrites() {
    Expects(vb);
    // Our EPBucket function will do the load for us as we re-use the code
    // for rollback.
    auto result = store.loadPreparedSyncWrites(*vb);
    if (result.success) {
        vb->getManifest().setDefaultCollectionLegacySeqnos(
                result.defaultCollectionMaxVisibleSeqno,
                vb->getId(),
                *store.getRWUnderlyingByShard(shardId));
    }
    return result;
}

EPBucket::FlushResult VBucketLoader::addToVBucketMap() {
    Expects(vb);
    // Take the vBucket lock to stop the flusher from racing with our
    // set vBucket state. It MUST go to disk in the first flush batch
    // or we run the risk of not rolling back replicas that we should
    auto lockedVb = store.getLockedVBucket(vb->getId());
    Expects(lockedVb.owns_lock());
    Expects(!lockedVb);

    vb->checkpointManager->queueSetVBState();

    {
        // Note this lock is here for correctness - the VBucket is not
        // accessible yet, so its state cannot be changed by other code.
        std::shared_lock rlh(vb->getStateLock());
        if (vb->getState() == vbucket_state_active) {
            // For all active vbuckets, call through to the manager so
            // that they are made 'current' with the manifest.
            store.getCollectionsManager().maybeUpdate(rlh, *vb);
        }
    }

    auto result =
            store.flushVBucket_UNLOCKED({vb, std::move(lockedVb.getLock())});

    store.vbMap.addBucket(vb);

    return result;
}
