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

#include "dcp/backfill_by_seqno_disk.h"
#include "bucket_logger.h"
#include "collections/collection_persisted_stats.h"
#include "collections/vbucket_manifest.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/active_stream.h"
#include "kv_bucket.h"
#include "kvstore/kvstore.h"
#include "vbucket.h"
#include "vbucket_state.h"

#include <mcbp/protocol/dcp_stream_end_status.h>
#include <platform/json_log_conversions.h>

// Here we must force call the baseclass (DCPBackfillToStream(s) )because of the
// use of multiple inheritance (and virtual inheritance), otherwise stream will
// be null as DCPBackfillToStream() would be used.
DCPBackfillBySeqnoDisk::DCPBackfillBySeqnoDisk(KVBucket& bucket,
                                               std::shared_ptr<ActiveStream> s,
                                               uint64_t startSeqno,
                                               uint64_t endSeqno)
    : DCPBackfillDiskToStream(bucket, std::move(s)),
      DCPBackfillBySeqno(startSeqno, endSeqno) {
}

backfill_status_t DCPBackfillBySeqnoDisk::create() {
    auto stream = streamPtr.lock();
    if (!stream) {
        EP_LOG_WARN_CTX(
                "DCPBackfillBySeqnoDisk::create(): backfill create ended "
                "prematurely as the associated stream is deleted by the "
                "producer conn",
                {"vb", getVBucketId()});
        return backfill_finished;
    }

    uint64_t lastPersistedSeqno = bucket.getLastPersistedSeqno(getVBucketId());

    if (lastPersistedSeqno < endSeqno) {
        stream->logWithContext(
                spdlog::level::level_enum::info,
                "Rescheduling backfill because the end seqno is not persisted",
                {{"end_seqno", endSeqno},
                 {"last_persisted_seqno", lastPersistedSeqno}});
        return backfill_snooze;
    }

    auto* kvstore = bucket.getROUnderlying(getVBucketId());
    Expects(kvstore);

    auto valFilter = stream->getValueFilter();

    auto scanCtx = kvstore->initBySeqnoScanContext(
            std::make_unique<BySeqnoDiskCallback>(stream),
            std::make_unique<CacheCallback>(
                    bucket,
                    stream,
                    static_cast<std::chrono::milliseconds>(
                            bucket.getConfiguration()
                                    .getDcpBackfillRunDurationLimit())),
            getVBucketId(),
            startSeqno,
            DocumentFilter::ALL_ITEMS,
            valFilter,
            SnapshotSource::Head);

    if (!scanCtx) {
        stream->logWithContext(
                spdlog::level::level_enum::warn,
                "DCPBackfillBySeqnoDisk::create failed to create scan",
                {{"start_seqno", startSeqno}});
        stream->setDead(cb::mcbp::DcpStreamEndStatus::BackfillFail);
        return backfill_finished;
    }
    // Set the persistedCompletedSeqno of DiskCallback taken from the
    // persistedCompletedSeqno of the scan context so it's consistent with
    // the file handle
    dynamic_cast<BySeqnoDiskCallback&>(scanCtx->getValueCallback())
            .persistedCompletedSeqno = scanCtx->persistedCompletedSeqno;

    auto [collHighSuccess, collHigh] =
            getHighSeqnoOfCollections(*scanCtx, *kvstore, stream->getFilter());
    if (!collHighSuccess) {
        stream->logWithContext(
                spdlog::level::level_enum::warn,
                "DCPBackfillBySeqnoDisk::getHighSeqnoOfCollections(): failed "
                "to access collections stats on disk");
        stream->setDead(cb::mcbp::DcpStreamEndStatus::BackfillFail);
        return backfill_finished;
    }

    bool allowNonRollBackStream = false;
    if (collHigh.has_value()) {
        // For a filtered stream we can avoid scanning if the stats show us that
        // the collection(s) on disk have no data above the startSeqno
        if (collHigh.value() < startSeqno) {
            stream->logWithContext(spdlog::level::level_enum::info,
                                   "DCPBackfillBySeqnoDisk: skipping as "
                                   "collection_high < start",
                                   {{"collection_high_seqno", collHigh.value()},
                                    {"start_seqno", startSeqno}});
            complete(*stream);
            return backfill_finished;
        }

        allowNonRollBackStream =
                stream->getStartSeqno() < scanCtx->purgeSeqno &&
                stream->getStartSeqno() >= collHigh.value() &&
                collHigh.value() <= scanCtx->purgeSeqno;
    }

    // For a collection filtered stream, if the startSeqno is the same as the
    // collection start seqno, then we do not require rollback.
    // This is to support collection filtering which may modify the startSeqno
    // to be > 1 and < purgeSeqno but required items within the collection are
    // requested so no rollback is needed.
    if (auto collStart = stream->getCollectionStartSeqno();
        stream->getFilter().isCollectionFilter() && collStart &&
        startSeqno == *collStart) {
        allowNonRollBackStream = true;
    }

    // We also permit rollback to be skipped if the Stream explicitly asked
    // to ignore purged tombstones.
    if (stream->isIgnoringPurgedTombstones()) {
        allowNonRollBackStream = true;
    }

    // Check startSeqno against the purge-seqno of the opened datafile.
    // 1) A normal stream request would of checked inside streamRequest, but
    //    compaction may have changed the purgeSeqno
    // 2) Cursor dropping can also schedule backfills and they must not re-start
    //    behind the current purge-seqno
    // 3) When the stream request was processed the remotePurgeSeqno could have
    //    been the same as the purge seqno of the vbucket. A compaction could
    //    run (and could have moved the purge seqno forward) before this
    //    backfill run - double check if remotePurgeSeqno is the same as the
    //    local purgeSeqno else rollback.
    //
    // If allowNonRollBackCollectionStream is false then the purge seqno,
    // collections high seqno must have moved or the stream isn't a
    // collection stream, making it not valid to prevent the stream from
    // rolling back.
    //
    // If the startSeqno != 1 (a client 0 to n request becomes 1 to n) then
    // start-seqno must be above purge-seqno
    backfill_status_t status = backfill_finished;
    if (startSeqno != 1 && (startSeqno <= scanCtx->purgeSeqno) &&
        !allowNonRollBackStream &&
        (stream->getRemotePurgeSeqno() != scanCtx->purgeSeqno)) {
        auto vb = bucket.getVBucket(getVBucketId());
        stream->logWithContext(
                spdlog::level::level_enum::warn,
                "DCPBackfillBySeqnoDisk::create(): cannot be scanned. "
                "Associated stream is set to dead state. startSeqno < "
                "purgeSeqno",
                {{"start_seqno", startSeqno},
                 {"purge_seqno", scanCtx->purgeSeqno},
                 {"vb_state",
                  vb ? VBucket::toString(vb->getState()) : "vb not found!!"},
                 {"collection_high_seqno", collHigh}});

        stream->setDead(cb::mcbp::DcpStreamEndStatus::Rollback);
    } else if (!setupForHistoryScan(*stream, *scanCtx, startSeqno)) {
        // setupForHistoryScan returns true if the scan is 100% within history
        // a return of false means we will attempt a scan of the non-history
        // window. Note: this code must change again to fix MB-55590
        bool markerSent = markDiskSnapshot(*stream, *scanCtx, *kvstore);

        if (markerSent) {
            // This value may be an overestimate - it includes prepares/aborts
            // which will not be sent if the stream is not sync write aware
            stream->setBackfillRemaining(scanCtx->documentCount);
            status = backfill_success;
            this->scanCtx = std::move(scanCtx);
        } else {
            complete(*stream);
        }
    } else {
        // setupForHistoryScan returned true. Return success and keep scanCtx
        status = backfill_success;
        this->scanCtx = std::move(scanCtx);
    }

    return status;
}

backfill_status_t DCPBackfillBySeqnoDisk::scan() {
    auto stream = streamPtr.lock();
    if (!stream) {
        EP_LOG_WARN_CTX(
                "DCPBackfillBySeqnoDisk::scan(): backfill scan ended "
                "prematurely as the associated stream is deleted by the "
                "producer conn",
                {"vb", getVBucketId()});
        return backfill_finished;
    }
    if (!(stream->isActive())) {
        stream->logWithContext(spdlog::level::level_enum::warn,
                               "DCPBackfillBySeqnoDisk::scan(): ended "
                               "prematurely as stream is not active");
        return backfill_finished;
    }

    auto* kvstore = bucket.getROUnderlying(getVBucketId());
    Expects(kvstore);

    auto& bySeqnoCtx = dynamic_cast<BySeqnoScanContext&>(*scanCtx);
    auto& cacheCallback =
            static_cast<CacheCallback&>(bySeqnoCtx.getCacheCallback());
    cacheCallback.setBackfillStartTime();
    switch (kvstore->scan(bySeqnoCtx)) {
    case ScanStatus::Success:
        if (!historyScan) {
            complete(*stream);
        }
        return backfill_finished;
    case ScanStatus::Cancelled:
        // Cancelled as vbucket/stream have gone away, normal behaviour
        complete(*stream);
        return backfill_finished;
    case ScanStatus::Yield:
        // Scan should run again (e.g. was paused by callback)
        stream->incrementNumBackfillPauses();
        return backfill_success;
    case ScanStatus::Failed:
        // Scan did not complete successfully. Backfill is missing data,
        // propagate error to stream and (unsuccessfully) finish scan.
        stream->logWithContext(spdlog::level::err,
                               "DCPBackfillBySeqnoDisk::create(): Scan failed "
                               "at lastReadSeqno. Setting "
                               "stream to dead state.",
                               {{"start_seqno", bySeqnoCtx.startSeqno},
                                {"max_seqno", bySeqnoCtx.maxSeqno},
                                {"last_read_seqno", bySeqnoCtx.lastReadSeqno}});
        scanCtx.reset();
        stream->setDead(cb::mcbp::DcpStreamEndStatus::BackfillFail);
        return backfill_finished;
    }
    folly::assume_unreachable();
}

void DCPBackfillBySeqnoDisk::complete(ActiveStream& stream) {
    seqnoScanComplete(stream,
                      scanCtx ? scanCtx->diskBytesRead : 0,
                      scanCtx ? scanCtx->keysScanned : 0,
                      startSeqno,
                      endSeqno,
                      scanCtx ? static_cast<BySeqnoScanContext*>(scanCtx.get())
                                        ->lastReadSeqno
                              : 0);
}

std::pair<bool, std::optional<uint64_t>>
DCPBackfillBySeqnoDisk::getHighSeqnoOfCollections(
        const BySeqnoScanContext& seqnoScanCtx,
        const KVStoreIface& kvStore,
        const Collections::VB::Filter& filter) const {
    if (!seqnoScanCtx.handle) {
        return {false, std::nullopt};
    }

    if (filter.isPassThroughFilter() || filter.isUserVisibleFilter()) {
        return {true, std::nullopt};
    }

    std::optional<uint64_t> collHigh;

    const auto& handle = *seqnoScanCtx.handle;
    for (auto cid : filter) {
        auto [status, collStats] =
                kvStore.getCollectionStats(handle, cid.first);
        if (status == KVStore::GetCollectionStatsStatus::Failed) {
            EP_LOG_WARN_CTX(
                    "DCPBackfillBySeqnoDisk::getHighSeqnoOfCollections(): "
                    "getCollectionStats failed",
                    {"vb", seqnoScanCtx.vbid},
                    {"cid", cid.first});
            return {false, std::nullopt};
        }
        collHigh = std::max(collHigh.value_or(0), collStats.highSeqno.load());
    }

    return {true, collHigh};
}

bool DCPBackfillBySeqnoDisk::markDiskSnapshot(ActiveStream& stream,
                                              BySeqnoScanContext& scanCtx,
                                              const KVStoreIface& kvs) {
    if (stream.getFilter().isLegacyFilter()) {
        return markLegacyDiskSnapshot(stream, scanCtx, kvs);
    }
    // HistoryScan: If a disk snapshot is being "split" into non-history and
    // history ranges, then the endSeqno of this first range should show the
    // entire snapshot. E.g.
    // disk snapshot is [a...d], but split
    // no-history[a..b]
    // history[c..d]
    // Then all of the markers from this backfill stats start:a, end:d and mvs
    // hcs can only be valid once d is reached.
    return stream.markDiskSnapshot(
            startSeqno,
            historyScan ? historyScan->snapshotInfo.range.getEnd()
                        : scanCtx.maxSeqno,
            scanCtx.persistedCompletedSeqno,
            scanCtx.highPreparedSeqno,
            scanCtx.persistedPreparedSeqno,
            scanCtx.maxVisibleSeqno,
            scanCtx.purgeSeqno,
            historyScan ? SnapshotType::NoHistoryPrecedingHistory
                        : SnapshotType::NoHistory);
}

// This function is used for backfills where the stream is configured as a
// 'legacy' stream. That means a DCP stream that can only see the default
// collection and cannot be sent DCP SeqnoAdvanced messages to skip 'gaps' in
// snapshots. The purpose of this function is to set the snapshot end value
// correctly, as seen in MB-47437 the highest item in the disk snapshot is not
// always the correct end value for the legacy stream.
//
// This function makes a few decisions about how to proceed and we can be in
// this function for KV to KV replication only during periods where the cluster
// is mid-upgrade.
//
// 1) If the manifest-UID we read from the disk snapshot is 0, that's the easy
//    case, and is the case we have been relying on for any upgrade to
//    collections. When the manifest-UID is 0.
//    a) only the default collection can exist
//    b) the cluster cannot change the UID (i.e. make a collection config
//       change) until all nodes are collection capable, at this point any
//       KV replication streams are switched from legacy to collection aware.
//    When only the default collection exists then we can mark the disksnapshot
//    as normal, all of the vbstate derived values (e.g. maxVisibleSeqno) are
//    relevant to the snapshot.
//
// 2) If the manifest-UID we read from the disk snapshot is not 0 fail if
//    sync-replication is enabled. This is because this function does not
//    support the discovery of the PCS if other collections are in play (it
//    could), the expectation is that there are no clients we need to support
//    that want sync-replication, other than KV itself (who will not be in
//    legacy mode).
//
//  3) Finally manifest-UID is not 0, and sync-replication is not enabled we
//     can process the backfill, we just need to figure out the maxVisibleSeqno
//     of the default-collection. We have the default collection's high-seqno
//     so that's the starting point for figuring this out.
bool DCPBackfillBySeqnoDisk::markLegacyDiskSnapshot(ActiveStream& stream,
                                                    BySeqnoScanContext& scanCtx,
                                                    const KVStoreIface& kvs) {
    // We enter here for a legacy DCP stream
    // but bail if syncReplication and ! manifest 0 (more work needed to
    // get all markDiskSnapshot parameters)
    const auto uid = kvs.getCollectionsManifestUid(*scanCtx.handle);

    if (!uid.has_value()) {
        // KVStore logs details
        stream.logWithContext(spdlog::level::level_enum::warn,
                              "DCPBackfillBySeqnoDisk::markLegacyDiskSnapshot: "
                              "aborting stream as failed to read uid");
        stream.setDead(cb::mcbp::DcpStreamEndStatus::BackfillFail);
        return false;
    }
    // Note: Replication streams will flip to collection aware after upgrade so
    // won't be here if the uid is > 0
    if (stream.supportSyncWrites() && uid != 0) {
        stream.logWithContext(
                spdlog::level::level_enum::warn,
                "DCPBackfillBySeqnoDisk::markLegacyDiskSnapshot: aborting "
                "stream as it has requested sync-writes + legacy filtering and "
                "manifest-uid is not zero",
                {{"value", uid.value().load()}});
        stream.setDead(cb::mcbp::DcpStreamEndStatus::BackfillFail);
        return false;
    }

    // The manifest-UID is 0, we can return here and mark the snapshot with all
    // of the data we already have.
    if (uid == 0) {
        return stream.markDiskSnapshot(startSeqno,
                                       scanCtx.maxSeqno,
                                       scanCtx.persistedCompletedSeqno,
                                       scanCtx.highPreparedSeqno,
                                       scanCtx.persistedPreparedSeqno,
                                       scanCtx.maxVisibleSeqno,
                                       0,
                                       SnapshotType::NoHistory);
    }

    // Need to figure out the maxSeqno/maxVisibleSeqno for calling
    // markDiskSnapshot, no need for the PCS or timestamp values as we checked
    // sync replication is not enabled.

    // Step 1. get the default collection high-seqno
    const auto [status, stats] =
            kvs.getCollectionStats(*scanCtx.handle, CollectionID::Default);

    // If we do not find stats for the default collection that means either
    // nothing has been flushed to the default collection or the default
    // collection has been dropped (after accepting the streamRequest. For both
    // cases return false, the backfill can be skipped and in-memory streaming
    // will drive the stream
    if (status != KVStore::GetCollectionStatsStatus::Success) {
        if (status == KVStore::GetCollectionStatsStatus::Failed) {
            stream.setDead(cb::mcbp::DcpStreamEndStatus::BackfillFail);
        } else {
            stream.logWithContext(
                    spdlog::level::level_enum::info,
                    "DCPBackfillBySeqnoDisk::markLegacyDiskSnapshot found no "
                    "stats for default collection");
        }
        // return false to ensure we cancel the backfill as we either have
        // failed and have set the stream to dead, or there is no items on disk
        // for the collection as no meta data for the collection was found on
        // disk.
        return false;
    }

    // Lambda function to check if we should end the stream if there are no more
    // items within the collection and we've checked pass the streams end seqno.
    auto endStreamIfNeeded = [&]() -> void {
        // If this stream's end is inf+, when we don't need to check if we
        // need to end the stream
        if (stream.getEndSeqno() == ~0ull) {
            return;
        }
        // Get hold of the vbucket ptr so we can get hold of the collections
        // manifest
        auto vb = bucket.getVBucket(vbid);
        if (!vb) {
            stream.logWithContext(
                    spdlog::level::level_enum::warn,
                    "DCPBackfillBySeqnoDisk::markLegacyDiskSnapshot unable to "
                    "get vbucket");
            stream.setDead(cb::mcbp::DcpStreamEndStatus::BackfillFail);
            return;
        }
        // lock the default manifests stats so we can read the high seqno of the
        // default collection
        auto handle = vb->getManifest().lock(CollectionID::Default);
        if (!handle.valid()) {
            stream.logWithContext(
                    spdlog::level::level_enum::warn,
                    "DCPBackfillBySeqnoDisk::markLegacyDiskSnapshot(): failed "
                    "to find Default collection, in the manifest");
            // We can't end the stream early as the collection is being dropped
            // but there might still be seqno's for the DCP Client
            return;
        }

        // End the stream if all the default collection's mutations are on disk
        // with "endSeqno" representing the end of the scan range for the
        // backfill
        if (stream.getEndSeqno() <= endSeqno &&
            handle.getHighSeqno() <= endSeqno) {
            stream.setDead(cb::mcbp::DcpStreamEndStatus::Ok);
        }
    };

    // Step 2. get the item @ the high-seqno
    const auto gv = kvs.getBySeqno(*scanCtx.handle,
                                   stream.getVBucket(),
                                   stats.highSeqno,
                                   ValueFilter::KEYS_ONLY);

    if (gv.getStatus() == cb::engine_errc::success) {
        if (gv.item->isCommitted()) {
            // Step 3. If this is a committed item, done.
            return stream.markDiskSnapshot(startSeqno,
                                           stats.highSeqno,
                                           {},
                                           {},
                                           {},
                                           stats.highSeqno,
                                           0,
                                           SnapshotType::NoHistory);
        }
    } else if (gv.getStatus() != cb::engine_errc::no_such_key) {
        stream.logWithContext(
                spdlog::level::level_enum::warn,
                "DCPBackfillBySeqnoDisk::markLegacyDiskSnapshot failed "
                "getBySeqno",
                {{"status", gv.getStatus()}});
        stream.setDead(cb::mcbp::DcpStreamEndStatus::BackfillFail);
        return false;
    }

    // Step 4. The *slow* path, we're in a situation where we need to find the
    // max visible seqno of the default collection due to the high seqno states:
    //  1. It points to a document in the prepared namespace
    //  2. It points to a document that has been tombstone purged
    //
    // This basic implementation will scan the seqno index (not reading values).
    // Possible improvements if required could be to do a key index scan in the
    // default collection range (maybe if the default collection was a small %
    // of the total vbucket).
    stream.logWithContext(
            spdlog::level::level_enum::info,
            "DCPBackfillBySeqnoDisk::markLegacyDiskSnapshot is scanning for "
            "the highest committed default item from to",
            {{"start_seqno", startSeqno},
             {"high_seqno", stats.highSeqno.load()}});

    // Basic callback that checks for the default collection's highest
    // committed item
    struct FindMaxCommittedItem : public StatusCallback<GetValue> {
        FindMaxCommittedItem(uint64_t maxSeqno) : maxSeqno(maxSeqno) {
        }

        void callback(GetValue& val) override {
            // Scan can stop once we go past the maxSeqno. Set status so that
            // the scan will yield
            if (uint64_t(val.item->getBySeqno()) > maxSeqno) {
                yield();
            }

            if (val.item->getKey().isInDefaultCollection() &&
                val.item->isCommitted()) {
                maxVisibleSeqno = std::max<uint64_t>(maxVisibleSeqno,
                                                     val.item->getBySeqno());
            }
        }
        const uint64_t maxSeqno{0};
        uint64_t maxVisibleSeqno{0};
    };

    // Set the end seqno to be the high seqno of the collection. However, if the
    // maxVisibleSeqno is lower then use it instead as we know that we can't
    // send anything greater than it to the client.
    auto endSeqnoForScan =
            std::min(scanCtx.maxVisibleSeqno, stats.highSeqno.load());

    // Less than pretty, but we want to scan the already open file, no opening
    // a new scan. So we create a new BySeqnoScanContext with callbacks bespoke
    // to the needs of this function and take the handle from the scanCtx
    auto scanForHighestCommitttedItem = kvs.initBySeqnoScanContext(
            std::make_unique<FindMaxCommittedItem>(endSeqnoForScan),
            std::make_unique<NoLookupCallback>(),
            scanCtx.vbid,
            startSeqno,
            DocumentFilter::ALL_ITEMS,
            ValueFilter::KEYS_ONLY,
            SnapshotSource::Head,
            std::move(scanCtx.handle));
    if (!scanForHighestCommitttedItem) {
        stream.logWithContext(
                spdlog::level::level_enum::err,
                "DCPBackfillBySeqnoDisk::markLegacyDiskSnapshot "
                "initBySeqnoScanContext() didn't return a scan context");
        // scan_again can be returned, but that is expected when the scan goes
        // past the end default collection high seqno
        stream.setDead(cb::mcbp::DcpStreamEndStatus::BackfillFail);
        return false;
    }
    // Amend the max seqno to be the min of high seqno of the default collection
    // or max visible seqno as initBySeqnoScanContext() will set it to be the
    // high seqno of the vbucket. This helps to ensure we finish the scan before
    // needing to call the get value callback.
    scanForHighestCommitttedItem->maxSeqno = endSeqnoForScan;

    const auto scanStatus = kvs.scan(*scanForHighestCommitttedItem);
    if (scanStatus == ScanStatus::Failed) {
        // scan_again can be returned, but that is expected when the scan goes
        // past the end default collection high seqno
        stream.setDead(cb::mcbp::DcpStreamEndStatus::BackfillFail);
        return false;
    }

    // Give the handle back to the main document scan
    scanCtx.handle = std::move(scanForHighestCommitttedItem->handle);

    auto& cb = static_cast<FindMaxCommittedItem&>(
            scanForHighestCommitttedItem->getValueCallback());

    if (cb.maxVisibleSeqno > 0) {
        const auto backfillRangeEndSeqno = endSeqno;
        // If the 'stream.getEndSeqno()' is the same as 'backfillRangeEndSeqno'
        // we've just scanned and the 'maxVisibleSeqno' is less than the
        // 'stream.getEndSeqno()' then we need to set the stream's endSeqno to
        // the 'maxVisibleSeqno'.
        // This will trigger the ActiveStream to send a DcpEndStream with status
        // OK. Which we need to do, as we've got no items to send between the
        // 'maxVisibleSeqno' and 'stream.getEndSeqno()'.
        if (backfillRangeEndSeqno == stream.getEndSeqno() &&
            cb.maxVisibleSeqno < backfillRangeEndSeqno) {
            stream.setEndSeqno(cb.maxVisibleSeqno);
        }
        return stream.markDiskSnapshot(startSeqno,
                                       cb.maxVisibleSeqno,
                                       {},
                                       {},
                                       {},
                                       cb.maxVisibleSeqno,
                                       0,
                                       SnapshotType::NoHistory);
    }
    endStreamIfNeeded();
    // Found nothing committed at all
    return false;
}

backfill_status_t DCPBackfillBySeqnoDisk::scanHistory() {
    return doHistoryScan(bucket, *scanCtx);
}
