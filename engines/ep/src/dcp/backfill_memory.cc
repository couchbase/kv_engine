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

#include "dcp/backfill_memory.h"
#include "bucket_logger.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/active_stream.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "ephemeral_vb.h"
#include "seqlist.h"
#include "stored-value.h"

#include <phosphor/phosphor.h>
#include <platform/json_log_conversions.h>

// Here we must force call the baseclass (DCPBackfillToStream(s))because of the
// use of multiple inheritance (and virtual inheritance), otherwise stream will
// be null as DCPBackfillToStream() would be used.
DCPBackfillMemoryBuffered::DCPBackfillMemoryBuffered(
        EphemeralVBucketPtr evb,
        std::shared_ptr<ActiveStream> s,
        uint64_t startSeqno,
        uint64_t endSeqno)
    : DCPBackfillToStream(s),
      DCPBackfillBySeqno(startSeqno, endSeqno),
      evb(evb),
      rangeItr(nullptr),
      backfillMaxDuration(
              evb->getConfiguration().getDcpBackfillRunDurationLimit()),
      maxNoProgressDuration(
              getBackfillIdleLimitSeconds(evb->getConfiguration())) {
    TRACE_ASYNC_START1("dcp/backfill",
                       "DCPBackfillMemoryBuffered",
                       this,
                       "vbid",
                       getVBucketId().get());
}

DCPBackfillMemoryBuffered::~DCPBackfillMemoryBuffered() {
    TRACE_ASYNC_END1("dcp/backfill",
                     "DCPBackfillMemoryBuffered",
                     this,
                     "vbid",
                     getVBucketId().get());
}

backfill_status_t DCPBackfillMemoryBuffered::create() {
    TRACE_EVENT1("dcp/backfill",
                 "MemoryBuffered::create",
                 "vbid",
                 (evb->getId()).get());

    std::shared_lock rlh(evb->getStateLock());
    if (evb->getState() == vbucket_state_dead) {
        /* We don't have to close the stream here. Task doing vbucket state
           change should handle stream closure */
        EP_LOG_WARN_CTX(
                "DCPBackfillMemoryBuffered::run(): backfill ended prematurely "
                "with vb in dead state",
                {"vb", getVBucketId()},
                {"start_seqno", startSeqno},
                {"end_seqno", endSeqno});
        return backfill_finished;
    }

    auto stream = streamPtr.lock();
    if (!stream) {
        EP_LOG_WARN_CTX(
                "DCPBackfillMemoryBuffered::create(): backfill create ended "
                "prematurely as the associated stream is deleted by the "
                "producer conn",
                {"vb", getVBucketId()});
        return backfill_finished;
    }

    /* Create range read cursor */
    try {
        auto rangeItrOptional = evb->makeRangeIterator(true /*isBackfill*/);
        if (rangeItrOptional) {
            rangeItr = std::move(*rangeItrOptional);
        } else {
            // Multiple range iterators are permitted, so if one could not be
            // created purgeTombstones must have acquired an exclusive range
            stream->logWithContext(
                    spdlog::level::level_enum::debug,
                    "Deferring backfill creation as another task needs "
                    "exclusive access to a range of the seqlist");
            return backfill_snooze;
        }
    } catch (const std::bad_alloc&) {
        stream->logWithContext(spdlog::level::level_enum::warn,
                               "Alloc error when trying to create a range "
                               "iterator on the sequence list for");
        /* Try backfilling again later; here we snooze because system has
           hit ENOMEM */
        return backfill_snooze;
    }

    const auto purgeSeqno = rangeItr.getHighestPurgedDeletedSeqno();

    bool allowNonRollBackStream = false;
    // Skip rollback due to purgeSeqno if the client has seen all changes
    // in the collections it filters by.
    auto collHigh = evb->getHighSeqnoOfCollections(stream->getFilter());
    if (collHigh.has_value()) {
        allowNonRollBackStream = stream->getStartSeqno() < purgeSeqno &&
                                 stream->getStartSeqno() >= collHigh.value() &&
                                 collHigh.value() <= purgeSeqno;
    }

    // We permit rollback to be skipped if the Stream explicitly asked
    // to ignore purged tombstones.
    if (stream->isIgnoringPurgedTombstones()) {
        allowNonRollBackStream = true;
    }

    /* Check startSeqno against the purge-seqno of the seqList.
     * If the startSeqno != 1 (a 0 to n request) then startSeqno must be
     * greater than purgeSeqno. */
    if ((startSeqno != 1 && (startSeqno <= purgeSeqno)) &&
        !allowNonRollBackStream) {
        EP_LOG_WARN_CTX(
                "DCPBackfillMemoryBuffered::create(): running backfill failed "
                "because startSeqno < purgeSeqno",
                {"vb", getVBucketId()},
                {"start_seqno", startSeqno},
                {"purge_seqno", purgeSeqno});
        stream->setDead(cb::mcbp::DcpStreamEndStatus::Rollback);
        return backfill_finished;
    }

    /* Advance the cursor till start, mark snapshot and update backfill
       remaining count */
    while (rangeItr.curr() != rangeItr.end()) {
        if (static_cast<uint64_t>((*rangeItr).getBySeqno()) >= startSeqno) {
            // Backfill covers the full SeqList range.
            endSeqno = rangeItr.back();

            // Send SnapMarker
            bool markerSent =
                    stream->markDiskSnapshot(startSeqno,
                                             endSeqno,
                                             rangeItr.getHighCompletedSeqno(),
                                             rangeItr.getMaxVisibleSeqno(),
                                             evb->getPurgeSeqno(),
                                             SnapshotType::NoHistory);

            if (markerSent) {
                // @todo: This value may be an overestimate, as it includes
                //  prepares/aborts which will not be sent if the stream is not
                //  sync write aware
                stream->setBackfillRemaining(rangeItr.count());

                /* return success so that we transition to scan */
                return backfill_success;
            }
            // func call complete before exiting, halting the
            // backfill as it is unneeded.
            break;
        }
        ++rangeItr;
    }

    /* Backfill is not needed as startSeqno > rangeItr end seqno */
    complete(*stream);
    return backfill_finished;
}

backfill_status_t DCPBackfillMemoryBuffered::scan() {
    TRACE_EVENT2("dcp/backfill",
                 "MemoryBuffered::scan",
                 "currSeqno",
                 rangeItr.curr(),
                 "endSeqno",
                 endSeqno);

    std::shared_lock rlh(evb->getStateLock());
    if (evb->getState() == vbucket_state_dead) {
        /* We don't have to close the stream here. Task doing vbucket state
           change should handle stream closure */
        EP_LOG_WARN_CTX(
                "DCPBackfillMemoryBuffered::scan(): backfill ended prematurely "
                "with vb in dead state",
                {"vb", getVBucketId()},
                {"start_seqno", startSeqno},
                {"end_seqno", endSeqno});
        return backfill_finished;
    }

    auto stream = streamPtr.lock();
    if (!stream) {
        EP_LOG_WARN_CTX(
                "DCPBackfillMemoryBuffered::scan(): backfill create ended "
                "prematurely as the associated stream is deleted by the "
                "producer conn",
                {"vb", getVBucketId()});
        return backfill_finished;
    }
    if (!(stream->isActive())) {
        /* Stop prematurely if the stream state changes */
        complete(*stream);
        return backfill_finished;
    }

    auto startTime = std::chrono::steady_clock::now();

    /* Read items */
    UniqueItemPtr item;
    while (static_cast<uint64_t>(rangeItr.curr()) <= endSeqno) {
        const auto& osv = *rangeItr;
        try {
            if (!osv.getKey().isInSystemEventCollection()) {
                if (evb->getManifest()
                            .lock(osv.getKey())
                            .isLogicallyDeleted(osv.getBySeqno())) {
                    ++rangeItr;
                    continue;
                }
            }
            // MB-27199: toItem will read the StoredValue members, which are
            // mutated with the HashBucketLock, so get the correct bucket lock
            // before calling StoredValue::toItem
            auto hbl = evb->ht.getLockedBucket(osv.getKey());

            // MB-44079: Skip any prepares that were completed at the point at
            // which we started this backfill. Why? Because the
            // HTTombstonePurger doesn't make items stale in seqno order it's
            // possible for the StaleItemDeleter to purge items out of seqno
            // order. That means that we could purge a commit of a prepare
            // without purging the prepare itself and stream a prepare without
            // a logical completion to a consumer. On transition to active that
            // node would attempt to re-commit the prepare and assertions would
            // fire on the new replicas or the active.
            //
            // Reading this you might ask why can't we just check if a prepare
            // is in the PrepareCommitted state to determine if we should send
            // it or not. The state of a prepare can be updated both outside of
            // the seqlist write lock and regardless of whether or not there is
            // a range read over the StoredValue being modified. This means that
            // the prepare states will not be consistent with what they were
            // when we took the rangeItr originally. To keep what we send
            // consistent we consider the HighCompletedSeqno of the seqlist
            // taken when we created our rangeItr instead. Any prepare committed
            // after we created the rangeItr will still be sent. Aborts will
            // always be sent (even if they have a seqno lower than the HCS) as
            // they are logically tombstones.
            switch (rangeItr->getCommitted()) {
            case CommittedState::CommittedViaMutation:
            case CommittedState::CommittedViaPrepare:
            case CommittedState::PrepareAborted:
                break;
            case CommittedState::Pending:
            case CommittedState::PreparedMaybeVisible:
            case CommittedState::PrepareCommitted:
                if (osv.getBySeqno() <=
                    static_cast<int64_t>(rangeItr.getHighCompletedSeqno())) {
                    ++rangeItr;
                    continue;
                }
            }

            // Ephemeral only supports a durable write level of Majority so
            // instead of storing a durability level in our OrderedStoredValues
            // we can just assume that all durable writes have the Majority
            // level. Given that this is a backfill item (we will send via DCP)
            // we also need to specify an infinite durable write timeout so that
            // we do not lose any durable writes. We can supply these items
            // for all stored values as they are only set if the underlying
            // StoredValue has the CommittedState of Pending.
            item = osv.toItem(getVBucketId(),
                              StoredValue::HideLockedCas::No,
                              StoredValue::IncludeValue::Yes,
                              {{cb::durability::Level::Majority,
                                cb::durability::Timeout::Infinity()}});
            // A deleted ephemeral item stores the delete time under a delete
            // time field, this must be copied to the expiry time so that DCP
            // can transmit the original time of deletion
            if (item->isDeleted()) {
                item->setExpTime(osv.getCompletedOrDeletedTime());
            }
        } catch (const std::bad_alloc&) {
            stream->logWithContext(spdlog::level::level_enum::warn,
                                   "Alloc error when trying to create an item "
                                   "copy from hash table",
                                   {{"seqno", osv.getBySeqno()}});
            /* Try backfilling again later; here we snooze because system has
               hit ENOMEM */
            return backfill_snooze;
        }

        // Note: The call in ActiveStream informs the caller to yield *after*
        //   processing the item. Let's move on to the next item unconditionally
        ++rangeItr;

        const auto seqnoDbg = item->getBySeqno();
        if (!stream->backfillReceived(std::move(item), BACKFILL_FROM_MEMORY)) {
            /* Try backfill again later; here we do not snooze because we
               want to check if other backfills can be run by the
               backfillMgr */
            TRACE_INSTANT1("dcp/backfill", "ScanDefer", "seqno", seqnoDbg);
            stream->logWithContext(
                    spdlog::level::level_enum::debug,
                    "Backfill yields after processing seqno: as scan buffer or "
                    "backfill buffer is full",
                    {{"seqno", seqnoDbg}});
            stream->incrementNumBackfillPauses();
            return backfill_success;
        }

        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - startTime);
        if (duration >= getBackfillMaxDuration()) {
            stream->incrementNumBackfillPauses();
            return backfill_success;
        }
    }

    /* Backfill has ran to completion */
    complete(*stream);

    return backfill_finished;
}

void DCPBackfillMemoryBuffered::complete(ActiveStream& stream) {
    TRACE_EVENT0("dcp/backfill", "MemoryBuffered::complete");
    /* [EPHE TODO]: invalidate cursor sooner before it gets deleted */
    runtime += (std::chrono::steady_clock::now() - runStart);
    stream.completeBackfill(endSeqno, runtime, 0, 0);
    stream.logWithContext(
            spdlog::level::level_enum::debug,
            "Backfill memory task complete",
            {{"start_seqno", startSeqno}, {"end_seqno", endSeqno}});
}

backfill_status_t DCPBackfillMemoryBuffered::scanHistory() {
    // No transition to this method available so error when called.
    throw std::runtime_error(
            "DCPBackfillMemoryBuffered::scanHistory unsupported");
}

DCPBackfill::State DCPBackfillMemoryBuffered::getNextScanState(
        DCPBackfill::State current) {
    // Memory scan always goes Create->Scan->Done, never to ScanHistory
    switch (current) {
    case DCPBackfill::State::Create:
        return DCPBackfill::State::Scan;
    case DCPBackfill::State::Scan:
        return DCPBackfill::State::Done;
    case DCPBackfill::State::ScanHistory:
    case DCPBackfill::State::Done:
        break;
    }
    throw std::invalid_argument(fmt::format(
            "DCPBackfillMemoryBuffered::getNextScanState invalid input {}",
            current));
}

bool DCPBackfillMemoryBuffered::isSlow(const ActiveStream& stream) {
    // Takeover streams are immune to this. Ns_server does not handle the stream
    // close gracefully in some cases. The optional also controls if this
    // feature is enabled
    if (!maxNoProgressDuration || stream.isTakeoverStream()) {
        return false;
    }

    if (!trackedPosition) {
        lastPositionChangedTime = std::chrono::steady_clock::now();
        trackedPosition = rangeItr.curr();
        return false;
    }

    if (*trackedPosition != rangeItr.curr()) {
        // The position has changed, save new position and the time.
        trackedPosition = rangeItr.curr();
        lastPositionChangedTime = std::chrono::steady_clock::now();
        return false;
    }

    // No change in position, check if the limit we are within limit
    if ((std::chrono::steady_clock::now() - lastPositionChangedTime) <
        *maxNoProgressDuration) {
        return false;
    }

    // No change and outside of threshold.
    stream.logWithContext(spdlog::level::level_enum::warn,
                          "DCPBackfillMemoryBuffered::isSlow true, no progress"
                          " has been made on the scan for more than the no "
                          "progress duration",
                          {{"max_no_progress_duration", *maxNoProgressDuration},
                           {"tracked_position", trackedPosition}});
    return true;
}
