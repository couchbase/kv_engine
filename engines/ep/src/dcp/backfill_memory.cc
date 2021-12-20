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
#include "collections/vbucket_manifest_handles.h"
#include "dcp/active_stream_impl.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "ephemeral_vb.h"
#include "seqlist.h"
#include "stored-value.h"

#include <phosphor/phosphor.h>

// Here we must force call the baseclass (DCPBackfill(s))because of the use of
// multiple inheritance (and virtual inheritance), otherwise stream will be null
// as DCPBackfill() would be used.
DCPBackfillMemoryBuffered::DCPBackfillMemoryBuffered(
        EphemeralVBucketPtr evb,
        std::shared_ptr<ActiveStream> s,
        uint64_t startSeqno,
        uint64_t endSeqno)
    : DCPBackfill(s),
      DCPBackfillBySeqno(startSeqno, endSeqno),
      evb(evb),
      state(BackfillState::Init),
      rangeItr(nullptr) {
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

backfill_status_t DCPBackfillMemoryBuffered::run() {
    folly::SharedMutex::ReadHolder rlh(evb->getStateLock());
    if (evb->getState() == vbucket_state_dead) {
        /* We don't have to close the stream here. Task doing vbucket state
           change should handle stream closure */
        EP_LOG_WARN(
                "DCPBackfillMemoryBuffered::run(): ({}) running backfill ended "
                "prematurely with vb in dead state; start seqno:{}, "
                "end seqno:{}",
                getVBucketId(),
                startSeqno,
                endSeqno);
        return backfill_finished;
    }

    auto runtimeGuard =
            folly::makeGuard([start = std::chrono::steady_clock::now(), this] {
                runtime += (std::chrono::steady_clock::now() - start);
            });

    TRACE_EVENT2("dcp/backfill",
                 "MemoryBuffered::run",
                 "vbid",
                 getVBucketId().get(),
                 "state",
                 uint8_t(state));

    backfill_status_t status = backfill_finished;
    switch (state) {
    case BackfillState::Init:
        status = create();
        break;
    case BackfillState::Scanning:
        status = scan();
        break;
    case BackfillState::Done:
        throw std::logic_error(
                "DCPBackfillMemoryBuffered::run: run should not be called in "
                "BackfillState::done");
    }

    if (status == backfill_finished) {
        transitionState(BackfillState::Done);
    }

    return status;
}

void DCPBackfillMemoryBuffered::cancel() {
    if (state != BackfillState::Done) {
        EP_LOG_WARN(
                "DCPBackfillMemoryBuffered::cancel ({}) cancelled before "
                "reaching State::done",
                getVBucketId());
    }
}

backfill_status_t DCPBackfillMemoryBuffered::create() {
    TRACE_EVENT1("dcp/backfill",
                 "MemoryBuffered::create",
                 "vbid",
                 getVBucketId().get());

    auto stream = streamPtr.lock();
    if (!stream) {
        EP_LOG_WARN(
                "DCPBackfillMemoryBuffered::create(): "
                "({}) backfill create ended prematurely as the associated "
                "stream is deleted by the producer conn ",
                getVBucketId());
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
            stream->log(spdlog::level::level_enum::debug,
                        "{}"
                        " Deferring backfill creation as another "
                        "task needs exclusive access to a range of the seqlist",
                        getVBucketId());
            return backfill_snooze;
        }
    } catch (const std::bad_alloc&) {
        stream->log(spdlog::level::level_enum::warn,
                    "Alloc error when trying to create a range iterator"
                    "on the sequence list for ({})",
                    getVBucketId());
        /* Try backfilling again later; here we snooze because system has
           hit ENOMEM */
        return backfill_snooze;
    }

    /* Check startSeqno against the purge-seqno of the vb.
     * If the startSeqno != 1 (a 0 to n request) then startSeqno must be
     * greater than purgeSeqno. */
    if (startSeqno != 1 && (startSeqno <= evb->getPurgeSeqno())) {
        EP_LOG_WARN(
                "DCPBackfillMemoryBuffered::create(): "
                "({}) running backfill failed because the startSeqno:{} is < "
                "purgeSeqno:{}",
                getVBucketId(),
                startSeqno,
                evb->getPurgeSeqno());
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
                                             {});

            if (markerSent) {
                // @todo: This value may be an overestimate, as it includes
                //  prepares/aborts which will not be sent if the stream is not
                //  sync write aware
                stream->setBackfillRemaining(rangeItr.count());

                /* Change the backfill state and return for next stage. */
                transitionState(BackfillState::Scanning);
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

    auto stream = streamPtr.lock();
    if (!stream) {
        EP_LOG_WARN(
                "DCPBackfillMemoryBuffered::scan(): "
                "({}) backfill create ended prematurely as the associated "
                "stream is deleted by the producer conn ",
                getVBucketId());
        return backfill_finished;
    } else if (!(stream->isActive())) {
        /* Stop prematurely if the stream state changes */
        complete(*stream);
        return backfill_finished;
    }

    /* Read items */
    UniqueItemPtr item;
    while (static_cast<uint64_t>(rangeItr.curr()) <= endSeqno) {
        const auto& osv = *rangeItr;
        try {
            if (!osv.getKey().isInSystemCollection()) {
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
            stream->log(spdlog::level::level_enum::warn,
                        "Alloc error when trying to create an "
                        "item copy from hash table. Item seqno:{}"
                        ", {}",
                        osv.getBySeqno(),
                        getVBucketId());
            /* Try backfilling again later; here we snooze because system has
               hit ENOMEM */
            return backfill_snooze;
        }

        int64_t seqnoDbg = item->getBySeqno();
        if (!stream->backfillReceived(std::move(item), BACKFILL_FROM_MEMORY)) {
            /* Try backfill again later; here we do not snooze because we
               want to check if other backfills can be run by the
               backfillMgr */
            TRACE_INSTANT1("dcp/backfill", "ScanDefer", "seqno", seqnoDbg);
            stream->log(spdlog::level::level_enum::debug,
                        "{} Deferring backfill at seqno:{} "
                        "as scan buffer or backfill buffer is full",
                        getVBucketId(),
                        seqnoDbg);
            return backfill_success;
        }
        ++rangeItr;
    }

    stream->setBackfillScanLastRead(endSeqno);

    /* Backfill has ran to completion */
    complete(*stream);

    return backfill_finished;
}

void DCPBackfillMemoryBuffered::complete(ActiveStream& stream) {
    TRACE_EVENT0("dcp/backfill", "MemoryBuffered::complete");
    /* [EPHE TODO]: invalidate cursor sooner before it gets deleted */
    stream.completeBackfill(runtime, 0);
    stream.log(spdlog::level::level_enum::debug,
               "({}) Backfill memory task ({} to {}) complete",
               getVBucketId(),
               startSeqno,
               endSeqno);
}

void DCPBackfillMemoryBuffered::transitionState(BackfillState newState) {
    if (state == newState) {
        return;
    }

    bool validTransition = false;
    switch (newState) {
    case BackfillState::Init:
        /* Not valid to transition back to 'init' */
        break;
    case BackfillState::Scanning:
        if (state == BackfillState::Init) {
            validTransition = true;
        }
        break;
    case BackfillState::Done:
        if (state == BackfillState::Init || state == BackfillState::Scanning) {
            validTransition = true;
        }
        break;
    }

    if (!validTransition) {
        throw std::invalid_argument(
                "DCPBackfillMemoryBuffered::transitionState:"
                " newState (which is " +
                backfillStateToString(newState) +
                ") is not valid for current state (which is " +
                backfillStateToString(state) + ")");
    }

    state = newState;
}

std::string DCPBackfillMemoryBuffered::backfillStateToString(
        BackfillState state) {
    switch (state) {
    case BackfillState::Init:
        return "initalizing";
    case BackfillState::Scanning:
        return "scanning";
    case BackfillState::Done:
        return "done";
    }
    return "Invalid state"; // dummy to avert certain compiler warnings
}
