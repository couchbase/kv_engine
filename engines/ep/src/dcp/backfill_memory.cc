/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "dcp/backfill_memory.h"
#include "dcp/active_stream_impl.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "ephemeral_vb.h"
#include "seqlist.h"
#include "stored-value.h"

#include <phosphor/phosphor.h>

DCPBackfillMemoryBuffered::DCPBackfillMemoryBuffered(
        EphemeralVBucketPtr evb,
        std::shared_ptr<ActiveStream> s,
        uint64_t startSeqno,
        uint64_t endSeqno)
    : DCPBackfill(s, startSeqno, endSeqno),
      evb(evb),
      state(BackfillState::Init),
      rangeItr(nullptr),
      vbid(evb->getId()) {
    TRACE_ASYNC_START1("dcp/backfill",
                       "DCPBackfillMemoryBuffered",
                       this,
                       "vbid",
                       vbid.get());
}

DCPBackfillMemoryBuffered::~DCPBackfillMemoryBuffered() {
    TRACE_ASYNC_END1("dcp/backfill",
                     "DCPBackfillMemoryBuffered",
                     this,
                     "vbid",
                     vbid.get());
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

    TRACE_EVENT2("dcp/backfill",
                 "MemoryBuffered::run",
                 "vbid",
                 (evb->getId()).get(),
                 "state",
                 uint8_t(state));

    switch (state) {
    case BackfillState::Init:
        return create();
    case BackfillState::Scanning:
        return scan();
    case BackfillState::Done:
        return backfill_finished;
    }

    throw std::logic_error("DCPBackfillDisk::run: Invalid backfill state " +
                           backfillStateToString(state));
}

void DCPBackfillMemoryBuffered::cancel() {
    if (state != BackfillState::Done) {
        complete(true);
    }
}

backfill_status_t DCPBackfillMemoryBuffered::create() {
    TRACE_EVENT1("dcp/backfill",
                 "MemoryBuffered::create",
                 "vbid",
                 (evb->getId()).get());

    auto stream = streamPtr.lock();
    if (!stream) {
        EP_LOG_WARN(
                "DCPBackfillMemoryBuffered::create(): "
                "({}) backfill create ended prematurely as the associated "
                "stream is deleted by the producer conn ",
                getVBucketId());
        transitionState(BackfillState::Done);
        return backfill_finished;
    }

    /* Create range read cursor */
    try {
        auto rangeItrOptional = evb->makeRangeIterator(true /*isBackfill*/);
        if (rangeItrOptional) {
            rangeItr = std::move(*rangeItrOptional);
        } else {
            stream->log(spdlog::level::level_enum::debug,
                        "{}"
                        " Deferring backfill creation as another "
                        "range iterator is already on the sequence list",
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
        stream->setDead(END_STREAM_ROLLBACK);
        return backfill_finished;
    }

    /* Advance the cursor till start, mark snapshot and update backfill
       remaining count */
    while (rangeItr.curr() != rangeItr.end()) {
        if (static_cast<uint64_t>((*rangeItr).getBySeqno()) >= startSeqno) {
            /* Set backfill remaining
               [EPHE TODO]: This will be inaccurate if do not backfill till end
                            of the iterator
             */
            stream->setBackfillRemaining(rangeItr.count());

            /* Determine the endSeqno of the current snapshot.
               We want to send till requested endSeqno, but if that cannot
               constitute a snapshot then we need to send till the point
               which can be called as snapshot end */
            endSeqno = std::max(
                    endSeqno,
                    static_cast<uint64_t>(rangeItr.getEarlySnapShotEnd()));

            /* We want to send items only till the point it is necessary to do
               so */
            endSeqno =
                    std::min(endSeqno, static_cast<uint64_t>(rangeItr.back()));

            /* Mark disk snapshot */
            stream->markDiskSnapshot(
                    startSeqno,
                    endSeqno,
                    evb->getHighCompletedSeqno(),
                    // @todo: Use proper value, not yet propagated by DCP
                    endSeqno /*maxVisibleSeqno*/);

            /* Change the backfill state */
            transitionState(BackfillState::Scanning);

            /* Jump to scan here itself */
            return scan();
        }
        ++rangeItr;
    }

    /* Backfill is not needed as startSeqno > rangeItr end seqno */
    complete(false);
    return backfill_success;
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
        transitionState(BackfillState::Done);
        return backfill_finished;
    }

    if (!(stream->isActive())) {
        /* Stop prematurely if the stream state changes */
        complete(true);
        return backfill_success;
    }

    /* Read items */
    UniqueItemPtr item;
    while (static_cast<uint64_t>(rangeItr.curr()) <= endSeqno) {
        try {
            // MB-27199: toItem will read the StoredValue members, which are
            // mutated with the HashBucketLock, so get the correct bucket lock
            // before calling StoredValue::toItem
            auto hbl = evb->ht.getLockedBucket((*rangeItr).getKey());
            // Ephemeral only supports a durable write level of Majority so
            // instead of storing a durability level in our OrderedStoredValues
            // we can just assume that all durable writes have the Majority
            // level. Given that this is a backfill item (we will send via DCP)
            // we also need to specify an infinite durable write timeout so that
            // we do not lose any durable writes. We can supply these items
            // for all stored values as they are only set if the underlying
            // StoredValue has the CommittedState of Pending.
            item = (*rangeItr).toItem(getVBucketId(),
                                      StoredValue::HideLockedCas::No,
                                      StoredValue::IncludeValue::Yes,
                                      {{cb::durability::Level::Majority,
                                        cb::durability::Timeout::Infinity()}});
            // A deleted ephemeral item stores the delete time under a delete
            // time field, this must be copied to the expiry time so that DCP
            // can transmit the original time of deletion
            if (item->isDeleted()) {
                item->setExpTime(
                        ep_abs_time((*rangeItr).getCompletedOrDeletedTime()));
            }
        } catch (const std::bad_alloc&) {
            stream->log(spdlog::level::level_enum::warn,
                        "Alloc error when trying to create an "
                        "item copy from hash table. Item seqno:{}"
                        ", {}",
                        (*rangeItr).getBySeqno(),
                        getVBucketId());
            /* Try backfilling again later; here we snooze because system has
               hit ENOMEM */
            return backfill_snooze;
        }

        int64_t seqnoDbg = item->getBySeqno();
        if (!stream->backfillReceived(
                    std::move(item), BACKFILL_FROM_MEMORY, /*force*/ false)) {
            /* Try backfill again later; here we do not snooze because we
               want to check if other backfills can be run by the
               backfillMgr */
            TRACE_INSTANT1("dcp/backfill", "ScanDefer", "seqno", seqnoDbg);
            stream->log(spdlog::level::level_enum::debug,
                        "{} Deferring backfill at seqno:{}"
                        "as scan buffer or backfill buffer is full",
                        getVBucketId(),
                        seqnoDbg);
            return backfill_success;
        }
        ++rangeItr;
    }

    /* Backfill has ran to completion */
    complete(false);

    return backfill_success;
}

void DCPBackfillMemoryBuffered::complete(bool cancelled) {
    TRACE_EVENT1(
            "dcp/backfill", "MemoryBuffered::complete", "cancelled", cancelled);

    auto stream = streamPtr.lock();
    if (!stream) {
        EP_LOG_WARN(
                "DCPBackfillMemoryBuffered::complete(): "
                "({}) backfill create ended prematurely as the associated "
                "stream is deleted by the producer conn; {}",
                getVBucketId(),
                cancelled ? "cancelled" : "finished");
        transitionState(BackfillState::Done);
        return;
    }

    /* [EPHE TODO]: invalidate cursor sooner before it gets deleted */

    stream->completeBackfill();

    auto severity = cancelled ? spdlog::level::level_enum::info
                              : spdlog::level::level_enum::debug;
    stream->log(severity,
                "({}) Backfill task ({} to {}) {}",
                getVBucketId(),
                startSeqno,
                endSeqno,
                cancelled ? "cancelled" : "finished");

    transitionState(BackfillState::Done);
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
