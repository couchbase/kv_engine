/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "passive_durability_monitor.h"

#include "active_durability_monitor.h"
#include "dead_durability_monitor.h"
#include "durability_monitor_impl.h"

#include "bucket_logger.h"
#include "item.h"
#include "vbucket.h"
#include "vbucket_state.h"

#include <gsl/gsl-lite.hpp>
#include <platform/optional.h>
#include <statistics/cbstat_collector.h>
#include <utilities/logtags.h>
#include <unordered_map>

PassiveDurabilityMonitor::PassiveDurabilityMonitor(VBucket& vb)
    : vb(vb), state(std::make_unique<State>(*this)) {
    // By design, instances of Container::Position can never be invalid
    auto s = state.wlock();
    s->highPreparedSeqno.it = s->trackedWrites.end();
    s->highCompletedSeqno.it = s->trackedWrites.end();
}

PassiveDurabilityMonitor::PassiveDurabilityMonitor(VBucket& vb,
                                                   int64_t highPreparedSeqno,
                                                   int64_t highCompletedSeqno)
    : PassiveDurabilityMonitor(vb) {
    auto s = state.wlock();
    s->highPreparedSeqno.lastWriteSeqno.reset(highPreparedSeqno);
    s->highCompletedSeqno.lastWriteSeqno.reset(highCompletedSeqno);
}

PassiveDurabilityMonitor::PassiveDurabilityMonitor(
        VBucket& vb,
        int64_t highPreparedSeqno,
        int64_t highCompletedSeqno,
        std::vector<queued_item>&& outstandingPrepares)
    : PassiveDurabilityMonitor(vb, highPreparedSeqno, highCompletedSeqno) {
    auto s = state.wlock();
    int64_t highestPrepareSeqno = -1;
    for (auto& prepare : outstandingPrepares) {
        Expects(highestPrepareSeqno < prepare->getBySeqno());
        highestPrepareSeqno = prepare->getBySeqno();
        s->trackedWrites.emplace_back(std::move(prepare));

        // Advance the highPreparedSeqno iterator to point to the highest
        // SyncWrite which has been prepared.
        auto lastIt = std::prev(s->trackedWrites.end());
        if (lastIt->getBySeqno() <= highPreparedSeqno) {
            s->highPreparedSeqno.it = lastIt;
        }

        // Advance the highCompletedSeqno iterator to point to the highest
        // SyncWrite which has been completed.
        //
        // Note: One might assume that this would always point to
        // trackedWrites.begin(), given that we are a newly minted PassiveDM and
        // hence would only be tracking incomplete SyncWrites. However, we
        // _could_ have been converted from an ActiveDM with null topology which
        // itself was converted from a previous PassiveDM which _did_ have
        // completed SyncWrites still in trackedWrites (because they haven't
        // been persisted locally yet).
        if (lastIt->getBySeqno() <= highCompletedSeqno) {
            s->highCompletedSeqno.it = lastIt;
        }
    }
}

PassiveDurabilityMonitor::PassiveDurabilityMonitor(VBucket& vb,
                                                   DurabilityMonitor&& dm)
    : PassiveDurabilityMonitor(
              vb, dm.getHighPreparedSeqno(), dm.getHighCompletedSeqno()) {
    EP_LOG_INFO(
            "PassiveDurabilityMonitor::ctor(DM&&): {} Transitioning to PDM. "
            "HPS:{}, HCS:{}, numTracked:{}, highestTracked:{}",
            vb.getId(),
            dm.getHighPreparedSeqno(),
            dm.getHighCompletedSeqno(),
            dm.getNumTracked(),
            dm.getHighestTrackedSeqno());
    auto s = state.wlock();

    // The adm will have to (read) lock it's own state to get these for us
    // so grab a copy for our use
    auto highPreparedSeqno = dm.getHighPreparedSeqno();
    auto highCompletedSeqno = dm.getHighCompletedSeqno();

    for (auto& write : dm.getTrackedWrites()) {
        s->trackedWrites.emplace_back(std::move(write));

        // Advance the highPreparedSeqno iterator to point to the highest
        // SyncWrite which has been prepared.
        auto lastIt = std::prev(s->trackedWrites.end());
        if (lastIt->getBySeqno() <= highPreparedSeqno) {
            s->highPreparedSeqno.it = lastIt;
        }

        // Advance the highCompletedSeqno iterator to point to the highest
        // SyncWrite which has been completed.
        //
        // Note: One might assume that this would always point to
        // trackedWrites.begin(), given that we are a newly minted PassiveDM
        // and hence would only be tracking incomplete SyncWrites. However,
        // we _could_ have been converted from an ActiveDM with null
        // topology which itself was converted from a previous PassiveDM
        // which _did_ have completed SyncWrites still in trackedWrites
        // (because they haven't been persisted locally yet).
        if (lastIt->getBySeqno() <= highCompletedSeqno) {
            s->highCompletedSeqno.it = lastIt;
        }
    }

    // When we transition from dead we may have to pass some extra state into
    // the new DM to ensure that we move the HPS appropriately. This extra state
    // is the last consistent point (which is the last point which we could move
    // the HPS up to). Consider the following scenario in which this code is
    // required:
    //
    // 1) vBucket was active - HPS = 0
    // 2) vBucket starts sync write requiring persistence (seqno 1) - HPS = 0
    // 3) vBucket set to dead - HPS = 0
    // 4) vBucket persists sync write from step 3 - HPS = 0
    // 5) vBucket set to replica - HPS = ???
    // 6) vBucket connects to new active and has to send a seqno ack - HPS = ???
    //
    // At step 5/6 we need to be able to move the HPS based on things that may
    // have happened when we are in the dead state but that relate to a previous
    // state that this node saw (i.e. being active at step 2). This is important
    // as the new active may not send a snapshot end if this node is up to date.
    // To do this, the DeadDurabilityMonitor tracks the last consistent point
    // which is either the high seqno if the node was previously active, or the
    // last received snapshot end if the node was previously a replica.
    auto* ddm = dynamic_cast<DeadDurabilityMonitor*>(&dm);
    if (ddm) {
        auto last = ddm->getLastConsistentSeqno();
        if (last && *last != 0) {
            s->processSnapshotEnd(vb.isReceivingDiskSnapshot()
                                          ? CheckpointType::Disk
                                          : CheckpointType::Memory,
                                  *last);
        }
    }

    // Similar to the above case, we want to be resilient to any state
    // transition including active->replica. It's expected that takeovers
    // generally do active->dead->replica transitions, but we don't want to
    // assume that this is always the case. In the active->dead->replica case
    // this code is handled by the DDM, but for an active->replica transition
    // we must handle it here.
    auto* adm = dynamic_cast<ActiveDurabilityMonitor*>(&dm);
    if (adm && vb.getHighSeqno() != 0) {
        s->processSnapshotEnd(vb.isReceivingDiskSnapshot()
                                      ? CheckpointType::Disk
                                      : CheckpointType::Memory,
                              vb.getHighSeqno());
    }
}

PassiveDurabilityMonitor::~PassiveDurabilityMonitor() = default;

std::list<DurabilityMonitor::SyncWrite>
PassiveDurabilityMonitor::getTrackedWrites() const {
    auto s = state.rlock();
    std::list<DurabilityMonitor::SyncWrite> ret;
    for (auto& write : s->trackedWrites) {
        ret.emplace_back(write);
    }
    return ret;
}

std::optional<int64_t> PassiveDurabilityMonitor::getLatestSnapshotEnd() const {
    auto s = state.rlock();
    if (s->receivedSnapshotEnds.empty()) {
        return {};
    }

    return s->receivedSnapshotEnds.back().seqno;
}

void PassiveDurabilityMonitor::addStats(const AddStatFn& addStat,
                                        CookieIface& cookie) const {
    try {
        const auto vbid = vb.getId().get();
        add_casted_stat(fmt::format("vb_{}:state", vbid),
                        VBucket::toString(vb.getState()),
                        addStat,
                        cookie);

        add_casted_stat(fmt::format("vb_{}:high_prepared_seqno", vbid),
                        getHighPreparedSeqno(),
                        addStat,
                        cookie);

        add_casted_stat(fmt::format("vb_{}:high_completed_seqno", vbid),
                        getHighCompletedSeqno(),
                        addStat,
                        cookie);

    } catch (const std::exception& e) {
        EP_LOG_WARN(
                "PassiveDurabilityMonitor::addStats: error building stats: {}",
                e.what());
    }
}

int64_t PassiveDurabilityMonitor::getHighPreparedSeqno() const {
    return state.rlock()->highPreparedSeqno.lastWriteSeqno;
}

int64_t PassiveDurabilityMonitor::getHighCompletedSeqno() const {
    return state.rlock()->highCompletedSeqno.lastWriteSeqno;
}

void PassiveDurabilityMonitor::addSyncWrite(
        queued_item item, std::optional<int64_t> overwritingPrepareSeqno) {
    auto durReq = item->getDurabilityReqs();

    if (durReq.getLevel() == cb::durability::Level::None) {
        throwException<std::invalid_argument>(__func__, "Level::None");
    }
    if (!durReq.getTimeout().isInfinite()) {
        throwException<std::invalid_argument>(
                __func__,
                "timeout is not infinite (PassiveDM cannot safely timeout "
                "SyncWrites)");
    }

    auto s = state.wlock();
    s->checkForAndRemoveDroppedCollections();

    if (overwritingPrepareSeqno) {
        // Remove any trackedWrites with the same key.
        auto itr = s->trackedWrites.begin();
        while (itr != s->trackedWrites.end() &&
               (itr->getKey() != item->getKey() || itr->isCompleted())) {
            itr = s->getIteratorNext(itr);
        }
        if (itr != s->trackedWrites.end()) {
            Expects(itr->getBySeqno() == overwritingPrepareSeqno);
            // We have found a trackedWrite with the same key to remove.
            s->safeEraseSyncWrite(itr);
        }
    }

#if CB_DEVELOPMENT_ASSERTS
    // Additional error checking for dev builds to validate that we don't have
    // any duplicate SyncWrites in trackedWrites. Only done for dev builds
    // as this is likely expensive.
    auto itr = std::find_if(s->trackedWrites.begin(),
                            s->trackedWrites.end(),
                            [&item](const SyncWrite& write) {
                                // Skip any completed SyncWrites
                                return !write.isCompleted() &&
                                       write.getKey() == item->getKey();
                            });
    if (itr != s->trackedWrites.end()) {
        throwException<std::logic_error>(
                __func__,
                fmt::format("Found SyncWrite '{}', whilst attempting to add "
                            "new SyncWrite for key {} with prepare seqno {}",
                            *itr,
                            cb::tagUserData(item->getKey().to_string()),
                            item->getBySeqno()));
    }
#endif

    s->trackedWrites.emplace_back(std::move(item));
    s->totalAccepted++;
}

size_t PassiveDurabilityMonitor::getNumTracked() const {
    return state.rlock()->trackedWrites.size();
}

size_t PassiveDurabilityMonitor::getNumAccepted() const {
    return state.rlock()->totalAccepted;
}
size_t PassiveDurabilityMonitor::getNumCommitted() const {
    return state.rlock()->totalCommitted;
}
size_t PassiveDurabilityMonitor::getNumAborted() const {
    return state.rlock()->totalAborted;
}

void PassiveDurabilityMonitor::notifySnapshotEndReceived(uint64_t snapEnd) {
    { // state locking scope
        auto s = state.wlock();

        auto prevHps = s->highPreparedSeqno.lastWriteSeqno;
        s->processSnapshotEnd(vb.isReceivingDiskSnapshot()
                                      ? CheckpointType::Disk
                                      : CheckpointType::Memory,
                              snapEnd);

        // Store the seqno ack to send after we drop the state lock
        storeSeqnoAck(prevHps, s->highPreparedSeqno.lastWriteSeqno);
    }

    notifySnapEndSeqnoAckPreProcessHook();

    sendSeqnoAck();
}

void PassiveDurabilityMonitor::notifyLocalPersistence() {
    { // state locking scope
        auto s = state.wlock();
        auto prevHps = s->highPreparedSeqno.lastWriteSeqno;
        s->updateHighPreparedSeqno();
        s->checkForAndRemoveDroppedCollections();

        // Store the seqno ack to send after we drop the state lock
        storeSeqnoAck(prevHps, s->highPreparedSeqno.lastWriteSeqno);
    }

    sendSeqnoAck();
}

void PassiveDurabilityMonitor::storeSeqnoAck(int64_t prevHps, int64_t newHps) {
    if (prevHps != newHps) {
        auto seqno = seqnoToAck.wlock();
        if (*seqno < newHps) {
            *seqno = newHps;
        }
    }
}

void PassiveDurabilityMonitor::sendSeqnoAck() {
    // Hold the lock throughout to ensure that we do not race with another ack
    auto seqno = seqnoToAck.wlock();
    if (*seqno != 0) {
        vb.sendSeqnoAck(*seqno);
    }
    *seqno = 0;
}

std::string PassiveDurabilityMonitor::to_string(Resolution res) {
    switch (res) {
    case Resolution::Commit:
        return "commit";
    case Resolution::Abort:
        return "abort";
    }
    folly::assume_unreachable();
}

void PassiveDurabilityMonitor::completeSyncWrite(
        const StoredDocKey& key,
        Resolution res,
        std::optional<uint64_t> prepareSeqno) {
    auto s = state.wlock();

    // If we are receiving a disk snapshot, we need to relax a few checks
    // to account for deduplication. E.g., commits may appear to be out
    // of order
    bool enforceOrderedCompletion = !vb.isReceivingDiskSnapshot();

    if (s->trackedWrites.empty()) {
        if (!enforceOrderedCompletion && res == Resolution::Abort) {
            // An abort might dedupe the associated prepare if received as part
            // of a disk snapshot. VBucket::abort applies numerous checks
            // to ensure the abort is acceptable prior to it reaching this point
            // so simply ignore it here.
            return;
        }

        throwException<std::logic_error>(
                __func__,
                fmt::format("No tracked, but received {} for key:{} with "
                            "prepareSeqno:{}, highSeqno:{}",
                            to_string(res),
                            cb::tagUserData(key.to_string()),
                            to_string_or_none(prepareSeqno),
                            vb.getHighSeqno()));
    }

    // If we can complete out of order, we have to check from the start of
    // tracked writes as the HCS may have advanced past a prepare we have not
    // seen a completion for
    auto next = enforceOrderedCompletion
                        ? s->getIteratorNext(s->highCompletedSeqno.it)
                        : s->trackedWrites.begin();

    if (!enforceOrderedCompletion) {
        // Advance the iterator to the right item, it might not be the first
        //
        // MB-37063: We may find a previous Prepare that has been already
        //   completed but not removed from tracking. That is a legal condition,
        //   refer to the MB for details. Just skip the completed Prepare and
        //   place 'next' to the correct in-flight Prepare (if any).
        //   Some sanity-checks follow to ensure that we are doing the right
        //   thing here.
        while (next != s->trackedWrites.end() &&
               (next->getKey() != key || next->isCompleted())) {
            next = s->getIteratorNext(next);
        }
    }

    // We may hit the case where the first thing(s) in trackedWrites are for
    // collections that have been dropped already but not yet been fully cleaned
    // up by the collections eraser task (compaction). In this case, we will
    // erase any SyncWrites for dropped collections.
    if (enforceOrderedCompletion && !s->droppedCollections.empty() &&
        next != s->trackedWrites.end() && next->getKey() != key) {
        // This should iterate through trackedWrites until the first thing that
        // is not part of a dropped collection
        while (next != s->trackedWrites.end()) {
            auto nextCid = next->getKey().getCollectionID();
            if (s->droppedCollections.find(nextCid) ==
                s->droppedCollections.end()) {
                // Common path - most things won't belong to a dropped
                // Collection
                break;
            } else {
                next = s->safeEraseSyncWrite(next);
            }
        }
    }

    if (next == s->trackedWrites.end()) {
        if (!enforceOrderedCompletion && res == Resolution::Abort) {
            // Ignore abort without matching prepare from disk snapshot as
            // above. See VBucket::abort for restrictions on when this is
            // allowed to happen.
            return;
        }
        throwException<std::logic_error>(
                __func__,
                fmt::format("No Prepare waiting for completion, but received "
                            "{} for key:{}, prepareSeqno:{}, highSeqno:{}, "
                            "enforceOrderedCompletion:{}",
                            to_string(res),
                            cb::tagUserData(key.to_string()),
                            to_string_or_none(prepareSeqno),
                            vb.getHighSeqno(),
                            enforceOrderedCompletion));
    }

    // Sanity checks (on key and seqno) for In-Order Commit
    if ((next->getKey() != key) ||
        (prepareSeqno &&
         next->getBySeqno() != static_cast<int64_t>(*prepareSeqno))) {
        // We want to see if we've seen a prepare for the key and if things
        // are just out of order so check the trackedWrites.
        std::string strSyncWrite("none");
        auto itr = std::find_if(s->trackedWrites.begin(),
                                s->trackedWrites.end(),
                                [&key](const SyncWrite& write) {
                                    return write.getKey() == key;
                                });
        if (itr != s->trackedWrites.end()) {
            strSyncWrite = fmt::format("{}", *itr);
        }

        std::optional<SnapshotEndInfo> lastReceivedSnapEndData;
        if (!s->receivedSnapshotEnds.empty()) {
            lastReceivedSnapEndData = s->receivedSnapshotEnds.back();
        }

        throwException<std::logic_error>(
                __func__,
                fmt::format("Pending resolution for '{}', but received "
                            "unexpected {} for key {} and prepare seqno: {}, "
                            "highSeqno:{},"
                            " HPS:{}, HCS:{}, foundKeyWithSeqno:'{}' "
                            "lastReceivedSnapEnd:'{}'",
                            *next,
                            to_string(res),
                            cb::tagUserData(key.to_string()),
                            *prepareSeqno,
                            vb.getHighSeqno(),
                            s->highPreparedSeqno.lastWriteSeqno,
                            s->highCompletedSeqno.lastWriteSeqno,
                            strSyncWrite,
                            to_string_or_none(lastReceivedSnapEndData)));
    }

    if (enforceOrderedCompletion ||
        next->getBySeqno() > s->highCompletedSeqno.lastWriteSeqno) {
        // Note: Update last-write-seqno first to enforce monotonicity and
        //     avoid any state-change if monotonicity checks fail
        // Do *not* update hcs if this is a commit for a prepare with seqno <=
        // hcs from disk backfill (can be seen due to a deduped commit) as that
        // would move us *backwards* and the monotonic would throw
        s->highCompletedSeqno.lastWriteSeqno = next->getBySeqno();
        s->highCompletedSeqno.it = next;
    }

    // Mark this prepare as completed so that we can allow non-completed
    // duplicates in trackedWrites in case it is not removed because it requires
    // persistence.

    // MB-36735: There is only one case where we may legally end-up with
    // "completing twice" a tracked Prepare at Replica:
    // 1) PDM is tracking a Level::PersistToMajority completed Prepare (that
    //     may happen if the prepare is not locally-satisfied), and..
    // 2) Replica is receiving a disk-snapshot, and..
    // 3) Replica receives an "unprepared abort" (possible only for disk-snap)
    if (next->isCompleted()) {
        Expects(next->getDurabilityReqs().getLevel() ==
                cb::durability::Level::PersistToMajority);
        Expects(s->highPreparedSeqno.lastWriteSeqno < next->getBySeqno());
        Expects(vb.isReceivingDiskSnapshot());
        Expects(res == Resolution::Abort);
    } else {
        next->setStatus(SyncWriteStatus::Completed);
    }

    // HCS may have moved, which could make some Prepare eligible for removal.
    s->checkForAndRemovePrepares();
    s->checkForAndRemoveDroppedCollections();

    switch (res) {
    case Resolution::Commit:
        s->totalCommitted++;
        return;
    case Resolution::Abort:
        s->totalAborted++;
        return;
    }
    folly::assume_unreachable();
}

void PassiveDurabilityMonitor::eraseSyncWrite(const DocKey& key,
                                              int64_t seqno) {
    auto s = state.wlock();

    // Have to start from the beginning of trackedWrites for a couple reasons:
    //
    // 1) Disk snapshots with de-dupe means a prepare could exist before the HCS
    // 2) Completed prepares can exist before the HCS for non disk snapshots if
    //    we have not yet persisted them but have a completion.
    auto toErase = std::find_if(
            s->trackedWrites.begin(),
            s->trackedWrites.end(),
            [key](const auto& write) -> bool { return write.getKey() == key; });

    // We might call into here with a prepare that does not exist in the DM if:
    //
    // 1) The prepare is for a collection that has been dropped
    // 2) A following prepare has been completed triggering the cleanup of
    //    prepares with lower seqnos belonging to dropped collections
    if (toErase == s->trackedWrites.end()) {
        return;
    }

    if (toErase->getBySeqno() != seqno) {
        throwException<std::logic_error>(
                __func__,
                fmt::format(
                        "Attempting to drop prepare for '{}' but seqno does "
                        "not match. Seqno of prepare: {}, seqno given: {}",
                        cb::tagUserData(key.to_string()),
                        toErase->getBySeqno(),
                        seqno));
    }

    // Don't change HCS or HPS values, but we do need to ensure the iterators
    // are correct.
    // Find the correct iterator for setting HCS and HPS. We can't leave them
    // pointing to invalid elements. We always need to move the iterator back
    // to ensure that we never advance the HCS or HPS.
    s->safeEraseSyncWrite(toErase);
}

void PassiveDurabilityMonitor::notifyDroppedCollection(CollectionID cid,
                                                       int64_t seqno) {
    auto s = state.wlock();
    s->droppedCollections[cid] = seqno;
}

size_t PassiveDurabilityMonitor::getNumDroppedCollections() const {
    auto s = state.rlock();
    return s->droppedCollections.size();
}

int64_t PassiveDurabilityMonitor::getHighestTrackedSeqno() const {
    auto s = state.rlock();
    if (!s->trackedWrites.empty()) {
        return s->trackedWrites.back().getBySeqno();
    } else {
        return 0;
    }
}

void PassiveDurabilityMonitor::dump() const {
    toOStream(std::cerr);
}

void PassiveDurabilityMonitor::toOStream(std::ostream& os) const {
    os << "PassiveDurabilityMonitor[" << this << "] " << *state.rlock();
}

PassiveDurabilityMonitor::State::State(const PassiveDurabilityMonitor& pdm)
    : pdm(pdm) {
    highPreparedSeqno.lastWriteSeqno.setLabeler(
            {highPreparedSeqnoPrefix, pdm.vb.getId()});
    highPreparedSeqno.lastAckSeqno.setLabeler(
            {highPreparedSeqnoPrefix, pdm.vb.getId()});
    highCompletedSeqno.lastWriteSeqno.setLabeler(
            {highCompletedSeqnoPrefix, pdm.vb.getId()});
    highCompletedSeqno.lastAckSeqno.setLabeler(
            {highCompletedSeqnoPrefix, pdm.vb.getId()});
}

PassiveDurabilityMonitor::Container::iterator
PassiveDurabilityMonitor::State::safeEraseSyncWrite(
        Container::iterator toErase) {
    // Don't change HCS or HPS values, but we do need to ensure the iterators
    // are correct.
    // Find the correct iterator for setting HCS and HPS. We can't leave them
    // pointing to invalid elements. We always need to move the iterator back
    // to ensure that we never advance the HCS or HPS.
    Expects(toErase != trackedWrites.end());
    auto valid = toErase == trackedWrites.begin() ? trackedWrites.end()
                                                  : std::prev(toErase);

    if (toErase == highPreparedSeqno.it) {
        highPreparedSeqno.it = valid;
    }

    if (toErase == highCompletedSeqno.it) {
        highCompletedSeqno.it = valid;
    }

    return trackedWrites.erase(toErase);
}

PassiveDurabilityMonitor::Container::iterator
PassiveDurabilityMonitor::State::getIteratorNext(
        const Container::iterator& it) {
    // Note: Container::end could be the new position when the pointed SyncWrite
    //     is removed from Container and the iterator repositioned.
    //     In that case next=Container::begin
    return (it == trackedWrites.end()) ? trackedWrites.begin() : std::next(it);
}

void PassiveDurabilityMonitor::State::updateHighPreparedSeqno() {
    // The HPS moves (ie, Prepares are locally-satisfied and ack'ed to Master)
    // at PDM under the following constraints:
    //
    // (1) Nothing is ack'ed before the complete snapshot is received
    //     (I.e., do nothing if receivedSnapshotEnds is empty)
    //
    // (2) Majority and MajorityAndPersistOnMaster Prepares (which don't need to
    //     be persisted for being locally satisfied) may be satisfied as soon as
    //     the complete snapshot is received
    //
    // (3) PersistToMajority Prepares represent a durability-fence. So, at (2)
    //     we can satisfy only Prepares up to before the durability-fence (if
    //     any)
    //
    // (4) The durability-fence can move (ie, PersistToMajority Prepares are
    //     locally-satisfied) only when the complete snapshot is persisted.
    //
    // (5) Once a disk snapshot is fully persisted, the HPS is advanced to the
    //     snapshot end - even if no prepares were seen during the snapshot
    //     or if trackedWrites is empty. This accounts for deduping; there may
    //     have been prepares we have not seen, but they are definitely
    //     satisfied (they are persisted) and should be acked.
    //
    // This function implements all the logic necessary for moving the HPS by
    // enforcing the rules above. The function is called:
    //
    // (a) Every time a snapshot-end is received for the owning VBucket.
    //     That updates the PDM::snapshotEnd and calls down here, where the HPS
    //     is potentially moved (given that a new snapshot-end received may
    //     immediately unblock some pending (locally-unsatisfied) Prepares; eg,
    //     Majority / MajorityAndPersistOnMaster Prepare)
    //
    // (b) Every time the Flusher has run, as persistence may move the
    //     durability-fence (ie, unblock some PersistToMajority Prepares, if
    //     any) and unblock any other Prepare previously blocked on
    //     durability-fence. As already mentioned, we can move the
    //     durability-fence only if the complete snapshot is persisted.

    const auto prevHPS = highPreparedSeqno.lastWriteSeqno;

    // Helper to keep conditions short and meaningful
    const auto inSnapshot = [trackedWritesEnd = trackedWrites.end()](
                                    uint64_t snapshotEndSeqno,
                                    auto prepareItr) {
        return prepareItr != trackedWritesEnd &&
               static_cast<uint64_t>(prepareItr->getBySeqno()) <=
                       snapshotEndSeqno;
    };

    while (!receivedSnapshotEnds.empty()) {
        const auto snapshotEnd = receivedSnapshotEnds.front();

        const bool snapshotFullyPersisted =
                static_cast<int64_t>(pdm.vb.getPersistenceSeqno()) >=
                snapshotEnd.seqno;

        const bool isDiskSnapshot = isDiskCheckpointType(snapshotEnd.type);

        using namespace cb::durability;

        Level maxLevelCanAdvanceOver;

        if (snapshotFullyPersisted) {
            // we have received and persisted an entire snapshot
            // All prepares from this snapshot are satisfied and the state
            // is consistent at snap end. The HPS can advance over Prepares of
            // PersistToMajority or lower (i.e., everything currently)
            maxLevelCanAdvanceOver = Level::PersistToMajority;
        } else if (!isDiskSnapshot) {
            // we have received but NOT persisted an entire snapshot
            //  We *may* be able to advance the HPS part way
            // into this snapshot - The HPS can be advanced over all Prepares of
            // MajorityAndPersistOnMaster level or lower, to the last Prepare
            // immediately preceding an *unpersisted* Prepare with Level ==
            // PersistToMajority. We cannot move the HPS past this Prepare until
            // it *is* persisted.
            maxLevelCanAdvanceOver = Level::MajorityAndPersistOnMaster;
        } else {
            // we have received but NOT persisted an entire *DISK* snapshot
            // we cannot ack anything until the entire snapshot has been
            // persisted because PersistToMajority level Prepares may have been
            // deduped by lower level prepares.
            // Therefore, the HPS cannot advance over *any* prepares.
            maxLevelCanAdvanceOver = Level::None;
        }

        // Advance the HPS, respecting maxLevelCanAdvanceOver
        if (!trackedWrites.empty()) {
            for (auto next = getIteratorNext(highPreparedSeqno.it);
                 inSnapshot(snapshotEnd.seqno, next) &&
                 next->getDurabilityReqs().getLevel() <= maxLevelCanAdvanceOver;
                 next = getIteratorNext(highPreparedSeqno.it)) {
                // Note: Update last-write-seqno first to enforce monotonicity
                // and avoid any state-change if monotonicity checks fail
                highPreparedSeqno.lastWriteSeqno = next->getBySeqno();
                highPreparedSeqno.it = next;
            }
        }

        if (isDiskSnapshot && snapshotFullyPersisted) {
            // Special case - prepares in disk snapshots may have been
            // deduplicated.
            // PRE(persistMajority), CMT, PRE(), ABORT, SET
            // may, after the abort has been purged be sent as:
            // SET
            // We would have no prepare for this op, but we still need to
            // seqno ack something. To resolve this, advance the HPS seqno to
            // the snapshotEndSeqno. There may not be an associated prepare.
            // NB: lastWriteSeqno is NOT guaranteed to match
            // highPreparedSeqno.it->getBySeqno()
            // because of this case
            highPreparedSeqno.lastWriteSeqno = snapshotEnd.seqno;
        }

        // Check if we could have acked everything within the snapshot and
        // might be able to continue checking the next one.
        if ((isDiskSnapshot && !snapshotFullyPersisted) ||
            inSnapshot(snapshotEnd.seqno,
                       getIteratorNext(highPreparedSeqno.it))) {
            // Either we have not fully persisted a disk snapshot and
            // the HPS is left <= the start of this snapshot
            // OR
            // we stopped advancing the HPS before the end of a memory
            // snapshot because we reached a PersistToMajority Prepare
            // HPS now points to the last Prepare before any
            // PersistToMajority
            break;
        }

        receivedSnapshotEnds.pop();
    }

    // We have now acked all the complete, persisted snapshots we received,
    // and advanced the HPS as far as it can go - cannot advance further into a
    // partial snapshot or past a PersistToMajority Prepare

    if (highPreparedSeqno.lastWriteSeqno != prevHPS) {
        Expects(highPreparedSeqno.lastWriteSeqno > prevHPS);
        // HPS has moved, which could make some Prepare eligible for removal.
        checkForAndRemovePrepares();
    }
}

void PassiveDurabilityMonitor::State::checkForAndRemovePrepares() {
    if (trackedWrites.empty()) {
        return;
    }

    // Remove prepares up to the HPS. We can't use the HCS here as we may have
    // prepared something that is not completed (if we have a prepare from a
    // previous snapshot but are in a disk snapshot and are awaiting a
    // completion). We only move the HPS at snapshot end (or persistence) up to
    // some consistent point. At this point we know that all of our completed
    // prepares should be at the beginning of trackedWrites. We will iterate on
    // trackedWrites up to the HPS or the first non complete SyncWrite removing
    // all of them.
    auto it = trackedWrites.begin();
    while (it != trackedWrites.end() &&
           it->getBySeqno() <= highPreparedSeqno.lastWriteSeqno) {
        if (!it->isCompleted()) {
            break;
        }

        // In PassiveDM we have two iterators pointing to items in the tracked
        // Container: the HPS and the High Completed Seqno.
        // Ensure that iterators are never invalid by pointing them to
        // Container::end if the underlying item is removed.
        if (it == highCompletedSeqno.it) {
            highCompletedSeqno.it = trackedWrites.end();
        }
        if (it == highPreparedSeqno.it) {
            highPreparedSeqno.it = trackedWrites.end();
        }

        // Note: 'it' will be invalidated, so it will need to be reset
        const auto next = std::next(it);
        trackedWrites.erase(it);
        it = next;
    }
}

void PassiveDurabilityMonitor::State::checkForAndRemoveDroppedCollections() {
    if (droppedCollections.empty()) {
        return;
    }

    if (trackedWrites.empty()) {
        // Nothing in trackedWrites means we should never need to check for
        // any collection added to droppedCollections and we can remove them
        // all
        droppedCollections.clear();
        return;
    }

    // Remove everything dropped before the first thing in
    // trackedWrites. This isn't perfect, but we expect collection drops
    // to be infrequent compared to SyncWrites so we don't want to
    // iterate trackedWrites here.
    int64_t firstSeqno = trackedWrites.begin()->getBySeqno();

    auto itr = droppedCollections.begin();
    while (itr != droppedCollections.end()) {
        if (itr->second < firstSeqno) {
            itr = droppedCollections.erase(itr);
        } else {
            ++itr;
        }
    }
}

void PassiveDurabilityMonitor::State::processSnapshotEnd(CheckpointType type,
                                                         uint64_t snapEnd) {
    receivedSnapshotEnds.push({int64_t(snapEnd), type});
    // Maybe the new tracked Prepare is already satisfied and could be
    // ack'ed back to the Active.
    updateHighPreparedSeqno();
    checkForAndRemoveDroppedCollections();
}

template <class exception>
[[noreturn]] void PassiveDurabilityMonitor::throwException(
        const std::string& thrower, const std::string& error) const {
    throw exception("PassiveDurabilityMonitor::" + thrower + " " +
                    vb.getId().to_string() + " " + error);
}

std::ostream& operator<<(std::ostream& os,
                         const PassiveDurabilityMonitor::State& state) {
    os << "State[" << &state << "] highPreparedSeqno:"
       << to_string(state.highPreparedSeqno, state.trackedWrites.end())
       << " highCompletedSeqno:"
       << to_string(state.highCompletedSeqno, state.trackedWrites.end())
       << "\ntrackedWrites:[\n";
    for (const auto& w : state.trackedWrites) {
        os << "    " << w << "\n";
    }
    os << "]\n";
    return os;
}
