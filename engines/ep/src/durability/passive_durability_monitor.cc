/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc.
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

#include "passive_durability_monitor.h"
#include "durability_monitor_impl.h"

#include "bucket_logger.h"
#include "item.h"
#include "rollback_result.h"
#include "statwriter.h"
#include "stored-value.h"
#include "vbucket.h"

#include <boost/range/adaptor/reversed.hpp>
#include <gsl.h>
#include <unordered_map>

PassiveDurabilityMonitor::PassiveDurabilityMonitor(VBucket& vb)
    : vb(vb), state(std::make_unique<State>(*this)) {
    // By design, instances of Container::Position can never be invalid
    auto s = state.wlock();
    s->highPreparedSeqno = Position(s->trackedWrites.end());
    s->highCompletedSeqno = Position(s->trackedWrites.end());
}

PassiveDurabilityMonitor::PassiveDurabilityMonitor(
        VBucket& vb, std::vector<queued_item>&& outstandingPrepares)
    : PassiveDurabilityMonitor(vb) {
    auto s = state.wlock();
    for (auto& prepare : outstandingPrepares) {
        // Any outstanding prepares "grandfathered" into the DM should have
        // already specified a non-default timeout.
        Expects(!prepare->getDurabilityReqs().getTimeout().isDefault());
        s->trackedWrites.emplace_back(nullptr,
                                      std::move(prepare),
                                      std::chrono::milliseconds{},
                                      nullptr,
                                      nullptr);
    }
}

PassiveDurabilityMonitor::~PassiveDurabilityMonitor() = default;

void PassiveDurabilityMonitor::addStats(const AddStatFn& addStat,
                                        const void* cookie) const {
    char buf[256];

    try {
        const auto vbid = vb.getId().get();

        checked_snprintf(buf, sizeof(buf), "vb_%d:state", vbid);
        add_casted_stat(buf, VBucket::toString(vb.getState()), addStat, cookie);

        checked_snprintf(buf, sizeof(buf), "vb_%d:high_prepared_seqno", vbid);
        add_casted_stat(buf, getHighPreparedSeqno(), addStat, cookie);

        checked_snprintf(buf, sizeof(buf), "vb_%d:high_completed_seqno", vbid);
        add_casted_stat(buf, getHighCompletedSeqno(), addStat, cookie);

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

void PassiveDurabilityMonitor::addSyncWrite(queued_item item) {
    auto durReq = item->getDurabilityReqs();

    if (durReq.getLevel() == cb::durability::Level::None) {
        throwException<std::invalid_argument>(__func__, "Level::None");
    }
    if (durReq.getTimeout().isDefault()) {
        throwException<std::invalid_argument>(
                __func__,
                "timeout is default (explicit value should have been specified "
                "by Active node)");
    }

    // Need to specify defaultTimeout for SyncWrite ctor, but we've already
    // checked just above the requirements have a non-default value,
    // just pass dummy value here.
    std::chrono::milliseconds dummy{};
    auto s = state.wlock();
    s->trackedWrites.emplace_back(nullptr /*cookie*/,
                                  std::move(item),
                                  dummy,
                                  nullptr /*firstChain*/,
                                  nullptr /*secondChain*/);
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
    int64_t prevHps{0};
    int64_t hps{0};
    {
        auto s = state.wlock();
        s->receivedSnapshotEnds.push({int64_t(snapEnd),
                                      vb.isReceivingDiskSnapshot()
                                              ? CheckpointType::Disk
                                              : CheckpointType::Memory});
        // Maybe the new tracked Prepare is already satisfied and could be
        // ack'ed back to the Active.
        prevHps = s->highPreparedSeqno.lastWriteSeqno;
        s->updateHighPreparedSeqno();
        hps = s->highPreparedSeqno.lastWriteSeqno;
    }

    // HPS may have not changed (e.g., a locally-non-satisfied PersistToMajority
    // Prepare has introduced a durability-fence), which would result in
    // re-acking the same HPS multiple times. Not wrong as HPS is weakly
    // monotonic at Active, but we want to avoid sending unnecessary messages.
    if (hps != prevHps) {
        Expects(hps > prevHps);
        vb.sendSeqnoAck(hps);
    }
}

void PassiveDurabilityMonitor::notifyLocalPersistence() {
    int64_t prevHps{0};
    int64_t hps{0};
    {
        auto s = state.wlock();
        prevHps = s->highPreparedSeqno.lastWriteSeqno;
        s->updateHighPreparedSeqno();
        hps = s->highPreparedSeqno.lastWriteSeqno;
    }

    // HPS may have not changed (e.g., we have just persisted a Majority Prepare
    // for which the HPS has been already increased at ADM::addSyncWrite), which
    // would result in re-acking the same HPS multiple times. Not wrong as HPS
    // is weakly monotonic at Active, but we want to avoid sending unnecessary
    // messages.
    if (hps != prevHps) {
        Expects(hps > prevHps);
        vb.sendSeqnoAck(hps);
    }
}

std::string PassiveDurabilityMonitor::to_string(Resolution res) {
    switch (res) {
    case Resolution::Commit:
        return "commit";
    case Resolution::Abort:
        return "abort";
    case Resolution::CompletionWasDeduped:
        return "completionWasDeduped";
    }
    folly::assume_unreachable();
}

void PassiveDurabilityMonitor::completeSyncWrite(const StoredDocKey& key,
                                                 Resolution res) {
    auto s = state.wlock();

    if (s->trackedWrites.empty()) {
        throwException<std::logic_error>(__func__,
                                         "No tracked, but received " +
                                                 to_string(res) + " for key " +
                                                 key.to_string());
    }

    const auto next = s->getIteratorNext(s->highCompletedSeqno.it);

    if (next == s->trackedWrites.end()) {
        throwException<std::logic_error>(
                __func__,
                "No Prepare waiting for completion, but received " +
                        to_string(res) + " for key " + key.to_string());
    }

    // Sanity check for In-Order Commit
    if (next->getKey() != key) {
        std::stringstream ss;
        ss << "Pending resolution for '" << *next
           << "', but received unexpected " + to_string(res) + " for key "
           << key;
        throwException<std::logic_error>(__func__, "" + ss.str());
    }

    // Note: Update last-write-seqno first to enforce monotonicity and
    //     avoid any state-change if monotonicity checks fail
    s->highCompletedSeqno.lastWriteSeqno = next->getBySeqno();
    s->highCompletedSeqno.it = next;

    // HCS has moved, which could make some Prepare eligible for removal.
    s->checkForAndRemovePrepares();

    switch (res) {
    case Resolution::Commit:
        s->totalCommitted++;
        return;
    case Resolution::Abort:
        s->totalAborted++;
        return;
    case Resolution::CompletionWasDeduped:
        return;
    }
    folly::assume_unreachable();
}

void PassiveDurabilityMonitor::postProcessRollback(
        const RollbackResult& rollbackResult) {
    // Sanity check that new HCS <= new HPS <= new high seqno
    Expects(rollbackResult.highCompletedSeqno <=
            rollbackResult.highPreparedSeqno);
    Expects(rollbackResult.highPreparedSeqno <= rollbackResult.highSeqno);

    auto s = state.wlock();

    // If we rolled back any commits or aborts then we will have put the
    // original prepare into the rollbackResult.preparesToAdd container. This
    // container should be in seqno order. To maintain the seqno ordering of the
    // trackedWrites container, we need to iterate on
    // rollbackResult.preparesToAdd in reverse order and push the items to the
    // front of trackedWrites.
    std::chrono::milliseconds dummy{};
    for (const auto& item :
         boost::adaptors::reverse(rollbackResult.preparesToAdd)) {
        if (static_cast<uint64_t>(item->getBySeqno()) >
            rollbackResult.highCompletedSeqno) {
            // Need to specify defaultTimeout for SyncWrite ctor, but we don't
            // care about the values on replica and have read from disk so give
            // it a dummy timeout.
            s->trackedWrites.emplace_front(nullptr /*cookie*/,
                                           std::move(item),
                                           dummy,
                                           nullptr /*firstChain*/,
                                           nullptr /*secondChain*/);
        }
    }

    // Remove everything with seqno > rollback point from trackedWrites
    auto itr =
            std::find_if(s->trackedWrites.begin(),
                         s->trackedWrites.end(),
                         [&rollbackResult](const auto& write) {
                             return static_cast<uint64_t>(write.getBySeqno()) >
                                    rollbackResult.highSeqno;
                         });
    s->trackedWrites.erase(itr, s->trackedWrites.end());

    // Post-rollback we should not have any prepares in the PDM that have not
    // been completed.
    s->highCompletedSeqno.it = s->trackedWrites.end();
    s->highCompletedSeqno.lastWriteSeqno.reset(
            rollbackResult.highCompletedSeqno);

    // The highPreparedSeqno should always point at the last item in
    // trackedWrites. Every in-flight prepare should be satisfied locally as it
    // will be on disk.
    if (!s->trackedWrites.empty()) {
        s->highPreparedSeqno.it = --s->trackedWrites.end();
    }
    s->highPreparedSeqno.lastWriteSeqno.reset(rollbackResult.highPreparedSeqno);
}

void PassiveDurabilityMonitor::toOStream(std::ostream& os) const {
    os << "PassiveDurabilityMonitor[" << this << "]"
       << " high_prepared_seqno:" << getHighPreparedSeqno();
}

DurabilityMonitor::Container::iterator
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

        const bool isDiskSnapshot = snapshotEnd.type == CheckpointType::Disk;

        using namespace cb::durability;

        Level maxLevelCanAdvanceOver{};

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

    const auto fence = std::min(int64_t(highCompletedSeqno.lastWriteSeqno),
                                int64_t(highPreparedSeqno.lastWriteSeqno));

    auto it = trackedWrites.begin();
    while (it != trackedWrites.end() && it->getBySeqno() <= fence) {
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

template <class exception>
[[noreturn]] void PassiveDurabilityMonitor::throwException(
        const std::string& thrower, const std::string& error) const {
    throw exception(thrower + error + vb.getId().to_string());
}
