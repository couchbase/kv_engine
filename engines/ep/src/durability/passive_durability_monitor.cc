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
#include "statwriter.h"
#include "stored-value.h"
#include "vbucket.h"

#include <gsl.h>
#include <unordered_map>

PassiveDurabilityMonitor::PassiveDurabilityMonitor(VBucket& vb)
    : vb(vb), state(*this) {
    // By design, instances of Container::Position can never be invalid
    auto s = state.wlock();
    s->highPreparedSeqno = Position(s->trackedWrites.end());
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
    // @todo: return proper value
    return 0;
}

void PassiveDurabilityMonitor::addSyncWrite(queued_item item) {
    auto durReq = item->getDurabilityReqs();

    if (durReq.getLevel() == cb::durability::Level::None) {
        throw std::invalid_argument(
                "PassiveDurabilityMonitor::addSyncWrite: Level::None");
    }
    if (durReq.getTimeout().isDefault()) {
        throw std::invalid_argument(
                "PassiveDurabilityMonitor::addSyncWrite: timeout is default "
                "(explicit value should have been specified by Active node)");
    }

    // Need to specify defaultTimeout for SyncWrite ctor, but we've already
    // checked just above the requirements have a non-default value,
    // just pass dummy value here.
    std::chrono::milliseconds dummy{};
    state.wlock()->trackedWrites.emplace_back(nullptr /*cookie*/,
                                              std::move(item),
                                              dummy,
                                              nullptr /*firstChain*/,
                                              nullptr /*secondChain*/);
}

size_t PassiveDurabilityMonitor::getNumTracked() const {
    return state.rlock()->trackedWrites.size();
}

void PassiveDurabilityMonitor::notifySnapshotEndReceived(uint64_t snapEnd) {
    int64_t prevHps{0};
    int64_t hps{0};
    {
        auto s = state.wlock();
        s->snapshotEnd = snapEnd;
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
    }
    folly::assume_unreachable();
}

void PassiveDurabilityMonitor::completeSyncWrite(const StoredDocKey& key,
                                                 Resolution res) {
    auto s = state.wlock();

    if (s->trackedWrites.empty()) {
        throw std::logic_error(
                "PassiveDurabilityMonitor::resolvePrepare: No tracked, but "
                "received " +
                to_string(res) + " for key " + key.to_string());
    }

    // Sanity check for In-Order Commit
    const auto& front = s->trackedWrites.front();
    if (front.getKey() != key) {
        std::stringstream ss;
        ss << "Pending resolution for '" << front
           << "', but received unexpected " + to_string(res) + " for key "
           << key;
        throw std::logic_error("PassiveDurabilityMonitor::resolvePrepare: " +
                               ss.str());
    }

    s->removeFront();
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

    if (trackedWrites.empty()) {
        return;
    }

    const auto updateHPS = [this](Container::iterator next) -> void {
        // Note: Update last-write-seqno first to enforce monotonicity and
        //     avoid any state-change if monotonicity checks fail
        highPreparedSeqno.lastWriteSeqno = next->getBySeqno();
        highPreparedSeqno.it = next;
    };

    Container::iterator next;

    // First, blindly move HPS up to the last persisted snapshot-end.
    // That ensures that the durability-fence moves only when the complete
    // snapshot is persisted.
    // Note that here we don't need to check any Durability Level: persistence
    // of a complete snapshot makes locally-satisfied all the pending Prepares
    // in that snapshot.
    if (pdm.vb.getPersistenceSeqno() >= snapshotEnd) {
        while ((next = getIteratorNext(highPreparedSeqno.it)) !=
                       trackedWrites.end() &&
               static_cast<uint64_t>(next->getBySeqno()) <= snapshotEnd) {
            updateHPS(next);
        }
    }

    // Then, move the HPS to the last Prepare with Level != PersistToMajority.
    //
    // I.e., all the Majority and MajorityAndPersistToMaster Prepares that were
    // blocked by non-locally-satisfied PersistToMajority Prepares
    // (durability-fence) maybe implicitly satisfied now (as the previous step
    // may have moved the durability-fence).
    //
    // So, here the HPS moves up to the first non-locally-satisfied
    // PersistToMajority Prepare.
    // Again, the HPS moves always at snapshot boundaries (ie, in this case
    // within the latest complete snapshot *received*).
    while ((next = getIteratorNext(highPreparedSeqno.it)) !=
                   trackedWrites.end() &&
           static_cast<uint64_t>(next->getBySeqno()) <= snapshotEnd) {
        const auto level = next->getDurabilityReqs().getLevel();
        Expects(level != cb::durability::Level::None);
        // Note: We are in the PassiveDM. The first Level::PersistToMajority
        // SyncWrite is our durability-fence.
        if (level == cb::durability::Level::PersistToMajority) {
            break;
        }

        updateHPS(next);
    }
}

void PassiveDurabilityMonitor::State::removeFront() {
    // In PassiveDM we have just one iterator pointing to items in the tracked
    // Container: the HPS. Ensure that the iterator is never invalid by pointing
    // it to Container::end if the underlying item is removed.
    auto begin = trackedWrites.begin();
    if (begin == highPreparedSeqno.it) {
        highPreparedSeqno.it = trackedWrites.end();
    }

    trackedWrites.erase(begin);
}
