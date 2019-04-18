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

void PassiveDurabilityMonitor::addSyncWrite(queued_item item) {
    auto durReq = item->getDurabilityReqs();

    if (durReq.getLevel() == cb::durability::Level::None) {
        throw std::invalid_argument(
                "PassiveDurabilityMonitor::addSyncWrite: Level::None");
    }

    int64_t prevHps{0};
    int64_t hps{0};
    {
        auto s = state.wlock();
        s->trackedWrites.emplace_back(
                nullptr /*cookie*/, std::move(item), nullptr /*firstChain*/);

        // Maybe the new tracked Prepare is already satisfied and could be
        // ack'ed back to the Active.
        prevHps = s->highPreparedSeqno.lastWriteSeqno;
        s->updateHighPreparedSeqno();
        hps = s->highPreparedSeqno.lastWriteSeqno;
    }

    // HPS may have not be changed, which would result in re-acking the same
    // HPS multiple times. Not wrong as HPS is weakly-monotonic at Active, but
    // we want to avoid sending unnecessary messages.
    if (hps != prevHps) {
        Expects(hps > prevHps);
        vb.sendSeqnoAck(hps);
    }
}

size_t PassiveDurabilityMonitor::getNumTracked() const {
    return state.rlock()->trackedWrites.size();
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
    // Note: This function is supposed to be called only when the Flusher has
    //     persisted a Prepare. So, HPS must have changed at this point.
    Expects(hps > prevHps);

    vb.sendSeqnoAck(hps);
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
    // First, blindly move HPS up to the given persistedSeqno. Note that
    // here we don't need to check any Durability Level: persistence makes
    // locally-satisfied all the pending Prepares up to persistedSeqno.
    while ((next = getIteratorNext(highPreparedSeqno.it)) !=
                   trackedWrites.end() &&
           static_cast<uint64_t>(next->getBySeqno()) <=
                   pdm.vb.getPersistenceSeqno()) {
        updateHPS(next);
    }

    // Then, move the HPS to the last Prepare with Level != PersistToMajority.
    // I.e., all the Majority and MajorityAndPersistToMaster Prepares that were
    // blocked by non-satisfied PersistToMajority Prepares are implicitly
    // satisfied now. The first non-satisfied Prepare is the first
    // Level:PersistToMajority not covered by persistedSeqno.
    while ((next = getIteratorNext(highPreparedSeqno.it)) !=
           trackedWrites.end()) {
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
