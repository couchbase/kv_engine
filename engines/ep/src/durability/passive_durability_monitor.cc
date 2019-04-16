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

PassiveDurabilityMonitor::PassiveDurabilityMonitor(VBucket& vb) : vb(vb) {
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
    // @todo-durability: return a correct value for this.
    return 0;
}

void PassiveDurabilityMonitor::addSyncWrite(queued_item item) {
    auto durReq = item->getDurabilityReqs();

    if (durReq.getLevel() == cb::durability::Level::None) {
        throw std::invalid_argument(
                "PassiveDurabilityMonitor::addSyncWrite: Level::None");
    }

    state.wlock()->trackedWrites.emplace_back(
            nullptr /*cookie*/, std::move(item), nullptr /*firstChain*/);

    /*
     * @todo: Missing step - we may be able to increase the high_prepared_seqno
     * and ack back back to the Active.
     */
}

size_t PassiveDurabilityMonitor::getNumTracked() const {
    return state.rlock()->trackedWrites.size();
}

void PassiveDurabilityMonitor::toOStream(std::ostream& os) const {
    os << "PassiveDurabilityMonitor[" << this << "]"
       << " high_prepared_seqno:" << getHighPreparedSeqno();
}
