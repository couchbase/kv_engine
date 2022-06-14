/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dead_durability_monitor.h"

#include "bucket_logger.h"
#include "durability_monitor_impl.h"

#include <statistics/cbstat_collector.h>

#include <spdlog/fmt/fmt.h>

DeadDurabilityMonitor::DeadDurabilityMonitor(VBucket& vb,
                                             DurabilityMonitor&& oldDM)
    : vb(vb),
      highPreparedSeqno(oldDM.getHighPreparedSeqno()),
      highCompletedSeqno(oldDM.getHighCompletedSeqno()),
      trackedWrites(oldDM.getTrackedWrites()) {
}

int64_t DeadDurabilityMonitor::getHighestTrackedSeqno() const {
    if (trackedWrites.size() == 0) {
        return 0;
    }

    return trackedWrites.back().getBySeqno();
}

std::list<DurabilityMonitor::SyncWrite>
DeadDurabilityMonitor::getTrackedWrites() const {
    return trackedWrites;
}

void DeadDurabilityMonitor::addStats(const AddStatFn& addStat,
                                     const CookieIface* cookie) const {
    try {
        const auto vbid = vb.getId().get();

        add_casted_stat(fmt::format("vb_{}:state", vbid),
                        VBucket::toString(vb.getState()),
                        addStat,
                        cookie);

        add_casted_stat(fmt::format("vb_{}:num_tracked", vbid),
                        trackedWrites.size(),
                        addStat,
                        cookie);

        add_casted_stat(fmt::format("vb_{}:last_tracked_seqno", vbid),
                        getHighestTrackedSeqno(),
                        addStat,
                        cookie);

        add_casted_stat(fmt::format("vb_{}:high_prepared_seqno", vbid),
                        highPreparedSeqno,
                        addStat,
                        cookie);

        add_casted_stat(fmt::format("vb_{}:high_completed_seqno", vbid),
                        highCompletedSeqno,
                        addStat,
                        cookie);
    } catch (const std::exception& e) {
        EP_LOG_WARN(
                "({}) DeadDurabilityMonitor::State:::addStats: error building "
                "stats: {}",
                vb.getId(),
                e.what());
    }
}

void DeadDurabilityMonitor::dump() const {
    toOStream(std::cerr);
}

void DeadDurabilityMonitor::toOStream(std::ostream& os) const {
    os << "DeadDurabilityMonitor["
       << "#trackedWrites:" << trackedWrites.size()
       << " highPreparedSeqno:" << highPreparedSeqno
       << " highCompletedSeqno:" << highCompletedSeqno
       << " lastTrackedSeqno:" << getHighestTrackedSeqno() << " trackedWrites:["
       << "\n";
    for (const auto& w : trackedWrites) {
        os << "    " << w << "\n";
    }

    os << "\n";
    os << "]";
}