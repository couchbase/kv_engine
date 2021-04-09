/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "replicationthrottle.h"
#include "configuration.h"
#include "stats.h"

ReplicationThrottleEP::ReplicationThrottleEP(const Configuration& config,
                                             EPStats& s)
    : queueCap(config.getReplicationThrottleQueueCap()),
      capPercent(config.getReplicationThrottleCapPcnt()),
      stats(s) {
}

bool ReplicationThrottleEP::persistenceQueueSmallEnough() const {
    size_t queueSize = stats.diskQueueSize.load();
    if (stats.replicationThrottleWriteQueueCap == -1) {
        return true;
    }
    return queueSize < static_cast<size_t>(stats.replicationThrottleWriteQueueCap);
}

bool ReplicationThrottleEP::hasSomeMemory() const {
    auto memoryUsed =
            static_cast<double>(stats.getEstimatedTotalMemoryUsed());
    auto maxSize = static_cast<double>(stats.getMaxDataSize());

    return memoryUsed <= (maxSize * stats.replicationThrottleThreshold);
}

ReplicationThrottleEP::Status ReplicationThrottleEP::getStatus() const {
    return (persistenceQueueSmallEnough() && hasSomeMemory()) ? Status::Process
                                                              : Status::Pause;
}

void ReplicationThrottleEP::adjustWriteQueueCap(size_t totalItems) {
    if (queueCap == -1) {
        stats.replicationThrottleWriteQueueCap.store(-1);
        return;
    }
    auto qcap = static_cast<size_t>(queueCap);
    size_t throttleCap = 0;
    if (capPercent > 0) {
        throttleCap = (static_cast<double>(capPercent) / 100.0) * totalItems;
    }
    stats.replicationThrottleWriteQueueCap.store(throttleCap > qcap ? throttleCap :
                                         qcap);
}

ReplicationThrottleEphe::ReplicationThrottleEphe(const Configuration& config,
                                                 EPStats& s)
    : ReplicationThrottleEP(config, s), config(config) {
}

ReplicationThrottleEP::Status ReplicationThrottleEphe::getStatus() const {
    auto status = ReplicationThrottleEP::getStatus();
    if (status == Status::Pause) {
        if (config.getEphemeralFullPolicy() == "fail_new_data") {
            return Status::Disconnect;
        }
    }
    return status;
}

bool ReplicationThrottleEphe::doDisconnectOnNoMem() const {
    if (config.getEphemeralFullPolicy() == "fail_new_data") {
        return true;
    }
    return false;
}
