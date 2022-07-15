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
#include "ep_engine.h"
#include "stats.h"

ReplicationThrottleEP::ReplicationThrottleEP(
        const EventuallyPersistentEngine& engine)
    : engine(engine) {
}

bool ReplicationThrottleEP::hasSomeMemory() const {
    const auto& stats = engine.getEpStats();
    const auto memoryUsed = stats.getEstimatedTotalMemoryUsed();
    const auto maxSize = stats.getMaxDataSize();
    return memoryUsed <= (maxSize * stats.replicationThrottleThreshold);
}

ReplicationThrottleEP::Status ReplicationThrottleEP::getStatus() const {
    return hasSomeMemory() ? Status::Process : Status::Pause;
}

ReplicationThrottleEphe::ReplicationThrottleEphe(
        const EventuallyPersistentEngine& engine)
    : ReplicationThrottleEP(engine) {
}

ReplicationThrottleEP::Status ReplicationThrottleEphe::getStatus() const {
    auto status = ReplicationThrottleEP::getStatus();
    if (status == Status::Pause) {
        if (engine.getConfiguration().getEphemeralFullPolicy() ==
            "fail_new_data") {
            return Status::Disconnect;
        }
    }
    return status;
}

bool ReplicationThrottleEphe::doDisconnectOnNoMem() const {
    if (engine.getConfiguration().getEphemeralFullPolicy() == "fail_new_data") {
        return true;
    }
    return false;
}
