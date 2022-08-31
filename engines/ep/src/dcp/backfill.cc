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

#include "dcp/backfill.h"
#include "bucket_logger.h"
#include "dcp/active_stream.h"

#include <folly/ScopeGuard.h>
#include <phosphor/phosphor.h>

DCPBackfill::DCPBackfill(Vbid vbid) : vbid(vbid) {
}

backfill_status_t DCPBackfill::run() {
    runStart = std::chrono::steady_clock::now();
    auto lockedState = state.wlock();
    auto runtimeGuard = folly::makeGuard([this] {
        runtime += (std::chrono::steady_clock::now() - runStart);
    });

    TRACE_EVENT2("dcp/backfill",
                 "DCPBackfill::run",
                 "vbid",
                 getVBucketId().get(),
                 "state",
                 uint8_t(*lockedState));

    backfill_status_t status = backfill_finished;
    switch (*lockedState) {
    case State::Create:
        status = create();
        Expects(status == backfill_success || status == backfill_snooze ||
                status == backfill_finished);
        if (status == backfill_success) {
            transitionState(*lockedState, State::Scan);
            status = scan();
        }
        break;
    case State::Scan:
        status = scan();
        Expects(status == backfill_success || status == backfill_finished);
        break;
    case State::Done:
        // As soon as we return finished, we change to State::Done, finished
        // signals the caller should not call us again so throw if that occurs
        throw std::logic_error(fmt::format("{}: {} called in State::Done",
                                           __PRETTY_FUNCTION__,
                                           getVBucketId()));
    }

    if (status == backfill_finished) {
        transitionState(*lockedState, State::Done);
    }

    return status;
}

void DCPBackfill::cancel() {
    if (*state.rlock() != State::Done) {
        EP_LOG_WARN(
                "DCPBackfill::cancel ({}) cancelled before reaching "
                "State::Done",
                getVBucketId());
    }
}

std::ostream& operator<<(std::ostream& os, DCPBackfill::State state) {
    switch (state) {
    case DCPBackfill::State::Create:
        return os << "State::Create";
    case DCPBackfill::State::Scan:
        return os << "State::Scan";
    case DCPBackfill::State::Done:
        return os << "State::Done";
    }
    throw std::logic_error(fmt::format("{}: Invalid state:{}",
                                       __PRETTY_FUNCTION__,
                                       std::to_string(int(state))));
    return os;
}

void DCPBackfill::transitionState(State& currentState, State newState) {
    bool validTransition = false;
    switch (newState) {
    case State::Create:
        // No valid transition to 'create'
        break;
    case State::Scan:
        if (currentState == State::Create) {
            validTransition = true;
        }
        break;
    case State::Done:
        if (currentState == State::Create || currentState == State::Scan) {
            validTransition = true;
        }
        break;
    }

    if (!validTransition) {
        throw std::invalid_argument(
                fmt::format("{}: newState:{} is not valid "
                            "for currentState:{}",
                            __PRETTY_FUNCTION__,
                            newState,
                            currentState));
    }

    currentState = newState;
}

bool KVStoreScanTracker::canCreateBackfill() {
    return scans.withLock([](auto& scans) {
        if (scans.running < scans.maxRunning) {
            ++scans.running;
            return true;
        }
        return false;
    });
}

void KVStoreScanTracker::decrNumRunningBackfills() {
    if (!scans.withLock([](auto& backfills) {
            if (backfills.running > 0) {
                --backfills.running;
                return true;
            }
            return false;
        })) {
        EP_LOG_WARN_RAW(
                "KVStoreScanTracker::decrNumRunningBackfills backfills.running "
                "is zero");
    }
}

void KVStoreScanTracker::updateMaxRunningScans(size_t maxDataSize) {
    auto newMaxRunningScans = getMaxRunningScansForQuota(maxDataSize);
    scans.lock()->maxRunning = newMaxRunningScans;
    EP_LOG_DEBUG("KVStoreScanTracker::updateMaxRunningScans maxRunning:{}",
                 newMaxRunningScans);
}

/* Db file memory */
const uint32_t dbFileMem = 10 * 1024;
/* Max num of scans we want to have irrespective of memory */
const uint16_t numScansThreshold = 4096;
/* Max percentage of memory we want scans to occupy */
const uint8_t numScansMemThreshold = 1;

uint16_t KVStoreScanTracker::getMaxRunningScansForQuota(size_t maxDataSize) {
    double numScansMemThresholdPercent =
            static_cast<double>(numScansMemThreshold) / 100;
    size_t max = maxDataSize * numScansMemThresholdPercent / dbFileMem;

    /* We must have at least one scan available */
    size_t newMaxScans =
            std::max(static_cast<size_t>(1),
                     std::min(max, static_cast<size_t>(numScansThreshold)));
    return gsl::narrow_cast<uint16_t>(newMaxScans);
}
