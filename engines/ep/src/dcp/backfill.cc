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

    auto& currentState = *lockedState;
    TRACE_EVENT2("dcp/backfill",
                 "DCPBackfill::run",
                 "vbid",
                 getVBucketId().get(),
                 "state",
                 uint8_t(currentState));

    backfill_status_t status = backfill_finished;

    switch (*lockedState) {
    case State::Create:
        status = create();
        Expects(status == backfill_success || status == backfill_snooze ||
                status == backfill_finished);
        if (status == backfill_success) {
            currentState = validateTransition(currentState,
                                              getNextScanState(currentState));
            status = scan(currentState);
        } else if (status == backfill_finished) {
            // Create->Done
            currentState = validateTransition(currentState, State::Done);
        }
        break;
    case State::Scan:
        status = scan();
        Expects(status == backfill_success || status == backfill_finished);
        break;
    case State::ScanHistory:
        status = scanHistory();
        Expects(status == backfill_success || status == backfill_finished);
        break;
    case State::Done:
        // As soon as we return finished, we change to State::Done, finished
        // signals the caller should not call us again so throw if that occurs
        throw std::logic_error(fmt::format("{}: {} called in State::Done",
                                           __PRETTY_FUNCTION__,
                                           getVBucketId()));
    }

    if (status == backfill_finished && currentState != State::Done) {
        currentState = validateTransition(currentState,
                                          getNextScanState(*lockedState));
        if (currentState != State::Done) {
            // This occurs on transition from Scan->ScanHistory. Scan is
            // finished but we need more callbacks so return success and the
            // next phase of scanning will be invoked.
            status = backfill_success;
        }
    }

    return status;
}

backfill_status_t DCPBackfill::scan(DCPBackfill::State state) {
    Expects(state == DCPBackfill::State::Scan ||
            state == DCPBackfill::State::ScanHistory);
    if (state == DCPBackfill::State::Scan) {
        return scan();
    }
    return scanHistory();
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
    case DCPBackfill::State::ScanHistory:
        return os << "State::ScanHistory";
    case DCPBackfill::State::Done:
        return os << "State::Done";
    }
    throw std::logic_error(fmt::format("{}: Invalid state:{}",
                                       __PRETTY_FUNCTION__,
                                       std::to_string(int(state))));
    return os;
}

std::ostream& operator<<(std::ostream& os, backfill_status_t status) {
    switch (status) {
    case backfill_success:
        return os << "backfill_success";
    case backfill_finished:
        return os << "backfill_finished";
    case backfill_snooze:
        return os << "backfill_snooze";
    }
    throw std::logic_error(fmt::format("{}: Invalid status:{}",
                                       __PRETTY_FUNCTION__,
                                       std::to_string(int(status))));
}

DCPBackfill::State DCPBackfill::validateTransition(State currentState,
                                                   State newState) {
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
    case State::ScanHistory: {
        if (currentState == State::Create || currentState == State::Scan) {
            validTransition = true;
        }
        break;
    }
    case State::Done:
        if (currentState == State::Create || currentState == State::Scan ||
            currentState == State::ScanHistory) {
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

    return newState;
}

bool KVStoreScanTracker::canCreateBackfill(size_t numInProgress) {
    // MB-57304: Given there's only one BackfillManager task per DCP
    // connection, and we use synchronous IO, we limit the number of backfills
    // in-progress per DCP connection to
    // dcp_backfill_in_progress_per_connection_limit - default "1".
    // This significantly reduces the number of concurrently in-progress
    // backfills, down from (up to) 1024 per connection to 1 per connection,
    // and hence significantly reduces the likelihood that one set of DCP
    // connections (asking for many vBuckets at once) could starve another DCP
    // connection also asking for backfills (e.g. replication).
    //
    // If / when we can actually backfill more than one vBucket at the same
    // time on a single connection then we can increase this limit (e.g. either
    // multiple BackfillManager tasks each performing sync IO, or async IO
    // support for BackfillManager).
    return scans.withLock([&numInProgress](auto& scans) {
        if (numInProgress >= scans.maxNumBackfillsPerConnection) {
            return false;
        }

        // For backfills compare total against the absolute max, maxRunning
        if (scans.getTotalRunning() < scans.maxRunning) {
            ++scans.runningBackfills;
            return true;
        }
        return false;
    });
}

bool KVStoreScanTracker::canCreateRangeScan() {
    return scans.withLock([](auto& scans) {
        // For RangeScan compare total against maxRunningRangeScans
        if (scans.getTotalRunning() < scans.maxRunningRangeScans) {
            ++scans.runningRangeScans;
            return true;
        }
        return false;
    });
}

void KVStoreScanTracker::decrNumRunningBackfills() {
    if (!scans.withLock([](auto& backfills) {
            if (backfills.runningBackfills > 0) {
                --backfills.runningBackfills;
                return true;
            }
            return false;
        })) {
        EP_LOG_WARN_RAW(
                "KVStoreScanTracker::decrNumRunningBackfills runningBackfills "
                "already zero");
    }
}

void KVStoreScanTracker::decrNumRunningRangeScans() {
    if (!scans.withLock([](auto& backfills) {
            if (backfills.runningRangeScans > 0) {
                --backfills.runningRangeScans;
                return true;
            }
            return false;
        })) {
        EP_LOG_WARN_RAW(
                "KVStoreScanTracker::decrNumRunningRangeScans "
                "backfills.runningRangeScans already zero");
    }
}

void KVStoreScanTracker::updateMaxRunningDcpBackfills(size_t maxBackfills) {
    scans.withLock([&maxBackfills](auto& backfills) {
        backfills.maxNumBackfillsPerConnection = maxBackfills;
    });
    EP_LOG_DEBUG("KVStoreScanTracker::updateMaxRunningDcpBackfills {}",
                 maxBackfills);
}

void KVStoreScanTracker::updateMaxRunningScans(size_t maxDataSize,
                                               float rangeScanRatio,
                                               size_t maxBackfills) {
    auto newMaxRunningScans =
            getMaxRunningScansForQuota(maxDataSize, rangeScanRatio);
    setMaxRunningScans(
            newMaxRunningScans.first, newMaxRunningScans.second, maxBackfills);
}

void KVStoreScanTracker::setMaxRunningScans(uint16_t newMaxRunningBackfills,
                                            uint16_t newMaxRunningRangeScans,
                                            size_t maxBackfills) {
    scans.withLock([&newMaxRunningBackfills,
                    &newMaxRunningRangeScans,
                    &maxBackfills](auto& backfills) {
        backfills.maxRunning = newMaxRunningBackfills;
        backfills.maxRunningRangeScans = newMaxRunningRangeScans;
        backfills.maxNumBackfillsPerConnection = maxBackfills;
    });
    EP_LOG_DEBUG(
            "KVStoreScanTracker::setMaxRunningScans scans:{} rangeScans:{} "
            "maxBackfills:{}",
            newMaxRunningBackfills,
            newMaxRunningRangeScans,
            maxBackfills);
}

/* Db file memory */
const uint32_t dbFileMem = 10 * 1024;
/* Max num of scans we want to have irrespective of memory */
const uint16_t numScansThreshold = 4096;
/* Max percentage of memory we want scans to occupy */
const uint8_t numScansMemThreshold = 1;

std::pair<uint16_t, uint16_t> KVStoreScanTracker::getMaxRunningScansForQuota(
        size_t maxDataSize, float rangeScanRatio) {
    Expects(rangeScanRatio >= 0.0 && rangeScanRatio <= 1.0);
    double numScansMemThresholdPercent =
            static_cast<double>(numScansMemThreshold) / 100;
    size_t max = maxDataSize * numScansMemThresholdPercent / dbFileMem;

    // Need at least 2 scans available, 1 range and 1 backfill
    size_t newMaxScans =
            std::max(static_cast<size_t>(2),
                     std::min(max, static_cast<size_t>(numScansThreshold)));

    // Calculate how many range scans can exist. RangeScans themselves must not
    // consumer all file handles, so are capped, but then further capped so that
    // there is some room for backfills.
    // The final max is either 1 or some % of newMaxScans
    size_t rangeScans = newMaxScans * rangeScanRatio;
    size_t newMaxRangeScans = std::max(static_cast<size_t>(1), rangeScans);

    Expects(newMaxScans > newMaxRangeScans);
    return std::make_pair(gsl::narrow_cast<uint16_t>(newMaxScans),
                          gsl::narrow_cast<uint16_t>(newMaxRangeScans));
}
