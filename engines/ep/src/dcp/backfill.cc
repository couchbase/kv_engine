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
#include <platform/monotonic.h>

static AtomicMonotonic<uint64_t> backfillUID{0};

DCPBackfill::DCPBackfill(Vbid vbid) : vbid(vbid), uid(++backfillUID) {
}

backfill_status_t DCPBackfill::run() {
    runStart = cb::time::steady_clock::now();
    auto lockedState = state.wlock();
    auto runtimeGuard = folly::makeGuard(
            [this] { runtime += (cb::time::steady_clock::now() - runStart); });

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
            // Continue with the scan, if allowed to.
            if (createMode == DCPBackfillCreateMode::CreateAndScan) {
                status = scan(currentState);
            }
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
        EP_LOG_WARN_CTX(
                "DCPBackfill::cancel cancelled before reaching State::Done",
                {"vb", getVBucketId()});
    }
}

std::string format_as(DCPBackfill::State state) {
    switch (state) {
    case DCPBackfill::State::Create:
        return "State::Create";
    case DCPBackfill::State::Scan:
        return "State::Scan";
    case DCPBackfill::State::ScanHistory:
        return "State::ScanHistory";
    case DCPBackfill::State::Done:
        return "State::Done";
    }
    throw std::logic_error(fmt::format("{}: Invalid state:{}",
                                       __PRETTY_FUNCTION__,
                                       std::to_string(int(state))));
}

std::ostream& operator<<(std::ostream& os, DCPBackfill::State state) {
    return os << format_as(state);
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
    // connection, and we use synchronous IO, in _theory_ we only need one
    // backfill in-progress per DCP connection to achieve the maximum backfill
    // throughput from disk.
    // This holds up in practice for a simple no-op DCP client like dcpdrain
    // (it just reads the DCP messages off the wire and discards them), where
    // we see the same throughput (+/- 2%) for a range of concurrent backfill
    // values (1, 2, 4, 8, 16).
    // If we reduce concurrent backfills to one, then this significantly
    // reduces the number of concurrently in-progress backfills, down from
    // (up to) 1024 per connection to 1 per connection, and hence significantly
    // reduces the likelihood that one set of DCP connections (asking for many
    // vBuckets at once) could starve another DCP connection also asking for
    // backfills (e.g. replication) - so it would be highly desirable to be
    // able to limit to one.
    // Unfortunately, for real DCP clients like GSI & XDCR, we see a
    // significant degradation in throughput (XDCR:20%, GSI:4x)  when limiting
    // concurrent backfills to one. This is due to the fact that XDCR & GSI
    // both extract concurrency from the DCP connection at the vBucket level
    // - i.e. they define some number of queues / worker threads and assign
    // incoming DCP messages to a given queue/worker based on the vbid. As such,
    // if KV-Engine send a single vBuckets' worth of DCP messages at once before
    // then sending the next entire vBucket, then XDCR/GSI lose their
    // concurrency in processing the DCP messages and their throughput
    // (transmitting to remote Bucket / building indexes) suffers greatly.
    // As such, we cannot be as aggressive as reducing to one, but we do still
    // constrain to a finite number (see configuration.json) to reduce the
    // impact different DCP Conncections have on each other.
    //
    // Note: If / when we can actually backfill more than one vBucket at the
    // same time on a single connection then there _is_ a throughput argument
    // to increasing this limit (e.g. either multiple BackfillManager tasks
    // each performing sync IO, or async IO support for BackfillManager).
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

bool KVStoreScanTracker::canCreateAndReserveRangeScan() {
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
    EP_LOG_DEBUG_CTX("KVStoreScanTracker::updateMaxRunningDcpBackfills",
                     {"max_backfills", maxBackfills});
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
    EP_LOG_DEBUG_CTX("KVStoreScanTracker::setMaxRunningScans",
                     {"new_max_running_backfills", newMaxRunningBackfills},
                     {"new_max_running_range_scans", newMaxRunningRangeScans},
                     {"max_backfills", maxBackfills});
}

/* Db file memory */
const uint32_t dbFileMem = 10_KiB;
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
