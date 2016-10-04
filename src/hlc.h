/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#pragma once

#include <chrono>

#include "atomic.h"
#include "statwriter.h"

/*
 * HLC manages a hybrid logical clock for 'time' stamping events.
 * The class only implements the send logic of the HLC algorithm.
 *   - http://www.cse.buffalo.edu/tech-reports/2014-04.pdf
 *
 * The class allows multiple threads to concurrently call
 *  - nextHLC
 *  - setMaxCas
 *  - setMaxHLCAndTrackDrift
 *
 * The paired "drift" counter, which is used to monitor drift over time
 * isn't atomic in that the total and update parts are not a consistent
 * snapshot.
 */
class HLC {
public:
    struct DriftStats {
        uint64_t total;
        uint64_t updates;
    };

    struct DriftExceptions {
        uint32_t ahead;
        uint32_t behind;
    };

    /*
     * @param initHLC a HLC value to start from
     * @param aheadThresholdAhead threshold a peer can be ahead before we
     *        increment driftAheadExceeded
     * @param behindThresholdhreshold a peer can be ahead before we
     *        increment driftBehindExceeded
     */
    HLC(uint64_t initHLC, uint64_t aheadThreshold, uint64_t behindThreshold)
        : maxHLC(initHLC),
          cummulativeDrift(0),
          cummulativeDriftIncrements(0),
          logicalClockTicks(0),
          driftAheadExceeded(0),
          driftBehindExceeded(0),
          driftAheadThreshold(aheadThreshold),
          driftBehindThreshold(behindThreshold) {}

    uint64_t nextHLC() {
        // Create a monotonic timestamp using part of the HLC algorithm by.
        // a) Reading system time
        // b) dropping 16-bits (done by nowHLC)
        // c) comparing it with the last known time (max_cas)
        // d) returning either now or max_cas + 1
        uint64_t timeNow = getMasked48(getTime());
        uint64_t l = maxHLC.load();

        if (timeNow > l) {
            atomic_setIfBigger(maxHLC, timeNow);
            return timeNow;
        }
        logicalClockTicks++;
        atomic_setIfBigger(maxHLC, l + 1);
        return l + 1;
    }

    void setMaxHLCAndTrackDrift(uint64_t hlc) {
        auto timeNow = getMasked48(getTime());

        // Track the +/- difference between our time and their time
        int64_t difference = getMasked48(hlc) - timeNow;

        // Accumulate the absolute drift
        cummulativeDrift += std::abs(difference);
        cummulativeDriftIncrements++;

        // If the difference is greater, count peer ahead exeception
        // If the difference is less than our -ve threshold.. count that
        if (difference > int64_t(driftAheadThreshold)) {
            driftAheadExceeded++;
        } else if(difference < (0 - int64_t(driftBehindThreshold))) {
            driftBehindExceeded++;
        }

        setMaxHLC(hlc);
    }

    void setMaxHLC(uint64_t hlc) {
        atomic_setIfBigger(maxHLC, hlc);
    }

    uint64_t getMaxHLC() const {
        return maxHLC;
    }

    DriftStats getDriftStats() const {
        // Deliberately not locking to read this pair
        return {cummulativeDrift, cummulativeDriftIncrements};
    }

    DriftExceptions getDriftExceptionCounters() const {
        // Deliberately not locking to read this pair
        return {driftAheadExceeded, driftBehindExceeded};
    }

    void setDriftAheadThreshold(uint64_t threshold) {
        driftAheadThreshold = threshold;
    }

    void setDriftBehindThreshold(uint64_t threshold) {
        driftBehindThreshold = threshold;
    }

    void addStats(const std::string& prefix, ADD_STAT add_stat, const void *c) const {
        add_prefixed_stat(prefix.data(), "max_cas", getMaxHLC(), add_stat, c);
        add_prefixed_stat(prefix.data(), "total_abs_drift", cummulativeDrift.load(), add_stat, c);
        add_prefixed_stat(prefix.data(), "total_abs_drift_count", cummulativeDriftIncrements.load(), add_stat, c);
        add_prefixed_stat(prefix.data(), "drift_ahead_threshold_exceeded", driftAheadExceeded.load(), add_stat, c);
        add_prefixed_stat(prefix.data(), "drift_ahead_threshold", driftAheadThreshold.load(), add_stat, c);
        add_prefixed_stat(prefix.data(), "drift_behind_threshold_exceeded", driftBehindExceeded.load(), add_stat, c);
        add_prefixed_stat(prefix.data(), "drift_behind_threshold", driftBehindThreshold.load(), add_stat, c);
        add_prefixed_stat(prefix.data(), "logical_clock_ticks", logicalClockTicks.load(), add_stat, c);
    }

private:
    /*
     * Returns 48-bit of t (bottom 16-bit zero)
     */
    static int64_t getMasked48(int64_t t) {
        return t & ~((1<<16)-1);
    }

    static int64_t getTime() {
        auto now = std::chrono::system_clock::now();
        return std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    }

    /*
     * maxHLC tracks the current highest time, either our own or a peer who
     * has a larger clock value. nextHLC and setMax* methods change this and can
     * be called from different threads
     */
    std::atomic<uint64_t> maxHLC;

    /*
     * The following are used for stats/drift tracking.
     * many threads could be setting cas so they need to be atomically
     * updated for consisent totals.
     */
    std::atomic<uint64_t> cummulativeDrift;
    std::atomic<uint64_t> cummulativeDriftIncrements;
    std::atomic<uint64_t> logicalClockTicks;
    std::atomic<uint32_t> driftAheadExceeded;
    std::atomic<uint32_t> driftBehindExceeded;
    std::atomic<uint64_t> driftAheadThreshold;
    std::atomic<uint64_t> driftBehindThreshold;
};