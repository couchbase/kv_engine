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

#include <platform/checked_snprintf.h>

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
     *        increment driftAheadExceeded. Expressed in us.
     * @param behindThresholdhreshold a peer can be ahead before we
     *        increment driftBehindExceeded. Expressed in us.
     */
    HLC(uint64_t initHLC,
        std::chrono::microseconds aheadThreshold,
        std::chrono::microseconds behindThreshold)
        : maxHLC(initHLC),
          cummulativeDrift(0),
          cummulativeDriftIncrements(0),
          logicalClockTicks(0),
          driftAheadExceeded(0),
          driftBehindExceeded(0) {
        setDriftAheadThreshold(aheadThreshold);
        setDriftBehindThreshold(behindThreshold);
    }

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

        // Accumulate the absolute drift in microseconds not nanoseconds.
        // E.g. 5s drift then has ~3.6 trillion updates before overflow vs 3.6
        // million if we tracked in nanoseconds.
        const auto ns = std::chrono::nanoseconds(std::abs(difference));
        cummulativeDrift +=
            std::chrono::duration_cast<std::chrono::microseconds>(ns).count();
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

    void forceMaxHLC(uint64_t hlc) {
        maxHLC = hlc;
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

    /*
     * Set the drift threshold for ahead exception counting
     * - externally we work in microseconds
     * - internally we work in nanoseconds
     */
    void setDriftAheadThreshold(std::chrono::microseconds threshold) {
        driftAheadThreshold =
            std::chrono::duration_cast<std::chrono::nanoseconds>(threshold).count();
    }

    /*
     * Set the drift threshold for behind exception counting
     * - externally we work in (u) microseconds
     * - internally we work in nanoseconds
     */
    void setDriftBehindThreshold(std::chrono::microseconds threshold) {
        driftBehindThreshold =
            std::chrono::duration_cast<std::chrono::nanoseconds>(threshold).count();
    }

    void addStats(const std::string& prefix, ADD_STAT add_stat, const void *c) const {
        auto maxCas = getMaxHLC();
        add_prefixed_stat(prefix.data(), "max_cas", maxCas, add_stat, c);

        // Print max_cas as a UTC human readable string
        auto nanoseconds = std::chrono::nanoseconds(maxCas);//duration
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(nanoseconds);
        time_t maxCasSeconds = seconds.count();

        std::tm tm;
        char timeString[30]; // Need to store 1970-12-31T23:23:59
        // Print as an ISO-8601 date format with nanosecond fractional part
        if (cb_gmtime_r(&maxCasSeconds, &tm) == 0 &&
            strftime(timeString, sizeof(timeString), "%Y-%m-%dT%H:%M:%S", &tm)) {
            // Get the fractional nanosecond part
            nanoseconds -= seconds;
            char finalString[40];// Needs to store 1970-12-31T23:23:59.999999999
            const char* maxCasStr = finalString;
            try {
                checked_snprintf(finalString, sizeof(finalString), "%s.%lld",
                                 timeString, nanoseconds.count());
            } catch (...) {
                // snprint fail, point at timeString which at least has the
                // majority of the time data.
                maxCasStr = timeString;
            }
            add_prefixed_stat(prefix.data(), "max_cas_str", maxCasStr, add_stat, c);
        } else {
            add_prefixed_stat(prefix.data(), "max_cas_str", "could not get string", add_stat, c);
        }

        add_prefixed_stat(prefix.data(), "total_abs_drift", cummulativeDrift.load(), add_stat, c);
        add_prefixed_stat(prefix.data(), "total_abs_drift_count", cummulativeDriftIncrements.load(), add_stat, c);
        add_prefixed_stat(prefix.data(), "drift_ahead_threshold_exceeded", driftAheadExceeded.load(), add_stat, c);
        add_prefixed_stat(prefix.data(), "drift_behind_threshold_exceeded", driftBehindExceeded.load(), add_stat, c);
        add_prefixed_stat(prefix.data(), "logical_clock_ticks", logicalClockTicks.load(), add_stat, c);

        // These are printed "as is" so we know what is being compared. Do not convert to microseconds
        add_prefixed_stat(prefix.data(), "drift_ahead_threshold", driftAheadThreshold.load(), add_stat, c);
        add_prefixed_stat(prefix.data(), "drift_behind_threshold", driftBehindThreshold.load(), add_stat, c);

    }

    void resetStats() {
        // Don't clear max_cas or the threshold values.
        cummulativeDrift = 0;
        cummulativeDriftIncrements = 0;
        driftAheadExceeded = 0;
        driftBehindExceeded = 0;
        logicalClockTicks = 0;
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
        return std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
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