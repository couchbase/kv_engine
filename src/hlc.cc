/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "hlc.h"

#include "statwriter.h"

void HLC::addStats(const std::string& prefix, ADD_STAT add_stat, const void *c) const {
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

void HLC::resetStats() {
    // Don't clear max_cas or the threshold values.
    cummulativeDrift = 0;
    cummulativeDriftIncrements = 0;
    driftAheadExceeded = 0;
    driftBehindExceeded = 0;
    logicalClockTicks = 0;
}
