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

#include "hlc.h"

#include <platform/platform_time.h>
#include <statistics/cbstat_collector.h>

#include <cinttypes>

template <class Clock>
cb::HlcTime HLCT<Clock>::peekHLC() const {
    // Create a monotonic timestamp using part of the HLC algorithm by.
    // a) Reading system time
    // b) dropping 16-bits (done by nowHLC)
    // c) comparing it with the last known time (max_cas)
    // d) returning either now or max_cas + 1
    uint64_t timeNow = getMasked48(getTime());
    uint64_t l = maxHLC.load();

    using namespace std::chrono;
    if (timeNow > l) {
        nanoseconds ns(timeNow);
        return {duration_cast<seconds>(ns), cb::HlcTime::Mode::Real};
    }
    nanoseconds ns(l + 1);
    return {duration_cast<seconds>(ns), cb::HlcTime::Mode::Logical};
}

template <class Clock>
void HLCT<Clock>::addStats(const std::string& prefix,
                           const AddStatFn& add_stat,
                           CookieIface& c) const {
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
            checked_snprintf(finalString,
                             sizeof(finalString),
                             "%s.%" PRId64,
                             timeString,
                             nanoseconds.count());
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

template <class Clock>
void HLCT<Clock>::resetStats() {
    // Don't clear max_cas or the threshold values.
    cummulativeDrift = 0;
    cummulativeDriftIncrements = 0;
    driftAheadExceeded = 0;
    driftBehindExceeded = 0;
    logicalClockTicks = 0;
}

template class HLCT<std::chrono::system_clock>;
