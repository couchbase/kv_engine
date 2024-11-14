/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <memcached/engine.h>
#include <memcached/engine_common.h>
#include <platform/atomic.h>
#include <platform/checked_snprintf.h>
#include <utilities/testing_hook.h>
#include <chrono>

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
 *
 * HLCT is templated on a Clock type which allows us to more easily test it.
 * HLC (a specific instantiation of the template) can be used by callers that
 * don't care about the template parameter (defaults to system_clock).
 */
template <class Clock>
class HLCT {
public:
    struct DriftStats {
        uint64_t total{0};
        uint64_t updates{0};
    };

    struct DriftExceptions {
        uint32_t ahead{0};
        uint32_t behind{0};
    };

    /*
     * @param initHLC a HLC value to start from
     * @param epochSeqno the first seqno which has a HLC generated CAS
     * @param aheadThresholdAhead threshold a peer can be ahead before we
     *        increment driftAheadExceeded. Expressed in us.
     * @param behindThresholdhreshold a peer can be ahead before we
     *        increment driftBehindExceeded. Expressed in us.
     * @param maxFutureThreshold acceptable threshold of drift at which we
     *        accept a new cas value.
     */
    HLCT(uint64_t initHLC,
         int64_t epochSeqno,
         std::chrono::microseconds aheadThreshold,
         std::chrono::microseconds behindThreshold,
         std::chrono::microseconds maxFutureThreshold)
        : maxHLC(initHLC),
          cummulativeDrift(0),
          cummulativeDriftIncrements(0),
          logicalClockTicks(0),
          driftAheadExceeded(0),
          driftBehindExceeded(0),
          epochSeqno(epochSeqno) {
        setDriftAheadThreshold(aheadThreshold);
        setDriftBehindThreshold(behindThreshold);
        setHlcMaxFutureThreshold(maxFutureThreshold);
    }

    /**
     * Returns what the next HLC value _would_ be if it was requested to
     * advance at this time, without actually advancing it; alongside
     * the current mode.
     */
    cb::HlcTime peekHLC() const;

    /**
     * Advance the HLC, returning the new time.
     */
    uint64_t nextHLC() {
        // Create a monotonic timestamp using part of the HLC algorithm by.
        // a) Reading system time
        // b) dropping 16-bits (done by nowHLC)
        // c) comparing it with the last known time (max_cas)
        // d) returning either now or max_cas + 1
        uint64_t timeNow = getMaskedTime();

        if (atomic_setIfBigger(maxHLC, timeNow)) {
            // Real clock mode
            // Set successfully, return timeNow which we just set maxHLC
            // to
            return timeNow;
        } else {
            // Logical clock mode

            // Test only hook - noop if not present
            logicalClockGetNextCasHook();

            logicalClockTicks++;

            // Implicitly successful, fetch_add should always succeed and
            // returns the value used in the increment which should be
            // unique.
            return maxHLC.fetch_add(1) + 1;
        }
    }

    void trackDrift(uint64_t hlc) {
        auto timeNow = getMaskedTime();

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
    }

    void setMaxHLCAndTrackDrift(uint64_t hlc) {
        trackDrift(hlc);
        setMaxHLC(hlc);
    }

    void forceMaxHLCAndTrackDrift(uint64_t hlc) {
        trackDrift(hlc);
        maxHLC = hlc;
    }

    void setMaxHLC(uint64_t hlc) {
        atomic_setIfBigger(maxHLC, hlc);
    }

    void forceMaxHLC(uint64_t hlc) {
        maxHLC = hlc;
    }

    bool isValidHLC(uint64_t hlc) const {
        return hlc < uint64_t(getMaskedTime() + hlcMaxFutureThreshold);
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

    void addStats(const std::string& prefix,
                  const AddStatFn& add_stat,
                  CookieIface& c) const;

    void resetStats();

    int64_t getEpochSeqno() const {
        return epochSeqno;
    }

    void setEpochSeqno(int64_t seqno) {
        epochSeqno = seqno;
    }

    static int64_t getMaskedTime() {
        return getMasked48(getTime());
    }

    uint64_t getHlcMaxFutureThreshold() const {
        return hlcMaxFutureThreshold.load();
    }

    void setHlcMaxFutureThreshold(std::chrono::microseconds duration) {
        hlcMaxFutureThreshold =
                std::chrono::duration_cast<std::chrono::nanoseconds>(duration)
                        .count();
    }

protected:
    /*
     * Returns 48-bit of t (bottom 16-bit zero)
     */
    static int64_t getMasked48(int64_t t) {
        return t & ~((1<<16)-1);
    }

    static int64_t getTime() {
        auto now = Clock::now();
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
    std::atomic<int64_t> hlcMaxFutureThreshold;

    /**
     * Documents with a seqno >= epochSeqno have a HLC generated CAS.
     */
    std::atomic<int64_t> epochSeqno;

    TestingHook<> logicalClockGetNextCasHook;
};

// Specific instantiation of the templace for general use can be found in the
// .cc.
using HLC = HLCT<std::chrono::system_clock>;
