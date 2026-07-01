/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "ep_time.h"

#include "programs/engine_testapp/mock_server.h"
#include "unit_test_server_core.h"

#include <folly/portability/GTest.h>
#include <gsl/gsl-lite.hpp>
#include <memcached/engine_error.h>

#include <limits>
#include <memory>

/**
 * A ServerCoreIface whose "system epoch" (seconds between the unix epoch and
 * memcached's epoch) is fixed at a value chosen by the test. This lets us drive
 * ep_convert_to_expiry_time() as if the wall clock were set arbitrarily far
 * into the future (e.g. near Feb 2106, when the epoch approaches 2^32) without
 * touching the real system clock.
 *
 * Derives from UnitTestServerCore so only the time-related members need
 * overriding; everything else (including get_current_time(), which keeps the
 * uptime standing still at 0) inherits the unit-test defaults.
 *
 * realtime()/abstime() mirror the production mc_time relationship with the
 * uptime held at 0 (unit-test convention that "time stands still"):
 *   realtime(t) : relative t (<= 30 days) maps straight through; an absolute t
 *                 (> 30 days) is expressed relative to the epoch.
 *   abstime(r)  : epoch + r, returned as a 64-bit time_t (may exceed uint32_t).
 */
class FixedEpochServerCore : public UnitTestServerCore {
public:
    explicit FixedEpochServerCore(time_t systemEpochSeconds)
        : systemEpochSeconds(systemEpochSeconds) {
    }

    // 30 days in seconds - the boundary above which an exptime is treated as an
    // absolute time (matches memcached_maximum_relative_time in mc_time.cc).
    static constexpr rel_time_t maxRelativeTime = 60 * 60 * 24 * 30;

    rel_time_t realtime(rel_time_t exptime) override {
        if (exptime == 0) {
            return 0;
        }
        if (exptime > maxRelativeTime) {
            // Absolute time - express it relative to the epoch.
            if (exptime <= systemEpochSeconds) {
                return 1;
            }
            return gsl::narrow_cast<rel_time_t>(exptime - systemEpochSeconds);
        }
        // Relative time; uptime is 0 so it passes straight through.
        return exptime;
    }

    time_t abstime(rel_time_t reltime) override {
        return systemEpochSeconds + reltime;
    }

private:
    const time_t systemEpochSeconds;
};

class EpConvertExpiryTest : public ::testing::Test {
protected:
    void TearDown() override {
        // Restore the shared mock-server core so subsequent tests (which may
        // rely on the global time functions) are unaffected.
        initialize_time_functions(get_mock_server_api()->core);
    }

    /// Install a core with the given system epoch.
    void installEpoch(time_t systemEpochSeconds) {
        core = std::make_unique<FixedEpochServerCore>(systemEpochSeconds);
        initialize_time_functions(core.get());
    }

    /// Install a core with the given system epoch, then run mcbpExpTime through
    /// the production expiry conversion.
    uint32_t convertWithEpoch(time_t systemEpochSeconds, uint32_t mcbpExpTime) {
        installEpoch(systemEpochSeconds);
        return ep_convert_to_expiry_time(mcbpExpTime);
    }

    std::unique_ptr<FixedEpochServerCore> core;
};

// A zero exptime means "never expires" and must be preserved regardless of the
// clock.
TEST_F(EpConvertExpiryTest, ZeroIsNeverExpire) {
    EXPECT_EQ(0, convertWithEpoch(0xffffffff - 10, 0));
}

// On a normally-configured system the absolute expiry fits in uint32_t and is
// returned unchanged (epoch + relative expiry).
TEST_F(EpConvertExpiryTest, NormalClockConverts) {
    const time_t epoch = 1700000000; // ~Nov 2023
    EXPECT_EQ(epoch + 100, convertWithEpoch(epoch, 100));
}

// MB-67576: when the system clock approaches 2^32 seconds since the unix epoch
// (~Feb 2106), epoch + relative-expiry overflows uint32_t. The old narrowing
// cast wrapped this far-future expiry into a small (past) value, instantly
// deleting the item. The conversion must instead throw so callers can fail the
// command rather than store an expiry that cannot be stored/replicated as a
// 32-bit value.
TEST_F(EpConvertExpiryTest, FailsWhenEpochNear2106) {
    // epoch is 10 seconds shy of 2^32; a 100s relative expiry would compute to
    // 2^32 + 90, which does not fit uint32_t.
    const time_t epoch = std::numeric_limits<uint32_t>::max() - 10;
    installEpoch(epoch);
    try {
        ep_convert_to_expiry_time(100);
        FAIL() << "expected ep_convert_to_expiry_time to throw on overflow";
    } catch (const cb::engine_error& e) {
        EXPECT_EQ(cb::engine_errc::expiry_overflow, e.engine_code());
    }
}

// Boundary: an absolute expiry of exactly uint32_t::max fits and is returned,
// whereas one second beyond overflows and throws. Confirms the guard is
// '> max' rather than '>= max'.
TEST_F(EpConvertExpiryTest, OverflowBoundaryIsExclusive) {
    const auto max = std::numeric_limits<uint32_t>::max();

    // epoch + 5 == max exactly -> fits, returned as-is.
    EXPECT_EQ(max, convertWithEpoch(max - 5, 5));

    // epoch + 6 == max + 1 -> overflows, conversion throws.
    installEpoch(max - 5);
    EXPECT_THROW(ep_convert_to_expiry_time(6), cb::engine_error);
}
