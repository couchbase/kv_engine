/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <folly/portability/GTest.h>
#include <folly/portability/Stdlib.h>
#include <memcached/isotime.h>
#include <platform/timeutils.h>
#include <chrono>

/**
 * Generate the timestamps by using the old and new method
 * representing the same time period.
 */
static std::pair<std::string, std::string> timestamps() {
    auto now = std::chrono::system_clock::now();
    const auto timestamp = now.time_since_epoch();
    const auto seconds =
            std::chrono::duration_cast<std::chrono::seconds>(timestamp);
    const auto usec = std::chrono::duration_cast<std::chrono::microseconds>(
            timestamp - seconds);

    return {to_string(now),
            ISOTime::generatetimestamp(seconds.count(), usec.count())};
}

/**
 * Generate the timestamps by using the old and new method
 * representing the user-provided time_t.
 */
static std::pair<std::string, std::string> timestamps(time_t t,
                                                      uint32_t frac = 0) {
    return {cb::time::timestamp(t, frac), ISOTime::generatetimestamp(t, frac)};
}

TEST(IsoTime, TestFormat_Compatible_Default_Locale) {
    const auto [new_style, old_style] = timestamps();
    EXPECT_EQ(old_style, new_style);
}

TEST(IsoTime, TestFormat_Compatible_from_time_t) {
    const auto [new_style, old_style] = timestamps(time(nullptr), 32);
    EXPECT_EQ(old_style, new_style);
}

/**
 * The "new" version generates an incorrect timezone offset on MacOSX
 * if we try to supply a time_t represent a time "back in the days".
 *
 * "2015-03-13T10:36:00.000000-75093:09"
 */
TEST(IsoTime, TestFormat_HistoricalTimeT_from_time_t) {
    const auto [new_style, old_style] = timestamps(1426239360);
#ifdef __APPLE__
    EXPECT_NE(old_style, new_style);
#else
    EXPECT_EQ(old_style, new_style);
#endif
}

/// Verify that a given input format generates the correct output (with
/// timezone offset)
TEST(IsoTime, TestFormat) {
    time_t now = 1426239360;
    const std::string expected("2015-03-13T02:36:00.000000-07:00");

#ifdef _MSC_VER
    setenv("TZ", "PST8PDT", 1);
#else
    setenv("TZ", "America/Los_Angeles", 1);
#endif
    tzset();
    EXPECT_EQ(expected, ISOTime::generatetimestamp(now, 0));
}

#ifndef WIN32
// It doesn't look like the new version in platform use TZ to determine
// the current time zone (on windows). Given that we're supposed to use the
// _system_ timezone (and do not override it anywhere in our system) ignore
// the tests on Windows for now.
TEST(IsoTime, TestFormat_Compatible_UTC) {
    setenv("TZ", "UTC+00:00", 1);
    tzset();
    const auto [new_style, old_style] = timestamps();

    EXPECT_EQ(old_style, new_style);
    EXPECT_EQ('Z', new_style.back()) << new_style;
    EXPECT_EQ('Z', old_style.back()) << old_style;
}

#endif