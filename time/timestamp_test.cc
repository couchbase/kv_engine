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
#include <chrono>

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
