/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>

#include "executor/tracer.h"
#include <folly/container/F14Map.h>

using namespace std::chrono_literals;
using cb::executor::Tracer;
using cb::tracing::Clock;

TEST(ExecutorTracing, Basic) {
    Tracer t;
    t.record("a", Clock::time_point(10s), Clock::time_point(15s));
    t.record("a", Clock::time_point(10s), Clock::time_point(15s));
    t.record("b", Clock::time_point(10s), Clock::time_point(15s));
    t.record("c", Clock::time_point(10s), Clock::time_point(10s));

    const auto& p = t.getProfile();
    EXPECT_EQ(3, p.size());

    EXPECT_EQ(Tracer::EventData(2, 10s), p.at("a")) << "Expected 2x5s recorded";
    EXPECT_EQ(Tracer::EventData(1, 5s), p.at("b")) << "Expected 5s recorded";
    EXPECT_EQ(Tracer::EventData(1, 0s), p.at("c")) << "Expected 0s recorded";
}

TEST(ExecutorTracing, Clear) {
    Tracer t;
    t.record("a", Clock::time_point(10s), Clock::time_point(15s));

    ASSERT_EQ(1, t.getProfile().size());
    t.clear();
    EXPECT_EQ(0, t.getProfile().size()) << "Expected to find 0 items!";
}

TEST(ExecutorTracing, Overflow) {
    Tracer t;
    t.record("a",
             Clock::time_point(0s),
             Clock::time_point(Tracer::Duration::max()));
    t.record("a",
             Clock::time_point(0s),
             Clock::time_point(Tracer::Duration::max()));

    EXPECT_EQ(Tracer::Duration::max(), t.getProfile().at("a").second)
            << "Expected to clamp on overflow!";
}
