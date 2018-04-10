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

#include "config.h"

#include "tests/mcbp/mock_connection.h"
#include "tracing/trace_helpers.h"

#include <unistd.h>
#include <algorithm>
#include <iostream>
#include <string>

#include <gtest/gtest.h>
#include <tracing/tracer.h>

class TracingTest : public ::testing::Test {
public:
    void SetUp() override {
        tracer.clear();
    }
    cb::tracing::Tracer tracer;
};

TEST_F(TracingTest, Basic) {
    // empty tracer
    EXPECT_EQ(tracer.getTotalMicros().count(), 0);

    EXPECT_EQ(tracer.begin(cb::tracing::TraceCode::REQUEST), 0);
    usleep(10000);

    // invalid end check
    EXPECT_FALSE(tracer.end(cb::tracing::TraceCode::GET));

    // valid end
    EXPECT_TRUE(tracer.end(cb::tracing::TraceCode::REQUEST));

    // valid micros
    EXPECT_GE(tracer.getTotalMicros().count(), 10000);
}

TEST_F(TracingTest, ErrorRate) {
    uint64_t micros_list[] = {5,
                              11,
                              1439,
                              6234,
                              7890,
                              99999,
                              4567321,
                              98882110,
                              78821369,
                              118916406};
    for (auto micros : micros_list) {
        auto repMicros = tracer.encodeMicros(micros);
        auto decoded = uint64_t(tracer.decodeMicros(repMicros).count());

        if (decoded > micros) {
            std::swap(micros, decoded);
        }
        // check if the error is less than 0.5%
        EXPECT_LE((micros - decoded) * 100.0 / micros, 0.5);
    }
}

/// Text fixture for session tracing associated with a Cookie object.
class TracingCookieTest : public ::testing::Test {
protected:
    TracingCookieTest() : cookie(connection) {
        cookie.setTracingEnabled(true);
    }

    void SetUp() {
        // Record initial time as an epoch (all subsequent times should be
        // greater or equal to this).
        epoch = ProcessClock::now();
    }

    MockConnection connection;
    Cookie cookie;
    ProcessClock::time_point epoch;
};

TEST_F(TracingCookieTest, InstantTracerBegin) {
    InstantTracer(cookie, cb::tracing::TraceCode::REQUEST, /*start*/ true);

    const auto& durations = cookie.getTracer().getDurations();
    EXPECT_EQ(1u, durations.size());
    EXPECT_GT(durations[0].start, epoch);
    EXPECT_EQ(cb::tracing::Span::Duration::max(), durations[0].duration)
            << "Span should be half-open when we have not yet ended it.";
}

TEST_F(TracingCookieTest, InstantTracerBeginEnd) {
    InstantTracer(cookie, cb::tracing::TraceCode::REQUEST, /*start*/ true);
    // Ensure we have a non-zero span duration
    std::this_thread::sleep_for(std::chrono::microseconds(1));
    InstantTracer(cookie, cb::tracing::TraceCode::REQUEST, /*start*/ false);

    const auto& durations = cookie.getTracer().getDurations();
    EXPECT_EQ(1u, durations.size());
    EXPECT_GT(durations[0].start, epoch);
    EXPECT_LT(durations[0].duration, cb::tracing::Span::Duration::max())
            << "Span should be closed once we have ended it.";
    EXPECT_GE(durations[0].duration, std::chrono::microseconds(1))
            << "Span should have non-zero duration once we have ended it.";
}

/// Test that omitting a begin all (and only ending) doesn't create a trace
/// span.
TEST_F(TracingCookieTest, InstantTracerEnd) {
    InstantTracer(cookie, cb::tracing::TraceCode::REQUEST, /*start*/ false);

    const auto& durations = cookie.getTracer().getDurations();
    EXPECT_EQ(0u, durations.size());
}