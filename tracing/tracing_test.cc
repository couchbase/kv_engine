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
#include "tests/mcbp/mock_connection.h"

#include <daemon/cookie.h>
#include <daemon/front_end_thread.h>
#include <folly/portability/GTest.h>
#include <memcached/tracer.h>
#include <chrono>
#include <string>
#include <thread>

FrontEndThread thread;

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

    EXPECT_EQ(tracer.begin(cb::tracing::Code::Request), 0);
    std::this_thread::sleep_for(std::chrono::microseconds(10000));

    // invalid end check
    EXPECT_FALSE(tracer.end(cb::tracing::SpanId{1}));

    // valid end
    EXPECT_TRUE(tracer.end(cb::tracing::SpanId{0}));

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
    TracingCookieTest() : connection(thread), cookie(connection) {
        cookie.setTracingEnabled(true);
    }

    void SetUp() override {
        // Record initial time as an epoch (all subsequent times should be
        // greater or equal to this).
        epoch = std::chrono::steady_clock::now();
    }

    MockConnection connection;
    Cookie cookie;
    std::chrono::steady_clock::time_point epoch;
};
