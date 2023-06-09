/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <daemon/cookie.h>
#include <folly/portability/GTest.h>
#include <memcached/tracer.h>
#include <chrono>
#include <thread>

class MockTracer : public cb::tracing::Tracer {
public:
    std::size_t size() const {
        return vecSpans.lock()->size();
    }
};

class TracingTest : public ::testing::Test {
public:
    void SetUp() override {
        tracer.clear();
    }
    MockTracer tracer;
};

TEST_F(TracingTest, Basic) {
    // empty tracer
    EXPECT_EQ(tracer.getTotalMicros().count(), 0);

    auto now = cb::tracing::Clock::now();
    tracer.record(cb::tracing::Code::Request,
                  now,
                  now + std::chrono::microseconds(10000));

    // valid micros
    EXPECT_GE(tracer.getTotalMicros().count(), 10000);
}

TEST_F(TracingTest, MB56972) {
    auto now = cb::tracing::Clock::now();
    for (std::size_t ii = 0; ii < cb::tracing::Tracer::MaxTraceSpans + 2;
         ++ii) {
        tracer.record(cb::tracing::Code::AssociateBucket,
                      now + std::chrono::nanoseconds{ii},
                      now + std::chrono::microseconds{ii + 2});
    }

    EXPECT_EQ(cb::tracing::Tracer::MaxTraceSpans, tracer.size());
    // But we should be allowed to add a Request span (which would contain
    // the complete duration of the object)
    tracer.record(cb::tracing::Code::Request,
                  now,
                  now + std::chrono::microseconds{1000});
    EXPECT_EQ(cb::tracing::Tracer::MaxTraceSpans + 1, tracer.size());
    EXPECT_NE(std::string::npos, tracer.to_string().find("overflow="));
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
