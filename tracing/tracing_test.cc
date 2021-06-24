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
