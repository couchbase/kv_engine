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

#include <unistd.h>
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
    uint64_t micros_list[] = {1,
                              11,
                              1439,
                              6234,
                              7890,
                              99999,
                              4567321,
                              98882110,
                              78821369,
                              138916406};
    for (const auto micros : micros_list) {
        auto repMicros = tracer.getEncodedMicros(micros);
        auto decoded = tracer.decodeMicros(repMicros).count();

        // check if the error is less than 0.1%
        EXPECT_LE((micros - decoded) * 100.0 / micros, 0.1);
    }
}
