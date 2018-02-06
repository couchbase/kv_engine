/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include <platform/processclock.h>
#include <iostream>

#include "testapp.h"

class TracingTest : public TestappTest {
public:
    void setTracingFeatureOnServer(bool enabled) {
        // Delete the old item from the array
        cJSON_DeleteItemFromObject(memcached_cfg.get(), "tracing_enabled");
        cJSON_AddBoolToObject(memcached_cfg.get(), "tracing_enabled", enabled);

        // update the server to use this!
        reconfigure(memcached_cfg);
    }

    void SetUp() override {
        TestappTest::SetUp();
        document.info.cas = mcbp::cas::Wildcard;
        document.info.flags = 0xcaffee;
        document.info.id = name;
        document.info.expiration = 0;
        document.value = to_string(memcached_cfg, false);
    }

protected:
    Document document;
};

TEST_F(TracingTest, NoDataUnlessRequested) {
    MemcachedConnection& conn = getConnection();

    // Enable Tracing feature on Server
    setTracingFeatureOnServer(true);

    // Turn OFF feature from client
    conn.setFeature(cb::mcbp::Feature::Tracing, false);

    // Tracing is NOT explicitly requested, so no trace data
    conn.mutate(document, 0, MutationType::Add);
    EXPECT_FALSE(conn.getTraceData());
    EXPECT_FALSE(conn.hasFeature(cb::mcbp::Feature::Tracing));
}

TEST_F(TracingTest, ValidDataOnRequest) {
    MemcachedConnection& conn = getConnection();
    // Enable Tracing feature on Server
    setTracingFeatureOnServer(true);

    // Request Trace Info
    conn.setFeature(cb::mcbp::Feature::Tracing, true);

    // Expect some trace data
    auto start = ProcessClock::now();
    conn.mutate(document, 0, MutationType::Add);
    auto end = ProcessClock::now();
    auto duration =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    auto traceData = conn.getTraceData();
    EXPECT_TRUE(traceData);
    EXPECT_TRUE(conn.hasFeature(cb::mcbp::Feature::Tracing));

    // expect the above operation to complete in <= overall time
    EXPECT_LE(*traceData, duration);
}

TEST_F(TracingTest, NoDataWhenDisabledOnServer) {
    MemcachedConnection& conn = getConnection();

    // Disable Tracing feature on Server
    setTracingFeatureOnServer(false);

    // Tracing is disabled on server, so no trace data
    conn.mutate(document, 0, MutationType::Add);
    EXPECT_FALSE(conn.getTraceData());
    EXPECT_FALSE(conn.hasFeature(cb::mcbp::Feature::Tracing));
}

TEST_F(TracingTest, FailOnFeatureRequestWhenDisabledOnServer) {
    MemcachedConnection& conn = getConnection();

    // Disable Tracing feature on Server
    setTracingFeatureOnServer(false);

    // Request Tracing Data
    // This will fail as the feature is disabled on Server
    EXPECT_THROW(conn.setFeature(cb::mcbp::Feature::Tracing, true),
                 std::runtime_error);
}
