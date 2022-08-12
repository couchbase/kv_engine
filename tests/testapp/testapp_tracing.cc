/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <chrono>
#include <iostream>

#include "testapp.h"

class TracingTest : public TestappTest {
public:
    void setTracingFeatureOnServer(bool enabled) {
        memcached_cfg["tracing_enabled"] = enabled;
        reconfigure();
    }

    void SetUp() override {
        TestappTest::SetUp();
        document.info.cas = cb::mcbp::cas::Wildcard;
        document.info.flags = 0xcaffee;
        document.info.id = name;
        document.info.expiration = 0;
        document.value = memcached_cfg.dump();
        userConnection->setFeature(cb::mcbp::Feature::Tracing, false);
    }

    static void SetUpTestCase() {
        TestappTest::SetUpTestCase();
        createUserConnection = true;
    }

protected:
    Document document;
};

TEST_F(TracingTest, NoDataUnlessRequested) {
    // Enable Tracing feature on Server
    setTracingFeatureOnServer(true);

    // Tracing is NOT explicitly requested, so no trace data
    userConnection->mutate(document, Vbid(0), MutationType::Add);
    EXPECT_FALSE(userConnection->getTraceData());
    EXPECT_FALSE(userConnection->hasFeature(cb::mcbp::Feature::Tracing));
}

TEST_F(TracingTest, ValidDataOnRequest) {
    // Enable Tracing feature on Server
    setTracingFeatureOnServer(true);

    // Request Trace Info
    userConnection->setFeature(cb::mcbp::Feature::Tracing, true);

    // Expect some trace data
    auto start = std::chrono::steady_clock::now();
    userConnection->mutate(document, Vbid(0), MutationType::Add);
    auto end = std::chrono::steady_clock::now();
    auto duration =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    auto traceData = userConnection->getTraceData();
    EXPECT_TRUE(traceData);
    EXPECT_TRUE(userConnection->hasFeature(cb::mcbp::Feature::Tracing));

    // expect the above operation to complete in <= overall time
    EXPECT_LE(*traceData, duration);
}

TEST_F(TracingTest, NoDataWhenDisabledOnServer) {
    // Disable Tracing feature on Server
    setTracingFeatureOnServer(false);

    // Tracing is disabled on server, so no trace data
    userConnection->mutate(document, Vbid(0), MutationType::Add);
    EXPECT_FALSE(userConnection->getTraceData());
    EXPECT_FALSE(userConnection->hasFeature(cb::mcbp::Feature::Tracing));
}

TEST_F(TracingTest, FailOnFeatureRequestWhenDisabledOnServer) {
    // Disable Tracing feature on Server
    setTracingFeatureOnServer(false);

    // Request Tracing Data
    // This will fail as the feature is disabled on Server
    EXPECT_THROW(userConnection->setFeature(cb::mcbp::Feature::Tracing, true),
                 std::runtime_error);
}
