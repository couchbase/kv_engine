/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <folly/portability/GTest.h>

class MemcachedConnection;

namespace cb::test {

class Cluster;

class ServerlessTest : public ::testing::Test {
public:
    // Per-test-case set-up.
    // Called before the first test in this test case.
    static void SetUpTestCase();

    // Per-test-case tear-down.
    // Called after the last test in this test case.
    static void TearDownTestCase();

    /// Start the cluster with 3 nodes all set to serverless deployment
    static void StartCluster();

    /// Shutdown the cluster
    static void ShutdownCluster();

protected:
    // per test setup function.
    void SetUp() override;

    // per test tear-down function.
    void TearDown() override;

    static std::unique_ptr<Cluster> cluster;
};

} // namespace cb::test
