/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <folly/portability/GTest.h>
#include <memcached/dockey.h>
#include <memcached/vbucket.h>

class MemcachedConnection;

namespace cb::test {

class Bucket;
class Cluster;

/**
 * Base class to build clustered tests. The test suite starts up a
 * cluster with 4 nodes, and creates a bucket named default with 8 vbuckets
 * and 2 replicas and 67MB memory size
 */
class ClusterTest : public ::testing::Test {
public:
    // Per-test-case set-up.
    // Called before the first test in this test case.
    static void SetUpTestCase();

    // Per-test-case tear-down.
    // Called after the last test in this test case.
    static void TearDownTestCase();

    // Create a bucket named default
    static void createDefaultBucket();

protected:
    // per test setup function.
    void SetUp() override;

    // per test tear-down function.
    void TearDown() override;

    void getReplica(MemcachedConnection& conn,
                    Vbid vbid,
                    const std::string& key);

    static std::unique_ptr<Cluster> cluster;
};

} // namespace cb::test
