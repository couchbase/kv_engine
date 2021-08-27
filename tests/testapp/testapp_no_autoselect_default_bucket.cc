/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp.h"
#include "testapp_client_test.h"
#include <folly/portability/Stdlib.h>

#define VARIABLE "MEMCACHED_UNIT_TESTS_NO_DEFAULT_BUCKET"

class NoAutoselectDefaultBucketTest : public TestappClientTest {
public:
    static void SetUpTestCase() {
        setenv(VARIABLE, "true", 1);
        ::TestappClientTest::SetUpTestCase();
    }

    static void TearDownTestCase() {
        // We need to unset the environment variable when we're done with
        // the testsuite in case someone runs all testsuites. If we don't
        // do that all of the following test suites will fail as they
        // (at least right now) expects to be associated with the default
        // bucket.
        unsetenv(VARIABLE);
        TestappClientTest::TearDownTestCase();
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         NoAutoselectDefaultBucketTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

TEST_P(NoAutoselectDefaultBucketTest, NoAutoselect) {
    // Verify that we have a bucket named default!
    bool found = false;
    for (const auto& b : adminConnection->listBuckets()) {
        if (b == "default") {
            found = true;
        }
    }
    ASSERT_TRUE(found) << "Did not find a bucket named default";

    auto& conn = getConnection();
    auto rsp = conn.execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::Get, "get"});
    // You would have expected NO BUCKET, but we don't have access
    // to this bucket ;)
    EXPECT_EQ(cb::mcbp::Status::Eaccess, rsp.getStatus())
            << rsp.getDataString();
}
