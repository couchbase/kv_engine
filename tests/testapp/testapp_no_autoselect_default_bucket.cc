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

#include <algorithm>
#include <platform/compress.h>

static std::string env{"MEMCACHED_UNIT_TESTS_NO_DEFAULT_BUCKET=true"};

class NoAutoselectDefaultBucketTest : public TestappClientTest {
public:
    static void SetUpTestCase() {
        putenv(const_cast<char*>(env.c_str()));
        ::TestappClientTest::SetUpTestCase();
    }

    static void TearDownTestCase() {
        // We need to unset the environment variable when we're done with
        // the testsuite in case someone runs all testsuites. If we don't
        // do that all of the following test suites will fail as they
        // (at least right now) expects to be associated with the default
        // bucket.
#ifdef WIN32
        // Windows don't have unsetenv, but use putenv with an empty variable
        env.resize(env.size() - 4);
        putenv(const_cast<char*>(env.c_str()));
#else
        env.resize(env.size() - 5);
        unsetenv(env.c_str());
#endif
        stop_memcached_server();
    }

    void SetUp() override {
    }

    void TearDown() override {
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         NoAutoselectDefaultBucketTest,
                         ::testing::Values(TransportProtocols::McbpPlain,
                                           TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

TEST_P(NoAutoselectDefaultBucketTest, NoAutoselect) {
    auto& conn = getAdminConnection();

    auto buckets = conn.listBuckets();
    for (auto& name : buckets) {
        if (name == "default") {
            conn.deleteBucket("default");
        }
    }
    conn.createBucket("default", "", BucketType::Memcached);

    // Reconnect (to drop the admin credentials)
    conn = getConnection();

    BinprotGetCommand cmd;
    cmd.setKey("GetKey");
    auto rsp = conn.execute(cmd);

    EXPECT_FALSE(rsp.isSuccess()) << rsp.getDataString();
    // You would have expected NO BUCKET, but we don't have access
    // to this bucket ;)
    EXPECT_EQ(cb::mcbp::Status::Eaccess, rsp.getStatus())
            << rsp.getDataString();

    conn = getAdminConnection();
    conn.deleteBucket("default");
}
