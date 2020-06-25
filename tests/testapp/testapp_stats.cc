/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

#include "testapp_client_test.h"
#include <protocol/mcbp/ewb_encode.h>
#include <gsl/gsl>

class StatsTest : public TestappClientTest {
public:
    void SetUp() override {
        TestappClientTest::SetUp();
        // Let all tests start with an empty set of stats (There is
        // a special test case that tests that reset actually work)
        resetBucket();
    }

protected:
    void resetBucket() {
        MemcachedConnection& conn = getConnection();
        ASSERT_NO_THROW(conn.authenticate("@admin", "password", "PLAIN"));
        ASSERT_NO_THROW(conn.selectBucket("default"));
        ASSERT_NO_THROW(conn.stats("reset"));
        ASSERT_NO_THROW(conn.reconnect());
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         StatsTest,
                         ::testing::Values(TransportProtocols::McbpPlain,
                                           TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

TEST_P(StatsTest, TestDefaultStats) {
    MemcachedConnection& conn = getConnection();
    auto stats = conn.stats("");

    // Don't expect the entire stats set, but we should at least have
    // the uptime
    EXPECT_NE(stats.end(), stats.find("uptime"));
}

TEST_P(StatsTest, TestGetMeta) {
    MemcachedConnection& conn = getConnection();

    // Set a document
    Document doc;
    doc.info.cas = mcbp::cas::Wildcard;
    doc.info.flags = 0xcaffee;
    doc.info.id = name;
    doc.value = memcached_cfg.dump();
    conn.mutate(doc, Vbid(0), MutationType::Set);

    // Send 10 GET_META, this should not increase the `cmd_get` and `get_hits` stats
    for (int i = 0; i < 10; i++) {
        auto meta = conn.getMeta(doc.info.id, Vbid(0), GetMetaVersion::V1);
        EXPECT_EQ(cb::mcbp::Status::Success, meta.first);
    }
    auto stats = conn.stats("");

    auto cmd_get = stats["cmd_get"].get<size_t>();
    EXPECT_EQ(0, cmd_get);

    auto get_hits = stats["get_hits"].get<size_t>();
    EXPECT_EQ(0, get_hits);

    // Now, send 10 GET_META for a document that does not exist, this should
    // not increase the `cmd_get` and `get_misses` stats or the `get_hits`
    // stat
    for (int i = 0; i < 10; i++) {
        auto meta = conn.getMeta("no_key", Vbid(0), GetMetaVersion::V1);
        EXPECT_EQ(cb::mcbp::Status::KeyEnoent, meta.first);
    }
    stats = conn.stats("");

    cmd_get = stats["cmd_get"].get<size_t>();
    EXPECT_EQ(0, cmd_get);

    auto get_misses = stats["get_misses"].get<size_t>();
    EXPECT_EQ(0, get_misses);

    get_hits = stats["get_hits"].get<size_t>();
    EXPECT_EQ(0, get_hits);
}

TEST_P(StatsTest, StatsResetIsPrivileged) {
    MemcachedConnection& conn = getConnection();

    try {
        conn.stats("reset");
        FAIL() << "reset is a privileged operation";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied());
    }

    conn.authenticate("@admin", "password", "PLAIN");
    conn.stats("reset");
}

TEST_P(StatsTest, TestReset) {
    MemcachedConnection& conn = getConnection();

    auto stats = conn.stats("");
    ASSERT_FALSE(stats.empty());

    auto before = stats["cmd_get"].get<size_t>();

    for (int ii = 0; ii < 10; ++ii) {
        EXPECT_THROW(conn.get("foo", Vbid(0)), ConnectionError);
    }

    stats = conn.stats("");
    EXPECT_NE(before, stats["cmd_get"].get<size_t>());

    // the cmd_get counter does work.. now check that reset sets it back..
    resetBucket();

    stats = conn.stats("");
    EXPECT_EQ(0, stats["cmd_get"].get<size_t>());

    // Just ensure that the "reset timings" is detected
    // @todo add a separate test case for cmd timings stats
    conn.authenticate("@admin", "password", "PLAIN");
    conn.selectBucket("default");
    stats = conn.stats("reset timings");

    // Just ensure that the "reset bogus" is detected..
    try {
        conn.stats("reset bogus");
        FAIL()<<"stats reset bogus should throw an exception (non a valid cmd)";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments());
    }
    conn.reconnect();
}

/**
 * MB-17815: The cmd_set stat is incremented multiple times if the underlying
 * engine returns EWOULDBLOCK (which would happen for all operations when
 * the underlying engine is operating in full eviction mode and the document
 * isn't resident)
 */
TEST_P(StatsTest, Test_MB_17815) {
    MemcachedConnection& conn = getConnection();

    auto stats = conn.stats("");
    EXPECT_EQ(0, stats["cmd_set"].get<size_t>());

    auto sequence = ewb::encodeSequence({cb::engine_errc::would_block,
                                         cb::engine_errc::success,
                                         ewb::Passthrough,
                                         cb::engine_errc::would_block,
                                         cb::engine_errc::success,
                                         ewb::Passthrough});
    conn.configureEwouldBlockEngine(EWBEngineMode::Sequence,
                                    /*unused*/ {},
                                    /*unused*/ {},
                                    sequence);

    Document doc;
    doc.info.cas = mcbp::cas::Wildcard;
    doc.info.flags = 0xcaffee;
    doc.info.id = name;
    doc.value = memcached_cfg.dump();

    conn.mutate(doc, Vbid(0), MutationType::Add);

    conn.disableEwouldBlockEngine();

    stats = conn.stats("");
    EXPECT_EQ(1, stats["cmd_set"].get<size_t>());
}

/**
 * MB-17815: The cmd_set stat is incremented multiple times if the underlying
 * engine returns EWOULDBLOCK (which would happen for all operations when
 * the underlying engine is operating in full eviction mode and the document
 * isn't resident). This test is specfically testing this error case with
 * append (due to MB-28850) rather than the other MB-17815 test which tests
 * Add.
 */
TEST_P(StatsTest, Test_MB_17815_Append) {
    MemcachedConnection& conn = getConnection();

    auto stats = conn.stats("");
    EXPECT_EQ(0, stats["cmd_set"].get<size_t>());

    // Allow first SET to succeed and then return EWOULDBLOCK for
    // the Append (2nd op).

    // Set a document
    Document doc;
    doc.info.cas = mcbp::cas::Wildcard;
    doc.info.flags = 0xcaffee;
    doc.info.id = name;
    doc.value = memcached_cfg.dump();
    conn.mutate(doc, Vbid(0), MutationType::Set);

    // bucket_get -> Passthrough,
    // bucket_allocate -> Passthrough,
    // bucket_CAS -> EWOULDBLOCK (success)
    // bucket_CAS (retry) -> Passthrough
    auto sequence = ewb::encodeSequence({ewb::Passthrough,
                                         ewb::Passthrough,
                                         cb::engine_errc::would_block,
                                         cb::engine_errc::success,
                                         ewb::Passthrough});
    conn.configureEwouldBlockEngine(EWBEngineMode::Sequence,
                                    /*unused*/ {},
                                    /*unused*/ {},
                                    sequence);

    // Now append to the same doc
    conn.mutate(doc, Vbid(0), MutationType::Append);

    conn.disableEwouldBlockEngine();

    stats = conn.stats("");
    EXPECT_EQ(2, stats["cmd_set"].get<size_t>());
}

/**
 * Verify that cmd_set is updated when we fail to perform the
 * append operation.
 */
TEST_P(StatsTest, Test_MB_29259_Append) {
    MemcachedConnection& conn = getConnection();

    auto stats = conn.stats("");
    EXPECT_EQ(0, stats["cmd_set"].get<size_t>());

    Document doc;
    doc.info.cas = mcbp::cas::Wildcard;
    doc.info.flags = 0xcaffee;
    doc.info.id = name;
    doc.value = memcached_cfg.dump();

    // Try to append to non-existing document
    try {
        conn.mutate(doc, Vbid(0), MutationType::Append);
        FAIL() << "Append on non-existing document should fail";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isNotStored());
    }

    stats = conn.stats("");
    EXPECT_EQ(1, stats["cmd_set"].get<size_t>());
}

TEST_P(StatsTest, TestAppend) {
    MemcachedConnection& conn = getConnection();

    // Set a document
    Document doc;
    doc.info.cas = mcbp::cas::Wildcard;
    doc.info.flags = 0xcaffee;
    doc.info.id = name;
    doc.value = memcached_cfg.dump();
    conn.mutate(doc, Vbid(0), MutationType::Set);

    // Send 10 appends, this should increase the `cmd_set` stat by 10
    for (int i = 0; i < 10; i++) {
        conn.mutate(doc, Vbid(0), MutationType::Append);
    }
    auto stats = conn.stats("");
    // In total we expect 11 sets, since there was the initial set
    // and then 10 appends
    EXPECT_EQ(11, stats["cmd_set"].get<size_t>());
}

/// Verify that we don't keep invalid pointers around when the packet is
/// relocated as part of EWB
TEST_P(StatsTest, MB37147_TestEWBReturnFromStat) {
    if (!mcd_env->getTestBucket().supportsPersistence()) {
        std::cout
                << "Note: skipping test '"
                << ::testing::UnitTest::GetInstance()
                           ->current_test_info()
                           ->name()
                << "' as the underlying engine don't support vbucket stats.\n";
        return;
    }
    MemcachedConnection& conn = getConnection();
    conn.authenticate("@admin", "password", "PLAIN");
    conn.selectBucket("default");

    auto sequence = ewb::encodeSequence({cb::engine_errc::would_block,
                                         cb::engine_errc::success,
                                         ewb::Passthrough});
    conn.configureEwouldBlockEngine(EWBEngineMode::Sequence,
                                    /*unused*/ {},
                                    /*unused*/ {},
                                    sequence);

    auto stats = conn.stats("vbucket");

    EXPECT_FALSE(stats.empty());
}

TEST_P(StatsTest, TestAuditNoAccess) {
    MemcachedConnection& conn = getConnection();

    try {
        conn.stats("audit");
        FAIL() << "stats audit should throw an exception (non privileged)";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied());
    }
}

TEST_P(StatsTest, TestAudit) {
    MemcachedConnection& conn = getConnection();
    conn.authenticate("@admin", "password", "PLAIN");

    auto stats = conn.stats("audit");
    EXPECT_EQ(2, stats.size());
    EXPECT_EQ(false, stats["enabled"].get<bool>());
    EXPECT_EQ(0, stats["dropped_events"].get<size_t>());

    conn.reconnect();
}

TEST_P(StatsTest, TestBucketDetailsNoAccess) {
    MemcachedConnection& conn = getConnection();

    try {
        conn.stats("bucket_details");
        FAIL() <<
               "stats bucket_details should throw an exception (non privileged)";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied());
    }
}

TEST_P(StatsTest, TestBucketDetails) {
    MemcachedConnection& conn = getConnection();
    conn.authenticate("@admin", "password", "PLAIN");

    auto stats = conn.stats("bucket_details");
    ASSERT_EQ(1, stats.size());
    ASSERT_EQ("bucket details", stats.begin().key());

    // bucket details contains a single entry which is named "buckets" and
    // contains an array
    auto array = stats.front()["buckets"];
    EXPECT_EQ(nlohmann::json::value_t::array, array.type());

    // we have two bucket2, nobucket and default
    EXPECT_EQ(2, array.size());

    // Validate each bucket entry (I should probably extend it with checking
    // of the actual values
    for (const auto& bucket : array) {
        EXPECT_EQ(5, bucket.size());
        EXPECT_NE(bucket.end(), bucket.find("index"));
        EXPECT_NE(bucket.end(), bucket.find("state"));
        EXPECT_NE(bucket.end(), bucket.find("clients"));
        EXPECT_NE(bucket.end(), bucket.find("name"));
        EXPECT_NE(bucket.end(), bucket.find("type"));
    }

    conn.reconnect();
}

TEST_P(StatsTest, TestSchedulerInfo) {
    auto stats = getAdminConnection().stats("worker_thread_info");
    // We should at least have an entry for the first thread
    EXPECT_NE(stats.end(), stats.find("0"));
}

TEST_P(StatsTest, TestSchedulerInfo_Aggregate) {
    auto stats = getAdminConnection().stats("worker_thread_info aggregate");
    EXPECT_NE(stats.end(), stats.find("aggregate"));
}

TEST_P(StatsTest, TestSchedulerInfo_InvalidSubcommand) {
    try {
        getAdminConnection().stats("worker_thread_info foo");
        FAIL() << "Invalid subcommand";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments());
    }
}

TEST_P(StatsTest, TestAggregate) {
    MemcachedConnection& conn = getConnection();
    auto stats = conn.stats("aggregate");
    // Don't expect the entire stats set, but we should at least have
    // the uptime
    EXPECT_NE(stats.end(), stats.find("uptime"));
}

TEST_P(StatsTest, TestPrivilegedConnections) {
    MemcachedConnection& conn = getAdminConnection();
    conn.setAgentName("TestPrivilegedConnections 1.0");
    conn.setFeatures({cb::mcbp::Feature::XERROR});
    auto stats = conn.stats("connections");
    // We have at _least_ 2 connections
    ASSERT_LE(2, stats.size());

    stats = conn.stats("connections self");
    ASSERT_EQ(1, stats.size());
    EXPECT_EQ("TestPrivilegedConnections 1.0",
              stats.front()["agent_name"].get<std::string>());

    stats = conn.stats("connections " +
                       std::to_string(stats.front()["socket"].get<size_t>()));
    ASSERT_EQ(1, stats.size());
    EXPECT_EQ("TestPrivilegedConnections 1.0",
              stats.front()["agent_name"].get<std::string>());
}

TEST_P(StatsTest, TestUnprivilegedConnections) {
    // Everyone should be allowed to see its own connection details
    MemcachedConnection& conn = getConnection();
    conn.setAgentName("TestUnprivilegedConnections 1.0");
    conn.setFeatures({cb::mcbp::Feature::XERROR});
    auto stats = conn.stats("connections");
    ASSERT_LE(1, stats.size());
    EXPECT_EQ("TestUnprivilegedConnections 1.0",
              stats.front()["agent_name"].get<std::string>());

    const auto me = stats.front()["socket"].get<size_t>();
    stats = conn.stats("connections " + std::to_string(me));
    ASSERT_EQ(1, stats.size());
    EXPECT_EQ(me, stats.front()["socket"].get<size_t>());
    EXPECT_EQ("TestUnprivilegedConnections 1.0",
              stats.front()["agent_name"].get<std::string>());
    stats = conn.stats("connections self");
    ASSERT_EQ(1, stats.size());
    EXPECT_EQ(me, stats.front()["socket"].get<size_t>());
    EXPECT_EQ("TestUnprivilegedConnections 1.0",
              stats.front()["agent_name"].get<std::string>());
    EXPECT_EQ(me, conn.getServerConnectionId());
}

TEST_P(StatsTest, TestConnectionsInvalidNumber) {
    MemcachedConnection& conn = getAdminConnection();
    try {
        auto stats = conn.stats("connections xxx");
        FAIL() << "Did not detect incorrect connection number";

    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments());
    }
}

TEST_P(StatsTest, TestTopkeys) {
    MemcachedConnection& conn = getConnection();

    for (int ii = 0; ii < 10; ++ii) {
        Document doc;
        doc.info.cas = mcbp::cas::Wildcard;
        doc.info.flags = 0xcaffee;
        doc.info.id = name;
        doc.value = memcached_cfg.dump();

        conn.mutate(doc, Vbid(0), MutationType::Set);
    }

    auto stats = conn.stats("topkeys");
    EXPECT_NE(stats.end(), stats.find(name));
}

TEST_P(StatsTest, TestTopkeysJson) {
    MemcachedConnection& conn = getConnection();

    for (int ii = 0; ii < 10; ++ii) {
        Document doc;
        doc.info.cas = mcbp::cas::Wildcard;
        doc.info.flags = 0xcaffee;
        doc.info.id = name;
        doc.value = memcached_cfg.dump();

        conn.mutate(doc, Vbid(0), MutationType::Set);
    }

    auto stats = conn.stats("topkeys_json").front();
    bool found = false;
    for (const auto& i : stats) {
        for (const auto j : i) {
            if (name == j["key"]) {
                found = true;
                break;
            }
        }
    }

    EXPECT_TRUE(found);
}

TEST_P(StatsTest, TestSubdocExecute) {
    MemcachedConnection& conn = getConnection();
    auto stats = conn.stats("subdoc_execute");

    // json returned should have zero samples as no ops have been performed
    EXPECT_TRUE(stats.is_object());
    EXPECT_EQ(0, stats["0"]["total"].get<uint64_t>());
}

TEST_P(StatsTest, TestResponseStats) {
    int successCount = getResponseCount(cb::mcbp::Status::Success);
    // 2 successes expected:
    // 1. The previous stats call sending the JSON
    // 2. The previous stats call sending a null packet to mark end of stats
    EXPECT_EQ(successCount + statResps(),
              getResponseCount(cb::mcbp::Status::Success));
}

TEST_P(StatsTest, TracingStatsIsPrivileged) {
    MemcachedConnection& conn = getConnection();

    try {
        conn.stats("tracing");
        FAIL() << "tracing is a privileged operation";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied());
    }

    conn.authenticate("@admin", "password", "PLAIN");
    conn.stats("tracing");
}

TEST_P(StatsTest, TestTracingStats) {
    MemcachedConnection& conn = getConnection();
    conn.authenticate("@admin", "password", "PLAIN");

    auto stats = conn.stats("tracing");

    // Just check that we got some stats, no need to check all of them
    // as we don't want memcached to be testing phosphor's logic
    EXPECT_FALSE(stats.empty());
    auto enabled = stats.find("log_is_enabled");
    EXPECT_NE(stats.end(), enabled);
}

TEST_P(StatsTest, TestSingleBucketOpStats) {
    MemcachedConnection& conn = getConnection();
    conn.authenticate("@admin", "password", "PLAIN");

    conn.selectBucket("default");

    std::string key = "key";

    // Set a document
    Document doc;
    doc.info.cas = mcbp::cas::Wildcard;
    doc.info.flags = 0xcaffee;
    doc.info.id = key;
    doc.value = "asdf";

    // mutate to bump stat
    conn.mutate(doc, Vbid(0), MutationType::Set);
    // lookup to bump stat
    conn.get(key, Vbid(0), {} /* getFrameInfo */);

    auto stats = conn.stats("");

    EXPECT_FALSE(stats.empty());

    auto lookup = stats.find("cmd_lookup");
    auto mutation = stats.find("cmd_mutation");

    ASSERT_NE(stats.end(), lookup);
    ASSERT_NE(stats.end(), mutation);

    EXPECT_EQ(1, int(*lookup));
    EXPECT_EQ(1, int(*mutation));
}

/**
 * Subclass of StatsTest which doesn't have a default bucket; hence connections
 * will intially not be associated with any bucket.
 */
class NoBucketStatsTest : public StatsTest {
public:
    static void SetUpTestCase() {
        StatsTest::SetUpTestCase();
    }

    // Setup as usual, but delete the default bucket before starting the
    // testcase and reconnect) so the user isn't associated with any bucket.
    void SetUp() override {
        StatsTest::SetUp();
        DeleteTestBucket();
        getConnection().reconnect();
    }

    // Reverse of above - re-create the default bucket to keep the parent
    // classes happy.
    void TearDown() override {
        CreateTestBucket();
        StatsTest::TearDown();
    }
};

TEST_P(NoBucketStatsTest, TestTopkeysNoBucket) {
    MemcachedConnection& conn = getConnection();
    conn.authenticate("@admin", "password", "PLAIN");

    // The actual request is expected fail with a nobucket exception.
    EXPECT_THROW(conn.stats("topkeys"), std::runtime_error);
}

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         NoBucketStatsTest,
                         ::testing::Values(TransportProtocols::McbpPlain,
                                           TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());
