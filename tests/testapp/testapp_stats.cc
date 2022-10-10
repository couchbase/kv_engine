/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp_client_test.h"
#include <gsl/gsl-lite.hpp>
#include <protocol/mcbp/ewb_encode.h>

using namespace std::string_view_literals;

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
        adminConnection->executeInBucket(bucketName, [](auto& connection) {
            connection.stats("reset");
        });
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         StatsTest,
                         ::testing::Values(TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

TEST_P(StatsTest, TestDefaultStats) {
    auto stats = userConnection->stats("");

    // Don't expect the entire stats set, but we should at least have
    // the uptime
    EXPECT_NE(stats.end(), stats.find("uptime"));

    // MB-39722: bytes_read and bytes_written counts all the bytes sent and
    //           received on _all_ connections. It should therefore be a
    //           positive number and never equal to 0
    EXPECT_LT(0, stats["bytes_written"].get<int>());
    EXPECT_LT(0, stats["bytes_read"].get<int>());
}

TEST_P(StatsTest, TestGetMeta) {
    // Set a document
    Document doc;
    doc.info.cas = cb::mcbp::cas::Wildcard;
    doc.info.flags = 0xcaffee;
    doc.info.id = name;
    doc.value = memcached_cfg.dump();
    userConnection->mutate(doc, Vbid(0), MutationType::Set);

    // Send 10 GET_META, this should not increase the `cmd_get` and `get_hits` stats
    for (int i = 0; i < 10; i++) {
        auto meta = userConnection->getMeta(
                doc.info.id, Vbid(0), GetMetaVersion::V1);
        EXPECT_EQ(cb::mcbp::Status::Success, meta.first);
    }
    auto stats = userConnection->stats("");

    auto cmd_get = stats["cmd_get"].get<size_t>();
    EXPECT_EQ(0, cmd_get);

    auto get_hits = stats["get_hits"].get<size_t>();
    EXPECT_EQ(0, get_hits);

    // Now, send 10 GET_META for a document that does not exist, this should
    // not increase the `cmd_get` and `get_misses` stats or the `get_hits`
    // stat
    for (int i = 0; i < 10; i++) {
        auto meta =
                userConnection->getMeta("no_key", Vbid(0), GetMetaVersion::V1);
        EXPECT_EQ(cb::mcbp::Status::KeyEnoent, meta.first);
    }
    stats = userConnection->stats("");

    cmd_get = stats["cmd_get"].get<size_t>();
    EXPECT_EQ(0, cmd_get);

    auto get_misses = stats["get_misses"].get<size_t>();
    EXPECT_EQ(0, get_misses);

    get_hits = stats["get_hits"].get<size_t>();
    EXPECT_EQ(0, get_hits);
}

TEST_P(StatsTest, StatsResetIsPrivileged) {
    try {
        userConnection->stats("reset");
        FAIL() << "reset is a privileged operation";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied());
    }
}

TEST_P(StatsTest, TestReset) {
    auto stats = userConnection->stats("");
    ASSERT_FALSE(stats.empty());

    auto before = stats["cmd_get"].get<size_t>();

    for (int ii = 0; ii < 10; ++ii) {
        EXPECT_THROW(userConnection->get("foo", Vbid(0)), ConnectionError);
    }

    stats = userConnection->stats("");
    EXPECT_NE(before, stats["cmd_get"].get<size_t>());

    // the cmd_get counter does work.. now check that reset sets it back..
    resetBucket();

    stats = userConnection->stats("");
    EXPECT_EQ(0, stats["cmd_get"].get<size_t>());

    // Just ensure that the "reset timings" is detected
    // @todo add a separate test case for cmd timings stats
    adminConnection->executeInBucket(bucketName, [](auto& connection) {
        connection.stats("reset timings");
        // Just ensure that the "reset bogus" is detected..
        try {
            connection.stats("reset bogus");
            FAIL() << "stats reset bogus should throw an exception (non a "
                      "valid cmd)";
        } catch (ConnectionError& error) {
            EXPECT_TRUE(error.isInvalidArguments()) << error.what();
        }
    });
}

/**
 * MB-17815: The cmd_set stat is incremented multiple times if the underlying
 * engine returns EWOULDBLOCK (which would happen for all operations when
 * the underlying engine is operating in full eviction mode and the document
 * isn't resident)
 */
TEST_P(StatsTest, Test_MB_17815) {
    MemcachedConnection& conn = getConnection();
    conn.authenticate("Luke", mcd_env->getPassword("Luke"));
    conn.selectBucket(bucketName);
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
    doc.info.cas = cb::mcbp::cas::Wildcard;
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
    conn.authenticate("Luke", mcd_env->getPassword("Luke"));
    conn.selectBucket(bucketName);

    auto stats = conn.stats("");
    EXPECT_EQ(0, stats["cmd_set"].get<size_t>());

    // Allow first SET to succeed and then return EWOULDBLOCK for
    // the Append (2nd op).

    // Set a document
    Document doc;
    doc.info.cas = cb::mcbp::cas::Wildcard;
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
    conn.authenticate("Luke", mcd_env->getPassword("Luke"));
    conn.selectBucket(bucketName);

    auto stats = conn.stats("");
    EXPECT_EQ(0, stats["cmd_set"].get<size_t>());

    Document doc;
    doc.info.cas = cb::mcbp::cas::Wildcard;
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
    conn.authenticate("Luke", mcd_env->getPassword("Luke"));
    conn.selectBucket(bucketName);

    // Set a document
    Document doc;
    doc.info.cas = cb::mcbp::cas::Wildcard;
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
    adminConnection->executeInBucket(bucketName, [](auto& connection) {
        auto sequence = ewb::encodeSequence({cb::engine_errc::would_block,
                                             cb::engine_errc::success,
                                             ewb::Passthrough});
        connection.configureEwouldBlockEngine(EWBEngineMode::Sequence,
                                              /*unused*/ {},
                                              /*unused*/ {},
                                              sequence);
        auto stats = connection.stats("vbucket");
        EXPECT_FALSE(stats.empty());
        connection.disableEwouldBlockEngine();
    });
}

/**
 * MB-52728: Verify that the background tasks which perform budket-level STAT
 * requests correctly handle async notificaiton - prior to the fix for this MB
 * we could end up calling notifyIoComplete *twice* for a single engine API
 * call (there should only be one).
 */
TEST_P(StatsTest, MB52728_TestEWBReturnFromStatBGTask) {
    // Need any stat key which is handled at the bucket level - doesn't really
    // matter which one, as we will not actually call down to real bucket
    // for the STAT call, EWB_Engine is used instead.
    const std::string statKey = "vbucket";

    // Setup EBS engine to return:
    //    1. STAT -> would_block, notifyIoComplete(success)
    //    2. STAT -> success
    // For the two stat calls. Prior to the bugfix, this caused 2x
    // notifyIoComplete calls for the first STAT call - one from EWB_Engine
    // after it returned would_block (correct), and a second spurious one
    // from StatsTaskBucketStats::run() task.
    //
    // This manifested originally as an intermittent failure when verifying
    // the Cookie during the processing of the 2nd spurious notifyIoComplete,
    // but with the additon of more Expect()s in this patch it manifests as
    // a failure in Connection::processNotifiedCookie() checking that
    // cookie.isEwouldblock() == true.
    adminConnection->executeInBucket(bucketName, [statKey](auto& connection) {
        auto sequence = ewb::encodeSequence({cb::engine_errc::would_block,
                                             cb::engine_errc::success,
                                             cb::engine_errc::success});
        connection.configureEwouldBlockEngine(EWBEngineMode::Sequence,
                                              /*unused*/ {},
                                              /*unused*/ {},
                                              sequence);
        auto stats = connection.stats(statKey);
        connection.disableEwouldBlockEngine();
    });
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
    auto stats = adminConnection->stats("audit");
    EXPECT_EQ(2, stats.size());
    EXPECT_EQ(false, stats["enabled"].get<bool>());
    EXPECT_EQ(0, stats["dropped_events"].get<size_t>());
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
    auto stats = adminConnection->stats("bucket_details");
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
        EXPECT_EQ(6, bucket.size());
        EXPECT_NE(bucket.end(), bucket.find("index"));
        EXPECT_NE(bucket.end(), bucket.find("state"));
        EXPECT_NE(bucket.end(), bucket.find("clients"));
        EXPECT_NE(bucket.end(), bucket.find("name"));
        EXPECT_NE(bucket.end(), bucket.find("type"));
        EXPECT_NE(bucket.end(), bucket.find("num_commands"));
    }
}

TEST_P(StatsTest, TestBucketDetailsSingleBucket) {
    nlohmann::json json;
    size_t ncallback = 0;

    adminConnection->stats(
            [this, &json, &ncallback](const auto& k, const auto& v) {
                ++ncallback;
                if (!k.empty()) {
                    ASSERT_EQ(bucketName, k);
                    ASSERT_FALSE(v.empty());
                    json = nlohmann::json::parse(v);
                } else {
                    ASSERT_TRUE(v.empty());
                }
            },
            "bucket_details " + bucketName);

    EXPECT_EQ("ready", json["state"].get<std::string>());
    EXPECT_LE(1, json["clients"].get<int>());
    EXPECT_EQ(bucketName, json["name"].get<std::string>());
    EXPECT_EQ("EWouldBlock", json["type"].get<std::string>());

    auto rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::Stat,
            "bucket_details this-bucket-does-not-not-exists"});
    ASSERT_FALSE(rsp.isSuccess());
    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
}

TEST_P(StatsTest, TestTasksAllInfo) {
    auto stats = adminConnection->stats("tasks-all");
    ASSERT_NE(stats.end(), stats.find("ep_tasks:cur_time:No bucket"));

    // Set up a config only bucket too to ensure that things work with that
    // present
    const std::string config = R"({"rev":1000})";
    auto rsp = adminConnection->execute(BinprotSetClusterConfigCommand{
            token, config, 1, 1000, "cluster-config"});
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus() << std::endl
                                 << rsp.getDataJson();

    // Verify that the bucket is there
    rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::SelectBucket, bucketName});
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus();

    // Need to reset the adminConnection bucket selection to be a good
    // neighbour to the other tests in the suite.
    adminConnection->unselectBucket();

    stats = adminConnection->stats("tasks-all");
    EXPECT_NE(stats.end(), stats.find("ep_tasks:cur_time:No bucket"));
}

TEST_P(StatsTest, TestSchedulerInfo) {
    auto stats = adminConnection->stats("worker_thread_info");
    // We should at least have an entry for the first thread
    EXPECT_NE(stats.end(), stats.find("0"));
}

TEST_P(StatsTest, TestSchedulerInfo_Aggregate) {
    auto stats = adminConnection->stats("worker_thread_info aggregate");
    EXPECT_NE(stats.end(), stats.find("aggregate"));
}

TEST_P(StatsTest, TestSchedulerInfo_InvalidSubcommand) {
    try {
        adminConnection->stats("worker_thread_info foo");
        FAIL() << "Invalid subcommand";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments());
    }
}

TEST_P(StatsTest, TestAggregate) {
    auto stats = userConnection->stats("aggregate");
    // Don't expect the entire stats set, but we should at least have
    // the uptime
    EXPECT_NE(stats.end(), stats.find("uptime"));
}

TEST_P(StatsTest, TestPrivilegedConnections) {
    adminConnection->setAgentName("TestPrivilegedConnections 1.0");
    adminConnection->setFeature(cb::mcbp::Feature::XERROR, true);
    auto stats = adminConnection->stats("connections");
    // We have at _least_ 2 connections
    ASSERT_LE(2, stats.size());

    stats = adminConnection->stats("connections self");
    ASSERT_EQ(1, stats.size());
    EXPECT_EQ("TestPrivilegedConnections 1.0",
              stats.front()["agent_name"].get<std::string>());

    stats = adminConnection->stats(
            "connections " +
            std::to_string(stats.front()["socket"].get<size_t>()));
    ASSERT_EQ(1, stats.size());
    EXPECT_EQ("TestPrivilegedConnections 1.0",
              stats.front()["agent_name"].get<std::string>());
}

TEST_P(StatsTest, TestUnprivilegedConnections) {
    // Everyone should be allowed to see its own connection details
    MemcachedConnection& conn = getConnection();
    conn.authenticate("Luke", mcd_env->getPassword("Luke"));
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
    EXPECT_NE(0, stats.front()["total_recv"].get<uint64_t>());
    const auto total_recv = stats.front()["total_recv"].get<uint64_t>();
    stats = conn.stats("connections self");
    ASSERT_EQ(1, stats.size());
    EXPECT_EQ(me, stats.front()["socket"].get<size_t>());
    EXPECT_EQ("TestUnprivilegedConnections 1.0",
              stats.front()["agent_name"].get<std::string>());
    EXPECT_EQ(me, conn.getServerConnectionId());

    // Verify that total_recv is updated
    EXPECT_LT(total_recv, stats.front()["total_recv"].get<uint64_t>());

#ifdef __linux__
    EXPECT_EQ(0, stats.front()["SIOCINQ"]);
    EXPECT_EQ(0, stats.front()["SIOCOUTQ"]);
    EXPECT_NE(0, stats.front()["SNDBUF"]);
    EXPECT_NE(0, stats.front()["RCVBUF"]);
#endif
}

TEST_P(StatsTest, TestUnprivilegedConnectionsWithSpecificFd) {
    auto conn1 = getConnection().clone();
    conn1->authenticate("Luke", mcd_env->getPassword("Luke"));
    conn1->setFeatures({cb::mcbp::Feature::XERROR});
    auto stats1 = conn1->stats("connections");
    ASSERT_EQ(1, stats1.size());

    auto conn2 = conn1->clone();
    conn2->authenticate("Luke", mcd_env->getPassword("Luke"));
    conn2->setFeatures({cb::mcbp::Feature::XERROR});
    auto stats2 = conn2->stats("connections");
    ASSERT_EQ(1, stats2.size());

    auto conn1sock = stats1.front()["socket"].get<size_t>();
    auto conn2sock = stats2.front()["socket"].get<size_t>();
    EXPECT_NE(conn1sock, conn2sock);

    // verify that I can request my own stat by providing my own socket
    stats1 = conn1->stats("connections " + std::to_string(conn1sock));
    ASSERT_EQ(1, stats1.size());
    EXPECT_EQ(conn1sock, stats1.front()["socket"].get<size_t>());

    // verify that an unprivileged connection can't get another connection
    // stat
    auto rsp = conn1->execute(
            BinprotGenericCommand(cb::mcbp::ClientOpcode::Stat,
                                  "connections " + std::to_string(conn2sock)));
    EXPECT_EQ(cb::mcbp::Status::Eaccess, rsp.getStatus());
}

TEST_P(StatsTest, TestConnectionsInvalidNumber) {
    try {
        auto stats = adminConnection->stats("connections xxx");
        FAIL() << "Did not detect incorrect connection number";

    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments());
    }
}

TEST_P(StatsTest, TestSubdocExecute) {
    auto stats = userConnection->stats("subdoc_execute");

    // json returned should have zero samples as no ops have been performed
    EXPECT_TRUE(stats.is_object());
    EXPECT_EQ(0, stats["0"]["total"].get<uint64_t>());
}

TEST_P(StatsTest, TestResponseStats) {
    int successCount = getResponseCount(cb::mcbp::Status::Success);
    // 2 successes expected:
    // 1. The previous stats call sending the JSON
    // 2. The previous stats call sending a null packet to mark end of stats
    EXPECT_EQ(successCount + 1, getResponseCount(cb::mcbp::Status::Success));
}

TEST_P(StatsTest, TracingStatsIsPrivileged) {
    MemcachedConnection& conn = getConnection();
    try {
        conn.stats("tracing");
        FAIL() << "tracing is a privileged operation";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied());
    }
}

TEST_P(StatsTest, TestTracingStats) {
    auto stats = adminConnection->stats("tracing");
    // Just check that we got some stats, no need to check all of them
    // as we don't want memcached to be testing phosphor's logic
    EXPECT_FALSE(stats.empty());
    auto enabled = stats.find("log_is_enabled");
    EXPECT_NE(stats.end(), enabled);
}

TEST_P(StatsTest, TestSingleBucketOpStats) {
    std::string key = "key";

    // Set a document
    Document doc;
    doc.info.cas = cb::mcbp::cas::Wildcard;
    doc.info.flags = 0xcaffee;
    doc.info.id = key;
    doc.value = "asdf";

    // mutate to bump stat
    userConnection->mutate(doc, Vbid(0), MutationType::Set);
    // lookup to bump stat
    userConnection->get(key, Vbid(0));

    auto stats = userConnection->stats("");

    EXPECT_FALSE(stats.empty());

    auto lookup = stats.find("cmd_lookup");
    auto mutation = stats.find("cmd_mutation");

    ASSERT_NE(stats.end(), lookup);
    ASSERT_NE(stats.end(), mutation);

    EXPECT_EQ(1, int(*lookup));
    EXPECT_EQ(1, int(*mutation));
}

/// Test that all stats which should be present in the "clocks" group are,
/// and they have values of the correct type.
TEST_P(StatsTest, TestClocksStats) {
    auto stats = userConnection->stats("clocks");

    std::array<std::string_view, 3> keys{{"clock_fine_overhead_ns"sv,
                                          "clock_coarse_overhead_ns"sv,
                                          "clock_measurement_period_ns"sv}};
    for (auto key : keys) {
        auto it = stats.find(key);
        EXPECT_NE(stats.end(), it) << "Expected key '" << key << "' not found.";
        uint64_t value;
        try {
            value = it->get<uint64_t>();
        } catch (const std::exception& e) {
            FAIL() << "Caught exception when converting value for key'" << key
                   << "' to uint64_t:" << e.what();
        }
        // Just check that numeric value is non-zero for all stats.
        EXPECT_NE(0, value);

        // Remove each found item, allowing us to check for no extra stat
        // keys at the end.
        stats.erase(it);
    }

    // Check for any unexpected extra stats.
    for (auto& [key, value] : stats.items()) {
        ADD_FAILURE() << "Unexpected stat in 'clocks' group: '" << key << "': '"
                      << value << "'";
    }
}

TEST_P(StatsTest, TestSettingAndGettingThreadCount) {
    nlohmann::json cfg;
    const uint32_t newNumThreads = 10;
    cfg["num_reader_threads"] = newNumThreads;
    cfg["num_writer_threads"] = newNumThreads;
    cfg["num_auxio_threads"] = newNumThreads;
    cfg["num_nonio_threads"] = newNumThreads;

    EXPECT_TRUE(reconfigure(cfg).isSuccess());

    std::vector<std::pair<std::string, std::string>> stats;
    adminConnection->stats(
            [&stats](const std::string& key, const std::string& value) -> void {
                stats.emplace_back(key, value);
            },
            "threads");
    bool seenThreadsKey = false;
    for (const auto& [key, value] : stats) {
        if (key == "num_frontend_threads") {
            seenThreadsKey = true;
            EXPECT_GT(std::stol(value), 0);
            continue;
        }
        EXPECT_EQ(newNumThreads, std::stol(value)) << "Stat key:" << key;
    }
    EXPECT_TRUE(seenThreadsKey);
}
