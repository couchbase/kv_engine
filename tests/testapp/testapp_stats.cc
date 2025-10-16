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
#include <fmt/format.h>
#include <folly/portability/GMock.h>
#include <memcached/stat_group.h>
#include <platform/dirutils.h>
#include <protocol/mcbp/ewb_encode.h>
#include <utilities/timing_histogram_printer.h>

using namespace std::string_view_literals;

class StatsTest : public TestappClientTest {
public:
    static void SetUpTestCase() {
        try {
            const auto path = std::filesystem::path(SOURCE_ROOT) /
                              "statistics" / "stat_definitions.json";
            stats_definitions = nlohmann::json::parse(
                    cb::io::loadFile(path), nullptr, true, true);
        } catch (const std::exception& e) {
            fmt::print(
                    stderr, "Failed to load stat definitions: {}\n", e.what());
            std::exit(EXIT_FAILURE);
        }

        TestappClientTest::SetUpTestCase();
    }

protected:
    void SetUp() override {
        TestappClientTest::SetUp();

        auto validator = [this](const auto& header) { validateStat(header); };
        userConnection->setUserValidateReceivedFrameCallback(validator);
        adminConnection->setUserValidateReceivedFrameCallback(validator);
    }

    void TearDown() override {
        TestappClientTest::SetUp();
        userConnection->setUserValidateReceivedFrameCallback({});
        adminConnection->setUserValidateReceivedFrameCallback({});
    }

private:
    void validateStat(const cb::mcbp::Header& header) {
        using namespace cb::mcbp;
        if (is_server_magic(Magic(header.getMagic())) || header.isRequest()) {
            return;
        }
        const auto& res = header.getResponse();
        auto key = res.getKeyString();
        if (res.getClientOpcode() != ClientOpcode::Stat || key.empty()) {
            return;
        }

        // 1. we should be able to locate the key
        // 2. we should be able to locate the description for the key
        if (key.starts_with("ep_") || key.starts_with("vb_") ||
            key == "mem_used_primary" || key == "mem_used_secondary") {
            // ignore for now. ep-engine may have its own tests
            return;
        }

        bool found = false;
        std::string descr;

        for (const auto& obj : stats_definitions) {
            // Ignore family descriptions from stats_definitions.json
            if (!obj.contains("key")) {
                continue;
            }
            if (obj["key"].get<std::string>() == key) {
                found = true;
            } else {
                auto iter = obj.find("cbstat");
                if (iter != obj.end() && iter->is_string()) {
                    auto v = iter->get<std::string>();
                    if (v.empty()) {
                        std::cout << obj.dump(2) << std::endl;
                    }
                    for (const auto& macro :
                         {"{tid}", ":{name}", "{stat_key}", "{sdk}"}) {
                        auto idx = v.find(macro);
                        if (idx != std::string::npos) {
                            v.resize(idx);
                        }
                    }
                    if (key.starts_with(v)) {
                        found = true;
                    }
                }
            }
            if (found) {
                descr = obj.value("description", "");
                break;
            }
        }

        if (found) {
            if (descr.empty()) {
                FAIL() << fmt::format(
                        "Missing description for stat key [{}] with value [{}]",
                        key,
                        res.getValueString());
            }
        } else {
            FAIL() << fmt::format(
                    "Missing entry for stat key [{}] with value [{}]",
                    key,
                    res.getValueString());
        }
    }

    static nlohmann::json stats_definitions;
};

nlohmann::json StatsTest::stats_definitions;

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

    auto validatePrivilegedStats = [](bool privileged, nlohmann::json json) {
        for (const auto& field : {"daemon_connections",
                                  "curr_connections",
                                  "system_connections",
                                  "user_connections",
                                  "max_user_connections",
                                  "max_system_connections",
                                  "total_connections",
                                  "connection_structures",
                                  "cmd_total_sets",
                                  "cmd_total_gets",
                                  "cmd_total_ops",
                                  "rejected_conns",
                                  "threads",
                                  "cmd_lookup_10s_count",
                                  "cmd_lookup_10s_duration_us",
                                  "cmd_mutation_10s_count",
                                  "cmd_mutation_10s_duration_us"}) {
            if (privileged) {
                EXPECT_NE(json.end(), json.find(field)) << field;
            } else {
                EXPECT_EQ(json.end(), json.find(field)) << field;
            }
        }
    };

    validatePrivilegedStats(false, stats);

    auto conn = userConnection->clone();
    conn->setAgentName("cb-internal/" PRODUCT_VERSION);
    conn->setFeature(cb::mcbp::Feature::XERROR, true);
    conn->authenticate("Luke");

    adminConnection->executeInBucket(
            bucketName, [&validatePrivilegedStats](auto& conn) {
                std::string_view keyname =
                        "current_sdk_connections:cb-internal/" PRODUCT_VERSION;
                const auto stats = conn.stats("");
                validatePrivilegedStats(true, stats);
                EXPECT_TRUE(stats.contains(keyname));
            });
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
        userConnection->getMeta(doc.info.id, Vbid(0), GetMetaVersion::V1);
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
        auto rsp = userConnection->execute(
                BinprotGetMetaCommand{"no_key", Vbid(0), GetMetaVersion::V1});
        EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    }
    stats = userConnection->stats("");

    cmd_get = stats["cmd_get"].get<size_t>();
    EXPECT_EQ(0, cmd_get);

    auto get_misses = stats["get_misses"].get<size_t>();
    EXPECT_EQ(0, get_misses);

    get_hits = stats["get_hits"].get<size_t>();
    EXPECT_EQ(0, get_hits);
}

TEST_P(StatsTest, TestReset) {
    const auto before = get_cmd_counter("cmd_get");
    for (int ii = 0; ii < 10; ++ii) {
        EXPECT_THROW(userConnection->get("foo", Vbid(0)), ConnectionError);
    }
    EXPECT_EQ(before + 10, get_cmd_counter("cmd_get"));

    // The cmd_get counter work. Verify that reset set it to 0
    adminConnection->executeInBucket(
            bucketName, [](auto& connection) { connection.stats("reset"); });

    EXPECT_EQ(0, get_cmd_counter("cmd_get"));

    // Just ensure that the "reset timings" is detected
    // @todo add a separate test case for cmd timings stats
    adminConnection->executeInBucket(bucketName, [](auto& connection) {
        connection.stats("reset timings");
        // Just ensure that the "reset bogus" is detected...
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
    if (GetTestBucket().isFullEviction()) {
        GTEST_SKIP();
    }
    const auto cmd_set_before = get_cmd_counter("cmd_set");
    auto sequence = ewb::encodeSequence({cb::engine_errc::would_block,
                                         cb::engine_errc::success,
                                         ewb::Passthrough,
                                         cb::engine_errc::would_block,
                                         cb::engine_errc::success,
                                         ewb::Passthrough});
    userConnection->configureEwouldBlockEngine(EWBEngineMode::Sequence,
                                               /*unused*/ {},
                                               /*unused*/ {},
                                               sequence);

    Document doc;
    doc.info.cas = cb::mcbp::cas::Wildcard;
    doc.info.flags = 0xcaffee;
    doc.info.id = name;
    doc.value = "value";

    userConnection->mutate(doc, Vbid(0), MutationType::Add);
    userConnection->disableEwouldBlockEngine();

    EXPECT_EQ(cmd_set_before + 1, get_cmd_counter("cmd_set"));
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
    const auto cmd_set_before = get_cmd_counter("cmd_set");

    // Allow first SET to succeed and then return EWOULDBLOCK for
    // the Append (2nd op).

    // Set a document
    Document doc;
    doc.info.cas = cb::mcbp::cas::Wildcard;
    doc.info.flags = 0xcaffee;
    doc.info.id = name;
    doc.value = memcached_cfg.dump();
    userConnection->mutate(doc, Vbid(0), MutationType::Set);

    // bucket_get -> Passthrough,
    // bucket_allocate -> Passthrough,
    // bucket_CAS -> EWOULDBLOCK (success)
    // bucket_CAS (retry) -> Passthrough
    auto sequence = ewb::encodeSequence({ewb::Passthrough,
                                         ewb::Passthrough,
                                         cb::engine_errc::would_block,
                                         cb::engine_errc::success,
                                         ewb::Passthrough});
    userConnection->configureEwouldBlockEngine(EWBEngineMode::Sequence,
                                               /*unused*/ {},
                                               /*unused*/ {},
                                               sequence);

    // Now append to the same doc
    userConnection->mutate(doc, Vbid(0), MutationType::Append);
    userConnection->disableEwouldBlockEngine();

    EXPECT_EQ(cmd_set_before + 2, get_cmd_counter("cmd_set"));
}

/**
 * Verify that cmd_set is updated when we fail to perform the
 * append operation.
 */
TEST_P(StatsTest, Test_MB_29259_Append) {
    const auto cmd_set_before = get_cmd_counter("cmd_set");

    Document doc;
    doc.info.cas = cb::mcbp::cas::Wildcard;
    doc.info.flags = 0xcaffee;
    doc.info.id = name;
    doc.value = memcached_cfg.dump();

    // Try to append to a non-existing document
    try {
        userConnection->mutate(doc, Vbid(0), MutationType::Append);
        FAIL() << "Append on non-existing document should fail";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isNotStored());
    }

    EXPECT_EQ(cmd_set_before + 1, get_cmd_counter("cmd_set"));
}

TEST_P(StatsTest, TestAppend) {
    const auto cmd_set_before = get_cmd_counter("cmd_set");

    // Set a document
    Document doc;
    doc.info.cas = cb::mcbp::cas::Wildcard;
    doc.info.flags = 0xcaffee;
    doc.info.id = name;
    doc.value = memcached_cfg.dump();
    userConnection->mutate(doc, Vbid(0), MutationType::Set);

    // Send 10 appends, this should increase the `cmd_set` stat by 10
    for (int i = 0; i < 10; i++) {
        userConnection->mutate(doc, Vbid(0), MutationType::Append);
    }

    // In total, we expect 11 sets, since there was the initial set
    // and then 10 appends
    EXPECT_EQ(cmd_set_before + 11, get_cmd_counter("cmd_set"));
}

/// Verify that we don't keep invalid pointers around when the packet is
/// relocated as part of EWB
TEST_P(StatsTest, MB37147_TestEWBReturnFromStat) {
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

TEST_P(StatsTest, TestAudit) {
    auto stats = adminConnection->stats("audit");
    EXPECT_EQ(2, stats.size());
    EXPECT_EQ(false, stats["enabled"].get<bool>());
    EXPECT_EQ(0, stats["dropped_events"].get<size_t>());
}

TEST_P(StatsTest, TestStatTimings) {
    auto stats = userConnection->stats("stat-timings");
    if (stats.empty()) {
        stats = userConnection->stats("stat-timings");
    }
    EXPECT_FALSE(stats.empty())
            << "We should at least have timings for stat-timings";
}

TEST_P(StatsTest, UnprivilegedUserCantGetPrivilegedStats) {
    // Verify that all of the stat groups listed as privileged fails
    // with eaccess
    StatsGroupManager::getInstance().iterate([](const auto& group) {
        if (group.privileged) {
            const auto rsp = userConnection->execute(BinprotGenericCommand{
                    cb::mcbp::ClientOpcode::Stat, std::string{group.key}});
            EXPECT_EQ(cb::mcbp::Status::Eaccess, rsp.getStatus())
                    << "Failed for key: " << group.key;
        }
    });
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
        EXPECT_EQ(16, bucket.size());
        EXPECT_NE(bucket.end(), bucket.find("index"));
        EXPECT_NE(bucket.end(), bucket.find("state"));
        EXPECT_NE(bucket.end(), bucket.find("clients"));
        EXPECT_NE(bucket.end(), bucket.find("name"));
        EXPECT_NE(bucket.end(), bucket.find("type"));
        EXPECT_NE(bucket.end(), bucket.find("num_rejected"));
        EXPECT_NE(bucket.end(), bucket.find("ru"));
        EXPECT_NE(bucket.end(), bucket.find("wu"));
        EXPECT_NE(bucket.end(), bucket.find("num_throttled"));
        EXPECT_NE(bucket.end(), bucket.find("throttle_reserved"));
        EXPECT_NE(bucket.end(), bucket.find("throttle_hard_limit"));
        EXPECT_NE(bucket.end(), bucket.find("throttle_wait_time"));
        EXPECT_NE(bucket.end(), bucket.find("num_commands_with_metered_units"));
        EXPECT_NE(bucket.end(), bucket.find("num_metered_dcp_messages"));
        EXPECT_NE(bucket.end(), bucket.find("num_commands"));
        EXPECT_EQ("Success", bucket["data_ingress_status"]);
    }
}

TEST_P(StatsTest, TestBucketDetailsSingleBucket) {
    nlohmann::json json;
    size_t ncallback = 0;

    adminConnection->setUserValidateReceivedFrameCallback({});
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
    auto rsp = adminConnection->execute(
            BinprotSetClusterConfigCommand{config, 1, 1000, "cluster-config"});
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
    int found = 0;
    adminConnection->stats(
            [&found](const auto& k, const auto& v) {
                TimingHistogramPrinter tp(nlohmann::json::parse(v));
                ++found;
            },
            "worker_thread_info");
    EXPECT_GT(4, found);
}

TEST_P(StatsTest, TestSchedulerInfo_Aggregate) {
    int found = 0;
    adminConnection->stats(
            [&found](const auto& k, const auto& v) {
                TimingHistogramPrinter tp(nlohmann::json::parse(v));
                ++found;
            },
            "worker_thread_info aggregate");
    EXPECT_EQ(1, found);
}

TEST_P(StatsTest, TestSchedulerInfo_InvalidSubcommand) {
    const auto rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::Stat, "worker_thread_info foo"});
    EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
}

TEST_P(StatsTest, TestPrivilegedConnections) {
    adminConnection->setAgentName("TestPrivilegedConnections 1.0");
    adminConnection->setFeature(cb::mcbp::Feature::XERROR, true);
    adminConnection->setUserValidateReceivedFrameCallback({});

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
    conn.authenticate("Luke");
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
    conn1->authenticate("Luke");
    conn1->setFeatures({cb::mcbp::Feature::XERROR});
    auto stats1 = conn1->stats("connections");
    ASSERT_EQ(1, stats1.size());

    auto conn2 = conn1->clone();
    conn2->authenticate("Luke");
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
    const auto rsp = userConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::Stat, "connections xxx"});
    EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus())
            << "Did not detect incorrect connection number";
}

TEST_P(StatsTest, TestSubdocExecute) {
    int found = 0;
    adminConnection->stats(
            [&found](const auto& k, const auto& v) {
                TimingHistogramPrinter tp(nlohmann::json::parse(v));
                ++found;
            },
            "subdoc_execute");
    EXPECT_EQ(1, found);
}

TEST_P(StatsTest, TestResponseStats) {
    int successCount = getResponseCount(cb::mcbp::Status::Success);
    // 2 successes expected:
    // 1. The previous stats call sending the JSON
    // 2. The previous stats call sending a null packet to mark end of stats
    EXPECT_EQ(successCount + 1, getResponseCount(cb::mcbp::Status::Success));
}

TEST_P(StatsTest, TestTracingStats) {
    auto stats = adminConnection->stats("tracing");
    // Just check that we got some stats, no need to check all of them
    // as we don't want memcached to be testing phosphor's logic
    EXPECT_FALSE(stats.empty());
    auto enabled = stats.find("log_is_enabled");
    EXPECT_NE(stats.end(), enabled);
}

#if defined(HAVE_JEMALLOC)
TEST_P(StatsTest, TestAllocatorStats) {
    auto stats =
            adminConnection->stats("allocator")["allocator"].get<std::string>();
    EXPECT_EQ(0, stats.find("___ Begin jemalloc statistics ___"));
    EXPECT_NE(std::string::npos, stats.find("--- End jemalloc statistics ---"));
}
#endif

/**
 * Check the format of the "frequency-counters" histograms.
 * We should have one histogram with 256 buckets for each vb state.
 */
TEST_P(StatsTest, TestFrequencyCountersStats) {
    using namespace std::string_view_literals;

    auto stats = userConnection->stats("frequency-counters");
    EXPECT_FALSE(stats.empty());

    // We expect 257 histogram buckets per vbucket state (256 buckets + mean)
    constexpr auto keys = 257 * 3;
    EXPECT_EQ(keys, stats.size());

    for (auto state : {"active", "replica", "pending"}) {
        for (int i = 0; i < 256; i++) {
            auto bucket =
                    fmt::format("vb_{}_evictable_mfu_{},{}", state, i, i + 1);
            EXPECT_TRUE(stats.contains(bucket));
        }
        auto mean = fmt::format("vb_{}_evictable_mfu_mean", state);
        EXPECT_TRUE(stats.contains(mean));
    }
}

TEST_P(StatsTest, TestSingleBucketOpStats) {
    std::string key = "key";

    // Set a document
    Document doc;
    doc.info.cas = cb::mcbp::cas::Wildcard;
    doc.info.flags = 0xcaffee;
    doc.info.id = key;
    doc.value = "asdf";

    const auto cmd_lookup_before = get_cmd_counter("cmd_lookup");
    const auto cmd_mutation_before = get_cmd_counter("cmd_mutation");

    // mutate to bump stat
    userConnection->mutate(doc, Vbid(0), MutationType::Set);
    // lookup to bump stat
    userConnection->get(key, Vbid(0));

    EXPECT_EQ(cmd_lookup_before + 1, get_cmd_counter("cmd_lookup"));
    EXPECT_EQ(cmd_mutation_before + 1, get_cmd_counter("cmd_mutation"));
}

/// Test that all stats which should be present in the "clocks" group are,
/// and they have values of the correct type.
TEST_P(StatsTest, TestClocksStats) {
    auto stats = userConnection->stats("clocks");

    std::array keys{"clock_fine_overhead_ns"sv,
                    "clock_fine_resolution_ns"sv,
                    "clock_coarse_overhead_ns"sv,
                    "clock_coarse_resolution_ns"sv,
                    "clock_measurement_period_ns"sv};
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
    using namespace ::testing;

    auto getThreadStats = [&]() {
        std::vector<std::pair<std::string, int>> stats;
        adminConnection->stats(
                [&stats](auto key, auto value) {
                    stats.emplace_back(key, std::stoi(value));
                },
                "threads");
        return stats;
    };

    // 1. Check the default values of configured and actual threads.
    EXPECT_THAT(
            getThreadStats(),
            UnorderedElementsAre(
                    // Frontend threads don't support symbolic values,
                    // so just check they are both non-zero.
                    Pair("num_frontend_threads_configured", Gt(0)),
                    Pair("num_frontend_threads_actual", Gt(0)),
                    // background threads by default are configured as
                    // "default", which is encoded as zero, or -1 for AuxIO /
                    // NonIO.
                    // Created should be non-zero however (based on CPU count).
                    Pair("num_reader_threads_configured", 0),
                    Pair("num_reader_threads_actual", Gt(0)),
                    Pair("num_writer_threads_configured", 0),
                    Pair("num_writer_threads_actual", Gt(0)),
                    Pair("num_auxio_threads_configured", -1),
                    Pair("num_auxio_threads_actual", Gt(0)),
                    Pair("num_nonio_threads_configured", -1),
                    Pair("num_nonio_threads_actual", Gt(0))));

    // 2. Reconfigure with a different number, check the stats update as
    // expected.
    nlohmann::json cfg;
    constexpr uint32_t newNumThreads = 10;
    cfg["num_reader_threads"] = newNumThreads;
    cfg["num_writer_threads"] = newNumThreads;
    cfg["num_auxio_threads"] = newNumThreads;
    cfg["num_nonio_threads"] = newNumThreads;

    EXPECT_TRUE(reconfigure(cfg).isSuccess());

    EXPECT_THAT(
            getThreadStats(),
            UnorderedElementsAre(
                    // We cannot change frontend threads at runtime, so just
                    // check those keys are present and have a non-zero value.
                    Pair("num_frontend_threads_configured", _),
                    Pair("num_frontend_threads_actual", _),
                    // background threads can be reconfigured, so should all
                    // have value of newNumThreads
                    Pair("num_reader_threads_configured", newNumThreads),
                    Pair("num_reader_threads_actual", newNumThreads),
                    Pair("num_writer_threads_configured", newNumThreads),
                    Pair("num_writer_threads_actual", newNumThreads),
                    Pair("num_auxio_threads_configured", newNumThreads),
                    Pair("num_auxio_threads_actual", newNumThreads),
                    Pair("num_nonio_threads_configured", newNumThreads),
                    Pair("num_nonio_threads_actual", newNumThreads)));
}

TEST_P(StatsTest, ThreadDetails) {
    nlohmann::json json;

    adminConnection->stats(
            [&json](const auto&, const auto& value) -> void {
                json = nlohmann::json::parse(value);
            },
            "threads details");
    EXPECT_FALSE(json.empty()) << "Expected a JSON payload to be returned";
    EXPECT_TRUE(json.contains("num_auxio_threads_configured"));
    EXPECT_TRUE(json.contains("num_auxio_threads_actual"));
    EXPECT_TRUE(json.contains("num_frontend_threads_configured"));
    EXPECT_TRUE(json.contains("num_frontend_threads_actual"));
    EXPECT_TRUE(json.contains("num_nonio_threads_configured"));
    EXPECT_TRUE(json.contains("num_nonio_threads_actual"));
    EXPECT_TRUE(json.contains("num_reader_threads_configured"));
    EXPECT_TRUE(json.contains("num_reader_threads_actual"));
    EXPECT_TRUE(json.contains("num_writer_threads_configured"));
    EXPECT_TRUE(json.contains("num_writer_threads_actual"));
    EXPECT_LT(10, json.size()) << "There should be some threads reported";
}

/// The stats should contain max user and system connections to allow
/// for alerting by monitoring the current levels with the max
TEST_P(StatsTest, MB58199) {
    nlohmann::json stats;
    adminConnection->executeInBucket(
            bucketName, [&stats](auto& conn) { stats = conn.stats(""); });
    ASSERT_TRUE(stats.contains("max_system_connections"));
    ASSERT_TRUE(stats.contains("max_user_connections"));
    constexpr auto max_system = Testapp::MAX_CONNECTIONS / 4;
    constexpr auto max_user = Testapp::MAX_CONNECTIONS - max_system;
    EXPECT_EQ(max_system, stats["max_system_connections"]);
    EXPECT_EQ(max_user, stats["max_user_connections"]);
}

// TEst fixture which doesn't use the validator
// This makes it easier to call things like connections which don't work with
// the validator
class StatsTestNoValidator : public TestappClientTest {
public:
    static void SetUpTestCase() {
        TestappClientTest::SetUpTestCase();
    }

protected:
    void SetUp() override {
        TestappClientTest::SetUp();
    }

    void TearDown() override {
        TestappClientTest::SetUp();
    }
};

TEST_P(StatsTestNoValidator, MB68697_json) {
    // Do a RAW hello with a junked JSON string
    BinprotHelloCommand cmd("{\"a\":\"8\xf8\"}");
    const auto resp = BinprotHelloResponse(adminConnection->execute(cmd));
    ASSERT_TRUE(resp.isSuccess());

    auto stats = adminConnection->stats("connections");
    // We have at _least_ 2 connections
    ASSERT_LE(2, stats.size());

    stats = adminConnection->stats("connections self");
    ASSERT_EQ(1, stats.size());
    // See sanitised agent name
    EXPECT_EQ(R"({"a":"8."})", stats.front()["agent_name"].get<std::string>());
}

TEST_P(StatsTestNoValidator, MB68697_raw) {
    // Do a RAW hello with a junked string that later cannot be turned into
    // JSON
    BinprotHelloCommand cmd("\"a\":\"8\xf8\"");
    const auto resp = BinprotHelloResponse(adminConnection->execute(cmd));
    ASSERT_TRUE(resp.isSuccess());

    auto stats = adminConnection->stats("connections");
    // We have at _least_ 2 connections
    ASSERT_LE(2, stats.size());

    stats = adminConnection->stats("connections self");
    ASSERT_EQ(1, stats.size());
    // See sanitised agent name
    EXPECT_EQ(R"("a":"8.")", stats.front()["agent_name"].get<std::string>());
}

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         StatsTestNoValidator,
                         ::testing::Values(TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());