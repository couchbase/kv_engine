/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp_xattr.h"
#include <thread>

class ClusterConfigTest : public TestappXattrClientTest {
protected:
    void SetUp() override {
        TestappXattrClientTest::SetUp();
        // Make sure we've specified a session token
        setClusterSessionToken(0xdeadbeef);

        protocol_binary_datatype_t expected = 0;
        if (hasSnappySupport() == ClientSnappySupport::Everywhere) {
            expected |= PROTOCOL_BINARY_DATATYPE_SNAPPY;
        }
        if (hasJSONSupport() == ClientJSONSupport::Yes) {
            expected |= PROTOCOL_BINARY_DATATYPE_JSON;
        }
        expectedDatatype = cb::mcbp::Datatype{expected};
    }

    void TearDown() override {
        TestappXattrClientTest::TearDown();
        userConnection.reset();
    }

    BinprotResponse setClusterConfig(uint64_t token,
                                     const std::string& config,
                                     int64_t revision) {
        return adminConnection->execute(BinprotSetClusterConfigCommand{
                token, config, 1, revision, bucketName});
    }

    void test_MB_17506(bool dedupe, bool client_setting);

    void test_CccpPushNotification(bool global, bool brief);

    std::string getInflatedValue(protocol_binary_datatype_t datatype,
                                 std::string_view data) {
        if (!cb::mcbp::datatype::is_snappy(datatype)) {
            return std::string{data};
        }
        cb::compression::Buffer buffer;
        if (!cb::compression::inflate(
                    cb::compression::Algorithm::Snappy, data, buffer)) {
            std::abort();
        }
        std::string ret{buffer.data(), buffer.size()};
        return ret;
    };

    cb::mcbp::Datatype expectedDatatype = cb::mcbp::Datatype::Raw;
};

INSTANTIATE_TEST_SUITE_P(
        TransportProtocols,
        ClusterConfigTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpSsl),
                           ::testing::Values(XattrSupport::Yes,
                                             XattrSupport::No),
                           ::testing::Values(ClientJSONSupport::Yes,
                                             ClientJSONSupport::No),
                           ::testing::Values(ClientSnappySupport::No,
                                             ClientSnappySupport::Yes,
                                             ClientSnappySupport::Everywhere)),
        PrintToStringCombinedName());

void ClusterConfigTest::test_MB_17506(bool dedupe, bool client_setting) {
    // First set the correct deduplication mode
    memcached_cfg["dedupe_nmvb_maps"] = dedupe;
    reconfigure();

    userConnection->setFeature(cb::mcbp::Feature::DedupeNotMyVbucketClustermap,
                               client_setting);

    const std::string clustermap{R"({"rev":100})"};

    // Make sure we have a cluster configuration installed
    auto response = setClusterConfig(token, clustermap, 100);
    EXPECT_TRUE(response.isSuccess());

    BinprotGetCommand command{"foo", Vbid{1}};

    // Execute the first get command. This one should _ALWAYS_ contain a map
    response = userConnection->execute(command);

    ASSERT_FALSE(response.isSuccess());
    ASSERT_EQ(cb::mcbp::Status::NotMyVbucket, response.getStatus());
    EXPECT_EQ(expectedDatatype, cb::mcbp::Datatype(response.getDatatype()));
    EXPECT_EQ(clustermap,
              getInflatedValue(response.getDatatype(), response.getDataView()));

    // Execute it one more time..
    response = userConnection->execute(command);

    ASSERT_FALSE(response.isSuccess());
    ASSERT_EQ(cb::mcbp::Status::NotMyVbucket, response.getStatus());

    if (dedupe || client_setting) {
        EXPECT_EQ(cb::mcbp::Datatype::Raw,
                  cb::mcbp::Datatype{response.getDatatype()});
        EXPECT_TRUE(response.getDataString().empty())
                << "Expected an empty stream, got ["
                << getInflatedValue(response.getDatatype(),
                                    response.getDataView())
                << "]";
    } else {
        EXPECT_EQ(expectedDatatype, cb::mcbp::Datatype(response.getDatatype()));
        EXPECT_EQ(clustermap,
                  getInflatedValue(response.getDatatype(),
                                   response.getDataView()));
    }
}

TEST_P(ClusterConfigTest, SetClusterConfigWithIncorrectSessionToken) {
    auto response = setClusterConfig(0xcafebeef, R"({"rev":100})", 100);
    EXPECT_FALSE(response.isSuccess()) << "Should not be allowed to set "
                                          "cluster config with invalid session "
                                          "token";
    EXPECT_EQ(cb::mcbp::Status::KeyEexists, response.getStatus());
}

TEST_P(ClusterConfigTest, SetClusterConfigWithCorrectToken) {
    auto response = setClusterConfig(token, R"({"rev":100})", 100);
    EXPECT_TRUE(response.isSuccess()) << "Should be allowed to set cluster "
                                         "config with the correct session "
                                         "token";
}

TEST_P(ClusterConfigTest, GetClusterConfig) {
    const std::string config{R"({"rev":100})"};
    ASSERT_TRUE(setClusterConfig(token, config, 100).isSuccess());

    BinprotGenericCommand cmd{cb::mcbp::ClientOpcode::GetClusterConfig};
    const auto response = userConnection->execute(cmd);
    EXPECT_TRUE(response.isSuccess()) << to_string(response.getStatus());
    EXPECT_EQ(expectedDatatype, cb::mcbp::Datatype(response.getDatatype()));
    EXPECT_EQ(config,
              getInflatedValue(response.getDatatype(), response.getDataView()));
}

TEST_P(ClusterConfigTest, GetClusterConfig_ClusterCompat) {
    // MB-51612: Test that GetClusterConfig works in a mixed version cluster

    // set a config with an epoch of -1 - used by ns_server when in a mixed
    // version cluster.
    int64_t epoch = -1;
    const std::string config{R"({"rev":100})"};
    ASSERT_TRUE(
            adminConnection
                    ->execute(BinprotSetClusterConfigCommand{
                            token, config, epoch, 100 /*revision */, "default"})
                    .isSuccess());

    BinprotGenericCommand cmd{cb::mcbp::ClientOpcode::GetClusterConfig};
    const auto response = userConnection->execute(cmd);
    EXPECT_TRUE(response.isSuccess()) << to_string(response.getStatus());
    EXPECT_EQ(expectedDatatype, cb::mcbp::Datatype(response.getDatatype()));
    EXPECT_EQ(config,
              getInflatedValue(response.getDatatype(), response.getDataView()));
}

TEST_P(ClusterConfigTest, test_MB_17506_no_dedupe) {
    test_MB_17506(false, false);
}

TEST_P(ClusterConfigTest, test_MB_17506_dedupe) {
    test_MB_17506(true, false);
}

TEST_P(ClusterConfigTest, test_MB_17506_no_dedupe_enabled_on_client) {
    test_MB_17506(false, true);
}

TEST_P(ClusterConfigTest, test_MB_17506_dedupe_enabled_on_client) {
    test_MB_17506(true, false);
}

TEST_P(ClusterConfigTest, Enable_CCCP_Push_Notifications) {
    auto conn = userConnection->clone();
    // The "connection class" ignore the context part in
    // extended error message unless we enable the JSON datatype
    conn->setDatatypeJson(true);
    conn->setClustermapChangeNotification(false);
    conn->setDuplexSupport(false);

    try {
        conn->setClustermapChangeNotification(true);
        FAIL() << "It should not be possible to enable CCCP push notifications "
                  "without duplex";
    } catch (const std::runtime_error& e) {
        EXPECT_STREQ(
                "Failed to say hello: 'Clustermap change notification needs "
                "Duplex', Invalid arguments (4)",
                e.what());
    }

    // With duplex we should we good to go
    conn->setDuplexSupport(true);
    conn->setClustermapChangeNotification(true);
}

void ClusterConfigTest::test_CccpPushNotification(bool global, bool brief) {
    const std::string bucket_map{R"({"rev":666, "bucket" : true})"};
    const std::string global_map{R"({"rev":532, "bucket" : false})"};
    std::string expected;
    if (global) {
        expected = global_map;
    } else {
        expected = bucket_map;
    }

    auto second = userConnection->clone();
    second->authenticate("Luke", mcd_env->getPassword("Luke"));
    second->selectBucket(bucketName);
    second->setFeature(cb::mcbp::Feature::UnorderedExecution, true);
    second->setFeature(cb::mcbp::Feature::Duplex, true);
    if (brief) {
        second->setFeature(cb::mcbp::Feature::ClustermapChangeNotificationBrief,
                           true);
    } else {
        second->setFeature(cb::mcbp::Feature::ClustermapChangeNotification,
                           true);
    }

    if (global) {
        adminConnection->executeInBucket(bucketName, [&global_map](auto& c) {
            ASSERT_TRUE(c.execute(BinprotSetClusterConfigCommand{
                                          token, global_map, 2, 532, {}})

                                .isSuccess());
        });
    } else {
        adminConnection->executeInBucket(bucketName, [&bucket_map](auto& c) {
            ASSERT_TRUE(
                    c.execute(BinprotSetClusterConfigCommand{
                                      token, bucket_map, 1, 666, bucketName})
                            .isSuccess());
        });
    }

    Frame frame;

    // Setting a new config should cause the server to push a new config
    // to me!
    second->recvFrame(frame);
    EXPECT_EQ(cb::mcbp::Magic::ServerRequest, frame.getMagic());

    auto* request = frame.getRequest();

    EXPECT_EQ(cb::mcbp::ServerOpcode::ClustermapChangeNotification,
              request->getServerOpcode());
    using cb::mcbp::request::SetClusterConfigPayload;
    EXPECT_EQ(sizeof(SetClusterConfigPayload), request->getExtlen());
    auto extras = request->getExtdata();
    auto* ver = reinterpret_cast<const SetClusterConfigPayload*>(extras.data());
    if (global) {
        EXPECT_TRUE(request->getKeyString().empty()) << request->getKeyString();
        EXPECT_EQ(532, ver->getRevision());
        EXPECT_EQ(2, ver->getEpoch());
    } else {
        EXPECT_EQ(666, ver->getRevision());
        EXPECT_EQ(1, ver->getEpoch());
        EXPECT_EQ(bucketName, request->getKeyString());
    }

    if (brief) {
        EXPECT_EQ(cb::mcbp::Datatype::Raw, request->getDatatype());
        EXPECT_TRUE(request->getValue().empty()) << request->getValueString();
    } else if (hasSnappySupport() == ClientSnappySupport::Everywhere) {
        if (hasJSONSupport() == ClientJSONSupport::Yes) {
            ASSERT_EQ(cb::mcbp::Datatype::SnappyCompressedJson,
                      request->getDatatype());
        } else {
            ASSERT_EQ(cb::mcbp::Datatype::Snappy, request->getDatatype());
        }
        ASSERT_EQ(expected,
                  getInflatedValue(
                          protocol_binary_datatype_t(request->getDatatype()),
                          request->getValueString()));
    } else {
        if (hasJSONSupport() == ClientJSONSupport::Yes) {
            EXPECT_EQ(cb::mcbp::Datatype::JSON, request->getDatatype());
        } else {
            EXPECT_EQ(cb::mcbp::Datatype::Raw, request->getDatatype());
        }
        EXPECT_EQ(expected, request->getValueString());
    }
}

TEST_P(ClusterConfigTest, CccpPushNotification_Bucket) {
    test_CccpPushNotification(false, false);
}

TEST_P(ClusterConfigTest, CccpPushNotification_Global) {
    test_CccpPushNotification(true, false);
}

TEST_P(ClusterConfigTest, ClustermapChangeNotificationBrief_Bucket) {
    test_CccpPushNotification(false, true);
}

TEST_P(ClusterConfigTest, ClustermapChangeNotificationBrief_Global) {
    test_CccpPushNotification(true, true);
}

TEST_P(ClusterConfigTest, SetGlobalClusterConfig) {
    // Set one for the default bucket
    setClusterConfig(token, R"({"rev":1000})", 1000);

    // Set the global config
    auto rsp = adminConnection->execute(BinprotSetClusterConfigCommand{
            token, R"({"foo" : "bar"})", 1, 100, ""});
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getDataString();

    rsp = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::GetClusterConfig});
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getDataString();
    EXPECT_EQ(R"({"foo" : "bar"})", rsp.getDataString());

    adminConnection->executeInBucket(bucketName, [](auto& c) {
        auto rsp = c.execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::GetClusterConfig});
        ASSERT_TRUE(rsp.isSuccess()) << rsp.getDataString();
        EXPECT_EQ(R"({"rev":1000})", rsp.getDataString());
    });
}

/**
 * The bucket configuration was not reset as part of bucket deletion
 */
TEST_P(ClusterConfigTest, MB35395) {
    setClusterConfig(token, R"({"rev":1000})", 1000);

    // Recreate the bucket, and the cluster config should be gone!
    DeleteTestBucket();
    CreateTestBucket();

    adminConnection->executeInBucket(bucketName, [](auto& c) {
        auto rsp = c.execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::GetClusterConfig});
        ASSERT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
        EXPECT_EQ("", rsp.getDataString());
    });
}

TEST_P(ClusterConfigTest, MB57311_RequestWithVersion) {
    setClusterConfig(token, R"({"rev":1000})", 1000);
    auto validatePushedRevno = [](int64_t epoch, int64_t revno) {
        nlohmann::json json;
        userConnection->stats(
                [&json](auto k, auto v) { json = nlohmann::json::parse(v); },
                "connections self");
        EXPECT_EQ(epoch, json["clustermap"]["epoch"].get<int64_t>());
        EXPECT_EQ(revno, json["clustermap"]["revno"].get<int64_t>());
    };

    auto rsp = userConnection->execute(BinprotGetClusterConfigCommand{});
    EXPECT_TRUE(rsp.isSuccess());
    EXPECT_EQ(expectedDatatype, cb::mcbp::Datatype{rsp.getDatatype()});
    EXPECT_EQ(R"({"rev":1000})",
              getInflatedValue(rsp.getDatatype(), rsp.getDataView()));
    validatePushedRevno(1, 1000);

    // select the bucket and reset the known pushed version
    userConnection->selectBucket(bucketName);
    validatePushedRevno(-1, 0);

    // If we provide an older version we should receive the map
    rsp = userConnection->execute(BinprotGetClusterConfigCommand{1, 999});
    EXPECT_EQ(expectedDatatype, cb::mcbp::Datatype{rsp.getDatatype()});
    EXPECT_TRUE(rsp.isSuccess());

    EXPECT_EQ(expectedDatatype, cb::mcbp::Datatype{rsp.getDatatype()});
    EXPECT_EQ(R"({"rev":1000})",
              getInflatedValue(rsp.getDatatype(), rsp.getDataView()));
    validatePushedRevno(1, 1000);

    // select the bucket and reset the known pushed version
    userConnection->selectBucket(bucketName);
    validatePushedRevno(-1, 0);

    /// If we know the version we should get an empty response
    rsp = userConnection->execute(BinprotGetClusterConfigCommand{1, 1000});
    EXPECT_TRUE(rsp.isSuccess());
    EXPECT_EQ(PROTOCOL_BINARY_RAW_BYTES, rsp.getDatatype());
    EXPECT_TRUE(rsp.getDataView().empty()) << rsp.getDataView();
    validatePushedRevno(1, 1000);

    // select the bucket and reset the known pushed version
    userConnection->selectBucket(bucketName);
    validatePushedRevno(-1, 0);

    /// If we know a newer version we should get an empty response
    rsp = userConnection->execute(BinprotGetClusterConfigCommand{1, 1001});
    EXPECT_TRUE(rsp.isSuccess());
    EXPECT_EQ(PROTOCOL_BINARY_RAW_BYTES, rsp.getDatatype());
    EXPECT_TRUE(rsp.getDataView().empty()) << rsp.getDataView();
    validatePushedRevno(1, 1001);
}

TEST_P(ClusterConfigTest, ClustermapChangeNotificationBothRequested) {
    BinprotHelloCommand cmd("client");
    using cb::mcbp::Feature;
    cmd.enableFeature(Feature::Duplex);
    cmd.enableFeature(Feature::ClustermapChangeNotification);
    cmd.enableFeature(Feature::ClustermapChangeNotificationBrief);
    auto conn = userConnection->clone(false);
    conn->connect();
    auto rsp = BinprotHelloResponse{conn->execute(cmd)};
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    std::vector<Feature> enabled = rsp.getFeatures();
    ASSERT_EQ(2, enabled.size());
    if (enabled.front() == Feature::ClustermapChangeNotificationBrief) {
        std::swap(enabled.front(), enabled.back());
    }
    const std::vector<Feature> expected{
            {Feature::Duplex, Feature::ClustermapChangeNotificationBrief}};
    EXPECT_EQ(expected, enabled);
}
