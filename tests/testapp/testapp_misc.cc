/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp.h"
#include "testapp_client_test.h"

#include <fmt/format.h>
#include <mcbp/codec/frameinfo.h>
#include <nlohmann/json.hpp>

// Test fixture for new MCBP miscellaneous commands
class MiscTest : public TestappClientTest {
public:
    static void SetUpTestCase() {
        TestappTest::SetUpTestCase();
        createUserConnection = true;
    }

    // Helper function to test if "default" thread setting is applied correctly
    // for the given named thread type, and a function to return the current
    // size of that thread pool.
    void testConfigDefaultThreads(const std::string& name,
                                  const std::function<int()>& getPoolSize);
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         MiscTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

TEST_P(MiscTest, GetFailoverLog) {
    // Test existing VBucket
    auto response = userConnection->getFailoverLog(Vbid(0));
    auto header = response.getResponse();
    EXPECT_EQ(cb::mcbp::Magic::ClientResponse, header.getMagic());
    EXPECT_EQ(cb::mcbp::ClientOpcode::GetFailoverLog, header.getClientOpcode());
    EXPECT_EQ(0, header.getKeylen());
    EXPECT_EQ(0, header.getExtlen());
    EXPECT_EQ(cb::mcbp::Datatype::Raw, header.getDatatype());
    EXPECT_EQ(header.getStatus(), cb::mcbp::Status::Success);
    // Note: We expect 1 entry in the failover log, which is the entry created
    // at VBucket creation (8 bytes for UUID + 8 bytes for SEQNO)
    EXPECT_EQ(0x10, header.getBodylen());
    EXPECT_EQ(0, header.getCas());
    EXPECT_EQ(response.getDataView().size(), 0x10);

    // Test non-existing VBucket
    response = userConnection->getFailoverLog(Vbid(1));
    header = response.getResponse();
    EXPECT_EQ(cb::mcbp::Magic::ClientResponse, header.getMagic());
    EXPECT_EQ(cb::mcbp::ClientOpcode::GetFailoverLog, header.getClientOpcode());
    EXPECT_EQ(0, header.getKeylen());
    EXPECT_EQ(0, header.getExtlen());
    EXPECT_EQ(cb::mcbp::Datatype::Raw, header.getDatatype());
    EXPECT_EQ(header.getStatus(), cb::mcbp::Status::NotMyVbucket);
    EXPECT_EQ(0, header.getBodylen());
    EXPECT_EQ(0, header.getCas());
}

/**
 * Send the UpdateUserPermissions with a valid username and paylaod.
 *
 * Unfortunately there isn't a way to verify that the user was actually
 * updated as we can't fetch the updated entry.
 */
TEST_P(MiscTest, UpdateUserPermissionsSuccess) {
    const std::string rbac = R"(
{"johndoe" : {
  "domain" : "external",
  "buckets": {
    "default": ["Read","SimpleStats","Insert","Delete","Upsert"]
  },
  "privileges": []
}})";
    const auto resp =
            adminConnection->execute(BinprotUpdateUserPermissionsCommand{rbac});
    EXPECT_TRUE(resp.isSuccess());
}

/**
 * Send the UpdateUserPermissions with a valid username, but invalid payload.
 *
 * Unfortunately there isn't a way to verify that the user was actually
 * updated as we can't fetch the updated entry.
 */
TEST_P(MiscTest, UpdateUserPermissionsInvalidPayload) {
    const auto resp = adminConnection->execute(
            BinprotUpdateUserPermissionsCommand{"bogus"});
    EXPECT_FALSE(resp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
}

TEST_P(MiscTest, Config_Validate_Empty) {
    const auto rsp = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::ConfigValidate});
    ASSERT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
}

TEST_P(MiscTest, Config_ValidateInvalidJSON) {
    const auto rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::ConfigValidate, "", "This isn't JSON"});
    ASSERT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
}

// Sanity test that the current config can be passed to ConfigValidate, and
// this returns Success.
TEST_P(MiscTest, Config_Identical) {
    const auto rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::ConfigValidate, "", memcached_cfg.dump()});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
}

// Check that AuxIO threads can be set to "default", and this derives the
// correct thread count (N CPUs).
TEST_P(MiscTest, Config_AuxIoThreadsDefault) {
    auto getAuxIoThreads = [] {
        return adminConnection->stats("threads")
                .at("num_auxio_threads_actual")
                .get<int>();
    };
    testConfigDefaultThreads("num_auxio_threads", getAuxIoThreads);
}

// Check that NonIO threads can be set to "default", and this derives the
// correct thread count (N CPUs).
TEST_P(MiscTest, Config_NonIoThreadsDefault) {
    auto getAuxIoThreads = [] {
        return adminConnection->stats("threads")
                .at("num_nonio_threads_actual")
                .get<int>();
    };
    testConfigDefaultThreads("num_nonio_threads", getAuxIoThreads);
}

void MiscTest::testConfigDefaultThreads(
        const std::string& name, const std::function<int()>& getPoolSize) {
    SCOPED_TRACE(name);
    // Lookup the "default" thread count; we will compare this to
    // what we get later when we flip away from "default" and back again.
    const auto defaultThreads = getPoolSize();
    ASSERT_GE(defaultThreads, 2)
            << "Sanity check - should always have at least 2 threads";

    // The initial value is "default", and nothing happens if config param
    // hasn't changed, so first need to reconfigure to some non-default value.
    auto newConfig = memcached_cfg;
    newConfig[name] = 1;
    auto rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::ConfigValidate, "", newConfig.dump()});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus());

    write_config_to_file(newConfig.dump());
    rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::ConfigReload, "", ""});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus());

    // Ok, now do the actual test - set to "default" and confirm this correctly
    // sets the thread count.
    newConfig[name] = "default";
    rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::ConfigValidate, "", newConfig.dump()});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus());

    write_config_to_file(newConfig.dump());
    rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::ConfigReload, "", ""});
    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());

    EXPECT_EQ(defaultThreads, getPoolSize());
}

/// No longer supported
TEST_P(MiscTest, SessionCtrlToken) {
    auto rsp = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::GetCtrlToken});
    ASSERT_EQ(cb::mcbp::Status::NotSupported, rsp.getStatus());
    rsp = adminConnection->execute(BinprotSetControlTokenCommand{1, 0});
    ASSERT_EQ(cb::mcbp::Status::NotSupported, rsp.getStatus());
}

TEST_P(MiscTest, ExceedMaxPacketSize) {
    cb::mcbp::Request request;
    request.setMagic(cb::mcbp::Magic::ClientRequest);
    request.setOpcode(cb::mcbp::ClientOpcode::Set);
    request.setExtlen(sizeof(cb::mcbp::request::MutationPayload));
    request.setKeylen(1);
    request.setBodylen(31_MiB);
    request.setOpaque(0xdeadbeef);

    auto mysocket = getConnection().releaseSocket();
    ASSERT_EQ(sizeof(request),
              cb::net::send(mysocket, &request, sizeof(request), 0));

    // the server will read the header, and figure out that the packet
    // is too big and close the socket
    std::vector<uint8_t> blob(1024);
    EXPECT_EQ(0, cb::net::recv(mysocket, blob.data(), blob.size(), 0));
    cb::net::closesocket(mysocket);
}

TEST_P(MiscTest, Version) {
    const auto rsp = userConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::Version});
    EXPECT_EQ(cb::mcbp::ClientOpcode::Version, rsp.getOp());
    EXPECT_TRUE(rsp.isSuccess());
}

TEST_P(MiscTest, SetChronicalAuthToken_NoArg) {
    adminConnection->executeInBucket(bucketName, [](auto& conn) {
        auto cmd = BinprotGenericCommand{
                cb::mcbp::ClientOpcode::SetChronicleAuthToken};
        cmd.setVBucket(Vbid(0));
        nlohmann::json json;
        cmd.setValue(json.dump());
        cmd.setDatatype(cb::mcbp::Datatype::JSON);
        const auto resp = conn.execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
    });
}

TEST_P(MiscTest, SetChronicalAuthToken_InvalidArgName) {
    adminConnection->executeInBucket(bucketName, [](auto& conn) {
        auto cmd = BinprotGenericCommand{
                cb::mcbp::ClientOpcode::SetChronicleAuthToken};
        cmd.setVBucket(Vbid(0));
        nlohmann::json json;
        json["wrong-token-name"] = "1234";
        cmd.setValue(json.dump());
        cmd.setDatatype(cb::mcbp::Datatype::JSON);
        const auto resp = conn.execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
    });
}

TEST_P(MiscTest, SetChronicalAuthToken_InvalidArgPayload) {
    adminConnection->executeInBucket(bucketName, [](auto& conn) {
        auto cmd = BinprotGenericCommand{
                cb::mcbp::ClientOpcode::SetChronicleAuthToken};
        cmd.setVBucket(Vbid(0));
        nlohmann::json json;
        json["token"] = 1234;
        cmd.setValue(json.dump());
        cmd.setDatatype(cb::mcbp::Datatype::JSON);
        const auto resp = conn.execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
    });
}

TEST_P(MiscTest, SetChronicalAuthToken) {
    adminConnection->executeInBucket(bucketName, [](auto& conn) {
        const auto expectedStatus = mcd_env->getTestBucket().isMagma()
                                            ? cb::mcbp::Status::Success
                                            : cb::mcbp::Status::NotSupported;

        auto cmd = BinprotGenericCommand{
                cb::mcbp::ClientOpcode::SetChronicleAuthToken};
        cmd.setVBucket(Vbid(0));
        nlohmann::json json;
        json["token"] = "1234";
        cmd.setValue(json.dump());
        cmd.setDatatype(cb::mcbp::Datatype::JSON);
        const auto resp = conn.execute(cmd);
        EXPECT_EQ(expectedStatus, resp.getStatus());
    });
}

TEST_F(TestappTest, CollectionsSelectBucket) {
    // Create and select a bucket on which we will be able to hello collections
    mcd_env->getTestBucket().createBucket("collections", "", *adminConnection);

    auto& conn = getAdminConnection();
    conn.selectBucket("collections");

    // Hello collections to enable collections for this connection
    BinprotHelloCommand cmd("Collections");
    cmd.enableFeature(cb::mcbp::Feature::Collections);
    const auto rsp = BinprotHelloResponse(conn.execute(cmd));
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus());

    try {
        conn.selectBucket(bucketName);
    } catch (const ConnectionError& e) {
        FAIL() << "Select bucket failed for unknown reason: " << e.getReason();
    }
    adminConnection->deleteBucket("collections");
}

/**
 * Encode a Alt Request (0x08) HELO command with framing extras but
 * too short total length field.
 *
 * See MB-46853.
 */
class MalformedHelloCommand : public BinprotHelloCommand {
public:
    MalformedHelloCommand() : BinprotHelloCommand("client") {
        // To repro the issue, It does not matter _what_ frame infos are
        // present, as long as there are two and they are valid.
        addFrameInfo(cb::mcbp::request::BarrierFrameInfo());
        addFrameInfo(cb::mcbp::request::BarrierFrameInfo());

        // _NO_ Features are advertised; the value should be empty.
    }

    void encode(std::vector<uint8_t>& buf) const override {
        BinprotHelloCommand::encode(buf);
        // At this point
        //   BodyLen = frameExtras + extras + key + value
        //      8     =     2       +   0    +  6  +  0
        auto& hdr = *reinterpret_cast<cb::mcbp::Request*>(buf.data());
        auto realBodyLen = hdr.getBodylen();
        EXPECT_EQ(8, realBodyLen);
        // shorten the bodyLen by two bytes. This _should_ fail validation
        // because now frameExtras + extras + key > BodyLen.
        // This would mean they key would extend past the end of the
        // request.
        // MB-46853 - this was accepted because frameExtras were not counted
        // leaving extras + key <= BodyLen
        hdr.setBodylen(realBodyLen - 2);
    }
};

TEST_F(TestappTest, MB_46853_TotalBodyLengthValidation) {
    auto& conn = getConnection();

    MalformedHelloCommand cmd;
    // MB-46853: memcached segfault, connection reset.
    // Good behaviour: validation catches malformed packet,
    //                 memcached fine, connection reset.
    try {
        conn.execute(cmd);
        // connection should be closed, reading the response should throw.
        // Fail if that didn't happen.
        FAIL();
    } catch (const std::system_error& e) {
        EXPECT_EQ(ECONNRESET, e.code().value());
    }

    // this will fail if memcached died because of the previous request.
    conn.reconnect();

    BinprotHelloCommand followup("StillAlive?");
    const auto rsp = BinprotHelloResponse(conn.execute(followup));
    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
}

TEST_F(TestappTest, ServerlessConfigCantBeSetInDefaultDeployment) {
    auto rsp = adminConnection->execute(SetNodeThrottlePropertiesCommand(
            nlohmann::json{{"capacity", 1024}}));
    EXPECT_EQ(cb::mcbp::Status::NotSupported, rsp.getStatus())
            << rsp.getDataJson().dump();
    rsp = adminConnection->execute(SetBucketThrottlePropertiesCommand(
            bucketName, {{"reserved", 1024}}));
    EXPECT_EQ(cb::mcbp::Status::NotSupported, rsp.getStatus())
            << rsp.getDataJson().dump();
}

TEST_F(TestappTest, TuneMaxConcurrentAuth) {
    auto waitfor = [](int prev, int next) {
        auto message = fmt::format(
                R"(Change max concurrent authentications {{"from":{},"to":{}}})",
                prev,
                next);
        bool found = false;
        const auto timeout =
                std::chrono::steady_clock::now() + std::chrono::seconds{10};
        while (!found && std::chrono::steady_clock::now() < timeout) {
            mcd_env->iterateLogLines([&found, message](const auto line) {
                const auto pos = line.find(message);
                if (pos == std::string_view::npos) {
                    return true;
                }
                found = true;
                return false;
            });
            if (!found) {
                std::this_thread::sleep_for(std::chrono::milliseconds{250});
            }
        }
        EXPECT_TRUE(found) << "Expected to find " << message;
    };

    memcached_cfg["max_concurrent_authentications"] = 2;
    reconfigure();
    waitfor(6, 2);

    memcached_cfg["max_concurrent_authentications"] = 8;
    reconfigure();
    waitfor(2, 8);
}

/// Unauthenticated connections should not be able to spool packets
/// larger than 1k
TEST_F(TestappTest, MB56893) {
    auto& conn = getConnection();
    conn.reconnect();

    Frame frame;
    frame.payload.resize(1_MiB + sizeof(cb::mcbp::Header));
    auto* header = reinterpret_cast<cb::mcbp::Request*>(frame.payload.data());
    header->setMagic(cb::mcbp::Magic::ClientRequest);
    header->setOpcode(cb::mcbp::ClientOpcode::Noop);
    header->setBodylen(1_MiB);

    try {
        // We don't really know how libevent will deliver the data
        // so just let try to send 1M and read something... We should either
        // fail in sending or receiving data.
        conn.sendFrame(frame);
        Frame resp;
        conn.recvFrame(resp);

        FAIL() << "It should not be possible to send a large frame to the "
                  "server: "
               << resp.getResponse()->getStatus();
    } catch (std::exception&) {
    }

    // Authenticate and try the same packet
    conn.reconnect();
    conn.authenticate("Luke");
    try {
        // We don't really know how libevent will deliver the data
        // so just let try to send 1M and read something... We should either
        // fail in sending or receiving data.
        conn.sendFrame(frame);
        Frame resp;
        conn.recvFrame(resp);
        const auto* packet = resp.getResponse();
        EXPECT_EQ(cb::mcbp::Status::Einval, packet->getStatus());
    } catch (std::exception& exception) {
        FAIL() << "It should be possible to send an invalid packet: "
               << exception.what();
    }
}
