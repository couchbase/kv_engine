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

#include <nlohmann/json.hpp>
#include <protocol/connection/frameinfo.h>

// Test fixture for new MCBP miscellaneous commands
class MiscTest : public TestappClientTest {
public:
    static void SetUpTestCase() {
        TestappTest::SetUpTestCase();
        createUserConnection = true;
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         MiscTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

TEST_P(MiscTest, GetFailoverLog) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::GetFailoverLog);

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
    EXPECT_EQ(response.getData().size(), 0x10);

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

/**
 * Create a basic test to verify that the ioctl to fetch the database
 * works. Once we add support for modifying the RBAC database we'll
 * add tests to verify the content
 */
TEST_P(MiscTest, GetRbacDatabase) {
    // Verify that we need privileges to do this
    auto& conn = getConnection();
    auto response = conn.execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::IoctlGet, "rbac.db.dump?domain=external"});
    ASSERT_FALSE(response.isSuccess());
    ASSERT_EQ(cb::mcbp::Status::Eaccess, response.getStatus());

    conn.authenticate("@admin", mcd_env->getPassword("@admin"));
    response = conn.execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::IoctlGet, "rbac.db.dump?domain=external"});
    ASSERT_TRUE(response.isSuccess());
    ASSERT_FALSE(response.getDataString().empty());
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

TEST_P(MiscTest, SessionCtrlToken) {
    // Validate that you may successfully set the token to a legal value
    auto rsp = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::GetCtrlToken});
    ASSERT_TRUE(rsp.isSuccess());

    uint64_t old_token = rsp.getCas();
    ASSERT_NE(0, old_token);
    uint64_t new_token = 0x0102030405060708;

    // Test that you can set it with the correct ctrl token
    rsp = adminConnection->execute(
            BinprotSetControlTokenCommand{new_token, old_token});
    ASSERT_TRUE(rsp.isSuccess());
    EXPECT_EQ(new_token, rsp.getCas());
    old_token = new_token;

    // Validate that you can't set 0 as the ctrl token
    rsp = adminConnection->execute(
            BinprotSetControlTokenCommand{0ull, old_token});
    ASSERT_FALSE(rsp.isSuccess())
            << "It shouldn't be possible to set token to 0";

    // Validate that you can't set it by providing an incorrect cas
    rsp = adminConnection->execute(
            BinprotSetControlTokenCommand{1234ull, old_token - 1});
    ASSERT_EQ(cb::mcbp::Status::KeyEexists, rsp.getStatus());

    // Validate that you can set it by providing the correct token
    rsp = adminConnection->execute(
            BinprotSetControlTokenCommand{0xdeadbeefull, old_token});
    ASSERT_TRUE(rsp.isSuccess());
    ASSERT_EQ(0xdeadbeefull, rsp.getCas());

    rsp = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::GetCtrlToken});
    ASSERT_TRUE(rsp.isSuccess());
    ASSERT_EQ(0xdeadbeefull, rsp.getCas());
}

TEST_P(MiscTest, ExceedMaxPacketSize) {
    cb::mcbp::Request request;
    request.setMagic(cb::mcbp::Magic::ClientRequest);
    request.setOpcode(cb::mcbp::ClientOpcode::Set);
    request.setExtlen(sizeof(cb::mcbp::request::MutationPayload));
    request.setKeylen(1);
    request.setBodylen(31 * 1024 * 1024);
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

TEST_F(TestappTest, CollectionsSelectBucket) {
    // Create and select a bucket on which we will be able to hello collections
    adminConnection->createBucket("collections", "", BucketType::Couchbase);

    auto& conn = getAdminConnection();
    conn.selectBucket("collections");

    // Hello collections to enable collections for this connection
    BinprotHelloCommand cmd("Collections");
    cmd.enableFeature(cb::mcbp::Feature::Collections);
    const auto rsp = BinprotHelloResponse(conn.execute(cmd));
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus());

    try {
        conn.selectBucket(bucketName);
        if (!GetTestBucket().supportsCollections()) {
            FAIL() << "Select bucket did not throw a not supported error when"
                      "attempting to select a memcache bucket with a "
                      "collections enabled connections";
        }
    } catch (const ConnectionError& e) {
        if (!GetTestBucket().supportsCollections()) {
            EXPECT_EQ(cb::mcbp::Status::NotSupported, e.getReason());
        } else {
            FAIL() << "Select bucket failed for unknown reason: "
                   << to_string(e.getReason());
        }
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
        addFrameInfo(BarrierFrameInfo());
        addFrameInfo(BarrierFrameInfo());

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
