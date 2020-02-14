/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include "testapp.h"
#include "testapp_client_test.h"

#include <platform/compress.h>
#include <protocol/connection/frameinfo.h>
#include <protocol/mcbp/ewb_encode.h>
#include <algorithm>

class RegressionTest : public TestappClientTest {};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         RegressionTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

/**
 * MB-26196: A client without xerror may still receive extended error
 *           codes instead of silently disconnect.
 */
TEST_P(RegressionTest, MB_26196) {
    auto& conn = getConnection();

    conn.authenticate("jones", "jonespassword", "PLAIN");

    BinprotGenericCommand cmd{cb::mcbp::ClientOpcode::GetClusterConfig, "", ""};
    auto response = conn.execute(cmd);
    EXPECT_FALSE(response.isSuccess());
    // We don't have access to the global config
    EXPECT_EQ(cb::mcbp::Status::Eaccess, response.getStatus());

    // Disable xerror
    conn.setXerrorSupport(false);

    // rerun get cluster config, and this time I should be disconnected.
    conn.sendCommand(cmd);
    try {
        conn.recvResponse(response);
        FAIL() << "Non-xerror aware clients should be disconnected";
    } catch (const std::system_error& e) {
        EXPECT_EQ(std::errc::connection_reset, std::errc(e.code().value()));
    } catch (...) {
        FAIL() << "Expected system error to be thrown";
    }
}

/**
 * MB-26828: Concurrent multi subdoc ops on same doc return not_stored error
 *
 * Subdoc use "add semantics" when trying to add the first version of a
 * document to the engine even if the client is trying to perform a set
 * with a CAS wildcard (in order to avoid racing on the set).
 *
 * The referenced bug identified a problem that the server didn't correctly
 * retry the situation where two threads tried to create the same document.
 *
 * This test verifies that the fix did not affect the normal Add semantics
 */
TEST_P(RegressionTest, MB_26828_AddIsUnaffected) {
    auto& conn = getConnection();

    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);

    cmd.addDocFlag(mcbp::subdoc::doc_flag::Add);
    cmd.addMutation(
            cb::mcbp::ClientOpcode::SubdocArrayPushLast,
            SUBDOC_FLAG_MKDIR_P,
            "cron_timers",
            R"({"callback_func": "NDtimerCallback", "payload": "doc_id_610"})");
    auto resp = conn.execute(cmd);

    EXPECT_TRUE(resp.isSuccess()) << "Expected to work for Add";
    // If we try it one more time, it should fail as we want to
    // _ADD_ the doc if it isn't there
    resp = conn.execute(cmd);
    EXPECT_FALSE(resp.isSuccess()) << "Add should fail when it isn't there";
    EXPECT_EQ(cb::mcbp::Status::KeyEexists, resp.getStatus());
}

/**
 * MB-26828: Concurrent multi subdoc ops on same doc return not_stored error
 *
 * Subdoc use "add semantics" when trying to add the first version of a
 * document to the engine even if the client is trying to perform a set
 * with a CAS wildcard (in order to avoid racing on the set).
 *
 * The referenced bug identified a problem that the server didn't correctly
 * retry the situation where two threads tried to create the same document.
 *
 * This test verifies that the fix resolved the problem
 */
TEST_P(RegressionTest, MB_26828_SetIsFixed) {
    auto& conn = getConnection();
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);

    // Configure the ewouldblock_engine to inject fake NOT STORED
    // failure for the 3rd call (i.e. the 1st engine->store() attempt).
    auto sequence = ewb::encodeSequence({
            ewb::Passthrough,
            ewb::Passthrough,
            cb::engine_errc::not_stored,
            ewb::Passthrough,
            ewb::Passthrough,
            ewb::Passthrough,
    });
    conn.configureEwouldBlockEngine(EWBEngineMode::Sequence,
                                    /*unused*/ {},
                                    /*unused*/ {},
                                    sequence);

    cmd.addDocFlag(mcbp::subdoc::doc_flag::Mkdoc);

    cmd.addMutation(
            cb::mcbp::ClientOpcode::SubdocArrayPushLast,
            SUBDOC_FLAG_MKDIR_P,
            "cron_timers",
            R"({"callback_func": "NDtimerCallback", "payload": "doc_id_610"})");
    auto resp = conn.execute(cmd);

    EXPECT_TRUE(resp.isSuccess());
    // Reset connection to make sure we're not affected by any ewb logic
    conn = getConnection();
    resp = conn.execute(cmd);
    EXPECT_TRUE(resp.isSuccess());
}

/**
 * https://issues.couchbase.com/browse/MB-31070
 *
 * Expiry time becomes 0 after append.
 *
 * This test validates that it is fixed by:
 *   1. Store a document with 30 seconds expiry time
 *   2. Fetch the expiry time of the document (absolute time)
 *   3. Performs an append
 *   4. Fetch the expiry time of the document and verifies that it is unchanged
 */
TEST_P(RegressionTest, MB_31070) {
    auto& conn = getConnection();

    Document document;
    document.info.cas = mcbp::cas::Wildcard;
    document.info.flags = 0xcaffee;
    document.info.id = name;
    document.info.expiration = 30;
    document.value = "hello";

    conn.mutate(document, Vbid(0), MutationType::Set);

    BinprotSubdocMultiLookupCommand cmd;
    cmd.setKey(name);
    cmd.addGet("$document.exptime", SUBDOC_FLAG_XATTR_PATH);

    auto multiResp = BinprotSubdocMultiLookupResponse(conn.execute(cmd));

    auto& results = multiResp.getResults();

    EXPECT_EQ(cb::mcbp::Status::Success, multiResp.getStatus());
    EXPECT_EQ(cb::mcbp::Status::Success, results[0].status);
    const auto exptime = std::stol(results[0].value);
    EXPECT_LT(0, exptime);

    document.value = " world";
    document.info.expiration = 0;
    conn.mutate(document, Vbid(0), MutationType::Append);

    multiResp = BinprotSubdocMultiLookupResponse(conn.execute(cmd));

    EXPECT_EQ(cb::mcbp::Status::Success, multiResp.getStatus());
    EXPECT_EQ(cb::mcbp::Status::Success, results[0].status);
    EXPECT_EQ(exptime, std::stol(results[0].value));
}

/**
 * https://issues.couchbase.com/browse/MB-31149
 *
 * When sending an Append (0x0e) request to the server I'm seeing a status
 * of success with a CAS value of 0 when the MutationSeqNo Hello Feature is
 * set. When the MutationSeqNo Hello Feature is not set then the CAS value is
 * correct and everything looks fine.
 */
TEST_P(RegressionTest, MB_31149) {
    auto& conn = getConnection();

    Document document;
    document.info.cas = mcbp::cas::Wildcard;
    document.info.flags = 0xcaffee;
    document.info.id = name;
    document.info.expiration = 30;
    document.value = "hello";

    conn.mutate(document, Vbid(0), MutationType::Set);
    document.info.expiration = 0;
    document.value = " world";
    const auto info = conn.mutate(document, Vbid(0), MutationType::Append);

    EXPECT_NE(0, info.cas);
}

/**
 * MB-32078: When the user hasn't specified a cas for the operation, if another
 * thread changes the key between the append state machine reading the key and
 * attempting to store the new value, the storeItem function should reset the
 * state machine and retry the operation. MB-32078 causes the function to return
 * the KEY_EEXISTS error, this prevents the state machine from continuing,
 * causing the operation to fail by returning the KEY_EEXISTS error to client.
 * We use ewouldblock engine to simulate this scenario by blindly returning
 * KEY_EEXISTS to the first store request.
 */
TEST_P(RegressionTest, MB_32078) {
    auto& connection = getConnection();
    connection.store("MB-32078-testkey", Vbid(0), "value");

    connection.configureEwouldBlockEngine(
            EWBEngineMode::CasMismatch, ENGINE_KEY_EEXISTS, 1);

    BinprotGenericCommand cmd(
            cb::mcbp::ClientOpcode::Append, "MB-32078-testkey", "+");
    connection.sendCommand(cmd);

    BinprotResponse response;
    connection.recvResponse(response);

    EXPECT_EQ(cb::mcbp::Status::Success, response.getStatus());
    EXPECT_STREQ("value+",
                 connection.get("MB-32078-testkey", Vbid(0)).value.c_str());

    connection.remove("MB-32078-testkey", Vbid(0));
}

TEST_P(RegressionTest, MB_32081) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::SetWithMeta);
    // The packet as found in MB-32081
    // See https://issues.couchbase.com/browse/MB-32113 for a full dump
    // of the packet
    std::vector<uint8_t> packet = {
            {0x80, 0xa2, 0x00, 0x04, 0x1e, 0x00, 0x00, 0x00, // |
             0x00, 0x00, 0x00, 0x3a, 0x94, 0x61, 0xd5, 0xd5, //  } Header
             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // |
             0x02, 0x00, 0x00, 0x00, // flags
             0x00, 0x00, 0x00, 0x00, // exptime
             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x27, // seqno
             0x15, 0x69, 0x49, 0x75, 0x23, 0x26, 0x00, 0x00, // cas
             0x00, 0x00, 0x00, 0x00, // options
             0x00, 0x00, // nmeta
             0x74, 0x65, 0x73, 0x74, // Key: Test
             0x7b, 0x22, 0x73, 0x65, 0x74, 0x5f, 0x77, 0x69, // |
             0x74, 0x68, 0x5f, 0x6d, 0x65, 0x74, 0x61, 0x22, //  } value
             0x3a, 0x22, 0x74, 0x65, 0x73, 0x74, 0x22, 0x7d}}; // |

    auto& conn = getConnection();
    Frame frame;
    frame.payload = std::move(packet);

    conn.sendFrame(frame);
    BinprotResponse rsp;
    conn.recvResponse(rsp);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getDataString();
}

/// BinprotSetControlTokenCommand did not use the provided old token,
/// but always used 0 (override)
TEST_P(RegressionTest, SetCtrlToken) {
    auto& conn = getAdminConnection();
    const auto rsp =
            conn.execute(BinprotSetControlTokenCommand{32, token - 10});
    ASSERT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::KeyEexists, rsp.getStatus());
}

/**
 * Increment should set the datatype to JSON
 */
TEST_P(RegressionTest, MB35528) {
    auto& conn = getConnection();
    conn.setDatatypeJson(true);
    conn.increment(name, 1, 0, 0, nullptr);
    const auto info = conn.get(name, Vbid{0});
    EXPECT_EQ(cb::mcbp::Datatype::JSON, info.info.datatype);
}

/// MB-37506 - Incorrect validation of frame attributes
TEST_P(RegressionTest, MB37506) {
    class InvalidDurabilityFrameInfo : public DurabilityFrameInfo {
    public:
        InvalidDurabilityFrameInfo()
            : DurabilityFrameInfo(cb::durability::Level::Majority,
                                  cb::durability::Timeout(32)) {
        }
        std::vector<uint8_t> encode() const override {
            auto ret = DurabilityFrameInfo::encode();
            ret.pop_back();
            return ret;
        }
    };

    auto& conn = getAdminConnection();
    conn.selectBucket("default");

    // Add the DcpStreamID (which we used to stop parsing after checking)
    // and add an invalid durability encoding...
    try {
        conn.get("MB37506", Vbid{0}, []() -> FrameInfoVector {
            FrameInfoVector ret;
            ret.emplace_back(std::make_unique<DcpStreamIdFrameInfo>(12));
            ret.emplace_back(std::make_unique<InvalidDurabilityFrameInfo>());
            return ret;
        });
        FAIL() << "Should not be able to find the document";
    } catch (const ConnectionError& e) {
        ASSERT_TRUE(e.isInvalidArguments());
    }
}
