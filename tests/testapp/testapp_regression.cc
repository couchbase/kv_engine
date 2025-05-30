/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "memcached/limits.h"
#include "testapp.h"
#include "testapp_client_test.h"

#include <evutil.h>
#include <mcbp/codec/frameinfo.h>
#include <protocol/mcbp/ewb_encode.h>
#include <algorithm>
#include <filesystem>

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

    conn.authenticate("jones");

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
    } catch (const std::exception& e) {
        FAIL() << "Expected system error to be thrown. got: " << e.what();
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
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);

    cmd.addDocFlag(cb::mcbp::subdoc::DocFlag::Add);
    cmd.addMutation(
            cb::mcbp::ClientOpcode::SubdocArrayPushLast,
            cb::mcbp::subdoc::PathFlag::Mkdir_p,
            "cron_timers",
            R"({"callback_func": "NDtimerCallback", "payload": "doc_id_610"})");
    auto resp = userConnection->execute(cmd);

    EXPECT_TRUE(resp.isSuccess()) << "Expected to work for Add";
    // If we try it one more time, it should fail as we want to
    // _ADD_ the doc if it isn't there
    resp = userConnection->execute(cmd);
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
    if (GetTestBucket().isFullEviction()) {
        // we can't test this test under full eviction as we're programming
        // a return sequence for ewb, and that may be different under full
        // eviction.
        GTEST_SKIP();
    }
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
    userConnection->configureEwouldBlockEngine(EWBEngineMode::Sequence,
                                               /*unused*/ {},
                                               /*unused*/ {},
                                               sequence);

    cmd.addDocFlag(cb::mcbp::subdoc::DocFlag::Mkdoc);

    cmd.addMutation(
            cb::mcbp::ClientOpcode::SubdocArrayPushLast,
            cb::mcbp::subdoc::PathFlag::Mkdir_p,
            "cron_timers",
            R"({"callback_func": "NDtimerCallback", "payload": "doc_id_610"})");
    auto resp = userConnection->execute(cmd);

    EXPECT_TRUE(resp.isSuccess());
    // Reset connection to make sure we're not affected by any ewb logic
    rebuildUserConnection(false);
    resp = userConnection->execute(cmd);
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
    Document document;
    document.info.cas = cb::mcbp::cas::Wildcard;
    document.info.flags = 0xcaffee;
    document.info.id = name;
    document.info.expiration = 30;
    document.value = "hello";

    userConnection->mutate(document, Vbid(0), MutationType::Set);

    BinprotSubdocMultiLookupCommand cmd;
    cmd.setKey(name);
    cmd.addGet("$document.exptime", cb::mcbp::subdoc::PathFlag::XattrPath);

    auto multiResp =
            BinprotSubdocMultiLookupResponse(userConnection->execute(cmd));

    const auto results = multiResp.getResults();
    EXPECT_EQ(cb::mcbp::Status::Success, multiResp.getStatus());
    EXPECT_EQ(cb::mcbp::Status::Success, results[0].status);
    const auto exptime = std::stol(results[0].value);
    EXPECT_LT(0, exptime);

    document.value = " world";
    document.info.expiration = 0;
    userConnection->mutate(document, Vbid(0), MutationType::Append);

    multiResp = BinprotSubdocMultiLookupResponse(userConnection->execute(cmd));

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
    Document document;
    document.info.cas = cb::mcbp::cas::Wildcard;
    document.info.flags = 0xcaffee;
    document.info.id = name;
    document.info.expiration = 30;
    document.value = "hello";

    userConnection->mutate(document, Vbid(0), MutationType::Set);
    document.info.expiration = 0;
    document.value = " world";
    const auto info =
            userConnection->mutate(document, Vbid(0), MutationType::Append);

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
    connection.authenticate("Luke");
    connection.selectBucket(bucketName);
    connection.store("MB-32078-testkey", Vbid(0), "value");

    connection.configureEwouldBlockEngine(
            EWBEngineMode::CasMismatch, cb::engine_errc::key_already_exists, 1);

    const auto response = connection.execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::Append, "MB-32078-testkey", "+"});
    EXPECT_EQ(cb::mcbp::Status::Success, response.getStatus());
    EXPECT_STREQ("value+",
                 connection.get("MB-32078-testkey", Vbid(0)).value.c_str());

    connection.remove("MB-32078-testkey", Vbid(0));
}

TEST_P(RegressionTest, MB_32081) {
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
    conn.authenticate("Luke");
    conn.selectBucket(bucketName);
    Frame frame;
    frame.payload = std::move(packet);

    conn.sendFrame(frame);
    BinprotResponse rsp;
    conn.recvResponse(rsp);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getDataView();
}

/**
 * Increment should set the datatype to JSON
 */
TEST_P(RegressionTest, MB35528) {
    auto& conn = getConnection();
    conn.authenticate("Luke");
    conn.selectBucket(bucketName);
    conn.setDatatypeJson(true);
    conn.increment(name, 1, 0, 0, nullptr);
    const auto info = conn.get(name, Vbid{0});
    EXPECT_EQ(cb::mcbp::Datatype::JSON, info.info.datatype);
}

/// MB-37506 - Incorrect validation of frame attributes
TEST_P(RegressionTest, MB37506) {
    class InvalidDurabilityFrameInfo
        : public cb::mcbp::request::DurabilityFrameInfo {
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

    auto& conn = getConnection();
    conn.authenticate("Luke");
    conn.selectBucket(bucketName);

    // Add the DcpStreamID (which we used to stop parsing after checking)
    // and add an invalid durability encoding...
    try {
        conn.get("MB37506", Vbid{0}, []() -> FrameInfoVector {
            FrameInfoVector ret;
            ret.emplace_back(
                    std::make_unique<cb::mcbp::request::DcpStreamIdFrameInfo>(
                            12));
            ret.emplace_back(std::make_unique<InvalidDurabilityFrameInfo>());
            return ret;
        });
        FAIL() << "Should not be able to find the document";
    } catch (const ConnectionError& e) {
        ASSERT_TRUE(e.isInvalidArguments());
    }
}

/// MB-38243 - Crash caused by server keeping the connection open after
///            processing a command where the validator return a failure
TEST_P(RegressionTest, MB38243) {
    auto& conn = getConnection();
    conn.authenticate("Luke");
    conn.selectBucket(bucketName);

    for (int ii = 0; ii < 10; ++ii) {
        const auto rsp = conn.execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::DcpFlush_Unsupported});
        ASSERT_FALSE(rsp.isSuccess());
    }
}

TEST_P(RegressionTest, MB39441) {
    userConnection->setFeature(cb::mcbp::Feature::JSON, true);

    userConnection->store("customer123",
                          Vbid{0},
                          R"({
  "name": "Douglas Reynholm",
  "email": "douglas@reynholmindustries.com",
  "addresses": {
    "billing": {
      "line1": "123 Any Street",
      "line2": "Anytown",
      "country": "United Kingdom"
    },
    "delivery": {
      "line1": "123 Any Street",
      "line2": "Anytown",
      "country": "United Kingdom"
    }
  },
  "purchases": {
    "complete": [
      339,
      976,
      442,
      666
    ],
    "abandoned": [
      157,
      42,
      999
    ]
  }
}
)",
                          cb::mcbp::Datatype::JSON);

    auto socket = userConnection->releaseSocket();
    rebuildUserConnection(false);
    const auto command = std::vector<uint8_t>{
            {0x80, 0xd1, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
             0x70, 0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
             0x00, 0x00, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x65, 0x72, 0x31,
             0x32, 0x33, 0xc8, 0x04, 0x00, 0x15, 0x00, 0x00, 0x00, 0x0a, 0x5f,
             0x66, 0x72, 0x61, 0x6d, 0x65, 0x77, 0x6f, 0x72, 0x6b, 0x2e, 0x6d,
             0x6f, 0x64, 0x65, 0x6c, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x22, 0x43,
             0x75, 0x73, 0x74, 0x6f, 0x6d, 0x65, 0x72, 0x22, 0xc9, 0x00, 0x00,
             0x14, 0x00, 0x00, 0x00, 0x00, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73,
             0x73, 0x65, 0x73, 0x2e, 0x62, 0x69, 0x6c, 0x6c, 0x69, 0x6e, 0x67,
             0x5b, 0x32, 0x5d, 0xca, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x15,
             0x65, 0x6d, 0x61, 0x69, 0x6c, 0x22, 0x64, 0x6f, 0x75, 0x67, 0x72,
             0x39, 0x36, 0x40, 0x68, 0x6f, 0x74, 0x6d, 0x61, 0x69, 0x6c, 0x2e,
             0x63, 0x6f, 0x6d, 0x22}};
    ASSERT_EQ(command.size(),
              cb::net::send(socket, command.data(), command.size(), 0));
    // The total response should be a 24 byte header and 3 bytes with the
    // error. We might not receive all the data in a single chunk, so lets
    // just read 1 by one (after all we just want 27 bytes so the overhead
    // is limited)
    std::vector<uint8_t> response(27);
    for (size_t ii = 0; ii < response.size(); ++ii) {
        ASSERT_EQ(1, cb::net::recv(socket, response.data() + ii, 1, 0));
    }

    const auto& header =
            *reinterpret_cast<const cb::mcbp::Header*>(response.data());
    ASSERT_TRUE(header.isResponse());
    ASSERT_EQ(3, header.getBodylen());
    ASSERT_EQ(cb::mcbp::Status::SubdocMultiPathFailure,
              header.getResponse().getStatus());

    // we should not have more data available on the socket
    evutil_make_socket_nonblocking(socket);
    const auto nb = cb::net::recv(socket, response.data(), response.size(), 0);
    EXPECT_EQ(ssize_t{-1}, nb);
    cb::net::closesocket(socket);
}

/// MB-40076 was caused by the fact that a command on a DCP connection (cookie
/// was reserved) threw an exception, but an earlier refactor in the command
/// execution path had _removed_ the cookie from the cookies array so the
/// cookie was no longer part of the cookies array when we checked if it was
/// safe to kill the object during shutdown
TEST_P(RegressionTest, MB40076) {
    userConnection->sendCommand(
            BinprotEWBCommand{EWBEngineMode::ThrowException, {}, {}, {}});
    auto s = userConnection->releaseSocket();
    char byte;
    EXPECT_EQ(0, cb::net::recv(s, &byte, 1, 0));
    cb::net::closesocket(s);
    rebuildUserConnection(false);
}

TEST_P(RegressionTest, MB44460) {
    auto& conn = getConnection();
    auto admin = conn.clone();
    admin->authenticate("@admin");
    conn.authenticate("Luke");
    auto stats = conn.stats("connections self");
    ASSERT_EQ(1, stats.size());
    const auto connectionid = stats.front()["socket"].get<size_t>();

    // Fill the sendqueue for the socket
    for (int ii = 0; ii < 1000; ++ii) {
        conn.sendCommand(BinprotGenericCommand{cb::mcbp::ClientOpcode::Noop});
    }
    stats = admin->stats("connections " + std::to_string(connectionid));
    EXPECT_EQ(1, stats.size());

    cb::net::closesocket(conn.releaseSocket());

    // We've got multiple connection threads and the "cleanup" happens async
    // so we might need to check a few times. Normally this happens within a
    // few ms..
    auto timeout = std::chrono::steady_clock::now() + std::chrono::seconds{5};

    // Let at least the other thread have a timeslot to operate on first.
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
    while (admin->stats("connections " + std::to_string(connectionid)) > 0) {
        if (std::chrono::steady_clock::now() > timeout) {
            FAIL() << "Timeout waiting 5 seconds for the connection to be "
                      "cleaned up";
        }
    }
}

/**
 *  Just test for MB-11548
 *  120 second expiry.
 *  Set clock back by some amount that's before the time we started memcached.
 *  wait 2 seconds (allow mc time to tick)
 *  (defect was that time went negative and expired keys immediatley)
 */
TEST_P(RegressionTest, MB11548_ExpiryRelativeWithClockChangeBackwards) {
    time_t now = time(nullptr);
    const auto clock_shift = (int)(0 - ((now - get_server_start_time()) * 2));
    userConnection->store(name, Vbid{0}, "value", cb::mcbp::Datatype::Raw, 120);
    userConnection->adjustMemcachedClock(
            clock_shift,
            cb::mcbp::request::AdjustTimePayload::TimeType::TimeOfDay);
    userConnection->get(name, Vbid{0});
    userConnection->adjustMemcachedClock(
            0, cb::mcbp::request::AdjustTimePayload::TimeType::TimeOfDay);
}

/// Verify that appending to a document won't cause it to be deleted when we
/// exceed the max size
TEST_P(RegressionTest, MB10114_append_e2big_wont_delete_doc) {
    // Disable ewouldblock_engine - not wanted / needed for this MB regression
    // test.
    userConnection->configureEwouldBlockEngine(
            EWBEngineMode::Next_N, cb::engine_errc::would_block, 0);
    const std::string key{"mb-10114"};
    userConnection->store(key, Vbid{0}, "world");

    Document document;
    document.info.id = key;
    document.value.resize(mcd_env->getTestBucket().getMaximumDocSize() - 100);

    while (true) {
        try {
            userConnection->mutate(document, Vbid{0}, MutationType::Append);
        } catch (const ConnectionError& error) {
            if (error.isTooBig()) {
                break;
            }
            throw;
        }
    }

    // We should be able to delete it
    userConnection->remove(key, Vbid{0});
}

/**
 * Test that opcode 255 is rejected and the server doesn't crash
 */
TEST_P(RegressionTest, MB16333_opcode_255_detected) {
    auto rsp = userConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::Invalid});
    ASSERT_EQ(cb::mcbp::Status::UnknownCommand, rsp.getStatus());
}

/**
 * Test that a bad SASL auth doesn't crash the server.
 * It should be rejected with EINVAL.
 */
TEST_P(RegressionTest, MB16197_malformed_sasl_auth) {
    auto& conn = getConnection();
    auto rsp = conn.execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::SaslAuth, "PLAIN", std::string{"\0", 1}});
    ASSERT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
}

/// Check the subdoc validator also check the size of the internal spec
/// elements
TEST_P(RegressionTest, MB47151) {
    Frame frame;
    frame.payload = std::vector<uint8_t>{
            {0x80, 0xd0, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
             0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
             0x46, 0x46, 0x46, 0x46, 0x24, 0x00, 0x09, 0x00, 0x04, 0x00}};

    userConnection->sendFrame(frame);
    BinprotResponse rsp;
    userConnection->recvResponse(rsp);
    EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus()) << rsp.getDataView();
    EXPECT_EQ(R"({"error":{"context":"Multi lookup spec truncated"}})",
              rsp.getDataView());
}

/// Verify that we may request stats from "no-bucket"
TEST_P(RegressionTest, MB49126) {
    // The userConnection does not have access to the Stats privilege
    auto rsp = userConnection->execute(BinprotGetCmdTimerCommand{
            "@no bucket@", cb::mcbp::ClientOpcode::CreateBucket});
    ASSERT_EQ(cb::mcbp::Status::Eaccess, rsp.getStatus());

    // The adminConnection should be able to read that
    rsp = adminConnection->execute(BinprotGetCmdTimerCommand{
            "@no bucket@", cb::mcbp::ClientOpcode::CreateBucket});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
    ASSERT_NE(0, rsp.getDataJson()["total"].get<int>());

    // We should be able to request it from "our own" bucket, but it should
    // contain no data
    rsp = userConnection->execute(BinprotGetCmdTimerCommand{
            "", cb::mcbp::ClientOpcode::CreateBucket});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
    ASSERT_EQ(0, rsp.getDataJson()["total"].get<int>());

    adminConnection->executeInBucket(bucketName, [](auto& c) {
        auto rsp = adminConnection->execute(BinprotGetCmdTimerCommand{
                "", cb::mcbp::ClientOpcode::CreateBucket});
        ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
        ASSERT_EQ(0, rsp.getDataJson()["total"].get<int>());
    });

    // It should be part of /all/ but still be zero (as we still don't
    // have permission to read the no bucket)
    rsp = userConnection->execute(BinprotGetCmdTimerCommand{
            "/all/", cb::mcbp::ClientOpcode::CreateBucket});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
    ASSERT_EQ(0, rsp.getDataJson()["total"].get<int>());

    // But admin have access to no bucket
    rsp = adminConnection->execute(BinprotGetCmdTimerCommand{
            "/all/", cb::mcbp::ClientOpcode::CreateBucket});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
    ASSERT_NE(0, rsp.getDataJson()["total"].get<int>());
}

/// Verify that the correct username get logged as part of authentication
/// failures
TEST_P(RegressionTest, MB54848) {
    auto& conn = getConnection();
    try {
        conn.authenticate("jones", "invalidpassword");
        FAIL() << "authentication should fail";
    } catch (const std::exception& e) {
        EXPECT_STREQ("Authentication failed: Auth failure (32)", e.what());
    }

    try {
        conn.authenticate("nouser", "password");
        FAIL() << "authentication should fail";
    } catch (const std::exception& e) {
        EXPECT_STREQ("Authentication failed: Auth failure (32)", e.what());
    }

    try {
        conn.authenticate("UserWithoutProfile");
        FAIL() << "authentication should fail";
    } catch (const std::exception& e) {
        EXPECT_STREQ("Authentication failed: Auth failure (32)", e.what());
    }

    // logging is async, so we need to iterate over the files until we've fond
    // the entries
    const auto timeout =
            std::chrono::steady_clock::now() + std::chrono::seconds{30};
    bool found_no_user = false;
    bool found_wrong_passwd = false;
    bool found_no_profile = false;
    do {
        mcd_env->iterateLogLines([&found_no_user,
                                  &found_wrong_passwd,
                                  &found_no_profile](auto line) {
            if (line.find("User not found") != std::string_view::npos &&
                line.find("<ud>nouser</ud>") != std::string_view::npos) {
                found_no_user = true;
            } else if (line.find("Invalid password specified") !=
                               std::string_view::npos &&
                       line.find("<ud>jones</ud>") != std::string_view::npos) {
                found_wrong_passwd = true;
            } else if (line.find(
                               "User is not defined as a user in Couchbase") !=
                               std::string_view::npos &&
                       line.find("<ud>UserWithoutProfile</ud>") !=
                               std::string_view::npos) {
                found_no_profile = true;
            }

            return true;
        });
        if (found_no_user && found_wrong_passwd && found_no_profile) {
            // We've found everything we're waiting for
            return;
        }
    } while (std::chrono::steady_clock::now() < timeout);
    FAIL() << "Timed out waiting for log messages to appear";
}

TEST_P(RegressionTest, DISABLED_MB55754) {
    // Store the document
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.setVBucket(Vbid{0});
    cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                    cb::mcbp::subdoc::PathFlag::XattrPath,
                    "user",
                    R"({"Name":"John Doe"})");
    cmd.addMutation(cb::mcbp::ClientOpcode::Set, {}, "", R"({"foo":"bar"})");
    cmd.addDocFlag(cb::mcbp::subdoc::DocFlag::Mkdoc);
    auto rsp = userConnection->execute(cmd);
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus());

    std::size_t num_subdoc_races_before = 0;
    userConnection->stats([&num_subdoc_races_before](auto k, auto v) {
        if (k == "subdoc_update_races") {
            num_subdoc_races_before = std::stoul(v);
        }
    });

    // Return key_already_exists "forever" to make sure we give up
    auto conn = userConnection->clone();
    conn->authenticate("Luke");
    conn->selectBucket(bucketName);
    conn->setAutoRetryTmpfail(false);
    nlohmann::json conn_stats_before;
    conn->stats(
            [&conn_stats_before](auto k, auto v) {
                conn_stats_before = nlohmann::json::parse(v);
            },
            "connections self");

    conn->configureEwouldBlockEngine(
            EWBEngineMode::SlowCasMismatch, cb::engine_errc::success, 1000);
    rsp = conn->execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::Etmpfail, rsp.getStatus()) << rsp.getDataView();

    std::size_t num_subdoc_races_after = 0;
    userConnection->stats([&num_subdoc_races_after](auto k, auto v) {
        if (k == "subdoc_update_races") {
            num_subdoc_races_after = std::stoul(v);
        }
    });

    EXPECT_EQ(cb::limits::SubdocMaxAutoRetries,
              num_subdoc_races_after - num_subdoc_races_before);

    nlohmann::json conn_stats_after;
    conn->stats(
            [&conn_stats_after](auto k, auto v) {
                conn_stats_after = nlohmann::json::parse(v);
            },
            "connections self");

    // We should have yield at least a few times. Each cas mismatch should
    // take ~1ms. Each scheduling timeslot is 25ms (and we allow up to
    // 100 retries).
    const auto yields = conn_stats_after["yields"].get<int>() -
                        conn_stats_before["yields"].get<int>();
    EXPECT_LE(3, yields) << "After: " << conn_stats_after["yields"].get<int>()
                         << " Before: "
                         << conn_stats_before["yields"].get<int>();
}

/**
 * Verify that we return No Bucket on a connection which span RBAC refresh
 * and bound to "no bucket".
 *
 * See https://jira.issues.couchbase.com/browse/MB-64825
 */
TEST_P(RegressionTest, MB64825) {
    userConnection->unselectBucket();
    auto rsp = userConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::Get, "foo"});
    ASSERT_EQ(cb::mcbp::Status::NoBucket, rsp.getStatus());

    rsp = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::RbacRefresh});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus());

    rsp = userConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::Get, "foo"});
    ASSERT_EQ(cb::mcbp::Status::NoBucket, rsp.getStatus());
}
