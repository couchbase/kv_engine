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
#include <algorithm>

class RegressionTest : public TestappClientTest {};

INSTANTIATE_TEST_CASE_P(TransportProtocols,
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

    BinprotGenericCommand cmd{PROTOCOL_BINARY_CMD_GET_CLUSTER_CONFIG, "", ""};
    BinprotResponse response;
    conn.executeCommand(cmd, response);
    EXPECT_FALSE(response.isSuccess());
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NO_BUCKET, response.getStatus());

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

    BinprotSubdocResponse resp;
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);

    cmd.addDocFlag(mcbp::subdoc::doc_flag::Add);
    cmd.addMutation(
            PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
            SUBDOC_FLAG_MKDIR_P,
            "cron_timers",
            R"({"callback_func": "NDtimerCallback", "payload": "doc_id_610"})");
    conn.executeCommand(cmd, resp);

    EXPECT_TRUE(resp.isSuccess()) << "Expected to work for Add";
    // If we try it one more time, it should fail as we want to
    // _ADD_ the doc if it isn't there
    conn.executeCommand(cmd, resp);
    EXPECT_FALSE(resp.isSuccess()) << "Add should fail when it isn't there";
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, resp.getStatus());
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
    BinprotSubdocResponse resp;
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);

    // Configure the ewouldblock_engine to inject fake NOT STORED
    // failure for the 3rd call (i.e. the 1st engine->store() attempt).
    conn.configureEwouldBlockEngine(
            EWBEngineMode::Sequence,
            ENGINE_NOT_STORED,
            0xffffffc4 /* <3 MSBytes all-ones>, 0b11,000,100 */);
    cmd.addDocFlag(mcbp::subdoc::doc_flag::Mkdoc);

    cmd.addMutation(
            PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
            SUBDOC_FLAG_MKDIR_P,
            "cron_timers",
            R"({"callback_func": "NDtimerCallback", "payload": "doc_id_610"})");
    conn.executeCommand(cmd, resp);

    EXPECT_TRUE(resp.isSuccess());
    // Reset connection to make sure we're not affected by any ewb logic
    conn = getConnection();
    conn.executeCommand(cmd, resp);
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

    BinprotSubdocMultiLookupResponse multiResp;
    conn.executeCommand(cmd, multiResp);

    auto& results = multiResp.getResults();

    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, multiResp.getStatus());
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, results[0].status);
    const auto exptime = std::stol(results[0].value);
    EXPECT_LT(0, exptime);

    document.value = " world";
    document.info.expiration = 0;
    conn.mutate(document, Vbid(0), MutationType::Append);

    conn.executeCommand(cmd, multiResp);

    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, multiResp.getStatus());
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, results[0].status);
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
