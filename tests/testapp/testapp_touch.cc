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

#include <algorithm>
#include <platform/compress.h>
#include <include/memcached/protocol_binary.h>

class TouchTest : public TestappClientTest {
public:
    void SetUp() {
        TestappClientTest::SetUp();
        document.info.cas = mcbp::cas::Wildcard;
        document.info.flags = 0xcaffee;
        document.info.id = name;
        document.value = to_string(memcached_cfg, false);
    }

protected:
    Document document;

    size_t get_cmd_counter(const std::string& name, MemcachedConnection& conn);

    void testHit(bool quiet);
    void testMiss(bool quiet);

};

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        TouchTest,
                        ::testing::Values(TransportProtocols::McbpPlain,
                                          TransportProtocols::McbpSsl),
                        ::testing::PrintToStringParamName());

size_t TouchTest::get_cmd_counter(const std::string& name,
                                  MemcachedConnection& conn) {
    auto stats = conn.statsMap("");
    const auto iter = stats.find(name);
    if (iter != stats.cend()) {
        return size_t(std::stoi(iter->second));
    } else {
        throw std::logic_error("get_cmd_counter: No entry for: " + name);
    }
}

void TouchTest::testHit(bool quiet) {
    auto& conn = getConnection();
    const auto info = conn.mutate(document, Vbid(0), MutationType::Add);

    // Verify that we can set the expiry time to the same value without
    // getting a new cas value generated
    BinprotGetAndTouchCommand cmd;
    cmd.setQuiet(quiet);
    cmd.setKey(name);
    cmd.setExpirytime(0);
    conn.sendCommand(cmd);

    BinprotGetAndTouchResponse rsp;
    conn.recvResponse(rsp);

    EXPECT_TRUE(rsp.isSuccess());
    EXPECT_EQ(info.cas, rsp.getCas());

    // Verify that get_hits and the cas get updated with the gat calls
    const auto before = get_cmd_counter("get_hits", conn);
    cmd.setExpirytime(10);
    conn.sendCommand(cmd);

    conn.recvResponse(rsp);

    EXPECT_TRUE(rsp.isSuccess());
    EXPECT_EQ(0xcaffee, rsp.getDocumentFlags());
    EXPECT_EQ(to_string(memcached_cfg, false), rsp.getDataString());
    EXPECT_NE(info.cas, rsp.getCas());

    // The stat should have been incremented with 1
    const auto after = get_cmd_counter("get_hits", conn);
    EXPECT_EQ(before + 1, after);
}

void TouchTest::testMiss(bool quiet) {
    auto& conn = getConnection();
    const auto before = get_cmd_counter("get_misses", conn);
    BinprotGetAndTouchCommand cmd;
    cmd.setQuiet(quiet);
    cmd.setKey(name);
    cmd.setExpirytime(10);
    conn.sendCommand(cmd);

    if (quiet) {
        // Send a noop command as not found shoudn't return anything...
        conn.sendCommand(BinprotGenericCommand{PROTOCOL_BINARY_CMD_NOOP});
    }

    BinprotResponse rsp;
    conn.recvResponse(rsp);

    if (quiet) {
        // this should be the NOOP
        EXPECT_EQ(PROTOCOL_BINARY_CMD_NOOP, rsp.getOp());
        EXPECT_TRUE(rsp.isSuccess());
    } else {
        // this should be a ENOENT
        EXPECT_EQ(PROTOCOL_BINARY_CMD_GAT, rsp.getOp());
        EXPECT_FALSE(rsp.isSuccess());
        EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    }

    const auto after = get_cmd_counter("get_misses", conn);
    EXPECT_EQ(before + 1, after);
}

TEST_P(TouchTest, Gat_Hit) {
    testHit(false);
}

TEST_P(TouchTest, Gat_Miss) {
    testMiss(false);
}

TEST_P(TouchTest, Gatq_Hit) {
    testHit(true);
}

TEST_P(TouchTest, Gatq_Miss) {
    testMiss(true);
}

TEST_P(TouchTest, Touch_Hit) {
    auto& conn = getConnection();
    const auto info = conn.mutate(document, Vbid(0), MutationType::Add);

    // Verify that we can set the expiry time to the same value without
    // getting a new cas value generated (we're using 0 as the value)
    BinprotTouchCommand cmd;
    cmd.setKey(name);
    conn.sendCommand(cmd);

    BinprotTouchResponse rsp;
    conn.recvResponse(rsp);

    EXPECT_TRUE(rsp.isSuccess());
    EXPECT_EQ(info.cas, rsp.getCas());

    // Verify that we can set it to something else and get a new CAS
    cmd.setExpirytime(10);
    conn.sendCommand(cmd);
    conn.recvResponse(rsp);
    EXPECT_TRUE(rsp.isSuccess());
    EXPECT_NE(info.cas, rsp.getCas());
    EXPECT_TRUE(rsp.getDataString().empty());
}

TEST_P(TouchTest, Touch_Miss) {
    auto& conn = getConnection();
    BinprotTouchCommand cmd;
    cmd.setKey(name);
    cmd.setExpirytime(10);
    conn.sendCommand(cmd);

    BinprotTouchResponse rsp;
    conn.recvResponse(rsp);
    EXPECT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
}
