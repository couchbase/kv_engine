/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp.h"
#include "testapp_client_test.h"

#include <algorithm>
#include <include/memcached/protocol_binary.h>

class TouchTest : public TestappClientTest {
public:
    void SetUp() override {
        TestappClientTest::SetUp();
        document.info.cas = cb::mcbp::cas::Wildcard;
        document.info.flags = 0xcaffee;
        document.info.id = name;
        document.value = memcached_cfg.dump();
    }

protected:
    Document document;

    size_t get_cmd_counter(const std::string& name);

    void testHit(bool quiet);
    void testMiss(bool quiet);

    void testGatAndTouch(const std::string& value);
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         TouchTest,
                         ::testing::Values(TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

size_t TouchTest::get_cmd_counter(const std::string& name) {
    auto stats = userConnection->statsMap("");
    const auto iter = stats.find(name);
    if (iter != stats.cend()) {
        return size_t(std::stoi(iter->second));
    } else {
        throw std::logic_error("get_cmd_counter: No entry for: " + name);
    }
}

void TouchTest::testHit(bool quiet) {
    const auto info =
            userConnection->mutate(document, Vbid(0), MutationType::Add);

    // Verify that we can set the expiry time to the same value without
    // getting a new cas value generated
    BinprotGetAndTouchCommand cmd(name, Vbid{0}, 0);
    cmd.setQuiet(quiet);
    userConnection->sendCommand(cmd);

    BinprotGetAndTouchResponse rsp;
    userConnection->recvResponse(rsp);

    EXPECT_TRUE(rsp.isSuccess());
    EXPECT_EQ(info.cas, rsp.getCas());

    // Verify that get_hits and the cas get updated with the gat calls
    const auto before = get_cmd_counter("get_hits");
    cmd.setExpirytime(10);
    userConnection->sendCommand(cmd);

    userConnection->recvResponse(rsp);

    EXPECT_TRUE(rsp.isSuccess());
    EXPECT_EQ(0xcaffee, rsp.getDocumentFlags());
    EXPECT_EQ(memcached_cfg.dump(), rsp.getDataString());
    EXPECT_NE(info.cas, rsp.getCas());

    // The stat should have been incremented with 1
    const auto after = get_cmd_counter("get_hits");
    EXPECT_EQ(before + 1, after);
}

void TouchTest::testMiss(bool quiet) {
    const auto before = get_cmd_counter("get_misses");
    BinprotGetAndTouchCommand cmd(name, Vbid{0}, 10);
    cmd.setQuiet(quiet);
    userConnection->sendCommand(cmd);

    if (quiet) {
        // Send a noop command as not found shoudn't return anything...
        userConnection->sendCommand(
                BinprotGenericCommand{cb::mcbp::ClientOpcode::Noop});
    }

    BinprotResponse rsp;
    userConnection->recvResponse(rsp);

    if (quiet) {
        // this should be the NOOP
        EXPECT_EQ(cb::mcbp::ClientOpcode::Noop, rsp.getOp());
        EXPECT_TRUE(rsp.isSuccess());
    } else {
        // this should be a ENOENT
        EXPECT_EQ(cb::mcbp::ClientOpcode::Gat, rsp.getOp());
        EXPECT_FALSE(rsp.isSuccess());
        EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    }

    const auto after = get_cmd_counter("get_misses");
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
    const auto info =
            userConnection->mutate(document, Vbid(0), MutationType::Add);

    // Verify that we can set the expiry time to the same value without
    // getting a new cas value generated (we're using 0 as the value)
    BinprotTouchCommand cmd;
    cmd.setKey(name);
    userConnection->sendCommand(cmd);

    BinprotTouchResponse rsp;
    userConnection->recvResponse(rsp);

    EXPECT_TRUE(rsp.isSuccess());
    EXPECT_EQ(info.cas, rsp.getCas());

    // Verify that we can set it to something else and get a new CAS
    cmd.setExpirytime(10);
    userConnection->sendCommand(cmd);
    userConnection->recvResponse(rsp);
    EXPECT_TRUE(rsp.isSuccess());
    EXPECT_NE(info.cas, rsp.getCas());
    EXPECT_TRUE(rsp.getDataString().empty());
}

TEST_P(TouchTest, Touch_Miss) {
    BinprotTouchCommand cmd;
    cmd.setKey(name);
    cmd.setExpirytime(10);
    userConnection->sendCommand(cmd);

    BinprotTouchResponse rsp;
    userConnection->recvResponse(rsp);
    EXPECT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
}

void TouchTest::testGatAndTouch(const std::string& input_doc) {
    document.value = input_doc;
    userConnection->mutate(document, Vbid(0), MutationType::Add);
    auto resp = BinprotGetAndTouchResponse{userConnection->execute(
            BinprotGetAndTouchCommand{name, Vbid{0}, 0})};
    EXPECT_TRUE(resp.isSuccess());
    auto value = resp.getData();
    std::string val(reinterpret_cast<const char*>(value.data()), value.size());
    EXPECT_EQ(document.value, val);
    resp = BinprotGetAndTouchResponse{
            userConnection->execute(BinprotTouchCommand{name})};
    EXPECT_TRUE(resp.isSuccess());
    EXPECT_TRUE(resp.getData().empty());
}

TEST_P(TouchTest, SmallValues) {
    testGatAndTouch("hello world");
}

TEST_P(TouchTest, LargeValues) {
    std::string value;
    value.resize(512 * 1024);
    std::fill(value.begin(), value.end(), 'd');
    testGatAndTouch("hello world");
}
