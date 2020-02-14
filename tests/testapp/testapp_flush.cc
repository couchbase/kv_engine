/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include <array>

class FlushTest : public TestappClientTest {
protected:
    void SetUp() override {
        TestappClientTest::SetUp();

        conn = &getAdminConnection();
        conn->selectBucket("default");

        // Store our lone document
        Document doc;
        doc.info.datatype = cb::mcbp::Datatype::Raw;
        doc.info.cas = 0;
        doc.info.flags = 0;
        doc.info.id = key;
        conn->mutate(doc, vbid, MutationType::Set);
    }

    void TearDown() override {
        try {
            conn->remove(key, vbid, 0);
        } catch (ConnectionError& err) {
            if (!err.isNotFound()) {
                throw err;
            }
        }
        conn = nullptr;
        TestappClientTest::TearDown();
    }

    cb::mcbp::Status get() {
        BinprotGetCommand cmd;
        cmd.setKey(key);
        cmd.setVBucket(vbid);
        return conn->execute(cmd).getStatus();
    }

    const char *key = "test_flush";
    const Vbid vbid = Vbid(0);
    MemcachedConnection* conn = nullptr;
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         FlushTest,
                         ::testing::Values(TransportProtocols::McbpPlain,
                                           TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

TEST_P(FlushTest, Flush) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::Flush);
    BinprotGenericCommand command(cb::mcbp::ClientOpcode::Flush);
    const auto response = conn->execute(command);
    ASSERT_EQ(cb::mcbp::Status::Success, response.getStatus());
    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, get());
}

TEST_P(FlushTest, FlushQ) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::Flush);
    BinprotGenericCommand command(cb::mcbp::ClientOpcode::Flushq);
    conn->sendCommand(command);
    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, get());
}

TEST_P(FlushTest, FlushWithExtlen) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::Flush);
    BinprotGenericCommand command(cb::mcbp::ClientOpcode::Flush);
    command.setExtrasValue(uint32_t(htonl(0)));

    // Ensure it still works
    const auto response = conn->execute(command);
    ASSERT_EQ(cb::mcbp::Status::Success, response.getStatus());
    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, get());
}

TEST_P(FlushTest, FlushQWithExtlen) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::Flush);
    BinprotGenericCommand command(cb::mcbp::ClientOpcode::Flushq);
    command.setExtrasValue(static_cast<uint32_t>(htonl(0)));
    conn->sendCommand(command);

    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, get());
}

TEST_P(FlushTest, DISABLED_DelayedFlushNotSupported) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::Flush);
    BinprotGenericCommand command;

    command.setExtrasValue(static_cast<uint32_t>(htonl(2)));

    std::array<cb::mcbp::ClientOpcode, 2> commands{
            {cb::mcbp::ClientOpcode::Flush, cb::mcbp::ClientOpcode::Flushq}};
    for (auto op : commands) {
        const auto response = conn->execute(command.setOp(op));
        ASSERT_EQ(cb::mcbp::Status::NotSupported, response.getStatus());

        conn->reconnect();
        ASSERT_EQ(cb::mcbp::Status::Success, get());
    }
}
