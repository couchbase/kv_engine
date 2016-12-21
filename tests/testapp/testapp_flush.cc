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

class FlushTest : public TestappClientTest {
protected:
    virtual void SetUp() {
        TestappClientTest::SetUp();

        conn = static_cast<MemcachedBinprotConnection*>(&getConnection());

        // Store our lone document
        Document doc;
        doc.info.compression = Greenstack::Compression::None;
        doc.info.datatype = Greenstack::Datatype::Raw;
        doc.info.cas = 0;
        doc.info.flags = 0;
        doc.info.id = key;
        conn->mutate(doc, vbid, Greenstack::MutationType::Set);
    }

    virtual void TearDown() {
        try {
            conn->remove(key, vbid, 0);
        } catch (BinprotConnectionError& err) {
            if (!err.isNotFound()) {
                throw err;
            }
        }
        conn = nullptr;
        TestappClientTest::TearDown();
    }

    protocol_binary_response_status get(BinprotResponse& resp) {
        resp.clear();
        BinprotGetCommand cmd;
        cmd.setKey(key);
        cmd.setVBucket(vbid);
        conn->executeCommand(cmd, resp);
        return resp.getStatus();
    }

    const char *key = "test_flush";
    const uint16_t vbid = 0;
    MemcachedBinprotConnection* conn = nullptr;
};

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        FlushTest,
                        ::testing::Values(TransportProtocols::McbpPlain,
                                          TransportProtocols::McbpIpv6Plain,
                                          TransportProtocols::McbpSsl,
                                          TransportProtocols::McbpIpv6Ssl
                                         ),
                        ::testing::PrintToStringParamName());

TEST_P(FlushTest, Flush) {
    BinprotGenericCommand command(PROTOCOL_BINARY_CMD_FLUSH);
    BinprotResponse response;
    conn->executeCommand(command, response);
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, response.getStatus());
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, get(response));
}

TEST_P(FlushTest, FlushQ) {
    BinprotGenericCommand command(PROTOCOL_BINARY_CMD_FLUSHQ);
    BinprotResponse response;
    conn->sendCommand(command);
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, get(response));
}

TEST_P(FlushTest, FlushWithExtlen) {
    BinprotGenericCommand command(PROTOCOL_BINARY_CMD_FLUSH);
    command.setExtrasValue(uint32_t(htonl(0)));

    // Ensure it still works
    BinprotResponse response;
    conn->executeCommand(command, response);
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, response.getStatus());
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, get(response));
}

TEST_P(FlushTest, FlushQWithExtlen) {
    BinprotGenericCommand command(PROTOCOL_BINARY_CMD_FLUSHQ);
    BinprotResponse response;
    command.setExtrasValue(static_cast<uint32_t>(htonl(0)));
    conn->sendCommand(command);

    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, get(response));
}

TEST_P(FlushTest, DelayedFlushNotSupported) {
    BinprotGenericCommand command;
    BinprotResponse response;

    command.setExtrasValue(static_cast<uint32_t>(htonl(2)));

    std::array<protocol_binary_command, 2> commands{{
        PROTOCOL_BINARY_CMD_FLUSH, PROTOCOL_BINARY_CMD_FLUSHQ
    }};
    for (auto op : commands) {
        conn->executeCommand(command.setOp(op), response);
        ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, response.getStatus());

        conn->reconnect();
        ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, get(response));
    }
}
