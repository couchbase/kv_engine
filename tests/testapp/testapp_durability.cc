/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include <include/memcached/protocol_binary.h>
#include <mcbp/protocol/framebuilder.h>
#include <memcached/durability_spec.h>

using namespace cb::mcbp;
using cb::mcbp::request::FrameInfoId;

class DurabilityTest : public TestappClientTest {
protected:
    void SetUp() override {
        TestappTest::SetUp();
        getConnection().store(name, Vbid{0}, "123");
    }

    /**
     * Encode the provided durability requirements as specified in the
     * spec (see BinaryProtocol.md for the full description on how
     * framing extras should be encoded).
     *
     * If timeout is specified as 0 we'll use the "server default" and
     * not encode a value.
     */
    std::vector<uint8_t> encode(cb::durability::Requirements spec) {
        std::vector<uint8_t> ret(1); // First we'll need the id and length tag
        ret.push_back(uint8_t(spec.getLevel()));
        auto timeout = spec.getTimeout();
        if (!timeout.isDefault()) {
            ret.resize(ret.size() + sizeof(uint16_t));
            auto* ptr = reinterpret_cast<uint16_t*>(&ret[2]);
            *ptr = htons(timeout.get());
        }
        ret[0] = uint8_t((ret.size() - 1) << 4) |
                 uint8_t(FrameInfoId::DurabilityRequirement);
        return ret;
    }

    void executeCommand(ClientOpcode opcode,
                        cb::const_byte_buffer extras,
                        const std::string& value,
                        cb::mcbp::Status expectedStatus) {
        std::vector<uint8_t> buffer(1024);
        RequestBuilder builder({buffer.data(), buffer.size()});
        builder.setOpcode(opcode);
        builder.setMagic(Magic::AltClientRequest);
        builder.setFramingExtras(encode(cb::durability::Requirements()));
        builder.setExtras(extras);
        builder.setKey(
                {reinterpret_cast<const uint8_t*>(name.data()), name.size()});
        builder.setValue(
                {reinterpret_cast<const uint8_t*>(value.data()), value.size()});
        buffer.resize(builder.getFrame()->getFrame().size());

        Frame frame;
        frame.payload = std::move(buffer);

        auto& conn = getConnection();
        conn.sendFrame(frame);

        BinprotResponse resp;
        conn.recvResponse(resp);

        EXPECT_EQ(expectedStatus, resp.getStatus());
        EXPECT_NE(0xdeadbeef, ntohll(resp.getCas()));
    }

    void executeMutationCommand(ClientOpcode opcode) {
        executeCommand(opcode,
                       request::MutationPayload().getBuffer(),
                       "hello",
                       getExpectedStatus());
    }

    void executeArithmeticOperation(ClientOpcode opcode) {
        executeCommand(opcode,
                       request::ArithmeticPayload().getBuffer(),
                       "",
                       getExpectedStatus());
    }

    void executeAppendPrependCommand(ClientOpcode opcode) {
        executeCommand(opcode, {}, "world", getExpectedStatus());
    }

    void executeTouchOrGatCommand(ClientOpcode opcode) {
        executeCommand(opcode,
                       request::GatPayload().getBuffer(),
                       "",
                       cb::mcbp::Status::NotSupported);
    }

    cb::mcbp::Status getExpectedStatus() const {
        return (mcd_env->getTestBucket().supportsSyncWrites())
                       ? Status::Success
                       : Status::NotSupported;
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         DurabilityTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

/**
 * Only ep-engine supports the Durability Specification
 * Run all of the affected commands and verify that we return NotSupported for
 * memcached.
 */

TEST_P(DurabilityTest, AddMaybeSupported) {
    auto& conn = getConnection();
    conn.remove(name, Vbid{0});

    executeMutationCommand(ClientOpcode::Add);
}

TEST_P(DurabilityTest, SetMaybeSupported) {
    executeMutationCommand(ClientOpcode::Set);
}

TEST_P(DurabilityTest, ReplaceMaybeSupported) {
    executeMutationCommand(ClientOpcode::Replace);
}

TEST_P(DurabilityTest, DeleteMaybeSupported) {
    executeCommand(ClientOpcode::Delete, {}, {}, getExpectedStatus());
}

TEST_P(DurabilityTest, IncrementMaybeSupported) {
    executeArithmeticOperation(ClientOpcode::Increment);
}

TEST_P(DurabilityTest, DecrementMaybeSupported) {
    executeArithmeticOperation(ClientOpcode::Decrement);
}

TEST_P(DurabilityTest, AppendMaybeSupported) {
    executeAppendPrependCommand(ClientOpcode::Append);
}

TEST_P(DurabilityTest, PrependMaybeSupported) {
    executeAppendPrependCommand(ClientOpcode::Prepend);
}

TEST_P(DurabilityTest, TouchNotSupported) {
    executeTouchOrGatCommand(ClientOpcode::Touch);
}

TEST_P(DurabilityTest, GetAndTouchNotSupported) {
    executeTouchOrGatCommand(ClientOpcode::Gat);
}

TEST_P(DurabilityTest, AckResponseHandled) {
    if (mcd_env->getTestBucket().getName() != "default_engine") {
        // Will fail in EP engine if the DCP consumer isn't present
        return;
    }
    // Send our response to see if the executor accepts it. If not, it would
    // disconnect the connection
    std::vector<uint8_t> rspBuffer(1024);
    ResponseBuilder rspBuilder({rspBuffer.data(), rspBuffer.size()});
    rspBuilder.setOpcode(cb::mcbp::ClientOpcode::DcpSeqnoAcknowledged);
    rspBuilder.setMagic(Magic::AltClientResponse);
    rspBuilder.setFramingExtras(encode(cb::durability::Requirements()));
    rspBuilder.setKey(
            {reinterpret_cast<const uint8_t*>(name.data()), name.size()});
    rspBuffer.resize(rspBuilder.getFrame()->getFrame().size());

    Frame rspFrame;
    rspFrame.payload = std::move(rspBuffer);

    auto& conn = getConnection();
    conn.sendFrame(rspFrame);

    // Send something else, a GAT in this case, to test that the connection is
    // still up
    std::vector<uint8_t> reqBuffer(1024);
    RequestBuilder reqBuilder({reqBuffer.data(), reqBuffer.size()});
    std::string value = "";
    reqBuilder.setOpcode(ClientOpcode::Gat);
    reqBuilder.setMagic(Magic::AltClientRequest);
    reqBuilder.setFramingExtras(encode(cb::durability::Requirements()));
    reqBuilder.setExtras(request::GatPayload().getBuffer());
    reqBuilder.setKey(
            {reinterpret_cast<const uint8_t*>(name.data()), name.size()});
    reqBuilder.setValue(
            {reinterpret_cast<const uint8_t*>(value.data()), value.size()});
    reqBuffer.resize(reqBuilder.getFrame()->getFrame().size());

    Frame reqFrame;
    reqFrame.payload = std::move(reqBuffer);
    conn.sendFrame(reqFrame);

    BinprotResponse resp;
    conn.recvResponse(resp);

    EXPECT_EQ(Status::NotSupported, resp.getStatus());
    EXPECT_NE(0xdeadbeef, ntohll(resp.getCas()));
}

class SubdocDurabilityTest : public DurabilityTest {
protected:
    void SetUp() override {
        DurabilityTest::SetUp();
        // Store a JSON document instead
        getConnection().store(name,
                              Vbid{0},
                              R"({"tag":"value","array":[0,1,2],"counter":0})");
    }

    /**
     * The size of the frame extras section:
     * 1 byte containing the id and size
     * 1 byte containing the level
     * 2 bytes containing the duration timeout
     */
    static const size_t FrameExtrasSize = 4;

    void executeCommand(std::vector<uint8_t>& command,
                        cb::mcbp::Status expectedStatus) {
        // Resize the underlying buffer to have room for the frame extras..
        command.resize(command.size() + FrameExtrasSize);

        RequestBuilder builder({command.data(), command.size()}, true);
        builder.setMagic(Magic::AltClientRequest);
        builder.setFramingExtras(encode(cb::durability::Requirements()));
        // We might not have used the full frame encoding so adjust the size
        command.resize(builder.getFrame()->getFrame().size());

        Frame frame;
        frame.payload = std::move(command);

        auto& conn = getConnection();
        conn.sendFrame(frame);

        BinprotResponse resp;
        conn.recvResponse(resp);

        EXPECT_EQ(expectedStatus, resp.getStatus());
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         SubdocDurabilityTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

TEST_P(SubdocDurabilityTest, SubdocDictAddMaybeSupported) {
    BinprotSubdocCommand cmd(
            ClientOpcode::SubdocDictAdd, name, "foo", "5", SUBDOC_FLAG_MKDIR_P);
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload, getExpectedStatus());
}

TEST_P(SubdocDurabilityTest, SubdocDictUpsertMaybeSupported) {
    BinprotSubdocCommand cmd(ClientOpcode::SubdocDictUpsert, name, "foo", "5");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload, getExpectedStatus());
}

TEST_P(SubdocDurabilityTest, SubdocDeleteMaybeSupported) {
    BinprotSubdocCommand cmd(ClientOpcode::SubdocDelete, name, "tag");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload, getExpectedStatus());
}

TEST_P(SubdocDurabilityTest, SubdocReplaceMaybeSupported) {
    BinprotSubdocCommand cmd(ClientOpcode::SubdocReplace, name, "tag", "5");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload, getExpectedStatus());
}

TEST_P(SubdocDurabilityTest, SubdocArrayPushLastMaybeSupported) {
    BinprotSubdocCommand cmd(
            ClientOpcode::SubdocArrayPushLast, name, "array", "3");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload, getExpectedStatus());
}

TEST_P(SubdocDurabilityTest, SubdocArrayPushFirstMaybeSupported) {
    BinprotSubdocCommand cmd(
            ClientOpcode::SubdocArrayPushFirst, name, "array", "3");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload, getExpectedStatus());
}

TEST_P(SubdocDurabilityTest, SubdocArrayInsertMaybeSupported) {
    BinprotSubdocCommand cmd(
            ClientOpcode::SubdocArrayInsert, name, "array.[3]", "3");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload, getExpectedStatus());
}

TEST_P(SubdocDurabilityTest, SubdocArrayAddUniqueMaybeSupported) {
    BinprotSubdocCommand cmd(
            ClientOpcode::SubdocArrayAddUnique, name, "array", "6");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload, getExpectedStatus());
}

TEST_P(SubdocDurabilityTest, SubdocCounterMaybeSupported) {
    BinprotSubdocCommand cmd(ClientOpcode::SubdocCounter, name, "counter", "1");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload, getExpectedStatus());
}

TEST_P(SubdocDurabilityTest, SubdocMultiMutationMaybeSupported) {
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                    SUBDOC_FLAG_MKDIR_P,
                    "hello",
                    R"("world")");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload, getExpectedStatus());
}
