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

using namespace cb::mcbp;
using cb::mcbp::request::FrameInfoId;

class DurabilityTest : public TestappClientTest {
protected:
    void SetUp() override {
        TestappTest::SetUp();
        store_document(name, "123");
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
        if (timeout != 0) {
            ret.resize(ret.size() + sizeof(uint16_t));
            auto* ptr = reinterpret_cast<uint16_t*>(&ret[2]);
            *ptr = htons(timeout);
        }
        ret[0] = uint8_t((ret.size() - 1) << 4) |
                 uint8_t(FrameInfoId::DurabilityRequirement);
        return ret;
    }

    void executeCommand(ClientOpcode opcode,
                        cb::const_byte_buffer extras,
                        const std::string& value) {
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

        EXPECT_FALSE(resp.isSuccess());
        EXPECT_EQ(Status::NotSupported, resp.getStatus());
    }

    void executeMutationCommand(ClientOpcode opcode) {
        executeCommand(opcode, request::MutationPayload().getBuffer(), "hello");
    }

    void executeArithmeticOperation(ClientOpcode opcode) {
        executeCommand(opcode, request::ArithmeticPayload().getBuffer(), "");
    }

    void executeAppendPrependCommand(ClientOpcode opcode) {
        executeCommand(opcode, {}, "world");
    }

    void executeTouchOrGatCommand(ClientOpcode opcode) {
        executeCommand(opcode, request::GatPayload().getBuffer(), "");
    }
};

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        DurabilityTest,
                        ::testing::Values(TransportProtocols::McbpPlain),
                        ::testing::PrintToStringParamName());

/**
 * None of the backends currently support the Durability Specification
 * Run all of the affected commands and verify that we return NotSupported
 */

TEST_P(DurabilityTest, AddNotSupported) {
    auto& conn = getConnection();
    conn.remove(name, Vbid{0});
    executeMutationCommand(ClientOpcode::Add);
}

TEST_P(DurabilityTest, SetNotSupported) {
    executeMutationCommand(ClientOpcode::Set);
}

TEST_P(DurabilityTest, ReplaceNotSupported) {
    executeMutationCommand(ClientOpcode::Replace);
}

TEST_P(DurabilityTest, IncrementNotSupported) {
    executeArithmeticOperation(ClientOpcode::Increment);
}

TEST_P(DurabilityTest, DecrementNotSupported) {
    executeArithmeticOperation(ClientOpcode::Decrement);
}

TEST_P(DurabilityTest, AppendNotSupported) {
    executeAppendPrependCommand(ClientOpcode::Append);
}

TEST_P(DurabilityTest, PrependNotSupported) {
    executeAppendPrependCommand(ClientOpcode::Prepend);
}

TEST_P(DurabilityTest, TouchNotSupported) {
    executeTouchOrGatCommand(ClientOpcode::Touch);
}

TEST_P(DurabilityTest, GetAndTouchNotSupported) {
    executeTouchOrGatCommand(ClientOpcode::Gat);
}

class SubdocDurabilityTest : public DurabilityTest {
protected:
    void SetUp() override {
        DurabilityTest::SetUp();
        // Store a JSON document instead
        store_document(name, R"({"tag":"value","array":[0,1,2],"counter":0})");
    }

    /**
     * The size of the frame extras section:
     * 1 byte containing the id and size
     * 1 byte containing the level
     * 2 bytes containing the duration timeout
     */
    static const size_t FrameExtrasSize = 4;

    void executeCommand(std::vector<uint8_t>& command) {
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

        EXPECT_FALSE(resp.isSuccess());
        EXPECT_EQ(Status::NotSupported, resp.getStatus())
                << resp.getDataString();
    }
};

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        SubdocDurabilityTest,
                        ::testing::Values(TransportProtocols::McbpPlain),
                        ::testing::PrintToStringParamName());

TEST_P(SubdocDurabilityTest, SubdocDictAddNotSupported) {
    BinprotSubdocCommand cmd(
            ClientOpcode::SubdocDictAdd, name, "foo", "5", SUBDOC_FLAG_MKDIR_P);
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload);
}

TEST_P(SubdocDurabilityTest, SubdocDictUpsertNotSupported) {
    BinprotSubdocCommand cmd(ClientOpcode::SubdocDictUpsert, name, "foo", "5");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload);
}

TEST_P(SubdocDurabilityTest, SubdocDeleteNotSupported) {
    BinprotSubdocCommand cmd(ClientOpcode::SubdocDelete, name, "tag");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload);
}

TEST_P(SubdocDurabilityTest, SubdocReplaceNotSupported) {
    BinprotSubdocCommand cmd(ClientOpcode::SubdocReplace, name, "tag", "5");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload);
}

TEST_P(SubdocDurabilityTest, SubdocArrayPushLastNotSupported) {
    BinprotSubdocCommand cmd(
            ClientOpcode::SubdocArrayPushLast, name, "array", "3");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload);
}

TEST_P(SubdocDurabilityTest, SubdocArrayPushFirstNotSupported) {
    BinprotSubdocCommand cmd(
            ClientOpcode::SubdocArrayPushFirst, name, "array", "3");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload);
}

TEST_P(SubdocDurabilityTest, SubdocArrayInsertNotSupported) {
    BinprotSubdocCommand cmd(
            ClientOpcode::SubdocArrayInsert, name, "array.[3]", "3");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload);
}

TEST_P(SubdocDurabilityTest, SubdocArrayAddUniqueNotSupported) {
    BinprotSubdocCommand cmd(
            ClientOpcode::SubdocArrayAddUnique, name, "array", "6");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload);
}

TEST_P(SubdocDurabilityTest, SubdocCounterNotSupported) {
    BinprotSubdocCommand cmd(ClientOpcode::SubdocCounter, name, "counter", "1");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload);
}

TEST_P(SubdocDurabilityTest, SubdocMultiMutationNotSupported) {
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                    SUBDOC_FLAG_MKDIR_P,
                    "hello",
                    R"("world")");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload);
}
