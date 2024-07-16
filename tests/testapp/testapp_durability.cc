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

#include <include/memcached/protocol_binary.h>
#include <mcbp/protocol/framebuilder.h>
#include <memcached/durability_spec.h>

using namespace cb::mcbp;
using request::FrameInfoId;

class DurabilityTest : public TestappClientTest {
protected:
    void SetUp() override {
        TestappTest::SetUp();
        userConnection->store(name, Vbid{0}, "123");
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
                        Status expectedStatus) {
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

        userConnection->sendFrame(frame);

        BinprotResponse resp;
        userConnection->recvResponse(resp);

        EXPECT_EQ(expectedStatus, resp.getStatus());
        EXPECT_NE(0xdeadbeef, ntohll(resp.getCas()));
    }

    void executeMutationCommand(ClientOpcode opcode) {
        executeCommand(opcode,
                       request::MutationPayload().getBuffer(),
                       "hello",
                       Status::Success);
    }

    void executeArithmeticOperation(ClientOpcode opcode) {
        executeCommand(opcode,
                       request::ArithmeticPayload().getBuffer(),
                       "",
                       Status::Success);
    }

    void executeAppendPrependCommand(ClientOpcode opcode) {
        executeCommand(opcode, {}, "world", Status::Success);
    }

    void executeTouchOrGatCommand(ClientOpcode opcode) {
        executeCommand(opcode,
                       request::GatPayload().getBuffer(),
                       "",
                       Status::NotSupported);
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
    userConnection->remove(name, Vbid{0});
    executeMutationCommand(ClientOpcode::Add);
}

TEST_P(DurabilityTest, SetMaybeSupported) {
    executeMutationCommand(ClientOpcode::Set);
}

TEST_P(DurabilityTest, ReplaceMaybeSupported) {
    executeMutationCommand(ClientOpcode::Replace);
}

TEST_P(DurabilityTest, DeleteMaybeSupported) {
    executeCommand(ClientOpcode::Delete, {}, {}, Status::Success);
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

class SubdocDurabilityTest : public DurabilityTest {
protected:
    void SetUp() override {
        DurabilityTest::SetUp();
        // Store a JSON document instead
        userConnection->store(name,
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

    void executeCommand(std::vector<uint8_t>& command, Status expectedStatus) {
        // Resize the underlying buffer to have room for the frame extras..
        command.resize(command.size() + FrameExtrasSize);

        RequestBuilder builder({command.data(), command.size()}, true);
        builder.setMagic(Magic::AltClientRequest);
        builder.setFramingExtras(encode(cb::durability::Requirements()));
        // We might not have used the full frame encoding so adjust the size
        command.resize(builder.getFrame()->getFrame().size());

        Frame frame;
        frame.payload = std::move(command);

        userConnection->sendFrame(frame);

        BinprotResponse resp;
        userConnection->recvResponse(resp);

        EXPECT_EQ(expectedStatus, resp.getStatus());
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         SubdocDurabilityTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

TEST_P(SubdocDurabilityTest, SubdocDictAddMaybeSupported) {
    BinprotSubdocCommand cmd(ClientOpcode::SubdocDictAdd,
                             name,
                             "foo",
                             "5",
                             cb::mcbp::subdoc::PathFlag::Mkdir_p);
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload, Status::Success);
}

TEST_P(SubdocDurabilityTest, SubdocDictUpsertMaybeSupported) {
    BinprotSubdocCommand cmd(ClientOpcode::SubdocDictUpsert, name, "foo", "5");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload, Status::Success);
}

TEST_P(SubdocDurabilityTest, SubdocDeleteMaybeSupported) {
    BinprotSubdocCommand cmd(ClientOpcode::SubdocDelete, name, "tag");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload, Status::Success);
}

TEST_P(SubdocDurabilityTest, SubdocReplaceMaybeSupported) {
    BinprotSubdocCommand cmd(ClientOpcode::SubdocReplace, name, "tag", "5");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload, Status::Success);
}

TEST_P(SubdocDurabilityTest, SubdocArrayPushLastMaybeSupported) {
    BinprotSubdocCommand cmd(
            ClientOpcode::SubdocArrayPushLast, name, "array", "3");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload, Status::Success);
}

TEST_P(SubdocDurabilityTest, SubdocArrayPushFirstMaybeSupported) {
    BinprotSubdocCommand cmd(
            ClientOpcode::SubdocArrayPushFirst, name, "array", "3");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload, Status::Success);
}

TEST_P(SubdocDurabilityTest, SubdocArrayInsertMaybeSupported) {
    BinprotSubdocCommand cmd(
            ClientOpcode::SubdocArrayInsert, name, "array.[3]", "3");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload, Status::Success);
}

TEST_P(SubdocDurabilityTest, SubdocArrayAddUniqueMaybeSupported) {
    BinprotSubdocCommand cmd(
            ClientOpcode::SubdocArrayAddUnique, name, "array", "6");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload, Status::Success);
}

TEST_P(SubdocDurabilityTest, SubdocCounterMaybeSupported) {
    BinprotSubdocCommand cmd(ClientOpcode::SubdocCounter, name, "counter", "1");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload, Status::Success);
}

TEST_P(SubdocDurabilityTest, SubdocMultiMutationMaybeSupported) {
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                    cb::mcbp::subdoc::PathFlag::Mkdir_p,
                    "hello",
                    R"("world")");
    std::vector<uint8_t> payload;
    cmd.encode(payload);
    executeCommand(payload, Status::Success);
}
