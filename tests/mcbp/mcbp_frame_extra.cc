/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "mcbp_test.h"

#include <mcbp/protocol/framebuilder.h>

using cb::mcbp::ClientOpcode;
using cb::mcbp::Status;
using cb::mcbp::request::FrameInfoId;

class FrameExtrasValidatorTests : public mcbp::test::ValidatorTest {
public:
    FrameExtrasValidatorTests()
        : ValidatorTest(false), builder({blob, sizeof(blob)}) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        builder.setMagic(cb::mcbp::Magic::AltClientRequest);
        builder.setOpcode(ClientOpcode::Set);
        cb::mcbp::request::MutationPayload ext;
        builder.setExtras(ext.getBuffer());
        builder.setKey("foo");
    }

protected:
    std::vector<uint8_t> encodeFrameInfo(FrameInfoId id,
                                         cb::const_byte_buffer payload) {
        std::vector<uint8_t> result;

        auto idbits = static_cast<uint16_t>(id);
        if (idbits < 0x0f) {
            result.emplace_back(uint8_t(idbits << 4u));
        } else {
            result.emplace_back(0xf0);
            result.emplace_back(uint8_t(idbits - 0x0f));
        }

        if (payload.size() < 0x0f) {
            result[0] |= uint8_t(payload.size());
        } else {
            result[0] |= 0x0fu;
            result.emplace_back(uint8_t(payload.size() - 0x0f));
        }

        std::copy(payload.begin(), payload.end(), std::back_inserter(result));
        return result;
    }

protected:
    cb::mcbp::RequestBuilder builder;
};

TEST_F(FrameExtrasValidatorTests, Barrier) {
    auto fe = encodeFrameInfo(FrameInfoId::Barrier, {});
    builder.setFramingExtras({fe.data(), fe.size()});
    EXPECT_EQ(Status::Success, validate(ClientOpcode::Set, blob));
}

TEST_F(FrameExtrasValidatorTests, BarrierInvalidSize) {
    auto fe = encodeFrameInfo(FrameInfoId::Barrier, {blob, 1});
    builder.setFramingExtras({fe.data(), fe.size()});
    EXPECT_EQ("Barrier should not contain value",
              validate_error_context(ClientOpcode::Set, blob, Status::Einval));
}

TEST_F(FrameExtrasValidatorTests, DurabilityRequirement) {
    uint8_t level = 1;
    auto fe = encodeFrameInfo(FrameInfoId::DurabilityRequirement, {&level, 1});
    builder.setFramingExtras({fe.data(), fe.size()});
    EXPECT_EQ(Status::Success, validate(ClientOpcode::Set, blob));
}

TEST_F(FrameExtrasValidatorTests, DurabilityRequirementInvalidLevel) {
    uint8_t level = 0;
    auto fe = encodeFrameInfo(FrameInfoId::DurabilityRequirement, {&level, 1});
    builder.setFramingExtras({fe.data(), fe.size()});
    EXPECT_EQ(Status::DurabilityInvalidLevel,
              validate(ClientOpcode::Set, blob));
}

TEST_F(FrameExtrasValidatorTests, DurabilityRequirementInvalidCommand) {
    uint8_t level = 1;
    auto fe = encodeFrameInfo(FrameInfoId::DurabilityRequirement, {&level, 1});
    builder.setFramingExtras({fe.data(), fe.size()});
    builder.setOpcode(ClientOpcode::Get);
    builder.setExtras(cb::const_byte_buffer{});
    EXPECT_EQ("The requested command does not support durability requirements",
              validate_error_context(ClientOpcode::Get, blob, Status::Einval));
}

TEST_F(FrameExtrasValidatorTests, DurabilityRequirementInvalidSize) {
    uint8_t level[10] = {1, 0, 1};
    auto fe = encodeFrameInfo(FrameInfoId::DurabilityRequirement, {level, 2});
    builder.setFramingExtras({fe.data(), fe.size()});
    EXPECT_EQ("Invalid sized buffer provided: 2",
              validate_error_context(ClientOpcode::Set, blob, Status::Einval));

    // size 3 == level + timeout
    fe = encodeFrameInfo(FrameInfoId::DurabilityRequirement, {level, 3});
    builder.setFramingExtras({fe.data(), fe.size()});
    EXPECT_EQ(Status::Success, validate(ClientOpcode::Set, blob));

    // size 4 invalid
    fe = encodeFrameInfo(FrameInfoId::DurabilityRequirement, {level, 4});
    builder.setFramingExtras({fe.data(), fe.size()});
    EXPECT_EQ("Invalid sized buffer provided: 4",
              validate_error_context(ClientOpcode::Set, blob, Status::Einval));
}

TEST_F(FrameExtrasValidatorTests, DcpStreamId) {
    uint16_t id = 0;

    auto fe = encodeFrameInfo(
            FrameInfoId::DcpStreamId,
            {reinterpret_cast<const uint8_t*>(&id), sizeof(id)});
    builder.setFramingExtras({fe.data(), fe.size()});
    EXPECT_EQ(Status::Success, validate(ClientOpcode::Set, blob));
}

TEST_F(FrameExtrasValidatorTests, DcpStreamIdInvalidSize) {
    uint32_t id = 0;
    auto fe = encodeFrameInfo(
            FrameInfoId::DcpStreamId,
            {reinterpret_cast<const uint8_t*>(&id), sizeof(id)});
    builder.setFramingExtras({fe.data(), fe.size()});
    EXPECT_EQ("DcpStreamId invalid size:4",
              validate_error_context(ClientOpcode::Set, blob, Status::Einval));
}

TEST_F(FrameExtrasValidatorTests, OpenTracingContext) {
    const std::string context{"context"};

    auto fe = encodeFrameInfo(
            FrameInfoId::OpenTracingContext,
            {reinterpret_cast<const uint8_t*>(&context), context.size()});
    builder.setFramingExtras({fe.data(), fe.size()});
    EXPECT_EQ(Status::Success, validate(ClientOpcode::Set, blob));
}

TEST_F(FrameExtrasValidatorTests, OpenTracingContextInvalidSize) {
    auto fe = encodeFrameInfo(FrameInfoId::OpenTracingContext, {});
    builder.setFramingExtras({fe.data(), fe.size()});
    EXPECT_EQ("OpenTracingContext cannot be empty",
              validate_error_context(ClientOpcode::Set, blob, Status::Einval));
}

TEST_F(FrameExtrasValidatorTests, PreserveTtl) {
    auto fe = encodeFrameInfo(FrameInfoId::PreserveTtl, {});
    builder.setFramingExtras({fe.data(), fe.size()});
    EXPECT_EQ(Status::Success, validate(ClientOpcode::Set, blob));
}

TEST_F(FrameExtrasValidatorTests, PreserveTtlInvalidSize) {
    auto fe = encodeFrameInfo(FrameInfoId::PreserveTtl, {blob, 1});
    builder.setFramingExtras({fe.data(), fe.size()});
    EXPECT_EQ("PreserveTtl should not contain value",
              validate_error_context(ClientOpcode::Set, blob, Status::Einval));
}

TEST_F(FrameExtrasValidatorTests, UnknownFrameId) {
    auto fe = encodeFrameInfo(FrameInfoId(0xff), {});
    builder.setFramingExtras({fe.data(), fe.size()});
    EXPECT_EQ(Status::UnknownFrameInfo, validate(ClientOpcode::Set, blob));
}

TEST_F(FrameExtrasValidatorTests, BufferOverflow) {
    std::vector<uint8_t> fe;
    fe.push_back(0x11); // Id 1, size 1 (but not present)
    builder.setFramingExtras({fe.data(), fe.size()});
    EXPECT_EQ("Invalid encoding in FrameExtras",
              validate_error_context(ClientOpcode::Set, blob, Status::Einval));
}
