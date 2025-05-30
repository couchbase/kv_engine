/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "mcbp_test.h"

#include <daemon/cookie.h>
#include <daemon/settings.h>
#include <event2/event.h>
#include <gsl/gsl-lite.hpp>
#include <mcbp/protocol/framebuilder.h>
#include <mcbp/protocol/header.h>
#include <memcached/protocol_binary.h>
#include <nlohmann/json.hpp>
#include <memory>

/**
 * Test all of the command validators we've got to ensure that they
 * catch broken packets. There is still a high number of commands we
 * don't have any command validators for...
 */
namespace mcbp::test {

ValidatorTest::ValidatorTest(bool collectionsEnabled)
    : connection(thread),
      request(reinterpret_cast<cb::mcbp::Request&>(blob)),
      collectionsEnabled(collectionsEnabled) {
}

void ValidatorTest::SetUp() {
    Settings::instance().setXattrEnabled(true);
    connection.setCollectionsSupported(collectionsEnabled);
    memset(&request, 0, sizeof(request));
    request.setMagic(cb::mcbp::Magic::ClientRequest);
    request.setDatatype(cb::mcbp::Datatype::Raw);
}

/**
 * Mock the cookie class and override the getPacket method so that we
 * may use the buffer directly instead of having to insert it into the read/
 * write buffers of the underlying connection
 */
class MockCookie : public Cookie {
public:
    MockCookie(Connection& connection, cb::const_byte_buffer buffer)
        : Cookie(connection) {
        setPacket(*reinterpret_cast<const cb::mcbp::Header*>(buffer.data()));
    }
};

cb::mcbp::Status ValidatorTest::validate(cb::mcbp::ClientOpcode opcode,
                                         void* packet) {
    // Mockup a McbpConnection and Cookie for the validator chain
    connection.enableDatatype(cb::mcbp::Feature::JSON);
    connection.enableDatatype(cb::mcbp::Feature::XATTR);
    connection.enableDatatype(cb::mcbp::Feature::SNAPPY);
    connection.setAllowUnorderedExecution(true);

    const auto& req = *reinterpret_cast<const cb::mcbp::Header*>(packet);
    const size_t size = sizeof(req) + req.getBodylen();
    cb::const_byte_buffer buffer{static_cast<uint8_t*>(packet), size};
    MockCookie cookie(connection, buffer);
    return validatorChains.validate(opcode, cookie);
}

std::string ValidatorTest::validate_error_context(
        cb::mcbp::ClientOpcode opcode,
        void* packet,
        cb::mcbp::Status expectedStatus) {
    const auto& req = *reinterpret_cast<const cb::mcbp::Header*>(packet);
    const size_t size = sizeof(req) + req.getBodylen();
    cb::const_byte_buffer buffer{static_cast<uint8_t*>(packet), size};
    MockCookie cookie(connection, buffer);
    EXPECT_EQ(expectedStatus, validatorChains.validate(opcode, cookie))
            << cookie.getErrorContext();
    return cookie.getErrorContext();
}

std::string ValidatorTest::validate_error_context(
        cb::mcbp::ClientOpcode opcode, cb::mcbp::Status expectedStatus) {
    void* packet = &request;
    return ValidatorTest::validate_error_context(
            opcode, packet, expectedStatus);
}

// Test the validators for GET, GETQ, GETK, GETKQ, GET_META and GETQ_META
class GetValidatorTest : public ::testing::WithParamInterface<
                                 std::tuple<cb::mcbp::ClientOpcode, bool>>,
                         public ValidatorTest {
public:
    void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.setMagic(cb::mcbp::Magic::ClientRequest);
        request.setExtlen(0);
        request.setKeylen(10);
        request.setBodylen(10);
        request.setDatatype(cb::mcbp::Datatype::Raw);
    }

    GetValidatorTest() : ValidatorTest(std::get<1>(GetParam())) {
        // empty
    }

    cb::mcbp::ClientOpcode getGetOpcode() const {
        return std::get<0>(GetParam());
    }

    bool isCollectionsEnabled() const {
        return std::get<1>(GetParam());
    }

protected:
    cb::mcbp::Status validateExtendedExtlen(uint8_t version) {
        request.setBodylen(request.getBodylen() + 1);
        request.setExtlen(1);
        blob[sizeof(cb::mcbp::Request)] = version;
        return validate();
    }

    cb::mcbp::Status validate() {
        auto opcode = cb::mcbp::ClientOpcode(std::get<0>(GetParam()));
        return ValidatorTest::validate(opcode, &request);
    }
};

TEST_P(GetValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GetValidatorTest, ExtendedExtlenV1) {
    switch (getGetOpcode()) {
        // Extended extlen is only supported for *Meta
    case cb::mcbp::ClientOpcode::GetMeta:
    case cb::mcbp::ClientOpcode::GetqMeta:
        EXPECT_EQ(cb::mcbp::Status::Success, validateExtendedExtlen(1));
        break;
    default:;
    }
}

TEST_P(GetValidatorTest, ExtendedExtlenV2) {
    switch (getGetOpcode()) {
        // Extended extlen is only supported for *Meta
    case cb::mcbp::ClientOpcode::GetMeta:
    case cb::mcbp::ClientOpcode::GetqMeta:
        EXPECT_EQ(cb::mcbp::Status::Success, validateExtendedExtlen(2));
        break;
    default:;
    }
}

TEST_P(GetValidatorTest, InvalidExtendedExtlenVersion) {
    switch (getGetOpcode()) {
        // Extended extlen is only supported for *Meta
    case cb::mcbp::ClientOpcode::GetMeta:
    case cb::mcbp::ClientOpcode::GetqMeta:
        EXPECT_EQ(cb::mcbp::Status::Einval, validateExtendedExtlen(3));
        break;
    default:;
    }
}

TEST_P(GetValidatorTest, InvalidExtlen) {
    request.setBodylen(request.getBodylen() + 21);
    request.setExtlen(21);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetValidatorTest, KeyLengthMin) {
    request.setKeylen(isCollectionsEnabled() ? 2 : 1);
    request.setBodylen(isCollectionsEnabled() ? 2 : 1);

    EXPECT_EQ("",
              validate_error_context(
                      cb::mcbp::ClientOpcode(std::get<0>(GetParam())),
                      cb::mcbp::Status::Success));

    // Collections requires 2 bytes minimum, non-collection 1 byte minimum
    request.setKeylen(isCollectionsEnabled() ? 1 : 0);
    request.setBodylen(isCollectionsEnabled() ? 1 : 0);

    std::string expected = isCollectionsEnabled() ? "Key length must be >= 2"
                                                  : "Request must include key";
    EXPECT_EQ(expected,
              validate_error_context(
                      cb::mcbp::ClientOpcode(std::get<0>(GetParam()))));
}

// Clients are restricted in the key length they can provide
TEST_P(GetValidatorTest, KeyLengthMax) {
    // Firstly create a zeroed key, this is a valid key value for collections or
    // non-collections, but importantly when used with collections enabled, it
    // appears to be a DefaultCollection key

    constexpr int maxKeyLen = 250;
    std::fill(
            blob + sizeof(request), blob + sizeof(request) + maxKeyLen + 1, 0);
    request.setKeylen(isCollectionsEnabled() ? maxKeyLen + 1 : maxKeyLen);
    request.setBodylen(isCollectionsEnabled() ? maxKeyLen + 1 : maxKeyLen);

    // The keylen is ok
    EXPECT_EQ("",
              validate_error_context(
                      cb::mcbp::ClientOpcode(std::get<0>(GetParam())),
                      cb::mcbp::Status::Success));

    request.setKeylen(isCollectionsEnabled() ? maxKeyLen + 2 : maxKeyLen + 1);
    request.setBodylen(isCollectionsEnabled() ? maxKeyLen + 2 : maxKeyLen + 1);

    std::string expected = isCollectionsEnabled() ? "Key length exceeds 251"
                                                  : "Key length exceeds 250";
    EXPECT_EQ(expected,
              validate_error_context(
                      cb::mcbp::ClientOpcode(std::get<0>(GetParam()))));

    // Next switch to a valid key for collections or non-collections, with a
    // non-default collection ID
    cb::mcbp::unsigned_leb128<CollectionIDType> leb128(88);
    std::ranges::copy(leb128, blob + sizeof(request));

    auto leb128Size = gsl::narrow_cast<uint16_t>(leb128.size());
    constexpr int maxCollectionsLogicalKeyLen = 246;
    // Valid maximum keylength
    request.setKeylen(isCollectionsEnabled()
                              ? (leb128Size + maxCollectionsLogicalKeyLen)
                              : maxKeyLen);
    request.setBodylen(isCollectionsEnabled()
                               ? (leb128Size + maxCollectionsLogicalKeyLen)
                               : maxKeyLen);

    EXPECT_EQ("",
              validate_error_context(
                      cb::mcbp::ClientOpcode(std::get<0>(GetParam())),
                      cb::mcbp::Status::Success));

    // Exceed valid by 1 byte
    request.setKeylen(isCollectionsEnabled()
                              ? (leb128Size + maxCollectionsLogicalKeyLen + 1)
                              : maxKeyLen + 1);
    request.setBodylen(isCollectionsEnabled()
                               ? (leb128Size + maxCollectionsLogicalKeyLen + 1)
                               : maxKeyLen + 1);

    expected = isCollectionsEnabled() ? "Logical key exceeds 246"
                                      : "Key length exceeds 250";
    EXPECT_EQ(expected,
              validate_error_context(
                      cb::mcbp::ClientOpcode(std::get<0>(GetParam()))));

    // Test max-collection ID
    if (isCollectionsEnabled()) {
        cb::mcbp::unsigned_leb128<CollectionIDType> leb128Max(0xFFFFFFFF);
        leb128Size = gsl::narrow_cast<uint16_t>(leb128Max.size());
        ASSERT_EQ(5, leb128Max.size());
        std::ranges::copy(leb128Max, blob + sizeof(request));
        request.setKeylen(isCollectionsEnabled()
                                  ? (leb128Size + maxCollectionsLogicalKeyLen)
                                  : maxKeyLen);
        request.setBodylen(isCollectionsEnabled()
                                   ? (leb128Size + maxCollectionsLogicalKeyLen)
                                   : maxKeyLen);

        EXPECT_EQ("",
                  validate_error_context(
                          cb::mcbp::ClientOpcode(std::get<0>(GetParam())),
                          cb::mcbp::Status::Success));
        request.setKeylen(
                isCollectionsEnabled()
                        ? (leb128Size + maxCollectionsLogicalKeyLen + 1)
                        : maxKeyLen + 1);
        request.setBodylen(
                isCollectionsEnabled()
                        ? (leb128Size + maxCollectionsLogicalKeyLen + 1)
                        : maxKeyLen + 1);

        expected = isCollectionsEnabled() ? "Key length exceeds 251"
                                          : "Key length exceeds 250";
        EXPECT_EQ(expected,
                  validate_error_context(
                          cb::mcbp::ClientOpcode(std::get<0>(GetParam()))));
    }
}

TEST_P(GetValidatorTest, InvalidKey) {
    if (!isCollectionsEnabled()) {
        // Non collections, anything goes
        return;
    }

    // Test invalid collection IDs
    for (CollectionIDType id = 1; id < 8; id++) {
        cb::mcbp::unsigned_leb128<CollectionIDType> invalidId(id);
        std::ranges::copy(invalidId, blob + sizeof(request));
        std::string expected = "Invalid collection-id:" + std::to_string(id);
        EXPECT_EQ(expected,
                  validate_error_context(
                          cb::mcbp::ClientOpcode(std::get<0>(GetParam()))));
    }
    // Collections requires the leading bytes to be a valid unsigned leb128
    // (varint), so if all key bytes are 0x80 (no stop byte) illegal.
    std::fill_n(blob + sizeof(request), 10, 0x81ull);
    request.setKeylen(10);
    request.setBodylen(10);
    EXPECT_EQ("No stop-byte found",
              validate_error_context(
                      cb::mcbp::ClientOpcode(std::get<0>(GetParam()))));

    // Now make a key which is only a leb128 prefix
    cb::mcbp::unsigned_leb128<CollectionIDType> leb128(2018);
    std::ranges::copy(leb128, blob + sizeof(request));
    auto leb128Size = gsl::narrow_cast<uint16_t>(leb128.size());
    request.setKeylen(leb128Size);
    request.setBodylen(leb128Size);
    EXPECT_EQ("No logical key found",
              validate_error_context(
                      cb::mcbp::ClientOpcode(std::get<0>(GetParam()))));
}

TEST_P(GetValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetValidatorTest, InvalidCas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}


// @todo add test case for the extra legal modes for the
// get meta case

INSTANTIATE_TEST_SUITE_P(
        GetOpcodes,
        GetValidatorTest,
        ::testing::Combine(::testing::Values(cb::mcbp::ClientOpcode::Get,
                                             cb::mcbp::ClientOpcode::Getq,
                                             cb::mcbp::ClientOpcode::Getk,
                                             cb::mcbp::ClientOpcode::Getkq,
                                             cb::mcbp::ClientOpcode::GetMeta,
                                             cb::mcbp::ClientOpcode::GetqMeta),
                           ::testing::Bool()));

// Test ADD & ADDQ
class AddValidatorTest : public ::testing::WithParamInterface<bool>,
                         public ValidatorTest {
public:
    AddValidatorTest() : ValidatorTest(GetParam()) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        request.setExtlen(8);
        request.setKeylen(10);
        request.setBodylen(20);
    }

    bool isCollectionsEnabled() const {
        return GetParam();
    }

protected:
    cb::mcbp::Status validate(cb::mcbp::ClientOpcode opcode) {
        return ValidatorTest::validate(opcode, &request);
    }
};

TEST_P(AddValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate(cb::mcbp::ClientOpcode::Add));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Addq));
}

TEST_P(AddValidatorTest, NoValue) {
    request.setBodylen(18);
    EXPECT_EQ(cb::mcbp::Status::Success, validate(cb::mcbp::ClientOpcode::Add));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Addq));
}

TEST_P(AddValidatorTest, InvalidExtlen) {
    request.setExtlen(9);
    request.setKeylen(10);
    request.setBodylen(21);
    EXPECT_EQ("Request must include extras of length 8",
              validate_error_context(cb::mcbp::ClientOpcode::Add));

    EXPECT_EQ("Request must include extras of length 8",
              validate_error_context(cb::mcbp::ClientOpcode::Addq));
}

TEST_P(AddValidatorTest, NoKey) {
    // Collections requires 2 bytes minimum, non-collection 1 byte minimum
    request.setKeylen(GetParam() ? 1 : 0);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Add));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Addq));
}

TEST_P(AddValidatorTest, InvalidKey) {
    if (!isCollectionsEnabled()) {
        // Non collections, anything goes
        return;
    }
    // Collections requires the leading bytes are a valid unsigned leb128
    // (varint), so if all key bytes are 0x80, illegal.
    auto fill = blob + request.getExtlen();
    std::fill_n(fill + sizeof(request), 10, 0x80ull);
    request.setKeylen(10);
    request.setBodylen(10 + request.getExtlen());
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Add));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Addq));
}

TEST_P(AddValidatorTest, InvalidCas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Add));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Addq));
}

// Test SET, SETQ, REPLACE, REPLACEQ
class SetReplaceValidatorTest : public ::testing::WithParamInterface<bool>,
                                public ValidatorTest {
public:
    SetReplaceValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.setExtlen(8);
        request.setKeylen(10);
        request.setBodylen(20);
    }

    bool isCollectionsEnabled() const {
        return GetParam();
    }

protected:
    cb::mcbp::Status validate(cb::mcbp::ClientOpcode opcode) {
        return ValidatorTest::validate(opcode, &request);
    }
};

TEST_P(SetReplaceValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate(cb::mcbp::ClientOpcode::Set));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Setq));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Replace));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Replaceq));
}

TEST_P(SetReplaceValidatorTest, NoValue) {
    request.setBodylen(18);
    EXPECT_EQ(cb::mcbp::Status::Success, validate(cb::mcbp::ClientOpcode::Set));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Setq));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Replace));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Replaceq));
}

TEST_P(SetReplaceValidatorTest, Cas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Success, validate(cb::mcbp::ClientOpcode::Set));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Setq));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Replace));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Replaceq));
}

TEST_P(SetReplaceValidatorTest, InvalidExtlen) {
    request.setExtlen(9);
    request.setKeylen(10);
    request.setBodylen(21);
    EXPECT_EQ("Request must include extras of length 8",
              validate_error_context(cb::mcbp::ClientOpcode::Set));

    EXPECT_EQ("Request must include extras of length 8",
              validate_error_context(cb::mcbp::ClientOpcode::Setq));
    EXPECT_EQ("Request must include extras of length 8",
              validate_error_context(cb::mcbp::ClientOpcode::Replace));
    EXPECT_EQ("Request must include extras of length 8",
              validate_error_context(cb::mcbp::ClientOpcode::Replaceq));
}

TEST_P(SetReplaceValidatorTest, NoKey) {
    // Collections requires 2 bytes minimum, non-collection 1 byte minimum
    request.setKeylen(GetParam() ? 1 : 0);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Set));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Setq));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Replace));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Replaceq));
}

TEST_P(SetReplaceValidatorTest, InvalidKey) {
    if (!isCollectionsEnabled()) {
        // Non collections, anything goes
        return;
    }
    // Collections requires the leading bytes are a valid unsigned leb128
    // (varint), so if all key bytes are 0x80, (no stop-byte) illegal.
    auto key = blob + sizeof(request) + request.getExtlen();
    std::fill_n(key, 10, 0x80ull);
    request.setKeylen(10);
    request.setBodylen(10 + request.getExtlen());
    EXPECT_EQ("No stop-byte found",
              validate_error_context(cb::mcbp::ClientOpcode::Set));
    EXPECT_EQ("No stop-byte found",
              validate_error_context(cb::mcbp::ClientOpcode::Setq));
    EXPECT_EQ("No stop-byte found",
              validate_error_context(cb::mcbp::ClientOpcode::Replace));
    EXPECT_EQ("No stop-byte found",
              validate_error_context(cb::mcbp::ClientOpcode::Replaceq));
}

// Test Append[q] and Prepend[q]
class AppendPrependValidatorTest : public ::testing::WithParamInterface<bool>,
                                   public ValidatorTest {
public:
    AppendPrependValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.setKeylen(10);
        request.setBodylen(20);
    }

    bool isCollectionsEnabled() const {
        return GetParam();
    }

protected:
    cb::mcbp::Status validate(cb::mcbp::ClientOpcode opcode) {
        return ValidatorTest::validate(opcode, &request);
    }
};

TEST_P(AppendPrependValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Append));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Appendq));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Prepend));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Prependq));
}

TEST_P(AppendPrependValidatorTest, NoValue) {
    request.setBodylen(10);
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Append));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Appendq));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Prepend));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Prependq));
}

TEST_P(AppendPrependValidatorTest, Cas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Append));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Appendq));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Prepend));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Prependq));
}

TEST_P(AppendPrependValidatorTest, InvalidExtlen) {
    request.setExtlen(21);
    request.setKeylen(10);
    request.setBodylen(20 + 21);
    EXPECT_EQ("Request must not include extras",
              validate_error_context(cb::mcbp::ClientOpcode::Append));
    EXPECT_EQ("Request must not include extras",
              validate_error_context(cb::mcbp::ClientOpcode::Appendq));
    EXPECT_EQ("Request must not include extras",
              validate_error_context(cb::mcbp::ClientOpcode::Prepend));
    EXPECT_EQ("Request must not include extras",
              validate_error_context(cb::mcbp::ClientOpcode::Prependq));
}

TEST_P(AppendPrependValidatorTest, NoKey) {
    // Collections requires 2 bytes minimum, non-collection 1 byte minimum
    request.setKeylen(isCollectionsEnabled() ? 1 : 0);
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Append));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Appendq));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Prepend));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Prependq));
}

TEST_P(AppendPrependValidatorTest, InvalidKey) {
    if (!isCollectionsEnabled()) {
        // Non collections, anything goes
        return;
    }
    // Collections requires the leading bytes are a valid unsigned leb128
    // (varint), so if all key bytes are 0x80, (no stop-byte) illegal.
    auto key = blob + sizeof(request) + request.getExtlen();
    std::fill_n(key, 10, 0x80ull);
    request.setKeylen(10);
    EXPECT_EQ("No stop-byte found",
              validate_error_context(cb::mcbp::ClientOpcode::Append));
    EXPECT_EQ("No stop-byte found",
              validate_error_context(cb::mcbp::ClientOpcode::Appendq));
    EXPECT_EQ("No stop-byte found",
              validate_error_context(cb::mcbp::ClientOpcode::Prepend));
    EXPECT_EQ("No stop-byte found",
              validate_error_context(cb::mcbp::ClientOpcode::Prependq));
}

// Test DELETE & DELETEQ
class DeleteValidatorTest : public ::testing::WithParamInterface<bool>,
                            public ValidatorTest {
public:
    DeleteValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();

        request.setKeylen(10);
        request.setBodylen(10);
    }

    bool isCollectionsEnabled() const {
        return GetParam();
    }

protected:
    cb::mcbp::Status validate(cb::mcbp::ClientOpcode opcode) {
        return ValidatorTest::validate(opcode, &request);
    }
};

TEST_P(DeleteValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Delete));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Deleteq));
}

TEST_P(DeleteValidatorTest, Cas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Delete));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Deleteq));
}

TEST_P(DeleteValidatorTest, WithValue) {
    request.setBodylen(20);
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Delete));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Deleteq));
}

TEST_P(DeleteValidatorTest, InvalidExtlen) {
    request.setExtlen(21);
    request.setBodylen(21 + 10);
    EXPECT_EQ("Request must not include extras",
              validate_error_context(cb::mcbp::ClientOpcode::Delete));
    EXPECT_EQ("Request must not include extras",
              validate_error_context(cb::mcbp::ClientOpcode::Deleteq));
}

TEST_P(DeleteValidatorTest, NoKey) {
    request.setKeylen(0);
    EXPECT_EQ("Request must include key",
              validate_error_context(cb::mcbp::ClientOpcode::Delete));
    EXPECT_EQ("Request must include key",
              validate_error_context(cb::mcbp::ClientOpcode::Deleteq));
}

TEST_P(DeleteValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Delete));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Deleteq));
}

// Test INCREMENT[q] and DECREMENT[q]
class IncrementDecrementValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    IncrementDecrementValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.setExtlen(20);
        request.setKeylen(10);
        request.setBodylen(30);
    }

    bool isCollectionsEnabled() const {
        return GetParam();
    }

protected:
    cb::mcbp::Status validate(cb::mcbp::ClientOpcode opcode) {
        return ValidatorTest::validate(opcode, &request);
    }
};

TEST_P(IncrementDecrementValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Increment));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Incrementq));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Decrement));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Decrementq));
}

TEST_P(IncrementDecrementValidatorTest, Cas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Increment));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Incrementq));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Decrement));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Decrementq));
}

TEST_P(IncrementDecrementValidatorTest, InvalidExtlen) {
    request.setExtlen(21);
    request.setKeylen(10);
    request.setBodylen(31);
    EXPECT_EQ("Request must include extras of length 20",
              validate_error_context(cb::mcbp::ClientOpcode::Increment));
    EXPECT_EQ("Request must include extras of length 20",
              validate_error_context(cb::mcbp::ClientOpcode::Incrementq));
    EXPECT_EQ("Request must include extras of length 20",
              validate_error_context(cb::mcbp::ClientOpcode::Decrement));
    EXPECT_EQ("Request must include extras of length 20",
              validate_error_context(cb::mcbp::ClientOpcode::Decrementq));
}

TEST_P(IncrementDecrementValidatorTest, NoKey) {
    // Collections requires 2 bytes minimum, non-collection 1 byte minimum
    request.setKeylen(0);
    EXPECT_EQ("Request must include key",
              validate_error_context(cb::mcbp::ClientOpcode::Increment));
    EXPECT_EQ("Request must include key",
              validate_error_context(cb::mcbp::ClientOpcode::Incrementq));
    EXPECT_EQ("Request must include key",
              validate_error_context(cb::mcbp::ClientOpcode::Decrement));
    EXPECT_EQ("Request must include key",
              validate_error_context(cb::mcbp::ClientOpcode::Decrementq));
}

TEST_P(IncrementDecrementValidatorTest, InvalidKey) {
    if (!isCollectionsEnabled()) {
        // Non collections, anything goes
        return;
    }
    // Collections requires the leading bytes are a valid unsigned leb128
    // (varint), so if all key bytes are 0x80, (no stop-byte) illegal.
    auto key = blob + sizeof(request) + request.getExtlen();
    std::fill_n(key, 10, 0x80ull);
    request.setKeylen(10);
    EXPECT_EQ("No stop-byte found",
              validate_error_context(cb::mcbp::ClientOpcode::Increment));
    EXPECT_EQ("No stop-byte found",
              validate_error_context(cb::mcbp::ClientOpcode::Incrementq));
    EXPECT_EQ("No stop-byte found",
              validate_error_context(cb::mcbp::ClientOpcode::Decrement));
    EXPECT_EQ("No stop-byte found",
              validate_error_context(cb::mcbp::ClientOpcode::Decrementq));
}

TEST_P(IncrementDecrementValidatorTest, WithValue) {
    request.setBodylen(40);
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Increment));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Incrementq));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Decrement));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Decrementq));
}

TEST_P(IncrementDecrementValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Increment));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Incrementq));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Decrement));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Decrementq));
}

// Test QUIT & QUITQ
class QuitValidatorTest : public ::testing::WithParamInterface<bool>,
                          public ValidatorTest {
public:
    QuitValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate(cb::mcbp::ClientOpcode opcode) {
        return ValidatorTest::validate(opcode, &request);
    }
};

TEST_P(QuitValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Quit));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::Quitq));
}

TEST_P(QuitValidatorTest, InvalidExtlen) {
    request.setExtlen(21);
    request.setBodylen(21);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Quit));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Quitq));
}

TEST_P(QuitValidatorTest, InvalidKey) {
    request.setExtlen(10);
    request.setBodylen(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Quit));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Quitq));
}

TEST_P(QuitValidatorTest, InvalidCas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Quit));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Quitq));
}

TEST_P(QuitValidatorTest, InvalidBodylen) {
    request.setBodylen(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Quit));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Quitq));
}

TEST_P(QuitValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(cb::mcbp::ClientOpcode::Quit));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::Quitq));
}

// test Noop
class NoopValidatorTest : public ::testing::WithParamInterface<bool>,
                          public ValidatorTest {
public:
    NoopValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::Noop, &request);
    }
};

TEST_P(NoopValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(NoopValidatorTest, InvalidExtlen) {
    request.setExtlen(21);
    request.setBodylen(21);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(NoopValidatorTest, InvalidKeylen) {
    request.setKeylen(32);
    request.setBodylen(32);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(NoopValidatorTest, InvalidBodylen) {
    request.setBodylen(100);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(NoopValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(NoopValidatorTest, InvalidCas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// test version
class VersionValidatorTest : public ::testing::WithParamInterface<bool>,
                             public ValidatorTest {
public:
    VersionValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::Version,
                                       &request);
    }
};

TEST_P(VersionValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(VersionValidatorTest, InvalidExtlen) {
    request.setExtlen(21);
    request.setBodylen(21);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(VersionValidatorTest, InvalidKeylen) {
    request.setKeylen(32);
    request.setBodylen(32);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(VersionValidatorTest, InvalidBodylen) {
    request.setBodylen(100);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(VersionValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(VersionValidatorTest, InvalidCas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// test stat
class StatValidatorTest : public ::testing::WithParamInterface<bool>,
                          public ValidatorTest {
public:
    StatValidatorTest() : ValidatorTest(GetParam()) {
        // Build a legal packet
        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::Stat);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::Stat, &request);
    }
};

TEST_P(StatValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(StatValidatorTest, WithKey) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)}, true);
    builder.setKey("statkey");
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(StatValidatorTest, InvalidExtlen) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)}, true);
    builder.setExtras({reinterpret_cast<const uint8_t*>("foo"), 3});
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(StatValidatorTest, ValuePresent) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)}, true);
    builder.setDatatype(cb::mcbp::Datatype::JSON);
    builder.setValue(R"({ "foo" : "bar" })");
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(StatValidatorTest, ValuePresent_NotJson) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)}, true);
    builder.setValue("body");
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(StatValidatorTest, ValuePresent_Json_Missing_Datatype) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)}, true);
    builder.setValue(R"({ "foo" : "bar" })");
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(StatValidatorTest, InvalidCas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// test verbosity
class VerbosityValidatorTest : public ::testing::WithParamInterface<bool>,
                               public ValidatorTest {
public:
    VerbosityValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.setExtlen(4);
        request.setBodylen(4);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::Verbosity,
                                       &request);
    }
};

TEST_P(VerbosityValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(VerbosityValidatorTest, InvalidExtlen) {
    request.setExtlen(21);
    request.setBodylen(21);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(VerbosityValidatorTest, InvalidBodylen) {
    request.setBodylen(100);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(VerbosityValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(VerbosityValidatorTest, InvalidKey) {
    request.setKeylen(21);
    request.setBodylen(21);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(VerbosityValidatorTest, InvalidCas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// test HELLO
class HelloValidatorTest : public ::testing::WithParamInterface<bool>,
                           public ValidatorTest {
public:
    HelloValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::Hello, &request);
    }
};

TEST_P(HelloValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(HelloValidatorTest, MultipleFeatures) {
    request.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
    request.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
    request.setBodylen(6);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(HelloValidatorTest, WithKey) {
    request.setKeylen(21);
    request.setBodylen(21);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(HelloValidatorTest, InvalidExtlen) {
    request.setExtlen(2);
    request.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(HelloValidatorTest, InvalidBodylen) {
    request.setBodylen(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(HelloValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(HelloValidatorTest, InvalidCas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// test SASL_LIST_MECHS
class SaslListMechValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
    SaslListMechValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::SaslListMechs,
                                       &request);
    }
};

TEST_P(SaslListMechValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(SaslListMechValidatorTest, InvalidExtlen) {
    request.setExtlen(2);
    request.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SaslListMechValidatorTest, InvalidKey) {
    request.setKeylen(21);
    request.setBodylen(21);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SaslListMechValidatorTest, InvalidBodylen) {
    request.setBodylen(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SaslListMechValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SaslListMechValidatorTest, InvalidCas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// test SASL_AUTH
class SaslAuthValidatorTest : public ::testing::WithParamInterface<bool>,
                              public ValidatorTest {
public:
    SaslAuthValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.setKeylen(10);
        request.setBodylen(10);
    }

protected:
    cb::mcbp::Status validate(cb::mcbp::ClientOpcode opcode) {
        return ValidatorTest::validate(opcode, &request);
    }
};

TEST_P(SaslAuthValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::SaslAuth));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::SaslStep));
}

TEST_P(SaslAuthValidatorTest, WithChallenge) {
    request.setBodylen(20);
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::SaslAuth));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::SaslStep));
}

TEST_P(SaslAuthValidatorTest, InvalidExtlen) {
    request.setExtlen(2);
    request.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::SaslAuth));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::SaslStep));
}

TEST_P(SaslAuthValidatorTest, InvalidKey) {
    request.setKeylen(0);
    request.setBodylen(0);
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::SaslAuth));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::SaslStep));
}

TEST_P(SaslAuthValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::SaslAuth));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::SaslStep));
}

TEST_P(SaslAuthValidatorTest, InvalidCas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::SaslAuth));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::SaslStep));
}

class GetErrmapValidatorTest : public ::testing::WithParamInterface<bool>,
                               public ValidatorTest {
public:
    GetErrmapValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::GetErrorMap,
                                       &request);
    }
};

TEST_P(GetErrmapValidatorTest, CorrectMessage) {
    request.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GetErrmapValidatorTest, MissingBody) {
    request.setBodylen(0);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// test IOCTL_GET
class IoctlGetValidatorTest : public ::testing::WithParamInterface<bool>,
                              public ValidatorTest {
public:
    IoctlGetValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.setKeylen(10);
        request.setBodylen(10);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::IoctlGet,
                                       &request);
    }
};

TEST_P(IoctlGetValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(IoctlGetValidatorTest, InvalidExtlen) {
    request.setExtlen(2);
    request.setBodylen(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(IoctlGetValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(IoctlGetValidatorTest, InvalidCas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(IoctlGetValidatorTest, InvalidBody) {
    request.setBodylen(20);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// test IOCTL_SET
class IoctlSetValidatorTest : public ::testing::WithParamInterface<bool>,
                              public ValidatorTest {
public:
    IoctlSetValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.setKeylen(10);
        request.setBodylen(10);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::IoctlSet,
                                       &request);
    }
};

TEST_P(IoctlSetValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(IoctlSetValidatorTest, InvalidExtlen) {
    request.setExtlen(2);
    request.setBodylen(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(IoctlSetValidatorTest, InvalidKey) {
    request.setKeylen(0);
    request.setBodylen(0);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(IoctlSetValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(IoctlSetValidatorTest, InvalidCas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// test AUDIT_PUT
class AuditPutValidatorTest : public ::testing::WithParamInterface<bool>,
                              public ValidatorTest {
public:
    AuditPutValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.setExtlen(4);
        request.setBodylen(10);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::AuditPut,
                                       &request);
    }
};

TEST_P(AuditPutValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(AuditPutValidatorTest, InvalidExtlen) {
    request.setExtlen(2);
    request.setBodylen(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(AuditPutValidatorTest, InvalidKey) {
    request.setKeylen(10);
    request.setBodylen(15);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(AuditPutValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(AuditPutValidatorTest, InvalidCas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(AuditPutValidatorTest, InvalidBody) {
    request.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// Test audit_config_reload
class AuditConfigReloadValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    AuditConfigReloadValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::AuditConfigReload, &request);
    }
};

TEST_P(AuditConfigReloadValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(AuditConfigReloadValidatorTest, InvalidExtlen) {
    request.setExtlen(2);
    request.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(AuditConfigReloadValidatorTest, InvalidKey) {
    request.setKeylen(10);
    request.setBodylen(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(AuditConfigReloadValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(AuditConfigReloadValidatorTest, InvalidCas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(AuditConfigReloadValidatorTest, InvalidBody) {
    request.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// Test shutdown
class ShutdownValidatorTest : public ::testing::WithParamInterface<bool>,
                              public ValidatorTest {
public:
    ShutdownValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::Shutdown,
                                       &request);
    }
};

TEST_P(ShutdownValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(ShutdownValidatorTest, InvalidExtlen) {
    request.setExtlen(2);
    request.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ShutdownValidatorTest, InvalidKey) {
    request.setKeylen(10);
    request.setBodylen(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ShutdownValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ShutdownValidatorTest, InvalidCas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ShutdownValidatorTest, InvalidBody) {
    request.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// Test observe seqno
class ObserveSeqnoValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
    ObserveSeqnoValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.setBodylen(8);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::ObserveSeqno,
                                       &request);
    }
};

TEST_P(ObserveSeqnoValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(ObserveSeqnoValidatorTest, InvalidExtlen) {
    request.setExtlen(8);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ObserveSeqnoValidatorTest, InvalidKey) {
    request.setKeylen(10);
    request.setBodylen(18);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ObserveSeqnoValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ObserveSeqnoValidatorTest, InvalidBody) {
    request.setBodylen(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

enum class RefreshOpcodes : uint8_t {
    Isasl = uint8_t(cb::mcbp::ClientOpcode::IsaslRefresh),
    Rbac = uint8_t(cb::mcbp::ClientOpcode::RbacRefresh)
};

std::string to_string(const RefreshOpcodes& opcode) {
    switch (opcode) {
    case RefreshOpcodes::Isasl:
        return "ISASL";
    case RefreshOpcodes::Rbac:
        return "RBAC";
    }
    throw std::invalid_argument("to_string(const RefreshOpcodes&): unknown opcode");
}

std::ostream& operator<<(std::ostream& os, const RefreshOpcodes& o) {
    os << to_string(o);
    return os;
}

class RefreshValidatorTest
    : public ::testing::WithParamInterface<std::tuple<RefreshOpcodes, bool>>,
      public ValidatorTest {
public:
    RefreshValidatorTest() : ValidatorTest(std::get<1>(GetParam())) {
    }

protected:
    cb::mcbp::Status validate() {
        auto opcode = (cb::mcbp::ClientOpcode)std::get<0>(GetParam());
        return ValidatorTest::validate(opcode, &request);
    }
};

INSTANTIATE_TEST_SUITE_P(
        RefreshOpcodes,
        RefreshValidatorTest,
        ::testing::Combine(::testing::Values(RefreshOpcodes::Isasl,
                                             RefreshOpcodes::Rbac),
                           ::testing::Bool()));

TEST_P(RefreshValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(RefreshValidatorTest, InvalidExtlen) {
    request.setExtlen(2);
    request.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RefreshValidatorTest, InvalidKey) {
    request.setKeylen(10);
    request.setBodylen(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RefreshValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RefreshValidatorTest, InvalidCas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RefreshValidatorTest, InvalidBody) {
    request.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// Test CmdTimer
class CmdTimerValidatorTest : public ::testing::WithParamInterface<bool>,
                              public ValidatorTest {
public:
    CmdTimerValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.setExtlen(1);
        request.setBodylen(1);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::GetCmdTimer,
                                       &request);
    }
};

TEST_P(CmdTimerValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}


TEST_P(CmdTimerValidatorTest, InvalidExtlen) {
    request.setExtlen(2);
    request.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(CmdTimerValidatorTest, InvalidKey) {
    request.setKeylen(10);
    request.setBodylen(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(CmdTimerValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(CmdTimerValidatorTest, InvalidCas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(CmdTimerValidatorTest, InvalidBody) {
    request.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// Test GetCtrlToken
class GetCtrlTokenValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
    GetCtrlTokenValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::GetCtrlToken,
                                       &request);
    }
};

TEST_P(GetCtrlTokenValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GetCtrlTokenValidatorTest, InvalidExtlen) {
    request.setExtlen(2);
    request.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetCtrlTokenValidatorTest, InvalidKey) {
    request.setKeylen(10);
    request.setBodylen(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetCtrlTokenValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetCtrlTokenValidatorTest, InvalidCas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetCtrlTokenValidatorTest, InvalidBody) {
    request.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// Test SetCtrlToken
class SetCtrlTokenValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
    SetCtrlTokenValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.setMagic(cb::mcbp::Magic::ClientRequest);
        request.setExtlen(sizeof(extras));
        request.setBodylen(sizeof(extras));
        extras.setCas(1);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::SetCtrlToken,
                                       &request);
    }

    cb::mcbp::Request& request = *reinterpret_cast<cb::mcbp::Request*>(blob);
    cb::mcbp::request::SetCtrlTokenPayload& extras =
            *reinterpret_cast<cb::mcbp::request::SetCtrlTokenPayload*>(
                    blob + sizeof(cb::mcbp::Request));
};

TEST_P(SetCtrlTokenValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(SetCtrlTokenValidatorTest, InvalidCas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetCtrlTokenValidatorTest, InvalidExtlen) {
    request.setExtlen(2);
    request.setBodylen(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetCtrlTokenValidatorTest, InvalidKey) {
    request.setKeylen(10);
    request.setBodylen(18);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetCtrlTokenValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetCtrlTokenValidatorTest, InvalidNewCas) {
    extras.setCas(0);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetCtrlTokenValidatorTest, InvalidBody) {
    request.setBodylen(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// cb::mcbp::ClientOpcode::GetAllVbSeqnos
class GetAllVbSeqnoValidatorTest : public ::testing::WithParamInterface<bool>,
                                   public ValidatorTest {
public:
    GetAllVbSeqnoValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.header.setMagic(cb::mcbp::Magic::ClientRequest);
        request.header.setDatatype(cb::mcbp::Datatype::Raw);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::GetAllVbSeqnos,
                                       &request);
    }

    protocol_binary_request_get_all_vb_seqnos &request =
        *reinterpret_cast<protocol_binary_request_get_all_vb_seqnos*>(blob);
};

TEST_P(GetAllVbSeqnoValidatorTest, CorrectMessageNoState) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, CorrectMessageWithState) {
    EXPECT_EQ(4, sizeof(vbucket_state_t));
    request.header.setExtlen(4);
    request.header.setBodylen(4);
    request.body.state =
            static_cast<RequestedVBState>(htonl(int(RequestedVBState::Active)));
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, CorrectMessageWithCollectionID) {
    EXPECT_EQ(4, sizeof(vbucket_state_t));
    EXPECT_EQ(4, sizeof(CollectionIDType));
    request.header.setExtlen(8);
    request.header.setBodylen(8);
    request.body.state = static_cast<RequestedVBState>(htonl(1));
    request.body.cid = htonl(8);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, InvalidExtlen) {
    request.header.setExtlen(2);
    request.header.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, InvalidKey) {
    request.header.setKeylen(10);
    request.header.setBodylen(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, InvalidDatatype) {
    request.header.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, InvalidCas) {
    request.header.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, InvalidBody) {
    request.header.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());

    request.header.setBodylen(8);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, InvalidBodylen) {
    request.header.setBodylen(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetAllVbSeqnoValidatorTest, InvalidVbucketState) {
    request.header.setExtlen(4);
    request.header.setBodylen(4);

    for (int ii = 0; ii < 100; ++ii) {
        request.body.state = static_cast<RequestedVBState>(htonl(ii));
        // Must be a valid vbucket state or 0 (indicating any alive state)
        if (static_cast<RequestedVBState>(ii) >= RequestedVBState::Alive &&
            static_cast<RequestedVBState>(ii) <= RequestedVBState::Dead) {
            EXPECT_EQ(cb::mcbp::Status::Success, validate());
        } else {
            EXPECT_EQ(cb::mcbp::Status::Einval, validate());
        }
    }
}

// cb::mcbp::ClientOpcode::GetLocked
class GetLockedValidatorTest : public ::testing::WithParamInterface<bool>,
                               public ValidatorTest {
public:
    GetLockedValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.setKeylen(10);
        request.setBodylen(10);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::GetLocked,
                                       &request);
    }
};

TEST_P(GetLockedValidatorTest, CorrectMessageDefaultTimeout) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GetLockedValidatorTest, CorrectMessageExplicitTimeout) {
    request.setExtlen(4);
    request.setBodylen(14);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GetLockedValidatorTest, InvalidExtlen) {
    request.setExtlen(2);
    request.setBodylen(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetLockedValidatorTest, InvalidKey) {
    request.setKeylen(10);
    request.setBodylen(11);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetLockedValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetLockedValidatorTest, InvalidCas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetLockedValidatorTest, InvalidBody) {
    request.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetLockedValidatorTest, InvalidBodylen) {
    request.setBodylen(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// PROTOCOL_BINARY_CMD_UNLOCK
class UnlockValidatorTest : public ::testing::WithParamInterface<bool>,
                            public ValidatorTest {
public:
    UnlockValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.setKeylen(10);
        request.setBodylen(10);
        request.setCas(0xdeadbeef);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::UnlockKey,
                                       &request);
    }
};

TEST_P(UnlockValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(UnlockValidatorTest, InvalidExtlen) {
    request.setExtlen(2);
    request.setBodylen(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(UnlockValidatorTest, InvalidKey) {
    request.setKeylen(10);
    request.setBodylen(11);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(UnlockValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(UnlockValidatorTest, InvalidCas) {
    request.setCas(0);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(UnlockValidatorTest, InvalidBody) {
    request.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(UnlockValidatorTest, InvalidBodylen) {
    request.setBodylen(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// Test config_reload
class ConfigReloadValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
    ConfigReloadValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::ConfigReload,
                                       &request);
    }
};

TEST_P(ConfigReloadValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}


TEST_P(ConfigReloadValidatorTest, InvalidExtlen) {
    request.setExtlen(2);
    request.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ConfigReloadValidatorTest, InvalidKey) {
    request.setKeylen(10);
    request.setBodylen(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ConfigReloadValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ConfigReloadValidatorTest, InvalidCas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ConfigReloadValidatorTest, InvalidBody) {
    request.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// cb::mcbp::ClientOpcode::EvictKey
class EvictKeyValidatorTest : public ::testing::WithParamInterface<bool>,
                              public ValidatorTest {
public:
    EvictKeyValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.setKeylen(10);
        request.setBodylen(10);
        request.setCas(0);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::EvictKey,
                                       &request);
    }
};

TEST_P(EvictKeyValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(EvictKeyValidatorTest, InvalidExtlen) {
    request.setExtlen(2);
    request.setBodylen(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(EvictKeyValidatorTest, InvalidKey) {
    request.setKeylen(10);
    request.setBodylen(11);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(EvictKeyValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(EvictKeyValidatorTest, InvalidCas) {
    request.setCas(0xff);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(EvictKeyValidatorTest, InvalidBody) {
    request.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(EvictKeyValidatorTest, InvalidBodylen) {
    request.setBodylen(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class RevokeUserPermissionsValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    RevokeUserPermissionsValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.setKeylen(10);
        request.setBodylen(10);
        request.setCas(0);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::EvictKey,
                                       &request);
    }
};

TEST_P(RevokeUserPermissionsValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(RevokeUserPermissionsValidatorTest, InvalidExtlen) {
    request.setExtlen(2);
    request.setBodylen(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RevokeUserPermissionsValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RevokeUserPermissionsValidatorTest, InvalidCas) {
    request.setCas(0xff);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RevokeUserPermissionsValidatorTest, MissingKey) {
    request.setKeylen(0);
    request.setBodylen(0);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RevokeUserPermissionsValidatorTest, InvalidBodylen) {
    request.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class ErrorContextTest : public ::testing::WithParamInterface<bool>,
                         public ValidatorTest {
public:
    ErrorContextTest() : ValidatorTest(GetParam()) {
    }

    bool isCollectionsEnabled() const {
        return GetParam();
    }
};

TEST_P(ErrorContextTest, ValidHeader) {
    // Error context should not be set on valid request
    EXPECT_EQ("",
              validate_error_context(cb::mcbp::ClientOpcode::Noop,
                                     cb::mcbp::Status::Success));
}

TEST_P(ErrorContextTest, InvalidDatatype) {
    // Nonexistent datatype
    request.setDatatype(cb::mcbp::Datatype(cb::mcbp::datatype::highest + 1));
    EXPECT_EQ("Request datatype invalid",
              validate_error_context(cb::mcbp::ClientOpcode::Noop));

    // Noop command does not accept JSON
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ("Request datatype invalid",
              validate_error_context(cb::mcbp::ClientOpcode::Noop));
}

TEST_P(ErrorContextTest, InvalidExtras) {
    // Noop command does not accept extras
    request.setExtlen(4);
    request.setKeylen(0);
    request.setBodylen(4);
    EXPECT_EQ("Request must not include extras",
              validate_error_context(cb::mcbp::ClientOpcode::Noop));

    // Add command requires extras
    request.setExtlen(0);
    request.setKeylen(10);
    request.setBodylen(14);
    EXPECT_EQ("Request must include extras of length 8",
              validate_error_context(cb::mcbp::ClientOpcode::Add));
}

TEST_P(ErrorContextTest, InvalidKey) {
    // Noop command does not accept key
    request.setExtlen(0);
    request.setKeylen(8);
    request.setBodylen(8);
    EXPECT_EQ("Request must not include key",
              validate_error_context(cb::mcbp::ClientOpcode::Noop));

    // Add command requires key
    request.setExtlen(8);
    request.setKeylen(0);
    request.setBodylen(8);
    EXPECT_EQ("Request must include key",
              validate_error_context(cb::mcbp::ClientOpcode::Add));
}

TEST_P(ErrorContextTest, InvalidValue) {
    // Noop command does not accept value
    request.setExtlen(0);
    request.setKeylen(0);
    request.setBodylen(8);
    EXPECT_EQ("Request must not include value",
              validate_error_context(cb::mcbp::ClientOpcode::Noop));

    // Create bucket command requires value
    request.setExtlen(0);
    request.setKeylen(8);
    request.setBodylen(8);
    EXPECT_EQ("Request must include value",
              validate_error_context(cb::mcbp::ClientOpcode::CreateBucket));
}

TEST_P(ErrorContextTest, InvalidCas) {
    // Unlock command requires CAS
    request.setExtlen(0);
    request.setKeylen(8);
    request.setBodylen(8);
    request.setCas(0);
    EXPECT_EQ("Request CAS must be set",
              validate_error_context(cb::mcbp::ClientOpcode::UnlockKey));

    // Noop command does not accept CAS
    request.setExtlen(0);
    request.setKeylen(0);
    request.setBodylen(0);
    request.setCas(10);
    EXPECT_EQ("Request CAS must not be set",
              validate_error_context(cb::mcbp::ClientOpcode::Noop));
}

class CommandSpecificErrorContextTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    CommandSpecificErrorContextTest() : ValidatorTest(GetParam()) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        memset(blob, 0, sizeof(blob));
        request.setMagic(cb::mcbp::Magic::ClientRequest);
        request.setDatatype(cb::mcbp::Datatype::Raw);
        connection.enableDatatype(cb::mcbp::Feature::XATTR);
        connection.enableDatatype(cb::mcbp::Feature::SNAPPY);
        connection.enableDatatype(cb::mcbp::Feature::JSON);
    }
    bool isCollectionsEnabled() const {
        return GetParam();
    }

protected:
    cb::mcbp::Request& header = request;
};

TEST_P(CommandSpecificErrorContextTest, DcpOpen) {
    header.setExtlen(8);
    header.setKeylen(10);
    header.setBodylen(20);

    // Non-JSON value
    EXPECT_EQ("datatype should be set to JSON for non-empty value",
              validate_error_context(cb::mcbp::ClientOpcode::DcpOpen));

    // JSON value, but unexpected key
    header.setDatatype(cb::mcbp::Datatype::JSON);
    nlohmann::json value = {{"unknown_key", "unknown_value"}};
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)}, true);
    builder.setValue(value.dump());
    EXPECT_EQ("Unsupported JSON property unknown_key",
              validate_error_context(cb::mcbp::ClientOpcode::DcpOpen));

    // DCP_OPEN_UNUSED flag is invalid
    header.setBodylen(18);
    auto extras = header.getExtdata();
    using cb::mcbp::request::DcpOpenPayload;
    auto* payload = reinterpret_cast<DcpOpenPayload*>(extras.data());
    payload->setFlags(cb::mcbp::DcpOpenFlag::Unused);
    EXPECT_EQ("Request contains invalid flags: \"unknown:0x10\"",
              validate_error_context(cb::mcbp::ClientOpcode::DcpOpen));
}

TEST_P(CommandSpecificErrorContextTest, DcpAddStream) {
    // DCP_ADD_STREAM_FLAG_NO_VALUE is no longer used
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)}, true);
    cb::mcbp::request::DcpAddStreamPayload extras;
    extras.setFlags(cb::mcbp::DcpAddStreamFlag::NoValue);
    builder.setExtras(extras.getBuffer());
    EXPECT_EQ("DCP_ADD_STREAM_FLAG_NO_VALUE{8} flag is no longer used",
              validate_error_context(cb::mcbp::ClientOpcode::DcpAddStream));

    // 256 is not a defined flag
    extras.setFlags(static_cast<cb::mcbp::DcpAddStreamFlag>(256));
    builder.setExtras(extras.getBuffer());
    EXPECT_EQ("Request contains invalid flags: \"unknown:0x100\"",
              validate_error_context(cb::mcbp::ClientOpcode::DcpAddStream));
}

TEST_P(CommandSpecificErrorContextTest, DcpStreamRequest) {
    header.setExtlen(48);
    header.setKeylen(0);
    header.setBodylen(48 + 10);

    if (isCollectionsEnabled()) {
        EXPECT_EQ("Attached bucket does not support DCP",
                  validate_error_context(cb::mcbp::ClientOpcode::DcpStreamReq,
                                         cb::mcbp::Status::NotSupported));
    } else {
        EXPECT_EQ("Request must not include value",
                  validate_error_context(cb::mcbp::ClientOpcode::DcpStreamReq));
    }

    header.setKeylen(5);
    header.setBodylen(48 + 5);
    EXPECT_EQ("Request must not include key",
              validate_error_context(cb::mcbp::ClientOpcode::DcpStreamReq));
    header.setKeylen(0);
    header.setBodylen(48);
    header.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ("Request datatype invalid",
              validate_error_context(cb::mcbp::ClientOpcode::DcpStreamReq));
}

TEST_P(CommandSpecificErrorContextTest, DcpSystemEvent) {
    cb::mcbp::request::DcpSystemEventPayload extras;
    extras.setEvent(10);

    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)}, true);
    builder.setExtras(extras.getBuffer());
    builder.setKey(std::string_view{});
    builder.setValue(std::string_view{});

    // System event ID must be 0, 1 or 2
    EXPECT_EQ("Invalid system event id",
              validate_error_context(cb::mcbp::ClientOpcode::DcpSystemEvent));
}

TEST_P(CommandSpecificErrorContextTest, DcpMutation) {
    // Connection must be Xattr enabled if datatype is Xattr
    constexpr auto extlen = sizeof(cb::mcbp::request::DcpMutationPayload);
    header.setExtlen(gsl::narrow<uint8_t>(extlen));
    header.setKeylen(10);
    header.setBodylen(extlen + 10);
    header.setDatatype(cb::mcbp::Datatype::Xattr);
    connection.disableAllDatatypes();
    EXPECT_EQ("Datatype (xattr) not enabled for the connection",
              validate_error_context(cb::mcbp::ClientOpcode::DcpMutation));

    // Request body must be valid Xattr blob if datatype is Xattr
    connection.enableDatatype(cb::mcbp::Feature::XATTR);
    EXPECT_EQ("The provided xattr segment is not valid",
              validate_error_context(cb::mcbp::ClientOpcode::DcpMutation,
                                     cb::mcbp::Status::XattrEinval));
}

TEST_P(CommandSpecificErrorContextTest, DcpDeletion) {
    // JSON is not a valid datatype for DcpDeletion
    uint8_t extlen = sizeof(cb::mcbp::request::DcpDeletionV1Payload);
    header.setExtlen(extlen);
    header.setKeylen(8);
    header.setBodylen(extlen + 8);
    header.setDatatype(cb::mcbp::Datatype::JSON);
    if (GetParam()) {
        // Collections enabled - we require a larger message
        EXPECT_EQ("Request must include extras of length 21",
                  validate_error_context(cb::mcbp::ClientOpcode::DcpDeletion));
    } else {
        EXPECT_EQ("Request datatype invalid",
                  validate_error_context(cb::mcbp::ClientOpcode::DcpDeletion));
    }
}

TEST_P(CommandSpecificErrorContextTest, DcpDeletionV2) {
    // JSON is not a valid datatype for DcpDeletion
    uint8_t extlen = sizeof(cb::mcbp::request::DcpDeletionV2Payload);
    header.setExtlen(extlen);
    header.setKeylen(8);
    header.setBodylen(extlen + 8);
    header.setDatatype(cb::mcbp::Datatype::JSON);
    if (!GetParam()) {
        // Collections enabled - we require a larger message
        EXPECT_EQ("Request must include extras of length 18",
                  validate_error_context(cb::mcbp::ClientOpcode::DcpDeletion));
    } else {
        EXPECT_EQ("Request datatype invalid",
                  validate_error_context(cb::mcbp::ClientOpcode::DcpDeletion));
    }
}

TEST_P(CommandSpecificErrorContextTest, DcpSetVbucketState) {
    // Body state must be between 1 and 4
    header.setExtlen(1);
    header.setKeylen(0);
    header.setBodylen(1);
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)}, true);
    cb::mcbp::request::DcpSetVBucketState extras;
    extras.setState(10);
    builder.setExtras(extras.getBuffer());
    EXPECT_EQ(
            "Request body state invalid",
            validate_error_context(cb::mcbp::ClientOpcode::DcpSetVbucketState));
}

TEST_P(CommandSpecificErrorContextTest, Hello) {
    // Hello requires even body length
    header.setExtlen(0);
    header.setKeylen(0);
    header.setBodylen(3);
    EXPECT_EQ("Request value must be of even length",
              validate_error_context(cb::mcbp::ClientOpcode::Hello));
}

TEST_P(CommandSpecificErrorContextTest, Add) {
    // Collections requires longer key for collection ID
    connection.setCollectionsSupported(true);
    header.setExtlen(8);
    header.setKeylen(1);
    header.setBodylen(9);
    EXPECT_EQ("Key length must be >= 2",
              validate_error_context(cb::mcbp::ClientOpcode::Add));
}

TEST_P(CommandSpecificErrorContextTest, Set) {
    // Collections requires longer key for collection ID
    connection.setCollectionsSupported(true);
    header.setExtlen(8);
    header.setKeylen(1);
    header.setBodylen(9);
    EXPECT_EQ("Key length must be >= 2",
              validate_error_context(cb::mcbp::ClientOpcode::Set));
}

TEST_P(CommandSpecificErrorContextTest, Append) {
    // Collections requires longer key for collection ID
    connection.setCollectionsSupported(true);
    header.setExtlen(0);
    header.setKeylen(1);
    header.setBodylen(2);
    EXPECT_EQ("Key length must be >= 2",
              validate_error_context(cb::mcbp::ClientOpcode::Append));
}

TEST_P(CommandSpecificErrorContextTest, Get) {
    // Collections requires longer key for collection ID
    connection.setCollectionsSupported(true);
    header.setExtlen(0);
    header.setKeylen(1);
    header.setBodylen(1);
    EXPECT_EQ("Key length must be >= 2",
              validate_error_context(cb::mcbp::ClientOpcode::Get));
}

TEST_P(CommandSpecificErrorContextTest, Gat) {
    // Collections requires longer key for collection ID
    connection.setCollectionsSupported(true);
    header.setExtlen(4);
    header.setKeylen(1);
    header.setBodylen(5);
    EXPECT_EQ("Key length must be >= 2",
              validate_error_context(cb::mcbp::ClientOpcode::Gat));
}

TEST_P(CommandSpecificErrorContextTest, Delete) {
    // Collections requires longer key for collection ID
    connection.setCollectionsSupported(true);
    header.setExtlen(0);
    header.setKeylen(1);
    header.setBodylen(1);
    EXPECT_EQ("Key length must be >= 2",
              validate_error_context(cb::mcbp::ClientOpcode::Delete));
}

TEST_P(CommandSpecificErrorContextTest, Increment) {
    // Collections requires longer key for collection ID
    connection.setCollectionsSupported(true);
    header.setExtlen(20);
    header.setKeylen(1);
    header.setBodylen(21);
    EXPECT_EQ("Key length must be >= 2",
              validate_error_context(cb::mcbp::ClientOpcode::Increment));
}

TEST_P(CommandSpecificErrorContextTest, SetCtrlToken) {
    // Set Ctrl Token requires new cas
    header.setExtlen(8);
    header.setKeylen(0);
    header.setBodylen(8);
    auto* req = reinterpret_cast<uint64_t*>(blob + sizeof(cb::mcbp::Request));
    *req = 0;
    EXPECT_EQ("New CAS must be set",
              validate_error_context(cb::mcbp::ClientOpcode::SetCtrlToken));
}

TEST_P(CommandSpecificErrorContextTest, ObserveSeqno) {
    header.setExtlen(0);
    header.setKeylen(0);
    header.setBodylen(4);
    EXPECT_EQ("Request value must be of length 8",
              validate_error_context(cb::mcbp::ClientOpcode::ObserveSeqno));
}

TEST_P(CommandSpecificErrorContextTest, CreateBucket) {
    // Create Bucket has maximum key length of 100
    header.setExtlen(0);
    header.setKeylen(101);
    header.setBodylen(102);
    EXPECT_EQ("Name too long (exceeds 100)",
              validate_error_context(cb::mcbp::ClientOpcode::CreateBucket));
}

TEST_P(CommandSpecificErrorContextTest, GetAllVbSeqnos) {
    // Extlen must be zero or sizeof(vbucket_state_t)
    header.setExtlen(sizeof(vbucket_state_t) + 1);
    header.setKeylen(0);
    header.setBodylen(sizeof(vbucket_state_t) + 1);
    EXPECT_EQ("Request extras must be of length 0 or " +
                      std::to_string(sizeof(vbucket_state_t)) + " or " +
                      std::to_string(sizeof(vbucket_state_t) +
                                     sizeof(CollectionIDType)),
              validate_error_context(cb::mcbp::ClientOpcode::GetAllVbSeqnos));

    // VBucket state must be between 1 and 4
    header.setExtlen(4);
    header.setKeylen(0);
    header.setBodylen(4);
    auto* req =
            reinterpret_cast<protocol_binary_request_get_all_vb_seqnos*>(blob);
    req->body.state = static_cast<RequestedVBState>(5);
    EXPECT_EQ("Request vbucket state invalid",
              validate_error_context(cb::mcbp::ClientOpcode::GetAllVbSeqnos));
}

TEST_P(CommandSpecificErrorContextTest, GetMeta) {
    // Collections requires longer key for collection ID
    connection.setCollectionsSupported(true);
    header.setExtlen(0);
    header.setKeylen(1);
    header.setBodylen(1);
    EXPECT_EQ("Key length must be >= 2",
              validate_error_context(cb::mcbp::ClientOpcode::GetMeta));

    // Get Meta requires extlen of 0 or 1
    connection.setCollectionsSupported(false);
    header.setExtlen(2);
    header.setKeylen(4);
    header.setBodylen(6);
    EXPECT_EQ("Request extras must be of length 0 or 1",
              validate_error_context(cb::mcbp::ClientOpcode::GetMeta));

    // If extlen is 1, then the extras byte must be 1 or 2
    header.setExtlen(1);
    header.setKeylen(4);
    header.setBodylen(5);
    blob[sizeof(request)] = 5;
    EXPECT_EQ("Request extras invalid",
              validate_error_context(cb::mcbp::ClientOpcode::GetMeta));
}

TEST_P(CommandSpecificErrorContextTest, MutateWithMeta) {
    // Mutate with meta commands must have extlen of 24, 26, 28 or 30
    header.setExtlen(20);
    header.setKeylen(10);
    header.setBodylen(30);
    EXPECT_EQ("Request extras invalid",
              validate_error_context(cb::mcbp::ClientOpcode::AddWithMeta));

    // If datatype is Xattr, xattr must be enabled on connection
    header.setExtlen(24);
    header.setKeylen(10);
    header.setBodylen(34);
    header.setDatatype(cb::mcbp::Datatype::Xattr);
    connection.disableAllDatatypes();
    EXPECT_EQ("Datatype (xattr) not enabled for the connection",
              validate_error_context(cb::mcbp::ClientOpcode::AddWithMeta));

    // If datatype is Xattr, command value must be valid xattr blob
    connection.enableDatatype(cb::mcbp::Feature::XATTR);
    EXPECT_EQ("The provided xattr segment is not valid",
              validate_error_context(cb::mcbp::ClientOpcode::AddWithMeta,
                                     cb::mcbp::Status::XattrEinval));

    // Collections requires longer key for collection ID
    connection.setCollectionsSupported(true);
    header.setExtlen(24);
    header.setKeylen(1);
    header.setBodylen(25);
    header.setDatatype(cb::mcbp::Datatype::Raw);
    EXPECT_EQ("Key length must be >= 2",
              validate_error_context(cb::mcbp::ClientOpcode::AddWithMeta));
}

TEST_P(CommandSpecificErrorContextTest, GetErrmap) {
    header.setExtlen(0);
    header.setKeylen(0);
    header.setBodylen(4);
    EXPECT_EQ("Request value must be of length 2",
              validate_error_context(cb::mcbp::ClientOpcode::GetErrorMap));

    // Get Errmap command requires vbucket id 0
    header.setExtlen(0);
    header.setKeylen(0);
    header.setBodylen(2);
    header.setVBucket(Vbid(1));
    EXPECT_EQ("Request vbucket id must be 0",
              validate_error_context(cb::mcbp::ClientOpcode::GetErrorMap));
}

TEST_P(CommandSpecificErrorContextTest, GetLocked) {
    header.setExtlen(2);
    header.setKeylen(8);
    header.setBodylen(10);
    EXPECT_EQ("Request extras must be of length 0 or 4",
              validate_error_context(cb::mcbp::ClientOpcode::GetLocked));

    // Collections requires longer key for collection ID
    connection.setCollectionsSupported(true);
    header.setExtlen(0);
    header.setKeylen(1);
    header.setBodylen(1);
    EXPECT_EQ("Key length must be >= 2",
              validate_error_context(cb::mcbp::ClientOpcode::GetLocked));
}

TEST_P(CommandSpecificErrorContextTest, UnlockKey) {
    // Collections requires longer key for collection ID
    connection.setCollectionsSupported(true);
    header.setExtlen(0);
    header.setKeylen(1);
    header.setBodylen(1);
    header.setCas(10);
    EXPECT_EQ("Key length must be >= 2",
              validate_error_context(cb::mcbp::ClientOpcode::UnlockKey));
}

TEST_P(CommandSpecificErrorContextTest, EvictKey) {
    // Collections requires longer key for collection ID
    connection.setCollectionsSupported(true);
    header.setExtlen(0);
    header.setKeylen(1);
    header.setBodylen(1);
    EXPECT_EQ("Key length must be >= 2",
              validate_error_context(cb::mcbp::ClientOpcode::EvictKey));
}

TEST_P(CommandSpecificErrorContextTest, CollectionsSetManifest) {
    // VBucket ID must not be set
    connection.setCollectionsSupported(true);
    header.setExtlen(0);
    header.setKeylen(0);
    header.setBodylen(4);
    header.setVBucket(Vbid(1));
    EXPECT_EQ("Request vbucket id must be 0",
              validate_error_context(
                      cb::mcbp::ClientOpcode::CollectionsSetManifest));
}

TEST_P(CommandSpecificErrorContextTest, CollectionsGetManifest) {
    connection.setCollectionsSupported(true);
    header.setExtlen(0);
    header.setKeylen(0);
    header.setBodylen(0);
    header.setVBucket(Vbid(1));
    EXPECT_EQ("Request vbucket id must be 0",
              validate_error_context(
                      cb::mcbp::ClientOpcode::CollectionsGetManifest));
}

TEST_P(CommandSpecificErrorContextTest, CollectionsGetID) {
    connection.setCollectionsSupported(true);
    header.setExtlen(0);
    header.setKeylen(1);
    header.setBodylen(2);
    EXPECT_EQ("Cannot set both key and value",
              validate_error_context(cb::mcbp::ClientOpcode::CollectionsGetID));
    header.setKeylen(1);
    header.setBodylen(1);
    header.setVBucket(Vbid(1));
    EXPECT_EQ("Request vbucket id must be 0",
              validate_error_context(cb::mcbp::ClientOpcode::CollectionsGetID));
}

class GetRandomKeyValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
    GetRandomKeyValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::GetRandomKey,
                                       &request);
    }
};

TEST_P(GetRandomKeyValidatorTest, CorrectMessage) {
    if (GetParam()) {
        // Collections expects 4-byte extras
        req.setExtlen(4);
        req.setBodylen(4);
    }
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GetRandomKeyValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    req.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetRandomKeyValidatorTest, InvalidDatatype) {
    req.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetRandomKeyValidatorTest, InvalidCas) {
    req.setCas(0xff);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetRandomKeyValidatorTest, InvalidKey) {
    req.setKeylen(2);
    req.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetRandomKeyValidatorTest, InvalidBodylen) {
    req.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class DelVBucketValidatorTest : public ::testing::WithParamInterface<bool>,
                                public ValidatorTest {
public:
    DelVBucketValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DelVbucket,
                                       &request);
    }
};

TEST_P(DelVBucketValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(DelVBucketValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    req.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DelVBucketValidatorTest, InvalidDatatype) {
    req.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DelVBucketValidatorTest, InvalidCas) {
    req.setCas(0xff);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DelVBucketValidatorTest, InvalidKey) {
    req.setKeylen(2);
    req.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DelVBucketValidatorTest, Bodylen) {
    // The command allows for the client to specify "async=0", but there
    // was no test for "unsupported" values.. Just verify that we don't
    // barf out on any ohter.
    req.setBodylen(8);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
    req.setBodylen(32);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

class GetVBucketValidatorTest : public ::testing::WithParamInterface<bool>,
                                public ValidatorTest {
public:
    GetVBucketValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::GetVbucket,
                                       &request);
    }
};

TEST_P(GetVBucketValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GetVBucketValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    req.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetVBucketValidatorTest, InvalidDatatype) {
    req.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetVBucketValidatorTest, IvalidCas) {
    req.setCas(0xff);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetVBucketValidatorTest, Key) {
    req.setKeylen(2);
    req.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GetVBucketValidatorTest, InvalidBodylen) {
    req.setBodylen(8);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         AddValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         SetReplaceValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         AppendPrependValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         DeleteValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         IncrementDecrementValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         QuitValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         NoopValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         VersionValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         StatValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         VerbosityValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         HelloValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         SaslListMechValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         SaslAuthValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         GetErrmapValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         IoctlGetValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         IoctlSetValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         AuditPutValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         AuditConfigReloadValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         ShutdownValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         ObserveSeqnoValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         CmdTimerValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         GetCtrlTokenValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         SetCtrlTokenValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         GetAllVbSeqnoValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         GetLockedValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         UnlockValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         ConfigReloadValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         EvictKeyValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         RevokeUserPermissionsValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         GetRandomKeyValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         DelVBucketValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         GetVBucketValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         ErrorContextTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         CommandSpecificErrorContextTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

} // namespace mcbp::test
