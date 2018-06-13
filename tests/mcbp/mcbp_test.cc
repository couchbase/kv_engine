/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#include "config.h"
#include "mcbp_test.h"
#include "utilities/protocol2text.h"

#include <event2/event.h>
#include <mcbp/protocol/header.h>
#include <memcached/protocol_binary.h>
#include <gsl/gsl>
#include <memory>

/**
 * Test all of the command validators we've got to ensure that they
 * catch broken packets. There is still a high number of commands we
 * don't have any command validators for...
 */
namespace mcbp {
namespace test {

ValidatorTest::ValidatorTest()
    : request(*reinterpret_cast<protocol_binary_request_no_extras*>(blob)) {
}

void ValidatorTest::SetUp() {
    settings.setXattrEnabled(true);
    McbpValidatorChains::initializeMcbpValidatorChains(validatorChains);
    memset(request.bytes, 0, sizeof(request));
    request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
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
        setPacket(PacketContent::Full, buffer);
    }
};

protocol_binary_response_status
ValidatorTest::validate(protocol_binary_command opcode, void* packet) {
    // Mockup a McbpConnection and Cookie for the validator chain
    connection.enableDatatype(cb::mcbp::Feature::XATTR);
    const auto& req = *reinterpret_cast<const cb::mcbp::Header*>(packet);
    const size_t size = sizeof(req) + req.getBodylen();
    cb::const_byte_buffer buffer{static_cast<uint8_t*>(packet), size};
    MockCookie cookie(connection, buffer);
    return validatorChains.invoke(opcode, cookie);
}

enum class GetOpcodes : uint8_t {
    Get = PROTOCOL_BINARY_CMD_GET,
    GetQ = PROTOCOL_BINARY_CMD_GETQ,
    GetK = PROTOCOL_BINARY_CMD_GETK,
    GetKQ = PROTOCOL_BINARY_CMD_GETKQ,
    GetMeta = PROTOCOL_BINARY_CMD_GET_META,
    GetQMeta = PROTOCOL_BINARY_CMD_GETQ_META
};

std::string to_string(const GetOpcodes& opcode) {
#ifdef JETBRAINS_CLION_IDE
    // CLion don't properly parse the output when the
    // output gets written as the string instead of the
    // number. This makes it harder to debug the tests
    // so let's just disable it while we're waiting
    // for them to supply a fix.
    // See https://youtrack.jetbrains.com/issue/CPP-6039
    return std::to_string(static_cast<int>(opcode));
#else
    switch (opcode) {
    case GetOpcodes::Get:
        return "Get";
    case GetOpcodes::GetQ:
        return "GetQ";
    case GetOpcodes::GetK:
        return "GetK";
    case GetOpcodes::GetKQ:
        return "GetKQ";
    case GetOpcodes::GetMeta:
        return "GetMeta";
    case GetOpcodes::GetQMeta:
        return "GetQMeta";
    }
    throw std::invalid_argument("to_string(): unknown opcode");
#endif
}

std::ostream& operator<<(std::ostream& os, const GetOpcodes& o) {
    os << to_string(o);
    return os;
}

// Test the validators for GET, GETQ, GETK, GETKQ, GET_META and GETQ_META
class GetValidatorTest : public ValidatorTest,
                         public ::testing::WithParamInterface<GetOpcodes> {
public:
    void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 0;
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

    GetValidatorTest()
        : ValidatorTest(),
          bodylen(request.message.header.request.bodylen) {
        // empty
    }

protected:

    protocol_binary_response_status validateExtendedExtlen(uint8_t version) {
        bodylen = htonl(ntohl(bodylen) + 1);
        request.message.header.request.extlen = 1;
        blob[sizeof(protocol_binary_request_get)] = version;
        return validate();
    }

    protocol_binary_response_status validate() {
        auto opcode = (protocol_binary_command)GetParam();
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }

    uint32_t& bodylen;
};

TEST_P(GetValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(GetValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetValidatorTest, ExtendedExtlenV1) {
    switch (GetParam()) {
    case GetOpcodes::Get:
    case GetOpcodes::GetQ:
    case GetOpcodes::GetK:
    case GetOpcodes::GetKQ:
        // Extended extlen is only supported for *Meta
        return;
    case GetOpcodes::GetMeta:
    case GetOpcodes::GetQMeta:
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validateExtendedExtlen(1));
        break;
    }
}

TEST_P(GetValidatorTest, ExtendedExtlenV2) {
    switch (GetParam()) {
    case GetOpcodes::Get:
    case GetOpcodes::GetQ:
    case GetOpcodes::GetK:
    case GetOpcodes::GetKQ:
        // Extended extlen is only supported for *Meta
        return;
    case GetOpcodes::GetMeta:
    case GetOpcodes::GetQMeta:
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validateExtendedExtlen(2));
        break;
    }
}

TEST_P(GetValidatorTest, InvalidExtendedExtlenVersion) {
    switch (GetParam()) {
    case GetOpcodes::Get:
    case GetOpcodes::GetQ:
    case GetOpcodes::GetK:
    case GetOpcodes::GetKQ:
        // Extended extlen is only supported for *Meta
        return;
    case GetOpcodes::GetMeta:
    case GetOpcodes::GetQMeta:
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validateExtendedExtlen(3));
        break;
    }
}

TEST_P(GetValidatorTest, InvalidExtlen) {
    bodylen = htonl(ntohl(bodylen) + 21);
    request.message.header.request.extlen = 21;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetValidatorTest, NoKey) {
    request.message.header.request.keylen = 0;
    bodylen = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}


// @todo add test case for the extra legal modes for the
// get meta case

INSTANTIATE_TEST_CASE_P(GetOpcodes,
                        GetValidatorTest,
                        ::testing::Values(GetOpcodes::Get,
                                          GetOpcodes::GetQ,
                                          GetOpcodes::GetK,
                                          GetOpcodes::GetKQ,
                                          GetOpcodes::GetMeta,
                                          GetOpcodes::GetQMeta),
                        ::testing::PrintToStringParamName());

// Test ADD & ADDQ
class AddValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 8;
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(20);
    }

protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_F(AddValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_ADD));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_ADDQ));
}

TEST_F(AddValidatorTest, NoValue) {
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_ADD));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_ADDQ));
}

TEST_F(AddValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_ADD));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_ADDQ));
}

TEST_F(AddValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_ADD));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_ADDQ));
}

TEST_F(AddValidatorTest, NoKey) {
    request.message.header.request.keylen = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_ADD));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_ADDQ));
}

TEST_F(AddValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_ADD));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_ADDQ));
}

// Test SET, SETQ, REPLACE, REPLACEQ
class SetReplaceValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 8;
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(20);
    }

protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_F(SetReplaceValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_SET));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_SETQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_REPLACE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_REPLACEQ));
}

TEST_F(SetReplaceValidatorTest, NoValue) {
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_SET));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_SETQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_REPLACE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_REPLACEQ));
}

TEST_F(SetReplaceValidatorTest, Cas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_SET));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_SETQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_REPLACE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_REPLACEQ));
}

TEST_F(SetReplaceValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SET));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SETQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_REPLACE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_REPLACEQ));
}

TEST_F(SetReplaceValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SET));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SETQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_REPLACE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_REPLACEQ));
}

TEST_F(SetReplaceValidatorTest, NoKey) {
    request.message.header.request.keylen = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SET));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SETQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_REPLACE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_REPLACEQ));
}

// Test Append[q] and Prepend[q]
class AppendPrependValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(20);
    }

protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_F(AppendPrependValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_APPEND));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_APPENDQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_PREPEND));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_PREPENDQ));
}

TEST_F(AppendPrependValidatorTest, NoValue) {
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_APPEND));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_APPENDQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_PREPEND));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_PREPENDQ));
}

TEST_F(AppendPrependValidatorTest, Cas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_APPEND));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_APPENDQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_PREPEND));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_PREPENDQ));
}

TEST_F(AppendPrependValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_APPEND));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_APPENDQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_PREPEND));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_PREPENDQ));
}

TEST_F(AppendPrependValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_APPEND));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_APPENDQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_PREPEND));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_PREPENDQ));
}

TEST_F(AppendPrependValidatorTest, NoKey) {
    request.message.header.request.keylen = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_APPEND));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_APPENDQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_PREPEND));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_PREPENDQ));
}

// Test DELETE & DELETEQ
class DeleteValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
    }

protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_F(DeleteValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_DELETE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_DELETEQ));
}

TEST_F(DeleteValidatorTest, Cas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_DELETE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_DELETEQ));
}

TEST_F(DeleteValidatorTest, WithValue) {
    request.message.header.request.bodylen = htonl(20);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DELETE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DELETEQ));
}

TEST_F(DeleteValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DELETE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DELETEQ));
}

TEST_F(DeleteValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DELETE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DELETEQ));
}

TEST_F(DeleteValidatorTest, NoKey) {
    request.message.header.request.keylen = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DELETE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DELETEQ));
}

TEST_F(DeleteValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DELETE));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DELETEQ));
}

// Test INCREMENT[q] and DECREMENT[q]
class IncrementDecrementValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 20;
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(30);
    }

protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_F(IncrementDecrementValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_INCREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_INCREMENTQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_DECREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_DECREMENTQ));
}

TEST_F(IncrementDecrementValidatorTest, Cas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_INCREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_INCREMENTQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DECREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DECREMENTQ));
}

TEST_F(IncrementDecrementValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_INCREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_INCREMENTQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DECREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DECREMENTQ));
}

TEST_F(IncrementDecrementValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_INCREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_INCREMENTQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DECREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DECREMENTQ));
}

TEST_F(IncrementDecrementValidatorTest, NoKey) {
    request.message.header.request.keylen = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_INCREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_INCREMENTQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DECREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DECREMENTQ));
}

TEST_F(IncrementDecrementValidatorTest, WithValue) {
    request.message.header.request.bodylen = htonl(40);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_INCREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_INCREMENTQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DECREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DECREMENTQ));
}

TEST_F(IncrementDecrementValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_INCREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_INCREMENTQ));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DECREMENT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_DECREMENTQ));
}

// Test QUIT & QUITQ
class QuitValidatorTest : public ValidatorTest {
protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_F(QuitValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_QUIT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_QUITQ));
}

TEST_F(QuitValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_QUIT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_QUITQ));
}

TEST_F(QuitValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_QUIT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_QUITQ));
}

TEST_F(QuitValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = ntohl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_QUIT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_QUITQ));
}

TEST_F(QuitValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_QUIT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_QUITQ));
}

TEST_F(QuitValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_QUIT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_QUITQ));
}

TEST_F(QuitValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_QUIT));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_QUITQ));
}

// Test FLUSH & FLUSHQ
class FlushValidatorTest : public ValidatorTest {
protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_F(FlushValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_FLUSH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_FLUSHQ));
}

TEST_F(FlushValidatorTest, CorrectMessageWithTime) {
    request.message.header.request.extlen = 4;
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_FLUSH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_FLUSHQ));
}

TEST_F(FlushValidatorTest, CorrectMessageWithUnsupportedTime) {
    request.message.header.request.extlen = 4;
    request.message.header.request.bodylen = htonl(4);
    *reinterpret_cast<uint32_t*>(request.bytes + sizeof(request.bytes)) = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED,
              validate(PROTOCOL_BINARY_CMD_FLUSH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED,
              validate(PROTOCOL_BINARY_CMD_FLUSHQ));
}

TEST_F(FlushValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_FLUSH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_FLUSHQ));
}

TEST_F(FlushValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_FLUSH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_FLUSHQ));
}

TEST_F(FlushValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = ntohl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_FLUSH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_FLUSHQ));
}

TEST_F(FlushValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_FLUSH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_FLUSHQ));
}

TEST_F(FlushValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_FLUSH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_FLUSHQ));
}

TEST_F(FlushValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_FLUSH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_FLUSHQ));
}

// test Noop
class NoopValidatorTest : public ValidatorTest {
protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_NOOP,
                                       static_cast<void*>(&request));
    }
};

TEST_F(NoopValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(NoopValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(NoopValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(NoopValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = ntohs(32);
    request.message.header.request.bodylen = htonl(32);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(NoopValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(100);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(NoopValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(NoopValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// test version
class VersionValidatorTest : public ValidatorTest {
protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_VERSION,
                                       static_cast<void*>(&request));
    }
};

TEST_F(VersionValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(VersionValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(VersionValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(VersionValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = ntohs(32);
    request.message.header.request.bodylen = htonl(32);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(VersionValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(100);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(VersionValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(VersionValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// test stat
class StatValidatorTest : public ValidatorTest {
protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_STAT,
                                       static_cast<void*>(&request));
    }
};

TEST_F(StatValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(StatValidatorTest, WithKey) {
    request.message.header.request.keylen = htons(21);
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(StatValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(StatValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(StatValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(100);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(StatValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(StatValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// test verbosity
class VerbosityValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 4;
        request.message.header.request.bodylen = htonl(4);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_VERBOSITY,
                                       static_cast<void*>(&request));
    }
};

TEST_F(VerbosityValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(VerbosityValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(VerbosityValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(VerbosityValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(100);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(VerbosityValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(VerbosityValidatorTest, InvalidKey) {
    request.message.header.request.keylen = htons(21);
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(VerbosityValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// test HELLO
class HelloValidatorTest : public ValidatorTest {
protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_HELLO,
                                       static_cast<void*>(&request));
    }
};

TEST_F(HelloValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(HelloValidatorTest, MultipleFeatures) {
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
    request.message.header.request.bodylen = htonl(6);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(HelloValidatorTest, WithKey) {
    request.message.header.request.keylen = htons(21);
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(HelloValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(HelloValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(HelloValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(1);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(HelloValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(HelloValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// test SASL_LIST_MECHS
class SaslListMechValidatorTest : public ValidatorTest {
protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_SASL_LIST_MECHS,
                                       static_cast<void*>(&request));
    }
};

TEST_F(SaslListMechValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(SaslListMechValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(SaslListMechValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(SaslListMechValidatorTest, InvalidKey) {
    request.message.header.request.keylen = htons(21);
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(SaslListMechValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(1);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(SaslListMechValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(SaslListMechValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// test SASL_AUTH
class SaslAuthValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
    }

protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_F(SaslAuthValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_SASL_AUTH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_SASL_STEP));
}

TEST_F(SaslAuthValidatorTest, WithChallenge) {
    request.message.header.request.bodylen = htonl(20);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_SASL_AUTH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_SASL_STEP));
}

TEST_F(SaslAuthValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SASL_AUTH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SASL_STEP));
}

TEST_F(SaslAuthValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SASL_AUTH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SASL_STEP));
}

TEST_F(SaslAuthValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 0;
    request.message.header.request.bodylen = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SASL_AUTH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SASL_STEP));
}

TEST_F(SaslAuthValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SASL_AUTH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SASL_STEP));
}

TEST_F(SaslAuthValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SASL_AUTH));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SASL_STEP));
}

class GetErrmapValidatorTest : public ValidatorTest {
protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_GET_ERROR_MAP,
                                       static_cast<void*>(&request));
    }
};

TEST_F(GetErrmapValidatorTest, CorrectMessage) {
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(GetErrmapValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(GetErrmapValidatorTest, MissingBody) {
    request.message.header.request.bodylen = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// test IOCTL_GET
class IoctlGetValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
    }

protected:
    // #defined in memcached.h..
    const int IOCTL_KEY_LENGTH = 128;

    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_IOCTL_GET,
                                       static_cast<void*>(&request));
    }
};

TEST_F(IoctlGetValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(IoctlGetValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(IoctlGetValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(IoctlGetValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 0;
    request.message.header.request.bodylen = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
    request.message.header.request.keylen = htons(IOCTL_KEY_LENGTH + 1);
    request.message.header.request.bodylen = htonl(IOCTL_KEY_LENGTH + 1);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(IoctlGetValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(IoctlGetValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(IoctlGetValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(20);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// test IOCTL_SET
class IoctlSetValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
    }

protected:
    // #defined in memcached.h..
    const int IOCTL_KEY_LENGTH = 128;
    const int IOCTL_VAL_LENGTH = 128;

    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_IOCTL_SET,
                                       static_cast<void*>(&request));
    }
};

TEST_F(IoctlSetValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(IoctlSetValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(IoctlSetValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(IoctlSetValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 0;
    request.message.header.request.bodylen = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
    request.message.header.request.keylen = htons(IOCTL_KEY_LENGTH + 1);
    request.message.header.request.bodylen = htonl(IOCTL_KEY_LENGTH + 1);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(IoctlSetValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(IoctlSetValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(IoctlSetValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(IOCTL_VAL_LENGTH + 11);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(IoctlSetValidatorTest, ValidBody) {
    request.message.header.request.bodylen = htonl(IOCTL_VAL_LENGTH + 10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

// test AUDIT_PUT
class AuditPutValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 4;
        request.message.header.request.bodylen = htonl(10);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_AUDIT_PUT,
                                       static_cast<void*>(&request));
    }
};

TEST_F(AuditPutValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(AuditPutValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(AuditPutValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(AuditPutValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(15);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(AuditPutValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(AuditPutValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(AuditPutValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// Test audit_config_reload
class AuditConfigReloadValidatorTest : public ValidatorTest {
protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_AUDIT_CONFIG_RELOAD,
                                       static_cast<void*>(&request));
    }
};

TEST_F(AuditConfigReloadValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(AuditConfigReloadValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(AuditConfigReloadValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(AuditConfigReloadValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(AuditConfigReloadValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(AuditConfigReloadValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(AuditConfigReloadValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// Test shutdown
class ShutdownValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.cas = 1;
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_SHUTDOWN,
                                       static_cast<void*>(&request));
    }
};

TEST_F(ShutdownValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(ShutdownValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(ShutdownValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(ShutdownValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(ShutdownValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(ShutdownValidatorTest, InvalidCas) {
    request.message.header.request.cas = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(ShutdownValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

class DcpOpenValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 8;
        request.message.header.request.keylen = htons(2);
        request.message.header.request.bodylen = htonl(10);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_OPEN,
                                       static_cast<void*>(&request));
    }

    protocol_binary_request_dcp_open &request = *reinterpret_cast<protocol_binary_request_dcp_open*>(blob);
};

TEST_F(DcpOpenValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_F(DcpOpenValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpOpenValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 9;
    request.message.header.request.bodylen = htonl(11);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpOpenValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 0;
    request.message.header.request.bodylen = htonl(8);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpOpenValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpOpenValidatorTest, ValueButNoCollections) {
    request.message.header.request.bodylen = htonl(10 + 20);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpOpenValidatorTest, CorrectMessageValueCollections) {
    request.message.header.request.bodylen = htonl(10 + 20);
    request.message.body.flags = ntohl(DCP_OPEN_COLLECTIONS);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

class DcpAddStreamValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 4;
        request.message.header.request.bodylen = htonl(4);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_ADD_STREAM,
                                       static_cast<void*>(&request));
    }
};

TEST_F(DcpAddStreamValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_F(DcpAddStreamValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpAddStreamValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpAddStreamValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(8);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpAddStreamValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpAddStreamValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

class DcpCloseStreamValidatorTest : public ValidatorTest {
protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_CLOSE_STREAM,
                                       static_cast<void*>(&request));
    }
};

TEST_F(DcpCloseStreamValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_F(DcpCloseStreamValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpCloseStreamValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpCloseStreamValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpCloseStreamValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpCloseStreamValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

class DcpGetFailoverLogValidatorTest : public ValidatorTest {
protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG,
                                       static_cast<void*>(&request));
    }
};

TEST_F(DcpGetFailoverLogValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_F(DcpGetFailoverLogValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpGetFailoverLogValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpGetFailoverLogValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpGetFailoverLogValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpGetFailoverLogValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

class DcpStreamReqValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 48;
        request.message.header.request.bodylen = htonl(48);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_STREAM_REQ,
                                       static_cast<void*>(&request));
    }
};

TEST_F(DcpStreamReqValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_F(DcpStreamReqValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpStreamReqValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpStreamReqValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(54);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpStreamReqValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}
// Can the stream req also conain data?
// TEST_F(DcpStreamReqValidatorTest, InvalidBody) {
//     request.message.header.request.bodylen = htonl(12);
//     EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
// }

class DcpStreamEndValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 4;
        request.message.header.request.bodylen = htonl(4);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_STREAM_END,
                                       static_cast<void*>(&request));
    }
};

TEST_F(DcpStreamEndValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_F(DcpStreamEndValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpStreamEndValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpStreamEndValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(8);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpStreamEndValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpStreamEndValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

class DcpSnapshotMarkerValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 20;
        request.message.header.request.bodylen = htonl(20);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER,
                                       static_cast<void*>(&request));
    }
};

TEST_F(DcpSnapshotMarkerValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_F(DcpSnapshotMarkerValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpSnapshotMarkerValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(21);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpSnapshotMarkerValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 32;
    request.message.header.request.bodylen = htonl(52);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpSnapshotMarkerValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(100);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpSnapshotMarkerValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

/**
 * Test class for DcpMutation validation - the bool parameter toggles
 * collections on/off (as that subtly changes the encoding of a mutation)
 */
class DcpMutationValidatorTest : public ValidatorTest,
                                 public ::testing::WithParamInterface<bool> {
public:
    DcpMutationValidatorTest()
        : request(GetParam(),
                  0 /*opaque*/,
                  0 /*vbucket*/,
                  0 /*cas*/,
                  1 /*keylen*/,
                  0 /*valueLen*/,
                  PROTOCOL_BINARY_RAW_BYTES,
                  0 /*bySeqno*/,
                  0 /*revSeqno*/,
                  0 /*flags*/,
                  0 /*expiration*/,
                  0 /*lockTime*/,
                  0 /*nmeta*/,
                  0 /*nru*/,
                  0 /*collectionLen*/) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        connection.setDcpCollectionAware(GetParam());
    }

protected:
    protocol_binary_response_status validate() {
        std::copy(request.bytes, request.bytes + sizeof(request.bytes), blob);
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_MUTATION,
                                       static_cast<void*>(blob));
    }

    protocol_binary_request_dcp_mutation request;
};

INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpMutationValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());

TEST_P(DcpMutationValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_P(DcpMutationValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpMutationValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(22);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpMutationValidatorTest, InvalidExtlenCollections) {
    request.message.header.request.extlen =
            protocol_binary_request_dcp_mutation::getExtrasLength(!GetParam());
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpMutationValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 0;
    request.message.header.request.bodylen = htonl(31);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

/**
 * Test class for DcpDeletion validation - the bool parameter toggles
 * collections on/off (as that subtly changes the encoding of a deletion)
 */
class DcpDeletionValidatorTest : public ValidatorTest,
                                 public ::testing::WithParamInterface<bool> {
public:
    DcpDeletionValidatorTest()
        : ValidatorTest(),
          request(GetParam() ? makeV2() : makeV1()),
          header(request->getHeader()) {
        header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_DELETION;
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        connection.setDcpCollectionAware(GetParam());
    }

protected:
    protocol_binary_response_status validate() {
        std::copy(request->getBytes(),
                  request->getBytes() + request->getSizeofBytes(),
                  blob);
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_DELETION,
                                       static_cast<void*>(blob));
    }

    class Request {
    public:
        virtual ~Request() = default;
        virtual protocol_binary_request_header& getHeader() = 0;

        virtual uint8_t* getBytes() = 0;

        virtual size_t getSizeofBytes() = 0;
    };

    class RequestV1 : public Request {
    public:
        RequestV1()
            : request(0 /*opaque*/,
                      0 /*vbucket*/,
                      0 /*cas*/,
                      2 /*keylen*/,
                      0 /*valueLen*/,
                      PROTOCOL_BINARY_RAW_BYTES,
                      0 /*bySeqno*/,
                      0 /*revSeqno*/,
                      0 /*nmeta*/) {
        }
        protocol_binary_request_header& getHeader() override {
            return request.message.header;
        }

        uint8_t* getBytes() override {
            return request.bytes;
        }

        size_t getSizeofBytes() override {
            return sizeof(request.bytes);
        }

    private:
        protocol_binary_request_dcp_deletion request;
    };

    class RequestV2 : public Request {
    public:
        RequestV2()
            : request(0 /*opaque*/,
                      0 /*vbucket*/,
                      0 /*cas*/,
                      2 /*keylen*/,
                      0 /*valueLen*/,
                      PROTOCOL_BINARY_RAW_BYTES,
                      0 /*bySeqno*/,
                      0 /*revSeqno*/,
                      0, /*deleteTime*/
                      0 /*collectionLen*/) {
        }
        protocol_binary_request_header& getHeader() override {
            return request.message.header;
        }

        uint8_t* getBytes() override {
            return request.bytes;
        }

        size_t getSizeofBytes() override {
            return sizeof(request.bytes);
        }

    private:
        protocol_binary_request_dcp_deletion_v2 request;
    };

    std::unique_ptr<Request> makeV1() {
        return std::make_unique<RequestV1>();
    }

    std::unique_ptr<Request> makeV2() {
        return std::make_unique<RequestV2>();
    }

    std::unique_ptr<Request> request;
    protocol_binary_request_header& header;
};

INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpDeletionValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());

TEST_P(DcpDeletionValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_P(DcpDeletionValidatorTest, InvalidMagic) {
    header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpDeletionValidatorTest, ValidDatatype) {
    using cb::mcbp::Datatype;
    const std::array<uint8_t, 3> datatypes = {
            {uint8_t(Datatype::Raw),
             uint8_t(Datatype::Xattr),
             uint8_t(Datatype::Xattr) | uint8_t(Datatype::Snappy)}};

    for (auto valid : datatypes) {
        header.request.datatype = valid;
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate())
                    << "Testing valid datatype:" << int(valid);
    }
}

TEST_P(DcpDeletionValidatorTest, InvalidDatatype) {
    using cb::mcbp::Datatype;
    const std::array<uint8_t, 3> datatypes = {
            {uint8_t(Datatype::JSON),
             uint8_t(Datatype::Snappy),
             uint8_t(Datatype::Snappy) | uint8_t(Datatype::JSON)}};

    for (auto invalid : datatypes) {
        header.request.datatype = invalid;
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate())
                    << "Testing invalid datatype:" << int(invalid);
    }
}

TEST_P(DcpDeletionValidatorTest, InvalidExtlen) {
    header.request.extlen = 5;
    header.request.bodylen = htonl(7);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpDeletionValidatorTest, InvalidExtlenCollections) {
    // Flip extlen, so when not collections, set the length collections uses
    header.request.extlen =
            GetParam() ? protocol_binary_request_dcp_deletion::extlen
                       : protocol_binary_request_dcp_deletion_v2::extlen;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpDeletionValidatorTest, InvalidKeylen) {
    header.request.keylen = 0;
    header.request.bodylen = htonl(18);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpDeletionValidatorTest, WithValue) {
    header.request.bodylen = htonl(100);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

/**
 * Test class for DcpExpiration validation - the bool parameter toggles
 * collections on/off (as that subtly changes the encoding of an expiration)
 */
class DcpExpirationValidatorTest : public ValidatorTest,
                                   public ::testing::WithParamInterface<bool> {
public:
    DcpExpirationValidatorTest()
        : ValidatorTest(),
          request(GetParam(),
                  0 /*opaque*/,
                  0 /*vbucket*/,
                  0 /*cas*/,
                  2 /*keylen*/,
                  0 /*valueLen*/,
                  PROTOCOL_BINARY_RAW_BYTES,
                  0 /*bySeqno*/,
                  0 /*revSeqno*/,
                  0 /*nmeta*/,
                  0 /*collectionLen*/) {
        request.message.header.request.opcode =
            (uint8_t)PROTOCOL_BINARY_CMD_DCP_EXPIRATION;
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        connection.setDcpCollectionAware(GetParam());
    }

protected:
    protocol_binary_response_status validate() {
        std::copy(request.bytes, request.bytes + sizeof(request.bytes), blob);
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_EXPIRATION,
                                       static_cast<void*>(blob));
    }

    protocol_binary_request_dcp_expiration request;
};

INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DcpExpirationValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());

TEST_P(DcpExpirationValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_P(DcpExpirationValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpExpirationValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(7);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpExpirationValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 0;
    request.message.header.request.bodylen = htonl(18);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(DcpExpirationValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(100);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

class DcpFlushValidatorTest : public ValidatorTest {
protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_FLUSH,
                                       static_cast<void*>(&request));
    }
};

TEST_F(DcpFlushValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_F(DcpFlushValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpFlushValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpFlushValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpFlushValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpFlushValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

class DcpSetVbucketStateValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 1;
        request.message.header.request.bodylen = htonl(1);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
        request.message.body.state = 1;
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(
            PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE,
            static_cast<void*>(&request));
    }

    protocol_binary_request_dcp_set_vbucket_state &request =
       *reinterpret_cast<protocol_binary_request_dcp_set_vbucket_state*>(blob);
};

TEST_F(DcpSetVbucketStateValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_F(DcpSetVbucketStateValidatorTest, LegalValues) {
    for (int ii = 1; ii < 5; ++ii) {
        request.message.body.state = ii;
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
    }
}

TEST_F(DcpSetVbucketStateValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpSetVbucketStateValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpSetVbucketStateValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpSetVbucketStateValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpSetVbucketStateValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpSetVbucketStateValidatorTest, IllegalValues) {
    request.message.body.state = 5;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
    request.message.body.state = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

class DcpNoopValidatorTest : public ValidatorTest {
protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_NOOP,
                                       static_cast<void*>(&request));
    }
};

TEST_F(DcpNoopValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_F(DcpNoopValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpNoopValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpNoopValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpNoopValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpNoopValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

class DcpBufferAckValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 4;
        request.message.header.request.bodylen = htonl(4);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(
            PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT,
            static_cast<void*>(&request));
    }
};

TEST_F(DcpBufferAckValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_F(DcpBufferAckValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpBufferAckValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(5);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpBufferAckValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 4;
    request.message.header.request.bodylen = htonl(8);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpBufferAckValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpBufferAckValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

class DcpControlValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(4);
        request.message.header.request.bodylen = htonl(8);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_CONTROL,
                                       static_cast<void*>(&request));
    }
};

TEST_F(DcpControlValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_F(DcpControlValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpControlValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(13);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpControlValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 0;
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpControlValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpControlValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// Test observe seqno
class ObserveSeqnoValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.bodylen = ntohl(8);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_OBSERVE_SEQNO,
                                       static_cast<void*>(&request));
    }
};

TEST_F(ObserveSeqnoValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(ObserveSeqnoValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(ObserveSeqnoValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 8;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(ObserveSeqnoValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(18);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(ObserveSeqnoValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(ObserveSeqnoValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// Test set drift counter state
class SetDriftCounterStateValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 9;
        request.message.header.request.bodylen = ntohl(9);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(
            PROTOCOL_BINARY_CMD_SET_DRIFT_COUNTER_STATE,
            static_cast<void*>(&request));
    }
};

TEST_F(SetDriftCounterStateValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(SetDriftCounterStateValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(SetDriftCounterStateValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(SetDriftCounterStateValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(19);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(SetDriftCounterStateValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(SetDriftCounterStateValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// Test get adjusted time
class GetAdjustedTimeValidatorTest : public ValidatorTest {
protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_GET_ADJUSTED_TIME,
                                       static_cast<void*>(&request));
    }
};

TEST_F(GetAdjustedTimeValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(GetAdjustedTimeValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(GetAdjustedTimeValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(GetAdjustedTimeValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(GetAdjustedTimeValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(GetAdjustedTimeValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(GetAdjustedTimeValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

enum class RefreshOpcodes : uint8_t {
    Isasl = uint8_t(PROTOCOL_BINARY_CMD_ISASL_REFRESH),
    Ssl = uint8_t(PROTOCOL_BINARY_CMD_SSL_CERTS_REFRESH),
    Rbac = uint8_t(PROTOCOL_BINARY_CMD_RBAC_REFRESH)
};

std::string to_string(const RefreshOpcodes& opcode) {
#ifdef JETBRAINS_CLION_IDE
    // CLion don't properly parse the output when the
    // output gets written as the string instead of the
    // number. This makes it harder to debug the tests
    // so let's just disable it while we're waiting
    // for them to supply a fix.
    // See https://youtrack.jetbrains.com/issue/CPP-6039
    return std::to_string(static_cast<int>(opcode));
#else
    switch (opcode) {
    case RefreshOpcodes::Isasl:
        return "ISASL";
    case RefreshOpcodes::Ssl:
        return "SSL";
    case RefreshOpcodes::Rbac:
        return "RBAC";
    }
    throw std::invalid_argument("to_string(const RefreshOpcodes&): unknown opcode");
#endif
}

std::ostream& operator<<(std::ostream& os, const RefreshOpcodes& o) {
    os << to_string(o);
    return os;
}

class RefreshValidatorTest : public ValidatorTest,
                             public ::testing::WithParamInterface<RefreshOpcodes> {
protected:
    protocol_binary_response_status validate() {
        auto opcode = (protocol_binary_command)GetParam();
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

INSTANTIATE_TEST_CASE_P(RefreshOpcodes,
                        RefreshValidatorTest,
                        ::testing::Values(RefreshOpcodes::Isasl,
                                          RefreshOpcodes::Ssl,
                                          RefreshOpcodes::Rbac),
                        ::testing::PrintToStringParamName());


TEST_P(RefreshValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(RefreshValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(RefreshValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(RefreshValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(RefreshValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(RefreshValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(RefreshValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// Test CmdTimer
class CmdTimerValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.extlen = 1;
        request.message.header.request.bodylen = htonl(1);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_GET_CMD_TIMER,
                                       static_cast<void*>(&request));
    }
};

TEST_F(CmdTimerValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(CmdTimerValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(CmdTimerValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(CmdTimerValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(CmdTimerValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(CmdTimerValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(CmdTimerValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// Test GetCtrlToken
class GetCtrlTokenValidatorTest : public ValidatorTest {
protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_GET_CTRL_TOKEN,
                                       static_cast<void*>(&request));
    }
};

TEST_F(GetCtrlTokenValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(GetCtrlTokenValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(GetCtrlTokenValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(GetCtrlTokenValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(GetCtrlTokenValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(GetCtrlTokenValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(GetCtrlTokenValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// Test SetCtrlToken
class SetCtrlTokenValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 8;
        request.message.header.request.bodylen = htonl(8);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
        request.message.body.new_cas = 1;
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_SET_CTRL_TOKEN,
                                       static_cast<void*>(&request));
    }

    protocol_binary_request_set_ctrl_token &request =
        *reinterpret_cast<protocol_binary_request_set_ctrl_token*>(blob);
};

TEST_F(SetCtrlTokenValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(SetCtrlTokenValidatorTest, Cas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(SetCtrlTokenValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(SetCtrlTokenValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(SetCtrlTokenValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(18);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(SetCtrlTokenValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(SetCtrlTokenValidatorTest, InvalidNewCas) {
    request.message.body.new_cas = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(SetCtrlTokenValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// PROTOCOL_BINARY_CMD_GET_ALL_VB_SEQNOS
class GetAllVbSeqnoValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_GET_ALL_VB_SEQNOS,
                                       static_cast<void*>(&request));
    }

    protocol_binary_request_get_all_vb_seqnos &request =
        *reinterpret_cast<protocol_binary_request_get_all_vb_seqnos*>(blob);
};

TEST_F(GetAllVbSeqnoValidatorTest, CorrectMessageNoState) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(GetAllVbSeqnoValidatorTest, CorrectMessageWithState) {
    EXPECT_EQ(4, sizeof(vbucket_state_t));
    request.message.header.request.extlen = 4;
    request.message.header.request.bodylen = htonl(4);
    request.message.body.state =
        static_cast<vbucket_state_t>(htonl(vbucket_state_active));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(GetAllVbSeqnoValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(GetAllVbSeqnoValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(GetAllVbSeqnoValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(GetAllVbSeqnoValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(GetAllVbSeqnoValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(GetAllVbSeqnoValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(GetAllVbSeqnoValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(1);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(GetAllVbSeqnoValidatorTest, InvalidVbucketState) {
    request.message.header.request.extlen = 4;
    request.message.header.request.bodylen = htonl(4);

    for (int ii = 0; ii < 100; ++ii) {
        request.message.body.state = static_cast<vbucket_state_t>(htonl(ii));
        if (is_valid_vbucket_state_t(static_cast<vbucket_state_t>(ii))) {
            EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
        } else {
            EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
        }
    }
}

// PROTOCOL_BINARY_CMD_GET_LOCKED
class GetLockedValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_GET_LOCKED,
                                       static_cast<void*>(&request));
    }
};

TEST_F(GetLockedValidatorTest, CorrectMessageDefaultTimeout) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(GetLockedValidatorTest, CorrectMessageExplicitTimeout) {
    request.message.header.request.extlen = 4;
    request.message.header.request.bodylen = htonl(14);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(GetLockedValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(GetLockedValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(GetLockedValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(11);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(GetLockedValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(GetLockedValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(GetLockedValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(GetLockedValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(1);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// PROTOCOL_BINARY_CMD_UNLOCK
class UnlockValidatorTest : public ValidatorTest {
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
        request.message.header.request.cas = 0xdeadbeef;
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_UNLOCK_KEY,
                                       static_cast<void*>(&request));
    }
};

TEST_F(UnlockValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(UnlockValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(UnlockValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(UnlockValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(11);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(UnlockValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(UnlockValidatorTest, InvalidCas) {
    request.message.header.request.cas = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(UnlockValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(UnlockValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(1);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// Test config_reload
class ConfigReloadValidatorTest : public ValidatorTest {
protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_CONFIG_RELOAD,
                                       static_cast<void*>(&request));
    }
};

TEST_F(ConfigReloadValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(ConfigReloadValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(ConfigReloadValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(ConfigReloadValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(ConfigReloadValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(ConfigReloadValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(ConfigReloadValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// PROTOCOL_BINARY_CMD_EVICT_KEY
class EvictKeyValidatorTest : public ValidatorTest {
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
        request.message.header.request.cas = 0;
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_EVICT_KEY,
                                       static_cast<void*>(&request));
    }
};

TEST_F(EvictKeyValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(EvictKeyValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(EvictKeyValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(12);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(EvictKeyValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(11);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(EvictKeyValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(EvictKeyValidatorTest, InvalidCas) {
    request.message.header.request.cas = 0xff;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(EvictKeyValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(EvictKeyValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(1);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

} // namespace test
} // namespace mcbp
