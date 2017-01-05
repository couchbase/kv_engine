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

#include <daemon/connection_mcbp.h>
#include <event2/event.h>
#include <memcached/protocol_binary.h>

/**
 * Test all of the command validators we've got to ensure that they
 * catch broken packets. There is still a high number of commands we
 * don't have any command validators for...
 */
namespace BinaryProtocolValidator {
void ValidatorTest::SetUp() {
    McbpValidatorChains::initializeMcbpValidatorChains(validatorChains);
}

int ValidatorTest::validate(protocol_binary_command opcode, void* packet) {
    // Mockup a McbpConnection and Cookie for the validator chain
    event_base* ev = event_base_new();
    McbpConnection connection(-1, ev);
    auto* req = reinterpret_cast<protocol_binary_request_header*>(packet);
    connection.binary_header = *req;
    connection.binary_header.request.keylen = ntohs(req->request.keylen);
    connection.binary_header.request.bodylen = ntohl(req->request.bodylen);
    connection.binary_header.request.vbucket = ntohs(req->request.vbucket);
    connection.binary_header.request.cas = ntohll(req->request.cas);

    // Mockup read.curr so that validators can find the packet
    connection.read.curr = static_cast<char*>(packet) +
                           (connection.binary_header.request.bodylen +
                            sizeof(connection.binary_header));
    Cookie cookie(&connection);
    int rv = validatorChains.invoke(opcode, cookie);
    event_base_free(ev);
    return rv;
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
    throw std::invalid_argument("to_string: unknown opcode");
#endif
}

std::ostream& operator<<(std::ostream& os, const GetOpcodes& o) {
    os << to_string(o);
    return os;
};

// Test the validators for GET, GETQ, GETK, GETKQ
class GetValidatorTest : public ValidatorTest,
                         public ::testing::WithParamInterface<GetOpcodes> {
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 0;
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        auto opcode = (protocol_binary_command)GetParam();
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
    protocol_binary_request_get request;
};

TEST_P(GetValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(GetValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(GetValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}
TEST_P(GetValidatorTest, NoKey) {
    // key != bodylen
    request.message.header.request.keylen = 0;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 8;
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(20);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
    protocol_binary_request_add request;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 8;
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(20);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
    protocol_binary_request_set request;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 0;
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(20);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
    protocol_binary_request_append request;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 0;
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
    protocol_binary_request_delete request;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 20;
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(30);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
    protocol_binary_request_incr request;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
    protocol_binary_request_quit request;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
    protocol_binary_request_flush request;
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
    request.message.body.expiration = 1;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_NOOP,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_noop request;
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
    request.message.header.request.bodylen = 100;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_VERSION,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_version request;
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
    request.message.header.request.bodylen = 100;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_STAT,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_stats request;
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
    request.message.header.request.bodylen = 100;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 4;
        request.message.header.request.bodylen = htonl(4);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_VERBOSITY,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_verbosity request;
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
    request.message.header.request.bodylen = 100;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_HELLO,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_hello request;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_SASL_LIST_MECHS,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_no_extras request;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate(protocol_binary_command opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
    protocol_binary_request_no_extras request;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_GET_ERROR_MAP,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_get_errmap request;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    // #defined in memcached.h..
    const int IOCTL_KEY_LENGTH = 128;
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_IOCTL_GET,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_ioctl_get request;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(10);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    // #defined in memcached.h..
    const int IOCTL_KEY_LENGTH = 128;
    const int IOCTL_VAL_LENGTH = 128;
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_IOCTL_SET,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_ioctl_set request;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 4;
        request.message.header.request.bodylen = htonl(10);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_AUDIT_PUT,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_audit_put request;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_AUDIT_CONFIG_RELOAD,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_no_extras request;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
        request.message.header.request.cas = 1;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_SHUTDOWN,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_no_extras request;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 8;
        request.message.header.request.keylen = htons(2);
        request.message.header.request.bodylen = htonl(10);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_OPEN,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_dcp_open request;
};

TEST_F(DcpOpenValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
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

class DcpAddStreamValidatorTest : public ValidatorTest {
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 4;
        request.message.header.request.bodylen = htonl(4);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_ADD_STREAM,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_dcp_add_stream request;
};

TEST_F(DcpAddStreamValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_CLOSE_STREAM,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_dcp_close_stream request;
};

TEST_F(DcpCloseStreamValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_dcp_close_stream request;
};

TEST_F(DcpGetFailoverLogValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 48;
        request.message.header.request.bodylen = htonl(48);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_STREAM_REQ,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_dcp_close_stream request;
};

TEST_F(DcpStreamReqValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 4;
        request.message.header.request.bodylen = htonl(4);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_STREAM_END,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_dcp_close_stream request;
};

TEST_F(DcpStreamEndValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 20;
        request.message.header.request.bodylen = htonl(20);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_dcp_snapshot_marker request;
};

TEST_F(DcpSnapshotMarkerValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
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

class DcpMutationValidatorTest : public ValidatorTest {
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 31;
        request.message.header.request.keylen = htons(1);
        request.message.header.request.bodylen = htonl(32);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_MUTATION,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_dcp_mutation request;
};

TEST_F(DcpMutationValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}
TEST_F(DcpMutationValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}
TEST_F(DcpMutationValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 21;
    request.message.header.request.bodylen = htonl(22);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}
TEST_F(DcpMutationValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 0;
    request.message.header.request.bodylen = htonl(31);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

class DcpDeletionValidatorTest : public ValidatorTest {
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 18;
        request.message.header.request.keylen = htons(2);
        request.message.header.request.bodylen = htonl(20);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_DELETION,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_dcp_deletion request;
};

TEST_F(DcpDeletionValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(DcpDeletionValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpDeletionValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(7);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}
TEST_F(DcpDeletionValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 0;
    request.message.header.request.bodylen = htonl(18);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}
// @todo can a deletion carry value?
// TEST_F(DcpDeletionValidatorTest, InvalidBodylen) {
//     request.message.header.request.bodylen = htonl(100);
//     EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
// }

class DcpExpirationValidatorTest : public ValidatorTest {
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 18;
        request.message.header.request.keylen = htons(2);
        request.message.header.request.bodylen = htonl(20);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_EXPIRATION,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_dcp_expiration request;
};

TEST_F(DcpExpirationValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(DcpExpirationValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(DcpExpirationValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 5;
    request.message.header.request.bodylen = htonl(7);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}
TEST_F(DcpExpirationValidatorTest, InvalidKeylen) {
    request.message.header.request.keylen = 0;
    request.message.header.request.bodylen = htonl(18);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}
TEST_F(DcpExpirationValidatorTest, InvalidBodylen) {
    request.message.header.request.bodylen = htonl(100);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

class DcpFlushValidatorTest : public ValidatorTest {
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_FLUSH,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_dcp_close_stream request;
};

TEST_F(DcpFlushValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 1;
        request.message.header.request.bodylen = htonl(1);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
        request.message.body.state = 1;
    }

protected:
    int validate() {
        return ValidatorTest::validate(
                PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE,
                static_cast<void*>(&request));
    }
    protocol_binary_request_dcp_set_vbucket_state request;
};

TEST_F(DcpSetVbucketStateValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}
TEST_F(DcpSetVbucketStateValidatorTest, LegalValues) {
    for (int ii = 1; ii < 5; ++ii) {
        request.message.body.state = ii;
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_NOOP,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_dcp_close_stream request;
};

TEST_F(DcpNoopValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 4;
        request.message.header.request.bodylen = htonl(4);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(
                PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT,
                static_cast<void*>(&request));
    }
    protocol_binary_request_dcp_buffer_acknowledgement request;
};

TEST_F(DcpBufferAckValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 0;
        request.message.header.request.keylen = htons(4);
        request.message.header.request.bodylen = htonl(8);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_DCP_CONTROL,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_dcp_control request;
};

TEST_F(DcpControlValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.bodylen = ntohl(8);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_OBSERVE_SEQNO,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_set_drift_counter_state request;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 9;
        request.message.header.request.bodylen = ntohl(9);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(
                PROTOCOL_BINARY_CMD_SET_DRIFT_COUNTER_STATE,
                static_cast<void*>(&request));
    }
    protocol_binary_request_set_drift_counter_state request;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_GET_ADJUSTED_TIME,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_get_adjusted_time request;
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

// Test IsaslRefresh
class IsaslRefreshValidatorTest : public ValidatorTest {
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_ISASL_REFRESH,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_no_extras request;
};

TEST_F(IsaslRefreshValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}
TEST_F(IsaslRefreshValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}
TEST_F(IsaslRefreshValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}
TEST_F(IsaslRefreshValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}
TEST_F(IsaslRefreshValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}
TEST_F(IsaslRefreshValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}
TEST_F(IsaslRefreshValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// Test SslCertsRefresh
class SslCertsRefreshValidatorTest : public ValidatorTest {
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_SSL_CERTS_REFRESH,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_no_extras request;
};

TEST_F(SslCertsRefreshValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}
TEST_F(SslCertsRefreshValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}
TEST_F(SslCertsRefreshValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}
TEST_F(SslCertsRefreshValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}
TEST_F(SslCertsRefreshValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}
TEST_F(SslCertsRefreshValidatorTest, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}
TEST_F(SslCertsRefreshValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// Test CmdTimer
class CmdTimerValidatorTest : public ValidatorTest {
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 1;
        request.message.header.request.bodylen = htonl(1);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_GET_CMD_TIMER,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_no_extras request;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_GET_CTRL_TOKEN,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_no_extras request;
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
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 8;
        request.message.header.request.bodylen = htonl(8);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
        request.message.body.new_cas = 1;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_SET_CTRL_TOKEN,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_set_ctrl_token request;
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

// Test init complete
class InitCompleteValidatorTest : public ValidatorTest {
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_INIT_COMPLETE,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_no_extras request;
};

TEST_F(InitCompleteValidatorTest, CorrectMessage) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}
TEST_F(InitCompleteValidatorTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}
TEST_F(InitCompleteValidatorTest, InvalidExtlen) {
    request.message.header.request.extlen = 2;
    request.message.header.request.bodylen = htonl(2);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}
TEST_F(InitCompleteValidatorTest, InvalidKey) {
    request.message.header.request.keylen = 10;
    request.message.header.request.bodylen = htonl(10);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}
TEST_F(InitCompleteValidatorTest, InvalidDatatype) {
    request.message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}
TEST_F(InitCompleteValidatorTest, InvalidBody) {
    request.message.header.request.bodylen = htonl(4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

// PROTOCOL_BINARY_CMD_GET_ALL_VB_SEQNOS
class GetAllVbSeqnoValidatorTest : public ValidatorTest {
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_GET_ALL_VB_SEQNOS,
                                       static_cast<void*>(&request));
    }
    protocol_binary_request_get_all_vb_seqnos request;
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
}
