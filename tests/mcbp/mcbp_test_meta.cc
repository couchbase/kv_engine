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

#include "mcbp_test.h"

#include <daemon/connection_mcbp.h>
#include <event2/event.h>
#include <memcached/protocol_binary.h>
#include <utilities/protocol2text.h>

namespace BinaryProtocolValidator {

enum class Opcodes : uint8_t {
    SetWithMeta = PROTOCOL_BINARY_CMD_SET_WITH_META,
    SetQWithMeta = PROTOCOL_BINARY_CMD_SETQ_WITH_META,
    AddWithMeta = PROTOCOL_BINARY_CMD_ADD_WITH_META,
    AddQWithMeta = PROTOCOL_BINARY_CMD_ADDQ_WITH_META,
    DelWithMeta = PROTOCOL_BINARY_CMD_DEL_WITH_META,
    DelQWithMeta = PROTOCOL_BINARY_CMD_DELQ_WITH_META
};

std::string to_string(const Opcodes& opcode) {
#ifdef JETBRAINS_CLION_IDE
    // CLion don't properly parse the output when the
    // output gets written as the string instead of the
    // number. This makes it harder to debug the tests
    // so let's just disable it while we're waiting
    // for them to supply a fix.
    // See https://youtrack.jetbrains.com/issue/CPP-6039
    return std::to_string(static_cast<int>(opcode));
#else
    return memcached_opcode_2_text(uint8_t(opcode));
#endif
}

std::ostream& operator<<(std::ostream& os, const Opcodes& o) {
    os << to_string(o);
    return os;
};

class MutationWithMetaTest : public ValidatorTest,
                             public ::testing::WithParamInterface<Opcodes> {
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 24;
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(512);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

protected:
    int validate() {
        auto opcode = (protocol_binary_command)GetParam();
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }

    protocol_binary_request_set_with_meta request;
};

TEST_P(MutationWithMetaTest, CorrectMessage) {
    // There are 4 legal extralength (we can just change the length as the
    // body will just get smaller (but we don't test that anyway)
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());

    request.message.header.request.extlen = 26;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());

    request.message.header.request.extlen = 28;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());

    request.message.header.request.extlen = 30;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_P(MutationWithMetaTest, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(MutationWithMetaTest, InvalidExtlen) {
    for (int ii = 0; ii < 256; ++ii) {
        request.message.header.request.extlen = uint8_t(ii);

        switch (ii) {
        case 24:
        case 26:
        case 28:
        case 30:
            EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
            break;
        default:
            EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate()) << "Extlen: "
                                                                   << ii;
        }
    }
}

TEST_P(MutationWithMetaTest, NoKey) {
    request.message.header.request.keylen = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_P(MutationWithMetaTest, InvalidDatatype) {
    request.message.header.request.datatype = 0xff;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

INSTANTIATE_TEST_CASE_P(Opcodes,
                        MutationWithMetaTest,
                        ::testing::Values(Opcodes::SetWithMeta,
                                          Opcodes::SetQWithMeta,
                                          Opcodes::AddWithMeta,
                                          Opcodes::AddQWithMeta,
                                          Opcodes::DelWithMeta,
                                          Opcodes::DelQWithMeta),
                        ::testing::PrintToStringParamName());
}
