/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <memcached/protocol_binary.h>
#include "mcbp_test.h"

/**
 * Test commands which are handled by the collections interface
 */
namespace mcbp {
namespace test {

class SetCollectionsValidator : public ValidatorTest {
public:
    SetCollectionsValidator() : ValidatorTest(true) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.opcode =
            PROTOCOL_BINARY_CMD_COLLECTIONS_SET_MANIFEST;
        request.message.header.request.bodylen = htonl(10);
    }

protected:
    protocol_binary_response_status validate() {
        return ValidatorTest::validate(
            PROTOCOL_BINARY_CMD_COLLECTIONS_SET_MANIFEST,
            static_cast<void*>(&request));
    }
};

TEST_F(SetCollectionsValidator, CorrectMessage) {
    // We expect success because the validator will check all the packet members
    // then find the collections interface doesn't define a handler
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, validate());
}

TEST_F(SetCollectionsValidator, InvalidMagic) {
    request.message.header.request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(SetCollectionsValidator, InvalidKeylen) {
    request.message.header.request.keylen = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(SetCollectionsValidator, InvalidExtlen) {
    request.message.header.request.extlen = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(SetCollectionsValidator, InvalidCas) {
    request.message.header.request.cas = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(SetCollectionsValidator, InvalidDatatype) {
    request.message.header.request.datatype = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(SetCollectionsValidator, InvalidVbucket) {
    request.message.header.request.vbucket = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(SetCollectionsValidator, InvalidBodylen) {
    request.message.header.request.bodylen = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

} // namespace test
} // namespace mcbp
