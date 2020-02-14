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

#include <daemon/connection.h>
#include <event2/event.h>
#include <memcached/protocol_binary.h>

namespace mcbp {
namespace test {

class MutationWithMetaTest : public ::testing::WithParamInterface<
                                     std::tuple<cb::mcbp::ClientOpcode, bool>>,
                             public ValidatorTest {
public:
    MutationWithMetaTest() : ValidatorTest(std::get<1>(GetParam())) {
    }
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.setExtlen(24);
        request.message.header.request.setKeylen(10);
        request.message.header.request.setBodylen(512);
    }

protected:
    cb::mcbp::Status validate() {
        auto opcode = cb::mcbp::ClientOpcode(std::get<0>(GetParam()));
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_P(MutationWithMetaTest, CorrectMessage) {
    // There are 4 legal extralength (we can just change the length as the
    // body will just get smaller (but we don't test that anyway)
    EXPECT_EQ(cb::mcbp::Status::Success, validate());

    request.message.header.request.setExtlen(26);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());

    request.message.header.request.setExtlen(28);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());

    request.message.header.request.setExtlen(30);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(MutationWithMetaTest, InvalidMagic) {
    blob[0] = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(MutationWithMetaTest, InvalidExtlen) {
    for (int ii = 0; ii < 256; ++ii) {
        request.message.header.request.setExtlen(ii);

        switch (ii) {
        case 24:
        case 26:
        case 28:
        case 30:
            EXPECT_EQ(cb::mcbp::Status::Success, validate());
            break;
        default:
            EXPECT_EQ(cb::mcbp::Status::Einval, validate()) << "Extlen: " << ii;
        }
    }
}

TEST_P(MutationWithMetaTest, NoKey) {
    request.message.header.request.setKeylen(0);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(MutationWithMetaTest, InvalidDatatype) {
    request.message.header.request.setDatatype(cb::mcbp::Datatype(0xff));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

INSTANTIATE_TEST_CASE_P(
        Opcodes,
        MutationWithMetaTest,
        ::testing::Combine(
                ::testing::Values(cb::mcbp::ClientOpcode::SetWithMeta,
                                  cb::mcbp::ClientOpcode::SetqWithMeta,
                                  cb::mcbp::ClientOpcode::AddWithMeta,
                                  cb::mcbp::ClientOpcode::AddqWithMeta,
                                  cb::mcbp::ClientOpcode::DelWithMeta,
                                  cb::mcbp::ClientOpcode::DelqWithMeta),
                ::testing::Bool()));
} // namespace test
} // namespace mcbp
