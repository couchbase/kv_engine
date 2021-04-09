/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "mcbp_test.h"

#include <daemon/connection.h>
#include <event2/event.h>
#include <memcached/protocol_binary.h>

namespace mcbp::test {

class MutationWithMetaTest : public ::testing::WithParamInterface<
                                     std::tuple<cb::mcbp::ClientOpcode, bool>>,
                             public ValidatorTest {
public:
    MutationWithMetaTest() : ValidatorTest(std::get<1>(GetParam())) {
    }
    void SetUp() override {
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

INSTANTIATE_TEST_SUITE_P(
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
} // namespace mcbp::test
