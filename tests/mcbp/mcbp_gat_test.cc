/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
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

// Test the validators for GAT, GATQ, GATK, GATKQ, GAT_META and GATQ_META
class GATValidatorTest
    : public ::testing::WithParamInterface<
              std::tuple<cb::mcbp::ClientOpcode, bool /*collections on/off*/>>,
      public ValidatorTest {
public:
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.setExtlen(4);
        request.message.header.request.setKeylen(10);
        request.message.header.request.setBodylen(14);
    }

    GATValidatorTest() : ValidatorTest(std::get<1>(GetParam())) {
        // empty
    }

protected:
    cb::mcbp::Status validate() {
        auto opcode = cb::mcbp::ClientOpcode(std::get<0>(GetParam()));
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
};

TEST_P(GATValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GATValidatorTest, InvalidExtlen) {
    request.message.header.request.setExtlen(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GATValidatorTest, NoKey) {
    request.message.header.request.setKeylen(0);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GATValidatorTest, InvalidDatatype) {
    request.message.header.request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GATValidatorTest, InvalidCas) {
    request.message.header.request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

INSTANTIATE_TEST_SUITE_P(
        GATOpcodes,
        GATValidatorTest,
        ::testing::Combine(::testing::Values(cb::mcbp::ClientOpcode::Gat,
                                             cb::mcbp::ClientOpcode::Gatq,
                                             cb::mcbp::ClientOpcode::Touch),
                           ::testing::Bool()));
} // namespace mcbp::test
