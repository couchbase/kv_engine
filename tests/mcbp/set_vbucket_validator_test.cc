/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "mcbp_test.h"

#include <daemon/cookie.h>
#include <gsl/gsl-lite.hpp>
#include <mcbp/protocol/datatype.h>
#include <mcbp/protocol/framebuilder.h>
#include <memory>

namespace mcbp::test {

using namespace cb::mcbp;

/**
 * 1 byte extras containing the VBUCKET state.
 *
 * There might be a body, which has to have the DATATYPE bit set to
 * JSON. Introduced in MadHatter. (The executor needs to validate
 * the case that it is valid JSON as otherwise we'd have to parse
 * it just to throw it away)
 */
class SetVBucketValidatorTest : public ::testing::WithParamInterface<bool>,
                                public ValidatorTest {
public:
    SetVBucketValidatorTest() : ValidatorTest(GetParam()) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(ClientOpcode::SetVbucket, blob);
    }
};

TEST_P(SetVBucketValidatorTest, SetVbucketMessage) {
    RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(Magic::ClientRequest);
    builder.setOpcode(ClientOpcode::SetVbucket);
    auto state = uint8_t(vbucket_state_active);
    builder.setExtras({&state, sizeof(state)});
    EXPECT_EQ(Status::Success, validate());

    // Validate that we detect invalid vbucket state:
    bool found_invalid = false;
    for (auto& val : std::vector<uint32_t>{{vbucket_state_active,
                                            vbucket_state_replica,
                                            vbucket_state_pending,
                                            vbucket_state_dead,
                                            0,
                                            100}}) {
        state = uint8_t(val);
        builder.setExtras({&state, sizeof(state)});
        if (is_valid_vbucket_state_t(val)) {
            EXPECT_EQ(Status::Success, validate());
        } else {
            found_invalid = true;
            EXPECT_EQ(Status::Einval, validate()) << val;
        }
    }
    EXPECT_TRUE(found_invalid) << "No invalid vbucket states was tested";
    state = uint8_t(vbucket_state_active);
    builder.setExtras({&state, sizeof(state)});

    // CAS may be anything (0 == override, otherwise it is the session token
    builder.setCas(uint64_t(time(nullptr)));
    EXPECT_EQ(Status::Success, validate());

    // We might have a value, but ONLY if datatype is set to JSON
    builder.setValue(R"({"foo":"bar"})");
    EXPECT_EQ(Status::Einval, validate());
    builder.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(Status::Success, validate());

    // It should fail with Datatype being something else
    builder.setDatatype(cb::mcbp::Datatype(uint8_t(cb::mcbp::Datatype::Snappy) |
                                           uint8_t(cb::mcbp::Datatype::JSON)));
    EXPECT_EQ(Status::Einval, validate());
    builder.setValue("");
    builder.setDatatype(cb::mcbp::Datatype::JSON);

    // It should fail with invalid extras size
    builder.setExtras({blob, 2});
    EXPECT_EQ(Status::Einval, validate());

    // It should fail if key is set
    builder.setKey("foo");
    EXPECT_EQ(Status::Einval, validate());
    builder.setKey("");

    // VBucket may be anything as that's the vbucket the command operates
    // on.
}

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         SetVBucketValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

} // namespace mcbp::test
