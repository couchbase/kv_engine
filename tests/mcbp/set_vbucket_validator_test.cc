/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include <daemon/cookie.h>
#include <mcbp/protocol/datatype.h>
#include <mcbp/protocol/framebuilder.h>
#include <gsl/gsl>
#include <memory>

namespace mcbp::test {

using namespace cb::mcbp;

/**
 * The SetVbucket may contain 3 different encodings:
 *
 * 1) extras contain the vbucket state in 4 bytes (pre MadHatter). Dataype
 *    must be RAW
 * 2) body may contain the vbucket state in 4 bytes (ns_server in pre
 *    MadHatter). Datatype must be RAW
 * 3) 1 byte extras containing the VBUCKET state.
 *    There might be a body, which has to have the DATATYPE bit set to
 *    JSON. Introduced in MadHatter. (The executor needs to validate
 *    the case that it is valid JSON as otherwise we'd have to parse
 *    it just to throw it away)
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
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::SetVbucket,
                                       static_cast<void*>(blob));
    }

    /**
     * Pre MadHatter we should have the vbucket state in 4 bytes in the extras
     * field of the packet, but ns_server encoded this differently (and we never
     * discovered that before it was shipped) so if the message comes from
     * ns_server it use the value field instead. The Datatype should however
     * be set to RAW no matter what. There should be no key, and cas may be
     * of any value.

     * @param useExtras Should we use the extras field of the value field
     *                  for the vbucket state
     */
    void testPreMadHatter(bool useExtras) {
        RequestBuilder builder({blob, sizeof(blob)});
        uint32_t value = ntohl(vbucket_state_active);
        builder.setMagic(Magic::ClientRequest);
        builder.setOpcode(ClientOpcode::SetVbucket);
        builder.setDatatype(cb::mcbp::Datatype::Raw);
        if (useExtras) {
            builder.setExtras(
                    {reinterpret_cast<const uint8_t*>(&value), sizeof(value)});
        } else {
            builder.setValue(
                    {reinterpret_cast<const uint8_t*>(&value), sizeof(value)});
        }
        EXPECT_EQ(Status::Success, validate());

        // CAS may be anything (0 == override, otherwise it is the session token
        builder.setCas(uint64_t(time(nullptr)));
        EXPECT_EQ(Status::Success, validate());

        // It should fail if key is set
        builder.setKey("foo");
        EXPECT_EQ(Status::Einval, validate());
        builder.setKey("");

        // Validate that we detect invalid vbucket state:
        bool found_invalid = false;
        for (auto& val : std::vector<uint32_t>{{vbucket_state_active,
                                                vbucket_state_replica,
                                                vbucket_state_pending,
                                                vbucket_state_dead,
                                                0,
                                                100}}) {
            value = ntohl(val);
            if (useExtras) {
                builder.setExtras({reinterpret_cast<const uint8_t*>(&value),
                                   sizeof(value)});
            } else {
                builder.setValue({reinterpret_cast<const uint8_t*>(&value),
                                  sizeof(value)});
            }
            if (is_valid_vbucket_state_t(val)) {
                EXPECT_EQ(Status::Success, validate());
            } else {
                found_invalid = true;
                EXPECT_EQ(Status::Einval, validate()) << val;
            }
        }
        EXPECT_TRUE(found_invalid) << "No invalid vbucket states was tested";

        // VBucket may be anything as that's the vbucket the command operates
        // on.
    }
};

TEST_P(SetVBucketValidatorTest, PreMadHatterMessageExtras) {
    testPreMadHatter(true);
}

TEST_P(SetVBucketValidatorTest, PreMadHatterMessageValue) {
    testPreMadHatter(false);
}

TEST_P(SetVBucketValidatorTest, MadHatterMessage) {
    RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(Magic::ClientRequest);
    builder.setOpcode(ClientOpcode::SetVbucket);
    auto state = uint8_t(vbucket_state_active);
    builder.setExtras({&state, sizeof(state)});
    ;
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
        ;
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
    ;

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
