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
namespace mcbp::test {

class SetCollectionsValidator : public ValidatorTest {
public:
    SetCollectionsValidator() : ValidatorTest(true) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.setOpcode(
                cb::mcbp::ClientOpcode::CollectionsSetManifest);
        request.message.header.request.setBodylen(10);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::CollectionsSetManifest,
                static_cast<void*>(&request));
    }
};

TEST_F(SetCollectionsValidator, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_F(SetCollectionsValidator, InvalidKeylen) {
    request.message.header.request.setKeylen(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_F(SetCollectionsValidator, InvalidExtlen) {
    request.message.header.request.setExtlen(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_F(SetCollectionsValidator, InvalidCas) {
    request.message.header.request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_F(SetCollectionsValidator, InvalidDatatype) {
    request.message.header.request.setDatatype(cb::mcbp::Datatype(1));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_F(SetCollectionsValidator, InvalidVbucket) {
    request.message.header.request.setVBucket(Vbid(1));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_F(SetCollectionsValidator, InvalidBodylen) {
    request.message.header.request.setBodylen(0);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}
class GetCollectionIdValidator : public ValidatorTest {
public:
    GetCollectionIdValidator() : ValidatorTest(true) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.setOpcode(
                cb::mcbp::ClientOpcode::CollectionsGetID);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::CollectionsGetID,
                                       static_cast<void*>(&request));
    }
};

TEST_F(GetCollectionIdValidator, CorrectMessage1) {
    request.message.header.request.setBodylen(10);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

// @todo MB-44807: this encoding will become invalid
TEST_F(GetCollectionIdValidator, CorrectMessage2) {
    request.message.header.request.setKeylen(10);
    request.message.header.request.setBodylen(10);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_F(SetCollectionsValidator, InvalidKeyAndBody) {
    request.message.header.request.setKeylen(1);
    request.message.header.request.setBodylen(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

} // namespace mcbp::test
