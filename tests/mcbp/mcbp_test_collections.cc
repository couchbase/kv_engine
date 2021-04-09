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
