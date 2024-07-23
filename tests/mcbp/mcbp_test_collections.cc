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
        request.setOpcode(cb::mcbp::ClientOpcode::CollectionsSetManifest);
        request.setBodylen(10);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::CollectionsSetManifest, &request);
    }
};

TEST_F(SetCollectionsValidator, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_F(SetCollectionsValidator, InvalidKeylen) {
    request.setKeylen(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_F(SetCollectionsValidator, InvalidExtlen) {
    request.setExtlen(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_F(SetCollectionsValidator, InvalidCas) {
    request.setCas(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_F(SetCollectionsValidator, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype(1));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_F(SetCollectionsValidator, InvalidVbucket) {
    request.setVBucket(Vbid(1));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_F(SetCollectionsValidator, InvalidBodylen) {
    request.setBodylen(0);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}
class GetCollectionIdValidator : public ValidatorTest {
public:
    GetCollectionIdValidator() : ValidatorTest(true) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.setOpcode(cb::mcbp::ClientOpcode::CollectionsGetID);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::CollectionsGetID,
                                       &request);
    }
};

TEST_F(GetCollectionIdValidator, CorrectMessage1) {
    request.setBodylen(10);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

// @todo MB-44807: this encoding will become invalid
TEST_F(GetCollectionIdValidator, CorrectMessage2) {
    request.setKeylen(10);
    request.setBodylen(10);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_F(SetCollectionsValidator, InvalidKeyAndBody) {
    request.setKeylen(1);
    request.setBodylen(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

} // namespace mcbp::test
