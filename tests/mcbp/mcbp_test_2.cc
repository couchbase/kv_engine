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
#include <mcbp/protocol/datatype.h>
#include <mcbp/protocol/framebuilder.h>
#include <mcbp/protocol/header.h>
#include <memcached/protocol_binary.h>
#include <memcached/unit_test_mode.h>
#include <memory>

/**
 * Test all of the command validators we've got to ensure that they
 * catch broken packets. There is still a high number of commands we
 * don't have any command validators for...
 */
namespace mcbp::test {

class DropPrivilegeValidatorTest : public ::testing::WithParamInterface<bool>,
                                   public ValidatorTest {
public:
    DropPrivilegeValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        req.setKeylen(10);
        req.setBodylen(10);
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DropPrivilege,
                                       &request);
    }
};

TEST_P(DropPrivilegeValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(DropPrivilegeValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    req.setBodylen(req.getBodylen() + 2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DropPrivilegeValidatorTest, InvalidDatatype) {
    req.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DropPrivilegeValidatorTest, IvalidCas) {
    req.setCas(0xff);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DropPrivilegeValidatorTest, InvalidKey) {
    req.setKeylen(0);
    req.setBodylen(0);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DropPrivilegeValidatorTest, InvalidBodylen) {
    req.setBodylen(req.getKeylen() + 10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class GetClusterConfigValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    GetClusterConfigValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::GetClusterConfig,
                                       &request);
    }
};

TEST_P(GetClusterConfigValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GetClusterConfigValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    req.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetClusterConfigValidatorTest, InvalidDatatype) {
    req.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetClusterConfigValidatorTest, IvalidCas) {
    req.setCas(0xff);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetClusterConfigValidatorTest, InvalidKey) {
    req.setKeylen(2);
    req.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetClusterConfigValidatorTest, InvalidBodylen) {
    req.setBodylen(8);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class SetClusterConfigValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    SetClusterConfigValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
        cb::mcbp::request::SetClusterConfigPayload extras;
        extras.setEpoch(1);
        extras.setRevision(1);
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::SetClusterConfig);
        builder.setExtras(extras.getBuffer());
        builder.setValue(R"({"rev":0})");
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::SetClusterConfig,
                                       &request);
    }
};

TEST_P(SetClusterConfigValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(SetClusterConfigValidatorTest, WithRevision) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    cb::mcbp::request::SetClusterConfigPayload extras;
    extras.setEpoch(1);
    extras.setRevision(1);
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::SetClusterConfig);
    builder.setExtras(extras.getBuffer());
    builder.setValue(R"({"rev":0})");
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(SetClusterConfigValidatorTest, InvalidRevisionNumber) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    cb::mcbp::request::SetClusterConfigPayload extras;
    extras.setEpoch(-1);
    extras.setRevision(-1);
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::SetClusterConfig);
    builder.setExtras(extras.getBuffer());
    builder.setValue(R"({"rev":-1})");
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetClusterConfigValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetClusterConfigValidatorTest, DocMayBeJSON) {
    req.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(SetClusterConfigValidatorTest, InvalidDatatype) {
    req.setDatatype(cb::mcbp::Datatype::Snappy);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetClusterConfigValidatorTest, InvalidCas) {
    req.setCas(0xff);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetClusterConfigValidatorTest, InvalidBodylen) {
    req.setBodylen(req.getExtlen());
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class StartStopPersistenceValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    StartStopPersistenceValidatorTest()
        : ValidatorTest(GetParam()), req(request) {
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate(bool start) {
        if (start) {
            return ValidatorTest::validate(
                    cb::mcbp::ClientOpcode::StartPersistence, &request);
        }
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::StopPersistence,
                                       &request);
    }
};

TEST_P(StartStopPersistenceValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate(true));
    EXPECT_EQ(cb::mcbp::Status::Success, validate(false));
}

TEST_P(StartStopPersistenceValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    req.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(true));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(false));
}

TEST_P(StartStopPersistenceValidatorTest, InvalidDatatype) {
    req.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(true));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(false));
}

TEST_P(StartStopPersistenceValidatorTest, IvalidCas) {
    req.setCas(0xff);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(true));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(false));
}

TEST_P(StartStopPersistenceValidatorTest, InvalidKey) {
    req.setKeylen(2);
    req.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(true));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(false));
}

TEST_P(StartStopPersistenceValidatorTest, InvalidBodylen) {
    req.setBodylen(8);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(true));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(false));
}

class EnableDisableTrafficValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    EnableDisableTrafficValidatorTest()
        : ValidatorTest(GetParam()), req(request) {
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate(bool start) {
        if (start) {
            return ValidatorTest::validate(
                    cb::mcbp::ClientOpcode::EnableTraffic, &request);
        }
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DisableTraffic,
                                       &request);
    }
};

TEST_P(EnableDisableTrafficValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate(true));
    EXPECT_EQ(cb::mcbp::Status::Success, validate(false));
}

TEST_P(EnableDisableTrafficValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    req.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(true));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(false));
}

TEST_P(EnableDisableTrafficValidatorTest, InvalidDatatype) {
    req.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(true));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(false));
}

TEST_P(EnableDisableTrafficValidatorTest, IvalidCas) {
    req.setCas(0xff);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(true));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(false));
}

TEST_P(EnableDisableTrafficValidatorTest, InvalidKey) {
    req.setKeylen(2);
    req.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(true));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(false));
}

TEST_P(EnableDisableTrafficValidatorTest, InvalidBodylen) {
    req.setBodylen(8);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(true));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(false));
}

class GetKeysValidatorTest : public ::testing::WithParamInterface<bool>,
                             public ValidatorTest {
public:
    GetKeysValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        req.setKeylen(2);
        req.setBodylen(2);
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::GetKeys,
                                       &request);
    }
};

TEST_P(GetKeysValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GetKeysValidatorTest, Extlen) {
    req.setExtlen(2);
    req.setBodylen(6);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());

    // But it may contain an optional uint32_t containing the count
    req.setExtlen(4);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GetKeysValidatorTest, InvalidDatatype) {
    req.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetKeysValidatorTest, IvalidCas) {
    req.setCas(0xff);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetKeysValidatorTest, InvalidKey) {
    // The key must be present
    req.setKeylen(0);
    req.setBodylen(0);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetKeysValidatorTest, InvalidBodylen) {
    req.setBodylen(8);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class SetParamValidatorTest : public ::testing::WithParamInterface<bool>,
                              public ValidatorTest {
public:
    SetParamValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        req.setExtlen(sizeof(cb::mcbp::request::SetParamPayload));
        req.setKeylen(2);
        req.setBodylen(req.getExtlen() + req.getKeylen() + 2);
        auto* payload = reinterpret_cast<cb::mcbp::request::SetParamPayload*>(
                blob + sizeof(request));
        payload->setParamType(cb::mcbp::request::SetParamPayload::Type::Flush);
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::SetParam,
                                       &request);
    }
};

TEST_P(SetParamValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(SetParamValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetParamValidatorTest, InvalidDatatype) {
    req.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetParamValidatorTest, InvalidCas) {
    req.setCas(0xff);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetParamValidatorTest, InvalidKey) {
    // The key must be present
    req.setKeylen(0);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetParamValidatorTest, InvalidBodylen) {
    // The value must be present
    req.setBodylen(req.getExtlen() + req.getKeylen());
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class GetReplicaValidatorTest : public ::testing::WithParamInterface<bool>,
                                public ValidatorTest {
public:
    GetReplicaValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        req.setKeylen(2);
        req.setBodylen(2);
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::GetReplica,
                                       &request);
    }
};

TEST_P(GetReplicaValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GetReplicaValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    req.setBodylen(6);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetReplicaValidatorTest, InvalidDatatype) {
    req.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetReplicaValidatorTest, IvalidCas) {
    req.setCas(0xff);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetReplicaValidatorTest, InvalidKey) {
    // The key must be present
    req.setKeylen(0);
    req.setBodylen(0);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetReplicaValidatorTest, InvalidBodylen) {
    req.setBodylen(8);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class ReturnMetaValidatorTest : public ::testing::WithParamInterface<bool>,
                                public ValidatorTest {
public:
    ReturnMetaValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        using cb::mcbp::request::ReturnMetaPayload;
        using cb::mcbp::request::ReturnMetaType;
        ReturnMetaPayload payload;
        payload.setExpiration(0);
        payload.setFlags(0xdeadbeef);
        payload.setMutationType(ReturnMetaType::Set);
        auto* ptr =
                reinterpret_cast<ReturnMetaPayload*>(blob + sizeof(request));
        memcpy(ptr, &payload, sizeof(payload));
        req.setExtlen(sizeof(payload));
        req.setKeylen(2);
        req.setBodylen(req.getExtlen() + req.getKeylen() + 2);
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::ReturnMeta,
                                       &request);
    }
};

TEST_P(ReturnMetaValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(ReturnMetaValidatorTest, InvalidExtlen) {
    req.setExtlen(0);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
    req.setExtlen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ReturnMetaValidatorTest, Extras) {
    using cb::mcbp::request::ReturnMetaPayload;
    using cb::mcbp::request::ReturnMetaType;
    auto* ptr = reinterpret_cast<ReturnMetaPayload*>(blob + sizeof(request));
    ptr->setMutationType(ReturnMetaType::Add);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
    ptr->setMutationType(ReturnMetaType::Set);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
    ptr->setMutationType(ReturnMetaType::Del);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
    ptr->setMutationType(ReturnMetaType(0));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
    ptr->setMutationType(ReturnMetaType(4));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ReturnMetaValidatorTest, InvalidDatatype) {
    using namespace cb::mcbp;
    for (const auto& datatype : {Datatype::JSON,
                                 Datatype::Snappy,
                                 Datatype::Xattr,
                                 Datatype::SnappyCompressedJson}) {
        req.setDatatype(datatype);
        EXPECT_EQ(cb::mcbp::Status::Einval, validate())
                << ::to_string(datatype);
    }
}

TEST_P(ReturnMetaValidatorTest, Cas) {
    req.setCas(0xff);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(ReturnMetaValidatorTest, InvalidKey) {
    // The key must be present
    req.setKeylen(0);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ReturnMetaValidatorTest, Bodylen) {
    req.setBodylen(req.getKeylen() + req.getExtlen());
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

class SeqnoPersistenceValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    SeqnoPersistenceValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        req.setExtlen(sizeof(uint64_t));
        req.setBodylen(req.getExtlen());
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::SeqnoPersistence,
                                       &request);
    }
};

TEST_P(SeqnoPersistenceValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(SeqnoPersistenceValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    req.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SeqnoPersistenceValidatorTest, InvalidDatatype) {
    req.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SeqnoPersistenceValidatorTest, IvalidCas) {
    req.setCas(0xff);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SeqnoPersistenceValidatorTest, InvalidKey) {
    req.setKeylen(2);
    req.setBodylen(req.getBodylen() + req.getKeylen());
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SeqnoPersistenceValidatorTest, InvalidBodylen) {
    req.setBodylen(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class CompactDbValidatorTest : public ::testing::WithParamInterface<bool>,
                               public ValidatorTest {
public:
    CompactDbValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

    class MockPayload : public cb::mcbp::request::CompactDbPayload {
    public:
        void setAlignPad1(uint8_t val) {
            align_pad1 = val;
        }
        void setAlignPad3(uint32_t val) {
            align_pad3 = val;
        }
    };

    void SetUp() override {
        ValidatorTest::SetUp();
        req.setExtlen(sizeof(MockPayload));
        req.setBodylen(req.getExtlen());
    }

    MockPayload& getPayload() {
        return *reinterpret_cast<MockPayload*>(blob + sizeof(request));
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::CompactDb,
                                       &request);
    }
};

TEST_P(CompactDbValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
    req.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(CompactDbValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(CompactDbValidatorTest, InvalidExtras) {
    auto& mock = getPayload();
    mock.setAlignPad1(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
    mock.setAlignPad1(0);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
    mock.setAlignPad3(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
    mock.setAlignPad1(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(CompactDbValidatorTest, InvalidDatatype) {
    req.setDatatype(cb::mcbp::Datatype::Snappy);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(CompactDbValidatorTest, IvalidCas) {
    req.setCas(0xff);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(CompactDbValidatorTest, InvalidKey) {
    req.setKeylen(2);
    req.setBodylen(req.getBodylen() + req.getKeylen());
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(CompactDbValidatorTest, InvalidBodylen) {
    req.setBodylen(req.getBodylen() + 10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// -----------------------------------------------------------------------------
// Bucket Management (CreateBucket, DeleteBucket, ListBuckets, SelectBucket,
// PauseBucket, ResumeBucket)
// -----------------------------------------------------------------------------

class CreateBucketValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
    CreateBucketValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::CreateBucket);
        builder.setKey("mybucket");
        builder.setValue(std::string("ep.so\0{}", 7));
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::CreateBucket,
                                       &request);
    }
};

TEST_P(CreateBucketValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(CreateBucketValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    req.setBodylen(req.getBodylen() + 2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(CreateBucketValidatorTest, InvalidDatatype) {
    req.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(CreateBucketValidatorTest, MissingKey) {
    req.setKeylen(0);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(CreateBucketValidatorTest, InvalidBucketName) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::CreateBucket);
    builder.setKey("invalid/bucket/name");
    builder.setValue("ep.so");
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(CreateBucketValidatorTest, UnknownBucketType) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::CreateBucket);
    builder.setKey("mybucket");
    builder.setValue("nonexistent_engine.so");
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(CreateBucketValidatorTest, NoBucketNotSupported) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::CreateBucket);
    builder.setKey("mybucket");
    builder.setValue("nobucket.so");
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

class ListBucketsValidatorTest : public ::testing::WithParamInterface<bool>,
                                 public ValidatorTest {
public:
    ListBucketsValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::ListBuckets,
                                       &request);
    }
};

TEST_P(ListBucketsValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(ListBucketsValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    req.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ListBucketsValidatorTest, InvalidKey) {
    req.setKeylen(2);
    req.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ListBucketsValidatorTest, InvalidValue) {
    req.setBodylen(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class DeleteBucketValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
    DeleteBucketValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::DeleteBucket);
        builder.setKey("mybucket");
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DeleteBucket,
                                       &request);
    }
};

TEST_P(DeleteBucketValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(DeleteBucketValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    req.setBodylen(req.getBodylen() + 2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DeleteBucketValidatorTest, MissingKey) {
    req.setKeylen(0);
    req.setBodylen(0);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class SelectBucketValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
    SelectBucketValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::SelectBucket);
        builder.setKey("mybucket");
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::SelectBucket,
                                       &request);
    }
};

TEST_P(SelectBucketValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(SelectBucketValidatorTest, EmptyKey) {
    req.setKeylen(0);
    req.setBodylen(0);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(SelectBucketValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    req.setBodylen(req.getBodylen() + 2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SelectBucketValidatorTest, InvalidValue) {
    req.setBodylen(req.getKeylen() + 4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class PauseResumeBucketValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    PauseResumeBucketValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setKey("mybucket");
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate(cb::mcbp::ClientOpcode opcode) {
        req.setOpcode(opcode);
        return ValidatorTest::validate(opcode, &request);
    }
};

TEST_P(PauseResumeBucketValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::PauseBucket));
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::ResumeBucket));
}

TEST_P(PauseResumeBucketValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    req.setBodylen(req.getBodylen() + 2);
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::PauseBucket));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::ResumeBucket));
}

TEST_P(PauseResumeBucketValidatorTest, InvalidCas) {
    req.setCas(0xbeef);
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::PauseBucket));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::ResumeBucket));
}

TEST_P(PauseResumeBucketValidatorTest, InvalidValue) {
    req.setBodylen(req.getKeylen() + 5);
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::PauseBucket));
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::ResumeBucket));
}

// -----------------------------------------------------------------------------
// Ifconfig Validator Test
// -----------------------------------------------------------------------------

class IfconfigValidatorTest : public ::testing::WithParamInterface<bool>,
                              public ValidatorTest {
public:
    IfconfigValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::Ifconfig,
                                       &request);
    }
};

TEST_P(IfconfigValidatorTest, ListCorrect) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::Ifconfig);
    builder.setKey("list");
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(IfconfigValidatorTest, ListWithValueFails) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::Ifconfig);
    builder.setKey("list");
    builder.setValue("non-empty");
    builder.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(IfconfigValidatorTest, DeleteCorrect) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::Ifconfig);
    builder.setKey("delete");
    builder.setValue(R"({"port": 11210})");
    builder.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(IfconfigValidatorTest, DeleteWithoutValueFails) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::Ifconfig);
    builder.setKey("delete");
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(IfconfigValidatorTest, DefineCorrect) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::Ifconfig);
    builder.setKey("define");
    builder.setValue(
            R"({"port": 11210, "host": "127.0.0.1", "family": "inet", "type": "mcbp"})");
    builder.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(IfconfigValidatorTest, DefineInvalidJson) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::Ifconfig);
    builder.setKey("define");
    builder.setValue("not valid json");
    builder.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(IfconfigValidatorTest, TlsCorrect) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::Ifconfig);
    builder.setKey("tls");
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(IfconfigValidatorTest, InvalidKey) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::Ifconfig);
    builder.setKey("invalid_key");
    builder.setValue(R"({"foo": "bar"})");
    builder.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(IfconfigValidatorTest, InvalidExtlen) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::Ifconfig);
    builder.setKey("list");
    uint8_t ext = 1;
    builder.setExtras({&ext, sizeof(ext)});
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// -----------------------------------------------------------------------------
// Security / Active Encryption Keys / Prune / Register Auth Token
// -----------------------------------------------------------------------------

class SetActiveEncryptionKeysValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    SetActiveEncryptionKeysValidatorTest()
        : ValidatorTest(GetParam()), req(request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::SetActiveEncryptionKeys);
        builder.setKey("@audit");
        builder.setValue(R"({"keystore": {}})");
        builder.setDatatype(cb::mcbp::Datatype::JSON);
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::SetActiveEncryptionKeys, &request);
    }
};

TEST_P(SetActiveEncryptionKeysValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(SetActiveEncryptionKeysValidatorTest, InvalidEntity) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::SetActiveEncryptionKeys);
    builder.setKey("@unknown_entity");
    builder.setValue(R"({"keystore": {}})");
    builder.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetActiveEncryptionKeysValidatorTest, InvalidJson) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::SetActiveEncryptionKeys);
    builder.setKey("@audit");
    builder.setValue("not json");
    builder.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetActiveEncryptionKeysValidatorTest, MissingKeystore) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::SetActiveEncryptionKeys);
    builder.setKey("@audit");
    builder.setValue(R"({"foo": "bar"})");
    builder.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetActiveEncryptionKeysValidatorTest, UnavailableUnencryptedKeyId) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::SetActiveEncryptionKeys);
    builder.setKey("@audit");
    builder.setValue(R"({"unavailable": ["unencrypted"], "keystore": {}})");
    builder.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetActiveEncryptionKeysValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    req.setBodylen(req.getBodylen() + 2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class PruneEncryptionKeysValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    PruneEncryptionKeysValidatorTest()
        : ValidatorTest(GetParam()), req(request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::PruneEncryptionKeys);
        builder.setKey("@audit");
        builder.setValue(R"(["key1", "key2"])");
        builder.setDatatype(cb::mcbp::Datatype::JSON);
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::PruneEncryptionKeys, &request);
    }
};

TEST_P(PruneEncryptionKeysValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(PruneEncryptionKeysValidatorTest, InvalidEntity) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::PruneEncryptionKeys);
    builder.setKey("@unknown_entity");
    builder.setValue(R"(["key1"])");
    builder.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(PruneEncryptionKeysValidatorTest, InvalidJson) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::PruneEncryptionKeys);
    builder.setKey("@audit");
    builder.setValue("not json");
    builder.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class RegisterAuthTokenValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    RegisterAuthTokenValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::RegisterAuthToken);
        builder.setValue(
                R"({"id": 1, "token": "jwt_token_val", "type": "JWT"})");
        builder.setDatatype(cb::mcbp::Datatype::JSON);
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::RegisterAuthToken, &request);
    }
};

TEST_P(RegisterAuthTokenValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(RegisterAuthTokenValidatorTest, RemoveTokenOnlyId) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::RegisterAuthToken);
    builder.setValue(R"({"id": 1})");
    builder.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(RegisterAuthTokenValidatorTest, MissingId) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::RegisterAuthToken);
    builder.setValue(R"({"token": "jwt_token_val", "type": "JWT"})");
    builder.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RegisterAuthTokenValidatorTest, UnsupportedType) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::RegisterAuthToken);
    builder.setValue(R"({"id": 1, "token": "val", "type": "OAUTH"})");
    builder.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(RegisterAuthTokenValidatorTest, InvalidDatatype) {
    req.setDatatype(cb::mcbp::Datatype::Raw);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RegisterAuthTokenValidatorTest, InvalidKey) {
    req.setKeylen(2);
    req.setBodylen(req.getBodylen() + 2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// -----------------------------------------------------------------------------
// Range Scan (RangeScanCreate, RangeScanContinue, RangeScanCancel)
// -----------------------------------------------------------------------------

class RangeScanCreateValidatorTest : public ::testing::WithParamInterface<bool>,
                                     public ValidatorTest {
public:
    RangeScanCreateValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::RangeScanCreate);
        builder.setValue(R"({"range": {"start": "a", "end": "z"}})");
        builder.setDatatype(cb::mcbp::Datatype::JSON);
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::RangeScanCreate,
                                       &request);
    }
};

TEST_P(RangeScanCreateValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(RangeScanCreateValidatorTest, InvalidDatatype) {
    req.setDatatype(cb::mcbp::Datatype::Raw);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RangeScanCreateValidatorTest, InvalidJson) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::RangeScanCreate);
    builder.setValue("invalid json");
    builder.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RangeScanCreateValidatorTest, InvalidKey) {
    req.setKeylen(2);
    req.setBodylen(req.getBodylen() + 2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RangeScanCreateValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    req.setBodylen(req.getBodylen() + 2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class RangeScanContinueValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    RangeScanContinueValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        cb::mcbp::request::RangeScanContinuePayload payload{};
        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::RangeScanContinue);
        builder.setExtras(payload.getBuffer());
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::RangeScanContinue, &request);
    }
};

TEST_P(RangeScanContinueValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(RangeScanContinueValidatorTest, InvalidExtlen) {
    req.setExtlen(4);
    req.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RangeScanContinueValidatorTest, InvalidKey) {
    req.setKeylen(2);
    req.setBodylen(req.getBodylen() + 2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(RangeScanContinueValidatorTest, InvalidValue) {
    req.setBodylen(req.getExtlen() + 4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class RangeScanCancelValidatorTest : public ::testing::WithParamInterface<bool>,
                                     public ValidatorTest {
public:
    RangeScanCancelValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        cb::rangescan::Id id{};
        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::RangeScanCancel);
        builder.setExtras(cb::const_byte_buffer{id.data(), id.size()});
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::RangeScanCancel,
                                       &request);
    }
};

TEST_P(RangeScanCancelValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(RangeScanCancelValidatorTest, InvalidExtlen) {
    req.setExtlen(4);
    req.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// -----------------------------------------------------------------------------
// Snapshots (PrepareSnapshot, ReleaseSnapshot, DownloadSnapshot,
// GetFileFragment)
// -----------------------------------------------------------------------------

class PrepareSnapshotValidatorTest : public ::testing::WithParamInterface<bool>,
                                     public ValidatorTest {
public:
    PrepareSnapshotValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::PrepareSnapshot);
        builder.setValue(R"({"storage": "couchstore"})");
        builder.setDatatype(cb::mcbp::Datatype::JSON);
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::PrepareSnapshot,
                                       &request);
    }
};

TEST_P(PrepareSnapshotValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(PrepareSnapshotValidatorTest, InvalidDatatype) {
    req.setDatatype(cb::mcbp::Datatype::Raw);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(PrepareSnapshotValidatorTest, InvalidJson) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::PrepareSnapshot);
    builder.setValue("not a json string");
    builder.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(PrepareSnapshotValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    req.setBodylen(req.getBodylen() + 2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(PrepareSnapshotValidatorTest, InvalidKey) {
    req.setKeylen(2);
    req.setBodylen(req.getBodylen() + 2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class ReleaseSnapshotValidatorTest : public ::testing::WithParamInterface<bool>,
                                     public ValidatorTest {
public:
    ReleaseSnapshotValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::ReleaseSnapshot);
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::ReleaseSnapshot,
                                       &request);
    }
};

TEST_P(ReleaseSnapshotValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(ReleaseSnapshotValidatorTest, WithKey) {
    req.setKeylen(4);
    req.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(ReleaseSnapshotValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    req.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ReleaseSnapshotValidatorTest, InvalidCas) {
    req.setCas(0xbeef);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class DownloadSnapshotValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    DownloadSnapshotValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::DownloadSnapshot);
        builder.setValue(R"({"foo": "bar"})");
        builder.setDatatype(cb::mcbp::Datatype::JSON);
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DownloadSnapshot,
                                       &request);
    }
};

TEST_P(DownloadSnapshotValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(DownloadSnapshotValidatorTest, InvalidDatatype) {
    req.setDatatype(cb::mcbp::Datatype::Raw);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DownloadSnapshotValidatorTest, InvalidJson) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DownloadSnapshot);
    builder.setValue("invalid json");
    builder.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class GetFileFragmentValidatorTest : public ::testing::WithParamInterface<bool>,
                                     public ValidatorTest {
public:
    GetFileFragmentValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::GetFileFragment);
        builder.setKey("fragment_file");
        builder.setValue(R"({"id": 1, "offset": "0", "length": "100"})");
        builder.setDatatype(cb::mcbp::Datatype::JSON);
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::GetFileFragment,
                                       &request);
    }
};

TEST_P(GetFileFragmentValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(GetFileFragmentValidatorTest, MissingId) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::GetFileFragment);
    builder.setKey("fragment_file");
    builder.setValue(R"({"offset": "0", "length": "100"})");
    builder.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetFileFragmentValidatorTest, NonStringOffset) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::GetFileFragment);
    builder.setKey("fragment_file");
    builder.setValue(R"({"id": 1, "offset": 0, "length": "100"})");
    builder.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(GetFileFragmentValidatorTest, MissingKey) {
    req.setKeylen(0);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

// -----------------------------------------------------------------------------
// SetBucketDataLimitExceeded & AdjustTimeofday
// -----------------------------------------------------------------------------

class SetBucketDataLimitExceededValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    SetBucketDataLimitExceededValidatorTest()
        : ValidatorTest(GetParam()), req(request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        cb::mcbp::request::SetBucketDataLimitExceededPayload payload;
        payload.setStatus(cb::mcbp::Status::BucketSizeLimitExceeded);
        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::SetBucketDataLimitExceeded);
        builder.setKey("mybucket");
        builder.setExtras(payload.getBuffer());
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::SetBucketDataLimitExceeded, &request);
    }
};

TEST_P(SetBucketDataLimitExceededValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(SetBucketDataLimitExceededValidatorTest, InvalidStatus) {
    cb::mcbp::request::SetBucketDataLimitExceededPayload payload;
    payload.setStatus(cb::mcbp::Status::Eaccess);
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::SetBucketDataLimitExceeded);
    builder.setKey("mybucket");
    builder.setExtras(payload.getBuffer());
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetBucketDataLimitExceededValidatorTest, MissingKey) {
    req.setKeylen(0);
    req.setBodylen(req.getExtlen());
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetBucketDataLimitExceededValidatorTest, InvalidExtlen) {
    req.setExtlen(1);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class AdjustTimeofdayValidatorTest : public ::testing::WithParamInterface<bool>,
                                     public ValidatorTest {
public:
    AdjustTimeofdayValidatorTest() : ValidatorTest(GetParam()), req(request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        setUnitTestMode(true);
        cb::mcbp::request::AdjustTimePayload payload;
        payload.setTimeType(
                cb::mcbp::request::AdjustTimePayload::TimeType::Uptime);
        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::AdjustTimeofday);
        builder.setExtras(payload.getBuffer());
    }

    void TearDown() override {
        setUnitTestMode(false);
        ValidatorTest::TearDown();
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::AdjustTimeofday,
                                       &request);
    }
};

TEST_P(AdjustTimeofdayValidatorTest, CorrectMessageInUnitTestMode) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(AdjustTimeofdayValidatorTest, NotSupportedWithoutUnitTestMode) {
    setUnitTestMode(false);
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(AdjustTimeofdayValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    req.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(AdjustTimeofdayValidatorTest, InvalidKey) {
    req.setKeylen(2);
    req.setBodylen(req.getExtlen() + 2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(AdjustTimeofdayValidatorTest, InvalidCas) {
    req.setCas(0xbeef);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         DropPrivilegeValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         GetClusterConfigValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         SetClusterConfigValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         StartStopPersistenceValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         EnableDisableTrafficValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         GetKeysValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         SetParamValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         GetReplicaValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         ReturnMetaValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         SeqnoPersistenceValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         CompactDbValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         CreateBucketValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         ListBucketsValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         DeleteBucketValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         SelectBucketValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         PauseResumeBucketValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         IfconfigValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         SetActiveEncryptionKeysValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         PruneEncryptionKeysValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         RegisterAuthTokenValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         RangeScanCreateValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         RangeScanContinueValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         RangeScanCancelValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         PrepareSnapshotValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         ReleaseSnapshotValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         DownloadSnapshotValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         GetFileFragmentValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         SetBucketDataLimitExceededValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         AdjustTimeofdayValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

} // namespace mcbp::test
