/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include <daemon/settings.h>
#include <event2/event.h>
#include <include/mcbp/protocol/framebuilder.h>
#include <mcbp/protocol/header.h>
#include <memcached/protocol_binary.h>
#include <gsl/gsl>
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
    DropPrivilegeValidatorTest()
        : ValidatorTest(GetParam()), req(request.message.header.request) {
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
                                       static_cast<void*>(&request));
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
    GetClusterConfigValidatorTest()
        : ValidatorTest(GetParam()), req(request.message.header.request) {
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::GetClusterConfig,
                                       static_cast<void*>(&request));
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
    SetClusterConfigValidatorTest()
        : ValidatorTest(GetParam()), req(request.message.header.request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        req.setExtlen(4);
        req.setBodylen(32);
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::SetClusterConfig,
                                       static_cast<void*>(&request));
    }
};

TEST_P(SetClusterConfigValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(SetClusterConfigValidatorTest, WithRevision) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    cb::mcbp::request::SetClusterConfigPayload extras;
    extras.setRevision(0);
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::SetClusterConfig);
    builder.setExtras(extras.getBuffer());
    builder.setValue(R"({"rev":0})");
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(SetClusterConfigValidatorTest, InvalidRevisionNumber) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
    cb::mcbp::request::SetClusterConfigPayload extras;
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

TEST_P(SetClusterConfigValidatorTest, Cas) {
    req.setCas(0xff);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
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
        : ValidatorTest(GetParam()), req(request.message.header.request) {
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate(bool start) {
        if (start) {
            return ValidatorTest::validate(
                    cb::mcbp::ClientOpcode::StartPersistence,
                    static_cast<void*>(&request));
        }
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::StopPersistence,
                                       static_cast<void*>(&request));
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
        : ValidatorTest(GetParam()), req(request.message.header.request) {
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate(bool start) {
        if (start) {
            return ValidatorTest::validate(
                    cb::mcbp::ClientOpcode::EnableTraffic,
                    static_cast<void*>(&request));
        }
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DisableTraffic,
                                       static_cast<void*>(&request));
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

class ScrubValidatorTest : public ::testing::WithParamInterface<bool>,
                           public ValidatorTest {
public:
    ScrubValidatorTest()
        : ValidatorTest(GetParam()), req(request.message.header.request) {
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::Scrub,
                                       static_cast<void*>(&request));
    }
};

TEST_P(ScrubValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(ScrubValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    req.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ScrubValidatorTest, InvalidDatatype) {
    req.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ScrubValidatorTest, IvalidCas) {
    req.setCas(0xff);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ScrubValidatorTest, InvalidKey) {
    req.setKeylen(2);
    req.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(ScrubValidatorTest, InvalidBodylen) {
    req.setBodylen(8);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class GetKeysValidatorTest : public ::testing::WithParamInterface<bool>,
                             public ValidatorTest {
public:
    GetKeysValidatorTest()
        : ValidatorTest(GetParam()), req(request.message.header.request) {
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
                                       static_cast<void*>(&request));
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
    SetParamValidatorTest()
        : ValidatorTest(GetParam()), req(request.message.header.request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        req.setExtlen(sizeof(cb::mcbp::request::SetParamPayload));
        req.setKeylen(2);
        req.setBodylen(req.getExtlen() + req.getKeylen() + 2);
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::SetParam,
                                       static_cast<void*>(&request));
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

TEST_P(SetParamValidatorTest, Cas) {
    req.setCas(0xff);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
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
    GetReplicaValidatorTest()
        : ValidatorTest(GetParam()), req(request.message.header.request) {
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
                                       static_cast<void*>(&request));
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
    ReturnMetaValidatorTest()
        : ValidatorTest(GetParam()), req(request.message.header.request) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        using cb::mcbp::request::ReturnMetaPayload;
        using cb::mcbp::request::ReturnMetaType;
        ReturnMetaPayload payload;
        payload.setExpiration(0);
        payload.setFlags(0xdeadbeef);
        payload.setMutationType(ReturnMetaType::Set);
        auto* ptr = reinterpret_cast<ReturnMetaPayload*>(request.bytes + 24);
        memcpy(ptr, &payload, sizeof(payload));
        req.setExtlen(sizeof(payload));
        req.setKeylen(2);
        req.setBodylen(req.getExtlen() + req.getKeylen() + 2);
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::ReturnMeta,
                                       static_cast<void*>(&request));
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
    auto* ptr = reinterpret_cast<ReturnMetaPayload*>(request.bytes + 24);
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
    req.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
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
    SeqnoPersistenceValidatorTest()
        : ValidatorTest(GetParam()), req(request.message.header.request) {
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
                                       static_cast<void*>(&request));
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

class LastClosedCheckpointValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    LastClosedCheckpointValidatorTest()
        : ValidatorTest(GetParam()), req(request.message.header.request) {
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::LastClosedCheckpoint,
                static_cast<void*>(&request));
    }
};

TEST_P(LastClosedCheckpointValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(LastClosedCheckpointValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    req.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(LastClosedCheckpointValidatorTest, InvalidDatatype) {
    req.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(LastClosedCheckpointValidatorTest, IvalidCas) {
    req.setCas(0xff);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(LastClosedCheckpointValidatorTest, InvalidKey) {
    req.setKeylen(2);
    req.setBodylen(req.getBodylen() + req.getKeylen());
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(LastClosedCheckpointValidatorTest, InvalidBodylen) {
    req.setBodylen(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class CreateCheckpointValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    CreateCheckpointValidatorTest()
        : ValidatorTest(GetParam()), req(request.message.header.request) {
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::CreateCheckpoint,
                                       static_cast<void*>(&request));
    }
};

TEST_P(CreateCheckpointValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(CreateCheckpointValidatorTest, InvalidExtlen) {
    req.setExtlen(2);
    req.setBodylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(CreateCheckpointValidatorTest, InvalidDatatype) {
    req.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(CreateCheckpointValidatorTest, IvalidCas) {
    req.setCas(0xff);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(CreateCheckpointValidatorTest, InvalidKey) {
    req.setKeylen(2);
    req.setBodylen(req.getBodylen() + req.getKeylen());
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(CreateCheckpointValidatorTest, InvalidBodylen) {
    req.setBodylen(10);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class CompactDbValidatorTest : public ::testing::WithParamInterface<bool>,
                               public ValidatorTest {
public:
    CompactDbValidatorTest()
        : ValidatorTest(GetParam()), req(request.message.header.request) {
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
        return *reinterpret_cast<MockPayload*>(request.bytes +
                                               sizeof(request.bytes));
    }

protected:
    cb::mcbp::Request& req;
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::CompactDb,
                                       static_cast<void*>(&request));
    }
};

TEST_P(CompactDbValidatorTest, CorrectMessage) {
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
    req.setDatatype(cb::mcbp::Datatype::JSON);
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
                         ScrubValidatorTest,
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
                         LastClosedCheckpointValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         CreateCheckpointValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         CompactDbValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

} // namespace mcbp::test
