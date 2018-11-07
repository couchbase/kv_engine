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

#include "config.h"
#include "mcbp_test.h"

#include <daemon/cookie.h>
#include <daemon/settings.h>
#include <event2/event.h>
#include <mcbp/protocol/header.h>
#include <memcached/protocol_binary.h>
#include <gsl/gsl>
#include <memory>

/**
 * Test all of the command validators we've got to ensure that they
 * catch broken packets. There is still a high number of commands we
 * don't have any command validators for...
 */
namespace mcbp {
namespace test {

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

TEST_P(DropPrivilegeValidatorTest, InvalidMagic) {
    req.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
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

TEST_P(GetClusterConfigValidatorTest, InvalidMagic) {
    req.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
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

TEST_P(SetClusterConfigValidatorTest, InvalidMagic) {
    req.magic = 0;
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

TEST_P(SetClusterConfigValidatorTest, InvalidKey) {
    req.setKeylen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(SetClusterConfigValidatorTest, InvalidBodylen) {
    req.setBodylen(0);
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

TEST_P(StartStopPersistenceValidatorTest, InvalidMagic) {
    req.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(true));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(false));
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

TEST_P(EnableDisableTrafficValidatorTest, InvalidMagic) {
    req.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(true));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(false));
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

TEST_P(ScrubValidatorTest, InvalidMagic) {
    req.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
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

TEST_P(GetKeysValidatorTest, InvalidMagic) {
    req.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
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
        req.setExtlen(sizeof(protocol_binary_engine_param_t));
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

TEST_P(SetParamValidatorTest, InvalidMagic) {
    req.magic = 0;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
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

INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        DropPrivilegeValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());

INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        GetClusterConfigValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());

INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        SetClusterConfigValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());

INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        StartStopPersistenceValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());

INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        EnableDisableTrafficValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());

INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        ScrubValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());

INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        GetKeysValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());

INSTANTIATE_TEST_CASE_P(CollectionsOnOff,
                        SetParamValidatorTest,
                        ::testing::Bool(),
                        ::testing::PrintToStringParamName());

} // namespace test
} // namespace mcbp
