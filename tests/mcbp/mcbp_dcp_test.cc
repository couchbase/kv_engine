/*
 *     Copyright 2019 Couchbase, Inc.
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
#include <daemon/front_end_thread.h>
#include <event2/event.h>
#include <mcbp/protocol/framebuilder.h>
#include <mcbp/protocol/header.h>
#include <memcached/protocol_binary.h>
#include <xattr/blob.h>
#include <memory>

using namespace std::string_view_literals;

namespace mcbp::test {

class DcpOpenValidatorTest : public ::testing::WithParamInterface<bool>,
                             public ValidatorTest {
public:
    DcpOpenValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        memset(blob, 0, sizeof(blob));
        request.setMagic(cb::mcbp::Magic::ClientRequest);
        request.setExtlen(8);
        request.setKeylen(2);
        request.setBodylen(10);
        request.setDatatype(cb::mcbp::Datatype::Raw);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DcpOpen,
                                       static_cast<void*>(&request));
    }

    cb::mcbp::Request& request = *reinterpret_cast<cb::mcbp::Request*>(blob);
};

TEST_P(DcpOpenValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpOpenValidatorTest, InvalidExtlen) {
    request.setExtlen(9);
    request.setBodylen(11);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpOpenValidatorTest, InvalidKeylen) {
    request.setKeylen(0);
    request.setBodylen(8);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpOpenValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpOpenValidatorTest, Value) {
    request.setBodylen(10 + 20);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpOpenValidatorTest, MB34280_NameTooLongKeylen) {
    auto blen = request.getBodylen() - request.getKeylen();
    request.setKeylen(201);
    request.setBodylen(blen + 201);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpOpenValidatorTest, Pitr) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)}, true);
    using cb::mcbp::request::DcpOpenPayload;
    DcpOpenPayload payload;

    // It is not allowed for consumers to use PiTR
    payload.setFlags(DcpOpenPayload::PiTR);
    builder.setExtras(
            {reinterpret_cast<const char*>(&payload), sizeof(payload)});
    EXPECT_EQ(cb::mcbp::Status::Einval, validate())
            << "Consumer can't use PiTR";

    // Neither should Notifiers
    payload.setFlags(DcpOpenPayload::PiTR | DcpOpenPayload::Notifier);
    builder.setExtras(
            {reinterpret_cast<const char*>(&payload), sizeof(payload)});
    EXPECT_EQ(cb::mcbp::Status::Einval, validate())
            << "Notifier can't use PiTR";

    // Producers should be able to use PiTR
    payload.setFlags(DcpOpenPayload::PiTR | DcpOpenPayload::Producer);
    builder.setExtras(
            {reinterpret_cast<const char*>(&payload), sizeof(payload)});
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate())
            << "Producer should be able to use PiTR";
}

class DcpAddStreamValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
    DcpAddStreamValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.setExtlen(4);
        request.message.header.request.setBodylen(4);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DcpAddStream,
                                       static_cast<void*>(&request));
    }
};

TEST_P(DcpAddStreamValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpAddStreamValidatorTest, InvalidExtlen) {
    request.message.header.request.setExtlen(5);
    request.message.header.request.setBodylen(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpAddStreamValidatorTest, InvalidKeylen) {
    request.message.header.request.setKeylen(4);
    request.message.header.request.setBodylen(8);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpAddStreamValidatorTest, InvalidDatatype) {
    request.message.header.request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpAddStreamValidatorTest, InvalidBody) {
    request.message.header.request.setBodylen(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class DcpCloseStreamValidatorTest : public ::testing::WithParamInterface<bool>,
                                    public ValidatorTest {
public:
    DcpCloseStreamValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DcpCloseStream,
                                       static_cast<void*>(&request));
    }
};

TEST_P(DcpCloseStreamValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpCloseStreamValidatorTest, InvalidExtlen) {
    request.message.header.request.setExtlen(5);
    request.message.header.request.setBodylen(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpCloseStreamValidatorTest, InvalidKeylen) {
    request.message.header.request.setKeylen(4);
    request.message.header.request.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpCloseStreamValidatorTest, InvalidDatatype) {
    request.message.header.request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpCloseStreamValidatorTest, InvalidBody) {
    request.message.header.request.setBodylen(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class DcpGetFailoverLogValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    DcpGetFailoverLogValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::DcpGetFailoverLog,
                static_cast<void*>(&request));
    }
};

TEST_P(DcpGetFailoverLogValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpGetFailoverLogValidatorTest, InvalidExtlen) {
    request.message.header.request.setExtlen(5);
    request.message.header.request.setBodylen(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpGetFailoverLogValidatorTest, InvalidKeylen) {
    request.message.header.request.setKeylen(4);
    request.message.header.request.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpGetFailoverLogValidatorTest, InvalidDatatype) {
    request.message.header.request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpGetFailoverLogValidatorTest, InvalidBody) {
    request.message.header.request.setBodylen(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class DcpStreamReqValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
    DcpStreamReqValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.setExtlen(48);
        request.message.header.request.setBodylen(48);
    }
    bool isCollectionsEnabled() const {
        return GetParam();
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DcpStreamReq,
                                       static_cast<void*>(&request));
    }
};

TEST_P(DcpStreamReqValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpStreamReqValidatorTest, InvalidExtlen) {
    request.message.header.request.setExtlen(5);
    request.message.header.request.setBodylen(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpStreamReqValidatorTest, InvalidKeylen) {
    request.message.header.request.setKeylen(4);
    request.message.header.request.setBodylen(54);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpStreamReqValidatorTest, InvalidDatatype) {
    request.message.header.request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpStreamReqValidatorTest, MessageValue) {
    request.message.header.request.setBodylen(48 + 20);
    // Only valid when collections enabled
    if (isCollectionsEnabled()) {
        EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());

    } else {
        EXPECT_EQ(cb::mcbp::Status::Einval, validate());
    }
}

class DcpStreamEndValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
    DcpStreamEndValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.setExtlen(4);
        request.message.header.request.setBodylen(4);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DcpStreamEnd,
                                       static_cast<void*>(&request));
    }
};

TEST_P(DcpStreamEndValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpStreamEndValidatorTest, InvalidExtlen) {
    request.message.header.request.setExtlen(5);
    request.message.header.request.setBodylen(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpStreamEndValidatorTest, InvalidKeylen) {
    request.message.header.request.setKeylen(4);
    request.message.header.request.setBodylen(8);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpStreamEndValidatorTest, InvalidDatatype) {
    request.message.header.request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpStreamEndValidatorTest, InvalidBody) {
    request.message.header.request.setBodylen(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class DcpSnapshotMarkerValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    DcpSnapshotMarkerValidatorTest()
        : ValidatorTest(GetParam()),
          builder({blob, sizeof(blob)}),
          header(*reinterpret_cast<cb::mcbp::Request*>(blob)) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.setExtlen(20);
        request.message.header.request.setBodylen(20);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                static_cast<void*>(&request));
    }
    std::string validate_error_context(
            cb::mcbp::Status expectedStatus = cb::mcbp::Status::Einval) {
        return ValidatorTest::validate_error_context(
                cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                static_cast<void*>(blob),
                expectedStatus);
    }
    cb::mcbp::RequestBuilder builder;
    cb::mcbp::Request& header;
};

TEST_P(DcpSnapshotMarkerValidatorTest, CorrectMessage) {
    // Validate V1 Payload
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());

    // V2.0
    request.message.header.request.setExtlen(1);
    request.message.header.request.setBodylen(37);
    cb::mcbp::request::DcpSnapshotMarkerV2xPayload extras{
            cb::mcbp::request::DcpSnapshotMarkerV2xVersion::Zero};
    builder.setExtras(extras.getBuffer());
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpSnapshotMarkerValidatorTest, InvalidExtlen) {
    request.message.header.request.setExtlen(22);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpSnapshotMarkerValidatorTest, InvalidKeylen) {
    request.message.header.request.setKeylen(32);
    request.message.header.request.setBodylen(52);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpSnapshotMarkerValidatorTest, InvalidBodylen) {
    request.message.header.request.setBodylen(100);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
    cb::mcbp::request::DcpSnapshotMarkerV2xPayload extras{
            cb::mcbp::request::DcpSnapshotMarkerV2xVersion::Zero};
    builder.setExtras(extras.getBuffer());
    request.message.header.request.setExtlen(1);
    request.message.header.request.setBodylen(40);
    EXPECT_EQ("valuelen not expected:36", validate_error_context());
}

TEST_P(DcpSnapshotMarkerValidatorTest, InvalidDatatype) {
    request.message.header.request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpSnapshotMarkerValidatorTest, InvalidV2Version) {
    // Valid V2.0 size
    request.message.header.request.setExtlen(1);
    request.message.header.request.setBodylen(37);
    uint8_t brokenVersion = 101;
    builder.setExtras({&brokenVersion, 1});
    EXPECT_EQ("Unsupported dcp snapshot version:101", validate_error_context());
}

/**
 * Test class for DcpMutation validation - the bool parameter toggles
 * collections on/off (as that subtly changes the encoding of a mutation)
 */
class DcpMutationValidatorTest : public ::testing::WithParamInterface<bool>,
                                 public ValidatorTest {
public:
public:
    DcpMutationValidatorTest() : ValidatorTest(GetParam()) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
        cb::mcbp::request::DcpMutationPayload extras;
        /// DcpMutation requires a non-zero seqno.
        extras.setBySeqno(1);
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::DcpMutation);
        uint8_t key[2] = {};
        builder.setExtras(extras.getBuffer());
        const size_t keysize = GetParam() ? 2 : 1;
        builder.setKey({key, keysize});
    }

    bool isCollectionsEnabled() const {
        return GetParam();
    }

protected:
    std::string validate_error_context(
            cb::mcbp::Status expectedStatus = cb::mcbp::Status::Einval) {
        return ValidatorTest::validate_error_context(
                cb::mcbp::ClientOpcode::DcpMutation,
                static_cast<void*>(blob),
                expectedStatus);
    }
};

TEST_P(DcpMutationValidatorTest, CorrectMessage) {
    EXPECT_EQ("Attached bucket does not support DCP",
              validate_error_context(cb::mcbp::Status::NotSupported));
}

TEST_P(DcpMutationValidatorTest, InvalidExtlen) {
    request.message.header.request.setExtlen(21);
    request.message.header.request.setBodylen(23);
    EXPECT_EQ("Request must include extras of length 31",
              validate_error_context());
}

TEST_P(DcpMutationValidatorTest, InvalidKeylen) {
    request.message.header.request.setKeylen(0);
    request.message.header.request.setBodylen(31);
    EXPECT_EQ("Request must include key", validate_error_context());
}

// A key which has no leb128 stop-byte
TEST_P(DcpMutationValidatorTest, InvalidKey1) {
    if (isCollectionsEnabled()) {
        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)}, true);
        uint8_t key[10] = {};
        std::fill(key, key + 10, 0x81ull);
        builder.setKey({key, sizeof(key)});
        EXPECT_EQ("No stop-byte found", validate_error_context());
    }
}

// A key which has a stop-byte, but no data after that
TEST_P(DcpMutationValidatorTest, InvalidKey2) {
    if (isCollectionsEnabled()) {
        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)}, true);
        // Just make a valid key that is all leb128 and no logical key.
        cb::mcbp::unsigned_leb128<uint32_t> leb128(0xdaf7);
        builder.setKey({leb128.data(), leb128.size()});
        EXPECT_EQ("No logical key found", validate_error_context());
    }
}

/**
 * Test class for DcpDeletion validation - the bool parameter toggles
 * collections on/off (as that subtly changes the encoding of a deletion)
 */
class DcpDeletionValidatorTest : public ::testing::WithParamInterface<bool>,
                                 public ValidatorTest {
public:
    DcpDeletionValidatorTest()
        : ValidatorTest(GetParam()),
          builder({blob, sizeof(blob)}),
          header(*reinterpret_cast<cb::mcbp::Request*>(blob)) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();

        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::DcpDeletion);
        builder.setDatatype(cb::mcbp::Datatype::Raw);
        builder.setKey("document-key");

        if (isCollectionsEnabled()) {
            cb::mcbp::request::DcpDeletionV2Payload extras(0, 0, 0);
            builder.setExtras(extras.getBuffer());
        } else {
            cb::mcbp::request::DcpDeletionV1Payload extras(0, 0);
            builder.setExtras(extras.getBuffer());
        }
    }

    bool isCollectionsEnabled() const {
        return GetParam();
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DcpDeletion,
                                       static_cast<void*>(blob));
    }

    cb::mcbp::RequestBuilder builder;
    cb::mcbp::Request& header;
};

TEST_P(DcpDeletionValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpDeletionValidatorTest, ValidDatatype) {
    using cb::mcbp::Datatype;
    const std::array<uint8_t, 4> datatypes = {
            {uint8_t(Datatype::Raw),
             uint8_t(Datatype::Raw) | uint8_t(Datatype::Snappy),
             uint8_t(Datatype::Xattr),
             uint8_t(Datatype::Xattr) | uint8_t(Datatype::Snappy)}};
    for (auto valid : datatypes) {
        header.setDatatype(Datatype(valid));

        std::string_view value = "My value"sv;
        cb::xattr::Blob blob;
        cb::compression::Buffer deflated;

        if (mcbp::datatype::is_xattr(valid)) {
            blob.set("_foo"sv, R"({"bar":5})"sv);
            value = blob.finalize();
        }

        if (mcbp::datatype::is_snappy(valid)) {
            cb::compression::deflate(
                    cb::compression::Algorithm::Snappy, value, deflated);
            value = deflated;
        }

        builder.setValue(value);
        EXPECT_EQ(cb::mcbp::Status::NotSupported, validate())
                << "Testing valid datatype: "
                << mcbp::datatype::to_string(protocol_binary_datatype_t(valid));
    }
}

TEST_P(DcpDeletionValidatorTest, InvalidDatatype) {
    using cb::mcbp::Datatype;
    const std::array<uint8_t, 2> datatypes = {
            {uint8_t(Datatype::JSON),
             uint8_t(Datatype::Snappy) | uint8_t(Datatype::JSON)}};

    for (auto invalid : datatypes) {
        header.setDatatype(Datatype(invalid));

        std::string_view value = R"({"foo":"bar"})"sv;
        cb::compression::Buffer deflated;

        if (mcbp::datatype::is_snappy(invalid)) {
            cb::compression::deflate(
                    cb::compression::Algorithm::Snappy, value, deflated);
            value = deflated;
        }
        builder.setValue(value);

        EXPECT_EQ(cb::mcbp::Status::Einval, validate())
                << "Testing invalid datatype: "
                << mcbp::datatype::to_string(
                           protocol_binary_datatype_t(invalid));
    }
}

TEST_P(DcpDeletionValidatorTest, InvalidExtlen) {
    header.setExtlen(5);
    header.setBodylen(7);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpDeletionValidatorTest, InvalidExtlenCollections) {
    // Flip extlen, so when not collections, set the length collections uses
    header.setExtlen(isCollectionsEnabled()
                             ? sizeof(cb::mcbp::request::DcpDeletionV1Payload)
                             : sizeof(cb::mcbp::request::DcpDeletionV2Payload));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpDeletionValidatorTest, InvalidKeylen) {
    header.setKeylen(GetParam() ? 1 : 0);
    header.setBodylen(18);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpDeletionValidatorTest, WithValue) {
    header.setBodylen(100);
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

/**
 * Test class for DcpExpiration validation - the bool parameter toggles
 * collections on/off (as that subtly changes the encoding of an expiration)
 */
class DcpExpirationValidatorTest : public ::testing::WithParamInterface<bool>,
                                   public ValidatorTest {
public:
public:
    DcpExpirationValidatorTest() : ValidatorTest(GetParam()) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        connection.setCollectionsSupported(GetParam());
        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)});
        cb::mcbp::request::DcpExpirationPayload extras;
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::DcpExpiration);
        uint8_t key[5] = {};
        builder.setExtras(extras.getBuffer());
        const size_t keysize = GetParam() ? 5 : 1;
        builder.setKey({key, keysize});
    }

protected:
    cb::mcbp::Status validate() {
        std::copy(request.bytes, request.bytes + sizeof(request.bytes), blob);
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DcpExpiration,
                                       static_cast<void*>(blob));
    }
};

TEST_P(DcpExpirationValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpExpirationValidatorTest, InvalidExtlen) {
    request.message.header.request.setExtlen(5);
    request.message.header.request.setBodylen(7);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpExpirationValidatorTest, InvalidKeylen) {
    request.message.header.request.setKeylen(GetParam() ? 1 : 0);
    request.message.header.request.setBodylen(19);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpExpirationValidatorTest, WithValue) {
    request.message.header.request.setBodylen(100);
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

class DcpSetVbucketStateValidatorTest
    : public ::testing::WithParamInterface<bool>,
      public ValidatorTest {
public:
    DcpSetVbucketStateValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.setMagic(cb::mcbp::Magic::ClientRequest);
        request.message.header.request.setExtlen(1);
        request.message.header.request.setBodylen(1);
        request.message.header.request.setDatatype(cb::mcbp::Datatype::Raw);

        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)}, true);
        cb::mcbp::request::DcpSetVBucketState extras;
        extras.setState(1);
        builder.setExtras(extras.getBuffer());
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::DcpSetVbucketState,
                static_cast<void*>(&request));
    }
};

TEST_P(DcpSetVbucketStateValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpSetVbucketStateValidatorTest, LegalValues) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)}, true);

    for (int ii = 1; ii < 5; ++ii) {
        cb::mcbp::request::DcpSetVBucketState extras;
        extras.setState(ii);
        builder.setExtras(extras.getBuffer());
        EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
    }
}

TEST_P(DcpSetVbucketStateValidatorTest, InvalidExtlen) {
    request.message.header.request.setExtlen(5);
    request.message.header.request.setBodylen(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpSetVbucketStateValidatorTest, InvalidKeylen) {
    request.message.header.request.setKeylen(4);
    request.message.header.request.setBodylen(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpSetVbucketStateValidatorTest, InvalidDatatype) {
    request.message.header.request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpSetVbucketStateValidatorTest, InvalidBody) {
    request.message.header.request.setBodylen(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpSetVbucketStateValidatorTest, IllegalValues) {
    cb::mcbp::RequestBuilder builder({blob, sizeof(blob)}, true);
    cb::mcbp::request::DcpSetVBucketState extras;
    extras.setState(5);
    builder.setExtras(extras.getBuffer());
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
    extras.setState(0);
    builder.setExtras(extras.getBuffer());
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class DcpNoopValidatorTest : public ::testing::WithParamInterface<bool>,
                             public ValidatorTest {
public:
    DcpNoopValidatorTest() : ValidatorTest(GetParam()) {
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DcpNoop,
                                       static_cast<void*>(&request));
    }
};

TEST_P(DcpNoopValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpNoopValidatorTest, InvalidExtlen) {
    request.message.header.request.setExtlen(5);
    request.message.header.request.setBodylen(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpNoopValidatorTest, InvalidKeylen) {
    request.message.header.request.setKeylen(4);
    request.message.header.request.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpNoopValidatorTest, InvalidDatatype) {
    request.message.header.request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpNoopValidatorTest, InvalidBody) {
    request.message.header.request.setBodylen(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class DcpBufferAckValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
    DcpBufferAckValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.setExtlen(4);
        request.message.header.request.setBodylen(4);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::DcpBufferAcknowledgement,
                static_cast<void*>(&request));
    }
};

TEST_P(DcpBufferAckValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpBufferAckValidatorTest, InvalidExtlen) {
    request.message.header.request.setExtlen(5);
    request.message.header.request.setBodylen(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpBufferAckValidatorTest, InvalidKeylen) {
    request.message.header.request.setKeylen(4);
    request.message.header.request.setBodylen(8);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpBufferAckValidatorTest, InvalidDatatype) {
    request.message.header.request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpBufferAckValidatorTest, InvalidBody) {
    request.message.header.request.setBodylen(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class DcpControlValidatorTest : public ::testing::WithParamInterface<bool>,
                                public ValidatorTest {
public:
    DcpControlValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.message.header.request.setKeylen(4);
        request.message.header.request.setBodylen(8);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DcpControl,
                                       static_cast<void*>(&request));
    }
};

TEST_P(DcpControlValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpControlValidatorTest, InvalidExtlen) {
    request.message.header.request.setExtlen(5);
    request.message.header.request.setBodylen(13);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpControlValidatorTest, InvalidKeylen) {
    request.message.header.request.setKeylen(0);
    request.message.header.request.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpControlValidatorTest, InvalidDatatype) {
    request.message.header.request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpControlValidatorTest, InvalidBody) {
    request.message.header.request.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         DcpOpenValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         DcpAddStreamValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         DcpCloseStreamValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         DcpGetFailoverLogValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         DcpStreamReqValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         DcpStreamEndValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         DcpSnapshotMarkerValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         DcpMutationValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         DcpDeletionValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         DcpExpirationValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         DcpSetVbucketStateValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         DcpNoopValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         DcpBufferAckValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         DcpControlValidatorTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
} // namespace mcbp::test
