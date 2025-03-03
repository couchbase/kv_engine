/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "mcbp_test.h"

#include <daemon/cookie.h>
#include <daemon/front_end_thread.h>
#include <event2/event.h>
#include <mcbp/codec/dcp_snapshot_marker.h>
#include <mcbp/protocol/framebuilder.h>
#include <mcbp/protocol/header.h>
#include <memcached/limits.h>
#include <memcached/protocol_binary.h>
#include <platform/compress.h>
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
                                       &request);
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

class DcpAddStreamValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
    DcpAddStreamValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.setExtlen(4);
        request.setBodylen(4);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DcpAddStream,
                                       &request);
    }
};

TEST_P(DcpAddStreamValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpAddStreamValidatorTest, InvalidExtlen) {
    request.setExtlen(5);
    request.setBodylen(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpAddStreamValidatorTest, InvalidKeylen) {
    request.setKeylen(4);
    request.setBodylen(8);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpAddStreamValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpAddStreamValidatorTest, InvalidBody) {
    request.setBodylen(12);
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
                                       &request);
    }
};

TEST_P(DcpCloseStreamValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpCloseStreamValidatorTest, InvalidExtlen) {
    request.setExtlen(5);
    request.setBodylen(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpCloseStreamValidatorTest, InvalidKeylen) {
    request.setKeylen(4);
    request.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpCloseStreamValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpCloseStreamValidatorTest, InvalidBody) {
    request.setBodylen(12);
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
                cb::mcbp::ClientOpcode::DcpGetFailoverLog, &request);
    }
};

TEST_P(DcpGetFailoverLogValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpGetFailoverLogValidatorTest, InvalidExtlen) {
    request.setExtlen(5);
    request.setBodylen(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpGetFailoverLogValidatorTest, InvalidKeylen) {
    request.setKeylen(4);
    request.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpGetFailoverLogValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpGetFailoverLogValidatorTest, InvalidBody) {
    request.setBodylen(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class DcpStreamReqValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
    DcpStreamReqValidatorTest()
        : ValidatorTest(GetParam()),
          builder({blob, sizeof(blob)}),
          header(*reinterpret_cast<cb::mcbp::Request*>(blob)) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        request.setExtlen(48);
        request.setBodylen(48);
    }
    bool isCollectionsEnabled() const {
        return GetParam();
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DcpStreamReq,
                                       &request);
    }

    cb::mcbp::RequestBuilder builder;
    cb::mcbp::Request& header;
};

TEST_P(DcpStreamReqValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpStreamReqValidatorTest, InvalidExtlen) {
    request.setExtlen(5);
    request.setBodylen(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpStreamReqValidatorTest, InvalidKeylen) {
    request.setKeylen(4);
    request.setBodylen(54);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpStreamReqValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpStreamReqValidatorTest, InvalidFlags) {
    cb::mcbp::request::DcpStreamReqPayload extras;

    struct FlagInfo {
        uint32_t flag;
        bool valid;
    };
    const std::array<FlagInfo, 32> flagInfo = {{
            {static_cast<uint32_t>(cb::mcbp::DcpAddStreamFlag::TakeOver), true},
            {static_cast<uint32_t>(cb::mcbp::DcpAddStreamFlag::DiskOnly), true},
            {static_cast<uint32_t>(cb::mcbp::DcpAddStreamFlag::ToLatest), true},
            // DCP_ADD_STREAM_FLAG_NO_VALUE is not used anymore, and is hence
            // considered invalid.
            {static_cast<uint32_t>(cb::mcbp::DcpAddStreamFlag::NoValue), false},
            {static_cast<uint32_t>(cb::mcbp::DcpAddStreamFlag::ActiveVbOnly),
             true},
            {static_cast<uint32_t>(cb::mcbp::DcpAddStreamFlag::StrictVbUuid),
             true},
            {static_cast<uint32_t>(cb::mcbp::DcpAddStreamFlag::FromLatest),
             true},
            {static_cast<uint32_t>(
                     cb::mcbp::DcpAddStreamFlag::IgnorePurgedTombstones),
             true},
            // 0x100 is the first of the undefined flags.
            {0x100, false},
            {0x200, false},
            {0x400, false},
            {0x800, false},
            {0x1000, false},
            {0x2000, false},
            {0x4000, false},
            {0x8000, false},
            {0x10000, false},
            {0x20000, false},
            {0x40000, false},
            {0x80000, false},
            {0x100000, false},
            {0x200000, false},
            {0x400000, false},
            {0x800000, false},
            {0x1000000, false},
            {0x2000000, false},
            {0x4000000, false},
            {0x8000000, false},
            {0x10000000, false},
            {0x20000000, false},
            {0x40000000, false},
            {0x80000000, false},
    }};

    for (const auto& info : flagInfo) {
        extras.setFlags(static_cast<cb::mcbp::DcpAddStreamFlag>(info.flag));
        builder.setExtras(extras.getBuffer());
        // "Valid" flags actually return 'NotSupported' as validation runs
        // against a memcache bucket which doesn't support DCP.
        using cb::mcbp::Status;
        EXPECT_EQ(info.valid ? Status::NotSupported : Status::Einval,
                  validate())
                << "for flag " << std::to_string(info.flag);
    }
}

TEST_P(DcpStreamReqValidatorTest, MessageValue) {
    request.setBodylen(48 + 20);
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
        request.setExtlen(4);
        request.setBodylen(4);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DcpStreamEnd,
                                       &request);
    }
};

TEST_P(DcpStreamEndValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpStreamEndValidatorTest, InvalidExtlen) {
    request.setExtlen(5);
    request.setBodylen(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpStreamEndValidatorTest, InvalidKeylen) {
    request.setKeylen(4);
    request.setBodylen(8);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpStreamEndValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpStreamEndValidatorTest, InvalidBody) {
    request.setBodylen(12);
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
        request.setExtlen(20);
        request.setBodylen(20);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::DcpSnapshotMarker, &request);
    }
    std::string validate_error_context(
            cb::mcbp::Status expectedStatus = cb::mcbp::Status::Einval) {
        return ValidatorTest::validate_error_context(
                cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                blob,
                expectedStatus);
    }
    cb::mcbp::RequestBuilder builder;
    cb::mcbp::Request& header;
};

TEST_P(DcpSnapshotMarkerValidatorTest, CorrectMessage) {
    // Validate V1 Payload
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());

    // V2.0
    request.setExtlen(1);
    request.setBodylen(37);
    cb::mcbp::request::DcpSnapshotMarkerV2xPayload extras{
            cb::mcbp::request::DcpSnapshotMarkerV2xVersion::Zero};
    builder.setExtras(extras.getBuffer());
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpSnapshotMarkerValidatorTest, InvalidExtlen) {
    request.setExtlen(22);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpSnapshotMarkerValidatorTest, InvalidKeylen) {
    request.setKeylen(32);
    request.setBodylen(52);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpSnapshotMarkerValidatorTest, InvalidBodylen) {
    request.setBodylen(100);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
    cb::mcbp::request::DcpSnapshotMarkerV2xPayload extras{
            cb::mcbp::request::DcpSnapshotMarkerV2xVersion::Zero};
    builder.setExtras(extras.getBuffer());
    request.setExtlen(1);
    request.setBodylen(40);
    EXPECT_EQ("valuelen not expected:36", validate_error_context());
}

TEST_P(DcpSnapshotMarkerValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpSnapshotMarkerValidatorTest, InvalidV2Version) {
    // Valid V2.0 size
    request.setExtlen(1);
    request.setBodylen(37);
    uint8_t brokenVersion = 101;
    builder.setExtras({&brokenVersion, 1});
    EXPECT_EQ("Unsupported dcp snapshot version:101", validate_error_context());
}

class DcpSnapshotMarkerCodecTest : public ::testing::TestWithParam<bool> {
public:
    void SetUp() override {
        TestWithParam::SetUp();
        flags = GetParam() ? cb::mcbp::request::DcpSnapshotMarkerFlag::Disk
                           : cb::mcbp::request::DcpSnapshotMarkerFlag::None;
    }

protected:
    void test(const cb::mcbp::DcpSnapshotMarker& marker);

    const size_t start = 1;
    const size_t end = 2;
    const size_t hcs = 3;
    const size_t hps = 4;
    const size_t mvs = 5;
    const size_t purge = 6;
    cb::mcbp::request::DcpSnapshotMarkerFlag flags{};
};

void DcpSnapshotMarkerCodecTest::test(
        const cb::mcbp::DcpSnapshotMarker& inMarker) {
    using namespace ::testing;
    using namespace cb::mcbp;
    using namespace cb::mcbp::request;

    auto version = DcpSnapshotMarkerV2xVersion::Zero;
    if (inMarker.getHighPreparedSeqno() || inMarker.getPurgeSeqno()) {
        version = DcpSnapshotMarkerV2xVersion::Two;
    }
    std::vector<uint8_t> buffer(sizeof(Request) + sizeof(DcpStreamIdFrameInfo) +
                                sizeof(DcpSnapshotMarkerV2xPayload) +
                                sizeof(DcpSnapshotMarkerV2_2Value));

    cb::mcbp::RequestBuilder::FrameBuilder builder(
            {buffer.data(), buffer.size()});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpSnapshotMarker);

    const bool requiresDiskFlag =
            inMarker.getHighCompletedSeqno() || inMarker.getHighPreparedSeqno();

    if (requiresDiskFlag &&
        !isFlagSet(inMarker.getFlags(), DcpSnapshotMarkerFlag::Disk)) {
        EXPECT_THROW(inMarker.encode(builder), std::logic_error);
        return;
    }

    inMarker.encode(builder);
    auto& request = *builder.getFrame();
    auto outMarker = DcpSnapshotMarker::decode(request);

    EXPECT_EQ(inMarker.getStartSeqno(), outMarker.getStartSeqno())
            << inMarker.to_json();
    EXPECT_EQ(inMarker.getEndSeqno(), outMarker.getEndSeqno())
            << inMarker.to_json();

    if (isFlagSet(inMarker.getFlags(), DcpSnapshotMarkerFlag::Disk)) {
        EXPECT_EQ(inMarker.getHighCompletedSeqno().value_or(0),
                  outMarker.getHighCompletedSeqno())
                << inMarker.to_json();
    } else {
        EXPECT_EQ(std::nullopt, outMarker.getHighCompletedSeqno())
                << inMarker.to_json();
    }

    if (version == DcpSnapshotMarkerV2xVersion::Two) {
        EXPECT_EQ(inMarker.getHighPreparedSeqno().value_or(0),
                  outMarker.getHighPreparedSeqno())
                << inMarker.to_json();
    } else {
        EXPECT_EQ(std::nullopt, outMarker.getHighPreparedSeqno())
                << inMarker.to_json();
    }

    if (version == DcpSnapshotMarkerV2xVersion::Two) {
        EXPECT_EQ(inMarker.getPurgeSeqno().value_or(0),
                  outMarker.getPurgeSeqno())
                << inMarker.to_json();
    } else {
        EXPECT_EQ(std::nullopt, outMarker.getPurgeSeqno())
                << inMarker.to_json();
    }
}

TEST_P(DcpSnapshotMarkerCodecTest, RoundtripV2_0_MVSOnly) {
    using namespace cb::mcbp;
    test(DcpSnapshotMarker(start, end, {}, {}, {}, mvs, {}));
    test(DcpSnapshotMarker(start, end, flags, {}, {}, mvs, {}));
}

TEST_P(DcpSnapshotMarkerCodecTest, RoundtripV2_0_HCSOnly) {
    using namespace cb::mcbp;
    test(DcpSnapshotMarker(start, end, {}, hcs, {}, {}, {}));
    test(DcpSnapshotMarker(start, end, flags, hcs, {}, {}, {}));
}

TEST_P(DcpSnapshotMarkerCodecTest, RoundtripV2_0_HCSAndMVS) {
    using namespace cb::mcbp;
    test(DcpSnapshotMarker(start, end, {}, hcs, {}, mvs, {}));
    test(DcpSnapshotMarker(start, end, flags, hcs, {}, mvs, {}));
}

TEST_P(DcpSnapshotMarkerCodecTest, RoundtripV2_2_HPSOnly) {
    using namespace cb::mcbp;
    test(DcpSnapshotMarker(start, end, {}, hcs, hps, mvs, {}));
    test(DcpSnapshotMarker(start, end, flags, hcs, hps, mvs, {}));
}

TEST_P(DcpSnapshotMarkerCodecTest, RoundtripV2_2_PurgeOnly) {
    using namespace cb::mcbp;
    test(DcpSnapshotMarker(start, end, {}, hcs, {}, mvs, purge));
    test(DcpSnapshotMarker(start, end, flags, hcs, hps, mvs, {}));
}

TEST_P(DcpSnapshotMarkerCodecTest, RoundtripV2_2_All) {
    using namespace cb::mcbp;
    test(DcpSnapshotMarker(start, end, {}, hcs, hps, mvs, purge));
    test(DcpSnapshotMarker(start, end, flags, hcs, hps, mvs, purge));
}

/**
 * Test class for DcpMutation validation - the bool parameter toggles
 * collections on/off (as that subtly changes the encoding of a mutation)
 */
class DcpMutationValidatorTest : public ::testing::WithParamInterface<bool>,
                                 public ValidatorTest {
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
                cb::mcbp::ClientOpcode::DcpMutation, blob, expectedStatus);
    }
};

TEST_P(DcpMutationValidatorTest, CorrectMessage) {
    EXPECT_EQ("Attached bucket does not support DCP",
              validate_error_context(cb::mcbp::Status::NotSupported));
}

TEST_P(DcpMutationValidatorTest, InvalidExtlen) {
    request.setExtlen(21);
    request.setBodylen(23);
    EXPECT_EQ("Request must include extras of length 31",
              validate_error_context());
}

TEST_P(DcpMutationValidatorTest, InvalidKeylen) {
    request.setKeylen(0);
    request.setBodylen(31);
    EXPECT_EQ("Request must include key", validate_error_context());
}

// A key which has no leb128 stop-byte
TEST_P(DcpMutationValidatorTest, InvalidKey1) {
    if (isCollectionsEnabled()) {
        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)}, true);
        uint8_t key[10] = {};
        std::fill_n(key, 10, 0x81ull);
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
                                       blob);
    }

    cb::mcbp::RequestBuilder builder;
    cb::mcbp::Request& header;
};

TEST_P(DcpDeletionValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpDeletionValidatorTest, ValidDatatype) {
    using cb::mcbp::Datatype;
    constexpr std::array<uint8_t, 4> datatypes = {
            {uint8_t(Datatype::Raw),
             uint8_t(Datatype::Raw) | uint8_t(Datatype::Snappy),
             uint8_t(Datatype::Xattr),
             uint8_t(Datatype::Xattr) | uint8_t(Datatype::Snappy)}};
    for (auto valid : datatypes) {
        header.setDatatype(valid);

        std::string_view value = "My value"sv;
        cb::xattr::Blob blob;
        cb::compression::Buffer deflated;

        if (cb::mcbp::datatype::is_xattr(valid)) {
            blob.set("_foo"sv, R"({"bar":5})"sv);
            value = blob.finalize();
        }

        if (cb::mcbp::datatype::is_snappy(valid)) {
            ASSERT_TRUE(deflateSnappy(value, deflated));
            value = deflated;
        }

        builder.setValue(value);
        EXPECT_EQ(cb::mcbp::Status::NotSupported, validate())
                << "Testing valid datatype: "
                << cb::mcbp::datatype::to_string(
                           protocol_binary_datatype_t(valid));
    }
}

TEST_P(DcpDeletionValidatorTest, InvalidDatatype) {
    using cb::mcbp::Datatype;
    constexpr std::array<uint8_t, 2> datatypes = {
            {uint8_t(Datatype::JSON),
             uint8_t(Datatype::Snappy) | uint8_t(Datatype::JSON)}};

    for (auto invalid : datatypes) {
        header.setDatatype(invalid);

        std::string_view value = R"({"foo":"bar"})"sv;
        cb::compression::Buffer deflated;

        if (cb::mcbp::datatype::is_snappy(invalid)) {
            ASSERT_TRUE(deflateSnappy(value, deflated));
            value = deflated;
        }
        builder.setValue(value);

        EXPECT_EQ(cb::mcbp::Status::Einval, validate())
                << "Testing invalid datatype: "
                << cb::mcbp::datatype::to_string(
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
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DcpExpiration,
                                       blob);
    }
};

TEST_P(DcpExpirationValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpExpirationValidatorTest, InvalidExtlen) {
    request.setExtlen(5);
    request.setBodylen(7);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpExpirationValidatorTest, InvalidKeylen) {
    request.setKeylen(GetParam() ? 1 : 0);
    request.setBodylen(19);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpExpirationValidatorTest, WithValue) {
    request.setBodylen(100);
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
        request.setMagic(cb::mcbp::Magic::ClientRequest);
        request.setExtlen(1);
        request.setBodylen(1);
        request.setDatatype(cb::mcbp::Datatype::Raw);

        cb::mcbp::RequestBuilder builder({blob, sizeof(blob)}, true);
        cb::mcbp::request::DcpSetVBucketState extras;
        extras.setState(1);
        builder.setExtras(extras.getBuffer());
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::DcpSetVbucketState, &request);
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
    request.setExtlen(5);
    request.setBodylen(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpSetVbucketStateValidatorTest, InvalidKeylen) {
    request.setKeylen(4);
    request.setBodylen(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpSetVbucketStateValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpSetVbucketStateValidatorTest, InvalidBody) {
    request.setBodylen(12);
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
                                       &request);
    }
};

TEST_P(DcpNoopValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpNoopValidatorTest, InvalidExtlen) {
    request.setExtlen(5);
    request.setBodylen(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpNoopValidatorTest, InvalidKeylen) {
    request.setKeylen(4);
    request.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpNoopValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpNoopValidatorTest, InvalidBody) {
    request.setBodylen(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class DcpBufferAckValidatorTest : public ::testing::WithParamInterface<bool>,
                                  public ValidatorTest {
public:
    DcpBufferAckValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.setExtlen(4);
        request.setBodylen(4);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::DcpBufferAcknowledgement, &request);
    }
};

TEST_P(DcpBufferAckValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpBufferAckValidatorTest, InvalidExtlen) {
    request.setExtlen(5);
    request.setBodylen(5);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpBufferAckValidatorTest, InvalidKeylen) {
    request.setKeylen(4);
    request.setBodylen(8);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpBufferAckValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpBufferAckValidatorTest, InvalidBody) {
    request.setBodylen(12);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

class DcpControlValidatorTest : public ::testing::WithParamInterface<bool>,
                                public ValidatorTest {
public:
    DcpControlValidatorTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        request.setKeylen(4);
        request.setBodylen(8);
    }

protected:
    cb::mcbp::Status validate() {
        return ValidatorTest::validate(cb::mcbp::ClientOpcode::DcpControl,
                                       &request);
    }
};

TEST_P(DcpControlValidatorTest, CorrectMessage) {
    EXPECT_EQ(cb::mcbp::Status::NotSupported, validate());
}

TEST_P(DcpControlValidatorTest, InvalidExtlen) {
    request.setExtlen(5);
    request.setBodylen(13);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpControlValidatorTest, InvalidKeylen) {
    request.setKeylen(0);
    request.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpControlValidatorTest, InvalidDatatype) {
    request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

TEST_P(DcpControlValidatorTest, InvalidBody) {
    request.setBodylen(4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
}

/**
 * Test class for DcpDeletion validation - the bool parameter toggles
 * collections on/off (as that subtly changes the encoding of a deletion)
 */
class DcpXattrValidatorTest : public ::testing::WithParamInterface<
                                      std::tuple<cb::mcbp::ClientOpcode, bool>>,
                              public ValidatorTest {
public:
    DcpXattrValidatorTest()
        : ValidatorTest(true), // collections always on
          builder({scratch.data(), scratch.size()}),
          header(*reinterpret_cast<cb::mcbp::Request*>(scratch.data())) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();
        connection.enableDatatype(cb::mcbp::Feature::XATTR);
        connection.enableDatatype(cb::mcbp::Feature::SNAPPY);

        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(std::get<0>(GetParam()));
        builder.setDatatype(cb::mcbp::Datatype(getDatatype()));

        std::array<uint8_t, 2> key = {0, 'a'};
        builder.setKey({key.data(), key.size()});

        if (std::get<0>(GetParam()) == cb::mcbp::ClientOpcode::DcpMutation) {
            cb::mcbp::request::DcpMutationPayload extras;
            extras.setBySeqno(1);
            builder.setExtras(extras.getBuffer());
        } else if (std::get<0>(GetParam()) ==
                   cb::mcbp::ClientOpcode::DcpDeletion) {
            cb::mcbp::request::DcpDeletionV2Payload extras(0, 0, 0);
            builder.setExtras(extras.getBuffer());
        } else if (std::get<0>(GetParam()) ==
                   cb::mcbp::ClientOpcode::DcpExpiration) {
            cb::mcbp::request::DcpExpirationPayload extras(0, 0, 0);
            builder.setExtras(extras.getBuffer());
        } else {
            FAIL() << "Invalid opcode";
        }
    }

    bool isSnappyEnabled() const {
        return std::get<1>(GetParam());
    }

    uint8_t getDatatype() const {
        using cb::mcbp::Datatype;
        if (isSnappyEnabled()) {
            return uint8_t(Datatype::Xattr) | uint8_t(Datatype::Snappy);
        }
        return uint8_t(Datatype::Xattr);
    }

    static std::string PrintToStringParamName(
            const testing::TestParamInfo<
                    std::tuple<cb::mcbp::ClientOpcode, bool>>& info) {
        return fmt::format("{}_snappy_is_{}",
                           std::get<0>(info.param),
                           std::get<1>(info.param));
    }

protected:
    std::array<uint8_t, 1024 * 1024 * 2> scratch;
    cb::mcbp::RequestBuilder builder;
    cb::mcbp::Request& header;
};

TEST_P(DcpXattrValidatorTest, SystemXattrTooLarge) {
    std::string_view value;
    cb::xattr::Blob blob;
    cb::compression::Buffer deflated;

    // Generate an xattr blob that is illegal (system data exceeds 1MiB limit)
    int keyIndex{0};
    // use a larger value to reduce iterations
    std::string longValue = "{\"" + std::string(2048, 'c') + "\"}";
    while (blob.size() <= cb::limits::PrivilegedBytes + 1) {
        std::string key = "_foo_" + std::to_string(++keyIndex);
        blob.set(key, longValue);
    }

    if (isSnappyEnabled()) {
        ASSERT_TRUE(deflateSnappy(value, deflated));
        value = deflated;
    } else {
        value = blob.finalize();
    }
    builder.setValue(value);
    EXPECT_EQ("The provided xattr segment is not valid",
              validate_error_context(std::get<0>(GetParam()),
                                     scratch.data(),
                                     cb::mcbp::Status::XattrEinval));
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
INSTANTIATE_TEST_SUITE_P(DiskSnapshotOnOff,
                         DcpSnapshotMarkerCodecTest,
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
INSTANTIATE_TEST_SUITE_P(
        SnappyOnOff,
        DcpXattrValidatorTest,
        ::testing::Combine(
                ::testing::Values(cb::mcbp::ClientOpcode::DcpMutation,
                                  cb::mcbp::ClientOpcode::DcpDeletion,
                                  cb::mcbp::ClientOpcode::DcpExpiration),
                ::testing::Bool()),
        DcpXattrValidatorTest::PrintToStringParamName);
} // namespace mcbp::test
