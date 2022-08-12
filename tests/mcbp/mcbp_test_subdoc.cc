/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "mcbp_test.h"
#include "protocol/connection/client_mcbp_commands.h"

#include <memcached/protocol_binary.h>
#include <algorithm>
#include <vector>

/*
 * Sub-document API validator tests
 */

namespace mcbp::test {

// Single-path subdocument API commands
class SubdocSingleTest : public ::testing::WithParamInterface<bool>,
                         public ValidatorTest {
public:
    SubdocSingleTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.setMagic(cb::mcbp::Magic::ClientRequest);
        request.message.header.request.setExtlen(3);
        request.message.header.request.setKeylen(10);
        request.message.header.request.setBodylen(/*keylen*/ 10 +
                                                  /*extlen*/ 3 +
                                                  /*pathlen*/ 1);
        request.message.header.request.setDatatype(cb::mcbp::Datatype::Raw);
        request.message.extras.pathlen = htons(1);
    }

protected:
    cb::mcbp::Status validate(cb::mcbp::ClientOpcode opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }

    std::string validate_error_context(
            cb::mcbp::ClientOpcode opcode,
            cb::mcbp::Status expectedStatus = cb::mcbp::Status::Einval) {
        return ValidatorTest::validate_error_context(
                opcode, static_cast<void*>(&request), expectedStatus);
    }

    protocol_binary_request_subdocument &request =
        *reinterpret_cast<protocol_binary_request_subdocument*>(blob);
};

TEST_P(SubdocSingleTest, Get_Baseline) {
    // Ensure that the initial request as formed by SetUp is valid.
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::SubdocGet));
    // Successful request should have empty error context message
    EXPECT_EQ("",
              validate_error_context(cb::mcbp::ClientOpcode::SubdocGet,
                                     cb::mcbp::Status::Success));
}

TEST_P(SubdocSingleTest, Get_InvalidBody) {
    // Need a non-zero body.
    request.message.header.request.setExtlen(0);
    request.message.header.request.setBodylen(
            request.message.header.request.getKeylen());
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::SubdocGet));
    EXPECT_EQ("Invalid extras section",
              validate_error_context(cb::mcbp::ClientOpcode::SubdocGet));
}

TEST_P(SubdocSingleTest, Get_InvalidPath) {
    // Need a non-zero path.
    request.message.header.request.setBodylen(/*keylen*/ 10 + /*extlen*/ 3);
    request.message.extras.pathlen = htons(0);

    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::SubdocGet));
    EXPECT_EQ("Request must include path",
              validate_error_context(cb::mcbp::ClientOpcode::SubdocGet));
}

TEST_P(SubdocSingleTest, DictAdd_InvalidValue) {
    // Need a non-zero value.
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::SubdocDictAdd));
    EXPECT_EQ("Request must include value",
              validate_error_context(cb::mcbp::ClientOpcode::SubdocDictAdd));
}

TEST_P(SubdocSingleTest, DictAdd_InvalidExtras) {
    // Extlen can be 3, 4, 7 or 8
    request.message.header.request.setExtlen(5);
    request.message.header.request.setBodylen(100);
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::SubdocDictAdd));
    EXPECT_EQ("Invalid extras section",
              validate_error_context(cb::mcbp::ClientOpcode::SubdocDictAdd));

    request.message.header.request.setExtlen(7);
    EXPECT_EQ(cb::mcbp::Status::Success,
              validate(cb::mcbp::ClientOpcode::SubdocDictAdd));
    EXPECT_EQ("",
              validate_error_context(cb::mcbp::ClientOpcode::SubdocDictAdd,
                                     cb::mcbp::Status::Success));

    request.message.header.request.setBodylen(10 + 7 + 1);
    EXPECT_EQ(cb::mcbp::Status::Einval,
              validate(cb::mcbp::ClientOpcode::SubdocExists));
    EXPECT_EQ("Request extras invalid",
              validate_error_context(cb::mcbp::ClientOpcode::SubdocExists));
}

class SubdocMultiLookupTest : public ::testing::WithParamInterface<bool>,
                              public ValidatorTest {
public:
    SubdocMultiLookupTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();

        // Setup basic, correct header.
        request.setKey("multi_lookup");
        request.addLookup({cb::mcbp::ClientOpcode::SubdocExists,
                           SUBDOC_FLAG_NONE,
                           "[0]"});
    }

protected:
    cb::mcbp::Status validate(const std::vector<uint8_t> request) {
        void* packet =
            const_cast<void*>(static_cast<const void*>(request.data()));
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::SubdocMultiLookup, packet);
    }

    cb::mcbp::Status validate(const BinprotSubdocMultiLookupCommand& cmd) {
        std::vector<uint8_t> packet;
        cmd.encode(packet);
        return validate(packet);
    }

    std::string validate_error_context(
            const std::vector<uint8_t> request,
            cb::mcbp::Status expectedStatus = cb::mcbp::Status::Einval) {
        void* packet =
                const_cast<void*>(static_cast<const void*>(request.data()));
        return ValidatorTest::validate_error_context(
                cb::mcbp::ClientOpcode::SubdocMultiLookup,
                packet,
                expectedStatus);
    }

    std::string validate_error_context(
            const BinprotSubdocMultiLookupCommand& cmd,
            cb::mcbp::Status expectedStatus = cb::mcbp::Status::Einval) {
        std::vector<uint8_t> packet;
        cmd.encode(packet);
        return validate_error_context(packet, expectedStatus);
    }

    BinprotSubdocMultiLookupCommand request;
};

TEST_P(SubdocMultiLookupTest, Baseline) {
    // Ensure that the initial request as formed by SetUp is valid.
    EXPECT_EQ(cb::mcbp::Status::Success, validate(request));
    EXPECT_EQ("", validate_error_context(request, cb::mcbp::Status::Success));
}

TEST_P(SubdocMultiLookupTest, InvalidDatatype) {
    std::vector<uint8_t> payload;
    request.encode(payload);
    auto* header =
        reinterpret_cast<protocol_binary_request_header*>(payload.data());
    header->request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(payload));
    EXPECT_EQ("Request datatype invalid", validate_error_context(payload));
    header->request.setDatatype(cb::mcbp::Datatype(
            PROTOCOL_BINARY_DATATYPE_SNAPPY | PROTOCOL_BINARY_DATATYPE_JSON));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(payload));
    EXPECT_EQ("Request datatype invalid", validate_error_context(payload));
    header->request.setDatatype(cb::mcbp::Datatype::Snappy);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(payload));
    EXPECT_EQ("Request datatype invalid", validate_error_context(payload));
}

TEST_P(SubdocMultiLookupTest, InvalidKey) {
    request.setKey("");
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request must include key", validate_error_context(request));
}

TEST_P(SubdocMultiLookupTest, InvalidExtras) {
    std::vector<uint8_t> payload;
    request.encode(payload);

    // add backing space for the extras
    payload.resize(payload.size() + 4);

    // Lookups accept extlen of 0 or 1
    auto* header =
        reinterpret_cast<protocol_binary_request_header*>(payload.data());
    header->request.setExtlen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(payload));
    EXPECT_EQ("Request extras invalid", validate_error_context(payload));

    // extlen of 4 permitted for mutations only.
    header->request.setExtlen(4);
    header->request.setBodylen(header->request.getBodylen() + 4);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(payload));
    EXPECT_EQ("Request extras invalid", validate_error_context(payload));
}

TEST_P(SubdocMultiLookupTest, NumPaths) {
    // Need at least one path.
    request.clearLookups();
    EXPECT_EQ(cb::mcbp::Status::SubdocInvalidCombo, validate(request));
    EXPECT_EQ("Request must contain at least one path",
              validate_error_context(request,
                                     cb::mcbp::Status::SubdocInvalidCombo));

    // Should handle total of 16 paths.
    request.clearLookups();
    // Add maximum number of paths.
    BinprotSubdocMultiLookupCommand::LookupSpecifier spec{
            cb::mcbp::ClientOpcode::SubdocExists, SUBDOC_FLAG_NONE, "[0]"};
    for (unsigned int i = 0; i < 16; i++) {
        request.addLookup(spec);
    }
    EXPECT_EQ(cb::mcbp::Status::Success, validate(request));
    EXPECT_EQ("", validate_error_context(request, cb::mcbp::Status::Success));

    // Add one more - should now fail.
    request.addLookup(spec);
    EXPECT_EQ(cb::mcbp::Status::SubdocInvalidCombo, validate(request));
    EXPECT_EQ("Request must contain at most 16 paths",
              validate_error_context(request,
                                     cb::mcbp::Status::SubdocInvalidCombo));
}

TEST_P(SubdocMultiLookupTest, ValidLocationOpcodes) {
    // Check that GET is supported.
    request.clearLookups();
    request.addLookup(
            {cb::mcbp::ClientOpcode::SubdocGet, SUBDOC_FLAG_NONE, "[0]"});
    EXPECT_EQ(cb::mcbp::Status::Success, validate(request));
    EXPECT_EQ("", validate_error_context(request, cb::mcbp::Status::Success));
}

TEST_P(SubdocMultiLookupTest, InvalidLocationOpcodes) {
    // Check that all opcodes apart from the two lookup ones are not supported.

    for (uint8_t ii = 0; ii < std::numeric_limits<uint8_t>::max(); ii++) {
        auto cmd = cb::mcbp::ClientOpcode(ii);
        // Skip over lookup opcodes
        if ((cmd == cb::mcbp::ClientOpcode::Get) ||
            (cmd == cb::mcbp::ClientOpcode::SubdocGet) ||
            (cmd == cb::mcbp::ClientOpcode::SubdocExists) ||
            (cmd == cb::mcbp::ClientOpcode::SubdocGetCount)) {
            continue;
        }
        request.at(0) = {cmd, SUBDOC_FLAG_NONE, "[0]"};
        EXPECT_EQ(cb::mcbp::Status::SubdocInvalidCombo, validate(request))
                << "Failed for cmd:" << ::to_string(cb::mcbp::ClientOpcode(ii));
    }
}

TEST_P(SubdocMultiLookupTest, InvalidLocationPaths) {
    // Path must not be zero length.
    request.at(0).path.clear();
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request must include path", validate_error_context(request));

    // Maximum length should be accepted...
    request.at(0).path.assign(1024, 'x');
    EXPECT_EQ(cb::mcbp::Status::Success, validate(request));
    EXPECT_EQ("", validate_error_context(request, cb::mcbp::Status::Success));

    // But any longer should be rejected.
    request.at(0).path.push_back('x');
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request path length exceeds maximum",
              validate_error_context(request));
}

TEST_P(SubdocMultiLookupTest, InvalidLocationFlags) {
    // Both GET and EXISTS do not accept any flags.
    for (auto& opcode : {cb::mcbp::ClientOpcode::SubdocExists,
                         cb::mcbp::ClientOpcode::SubdocGet}) {
        request.at(0).opcode = opcode;
        request.at(0).flags = SUBDOC_FLAG_MKDIR_P;
        EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
        EXPECT_EQ("Request flags invalid", validate_error_context(request));
        request.at(0).flags = SUBDOC_FLAG_NONE;

        request.addDocFlag(cb::mcbp::subdoc::doc_flag::Mkdoc);
        EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
        EXPECT_EQ("Request document flags invalid",
                  validate_error_context(request));
        request.clearDocFlags();

        request.addDocFlag(cb::mcbp::subdoc::doc_flag::Add);
        EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
        EXPECT_EQ("Request document flags invalid",
                  validate_error_context(request));

        request.addDocFlag(cb::mcbp::subdoc::doc_flag::Mkdoc);
        EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
        EXPECT_EQ("Request must not contain both add and mkdoc flags",
                  validate_error_context(request));
        request.clearDocFlags();
    }
}

/*** MULTI_MUTATION **********************************************************/

class SubdocMultiMutationTest : public ::testing::WithParamInterface<bool>,
                                public ValidatorTest {
public:
    SubdocMultiMutationTest() : ValidatorTest(GetParam()) {
    }
    void SetUp() override {
        ValidatorTest::SetUp();

        // Setup basic, correct header.
        request.setKey("multi_mutation");
        request.addMutation({cb::mcbp::ClientOpcode::SubdocDictAdd,
                             protocol_binary_subdoc_flag(0),
                             "key",
                             "value"});
    }

protected:
    cb::mcbp::Status validate(const BinprotSubdocMultiMutationCommand& cmd) {
        std::vector<uint8_t> packet;
        cmd.encode(packet);
        return validate(packet);
    }

    cb::mcbp::Status validate(std::vector<uint8_t>& packet) {
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::SubdocMultiMutation, packet.data());
    }

    std::string validate_error_context(
            const BinprotSubdocMultiMutationCommand& cmd,
            cb::mcbp::Status expectedStatus = cb::mcbp::Status::Einval) {
        std::vector<uint8_t> packet;
        cmd.encode(packet);
        return validate_error_context(packet, expectedStatus);
    }

    std::string validate_error_context(
            std::vector<uint8_t>& packet,
            cb::mcbp::Status expectedStatus = cb::mcbp::Status::Einval) {
        return ValidatorTest::validate_error_context(
                cb::mcbp::ClientOpcode::SubdocMultiMutation,
                packet.data(),
                expectedStatus);
    }

    /*
     * Tests the request with both the pathFlag and docFlag specified. It does
     * not test when both are used together
     */
    void testFlags(protocol_binary_subdoc_flag pathFlag,
                   cb::mcbp::subdoc::doc_flag docFlag,
                   cb::mcbp::Status expected,
                   uint8_t spec) {
        request.at(spec).flags = pathFlag;
        EXPECT_EQ(expected, validate(request));
        if (expected == cb::mcbp::Status::Einval) {
            EXPECT_EQ("Request flags invalid", validate_error_context(request));
        } else if (expected == cb::mcbp::Status::Success) {
            EXPECT_EQ(
                    "",
                    validate_error_context(request, cb::mcbp::Status::Success));
        }
        request.at(spec).flags = SUBDOC_FLAG_NONE;

        request.addDocFlag(docFlag);
        EXPECT_EQ(expected, validate(request));
        if (expected == cb::mcbp::Status::Einval) {
            EXPECT_EQ("Request document flags invalid",
                      validate_error_context(request));
        } else if (expected == cb::mcbp::Status::Success) {
            EXPECT_EQ(
                    "",
                    validate_error_context(request, cb::mcbp::Status::Success));
        }
        request.clearDocFlags();
    }

    /*
     * Tests the request with both the pathFlag and docFlag specified. It does
     * not test when both are used together
     */
    void testFlagCombo(protocol_binary_subdoc_flag pathFlag,
                       cb::mcbp::subdoc::doc_flag docFlag,
                       cb::mcbp::Status expected,
                       uint8_t spec) {
        request.at(spec).flags = pathFlag;
        EXPECT_EQ(expected, validate(request));
        request.addDocFlag(docFlag);
        EXPECT_EQ(expected, validate(request));
        request.at(spec).flags = SUBDOC_FLAG_NONE;
        EXPECT_EQ(expected, validate(request));
    }

    BinprotSubdocMultiMutationCommand request;
};

TEST_P(SubdocMultiMutationTest, Baseline) {
    // Ensure that the initial request as formed by SetUp is valid.
    EXPECT_EQ(cb::mcbp::Status::Success, validate(request));
    EXPECT_EQ("", validate_error_context(request, cb::mcbp::Status::Success));
}

TEST_P(SubdocMultiMutationTest, InvalidDatatype) {
    std::vector<uint8_t> payload;
    request.encode(payload);
    auto* header =
        reinterpret_cast<protocol_binary_request_header*>(payload.data());
    header->request.setDatatype(cb::mcbp::Datatype::JSON);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(payload));
    EXPECT_EQ("Request datatype invalid", validate_error_context(payload));
    header->request.setDatatype(cb::mcbp::Datatype(
            PROTOCOL_BINARY_DATATYPE_SNAPPY | PROTOCOL_BINARY_DATATYPE_JSON));
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(payload));
    EXPECT_EQ("Request datatype invalid", validate_error_context(payload));
    header->request.setDatatype(cb::mcbp::Datatype::Snappy);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(payload));
    EXPECT_EQ("Request datatype invalid", validate_error_context(payload));
}

TEST_P(SubdocMultiMutationTest, InvalidKey) {
    request.setKey("");
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request must include key", validate_error_context(request));
}

TEST_P(SubdocMultiMutationTest, InvalidExtras) {
    std::vector<uint8_t> payload;
    request.encode(payload);
    auto* header =
        reinterpret_cast<protocol_binary_request_header*>(payload.data());
    header->request.setExtlen(2);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(payload));
    EXPECT_EQ("Request extras invalid", validate_error_context(payload));
}

TEST_P(SubdocMultiMutationTest, Expiry) {
    // extlen of 4 permitted for mutations.
    request.setExpiry(10);
    std::vector<uint8_t> payload;
    request.encode(payload);

    // Check that we encoded correctly.
    auto* header =
        reinterpret_cast<protocol_binary_request_header*>(payload.data());
    ASSERT_EQ(4, header->request.getExtlen());

    EXPECT_EQ(cb::mcbp::Status::Success, validate(payload));
    EXPECT_EQ("", validate_error_context(payload, cb::mcbp::Status::Success));
}

TEST_P(SubdocMultiMutationTest, ExplicitZeroExpiry) {
    // extlen of 4 permitted for mutations.
    request.setExpiry(0);
    std::vector<uint8_t> payload;
    request.encode(payload);

    auto* header =
        reinterpret_cast<protocol_binary_request_header*>(payload.data());
    ASSERT_EQ(4, header->request.getExtlen());

    EXPECT_EQ(cb::mcbp::Status::Success, validate(payload));
    EXPECT_EQ("", validate_error_context(payload, cb::mcbp::Status::Success));
}

TEST_P(SubdocMultiMutationTest, NumPaths) {
    // Need at least one path.
    request.clearMutations();
    EXPECT_EQ(cb::mcbp::Status::SubdocInvalidCombo, validate(request));
    EXPECT_EQ("Request must contain at least one path",
              validate_error_context(request,
                                     cb::mcbp::Status::SubdocInvalidCombo));
    // Should handle total of 16 paths.
    request.clearMutations();
    // Add maximum number of paths.
    BinprotSubdocMultiMutationCommand::MutationSpecifier spec{
            cb::mcbp::ClientOpcode::SubdocArrayPushLast,
            protocol_binary_subdoc_flag(0),
            "",
            "0"};
    for (unsigned int i = 0; i < 16; i++) {
        request.addMutation(spec);
    }
    EXPECT_EQ(cb::mcbp::Status::Success, validate(request));
    EXPECT_EQ("", validate_error_context(request, cb::mcbp::Status::Success));

    // Add one more - should now fail.
    request.addMutation(spec);
    EXPECT_EQ(cb::mcbp::Status::SubdocInvalidCombo, validate(request));
    EXPECT_EQ("Request must contain at most 16 paths",
              validate_error_context(request,
                                     cb::mcbp::Status::SubdocInvalidCombo));
}

TEST_P(SubdocMultiMutationTest, ValidDictAdd) {
    // Only allowed empty flags or
    // SUBDOC_FLAG_MKDIR_P/mcbp::subdoc::doc_flag::Mkdoc
    request.addMutation({cb::mcbp::ClientOpcode::SubdocDictAdd,
                         protocol_binary_subdoc_flag(0),
                         "path",
                         "value"});
    EXPECT_EQ(cb::mcbp::Status::Success, validate(request));
    EXPECT_EQ("", validate_error_context(request, cb::mcbp::Status::Success));
    testFlagCombo(SUBDOC_FLAG_MKDIR_P,
                  cb::mcbp::subdoc::doc_flag::Mkdoc,
                  cb::mcbp::Status::Success,
                  1);
}

TEST_P(SubdocMultiMutationTest, InvalidDictAdd) {
    request.addMutation({cb::mcbp::ClientOpcode::SubdocDictAdd,
                         protocol_binary_subdoc_flag(0xff),
                         "path",
                         "value"});
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request flags invalid", validate_error_context(request));

    // Must have path.
    request.at(1) = {cb::mcbp::ClientOpcode::SubdocDictAdd,
                     protocol_binary_subdoc_flag(0),
                     "",
                     ""};
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request must include path", validate_error_context(request));

    // Must have value.
    request.at(1) = {cb::mcbp::ClientOpcode::SubdocDictAdd,
                     protocol_binary_subdoc_flag(0),
                     "path",
                     ""};
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request must include value", validate_error_context(request));
}

TEST_P(SubdocMultiMutationTest, ValidDictUpsert) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P(0x01)/MKDOC(0x02)
    request.addMutation({cb::mcbp::ClientOpcode::SubdocDictUpsert,
                         protocol_binary_subdoc_flag(0),
                         "path",
                         "value"});
    EXPECT_EQ(cb::mcbp::Status::Success, validate(request));
    EXPECT_EQ("", validate_error_context(request, cb::mcbp::Status::Success));
    testFlagCombo(SUBDOC_FLAG_MKDIR_P,
                  cb::mcbp::subdoc::doc_flag::Mkdoc,
                  cb::mcbp::Status::Success,
                  1);
}

TEST_P(SubdocMultiMutationTest, InvalidDictUpsert) {
    // Only allowed empty flags SUBDOC_FLAG_{MKDIR_P (0x1), MKDOC (0x2)}
    request.addMutation({cb::mcbp::ClientOpcode::SubdocDictUpsert,
                         protocol_binary_subdoc_flag(0xff),
                         "path",
                         "value"});
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request flags invalid", validate_error_context(request));

    // Must have path.
    request.at(1) = {cb::mcbp::ClientOpcode::SubdocDictUpsert,
                     protocol_binary_subdoc_flag(0),
                     "",
                     ""};
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request must include path", validate_error_context(request));

    // Must have value.
    request.at(1) = {cb::mcbp::ClientOpcode::SubdocDictUpsert,
                     protocol_binary_subdoc_flag(0),
                     "path",
                     ""};

    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request must include value", validate_error_context(request));
}

TEST_P(SubdocMultiMutationTest, ValidDelete) {
    request.addMutation({cb::mcbp::ClientOpcode::SubdocDelete,
                         protocol_binary_subdoc_flag(0),
                         "path",
                         ""});
    EXPECT_EQ(cb::mcbp::Status::Success, validate(request));
    EXPECT_EQ("", validate_error_context(request, cb::mcbp::Status::Success));
}

TEST_P(SubdocMultiMutationTest, InvalidDelete) {
    // Shouldn't have value.
    request.addMutation({cb::mcbp::ClientOpcode::SubdocDelete,
                         protocol_binary_subdoc_flag(0),
                         "path",
                         "value"});
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request must not include value",
              validate_error_context(request));

    // Shouldn't have flags.
    testFlags(SUBDOC_FLAG_MKDIR_P,
              cb::mcbp::subdoc::doc_flag::Mkdoc,
              cb::mcbp::Status::Einval,
              1);

    // Must have path.
    request.at(1) = {cb::mcbp::ClientOpcode::SubdocDelete,
                     protocol_binary_subdoc_flag(0),
                     "",
                     ""};
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request must include path", validate_error_context(request));
}

TEST_P(SubdocMultiMutationTest, ValidReplace) {
    request.addMutation({cb::mcbp::ClientOpcode::SubdocReplace,
                         protocol_binary_subdoc_flag(0),
                         "path",
                         "new_value"});
    EXPECT_EQ(cb::mcbp::Status::Success, validate(request));
    EXPECT_EQ("", validate_error_context(request, cb::mcbp::Status::Success));
}

TEST_P(SubdocMultiMutationTest, InvalidReplace) {
    // Must have path.
    request.addMutation({cb::mcbp::ClientOpcode::SubdocReplace,
                         protocol_binary_subdoc_flag(0),
                         "",
                         "new_value"});
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request must include path", validate_error_context(request));

    // Must have value
    request.at(1) = {cb::mcbp::ClientOpcode::SubdocReplace,
                     protocol_binary_subdoc_flag(0),
                     "path",
                     ""};
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request must include value", validate_error_context(request));

    // Shouldn't have flags.
    request.at(1) = {cb::mcbp::ClientOpcode::SubdocReplace,
                     SUBDOC_FLAG_NONE,
                     "path",
                     "new_value"};
    testFlags(SUBDOC_FLAG_MKDIR_P,
              cb::mcbp::subdoc::doc_flag::Mkdoc,
              cb::mcbp::Status::Einval,
              1);
}

TEST_P(SubdocMultiMutationTest, ValidArrayPushLast) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.addMutation({cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                         protocol_binary_subdoc_flag(0),
                         "path",
                         "value"});
    EXPECT_EQ(cb::mcbp::Status::Success, validate(request));
    EXPECT_EQ("", validate_error_context(request, cb::mcbp::Status::Success));

    testFlagCombo(SUBDOC_FLAG_MKDIR_P,
                  cb::mcbp::subdoc::doc_flag::Mkdoc,
                  cb::mcbp::Status::Success,
                  1);
    // Allowed empty path.
    request.at(1).path.clear();
    testFlagCombo(SUBDOC_FLAG_MKDIR_P,
                  cb::mcbp::subdoc::doc_flag::Mkdoc,
                  cb::mcbp::Status::Success,
                  1);
}

TEST_P(SubdocMultiMutationTest, InvalidArrayPushLast) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.addMutation({cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                         protocol_binary_subdoc_flag(0xff),
                         "",
                         "value"});
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request flags invalid", validate_error_context(request));

    // Must have value
    request.at(1) = {cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                     protocol_binary_subdoc_flag(0),
                     "",
                     ""};
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request must include value", validate_error_context(request));
}

TEST_P(SubdocMultiMutationTest, ValidArrayPushFirst) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.addMutation({cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                         protocol_binary_subdoc_flag(0),
                         "path",
                         "value"});
    EXPECT_EQ(cb::mcbp::Status::Success, validate(request));
    EXPECT_EQ("", validate_error_context(request, cb::mcbp::Status::Success));

    testFlagCombo(SUBDOC_FLAG_MKDIR_P,
                  cb::mcbp::subdoc::doc_flag::Mkdoc,
                  cb::mcbp::Status::Success,
                  1);
    // Allowed empty path.
    request.at(1).path.clear();
    testFlagCombo(SUBDOC_FLAG_MKDIR_P,
                  cb::mcbp::subdoc::doc_flag::Mkdoc,
                  cb::mcbp::Status::Success,
                  1);
}

TEST_P(SubdocMultiMutationTest, InvalidArrayPushFirst) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.addMutation({cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                         protocol_binary_subdoc_flag(0xff),
                         "",
                         "value"});
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request flags invalid", validate_error_context(request));

    // Must have value
    request.at(1) = {cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                     protocol_binary_subdoc_flag(0),
                     "",
                     ""};
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request must include value", validate_error_context(request));
}

TEST_P(SubdocMultiMutationTest, ValidArrayInsert) {
    // Only allowed empty flags.
    request.addMutation({cb::mcbp::ClientOpcode::SubdocArrayInsert,
                         protocol_binary_subdoc_flag(0),
                         "path",
                         "value"});
    EXPECT_EQ(cb::mcbp::Status::Success, validate(request));
    EXPECT_EQ("", validate_error_context(request, cb::mcbp::Status::Success));
}

TEST_P(SubdocMultiMutationTest, InvalidArrayInsert) {
    // Only allowed empty flags.
    for (size_t i = 0; i < 2; i++) {
        request.addMutation({});
    }

    request.at(1) = {cb::mcbp::ClientOpcode::SubdocArrayInsert,
                     SUBDOC_FLAG_NONE,
                     "path",
                     "value"};
    testFlags(SUBDOC_FLAG_MKDIR_P,
              cb::mcbp::subdoc::doc_flag::Mkdoc,
              cb::mcbp::Status::Einval,
              1);

    // Must have path
    request.at(1) = {cb::mcbp::ClientOpcode::SubdocArrayInsert,
                     protocol_binary_subdoc_flag(0),
                     "",
                     "value"};
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request must include path", validate_error_context(request));

    // Must have value
    request.at(1) = {cb::mcbp::ClientOpcode::SubdocArrayInsert,
                     protocol_binary_subdoc_flag(0),
                     "path",
                     ""};
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request must include value", validate_error_context(request));
}

TEST_P(SubdocMultiMutationTest, ValidArrayAddUnique) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.addMutation({cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                         protocol_binary_subdoc_flag(0),
                         "path",
                         "value"});
    EXPECT_EQ(cb::mcbp::Status::Success, validate(request));
    EXPECT_EQ("", validate_error_context(request, cb::mcbp::Status::Success));

    testFlags(SUBDOC_FLAG_MKDIR_P,
              cb::mcbp::subdoc::doc_flag::Mkdoc,
              cb::mcbp::Status::Success,
              1);

    // Allowed empty path.
    request.at(1) = {cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                     protocol_binary_subdoc_flag(0),
                     "",
                     "value"};
    EXPECT_EQ(cb::mcbp::Status::Success, validate(request));
    EXPECT_EQ("", validate_error_context(request, cb::mcbp::Status::Success));
}

TEST_P(SubdocMultiMutationTest, InvalidArrayAddUnique) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.addMutation({cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                         protocol_binary_subdoc_flag(0xff),
                         "path",
                         "value"});
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request flags invalid", validate_error_context(request));

    // Must have value
    request.at(1) = {cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                     protocol_binary_subdoc_flag(0),
                     "path",
                     ""};
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request must include value", validate_error_context(request));
}

TEST_P(SubdocMultiMutationTest, ValidArrayCounter) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.addMutation({cb::mcbp::ClientOpcode::SubdocCounter,
                         protocol_binary_subdoc_flag(0),
                         "path",
                         "value"});
    EXPECT_EQ(cb::mcbp::Status::Success, validate(request));
    EXPECT_EQ("", validate_error_context(request, cb::mcbp::Status::Success));

    testFlags(SUBDOC_FLAG_MKDIR_P,
              cb::mcbp::subdoc::doc_flag::Mkdoc,
              cb::mcbp::Status::Success,
              1);

    // Empty path invalid
    request.at(1) = {cb::mcbp::ClientOpcode::SubdocCounter,
                     protocol_binary_subdoc_flag(0),
                     "",
                     "value"};
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request must include path", validate_error_context(request));
}

TEST_P(SubdocMultiMutationTest, InvalidArrayCounter) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.addMutation({cb::mcbp::ClientOpcode::SubdocCounter,
                         protocol_binary_subdoc_flag(0xff),
                         "path",
                         "value"});
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request flags invalid", validate_error_context(request));

    // Must have value
    request.at(1) = {cb::mcbp::ClientOpcode::SubdocCounter,
                     protocol_binary_subdoc_flag(0),
                     "path",
                     ""};
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request must include value", validate_error_context(request));
}

TEST_P(SubdocMultiMutationTest, InvalidLocationOpcodes) {
    // Check that all opcodes apart from the mutation ones are not supported.
    for (uint8_t ii = 0; ii < std::numeric_limits<uint8_t>::max(); ii++) {
        auto cmd = cb::mcbp::ClientOpcode(ii);
        // Skip over mutation opcodes.
        switch (cmd) {
        case cb::mcbp::ClientOpcode::Set:
        case cb::mcbp::ClientOpcode::Delete:
        case cb::mcbp::ClientOpcode::SubdocDictAdd:
        case cb::mcbp::ClientOpcode::SubdocDictUpsert:
        case cb::mcbp::ClientOpcode::SubdocDelete:
        case cb::mcbp::ClientOpcode::SubdocReplace:
        case cb::mcbp::ClientOpcode::SubdocArrayPushLast:
        case cb::mcbp::ClientOpcode::SubdocArrayPushFirst:
        case cb::mcbp::ClientOpcode::SubdocArrayInsert:
        case cb::mcbp::ClientOpcode::SubdocArrayAddUnique:
        case cb::mcbp::ClientOpcode::SubdocCounter:
        case cb::mcbp::ClientOpcode::SubdocReplaceBodyWithXattr:
            continue;
        default:
            break;
        }

        request.at(0) = {cmd, protocol_binary_subdoc_flag(0), "[0]", {}};
        EXPECT_EQ(cb::mcbp::Status::SubdocInvalidCombo, validate(request))
                << "Failed for cmd:" << ::to_string(cb::mcbp::ClientOpcode(ii));
    }
}

TEST_P(SubdocMultiMutationTest, InvalidCas) {
    // Check that a non 0 CAS is rejected
    request.addMutation({cb::mcbp::ClientOpcode::SubdocDictUpsert,
                         protocol_binary_subdoc_flag(0),
                         "path",
                         "value"});
    request.setCas(12234);
    request.addDocFlag(cb::mcbp::subdoc::doc_flag::Add);
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request with add flag must have CAS 0",
              validate_error_context(request));
}

TEST_P(SubdocMultiMutationTest, WholeDocDeleteInvalidValue) {
    request.clearMutations();
    // Shouldn't have value.
    request.addMutation({cb::mcbp::ClientOpcode::Delete,
                         protocol_binary_subdoc_flag(0),
                         "",
                         "value"});
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request must not include value",
              validate_error_context(request));
}

TEST_P(SubdocMultiMutationTest, WholeDocDeleteInvalidPath) {
    request.clearMutations();
    // Must not have path.
    request.addMutation({cb::mcbp::ClientOpcode::Delete,
                         protocol_binary_subdoc_flag(0),
                         "_sync",
                         ""});
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request must not include path for wholedoc operations",
              validate_error_context(request));
}

TEST_P(SubdocMultiMutationTest, WholeDocDeleteInvalidXattrFlag) {
    request.clearMutations();
    // Can't use CMD_DELETE to delete Xattr
    request.addMutation(
            {cb::mcbp::ClientOpcode::Delete, SUBDOC_FLAG_XATTR_PATH, "", ""});
    EXPECT_EQ(cb::mcbp::Status::Einval, validate(request));
    EXPECT_EQ("Request flags invalid", validate_error_context(request));
}

TEST_P(SubdocMultiMutationTest, ValidWholeDocDeleteFlags) {
    request.clearMutations();
    request.addMutation({cb::mcbp::ClientOpcode::Delete,
                         protocol_binary_subdoc_flag(0),
                         "",
                         ""});
    request.addDocFlag(cb::mcbp::subdoc::doc_flag::AccessDeleted);
    EXPECT_EQ(cb::mcbp::Status::Success, validate(request));
    EXPECT_EQ("", validate_error_context(request, cb::mcbp::Status::Success));
}

TEST_P(SubdocMultiMutationTest, InvalidWholeDocDeleteMulti) {
    // Doing a delete and another subdoc/wholedoc command on the body in
    // the same multi mutation is invalid
    // Note that the setup of this test adds an initial mutation
    request.addMutation({cb::mcbp::ClientOpcode::Delete,
                         protocol_binary_subdoc_flag(0),
                         "",
                         ""});
    EXPECT_EQ(cb::mcbp::Status::SubdocInvalidCombo, validate(request));
    EXPECT_EQ("Request contains an invalid combination of commands",
              validate_error_context(request,
                                     cb::mcbp::Status::SubdocInvalidCombo));

    // Now try the delete first
    request.clearMutations();
    request.addMutation({cb::mcbp::ClientOpcode::Delete,
                         protocol_binary_subdoc_flag(0),
                         "",
                         ""});
    request.addMutation({cb::mcbp::ClientOpcode::SubdocDictAdd,
                         protocol_binary_subdoc_flag(0),
                         "key",
                         "value"});
    EXPECT_EQ(cb::mcbp::Status::SubdocInvalidCombo, validate(request));
    EXPECT_EQ("Request contains an invalid combination of commands",
              validate_error_context(request,
                                     cb::mcbp::Status::SubdocInvalidCombo));
}

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         SubdocSingleTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         SubdocMultiLookupTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         SubdocMultiMutationTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

} // namespace mcbp::test
