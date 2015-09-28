/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#include "utilities/subdoc_encoder.h"
#include "utilities/protocol2text.h"

#include <memcached/protocol_binary.h>
#include <algorithm>
#include <vector>

/*
 * Sub-document API validator tests
 */

namespace BinaryProtocolValidator {

// Single-path subdocument API commands
class SubdocSingleTest : public ValidatorTest {
    virtual void SetUp() override {
        ValidatorTest::SetUp();
        memset(&request, 0, sizeof(request));
        request.message.header.request.magic = PROTOCOL_BINARY_REQ;
        request.message.header.request.extlen = 3;
        request.message.header.request.keylen = htons(10);
        request.message.header.request.bodylen = htonl(/*keylen*/10 +
                                                       /*extlen*/3 +
                                                       /*pathlen*/1);
        request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
        request.message.extras.pathlen = htons(1);
    }

protected:
    int validate(uint8_t opcode) {
        return ValidatorTest::validate(opcode, static_cast<void*>(&request));
    }
    protocol_binary_request_subdocument request;
};

TEST_F(SubdocSingleTest, Get_Baseline) {
    // Ensure that the initial request as formed by SetUp is valid.
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_SUBDOC_GET));
}

TEST_F(SubdocSingleTest, Get_InvalidBody) {
    // Need a non-zero body.
    request.message.header.request.bodylen = htonl(0);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SUBDOC_GET));
}

TEST_F(SubdocSingleTest, Get_InvalidPath) {
    // Need a non-zero path.
    request.message.header.request.bodylen = htonl(/*keylen*/10 + /*extlen*/3);
    request.message.extras.pathlen = htons(0);

    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SUBDOC_GET));
}

TEST_F(SubdocSingleTest, DictAdd_InvalidValue) {
    // Need a non-zero value.
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD));
}

TEST_F(SubdocSingleTest, DictAdd_InvalidExtras) {
    // Extras can only be '3' for lookups, may also be '7' for mutations
    // (specifying expiration).
    request.message.header.request.extlen = 4;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD));

    request.message.header.request.extlen = 7;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              validate(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD));

    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL,
              validate(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS));
}

class SubdocMultiLookupTest : public ValidatorTest {
    virtual void SetUp() override {
        ValidatorTest::SetUp();

        // Setup basic, correct header.
        request.key = "multi_lookup";
        request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
                                 SUBDOC_FLAG_NONE, "[0]"});
    }

protected:
    int validate(const std::vector<char>& request) {
        void* packet = const_cast<void*>
            (static_cast<const void*>(request.data()));
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_SUBDOC_MULTI_LOOKUP,
                                       packet);
    }

    SubdocMultiLookupCmd request;
};

TEST_F(SubdocMultiLookupTest, Baseline) {
    // Ensure that the initial request as formed by SetUp is valid.
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));
}

TEST_F(SubdocMultiLookupTest, InvalidMagic) {
    std::vector<char> payload = request.encode();
    auto* header = reinterpret_cast<protocol_binary_request_header*>
        (payload.data());
    header->request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(payload));
}

TEST_F(SubdocMultiLookupTest, InvalidDatatype) {
    std::vector<char> payload = request.encode();
    auto* header = reinterpret_cast<protocol_binary_request_header*>
        (payload.data());
    header->request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(payload));
    header->request.datatype = PROTOCOL_BINARY_DATATYPE_COMPRESSED_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(payload));
    header->request.datatype = PROTOCOL_BINARY_DATATYPE_COMPRESSED;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(payload));
}

TEST_F(SubdocMultiLookupTest, InvalidKey) {
    request.key = "";
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));
}

TEST_F(SubdocMultiLookupTest, InvalidExtras) {
    std::vector<char> payload = request.encode();
    auto* header = reinterpret_cast<protocol_binary_request_header*>
        (payload.data());
    header->request.extlen = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(payload));

    // extlen of 4 permitted for mutations only.
    header->request.extlen = 4;
    header->request.bodylen = htonl(ntohl(header->request.bodylen) + 4);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(payload));
}


TEST_F(SubdocMultiLookupTest, NumPaths) {
    // Need at least one path.
    request.specs.clear();
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_COMBO,
              validate(request.encode()));

    // Should handle total of 16 paths.
    request.specs.clear();
    // Add maximum number of paths.
    SubdocMultiLookupCmd::LookupSpec spec{PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
                                          SUBDOC_FLAG_NONE,
                                          "[0]"};
    for (unsigned int i = 0; i<16; i++) {
        request.specs.push_back(spec);
    }
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));

    // Add one more - should now fail.
    request.specs.push_back(spec);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_COMBO,
              validate(request.encode()));
}

TEST_F(SubdocMultiLookupTest, ValidLocationOpcodes) {
    // Check that GET is supported.
    request.specs.clear();
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_GET,
                             SUBDOC_FLAG_NONE,
                             "[0]"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));
}

TEST_F(SubdocMultiLookupTest, InvalidLocationOpcodes) {
    // Check that all opcodes apart from the two lookup ones are not supported.

    for (uint8_t ii = 0; ii < std::numeric_limits<uint8_t>::max(); ii++) {
        auto cmd = protocol_binary_command(ii);
        // Skip over lookup opcodes
        if ((cmd == PROTOCOL_BINARY_CMD_SUBDOC_GET) ||
            (cmd == PROTOCOL_BINARY_CMD_SUBDOC_EXISTS)) {
            continue;
        }
        request.specs.at(0) = {cmd, SUBDOC_FLAG_NONE, "[0]"};
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_COMBO,
                  validate(request.encode()))
            << "Failed for cmd:" << memcached_opcode_2_text(ii);
    }
}

TEST_F(SubdocMultiLookupTest, InvalidLocationPaths) {
    // Path must not be zero length.
    request.specs.at(0).path.clear();
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));

    // Maximum length should be accepted...
    request.specs.at(0).path.assign(1024, 'x');
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));

    // But any longer should be rejected.
    request.specs.at(0).path.push_back('x');
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));
}

TEST_F(SubdocMultiLookupTest, InvalidLocationFlags) {
    // Both GET and EXISTS do not accept any flags.
    request.specs.at(0).opcode = PROTOCOL_BINARY_CMD_SUBDOC_EXISTS;
    request.specs.at(0).flags = SUBDOC_FLAG_MKDIR_P;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));

    request.specs.at(0).opcode = PROTOCOL_BINARY_CMD_SUBDOC_GET;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));
}

/*** MULTI_MUTATION **********************************************************/

class SubdocMultiMutationTest : public ValidatorTest {
    virtual void SetUp() override {
        ValidatorTest::SetUp();

        // Setup basic, correct header.
        request.key = "multi_mutation";
        request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                                 protocol_binary_subdoc_flag(0), "key", "value"});
    }

protected:
    int validate(const std::vector<char>& request) {
        void* packet = const_cast<void*>
            (static_cast<const void*>(request.data()));
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION,
                                       packet);
    }

    SubdocMultiMutationCmd request;
};

TEST_F(SubdocMultiMutationTest, Baseline) {
    // Ensure that the initial request as formed by SetUp is valid.
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));
}

TEST_F(SubdocMultiMutationTest, InvalidMagic) {
    std::vector<char> payload = request.encode();
    auto* header = reinterpret_cast<protocol_binary_request_header*>
        (payload.data());
    header->request.magic = 0;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(payload));
}

TEST_F(SubdocMultiMutationTest, InvalidDatatype) {
    std::vector<char> payload = request.encode();
    auto* header = reinterpret_cast<protocol_binary_request_header*>
        (payload.data());
    header->request.datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(payload));
    header->request.datatype = PROTOCOL_BINARY_DATATYPE_COMPRESSED_JSON;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(payload));
    header->request.datatype = PROTOCOL_BINARY_DATATYPE_COMPRESSED;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(payload));
}

TEST_F(SubdocMultiMutationTest, InvalidKey) {
    request.key = "";
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));
}

TEST_F(SubdocMultiMutationTest, InvalidExtras) {
    std::vector<char> payload = request.encode();
    auto* header = reinterpret_cast<protocol_binary_request_header*>
        (payload.data());
    header->request.extlen = 1;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(payload));
}

TEST_F(SubdocMultiMutationTest, Expiry) {
    // extlen of 4 permitted for mutations.
    request.expiry = 10;
    std::vector<char> payload = request.encode();

    // Check that we encoded correctly.
    auto* header = reinterpret_cast<protocol_binary_request_header*>
        (payload.data());
    ASSERT_EQ(4, header->request.extlen);

    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(payload));
}

TEST_F(SubdocMultiMutationTest, ExplicitZeroExpiry) {
    // extlen of 4 permitted for mutations.
    request.expiry = 0;
    request.encode_zero_expiry_on_wire = 1;
    std::vector<char> payload = request.encode();

    auto* header = reinterpret_cast<protocol_binary_request_header*>
        (payload.data());
    ASSERT_EQ(4, header->request.extlen);

    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(payload));
}

TEST_F(SubdocMultiMutationTest, NumPaths) {
    // Need at least one path.
    request.specs.clear();
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_COMBO,
              validate(request.encode()));

    // Should handle total of 16 paths.
    request.specs.clear();
    // Add maximum number of paths.
    SubdocMultiMutationCmd::LookupSpec spec{PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                                            protocol_binary_subdoc_flag(0),
                                            "", "0"};
    for (unsigned int i = 0; i<16; i++) {
        request.specs.push_back(spec);
    }
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));

    // Add one more - should now fail.
    request.specs.push_back(spec);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_COMBO,
              validate(request.encode()));
}

TEST_F(SubdocMultiMutationTest, ValidDictAdd) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                             protocol_binary_subdoc_flag(0),
                             "path", "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));

    request.specs.at(1).flags = SUBDOC_FLAG_MKDIR_P;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));
}

TEST_F(SubdocMultiMutationTest, InvalidDictAdd) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                             protocol_binary_subdoc_flag(2),
                             "path", "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));

    // Must have path.
    request.specs.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                           protocol_binary_subdoc_flag(0),
                           "", ""};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));

    // Must have value.
    request.specs.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                           protocol_binary_subdoc_flag(0),
                           "path", ""};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));
}

TEST_F(SubdocMultiMutationTest, ValidDictUpsert) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                             protocol_binary_subdoc_flag(0),
                             "path", "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));

    request.specs.at(1).flags = SUBDOC_FLAG_MKDIR_P;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));
}

TEST_F(SubdocMultiMutationTest, InvalidDictUpsert) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                             protocol_binary_subdoc_flag(2),
                             "path", "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));

    // Must have path.
    request.specs.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                           protocol_binary_subdoc_flag(0),
                           "", ""};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));

    // Must have value.
    request.specs.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                           protocol_binary_subdoc_flag(0),
                           "path", ""};
}

TEST_F(SubdocMultiMutationTest, ValidDelete) {
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DELETE,
                             protocol_binary_subdoc_flag(0),
                             "path", ""});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));
}

TEST_F(SubdocMultiMutationTest, InvalidDelete) {
    // Shouldn't have value.
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_DELETE,
                             protocol_binary_subdoc_flag(0),
                             "path", "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));

    // Shouldn't have flags.
    request.specs.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_DELETE,
                           SUBDOC_FLAG_MKDIR_P,
                           "path", ""};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));

    // Must have path.
    request.specs.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_DELETE,
                           protocol_binary_subdoc_flag(0),
                           "", ""};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));
}

TEST_F(SubdocMultiMutationTest, ValidReplace) {
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                             protocol_binary_subdoc_flag(0),
                             "path", "new_value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));
}

TEST_F(SubdocMultiMutationTest, InvalidReplace) {
    // Must have path.
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                             protocol_binary_subdoc_flag(0),
                             "", "new_value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));

    // Must have value
    request.specs.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                           protocol_binary_subdoc_flag(0),
                           "path", ""};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));

    // Shouldn't have flags.
    request.specs.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                           SUBDOC_FLAG_MKDIR_P,
                           "path", "new_value"};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));
}

TEST_F(SubdocMultiMutationTest, ValidArrayPushLast) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                             protocol_binary_subdoc_flag(0),
                             "path", "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));

    request.specs.at(1).flags = SUBDOC_FLAG_MKDIR_P;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));

    // Allowed empty path.
    request.specs.at(1).path.clear();
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));
}

TEST_F(SubdocMultiMutationTest, InvalidArrayPushLast) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                             protocol_binary_subdoc_flag(2),
                             "", "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));

    // Must have value
    request.specs.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
                           protocol_binary_subdoc_flag(0),
                           "", ""};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));
}

TEST_F(SubdocMultiMutationTest, ValidArrayPushFirst) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                             protocol_binary_subdoc_flag(0),
                             "path", "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));

    request.specs.at(1).flags = SUBDOC_FLAG_MKDIR_P;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));

    // Allowed empty path.
    request.specs.at(1).path.clear();
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));
}

TEST_F(SubdocMultiMutationTest, InvalidArrayPushFirst) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                             protocol_binary_subdoc_flag(2),
                             "", "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));

    // Must have value
    request.specs.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
                           protocol_binary_subdoc_flag(0),
                           "", ""};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));
}

TEST_F(SubdocMultiMutationTest, ValidArrayInsert) {
    // Only allowed empty flags.
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                             protocol_binary_subdoc_flag(0),
                             "path", "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));
}

TEST_F(SubdocMultiMutationTest, InvalidArrayInsert) {
    // Only allowed empty flags.
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                             SUBDOC_FLAG_MKDIR_P,
                             "path", "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));

    // Must have path
    request.specs.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                           protocol_binary_subdoc_flag(0),
                           "", "value"};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));

    // Must have value
    request.specs.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
                           protocol_binary_subdoc_flag(0),
                           "path", ""};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));
}

TEST_F(SubdocMultiMutationTest, ValidArrayAddUnique) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                             protocol_binary_subdoc_flag(0),
                             "path", "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));

    request.specs.at(1).flags = SUBDOC_FLAG_MKDIR_P;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));

    // Allowed empty path.
    request.specs.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                           protocol_binary_subdoc_flag(0),
                           "", "value"};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));
}

TEST_F(SubdocMultiMutationTest, InvalidArrayAddUnique) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                             protocol_binary_subdoc_flag(2),
                             "path", "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));

    // Must have value
    request.specs.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
                           protocol_binary_subdoc_flag(0),
                           "path", ""};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));
}

TEST_F(SubdocMultiMutationTest, ValidArrayCounter) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                             protocol_binary_subdoc_flag(0),
                             "path", "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));

    request.specs.at(1).flags = SUBDOC_FLAG_MKDIR_P;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));

    // Allowed empty path.
    request.specs.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                           protocol_binary_subdoc_flag(0),
                           "", "value"};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate(request.encode()));
}

TEST_F(SubdocMultiMutationTest, InvalidArrayCounter) {
    // Only allowed empty flags or SUBDOC_FLAG_MKDIR_P (0x1)
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                             protocol_binary_subdoc_flag(2),
                             "path", "value"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));

    // Must have value
    request.specs.at(1) = {PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
                           protocol_binary_subdoc_flag(0),
                           "path", ""};
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate(request.encode()));
}

TEST_F(SubdocMultiMutationTest, InvalidLocationOpcodes) {
    // Check that all opcodes apart from the mutation ones are not supported.
    for (uint8_t ii = 0; ii < std::numeric_limits<uint8_t>::max(); ii++) {
        auto cmd = protocol_binary_command(ii);
        // Skip over mutation opcodes.
        switch (cmd) {
        case PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD:
        case PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT:
        case PROTOCOL_BINARY_CMD_SUBDOC_DELETE:
        case PROTOCOL_BINARY_CMD_SUBDOC_REPLACE:
        case PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST:
        case PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST:
        case PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT:
        case PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE:
        case PROTOCOL_BINARY_CMD_SUBDOC_COUNTER:
            continue;
        default:
            break;
        }

        request.specs.at(0) = {cmd, protocol_binary_subdoc_flag(0), "[0]"};
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_COMBO,
                  validate(request.encode()))
            << "Failed for cmd:" << memcached_opcode_2_text(ii);
    }
}

} // namespace BinaryProtocolValidator
