/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include <include/memcached/protocol_binary.h>

namespace BinaryProtocolValidator {

enum class SubdocOpcodes : uint8_t {
    Get = PROTOCOL_BINARY_CMD_SUBDOC_GET,
    Exists = PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
    DictAdd = PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
    Upsert = PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
    Delete = PROTOCOL_BINARY_CMD_SUBDOC_DELETE,
    Replace = PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
    PushLast = PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
    PushFirst = PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
    Insert = PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
    AddUnique = PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
    Counter = PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
    MultiLookup = PROTOCOL_BINARY_CMD_SUBDOC_MULTI_LOOKUP,
    MultiMutation = PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION,
    GetCount = PROTOCOL_BINARY_CMD_SUBDOC_GET_COUNT
};

std::string to_string(const SubdocOpcodes& opcode) {
#ifdef JETBRAINS_CLION_IDE
    // CLion don't properly parse the output when the
    // output gets written as the string instead of the
    // number. This makes it harder to debug the tests
    // so let's just disable it while we're waiting
    // for them to supply a fix.
    // See https://youtrack.jetbrains.com/issue/CPP-6039
    return std::to_string(static_cast<int>(opcode));
#else
    return memcached_opcode_2_text(uint8_t(opcode));
#endif
}

std::ostream& operator<<(std::ostream& os, const SubdocOpcodes& o) {
    os << to_string(o);
    return os;
};

/**
 * Test the extra checks needed for XATTR access in subdoc
 */
class SubdocXattrSingleTest : public ValidatorTest,
                              public ::testing::WithParamInterface<SubdocOpcodes> {
public:
    SubdocXattrSingleTest()
        : doc("Document"),
          path("_sync.cas"),
          value("\"${Mutation.CAS}\""),
          flags(SUBDOC_FLAG_XATTR_PATH) {}

    virtual void SetUp() override {
        ValidatorTest::SetUp();

        if (!needPayload()) {
            value.clear();
        }
    }

protected:
    int validate() {
        auto opcode = (protocol_binary_command)GetParam();
        protocol_binary_request_subdocument* req;
        std::vector<uint8_t> blob(
            sizeof(req->bytes) + doc.length() + path.length() + value.length());
        req = reinterpret_cast<protocol_binary_request_subdocument*>(blob.data());

        req->message.header.request.magic = PROTOCOL_BINARY_REQ;
        req->message.header.request.extlen = 3;
        req->message.header.request.keylen = ntohs(doc.length());
        req->message.header.request.bodylen = ntohl(
            3 + doc.length() + path.length() + value.length());
        req->message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;

        req->message.extras.subdoc_flags = flags;
        req->message.extras.pathlen = ntohs(path.length());

        auto* ptr = blob.data() + sizeof(req->bytes);
        memcpy(ptr, doc.data(), doc.length());
        memcpy(ptr + doc.length(), path.data(), path.length());
        memcpy(ptr + doc.length() + path.length(), value.data(),
               value.length());

        return ValidatorTest::validate(opcode, static_cast<void*>(blob.data()));
    }

    bool needPayload() {
        switch (GetParam()) {
        case SubdocOpcodes::Get:
        case SubdocOpcodes::Exists:
        case SubdocOpcodes::GetCount:
        case SubdocOpcodes::Delete:
            return false;

        case SubdocOpcodes::MultiMutation:
        case SubdocOpcodes::MultiLookup:
            throw std::logic_error("allowMacroExpansion: Multi* is not valid");

        case SubdocOpcodes::Counter:
        case SubdocOpcodes::AddUnique:
        case SubdocOpcodes::Insert:
        case SubdocOpcodes::PushFirst:
        case SubdocOpcodes::PushLast:
        case SubdocOpcodes::Replace:
        case SubdocOpcodes::Upsert:
        case SubdocOpcodes::DictAdd:
            return true;
        }
    }

    bool allowMacroExpansion() {
        switch (GetParam()) {
        case SubdocOpcodes::Get:
        case SubdocOpcodes::Exists:
        case SubdocOpcodes::GetCount:
        case SubdocOpcodes::Delete:
        case SubdocOpcodes::Counter:
        case SubdocOpcodes::AddUnique:
            return false;

        case SubdocOpcodes::MultiMutation:
        case SubdocOpcodes::MultiLookup:
            throw std::logic_error("allowMacroExpansion: Multi* is not valid");

        case SubdocOpcodes::Insert:
        case SubdocOpcodes::PushFirst:
        case SubdocOpcodes::PushLast:
        case SubdocOpcodes::Replace:
        case SubdocOpcodes::Upsert:
        case SubdocOpcodes::DictAdd:
            return true;
        }
        throw std::logic_error("allowMacroExpansion: Unknown parameter");
    }

    const std::string doc;
    std::string path;
    std::string value;
    uint8_t flags;
};

INSTANTIATE_TEST_CASE_P(SubdocOpcodes,
                        SubdocXattrSingleTest,
                        ::testing::Values(SubdocOpcodes::Get,
                                          SubdocOpcodes::Exists,
                                          SubdocOpcodes::DictAdd,
                                          SubdocOpcodes::Upsert,
                                          SubdocOpcodes::Delete,
                                          SubdocOpcodes::Replace,
                                          SubdocOpcodes::PushLast,
                                          SubdocOpcodes::PushFirst,
                                          SubdocOpcodes::Insert,
                                          SubdocOpcodes::AddUnique,
                                          SubdocOpcodes::Counter,
                                          SubdocOpcodes::GetCount),
                        ::testing::PrintToStringParamName());

TEST_P(SubdocXattrSingleTest, PathTest) {
    path = "superduperlongpath";
    flags = SUBDOC_FLAG_NONE;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate())
                << memcached_opcode_2_text(uint8_t(GetParam()));

    // XATTR keys must be < 16 characters (we've got standalone tests
    // to validate all of the checks for the xattr keys, this is just
    // to make sure that our validator calls it ;-)
    flags = SUBDOC_FLAG_XATTR_PATH;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate())
                << memcached_opcode_2_text(uint8_t(GetParam()));

    // Truncate it to a shorter one, and this time it should pass
    path = "_sync.cas";
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate())
                << memcached_opcode_2_text(uint8_t(GetParam()));
}

TEST_P(SubdocXattrSingleTest, ValidateFlags) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());

    // Access Deleted should fail without XATTR_PATH
    flags = SUBDOC_FLAG_ACCESS_DELETED;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());

    flags |= SUBDOC_FLAG_XATTR_PATH;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());

    flags |= SUBDOC_FLAG_EXPAND_MACROS;
    if (allowMacroExpansion()) {
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());

        // but it should fail if we don't have the XATTR_PATH
        flags = SUBDOC_FLAG_EXPAND_MACROS;
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());

        // And it should also fail if we have Illegal macros
        flags |= SUBDOC_FLAG_XATTR_PATH;
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
        value = "${UnknownMacro}";
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
    } else {
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
    }
}

/**
 * The SubdocXattrMultiLookupTest tests the XATTR specific constraints
 * over the normal subdoc constraints tested elsewhere
 */
class SubdocXattrMultiLookupTest : public ValidatorTest {
public:

    SubdocXattrMultiLookupTest() {
        request.key = "Document";
    }

    virtual void SetUp() override {
        ValidatorTest::SetUp();
    }

protected:
    int validate() {
        auto packet = request.encode();
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_SUBDOC_MULTI_LOOKUP,
                                       static_cast<void*>(packet.data()));
    }

    SubdocMultiLookupCmd request;
};

TEST_F(SubdocXattrMultiLookupTest, XAttrMayBeFirst) {
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
                             SUBDOC_FLAG_XATTR_PATH,
                             "_sync.cas"});
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
                             SUBDOC_FLAG_NONE,
                             "meta.author"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(SubdocXattrMultiLookupTest, XAttrCantBeLast) {
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
                             SUBDOC_FLAG_NONE,
                             "meta.author"});
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
                             SUBDOC_FLAG_XATTR_PATH,
                             "_sync.cas"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(SubdocXattrMultiLookupTest, XAttrKeyIsChecked) {
    // We got other unit tests that tests all of the different restrictions
    // for the subdoc key.. just make sure that it is actually called..
    // Check that we can't insert a key > 16 chars
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
                             SUBDOC_FLAG_XATTR_PATH,
                             "ThisIsASuperDuperLongPath"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(SubdocXattrMultiLookupTest, XattrFlagsMakeSense) {
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
                             SUBDOC_FLAG_XATTR_PATH,
                             "_sync.cas"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());

    // We shouldn't be allowed to expand macros for a lookup command
    request.specs[0].flags = SUBDOC_FLAG_EXPAND_MACROS;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());

    // We shouldn't be allowed to expand macros for a lookup command
    // and the SUBDOC_FLAG_EXPAND_MACROS must have SUBDOC_FLAG_XATTR_PATH
    request.specs[0].flags = SUBDOC_FLAG_EXPAND_MACROS | SUBDOC_FLAG_XATTR_PATH;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());

    // Let's try an invalid access deleted flag (needs xattr path)
    request.specs[0].flags = SUBDOC_FLAG_ACCESS_DELETED;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());

    // We should be able to access deleted docs if both flags are set
    request.specs[0].flags = SUBDOC_FLAG_ACCESS_DELETED | SUBDOC_FLAG_XATTR_PATH;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(SubdocXattrMultiLookupTest, AllowMultipleLookups) {
    for (int ii = 0; ii < 10; ii++) {
        request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
                                 SUBDOC_FLAG_XATTR_PATH,
                                 "_sync.cas"});
    }
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(SubdocXattrMultiLookupTest, AllLookupsMustBeOnTheSamePath) {
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
                             SUBDOC_FLAG_XATTR_PATH,
                             "_sync.cas"});
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
                             SUBDOC_FLAG_XATTR_PATH,
                             "foo.bar"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}


/**
 * The SubdocXattrMultiLookupTest tests the XATTR specific constraints
 * over the normal subdoc constraints tested elsewhere
 */
class SubdocXattrMultiMutationTest : public ValidatorTest {
public:

    SubdocXattrMultiMutationTest() {
        request.key = "Document";
    }

    virtual void SetUp() override {
        ValidatorTest::SetUp();
    }

protected:
    int validate() {
        auto packet = request.encode();
        return ValidatorTest::validate(PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION,
                                       static_cast<void*>(packet.data()));
    }

    SubdocMultiMutationCmd request;
};

TEST_F(SubdocXattrMultiMutationTest, XAttrMayBeFirst) {
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                             SUBDOC_FLAG_XATTR_PATH,
                             "_sync.cas", "{\"foo\" : \"bar\"}"});
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                             SUBDOC_FLAG_NONE,
                             "meta.author", "{\"name\" : \"Bubba\"}"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(SubdocXattrMultiMutationTest, XAttrCantBeLast) {
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                             SUBDOC_FLAG_NONE,
                             "meta.author", "{\"name\" : \"Bubba\"}"});
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                             SUBDOC_FLAG_XATTR_PATH,
                             "_sync.cas", "{\"foo\" : \"bar\"}"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(SubdocXattrMultiMutationTest, XAttrKeyIsChecked) {
    // We got other unit tests that tests all of the different restrictions
    // for the subdoc key.. just make sure that it is actually called..
    // Check that we can't insert a key > 16 chars
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                             SUBDOC_FLAG_XATTR_PATH,
                             "ThisIsASuperDuperLongPath",
                             "{\"foo\" : \"bar\"}"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

TEST_F(SubdocXattrMultiMutationTest, XattrFlagsMakeSense) {
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                             SUBDOC_FLAG_XATTR_PATH,
                             "_sync.cas", "\"${Mutation.CAS}\""});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());

    request.specs[0].flags = SUBDOC_FLAG_EXPAND_MACROS;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());

    request.specs[0].flags = SUBDOC_FLAG_EXPAND_MACROS | SUBDOC_FLAG_XATTR_PATH;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());

    request.specs[0].flags = SUBDOC_FLAG_EXPAND_MACROS |
        SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_ACCESS_DELETED;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());

    request.specs[0].value = "${UnknownMacro}";
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
    request.specs[0].value = "\"${Mutation.CAS}\"";

    // Let's try an invalid access deleted flag (needs xattr path)
    request.specs[0].flags = SUBDOC_FLAG_ACCESS_DELETED;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());

    // We should be able to access deleted docs if both flags are set
    request.specs[0].flags = SUBDOC_FLAG_ACCESS_DELETED | SUBDOC_FLAG_XATTR_PATH;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(SubdocXattrMultiMutationTest, AllowMultipleMutations) {
    for (int ii = 0; ii < 10; ii++) {
        request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                                 SUBDOC_FLAG_XATTR_PATH,
                                 "_sync.cas", "{\"foo\" : \"bar\"}"});
    }
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, validate());
}

TEST_F(SubdocXattrMultiMutationTest, AllMutationsMustBeOnTheSamePath) {
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                             SUBDOC_FLAG_XATTR_PATH,
                             "_sync.cas", "{\"foo\" : \"bar\"}"});
    request.specs.push_back({PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                             SUBDOC_FLAG_XATTR_PATH,
                             "foo.bar", "{\"foo\" : \"bar\"}"});
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, validate());
}

} // namespace BinaryProtocolValidator
