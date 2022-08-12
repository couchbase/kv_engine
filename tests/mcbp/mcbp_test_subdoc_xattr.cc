/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "mcbp_test.h"
#include "protocol/connection/client_mcbp_commands.h"

#include <gsl/gsl-lite.hpp>
#include <memcached/protocol_binary.h>
#include <algorithm>
#include <vector>

namespace mcbp::test {

/**
 * Test the extra checks needed for XATTR access in subdoc
 */
class SubdocXattrSingleTest : public ::testing::WithParamInterface<
                                      std::tuple<cb::mcbp::ClientOpcode, bool>>,
                              public ValidatorTest {
public:
    SubdocXattrSingleTest()
        : ValidatorTest(std::get<1>(GetParam())),
          doc("Document"),
          path("_sync.cas"),
          value("\"${Mutation.CAS}\""),
          flags(SUBDOC_FLAG_XATTR_PATH),
          docFlags(cb::mcbp::subdoc::doc_flag::None) {
    }

    void SetUp() override {
        ValidatorTest::SetUp();

        if (!needPayload()) {
            value.clear();
        }
    }

protected:
    cb::mcbp::Status validate() {
        auto opcode = cb::mcbp::ClientOpcode(std::get<0>(GetParam()));
        protocol_binary_request_subdocument* req;
        const size_t extlen = !isNone(docFlags) ? 1 : 0;
        std::vector<uint8_t> blob(sizeof(req->bytes) + doc.length() +
                                  path.length() + value.length() + extlen);
        req = reinterpret_cast<protocol_binary_request_subdocument*>(
            blob.data());

        req->message.header.request.setMagic(cb::mcbp::Magic::ClientRequest);
        req->message.header.request.setExtlen(
                gsl::narrow_cast<uint8_t>(3 + extlen));
        req->message.header.request.setKeylen(
                gsl::narrow<uint16_t>(doc.length()));
        req->message.header.request.setBodylen(gsl::narrow<uint32_t>(
                3 + doc.length() + path.length() + value.length() + extlen));
        req->message.header.request.setDatatype(cb::mcbp::Datatype::Raw);

        req->message.extras.subdoc_flags = (flags);
        req->message.extras.pathlen =
                ntohs(gsl::narrow<uint16_t>(path.length()));

        auto* ptr = blob.data() + sizeof(req->bytes);
        if (extlen) {
            memcpy(ptr, &docFlags, sizeof(docFlags));
            ptr += sizeof(docFlags);
        }
        memcpy(ptr, doc.data(), doc.length());
        memcpy(ptr + doc.length(), path.data(), path.length());
        memcpy(ptr + doc.length() + path.length(),
               value.data(),
               value.length());

        return ValidatorTest::validate(opcode, static_cast<void*>(blob.data()));
    }

    bool needPayload() {
        switch (std::get<0>(GetParam())) {
        case cb::mcbp::ClientOpcode::SubdocGet:
        case cb::mcbp::ClientOpcode::SubdocExists:
        case cb::mcbp::ClientOpcode::SubdocGetCount:
        case cb::mcbp::ClientOpcode::SubdocDelete:
            return false;

        case cb::mcbp::ClientOpcode::SubdocMultiLookup:
        case cb::mcbp::ClientOpcode::SubdocMultiMutation:
            throw std::logic_error("needPayload: Multi* is not valid");

        case cb::mcbp::ClientOpcode::SubdocDictAdd:
        case cb::mcbp::ClientOpcode::SubdocDictUpsert:
        case cb::mcbp::ClientOpcode::SubdocReplace:
        case cb::mcbp::ClientOpcode::SubdocArrayPushLast:
        case cb::mcbp::ClientOpcode::SubdocArrayPushFirst:
        case cb::mcbp::ClientOpcode::SubdocArrayInsert:
        case cb::mcbp::ClientOpcode::SubdocArrayAddUnique:
        case cb::mcbp::ClientOpcode::SubdocCounter:
            return true;

        default:
            throw std::logic_error("needPayload: Unknown parameter");
        }
    }

    bool allowMacroExpansion() {
        switch (std::get<0>(GetParam())) {
        case cb::mcbp::ClientOpcode::SubdocGet:
        case cb::mcbp::ClientOpcode::SubdocExists:
        case cb::mcbp::ClientOpcode::SubdocGetCount:
        case cb::mcbp::ClientOpcode::SubdocDelete:
        case cb::mcbp::ClientOpcode::SubdocCounter:
        case cb::mcbp::ClientOpcode::SubdocArrayAddUnique:
            return false;

        case cb::mcbp::ClientOpcode::SubdocMultiLookup:
        case cb::mcbp::ClientOpcode::SubdocMultiMutation:
            throw std::logic_error("allowMacroExpansion: Multi* is not valid");

        case cb::mcbp::ClientOpcode::SubdocArrayInsert:
        case cb::mcbp::ClientOpcode::SubdocArrayPushLast:
        case cb::mcbp::ClientOpcode::SubdocArrayPushFirst:
        case cb::mcbp::ClientOpcode::SubdocReplace:
        case cb::mcbp::ClientOpcode::SubdocDictAdd:
        case cb::mcbp::ClientOpcode::SubdocDictUpsert:
            return true;
        default:
            throw std::logic_error("allowMacroExpansion: Unknown parameter");
        }
    }

    const std::string doc;
    std::string path;
    std::string value;
    uint8_t flags;
    cb::mcbp::subdoc::doc_flag docFlags;
};

INSTANTIATE_TEST_SUITE_P(
        SubdocOpcodes,
        SubdocXattrSingleTest,
        ::testing::Combine(
                ::testing::Values(cb::mcbp::ClientOpcode::SubdocGet,
                                  cb::mcbp::ClientOpcode::SubdocExists,
                                  cb::mcbp::ClientOpcode::SubdocDictAdd,
                                  cb::mcbp::ClientOpcode::SubdocDictUpsert,
                                  cb::mcbp::ClientOpcode::SubdocDelete,
                                  cb::mcbp::ClientOpcode::SubdocReplace,
                                  cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                                  cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                                  cb::mcbp::ClientOpcode::SubdocArrayInsert,
                                  cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                                  cb::mcbp::ClientOpcode::SubdocCounter,
                                  cb::mcbp::ClientOpcode::SubdocGetCount),
                ::testing::Bool()));

TEST_P(SubdocXattrSingleTest, PathTest) {
    path = "superduperlongpath";
    flags = SUBDOC_FLAG_NONE;
    EXPECT_EQ(cb::mcbp::Status::Success, validate())
            << ::to_string(cb::mcbp::ClientOpcode(std::get<0>(GetParam())));

    // XATTR keys must be < 16 characters (we've got standalone tests
    // to validate all of the checks for the xattr keys, this is just
    // to make sure that our validator calls it ;-)
    flags = SUBDOC_FLAG_XATTR_PATH;
    EXPECT_EQ(cb::mcbp::Status::XattrEinval, validate())
            << ::to_string(cb::mcbp::ClientOpcode(std::get<0>(GetParam())));

    // Truncate it to a shorter one, and this time it should pass
    path = "_sync.cas";
    EXPECT_EQ(cb::mcbp::Status::Success, validate())
            << ::to_string(cb::mcbp::ClientOpcode(std::get<0>(GetParam())));
}

TEST_P(SubdocXattrSingleTest, ValidateFlags) {
    EXPECT_EQ(cb::mcbp::Status::Success, validate());

    // Access Deleted should pass without XATTR flag
    flags = SUBDOC_FLAG_NONE;
    docFlags = cb::mcbp::subdoc::doc_flag::AccessDeleted;
    EXPECT_EQ(cb::mcbp::Status::Success, validate());

    flags |= SUBDOC_FLAG_XATTR_PATH;
    EXPECT_EQ(cb::mcbp::Status::Success, validate());

    // Check that Add & Mkdoc can't be used together
    docFlags =
            cb::mcbp::subdoc::doc_flag::Mkdoc | cb::mcbp::subdoc::doc_flag::Add;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());
    docFlags = cb::mcbp::subdoc::doc_flag::AccessDeleted;

    flags |= SUBDOC_FLAG_EXPAND_MACROS;
    if (allowMacroExpansion()) {
        EXPECT_EQ(cb::mcbp::Status::Success, validate());

        // but it should fail if we don't have the XATTR_PATH
        flags = SUBDOC_FLAG_EXPAND_MACROS;
        EXPECT_EQ(cb::mcbp::Status::SubdocXattrInvalidFlagCombo, validate());

        // And it should also fail if we have Illegal macros
        flags |= SUBDOC_FLAG_XATTR_PATH;
        EXPECT_EQ(cb::mcbp::Status::Success, validate());
        value = "${UnknownMacro}";
        EXPECT_EQ(cb::mcbp::Status::SubdocXattrUnknownMacro, validate());
    } else {
        EXPECT_EQ(cb::mcbp::Status::Einval, validate());
    }
}

/**
 * The SubdocXattrMultiLookupTest tests the XATTR specific constraints
 * over the normal subdoc constraints tested elsewhere
 */
class SubdocXattrMultiLookupTest : public ::testing::WithParamInterface<bool>,
                                   public ValidatorTest {
public:
    SubdocXattrMultiLookupTest() : ValidatorTest(GetParam()) {
        request.setKey("Document");
    }

    void SetUp() override {
        ValidatorTest::SetUp();
    }

protected:
    cb::mcbp::Status validate() {
        std::vector<uint8_t> packet;
        request.encode(packet);
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::SubdocMultiLookup,
                static_cast<void*>(packet.data()));
    }

    BinprotSubdocMultiLookupCommand request;
};

TEST_P(SubdocXattrMultiLookupTest, XAttrMayBeFirst) {
    request.addLookup({cb::mcbp::ClientOpcode::SubdocExists,
                       SUBDOC_FLAG_XATTR_PATH,
                       "_sync.cas"});
    request.addLookup({cb::mcbp::ClientOpcode::SubdocExists,
                       SUBDOC_FLAG_NONE,
                       "meta.author"});
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(SubdocXattrMultiLookupTest, XAttrCantBeLast) {
    request.addLookup({cb::mcbp::ClientOpcode::SubdocExists,
                       SUBDOC_FLAG_NONE,
                       "meta.author"});
    request.addLookup({cb::mcbp::ClientOpcode::SubdocExists,
                       SUBDOC_FLAG_XATTR_PATH,
                       "_sync.cas"});
    EXPECT_EQ(cb::mcbp::Status::SubdocInvalidXattrOrder, validate());
}

TEST_P(SubdocXattrMultiLookupTest, XAttrKeyIsChecked) {
    // We got other unit tests that tests all of the different restrictions
    // for the subdoc key.. just make sure that it is actually called..
    // Check that we can't insert a key > 16 chars
    request.addLookup({cb::mcbp::ClientOpcode::SubdocExists,
                       SUBDOC_FLAG_XATTR_PATH,
                       "ThisIsASuperDuperLongPath"});
    EXPECT_EQ(cb::mcbp::Status::XattrEinval, validate());
}

TEST_P(SubdocXattrMultiLookupTest, XattrFlagsMakeSense) {
    request.addLookup({cb::mcbp::ClientOpcode::SubdocExists,
                       SUBDOC_FLAG_XATTR_PATH,
                       "_sync.cas"});
    EXPECT_EQ(cb::mcbp::Status::Success, validate());

    // We shouldn't be allowed to expand macros for a lookup command
    request[0].flags = SUBDOC_FLAG_EXPAND_MACROS;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());

    // We shouldn't be allowed to expand macros for a lookup command
    // and the SUBDOC_FLAG_EXPAND_MACROS must have SUBDOC_FLAG_XATTR_PATH
    request[0].flags = SUBDOC_FLAG_EXPAND_MACROS | SUBDOC_FLAG_XATTR_PATH;
    EXPECT_EQ(cb::mcbp::Status::Einval, validate());

    // Let's try a valid access deleted flag
    request[0].flags = SUBDOC_FLAG_NONE;
    request.addDocFlag(cb::mcbp::subdoc::doc_flag::AccessDeleted);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());

    // We should be able to access deleted docs if both flags are set
    request[0].flags = SUBDOC_FLAG_XATTR_PATH;
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(SubdocXattrMultiLookupTest, AllowWholeDocAndXattrLookup) {
    request.addLookup({cb::mcbp::ClientOpcode::SubdocGet,
                       SUBDOC_FLAG_XATTR_PATH,
                       "_sync"});
    request.addLookup({cb::mcbp::ClientOpcode::Get, SUBDOC_FLAG_NONE, ""});
    request.addDocFlag(cb::mcbp::subdoc::doc_flag::AccessDeleted);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(SubdocXattrMultiLookupTest, AllowMultipleLookups) {
    for (int ii = 0; ii < 10; ii++) {
        request.addLookup({cb::mcbp::ClientOpcode::SubdocExists,
                           SUBDOC_FLAG_XATTR_PATH,
                           "_sync.cas"});
    }
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(SubdocXattrMultiLookupTest, AllLookupsMustBeOnTheSamePath) {
    request.addLookup({cb::mcbp::ClientOpcode::SubdocExists,
                       SUBDOC_FLAG_XATTR_PATH,
                       "_sync.cas"});
    request.addLookup({cb::mcbp::ClientOpcode::SubdocExists,
                       SUBDOC_FLAG_XATTR_PATH,
                       "foo.bar"});
    EXPECT_EQ(cb::mcbp::Status::SubdocXattrInvalidKeyCombo, validate());
}

/**
 * The SubdocXattrMultiLookupTest tests the XATTR specific constraints
 * over the normal subdoc constraints tested elsewhere
 */
class SubdocXattrMultiMutationTest : public ::testing::WithParamInterface<bool>,
                                     public ValidatorTest {
public:
    SubdocXattrMultiMutationTest() : ValidatorTest(GetParam()) {
        request.setKey("Document");
    }

    void SetUp() override {
        ValidatorTest::SetUp();
    }

protected:
    cb::mcbp::Status validate() {
        std::vector<uint8_t> packet;
        request.encode(packet);
        return ValidatorTest::validate(
                cb::mcbp::ClientOpcode::SubdocMultiMutation,
                static_cast<void*>(packet.data()));
    }

    BinprotSubdocMultiMutationCommand request;
};

TEST_P(SubdocXattrMultiMutationTest, XAttrMayBeFirst) {
    request.addMutation({cb::mcbp::ClientOpcode::SubdocReplace,
                         SUBDOC_FLAG_XATTR_PATH,
                         "_sync.cas",
                         R"({"foo" : "bar"})"});
    request.addMutation({cb::mcbp::ClientOpcode::SubdocReplace,
                         SUBDOC_FLAG_NONE,
                         "meta.author",
                         R"({"name" : "Bubba"})"});
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(SubdocXattrMultiMutationTest, XAttrCantBeLast) {
    request.addMutation({cb::mcbp::ClientOpcode::SubdocReplace,
                         SUBDOC_FLAG_NONE,
                         "meta.author",
                         R"({"name" : "Bubba"})"});
    request.addMutation({cb::mcbp::ClientOpcode::SubdocReplace,
                         SUBDOC_FLAG_XATTR_PATH,
                         "_sync.cas",
                         R"({"foo" : "bar"})"});
    EXPECT_EQ(cb::mcbp::Status::SubdocInvalidXattrOrder, validate());
}

TEST_P(SubdocXattrMultiMutationTest, XAttrKeyIsChecked) {
    // We got other unit tests that tests all of the different restrictions
    // for the subdoc key.. just make sure that it is actually called..
    // Check that we can't insert a key > 16 chars
    request.addMutation({cb::mcbp::ClientOpcode::SubdocReplace,
                         SUBDOC_FLAG_XATTR_PATH,
                         "ThisIsASuperDuperLongPath",
                         R"({"foo" : "bar"})"});
    EXPECT_EQ(cb::mcbp::Status::XattrEinval, validate());
}

TEST_P(SubdocXattrMultiMutationTest, XattrFlagsMakeSense) {
    request.addMutation({cb::mcbp::ClientOpcode::SubdocReplace,
                         SUBDOC_FLAG_XATTR_PATH,
                         "_sync.cas",
                         "\"${Mutation.CAS}\""});
    EXPECT_EQ(cb::mcbp::Status::Success, validate());

    request[0].flags = SUBDOC_FLAG_EXPAND_MACROS;
    EXPECT_EQ(cb::mcbp::Status::SubdocXattrInvalidFlagCombo, validate());

    request[0].flags = SUBDOC_FLAG_EXPAND_MACROS | SUBDOC_FLAG_XATTR_PATH;
    EXPECT_EQ(cb::mcbp::Status::Success, validate());

    request.addDocFlag(cb::mcbp::subdoc::doc_flag::AccessDeleted);
    request[0].flags = SUBDOC_FLAG_EXPAND_MACROS | SUBDOC_FLAG_XATTR_PATH;
    EXPECT_EQ(cb::mcbp::Status::Success, validate());

    request[0].value = "${UnknownMacro}";
    EXPECT_EQ(cb::mcbp::Status::SubdocXattrUnknownMacro, validate());
    request[0].value = "\"${Mutation.CAS}\"";

    // Let's try a valid access deleted flag
    request[0].flags = SUBDOC_FLAG_NONE;
    request.addDocFlag(cb::mcbp::subdoc::doc_flag::AccessDeleted);
    EXPECT_EQ(cb::mcbp::Status::Success, validate());

    // We should be able to access deleted docs if both flags are set
    request[0].flags = SUBDOC_FLAG_XATTR_PATH;
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(SubdocXattrMultiMutationTest, AllowMultipleMutations) {
    for (int ii = 0; ii < 10; ii++) {
        request.addMutation({cb::mcbp::ClientOpcode::SubdocReplace,
                             SUBDOC_FLAG_XATTR_PATH,
                             "_sync.cas",
                             R"({"foo" : "bar"})"});
    }
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

TEST_P(SubdocXattrMultiMutationTest, AllMutationsMustBeOnTheSamePath) {
    request.addMutation({cb::mcbp::ClientOpcode::SubdocReplace,
                         SUBDOC_FLAG_XATTR_PATH,
                         "_sync.cas",
                         R"({"foo" : "bar"})"});
    request.addMutation({cb::mcbp::ClientOpcode::SubdocReplace,
                         SUBDOC_FLAG_XATTR_PATH,
                         "foo.bar",
                         R"({"foo" : "bar"})"});
    EXPECT_EQ(cb::mcbp::Status::SubdocXattrInvalidKeyCombo, validate());
}

TEST_P(SubdocXattrMultiMutationTest, AllowXattrUpdateAndWholeDocDelete) {
    request.addMutation({cb::mcbp::ClientOpcode::SubdocReplace,
                         SUBDOC_FLAG_XATTR_PATH,
                         "_sync.cas",
                         R"({"foo" : "bar"})"});
    request.addMutation(
            {cb::mcbp::ClientOpcode::Delete, SUBDOC_FLAG_NONE, "", ""});
    EXPECT_EQ(cb::mcbp::Status::Success, validate());
}

INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         SubdocXattrMultiLookupTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());
INSTANTIATE_TEST_SUITE_P(CollectionsOnOff,
                         SubdocXattrMultiMutationTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

} // namespace mcbp::test
