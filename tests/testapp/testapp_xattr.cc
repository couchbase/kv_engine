/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "testapp_xattr.h"

#include <folly/Range.h>
#include <folly/io/IOBuf.h>
#include <platform/compress.h>
#include <platform/crc32c.h>
#include <platform/dirutils.h>
#include <platform/string_hex.h>
#include <utilities/string_utilities.h>
#include <array>

using namespace cb::mcbp;
using namespace cb::mcbp::subdoc;

BinprotSubdocMultiLookupResponse XattrNoDocTest::subdoc_multi_lookup(
        std::vector<BinprotSubdocMultiLookupCommand::LookupSpecifier> specs,
        DocFlag docFlags) {
    BinprotSubdocMultiLookupCommand cmd{name, std::move(specs), docFlags};
    return BinprotSubdocMultiLookupResponse(userConnection->execute(cmd));
}

BinprotSubdocMultiMutationResponse XattrNoDocTest::subdoc_multi_mutation(
        std::vector<BinprotSubdocMultiMutationCommand::MutationSpecifier> specs,
        DocFlag docFlags) {
    BinprotSubdocMultiMutationCommand cmd{name, std::move(specs), docFlags};
    return BinprotSubdocMultiMutationResponse(userConnection->execute(cmd));
}

GetMetaPayload XattrNoDocTest::get_meta() {
    return userConnection->getMeta(name, Vbid(0), GetMetaVersion::V2);
}

BinprotSubdocMultiMutationCommand XattrNoDocTest::makeSDKTxnMultiMutation()
        const {
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(
            ClientOpcode::SubdocDictAdd, PathFlag::XattrPath, "txn", "{}");
    cmd.addMutation(
            ClientOpcode::SubdocDictUpsert, PathFlag::XattrPath, "txn.id", "2");
    cmd.addMutation(ClientOpcode::SubdocArrayPushLast,
                    PathFlag::XattrPath,
                    "txn.array",
                    "2");
    cmd.addMutation(ClientOpcode::SubdocArrayPushFirst,
                    PathFlag::XattrPath,
                    "txn.array",
                    "0");
    cmd.addMutation(ClientOpcode::SubdocArrayAddUnique,
                    PathFlag::XattrPath,
                    "txn.array",
                    "3");
    cmd.addMutation(ClientOpcode::SubdocCounter,
                    PathFlag::XattrPath,
                    "txn.counter",
                    "1");
    return cmd;
}

class XattrTest : public XattrNoDocTest {
protected:
    void SetUp() override {
        XattrNoDocTest::SetUp();

        // Create the document to operate on (with some compressible data).
        document.info.flags = 0;
        document.info.id = name;
        document.info.datatype = cb::mcbp::Datatype::Raw;
        document.value =
                R"({"couchbase": {"version": "spock", "next_version": "vulcan"}})";
        if (hasSnappySupport() == ClientSnappySupport::Yes) {
            // Compress the complete body.
            document.compress();
        }
        userConnection->mutate(document, Vbid(0), MutationType::Set);
    }

    void doArrayInsertTest(const std::string& path) {
        auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                           name,
                           path,
                           "\"Smith\"",
                           PathFlag::XattrPath | PathFlag::Mkdir_p);
        EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        resp = subdoc(cb::mcbp::ClientOpcode::SubdocArrayInsert,
                      name,
                      path + "[0]",
                      "\"Bart\"",
                      PathFlag::XattrPath);
        EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        resp = subdoc(cb::mcbp::ClientOpcode::SubdocArrayInsert,
                      name,
                      path + "[1]",
                      "\"Jones\"",
                      PathFlag::XattrPath);
        EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        resp = subdoc_get(path, PathFlag::XattrPath);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        EXPECT_EQ("[\"Bart\",\"Jones\",\"Smith\"]", resp.getDataView());
    }

    void doArrayPushLastTest(const std::string& path) {
        auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                           name,
                           path,
                           "\"Smith\"",
                           PathFlag::XattrPath | PathFlag::Mkdir_p);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        resp = subdoc_get(path, PathFlag::XattrPath);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        EXPECT_EQ("[\"Smith\"]", resp.getDataView());

        // Add a second one so we know it was added last ;-)
        resp = subdoc(cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                      name,
                      path,
                      "\"Jones\"",
                      PathFlag::XattrPath | PathFlag::Mkdir_p);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        resp = subdoc_get(path, PathFlag::XattrPath);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        EXPECT_EQ("[\"Smith\",\"Jones\"]", resp.getDataView());
    }

    void doArrayPushFirstTest(const std::string& path) {
        auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                           name,
                           path,
                           "\"Smith\"",
                           PathFlag::XattrPath | PathFlag::Mkdir_p);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        resp = subdoc_get(path, PathFlag::XattrPath);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        EXPECT_EQ("[\"Smith\"]", resp.getDataView());

        // Add a second one so we know it was added first ;-)
        resp = subdoc(cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                      name,
                      path,
                      "\"Jones\"",
                      PathFlag::XattrPath | PathFlag::Mkdir_p);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        resp = subdoc_get(path, PathFlag::XattrPath);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        EXPECT_EQ("[\"Jones\",\"Smith\"]", resp.getDataView());
    }

    void doAddUniqueTest(const std::string& path) {
        auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                           name,
                           path,
                           "\"Smith\"",
                           PathFlag::XattrPath | PathFlag::Mkdir_p);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        resp = subdoc_get(path, PathFlag::XattrPath);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        EXPECT_EQ("[\"Smith\"]", resp.getDataView());

        resp = subdoc(cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                      name,
                      path,
                      "\"Jones\"",
                      PathFlag::XattrPath);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        resp = subdoc_get(path, PathFlag::XattrPath);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        EXPECT_EQ("[\"Smith\",\"Jones\"]", resp.getDataView());

        resp = subdoc(cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                      name,
                      path,
                      "\"Jones\"",
                      PathFlag::XattrPath);
        ASSERT_EQ(cb::mcbp::Status::SubdocPathEexists, resp.getStatus());

        resp = subdoc_get(path, PathFlag::XattrPath);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        EXPECT_EQ("[\"Smith\",\"Jones\"]", resp.getDataView());
    }

    void doCounterTest(const std::string& path) {
        auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocCounter,
                           name,
                           path,
                           "1",
                           PathFlag::XattrPath | PathFlag::Mkdir_p);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        resp = subdoc_get(path, PathFlag::XattrPath);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        EXPECT_EQ("1", resp.getDataView());

        resp = subdoc(cb::mcbp::ClientOpcode::SubdocCounter,
                      name,
                      path,
                      "1",
                      PathFlag::XattrPath | PathFlag::Mkdir_p);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        resp = subdoc_get(path, PathFlag::XattrPath);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        EXPECT_EQ("2", resp.getDataView());
    }

    // Test replacing a compressed/uncompressed value (based on test
    // variant) with an compressed/uncompressed value (based on input
    // paramter). XATTRs should be correctly merged.
    void doReplaceWithXattrTest(bool compress) {
        // Set initial body+XATTR, compressed depending on test variant.
        setBodyAndXattr(value, {{sysXattr, xattrVal}});

        // Replace body with new body.
        const std::string replacedValue = "\"JSON string\"";
        document.value = replacedValue;
        document.info.cas = cb::mcbp::cas::Wildcard;
        document.info.datatype = cb::mcbp::Datatype::Raw;
        if (compress) {
            document.compress();
        }
        userConnection->mutate(document, Vbid(0), MutationType::Replace);

        // Validate contents.
        EXPECT_EQ(xattrVal, getXattr(sysXattr).getDataView());
        auto response = userConnection->get(name, Vbid(0));
        EXPECT_EQ(replacedValue, response.value);
        // Combined result will not be compressed; so just check for
        // JSON / not JSON.
        EXPECT_EQ(expectedJSONDatatype(), response.info.datatype);
    }

    /**
     * Takes a subdoc multimutation command, sends it and checks that the
     * values set correctly
     * @param cmd The command to send
     * @param expectedDatatype The expected datatype of the resulting document.
     * @return Returns the response from the multi-mutation
     */
    BinprotSubdocMultiMutationResponse testBodyAndXattrCmd(
            BinprotSubdocMultiMutationCommand& cmd,
            protocol_binary_datatype_t expectedDatatype =
                    PROTOCOL_BINARY_DATATYPE_JSON |
                    PROTOCOL_BINARY_DATATYPE_XATTR) {
        auto multiResp = BinprotSubdocMultiMutationResponse(
                userConnection->execute(cmd));
        EXPECT_EQ(cb::mcbp::Status::Success, multiResp.getStatus());

        // Check the body was set correctly
        auto doc = userConnection->get(name, Vbid(0));
        EXPECT_EQ(value, doc.value);

        // Check the xattr was set correctly
        const auto resp = subdoc_get(sysXattr, PathFlag::XattrPath);
        EXPECT_EQ(xattrVal, resp.getDataView());

        // Check the datatype.
        const auto meta = get_meta();
        EXPECT_EQ(expectedDatatype, meta.getDatatype());

        return multiResp;
    }

    void verify_xtoc_user_system_xattr() {
        // Test to check that we can get both an xattr and the main body in
        // subdoc multi-lookup
        setBodyAndXattr(value, {{sysXattr, xattrVal}});

        // Sanity checks and setup done lets try the multi-lookup

        BinprotSubdocMultiLookupCommand cmd;
        cmd.setKey(name);
        cmd.addGet("$XTOC", PathFlag::XattrPath);
        cmd.addLookup("", cb::mcbp::ClientOpcode::Get, PathFlag::None);

        auto multiResp =
                BinprotSubdocMultiLookupResponse(userConnection->execute(cmd));
        ASSERT_EQ(cb::mcbp::Status::Success, multiResp.getStatus());
        auto results = multiResp.getResults();
        EXPECT_EQ(cb::mcbp::Status::Success, results[0].status);
        EXPECT_EQ(R"(["_sync"])", results[0].value);
        EXPECT_EQ(value, results[1].value);

        xattr_upsert("userXattr", R"(["Test"])");
        multiResp =
                BinprotSubdocMultiLookupResponse(userConnection->execute(cmd));
        ASSERT_EQ(cb::mcbp::Status::Success, multiResp.getStatus());
        results = multiResp.getResults();
        EXPECT_EQ(cb::mcbp::Status::Success, results[0].status);
        EXPECT_EQ(R"(["_sync","userXattr"])", results[0].value);
    }

    std::string value = "{\"Field\":56}";
    const std::string sysXattr = "_sync";
    const std::string xattrVal = "{\"eg\":99}";
};

/// Explicit text fixutre for tests which want Xattr support disabled.
class XattrDisabledTest : public XattrTest {};

// Note: We always need XattrSupport::Yes for these tests
INSTANTIATE_TEST_SUITE_P(
        TransportProtocols,
        XattrTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpPlain),
                           ::testing::Values(XattrSupport::Yes),
                           ::testing::Values(ClientJSONSupport::Yes,
                                             ClientJSONSupport::No),
                           ::testing::Values(ClientSnappySupport::Yes)),
        PrintToStringCombinedName());

// Instantiation for tests which don't want an initial document.
INSTANTIATE_TEST_SUITE_P(
        TransportProtocols,
        XattrNoDocTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpPlain),
                           ::testing::Values(XattrSupport::Yes),
                           ::testing::Values(ClientJSONSupport::Yes,
                                             ClientJSONSupport::No),
                           ::testing::Values(ClientSnappySupport::Yes)),
        PrintToStringCombinedName());

INSTANTIATE_TEST_SUITE_P(
        TransportProtocols,
        XattrNoDocDurabilityTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpPlain),
                           ::testing::Values(XattrSupport::Yes),
                           ::testing::Values(ClientJSONSupport::Yes,
                                             ClientJSONSupport::No),
                           ::testing::Values(ClientSnappySupport::Yes)),
        PrintToStringCombinedName());

// Instantiation for tests which want XATTR support disabled.
INSTANTIATE_TEST_SUITE_P(
        TransportProtocols,
        XattrDisabledTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpPlain),
                           ::testing::Values(XattrSupport::No),
                           ::testing::Values(ClientJSONSupport::Yes,
                                             ClientJSONSupport::No),
                           ::testing::Values(ClientSnappySupport::No)),
        PrintToStringCombinedName());

TEST_P(XattrTest, GetXattrAndBody) {
    // Test to check that we can get both an xattr and the main body in
    // subdoc multi-lookup
    setBodyAndXattr(value, {{sysXattr, xattrVal}});

    // Sanity checks and setup done lets try the multi-lookup
    auto multiResp = subdoc_multi_lookup(
            {{cb::mcbp::ClientOpcode::SubdocGet, PathFlag::XattrPath, sysXattr},
             {cb::mcbp::ClientOpcode::Get, PathFlag::None, ""}});

    ASSERT_EQ(cb::mcbp::Status::Success, multiResp.getStatus());
    const auto results = multiResp.getResults();
    EXPECT_EQ(xattrVal, results[0].value);
    EXPECT_EQ(value, results[1].value);
}

TEST_P(XattrTest, SetXattrAndBodyNewDoc) {
    // Ensure we are working on a new doc
    userConnection->remove(name, Vbid(0));
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                    PathFlag::XattrPath,
                    sysXattr,
                    xattrVal);
    cmd.addMutation(cb::mcbp::ClientOpcode::Set, PathFlag::None, "", value);
    cmd.addDocFlag(cb::mcbp::subdoc::DocFlag::Mkdoc);

    testBodyAndXattrCmd(cmd);
}

// Test setting just an XATTR path without a value using a single-path
// dictionary operation.
// Regression test for MB-40262.
TEST_P(XattrTest, SetXattrNoValueNewDocDict) {
    // Ensure we are working on a new doc
    userConnection->remove(name, Vbid(0));

    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictAdd,
                       name,
                       "meta.deleted",
                       "true",
                       PathFlag::XattrPath,
                       cb::mcbp::subdoc::DocFlag::Mkdoc);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    auto doc = userConnection->get(name, Vbid(0));
    EXPECT_EQ("{}", doc.value);
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR,
              get_meta().getDatatype());
}

// Test setting just an XATTR path without a value using a single-path
// array operation.
// Regression test for MB-40262.
TEST_P(XattrTest, SetXattrNoValueNewDocArray) {
    // Ensure we are working on a new doc
    userConnection->remove(name, Vbid(0));

    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                       name,
                       "array",
                       "0",
                       PathFlag::XattrPath,
                       cb::mcbp::subdoc::DocFlag::Mkdoc);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    // Expect value to be empty JSON object.
    auto doc = userConnection->get(name, Vbid(0));
    EXPECT_EQ("{}", doc.value);
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR,
              get_meta().getDatatype());
}

// Test setting just an XATTR path without a value using a multi-mutation
// dictionary operation.
// Regression test for MB-40262.
TEST_P(XattrTest, SetXattrNoValueNewDocMultipathDict) {
    // Ensure we are working on a new doc
    userConnection->remove(name, Vbid(0));

    auto response =
            subdoc_multi_mutation({{cb::mcbp::ClientOpcode::SubdocDictUpsert,
                                    PathFlag::XattrPath,
                                    "meta.deleted",
                                    "true"}},
                                  cb::mcbp::subdoc::DocFlag::Mkdoc);
    ASSERT_EQ(cb::mcbp::Status::Success, response.getStatus());

    // Expect value to be empty JSON object.
    auto doc = userConnection->get(name, Vbid(0));
    EXPECT_EQ("{}", doc.value);
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR,
              get_meta().getDatatype());
}

// Test setting just an XATTR path without a value using a multi-mutation
// dictionary operation.
// Regression test for MB-40262.
TEST_P(XattrTest, SetXattrNoValueNewDocMultipathArray) {
    // Ensure we are working on a new doc
    userConnection->remove(name, Vbid(0));

    auto response =
            subdoc_multi_mutation({{cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                                    PathFlag::XattrPath,
                                    "meta.ids",
                                    "123"}},
                                  cb::mcbp::subdoc::DocFlag::Mkdoc);
    ASSERT_EQ(cb::mcbp::Status::Success, response.getStatus());

    // Expect value to be empty JSON dict.
    auto doc = userConnection->get(name, Vbid(0));
    EXPECT_EQ("{}", doc.value);
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_XATTR,
              get_meta().getDatatype());
}

TEST_P(XattrTest, SetXattrAndBodyNewDocWithExpiry) {
    // For MB-24542
    // Ensure we are working on a new doc
    userConnection->remove(name, Vbid(0));
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.setExpiry(3);
    cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                    PathFlag::XattrPath,
                    sysXattr,
                    xattrVal);
    cmd.addMutation(cb::mcbp::ClientOpcode::Set, PathFlag::None, "", value);
    cmd.addDocFlag(cb::mcbp::subdoc::DocFlag::Mkdoc);

    testBodyAndXattrCmd(cmd);

    // Jump forward in time to expire item
    userConnection->adjustMemcachedClock(
            4, cb::mcbp::request::AdjustTimePayload::TimeType::Uptime);

    auto getResp = subdoc_multi_lookup(
            {{cb::mcbp::ClientOpcode::Get, PathFlag::None, ""}});
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, getResp.getStatus());

    // Restore time.
    userConnection->adjustMemcachedClock(
            0, cb::mcbp::request::AdjustTimePayload::TimeType::Uptime);
}

TEST_P(XattrTest, SetXattrAndBodyExistingDoc) {
    // Ensure that a doc is already present
    setBodyAndXattr("{\"TestField\":56788}", {{sysXattr, "4543"}});
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                    PathFlag::XattrPath | PathFlag::Mkdir_p,
                    sysXattr,
                    xattrVal);
    cmd.addMutation(cb::mcbp::ClientOpcode::Set, PathFlag::None, "", value);

    testBodyAndXattrCmd(cmd);
}

TEST_P(XattrTest, SetXattrAndBodyInvalidFlags) {
    // First test invalid path flags
    std::array<PathFlag, 3> badFlags = {
            {PathFlag::Mkdir_p, PathFlag::XattrPath, PathFlag::ExpandMacros}};

    for (const auto& flag : badFlags) {
        BinprotSubdocMultiMutationCommand cmd;
        cmd.setKey(name);

        // Should not be able to set all XATTRs
        cmd.addMutation(ClientOpcode::Set, flag, "", value);
        const auto resp = userConnection->execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
    }

    // Now test the invalid doc flags
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);

    // Should not be able to set all XATTRs
    cmd.addMutation(cb::mcbp::ClientOpcode::Set, PathFlag::None, "", value);
    cmd.addDocFlag(cb::mcbp::subdoc::DocFlag::AccessDeleted);

    const auto resp = userConnection->execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
}

TEST_P(XattrTest, SetBodyInMultiLookup) {
    // Check that we can't put a CMD_SET in a multi lookup
    auto multiResp = subdoc_multi_lookup(
            {{cb::mcbp::ClientOpcode::Set, PathFlag::None, ""}});

    EXPECT_EQ(cb::mcbp::Status::SubdocInvalidCombo, multiResp.getStatus());
}

TEST_P(XattrTest, GetBodyInMultiMutation) {
    // Check that we can't put a CMD_GET in a multi mutation
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);

    // Should not be able to put a get in a multi multi-mutation
    cmd.addMutation(cb::mcbp::ClientOpcode::Get, PathFlag::None, "", value);
    const auto resp = userConnection->execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::SubdocInvalidCombo, resp.getStatus());
}

TEST_P(XattrTest, AddBodyAndXattr) {
    // Check that we can use the Add doc flag to create a new document

    // Get rid of any existing doc
    userConnection->remove(name, Vbid(0));

    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                    PathFlag::XattrPath,
                    sysXattr,
                    xattrVal);
    cmd.addMutation(cb::mcbp::ClientOpcode::Set, PathFlag::None, "", value);
    cmd.addDocFlag(cb::mcbp::subdoc::DocFlag::Add);

    const auto resp = userConnection->execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
}

TEST_P(XattrTest, AddBodyAndXattrAlreadyExistDoc) {
    // Check that usage of the Add flag will return EEXISTS if a key already
    // exists

    // Make sure a doc exists
    setBodyAndXattr("{\"TestField\":56788}", {{sysXattr, "4543"}});

    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                    PathFlag::XattrPath,
                    sysXattr,
                    xattrVal);
    cmd.addMutation(cb::mcbp::ClientOpcode::Set, PathFlag::None, "", value);
    cmd.addDocFlag(cb::mcbp::subdoc::DocFlag::Add);

    const auto resp = userConnection->execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::KeyEexists, resp.getStatus());
}

TEST_P(XattrTest, AddBodyAndXattrInvalidDocFlags) {
    // Check that usage of the Add flag will return EINVAL if the mkdoc doc
    // flag is also passed. The preexisting document exists to check that
    // we fail with the right error. i.e. we shouldn't even be fetching
    // the document from the engine if these two flags are set.

    // Make sure a doc exists
    setBodyAndXattr("{\"TestField\":56788}", {{sysXattr, "4543"}});

    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                    PathFlag::XattrPath,
                    sysXattr,
                    xattrVal);
    cmd.addMutation(cb::mcbp::ClientOpcode::Set, PathFlag::None, "", value);
    cmd.addDocFlag(cb::mcbp::subdoc::DocFlag::Add);
    cmd.addDocFlag(cb::mcbp::subdoc::DocFlag::Mkdoc);

    const auto resp = userConnection->execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
}

TEST_P(XattrTest, TestSeqnoMacroExpansion) {
    // Test that we don't replace it when we don't send EXPAND_MACROS
    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                       name,
                       "_sync.seqno",
                       "\"${Mutation.seqno}\"",
                       PathFlag::XattrPath | PathFlag::Mkdir_p);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    resp = subdoc_get("_sync.seqno", PathFlag::XattrPath);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    EXPECT_EQ("\"${Mutation.seqno}\"", resp.getDataView());

    // Verify that we expand the macro to something that isn't the macro
    // literal. i.e. If we send ${Mutation.SEQNO} we want to check that we
    // replaced that with something else (hopefully the correct value).
    // Unfortunately, unlike the cas, we do not get the seqno so we cannot
    // check it.
    resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                  name,
                  "_sync.seqno",
                  "\"${Mutation.seqno}\"",
                  PathFlag::XattrPath | PathFlag::ExpandMacros);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    resp = subdoc_get("_sync.seqno", PathFlag::XattrPath);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    EXPECT_NE("\"${Mutation.seqno}\"", resp.getDataView());
}

TEST_P(XattrTest, TestMacroExpansionAndIsolation) {
    // This test verifies that you can have the same path in xattr's
    // and in the document without one affecting the other.
    // In addition to that we're testing that macro expansion works
    // as expected.

    // Lets store the macro and verify that it isn't expanded without the
    // expand macro flag
    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                       name,
                       "_sync.cas",
                       "\"${Mutation.CAS}\"",
                       PathFlag::XattrPath | PathFlag::Mkdir_p);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    resp = subdoc_get("_sync.cas", PathFlag::XattrPath);
    EXPECT_EQ("\"${Mutation.CAS}\"", resp.getDataView());

    // Let's update the body version..
    resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                  name,
                  "_sync.cas",
                  "\"If you don't know me by now\"",
                  PathFlag::Mkdir_p);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    // The xattr version should have been unchanged...
    resp = subdoc_get("_sync.cas", PathFlag::XattrPath);
    EXPECT_EQ("\"${Mutation.CAS}\"", resp.getDataView());

    // And the body version should be what we set it to
    resp = subdoc_get("_sync.cas");
    EXPECT_EQ("\"If you don't know me by now\"", resp.getDataView());

    // Then change it to macro expansion
    resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                  name,
                  "_sync.cas",
                  "\"${Mutation.CAS}\"",
                  PathFlag::XattrPath | PathFlag::ExpandMacros);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    /// Fetch the field and verify that it expanded the cas! (the expanded
    /// value is in _NETWORK BYTE ORDER_
    resp = subdoc_get("_sync.cas", PathFlag::XattrPath);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    const auto first_cas = resp.getCas();
    const auto cas_string = "\"" + cb::to_hex(htonll(resp.getCas())) + "\"";
    EXPECT_EQ(cas_string, resp.getDataView());

    // Let's update the body version..
    resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                  name,
                  "_sync.cas",
                  "\"Hell ain't such a bad place to be\"");
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    // The macro should not have been expanded again...
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    resp = subdoc_get("_sync.cas", PathFlag::XattrPath);
    EXPECT_EQ(cas_string, resp.getDataView());
    EXPECT_NE(first_cas, resp.getCas());
}

// Test that macro expansion only happens once if the value is replaced.
TEST_P(XattrTest, TestMacroExpansionOccursOnce) {
    userConnection->mutate(document, Vbid(0), MutationType::Set);

    createXattr("meta.cas", "\"${Mutation.CAS}\"", true);
    const auto mutation_cas = getXattr("meta.cas");
    EXPECT_NE("\"${Mutation.CAS}\"", mutation_cas.getDataView())
            << "Macro expansion did not occur when requested";
    userConnection->mutate(document, Vbid(0), MutationType::Replace);
    EXPECT_EQ(mutation_cas.getDataView(), getXattr("meta.cas").getDataView())
            << "'meta.cas' should be unchanged when value replaced";
}

TEST_P(XattrTest, AddSystemXattrToDeletedItem) {
    userConnection->remove(name, Vbid(0));

    // let's add a system XATTR to the deleted document
    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictAdd,
                       name,
                       "_sync.deleted",
                       "true",
                       PathFlag::XattrPath | PathFlag::Mkdir_p,
                       cb::mcbp::subdoc::DocFlag::AccessDeleted);
    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());

    resp = subdoc_get("_sync.deleted",
                      PathFlag::XattrPath,
                      cb::mcbp::subdoc::DocFlag::AccessDeleted);
    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());
    EXPECT_EQ("true", resp.getDataView());
}

TEST_P(XattrTest, AddUserXattrToDeletedItem) {
    using namespace cb::mcbp::subdoc;
    userConnection->remove(name, Vbid(0));

    // let's add a user XATTR to the deleted document
    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictAdd,
                       name,
                       "txn.deleted",
                       "true",
                       PathFlag::XattrPath | PathFlag::Mkdir_p,
                       DocFlag::Mkdoc | DocFlag::AccessDeleted);
    EXPECT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());

    resp = subdoc_get(
            "txn.deleted", PathFlag::XattrPath, DocFlag::AccessDeleted);
    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());
    EXPECT_EQ("true", resp.getDataView());
}

TEST_P(XattrTest, MB_22318) {
    EXPECT_EQ(cb::mcbp::Status::Success,
              xattr_upsert("doc", R"({"author": "Bart"})"));
}

TEST_P(XattrTest, MB_22319) {
    // This is listed as working in the bug report
    EXPECT_EQ(uint8_t(cb::mcbp::Status::Success),
              uint8_t(xattr_upsert("doc.readcount", "0")));
    EXPECT_EQ(cb::mcbp::Status::Success,
              xattr_upsert("doc.author", "\"jack\""));

    // The failing bits is:
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                    PathFlag::XattrPath,
                    "doc.readcount",
                    "1");
    cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                    PathFlag::XattrPath,
                    "doc.author",
                    "\"jones\"");

    const auto resp = userConnection->execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
}


/*
 * The spec lists a table of the behavior when operating on a
 * full XATTR spec or if it is a partial XATTR spec.
 */

/**
 * Reads the value of the given XATTR.
 */
TEST_P(XattrTest, Get_FullXattrSpec) {
    EXPECT_EQ(cb::mcbp::Status::Success,
              xattr_upsert("doc", R"({"author": "Bart","rev":0})"));

    auto response = subdoc_get("doc", PathFlag::XattrPath);
    ASSERT_EQ(cb::mcbp::Status::Success, response.getStatus());
    EXPECT_EQ(R"({"author":"Bart","rev":0})"_json,
              nlohmann::json::parse(response.getDataView()));
}

/**
 * Reads the sub-part of the given XATTR.
 */
TEST_P(XattrTest, Get_PartialXattrSpec) {
    EXPECT_EQ(cb::mcbp::Status::Success,
              xattr_upsert("doc", R"({"author": "Bart","rev":0})"));

    auto response = subdoc_get("doc.author", PathFlag::XattrPath);
    ASSERT_EQ(cb::mcbp::Status::Success, response.getStatus());
    EXPECT_EQ("\"Bart\"", response.getDataView());
}

/**
 * Returns true if the given XATTR exists.
 */
TEST_P(XattrTest, Exists_FullXattrSpec) {
    // The document exists, but we should not have any xattr's
    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocExists,
                       name,
                       "doc",
                       {},
                       PathFlag::XattrPath);
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, resp.getStatus());

    // Create the xattr
    EXPECT_EQ(cb::mcbp::Status::Success,
              xattr_upsert("doc", R"({"author": "Bart","rev":0})"));

    // Now it should exist
    resp = subdoc(cb::mcbp::ClientOpcode::SubdocExists,
                  name,
                  "doc",
                  {},
                  PathFlag::XattrPath);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
}

/**
 * Returns true if the given XATTR exists and the given sub-part
 * of the XATTR exists.
 */
TEST_P(XattrTest, Exists_PartialXattrSpec) {
    // The document exists, but we should not have any xattr's
    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocExists,
                       name,
                       "doc",
                       {},
                       PathFlag::XattrPath);
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, resp.getStatus());

    // Create the xattr
    EXPECT_EQ(cb::mcbp::Status::Success,
              xattr_upsert("doc", R"({"author": "Bart","rev":0})"));

    // Now it should exist
    resp = subdoc(cb::mcbp::ClientOpcode::SubdocExists,
                  name,
                  "doc.author",
                  {},
                  PathFlag::XattrPath);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    // But we don't have one named _sync
    resp = subdoc(cb::mcbp::ClientOpcode::SubdocExists,
                  name,
                  "_sync.cas",
                  {},
                  PathFlag::XattrPath);
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, resp.getStatus());
}

/**
 * If XATTR specified by X-Key does not exist then create it with the
 * given value.
 * If XATTR already exists - fail with SUBDOC_PATH_EEXISTS
 */
TEST_P(XattrTest, DictAdd_FullXattrSpec) {
    // Adding it the first time should work
    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictAdd,
                       name,
                       "doc",
                       R"({"author": "Bart"})",
                       PathFlag::XattrPath | PathFlag::Mkdir_p);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    // Adding it the first time should work, second time we should get EEXISTS
    resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictAdd,
                  name,
                  "doc",
                  R"({"author": "Bart"})",
                  PathFlag::XattrPath);
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEexists, resp.getStatus());
}

/**
 * Adds a dictionary element specified by the X-Path to the given X-Key.
 */
TEST_P(XattrTest, DictAdd_PartialXattrSpec) {
    // Adding it the first time should work
    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictAdd,
                       name,
                       "doc.author",
                       "\"Bart\"",
                       PathFlag::XattrPath | PathFlag::Mkdir_p);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    // Adding it the first time should work, second time we should get EEXISTS
    resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictAdd,
                  name,
                  "doc.author",
                  "\"Bart\"",
                  PathFlag::XattrPath);
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEexists, resp.getStatus());
}

/**
 * Replaces the whole XATTR specified by X-Key with the given value if
 * the XATTR exists, or creates it with the given value.
 */
TEST_P(XattrTest, DictUpsert_FullXattrSpec) {
    // Adding it the first time should work
    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                       name,
                       "doc",
                       R"({"author": "Bart"})",
                       PathFlag::XattrPath | PathFlag::Mkdir_p);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    // We should be able to update it...
    resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                  name,
                  "doc",
                  R"({"author": "Jones"})",
                  PathFlag::XattrPath);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    resp = subdoc_get("doc.author", PathFlag::XattrPath);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    EXPECT_EQ("\"Jones\"", resp.getDataView());
}

/**
 * Upserts a dictionary element specified by the X-Path to the given X-Key.
 */
TEST_P(XattrTest, DictUpsert_PartialXattrSpec) {
    // Adding it the first time should work
    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                       name,
                       "doc",
                       R"({"author": "Bart"})",
                       PathFlag::XattrPath | PathFlag::Mkdir_p);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    // We should be able to update it...
    resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                  name,
                  "doc.author",
                  "\"Jones\"",
                  PathFlag::XattrPath);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    resp = subdoc_get("doc.author", PathFlag::XattrPath);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    EXPECT_EQ("\"Jones\"", resp.getDataView());
}

/**
 * Deletes the whole XATTR specified by X-Key
 */
TEST_P(XattrTest, Delete_FullXattrSpec) {
    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                       name,
                       "doc",
                       R"({"author": "Bart"})",
                       PathFlag::XattrPath | PathFlag::Mkdir_p);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    resp = subdoc(cb::mcbp::ClientOpcode::SubdocDelete,
                  name,
                  "doc",
                  {},
                  PathFlag::XattrPath);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    // THe entire stuff should be gone
    resp = subdoc_get("doc", PathFlag::XattrPath);
    ASSERT_EQ(cb::mcbp::Status::SubdocPathEnoent, resp.getStatus());
}

/**
 * Deletes the sub-part of the XATTR specified by X-Key and X-Path
 */
TEST_P(XattrTest, Delete_PartialXattrSpec) {
    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                       name,
                       "doc",
                       R"({"author":"Bart","ref":0})",
                       PathFlag::XattrPath | PathFlag::Mkdir_p);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    resp = subdoc(cb::mcbp::ClientOpcode::SubdocDelete,
                  name,
                  "doc.ref",
                  {},
                  PathFlag::XattrPath);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    // THe entire stuff should be gone
    resp = subdoc_get("doc", PathFlag::XattrPath);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    EXPECT_EQ("{\"author\":\"Bart\"}", resp.getDataView());
}

/**
 * If the XATTR specified by X-Key exists, then replace the whole XATTR,
 * otherwise fail with SUBDOC_PATH_EEXISTS
 */
TEST_P(XattrTest, Replace_FullXattrSpec) {
    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocReplace,
                       name,
                       "doc",
                       R"({"author":"Bart","ref":0})",
                       PathFlag::XattrPath);
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, resp.getStatus());

    resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictAdd,
                  name,
                  "doc",
                  R"({"author": "Bart"})",
                  PathFlag::XattrPath | PathFlag::Mkdir_p);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    resp = subdoc(cb::mcbp::ClientOpcode::SubdocReplace,
                  name,
                  "doc",
                  R"({"author":"Bart","ref":0})",
                  PathFlag::XattrPath);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    resp = subdoc_get("doc", PathFlag::XattrPath);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    EXPECT_EQ("{\"author\":\"Bart\",\"ref\":0}", resp.getDataView());
}

/**
 * Replaces the sub-part of the XATTR-specified by X-Key and X-path.
 */
TEST_P(XattrTest, Replace_PartialXattrSpec) {
    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocReplace,
                       name,
                       "doc.author",
                       "\"Bart\"",
                       PathFlag::XattrPath);
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, resp.getStatus());
    resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictAdd,
                  name,
                  "doc",
                  R"({"author":"Bart","rev":0})",
                  PathFlag::XattrPath | PathFlag::Mkdir_p);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    resp = subdoc(cb::mcbp::ClientOpcode::SubdocReplace,
                  name,
                  "doc.author",
                  "\"Jones\"",
                  PathFlag::XattrPath);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    resp = subdoc_get("doc", PathFlag::XattrPath);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    EXPECT_EQ("{\"author\":\"Jones\",\"rev\":0}", resp.getDataView());
}

/**
 * Appends an array element to the root of the given XATTR.
 */
TEST_P(XattrTest, ArrayPushLast_FullXattrSpec) {
    doArrayPushLastTest("authors");
}

/**
 * Appends an array element specified by X-Path to the given X-Key.
 */
TEST_P(XattrTest, ArrayPushLast_PartialXattrSpec) {
    doArrayPushLastTest("doc.authors");
}

/**
 * Appends an array element to the root of the given XATTR.
 */
TEST_P(XattrTest, ArrayPushFirst_FullXattrSpec) {
    doArrayPushFirstTest("authors");
}

/**
 * Prepends an array element specified by X-Path to the given X-Key.
 */
TEST_P(XattrTest, ArrayPushFirst_PartialXattrSpec) {
    doArrayPushFirstTest("doc.authors");
}

/**
 * Inserts an array element specified by X-Path to the given X-Key.
 */
TEST_P(XattrTest, ArrayInsert_FullXattrSpec) {
    doArrayInsertTest("doc.");
    // It should also work for just "foo[0]"
    doArrayInsertTest("foo");
}

/**
 * Inserts an array element specified by X-Path to the given X-Key.
 */
TEST_P(XattrTest, ArrayInsert_PartialXattrSpec) {
    doArrayInsertTest("doc.authors");
}

/**
 * Adds an array element specified to the root of the given X-Key,
 * iff that element doesn't already exist in the root.
 */
TEST_P(XattrTest, ArrayAddUnique_FullXattrSpec) {
    doAddUniqueTest("doc");
}

/**
 * Adds an array element specified by X-Path to the given X-Key,
 * iff that element doesn't already exist in the array.
 */
TEST_P(XattrTest, ArrayAddUnique_PartialXattrSpec) {
    doAddUniqueTest("doc.authors");
}

/**
 * Increments/decrements the value at the root of the given X-Key
 */
TEST_P(XattrTest, Counter_FullXattrSpec) {
    doCounterTest("doc");
}

/**
 * Increments/decrements the value at the given X-Path of the given X-Key.
 */
TEST_P(XattrTest, Counter_PartialXattrSpec) {
    doCounterTest("doc.counter");
}

////////////////////////////////////////////////////////////////////////
//  Verify that I can't do subdoc ops if it's not enabled by hello
////////////////////////////////////////////////////////////////////////
TEST_P(XattrDisabledTest, VerifyNotEnabled) {
    userConnection->setXattrSupport(false);

    // All of the subdoc commands end up using the same method
    // to validate the xattr portion of the command so we'll just
    // check one
    BinprotSubdocCommand cmd;
    cmd.setOp(ClientOpcode::SubdocDictAdd);
    cmd.setKey(name);
    cmd.setPath("_sync.deleted");
    cmd.setValue("true");
    cmd.addPathFlags(PathFlag::XattrPath | PathFlag::Mkdir_p);
    cmd.addDocFlags(DocFlag::AccessDeleted | DocFlag::Mkdoc);

    const auto resp = userConnection->execute(cmd);
    ASSERT_EQ(cb::mcbp::Status::NotSupported, resp.getStatus());
}

TEST_P(XattrTest, MB_22691) {
    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                       name,
                       "integer_extra",
                       "1",
                       PathFlag::XattrPath | PathFlag::Mkdir_p);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                  name,
                  "integer",
                  "2",
                  PathFlag::XattrPath | PathFlag::Mkdir_p);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus()) << resp.getStatus();
}

TEST_P(XattrTest, MB_23882_VirtualXattrs) {
    // MB-32147: Testing last_modified requires at least 1 item to have been
    // flushed.
    storeAndPersistItem(*userConnection, Vbid(0), "flushed_key");

    // Test to check that we can get both an xattr and the main body in
    // subdoc multi-lookup
    setBodyAndXattr(value, {{sysXattr, xattrVal}});

    // Sanity checks and setup done lets try the multi-lookup
    auto multiResp = subdoc_multi_lookup(
            {{ClientOpcode::SubdocGet, PathFlag::XattrPath, "$document"},
             {ClientOpcode::SubdocGet, PathFlag::XattrPath, "$document.CAS"},
             {ClientOpcode::SubdocGet, PathFlag::XattrPath, "$document.foobar"},
             {ClientOpcode::SubdocGet, PathFlag::XattrPath, "_sync.eg"}});
    const auto results = multiResp.getResults();

    EXPECT_EQ(cb::mcbp::Status::SubdocMultiPathFailure, multiResp.getStatus());
    EXPECT_EQ(cb::mcbp::Status::Success, results[0].status);

    // Ensure that we found all we expected and they're of the correct type:
    auto json = nlohmann::json::parse(results[0].value);
    EXPECT_TRUE(json["CAS"].is_string());
    EXPECT_TRUE(json["vbucket_uuid"].is_string());
    EXPECT_TRUE(json["seqno"].is_string());
    EXPECT_TRUE(json["revid"].is_string());
    EXPECT_TRUE(json["exptime"].is_number());
    EXPECT_TRUE(json["value_bytes"].is_number());
    EXPECT_TRUE(json["deleted"].is_boolean());
    EXPECT_TRUE(json["flags"].is_number());
    EXPECT_NE(json.end(), json.find("last_modified"));
    EXPECT_STREQ("string", json["last_modified"].type_name());

    // Verify exptime is showing as 0 (document has no expiry)
    EXPECT_EQ(0, json["exptime"].get<int>());

    // Verify that the flags is correct
    EXPECT_EQ(0xcaffee, json["flags"].get<int>());

    // verify that the datatype is correctly encoded and contains
    // the correct bits
    auto datatype = json["datatype"];
    ASSERT_TRUE(datatype.is_array());
    bool found_xattr = false;
    bool found_json = false;

    for (const auto& tag : datatype) {
        if (tag.get<std::string>() == "xattr") {
            found_xattr = true;
        } else if (tag.get<std::string>() == "json") {
            found_json = true;
        } else if (tag.get<std::string>() == "snappy") {
            // Not currently checked; default engine doesn't support
            // storing as Snappy (will inflate) so not trivial to assert
            // when this should be true.
        } else {
            FAIL() << "Unexpected datatype: " << tag.get<std::string>();
        }
    }
    EXPECT_TRUE(found_json);
    EXPECT_TRUE(found_xattr);

    // Verify that we got a partial from the second one
    EXPECT_EQ(cb::mcbp::Status::Success, results[1].status);
    const std::string cas{std::string{"\""} + json["CAS"].get<std::string>() +
                          "\""};
    json = nlohmann::json::parse(results[1].value);
    EXPECT_EQ(cas, json.dump());

    // The third one didn't exist
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, results[2].status);

    // Expect that we could find _sync.eg
    EXPECT_EQ(cb::mcbp::Status::Success, results[3].status);
    EXPECT_EQ("99", results[3].value);
}

TEST_P(XattrTest, MB_23882_VirtualXattrs_GetXattrAndBody) {
    // Test to check that we can get both an xattr and the main body in
    // subdoc multi-lookup
    setBodyAndXattr(value, {{sysXattr, xattrVal}});

    // Sanity checks and setup done lets try the multi-lookup
    auto multiResp = subdoc_multi_lookup(
            {{cb::mcbp::ClientOpcode::SubdocGet,
              PathFlag::XattrPath,
              "$document.deleted"},
             {cb::mcbp::ClientOpcode::Get, PathFlag::None, ""}});

    EXPECT_EQ(cb::mcbp::Status::Success, multiResp.getStatus());
    const auto results = multiResp.getResults();
    EXPECT_EQ("false", results[0].value);
    EXPECT_EQ(value, results[1].value);
}

TEST_P(XattrTest, MB_23882_VirtualXattrs_IsReadOnly) {
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                    PathFlag::XattrPath | PathFlag::Mkdir_p,
                    "$document.CAS",
                    "foo");
    cmd.addMutation(cb::mcbp::ClientOpcode::Set, PathFlag::None, "", value);

    const auto resp = userConnection->execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::SubdocXattrCantModifyVattr, resp.getStatus());
}

TEST_P(XattrTest, MB_23882_VirtualXattrs_UnknownVattr) {
    auto multiResp =
            subdoc_multi_lookup({{cb::mcbp::ClientOpcode::SubdocGet,
                                  PathFlag::XattrPath,
                                  "$documents"}}); // should be $document

    EXPECT_EQ(cb::mcbp::Status::SubdocMultiPathFailure, multiResp.getStatus());
    const auto results = multiResp.getResults();
    EXPECT_EQ(cb::mcbp::Status::SubdocXattrUnknownVattr, results[0].status);
}

TEST_P(XattrTest, MB_25786_XTOC_Vattr_XattrReadPrivOnly) {
    verify_xtoc_user_system_xattr();
    userConnection->dropPrivilege(cb::rbac::Privilege::SystemXattrRead);

    BinprotSubdocMultiLookupCommand cmd;
    cmd.setKey(name);
    cmd.addGet("$XTOC", PathFlag::XattrPath);
    cmd.addLookup("", cb::mcbp::ClientOpcode::Get, PathFlag::None);

    const auto multiResp =
            BinprotSubdocMultiLookupResponse(userConnection->execute(cmd));
    ASSERT_EQ(cb::mcbp::Status::Success, multiResp.getStatus());
    const auto results = multiResp.getResults();
    EXPECT_EQ(cb::mcbp::Status::Success, results[0].status);
    EXPECT_EQ(R"(["userXattr"])", results[0].value);
    rebuildUserConnection(TestappXattrClientTest::isTlsEnabled());
}

TEST_P(XattrTest, MB_25786_XTOC_Vattr_XattrSystemReadPriv) {
    verify_xtoc_user_system_xattr();
    BinprotSubdocMultiLookupCommand cmd;
    cmd.setKey(name);
    cmd.addGet("$XTOC", PathFlag::XattrPath);
    cmd.addLookup("", cb::mcbp::ClientOpcode::Get, PathFlag::None);

    const auto multiResp =
            BinprotSubdocMultiLookupResponse(userConnection->execute(cmd));
    ASSERT_EQ(cb::mcbp::Status::Success, multiResp.getStatus());
    const auto results = multiResp.getResults();
    EXPECT_EQ(cb::mcbp::Status::Success, results[0].status);
    EXPECT_EQ(R"(["_sync","userXattr"])", results[0].value);
    rebuildUserConnection(TestappXattrClientTest::isTlsEnabled());
}

TEST_P(XattrTest, MB_25786_XTOC_VattrNoXattrs) {
    std::string value = R"({"Test":45})";
    Document document;
    document.info.cas = cb::mcbp::cas::Wildcard;
    document.info.flags = 0xcaffee;
    document.info.id = name;
    document.value = value;
    userConnection->mutate(document, Vbid(0), MutationType::Set);
    auto doc = userConnection->get(name, Vbid(0));

    EXPECT_EQ(doc.value, document.value);

    auto resp = subdoc_get("$XTOC", PathFlag::XattrPath);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    EXPECT_EQ("[]", resp.getDataView());
}

TEST_P(XattrTest, MB_25562_IncludeValueCrc32cInDocumentVAttr) {
    // I want to test that the expected document value checksum is
    // returned as part of the '$document' Virtual XAttr.

    // Create a docuement with a given value
    Vbid vbid = Vbid(0);
    if (std::get<2>(GetParam()) == ClientJSONSupport::Yes) {
        document.info.datatype = cb::mcbp::Datatype::JSON;
        document.value = R"({"Test":45})";
    } else {
        document.info.datatype = cb::mcbp::Datatype::Raw;
        document.value = "raw value";
    }
    userConnection->mutate(document, vbid, MutationType::Set);
    EXPECT_EQ(document.value,
              userConnection->get(document.info.id, vbid).value);

    // Add an XAttr to the document.
    // We want to check that the checksum computed by the server takes in
    // input only the document value (XAttrs excluded)
    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                       name,
                       "userXattr",
                       R"({"a":1})",
                       PathFlag::XattrPath | PathFlag::Mkdir_p);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    resp = subdoc_get("userXattr", PathFlag::XattrPath);
    EXPECT_EQ(R"({"a":1})", resp.getDataView());

    // Compute the expected value checksum
    auto _crc32c = crc32c(document.value);
    auto expectedValueCrc32c = "\"" + cb::to_hex(_crc32c) + "\"";

    // Get and verify the actual value checksum
    BinprotSubdocMultiLookupCommand cmd;
    cmd.setKey(document.info.id);
    cmd.addGet("$document.value_crc32c", PathFlag::XattrPath);
    const auto multiResp =
            BinprotSubdocMultiLookupResponse(userConnection->execute(cmd));
    ASSERT_EQ(cb::mcbp::Status::Success, multiResp.getStatus());
    const auto results = multiResp.getResults();
    EXPECT_EQ(cb::mcbp::Status::Success, results[0].status);
    EXPECT_EQ(expectedValueCrc32c, results[0].value);
}

TEST_P(XattrTest, MB_25562_StampValueCrc32cInUserXAttr) {
    // I want to test that the expansion of macro '${Mutation.value_crc32c}'
    // sets the correct value checksum into the given user XAttr

    // Store the macro and verify that it is not expanded without the
    // PathFlag::ExpandMacros flag.
    // Note: as the document will contain an XAttr, we prove also that the
    // checksum computed by the server takes in input only the document
    // value (XAttrs excluded).
    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                       name,
                       "_sync.value_crc32c",
                       "\"${Mutation.value_crc32c}\"",
                       PathFlag::XattrPath | PathFlag::Mkdir_p);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    resp = subdoc_get("_sync.value_crc32c", PathFlag::XattrPath);
    EXPECT_EQ("\"${Mutation.value_crc32c}\"", resp.getDataView());

    // Now change the user xattr to macro expansion
    resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                  name,
                  "_sync.value_crc32c",
                  "\"${Mutation.value_crc32c}\"",
                  PathFlag::XattrPath | PathFlag::ExpandMacros);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

    // Compute the expected value_crc32c
    auto value = userConnection->get(name, Vbid(0)).value;
    auto _crc32c = crc32c(value);
    auto expectedValueCrc32c = "\"" + cb::to_hex(_crc32c) + "\"";

    // Fetch the xattr and verify that the macro expanded to the
    // expected body checksum
    resp = subdoc_get("_sync.value_crc32c", PathFlag::XattrPath);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    EXPECT_EQ(expectedValueCrc32c, resp.getDataView());

    // Repeat the check fetching the entire '_sync' path. Differently from the
    // check above, this check exposed issues in macro padding.
    resp = subdoc_get("_sync", PathFlag::XattrPath);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    EXPECT_EQ("{\"value_crc32c\":" + expectedValueCrc32c + "}",
              resp.getDataView());
}

// Test that one can fetch both the body and an XATTR on a deleted document.
TEST_P(XattrTest, MB24152_GetXattrAndBodyDeleted) {
    setBodyAndXattr(value, {{sysXattr, xattrVal}});

    auto multiResp = subdoc_multi_lookup(
            {{cb::mcbp::ClientOpcode::SubdocGet, PathFlag::XattrPath, sysXattr},
             {cb::mcbp::ClientOpcode::Get, PathFlag::None, ""}},
            cb::mcbp::subdoc::DocFlag::AccessDeleted);

    ASSERT_EQ(cb::mcbp::Status::Success, multiResp.getStatus());
    const auto results = multiResp.getResults();
    EXPECT_EQ(xattrVal, results[0].value);
    EXPECT_EQ(value, results[1].value);
}

// Test that attempting to get an XATTR and a Body when the XATTR doesn't exist
// (partially) succeeds - the body is returned.
TEST_P(XattrTest, MB24152_GetXattrAndBodyWithoutXattr) {

    // Create a document without an XATTR.
    userConnection->store(name, Vbid(0), value);

    // Attempt to request both the body and a non-existent XATTR.
    auto multiResp = subdoc_multi_lookup(
            {{cb::mcbp::ClientOpcode::SubdocGet, PathFlag::XattrPath, sysXattr},
             {cb::mcbp::ClientOpcode::Get, PathFlag::None, ""}},
            cb::mcbp::subdoc::DocFlag::AccessDeleted);

    EXPECT_EQ(cb::mcbp::Status::SubdocMultiPathFailure, multiResp.getStatus());
    const auto results = multiResp.getResults();
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, results[0].status);
    EXPECT_EQ("", results[0].value);
    EXPECT_EQ(cb::mcbp::Status::Success, results[1].status);
    EXPECT_EQ(value, results[1].value);
}

// Test that attempting to get an XATTR and a Body when the doc is deleted and
// empty (partially) succeeds - the XATTR is returned.
TEST_P(XattrTest, MB24152_GetXattrAndBodyDeletedAndEmpty) {
    // Store a document with body+XATTR; then delete it (so the body
    // becomes empty).
    setBodyAndXattr(value, {{sysXattr, xattrVal}});
    userConnection->remove(name, Vbid(0));

    auto multiResp = subdoc_multi_lookup(
            {{cb::mcbp::ClientOpcode::SubdocGet, PathFlag::XattrPath, sysXattr},
             {cb::mcbp::ClientOpcode::Get, PathFlag::None, ""}},
            cb::mcbp::subdoc::DocFlag::AccessDeleted);

    EXPECT_EQ(cb::mcbp::Status::SubdocMultiPathFailureDeleted,
              multiResp.getStatus());
    const auto results = multiResp.getResults();
    EXPECT_EQ(cb::mcbp::Status::Success, results[0].status);
    EXPECT_EQ(xattrVal, results[0].value);
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, results[1].status);
    EXPECT_EQ("", results[1].value);
}

// Test that attempting to get an XATTR and a Body when the body is non-JSON
// succeeds.
TEST_P(XattrTest, MB24152_GetXattrAndBodyNonJSON) {
    // Store a document with a non-JSON body + XATTR.
    value = "non-JSON value";
    setBodyAndXattr(value, {{sysXattr, xattrVal}});

    auto multiResp = subdoc_multi_lookup(
            {{cb::mcbp::ClientOpcode::SubdocGet, PathFlag::XattrPath, sysXattr},
             {cb::mcbp::ClientOpcode::Get, PathFlag::None, ""}},
            cb::mcbp::subdoc::DocFlag::AccessDeleted);

    ASSERT_EQ(cb::mcbp::Status::Success, multiResp.getStatus());
    const auto results = multiResp.getResults();
    EXPECT_EQ(cb::mcbp::Status::Success, results[0].status);
    EXPECT_EQ(xattrVal, results[0].value);
    EXPECT_EQ(cb::mcbp::Status::Success, results[1].status);
    EXPECT_EQ(value, results[1].value);
}

// Test that the $vbucket VATTR can be accessed and all properties are present
TEST_P(XattrTest, MB_35388_VATTR_Vbucket) {
    // Test to check that we can get both an xattr and the main body in
    // subdoc multi-lookup
    setBodyAndXattr(value, {{sysXattr, xattrVal}});

    // Sanity checks and setup done lets try the multi-lookup
    auto multiResp = subdoc_multi_lookup({
            {ClientOpcode::SubdocGet, PathFlag::XattrPath, "$vbucket"},
            {ClientOpcode::SubdocGet, PathFlag::XattrPath, "$vbucket.HLC"},
            {ClientOpcode::SubdocGet, PathFlag::XattrPath, "$vbucket.HLC.now"},
            {ClientOpcode::SubdocGet, PathFlag::XattrPath, "_sync.eg"},
    });
    const auto results = multiResp.getResults();
    EXPECT_EQ(cb::mcbp::Status::Success, multiResp.getStatus());
    EXPECT_EQ(cb::mcbp::Status::Success, results[0].status);

    // Ensure that we found all we expected and they're of the correct type:
    auto json0 = nlohmann::json::parse(results[0].value);
    EXPECT_EQ(1, json0.size());
    EXPECT_TRUE(json0.at("HLC").is_object());

    auto json1 = nlohmann::json::parse(results[1].value);
    EXPECT_TRUE(json1.is_object());
    EXPECT_EQ(2, json1.size());
    EXPECT_TRUE(json1.at("now").is_string());
    EXPECT_TRUE(json1.at("mode").is_string());

    auto json2 = nlohmann::json::parse(results[2].value);
    EXPECT_TRUE(json2.is_string());

    // Expect that we could find another XATTR (_sync.eg)
    EXPECT_EQ(cb::mcbp::Status::Success, results[3].status);
    EXPECT_EQ("99", results[3].value);
}

// Test that the $vbucket and $document VATTR can be accessed at the same time,
// along with a normal (system) XATTR
TEST_P(XattrTest, MB_35388_VATTR_Document_Vbucket) {
    setBodyAndXattr(value, {{sysXattr, xattrVal}});

    auto multiResp = subdoc_multi_lookup({
            {ClientOpcode::SubdocGet, PathFlag::XattrPath, "$document"},
            {ClientOpcode::SubdocGet, PathFlag::XattrPath, "$vbucket"},
            {ClientOpcode::SubdocGet, PathFlag::XattrPath, "_sync.eg"},
    });
    const auto results = multiResp.getResults();
    EXPECT_EQ(cb::mcbp::Status::Success, multiResp.getStatus());
    EXPECT_EQ(3, results.size());
    for (const auto& result : results) {
        EXPECT_EQ(cb::mcbp::Status::Success, result.status);
    }
    EXPECT_EQ("99", results[2].value);
}

// Test that the $vbucket.HLC.now VATTR is at least as large as the
// last_modified time for the last document written.
TEST_P(XattrTest, MB_35388_VbucketHlcNowIsValid) {
    setBodyAndXattr(value, {{sysXattr, xattrVal}});

    auto multiResp = subdoc_multi_lookup({
            {cb::mcbp::ClientOpcode::SubdocGet,
             PathFlag::XattrPath,
             "$document.last_modified"},
            {cb::mcbp::ClientOpcode::SubdocGet,
             PathFlag::XattrPath,
             "$vbucket.HLC.mode"},
            {cb::mcbp::ClientOpcode::SubdocGet,
             PathFlag::XattrPath,
             "$vbucket.HLC.now"},
    });
    const auto results = multiResp.getResults();
    EXPECT_EQ(cb::mcbp::Status::Success, multiResp.getStatus());
    EXPECT_EQ(3, results.size());

    auto lastModifiedJSON = nlohmann::json::parse(results[0].value);
    EXPECT_TRUE(lastModifiedJSON.is_string());
    uint64_t lastModified = std::stoull(lastModifiedJSON.get<std::string>());

    EXPECT_TRUE(results[1].value == "\"real\"" ||
                results[1].value == "\"logical\"");

    auto hlcJSON = nlohmann::json::parse(results[2].value);
    EXPECT_TRUE(hlcJSON.is_string());
    uint64_t hlc = std::stoull(hlcJSON.get<std::string>());

    EXPECT_GE(hlc, lastModified);
    EXPECT_GT(lastModified + 60, hlc)
            << "Expected difference between document being written and HLC.now "
               "being read to be under 60s";
}

// Test that a partial failure on a multi-lookup on a deleted document returns
// SUBDOC_MULTI_PATH_FAILURE_DELETED
TEST_P(XattrTest, MB23808_MultiPathFailureDeleted) {
    // Store an initial body+XATTR; then delete it.
    setBodyAndXattr(value, {{sysXattr, xattrVal}});
    userConnection->remove(name, Vbid(0));

    // Lookup two XATTRs - one which exists and one which doesn't.
    auto multiResp = subdoc_multi_lookup(
            {{ClientOpcode::SubdocGet, PathFlag::XattrPath, sysXattr},
             {ClientOpcode::SubdocGet,
              PathFlag::XattrPath,
              "_sync.non_existant"}},
            cb::mcbp::subdoc::DocFlag::AccessDeleted);

    // We expect to successfully access the first (existing) XATTR; but not
    // the second.
    EXPECT_EQ(cb::mcbp::Status::SubdocMultiPathFailureDeleted,
              multiResp.getStatus());
    const auto results = multiResp.getResults();
    EXPECT_EQ(cb::mcbp::Status::Success, results[0].status);
    EXPECT_EQ(xattrVal, results[0].value);
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, results[1].status);
}

TEST_P(XattrTest, SetXattrAndDeleteBasic) {
    setBodyAndXattr(value, {{sysXattr, "55"}});
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                    PathFlag::XattrPath,
                    sysXattr,
                    xattrVal);
    cmd.addMutation(cb::mcbp::ClientOpcode::Delete, PathFlag::None, "", "");

    EXPECT_EQ(cb::mcbp::Status::Success,
              userConnection->execute(cmd).getStatus());

    // Should now only be XATTR datatype
    auto meta = get_meta();
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_XATTR, meta.getDatatype());
    // Should also be marked as deleted
    EXPECT_EQ(1, meta.getDeleted());

    auto resp = subdoc_get(sysXattr,
                           PathFlag::XattrPath,
                           cb::mcbp::subdoc::DocFlag::AccessDeleted);
    EXPECT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());
    EXPECT_EQ(xattrVal, resp.getDataView());

    // Check we can't access the deleted document
    resp = subdoc_get(sysXattr, PathFlag::XattrPath);
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, resp.getStatus());

    auto getResp = subdoc_multi_lookup(
            {{cb::mcbp::ClientOpcode::Get, PathFlag::None, ""}});
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, getResp.getStatus());

    // Worth noting the difference in the way it fails if AccessDeleted is set.
    getResp = subdoc_multi_lookup(
            {{cb::mcbp::ClientOpcode::Get, PathFlag::None, ""}},
            cb::mcbp::subdoc::DocFlag::AccessDeleted);
    EXPECT_EQ(cb::mcbp::Status::SubdocMultiPathFailureDeleted,
              getResp.getStatus());
    const auto results = getResp.getResults();
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, results[0].status);
}

TEST_P(XattrTest, SetXattrAndDeleteCheckUserXattrsDeleted) {
    setBodyAndXattr(value, {{sysXattr, xattrVal}});
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                    PathFlag::XattrPath,
                    "userXattr",
                    "66");
    cmd.addMutation(cb::mcbp::ClientOpcode::Delete, PathFlag::None, "", "");

    EXPECT_EQ(cb::mcbp::Status::Success,
              userConnection->execute(cmd).getStatus());

    // Should now only be XATTR datatype
    auto meta = get_meta();
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_XATTR, meta.getDatatype());
    // Should also be marked as deleted
    EXPECT_EQ(1, meta.getDeleted());

    auto resp = subdoc_get("userXattr", PathFlag::XattrPath);
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, resp.getStatus());

    // The delete should delete user Xattrs as well as the body, leaving only
    // system Xattrs
    resp = subdoc_get("userXattr",
                      PathFlag::XattrPath,
                      cb::mcbp::subdoc::DocFlag::AccessDeleted);
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, resp.getStatus());

    // System Xattr should still be there so lets check it
    resp = subdoc_get(sysXattr,
                      PathFlag::XattrPath,
                      cb::mcbp::subdoc::DocFlag::AccessDeleted);
    EXPECT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());
    EXPECT_EQ(xattrVal, resp.getDataView());
}

TEST_P(XattrTest, SetXattrAndDeleteJustUserXattrs) {
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(ClientOpcode::SubdocDictUpsert,
                    PathFlag::XattrPath,
                    "userXattr",
                    "66");
    cmd.addMutation(ClientOpcode::Set, PathFlag::None, "", "{\"Field\": 88}");

    EXPECT_EQ(cb::mcbp::Status::Success,
              userConnection->execute(cmd).getStatus());

    cmd.clearMutations();
    cmd.addMutation(ClientOpcode::Delete, PathFlag::None, "", "");
    EXPECT_EQ(cb::mcbp::Status::Success,
              userConnection->execute(cmd).getStatus());
}

TEST_P(XattrTest, TestXattrDeleteDatatypes) {
    // See MB-25422. We test to make sure that the datatype of a document is
    // correctly altered after a soft delete.
    setBodyAndXattr(value, {{sysXattr, "55"}});
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(ClientOpcode::SubdocDictUpsert,
                    PathFlag::XattrPath,
                    sysXattr,
                    xattrVal);
    cmd.addMutation(ClientOpcode::Delete, PathFlag::None, "", "");

    EXPECT_EQ(cb::mcbp::Status::Success,
              userConnection->execute(cmd).getStatus());

    // Should now only be XATTR datatype
    auto meta = get_meta();
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_XATTR, meta.getDatatype());
    // Should also be marked as deleted
    EXPECT_EQ(1, meta.getDeleted());
}

// Verify that when a document with a user XATTR is deleted, then the
// user XATTR is pruned leaving the body empty.
TEST_P(XattrTest, UserXAttrPrunedOnDelete) {
    setBodyAndXattr(value, {{"user_XATTR", "123"}});

    userConnection->remove(name, Vbid(0));

    // Should now be a deleted document with no body.
    auto meta = get_meta();
    EXPECT_EQ(PROTOCOL_BINARY_RAW_BYTES, meta.getDatatype());
    EXPECT_EQ(1, meta.getDeleted());
}

/**
 * Users xattrs should be stored inside the user data, which means
 * that if one tries to add xattrs to a document which is at the
 * max size you can't add any additional xattrs
 */
TEST_P(XattrTest, mb25928_UserCantExceedDocumentLimit) {
    std::string blob(GetTestBucket().getMaximumDocSize(), '\0');
    userConnection->store("mb25928", Vbid(0), std::move(blob));

    std::string value(300, 'a');
    value.front() = '"';
    value.back() = '"';

    BinprotSubdocCommand cmd;
    cmd.setOp(cb::mcbp::ClientOpcode::SubdocDictUpsert);
    cmd.setKey("mb25928");
    cmd.setPath("user.long_string");
    cmd.setValue(value);
    cmd.addPathFlags(PathFlag::XattrPath | PathFlag::Mkdir_p);
    cmd.addDocFlags(cb::mcbp::subdoc::DocFlag::None);

    const auto resp = userConnection->execute(cmd);
    EXPECT_FALSE(resp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::E2big, resp.getStatus());
}

/**
 * System xattrs should be stored in a separate 1MB chunk (in addition to
 * the users normal document limit). Verify that we can add a system xattr
 * on a document which is at the max size
 */
TEST_P(XattrTest, mb25928_SystemCanExceedDocumentLimit) {
    std::string blob(GetTestBucket().getMaximumDocSize(), '\0');
    userConnection->store("mb25928", Vbid(0), std::move(blob));

    // Let it be almost 1MB (the internal length fields and keys
    // is accounted for in the system space
    std::string value(1024 * 1024 - 40, 'a');
    value.front() = '"';
    value.back() = '"';

    BinprotSubdocCommand cmd;
    cmd.setOp(cb::mcbp::ClientOpcode::SubdocDictUpsert);
    cmd.setKey("mb25928");
    cmd.setPath("_system.long_string");
    cmd.setValue(value);
    cmd.addPathFlags(PathFlag::XattrPath | PathFlag::Mkdir_p);
    cmd.addDocFlags(cb::mcbp::subdoc::DocFlag::None);

    const auto resp = userConnection->execute(cmd);
    EXPECT_TRUE(resp.isSuccess())
                << "Expected to be able to store system xattrs";
}

/**
 * System xattrs should be stored in a separate 1MB chunk (in addition to
 * the users normal document limit). Verify that we can't add system xattrs
 * which exceeds this limit.
 */
TEST_P(XattrTest, mb25928_SystemCantExceedSystemLimit) {
    std::string blob(GetTestBucket().getMaximumDocSize(), '\0');
    userConnection->store("mb25928", Vbid(0), std::move(blob));

    std::string value(1024 * 1024, 'a');
    value.front() = '"';
    value.back() = '"';

    BinprotSubdocCommand cmd;
    cmd.setOp(cb::mcbp::ClientOpcode::SubdocDictUpsert);
    cmd.setKey("mb25928");
    cmd.setPath("_system.long_string");
    cmd.setValue(value);
    cmd.addPathFlags(PathFlag::XattrPath | PathFlag::Mkdir_p);
    cmd.addDocFlags(cb::mcbp::subdoc::DocFlag::None);

    const auto resp = userConnection->execute(cmd);
    EXPECT_FALSE(resp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::E2big, resp.getStatus())
            << "The system space is max 1M";
}

// Test replacing a compressed/uncompressed value with an uncompressed
// value. XATTRs should be correctly merged.
TEST_P(XattrTest, MB_28524_TestReplaceWithXattrUncompressed) {
    doReplaceWithXattrTest(false);
}

// Test replacing a compressed/uncompressed value with a compressed
// value. XATTRs should be correctly merged.
TEST_P(XattrTest, MB_28524_TestReplaceWithXattrCompressed) {
    doReplaceWithXattrTest(true);
}

/**
 * If the client tries to fetch a mix of multiple virtual xattrs it might not
 * work as expected.  Ex:
 *
 * $document
 * tnx
 * will work, but
 *
 * tnx
 * $document
 * will not work. In addition
 *
 * $XTOC
 * tnx
 * returns "null" for the toc
 */
TEST_P(XattrTest, MB35079_virtual_xattr_mix) {
    // Store the document we want to operate on
    {
        BinprotSubdocMultiMutationCommand cmd;
        cmd.setKey(name);
        cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                        PathFlag::XattrPath,
                        "tnx",
                        "{\"id\":666}");
        cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                        PathFlag::Mkdir_p,
                        "value",
                        R"("value")");
        ASSERT_EQ(cb::mcbp::Status::Success,
                  userConnection->execute(cmd).getStatus());
    }

    // Problem 1 : The order of the virtual xattr and key matters:
    {
        auto resp = subdoc_multi_lookup(
                {{ClientOpcode::SubdocGet, PathFlag::XattrPath, "tnx"},
                 {ClientOpcode::SubdocGet,
                  PathFlag::XattrPath,
                  "$document.CAS"},
                 {ClientOpcode::SubdocGet, PathFlag::None, "value"}});
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        const auto results = resp.getResults();
        EXPECT_EQ(cb::mcbp::Status::Success, results[0].status);
        EXPECT_EQ(R"({"id":666})", results[0].value);
        EXPECT_EQ(cb::mcbp::Status::Success, results[1].status);
        // We can't really check the CAS, but it should be a hex value
        EXPECT_EQ(0, results[1].value.find("\"0x"));
        EXPECT_EQ(cb::mcbp::Status::Success, results[2].status);
        EXPECT_EQ(R"("value")", results[2].value);
    }
    // Try it with cas first and then the xattr
    {
        auto resp = subdoc_multi_lookup(
                {{ClientOpcode::SubdocGet,
                  PathFlag::XattrPath,
                  "$document.CAS"},
                 {ClientOpcode::SubdocGet, PathFlag::XattrPath, "tnx"},
                 {ClientOpcode::SubdocGet, PathFlag::None, "value"}});
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        const auto results = resp.getResults();
        EXPECT_EQ(cb::mcbp::Status::Success, results[0].status);
        // We can't really check the CAS, but it should be a hex value
        EXPECT_EQ(0, results[0].value.find("\"0x"));
        EXPECT_EQ(cb::mcbp::Status::Success, results[1].status);
        EXPECT_EQ(R"({"id":666})", results[1].value);
        EXPECT_EQ(cb::mcbp::Status::Success, results[2].status);
        EXPECT_EQ(R"("value")", results[2].value);
    }

    // Problem 2 : Requesting XTOC with another xattr fails to populate XTOC
    {
        auto resp = subdoc_multi_lookup(
                {{ClientOpcode::SubdocGet, PathFlag::XattrPath, "$XTOC"},
                 {ClientOpcode::SubdocGet, PathFlag::XattrPath, "tnx"},
                 {ClientOpcode::SubdocGet, PathFlag::None, "value"}});
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        const auto results = resp.getResults();
        EXPECT_EQ(cb::mcbp::Status::Success, results[0].status);
        EXPECT_EQ(R"(["tnx"])", results[0].value);
        EXPECT_EQ(cb::mcbp::Status::Success, results[1].status);
        EXPECT_EQ(R"({"id":666})", results[1].value);
        EXPECT_EQ(cb::mcbp::Status::Success, results[2].status);
        EXPECT_EQ(R"("value")", results[2].value);
    }
}

TEST_P(XattrTest, MB40980_InputMacroExpansion) {
    // Verify that hello reports the feature
    userConnection->setFeature(cb::mcbp::Feature::SubdocDocumentMacroSupport,
                               true);

    // Fetch the original $documents values
    nlohmann::json original;
    {
        auto resp = subdoc_multi_lookup(
                {{ClientOpcode::SubdocGet, PathFlag::XattrPath, "$document"}});
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        const auto results = resp.getResults();
        ASSERT_EQ(cb::mcbp::Status::Success, results[0].status);
        original = nlohmann::json::parse(results[0].value);
    }

    // Insert a tnx field with the new input macro's
    {
        BinprotSubdocMultiMutationCommand cmd;
        cmd.setKey(name);
        cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                        PathFlag::XattrPath | PathFlag::ExpandMacros |
                                PathFlag::Mkdir_p,
                        "tnx.CAS",
                        "\"${$document.CAS}\"");
        cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                        PathFlag::XattrPath | PathFlag::ExpandMacros,
                        "tnx.exptime",
                        "\"${$document.exptime}\"");
        cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                        PathFlag::XattrPath | PathFlag::ExpandMacros,
                        "tnx.revid",
                        "\"${$document.revid}\"");
        cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                        PathFlag::XattrPath | PathFlag::ExpandMacros,
                        "tnx.doc",
                        "\"${$document}\"");

        ASSERT_EQ(cb::mcbp::Status::Success,
                  userConnection->execute(cmd).getStatus())
                << "Failed to update the document to expand the Input macros";
    }

    // request the $document and tnx field and verify that
    // 1. the $document.CAS differs
    // 2. The tnx inputs match the values in the original $document
    nlohmann::json modified;
    nlohmann::json tnx;
    {
        auto resp = subdoc_multi_lookup(
                {{ClientOpcode::SubdocGet, PathFlag::XattrPath, "$document"},
                 {ClientOpcode::SubdocGet, PathFlag::XattrPath, "tnx"}});
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        const auto results = resp.getResults();
        ASSERT_EQ(cb::mcbp::Status::Success, results[0].status);
        modified = nlohmann::json::parse(results[0].value);

        ASSERT_EQ(cb::mcbp::Status::Success, results[1].status);
        tnx = nlohmann::json::parse(results[1].value);
    }

    EXPECT_TRUE(tnx["CAS"].is_string());
    EXPECT_TRUE(tnx["exptime"].is_number());
    EXPECT_TRUE(tnx["revid"].is_string());

    EXPECT_NE(cb::from_hex(original["CAS"].get<std::string>()),
              cb::from_hex(modified["CAS"].get<std::string>()));
    EXPECT_EQ(original["exptime"].get<uint64_t>(),
              tnx["exptime"].get<uint64_t>());
    EXPECT_EQ(cb::from_hex(original["CAS"].get<std::string>()),
              cb::from_hex(tnx["CAS"].get<std::string>()));
    EXPECT_EQ(original["revid"], tnx["revid"]);

    EXPECT_EQ(original, tnx["doc"]);
}

/// Verify that we can replace the content of the body with a value in an
/// xattr
TEST_P(XattrTest, ReplaceBodyWithXattr) {
    {
        BinprotSubdocMultiMutationCommand cmd;
        cmd.setKey(name);
        cmd.addMutation(
                cb::mcbp::ClientOpcode::SubdocDictUpsert,
                PathFlag::XattrPath | PathFlag::Mkdir_p,
                "tnx.op.staged",
                R"({"couchbase": {"version": "cheshire-cat", "next_version": "unknown"}})");
        cmd.addMutation(
                cb::mcbp::ClientOpcode::SubdocDictUpsert,
                PathFlag::None,
                "couchbase",
                R"({"version": "mad-hatter", "next_version": "cheshire-cat"})");
        ASSERT_EQ(cb::mcbp::Status::Success,
                  userConnection->execute(cmd).getStatus())
                << "Failed to update the document to expand the Input macros";
    }

    // Verify that the body is as expected:
    {
        auto resp = subdoc_multi_lookup({
                {ClientOpcode::SubdocGet, PathFlag::None, "couchbase.version"},
        });
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        const auto results = resp.getResults();
        ASSERT_EQ(cb::mcbp::Status::Success, results[0].status);
        ASSERT_EQ(R"("mad-hatter")", results[0].value);
    }

    // Replace the body with the staged value and remove that
    {
        BinprotSubdocMultiMutationCommand cmd;
        cmd.setKey(name);
        cmd.addMutation(cb::mcbp::ClientOpcode::SubdocReplaceBodyWithXattr,
                        PathFlag::XattrPath,
                        "tnx.op.staged",
                        {});
        cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDelete,
                        PathFlag::XattrPath,
                        "tnx.op.staged",
                        {});
        ASSERT_EQ(cb::mcbp::Status::Success,
                  userConnection->execute(cmd).getStatus());
    }

    // Verify that things looks like we expect them to
    {
        auto resp = subdoc_multi_lookup(
                {{ClientOpcode::SubdocGet, PathFlag::XattrPath, "tnx.op"},
                 {ClientOpcode::SubdocGet,
                  PathFlag::None,
                  "couchbase.version"}});
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        const auto results = resp.getResults();
        ASSERT_EQ(cb::mcbp::Status::Success, results[0].status);
        ASSERT_EQ("{}", results[0].value);
        ASSERT_EQ(cb::mcbp::Status::Success, results[1].status);
        ASSERT_EQ(R"("cheshire-cat")", results[1].value);
    }
}

TEST_P(XattrTest, ReplaceBodyWithXattr_WithoutXattrFlag) {
    auto rsp = userConnection->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocReplaceBodyWithXattr,
            name,
            "tnx.op.staged",
            {},
            PathFlag::None,
            cb::mcbp::subdoc::DocFlag::None,
            0});
    ASSERT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
}

/// 6.5 introduced a regression where we would return "null" if the document
/// only contains system xattrs and the connection don't have access
/// to system xattrs
TEST_P(XattrTest, MB54776) {
    setBodyAndXattr(value, {{sysXattr, xattrVal}});
    BinprotSubdocMultiLookupCommand cmd;
    cmd.setKey(name);
    cmd.addGet("$XTOC", PathFlag::XattrPath);
    cmd.addLookup("", cb::mcbp::ClientOpcode::Get, PathFlag::None);

    {
        BinprotSubdocMultiLookupResponse multiResp(
                userConnection->execute(cmd));
        ASSERT_EQ(cb::mcbp::Status::Success, multiResp.getStatus());
        const auto results = multiResp.getResults();
        EXPECT_EQ(cb::mcbp::Status::Success, results[0].status);
        EXPECT_EQ(R"(["_sync"])", results[0].value);
    }

    // Drop the privilege and rerun the command and verify
    userConnection->dropPrivilege(cb::rbac::Privilege::SystemXattrRead);

    {
        BinprotSubdocMultiLookupResponse multiResp(
                userConnection->execute(cmd));
        ASSERT_EQ(cb::mcbp::Status::Success, multiResp.getStatus());
        const auto results = multiResp.getResults();
        EXPECT_EQ(cb::mcbp::Status::Success, results[0].status);
        EXPECT_EQ("[]", results[0].value);
    }
    userConnection.reset();
}

/// MB-57864 requested support to operate on multiple xattrs in a single
/// call.
TEST_P(XattrTest, MB57864) {
    // Validate the input document
    {
        BinprotSubdocMultiLookupCommand lcmd;
        lcmd.setKey(name);
        lcmd.addGet("$XTOC", PathFlag::XattrPath);
        lcmd.addLookup("", cb::mcbp::ClientOpcode::Get, PathFlag::None);
        auto rsp = userConnection->execute(lcmd);
        const auto response = BinprotSubdocMultiLookupResponse(std::move(rsp));
        ASSERT_EQ(cb::mcbp::Status::Success, response.getStatus());
        const auto results = response.getResults();
        ASSERT_EQ(2, results.size());
        auto& xtoc = results[0];
        EXPECT_EQ(cb::mcbp::Status::Success, xtoc.status);
        EXPECT_EQ("[]", xtoc.value);
        auto& doc = results[1];
        EXPECT_EQ(cb::mcbp::Status::Success, doc.status);
        EXPECT_EQ(
                R"({"couchbase": {"version": "spock", "next_version": "vulcan"}})",
                doc.value);
    }

    // Add a user and a system xattr and a new value for the doc
    {
        BinprotSubdocMultiMutationCommand cmd;
        cmd.setKey(name);
        cmd.setVBucket(Vbid{0});
        cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                        PathFlag::XattrPath,
                        "user",
                        R"({"MB57864":"user"})");
        cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                        PathFlag::XattrPath,
                        "_system",
                        R"({"MB57864":"_system"})");
        cmd.addMutation(cb::mcbp::ClientOpcode::Set, PathFlag::None, "", value);
        cmd.addDocFlag(cb::mcbp::subdoc::DocFlag::Mkdoc);
        auto rsp = userConnection->execute(cmd);
        ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus())
                << rsp.getDataView();
    }

    // Validate that the document contains the new xattrs and value
    {
        BinprotSubdocMultiLookupCommand lcmd;
        lcmd.setKey(name);
        lcmd.addGet("$XTOC", PathFlag::XattrPath);
        lcmd.addGet("user", PathFlag::XattrPath);
        lcmd.addGet("_system", PathFlag::XattrPath);
        lcmd.addLookup("", cb::mcbp::ClientOpcode::Get, PathFlag::None);
        auto rsp = userConnection->execute(lcmd);
        const auto response = BinprotSubdocMultiLookupResponse(std::move(rsp));
        ASSERT_EQ(cb::mcbp::Status::Success, response.getStatus());
        const auto results = response.getResults();
        ASSERT_EQ(4, results.size());
        auto& xtoc = results[0];
        EXPECT_EQ(cb::mcbp::Status::Success, xtoc.status);
        EXPECT_EQ(R"(["_system","user"])", xtoc.value);
        auto& user = results[1];
        EXPECT_EQ(cb::mcbp::Status::Success, user.status);
        EXPECT_EQ(R"({"MB57864":"user"})", user.value);
        auto& _system = results[2];
        EXPECT_EQ(cb::mcbp::Status::Success, _system.status);
        EXPECT_EQ(R"({"MB57864":"_system"})", _system.value);
        auto& doc = results[3];
        EXPECT_EQ(cb::mcbp::Status::Success, doc.status);
        EXPECT_EQ(value, doc.value);
    }

    // Add one new xattrs and delete one, but leave the value unchanged
    {
        BinprotSubdocMultiMutationCommand cmd;
        cmd.setKey(name);
        cmd.setVBucket(Vbid{0});
        cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                        PathFlag::XattrPath,
                        "user2",
                        "true");
        cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDelete,
                        PathFlag::XattrPath,
                        "_system",
                        {});
        auto rsp = userConnection->execute(cmd);
        ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus())
                << rsp.getDataView();
    }

    // Validate that the document looks as expected
    {
        BinprotSubdocMultiLookupCommand lcmd;
        lcmd.setKey(name);
        lcmd.addGet("$XTOC", PathFlag::XattrPath);
        lcmd.addGet("user", PathFlag::XattrPath);
        lcmd.addGet("user2", PathFlag::XattrPath);
        lcmd.addGet("_system", PathFlag::XattrPath);
        lcmd.addLookup("", cb::mcbp::ClientOpcode::Get, PathFlag::None);
        auto rsp = userConnection->execute(lcmd);
        const auto response = BinprotSubdocMultiLookupResponse(std::move(rsp));
        ASSERT_EQ(cb::mcbp::Status::SubdocMultiPathFailure,
                  response.getStatus());
        const auto results = response.getResults();
        ASSERT_EQ(5, results.size());
        auto& xtoc = results[0];
        EXPECT_EQ(cb::mcbp::Status::Success, xtoc.status);
        EXPECT_EQ(R"(["user","user2"])", xtoc.value);
        auto& user = results[1];
        EXPECT_EQ(cb::mcbp::Status::Success, user.status);
        EXPECT_EQ(R"({"MB57864":"user"})", user.value);
        auto& user2 = results[2];
        EXPECT_EQ(cb::mcbp::Status::Success, user2.status);
        EXPECT_EQ("true", user2.value);
        auto& _system = results[3];
        EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, _system.status);
        auto& doc = results[4];
        EXPECT_EQ(cb::mcbp::Status::Success, doc.status);
        EXPECT_EQ(value, doc.value);
    }
}

TEST_P(XattrTest, MB57864_macro_expansion) {
    enum class Expansion {
        /// Do macro expansion for user.cas
        User,
        /// Do macro expansion for system.cas
        System,
        /// Do macro expansion for user.cas and system.cas
        Both
    };

    for (const auto mode :
         {Expansion::User, Expansion::System, Expansion::Both}) {
        BinprotSubdocMultiMutationCommand mcmd;
        mcmd.setKey(name);
        mcmd.setVBucket(Vbid{0});
        switch (mode) {
        case Expansion::User:
            mcmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                             PathFlag::XattrPath | PathFlag::ExpandMacros,
                             "user.cas",
                             R"("${Mutation.CAS}")");

            mcmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                             PathFlag::XattrPath,
                             "_system.cas",
                             R"("${Mutation.CAS}")");

            break;
        case Expansion::System:
            mcmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                             PathFlag::XattrPath,
                             "user.cas",
                             R"("${Mutation.CAS}")");

            mcmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                             PathFlag::XattrPath | PathFlag::ExpandMacros,
                             "_system.cas",
                             R"("${Mutation.CAS}")");
            break;
        case Expansion::Both:
            mcmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                             PathFlag::XattrPath | PathFlag::ExpandMacros,
                             "user.cas",
                             R"("${Mutation.CAS}")");

            mcmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                             PathFlag::XattrPath | PathFlag::ExpandMacros,
                             "_system.cas",
                             R"("${Mutation.CAS}")");
            break;
        }
        mcmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                         PathFlag::XattrPath,
                         "never.cas",
                         R"("${Mutation.CAS}")");
        mcmd.addMutation(
                cb::mcbp::ClientOpcode::Set, PathFlag::None, "", value);
        mcmd.addDocFlag(cb::mcbp::subdoc::DocFlag::Mkdoc);
        auto mrsp = userConnection->execute(mcmd);
        ASSERT_EQ(cb::mcbp::Status::Success, mrsp.getStatus())
                << mrsp.getDataView();

        // Validate that the document contains the new xattrs and value

        BinprotSubdocMultiLookupCommand lcmd;
        lcmd.setKey(name);
        lcmd.addGet("user.cas", PathFlag::XattrPath);
        lcmd.addGet("_system.cas", PathFlag::XattrPath);
        lcmd.addGet("never.cas", PathFlag::XattrPath);
        auto rsp = userConnection->execute(lcmd);
        const auto response = BinprotSubdocMultiLookupResponse(std::move(rsp));
        ASSERT_EQ(cb::mcbp::Status::Success, response.getStatus());
        const auto results = response.getResults();
        ASSERT_EQ(3, results.size());
        auto& user = results[0];
        auto& _system = results[1];
        auto& never = results[2];
        EXPECT_EQ(cb::mcbp::Status::Success, user.status);
        EXPECT_EQ(cb::mcbp::Status::Success, _system.status);
        EXPECT_EQ(cb::mcbp::Status::Success, never.status);
        EXPECT_EQ(R"("${Mutation.CAS}")", never.value);

        switch (mode) {
        case Expansion::User:
            EXPECT_NE(R"("${Mutation.CAS}")", user.value);
            EXPECT_EQ(R"("${Mutation.CAS}")", _system.value);
            break;
        case Expansion::System:
            EXPECT_EQ(R"("${Mutation.CAS}")", user.value);
            EXPECT_NE(R"("${Mutation.CAS}")", _system.value);
            break;
        case Expansion::Both:
            EXPECT_NE(R"("${Mutation.CAS}")", user.value);
            EXPECT_NE(R"("${Mutation.CAS}")", _system.value);
            EXPECT_EQ(user.value, _system.value);
            break;
        }
    }
}

TEST_P(XattrTest, ReplaceBodyWithXattr_binary_value) {
    std::string payload;
    {
        const auto fname = std::filesystem::path{SOURCE_ROOT} / "tests" /
                           "testapp" / "testapp_xattr.cc";
        auto content = cb::io::loadFile(fname);
        payload = folly::StringPiece{
                cb::compression::deflateSnappy(content)->coalesce()};
    }

    {
        BinprotSubdocMultiMutationCommand cmd;
        cmd.setKey(name);
        cmd.addMutation(
                ClientOpcode::SubdocDictUpsert,
                PathFlag::XattrPath | PathFlag::Mkdir_p | PathFlag::BinaryValue,
                "tnx.op.staged",
                payload);
        cmd.addMutation(
                ClientOpcode::SubdocDictUpsert,
                PathFlag::None,
                "couchbase",
                R"({"version": "mad-hatter", "next_version": "cheshire-cat"})");
        EXPECT_EQ(cb::mcbp::Status::Success,
                  userConnection->execute(cmd).getStatus())
                << "Failed to store the data";
    }

    // Verify that things looks like we expect them to
    {
        auto resp = subdoc_multi_lookup({{ClientOpcode::SubdocGet,
                                          PathFlag::XattrPath,
                                          "tnx.op.staged"},
                                         {ClientOpcode::SubdocGet,
                                          PathFlag::None,
                                          "couchbase.version"}});
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        const auto results = resp.getResults();
        ASSERT_EQ(cb::mcbp::Status::Success, results[0].status);
        ASSERT_EQ(base64_encode_value(payload), results[0].value);
        ASSERT_EQ(cb::mcbp::Status::Success, results[1].status);
        ASSERT_EQ(R"("mad-hatter")", results[1].value);
    }

    // and it gets returned as binary if we request binary read
    {
        auto resp = subdoc_multi_lookup(
                {{ClientOpcode::SubdocGet,
                  PathFlag::XattrPath | PathFlag::BinaryValue,
                  "tnx.op.staged"},
                 {ClientOpcode::SubdocGet,
                  PathFlag::None,
                  "couchbase.version"}});
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        const auto results = resp.getResults();
        ASSERT_EQ(cb::mcbp::Status::Success, results[0].status);
        ASSERT_EQ(payload, results[0].value);
        ASSERT_EQ(cb::mcbp::Status::Success, results[1].status);
        ASSERT_EQ(R"("mad-hatter")", results[1].value);
    }

    // Replace the body with the staged value (but don't tell the server
    // that the value should be treated as a binary value and flipped
    // back
    {
        BinprotSubdocMultiMutationCommand cmd;
        cmd.setKey(name);
        cmd.addMutation(ClientOpcode::SubdocReplaceBodyWithXattr,
                        PathFlag::XattrPath,
                        "tnx.op.staged",
                        {});

        ASSERT_EQ(cb::mcbp::Status::Success,
                  userConnection->execute(cmd).getStatus());
    }

    auto val = userConnection->get(name, Vbid{0});
    // Verify that the server didn't "decode" the value
    EXPECT_EQ(base64_encode_value(payload), val.value);
    EXPECT_EQ(0x0, val.info.flags);

    // And finally replace with the binary value...
    // Replace the body with the staged value and remove the staged xattr
    {
        BinprotSubdocMultiMutationCommand cmd;
        cmd.setKey(name);
        cmd.setUserFlags(0xbeefcafe);
        cmd.addMutation(ClientOpcode::SubdocReplaceBodyWithXattr,
                        PathFlag::XattrPath | PathFlag::BinaryValue,
                        "tnx.op.staged",
                        {});
        cmd.addMutation(ClientOpcode::SubdocDelete,
                        PathFlag::XattrPath,
                        "tnx.op.staged",
                        {});
        ASSERT_EQ(cb::mcbp::Status::Success,
                  userConnection->execute(cmd).getStatus());
    }

    val = userConnection->get(name, Vbid{0});
    EXPECT_EQ(val.info.datatype, Datatype::Raw);
    EXPECT_EQ(val.value, payload);
    EXPECT_EQ(0xbeefcafe, val.info.flags);
}

TEST_P(XattrTest, UpsertWithXattrBase64EncodedBinaryValue) {
    auto upsert = [this](const std::string& value) {
        BinprotSubdocMultiMutationCommand cmd;
        cmd.setKey(name);
        cmd.addDocFlag(DocFlag::Mkdoc);
        cmd.addMutation(ClientOpcode::SubdocDictUpsert,
                        PathFlag::XattrPath | PathFlag::Mkdir_p,
                        "base64",
                        value);
        EXPECT_EQ(cb::mcbp::Status::Success,
                  userConnection->execute(cmd).getStatus())
                << "Failed to store the data";
    };

    // Verify that we can store the actual encoded bits
    std::string encoded = base64_encode_value("base64_encode_value");
    upsert(encoded);

    // verify that we can store it as part of an object

    std::string_view view = encoded;
    // Remove the encosed " " in the string to avoid getting it escaped
    // in the json due to nlohmanns automatic " of strings
    view.remove_prefix(1);
    view.remove_suffix(1);
    nlohmann::json json = {{"raw", "raw"}, {"base64", view}};
    upsert(json.dump());
}

TEST_P(XattrTest, BinaryFlagMustBeWithXattr) {
    BinprotSubdocCommand cmd{ClientOpcode::SubdocGet, name, "foo"};
    auto rsp = userConnection->execute(cmd);
    EXPECT_EQ(Status::SubdocPathEnoent, rsp.getStatus());

    cmd.addPathFlags(PathFlag::BinaryValue);
    rsp = userConnection->execute(cmd);
    EXPECT_EQ(Status::SubdocXattrInvalidFlagCombo, rsp.getStatus());
    EXPECT_EQ("BINARY_VALUE flag requires XATTR flag to be set",
              rsp.getErrorContext());
}

TEST_P(XattrTest, BinaryFlagAndExpandMacrosIsNotLegal) {
    auto rsp = userConnection->execute(
            BinprotSubdocCommand{ClientOpcode::SubdocDictUpsert,
                                 name,
                                 "foo",
                                 "blob",
                                 PathFlag::ExpandMacros | PathFlag::XattrPath |
                                         PathFlag::BinaryValue});
    EXPECT_EQ(Status::SubdocXattrInvalidFlagCombo, rsp.getStatus());
    EXPECT_EQ("BINARY_VALUE can't be used together with EXPAND_MACROS",
              rsp.getErrorContext());
}

TEST_P(XattrTest, UpdateUserFlags) {
    auto info = userConnection->get(name, Vbid{0});
    EXPECT_EQ(0, info.info.flags);
    BinprotSubdocCommand cmd{ClientOpcode::SubdocDictUpsert,
                             name,
                             "foo",
                             "true",
                             PathFlag::XattrPath | PathFlag::Mkdir_p};
    cmd.setUserFlags(0xdeadcafe);
    auto rsp = userConnection->execute(cmd);
    ASSERT_EQ(Status::Success, rsp.getStatus());
    info = userConnection->get(name, Vbid{0});
    EXPECT_EQ(0xdeadcafe, info.info.flags);
}

/// Simulate that an old client (without support for binary values)
/// Fetch an object which contains a binary value, modify the value
/// and store the resulting object. Let the "new" client read
/// its binary value and verify that it is correct.
TEST_P(XattrTest, BinaryValueAccessedFromOldClient) {
    BinprotSubdocCommand cmd{
            ClientOpcode::SubdocDictUpsert,
            name,
            "foo.bar.binary",
            "value",
            PathFlag::BinaryValue | PathFlag::Mkdir_p | PathFlag::XattrPath};
    auto rsp = userConnection->execute(cmd);
    EXPECT_EQ(Status::Success, rsp.getStatus());

    // Receive the parent object
    cmd = BinprotSubdocCommand{
            ClientOpcode::SubdocGet, name, "foo.bar", {}, PathFlag::XattrPath};
    rsp = userConnection->execute(cmd);
    EXPECT_EQ(Status::Success, rsp.getStatus());
    EXPECT_EQ(R"({"binary":"cb-content-base64-encoded:dmFsdWU="})",
              rsp.getDataView());
    auto json = rsp.getDataJson();

    // Add a new attribute to the object
    json["ascii"] = "ascii-value";
    cmd = BinprotSubdocCommand{ClientOpcode::SubdocDictUpsert,
                               name,
                               "foo.bar",
                               json.dump(),
                               PathFlag::XattrPath};
    rsp = userConnection->execute(cmd);
    EXPECT_EQ(Status::Success, rsp.getStatus());

    // Read the object back
    cmd = BinprotSubdocCommand{
            ClientOpcode::SubdocGet, name, "foo.bar", {}, PathFlag::XattrPath};
    rsp = userConnection->execute(cmd);
    EXPECT_EQ(Status::Success, rsp.getStatus());
    EXPECT_EQ(
            R"({"ascii":"ascii-value","binary":"cb-content-base64-encoded:dmFsdWU="})",
            rsp.getDataView());

    // And we could also read the binary piece back:
    cmd = BinprotSubdocCommand{ClientOpcode::SubdocGet,
                               name,
                               "foo.bar.binary",
                               {},
                               PathFlag::XattrPath | PathFlag::BinaryValue};
    rsp = userConnection->execute(cmd);
    EXPECT_EQ(Status::Success, rsp.getStatus());
    EXPECT_EQ("value", rsp.getDataView());
}

TEST_P(XattrTest, NonBinaryValueRequestedWithBinaryFlag) {
    BinprotSubdocCommand cmd{ClientOpcode::SubdocDictUpsert,
                             name,
                             "foo.nonbinary",
                             R"("value")",
                             PathFlag::Mkdir_p | PathFlag::XattrPath};
    auto rsp = userConnection->execute(cmd);
    EXPECT_EQ(Status::Success, rsp.getStatus());

    // Receive the parent object
    cmd = BinprotSubdocCommand{ClientOpcode::SubdocGet,
                               name,
                               "foo.nonbinary",
                               {},
                               PathFlag::BinaryValue | PathFlag::XattrPath};
    rsp = userConnection->execute(cmd);
    EXPECT_EQ(Status::SubdocFieldNotBinaryValue, rsp.getStatus())
            << rsp.getDataView();
}
