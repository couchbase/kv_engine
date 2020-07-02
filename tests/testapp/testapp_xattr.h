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
#pragma once

#include "testapp_client_test.h"
#include "xattr/blob.h"

#include <platform/cb_malloc.h>

/**
 * Text fixture for XAttr tests which do not want to have an initial document
 * created.
 */
class XattrNoDocTest : public TestappXattrClientTest {
protected:
    void SetUp() override {
        TestappXattrClientTest::SetUp();
    }

    BinprotSubdocResponse subdoc_get(
            const std::string& path,
            protocol_binary_subdoc_flag flag = SUBDOC_FLAG_NONE,
            mcbp::subdoc::doc_flag docFlag = mcbp::subdoc::doc_flag::None) {
        return subdoc(cb::mcbp::ClientOpcode::SubdocGet,
                      name,
                      path,
                      {},
                      flag,
                      docFlag);
    }

    BinprotSubdocMultiLookupResponse subdoc_multi_lookup(
            std::vector<BinprotSubdocMultiLookupCommand::LookupSpecifier> specs,
            mcbp::subdoc::doc_flag docFlags = mcbp::subdoc::doc_flag::None);

    BinprotSubdocMultiMutationResponse subdoc_multi_mutation(
            std::vector<BinprotSubdocMultiMutationCommand::MutationSpecifier>
                    specs,
            mcbp::subdoc::doc_flag docFlags = mcbp::subdoc::doc_flag::None);

    GetMetaResponse get_meta();

    bool supportSyncRepl() const {
        return mcd_env->getTestBucket().supportsSyncWrites();
    }

    /// Constructs a Subdoc multi-mutation matching the style of an SDK
    // Transactions mutation.
    BinprotSubdocMultiMutationCommand makeSDKTxnMultiMutation() const;

    void testRequiresMkdocOrAdd();
    void testRequiresXattrPath();
    void testSinglePathDictAdd();
    void testMultipathDictAdd();
    void testMultipathDictUpsert();
    void testMultipathArrayPushLast();
    void testMultipathArrayPushFirst();
    void testMultipathArrayAddUnique();
    void testMultipathCounter();
    void testMultipathCombo();
    void testMultipathAccessDeletedCreateAsDeleted();

    std::optional<cb::durability::Requirements> durReqs = {};
};

class XattrNoDocDurabilityTest : public XattrNoDocTest {
protected:
    void SetUp() override {
        XattrNoDocTest::SetUp();
        // level:majority, timeout:default
        durReqs = cb::durability::Requirements();
    }
};

class XattrTest : public XattrNoDocTest {
protected:
    void SetUp() override {
        XattrNoDocTest::SetUp();

        // Create the document to operate on (with some compressible data).
        document.info.id = name;
        document.info.datatype = cb::mcbp::Datatype::Raw;
        document.value =
                R"({"couchbase": {"version": "spock", "next_version": "vulcan"}})";
        if (hasSnappySupport() == ClientSnappySupport::Yes) {
            // Compress the complete body.
            document.compress();
        }
        getConnection().mutate(document, Vbid(0), MutationType::Set);
    }

protected:
    void doArrayInsertTest(const std::string& path) {
        auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                           name,
                           path,
                           "\"Smith\"",
                           SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
        EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        resp = subdoc(cb::mcbp::ClientOpcode::SubdocArrayInsert,
                      name,
                      path + "[0]",
                      "\"Bart\"",
                      SUBDOC_FLAG_XATTR_PATH);
        EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        resp = subdoc(cb::mcbp::ClientOpcode::SubdocArrayInsert,
                      name,
                      path + "[1]",
                      "\"Jones\"",
                      SUBDOC_FLAG_XATTR_PATH);
        EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        resp = subdoc_get(path, SUBDOC_FLAG_XATTR_PATH);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        EXPECT_EQ("[\"Bart\",\"Jones\",\"Smith\"]", resp.getValue());
    }

    void doArrayPushLastTest(const std::string& path) {
        auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                           name,
                           path,
                           "\"Smith\"",
                           SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        resp = subdoc_get(path, SUBDOC_FLAG_XATTR_PATH);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        EXPECT_EQ("[\"Smith\"]", resp.getValue());

        // Add a second one so we know it was added last ;-)
        resp = subdoc(cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                      name,
                      path,
                      "\"Jones\"",
                      SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        resp = subdoc_get(path, SUBDOC_FLAG_XATTR_PATH);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        EXPECT_EQ("[\"Smith\",\"Jones\"]", resp.getValue());
    }

    void doArrayPushFirstTest(const std::string& path) {
        auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                           name,
                           path,
                           "\"Smith\"",
                           SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        resp = subdoc_get(path, SUBDOC_FLAG_XATTR_PATH);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        EXPECT_EQ("[\"Smith\"]", resp.getValue());

        // Add a second one so we know it was added first ;-)
        resp = subdoc(cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                      name,
                      path,
                      "\"Jones\"",
                      SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        resp = subdoc_get(path, SUBDOC_FLAG_XATTR_PATH);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        EXPECT_EQ("[\"Jones\",\"Smith\"]", resp.getValue());
    }

    void doAddUniqueTest(const std::string& path) {
        auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                           name,
                           path,
                           "\"Smith\"",
                           SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        resp = subdoc_get(path, SUBDOC_FLAG_XATTR_PATH);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        EXPECT_EQ("[\"Smith\"]", resp.getValue());

        resp = subdoc(cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                      name,
                      path,
                      "\"Jones\"",
                      SUBDOC_FLAG_XATTR_PATH);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        resp = subdoc_get(path, SUBDOC_FLAG_XATTR_PATH);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        EXPECT_EQ("[\"Smith\",\"Jones\"]", resp.getValue());

        resp = subdoc(cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                      name,
                      path,
                      "\"Jones\"",
                      SUBDOC_FLAG_XATTR_PATH);
        ASSERT_EQ(cb::mcbp::Status::SubdocPathEexists, resp.getStatus());

        resp = subdoc_get(path, SUBDOC_FLAG_XATTR_PATH);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        EXPECT_EQ("[\"Smith\",\"Jones\"]", resp.getValue());

    }

    void doCounterTest(const std::string& path) {
        auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocCounter,
                           name,
                           path,
                           "1",
                           SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        resp = subdoc_get(path, SUBDOC_FLAG_XATTR_PATH);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        EXPECT_EQ("1", resp.getValue());

        resp = subdoc(cb::mcbp::ClientOpcode::SubdocCounter,
                      name,
                      path,
                      "1",
                      SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());

        resp = subdoc_get(path, SUBDOC_FLAG_XATTR_PATH);
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        EXPECT_EQ("2", resp.getValue());

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
        document.info.cas = mcbp::cas::Wildcard;
        document.info.datatype = cb::mcbp::Datatype::Raw;
        if (compress) {
            document.compress();
        }
        getConnection().mutate(document, Vbid(0), MutationType::Replace);

        // Validate contents.
        EXPECT_EQ(xattrVal, getXattr(sysXattr).getDataString());
        auto response = getConnection().get(name, Vbid(0));
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
        auto& conn = getConnection();
        conn.sendCommand(cmd);

        BinprotSubdocMultiMutationResponse multiResp;
        conn.recvResponse(multiResp);
        EXPECT_EQ(cb::mcbp::Status::Success, multiResp.getStatus());

        // Check the body was set correctly
        auto doc = getConnection().get(name, Vbid(0));
        EXPECT_EQ(value, doc.value);

        // Check the xattr was set correctly
        auto resp = subdoc_get(sysXattr, SUBDOC_FLAG_XATTR_PATH);
        EXPECT_EQ(xattrVal, resp.getValue());

        // Check the datatype.
        auto meta = get_meta();
        EXPECT_EQ(expectedDatatype, meta.datatype);

        return multiResp;
    }

    void verify_xtoc_user_system_xattr() {
        // Test to check that we can get both an xattr and the main body in
        // subdoc multi-lookup
        setBodyAndXattr(value, {{sysXattr, xattrVal}});

        // Sanity checks and setup done lets try the multi-lookup

        BinprotSubdocMultiLookupCommand cmd;
        cmd.setKey(name);
        cmd.addGet("$XTOC", SUBDOC_FLAG_XATTR_PATH);
        cmd.addLookup("", cb::mcbp::ClientOpcode::Get, SUBDOC_FLAG_NONE);

        auto& conn = getConnection();
        conn.sendCommand(cmd);

        BinprotSubdocMultiLookupResponse multiResp;
        conn.recvResponse(multiResp);
        EXPECT_EQ(cb::mcbp::Status::Success, multiResp.getStatus());
        EXPECT_EQ(cb::mcbp::Status::Success, multiResp.getResults()[0].status);
        EXPECT_EQ(R"(["_sync"])", multiResp.getResults()[0].value);
        EXPECT_EQ(value, multiResp.getResults()[1].value);

        xattr_upsert("userXattr", R"(["Test"])");
        conn.sendCommand(cmd);
        multiResp.clear();
        conn.recvResponse(multiResp);
        EXPECT_EQ(cb::mcbp::Status::Success, multiResp.getStatus());
        EXPECT_EQ(cb::mcbp::Status::Success, multiResp.getResults()[0].status);
        EXPECT_EQ(R"(["_sync","userXattr"])", multiResp.getResults()[0].value);
    }

    std::string value = "{\"Field\":56}";
    const std::string sysXattr = "_sync";
    const std::string xattrVal = "{\"eg\":99}";
};

/// Explicit text fixutre for tests which want Xattr support disabled.
class XattrDisabledTest : public XattrTest {};
