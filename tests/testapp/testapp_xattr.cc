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
#include "testapp_xattr.h"

// @todo add the other transport protocols
INSTANTIATE_TEST_CASE_P(TransportProtocols,
    XattrTest,
    ::testing::Values(TransportProtocols::McbpPlain),
    ::testing::PrintToStringParamName());

TEST_P(XattrTest, GetXattrAndBody) {
    // Test to check that we can get both an xattr and the main body in
    // subdoc multi-lookup
    setBodyAndXattr(value, xattrVal);

    // Sanity checks and setup done lets try the multi-lookup

    BinprotSubdocMultiLookupCommand cmd;
    cmd.setKey(name);
    cmd.addGet(sysXattr, SUBDOC_FLAG_XATTR_PATH);
    cmd.addLookup("", PROTOCOL_BINARY_CMD_GET, SUBDOC_FLAG_NONE);

    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    conn.sendCommand(cmd);

    BinprotSubdocMultiLookupResponse multiResp;
    conn.recvResponse(multiResp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, multiResp.getStatus());
    EXPECT_EQ(xattrVal, multiResp.getResults()[0].value);
    EXPECT_EQ(value, multiResp.getResults()[1].value);
}

TEST_P(XattrTest, SetXattrAndBodyNewDoc) {
    // Ensure we are working on a new doc
    getConnection().remove(name, 0);
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                    SUBDOC_FLAG_XATTR_PATH,
                    sysXattr,
                    xattrVal);
    cmd.addMutation(PROTOCOL_BINARY_CMD_SET, SUBDOC_FLAG_NONE, "", value);
    cmd.addDocFlag(mcbp::subdoc::doc_flag::Mkdoc);

    testBodyAndXattrCmd(cmd);
}

TEST_P(XattrTest, SetXattrAndBodyNewDocWithExpiry) {
    // For MB-24542
    // Ensure we are working on a new doc
    getConnection().remove(name, 0);
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.setExpiry(3);
    cmd.addMutation(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                    SUBDOC_FLAG_XATTR_PATH,
                    sysXattr,
                    xattrVal);
    cmd.addMutation(PROTOCOL_BINARY_CMD_SET, SUBDOC_FLAG_NONE, "", value);
    cmd.addDocFlag(mcbp::subdoc::doc_flag::Mkdoc);

    testBodyAndXattrCmd(cmd);

    // Jump forward in time to expire item
    adjust_memcached_clock(4, TimeType::Uptime);

    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    BinprotSubdocMultiLookupCommand getCmd;
    getCmd.setKey(name);
    getCmd.addLookup("", PROTOCOL_BINARY_CMD_GET);
    conn.sendCommand(getCmd);

    BinprotSubdocMultiLookupResponse getResp;
    conn.recvResponse(getResp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, getResp.getStatus());
}

TEST_P(XattrTest, SetXattrAndBodyExistingDoc) {
    // Ensure that a doc is already present
    setBodyAndXattr("{\"TestField\":56788}", "4543");
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                    SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P,
                    sysXattr,
                    xattrVal);
    cmd.addMutation(PROTOCOL_BINARY_CMD_SET, SUBDOC_FLAG_NONE, "", value);

    testBodyAndXattrCmd(cmd);
}

TEST_P(XattrTest, SetXattrAndBodyInvalidFlags) {
    // First test invalid path flags
    std::array<protocol_binary_subdoc_flag, 3> badFlags = {
            {SUBDOC_FLAG_MKDIR_P,
             SUBDOC_FLAG_XATTR_PATH,
             SUBDOC_FLAG_EXPAND_MACROS}};

    for (const auto& flag : badFlags) {
        BinprotSubdocMultiMutationCommand cmd;
        cmd.setKey(name);

        // Should not be able to set all XATTRs
        cmd.addMutation(PROTOCOL_BINARY_CMD_SET, flag, "", value);

        auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
        conn.sendCommand(cmd);

        BinprotSubdocMultiMutationResponse multiResp;
        conn.recvResponse(multiResp);
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, multiResp.getStatus());
    }

    // Now test the invalid doc flags
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);

    // Should not be able to set all XATTRs
    cmd.addMutation(
            PROTOCOL_BINARY_CMD_SET, SUBDOC_FLAG_NONE, "", value);
    cmd.addDocFlag(mcbp::subdoc::doc_flag::AccessDeleted);

    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    conn.sendCommand(cmd);

    BinprotSubdocMultiMutationResponse multiResp;
    conn.recvResponse(multiResp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, multiResp.getStatus());
}

TEST_P(XattrTest, SetBodyInMultiLookup) {
    // Check that we can't put a CMD_SET in a multi lookup
    BinprotSubdocMultiLookupCommand cmd;
    cmd.setKey(name);
    // Should not be able to put a set in a multi lookup
    cmd.addLookup("", PROTOCOL_BINARY_CMD_SET, SUBDOC_FLAG_NONE);
    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    conn.sendCommand(cmd);

    BinprotSubdocMultiLookupResponse multiResp;
    conn.recvResponse(multiResp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_COMBO,
              multiResp.getStatus());
}

TEST_P(XattrTest, GetBodyInMultiMutation) {
    // Check that we can't put a CMD_GET in a multi mutation
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);

    // Should not be able to put a get in a multi multi-mutation
    cmd.addMutation(PROTOCOL_BINARY_CMD_GET, SUBDOC_FLAG_NONE, "", value);
    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    conn.sendCommand(cmd);

    BinprotSubdocMultiMutationResponse multiResp;
    conn.recvResponse(multiResp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_COMBO,
              multiResp.getStatus());
}

TEST_P(XattrTest, AddBodyAndXattr) {
    // Check that we can use the Add doc flag to create a new document

    // Get rid of any existing doc
    getConnection().remove(name, 0);

    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                    SUBDOC_FLAG_XATTR_PATH,
                    sysXattr,
                    xattrVal);
    cmd.addMutation(PROTOCOL_BINARY_CMD_SET, SUBDOC_FLAG_NONE, "", value);
    cmd.addDocFlag(mcbp::subdoc::doc_flag::Add);

    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    conn.sendCommand(cmd);

    BinprotSubdocMultiMutationResponse multiResp;
    conn.recvResponse(multiResp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, multiResp.getStatus());
}

TEST_P(XattrTest, AddBodyAndXattrAlreadyExistDoc) {
    // Check that usage of the Add flag will return EEXISTS if a key already
    // exists

    // Make sure a doc exists
    setBodyAndXattr("{\"TestField\":56788}", "4543");

    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                    SUBDOC_FLAG_XATTR_PATH,
                    sysXattr,
                    xattrVal);
    cmd.addMutation(PROTOCOL_BINARY_CMD_SET, SUBDOC_FLAG_NONE, "", value);
    cmd.addDocFlag(mcbp::subdoc::doc_flag::Add);

    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    conn.sendCommand(cmd);

    BinprotSubdocMultiMutationResponse multiResp;
    conn.recvResponse(multiResp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, multiResp.getStatus());
}

TEST_P(XattrTest, AddBodyAndXattrInvalidDocFlags) {
    // Check that usage of the Add flag will return EINVAL if the mkdoc doc
    // flag is also passed. The preexisting document exists to check that
    // we fail with the right error. i.e. we shouldn't even be fetching
    // the document from the engine if these two flags are set.

    // Make sure a doc exists
    setBodyAndXattr("{\"TestField\":56788}", "4543");

    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                    SUBDOC_FLAG_XATTR_PATH,
                    sysXattr,
                    xattrVal);
    cmd.addMutation(PROTOCOL_BINARY_CMD_SET, SUBDOC_FLAG_NONE, "", value);
    cmd.addDocFlag(mcbp::subdoc::doc_flag::Add);
    cmd.addDocFlag(mcbp::subdoc::doc_flag::Mkdoc);

    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    conn.sendCommand(cmd);

    BinprotSubdocMultiMutationResponse multiResp;
    conn.recvResponse(multiResp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, multiResp.getStatus());
}

TEST_P(XattrTest, TestSeqnoMacroExpansion) {
    // Test that we don't replace it when we don't send EXPAND_MACROS
    auto resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                       name,
                       "_sync.seqno",
                       "\"${Mutation.seqno}\"",
                       SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());
    resp = subdoc_get("_sync.seqno", SUBDOC_FLAG_XATTR_PATH);
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());
    EXPECT_EQ("\"${Mutation.seqno}\"", resp.getValue());

    // Verify that we expand the macro to something that isn't the macro
    // literal. i.e. If we send ${Mutation.SEQNO} we want to check that we
    // replaced that with something else (hopefully the correct value).
    // Unfortunately, unlike the cas, we do not get the seqno so we cannot
    // check it.
    resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                  name,
                  "_sync.seqno",
                  "\"${Mutation.seqno}\"",
                  SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_EXPAND_MACROS);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());

    resp = subdoc_get("_sync.seqno", SUBDOC_FLAG_XATTR_PATH);
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());
    EXPECT_NE("\"${Mutation.seqno}\"", resp.getValue());
}

TEST_P(XattrTest, TestMacroExpansionAndIsolation) {
    // This test verifies that you can have the same path in xattr's
    // and in the document without one affecting the other.
    // In addition to that we're testing that macro expansion works
    // as expected.

    // Lets store the macro and verify that it isn't expanded without the
    // expand macro flag
    auto resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                       name, "_sync.cas", "\"${Mutation.CAS}\"",
                       SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());

    resp = subdoc_get("_sync.cas", SUBDOC_FLAG_XATTR_PATH);
    EXPECT_EQ("\"${Mutation.CAS}\"", resp.getValue());

    // Let's update the body version..
    resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT, name, "_sync.cas",
           "\"If you don't know me by now\"", SUBDOC_FLAG_MKDIR_P);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());

    // The xattr version should have been unchanged...
    resp = subdoc_get("_sync.cas", SUBDOC_FLAG_XATTR_PATH);
    EXPECT_EQ("\"${Mutation.CAS}\"", resp.getValue());

    // And the body version should be what we set it to
    resp = subdoc_get("_sync.cas");
    EXPECT_EQ("\"If you don't know me by now\"", resp.getValue());

    // Then change it to macro expansion
    resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                  name, "_sync.cas", "\"${Mutation.CAS}\"",
                  SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_EXPAND_MACROS);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());

    /// Fetch the field and verify that it expanded the cas!
    std::string cas_string;
    resp = subdoc_get("_sync.cas", SUBDOC_FLAG_XATTR_PATH);
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());
    auto first_cas = resp.getCas();
    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    ss << "\"0x" << std::setw(16) << resp.getCas() << "\"";
    cas_string = ss.str();
    EXPECT_EQ(cas_string, resp.getValue());

    // Let's update the body version..
    resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT, name, "_sync.cas",
                  "\"Hell ain't such a bad place to be\"");
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());

    // The macro should not have been expanded again...
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());
    resp = subdoc_get("_sync.cas", SUBDOC_FLAG_XATTR_PATH);
    EXPECT_EQ(cas_string, resp.getValue());
    EXPECT_NE(first_cas, resp.getCas());
}

TEST_P(XattrTest, OperateOnDeletedItem) {
    getConnection().remove(name, 0);

    // let's add an attribute to the deleted document
    auto resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                       name,
                       "_sync.deleted",
                       "true",
                       SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P,
                       mcbp::subdoc::doc_flag::AccessDeleted);
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_SUCCESS_DELETED,
              resp.getStatus());

    resp = subdoc_get("_sync.deleted",
                      SUBDOC_FLAG_XATTR_PATH,
                      mcbp::subdoc::doc_flag::AccessDeleted);
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_SUCCESS_DELETED,
              resp.getStatus());
    EXPECT_EQ("true", resp.getValue());
}

TEST_P(XattrTest, MB_22318) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              xattr_upsert("doc", "{\"author\": \"Bart\"}"));
}

TEST_P(XattrTest, MB_22319) {
    // This is listed as working in the bug report
    EXPECT_EQ(uint8_t(PROTOCOL_BINARY_RESPONSE_SUCCESS),
              uint8_t(xattr_upsert("doc.readcount", "0")));
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              xattr_upsert("doc.author", "\"jack\""));

    // The failing bits is:
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                    SUBDOC_FLAG_XATTR_PATH,
                    "doc.readcount", "1");
    cmd.addMutation(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                    SUBDOC_FLAG_XATTR_PATH,
                    "doc.author", "\"jones\"");

    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    conn.sendCommand(cmd);

    BinprotResponse resp;
    conn.recvResponse(resp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());
}


/*
 * The spec lists a table of the behavior when operating on a
 * full XATTR spec or if it is a partial XATTR spec.
 */

/**
 * Reads the value of the given XATTR.
 */
TEST_P(XattrTest, Get_FullXattrSpec) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              xattr_upsert("doc", "{\"author\": \"Bart\",\"rev\":0}"));

    auto response = subdoc_get("doc", SUBDOC_FLAG_XATTR_PATH);
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, response.getStatus());
    EXPECT_EQ("{\"author\": \"Bart\",\"rev\":0}", response.getValue());
}

/**
 * Reads the sub-part of the given XATTR.
 */
TEST_P(XattrTest, Get_PartialXattrSpec) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              xattr_upsert("doc", "{\"author\": \"Bart\",\"rev\":0}"));

    auto response = subdoc_get("doc.author", SUBDOC_FLAG_XATTR_PATH);
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, response.getStatus());
    EXPECT_EQ("\"Bart\"", response.getValue());
}

/**
 * Returns true if the given XATTR exists.
 */
TEST_P(XattrTest, Exists_FullXattrSpec) {
    // The document exists, but we should not have any xattr's
    auto resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
                       name, "doc", {}, SUBDOC_FLAG_XATTR_PATH);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT, resp.getStatus());

    // Create the xattr
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              xattr_upsert("doc", "{\"author\": \"Bart\",\"rev\":0}"));

    // Now it should exist
    resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
                  name, "doc", {}, SUBDOC_FLAG_XATTR_PATH);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());
}

/**
 * Returns true if the given XATTR exists and the given sub-part
 * of the XATTR exists.
 */
TEST_P(XattrTest, Exists_PartialXattrSpec) {
    // The document exists, but we should not have any xattr's
    auto resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
                       name, "doc", {}, SUBDOC_FLAG_XATTR_PATH);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT, resp.getStatus());

    // Create the xattr
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              xattr_upsert("doc", "{\"author\": \"Bart\",\"rev\":0}"));

    // Now it should exist
    resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
                  name, "doc.author", {}, SUBDOC_FLAG_XATTR_PATH);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());

    // But we don't have one named _sync
    resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
                  name, "_sync.cas", {}, SUBDOC_FLAG_XATTR_PATH);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT, resp.getStatus());
}

/**
 * If XATTR specified by X-Key does not exist then create it with the
 * given value.
 * If XATTR already exists - fail with SUBDOC_PATH_EEXISTS
 */
TEST_P(XattrTest, DictAdd_FullXattrSpec) {
    // Adding it the first time should work
    auto resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                       name, "doc", "{\"author\": \"Bart\"}",
                       SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());

    // Adding it the first time should work, second time we should get EEXISTS
    resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                  name, "doc", "{\"author\": \"Bart\"}",
                  SUBDOC_FLAG_XATTR_PATH);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_EEXISTS, resp.getStatus());
}

/**
 * Adds a dictionary element specified by the X-Path to the given X-Key.
 */
TEST_P(XattrTest, DictAdd_PartialXattrSpec) {
    // Adding it the first time should work
    auto resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                       name, "doc.author", "\"Bart\"",
                       SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());

    // Adding it the first time should work, second time we should get EEXISTS
    resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                  name, "doc.author", "\"Bart\"",
                  SUBDOC_FLAG_XATTR_PATH);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_EEXISTS, resp.getStatus());
}

/**
 * Replaces the whole XATTR specified by X-Key with the given value if
 * the XATTR exists, or creates it with the given value.
 */
TEST_P(XattrTest, DictUpsert_FullXattrSpec) {
    // Adding it the first time should work
    auto resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                       name, "doc", "{\"author\": \"Bart\"}",
                       SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());

    // We should be able to update it...
    resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                  name, "doc", "{\"author\": \"Jones\"}",
                  SUBDOC_FLAG_XATTR_PATH);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());

    resp = subdoc_get("doc.author", SUBDOC_FLAG_XATTR_PATH);
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());
    EXPECT_EQ("\"Jones\"", resp.getValue());
}

/**
 * Upserts a dictionary element specified by the X-Path to the given X-Key.
 */
TEST_P(XattrTest, DictUpsert_PartialXattrSpec) {
    // Adding it the first time should work
    auto resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                       name, "doc", "{\"author\": \"Bart\"}",
                       SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());

    // We should be able to update it...
    resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                  name, "doc.author", "\"Jones\"",
                  SUBDOC_FLAG_XATTR_PATH);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());

    resp = subdoc_get("doc.author", SUBDOC_FLAG_XATTR_PATH);
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());
    EXPECT_EQ("\"Jones\"", resp.getValue());
}

/**
 * Deletes the whole XATTR specified by X-Key
 */
TEST_P(XattrTest, Delete_FullXattrSpec) {
    auto resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                       name, "doc", "{\"author\": \"Bart\"}",
                       SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());
    resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_DELETE,
                 name, "doc", {}, SUBDOC_FLAG_XATTR_PATH);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());

    // THe entire stuff should be gone
    resp = subdoc_get("doc", SUBDOC_FLAG_XATTR_PATH);
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT, resp.getStatus());
}

/**
 * Deletes the sub-part of the XATTR specified by X-Key and X-Path
 */
TEST_P(XattrTest, Delete_PartialXattrSpec) {
    auto resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                       name, "doc", "{\"author\":\"Bart\",\"ref\":0}",
                       SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());
    resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_DELETE,
                  name, "doc.ref", {}, SUBDOC_FLAG_XATTR_PATH);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());

    // THe entire stuff should be gone
    resp = subdoc_get("doc", SUBDOC_FLAG_XATTR_PATH);
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());
    EXPECT_EQ("{\"author\":\"Bart\"}", resp.getValue());
}

/**
 * If the XATTR specified by X-Key exists, then replace the whole XATTR,
 * otherwise fail with SUBDOC_PATH_EEXISTS
 */
TEST_P(XattrTest, Replace_FullXattrSpec) {
    auto resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                       name, "doc", "{\"author\":\"Bart\",\"ref\":0}",
                       SUBDOC_FLAG_XATTR_PATH);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT, resp.getStatus());

    resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                  name, "doc", "{\"author\": \"Bart\"}",
                  SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());

    resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                  name, "doc", "{\"author\":\"Bart\",\"ref\":0}",
                  SUBDOC_FLAG_XATTR_PATH);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());

    resp = subdoc_get("doc", SUBDOC_FLAG_XATTR_PATH);
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());
    EXPECT_EQ("{\"author\":\"Bart\",\"ref\":0}", resp.getValue());
}

/**
 * Replaces the sub-part of the XATTR-specified by X-Key and X-path.
 */
TEST_P(XattrTest, Replace_PartialXattrSpec) {
    auto resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                       name, "doc.author", "\"Bart\"",
                       SUBDOC_FLAG_XATTR_PATH);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT, resp.getStatus());
    resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
                  name, "doc", "{\"author\":\"Bart\",\"rev\":0}",
                  SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());

    resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
                  name, "doc.author", "\"Jones\"",
                  SUBDOC_FLAG_XATTR_PATH);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());

    resp = subdoc_get("doc", SUBDOC_FLAG_XATTR_PATH);
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());
    EXPECT_EQ("{\"author\":\"Jones\",\"rev\":0}", resp.getValue());
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
TEST_P(XattrTest, VerifyNotEnabled) {
    auto& conn = getMCBPConnection();
    conn.setXattrSupport(false);

    // All of the subdoc commands end up using the same method
    // to validate the xattr portion of the command so we'll just
    // check one
    BinprotSubdocCommand cmd;
    cmd.setOp(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD);
    cmd.setKey(name);
    cmd.setPath("_sync.deleted");
    cmd.setValue("true");
    cmd.addPathFlags(SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
    cmd.addDocFlags(mcbp::subdoc::doc_flag::AccessDeleted |
                    mcbp::subdoc::doc_flag::Mkdoc);
    conn.sendCommand(cmd);

    BinprotSubdocResponse resp;
    conn.recvResponse(resp);

    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED,
              resp.getStatus());
}

TEST_P(XattrTest, MB_22691) {
    auto resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                  name, "integer_extra", "1",
                  SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus());

    resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                  name, "integer", "2",
                  SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, resp.getStatus())
                << memcached_status_2_text(resp.getStatus());
}

/**
 * Verify that a vattr entry exists, and is of the specified type
 *
 * @param root The root in the JSON object
 * @param name The name of the tag to search for
 * @param type The expected type for this object
 */
static void verify_vattr_entry(cJSON* root, const char* name, int type) {
    auto* obj = cJSON_GetObjectItem(root, name);
    ASSERT_NE(nullptr, obj);
    EXPECT_EQ(type, obj->type);
}

TEST_P(XattrTest, MB_23882_VirtualXattrs) {
    // Test to check that we can get both an xattr and the main body in
    // subdoc multi-lookup
    setBodyAndXattr(value, xattrVal);

    // Sanity checks and setup done lets try the multi-lookup

    BinprotSubdocMultiLookupCommand cmd;
    cmd.setKey(name);
    cmd.addGet("$document", SUBDOC_FLAG_XATTR_PATH);
    cmd.addGet("$document.CAS", SUBDOC_FLAG_XATTR_PATH);
    cmd.addGet("$document.foobar", SUBDOC_FLAG_XATTR_PATH);
    cmd.addGet("_sync.eg", SUBDOC_FLAG_XATTR_PATH);

    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    conn.sendCommand(cmd);

    BinprotSubdocMultiLookupResponse multiResp;
    conn.recvResponse(multiResp);

    auto& results = multiResp.getResults();

    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_MULTI_PATH_FAILURE,
              multiResp.getStatus());
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, results[0].status);

    // Ensure that we found all we expected and they're of the correct type:
    unique_cJSON_ptr meta(cJSON_Parse(results[0].value.c_str()));
    verify_vattr_entry(meta.get(), "CAS", cJSON_String);
    verify_vattr_entry(meta.get(), "vbucket_uuid", cJSON_String);
    verify_vattr_entry(meta.get(), "seqno", cJSON_String);
    verify_vattr_entry(meta.get(), "exptime", cJSON_Number);
    verify_vattr_entry(meta.get(), "value_bytes", cJSON_Number);
    verify_vattr_entry(meta.get(), "deleted", cJSON_False);

    if (mcd_env->getTestBucket().supportsLastModifiedVattr()) {
        verify_vattr_entry(meta.get(), "last_modified", cJSON_String);
    }

    // Verify exptime is showing as 0 (document has no expiry)
    EXPECT_EQ(0, cJSON_GetObjectItem(meta.get(), "exptime")->valueint);

    // verify that the datatype is correctly encoded and contains
    // the correct bits
    auto* obj = cJSON_GetObjectItem(meta.get(), "datatype");
    ASSERT_NE(nullptr, obj);
    ASSERT_EQ(cJSON_Array, obj->type);
    bool found_xattr = false;
    bool found_json = false;

    for (obj = obj->child; obj != nullptr; obj = obj->next) {
        EXPECT_EQ(cJSON_String, obj->type);
        const std::string tag{obj->valuestring};
        if (tag == "xattr") {
            found_xattr = true;
        } else if (tag == "json") {
            found_json = true;
        } else {
            EXPECT_EQ(nullptr, tag.c_str());
        }
    }
    EXPECT_TRUE(found_json);
    EXPECT_TRUE(found_xattr);

    // Verify that we got a partial from the second one
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, results[1].status);
    const std::string cas{std::string{"\""} +
                          cJSON_GetObjectItem(meta.get(), "CAS")->valuestring +
                          "\""};
    meta.reset(cJSON_Parse(multiResp.getResults()[1].value.c_str()));
    EXPECT_EQ(cas, to_string(meta));

    // The third one didn't exist
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT, results[2].status);

    // Expect that we could find _sync.eg
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, results[3].status);
    EXPECT_EQ(xattrVal, results[3].value);
}

TEST_P(XattrTest, MB_23882_VirtualXattrs_GetXattrAndBody) {
    // Test to check that we can get both an xattr and the main body in
    // subdoc multi-lookup
    setBodyAndXattr(value, xattrVal);

    // Sanity checks and setup done lets try the multi-lookup

    BinprotSubdocMultiLookupCommand cmd;
    cmd.setKey(name);
    cmd.addGet("$document.deleted", SUBDOC_FLAG_XATTR_PATH);
    cmd.addLookup("", PROTOCOL_BINARY_CMD_GET, SUBDOC_FLAG_NONE);

    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    conn.sendCommand(cmd);

    BinprotSubdocMultiLookupResponse multiResp;
    conn.recvResponse(multiResp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, multiResp.getStatus());
    EXPECT_EQ("false", multiResp.getResults()[0].value);
    EXPECT_EQ(value, multiResp.getResults()[1].value);
}

TEST_P(XattrTest, MB_23882_VirtualXattrs_IsReadOnly) {
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                    SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P,
                    "$document.CAS", "foo");
    cmd.addMutation(PROTOCOL_BINARY_CMD_SET, SUBDOC_FLAG_NONE, "", value);

    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    conn.sendCommand(cmd);

    BinprotSubdocMultiMutationResponse multiResp;
    conn.recvResponse(multiResp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_XATTR_CANT_MODIFY_VATTR,
              multiResp.getStatus());
}

TEST_P(XattrTest, MB_23882_VirtualXattrs_UnknownVattr) {
    BinprotSubdocMultiLookupCommand cmd;
    cmd.setKey(name);
    cmd.addGet("$documents", SUBDOC_FLAG_XATTR_PATH); // should be $document

    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    conn.sendCommand(cmd);

    BinprotSubdocMultiMutationResponse multiResp;
    conn.recvResponse(multiResp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_XATTR_UNKNOWN_VATTR,
              multiResp.getStatus());
}

// Test that one can fetch both the body and an XATTR on a deleted document.
TEST_P(XattrTest, MB24152_GetXattrAndBodyDeleted) {
    setBodyAndXattr(value, xattrVal);

    BinprotSubdocMultiLookupCommand cmd;
    cmd.setKey(name);
    cmd.addDocFlag(mcbp::subdoc::doc_flag::AccessDeleted);
    cmd.addGet(sysXattr, SUBDOC_FLAG_XATTR_PATH);
    cmd.addLookup("", PROTOCOL_BINARY_CMD_GET);

    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    conn.sendCommand(cmd);

    BinprotSubdocMultiLookupResponse multiResp;
    conn.recvResponse(multiResp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, multiResp.getStatus());
    EXPECT_EQ(xattrVal, multiResp.getResults()[0].value);
    EXPECT_EQ(value, multiResp.getResults()[1].value);
}

// Test that attempting to get an XATTR and a Body when the XATTR doesn't exist
// (partially) succeeds - the body is returned.
TEST_P(XattrTest, MB24152_GetXattrAndBodyWithoutXattr) {

    // Create a document without an XATTR.
    getConnection().store(name, 0, value, cb::mcbp::Datatype::JSON);

    // Attempt to request both the body and a non-existent XATTR.
    BinprotSubdocMultiLookupCommand cmd;
    cmd.setKey(name);
    cmd.addDocFlag(mcbp::subdoc::doc_flag::AccessDeleted);
    cmd.addGet(sysXattr, SUBDOC_FLAG_XATTR_PATH);
    cmd.addLookup("", PROTOCOL_BINARY_CMD_GET);

    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    conn.sendCommand(cmd);

    BinprotSubdocMultiLookupResponse multiResp;
    conn.recvResponse(multiResp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_MULTI_PATH_FAILURE,
              multiResp.getStatus());

    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT,
              multiResp.getResults()[0].status);
    EXPECT_EQ("", multiResp.getResults()[0].value);

    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              multiResp.getResults()[1].status);
    EXPECT_EQ(value, multiResp.getResults()[1].value);
}

// Test that attempting to get an XATTR and a Body when the doc is deleted and
// empty (partially) succeeds - the XATTR is returned.
TEST_P(XattrTest, MB24152_GetXattrAndBodyDeletedAndEmpty) {
    // Store a document with body+XATTR; then delete it (so the body
    // becomes empty).
    setBodyAndXattr(value, xattrVal);
    getConnection().remove(name, 0);

    BinprotSubdocMultiLookupCommand cmd;
    cmd.setKey(name);
    cmd.addDocFlag(mcbp::subdoc::doc_flag::AccessDeleted);
    cmd.addGet(sysXattr, SUBDOC_FLAG_XATTR_PATH);
    cmd.addLookup("", PROTOCOL_BINARY_CMD_GET);

    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    conn.sendCommand(cmd);

    BinprotSubdocMultiLookupResponse multiResp;
    conn.recvResponse(multiResp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_MULTI_PATH_FAILURE_DELETED,
              multiResp.getStatus());
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              multiResp.getResults()[0].status);
    EXPECT_EQ(xattrVal, multiResp.getResults()[0].value);

    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT,
              multiResp.getResults()[1].status);
    EXPECT_EQ("", multiResp.getResults()[1].value);
}

// Test that attempting to get an XATTR and a Body when the body is non-JSON
// succeeds.
TEST_P(XattrTest, MB24152_GetXattrAndBodyNonJSON) {
    // Store a document with a non-JSON body + XATTR.
    value = "non-JSON value";
    setBodyAndXattr(value, xattrVal, cb::mcbp::Datatype::Raw);

    BinprotSubdocMultiLookupCommand cmd;
    cmd.setKey(name);
    cmd.addDocFlag(mcbp::subdoc::doc_flag::AccessDeleted);
    cmd.addGet(sysXattr, SUBDOC_FLAG_XATTR_PATH);
    cmd.addLookup("", PROTOCOL_BINARY_CMD_GET);

    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    conn.sendCommand(cmd);

    BinprotSubdocMultiLookupResponse multiResp;
    conn.recvResponse(multiResp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              multiResp.getStatus());
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              multiResp.getResults()[0].status);
    EXPECT_EQ(xattrVal, multiResp.getResults()[0].value);

    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              multiResp.getResults()[1].status);
    EXPECT_EQ(value, multiResp.getResults()[1].value);
}

// Test that a partial failure on a multi-lookup on a deleted document returns
// SUBDOC_MULTI_PATH_FAILURE_DELETED
TEST_P(XattrTest, MB23808_MultiPathFailureDeleted) {
    // Store an initial body+XATTR; then delete it.
    setBodyAndXattr(value, xattrVal);
    getConnection().remove(name, 0);

    // Lookup two XATTRs - one which exists and one which doesn't.
    BinprotSubdocMultiLookupCommand cmd;
    cmd.setKey(name);
    cmd.addDocFlag(mcbp::subdoc::doc_flag::AccessDeleted);
    cmd.addGet(sysXattr, SUBDOC_FLAG_XATTR_PATH);
    cmd.addGet("_sync.non_existant", SUBDOC_FLAG_XATTR_PATH);

    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    conn.sendCommand(cmd);

    // We expect to successfully access the first (existing) XATTR; but not
    // the second.
    BinprotSubdocMultiLookupResponse multiResp;
    conn.recvResponse(multiResp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_MULTI_PATH_FAILURE_DELETED,
              multiResp.getStatus());
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              multiResp.getResults()[0].status);
    EXPECT_EQ(xattrVal, multiResp.getResults()[0].value);

    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT,
              multiResp.getResults()[1].status);
}

TEST_P(XattrTest, SetXattrAndDeleteBasic) {
    setBodyAndXattr(value, "55");
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                    SUBDOC_FLAG_XATTR_PATH,
                    sysXattr,
                    xattrVal);
    cmd.addMutation(PROTOCOL_BINARY_CMD_DELETE, SUBDOC_FLAG_NONE, "", "");
    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    conn.sendCommand(cmd);

    BinprotSubdocMultiMutationResponse multiResp;
    conn.recvResponse(multiResp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, multiResp.getStatus());

    // Should now only be XATTR datatype
    auto meta = conn.getMeta(name, 0, 0);
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_XATTR, meta.datatype);
    // Should also be marked as deleted
    EXPECT_EQ(1, meta.deleted);

    auto resp = subdoc_get(sysXattr,
                           SUBDOC_FLAG_XATTR_PATH,
                           mcbp::subdoc::doc_flag::AccessDeleted);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_SUCCESS_DELETED,
              resp.getStatus());
    EXPECT_EQ(xattrVal, resp.getValue());

    // Check we can't access the deleted document
    resp = subdoc_get(sysXattr, SUBDOC_FLAG_XATTR_PATH);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, resp.getStatus());

    BinprotSubdocMultiLookupCommand getCmd;
    getCmd.setKey(name);
    getCmd.addLookup("", PROTOCOL_BINARY_CMD_GET);
    conn.sendCommand(getCmd);

    BinprotSubdocMultiLookupResponse getResp;
    conn.recvResponse(getResp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, getResp.getStatus());

    // Worth noting the difference in the way it fails if AccessDeleted is set.
    getCmd.addDocFlag(mcbp::subdoc::doc_flag::AccessDeleted);
    conn.sendCommand(getCmd);
    conn.recvResponse(getResp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_MULTI_PATH_FAILURE_DELETED,
              getResp.getStatus());
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT,
              getResp.getResults().at(0).status);
}

TEST_P(XattrTest, SetXattrAndDeleteCheckUserXattrsDeleted) {
    setBodyAndXattr(value, xattrVal);
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                    SUBDOC_FLAG_XATTR_PATH,
                    "userXattr",
                    "66");
    cmd.addMutation(PROTOCOL_BINARY_CMD_DELETE, SUBDOC_FLAG_NONE, "", "");
    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    conn.sendCommand(cmd);

    BinprotSubdocMultiMutationResponse multiResp;
    conn.recvResponse(multiResp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, multiResp.getStatus());

    // Should now only be XATTR datatype
    auto meta = conn.getMeta(name, 0, 0);
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_XATTR, meta.datatype);
    // Should also be marked as deleted
    EXPECT_EQ(1, meta.deleted);

    auto resp = subdoc_get("userXattr", SUBDOC_FLAG_XATTR_PATH);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, resp.getStatus());

    // The delete should delete user Xattrs as well as the body, leaving only
    // system Xattrs
    resp = subdoc_get("userXattr",
                      SUBDOC_FLAG_XATTR_PATH,
                      mcbp::subdoc::doc_flag::AccessDeleted);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT, resp.getStatus());

    // System Xattr should still be there so lets check it
    resp = subdoc_get(sysXattr,
                      SUBDOC_FLAG_XATTR_PATH,
                      mcbp::subdoc::doc_flag::AccessDeleted);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_SUCCESS_DELETED,
              resp.getStatus());
    EXPECT_EQ(xattrVal, resp.getValue());
}

TEST_P(XattrTest, SetXattrAndDeleteJustUserXattrs) {
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                    SUBDOC_FLAG_XATTR_PATH,
                    "userXattr",
                    "66");
    cmd.addMutation(
            PROTOCOL_BINARY_CMD_SET, SUBDOC_FLAG_NONE, "", "{\"Field\": 88}");
    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    conn.sendCommand(cmd);

    BinprotSubdocMultiMutationResponse multiResp;
    conn.recvResponse(multiResp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, multiResp.getStatus());

    cmd.clearMutations();
    cmd.addMutation(PROTOCOL_BINARY_CMD_DELETE, SUBDOC_FLAG_NONE, "", "");
    conn.sendCommand(cmd);
    conn.recvResponse(multiResp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, multiResp.getStatus());
}

TEST_P(XattrTest, TestXattrDeleteDatatypes) {
    // See MB-25422. We test to make sure that the datatype of a document is
    // correctly altered after a soft delete.
    setBodyAndXattr(value, "55");
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                    SUBDOC_FLAG_XATTR_PATH,
                    sysXattr,
                    xattrVal);
    cmd.addMutation(PROTOCOL_BINARY_CMD_DELETE, SUBDOC_FLAG_NONE, "", "");
    auto& conn = dynamic_cast<MemcachedBinprotConnection&>(getConnection());
    conn.sendCommand(cmd);

    BinprotSubdocMultiMutationResponse multiResp;
    conn.recvResponse(multiResp);
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, multiResp.getStatus());

    // Should now only be XATTR datatype
    auto meta = conn.getMeta(name, 0, 0);
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_XATTR, meta.datatype);
    // Should also be marked as deleted
    EXPECT_EQ(1, meta.deleted);
}
