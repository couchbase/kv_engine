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
    auto resp = subdoc(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD, name,
                       "_sync.deleted", "true",
                       SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_ACCESS_DELETED |
                       SUBDOC_FLAG_MKDIR_P);
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUBDOC_SUCCESS_DELETED,
              resp.getStatus());

    resp = subdoc_get("_sync.deleted", SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_ACCESS_DELETED);
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
    cmd.add_mutation(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
                     SUBDOC_FLAG_XATTR_PATH,
                    "doc.readcount", "1");
    cmd.add_mutation(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
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
