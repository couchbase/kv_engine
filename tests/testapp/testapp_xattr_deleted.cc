/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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

/*
 * Test extended attribute functionality related to the 'CreateAsDeleted'
 * doc flag.
 *
 * Initial use-case for this doc flag is Transactions.
 */

#include "testapp_xattr.h"

using namespace mcbp::subdoc;
using namespace cb::mcbp;

// Negative test: The subdoc operation returns Einval as
// doc_flag::CreateAsDeleted requires one of doc_flag::Mkdoc/Add.
TEST_P(XattrNoDocTest, AddUserXattrToNonExistentItem_RequiresMkdocOrAdd) {
    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictAdd,
                       name,
                       "txn.deleted",
                       "true",
                       SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P,
                       doc_flag::CreateAsDeleted);
    EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
}

// Negative test: The Subdoc CreateAsDeleted doesn't allow to write in the
// body.
TEST_P(XattrNoDocTest, AddUserXattrToNonExistentItem_RequiresXattrPath) {
    // Note: subdoc-flags doesn't set SUBDOC_FLAG_XATTR_PATH
    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictAdd,
                       name,
                       "txn.deleted",
                       "true",
                       SUBDOC_FLAG_MKDIR_P,
                       doc_flag::Mkdoc | doc_flag::CreateAsDeleted);
    EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
}

// Positive test: Can User XAttrs be added to a document which doesn't exist
// (and doesn't have a tombstone) using the new CreateAsDeleted flag.
TEST_P(XattrNoDocTest, AddUserXattrToNonExistentItem) {
    // let's add a user XATTR to a non-existing document
    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictAdd,
                       name,
                       "txn.deleted",
                       "true",
                       SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P,
                       doc_flag::Mkdoc | doc_flag::CreateAsDeleted);
    EXPECT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());

    resp = subdoc_get(
            "txn.deleted", SUBDOC_FLAG_XATTR_PATH, doc_flag::AccessDeleted);
    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());
    EXPECT_EQ("true", resp.getValue());
}

// Positive tests: Can User XAttrs be added to a document which doesn't exist
// (and doesn't have a tombstone) using the new CreateAsDeleted flag, using
// multi-mutation with each mutation opcode.
TEST_P(XattrNoDocTest, AddUserXattrToNonExistentItemMultipathDictAdd) {
    BinprotSubdocMultiMutationCommand cmd(
            name,
            {{ClientOpcode::SubdocDictAdd,
              SUBDOC_FLAG_XATTR_PATH,
              "txn",
              "\"foo\""}},
            doc_flag::Mkdoc | doc_flag::CreateAsDeleted);

    auto resp = subdocMultiMutation(cmd);
    EXPECT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());

    // Check the last path was created correctly.
    resp = subdoc_get("txn", SUBDOC_FLAG_XATTR_PATH, doc_flag::AccessDeleted);
    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());
    EXPECT_EQ("\"foo\"", resp.getValue());
}

TEST_P(XattrNoDocTest, AddUserXattrToNonExistentItemMultipathDictUpsert) {
    BinprotSubdocMultiMutationCommand cmd(
            name,
            {{ClientOpcode::SubdocDictUpsert,
              SUBDOC_FLAG_XATTR_PATH,
              "txn",
              "\"bar\""}},
            doc_flag::Mkdoc | doc_flag::CreateAsDeleted);

    auto resp = subdocMultiMutation(cmd);
    EXPECT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());

    resp = subdoc_get("txn", SUBDOC_FLAG_XATTR_PATH, doc_flag::AccessDeleted);
    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());
    EXPECT_EQ("\"bar\"", resp.getValue());
}

TEST_P(XattrNoDocTest, AddUserXattrToNonExistentItemMultipathArrayPushLast) {
    BinprotSubdocMultiMutationCommand cmd(
            name,
            {{ClientOpcode::SubdocArrayPushLast,
              SUBDOC_FLAG_XATTR_PATH,
              "array",
              "1"}},
            doc_flag::Mkdoc | doc_flag::CreateAsDeleted);

    auto resp = subdocMultiMutation(cmd);
    EXPECT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());

    resp = subdoc_get("array", SUBDOC_FLAG_XATTR_PATH, doc_flag::AccessDeleted);
    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());
    EXPECT_EQ("[1]", resp.getValue());
}

TEST_P(XattrNoDocTest, AddUserXattrToNonExistentItemMultipathArrayPushFirst) {
    BinprotSubdocMultiMutationCommand cmd(
            name,
            {{ClientOpcode::SubdocArrayPushFirst,
              SUBDOC_FLAG_XATTR_PATH,
              "array",
              "2"}},
            doc_flag::Mkdoc | doc_flag::CreateAsDeleted);

    auto resp = subdocMultiMutation(cmd);
    EXPECT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());

    resp = subdoc_get("array", SUBDOC_FLAG_XATTR_PATH, doc_flag::AccessDeleted);
    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());
    EXPECT_EQ("[2]", resp.getValue());
}

// @todo MB-39545: This test fails because doc_flag::Mkdoc is invalid with
//  ArrayInsert (the test fails at validation for ArrayInsert with "Invalid
//  arguments").
//  By changing to doc_flag::Add (valid for ArrayInsert) the test gets to the
//  execution of ArrayInsert but is fails with SubdocMultiPathFailure.
//  Specific and simple (single-path) tests on ArrayInsert suggests that we may
//  have some existing issue in that area that we should address in a dedicated
//  patch.
TEST_P(XattrNoDocTest,
       DISABLED_AddUserXattrToNonExistentItemMultipathArrayInsert) {
    BinprotSubdocMultiMutationCommand cmd(
            name,
            {{ClientOpcode::SubdocArrayPushFirst,
              SUBDOC_FLAG_XATTR_PATH,
              "array",
              "0"},
             {ClientOpcode::SubdocArrayInsert,
              SUBDOC_FLAG_XATTR_PATH,
              "array.[0]",
              "1"}},
            doc_flag::Mkdoc | doc_flag::CreateAsDeleted);

    auto resp = subdocMultiMutation(cmd);
    EXPECT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());

    resp = subdoc_get("array", SUBDOC_FLAG_XATTR_PATH, doc_flag::AccessDeleted);
    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());
    EXPECT_EQ("[0,1]", resp.getValue());
}

TEST_P(XattrNoDocTest, AddUserXattrToNonExistentItemMultipathArrayAddUnique) {
    BinprotSubdocMultiMutationCommand cmd(
            name,
            {{ClientOpcode::SubdocArrayAddUnique,
              SUBDOC_FLAG_XATTR_PATH,
              "array",
              "4"}},
            doc_flag::Mkdoc | doc_flag::CreateAsDeleted);

    auto resp = subdocMultiMutation(cmd);
    EXPECT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());

    resp = subdoc_get("array", SUBDOC_FLAG_XATTR_PATH, doc_flag::AccessDeleted);
    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());
    EXPECT_EQ("[4]", resp.getValue());
}

TEST_P(XattrNoDocTest, AddUserXattrToNonExistentItemMultipathCounter) {
    BinprotSubdocMultiMutationCommand cmd(
            name,
            {{ClientOpcode::SubdocCounter,
              SUBDOC_FLAG_XATTR_PATH,
              "counter",
              "5"}},
            doc_flag::Mkdoc | doc_flag::CreateAsDeleted);

    auto resp = subdocMultiMutation(cmd);
    EXPECT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());

    // Check the last path was created correctly.
    resp = subdoc_get(
            "counter", SUBDOC_FLAG_XATTR_PATH, doc_flag::AccessDeleted);
    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());
    EXPECT_EQ("5", resp.getValue());
}

// Positive test: Can User XAttrs be added to a document which doesn't exist
// (and doesn't have a tombstone) using the new CreateAsDeleted flag, using
// a combination of subdoc-multi mutation types.
TEST_P(XattrNoDocTest, AddUserXattrToNonExistentItemMultipathCombo) {
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addMutation(
            ClientOpcode::SubdocDictAdd, SUBDOC_FLAG_XATTR_PATH, "txn", "{}");
    cmd.addMutation(ClientOpcode::SubdocDictUpsert,
                    SUBDOC_FLAG_XATTR_PATH,
                    "txn.id",
                    "2");
    cmd.addMutation(ClientOpcode::SubdocArrayPushLast,
                    SUBDOC_FLAG_XATTR_PATH,
                    "txn.array",
                    "2");
    cmd.addMutation(ClientOpcode::SubdocArrayPushFirst,
                    SUBDOC_FLAG_XATTR_PATH,
                    "txn.array",
                    "0");
    cmd.addMutation(ClientOpcode::SubdocArrayAddUnique,
                    SUBDOC_FLAG_XATTR_PATH,
                    "txn.array",
                    "3");
    cmd.addMutation(ClientOpcode::SubdocCounter,
                    SUBDOC_FLAG_XATTR_PATH,
                    "txn.counter",
                    "1");

    cmd.addDocFlag(doc_flag::Mkdoc);
    cmd.addDocFlag(doc_flag::CreateAsDeleted);

    auto& conn = getConnection();
    conn.sendCommand(cmd);
    BinprotSubdocResponse resp;
    conn.recvResponse(resp);
    EXPECT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());

    // Check the last path was created correctly.
    resp = subdoc_get(
            "txn.counter", SUBDOC_FLAG_XATTR_PATH, doc_flag::AccessDeleted);
    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());
    EXPECT_EQ("1", resp.getValue());
}
