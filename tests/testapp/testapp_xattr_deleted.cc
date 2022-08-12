/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * Test extended attribute functionality related to the 'CreateAsDeleted'
 * doc flag.
 *
 * Initial use-case for this doc flag is Transactions.
 */

#include "testapp_xattr.h"

using namespace cb::mcbp::subdoc;
using namespace cb::mcbp;

// Negative test: The subdoc operation returns Einval as
// doc_flag::CreateAsDeleted requires one of doc_flag::Mkdoc/Add.
void XattrNoDocTest::testRequiresMkdocOrAdd() {
    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictAdd,
                       name,
                       "txn.deleted",
                       "true",
                       SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P,
                       doc_flag::CreateAsDeleted,
                       durReqs);
    EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
}

TEST_P(XattrNoDocTest, RequiresMkdocOrAdd) {
    testRequiresMkdocOrAdd();
}

TEST_P(XattrNoDocDurabilityTest, RequiresMkdocOrAdd) {
    testRequiresMkdocOrAdd();
}

// Negative test: The Subdoc CreateAsDeleted doesn't allow to write in the
// body.
void XattrNoDocTest::testRequiresXattrPath() {
    // Note: subdoc-flags doesn't set SUBDOC_FLAG_XATTR_PATH
    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictAdd,
                       name,
                       "txn.deleted",
                       "true",
                       SUBDOC_FLAG_MKDIR_P,
                       doc_flag::Mkdoc | doc_flag::CreateAsDeleted);
    EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
}

TEST_P(XattrNoDocTest, RequiresXattrPath) {
    testRequiresXattrPath();
}

TEST_P(XattrNoDocDurabilityTest, RequiresXattrPath) {
    testRequiresXattrPath();
}

// Positive test: Can User XAttrs be added to a document which doesn't exist
// (and doesn't have a tombstone) using the new CreateAsDeleted flag.
void XattrNoDocTest::testSinglePathDictAdd() {
    if (durReqs && !supportSyncRepl()) {
        GTEST_SKIP();
    }

    // let's add a user XATTR to a non-existing document
    auto resp = subdoc(cb::mcbp::ClientOpcode::SubdocDictAdd,
                       name,
                       "txn.deleted",
                       "true",
                       SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P,
                       doc_flag::Mkdoc | doc_flag::CreateAsDeleted,
                       durReqs);
    EXPECT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());

    // Check that the User XATTR is present.
    resp = subdoc_get(
            "txn.deleted", SUBDOC_FLAG_XATTR_PATH, doc_flag::AccessDeleted);
    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());
    EXPECT_EQ("true", resp.getValue());

    // Check that the value is deleted and empty - which is treated as
    // not existing with normal subdoc operations.
    auto lookup = subdoc_multi_lookup(
            {{cb::mcbp::ClientOpcode::Get, SUBDOC_FLAG_NONE, ""}},
            cb::mcbp::subdoc::doc_flag::AccessDeleted);
    EXPECT_EQ(cb::mcbp::Status::SubdocMultiPathFailureDeleted,
              lookup.getStatus());

    // Also check via getMeta - which should show the document exists but
    // as deleted.
    auto meta = get_meta();
    EXPECT_TRUE(mcbp::datatype::is_xattr(meta.datatype));
    EXPECT_EQ(1, meta.deleted);
}

TEST_P(XattrNoDocTest, SinglePathDictAdd) {
    testSinglePathDictAdd();
}

TEST_P(XattrNoDocDurabilityTest, SinglePathDictAdd) {
    testSinglePathDictAdd();
}

// Positive tests: Can User XAttrs be added to a document which doesn't exist
// (and doesn't have a tombstone) using the new CreateAsDeleted flag, using
// multi-mutation with each mutation opcode.
void XattrNoDocTest::testMultipathDictAdd() {
    if (durReqs && !supportSyncRepl()) {
        GTEST_SKIP();
    }

    BinprotSubdocMultiMutationCommand cmd(
            name,
            {{ClientOpcode::SubdocDictAdd,
              SUBDOC_FLAG_XATTR_PATH,
              "txn",
              "\"foo\""}},
            doc_flag::Mkdoc | doc_flag::CreateAsDeleted,
            durReqs);

    auto resp = subdocMultiMutation(cmd);
    EXPECT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());

    // Check the last path was created correctly.
    resp = subdoc_get("txn", SUBDOC_FLAG_XATTR_PATH, doc_flag::AccessDeleted);
    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());
    EXPECT_EQ("\"foo\"", resp.getValue());

    // Check that the value is deleted and empty - which is treated as
    // not existing with normal subdoc operations.
    auto lookup = subdoc_multi_lookup(
            {{cb::mcbp::ClientOpcode::Get, SUBDOC_FLAG_NONE, ""}},
            cb::mcbp::subdoc::doc_flag::AccessDeleted);
    EXPECT_EQ(cb::mcbp::Status::SubdocMultiPathFailureDeleted,
              lookup.getStatus());

    // Also check via getMeta - which should show the document exists but
    // as deleted.
    auto meta = get_meta();
    EXPECT_TRUE(mcbp::datatype::is_xattr(meta.datatype));
    EXPECT_EQ(1, meta.deleted);
}

TEST_P(XattrNoDocTest, MultipathDictAdd) {
    testMultipathDictAdd();
}

TEST_P(XattrNoDocDurabilityTest, MultipathDictAdd) {
    testMultipathDictAdd();
}

void XattrNoDocTest::testMultipathDictUpsert() {
    if (durReqs && !supportSyncRepl()) {
        GTEST_SKIP();
    }

    BinprotSubdocMultiMutationCommand cmd(
            name,
            {{ClientOpcode::SubdocDictUpsert,
              SUBDOC_FLAG_XATTR_PATH,
              "txn",
              "\"bar\""}},
            doc_flag::Mkdoc | doc_flag::CreateAsDeleted,
            durReqs);

    auto resp = subdocMultiMutation(cmd);
    EXPECT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());

    resp = subdoc_get("txn", SUBDOC_FLAG_XATTR_PATH, doc_flag::AccessDeleted);
    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());
    EXPECT_EQ("\"bar\"", resp.getValue());
}

TEST_P(XattrNoDocTest, MultipathDictUpsert) {
    testMultipathDictUpsert();
}

TEST_P(XattrNoDocDurabilityTest, MultipathDictUpsert) {
    testMultipathDictUpsert();
}

void XattrNoDocTest::testMultipathArrayPushLast() {
    if (durReqs && !supportSyncRepl()) {
        GTEST_SKIP();
    }

    BinprotSubdocMultiMutationCommand cmd(
            name,
            {{ClientOpcode::SubdocArrayPushLast,
              SUBDOC_FLAG_XATTR_PATH,
              "array",
              "1"}},
            doc_flag::Mkdoc | doc_flag::CreateAsDeleted,
            durReqs);

    auto resp = subdocMultiMutation(cmd);
    EXPECT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());

    resp = subdoc_get("array", SUBDOC_FLAG_XATTR_PATH, doc_flag::AccessDeleted);
    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());
    EXPECT_EQ("[1]", resp.getValue());
}

TEST_P(XattrNoDocTest, MultipathArrayPushLast) {
    testMultipathArrayPushLast();
}

TEST_P(XattrNoDocDurabilityTest, MultipathArrayPushLast) {
    testMultipathArrayPushLast();
}

void XattrNoDocTest::testMultipathArrayPushFirst() {
    if (durReqs && !supportSyncRepl()) {
        GTEST_SKIP();
    }

    BinprotSubdocMultiMutationCommand cmd(
            name,
            {{ClientOpcode::SubdocArrayPushFirst,
              SUBDOC_FLAG_XATTR_PATH,
              "array",
              "2"}},
            doc_flag::Mkdoc | doc_flag::CreateAsDeleted,
            durReqs);

    auto resp = subdocMultiMutation(cmd);
    EXPECT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());

    resp = subdoc_get("array", SUBDOC_FLAG_XATTR_PATH, doc_flag::AccessDeleted);
    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());
    EXPECT_EQ("[2]", resp.getValue());
}

TEST_P(XattrNoDocTest, MultipathArrayPushFirst) {
    testMultipathArrayPushFirst();
}

TEST_P(XattrNoDocDurabilityTest, MultipathArrayPushFirst) {
    testMultipathArrayPushFirst();
}

// @todo MB-39545: This test fails because doc_flag::Mkdoc is invalid with
//  ArrayInsert (the test fails at validation for ArrayInsert with "Invalid
//  arguments").
//  By changing to doc_flag::Add (valid for ArrayInsert) the test gets to the
//  execution of ArrayInsert but is fails with SubdocMultiPathFailure.
//  Specific and simple (single-path) tests on ArrayInsert suggests that we may
//  have some existing issue in that area that we should address in a dedicated
//  patch.
TEST_P(XattrNoDocTest, DISABLED_MultipathArrayInsert) {
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
            doc_flag::Mkdoc | doc_flag::CreateAsDeleted,
            durReqs);

    auto resp = subdocMultiMutation(cmd);
    EXPECT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());

    resp = subdoc_get("array", SUBDOC_FLAG_XATTR_PATH, doc_flag::AccessDeleted);
    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());
    EXPECT_EQ("[0,1]", resp.getValue());
}

void XattrNoDocTest::testMultipathArrayAddUnique() {
    if (durReqs && !supportSyncRepl()) {
        GTEST_SKIP();
    }

    BinprotSubdocMultiMutationCommand cmd(
            name,
            {{ClientOpcode::SubdocArrayAddUnique,
              SUBDOC_FLAG_XATTR_PATH,
              "array",
              "4"}},
            doc_flag::Mkdoc | doc_flag::CreateAsDeleted,
            durReqs);

    auto resp = subdocMultiMutation(cmd);
    EXPECT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());

    resp = subdoc_get("array", SUBDOC_FLAG_XATTR_PATH, doc_flag::AccessDeleted);
    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());
    EXPECT_EQ("[4]", resp.getValue());
}

TEST_P(XattrNoDocTest, MultipathArrayAddUnique) {
    testMultipathArrayAddUnique();
}

TEST_P(XattrNoDocDurabilityTest, MultipathArrayAddUnique) {
    testMultipathArrayAddUnique();
}

void XattrNoDocTest::testMultipathCounter() {
    if (durReqs && !supportSyncRepl()) {
        GTEST_SKIP();
    }

    BinprotSubdocMultiMutationCommand cmd(
            name,
            {{ClientOpcode::SubdocCounter,
              SUBDOC_FLAG_XATTR_PATH,
              "counter",
              "5"}},
            doc_flag::Mkdoc | doc_flag::CreateAsDeleted,
            durReqs);

    auto resp = subdocMultiMutation(cmd);
    EXPECT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());

    // Check the last path was created correctly.
    resp = subdoc_get(
            "counter", SUBDOC_FLAG_XATTR_PATH, doc_flag::AccessDeleted);
    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());
    EXPECT_EQ("5", resp.getValue());
}

TEST_P(XattrNoDocTest, MultipathCounter) {
    testMultipathCounter();
}

TEST_P(XattrNoDocDurabilityTest, MultipathCounter) {
    testMultipathCounter();
}

std::ostream& operator<<(std::ostream& os,
                         const BinprotSubdocMultiMutationResponse& resp) {
    auto& results = resp.getResults();
    auto getValue = [](const std::string& s) {
        if (s.empty()) {
            return s;
        }
        return " [" + s + "]";
    };
    for (const auto& r : results) {
        os << "\tindex " << uint32_t(r.index) << ": " << to_string(r.status)
           << getValue(r.value) << std::endl;
    }
    return os;
}

// Positive test: Can User XAttrs be added to a document which doesn't exist
// using the new CreateAsDeleted flag, using a combination of subdoc-multi
// mutation types.
void XattrNoDocTest::testMultipathCombo() {
    if (durReqs && !supportSyncRepl()) {
        GTEST_SKIP();
    }

    auto cmd = makeSDKTxnMultiMutation();
    cmd.addDocFlag(doc_flag::Mkdoc);
    cmd.addDocFlag(doc_flag::CreateAsDeleted);

    if (durReqs) {
        cmd.setDurabilityReqs(*durReqs);
    }

    userConnection->sendCommand(cmd);
    BinprotSubdocMultiMutationResponse resp;
    userConnection->recvResponse(resp);

    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus()) << resp;

    // Check the last path was created correctly.
    auto resp2 = subdoc_get(
            "txn.counter", SUBDOC_FLAG_XATTR_PATH, doc_flag::AccessDeleted);
    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp2.getStatus());
    EXPECT_EQ("1", resp2.getValue());
}

TEST_P(XattrNoDocTest, MultipathCombo) {
    testMultipathCombo();
}

TEST_P(XattrNoDocDurabilityTest, MultipathCombo) {
    testMultipathCombo();
}

// Positive test: Can User XAttrs be added to a document which doesn't exist
// (and doesn't have a tombstone) using the new CreateAsDeleted flag alongside
// AccessDeleted (to check for an existing tombstone).
// This is also a regression test for MB-40162.
void XattrNoDocTest::testMultipathAccessDeletedCreateAsDeleted() {
    if (durReqs && !supportSyncRepl()) {
        GTEST_SKIP();
    }

    auto cmd = makeSDKTxnMultiMutation();

    cmd.addDocFlag(doc_flag::Add);
    cmd.addDocFlag(doc_flag::AccessDeleted);
    cmd.addDocFlag(doc_flag::CreateAsDeleted);

    if (durReqs) {
        cmd.setDurabilityReqs(*durReqs);
    }

    userConnection->sendCommand(cmd);
    BinprotSubdocResponse resp;
    userConnection->recvResponse(resp);
    EXPECT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());

    // Check the last path was created correctly.
    resp = subdoc_get(
            "txn.counter", SUBDOC_FLAG_XATTR_PATH, doc_flag::AccessDeleted);
    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());
    EXPECT_EQ("1", resp.getValue());
}

TEST_P(XattrNoDocTest, MultipathAccessDeletedCreateAsDeleted) {
    testMultipathAccessDeletedCreateAsDeleted();
}

TEST_P(XattrNoDocDurabilityTest, MultipathAccessDeletedCreateAsDeleted) {
    testMultipathAccessDeletedCreateAsDeleted();
}

TEST_P(XattrNoDocTest, ReplaceBodyWithXattr_DeletedDocument) {
    uint64_t cas;
    {
        BinprotSubdocMultiMutationCommand cmd;
        cmd.setKey(name);
        cmd.addDocFlag(doc_flag::Add);
        cmd.addDocFlag(doc_flag::AccessDeleted);
        cmd.addDocFlag(doc_flag::CreateAsDeleted);
        cmd.addMutation(
                cb::mcbp::ClientOpcode::SubdocDictUpsert,
                SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P,
                "tnx.op.staged",
                R"({"couchbase": {"version": "cheshire-cat", "next_version": "unknown"}})");
        userConnection->sendCommand(cmd);

        BinprotSubdocMultiMutationResponse multiResp;
        userConnection->recvResponse(multiResp);
        ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, multiResp.getStatus())
                << "Failed to update the document to expand the Input macros"
                << std::endl
                << multiResp;
        cas = multiResp.getCas();
    }

    // Replace the body with the staged value and remove that
    {
        BinprotSubdocMultiMutationCommand cmd;
        cmd.setCas(cas);
        cmd.setKey(name);
        cmd.addDocFlag(doc_flag::AccessDeleted);
        cmd.addDocFlag(doc_flag::ReviveDocument);
        cmd.addMutation(cb::mcbp::ClientOpcode::SubdocReplaceBodyWithXattr,
                        SUBDOC_FLAG_XATTR_PATH,
                        "tnx.op.staged",
                        {});
        cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDelete,
                        SUBDOC_FLAG_XATTR_PATH,
                        "tnx",
                        {});
        userConnection->sendCommand(cmd);

        BinprotSubdocMultiMutationResponse multiResp;
        userConnection->recvResponse(multiResp);
        ASSERT_EQ(cb::mcbp::Status::Success, multiResp.getStatus())
                << multiResp;
    }

    // Verify that things looks like we expect them to
    {
        auto resp = subdoc_multi_lookup(
                {{ClientOpcode::SubdocGet, SUBDOC_FLAG_XATTR_PATH, "tnx.op"},
                 {ClientOpcode::SubdocGet,
                  SUBDOC_FLAG_NONE,
                  "couchbase.version"}});
        ASSERT_EQ(cb::mcbp::Status::SubdocMultiPathFailure, resp.getStatus());
        auto& results = resp.getResults();
        ASSERT_EQ(cb::mcbp::Status::SubdocPathEnoent, results[0].status);
        ASSERT_EQ(cb::mcbp::Status::Success, results[1].status);
        ASSERT_EQ(R"("cheshire-cat")", results[1].value);
    }
}

/// Verify that Revive on a document which _isn't_ deleted fails
TEST_P(XattrNoDocTest, ReviveRequireDeletedDocument) {
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey(name);
    cmd.addDocFlag(doc_flag::Add);
    cmd.addMutation(
            cb::mcbp::ClientOpcode::SubdocDictUpsert,
            SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P,
            "tnx.op.staged",
            R"({"couchbase": {"version": "cheshire-cat", "next_version": "unknown"}})");
    userConnection->sendCommand(cmd);

    BinprotSubdocMultiMutationResponse resp;
    userConnection->recvResponse(resp);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus()) << resp;

    cmd = {};
    cmd.setKey(name);
    cmd.addDocFlag(doc_flag::AccessDeleted);
    cmd.addDocFlag(doc_flag::ReviveDocument);
    cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                    SUBDOC_FLAG_XATTR_PATH,
                    "tnx.bubba",
                    R"("This should fail")");
    userConnection->sendCommand(cmd);
    userConnection->recvResponse(resp);
    ASSERT_EQ(cb::mcbp::Status::SubdocCanOnlyReviveDeletedDocuments,
              resp.getStatus());
}
