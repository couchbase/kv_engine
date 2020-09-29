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

#include "testapp.h"
#include "testapp_client_test.h"

#include <algorithm>
#include <platform/compress.h>

class RemoveTest : public TestappXattrClientTest {
protected:
    void verify_MB_22553(const std::string& config);

    /**
     * Create a document and keep the information about the document in
     * the info member
     */
    void createDocument() {
        info = getConnection().mutate(document, Vbid(0), MutationType::Add);
    }

    MutationInfo info;
};

void RemoveTest::verify_MB_22553(const std::string& config) {
    // default (memcache) bucket only test.
    if (mcd_env->getTestBucket().getName() != "default_engine") {
        return;
    }

    auto& conn = getAdminConnection();
    conn.deleteBucket(bucketName);
    mcd_env->getTestBucket().setUpBucket(bucketName, config, conn);

    // Create a document with an XATTR.
    conn = getConnection();
    setBodyAndXattr(
            conn, "foobar", {{"_rbac", R"({"attribute": "read-only"})"}});

    // Delete the document
    conn.remove(name, Vbid(0));

    // The document itself should not be accessible MB-22553
    try {
        conn.get(name, Vbid(0));
        FAIL() << "Document with XATTRs should not be accessible after remove";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound())
                    << "MB-22553: doc with xattr is still accessible";
    }

    // It should not be accessible over subdoc.
    auto resp =
            subdoc(conn, cb::mcbp::ClientOpcode::SubdocGet, name, "verbosity");
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, resp.getStatus())
            << "MB-22553: doc with xattr is still accessible";
}

INSTANTIATE_TEST_SUITE_P(
        TransportProtocols,
        RemoveTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpPlain,
                                             TransportProtocols::McbpSsl),
                           ::testing::Values(XattrSupport::Yes),
                           ::testing::Values(ClientJSONSupport::Yes,
                                             ClientJSONSupport::No),
                           ::testing::Values(ClientSnappySupport::Yes,
                                             ClientSnappySupport::No)),
        PrintToStringCombinedName());

/**
 * Verify that remove of an non-existing object work (and return the expected
 * value)
 */
TEST_P(RemoveTest, RemoveNonexisting) {
    auto& conn = getConnection();

    try {
        conn.remove(name, Vbid(0));
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isNotFound()) << error.what();
    }
}

/**
 * Verify that remove of an existing document with setting the CAS value
 * to the wildcard works
 */
TEST_P(RemoveTest, RemoveCasWildcard) {
    auto& conn = getConnection();

    createDocument();
    auto deleted = conn.remove(name, Vbid(0));
    EXPECT_NE(info.cas, deleted.cas);
}

/**
 * Verify that remove of an existing document with an incorrect value
 * fails with EEXISTS
 */
TEST_P(RemoveTest, RemoveWithInvalidCas) {
    auto& conn = getConnection();
    createDocument();
    try {
        conn.remove(name, Vbid(0), info.cas + 1);
        FAIL() << "Invalid cas should return EEXISTS";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isAlreadyExists()) << error.what();
    }
}

/**
 * Verify that remove of an existing document with the correct CAS
 * value works
 */
TEST_P(RemoveTest, RemoveWithCas) {
    auto& conn = getConnection();

    createDocument();
    auto deleted = conn.remove(name, Vbid(0), info.cas);
    EXPECT_NE(info.cas, deleted.cas);
}

/**
 * Verify that you may access system attributes of a deleted
 * document, and that the user attributes will be nuked off
 */
TEST_P(RemoveTest, RemoveWithXattr) {
    auto& conn = getConnection();
    setBodyAndXattr(
            conn,
            document.value,
            {{"meta", R"({"content-type": "application/json; charset=utf-8"})"},
             {"_rbac", R"({"attribute": "read-only"})"}});
    conn.remove(name, Vbid(0), 0);

    // The system xattr should have been preserved
    const auto status = getXattr(conn, "_rbac.attribute", true);
    if (status.getStatus() == cb::mcbp::Status::Success) {
        EXPECT_EQ("\"read-only\"", status.getValue());
    }

    // The user xattr should not be there
    try {
        if (getXattr(conn, "meta.content_type", true).getStatus() !=
            xattrOperationStatus) {
            FAIL() << "The user xattr should be gone!";
        }
    } catch (const ConnectionError& exp) {
        EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, exp.getReason())
                << to_string(exp.getReason());
    }
}

/**
 * Verify that you cannot get a document (with xattrs) which is deleted
 */
TEST_P(RemoveTest, MB_22553_DeleteDocWithXAttr_keep_deleted) {
    verify_MB_22553("keep_deleted=true");}

/**
 * Verify that you cannot get a document (with xattrs) which is deleted
 * when the memcached bucket isn't using the keep deleted flag
 */
TEST_P(RemoveTest, MB_22553_DeleteDocWithXAttr) {
    verify_MB_22553("keep_deleted=false");
}
