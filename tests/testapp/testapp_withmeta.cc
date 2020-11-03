/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <protocol/connection/client_mcbp_commands.h>
#include "testapp.h"
#include "testapp_client_test.h"

#include <utilities/string_utilities.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

class WithMetaTest : public TestappXattrClientTest {
public:
    void SetUp() override {
        TestappXattrClientTest::SetUp();
        document.info.cas = testCas; // Must have a cas for meta ops
    }

    /**
     * Check the CAS of the set document against our value
     * using vattr for the lookup
     */
    void checkCas() {
        auto& conn = getConnection();
        BinprotSubdocCommand cmd;
        cmd.setOp(cb::mcbp::ClientOpcode::SubdocGet);
        cmd.setKey(name);
        cmd.setPath("$document");
        cmd.addPathFlags(SUBDOC_FLAG_XATTR_PATH);
        cmd.addDocFlags(mcbp::subdoc::doc_flag::None);

        auto resp = conn.execute(cmd);

        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        auto json = nlohmann::json::parse(resp.getDataString());
        EXPECT_STREQ(testCasStr, json["CAS"].get<std::string>().c_str());
    }

    /**
     * Make ::document an xattr value
     */
    void makeDocumentXattrValue() {
        cb::xattr::Blob blob;
        blob.set("user", "{\"author\":\"bubba\"}");
        blob.set("meta", "{\"content-type\":\"text\"}");

        auto xattrValue = blob.finalize();

        // append body to the xattrs and store in data
        std::string body = "document_body";
        document.value.clear();
        std::copy_n(xattrValue.buf,
                    xattrValue.len,
                    std::back_inserter(document.value));
        std::copy_n(
                body.c_str(), body.size(), std::back_inserter(document.value));
        cb::const_char_buffer xattr{(char*)document.value.data(),
                                    document.value.size()};

        document.info.datatype = cb::mcbp::Datatype::Xattr;

        if (hasSnappySupport() == ClientSnappySupport::Yes) {
            document.compress();
        }
    }

protected:
    /**
     * Test that DelWithMeta accepts user-xattrs in the payload.
     *
     * @param allowValuePruning Whether the engine sanitizes bad user payloads
     *  or fails the request
     * @param compressed Whether the payload is compressed
     */
    void testDeleteWithMetaAcceptsUserXattrs(bool allowValuePruning,
                                             bool compressed = false);

    /**
     * Test that DelWithMeta rejects body in the payload.
     *
     * @param allowValuePruning Whether the engine sanitizes bad user payloads
     *  or fails the request
     * @param dtXattr Whether the value under test is DT Xattr
     */
    void testDeleteWithMetaRejectsBody(bool allowValuePruning, bool dtXattr);

    const uint64_t testCas = 0xb33ff00dcafef00dull;
    const char* testCasStr = "0xb33ff00dcafef00d";
};

INSTANTIATE_TEST_CASE_P(
        TransportProtocols,
        WithMetaTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpPlain,
                                             TransportProtocols::McbpSsl),
                           ::testing::Values(XattrSupport::Yes,
                                             XattrSupport::No),
                           ::testing::Values(ClientJSONSupport::Yes,
                                             ClientJSONSupport::No),
                           ::testing::Values(ClientSnappySupport::Yes,
                                             ClientSnappySupport::No)),
        PrintToStringCombinedName());

TEST_P(WithMetaTest, basicSet) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::SetWithMeta);

    MutationInfo resp;
    try {
        resp = getConnection().mutateWithMeta(document,
                                              Vbid(0),
                                              mcbp::cas::Wildcard,
                                              /*seqno*/ 1,
                                              /*options*/ 0,
                                              {});
    } catch (std::exception&) {
        FAIL() << "mutateWithMeta threw an exception";
    }

    if (::testing::get<1>(GetParam()) == XattrSupport::Yes) {
        checkCas();
    }
}

TEST_P(WithMetaTest, basicSetXattr) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::SetWithMeta);
    makeDocumentXattrValue();

    MutationInfo resp;
    try {
        resp = getConnection().mutateWithMeta(document,
                                              Vbid(0),
                                              mcbp::cas::Wildcard,
                                              /*seqno*/ 1,
                                              /*options*/ 0,
                                              {});
        EXPECT_EQ(XattrSupport::Yes, ::testing::get<1>(GetParam()));
        EXPECT_EQ(testCas, ntohll(resp.cas));
    } catch (std::exception&) {
        EXPECT_EQ(XattrSupport::No, ::testing::get<1>(GetParam()));
    }

    if (::testing::get<1>(GetParam()) == XattrSupport::Yes) {
        checkCas();
    }
}

TEST_P(WithMetaTest, MB36304_DocumetTooBig) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::SetWithMeta);
    if (::testing::get<3>(GetParam()) == ClientSnappySupport::No) {
        return;
    }

    std::vector<char> blob(21 * 1024 * 1024);
    cb::compression::Buffer deflated;
    ASSERT_TRUE(cb::compression::deflate(cb::compression::Algorithm::Snappy,
                                         {blob.data(), blob.size()},
                                         deflated));
    cb::const_char_buffer doc = deflated;
    document.value.clear();
    std::copy(doc.begin(), doc.end(), std::back_inserter(document.value));
    document.info.datatype = cb::mcbp::Datatype::Snappy;
    try {
        getConnection().mutateWithMeta(
                document, Vbid(0), mcbp::cas::Wildcard, 1, 0, {});
        FAIL() << "It should not be possible to store documents which exceeds "
                  "the max document size";
    } catch (const ConnectionError& error) {
        EXPECT_EQ(cb::mcbp::Status::E2big, error.getReason());
    }
}

TEST_P(WithMetaTest, MB36304_DocumentMaxSizeWithXattr) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::SetWithMeta);
    if (::testing::get<1>(GetParam()) == XattrSupport::No) {
        return;
    }

    document.value.clear();
    cb::xattr::Blob blob;
    blob.set("_sys", R"({"author":"bubba"})");
    auto xattrValue = blob.finalize();
    std::copy(xattrValue.begin(),
              xattrValue.end(),
              std::back_inserter(document.value));

    document.value.resize((20 * 1024 * 1024) + xattrValue.size());
    document.info.datatype = cb::mcbp::Datatype::Xattr;
    auto& conn = getConnection();
    conn.mutateWithMeta(document, Vbid(0), mcbp::cas::Wildcard, 1, 0, {});
    conn.remove(name, Vbid(0));
}

TEST_P(WithMetaTest, MB36321_DeleteWithMetaRefuseUserXattrs) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::DelWithMeta);
    if (::testing::get<1>(GetParam()) == XattrSupport::No ||
        ::testing::get<2>(GetParam()) == ClientJSONSupport::No) {
        return;
    }

    {
        auto& conn = getAdminConnection();
        conn.selectBucket(bucketName);
        const auto setParam = BinprotSetParamCommand(
                cb::mcbp::request::SetParamPayload::Type::Flush,
                "allow_del_with_meta_prune_user_data",
                "false");
        const auto resp = BinprotMutationResponse(conn.execute(setParam));
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    }

    cb::xattr::Blob blob;
    blob.set("user", R"({"band":"Steel Panther"})");
    auto xattrValue = blob.finalize();

    document.value.clear();
    std::copy(xattrValue.begin(),
              xattrValue.end(),
              std::back_inserter(document.value));
    document.value.append(R"({"Bug":"MB-36321"})");
    using cb::mcbp::Datatype;
    document.info.datatype =
            Datatype(uint8_t(Datatype::Xattr) | uint8_t(Datatype::JSON));
    auto& conn = getConnection();

    BinprotDelWithMetaCommand cmd(document, Vbid(0), 0, 0, 1, 0);
    const auto rsp = BinprotMutationResponse(conn.execute(cmd));
    ASSERT_FALSE(rsp.isSuccess()) << rsp.getStatus();
    auto error = nlohmann::json::parse(rsp.getDataString());

    ASSERT_EQ(
            "It is only possible to specify Xattrs as a value to "
            "DeleteWithMeta",
            error["error"]["context"]);
}

TEST_P(WithMetaTest, MB36321_DeleteWithMetaAllowSystemXattrs) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::DelWithMeta);
    if (::testing::get<1>(GetParam()) == XattrSupport::No ||
        ::testing::get<2>(GetParam()) == ClientJSONSupport::No) {
        return;
    }

    cb::xattr::Blob blob;
    blob.set("_sys", R"({"author":"bubba"})");
    auto xattrValue = blob.finalize();

    document.value.clear();
    std::copy(xattrValue.begin(),
              xattrValue.end(),
              std::back_inserter(document.value));
    document.info.datatype = cb::mcbp::Datatype::Xattr;
    auto& conn = getConnection();

    BinprotDelWithMetaCommand cmd(document, Vbid(0), 0, 0, 1, 0);
    const auto rsp = BinprotMutationResponse(conn.execute(cmd));
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus();

    // The system xattr should be there
    auto sresp = subdoc(cb::mcbp::ClientOpcode::SubdocGet,
                        name,
                        "_sys.author",
                        {},
                        SUBDOC_FLAG_XATTR_PATH,
                        mcbp::subdoc::doc_flag::AccessDeleted);
    EXPECT_TRUE(sresp.isSuccess()) << sresp.getStatus();
}

void WithMetaTest::testDeleteWithMetaAcceptsUserXattrs(bool allowValuePruning,
                                                       bool compressed) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::DelWithMeta);
    if (::testing::get<1>(GetParam()) == XattrSupport::No ||
        ::testing::get<2>(GetParam()) == ClientJSONSupport::No) {
        return;
    }

    {
        auto& conn = getAdminConnection();
        conn.selectBucket(bucketName);
        const auto setParam = BinprotSetParamCommand(
                cb::mcbp::request::SetParamPayload::Type::Flush,
                "allow_del_with_meta_prune_user_data",
                allowValuePruning ? "true" : "false");
        const auto resp = BinprotMutationResponse(conn.execute(setParam));
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    }

    // Value with User/Sys Xattrs and no Body
    cb::xattr::Blob blob;
    blob.set("user", R"({"a":"b"})");
    blob.set("_sys", R"({"c":"d"})");
    const auto xattrs = blob.finalize();
    document.value.clear();
    std::copy(xattrs.begin(), xattrs.end(), std::back_inserter(document.value));
    using Datatype = cb::mcbp::Datatype;
    document.info.datatype = Datatype::Xattr;

    if (compressed) {
        cb::compression::Buffer deflated;
        ASSERT_TRUE(cb::compression::deflate(
                cb::compression::Algorithm::Snappy,
                {document.value.c_str(), document.value.size()},
                deflated));
        document.value = {deflated.data(), deflated.size()};
        document.info.datatype =
                Datatype(static_cast<uint8_t>(cb::mcbp::Datatype::Xattr) |
                         static_cast<uint8_t>(cb::mcbp::Datatype::Snappy));
    }

    BinprotDelWithMetaCommand delWithMeta(
            document, Vbid(0), 0, 0 /*delTime*/, 1 /*seqno*/, 0 /*opCas*/);
    auto& conn = getConnection();
    const auto resp = BinprotMutationResponse(conn.execute(delWithMeta));
    const auto expectedStatus = compressed && ::testing::get<3>(GetParam()) ==
                                                        ClientSnappySupport::No
                                        ? cb::mcbp::Status::Einval
                                        : cb::mcbp::Status::Success;
    EXPECT_EQ(expectedStatus, resp.getStatus());
}

TEST_P(WithMetaTest, DeleteWithMetaAcceptsUserXattrs) {
    testDeleteWithMetaAcceptsUserXattrs(false);
}

TEST_P(WithMetaTest, DeleteWithMetaAcceptsUserXattrs_AllowValuePruning) {
    testDeleteWithMetaAcceptsUserXattrs(true);
}

TEST_P(WithMetaTest, DeleteWithMetaAcceptsUserXattrs_Compressed) {
    testDeleteWithMetaAcceptsUserXattrs(false, true);
}

void WithMetaTest::testDeleteWithMetaRejectsBody(bool allowValuePruning,
                                                 bool dtXattr) {
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::DelWithMeta);
    if (::testing::get<1>(GetParam()) == XattrSupport::No ||
        ::testing::get<2>(GetParam()) == ClientJSONSupport::No) {
        return;
    }

    {
        auto& conn = getAdminConnection();
        conn.selectBucket(bucketName);
        const auto setParam = BinprotSetParamCommand(
                cb::mcbp::request::SetParamPayload::Type::Flush,
                "allow_del_with_meta_prune_user_data",
                allowValuePruning ? "true" : "false");
        const auto resp = BinprotMutationResponse(conn.execute(setParam));
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    }

    // Value with User/Sys Xattrs and Body
    if (dtXattr) {
        cb::xattr::Blob blob;
        blob.set("user", R"({"a":"b"})");
        blob.set("_sys", R"({"c":"d"})");
        const auto xattrs = blob.finalize();
        document.value.clear();
        std::copy(xattrs.begin(),
                  xattrs.end(),
                  std::back_inserter(document.value));
        document.value.append("body");
        document.info.datatype = cb::mcbp::Datatype::Xattr;
    } else {
        document.value = "body";
        document.info.datatype = cb::mcbp::Datatype::Raw;
    }

    BinprotDelWithMetaCommand delWithMeta(
            document, Vbid(0), 0, 0 /*delTime*/, 1 /*seqno*/, 0 /*opCas*/);
    auto& conn = getConnection();
    const auto resp = BinprotMutationResponse(conn.execute(delWithMeta));
    if (allowValuePruning) {
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    } else {
        ASSERT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
        ASSERT_TRUE(
                resp.getDataString().find("only possible to specify Xattrs") !=
                std::string::npos);
    }
}

TEST_P(WithMetaTest, DeleteWithMetaRejectsBody_AllowValuePruning) {
    testDeleteWithMetaRejectsBody(true, false);
}

TEST_P(WithMetaTest, DeleteWithMetaRejectsBody) {
    testDeleteWithMetaRejectsBody(false, false);
}

TEST_P(WithMetaTest, DeleteWithMetaRejectsBody_AllowValuePruning_DTXattr) {
    testDeleteWithMetaRejectsBody(true, true);
}

TEST_P(WithMetaTest, DeleteWithMetaRejectsBody_DTXattr) {
    testDeleteWithMetaRejectsBody(false, true);
}