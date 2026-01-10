/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp.h"
#include "testapp_client_test.h"

#include <mcbp/codec/mutate_with_meta_payload.h>
#include <platform/string_hex.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

/// Find all offsets of a pattern in a string
static std::vector<std::size_t> findAllOffsets(std::string_view haystack,
                                               std::string_view needle) {
    std::vector<std::size_t> offsets;
    std::size_t pos = 0;
    while ((pos = haystack.find(needle, pos)) != std::string_view::npos) {
        offsets.push_back(pos);
        pos += needle.size();
    }
    return offsets;
}

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
        BinprotSubdocCommand cmd;
        cmd.setOp(cb::mcbp::ClientOpcode::SubdocGet);
        cmd.setKey(name);
        cmd.setPath("$document");
        cmd.addPathFlags(cb::mcbp::subdoc::PathFlag::XattrPath);
        cmd.addDocFlags(cb::mcbp::subdoc::DocFlag::None);

        auto resp = userConnection->execute(cmd);

        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        auto json = resp.getDataJson();
        EXPECT_STREQ(testCasStr, json["CAS"].get<std::string>().c_str());
    }

    /**
     * Make ::document an xattr value
     */
    void makeDocumentXattrValue() {
        cb::xattr::Blob blob;
        blob.set("user", R"({"author":"bubba"})");
        blob.set("meta", R"({"content-type":"text"})");

        auto xattrValue = blob.finalize();

        // append body to the xattrs and store in data
        std::string body = "document_body";
        document.value = xattrValue;
        document.value += body;
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

INSTANTIATE_TEST_SUITE_P(
        TransportProtocols,
        WithMetaTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpSsl),
                           ::testing::Values(XattrSupport::Yes,
                                             XattrSupport::No),
                           ::testing::Values(ClientJSONSupport::Yes,
                                             ClientJSONSupport::No),
                           ::testing::Values(ClientSnappySupport::Yes,
                                             ClientSnappySupport::No)),
        PrintToStringCombinedName());

TEST_P(WithMetaTest, basicSet) {
    MutationInfo resp;
    try {
        resp = userConnection->mutateWithMeta(document,
                                              Vbid(0),
                                              cb::mcbp::cas::Wildcard,
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

// Verify that SetWithMeta respects the fact a document is locked.
TEST_P(WithMetaTest, SetWhileLocked) {
    // Store an initial document so we can lock it.
    userConnection->mutateWithMeta(document,
                                   Vbid(0),
                                   cb::mcbp::cas::Wildcard,
                                   /*seqno*/ 1,
                                   /*options*/ 0,
                                   {});
    const auto locked = userConnection->get_and_lock(name, Vbid(0), 0);
    auto doSetWithMeta = [&](uint64_t operationCas) {
        userConnection->mutateWithMeta(document,
                                       Vbid(0),
                                       operationCas,
                                       /*seqno*/ 2,
                                       /*options*/ 0,
                                       {});
    };
    // While locked, SetWithMeta should fail when lockedCAS not specified.
    try {
        doSetWithMeta(cb::mcbp::cas::Wildcard);
        FAIL() << "setWithmeta against a locked document should not be "
                  "possible";
    } catch (const ConnectionError& ex) {
        EXPECT_TRUE(ex.isLocked());
    }
    // Retry with locked CAS, should succeed.
    doSetWithMeta(locked.info.cas);

    // Should no longer be locked
    auto getInfo = userConnection->get(name, Vbid(0));
    EXPECT_NE(getInfo.info.cas, cb::mcbp::cas::Locked);

    userConnection->remove(name, Vbid(0));
}

TEST_P(WithMetaTest, basicSetXattr) {
    makeDocumentXattrValue();

    MutationInfo resp;
    try {
        resp = userConnection->mutateWithMeta(document,
                                              Vbid(0),
                                              cb::mcbp::cas::Wildcard,
                                              /*seqno*/ 1,
                                              /*options*/ 0,
                                              {});
        EXPECT_EQ(XattrSupport::Yes, ::testing::get<1>(GetParam()));
        EXPECT_EQ(testCas, resp.cas);
    } catch (std::exception&) {
        EXPECT_EQ(XattrSupport::No, ::testing::get<1>(GetParam()));
    }

    if (::testing::get<1>(GetParam()) == XattrSupport::Yes) {
        checkCas();
    }
}

TEST_P(WithMetaTest, MB36304_DocumetTooBig) {
    if (::testing::get<3>(GetParam()) == ClientSnappySupport::No) {
        return;
    }

    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.value.clear();
    document.value.resize(21_MiB);
    document.compress();
    try {
        userConnection->mutateWithMeta(
                document, Vbid(0), cb::mcbp::cas::Wildcard, 1, 0, {});
        FAIL() << "It should not be possible to store documents which exceeds "
                  "the max document size";
    } catch (const ConnectionError& error) {
        EXPECT_EQ(cb::mcbp::Status::E2big, error.getReason());
    }
}

TEST_P(WithMetaTest, MB36304_DocumentMaxSizeWithXattr) {
    if (::testing::get<1>(GetParam()) == XattrSupport::No) {
        return;
    }

    document.value.clear();
    cb::xattr::Blob blob;
    blob.set("_sys", R"({"author":"bubba"})");
    auto xattrValue = blob.finalize();
    std::ranges::copy(xattrValue, std::back_inserter(document.value));

    document.value.resize((20_MiB) + xattrValue.size());
    document.info.datatype = cb::mcbp::Datatype::Xattr;
    userConnection->mutateWithMeta(
            document, Vbid(0), cb::mcbp::cas::Wildcard, 1, 0, {});
    userConnection->remove(name, Vbid(0));
}

TEST_P(WithMetaTest, MB36321_DeleteWithMetaRefuseUserXattrs) {
    if (::testing::get<1>(GetParam()) == XattrSupport::No ||
        ::testing::get<2>(GetParam()) == ClientJSONSupport::No) {
        return;
    }

    adminConnection->executeInBucket(bucketName, [](auto& connection) {
        const auto setParam = BinprotSetParamCommand(
                cb::mcbp::request::SetParamPayload::Type::Flush,
                "allow_sanitize_value_in_deletion",
                "false");
        const auto resp = BinprotMutationResponse(connection.execute(setParam));
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    });

    cb::xattr::Blob blob;
    blob.set("user", R"({"band":"Steel Panther"})");
    auto xattrValue = blob.finalize();

    document.value.clear();
    std::ranges::copy(xattrValue, std::back_inserter(document.value));
    document.value.append(R"({"Bug":"MB-36321"})");
    using cb::mcbp::Datatype;
    document.info.datatype =
            Datatype(uint8_t(Datatype::Xattr) | uint8_t(Datatype::JSON));

    BinprotDelWithMetaCommand cmd(document, Vbid(0), 0, 0, 1, 0);
    const auto rsp = BinprotMutationResponse(userConnection->execute(cmd));
    ASSERT_FALSE(rsp.isSuccess()) << rsp.getStatus();
    auto error = rsp.getDataJson();

    ASSERT_EQ(
            "It is only possible to specify Xattrs as a value to "
            "DeleteWithMeta",
            error["error"]["context"]);
}

TEST_P(WithMetaTest, MB36321_DeleteWithMetaAllowSystemXattrs) {
    if (::testing::get<1>(GetParam()) == XattrSupport::No ||
        ::testing::get<2>(GetParam()) == ClientJSONSupport::No) {
        return;
    }

    cb::xattr::Blob blob;
    blob.set("_sys", R"({"author":"bubba"})");
    auto xattrValue = blob.finalize();

    document.value.clear();
    std::ranges::copy(xattrValue, std::back_inserter(document.value));
    document.info.datatype = cb::mcbp::Datatype::Xattr;

    BinprotDelWithMetaCommand cmd(document, Vbid(0), 0, 0, 1, 0);
    const auto rsp = BinprotMutationResponse(userConnection->execute(cmd));
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus();

    // The system xattr should be there
    auto sresp = subdoc(cb::mcbp::ClientOpcode::SubdocGet,
                        name,
                        "_sys.author",
                        {},
                        cb::mcbp::subdoc::PathFlag::XattrPath,
                        cb::mcbp::subdoc::DocFlag::AccessDeleted);
    EXPECT_TRUE(sresp.isSuccess()) << sresp.getStatus();
}

void WithMetaTest::testDeleteWithMetaAcceptsUserXattrs(bool allowValuePruning,
                                                       bool compressed) {
    if (::testing::get<1>(GetParam()) == XattrSupport::No ||
        ::testing::get<2>(GetParam()) == ClientJSONSupport::No) {
        return;
    }

    adminConnection->executeInBucket(bucketName, [&](auto& connection) {
        const auto setParam = BinprotSetParamCommand(
                cb::mcbp::request::SetParamPayload::Type::Flush,
                "allow_sanitize_value_in_deletion",
                allowValuePruning ? "true" : "false");
        const auto resp = BinprotMutationResponse(connection.execute(setParam));
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    });

    // Value with User/Sys Xattrs and no Body
    cb::xattr::Blob blob;
    blob.set("user", R"({"a":"b"})");
    blob.set("_sys", R"({"c":"d"})");
    const auto xattrs = blob.finalize();
    document.value.clear();
    std::ranges::copy(xattrs, std::back_inserter(document.value));
    using Datatype = cb::mcbp::Datatype;
    document.info.datatype = Datatype::Xattr;

    if (compressed) {
        document.compress();
    }

    BinprotDelWithMetaCommand delWithMeta(
            document, Vbid(0), 0, 0 /*delTime*/, 1 /*seqno*/, 0 /*opCas*/);
    const auto resp =
            BinprotMutationResponse(userConnection->execute(delWithMeta));
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
    if (::testing::get<1>(GetParam()) == XattrSupport::No ||
        ::testing::get<2>(GetParam()) == ClientJSONSupport::No) {
        return;
    }

    adminConnection->executeInBucket(bucketName, [&](auto& connection) {
        const auto setParam = BinprotSetParamCommand(
                cb::mcbp::request::SetParamPayload::Type::Flush,
                "allow_sanitize_value_in_deletion",
                allowValuePruning ? "true" : "false");
        const auto resp = BinprotMutationResponse(connection.execute(setParam));
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    });

    // Value with User/Sys Xattrs and Body
    if (dtXattr) {
        cb::xattr::Blob blob;
        blob.set("user", R"({"a":"b"})");
        blob.set("_sys", R"({"c":"d"})");
        const auto xattrs = blob.finalize();
        document.value.clear();
        std::ranges::copy(xattrs, std::back_inserter(document.value));
        document.value.append("body");
        document.info.datatype = cb::mcbp::Datatype::Xattr;
    } else {
        document.value = "body";
        document.info.datatype = cb::mcbp::Datatype::Raw;
    }

    BinprotDelWithMetaCommand delWithMeta(
            document, Vbid(0), 0, 0 /*delTime*/, 1 /*seqno*/, 0 /*opCas*/);
    const auto resp =
            BinprotMutationResponse(userConnection->execute(delWithMeta));
    if (allowValuePruning) {
        ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    } else {
        ASSERT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
        ASSERT_TRUE(
                resp.getDataView().find("only possible to specify Xattrs") !=
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

TEST_P(WithMetaTest, MutateWithMeta) {
    using namespace cb::mcbp;
    using namespace cb::mcbp::subdoc;

    if (::testing::get<1>(GetParam()) == XattrSupport::No ||
        ::testing::get<2>(GetParam()) == ClientJSONSupport::No) {
        GTEST_SKIP();
    }

    Document doc{};

    cb::xattr::Blob blob;
    blob.set("user",
             R"({"CAS":"1xffffffffffffffff", "seqno":"1xfffffffffffffffe"})");
    blob.set("_sys",
             R"({"CAS":"1xffffffffffffffff", "seqno":"1xfffffffffffffffe"})");
    const auto xattrs = blob.finalize();
    doc.value.clear();
    std::ranges::copy(xattrs, std::back_inserter(doc.value));
    doc.value.append(
            R"({"CAS":"1xffffffffffffffff", "seqno":"1xfffffffffffffffe"})");
    doc.info.datatype = static_cast<cb::mcbp::Datatype>(
            PROTOCOL_BINARY_DATATYPE_XATTR | PROTOCOL_BINARY_DATATYPE_JSON);
    doc.info.id = name;
    doc.info.cas = 1;
    doc.info.flags = 0xdeadbeef;

    // Find offsets for CAS and seqno replacement within the xattrs section only
    auto xattr_section =
            doc.value.substr(0, cb::xattr::get_body_offset(doc.value));
    auto cas_offsets = findAllOffsets(xattr_section, "1xffffffffffffffff");
    auto seqno_offsets = findAllOffsets(xattr_section, "1xfffffffffffffffe");

    auto info = userConnection->mutateWithMeta(
            doc,
            Vbid{0},
            cb::mcbp::request::MutateWithMetaCommand::Add,
            cb::mcbp::cas::Wildcard,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            1,
            cas_offsets,
            seqno_offsets);

    auto validateDocument = [this, &info]() {
        BinprotSubdocMultiLookupCommand cmd{
                name,
                {
                        {ClientOpcode::SubdocGet,
                         PathFlag::XattrPath,
                         "$document"},
                        {ClientOpcode::SubdocGet, PathFlag::XattrPath, "user"},
                        {ClientOpcode::SubdocGet, PathFlag::XattrPath, "_sys"},
                        {ClientOpcode::SubdocGet, PathFlag::None, "CAS"},
                        {ClientOpcode::SubdocGet, PathFlag::None, "seqno"},
                },
                DocFlag::None};

        const auto resp =
                BinprotSubdocMultiLookupResponse(userConnection->execute(cmd));
        ASSERT_EQ(Status::Success, resp.getStatus());

        // Ensure that the CAS is equal:
        EXPECT_EQ(info.cas, resp.getCas());
        auto results = resp.getResults();
        EXPECT_EQ(Status::Success, results[0].status);
        nlohmann::json meta = nlohmann::json::parse(results[0].value);
        EXPECT_EQ(Status::Success, results[1].status);
        nlohmann::json user = nlohmann::json::parse(results[1].value);
        // So this is a bit of a mess.. using macro-expansion will store the
        // cas in network byte order.. but returned from $document it is in
        // host byte order...
        EXPECT_EQ(user["CAS"].get<std::string>(),
                  cb::to_hex(htonll(
                          cb::from_hex(meta["CAS"].get<std::string>()))));
        EXPECT_EQ(user["CAS"].get<std::string>(), cb::to_hex(htonll(info.cas)));
        EXPECT_EQ(user["seqno"].get<std::string>(),
                  meta["seqno"].get<std::string>());
        EXPECT_EQ(user["seqno"].get<std::string>(), cb::to_hex(info.seqno));
        EXPECT_EQ(Status::Success, results[2].status);
        nlohmann::json sys = nlohmann::json::parse(results[2].value);
        EXPECT_EQ(sys["CAS"].get<std::string>(),
                  cb::to_hex(htonll(
                          cb::from_hex(meta["CAS"].get<std::string>()))));
        EXPECT_EQ(sys["CAS"].get<std::string>(), cb::to_hex(htonll(info.cas)));
        EXPECT_EQ(sys["seqno"].get<std::string>(),
                  meta["seqno"].get<std::string>());
        // The fields in the body should not have been touched.
        EXPECT_EQ(Status::Success, results[3].status);
        EXPECT_EQ(R"("1xffffffffffffffff")", results[3].value);
        EXPECT_EQ(Status::Success, results[4].status);
        EXPECT_EQ(R"("1xfffffffffffffffe")", results[4].value);
    };

    validateDocument();

    try {
        userConnection->mutateWithMeta(
                doc,
                Vbid{0},
                cb::mcbp::request::MutateWithMetaCommand::Add,
                cb::mcbp::cas::Wildcard,
                REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
                1,
                cas_offsets,
                seqno_offsets);
        FAIL() << "Expected a key exists error";
    } catch (const ConnectionError& error) {
        EXPECT_EQ(cb::mcbp::Status::KeyEexists, error.getReason());
    }

    // but we should be able to replace it (using cas)
    info = userConnection->mutateWithMeta(
            doc,
            Vbid{0},
            cb::mcbp::request::MutateWithMetaCommand::Set,
            info.cas,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            1,
            cas_offsets,
            seqno_offsets);

    validateDocument();

    // But fail if the cas is incorrect
    try {
        userConnection->mutateWithMeta(
                doc,
                Vbid{0},
                cb::mcbp::request::MutateWithMetaCommand::Set,
                info.cas + 1,
                REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
                1,
                cas_offsets,
                seqno_offsets);
        FAIL() << "Expected a key exists error";
    } catch (const ConnectionError& error) {
        EXPECT_EQ(cb::mcbp::Status::KeyEexists, error.getReason());
    }

    // But succeed with unconditional set
    info = userConnection->mutateWithMeta(
            doc,
            Vbid{0},
            cb::mcbp::request::MutateWithMetaCommand::Set,
            cb::mcbp::cas::Wildcard,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            1,
            cas_offsets,
            seqno_offsets);

    validateDocument();

    blob.prune_user_keys();
    const auto system_xattrs = blob.finalize();
    doc.value.clear();
    std::ranges::copy(system_xattrs, std::back_inserter(doc.value));

    // Recalculate offsets for the reduced document
    xattr_section = doc.value.substr(0, cb::xattr::get_body_offset(doc.value));
    cas_offsets = findAllOffsets(xattr_section, "1xffffffffffffffff");
    seqno_offsets = findAllOffsets(xattr_section, "1xfffffffffffffffe");

    // and we can delete it
    info = userConnection->mutateWithMeta(
            doc,
            Vbid{0},
            cb::mcbp::request::MutateWithMetaCommand::Delete,
            cb::mcbp::cas::Wildcard,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            1,
            cas_offsets,
            seqno_offsets);

    BinprotSubdocMultiLookupCommand cmd{
            name,
            {{ClientOpcode::SubdocGet, PathFlag::XattrPath, "$document"},
             {ClientOpcode::SubdocGet, PathFlag::XattrPath, "_sys"}},
            DocFlag::AccessDeleted};

    auto resp = BinprotSubdocMultiLookupResponse(userConnection->execute(cmd));
    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());

    // Ensure that the CAS is equal:
    EXPECT_EQ(info.cas, resp.getCas());
    auto results = resp.getResults();
    EXPECT_EQ(cb::mcbp::Status::Success, results[0].status);
    nlohmann::json meta = nlohmann::json::parse(results[0].value);
    EXPECT_EQ(cb::mcbp::Status::Success, results[1].status);
    nlohmann::json sys = nlohmann::json::parse(results[1].value);
    EXPECT_EQ(sys["CAS"].get<std::string>(),
              cb::to_hex(htonll(cb::from_hex(meta["CAS"].get<std::string>()))));
    EXPECT_EQ(sys["CAS"].get<std::string>(), cb::to_hex(htonll(info.cas)));
    EXPECT_EQ(sys["seqno"].get<std::string>(),
              meta["seqno"].get<std::string>());
}

TEST_P(WithMetaTest, MutateWithMetaInvalidOffsets) {
    using namespace cb::mcbp;
    using namespace cb::mcbp::subdoc;

    if (::testing::get<1>(GetParam()) == XattrSupport::No ||
        ::testing::get<2>(GetParam()) == ClientJSONSupport::No) {
        GTEST_SKIP();
    }

    Document doc{};

    cb::xattr::Blob blob;
    blob.set("user",
             R"({"CAS":"1xffffffffffffffff", "seqno":"1xfffffffffffffffe"})");
    blob.set("_sys",
             R"({"CAS":"1xffffffffffffffff", "seqno":"1xfffffffffffffffe"})");
    const auto xattrs = blob.finalize();
    doc.value.clear();
    std::ranges::copy(xattrs, std::back_inserter(doc.value));
    doc.value.append(
            R"({"CAS":"1xffffffffffffffff", "seqno":"1xfffffffffffffffe"})");
    doc.info.datatype = static_cast<cb::mcbp::Datatype>(
            PROTOCOL_BINARY_DATATYPE_XATTR | PROTOCOL_BINARY_DATATYPE_JSON);
    doc.info.id = name;
    doc.info.cas = 1;
    doc.info.flags = 0xdeadbeef;
    std::vector<std::size_t> cas_offsets = {{2, 3, 4}};

    try {
        userConnection->mutateWithMeta(
                doc,
                Vbid{0},
                cb::mcbp::request::MutateWithMetaCommand::Add,
                cb::mcbp::cas::Wildcard,
                REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
                1,
                cas_offsets,
                {});
        FAIL() << "Expected mutateWithMeta to throw due to invalid offsets";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments())
                << "mutateWithMeta threw an unexpected exception: "
                << error.what();
        EXPECT_EQ("Patching seqno and cas would garble the xattr section",
                  error.getErrorContext());
    }
}

TEST_P(WithMetaTest, MutateWithMetaCasOffsetOutOfBounds) {
    using namespace cb::mcbp;

    if (::testing::get<1>(GetParam()) == XattrSupport::No ||
        ::testing::get<2>(GetParam()) == ClientJSONSupport::No) {
        GTEST_SKIP();
    }

    Document doc{};
    cb::xattr::Blob blob;
    blob.set("user", R"({"CAS":"1xffffffffffffffff"})");
    const auto xattrs = blob.finalize();
    doc.value.clear();
    std::ranges::copy(xattrs, std::back_inserter(doc.value));
    doc.info.datatype = Datatype::Xattr;
    doc.info.id = name;
    doc.info.cas = 1;
    doc.info.flags = 0xdeadbeef;

    // Use an offset that would be beyond the xattr section
    std::vector<std::size_t> cas_offsets = {1000};

    try {
        userConnection->mutateWithMeta(
                doc,
                Vbid{0},
                request::MutateWithMetaCommand::Add,
                cas::Wildcard,
                REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
                1,
                cas_offsets,
                {});
        FAIL() << "Expected mutateWithMeta to throw due to out of bounds "
                  "CAS offset";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments()) << error.what();
        EXPECT_EQ("CAS offset is out of bounds of the xattr section",
                  error.getErrorContext());
    }
}

TEST_P(WithMetaTest, MutateWithMetaSeqnoOffsetOutOfBounds) {
    using namespace cb::mcbp;

    if (::testing::get<1>(GetParam()) == XattrSupport::No ||
        ::testing::get<2>(GetParam()) == ClientJSONSupport::No) {
        GTEST_SKIP();
    }

    Document doc{};
    cb::xattr::Blob blob;
    blob.set("user", R"({"seqno":"1xfffffffffffffffe"})");
    const auto xattrs = blob.finalize();
    doc.value.clear();
    std::ranges::copy(xattrs, std::back_inserter(doc.value));
    doc.info.datatype = Datatype::Xattr;
    doc.info.id = name;
    doc.info.cas = 1;
    doc.info.flags = 0xdeadbeef;

    // Use an offset that would be beyond the xattr section
    std::vector<std::size_t> seqno_offsets = {1000};

    try {
        userConnection->mutateWithMeta(
                doc,
                Vbid{0},
                request::MutateWithMetaCommand::Add,
                cas::Wildcard,
                REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
                1,
                {},
                seqno_offsets);
        FAIL() << "Expected mutateWithMeta to throw due to out of bounds "
                  "seqno offset";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments()) << error.what();
        EXPECT_EQ("Seqno offset is out of bounds of the xattr section",
                  error.getErrorContext());
    }
}

TEST_P(WithMetaTest, MutateWithMetaPatternOnEmptyDocument) {
    using namespace cb::mcbp;

    if (::testing::get<1>(GetParam()) == XattrSupport::No ||
        ::testing::get<2>(GetParam()) == ClientJSONSupport::No) {
        GTEST_SKIP();
    }

    Document doc{};
    doc.value.clear();
    doc.info.datatype = Datatype::Raw;
    doc.info.id = name;
    doc.info.cas = 1;
    doc.info.flags = 0xdeadbeef;

    std::vector<std::size_t> cas_offsets = {0};

    try {
        userConnection->mutateWithMeta(
                doc,
                Vbid{0},
                request::MutateWithMetaCommand::Add,
                cas::Wildcard,
                REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
                1,
                cas_offsets,
                {});
        FAIL() << "Expected mutateWithMeta to throw due to empty document";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments()) << error.what();
        EXPECT_EQ("Cannot use pattern macros on empty documents",
                  error.getErrorContext());
    }
}

TEST_P(WithMetaTest, MutateWithMetaPatternOnDocumentWithoutXattr) {
    using namespace cb::mcbp;

    if (::testing::get<1>(GetParam()) == XattrSupport::No ||
        ::testing::get<2>(GetParam()) == ClientJSONSupport::No) {
        GTEST_SKIP();
    }

    Document doc{};
    doc.value = R"({"body":"value"})";
    doc.info.datatype = Datatype::JSON;
    doc.info.id = name;
    doc.info.cas = 1;
    doc.info.flags = 0xdeadbeef;

    std::vector<std::size_t> cas_offsets = {0};

    try {
        userConnection->mutateWithMeta(
                doc,
                Vbid{0},
                request::MutateWithMetaCommand::Add,
                cas::Wildcard,
                REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
                1,
                cas_offsets,
                {});
        FAIL() << "Expected mutateWithMeta to throw due to missing xattrs";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments()) << error.what();
        EXPECT_EQ("Cannot use pattern macros documents without xattr",
                  error.getErrorContext());
    }
}

TEST_P(WithMetaTest, MutateWithMetaDeleteWithBody) {
    using namespace cb::mcbp;

    if (::testing::get<1>(GetParam()) == XattrSupport::No ||
        ::testing::get<2>(GetParam()) == ClientJSONSupport::No) {
        GTEST_SKIP();
    }

    Document doc{};
    cb::xattr::Blob blob;
    blob.set("_sys", R"({"author":"bubba"})");
    const auto xattrs = blob.finalize();
    doc.value.clear();
    std::ranges::copy(xattrs, std::back_inserter(doc.value));
    doc.value.append("body content");
    doc.info.datatype = Datatype::Xattr;
    doc.info.id = name;
    doc.info.cas = 1;
    doc.info.flags = 0xdeadbeef;

    try {
        userConnection->mutateWithMeta(
                doc,
                Vbid{0},
                request::MutateWithMetaCommand::Delete,
                cas::Wildcard,
                REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
                1,
                {},
                {});
        FAIL() << "Expected mutateWithMeta to throw due to body in delete";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments()) << error.what();
        EXPECT_EQ("Delete with meta: Document cannot have a body",
                  error.getErrorContext());
    }
}

TEST_P(WithMetaTest, MutateWithMetaDeleteWithUserXattrs) {
    using namespace cb::mcbp;

    if (::testing::get<1>(GetParam()) == XattrSupport::No ||
        ::testing::get<2>(GetParam()) == ClientJSONSupport::No) {
        GTEST_SKIP();
    }

    Document doc{};
    cb::xattr::Blob blob;
    blob.set("user", R"({"author":"bubba"})");
    const auto xattrs = blob.finalize();
    doc.value.clear();
    std::ranges::copy(xattrs, std::back_inserter(doc.value));
    doc.info.datatype = Datatype::Xattr;
    doc.info.id = name;
    doc.info.cas = 1;
    doc.info.flags = 0xdeadbeef;

    try {
        userConnection->mutateWithMeta(
                doc,
                Vbid{0},
                request::MutateWithMetaCommand::Delete,
                cas::Wildcard,
                REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
                1,
                {},
                {});
        FAIL() << "Expected mutateWithMeta to throw due to user xattrs in "
                  "delete";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments()) << error.what();
        EXPECT_EQ("Delete with meta: user-xattrs is not allowed",
                  error.getErrorContext());
    }
}

TEST_P(WithMetaTest, MutateWithMetaDeleteWithBodyNoXattr) {
    using namespace cb::mcbp;

    if (::testing::get<1>(GetParam()) == XattrSupport::No ||
        ::testing::get<2>(GetParam()) == ClientJSONSupport::No) {
        GTEST_SKIP();
    }

    Document doc{};
    doc.value = "body content";
    doc.info.datatype = Datatype::Raw;
    doc.info.id = name;
    doc.info.cas = 1;
    doc.info.flags = 0xdeadbeef;

    try {
        userConnection->mutateWithMeta(
                doc,
                Vbid{0},
                request::MutateWithMetaCommand::Delete,
                cas::Wildcard,
                REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
                1,
                {},
                {});
        FAIL() << "Expected mutateWithMeta to throw due to body in delete";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments()) << error.what();
        EXPECT_EQ("Delete with meta: Document cannot have a body",
                  error.getErrorContext());
    }
}

class WithMetaRegenerateCasTest : public TestappXattrClientTest {
public:
    void SetUp() override {
        TestappXattrClientTest::SetUp();
        // set strategy to "replace" to ensure that the CAS is regenerated
        setHlcInvalidStrategyToReplace();
    }

    void setHlcInvalidStrategyToReplace() {
        adminConnection->executeInBucket(bucketName, [](auto& connection) {
            const auto setParam = BinprotSetParamCommand(
                    cb::mcbp::request::SetParamPayload::Type::Vbucket,
                    "hlc_invalid_strategy",
                    "replace");
            const auto resp =
                    BinprotMutationResponse(connection.execute(setParam));
            ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
        });
    }
};

TEST_P(WithMetaRegenerateCasTest, setWithMetaReturnsRegeratedCas) {
    auto poisonedCas = std::numeric_limits<int64_t>::max() & ~0xffffull;

    // store document with a poisoned CAS
    Document doc{};
    doc.value = "body";
    doc.info.id = "Test_Return_Cas";
    doc.info.datatype = cb::mcbp::Datatype::Raw;
    doc.info.cas = poisonedCas;

    const auto resp = userConnection->mutateWithMeta(doc,
                                                     Vbid(0),
                                                     cb::mcbp::cas::Wildcard,
                                                     /*seqno*/ 1,
                                                     /*options*/ 0,
                                                     {});

    // verify returned CAS is not the inital posioned CAS
    EXPECT_LT(resp.cas, poisonedCas);
}

TEST_P(WithMetaRegenerateCasTest, deleteWithMetaReturnsRegeratedCas) {
    auto poisonedCas = std::numeric_limits<int64_t>::max() & ~0xffffull;

    // store document with a poisoned CAS
    Document doc{};
    doc.value = "body";
    doc.info.id = "Test_Return_Cas";
    doc.info.datatype = cb::mcbp::Datatype::Raw;
    doc.info.cas = poisonedCas;

    BinprotDelWithMetaCommand delWithMeta(
            doc, Vbid(0), 0, 0 /*delTime*/, 1 /*seqno*/, 0 /*opCas*/);
    const auto delteOp =
            BinprotMutationResponse(userConnection->execute(delWithMeta));
    if (!delteOp.isSuccess()) {
        throw ConnectionError(fmt::format("Failed to delete {} {}",
                                          doc.info.id,
                                          delteOp.getDataView()),
                              delteOp.getStatus());
    }

    // verify returned CAS is not the inital posioned CAS
    EXPECT_LT(delteOp.getMutationInfo().cas, poisonedCas);
}

INSTANTIATE_TEST_SUITE_P(
        TransportProtocols,
        WithMetaRegenerateCasTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpSsl),
                           ::testing::Values(XattrSupport::No),
                           ::testing::Values(ClientJSONSupport::No),
                           ::testing::Values(ClientSnappySupport::No)),
        PrintToStringCombinedName());