/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "clustertest.h"

#include <cluster_framework/auth_provider_service.h>
#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <mcbp/codec/mutate_with_meta_payload.h>
#include <platform/string_hex.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <xattr/blob.h>

using namespace cb::mcbp;
using namespace cb::mcbp::subdoc;

class MutateWithMetaTest : public cb::test::ClusterTest {
protected:
    void SetUp() override;

    void validateDocument(MemcachedConnection& conn,
                          bool macro_expansion,
                          uint64_t expected_cas,
                          std::optional<uint64_t> rev_seqno = std::nullopt);

    Document doc;
};

void MutateWithMetaTest::SetUp() {
    ClusterTest::SetUp();
    cb::xattr::Blob blob;
    blob.set("user",
             R"({"CAS":"1xffffffffffffffff", "seqno":"1xfffffffffffffffe"})");
    blob.set("_sys",
             R"({"CAS":"1xffffffffffffffff", "seqno":"1xfffffffffffffffe"})");
    const auto xattrs = blob.finalize();
    std::ranges::copy(xattrs, std::back_inserter(doc.value));
    doc.value.append(
            R"({"CAS":"1xffffffffffffffff", "seqno":"1xfffffffffffffffe"})");
    doc.info.datatype = static_cast<cb::mcbp::Datatype>(
            PROTOCOL_BINARY_DATATYPE_XATTR | PROTOCOL_BINARY_DATATYPE_JSON);
    doc.info.id = "StoreItemWithExistingProperties";
    doc.info.cas = 0xcafefeed;
    doc.info.flags = 0xdeadbeef;
    doc.info.expiration = 0;

    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    std::string name = info->test_case_name();
    name.append("_");
    name.append(info->name());
    std::ranges::replace(name, '/', '_');
    doc.info.id = std::move(name);
}

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

void MutateWithMetaTest::validateDocument(MemcachedConnection& conn,
                                          bool macro_expansion,
                                          uint64_t expected_cas,
                                          std::optional<uint64_t> rev_seqno) {
    std::string expected_cas_str = cb::to_hex(htonll(expected_cas));
    std::string hostlocal_expected_cas_str = cb::to_hex(expected_cas);

    BinprotSubdocMultiLookupCommand cmd{
            doc.info.id,
            {
                    {ClientOpcode::SubdocGet, PathFlag::XattrPath, "$document"},
                    {ClientOpcode::SubdocGet, PathFlag::XattrPath, "user"},
                    {ClientOpcode::SubdocGet, PathFlag::XattrPath, "_sys"},
                    {ClientOpcode::SubdocGet, PathFlag::None, "CAS"},
                    {ClientOpcode::SubdocGet, PathFlag::None, "seqno"},
            },
            DocFlag::None};

    const auto resp = BinprotSubdocMultiLookupResponse(conn.execute(cmd));
    ASSERT_EQ(Status::Success, resp.getStatus());

    // Ensure that the CAS is equal:
    EXPECT_EQ(expected_cas, resp.getCas());
    auto results = resp.getResults();
    ASSERT_EQ(Status::Success, results[0].status);
    nlohmann::json meta = nlohmann::json::parse(results[0].value);
    EXPECT_EQ(hostlocal_expected_cas_str, meta["CAS"].get<std::string>());
    if (rev_seqno) {
        EXPECT_EQ(*rev_seqno, std::stoull(meta["revid"].get<std::string>()));
    }
    ASSERT_EQ(Status::Success, results[1].status);
    nlohmann::json user = nlohmann::json::parse(results[1].value);
    if (macro_expansion) {
        EXPECT_EQ(expected_cas_str, user["CAS"].get<std::string>());
        EXPECT_EQ(meta["seqno"], user["seqno"].get<std::string>());
    } else {
        EXPECT_EQ(user["CAS"].get<std::string>(), "1xffffffffffffffff");
        EXPECT_EQ(user["seqno"].get<std::string>(), "1xfffffffffffffffe");
    }
    ASSERT_EQ(Status::Success, results[2].status);
    nlohmann::json sys = nlohmann::json::parse(results[2].value);
    if (macro_expansion) {
        EXPECT_EQ(expected_cas_str, sys["CAS"].get<std::string>());
        EXPECT_EQ(meta["seqno"], sys["seqno"].get<std::string>());
    } else {
        EXPECT_EQ(sys["CAS"].get<std::string>(), "1xffffffffffffffff");
        EXPECT_EQ(sys["seqno"].get<std::string>(), "1xfffffffffffffffe");
    }
    ASSERT_EQ(Status::Success, results[3].status);
    EXPECT_EQ(R"("1xffffffffffffffff")", results[3].value);
    ASSERT_EQ(Status::Success, results[4].status);
    EXPECT_EQ(R"("1xfffffffffffffffe")", results[4].value);
}

TEST_F(MutateWithMetaTest, StoreItemWithExistingProperties) {
    const uint64_t rev_seqno = 100;

    auto bucket = cluster->getBucket("default");
    auto conn = bucket->getConnection(Vbid(0));
    conn->authenticate("@admin");
    conn->selectBucket("default");
    auto info =
            conn->mutateWithMeta(doc,
                                 Vbid{0},
                                 cb::mcbp::request::MutateWithMetaCommand::Add,
                                 cb::mcbp::cas::Wildcard,
                                 cb::mcbp::WithMetaOptions{}.encode(),
                                 rev_seqno);

    EXPECT_EQ(doc.info.cas, info.cas);
    validateDocument(*conn, false, doc.info.cas, rev_seqno);
}

TEST_F(MutateWithMetaTest, StoreItemWithMacroExpansion) {
    auto bucket = cluster->getBucket("default");
    auto conn = bucket->getConnection(Vbid(0));
    conn->authenticate("@admin");
    conn->selectBucket("default");

    // Find all offsets of the CAS and seqno placeholders in the xattr section
    auto xattr_section =
            doc.value.substr(0, cb::xattr::get_body_offset(doc.value));
    auto cas_offsets = findAllOffsets(xattr_section, "1xffffffffffffffff");
    auto seqno_offsets = findAllOffsets(xattr_section, "1xfffffffffffffffe");

    const uint64_t rev_seqno = 200;
    auto info =
            conn->mutateWithMeta(doc,
                                 Vbid{0},
                                 cb::mcbp::request::MutateWithMetaCommand::Set,
                                 cb::mcbp::cas::Wildcard,
                                 REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
                                 rev_seqno,
                                 cas_offsets,
                                 seqno_offsets);

    // The CAS should have been regenerated
    EXPECT_NE(doc.info.cas, info.cas);
    validateDocument(*conn, true, info.cas, rev_seqno);
}
