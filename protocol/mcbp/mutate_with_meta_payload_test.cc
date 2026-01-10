/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/GTest.h>
#include <mcbp/codec/mutate_with_meta_payload.h>
#include <nlohmann/json.hpp>

using namespace cb::mcbp::request;
using namespace cb::mcbp;

// ---- MutateWithMetaCommand to_json / from_json -----------------------------

TEST(MutateWithMetaCommandTest, ToJsonAdd) {
    nlohmann::json j = MutateWithMetaCommand::Add;
    EXPECT_EQ("add", j.get<std::string>());
}

TEST(MutateWithMetaCommandTest, ToJsonSet) {
    nlohmann::json j = MutateWithMetaCommand::Set;
    EXPECT_EQ("set", j.get<std::string>());
}

TEST(MutateWithMetaCommandTest, ToJsonDelete) {
    nlohmann::json j = MutateWithMetaCommand::Delete;
    EXPECT_EQ("delete", j.get<std::string>());
}

TEST(MutateWithMetaCommandTest, FromJsonAdd) {
    MutateWithMetaCommand cmd = nlohmann::json("add");
    EXPECT_EQ(MutateWithMetaCommand::Add, cmd);
}

TEST(MutateWithMetaCommandTest, FromJsonSet) {
    MutateWithMetaCommand cmd = nlohmann::json("set");
    EXPECT_EQ(MutateWithMetaCommand::Set, cmd);
}

TEST(MutateWithMetaCommandTest, FromJsonDelete) {
    MutateWithMetaCommand cmd = nlohmann::json("delete");
    EXPECT_EQ(MutateWithMetaCommand::Delete, cmd);
}

TEST(MutateWithMetaCommandTest, FromJsonInvalidThrows) {
    try {
        MutateWithMetaCommand cmd = nlohmann::json("unknown");
        FAIL() << "Expected from_json to throw for unknown command"
               << " but got " << nlohmann::json(cmd);
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ("Unknown MutateWithMetaCommand: unknown", e.what());
    }
}

// ---- MutateWithMetaPayload to_json / from_json -----------------------------

/// Build a minimal payload with no optional fields set.
static MutateWithMetaPayload makeMinimalPayload() {
    MutateWithMetaPayload p;
    p.command = MutateWithMetaCommand::Set;
    p.cas = std::nullopt;
    p.rev_seqno = 0x0102030405060708ULL;
    p.flags = 0xdeadbeef;
    p.exptime = 3600;
    p.options = WithMetaOptions{0};
    p.cas_offsets = {};
    p.seqno_offsets = {};
    return p;
}

TEST(MutateWithMetaPayloadTest, RoundTripMinimal) {
    const auto original = makeMinimalPayload();
    const nlohmann::json j = original;
    const auto decoded = j.get<MutateWithMetaPayload>();
    EXPECT_EQ(original, decoded);
}

TEST(MutateWithMetaPayloadTest, RoundTripWithCas) {
    auto original = makeMinimalPayload();
    original.cas = 0xCAFEBABEDEAD1234ULL;
    const nlohmann::json j = original;
    const auto decoded = j.get<MutateWithMetaPayload>();
    EXPECT_EQ(original, decoded);
}

TEST(MutateWithMetaPayloadTest, RoundTripWithOffsets) {
    auto original = makeMinimalPayload();
    original.cas_offsets = {4, 12, 20};
    original.seqno_offsets = {0, 8};
    const nlohmann::json j = original;
    const auto decoded = j.get<MutateWithMetaPayload>();
    EXPECT_EQ(original, decoded);
}

TEST(MutateWithMetaPayloadTest, RoundTripAllCommands) {
    for (auto cmd : {MutateWithMetaCommand::Add,
                     MutateWithMetaCommand::Set,
                     MutateWithMetaCommand::Delete}) {
        auto original = makeMinimalPayload();
        original.command = cmd;
        const nlohmann::json j = original;
        const auto decoded = j.get<MutateWithMetaPayload>();
        EXPECT_EQ(original, decoded);
    }
}

TEST(MutateWithMetaPayloadTest, RoundTripWithOptions) {
    auto original = makeMinimalPayload();
    // Enable all option flags
    original.options.check_conflicts = CheckConflicts::No;
    original.options.generate_cas = GenerateCas::Yes;
    original.options.force_accept = ForceAcceptWithMetaOperation::Yes;
    original.options.delete_source = DeleteSource::TTL;
    const nlohmann::json j = original;
    const auto decoded = j.get<MutateWithMetaPayload>();
    EXPECT_EQ(original, decoded);
}

// ---- JSON structure checks --------------------------------------------------

TEST(MutateWithMetaPayloadTest, JsonKeysPresent) {
    const auto p = makeMinimalPayload();
    const nlohmann::json j = p;
    EXPECT_TRUE(j.contains("command"));
    EXPECT_TRUE(j.contains("rev_seqno"));
    EXPECT_TRUE(j.contains("flags"));
    EXPECT_TRUE(j.contains("expiration"));
    EXPECT_TRUE(j.contains("options"));
}

TEST(MutateWithMetaPayloadTest, NoCasKeyWhenAbsent) {
    const auto p = makeMinimalPayload();
    const nlohmann::json j = p;
    EXPECT_FALSE(j.contains("cas"));
}

TEST(MutateWithMetaPayloadTest, CasKeyPresentWhenSet) {
    auto p = makeMinimalPayload();
    p.cas = 0x1;
    const nlohmann::json j = p;
    EXPECT_TRUE(j.contains("cas"));
}

TEST(MutateWithMetaPayloadTest, NoCasOffsetsKeyWhenEmpty) {
    const auto p = makeMinimalPayload();
    const nlohmann::json j = p;
    EXPECT_FALSE(j.contains("cas_offsets"));
}

TEST(MutateWithMetaPayloadTest, NoSeqnoOffsetsKeyWhenEmpty) {
    const auto p = makeMinimalPayload();
    const nlohmann::json j = p;
    EXPECT_FALSE(j.contains("seqno_offsets"));
}

TEST(MutateWithMetaPayloadTest, OffsetsKeysPresentWhenNonEmpty) {
    auto p = makeMinimalPayload();
    p.cas_offsets = {0};
    p.seqno_offsets = {1};
    const nlohmann::json j = p;
    EXPECT_TRUE(j.contains("cas_offsets"));
    EXPECT_TRUE(j.contains("seqno_offsets"));
}

TEST(MutateWithMetaPayloadTest, NumericFieldsAreHexStrings) {
    auto p = makeMinimalPayload();
    p.cas = 0xABCDULL;
    const nlohmann::json j = p;
    // All numeric fields are serialized as hex strings starting with "0x"
    EXPECT_TRUE(j.at("cas").get<std::string>().starts_with("0x"));
    EXPECT_TRUE(j.at("rev_seqno").get<std::string>().starts_with("0x"));
    EXPECT_TRUE(j.at("flags").get<std::string>().starts_with("0x"));
    EXPECT_TRUE(j.at("expiration").get<std::string>().starts_with("0x"));
    EXPECT_TRUE(j.at("options").get<std::string>().starts_with("0x"));
}

TEST(MutateWithMetaPayloadTest, CommandFieldIsString) {
    const auto p = makeMinimalPayload();
    const nlohmann::json j = p;
    EXPECT_TRUE(j.at("command").is_string());
    EXPECT_EQ("set", j.at("command").get<std::string>());
}
