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

TEST(MutateWithMetaCommandTest, to_json_add) {
    nlohmann::json json = MutateWithMetaCommand::Add;
    EXPECT_EQ(json.get<std::string>(), "add");
}

TEST(MutateWithMetaCommandTest, to_json_set) {
    nlohmann::json json = MutateWithMetaCommand::Set;
    EXPECT_EQ(json.get<std::string>(), "set");
}

TEST(MutateWithMetaCommandTest, to_json_delete) {
    nlohmann::json json = MutateWithMetaCommand::Delete;
    EXPECT_EQ(json.get<std::string>(), "delete");
}

TEST(MutateWithMetaCommandTest, to_json_invalid) {
    try {
        nlohmann::json json = static_cast<MutateWithMetaCommand>(0xff);
        FAIL() << "Expected to_json to throw for invalid command";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ(e.what(), "Unknown MutateWithMetaCommand");
    }
}

TEST(MutateWithMetaCommandTest, from_json_add) {
    nlohmann::json json = "add";
    EXPECT_EQ(MutateWithMetaCommand::Add, json);
}

TEST(MutateWithMetaCommandTest, from_json_set) {
    nlohmann::json json = "set";
    EXPECT_EQ(MutateWithMetaCommand::Set, json);
}

TEST(MutateWithMetaCommandTest, from_json_delete) {
    nlohmann::json json = "delete";
    EXPECT_EQ(MutateWithMetaCommand::Delete, json);
}

TEST(MutateWithMetaCommandTest, from_json_invalid) {
    try {
        nlohmann::json json = "unknown";
        (void)json.get<MutateWithMetaCommand>();
        FAIL() << "Expected from_json to throw for invalid command";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ(e.what(), "Unknown MutateWithMetaCommand: unknown");
    }
}

TEST(MutateWithMetaPayloadTest, to_json_basic) {
    MutateWithMetaPayload payload;
    payload.command = MutateWithMetaCommand::Set;
    payload.cas = 0x1122334455667788ull;
    payload.rev_seqno = 0x12345678;
    payload.flags = 0xdeadbeef;
    payload.exptime = 3600;
    payload.options = WithMetaOptions{0x04};

    nlohmann::json json;
    to_json(json, payload);

    EXPECT_EQ(json["command"].get<std::string>(), "set");
    EXPECT_EQ(json["cas"].get<std::string>(), "0x1122334455667788");
    EXPECT_EQ(json["rev_seqno"].get<std::string>(), "0x0000000012345678");
    EXPECT_EQ(json["flags"].get<std::string>(), "0xdeadbeef");
    EXPECT_EQ(json["expiration"].get<std::string>(), "0x00000e10");
    EXPECT_EQ(json["options"].get<std::string>(), "0x00000004");
    EXPECT_FALSE(json.contains("cas_offsets"));
    EXPECT_FALSE(json.contains("seqno_offsets"));
}

TEST(MutateWithMetaPayloadTest, to_json_with_offsets) {
    MutateWithMetaPayload payload;
    payload.command = MutateWithMetaCommand::Set;
    payload.cas = 0x1;
    payload.cas_offsets = {10, 50, 100};
    payload.seqno_offsets = {30, 70, 120};

    nlohmann::json json;
    to_json(json, payload);

    EXPECT_EQ(json["cas_offsets"].get<std::vector<std::size_t>>(),
              (std::vector<std::size_t>{10, 50, 100}));
    EXPECT_EQ(json["seqno_offsets"].get<std::vector<std::size_t>>(),
              (std::vector<std::size_t>{30, 70, 120}));
}

TEST(MutateWithMetaPayloadTest, to_json_without_optional_fields) {
    MutateWithMetaPayload payload;
    payload.command = MutateWithMetaCommand::Set;
    payload.flags = 0x12345678;
    payload.exptime = 7200;
    payload.options = WithMetaOptions{0x0c};

    nlohmann::json json;
    to_json(json, payload);

    EXPECT_FALSE(json.contains("cas"));
    EXPECT_FALSE(json.contains("rev_seqno"));
    EXPECT_FALSE(json.contains("cas_offsets"));
    EXPECT_FALSE(json.contains("seqno_offsets"));
}

TEST(MutateWithMetaPayloadTest, from_json_basic) {
    nlohmann::json json = {{"command", "set"},
                           {"cas", "0x1122334455667788"},
                           {"rev_seqno", "0x12345678"},
                           {"flags", "0xdeadbeef"},
                           {"expiration", "3600"},
                           {"options", "0x0c"}};

    MutateWithMetaPayload payload;
    from_json(json, payload);

    EXPECT_EQ(payload.command, MutateWithMetaCommand::Set);
    EXPECT_EQ(payload.cas.value(), 0x1122334455667788ull);
    EXPECT_EQ(payload.rev_seqno.value(), 0x12345678);
    EXPECT_EQ(payload.flags, 0xdeadbeef);
    EXPECT_EQ(payload.exptime, 3600);
    EXPECT_EQ(payload.options.encode(), 0x0c);
    EXPECT_TRUE(payload.cas_offsets.empty());
    EXPECT_TRUE(payload.seqno_offsets.empty());
}

TEST(MutateWithMetaPayloadTest, from_json_with_offsets) {
    nlohmann::json json = {{"command", "delete"},
                           {"cas", "0x1"},
                           {"flags", "0x2"},
                           {"expiration", "0x3"},
                           {"options", "0x4"},
                           {"cas_offsets", {10, 50, 100}},
                           {"seqno_offsets", {30, 70, 120}}};

    MutateWithMetaPayload payload;
    from_json(json, payload);

    EXPECT_EQ(payload.command, MutateWithMetaCommand::Delete);
    EXPECT_EQ(payload.cas.value(), 1);
    EXPECT_EQ(payload.flags, 2);
    EXPECT_EQ(payload.exptime, 3);
    EXPECT_EQ(payload.options.encode(), 4);
    EXPECT_EQ(payload.cas_offsets, (std::vector<std::size_t>{10, 50, 100}));
    EXPECT_EQ(payload.seqno_offsets, (std::vector<std::size_t>{30, 70, 120}));
}

TEST(MutateWithMetaPayloadTest, from_json_without_optional_fields) {
    nlohmann::json json = {{"command", "add"},
                           {"flags", "0x1234"},
                           {"expiration", "0x10"},
                           {"options", "0x04"}};

    MutateWithMetaPayload payload;
    from_json(json, payload);

    EXPECT_EQ(payload.command, MutateWithMetaCommand::Add);
    EXPECT_FALSE(payload.cas.has_value());
    EXPECT_FALSE(payload.rev_seqno.has_value());
    EXPECT_EQ(payload.flags, 0x1234);
    EXPECT_EQ(payload.exptime, 0x10);
    EXPECT_EQ(payload.options.encode(), 0x04);
    EXPECT_TRUE(payload.cas_offsets.empty());
    EXPECT_TRUE(payload.seqno_offsets.empty());
}

TEST(MutateWithMetaPayloadTest, from_json_missing_required_field) {
    nlohmann::json json = {
            {"command", "set"}, {"flags", "0x1234"}, {"options", "0x04"}};

    try {
        MutateWithMetaPayload payload = json;
    } catch (const std::exception& e) {
        EXPECT_STREQ(
                e.what(),
                "cb::getValueFromJson(): Missing required field: expiration");
    }
}

TEST(MutateWithMetaPayloadTest, round_trip_minimal) {
    MutateWithMetaPayload original;
    original.command = MutateWithMetaCommand::Set;
    original.flags = 0xdeadbeef;
    original.exptime = 3600;
    original.options = WithMetaOptions{0x04};

    nlohmann::json json;
    to_json(json, original);

    MutateWithMetaPayload decoded;
    from_json(json, decoded);

    EXPECT_EQ(decoded.command, original.command);
    EXPECT_EQ(decoded.cas, original.cas);
    EXPECT_EQ(decoded.rev_seqno, original.rev_seqno);
    EXPECT_EQ(decoded.flags, original.flags);
    EXPECT_EQ(decoded.exptime, original.exptime);
    EXPECT_EQ(decoded.options.encode(), original.options.encode());
    EXPECT_EQ(decoded.cas_offsets, original.cas_offsets);
    EXPECT_EQ(decoded.seqno_offsets, original.seqno_offsets);
}

TEST(MutateWithMetaPayloadTest, round_trip_complete) {
    MutateWithMetaPayload original;
    original.command = MutateWithMetaCommand::Delete;
    original.cas = 0x8877665544332211ull;
    original.rev_seqno = 0x99887766;
    original.flags = 0xaabbccdd;
    original.exptime = 7200;
    original.options = WithMetaOptions{0x1c};
    original.cas_offsets = {10, 50, 100};
    original.seqno_offsets = {30, 70, 120};

    nlohmann::json json;
    to_json(json, original);

    MutateWithMetaPayload decoded;
    from_json(json, decoded);

    EXPECT_EQ(decoded.command, original.command);
    EXPECT_EQ(decoded.cas, original.cas);
    EXPECT_EQ(decoded.rev_seqno, original.rev_seqno);
    EXPECT_EQ(decoded.flags, original.flags);
    EXPECT_EQ(decoded.exptime, original.exptime);
    EXPECT_EQ(decoded.options.encode(), original.options.encode());
    EXPECT_EQ(decoded.cas_offsets, original.cas_offsets);
    EXPECT_EQ(decoded.seqno_offsets, original.seqno_offsets);
}

TEST(MutateWithMetaPayloadTest, from_json_decimal_numbers) {
    nlohmann::json json = {{"command", "set"},
                           {"cas", 123456789},
                           {"rev_seqno", 987654321},
                           {"flags", 0x12345678},
                           {"expiration", 3600},
                           {"options", 0x04}};

    MutateWithMetaPayload payload;
    from_json(json, payload);

    EXPECT_EQ(payload.command, MutateWithMetaCommand::Set);
    EXPECT_EQ(payload.cas.value(), 123456789ull);
    EXPECT_EQ(payload.rev_seqno.value(), 987654321ull);
}
