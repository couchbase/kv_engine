/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <mcbp/codec/mutate_with_meta_payload.h>
#include <nlohmann/json.hpp>
#include <platform/string_hex.h>
#include <utilities/json_utilities.h>

namespace cb::mcbp::request {
void to_json(nlohmann::json& json, const MutateWithMetaCommand& cmd) {
    switch (cmd) {
    case MutateWithMetaCommand::Add:
        json = "add";
        return;
    case MutateWithMetaCommand::Set:
        json = "set";
        return;
    case MutateWithMetaCommand::Delete:
        json = "delete";
        return;
    }
    throw std::invalid_argument("Unknown MutateWithMetaCommand");
}

void from_json(const nlohmann::json& json, MutateWithMetaCommand& cmd) {
    auto str = json.get<std::string>();
    if (str == "add") {
        cmd = MutateWithMetaCommand::Add;
        return;
    }
    if (str == "set") {
        cmd = MutateWithMetaCommand::Set;
        return;
    }
    if (str == "delete") {
        cmd = MutateWithMetaCommand::Delete;
        return;
    }
    throw std::invalid_argument("Unknown MutateWithMetaCommand: " + str);
}

void to_json(nlohmann::json& json, const MutateWithMetaPayload& payload) {
    json = nlohmann::json{
            {"command", payload.command},
            {"cas", cb::to_hex(payload.cas.value_or(0))},
            {"rev_seqno", cb::to_hex(payload.rev_seqno.value_or(0))},
            {"flags", cb::to_hex(payload.flags)},
            {"expiration", cb::to_hex(payload.exptime)},
            {"options", cb::to_hex(payload.options.encode())},
            {"cas_offsets", payload.cas_offsets},
            {"seqno_offsets", payload.seqno_offsets}};
    if (!payload.cas.has_value()) {
        json.erase("cas");
    }
    if (!payload.rev_seqno.has_value()) {
        json.erase("rev_seqno");
    }
    if (payload.cas_offsets.empty()) {
        json.erase("cas_offsets");
    }
    if (payload.seqno_offsets.empty()) {
        json.erase("seqno_offsets");
    }
}

void from_json(const nlohmann::json& json, MutateWithMetaPayload& payload) {
    payload.command = json.at("command").get<MutateWithMetaCommand>();
    if (json.contains("cas")) {
        payload.cas = cb::getValueFromJson<uint64_t>(json, "cas");
    }
    if (json.contains("rev_seqno")) {
        payload.rev_seqno = cb::getValueFromJson<uint64_t>(json, "rev_seqno");
    }

    payload.flags = cb::getValueFromJson<uint32_t>(json, "flags");
    payload.exptime = cb::getValueFromJson<uint32_t>(json, "expiration");
    payload.options =
            WithMetaOptions{cb::getValueFromJson<uint32_t>(json, "options")};

    if (json.contains("cas_offsets")) {
        payload.cas_offsets =
                json.at("cas_offsets").get<std::vector<std::size_t>>();
    }
    if (json.contains("seqno_offsets")) {
        payload.seqno_offsets =
                json.at("seqno_offsets").get<std::vector<std::size_t>>();
    }
}

} // namespace cb::mcbp::request
