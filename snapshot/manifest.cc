/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "manifest.h"
#include <nlohmann/json.hpp>

namespace cb::snapshot {

void to_json(nlohmann::json& json, const FileInfo& info) {
    json = {{"id", info.id},
            {"path", info.path.string()},
            {"size", std::to_string(info.size)}};
    if (info.crc32c.has_value()) {
        json["crc32c"] = std::to_string(*info.crc32c);
    }
}

static bool isAllDigits(std::string_view view) {
    return std::all_of(view.begin(), view.end(), ::isdigit);
}

void from_json(const nlohmann::json& json, FileInfo& info) {
    if (!json.contains("path") || !json["path"].is_string()) {
        throw std::invalid_argument(
                "from_json: path must be present as string");
    }

    if (!json.contains("size") || !json["size"].is_string() ||
        !isAllDigits(json["size"].get<std::string>())) {
        throw std::invalid_argument(
                "from_json: size must be present as string");
    }

    if (!json.contains("id") || !json["id"].is_number()) {
        throw std::invalid_argument("from_json: id must be present as number");
    }

    info = FileInfo(json["path"].get<std::string>(),
                    std::stoul(json["size"].get<std::string>()),
                    json["id"].get<std::size_t>());

    if (json.contains("crc32c")) {
        if (!json["crc32c"].is_string() ||
            !isAllDigits(json["crc32c"].get<std::string>())) {
            throw std::invalid_argument(
                    "from_json: crc32c must be a string of digits");
        }
        info.crc32c = std::stoul(json["crc32c"].get<std::string>());
    }
}

void to_json(nlohmann::json& json, const Manifest& manifest) {
    json = {{"uuid", manifest.uuid},
            {"vbid", manifest.vbid.get()},
            {"files", manifest.files},
            {"deks", manifest.deks}};
}

void from_json(const nlohmann::json& json, Manifest& manifest) {
    if (!json.contains("uuid") || !json["uuid"].is_string()) {
        throw std::invalid_argument(
                "from_json: uuid must be present as string");
    }

    if (!json.contains("vbid") || !json["vbid"].is_number()) {
        throw std::invalid_argument(
                "from_json: vbid must be present as a number");
    }

    manifest.uuid = json["uuid"].get<std::string>();
    manifest.vbid = Vbid(json["vbid"].get<uint16_t>());
    if (json.contains("files")) {
        manifest.files = json["files"].get<std::vector<FileInfo>>();
    }
    if (json.contains("deks")) {
        manifest.deks = json["deks"].get<std::vector<FileInfo>>();
    }
}

} // namespace cb::snapshot
