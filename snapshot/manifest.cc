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
#include <statistics/collector.h>

namespace cb::snapshot {

void to_json(nlohmann::json& json, const FileInfo& info) {
    json = {{"id", info.id},
            {"path", info.path.string()},
            {"size", std::to_string(info.size)}};
    if (!info.sha512.empty()) {
        json["sha512"] = info.sha512;
    }
}

static bool isAllDigits(std::string_view view) {
    return std::ranges::all_of(view, ::isdigit);
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

    if (json.contains("sha512")) {
        if (!json["sha512"].is_string()) {
            throw std::invalid_argument("from_json: sha512 must be a string");
        }
        info.sha512 = json["sha512"].get<std::string>();
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

Manifest::Manifest(const nlohmann::json& json) {
    from_json(json, *this);
}

void Manifest::addDebugStats(const StatCollector& collector) const {
    addUuidStat(collector);
    for (const auto& file : files) {
        file.addDebugStats(fmt::format("vb_{}", vbid.get()), collector);
    }
    addDekStats(collector);
}

void Manifest::addUuidStat(const StatCollector& collector) const {
    collector.addStat(std::string_view{fmt::format("vb_{}:uuid", vbid.get())},
                      uuid);
}

void Manifest::addDekStats(const StatCollector& collector) const {
    for (const auto& dek : deks) {
        dek.addDebugStats(fmt::format("vb_{}:dek:", vbid.get()), collector);
    }
}

void FileInfo::addDebugStats(std::string_view label,
                             const StatCollector& collector) const {
    collector.addStat(std::string_view{fmt::format("{}:path", label)},
                      path.string());
    collector.addStat(std::string_view{fmt::format("{}:size", label)}, size);
    collector.addStat(std::string_view{fmt::format("{}:id", label)}, id);
    collector.addStat(std::string_view{fmt::format("{}:sha512", label)},
                      sha512);
    collector.addStat(std::string_view{fmt::format("{}:status", label)},
                      format_as(status));
}

std::string format_as(FileStatus status) {
    switch (status) {
    case FileStatus::Present:
        return "present";
    case FileStatus::Absent:
        return "absent";
    case FileStatus::Truncated:
        return "truncated";
    }
    throw std::invalid_argument(
            fmt::format("Invalid FileStatus:{}", int(status)));
}

} // namespace cb::snapshot
