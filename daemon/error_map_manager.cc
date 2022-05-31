/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "error_map_manager.h"
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <filesystem>

class ErrorMapManagerImpl : public ErrorMapManager {
public:
    ErrorMapManagerImpl(std::vector<std::string> maps)
        : ErrorMapManager(std::move(maps)) {
    }
};

std::string_view ErrorMapManager::getErrorMap(size_t version) {
    return error_maps[std::min(version, error_maps.size() - 1)];
}

namespace cb::errormap {
/// The one and only instance of the error map
static std::unique_ptr<ErrorMapManager> instance;

/// Version 1 is equal to v2 except that it allows for new attributes
nlohmann::json v2to1(nlohmann::json map) {
    const std::array<std::string, 16> attributes = {{"item-deleted",
                                                     "item-locked",
                                                     "item-only",
                                                     "invalid-input",
                                                     "fetch-config",
                                                     "conn-state-invalidated",
                                                     "auth",
                                                     "special-handling",
                                                     "support",
                                                     "temp",
                                                     "internal",
                                                     "retry-now",
                                                     "retry-later",
                                                     "subdoc",
                                                     "dcp",
                                                     "success"}};
    map["version"] = 1;
    // The highest revision we had in the old format was 4
    map["revision"] = 4 + map["revision"].get<int>();
    for (auto& entry : map["errors"]) {
        if (entry.is_object()) {
            std::vector<std::string> at;
            for (auto& attr : entry["attrs"]) {
                auto val = attr.get<std::string>();
                // strip off unsupported attributes
                if (std::find(attributes.begin(), attributes.end(), val) !=
                    attributes.end()) {
                    at.emplace_back(std::move(val));
                }
            }
            entry["attrs"] = at;
        }
    }
    return map;
}

std::vector<std::string> getMap(const std::filesystem::path& directory) {
    // we need revision 0, 1 and 2
    std::vector<std::string> maps(3);

    auto file = directory / "error_map_v2.json";
    // throws an exception if the file doesn't exist, file IO problems
    // and JSON parse problems
    auto v2 = nlohmann::json::parse(cb::io::loadFile(file.generic_string()));
    auto v1 = cb::errormap::v2to1(v2);
    maps[2] = v2.dump();
    maps[1] = v1.dump();
    return maps;
}

} // namespace cb::errormap

void ErrorMapManager::initialize(const std::filesystem::path& directory) {
    cb::errormap::instance = std::make_unique<ErrorMapManagerImpl>(
            cb::errormap::getMap(directory));
}

void ErrorMapManager::shutdown() {
    cb::errormap::instance.reset();
}

ErrorMapManager& ErrorMapManager::instance() {
    return *cb::errormap::instance;
}
