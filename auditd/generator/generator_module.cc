/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "generator_module.h"
#include "auditevent_generator.h"
#include "generator_event.h"
#include "generator_utilities.h"
#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <utilities/json_utilities.h>
#include <fstream>
#include <iostream>
#include <memory>

void Module::addEvent(Event event) {
    if (event.id >= start && event.id < (start + max_events_per_module)) {
        events.emplace_back(std::move(event));
    } else {
        throw std::runtime_error(
                fmt::format("Error in {}: Event identifier {} is outside the "
                            "legal range for module {}s legal range: [{}, {}>",
                            path,
                            event.id,
                            name,
                            start,
                            start + max_events_per_module));
    }
}

void Module::createMacros(std::ostream& out) {
    if (!generateMacros) {
        return;
    }
    for (const auto& ev : events) {
        std::string nm(name);
        nm.append("_AUDIT_");
        nm.append(ev.name);
        for (auto& c : nm) {
            if (std::isalnum(c)) {
                c = toupper(c);
            } else {
                c = '_';
            }
        }
        out << "#define " << nm << " " << ev.id << std::endl;
    }
}

void Module::loadEventDescriptorFile(const std::filesystem::path& source_root) {
    auto file = source_root / path;
    if (!is_production_build() && !exists(file)) {
        std::cerr << "Audit: Ignoring missing file: " << file << std::endl;
        // Ignore the missing file if this isn't a production build
        // (one may try to build a subset of the modules)
        return;
    }

    json = load_file(file.generic_string());
    auto v = cb::jsonGet<int32_t>(json, "version");
    if (v != SupportedVersion) {
        throw std::runtime_error(
                fmt::format("Invalid version in {}: {}. Must be set to {}",
                            file.generic_string(),
                            v,
                            SupportedVersion));
    }

    auto n = cb::jsonGet<std::string>(json, "module");
    if (n != name) {
        throw std::runtime_error(fmt::format(
                "Invalid name in {}: {} can't load a module named {}",
                file.generic_string(),
                name,
                n));
    }

    // Parse all the individual events
    auto e = json["events"];
    if (!e.is_array()) {
        throw std::runtime_error(
                fmt::format("Invalid entry in {}: \"events\" must be an array",
                            file.generic_string()));
    }

    for (const auto& event : json["events"]) {
        addEvent(event.get<Event>());
    }

    // Replace the JSON with one where we've updated all default variables
    json = {{"version", SupportedVersion},
            {"module", name},
            {"events", events}};
}

bool Module::includeInConfiguration() const {
    return std::find(configurations.begin(),
                     configurations.end(),
                     CB_BUILD_CONFIGURATION) != configurations.end();
}

void from_json(const nlohmann::json& json, Module& module) {
    if (!json.contains("name")) {
        throw std::runtime_error(fmt::format(
                "from_json(Module): 'name' must be set: {}", json.dump()));
    }
    if (!json.contains("startid")) {
        throw std::runtime_error(fmt::format(
                "from_json(Module): 'startid' must be set: {}", json.dump()));
    }
    if (!json.contains("file")) {
        throw std::runtime_error(fmt::format(
                "from_json(Module): 'file' must be set: {}", json.dump()));
    }
    if (!json.contains("configurations") ||
        !json["configurations"].is_array()) {
        throw std::runtime_error(
                fmt::format("from_json(Module): 'configurations' must be set "
                            "(and an array): {}",
                            json.dump()));
    }
    module.start = json.value("startid", -1);
    module.path = json["file"].get<std::string>();
    module.name = json["name"];
    module.configurations = json["configurations"];
    module.generateMacros = json.value("generate_macros", false);
}
