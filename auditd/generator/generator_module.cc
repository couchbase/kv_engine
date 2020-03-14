/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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
#include "generator_module.h"
#include "auditevent_generator.h"
#include "generator_event.h"
#include "generator_utilities.h"

#include <nlohmann/json.hpp>
#include <utilities/json_utilities.h>

#include <fstream>
#include <gsl/gsl>
#include <memory>
#include <sstream>
#include <system_error>

Module::Module(const nlohmann::json& object,
               const std::string& srcRoot,
               const std::string& objRoot) {
    /*
     * https://github.com/nlohmann/json/issues/67
     * There is no way for a JSON value to "know" whether it is stored in an
     * object, and if so under which key Use an iterator to get the first (and
     * only) object and extract the key and data from it
     */
    auto it = object.begin();
    name = it.key();
    auto data = it.value();

    // Expect a single item in the dictionary, if this is not the case, throw an
    // exception
    if (object.size() > 1) {
        std::stringstream ss;
        ss << "Module::Module expected single item dictionary, got: " << object;
        throw std::runtime_error(ss.str());
    }

    // Each module contains:
    //   startid - mandatory
    //   file - mandatory
    //   header - optional
    size_t expected = 2;
    start = data.at("startid");

    file.assign(srcRoot);
    file.append("/");
    file.append(cb::jsonGet<std::string>(data, "file"));
    file = cb::io::sanitizePath(file);

    auto hfile = data.value("header", "");
    if (!hfile.empty()) {
        header.assign(objRoot);
        header.append("/");
        header.append(hfile);
        header = cb::io::sanitizePath(header);
        ++expected;
    }

    auto ent = data.value("enterprise", -1);
    if (ent != -1) {
        enterprise = gsl::narrow_cast<bool>(ent);
        ++expected;
    }

    if (data.size() != expected) {
        std::stringstream ss;
        ss << "Unknown elements for " << name << ": " << std::endl
           << data << std::endl;
        throw std::runtime_error(ss.str());
    }

    parseEventDescriptorFile();
}

void Module::addEvent(std::unique_ptr<Event> event) {
    if (event->id >= start && event->id < (start + max_events_per_module)) {
        events.push_back(std::move(event));
    } else {
        std::stringstream ss;
        ss << "Event identifier " << event->id
           << " outside the legal range for "
           << "module " << name << "s legal range: " << start << " - "
           << start + max_events_per_module;
        throw std::invalid_argument(ss.str());
    }
}

void Module::createHeaderFile() {
    if (header.empty()) {
        return;
    }

    std::ofstream headerfile;
    headerfile.open(header);
    if (!headerfile.is_open()) {
        throw std::system_error(
                errno, std::system_category(), "Failed to open " + header);
    }

    headerfile << "// This is a generated file, do not edit" << std::endl
               << "#pragma once" << std::endl;

    for (const auto& ev : events) {
        std::string nm(name);
        nm.append("_AUDIT_");
        nm.append(ev->name);
        std::replace(nm.begin(), nm.end(), ' ', '_');
        std::transform(nm.begin(), nm.end(), nm.begin(), toupper);

        headerfile << "#define " << nm << " " << ev->id << std::endl;
    }

    headerfile.close();
}
void Module::parseEventDescriptorFile() {
    if (!is_enterprise_edition() && enterprise) {
        // enterprise files should only be loaded for enterprise builds
        return;
    }

    json = load_file(file);
    auto v = cb::jsonGet<int32_t>(json, "version");
    if (v != 1 && v != 2) {
        throw std::runtime_error("Invalid version in " + file + ": " +
                                 std::to_string(v));
    }

    auto n = cb::jsonGet<std::string>(json, "module");
    if (n != name) {
        throw std::runtime_error(name + " can't load a module named " + n);
    }

    // Parse all of the individual events
    auto e = json["events"];
    assert(e.is_array());
    auto it = e.begin();
    while (it != e.end()) {
        auto event = std::make_unique<Event>(*it);
        addEvent(std::move(event));
        ++it;
    }
}
