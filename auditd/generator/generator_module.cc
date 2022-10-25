/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include <nlohmann/json.hpp>
#include <utilities/json_utilities.h>

#include <gsl/gsl-lite.hpp>
#include <fstream>
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

void Module::createHeaderFile(std::ostream& out) {
    for (const auto& ev : events) {
        std::string nm(name);
        nm.append("_AUDIT_");
        nm.append(ev->name);
        std::replace(nm.begin(), nm.end(), ' ', '_');
        std::replace(nm.begin(), nm.end(), '/', '_');
        std::replace(nm.begin(), nm.end(), ':', '_');
        std::transform(nm.begin(), nm.end(), nm.begin(), toupper);
        out << "#define " << nm << " " << ev->id << std::endl;
    }
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
