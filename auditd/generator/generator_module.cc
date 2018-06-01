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

#include <platform/make_unique.h>

#include <fstream>
#include <sstream>
#include <system_error>

Module::Module(gsl::not_null<const cJSON*> object,
               const std::string& srcRoot,
               const std::string& objRoot)
    : name(object->string) {
    auto* data = const_cast<cJSON*>(object.get());

    // Each module contains:
    //   startid - mandatory
    //   file - mandatory
    //   header - optional
    cJSON* sid = getMandatoryObject(data, "startid", cJSON_Number);
    cJSON* fname = getMandatoryObject(data, "file", cJSON_String);
    cJSON* hfile = getOptionalObject(data, "header", cJSON_String);
    auto* ent = getOptionalObject(data, "enterprise", -1);

    start = sid->valueint;
    file.assign(srcRoot);
    file.append("/");
    file.append(fname->valuestring);
    cb::io::sanitizePath(file);

    int expected = 2;

    if (hfile) {
        std::string hp = hfile->valuestring;
        if (!hp.empty()) {
            header.assign(objRoot);
            header.append("/");
            header.append(hp);
            cb::io::sanitizePath(header);
        }
        ++expected;
    }

    if (ent) {
        enterprise = ent->type == cJSON_True;
        ++expected;
    }

    if (cJSON_GetArraySize(data) != expected) {
        std::stringstream ss;
        ss << "Unknown elements for " << name << ": " << std::endl
           << to_string(data) << std::endl;
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
    auto* obj = getMandatoryObject(json.get(), "version", cJSON_Number);
    if (obj->valueint != 1 && obj->valueint != 2) {
        throw std::runtime_error("Invalid version in " + file + ": " +
                                 std::to_string(obj->valueint));
    }

    obj = getMandatoryObject(json.get(), "module", cJSON_String);
    if (name != obj->valuestring) {
        throw std::runtime_error(name + " can't load a module named " +
                                 obj->valuestring);
    }

    obj = getMandatoryObject(json.get(), "events", cJSON_Array);
    // Parse all of the individual events
    for (auto* ev = obj->child; ev != nullptr; ev = ev->next) {
        auto event = std::make_unique<Event>(ev);
        addEvent(std::move(event));
    }
}
