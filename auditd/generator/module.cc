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
#include "module.h"

#include "auditevent_generator.h"
#include "event.h"

#include <fstream>
#include <sstream>

Module::Module(cJSON* data,
               const std::string& srcRoot,
               const std::string& objRoot)
    : name(data->string) {
    // Each module contains:
    //   startid - mandatory
    //   file - mandatory
    //   header - optional
    cJSON* sid = getMandatoryObject(data, "startid", cJSON_Number);
    cJSON* fname = getMandatoryObject(data, "file", cJSON_String);
    cJSON* hfile = getOptionalObject(data, "header", cJSON_String);
    auto* ent = getOptionalObject(data, "enterprise", cJSON_True);

    start = gsl::narrow<int>(sid->valueint);
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
        throw ss.str();
    }
}

Module::~Module() {
    if (json) {
        cJSON_Delete(json);
    }

    for (auto iter = events.begin(); iter != events.end(); ++iter) {
        delete *iter;
    }
}

void Module::addEvent(Event* event) {
    events.push_back(event);
}

void Module::createHeaderFile(void) {
    if (header.empty()) {
        return;
    }

    std::ofstream headerfile;
    headerfile.open(header);
    if (!headerfile.is_open()) {
        std::stringstream ss;
        ss << "Failed to open " << header;
        throw ss.str();
    }

    headerfile << "// This is a generated file, do not edit" << std::endl
               << "#pragma once" << std::endl;

    for (auto iter = events.begin(); iter != events.end(); ++iter) {
        std::string nm(name);
        nm.append("_AUDIT_");
        auto ev = *iter;
        nm.append(ev->name);
        std::replace(nm.begin(), nm.end(), ' ', '_');
        std::transform(nm.begin(), nm.end(), nm.begin(), toupper);

        headerfile << "#define " << nm << " " << ev->id << std::endl;
    }

    headerfile.close();
}
