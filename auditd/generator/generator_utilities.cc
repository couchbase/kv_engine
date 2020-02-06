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
#include "generator_utilities.h"
#include "generator_event.h"
#include "generator_module.h"
#include "utilities/readfile.h"

#include <nlohmann/json.hpp>
#include <platform/strerror.h>
#include <fstream>
#include <iostream>
#include <memory>

#ifdef COUCHBASE_ENTERPRISE_EDITION
static bool enterprise_edition = true;
#else
static bool enterprise_edition = false;
#endif

void set_enterprise_edition(bool enable) {
    enterprise_edition = enable;
}

bool is_enterprise_edition() {
    return enterprise_edition;
}

/* Events types are defined as a hexidecimal number.
 * The event ids starts at 0x1000.
 * Each module is permitted a maximum of 4095 unique event types.
 * i.e. module1: 0x1000 - 0x1FFF
 *      module2: 0x2000 - 0x2FFF
 *      module3: 0x3000 - 0x3FFF
 *      ...
 *
 * Unfortunately JSON does not support hexidecimal numbers and therefore
 * the type id needs to be specified in decimal, i.e. using 4096 instead
 * 0x1000
 *
 * The numbering means that given an id, by using a logical
 * shift right operation ( >> 12) we can quickly identify the module.
 * Further by doing a bit-wise AND with 0xFFF we can quickly identify the
 * event in the module.
 */

nlohmann::json load_file(const std::string& fname) {
    auto str = readFile(fname);
    if (str.empty()) {
        throw std::runtime_error(fname + " contained no data");
    }

    nlohmann::json json;
    try {
        json = nlohmann::json::parse(str);
    } catch (nlohmann::json::parse_error& e) {
        throw std::runtime_error("Failed to parse " + fname + " containing: [" +
                                 str + "] with error: " + e.what());
    }

    return json;
}

void parse_module_descriptors(const nlohmann::json& json,
                              std::list<std::unique_ptr<Module>>& modules,
                              const std::string& srcroot,
                              const std::string& objroot) {
    auto mod = json["modules"];
    if (mod.is_array()) {
        for (auto& module : mod) {
            auto new_module =
                    std::make_unique<Module>(module, srcroot, objroot);
            if (new_module->enterprise && !is_enterprise_edition()) {
                // Community edition should ignore modules from enterprise
                // Edition
            } else {
                modules.emplace_back(std::move(new_module));
            }
        }
    } else {
        throw std::runtime_error("Failed to get module descriptors");
    }
}

void create_master_file(const std::list<std::unique_ptr<Module>>& modules,
                        const std::string& output_file) {
    nlohmann::json output_json;
    output_json["version"] = 2;

    auto arr = nlohmann::json::array();
    for (const auto& mod_ptr : modules) {
        arr.push_back(mod_ptr->json);
    }
    output_json["modules"] = arr;

    try {
        std::ofstream out(output_file);
        out << output_json << std::endl;
        out.close();
    } catch (...) {
        throw std::system_error(errno,
                                std::system_category(),
                                "Failed to write \"" + output_file + "\"");
    }
}
