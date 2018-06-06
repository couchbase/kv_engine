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
#include "auditevent_generator.h"
#include "generator_event.h"
#include "generator_module.h"

#include <cJSON.h>
#include <platform/dirutils.h>
#include <platform/make_unique.h>
#include <platform/strerror.h>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>

cJSON* getMandatoryObject(gsl::not_null<const cJSON*> root,
                          const std::string& name,
                          int type) {
    cJSON* ret = getOptionalObject(root, name, type);
    if (ret == nullptr) {
        throw std::runtime_error("Mandatory element \"" + name +
                                 "\" is missing");
    }
    return ret;
}

cJSON* getOptionalObject(gsl::not_null<const cJSON*> root,
                         const std::string& name,
                         int type) {
    cJSON* ret =
            cJSON_GetObjectItem(const_cast<cJSON*>(root.get()), name.c_str());
    if (ret && ret->type != type) {
        if (type == -1) {
            if (ret->type == cJSON_True || ret->type == cJSON_False) {
                return ret;
            }
        }

        std::stringstream ss;
        ss << "Incorrect type for \"" << name << "\". Should be ";
        switch (type) {
        case cJSON_String:
            ss << "string";
            break;
        case cJSON_Number:
            ss << "number";
            break;
        default:
            ss << type;
        }

        throw std::runtime_error(ss.str());
    }

    return ret;
}

bool is_enterprise_edition() {
#ifdef COUCHBASE_ENTERPRISE_EDITION
    return true;
#else
    return false;
#endif
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

unique_cJSON_ptr load_file(const std::string& fname) {
    auto str = cb::io::loadFile(fname);
    if (str.empty()) {
        throw std::runtime_error(fname + " contained no data");
    }

    unique_cJSON_ptr ret(cJSON_Parse(str.c_str()));
    if (!ret) {
        throw std::runtime_error("Failed to parse " + fname + " containing: [" +
                                 str + "]");
    }

    return ret;
}

void parse_module_descriptors(gsl::not_null<const cJSON*> ptr,
                              std::list<std::unique_ptr<Module>>& modules,
                              const std::string& srcroot,
                              const std::string& objroot) {
    auto* mod = getMandatoryObject(ptr, "modules", cJSON_Array);
    for (auto* obj = mod->child; obj != nullptr; obj = obj->next) {
        auto new_module =
                std::make_unique<Module>(obj->child, srcroot, objroot);
        if (new_module->enterprise && !is_enterprise_edition()) {
            // Community edition should ignore modules from enterprise Edition
        } else {
            modules.emplace_back(std::move(new_module));
        }
    }
}

void create_master_file(const std::list<std::unique_ptr<Module>>& modules,
                        const std::string& output_file) {
    unique_cJSON_ptr output_json(cJSON_CreateObject());

    cJSON_AddNumberToObject(output_json.get(), "version", 2);

    cJSON* arr = cJSON_CreateArray();
    for (const auto& mod_ptr : modules) {
        if (mod_ptr->json) {
            cJSON_AddItemReferenceToArray(arr, mod_ptr->json.get());
        }
    }
    cJSON_AddItemToObject(output_json.get(), "modules", arr);

    try {
        std::ofstream out(output_file);
        out << to_string(output_json) << std::endl;
        out.close();
    } catch (...) {
        throw std::system_error(errno,
                                std::system_category(),
                                "Failed to write \"" + output_file + "\"");
    }
}
