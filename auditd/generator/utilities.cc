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
#include "utilities.h"
#include "auditevent_generator.h"
#include "event.h"
#include "module.h"

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
        throw std::logic_error("Mandatory element \"" + name + "\" is missing");
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

        throw std::logic_error(ss.str());
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

unique_cJSON_ptr load_file(const std::string fname) {
    auto str = cb::io::loadFile(fname);
    if (str.empty()) {
        throw std::logic_error(fname + " contained no data");
    }

    unique_cJSON_ptr ret(cJSON_Parse(str.c_str()));
    if (!ret) {
        throw std::logic_error("Failed to parse " + fname + " containing: [" +
                               str + "]");
    }

    return ret;
}

void validate_module_descriptors(gsl::not_null<const cJSON*> ptr,
                                 std::list<Module*>& modules,
                                 const std::string& srcroot,
                                 const std::string& objroot) {
    if (ptr->type != cJSON_Object) {
        fprintf(stderr, "audit descriptors: missing JSON object\n");
        exit(EXIT_FAILURE);
    }

    cJSON* modulelist_ptr = ptr->child;
    if (modulelist_ptr == NULL) {
        fprintf(stderr, "audit descriptors: missing JSON data\n");
        exit(EXIT_FAILURE);
    }
    if (modulelist_ptr->type != cJSON_Array) {
        fprintf(stderr, "audit descriptors: missing JSON array\n");
        exit(EXIT_FAILURE);
    }
    if (strcmp("modules", modulelist_ptr->string) != 0) {
        fprintf(stderr, "audit descriptors: key \"modules\" error\n");
        exit(EXIT_FAILURE);
    }

    cJSON* module_ptr = modulelist_ptr->child;
    while (module_ptr != NULL) {
        if (module_ptr->child == NULL) {
            fprintf(stderr, "audit descriptors: missing JSON data\n");
            exit(EXIT_FAILURE);
        }
        if ((module_ptr->type != cJSON_Object) ||
            (module_ptr->child->type != cJSON_Object)) {
            fprintf(stderr, "audit descriptors: missing JSON object\n");
            exit(EXIT_FAILURE);
        }
        if (module_ptr->child->string == NULL) {
            fprintf(stderr, "audit descriptors: key cannot be nullptr");
            exit(EXIT_FAILURE);
        }

        auto new_module =
                std::make_unique<Module>(module_ptr->child, srcroot, objroot);
        if (new_module->enterprise && !is_enterprise_edition()) {
            // Community edition should ignore modules from enterprise Edition
        } else {
            modules.push_back(new_module.release());
        }
        module_ptr = module_ptr->next;
    }
}

void validate_events(const Event& ev,
                     const Module* module,
                     cJSON* event_id_arr) {
    if (ev.id < module->start ||
        ev.id > (module->start + max_events_per_module)) {
        std::stringstream ss;
        ss << "Event identifier " << ev.id << " outside the legal range for "
           << "module " << module->name << "s legal range: " << module->start
           << " - " << module->start + max_events_per_module;
        throw std::logic_error(ss.str());
    }

    if (!ev.enabled) {
        cJSON_AddItemToArray(event_id_arr, cJSON_CreateNumber(ev.id));
    }
}

void validate_modules(const std::list<Module*>& modules, cJSON* event_id_arr) {
    for (auto iter = modules.begin(); iter != modules.end(); ++iter) {
        auto mod_ptr = *iter;
        cJSON* ptr = mod_ptr->json.get();
        if (ptr == nullptr || ptr->type != cJSON_Object) {
            fprintf(stderr, "module descriptor: missing JSON object\n");
            exit(EXIT_FAILURE);
        }
        ptr = ptr->child;
        if (ptr == NULL) {
            fprintf(stderr, "module descriptor: missing JSON data\n");
            exit(EXIT_FAILURE);
        }
        bool version_found = false;
        bool module_found = false;
        bool events_found = false;

        while (ptr != NULL) {
            cJSON* event_data = 0;

            switch (ptr->type) {
            case cJSON_Number:
                if (strcmp("version", ptr->string) != 0) {
                    fprintf(stderr,
                            "module descriptor: key \"version\" error\n");
                    exit(EXIT_FAILURE);
                }
                version_found = true;
                break;

            case cJSON_String:
                if (strcmp("module", ptr->string) != 0) {
                    fprintf(stderr,
                            "module descriptor: key \"module\" error\n");
                    exit(EXIT_FAILURE);
                }
                if (strcmp(mod_ptr->name.c_str(), ptr->valuestring) != 0) {
                    fprintf(stderr,
                            "module descriptor: valuestring error: %s\n",
                            mod_ptr->name.c_str());
                    exit(EXIT_FAILURE);
                }
                module_found = true;
                break;

            case cJSON_Array:
                if (strcmp("events", ptr->string) != 0) {
                    fprintf(stderr,
                            "module descriptor: key \"events\" error\n");
                    exit(EXIT_FAILURE);
                }
                if (ptr->child->type != cJSON_Object) {
                    fprintf(stderr, "module descriptor: missing JSON object\n");
                    exit(EXIT_FAILURE);
                }
                event_data = ptr->child;
                if (event_data == NULL) {
                    fprintf(stderr, "module descriptor: missing JSON data\n");
                    exit(EXIT_FAILURE);
                }
                while (event_data != NULL) {
                    if (event_data->child == NULL) {
                        fprintf(stderr,
                                "module descriptor: missing JSON data\n");
                        exit(EXIT_FAILURE);
                    }

                    try {
                        auto ev = std::make_unique<Event>(event_data);
                        validate_events(*ev, mod_ptr, event_id_arr);
                        mod_ptr->addEvent(std::move(ev));
                    } catch (const std::exception& error) {
                        std::cerr << error.what() << std::endl;
                        ;
                        exit(EXIT_FAILURE);
                    }

                    event_data = event_data->next;
                }
                events_found = true;
                break;
            default:
                fprintf(stderr, "audit descriptors: unknown field\n");
                exit(EXIT_FAILURE);
            }
            ptr = ptr->next;
        }

        if (!(version_found && module_found && events_found)) {
            fprintf(stderr, "module descriptor: missing JSON data\n");
            exit(EXIT_FAILURE);
        }
    }
}

void create_master_file(const std::list<Module*>& modules,
                        const std::string& output_file) {
    unique_cJSON_ptr output_json(cJSON_CreateObject());

    cJSON_AddNumberToObject(output_json.get(), "version", 2);

    cJSON* arr = cJSON_CreateArray();
    for (auto iter = modules.begin(); iter != modules.end(); ++iter) {
        auto mod_ptr = *iter;
        ;
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
        fprintf(stderr,
                "open error on file %s: %s\n",
                output_file.c_str(),
                cb_strerror().c_str());
        exit(EXIT_FAILURE);
    }
}
