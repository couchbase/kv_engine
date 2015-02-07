/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc.
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

#include <limits.h>
#include "config.h"
#include <algorithm>
#include <fstream>
#include <sstream>
#include <iostream>
#include <list>
#include <string>
#include <assert.h>
#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <cJSON.h>
#include <strings.h>
#include <getopt.h>
#include "auditevent_generator.h"

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

static cJSON *load_file(const std::string fname) {
    std::ifstream file(fname, std::ios::in | std::ios::binary);
    if (!file.is_open()) {
        std::stringstream ss;
        ss << "Failed to open: " << fname;
        throw ss.str();
    }

    std::string str((std::istreambuf_iterator<char>(file)),
                    std::istreambuf_iterator<char>());
    file.close();
    if (str.empty()) {
        std::stringstream ss;
        ss << fname << " contained no data";
        throw ss.str();
    }

    cJSON *ret = cJSON_Parse(str.c_str());
    if (ret == NULL) {
        std::stringstream ss;
        ss << "Failed to parse " << fname << " containing: " << std::endl
           << str << std::endl;
        throw ss.str();
    }
    return ret;
}

static void error_exit(const ReturnCode return_code, const char *string) {
    switch (return_code) {
    case USAGE_ERROR:
        assert(string != NULL);
        fprintf(stderr, "usage: %s -r PATH -i FILE -o FILE\n", string);
        break;
    case FILE_LOOKUP_ERROR:
        assert(string != NULL);
        fprintf(stderr, "lookup error on file %s: %s\n", string, strerror(errno));
        break;
    case FILE_OPEN_ERROR:
        assert(string != NULL);
        fprintf(stderr, "open error on file %s: %s\n", string, strerror(errno));
        break;
    case SPOOL_ERROR:
        assert(string != NULL);
        fprintf(stderr, "spool error on file %s: %s\n", string, strerror(errno));
        break;
    case MEMORY_ALLOCATION_ERROR:
        fprintf(stderr, "memory allocation failed: %s\n", strerror(errno));
        break;
    case STRDUP_ERROR:
        assert(string != NULL);
        fprintf(stderr, "strdup of %s failed: %s\n",string, strerror(errno));
        break;
    case CREATE_JSON_OBJECT_ERROR:
        fprintf(stderr, "create json object error\n");
        break;
    case CREATE_JSON_ARRAY_ERROR:
        fprintf(stderr, "create json array error\n");
        break;
    case AUDIT_DESCRIPTORS_PARSING_ERROR:
        fprintf(stderr, "audit descriptors: JSON parsing error\n");
        break;
    case AUDIT_DESCRIPTORS_MISSING_JSON_DATA_ERROR:
        fprintf(stderr, "audit descriptors: missing JSON data\n");
        break;
    case AUDIT_DESCRIPTORS_MISSING_JSON_OBJECT_ERROR:
        fprintf(stderr, "audit descriptors: missing JSON object\n");
        break;
    case AUDIT_DESCRIPTORS_MISSING_JSON_ARRAY_ERROR:
        fprintf(stderr, "audit descriptors: missing JSON array\n");
        break;
    case AUDIT_DESCRIPTORS_MISSING_JSON_NUMBER_ERROR:
        fprintf(stderr, "audit descriptors: missing JSON number\n");
        break;
    case AUDIT_DESCRIPTORS_MISSING_JSON_STRING_ERROR:
        fprintf(stderr, "audit descriptors: missing JSON string\n");
        break;
    case AUDIT_DESCRIPTORS_MISSING_JSON_BOOL_ERROR:
        fprintf(stderr, "audit descriptors: missing JSON boolean\n");
        break;
    case AUDIT_DESCRIPTORS_KEY_ERROR:
        assert(string != NULL);
        fprintf(stderr, "audit descriptors: key \"%s\" error\n", string);
        break;
    case AUDIT_DESCRIPTORS_ID_ERROR:
        fprintf(stderr, "audit descriptors: eventid error\n");
        break;
    case AUDIT_DESCRIPTORS_VALUESTRING_ERROR:
        assert(string != NULL);
        fprintf(stderr, "audit descriptors: valuestring error: %s\n", string);
        break;
    case AUDIT_DESCRIPTORS_UNKNOWN_FIELD_ERROR:
        fprintf(stderr, "audit descriptors: unknown field\n");
        break;
    case MODULE_DESCRIPTOR_PARSING_ERROR:
        fprintf(stderr, "module descriptor parsing error\n");
        break;
    case MODULE_DESCRIPTOR_MISSING_JSON_DATA_ERROR:
        fprintf(stderr, "module descriptor: missing JSON data\n");
        break;
    case MODULE_DESCRIPTOR_MISSING_JSON_OBJECT_ERROR:
        fprintf(stderr, "module descriptor: missing JSON object\n");
        break;
    case MODULE_DESCRIPTOR_MISSING_JSON_ARRAY_ERROR:
        fprintf(stderr, "module descriptor: missing JSON array\n");
        break;
    case MODULE_DESCRIPTOR_MISSING_JSON_NUMBER_ERROR:
        fprintf(stderr, "module descriptor: missing JSON number\n");
        break;
    case MODULE_DESCRIPTOR_MISSING_JSON_STRING_ERROR:
        fprintf(stderr, "module descriptor: missing JSON string\n");
        break;
    case MODULE_DESCRIPTOR_MISSING_JSON_BOOL_ERROR:
        fprintf(stderr, "module descriptor: missing JSON bool\n");
        break;
    case MODULE_DESCRIPTOR_KEY_ERROR:
        fprintf(stderr, "module descriptor: key \"%s\" error\n", string);
        break;
    case MODULE_DESCRIPTOR_ID_ERROR:
        fprintf(stderr, "module descriptor: eventid error\n");
        break;
    case MODULE_DESCRIPTOR_VALUESTRING_ERROR:
        assert(string != NULL);
        fprintf(stderr, "module descriptor: valuestring error: %s\n", string);
        break;
    case MODULE_DESCRIPTOR_UNKNOWN_FIELD_ERROR:
        fprintf(stderr, "module descriptor: unknown field\n");
        break;
    default:
        assert(false);
    }
    exit(EXIT_FAILURE);
}

cJSON *getMandatoryObject(cJSON *root, const std::string &name, int type) {
    cJSON *ret = getOptionalObject(root, name, type);
    if (ret == NULL) {
        std::stringstream ss;
        ss << "Mandatory element \"" << name << "\" is missing ";
        throw ss.str();
    }
    return ret;
}

cJSON *getOptionalObject(cJSON *root, const std::string &name, int type)
{
    cJSON *ret = cJSON_GetObjectItem(root, name.c_str());
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

        throw ss.str();
    }

    return ret;
}

static void validate_module_descriptors(const cJSON *ptr,
                                        std::list<Module*> &modules,
                                        const std::string &srcroot,
                                        const std::string &objroot) {
    assert(ptr != NULL);

    if (ptr->type != cJSON_Object) {
        error_exit(AUDIT_DESCRIPTORS_MISSING_JSON_OBJECT_ERROR, NULL);
    }

    cJSON *modulelist_ptr = ptr->child;
    if  (modulelist_ptr == NULL) {
        error_exit(AUDIT_DESCRIPTORS_MISSING_JSON_DATA_ERROR, NULL);
    }
    if (modulelist_ptr->type != cJSON_Array) {
        error_exit(AUDIT_DESCRIPTORS_MISSING_JSON_ARRAY_ERROR, NULL);
    }
    if (strcmp("modules",modulelist_ptr->string) != 0) {
        error_exit(AUDIT_DESCRIPTORS_KEY_ERROR,"modules");
    }

    cJSON *module_ptr = modulelist_ptr->child;
    while (module_ptr != NULL) {
        if (module_ptr->child == NULL) {
            error_exit(AUDIT_DESCRIPTORS_MISSING_JSON_DATA_ERROR, NULL);
        }
        if ((module_ptr->type != cJSON_Object) ||
            (module_ptr->child->type != cJSON_Object)) {
            error_exit(AUDIT_DESCRIPTORS_MISSING_JSON_OBJECT_ERROR, NULL);
        }
        if (module_ptr->child->string == NULL) {
            error_exit(AUDIT_DESCRIPTORS_KEY_ERROR, NULL);
        }

        Module *new_module = new Module(module_ptr->child, srcroot, objroot);
        modules.push_back(new_module);
        module_ptr = module_ptr->next;
    }
}


static void validate_events(const Event &ev,
                            const Module *module,
                            cJSON* event_id_arr)
{
    if (ev.id < module->start || ev.id > (module->start + max_events_per_module)) {
        std::stringstream ss;
        ss << "Event identifier " << ev.id << " outside the legal range for "
           << "module " << module->name << "s legal range: "
           << module->start << " - " << module->start + max_events_per_module;
        throw ss.str();
    }

    if (!ev.enabled) {
        cJSON_AddItemToArray(event_id_arr, cJSON_CreateNumber(ev.id));
    }
}

static void validate_modules(const std::list<Module *> &modules,
                             cJSON *event_id_arr) {

    for (auto iter = modules.begin(); iter != modules.end(); ++iter) {
        auto mod_ptr = *iter;
        cJSON *ptr = mod_ptr->json;
        assert(ptr != NULL);
        if (ptr->type != cJSON_Object) {
            error_exit(MODULE_DESCRIPTOR_MISSING_JSON_OBJECT_ERROR, NULL);
        }
        ptr = ptr->child;
        if (ptr == NULL) {
            error_exit(MODULE_DESCRIPTOR_MISSING_JSON_DATA_ERROR, NULL);
        }
        bool version_found = false;
        bool module_found = false;
        bool events_found = false;

        while (ptr != NULL) {
            cJSON *event_data = 0;

            switch (ptr->type) {
                case cJSON_Number:
                    if (strcmp("version",ptr->string) != 0) {
                        error_exit(MODULE_DESCRIPTOR_KEY_ERROR, "version");
                    }
                    version_found = true;
                    break;

                case cJSON_String:
                    if (strcmp("module",ptr->string) != 0) {
                        error_exit(MODULE_DESCRIPTOR_KEY_ERROR, "module");
                    }
                    if (strcmp(mod_ptr->name.c_str(), ptr->valuestring) != 0) {
                        error_exit(MODULE_DESCRIPTOR_VALUESTRING_ERROR,
                                   mod_ptr->name.c_str());
                    }
                    module_found = true;
                    break;

                case cJSON_Array:
                    if (strcmp("events",ptr->string) != 0) {
                        error_exit(MODULE_DESCRIPTOR_KEY_ERROR, "events");
                    }
                    if (ptr->child->type != cJSON_Object) {
                        error_exit(MODULE_DESCRIPTOR_MISSING_JSON_OBJECT_ERROR, NULL);
                    }
                    event_data = ptr->child;
                    if (event_data == NULL) {
                        error_exit(MODULE_DESCRIPTOR_MISSING_JSON_DATA_ERROR, NULL);
                    }
                    while(event_data != NULL) {
                        if (event_data->child == NULL) {
                            error_exit(MODULE_DESCRIPTOR_MISSING_JSON_DATA_ERROR, NULL);
                        }

                        try {
                            Event *ev = new Event(event_data);
                            validate_events(*ev, mod_ptr, event_id_arr);
                            mod_ptr->addEvent(ev);

                        } catch (std::string error) {
                            std::cerr << error << std::endl;;
                            exit(EXIT_FAILURE);
                        }

                        event_data = event_data->next;
                    }
                    events_found = true;
                    break;
                default:
                    error_exit(AUDIT_DESCRIPTORS_UNKNOWN_FIELD_ERROR, NULL);
            }
            ptr = ptr->next;
        }

        if (!(version_found && module_found && events_found)) {
            error_exit(MODULE_DESCRIPTOR_MISSING_JSON_DATA_ERROR, NULL);
        }
    }
}

static void create_master_file(const std::list<Module *> &modules,
                               const std::string &output_file) {
    cJSON *output_json = cJSON_CreateObject();
    if (output_json == NULL) {
        error_exit(CREATE_JSON_OBJECT_ERROR, NULL);
    }

    cJSON_AddNumberToObject(output_json, "version", 1);

    cJSON *arr = cJSON_CreateArray();
    if (arr == NULL) {
        error_exit(CREATE_JSON_ARRAY_ERROR, NULL);
    }

    for (auto iter = modules.begin(); iter != modules.end(); ++iter) {
        auto mod_ptr = *iter;;
        assert(mod_ptr->json != NULL);
        cJSON_AddItemReferenceToArray(arr, mod_ptr->json);
    }
    cJSON_AddItemToObject(output_json, "modules", arr);

    char *data = cJSON_Print(output_json);
    assert(data != NULL);

    try {
        std::ofstream out(output_file);
        out << data << std::endl;
        out.close();
    } catch (...) {
        error_exit(FILE_OPEN_ERROR, output_file.c_str());
    }

    cJSON_Delete(output_json);
    cJSON_Free(data);
}

static void create_config_file(const std::string &config_file,
                               cJSON *event_id_arr) {
    cJSON *config_json = cJSON_CreateObject();
    if (config_json == NULL) {
        error_exit(CREATE_JSON_OBJECT_ERROR, NULL);
    }

    char full_path[PATH_MAX];
    sprintf(full_path, "%s%cvar%clib%ccouchbase%clogs", DESTINATION_ROOT,
            DIRECTORY_SEPARATOR_CHARACTER, DIRECTORY_SEPARATOR_CHARACTER,
            DIRECTORY_SEPARATOR_CHARACTER, DIRECTORY_SEPARATOR_CHARACTER);
    cJSON_AddNumberToObject(config_json, "version", 1);
    cJSON_AddTrueToObject(config_json,"auditd_enabled");
    cJSON_AddNumberToObject(config_json, "rotate_interval", 86400);
    cJSON_AddStringToObject(config_json, "log_path", full_path);
    cJSON_AddStringToObject(config_json, "archive_path", full_path);
    cJSON_AddStringToObject(config_json, "descriptors_path", full_path);
    cJSON_AddItemToObject(config_json, "disabled", event_id_arr);

    cJSON *arr = cJSON_CreateArray();
    if (arr == NULL) {
        error_exit(CREATE_JSON_ARRAY_ERROR, NULL);
    }
    cJSON_AddItemToObject(config_json, "sync", arr);

    char *data = cJSON_Print(config_json);
    assert(data != NULL);

    try {
        std::ofstream out(config_file);
        out << data << std::endl;
        out.close();
    } catch (...) {
        error_exit(FILE_OPEN_ERROR, config_file.c_str());
    }

    cJSON_Delete(config_json);
    cJSON_Free(data);
}

int main(int argc, char **argv) {
    std::string input_file;
    std::string output_file;
    std::string config_file;
    std::string srcroot;
    std::string objroot;
    int cmd;

    while ((cmd = getopt(argc, argv, "c:i:r:b:o:")) != -1) {
        switch (cmd) {
        case 'r': /* root */
            srcroot.assign(optarg);
            break;
        case 'b': /* binary root */
            objroot.assign(optarg);
            break;
        case 'o': /* output file */
            output_file.assign(optarg);
            break;
        case 'i': /* input file */
            input_file.assign(optarg);
            break;
        case 'c': /* config file */
            config_file.assign(optarg);
            break;
        default:
            error_exit(USAGE_ERROR, argv[0]);
        }
    }

    cJSON *ptr;
    try {
        ptr = load_file(input_file);
    } catch (std::string err) {
        std::cerr << err;
        exit(EXIT_FAILURE);
    }

    std::list<Module*> modules;

    try {
        validate_module_descriptors(ptr, modules, srcroot, objroot);
        for (auto iter = modules.begin(); iter != modules.end(); ++iter) {
            auto module = *iter;
            module->json = load_file(module->file);
        }
    } catch (std::string error) {
        std::cerr << "Failed to load " << input_file << ":" << std::endl
                  << error << std::endl;
        exit(EXIT_FAILURE);
    }

    cJSON *event_id_arr = cJSON_CreateArray();
    if (event_id_arr == NULL) {
        error_exit(CREATE_JSON_ARRAY_ERROR, NULL);
    }

    validate_modules(modules, event_id_arr);
    create_master_file(modules, output_file);
    create_config_file(config_file, event_id_arr);

    cJSON_Delete(ptr);
    for (auto iter = modules.begin(); iter != modules.end(); ++iter) {
        auto module = *iter;
        try {
            module->createHeaderFile();
        } catch (std::string error) {
            std::cerr << "Failed to write heder file for " << module->name
                      << ":" << std::endl << error << std::endl;
            exit(EXIT_FAILURE);
        }
        delete module;
    }
    exit(EXIT_SUCCESS);
}
