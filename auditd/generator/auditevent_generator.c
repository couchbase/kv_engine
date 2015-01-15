/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

static int8_t spool(FILE *fp, char *dest, const size_t size) {
    size_t offset = 0;
    clearerr(fp);
    while (offset < size) {
        offset += fread(dest + offset, 1, size - offset, fp);
        if (ferror(fp) || feof(fp)) {
            return -1;
        }
    }
    return 0;
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


static void free_modules(Module *modules) {
    Module *mod_ptr = modules;

    while (mod_ptr != NULL) {
        assert(mod_ptr->name != NULL);
        free(mod_ptr->name);
        assert(mod_ptr->file != NULL);
        free(mod_ptr->file);
        assert(mod_ptr->data != NULL);
        free(mod_ptr->data);
        assert(mod_ptr->json_ptr != NULL);
        cJSON_Delete(mod_ptr->json_ptr);
        Module *tmp = mod_ptr;
        mod_ptr = mod_ptr->next;
        free(tmp);
    }
}


static void* load_file(const char *file) {
    FILE *fp;
    struct stat st;
    char *data;

    if (stat(file, &st) == -1) {
        error_exit(FILE_LOOKUP_ERROR, file);
    }

    fp = fopen(file, "rb");
    if (fp == NULL) {
        error_exit(FILE_OPEN_ERROR, file);
    }

    data = malloc(st.st_size + 1);
    if (data == NULL) {
        fclose(fp);
        error_exit(MEMORY_ALLOCATION_ERROR, NULL);
    }

    if (spool(fp, data, st.st_size) == -1) {
        free(data);
        fclose(fp);
        error_exit(SPOOL_ERROR, file);
    }

    fclose(fp);
    data[st.st_size] = 0;
    return data;
}


static Module* validate_module_descriptors(const cJSON *ptr,
                                           char *root_path) {
    assert(ptr != NULL);
    Module *modules = NULL;

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

        Module *new_module = malloc(sizeof(Module));
        if (new_module == NULL) {
            error_exit(MEMORY_ALLOCATION_ERROR, NULL);
        }
        new_module->name = strdup(module_ptr->child->string);
        if (new_module->name == NULL) {
            error_exit(STRDUP_ERROR, module_ptr->child->string);
        }

        new_module->file = new_module->data = NULL;
        new_module->json_ptr = NULL;
        new_module->next = modules;
        modules = new_module;

        cJSON *modulevalues_ptr = module_ptr->child->child;
        if (modulevalues_ptr == NULL) {
            error_exit(AUDIT_DESCRIPTORS_MISSING_JSON_DATA_ERROR, NULL);
        }
        bool startid_assigned, file_assigned = false;

        while (modulevalues_ptr != NULL) {
            switch (modulevalues_ptr->type) {
                case cJSON_Number:
                    if (strcmp("startid",modulevalues_ptr->string) != 0) {
                        error_exit(AUDIT_DESCRIPTORS_KEY_ERROR,"startid");
                    }
                    if ((modulevalues_ptr->valueint % eventid_modulus) != 0) {
                        error_exit(AUDIT_DESCRIPTORS_ID_ERROR, NULL);
                    }
                    new_module->start = modulevalues_ptr->valueint;
                    startid_assigned = true;
                    break;

                case cJSON_String:
                    if (strcmp("file",modulevalues_ptr->string) != 0) {
                        error_exit(AUDIT_DESCRIPTORS_KEY_ERROR, "file");
                    }
                    if (strlen(modulevalues_ptr->valuestring) == 0) {
                        error_exit(AUDIT_DESCRIPTORS_VALUESTRING_ERROR, "<EMPTY>");
                    }
                    char *universal_filename = strdup(modulevalues_ptr->valuestring);
                    for (uint16_t ii=0; ii < strlen(universal_filename); ++ii) {
#ifdef WIN32
                        if (universal_filename[ii] == '/') {
#else
                        if (universal_filename[ii] == '\\') {
#endif
                            universal_filename[ii] = DIRECTORY_SEPARATOR_CHARACTER;
                        }
                    }
                    uint16_t path_length = (uint16_t)strlen(root_path);
                    uint16_t filename_length = (uint16_t)strlen(universal_filename);
                    uint16_t buffer_size = path_length + 1 + filename_length + 1;
                    new_module->file = malloc(buffer_size);
                    if (new_module->file == NULL) {
                        error_exit(MEMORY_ALLOCATION_ERROR, NULL);
                    }
                    snprintf(new_module->file, buffer_size, "%s%c%s",
                             root_path, DIRECTORY_SEPARATOR_CHARACTER, universal_filename);
                    free(universal_filename);
                    file_assigned = true;
                    break;
                default:
                    error_exit(AUDIT_DESCRIPTORS_UNKNOWN_FIELD_ERROR, NULL);
            }
            modulevalues_ptr = modulevalues_ptr->next;
        }

        if (!(startid_assigned && file_assigned)) {
            error_exit(AUDIT_DESCRIPTORS_MISSING_JSON_DATA_ERROR, NULL);
        }

        module_ptr = module_ptr->next;
    }
    return modules;
}


static void validate_events(cJSON* values_ptr, const Module* mod_ptr, cJSON* event_id_arr) {
    bool id_assigned = false;
    bool name_found = false;
    bool description_found = false;
    bool sync_found = false;
    bool enabled_found = false;
    bool mandatory_fields_found = false;
    bool optional_fields_found = false;
    double id;
    bool enabled;

    while (values_ptr != NULL) {
        switch (values_ptr->type) {
            case cJSON_Number:
                if (strcmp("id",values_ptr->string) != 0) {
                    error_exit(MODULE_DESCRIPTOR_KEY_ERROR, "id");
                } else {
                    uint32_t id = (uint32_t)values_ptr->valueint;
                    if ((id < mod_ptr->start) ||
                        (id > (mod_ptr->start + max_events_per_module))) {
                        error_exit(MODULE_DESCRIPTOR_ID_ERROR, NULL);
                    }
                }
                id = values_ptr->valuedouble;
                id_assigned = true;
                break;

            case cJSON_String:
                if (strcmp("name",values_ptr->string) == 0) {
                    name_found = true;
                } else if (strcmp("description",values_ptr->string) == 0) {
                     description_found = true;
                } else {
                    error_exit(MODULE_DESCRIPTOR_KEY_ERROR, values_ptr->string);
                }
                break;

            case cJSON_False:
            case cJSON_True:
                if (strcmp("sync",values_ptr->string) == 0) {
                    sync_found = true;
                } else if (strcmp("enabled",values_ptr->string) == 0) {
#ifdef ENTERPRISE_EDITION
                    enabled = (values_ptr->type == cJSON_True) ? true : false;
#else
                    values_ptr->type = cJSON_False;
                    enabled = false;
#endif

                    enabled_found = true;
                } else {
                    error_exit(MODULE_DESCRIPTOR_KEY_ERROR, values_ptr->string);
                }
                break;

            case cJSON_Object:
                if (strcmp("mandatory_fields",values_ptr->string) == 0) {
                    mandatory_fields_found = true;
                } else if (strcmp("optional_fields",values_ptr->string) == 0) {
                    optional_fields_found = true;
                } else {
                    error_exit(MODULE_DESCRIPTOR_KEY_ERROR, values_ptr->string);
                }
                break;
            default:
                error_exit(AUDIT_DESCRIPTORS_UNKNOWN_FIELD_ERROR, NULL);
        }
        values_ptr = values_ptr->next;
    }
    if (!(id_assigned && name_found && description_found && sync_found &&
        enabled_found && mandatory_fields_found && optional_fields_found)) {
        error_exit(AUDIT_DESCRIPTORS_MISSING_JSON_DATA_ERROR, NULL);
    }
    if (enabled) {
        cJSON_AddItemToArray(event_id_arr, cJSON_CreateNumber(id));
    }
}


static void validate_modules(Module *modules, cJSON *event_id_arr) {
    Module *mod_ptr = modules;
    while (mod_ptr != NULL) {
        cJSON *ptr = mod_ptr->json_ptr;
        assert(ptr != NULL);
        if (ptr->type != cJSON_Object) {
            error_exit(MODULE_DESCRIPTOR_MISSING_JSON_OBJECT_ERROR, NULL);
        }
        ptr = ptr->child;
        if (ptr == NULL) {
            error_exit(MODULE_DESCRIPTOR_MISSING_JSON_DATA_ERROR, NULL);
        }
        bool version_found, module_found, events_found = false;

        while (ptr != NULL) {
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
                    if (strcmp(mod_ptr->name,ptr->valuestring) != 0) {
                        error_exit(MODULE_DESCRIPTOR_VALUESTRING_ERROR, mod_ptr->name);
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
                    cJSON *event_data = ptr->child;
                    if (event_data == NULL) {
                        error_exit(MODULE_DESCRIPTOR_MISSING_JSON_DATA_ERROR, NULL);
                    }
                    while(event_data != NULL) {
                        if (event_data->child == NULL) {
                            error_exit(MODULE_DESCRIPTOR_MISSING_JSON_DATA_ERROR, NULL);
                        }
                        validate_events(event_data->child, mod_ptr, event_id_arr);
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

        mod_ptr = mod_ptr -> next;
    }
}


static void create_master_file(Module *modules, const char* output_file) {
    FILE *fp = fopen(output_file, "w");
    if (fp == NULL) {
        error_exit(FILE_OPEN_ERROR, output_file);
    }

    cJSON *output_json = cJSON_CreateObject();
    if (output_json == NULL) {
        error_exit(CREATE_JSON_OBJECT_ERROR, NULL);
    }

    cJSON_AddNumberToObject(output_json, "version", 1);

    cJSON *arr = cJSON_CreateArray();
    if (arr == NULL) {
        error_exit(CREATE_JSON_ARRAY_ERROR, NULL);
    }

    Module *mod_ptr = modules;
    while (mod_ptr != NULL) {
        assert(mod_ptr->json_ptr != NULL);
        cJSON_AddItemReferenceToArray(arr,
                                      mod_ptr->json_ptr);
        mod_ptr = mod_ptr -> next;
    }

    cJSON_AddItemToObject(output_json, "modules", arr);
    char *data = cJSON_Print(output_json);
    assert(data != NULL);
    fprintf(fp, "%s", data);
    cJSON_Delete(output_json);
    free(data);
    fclose(fp);
}


static void create_config_file(Module *modules, const char* config_file,
                               cJSON *event_id_arr) {
    FILE *fp = fopen(config_file, "w");
    if (fp == NULL) {
        error_exit(FILE_OPEN_ERROR, config_file);
    }

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

    Module *mod_ptr = modules;
    while (mod_ptr != NULL) {
        assert(mod_ptr->json_ptr != NULL);
        mod_ptr = mod_ptr -> next;
    }
    cJSON_AddItemToObject(config_json, "enabled", event_id_arr);

    cJSON *arr = cJSON_CreateArray();
    if (arr == NULL) {
        error_exit(CREATE_JSON_ARRAY_ERROR, NULL);
    }
    cJSON_AddItemToObject(config_json, "sync", arr);

    char *data = cJSON_Print(config_json);
    assert(data != NULL);
    fprintf(fp, "%s", data);
    cJSON_Delete(config_json);
    cJSON_Free(data);
    fclose(fp);
}


int main(int argc, char **argv) {
    char *data;
    Module *modules = NULL;
    char *input_file;
    char *output_file;
    char *config_file;
    char *root_path;
    int cmd;

    while ((cmd = getopt(argc, argv, "c:i:r:o:")) != -1) {
        switch (cmd) {
        case 'r': /* root */
            root_path = optarg;
            break;
        case 'o': /* output file */
            output_file = optarg;
            break;
        case 'i': /* input file */
            input_file = optarg;
            break;
        case 'c': /* config file */
            config_file = optarg;
            break;
        default:
            error_exit(USAGE_ERROR,argv[0]);
        }
    }

    data = load_file(input_file);
    assert(data != NULL);

    cJSON *ptr = cJSON_Parse(data);
    if (ptr == NULL) {
        error_exit(AUDIT_DESCRIPTORS_PARSING_ERROR, NULL);
    }

    modules = validate_module_descriptors(ptr, root_path);

    Module *mod_ptr = modules;
    while (mod_ptr != NULL) {
        assert(mod_ptr->file != NULL);
        mod_ptr->data = load_file(mod_ptr->file);
        assert(mod_ptr->data != NULL);
        mod_ptr->json_ptr = cJSON_Parse(mod_ptr->data);
        if (mod_ptr->json_ptr == NULL) {
            error_exit(MODULE_DESCRIPTOR_PARSING_ERROR, NULL);
        }
        mod_ptr = mod_ptr->next;
    }

    cJSON *event_id_arr = cJSON_CreateArray();
    if (event_id_arr == NULL) {
        error_exit(CREATE_JSON_ARRAY_ERROR, NULL);
    }

    validate_modules(modules, event_id_arr);
    create_master_file(modules, output_file);
    create_config_file(modules, config_file, event_id_arr);
    cJSON_Delete(ptr);
    free(data);
    free_modules(modules);
    exit(EXIT_SUCCESS);
}
