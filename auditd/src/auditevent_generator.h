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

#ifndef AUDIT_DESCRIPTORS_H
#define AUDIT_DESCRIPTORS_H

#define eventid_modulus 4096
#define max_events_per_module (eventid_modulus - 1)

typedef enum {
    OK,
    USAGE_ERROR,
    FILE_LOOKUP_ERROR,
    FILE_OPEN_ERROR,
    SPOOL_ERROR,
    MEMORY_ALLOCATION_ERROR,
    STRDUP_ERROR,
    CREATE_JSON_OBJECT_ERROR,
    CREATE_JSON_ARRAY_ERROR,
    AUDIT_DESCRIPTORS_PARSING_ERROR,
    AUDIT_DESCRIPTORS_MISSING_JSON_DATA_ERROR,
    AUDIT_DESCRIPTORS_MISSING_JSON_OBJECT_ERROR,
    AUDIT_DESCRIPTORS_MISSING_JSON_ARRAY_ERROR,
    AUDIT_DESCRIPTORS_MISSING_JSON_NUMBER_ERROR,
    AUDIT_DESCRIPTORS_MISSING_JSON_STRING_ERROR,
    AUDIT_DESCRIPTORS_MISSING_JSON_BOOL_ERROR,
    AUDIT_DESCRIPTORS_KEY_ERROR,
    AUDIT_DESCRIPTORS_ID_ERROR,
    AUDIT_DESCRIPTORS_VALUESTRING_ERROR,
    AUDIT_DESCRIPTORS_UNKNOWN_FIELD_ERROR,
    MODULE_DESCRIPTOR_PARSING_ERROR,
    MODULE_DESCRIPTOR_MISSING_JSON_DATA_ERROR,
    MODULE_DESCRIPTOR_MISSING_JSON_OBJECT_ERROR,
    MODULE_DESCRIPTOR_MISSING_JSON_ARRAY_ERROR,
    MODULE_DESCRIPTOR_MISSING_JSON_NUMBER_ERROR,
    MODULE_DESCRIPTOR_MISSING_JSON_STRING_ERROR,
    MODULE_DESCRIPTOR_MISSING_JSON_BOOL_ERROR,
    MODULE_DESCRIPTOR_KEY_ERROR,
    MODULE_DESCRIPTOR_ID_ERROR,
    MODULE_DESCRIPTOR_VALUESTRING_ERROR,
    MODULE_DESCRIPTOR_UNKNOWN_FIELD_ERROR
} ReturnCode;

typedef struct module {
    char *name;
    uint32_t start;
    char* file;
    char* data;
    cJSON *json_ptr;
    struct module* next;
}Module;

#endif
