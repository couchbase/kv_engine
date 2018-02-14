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

#ifndef AUDIT_DESCRIPTORS_H
#define AUDIT_DESCRIPTORS_H

#include <string>

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

cJSON *getMandatoryObject(cJSON *root, const std::string &name, int type);
cJSON *getOptionalObject(cJSON *root, const std::string &name, int type);

class Event {
public:
    Event(cJSON *root) {
        cJSON *cId = getMandatoryObject(root, "id", cJSON_Number);
        cJSON *cName = getMandatoryObject(root, "name", cJSON_String);
        cJSON *cDescr = getMandatoryObject(root, "description", cJSON_String);
        cJSON *cSync = getMandatoryObject(root, "sync", -1);
        cJSON *cEnabled = getMandatoryObject(root, "enabled", -1);
        cJSON *cMand = getMandatoryObject(root, "mandatory_fields", cJSON_Object);
        cJSON *cOpt = getMandatoryObject(root, "optional_fields", cJSON_Object);

        id = uint32_t(cId->valueint);
        name.assign(cName->valuestring);
        description.assign(cDescr->valuestring);
        sync = cSync->type == cJSON_True;
        enabled = cEnabled->type == cJSON_True;
        char *ptr = cJSON_PrintUnformatted(cMand);
        mandatory_fields.assign(ptr);
        cJSON_Free(ptr);
        ptr = cJSON_PrintUnformatted(cOpt);
        optional_fields.assign(ptr);
        cJSON_Free(ptr);

        int num_elem = cJSON_GetArraySize(root);
        if (num_elem != 7) {
            std::stringstream ss;
            char *bulk = cJSON_Print(root);
            ss << "Unknown elements for " << name
               << ": " << std::endl << bulk << std::endl;
            cJSON_Free(bulk);
            throw ss.str();
        }
    }

    uint32_t id;
    std::string name;
    std::string description;
    bool sync;
    bool enabled;
    std::string mandatory_fields;
    std::string optional_fields;
};

class Module {
public:
    Module(cJSON *data,
           const std::string &srcRoot,
           const std::string &objRoot)
        : name(data->string),
          json(NULL)
    {
        // Each module contains:
        //   startid - mandatory
        //   file - mandatory
        //   header - optional
        cJSON *sid = getMandatoryObject(data, "startid", cJSON_Number);
        cJSON *fname = getMandatoryObject(data, "file", cJSON_String);
        cJSON *hfile = getOptionalObject(data, "header", cJSON_String);

        start = sid->valueint;
        file.assign(srcRoot);
        file.append("/");
        file.append(fname->valuestring);

        if (DIRECTORY_SEPARATOR_CHARACTER == '\\') {
            std::replace(file.begin(), file.end(), '/', '\\');
        }

        if (hfile) {
            std::string hp = hfile->valuestring;
            if (!hp.empty()) {
                header.assign(objRoot);
                header.append("/");
                header.append(hp);

                if (DIRECTORY_SEPARATOR_CHARACTER == '\\') {
                    std::replace(header.begin(), header.end(), '/', '\\');
                }
            }
        }

        int num_elem = cJSON_GetArraySize(data);
        int expected = (hfile == NULL) ? 2 : 3;
        if (num_elem != expected) {
            std::stringstream ss;
            char *bulk = cJSON_Print(data);
            ss << "Unknown elements for " << name
               << ": " << std::endl << bulk << std::endl;
            cJSON_Free(bulk);
            throw ss.str();
        }
    }

    ~Module() {
        if (json) {
            cJSON_Delete(json);
        }

        for (auto iter = events.begin(); iter != events.end(); ++iter) {
            delete *iter;
        }
    }

    void addEvent(Event *event) {
        events.push_back(event);
    }

    void createHeaderFile(void) {
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

            headerfile << "#define " << nm << " "
                       << ev->id << std::endl;
        }

        headerfile.close();
    }

    /**
     * The name of the module
     */
    std::string name;
    /**
     * The lowest identifier for the audit events in this module. All
     * audit descriptor defined for this module MUST be within the range
     * [start, start + max_events_per_module]
     */
    uint32_t start;
    /**
     * The name of the file containing the audit descriptors for this
     * module.
     */
    std::string file;
    /**
     * The JSON data describing the audit descriptors for this module
     */
    cJSON *json;
private:
    /**
     * If present this is the name of a C headerfile to generate with
     * #defines for all audit identifiers for the module.
     */
    std::string header;
    /**
     * A list of all of the events defined for this module
     */
    std::list<Event *> events;
};

#endif
