/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

#include <iostream>
#include <cstring>
#include <cstdio>
#include <sstream>
#include <cJSON.h>
#include <cerrno>
#include <algorithm>

#include <platform/dirutils.h>

#include "auditd.h"
#include "audit.h"
#include "auditconfig.h"

/**
 * Get a named object from located under "root". It should be of the
 * given type (or -1 for a boolean value)
 *
 * @param root the root of the json object
 * @param name the name of the tag to search for
 * @param type the type for the object (or -1 for boolean)
 */
static cJSON *getObject(const cJSON *root, const char *name, int type)
{
    cJSON *ret = cJSON_GetObjectItem(const_cast<cJSON*>(root), name);
    if (ret) {
        if (ret->type != type) {
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
    } else {
        std::stringstream ss;
        ss << "Element \"" << name << "\" is not present.";
        throw ss.str();
    }

    return ret;
}

AuditConfig::AuditConfig(const cJSON *json) : AuditConfig() {
    cJSON *version = getObject(json, "version", cJSON_Number);
    if (version->valueint != 1) {
        std::stringstream ss;
        ss << "error: version " << version->valueint << " is not supported";
        throw ss.str();
    }

    set_rotate_size(getObject(json, "rotate_size", cJSON_Number));
    set_rotate_interval(getObject(json, "rotate_interval", cJSON_Number));
    set_auditd_enabled(getObject(json, "auditd_enabled", -1));
    set_buffered(cJSON_GetObjectItem(const_cast<cJSON*>(json), "buffered"));
    set_log_directory(getObject(json, "log_path", cJSON_String));
    set_descriptors_path(getObject(json, "descriptors_path", cJSON_String));
    set_sync(getObject(json, "sync", cJSON_Array));
    set_disabled(getObject(json, "disabled", cJSON_Array));

    std::map<std::string, int> tags;
    tags["version"] = 1;
    tags["rotate_size"] = 1;
    tags["rotate_interval"] = 1;
    tags["auditd_enabled"] = 1;
    tags["buffered"] = 1;
    tags["log_path"] = 1;
    tags["descriptors_path"] = 1;
    tags["sync"] = 1;
    tags["disabled"] = 1;

    for (cJSON *items = json->child; items != NULL; items = items->next) {
        if (tags.find(items->string) == tags.end()) {
            std::stringstream ss;
            ss << "Error: Unknown token \"" << items->string << "\""
               << std::endl;
            throw ss.str();
        }
    }
}

bool AuditConfig::is_auditd_enabled(void) const {
    return auditd_enabled;
}

void AuditConfig::set_auditd_enabled(bool value) {
    auditd_enabled = value;
}

void AuditConfig::set_rotate_size(size_t size) {
    if (size > max_rotate_file_size) {
        std::stringstream ss;
        ss << "error: rotation size " << size
           << " is too big. Legal range is <0, "
           << max_rotate_file_size << "]";
        throw ss.str();
    }
    rotate_size = size;
}

size_t AuditConfig::get_rotate_size(void) const {
    return rotate_size;
}

void AuditConfig::set_rotate_interval(uint32_t interval) {
    if (interval > max_file_rotation_time ||
        interval < min_file_rotation_time) {
        std::stringstream ss;
        ss << "error: rotation interval "
           << interval << " is outside the legal range ["
           << min_file_rotation_time << ", "
           << max_file_rotation_time << "]";
        throw ss.str();
    }

    rotate_interval = interval;
}

uint32_t AuditConfig::get_rotate_interval(void) const {
    return rotate_interval;
}

void AuditConfig::set_buffered(bool enable) {
    buffered = enable;
}

bool AuditConfig::is_buffered(void) const {
    return buffered;
}

void AuditConfig::set_log_directory(const std::string &directory) {
    /* Sanitize path */
    log_path = directory;
    sanitize_path(log_path);
    if (!CouchbaseDirectoryUtilities::mkdirp(log_path)) {
        std::stringstream ss;
        ss << "error: failed to create log directory \""
           << log_path << "\"";
        throw ss.str();
    }
}

const std::string &AuditConfig::get_log_directory(void) const {
    return log_path;
}

void AuditConfig::set_descriptors_path(const std::string &directory) {
    /* Sanitize path */
    descriptors_path = directory;
    sanitize_path(descriptors_path);

    std::stringstream tmp;
    tmp << descriptors_path << DIRECTORY_SEPARATOR_CHARACTER;
    tmp << "audit_events.json";
    FILE *fp = fopen(tmp.str().c_str(), "r");
    if (fp == NULL) {
        std::stringstream ss;
        ss << "Failed to open \"" << tmp.str().c_str() << "\": "
           << strerror(errno);
        throw ss.str();
    }
    fclose(fp);
}

const std::string &AuditConfig::get_descriptors_path(void) const {
    return descriptors_path;
}

bool AuditConfig::is_event_sync(uint32_t id) {
    return std::find(sync.begin(), sync.end(), id) != sync.end();
}

bool AuditConfig::is_event_disabled(uint32_t id) {
    return std::find(disabled.begin(), disabled.end(), id) != disabled.end();
}

void AuditConfig::sanitize_path(std::string &path) {
#ifdef WIN32
    // Make sure that the path is in windows format
    std::replace(path.begin(), path.end(), '/', '\\');
#endif

    if (path.length() > 1 && path.data()[path.length() - 1] == DIRECTORY_SEPARATOR_CHARACTER) {
        path.resize(path.length() - 1);
    }
}

void AuditConfig::set_rotate_size(cJSON *obj) {
    set_rotate_size(static_cast<size_t>(obj->valueint));
}

void AuditConfig::set_rotate_interval(cJSON *obj) {
    set_rotate_interval(static_cast<uint32_t>(obj->valueint));
}

void AuditConfig::set_auditd_enabled(cJSON *obj) {
    set_auditd_enabled(obj->type == cJSON_True);
}

void AuditConfig::set_buffered(cJSON *obj) {
    if (obj) {
        if (obj->type == cJSON_True) {
            set_buffered(true);
        } else if (obj->type == cJSON_False) {
            set_buffered(false);
        } else {
            std::stringstream ss;
            ss << "Incorrect type (" << obj->type
               << ") for \"buffered\". Should be boolean";
            throw ss.str();
        }
    }
}

void AuditConfig::set_log_directory(cJSON *obj) {
    set_log_directory(obj->valuestring);
}

void AuditConfig::set_descriptors_path(cJSON *obj) {
    set_descriptors_path(obj->valuestring);
}

void AuditConfig::add_array(std::vector<uint32_t> &vec, cJSON *array, const char *name) {
    vec.clear();
    for (auto *ii = array->child; ii != NULL; ii = ii->next) {
        if (ii->type != cJSON_Number) {
            std::stringstream ss;
            ss << "Incorrect type (" << ii->type
               << ") for element in " << name
               <<" array. Expected numbers";
            throw ss.str();
        }
        vec.push_back(ii->valueint);
    }
}

void AuditConfig::set_sync(cJSON *array) {
    add_array(sync, array, "sync");
}

void AuditConfig::set_disabled(cJSON *array) {
    add_array(disabled, array, "disabled");
}
