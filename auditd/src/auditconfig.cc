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

#include <cJSON.h>
#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <sstream>

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
cJSON* AuditConfig::getObject(const cJSON* root, const char* name, int type)
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
    set_version(getObject(json, "version", cJSON_Number));
    set_rotate_size(getObject(json, "rotate_size", cJSON_Number));
    set_rotate_interval(getObject(json, "rotate_interval", cJSON_Number));
    set_auditd_enabled(getObject(json, "auditd_enabled", -1));
    set_buffered(cJSON_GetObjectItem(const_cast<cJSON*>(json), "buffered"));
    set_log_directory(getObject(json, "log_path", cJSON_String));
    set_descriptors_path(getObject(json, "descriptors_path", cJSON_String));
    set_sync(getObject(json, "sync", cJSON_Array));
    // The disabled list is depreciated in version 2
    if (get_version() == 1) {
        set_disabled(getObject(json, "disabled", cJSON_Array));
    }
    if (get_version() == 2) {
        set_filtering_enabled(getObject(json, "filtering_enabled", -1));
        set_uuid(getObject(json, "uuid", cJSON_String));
        set_disabled_userids(getObject(json, "disabled_userids", cJSON_Array));
        // event_states is optional so if not defined will not throw an
        // exception.
        if (cJSON_GetObjectItem(const_cast<cJSON*>(json), "event_states")) {
            set_event_states(getObject(json, "event_states", cJSON_Object));
        }
    }

    std::map<std::string, int> tags;
    tags["version"] = 1;
    tags["rotate_size"] = 1;
    tags["rotate_interval"] = 1;
    tags["auditd_enabled"] = 1;
    tags["buffered"] = 1;
    tags["log_path"] = 1;
    tags["descriptors_path"] = 1;
    tags["sync"] = 1;
    // The disabled list is depreciated in version 2 - if defined will
    // just be ignored.
    tags["disabled"] = 1;
    if (get_version() == 2) {
        tags["filtering_enabled"] = 1;
        tags["uuid"] = 1;
        tags["disabled_userids"] = 1;
        tags["event_states"] = 1;
    }

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
    std::lock_guard<std::mutex> guard(log_path_mutex);
    /* Sanitize path */
    log_path = directory;
    sanitize_path(log_path);
    try {
        cb::io::mkdirp(log_path);
    } catch (const std::runtime_error& error) {
        std::stringstream ss;
        ss << "error: failed to create log directory \""
           << log_path << "\": " << error.what();
        throw ss.str();
    }
}

std::string AuditConfig::get_log_directory(void) const {
    std::lock_guard<std::mutex> guard(log_path_mutex);
    return log_path;
}

void AuditConfig::set_descriptors_path(const std::string &directory) {
    std::lock_guard<std::mutex> guard(descriptor_path_mutex);
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

std::string AuditConfig::get_descriptors_path(void) const {
    std::lock_guard<std::mutex> guard(descriptor_path_mutex);
    return descriptors_path;
}

void AuditConfig::set_version(uint32_t ver) {
    if ((ver != 1) && (ver != 2))  {
           std::stringstream ss;
           ss << "error: version " << ver << " is not supported";
           throw ss.str();
       }
    version = ver;
}

uint32_t AuditConfig::get_version() const {
    return version;
}

bool AuditConfig::is_event_sync(uint32_t id) {
    std::lock_guard<std::mutex> guard(sync_mutex);
    return std::find(sync.begin(), sync.end(), id) != sync.end();
}

bool AuditConfig::is_event_disabled(uint32_t id) {
    std::lock_guard<std::mutex> guard(disabled_mutex);
    return std::find(disabled.begin(), disabled.end(), id) != disabled.end();
}

AuditConfig::EventState AuditConfig::get_event_state(uint32_t id) const {
    std::lock_guard<std::mutex> guard(event_states_mutex);
    const auto it = event_states.find(id);
    if (it == event_states.end()) {
        // If event state is not defined (as either enabled or disabled) then
        // return undefined.
        return EventState::undefined;
    }
    return it->second;
}

bool AuditConfig::is_event_filtered(
        const std::pair<std::string, std::string>& userid) const {
    std::lock_guard<std::mutex> guard(disabled_userids_mutex);
    return std::find(disabled_userids.begin(),
                     disabled_userids.end(),
                     userid) != disabled_userids.end();
}

void AuditConfig::set_filtering_enabled(bool value) {
    filtering_enabled = value;
}

bool AuditConfig::is_filtering_enabled() const {
    return filtering_enabled;
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

void AuditConfig::set_uuid(const std::string &_uuid) {
    std::lock_guard<std::mutex> guard(uuid_mutex);
    uuid = _uuid;
}

std::string AuditConfig::get_uuid() const {
    std::lock_guard<std::mutex> guard(uuid_mutex);
    return uuid;
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

void AuditConfig::set_filtering_enabled(cJSON *obj) {
    set_filtering_enabled(obj->type == cJSON_True);
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

void AuditConfig::set_version(cJSON *obj) {
    set_version(static_cast<uint32_t>(obj->valueint));
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
        vec.push_back(gsl::narrow<uint32_t>(ii->valueint));
    }
}

void AuditConfig::add_event_states_object(
        std::unordered_map<uint32_t, EventState>& eventStates,
        cJSON* object,
        const char* name) {
    eventStates.clear();
    cJSON* obj = object->child;
    while (obj != nullptr) {
        std::string event(obj->string);
        if (obj->type != cJSON_String) {
            throw std::invalid_argument(
                    "Incorrect type for state. Should be string.");
        }
        std::string state{obj->valuestring};
        EventState estate{EventState::undefined};
        if (state == "enabled") {
            estate = EventState::enabled;
        } else if (state == "disabled") {
            estate = EventState::disabled;
        }
        // add to the eventStates map
        eventStates[std::stoi(event)] = estate;
        obj = obj->next;
    }
}

void AuditConfig::add_pair_string_array(
        std::vector<std::pair<std::string, std::string>>& vec,
        cJSON* array,
        const char* name) {
    vec.clear();
    for (int ii = 0; ii < cJSON_GetArraySize(array); ii++) {
        auto element = cJSON_GetArrayItem(array, ii);
        if (element->type != cJSON_Object) {
            std::stringstream ss;
            ss << "Incorrect type (" << element->type << ") for element in "
               << name << " array. Expected objects";
            throw std::invalid_argument(ss.str());
        }
        auto* source = cJSON_GetObjectItem(element, "source");
        if (source != nullptr) {
            if (source->type != cJSON_String) {
                throw std::invalid_argument(
                        "Incorrect type for source. Should be string.");
            }
            auto* user = cJSON_GetObjectItem(element, "user");
            if (user != nullptr) {
                if (user->type != cJSON_String) {
                    throw std::invalid_argument(
                            "Incorrect type for user. Should be string.");
                }
                // Have a source and user so build the pair and add to the
                // vector
                const auto& userid =
                        std::make_pair(source->valuestring, user->valuestring);
                vec.push_back(userid);
            }
        }
    }
}

void AuditConfig::set_sync(cJSON *array) {
    std::lock_guard<std::mutex> guard(sync_mutex);
    add_array(sync, array, "sync");
}

void AuditConfig::set_disabled(cJSON *array) {
    std::lock_guard<std::mutex> guard(disabled_mutex);
    add_array(disabled, array, "disabled");
}

void AuditConfig::set_disabled_userids(cJSON* array) {
    std::lock_guard<std::mutex> guard(disabled_userids_mutex);
    add_pair_string_array(disabled_userids, array, "disabled_userids");
}

void AuditConfig::set_event_states(cJSON* object) {
    std::lock_guard<std::mutex> guard(event_states_mutex);
    add_event_states_object(event_states, object, "event_states");
}

void AuditConfig::set_uuid(cJSON *obj) {
    set_uuid(obj->valuestring);
}

unique_cJSON_ptr AuditConfig::to_json() const {
    unique_cJSON_ptr ret(cJSON_CreateObject());
    cJSON* root = ret.get();
    if (root == nullptr) {
        throw std::bad_alloc();
    }

    cJSON_AddNumberToObject(root, "version", get_version());
    cJSON_AddBoolToObject(root, "auditd_enabled", is_auditd_enabled());
    cJSON_AddNumberToObject(root, "rotate_size", get_rotate_size());
    cJSON_AddNumberToObject(root, "rotate_interval", get_rotate_interval());
    cJSON_AddBoolToObject(root, "buffered", is_buffered());
    cJSON_AddStringToObject(root, "log_path", get_log_directory().c_str());
    cJSON_AddStringToObject(root, "descriptors_path", get_descriptors_path().c_str());
    cJSON_AddBoolToObject(root, "filtering_enabled", is_filtering_enabled());
    cJSON_AddStringToObject(root, "uuid", get_uuid().c_str());

    cJSON* array = cJSON_CreateArray();
    for (const auto& v : sync) {
        cJSON_AddItemToArray(array, cJSON_CreateNumber(v));
    }
    cJSON_AddItemToObject(root, "sync", array);

    array = cJSON_CreateArray();
    for (const auto& v : disabled) {
        cJSON_AddItemToArray(array, cJSON_CreateNumber(v));
    }
    cJSON_AddItemToObject(root, "disabled", array);

    array = cJSON_CreateArray();
    for (const auto& v : disabled_userids) {
        cJSON* userIdRoot = cJSON_CreateObject();
        if (userIdRoot == nullptr) {
            throw std::runtime_error(
                    "AuditConfig::to_json - Error creating "
                    "cJSON object");
        }
        std::string source;
        std::string user;
        std::tie(source, user) = v;
        cJSON_AddStringToObject(userIdRoot, "source", source.c_str());
        cJSON_AddStringToObject(userIdRoot, "user", user.c_str());
        cJSON_AddItemToArray(array, userIdRoot);
    }
    cJSON_AddItemToObject(root, "disabled_userids", array);

    cJSON* object = cJSON_CreateObject();
    for (const auto& v : event_states) {
        std::string event = std::to_string(v.first);
        EventState estate = v.second;
        std::string state;
        switch (estate) {
        case EventState::enabled: {
            state = "enabled";
            break;
        }
        case EventState::disabled: {
            state = "disabled";
            break;
        }
        case EventState::undefined: {
            throw std::logic_error(
                    "AuditConfig::to_json - EventState:undefined should not be "
                    "found in the event_states list");
        }
        }
        cJSON_AddStringToObject(object, event.c_str(), state.c_str());
    }
    cJSON_AddItemToObject(root, "event_states", object);

    return ret;
}

void AuditConfig::initialize_config(const cJSON* json) {
    AuditConfig other(json);

    auditd_enabled = other.auditd_enabled;
    rotate_interval = other.rotate_interval;
    rotate_size = other.rotate_size;
    buffered = other.buffered;
    filtering_enabled = other.filtering_enabled;
    {
        std::lock_guard<std::mutex> guard(log_path_mutex);
        log_path = other.log_path;
    }
    {
        std::lock_guard<std::mutex> guard(descriptor_path_mutex);
        descriptors_path = other.descriptors_path;
    }
    {
        std::lock_guard<std::mutex> guard(sync_mutex);
        sync = other.sync;
    }

    {
        std::lock_guard<std::mutex> guard(disabled_mutex);
        disabled = other.disabled;
    }

    {
        std::lock_guard<std::mutex> guard(disabled_userids_mutex);
        disabled_userids = other.disabled_userids;
    }

    {
        std::lock_guard<std::mutex> guard(event_states_mutex);
        event_states = other.event_states;
    }

    {
        std::lock_guard<std::mutex> guard(uuid_mutex);
        uuid = other.uuid;
    }

    min_file_rotation_time = other.min_file_rotation_time;
    max_file_rotation_time = other.max_file_rotation_time;
    max_rotate_file_size = other.max_rotate_file_size;
    version = other.version;
}
