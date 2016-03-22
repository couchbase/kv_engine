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
#ifndef AUDITCONFIG_H
#define AUDITCONFIG_H

#include <inttypes.h>
#include <string>
#include <vector>
#include <cJSON.h>
#include <cJSON_utils.h>

class AuditConfig {
public:

    AuditConfig(void) :
        auditd_enabled(false),
        rotate_interval(900),
        rotate_size(20 * 1024 * 1024),
        buffered(true),
        min_file_rotation_time(900), // 15 minutes
        max_file_rotation_time(604800), // 1 week
        max_rotate_file_size(500 * 1024 * 1024)
    {
        // Empty
    }

    /**
     * Initialize from a JSON structure
     *
     * @param json the JSON document describing the configuration
     */
    AuditConfig(const cJSON *json);

    /**
     * Initialize the object from the specified JSON payload
     *
     * @todo refactor the logic to
     */
    void initialize_config(const cJSON *json) {
        AuditConfig other(json);
        *this = other;
    }

    // methods to access the private parts
    bool is_auditd_enabled(void) const;
    void set_auditd_enabled(bool value);
    void set_rotate_size(size_t size);
    size_t get_rotate_size(void) const;
    void set_rotate_interval(uint32_t interval);
    uint32_t get_rotate_interval(void) const;
    void set_buffered(bool enable);
    bool is_buffered(void) const;
    void set_log_directory(const std::string &directory);
    const std::string &get_log_directory(void) const;
    void set_descriptors_path(const std::string &directory);
    const std::string &get_descriptors_path(void) const;
    bool is_event_sync(uint32_t id);
    bool is_event_disabled(uint32_t id);


    void set_min_file_rotation_time(uint32_t min_file_rotation_time) {
        AuditConfig::min_file_rotation_time = min_file_rotation_time;
    }

    uint32_t get_min_file_rotation_time() const {
        return min_file_rotation_time;
    }

    void set_max_file_rotation_time(uint32_t max_file_rotation_time) {
        AuditConfig::max_file_rotation_time = max_file_rotation_time;
    }

    uint32_t get_max_file_rotation_time() const {
        return max_file_rotation_time;
    }

    size_t get_max_rotate_file_size() const {
        return max_rotate_file_size;
    }

    /**
     * Create a JSON representation of the audit configuration. This is
     * the same JSON representation that the constructor would accept.
     */
    unique_cJSON_ptr to_json() const;

protected:
    void sanitize_path(std::string &path);
    void set_rotate_size(cJSON *obj);
    void set_rotate_interval(cJSON *obj);
    void set_auditd_enabled(cJSON *obj);
    void set_buffered(cJSON *obj);
    void set_log_directory(cJSON *obj);
    void set_descriptors_path(cJSON *obj);
    void add_array(std::vector<uint32_t> &vec, cJSON *array, const char *name);
    void set_sync(cJSON *array);
    void set_disabled(cJSON *array);

    bool auditd_enabled;
    uint32_t rotate_interval;
    size_t rotate_size;
    bool buffered;
    std::string log_path;
    std::string descriptors_path;
    std::vector<uint32_t> sync;
    std::vector<uint32_t> disabled;

    uint32_t min_file_rotation_time;
    uint32_t max_file_rotation_time;
    size_t max_rotate_file_size;
};

#endif
