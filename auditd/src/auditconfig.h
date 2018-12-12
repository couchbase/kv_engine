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
#pragma once

#include <inttypes.h>
#include <nlohmann/json.hpp>
#include <atomic>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility> // For std::pair
#include <vector>

#include <relaxed_atomic.h>

class AuditConfig {
public:
    enum class EventState { /* event state defined as enabled */ enabled,
                            /* event state defined as disabled */ disabled,
                            /* event state is not defined */ undefined };

    AuditConfig(void) :
        auditd_enabled(false),
        rotate_interval(900),
        rotate_size(20 * 1024 * 1024),
        buffered(true),
        filtering_enabled(false),
        version(0),
        uuid(""),
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
    AuditConfig(const nlohmann::json& json);

    /**
     * Initialize the object from the specified JSON payload
     *
     * @todo refactor the logic to
     */
    void initialize_config(const nlohmann::json& json);

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
    std::string get_log_directory(void) const;
    void set_descriptors_path(const std::string &directory);
    std::string get_descriptors_path(void) const;
    void set_version(uint32_t ver);
    uint32_t get_version() const;
    bool is_event_sync(uint32_t id);
    // is_event_disabled is depreciated in version 2 of the configuration.
    bool is_event_disabled(uint32_t id);
    AuditConfig::EventState get_event_state(uint32_t id) const;
    bool is_event_filtered(
            const std::pair<std::string, std::string>& userid) const;
    bool is_filtering_enabled() const;
    void set_filtering_enabled(bool value);
    void set_uuid(const std::string &uuid);
    std::string get_uuid() const;

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
    nlohmann::json to_json() const;

protected:
    void sanitize_path(std::string &path);
    void add_array(std::vector<uint32_t>& vec,
                   const nlohmann::json& array,
                   const char* name);
    void add_pair_string_array(
            std::vector<std::pair<std::string, std::string>>& vec,
            const nlohmann::json& array,
            const char* name);
    void add_event_states_object(
            std::unordered_map<uint32_t, EventState>& eventStates,
            const nlohmann::json& object,
            const char* name);
    void set_sync(const nlohmann::json& array);
    void set_disabled(const nlohmann::json& array);
    void set_disabled_userids(const nlohmann::json& array);
    void set_event_states(const nlohmann::json& array);

    Couchbase::RelaxedAtomic<bool> auditd_enabled;
    Couchbase::RelaxedAtomic<uint32_t> rotate_interval;
    Couchbase::RelaxedAtomic<size_t> rotate_size;
    Couchbase::RelaxedAtomic<bool> buffered;
    Couchbase::RelaxedAtomic<bool> filtering_enabled;
    Couchbase::RelaxedAtomic<uint32_t> version;

    mutable std::mutex log_path_mutex;
    std::string log_path;

    mutable std::mutex descriptor_path_mutex;
    std::string descriptors_path;

    std::mutex sync_mutex;
    std::vector<uint32_t> sync;

    std::mutex disabled_mutex;
    std::vector<uint32_t> disabled;

    mutable std::mutex disabled_userids_mutex;
    std::vector<std::pair<std::string, std::string>> disabled_userids;

    mutable std::mutex event_states_mutex;
    std::unordered_map<uint32_t, EventState> event_states;

    mutable std::mutex uuid_mutex;
    std::string uuid;

    Couchbase::RelaxedAtomic<uint32_t> min_file_rotation_time;
    Couchbase::RelaxedAtomic<uint32_t> max_file_rotation_time;
    Couchbase::RelaxedAtomic<size_t> max_rotate_file_size;
};
