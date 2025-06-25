/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <nlohmann/json_fwd.hpp>
#include <platform/byte_literals.h>
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility> // For std::pair
#include <vector>

class AuditEventFilter;

class AuditConfig {
public:
    enum class EventState { /** event state defined as enabled */ enabled,
                            /** event state defined as disabled */ disabled,
                            /** event state is not defined */ undefined };

    AuditConfig() = default;

    /**
     * Initialize from a JSON structure
     *
     * @param json the JSON document describing the configuration
     */
    explicit AuditConfig(const nlohmann::json& json);

    // methods to access the private parts
    bool is_auditd_enabled() const;
    void set_auditd_enabled(bool value);
    void set_rotate_size(size_t size);
    size_t get_rotate_size() const;
    void set_rotate_interval(uint32_t interval);
    uint32_t get_rotate_interval() const;
    void set_buffered(bool enable);
    bool is_buffered() const;
    void set_log_directory(std::string directory);
    std::string get_log_directory() const;
    void set_version(uint32_t ver);
    uint32_t get_version() const;
    bool is_event_sync(uint32_t id);
    AuditConfig::EventState get_event_state(uint32_t id) const;
    bool is_event_filtered(
            const std::pair<std::string, std::string>& userid) const;
    bool is_filtering_enabled() const;
    void set_filtering_enabled(bool value);
    void set_uuid(std::string uuid);
    std::string get_uuid() const;

    /**
     * Create a JSON representation of the audit configuration. This is
     * the same JSON representation that the constructor would accept.
     */
    nlohmann::json to_json() const;

    /// Get the filter to use for audit filtering. Currently this method
    /// maps an old style configuration into a new style configuration
    /// by building up the new JSON (and should therefore not be called
    /// while holding a mutex ;))
    nlohmann::json get_audit_event_filter() const;

    [[nodiscard]] std::optional<std::chrono::seconds> get_prune_age() const {
        return prune_age;
    }

protected:
    /// Temporary function until the server provides a new type of configuration
    std::vector<std::string> get_disabled_users() const;
    static void sanitize_path(std::string& path);
    static void add_array(std::vector<uint32_t>& vec,
                          const nlohmann::json& array,
                          const char* name);
    static void add_pair_string_array(
            std::vector<std::pair<std::string, std::string>>& vec,
            const nlohmann::json& array,
            const char* name);
    void set_sync(const nlohmann::json& array);
    void set_disabled_userids(const nlohmann::json& array);
    void set_event_states(const nlohmann::json& array);

    bool auditd_enabled{false};
    uint32_t rotate_interval{900};
    size_t rotate_size{20_MiB};
    std::optional<std::chrono::seconds> prune_age;
    bool buffered{true};
    bool filtering_enabled{false};
    uint32_t version{0};

    std::string log_path;
    std::vector<uint32_t> sync;
    std::vector<std::pair<std::string, std::string>> disabled_userids;
    std::unordered_map<uint32_t, EventState> event_states;
    std::string uuid;
};
