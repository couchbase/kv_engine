/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "auditconfig.h"
#include "audit_descriptor_manager.h"
#include <fmt/format.h>
#include <gsl/gsl-lite.hpp>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <platform/strerror.h>
#include <utilities/json_utilities.h>
#include <algorithm>

AuditConfig::AuditConfig(const nlohmann::json& json) {
    set_version(json.at("version"));
    set_rotate_size(json.at("rotate_size"));
    set_rotate_interval(json.at("rotate_interval"));
    set_auditd_enabled(json.at("auditd_enabled"));
    set_buffered(json.value("buffered", true));
    set_log_directory(json.at("log_path"));
    set_sync(json.at("sync"));

    set_filtering_enabled(json.at("filtering_enabled"));
    set_uuid(json.at("uuid"));
    auto duids = json.at("disabled_userids");
    if (duids.is_array()) {
        set_disabled_userids(duids);
    } else {
        throw std::invalid_argument(fmt::format(
                "AuditConfig::AuditConfig 'disabled_userids' should "
                "be array, but got: '{}'",
                duids.type_name()));
    }
    // event_states is optional so if not defined will not throw an
    // exception. It is used to override default values, so
    // lets start by initialize the default values
    AuditDescriptorManager::iterate([this](const auto& e) {
        if (e.isEnabled()) {
            event_states[e.getId()] = EventState::enabled;
        }
    });

    if (json.find("event_states") != json.end()) {
        set_event_states(json.at("event_states"));
    }

    std::map<std::string, int> tags;
    tags["version"] = 1;
    tags["rotate_size"] = 1;
    tags["rotate_interval"] = 1;
    tags["auditd_enabled"] = 1;
    tags["buffered"] = 1;
    tags["log_path"] = 1;
    tags["descriptors_path"] = 1; // currently ignored
    tags["sync"] = 1;
    // The disabled list is depreciated in version 2 - if defined will
    // just be ignored.
    tags["disabled"] = 1;
    tags["filtering_enabled"] = 1;
    tags["uuid"] = 1;
    tags["disabled_userids"] = 1;
    tags["event_states"] = 1;

    for (auto it = json.begin(); it != json.end(); ++it) {
        if (tags.find(it.key()) == tags.end()) {
            throw std::invalid_argument(fmt::format(
                    R"(AuditConfig::AuditConfig(): Error: Unknown token "{}")",
                    it.key()));
        }
    }
}

bool AuditConfig::is_auditd_enabled() const {
    return auditd_enabled;
}

void AuditConfig::set_auditd_enabled(bool value) {
    auditd_enabled = value;
}

void AuditConfig::set_rotate_size(size_t size) {
    if (size > max_rotate_file_size) {
        throw std::invalid_argument(
                fmt::format("AuditConfig::set_rotate_size(): Rotation size {} "
                            "is too big. Legal range is [0, {}]",
                            size,
                            max_rotate_file_size));
    }
    rotate_size = size;
}

size_t AuditConfig::get_rotate_size() const {
    return rotate_size;
}

void AuditConfig::set_rotate_interval(uint32_t interval) {
    if (interval != 0 && (interval > max_file_rotation_time ||
                          interval < min_file_rotation_time)) {
        throw std::invalid_argument(
                fmt::format("AuditConfig::set_rotate_interval(): Rotation "
                            "interval {} is outside the legal range [{}, {}]",
                            interval,
                            min_file_rotation_time,
                            max_file_rotation_time));
    }

    rotate_interval = interval;
}

uint32_t AuditConfig::get_rotate_interval() const {
    return rotate_interval;
}

void AuditConfig::set_buffered(bool enable) {
    buffered = enable;
}

bool AuditConfig::is_buffered() const {
    return buffered;
}

void AuditConfig::set_log_directory(std::string directory) {
    sanitize_path(directory);
    log_path.swap(directory);
}

std::string AuditConfig::get_log_directory() const {
    return log_path;
}

void AuditConfig::set_version(uint32_t ver) {
    if (ver != 2) {
        throw std::invalid_argument(fmt::format(
                "AuditConfig::set_version(): version {} is not supported",
                ver));
    }
    version = ver;
}

uint32_t AuditConfig::get_version() const {
    return version;
}

bool AuditConfig::is_event_sync(uint32_t id) {
    return std::find(sync.begin(), sync.end(), id) != sync.end();
}

AuditConfig::EventState AuditConfig::get_event_state(uint32_t id) const {
    const auto it = event_states.find(id);
    if (it == event_states.end()) {
        // If event state is not defined (as either enabled or disabled)
        // then return undefined.
        return EventState::undefined;
    }
    return it->second;
}

bool AuditConfig::is_event_filtered(
        const std::pair<std::string, std::string>& userid) const {
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

void AuditConfig::sanitize_path(std::string& path) {
    path = cb::io::sanitizePath(path);
    if (path.length() > 1 && path.back() == cb::io::DirectorySeparator) {
        path.resize(path.length() - 1);
    }
}

void AuditConfig::set_uuid(std::string _uuid) {
    uuid.swap(_uuid);
}

std::string AuditConfig::get_uuid() const {
    return uuid;
}

void AuditConfig::add_array(std::vector<uint32_t>& vec,
                            const nlohmann::json& json,
                            const char* name) {
    vec.clear();

    for (const auto& elem : json) {
        if (elem.is_number()) {
            vec.push_back(gsl::narrow<uint32_t>(elem));
        } else {
            throw std::invalid_argument(
                    fmt::format("AuditConfig::add_array(): Incorrect type ({}) "
                                "for element in {} array. Expected numbers",
                                elem.type_name(),
                                name));
        }
    }
}

void AuditConfig::add_pair_string_array(
        std::vector<std::pair<std::string, std::string>>& vec,
        const nlohmann::json& array,
        const char* name) {
    vec.clear();

    for (auto& elem : array) {
        if (!elem.is_object()) {
            throw std::invalid_argument(fmt::format(
                    "AuditConfig::add_pair_string_array(): Incorrect type({}) "
                    "for element in {} array. Expected objects",
                    elem.type_name(),
                    name));
        }
        std::string source;
        std::string domain;

        if (elem.find("source") != elem.end()) {
            auto s = elem.at("source");
            if (!s.is_string()) {
                throw std::invalid_argument(
                        "Incorrect type for source. Should be string.");
            }
            source = s.get<std::string>();
        }

        if (elem.find("domain") != elem.end()) {
            auto d = elem.at("domain");
            if (!d.is_string()) {
                throw std::invalid_argument(
                        "Incorrect type for domain. Should be string.");
            }
            domain = d.get<std::string>();
        }

        if (!source.empty() || !domain.empty()) {
            std::string user;

            if (elem.find("user") != elem.end()) {
                auto u = elem.at("user");
                if (!u.is_string()) {
                    throw std::invalid_argument(
                            "Incorrect type for user. Should be string.");
                }
                user = u.get<std::string>();
            }

            if (!user.empty()) {
                // Have a source/domain and user so build the pair and add to
                // the vector
                auto sourceValueString = (!source.empty()) ? source : domain;
                const auto& userid = std::make_pair(sourceValueString, user);
                vec.push_back(userid);
            }
        }
    }
}

void AuditConfig::set_sync(const nlohmann::json& array) {
    add_array(sync, array, "sync");
}

void AuditConfig::set_disabled_userids(const nlohmann::json& array) {
    add_pair_string_array(disabled_userids, array, "disabled_userids");
}

void AuditConfig::set_event_states(const nlohmann::json& object) {
    for (auto it = object.begin(); it != object.end(); ++it) {
        std::string event(it.key());
        std::string state{cb::jsonGet<std::string>(it)};
        EventState estate{EventState::undefined};
        if (state == "enabled") {
            estate = EventState::enabled;
        } else if (state == "disabled") {
            estate = EventState::disabled;
        }
        // add to the eventStates map
        event_states[std::stoi(event)] = estate;
    }
}

nlohmann::json AuditConfig::to_json() const {
    nlohmann::json ret;
    ret["version"] = get_version();
    ret["auditd_enabled"] = is_auditd_enabled();
    ret["rotate_size"] = get_rotate_size();
    ret["rotate_interval"] = get_rotate_interval();
    ret["buffered"] = is_buffered();
    ret["log_path"] = get_log_directory();
    ret["filtering_enabled"] = is_filtering_enabled();
    ret["uuid"] = get_uuid();

    ret["sync"] = sync;

    auto array = nlohmann::json::array();

    for (const auto& v : disabled_userids) {
        nlohmann::json userIdRoot;
        userIdRoot["domain"] = v.first;
        userIdRoot["user"] = v.second;
        array.push_back(userIdRoot);
    }

    ret["disabled_userids"] = array;

    nlohmann::json object;
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
                    "AuditConfig::to_json - EventState:undefined should "
                    "not be found in the event_states list");
        }
        }
        object[event] = state;
    }

    ret["event_states"] = object;

    return ret;
}

std::vector<std::string> AuditConfig::get_disabled_users() const {
    std::vector<std::string> ret;

    for (const auto& [domain, user] : disabled_userids) {
        if (domain == "local") {
            ret.emplace_back(user + "/couchbase");
        } else {
            ret.emplace_back(user + "/external");
        }
    }
    return ret;
}

nlohmann::json AuditConfig::get_audit_event_filter() const {
    if (!auditd_enabled) {
        return {};
    }
    nlohmann::json ret;
    auto& def = ret["default"];
    std::vector<uint32_t> enabled;

    for (const auto& [id, state] : event_states) {
        if (state == EventState::enabled) {
            enabled.push_back(id);
        }
    }

    def["enabled"] = enabled;
    auto users = get_disabled_users();
    for (const auto& u : users) {
        def["filter_out"][u] = enabled;
    }
    return ret;
}