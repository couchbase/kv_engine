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

#include "event.h"
#include "audit.h"
#include <cJSON.h>
#include <memcached/isotime.h>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>

bool Event::filterEventByUserid(const nlohmann::json& eventPayload,
                                const AuditConfig& config,
                                const std::string& userid_type) {
    if (eventPayload.find(userid_type) != eventPayload.end()) {
        auto id = eventPayload[userid_type];

        auto sourceIt = id.find("source");
        if (sourceIt != id.end() && !sourceIt->is_string()) {
            std::stringstream ss;
            ss << "Incorrect type for \"" << userid_type
               << "::source\". Should be string.";
            throw std::invalid_argument(ss.str());
        }

        auto domainIt = id.find("domain");
        if (domainIt != id.end() && !domainIt->is_string()) {
            std::stringstream ss;
            ss << "Incorrect type for \"" << userid_type
               << "::domain\". Should be string.";
            throw std::invalid_argument(ss.str());
        }

        if (sourceIt != id.end() || domainIt != id.end()) {
            auto userIt = id.find("user");
            if (userIt != id.end() && !userIt->is_string()) {
                std::stringstream ss;
                ss << "Incorrect type for \"" << userid_type
                   << "::user\". Should be string.";
                throw std::invalid_argument(ss.str());
            }

            if (userIt != id.end()) {
                // Have a source/domain and user so build the tuple and check if the
                // event is filtered
                auto& sourceValueString =
                        (sourceIt != id.end())
                                ? sourceIt->get_ref<std::string&>()
                                : domainIt->get_ref<std::string&>();
                const auto& userid = std::make_pair(
                        sourceValueString, (*userIt).get<std::string>());
                if (config.is_event_filtered(userid)) {
                    return true;
                }
            }
        }
    }
    // Do not filter out the event
    return false;
}

bool Event::filterEvent(const nlohmann::json& eventPayload,
                        const AuditConfig& config) {
    // Check to see if the real_userid is in the filter list.
    if (filterEventByUserid(eventPayload, config, "real_userid")) {
        return true;
    } else {
        // Check to see if the effective_userid is in the filter list.
        return filterEventByUserid(eventPayload, config, "effective_userid");
    }
}

bool Event::process(Audit& audit) {
    // Audit is disabled
    if (!audit.config.is_auditd_enabled()) {
        return true;
    }

    // convert the event.payload into JSON
    nlohmann::json json_payload;
    try {
        json_payload = nlohmann::json::parse(payload);
    } catch (const nlohmann::json::exception&) {
        Audit::log_error(AuditErrorCode::JSON_PARSING_ERROR, payload);
        return false;
    }

    if (json_payload.find("timestamp") == json_payload.end()) {
        // the audit does not contain a timestamp, so the server
        // needs to insert one
        const auto timestamp = ISOTime::generatetimestamp();
        json_payload["timestamp"] = timestamp;
    }

    auto evt = audit.events.find(id);
    if (evt == audit.events.end()) {
        // it is an unknown event
        Audit::log_error(AuditErrorCode::UNKNOWN_EVENT_ERROR,
                         std::to_string(id));
        return false;
    }
    if (!evt->second->isEnabled()) {
        // the event is not enabled so ignore event
        return true;
    }

    if (audit.config.is_filtering_enabled() &&
        evt->second->isFilteringPermitted() &&
        filterEvent(json_payload, audit.config)) {
        return true;
    }

    if (!audit.auditfile.ensure_open()) {
        Audit::log_error(AuditErrorCode::OPEN_AUDITFILE_ERROR,
                         json_payload.dump());
        return false;
    }
    json_payload["id"] = id;
    json_payload["name"] = evt->second->getName();
    json_payload["description"] = evt->second->getDescription();

    if (audit.auditfile.write_event_to_disk(json_payload)) {
        return true;
    }

    Audit::log_error(AuditErrorCode::WRITE_EVENT_TO_DISK_ERROR,
                     json_payload.dump());

    // If the write_event_to_disk function returns false then it is
    // possible the audit file has been closed.  Therefore ensure
    // the file is open.
    audit.auditfile.ensure_open();
    return false;
}
