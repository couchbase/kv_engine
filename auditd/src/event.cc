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

#include <sstream>
#include <string>
#include <cJSON.h>
#include <memcached/isotime.h>
#include "event.h"
#include "audit.h"

bool Event::filterEventByUserid(cJSON* json_payload,
                                const AuditConfig& config,
                                const std::string& userid_type) {
    auto* id = cJSON_GetObjectItem(json_payload, userid_type.c_str());
    if (id != nullptr) {
        auto* source = cJSON_GetObjectItem(id, "source");
        auto* domain = cJSON_GetObjectItem(id, "domain");
        if (source != nullptr && source->type != cJSON_String) {
            std::stringstream ss;
            ss << "Incorrect type for \"" << userid_type
                    << "::source\". Should be string.";
            throw std::invalid_argument(ss.str());
        }
        if (domain != nullptr && domain->type != cJSON_String) {
            std::stringstream ss;
            ss << "Incorrect type for \"" << userid_type
                    << "::domain\". Should be string.";
            throw std::invalid_argument(ss.str());
        }
        if (source != nullptr || domain != nullptr) {
            auto* user = cJSON_GetObjectItem(id, "user");
            if (user != nullptr) {
                if (user->type != cJSON_String) {
                    std::stringstream ss;
                    ss << "Incorrect type for \"" << userid_type
                       << "::user\". Should be string.";
                    throw std::invalid_argument(ss.str());
                }
                // Have a source/domain and user so build the tuple and check if the
                // event is filtered
                auto* sourceValueString = (source != nullptr) ?
                        source->valuestring : domain->valuestring;
                const auto& userid =
                        std::make_pair(sourceValueString, user->valuestring);
                if (config.is_event_filtered(userid)) {
                    return true;
                }
            }
        }
    }
    // Do not filter out the event
    return false;
}

bool Event::filterEvent(cJSON* json_payload, const AuditConfig& config) {
    // Check to see if the real_userid is in the filter list.
    if (filterEventByUserid(json_payload, config, "real_userid")) {
        return true;
    } else {
        // Check to see if the effective_userid is in the filter list.
        return filterEventByUserid(json_payload, config, "effective_userid");
    }
}

bool Event::process(Audit& audit) {
    // Audit is disabled
    if (!audit.config.is_auditd_enabled()) {
        return true;
    }

    // convert the event.payload into JSON
    unique_cJSON_ptr json_payload(cJSON_Parse(payload.c_str()));
    if (!json_payload) {
        Audit::log_error(AuditErrorCode::JSON_PARSING_ERROR, payload);
        return false;
    }

    cJSON* timestamp_ptr = cJSON_GetObjectItem(json_payload.get(), "timestamp");
    if (timestamp_ptr == nullptr) {
        // the audit does not contain a timestamp, so the server
        // needs to insert one
        const auto timestamp = ISOTime::generatetimestamp();
        cJSON_AddStringToObject(
                json_payload.get(), "timestamp", timestamp.c_str());
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
        filterEvent(json_payload.get(), audit.config)) {
        return true;
    }

    if (!audit.auditfile.ensure_open()) {
        Audit::log_error(AuditErrorCode::OPEN_AUDITFILE_ERROR,
                         to_string(json_payload, false));
        return false;
    }
    cJSON_AddNumberToObject(json_payload.get(), "id", id);
    cJSON_AddStringToObject(
            json_payload.get(), "name", evt->second->getName().c_str());
    cJSON_AddStringToObject(json_payload.get(),
                            "description",
                            evt->second->getDescription().c_str());

    if (audit.auditfile.write_event_to_disk(json_payload.get())) {
        return true;
    }

    Audit::log_error(AuditErrorCode::WRITE_EVENT_TO_DISK_ERROR,
                     to_string(json_payload, false));

    // If the write_event_to_disk function returns false then it is
    // possible the audit file has been closed.  Therefore ensure
    // the file is open.
    audit.auditfile.ensure_open();
    return false;
}
