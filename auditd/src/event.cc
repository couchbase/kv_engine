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

bool Event::filterEventByUser(cJSON* json_payload,
                              const AuditConfig& config,
                              const std::string& userid_type) {
    auto* id = cJSON_GetObjectItem(json_payload, userid_type.c_str());
    if (id != nullptr) {
        auto* user = cJSON_GetObjectItem(id, "user");
        if (user != nullptr) {
            if (user->type != cJSON_String) {
                std::stringstream ss;
                ss << "Incorrect type for \"" << userid_type
                        << "::user\". Should be string.";
                throw std::invalid_argument(ss.str());
            } else if (config.is_event_filtered(user->valuestring)) {
                return true;
            }
        }
    }
    // Do not filter out the event
    return false;
}

bool Event::filterEvent(cJSON* json_payload, const AuditConfig& config) {
    // Check to see if real_userid::user is in the filter list.
    if (filterEventByUser(json_payload, config, "real_userid")) {
        return true;
    } else {
        // Check to see if effective_userid::user is in the filter list.
        return filterEventByUser(json_payload, config, "effective_userid");
    }
}

bool Event::process(Audit& audit) {
    // Audit is disabled
    if (!audit.config.is_auditd_enabled()) {
        return true;
    }

    // convert the event.payload into JSON
    cJSON *json_payload = cJSON_Parse(payload.c_str());
    if (json_payload == NULL) {
        Audit::log_error(AuditErrorCode::JSON_PARSING_ERROR, payload.c_str());
        return false;
    }
    cJSON *timestamp_ptr = cJSON_GetObjectItem(json_payload, "timestamp");
    if (timestamp_ptr == NULL) {
        std::string timestamp;
        timestamp = ISOTime::generatetimestamp();
        cJSON_AddStringToObject(json_payload, "timestamp", timestamp.c_str());
    }
    auto evt = audit.events.find(id);
    if (evt == audit.events.end()) {
        // it is an unknown event
        std::ostringstream convert;
        convert << id;
        Audit::log_error(AuditErrorCode::UNKNOWN_EVENT_ERROR, convert.str().c_str());
        cJSON_Delete(json_payload);
        return false;
    }
    if (!evt->second->isEnabled()) {
        // the event is not enabled so ignore event
        cJSON_Delete(json_payload);
        return true;
    }

    if (evt->second->isFilteringPermitted() &&
            filterEvent(json_payload, audit.config)) {
        return true;
    }

    if (!audit.auditfile.ensure_open()) {
        Audit::log_error(AuditErrorCode::OPEN_AUDITFILE_ERROR);
        cJSON_Delete(json_payload);
        return false;
    }
    cJSON_AddNumberToObject(json_payload, "id", id);
    cJSON_AddStringToObject(json_payload, "name", evt->second->getName().c_str());
    cJSON_AddStringToObject(json_payload, "description", evt->second->getDescription().c_str());

    bool success = audit.auditfile.write_event_to_disk(json_payload);

    // Release allocated resources
    cJSON_Delete(json_payload);

    if (success) {
        return true;
    } else {
        Audit::log_error(AuditErrorCode::WRITE_EVENT_TO_DISK_ERROR);
        return false;
    }
}
