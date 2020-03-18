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
#include <nlohmann/json_fwd.hpp>
#include <platform/sized_buffer.h>
#include <string>

class AuditImpl;
class AuditConfig;

class Event {
public:
    const uint32_t id;
    const std::string payload;

    // Constructor required for ConfigureEvent
    Event()
        : id(0) {}

    Event(const uint32_t event_id, cb::const_char_buffer payload)
        : id(event_id), payload(payload.data(), payload.size()) {
    }

    virtual bool process(AuditImpl& audit);

    /**
     * State whether a given event should be filtered out given the userid.
     *
     * @param eventPayload  pointer to the event payload
     * @param config  reference to the audit configuration
     * @param userid  reference to the userid_type string, which will either be
     *                effective_userid or real_userid
     * @return true if event should be filtered out, else false.
     */
    bool filterEventByUserid(const nlohmann::json& eventPayload,
                             const AuditConfig& config,
                             const std::string& userid_type);

    /**
     * State whether a given event should be filtered out.
     *
     * @param eventPayload  pointer to the event payload
     * @param config  reference to the audit configuration
     * @return true if event should be filtered out, else false.
     */
    bool filterEvent(const nlohmann::json& eventPayload,
                     const AuditConfig& audit);

    virtual ~Event() {}

};
