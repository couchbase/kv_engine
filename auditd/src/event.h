/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include <inttypes.h>
#include <nlohmann/json_fwd.hpp>
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

    Event(const uint32_t event_id, std::string_view payload)
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
