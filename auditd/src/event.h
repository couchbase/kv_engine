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

#include <nlohmann/json.hpp>
#include <cinttypes>
#include <string>

class AuditImpl;

class Event {
public:
    const uint32_t id;
    nlohmann::json payload;

    // Constructor required for ConfigureEvent
    Event() : id(0) {
    }

    Event(const uint32_t event_id, nlohmann::json payload)
        : id(event_id), payload(std::move(payload)) {
    }

    virtual bool process(AuditImpl& audit);

    virtual ~Event() = default;
};
