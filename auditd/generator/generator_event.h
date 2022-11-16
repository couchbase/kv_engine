/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <nlohmann/json_fwd.hpp>
#include <cstdint>
#include <string>

/**
 * The Event class represents the information needed for a single
 * audit event entry.
 */
struct Event {
    /// The identifier for this entry
    uint32_t id;
    /// The name of the entry
    std::string name;
    /// The full description of the entry
    std::string description;
    /// Set to true if this entry should be handled synchronously
    bool sync;
    /// Set to true if this entry is enabled (or should be dropped)
    bool enabled;
    /// Set to true if the user may enable filtering for the entry
    bool filtering_permitted;
    /// The textual representation of the JSON describing mandatory
    /// fields in the event (NOTE: this is currently not enforced
    /// by the audit daemon)
    std::string mandatory_fields;
    /// The textual representation of the JSON describing the optional
    /// fields in the event (NOTE: this is currently not enforced
    /// by the audit daemon)
    std::string optional_fields;
};

void to_json(nlohmann::json& json, const Event& event);
void from_json(const nlohmann::json& j, Event& event);
