/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include <gsl/gsl-lite.hpp>
#include <nlohmann/json_fwd.hpp>
#include <cstdint>
#include <string>

/**
 * The Event class represents the information needed for a single
 * audit event entry.
 */
class Event {
public:
    Event() = delete;

    /**
     * Construct and initialize a new Event structure based off the
     * provided JSON. See ../README.md for information about the
     * layout of the JSON element.
     *
     * @param entry
     * @throws std::runtime_error for errors accessing the expected
     *                            elements
     */
    explicit Event(const nlohmann::json& json);

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
    /// Set to true if the user may enable filtering for the enry
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
