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

#include <nlohmann/json.hpp>
#include <vector>

struct Event;

/**
 * The Module class represents the configuration for a single module.
 * See ../README.md for more information.
 */
class Module {
public:
    /// The version of the descriptor file format we support
    constexpr static size_t SupportedVersion = 2;

    /// The name of the module
    std::string name;
    /**
     * The lowest identifier for the audit events in this module. All
     * audit descriptor defined for this module MUST be within the range
     * [start, start + max_events_per_module]
     */
    int64_t start;

    /// The name of the file containing the audit descriptors for this module.
    std::string path;

    /// The various configurations the module should be included in
    std::vector<std::string> configurations;

    /// Should macros be defined for the events in this module?
    bool generateMacros = false;

    /**
     * The JSON data describing the audit descriptors for this module
     * (read from the file specified in `path` when calling
     * `loadEventDescriptorFile`)
     */
    nlohmann::json json;

    /**
     * A list of all of the events defined for this module (created as part
     * of the `loadEventDescriptorFile` method)
     */
    std::vector<Event> events;

    /// Parse the event descriptor file and add all of the events into
    /// the list of events
    void loadEventDescriptorFile(const std::filesystem::path& source_root);

    /// Is this module included in the current configuration?
    bool includeInConfiguration() const;

    /// Create the header file containing the macros for the events
    void createMacros(std::ostream& out);

protected:
    /**
     * Add the event to the list of events for the module
     *
     * @param event the event to add
     * @throws std::invalid_argument if the event is outside the legal range
     *                               for the module
     */
    void addEvent(Event event);
};

void from_json(const nlohmann::json& json, Module& module);
