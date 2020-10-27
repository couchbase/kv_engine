/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include <gsl/gsl-lite.hpp>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <vector>

class Event;

/**
 * The Module class represents the configuration for a single module.
 * See ../README.md for more information.
 */
class Module {
public:
    Module() = delete;
    Module(const nlohmann::json& json,
           const std::string& srcRoot,
           const std::string& objRoot);

    void createHeaderFile();

    /**
     * The name of the module
     */
    std::string name;
    /**
     * The lowest identifier for the audit events in this module. All
     * audit descriptor defined for this module MUST be within the range
     * [start, start + max_events_per_module]
     */
    int64_t start;
    /**
     * The name of the file containing the audit descriptors for this
     * module.
     */
    std::string file;
    /**
     * The JSON data describing the audit descriptors for this module
     */
    nlohmann::json json;
    /**
     * Is this module enterprise only?
     */
    bool enterprise = false;

    /**
     * If present this is the name of a C headerfile to generate with
     * #defines for all audit identifiers for the module.
     */
    std::string header;

    /**
     * A list of all of the events defined for this module
     */
    std::vector<std::unique_ptr<Event>> events;

protected:
    /**
     * Add the event to the list of events for the module
     *
     * @param event the event to add
     * @throws std::invalid_argument if the event is outside the legal range
     *                               for the module
     */
    void addEvent(std::unique_ptr<Event> event);

    /// Parse the event descriptor file and add all of the events into
    /// the list of events
    void parseEventDescriptorFile();
};
