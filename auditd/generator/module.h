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

#include <cJSON_utils.h>
#include <gsl.h>
#include <platform/dirutils.h>
#include <list>

class Event;

class Module {
public:
    Module(cJSON* data, const std::string& srcRoot, const std::string& objRoot);

    void addEvent(std::unique_ptr<Event> event);

    void createHeaderFile(void);

    /**
     * The name of the module
     */
    std::string name;
    /**
     * The lowest identifier for the audit events in this module. All
     * audit descriptor defined for this module MUST be within the range
     * [start, start + max_events_per_module]
     */
    uint32_t start;
    /**
     * The name of the file containing the audit descriptors for this
     * module.
     */
    std::string file;
    /**
     * The JSON data describing the audit descriptors for this module
     */
    unique_cJSON_ptr json;
    /**
     * Is this module enterprise only?
     */
    bool enterprise = false;

private:
    /**
     * If present this is the name of a C headerfile to generate with
     * #defines for all audit identifiers for the module.
     */
    std::string header;
    /**
     * A list of all of the events defined for this module
     */
    std::list<std::unique_ptr<Event>> events;
};
