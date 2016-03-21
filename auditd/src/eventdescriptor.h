/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include <string>
#include <cJSON_utils.h>

/**
 * The EventDescriptor class represents a single event descriptor and
 * contains the meta information for a single event.
 *
 * The current implementation ignores the mandatory and optional attributes
 */
class EventDescriptor {
public:
    /**
     * Initialize an object from the JSON representation of the event
     * descriptor.
     *
     * @param root pointer to the json represenatation
     * @throws std::logic_error if called with a JSON representation which
     *         isn't what we expect
     */
    EventDescriptor(const cJSON* root);

    const uint32_t getId() const {
        return id;
    }

    const std::string& getName() const {
        return name;
    }

    const std::string& getDescription() const {
        return description;
    }

    const bool isSync() const {
        return sync;
    }

    const bool isEnabled() const {
        return enabled;
    }


    void setSync(bool sync) {
        EventDescriptor::sync = sync;
    }

    void setEnabled(bool enabled) {
        EventDescriptor::enabled = enabled;
    }

protected:
    /**
     * Locate the given field in the JSON
     *
     * @param root pointer to the first entry
     * @param field the name of the field to locate
     * @param type the expected type of the object (Note: there is no
     *             cJSON_Bool #define, so you have to pass in cJSON_True
     *             for that).
     * @return the cJSON element for the object
     * @throws std::logic_error if the field is missing or is of an
     *         incorrect type
     */
    const cJSON* locate(const cJSON* root, const char* field, int type);

    const uint32_t id;
    const std::string name;
    const std::string description;
    bool sync;
    bool enabled;
};


