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
#include "generator_event.h"
#include "generator_utilities.h"

#include <nlohmann/json.hpp>
#include <gsl/gsl>
#include <sstream>

Event::Event(const nlohmann::json& json) {
    id = json.at("id");
    name = json.at("name");
    description = json.at("description");
    sync = json.at("sync");
    enabled = json.at("enabled");
    auto cFilteringPermitted = json.value("filtering_permitted", -1);
    mandatory_fields = json.at("mandatory_fields").dump();
    optional_fields = json.at("optional_fields").dump();

    if (cFilteringPermitted != -1) {
        filtering_permitted = gsl::narrow_cast<bool>(cFilteringPermitted);
    } else {
        filtering_permitted = false;
    }

    auto num_elem = json.size();
    if ((cFilteringPermitted == -1 && num_elem != 7) ||
        (cFilteringPermitted != -1 && num_elem != 8)) {
        std::stringstream ss;
        ss << "Unknown elements for " << name << ": " << std::endl
           << json << std::endl;
        throw std::runtime_error(ss.str());
    }
}