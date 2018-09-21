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
#include "eventdescriptor.h"

#include <utilities/json_utilities.h>

#include <boost/optional.hpp>
#include <stdexcept>

EventDescriptor::EventDescriptor(const nlohmann::json& root)
    : id(root.at("id").get<uint32_t>()),
      name(root.at("name").get<std::string>()),
      description(root.at("description").get<std::string>()),
      sync(root.at("sync").get<bool>()),
      enabled(root.at("enabled").get<bool>()),
      filteringPermitted(false) {
    size_t expected = 5;

    // Look for the optional parameter filtering_permitted
    filteringPermitted = root.value("filtering_permitted", false);
    expected += root.count("filtering_permitted");

    auto obj = cb::getOptionalJsonObject(root, "mandatory_fields");
    if (obj.is_initialized()) {
        if ((*obj).type() != nlohmann::json::value_t::array &&
            (*obj).type() != nlohmann::json::value_t::object) {
            throw std::invalid_argument(
                    "EventDescriptor::EventDescriptor: "
                    "Invalid type for mandatory_fields");
        } else {
            expected++;
        }
    }

    obj = cb::getOptionalJsonObject(root, "optional_fields");
    if (obj.is_initialized()) {
        if ((*obj).type() != nlohmann::json::value_t::array &&
            (*obj).type() != nlohmann::json::value_t::object) {
            throw std::invalid_argument(
                    "EventDescriptor::EventDescriptor: "
                    "Invalid type for optional_fields");
        } else {
            expected++;
        }
    }

    if (expected != root.size()) {
        throw std::invalid_argument(
                "EventDescriptor::EventDescriptor: "
                "Unknown elements specified. Number expected:" +
                std::to_string(expected) +
                " actual:" + std::to_string(root.size()));
    }
}
