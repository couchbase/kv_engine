/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "eventdescriptor.h"

#include <utilities/json_utilities.h>

#include <optional>

EventDescriptor::EventDescriptor(const nlohmann::json& root)
    : id(cb::jsonGet<uint32_t>(root, "id")),
      name(cb::jsonGet<std::string>(root, "name")),
      description(cb::jsonGet<std::string>(root, "description")),
      sync(cb::jsonGet<bool>(root, "sync")),
      enabled(cb::jsonGet<bool>(root, "enabled")),
      filteringPermitted(false) {
    size_t expected = 5;

    // Look for the optional parameter filtering_permitted
    filteringPermitted = root.value("filtering_permitted", false);
    expected += root.count("filtering_permitted");

    auto obj = cb::getOptionalJsonObject(root, "mandatory_fields");
    if (obj.has_value()) {
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
    if (obj.has_value()) {
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
