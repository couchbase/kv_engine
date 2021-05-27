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
#include "generator_event.h"
#include "generator_utilities.h"

#include <gsl/gsl-lite.hpp>
#include <nlohmann/json.hpp>
#include <sstream>

Event::Event(const nlohmann::json& json) {
    id = json.at("id");
    name = json.at("name").get<std::string>();
    description = json.at("description").get<std::string>();
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
