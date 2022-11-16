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
#include <nlohmann/json.hpp>

void to_json(nlohmann::json& json, const Event& event) {
    json = nlohmann::json{
            {"id", event.id},
            {"name", event.name},
            {"description", event.description},
            {"sync", event.sync},
            {"enabled", event.enabled},
            {"filtering_permitted", event.filtering_permitted},
            {"mandatory_fields", nlohmann::json::parse(event.mandatory_fields)},
            {"optional_fields", nlohmann::json::parse(event.optional_fields)}};
}

void from_json(const nlohmann::json& j, Event& event) {
    j.at("id").get_to(event.id);
    j.at("name").get_to(event.name);
    j.at("description").get_to(event.description);
    event.sync = j.value("sync", false);
    event.enabled = j.value("enabled", true);
    event.filtering_permitted = j.value("filtering_permitted", false);
    event.mandatory_fields = j.at("mandatory_fields").dump();
    if (j.contains("optional_fields")) {
        event.optional_fields = j.at("optional_fields").dump();
    } else {
        event.optional_fields = "{}";
    }
}
