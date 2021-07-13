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
#include "config_parse.h"
#include "cmdline.h"
#include "settings.h"

#include <logger/logger.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>

/******************************************************************************
 * Public functions
 *****************************************************************************/

void load_config_file(const std::string& file, Settings& settings) {
    auto content = cb::io::loadFile(file, std::chrono::seconds{5});
    settings.reconfigure(nlohmann::json::parse(content));
}

std::optional<nlohmann::json> validate_proposed_config_changes(
        const char* new_cfg) {
    nlohmann::json errors = nlohmann::json::array();
    // Earlier we returned all of the errors, now I'm terminating on
    // the first... Ideally all of the errors would be best, but
    // the code is easier if we can use exceptions to abort the parsing
    // when we hit an error. Given that this isn't something that the
    // user would be calling every time I don't think it is a big problem..
    try {
        auto json = nlohmann::json::parse(new_cfg);
        Settings new_settings(json);
        Settings::instance().updateSettings(new_settings, false);
        return {};
    } catch (const std::exception& exception) {
        errors.push_back(exception.what());
    }

    return errors;
}
